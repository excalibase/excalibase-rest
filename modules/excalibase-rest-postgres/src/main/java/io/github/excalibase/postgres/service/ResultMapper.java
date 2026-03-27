package io.github.excalibase.postgres.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.model.MappedResult;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.postgres.util.PostgresTypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Converts the single-row JSON result produced by the QueryCompiler
 * into the list of record maps expected by the controller.
 *
 * <p>The compiler now generates SQL that returns exactly ONE row with two columns:
 * <ul>
 *   <li>{@code body}        — a {@code jsonb} value containing the records as a JSON array.</li>
 *   <li>{@code total_count} — optional {@code bigint}; present only when {@code includeCount=true}.</li>
 * </ul>
 *
 * <p>The JDBC driver delivers {@code jsonb} values as a {@code PGobject} instance whose
 * {@code getValue()} method returns the raw JSON string. This class parses that string with
 * Jackson and, for backward compatibility, applies {@link PostgresTypeConverter} to normalise
 * any PostgreSQL-specific scalar types (timestamps, UUIDs, etc.) that may still appear as
 * strings inside the JSON.
 *
 * <p>The old {@code mapResults(rows, query, tableInfo)} method is retained for the
 * single-record GET /{table}/{id} path, which still executes a regular (non-JSON-wrapped) query.
 */
@Service
public class ResultMapper implements io.github.excalibase.service.IResultMapper {

    private static final Logger log = LoggerFactory.getLogger(ResultMapper.class);

    private static final TypeReference<List<Map<String, Object>>> LIST_MAP_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;

    public ResultMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // ─── New JSON-body path ───────────────────────────────────────────────────

    /**
     * Extract records and optional total count from the single-row JSON result produced
     * by the new compiler.
     *
     * @param singleRow the one row returned by the compiled query; must contain a {@code body}
     *                  column (jsonb) and optionally a {@code total_count} column (bigint).
     * @param tableInfo table metadata used for PostgreSQL type normalisation.
     * @return the list of record maps parsed from the JSON body.
     */
    public MappedResult mapJsonBody(Map<String, Object> singleRow, TableInfo tableInfo) {
        if (singleRow == null) {
            return MappedResult.empty();
        }

        // ── Extract total_count ───────────────────────────────────────────────
        long totalCount = -1;
        Object countValue = singleRow.get("total_count");
        if (countValue instanceof Number num) {
            totalCount = num.longValue();
        }

        // ── Extract body column ───────────────────────────────────────────────
        Object bodyValue = singleRow.get("body");
        if (bodyValue == null) {
            return new MappedResult(List.of(), totalCount);
        }

        String jsonString = extractJsonString(bodyValue);
        if (jsonString == null || jsonString.isBlank()) {
            return new MappedResult(List.of(), totalCount);
        }

        String trimmed = jsonString.trim();
        if ("[]".equals(trimmed)) {
            return new MappedResult(List.of(), totalCount);
        }

        try {
            List<Map<String, Object>> records = objectMapper.readValue(trimmed, LIST_MAP_TYPE);
            return new MappedResult(PostgresTypeConverter.convertPostgresTypes(records, tableInfo), totalCount);
        } catch (Exception e) {
            log.error("Failed to parse JSON body column: {}", e.getMessage(), e);
            return new MappedResult(List.of(), totalCount);
        }
    }

    // ─── Legacy row-by-row path (used for single-record GET /{table}/{id}) ───

    /**
     * Map raw JDBC rows into properly typed response rows.
     *
     * <p>This method is still used by the single-record endpoint which does not go through
     * the JSON-wrapping compiler path.
     *
     * @param rows    raw rows from JdbcTemplate
     * @param query   the compiled query that produced the rows
     * @param tableInfo table metadata for type conversion
     * @return processed rows (safe to serialise as JSON)
     */
    public MappedResult mapResults(List<Map<String, Object>> rows,
                                     io.github.excalibase.compiler.CompiledQuery query,
                                     TableInfo tableInfo) {
        if (rows == null || rows.isEmpty()) {
            return new MappedResult(List.of(), 0);
        }

        long totalCount = -1;

        java.util.List<Map<String, Object>> result = new java.util.ArrayList<>(rows.size());

        for (Map<String, Object> rawRow : rows) {
            java.util.Map<String, Object> row = new java.util.HashMap<>(rawRow);

            // ── Extract _total_count (legacy window function path) ────────────
            if (query.hasCountWindow()) {
                Object countValue = row.remove("_total_count");
                if (countValue instanceof Number num && totalCount < 0) {
                    totalCount = num.longValue();
                }
            }

            // ── Parse JSON columns produced by subqueries ─────────────────────
            for (String jsonCol : query.jsonColumns()) {
                Object rawValue = row.get(jsonCol);
                if (rawValue == null) {
                    continue;
                }
                if (rawValue instanceof String jsonStr) {
                    row.put(jsonCol, parseJsonColumn(jsonStr, jsonCol));
                } else if (rawValue instanceof Map || rawValue instanceof List) {
                    // Already parsed — leave as is
                } else {
                    String jsonStr = extractJsonString(rawValue);
                    if (jsonStr != null) {
                        row.put(jsonCol, parseJsonColumn(jsonStr, jsonCol));
                    }
                }
            }

            result.add(row);
        }

        return new MappedResult(PostgresTypeConverter.convertPostgresTypes(result, tableInfo), totalCount);
    }

    // ─── Private helpers ──────────────────────────────────────────────────────

    /**
     * Extract the JSON string value from an arbitrary JDBC type.
     * Handles {@code PGobject} (the PostgreSQL JDBC driver's wrapper for json/jsonb columns)
     * and any other type by calling {@code toString()}.
     */
    private String extractJsonString(Object value) {
        if (value == null) return null;
        // PGobject has a getValue() method that returns the raw SQL value string
        try {
            java.lang.reflect.Method getValueMethod = value.getClass().getMethod("getValue");
            Object raw = getValueMethod.invoke(value);
            return raw != null ? raw.toString() : null;
        } catch (NoSuchMethodException e) {
            // Not a PGobject — fall through to toString()
        } catch (Exception e) {
            log.warn("Failed to extract JSON from {} via reflection: {}", value.getClass().getSimpleName(), e.getMessage());
        }
        String str = value.toString();
        if (str.startsWith("{") || str.startsWith("[")) {
            return str;
        }
        return null;
    }

    private Object parseJsonColumn(String json, String columnName) {
        if (json == null || json.isBlank()) return null;
        String trimmed = json.trim();
        try {
            if (trimmed.startsWith("{")) {
                return objectMapper.readValue(trimmed, new TypeReference<Map<String, Object>>() {});
            } else if (trimmed.startsWith("[")) {
                return objectMapper.readValue(trimmed, new TypeReference<List<Object>>() {});
            } else {
                return json;
            }
        } catch (Exception e) {
            log.warn("Failed to parse JSON for column '{}': {}", columnName, e.getMessage());
            return json;
        }
    }
}
