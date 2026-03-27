package io.github.excalibase.postgres.compiler;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.compiler.ICommandCompiler;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.service.TypeConversionService;
import io.github.excalibase.util.SqlIdentifier;
import static io.github.excalibase.util.SqlIdentifier.quoteIdentifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Postgres implementation of {@link ICommandCompiler}.
 *
 * <p>Compiles INSERT / UPDATE / DELETE / UPSERT commands into parameterised SQL statements
 * with {@code RETURNING *} so the controller can echo the affected record back to the client.
 *
 * <p>All column and table identifiers are double-quoted to prevent SQL injection and to
 * handle identifiers that contain reserved words or mixed case.
 */
@Service
public class PostgresCommandCompiler implements ICommandCompiler {

    private final TypeConversionService typeConversionService;

    public PostgresCommandCompiler(TypeConversionService typeConversionService) {
        this.typeConversionService = typeConversionService;
    }

    // ─── ICommandCompiler ─────────────────────────────────────────────────────

    @Override
    public CompiledQuery insert(String table, TableInfo info, Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Insert data cannot be null or empty");
        }

        List<String> columns = new ArrayList<>(data.keySet());
        List<Object> params = new ArrayList<>();

        for (String col : columns) {
            params.add(typeConversionService.convertValueToColumnType(col, data.get(col), info));
        }

        String columnList = columns.stream().map(SqlIdentifier::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = columns.stream()
                .map(col -> typeConversionService.buildPlaceholderWithCast(col, info))
                .collect(Collectors.joining(", "));

        String sql = "INSERT INTO " + quoteIdentifier(table) + " (" + columnList + ") VALUES (" + placeholders + ") RETURNING *";
        return new CompiledQuery(sql, params.toArray(), false, List.of());
    }

    @Override
    public CompiledQuery bulkInsert(String table, TableInfo info, List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("Bulk insert data cannot be null or empty");
        }

        // Collect all column names across all rows (preserving insertion order)
        Set<String> allColumns = new java.util.LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            allColumns.addAll(row.keySet());
        }
        List<String> columns = new ArrayList<>(allColumns);

        String columnList = columns.stream().map(SqlIdentifier::quoteIdentifier).collect(Collectors.joining(", "));

        // Build placeholder row template using the first row's types (all rows assumed same structure)
        String placeholderRow = "(" + columns.stream()
                .map(col -> typeConversionService.buildPlaceholderWithCast(col, info))
                .collect(Collectors.joining(", ")) + ")";

        List<String> valueGroups = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        for (Map<String, Object> row : rows) {
            valueGroups.add(placeholderRow);
            for (String col : columns) {
                Object value = row.getOrDefault(col, null);
                params.add(value != null
                        ? typeConversionService.convertValueToColumnType(col, value, info)
                        : null);
            }
        }

        String sql = "INSERT INTO " + quoteIdentifier(table) + " (" + columnList + ") VALUES "
                + String.join(", ", valueGroups) + " RETURNING *";
        return new CompiledQuery(sql, params.toArray(), false, List.of());
    }

    @Override
    public CompiledQuery update(String table, TableInfo info, String id, Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Update data cannot be null or empty");
        }
        return buildUpdate(table, info, id, data);
    }

    @Override
    public CompiledQuery patch(String table, TableInfo info, String id, Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Patch data cannot be null or empty");
        }
        return buildUpdate(table, info, id, data);
    }

    @Override
    public CompiledQuery delete(String table, TableInfo info, String id) {
        List<ColumnInfo> pkColumns = resolvePkColumns(info);
        String[] keyValues = splitCompositeKey(id, pkColumns.size());

        List<Object> params = new ArrayList<>();
        List<String> conditions = new ArrayList<>();
        for (int i = 0; i < pkColumns.size(); i++) {
            String col = pkColumns.get(i).getName();
            params.add(typeConversionService.convertValueToColumnType(col, keyValues[i], info));
            conditions.add(quoteIdentifier(col) + " = " + typeConversionService.buildPlaceholderWithCast(col, info));
        }

        String sql = "DELETE FROM " + quoteIdentifier(table) + " WHERE "
                + String.join(" AND ", conditions) + " RETURNING *";
        return new CompiledQuery(sql, params.toArray(), false, List.of());
    }

    @Override
    public CompiledQuery upsert(String table, TableInfo info, Map<String, Object> data, List<String> conflictColumns) {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Upsert data cannot be null or empty");
        }
        if (conflictColumns == null || conflictColumns.isEmpty()) {
            throw new IllegalArgumentException("Conflict columns are required for upsert");
        }

        List<String> columns = new ArrayList<>(data.keySet());
        List<Object> params = new ArrayList<>();

        for (String col : columns) {
            params.add(typeConversionService.convertValueToColumnType(col, data.get(col), info));
        }

        String columnList = columns.stream().map(SqlIdentifier::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = columns.stream()
                .map(col -> typeConversionService.buildPlaceholderWithCast(col, info))
                .collect(Collectors.joining(", "));

        Set<String> conflictSet = Set.copyOf(conflictColumns);

        // Update all columns except the conflict columns
        List<String> updateCols = columns.stream()
                .filter(c -> !conflictSet.contains(c))
                .collect(Collectors.toList());

        String updateSet = updateCols.stream()
                .map(col -> quoteIdentifier(col) + " = EXCLUDED." + quoteIdentifier(col))
                .collect(Collectors.joining(", "));

        String conflictList = conflictColumns.stream()
                .map(SqlIdentifier::quoteIdentifier)
                .collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder("INSERT INTO ")
                .append(quoteIdentifier(table))
                .append(" (").append(columnList).append(")")
                .append(" VALUES (").append(placeholders).append(")")
                .append(" ON CONFLICT (").append(conflictList).append(")");

        if (!updateCols.isEmpty()) {
            sql.append(" DO UPDATE SET ").append(updateSet);
        } else {
            sql.append(" DO NOTHING");
        }

        sql.append(" RETURNING *");
        return new CompiledQuery(sql.toString(), params.toArray(), false, List.of());
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    private CompiledQuery buildUpdate(String table, TableInfo info, String id,
                                       Map<String, Object> data) {
        List<ColumnInfo> pkColumns = resolvePkColumns(info);
        String[] keyValues = splitCompositeKey(id, pkColumns.size());

        List<Object> params = new ArrayList<>();

        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String col = entry.getKey();
            params.add(typeConversionService.convertValueToColumnType(col, entry.getValue(), info));
            setClauses.add(quoteIdentifier(col) + " = " + typeConversionService.buildPlaceholderWithCast(col, info));
        }

        // WHERE pk1 = ? AND pk2 = ? ...
        List<String> conditions = new ArrayList<>();
        for (int i = 0; i < pkColumns.size(); i++) {
            String col = pkColumns.get(i).getName();
            params.add(typeConversionService.convertValueToColumnType(col, keyValues[i], info));
            conditions.add(quoteIdentifier(col) + " = " + typeConversionService.buildPlaceholderWithCast(col, info));
        }

        String sql = "UPDATE " + quoteIdentifier(table)
                + " SET " + String.join(", ", setClauses)
                + " WHERE " + String.join(" AND ", conditions)
                + " RETURNING *";

        return new CompiledQuery(sql, params.toArray(), false, List.of());
    }

    private List<ColumnInfo> resolvePkColumns(TableInfo info) {
        List<ColumnInfo> pks = info.getColumns().stream()
                .filter(ColumnInfo::isPrimaryKey)
                .collect(Collectors.toList());
        if (pks.isEmpty()) {
            // Fallback: assume a column named "id"
            return info.getColumns().stream()
                    .filter(c -> c.getName().equalsIgnoreCase("id"))
                    .collect(Collectors.toList());
        }
        return pks;
    }

    /**
     * Split a composite key string (e.g. "1,2") into individual values.
     * If the number of parts does not match expectedCount the raw value is returned as-is
     * in a single-element array (single-PK fallback).
     */
    private static String[] splitCompositeKey(String id, int expectedCount) {
        if (expectedCount <= 1) {
            return new String[]{id};
        }
        String[] parts = id.split(",", -1);
        if (parts.length == expectedCount) {
            return parts;
        }
        // Mismatch — fall back to treating the whole string as a single value
        return new String[]{id};
    }

}
