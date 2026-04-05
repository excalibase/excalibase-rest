package io.github.excalibase.service;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.compiler.IQueryCompiler;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.security.UserContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service encapsulating query execution logic extracted from the controller layer.
 * Handles list queries (raw JSON pass-through and parsed), cursor pagination,
 * and utility methods for JSON counting and cursor encoding.
 */
@Service
public class QueryExecutionService {

    private static final Logger log = LoggerFactory.getLogger(QueryExecutionService.class);

    private final IQueryCompiler queryCompiler;
    private final JdbcTemplate jdbcTemplate;
    private final IResultMapper resultMapper;
    private final FilterService filterService;
    private final RlsQueryExecutor rlsQueryExecutor;

    public QueryExecutionService(IQueryCompiler queryCompiler,
                                  JdbcTemplate jdbcTemplate,
                                  IResultMapper resultMapper,
                                  FilterService filterService,
                                  RlsQueryExecutor rlsQueryExecutor) {
        this.queryCompiler = queryCompiler;
        this.jdbcTemplate = jdbcTemplate;
        this.resultMapper = resultMapper;
        this.filterService = filterService;
        this.rlsQueryExecutor = rlsQueryExecutor;
    }

    /**
     * Execute list query and return raw JSON string directly from PostgreSQL.
     * Avoids Jackson parse-serialize round-trip.
     */
    public String executeListQueryRaw(String table, TableInfo tableInfo,
                                       MultiValueMap<String, String> filters,
                                       String select, String expand,
                                       String orderBy, String orderDirection,
                                       int offset, int limit, boolean includeCount) {
        CompiledQuery compiled = queryCompiler.compile(table, tableInfo, filters,
                select, expand, orderBy, orderDirection, offset, limit, includeCount);

        List<Map<String, Object>> rawRows = jdbcTemplate.queryForList(compiled.sql(), compiled.params());
        Map<String, Object> singleRow = (rawRows != null && !rawRows.isEmpty()) ? rawRows.get(0) : null;

        String bodyJson = "[]";
        long totalCount = -1;
        int recordCount = 0;

        if (singleRow != null) {
            Object bodyObj = singleRow.get("body");
            if (bodyObj != null) {
                bodyJson = bodyObj.toString();
                recordCount = countJsonArrayElements(bodyJson);
            }
            Object totalObj = singleRow.get("total_count");
            if (totalObj instanceof Number) {
                totalCount = ((Number) totalObj).longValue();
            }
        }

        StringBuilder json = new StringBuilder();
        json.append("{\"data\":").append(bodyJson);
        json.append(",\"pagination\":{");
        json.append("\"offset\":").append(offset);
        json.append(",\"limit\":").append(limit);
        json.append(",\"hasMore\":").append(recordCount >= limit);
        if (includeCount && totalCount >= 0) {
            json.append(",\"total\":").append(totalCount);
        }
        json.append("}}");
        return json.toString();
    }

    /**
     * Execute list query and parse results into Map (needed for singular object, Content-Range).
     */
    public Map<String, Object> executeListQuery(String table, TableInfo tableInfo,
                                                  MultiValueMap<String, String> filters,
                                                  String select, String expand,
                                                  String orderBy, String orderDirection,
                                                  int offset, int limit, boolean includeCount) {
        CompiledQuery compiled = queryCompiler.compile(table, tableInfo, filters,
                select, expand, orderBy, orderDirection, offset, limit, includeCount);

        List<Map<String, Object>> rawRows = jdbcTemplate.queryForList(compiled.sql(), compiled.params());
        Map<String, Object> singleRow = (rawRows != null && !rawRows.isEmpty()) ? rawRows.get(0) : null;
        var mapped = resultMapper.mapJsonBody(singleRow, tableInfo);

        Map<String, Object> result = new HashMap<>();
        result.put("data", mapped.records());

        Map<String, Object> pagination = new HashMap<>();
        pagination.put("offset", offset);
        pagination.put("limit", limit);
        pagination.put("hasMore", mapped.records().size() == limit);
        if (includeCount) {
            long total = mapped.totalCount();
            pagination.put("total", total >= 0 ? total : 0L);
        }
        result.put("pagination", pagination);
        return result;
    }

    /**
     * Execute cursor-based pagination query.
     */
    public Map<String, Object> executeCursorQuery(String table, TableInfo tableInfo,
                                                    MultiValueMap<String, String> filters,
                                                    String select, String expand,
                                                    String orderBy, String orderDirection,
                                                    String first, String after,
                                                    String last, String before,
                                                    int maxPageSize) {
        boolean forward = (last == null);
        int limit = 100;
        if (first != null) {
            limit = Math.min(Integer.parseInt(first), maxPageSize);
        } else if (last != null) {
            limit = Math.min(Integer.parseInt(last), maxPageSize);
        }

        CompiledQuery compiled = queryCompiler.compileCursor(table, tableInfo, filters,
                select, expand, orderBy, orderDirection, first, after, last, before);

        List<Map<String, Object>> rawRows = jdbcTemplate.queryForList(compiled.sql(), compiled.params());
        Map<String, Object> singleRow = (rawRows != null && !rawRows.isEmpty()) ? rawRows.get(0) : null;
        var mapped = resultMapper.mapJsonBody(singleRow, tableInfo);
        List<Map<String, Object>> allRecords = mapped.records();
        long totalCount = mapped.totalCount();

        boolean hasMore = allRecords.size() > limit;
        List<Map<String, Object>> records = hasMore ? allRecords.subList(0, limit) : allRecords;

        if (!forward) {
            records = new ArrayList<>(records);
            Collections.reverse(records);
        }

        String effectiveOrderBy = (orderBy != null && !orderBy.isBlank()) ? orderBy
                : tableInfo.getColumns().stream()
                        .filter(ColumnInfo::isPrimaryKey)
                        .map(ColumnInfo::getName)
                        .findFirst().orElse("id");

        List<Map<String, Object>> edges = new ArrayList<>();
        for (Map<String, Object> record : records) {
            Object cursorValue = record.get(effectiveOrderBy);
            String cursor = encodeCursor(cursorValue != null ? cursorValue.toString() : "");
            Map<String, Object> edge = new HashMap<>();
            edge.put("node", record);
            edge.put("cursor", cursor);
            edges.add(edge);
        }

        Map<String, Object> pageInfo = new HashMap<>();
        pageInfo.put("startCursor", edges.isEmpty() ? null : edges.get(0).get("cursor"));
        pageInfo.put("endCursor", edges.isEmpty() ? null : edges.get(edges.size() - 1).get("cursor"));
        if (forward) {
            pageInfo.put("hasNextPage", hasMore);
            pageInfo.put("hasPreviousPage", after != null);
        } else {
            pageInfo.put("hasNextPage", before != null);
            pageInfo.put("hasPreviousPage", hasMore);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("edges", edges);
        result.put("pageInfo", pageInfo);
        result.put("totalCount", totalCount >= 0 ? totalCount : 0L);
        return result;
    }

    // ─── RLS-aware overloads ──────────────────────────────────────────────────

    /**
     * Execute list query with RLS context. Returns raw JSON string.
     */
    public String executeListQueryRaw(UserContext ctx, String table, TableInfo tableInfo,
                                       MultiValueMap<String, String> filters,
                                       String select, String expand,
                                       String orderBy, String orderDirection,
                                       int offset, int limit, boolean includeCount) {
        if (ctx == null) {
            return executeListQueryRaw(table, tableInfo, filters, select, expand,
                    orderBy, orderDirection, offset, limit, includeCount);
        }
        CompiledQuery compiled = queryCompiler.compile(table, tableInfo, filters,
                select, expand, orderBy, orderDirection, offset, limit, includeCount);

        List<Map<String, Object>> rawRows = rlsQueryForList(ctx, compiled);
        Map<String, Object> singleRow = (rawRows != null && !rawRows.isEmpty()) ? rawRows.get(0) : null;

        String bodyJson = "[]";
        long totalCount = -1;
        int recordCount = 0;

        if (singleRow != null) {
            Object bodyObj = singleRow.get("body");
            if (bodyObj != null) {
                bodyJson = bodyObj.toString();
                recordCount = countJsonArrayElements(bodyJson);
            }
            Object totalObj = singleRow.get("total_count");
            if (totalObj instanceof Number) {
                totalCount = ((Number) totalObj).longValue();
            }
        }

        StringBuilder json = new StringBuilder();
        json.append("{\"data\":").append(bodyJson);
        json.append(",\"pagination\":{");
        json.append("\"offset\":").append(offset);
        json.append(",\"limit\":").append(limit);
        json.append(",\"hasMore\":").append(recordCount >= limit);
        if (includeCount && totalCount >= 0) {
            json.append(",\"total\":").append(totalCount);
        }
        json.append("}}");
        return json.toString();
    }

    /**
     * Execute list query with RLS context. Returns parsed Map.
     */
    public Map<String, Object> executeListQuery(UserContext ctx, String table, TableInfo tableInfo,
                                                  MultiValueMap<String, String> filters,
                                                  String select, String expand,
                                                  String orderBy, String orderDirection,
                                                  int offset, int limit, boolean includeCount) {
        if (ctx == null) {
            return executeListQuery(table, tableInfo, filters, select, expand,
                    orderBy, orderDirection, offset, limit, includeCount);
        }
        CompiledQuery compiled = queryCompiler.compile(table, tableInfo, filters,
                select, expand, orderBy, orderDirection, offset, limit, includeCount);

        List<Map<String, Object>> rawRows = rlsQueryForList(ctx, compiled);
        Map<String, Object> singleRow = (rawRows != null && !rawRows.isEmpty()) ? rawRows.get(0) : null;
        var mapped = resultMapper.mapJsonBody(singleRow, tableInfo);

        Map<String, Object> result = new HashMap<>();
        result.put("data", mapped.records());

        Map<String, Object> pagination = new HashMap<>();
        pagination.put("offset", offset);
        pagination.put("limit", limit);
        pagination.put("hasMore", mapped.records().size() == limit);
        if (includeCount) {
            long total = mapped.totalCount();
            pagination.put("total", total >= 0 ? total : 0L);
        }
        result.put("pagination", pagination);
        return result;
    }

    /**
     * Execute cursor-based pagination query with RLS context.
     */
    public Map<String, Object> executeCursorQuery(UserContext ctx, String table, TableInfo tableInfo,
                                                    MultiValueMap<String, String> filters,
                                                    String select, String expand,
                                                    String orderBy, String orderDirection,
                                                    String first, String after,
                                                    String last, String before,
                                                    int maxPageSize) {
        if (ctx == null) {
            return executeCursorQuery(table, tableInfo, filters, select, expand,
                    orderBy, orderDirection, first, after, last, before, maxPageSize);
        }
        boolean forward = (last == null);
        int limit = 100;
        if (first != null) {
            limit = Math.min(Integer.parseInt(first), maxPageSize);
        } else if (last != null) {
            limit = Math.min(Integer.parseInt(last), maxPageSize);
        }

        CompiledQuery compiled = queryCompiler.compileCursor(table, tableInfo, filters,
                select, expand, orderBy, orderDirection, first, after, last, before);

        List<Map<String, Object>> rawRows = rlsQueryForList(ctx, compiled);
        Map<String, Object> singleRow = (rawRows != null && !rawRows.isEmpty()) ? rawRows.get(0) : null;
        var mapped = resultMapper.mapJsonBody(singleRow, tableInfo);
        List<Map<String, Object>> allRecords = mapped.records();
        long totalCount = mapped.totalCount();

        boolean hasMore = allRecords.size() > limit;
        List<Map<String, Object>> records = hasMore ? allRecords.subList(0, limit) : allRecords;

        if (!forward) {
            records = new ArrayList<>(records);
            Collections.reverse(records);
        }

        String effectiveOrderBy = (orderBy != null && !orderBy.isBlank()) ? orderBy
                : tableInfo.getColumns().stream()
                        .filter(ColumnInfo::isPrimaryKey)
                        .map(ColumnInfo::getName)
                        .findFirst().orElse("id");

        List<Map<String, Object>> edges = new ArrayList<>();
        for (Map<String, Object> record : records) {
            Object cursorValue = record.get(effectiveOrderBy);
            String cursor = encodeCursor(cursorValue != null ? cursorValue.toString() : "");
            Map<String, Object> edge = new HashMap<>();
            edge.put("node", record);
            edge.put("cursor", cursor);
            edges.add(edge);
        }

        Map<String, Object> pageInfo = new HashMap<>();
        pageInfo.put("startCursor", edges.isEmpty() ? null : edges.get(0).get("cursor"));
        pageInfo.put("endCursor", edges.isEmpty() ? null : edges.get(edges.size() - 1).get("cursor"));
        if (forward) {
            pageInfo.put("hasNextPage", hasMore);
            pageInfo.put("hasPreviousPage", after != null);
        } else {
            pageInfo.put("hasNextPage", before != null);
            pageInfo.put("hasPreviousPage", hasMore);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("edges", edges);
        result.put("pageInfo", pageInfo);
        result.put("totalCount", totalCount >= 0 ? totalCount : 0L);
        return result;
    }

    /**
     * Execute a compiled query via RLS executor and return results.
     * Wraps checked SQLException as unchecked DataAccessException for consistency with JdbcTemplate.
     */
    private List<Map<String, Object>> rlsQueryForList(UserContext ctx, CompiledQuery compiled) {
        try {
            return rlsQueryExecutor.queryForList(ctx.userId(), ctx.claims(),
                    compiled.sql(), compiled.params());
        } catch (SQLException e) {
            throw new DataAccessException("RLS query failed: " + e.getMessage(), e) {};
        }
    }

    /** Fast count of top-level elements in a JSON array string. */
    public static int countJsonArrayElements(String json) {
        if (json == null || json.length() <= 2) return 0;
        int count = 0;
        int depth = 0;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '{' || c == '[') depth++;
            else if (c == '}' || c == ']') depth--;
            else if (c == ',' && depth == 1) count++;
        }
        return json.charAt(0) == '[' && depth == 0 ? count + 1 : 0;
    }

    /** Encode a cursor value as Base64. */
    public static String encodeCursor(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}
