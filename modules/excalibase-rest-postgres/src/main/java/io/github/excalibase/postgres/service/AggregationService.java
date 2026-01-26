package io.github.excalibase.postgres.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for handling aggregate functions (COUNT, SUM, AVG, MIN, MAX).
 * Supports both PostgREST-style inline aggregates and dedicated aggregate endpoints.
 */
@Service
public class AggregationService {
    private static final Logger log = LoggerFactory.getLogger(AggregationService.class);

    private final JdbcTemplate jdbcTemplate;
    private final DatabaseSchemaService schemaService;
    private final ValidationService validationService;

    // Supported aggregate functions
    private static final Set<String> AGGREGATE_FUNCTIONS = Set.of("count", "sum", "avg", "min", "max");

    // Numeric types that support SUM and AVG
    private static final Set<String> NUMERIC_TYPES = Set.of(
        "integer", "int", "int2", "int4", "int8",
        "smallint", "bigint",
        "numeric", "decimal",
        "real", "float", "float4", "float8",
        "double precision", "money"
    );

    // Comparable types that support MIN and MAX
    private static final Set<String> COMPARABLE_TYPES = Set.of(
        // Numeric types
        "integer", "int", "int2", "int4", "int8",
        "smallint", "bigint",
        "numeric", "decimal",
        "real", "float", "float4", "float8",
        "double precision", "money",
        // Date/time types
        "date", "timestamp", "timestamptz", "timestamp with time zone",
        "timestamp without time zone", "time", "timetz",
        // String types
        "text", "varchar", "character varying", "char", "character", "bpchar"
    );

    public AggregationService(JdbcTemplate jdbcTemplate,
                              DatabaseSchemaService schemaService,
                              ValidationService validationService) {
        this.jdbcTemplate = jdbcTemplate;
        this.schemaService = schemaService;
        this.validationService = validationService;
    }

    /**
     * Get aggregates for a table with filters (dedicated endpoint style).
     * Returns structured nested format like GraphQL.
     *
     * @param tableName The table name
     * @param filters   Filter parameters
     * @param functions Optional list of specific functions to compute (null = all)
     * @param columns   Optional list of specific columns to aggregate (null = all applicable)
     * @return Map with structured aggregate results
     */
    public Map<String, Object> getAggregates(String tableName,
                                             MultiValueMap<String, String> filters,
                                             List<String> functions,
                                             List<String> columns) {
        // Validate permissions
        validationService.validateTablePermission(tableName, "SELECT");

        // Get table info
        Map<String, TableInfo> schema = schemaService.getTableSchema();
        TableInfo tableInfo = schema.get(tableName);
        if (tableInfo == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        // Build WHERE clause
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();
        buildWhereClause(tableInfo, filters, whereClause, params);

        Map<String, Object> result = new HashMap<>();

        // Determine which functions to compute
        Set<String> functionsToCompute = functions != null && !functions.isEmpty()
            ? new HashSet<>(functions)
            : AGGREGATE_FUNCTIONS;

        // COUNT is always available
        if (functionsToCompute.contains("count")) {
            result.put("count", computeCount(tableName, whereClause.toString(), params));
        }

        // Get columns to aggregate
        List<ColumnInfo> columnsToAggregate = getColumnsToAggregate(tableInfo, columns);

        // Compute numeric aggregates (SUM, AVG)
        if (functionsToCompute.contains("sum") || functionsToCompute.contains("avg")) {
            List<ColumnInfo> numericColumns = columnsToAggregate.stream()
                .filter(col -> isNumericType(col.getType()))
                .collect(Collectors.toList());

            if (!numericColumns.isEmpty()) {
                if (functionsToCompute.contains("sum")) {
                    result.put("sum", computeAggregates("SUM", tableName, numericColumns, whereClause.toString(), params));
                }
                if (functionsToCompute.contains("avg")) {
                    result.put("avg", computeAggregates("AVG", tableName, numericColumns, whereClause.toString(), params));
                }
            }
        }

        // Compute comparable aggregates (MIN, MAX)
        if (functionsToCompute.contains("min") || functionsToCompute.contains("max")) {
            List<ColumnInfo> comparableColumns = columnsToAggregate.stream()
                .filter(col -> isComparableType(col.getType()))
                .collect(Collectors.toList());

            if (!comparableColumns.isEmpty()) {
                if (functionsToCompute.contains("min")) {
                    result.put("min", computeAggregates("MIN", tableName, comparableColumns, whereClause.toString(), params));
                }
                if (functionsToCompute.contains("max")) {
                    result.put("max", computeAggregates("MAX", tableName, comparableColumns, whereClause.toString(), params));
                }
            }
        }

        return result;
    }

    /**
     * Parse and execute inline aggregates from select parameter (PostgREST style).
     * Examples: "amount.sum()", "count()", "customer_id,amount.sum()"
     *
     * @param tableName    The table name
     * @param selectParam  The select parameter with aggregate expressions
     * @param filters      Filter parameters
     * @param groupByColumns Columns to group by (auto-detected from non-aggregate columns)
     * @return List of aggregate result rows
     */
    public List<Map<String, Object>> getInlineAggregates(String tableName,
                                                          String selectParam,
                                                          MultiValueMap<String, String> filters,
                                                          List<String> groupByColumns) {
        // Validate permissions
        validationService.validateTablePermission(tableName, "SELECT");

        // Get table info
        Map<String, TableInfo> schema = schemaService.getTableSchema();
        TableInfo tableInfo = schema.get(tableName);
        if (tableInfo == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }

        // Parse select expressions
        List<SelectExpression> expressions = parseSelectExpressions(selectParam, tableInfo);

        // Build SQL query
        String sql = buildInlineAggregateQuery(tableName, expressions, filters, groupByColumns, tableInfo);

        // Build parameters
        List<Object> params = new ArrayList<>();
        StringBuilder whereClause = new StringBuilder();
        buildWhereClause(tableInfo, filters, whereClause, params);

        log.debug("Executing inline aggregate query: {}", sql);

        try {
            return jdbcTemplate.query(sql, params.toArray(), (rs, rowNum) -> {
                Map<String, Object> row = new HashMap<>();
                int columnCount = rs.getMetaData().getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rs.getMetaData().getColumnLabel(i);
                    row.put(columnName, rs.getObject(i));
                }
                return row;
            });
        } catch (Exception e) {
            log.error("Error executing inline aggregate query: {}", e.getMessage());
            throw new RuntimeException("Failed to execute aggregate query: " + e.getMessage(), e);
        }
    }

    /**
     * Parse select expressions like "amount.sum()", "count()", "customer_id"
     */
    private List<SelectExpression> parseSelectExpressions(String selectParam, TableInfo tableInfo) {
        List<SelectExpression> expressions = new ArrayList<>();

        if (selectParam == null || selectParam.trim().isEmpty()) {
            return expressions;
        }

        String[] parts = selectParam.split(",");
        for (String partRaw : parts) {
            final String part = partRaw.trim();

            // Check if it's an aggregate expression (column.function())
            if (part.contains(".") && part.endsWith("()")) {
                String[] aggregateParts = part.split("\\.");
                if (aggregateParts.length == 2) {
                    String columnName = aggregateParts[0];
                    String functionName = aggregateParts[1].replace("()", "");

                    if (AGGREGATE_FUNCTIONS.contains(functionName.toLowerCase())) {
                        expressions.add(new SelectExpression(columnName, functionName.toUpperCase(), true));
                        continue;
                    }
                }
            }

            // Check if it's count()
            if (part.equals("count()")) {
                expressions.add(new SelectExpression(null, "COUNT", true));
                continue;
            }

            // Regular column (for GROUP BY)
            if (tableInfo.getColumns().stream().anyMatch(col -> col.getName().equals(part))) {
                expressions.add(new SelectExpression(part, null, false));
            }
        }

        return expressions;
    }

    /**
     * Build SQL query for inline aggregates
     */
    private String buildInlineAggregateQuery(String tableName,
                                             List<SelectExpression> expressions,
                                             MultiValueMap<String, String> filters,
                                             List<String> groupByColumns,
                                             TableInfo tableInfo) {
        StringBuilder sql = new StringBuilder("SELECT ");

        List<String> selectParts = new ArrayList<>();
        List<String> groupBy = new ArrayList<>();

        for (SelectExpression expr : expressions) {
            if (expr.isAggregate) {
                if (expr.columnName != null) {
                    selectParts.add(String.format("%s(\"%s\") as %s",
                        expr.function, expr.columnName, expr.function.toLowerCase()));
                } else {
                    // count()
                    selectParts.add(String.format("%s(*) as count", expr.function));
                }
            } else {
                // Regular column - add to both SELECT and GROUP BY
                selectParts.add("\"" + expr.columnName + "\"");
                groupBy.add("\"" + expr.columnName + "\"");
            }
        }

        sql.append(String.join(", ", selectParts));
        sql.append(" FROM \"").append(tableName).append("\"");

        // Add WHERE clause
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();
        buildWhereClause(tableInfo, filters, whereClause, params);
        if (whereClause.length() > 0) {
            sql.append(whereClause);
        }

        // Add GROUP BY if needed
        if (!groupBy.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupBy));
        }

        return sql.toString();
    }

    private Integer computeCount(String tableName, String whereClause, List<Object> params) {
        String sql = "SELECT COUNT(*) FROM \"" + tableName + "\"" + whereClause;
        log.debug("Computing count: {}", sql);

        try {
            return jdbcTemplate.queryForObject(sql, params.toArray(), Integer.class);
        } catch (Exception e) {
            log.error("Error computing count: {}", e.getMessage());
            return 0;
        }
    }

    private Map<String, Object> computeAggregates(String function,
                                                   String tableName,
                                                   List<ColumnInfo> columns,
                                                   String whereClause,
                                                   List<Object> params) {
        Map<String, Object> result = new HashMap<>();

        for (ColumnInfo column : columns) {
            String sql = String.format("SELECT %s(\"%s\") as result FROM \"%s\"%s",
                function, column.getName(), tableName, whereClause);

            log.debug("Computing {} for column {}: {}", function, column.getName(), sql);

            try {
                Object value = jdbcTemplate.queryForObject(sql, params.toArray(), Object.class);
                result.put(column.getName(), value);
            } catch (Exception e) {
                log.error("Error computing {} for column {}: {}", function, column.getName(), e.getMessage());
                result.put(column.getName(), null);
            }
        }

        return result;
    }

    private void buildWhereClause(TableInfo tableInfo,
                                   MultiValueMap<String, String> filters,
                                   StringBuilder whereClause,
                                   List<Object> params) {
        // This is a simplified version - in production, integrate with existing filter parsing
        if (filters != null && !filters.isEmpty()) {
            whereClause.append(" WHERE 1=1");
            // TODO: Integrate with existing filter parsing logic from RestApiService
        }
    }

    private List<ColumnInfo> getColumnsToAggregate(TableInfo tableInfo, List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return tableInfo.getColumns();
        }

        return tableInfo.getColumns().stream()
            .filter(col -> columns.contains(col.getName()))
            .collect(Collectors.toList());
    }

    private boolean isNumericType(String type) {
        return NUMERIC_TYPES.contains(type.toLowerCase());
    }

    private boolean isComparableType(String type) {
        return COMPARABLE_TYPES.contains(type.toLowerCase());
    }

    /**
     * Represents a select expression (column or aggregate)
     */
    private static class SelectExpression {
        String columnName;
        String function;
        boolean isAggregate;

        SelectExpression(String columnName, String function, boolean isAggregate) {
            this.columnName = columnName;
            this.function = function;
            this.isAggregate = isAggregate;
        }
    }
}