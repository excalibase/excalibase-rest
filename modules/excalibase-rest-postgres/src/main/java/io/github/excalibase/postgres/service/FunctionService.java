package io.github.excalibase.postgres.service;

import io.github.excalibase.cache.TTLCache;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ComputedFieldFunction;
import io.github.excalibase.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for handling PostgreSQL functions:
 * - Computed fields (functions with table row parameters)
 * - RPC (Remote Procedure Call) for arbitrary functions
 *
 * Supports auto-discovery, auto-embedding, and proven execution patterns.
 */
@Service
public class FunctionService implements io.github.excalibase.service.IFunctionService {
    private static final Logger log = LoggerFactory.getLogger(FunctionService.class);

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final DatabaseSchemaService schemaService;

    // Cache for computed field results per request
    private final ThreadLocal<Map<String, Object>> computedFieldCache = ThreadLocal.withInitial(HashMap::new);

    // Cache for computed field metadata (schema -> table -> functions)
    private final Map<String, Map<String, List<ComputedFieldFunction>>> computedFieldsCache = new ConcurrentHashMap<>();

    // Cache for RPC function metadata (schema.functionName -> metadata)
    private final TTLCache<String, RpcFunctionMetadata> rpcFunctionCache;

    @Value("${app.rpc.function-cache-ttl-seconds:3600}")
    private int rpcFunctionCacheTtlSeconds;

    public FunctionService(JdbcTemplate jdbcTemplate,
                           NamedParameterJdbcTemplate namedJdbcTemplate,
                           DatabaseSchemaService schemaService,
                           @Value("${app.rpc.function-cache-ttl-seconds:3600}") int rpcFunctionCacheTtlSeconds) {
        this.jdbcTemplate = jdbcTemplate;
        this.namedJdbcTemplate = namedJdbcTemplate;
        this.schemaService = schemaService;
        this.rpcFunctionCacheTtlSeconds = rpcFunctionCacheTtlSeconds;
        this.rpcFunctionCache = new TTLCache<>(Duration.ofSeconds(rpcFunctionCacheTtlSeconds));
        log.info("Initialized RPC function cache with TTL: {} seconds", rpcFunctionCacheTtlSeconds);
    }

    /**
     * Simple class to hold function parameter info
     */
    private static class FunctionParameter {
        String name;
        String type;

        FunctionParameter(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }

    /**
     * Cached metadata for RPC functions
     */
    private static class RpcFunctionMetadata {
        String functionName;
        String schema;
        List<FunctionParameter> parameters;
        String returnType;
        Integer argCount;

        RpcFunctionMetadata(String schema, String functionName, List<FunctionParameter> parameters,
                            String returnType, Integer argCount) {
            this.schema = schema;
            this.functionName = functionName;
            this.parameters = parameters;
            this.returnType = returnType;
            this.argCount = argCount;
        }
    }

    /**
     * Parse function signature to extract parameter names and types.
     *
     * Example: "customer_tier text, order_amount numeric"
     * Returns: [{"customer_tier", "text"}, {"order_amount", "numeric"}]
     */
    private List<FunctionParameter> parseFunctionSignature(String signature) {
        List<FunctionParameter> params = new ArrayList<>();

        if (signature == null || signature.trim().isEmpty()) {
            return params;
        }

        // Split by comma to get individual parameters
        String[] paramStrings = signature.split(",");
        for (String paramString : paramStrings) {
            String trimmed = paramString.trim();
            if (trimmed.isEmpty()) continue;

            // Split by whitespace to separate name and type
            String[] parts = trimmed.split("\\s+", 2);
            if (parts.length >= 2) {
                String paramName = parts[0];
                String paramType = parts[1];
                params.add(new FunctionParameter(paramName, paramType));
            }
        }

        return params;
    }

    /**
     * Discover computed fields for all tables in the schema.
     * Uses GraphQL's proven pg_proc discovery query.
     *
     * @param schema The database schema to scan
     * @return Map of table name to list of computed field functions
     */
    public Map<String, List<ComputedFieldFunction>> discoverComputedFields(String schema) {
        // Check cache first
        if (computedFieldsCache.containsKey(schema)) {
            return computedFieldsCache.get(schema);
        }

        log.info("Discovering computed field functions in schema '{}'", schema);

        // GraphQL's proven discovery SQL - finds functions with exactly 1 table parameter
        String sql = """
            SELECT
              p.proname AS function_name,
              t.typname AS return_type,
              param_t.typname AS param_table,
              p.provolatile AS volatility
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            JOIN pg_type t ON p.prorettype = t.oid
            JOIN pg_type param_t ON p.proargtypes[0] = param_t.oid
            WHERE n.nspname = :schema
              AND p.pronargs = 1
              AND param_t.typtype = 'c'
            """;

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("schema", schema);

        Map<String, List<ComputedFieldFunction>> computedFields = new HashMap<>();

        try {
            List<Map<String, Object>> functions = namedJdbcTemplate.queryForList(sql, params);

            for (Map<String, Object> row : functions) {
                String functionName = (String) row.get("function_name");
                String returnType = (String) row.get("return_type");
                String tableName = (String) row.get("param_table");
                String volatility = (String) row.get("volatility");

                // Extract field name from function name
                // Pattern: "tablename_fieldname" -> "fieldname"
                String fieldName = functionName;
                if (functionName.startsWith(tableName + "_")) {
                    fieldName = functionName.substring(tableName.length() + 1);
                }

                ComputedFieldFunction function = new ComputedFieldFunction(
                    functionName,
                    tableName,
                    fieldName,
                    returnType,
                    schema
                );
                function.setVolatility(volatility);

                computedFields.computeIfAbsent(tableName, k -> new ArrayList<>()).add(function);

                log.debug("Discovered computed field: {}.{} -> {} (function: {})",
                    tableName, fieldName, returnType, functionName);
            }

            log.info("Discovered {} computed field functions across {} tables in schema '{}'",
                computedFields.values().stream().mapToInt(List::size).sum(),
                computedFields.size(),
                schema);

            // Cache the results
            computedFieldsCache.put(schema, computedFields);

            return computedFields;

        } catch (Exception e) {
            log.error("Error discovering computed fields: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    /**
     * Add computed fields to a record with auto-embedding.
     *
     * @param tableName The table name
     * @param record    The record to enhance
     * @param schema    The database schema
     * @return Enhanced record with computed fields
     */
    public Map<String, Object> addComputedFields(String tableName,
                                                  Map<String, Object> record,
                                                  String schema) {
        Map<String, List<ComputedFieldFunction>> computedFields = discoverComputedFields(schema);
        List<ComputedFieldFunction> tableComputedFields = computedFields.get(tableName);

        if (tableComputedFields == null || tableComputedFields.isEmpty()) {
            return record;
        }

        Map<String, TableInfo> tableSchema = schemaService.getTableSchema();
        TableInfo tableInfo = tableSchema.get(tableName);
        if (tableInfo == null) {
            return record;
        }

        // Find primary key value
        ColumnInfo pkColumn = tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .findFirst()
            .orElse(null);

        if (pkColumn == null) {
            log.warn("No primary key found for table: {}, cannot compute fields", tableName);
            return record;
        }

        Object pkValue = record.get(pkColumn.getName());
        if (pkValue == null) {
            return record;
        }

        // Compute each field
        for (ComputedFieldFunction function : tableComputedFields) {
            String cacheKey = tableName + ":" + pkValue + ":" + function.getFunctionName();

            // Check cache first
            Object cachedValue = computedFieldCache.get().get(cacheKey);
            if (cachedValue != null) {
                record.put(function.getFieldName(), cachedValue);
                continue;
            }

            try {
                Object value = executeComputedField(tableName, function.getFunctionName(),
                    pkColumn.getName(), pkValue, schema);

                // Cache the result
                computedFieldCache.get().put(cacheKey, value);

                // Add to record
                record.put(function.getFieldName(), value);

                log.debug("Computed field '{}' for {}.{} = {}",
                    function.getFieldName(), tableName, pkValue, value);

            } catch (Exception e) {
                log.error("Error computing field '{}' using function '{}': {}",
                    function.getFieldName(), function.getFunctionName(), e.getMessage());
                record.put(function.getFieldName(), null);
            }
        }

        return record;
    }

    /**
     * Execute a computed field function for a specific record.
     *
     * @param tableName    The table name
     * @param functionName The function to call
     * @param pkColumnName Primary key column name
     * @param pkValue      Primary key value
     * @param schema       Database schema
     * @return Computed field value
     */
    public Object executeComputedField(String tableName,
                                        String functionName,
                                        String pkColumnName,
                                        Object pkValue,
                                        String schema) {
        // GraphQL's execution pattern - pass entire row to function
        String qualifiedFunctionName = io.github.excalibase.util.SqlIdentifier.quoteIdentifier(schema)
                + "." + io.github.excalibase.util.SqlIdentifier.quoteIdentifier(functionName);
        String qualifiedTableName = "\"" + schema + "\".\"" + tableName + "\"";

        String sql = "SELECT " + qualifiedFunctionName + "(row_data.*) as result " +
                    "FROM (SELECT * FROM " + qualifiedTableName + " WHERE \"" +
                    pkColumnName + "\" = ?) row_data";

        log.debug("Executing computed field: {}", sql);

        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{pkValue}, Object.class);
        } catch (Exception e) {
            log.error("Error executing computed field function '{}': {}", functionName, e.getMessage());
            throw new RuntimeException("Failed to execute computed field: " + e.getMessage(), e);
        }
    }

    /**
     * Execute an RPC function call.
     *
     * @param functionName Function name
     * @param parameters   Function parameters as key-value map
     * @param schema       Database schema
     * @return Function result
     */
    public Object executeRpc(String functionName, Map<String, Object> parameters, String schema) {
        // Validate identifiers upfront to prevent SQL injection
        io.github.excalibase.util.SqlIdentifier.quoteIdentifier(functionName);
        io.github.excalibase.util.SqlIdentifier.quoteIdentifier(schema);

        log.debug("Executing RPC function: {}.{}", schema, functionName);

        // Build cache key
        String cacheKey = schema + "." + functionName;

        // Get function metadata from cache or fetch from database
        RpcFunctionMetadata metadata = rpcFunctionCache.computeIfAbsent(cacheKey, key -> {
            log.info("Cache MISS for function: {} - fetching from database", cacheKey);

            // Query function metadata from pg_proc
            String paramInfoSql = """
                SELECT
                  p.proname,
                  p.proargtypes,
                  p.proargnames,
                  p.prorettype,
                  p.pronargs,
                  t.typname as return_type,
                  pg_get_function_arguments(p.oid) as function_signature
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                JOIN pg_type t ON p.prorettype = t.oid
                WHERE n.nspname = :schema
                  AND p.proname = :function_name
                LIMIT 1
                """;

            MapSqlParameterSource paramInfoParams = new MapSqlParameterSource();
            paramInfoParams.addValue("schema", schema);
            paramInfoParams.addValue("function_name", functionName);

            try {
                Map<String, Object> functionInfo = namedJdbcTemplate.queryForMap(paramInfoSql, paramInfoParams);
                String returnType = (String) functionInfo.get("return_type");
                Integer pronargs = ((Number) functionInfo.get("pronargs")).intValue();
                String functionSignature = (String) functionInfo.get("function_signature");

                // Parse function signature to get parameter names AND types
                List<FunctionParameter> functionParams = parseFunctionSignature(functionSignature);

                log.info("Cached function metadata for {}: {} parameters", cacheKey, functionParams.size());

                return new RpcFunctionMetadata(schema, functionName, functionParams, returnType, pronargs);
            } catch (Exception e) {
                log.error("Failed to fetch function metadata for {}: {}", cacheKey, e.getMessage());
                throw new RuntimeException("Function not found: " + functionName, e);
            }
        });

        log.info("Cache HIT for function: {} ({} parameters)", cacheKey, metadata.parameters.size());

        // Build function call SQL
        String qualifiedFunctionName = io.github.excalibase.util.SqlIdentifier.quoteIdentifier(schema)
                + "." + io.github.excalibase.util.SqlIdentifier.quoteIdentifier(functionName);
        List<FunctionParameter> functionParams = metadata.parameters;
        String returnType = metadata.returnType;
        Integer pronargs = metadata.argCount;

        try {
            // Build function call with parameters in correct order
            StringBuilder sql = new StringBuilder("SELECT ");
            sql.append(qualifiedFunctionName).append("(");

            MapSqlParameterSource callParams = new MapSqlParameterSource();

            if (!functionParams.isEmpty() && parameters != null) {
                List<String> paramPlaceholders = new ArrayList<>();
                for (FunctionParameter fp : functionParams) {
                    // Cast to the correct PostgreSQL type
                    paramPlaceholders.add("CAST(:" + fp.name + " AS " + fp.type.toUpperCase() + ")");
                    // Add parameter value in correct order
                    if (parameters.containsKey(fp.name)) {
                        callParams.addValue(fp.name, parameters.get(fp.name));
                    } else {
                        throw new IllegalArgumentException("Missing required parameter: " + fp.name);
                    }
                }
                sql.append(String.join(", ", paramPlaceholders));
            } else if (pronargs > 0) {
                // Extra safety check: function requires parameters but none resolved
                throw new IllegalArgumentException(
                    String.format("Function %s requires %d parameters but none could be resolved",
                        functionName, pronargs));
            }

            sql.append(") as result");

            log.debug("RPC SQL: {}", sql);
            log.debug("RPC Params: {}", callParams.getValues());

            // Check if function returns a table (SETOF)
            if (returnType.startsWith("SETOF") || isCompositeType(returnType, schema)) {
                // Table-returning function - return list
                return namedJdbcTemplate.queryForList(sql.toString(), callParams);
            } else {
                // Scalar function - return single value
                return namedJdbcTemplate.queryForObject(sql.toString(), callParams, Object.class);
            }

        } catch (Exception e) {
            log.error("Error executing RPC function '{}': {}", functionName, e.getMessage(), e);
            throw new RuntimeException("Failed to execute RPC function: " + e.getMessage(), e);
        }
    }

    /**
     * Check if a type is a composite type (table row type)
     */
    private boolean isCompositeType(String typeName, String schema) {
        String sql = """
            SELECT EXISTS(
                SELECT 1 FROM pg_type t
                JOIN pg_namespace n ON t.typnamespace = n.oid
                WHERE t.typname = :type_name
                  AND n.nspname = :schema
                  AND t.typtype = 'c'
            )
            """;

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("type_name", typeName);
        params.addValue("schema", schema);

        try {
            return Boolean.TRUE.equals(namedJdbcTemplate.queryForObject(sql, params, Boolean.class));
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Clear the computed field cache (call at end of request)
     */
    public void clearCache() {
        computedFieldCache.remove();
    }

    /**
     * Invalidate the function metadata cache
     */
    public void invalidateMetadataCache() {
        computedFieldsCache.clear();
        log.info("Function metadata cache invalidated");
    }

    /**
     * Get list of computed fields for a table
     */
    public List<ComputedFieldFunction> getComputedFields(String tableName, String schema) {
        Map<String, List<ComputedFieldFunction>> computedFields = discoverComputedFields(schema);
        return computedFields.getOrDefault(tableName, Collections.emptyList());
    }
}