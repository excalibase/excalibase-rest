package io.github.excalibase.controller;

import io.github.excalibase.exception.ValidationException;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.postgres.service.AggregationService;
import io.github.excalibase.postgres.service.DatabaseSchemaService;
import io.github.excalibase.postgres.service.FunctionService;
import io.github.excalibase.postgres.service.OpenApiService;
import io.github.excalibase.postgres.service.QueryComplexityService;
import io.github.excalibase.postgres.service.RestApiService;
import io.github.excalibase.postgres.service.ValidationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class RestApiController {

    private static final Logger log = LoggerFactory.getLogger(RestApiController.class);

    // Security limits (available for future request body size validation)
    @SuppressWarnings("unused")
    private static final int MAX_REQUEST_BODY_SIZE = 1024 * 1024; // 1MB

    private final RestApiService restApiService;
    private final OpenApiService openApiService;
    private final DatabaseSchemaService schemaService;
    private final QueryComplexityService complexityService;
    private final ValidationService validationService;
    private final AggregationService aggregationService;
    private final FunctionService functionService;

    public RestApiController(RestApiService restApiService, OpenApiService openApiService,
                           DatabaseSchemaService schemaService, QueryComplexityService complexityService,
                           ValidationService validationService, AggregationService aggregationService,
                           FunctionService functionService) {
        this.restApiService = restApiService;
        this.openApiService = openApiService;
        this.schemaService = schemaService;
        this.complexityService = complexityService;
        this.validationService = validationService;
        this.aggregationService = aggregationService;
        this.functionService = functionService;
    }

    @GetMapping("/{table}")
    public ResponseEntity<Map<String, Object>> getRecords(
            @PathVariable String table,
            @RequestParam MultiValueMap<String, String> allParams) {
        try {
            int offset = Integer.parseInt(allParams.getFirst("offset") != null ? allParams.getFirst("offset") : "0");
            int limit = Integer.parseInt(allParams.getFirst("limit") != null ? allParams.getFirst("limit") : "100");
            String orderBy = allParams.getFirst("orderBy");
            String orderDirection = allParams.getFirst("orderDirection") != null ? allParams.getFirst("orderDirection") : "asc";
            String select = allParams.getFirst("select");
            String expand = allParams.getFirst("expand");

            String first = allParams.getFirst("first");
            String after = allParams.getFirst("after");
            String last = allParams.getFirst("last");
            String before = allParams.getFirst("before");

            Map<String, Object> result;


            if (containsAggregateFunction(select)) {
                List<Map<String, Object>> aggregateResults = aggregationService.getInlineAggregates(
                    table, select, allParams, null
                );

                result = new HashMap<>();
                result.put("data", aggregateResults);
                return ResponseEntity.ok(result);
            }

            if (first != null || after != null || last != null || before != null) {
                result = restApiService.getRecordsWithCursor(table, allParams, first, after, last, before, orderBy, orderDirection, select, expand);
            } else {
                result = restApiService.getRecords(table, allParams, offset, limit, orderBy, orderDirection, select, expand);
            }
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    /**
     * Check if select parameter contains aggregate functions
     * Examples: count(), amount.sum(), total.avg()
     */
    private boolean containsAggregateFunction(String select) {
        if (select == null || select.trim().isEmpty()) {
            return false;
        }

        return select.contains("count()") ||
               select.contains(".sum()") ||
               select.contains(".avg()") ||
               select.contains(".min()") ||
               select.contains(".max()");
    }

    @GetMapping("/{table}/{id}")
    public ResponseEntity<Map<String, Object>> getRecord(
            @PathVariable String table,
            @PathVariable String id,
            @RequestParam(required = false) String select,
            @RequestParam(required = false) String expand) {

        try {
            Map<String, Object> result = restApiService.getRecord(table, id, select, expand);
            if (result == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Record not found"));
            }
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "Table not found: " + table));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // POST /api/v1/{table} - Create a new record (single or bulk) with optional upsert support
    @PostMapping("/{table}")
    public ResponseEntity<?> createRecord(
            @PathVariable String table,
            @RequestBody Object data,
            @RequestParam(required = false) String prefer) {

        try {
            boolean isUpsert = "resolution=merge-duplicates".equals(prefer);

            if (data instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> bulkData = (List<Map<String, Object>>) data;

                if (bulkData.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body cannot be empty"));
                }

                List<Map<String, Object>> results;
                if (isUpsert) {
                    results = restApiService.upsertBulkRecords(table, bulkData);
                } else {
                    results = restApiService.createBulkRecords(table, bulkData);
                }
                return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("data", results, "count", results.size()));
            } else if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> singleData = (Map<String, Object>) data;

                if (singleData.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body cannot be empty"));
                }

                Map<String, Object> result;
                if (isUpsert) {
                    result = restApiService.upsertRecord(table, singleData);
                } else {
                    result = restApiService.createRecord(table, singleData);
                }

                if (result == null) {
                    // This can happen with upsert + DO NOTHING
                    return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
                }

                return ResponseEntity.status(HttpStatus.CREATED).body(result);
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Request body must be an object or array"));
            }
        } catch (ValidationException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // PUT /api/v1/{table}/{id} - Update a record (full update)
    // PUT /api/v1/{table} - Bulk update records (array input or query filters)
    @PutMapping({"/{table}/{id}", "/{table}"})
    public ResponseEntity<?> updateRecord(
            @PathVariable String table,
            @PathVariable(required = false) String id,
            @RequestParam MultiValueMap<String, String> allParams,
            @RequestBody Object data) {

        try {
            if (data == null) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Request body cannot be empty"));
            }

            // Check if it's bulk update (array) or single/bulk update (object)
            if (data instanceof List) {
                // Bulk update - array of update objects with id and data
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> bulkUpdates = (List<Map<String, Object>>) data;

                if (bulkUpdates.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body cannot be empty"));
                }

                List<Map<String, Object>> results = restApiService.updateBulkRecords(table, bulkUpdates);
                return ResponseEntity.ok(Map.of("data", results, "count", results.size()));
            } else if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> mapData = (Map<String, Object>) data;

                if (mapData.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body cannot be empty"));
                }

                // Check if we have an ID in path for single update
                if (id != null && !id.trim().isEmpty()) {
                    // Single update
                    Map<String, Object> result = restApiService.updateRecord(table, id, mapData, false);
                    if (result == null) {
                        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                            .body(Map.of("error", "Record not found"));
                    }
                    return ResponseEntity.ok(result);
                } else {
                    // Check if we have query filters for bulk update
                    MultiValueMap<String, String> filters = new org.springframework.util.LinkedMultiValueMap<>();

                    if (allParams != null) {
                        for (Map.Entry<String, List<String>> entry : allParams.entrySet()) {
                            String key = entry.getKey();
                            // Filter out control parameters
                            if (!key.equals("offset") && !key.equals("limit") && !key.equals("orderBy") &&
                                !key.equals("orderDirection") && !key.equals("select") && !key.equals("order") &&
                                !key.equals("expand") && !key.equals("first") && !key.equals("after") &&
                                !key.equals("last") && !key.equals("before")) {
                                filters.put(key, entry.getValue());
                            }
                        }
                    }

                    if (!filters.isEmpty()) {
                        // Bulk update: PUT /table?column=value with update data in body
                        Map<String, Object> result = restApiService.updateRecordsByFilters(table, filters, mapData);
                        return ResponseEntity.ok(result);
                    } else {
                        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                            .body(Map.of("error", "Either ID in path, array of update objects, or query filters required"));
                    }
                }
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Request body must be an object or array"));
            }
        } catch (ValidationException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // PATCH /api/v1/{table}/{id} - Update a record (partial update)
    @PatchMapping("/{table}/{id}")
    public ResponseEntity<Map<String, Object>> patchRecord(
            @PathVariable String table,
            @PathVariable String id,
            @RequestBody Map<String, Object> data) {

        try {
            // Basic security validation
            if (data == null || data.isEmpty()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Request body cannot be empty"));
            }

            Map<String, Object> result = restApiService.updateRecord(table, id, data, true);
            if (result == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Record not found"));
            }
            return ResponseEntity.ok(result);
        } catch (ValidationException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // DELETE /api/v1/{table}/{id} - Delete a specific record
    // DELETE /api/v1/{table} - Delete records by query filters (horizontal filtering)
    @DeleteMapping({"/{table}/{id}", "/{table}"})
    public ResponseEntity<Map<String, Object>> deleteRecord(
            @PathVariable String table,
            @PathVariable(required = false) String id,
            @RequestParam MultiValueMap<String, String> allParams) {

        try {
            if (id != null) {
                // Single record deletion by ID
                boolean deleted = restApiService.deleteRecord(table, id);
                if (!deleted) {
                    return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "Record not found"));
                }
                return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
            } else {
                // Horizontal filtering: DELETE /table?column=value
                // Filter out control parameters to get only filter parameters (same as in RestApiService)
                MultiValueMap<String, String> filters = new org.springframework.util.LinkedMultiValueMap<>();

                if (allParams != null) {
                    for (Map.Entry<String, List<String>> entry : allParams.entrySet()) {
                        String key = entry.getKey();
                        // Use same control parameters list as RestApiService.CONTROL_PARAMETERS
                        if (!key.equals("offset") && !key.equals("limit") && !key.equals("orderBy") &&
                            !key.equals("orderDirection") && !key.equals("select") && !key.equals("order") &&
                            !key.equals("expand") && !key.equals("first") && !key.equals("after") &&
                            !key.equals("last") && !key.equals("before")) {
                            filters.put(key, entry.getValue());
                        }
                    }
                }

                if (filters.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Query filters required for horizontal delete. Use DELETE /{table}?column=value"));
                }

                Map<String, Object> result = restApiService.deleteRecordsByFilters(table, filters);
                return ResponseEntity.ok(result);
            }
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // GET /api/v1 - Get schema information (list all available tables)
    @GetMapping("")
    public ResponseEntity<Map<String, Object>> getSchema() {
        try {
            Map<String, TableInfo> schema = schemaService.getTableSchema();
            return ResponseEntity.ok(Map.of("tables", schema.keySet()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // GET /api/v1/{table}/schema - Get schema information for a specific table
    @GetMapping("/{table}/schema")
    public ResponseEntity<Map<String, Object>> getTableSchema(@PathVariable String table) {
        try {
            Map<String, TableInfo> schema = schemaService.getTableSchema();
            TableInfo tableInfo = schema.get(table);
            if (tableInfo == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Table not found: " + table));
            }

            // Enhanced schema with PostgreSQL type information
            Map<String, Object> enhancedTableInfo = enhanceTableSchemaInfo(tableInfo);
            return ResponseEntity.ok(Map.of("table", enhancedTableInfo));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // GET /api/v1/types/{typeName} - Get information about a PostgreSQL custom type
    @GetMapping("/types/{typeName}")
    public ResponseEntity<Map<String, Object>> getCustomTypeInfo(@PathVariable String typeName) {
        try {
            Map<String, Object> typeInfo = new HashMap<>();

            // Check if it's an enum type
            List<String> enumValues = schemaService.getEnumValues(typeName);
            if (!enumValues.isEmpty()) {
                typeInfo.put("type", "enum");
                typeInfo.put("name", typeName);
                typeInfo.put("values", enumValues);
                return ResponseEntity.ok(typeInfo);
            }

            // Check if it's a composite type
            Map<String, String> compositeDefinition = schemaService.getCompositeTypeDefinition(typeName);
            if (!compositeDefinition.isEmpty()) {
                typeInfo.put("type", "composite");
                typeInfo.put("name", typeName);
                typeInfo.put("fields", compositeDefinition);
                return ResponseEntity.ok(typeInfo);
            }

            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "Custom type not found: " + typeName));

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // GET /api/v1/openapi.json - Get OpenAPI specification in JSON format
    @GetMapping(value = "/openapi.json", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> getOpenApiJson() {
        try {
            Map<String, Object> openApiSpec = openApiService.generateOpenApiSpec();
            return ResponseEntity.ok(openApiSpec);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to generate OpenAPI specification: " + e.getMessage()));
        }
    }

    // GET /api/v1/openapi.yaml - Get OpenAPI specification in YAML format
    @GetMapping(value = "/openapi.yaml", produces = "application/yaml")
    public ResponseEntity<String> getOpenApiYaml() {
        try {
            Map<String, Object> openApiSpec = openApiService.generateOpenApiSpec();
            String yamlContent = convertToYaml(openApiSpec);
            return ResponseEntity.ok(yamlContent);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("error: Failed to generate OpenAPI specification: " + e.getMessage());
        }
    }

    // GET /api/v1/docs - Redirect to Swagger UI (if available) or return API docs info
    @GetMapping("/docs")
    public ResponseEntity<Map<String, Object>> getApiDocs() {
        Map<String, Object> docsInfo = Map.of(
            "title", "Excalibase REST API Documentation",
            "description", "Interactive API documentation for Excalibase REST",
            "openapi_json", "/api/v1/openapi.json",
            "openapi_yaml", "/api/v1/openapi.yaml",
            "swagger_ui", "https://swagger.io/tools/swagger-ui/ (Use openapi.json URL)",
            "postman_collection", "Import openapi.json into Postman",
            "insomnia", "Import openapi.json into Insomnia",
            "note", "Copy the openapi.json URL into any OpenAPI-compatible tool for interactive documentation"
        );
        return ResponseEntity.ok(docsInfo);
    }

    // GET /api/v1/complexity/limits - Get current query complexity limits
    @GetMapping("/complexity/limits")
    public ResponseEntity<Map<String, Object>> getComplexityLimits() {
        try {
            Map<String, Object> limits = complexityService.getComplexityLimits();
            return ResponseEntity.ok(limits);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // POST /api/v1/complexity/analyze - Analyze query complexity without executing
    @PostMapping("/complexity/analyze")
    public ResponseEntity<Map<String, Object>> analyzeQueryComplexity(
            @RequestBody Map<String, Object> queryRequest) {
        try {
            String tableName = (String) queryRequest.get("table");
            if (tableName == null || tableName.trim().isEmpty()) {
                return ResponseEntity.badRequest()
                    .body(Map.of("error", "Table name is required"));
            }

            // Extract parameters from request
            @SuppressWarnings("unchecked")
            Map<String, Object> params = (Map<String, Object>) queryRequest.getOrDefault("params", Map.of());
            int limit = (Integer) queryRequest.getOrDefault("limit", 100);
            String expand = (String) queryRequest.getOrDefault("expand", null);

            // Convert params to MultiValueMap (simplified version)
            org.springframework.util.MultiValueMap<String, String> multiValueParams = new org.springframework.util.LinkedMultiValueMap<>();
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                String value = entry.getValue() != null ? entry.getValue().toString() : "";
                multiValueParams.add(entry.getKey(), value);
            }

            // Analyze complexity
            QueryComplexityService.QueryAnalysis analysis = complexityService.analyzeQuery(tableName, multiValueParams, limit, expand);

            Map<String, Object> result = new HashMap<>();
            result.put("analysis", Map.of(
                "complexityScore", analysis.complexityScore,
                "depth", analysis.depth,
                "breadth", analysis.breadth
            ));
            result.put("limits", complexityService.getComplexityLimits());
            result.put("valid", analysis.complexityScore <= (Integer) complexityService.getComplexityLimits().get("maxComplexityScore"));

            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }




    // Simple YAML converter (basic implementation)
    private String convertToYaml(Map<String, Object> map) {
        StringBuilder yaml = new StringBuilder();
        convertMapToYaml(map, yaml, 0);
        return yaml.toString();
    }

    @SuppressWarnings("unchecked")
    private void convertMapToYaml(Map<String, Object> map, StringBuilder yaml, int indent) {
        String indentStr = "  ".repeat(indent);

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            yaml.append(indentStr).append(entry.getKey()).append(":");

            Object value = entry.getValue();
            if (value instanceof Map) {
                yaml.append("\n");
                convertMapToYaml((Map<String, Object>) value, yaml, indent + 1);
            } else if (value instanceof List) {
                yaml.append("\n");
                convertListToYaml((List<Object>) value, yaml, indent + 1);
            } else {
                yaml.append(" ").append(formatYamlValue(value)).append("\n");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void convertListToYaml(List<Object> list, StringBuilder yaml, int indent) {
        String indentStr = "  ".repeat(indent);

        for (Object item : list) {
            yaml.append(indentStr).append("-");

            if (item instanceof Map) {
                yaml.append("\n");
                convertMapToYaml((Map<String, Object>) item, yaml, indent + 1);
            } else if (item instanceof List) {
                yaml.append("\n");
                convertListToYaml((List<Object>) item, yaml, indent + 1);
            } else {
                yaml.append(" ").append(formatYamlValue(item)).append("\n");
            }
        }
    }

    private String formatYamlValue(Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            String str = (String) value;
            // Escape strings that contain special characters
            if (str.contains(":") || str.contains("#") || str.contains("'") || str.contains("\"") ||
                str.contains("\n") || str.contains("[") || str.contains("]") || str.contains("{") || str.contains("}")) {
                return "\"" + str.replace("\"", "\\\"") + "\"";
            }
            return str;
        } else {
            return value.toString();
        }
    }

    /**
     * Enhance table schema information with PostgreSQL type details
     */
    private Map<String, Object> enhanceTableSchemaInfo(TableInfo tableInfo) {
        Map<String, Object> enhanced = new HashMap<>();
        enhanced.put("name", tableInfo.getName());
        enhanced.put("isView", tableInfo.isView());
        enhanced.put("columns", enhanceColumnSchemaInfo(tableInfo.getColumns()));
        enhanced.put("foreignKeys", tableInfo.getForeignKeys());
        return enhanced;
    }





    /**
     * Enhance column schema information with PostgreSQL type details
     */
    private List<Map<String, Object>> enhanceColumnSchemaInfo(List<io.github.excalibase.model.ColumnInfo> columns) {
        return columns.stream().map(column -> {
            Map<String, Object> columnInfo = new HashMap<>();
            columnInfo.put("name", column.getName());
            columnInfo.put("type", column.getType());
            columnInfo.put("isPrimaryKey", column.isPrimaryKey());
            columnInfo.put("isNullable", column.isNullable());

            // Add enhanced type information for PostgreSQL custom types
            String type = column.getType();
            if (type.startsWith("postgres_enum:")) {
                String enumTypeName = type.substring("postgres_enum:".length());
                List<String> enumValues = schemaService.getEnumValues(enumTypeName);
                columnInfo.put("enumValues", enumValues);
                columnInfo.put("baseType", "enum");
            } else if (type.startsWith("postgres_composite:")) {
                String compositeTypeName = type.substring("postgres_composite:".length());
                Map<String, String> compositeDefinition = schemaService.getCompositeTypeDefinition(compositeTypeName);
                columnInfo.put("compositeFields", compositeDefinition);
                columnInfo.put("baseType", "composite");
            } else if (type.equals("inet") || type.equals("cidr")) {
                columnInfo.put("baseType", "network");
                columnInfo.put("format", type.equals("inet") ? "IPv4/IPv6 address" : "IPv4/IPv6 network with CIDR");
            } else if (type.equals("macaddr") || type.equals("macaddr8")) {
                columnInfo.put("baseType", "mac_address");
                columnInfo.put("format", type.equals("macaddr") ? "6-byte MAC address" : "8-byte MAC address");
            }

            return columnInfo;
        }).toList();
    }

    // ========== Cache Management Endpoints ==========

    /**
     * POST /api/v1/admin/cache/invalidate - Invalidate all caches
     *
     * Clears both schema cache and permission cache.
     * Useful when database permissions or schema changes are made.
     */
    @PostMapping("/admin/cache/invalidate")
    public ResponseEntity<Map<String, Object>> invalidateCaches() {
        try {
            validationService.invalidatePermissionCache();
            schemaService.invalidateSchemaCache();

            return ResponseEntity.ok(Map.of(
                "message", "All caches invalidated successfully",
                "caches", List.of("permissions", "schema")
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to invalidate caches: " + e.getMessage()));
        }
    }

    /**
     * GET /api/v1/admin/cache/stats - Get cache statistics
     *
     * Returns statistics for all caches including:
     * - Permission cache (hit/miss rates, entry counts)
     * - Schema cache (entry counts, TTL info)
     */
    @GetMapping("/admin/cache/stats")
    public ResponseEntity<Map<String, Object>> getCacheStats() {
        try {
            Map<String, Object> permissionCacheStats = validationService.getPermissionCacheStats();
            Map<String, Object> schemaCacheStats = schemaService.getSchemaCacheStats();

            return ResponseEntity.ok(Map.of(
                "permissionCache", permissionCacheStats,
                "schemaCache", schemaCacheStats
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to get cache stats: " + e.getMessage()));
        }
    }

    /**
     * POST /api/v1/admin/cache/permissions/invalidate - Invalidate permission cache only
     *
     * Clears cached permission checks. Use when database permissions change.
     */
    @PostMapping("/admin/cache/permissions/invalidate")
    public ResponseEntity<Map<String, Object>> invalidatePermissionCache() {
        try {
            validationService.invalidatePermissionCache();

            return ResponseEntity.ok(Map.of(
                "message", "Permission cache invalidated successfully"
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to invalidate permission cache: " + e.getMessage()));
        }
    }

    /**
     * POST /api/v1/admin/cache/schema/invalidate - Invalidate schema cache only
     *
     * Clears cached schema metadata. Use when database schema changes.
     */
    @PostMapping("/admin/cache/schema/invalidate")
    public ResponseEntity<Map<String, Object>> invalidateSchemaCache() {
        try {
            schemaService.invalidateSchemaCache();

            return ResponseEntity.ok(Map.of(
                "message", "Schema cache invalidated successfully"
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to invalidate schema cache: " + e.getMessage()));
        }
    }

    // ==================== AGGREGATION ENDPOINTS ====================

    /**
     * GET /api/v1/{table}/aggregate - Get aggregates for a table (GraphQL style)
     *
     * Supports: count, sum, avg, min, max with filters
     * Returns structured nested format.
     *
     * Examples:
     * - GET /orders/aggregate?status=eq.completed
     * - GET /orders/aggregate?status=eq.completed&amp;functions=count,sum&amp;columns=amount,tax
     *
     * Response format:
     * {
     *   "count": 150,
     *   "sum": {"amount": 45000.50, "tax": 6750.08},
     *   "avg": {"amount": 300.00},
     *   "min": {"amount": 10.00},
     *   "max": {"amount": 5000.00}
     * }
     */
    @GetMapping("/{table}/aggregate")
    public ResponseEntity<Map<String, Object>> getAggregates(
            @PathVariable String table,
            @RequestParam MultiValueMap<String, String> allParams) {
        try {
            // Extract aggregate-specific parameters
            String functionsParam = allParams.getFirst("functions");
            String columnsParam = allParams.getFirst("columns");

            List<String> functions = functionsParam != null ?
                List.of(functionsParam.split(",")) : null;
            List<String> columns = columnsParam != null ?
                List.of(columnsParam.split(",")) : null;

            Map<String, Object> result = aggregationService.getAggregates(
                table, allParams, functions, columns
            );

            return ResponseEntity.ok(result);

        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to compute aggregates: " + e.getMessage()));
        }
    }

    // ==================== RPC ENDPOINTS ====================

    /**
     * POST /api/v1/rpc/{function} - Call a PostgreSQL function
     *
     * Execute any database function with JSON parameters.
     *
     * Examples:
     * - POST /rpc/calculate_tax {"amount": 100, "rate": 0.15}
     * - POST /rpc/get_user_stats {"user_id": 123}
     *
     * Response:
     * - Scalar functions: {"result": value}
     * - Table-returning: [{"col1": val1}, {"col2": val2}]
     */
    @PostMapping("/rpc/{function}")
    public ResponseEntity<Object> callFunctionPost(
            @PathVariable String function,
            @RequestBody(required = false) Map<String, Object> parameters) {
        log.info("RPC endpoint called: function={}, parameters={}", function, parameters);
        try {
            // Get schema from configuration (could be made configurable)
            String schema = "public"; // TODO: get from config

            Object result = functionService.executeRpc(function, parameters, schema);

            // Wrap scalar results in a "result" field
            if (result instanceof List) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.ok(Map.of("result", result != null ? result : "null"));
            }

        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to execute function: " + e.getMessage()));
        }
    }

    /**
     * GET /api/v1/rpc/{function} - Call a read-only PostgreSQL function
     *
     * For read-only functions, supports GET with query parameters.
     *
     * Examples:
     * - GET /rpc/get_user_stats?user_id=123
     * - GET /rpc/calculate_tax?amount=100&amp;rate=0.15
     */
    @GetMapping("/rpc/{function}")
    public ResponseEntity<Object> callFunctionGet(
            @PathVariable String function,
            @RequestParam MultiValueMap<String, String> allParams) {
        try {
            // Convert query parameters to Map
            Map<String, Object> parameters = new HashMap<>();
            allParams.forEach((key, values) -> {
                if (values != null && !values.isEmpty()) {
                    parameters.put(key, values.get(0));
                }
            });

            String schema = "public"; // TODO: get from config

            Object result = functionService.executeRpc(function, parameters, schema);

            // Wrap scalar results
            if (result instanceof List) {
                return ResponseEntity.ok(result);
            } else {
                return ResponseEntity.ok(Map.of("result", result != null ? result : "null"));
            }

        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to execute function: " + e.getMessage()));
        }
    }

    // ==================== COMPUTED FIELDS ENDPOINTS ====================

    /**
     * GET /api/v1/{table}/functions - List computed fields for a table
     *
     * Returns all auto-discovered computed field functions for the table.
     *
     * Response: [
     *   {"functionName": "customer_full_name", "fieldName": "full_name", "returnType": "text"},
     *   {"functionName": "customer_age", "fieldName": "age", "returnType": "integer"}
     * ]
     */
    @GetMapping("/{table}/functions")
    public ResponseEntity<Object> getComputedFields(@PathVariable String table) {
        try {
            String schema = "public"; // TODO: get from config

            var functions = functionService.getComputedFields(table, schema);

            return ResponseEntity.ok(functions);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to get computed fields: " + e.getMessage()));
        }
    }

    /**
     * POST /api/v1/admin/cache/functions/invalidate - Invalidate function metadata cache
     *
     * Clears cached function discovery results. Use when functions are added/removed.
     */
    @PostMapping("/admin/cache/functions/invalidate")
    public ResponseEntity<Map<String, Object>> invalidateFunctionCache() {
        try {
            functionService.invalidateMetadataCache();

            return ResponseEntity.ok(Map.of(
                "message", "Function cache invalidated successfully"
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Failed to invalidate function cache: " + e.getMessage()));
        }
    }
}