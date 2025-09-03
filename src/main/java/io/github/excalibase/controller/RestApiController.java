package io.github.excalibase.controller;

import io.github.excalibase.model.TableInfo;
import io.github.excalibase.service.DatabaseSchemaService;
import io.github.excalibase.service.OpenApiService;
import io.github.excalibase.service.QueryComplexityService;
import io.github.excalibase.service.RestApiService;
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
    
    // Security limits (available for future request body size validation)
    @SuppressWarnings("unused")
    private static final int MAX_REQUEST_BODY_SIZE = 1024 * 1024; // 1MB
    
    private final RestApiService restApiService;
    private final OpenApiService openApiService;
    private final DatabaseSchemaService schemaService;
    private final QueryComplexityService complexityService;

    public RestApiController(RestApiService restApiService, OpenApiService openApiService, 
                           DatabaseSchemaService schemaService, QueryComplexityService complexityService) {
        this.restApiService = restApiService;
        this.openApiService = openApiService;
        this.schemaService = schemaService;
        this.complexityService = complexityService;
    }

    // GET /api/v1/{table} - Get all records from a table with optional filtering, pagination, relationships
    @GetMapping("/{table}")
    public ResponseEntity<Map<String, Object>> getRecords(
            @PathVariable String table,
            @RequestParam MultiValueMap<String, String> allParams) {
        
        try {
            // Extract control parameters with defaults
            int offset = Integer.parseInt(allParams.getFirst("offset") != null ? allParams.getFirst("offset") : "0");
            int limit = Integer.parseInt(allParams.getFirst("limit") != null ? allParams.getFirst("limit") : "100");
            String orderBy = allParams.getFirst("orderBy");
            String orderDirection = allParams.getFirst("orderDirection") != null ? allParams.getFirst("orderDirection") : "asc";
            String select = allParams.getFirst("select");
            String expand = allParams.getFirst("expand"); // Relationship expansion
            
            // Cursor-based pagination (GraphQL Connections style)
            String first = allParams.getFirst("first");
            String after = allParams.getFirst("after");
            String last = allParams.getFirst("last");
            String before = allParams.getFirst("before");
            
            Map<String, Object> result;
            
            // Check if using cursor-based pagination
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

    // GET /api/v1/{table}/{id} - Get a specific record by primary key with relationships
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

    // POST /api/v1/{table} - Create a new record (single or bulk)
    @PostMapping("/{table}")
    public ResponseEntity<?> createRecord(
            @PathVariable String table,
            @RequestBody Object data) {
        
        try {
            // Check if it's bulk creation (array) or single creation (object)
            if (data instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> bulkData = (List<Map<String, Object>>) data;
                
                if (bulkData.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body cannot be empty"));
                }
                
                List<Map<String, Object>> results = restApiService.createBulkRecords(table, bulkData);
                return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("data", results, "count", results.size()));
            } else if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> singleData = (Map<String, Object>) data;
                
                if (singleData.isEmpty()) {
                    return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body(Map.of("error", "Request body cannot be empty"));
                }
                
                Map<String, Object> result = restApiService.createRecord(table, singleData);
                return ResponseEntity.status(HttpStatus.CREATED).body(result);
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Request body must be an object or array"));
            }
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // PUT /api/v1/{table}/{id} - Update a record (full update)
    @PutMapping("/{table}/{id}")
    public ResponseEntity<Map<String, Object>> updateRecord(
            @PathVariable String table,
            @PathVariable String id,
            @RequestBody Map<String, Object> data) {
        
        try {
            // Basic security validation
            if (data == null || data.isEmpty()) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Request body cannot be empty"));
            }
            
            Map<String, Object> result = restApiService.updateRecord(table, id, data, false);
            if (result == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Record not found"));
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
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "Internal server error: " + e.getMessage()));
        }
    }

    // DELETE /api/v1/{table}/{id} - Delete a record
    @DeleteMapping("/{table}/{id}")
    public ResponseEntity<Map<String, Object>> deleteRecord(
            @PathVariable String table,
            @PathVariable String id) {
        
        try {
            boolean deleted = restApiService.deleteRecord(table, id);
            if (!deleted) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Record not found"));
            }
            return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
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
}