package io.github.excalibase.postgres.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class OpenApiService implements io.github.excalibase.service.IOpenApiService {

    private final DatabaseSchemaService schemaService;

    public OpenApiService(DatabaseSchemaService schemaService) {
        this.schemaService = schemaService;
    }

    /**
     * Generate OpenAPI 3.0 specification for the REST API
     */
    public Map<String, Object> generateOpenApiSpec() {
        Map<String, Object> spec = new LinkedHashMap<>();
        
        // OpenAPI version and info
        spec.put("openapi", "3.0.3");
        spec.put("info", createInfoSection());
        spec.put("servers", createServersSection());
        spec.put("paths", createPathsSection());
        spec.put("components", createComponentsSection());
        
        return spec;
    }

    private Map<String, Object> createInfoSection() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("title", "Excalibase REST API");
        info.put("description", "Auto-generated REST API for PostgreSQL database with advanced filtering, relationships, and PostgreSQL type support");
        info.put("version", "1.0.0");
        info.put("contact", Map.of(
            "name", "Excalibase Team",
            "url", "https://github.com/excalibase/excalibase-rest"
        ));
        info.put("license", Map.of(
            "name", "Apache 2.0",
            "url", "https://www.apache.org/licenses/LICENSE-2.0"
        ));
        return info;
    }

    private List<Map<String, Object>> createServersSection() {
        List<Map<String, Object>> servers = new ArrayList<>();
        servers.add(Map.of(
            "url", "http://localhost:20000",
            "description", "Development server"
        ));
        return servers;
    }

    private Map<String, Object> createPathsSection() {
        Map<String, Object> paths = new LinkedHashMap<>();
        Map<String, TableInfo> schema = schemaService.getTableSchema();
        
        // Add schema endpoints
        paths.put("/api/v1", createSchemaEndpoint());
        paths.put("/api/v1/openapi.json", createOpenApiJsonEndpoint());
        paths.put("/api/v1/openapi.yaml", createOpenApiYamlEndpoint());
        paths.put("/api/v1/docs", createDocsEndpoint());
        
        // Add table-specific endpoints
        for (String tableName : schema.keySet()) {
            TableInfo tableInfo = schema.get(tableName);
            
            // Collection endpoints: /api/v1/{table}
            paths.put("/api/v1/" + tableName, createTableCollectionEndpoint(tableName, tableInfo));
            
            // Item endpoints: /api/v1/{table}/{id}
            paths.put("/api/v1/" + tableName + "/{id}", createTableItemEndpoint(tableName, tableInfo));
            
            // Schema endpoint: /api/v1/{table}/schema
            paths.put("/api/v1/" + tableName + "/schema", createTableSchemaEndpoint(tableName, tableInfo));
        }
        
        return paths;
    }

    private Map<String, Object> createSchemaEndpoint() {
        Map<String, Object> endpoint = new LinkedHashMap<>();
        
        Map<String, Object> get = new LinkedHashMap<>();
        get.put("summary", "Get database schema");
        get.put("description", "Returns a list of all available tables in the database");
        get.put("tags", List.of("Schema"));
        
        Map<String, Object> responses = new LinkedHashMap<>();
        responses.put("200", Map.of(
            "description", "List of available tables",
            "content", Map.of(
                "application/json", Map.of(
                    "schema", Map.of(
                        "type", "object",
                        "properties", Map.of(
                            "tables", Map.of(
                                "type", "array",
                                "items", Map.of("type", "string")
                            )
                        )
                    )
                )
            )
        ));
        get.put("responses", responses);
        
        endpoint.put("get", get);
        return endpoint;
    }

    private Map<String, Object> createOpenApiJsonEndpoint() {
        Map<String, Object> endpoint = new LinkedHashMap<>();
        
        Map<String, Object> get = new LinkedHashMap<>();
        get.put("summary", "Get OpenAPI specification (JSON)");
        get.put("description", "Returns the OpenAPI 3.0 specification in JSON format");
        get.put("tags", List.of("Documentation"));
        
        Map<String, Object> responses = new LinkedHashMap<>();
        responses.put("200", Map.of(
            "description", "OpenAPI specification",
            "content", Map.of(
                "application/json", Map.of(
                    "schema", Map.of("type", "object")
                )
            )
        ));
        get.put("responses", responses);
        
        endpoint.put("get", get);
        return endpoint;
    }

    private Map<String, Object> createOpenApiYamlEndpoint() {
        Map<String, Object> endpoint = new LinkedHashMap<>();
        
        Map<String, Object> get = new LinkedHashMap<>();
        get.put("summary", "Get OpenAPI specification (YAML)");
        get.put("description", "Returns the OpenAPI 3.0 specification in YAML format");
        get.put("tags", List.of("Documentation"));
        
        Map<String, Object> responses = new LinkedHashMap<>();
        responses.put("200", Map.of(
            "description", "OpenAPI specification in YAML",
            "content", Map.of(
                "application/yaml", Map.of(
                    "schema", Map.of("type", "string")
                )
            )
        ));
        get.put("responses", responses);
        
        endpoint.put("get", get);
        return endpoint;
    }

    private Map<String, Object> createDocsEndpoint() {
        Map<String, Object> endpoint = new LinkedHashMap<>();
        
        Map<String, Object> get = new LinkedHashMap<>();
        get.put("summary", "Get API documentation info");
        get.put("description", "Returns links to interactive API documentation");
        get.put("tags", List.of("Documentation"));
        
        Map<String, Object> responses = new LinkedHashMap<>();
        responses.put("200", Map.of(
            "description", "Documentation information",
            "content", Map.of(
                "application/json", Map.of(
                    "schema", Map.of("type", "object")
                )
            )
        ));
        get.put("responses", responses);
        
        endpoint.put("get", get);
        return endpoint;
    }

    private Map<String, Object> createTableCollectionEndpoint(String tableName, TableInfo tableInfo) {
        Map<String, Object> endpoint = new LinkedHashMap<>();
        
        // GET endpoint for collection
        Map<String, Object> get = new LinkedHashMap<>();
        get.put("summary", "Get " + tableName + " records");
        get.put("description", "Retrieve records from " + tableName + " table with filtering, pagination, and relationship expansion");
        get.put("tags", List.of(capitalizeFirst(tableName)));
        get.put("parameters", createCollectionParameters(tableInfo));
        
        Map<String, Object> getResponses = new LinkedHashMap<>();
        getResponses.put("200", Map.of(
            "description", "List of " + tableName + " records",
            "content", Map.of(
                "application/json", Map.of(
                    "schema", createPaginatedResponseSchema(tableName)
                )
            )
        ));
        getResponses.put("400", createErrorResponse("Bad Request"));
        getResponses.put("500", createErrorResponse("Internal Server Error"));
        get.put("responses", getResponses);
        
        endpoint.put("get", get);
        
        // POST endpoint for creation
        if (!tableInfo.isView()) {
            Map<String, Object> post = new LinkedHashMap<>();
            post.put("summary", "Create " + tableName + " record(s)");
            post.put("description", "Create one or more records in " + tableName + " table");
            post.put("tags", List.of(capitalizeFirst(tableName)));
            post.put("requestBody", Map.of(
                "required", true,
                "content", Map.of(
                    "application/json", Map.of(
                        "schema", Map.of(
                            "oneOf", List.of(
                                Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName) + "Input"),
                                Map.of(
                                    "type", "array",
                                    "items", Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName) + "Input")
                                )
                            )
                        )
                    )
                )
            ));
            
            Map<String, Object> postResponses = new LinkedHashMap<>();
            postResponses.put("201", Map.of(
                "description", "Created " + tableName + " record(s)",
                "content", Map.of(
                    "application/json", Map.of(
                        "schema", Map.of(
                            "oneOf", List.of(
                                Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName)),
                                Map.of(
                                    "type", "object",
                                    "properties", Map.of(
                                        "data", Map.of(
                                            "type", "array",
                                            "items", Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName))
                                        ),
                                        "count", Map.of("type", "integer")
                                    )
                                )
                            )
                        )
                    )
                )
            ));
            postResponses.put("400", createErrorResponse("Bad Request"));
            postResponses.put("500", createErrorResponse("Internal Server Error"));
            post.put("responses", postResponses);
            
            endpoint.put("post", post);
        }
        
        return endpoint;
    }

    private Map<String, Object> createTableItemEndpoint(String tableName, TableInfo tableInfo) {
        Map<String, Object> endpoint = new LinkedHashMap<>();
        
        // GET endpoint for single item
        Map<String, Object> get = new LinkedHashMap<>();
        get.put("summary", "Get " + tableName + " record by ID");
        get.put("description", "Retrieve a single record from " + tableName + " table by its primary key");
        get.put("tags", List.of(capitalizeFirst(tableName)));
        
        List<Map<String, Object>> parameters = new ArrayList<>();
        parameters.add(Map.of(
            "name", "id",
            "in", "path",
            "required", true,
            "description", "Primary key of the " + tableName + " record",
            "schema", Map.of("type", "string")
        ));
        parameters.add(Map.of(
            "name", "select",
            "in", "query",
            "required", false,
            "description", "Comma-separated list of columns to return",
            "schema", Map.of("type", "string"),
            "example", getExampleSelectColumns(tableInfo)
        ));
        parameters.add(Map.of(
            "name", "expand",
            "in", "query",
            "required", false,
            "description", "Comma-separated list of relationships to expand",
            "schema", Map.of("type", "string"),
            "example", getExampleExpandRelations(tableInfo)
        ));
        get.put("parameters", parameters);
        
        Map<String, Object> getResponses = new LinkedHashMap<>();
        getResponses.put("200", Map.of(
            "description", capitalizeFirst(tableName) + " record",
            "content", Map.of(
                "application/json", Map.of(
                    "schema", Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName))
                )
            )
        ));
        getResponses.put("404", createErrorResponse("Record not found"));
        getResponses.put("500", createErrorResponse("Internal Server Error"));
        get.put("responses", getResponses);
        
        endpoint.put("get", get);
        
        if (!tableInfo.isView()) {
            // PUT endpoint for full update
            Map<String, Object> put = new LinkedHashMap<>();
            put.put("summary", "Update " + tableName + " record (full)");
            put.put("description", "Fully update a record in " + tableName + " table");
            put.put("tags", List.of(capitalizeFirst(tableName)));
            put.put("parameters", List.of(Map.of(
                "name", "id",
                "in", "path",
                "required", true,
                "description", "Primary key of the " + tableName + " record",
                "schema", Map.of("type", "string")
            )));
            put.put("requestBody", Map.of(
                "required", true,
                "content", Map.of(
                    "application/json", Map.of(
                        "schema", Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName) + "Input")
                    )
                )
            ));
            
            Map<String, Object> putResponses = new LinkedHashMap<>();
            putResponses.put("200", Map.of(
                "description", "Updated " + tableName + " record",
                "content", Map.of(
                    "application/json", Map.of(
                        "schema", Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName))
                    )
                )
            ));
            putResponses.put("404", createErrorResponse("Record not found"));
            putResponses.put("400", createErrorResponse("Bad Request"));
            putResponses.put("500", createErrorResponse("Internal Server Error"));
            put.put("responses", putResponses);
            
            endpoint.put("put", put);
            
            // PATCH endpoint for partial update
            Map<String, Object> patch = new LinkedHashMap<>();
            patch.put("summary", "Update " + tableName + " record (partial)");
            patch.put("description", "Partially update a record in " + tableName + " table");
            patch.put("tags", List.of(capitalizeFirst(tableName)));
            patch.put("parameters", List.of(Map.of(
                "name", "id",
                "in", "path",
                "required", true,
                "description", "Primary key of the " + tableName + " record",
                "schema", Map.of("type", "string")
            )));
            patch.put("requestBody", Map.of(
                "required", true,
                "content", Map.of(
                    "application/json", Map.of(
                        "schema", Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName) + "Input")
                    )
                )
            ));
            
            Map<String, Object> patchResponses = new LinkedHashMap<>();
            patchResponses.put("200", Map.of(
                "description", "Updated " + tableName + " record",
                "content", Map.of(
                    "application/json", Map.of(
                        "schema", Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName))
                    )
                )
            ));
            patchResponses.put("404", createErrorResponse("Record not found"));
            patchResponses.put("400", createErrorResponse("Bad Request"));
            patchResponses.put("500", createErrorResponse("Internal Server Error"));
            patch.put("responses", patchResponses);
            
            endpoint.put("patch", patch);
            
            // DELETE endpoint
            Map<String, Object> delete = new LinkedHashMap<>();
            delete.put("summary", "Delete " + tableName + " record");
            delete.put("description", "Delete a record from " + tableName + " table");
            delete.put("tags", List.of(capitalizeFirst(tableName)));
            delete.put("parameters", List.of(Map.of(
                "name", "id",
                "in", "path",
                "required", true,
                "description", "Primary key of the " + tableName + " record",
                "schema", Map.of("type", "string")
            )));
            
            Map<String, Object> deleteResponses = new LinkedHashMap<>();
            deleteResponses.put("204", Map.of("description", "Record deleted successfully"));
            deleteResponses.put("404", createErrorResponse("Record not found"));
            deleteResponses.put("500", createErrorResponse("Internal Server Error"));
            delete.put("responses", deleteResponses);
            
            endpoint.put("delete", delete);
        }
        
        return endpoint;
    }

    private Map<String, Object> createTableSchemaEndpoint(String tableName, TableInfo tableInfo) {
        Map<String, Object> endpoint = new LinkedHashMap<>();
        
        Map<String, Object> get = new LinkedHashMap<>();
        get.put("summary", "Get " + tableName + " table schema");
        get.put("description", "Get detailed schema information for " + tableName + " table");
        get.put("tags", List.of("Schema"));
        
        Map<String, Object> responses = new LinkedHashMap<>();
        responses.put("200", Map.of(
            "description", "Table schema information",
            "content", Map.of(
                "application/json", Map.of(
                    "schema", Map.of(
                        "type", "object",
                        "properties", Map.of(
                            "table", Map.of("$ref", "#/components/schemas/TableSchema")
                        )
                    )
                )
            )
        ));
        get.put("responses", responses);
        
        endpoint.put("get", get);
        return endpoint;
    }

    private List<Map<String, Object>> createCollectionParameters(TableInfo tableInfo) {
        List<Map<String, Object>> parameters = new ArrayList<>();
        
        // Basic pagination parameters
        parameters.add(Map.of(
            "name", "offset",
            "in", "query",
            "required", false,
            "description", "Number of records to skip (offset pagination)",
            "schema", Map.of("type", "integer", "minimum", 0, "default", 0)
        ));
        parameters.add(Map.of(
            "name", "limit",
            "in", "query",
            "required", false,
            "description", "Maximum number of records to return",
            "schema", Map.of("type", "integer", "minimum", 1, "maximum", 1000, "default", 100)
        ));
        
        // Cursor-based pagination
        parameters.add(Map.of(
            "name", "first",
            "in", "query",
            "required", false,
            "description", "Get first N records (cursor pagination)",
            "schema", Map.of("type", "integer", "minimum", 1, "maximum", 1000)
        ));
        parameters.add(Map.of(
            "name", "after",
            "in", "query",
            "required", false,
            "description", "Get records after this cursor",
            "schema", Map.of("type", "string")
        ));
        parameters.add(Map.of(
            "name", "last",
            "in", "query",
            "required", false,
            "description", "Get last N records (cursor pagination)",
            "schema", Map.of("type", "integer", "minimum", 1, "maximum", 1000)
        ));
        parameters.add(Map.of(
            "name", "before",
            "in", "query",
            "required", false,
            "description", "Get records before this cursor",
            "schema", Map.of("type", "string")
        ));
        
        // Sorting parameters
        parameters.add(Map.of(
            "name", "orderBy",
            "in", "query",
            "required", false,
            "description", "Column to sort by",
            "schema", Map.of("type", "string"),
            "example", getExampleOrderByColumn(tableInfo)
        ));
        parameters.add(Map.of(
            "name", "orderDirection",
            "in", "query",
            "required", false,
            "description", "Sort direction",
            "schema", Map.of("type", "string", "enum", List.of("asc", "desc"), "default", "asc")
        ));
        parameters.add(Map.of(
            "name", "order",
            "in", "query",
            "required", false,
            "description", "Ordering parameter: column.direction,column2.direction",
            "schema", Map.of("type", "string"),
            "example", "created_at.desc,name.asc"
        ));
        
        // Field selection and relationships
        parameters.add(Map.of(
            "name", "select",
            "in", "query",
            "required", false,
            "description", "Comma-separated list of columns to return",
            "schema", Map.of("type", "string"),
            "example", getExampleSelectColumns(tableInfo)
        ));
        parameters.add(Map.of(
            "name", "expand",
            "in", "query",
            "required", false,
            "description", "Comma-separated list of relationships to expand",
            "schema", Map.of("type", "string"),
            "example", getExampleExpandRelations(tableInfo)
        ));
        
        // Add column-specific filter parameters
        for (ColumnInfo column : tableInfo.getColumns()) {
            parameters.add(Map.of(
                "name", column.getName(),
                "in", "query",
                "required", false,
                "description", "Filter by " + column.getName() + " using operators (eq.value, gt.value, like.value, etc.)",
                "schema", Map.of("type", "string"),
                "example", getExampleFilterValue(column)
            ));
        }
        
        // OR parameter
        parameters.add(Map.of(
            "name", "or",
            "in", "query",
            "required", false,
            "description", "OR conditions in format: or=(col1.op.value,col2.op.value)",
            "schema", Map.of("type", "string"),
            "example", "or=(name.like.John,age.gt.30)"
        ));
        
        return parameters;
    }

    private Map<String, Object> createComponentsSection() {
        Map<String, Object> components = new LinkedHashMap<>();
        Map<String, Object> schemas = new LinkedHashMap<>();
        
        // Add common schemas
        schemas.put("Error", Map.of(
            "type", "object",
            "properties", Map.of(
                "error", Map.of("type", "string")
            ),
            "required", List.of("error")
        ));
        
        schemas.put("TableSchema", Map.of(
            "type", "object",
            "properties", Map.of(
                "name", Map.of("type", "string"),
                "type", Map.of("type", "string", "enum", List.of("TABLE", "VIEW")),
                "columns", Map.of(
                    "type", "array",
                    "items", Map.of("$ref", "#/components/schemas/ColumnSchema")
                ),
                "foreignKeys", Map.of(
                    "type", "array",
                    "items", Map.of("$ref", "#/components/schemas/ForeignKeySchema")
                )
            )
        ));
        
        schemas.put("ColumnSchema", Map.of(
            "type", "object",
            "properties", Map.of(
                "name", Map.of("type", "string"),
                "type", Map.of("type", "string"),
                "nullable", Map.of("type", "boolean"),
                "primaryKey", Map.of("type", "boolean")
            )
        ));
        
        schemas.put("ForeignKeySchema", Map.of(
            "type", "object",
            "properties", Map.of(
                "columnName", Map.of("type", "string"),
                "referencedTable", Map.of("type", "string"),
                "referencedColumn", Map.of("type", "string")
            )
        ));
        
        // Add table-specific schemas
        Map<String, TableInfo> schemaMap = schemaService.getTableSchema();
        for (Map.Entry<String, TableInfo> entry : schemaMap.entrySet()) {
            String tableName = entry.getKey();
            TableInfo tableInfo = entry.getValue();
            
            // Main table schema
            schemas.put(capitalizeFirst(tableName), createTableSchema(tableInfo));
            
            // Input schema for creation/update
            if (!tableInfo.isView()) {
                schemas.put(capitalizeFirst(tableName) + "Input", createTableInputSchema(tableInfo));
            }
        }
        
        components.put("schemas", schemas);
        return components;
    }

    private Map<String, Object> createTableSchema(TableInfo tableInfo) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        
        Map<String, Object> properties = new LinkedHashMap<>();
        List<String> required = new ArrayList<>();
        
        for (ColumnInfo column : tableInfo.getColumns()) {
            properties.put(column.getName(), createColumnSchema(column));
            if (!column.isNullable() && !column.isPrimaryKey()) {
                required.add(column.getName());
            }
        }
        
        schema.put("properties", properties);
        if (!required.isEmpty()) {
            schema.put("required", required);
        }
        
        return schema;
    }

    private Map<String, Object> createTableInputSchema(TableInfo tableInfo) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        
        Map<String, Object> properties = new LinkedHashMap<>();
        List<String> required = new ArrayList<>();
        
        for (ColumnInfo column : tableInfo.getColumns()) {
            // Skip auto-generated primary keys in input schema
            if (column.isPrimaryKey() && isAutoGeneratedColumn(column)) {
                continue;
            }
            
            properties.put(column.getName(), createColumnSchema(column));
            if (!column.isNullable() && !column.isPrimaryKey()) {
                required.add(column.getName());
            }
        }
        
        schema.put("properties", properties);
        if (!required.isEmpty()) {
            schema.put("required", required);
        }
        
        return schema;
    }

    private Map<String, Object> createColumnSchema(ColumnInfo column) {
        Map<String, Object> schema = new LinkedHashMap<>();
        String columnType = column.getType().toLowerCase();
        
        // Map PostgreSQL types to OpenAPI types
        if (columnType.contains("int") || columnType.contains("serial")) {
            schema.put("type", "integer");
            if (columnType.contains("bigint") || columnType.contains("bigserial") || columnType.contains("int8")) {
                schema.put("format", "int64");
            } else {
                schema.put("format", "int32");
            }
        } else if (columnType.contains("decimal") || columnType.contains("numeric") || 
                   columnType.contains("real") || columnType.contains("double") || 
                   columnType.contains("float")) {
            schema.put("type", "number");
            if (columnType.contains("double") || columnType.contains("float8")) {
                schema.put("format", "double");
            } else {
                schema.put("format", "float");
            }
        } else if (columnType.equals("boolean") || columnType.equals("bool")) {
            schema.put("type", "boolean");
        } else if (columnType.contains("timestamp") || columnType.contains("date")) {
            schema.put("type", "string");
            if (columnType.contains("timestamp")) {
                schema.put("format", "date-time");
            } else {
                schema.put("format", "date");
            }
        } else if (columnType.contains("time")) {
            schema.put("type", "string");
            schema.put("format", "time");
        } else if (columnType.equals("uuid")) {
            schema.put("type", "string");
            schema.put("format", "uuid");
        } else if (columnType.equals("json") || columnType.equals("jsonb")) {
            schema.put("type", "object");
            schema.put("description", "JSON object");
        } else if (columnType.endsWith("[]")) {
            schema.put("type", "array");
            schema.put("items", Map.of("type", "string"));
            schema.put("description", "PostgreSQL array type");
        } else {
            schema.put("type", "string");
        }
        
        // Add description
        schema.put("description", "Column: " + column.getName() + " (Type: " + column.getType() + 
                   ", Nullable: " + column.isNullable() + ", Primary Key: " + column.isPrimaryKey() + ")");
        
        return schema;
    }

    private Map<String, Object> createPaginatedResponseSchema(String tableName) {
        return Map.of(
            "type", "object",
            "properties", Map.of(
                "data", Map.of(
                    "type", "array",
                    "items", Map.of("$ref", "#/components/schemas/" + capitalizeFirst(tableName))
                ),
                "pagination", Map.of(
                    "type", "object",
                    "properties", Map.of(
                        "offset", Map.of("type", "integer"),
                        "limit", Map.of("type", "integer"),
                        "total", Map.of("type", "integer"),
                        "hasMore", Map.of("type", "boolean")
                    )
                )
            )
        );
    }

    private Map<String, Object> createErrorResponse(String description) {
        return Map.of(
            "description", description,
            "content", Map.of(
                "application/json", Map.of(
                    "schema", Map.of("$ref", "#/components/schemas/Error")
                )
            )
        );
    }

    private String capitalizeFirst(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    private String getExampleSelectColumns(TableInfo tableInfo) {
        List<String> columns = tableInfo.getColumns().stream()
            .map(ColumnInfo::getName)
            .limit(3)
            .toList();
        return String.join(",", columns);
    }

    private String getExampleExpandRelations(TableInfo tableInfo) {
        if (tableInfo.getForeignKeys().isEmpty()) {
            return "related_table";
        }
        return tableInfo.getForeignKeys().get(0).getReferencedTable();
    }

    private String getExampleOrderByColumn(TableInfo tableInfo) {
        return tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .findFirst()
            .orElse(tableInfo.getColumns().isEmpty() ? "id" : tableInfo.getColumns().get(0).getName());
    }

    private String getExampleFilterValue(ColumnInfo column) {
        String columnType = column.getType().toLowerCase();
        
        if (columnType.contains("int") || columnType.contains("serial")) {
            return "eq.123";
        } else if (columnType.contains("decimal") || columnType.contains("numeric") || 
                   columnType.contains("real") || columnType.contains("double") || 
                   columnType.contains("float")) {
            return "gte.10.5";
        } else if (columnType.equals("boolean") || columnType.equals("bool")) {
            return "is.true";
        } else if (columnType.contains("timestamp") || columnType.contains("date")) {
            return "gte.2023-01-01";
        } else if (columnType.equals("uuid")) {
            return "eq.550e8400-e29b-41d4-a716-446655440000";
        } else if (columnType.equals("json") || columnType.equals("jsonb")) {
            return "haskey.admin";
        } else if (columnType.endsWith("[]")) {
            return "arraycontains.value";
        } else {
            return "like.example";
        }
    }

    private boolean isAutoGeneratedColumn(ColumnInfo column) {
        String columnType = column.getType().toLowerCase();
        return column.isPrimaryKey() && 
               (columnType.contains("serial") || columnType.equals("uuid"));
    }
}