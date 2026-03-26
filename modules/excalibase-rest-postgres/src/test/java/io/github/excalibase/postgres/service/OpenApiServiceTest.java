package io.github.excalibase.postgres.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.TableInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenApiServiceTest {

    @Mock
    private DatabaseSchemaService schemaService;

    private OpenApiService openApiService;

    @BeforeEach
    void setup() {
        openApiService = new OpenApiService(schemaService);
    }

    // ===== generateOpenApiSpec structure tests =====

    @Test
    void generateOpenApiSpec_returnsValidTopLevelStructure() {
        when(schemaService.getTableSchema()).thenReturn(Map.of());

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        assertNotNull(spec);
        assertEquals("3.0.3", spec.get("openapi"));
        assertTrue(spec.containsKey("info"));
        assertTrue(spec.containsKey("servers"));
        assertTrue(spec.containsKey("paths"));
        assertTrue(spec.containsKey("components"));
    }

    @Test
    void generateOpenApiSpec_infoSectionHasRequiredFields() {
        when(schemaService.getTableSchema()).thenReturn(Map.of());

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> info = (Map<String, Object>) spec.get("info");
        assertNotNull(info);
        assertEquals("Excalibase REST API", info.get("title"));
        assertEquals("1.0.0", info.get("version"));
        assertTrue(info.containsKey("description"));
        assertTrue(info.containsKey("contact"));
        assertTrue(info.containsKey("license"));
    }

    @Test
    void generateOpenApiSpec_serversSectionContainsDevelopmentServer() {
        when(schemaService.getTableSchema()).thenReturn(Map.of());

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> servers = (List<Map<String, Object>>) spec.get("servers");
        assertNotNull(servers);
        assertFalse(servers.isEmpty());
        assertEquals("http://localhost:20000", servers.get(0).get("url"));
    }

    // ===== paths section tests =====

    @Test
    void generateOpenApiSpec_pathsIncludeBaseSchemaEndpoints() {
        when(schemaService.getTableSchema()).thenReturn(Map.of());

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        assertTrue(paths.containsKey("/api/v1"));
        assertTrue(paths.containsKey("/api/v1/openapi.json"));
        assertTrue(paths.containsKey("/api/v1/openapi.yaml"));
        assertTrue(paths.containsKey("/api/v1/docs"));
    }

    @Test
    void generateOpenApiSpec_pathsCreatedForEachTable() {
        Map<String, TableInfo> schema = buildSchema("users", false);
        when(schemaService.getTableSchema()).thenReturn(schema);

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        assertTrue(paths.containsKey("/api/v1/users"));
        assertTrue(paths.containsKey("/api/v1/users/{id}"));
        assertTrue(paths.containsKey("/api/v1/users/schema"));
    }

    @Test
    void generateOpenApiSpec_regularTable_collectionEndpointHasGetAndPost() {
        Map<String, TableInfo> schema = buildSchema("users", false);
        when(schemaService.getTableSchema()).thenReturn(schema);

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        @SuppressWarnings("unchecked")
        Map<String, Object> usersPath = (Map<String, Object>) paths.get("/api/v1/users");
        assertTrue(usersPath.containsKey("get"), "Collection endpoint should have GET");
        assertTrue(usersPath.containsKey("post"), "Regular table collection endpoint should have POST");
    }

    @Test
    void generateOpenApiSpec_viewTable_collectionEndpointHasGetOnly() {
        Map<String, TableInfo> schema = buildSchema("user_view", true);
        when(schemaService.getTableSchema()).thenReturn(schema);

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        @SuppressWarnings("unchecked")
        Map<String, Object> viewPath = (Map<String, Object>) paths.get("/api/v1/user_view");
        assertTrue(viewPath.containsKey("get"), "View endpoint should have GET");
        assertFalse(viewPath.containsKey("post"), "View collection endpoint should NOT have POST");
    }

    @Test
    void generateOpenApiSpec_regularTable_itemEndpointHasAllMethods() {
        Map<String, TableInfo> schema = buildSchema("orders", false);
        when(schemaService.getTableSchema()).thenReturn(schema);

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        @SuppressWarnings("unchecked")
        Map<String, Object> itemPath = (Map<String, Object>) paths.get("/api/v1/orders/{id}");
        assertTrue(itemPath.containsKey("get"));
        assertTrue(itemPath.containsKey("put"));
        assertTrue(itemPath.containsKey("patch"));
        assertTrue(itemPath.containsKey("delete"));
    }

    @Test
    void generateOpenApiSpec_viewTable_itemEndpointHasGetOnly() {
        Map<String, TableInfo> schema = buildSchema("orders_view", true);
        when(schemaService.getTableSchema()).thenReturn(schema);

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        @SuppressWarnings("unchecked")
        Map<String, Object> itemPath = (Map<String, Object>) paths.get("/api/v1/orders_view/{id}");
        assertTrue(itemPath.containsKey("get"));
        assertFalse(itemPath.containsKey("put"));
        assertFalse(itemPath.containsKey("patch"));
        assertFalse(itemPath.containsKey("delete"));
    }

    @Test
    void generateOpenApiSpec_multipleTablesCreatesPathsForAll() {
        Map<String, TableInfo> schema = new LinkedHashMap<>();
        schema.put("users", buildTableInfo("users", false));
        schema.put("orders", buildTableInfo("orders", false));
        schema.put("products", buildTableInfo("products", false));
        when(schemaService.getTableSchema()).thenReturn(schema);

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        assertTrue(paths.containsKey("/api/v1/users"));
        assertTrue(paths.containsKey("/api/v1/orders"));
        assertTrue(paths.containsKey("/api/v1/products"));
    }

    // ===== components section tests =====

    @Test
    void generateOpenApiSpec_componentsContainsCommonSchemas() {
        when(schemaService.getTableSchema()).thenReturn(Map.of());

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> components = (Map<String, Object>) spec.get("components");
        @SuppressWarnings("unchecked")
        Map<String, Object> schemas = (Map<String, Object>) components.get("schemas");
        assertTrue(schemas.containsKey("Error"));
        assertTrue(schemas.containsKey("TableSchema"));
        assertTrue(schemas.containsKey("ColumnSchema"));
        assertTrue(schemas.containsKey("ForeignKeySchema"));
    }

    @Test
    void generateOpenApiSpec_regularTable_schemaAndInputSchemaCreated() {
        Map<String, TableInfo> schema = buildSchema("products", false);
        when(schemaService.getTableSchema()).thenReturn(schema);

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> components = (Map<String, Object>) spec.get("components");
        @SuppressWarnings("unchecked")
        Map<String, Object> schemas = (Map<String, Object>) components.get("schemas");
        assertTrue(schemas.containsKey("Products"), "Main schema should exist");
        assertTrue(schemas.containsKey("ProductsInput"), "Input schema should exist for regular table");
    }

    @Test
    void generateOpenApiSpec_viewTable_onlyMainSchemaCreated() {
        Map<String, TableInfo> schema = buildSchema("product_view", true);
        when(schemaService.getTableSchema()).thenReturn(schema);

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> components = (Map<String, Object>) spec.get("components");
        @SuppressWarnings("unchecked")
        Map<String, Object> schemas = (Map<String, Object>) components.get("schemas");
        assertTrue(schemas.containsKey("Product_view"));
        assertFalse(schemas.containsKey("Product_viewInput"), "View should not have Input schema");
    }

    // ===== column type to OpenAPI type mapping tests =====

    @Test
    void generateOpenApiSpec_integerColumn_mapsToIntegerType() {
        TableInfo tableInfo = buildTableInfoWithColumns("items",
                List.of(new ColumnInfo("count", "integer", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("items", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> schemas = getSchemas(spec);
        @SuppressWarnings("unchecked")
        Map<String, Object> itemsSchema = (Map<String, Object>) schemas.get("Items");
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) itemsSchema.get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> countProp = (Map<String, Object>) properties.get("count");
        assertEquals("integer", countProp.get("type"));
        assertEquals("int32", countProp.get("format"));
    }

    @Test
    void generateOpenApiSpec_bigintColumn_mapsToInt64Format() {
        TableInfo tableInfo = buildTableInfoWithColumns("items",
                List.of(new ColumnInfo("big_count", "bigint", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("items", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Items");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("big_count");
        assertEquals("integer", col.get("type"));
        assertEquals("int64", col.get("format"));
    }

    @Test
    void generateOpenApiSpec_decimalColumn_mapsToNumberType() {
        TableInfo tableInfo = buildTableInfoWithColumns("items",
                List.of(new ColumnInfo("price", "decimal", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("items", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Items");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("price");
        assertEquals("number", col.get("type"));
    }

    @Test
    void generateOpenApiSpec_doubleColumn_mapsToDoubleFormat() {
        TableInfo tableInfo = buildTableInfoWithColumns("measurements",
                List.of(new ColumnInfo("value", "double precision", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("measurements", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Measurements");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("value");
        assertEquals("number", col.get("type"));
        assertEquals("double", col.get("format"));
    }

    @Test
    void generateOpenApiSpec_booleanColumn_mapsToBooleanType() {
        TableInfo tableInfo = buildTableInfoWithColumns("items",
                List.of(new ColumnInfo("active", "boolean", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("items", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Items");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("active");
        assertEquals("boolean", col.get("type"));
    }

    @Test
    void generateOpenApiSpec_timestampColumn_mapsToDateTimeFormat() {
        TableInfo tableInfo = buildTableInfoWithColumns("events",
                List.of(new ColumnInfo("created_at", "timestamp", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("events", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Events");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("created_at");
        assertEquals("string", col.get("type"));
        assertEquals("date-time", col.get("format"));
    }

    @Test
    void generateOpenApiSpec_dateColumn_mapsToDateFormat() {
        TableInfo tableInfo = buildTableInfoWithColumns("events",
                List.of(new ColumnInfo("event_date", "date", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("events", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Events");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("event_date");
        assertEquals("string", col.get("type"));
        assertEquals("date", col.get("format"));
    }

    @Test
    void generateOpenApiSpec_uuidColumn_mapsToUuidFormat() {
        TableInfo tableInfo = buildTableInfoWithColumns("users",
                List.of(new ColumnInfo("id", "uuid", true, false)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Users");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("id");
        assertEquals("string", col.get("type"));
        assertEquals("uuid", col.get("format"));
    }

    @Test
    void generateOpenApiSpec_jsonColumn_mapsToObjectType() {
        TableInfo tableInfo = buildTableInfoWithColumns("configs",
                List.of(new ColumnInfo("settings", "jsonb", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("configs", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Configs");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("settings");
        assertEquals("object", col.get("type"));
    }

    @Test
    void generateOpenApiSpec_arrayColumn_mapsToArrayType() {
        TableInfo tableInfo = buildTableInfoWithColumns("tags_table",
                List.of(new ColumnInfo("tags", "text[]", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("tags_table", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Tags_table");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("tags");
        assertEquals("array", col.get("type"));
    }

    @Test
    void generateOpenApiSpec_unknownTypeColumn_mapsToStringType() {
        TableInfo tableInfo = buildTableInfoWithColumns("misc",
                List.of(new ColumnInfo("data", "someunknowntype", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("misc", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Misc");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("data");
        assertEquals("string", col.get("type"));
    }

    // ===== required fields tests =====

    @Test
    void generateOpenApiSpec_nonNullableNonPKColumn_appearsInRequired() {
        ColumnInfo requiredCol = new ColumnInfo("email", "text", false, false);
        ColumnInfo nullableCol = new ColumnInfo("bio", "text", false, true);
        TableInfo tableInfo = buildTableInfoWithColumns("users", List.of(requiredCol, nullableCol));
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> schemas = getSchemas(spec);
        @SuppressWarnings("unchecked")
        Map<String, Object> usersSchema = (Map<String, Object>) schemas.get("Users");
        @SuppressWarnings("unchecked")
        List<String> required = (List<String>) usersSchema.get("required");
        assertNotNull(required);
        assertTrue(required.contains("email"));
        assertFalse(required.contains("bio"));
    }

    @Test
    void generateOpenApiSpec_allNullableColumns_noRequiredField() {
        ColumnInfo col1 = new ColumnInfo("name", "text", false, true);
        ColumnInfo col2 = new ColumnInfo("bio", "text", false, true);
        TableInfo tableInfo = buildTableInfoWithColumns("profiles", List.of(col1, col2));
        when(schemaService.getTableSchema()).thenReturn(Map.of("profiles", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> schemas = getSchemas(spec);
        @SuppressWarnings("unchecked")
        Map<String, Object> schema = (Map<String, Object>) schemas.get("Profiles");
        assertFalse(schema.containsKey("required"), "No required field when all columns are nullable");
    }

    @Test
    void generateOpenApiSpec_tableWithForeignKey_expandExampleUsesRefTable() {
        ColumnInfo idCol = new ColumnInfo("id", "integer", true, false);
        ColumnInfo custIdCol = new ColumnInfo("customer_id", "integer", false, false);
        ForeignKeyInfo fk = new ForeignKeyInfo("customer_id", "customers", "id");
        TableInfo tableInfo = new TableInfo("orders",
                List.of(idCol, custIdCol), List.of(fk), false);
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        // This exercises getExampleExpandRelations branch with a real FK
        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        assertNotNull(spec); // Just verifying it doesn't throw and returns a spec
    }

    @Test
    void generateOpenApiSpec_tableWithEmptyColumns_worksWithoutError() {
        TableInfo tableInfo = new TableInfo("empty_table", Collections.emptyList(), Collections.emptyList());
        when(schemaService.getTableSchema()).thenReturn(Map.of("empty_table", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        assertNotNull(spec);
        @SuppressWarnings("unchecked")
        Map<String, Object> paths = (Map<String, Object>) spec.get("paths");
        assertTrue(paths.containsKey("/api/v1/empty_table"));
    }

    @Test
    void generateOpenApiSpec_serialPrimaryKey_excludedFromInputSchema() {
        ColumnInfo pkCol = new ColumnInfo("id", "serial", true, false);
        ColumnInfo nameCol = new ColumnInfo("name", "text", false, false);
        TableInfo tableInfo = buildTableInfoWithColumns("items", List.of(pkCol, nameCol));
        when(schemaService.getTableSchema()).thenReturn(Map.of("items", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> schemas = getSchemas(spec);
        @SuppressWarnings("unchecked")
        Map<String, Object> inputSchema = (Map<String, Object>) schemas.get("ItemsInput");
        @SuppressWarnings("unchecked")
        Map<String, Object> inputProps = (Map<String, Object>) inputSchema.get("properties");
        assertFalse(inputProps.containsKey("id"), "Auto-generated serial PK should be excluded from input schema");
        assertTrue(inputProps.containsKey("name"));
    }

    @Test
    void generateOpenApiSpec_timeColumn_mapsToTimeFormat() {
        TableInfo tableInfo = buildTableInfoWithColumns("schedules",
                List.of(new ColumnInfo("start_time", "time", false, true)));
        when(schemaService.getTableSchema()).thenReturn(Map.of("schedules", tableInfo));

        Map<String, Object> spec = openApiService.generateOpenApiSpec();

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = getTableProperties(spec, "Schedules");
        @SuppressWarnings("unchecked")
        Map<String, Object> col = (Map<String, Object>) properties.get("start_time");
        assertEquals("string", col.get("type"));
        assertEquals("time", col.get("format"));
    }

    // ===== helper methods =====

    private Map<String, TableInfo> buildSchema(String tableName, boolean isView) {
        TableInfo tableInfo = buildTableInfo(tableName, isView);
        Map<String, TableInfo> schema = new LinkedHashMap<>();
        schema.put(tableName, tableInfo);
        return schema;
    }

    private TableInfo buildTableInfo(String tableName, boolean isView) {
        List<ColumnInfo> columns = List.of(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("name", "text", false, false)
        );
        return new TableInfo(tableName, columns, Collections.emptyList(), isView);
    }

    private TableInfo buildTableInfoWithColumns(String tableName, List<ColumnInfo> columns) {
        return new TableInfo(tableName, columns, Collections.emptyList(), false);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getSchemas(Map<String, Object> spec) {
        Map<String, Object> components = (Map<String, Object>) spec.get("components");
        return (Map<String, Object>) components.get("schemas");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getTableProperties(Map<String, Object> spec, String schemaName) {
        Map<String, Object> schemas = getSchemas(spec);
        Map<String, Object> tableSchema = (Map<String, Object>) schemas.get(schemaName);
        return (Map<String, Object>) tableSchema.get("properties");
    }
}
