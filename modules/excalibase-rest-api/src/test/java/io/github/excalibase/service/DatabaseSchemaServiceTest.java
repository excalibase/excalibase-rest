package io.github.excalibase.service;

import io.github.excalibase.constant.DatabaseType;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.postgres.service.DatabaseSchemaService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class DatabaseSchemaServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private DatabaseSchemaService schemaService;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        schemaService = new DatabaseSchemaService(jdbcTemplate, "test_schema", "postgres");
    }

    @Test
    void shouldReturnCachedSchemaWhenCacheIsValid() throws Exception {
        // Given: a schema is already cached
        TableInfo tableInfo = new TableInfo("test_table", List.of(), List.of());
        setPrivateField(schemaService, "schemaCache",
            new ConcurrentHashMap<>(Map.of("test_schema", Map.of("test_table", tableInfo))));
        setPrivateField(schemaService, "lastCacheUpdate", System.currentTimeMillis());

        // When: getting table schema
        Map<String, TableInfo> result = schemaService.getTableSchema();

        // Then: should return cached schema without database calls
        assertEquals(1, result.size());
        assertEquals(tableInfo, result.get("test_table"));
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void shouldRefreshCacheWhenTTLExpired() throws Exception {
        // Given: an expired cache
        setPrivateField(schemaService, "schemaCache",
            new ConcurrentHashMap<>(Map.of("test_schema", Map.of("old_table", new TableInfo()))));
        setPrivateField(schemaService, "lastCacheUpdate", System.currentTimeMillis() - 400_000L); // 6+ minutes ago

        // Mock database responses
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("test_schema")))
                .thenReturn(List.of("new_table"));
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("new_table"), eq("test_schema"), eq("new_table")))
                .thenReturn(List.of(new ColumnInfo("id", "integer", true, false)));
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("new_table")))
                .thenReturn(List.of());
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq("test_schema"), eq("new_table")))
                .thenReturn(0);

        // When: getting table schema
        Map<String, TableInfo> result = schemaService.getTableSchema();

        // Then: should refresh cache with new data
        assertEquals(1, result.size());
        assertTrue(result.containsKey("new_table"));
        assertFalse(result.containsKey("old_table"));
    }

    @Test
    void shouldHandleDatabaseConnectionErrorsGracefully() {
        // Given: database throws exception
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("test_schema")))
                .thenThrow(new RuntimeException("DB connection failed"));

        // When & Then: should propagate exception with context
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            schemaService.getTableSchema();
        });
        assertEquals("Failed to reflect database schema", exception.getMessage());
        assertEquals("DB connection failed", exception.getCause().getMessage());
    }

    @Test
    void shouldClearCacheWhenRequested() throws Exception {
        // Given: a populated cache
        setPrivateField(schemaService, "schemaCache",
            new ConcurrentHashMap<>(Map.of("test_schema", Map.of("table1", new TableInfo()))));
        setPrivateField(schemaService, "lastCacheUpdate", System.currentTimeMillis());

        // When: clearing cache
        schemaService.clearCache();

        // Then: cache should be empty
        Map<String, Map<String, TableInfo>> schemaCache = getPrivateField(schemaService, "schemaCache");
        long lastCacheUpdate = getPrivateField(schemaService, "lastCacheUpdate");
        assertTrue(schemaCache.isEmpty());
        assertEquals(0L, lastCacheUpdate);
    }

    @Test
    void shouldReturnCorrectAllowedSchemaAndDatabaseType() {
        // Expect: configuration values are returned correctly
        assertEquals("test_schema", schemaService.getAllowedSchema());
        assertEquals(DatabaseType.POSTGRES, schemaService.getDatabaseType());
    }

    @Test
    void shouldBuildTableInfoWithColumnsAndForeignKeys() throws SQLException {
        // Given: mock database responses for table reflection
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("test_schema")))
                .thenReturn(List.of("users"));

        // Mock columns query
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("users"), eq("test_schema"), eq("users")))
                .thenAnswer(invocation -> {
                    RowMapper<?> rowMapper = invocation.getArgument(1);
                    return List.of(
                            createMockColumnResult(rowMapper, "id", "integer", "integer", false, true),
                            createMockColumnResult(rowMapper, "name", "varchar", "varchar", true, false),
                            createMockColumnResult(rowMapper, "email", "varchar", "varchar", false, false)
                    );
                });

        // Mock foreign keys query
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("users")))
                .thenAnswer(invocation -> {
                    RowMapper<?> rowMapper = invocation.getArgument(1);
                    return List.of(
                            createMockForeignKeyResult(rowMapper, "department_id", "departments", "id")
                    );
                });

        // Mock view check
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq("test_schema"), eq("users")))
                .thenReturn(0);

        // When: getting table schema
        Map<String, TableInfo> result = schemaService.getTableSchema();

        // Then: should return properly structured table info
        assertEquals(1, result.size());
        TableInfo tableInfo = result.get("users");
        assertEquals("users", tableInfo.getName());
        assertEquals(3, tableInfo.getColumns().size());
        assertEquals(1, tableInfo.getForeignKeys().size());
        assertFalse(tableInfo.isView());

        // And: columns should be properly mapped
        ColumnInfo idColumn = tableInfo.getColumns().stream()
                .filter(c -> "id".equals(c.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull(idColumn);
        assertEquals("integer", idColumn.getType());
        assertTrue(idColumn.isPrimaryKey());
        assertFalse(idColumn.isNullable());

        ColumnInfo nameColumn = tableInfo.getColumns().stream()
                .filter(c -> "name".equals(c.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull(nameColumn);
        assertEquals("varchar", nameColumn.getType());
        assertFalse(nameColumn.isPrimaryKey());
        assertTrue(nameColumn.isNullable());

        // And: foreign keys should be properly mapped
        ForeignKeyInfo fk = tableInfo.getForeignKeys().get(0);
        assertEquals("department_id", fk.getColumnName());
        assertEquals("departments", fk.getReferencedTable());
        assertEquals("id", fk.getReferencedColumn());
    }

    @Test
    void shouldIdentifyViewsCorrectly() {
        // Given: mock database responses for view
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("test_schema")))
                .thenReturn(List.of("user_view"));
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("user_view"), eq("test_schema"), eq("user_view")))
                .thenReturn(List.of(new ColumnInfo("id", "integer", false, false)));
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("user_view")))
                .thenReturn(List.of());
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq("test_schema"), eq("user_view")))
                .thenReturn(1);

        // When: getting table schema
        Map<String, TableInfo> result = schemaService.getTableSchema();

        // Then: should identify as view
        TableInfo tableInfo = result.get("user_view");
        assertTrue(tableInfo.isView());
    }

    @Test
    void shouldEnhancePostgreSQLArrayTypesCorrectly() throws SQLException {
        // Given: mock database responses for array type
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("test_schema")))
                .thenReturn(List.of("array_table"));

        // Mock columns query with array type
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("array_table"), eq("test_schema"), eq("array_table")))
                .thenAnswer(invocation -> {
                    RowMapper<?> rowMapper = invocation.getArgument(1);
                    return List.of(
                            createMockColumnResult(rowMapper, "id", "integer", "integer", false, true),
                            createMockColumnResult(rowMapper, "tags", "varchar[]", "_varchar", true, false),
                            createMockColumnResult(rowMapper, "scores", "integer[]", "_integer", true, false)
                    );
                });

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("array_table")))
                .thenReturn(List.of());
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq("test_schema"), eq("array_table")))
                .thenReturn(0);

        // When: getting table schema with array types
        Map<String, TableInfo> result = schemaService.getTableSchema();

        // Then: should convert array types correctly
        TableInfo tableInfo = result.get("array_table");
        ColumnInfo tagsColumn = tableInfo.getColumns().stream()
                .filter(c -> "tags".equals(c.getName()))
                .findFirst()
                .orElse(null);
        ColumnInfo scoresColumn = tableInfo.getColumns().stream()
                .filter(c -> "scores".equals(c.getName()))
                .findFirst()
                .orElse(null);

        assertNotNull(tagsColumn);
        assertNotNull(scoresColumn);
        assertEquals("varchar[]", tagsColumn.getType());
        assertEquals("integer[]", scoresColumn.getType());
    }

    @Test
    void shouldDetectEnumTypesCorrectly() throws SQLException {
        // Given: enum type exists
        when(jdbcTemplate.queryForObject(anyString(), eq(Boolean.class), eq("status_enum")))
                .thenReturn(true);

        // Mock database responses for enum type
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("test_schema")))
                .thenReturn(List.of("enum_table"));

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("enum_table"), eq("test_schema"), eq("enum_table")))
                .thenAnswer(invocation -> {
                    RowMapper<?> rowMapper = invocation.getArgument(1);
                    return List.of(
                            createMockColumnResult(rowMapper, "id", "integer", "integer", false, true),
                            createMockColumnResult(rowMapper, "status", "status_enum", "status_enum", false, false)
                    );
                });

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("enum_table")))
                .thenReturn(List.of());
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq("test_schema"), eq("enum_table")))
                .thenReturn(0);

        // When: getting table schema with enum type
        Map<String, TableInfo> result = schemaService.getTableSchema();

        // Then: should identify enum type correctly
        TableInfo tableInfo = result.get("enum_table");
        ColumnInfo statusColumn = tableInfo.getColumns().stream()
                .filter(c -> "status".equals(c.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull(statusColumn);
        assertTrue(statusColumn.getType().startsWith("postgres_enum:status_enum"));
    }

    @Test
    void shouldDetectCompositeTypesCorrectly() throws SQLException {
        // Given: composite type exists
        when(jdbcTemplate.queryForObject(argThat(sql -> sql != null && sql.contains("pg_enum")), eq(Boolean.class), eq("address_type")))
                .thenReturn(false);
        when(jdbcTemplate.queryForObject(argThat(sql -> sql != null && sql.contains("typtype = 'c'")), eq(Boolean.class), eq("address_type")))
                .thenReturn(true);

        // Mock database responses for composite type
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("test_schema")))
                .thenReturn(List.of("composite_table"));

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("composite_table"), eq("test_schema"), eq("composite_table")))
                .thenAnswer(invocation -> {
                    RowMapper<?> rowMapper = invocation.getArgument(1);
                    return List.of(
                            createMockColumnResult(rowMapper, "id", "integer", "integer", false, true),
                            createMockColumnResult(rowMapper, "address", "address_type", "address_type", true, false)
                    );
                });

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("composite_table")))
                .thenReturn(List.of());
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq("test_schema"), eq("composite_table")))
                .thenReturn(0);

        // When: getting table schema with composite type
        Map<String, TableInfo> result = schemaService.getTableSchema();

        // Then: should identify composite type correctly
        TableInfo tableInfo = result.get("composite_table");
        ColumnInfo addressColumn = tableInfo.getColumns().stream()
                .filter(c -> "address".equals(c.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull(addressColumn);
        assertTrue(addressColumn.getType().startsWith("postgres_composite:address_type"));
    }

    @Test
    void shouldHandleNetworkTypesCorrectly() throws SQLException {
        // Given: mock database responses for network types
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("test_schema")))
                .thenReturn(List.of("network_table"));

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("network_table"), eq("test_schema"), eq("network_table")))
                .thenAnswer(invocation -> {
                    RowMapper<?> rowMapper = invocation.getArgument(1);
                    return List.of(
                            createMockColumnResult(rowMapper, "id", "integer", "integer", false, true),
                            createMockColumnResult(rowMapper, "ip_address", "inet", "inet", true, false),
                            createMockColumnResult(rowMapper, "network_range", "cidr", "cidr", true, false),
                            createMockColumnResult(rowMapper, "mac_address", "macaddr", "macaddr", true, false)
                    );
                });

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("network_table")))
                .thenReturn(List.of());
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq("test_schema"), eq("network_table")))
                .thenReturn(0);

        // When: getting table schema with network types
        Map<String, TableInfo> result = schemaService.getTableSchema();

        // Then: should map network types correctly
        TableInfo tableInfo = result.get("network_table");
        ColumnInfo ipColumn = tableInfo.getColumns().stream()
                .filter(c -> "ip_address".equals(c.getName()))
                .findFirst()
                .orElse(null);
        ColumnInfo networkColumn = tableInfo.getColumns().stream()
                .filter(c -> "network_range".equals(c.getName()))
                .findFirst()
                .orElse(null);
        ColumnInfo macColumn = tableInfo.getColumns().stream()
                .filter(c -> "mac_address".equals(c.getName()))
                .findFirst()
                .orElse(null);

        assertNotNull(ipColumn);
        assertNotNull(networkColumn);
        assertNotNull(macColumn);
        assertEquals("inet", ipColumn.getType());
        assertEquals("cidr", networkColumn.getType());
        assertEquals("macaddr", macColumn.getType());
    }

    @Test
    void shouldGetEnumValuesCorrectly() {
        // Given: enum values exist
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("status_type")))
                .thenReturn(List.of("pending", "active", "inactive", "suspended"));

        // When: getting enum values
        List<String> result = schemaService.getEnumValues("status_type");

        // Then: should return all enum values in order
        assertEquals(4, result.size());
        assertEquals(List.of("pending", "active", "inactive", "suspended"), result);
    }

    @Test
    void shouldHandleEnumValuesQueryFailureGracefully() {
        // Given: database throws exception for enum values
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("invalid_enum")))
                .thenThrow(new RuntimeException("Enum not found"));

        // When: getting enum values for invalid enum
        List<String> result = schemaService.getEnumValues("invalid_enum");

        // Then: should return empty list
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldGetCompositeTypeDefinitionCorrectly() throws SQLException {
        // Given: composite type definition exists
        doAnswer(invocation -> {
            RowCallbackHandler callback = invocation.getArgument(1);

            ResultSet mockResultSet1 = mock(ResultSet.class);
            when(mockResultSet1.getString("attname")).thenReturn("street");
            when(mockResultSet1.getString("typname")).thenReturn("varchar");
            callback.processRow(mockResultSet1);

            ResultSet mockResultSet2 = mock(ResultSet.class);
            when(mockResultSet2.getString("attname")).thenReturn("city");
            when(mockResultSet2.getString("typname")).thenReturn("varchar");
            callback.processRow(mockResultSet2);

            ResultSet mockResultSet3 = mock(ResultSet.class);
            when(mockResultSet3.getString("attname")).thenReturn("zip_code");
            when(mockResultSet3.getString("typname")).thenReturn("varchar");
            callback.processRow(mockResultSet3);

            return null;
        }).when(jdbcTemplate).query(anyString(), any(RowCallbackHandler.class), eq("address_type"));

        // When: getting composite type definition
        Map<String, String> result = schemaService.getCompositeTypeDefinition("address_type");

        // Then: should return field definitions in order
        assertEquals(3, result.size());
        assertEquals("varchar", result.get("street"));
        assertEquals("varchar", result.get("city"));
        assertEquals("varchar", result.get("zip_code"));
    }

    @Test
    void shouldHandleCompositeTypeDefinitionQueryFailureGracefully() {
        // Given: database throws exception for composite type
        doThrow(new RuntimeException("Type not found"))
                .when(jdbcTemplate).query(anyString(), any(RowCallbackHandler.class), eq("invalid_type"));

        // When: getting definition for invalid composite type
        Map<String, String> result = schemaService.getCompositeTypeDefinition("invalid_type");

        // Then: should return empty map
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldHandleTableNamesQueryFallbackOnPermissionError() {
        // Given: role-based query fails but fallback succeeds
        when(jdbcTemplate.queryForList(argThat(sql -> sql != null && sql.contains("has_table_privilege")), eq(String.class), eq("test_schema")))
                .thenThrow(new RuntimeException("Permission check failed"));
        when(jdbcTemplate.queryForList(argThat(sql -> sql != null && !sql.contains("has_table_privilege")), eq(String.class), eq("test_schema")))
                .thenReturn(List.of("fallback_table"));

        // When: getting table names with permission error
        schemaService.clearCache();

        // Then: should use fallback query and return tables
        assertDoesNotThrow(() -> schemaService.getTableSchema());
    }

    @Test
    void shouldHandleTypeEnhancementErrorGracefully() throws SQLException {
        // Given: type enhancement fails
        when(jdbcTemplate.queryForList(anyString(), eq(String.class), eq("test_schema")))
                .thenReturn(List.of("error_table"));

        // Mock enum/composite type checks to throw exceptions
        when(jdbcTemplate.queryForObject(argThat(sql -> sql != null && sql.contains("pg_enum")), eq(Boolean.class), any()))
                .thenThrow(new RuntimeException("Type check failed"));
        when(jdbcTemplate.queryForObject(argThat(sql -> sql != null && sql.contains("typtype = 'c'")), eq(Boolean.class), any()))
                .thenThrow(new RuntimeException("Type check failed"));

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("error_table"), eq("test_schema"), eq("error_table")))
                .thenAnswer(invocation -> {
                    RowMapper<?> rowMapper = invocation.getArgument(1);
                    return List.of(
                            createMockColumnResult(rowMapper, "id", "custom_type", "custom_type", false, true)
                    );
                });

        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq("test_schema"), eq("error_table")))
                .thenReturn(List.of());
        when(jdbcTemplate.queryForObject(anyString(), eq(Integer.class), eq("test_schema"), eq("error_table")))
                .thenReturn(0);

        // When: getting table schema with type enhancement errors
        Map<String, TableInfo> result = schemaService.getTableSchema();

        // Then: should handle gracefully and return fallback type
        TableInfo tableInfo = result.get("error_table");
        ColumnInfo idColumn = tableInfo.getColumns().stream()
                .filter(c -> "id".equals(c.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull(idColumn);
        assertEquals("custom_type", idColumn.getType());
    }

    // Helper methods
    private Object createMockColumnResult(RowMapper<?> rowMapper, String columnName, String dataType, String udtName, boolean isNullable, boolean isPrimaryKey) throws SQLException {
        ResultSet mockResultSet = mock(ResultSet.class);
        when(mockResultSet.getString("column_name")).thenReturn(columnName);
        when(mockResultSet.getString("full_type")).thenReturn(dataType);
        when(mockResultSet.getBoolean("is_nullable")).thenReturn(isNullable);
        when(mockResultSet.getBoolean("is_primary_key")).thenReturn(isPrimaryKey);

        return rowMapper.mapRow(mockResultSet, 0);
    }

    private Object createMockForeignKeyResult(RowMapper<?> rowMapper, String columnName, String referencedTable, String referencedColumn) throws SQLException {
        ResultSet mockResultSet = mock(ResultSet.class);
        when(mockResultSet.getString("column_name")).thenReturn(columnName);
        when(mockResultSet.getString("referenced_table")).thenReturn(referencedTable);
        when(mockResultSet.getString("referenced_column")).thenReturn(referencedColumn);

        return rowMapper.mapRow(mockResultSet, 0);
    }

    private void setPrivateField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @SuppressWarnings("unchecked")
    private <T> T getPrivateField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(target);
    }
}
