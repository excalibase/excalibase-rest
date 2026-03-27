package io.github.excalibase.postgres.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ComputedFieldFunction;
import io.github.excalibase.model.TableInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FunctionServiceTest {

    @Mock(lenient = true)
    private JdbcTemplate jdbcTemplate;

    @Mock(lenient = true)
    private NamedParameterJdbcTemplate namedJdbcTemplate;

    @Mock(lenient = true)
    private DatabaseSchemaService schemaService;

    private FunctionService functionService;

    @BeforeEach
    void setup() {
        functionService = new FunctionService(jdbcTemplate, namedJdbcTemplate, schemaService, 3600);
    }

    @Test
    void shouldDiscoverComputedFields() {
        // Given: database has computed field functions
        List<Map<String, Object>> functionRows = Arrays.asList(
            createFunctionRow("customer_full_name", "text", "customer", "s"),
            createFunctionRow("customer_age", "integer", "customer", "s")
        );
        when(namedJdbcTemplate.queryForList(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionRows);

        // When: discovering computed fields
        Map<String, List<ComputedFieldFunction>> result = functionService.discoverComputedFields("public");

        // Then: should discover functions
        assertNotNull(result);
        assertTrue(result.containsKey("customer"));
        List<ComputedFieldFunction> customerFunctions = result.get("customer");
        assertEquals(2, customerFunctions.size());

        // Verify function details
        ComputedFieldFunction fullNameFunc = customerFunctions.stream()
            .filter(f -> f.getFunctionName().equals("customer_full_name"))
            .findFirst()
            .orElse(null);
        assertNotNull(fullNameFunc);
        assertEquals("full_name", fullNameFunc.getFieldName());
        assertEquals("text", fullNameFunc.getReturnType());
        assertEquals("customer", fullNameFunc.getTableName());
    }

    @Test
    void shouldCacheDiscoveredFunctions() {
        // Given: database has functions
        List<Map<String, Object>> functionRows = Arrays.asList(
            createFunctionRow("customer_full_name", "text", "customer", "s")
        );
        when(namedJdbcTemplate.queryForList(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionRows);

        // When: discovering functions twice
        functionService.discoverComputedFields("public");
        functionService.discoverComputedFields("public");

        // Then: should only query database once (cached)
        verify(namedJdbcTemplate, times(1)).queryForList(anyString(), any(MapSqlParameterSource.class));
    }

    @Test
    void shouldInvalidateMetadataCache() {
        // Given: cached function metadata
        List<Map<String, Object>> functionRows = Arrays.asList(
            createFunctionRow("customer_full_name", "text", "customer", "s")
        );
        when(namedJdbcTemplate.queryForList(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionRows);

        functionService.discoverComputedFields("public");

        // When: invalidating cache
        functionService.invalidateMetadataCache();
        functionService.discoverComputedFields("public");

        // Then: should query database again
        verify(namedJdbcTemplate, times(2)).queryForList(anyString(), any(MapSqlParameterSource.class));
    }

    @Test
    void shouldAddComputedFieldsToRecord() {
        // Given: a table with computed fields
        setupComputedFieldMocks();

        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("first_name", "John");
        record.put("last_name", "Doe");

        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenReturn("John Doe"); // Computed full_name

        // When: adding computed fields
        Map<String, Object> result = functionService.addComputedFields("customer", record, "public");

        // Then: should add computed field to record
        assertNotNull(result);
        assertEquals("John Doe", result.get("full_name"));
        assertEquals("John", result.get("first_name")); // Original fields preserved
    }

    @Test
    void shouldCacheComputedFieldResultsPerRequest() {
        // Given: a table with computed fields
        setupComputedFieldMocks();

        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("first_name", "John");
        record.put("last_name", "Doe");

        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenReturn("John Doe");

        // When: adding computed fields twice for same record
        functionService.addComputedFields("customer", record, "public");
        functionService.addComputedFields("customer", record, "public");

        // Then: should only execute function once (per-request cache)
        verify(jdbcTemplate, times(1)).queryForObject(anyString(), any(Object[].class), eq(Object.class));
    }

    @Test
    void shouldClearPerRequestCache() {
        // Given: cached computed field results
        setupComputedFieldMocks();

        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("first_name", "John");
        record.put("last_name", "Doe");

        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenReturn("John Doe");

        functionService.addComputedFields("customer", record, "public");

        // When: clearing cache and calling again
        functionService.clearCache();
        functionService.addComputedFields("customer", record, "public");

        // Then: should execute function again
        verify(jdbcTemplate, times(2)).queryForObject(anyString(), any(Object[].class), eq(Object.class));
    }

    @Test
    void shouldHandleRecordWithoutPrimaryKey() {
        // Given: a table with computed fields but record has no PK
        setupComputedFieldMocks();

        Map<String, Object> record = new HashMap<>();
        record.put("first_name", "John");
        // No id (primary key)

        // When: adding computed fields
        Map<String, Object> result = functionService.addComputedFields("customer", record, "public");

        // Then: should return record unchanged (cannot compute without PK)
        assertNotNull(result);
        assertFalse(result.containsKey("full_name"));
    }

    @Test
    void shouldHandleTableWithNoComputedFields() {
        // Given: a table without computed fields
        when(namedJdbcTemplate.queryForList(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(Collections.emptyList());

        TableInfo tableInfo = createCustomerTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("customer", tableInfo));

        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);

        // When: adding computed fields
        Map<String, Object> result = functionService.addComputedFields("customer", record, "public");

        // Then: should return record unchanged
        assertNotNull(result);
        assertEquals(record, result);
    }

    @Test
    void shouldExecuteScalarRpcFunction() {
        // Given: a scalar function with signature
        Map<String, Object> functionInfo = Map.of(
            "proname", "calculate_tax",
            "return_type", "numeric",
            "pronargs", 2,
            "function_signature", "amount numeric, rate numeric"
        );
        when(namedJdbcTemplate.queryForMap(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionInfo);
        when(namedJdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Object.class)))
            .thenReturn(15.00);

        // When: executing RPC function
        Map<String, Object> params = Map.of("amount", 100.00, "rate", 0.15);
        Object result = functionService.executeRpc("calculate_tax", params, "public");

        // Then: should return scalar result
        assertNotNull(result);
        assertEquals(15.00, result);
    }

    @Test
    void shouldExecuteTableReturningRpcFunction() {
        // Given: a table-returning function with signature
        Map<String, Object> functionInfo = Map.of(
            "proname", "get_top_customers",
            "return_type", "customer", // Composite type
            "pronargs", 1,
            "function_signature", "limit_count integer"
        );
        when(namedJdbcTemplate.queryForMap(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionInfo);
        when(namedJdbcTemplate.queryForObject(
            contains("FROM pg_type"),
            any(MapSqlParameterSource.class),
            eq(Boolean.class)))
            .thenReturn(true); // Is composite type

        List<Map<String, Object>> mockResults = Arrays.asList(
            Map.of("customer_id", 1, "total_spent", 50000.00),
            Map.of("customer_id", 2, "total_spent", 45000.00)
        );
        when(namedJdbcTemplate.queryForList(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(mockResults);

        // When: executing table-returning RPC function
        Map<String, Object> params = Map.of("limit_count", 10);
        Object result = functionService.executeRpc("get_top_customers", params, "public");

        // Then: should return list of results
        assertNotNull(result);
        assertTrue(result instanceof List);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> resultList = (List<Map<String, Object>>) result;
        assertEquals(2, resultList.size());
    }

    @Test
    void shouldExecuteRpcWithNoParameters() {
        // Given: a function with no parameters
        Map<String, Object> functionInfo = Map.of(
            "proname", "get_dashboard_stats",
            "return_type", "json",
            "pronargs", 0,
            "function_signature", ""
        );
        when(namedJdbcTemplate.queryForMap(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionInfo);
        when(namedJdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Object.class)))
            .thenReturn("{\"total_orders\": 1500}");

        // When: executing RPC with null parameters
        Object result = functionService.executeRpc("get_dashboard_stats", null, "public");

        // Then: should execute successfully
        assertNotNull(result);
    }

    @Test
    void shouldHandleRpcExecutionError() {
        // Given: a function that throws error
        Map<String, Object> functionInfo = Map.of(
            "proname", "error_function",
            "return_type", "text"
        );
        when(namedJdbcTemplate.queryForMap(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionInfo);
        when(namedJdbcTemplate.queryForObject(anyString(), any(MapSqlParameterSource.class), eq(Object.class)))
            .thenThrow(new RuntimeException("Database error"));

        // When/Then: should propagate error
        assertThrows(RuntimeException.class, () -> {
            functionService.executeRpc("error_function", null, "public");
        });
    }

    @Test
    void shouldGetComputedFieldsForTable() {
        // Given: functions discovered for multiple tables
        List<Map<String, Object>> functionRows = Arrays.asList(
            createFunctionRow("customer_full_name", "text", "customer", "s"),
            createFunctionRow("order_total", "numeric", "orders", "s")
        );
        when(namedJdbcTemplate.queryForList(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionRows);

        // When: getting computed fields for specific table
        List<ComputedFieldFunction> result = functionService.getComputedFields("customer", "public");

        // Then: should return only customer functions
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("customer_full_name", result.get(0).getFunctionName());
    }

    @Test
    void shouldReturnEmptyListForTableWithoutFunctions() {
        // Given: no functions for table
        when(namedJdbcTemplate.queryForList(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(Collections.emptyList());

        // When: getting computed fields
        List<ComputedFieldFunction> result = functionService.getComputedFields("nonexistent", "public");

        // Then: should return empty list
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldHandleComputedFieldExecutionError() {
        // Given: a table with computed fields
        setupComputedFieldMocks();

        Map<String, Object> record = new HashMap<>();
        record.put("id", 1);
        record.put("first_name", "John");
        record.put("last_name", "Doe");

        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenThrow(new RuntimeException("Function error"));

        // When: adding computed fields with error
        Map<String, Object> result = functionService.addComputedFields("customer", record, "public");

        // Then: should set field to null on error
        assertNotNull(result);
        assertNull(result.get("full_name"));
    }

    @Test
    void shouldExtractFieldNameFromFunctionName() {
        // Given: function follows naming convention table_fieldname
        List<Map<String, Object>> functionRows = Arrays.asList(
            createFunctionRow("customer_full_name", "text", "customer", "s"),
            createFunctionRow("customer_display_info", "text", "customer", "s")
        );
        when(namedJdbcTemplate.queryForList(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionRows);

        // When: discovering functions
        Map<String, List<ComputedFieldFunction>> result = functionService.discoverComputedFields("public");

        // Then: should extract field names correctly
        List<ComputedFieldFunction> functions = result.get("customer");
        assertEquals("full_name", functions.get(0).getFieldName());
        assertEquals("display_info", functions.get(1).getFieldName());
    }

    // Helper methods

    private Map<String, Object> createFunctionRow(String functionName, String returnType,
                                                   String tableName, String volatility) {
        Map<String, Object> row = new HashMap<>();
        row.put("function_name", functionName);
        row.put("return_type", returnType);
        row.put("param_table", tableName);
        row.put("volatility", volatility);
        return row;
    }

    private void setupComputedFieldMocks() {
        List<Map<String, Object>> functionRows = Arrays.asList(
            createFunctionRow("customer_full_name", "text", "customer", "s")
        );
        when(namedJdbcTemplate.queryForList(anyString(), any(MapSqlParameterSource.class)))
            .thenReturn(functionRows);

        TableInfo tableInfo = createCustomerTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("customer", tableInfo));
    }

    private TableInfo createCustomerTableInfo() {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setName("customer");

        List<ColumnInfo> columns = new ArrayList<>();

        // id column (primary key)
        ColumnInfo idCol = new ColumnInfo();
        idCol.setName("id");
        idCol.setType("integer");
        idCol.setNullable(false);
        idCol.setPrimaryKey(true);
        columns.add(idCol);

        // first_name column
        ColumnInfo firstNameCol = new ColumnInfo();
        firstNameCol.setName("first_name");
        firstNameCol.setType("text");
        firstNameCol.setNullable(false);
        columns.add(firstNameCol);

        // last_name column
        ColumnInfo lastNameCol = new ColumnInfo();
        lastNameCol.setName("last_name");
        lastNameCol.setType("text");
        lastNameCol.setNullable(false);
        columns.add(lastNameCol);

        tableInfo.setColumns(columns);
        return tableInfo;
    }

    @Test
    void executeRpc_rejectsSqlInjectionInFunctionName() {
        // A function name with SQL injection characters should be rejected
        assertThrows(IllegalArgumentException.class, () ->
                functionService.executeRpc("drop_table\"; --", Map.of(), "public"));
    }
}
