package io.github.excalibase.postgres.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AggregationServiceTest {

    @Mock(lenient = true)
    private JdbcTemplate jdbcTemplate;

    @Mock(lenient = true)
    private DatabaseSchemaService schemaService;

    @Mock(lenient = true)
    private ValidationService validationService;

    @Mock(lenient = true)
    private io.github.excalibase.service.FilterService filterService;

    private AggregationService aggregationService;

    @BeforeEach
    void setup() {
        aggregationService = new AggregationService(jdbcTemplate, schemaService, validationService, filterService);

        // Mock permission checks
        doNothing().when(validationService).validateTablePermission(anyString(), anyString());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldCountAllRecords() {
        // Given: a valid table with records
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Integer.class))).thenReturn(150);

        // When: getting count aggregate
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters, List.of("count"), null);

        // Then: should return count
        assertNotNull(result);
        assertEquals(150, result.get("count"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSumNumericColumns() {
        // Given: a table with numeric columns
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenReturn(45000.50); // total_amount sum

        // When: getting sum aggregate
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters,
            List.of("sum"), List.of("total_amount"));

        // Then: should return sum for numeric column
        assertNotNull(result);
        assertTrue(result.containsKey("sum"));
        @SuppressWarnings("unchecked")
        Map<String, Object> sumResult = (Map<String, Object>) result.get("sum");
        assertEquals(45000.50, sumResult.get("total_amount"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldComputeAverageForNumericColumns() {
        // Given: a table with numeric columns
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenReturn(300.00); // avg total_amount

        // When: getting avg aggregate
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters,
            List.of("avg"), List.of("total_amount"));

        // Then: should return average
        assertNotNull(result);
        assertTrue(result.containsKey("avg"));
        @SuppressWarnings("unchecked")
        Map<String, Object> avgResult = (Map<String, Object>) result.get("avg");
        assertEquals(300.00, avgResult.get("total_amount"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldComputeMinMaxForComparableColumns() {
        // Given: a table with comparable columns
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenReturn(10.00) // min
            .thenReturn(5000.00); // max

        // When: getting min/max aggregates
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters,
            List.of("min", "max"), List.of("total_amount"));

        // Then: should return min and max
        assertNotNull(result);
        assertTrue(result.containsKey("min"));
        assertTrue(result.containsKey("max"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleMultipleAggregatesSimultaneously() {
        // Given: a valid table
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        // Mock different aggregate results
        when(jdbcTemplate.queryForObject(contains("COUNT"), any(Object[].class), eq(Integer.class)))
            .thenReturn(150);
        when(jdbcTemplate.queryForObject(contains("SUM"), any(Object[].class), eq(Object.class)))
            .thenReturn(45000.50);
        when(jdbcTemplate.queryForObject(contains("AVG"), any(Object[].class), eq(Object.class)))
            .thenReturn(300.00);

        // When: getting multiple aggregates
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters,
            List.of("count", "sum", "avg"), null);

        // Then: should return all requested aggregates
        assertNotNull(result);
        assertEquals(150, result.get("count"));
        assertTrue(result.containsKey("sum"));
        assertTrue(result.containsKey("avg"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleEmptyFunctionsList() {
        // Given: a valid table with no specific functions requested
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        when(jdbcTemplate.queryForObject(contains("COUNT"), any(Object[].class), eq(Integer.class)))
            .thenReturn(150);
        when(jdbcTemplate.queryForObject(contains("SUM"), any(Object[].class), eq(Object.class)))
            .thenReturn(45000.50);
        when(jdbcTemplate.queryForObject(contains("AVG"), any(Object[].class), eq(Object.class)))
            .thenReturn(300.00);

        // When: getting aggregates with null functions (all functions)
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters, null, null);

        // Then: should compute all default aggregates
        assertNotNull(result);
        assertTrue(result.containsKey("count"));
        assertTrue(result.containsKey("sum"));
        assertTrue(result.containsKey("avg"));
        assertTrue(result.containsKey("min"));
        assertTrue(result.containsKey("max"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldThrowExceptionForNonExistentTable() {
        // Given: a non-existent table
        when(schemaService.getTableSchema()).thenReturn(Map.of());

        // When/Then: should throw exception
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        assertThrows(IllegalArgumentException.class, () -> {
            aggregationService.getAggregates("nonexistent_table", filters, null, null);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldValidateTablePermissions() {
        // Given: a valid table
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        doThrow(new IllegalArgumentException("Permission denied"))
            .when(validationService).validateTablePermission("orders", "SELECT");

        // When/Then: should validate permissions
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        assertThrows(IllegalArgumentException.class, () -> {
            aggregationService.getAggregates("orders", filters, null, null);
        });

        verify(validationService).validateTablePermission("orders", "SELECT");
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleSpecificColumnsOnly() {
        // Given: a table with multiple numeric columns
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenReturn(45000.50);

        // When: aggregating only specific columns
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters,
            List.of("sum"), List.of("total_amount")); // Only total_amount, not tax

        // Then: should only aggregate specified column
        assertNotNull(result);
        @SuppressWarnings("unchecked")
        Map<String, Object> sumResult = (Map<String, Object>) result.get("sum");
        assertTrue(sumResult.containsKey("total_amount"));
        assertEquals(1, sumResult.size()); // Only one column
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSkipNonNumericColumnsForSum() {
        // Given: a table with both numeric and non-numeric columns
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        // When: trying to sum all columns (including non-numeric)
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters,
            List.of("sum"), null);

        // Then: should only sum numeric columns (status is text, should be skipped)
        assertNotNull(result);
        @SuppressWarnings("unchecked")
        Map<String, Object> sumResult = (Map<String, Object>) result.get("sum");
        // Should contain total_amount and tax (numeric), but not status (text)
        assertFalse(sumResult.containsKey("status"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleQueryExecutionErrors() {
        // Given: a valid table but database error
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Integer.class)))
            .thenThrow(new RuntimeException("Database error"));

        // When: executing aggregate query
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();

        // Then: should handle error gracefully (returns 0 for count)
        Map<String, Object> result = aggregationService.getAggregates("orders", filters,
            List.of("count"), null);

        assertNotNull(result);
        assertEquals(0, result.get("count")); // Service returns 0 on error
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleAggregateErrorsGracefully() {
        // Given: table with error on aggregate computation
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenThrow(new RuntimeException("Aggregate error"));

        // When: computing sum with error
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters,
            List.of("sum"), List.of("total_amount"));

        // Then: should return null for errored column
        assertNotNull(result);
        @SuppressWarnings("unchecked")
        Map<String, Object> sumResult = (Map<String, Object>) result.get("sum");
        assertNull(sumResult.get("total_amount"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldExecuteInlineAggregateWithCountOnly() {
        // Given: a valid table
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        List<Map<String, Object>> mockResult = List.of(Map.of("count", 150));
        when(jdbcTemplate.query(anyString(), any(Object[].class), org.mockito.ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<Map<String, Object>>>any()))
            .thenReturn(mockResult);

        // When: executing inline aggregate with count()
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        List<Map<String, Object>> result = aggregationService.getInlineAggregates(
            "orders", "count()", filters, null);

        // Then: should return count result
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(150, result.get(0).get("count"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldExecuteInlineAggregateWithColumnFunction() {
        // Given: a valid table
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        List<Map<String, Object>> mockResult = List.of(Map.of("sum", 45000.50));
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.RowMapper.class)))
            .thenReturn(mockResult);

        // When: executing inline aggregate with column.sum()
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        List<Map<String, Object>> result = aggregationService.getInlineAggregates(
            "orders", "total_amount.sum()", filters, null);

        // Then: should return sum result
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(45000.50, result.get(0).get("sum"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldExecuteInlineAggregateWithGroupBy() {
        // Given: a valid table
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        List<Map<String, Object>> mockResult = Arrays.asList(
            Map.of("status", "completed", "count", 100),
            Map.of("status", "pending", "count", 50)
        );
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.RowMapper.class)))
            .thenReturn(mockResult);

        // When: executing with regular column (GROUP BY)
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        List<Map<String, Object>> result = aggregationService.getInlineAggregates(
            "orders", "status,count()", filters, null);

        // Then: should return grouped results
        assertNotNull(result);
        assertEquals(2, result.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldExecuteInlineAggregateWithMultipleFunctions() {
        // Given: a valid table
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        List<Map<String, Object>> mockResult = List.of(
            Map.of("sum", 45000.50, "avg", 300.00, "count", 150)
        );
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.RowMapper.class)))
            .thenReturn(mockResult);

        // When: executing with multiple aggregates
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        List<Map<String, Object>> result = aggregationService.getInlineAggregates(
            "orders", "total_amount.sum(),total_amount.avg(),count()", filters, null);

        // Then: should return all aggregates
        assertNotNull(result);
        assertEquals(1, result.size());
        Map<String, Object> row = result.get(0);
        assertTrue(row.containsKey("sum"));
        assertTrue(row.containsKey("avg"));
        assertTrue(row.containsKey("count"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleEmptySelectParameter() {
        // Given: a valid table
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        List<Map<String, Object>> mockResult = Collections.emptyList();
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.RowMapper.class)))
            .thenReturn(mockResult);

        // When: executing with empty select
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        List<Map<String, Object>> result = aggregationService.getInlineAggregates(
            "orders", "", filters, null);

        // Then: should return empty result
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleNullSelectParameter() {
        // Given: a valid table
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        List<Map<String, Object>> mockResult = Collections.emptyList();
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.RowMapper.class)))
            .thenReturn(mockResult);

        // When: executing with null select
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        List<Map<String, Object>> result = aggregationService.getInlineAggregates(
            "orders", null, filters, null);

        // Then: should return empty result
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldIgnoreInvalidAggregateExpressions() {
        // Given: a valid table
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        List<Map<String, Object>> mockResult = List.of(Map.of("count", 150));
        when(jdbcTemplate.query(anyString(), any(Object[].class), org.mockito.ArgumentMatchers.<org.springframework.jdbc.core.RowMapper<Map<String, Object>>>any()))
            .thenReturn(mockResult);

        // When: executing with invalid expressions mixed with valid ones
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        List<Map<String, Object>> result = aggregationService.getInlineAggregates(
            "orders", "invalid.function(),count(),nonexistent_column", filters, null);

        // Then: should only process valid expressions
        assertNotNull(result);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldThrowExceptionOnInlineAggregateQueryError() {
        // Given: table that throws error during query execution
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.RowMapper.class)))
            .thenThrow(new RuntimeException("Query execution failed"));

        // When/Then: should throw RuntimeException
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        assertThrows(RuntimeException.class, () -> {
            aggregationService.getInlineAggregates("orders", "count()", filters, null);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleComparableTypesForMinMax() {
        // Given: table with date and string columns
        TableInfo tableInfo = createOrdersTableWithDatesAndStrings();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Object.class)))
            .thenReturn("2024-01-01") // min date
            .thenReturn("2024-12-31") // max date
            .thenReturn("AAA")        // min status
            .thenReturn("ZZZ");       // max status

        // When: computing min/max on date and string columns
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        Map<String, Object> result = aggregationService.getAggregates("orders", filters,
            List.of("min", "max"), List.of("created_at", "status"));

        // Then: should compute min/max for comparable types
        assertNotNull(result);
        assertTrue(result.containsKey("min"));
        assertTrue(result.containsKey("max"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldHandleFiltersInInlineAggregates() {
        // Given: a valid table with filters
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));

        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("status", "eq.completed");

        List<Map<String, Object>> mockResult = List.of(Map.of("count", 100));
        when(jdbcTemplate.query(anyString(), any(Object[].class), any(org.springframework.jdbc.core.RowMapper.class)))
            .thenReturn(mockResult);

        // When: executing with filters
        List<Map<String, Object>> result = aggregationService.getInlineAggregates(
            "orders", "count()", filters, null);

        // Then: should apply filters
        assertNotNull(result);
        assertEquals(1, result.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldThrowExceptionForInlineAggregateOnNonExistentTable() {
        // Given: non-existent table
        when(schemaService.getTableSchema()).thenReturn(Map.of());

        // When/Then: should throw exception
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        assertThrows(IllegalArgumentException.class, () -> {
            aggregationService.getInlineAggregates("nonexistent", "count()", filters, null);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldValidatePermissionsForInlineAggregates() {
        // Given: table with permission denied
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        doThrow(new IllegalArgumentException("Permission denied"))
            .when(validationService).validateTablePermission("orders", "SELECT");

        // When/Then: should validate permissions
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        assertThrows(IllegalArgumentException.class, () -> {
            aggregationService.getInlineAggregates("orders", "count()", filters, null);
        });
    }

    // Helper method to create test table info
    private TableInfo createOrdersTableInfo() {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setName("orders");

        List<ColumnInfo> columns = new ArrayList<>();

        // id column (primary key)
        ColumnInfo idCol = new ColumnInfo();
        idCol.setName("id");
        idCol.setType("integer");
        idCol.setNullable(false);
        idCol.setPrimaryKey(true);
        columns.add(idCol);

        // total_amount column (numeric)
        ColumnInfo amountCol = new ColumnInfo();
        amountCol.setName("total_amount");
        amountCol.setType("numeric");
        amountCol.setNullable(false);
        columns.add(amountCol);

        // tax column (numeric)
        ColumnInfo taxCol = new ColumnInfo();
        taxCol.setName("tax");
        taxCol.setType("numeric");
        taxCol.setNullable(true);
        columns.add(taxCol);

        // status column (text - not numeric)
        ColumnInfo statusCol = new ColumnInfo();
        statusCol.setName("status");
        statusCol.setType("text");
        statusCol.setNullable(false);
        columns.add(statusCol);

        tableInfo.setColumns(columns);
        return tableInfo;
    }

    private TableInfo createOrdersTableWithDatesAndStrings() {
        TableInfo tableInfo = new TableInfo();
        tableInfo.setName("orders");

        List<ColumnInfo> columns = new ArrayList<>();

        // id column (primary key)
        ColumnInfo idCol = new ColumnInfo();
        idCol.setName("id");
        idCol.setType("integer");
        idCol.setNullable(false);
        idCol.setPrimaryKey(true);
        columns.add(idCol);

        // created_at column (timestamp - comparable)
        ColumnInfo createdAtCol = new ColumnInfo();
        createdAtCol.setName("created_at");
        createdAtCol.setType("timestamp");
        createdAtCol.setNullable(false);
        columns.add(createdAtCol);

        // status column (text - comparable)
        ColumnInfo statusCol = new ColumnInfo();
        statusCol.setName("status");
        statusCol.setType("text");
        statusCol.setNullable(false);
        columns.add(statusCol);

        tableInfo.setColumns(columns);
        return tableInfo;
    }

    @Test
    void shouldApplyFiltersToAggregateQuery() {
        // Given: a table with filters applied
        TableInfo tableInfo = createOrdersTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("orders", tableInfo));
        when(jdbcTemplate.queryForObject(anyString(), any(Object[].class), eq(Integer.class))).thenReturn(42);

        // Mock FilterService to return a condition for the filter
        when(filterService.parseFilters(any(), any(), any()))
                .thenReturn(List.of("\"status\" = ?"));

        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("status", "eq.shipped");

        // When: getting count with filters
        Map<String, Object> result = aggregationService.getAggregates("orders", filters, List.of("count"), null);

        // Then: the SQL should contain a WHERE clause with the filter condition
        verify(jdbcTemplate).queryForObject(argThat(sql ->
                sql.contains("WHERE") && sql.contains("\"status\" = ?")
        ), any(Object[].class), eq(Integer.class));
    }
}
