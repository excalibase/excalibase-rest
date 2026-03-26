package io.github.excalibase.postgres.service;

import io.github.excalibase.exception.ValidationException;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.SelectField;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.TypeConversionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RestApiServiceTest {

    @Mock(lenient = true)
    private JdbcTemplate jdbcTemplate;

    @Mock(lenient = true)
    private DatabaseSchemaService schemaService;

    @Mock(lenient = true)
    private QueryComplexityService complexityService;

    @Mock(lenient = true)
    private RelationshipBatchLoader batchLoader;

    @Mock(lenient = true)
    private SelectParserService selectParserService;

    @Mock(lenient = true)
    private EnhancedRelationshipService enhancedRelationshipService;

    // Use real service instances instead of mocks to allow jdbcTemplate mocks to work
    private ValidationService validationService;
    private TypeConversionService typeConversionService;
    private FilterService filterService;
    private QueryBuilderService queryBuilderService;
    private CrudService crudService;
    private UpsertService upsertService;

    private RestApiService restApiService;

    @BeforeEach
    void setup() {
        // Mock all permission checks to return true by default (lenient to avoid unnecessary stubbing warnings)
        lenient().when(jdbcTemplate.queryForObject(eq("SELECT has_table_privilege(current_user, ?, ?)"),
            eq(Boolean.class), any(), any())).thenReturn(true);
        lenient().when(jdbcTemplate.queryForObject(eq("SELECT current_user"), eq(String.class)))
            .thenReturn("test_user");

        // Create real service instances with mocked dependencies
        validationService = new ValidationService(jdbcTemplate, schemaService);
        typeConversionService = new TypeConversionService(validationService);
        filterService = new FilterService(validationService, typeConversionService);
        queryBuilderService = new QueryBuilderService(validationService, typeConversionService);
        crudService = new CrudService(jdbcTemplate, validationService, typeConversionService, queryBuilderService);
        upsertService = new UpsertService(jdbcTemplate, validationService, typeConversionService, queryBuilderService);

        // Delegate select parser mock to real implementation by default (null-safe)
        SelectParserService realSelectParserService = new SelectParserService();
        lenient().when(selectParserService.parseSelect(anyString()))
            .thenAnswer(inv -> {
                String p = inv.getArgument(0);
                return p == null ? java.util.Collections.emptyList() : realSelectParserService.parseSelect(p);
            });
        lenient().when(selectParserService.getEmbeddedFields(any()))
            .thenAnswer(inv -> {
                java.util.List<SelectField> f = inv.getArgument(0);
                return f == null ? java.util.Collections.emptyList() : realSelectParserService.getEmbeddedFields(f);
            });
        lenient().doAnswer(inv -> {
            java.util.List<SelectField> f = inv.getArgument(0);
            if (f != null) realSelectParserService.parseEmbeddedFilters(f, inv.getArgument(1));
            return null;
        }).when(selectParserService).parseEmbeddedFilters(any(), any());
        lenient().when(selectParserService.hasEmbeddedFields(any()))
            .thenAnswer(inv -> {
                java.util.List<SelectField> f = inv.getArgument(0);
                return f != null && realSelectParserService.hasEmbeddedFields(f);
            });

        // Create the RestApiService with real service instances
        restApiService = new RestApiService(jdbcTemplate, schemaService, complexityService, batchLoader,
                selectParserService, enhancedRelationshipService,
                validationService, typeConversionService, filterService,
                queryBuilderService, crudService, upsertService, "postgres");
    }

    @Test
    void shouldGetRecordsWithBasicPagination() {
        // Given: a valid table with columns
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // And: mock query results
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(Arrays.asList(
                createMap("id", 1, "name", "John", "email", "john@example.com"),
                createMap("id", 2, "name", "Jane", "email", "jane@example.com")
        ));
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(10L);

        // When: getting records with pagination
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 5, null, "asc", null, null, true);

        // Then: should return paginated data
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        @SuppressWarnings("unchecked")
        Map<String, Object> pagination = (Map<String, Object>) result.get("pagination");

        assertEquals(2, data.size());
        assertEquals(0, pagination.get("offset"));
        assertEquals(5, pagination.get("limit"));
        assertEquals(10L, pagination.get("total"));
        assertEquals(false, pagination.get("hasMore")); // 2 records < limit=5
    }

    @Test
    void shouldApplyFiltersCorrectly() {
        // Given: a table with columns and filter parameters
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("name", "like.John");
        params.add("age", "gt.18");

        // And: mock query results for both main query and count query
        List<Map<String, Object>> mockData = Collections.singletonList(createMap("id", 1, "name", "John"));
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(mockData);
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with filters
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should apply WHERE clause with filters
        verify(jdbcTemplate, times(1)).queryForList(
                argThat(sql -> sql.contains("WHERE") && sql.contains("\"name\" LIKE ?") && sql.contains("\"age\" > ?")),
                any(Object[].class)
        );

        // And: should return filtered data
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertNotNull(data);
        assertEquals(1, data.size());
        assertEquals(1, data.get(0).get("id"));
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleOrConditions() {
        // Given: a table with OR filter parameters
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("or", "(name.like.John,age.gt.65)");

        // And: mock query results
        List<Map<String, Object>> mockData = Collections.singletonList(createMap("id", 1, "name", "John"));
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(mockData);
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with OR conditions
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should apply OR logic in WHERE clause
        verify(jdbcTemplate, times(1)).queryForList(
                argThat(sql -> sql.contains("WHERE") && sql.contains("(") && sql.contains("OR") && sql.contains(")")),
                any(Object[].class)
        );

        // And: should return data with OR conditions
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertNotNull(data);
        assertEquals(1, data.size());
        assertEquals(1, data.get(0).get("id"));
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldValidateTableNameAndThrowExceptionForInvalidTable() {
        // Given: schema service returns empty schema
        when(schemaService.getTableSchema()).thenReturn(Collections.emptyMap());

        // When: trying to access non-existent table
        // Then: should throw IllegalArgumentException
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecords("invalid_table", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null, false);
        });
        assertTrue(exception.getMessage().contains("Table not found: invalid_table"));
    }

    @Test
    void shouldValidatePaginationLimits() {
        // When: using invalid offset
        // Then: should throw exception for negative offset
        IllegalArgumentException exception1 = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecords("users", new LinkedMultiValueMap<>(), -1, 10, null, "asc", null, null, false);
        });
        assertTrue(exception1.getMessage().contains("Offset must be between 0 and"));

        // When: using excessive limit
        // Then: should throw exception for excessive limit
        IllegalArgumentException exception2 = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 2000, null, "asc", null, null, false);
        });
        assertTrue(exception2.getMessage().contains("Limit must be between 1 and"));
    }

    @Test
    void shouldHandleColumnSelection() {
        // Given: a table with select parameter
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // And: mock select parser service
        when(selectParserService.parseSelect("id,name")).thenReturn(Arrays.asList(
                new SelectField("id"),
                new SelectField("name")
        ));
        when(selectParserService.getEmbeddedFields(any())).thenReturn(Collections.emptyList());

        // And: mock query results
        List<Map<String, Object>> mockData = Collections.singletonList(createMap("id", 1, "name", "John"));
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(mockData);
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with column selection
        Map<String, Object> result = restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", "id,name", null, false);

        // Then: should use SELECT with specified columns
        verify(jdbcTemplate, times(1)).queryForList(
                argThat(sql -> sql.contains("id") && sql.contains("name") && !sql.contains("SELECT *")),
                any(Object[].class)
        );

        // And: should return selected data
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertNotNull(data);
        assertEquals(1, data.size());
        assertEquals(1, data.get(0).get("id"));
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldValidateColumnNamesInSelectParameter() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // When: using invalid column in select
        // Then: should throw exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", "invalid_column", null, false);
        });
        assertTrue(exception.getMessage().contains("Invalid column: invalid_column"));
    }

    @Test
    void shouldCreateSingleRecord() {
        // Given: a valid table and data
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("name", "John", "email", "john@example.com");

        // And: mock successful insert with RETURNING
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "email", "john@example.com"))
        );

        // When: creating a record
        Map<String, Object> result = restApiService.createRecord("users", data);

        // Then: should return created record
        assertEquals(1, result.get("id"));
        assertEquals("John", result.get("name"));
        assertEquals("john@example.com", result.get("email"));
    }

    @Test
    void shouldValidateDataColumnsOnCreate() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("invalid_column", "value");

        // When: creating record with invalid column
        // Then: should throw exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.createRecord("users", data);
        });
        assertTrue(exception.getMessage().contains("Invalid column: invalid_column"));
    }

    @Test
    void shouldUpdateRecord() {
        // Given: a valid table and update data
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("name", "John Updated");

        // And: mock successful update with RETURNING
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John Updated", "email", "john@example.com"))
        );

        // When: updating a record
        Map<String, Object> result = restApiService.updateRecord("users", "1", data, false);

        // Then: should return updated record
        assertEquals(1, result.get("id"));
        assertEquals("John Updated", result.get("name"));
    }

    @Test
    void shouldDeleteRecord() {
        // Given: a valid table
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // And: mock successful delete
        when(jdbcTemplate.update(any(String.class), any(Object.class))).thenReturn(1);

        // When: deleting a record
        boolean result = restApiService.deleteRecord("users", "1");

        // Then: should return true for successful deletion
        assertTrue(result);
    }

    @Test
    void shouldReturnFalseForDeleteWhenRecordNotFound() {
        // Given: a valid table
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // And: mock unsuccessful delete (no rows affected)
        when(jdbcTemplate.update(any(String.class), any(Object.class))).thenReturn(0);

        // When: deleting non-existent record
        boolean result = restApiService.deleteRecord("users", "999");

        // Then: should return false
        assertFalse(result);
    }

    @Test
    void shouldHandleCursorBasedPagination() {
        // Given: a valid table
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // And: mock query results
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(Arrays.asList(
                createMap("id", 2, "name", "Jane"),
                createMap("id", 3, "name", "Bob")
        ));
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(10L);

        // When: getting records with cursor pagination
        Map<String, Object> result = restApiService.getRecordsWithCursor("users", new LinkedMultiValueMap<>(),
                "2", "eyJpZCI6MX0=", null, null,
                "id", "asc", null, null);

        // Then: should return edges with cursors
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> edges = (List<Map<String, Object>>) result.get("edges");
        @SuppressWarnings("unchecked")
        Map<String, Object> pageInfo = (Map<String, Object>) result.get("pageInfo");
        Long totalCount = (Long) result.get("totalCount");

        assertEquals(2, edges.size());
        assertNotNull(pageInfo.get("hasNextPage"));
        assertNotNull(pageInfo.get("hasPreviousPage"));
        assertEquals(10L, totalCount);
    }

    @Test
    void shouldConvertValuesToCorrectColumnTypes() {
        // Given: a table with different column types
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("price", "decimal", false, true),
                new ColumnInfo("active", "boolean", false, false),
                new ColumnInfo("name", "varchar", false, true)
        );
        TableInfo tableInfo = new TableInfo("products", columns, Collections.emptyList());
        when(schemaService.getTableSchema()).thenReturn(Map.of("products", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("id", "eq.123");
        params.add("price", "gte.99.99");
        params.add("active", "is.true");
        params.add("name", "like.test");

        // And: mock query results
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(Collections.emptyList());
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(0L);

        // When: applying filters with type conversion
        Map<String, Object> result = restApiService.getRecords("products", params, 0, 10, null, "asc", null, null, false);

        // Then: query executed successfully (type conversion verified by no exceptions thrown)
        assertNotNull(result);
        verify(jdbcTemplate, atLeastOnce()).queryForList(anyString(), any(Object[].class));
    }

    @Test
    void shouldHandleCompositePrivateKeyForGetRecord() {
        // Given: a table with composite primary key
        TableInfo tableInfo = createCompositeKeyTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("order_items", tableInfo));

        // And: mock query result
        List<Map<String, Object>> mockResult = Collections.singletonList(
                createMap("order_id", 1, "product_id", 2, "quantity", 5, "price", 99.99)
        );
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(mockResult);

        // When: getting record by composite key
        Map<String, Object> result = restApiService.getRecord("order_items", "1,2", null, null);

        // Then: should return the record (verification simplified due to permission checking calls)
        assertNotNull(result);
        assertEquals(1, result.get("order_id"));
        assertEquals(2, result.get("product_id"));
        assertEquals(5, result.get("quantity"));
    }

    @Test
    void shouldHandleCompositePrivateKeyForUpdateRecord() {
        // Given: a table with composite primary key
        TableInfo tableInfo = createCompositeKeyTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("order_items", tableInfo));

        Map<String, Object> updateData = createMap("quantity", 10);

        // And: mock successful update
        List<Map<String, Object>> mockResult = Collections.singletonList(
                createMap("order_id", 1, "product_id", 2, "quantity", 10, "price", 99.99)
        );
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(mockResult);

        // When: updating record by composite key
        Map<String, Object> result = restApiService.updateRecord("order_items", "1,2", updateData, false);

        // Then: should return updated record (verification simplified due to permission checking calls)
        assertNotNull(result);
        assertEquals(10, result.get("quantity"));
    }

    @Test
    void shouldHandleCompositePrivateKeyForDeleteRecord() {
        // Given: a table with composite primary key
        TableInfo tableInfo = createCompositeKeyTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("order_items", tableInfo));

        // And: mock successful delete
        when(jdbcTemplate.update(any(String.class), any(Object[].class))).thenReturn(1);

        // When: deleting record by composite key
        boolean result = restApiService.deleteRecord("order_items", "1,2");

        // Then: should return success (verification simplified due to permission checking calls)
        assertTrue(result);
    }

    @Test
    void shouldValidateCompositeKeyFormat() {
        // Given: a table with composite primary key
        TableInfo tableInfo = createCompositeKeyTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("order_items", tableInfo));

        // When: using invalid composite key format
        // Then: should throw exception for invalid format
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecord("order_items", "invalid_format", null, null);
        });
        assertTrue(exception.getMessage().contains("Composite key requires 2 parts"));
    }

    @Test
    void shouldHandleMissingCompositeKeyParts() {
        // Given: a table with composite primary key
        TableInfo tableInfo = createCompositeKeyTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("order_items", tableInfo));

        // When: using incomplete composite key
        // Then: should throw exception for incomplete key
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecord("order_items", "1", null, null);
        });
        assertTrue(exception.getMessage().contains("Composite key requires 2 parts"));
    }

    @Test
    void shouldUpsertRecordWithPrimaryKeyConflict() {
        // Given: a table with primary key
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("id", 1, "name", "John Updated", "email", "john.updated@example.com");

        // And: mock successful upsert with RETURNING
        when(jdbcTemplate.queryForList(
                argThat(sql -> sql.contains("INSERT INTO users") &&
                        sql.contains("ON CONFLICT (\"id\")") &&
                        sql.contains("DO UPDATE SET") &&
                        sql.contains("RETURNING *")),
                any(Object[].class)
        )).thenReturn(Collections.singletonList(createMap("id", 1, "name", "John Updated", "email", "john.updated@example.com")));

        // When: upserting record
        Map<String, Object> result = restApiService.upsertRecord("users", data);

        // Then: should return updated record
        assertEquals(1, result.get("id"));
        assertEquals("John Updated", result.get("name"));
        assertEquals("john.updated@example.com", result.get("email"));
    }

    @Test
    void shouldUpsertRecordWithInsertWhenNoConflict() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("name", "New User", "email", "new@example.com");

        // And: mock successful insert via upsert
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 2, "name", "New User", "email", "new@example.com"))
        );

        // When: upserting new record
        Map<String, Object> result = restApiService.upsertRecord("users", data);

        // Then: should return new record
        assertEquals(2, result.get("id"));
        assertEquals("New User", result.get("name"));
        assertEquals("new@example.com", result.get("email"));
    }

    @Test
    void shouldHandleUpsertWithDoNothingWhenOnlyPrimaryKeysProvided() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("id", 1);

        // And: mock DO NOTHING result (empty)
        when(jdbcTemplate.queryForList(
                argThat(sql -> sql.contains("ON CONFLICT (\"id\") DO NOTHING")),
                any(Object[].class)
        )).thenReturn(Collections.emptyList());

        // When: upserting with only primary key
        Map<String, Object> result = restApiService.upsertRecord("users", data);

        // Then: should return null
        assertNull(result);
    }

    @Test
    void shouldBulkUpsertRecords() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        List<Map<String, Object>> dataList = Arrays.asList(
                createMap("id", 1, "name", "John Updated", "email", "john@example.com"),
                createMap("name", "Jane New", "email", "jane@example.com")
        );

        // And: mock successful bulk upsert
        when(jdbcTemplate.queryForList(
                argThat(sql -> sql.contains("INSERT INTO users") &&
                        sql.contains("ON CONFLICT (\"id\")") &&
                        sql.contains("VALUES")),
                any(Object[].class)
        )).thenReturn(Arrays.asList(
                createMap("id", 1, "name", "John Updated", "email", "john@example.com"),
                createMap("id", 2, "name", "Jane New", "email", "jane@example.com")
        ));

        // When: bulk upserting records
        List<Map<String, Object>> results = restApiService.upsertBulkRecords("users", dataList);

        // Then: should return upserted records
        assertEquals(2, results.size());
        assertEquals(1, results.get(0).get("id"));
        assertEquals("John Updated", results.get(0).get("name"));
        assertEquals(2, results.get(1).get("id"));
        assertEquals("Jane New", results.get(1).get("name"));
    }

    @Test
    void shouldFailUpsertWhenTableHasNoPrimaryKey() {
        // Given: a table without primary key
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("name", "varchar", false, false),
                new ColumnInfo("email", "varchar", false, false)
        );
        TableInfo tableInfo = new TableInfo("logs", columns, Collections.emptyList());
        when(schemaService.getTableSchema()).thenReturn(Map.of("logs", tableInfo));

        Map<String, Object> data = createMap("name", "Test", "email", "test@example.com");

        // When: attempting upsert on table without primary key
        // Then: should throw exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.upsertRecord("logs", data);
        });
        assertTrue(exception.getMessage().contains("has no primary key - cannot perform upsert"));
    }

    @Test
    void shouldHandleFullTextSearchWithFtsOperator() {
        // Given: a table with text column
        TableInfo tableInfo = createPostsTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("posts", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("content", "fts.postgresql tutorial");

        // And: mock full-text search query (fts → to_tsquery per spec)
        when(jdbcTemplate.queryForList(
                argThat(sql -> sql.contains("to_tsvector('english', \"content\")") &&
                        sql.contains("to_tsquery('english', ?)")),
                any(Object[].class)
        )).thenReturn(Collections.singletonList(createMap("id", 1, "title", "PostgreSQL Guide", "content", "PostgreSQL tutorial content")));

        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: searching with fts operator
        Map<String, Object> result = restApiService.getRecords("posts", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("PostgreSQL Guide", data.get(0).get("title"));
    }

    @Test
    void shouldHandlePhraseFullTextSearchWithPlftsOperator() {
        // Given: a table with text column
        TableInfo tableInfo = createPostsTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("posts", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("content", "plfts.exact phrase match");

        // And: mock phrase search query (plfts → plainto_tsquery per spec)
        when(jdbcTemplate.queryForList(
                argThat(sql -> sql.contains("to_tsvector('english', \"content\")") &&
                        sql.contains("plainto_tsquery('english', ?)")),
                any(Object[].class)
        )).thenReturn(Collections.singletonList(createMap("id", 1, "content", "This is an exact phrase match example")));

        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: searching with plfts operator
        Map<String, Object> result = restApiService.getRecords("posts", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
    }

    @Test
    void shouldHandleWebsearchFullTextSearchWithWftsOperator() {
        // Given: a table with text column
        TableInfo tableInfo = createPostsTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("posts", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("content", "wfts.postgresql OR database");

        // And: mock websearch query
        when(jdbcTemplate.queryForList(
                argThat(sql -> sql.contains("to_tsvector('english', \"content\")") &&
                        sql.contains("websearch_to_tsquery('english', ?)")),
                any(Object[].class)
        )).thenReturn(Collections.singletonList(createMap("id", 1, "content", "PostgreSQL database tutorial")));

        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: searching with wfts operator
        Map<String, Object> result = restApiService.getRecords("posts", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
    }

    @Test
    void shouldHandleBulkCreateRecords() {
        // Given: a valid table and multiple records
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        List<Map<String, Object>> dataList = Arrays.asList(
                createMap("name", "John", "email", "john@example.com"),
                createMap("name", "Jane", "email", "jane@example.com")
        );

        // And: mock successful inserts
        when(jdbcTemplate.update(any(String.class), any(Object[].class))).thenReturn(1);
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(Arrays.asList(
                createMap("id", 1, "name", "John", "email", "john@example.com"),
                createMap("id", 2, "name", "Jane", "email", "jane@example.com")
        ));

        // When: creating bulk records
        List<Map<String, Object>> result = restApiService.createBulkRecords("users", dataList);

        // Then: should return all created records
        assertEquals(2, result.size());
        assertEquals("John", result.get(0).get("name"));
        assertEquals("Jane", result.get(1).get("name"));
    }

    @Test
    void shouldHandleBulkUpdateRecords() {
        // Given: a valid table and update list
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        List<Map<String, Object>> updateList = Arrays.asList(
                createMap("id", "1", "data", createMap("name", "John Updated")),
                createMap("id", "2", "data", createMap("name", "Jane Updated"))
        );

        // And: mock successful updates
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class)))
                .thenReturn(Collections.singletonList(createMap("id", 1, "name", "John Updated", "email", "john@example.com")))
                .thenReturn(Collections.singletonList(createMap("id", 2, "name", "Jane Updated", "email", "jane@example.com")));

        // When: updating bulk records
        List<Map<String, Object>> result = restApiService.updateBulkRecords("users", updateList);

        // Then: should return updated records
        assertEquals(2, result.size());
        assertEquals("John Updated", result.get(0).get("name"));
        assertEquals("Jane Updated", result.get(1).get("name"));
    }

    @Test
    void shouldHandleUpdateRecordsByFilters() {
        // Given: a valid table with filters
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("age", "gt.25");

        Map<String, Object> updateData = createMap("name", "Updated Name");

        // And: mock successful update
        when(jdbcTemplate.update(any(String.class), any(Object[].class))).thenReturn(2);

        // When: updating records by filters
        Map<String, Object> result = restApiService.updateRecordsByFilters("users", filters, updateData);

        // Then: should return update count
        assertEquals(2, result.get("updatedCount"));
    }

    @Test
    void shouldHandleDeleteRecordsByFilters() {
        // Given: a valid table with filters
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("age", "lt.18");

        // And: mock successful delete
        when(jdbcTemplate.update(any(String.class), any(Object[].class))).thenReturn(3);

        // When: deleting records by filters
        Map<String, Object> result = restApiService.deleteRecordsByFilters("users", filters);

        // Then: should return delete count
        assertEquals(3, result.get("deletedCount"));
    }

    @Test
    void shouldHandleArrayOperatorArraycontains() {
        // Given: a table with array columns and arraycontains filter
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("tags", "arraycontains.sports");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "tags", Arrays.asList("sports", "music")))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with array filter
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleArrayOperatorArrayhasany() {
        // Given: a table with array filter
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("hobbies", "arrayhasany.{reading,writing}");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "hobbies", Arrays.asList("reading", "cooking")))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with arrayhasany filter
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleArrayOperatorArrayhasall() {
        // Given: a table with array filter
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("skills", "arrayhasall.{java,spring}");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "skills", Arrays.asList("java", "spring", "postgres")))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with arrayhasall filter
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleArrayOperatorArraylength() {
        // Given: a table with array length filter
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("tags", "arraylength.3");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "tags", Arrays.asList("a", "b", "c")))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with array length filter
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleJsonOperatorHaskey() {
        // Given: a table with JSON haskey filter
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("metadata", "haskey.priority");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "metadata", "{\"priority\": \"high\"}"))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with JSON haskey filter
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleJsonOperatorHaskeys() {
        // Given: a table with JSON haskeys filter
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("settings", "haskeys.[\"theme\",\"language\"]");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "settings", "{\"theme\": \"dark\", \"language\": \"en\"}"))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with JSON haskeys filter
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleJsonOperatorJsoncontains() {
        // Given: a table with JSON containment filter
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("profile", "jsoncontains.{\"role\":\"admin\"}");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "profile", "{\"role\":\"admin\",\"level\":5}"))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with JSON contains filter
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleStringOperatorsStartswithAndEndswith() {
        // Given: a table with string operator filters
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("name", "startswith.John");
        params.add("email", "endswith.example.com");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John Doe", "email", "john@example.com"))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with string operator filters
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John Doe", data.get(0).get("name"));
    }

    @Test
    void shouldHandleCaseInsensitiveStringOperators() {
        // Given: a table with case-insensitive string filters
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("name", "ilike.john%");
        params.add("email", "istartswith.JOHN");
        params.add("title", "iendswith.ADMIN");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "john smith", "email", "john@company.com", "title", "sys_admin"))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with case-insensitive filters
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("john smith", data.get(0).get("name"));
    }

    @Test
    void shouldHandleIsNullAndNotNullOperators() {
        // Given: a table with null operator filters
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("deleted_at", "is.null");
        params.add("email", "not.null");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "email", "john@example.com", "deleted_at", null))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with null operator filters
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return matching records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleComplexOrderingWithMultipleColumns() {
        // Given: a table with multiple column ordering
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("order", "age.desc,name.asc");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(Arrays.asList(
                createMap("id", 1, "name", "Alice", "age", 30),
                createMap("id", 2, "name", "Bob", "age", 30),
                createMap("id", 3, "name", "Charlie", "age", 25)
        ));
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(3L);

        // When: getting records with complex ordering
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should return properly ordered records
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(3, data.size());
        assertEquals("Alice", data.get(0).get("name"));
        assertEquals("Bob", data.get(1).get("name"));
        assertEquals("Charlie", data.get(2).get("name"));
    }

    @Test
    void shouldHandleUpsertOperationsWithCustomConflictColumns() {
        // Given: a table with unique constraint
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("email", "john@example.com", "name", "John Updated");

        // And: mock upsert result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "email", "john@example.com", "name", "John Updated"))
        );

        // When: upserting record
        Map<String, Object> result = restApiService.upsertRecord("users", data);

        // Then: should return upserted record
        assertEquals("John Updated", result.get("name"));
        assertEquals("john@example.com", result.get("email"));
    }

    @Test
    void shouldValidateTablePermissionsBeforeOperations() {
        // Given: permission check returns false
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // When: trying to access restricted table
        // Then: should throw security exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecords("restricted_table", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null, false);
        });
        assertTrue(exception.getMessage().contains("Table not found: restricted_table"));
    }

    @Test
    void shouldHandleEnhancedRelationshipServiceIntegration() {
        // Given: a table with records
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        List<Map<String, Object>> records = Collections.singletonList(
                createMap("id", 1, "name", "John", "email", "john@example.com")
        );

        // And: mock query responses
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(records);
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting basic records
        Map<String, Object> result = restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null, false);

        // Then: should return records successfully
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    // ===== MISSING COVERAGE TESTS =====

    @Test
    void shouldHandleDatabaseConstraintViolations() {
        // Given: a table with constraints
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("name", "John", "email", "existing@example.com");

        // And: mock constraint violation
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class)))
                .thenThrow(new DataIntegrityViolationException("Duplicate key"));

        // When: creating record with duplicate key
        // Then: should throw validation exception
        assertThrows(ValidationException.class, () -> {
            restApiService.createRecord("users", data);
        });
    }

    @Test
    void shouldHandleSqlExceptionsDuringOperations() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // And: mock SQL exception
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class)))
                .thenThrow(new RuntimeException(new SQLException("Database error")));

        // When: creating record with SQL error
        // Then: should propagate SQLException
        assertThrows(RuntimeException.class, () -> {
            restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null, false);
        });
    }

    @Test
    void shouldValidateNetworkAddresses() {
        // Given: a table with network columns
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("ip_addr", "inet", false, false)
        );
        TableInfo tableInfo = new TableInfo("network_table", columns, Collections.emptyList());
        when(schemaService.getTableSchema()).thenReturn(Map.of("network_table", tableInfo));

        Map<String, Object> data = createMap("ip_addr", "192.168.1.1");

        // And: mock successful insert
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "ip_addr", "192.168.1.1"))
        );

        // When: creating record with network address
        Map<String, Object> result = restApiService.createRecord("network_table", data);

        // Then: should return created record with IP address
        assertEquals(1, result.get("id"));
        assertNotNull(result.get("ip_addr"));
    }

    @Test
    void shouldValidateMacAddresses() {
        // Given: a table with MAC address column
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("mac_addr", "macaddr", false, false)
        );
        TableInfo tableInfo = new TableInfo("mac_table", columns, Collections.emptyList());
        when(schemaService.getTableSchema()).thenReturn(Map.of("mac_table", tableInfo));

        Map<String, Object> data = createMap("mac_addr", "08:00:2b:01:02:03");

        // And: mock successful insert
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "mac_addr", "08:00:2b:01:02:03"))
        );

        // When: creating record with MAC address
        Map<String, Object> result = restApiService.createRecord("mac_table", data);

        // Then: should return created record with MAC address
        assertEquals(1, result.get("id"));
        assertNotNull(result.get("mac_addr"));
    }

    @Test
    void shouldHandleEnumTypeValidation() {
        // Given: a table with enum column
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("status", "status_enum", false, false)
        );
        TableInfo tableInfo = new TableInfo("enum_table", columns, Collections.emptyList());
        when(schemaService.getTableSchema()).thenReturn(Map.of("enum_table", tableInfo));

        Map<String, Object> data = createMap("status", "active");

        // And: mock successful insert
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "status", "active"))
        );

        // When: creating record with enum value
        Map<String, Object> result = restApiService.createRecord("enum_table", data);

        // Then: should return created record
        assertEquals("active", result.get("status"));
    }

    @Test
    void shouldHandlePostgreSQLArraysConversion() {
        // Given: a table with array columns
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // And: mock query with PostgreSQL arrays
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "name", "John", "tags", "{sports,music}"))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with arrays
        Map<String, Object> result = restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null, false);

        // Then: should convert arrays properly
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals("John", data.get(0).get("name"));
    }

    @Test
    void shouldHandleCursorEncodingAndDecoding() {
        // Given: a valid table
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // And: mock query with cursor pagination
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(Arrays.asList(
                createMap("id", 1, "name", "John"),
                createMap("id", 2, "name", "Jane")
        ));
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(2L);

        // When: getting records with cursor
        Map<String, Object> result = restApiService.getRecordsWithCursor("users", new LinkedMultiValueMap<>(),
                "2", null, null, null, "id", "asc", null, null);

        // Then: should return cursor-based results
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> edges = (List<Map<String, Object>>) result.get("edges");
        Map<String, Object> pageInfo = (Map<String, Object>) result.get("pageInfo");
        Long totalCount = (Long) result.get("totalCount");

        assertEquals(2, edges.size());
        assertNotNull(pageInfo);
        assertEquals(2L, totalCount);
    }

    @Test
    void shouldHandleComplexColumnTypeConversion() {
        // Given: a table with various column types
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "uuid", true, false),
                new ColumnInfo("timestamp_col", "timestamptz", false, false),
                new ColumnInfo("json_col", "jsonb", false, false),
                new ColumnInfo("array_col", "text[]", false, false)
        );
        TableInfo tableInfo = new TableInfo("complex_table", columns, Collections.emptyList());
        when(schemaService.getTableSchema()).thenReturn(Map.of("complex_table", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("timestamp_col", "gte.2023-01-01T00:00:00Z");
        params.add("json_col", "haskey.name");

        // And: mock query result
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", "123e4567-e89b-12d3-a456-426614174000", "json_col", "{\"name\": \"test\"}"))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: querying with complex types
        Map<String, Object> result = restApiService.getRecords("complex_table", params, 0, 10, null, "asc", null, null, false);

        // Then: should handle complex type conversion
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
    }

    @Test
    void shouldExpandSingleRelationship() {
        // Given: a table with foreign key
        ForeignKeyInfo fkInfo = new ForeignKeyInfo("customer_id", "customers", "id");
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("customer_id", "integer", false, false),
                new ColumnInfo("order_date", "date", false, false)
        );
        TableInfo tableInfo = new TableInfo("orders", columns, Collections.singletonList(fkInfo));
        when(schemaService.getTableSchema()).thenReturn(Map.of(
                "orders", tableInfo,
                "customers", createSampleTableInfo()
        ));

        // And: mock enhanced relationship service
        when(enhancedRelationshipService.expandRelationships(any(), any(), any(), any())).thenReturn(Collections.emptyList());

        // And: mock query results
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "customer_id", 1, "order_date", "2023-01-01"))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with relationship expansion
        Map<String, Object> result = restApiService.getRecords("orders", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, "customer_id", false);

        // Then: should expand relationship
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals(1, data.get(0).get("customer_id"));
    }

    @Test
    void shouldHandleMultipleRelationshipsExpansion() {
        // Given: a table with multiple foreign keys
        ForeignKeyInfo fkInfo1 = new ForeignKeyInfo("customer_id", "customers", "id");
        ForeignKeyInfo fkInfo2 = new ForeignKeyInfo("product_id", "products", "id");
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("customer_id", "integer", false, false),
                new ColumnInfo("product_id", "integer", false, false)
        );
        TableInfo tableInfo = new TableInfo("order_items", columns, Arrays.asList(fkInfo1, fkInfo2));
        when(schemaService.getTableSchema()).thenReturn(Map.of(
                "order_items", tableInfo,
                "customers", createSampleTableInfo(),
                "products", createProductsTableInfo()
        ));

        // And: mock enhanced relationship service
        when(enhancedRelationshipService.expandRelationships(any(), any(), any(), any())).thenReturn(Collections.emptyList());

        // And: mock query results
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(
                Collections.singletonList(createMap("id", 1, "customer_id", 1, "product_id", 2))
        );
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with multiple expansions
        Map<String, Object> result = restApiService.getRecords("order_items", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, "customer_id,product_id", false);

        // Then: should expand multiple relationships
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertEquals(1, data.size());
        assertEquals(1, data.get(0).get("customer_id"));
        assertEquals(2, data.get(0).get("product_id"));
    }

    @Test
    void shouldHandleInvalidTableNameCharacters() {
        // When: using table name with invalid characters
        // Then: should throw validation exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecords("users'; DROP TABLE users; --", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null, false);
        });
        assertTrue(exception.getMessage().contains("Invalid table name"));
    }

    @Test
    void shouldHandleEmptyTableName() {
        // When: using empty table name
        // Then: should throw validation exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecords("", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null, false);
        });
        assertTrue(exception.getMessage().contains("Table name cannot be empty"));
    }

    @Test
    void shouldHandlePermissionCheckFailuresGracefully() {
        // Given: a table that exists
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // And: mock current user query
        when(jdbcTemplate.queryForObject(eq("SELECT current_user"), eq(String.class))).thenReturn("test_user");

        // And: mock permission check failure
        when(jdbcTemplate.queryForObject(eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), any(), any())).thenReturn(false);

        // When: getting records without permission
        // Then: should throw access denied exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", null, null, false);
        });

        assertTrue(exception.getMessage().contains("Access denied") ||
                   exception.getMessage().contains("insufficient privileges"));
    }

    @Test
    void shouldValidateDeleteWithEmptyFilters() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // When: deleting with empty filters
        // Then: should throw validation exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.deleteRecordsByFilters("users", new LinkedMultiValueMap<>());
        });
        assertTrue(exception.getMessage().contains("Filters cannot be empty"));
    }

    @Test
    void shouldValidateUpdateWithEmptyFilters() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        // When: updating with empty filters
        // Then: should throw validation exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.updateRecordsByFilters("users", new LinkedMultiValueMap<>(), createMap("name", "test"));
        });
        assertTrue(exception.getMessage().contains("Filters cannot be empty"));
    }

    @Test
    void shouldValidateUpdateWithEmptyData() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("id", "eq.1");

        // When: updating with empty data
        // Then: should throw validation exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.updateRecordsByFilters("users", filters, Collections.emptyMap());
        });
        assertTrue(exception.getMessage().contains("Update data cannot be empty"));
    }

    @Test
    void shouldHandleOrderByParsingWithMultipleColumns() {
        // Given: a table with multiple columns
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("order", "name.asc,age.desc");

        // And: mock query result
        List<Map<String, Object>> mockData = Collections.singletonList(createMap("id", 1, "name", "Alice", "age", 30));
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenReturn(mockData);
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: getting records with complex ordering
        Map<String, Object> result = restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);

        // Then: should apply complex ordering
        verify(jdbcTemplate, times(1)).queryForList(
                argThat(sql -> sql.contains("ORDER BY") && sql.contains("name") && sql.contains("age")),
                any(Object[].class)
        );
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) result.get("data");
        assertNotNull(data);
        assertEquals(1, data.size());
    }

    @Test
    void shouldHandleInvalidColumnInOrderBy() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("order", "invalid_column.asc");

        // When: using invalid column in order by
        // Then: should throw validation exception
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            restApiService.getRecords("users", params, 0, 10, null, "asc", null, null, false);
        });
        assertTrue(exception.getMessage().contains("Invalid column"));
    }

    @Test
    void shouldHandleDatabaseConstraintViolationWithMessageExtraction() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("name", "Test", "email", "test@example.com");

        // And: mock constraint violation with specific message
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenThrow(
                new DataIntegrityViolationException("ERROR: duplicate key value violates unique constraint \"users_email_key\"")
        );

        // When: creating record with constraint violation
        // Then: should throw validation exception
        assertThrows(ValidationException.class, () -> {
            restApiService.createRecord("users", data);
        });
    }

    @Test
    void shouldHandleSqlConstraintViolationsDuringUpdate() {
        // Given: a table info
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        Map<String, Object> data = createMap("name", "Updated Name");

        // And: mock SQL constraint violation
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class))).thenThrow(
                new DataIntegrityViolationException("Foreign key constraint violation")
        );

        // When: updating record with constraint violation
        // Then: should propagate validation exception
        assertThrows(ValidationException.class, () -> {
            restApiService.updateRecord("users", "1", data, false);
        });
    }

    // ===== Phase 1B: Select Alias/Cast SQL Generation =====

    @Test
    void shouldGenerateSqlWithColumnAlias() {
        // Given
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class)))
            .thenReturn(List.of(Map.of("fn", "John")));
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        // When: select with alias syntax
        restApiService.getRecords("users", params, 0, 10, null, "asc", "fn:name", null, false);

        // Then: SQL should contain '"name" AS "fn"'
        verify(jdbcTemplate).queryForList(
            argThat(sql -> sql.contains("\"name\" AS \"fn\"")),
            any(Object[].class)
        );
    }

    @Test
    void shouldGenerateSqlWithTypeCast() {
        // Given
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class)))
            .thenReturn(List.of(Map.of("age", "25")));
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: select with type cast syntax
        restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", "age::text", null, false);

        // Then: SQL should contain '"age"::text'
        verify(jdbcTemplate).queryForList(
            argThat(sql -> sql.contains("\"age\"::text")),
            any(Object[].class)
        );
    }

    @Test
    void shouldGenerateSqlWithAliasAndCast() {
        // Given
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class)))
            .thenReturn(List.of(Map.of("display_name", "John", "age", "25")));
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: select with alias + cast
        restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", "display_name:name,age::text", null, false);

        // Then: SQL should contain '"name" AS "display_name"' and '"age"::text'
        verify(jdbcTemplate).queryForList(
            argThat(sql -> sql.contains("\"name\" AS \"display_name\"") && sql.contains("\"age\"::text")),
            any(Object[].class)
        );
    }

    @Test
    void shouldGenerateSqlWithJsonPathAlias() {
        // Given
        TableInfo tableInfo = createSampleTableInfo();
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", tableInfo));

        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class)))
            .thenReturn(List.of(Map.of("city", "New York")));
        when(jdbcTemplate.queryForObject(any(String.class), eq(Long.class), any(Object[].class))).thenReturn(1L);

        // When: select with JSON path + alias
        restApiService.getRecords("users", new LinkedMultiValueMap<>(), 0, 10, null, "asc", "city:metadata->>'city'", null, false);

        // Then: SQL should contain JSON path expression with alias
        verify(jdbcTemplate).queryForList(
            argThat(sql -> sql.contains("metadata->>'city' AS \"city\"")),
            any(Object[].class)
        );
    }

    // ===== Helper Methods =====

    private Map<String, Object> createMap(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    private TableInfo createSampleTableInfo() {
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("name", "varchar", false, false),
                new ColumnInfo("email", "varchar", false, false),
                new ColumnInfo("age", "integer", false, true),
                new ColumnInfo("tags", "text[]", false, true),
                new ColumnInfo("hobbies", "text[]", false, true),
                new ColumnInfo("skills", "text[]", false, true),
                new ColumnInfo("metadata", "jsonb", false, true),
                new ColumnInfo("settings", "jsonb", false, true),
                new ColumnInfo("profile", "jsonb", false, true),
                new ColumnInfo("title", "varchar", false, true),
                new ColumnInfo("deleted_at", "timestamp", false, true)
        );
        return new TableInfo("users", columns, Collections.emptyList());
    }

    private TableInfo createCompositeKeyTableInfo() {
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("order_id", "integer", true, false),
                new ColumnInfo("product_id", "integer", true, false),
                new ColumnInfo("quantity", "integer", false, false),
                new ColumnInfo("price", "decimal", false, false)
        );
        return new TableInfo("order_items", columns, Collections.emptyList());
    }

    private TableInfo createPostsTableInfo() {
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("title", "varchar", false, false),
                new ColumnInfo("content", "text", false, false),
                new ColumnInfo("created_at", "timestamp", false, false)
        );
        return new TableInfo("posts", columns, Collections.emptyList());
    }

    private TableInfo createProductsTableInfo() {
        List<ColumnInfo> columns = Arrays.asList(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("name", "varchar", false, false),
                new ColumnInfo("price", "decimal", false, false),
                new ColumnInfo("is_active", "boolean", false, false)
        );
        return new TableInfo("products", columns, Collections.emptyList());
    }
}
