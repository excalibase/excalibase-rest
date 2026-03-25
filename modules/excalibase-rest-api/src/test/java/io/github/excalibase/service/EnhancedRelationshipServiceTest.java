package io.github.excalibase.service;

import io.github.excalibase.model.SelectField;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.postgres.service.DatabaseSchemaService;
import io.github.excalibase.postgres.service.EnhancedRelationshipService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.LinkedMultiValueMap;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EnhancedRelationshipServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private DatabaseSchemaService schemaService;

    private EnhancedRelationshipService relationshipService;

    @BeforeEach
    void setup() {
        relationshipService = new EnhancedRelationshipService(jdbcTemplate, schemaService);
    }

    @Test
    void shouldExpandForwardRelationshipWithColumnSelection() {
        // Given: records with foreign key references
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "title", "Post 1", "author_id", 101),
            createMap("id", 2, "title", "Post 2", "author_id", 102)
        );

        TableInfo tableInfo = createPostsTableInfo();
        List<SelectField> embeddedFields = Arrays.asList(
            new SelectField("authors", Arrays.asList(
                new SelectField("name"),
                new SelectField("email")
            ))
        );

        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("posts", tableInfo);
        allTables.put("authors", createAuthorsTableInfo());

        List<Map<String, Object>> relatedRecords = Arrays.asList(
            createMap("id", 101, "name", "John Doe", "email", "john@example.com"),
            createMap("id", 102, "name", "Jane Smith", "email", "jane@example.com")
        );

        // When: expanding relationships
        when(schemaService.getTableSchema()).thenReturn(allTables);
        when(jdbcTemplate.queryForList(
            argThat(sql -> sql.contains("SELECT name, email FROM authors") &&
                          sql.contains("WHERE id IN (?,?)")),
            any(Object[].class)
        )).thenReturn(relatedRecords);

        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());

        // Then: should query related table with selected columns
        verify(schemaService, times(1)).getTableSchema();
        verify(jdbcTemplate, times(1)).queryForList(
            argThat(sql -> sql.contains("SELECT name, email FROM authors") &&
                          sql.contains("WHERE id IN (?,?)")),
            any(Object[].class)
        );

        // And: should attach related records
        Map<String, Object> authorsRecord0 = getNestedMap(records.get(0), "authors");
        assertEquals("John Doe", authorsRecord0.get("name"));

        Map<String, Object> authorsRecord1 = getNestedMap(records.get(1), "authors");
        assertEquals("Jane Smith", authorsRecord1.get("name"));
    }

    @Test
    void shouldExpandForwardRelationshipWithWildcardSelection() {
        // Given: records and wildcard embedded field
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "title", "Post 1", "author_id", 101)
        );

        TableInfo tableInfo = createPostsTableInfo();
        List<SelectField> embeddedFields = Arrays.asList(
            new SelectField("authors", Arrays.asList(new SelectField("*")))
        );

        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("posts", tableInfo);
        allTables.put("authors", createAuthorsTableInfo());

        List<Map<String, Object>> relatedRecords = Arrays.asList(
            createMap("id", 101, "name", "John Doe", "email", "john@example.com", "bio", "Author bio")
        );

        // When: expanding with wildcard
        when(schemaService.getTableSchema()).thenReturn(allTables);
        when(jdbcTemplate.queryForList(
            argThat(sql -> sql.contains("SELECT * FROM authors")),
            any(Object[].class)
        )).thenReturn(relatedRecords);

        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());

        // Then: should query with SELECT *
        verify(schemaService, times(1)).getTableSchema();
        verify(jdbcTemplate, times(1)).queryForList(
            argThat(sql -> sql.contains("SELECT * FROM authors")),
            any(Object[].class)
        );

        // And: should attach full record
        Map<String, Object> authorsRecord = getNestedMap(records.get(0), "authors");
        assertEquals("John Doe", authorsRecord.get("name"));
        assertEquals("Author bio", authorsRecord.get("bio"));
    }

    @Test
    void shouldExpandReverseRelationshipOneToMany() {
        // Given: author records
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 101, "name", "John Doe"),
            createMap("id", 102, "name", "Jane Smith")
        );

        TableInfo tableInfo = createAuthorsTableInfo();
        List<SelectField> embeddedFields = Arrays.asList(
            new SelectField("posts", Arrays.asList(
                new SelectField("title"),
                new SelectField("content")
            ))
        );

        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("authors", tableInfo);
        allTables.put("posts", createPostsTableInfo());

        List<Map<String, Object>> relatedRecords = Arrays.asList(
            createMap("author_id", 101, "title", "Post 1", "content", "Content 1"),
            createMap("author_id", 101, "title", "Post 2", "content", "Content 2"),
            createMap("author_id", 102, "title", "Post 3", "content", "Content 3")
        );

        // When: expanding reverse relationships
        when(schemaService.getTableSchema()).thenReturn(allTables);
        when(jdbcTemplate.queryForList(
            argThat(sql -> sql.contains("SELECT title, content FROM posts") &&
                          sql.contains("WHERE author_id IN (?,?)")),
            any(Object[].class)
        )).thenReturn(relatedRecords);

        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());

        // Then: should query posts table
        verify(schemaService, times(1)).getTableSchema();
        verify(jdbcTemplate, times(1)).queryForList(
            argThat(sql -> sql.contains("SELECT title, content FROM posts") &&
                          sql.contains("WHERE author_id IN (?,?)")),
            any(Object[].class)
        );

        // And: should group posts by author
        List<Map<String, Object>> posts0 = getNestedList(records.get(0), "posts");
        assertEquals(2, posts0.size());
        assertEquals("Post 1", posts0.get(0).get("title"));
        assertEquals("Post 2", posts0.get(1).get("title"));

        List<Map<String, Object>> posts1 = getNestedList(records.get(1), "posts");
        assertEquals(1, posts1.size());
        assertEquals("Post 3", posts1.get(0).get("title"));
    }

    @Test
    void shouldApplyEmbeddedFiltersOnForwardRelationships() {
        // Given: records with filter parameters
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "title", "Post 1", "author_id", 101)
        );

        TableInfo tableInfo = createPostsTableInfo();
        SelectField embeddedField = new SelectField("authors", Arrays.asList(new SelectField("name")));
        embeddedField.addFilter("status", "eq.active");
        embeddedField.addFilter("age", "gt.25");

        List<SelectField> embeddedFields = Arrays.asList(embeddedField);
        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("posts", tableInfo);
        allTables.put("authors", createAuthorsTableInfo());

        List<Map<String, Object>> relatedRecords = Arrays.asList(
            createMap("id", 101, "name", "John Doe")
        );

        // When: expanding with filters
        when(schemaService.getTableSchema()).thenReturn(allTables);
        when(jdbcTemplate.queryForList(
            argThat(sql -> sql.contains("SELECT name FROM authors") &&
                          sql.contains("WHERE id IN (?)") &&
                          sql.contains("AND status = ?") &&
                          sql.contains("AND age > ?")),
            any(Object[].class)
        )).thenReturn(relatedRecords);

        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());

        // Then: should apply filters in WHERE clause (parameterized, not concatenated)
        verify(schemaService, times(1)).getTableSchema();
        verify(jdbcTemplate, times(1)).queryForList(
            argThat(sql -> sql.contains("SELECT name FROM authors") &&
                          sql.contains("WHERE id IN (?)") &&
                          sql.contains("AND status = ?") &&
                          sql.contains("AND age > ?")),
            any(Object[].class)
        );
    }

    @Test
    void shouldHandleEmbeddedFiltersWithDifferentOperators() {
        // Given: records with various filter operators
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "author_id", 101)
        );
        TableInfo tableInfo = createPostsTableInfo();

        SelectField embeddedField = new SelectField("authors", Arrays.asList(new SelectField("name")));
        embeddedField.addFilter("age", "gte.30");
        embeddedField.addFilter("status", "neq.inactive");
        embeddedField.addFilter("bio", "like.writer");
        embeddedField.addFilter("score", "lt.100");
        embeddedField.addFilter("rating", "lte.5");

        List<SelectField> embeddedFields = Arrays.asList(embeddedField);
        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("posts", tableInfo);
        allTables.put("authors", createAuthorsTableInfo());

        List<Map<String, Object>> relatedRecords = Arrays.asList(
            createMap("id", 101, "name", "John")
        );

        // When: expanding with various operators
        when(schemaService.getTableSchema()).thenReturn(allTables);
        when(jdbcTemplate.queryForList(
            argThat(sql -> sql.contains("age >= ?") &&
                          sql.contains("status != ?") &&
                          sql.contains("bio LIKE ?") &&
                          sql.contains("score < ?") &&
                          sql.contains("rating <= ?")),
            any(Object[].class)
        )).thenReturn(relatedRecords);

        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());

        // Then: should handle all operators correctly with parameterized queries
        verify(schemaService, times(1)).getTableSchema();
        verify(jdbcTemplate, times(1)).queryForList(
            argThat(sql -> sql.contains("age >= ?") &&
                          sql.contains("status != ?") &&
                          sql.contains("bio LIKE ?") &&
                          sql.contains("score < ?") &&
                          sql.contains("rating <= ?")),
            any(Object[].class)
        );
    }

    @Test
    void shouldHandleEmptyRecordsGracefully() {
        // Given: empty records list
        List<Map<String, Object>> records = new ArrayList<>();
        TableInfo tableInfo = createPostsTableInfo();
        List<SelectField> embeddedFields = Arrays.asList(new SelectField("authors", Collections.emptyList()));

        // When: expanding empty records
        List<Map<String, Object>> result = relationshipService.expandRelationships(
            records, tableInfo, embeddedFields, new LinkedMultiValueMap<>()
        );

        // Then: should return empty list without database calls
        verifyNoInteractions(jdbcTemplate);
        verifyNoInteractions(schemaService);
        assertTrue(result.isEmpty());
    }

    @Test
    void shouldHandleEmptyEmbeddedFieldsGracefully() {
        // Given: records but no embedded fields
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "title", "Post")
        );
        TableInfo tableInfo = createPostsTableInfo();
        List<SelectField> embeddedFields = Collections.emptyList();

        // When: expanding with no embedded fields
        List<Map<String, Object>> result = relationshipService.expandRelationships(
            records, tableInfo, embeddedFields, new LinkedMultiValueMap<>()
        );

        // Then: should return records unchanged
        verifyNoInteractions(jdbcTemplate);
        verifyNoInteractions(schemaService);
        assertEquals(records, result);
    }

    @Test
    void shouldHandleRelationshipNotFoundGracefully() {
        // Given: records with non-existent relationship
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "title", "Post")
        );
        TableInfo tableInfo = createPostsTableInfo();
        List<SelectField> embeddedFields = Arrays.asList(new SelectField("nonexistent", Collections.emptyList()));
        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("posts", tableInfo);

        // When: expanding non-existent relationship
        when(schemaService.getTableSchema()).thenReturn(allTables);

        assertDoesNotThrow(() -> {
            relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());
        });

        // Then: should handle gracefully without errors
        verify(schemaService, times(1)).getTableSchema();
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void shouldHandleNullForeignKeyValues() {
        // Given: records with null foreign key
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "title", "Post 1", "author_id", null),
            createMap("id", 2, "title", "Post 2", "author_id", 101)
        );

        TableInfo tableInfo = createPostsTableInfo();
        List<SelectField> embeddedFields = Arrays.asList(
            new SelectField("authors", Arrays.asList(new SelectField("name")))
        );
        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("posts", tableInfo);
        allTables.put("authors", createAuthorsTableInfo());

        List<Map<String, Object>> relatedRecords = Arrays.asList(
            createMap("id", 101, "name", "John")
        );

        // When: expanding with null foreign keys
        when(schemaService.getTableSchema()).thenReturn(allTables);
        when(jdbcTemplate.queryForList(
            argThat(sql -> sql.contains("WHERE id IN (?)")),
            any(Object[].class)
        )).thenReturn(relatedRecords);

        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());

        // Then: should only query for non-null values
        verify(schemaService, times(1)).getTableSchema();
        verify(jdbcTemplate, times(1)).queryForList(
            argThat(sql -> sql.contains("WHERE id IN (?)")),
            any(Object[].class)
        );

        // And: should handle null appropriately
        assertNull(records.get(0).get("authors")); // No match for null foreign key
        Map<String, Object> authorsRecord = getNestedMap(records.get(1), "authors");
        assertEquals("John", authorsRecord.get("name"));
    }

    @Test
    void shouldHandleSqlExceptionsGracefully() {
        // Given: records that will cause SQL error
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "author_id", 101)
        );
        TableInfo tableInfo = createPostsTableInfo();
        List<SelectField> embeddedFields = Arrays.asList(
            new SelectField("authors", Arrays.asList(new SelectField("name")))
        );
        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("posts", tableInfo);
        allTables.put("authors", createAuthorsTableInfo());

        // When: SQL exception occurs
        when(schemaService.getTableSchema()).thenReturn(allTables);
        when(jdbcTemplate.queryForList(any(String.class), any(Object[].class)))
            .thenThrow(new RuntimeException("SQL Error"));

        // Then: should handle exception gracefully
        assertDoesNotThrow(() -> {
            relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());
        });

        verify(schemaService, times(1)).getTableSchema();
        verify(jdbcTemplate, times(1)).queryForList(any(String.class), any(Object[].class));
    }

    @Test
    void shouldHandleFiltersWithoutOperatorDefaultToEquality() {
        // Given: embedded field with filter without operator
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "author_id", 101)
        );
        TableInfo tableInfo = createPostsTableInfo();

        SelectField embeddedField = new SelectField("authors", Arrays.asList(new SelectField("name")));
        embeddedField.addFilter("status", "active"); // No operator, should default to equality

        List<SelectField> embeddedFields = Arrays.asList(embeddedField);
        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("posts", tableInfo);
        allTables.put("authors", createAuthorsTableInfo());

        List<Map<String, Object>> relatedRecords = Arrays.asList(
            createMap("id", 101, "name", "John")
        );

        // When: expanding with default equality filter
        when(schemaService.getTableSchema()).thenReturn(allTables);
        when(jdbcTemplate.queryForList(
            argThat(sql -> sql.contains("status = ?")),
            any(Object[].class)
        )).thenReturn(relatedRecords);

        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());

        // Then: should use parameterized equality operator (not string concatenation)
        verify(schemaService, times(1)).getTableSchema();
        verify(jdbcTemplate, times(1)).queryForList(
            argThat(sql -> sql.contains("status = ?")),
            any(Object[].class)
        );
    }

    @Test
    void shouldHandleEmptySubFieldsSelectAll() {
        // Given: embedded field with no sub-fields
        List<Map<String, Object>> records = Arrays.asList(
            createMap("id", 1, "author_id", 101)
        );
        TableInfo tableInfo = createPostsTableInfo();
        List<SelectField> embeddedFields = Arrays.asList(
            new SelectField("authors", Collections.emptyList())
        ); // No sub-fields = select all
        Map<String, TableInfo> allTables = new HashMap<>();
        allTables.put("posts", tableInfo);
        allTables.put("authors", createAuthorsTableInfo());

        List<Map<String, Object>> relatedRecords = Arrays.asList(
            createMap("id", 101, "name", "John", "email", "john@example.com")
        );

        // When: expanding with empty sub-fields
        when(schemaService.getTableSchema()).thenReturn(allTables);
        when(jdbcTemplate.queryForList(
            argThat(sql -> sql.contains("SELECT * FROM authors")),
            any(Object[].class)
        )).thenReturn(relatedRecords);

        relationshipService.expandRelationships(records, tableInfo, embeddedFields, new LinkedMultiValueMap<>());

        // Then: should select all columns
        verify(schemaService, times(1)).getTableSchema();
        verify(jdbcTemplate, times(1)).queryForList(
            argThat(sql -> sql.contains("SELECT * FROM authors")),
            any(Object[].class)
        );
    }

    // Helper methods
    private Map<String, Object> createMap(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put((String) keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getNestedMap(Map<String, Object> parent, String key) {
        return (Map<String, Object>) parent.get(key);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getNestedList(Map<String, Object> parent, String key) {
        return (List<Map<String, Object>>) parent.get(key);
    }

    private TableInfo createPostsTableInfo() {
        List<ColumnInfo> columns = Arrays.asList(
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("title", "varchar", false, false),
            new ColumnInfo("content", "text", false, false),
            new ColumnInfo("author_id", "integer", false, false)
        );
        List<ForeignKeyInfo> foreignKeys = Arrays.asList(
            new ForeignKeyInfo("author_id", "authors", "id")
        );
        return new TableInfo("posts", columns, foreignKeys);
    }

    private TableInfo createAuthorsTableInfo() {
        List<ColumnInfo> columns = Arrays.asList(
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "varchar", false, false),
            new ColumnInfo("email", "varchar", false, false),
            new ColumnInfo("bio", "text", false, false)
        );
        List<ForeignKeyInfo> foreignKeys = Collections.emptyList();
        // Reverse relationship - posts reference authors
        return new TableInfo("authors", columns, foreignKeys);
    }
}
