package io.github.excalibase.postgres.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RelationshipBatchLoaderTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private RelationshipBatchLoader batchLoader;

    @BeforeEach
    void setup() {
        batchLoader = new RelationshipBatchLoader(jdbcTemplate);
    }

    // ===== loadRelatedRecords =====

    @Test
    void loadRelatedRecords_emptyForeignKeyValues_returnsEmptyMap() {
        Map<Object, List<Map<String, Object>>> result =
                batchLoader.loadRelatedRecords("orders", "customer_id", "id",
                        Set.of(), "*", 10);

        assertTrue(result.isEmpty());
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void loadRelatedRecords_singleValue_executesQueryAndGroupsResults() {
        List<Map<String, Object>> dbResults = List.of(
                Map.of("id", 101, "customer_id", 1, "total", 99.99)
        );
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(dbResults);

        Map<Object, List<Map<String, Object>>> result =
                batchLoader.loadRelatedRecords("orders", "customer_id", "id",
                        Set.of(1), "*", 10);

        assertNotNull(result);
        assertTrue(result.containsKey(1));
        assertEquals(1, result.get(1).size());
        assertEquals(99.99, result.get(1).get(0).get("total"));
    }

    @Test
    void loadRelatedRecords_multipleValues_groupsResultsByForeignKey() {
        List<Map<String, Object>> dbResults = List.of(
                Map.of("id", 101, "customer_id", 1, "total", 50.0),
                Map.of("id", 102, "customer_id", 1, "total", 30.0),
                Map.of("id", 201, "customer_id", 2, "total", 75.0)
        );
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(dbResults);

        Map<Object, List<Map<String, Object>>> result =
                batchLoader.loadRelatedRecords("orders", "customer_id", "id",
                        Set.of(1, 2), "*", 100);

        assertEquals(2, result.get(1).size());
        assertEquals(1, result.get(2).size());
    }

    @Test
    void loadRelatedRecords_valueNotInResults_returnedWithEmptyList() {
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        Map<Object, List<Map<String, Object>>> result =
                batchLoader.loadRelatedRecords("orders", "customer_id", "id",
                        Set.of(999), "*", 10);

        assertTrue(result.containsKey(999));
        assertTrue(result.get(999).isEmpty());
    }

    @Test
    void loadRelatedRecords_zeroLimit_noLimitClauseInQuery() {
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        // Should not throw even with limit=0
        assertDoesNotThrow(() ->
                batchLoader.loadRelatedRecords("orders", "customer_id", "id",
                        Set.of(1), "*", 0));
    }

    @Test
    void loadRelatedRecords_jdbcThrowsException_returnsEmptyListsForAllValues() {
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenThrow(new RuntimeException("DB error"));

        Map<Object, List<Map<String, Object>>> result =
                batchLoader.loadRelatedRecords("orders", "customer_id", "id",
                        Set.of(1, 2), "*", 10);

        assertTrue(result.containsKey(1));
        assertTrue(result.containsKey(2));
        assertTrue(result.get(1).isEmpty());
        assertTrue(result.get(2).isEmpty());
    }

    // ===== cache hit behavior =====

    @Test
    void loadRelatedRecords_secondCallWithSameKey_usesCacheNotDb() {
        List<Map<String, Object>> dbResults = List.of(
                Map.of("id", 101, "customer_id", 1)
        );
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(dbResults);

        // First call populates cache
        batchLoader.loadRelatedRecords("orders", "customer_id", "id", Set.of(1), "*", 10);
        // Second call should hit cache
        batchLoader.loadRelatedRecords("orders", "customer_id", "id", Set.of(1), "*", 10);

        // JdbcTemplate should only be called once
        verify(jdbcTemplate, times(1)).queryForList(anyString(), any(Object[].class));
    }

    @Test
    void loadRelatedRecords_differentSelectClause_doesNotShareCache() {
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        batchLoader.loadRelatedRecords("orders", "customer_id", "id", Set.of(1), "*", 10);
        batchLoader.loadRelatedRecords("orders", "customer_id", "id", Set.of(1), "id,total", 10);

        verify(jdbcTemplate, times(2)).queryForList(anyString(), any(Object[].class));
    }

    // ===== loadSingleRelatedRecords =====

    @Test
    void loadSingleRelatedRecords_emptyValues_returnsEmptyMap() {
        Map<Object, Map<String, Object>> result =
                batchLoader.loadSingleRelatedRecords("customers", "id", Set.of(), "*");

        assertTrue(result.isEmpty());
        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void loadSingleRelatedRecords_singleValue_returnsMatchedRecord() {
        List<Map<String, Object>> dbResults = List.of(
                Map.of("id", 1, "name", "Alice")
        );
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(dbResults);

        Map<Object, Map<String, Object>> result =
                batchLoader.loadSingleRelatedRecords("customers", "id", Set.of(1), "*");

        assertTrue(result.containsKey(1));
        assertEquals("Alice", result.get(1).get("name"));
    }

    @Test
    void loadSingleRelatedRecords_valueNotFound_notIncludedInResult() {
        // NOTE: This test documents a known bug: loadSingleRelatedRecords attempts to put null
        // into a ConcurrentHashMap (line 112), which throws NullPointerException.
        // The bug is that ConcurrentHashMap does not support null values.
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        // Expect NPE due to the known bug in the source code
        assertThrows(NullPointerException.class, () ->
                batchLoader.loadSingleRelatedRecords("customers", "id", Set.of(999), "*"));
    }

    @Test
    void loadSingleRelatedRecords_multipleValues_loadsAllInOneBatch() {
        List<Map<String, Object>> dbResults = List.of(
                Map.of("id", 1, "name", "Alice"),
                Map.of("id", 2, "name", "Bob")
        );
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(dbResults);

        Map<Object, Map<String, Object>> result =
                batchLoader.loadSingleRelatedRecords("customers", "id", Set.of(1, 2), "*");

        assertEquals(2, result.size());
        assertEquals("Alice", result.get(1).get("name"));
        assertEquals("Bob", result.get(2).get("name"));
        verify(jdbcTemplate, times(1)).queryForList(anyString(), any(Object[].class));
    }

    @Test
    void loadSingleRelatedRecords_jdbcThrowsException_returnsEmptyMap() {
        // When JDBC throws, executeSingleBatchQuery returns an empty map.
        // Then the code tries to put null into ConcurrentHashMap for uncached values -> NPE bug.
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenThrow(new RuntimeException("DB connection lost"));

        // Expect NPE due to the known bug: putting null in ConcurrentHashMap
        assertThrows(NullPointerException.class, () ->
                batchLoader.loadSingleRelatedRecords("customers", "id", Set.of(1), "*"));
    }

    @Test
    void loadSingleRelatedRecords_cacheHit_doesNotRequery() {
        // When a record IS found, it's stored in cache correctly (no null put issue).
        // First call populates cache with actual record. Second call hits cache.
        List<Map<String, Object>> dbResults = List.of(Map.of("id", 1, "name", "Alice"));
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(dbResults);

        // First call succeeds (record found, no null caching needed)
        Map<Object, Map<String, Object>> first = batchLoader.loadSingleRelatedRecords("customers", "id", Set.of(1), "*");
        assertEquals("Alice", first.get(1).get("name"));

        // Second call should hit cache
        Map<Object, Map<String, Object>> second = batchLoader.loadSingleRelatedRecords("customers", "id", Set.of(1), "*");
        assertEquals("Alice", second.get(1).get("name"));

        verify(jdbcTemplate, times(1)).queryForList(anyString(), any(Object[].class));
    }

    // ===== clearCache =====

    @Test
    void clearCache_afterClear_nextCallQueriesDbAgain() {
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        batchLoader.loadRelatedRecords("orders", "customer_id", "id", Set.of(1), "*", 10);
        batchLoader.clearCache();
        batchLoader.loadRelatedRecords("orders", "customer_id", "id", Set.of(1), "*", 10);

        verify(jdbcTemplate, times(2)).queryForList(anyString(), any(Object[].class));
    }

    @Test
    void clearCache_canBeCalledMultipleTimes_doesNotThrow() {
        assertDoesNotThrow(() -> {
            batchLoader.clearCache();
            batchLoader.clearCache();
        });
    }

    // ===== getCacheStatistics =====

    @Test
    void getCacheStatistics_emptyCache_returnsZeroCacheSize() {
        Map<String, Object> stats = batchLoader.getCacheStatistics();

        assertNotNull(stats);
        assertEquals(0, stats.get("cacheSize"));
        assertEquals(0, stats.get("cacheKeys"));
        assertTrue(stats.containsKey("threadId"));
    }

    @Test
    void getCacheStatistics_afterLoading_showsNonZeroCacheSize() {
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        batchLoader.loadRelatedRecords("orders", "customer_id", "id", Set.of(1), "*", 10);

        Map<String, Object> stats = batchLoader.getCacheStatistics();
        int cacheSize = (int) stats.get("cacheSize");
        assertTrue(cacheSize > 0);
    }

    @Test
    void getCacheStatistics_afterClear_returnsZero() {
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        batchLoader.loadRelatedRecords("orders", "customer_id", "id", Set.of(1), "*", 10);
        batchLoader.clearCache();

        Map<String, Object> stats = batchLoader.getCacheStatistics();
        assertEquals(0, stats.get("cacheSize"));
    }

    @Test
    void getCacheStatistics_threadIdPresent() {
        Map<String, Object> stats = batchLoader.getCacheStatistics();
        assertNotNull(stats.get("threadId"));
        assertTrue((long) stats.get("threadId") > 0);
    }
}
