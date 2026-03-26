package io.github.excalibase.postgres.util;

import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresArrayConverterTest {

    // ===== convertPostgresArrays =====

    @Test
    void convertPostgresArrays_nullInput_returnsNull() {
        assertNull(PostgresArrayConverter.convertPostgresArrays(null));
    }

    @Test
    void convertPostgresArrays_emptyList_returnsEmptyList() {
        List<Map<String, Object>> result = PostgresArrayConverter.convertPostgresArrays(List.of());
        assertTrue(result.isEmpty());
    }

    @Test
    void convertPostgresArrays_recordWithNoArrays_returnsUnchanged() {
        Map<String, Object> record = new HashMap<>();
        record.put("name", "Alice");
        record.put("age", 30);

        List<Map<String, Object>> result = PostgresArrayConverter.convertPostgresArrays(List.of(record));

        assertEquals(1, result.size());
        assertEquals("Alice", result.get(0).get("name"));
        assertEquals(30, result.get(0).get("age"));
    }

    @Test
    void convertPostgresArrays_multipleRecords_convertsAllArrays() throws SQLException {
        Array array1 = mock(Array.class);
        when(array1.getArray()).thenReturn(new Object[]{"a", "b"});
        Array array2 = mock(Array.class);
        when(array2.getArray()).thenReturn(new Object[]{"x", "y", "z"});

        Map<String, Object> r1 = new HashMap<>();
        r1.put("tags", array1);
        Map<String, Object> r2 = new HashMap<>();
        r2.put("tags", array2);

        List<Map<String, Object>> result = PostgresArrayConverter.convertPostgresArrays(List.of(r1, r2));

        assertEquals(2, result.size());
        assertEquals(2, ((List<?>) result.get(0).get("tags")).size());
        assertEquals(3, ((List<?>) result.get(1).get("tags")).size());
    }

    // ===== convertPostgresArraysInRecord =====

    @Test
    void convertPostgresArraysInRecord_nullRecord_returnsNull() {
        assertNull(PostgresArrayConverter.convertPostgresArraysInRecord(null));
    }

    @Test
    void convertPostgresArraysInRecord_emptyRecord_returnsEmptyRecord() {
        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(Map.of());
        assertTrue(result.isEmpty());
    }

    @Test
    void convertPostgresArraysInRecord_sqlArray_convertedToList() throws SQLException {
        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenReturn(new Object[]{"apple", "banana", "cherry"});

        Map<String, Object> record = new HashMap<>();
        record.put("fruits", mockArray);
        record.put("count", 3);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(result.get("fruits") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> fruits = (List<Object>) result.get("fruits");
        assertEquals(3, fruits.size());
        assertTrue(fruits.contains("apple"));
        assertEquals(3, result.get("count")); // Non-array unchanged
    }

    @Test
    void convertPostgresArraysInRecord_nullArrayElements_returnsEmptyList() throws SQLException {
        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenReturn(null);

        Map<String, Object> record = new HashMap<>();
        record.put("data", mockArray);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(result.get("data") instanceof List);
        assertTrue(((List<?>) result.get("data")).isEmpty());
    }

    @Test
    void convertPostgresArraysInRecord_nullValue_preservedAsNull() {
        Map<String, Object> record = new HashMap<>();
        record.put("col", null);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertNull(result.get("col"));
    }

    @Test
    void convertPostgresArraysInRecord_integerArray_convertsToList() throws SQLException {
        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenReturn(new Object[]{1, 2, 3, 4, 5});

        Map<String, Object> record = new HashMap<>();
        record.put("numbers", mockArray);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(result.get("numbers") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> nums = (List<Object>) result.get("numbers");
        assertEquals(5, nums.size());
        assertTrue(nums.contains(1));
        assertTrue(nums.contains(5));
    }

    // ===== SQL array exception fallback to string parsing =====

    @Test
    void convertPostgresArraysInRecord_sqlExceptionOnGetArray_fallsBackToStringParsing() throws SQLException {
        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenThrow(new SQLException("Connection already closed"));
        when(mockArray.toString()).thenReturn("{red,green,blue}");

        Map<String, Object> record = new HashMap<>();
        record.put("colors", mockArray);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(result.get("colors") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> colors = (List<Object>) result.get("colors");
        assertEquals(3, colors.size());
        assertTrue(colors.contains("red"));
        assertTrue(colors.contains("blue"));
    }

    @Test
    void convertPostgresArraysInRecord_getArrayThrowsAnyException_setsNull() throws SQLException {
        Array mockArray = mock(Array.class);
        // Throw a non-SQL exception to hit the outer catch path
        when(mockArray.getArray()).thenThrow(new RuntimeException("Unexpected error"));

        Map<String, Object> record = new HashMap<>();
        record.put("data", mockArray);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertNull(result.get("data"));
    }

    // ===== parsePostgresArrayString tests (accessed via fallback) =====

    @Test
    void convertPostgresArraysInRecord_emptyStringArray_returnsEmptyList() throws SQLException {
        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenThrow(new SQLException("closed"));
        when(mockArray.toString()).thenReturn("{}");

        Map<String, Object> record = new HashMap<>();
        record.put("items", mockArray);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(result.get("items") instanceof List);
        assertTrue(((List<?>) result.get("items")).isEmpty());
    }

    @Test
    void convertPostgresArraysInRecord_nullStringArrayFallback_returnsEmptyList() throws SQLException {
        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenThrow(new SQLException("closed"));
        when(mockArray.toString()).thenReturn("");

        Map<String, Object> record = new HashMap<>();
        record.put("items", mockArray);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(((List<?>) result.get("items")).isEmpty());
    }

    @Test
    void convertPostgresArraysInRecord_quotedStringArrayFallback_parsedCorrectly() throws SQLException {
        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenThrow(new SQLException("closed"));
        when(mockArray.toString()).thenReturn("{\"hello world\",\"foo bar\",\"baz\"}");

        Map<String, Object> record = new HashMap<>();
        record.put("phrases", mockArray);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(result.get("phrases") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> phrases = (List<Object>) result.get("phrases");
        assertEquals(3, phrases.size());
        assertTrue(phrases.contains("hello world"));
        assertTrue(phrases.contains("foo bar"));
    }

    @Test
    void convertPostgresArraysInRecord_quotedWithEscapedQuotes_parsedCorrectly() throws SQLException {
        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenThrow(new SQLException("closed"));
        // Array with escaped quotes inside element
        when(mockArray.toString()).thenReturn("{\"say \\\"hello\\\"\",\"world\"}");

        Map<String, Object> record = new HashMap<>();
        record.put("msgs", mockArray);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(result.get("msgs") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> msgs = (List<Object>) result.get("msgs");
        // Should have 2 elements
        assertEquals(2, msgs.size());
    }

    @Test
    void convertPostgresArraysInRecord_numericArrayFallback_parsedCorrectly() throws SQLException {
        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenThrow(new SQLException("closed"));
        when(mockArray.toString()).thenReturn("{10,20,30,40}");

        Map<String, Object> record = new HashMap<>();
        record.put("scores", mockArray);

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(result.get("scores") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> scores = (List<Object>) result.get("scores");
        assertEquals(4, scores.size());
        assertTrue(scores.contains("10"));
        assertTrue(scores.contains("40"));
    }

    @Test
    void convertPostgresArraysInRecord_multipleArrayColumns_allConverted() throws SQLException {
        Array arr1 = mock(Array.class);
        when(arr1.getArray()).thenReturn(new Object[]{"a", "b"});
        Array arr2 = mock(Array.class);
        when(arr2.getArray()).thenReturn(new Object[]{1, 2, 3});

        Map<String, Object> record = new HashMap<>();
        record.put("letters", arr1);
        record.put("numbers", arr2);
        record.put("name", "test");

        Map<String, Object> result = PostgresArrayConverter.convertPostgresArraysInRecord(record);

        assertTrue(result.get("letters") instanceof List);
        assertTrue(result.get("numbers") instanceof List);
        assertEquals("test", result.get("name"));
        assertEquals(2, ((List<?>) result.get("letters")).size());
        assertEquals(3, ((List<?>) result.get("numbers")).size());
    }
}
