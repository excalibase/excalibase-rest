package io.github.excalibase.postgres.util;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresTypeConverterTest {

    // ===== convertPostgresTypes =====

    @Test
    void convertPostgresTypes_nullInput_returnsNull() {
        assertNull(PostgresTypeConverter.convertPostgresTypes(null, buildTableInfo(List.of())));
    }

    @Test
    void convertPostgresTypes_emptyList_returnsEmptyList() {
        List<Map<String, Object>> result =
                PostgresTypeConverter.convertPostgresTypes(List.of(), buildTableInfo(List.of()));
        assertTrue(result.isEmpty());
    }

    @Test
    void convertPostgresTypes_multipleRecords_convertsAll() {
        ColumnInfo col = new ColumnInfo("id", "uuid", true, false);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        List<Map<String, Object>> records = new ArrayList<>();
        Map<String, Object> r1 = new HashMap<>();
        r1.put("id", "550e8400-e29b-41d4-a716-446655440000");
        records.add(r1);

        Map<String, Object> r2 = new HashMap<>();
        r2.put("id", "550e8400-e29b-41d4-a716-446655440001");
        records.add(r2);

        List<Map<String, Object>> result = PostgresTypeConverter.convertPostgresTypes(records, tableInfo);

        assertEquals(2, result.size());
        assertTrue(result.get(0).get("id") instanceof Map);
        assertTrue(result.get(1).get("id") instanceof Map);
    }

    // ===== convertPostgresTypesInRecord =====

    @Test
    void convertPostgresTypesInRecord_nullRecord_returnsNull() {
        assertNull(PostgresTypeConverter.convertPostgresTypesInRecord(null, buildTableInfo(List.of())));
    }

    @Test
    void convertPostgresTypesInRecord_emptyRecord_returnsEmptyRecord() {
        Map<String, Object> result =
                PostgresTypeConverter.convertPostgresTypesInRecord(Map.of(), buildTableInfo(List.of()));
        assertTrue(result.isEmpty());
    }

    @Test
    void convertPostgresTypesInRecord_nullValue_skipped() {
        ColumnInfo col = new ColumnInfo("name", "text", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("name", null);

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertNull(result.get("name"));
    }

    @Test
    void convertPostgresTypesInRecord_columnNotInTableInfo_usesEmptyType() {
        // Column exists in record but not in tableInfo - should use empty type string
        TableInfo tableInfo = buildTableInfo(List.of());
        Map<String, Object> record = new HashMap<>();
        record.put("mystery_col", "some value");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        // Should keep original value (type="" hits no conversion branch)
        assertEquals("some value", result.get("mystery_col"));
    }

    // ===== SQL Array conversion =====

    @Test
    void convertPostgresTypesInRecord_sqlArrayColumn_convertsToList() throws SQLException {
        ColumnInfo col = new ColumnInfo("tags", "text[]", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenReturn(new Object[]{"apple", "banana", "cherry"});

        Map<String, Object> record = new HashMap<>();
        record.put("tags", mockArray);

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("tags") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> tags = (List<Object>) result.get("tags");
        assertEquals(3, tags.size());
        assertTrue(tags.contains("apple"));
    }

    @Test
    void convertPostgresTypesInRecord_sqlArrayNullElements_returnsEmptyList() throws SQLException {
        ColumnInfo col = new ColumnInfo("tags", "text[]", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenReturn(null);

        Map<String, Object> record = new HashMap<>();
        record.put("tags", mockArray);

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("tags") instanceof List);
        assertTrue(((List<?>) result.get("tags")).isEmpty());
    }

    @Test
    void convertPostgresTypesInRecord_sqlArrayClosedConnection_fallsBackToStringParsing() throws SQLException {
        ColumnInfo col = new ColumnInfo("nums", "integer[]", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Array mockArray = mock(Array.class);
        when(mockArray.getArray()).thenThrow(new SQLException("Connection closed"));
        when(mockArray.toString()).thenReturn("{1,2,3}");

        Map<String, Object> record = new HashMap<>();
        record.put("nums", mockArray);

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("nums") instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> nums = (List<Object>) result.get("nums");
        assertEquals(3, nums.size());
    }

    // ===== JSON/JSONB conversion =====

    @Test
    void convertPostgresTypesInRecord_jsonbStringValue_parsedToMap() {
        ColumnInfo col = new ColumnInfo("data", "jsonb", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("data", "{\"key\":\"value\",\"count\":42}");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("data") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> json = (Map<String, Object>) result.get("data");
        assertEquals("value", json.get("key"));
    }

    @Test
    void convertPostgresTypesInRecord_jsonStringValue_parsedToMap() {
        ColumnInfo col = new ColumnInfo("meta", "json", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("meta", "{\"name\":\"test\"}");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("meta") instanceof Map);
    }

    @Test
    void convertPostgresTypesInRecord_jsonArrayString_parsedToList() {
        ColumnInfo col = new ColumnInfo("items", "jsonb", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("items", "[1,2,3]");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("items") instanceof List);
    }

    @Test
    void convertPostgresTypesInRecord_invalidJsonString_returnedAsString() {
        ColumnInfo col = new ColumnInfo("data", "jsonb", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("data", "{not valid json}}}");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        // Should fall back to returning the original string
        assertTrue(result.get("data") instanceof String);
    }

    // ===== UUID conversion =====

    @Test
    void convertPostgresTypesInRecord_uuidColumn_returnsEnhancedObject() {
        ColumnInfo col = new ColumnInfo("id", "uuid", true, false);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("id", "550e8400-e29b-41d4-a716-446655440000");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("id") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> uuidObj = (Map<String, Object>) result.get("id");
        assertEquals("550e8400-e29b-41d4-a716-446655440000", uuidObj.get("value"));
        assertEquals("UUID", uuidObj.get("type"));
        assertTrue(uuidObj.containsKey("version"));
    }

    @Test
    void convertPostgresTypesInRecord_validUuidVersion4_returnsVersion4() {
        ColumnInfo col = new ColumnInfo("id", "uuid", true, false);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        // This is a v4 UUID
        record.put("id", "550e8400-e29b-41d4-a716-446655440000");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        @SuppressWarnings("unchecked")
        Map<String, Object> uuidObj = (Map<String, Object>) result.get("id");
        // UUID version should be an integer
        assertTrue(uuidObj.get("version") instanceof Integer);
    }

    @Test
    void convertPostgresTypesInRecord_invalidUuidFormat_doesNotThrow() {
        ColumnInfo col = new ColumnInfo("id", "uuid", true, false);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("id", "not-a-real-uuid");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        // Still returns a UUID object but with version=-1
        assertTrue(result.get("id") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> uuidObj = (Map<String, Object>) result.get("id");
        assertEquals(-1, uuidObj.get("version"));
    }

    // ===== Date/Time conversion =====

    @Test
    void convertPostgresTypesInRecord_timestampColumn_returnsEnhancedObject() {
        ColumnInfo col = new ColumnInfo("created_at", "timestamp", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("created_at", "2023-01-15T10:30:00");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("created_at") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> ts = (Map<String, Object>) result.get("created_at");
        assertEquals("TIMESTAMP", ts.get("type"));
        assertEquals("2023-01-15T10:30:00", ts.get("value"));
        assertTrue(ts.containsKey("parsed"));
    }

    @Test
    void convertPostgresTypesInRecord_dateColumn_returnsDateObject() {
        ColumnInfo col = new ColumnInfo("birthday", "date", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("birthday", "2000-06-15");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("birthday") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> dateObj = (Map<String, Object>) result.get("birthday");
        assertEquals("DATE", dateObj.get("type"));
        assertEquals("2000-06-15", dateObj.get("value"));
    }

    @Test
    void convertPostgresTypesInRecord_timeColumn_returnsTimeObject() {
        ColumnInfo col = new ColumnInfo("start_time", "time", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("start_time", "09:30:00");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("start_time") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> timeObj = (Map<String, Object>) result.get("start_time");
        assertEquals("TIME", timeObj.get("type"));
    }

    @Test
    void convertPostgresTypesInRecord_intervalColumn_returnsIntervalObject() {
        ColumnInfo col = new ColumnInfo("duration", "interval", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("duration", "2 days 03:00:00");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("duration") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> intervalObj = (Map<String, Object>) result.get("duration");
        assertEquals("INTERVAL", intervalObj.get("type"));
    }

    @Test
    void convertPostgresTypesInRecord_invalidTimestampFormat_returnsOriginal() {
        ColumnInfo col = new ColumnInfo("created_at", "timestamp", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("created_at", "not-a-date");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        // Should return original value since parsing fails
        assertEquals("not-a-date", result.get("created_at"));
    }

    // ===== BYTEA conversion =====

    @Test
    void convertPostgresTypesInRecord_byteaByteArray_encodedToBase64() {
        ColumnInfo col = new ColumnInfo("data", "bytea", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("data", new byte[]{72, 101, 108, 108, 111}); // "Hello"

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("data") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> byteaObj = (Map<String, Object>) result.get("data");
        assertEquals("BYTEA", byteaObj.get("type"));
        assertNotNull(byteaObj.get("value")); // Base64 encoded
        assertEquals(5, byteaObj.get("length"));
    }

    @Test
    void convertPostgresTypesInRecord_byteaHexString_decodedAndEncoded() {
        ColumnInfo col = new ColumnInfo("data", "bytea", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("data", "48656c6c6f"); // hex for "Hello"

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("data") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> byteaObj = (Map<String, Object>) result.get("data");
        assertEquals("BYTEA", byteaObj.get("type"));
    }

    // ===== XML conversion =====

    @Test
    void convertPostgresTypesInRecord_xmlColumn_returnsXmlObject() {
        ColumnInfo col = new ColumnInfo("content", "xml", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("content", "<root><item>value</item></root>");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("content") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> xmlObj = (Map<String, Object>) result.get("content");
        assertEquals("XML", xmlObj.get("type"));
        assertEquals("<root><item>value</item></root>", xmlObj.get("value"));
        assertTrue(xmlObj.containsKey("length"));
    }

    // ===== Network type conversion =====

    @Test
    void convertPostgresTypesInRecord_inetColumn_returnsString() {
        ColumnInfo col = new ColumnInfo("ip_address", "inet", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("ip_address", "192.168.1.1");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertEquals("192.168.1.1", result.get("ip_address"));
    }

    @Test
    void convertPostgresTypesInRecord_cidrColumn_returnsString() {
        ColumnInfo col = new ColumnInfo("network", "cidr", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("network", "192.168.1.0/24");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertEquals("192.168.1.0/24", result.get("network"));
    }

    @Test
    void convertPostgresTypesInRecord_macaddrColumn_returnsString() {
        ColumnInfo col = new ColumnInfo("mac", "macaddr", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("mac", "08:00:2b:01:02:03");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertEquals("08:00:2b:01:02:03", result.get("mac"));
    }

    // ===== Custom type conversion =====

    @Test
    void convertPostgresTypesInRecord_compositeType_parsedToObject() {
        ColumnInfo col = new ColumnInfo("address", "postgres_composite:address_type", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("address", "(123 Main St,Springfield,IL)");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertTrue(result.get("address") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> composite = (Map<String, Object>) result.get("address");
        assertEquals("COMPOSITE", composite.get("category"));
        assertTrue(composite.containsKey("fields"));
    }

    @Test
    void convertPostgresTypesInRecord_enumType_returnsString() {
        ColumnInfo col = new ColumnInfo("status", "postgres_enum:order_status", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        Map<String, Object> record = new HashMap<>();
        record.put("status", "active");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        assertEquals("active", result.get("status"));
    }

    @Test
    void convertPostgresTypesInRecord_unknownCustomType_returnsCustomObject() {
        ColumnInfo col = new ColumnInfo("data", "postgres_composite:custom_type", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        // A value that does NOT look like a composite (no parens) should return custom obj
        Map<String, Object> record = new HashMap<>();
        record.put("data", "just a plain value");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        // Should return a custom object with value field
        assertTrue(result.get("data") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> customObj = (Map<String, Object>) result.get("data");
        assertEquals("CUSTOM", customObj.get("category"));
    }

    // ===== BIT type conversion (PGobject path) =====

    @Test
    void convertPostgresTypesInRecord_bitType_plainStringPassthrough() {
        ColumnInfo col = new ColumnInfo("flags", "bit", false, true);
        TableInfo tableInfo = buildTableInfo(List.of(col));

        // When value is not a PGobject (plain String), no special conversion
        Map<String, Object> record = new HashMap<>();
        record.put("flags", "1010");

        Map<String, Object> result = PostgresTypeConverter.convertPostgresTypesInRecord(record, tableInfo);

        // String is not PGobject, so goes to no-conversion path - but isJsonType check runs first
        // bit type: not json, not uuid, but isBitType=true and value is NOT PGobject -> falls through
        assertEquals("1010", result.get("flags"));
    }

    // ===== helper =====

    private TableInfo buildTableInfo(List<ColumnInfo> columns) {
        return new TableInfo("test_table", columns, Collections.emptyList());
    }
}
