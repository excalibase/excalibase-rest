package io.github.excalibase.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class TypeConversionServiceTest {

    @Mock
    private IValidationService validationService;

    private TypeConversionService service;

    // Helper: build a TableInfo with a single column
    private TableInfo tableWithColumn(String colName, String colType) {
        ColumnInfo col = new ColumnInfo(colName, colType, false, true);
        return new TableInfo("test_table", List.of(col), List.of());
    }

    @BeforeEach
    void setUp() {
        service = new TypeConversionService(validationService, new com.fasterxml.jackson.databind.ObjectMapper());
    }

    // ── convertValueToColumnType(name, String) — pattern-matching ────────────

    @Test
    void convertString_integer_parsesAsInteger() {
        Object result = service.convertValueToColumnType("col", "42");
        assertEquals(42, result);
        assertInstanceOf(Integer.class, result);
    }

    @Test
    void convertString_negativeInteger_parsesAsInteger() {
        Object result = service.convertValueToColumnType("col", "-7");
        assertEquals(-7, result);
    }

    @Test
    void convertString_decimal_parsesAsDouble() {
        Object result = service.convertValueToColumnType("col", "3.14");
        assertEquals(3.14, (double) result, 0.0001);
        assertInstanceOf(Double.class, result);
    }

    @Test
    void convertString_negativeDecimal_parsesAsDouble() {
        Object result = service.convertValueToColumnType("col", "-9.99");
        assertEquals(-9.99, (double) result, 0.0001);
    }

    @Test
    void convertString_booleanTrue_parsesAsTrue() {
        Object result = service.convertValueToColumnType("col", "true");
        assertEquals(Boolean.TRUE, result);
    }

    @Test
    void convertString_booleanFalse_parsesAsFalse() {
        Object result = service.convertValueToColumnType("col", "false");
        assertEquals(Boolean.FALSE, result);
    }

    @Test
    void convertString_booleanUppercase_parsesAsBoolean() {
        Object result = service.convertValueToColumnType("col", "TRUE");
        assertEquals(Boolean.TRUE, result);
    }

    @Test
    void convertString_plainString_returnsAsIs() {
        Object result = service.convertValueToColumnType("col", "hello world");
        assertEquals("hello world", result);
    }

    @Test
    void convertString_uuidString_returnsAsIs() {
        String uuid = "550e8400-e29b-41d4-a716-446655440000";
        Object result = service.convertValueToColumnType("col", uuid);
        assertEquals(uuid, result);
    }

    // ── convertValueToColumnType(name, String, TableInfo) — type-aware ────────

    @Test
    void convertStringWithTable_nullValue_returnsNull() {
        TableInfo table = tableWithColumn("age", "integer");
        assertNull(service.convertValueToColumnType("age", (String) null, table));
    }

    @Test
    void convertStringWithTable_integerColumn_returnsInteger() {
        TableInfo table = tableWithColumn("age", "integer");
        Object result = service.convertValueToColumnType("age", "25", table);
        assertEquals(25, result);
    }

    @Test
    void convertStringWithTable_intColumn_returnsInteger() {
        TableInfo table = tableWithColumn("qty", "int");
        Object result = service.convertValueToColumnType("qty", "10", table);
        assertEquals(10, result);
    }

    @Test
    void convertStringWithTable_bigintColumn_returnsInteger() {
        TableInfo table = tableWithColumn("big_id", "bigint");
        Object result = service.convertValueToColumnType("big_id", "999", table);
        assertEquals(999, result);
    }

    @Test
    void convertStringWithTable_serialColumn_returnsInteger() {
        TableInfo table = tableWithColumn("id", "serial");
        Object result = service.convertValueToColumnType("id", "1", table);
        assertEquals(1, result);
    }

    @Test
    void convertStringWithTable_bigserialColumn_returnsInteger() {
        TableInfo table = tableWithColumn("id", "bigserial");
        Object result = service.convertValueToColumnType("id", "100", table);
        assertEquals(100, result);
    }

    @Test
    void convertStringWithTable_smallserialColumn_returnsInteger() {
        TableInfo table = tableWithColumn("id", "smallserial");
        Object result = service.convertValueToColumnType("id", "5", table);
        assertEquals(5, result);
    }

    @Test
    void convertStringWithTable_decimalColumn_returnsDouble() {
        TableInfo table = tableWithColumn("price", "decimal");
        Object result = service.convertValueToColumnType("price", "19.99", table);
        assertEquals(19.99, (double) result, 0.0001);
    }

    @Test
    void convertStringWithTable_numericColumn_returnsDouble() {
        TableInfo table = tableWithColumn("amount", "numeric");
        Object result = service.convertValueToColumnType("amount", "100.50", table);
        assertInstanceOf(Double.class, result);
    }

    @Test
    void convertStringWithTable_realColumn_returnsDouble() {
        TableInfo table = tableWithColumn("weight", "real");
        Object result = service.convertValueToColumnType("weight", "1.5", table);
        assertInstanceOf(Double.class, result);
    }

    @Test
    void convertStringWithTable_doubleColumn_returnsDouble() {
        TableInfo table = tableWithColumn("ratio", "double");
        Object result = service.convertValueToColumnType("ratio", "0.5", table);
        assertInstanceOf(Double.class, result);
    }

    @Test
    void convertStringWithTable_floatColumn_returnsDouble() {
        TableInfo table = tableWithColumn("score", "float");
        Object result = service.convertValueToColumnType("score", "95.5", table);
        assertInstanceOf(Double.class, result);
    }

    @Test
    void convertStringWithTable_booleanColumn_returnsBoolean() {
        TableInfo table = tableWithColumn("active", "boolean");
        Object result = service.convertValueToColumnType("active", "true", table);
        assertEquals(Boolean.TRUE, result);
    }

    @Test
    void convertStringWithTable_boolColumn_returnsBoolean() {
        TableInfo table = tableWithColumn("flag", "bool");
        Object result = service.convertValueToColumnType("flag", "false", table);
        assertEquals(Boolean.FALSE, result);
    }

    @Test
    void convertStringWithTable_timestampColumn_returnsTimestamp_isoFormat() {
        TableInfo table = tableWithColumn("created_at", "timestamp");
        Object result = service.convertValueToColumnType("created_at", "2025-01-15T10:30:00", table);
        assertInstanceOf(Timestamp.class, result);
    }

    @Test
    void convertStringWithTable_timestampColumn_returnsTimestamp_dateOnlyFormat() {
        TableInfo table = tableWithColumn("created_at", "timestamp");
        Object result = service.convertValueToColumnType("created_at", "2025-01-15", table);
        assertInstanceOf(Timestamp.class, result);
    }

    @Test
    void convertStringWithTable_timestampColumn_returnsTimestamp_zSuffixFormat() {
        TableInfo table = tableWithColumn("created_at", "timestamp");
        Object result = service.convertValueToColumnType("created_at", "2025-01-15T10:30:00Z", table);
        assertInstanceOf(Timestamp.class, result);
    }

    @Test
    void convertStringWithTable_timestampColumn_invalidFormat_returnsString() {
        TableInfo table = tableWithColumn("created_at", "timestamp");
        Object result = service.convertValueToColumnType("created_at", "not-a-date", table);
        assertInstanceOf(String.class, result);
    }

    @Test
    void convertStringWithTable_dateColumn_returnsSqlDate() {
        TableInfo table = tableWithColumn("birth_date", "date");
        Object result = service.convertValueToColumnType("birth_date", "1990-05-20", table);
        assertInstanceOf(Date.class, result);
    }

    @Test
    void convertStringWithTable_dateColumn_invalidDate_returnsString() {
        TableInfo table = tableWithColumn("d", "date");
        Object result = service.convertValueToColumnType("d", "not-a-date", table);
        assertInstanceOf(String.class, result);
    }

    @Test
    void convertStringWithTable_timeColumn_returnsString() {
        TableInfo table = tableWithColumn("start_time", "time");
        Object result = service.convertValueToColumnType("start_time", "10:30:00", table);
        assertEquals("10:30:00", result);
    }

    @Test
    void convertStringWithTable_jsonbColumn_returnsValue() {
        TableInfo table = tableWithColumn("meta", "jsonb");
        Object result = service.convertValueToColumnType("meta", "{\"key\":\"val\"}", table);
        assertEquals("{\"key\":\"val\"}", result);
    }

    @Test
    void convertStringWithTable_jsonColumn_returnsValue() {
        TableInfo table = tableWithColumn("data", "json");
        Object result = service.convertValueToColumnType("data", "{}", table);
        assertEquals("{}", result);
    }

    @Test
    void convertStringWithTable_enumColumn_returnsString() {
        TableInfo table = tableWithColumn("status", "postgres_enum:status_type");
        Object result = service.convertValueToColumnType("status", "active", table);
        assertEquals("active", result);
    }

    @Test
    void convertStringWithTable_compositeColumn_returnsString() {
        TableInfo table = tableWithColumn("address", "postgres_composite:address_type");
        Object result = service.convertValueToColumnType("address", "(123 Main St,Springfield)", table);
        assertEquals("(123 Main St,Springfield)", result);
    }

    @Test
    void convertStringWithTable_inetColumn_returnsString() {
        TableInfo table = tableWithColumn("ip_addr", "inet");
        Object result = service.convertValueToColumnType("ip_addr", "192.168.1.1", table);
        assertEquals("192.168.1.1", result);
    }

    @Test
    void convertStringWithTable_cidrColumn_returnsString() {
        TableInfo table = tableWithColumn("network", "cidr");
        Object result = service.convertValueToColumnType("network", "192.168.1.0/24", table);
        assertEquals("192.168.1.0/24", result);
    }

    @Test
    void convertStringWithTable_macaddrColumn_returnsString() {
        TableInfo table = tableWithColumn("mac", "macaddr");
        Object result = service.convertValueToColumnType("mac", "08:00:2b:01:02:03", table);
        assertEquals("08:00:2b:01:02:03", result);
    }

    @Test
    void convertStringWithTable_macaddr8Column_returnsString() {
        TableInfo table = tableWithColumn("mac8", "macaddr8");
        Object result = service.convertValueToColumnType("mac8", "08:00:2b:01:02:03:04:05", table);
        assertEquals("08:00:2b:01:02:03:04:05", result);
    }

    @Test
    void convertStringWithTable_bitColumn_returnsString() {
        TableInfo table = tableWithColumn("flags", "bit(8)");
        Object result = service.convertValueToColumnType("flags", "10101010", table);
        assertEquals("10101010", result);
    }

    @Test
    void convertStringWithTable_columnNotFound_fallsBackToPatternMatching() {
        TableInfo table = tableWithColumn("other_col", "text");
        Object result = service.convertValueToColumnType("missing_col", "42", table);
        // Should fall back to basic pattern matching -> Integer
        assertEquals(42, result);
    }

    @Test
    void convertStringWithTable_unknownType_numericLike_returnsInteger() {
        TableInfo table = tableWithColumn("myfield", "custom_domain_type");
        Object result = service.convertValueToColumnType("myfield", "100", table);
        assertEquals(100, result);
    }

    @Test
    void convertStringWithTable_unknownType_decimalLike_returnsDouble() {
        TableInfo table = tableWithColumn("myfield", "custom_domain_type");
        Object result = service.convertValueToColumnType("myfield", "1.23", table);
        assertEquals(1.23, (double) result, 0.0001);
    }

    @Test
    void convertStringWithTable_unknownType_plainString_returnsString() {
        TableInfo table = tableWithColumn("myfield", "custom_domain_type");
        Object result = service.convertValueToColumnType("myfield", "some-text", table);
        assertEquals("some-text", result);
    }

    @Test
    void convertStringWithTable_numberFormatException_returnsString() {
        // "integer" type but value is not parseable
        TableInfo table = tableWithColumn("age", "integer");
        Object result = service.convertValueToColumnType("age", "not_a_number", table);
        // Should fall back to string on NumberFormatException
        assertInstanceOf(String.class, result);
        assertEquals("not_a_number", result);
    }

    // ── convertValueToColumnType(name, Object, TableInfo) ────────────────────

    @Test
    void convertObjectWithTable_nullValue_returnsNull() {
        TableInfo table = tableWithColumn("col", "text");
        assertNull(service.convertValueToColumnType("col", (Object) null, table));
    }

    @Test
    void convertObjectWithTable_jsonbColumn_mapValue_serializedToJson() throws Exception {
        TableInfo table = tableWithColumn("meta", "jsonb");
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        Object result = service.convertValueToColumnType("meta", map, table);
        assertInstanceOf(String.class, result);
        assertTrue(result.toString().contains("key"));
        assertTrue(result.toString().contains("value"));
    }

    @Test
    void convertObjectWithTable_jsonColumn_listValue_serializedToJson() throws Exception {
        TableInfo table = tableWithColumn("tags", "json");
        List<String> list = List.of("tag1", "tag2");
        Object result = service.convertValueToColumnType("tags", list, table);
        assertInstanceOf(String.class, result);
        assertTrue(result.toString().contains("tag1"));
    }

    @Test
    void convertObjectWithTable_jsonbColumn_validJsonString_returnedAsIs() {
        TableInfo table = tableWithColumn("meta", "jsonb");
        Object result = service.convertValueToColumnType("meta", "{\"k\":1}", table);
        assertEquals("{\"k\":1}", result);
    }

    @Test
    void convertObjectWithTable_jsonbColumn_invalidJsonString_returnedAsIs() {
        TableInfo table = tableWithColumn("meta", "jsonb");
        Object result = service.convertValueToColumnType("meta", "not-json", table);
        assertEquals("not-json", result);
    }

    @Test
    void convertObjectWithTable_jsonbColumn_otherType_convertedToString() {
        TableInfo table = tableWithColumn("meta", "jsonb");
        Object result = service.convertValueToColumnType("meta", 42, table);
        assertEquals("42", result);
    }

    @Test
    void convertObjectWithTable_arrayColumn_listValue_returnsPostgresArrayLiteral() {
        TableInfo table = tableWithColumn("tags", "_text");
        List<String> list = List.of("a", "b", "c");
        Object result = service.convertValueToColumnType("tags", list, table);
        assertInstanceOf(String.class, result);
        String str = (String) result;
        assertTrue(str.startsWith("{"));
        assertTrue(str.endsWith("}"));
        assertTrue(str.contains("\"a\""));
    }

    @Test
    void convertObjectWithTable_arrayColumnWithBrackets_listValue_returnsArrayLiteral() {
        TableInfo table = tableWithColumn("ids", "integer[]");
        List<Integer> list = List.of(1, 2, 3);
        Object result = service.convertValueToColumnType("ids", list, table);
        assertInstanceOf(String.class, result);
        assertTrue(result.toString().contains("{"));
    }

    @Test
    void convertObjectWithTable_arrayColumn_stringValue_returnedAsIs() {
        TableInfo table = tableWithColumn("tags", "_text");
        Object result = service.convertValueToColumnType("tags", "{a,b}", table);
        assertEquals("{a,b}", result);
    }

    @Test
    void convertObjectWithTable_arrayColumn_otherType_convertedToString() {
        TableInfo table = tableWithColumn("tags", "_text");
        Object result = service.convertValueToColumnType("tags", 123, table);
        assertEquals("123", result);
    }

    @Test
    void convertObjectWithTable_arrayColumn_nestedList_handledRecursively() {
        TableInfo table = tableWithColumn("matrix", "_int4");
        List<List<Integer>> matrix = List.of(List.of(1, 2), List.of(3, 4));
        Object result = service.convertValueToColumnType("matrix", matrix, table);
        assertInstanceOf(String.class, result);
        String str = (String) result;
        // Should contain nested braces
        assertTrue(str.contains("{"));
    }

    @Test
    void convertObjectWithTable_arrayColumn_nullItemInList_handledAsNULL() {
        TableInfo table = tableWithColumn("tags", "_text");
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add(null);
        list.add("b");
        Object result = service.convertValueToColumnType("tags", list, table);
        assertInstanceOf(String.class, result);
        assertTrue(result.toString().contains("NULL"));
    }

    @Test
    void convertObjectWithTable_columnNotFound_usesStringConversion() {
        TableInfo table = tableWithColumn("other", "text");
        Object result = service.convertValueToColumnType("missing", "42", table);
        // Falls back to convertValueToColumnType(name, value.toString()) -> Integer
        assertEquals(42, result);
    }

    @Test
    void convertObjectWithTable_nonJsonbColumn_integerObject_convertsViaStringMethod() {
        TableInfo table = tableWithColumn("age", "integer");
        Object result = service.convertValueToColumnType("age", 25, table);
        assertEquals(25, result);
    }

    // ── buildPlaceholderWithCast ──────────────────────────────────────────────

    @Test
    void buildPlaceholder_columnNotFound_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("other", "text");
        String result = service.buildPlaceholderWithCast("missing", table);
        assertEquals("?", result);
    }

    @Test
    void buildPlaceholder_textColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("name", "text");
        assertEquals("?", service.buildPlaceholderWithCast("name", table));
    }

    @Test
    void buildPlaceholder_varcharColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("email", "varchar");
        assertEquals("?", service.buildPlaceholderWithCast("email", table));
    }

    @Test
    void buildPlaceholder_integerColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("age", "integer");
        assertEquals("?", service.buildPlaceholderWithCast("age", table));
    }

    @Test
    void buildPlaceholder_arrayColumn_returnsCastPlaceholder() {
        TableInfo table = tableWithColumn("tags", "text[]");
        assertEquals("?::text[]", service.buildPlaceholderWithCast("tags", table));
    }

    @Test
    void buildPlaceholder_integerArrayColumn_returnsCastPlaceholder() {
        TableInfo table = tableWithColumn("ids", "integer[]");
        assertEquals("?::integer[]", service.buildPlaceholderWithCast("ids", table));
    }

    @Test
    void buildPlaceholder_inetColumn_returnsCastPlaceholder() {
        TableInfo table = tableWithColumn("ip", "inet");
        assertEquals("?::inet", service.buildPlaceholderWithCast("ip", table));
    }

    @Test
    void buildPlaceholder_cidrColumn_returnsCastPlaceholder() {
        TableInfo table = tableWithColumn("network", "cidr");
        assertEquals("?::cidr", service.buildPlaceholderWithCast("network", table));
    }

    @Test
    void buildPlaceholder_macaddrColumn_returnsCastPlaceholder() {
        TableInfo table = tableWithColumn("mac", "macaddr");
        assertEquals("?::macaddr", service.buildPlaceholderWithCast("mac", table));
    }

    @Test
    void buildPlaceholder_macaddr8Column_returnsCastPlaceholder() {
        TableInfo table = tableWithColumn("mac8", "macaddr8");
        assertEquals("?::macaddr8", service.buildPlaceholderWithCast("mac8", table));
    }

    @Test
    void buildPlaceholder_bitColumn_returnsCastPlaceholder() {
        TableInfo table = tableWithColumn("flags", "bit(8)");
        assertEquals("?::bit(8)", service.buildPlaceholderWithCast("flags", table));
    }

    @Test
    void buildPlaceholder_bitVaryingColumn_returnsCastPlaceholder() {
        TableInfo table = tableWithColumn("bits", "bit varying");
        assertEquals("?::bit varying", service.buildPlaceholderWithCast("bits", table));
    }

    @Test
    void buildPlaceholder_enumColumn_returnsCastWithEnumType() {
        TableInfo table = tableWithColumn("status", "postgres_enum:my_status");
        assertEquals("?::my_status", service.buildPlaceholderWithCast("status", table));
    }

    @Test
    void buildPlaceholder_jsonbColumn_returnsCastJsonb() {
        TableInfo table = tableWithColumn("meta", "jsonb");
        assertEquals("?::jsonb", service.buildPlaceholderWithCast("meta", table));
    }

    @Test
    void buildPlaceholder_jsonColumn_returnsCastJson() {
        TableInfo table = tableWithColumn("data", "json");
        assertEquals("?::json", service.buildPlaceholderWithCast("data", table));
    }

    @Test
    void buildPlaceholder_uuidColumn_returnsCastUuid() {
        TableInfo table = tableWithColumn("id", "uuid");
        assertEquals("?::uuid", service.buildPlaceholderWithCast("id", table));
    }

    @Test
    void buildPlaceholder_timestamptzColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("ts", "timestamptz");
        assertEquals("?", service.buildPlaceholderWithCast("ts", table));
    }

    @Test
    void buildPlaceholder_timestampWithTimeZoneColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("ts", "timestamp with time zone");
        assertEquals("?", service.buildPlaceholderWithCast("ts", table));
    }

    @Test
    void buildPlaceholder_timestampColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("ts", "timestamp");
        assertEquals("?", service.buildPlaceholderWithCast("ts", table));
    }

    @Test
    void buildPlaceholder_timestampWithoutTimeZoneColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("ts", "timestamp without time zone");
        assertEquals("?", service.buildPlaceholderWithCast("ts", table));
    }

    @Test
    void buildPlaceholder_dateColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("d", "date");
        assertEquals("?", service.buildPlaceholderWithCast("d", table));
    }

    @Test
    void buildPlaceholder_timeColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("t", "time");
        assertEquals("?", service.buildPlaceholderWithCast("t", table));
    }

    @Test
    void buildPlaceholder_timeWithoutTimeZoneColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("t", "time without time zone");
        assertEquals("?", service.buildPlaceholderWithCast("t", table));
    }

    @Test
    void buildPlaceholder_timetzColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("t", "timetz");
        assertEquals("?", service.buildPlaceholderWithCast("t", table));
    }

    @Test
    void buildPlaceholder_timeWithTimeZoneColumn_returnsSimpleQuestion() {
        TableInfo table = tableWithColumn("t", "time with time zone");
        assertEquals("?", service.buildPlaceholderWithCast("t", table));
    }

    @Test
    void buildPlaceholder_domainTypeWithDot_returnsCastWithDomain() {
        TableInfo table = tableWithColumn("yr", "public.year");
        assertEquals("?::public.year", service.buildPlaceholderWithCast("yr", table));
    }

    // ── getColumnType ─────────────────────────────────────────────────────────

    @Test
    void getColumnType_regularType_returnsType() {
        TableInfo table = tableWithColumn("name", "text");
        assertEquals("text", service.getColumnType("name", table));
    }

    @Test
    void getColumnType_arrayType_stripsArraySuffix() {
        TableInfo table = tableWithColumn("tags", "text[]");
        assertEquals("text", service.getColumnType("tags", table));
    }

    @Test
    void getColumnType_integerArrayType_stripsArraySuffix() {
        TableInfo table = tableWithColumn("ids", "integer[]");
        assertEquals("integer", service.getColumnType("ids", table));
    }

    @Test
    void getColumnType_columnNotFound_returnsDefaultText() {
        TableInfo table = tableWithColumn("other", "integer");
        assertEquals("text", service.getColumnType("missing", table));
    }

    @Test
    void getColumnType_returnsLowercase() {
        TableInfo table = tableWithColumn("col", "VARCHAR");
        assertEquals("varchar", service.getColumnType("col", table));
    }
}
