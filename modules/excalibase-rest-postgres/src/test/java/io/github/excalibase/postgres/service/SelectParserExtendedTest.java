package io.github.excalibase.postgres.service;

import io.github.excalibase.model.SelectField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for extended select syntax:
 * - Column aliasing:  ?select=alias:col
 * - Type casting:     ?select=col::text
 * - JSON path access: ?select=data->>key, ?select=meta->nested->0->>field
 */
class SelectParserExtendedTest {

    private SelectParserService selectParserService;

    @BeforeEach
    void setup() {
        selectParserService = new SelectParserService();
    }

    // ─── Column aliasing: alias:col ───────────────────────────────────────────

    @Test
    void shouldParseColumnAlias() {
        List<SelectField> fields = selectParserService.parseSelect("fullName:name");

        assertEquals(1, fields.size());
        SelectField f = fields.get(0);
        assertEquals("name", f.getName());          // actual column
        assertEquals("fullName", f.getAlias());     // alias for response
    }

    @Test
    void shouldParseMultipleColumnsWithAliases() {
        List<SelectField> fields = selectParserService.parseSelect("id,fullName:name,yr:year");

        assertEquals(3, fields.size());
        assertEquals("id", fields.get(0).getName());
        assertNull(fields.get(0).getAlias());

        assertEquals("name", fields.get(1).getName());
        assertEquals("fullName", fields.get(1).getAlias());

        assertEquals("year", fields.get(2).getName());
        assertEquals("yr", fields.get(2).getAlias());
    }

    @Test
    void shouldBuildSqlWithAlias() {
        List<SelectField> fields = selectParserService.parseSelect("fullName:name");

        String sql = fields.get(0).toSqlExpression();
        assertEquals("name AS \"fullName\"", sql);
    }

    @Test
    void shouldBuildSqlWithoutAliasForSimpleColumn() {
        List<SelectField> fields = selectParserService.parseSelect("name");

        String sql = fields.get(0).toSqlExpression();
        assertEquals("name", sql);
    }

    // ─── Type casting: col::type ──────────────────────────────────────────────

    @Test
    void shouldParseTypeCastInSelect() {
        List<SelectField> fields = selectParserService.parseSelect("age::text");

        assertEquals(1, fields.size());
        SelectField f = fields.get(0);
        assertEquals("age", f.getName());
        assertEquals("text", f.getCast());
    }

    @Test
    void shouldBuildSqlWithTypeCast() {
        List<SelectField> fields = selectParserService.parseSelect("age::text");

        String sql = fields.get(0).toSqlExpression();
        assertEquals("age::text", sql);
    }

    @Test
    void shouldParseAliasAndCastTogether() {
        // alias:col::type → rename alias, cast to type
        List<SelectField> fields = selectParserService.parseSelect("ageStr:age::text");

        assertEquals(1, fields.size());
        SelectField f = fields.get(0);
        assertEquals("age", f.getName());
        assertEquals("ageStr", f.getAlias());
        assertEquals("text", f.getCast());
    }

    @Test
    void shouldBuildSqlWithAliasAndCast() {
        List<SelectField> fields = selectParserService.parseSelect("ageStr:age::text");

        String sql = fields.get(0).toSqlExpression();
        assertEquals("age::text AS \"ageStr\"", sql);
    }

    // ─── JSON path access: col->>key, col->key ────────────────────────────────

    @Test
    void shouldParseJsonArrowOperator() {
        // data->>key → select JSON text value
        List<SelectField> fields = selectParserService.parseSelect("data->>key");

        assertEquals(1, fields.size());
        SelectField f = fields.get(0);
        // The field name captures the full JSON path expression
        assertEquals("data->>key", f.getName());
        assertTrue(f.isJsonPath(), "Should be flagged as JSON path expression");
    }

    @Test
    void shouldParseJsonObjectOperator() {
        List<SelectField> fields = selectParserService.parseSelect("meta->nested");

        assertEquals(1, fields.size());
        assertTrue(fields.get(0).isJsonPath());
        assertEquals("meta->nested", fields.get(0).getName());
    }

    @Test
    void shouldParseDeepJsonPath() {
        List<SelectField> fields = selectParserService.parseSelect("meta->nested->0->>field");

        assertEquals(1, fields.size());
        assertTrue(fields.get(0).isJsonPath());
        assertEquals("meta->nested->0->>field", fields.get(0).getName());
    }

    @Test
    void shouldParseJsonPathWithAlias() {
        // alias:col->>key
        List<SelectField> fields = selectParserService.parseSelect("cityName:address->>city");

        assertEquals(1, fields.size());
        SelectField f = fields.get(0);
        assertEquals("address->>city", f.getName());
        assertEquals("cityName", f.getAlias());
        assertTrue(f.isJsonPath());
    }

    @Test
    void shouldBuildSqlForJsonPath() {
        List<SelectField> fields = selectParserService.parseSelect("data->>key");

        // JSON path expressions are used as-is in SQL
        String sql = fields.get(0).toSqlExpression();
        assertEquals("data->>key", sql);
    }

    @Test
    void shouldBuildSqlForJsonPathWithAlias() {
        List<SelectField> fields = selectParserService.parseSelect("cityName:address->>city");

        String sql = fields.get(0).toSqlExpression();
        assertEquals("address->>city AS \"cityName\"", sql);
    }

    // ─── Mixed scenarios ──────────────────────────────────────────────────────

    @Test
    void shouldParseMixOfAliasTypeCastAndJsonPath() {
        List<SelectField> fields = selectParserService.parseSelect(
                "id,fullName:name,ageStr:age::text,city:address->>city");

        assertEquals(4, fields.size());

        // id — plain
        assertEquals("id", fields.get(0).getName());
        assertNull(fields.get(0).getAlias());

        // fullName:name — alias
        assertEquals("name", fields.get(1).getName());
        assertEquals("fullName", fields.get(1).getAlias());

        // ageStr:age::text — alias + cast
        assertEquals("age", fields.get(2).getName());
        assertEquals("ageStr", fields.get(2).getAlias());
        assertEquals("text", fields.get(2).getCast());

        // city:address->>city — alias + JSON path
        assertEquals("address->>city", fields.get(3).getName());
        assertEquals("city", fields.get(3).getAlias());
        assertTrue(fields.get(3).isJsonPath());
    }

    @Test
    void shouldNotBreakExistingSimpleSelectParsing() {
        List<SelectField> fields = selectParserService.parseSelect("id,name,email");

        assertEquals(3, fields.size());
        for (SelectField f : fields) {
            assertNull(f.getAlias());
            assertNull(f.getCast());
            assertFalse(f.isJsonPath());
        }
    }

    @Test
    void shouldNotBreakExistingEmbeddedParsing() {
        List<SelectField> fields = selectParserService.parseSelect("title,actors(name,age)");

        assertEquals(2, fields.size());
        assertTrue(fields.get(1).isEmbedded());
    }
}
