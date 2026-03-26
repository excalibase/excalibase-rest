package io.github.excalibase.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SelectFieldTest {

    // ── Constructor: name only ────────────────────────────────────────────────

    @Test
    void constructor_name_setsAllDefaults() {
        SelectField field = new SelectField("username");
        assertEquals("username", field.getName());
        assertNull(field.getAlias());
        assertNull(field.getCast());
        assertFalse(field.isJsonPath());
        assertFalse(field.isWildcard());
        assertFalse(field.isInner());
        assertFalse(field.isEmbedded());
        assertTrue(field.getSubFields().isEmpty());
        assertTrue(field.getFilters().isEmpty());
    }

    @Test
    void constructor_name_wildcard_setsIsWildcard() {
        SelectField field = new SelectField("*");
        assertTrue(field.isWildcard());
    }

    @Test
    void constructor_name_jsonPath_setsIsJsonPath() {
        SelectField field = new SelectField("data->>'key'");
        assertTrue(field.isJsonPath());
    }

    @Test
    void constructor_name_arrowPath_setsIsJsonPath() {
        SelectField field = new SelectField("data->'nested'");
        assertTrue(field.isJsonPath());
    }

    // ── Constructor: name + subFields ─────────────────────────────────────────

    @Test
    void constructor_nameAndSubFields_setsEmbedded() {
        SelectField sub = new SelectField("name");
        SelectField field = new SelectField("actors", List.of(sub));
        assertEquals("actors", field.getName());
        assertTrue(field.isEmbedded());
        assertFalse(field.isInner());
        assertEquals(1, field.getSubFields().size());
    }

    @Test
    void constructor_nameAndSubFields_copiesListDefensively() {
        SelectField sub1 = new SelectField("name");
        List<SelectField> original = new java.util.ArrayList<>(List.of(sub1));
        SelectField field = new SelectField("actors", original);
        original.add(new SelectField("age"));
        assertEquals(1, field.getSubFields().size()); // not affected by external mutation
    }

    // ── Constructor: name + subFields + inner ─────────────────────────────────

    @Test
    void constructor_nameSubFieldsInner_setsInnerFlag() {
        SelectField sub = new SelectField("id");
        SelectField field = new SelectField("orders", List.of(sub), true);
        assertTrue(field.isInner());
        assertTrue(field.isEmbedded());
    }

    @Test
    void constructor_nameSubFieldsInner_falseInnerFlag() {
        SelectField sub = new SelectField("id");
        SelectField field = new SelectField("orders", List.of(sub), false);
        assertFalse(field.isInner());
    }

    // ── Constructor: name + alias + cast ─────────────────────────────────────

    @Test
    void constructor_nameAliasCast_setsAllFields() {
        SelectField field = new SelectField("age", "years", "text");
        assertEquals("age", field.getName());
        assertEquals("years", field.getAlias());
        assertEquals("text", field.getCast());
    }

    @Test
    void constructor_nameAliasNullCast_works() {
        SelectField field = new SelectField("name", "fullName", null);
        assertEquals("name", field.getName());
        assertEquals("fullName", field.getAlias());
        assertNull(field.getCast());
    }

    @Test
    void constructor_nameNullAliasCast_works() {
        SelectField field = new SelectField("age", null, "integer");
        assertNull(field.getAlias());
        assertEquals("integer", field.getCast());
    }

    @Test
    void constructor_nameAliasNull_wildcardDetected() {
        SelectField field = new SelectField("*", null, null);
        assertTrue(field.isWildcard());
    }

    // ── toSqlExpression ───────────────────────────────────────────────────────

    @Test
    void toSqlExpression_simpleColumn_quotesName() {
        SelectField field = new SelectField("username");
        assertEquals("\"username\"", field.toSqlExpression());
    }

    @Test
    void toSqlExpression_withAlias_appendsAs() {
        SelectField field = new SelectField("first_name", "fn", null);
        assertEquals("\"first_name\" AS \"fn\"", field.toSqlExpression());
    }

    @Test
    void toSqlExpression_withCast_appendsCast() {
        SelectField field = new SelectField("age", null, "text");
        assertEquals("\"age\"::text", field.toSqlExpression());
    }

    @Test
    void toSqlExpression_withAliasAndCast_appendsBoth() {
        SelectField field = new SelectField("age", "years", "text");
        assertEquals("\"age\"::text AS \"years\"", field.toSqlExpression());
    }

    @Test
    void toSqlExpression_jsonPath_doesNotQuote() {
        SelectField field = new SelectField("data->>'city'");
        assertEquals("data->>'city'", field.toSqlExpression());
    }

    @Test
    void toSqlExpression_jsonPathWithAlias_appendsAs() {
        SelectField field = new SelectField("data->>'city'", "city", null);
        assertEquals("data->>'city' AS \"city\"", field.toSqlExpression());
    }

    @Test
    void toSqlExpression_aggregateFunction_doesNotQuote() {
        SelectField field = new SelectField("count(*)");
        assertTrue(field.toSqlExpression().contains("count(*)"));
    }

    @Test
    void toSqlExpression_wildcardColumn_quotesAsterisks() {
        SelectField field = new SelectField("*");
        // * is not a JSON path, not an aggregate, so it gets quoted
        assertEquals("\"*\"", field.toSqlExpression());
    }

    // ── isSimpleColumn ────────────────────────────────────────────────────────

    @Test
    void isSimpleColumn_trueForRegularColumn() {
        SelectField field = new SelectField("name");
        assertTrue(field.isSimpleColumn());
    }

    @Test
    void isSimpleColumn_falseForWildcard() {
        SelectField field = new SelectField("*");
        assertFalse(field.isSimpleColumn());
    }

    @Test
    void isSimpleColumn_falseForEmbedded() {
        SelectField field = new SelectField("actors", List.of(new SelectField("name")));
        assertFalse(field.isSimpleColumn());
    }

    // ── isEmbedded ────────────────────────────────────────────────────────────

    @Test
    void isEmbedded_trueWhenSubFieldsPresent() {
        SelectField field = new SelectField("orders", List.of(new SelectField("id")));
        assertTrue(field.isEmbedded());
    }

    @Test
    void isEmbedded_falseWhenNoSubFields() {
        SelectField field = new SelectField("name");
        assertFalse(field.isEmbedded());
    }

    // ── addSubField ───────────────────────────────────────────────────────────

    @Test
    void addSubField_appendsToSubFieldList() {
        SelectField parent = new SelectField("actors");
        SelectField child = new SelectField("name");
        parent.addSubField(child);
        assertEquals(1, parent.getSubFields().size());
        assertTrue(parent.isEmbedded());
    }

    @Test
    void addSubField_multipleSubFields() {
        SelectField parent = new SelectField("actors");
        parent.addSubField(new SelectField("name"));
        parent.addSubField(new SelectField("age"));
        assertEquals(2, parent.getSubFields().size());
    }

    // ── addFilter ─────────────────────────────────────────────────────────────

    @Test
    void addFilter_storesFilter() {
        SelectField field = new SelectField("actors");
        field.addFilter("age", "gt.30");
        assertEquals("gt.30", field.getFilters().get("age"));
    }

    @Test
    void addFilter_multipleFilters() {
        SelectField field = new SelectField("actors");
        field.addFilter("age", "gt.30");
        field.addFilter("status", "eq.active");
        assertEquals(2, field.getFilters().size());
    }

    // ── getSimpleColumnNames ──────────────────────────────────────────────────

    @Test
    void getSimpleColumnNames_simpleColumn_returnsName() {
        SelectField field = new SelectField("name");
        List<String> names = field.getSimpleColumnNames();
        assertEquals(1, names.size());
        assertEquals("name", names.get(0));
    }

    @Test
    void getSimpleColumnNames_wildcard_returnsAsterisk() {
        SelectField field = new SelectField("*");
        List<String> names = field.getSimpleColumnNames();
        assertEquals(1, names.size());
        assertEquals("*", names.get(0));
    }

    @Test
    void getSimpleColumnNames_embedded_returnsForeignKeyName() {
        SelectField field = new SelectField("customer", List.of(new SelectField("id")));
        List<String> names = field.getSimpleColumnNames();
        assertEquals(1, names.size());
        assertEquals("customer_id", names.get(0));
    }

    // ── toString ──────────────────────────────────────────────────────────────

    @Test
    void toString_simpleColumn_returnsName() {
        SelectField field = new SelectField("username");
        assertEquals("username", field.toString());
    }

    @Test
    void toString_embeddedField_returnsNameWithSubfields() {
        SelectField parent = new SelectField("actors");
        parent.addSubField(new SelectField("name"));
        parent.addSubField(new SelectField("age"));
        assertEquals("actors(name,age)", parent.toString());
    }

    @Test
    void toString_nestedEmbedded_returnsNestedRepresentation() {
        SelectField inner = new SelectField("address");
        inner.addSubField(new SelectField("city"));
        SelectField outer = new SelectField("customer");
        outer.addSubField(new SelectField("name"));
        outer.addSubField(inner);
        String result = outer.toString();
        assertTrue(result.startsWith("customer("));
        assertTrue(result.contains("address(city)"));
    }
}
