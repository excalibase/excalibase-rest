package io.github.excalibase.service;

import io.github.excalibase.model.SelectField;
import io.github.excalibase.postgres.service.SelectParserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SelectParserServiceTest {

    private SelectParserService selectParserService;

    @BeforeEach
    void setup() {
        selectParserService = new SelectParserService();
    }

    @Test
    void shouldParseSimpleSelectFields() {
        List<SelectField> fields = selectParserService.parseSelect("name,age,email");

        assertEquals(3, fields.size());
        assertEquals("name", fields.get(0).getName());
        assertTrue(fields.get(0).isSimpleColumn());
        assertEquals("age", fields.get(1).getName());
        assertTrue(fields.get(1).isSimpleColumn());
        assertEquals("email", fields.get(2).getName());
        assertTrue(fields.get(2).isSimpleColumn());
    }

    @Test
    void shouldParseWildcardSelect() {
        List<SelectField> fields = selectParserService.parseSelect("*");

        assertEquals(1, fields.size());
        assertEquals("*", fields.get(0).getName());
        assertTrue(fields.get(0).isWildcard());
        assertFalse(fields.get(0).isSimpleColumn());
    }

    @Test
    void shouldParseEmbeddedFields() {
        List<SelectField> fields = selectParserService.parseSelect("title,actors(name,age)");

        assertEquals(2, fields.size());
        assertEquals("title", fields.get(0).getName());
        assertTrue(fields.get(0).isSimpleColumn());

        SelectField actorsField = fields.get(1);
        assertEquals("actors", actorsField.getName());
        assertTrue(actorsField.isEmbedded());
        assertEquals(2, actorsField.getSubFields().size());
        assertEquals("name", actorsField.getSubFields().get(0).getName());
        assertEquals("age", actorsField.getSubFields().get(1).getName());
    }

    @Test
    void shouldParseComplexNestedEmbeddedFields() {
        List<SelectField> fields = selectParserService.parseSelect("name,posts(title,content)");

        assertEquals(2, fields.size());
        assertEquals("name", fields.get(0).getName());

        SelectField postsField = fields.get(1);
        assertEquals("posts", postsField.getName());
        assertTrue(postsField.isEmbedded());
        assertEquals(2, postsField.getSubFields().size());
        assertEquals("title", postsField.getSubFields().get(0).getName());
        assertEquals("content", postsField.getSubFields().get(1).getName());
    }

    @Test
    void shouldParseEmbeddedFieldsWithWildcards() {
        List<SelectField> fields = selectParserService.parseSelect("*,actors(*)");

        assertEquals(2, fields.size());
        assertEquals("*", fields.get(0).getName());
        assertTrue(fields.get(0).isWildcard());

        SelectField actorsField = fields.get(1);
        assertEquals("actors", actorsField.getName());
        assertTrue(actorsField.isEmbedded());
        assertEquals(1, actorsField.getSubFields().size());
        assertEquals("*", actorsField.getSubFields().get(0).getName());
        assertTrue(actorsField.getSubFields().get(0).isWildcard());
    }

    @Test
    void shouldHandleEmptySelectParameter() {
        List<SelectField> fields = selectParserService.parseSelect("");

        assertEquals(1, fields.size());
        assertEquals("*", fields.get(0).getName());
        assertTrue(fields.get(0).isWildcard());
    }

    @Test
    void shouldHandleNullSelectParameter() {
        List<SelectField> fields = selectParserService.parseSelect(null);

        assertEquals(1, fields.size());
        assertEquals("*", fields.get(0).getName());
        assertTrue(fields.get(0).isWildcard());
    }

    @Test
    void shouldParseEmbeddedFiltersFromQueryParameters() {
        List<SelectField> fields = selectParserService.parseSelect("title,actors(name,age)");
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("actors.age", "gt.30");
        params.add("actors.nationality", "eq.American");
        params.add("title", "like.Matrix");

        selectParserService.parseEmbeddedFilters(fields, params);

        SelectField actorsField = fields.stream()
                .filter(f -> "actors".equals(f.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull(actorsField);
        assertEquals(2, actorsField.getFilters().size());
        assertEquals("gt.30", actorsField.getFilters().get("age"));
        assertEquals("eq.American", actorsField.getFilters().get("nationality"));

        // Regular filters should not be added to embedded fields
        SelectField titleField = fields.stream()
                .filter(f -> "title".equals(f.getName()))
                .findFirst()
                .orElse(null);
        assertNotNull(titleField);
        assertTrue(titleField.getFilters().isEmpty());
    }

    @Test
    void shouldIdentifySimpleColumnNamesCorrectly() {
        List<SelectField> fields = selectParserService.parseSelect("name,age,actors(first_name)");
        List<String> simpleColumns = selectParserService.getSimpleColumnNames(fields);

        assertEquals(2, simpleColumns.size());
        assertTrue(simpleColumns.contains("name"));
        assertTrue(simpleColumns.contains("age"));
    }

    @Test
    void shouldIdentifyEmbeddedFieldsCorrectly() {
        List<SelectField> fields = selectParserService.parseSelect("name,actors(first_name),posts(title)");
        List<SelectField> embeddedFields = selectParserService.getEmbeddedFields(fields);

        assertEquals(2, embeddedFields.size());
        assertEquals("actors", embeddedFields.get(0).getName());
        assertEquals("posts", embeddedFields.get(1).getName());
    }

    @Test
    void shouldDetectEmbeddedFieldsPresence() {
        assertFalse(selectParserService.hasEmbeddedFields(
                selectParserService.parseSelect("name,age")));
        assertTrue(selectParserService.hasEmbeddedFields(
                selectParserService.parseSelect("name,actors(age)")));
    }

    @Test
    void shouldHandleMalformedEmbeddedSyntaxGracefully() {
        List<SelectField> fields = selectParserService.parseSelect("name,actors(incomplete");

        assertEquals(2, fields.size());
        assertEquals("name", fields.get(0).getName());
        assertEquals("actors(incomplete", fields.get(1).getName());
        assertFalse(fields.get(1).isEmbedded());
    }

    @Test
    void shouldParseMixedSimpleAndEmbeddedFieldsWithComplexNesting() {
        List<SelectField> fields = selectParserService.parseSelect("id,title,actors(name,age),reviews(*)");

        assertEquals(4, fields.size());

        // Simple fields
        assertEquals("id", fields.get(0).getName());
        assertEquals("title", fields.get(1).getName());

        // Embedded field
        SelectField actorsField = fields.get(2);
        assertEquals("actors", actorsField.getName());
        assertTrue(actorsField.isEmbedded());
        assertEquals(2, actorsField.getSubFields().size());
        assertEquals("name", actorsField.getSubFields().get(0).getName());
        assertEquals("age", actorsField.getSubFields().get(1).getName());

        // Embedded field with wildcard
        SelectField reviewsField = fields.get(3);
        assertEquals("reviews", reviewsField.getName());
        assertTrue(reviewsField.isEmbedded());
        assertEquals(1, reviewsField.getSubFields().size());
        assertTrue(reviewsField.getSubFields().get(0).isWildcard());
    }
}
