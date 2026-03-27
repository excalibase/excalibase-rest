package io.github.excalibase.postgres.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.TypeConversionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;

/**
 * Tests for extended filter syntax:
 * - not prefix: ?col=not.eq.5
 * - nested and/or: ?or=(a.eq.1,not.and(b.gte.5,b.lte.10))
 * - is operator: ?col=is.null, ?col=is.unknown
 * - match/imatch: POSIX regex
 * - isdistinct: IS DISTINCT FROM
 * - phfts: phraseto_tsquery (fix plfts→plainto_tsquery)
 */
@ExtendWith(MockitoExtension.class)
class FilterServiceExtendedTest {

    @Mock(lenient = true)
    private JdbcTemplate jdbcTemplate;

    @Mock(lenient = true)
    private DatabaseSchemaService schemaService;

    private FilterService filterService;
    private TableInfo tableInfo;

    @BeforeEach
    void setup() {
        lenient().when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), any(), any())).thenReturn(true);
        lenient().when(jdbcTemplate.queryForObject(eq("SELECT current_user"), eq(String.class)))
                .thenReturn("test_user");

        ValidationService validationService = new ValidationService(jdbcTemplate, schemaService);
        com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
        TypeConversionService typeConversionService = new TypeConversionService(validationService, objectMapper);
        filterService = new FilterService(validationService, typeConversionService, objectMapper);

        // Build a simple table with integer, text, and boolean columns
        List<ColumnInfo> columns = new ArrayList<>();
        columns.add(new ColumnInfo("id", "integer", true, false));
        columns.add(new ColumnInfo("name", "text", false, true));
        columns.add(new ColumnInfo("age", "integer", false, true));
        columns.add(new ColumnInfo("active", "boolean", false, true));
        columns.add(new ColumnInfo("score", "numeric", false, true));
        columns.add(new ColumnInfo("description", "text", false, true));
        columns.add(new ColumnInfo("age_range", "int4range", false, true));
        tableInfo = new TableInfo("users", columns, List.of(), false);
    }

    // ─── Phase 1A: not prefix ─────────────────────────────────────────────────

    @Test
    void notEq_shouldNegateEqualityCondition() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age", "not.eq.25");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.contains("NOT") || cond.contains("<>"),
                "Expected NOT or <> but got: " + cond);
    }

    @Test
    void notLike_shouldNegatePatternMatch() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("name", "not.like.%admin%");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.toUpperCase().contains("NOT") && cond.toUpperCase().contains("LIKE"),
                "Expected NOT LIKE but got: " + cond);
    }

    @Test
    void notIn_shouldNegateInCondition() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age", "not.in.(18,19,20)");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.toUpperCase().contains("NOT IN"),
                "Expected NOT IN but got: " + cond);
    }

    @Test
    void notIsNull_shouldProduceIsNotNull() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("name", "not.is.null");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0).toUpperCase();
        assertTrue(cond.contains("IS NOT NULL"),
                "Expected IS NOT NULL but got: " + cond);
    }

    @Test
    void notGt_shouldNegateGreaterThan() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age", "not.gt.30");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.contains("NOT") || cond.contains("<="),
                "Expected NOT or <= but got: " + cond);
    }

    // ─── Phase 1A: is.unknown ─────────────────────────────────────────────────

    @Test
    void isNull_shouldProduceIsNull() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("name", "is.null");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).toUpperCase().contains("IS NULL"),
                "Expected IS NULL but got: " + conditions.get(0));
    }

    @Test
    void isUnknown_shouldProduceIsUnknown() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("active", "is.unknown");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).toUpperCase().contains("IS UNKNOWN"),
                "Expected IS UNKNOWN but got: " + conditions.get(0));
    }

    // ─── Phase 1A: nested and/or in or() ─────────────────────────────────────

    @Test
    void or_withNestedAndCondition_shouldParseCorrectly() {
        // or=(age.eq.18,not.and(age.gte.30,age.lte.40))
        // → age = 18 OR NOT (age >= 30 AND age <= 40)
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("or", "(age.eq.18,not.and(age.gte.30,age.lte.40))");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0).toUpperCase();
        assertTrue(cond.contains("OR"), "Expected OR but got: " + cond);
        assertTrue(cond.contains("NOT"), "Expected NOT but got: " + cond);
        assertTrue(cond.contains("AND"), "Expected AND but got: " + cond);
    }

    @Test
    void or_withSimpleConditions_shouldStillWork() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("or", "(age.eq.18,age.eq.21)");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).toUpperCase().contains("OR"),
                "Expected OR but got: " + conditions.get(0));
    }

    // ─── Phase 1C: POSIX regex operators ──────────────────────────────────────

    @Test
    void match_shouldProducePosixCaseSensitiveRegex() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("name", "match.^Admin");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.contains("~"), "Expected ~ operator but got: " + cond);
        assertFalse(cond.contains("~*"), "Should be case-sensitive ~ not ~*");
        assertEquals(1, queryParams.size());
        assertEquals("^Admin", queryParams.get(0));
    }

    @Test
    void imatch_shouldProducePosixCaseInsensitiveRegex() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("name", "imatch.^admin");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.contains("~*"), "Expected ~* operator but got: " + cond);
        assertEquals(1, queryParams.size());
        assertEquals("^admin", queryParams.get(0));
    }

    // ─── Phase 1C: isdistinct operator ────────────────────────────────────────

    @Test
    void isdistinct_shouldProduceIsDistinctFrom() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age", "isdistinct.30");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0).toUpperCase();
        assertTrue(cond.contains("IS DISTINCT FROM"),
                "Expected IS DISTINCT FROM but got: " + cond);
        assertEquals(1, queryParams.size());
    }

    // ─── Phase 1C: phfts (phrase FTS) + fix FTS operators ────────────────────

    @Test
    void phfts_shouldUsePhraseToTsquery() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("description", "phfts.hello world");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.contains("phraseto_tsquery"),
                "Expected phraseto_tsquery but got: " + cond);
    }

    @Test
    void plfts_shouldUsePlainToTsquery() {
        // Excalibase: plfts = plainto_tsquery (NOT phraseto_tsquery)
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("description", "plfts.hello world");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.contains("plainto_tsquery"),
                "Expected plainto_tsquery but got: " + cond);
        assertFalse(cond.contains("phraseto_tsquery"),
                "plfts should NOT use phraseto_tsquery");
    }

    @Test
    void fts_shouldUseTsquery() {
        // Excalibase: fts = to_tsquery (NOT plainto_tsquery)
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("description", "fts.hello");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        // fts should use to_tsquery
        assertTrue(cond.contains("to_tsquery") || cond.contains("tsquery"),
                "Expected to_tsquery but got: " + cond);
    }

    @Test
    void wfts_shouldUseWebsearchToTsquery() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("description", "wfts.hello world");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.contains("websearch_to_tsquery"),
                "Expected websearch_to_tsquery but got: " + cond);
    }

    // ─── Combination: not + isdistinct ────────────────────────────────────────

    @Test
    void notIsdistinct_shouldNegateIsDistinctFrom() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age", "not.isdistinct.30");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0).toUpperCase();
        // NOT (col IS DISTINCT FROM ?) = col IS NOT DISTINCT FROM ?
        assertTrue(cond.contains("NOT") && cond.contains("DISTINCT FROM"),
                "Expected NOT ... DISTINCT FROM but got: " + cond);
    }

    // ─── Phase 5C: Excalibase range operators ─────────────────────────────────

    @Test
    void cs_containsOperator_shouldUseAtGreaterThan() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age_range", "cs.[25,50]");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("@>"), "cs should produce @>: " + conditions.get(0));
    }

    @Test
    void cd_containedInOperator_shouldUseLessThanAt() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age_range", "cd.[20,60]");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("<@"), "cd should produce <@: " + conditions.get(0));
    }

    @Test
    void ov_overlapsOperator_shouldUseAmpersandAmpersand() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age_range", "ov.[30,40]");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("&&"), "ov should produce &&: " + conditions.get(0));
    }

    @Test
    void sl_strictlyLeftOperator_shouldUseDoubleLessThan() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age_range", "sl.[50,100]");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("<<"), "sl should produce <<: " + conditions.get(0));
    }

    @Test
    void sr_strictlyRightOperator_shouldUseDoubleGreaterThan() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age_range", "sr.[1,10]");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains(">>"), "sr should produce >>: " + conditions.get(0));
    }

    @Test
    void nxl_notExtendLeftOperator_shouldUseAmpLessThan() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age_range", "nxl.[30,50]");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("&<"), "nxl should produce &<: " + conditions.get(0));
    }

    @Test
    void nxr_notExtendRightOperator_shouldUseAmpGreaterThan() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age_range", "nxr.[30,50]");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("&>"), "nxr should produce &>: " + conditions.get(0));
    }

    @Test
    void adj_adjacentToOperator_shouldUseDashPipeDash() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("age_range", "adj.[51,100]");
        List<Object> queryParams = new ArrayList<>();

        List<String> conditions = filterService.parseFilters(params, queryParams, tableInfo);

        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("-|-"), "adj should produce -|-: " + conditions.get(0));
    }
}
