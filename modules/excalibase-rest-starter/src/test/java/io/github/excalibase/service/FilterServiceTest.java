package io.github.excalibase.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FilterServiceTest {

    @Mock
    private IValidationService validationService;

    private FilterService filterService;
    private TypeConversionService typeConversionService;
    private TableInfo tableInfo;

    @BeforeEach
    void setUp() {
        typeConversionService = new TypeConversionService(validationService);
        filterService = new FilterService(validationService, typeConversionService);

        // Build a table with several typed columns
        List<ColumnInfo> columns = List.of(
            new ColumnInfo("id", "integer", true, false),
            new ColumnInfo("name", "text", false, true),
            new ColumnInfo("age", "integer", false, true),
            new ColumnInfo("email", "varchar", false, true),
            new ColumnInfo("active", "boolean", false, true),
            new ColumnInfo("score", "numeric", false, true),
            new ColumnInfo("tags", "text[]", false, true),
            new ColumnInfo("meta", "jsonb", false, true),
            new ColumnInfo("ip_addr", "inet", false, true),
            new ColumnInfo("created_at", "timestamp", false, true),
            new ColumnInfo("status", "postgres_enum:status_type", false, true),
            new ColumnInfo("student", "boolean", false, true)
        );
        tableInfo = new TableInfo("users", columns, List.of());

        // Default: validation passes
        doNothing().when(validationService).validateFilterValue(anyString());
        doNothing().when(validationService).validateInOperatorValues(anyString());
    }

    // Helper: single-entry MultiValueMap
    private MultiValueMap<String, String> params(String key, String value) {
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add(key, value);
        return map;
    }

    // ── Basic comparison operators ────────────────────────────────────────────

    @Test
    void parseFilters_eqOperator_generatesEqualsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "eq.25"), p, tableInfo);
        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("="));
        assertTrue(conditions.get(0).contains("\"age\""));
        assertEquals(25, p.get(0));
    }

    @Test
    void parseFilters_neqOperator_generatesNotEqualsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "neq.30"), p, tableInfo);
        assertTrue(conditions.get(0).contains("<>"));
    }

    @Test
    void parseFilters_gtOperator_generatesGreaterThanCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "gt.18"), p, tableInfo);
        assertTrue(conditions.get(0).contains(">"));
    }

    @Test
    void parseFilters_gteOperator_generatesGreaterThanOrEqualCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "gte.18"), p, tableInfo);
        assertTrue(conditions.get(0).contains(">="));
    }

    @Test
    void parseFilters_ltOperator_generatesLessThanCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "lt.65"), p, tableInfo);
        assertTrue(conditions.get(0).contains("<"));
    }

    @Test
    void parseFilters_lteOperator_generatesLessThanOrEqualCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "lte.65"), p, tableInfo);
        assertTrue(conditions.get(0).contains("<="));
    }

    // ── No operator — defaults to equality ────────────────────────────────────

    @Test
    void parseFilters_noOperator_defaultsToEquality() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("name", "Alice"), p, tableInfo);
        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("="));
        assertEquals("Alice", p.get(0));
    }

    // ── String operators ──────────────────────────────────────────────────────

    @Test
    void parseFilters_likeOperator_wrapsValueWithWildcards() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("name", "like.Al"), p, tableInfo);
        assertTrue(conditions.get(0).contains("LIKE"));
        assertEquals("%Al%", p.get(0));
    }

    @Test
    void parseFilters_ilikeOperator_wrapsValueWithWildcards() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("name", "ilike.alice"), p, tableInfo);
        assertTrue(conditions.get(0).contains("ILIKE"));
        assertEquals("%alice%", p.get(0));
    }

    @Test
    void parseFilters_startswithOperator_appendsWildcard() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("email", "startswith.user"), p, tableInfo);
        assertTrue(conditions.get(0).contains("LIKE"));
        assertEquals("user%", p.get(0));
    }

    @Test
    void parseFilters_endswithOperator_prependsWildcard() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("email", "endswith.@example.com"), p, tableInfo);
        assertTrue(conditions.get(0).contains("LIKE"));
        assertTrue(p.get(0).toString().startsWith("%"));
    }

    // ── Regex operators ───────────────────────────────────────────────────────

    @Test
    void parseFilters_matchOperator_generatesTildeCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("name", "match.^A.*"), p, tableInfo);
        assertTrue(conditions.get(0).contains("~"));
        assertEquals("^A.*", p.get(0));
    }

    @Test
    void parseFilters_imatchOperator_generatesCaseInsensitiveTilde() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("name", "imatch.^a.*"), p, tableInfo);
        assertTrue(conditions.get(0).contains("~*"));
        assertEquals("^a.*", p.get(0));
    }

    // ── NULL operators ────────────────────────────────────────────────────────

    @Test
    void parseFilters_isNull_generatesIsNullCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("email", "is.null"), p, tableInfo);
        assertTrue(conditions.get(0).contains("IS NULL"));
        assertTrue(p.isEmpty());
    }

    @Test
    void parseFilters_isTrue_generatesIsTrueCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("active", "is.true"), p, tableInfo);
        assertTrue(conditions.get(0).contains("IS TRUE"));
    }

    @Test
    void parseFilters_isFalse_generatesIsFalseCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("active", "is.false"), p, tableInfo);
        assertTrue(conditions.get(0).contains("IS FALSE"));
    }

    @Test
    void parseFilters_isUnknown_generatesIsUnknownCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("active", "is.unknown"), p, tableInfo);
        assertTrue(conditions.get(0).contains("IS UNKNOWN"));
    }

    @Test
    void parseFilters_isOtherValue_generatesEqualityCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("active", "is.active"), p, tableInfo);
        assertTrue(conditions.get(0).contains("="));
    }

    @Test
    void parseFilters_isnotnull_generatesIsNotNullCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("email", "isnotnull."), p, tableInfo);
        assertTrue(conditions.get(0).contains("IS NOT NULL"));
    }

    @Test
    void parseFilters_isdistinct_generatesIsDistinctFromCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "isdistinct.25"), p, tableInfo);
        assertTrue(conditions.get(0).contains("IS DISTINCT FROM"));
    }

    // ── NOT prefix ────────────────────────────────────────────────────────────

    @Test
    void parseFilters_notIsNull_generatesIsNotNull() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("email", "not.is.null"), p, tableInfo);
        assertTrue(conditions.get(0).contains("IS NOT NULL"));
    }

    @Test
    void parseFilters_notIn_generatesNotInCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "not.in.(18,21,25)"), p, tableInfo);
        assertTrue(conditions.get(0).contains("NOT IN"));
    }

    @Test
    void parseFilters_notEq_generatesNotWrappedCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "not.eq.25"), p, tableInfo);
        // "NOT" followed by eq-based condition
        assertTrue(conditions.get(0).toUpperCase().contains("NOT"));
    }

    @Test
    void parseFilters_notWithNoDot_treatedAsPlainValue() {
        List<Object> p = new ArrayList<>();
        // "not" with no dot — no operator, treated as plain value equality
        // parseCondition uses the default equality path since there's no "."
        List<String> conditions = filterService.parseFilters(params("name", "not"), p, tableInfo);
        assertFalse(conditions.isEmpty());
        // Default: equals condition
        assertTrue(conditions.get(0).contains("="));
        assertEquals("not", p.get(0));
    }

    // ── IN / NOT IN ───────────────────────────────────────────────────────────

    @Test
    void parseFilters_inOperator_generatesInCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "in.(18,21,25)"), p, tableInfo);
        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("IN"));
        assertEquals(3, p.size());
    }

    @Test
    void parseFilters_notinOperator_generatesNotInCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "notin.(18,21)"), p, tableInfo);
        assertTrue(conditions.get(0).contains("NOT IN"));
        assertEquals(2, p.size());
    }

    @Test
    void parseFilters_inOperator_withoutParens_returnsNull() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "in.18,21"), p, tableInfo);
        // Returns null condition since no parens
        assertTrue(conditions.isEmpty());
    }

    // ── JSON operators ────────────────────────────────────────────────────────

    @Test
    void parseFilters_haskeyOperator_generatesJsonbExistsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "haskey.field1"), p, tableInfo);
        assertTrue(conditions.get(0).contains("jsonb_exists"));
        assertEquals("field1", p.get(0));
    }

    @Test
    void parseFilters_jsoncontainsOperator_generatesContainsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "jsoncontains.{\"k\":1}"), p, tableInfo);
        assertTrue(conditions.get(0).contains("@>"));
        assertTrue(conditions.get(0).contains("jsonb"));
    }

    @Test
    void parseFilters_containsAlias_generatesContainsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "contains.{\"k\":1}"), p, tableInfo);
        assertTrue(conditions.get(0).contains("@>"));
    }

    @Test
    void parseFilters_jsoncontains_invalidJson_throwsException() {
        List<Object> p = new ArrayList<>();
        assertThrows(IllegalArgumentException.class, () ->
            filterService.parseFilters(params("meta", "jsoncontains.not-valid-json"), p, tableInfo)
        );
    }

    @Test
    void parseFilters_jsoncontainedOperator_generatesContainedInCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "jsoncontained.{\"k\":1}"), p, tableInfo);
        assertTrue(conditions.get(0).contains("<@"));
    }

    @Test
    void parseFilters_containedinAlias_generatesContainedInCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "containedin.{\"k\":1}"), p, tableInfo);
        assertTrue(conditions.get(0).contains("<@"));
    }

    @Test
    void parseFilters_jsonexistsOperator_generatesKeyExistsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "jsonexists.mykey"), p, tableInfo);
        assertTrue(conditions.get(0).contains("?"));
        assertEquals("mykey", p.get(0));
    }

    @Test
    void parseFilters_existsAlias_generatesKeyExistsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "exists.mykey"), p, tableInfo);
        assertTrue(conditions.get(0).contains("?"));
    }

    @Test
    void parseFilters_jsonpathOperator_generatesJsonPathCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "jsonpath.$.price"), p, tableInfo);
        assertTrue(conditions.get(0).contains("@?"));
        assertTrue(conditions.get(0).contains("jsonpath"));
    }

    @Test
    void parseFilters_jsonpathexistsOperator_generatesJsonPathExistsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "jsonpathexists.$.price"), p, tableInfo);
        assertTrue(conditions.get(0).contains("@@"));
    }

    @Test
    void parseFilters_haskeysOperator_requiresArrayFormat() {
        List<Object> p = new ArrayList<>();
        assertThrows(IllegalArgumentException.class, () ->
            filterService.parseFilters(params("meta", "haskeys.k1,k2"), p, tableInfo)
        );
    }

    @Test
    void parseFilters_haskeysOperator_withArrayFormat_succeeds() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "haskeys.[\"k1\",\"k2\"]"), p, tableInfo);
        assertTrue(conditions.get(0).contains("?&"));
        assertTrue(conditions.get(0).contains("ARRAY"));
    }

    @Test
    void parseFilters_hasanykeysOperator_withArrayFormat_succeeds() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "hasanykeys.[\"k1\"]"), p, tableInfo);
        assertTrue(conditions.get(0).contains("?|"));
    }

    @Test
    void parseFilters_jsonexistsanyAlias_withArrayFormat_succeeds() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "existsany.[\"k1\"]"), p, tableInfo);
        assertTrue(conditions.get(0).contains("?|"));
    }

    @Test
    void parseFilters_jsonexistsallAlias_withArrayFormat_succeeds() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("meta", "existsall.[\"k1\"]"), p, tableInfo);
        assertTrue(conditions.get(0).contains("?&"));
    }

    // ── Array operators ───────────────────────────────────────────────────────

    @Test
    void parseFilters_arraycontainsOperator_generatesArrayContainsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("tags", "arraycontains.go"), p, tableInfo);
        assertTrue(conditions.get(0).contains("@>"));
        assertTrue(conditions.get(0).contains("ARRAY"));
    }

    @Test
    void parseFilters_arrayhasanyOperator_withBracketFormat_generatesOverlapsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("tags", "arrayhasany.[go,java]"), p, tableInfo);
        assertTrue(conditions.get(0).contains("&&"));
    }

    @Test
    void parseFilters_arrayhasanyOperator_withCurlyFormat_generatesOverlapsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("tags", "arrayhasany.{go,java}"), p, tableInfo);
        assertTrue(conditions.get(0).contains("&&"));
    }

    @Test
    void parseFilters_arrayhasanyOperator_withInvalidFormat_returnsNull() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("tags", "arrayhasany.go"), p, tableInfo);
        // No valid array format -> null condition -> empty list
        assertTrue(conditions.isEmpty());
    }

    @Test
    void parseFilters_arrayhasallOperator_withBracketFormat_generatesContainsAllCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("tags", "arrayhasall.[go,java]"), p, tableInfo);
        assertTrue(conditions.get(0).contains("@>"));
    }

    @Test
    void parseFilters_arrayhasallOperator_withNoBrackets_returnsNull() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("tags", "arrayhasall.go"), p, tableInfo);
        assertTrue(conditions.isEmpty());
    }

    @Test
    void parseFilters_arraylengthOperator_generatesArrayLengthCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("tags", "arraylength.3"), p, tableInfo);
        assertTrue(conditions.get(0).contains("array_length"));
        assertEquals(3, p.get(0));
    }

    // ── Full-text search operators ────────────────────────────────────────────

    @Test
    void parseFilters_ftsOperator_generatesTsvectorCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("name", "fts.hello"), p, tableInfo);
        assertTrue(conditions.get(0).contains("to_tsvector"));
        assertTrue(conditions.get(0).contains("to_tsquery"));
    }

    @Test
    void parseFilters_plftsOperator_generatesPlainto_tsqueryCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("name", "plfts.hello world"), p, tableInfo);
        assertTrue(conditions.get(0).contains("plainto_tsquery"));
    }

    @Test
    void parseFilters_phftsOperator_generatesPhraseto_tsqueryCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("name", "phfts.hello world"), p, tableInfo);
        assertTrue(conditions.get(0).contains("phraseto_tsquery"));
    }

    @Test
    void parseFilters_wftsOperator_generatesWebsearchCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("name", "wfts.hello"), p, tableInfo);
        assertTrue(conditions.get(0).contains("websearch_to_tsquery"));
    }

    // ── Range operators ───────────────────────────────────────────────────────

    @Test
    void parseFilters_csOperator_generatesContainsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "cs.25"), p, tableInfo);
        assertTrue(conditions.get(0).contains("@>"));
    }

    @Test
    void parseFilters_cdOperator_generatesContainedInCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "cd.25"), p, tableInfo);
        assertTrue(conditions.get(0).contains("<@"));
    }

    @Test
    void parseFilters_ovOperator_generatesOverlapsCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "ov.25"), p, tableInfo);
        assertTrue(conditions.get(0).contains("&&"));
    }

    @Test
    void parseFilters_slOperator_generatesStrictlyLeftCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "sl.25"), p, tableInfo);
        assertTrue(conditions.get(0).contains("<<"));
    }

    @Test
    void parseFilters_srOperator_generatesStrictlyRightCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "sr.25"), p, tableInfo);
        assertTrue(conditions.get(0).contains(">>"));
    }

    @Test
    void parseFilters_nxlOperator_generatesNoExtendLeftCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "nxl.25"), p, tableInfo);
        assertTrue(conditions.get(0).contains("&<"));
    }

    @Test
    void parseFilters_nxrOperator_generatesNoExtendRightCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "nxr.25"), p, tableInfo);
        assertTrue(conditions.get(0).contains("&>"));
    }

    @Test
    void parseFilters_adjOperator_generatesAdjacentCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "adj.25"), p, tableInfo);
        assertTrue(conditions.get(0).contains("-|-"));
    }

    // ── Default (unknown) operator ────────────────────────────────────────────

    @Test
    void parseFilters_unknownOperator_defaultsToEqualityCondition() {
        List<Object> p = new ArrayList<>();
        List<String> conditions = filterService.parseFilters(params("age", "customop.42"), p, tableInfo);
        assertTrue(conditions.get(0).contains("="));
    }

    // ── OR conditions ─────────────────────────────────────────────────────────

    @Test
    void parseFilters_orCondition_generatesOrJoinedConditions() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("or", "(age.gte.18,student.is.true)");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);

        assertEquals(1, conditions.size());
        String cond = conditions.get(0);
        assertTrue(cond.contains("OR"));
        assertTrue(cond.contains("\"age\""));
        assertTrue(cond.contains("\"student\""));
    }

    @Test
    void parseFilters_orCondition_withoutParens_stillParses() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("or", "age.gte.18,student.is.true");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        assertEquals(1, conditions.size());
        assertTrue(conditions.get(0).contains("OR"));
    }

    @Test
    void parseFilters_orCondition_emptyTokensIgnored() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("or", "(age.gte.18,,)");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        // should still produce 1 condition (the age one)
        assertFalse(conditions.isEmpty());
    }

    @Test
    void parseFilters_orCondition_emptyOrValue_returnsNoCondition() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("or", "()");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        // empty OR group -> null -> not added
        assertTrue(conditions.isEmpty());
    }

    // ── Nested NOT in OR ──────────────────────────────────────────────────────

    @Test
    void parseFilters_orWithNotAnd_generatesNotAndCondition() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("or", "age.eq.18,not.and(age.gte.30,age.lte.40)");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        assertFalse(conditions.isEmpty());
        String cond = conditions.get(0);
        assertTrue(cond.contains("NOT"));
        assertTrue(cond.contains("AND"));
    }

    @Test
    void parseFilters_orWithNotOr_generatesNotOrCondition() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("or", "age.eq.18,not.or(age.gte.60,age.lte.5)");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        assertFalse(conditions.isEmpty());
        String cond = conditions.get(0);
        assertTrue(cond.contains("NOT"));
        assertTrue(cond.contains("OR"));
    }

    @Test
    void parseFilters_orWithAndGroup_generatesAndGroup() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("or", "name.eq.Alice,and(age.gte.18,age.lte.65)");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        assertFalse(conditions.isEmpty());
        String cond = conditions.get(0);
        assertTrue(cond.contains("AND"));
    }

    @Test
    void parseFilters_orWithNestedOr_generatesNestedOrCondition() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("or", "age.eq.18,or(age.eq.21,age.eq.25)");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        assertFalse(conditions.isEmpty());
    }

    // ── Token without dot — no-op ─────────────────────────────────────────────

    @Test
    void parseFilters_orTokenWithoutDot_returnedAsNull_ignored() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        // "justtoken" has no dot so parseLogicToken returns null
        map.add("or", "justtoken,age.eq.25");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        // Only the age.eq.25 condition survives
        assertFalse(conditions.isEmpty());
        assertTrue(conditions.get(0).contains("\"age\""));
    }

    // ── Invalid column throws exception ───────────────────────────────────────

    @Test
    void parseFilters_invalidColumn_throwsIllegalArgumentException() {
        List<Object> p = new ArrayList<>();
        assertThrows(IllegalArgumentException.class, () ->
            filterService.parseFilters(params("nonexistent_column", "eq.5"), p, tableInfo)
        );
    }

    // ── Multiple filter conditions ────────────────────────────────────────────

    @Test
    void parseFilters_multipleFilters_generatesMultipleConditions() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("age", "gte.18");
        map.add("name", "like.Al");
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        assertEquals(2, conditions.size());
    }

    @Test
    void parseFilters_emptyFilters_returnsEmptyList() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        List<String> conditions = filterService.parseFilters(map, p, tableInfo);
        assertTrue(conditions.isEmpty());
    }

    // ── OR condition with not.and whose inner evaluates to null ───────────────

    @Test
    void parseFilters_notAndWithNoValidTokens_returnsNull_ignored() {
        List<Object> p = new ArrayList<>();
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        // Invalid column inside not.and -> IllegalArgumentException
        // This tests the not.and path
        assertThrows(IllegalArgumentException.class, () ->
            filterService.parseFilters(buildOrMap("not.and(bad_col.eq.5)"), p, tableInfo)
        );
    }

    private MultiValueMap<String, String> buildOrMap(String orValue) {
        MultiValueMap<String, String> map = new LinkedMultiValueMap<>();
        map.add("or", orValue);
        return map;
    }
}
