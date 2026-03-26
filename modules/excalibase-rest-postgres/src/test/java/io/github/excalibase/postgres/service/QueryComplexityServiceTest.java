package io.github.excalibase.postgres.service;

import org.junit.jupiter.api.Test;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryComplexityServiceTest {

    // ===== analyzeQuery: base complexity =====

    @Test
    void analyzeQuery_baseScore_includesTableAccessCost() {
        QueryComplexityService service = buildService(1000, 10, 50, true);
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        QueryComplexityService.QueryAnalysis analysis = service.analyzeQuery("users", params, 10, null);

        // Base=10, limit=10 -> 20 at minimum
        assertTrue(analysis.complexityScore >= 20);
        assertEquals(1, analysis.depth);
    }

    @Test
    void analyzeQuery_limitAddsToScore() {
        QueryComplexityService service = buildService(1000, 10, 50, true);
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        QueryComplexityService.QueryAnalysis noLimit = service.analyzeQuery("t", params, 0, null);
        QueryComplexityService.QueryAnalysis withLimit = service.analyzeQuery("t", params, 50, null);

        assertTrue(withLimit.complexityScore > noLimit.complexityScore);
    }

    @Test
    void analyzeQuery_limitCappedAt100() {
        QueryComplexityService service = buildService(10000, 10, 50, true);
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        QueryComplexityService.QueryAnalysis big = service.analyzeQuery("t", params, 500, null);
        QueryComplexityService.QueryAnalysis max = service.analyzeQuery("t", params, 100, null);

        // Both should use the cap, so big (500) should equal max (100) in limit contribution
        assertEquals(max.complexityScore, big.complexityScore);
    }

    @Test
    void analyzeQuery_nullParams_handledGracefully() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        QueryComplexityService.QueryAnalysis analysis = service.analyzeQuery("users", null, 10, null);

        assertNotNull(analysis);
        assertTrue(analysis.complexityScore > 0);
    }

    // ===== filters complexity =====

    @Test
    void analyzeQuery_eachFilterAddsBaseCost() {
        QueryComplexityService service = buildService(1000, 10, 50, true);
        MultiValueMap<String, String> noFilter = new LinkedMultiValueMap<>();
        MultiValueMap<String, String> withFilter = new LinkedMultiValueMap<>();
        withFilter.add("name", "eq.Alice");

        QueryComplexityService.QueryAnalysis without = service.analyzeQuery("t", noFilter, 0, null);
        QueryComplexityService.QueryAnalysis with = service.analyzeQuery("t", withFilter, 0, null);

        assertTrue(with.complexityScore > without.complexityScore);
    }

    @Test
    void analyzeQuery_orConditionsHigherCostThanSimpleFilters() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        MultiValueMap<String, String> simpleFilter = new LinkedMultiValueMap<>();
        simpleFilter.add("name", "eq.Alice");

        MultiValueMap<String, String> orFilter = new LinkedMultiValueMap<>();
        orFilter.add("or", "(name.eq.Alice,age.gt.18)");

        QueryComplexityService.QueryAnalysis simple = service.analyzeQuery("t", simpleFilter, 0, null);
        QueryComplexityService.QueryAnalysis or = service.analyzeQuery("t", orFilter, 0, null);

        assertTrue(or.complexityScore > simple.complexityScore, "OR conditions should cost more than simple filters");
    }

    @Test
    void analyzeQuery_likeOperatorAddsExtraCost() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        MultiValueMap<String, String> noLike = new LinkedMultiValueMap<>();
        noLike.add("name", "eq.Alice");

        MultiValueMap<String, String> withLike = new LinkedMultiValueMap<>();
        withLike.add("name", "like.%Alice%");

        QueryComplexityService.QueryAnalysis eq = service.analyzeQuery("t", noLike, 0, null);
        QueryComplexityService.QueryAnalysis like = service.analyzeQuery("t", withLike, 0, null);

        assertTrue(like.complexityScore > eq.complexityScore, "LIKE should cost more than eq");
    }

    @Test
    void analyzeQuery_ilikeOperatorAddsExtraCost() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        MultiValueMap<String, String> withIlike = new LinkedMultiValueMap<>();
        withIlike.add("name", "ilike.%alice%");

        QueryComplexityService.QueryAnalysis analysis = service.analyzeQuery("t", withIlike, 0, null);

        assertTrue(analysis.complexityScore > 10, "ILIKE adds extra cost");
    }

    @Test
    void analyzeQuery_inOperatorCountsItems() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        MultiValueMap<String, String> inFilter = new LinkedMultiValueMap<>();
        inFilter.add("id", "in.(1,2,3,4,5)");

        MultiValueMap<String, String> singleFilter = new LinkedMultiValueMap<>();
        singleFilter.add("id", "eq.1");

        QueryComplexityService.QueryAnalysis inAnalysis = service.analyzeQuery("t", inFilter, 0, null);
        QueryComplexityService.QueryAnalysis singleAnalysis = service.analyzeQuery("t", singleFilter, 0, null);

        assertTrue(inAnalysis.complexityScore > singleAnalysis.complexityScore, "IN clause with more items should cost more");
    }

    @Test
    void analyzeQuery_jsonOperators_addExtraCost() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        MultiValueMap<String, String> jsonFilter = new LinkedMultiValueMap<>();
        jsonFilter.add("data", "haskey.admin");

        MultiValueMap<String, String> eqFilter = new LinkedMultiValueMap<>();
        eqFilter.add("name", "eq.Alice");

        QueryComplexityService.QueryAnalysis json = service.analyzeQuery("t", jsonFilter, 0, null);
        QueryComplexityService.QueryAnalysis eq = service.analyzeQuery("t", eqFilter, 0, null);

        assertTrue(json.complexityScore > eq.complexityScore, "JSON operators should cost more");
    }

    @Test
    void analyzeQuery_jsonContainsOperator_addsCost() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("meta", "jsoncontains.{\"key\":\"value\"}");

        QueryComplexityService.QueryAnalysis analysis = service.analyzeQuery("t", params, 0, null);

        assertTrue(analysis.complexityScore > 10);
    }

    // ===== expansion complexity =====

    @Test
    void analyzeQuery_expansion_addsDepthAndCost() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        QueryComplexityService.QueryAnalysis without = service.analyzeQuery("orders", null, 0, null);
        QueryComplexityService.QueryAnalysis with = service.analyzeQuery("orders", null, 0, "customers");

        assertTrue(with.complexityScore > without.complexityScore);
        assertTrue(with.depth > without.depth);
    }

    @Test
    void analyzeQuery_multipleExpansions_addsBreadth() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        QueryComplexityService.QueryAnalysis one = service.analyzeQuery("orders", null, 0, "customers");
        QueryComplexityService.QueryAnalysis multi = service.analyzeQuery("orders", null, 0, "customers,products,items");

        assertTrue(multi.complexityScore > one.complexityScore);
    }

    @Test
    void analyzeQuery_emptyExpansion_treatedAsNoExpansion() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        QueryComplexityService.QueryAnalysis withEmpty = service.analyzeQuery("orders", null, 0, "   ");
        QueryComplexityService.QueryAnalysis withNull = service.analyzeQuery("orders", null, 0, null);

        assertEquals(withNull.complexityScore, withEmpty.complexityScore);
    }

    @Test
    void analyzeQuery_expansionWithLimitParam_addsExtraCost() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        QueryComplexityService.QueryAnalysis noLimit = service.analyzeQuery("orders", null, 0, "customers");
        QueryComplexityService.QueryAnalysis withLimit = service.analyzeQuery("orders", null, 0, "customers(limit:50)");

        assertTrue(withLimit.complexityScore > noLimit.complexityScore);
    }

    // ===== validateQueryComplexity: exception tests =====

    @Test
    void validateQueryComplexity_underLimit_doesNotThrow() {
        QueryComplexityService service = buildService(10000, 10, 50, true);
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        assertDoesNotThrow(() ->
                service.validateQueryComplexity("users", params, 10, null));
    }

    @Test
    void validateQueryComplexity_complexityOverLimit_throwsIllegalArgumentException() {
        // maxComplexityScore=5: even base cost of 10+limit will exceed
        QueryComplexityService service = buildService(5, 10, 50, true);
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> service.validateQueryComplexity("t", params, 10, null));

        assertTrue(ex.getMessage().contains("complexity score"));
    }

    @Test
    void validateQueryComplexity_depthOverLimit_throwsIllegalArgumentException() {
        // maxDepth=0 means base depth of 1 already exceeds
        QueryComplexityService service = buildService(10000, 0, 50, true);
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> service.validateQueryComplexity("t", params, 1, null));

        assertTrue(ex.getMessage().contains("depth"));
    }

    @Test
    void validateQueryComplexity_breadthOverLimit_throwsIllegalArgumentException() {
        // maxBreadth=0 means base breadth of 1 already exceeds
        QueryComplexityService service = buildService(10000, 10, 0, true);
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> service.validateQueryComplexity("t", params, 1, null));

        assertTrue(ex.getMessage().contains("breadth"));
    }

    @Test
    void validateQueryComplexity_analysisDisabled_neverThrows() {
        // Even with very low limits, disabled analysis allows everything
        QueryComplexityService service = buildService(1, 0, 0, false);
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        // Add tons of filters
        for (int i = 0; i < 20; i++) {
            params.add("col" + i, "eq.value" + i);
        }

        assertDoesNotThrow(() ->
                service.validateQueryComplexity("t", params, 1000, "expand1,expand2,expand3"));
    }

    // ===== getComplexityLimits =====

    @Test
    void getComplexityLimits_returnsConfiguredValues() {
        QueryComplexityService service = buildService(999, 7, 42, true);

        Map<String, Object> limits = service.getComplexityLimits();

        assertEquals(999, limits.get("maxComplexityScore"));
        assertEquals(7, limits.get("maxDepth"));
        assertEquals(42, limits.get("maxBreadth"));
        assertEquals(true, limits.get("analysisEnabled"));
    }

    // ===== QueryAnalysis toString =====

    @Test
    void queryAnalysisToString_includesAllFields() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        QueryComplexityService.QueryAnalysis analysis = service.analyzeQuery("t", null, 0, null);
        String str = analysis.toString();

        assertTrue(str.contains("score="));
        assertTrue(str.contains("depth="));
        assertTrue(str.contains("breadth="));
    }

    // ===== breadth from filter params =====

    @Test
    void analyzeQuery_multipleDifferentParams_increasesBreadth() {
        QueryComplexityService service = buildService(1000, 10, 50, true);
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("name", "eq.Alice");
        params.add("age", "gt.18");
        params.add("active", "is.true");

        QueryComplexityService.QueryAnalysis analysis = service.analyzeQuery("t", params, 0, null);

        // breadth = 1 (base) + 3 (params)
        assertEquals(4, analysis.breadth);
    }

    @Test
    void analyzeQuery_notinOperator_countedLikeIn() {
        QueryComplexityService service = buildService(1000, 10, 50, true);

        MultiValueMap<String, String> notIn = new LinkedMultiValueMap<>();
        notIn.add("status", "notin.(active,inactive,pending)");

        QueryComplexityService.QueryAnalysis analysis = service.analyzeQuery("t", notIn, 0, null);

        assertTrue(analysis.complexityScore > 15);
    }

    // ===== private helper =====

    private QueryComplexityService buildService(int maxScore, int maxDepth, int maxBreadth, boolean enabled) {
        return new QueryComplexityService(maxScore, maxDepth, maxBreadth, enabled);
    }
}
