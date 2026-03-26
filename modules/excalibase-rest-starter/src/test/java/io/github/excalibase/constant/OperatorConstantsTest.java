package io.github.excalibase.constant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class OperatorConstantsTest {

    // ── getOperatorCost ───────────────────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = {"eq", "neq", "gt", "gte", "lt", "lte"})
    void getOperatorCost_returnsComparisonCostForComparisonOperators(String op) {
        assertEquals(OperatorConstants.COMPARISON_COST, OperatorConstants.getOperatorCost(op));
    }

    @ParameterizedTest
    @ValueSource(strings = {"like", "ilike", "startswith", "endswith"})
    void getOperatorCost_returnsStringPatternCostForStringOperators(String op) {
        assertEquals(OperatorConstants.STRING_PATTERN_COST, OperatorConstants.getOperatorCost(op));
    }

    @ParameterizedTest
    @ValueSource(strings = {"match", "imatch"})
    void getOperatorCost_returnsStringPatternCostForRegexOperators(String op) {
        assertEquals(OperatorConstants.STRING_PATTERN_COST, OperatorConstants.getOperatorCost(op));
    }

    @ParameterizedTest
    @ValueSource(strings = {"in", "notin"})
    void getOperatorCost_returnsArrayMembershipCostForMembershipOperators(String op) {
        assertEquals(OperatorConstants.ARRAY_MEMBERSHIP_COST, OperatorConstants.getOperatorCost(op));
    }

    @ParameterizedTest
    @ValueSource(strings = {"arraycontains", "arrayhasany", "arrayhasall", "arraylength"})
    void getOperatorCost_returnsArrayOperationCostForArrayOperators(String op) {
        assertEquals(OperatorConstants.ARRAY_OPERATION_COST, OperatorConstants.getOperatorCost(op));
    }

    @ParameterizedTest
    @ValueSource(strings = {"haskey", "haskeys", "hasanykeys", "jsoncontains", "contains",
            "jsoncontained", "containedin", "jsonexists", "exists",
            "jsonexistsany", "existsany", "jsonexistsall", "existsall",
            "jsonpath", "jsonpathexists"})
    void getOperatorCost_returnsJsonOperationCostForJsonOperators(String op) {
        assertEquals(OperatorConstants.JSON_OPERATION_COST, OperatorConstants.getOperatorCost(op));
    }

    @ParameterizedTest
    @ValueSource(strings = {"fts", "plfts", "phfts", "wfts"})
    void getOperatorCost_returnsFtsCostForFtsOperators(String op) {
        assertEquals(OperatorConstants.FTS_COST, OperatorConstants.getOperatorCost(op));
    }

    @ParameterizedTest
    @ValueSource(strings = {"is", "isnotnull", "isdistinct"})
    void getOperatorCost_returnsNullCheckCostForNullOperators(String op) {
        assertEquals(OperatorConstants.NULL_CHECK_COST, OperatorConstants.getOperatorCost(op));
    }

    @Test
    void getOperatorCost_returnsComparisonCostForUnknownOperator() {
        assertEquals(OperatorConstants.COMPARISON_COST, OperatorConstants.getOperatorCost("unknownop"));
    }

    @Test
    void getOperatorCost_isCaseInsensitive() {
        assertEquals(OperatorConstants.COMPARISON_COST, OperatorConstants.getOperatorCost("EQ"));
        assertEquals(OperatorConstants.COMPARISON_COST, OperatorConstants.getOperatorCost("Eq"));
        assertEquals(OperatorConstants.STRING_PATTERN_COST, OperatorConstants.getOperatorCost("LIKE"));
    }

    // ── Constant values ───────────────────────────────────────────────────────

    @Test
    void constantValues_haveExpectedValues() {
        assertEquals(3, OperatorConstants.COMPARISON_COST);
        assertEquals(10, OperatorConstants.STRING_PATTERN_COST);
        assertEquals(5, OperatorConstants.ARRAY_MEMBERSHIP_COST);
        assertEquals(8, OperatorConstants.ARRAY_OPERATION_COST);
        assertEquals(8, OperatorConstants.JSON_OPERATION_COST);
        assertEquals(12, OperatorConstants.FTS_COST);
        assertEquals(2, OperatorConstants.NULL_CHECK_COST);
        assertEquals(3, OperatorConstants.OR_MULTIPLIER);
        assertEquals(20, OperatorConstants.EXPANSION_BASE_COST);
    }

    // ── isControlParameter ───────────────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = {"offset", "limit", "first", "after", "last", "before",
            "orderBy", "orderDirection", "order",
            "select", "fields",
            "expand", "join", "include",
            "groupBy", "having", "distinct", "aggregate",
            "batch", "query", "variables", "fragment", "alias",
            "transform", "validate", "explain", "format", "stream"})
    void isControlParameter_returnsTrueForAllControlParams(String param) {
        assertTrue(OperatorConstants.isControlParameter(param));
    }

    @ParameterizedTest
    @ValueSource(strings = {"age", "name", "status", "created_at", "user_id"})
    void isControlParameter_returnsFalseForRegularColumnNames(String param) {
        assertFalse(OperatorConstants.isControlParameter(param));
    }

    @Test
    void isControlParameter_isCaseSensitive() {
        // "offset" is a control param, "OFFSET" is not
        assertTrue(OperatorConstants.isControlParameter("offset"));
        assertFalse(OperatorConstants.isControlParameter("OFFSET"));
    }

    // ── getAllOperators ───────────────────────────────────────────────────────

    @Test
    void getAllOperators_containsAllCategories() {
        Set<String> all = OperatorConstants.getAllOperators();

        // From COMPARISON_OPERATORS
        assertTrue(all.contains("eq"));
        assertTrue(all.contains("neq"));
        assertTrue(all.contains("gt"));
        assertTrue(all.contains("gte"));
        assertTrue(all.contains("lt"));
        assertTrue(all.contains("lte"));

        // From STRING_OPERATORS
        assertTrue(all.contains("like"));
        assertTrue(all.contains("ilike"));
        assertTrue(all.contains("startswith"));
        assertTrue(all.contains("endswith"));

        // From REGEX_OPERATORS
        assertTrue(all.contains("match"));
        assertTrue(all.contains("imatch"));

        // From ARRAY_MEMBERSHIP_OPERATORS
        assertTrue(all.contains("in"));
        assertTrue(all.contains("notin"));

        // From ARRAY_OPERATORS
        assertTrue(all.contains("arraycontains"));
        assertTrue(all.contains("arrayhasany"));
        assertTrue(all.contains("arrayhasall"));
        assertTrue(all.contains("arraylength"));

        // From JSON_OPERATORS
        assertTrue(all.contains("haskey"));
        assertTrue(all.contains("jsoncontains"));
        assertTrue(all.contains("jsonpath"));

        // From FTS_OPERATORS
        assertTrue(all.contains("fts"));
        assertTrue(all.contains("plfts"));

        // From NULL_OPERATORS
        assertTrue(all.contains("is"));
        assertTrue(all.contains("isnotnull"));
    }

    @Test
    void getAllOperators_hasCorrectTotalCount() {
        Set<String> all = OperatorConstants.getAllOperators();
        int expected = OperatorConstants.COMPARISON_OPERATORS.size()
                + OperatorConstants.STRING_OPERATORS.size()
                + OperatorConstants.REGEX_OPERATORS.size()
                + OperatorConstants.ARRAY_MEMBERSHIP_OPERATORS.size()
                + OperatorConstants.ARRAY_OPERATORS.size()
                + OperatorConstants.JSON_OPERATORS.size()
                + OperatorConstants.FTS_OPERATORS.size()
                + OperatorConstants.NULL_OPERATORS.size();
        assertEquals(expected, all.size());
    }

    @Test
    void getAllOperators_returnsModifiableCopy() {
        Set<String> all = OperatorConstants.getAllOperators();
        // Adding to the returned set should not affect subsequent calls
        all.add("fake_operator");
        Set<String> second = OperatorConstants.getAllOperators();
        assertFalse(second.contains("fake_operator"));
    }

    // ── operator set immutability ─────────────────────────────────────────────

    @Test
    void comparisonOperators_setIsImmutable() {
        assertThrows(UnsupportedOperationException.class,
                () -> OperatorConstants.COMPARISON_OPERATORS.add("newop"));
    }

    @Test
    void stringOperators_setIsImmutable() {
        assertThrows(UnsupportedOperationException.class,
                () -> OperatorConstants.STRING_OPERATORS.add("newop"));
    }
}
