package io.github.excalibase.constant;

import java.util.Set;

/**
 * Centralized operator definitions and complexity weights for query analysis.
 *
 * This class serves as the single source of truth for:
 * 1. All supported PostgREST-style operators
 * 2. Complexity cost calculations for QueryComplexityService
 * 3. Control parameters that should be excluded from filter breadth calculation
 *
 * Complexity weights are based on typical PostgreSQL query costs:
 * - COMPARISON_COST (3): Simple indexed comparisons (=, <, >, etc.)
 * - STRING_PATTERN_COST (10): LIKE/ILIKE requires full scan if no index
 * - ARRAY_MEMBERSHIP_COST (5): IN clause cost per item
 * - ARRAY_OPERATION_COST (8): Array operations moderately expensive
 * - JSON_OPERATION_COST (8): JSONB operations use GIN indexes
 * - FTS_COST (12): Full-text search with tsvector
 * - OR_MULTIPLIER (3): OR prevents some query optimizations
 */
public class OperatorConstants {

    // ========== Operator Categories ==========

    /**
     * Basic comparison operators: eq, neq, gt, gte, lt, lte
     * These typically use indexes and are efficient.
     */
    public static final Set<String> COMPARISON_OPERATORS = Set.of(
        "eq",      // equals (=)
        "neq",     // not equals (<>)
        "gt",      // greater than (>)
        "gte",     // greater than or equal (>=)
        "lt",      // less than (<)
        "lte"      // less than or equal (<=)
    );

    /**
     * String pattern matching operators: like, ilike, startswith, endswith
     * These can be expensive without appropriate indexes (especially with leading wildcards).
     */
    public static final Set<String> STRING_OPERATORS = Set.of(
        "like",        // case-sensitive pattern match
        "ilike",       // case-insensitive pattern match
        "startswith",  // LIKE 'value%'
        "endswith"     // LIKE '%value'
    );

    /**
     * Array membership operators: in, notin
     * Cost scales with number of items in the array.
     */
    public static final Set<String> ARRAY_MEMBERSHIP_OPERATORS = Set.of(
        "in",      // IN (value1, value2, ...)
        "notin"    // NOT IN (value1, value2, ...)
    );

    /**
     * PostgreSQL array operators: arraycontains, arrayhasany, arrayhasall, arraylength
     * These work with PostgreSQL array types and use GIN indexes when available.
     */
    public static final Set<String> ARRAY_OPERATORS = Set.of(
        "arraycontains",  // @> (contains)
        "arrayhasany",    // && (overlaps)
        "arrayhasall",    // @> (contains all)
        "arraylength"     // array_length()
    );

    /**
     * JSONB operators for PostgreSQL
     * These use GIN indexes and are moderately expensive.
     */
    public static final Set<String> JSON_OPERATORS = Set.of(
        "haskey",          // jsonb_exists(column, 'key')
        "haskeys",         // ?& (has all keys)
        "hasanykeys",      // ?| (has any key)
        "jsoncontains",    // @> (contains)
        "contains",        // alias for jsoncontains
        "jsoncontained",   // <@ (contained in)
        "containedin",     // alias for jsoncontained
        "jsonexists",      // ? (key exists)
        "exists",          // alias for jsonexists
        "jsonexistsany",   // ?| (any key exists)
        "existsany",       // alias for jsonexistsany
        "jsonexistsall",   // ?& (all keys exist)
        "existsall",       // alias for jsonexistsall
        "jsonpath",        // @? (jsonpath query)
        "jsonpathexists"   // @@ (jsonpath predicate)
    );

    /**
     * Full-text search operators using PostgreSQL tsvector
     * These are expensive but optimized with GIN/GiST indexes.
     */
    public static final Set<String> FTS_OPERATORS = Set.of(
        "fts",      // plainto_tsquery
        "plfts",    // phraseto_tsquery
        "wfts"      // websearch_to_tsquery
    );

    /**
     * NULL checking operators: is, isnotnull
     * These are efficient and use indexes.
     */
    public static final Set<String> NULL_OPERATORS = Set.of(
        "is",          // IS NULL, IS TRUE, IS FALSE
        "isnotnull"    // IS NOT NULL
    );

    // ========== Complexity Weights ==========

    /**
     * Cost for simple indexed comparisons (equals, less than, greater than, etc.)
     * Rationale: These operations are fast with B-tree indexes.
     */
    public static final int COMPARISON_COST = 3;

    /**
     * Cost for LIKE/ILIKE pattern matching
     * Rationale: Full table scan if no trigram index; expensive without proper indexing.
     */
    public static final int STRING_PATTERN_COST = 10;

    /**
     * Cost per item in IN/NOT IN clause
     * Rationale: Each item requires a comparison; scales with array size.
     */
    public static final int ARRAY_MEMBERSHIP_COST = 5;

    /**
     * Cost for array operations (contains, overlaps, etc.)
     * Rationale: GIN indexes help but these are still moderately expensive.
     */
    public static final int ARRAY_OPERATION_COST = 8;

    /**
     * Cost for JSONB operations
     * Rationale: GIN indexes optimize these but traversing JSON still has overhead.
     */
    public static final int JSON_OPERATION_COST = 8;

    /**
     * Cost for full-text search operations
     * Rationale: tsvector operations are expensive even with GIN indexes.
     */
    public static final int FTS_COST = 12;

    /**
     * Cost for NULL checking operations
     * Rationale: Very efficient, index-supported.
     */
    public static final int NULL_CHECK_COST = 2;

    /**
     * Multiplier for OR conditions
     * Rationale: OR prevents query optimizer from using indexes effectively,
     * often requiring multiple index scans or full table scans.
     */
    public static final int OR_MULTIPLIER = 3;

    /**
     * Base cost for relationship expansion (JOIN operation)
     * Rationale: Each JOIN adds significant cost, especially for large tables.
     */
    public static final int EXPANSION_BASE_COST = 20;

    // ========== Control Parameters ==========

    /**
     * Special query parameters that control request behavior rather than filter data.
     * These should be excluded from breadth calculation in QueryComplexityService.
     *
     * Includes:
     * - Pagination: offset, limit, first, after, last, before
     * - Sorting: orderBy, orderDirection, order
     * - Field selection: select, fields
     * - Relationship expansion: expand, join, include
     * - Grouping/aggregation: groupBy, having, distinct, aggregate
     * - Advanced: batch, query, variables, fragment, alias, transform, validate, explain, format, stream
     */
    public static final Set<String> CONTROL_PARAMETERS = Set.of(
        // Pagination
        "offset", "limit", "first", "after", "last", "before",
        // Sorting
        "orderBy", "orderDirection", "order",
        // Field selection
        "select", "fields",
        // Relationship expansion
        "expand", "join", "include",
        // Grouping and aggregation
        "groupBy", "having", "distinct", "aggregate",
        // Advanced features
        "batch", "query", "variables", "fragment", "alias",
        "transform", "validate", "explain", "format", "stream"
    );

    // ========== Utility Methods ==========

    /**
     * Get the complexity cost for a given operator.
     *
     * @param operator The operator name (lowercase)
     * @return The complexity cost for this operator
     */
    public static int getOperatorCost(String operator) {
        String op = operator.toLowerCase();

        if (COMPARISON_OPERATORS.contains(op)) {
            return COMPARISON_COST;
        } else if (STRING_OPERATORS.contains(op)) {
            return STRING_PATTERN_COST;
        } else if (ARRAY_MEMBERSHIP_OPERATORS.contains(op)) {
            return ARRAY_MEMBERSHIP_COST; // Base cost, may be multiplied by item count
        } else if (ARRAY_OPERATORS.contains(op)) {
            return ARRAY_OPERATION_COST;
        } else if (JSON_OPERATORS.contains(op)) {
            return JSON_OPERATION_COST;
        } else if (FTS_OPERATORS.contains(op)) {
            return FTS_COST;
        } else if (NULL_OPERATORS.contains(op)) {
            return NULL_CHECK_COST;
        } else {
            // Unknown operator, default to comparison cost
            return COMPARISON_COST;
        }
    }

    /**
     * Check if a parameter is a control parameter (not a filter).
     *
     * @param paramName The parameter name
     * @return true if this is a control parameter, false otherwise
     */
    public static boolean isControlParameter(String paramName) {
        return CONTROL_PARAMETERS.contains(paramName);
    }

    /**
     * Get all supported operators across all categories.
     *
     * @return Set of all operator names
     */
    public static Set<String> getAllOperators() {
        Set<String> all = new java.util.HashSet<>();
        all.addAll(COMPARISON_OPERATORS);
        all.addAll(STRING_OPERATORS);
        all.addAll(ARRAY_MEMBERSHIP_OPERATORS);
        all.addAll(ARRAY_OPERATORS);
        all.addAll(JSON_OPERATORS);
        all.addAll(FTS_OPERATORS);
        all.addAll(NULL_OPERATORS);
        return all;
    }
}
