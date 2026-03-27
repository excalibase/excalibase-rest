package io.github.excalibase.postgres.compiler;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.postgres.service.DatabaseSchemaService;
import io.github.excalibase.postgres.service.SelectParserService;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.TypeConversionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("PostgresQueryCompiler")
class PostgresQueryCompilerTest {

    @Mock(lenient = true)
    private FilterService filterService;

    @Mock(lenient = true)
    private DatabaseSchemaService databaseSchemaService;

    @Mock(lenient = true)
    private TypeConversionService typeConversionService;

    @Mock(lenient = true)
    private SelectParserService selectParserService;

    private PostgresQueryCompiler compiler;

    private TableInfo customersTable;
    private TableInfo ordersTable;

    @BeforeEach
    void setUp() {
        // Wire real SelectParserService through the mock so parsing works correctly
        SelectParserService realParser = new SelectParserService();
        when(selectParserService.parseSelect(anyString()))
                .thenAnswer(inv -> realParser.parseSelect(inv.getArgument(0)));
        when(selectParserService.getEmbeddedFields(any()))
                .thenAnswer(inv -> realParser.getEmbeddedFields(inv.getArgument(0)));

        compiler = new PostgresQueryCompiler(
                filterService, databaseSchemaService, typeConversionService, selectParserService, "public");

        // customers has no outbound FK — it is referenced by orders
        customersTable = new TableInfo(
                "customers",
                List.of(
                        new ColumnInfo("customer_id", "integer", true, false),
                        new ColumnInfo("name", "varchar", false, false),
                        new ColumnInfo("email", "varchar", false, true),
                        new ColumnInfo("tier", "varchar", false, true)
                ),
                List.of()
        );

        // orders has FK customer_id → customers.customer_id
        ordersTable = new TableInfo(
                "orders",
                List.of(
                        new ColumnInfo("order_id", "integer", true, false),
                        new ColumnInfo("customer_id", "integer", false, false),
                        new ColumnInfo("order_number", "varchar", false, false),
                        new ColumnInfo("total", "decimal", false, true)
                ),
                List.of(new ForeignKeyInfo("customer_id", "customers", "customer_id"))
        );
    }

    // ─── compile() — basic list queries ──────────────────────────────────────

    @Test
    @DisplayName("compile_simpleList_wrapsInJsonbAgg")
    void compile_simpleList_wrapsInJsonbAgg() {
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, null, null, "asc", 0, 100, false);

        // New: outer SELECT wraps result in jsonb_agg(row_to_json(...)) for SELECT *
        assertThat(q.sql()).contains("jsonb_agg");
        assertThat(q.sql()).contains("row_to_json");
        assertThat(q.sql()).contains("AS body");
        // Inner subquery contains the actual data query
        assertThat(q.sql()).contains("FROM \"customers\"");
        assertThat(q.sql()).contains("LIMIT 100");
        assertThat(q.sql()).contains("OFFSET 0");
        // No count column when includeCount=false
        assertThat(q.sql()).doesNotContain("total_count");
        // hasCountWindow is false — count is now a scalar subquery, not a window
        assertThat(q.hasCountWindow()).isFalse();
        // jsonColumns is empty — the whole body is JSON, no per-column parsing needed
        assertThat(q.jsonColumns()).isEmpty();
    }

    @Test
    @DisplayName("compile_selectStar_usesRowToJson")
    void compile_selectStar_usesRowToJson() {
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, null, null, "asc", 0, 100, false);

        // SELECT * → jsonb_agg(row_to_json(c))
        assertThat(q.sql()).contains("jsonb_agg(row_to_json(");
        // COALESCE to handle empty result set
        assertThat(q.sql()).contains("coalesce(");
        assertThat(q.sql()).contains("'[]'::jsonb");
    }

    @Test
    @DisplayName("compile_withCount_addsScalarSubquery")
    void compile_withCount_addsScalarSubquery() {
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, null, null, "asc", 0, 10, true);

        // New: scalar subquery instead of window function
        assertThat(q.sql()).contains("(SELECT count(*)");
        assertThat(q.sql()).contains("AS total_count");
        // No old-style window function
        assertThat(q.sql()).doesNotContain("COUNT(*) OVER()");
        // hasCountWindow stays false — controller reads total_count column directly
        assertThat(q.hasCountWindow()).isFalse();
    }

    @Test
    @DisplayName("compile_withCountAndFilter_scalarSubqueryIncludesWhereClause")
    void compile_withCountAndFilter_scalarSubqueryIncludesWhereClause() {
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("tier", "eq.gold");

        when(filterService.parseFilters(any(), any(), any()))
                .thenAnswer(inv -> {
                    // Simulate FilterService adding one param value
                    List<Object> params = inv.getArgument(1);
                    params.add("gold");
                    return List.of("\"tier\" = ?");
                });

        CompiledQuery q = compiler.compile("customers", customersTable, filters,
                null, null, null, "asc", 0, 10, true);

        // The scalar count subquery should include the same WHERE
        assertThat(q.sql()).contains("(SELECT count(*)");
        assertThat(q.sql()).contains("AS total_count");
        // The WHERE clause must appear in both the inner data query and the count subquery
        assertThat(q.sql()).contains("WHERE");
        assertThat(q.sql()).contains("\"tier\" = ?");
        // Params must be duplicated: once for count subquery, once for inner data subquery
        // Both ? placeholders need a bound value
        assertThat(q.params()).hasSize(2);
        assertThat(q.params()[0]).isEqualTo("gold");
        assertThat(q.params()[1]).isEqualTo("gold");
    }

    @Test
    @DisplayName("compile_withFilter_delegatesToFilterService")
    void compile_withFilter_delegatesToFilterService() {
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("tier", "eq.gold");

        when(filterService.parseFilters(any(), any(), any()))
                .thenReturn(List.of("\"tier\" = ?"));

        CompiledQuery q = compiler.compile("customers", customersTable, filters,
                null, null, null, "asc", 0, 100, false);

        assertThat(q.sql()).contains("WHERE");
        assertThat(q.sql()).contains("\"tier\" = ?");
    }

    @Test
    @DisplayName("compile_withOrderBy_addsOrderClause")
    void compile_withOrderBy_addsOrderClause() {
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, null, "name", "asc", 0, 100, false);

        assertThat(q.sql()).containsIgnoringCase("ORDER BY \"name\" ASC");
    }

    @Test
    @DisplayName("compile_withOrderByDesc_addsDescClause")
    void compile_withOrderByDesc_addsDescClause() {
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, null, "name", "desc", 0, 100, false);

        assertThat(q.sql()).containsIgnoringCase("ORDER BY \"name\" DESC");
    }

    @Test
    @DisplayName("compile_withOrderParam_parsesPostgrestStyle")
    void compile_withOrderParam_parsesPostgrestStyle() {
        // PostgREST-style: order=price.desc passed as filter param
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("order", "total.desc");

        CompiledQuery q = compiler.compile("orders", ordersTable, params,
                null, null, null, "asc", 0, 100, false);

        assertThat(q.sql()).containsIgnoringCase("ORDER BY \"total\" DESC");
    }

    @Test
    @DisplayName("compile_withSelect_usesJsonbBuildObject")
    void compile_withSelect_usesJsonbBuildObject() {
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                "name,email", null, null, "asc", 0, 100, false);

        // Explicit columns → jsonb_build_object with named keys
        assertThat(q.sql()).contains("jsonb_build_object(");
        assertThat(q.sql()).contains("'name'");
        assertThat(q.sql()).contains("'email'");
        // Should NOT use row_to_json when specific columns given
        assertThat(q.sql()).doesNotContain("row_to_json");
        // The outer aggregation uses jsonb_build_object, not a wildcard star projection
        assertThat(q.sql()).doesNotContain("jsonb_agg(row_to_json(");
    }

    @Test
    @DisplayName("compile_withSelectAlias_usesAliasInJsonbBuildObject")
    void compile_withSelectAlias_usesAliasInJsonbBuildObject() {
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                "pname:name", null, null, "asc", 0, 100, false);

        // The alias 'pname' becomes the JSON key in jsonb_build_object
        assertThat(q.sql()).contains("'pname'");
    }

    @Test
    @DisplayName("compile_forwardRelationship_embedsSubqueryInJsonbBuildObject")
    void compile_forwardRelationship_embedsSubqueryInJsonbBuildObject() {
        // orders has FK → customers  →  forward relationship
        CompiledQuery q = compiler.compile("orders", ordersTable, null,
                null, "customers", null, "asc", 0, 100, false);

        // Relationship embedded inside jsonb_build_object as a subquery value
        assertThat(q.sql()).contains("row_to_json");
        assertThat(q.sql()).contains("'customers'");
        // The whole outer query is still wrapped in jsonb_agg ... AS body
        assertThat(q.sql()).contains("AS body");
        // jsonColumns is empty — relationships are now inside the JSON body
        assertThat(q.jsonColumns()).isEmpty();
    }

    @Test
    @DisplayName("compile_reverseRelationship_embedsJsonAggSubqueryInBody")
    void compile_reverseRelationship_embedsJsonAggSubqueryInBody() {
        // customers ← orders  →  reverse (one-to-many) relationship
        when(databaseSchemaService.getTableSchema())
                .thenReturn(Map.of("orders", ordersTable));

        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, "orders", null, "asc", 0, 100, false);

        assertThat(q.sql()).contains("json_agg");
        assertThat(q.sql()).containsIgnoringCase("'orders'");
        // The whole outer query is still wrapped in jsonb_agg ... AS body
        assertThat(q.sql()).contains("AS body");
        // jsonColumns is empty — everything inside the body JSON
        assertThat(q.jsonColumns()).isEmpty();
        // Must be wrapped in COALESCE to handle empty arrays
        assertThat(q.sql()).contains("COALESCE");
    }

    @Test
    @DisplayName("compile_embeddedSelectWithSubfields_filtersSubqueryColumns")
    void compile_embeddedSelectWithSubfields_filtersSubqueryColumns() {
        // select=id,customers(name,email)  — only fetch name+email from customers subquery
        when(databaseSchemaService.getTableSchema())
                .thenReturn(Map.of("orders", ordersTable));

        // orders → customers (forward), so expand inside select
        CompiledQuery q = compiler.compile("orders", ordersTable, null,
                "order_id,customers(name,email)", null, null, "asc", 0, 100, false);

        // Subquery should project name and email
        assertThat(q.sql()).contains("\"name\"");
        assertThat(q.sql()).contains("\"email\"");
        // jsonColumns empty — all inside body
        assertThat(q.jsonColumns()).isEmpty();
    }

    @Test
    @DisplayName("compile_multipleExpands_embedsBothInBody")
    void compile_multipleExpands_embedsBothInBody() {
        // Suppose orders table has 2 FKs — mock a second one
        TableInfo ordersWithTwo = new TableInfo(
                "orders",
                List.of(
                        new ColumnInfo("order_id", "integer", true, false),
                        new ColumnInfo("customer_id", "integer", false, false),
                        new ColumnInfo("product_id", "integer", false, false),
                        new ColumnInfo("total", "decimal", false, true)
                ),
                List.of(
                        new ForeignKeyInfo("customer_id", "customers", "customer_id"),
                        new ForeignKeyInfo("product_id", "products", "product_id")
                )
        );

        CompiledQuery q = compiler.compile("orders", ordersWithTwo, null,
                null, "customers,products", null, "asc", 0, 100, false);

        // Both relationships embedded in the JSON body
        assertThat(q.sql()).contains("'customers'");
        assertThat(q.sql()).contains("'products'");
        assertThat(q.sql()).contains("AS body");
        // jsonColumns is empty since everything is in body
        assertThat(q.jsonColumns()).isEmpty();
    }

    @Test
    @DisplayName("compile_withFilterAndExpandAndCount_allCombined")
    void compile_withFilterAndExpandAndCount_allCombined() {
        when(filterService.parseFilters(any(), any(), any()))
                .thenReturn(List.of("\"tier\" = ?"));

        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("tier", "eq.gold");

        when(databaseSchemaService.getTableSchema())
                .thenReturn(Map.of("orders", ordersTable));

        CompiledQuery q = compiler.compile("customers", customersTable, filters,
                null, "orders", null, "asc", 0, 10, true);

        // Count is a scalar subquery
        assertThat(q.sql()).contains("(SELECT count(*)");
        assertThat(q.sql()).contains("AS total_count");
        // Filter applied
        assertThat(q.sql()).contains("WHERE");
        // Relationship embedded
        assertThat(q.sql()).contains("json_agg");
        assertThat(q.sql()).contains("LIMIT 10");
        // hasCountWindow is false — scalar subquery approach
        assertThat(q.hasCountWindow()).isFalse();
        // jsonColumns empty
        assertThat(q.jsonColumns()).isEmpty();
    }

    // ─── compile() — aggregate functions ─────────────────────────────────────

    @Test
    @DisplayName("compile_countOnlyAggregate_generatesCountStarWithJsonbAgg")
    void compile_countOnlyAggregate_generatesCountStarWithJsonbAgg() {
        CompiledQuery q = compiler.compile("orders", ordersTable, null,
                "count()", null, null, "asc", 0, 100, false);

        // Aggregate result still wrapped in jsonb_agg for consistency
        assertThat(q.sql()).containsIgnoringCase("COUNT(*)");
        assertThat(q.sql()).contains("AS body");
        // No GROUP BY when only aggregate with no regular columns
        assertThat(q.sql()).doesNotContain("GROUP BY");
    }

    @Test
    @DisplayName("compile_inlineAggregates_generatesGroupBy")
    void compile_inlineAggregates_generatesGroupBy() {
        // select=customer_id,total.sum()  →  GROUP BY customer_id
        CompiledQuery q = compiler.compile("orders", ordersTable, null,
                "customer_id,total.sum()", null, null, "asc", 0, 100, false);

        assertThat(q.sql()).containsIgnoringCase("SUM(\"total\")");
        assertThat(q.sql()).containsIgnoringCase("GROUP BY");
        assertThat(q.sql()).contains("\"customer_id\"");
        // Still wrapped in jsonb_agg
        assertThat(q.sql()).contains("AS body");
    }

    // ─── compileCursor() ─────────────────────────────────────────────────────

    @Test
    @DisplayName("compileCursor_forward_addsKeysetConditionWithJsonbAgg")
    void compileCursor_forward_addsKeysetConditionWithJsonbAgg() {
        String cursor = Base64.getEncoder().encodeToString("5".getBytes());

        when(typeConversionService.convertValueToColumnType(anyString(), anyString(), any()))
                .thenAnswer(inv -> inv.getArgument(1)); // return value as-is

        CompiledQuery q = compiler.compileCursor("customers", customersTable, null,
                null, null, "customer_id", "asc",
                "10", cursor, null, null);

        // Still JSON body
        assertThat(q.sql()).contains("AS body");
        assertThat(q.sql()).contains("jsonb_agg");
        // Cursor condition in inner query
        assertThat(q.sql()).contains("\"customer_id\" >");
        assertThat(q.sql()).contains("ORDER BY \"customer_id\" ASC");
        // LIMIT N+1 for hasNextPage detection
        assertThat(q.sql()).contains("LIMIT 11");
        // hasCountWindow is false — cursor queries no longer use window count
        assertThat(q.hasCountWindow()).isFalse();
        assertThat(q.params()).hasSize(1);
    }

    @Test
    @DisplayName("compileCursor_backward_reverseKeysetCondition")
    void compileCursor_backward_reverseKeysetCondition() {
        String cursor = Base64.getEncoder().encodeToString("20".getBytes());

        when(typeConversionService.convertValueToColumnType(anyString(), anyString(), any()))
                .thenAnswer(inv -> inv.getArgument(1));

        // last + before = backward pagination
        CompiledQuery q = compiler.compileCursor("customers", customersTable, null,
                null, null, "customer_id", "asc",
                null, null, "10", cursor);

        assertThat(q.sql()).contains("AS body");
        assertThat(q.sql()).contains("WHERE");
        assertThat(q.sql()).contains("\"customer_id\"");
        assertThat(q.sql()).contains("LIMIT 11");
        // Order is reversed for backward pagination
        assertThat(q.sql()).containsIgnoringCase("ORDER BY \"customer_id\" DESC");
    }

    @Test
    @DisplayName("compileCursor_noAfterOrBefore_noWhereClauseStillWrapsInJsonbAgg")
    void compileCursor_noAfterOrBefore_noWhereClauseStillWrapsInJsonbAgg() {
        CompiledQuery q = compiler.compileCursor("customers", customersTable, null,
                null, null, null, "asc",
                "10", null, null, null);

        // No cursor value means no WHERE clause for cursor condition
        assertThat(q.sql()).doesNotContain("WHERE");
        assertThat(q.sql()).contains("LIMIT 11");
        // Still JSON body
        assertThat(q.sql()).contains("AS body");
        assertThat(q.sql()).contains("jsonb_agg");
        // hasCountWindow false
        assertThat(q.hasCountWindow()).isFalse();
    }

    @Test
    @DisplayName("compileCursor_alwaysHasJsonBodyColumn")
    void compileCursor_alwaysHasJsonBodyColumn() {
        CompiledQuery q = compiler.compileCursor("customers", customersTable, null,
                null, null, null, "asc",
                "10", null, null, null);

        // The result always has a body column (JSON array) — jsonColumns empty
        // because the entire result is in body
        assertThat(q.jsonColumns()).isEmpty();
        assertThat(q.hasCountWindow()).isFalse();
    }

    // ─── Additional branch coverage tests ────────────────────────────────────

    @Test
    @DisplayName("compile_withNullFilters_noWhereClause")
    void compile_withNullFilters_noWhereClause() {
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, null, null, null, 0, 50, false);

        assertThat(q.sql()).doesNotContain("WHERE");
        assertThat(q.sql()).contains("LIMIT 50");
    }

    @Test
    @DisplayName("compile_withEmptyFilters_noWhereClause")
    void compile_withEmptyFilters_noWhereClause() {
        MultiValueMap<String, String> emptyFilters = new LinkedMultiValueMap<>();
        CompiledQuery q = compiler.compile("customers", customersTable, emptyFilters,
                null, null, null, null, 0, 50, false);

        assertThat(q.sql()).doesNotContain("WHERE");
    }

    @Test
    @DisplayName("compile_withPostgrestOrderAndNoDirection_defaultsToAsc")
    void compile_withPostgrestOrderAndNoDirection_defaultsToAsc() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("order", "name");

        CompiledQuery q = compiler.compile("customers", customersTable, params,
                null, null, null, null, 0, 100, false);

        assertThat(q.sql()).containsIgnoringCase("ORDER BY \"name\" ASC");
    }

    @Test
    @DisplayName("compile_aggregateWithFilter_whereInAggregateQuery")
    void compile_aggregateWithFilter_whereInAggregateQuery() {
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("customer_id", "eq.1");

        when(filterService.parseFilters(any(), any(), any()))
                .thenReturn(List.of("\"customer_id\" = ?"));

        CompiledQuery q = compiler.compile("orders", ordersTable, filters,
                "count()", null, null, "asc", 0, 100, false);

        assertThat(q.sql()).contains("WHERE");
        assertThat(q.sql()).contains("COUNT(*)");
        assertThat(q.sql()).contains("AS body");
    }

    @Test
    @DisplayName("compile_aggregateMinMax_generatesMinMaxExpressions")
    void compile_aggregateMinMax_generatesMinMaxExpressions() {
        CompiledQuery q = compiler.compile("orders", ordersTable, null,
                "total.min(),total.max()", null, null, "asc", 0, 100, false);

        assertThat(q.sql()).containsIgnoringCase("MIN(\"total\")");
        assertThat(q.sql()).containsIgnoringCase("MAX(\"total\")");
        assertThat(q.sql()).contains("AS body");
    }

    @Test
    @DisplayName("compile_aggregateAvg_generatesAvgExpression")
    void compile_aggregateAvg_generatesAvgExpression() {
        CompiledQuery q = compiler.compile("orders", ordersTable, null,
                "total.avg()", null, null, "asc", 0, 100, false);

        assertThat(q.sql()).containsIgnoringCase("AVG(\"total\")");
        assertThat(q.sql()).contains("AS body");
    }

    @Test
    @DisplayName("compile_expandWithUnknownRelationship_ignoresUnknown")
    void compile_expandWithUnknownRelationship_ignoresUnknown() {
        // customers table has no FK to "nonexistent" and nonexistent is not in schema
        when(databaseSchemaService.getTableSchema()).thenReturn(Map.of());

        // Should not throw — just logs a warning and falls back to row_to_json
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, "nonexistent", null, "asc", 0, 100, false);

        // Still generates valid JSON body SQL, just without the unknown relationship
        assertThat(q.sql()).contains("AS body");
    }

    @Test
    @DisplayName("compileCursor_withFilters_filtersInInnerSubquery")
    void compileCursor_withFilters_filtersInInnerSubquery() {
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("tier", "eq.gold");

        when(filterService.parseFilters(any(), any(), any()))
                .thenReturn(List.of("\"tier\" = ?"));

        CompiledQuery q = compiler.compileCursor("customers", customersTable, filters,
                null, null, "customer_id", "asc",
                "10", null, null, null);

        assertThat(q.sql()).contains("WHERE");
        assertThat(q.sql()).contains("\"tier\" = ?");
        assertThat(q.sql()).contains("AS body");
    }

    @Test
    @DisplayName("compileCursor_withOrderParamInFilters_parsesPostgrestOrder")
    void compileCursor_withOrderParamInFilters_parsesPostgrestOrder() {
        // Verify that the 'order' control param is stripped from cursor query filters too
        MultiValueMap<String, String> filters = new LinkedMultiValueMap<>();
        filters.add("order", "name.asc");

        // FilterService should not be called with 'order' as a column filter
        when(filterService.parseFilters(any(), any(), any()))
                .thenReturn(List.of());

        CompiledQuery q = compiler.compileCursor("customers", customersTable, filters,
                null, null, "customer_id", "asc",
                "5", null, null, null);

        assertThat(q.sql()).contains("AS body");
        assertThat(q.sql()).contains("LIMIT 6");
    }

    @Test
    @DisplayName("compile_embeddedFieldAlreadyInExpand_deduplicates")
    void compile_embeddedFieldAlreadyInExpand_deduplicates() {
        // When the same relationship appears in both select=customers(...) and expand=customers,
        // it should only be embedded once.
        when(databaseSchemaService.getTableSchema())
                .thenReturn(Map.of("orders", ordersTable));

        CompiledQuery q = compiler.compile("orders", ordersTable, null,
                "order_id,customers(name)", "customers", null, "asc", 0, 100, false);

        // 'customers' should appear only once as a key
        long occurrences = q.sql().chars()
                .filter(c -> q.sql().indexOf("'customers'") >= 0)
                .count();
        assertThat(q.sql()).contains("'customers'");
        // The SQL should have exactly one 'customers' key in jsonb_build_object
        int firstIdx = q.sql().indexOf("'customers'");
        int secondIdx = q.sql().indexOf("'customers'", firstIdx + 1);
        assertThat(secondIdx).isEqualTo(-1); // Only one occurrence
    }

    @Test
    @DisplayName("compile_postgrestOrderInvalidColumn_throwsException")
    void compile_postgrestOrderInvalidColumn_throwsException() {
        MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
        params.add("order", "nonexistent_col.desc");

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                compiler.compile("customers", customersTable, params,
                        null, null, null, "asc", 0, 100, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid column for ordering");
    }

    @Test
    @DisplayName("compile_selectWithWildcardAndRelationship_allColumnsInJsonbBuildObject")
    void compile_selectWithWildcardAndRelationship_allColumnsInJsonbBuildObject() {
        // When select=* but with relationships, all columns must be explicitly listed
        when(databaseSchemaService.getTableSchema())
                .thenReturn(Map.of("orders", ordersTable));

        CompiledQuery q = compiler.compile("customers", customersTable, null,
                "*", "orders", null, "asc", 0, 100, false);

        // All customer columns should appear in jsonb_build_object
        assertThat(q.sql()).contains("'customer_id'");
        assertThat(q.sql()).contains("'name'");
        assertThat(q.sql()).contains("'email'");
        assertThat(q.sql()).contains("'tier'");
        // Relationship embedded
        assertThat(q.sql()).contains("'orders'");
    }

    @Test
    @DisplayName("containsAggregateExpression_detectsAllAggFunctions")
    void containsAggregateExpression_detectsAllAggFunctions() {
        assertThat(PostgresQueryCompiler.containsAggregateExpression("count()")).isTrue();
        assertThat(PostgresQueryCompiler.containsAggregateExpression("total.sum()")).isTrue();
        assertThat(PostgresQueryCompiler.containsAggregateExpression("price.avg()")).isTrue();
        assertThat(PostgresQueryCompiler.containsAggregateExpression("amount.min()")).isTrue();
        assertThat(PostgresQueryCompiler.containsAggregateExpression("val.max()")).isTrue();
        assertThat(PostgresQueryCompiler.containsAggregateExpression("name,email")).isFalse();
        assertThat(PostgresQueryCompiler.containsAggregateExpression(null)).isFalse();
        assertThat(PostgresQueryCompiler.containsAggregateExpression("")).isFalse();
    }

    @Test
    @DisplayName("isAggregate_identifiesAggregateFields")
    void isAggregate_identifiesAggregateFields() {
        assertThat(PostgresQueryCompiler.isAggregate(new io.github.excalibase.model.SelectField("count()"))).isTrue();
        assertThat(PostgresQueryCompiler.isAggregate(new io.github.excalibase.model.SelectField("total.sum()"))).isTrue();
        assertThat(PostgresQueryCompiler.isAggregate(new io.github.excalibase.model.SelectField("price.avg()"))).isTrue();
        assertThat(PostgresQueryCompiler.isAggregate(new io.github.excalibase.model.SelectField("name"))).isFalse();
        assertThat(PostgresQueryCompiler.isAggregate(new io.github.excalibase.model.SelectField("*"))).isFalse();
    }

    // ─── SQL injection guard ────────────────────────────────────────────────

    @Test
    @DisplayName("compile_rejectsInjectionInExpandSelect")
    void compile_rejectsInjectionInExpandSelect() {
        when(databaseSchemaService.getTableSchema()).thenReturn(Map.of(
                "customers", customersTable, "orders", ordersTable));

        // Attempt SQL injection via expand select param
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
                compiler.compile("orders", ordersTable, null,
                        null, "customers(select:id\") UNION SELECT * FROM pg_shadow--)",
                        null, "asc", 0, 10, false));
    }

    @Test
    @DisplayName("compile_rejectsInjectionInExpandOrder")
    void compile_rejectsInjectionInExpandOrder() {
        when(databaseSchemaService.getTableSchema()).thenReturn(Map.of(
                "customers", customersTable, "orders", ordersTable));

        // Reverse relationship: customers → orders. Injection in order param.
        // The q() guard rejects the identifier, relationship is silently dropped.
        // Verify the injection payload does NOT appear in the generated SQL.
        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, "orders(order:total\"; DROP TABLE orders--)",
                null, "asc", 0, 10, false);

        assertThat(q.sql()).doesNotContain("DROP TABLE");
        assertThat(q.sql()).doesNotContain("orders");  // Relationship silently dropped
    }

    // ─── Parameterized expand ───────────────────────────────────────────────

    @Test
    @DisplayName("compile_parameterizedExpand_appliesLimitAndOrder")
    void compile_parameterizedExpand_appliesLimitAndOrder() {
        when(databaseSchemaService.getTableSchema()).thenReturn(Map.of(
                "customers", customersTable, "orders", ordersTable));

        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, "orders(limit:5,order:total.desc)",
                null, "asc", 0, 10, false);

        assertThat(q.sql()).contains("LIMIT 5");
        assertThat(q.sql()).contains("\"total\" DESC");
        assertThat(q.sql()).contains("COALESCE");
    }

    @Test
    @DisplayName("compile_parameterizedExpand_appliesSelect")
    void compile_parameterizedExpand_appliesSelect() {
        when(databaseSchemaService.getTableSchema()).thenReturn(Map.of(
                "customers", customersTable, "orders", ordersTable));

        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, "orders(select:order_id,total)",
                null, "asc", 0, 10, false);

        // Should have specific columns in the relationship subquery
        assertThat(q.sql()).contains("\"order_id\"");
        assertThat(q.sql()).contains("\"total\"");
        // The relationship subquery should use explicit columns, not SELECT *
        // Extract the COALESCE subquery part (the reverse relationship)
        int coalesceStart = q.sql().indexOf("COALESCE(");
        assertThat(coalesceStart).isGreaterThan(0);
        String relSubquery = q.sql().substring(coalesceStart, q.sql().indexOf("'[]'::json)", coalesceStart) + 11);
        assertThat(relSubquery).doesNotContain("SELECT *");
    }

    // ─── Nested expand ──────────────────────────────────────────────────────

    @Test
    @DisplayName("compile_nestedExpand_generatesNestedSubqueries")
    void compile_nestedExpand_generatesNestedSubqueries() {
        TableInfo orderItemsTable = new TableInfo("order_items",
                List.of(
                        new ColumnInfo("id", "integer", true, false),
                        new ColumnInfo("order_id", "integer", false, false),
                        new ColumnInfo("quantity", "integer", false, false)
                ),
                List.of(new ForeignKeyInfo("order_id", "orders", "order_id")));

        when(databaseSchemaService.getTableSchema()).thenReturn(Map.of(
                "customers", customersTable, "orders", ordersTable, "order_items", orderItemsTable));

        CompiledQuery q = compiler.compile("customers", customersTable, null,
                null, "orders.order_items",
                null, "asc", 0, 10, false);

        // Should have nested subqueries — orders containing order_items
        assertThat(q.sql()).contains("\"orders\"");
        assertThat(q.sql()).contains("\"order_items\"");
        // Should reference order_items within the orders subquery
        assertThat(q.sql()).contains("AS \"order_items\"");
    }

    // ─── Cursor + expand ────────────────────────────────────────────────────

    @Test
    @DisplayName("compileCursor_withExpand_includesRelationship")
    void compileCursor_withExpand_includesRelationship() {
        when(databaseSchemaService.getTableSchema()).thenReturn(Map.of(
                "customers", customersTable, "orders", ordersTable));

        CompiledQuery q = compiler.compileCursor("orders", ordersTable, null,
                null, "customers",
                "order_id", "asc",
                "5", null, null, null);

        assertThat(q.sql()).contains("'customers'");
        assertThat(q.sql()).contains("row_to_json");
    }
}
