package io.github.excalibase.postgres.compiler;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.compiler.IQueryCompiler;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.SelectField;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.postgres.service.DatabaseSchemaService;
import io.github.excalibase.postgres.service.SelectParserService;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.TypeConversionService;
import static io.github.excalibase.util.SqlIdentifier.quoteIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Postgres implementation of {@link IQueryCompiler}.
 *
 * <p>Compiles all query requirements (data, count, relationships, aggregates) into a
 * <em>single</em> SQL statement that returns <strong>1 row / 1 column</strong> containing
 * the result as a pre-built JSON array. This eliminates all Java-side row iteration and
 * type mapping for collection queries.
 *
 * <ul>
 *   <li>SELECT * queries use {@code jsonb_agg(row_to_json(c))} for zero-overhead JSON building.</li>
 *   <li>Explicit column lists use {@code jsonb_agg(jsonb_build_object('col', c."col", ...))}.</li>
 *   <li>Relationships are embedded directly inside the {@code jsonb_build_object} call as subquery values.</li>
 *   <li>Count uses a correlated scalar subquery {@code (SELECT count(*) FROM "t" WHERE ...)} instead of a window
 *       function, keeping the outer aggregation clean.</li>
 *   <li>Aggregate queries (GROUP BY) wrap their result in {@code jsonb_agg(jsonb_build_object(...))} as well.</li>
 *   <li>Cursor (keyset) pagination embeds the cursor condition in the inner subquery and still returns a JSON body.</li>
 * </ul>
 *
 * <p>The result set always has a {@code body} column containing a jsonb value (parsed by the JDBC
 * driver as a {@code PGobject}). When {@code includeCount=true} a second {@code total_count} column
 * (bigint) is present. {@link io.github.excalibase.postgres.service.ResultMapper} reads these two
 * columns directly — no per-row iteration needed.
 */
@Service
public class PostgresQueryCompiler implements IQueryCompiler {

    private static final Logger log = LoggerFactory.getLogger(PostgresQueryCompiler.class);

    private static final Set<String> AGGREGATE_NAMES = Set.of("count", "sum", "avg", "min", "max");

    private final FilterService filterService;
    private final DatabaseSchemaService databaseSchemaService;
    private final TypeConversionService typeConversionService;
    private final SelectParserService selectParserService;
    private final String allowedSchema;

    public PostgresQueryCompiler(FilterService filterService,
                                 DatabaseSchemaService databaseSchemaService,
                                 TypeConversionService typeConversionService,
                                 SelectParserService selectParserService,
                                 @Value("${app.allowed-schema:public}") String allowedSchema) {
        this.filterService = filterService;
        this.databaseSchemaService = databaseSchemaService;
        this.typeConversionService = typeConversionService;
        this.selectParserService = selectParserService;
        this.allowedSchema = allowedSchema;
    }

    // ─── IQueryCompiler ───────────────────────────────────────────────────────

    @Override
    public CompiledQuery compile(String tableName, TableInfo tableInfo,
                                 MultiValueMap<String, String> filters,
                                 String select, String expand,
                                 String orderBy, String orderDirection,
                                 int offset, int limit,
                                 boolean includeCount) {

        aliasCounter.set(0);
        List<Object> params = new ArrayList<>();

        // ── Detect aggregates from raw select string ──────────────────────────
        boolean hasAggregates = containsAggregateExpression(select);

        if (hasAggregates) {
            return compileAggregateQuery(tableName, tableInfo, filters, select, params);
        }

        return compileJsonQuery(tableName, tableInfo, filters, select, expand,
                orderBy, orderDirection, offset, limit, includeCount, params);
    }

    @Override
    public CompiledQuery compileCursor(String tableName, TableInfo tableInfo,
                                       MultiValueMap<String, String> filters,
                                       String select, String expand,
                                       String orderBy, String orderDirection,
                                       String first, String after,
                                       String last, String before) {

        aliasCounter.set(0);
        List<Object> params = new ArrayList<>();

        String effectiveOrderBy = resolveOrderBy(orderBy, tableInfo);
        boolean forward = (last == null);
        int limit = resolveLimit(first, last);

        // ── Build WHERE conditions (filters + cursor) ─────────────────────────
        List<String> conditions = new ArrayList<>();
        String orderParam = null;
        if (filters != null && !filters.isEmpty()) {
            MultiValueMap<String, String> dataFilters = stripControlParams(filters);
            orderParam = filters.getFirst("order");
            if (!dataFilters.isEmpty()) {
                conditions.addAll(filterService.parseFilters(dataFilters, params, tableInfo));
            }
        }
        appendCursorCondition(conditions, params, effectiveOrderBy, tableInfo, after, before, forward);

        // ── ORDER BY direction ────────────────────────────────────────────────
        String dir = forward
                ? (orderDirection != null && orderDirection.equalsIgnoreCase("desc") ? "DESC" : "ASC")
                : (orderDirection != null && orderDirection.equalsIgnoreCase("desc") ? "ASC" : "DESC");

        // ── Build inner subquery ──────────────────────────────────────────────
        // Fetch limit+1 to detect hasNextPage
        String innerSql = buildInnerSubquery(tableName, tableInfo, conditions,
                effectiveOrderBy, dir, limit + 1, 0);

        // ── Parse select for outer JSON projection ────────────────────────────
        List<SelectField> selectFields = parseSelectFields(select);
        List<SelectField> embeddedFields = selectParserService.getEmbeddedFields(selectFields);
        List<SelectField> simpleFields = selectFields.stream()
                .filter(f -> !f.isEmbedded())
                .collect(Collectors.toList());

        boolean selectStar = isSelectStar(simpleFields);

        // ── Build outer SELECT ────────────────────────────────────────────────
        String outerAgg = buildOuterAggExpression(selectStar, simpleFields, embeddedFields,
                tableName, tableInfo, expand, "c");

        String sql = "SELECT coalesce(" + outerAgg + ", '[]'::jsonb) AS body"
                + " FROM (" + innerSql + ") c";

        return new CompiledQuery(sql, params.toArray(), false, List.of());
    }

    // ─── Core JSON query builder ──────────────────────────────────────────────

    /**
     * Builds the main non-aggregate JSON-returning query.
     * Pattern:
     * <pre>
     * SELECT coalesce(jsonb_agg(...), '[]'::jsonb) AS body
     *      [, (SELECT count(*) FROM "t" WHERE ...) AS total_count]
     * FROM (SELECT * FROM "t" WHERE ... ORDER BY ... LIMIT N OFFSET M) c
     * </pre>
     */
    private CompiledQuery compileJsonQuery(String tableName, TableInfo tableInfo,
                                           MultiValueMap<String, String> filters,
                                           String select, String expand,
                                           String orderBy, String orderDirection,
                                           int offset, int limit,
                                           boolean includeCount,
                                           List<Object> params) {

        // ── Extract 'order' param before filtering ────────────────────────────
        String orderParam = null;
        List<String> conditions = new ArrayList<>();
        // Collect filter params separately so we can duplicate them for the count subquery
        List<Object> filterParams = new ArrayList<>();
        if (filters != null && !filters.isEmpty()) {
            MultiValueMap<String, String> dataFilters = stripControlParams(filters);
            orderParam = filters.getFirst("order");
            if (!dataFilters.isEmpty()) {
                conditions.addAll(filterService.parseFilters(dataFilters, filterParams, tableInfo));
            }
        }

        // ── ORDER BY resolution ───────────────────────────────────────────────
        String resolvedOrderBy = resolveOrderByString(orderParam, orderBy, orderDirection, tableInfo);

        // ── Build inner subquery ──────────────────────────────────────────────
        String innerSql = buildInnerSubqueryWithOrderString(tableName, conditions, resolvedOrderBy, limit, offset);

        // ── Parse select fields ───────────────────────────────────────────────
        List<SelectField> selectFields = parseSelectFields(select);
        List<SelectField> embeddedFields = selectParserService.getEmbeddedFields(selectFields);
        List<SelectField> simpleFields = selectFields.stream()
                .filter(f -> !f.isEmbedded())
                .collect(Collectors.toList());

        boolean selectStar = isSelectStar(simpleFields);

        // ── Build outer aggregation expression ────────────────────────────────
        String outerAgg = buildOuterAggExpression(selectStar, simpleFields, embeddedFields,
                tableName, tableInfo, expand, "c");

        // ── Assemble final SQL ────────────────────────────────────────────────
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT coalesce(").append(outerAgg).append(", '[]'::jsonb) AS body");

        if (includeCount) {
            // Scalar count subquery — same WHERE conditions as the inner query.
            // The count subquery is a separate SQL clause so its ? placeholders need
            // their own copies of the bind parameters — append them BEFORE the inner
            // subquery params so the parameter order matches left-to-right in the SQL.
            sql.append(", (SELECT count(*) FROM \"").append(tableName).append("\"");
            if (!conditions.isEmpty()) {
                sql.append(" WHERE ").append(String.join(" AND ", conditions));
                // Count subquery params come first (they appear first in the SQL)
                params.addAll(filterParams);
            }
            sql.append(") AS total_count");
        }

        // Inner subquery params come last
        params.addAll(filterParams);

        sql.append(" FROM (").append(innerSql).append(") c");

        return new CompiledQuery(sql.toString(), params.toArray(), false, List.of());
    }

    /**
     * Builds an aggregate (GROUP BY) query that still returns JSON via jsonb_agg.
     */
    private CompiledQuery compileAggregateQuery(String tableName, TableInfo tableInfo,
                                                 MultiValueMap<String, String> filters,
                                                 String select,
                                                 List<Object> params) {
        List<SelectField> simpleFields = parseAggregateFields(select);

        // ── Build the aggregate inner SELECT clause ───────────────────────────
        StringBuilder innerSql = new StringBuilder("SELECT ");
        buildAggregateSelectClause(innerSql, simpleFields, "a");
        innerSql.append(" FROM \"").append(tableName).append("\" a");

        // ── WHERE ─────────────────────────────────────────────────────────────
        if (filters != null && !filters.isEmpty()) {
            MultiValueMap<String, String> dataFilters = stripControlParams(filters);
            if (!dataFilters.isEmpty()) {
                List<String> conditions = filterService.parseFilters(dataFilters, params, tableInfo);
                if (!conditions.isEmpty()) {
                    innerSql.append(" WHERE ").append(String.join(" AND ", conditions));
                }
            }
        }

        // ── GROUP BY ──────────────────────────────────────────────────────────
        List<String> groupByCols = simpleFields.stream()
                .filter(f -> !isAggregate(f))
                .filter(f -> !f.isWildcard())
                .map(f -> "a.\"" + f.getName() + "\"")
                .collect(Collectors.toList());
        if (!groupByCols.isEmpty()) {
            innerSql.append(" GROUP BY ").append(String.join(", ", groupByCols));
        }

        // ── Wrap in jsonb_agg ─────────────────────────────────────────────────
        // Build jsonb_build_object keys from aggregate fields
        String jsonbBuildObj = buildAggregateJsonbBuildObject(simpleFields, "c");
        String sql = "SELECT coalesce(jsonb_agg(" + jsonbBuildObj + "), '[]'::jsonb) AS body"
                + " FROM (" + innerSql + ") c";

        return new CompiledQuery(sql, params.toArray(), false, List.of());
    }

    // ─── Inner subquery builders ──────────────────────────────────────────────

    /**
     * Build an inner subquery: {@code SELECT * FROM "table" [WHERE ...] ORDER BY "col" DIR LIMIT N OFFSET M}
     */
    private String buildInnerSubquery(String tableName, TableInfo tableInfo,
                                       List<String> conditions,
                                       String orderBy, String dir,
                                       int limit, int offset) {
        StringBuilder sb = new StringBuilder("SELECT * FROM \"").append(tableName).append("\"");
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        sb.append(" ORDER BY ").append(quoteIdentifier(orderBy)).append(" ").append(dir);
        sb.append(" LIMIT ").append(limit);
        if (offset > 0) {
            sb.append(" OFFSET ").append(offset);
        }
        return sb.toString();
    }

    /**
     * Build an inner subquery with a fully-resolved ORDER BY string (handles PostgREST style,
     * no-order, etc.) and always includes OFFSET.
     */
    private String buildInnerSubqueryWithOrderString(String tableName,
                                                      List<String> conditions,
                                                      String orderByClause,
                                                      int limit, int offset) {
        StringBuilder sb = new StringBuilder("SELECT * FROM \"").append(tableName).append("\"");
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        if (orderByClause != null && !orderByClause.isBlank()) {
            sb.append(" ORDER BY ").append(orderByClause);
        }
        sb.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);
        return sb.toString();
    }

    // ─── Outer aggregation expression builder ─────────────────────────────────

    /**
     * Returns the aggregation expression to place inside {@code coalesce(..., '[]'::jsonb)}.
     *
     * <ul>
     *   <li>SELECT * with no relationships → {@code jsonb_agg(row_to_json(c))}</li>
     *   <li>Explicit columns or relationships → {@code jsonb_agg(jsonb_build_object('col', c."col", ...))}</li>
     * </ul>
     *
     * @param selectStar    true if the query requests all columns with no explicit list
     * @param simpleFields  non-embedded select fields
     * @param embeddedFields embedded (relationship) fields from select
     * @param tableName     base table name (for relationship resolution)
     * @param tableInfo     table metadata
     * @param expand        legacy expand parameter
     * @param alias         alias of the inner subquery (typically "c")
     */
    private String buildOuterAggExpression(boolean selectStar,
                                            List<SelectField> simpleFields,
                                            List<SelectField> embeddedFields,
                                            String tableName,
                                            TableInfo tableInfo,
                                            String expand,
                                            String alias) {

        // Collect relationship entries from both embedded fields and legacy expand param
        List<String[]> relEntries = buildRelationshipEntries(embeddedFields, expand, tableName, tableInfo, alias);

        if (selectStar && relEntries.isEmpty()) {
            // Simplest form: no explicit columns, no relationships
            return "jsonb_agg(row_to_json(" + alias + "))";
        }

        // Build jsonb_build_object entries
        List<String> entries = new ArrayList<>();

        if (selectStar) {
            // SELECT * but with relationships — need to use jsonb_build_object
            // Include all table columns explicitly so we can append relationships
            for (ColumnInfo col : tableInfo.getColumns()) {
                entries.add("'" + col.getName() + "', " + alias + ".\"" + col.getName() + "\"");
            }
        } else {
            for (SelectField sf : simpleFields) {
                if (sf.isWildcard()) {
                    // wildcard mixed with explicit — include all columns
                    for (ColumnInfo col : tableInfo.getColumns()) {
                        entries.add("'" + col.getName() + "', " + alias + ".\"" + col.getName() + "\"");
                    }
                } else {
                    String jsonKey = sf.getAlias() != null ? sf.getAlias() : sf.getName();
                    String sqlExpr = alias + ".\"" + sf.getName() + "\"";
                    entries.add("'" + jsonKey + "', " + sqlExpr);
                }
            }
        }

        // Append relationship entries
        for (String[] entry : relEntries) {
            entries.add(entry[0]);
        }

        return "jsonb_agg(jsonb_build_object(" + String.join(", ", entries) + "))";
    }

    /**
     * Build relationship key-value entries for jsonb_build_object.
     * Returns list of pairs where [0] is the full {@code 'key', subquery} string.
     */
    private List<String[]> buildRelationshipEntries(List<SelectField> embeddedFields,
                                                     String expand,
                                                     String tableName,
                                                     TableInfo tableInfo,
                                                     String alias) {
        List<String[]> entries = new ArrayList<>();
        Set<String> seen = new java.util.HashSet<>();

        // Embedded fields from select parameter
        for (SelectField embedded : embeddedFields) {
            String relName = embedded.getName();
            String subQuery = buildRelationshipSubquery(relName, embedded.getSubFields(),
                    ExpandParams.EMPTY, null, tableName, tableInfo, alias);
            if (subQuery != null) {
                entries.add(new String[]{"'" + relName + "', " + subQuery});
                seen.add(relName);
            }
        }

        // Expand parameter with support for params and nesting
        if (expand != null && !expand.isBlank()) {
            for (String expandItem : expand.split(",(?![^(]*\\))")) {
                ParsedExpand parsed = parseExpandItem(expandItem);
                if (parsed == null || seen.contains(parsed.relationName())) continue;

                String subQuery = buildRelationshipSubquery(parsed.relationName(), List.of(),
                        parsed.params(), parsed.nestedExpand(), tableName, tableInfo, alias);
                if (subQuery != null) {
                    entries.add(new String[]{"'" + parsed.relationName() + "', " + subQuery});
                    seen.add(parsed.relationName());
                }
            }
        }

        return entries;
    }

    // ─── Aggregate JSON expression builder ────────────────────────────────────

    /**
     * Build a jsonb_build_object expression for aggregate query results.
     * Each field becomes a key-value pair using the alias name.
     */
    private String buildAggregateJsonbBuildObject(List<SelectField> simpleFields, String alias) {
        List<String> entries = new ArrayList<>();
        for (SelectField sf : simpleFields) {
            if (sf.isWildcard()) continue;
            String name = sf.getName();
            String jsonKey;
            String colExpr;

            if ("count()".equals(name)) {
                jsonKey = sf.getAlias() != null ? sf.getAlias() : "count";
                colExpr = alias + ".\"" + jsonKey + "\"";
            } else if (name.endsWith(".sum()") || name.endsWith(".avg()") || name.endsWith(".min()") || name.endsWith(".max()")) {
                int dot = name.lastIndexOf('.');
                String func = name.substring(dot + 1, name.length() - 2).toLowerCase();
                jsonKey = sf.getAlias() != null ? sf.getAlias() : func;
                colExpr = alias + ".\"" + jsonKey + "\"";
            } else {
                jsonKey = sf.getAlias() != null ? sf.getAlias() : name;
                colExpr = alias + ".\"" + name + "\"";
            }
            entries.add("'" + jsonKey + "', " + colExpr);
        }
        if (entries.isEmpty()) {
            return "row_to_json(" + alias + ")";
        }
        return "jsonb_build_object(" + String.join(", ", entries) + ")";
    }

    // ─── SELECT clause builders ───────────────────────────────────────────────

    /**
     * Build the SELECT clause for aggregate queries.
     * e.g. {@code SELECT "status", COUNT(*) AS "count", SUM("total") AS "sum"}
     */
    private void buildAggregateSelectClause(StringBuilder sql,
                                             List<SelectField> simpleFields,
                                             String tableAlias) {
        List<String> parts = new ArrayList<>();

        for (SelectField sf : simpleFields) {
            if (sf.isWildcard()) {
                parts.add(tableAlias + ".*");
                continue;
            }
            String name = sf.getName();
            // count() — no column
            if ("count()".equals(name)) {
                String alias = sf.getAlias() != null ? sf.getAlias() : "count";
                parts.add("COUNT(*) AS " + quoteIdentifier(alias));
                continue;
            }
            // col.func()  e.g. "total.sum()"
            if (name.endsWith(".sum()") || name.endsWith(".avg()") || name.endsWith(".min()") || name.endsWith(".max()")) {
                int dot = name.lastIndexOf('.');
                String col = name.substring(0, dot);
                String func = name.substring(dot + 1, name.length() - 2).toUpperCase();
                String alias = sf.getAlias() != null ? sf.getAlias() : func.toLowerCase();
                parts.add(func + "(" + quoteIdentifier(col) + ") AS " + quoteIdentifier(alias));
                continue;
            }
            // Regular column (part of GROUP BY)
            parts.add(tableAlias + ".\"" + sf.getName() + "\"");
        }

        sql.append(String.join(", ", parts));
    }

    // ─── Expand parameter parsing ──────────────────────────────────────────

    /**
     * Parsed expand parameters from syntax like {@code orders(limit:2,select:id,status,order:total.desc)}.
     */
    private record ExpandParams(String select, String order, int limit) {
        static final ExpandParams EMPTY = new ExpandParams(null, null, -1);
    }

    /**
     * Parse an expand item like {@code orders(limit:2,select:id,status,order:total.desc)}.
     * Returns [relationName, ExpandParams].
     */
    private record ParsedExpand(String relationName, String nestedExpand, ExpandParams params) {}

    private ParsedExpand parseExpandItem(String expandItem) {
        String item = expandItem.trim();
        if (item.isEmpty()) return null;

        // Handle nested expand: orders.order_items → relation=orders, nested=order_items
        String nestedExpand = null;

        // Extract params portion (inside parentheses)
        ExpandParams params = ExpandParams.EMPTY;
        String relPart = item;
        if (item.contains("(")) {
            int parenStart = item.indexOf('(');
            int parenEnd = item.lastIndexOf(')');
            relPart = item.substring(0, parenStart);
            if (parenEnd > parenStart) {
                params = parseExpandParams(item.substring(parenStart + 1, parenEnd));
            }
        }

        // Handle dot-notation for nested expand
        if (relPart.contains(".")) {
            int dot = relPart.indexOf('.');
            nestedExpand = relPart.substring(dot + 1);
            relPart = relPart.substring(0, dot);
        }

        return new ParsedExpand(relPart, nestedExpand, params);
    }

    private ExpandParams parseExpandParams(String paramsStr) {
        String select = null;
        String order = null;
        int limit = -1;

        // Parse key:value pairs — but 'select' values can contain commas (e.g., select:id,name,status)
        // Strategy: scan for known keys, extract their values
        String remaining = paramsStr;
        while (!remaining.isEmpty()) {
            remaining = remaining.stripLeading();
            if (remaining.startsWith("limit:")) {
                remaining = remaining.substring(6);
                int end = remaining.indexOf(',');
                String val = end >= 0 ? remaining.substring(0, end) : remaining;
                // Check if next part is a key (contains ':') — if not, it's part of a value list
                try { limit = Integer.parseInt(val.trim()); } catch (NumberFormatException ignored) {}
                remaining = end >= 0 ? remaining.substring(end + 1) : "";
            } else if (remaining.startsWith("order:")) {
                remaining = remaining.substring(6);
                // order value continues until next known key or end
                int nextKey = findNextKey(remaining);
                String val = nextKey >= 0 ? remaining.substring(0, nextKey) : remaining;
                order = val.stripTrailing();
                if (order.endsWith(",")) order = order.substring(0, order.length() - 1);
                remaining = nextKey >= 0 ? remaining.substring(nextKey) : "";
            } else if (remaining.startsWith("select:")) {
                remaining = remaining.substring(7);
                // select value continues until next known key or end
                int nextKey = findNextKey(remaining);
                String val = nextKey >= 0 ? remaining.substring(0, nextKey) : remaining;
                select = val.stripTrailing();
                if (select.endsWith(",")) select = select.substring(0, select.length() - 1);
                remaining = nextKey >= 0 ? remaining.substring(nextKey) : "";
            } else {
                // Skip unknown content
                int comma = remaining.indexOf(',');
                remaining = comma >= 0 ? remaining.substring(comma + 1) : "";
            }
        }

        return new ExpandParams(select, order, limit);
    }

    private int findNextKey(String str) {
        String[] keys = {"limit:", "order:", "select:"};
        int min = -1;
        for (String key : keys) {
            int idx = str.indexOf(key);
            if (idx >= 0 && (min < 0 || idx < min)) {
                min = idx;
            }
        }
        return min;
    }

    // ─── Relationship subquery builders ──────────────────────────────────────

    private final ThreadLocal<Integer> aliasCounter = ThreadLocal.withInitial(() -> 0);

    private String buildRelationshipSubquery(String relationName, List<SelectField> subFields,
                                              ExpandParams params, String nestedExpand,
                                              String tableName, TableInfo tableInfo,
                                              String tableAlias) {
        // 1. Forward relationship: FK in current table pointing to relationName
        for (ForeignKeyInfo fk : tableInfo.getForeignKeys()) {
            if (fk.getReferencedTable().equalsIgnoreCase(relationName)) {
                return buildForwardSubquery(fk, subFields, params, nestedExpand, tableAlias);
            }
        }

        // 2. Reverse relationship: FK in relationName table pointing to current table
        try {
            Map<String, TableInfo> allTables = databaseSchemaService.getTableSchema();
            TableInfo relatedTable = allTables.get(relationName);
            if (relatedTable != null) {
                for (ForeignKeyInfo fk : relatedTable.getForeignKeys()) {
                    if (fk.getReferencedTable().equalsIgnoreCase(tableName)) {
                        return buildReverseSubquery(fk, subFields, params, nestedExpand,
                                tableInfo, tableAlias, relationName);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Could not resolve reverse relationship '{}' for table '{}': {}",
                    relationName, tableName, e.getMessage());
        }

        log.warn("Relationship '{}' not found for table '{}'", relationName, tableName);
        return null;
    }

    private String nextAlias() {
        int val = aliasCounter.get() + 1;
        aliasCounter.set(val);
        return "r" + val;
    }

    /**
     * Forward (many-to-one): returns a scalar row_to_json subquery.
     */
    private String buildForwardSubquery(ForeignKeyInfo fk, List<SelectField> subFields,
                                         ExpandParams params, String nestedExpand,
                                         String tableAlias) {
        String referencedTable = fk.getReferencedTable();
        String fkColumn = fk.getColumnName();
        String refColumn = fk.getReferencedColumn();
        String innerAlias = nextAlias();
        String outerAlias = nextAlias();

        String innerSelect = buildInnerSelect(subFields, params, innerAlias);

        // Nested expand for forward relationship
        if (nestedExpand != null) {
            Map<String, TableInfo> allTables = databaseSchemaService.getTableSchema();
            TableInfo refTable = allTables.get(referencedTable);
            if (refTable != null) {
                String nestedSub = buildRelationshipSubquery(nestedExpand, List.of(),
                        ExpandParams.EMPTY, null, referencedTable, refTable, innerAlias);
                if (nestedSub != null) {
                    innerSelect = innerSelect + ", " + nestedSub + " AS \"" + nestedExpand + "\"";
                }
            }
        }

        return "(SELECT row_to_json(" + outerAlias + ".*) FROM (" + innerSelect
                + " FROM \"" + referencedTable + "\" " + innerAlias
                + " WHERE " + innerAlias + ".\"" + refColumn + "\" = " + tableAlias + ".\"" + fkColumn + "\") " + outerAlias + ")";
    }

    /**
     * Reverse (one-to-many): returns a json_agg subquery wrapped in COALESCE.
     */
    private String buildReverseSubquery(ForeignKeyInfo fk, List<SelectField> subFields,
                                         ExpandParams params, String nestedExpand,
                                         TableInfo tableInfo,
                                         String tableAlias, String relatedTableName) {
        String fkColumn = fk.getColumnName();
        String refColumn = fk.getReferencedColumn();
        String innerAlias = nextAlias();
        String outerAlias = nextAlias();

        String pkCol = tableInfo.getColumns().stream()
                .filter(c -> c.getName().equalsIgnoreCase(refColumn))
                .map(ColumnInfo::getName)
                .findFirst()
                .orElse(refColumn);

        String innerSelect = buildInnerSelect(subFields, params, innerAlias);

        // Nested expand for reverse relationship
        if (nestedExpand != null) {
            Map<String, TableInfo> allTables = databaseSchemaService.getTableSchema();
            TableInfo relTable = allTables.get(relatedTableName);
            if (relTable != null) {
                String nestedSub = buildRelationshipSubquery(nestedExpand, List.of(),
                        ExpandParams.EMPTY, null, relatedTableName, relTable, innerAlias);
                if (nestedSub != null) {
                    innerSelect = innerSelect + ", " + nestedSub + " AS \"" + nestedExpand + "\"";
                }
            }
        }

        StringBuilder subQuery = new StringBuilder();
        subQuery.append("(SELECT json_agg(row_to_json(").append(outerAlias).append(".*)) FROM (")
                .append(innerSelect)
                .append(" FROM \"").append(relatedTableName).append("\" ").append(innerAlias)
                .append(" WHERE ").append(innerAlias).append(".\"").append(fkColumn).append("\" = ")
                .append(tableAlias).append(".\"").append(pkCol).append("\"");

        // Apply ORDER BY from expand params
        if (params.order() != null) {
            subQuery.append(" ORDER BY ").append(parseSimpleOrder(params.order(), innerAlias));
        }

        // Apply LIMIT from expand params
        if (params.limit() > 0) {
            subQuery.append(" LIMIT ").append(params.limit());
        }

        subQuery.append(") ").append(outerAlias).append(")");

        return "COALESCE(" + subQuery + ", '[]'::json)";
    }

    /**
     * Parse a simple order string like {@code total.desc} into SQL {@code alias."total" DESC}.
     */
    private String parseSimpleOrder(String order, String alias) {
        List<String> clauses = new ArrayList<>();
        for (String part : order.split(",")) {
            part = part.trim();
            if (part.isEmpty()) continue;
            if (part.contains(".")) {
                String[] bits = part.split("\\.");
                String col = bits[0];
                String dir = bits.length > 1 && bits[1].equalsIgnoreCase("desc") ? "DESC" : "ASC";
                clauses.add(alias + "." + quoteIdentifier(col) + " " + dir);
            } else {
                clauses.add(alias + "." + quoteIdentifier(part) + " ASC");
            }
        }
        return String.join(", ", clauses);
    }

    private String buildInnerSelect(List<SelectField> subFields, ExpandParams params, String innerAlias) {
        // Prefer params.select over subFields
        if (params.select() != null && !params.select().isBlank()) {
            String[] cols = params.select().split(",");
            List<String> parts = new ArrayList<>();
            for (String col : cols) {
                col = col.trim();
                if (!col.isEmpty()) {
                    // quoteIdentifier() validates the identifier format — rejects injection attempts
                    parts.add(innerAlias + "." + quoteIdentifier(col));
                }
            }
            return parts.isEmpty() ? "SELECT *" : "SELECT " + String.join(", ", parts);
        }

        if (subFields == null || subFields.isEmpty()) {
            return "SELECT *";
        }
        List<SelectField> nonWildcard = subFields.stream()
                .filter(sf -> !sf.isWildcard())
                .collect(Collectors.toList());
        if (nonWildcard.isEmpty()) {
            return "SELECT *";
        }
        String cols = nonWildcard.stream()
                .map(sf -> innerAlias + "." + quoteIdentifier(sf.getName()))
                .collect(Collectors.joining(", "));
        return "SELECT " + cols;
    }

    // ─── ORDER BY helpers ─────────────────────────────────────────────────────

    /**
     * Resolve ORDER BY into a SQL fragment (no "ORDER BY" keyword prefix).
     * Returns null/blank if no ordering is specified.
     */
    private String resolveOrderByString(String orderParam, String orderBy, String orderDirection,
                                         TableInfo tableInfo) {
        if (orderParam != null && !orderParam.isBlank()) {
            return parsePostgrestOrderBy(orderParam, tableInfo);
        }
        if (orderBy != null && !orderBy.isBlank()) {
            Set<String> validColumns = tableInfo.getColumns().stream()
                    .map(ColumnInfo::getName).collect(Collectors.toSet());
            if (!validColumns.contains(orderBy)) {
                throw new IllegalArgumentException("Invalid column for ordering: " + orderBy);
            }
            String dir = (orderDirection != null && orderDirection.equalsIgnoreCase("desc")) ? "DESC" : "ASC";
            return quoteIdentifier(orderBy) + " " + dir;
        }
        return null;
    }

    /**
     * Parse PostgREST-style order parameter: {@code order=col.desc,col2.asc}
     * Returns the ORDER BY clause fragment (without the "ORDER BY" keyword).
     */
    private String parsePostgrestOrderBy(String orderParam, TableInfo tableInfo) {
        Set<String> validColumns = tableInfo.getColumns().stream()
                .map(ColumnInfo::getName)
                .collect(Collectors.toSet());

        List<String> clauses = new ArrayList<>();
        for (String part : orderParam.split(",")) {
            part = part.trim();
            if (part.isEmpty()) continue;

            String col;
            String dir = "ASC";
            String nullsOrder = "";

            if (part.contains(".")) {
                String[] dotParts = part.split("\\.");
                col = dotParts[0];
                for (int i = 1; i < dotParts.length; i++) {
                    String modifier = dotParts[i].toLowerCase();
                    switch (modifier) {
                        case "asc" -> dir = "ASC";
                        case "desc" -> dir = "DESC";
                        case "nullsfirst" -> nullsOrder = " NULLS FIRST";
                        case "nullslast" -> nullsOrder = " NULLS LAST";
                    }
                }
            } else {
                col = part;
            }

            if (!validColumns.contains(col)) {
                throw new IllegalArgumentException("Invalid column for ordering: " + col);
            }
            clauses.add(quoteIdentifier(col) + " " + dir + nullsOrder);
        }

        return clauses.isEmpty() ? null : String.join(", ", clauses);
    }

    // ─── Cursor helpers ───────────────────────────────────────────────────────

    private void appendCursorCondition(List<String> conditions, List<Object> params,
                                        String orderBy, TableInfo tableInfo,
                                        String after, String before, boolean forward) {
        if (after != null) {
            String decoded = decodeCursor(after);
            Object converted = typeConversionService.convertValueToColumnType(orderBy, decoded, tableInfo);
            params.add(converted);
            conditions.add(quoteIdentifier(orderBy) + " " + (forward ? ">" : "<") + " ?");
        }
        if (before != null) {
            String decoded = decodeCursor(before);
            Object converted = typeConversionService.convertValueToColumnType(orderBy, decoded, tableInfo);
            params.add(converted);
            conditions.add(quoteIdentifier(orderBy) + " " + (forward ? "<" : ">") + " ?");
        }
    }

    private String decodeCursor(String cursor) {
        try {
            return new String(Base64.getDecoder().decode(cursor));
        } catch (Exception e) {
            return cursor;
        }
    }

    // ─── Utility helpers ──────────────────────────────────────────────────────

    private List<SelectField> parseSelectFields(String select) {
        if (select == null || select.isBlank()) {
            return List.of(new SelectField("*"));
        }
        return selectParserService.parseSelect(select);
    }

    private boolean isSelectStar(List<SelectField> fields) {
        return fields.isEmpty()
                || (fields.size() == 1 && fields.get(0).isWildcard());
    }

    private String resolveOrderBy(String orderBy, TableInfo tableInfo) {
        if (orderBy != null && !orderBy.isBlank()) {
            return orderBy;
        }
        return tableInfo.getColumns().stream()
                .filter(ColumnInfo::isPrimaryKey)
                .map(ColumnInfo::getName)
                .findFirst()
                .orElse("id");
    }

    private int resolveLimit(String first, String last) {
        String raw = (first != null) ? first : last;
        if (raw == null) return 100;
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            return 100;
        }
    }

    /**
     * Return true if this SelectField represents an aggregate expression
     * (e.g. {@code count()}, {@code total.sum()}).
     */
    static boolean isAggregate(SelectField sf) {
        if (sf.isWildcard() || sf.isEmbedded()) return false;
        String name = sf.getName();
        if ("count()".equals(name)) return true;
        for (String agg : AGGREGATE_NAMES) {
            if (name.endsWith("." + agg + "()")) return true;
        }
        return false;
    }

    /**
     * Strip control parameters (pagination, ordering, etc.) from filters so only
     * column filters are passed to FilterService.
     */
    private static final Set<String> CONTROL_PARAMS = Set.of(
            "offset", "limit", "orderBy", "orderDirection", "select", "expand",
            "first", "after", "last", "before", "order"
    );

    private MultiValueMap<String, String> stripControlParams(MultiValueMap<String, String> filters) {
        org.springframework.util.LinkedMultiValueMap<String, String> result =
                new org.springframework.util.LinkedMultiValueMap<>();
        for (Map.Entry<String, List<String>> entry : filters.entrySet()) {
            if (!CONTROL_PARAMS.contains(entry.getKey())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    /**
     * Check if the raw select string contains any aggregate function syntax.
     */
    static boolean containsAggregateExpression(String select) {
        if (select == null || select.isBlank()) return false;
        if (select.contains("count()")) return true;
        for (String agg : AGGREGATE_NAMES) {
            if (select.contains("." + agg + "()")) return true;
        }
        return false;
    }

    /**
     * Parse aggregate select string directly (bypassing SelectParserService which
     * doesn't understand aggregate syntax like {@code total.sum()} or {@code count()}).
     */
    static List<SelectField> parseAggregateFields(String select) {
        if (select == null || select.isBlank()) {
            return List.of(new SelectField("count()"));
        }
        List<SelectField> fields = new ArrayList<>();
        for (String token : select.split(",")) {
            token = token.trim();
            if (token.isEmpty()) continue;
            fields.add(new SelectField(token));
        }
        return fields.isEmpty() ? List.of(new SelectField("count()")) : fields;
    }

}
