package io.github.excalibase.compiler;

import io.github.excalibase.model.TableInfo;
import org.springframework.util.MultiValueMap;

/**
 * Compiles all read-query requirements (data, count, relationships) into a single SQL statement.
 *
 * <p>Implementations are database-specific (e.g. {@code PostgresQueryCompiler}) and reside
 * in the database-specific module.  The controller and tests program to this interface so
 * that the postgres module can be swapped for a mysql module without touching the API layer.
 */
public interface IQueryCompiler {

    /**
     * Compile a list/collection query into a single SQL statement.
     *
     * @param table          target table name
     * @param info           pre-resolved table metadata (columns, FKs)
     * @param filters        non-control query parameters (column filters)
     * @param select         select fields string (nullable → SELECT *)
     * @param expand         comma-separated relationship names to expand (nullable)
     * @param orderBy        column to sort by (nullable)
     * @param orderDirection "asc" or "desc"
     * @param offset         pagination offset
     * @param limit          page size
     * @param includeCount   true → include COUNT(*) OVER() window in SELECT
     * @return compiled query ready for JdbcTemplate execution
     */
    CompiledQuery compile(String table, TableInfo info, MultiValueMap<String, String> filters,
                          String select, String expand, String orderBy, String orderDirection,
                          int offset, int limit, boolean includeCount);

    /**
     * Compile a cursor-based (keyset) pagination query.
     *
     * @param first  number of records to return (forward pagination)
     * @param after  base64-encoded cursor value for forward pagination
     * @param last   number of records to return (backward pagination)
     * @param before base64-encoded cursor value for backward pagination
     */
    CompiledQuery compileCursor(String table, TableInfo info, MultiValueMap<String, String> filters,
                                String select, String expand, String orderBy, String orderDirection,
                                String first, String after, String last, String before);
}
