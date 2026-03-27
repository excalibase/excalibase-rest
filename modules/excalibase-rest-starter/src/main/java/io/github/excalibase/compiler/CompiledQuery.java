package io.github.excalibase.compiler;

import java.util.List;

/**
 * Represents a compiled SQL query with all parameters ready for execution.
 * Immutable value type carrying the SQL string, bind parameters, and metadata
 * needed by ResultMapper to post-process the JDBC result set.
 */
public record CompiledQuery(
        String sql,
        Object[] params,
        boolean hasCountWindow,
        List<String> jsonColumns
) {
    public CompiledQuery {
        params = params == null ? new Object[0] : params;
        jsonColumns = jsonColumns == null ? List.of() : List.copyOf(jsonColumns);
    }
}
