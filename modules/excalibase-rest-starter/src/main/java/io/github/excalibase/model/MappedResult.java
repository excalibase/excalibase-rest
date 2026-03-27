package io.github.excalibase.model;

import java.util.List;
import java.util.Map;

/**
 * Immutable result from ResultMapper containing both records and total count.
 * Replaces the previous mutable {@code lastTotalCount} field pattern which was not thread-safe.
 *
 * @param records    the parsed records from the query result
 * @param totalCount the total count from the query, or {@code -1} if not requested
 */
public record MappedResult(List<Map<String, Object>> records, long totalCount) {

    private static final MappedResult EMPTY = new MappedResult(List.of(), -1);

    public static MappedResult empty() {
        return EMPTY;
    }
}
