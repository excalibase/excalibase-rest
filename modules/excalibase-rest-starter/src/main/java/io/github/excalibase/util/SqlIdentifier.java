package io.github.excalibase.util;

import java.util.regex.Pattern;

/**
 * Utility for safely quoting SQL identifiers (table names, column names).
 * Validates against a whitelist regex before wrapping in double-quotes.
 */
public final class SqlIdentifier {

    private static final Pattern SAFE_IDENTIFIER =
            Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_ ]*$");

    private SqlIdentifier() {
    }

    /**
     * Quote a SQL identifier by wrapping in double-quotes after validating its format.
     * Rejects identifiers containing characters that could enable SQL injection.
     *
     * @param identifier the table or column name to quote
     * @return the double-quoted identifier, e.g. {@code "my_column"}
     * @throws IllegalArgumentException if the identifier contains unsafe characters
     */
    public static String quoteIdentifier(String identifier) {
        if (identifier == null || !SAFE_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier: " + identifier);
        }
        return "\"" + identifier + "\"";
    }
}
