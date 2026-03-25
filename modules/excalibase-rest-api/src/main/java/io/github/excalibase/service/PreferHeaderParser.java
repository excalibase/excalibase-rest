package io.github.excalibase.service;

import org.springframework.stereotype.Component;

/**
 * Parses the Prefer request header (RFC 7240).
 *
 * Prefer header format:
 *   Prefer: return=representation, count=exact
 *
 * Supported directives:
 *   return=representation  — include full body in response (default)
 *   return=headers-only    — 201/200 with Location header, empty body
 *   return=minimal         — 204 No Content
 *   count=exact            — include exact total count in Content-Range header
 *   count=planned          — estimated count (treated same as exact here)
 *   count=estimated        — estimated count (treated same as exact here)
 *   resolution=merge-duplicates — upsert on conflict
 */
@Component
public class PreferHeaderParser {

    public static final String RETURN_REPRESENTATION = "representation";
    public static final String RETURN_HEADERS_ONLY = "headers-only";
    public static final String RETURN_MINIMAL = "minimal";

    /**
     * Extract the return preference. Defaults to "representation" if not specified.
     */
    public String getReturn(String preferHeader) {
        if (preferHeader == null || preferHeader.isBlank()) return RETURN_REPRESENTATION;
        for (String token : preferHeader.split(",")) {
            String trimmed = token.trim();
            if (trimmed.startsWith("return=")) {
                return trimmed.substring(7).trim();
            }
        }
        return RETURN_REPRESENTATION;
    }

    /**
     * Extract the count preference (exact, planned, estimated), or null if not specified.
     */
    public String getCount(String preferHeader) {
        if (preferHeader == null || preferHeader.isBlank()) return null;
        for (String token : preferHeader.split(",")) {
            String trimmed = token.trim();
            if (trimmed.startsWith("count=")) {
                return trimmed.substring(6).trim();
            }
        }
        return null;
    }

    /**
     * Check if upsert resolution is requested.
     */
    public boolean isUpsert(String preferHeader) {
        if (preferHeader == null || preferHeader.isBlank()) return false;
        for (String token : preferHeader.split(",")) {
            if ("resolution=merge-duplicates".equals(token.trim())) return true;
        }
        return false;
    }

    /**
     * Check if tx=rollback is requested (dry-run).
     */
    public boolean isTxRollback(String preferHeader) {
        if (preferHeader == null || preferHeader.isBlank()) return false;
        for (String token : preferHeader.split(",")) {
            if ("tx=rollback".equals(token.trim())) return true;
        }
        return false;
    }
}
