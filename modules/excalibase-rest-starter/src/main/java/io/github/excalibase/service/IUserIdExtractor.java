package io.github.excalibase.service;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;

/**
 * Interface for extracting user ID and claims from HTTP requests.
 * Allows pluggable strategies: headers, JWT tokens, OAuth2, session cookies, etc.
 *
 * Implementations should be annotated with @ExcalibaseService(serviceName = "type")
 * where type matches the configured extractor type (e.g., "header", "jwt", "oauth2").
 */
public interface IUserIdExtractor {

    /**
     * Extract user ID from the HTTP request.
     *
     * @param request the HTTP request
     * @return user ID string, or null if not found/not authenticated
     */
    String extractUserId(HttpServletRequest request);

    /**
     * Extract additional claims/attributes from the request.
     * These are typically used for multi-tenant isolation or fine-grained permissions.
     *
     * Common claims:
     * - tenant_id: Multi-tenant isolation
     * - organization_id: Organization-level access
     * - department_id: Department-level access
     * - role: User role for RBAC
     *
     * @param request the HTTP request
     * @return map of claim name to value, never null (empty map if no claims)
     */
    Map<String, String> extractAdditionalClaims(HttpServletRequest request);
}
