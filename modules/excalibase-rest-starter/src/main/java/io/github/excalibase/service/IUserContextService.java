package io.github.excalibase.service;

import java.util.Map;

/**
 * Service for setting user context in database sessions.
 * Enables Row Level Security (RLS) policies to access authenticated user information.
 *
 * Implementations should be database-specific and annotated with @ExcalibaseService
 * to match the database type (e.g., "postgres", "mysql").
 *
 * Thread Safety: Implementations must be thread-safe as they're shared across requests.
 * Connection Safety: Must work correctly with connection pooling (use transaction-scoped variables).
 */
public interface IUserContextService {

    /**
     * Sets user context in the database session.
     * This allows RLS policies to access the authenticated user's information.
     *
     * Example PostgreSQL RLS policy:
     * <pre>
     * CREATE POLICY user_isolation ON orders
     * FOR SELECT TO PUBLIC
     * USING (user_id = current_setting('request.user_id'));
     * </pre>
     *
     * Thread Safety: This method will be called from filter threads.
     * Connection Pooling: Use transaction-scoped variables (e.g., SET LOCAL in PostgreSQL).
     *
     * @param userId the authenticated user ID
     * @param additionalClaims optional claims like tenant_id, organization_id, role
     */
    void setUserContext(String userId, Map<String, String> additionalClaims);

    /**
     * Clears user context from the database session.
     * MUST be called after each request to prevent context leakage in connection pools.
     *
     * This is typically called in a finally block to ensure cleanup even on errors.
     */
    void clearUserContext();
}
