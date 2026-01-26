package io.github.excalibase.filter;

import io.github.excalibase.config.RestApiConfig;
import io.github.excalibase.service.IUserContextService;
import io.github.excalibase.service.IUserIdExtractor;
import io.github.excalibase.service.ServiceLookup;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

/**
 * Filter that sets user context for RLS (Row Level Security) policies.
 *
 * This filter runs before all requests and:
 * 1. Uses pluggable IUserIdExtractor to extract user ID and claims
 * 2. Uses database-specific IUserContextService to set session variables
 * 3. Clears context after request completes (prevents connection pool leakage)
 *
 * Configuration:
 * <pre>
 * app:
 *   security:
 *     user-context-enabled: true
 *     user-id-extractor-type: header  # "header", "jwt", "oauth2", etc.
 *     user-id-header: X-User-Id
 * </pre>
 *
 * Example request:
 * <pre>
 * POST /api/v1/orders
 * X-User-Id: user-123
 * X-Claim-tenant_id: acme-corp
 * X-Claim-department_id: engineering
 * </pre>
 *
 * RLS policy example:
 * <pre>
 * CREATE POLICY user_isolation ON orders
 * FOR SELECT TO PUBLIC
 * USING (user_id = current_setting('request.user_id'));
 * </pre>
 */
@Component
@Order(1) // Run early in filter chain, before Spring Security
public class UserContextFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(UserContextFilter.class);

    private final ServiceLookup serviceLookup;
    private final RestApiConfig appConfig;

    private IUserIdExtractor userIdExtractor;
    private IUserContextService userContextService;

    public UserContextFilter(ServiceLookup serviceLookup, RestApiConfig appConfig) {
        this.serviceLookup = serviceLookup;
        this.appConfig = appConfig;
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Lazy initialize services based on configuration
        if (!appConfig.getSecurity().isUserContextEnabled()) {
            log.info("User context filter is disabled");
            return;
        }

        try {
            // Get pluggable user ID extractor by configured type
            String extractorType = appConfig.getSecurity().getUserIdExtractorType();
            this.userIdExtractor = serviceLookup.forBean(
                IUserIdExtractor.class,
                extractorType
            );
            log.info("Initialized user ID extractor: type={}", extractorType);

            // Get database-specific user context service
            String databaseType = appConfig.getDatabaseType().toLowerCase();
            this.userContextService = serviceLookup.forBean(
                IUserContextService.class,
                databaseType
            );
            log.info("Initialized user context service: database={}", databaseType);

        } catch (Exception e) {
            log.error("Failed to initialize user context filter: {}", e.getMessage());
            throw new ServletException("User context filter initialization failed", e);
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        if (!appConfig.getSecurity().isUserContextEnabled() ||
            userIdExtractor == null ||
            userContextService == null) {
            // User context disabled or not available - skip
            chain.doFilter(request, response);
            return;
        }

        HttpServletRequest httpRequest = (HttpServletRequest) request;

        try {
            // Extract user ID using pluggable extractor
            String userId = userIdExtractor.extractUserId(httpRequest);

            if (userId != null && !userId.trim().isEmpty()) {
                // Extract additional claims
                Map<String, String> additionalClaims = userIdExtractor.extractAdditionalClaims(httpRequest);

                // Set user context in database session
                userContextService.setUserContext(userId, additionalClaims);

                log.debug("Set user context: userId={}, claims={}", userId, additionalClaims.keySet());
            } else {
                log.debug("No user ID extracted - skipping user context");
            }

            // Continue filter chain
            chain.doFilter(request, response);

        } finally {
            // Always clear user context after request to prevent leakage in connection pools
            if (userContextService != null) {
                userContextService.clearUserContext();
            }
        }
    }

    @Override
    public void destroy() {
        // Cleanup if needed
        log.info("User context filter destroyed");
    }
}
