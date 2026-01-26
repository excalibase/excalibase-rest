package io.github.excalibase.extractor;

import io.github.excalibase.annotation.ExcalibaseService;
import io.github.excalibase.config.RestApiConfig;
import io.github.excalibase.service.IUserIdExtractor;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Default implementation that extracts user ID from HTTP headers.
 * Suitable for API Gateway scenarios where authentication is handled upstream.
 *
 * Configuration:
 * - app.security.user-id-header: Header name for user ID (default: X-User-Id)
 * - Additional claims via X-Claim-* headers (e.g., X-Claim-tenant_id)
 *
 * Example request:
 * <pre>
 * GET /api/v1/orders
 * X-User-Id: user-123
 * X-Claim-tenant_id: acme-corp
 * X-Claim-department_id: engineering
 * </pre>
 */
@Service
@ExcalibaseService(serviceName = "header")
public class HeaderUserIdExtractor implements IUserIdExtractor {
    private static final Logger log = LoggerFactory.getLogger(HeaderUserIdExtractor.class);
    private static final String CLAIM_HEADER_PREFIX = "x-claim-";

    private final RestApiConfig appConfig;

    public HeaderUserIdExtractor(RestApiConfig appConfig) {
        this.appConfig = appConfig;
    }

    @Override
    public String extractUserId(HttpServletRequest request) {
        String userIdHeader = appConfig.getSecurity().getUserIdHeader();
        String userId = request.getHeader(userIdHeader);

        if (userId != null && !userId.trim().isEmpty()) {
            log.debug("Extracted user ID from header '{}': {}", userIdHeader, userId);
            return userId.trim();
        }

        log.debug("No user ID found in header '{}'", userIdHeader);
        return null;
    }

    @Override
    public Map<String, String> extractAdditionalClaims(HttpServletRequest request) {
        Map<String, String> claims = new HashMap<>();

        var headerNames = request.getHeaderNames();
        if (headerNames == null) {
            return claims;
        }

        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();

            // Extract claims from X-Claim-* headers (case-insensitive)
            if (headerName.toLowerCase().startsWith(CLAIM_HEADER_PREFIX)) {
                String claimName = headerName.substring(CLAIM_HEADER_PREFIX.length());
                String claimValue = request.getHeader(headerName);

                if (claimValue != null && !claimValue.trim().isEmpty()) {
                    claims.put(claimName, claimValue.trim());
                    log.debug("Extracted claim: {} = {}", claimName, claimValue);
                }
            }
        }

        return claims;
    }
}
