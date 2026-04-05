package io.github.excalibase.security;

import io.github.excalibase.service.IUserIdExtractor;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "app.security.jwt-enabled", havingValue = "true")
public class JwtUserIdExtractor implements IUserIdExtractor {

    @org.springframework.beans.factory.annotation.Value("${app.security.user-id-header:X-User-Id}")
    private String fallbackHeader;

    @Override
    public String extractUserId(HttpServletRequest request) {
        // Prefer JWT claims
        JwtClaims claims = (JwtClaims) request.getAttribute(JwtAuthFilter.JWT_CLAIMS_ATTR);
        if (claims != null) {
            return String.valueOf(claims.userId());
        }
        // Fallback to X-User-Id header (backward compatibility)
        return request.getHeader(fallbackHeader);
    }

    @Override
    public Map<String, String> extractAdditionalClaims(HttpServletRequest request) {
        JwtClaims claims = (JwtClaims) request.getAttribute(JwtAuthFilter.JWT_CLAIMS_ATTR);
        if (claims == null) {
            return Map.of();
        }
        Map<String, String> additional = new HashMap<>();
        additional.put("project_id", claims.projectId());
        if (claims.role() != null) {
            additional.put("role", claims.role());
        }
        if (claims.email() != null) {
            additional.put("email", claims.email());
        }
        return additional;
    }
}
