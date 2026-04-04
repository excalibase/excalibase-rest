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

    @Override
    public String extractUserId(HttpServletRequest request) {
        JwtClaims claims = (JwtClaims) request.getAttribute(JwtAuthFilter.JWT_CLAIMS_ATTR);
        if (claims == null) {
            return null;
        }
        return String.valueOf(claims.userId());
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
