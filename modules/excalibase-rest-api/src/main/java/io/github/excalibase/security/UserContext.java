package io.github.excalibase.security;

import jakarta.servlet.http.HttpServletRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * Immutable user context extracted from an HTTP request.
 * Used to pass RLS identity (user ID + JWT claims) to the query execution layer.
 */
public record UserContext(String userId, Map<String, String> claims) {

    /**
     * Extract user context from the request, checking JWT claims first, then X-User-Id header.
     *
     * @param request the HTTP servlet request
     * @return UserContext if identity found, null otherwise
     */
    public static UserContext fromRequest(HttpServletRequest request) {
        // 1. Try JWT claims set by JwtAuthFilter
        JwtClaims jwt = (JwtClaims) request.getAttribute(JwtAuthFilter.JWT_CLAIMS_ATTR);
        if (jwt != null) {
            Map<String, String> claims = new HashMap<>();
            claims.put("project_id", jwt.projectId());
            if (jwt.role() != null) {
                claims.put("role", jwt.role());
            }
            if (jwt.email() != null) {
                claims.put("email", jwt.email());
            }
            return new UserContext(String.valueOf(jwt.userId()), Map.copyOf(claims));
        }

        // 2. Fallback to X-User-Id header (backward compatibility)
        String header = request.getHeader("X-User-Id");
        if (header != null && !header.isBlank()) {
            return new UserContext(header, Map.of());
        }

        return null;
    }
}
