package io.github.excalibase.security;

import io.github.excalibase.service.IUserIdExtractor;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConditionalOnProperty(name = "app.security.jwt-enabled", havingValue = "false", matchIfMissing = true)
public class HeaderUserIdExtractor implements IUserIdExtractor {

    private final String headerName;

    public HeaderUserIdExtractor(
            @Value("${app.security.user-id-header:X-User-Id}") String headerName) {
        this.headerName = headerName;
    }

    @Override
    public String extractUserId(HttpServletRequest request) {
        return request.getHeader(headerName);
    }

    @Override
    public Map<String, String> extractAdditionalClaims(HttpServletRequest request) {
        return Map.of();
    }
}
