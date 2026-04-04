package io.github.excalibase.security;

import io.github.excalibase.service.IUserContextService;
import io.github.excalibase.service.IUserIdExtractor;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "app.security.user-context-enabled", havingValue = "true")
public class UserContextFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(UserContextFilter.class);

    private final IUserIdExtractor extractor;
    private final IUserContextService userContextService;

    public UserContextFilter(IUserIdExtractor extractor, IUserContextService userContextService) {
        this.extractor = extractor;
        this.userContextService = userContextService;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain chain) throws ServletException, IOException {
        String userId = extractor.extractUserId(request);
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        if (userId != null) {
            userContextService.setUserContext(userId, claims);
            log.debug("Set user context: userId={}", userId);
        }

        try {
            chain.doFilter(request, response);
        } finally {
            userContextService.clearUserContext();
        }
    }
}
