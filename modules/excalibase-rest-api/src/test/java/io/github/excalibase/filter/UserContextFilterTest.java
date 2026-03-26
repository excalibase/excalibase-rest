package io.github.excalibase.filter;

import io.github.excalibase.config.RestApiConfig;
import io.github.excalibase.service.IUserContextService;
import io.github.excalibase.service.IUserIdExtractor;
import io.github.excalibase.service.ServiceLookup;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for UserContextFilter covering disabled/enabled paths,
 * user ID extraction, claim propagation, context cleanup, and error handling.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class UserContextFilterTest {

    @Mock private ServiceLookup serviceLookup;
    @Mock private RestApiConfig appConfig;
    @Mock private RestApiConfig.SecurityConfig securityConfig;
    @Mock private IUserIdExtractor userIdExtractor;
    @Mock private IUserContextService userContextService;
    @Mock private FilterChain filterChain;
    @Mock private HttpServletRequest httpRequest;
    @Mock private HttpServletResponse httpResponse;
    @Mock private FilterConfig filterConfig;

    private UserContextFilter filter;

    @BeforeEach
    void setUp() {
        when(appConfig.getSecurity()).thenReturn(securityConfig);
        filter = new UserContextFilter(serviceLookup, appConfig);
    }

    // ==================== init ====================

    @Test
    void init_whenDisabled_skipsServiceLookup() throws ServletException {
        when(securityConfig.isUserContextEnabled()).thenReturn(false);

        filter.init(filterConfig);

        verifyNoInteractions(serviceLookup);
    }

    @Test
    void init_whenEnabled_loadsExtractorAndContextService() throws ServletException {
        when(securityConfig.isUserContextEnabled()).thenReturn(true);
        when(securityConfig.getUserIdExtractorType()).thenReturn("header");
        when(appConfig.getDatabaseType()).thenReturn("postgres");
        when(serviceLookup.forBean(IUserIdExtractor.class, "header")).thenReturn(userIdExtractor);
        when(serviceLookup.forBean(IUserContextService.class, "postgres")).thenReturn(userContextService);

        filter.init(filterConfig);

        verify(serviceLookup).forBean(IUserIdExtractor.class, "header");
        verify(serviceLookup).forBean(IUserContextService.class, "postgres");
    }

    @Test
    void init_whenServiceLookupFails_throwsServletException() {
        when(securityConfig.isUserContextEnabled()).thenReturn(true);
        when(securityConfig.getUserIdExtractorType()).thenReturn("jwt");
        // Do NOT stub getDatabaseType — the exception is thrown before it's reached
        when(serviceLookup.forBean(eq(IUserIdExtractor.class), any()))
            .thenThrow(new RuntimeException("bean not found"));

        assertThatThrownBy(() -> filter.init(filterConfig))
            .isInstanceOf(ServletException.class)
            .hasMessageContaining("User context filter initialization failed");
    }

    // ==================== doFilter — disabled path ====================

    @Test
    void doFilter_whenDisabled_skipsContextSetup() throws IOException, ServletException {
        when(securityConfig.isUserContextEnabled()).thenReturn(false);

        filter.doFilter(httpRequest, httpResponse, filterChain);

        verify(filterChain).doFilter(httpRequest, httpResponse);
        verifyNoInteractions(userIdExtractor, userContextService);
    }

    @Test
    void doFilter_whenExtractorIsNull_skipsContextSetup() throws IOException, ServletException {
        // Enable filter but do NOT call init so extractor stays null
        when(securityConfig.isUserContextEnabled()).thenReturn(true);

        filter.doFilter(httpRequest, httpResponse, filterChain);

        verify(filterChain).doFilter(httpRequest, httpResponse);
        verifyNoInteractions(userContextService);
    }

    // ==================== doFilter — enabled path ====================

    @Test
    void doFilter_withValidUserId_setsContextAndClearsAfter() throws Exception {
        initFilterEnabled();

        when(userIdExtractor.extractUserId(httpRequest)).thenReturn("user-42");
        when(userIdExtractor.extractAdditionalClaims(httpRequest))
            .thenReturn(Map.of("tenant_id", "acme"));

        filter.doFilter(httpRequest, httpResponse, filterChain);

        verify(userContextService).setUserContext(eq("user-42"), eq(Map.of("tenant_id", "acme")));
        verify(filterChain).doFilter(httpRequest, httpResponse);
        verify(userContextService).clearUserContext();
    }

    @Test
    void doFilter_noUserId_skipsSetContextButStillClearsAfter() throws Exception {
        initFilterEnabled();

        when(userIdExtractor.extractUserId(httpRequest)).thenReturn(null);

        filter.doFilter(httpRequest, httpResponse, filterChain);

        verify(userContextService, never()).setUserContext(any(), any());
        verify(filterChain).doFilter(httpRequest, httpResponse);
        verify(userContextService).clearUserContext();
    }

    @Test
    void doFilter_blankUserId_skipsSetContextButStillClearsAfter() throws Exception {
        initFilterEnabled();

        when(userIdExtractor.extractUserId(httpRequest)).thenReturn("   ");

        filter.doFilter(httpRequest, httpResponse, filterChain);

        verify(userContextService, never()).setUserContext(any(), any());
        verify(userContextService).clearUserContext();
    }

    @Test
    void doFilter_filterChainThrows_contextStillCleared() throws Exception {
        initFilterEnabled();

        when(userIdExtractor.extractUserId(httpRequest)).thenReturn("user-1");
        when(userIdExtractor.extractAdditionalClaims(httpRequest)).thenReturn(Map.of());
        doThrow(new ServletException("chain error")).when(filterChain).doFilter(any(), any());

        assertThatThrownBy(() -> filter.doFilter(httpRequest, httpResponse, filterChain))
            .isInstanceOf(ServletException.class);

        // Context must still be cleared in finally block
        verify(userContextService).clearUserContext();
    }

    // ==================== destroy ====================

    @Test
    void destroy_doesNotThrow() {
        filter.destroy();
        // No exception — just verifying it runs cleanly
    }

    // ==================== Helper ====================

    private void initFilterEnabled() throws ServletException {
        when(securityConfig.isUserContextEnabled()).thenReturn(true);
        when(securityConfig.getUserIdExtractorType()).thenReturn("header");
        when(appConfig.getDatabaseType()).thenReturn("postgres");
        when(serviceLookup.forBean(IUserIdExtractor.class, "header")).thenReturn(userIdExtractor);
        when(serviceLookup.forBean(IUserContextService.class, "postgres")).thenReturn(userContextService);
        filter.init(filterConfig);
    }
}
