package io.github.excalibase.extractor;

import io.github.excalibase.config.RestApiConfig;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Collections;
import java.util.Map;
import java.util.Vector;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * Unit test for HeaderUserIdExtractor.
 *
 * Tests user ID and claims extraction from HTTP headers.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HeaderUserIdExtractorTest {

    @Mock
    private HttpServletRequest request;

    @Mock
    private RestApiConfig appConfig;

    @Mock
    private RestApiConfig.SecurityConfig securityConfig;

    private HeaderUserIdExtractor extractor;

    @BeforeEach
    void setUp() {
        lenient().when(appConfig.getSecurity()).thenReturn(securityConfig);
        lenient().when(securityConfig.getUserIdHeader()).thenReturn("X-User-Id");
        extractor = new HeaderUserIdExtractor(appConfig);
    }

    @Test
    void shouldExtractUserIdFromHeader() {
        // Given
        when(request.getHeader("X-User-Id")).thenReturn("user-123");

        // When
        String userId = extractor.extractUserId(request);

        // Then
        assertEquals("user-123", userId);
    }

    @Test
    void shouldTrimUserIdValue() {
        // Given
        when(request.getHeader("X-User-Id")).thenReturn("  user-456  ");

        // When
        String userId = extractor.extractUserId(request);

        // Then
        assertEquals("user-456", userId);
    }

    @Test
    void shouldReturnNullWhenHeaderMissing() {
        // Given
        when(request.getHeader("X-User-Id")).thenReturn(null);

        // When
        String userId = extractor.extractUserId(request);

        // Then
        assertNull(userId);
    }

    @Test
    void shouldReturnNullWhenHeaderEmpty() {
        // Given
        when(request.getHeader("X-User-Id")).thenReturn("");

        // When
        String userId = extractor.extractUserId(request);

        // Then
        assertNull(userId);
    }

    @Test
    void shouldReturnNullWhenHeaderOnlyWhitespace() {
        // Given
        when(request.getHeader("X-User-Id")).thenReturn("   ");

        // When
        String userId = extractor.extractUserId(request);

        // Then
        assertNull(userId);
    }

    @Test
    void shouldExtractAdditionalClaims() {
        // Given - mock header names including claim headers
        Vector<String> headerNames = new Vector<>();
        headerNames.add("X-User-Id");
        headerNames.add("X-Claim-tenant_id");
        headerNames.add("X-Claim-department_id");
        headerNames.add("Content-Type");
        when(request.getHeaderNames()).thenReturn(headerNames.elements());

        when(request.getHeader("X-Claim-tenant_id")).thenReturn("acme-corp");
        when(request.getHeader("X-Claim-department_id")).thenReturn("engineering");

        // When
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // Then
        assertEquals(2, claims.size());
        assertEquals("acme-corp", claims.get("tenant_id"));
        assertEquals("engineering", claims.get("department_id"));
    }

    @Test
    void shouldHandleCaseInsensitiveClaimHeaders() {
        // Given - mixed case headers
        Vector<String> headerNames = new Vector<>();
        headerNames.add("X-CLAIM-TENANT_ID");
        headerNames.add("x-claim-role");
        when(request.getHeaderNames()).thenReturn(headerNames.elements());

        when(request.getHeader("X-CLAIM-TENANT_ID")).thenReturn("test-tenant");
        when(request.getHeader("x-claim-role")).thenReturn("admin");

        // When
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // Then
        assertEquals(2, claims.size());
        assertEquals("test-tenant", claims.get("TENANT_ID"));
        assertEquals("admin", claims.get("role"));
    }

    @Test
    void shouldTrimClaimValues() {
        // Given
        Vector<String> headerNames = new Vector<>();
        headerNames.add("X-Claim-tenant_id");
        when(request.getHeaderNames()).thenReturn(headerNames.elements());

        when(request.getHeader("X-Claim-tenant_id")).thenReturn("  tenant-value  ");

        // When
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // Then
        assertEquals("tenant-value", claims.get("tenant_id"));
    }

    @Test
    void shouldSkipNullClaimValues() {
        // Given
        Vector<String> headerNames = new Vector<>();
        headerNames.add("X-Claim-valid");
        headerNames.add("X-Claim-null");
        when(request.getHeaderNames()).thenReturn(headerNames.elements());

        when(request.getHeader("X-Claim-valid")).thenReturn("valid-value");
        when(request.getHeader("X-Claim-null")).thenReturn(null);

        // When
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // Then
        assertEquals(1, claims.size());
        assertEquals("valid-value", claims.get("valid"));
        assertFalse(claims.containsKey("null"));
    }

    @Test
    void shouldSkipEmptyClaimValues() {
        // Given
        Vector<String> headerNames = new Vector<>();
        headerNames.add("X-Claim-valid");
        headerNames.add("X-Claim-empty");
        headerNames.add("X-Claim-whitespace");
        when(request.getHeaderNames()).thenReturn(headerNames.elements());

        when(request.getHeader("X-Claim-valid")).thenReturn("valid-value");
        when(request.getHeader("X-Claim-empty")).thenReturn("");
        when(request.getHeader("X-Claim-whitespace")).thenReturn("   ");

        // When
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // Then
        assertEquals(1, claims.size());
        assertEquals("valid-value", claims.get("valid"));
        assertFalse(claims.containsKey("empty"));
        assertFalse(claims.containsKey("whitespace"));
    }

    @Test
    void shouldIgnoreNonClaimHeaders() {
        // Given
        Vector<String> headerNames = new Vector<>();
        headerNames.add("X-User-Id");
        headerNames.add("Content-Type");
        headerNames.add("Authorization");
        headerNames.add("X-Api-Key");
        when(request.getHeaderNames()).thenReturn(headerNames.elements());

        // When
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // Then
        assertTrue(claims.isEmpty());
    }

    @Test
    void shouldReturnEmptyMapWhenNoHeaderNames() {
        // Given
        when(request.getHeaderNames()).thenReturn(null);

        // When
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // Then
        assertNotNull(claims);
        assertTrue(claims.isEmpty());
    }

    @Test
    void shouldReturnEmptyMapWhenNoClaimHeaders() {
        // Given
        Vector<String> headerNames = new Vector<>();
        headerNames.add("X-User-Id");
        headerNames.add("Content-Type");
        when(request.getHeaderNames()).thenReturn(headerNames.elements());

        // When
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // Then
        assertNotNull(claims);
        assertTrue(claims.isEmpty());
    }

    @Test
    void shouldHandleMultipleClaimsCorrectly() {
        // Given
        Vector<String> headerNames = new Vector<>();
        headerNames.add("X-Claim-tenant_id");
        headerNames.add("X-Claim-department_id");
        headerNames.add("X-Claim-role");
        headerNames.add("X-Claim-organization_id");
        when(request.getHeaderNames()).thenReturn(headerNames.elements());

        when(request.getHeader("X-Claim-tenant_id")).thenReturn("tenant-1");
        when(request.getHeader("X-Claim-department_id")).thenReturn("dept-2");
        when(request.getHeader("X-Claim-role")).thenReturn("admin");
        when(request.getHeader("X-Claim-organization_id")).thenReturn("org-3");

        // When
        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // Then
        assertEquals(4, claims.size());
        assertEquals("tenant-1", claims.get("tenant_id"));
        assertEquals("dept-2", claims.get("department_id"));
        assertEquals("admin", claims.get("role"));
        assertEquals("org-3", claims.get("organization_id"));
    }

    @Test
    void shouldUseConfiguredUserIdHeader() {
        // Given - custom header name
        when(securityConfig.getUserIdHeader()).thenReturn("X-Custom-User-Header");
        extractor = new HeaderUserIdExtractor(appConfig);

        when(request.getHeader("X-Custom-User-Header")).thenReturn("custom-user-123");

        // When
        String userId = extractor.extractUserId(request);

        // Then
        assertEquals("custom-user-123", userId);
    }
}
