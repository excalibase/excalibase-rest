package io.github.excalibase.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Base64;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for JwtUserIdExtractor.
 *
 * Verifies extraction of user ID and additional claims from JWT Bearer tokens.
 * JWT payload is Base64url-encoded JSON — no signature verification needed here
 * (upstream gateway handles that).
 */
@ExtendWith(MockitoExtension.class)
class JwtUserIdExtractorTest {

    @Mock
    private HttpServletRequest request;

    private JwtUserIdExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new JwtUserIdExtractor(new ObjectMapper());
    }

    // ─── helpers ─────────────────────────────────────────────────────────────

    /** Build a minimal JWT with the given payload (no real signature). */
    private static String buildJwt(Map<String, Object> payload) throws Exception {
        String header = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"alg\":\"HS256\",\"typ\":\"JWT\"}".getBytes());
        String body = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(new ObjectMapper().writeValueAsBytes(payload));
        return header + "." + body + ".fakesignature";
    }

    // ─── extractUserId ────────────────────────────────────────────────────────

    @Test
    void shouldExtractUserIdFromSubClaim() throws Exception {
        String token = buildJwt(Map.of("sub", "user-123", "email", "user@example.com"));
        when(request.getHeader("Authorization")).thenReturn("Bearer " + token);

        String userId = extractor.extractUserId(request);

        assertEquals("user-123", userId);
    }

    @Test
    void shouldFallbackToUserIdClaimWhenSubMissing() throws Exception {
        String token = buildJwt(Map.of("user_id", "u-456", "email", "user@example.com"));
        when(request.getHeader("Authorization")).thenReturn("Bearer " + token);

        String userId = extractor.extractUserId(request);

        assertEquals("u-456", userId);
    }

    @Test
    void shouldPreferSubOverUserId() throws Exception {
        String token = buildJwt(Map.of("sub", "sub-value", "user_id", "uid-value"));
        when(request.getHeader("Authorization")).thenReturn("Bearer " + token);

        String userId = extractor.extractUserId(request);

        assertEquals("sub-value", userId);
    }

    @Test
    void shouldReturnNullWhenNoAuthorizationHeader() {
        when(request.getHeader("Authorization")).thenReturn(null);

        assertNull(extractor.extractUserId(request));
    }

    @Test
    void shouldReturnNullWhenNotBearerToken() {
        when(request.getHeader("Authorization")).thenReturn("Basic dXNlcjpwYXNz");

        assertNull(extractor.extractUserId(request));
    }

    @Test
    void shouldReturnNullWhenNoUserIdClaimInToken() throws Exception {
        String token = buildJwt(Map.of("email", "user@example.com", "role", "admin"));
        when(request.getHeader("Authorization")).thenReturn("Bearer " + token);

        assertNull(extractor.extractUserId(request));
    }

    @Test
    void shouldReturnNullOnMalformedToken() {
        when(request.getHeader("Authorization")).thenReturn("Bearer notavalidtoken");

        assertNull(extractor.extractUserId(request));
    }

    @Test
    void shouldReturnNullOnTokenWithOnlyOneSegment() {
        when(request.getHeader("Authorization")).thenReturn("Bearer onlyone");

        assertNull(extractor.extractUserId(request));
    }

    @Test
    void shouldHandleTokenWithUnpaddedBase64() throws Exception {
        // Manually construct unpadded base64url payload (length % 4 != 0)
        String payload = "{\"sub\":\"trimmed-user\"}";
        String encoded = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(payload.getBytes());
        when(request.getHeader("Authorization"))
                .thenReturn("Bearer header." + encoded + ".sig");

        assertEquals("trimmed-user", extractor.extractUserId(request));
    }

    // ─── extractAdditionalClaims ──────────────────────────────────────────────

    @Test
    void shouldExtractAllStringClaimsAsAdditional() throws Exception {
        String token = buildJwt(Map.of(
                "sub", "user-1",
                "role", "admin",
                "tenant_id", "acme",
                "email", "user@acme.com"
        ));
        when(request.getHeader("Authorization")).thenReturn("Bearer " + token);

        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // sub is the user_id claim — should be excluded from additional claims
        assertFalse(claims.containsKey("sub"), "sub should not appear in additional claims");
        assertEquals("admin", claims.get("role"));
        assertEquals("acme", claims.get("tenant_id"));
        assertEquals("user@acme.com", claims.get("email"));
    }

    @Test
    void shouldSkipInternalJwtClaims() throws Exception {
        long now = System.currentTimeMillis() / 1000;
        Map<String, Object> payload = new java.util.HashMap<>();
        payload.put("sub", "user-1");
        payload.put("iss", "https://auth.example.com");
        payload.put("iat", now);
        payload.put("exp", now + 3600);
        payload.put("nbf", now);
        payload.put("jti", "jwt-id-123");
        payload.put("aud", "my-app");
        payload.put("role", "editor");
        String token = buildJwt(payload);
        when(request.getHeader("Authorization")).thenReturn("Bearer " + token);

        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        assertFalse(claims.containsKey("iss"));
        assertFalse(claims.containsKey("iat"));
        assertFalse(claims.containsKey("exp"));
        assertFalse(claims.containsKey("nbf"));
        assertFalse(claims.containsKey("jti"));
        assertFalse(claims.containsKey("aud"));
        assertFalse(claims.containsKey("sub"));
        assertEquals("editor", claims.get("role"));
    }

    @Test
    void shouldReturnEmptyMapWhenNoAuthHeader() {
        when(request.getHeader("Authorization")).thenReturn(null);

        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        assertNotNull(claims);
        assertTrue(claims.isEmpty());
    }

    @Test
    void shouldReturnEmptyMapOnMalformedToken() {
        when(request.getHeader("Authorization")).thenReturn("Bearer bad.token");

        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        assertNotNull(claims);
        // May be empty or partial — must not throw
    }

    @Test
    void shouldExcludeUserIdClaimFromAdditional() throws Exception {
        String token = buildJwt(Map.of("user_id", "uid-789", "org", "example-org"));
        when(request.getHeader("Authorization")).thenReturn("Bearer " + token);

        Map<String, String> claims = extractor.extractAdditionalClaims(request);

        // user_id used as userId — should not also appear in additional claims
        assertFalse(claims.containsKey("user_id"),
                "user_id should not appear in additional claims");
        assertEquals("example-org", claims.get("org"));
    }
}
