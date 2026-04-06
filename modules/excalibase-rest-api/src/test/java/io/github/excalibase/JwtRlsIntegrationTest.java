package io.github.excalibase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import java.util.Date;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration test: JWT verification + PostgreSQL RLS enforcement via REST API.
 * Mock vault key server returns test EC P-256 public key.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class JwtRlsIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withInitScript("init-jwt-rls.sql");

    static ECPrivateKey privateKey;
    static ECPublicKey publicKey;
    static HttpServer mockVault;
    static int mockVaultPort;

    static {
        try {
            KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");
            gen.initialize(new ECGenParameterSpec("secp256r1"));
            KeyPair kp = gen.generateKeyPair();
            privateKey = (ECPrivateKey) kp.getPrivate();
            publicKey = (ECPublicKey) kp.getPublic();

            mockVault = HttpServer.create(new InetSocketAddress(0), 0);
            mockVaultPort = mockVault.getAddress().getPort();

            String pubPem = toPem(publicKey);
            String keyJson = new ObjectMapper().writeValueAsString(
                    Map.of("key", pubPem, "algorithm", "EC-P256"));

            mockVault.createContext("/vault/pki/public-key", exchange -> {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                byte[] body = keyJson.getBytes();
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.getResponseBody().close();
            });
            mockVault.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to set up mock vault", e);
        }
    }

    @AfterAll
    static void teardown() {
        if (mockVault != null) mockVault.stop(0);
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", () -> "app_user");
        registry.add("spring.datasource.password", () -> "apppass");
        registry.add("app.allowed-schema", () -> "public");
        registry.add("app.database-type", () -> "postgres");
        registry.add("app.security.jwt-enabled", () -> "true");
        registry.add("app.security.user-context-enabled", () -> "true");
        registry.add("app.security.provisioning-url", () -> "http://localhost:" + mockVaultPort);
    }

    @Autowired
    private MockMvc mockMvc;

    private String signJwt(long userId, String projectId, String role, String email) {
        return Jwts.builder()
                .subject(email)
                .claim("userId", userId)
                .claim("projectId", projectId)
                .claim("role", role)
                .issuer("excalibase")
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + 3600_000))
                .signWith(privateKey)
                .compact();
    }

    @Test
    @Order(1)
    void jwt_user42_getOrders() throws Exception {
        String jwt = signJwt(42, "test-project", "user", "alice@test.com");
        mockMvc.perform(get("/api/v1/orders")
                        .header("Authorization", "Bearer " + jwt))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data", hasSize(2)));
    }

    @Test
    @Order(2)
    void jwt_user99_getOrders() throws Exception {
        String jwt = signJwt(99, "test-project", "user", "bob@test.com");
        mockMvc.perform(get("/api/v1/orders")
                        .header("Authorization", "Bearer " + jwt))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data", hasSize(1)));
    }

    @Test
    @Order(3)
    void jwt_multiTable() throws Exception {
        String jwt = signJwt(42, "test-project", "user", "alice@test.com");
        // Both tables should be filtered by RLS for user 42
        mockMvc.perform(get("/api/v1/orders")
                        .header("Authorization", "Bearer " + jwt))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data", hasSize(2)));

        mockMvc.perform(get("/api/v1/payments")
                        .header("Authorization", "Bearer " + jwt))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data", hasSize(3)));
    }

    @Test
    @Order(4)
    void noJwt_passThrough() throws Exception {
        // No auth header -> filter passes through, RLS FORCE blocks all rows (empty result)
        mockMvc.perform(get("/api/v1/orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data", hasSize(0)));
    }

    @Test
    @Order(5)
    void invalidJwt_401() throws Exception {
        mockMvc.perform(get("/api/v1/orders")
                        .header("Authorization", "Bearer invalid.jwt.token"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @Order(7)
    void jwt_user42_multiTablePayments() throws Exception {
        String jwt = signJwt(42, "test-project", "user", "alice@test.com");
        mockMvc.perform(get("/api/v1/payments")
                        .header("Authorization", "Bearer " + jwt))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data", hasSize(3)));
    }

    private static String toPem(ECPublicKey key) {
        byte[] encoded = key.getEncoded();
        String base64 = Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(encoded);
        return "-----BEGIN PUBLIC KEY-----\n" + base64 + "\n-----END PUBLIC KEY-----\n";
    }
}
