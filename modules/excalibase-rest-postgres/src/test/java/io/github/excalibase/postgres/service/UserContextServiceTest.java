package io.github.excalibase.postgres.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for UserContextService - PostgreSQL RLS support.
 *
 * Tests session variable setting using SET LOCAL for RLS policies.
 * Following user guidance: "for unit database test u just need to setup set user_id
 * no need to complex mock filter"
 */
@Testcontainers
class UserContextServiceTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    private JdbcTemplate jdbcTemplate;
    private UserContextService userContextService;
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void setUp() {
        // Create datasource and JdbcTemplate from test container
        DataSource dataSource = new DriverManagerDataSource(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword()
        );
        jdbcTemplate = new JdbcTemplate(dataSource);
        userContextService = new UserContextService(jdbcTemplate);

        // Create transaction template for testing SET LOCAL (which requires transaction)
        PlatformTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Test
    void shouldSetUserIdInSessionVariable() {
        // Given
        String userId = "user-123";

        // When & Then - operations must be in same transaction
        transactionTemplate.execute(status -> {
            userContextService.setUserContext(userId, null);

            // Verify variable is set within same transaction
            String actualUserId = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.user_id', true)",
                    String.class
            );
            assertEquals(userId, actualUserId);
            return null;
        });
    }

    @Test
    void shouldSetUserIdWithAdditionalClaims() {
        // Given
        String userId = "user-456";
        Map<String, String> claims = new HashMap<>();
        claims.put("tenant_id", "acme-corp");
        claims.put("department_id", "engineering");

        // When & Then - operations must be in same transaction
        transactionTemplate.execute(status -> {
            userContextService.setUserContext(userId, claims);

            // Verify user_id
            String actualUserId = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.user_id', true)",
                    String.class
            );
            assertEquals(userId, actualUserId);

            // Verify claims
            String tenantId = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.jwt.tenant_id', true)",
                    String.class
            );
            assertEquals("acme-corp", tenantId);

            String departmentId = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.jwt.department_id', true)",
                    String.class
            );
            assertEquals("engineering", departmentId);
            return null;
        });
    }

    @Test
    void shouldClearUserContext() {
        // Given - set user context
        String userId = "user-789";
        Map<String, String> claims = Map.of("tenant_id", "test-tenant");
        userContextService.setUserContext(userId, claims);

        // When - clear context
        userContextService.clearUserContext();

        // Then - verify variables are cleared
        String clearedUserId = jdbcTemplate.queryForObject(
                "SELECT current_setting('request.user_id', true)",
                String.class
        );
        assertNull(clearedUserId);
    }

    @Test
    void shouldHandleNullUserId() {
        // When - null userId
        userContextService.setUserContext(null, null);

        // Then - no exception, variable not set
        String userId = jdbcTemplate.queryForObject(
                "SELECT current_setting('request.user_id', true)",
                String.class
        );
        assertNull(userId);
    }

    @Test
    void shouldHandleEmptyUserId() {
        // When - empty userId
        userContextService.setUserContext("", null);

        // Then - no exception, variable not set
        String userId = jdbcTemplate.queryForObject(
                "SELECT current_setting('request.user_id', true)",
                String.class
        );
        assertNull(userId);
    }

    @Test
    void shouldSanitizeVariableNames() {
        // Given - claim with special characters
        String userId = "user-sanitize";
        Map<String, String> claims = Map.of("tenant-id@special!", "tenant-value");

        // When & Then - operations must be in same transaction
        transactionTemplate.execute(status -> {
            userContextService.setUserContext(userId, claims);

            // Special characters replaced with underscores
            String tenantId = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.jwt.tenant_id_special_', true)",
                    String.class
            );
            assertEquals("tenant-value", tenantId);
            return null;
        });
    }

    @Test
    void shouldEscapeSingleQuotesInValues() {
        // Given - value with single quote (SQL injection attempt)
        String userId = "user-quote";
        Map<String, String> claims = Map.of("data", "O'Brien");

        // When & Then - operations must be in same transaction
        transactionTemplate.execute(status -> {
            assertDoesNotThrow(() -> {
                userContextService.setUserContext(userId, claims);
            });

            // Verify value is properly escaped
            String data = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.jwt.data', true)",
                    String.class
            );
            assertEquals("O'Brien", data);
            return null;
        });
    }

    @Test
    void shouldSkipNullClaimValues() {
        // Given
        String userId = "user-null-claim";
        Map<String, String> claims = new HashMap<>();
        claims.put("valid_claim", "valid-value");
        claims.put("null_claim", null);
        claims.put("empty_claim", "");

        // When & Then - operations must be in same transaction
        transactionTemplate.execute(status -> {
            userContextService.setUserContext(userId, claims);

            // Only valid claim is set
            String validClaim = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.jwt.valid_claim', true)",
                    String.class
            );
            assertEquals("valid-value", validClaim);

            // null and empty claims should not be set
            String nullClaim = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.jwt.null_claim', true)",
                    String.class
            );
            assertNull(nullClaim);

            String emptyClaim = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.jwt.empty_claim', true)",
                    String.class
            );
            assertNull(emptyClaim);
            return null;
        });
    }

    @Test
    void shouldWorkWithTransactions() {
        // Given - set context in transaction
        String userId = "user-tx";

        // When & Then - use transaction template
        transactionTemplate.execute(status -> {
            userContextService.setUserContext(userId, null);

            // Variable available in transaction
            String actualUserId = jdbcTemplate.queryForObject(
                    "SELECT current_setting('request.user_id', true)",
                    String.class
            );
            assertEquals(userId, actualUserId);
            return null;
        });
    }
}
