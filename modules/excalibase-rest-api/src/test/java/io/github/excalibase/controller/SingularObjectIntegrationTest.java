package io.github.excalibase.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.service.IDatabaseSchemaService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Phase 8A: Tests for Accept: application/vnd.pgrst.object+json
 * - Returns a single JSON object (not array) when exactly one row matches
 * - Returns 406 Not Acceptable when multiple rows would be returned
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SingularObjectIntegrationTest {

    private static final String SINGULAR_MEDIA_TYPE = "application/vnd.pgrst.object+json";

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("singular_testdb")
            .withUsername("singular_user")
            .withPassword("singular_pass");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private IDatabaseSchemaService databaseSchemaService;

    @BeforeAll
    static void setupAll() {
        postgres.start();
        System.setProperty("spring.datasource.url", postgres.getJdbcUrl());
        System.setProperty("spring.datasource.username", postgres.getUsername());
        System.setProperty("spring.datasource.password", postgres.getPassword());
        System.setProperty("spring.datasource.driver-class-name", "org.postgresql.Driver");
    }

    @BeforeEach
    void setup() {
        databaseSchemaService.clearCache();
        jdbcTemplate.execute("DROP TABLE IF EXISTS products CASCADE");
        jdbcTemplate.execute("""
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price NUMERIC(10,2)
            )
        """);
        jdbcTemplate.execute("INSERT INTO products (name, price) VALUES ('Widget', 9.99)");
        jdbcTemplate.execute("INSERT INTO products (name, price) VALUES ('Gadget', 19.99)");
    }

    @Test
    @Order(1)
    void withSingularAcceptAndOneRow_returnsSingleObject() throws Exception {
        // Filter to a single row
        mockMvc.perform(get("/api/v1/products")
                .param("name", "eq.Widget")
                .header("Accept", SINGULAR_MEDIA_TYPE))
               .andExpect(status().isOk())
               .andExpect(content().contentTypeCompatibleWith(SINGULAR_MEDIA_TYPE))
               // Body should be a JSON object, not an array
               .andExpect(jsonPath("$.name").value("Widget"))
               .andExpect(jsonPath("$.price").value(9.99));
    }

    @Test
    @Order(2)
    void withSingularAcceptAndMultipleRows_returns406() throws Exception {
        // No filter — both rows returned → 406
        mockMvc.perform(get("/api/v1/products")
                .header("Accept", SINGULAR_MEDIA_TYPE))
               .andExpect(status().is(406));
    }

    @Test
    @Order(3)
    void withSingularAcceptAndZeroRows_returns406() throws Exception {
        mockMvc.perform(get("/api/v1/products")
                .param("name", "eq.Nonexistent")
                .header("Accept", SINGULAR_MEDIA_TYPE))
               .andExpect(status().is(406));
    }

    @Test
    @Order(4)
    void withoutSingularAccept_returnsArray() throws Exception {
        mockMvc.perform(get("/api/v1/products")
                .param("name", "eq.Widget"))
               .andExpect(status().isOk())
               .andExpect(jsonPath("$.data").isArray());
    }
}
