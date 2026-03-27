package io.github.excalibase.controller;

import io.github.excalibase.service.IDatabaseSchemaService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
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
 * Integration tests for PostgreSQL edge cases:
 * - Column names with spaces (e.g. "zip code")
 * - Domain types (custom types over base types, e.g. year over int4)
 * - Enum values with special characters (e.g. PG-13)
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SpecialTypesIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("special_types_testdb")
            .withUsername("test_user")
            .withPassword("test_pass");

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
        setupTestData();
    }

    @AfterEach
    void cleanup() {
        databaseSchemaService.clearCache();
    }

    private void setupTestData() {
        // Domain type: year (integer with constraint, like dvdrental)
        jdbcTemplate.execute("DO $$ BEGIN CREATE DOMAIN year_type AS integer CHECK (VALUE >= 1900 AND VALUE <= 2100); EXCEPTION WHEN duplicate_object THEN null; END $$");

        // Enum with special characters (hyphen)
        jdbcTemplate.execute("DO $$ BEGIN CREATE TYPE rating_type AS ENUM ('G', 'PG', 'PG-13', 'R', 'NC-17'); EXCEPTION WHEN duplicate_object THEN null; END $$");

        // Table with space in column name, domain type, and enum
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS special_types_test (
                id SERIAL PRIMARY KEY,
                title VARCHAR(200) NOT NULL,
                "release year" year_type,
                rating rating_type DEFAULT 'PG',
                "zip code" VARCHAR(10),
                "full name" VARCHAR(200)
            )
        """);

        // Clear data before each test to prevent accumulation across @BeforeEach calls
        jdbcTemplate.execute("TRUNCATE TABLE special_types_test RESTART IDENTITY");

        // Seed data
        jdbcTemplate.execute("""
            INSERT INTO special_types_test (title, "release year", rating, "zip code", "full name")
            VALUES
                ('Academy Dinosaur', 2006, 'PG', '10001', 'John Doe'),
                ('Ace Goldfinger', 2006, 'G', '90210', 'Jane Smith'),
                ('Airport Pollock', 2010, 'R', '60601', 'Bob Johnson'),
                ('Bright Encounters', 2015, 'PG-13', '', 'Alice Brown'),
                ('Chamber Italian', 2020, 'NC-17', '98101', 'Charlie Wilson')
        """);

        databaseSchemaService.clearCache();
    }

    // ─── Column names with spaces ──────────────────────────────────────────────

    @Test
    @Order(1)
    void shouldListRecordsWithSpaceColumns() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test").param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data[0]['zip code']").exists())
                .andExpect(jsonPath("$.data[0]['full name']").exists());
    }

    @Test
    @Order(2)
    void shouldFilterByColumnWithSpace() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("zip code", "eq.90210"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data.length()").value(1))
                .andExpect(jsonPath("$.data[0].title").value("Ace Goldfinger"));
    }

    @Test
    @Order(3)
    void shouldFilterByEmptySpaceColumn() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("zip code", "eq."))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data[0].title").value("Bright Encounters"));
    }

    @Test
    @Order(4)
    void shouldSelectSpaceColumnByName() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("select", "title,zip code")
                        .param("limit", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].title").exists())
                .andExpect(jsonPath("$.data[0]['zip code']").exists())
                .andExpect(jsonPath("$.data[0].id").doesNotExist());
    }

    @Test
    @Order(5)
    void shouldSortByColumnWithSpace() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("orderBy", "zip code")
                        .param("orderDirection", "asc")
                        .param("select", "title,zip code")
                        .param("limit", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray());
    }

    // ─── Domain types ──────────────────────────────────────────────────────────

    @Test
    @Order(10)
    void shouldFilterByDomainType() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("release year", "eq.2006"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data.length()").value(2))
                .andExpect(jsonPath("$.data[0]['release year']").value(2006));
    }

    @Test
    @Order(11)
    void shouldFilterDomainTypeWithRange() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("release year", "gte.2015"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data.length()").value(2));
    }

    @Test
    @Order(12)
    void shouldSelectDomainTypeColumn() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("select", "title,release year")
                        .param("limit", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].title").exists())
                .andExpect(jsonPath("$.data[0]['release year']").exists());
    }

    // ─── Enum with special characters ──────────────────────────────────────────

    @Test
    @Order(20)
    void shouldFilterByEnumWithHyphen() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("rating", "eq.PG-13"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data.length()").value(1))
                .andExpect(jsonPath("$.data[0].title").value("Bright Encounters"))
                .andExpect(jsonPath("$.data[0].rating").value("PG-13"));
    }

    @Test
    @Order(21)
    void shouldFilterEnumWithInOperator() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("rating", "in.(PG,G,PG-13)"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data.length()").value(3));
    }

    @Test
    @Order(22)
    void shouldFilterEnumWithNotEqual() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("rating", "neq.R"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data.length()").value(4));
    }

    // ─── Combined: space column + domain + enum ────────────────────────────────

    @Test
    @Order(30)
    void shouldCombineSpaceColumnFilterAndDomainFilter() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("release year", "eq.2006")
                        .param("zip code", "eq.90210"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(1))
                .andExpect(jsonPath("$.data[0].title").value("Ace Goldfinger"));
    }

    @Test
    @Order(31)
    void shouldCombineAllSpecialTypes() throws Exception {
        mockMvc.perform(get("/api/v1/special_types_test")
                        .param("rating", "in.(PG,G)")
                        .param("release year", "eq.2006")
                        .param("select", "title,rating,release year,zip code")
                        .param("limit", "10")
                        .header("Prefer", "count=exact"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.length()").value(2))
                .andExpect(jsonPath("$.pagination.total").value(2));
    }

    // ─── CRUD with space columns ───────────────────────────────────────────────

    @Test
    @Order(40)
    void shouldCreateRecordWithSpaceColumns() throws Exception {
        String body = """
            {"title": "New Film", "release year": 2024, "rating": "PG-13", "zip code": "12345", "full name": "Test User"}
        """;

        mockMvc.perform(post("/api/v1/special_types_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.title").value("New Film"))
                .andExpect(jsonPath("$['release year']").value(2024))
                .andExpect(jsonPath("$.rating").value("PG-13"))
                .andExpect(jsonPath("$['zip code']").value("12345"))
                .andExpect(jsonPath("$['full name']").value("Test User"));
    }

    @Test
    @Order(41)
    void shouldUpdateRecordWithSpaceColumns() throws Exception {
        // Get the ID of first record
        Integer id = jdbcTemplate.queryForObject(
                "SELECT id FROM special_types_test WHERE title = 'Academy Dinosaur' LIMIT 1", Integer.class);

        String body = """
            {"zip code": "99999", "full name": "Updated Name"}
        """;

        mockMvc.perform(patch("/api/v1/special_types_test/" + id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$['zip code']").value("99999"))
                .andExpect(jsonPath("$['full name']").value("Updated Name"));
    }
}
