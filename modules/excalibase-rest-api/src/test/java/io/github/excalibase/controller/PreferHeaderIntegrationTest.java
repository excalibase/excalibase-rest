package io.github.excalibase.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.postgres.service.DatabaseSchemaService;
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

import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PreferHeaderIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("prefer_testdb")
            .withUsername("prefer_user")
            .withPassword("prefer_pass");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private DatabaseSchemaService databaseSchemaService;

    private final ObjectMapper objectMapper = new ObjectMapper();

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
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                price DECIMAL(10,2)
            )
        """);
        jdbcTemplate.update("INSERT INTO items (name, price) VALUES (?, ?)", "Widget", 9.99);
        jdbcTemplate.update("INSERT INTO items (name, price) VALUES (?, ?)", "Gadget", 19.99);
        databaseSchemaService.clearCache();
    }

    @AfterEach
    void cleanup() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS items CASCADE");
        databaseSchemaService.clearCache();
    }

    @AfterAll
    static void teardownAll() {
        postgres.stop();
        System.clearProperty("spring.datasource.url");
        System.clearProperty("spring.datasource.username");
        System.clearProperty("spring.datasource.password");
        System.clearProperty("spring.datasource.driver-class-name");
    }

    @Test
    @Order(1)
    void postWithReturnRepresentation_returnsBody() throws Exception {
        String body = objectMapper.writeValueAsString(Map.of("name", "Doohickey", "price", 4.99));

        mockMvc.perform(post("/api/v1/items")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Prefer", "return=representation")
                .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Doohickey"));
    }

    @Test
    @Order(2)
    void postWithReturnHeadersOnly_returnsNoBodyAndLocationHeader() throws Exception {
        String body = objectMapper.writeValueAsString(Map.of("name", "Thingamajig", "price", 2.49));

        mockMvc.perform(post("/api/v1/items")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Prefer", "return=headers-only")
                .content(body))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", containsString("/api/v1/items/")))
                .andExpect(content().string(""));
    }

    @Test
    @Order(3)
    void postWithReturnMinimal_returns204() throws Exception {
        String body = objectMapper.writeValueAsString(Map.of("name", "Whatchamacallit", "price", 7.77));

        mockMvc.perform(post("/api/v1/items")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Prefer", "return=minimal")
                .content(body))
                .andExpect(status().isNoContent());
    }

    @Test
    @Order(4)
    void postWithNoPrefer_returnsBodyByDefault() throws Exception {
        String body = objectMapper.writeValueAsString(Map.of("name", "Doohickey2", "price", 1.11));

        mockMvc.perform(post("/api/v1/items")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Doohickey2"));
    }

    @Test
    @Order(5)
    void getWithCountExact_returnsContentRangeHeader() throws Exception {
        mockMvc.perform(get("/api/v1/items")
                .header("Prefer", "count=exact"))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Range", matchesPattern("0-\\d+/\\d+")));
    }

    @Test
    @Order(6)
    void patchWithReturnMinimal_returns204() throws Exception {
        String body = objectMapper.writeValueAsString(Map.of("price", 5.55));

        mockMvc.perform(patch("/api/v1/items/1")
                .contentType(MediaType.APPLICATION_JSON)
                .header("Prefer", "return=minimal")
                .content(body))
                .andExpect(status().isNoContent());
    }

    @Test
    @Order(7)
    void deleteWithReturnRepresentation_returns200WithBody() throws Exception {
        mockMvc.perform(delete("/api/v1/items/1")
                .header("Prefer", "return=representation"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.message").exists());
    }
}
