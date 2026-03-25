package io.github.excalibase.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CompositeKeyIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("composite_testdb")
            .withUsername("composite_user")
            .withPassword("composite_pass");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

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
        setupTestData();
    }

    @AfterEach
    void cleanup() {
        cleanupTestData();
    }

    @AfterAll
    static void teardownAll() {
        postgres.stop();
        System.clearProperty("spring.datasource.url");
        System.clearProperty("spring.datasource.username");
        System.clearProperty("spring.datasource.password");
        System.clearProperty("spring.datasource.driver-class-name");
    }

    private void setupTestData() {
        // Create order_items table with composite primary key
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                order_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                unit_price DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (order_id, product_id)
            )
        """);

        // Insert test data
        jdbcTemplate.update(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
                1, 2, 5, 99.99
        );
        jdbcTemplate.update(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
                1, 3, 2, 49.99
        );
        jdbcTemplate.update(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
                2, 2, 1, 99.99
        );
    }

    private void cleanupTestData() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS order_items CASCADE");
    }

    @Test
    @Order(1)
    void shouldGetRecordByCompositeKey() throws Exception {
        mockMvc.perform(get("/api/v1/order_items/1,2"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.order_id").value(1))
                .andExpect(jsonPath("$.product_id").value(2))
                .andExpect(jsonPath("$.quantity").value(5))
                .andExpect(jsonPath("$.unit_price").value(99.99));
    }

    @Test
    @Order(2)
    void shouldUpdateRecordByCompositeKey() throws Exception {
        Map<String, Object> updateData = new HashMap<>();
        updateData.put("quantity", 10);

        mockMvc.perform(patch("/api/v1/order_items/1,2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(updateData)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.order_id").value(1))
                .andExpect(jsonPath("$.product_id").value(2))
                .andExpect(jsonPath("$.quantity").value(10))
                .andExpect(jsonPath("$.unit_price").value(99.99));
    }

    @Test
    @Order(3)
    void shouldDeleteRecordByCompositeKey() throws Exception {
        mockMvc.perform(delete("/api/v1/order_items/1,3"))
                .andExpect(status().isNoContent());

        // Verify record is deleted
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM order_items WHERE order_id = ? AND product_id = ?",
                Integer.class, 1, 3
        );
        assertEquals(0, count);
    }

    @Test
    @Order(4)
    void shouldReturn404ForNonExistentCompositeKey() throws Exception {
        mockMvc.perform(get("/api/v1/order_items/999,888"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("Record not found"));
    }

    @Test
    @Order(5)
    void shouldHandleCompositeKeyInQueries() throws Exception {
        mockMvc.perform(get("/api/v1/order_items")
                        .header("Prefer", "count=exact"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").value(hasSize(3)))
                .andExpect(jsonPath("$.pagination.total").value(3));
    }

    @Test
    @Order(6)
    void shouldCreateRecordWithCompositeKeyData() throws Exception {
        Map<String, Object> newOrderItem = new HashMap<>();
        newOrderItem.put("order_id", 3);
        newOrderItem.put("product_id", 4);
        newOrderItem.put("quantity", 7);
        newOrderItem.put("unit_price", 199.99);

        mockMvc.perform(post("/api/v1/order_items")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(newOrderItem)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.order_id").value(3))
                .andExpect(jsonPath("$.product_id").value(4))
                .andExpect(jsonPath("$.quantity").value(7))
                .andExpect(jsonPath("$.unit_price").value(199.99));
    }

    private String toJson(Object obj) throws JsonProcessingException {
        return objectMapper.writeValueAsString(obj);
    }
}
