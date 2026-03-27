package io.github.excalibase.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
class RestApiControllerIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("restapi_testdb")
            .withUsername("restapi_user")
            .withPassword("restapi_pass");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private IDatabaseSchemaService databaseSchemaService;

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
        // Install PostgreSQL extensions needed for advanced data types
        jdbcTemplate.execute("CREATE EXTENSION IF NOT EXISTS hstore");
        jdbcTemplate.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");

        // Create custom enum type for RPC testing
        jdbcTemplate.execute("""
            DO $$ BEGIN
                CREATE TYPE tier_type AS ENUM ('bronze', 'silver', 'gold', 'platinum');
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        """);

        // Create users table
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE,
                tier tier_type DEFAULT 'bronze',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);

        // Create orders table for relationship testing
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id),
                product_name VARCHAR(255) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);

        // Insert test data
        jdbcTemplate.update("INSERT INTO users (name, email, tier) VALUES (?, ?, ?::tier_type)", "John", "john@example.com", "gold");
        jdbcTemplate.update("INSERT INTO users (name, email, tier) VALUES (?, ?, ?::tier_type)", "Jane", "jane@example.com", "silver");
        jdbcTemplate.update("INSERT INTO orders (user_id, product_name, amount) VALUES (?, ?, ?)", 1, "Laptop", 999.99);
        jdbcTemplate.update("INSERT INTO orders (user_id, product_name, amount) VALUES (?, ?, ?)", 1, "Mouse", 29.99);

        // Create RPC test functions

        // Scalar function with basic types
        jdbcTemplate.execute("""
            CREATE OR REPLACE FUNCTION calculate_discount(
                customer_tier text,
                order_amount decimal
            )
            RETURNS decimal AS $$
            BEGIN
                RETURN CASE customer_tier
                    WHEN 'platinum' THEN order_amount * 0.20
                    WHEN 'gold' THEN order_amount * 0.15
                    WHEN 'silver' THEN order_amount * 0.10
                    WHEN 'bronze' THEN order_amount * 0.05
                    ELSE 0.00
                END;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE
        """);

        // Table-returning function with enum parameter
        jdbcTemplate.execute("""
            CREATE OR REPLACE FUNCTION get_users_by_tier(tier_param tier_type)
            RETURNS SETOF users AS $$
            BEGIN
                RETURN QUERY
                SELECT * FROM users
                WHERE tier = tier_param
                ORDER BY name;
            END;
            $$ LANGUAGE plpgsql STABLE
        """);

        // Function with multiple parameters
        jdbcTemplate.execute("""
            CREATE OR REPLACE FUNCTION get_order_total(
                user_id_param integer,
                min_amount decimal
            )
            RETURNS TABLE(
                total_orders bigint,
                total_amount decimal
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT
                    COUNT(*)::bigint,
                    COALESCE(SUM(amount), 0)
                FROM orders
                WHERE user_id = user_id_param
                  AND amount >= min_amount;
            END;
            $$ LANGUAGE plpgsql STABLE
        """);

        // Computed field function
        jdbcTemplate.execute("""
            CREATE OR REPLACE FUNCTION users_display_name(users)
            RETURNS text AS $$
                SELECT $1.name || ' [' || UPPER($1.tier::text) || ']';
            $$ LANGUAGE SQL IMMUTABLE
        """);
    }

    private void cleanupTestData() {
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS users_display_name(users) CASCADE");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS get_order_total(integer, decimal) CASCADE");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS get_users_by_tier(tier_type) CASCADE");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS calculate_discount(text, decimal) CASCADE");
        jdbcTemplate.execute("DROP TABLE IF EXISTS orders CASCADE");
        jdbcTemplate.execute("DROP TABLE IF EXISTS users CASCADE");
        jdbcTemplate.execute("DROP TYPE IF EXISTS tier_type CASCADE");
    }

    @Test
    @Order(1)
    void shouldGetRecordsSuccessfully() throws Exception {
        mockMvc.perform(get("/api/v1/users")
                        .header("Prefer", "count=exact"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").isNotEmpty())
                .andExpect(jsonPath("$.data[0].name").exists())
                .andExpect(jsonPath("$.pagination.total").value(2));
    }

    @Test
    @Order(2)
    void shouldHandleQueryParametersCorrectly() throws Exception {
        mockMvc.perform(get("/api/v1/users")
                        .param("offset", "0")
                        .param("limit", "1")
                        .param("orderBy", "name")
                        .param("orderDirection", "asc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").exists())
                .andExpect(jsonPath("$.pagination.limit").value(1));
    }

    @Test
    @Order(3)
    void shouldGetSingleRecordById() throws Exception {
        mockMvc.perform(get("/api/v1/users/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.name").value("John"))
                .andExpect(jsonPath("$.email").value("john@example.com"));
    }

    @Test
    @Order(4)
    void shouldReturn404ForNonExistentRecord() throws Exception {
        mockMvc.perform(get("/api/v1/users/999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("Record not found"));
    }

    @Test
    @Order(5)
    void shouldCreateSingleRecord() throws Exception {
        Map<String, Object> newUser = new HashMap<>();
        newUser.put("name", "Alice");
        newUser.put("email", "alice@example.com");

        mockMvc.perform(post("/api/v1/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(newUser)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Alice"))
                .andExpect(jsonPath("$.email").value("alice@example.com"));
    }

    @Test
    @Order(6)
    void shouldCreateBulkRecords() throws Exception {
        Map<String, Object> user1 = new HashMap<>();
        user1.put("name", "Bob");

        Map<String, Object> user2 = new HashMap<>();
        user2.put("name", "Charlie");

        List<Map<String, Object>> users = Arrays.asList(user1, user2);

        mockMvc.perform(post("/api/v1/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(users)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data[0].name").value("Bob"))
                .andExpect(jsonPath("$.data[1].name").value("Charlie"))
                .andExpect(jsonPath("$.count").value(2));
    }

    @Test
    @Order(7)
    void shouldUpdateRecordWithPut() throws Exception {
        Map<String, Object> updateData = new HashMap<>();
        updateData.put("name", "John Updated");
        updateData.put("email", "john.updated@example.com");

        mockMvc.perform(put("/api/v1/users/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(updateData)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("John Updated"))
                .andExpect(jsonPath("$.email").value("john.updated@example.com"));
    }

    @Test
    @Order(8)
    void shouldPartiallyUpdateRecordWithPatch() throws Exception {
        Map<String, Object> patchData = new HashMap<>();
        patchData.put("email", "updated@example.com");

        mockMvc.perform(patch("/api/v1/users/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(patchData)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.email").value("updated@example.com"))
                .andExpect(jsonPath("$.name").value("John")); // Name should remain unchanged
    }

    @Test
    @Order(9)
    void shouldDeleteRecord() throws Exception {
        mockMvc.perform(delete("/api/v1/users/2"))
                .andExpect(status().isNoContent());
    }

    @Test
    @Order(10)
    void shouldReturn404WhenDeletingNonExistentRecord() throws Exception {
        mockMvc.perform(delete("/api/v1/users/999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("Record not found"));
    }

    @Test
    @Order(11)
    void shouldReturnSchemaInformation() throws Exception {
        mockMvc.perform(get("/api/v1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.tables").isArray())
                .andExpect(jsonPath("$.tables").isNotEmpty());
    }

    @Test
    @Order(12)
    void shouldReturnTableSchema() throws Exception {
        mockMvc.perform(get("/api/v1/users/schema"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.table.name").value("users"))
                .andExpect(jsonPath("$.table.columns").isArray())
                .andExpect(jsonPath("$.table.columns").isNotEmpty());
    }

    @Test
    @Order(13)
    void shouldReturn404ForNonExistentTableSchema() throws Exception {
        mockMvc.perform(get("/api/v1/nonexistent/schema"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("Table not found: nonexistent"));
    }

    @Test
    @Order(14)
    void shouldReturnOpenApiJsonSpecification() throws Exception {
        mockMvc.perform(get("/api/v1/openapi.json"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.openapi").exists())
                .andExpect(jsonPath("$.info").exists());
    }

    @Test
    @Order(15)
    void shouldReturnOpenApiYamlSpecification() throws Exception {
        mockMvc.perform(get("/api/v1/openapi.yaml"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/yaml"))
                .andExpect(content().string(containsString("openapi:")));
    }

    @Test
    @Order(16)
    void shouldReturnApiDocumentationInfo() throws Exception {
        mockMvc.perform(get("/api/v1/docs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.title").value("Excalibase REST API Documentation"))
                .andExpect(jsonPath("$.openapi_json").value("/api/v1/openapi.json"))
                .andExpect(jsonPath("$.swagger_ui").exists());
    }

    @Test
    @Order(17)
    void shouldApplyOrConditionsInFilters() throws Exception {
        mockMvc.perform(get("/api/v1/users").param("or", "(name.like.John,name.like.Jane)"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").isNotEmpty());
    }

    @Test
    @Order(18)
    void shouldHandleComplexPostgreSqlTypes() throws Exception {
        jdbcTemplate.execute("""
            ALTER TABLE orders ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'
        """);

        jdbcTemplate.execute("""
            UPDATE orders SET metadata = '{"priority": "high", "source": "web"}' WHERE id = 1
        """);

        jdbcTemplate.execute("""
            UPDATE orders SET metadata = '{"status": "active", "category": "electronics"}' WHERE id = 2
        """);

        try {
            mockMvc.perform(get("/api/v1/orders"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.data").isArray())
                    .andExpect(jsonPath("$.data").isNotEmpty())
                    .andExpect(jsonPath("$.data[0].metadata").exists());
        } finally {
            jdbcTemplate.execute("ALTER TABLE orders DROP COLUMN IF EXISTS metadata");
        }
    }

    @Test
    @Order(19)
    void shouldHandleSortingWithOrderByParameter() throws Exception {
        mockMvc.perform(get("/api/v1/users")
                        .param("orderBy", "name")
                        .param("orderDirection", "desc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").isNotEmpty());
    }

    @Test
    @Order(20)
    void shouldHandleOrderParameter() throws Exception {
        mockMvc.perform(get("/api/v1/users").param("order", "name.desc,id.asc"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").isNotEmpty());
    }

    @Test
    @Order(21)
    void shouldSelectSpecificColumns() throws Exception {
        mockMvc.perform(get("/api/v1/users").param("select", "name,email"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data[0].name").exists())
                .andExpect(jsonPath("$.data[0].email").exists());
    }

    @Test
    @Order(22)
    void shouldHandleCursorBasedPagination() throws Exception {
        mockMvc.perform(get("/api/v1/users").param("first", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.edges").isArray())
                .andExpect(jsonPath("$.pageInfo.hasNextPage").exists())
                .andExpect(jsonPath("$.pageInfo.hasPreviousPage").exists())
                .andExpect(jsonPath("$.totalCount").exists());
    }

    @Test
    @Order(23)
    void shouldExpandRelationships() throws Exception {
        jdbcTemplate.execute("""
            INSERT INTO orders (user_id, product_name, amount) VALUES (1, 'Test Product', 99.99)
            ON CONFLICT DO NOTHING
        """);

        mockMvc.perform(get("/api/v1/orders").param("expand", "users"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").isNotEmpty());
    }

    @Test
    @Order(24)
    void shouldValidateColumnNamesInFilters() throws Exception {
        mockMvc.perform(get("/api/v1/users").param("invalid_column", "eq.value"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value(containsString("Invalid column")));
    }

    @Test
    @Order(25)
    void shouldValidateEmptyRequestBody() throws Exception {
        mockMvc.perform(post("/api/v1/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Request body cannot be empty"));
    }

    @Test
    @Order(26)
    void shouldValidateRequestBodyType() throws Exception {
        mockMvc.perform(post("/api/v1/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("\"invalid string body\""))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Request body must be an object or array"));
    }

    @Test
    @Order(27)
    void shouldHandleAdvancedFilteringWithJsonOperators() throws Exception {
        jdbcTemplate.execute("""
            ALTER TABLE orders ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'
        """);

        jdbcTemplate.execute("""
            UPDATE orders SET metadata = '{"priority": "high", "source": "web"}' WHERE id = 1
        """);

        try {
            mockMvc.perform(get("/api/v1/orders"))
                    .andExpect(status().isOk())
                    .andExpect(jsonPath("$.data").isArray())
                    .andExpect(jsonPath("$.data").isNotEmpty())
                    .andExpect(jsonPath("$.data[0].metadata").exists());
        } finally {
            jdbcTemplate.execute("ALTER TABLE orders DROP COLUMN IF EXISTS metadata");
        }
    }

    @Test
    @Order(28)
    void shouldTestStringVsJsonbColumnCreation() throws Exception {
        // Test string column first
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS string_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                metadata TEXT NOT NULL
            )
        """);
        databaseSchemaService.clearCache();

        Map<String, Object> stringData = new HashMap<>();
        stringData.put("name", "String Product");
        stringData.put("metadata", "{\"category\": \"electronics\", \"price\": 100}");

        MvcResult result1 = mockMvc.perform(post("/api/v1/string_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(stringData)))
                .andReturn();

        System.err.println("===== STRING CREATE RESPONSE =====");
        System.err.println("Response status: " + result1.getResponse().getStatus());
        System.err.println("Response body: " + result1.getResponse().getContentAsString());
        System.err.println("===================================");

        Assertions.assertEquals(201, result1.getResponse().getStatus());

        // Test JSONB column
        jdbcTemplate.execute("DROP TABLE IF EXISTS string_test");
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS jsonb_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                metadata JSONB NOT NULL
            )
        """);
        databaseSchemaService.clearCache();

        Map<String, Object> jsonbData1 = new HashMap<>();
        jsonbData1.put("name", "JSONB Product String");
        jsonbData1.put("metadata", "{\"category\": \"electronics\", \"price\": 100}");

        MvcResult result2 = mockMvc.perform(post("/api/v1/jsonb_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(jsonbData1)))
                .andReturn();

        System.err.println("===== JSONB STRING CREATE =====");
        System.err.println("Response status: " + result2.getResponse().getStatus());
        System.err.println("Response body: " + result2.getResponse().getContentAsString());
        System.err.println("================================");

        Map<String, Object> jsonbData2 = new HashMap<>();
        jsonbData2.put("name", "JSONB Product Object");
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("category", "electronics");
        metadata.put("price", 100);
        jsonbData2.put("metadata", metadata);

        MvcResult result3 = mockMvc.perform(post("/api/v1/jsonb_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(jsonbData2)))
                .andReturn();

        System.err.println("===== JSONB OBJECT CREATE =====");
        System.err.println("Response status: " + result3.getResponse().getStatus());
        System.err.println("Response body: " + result3.getResponse().getContentAsString());
        System.err.println("================================");

        jdbcTemplate.execute("DROP TABLE IF EXISTS string_test");
        jdbcTemplate.execute("DROP TABLE IF EXISTS jsonb_test");
    }

    @Test
    @Order(29)
    void shouldTestIfTheIssueIsWithDynamicTableCreation() throws Exception {
        mockMvc.perform(get("/api/v1/users"))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/v1/nonexistent"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Table not found: nonexistent"));

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )
        """);
        databaseSchemaService.clearCache();

        MvcResult result = mockMvc.perform(get("/api/v1/test_table"))
                .andReturn();

        System.err.println("New table response status: " + result.getResponse().getStatus());
        System.err.println("New table response body: " + result.getResponse().getContentAsString());

        mockMvc.perform(get("/api/v1/test_table"))
                .andExpect(status().isOk());

        jdbcTemplate.execute("DROP TABLE IF EXISTS test_table");
    }

    @Test
    @Order(30)
    void shouldCreateSimpleJsonbRecordThroughRestApi() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                metadata JSONB NOT NULL,
                settings JSONB
            )
        """);

        databaseSchemaService.clearCache();

        Map<String, Object> productData = new HashMap<>();
        productData.put("name", "Smart Phone");

        Map<String, Object> specs = new HashMap<>();
        specs.put("cpu", "A15 Bionic");
        specs.put("ram", "6GB");
        specs.put("storage", "128GB");
        specs.put("features", Arrays.asList("5G", "Face ID", "Wireless Charging"));

        Map<String, Object> pricing = new HashMap<>();
        pricing.put("msrp", 999.99);
        pricing.put("currency", "USD");

        Map<String, Object> discount1 = new HashMap<>();
        discount1.put("type", "student");
        discount1.put("percentage", 10);

        Map<String, Object> discount2 = new HashMap<>();
        discount2.put("type", "employee");
        discount2.put("percentage", 15);

        pricing.put("discounts", Arrays.asList(discount1, discount2));

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("specs", specs);
        metadata.put("pricing", pricing);

        Map<String, Object> display = new HashMap<>();
        display.put("theme", "dark");
        display.put("language", "en");
        display.put("notifications", true);

        Map<String, Object> privacy = new HashMap<>();
        privacy.put("analytics", false);
        privacy.put("cookies", true);

        Map<String, Object> settings = new HashMap<>();
        settings.put("display", display);
        settings.put("privacy", privacy);

        productData.put("metadata", metadata);
        productData.put("settings", settings);

        mockMvc.perform(post("/api/v1/products")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(productData)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Smart Phone"))
                .andExpect(jsonPath("$.metadata.specs.cpu").value("A15 Bionic"))
                .andExpect(jsonPath("$.metadata.specs.features").isArray())
                .andExpect(jsonPath("$.metadata.pricing.discounts").isArray())
                .andExpect(jsonPath("$.settings.display.theme").value("dark"));

        mockMvc.perform(get("/api/v1/products"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].metadata.specs.features[0]").value("5G"))
                .andExpect(jsonPath("$.data[0].metadata.pricing.discounts[0].type").value("student"));

        jdbcTemplate.execute("DROP TABLE IF EXISTS products");
    }

    @Test
    @Order(31)
    void shouldUpdateRecordsWithJsonbDataThroughRestApi() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content JSONB NOT NULL,
                metadata JSONB
            )
        """);

        databaseSchemaService.clearCache();

        jdbcTemplate.update("""
            INSERT INTO articles (title, content, metadata)
            VALUES (?, ?::jsonb, ?::jsonb)
        """, "Initial Title",
                "{\"body\": \"Initial content\", \"tags\": [\"tech\", \"startup\"]}",
                "{\"author\": \"John Doe\", \"created\": \"2023-01-01\"}");

        Map<String, Object> updateData = new HashMap<>();
        updateData.put("title", "Updated Article Title");

        Map<String, Object> content = new HashMap<>();
        content.put("body", "Updated article content with more details");
        content.put("tags", Arrays.asList("technology", "innovation", "startup", "AI"));

        Map<String, Object> section1 = new HashMap<>();
        section1.put("heading", "Introduction");
        section1.put("paragraphs", Arrays.asList("First intro paragraph"));

        Map<String, Object> section2 = new HashMap<>();
        section2.put("heading", "Main Content");
        section2.put("paragraphs", Arrays.asList("Main paragraph 1", "Main paragraph 2"));

        Map<String, Object> section3 = new HashMap<>();
        section3.put("heading", "Conclusion");
        section3.put("paragraphs", Arrays.asList("Final thoughts"));

        content.put("sections", Arrays.asList(section1, section2, section3));

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("author", "Jane Smith");
        metadata.put("updated", "2023-12-01");
        metadata.put("version", 2);
        metadata.put("reviewers", Arrays.asList("Alice", "Bob", "Charlie"));

        Map<String, Object> stats = new HashMap<>();
        stats.put("wordCount", 1500);
        stats.put("readTime", 7);
        metadata.put("stats", stats);

        updateData.put("content", content);
        updateData.put("metadata", metadata);

        mockMvc.perform(put("/api/v1/articles/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(updateData)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.title").value("Updated Article Title"))
                .andExpect(jsonPath("$.content.tags").isArray())
                .andExpect(jsonPath("$.content.sections").isArray())
                .andExpect(jsonPath("$.metadata.reviewers").isArray())
                .andExpect(jsonPath("$.metadata.stats.wordCount").value(1500));

        jdbcTemplate.execute("DROP TABLE IF EXISTS articles");
    }

    @Test
    @Order(32)
    void shouldCreateAndRetrieveRecordsWithPostgreSqlArraysThroughRestApi() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS array_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                text_array TEXT[] NOT NULL,
                integer_array INTEGER[],
                decimal_array DECIMAL[],
                boolean_array BOOLEAN[],
                uuid_array UUID[],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """);

        databaseSchemaService.clearCache();

        Map<String, Object> arrayData = new HashMap<>();
        arrayData.put("name", "Array Test Record");
        arrayData.put("text_array", Arrays.asList("hello", "world", "postgresql", "arrays"));
        arrayData.put("integer_array", Arrays.asList(1, 2, 3, 100, -50, 0));
        arrayData.put("decimal_array", Arrays.asList(1.1, 2.5, 99.99, -10.25, 0.0));
        arrayData.put("boolean_array", Arrays.asList(true, false, true, false));
        arrayData.put("uuid_array", Arrays.asList("123e4567-e89b-12d3-a456-426614174000", "987fcdeb-51a2-43d1-b789-123456789abc"));

        mockMvc.perform(post("/api/v1/array_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(arrayData)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Array Test Record"))
                .andExpect(jsonPath("$.text_array").isArray())
                .andExpect(jsonPath("$.text_array[0]").value("hello"))
                .andExpect(jsonPath("$.integer_array").isArray())
                .andExpect(jsonPath("$.integer_array[3]").value(100))
                .andExpect(jsonPath("$.boolean_array").isArray())
                .andExpect(jsonPath("$.uuid_array").isArray());

        mockMvc.perform(get("/api/v1/array_test"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].text_array").value(hasSize(4)))
                .andExpect(jsonPath("$.data[0].integer_array").value(hasSize(6)))
                .andExpect(jsonPath("$.data[0].decimal_array").value(hasSize(5)));

        jdbcTemplate.execute("DROP TABLE IF EXISTS array_test");
    }

    @Test
    @Order(33)
    void shouldUpdateRecordsWithPostgreSqlArraysThroughRestApi() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                id SERIAL PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                tags TEXT[],
                scores NUMERIC[],
                permissions TEXT[],
                settings JSONB
            )
        """);

        databaseSchemaService.clearCache();

        jdbcTemplate.update("""
            INSERT INTO user_profiles (username, tags, scores, permissions, settings)
            VALUES (?, ?::text[], ?::numeric[], ?::text[], ?::jsonb)
        """, "testuser",
                "{basic,user}",
                "{10.5,20.0,15.7}",
                "{read,comment}",
                "{\"theme\": \"light\", \"notifications\": true}");

        Map<String, Object> updateData = new HashMap<>();
        updateData.put("username", "advanced_user");
        updateData.put("tags", Arrays.asList("advanced", "premium", "verified", "contributor"));
        updateData.put("scores", Arrays.asList(95.5, 87.2, 100.0, 92.8, 88.1, 99.3));
        updateData.put("permissions", Arrays.asList("read", "write", "delete", "admin", "moderate"));

        Map<String, Object> settings = new HashMap<>();
        settings.put("theme", "dark");
        settings.put("notifications", false);
        settings.put("features", Arrays.asList("beta_testing", "advanced_analytics"));

        Map<String, Object> preferences = new HashMap<>();
        preferences.put("language", "en");
        preferences.put("timezone", "UTC");
        preferences.put("auto_save", true);
        settings.put("preferences", preferences);

        updateData.put("settings", settings);

        mockMvc.perform(put("/api/v1/user_profiles/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(updateData)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("advanced_user"))
                .andExpect(jsonPath("$.tags").value(hasSize(4)))
                .andExpect(jsonPath("$.scores").value(hasSize(6)))
                .andExpect(jsonPath("$.permissions").value(hasSize(5)))
                .andExpect(jsonPath("$.settings.features").isArray());

        jdbcTemplate.execute("DROP TABLE IF EXISTS user_profiles");
    }

    @Test
    @Order(34)
    void shouldHandleMultidimensionalArraysThroughRestApi() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS matrix_data (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                integer_matrix INTEGER[][],
                text_matrix TEXT[][]
            )
        """);

        databaseSchemaService.clearCache();

        Map<String, Object> matrixData = new HashMap<>();
        matrixData.put("name", "Matrix Test");
        matrixData.put("integer_matrix", Arrays.asList(
                Arrays.asList(1, 2, 3),
                Arrays.asList(4, 5, 6),
                Arrays.asList(7, 8, 9)
        ));
        matrixData.put("text_matrix", Arrays.asList(
                Arrays.asList("hello", "world", "test"),
                Arrays.asList("foo", "bar", "baz"),
                Arrays.asList("postgresql", "arrays", "multidimensional")
        ));

        mockMvc.perform(post("/api/v1/matrix_data")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(matrixData)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Matrix Test"))
                .andExpect(jsonPath("$.integer_matrix").isArray())
                .andExpect(jsonPath("$.integer_matrix[0]").isArray())
                .andExpect(jsonPath("$.text_matrix").isArray())
                .andExpect(jsonPath("$.text_matrix[2]").isArray());

        jdbcTemplate.execute("DROP TABLE IF EXISTS matrix_data");
    }

    @Test
    @Order(35)
    void shouldHandleComplexJsonContainingArraysThroughRestApi() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS complex_data (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                data JSONB NOT NULL,
                tags TEXT[],
                numbers INTEGER[]
            )
        """);

        databaseSchemaService.clearCache();

        Map<String, Object> complexData = new HashMap<>();
        complexData.put("name", "Complex Test Record");

        Map<String, Object> basicInfo = new HashMap<>();
        basicInfo.put("name", "John Doe");
        basicInfo.put("age", 30);
        basicInfo.put("emails", Arrays.asList("john@work.com", "john@personal.com"));
        basicInfo.put("phone_numbers", Arrays.asList("+1-555-0123", "+1-555-0456"));

        Map<String, Object> preferences = new HashMap<>();
        preferences.put("languages", Arrays.asList("English", "Spanish", "French"));
        preferences.put("hobbies", Arrays.asList("reading", "coding", "hiking"));
        preferences.put("skills", Arrays.asList(
                Arrays.asList("Java", "Expert"),
                Arrays.asList("PostgreSQL", "Advanced"),
                Arrays.asList("Spring", "Intermediate")
        ));

        Map<String, Object> userProfile = new HashMap<>();
        userProfile.put("basic_info", basicInfo);
        userProfile.put("preferences", preferences);

        Map<String, Object> activity1 = new HashMap<>();
        activity1.put("date", "2023-12-01");
        activity1.put("actions", Arrays.asList("login", "view_dashboard", "update_profile"));
        activity1.put("duration_minutes", 45);

        Map<String, Object> activity2 = new HashMap<>();
        activity2.put("date", "2023-12-02");
        activity2.put("actions", Arrays.asList("login", "create_report", "logout"));
        activity2.put("duration_minutes", 120);

        Map<String, Object> data = new HashMap<>();
        data.put("user_profile", userProfile);
        data.put("activity_log", Arrays.asList(activity1, activity2));

        complexData.put("data", data);
        complexData.put("tags", Arrays.asList("complex", "json", "arrays", "test"));
        complexData.put("numbers", Arrays.asList(100, 200, 300, 400, 500));

        mockMvc.perform(post("/api/v1/complex_data")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(complexData)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Complex Test Record"))
                .andExpect(jsonPath("$.data.user_profile.basic_info.emails").isArray())
                .andExpect(jsonPath("$.data.activity_log").isArray())
                .andExpect(jsonPath("$.tags").isArray())
                .andExpect(jsonPath("$.numbers").isArray());

        mockMvc.perform(get("/api/v1/complex_data")
                        .param("data", "haskey.user_profile"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").isNotEmpty());

        jdbcTemplate.execute("DROP TABLE IF EXISTS complex_data");
    }

    @Test
    @Order(36)
    void shouldHandleBulkOperationsWithJsonAndArraysThroughRestApi() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS bulk_complex (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                metadata JSONB,
                tags TEXT[]
            )
        """);

        databaseSchemaService.clearCache();

        Map<String, Object> user1 = new HashMap<>();
        user1.put("name", "User 1");
        Map<String, Object> metadata1 = new HashMap<>();
        metadata1.put("role", "admin");
        metadata1.put("permissions", Arrays.asList("read", "write", "delete"));
        Map<String, Object> profile1 = new HashMap<>();
        profile1.put("department", "engineering");
        profile1.put("level", "senior");
        metadata1.put("profile", profile1);
        user1.put("metadata", metadata1);
        user1.put("tags", Arrays.asList("admin", "engineering", "senior"));

        Map<String, Object> user2 = new HashMap<>();
        user2.put("name", "User 2");
        Map<String, Object> metadata2 = new HashMap<>();
        metadata2.put("role", "user");
        metadata2.put("permissions", Arrays.asList("read"));
        Map<String, Object> profile2 = new HashMap<>();
        profile2.put("department", "marketing");
        profile2.put("level", "junior");
        metadata2.put("profile", profile2);
        user2.put("metadata", metadata2);
        user2.put("tags", Arrays.asList("user", "marketing", "junior"));

        Map<String, Object> user3 = new HashMap<>();
        user3.put("name", "User 3");
        Map<String, Object> metadata3 = new HashMap<>();
        metadata3.put("role", "moderator");
        metadata3.put("permissions", Arrays.asList("read", "write"));
        Map<String, Object> profile3 = new HashMap<>();
        profile3.put("department", "support");
        profile3.put("level", "mid");
        metadata3.put("profile", profile3);
        user3.put("metadata", metadata3);
        user3.put("tags", Arrays.asList("moderator", "support", "mid-level"));

        List<Map<String, Object>> bulkData = Arrays.asList(user1, user2, user3);

        mockMvc.perform(post("/api/v1/bulk_complex")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(bulkData)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.data").isArray())
                .andExpect(jsonPath("$.data").value(hasSize(3)))
                .andExpect(jsonPath("$.data[0].metadata.role").value("admin"))
                .andExpect(jsonPath("$.data[1].tags").isArray())
                .andExpect(jsonPath("$.data[2].metadata.permissions").isArray())
                .andExpect(jsonPath("$.count").value(3));

        jdbcTemplate.execute("DROP TABLE IF EXISTS bulk_complex");
    }

    @Test
    @Order(37)
    void shouldFilterRecordsUsingArrayOperatorsThroughRestApi() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS filter_arrays (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                skills TEXT[],
                scores INTEGER[],
                categories TEXT[]
            )
        """);

        databaseSchemaService.clearCache();

        jdbcTemplate.update("""
            INSERT INTO filter_arrays (name, skills, scores, categories) VALUES
            (?, ?::text[], ?::integer[], ?::text[]),
            (?, ?::text[], ?::integer[], ?::text[]),
            (?, ?::text[], ?::integer[], ?::text[])
        """,
                "Developer 1", "{java,spring,postgresql}", "{95,87,92}", "{backend,database}",
                "Developer 2", "{javascript,react,nodejs}", "{88,91,85}", "{frontend,web}",
                "Full Stack", "{java,javascript,postgresql,react}", "{90,89,93,88}", "{backend,frontend,database}");

        mockMvc.perform(get("/api/v1/filter_arrays")
                        .param("skills", "arraycontains.java"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").value(hasSize(2)));

        mockMvc.perform(get("/api/v1/filter_arrays")
                        .param("categories", "arrayhasany.{frontend,mobile}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").value(hasSize(2)));

        mockMvc.perform(get("/api/v1/filter_arrays")
                        .param("skills", "arraylength.4"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").value(hasSize(1)))
                .andExpect(jsonPath("$.data[0].name").value("Full Stack"));

        jdbcTemplate.execute("DROP TABLE IF EXISTS filter_arrays");
    }

    @Test
    @Order(38)
    void shouldCreateRecordWithMacAddressType() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS mac_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                mac_address MACADDR NOT NULL
            )
        """);

        databaseSchemaService.clearCache();

        Map<String, Object> macData = new HashMap<>();
        macData.put("name", "Device 1");
        macData.put("mac_address", "08:00:2b:01:02:03");

        mockMvc.perform(post("/api/v1/mac_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(macData)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Device 1"))
                .andExpect(jsonPath("$.mac_address").value("08:00:2b:01:02:03"));

        jdbcTemplate.execute("DROP TABLE IF EXISTS mac_test");
    }

    @Test
    @Order(39)
    void shouldCreateRecordWithBitType() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS bit_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                status_bits BIT(8) NOT NULL
            )
        """);

        databaseSchemaService.clearCache();

        Map<String, Object> bitData = new HashMap<>();
        bitData.put("name", "Status Record");
        bitData.put("status_bits", "10110101");

        MvcResult result = mockMvc.perform(post("/api/v1/bit_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(bitData)))
                .andReturn();

        System.out.println("===== BIT TYPE DEBUG =====");
        System.out.println("Response status: " + result.getResponse().getStatus());
        System.out.println("Response body: " + result.getResponse().getContentAsString());
        System.out.println("==========================");

        mockMvc.perform(post("/api/v1/bit_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(bitData)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Status Record"))
                .andExpect(jsonPath("$.status_bits").exists());

        jdbcTemplate.execute("DROP TABLE IF EXISTS bit_test");
    }

    @Test
    @Order(40)
    void shouldDebugSimpleJsonbTest() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS simple_jsonb_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                data JSONB
            )
        """);

        databaseSchemaService.clearCache();

        Map<String, Object> simpleData = new HashMap<>();
        simpleData.put("name", "Simple JSONB Test");
        Map<String, Object> data = new HashMap<>();
        data.put("theme", "dark");
        data.put("notifications", false);
        data.put("features", Arrays.asList("feature1", "feature2"));
        simpleData.put("data", data);

        MvcResult result = mockMvc.perform(post("/api/v1/simple_jsonb_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(simpleData)))
                .andReturn();

        System.out.println("===== SIMPLE JSONB DEBUG =====");
        System.out.println("Response status: " + result.getResponse().getStatus());
        System.out.println("Response body: " + result.getResponse().getContentAsString());
        System.out.println("Response error: " + result.getResponse().getErrorMessage());
        System.out.println("===============================");

        jdbcTemplate.execute("DROP TABLE IF EXISTS simple_jsonb_test");
    }

    @Test
    @Order(41)
    void shouldDebugSimpleArrayTest() throws Exception {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS simple_array_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                tags TEXT[]
            )
        """);

        databaseSchemaService.clearCache();

        Map<String, Object> simpleData = new HashMap<>();
        simpleData.put("name", "Simple Test");
        simpleData.put("tags", Arrays.asList("tag1", "tag2"));

        MvcResult result = mockMvc.perform(post("/api/v1/simple_array_test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(simpleData)))
                .andReturn();

        System.out.println("===== SIMPLE ARRAY DEBUG =====");
        System.out.println("Response status: " + result.getResponse().getStatus());
        System.out.println("Response body: " + result.getResponse().getContentAsString());
        System.out.println("Response error: " + result.getResponse().getErrorMessage());
        System.out.println("===============================");

        jdbcTemplate.execute("DROP TABLE IF EXISTS simple_array_test");
    }

    // ==================== RPC FUNCTION TESTS ====================

    @Test
    @Order(42)
    void shouldExecuteScalarRpcFunction() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("customer_tier", "gold");
        params.put("order_amount", 100.0);

        mockMvc.perform(post("/api/v1/rpc/calculate_discount")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value(15.00));
    }

    @Test
    @Order(43)
    void shouldExecuteScalarRpcFunctionWithDifferentTiers() throws Exception {
        // Test platinum tier (20%)
        Map<String, Object> platinumParams = new HashMap<>();
        platinumParams.put("customer_tier", "platinum");
        platinumParams.put("order_amount", 200.0);

        mockMvc.perform(post("/api/v1/rpc/calculate_discount")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(platinumParams)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value(40.00));

        // Test silver tier (10%)
        Map<String, Object> silverParams = new HashMap<>();
        silverParams.put("customer_tier", "silver");
        silverParams.put("order_amount", 100.0);

        mockMvc.perform(post("/api/v1/rpc/calculate_discount")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(silverParams)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value(10.00));

        // Test bronze tier (5%)
        Map<String, Object> bronzeParams = new HashMap<>();
        bronzeParams.put("customer_tier", "bronze");
        bronzeParams.put("order_amount", 100.0);

        mockMvc.perform(post("/api/v1/rpc/calculate_discount")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(bronzeParams)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value(5.00));
    }

    @Test
    @Order(44)
    void shouldExecuteTableReturningFunctionWithEnumParameter() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("tier_param", "gold");

        mockMvc.perform(post("/api/v1/rpc/get_users_by_tier")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].result.value").exists())
                .andExpect(jsonPath("$[0].result.value").value(containsString("John")));
    }

    @Test
    @Order(45)
    void shouldExecuteFunctionWithMultipleParameters() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("user_id_param", 1);
        params.put("min_amount", 100.0);

        mockMvc.perform(post("/api/v1/rpc/get_order_total")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").exists())
                .andExpect(jsonPath("$.result.value").value(containsString("1")))
                .andExpect(jsonPath("$.result.value").value(containsString("999.99")));
    }

    @Test
    @Order(46)
    void shouldReturnErrorForNonExistentRpcFunction() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("param1", "value1");

        mockMvc.perform(post("/api/v1/rpc/non_existent_function")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value(containsString("Failed to execute function")));
    }

    @Test
    @Order(47)
    void shouldReturnErrorForMissingRequiredParameter() throws Exception {
        Map<String, Object> params = new HashMap<>();
        // Missing order_amount parameter
        params.put("customer_tier", "gold");

        mockMvc.perform(post("/api/v1/rpc/calculate_discount")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params)))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value(containsString("Failed to execute function")));
    }

    @Test
    @Order(48)
    void shouldHandleEmptyParametersForParameterlessFunction() throws Exception {
        // Create a parameterless function for testing
        jdbcTemplate.execute("""
            CREATE OR REPLACE FUNCTION get_current_timestamp_text()
            RETURNS text AS $$
                SELECT CURRENT_TIMESTAMP::text;
            $$ LANGUAGE SQL IMMUTABLE
        """);

        mockMvc.perform(post("/api/v1/rpc/get_current_timestamp_text")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").exists());

        jdbcTemplate.execute("DROP FUNCTION IF EXISTS get_current_timestamp_text() CASCADE");
    }

    @Test
    @Order(49)
    void shouldCacheRpcFunctionMetadata() throws Exception {
        // First call - should cache metadata
        Map<String, Object> params1 = new HashMap<>();
        params1.put("customer_tier", "gold");
        params1.put("order_amount", 100.0);

        mockMvc.perform(post("/api/v1/rpc/calculate_discount")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params1)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value(15.00));

        // Second call with different parameters - should use cached metadata
        Map<String, Object> params2 = new HashMap<>();
        params2.put("customer_tier", "platinum");
        params2.put("order_amount", 200.0);

        mockMvc.perform(post("/api/v1/rpc/calculate_discount")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params2)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value(40.00));

        // Both calls should succeed, verifying cache works correctly
    }

    @Test
    @Order(50)
    void shouldHandleEnumTypeParameterCorrectly() throws Exception {
        // Test with silver tier enum
        Map<String, Object> params = new HashMap<>();
        params.put("tier_param", "silver");

        MvcResult result = mockMvc.perform(post("/api/v1/rpc/get_users_by_tier")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params)))
                .andExpect(status().isOk())
                .andReturn();

        String response = result.getResponse().getContentAsString();
        System.out.println("===== ENUM PARAMETER TEST RESPONSE =====");
        System.out.println(response);
        System.out.println("=========================================");

        // Verify response contains Jane (who has silver tier)
        mockMvc.perform(post("/api/v1/rpc/get_users_by_tier")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].result.value").value(containsString("Jane")));
    }

    @Test
    @Order(51)
    void shouldHandleDifferentDataTypes() throws Exception {
        // Create function with various data types
        jdbcTemplate.execute("""
            CREATE OR REPLACE FUNCTION test_data_types(
                int_param integer,
                text_param text,
                bool_param boolean,
                decimal_param decimal
            )
            RETURNS TABLE(
                int_result integer,
                text_result text,
                bool_result boolean,
                decimal_result decimal
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT int_param, text_param, bool_param, decimal_param;
            END;
            $$ LANGUAGE plpgsql IMMUTABLE
        """);

        Map<String, Object> params = new HashMap<>();
        params.put("int_param", 42);
        params.put("text_param", "test string");
        params.put("bool_param", true);
        params.put("decimal_param", 99.99);

        mockMvc.perform(post("/api/v1/rpc/test_data_types")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").exists())
                .andExpect(jsonPath("$.result.value").value(containsString("42")))
                .andExpect(jsonPath("$.result.value").value(containsString("test string")))
                .andExpect(jsonPath("$.result.value").value(containsString("99.99")));

        jdbcTemplate.execute("DROP FUNCTION IF EXISTS test_data_types(integer, text, boolean, decimal) CASCADE");
    }

    @Test
    @Order(52)
    void shouldHandleNullParameters() throws Exception {
        // Create function that handles nulls
        jdbcTemplate.execute("""
            CREATE OR REPLACE FUNCTION calculate_with_default(
                value1 decimal,
                value2 decimal
            )
            RETURNS decimal AS $$
            BEGIN
                RETURN COALESCE(value1, 0) + COALESCE(value2, 0);
            END;
            $$ LANGUAGE plpgsql IMMUTABLE
        """);

        Map<String, Object> params = new HashMap<>();
        params.put("value1", 10.0);
        params.put("value2", null);

        mockMvc.perform(post("/api/v1/rpc/calculate_with_default")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(toJson(params)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.result").value(10.0));

        jdbcTemplate.execute("DROP FUNCTION IF EXISTS calculate_with_default(decimal, decimal) CASCADE");
    }

    private String toJson(Object obj) throws JsonProcessingException {
        return objectMapper.writeValueAsString(obj);
    }
}
