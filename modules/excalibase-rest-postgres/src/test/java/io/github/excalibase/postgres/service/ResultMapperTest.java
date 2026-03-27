package io.github.excalibase.postgres.service;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.MappedResult;
import io.github.excalibase.model.TableInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class ResultMapperTest {

    private ResultMapper resultMapper;
    private TableInfo tableInfo;

    @BeforeEach
    void setUp() {
        resultMapper = new ResultMapper(new com.fasterxml.jackson.databind.ObjectMapper());
        tableInfo = new TableInfo(
                "orders",
                List.of(
                        new ColumnInfo("order_id", "integer", true, false),
                        new ColumnInfo("customer_id", "integer", false, false),
                        new ColumnInfo("total", "decimal", false, false)
                ),
                List.of()
        );
    }

    // ─── 1. Parse JSON object column ─────────────────────────────────────────

    @Test
    @DisplayName("mapResults_parsesJsonColumn: JSON string → Map conversion")
    void mapResults_parsesJsonColumn() {
        Map<String, Object> row = new HashMap<>();
        row.put("order_id", 1);
        row.put("total", 100.0);
        row.put("customers", "{\"customer_id\":1,\"name\":\"Alice\"}");

        CompiledQuery query = new CompiledQuery(
                "SELECT ...", new Object[0], false, List.of("customers"));

        MappedResult result = resultMapper.mapResults(List.of(row), query, tableInfo);

        assertThat(result.records()).hasSize(1);
        Object customersValue = result.records().get(0).get("customers");
        assertThat(customersValue).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> customerMap = (Map<String, Object>) customersValue;
        assertThat(customerMap.get("name")).isEqualTo("Alice");
    }

    // ─── 2. Parse JSON array column ──────────────────────────────────────────

    @Test
    @DisplayName("mapResults_parsesJsonArrayColumn: JSON array string → List conversion")
    void mapResults_parsesJsonArrayColumn() {
        Map<String, Object> row = new HashMap<>();
        row.put("order_id", 1);
        row.put("items", "[{\"name\":\"Laptop\"},{\"name\":\"Mouse\"}]");

        CompiledQuery query = new CompiledQuery(
                "SELECT ...", new Object[0], false, List.of("items"));

        MappedResult result = resultMapper.mapResults(List.of(row), query, tableInfo);

        assertThat(result.records()).hasSize(1);
        Object itemsValue = result.records().get(0).get("items");
        assertThat(itemsValue).isInstanceOf(List.class);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> itemsList = (List<Map<String, Object>>) itemsValue;
        assertThat(itemsList).hasSize(2);
    }

    // ─── 3. Extract _total_count ─────────────────────────────────────────────

    @Test
    @DisplayName("mapResults_extractsTotalCount: _total_count removed from row data")
    void mapResults_extractsTotalCount() {
        Map<String, Object> row = new HashMap<>();
        row.put("order_id", 1);
        row.put("_total_count", 42L);

        CompiledQuery query = new CompiledQuery(
                "SELECT ...", new Object[0], true, List.of());

        MappedResult result = resultMapper.mapResults(List.of(row), query, tableInfo);

        assertThat(result.records()).hasSize(1);
        // _total_count should be stripped from the row data
        assertThat(result.records().get(0)).doesNotContainKey("_total_count");
    }

    // ─── 4. Handle null JSON column ──────────────────────────────────────────

    @Test
    @DisplayName("mapResults_handlesNullJsonColumn: null JSON value stays null")
    void mapResults_handlesNullJsonColumn() {
        Map<String, Object> row = new HashMap<>();
        row.put("order_id", 1);
        row.put("customers", null);

        CompiledQuery query = new CompiledQuery(
                "SELECT ...", new Object[0], false, List.of("customers"));

        MappedResult result = resultMapper.mapResults(List.of(row), query, tableInfo);

        assertThat(result.records()).hasSize(1);
        assertThat(result.records().get(0).get("customers")).isNull();
    }

    // ─── 5. Empty result set ─────────────────────────────────────────────────

    @Test
    @DisplayName("mapResults_emptyRows: returns empty list")
    void mapResults_emptyRows() {
        CompiledQuery query = new CompiledQuery(
                "SELECT ...", new Object[0], false, List.of());

        MappedResult result = resultMapper.mapResults(List.of(), query, tableInfo);

        assertThat(result.records()).isEmpty();
    }

    // ─── 6. Multiple rows ────────────────────────────────────────────────────

    @Test
    @DisplayName("mapResults_multipleRows: all rows processed independently")
    void mapResults_multipleRows() {
        Map<String, Object> row1 = new HashMap<>();
        row1.put("order_id", 1);
        row1.put("_total_count", 2L);
        row1.put("customers", "{\"name\":\"Alice\"}");

        Map<String, Object> row2 = new HashMap<>();
        row2.put("order_id", 2);
        row2.put("_total_count", 2L);
        row2.put("customers", "{\"name\":\"Bob\"}");

        CompiledQuery query = new CompiledQuery(
                "SELECT ...", new Object[0], true, List.of("customers"));

        MappedResult result = resultMapper.mapResults(
                new ArrayList<>(List.of(row1, row2)), query, tableInfo);

        assertThat(result.records()).hasSize(2);
        assertThat(result.records().get(0)).doesNotContainKey("_total_count");
        assertThat(result.records().get(1)).doesNotContainKey("_total_count");

        @SuppressWarnings("unchecked")
        Map<String, Object> cust1 = (Map<String, Object>) result.records().get(0).get("customers");
        assertThat(cust1.get("name")).isEqualTo("Alice");

        @SuppressWarnings("unchecked")
        Map<String, Object> cust2 = (Map<String, Object>) result.records().get(1).get("customers");
        assertThat(cust2.get("name")).isEqualTo("Bob");
    }

    // ─── 7. Get total count ───────────────────────────────────────────────────

    @Test
    @DisplayName("mapResults_returnsMappedResultWithTotalCount")
    void mapResults_returnsMappedResultWithTotalCount() {
        Map<String, Object> row1 = new HashMap<>();
        row1.put("order_id", 1);
        row1.put("_total_count", 50L);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("order_id", 2);
        row2.put("_total_count", 50L);

        CompiledQuery query = new CompiledQuery(
                "SELECT ...", new Object[0], true, List.of());

        MappedResult result = resultMapper.mapResults(new ArrayList<>(List.of(row1, row2)), query, tableInfo);

        assertThat(result.totalCount()).isEqualTo(50L);
        assertThat(result.records()).hasSize(2);
    }

    // ─── 8. PGobject-like wrapper ─────────────────────────────────────────────

    @Test
    @DisplayName("mapResults_handlesPGobjectLikeWrapper: objects with getValue() are parsed")
    void mapResults_handlesPGobjectLikeWrapper() {
        // Simulate a PGobject-like wrapper (has getValue() method returning JSON)
        Object pgLike = new Object() {
            @SuppressWarnings("unused")
            public String getValue() {
                return "{\"name\":\"Alice\",\"tier\":\"gold\"}";
            }
            @Override
            public String toString() {
                return "{\"name\":\"Alice\",\"tier\":\"gold\"}";
            }
        };

        Map<String, Object> row = new HashMap<>();
        row.put("order_id", 1);
        row.put("customers", pgLike);

        CompiledQuery query = new CompiledQuery(
                "SELECT ...", new Object[0], false, List.of("customers"));

        MappedResult result = resultMapper.mapResults(List.of(row), query, tableInfo);

        assertThat(result.records()).hasSize(1);
        Object customersValue = result.records().get(0).get("customers");
        assertThat(customersValue).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> customerMap = (Map<String, Object>) customersValue;
        assertThat(customerMap.get("name")).isEqualTo("Alice");
    }

    // ─── 9. JSON with empty array literal ────────────────────────────────────

    @Test
    @DisplayName("mapResults_emptyJsonArrayLiteral: empty JSON array string → empty List")
    void mapResults_emptyJsonArrayLiteral() {
        Map<String, Object> row = new HashMap<>();
        row.put("order_id", 1);
        row.put("orders", "[]");

        CompiledQuery query = new CompiledQuery(
                "SELECT ...", new Object[0], false, List.of("orders"));

        MappedResult result = resultMapper.mapResults(List.of(row), query, tableInfo);

        Object ordersValue = result.records().get(0).get("orders");
        assertThat(ordersValue).isInstanceOf(List.class);
        assertThat((List<?>) ordersValue).isEmpty();
    }

    // ─── mapJsonBody() tests (new JSON-body path) ────────────────────────────

    @Test
    @DisplayName("mapJsonBody_parsesBodyColumnAsJsonArray")
    void mapJsonBody_parsesBodyColumnAsJsonArray() {
        Map<String, Object> singleRow = new HashMap<>();
        singleRow.put("body", "[{\"order_id\":1,\"total\":99.99},{\"order_id\":2,\"total\":49.00}]");

        MappedResult result = resultMapper.mapJsonBody(singleRow, tableInfo);

        assertThat(result.records()).hasSize(2);
        assertThat(result.records().get(0).get("order_id")).isEqualTo(1);
        assertThat(result.records().get(1).get("order_id")).isEqualTo(2);
    }

    @Test
    @DisplayName("mapJsonBody_emptyJsonArrayBody_returnsEmptyList")
    void mapJsonBody_emptyJsonArrayBody_returnsEmptyList() {
        Map<String, Object> singleRow = new HashMap<>();
        singleRow.put("body", "[]");

        MappedResult result = resultMapper.mapJsonBody(singleRow, tableInfo);

        assertThat(result.records()).isEmpty();
    }

    @Test
    @DisplayName("mapJsonBody_nullRow_returnsEmptyList")
    void mapJsonBody_nullRow_returnsEmptyList() {
        MappedResult result = resultMapper.mapJsonBody(null, tableInfo);
        assertThat(result.records()).isEmpty();
    }

    @Test
    @DisplayName("mapJsonBody_returnsMappedResultWithCount")
    void mapJsonBody_returnsMappedResultWithCount() {
        Map<String, Object> singleRow = new HashMap<>();
        singleRow.put("body", "[{\"order_id\":1}]");
        singleRow.put("total_count", 42L);

        MappedResult result = resultMapper.mapJsonBody(singleRow, tableInfo);

        assertThat(result.records()).hasSize(1);
        assertThat(result.totalCount()).isEqualTo(42L);
    }

    @Test
    @DisplayName("mapJsonBody_noTotalCount_returnsMinusOne")
    void mapJsonBody_noTotalCount_returnsMinusOne() {
        Map<String, Object> singleRow = new HashMap<>();
        singleRow.put("body", "[{\"order_id\":1}]");

        MappedResult result = resultMapper.mapJsonBody(singleRow, tableInfo);

        assertThat(result.totalCount()).isEqualTo(-1L);
    }

    @Test
    @DisplayName("mapJsonBody_threadSafe_noCrossContamination")
    void mapJsonBody_threadSafe_noCrossContamination() throws Exception {
        // Thread 1: total_count = 100, Thread 2: total_count = 999
        // With mutable state, one could overwrite the other.
        // With MappedResult, each thread gets its own result — no race.
        Map<String, Object> row1 = new HashMap<>();
        row1.put("body", "[{\"order_id\":1}]");
        row1.put("total_count", 100L);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("body", "[{\"order_id\":2}]");
        row2.put("total_count", 999L);

        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicReference<MappedResult> result1 = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<MappedResult> result2 = new java.util.concurrent.atomic.AtomicReference<>();

        Thread t1 = new Thread(() -> {
            try { latch.await(); } catch (InterruptedException ignored) {}
            result1.set(resultMapper.mapJsonBody(row1, tableInfo));
        });
        Thread t2 = new Thread(() -> {
            try { latch.await(); } catch (InterruptedException ignored) {}
            result2.set(resultMapper.mapJsonBody(row2, tableInfo));
        });

        t1.start();
        t2.start();
        latch.countDown(); // start both simultaneously
        t1.join(5000);
        t2.join(5000);

        // Each thread's result is independent — no cross-contamination
        assertThat(result1.get().totalCount()).isEqualTo(100L);
        assertThat(result2.get().totalCount()).isEqualTo(999L);
    }

    @Test
    @DisplayName("mapJsonBody_handlesPGobjectBodyColumn")
    void mapJsonBody_handlesPGobjectBodyColumn() {
        // Simulate PGobject wrapping the jsonb value
        Object pgLike = new Object() {
            @SuppressWarnings("unused")
            public String getValue() {
                return "[{\"order_id\":7,\"total\":10.0}]";
            }

            @Override
            public String toString() {
                return "[{\"order_id\":7,\"total\":10.0}]";
            }
        };

        Map<String, Object> singleRow = new HashMap<>();
        singleRow.put("body", pgLike);

        MappedResult result = resultMapper.mapJsonBody(singleRow, tableInfo);

        assertThat(result.records()).hasSize(1);
        assertThat(result.records().get(0).get("order_id")).isEqualTo(7);
    }

    @Test
    @DisplayName("mapJsonBody_nullBody_returnsEmptyList")
    void mapJsonBody_nullBody_returnsEmptyList() {
        Map<String, Object> singleRow = new HashMap<>();
        singleRow.put("body", null);

        MappedResult result = resultMapper.mapJsonBody(singleRow, tableInfo);

        assertThat(result.records()).isEmpty();
    }

    @Test
    @DisplayName("mapJsonBody_totalCountAsInteger_coercedToLong")
    void mapJsonBody_totalCountAsInteger_coercedToLong() {
        Map<String, Object> singleRow = new HashMap<>();
        singleRow.put("body", "[{\"order_id\":1}]");
        singleRow.put("total_count", 100); // Integer instead of Long

        MappedResult result = resultMapper.mapJsonBody(singleRow, tableInfo);

        assertThat(result.totalCount()).isEqualTo(100L);
    }
}
