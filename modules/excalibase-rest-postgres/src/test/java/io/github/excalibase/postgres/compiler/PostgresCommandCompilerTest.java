package io.github.excalibase.postgres.compiler;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.service.TypeConversionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("PostgresCommandCompiler")
class PostgresCommandCompilerTest {

    @Mock(lenient = true)
    private TypeConversionService typeConversionService;

    private PostgresCommandCompiler compiler;

    private TableInfo customersTable;
    private TableInfo ordersTable;

    @BeforeEach
    void setUp() {
        // Default: return placeholder "?" (no casting needed for simple types)
        when(typeConversionService.buildPlaceholderWithCast(anyString(), any()))
                .thenReturn("?");
        when(typeConversionService.convertValueToColumnType(anyString(), any(), any()))
                .thenAnswer(inv -> inv.getArgument(1));

        compiler = new PostgresCommandCompiler(typeConversionService);

        customersTable = new TableInfo(
                "customers",
                List.of(
                        new ColumnInfo("customer_id", "integer", true, false),
                        new ColumnInfo("name", "varchar", false, false),
                        new ColumnInfo("email", "varchar", false, true),
                        new ColumnInfo("tier", "customer_tier", false, true)
                ),
                List.of()
        );

        ordersTable = new TableInfo(
                "orders",
                List.of(
                        new ColumnInfo("order_id", "integer", true, false),
                        new ColumnInfo("customer_id", "integer", false, false),
                        new ColumnInfo("status", "order_status", false, true),
                        new ColumnInfo("total", "decimal", false, true)
                ),
                List.of()
        );
    }

    // ─── insert ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("insert_generatesInsertReturning")
    void insert_generatesInsertReturning() {
        Map<String, Object> data = Map.of("name", "Alice", "email", "alice@example.com");

        CompiledQuery q = compiler.insert("customers", customersTable, data);

        assertThat(q.sql()).startsWith("INSERT INTO \"customers\"");
        assertThat(q.sql()).contains("RETURNING *");
        assertThat(q.sql()).contains("\"name\"");
        assertThat(q.sql()).contains("\"email\"");
        assertThat(q.sql()).contains("VALUES");
        assertThat(q.params()).hasSize(2);
        assertThat(q.hasCountWindow()).isFalse();
    }

    @Test
    @DisplayName("insert_quotesColumnNames")
    void insert_quotesColumnNames() {
        Map<String, Object> data = Map.of("customer_id", 1);

        CompiledQuery q = compiler.insert("customers", customersTable, data);

        // Column names must be double-quoted for SQL injection safety
        assertThat(q.sql()).contains("\"customer_id\"");
    }

    @Test
    @DisplayName("insert_throwsOnNullData")
    void insert_throwsOnNullData() {
        assertThatThrownBy(() -> compiler.insert("customers", customersTable, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("insert_throwsOnEmptyData")
    void insert_throwsOnEmptyData() {
        assertThatThrownBy(() -> compiler.insert("customers", customersTable, Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ─── bulkInsert ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("bulkInsert_generatesMultiRowValues")
    void bulkInsert_generatesMultiRowValues() {
        List<Map<String, Object>> rows = List.of(
                Map.of("name", "Alice", "email", "a@example.com"),
                Map.of("name", "Bob", "email", "b@example.com"),
                Map.of("name", "Carol", "email", "c@example.com")
        );

        CompiledQuery q = compiler.bulkInsert("customers", customersTable, rows);

        assertThat(q.sql()).startsWith("INSERT INTO \"customers\"");
        assertThat(q.sql()).contains("RETURNING *");
        // 3 rows → 3 value groups
        long valueGroupCount = q.sql().chars().filter(c -> c == '(').count();
        // At least 3 opening parens for value groups (plus the column list)
        assertThat(valueGroupCount).isGreaterThanOrEqualTo(4L); // col list + 3 value groups
        // Total params: 3 rows × 2 columns = 6
        assertThat(q.params()).hasSize(6);
    }

    @Test
    @DisplayName("bulkInsert_throwsOnEmptyList")
    void bulkInsert_throwsOnEmptyList() {
        assertThatThrownBy(() -> compiler.bulkInsert("customers", customersTable, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ─── update ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("update_generatesUpdateWithPkWhere")
    void update_generatesUpdateWithPkWhere() {
        Map<String, Object> data = Map.of("name", "Alice Updated", "email", "new@example.com");

        CompiledQuery q = compiler.update("customers", customersTable, "42", data);

        assertThat(q.sql()).startsWith("UPDATE \"customers\"");
        assertThat(q.sql()).contains("SET");
        assertThat(q.sql()).contains("WHERE");
        assertThat(q.sql()).contains("RETURNING *");
        assertThat(q.sql()).contains("\"customer_id\"");
        // data params + 1 for the WHERE pk value
        assertThat(q.params()).hasSize(3);
        assertThat(q.hasCountWindow()).isFalse();
    }

    @Test
    @DisplayName("update_throwsOnNullData")
    void update_throwsOnNullData() {
        assertThatThrownBy(() -> compiler.update("customers", customersTable, "1", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ─── patch ────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("patch_generatesPartialUpdate")
    void patch_generatesPartialUpdate() {
        // Only provide one column — patch should only update that column
        Map<String, Object> data = Map.of("name", "Patched Name");

        CompiledQuery q = compiler.patch("customers", customersTable, "7", data);

        assertThat(q.sql()).startsWith("UPDATE \"customers\"");
        assertThat(q.sql()).contains("\"name\"");
        assertThat(q.sql()).contains("WHERE");
        assertThat(q.sql()).contains("RETURNING *");
        // 1 data param + 1 WHERE pk param
        assertThat(q.params()).hasSize(2);
    }

    // ─── delete ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("delete_generatesDeleteWithPk")
    void delete_generatesDeleteWithPk() {
        CompiledQuery q = compiler.delete("customers", customersTable, "99");

        assertThat(q.sql()).startsWith("DELETE FROM \"customers\"");
        assertThat(q.sql()).contains("WHERE");
        assertThat(q.sql()).contains("\"customer_id\"");
        assertThat(q.sql()).contains("RETURNING *");
        assertThat(q.params()).hasSize(1);
        assertThat(q.hasCountWindow()).isFalse();
    }

    // ─── upsert ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("upsert_generatesOnConflict")
    void upsert_generatesOnConflict() {
        Map<String, Object> data = Map.of("customer_id", 1, "name", "Alice", "email", "a@example.com");

        CompiledQuery q = compiler.upsert("customers", customersTable, data, List.of("customer_id"));

        assertThat(q.sql()).startsWith("INSERT INTO \"customers\"");
        assertThat(q.sql()).contains("ON CONFLICT");
        assertThat(q.sql()).contains("\"customer_id\"");
        assertThat(q.sql()).contains("DO UPDATE SET");
        assertThat(q.sql()).contains("RETURNING *");
        assertThat(q.params()).hasSize(3);
    }

    @Test
    @DisplayName("upsert_handlesDomainTypes_withTypeCast")
    void upsert_handlesDomainTypes_withTypeCast() {
        // For specific column "tier" return a cast placeholder
        when(typeConversionService.buildPlaceholderWithCast("tier", customersTable))
                .thenReturn("?::customer_tier");

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("customer_id", 1);
        data.put("tier", "gold");

        CompiledQuery q = compiler.upsert("customers", customersTable, data, List.of("customer_id"));

        assertThat(q.sql()).contains("?::customer_tier");
    }

    @Test
    @DisplayName("upsert_throwsOnNullData")
    void upsert_throwsOnNullData() {
        assertThatThrownBy(() -> compiler.upsert("customers", customersTable, null, List.of("customer_id")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("upsert_throwsOnMissingConflictColumn")
    void upsert_throwsOnMissingConflictColumn() {
        Map<String, Object> data = Map.of("name", "Alice");
        assertThatThrownBy(() -> compiler.upsert("customers", customersTable, data, (List<String>) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ─── composite key (patch / delete) ──────────────────────────────────────

    @Test
    @DisplayName("patch_compositeKey_generatesWhereWithBothPkColumns")
    void patch_compositeKey_generatesWhereWithBothPkColumns() {
        TableInfo orderItemsTable = new TableInfo(
                "order_items",
                List.of(
                        new ColumnInfo("order_id", "integer", true, false),
                        new ColumnInfo("product_id", "integer", true, false),
                        new ColumnInfo("quantity", "integer", false, false)
                ),
                List.of()
        );
        Map<String, Object> data = Map.of("quantity", 10);

        CompiledQuery q = compiler.patch("order_items", orderItemsTable, "1,2", data);

        assertThat(q.sql()).startsWith("UPDATE \"order_items\"");
        assertThat(q.sql()).contains("WHERE");
        // Both PK columns must appear in WHERE clause
        assertThat(q.sql()).contains("\"order_id\"");
        assertThat(q.sql()).contains("\"product_id\"");
        assertThat(q.sql()).contains("RETURNING *");
        // 1 data param + 2 WHERE pk params
        assertThat(q.params()).hasSize(3);
        assertThat(q.params()[1]).isEqualTo("1");
        assertThat(q.params()[2]).isEqualTo("2");
    }

    @Test
    @DisplayName("delete_compositeKey_generatesWhereWithBothPkColumns")
    void delete_compositeKey_generatesWhereWithBothPkColumns() {
        TableInfo orderItemsTable = new TableInfo(
                "order_items",
                List.of(
                        new ColumnInfo("order_id", "integer", true, false),
                        new ColumnInfo("product_id", "integer", true, false),
                        new ColumnInfo("quantity", "integer", false, false)
                ),
                List.of()
        );

        CompiledQuery q = compiler.delete("order_items", orderItemsTable, "1,3");

        assertThat(q.sql()).startsWith("DELETE FROM \"order_items\"");
        assertThat(q.sql()).contains("WHERE");
        assertThat(q.sql()).contains("\"order_id\"");
        assertThat(q.sql()).contains("\"product_id\"");
        assertThat(q.sql()).contains("RETURNING *");
        // 2 params for the 2 composite PK columns
        assertThat(q.params()).hasSize(2);
        assertThat(q.params()[0]).isEqualTo("1");
        assertThat(q.params()[1]).isEqualTo("3");
    }
}
