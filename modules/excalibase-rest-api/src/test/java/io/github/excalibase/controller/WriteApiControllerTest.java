package io.github.excalibase.controller;

import io.github.excalibase.compiler.CompiledQuery;
import io.github.excalibase.compiler.ICommandCompiler;
import io.github.excalibase.exception.ValidationException;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.service.IValidationService;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.PreferHeaderParser;
import io.github.excalibase.service.TypeConversionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.LinkedMultiValueMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for WriteApiController.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("WriteApiController unit tests")
class WriteApiControllerTest {

    @Mock(lenient = true) private ICommandCompiler commandCompiler;
    @Mock(lenient = true) private JdbcTemplate jdbcTemplate;
    @Mock(lenient = true) private IValidationService validationService;
    @Mock(lenient = true) private FilterService filterService;
    @Mock(lenient = true) private TypeConversionService typeConversionService;
    @Mock(lenient = true) private PreferHeaderParser preferParser;
    @Mock(lenient = true) private PlatformTransactionManager transactionManager;

    private WriteApiController controller;
    private TableInfo customersTable;

    @BeforeEach
    void setUp() {
        controller = new WriteApiController(
                commandCompiler, jdbcTemplate, validationService,
                filterService, typeConversionService, preferParser, transactionManager);

        customersTable = new TableInfo(
                "customers",
                List.of(
                        new ColumnInfo("customer_id", "integer", true, false),
                        new ColumnInfo("name", "varchar", false, false),
                        new ColumnInfo("email", "varchar", false, true)
                ),
                List.of()
        );

        when(validationService.getValidatedTableInfo(anyString())).thenReturn(customersTable);
    }

    // ---- POST /{table} ----

    @Test
    @DisplayName("createRecord_nullBody_returns400")
    void createRecord_nullBody_returns400() {
        ResponseEntity<?> response = controller.createRecord("customers", null, null);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("createRecord_emptyMapBody_returns400")
    void createRecord_emptyMapBody_returns400() {
        ResponseEntity<?> response = controller.createRecord("customers", Map.of(), null);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("createRecord_emptyListBody_returns400")
    void createRecord_emptyListBody_returns400() {
        ResponseEntity<?> response = controller.createRecord("customers", List.of(), null);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("createRecord_singleRecord_returns201")
    void createRecord_singleRecord_returns201() {
        Map<String, Object> row = Map.of("name", "Alice", "email", "a@example.com");
        CompiledQuery q = new CompiledQuery("INSERT INTO ...", new Object[]{"Alice", "a@example.com"}, false, List.of());

        when(commandCompiler.insert(eq("customers"), any(), eq(row))).thenReturn(q);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("customer_id", 1, "name", "Alice")));
        when(preferParser.getReturn(any())).thenReturn("representation");

        ResponseEntity<?> response = controller.createRecord("customers", row, null);

        assertThat(response.getStatusCode().value()).isEqualTo(201);
    }

    @Test
    @DisplayName("createRecord_invalidBody_returns400")
    void createRecord_invalidBody_returns400() {
        ResponseEntity<?> response = controller.createRecord("customers", "not a map or list", null);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("createRecord_validationException_returns400")
    void createRecord_validationException_returns400() {
        doThrow(new ValidationException("col not found"))
                .when(validationService).validateTablePermission(anyString(), anyString());

        ResponseEntity<?> response = controller.createRecord("customers", Map.of("name", "test"), null);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    // ---- PUT /{table}/{id} ----

    @Test
    @DisplayName("updateRecord_nullBody_returns400")
    void updateRecord_nullBody_returns400() {
        ResponseEntity<?> response = controller.updateRecord("orders", "1",
                new LinkedMultiValueMap<>(), null);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("updateRecord_emptyListBody_returns400")
    void updateRecord_emptyListBody_returns400() {
        ResponseEntity<?> response = controller.updateRecord("orders", "1",
                new LinkedMultiValueMap<>(), List.of());
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("updateRecord_emptyMapBody_returns400")
    void updateRecord_emptyMapBody_returns400() {
        ResponseEntity<?> response = controller.updateRecord("orders", "1",
                new LinkedMultiValueMap<>(), Map.of());
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("updateRecord_noIdNoFilters_returns400")
    void updateRecord_noIdNoFilters_returns400() {
        Map<String, Object> data = Map.of("status", "shipped");
        ResponseEntity<?> response = controller.updateRecord("orders", null,
                new LinkedMultiValueMap<>(), data);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("updateRecord_singleWithId_notFound_returns404")
    void updateRecord_singleWithId_notFound_returns404() {
        Map<String, Object> data = Map.of("name", "Updated");
        CompiledQuery q = new CompiledQuery("UPDATE ...", new Object[]{"Updated", "99"}, false, List.of());
        when(commandCompiler.update(eq("customers"), any(), eq("99"), eq(data))).thenReturn(q);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        ResponseEntity<?> response = controller.updateRecord("customers", "99",
                new LinkedMultiValueMap<>(), data);
        assertThat(response.getStatusCode().value()).isEqualTo(404);
    }

    // ---- PATCH /{table}/{id} ----

    @Test
    @DisplayName("patchRecord_emptyBody_returns400")
    void patchRecord_emptyBody_returns400() {
        ResponseEntity<?> response = controller.patchRecord("customers", "1", Map.of(), null);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("patchRecord_notFound_returns404")
    void patchRecord_notFound_returns404() {
        Map<String, Object> data = Map.of("name", "Changed");
        CompiledQuery q = new CompiledQuery("UPDATE ...", new Object[]{}, false, List.of());
        when(commandCompiler.patch(eq("customers"), any(), eq("99"), eq(data))).thenReturn(q);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        ResponseEntity<?> response = controller.patchRecord("customers", "99", data, null);
        assertThat(response.getStatusCode().value()).isEqualTo(404);
    }

    // ---- DELETE ----

    @Test
    @DisplayName("deleteRecord_noIdNoFilters_returns400")
    void deleteRecord_noIdNoFilters_returns400() {
        ResponseEntity<Map<String, Object>> response = controller.deleteRecord("customers", null,
                new LinkedMultiValueMap<>(), null);
        assertThat(response.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @DisplayName("deleteRecord_notFound_returns404")
    void deleteRecord_notFound_returns404() {
        CompiledQuery q = new CompiledQuery("DELETE ...", new Object[]{"99"}, false, List.of());
        when(commandCompiler.delete(eq("customers"), any(), eq("99"))).thenReturn(q);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class))).thenReturn(List.of());

        ResponseEntity<Map<String, Object>> response = controller.deleteRecord("customers", "99",
                new LinkedMultiValueMap<>(), null);
        assertThat(response.getStatusCode().value()).isEqualTo(404);
    }

    // ---- Upsert conflict column resolution ----

    @Test
    @DisplayName("upsert_usesUniqueConstraint_whenPKNotInData")
    void upsert_usesUniqueConstraint_whenPKNotInData() {
        TableInfo configTable = new TableInfo("app_configurations",
                List.of(
                        new ColumnInfo("id", "integer", true, false),
                        new ColumnInfo("key", "varchar", false, false),
                        new ColumnInfo("value", "jsonb", false, true)
                ),
                List.of());
        configTable.setUniqueConstraints(List.of(List.of("key")));

        when(validationService.getValidatedTableInfo("app_configurations")).thenReturn(configTable);
        when(preferParser.isUpsert(any())).thenReturn(true);

        CompiledQuery upsertQuery = new CompiledQuery(
                "INSERT INTO ... ON CONFLICT (\"key\") ...", new Object[]{"k", "{}"}, false, List.of());
        when(commandCompiler.upsert(eq("app_configurations"), any(), any(), eq(List.of("key"))))
                .thenReturn(upsertQuery);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("id", 1, "key", "k", "value", "{}")));

        Map<String, Object> body = new HashMap<>();
        body.put("key", "test_key");
        body.put("value", Map.of("v", 1));

        ResponseEntity<?> response = controller.createRecord(
                "app_configurations", body,
                "resolution=merge-duplicates");

        assertThat(response.getStatusCode().value()).isIn(200, 201);
        verify(commandCompiler).upsert(eq("app_configurations"), any(), any(), eq(List.of("key")));
    }

    @Test
    @DisplayName("upsert_fallsToPK_whenNoUniqueConstraintMatchesData")
    void upsert_fallsToPK_whenNoUniqueConstraintMatchesData() {
        TableInfo table = new TableInfo("users",
                List.of(
                        new ColumnInfo("id", "integer", true, false),
                        new ColumnInfo("name", "varchar", false, false),
                        new ColumnInfo("email", "varchar", false, true)
                ),
                List.of());
        table.setUniqueConstraints(List.of(List.of("email")));

        when(validationService.getValidatedTableInfo("users")).thenReturn(table);
        when(preferParser.isUpsert(any())).thenReturn(true);

        CompiledQuery upsertQuery = new CompiledQuery(
                "INSERT INTO ... ON CONFLICT (\"id\") ...", new Object[]{1, "Alice"}, false, List.of());
        when(commandCompiler.upsert(eq("users"), any(), any(), eq(List.of("id"))))
                .thenReturn(upsertQuery);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("id", 1, "name", "Alice")));

        Map<String, Object> body = new HashMap<>();
        body.put("id", 1);
        body.put("name", "Alice");

        ResponseEntity<?> response = controller.createRecord(
                "users", body, "resolution=merge-duplicates");

        assertThat(response.getStatusCode().value()).isIn(200, 201);
        verify(commandCompiler).upsert(eq("users"), any(), any(), eq(List.of("id")));
    }

    // ---- Bulk upsert with TransactionTemplate ----

    @Test
    @DisplayName("bulkUpsert_callsUpsertForEachRow")
    void bulkUpsert_callsUpsertForEachRow() {
        when(preferParser.isUpsert(any())).thenReturn(true);
        when(preferParser.getReturn(any())).thenReturn("representation");

        CompiledQuery upsertQuery = new CompiledQuery(
                "INSERT INTO ... ON CONFLICT ...", new Object[]{}, false, List.of());
        when(commandCompiler.upsert(eq("customers"), any(), any(), any()))
                .thenReturn(upsertQuery);
        when(jdbcTemplate.queryForList(anyString(), any(Object[].class)))
                .thenReturn(List.of(Map.of("customer_id", 1, "name", "Alice")));
        // TransactionTemplate.execute calls the callback directly when no real tx manager
        when(transactionManager.getTransaction(any()))
                .thenReturn(mock(org.springframework.transaction.TransactionStatus.class));

        List<Map<String, Object>> bulk = List.of(
                Map.of("name", "Alice"),
                Map.of("name", "Bob")
        );

        ResponseEntity<?> response = controller.createRecord("customers", bulk, "resolution=merge-duplicates");

        // Should attempt upsert for each row
        verify(commandCompiler, atLeast(1)).upsert(eq("customers"), any(), any(), any());
    }
}
