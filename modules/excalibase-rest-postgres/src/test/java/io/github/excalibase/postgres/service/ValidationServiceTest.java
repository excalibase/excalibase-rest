package io.github.excalibase.postgres.service;

import io.github.excalibase.exception.ValidationException;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.TableInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ValidationServiceTest {

    @Mock(lenient = true)
    private JdbcTemplate jdbcTemplate;

    @Mock(lenient = true)
    private DatabaseSchemaService schemaService;

    private ValidationService validationService;

    @BeforeEach
    void setup() {
        lenient().when(jdbcTemplate.queryForObject(
                eq("SELECT current_user"), eq(String.class))).thenReturn("test_user");
        lenient().when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), any(), any())).thenReturn(true);

        validationService = new ValidationService(jdbcTemplate, schemaService);
    }

    // ===== validatePaginationParams =====

    @Test
    void validatePaginationParams_validValues_doesNotThrow() {
        assertDoesNotThrow(() -> validationService.validatePaginationParams(0, 100));
    }

    @Test
    void validatePaginationParams_negativeOffset_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validatePaginationParams(-1, 100));
    }

    @Test
    void validatePaginationParams_offsetBeyondMax_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validatePaginationParams(1_000_001, 100));
    }

    @Test
    void validatePaginationParams_zeroLimit_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validatePaginationParams(0, 0));
    }

    @Test
    void validatePaginationParams_limitExceedsMax_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validatePaginationParams(0, 1001));
    }

    @Test
    void validatePaginationParams_boundaryValues_doesNotThrow() {
        assertDoesNotThrow(() -> validationService.validatePaginationParams(0, 1));
        assertDoesNotThrow(() -> validationService.validatePaginationParams(1_000_000, 1000));
    }

    // ===== validateTableName =====

    @Test
    void validateTableName_validName_doesNotThrow() {
        assertDoesNotThrow(() -> validationService.validateTableName("users"));
        assertDoesNotThrow(() -> validationService.validateTableName("order_items"));
        assertDoesNotThrow(() -> validationService.validateTableName("_private_table"));
        assertDoesNotThrow(() -> validationService.validateTableName("Table123"));
    }

    @Test
    void validateTableName_nullName_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTableName(null));
    }

    @Test
    void validateTableName_emptyName_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTableName(""));
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTableName("   "));
    }

    @Test
    void validateTableName_withSqlInjectionPatterns_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTableName("users; DROP TABLE users--"));
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTableName("users OR 1=1"));
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTableName("users-admin"));
    }

    @Test
    void validateTableName_withSpecialCharacters_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTableName("users.admin"));
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTableName("users'admin"));
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTableName("123users")); // starts with digit
    }

    // ===== getValidatedTableInfo =====

    @Test
    void getValidatedTableInfo_existingTable_returnsTableInfo() {
        TableInfo expectedTable = buildTableInfo("users");
        when(schemaService.getTableSchema()).thenReturn(Map.of("users", expectedTable));

        TableInfo result = validationService.getValidatedTableInfo("users");

        assertNotNull(result);
        assertEquals("users", result.getName());
    }

    @Test
    void getValidatedTableInfo_nonExistentTable_throwsIllegalArgumentException() {
        when(schemaService.getTableSchema()).thenReturn(Map.of());

        assertThrows(IllegalArgumentException.class,
                () -> validationService.getValidatedTableInfo("nonexistent"));
    }

    @Test
    void getValidatedTableInfo_invalidTableName_throwsBeforeDbLookup() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.getValidatedTableInfo("bad-table!"));
        verifyNoInteractions(schemaService);
    }

    // ===== hasTablePermission =====

    @Test
    void hasTablePermission_permissionGranted_returnsTrue() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("users"), eq("SELECT"))).thenReturn(true);

        boolean result = validationService.hasTablePermission("users", "SELECT");

        assertTrue(result);
    }

    @Test
    void hasTablePermission_permissionDenied_returnsFalse() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("admin_table"), eq("DELETE"))).thenReturn(false);

        boolean result = validationService.hasTablePermission("admin_table", "DELETE");

        assertFalse(result);
    }

    @Test
    void hasTablePermission_dbThrowsException_returnsTrue() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("table1"), eq("SELECT")))
                .thenThrow(new RuntimeException("DB unavailable"));

        // Fallback behavior: allow access when permission check fails
        boolean result = validationService.hasTablePermission("table1", "SELECT");

        assertTrue(result);
    }

    @Test
    void hasTablePermission_nullResult_returnsFalse() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("some_table"), eq("INSERT"))).thenReturn(null);

        boolean result = validationService.hasTablePermission("some_table", "INSERT");

        assertFalse(result);
    }

    @Test
    void hasTablePermission_secondCallSameKey_usesCache() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("users"), eq("SELECT"))).thenReturn(true);

        validationService.hasTablePermission("users", "SELECT");
        validationService.hasTablePermission("users", "SELECT");

        // Database queried only once due to cache
        verify(jdbcTemplate, times(1)).queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("users"), eq("SELECT"));
    }

    @Test
    void hasTablePermission_getCurrentUserFails_usesUnknownUser() {
        when(jdbcTemplate.queryForObject(eq("SELECT current_user"), eq(String.class)))
                .thenThrow(new RuntimeException("cannot get user"));
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), any(), any())).thenReturn(true);

        // Should not throw, should fallback gracefully
        boolean result = validationService.hasTablePermission("users", "SELECT");

        assertTrue(result); // Fallback returns true
    }

    // ===== validateTablePermission =====

    @Test
    void validateTablePermission_selectOperation_checksSelectPermission() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("t"), eq("SELECT"))).thenReturn(true);

        assertDoesNotThrow(() -> validationService.validateTablePermission("t", "select"));
        assertDoesNotThrow(() -> validationService.validateTablePermission("t", "read"));
    }

    @Test
    void validateTablePermission_insertOperation_checksInsertPermission() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("t"), eq("INSERT"))).thenReturn(true);

        assertDoesNotThrow(() -> validationService.validateTablePermission("t", "insert"));
        assertDoesNotThrow(() -> validationService.validateTablePermission("t", "create"));
    }

    @Test
    void validateTablePermission_updateOperation_checksUpdatePermission() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("t"), eq("UPDATE"))).thenReturn(true);

        assertDoesNotThrow(() -> validationService.validateTablePermission("t", "update"));
        assertDoesNotThrow(() -> validationService.validateTablePermission("t", "patch"));
        assertDoesNotThrow(() -> validationService.validateTablePermission("t", "put"));
    }

    @Test
    void validateTablePermission_deleteOperation_checksDeletePermission() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("t"), eq("DELETE"))).thenReturn(true);

        assertDoesNotThrow(() -> validationService.validateTablePermission("t", "delete"));
    }

    @Test
    void validateTablePermission_unknownOperation_defaultsToSelect() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("t"), eq("SELECT"))).thenReturn(true);

        assertDoesNotThrow(() -> validationService.validateTablePermission("t", "unknown_op"));
    }

    @Test
    void validateTablePermission_permissionDenied_throwsIllegalArgumentException() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("secure_table"), eq("DELETE"))).thenReturn(false);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> validationService.validateTablePermission("secure_table", "delete"));

        assertTrue(ex.getMessage().contains("Access denied"));
        assertTrue(ex.getMessage().contains("secure_table"));
    }

    // ===== validateColumns =====

    @Test
    void validateColumns_allValidColumns_doesNotThrow() {
        TableInfo tableInfo = buildTableInfo("users");
        assertDoesNotThrow(() ->
                validationService.validateColumns(Set.of("id", "name"), tableInfo));
    }

    @Test
    void validateColumns_invalidColumn_throwsIllegalArgumentException() {
        TableInfo tableInfo = buildTableInfo("users");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> validationService.validateColumns(Set.of("id", "nonexistent"), tableInfo));

        assertTrue(ex.getMessage().contains("nonexistent"));
    }

    @Test
    void validateColumns_emptySet_doesNotThrow() {
        TableInfo tableInfo = buildTableInfo("users");
        assertDoesNotThrow(() -> validationService.validateColumns(Set.of(), tableInfo));
    }

    // ===== validateSelectColumns =====

    @Test
    void validateSelectColumns_validSelect_doesNotThrow() {
        TableInfo tableInfo = buildTableInfo("users");
        assertDoesNotThrow(() -> validationService.validateSelectColumns("id,name", tableInfo));
    }

    @Test
    void validateSelectColumns_nullSelect_doesNotThrow() {
        TableInfo tableInfo = buildTableInfo("users");
        assertDoesNotThrow(() -> validationService.validateSelectColumns(null, tableInfo));
    }

    @Test
    void validateSelectColumns_emptySelect_doesNotThrow() {
        TableInfo tableInfo = buildTableInfo("users");
        assertDoesNotThrow(() -> validationService.validateSelectColumns("", tableInfo));
    }

    @Test
    void validateSelectColumns_invalidColumn_throwsIllegalArgumentException() {
        TableInfo tableInfo = buildTableInfo("users");

        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateSelectColumns("id,hacked_col", tableInfo));
    }

    // ===== validateOrderByColumn =====

    @Test
    void validateOrderByColumn_validColumn_doesNotThrow() {
        TableInfo tableInfo = buildTableInfo("users");
        assertDoesNotThrow(() -> validationService.validateOrderByColumn("id", tableInfo));
    }

    @Test
    void validateOrderByColumn_nullOrderBy_doesNotThrow() {
        TableInfo tableInfo = buildTableInfo("users");
        assertDoesNotThrow(() -> validationService.validateOrderByColumn(null, tableInfo));
    }

    @Test
    void validateOrderByColumn_emptyOrderBy_doesNotThrow() {
        TableInfo tableInfo = buildTableInfo("users");
        assertDoesNotThrow(() -> validationService.validateOrderByColumn("", tableInfo));
    }

    @Test
    void validateOrderByColumn_invalidColumn_throwsIllegalArgumentException() {
        TableInfo tableInfo = buildTableInfo("users");

        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateOrderByColumn("nonexistent_col", tableInfo));
    }

    // ===== validateFilterValue =====

    @Test
    void validateFilterValue_safeValue_doesNotThrow() {
        assertDoesNotThrow(() -> validationService.validateFilterValue("Alice"));
        assertDoesNotThrow(() -> validationService.validateFilterValue("hello@example.com"));
        assertDoesNotThrow(() -> validationService.validateFilterValue("123"));
    }

    @Test
    void validateFilterValue_semicolonInjection_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateFilterValue("value; DROP TABLE users"));
    }

    @Test
    void validateFilterValue_commentInjection_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateFilterValue("value -- comment"));
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateFilterValue("/* comment */ value"));
    }

    @Test
    void validateFilterValue_dropKeyword_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateFilterValue("1 DROP table"));
    }

    @Test
    void validateFilterValue_unionKeyword_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateFilterValue("1 UNION SELECT * FROM secrets"));
    }

    @Test
    void validateFilterValue_execKeyword_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateFilterValue("EXEC xp_cmdshell"));
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateFilterValue("EXECUTE proc"));
    }

    @Test
    void validateFilterValue_deleteKeyword_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateFilterValue("1 DELETE FROM users"));
    }

    // ===== validateInOperatorValues =====

    @Test
    void validateInOperatorValues_safeValues_doesNotThrow() {
        assertDoesNotThrow(() -> validationService.validateInOperatorValues("1,2,3"));
        assertDoesNotThrow(() -> validationService.validateInOperatorValues("active,inactive"));
    }

    @Test
    void validateInOperatorValues_injectionPatterns_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateInOperatorValues("1; DROP TABLE users"));
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateInOperatorValues("1, DROP users"));
        assertThrows(IllegalArgumentException.class,
                () -> validationService.validateInOperatorValues("1,DELETE FROM t"));
    }

    // ===== handleDatabaseConstraintViolation =====

    @Test
    void handleDatabaseConstraintViolation_notNullViolation_throwsValidationException() {
        String rootMsg = "null value in column \"email\" violates not-null constraint";
        DataIntegrityViolationException ex = mockDataIntegrityException(rootMsg);

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleDatabaseConstraintViolation(ex, "users", Map.of()));

        assertTrue(ve.getMessage().contains("email"));
        assertTrue(ve.getMessage().contains("required"));
    }

    @Test
    void handleDatabaseConstraintViolation_uniqueViolation_throwsValidationException() {
        String rootMsg = "duplicate key value violates unique constraint \"users_email_key\"";
        DataIntegrityViolationException ex = mockDataIntegrityException(rootMsg);

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleDatabaseConstraintViolation(ex, "users", Map.of()));

        assertTrue(ve.getMessage().contains("unique constraint"));
        assertTrue(ve.getMessage().contains("users_email_key"));
    }

    @Test
    void handleDatabaseConstraintViolation_foreignKeyViolation_throwsValidationException() {
        String rootMsg = "insert or update on table violates foreign key constraint \"orders_customer_id_fkey\"";
        DataIntegrityViolationException ex = mockDataIntegrityException(rootMsg);

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleDatabaseConstraintViolation(ex, "orders", Map.of()));

        assertTrue(ve.getMessage().contains("Foreign key"));
        assertTrue(ve.getMessage().contains("orders_customer_id_fkey"));
    }

    @Test
    void handleDatabaseConstraintViolation_checkViolation_throwsValidationException() {
        String rootMsg = "new row violates check constraint \"orders_amount_check\"";
        DataIntegrityViolationException ex = mockDataIntegrityException(rootMsg);

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleDatabaseConstraintViolation(ex, "orders", Map.of()));

        assertTrue(ve.getMessage().contains("Check constraint"));
    }

    @Test
    void handleDatabaseConstraintViolation_enumViolation_throwsValidationException() {
        String rootMsg = "invalid input value for enum order_status: \"unknown_value\"";
        DataIntegrityViolationException ex = mockDataIntegrityException(rootMsg);

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleDatabaseConstraintViolation(ex, "orders", Map.of()));

        assertTrue(ve.getMessage().contains("enum"));
        assertTrue(ve.getMessage().contains("order_status"));
    }

    @Test
    void handleDatabaseConstraintViolation_unknownViolation_throwsGenericValidationException() {
        String rootMsg = "some unknown constraint violation";
        DataIntegrityViolationException ex = mockDataIntegrityException(rootMsg);

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleDatabaseConstraintViolation(ex, "t", Map.of()));

        assertTrue(ve.getMessage().contains("Data validation error"));
    }

    @Test
    void handleDatabaseConstraintViolation_noRootCause_usesMainMessage() {
        DataIntegrityViolationException ex = mock(DataIntegrityViolationException.class);
        when(ex.getMessage()).thenReturn("some constraint error");
        when(ex.getRootCause()).thenReturn(null);

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleDatabaseConstraintViolation(ex, "t", Map.of()));

        assertTrue(ve.getMessage().contains("Data validation error"));
    }

    // ===== handleSqlConstraintViolation =====

    @Test
    void handleSqlConstraintViolation_23502NotNull_throwsValidationException() {
        SQLException sqlEx = new SQLException(
                "null value in column \"name\" violates not-null constraint", "23502");

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleSqlConstraintViolation(sqlEx, "t", Map.of()));

        assertTrue(ve.getMessage().contains("name"));
    }

    @Test
    void handleSqlConstraintViolation_23505Unique_throwsValidationException() {
        SQLException sqlEx = new SQLException(
                "duplicate key violates unique constraint \"users_email_key\"", "23505");

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleSqlConstraintViolation(sqlEx, "t", Map.of()));

        assertTrue(ve.getMessage().contains("unique constraint"));
    }

    @Test
    void handleSqlConstraintViolation_23503ForeignKey_throwsValidationException() {
        SQLException sqlEx = new SQLException(
                "violates foreign key constraint \"fk_name\"", "23503");

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleSqlConstraintViolation(sqlEx, "t", Map.of()));

        assertTrue(ve.getMessage().contains("Foreign key"));
    }

    @Test
    void handleSqlConstraintViolation_23514Check_throwsValidationException() {
        SQLException sqlEx = new SQLException(
                "violates check constraint \"check_name\"", "23514");

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleSqlConstraintViolation(sqlEx, "t", Map.of()));

        assertTrue(ve.getMessage().contains("Check constraint"));
    }

    @Test
    void handleSqlConstraintViolation_22P02EnumInvalid_throwsValidationException() {
        SQLException sqlEx = new SQLException(
                "invalid input value for enum my_status: \"bad_val\"", "22P02");

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleSqlConstraintViolation(sqlEx, "t", Map.of()));

        assertTrue(ve.getMessage().contains("enum"));
        assertTrue(ve.getMessage().contains("my_status"));
    }

    @Test
    void handleSqlConstraintViolation_22P02OtherFormat_throwsGenericValidationException() {
        SQLException sqlEx = new SQLException("Invalid data format error", "22P02");

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleSqlConstraintViolation(sqlEx, "t", Map.of()));

        assertTrue(ve.getMessage().contains("Invalid data format"));
    }

    @Test
    void handleSqlConstraintViolation_nullSqlState_throwsGenericValidationException() {
        SQLException sqlEx = new SQLException("some error", (String) null);

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleSqlConstraintViolation(sqlEx, "t", Map.of()));

        assertTrue(ve.getMessage().contains("Data validation error"));
    }

    @Test
    void handleSqlConstraintViolation_unknownSqlState_throwsGenericValidationException() {
        SQLException sqlEx = new SQLException("unknown constraint violation", "99999");

        ValidationException ve = assertThrows(ValidationException.class,
                () -> validationService.handleSqlConstraintViolation(sqlEx, "t", Map.of()));

        assertTrue(ve.getMessage().contains("Data validation error"));
    }

    // ===== extractColumnNameFromConstraint =====

    @Test
    void extractColumnNameFromConstraint_validMessage_extractsColumnName() {
        String message = "null value in column \"email\" violates not-null constraint";

        String col = validationService.extractColumnNameFromConstraint(message, "violates not-null constraint");

        assertEquals("email", col);
    }

    @Test
    void extractColumnNameFromConstraint_noMatch_returnsUnknown() {
        String message = "some generic error message";

        String col = validationService.extractColumnNameFromConstraint(message, "violates not-null constraint");

        assertEquals("unknown", col);
    }

    @Test
    void extractColumnNameFromConstraint_nullMessage_returnsUnknown() {
        String col = validationService.extractColumnNameFromConstraint(null, "violates not-null constraint");

        assertEquals("unknown", col);
    }

    // ===== invalidatePermissionCache =====

    @Test
    void invalidatePermissionCache_doesNotThrow() {
        assertDoesNotThrow(() -> validationService.invalidatePermissionCache());
    }

    @Test
    void invalidatePermissionCache_afterInvalidate_requeriesOnNextCheck() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("users"), eq("SELECT"))).thenReturn(true);

        validationService.hasTablePermission("users", "SELECT"); // populates cache
        validationService.invalidatePermissionCache();
        validationService.hasTablePermission("users", "SELECT"); // should re-query

        verify(jdbcTemplate, times(2)).queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), eq("users"), eq("SELECT"));
    }

    @Test
    void invalidatePermissionForTable_invalidatesAll() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), any(), any())).thenReturn(true);

        validationService.hasTablePermission("users", "SELECT");
        validationService.invalidatePermissionForTable("users");
        validationService.hasTablePermission("users", "SELECT"); // cache cleared, re-queries

        verify(jdbcTemplate, times(2)).queryForObject(
                eq("SELECT has_table_privilege(current_user, ?, ?)"),
                eq(Boolean.class), any(), any());
    }

    // ===== getPermissionCacheStats =====

    @Test
    void getPermissionCacheStats_returnsExpectedFields() {
        Map<String, Object> stats = validationService.getPermissionCacheStats();

        assertNotNull(stats);
        assertTrue(stats.containsKey("totalEntries"));
        assertTrue(stats.containsKey("validEntries"));
        assertTrue(stats.containsKey("expiredEntries"));
        assertTrue(stats.containsKey("ttlSeconds"));
    }

    @Test
    void getPermissionCacheStats_emptyCache_totalEntriesZero() {
        Map<String, Object> stats = validationService.getPermissionCacheStats();

        assertEquals(0, ((Number) stats.get("totalEntries")).intValue());
    }

    // ===== getMaxLimit / getMaxOffset =====

    @Test
    void getMaxLimit_returns1000() {
        assertEquals(1000, validationService.getMaxLimit());
    }

    @Test
    void getMaxOffset_returns1000000() {
        assertEquals(1_000_000, validationService.getMaxOffset());
    }

    // ===== helper methods =====

    private TableInfo buildTableInfo(String name) {
        List<ColumnInfo> columns = List.of(
                new ColumnInfo("id", "integer", true, false),
                new ColumnInfo("name", "text", false, false)
        );
        return new TableInfo(name, columns, Collections.emptyList());
    }

    private DataIntegrityViolationException mockDataIntegrityException(String rootMsg) {
        DataIntegrityViolationException ex = mock(DataIntegrityViolationException.class);
        when(ex.getMessage()).thenReturn("DataIntegrityViolationException: " + rootMsg);
        Throwable rootCause = mock(Throwable.class);
        when(rootCause.getMessage()).thenReturn(rootMsg);
        when(ex.getRootCause()).thenReturn(rootCause);
        return ex;
    }
}
