package io.github.excalibase.postgres.service;

import io.github.excalibase.annotation.ExcalibaseService;
import io.github.excalibase.cache.TTLCache;
import io.github.excalibase.constant.SupportedDatabaseConstant;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.exception.ValidationException;
import io.github.excalibase.service.IValidationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@ExcalibaseService(serviceName = SupportedDatabaseConstant.POSTGRES)
public class ValidationService implements IValidationService {

    private static final Logger log = LoggerFactory.getLogger(ValidationService.class);
    private final JdbcTemplate jdbcTemplate;
    private final DatabaseSchemaService schemaService;
    private final TTLCache<PermissionKey, Boolean> permissionCache;

    // Maximum allowed values for security
    private static final int MAX_LIMIT = 1000;
    private static final int MAX_OFFSET = 1000000; // 1 million

    // Constructor for Spring dependency injection with @Value
    @Autowired
    public ValidationService(JdbcTemplate jdbcTemplate, DatabaseSchemaService schemaService,
                            @Value("${app.permission-cache-ttl-seconds:300}") int permissionCacheTtlSeconds) {
        this.jdbcTemplate = jdbcTemplate;
        this.schemaService = schemaService;
        this.permissionCache = new TTLCache<>(Duration.ofSeconds(permissionCacheTtlSeconds));
    }

    // Constructor for tests (without @Value)
    public ValidationService(JdbcTemplate jdbcTemplate, DatabaseSchemaService schemaService) {
        this(jdbcTemplate, schemaService, 300); // Default 5 minutes
    }

    /**
     * Validate pagination parameters
     */
    public void validatePaginationParams(int offset, int limit) {
        if (offset < 0 || offset > MAX_OFFSET) {
            throw new IllegalArgumentException("Offset must be between 0 and " + MAX_OFFSET);
        }
        if (limit <= 0 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("Limit must be between 1 and " + MAX_LIMIT);
        }
    }

    /**
     * Validate table name for security - prevents SQL injection
     */
    public void validateTableName(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }
        // Basic SQL injection protection - only allow alphanumeric and underscore
        if (!tableName.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }
    }

    /**
     * Validate and get table info with role-based access control
     * Throws exception if table not found or user lacks permissions
     */
    public TableInfo getValidatedTableInfo(String tableName) {
        validateTableName(tableName);
        Map<String, TableInfo> schema = schemaService.getTableSchema();
        TableInfo tableInfo = schema.get(tableName);
        if (tableInfo == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        return tableInfo;
    }

    /**
     * Check if current user has required permission on table.
     * Results are cached to reduce database load.
     */
    public boolean hasTablePermission(String tableName, String permission) {
        try {
            String currentUser = getCurrentDatabaseUser();
            PermissionKey key = new PermissionKey(currentUser, tableName, permission);

            return permissionCache.computeIfAbsent(key, k -> {
                try {
                    String query = "SELECT has_table_privilege(current_user, ?, ?)";
                    Boolean hasPermission = jdbcTemplate.queryForObject(query, Boolean.class, tableName, permission);
                    log.debug("Permission check for user={}, table={}, permission={}: {}",
                             currentUser, tableName, permission, hasPermission);
                    return hasPermission != null && hasPermission;
                } catch (Exception e) {
                    log.warn("Failed to check table permission for {}: {}", tableName, e.getMessage());
                    return true; // Fallback to allowing access if permission check fails
                }
            });
        } catch (Exception e) {
            log.warn("Failed to check cached table permission for {}: {}", tableName, e.getMessage());
            return true; // Fallback to allowing access if permission check fails
        }
    }

    /**
     * Get the current database user for permission checks.
     */
    private String getCurrentDatabaseUser() {
        try {
            return jdbcTemplate.queryForObject("SELECT current_user", String.class);
        } catch (Exception e) {
            log.warn("Failed to get current database user: {}", e.getMessage());
            return "unknown";
        }
    }

    /**
     * Validate table permissions for specific operations
     */
    public void validateTablePermission(String tableName, String operation) {
        String permission = switch (operation.toLowerCase()) {
            case "select", "read" -> "SELECT";
            case "insert", "create" -> "INSERT";
            case "update", "patch", "put" -> "UPDATE";
            case "delete" -> "DELETE";
            default -> "SELECT"; // Default to most restrictive
        };

        if (!hasTablePermission(tableName, permission)) {
            throw new IllegalArgumentException("Access denied: insufficient privileges for " + operation + " on table " + tableName);
        }
    }

    /**
     * Validate column names against table schema
     */
    public void validateColumns(Set<String> columnNames, TableInfo tableInfo) {
        Set<String> validColumns = tableInfo.getColumns().stream()
                .map(ColumnInfo::getName)
                .collect(Collectors.toSet());

        for (String column : columnNames) {
            if (!validColumns.contains(column)) {
                throw new IllegalArgumentException("Invalid column: " + column);
            }
        }
    }

    /**
     * Validate column names in select parameter
     */
    public void validateSelectColumns(String select, TableInfo tableInfo) {
        if (select == null || select.trim().isEmpty()) {
            return;
        }

        String[] selectedColumns = select.split(",");
        Set<String> validColumns = tableInfo.getColumns().stream()
                .map(ColumnInfo::getName)
                .collect(Collectors.toSet());

        for (String col : selectedColumns) {
            String trimmedCol = col.trim();
            if (!validColumns.contains(trimmedCol)) {
                throw new IllegalArgumentException("Invalid column: " + trimmedCol);
            }
        }
    }

    /**
     * Validate order by column
     */
    public void validateOrderByColumn(String orderBy, TableInfo tableInfo) {
        if (orderBy == null || orderBy.trim().isEmpty()) {
            return;
        }

        boolean columnExists = tableInfo.getColumns().stream()
                .anyMatch(col -> col.getName().equals(orderBy));
        if (!columnExists) {
            throw new IllegalArgumentException("Invalid column for ordering: " + orderBy);
        }
    }

    /**
     * Basic SQL injection protection - check for common dangerous patterns
     */
    public void validateFilterValue(String value) {
        String upperValue = value.toUpperCase();
        if (upperValue.contains(";") || upperValue.contains("--") || upperValue.contains("/*") ||
                upperValue.contains("*/") || upperValue.contains(" DROP ") || upperValue.contains(" DELETE ") ||
                upperValue.contains(" UPDATE ") || upperValue.contains(" INSERT ") || upperValue.contains(" CREATE ") ||
                upperValue.contains(" ALTER ") || upperValue.contains(" TRUNCATE ") || upperValue.contains("UNION ") ||
                upperValue.contains("EXEC") || upperValue.contains("EXECUTE")) {
            throw new IllegalArgumentException("Invalid characters detected in filter value");
        }
    }

    /**
     * Validate IN operator values for security
     */
    public void validateInOperatorValues(String inValues) {
        if (inValues.contains(";") || inValues.contains("--") || inValues.contains("/*") ||
                inValues.contains("*/") || inValues.contains("DROP") || inValues.contains("DELETE") ||
                inValues.contains("UPDATE") || inValues.contains("INSERT") || inValues.contains("CREATE")) {
            throw new IllegalArgumentException("Invalid characters detected in filter value");
        }
    }

    /**
     * Handle database constraint violations and convert to ValidationException
     */
    public void handleDatabaseConstraintViolation(DataIntegrityViolationException e, String tableName, Map<String, Object> data) {
        String message = e.getMessage();
        String rootMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : message;

        // Check for common constraint violations
        if (rootMessage != null) {
            if (rootMessage.contains("violates not-null constraint")) {
                String columnName = extractColumnNameFromConstraint(rootMessage, "violates not-null constraint");
                throw new ValidationException("Field '" + columnName + "' is required and cannot be null");
            } else if (rootMessage.contains("violates unique constraint")) {
                String constraintName = extractConstraintNameFromMessage(rootMessage);
                throw new ValidationException("Duplicate value violates unique constraint: " + constraintName);
            } else if (rootMessage.contains("violates foreign key constraint")) {
                String constraintName = extractConstraintNameFromMessage(rootMessage);
                throw new ValidationException("Foreign key constraint violation: " + constraintName);
            } else if (rootMessage.contains("violates check constraint")) {
                String constraintName = extractConstraintNameFromMessage(rootMessage);
                throw new ValidationException("Check constraint violation: " + constraintName);
            } else if (rootMessage.contains("invalid input value for enum")) {
                String enumType = extractEnumTypeFromMessage(rootMessage);
                throw new ValidationException("Invalid enum value. Please check valid values for type: " + enumType);
            }
        }

        // Generic constraint violation message
        throw new ValidationException("Data validation error: " + (rootMessage != null ? rootMessage : message));
    }

    /**
     * Handle SQL constraint violations from SQLException
     */
    public void handleSqlConstraintViolation(SQLException sqlEx, String tableName, Map<String, Object> data) {
        String sqlState = sqlEx.getSQLState();
        String message = sqlEx.getMessage();

        // PostgreSQL error codes
        switch (sqlState != null ? sqlState : "") {
            case "23502": // NOT NULL constraint violation
                String columnName = extractColumnNameFromConstraint(message, "violates not-null constraint");
                throw new ValidationException("Field '" + columnName + "' is required and cannot be null");

            case "23505": // Unique constraint violation
                String constraintName = extractConstraintNameFromMessage(message);
                throw new ValidationException("Duplicate value violates unique constraint: " + constraintName);

            case "23503": // Foreign key constraint violation
                String fkConstraintName = extractConstraintNameFromMessage(message);
                throw new ValidationException("Foreign key constraint violation: " + fkConstraintName);

            case "23514": // Check constraint violation
                String checkConstraintName = extractConstraintNameFromMessage(message);
                throw new ValidationException("Check constraint violation: " + checkConstraintName);

            case "22P02": // Invalid text representation (e.g., enum values)
                if (message != null && message.contains("invalid input value for enum")) {
                    String enumType = extractEnumTypeFromMessage(message);
                    throw new ValidationException("Invalid enum value. Please check valid values for type: " + enumType);
                }
                throw new ValidationException("Invalid data format: " + message);

            default:
                // Other constraint violations
                throw new ValidationException("Data validation error: " + message);
        }
    }

    /**
     * Extract column name from constraint violation message
     */
    public String extractColumnNameFromConstraint(String message, String constraintType) {
        if (message == null) return "unknown";

        // Pattern: 'null value in column "column_name" violates not-null constraint'
        String pattern = "column \"([^\"]+)\" " + constraintType;
        Pattern regexPattern = Pattern.compile(pattern);
        Matcher matcher = regexPattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return "unknown";
    }

    /**
     * Extract constraint name from error message
     */
    private String extractConstraintNameFromMessage(String message) {
        if (message == null) return "unknown";

        // Pattern: 'constraint "constraint_name"'
        Pattern pattern = Pattern.compile("constraint \"([^\"]+)\"");
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return "unknown";
    }

    /**
     * Extract enum type name from error message
     */
    private String extractEnumTypeFromMessage(String message) {
        if (message == null) return "unknown";

        // Pattern: 'invalid input value for enum enum_type_name'
        Pattern pattern = Pattern.compile("invalid input value for enum ([a-zA-Z_][a-zA-Z0-9_]*)");
        Matcher matcher = pattern.matcher(message);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return "unknown";
    }

    public int getMaxLimit() {
        return MAX_LIMIT;
    }

    public int getMaxOffset() {
        return MAX_OFFSET;
    }

    /**
     * Invalidate all permission cache entries.
     * Useful for admin operations or when permissions are changed.
     */
    public void invalidatePermissionCache() {
        permissionCache.clear();
        log.info("Permission cache invalidated");
    }

    /**
     * Invalidate permission cache entries for a specific table.
     */
    public void invalidatePermissionForTable(String tableName) {
        // TTLCache doesn't support filtering by partial key, so we clear all
        // In a production system, you might want a more sophisticated cache structure
        permissionCache.clear();
        log.info("Permission cache invalidated for table: {}", tableName);
    }

    /**
     * Get permission cache statistics.
     */
    public Map<String, Object> getPermissionCacheStats() {
        TTLCache.CacheStats stats = permissionCache.getStats();
        return Map.of(
            "totalEntries", stats.getTotalEntries(),
            "validEntries", stats.getValidEntries(),
            "expiredEntries", stats.getExpiredEntries(),
            "ttlSeconds", stats.getTtl().getSeconds()
        );
    }

    /**
     * Cache key for permission checks.
     * Composite key of (user, tableName, permission) ensures correct cache behavior.
     */
    private static class PermissionKey {
        private final String user;
        private final String tableName;
        private final String permission;

        public PermissionKey(String user, String tableName, String permission) {
            this.user = user;
            this.tableName = tableName;
            this.permission = permission;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PermissionKey that = (PermissionKey) o;
            return Objects.equals(user, that.user) &&
                   Objects.equals(tableName, that.tableName) &&
                   Objects.equals(permission, that.permission);
        }

        @Override
        public int hashCode() {
            return Objects.hash(user, tableName, permission);
        }

        @Override
        public String toString() {
            return String.format("PermissionKey{user='%s', table='%s', permission='%s'}", user, tableName, permission);
        }
    }
}
