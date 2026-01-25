package io.github.excalibase.service;

import io.github.excalibase.model.TableInfo;
import org.springframework.dao.DataIntegrityViolationException;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * Interface for validation services providing input validation and security checks.
 * This interface matches the actual ValidationService method signatures for multi-database support.
 */
public interface IValidationService {

    void validatePaginationParams(int offset, int limit);

    void validateTableName(String tableName);

    TableInfo getValidatedTableInfo(String tableName);

    boolean hasTablePermission(String tableName, String permission);

    void validateTablePermission(String tableName, String operation);

    void validateColumns(Set<String> columnNames, TableInfo tableInfo);

    void validateSelectColumns(String select, TableInfo tableInfo);

    void validateOrderByColumn(String orderBy, TableInfo tableInfo);

    void validateFilterValue(String value);

    void validateInOperatorValues(String inValues);

    void handleDatabaseConstraintViolation(DataIntegrityViolationException e, String tableName, Map<String, Object> data);

    void handleSqlConstraintViolation(SQLException sqlEx, String tableName, Map<String, Object> data);

    String extractColumnNameFromConstraint(String message, String constraintType);

    int getMaxLimit();
}