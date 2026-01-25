package io.github.excalibase.postgres.service;

import io.github.excalibase.annotation.ExcalibaseService;
import io.github.excalibase.constant.SupportedDatabaseConstant;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.postgres.util.PostgresTypeConverter;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.ICrudService;
import io.github.excalibase.service.TypeConversionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.MultiValueMap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ExcalibaseService(serviceName = SupportedDatabaseConstant.POSTGRES)
public class CrudService implements ICrudService {

    private static final Logger log = LoggerFactory.getLogger(CrudService.class);
    private final JdbcTemplate jdbcTemplate;
    private final ValidationService validationService;
    private final TypeConversionService typeConversionService;
    private final QueryBuilderService queryBuilderService;

    public CrudService(JdbcTemplate jdbcTemplate, ValidationService validationService, 
                       TypeConversionService typeConversionService, QueryBuilderService queryBuilderService) {
        this.jdbcTemplate = jdbcTemplate;
        this.validationService = validationService;
        this.typeConversionService = typeConversionService;
        this.queryBuilderService = queryBuilderService;
    }

    /**
     * Create a single record
     */
    public Map<String, Object> createRecord(String tableName, Map<String, Object> data) {
        // Validate input
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "INSERT");

        // Validate columns
        validationService.validateColumns(data.keySet(), tableInfo);

        // Validate and convert data types
        // Build INSERT query
        List<String> columns = new ArrayList<>(data.keySet());
        List<Object> values = new ArrayList<>();
        
        // Convert values with proper type casting
        for (String columnName : columns) {
            Object rawValue = data.get(columnName);
            Object convertedValue = typeConversionService.convertValueToColumnType(columnName, rawValue, tableInfo);
            values.add(convertedValue);
        }

        String query = queryBuilderService.buildInsertQuery(tableName, columns, tableInfo);

        try {
            List<Map<String, Object>> results = jdbcTemplate.queryForList(query, values.toArray());
            if (!results.isEmpty()) {
                results = PostgresTypeConverter.convertPostgresTypes(results, tableInfo);
            }
            return results.isEmpty() ? null : results.get(0);
        } catch (DataIntegrityViolationException e) {
            validationService.handleDatabaseConstraintViolation(e, tableName, data);
            return null; // This line won't be reached
        } catch (Exception e) {
            log.error("Unexpected error during record creation: " + e.getMessage(), e);
            throw new RuntimeException("Unexpected error during record creation: " + e.getMessage(), e);
        }
    }

    /**
     * Create multiple records in bulk
     */
    public List<Map<String, Object>> createBulkRecords(String tableName, List<Map<String, Object>> dataList) {
        // Validate input
        if (dataList == null || dataList.isEmpty()) {
            throw new IllegalArgumentException("Data list cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "INSERT");
        
        // Validate each record and filter non-null values
        List<Map<String, Object>> processedRecords = new ArrayList<>();
        
        for (Map<String, Object> data : dataList) {
            Map<String, Object> processedData = new HashMap<>();
            
            // Validate columns
            validationService.validateColumns(data.keySet(), tableInfo);
            
            for (String column : data.keySet()) {
                if (data.get(column) != null) {
                    processedData.put(column, data.get(column));
                }
            }
            
            if (!processedData.isEmpty()) {
                processedRecords.add(processedData);
            }
        }
        
        if (processedRecords.isEmpty()) {
            throw new IllegalArgumentException("No valid records to create");
        }
        
        // Get all unique columns across all records
        Set<String> allColumns = processedRecords.stream()
            .flatMap(record -> record.keySet().stream())
            .collect(Collectors.toSet());
        
        // Build bulk INSERT query
        List<String> columnList = new ArrayList<>(allColumns);
        String query = queryBuilderService.buildBulkInsertQuery(tableName, columnList, processedRecords.size(), tableInfo);
        
        List<Object> allParams = new ArrayList<>();
        for (Map<String, Object> record : processedRecords) {
            // Add parameters in the same order as columns with proper type conversion
            for (String column : columnList) {
                Object value = record.getOrDefault(column, null);
                if (value != null) {
                    allParams.add(typeConversionService.convertValueToColumnType(column, value, tableInfo));
                } else {
                    allParams.add(null);
                }
            }
        }
        
        try {
            List<Map<String, Object>> results = jdbcTemplate.queryForList(query, allParams.toArray());
            if (!results.isEmpty()) {
                results = PostgresTypeConverter.convertPostgresTypes(results, tableInfo);
            }
            log.debug("Created {} records in table {}", results.size(), tableName);
            return results;
        } catch (DataIntegrityViolationException e) {
            validationService.handleDatabaseConstraintViolation(e, tableName, Map.of());
            return null; // This line won't be reached
        } catch (DataAccessException e) {
            // Check if it's a constraint violation in the underlying cause
            Throwable rootCause = e.getRootCause();
            if (rootCause instanceof SQLException) {
                SQLException sqlEx = (SQLException) rootCause;
                validationService.handleSqlConstraintViolation(sqlEx, tableName, Map.of());
            }
            log.error("Error creating bulk records in table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Error creating bulk records: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error creating bulk records in table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Error creating bulk records: " + e.getMessage(), e);
        }
    }

    /**
     * Update a single record by ID
     */
    public Map<String, Object> updateRecord(String tableName, String id, Map<String, Object> data) {
        // Validate input
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("ID cannot be empty");
        }
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "UPDATE");

        // Get primary key columns (supports composite keys)
        List<ColumnInfo> primaryKeyColumns = getPrimaryKeyColumns(tableInfo);
        if (primaryKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("Table " + tableName + " has no primary key defined");
        }

        // Validate columns
        validationService.validateColumns(data.keySet(), tableInfo);

        Set<String> primaryKeyColumnNames = primaryKeyColumns.stream()
            .map(ColumnInfo::getName)
            .collect(Collectors.toSet());

        // Build UPDATE query
        List<String> updateColumns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!primaryKeyColumnNames.contains(entry.getKey())) { // Don't update primary key columns
                updateColumns.add(entry.getKey());
                values.add(typeConversionService.convertValueToColumnType(entry.getKey(), entry.getValue(), tableInfo));
            }
        }
        
        if (updateColumns.isEmpty()) {
            throw new IllegalArgumentException("No valid columns to update");
        }
        
        // Parse composite key values and add to WHERE clause
        String[] keyValues = queryBuilderService.parseCompositeKey(id, primaryKeyColumns.size());
        List<String> whereConditions = queryBuilderService.buildCompositeKeyConditions(primaryKeyColumns, tableInfo);
        
        for (int i = 0; i < primaryKeyColumns.size(); i++) {
            ColumnInfo pkColumn = primaryKeyColumns.get(i);
            values.add(typeConversionService.convertValueToColumnType(pkColumn.getName(), keyValues[i], tableInfo));
        }
        
        String query = queryBuilderService.buildUpdateQuery(tableName, updateColumns, whereConditions, tableInfo);
        
        try {
            List<Map<String, Object>> results = jdbcTemplate.queryForList(query, values.toArray());
            if (!results.isEmpty()) {
                results = PostgresTypeConverter.convertPostgresTypes(results, tableInfo);
            }
            return results.isEmpty() ? null : results.get(0);
        } catch (DataIntegrityViolationException e) {
            validationService.handleDatabaseConstraintViolation(e, tableName, data);
            return null; // This line won't be reached
        } catch (DataAccessException e) {
            // Check if it's a constraint violation in the underlying cause
            Throwable rootCause = e.getRootCause();
            if (rootCause instanceof SQLException) {
                SQLException sqlEx = (SQLException) rootCause;
                validationService.handleSqlConstraintViolation(sqlEx, tableName, data);
            }
            // Continue with fallback approach
        } catch (Exception e) {
            log.warn("RETURNING clause not supported in UPDATE, using fallback approach: " + e.getMessage());
        }
        
        return null; // Should not reach here
    }

    /**
     * Update multiple records in bulk
     */
    public List<Map<String, Object>> updateBulkRecords(String tableName, List<Map<String, Object>> updateList) {
        // Validate input
        if (updateList == null || updateList.isEmpty()) {
            throw new IllegalArgumentException("Update list cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "UPDATE");
        
        List<Map<String, Object>> results = new ArrayList<>();
        
        for (Map<String, Object> updateItem : updateList) {
            String id = (String) updateItem.get("id");
            if (id == null || id.trim().isEmpty()) {
                throw new IllegalArgumentException("Each update item must have an 'id' field");
            }
            
            @SuppressWarnings("unchecked")
            Map<String, Object> data = (Map<String, Object>) updateItem.get("data");
            if (data == null || data.isEmpty()) {
                throw new IllegalArgumentException("Each update item must have a 'data' field with update values");
            }
            
            Map<String, Object> result = updateRecord(tableName, id, data);
            if (result != null) {
                results.add(result);
            }
        }
        
        return results;
    }

    /**
     * Delete a single record by ID
     */
    public boolean deleteRecord(String tableName, String id) {
        // Validate input
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("ID cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "DELETE");

        // Get primary key columns (supports composite keys)
        List<ColumnInfo> primaryKeyColumns = getPrimaryKeyColumns(tableInfo);
        if (primaryKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("Table " + tableName + " has no primary key defined");
        }

        // Parse composite key values and build WHERE clause
        String[] keyValues = queryBuilderService.parseCompositeKey(id, primaryKeyColumns.size());
        List<String> whereConditions = queryBuilderService.buildCompositeKeyConditions(primaryKeyColumns, tableInfo);
        List<Object> queryParams = new ArrayList<>();
        
        for (int i = 0; i < primaryKeyColumns.size(); i++) {
            ColumnInfo pkColumn = primaryKeyColumns.get(i);
            queryParams.add(typeConversionService.convertValueToColumnType(pkColumn.getName(), keyValues[i], tableInfo));
        }
        
        String query = queryBuilderService.buildDeleteQuery(tableName, whereConditions);
        int affectedRows = jdbcTemplate.update(query, queryParams.toArray());
        
        return affectedRows > 0;
    }

    /**
     * Delete records by filters
     */
    public Map<String, Object> deleteRecordsByFilters(String tableName, MultiValueMap<String, String> filters, 
                                                      FilterService filterService) {
        // Validate input
        if (filters == null || filters.isEmpty()) {
            throw new IllegalArgumentException("Filters cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "DELETE");
        
        // Build DELETE query with WHERE clause
        StringBuilder query = new StringBuilder();
        List<Object> params = new ArrayList<>();
        
        query.append("DELETE FROM ").append(tableName);
        
        // Build WHERE clause using filter service
        List<String> conditions = filterService.parseFilters(filters, params, tableInfo);
        if (!conditions.isEmpty()) {
            query.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        
        // Execute delete query
        int deletedCount = jdbcTemplate.update(query.toString(), params.toArray());
        
        Map<String, Object> result = new HashMap<>();
        result.put("deletedCount", deletedCount);
        result.put("success", true);
        
        return result;
    }

    /**
     * Update records by filters
     */
    public Map<String, Object> updateRecordsByFilters(String tableName, MultiValueMap<String, String> filters, 
                                                      Map<String, Object> updateData, FilterService filterService) {
        // Validate input
        if (filters == null || filters.isEmpty()) {
            throw new IllegalArgumentException("Filters cannot be empty");
        }
        if (updateData == null || updateData.isEmpty()) {
            throw new IllegalArgumentException("Update data cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "UPDATE");
        
        // Validate columns
        validationService.validateColumns(updateData.keySet(), tableInfo);
        
        // Build UPDATE query with WHERE clause
        StringBuilder query = new StringBuilder();
        List<Object> params = new ArrayList<>();
        
        // Build SET clause
        List<String> setParts = new ArrayList<>();
        for (Map.Entry<String, Object> entry : updateData.entrySet()) {
            setParts.add(entry.getKey() + " = " + typeConversionService.buildPlaceholderWithCast(entry.getKey(), tableInfo));
            params.add(typeConversionService.convertValueToColumnType(entry.getKey(), entry.getValue(), tableInfo));
        }
        
        query.append("UPDATE ").append(tableName)
             .append(" SET ").append(String.join(", ", setParts));
        
        // Build WHERE clause using filter service
        List<String> conditions = filterService.parseFilters(filters, params, tableInfo);
        if (!conditions.isEmpty()) {
            query.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        
        // Execute update query
        int updatedCount = jdbcTemplate.update(query.toString(), params.toArray());
        
        Map<String, Object> result = new HashMap<>();
        result.put("updatedCount", updatedCount);
        result.put("success", true);
        
        return result;
    }

    /**
     * Get all primary key columns from a table (supports composite keys)
     */
    private List<ColumnInfo> getPrimaryKeyColumns(TableInfo tableInfo) {
        return tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .collect(Collectors.toList());
    }
}
