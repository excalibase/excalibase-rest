package io.github.excalibase.postgres.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.postgres.util.PostgresTypeConverter;
import io.github.excalibase.service.TypeConversionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class UpsertService {

    private static final Logger log = LoggerFactory.getLogger(UpsertService.class);
    private final JdbcTemplate jdbcTemplate;
    private final ValidationService validationService;
    private final TypeConversionService typeConversionService;
    private final QueryBuilderService queryBuilderService;

    public UpsertService(JdbcTemplate jdbcTemplate, ValidationService validationService, 
                        TypeConversionService typeConversionService, QueryBuilderService queryBuilderService) {
        this.jdbcTemplate = jdbcTemplate;
        this.validationService = validationService;
        this.typeConversionService = typeConversionService;
        this.queryBuilderService = queryBuilderService;
    }

    /**
     * Upsert a single record (INSERT ... ON CONFLICT DO UPDATE)
     * Upsert with conflict resolution
     */
    public Map<String, Object> upsertRecord(String tableName, Map<String, Object> data) {
        // Validate input
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "INSERT");
        validationService.validateTablePermission(tableName, "UPDATE");
        
        // Get primary key column(s) for conflict resolution
        List<String> primaryKeyColumns = tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .toList();
            
        if (primaryKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("Table " + tableName + " has no primary key - cannot perform upsert");
        }
        
        // Validate columns
        validationService.validateColumns(data.keySet(), tableInfo);

        Map<String, Object> processedData = new HashMap<>();
        for (String column : data.keySet()) {
            if (data.get(column) != null) {
                processedData.put(column, data.get(column));
            }
        }
        
        if (processedData.isEmpty()) {
            throw new IllegalArgumentException("No valid data to upsert");
        }
        
        // Build upsert query (PostgreSQL INSERT ... ON CONFLICT DO UPDATE)
        List<String> columns = new ArrayList<>(processedData.keySet());
        
        // Build update SET clause (exclude primary key columns from update)
        List<String> updateColumns = columns.stream()
            .filter(col -> !primaryKeyColumns.contains(col))
            .collect(Collectors.toList());
        
        String query = queryBuilderService.buildUpsertQuery(tableName, columns, primaryKeyColumns, updateColumns, tableInfo);
        
        // Prepare parameters
        List<Object> params = new ArrayList<>();
        for (String column : columns) {
            Object value = processedData.get(column);
            params.add(typeConversionService.convertValueToColumnType(column, value, tableInfo));
        }
        
        try {
            List<Map<String, Object>> results = jdbcTemplate.queryForList(query, params.toArray());
            if (results.isEmpty()) {
                // This can happen with ON CONFLICT DO NOTHING
                return null;
            }
            
            Map<String, Object> result = results.get(0);
            result = PostgresTypeConverter.convertPostgresTypesInRecord(result, tableInfo);
            log.debug("Upserted record in table {}", tableName);
            return result;
            
        } catch (DataIntegrityViolationException e) {
            validationService.handleDatabaseConstraintViolation(e, tableName, processedData);
            return null; // This line won't be reached
        } catch (DataAccessException e) {
            Throwable rootCause = e.getRootCause();
            if (rootCause instanceof SQLException) {
                SQLException sqlEx = (SQLException) rootCause;
                validationService.handleSqlConstraintViolation(sqlEx, tableName, processedData);
            }
            log.error("Error upserting record in table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Error upserting record: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error upserting record in table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Error upserting record: " + e.getMessage(), e);
        }
    }
    
    /**
     * Bulk upsert records with conflict resolution
     */
    public List<Map<String, Object>> upsertBulkRecords(String tableName, List<Map<String, Object>> dataList) {
        // Validate input
        if (dataList == null || dataList.isEmpty()) {
            throw new IllegalArgumentException("Data list cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "INSERT");
        validationService.validateTablePermission(tableName, "UPDATE");
        
        // Get primary key column(s) for conflict resolution
        List<String> primaryKeyColumns = tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .toList();
            
        if (primaryKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("Table " + tableName + " has no primary key - cannot perform bulk upsert");
        }
        
        // Process each record similarly to createBulkRecords
        List<Map<String, Object>> processedRecords = new ArrayList<>();
        
        for (Map<String, Object> data : dataList) {
            Map<String, Object> processedData = new HashMap<>();
            
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
            throw new IllegalArgumentException("No valid records to upsert");
        }
        
        // Build bulk upsert query
        Set<String> allColumns = processedRecords.stream()
            .flatMap(record -> record.keySet().stream())
            .collect(Collectors.toSet());
        
        List<String> columnList = new ArrayList<>(allColumns);
        
        // Build update SET clause (exclude primary key columns from update)
        List<String> updateColumns = columnList.stream()
            .filter(col -> !primaryKeyColumns.contains(col))
            .collect(Collectors.toList());
        
        String query = queryBuilderService.buildBulkUpsertQuery(tableName, columnList, primaryKeyColumns, 
                                                               updateColumns, processedRecords.size(), tableInfo);
        
        List<Object> allParams = new ArrayList<>();
        for (Map<String, Object> record : processedRecords) {
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
            log.debug("Upserted {} records in table {}", results.size(), tableName);
            return results;
            
        } catch (DataIntegrityViolationException e) {
            validationService.handleDatabaseConstraintViolation(e, tableName, Map.of());
            return null; // This line won't be reached
        } catch (DataAccessException e) {
            Throwable rootCause = e.getRootCause();
            if (rootCause instanceof SQLException) {
                SQLException sqlEx = (SQLException) rootCause;
                validationService.handleSqlConstraintViolation(sqlEx, tableName, Map.of());
            }
            log.error("Error upserting bulk records in table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Error upserting bulk records: " + e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error upserting bulk records in table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Error upserting bulk records: " + e.getMessage(), e);
        }
    }
}
