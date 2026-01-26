package io.github.excalibase.postgres.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.SelectField;
import io.github.excalibase.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Enhanced relationship service that supports nested filtering and column selection
 */
@Service
public class EnhancedRelationshipService {

    private static final Logger log = LoggerFactory.getLogger(EnhancedRelationshipService.class);
    
    private final JdbcTemplate jdbcTemplate;
    private final DatabaseSchemaService schemaService;
    
    public EnhancedRelationshipService(JdbcTemplate jdbcTemplate, DatabaseSchemaService schemaService) {
        this.jdbcTemplate = jdbcTemplate;
        this.schemaService = schemaService;
    }
    
    /**
     * Expand relationships using enhanced select fields with filtering
     */
    public List<Map<String, Object>> expandRelationships(
            List<Map<String, Object>> records, 
            TableInfo tableInfo, 
            List<SelectField> embeddedFields,
            MultiValueMap<String, String> allParams) {
        
        if (records.isEmpty() || embeddedFields.isEmpty()) {
            return records;
        }
        
        Map<String, TableInfo> allTables = schemaService.getTableSchema();
        
        for (SelectField embeddedField : embeddedFields) {
            expandSingleRelationshipEnhanced(records, tableInfo, embeddedField, allTables, allParams);
        }
        
        return records;
    }
    
    /**
     * Expand a single relationship with enhanced filtering and column selection
     */
    private void expandSingleRelationshipEnhanced(
            List<Map<String, Object>> records,
            TableInfo tableInfo,
            SelectField embeddedField,
            Map<String, TableInfo> allTables,
            MultiValueMap<String, String> allParams) {
        
        String relationshipName = embeddedField.getName();
        
        try {
            // Check if it's a forward relationship (FK in current table)
            for (var fk : tableInfo.getForeignKeys()) {
                if (fk.getReferencedTable().equalsIgnoreCase(relationshipName)) {
                    expandForwardRelationshipEnhanced(records, fk, embeddedField, allParams);
                    return;
                }
            }
            
            // Check if it's a reverse relationship (FK in other table pointing to this table)
            for (var otherTableEntry : allTables.entrySet()) {
                String otherTableName = otherTableEntry.getKey();
                TableInfo otherTableInfo = otherTableEntry.getValue();
                
                if (otherTableName.equalsIgnoreCase(relationshipName)) {
                    for (var otherFk : otherTableInfo.getForeignKeys()) {
                        if (otherFk.getReferencedTable().equalsIgnoreCase(tableInfo.getName())) {
                            expandReverseRelationshipEnhanced(records, tableInfo, otherTableName, otherFk, embeddedField, allParams);
                            return;
                        }
                    }
                }
            }
            
            log.warn("Relationship '{}' not found for table '{}'", relationshipName, tableInfo.getName());
        } catch (Exception e) {
            log.error("Error expanding enhanced relationship '{}': {}", relationshipName, e.getMessage());
        }
    }
    
    /**
     * Expand forward relationship (Many-to-One) with enhanced filtering
     */
    private void expandForwardRelationshipEnhanced(
            List<Map<String, Object>> records,
            ForeignKeyInfo fk,
            SelectField embeddedField,
            MultiValueMap<String, String> allParams) {
        
        String referencedTable = fk.getReferencedTable();
        String foreignKeyColumn = fk.getColumnName();
        String referencedColumn = fk.getReferencedColumn();
        
        // Collect foreign key values
        Set<Object> fkValues = records.stream()
            .map(record -> record.get(foreignKeyColumn))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
        
        if (fkValues.isEmpty()) {
            return;
        }
        
        // Build enhanced query with column selection and filtering
        String selectClause = buildSelectClause(embeddedField, referencedTable);
        List<Object> queryParams = new ArrayList<>(fkValues);
        String whereClause = buildWhereClause(embeddedField, allParams, referencedColumn, fkValues, queryParams);
        
        String query = String.format("SELECT %s FROM %s WHERE %s", 
            selectClause, referencedTable, whereClause);
        
        log.debug("Enhanced forward relationship query: {}", query);
        
        try {
            List<Map<String, Object>> relatedRecords = jdbcTemplate.queryForList(query, queryParams.toArray());
            
            // Create lookup map
            Map<Object, Map<String, Object>> lookupMap = relatedRecords.stream()
                .collect(Collectors.toMap(
                    record -> record.get(referencedColumn),
                    record -> record,
                    (existing, replacement) -> existing
                ));
            
            // Attach related records to main records
            for (Map<String, Object> record : records) {
                Object fkValue = record.get(foreignKeyColumn);
                Map<String, Object> relatedRecord = lookupMap.get(fkValue);
                record.put(embeddedField.getName(), relatedRecord);
            }
            
        } catch (Exception e) {
            log.error("Error executing enhanced forward relationship query: {}", e.getMessage());
        }
    }
    
    /**
     * Expand reverse relationship (One-to-Many) with enhanced filtering
     */
    private void expandReverseRelationshipEnhanced(
            List<Map<String, Object>> records,
            TableInfo currentTableInfo,
            String relatedTableName,
            ForeignKeyInfo fk,
            SelectField embeddedField,
            MultiValueMap<String, String> allParams) {
        
        String currentPkColumn = currentTableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .findFirst()
            .orElse("id");
        
        String foreignKeyColumn = fk.getColumnName();
        
        // Collect primary key values
        Set<Object> pkValues = records.stream()
            .map(record -> record.get(currentPkColumn))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
        
        if (pkValues.isEmpty()) {
            return;
        }
        
        // Build enhanced query
        String selectClause = buildSelectClause(embeddedField, relatedTableName);
        List<Object> queryParams = new ArrayList<>(pkValues);
        String whereClause = buildWhereClause(embeddedField, allParams, foreignKeyColumn, pkValues, queryParams);
        
        String query = String.format("SELECT %s FROM %s WHERE %s", 
            selectClause, relatedTableName, whereClause);
        
        log.debug("Enhanced reverse relationship query: {}", query);
        
        try {
            List<Map<String, Object>> relatedRecords = jdbcTemplate.queryForList(query, queryParams.toArray());
            
            // Group by foreign key
            Map<Object, List<Map<String, Object>>> groupedRecords = relatedRecords.stream()
                .collect(Collectors.groupingBy(record -> record.get(foreignKeyColumn)));
            
            // Attach related records to main records
            for (Map<String, Object> record : records) {
                Object pkValue = record.get(currentPkColumn);
                List<Map<String, Object>> relatedList = groupedRecords.getOrDefault(pkValue, List.of());
                record.put(embeddedField.getName(), relatedList);
            }
            
        } catch (Exception e) {
            log.error("Error executing enhanced reverse relationship query: {}", e.getMessage());
        }
    }
    
    /**
     * Build SELECT clause based on embedded field column selection
     */
    private String buildSelectClause(SelectField embeddedField, String tableName) {
        if (embeddedField.getSubFields().isEmpty()) {
            return "*"; // No specific columns selected
        }
        
        List<String> selectedColumns = new ArrayList<>();
        
        for (SelectField subField : embeddedField.getSubFields()) {
            if (subField.isWildcard()) {
                return "*"; // Wildcard found, select all
            } else if (subField.isSimpleColumn()) {
                selectedColumns.add(subField.getName());
            }
        }
        
        return selectedColumns.isEmpty() ? "*" : String.join(", ", selectedColumns);
    }
    
    /**
     * Build WHERE clause with embedded filters and FK/PK constraints
     */
    private String buildWhereClause(SelectField embeddedField, MultiValueMap<String, String> allParams,
                                   String keyColumn, Set<Object> keyValues, List<Object> queryParams) {
        
        List<String> conditions = new ArrayList<>();
        
        // Add FK/PK constraint
        String inClause = keyValues.stream()
            .map(val -> "?")
            .collect(Collectors.joining(","));
        conditions.add(String.format("%s IN (%s)", keyColumn, inClause));
        
        // Add embedded filters
        for (Map.Entry<String, String> filter : embeddedField.getFilters().entrySet()) {
            String column = filter.getKey();
            String operatorValue = filter.getValue();
            
            // Parse operator.value format (e.g., "gt.30")
            if (operatorValue.contains(".")) {
                String[] parts = operatorValue.split("\\.", 2);
                String operator = parts[0];
                String value = parts[1];
                
                switch (operator.toLowerCase()) {
                    case "eq":
                        conditions.add(column + " = '" + value + "'");
                        break;
                    case "gt":
                        conditions.add(column + " > '" + value + "'");
                        break;
                    case "gte":
                        conditions.add(column + " >= '" + value + "'");
                        break;
                    case "lt":
                        conditions.add(column + " < '" + value + "'");
                        break;
                    case "lte":
                        conditions.add(column + " <= '" + value + "'");
                        break;
                    case "neq":
                        conditions.add(column + " != '" + value + "'");
                        break;
                    case "like":
                        conditions.add(column + " LIKE '%" + value + "%'");
                        break;
                    default:
                        conditions.add(column + " = '" + value + "'");
                }
            } else {
                conditions.add(column + " = '" + operatorValue + "'");
            }
        }
        
        return String.join(" AND ", conditions);
    }
}