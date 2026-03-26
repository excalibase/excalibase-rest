package io.github.excalibase.postgres.service;

import io.github.excalibase.constant.OperatorConstants;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.SelectField;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.postgres.util.PostgresTypeConverter;
import io.github.excalibase.service.FilterService;
import io.github.excalibase.service.ICrudService;
import io.github.excalibase.service.IQueryBuilderService;
import io.github.excalibase.service.IValidationService;
import io.github.excalibase.service.ServiceLookup;
import io.github.excalibase.service.TypeConversionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class RestApiService {

    private static final Logger log = LoggerFactory.getLogger(RestApiService.class);
    private final JdbcTemplate jdbcTemplate;
    private final DatabaseSchemaService schemaService;
    private final QueryComplexityService complexityService;
    private final RelationshipBatchLoader batchLoader;
    private final SelectParserService selectParserService;
    private final EnhancedRelationshipService enhancedRelationshipService;
    
    // Interface-based services for multi-database support
    private final IValidationService validationService;
    private final TypeConversionService typeConversionService;
    private final FilterService filterService;
    private final IQueryBuilderService queryBuilderService;
    private final ICrudService crudService;
    private final UpsertService upsertService;
    private final ServiceLookup serviceLookup;

    @Value("${app.database-type:postgres}")
    private String databaseType;

    @Value("${app.db-max-rows:1000}")
    private int dbMaxRows;
    
    // Constructor for direct service injection (used by tests and Spring)
    public RestApiService(JdbcTemplate jdbcTemplate, DatabaseSchemaService schemaService,
                         QueryComplexityService complexityService, RelationshipBatchLoader batchLoader,
                         SelectParserService selectParserService, EnhancedRelationshipService enhancedRelationshipService,
                         IValidationService validationService, TypeConversionService typeConversionService,
                         FilterService filterService, IQueryBuilderService queryBuilderService,
                         ICrudService crudService, UpsertService upsertService,
                         @Value("${app.database-type:postgres}") String databaseType) {
        this.jdbcTemplate = jdbcTemplate;
        this.schemaService = schemaService;
        this.complexityService = complexityService;
        this.batchLoader = batchLoader;
        this.selectParserService = selectParserService;
        this.enhancedRelationshipService = enhancedRelationshipService;
        this.validationService = validationService;
        this.typeConversionService = typeConversionService;
        this.filterService = filterService;
        this.queryBuilderService = queryBuilderService;
        this.crudService = crudService;
        this.upsertService = upsertService;
        this.databaseType = databaseType;
        this.serviceLookup = null; // Not used in this constructor

        // Initialize configuration with defaults for tests
        // These will be overridden by @Value when Spring creates the bean
        this.dbMaxRows = 1000; // 1000 rows default
    }


    public Map<String, Object> getRecords(String tableName, MultiValueMap<String, String> allParams,
                                        int offset, int limit, String orderBy,
                                        String orderDirection, String select, String expand,
                                        boolean includeCount) {
        // Security validations
        validationService.validatePaginationParams(offset, limit);
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        // Enforce db-max-rows limit
        if (limit > dbMaxRows) {
            throw new IllegalArgumentException("Requested limit (" + limit + ") exceeds maximum allowed rows (" + dbMaxRows + ")");
        }

        // Validate table exists and permissions (uses permission cache)
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "SELECT");

        // Validate query complexity (if enabled)
        complexityService.validateQueryComplexity(tableName, allParams, limit, expand);

        // Filter out control parameters to get only filter parameters
        MultiValueMap<String, String> filters = new org.springframework.util.LinkedMultiValueMap<>();
        if (allParams != null) {
            for (Map.Entry<String, List<String>> entry : allParams.entrySet()) {
                String key = entry.getKey();
                if (!OperatorConstants.isControlParameter(key)) {
                    filters.put(key, entry.getValue());
                }
            }
        }

        // Build query
        StringBuilder query = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // Parse select fields early — used for both SQL generation and embedded expansion
        List<SelectField> selectFields = null;
        if (select != null && !select.trim().isEmpty()) {
            selectFields = selectParserService.parseSelect(select);
            selectParserService.parseEmbeddedFilters(selectFields, allParams);
            query.append(buildSelectSqlFromFields(selectFields, tableInfo));
        } else {
            query.append("SELECT *");
        }
        query.append(" FROM ").append(tableName);

        // WHERE clause
        if (!filters.isEmpty()) {
            query.append(" WHERE ");
            List<String> conditions = filterService.parseFilters(filters, params, tableInfo);
            query.append(String.join(" AND ", conditions));
        }

        // ORDER BY clause
        if (orderBy != null && !orderBy.trim().isEmpty()) {
            validationService.validateOrderByColumn(orderBy, tableInfo);
            String direction = orderDirection.equalsIgnoreCase("desc") ? "DESC" : "ASC";
            query.append(" ORDER BY \"").append(orderBy).append("\" ").append(direction);
        }

        // Support "order" parameter for ordering
        String order = allParams != null ? allParams.getFirst("order") : null;
        if (order != null && !order.trim().isEmpty() && (orderBy == null || orderBy.trim().isEmpty())) {
            List<String> orderClauses = queryBuilderService.parseOrderBy(order, tableInfo);
            if (!orderClauses.isEmpty()) {
                query.append(" ORDER BY ").append(String.join(", ", orderClauses));
            }
        }

        // LIMIT and OFFSET
        query.append(" LIMIT ").append(limit).append(" OFFSET ").append(offset);

        // Execute query
        List<Map<String, Object>> records = jdbcTemplate.queryForList(query.toString(), params.toArray());

        // Convert all PostgreSQL types using comprehensive converter (GraphQL parity)
        if (records != null) {
            records = PostgresTypeConverter.convertPostgresTypes(records, tableInfo);
        }

        // Handle enhanced select with embedded fields or legacy expand parameter
        if (selectFields != null) {
            List<SelectField> embeddedFields = selectParserService.getEmbeddedFields(selectFields);
            if (!embeddedFields.isEmpty()) {
                records = enhancedRelationshipService.expandRelationships(records, tableInfo, embeddedFields, allParams);
            }
        } else if (expand != null && !expand.trim().isEmpty()) {
            // Fallback to legacy expand functionality
            records = expandRelationships(records, tableInfo, expand);
        }
        
        // COUNT(*) only when explicitly requested (Prefer: count=exact)
        // Skipping it by default saves one DB round-trip per request
        Long totalCount = null;
        if (includeCount) {
            String countQuery = "SELECT COUNT(*) FROM " + tableName;
            List<Object> countParams = new ArrayList<>();
            if (!filters.isEmpty()) {
                countQuery += " WHERE ";
                List<String> conditions = filterService.parseFilters(filters, countParams, tableInfo);
                countQuery += String.join(" AND ", conditions);
            }
            totalCount = jdbcTemplate.queryForObject(countQuery, Long.class, countParams.toArray());
        }

        Map<String, Object> result = new HashMap<>();
        result.put("data", records);
        Map<String, Object> pagination = new HashMap<>();
        pagination.put("offset", offset);
        pagination.put("limit", limit);
        pagination.put("hasMore", records.size() == limit);
        if (totalCount != null) {
            pagination.put("total", totalCount);
        }
        result.put("pagination", pagination);
        
        // Clear batch cache to prevent memory issues
        batchLoader.clearCache();
        
        return result;
    }

    public Map<String, Object> getRecord(String tableName, String id, String select, String expand) {
        // Validate input
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("ID cannot be empty");
        }
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "SELECT");

        // Get primary key columns (supports composite keys)
        List<ColumnInfo> primaryKeyColumns = getPrimaryKeyColumns(tableInfo);
        if (primaryKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("Table " + tableName + " has no primary key defined");
        }

        // Build query — use enhanced select clause builder
        List<SelectField> selectFields = null;
        String selectSql;
        if (select != null && !select.trim().isEmpty()) {
            selectFields = selectParserService.parseSelect(select);
            selectSql = buildSelectSqlFromFields(selectFields, tableInfo);
        } else {
            selectSql = "SELECT *";
        }
        String selectClause = selectSql.substring(7); // Remove "SELECT "

        // Parse composite key values and build WHERE clause
        String[] keyValues = queryBuilderService.parseCompositeKey(id, primaryKeyColumns.size());
        List<String> whereConditions = queryBuilderService.buildCompositeKeyConditions(primaryKeyColumns, tableInfo);
        List<Object> queryParams = new ArrayList<>();
        
        for (int i = 0; i < primaryKeyColumns.size(); i++) {
            ColumnInfo pkColumn = primaryKeyColumns.get(i);
            queryParams.add(typeConversionService.convertValueToColumnType(pkColumn.getName(), keyValues[i], tableInfo));
        }
        
        String query = "SELECT " + selectClause + " FROM " + tableName + " WHERE " + String.join(" AND ", whereConditions);
        
        List<Map<String, Object>> results = jdbcTemplate.queryForList(query, queryParams.toArray());
        if (results.isEmpty()) {
            return null;
        }
        
        // Convert all PostgreSQL types using comprehensive converter
        results = PostgresTypeConverter.convertPostgresTypes(results, tableInfo);
        
        Map<String, Object> result = results.get(0);
        
        // Handle enhanced select with embedded fields or legacy expand parameter
        if (selectFields != null) {
            // Note: For single record, we don't have allParams, so no embedded filters
            List<SelectField> embeddedFields = selectParserService.getEmbeddedFields(selectFields);
            if (!embeddedFields.isEmpty()) {
                // Use enhanced relationship expansion
                List<Map<String, Object>> singleRecord = List.of(result);
                List<Map<String, Object>> expandedRecords = enhancedRelationshipService.expandRelationships(
                    singleRecord, tableInfo, embeddedFields, new org.springframework.util.LinkedMultiValueMap<>());
                result = expandedRecords.isEmpty() ? result : expandedRecords.get(0);
            }
        } else if (expand != null && !expand.trim().isEmpty()) {
            // Fallback to legacy expand functionality
            List<Map<String, Object>> singleRecord = List.of(result);
            List<Map<String, Object>> expandedRecords = expandRelationships(singleRecord, tableInfo, expand);
            result = expandedRecords.isEmpty() ? result : expandedRecords.get(0);
        }
        
        // Clear batch cache to prevent memory issues
        batchLoader.clearCache();
        
        return result;
    }

    public Map<String, Object> getRecordsWithCursor(String tableName, MultiValueMap<String, String> allParams, 
                                                   String first, String after, String last, String before,
                                                   String orderBy, String orderDirection, String select, String expand) {
        
        // Validate table exists and permissions
        TableInfo tableInfo = validationService.getValidatedTableInfo(tableName);
        validationService.validateTablePermission(tableName, "SELECT");
        
        // Determine pagination parameters
        int limit = 100; // Default
        boolean forward = true; // Default direction
        
        if (first != null) {
            limit = Math.min(Integer.parseInt(first), validationService.getMaxLimit());
            forward = true;
        } else if (last != null) {
            limit = Math.min(Integer.parseInt(last), validationService.getMaxLimit());
            forward = false;
        }
        
        // Validate query complexity
        complexityService.validateQueryComplexity(tableName, allParams, limit, expand);
        
        // Get primary key for cursor
        String primaryKeyColumn = tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .findFirst()
            .orElse("id");
        
        // If no explicit orderBy, use primary key
        if (orderBy == null || orderBy.trim().isEmpty()) {
            orderBy = primaryKeyColumn;
        }
        
        // Filter out control parameters to get only filter parameters
        MultiValueMap<String, String> filters = new org.springframework.util.LinkedMultiValueMap<>();
        if (allParams != null) {
            for (Map.Entry<String, List<String>> entry : allParams.entrySet()) {
                String key = entry.getKey();
                if (!OperatorConstants.isControlParameter(key)) {
                    filters.put(key, entry.getValue());
                }
            }
        }
        
        // Build query
        StringBuilder query = new StringBuilder();
        List<Object> params = new ArrayList<>();

        // SELECT clause — enhanced
        if (select != null && !select.trim().isEmpty()) {
            List<SelectField> cursorSelectFields = selectParserService.parseSelect(select);
            query.append(buildSelectSqlFromFields(cursorSelectFields, tableInfo));
        } else {
            query.append("SELECT *");
        }
        query.append(" FROM ").append(tableName);

        // WHERE clause (including cursor conditions)
        List<String> conditions = new ArrayList<>();
        
        // Add regular filters
        if (!filters.isEmpty()) {
            conditions.addAll(filterService.parseFilters(filters, params, tableInfo));
        }
        
        // Add cursor conditions
        if (after != null) {
            String decodedCursor = queryBuilderService.decodeCursor(after);
            if (forward) {
                conditions.add("\"" + orderBy + "\" > ?");
            } else {
                conditions.add("\"" + orderBy + "\" < ?");
            }
            params.add(typeConversionService.convertValueToColumnType(orderBy, decodedCursor, tableInfo));
        }

        if (before != null) {
            String decodedCursor = queryBuilderService.decodeCursor(before);
            if (forward) {
                conditions.add("\"" + orderBy + "\" < ?");
            } else {
                conditions.add("\"" + orderBy + "\" > ?");
            }
            params.add(typeConversionService.convertValueToColumnType(orderBy, decodedCursor, tableInfo));
        }
        
        if (!conditions.isEmpty()) {
            query.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        
        // ORDER BY clause
        String direction = forward ? 
            (orderDirection.equalsIgnoreCase("desc") ? "DESC" : "ASC") :
            (orderDirection.equalsIgnoreCase("desc") ? "ASC" : "DESC"); // Reverse for backward pagination
            
        query.append(" ORDER BY \"").append(orderBy).append("\" ").append(direction);

        // LIMIT
        query.append(" LIMIT ").append(limit + 1); // Fetch one extra to check hasNextPage/hasPreviousPage
        
        // Execute query
        List<Map<String, Object>> records = jdbcTemplate.queryForList(query.toString(), params.toArray());
        
        // Convert all PostgreSQL types using comprehensive converter (GraphQL parity)
        if (records != null) {
            records = PostgresTypeConverter.convertPostgresTypes(records, tableInfo);
        }
        
        // Check if there are more records
        boolean hasMore = records.size() > limit;
        if (hasMore) {
            records = records.subList(0, limit); // Remove the extra record
        }
        
        // If backward pagination, reverse the results
        if (!forward) {
            Collections.reverse(records);
        }
        
        // Handle enhanced select with embedded fields or legacy expand parameter
        if (select != null && !select.trim().isEmpty()) {
            // Parse select parameter for relationship embedding
            List<SelectField> selectFields = selectParserService.parseSelect(select);
            selectParserService.parseEmbeddedFilters(selectFields, allParams);
            
            List<SelectField> embeddedFields = selectParserService.getEmbeddedFields(selectFields);
            if (!embeddedFields.isEmpty()) {
                // Use enhanced relationship expansion
                records = enhancedRelationshipService.expandRelationships(records, tableInfo, embeddedFields, allParams);
            }
        } else if (expand != null && !expand.trim().isEmpty()) {
            // Fallback to legacy expand functionality
            records = expandRelationships(records, tableInfo, expand);
        }
        
        // Create edges with cursors
        List<Map<String, Object>> edges = new ArrayList<>();
        for (Map<String, Object> record : records) {
            Object cursorValue = record.get(orderBy);
            String cursor = queryBuilderService.encodeCursor(cursorValue != null ? cursorValue.toString() : "");
            
            Map<String, Object> edge = new HashMap<>();
            edge.put("node", record);
            edge.put("cursor", cursor);
            edges.add(edge);
        }
        
        // Create page info
        Map<String, Object> pageInfo = new HashMap<>();
        if (!edges.isEmpty()) {
            pageInfo.put("startCursor", edges.get(0).get("cursor"));
            pageInfo.put("endCursor", edges.get(edges.size() - 1).get("cursor"));
        } else {
            pageInfo.put("startCursor", null);
            pageInfo.put("endCursor", null);
        }
        
        if (forward) {
            pageInfo.put("hasNextPage", hasMore);
            pageInfo.put("hasPreviousPage", after != null);
        } else {
            pageInfo.put("hasNextPage", before != null);
            pageInfo.put("hasPreviousPage", hasMore);
        }
        
        // Get total count
        String countQuery = "SELECT COUNT(*) FROM " + tableName;
        List<Object> countParams = new ArrayList<>();
        if (!filters.isEmpty()) {
            countQuery += " WHERE ";
            List<String> countConditions = filterService.parseFilters(filters, countParams, tableInfo);
            countQuery += String.join(" AND ", countConditions);
        }
        Long totalCount = jdbcTemplate.queryForObject(countQuery, Long.class, countParams.toArray());
        
        // Build connection response
        Map<String, Object> result = new HashMap<>();
        result.put("edges", edges);
        result.put("pageInfo", pageInfo);
        result.put("totalCount", totalCount != null ? totalCount : 0);
        
        // Clear batch cache to prevent memory issues
        batchLoader.clearCache();
        
        return result;
    }
    
    // CRUD operations - delegate to CrudService
    public Map<String, Object> createRecord(String tableName, Map<String, Object> data) {
        return crudService.createRecord(tableName, data);
    }

    public List<Map<String, Object>> createBulkRecords(String tableName, List<Map<String, Object>> dataList) {
        return crudService.createBulkRecords(tableName, dataList);
    }

    public Map<String, Object> updateRecord(String tableName, String id, Map<String, Object> data, boolean isPartial) {
        return crudService.updateRecord(tableName, id, data);
    }

    public List<Map<String, Object>> updateBulkRecords(String tableName, List<Map<String, Object>> updateList) {
        return crudService.updateBulkRecords(tableName, updateList);
    }

    public boolean deleteRecord(String tableName, String id) {
        return crudService.deleteRecord(tableName, id);
    }

    public Map<String, Object> deleteRecordsByFilters(String tableName, MultiValueMap<String, String> filters) {
        return crudService.deleteRecordsByFilters(tableName, filters, filterService);
    }

    public Map<String, Object> updateRecordsByFilters(String tableName, MultiValueMap<String, String> filters, Map<String, Object> updateData) {
        return crudService.updateRecordsByFilters(tableName, filters, updateData, filterService);
    }

    // Upsert operations - delegate to UpsertService
    public Map<String, Object> upsertRecord(String tableName, Map<String, Object> data) {
        return upsertService.upsertRecord(tableName, data);
    }

    public List<Map<String, Object>> upsertBulkRecords(String tableName, List<Map<String, Object>> dataList) {
        return upsertService.upsertBulkRecords(tableName, dataList);
    }

    public String extractEnumTypeFromMessage(String message) {
        // This method was private in the original, now we can delegate or implement here
        if (message == null) return "unknown";
        
        // Pattern: 'invalid input value for enum enum_type_name'
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("invalid input value for enum ([a-zA-Z_][a-zA-Z0-9_]*)");
        java.util.regex.Matcher matcher = pattern.matcher(message);
        
        if (matcher.find()) {
            return matcher.group(1);
        }
        
        return "unknown";
    }
    
    /**
     * Expand relationships in records based on expand parameter
     * Supports syntax: "customer" or "customer(limit:5)" or "customer,orders"
     */
    private List<Map<String, Object>> expandRelationships(List<Map<String, Object>> records, TableInfo tableInfo, String expand) {
        if (records.isEmpty()) {
            return records;
        }
        
        String[] expansions = expand.split(",");
        for (String expansion : expansions) {
            expansion = expansion.trim();
            if (expansion.isEmpty()) {
                continue;
            }
            
            // Parse expansion (e.g., "customer(limit:5)" -> "customer", limit=5)
            String relationshipName = expansion;
            Map<String, String> expansionParams = new HashMap<>();
            
            if (expansion.contains("(") && expansion.endsWith(")")) {
                int parenIndex = expansion.indexOf("(");
                relationshipName = expansion.substring(0, parenIndex);
                String paramsStr = expansion.substring(parenIndex + 1, expansion.length() - 1);
                
                // Parse parameters like "limit:5,select:name"
                String[] paramPairs = paramsStr.split(",");
                for (String pair : paramPairs) {
                    String[] keyValue = pair.split(":");
                    if (keyValue.length == 2) {
                        expansionParams.put(keyValue[0].trim(), keyValue[1].trim());
                    }
                }
            }
            
            expandSingleRelationship(records, tableInfo, relationshipName, expansionParams);
        }
        
        return records;
    }
    
    /**
     * Expand a single relationship for all records
     */
    private void expandSingleRelationship(List<Map<String, Object>> records, TableInfo tableInfo, 
                                        String relationshipName, Map<String, String> expansionParams) {
        try {
            Map<String, TableInfo> allTables = schemaService.getTableSchema();
            
            // Check if it's a forward relationship (FK in current table)
            for (var fk : tableInfo.getForeignKeys()) {
                if (fk.getReferencedTable().equalsIgnoreCase(relationshipName)) {
                    expandForwardRelationship(records, fk, expansionParams);
                    return;
                }
            }
            
            // Check if it's a reverse relationship (FK in other table pointing to this table)
            for (var otherTableEntry : allTables.entrySet()) {
                String otherTableName = otherTableEntry.getKey();
                TableInfo otherTableInfo = otherTableEntry.getValue();
                
                if (otherTableName.equalsIgnoreCase(relationshipName)) {
                    // Look for FK in other table pointing to current table
                    for (var otherFk : otherTableInfo.getForeignKeys()) {
                        if (otherFk.getReferencedTable().equalsIgnoreCase(tableInfo.getName())) {
                            expandReverseRelationship(records, tableInfo, otherTableName, otherFk, expansionParams);
                            return;
                        }
                    }
                }
            }
            
            log.warn("Relationship '{}' not found for table '{}'", relationshipName, tableInfo.getName());
        } catch (Exception e) {
            log.error("Error expanding relationship '{}': {}", relationshipName, e.getMessage());
        }
    }
    
    /**
     * Expand forward relationship (Many-to-One): current table has FK to referenced table
     * Uses batch loading to prevent N+1 queries
     */
    private void expandForwardRelationship(List<Map<String, Object>> records, ForeignKeyInfo fk, Map<String, String> expansionParams) {
        String referencedTable = fk.getReferencedTable();
        String foreignKeyColumn = fk.getColumnName();
        String referencedColumn = fk.getReferencedColumn();
        
        // Collect all foreign key values
        Set<Object> fkValues = records.stream()
            .map(record -> record.get(foreignKeyColumn))
            .filter(java.util.Objects::nonNull)
            .collect(Collectors.toSet());
        
        if (fkValues.isEmpty()) {
            return;
        }
        
        // Use batch loader for efficient querying
        String selectClause = expansionParams.getOrDefault("select", "*");
        Map<Object, Map<String, Object>> relatedLookup = batchLoader.loadSingleRelatedRecords(
            referencedTable, referencedColumn, fkValues, selectClause);
        
        // Add related records to main records
        for (Map<String, Object> record : records) {
            Object fkValue = record.get(foreignKeyColumn);
            if (fkValue != null && relatedLookup.containsKey(fkValue)) {
                record.put(referencedTable, relatedLookup.get(fkValue));
            }
        }
        
        log.debug("Expanded forward relationship {} -> {} for {} records using batch loading", 
                 foreignKeyColumn, referencedTable, records.size());
    }
    
    /**
     * Expand reverse relationship (One-to-Many): other table has FK to current table
     * Uses batch loading to prevent N+1 queries
     */
    private void expandReverseRelationship(List<Map<String, Object>> records, TableInfo currentTableInfo, 
                                         String relatedTableName, ForeignKeyInfo fk, Map<String, String> expansionParams) {
        String currentPkColumn = currentTableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .map(ColumnInfo::getName)
            .findFirst()
            .orElse("id");
        
        String foreignKeyColumn = fk.getColumnName();
        
        // Collect all primary key values
        Set<Object> pkValues = records.stream()
            .map(record -> record.get(currentPkColumn))
            .filter(java.util.Objects::nonNull)
            .collect(Collectors.toSet());
        
        if (pkValues.isEmpty()) {
            return;
        }
        
        // Use batch loader for efficient querying
        String selectClause = expansionParams.getOrDefault("select", "*");
        int limit = 0; // Default to no limit
        if (expansionParams.containsKey("limit")) {
            try {
                limit = Math.min(Integer.parseInt(expansionParams.get("limit")), validationService.getMaxLimit());
            } catch (NumberFormatException e) {
                log.warn("Invalid limit parameter: {}", expansionParams.get("limit"));
            }
        }
        
        Map<Object, List<Map<String, Object>>> relatedGrouped = batchLoader.loadRelatedRecords(
            relatedTableName, foreignKeyColumn, currentPkColumn, pkValues, selectClause, limit);
        
        // Add related records to main records
        for (Map<String, Object> record : records) {
            Object pkValue = record.get(currentPkColumn);
            if (pkValue != null && relatedGrouped.containsKey(pkValue)) {
                record.put(relatedTableName, relatedGrouped.get(pkValue));
            } else {
                record.put(relatedTableName, new ArrayList<>());
            }
        }
        
        log.debug("Expanded reverse relationship {} -> {} for {} records using batch loading", 
                 relatedTableName, foreignKeyColumn, records.size());
    }
    
    /**
     * Get all primary key columns from a table (supports composite keys)
     */
    private List<ColumnInfo> getPrimaryKeyColumns(TableInfo tableInfo) {
        return tableInfo.getColumns().stream()
            .filter(ColumnInfo::isPrimaryKey)
            .collect(Collectors.toList());
    }
    
    /**
     * Extract column name from constraint violation message
     */
    public String extractColumnNameFromConstraint(String message, String constraintType) {
        return validationService.extractColumnNameFromConstraint(message, constraintType);
    }

    /**
     * Handle SQL constraint violations
     */
    public void handleSqlConstraintViolation(SQLException e, String tableName, Map<String, Object> data) {
        validationService.handleSqlConstraintViolation(e, tableName, data);
    }

    /**
    /**
     * Build a SQL SELECT clause from parsed SelectField objects, applying extended
     * alias, type cast, and JSON path syntax. Embedded (relationship) fields are skipped here
     * since they are handled via JOIN/subquery expansion separately.
     */
    private String buildSelectSqlFromFields(List<SelectField> fields, TableInfo tableInfo) {
        Set<String> validColumns = tableInfo.getColumns().stream()
            .map(ColumnInfo::getName)
            .collect(Collectors.toSet());

        List<String> sqlExprs = new ArrayList<>();
        for (SelectField field : fields) {
            if (field.isEmbedded()) continue;
            if (field.isWildcard()) {
                sqlExprs.add("*");
                continue;
            }
            // Validate the base column name (strip JSON path operator to get the column)
            String baseName = field.getName().contains("->")
                ? field.getName().substring(0, field.getName().indexOf("->"))
                : field.getName();
            if (!field.isJsonPath() && !validColumns.contains(baseName)) {
                throw new IllegalArgumentException("Invalid column: " + baseName);
            }
            if (field.isJsonPath() && !validColumns.contains(baseName)) {
                throw new IllegalArgumentException("Invalid column: " + baseName);
            }
            sqlExprs.add(field.toSqlExpression());
        }

        if (sqlExprs.isEmpty()) return "SELECT *";
        return "SELECT " + String.join(", ", sqlExprs);
    }
}
