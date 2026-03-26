package io.github.excalibase.postgres.service;

import io.github.excalibase.annotation.ExcalibaseService;
import io.github.excalibase.constant.SupportedDatabaseConstant;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.service.IQueryBuilderService;
import io.github.excalibase.service.IValidationService;
import io.github.excalibase.service.TypeConversionService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ExcalibaseService(serviceName = SupportedDatabaseConstant.POSTGRES)
public class QueryBuilderService implements IQueryBuilderService {

    private final IValidationService validationService;
    private final TypeConversionService typeConversionService;

    public QueryBuilderService(IValidationService validationService, TypeConversionService typeConversionService) {
        this.validationService = validationService;
        this.typeConversionService = typeConversionService;
    }

    /**
     * Quote a SQL identifier with double quotes to handle spaces and reserved words.
     */
    private static String q(String identifier) {
        return "\"" + identifier + "\"";
    }

    /**
     * Parse SQL-style order parameter: order=age.desc,height.asc
     */
    public List<String> parseOrderBy(String order, TableInfo tableInfo) {
        List<String> orderClauses = new ArrayList<>();
        Set<String> validColumns = tableInfo.getColumns().stream()
            .map(ColumnInfo::getName)
            .collect(Collectors.toSet());

        String[] orderParts = order.split(",");
        for (String part : orderParts) {
            part = part.trim();

            String column;
            String direction = "ASC";
            String nullsOrder = "";

            if (part.contains(".")) {
                String[] dotParts = part.split("\\.");
                column = dotParts[0];

                for (int i = 1; i < dotParts.length; i++) {
                    String modifier = dotParts[i].toLowerCase();
                    if (modifier.equals("asc") || modifier.equals("desc")) {
                        direction = modifier.toUpperCase();
                    } else if (modifier.equals("nullsfirst")) {
                        nullsOrder = " NULLS FIRST";
                    } else if (modifier.equals("nullslast")) {
                        nullsOrder = " NULLS LAST";
                    }
                }
            } else {
                column = part;
            }

            if (!validColumns.contains(column)) {
                throw new IllegalArgumentException("Invalid column for ordering: " + column);
            }

            orderClauses.add(q(column) + " " + direction + nullsOrder);
        }
        
        return orderClauses;
    }

    /**
     * Build SELECT clause with column validation
     */
    public String buildSelectClause(String select, TableInfo tableInfo) {
        if (select != null && !select.trim().isEmpty()) {
            validationService.validateSelectColumns(select, tableInfo);
            return "SELECT " + select;
        } else {
            return "SELECT *";
        }
    }

    /**
     * Build INSERT query with proper type casting
     */
    public String buildInsertQuery(String tableName, List<String> columns, TableInfo tableInfo) {
        String columnList = columns.stream().map(QueryBuilderService::q).collect(Collectors.joining(", "));

        List<String> placeholderList = new ArrayList<>();
        for (String columnName : columns) {
            placeholderList.add(typeConversionService.buildPlaceholderWithCast(columnName, tableInfo));
        }
        String placeholders = String.join(", ", placeholderList);

        return "INSERT INTO " + tableName + " (" + columnList + ") VALUES (" + placeholders + ") RETURNING *";
    }

    /**
     * Build bulk INSERT query with proper type casting
     */
    public String buildBulkInsertQuery(String tableName, List<String> columns, int recordCount, TableInfo tableInfo) {
        String columnList = columns.stream().map(QueryBuilderService::q).collect(Collectors.joining(", "));
        String placeholderRow = "(" + columns.stream()
            .map(c -> typeConversionService.buildPlaceholderWithCast(c, tableInfo))
            .collect(Collectors.joining(", ")) + ")";

        List<String> valuePlaceholders = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            valuePlaceholders.add(placeholderRow);
        }

        return "INSERT INTO " + tableName + " (" + columnList + ") VALUES " +
               String.join(", ", valuePlaceholders) + " RETURNING *";
    }

    /**
     * Build UPDATE query with proper type casting
     */
    public String buildUpdateQuery(String tableName, List<String> updateColumns, List<String> whereConditions, TableInfo tableInfo) {
        List<String> setParts = new ArrayList<>();
        for (String column : updateColumns) {
            setParts.add(q(column) + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo));
        }

        String whereClause = String.join(" AND ", whereConditions);

        return "UPDATE " + tableName + " SET " + String.join(", ", setParts) +
               " WHERE " + whereClause + " RETURNING *";
    }

    /**
     * Build DELETE query
     */
    public String buildDeleteQuery(String tableName, List<String> whereConditions) {
        String whereClause = String.join(" AND ", whereConditions);
        return "DELETE FROM " + tableName + " WHERE " + whereClause;
    }

    /**
     * Build UPSERT query with conflict resolution
     */
    public String buildUpsertQuery(String tableName, List<String> columns, List<String> primaryKeyColumns,
                                   List<String> updateColumns, TableInfo tableInfo) {
        String columnListStr = columns.stream().map(QueryBuilderService::q).collect(Collectors.joining(", "));
        String placeholders = columns.stream()
            .map(c -> typeConversionService.buildPlaceholderWithCast(c, tableInfo))
            .collect(Collectors.joining(", "));

        String conflictColumns = primaryKeyColumns.stream().map(QueryBuilderService::q).collect(Collectors.joining(", "));

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(tableName)
                  .append(" (").append(columnListStr).append(") VALUES (")
                  .append(placeholders).append(")");

        if (!updateColumns.isEmpty()) {
            String updateSet = updateColumns.stream()
                .map(col -> q(col) + " = EXCLUDED." + q(col))
                .collect(Collectors.joining(", "));
            sqlBuilder.append(" ON CONFLICT (").append(conflictColumns).append(")")
                      .append(" DO UPDATE SET ").append(updateSet);
        } else {
            sqlBuilder.append(" ON CONFLICT (").append(conflictColumns).append(")")
                      .append(" DO NOTHING");
        }

        sqlBuilder.append(" RETURNING *");
        return sqlBuilder.toString();
    }

    /**
     * Build bulk UPSERT query
     */
    public String buildBulkUpsertQuery(String tableName, List<String> columns, List<String> primaryKeyColumns,
                                       List<String> updateColumns, int recordCount, TableInfo tableInfo) {
        String columnListStr = columns.stream().map(QueryBuilderService::q).collect(Collectors.joining(", "));
        String placeholderRow = "(" + columns.stream()
            .map(c -> typeConversionService.buildPlaceholderWithCast(c, tableInfo))
            .collect(Collectors.joining(", ")) + ")";

        List<String> valuePlaceholders = new ArrayList<>();
        for (int i = 0; i < recordCount; i++) {
            valuePlaceholders.add(placeholderRow);
        }

        String conflictColumns = primaryKeyColumns.stream().map(QueryBuilderService::q).collect(Collectors.joining(", "));

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(tableName)
                  .append(" (").append(columnListStr).append(") VALUES ")
                  .append(String.join(", ", valuePlaceholders));

        if (!updateColumns.isEmpty()) {
            String updateSet = updateColumns.stream()
                .map(col -> q(col) + " = EXCLUDED." + q(col))
                .collect(Collectors.joining(", "));
            sqlBuilder.append(" ON CONFLICT (").append(conflictColumns).append(")")
                      .append(" DO UPDATE SET ").append(updateSet);
        } else {
            sqlBuilder.append(" ON CONFLICT (").append(conflictColumns).append(")")
                      .append(" DO NOTHING");
        }

        sqlBuilder.append(" RETURNING *");
        return sqlBuilder.toString();
    }

    /**
     * Parse composite key string into individual key values
     * Format: "value1,value2,value3" for composite keys or "value" for single key
     */
    public String[] parseCompositeKey(String compositeKey, int expectedKeyCount) {
        if (compositeKey == null || compositeKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
        
        // Handle single key case
        if (expectedKeyCount == 1) {
            return new String[]{compositeKey.trim()};
        }
        
        // Handle composite key case
        String[] keyParts = compositeKey.split(",");
        
        // Trim each part
        for (int i = 0; i < keyParts.length; i++) {
            keyParts[i] = keyParts[i].trim();
        }
        
        // Validate key part count
        if (keyParts.length != expectedKeyCount) {
            if (expectedKeyCount == 1) {
                throw new IllegalArgumentException("Single key expected, but got composite key format");
            } else {
                throw new IllegalArgumentException("Composite key requires " + expectedKeyCount + " parts, got " + keyParts.length);
            }
        }
        
        // Validate no empty parts
        for (int i = 0; i < keyParts.length; i++) {
            if (keyParts[i].isEmpty()) {
                throw new IllegalArgumentException("Invalid composite key format: empty key part at position " + (i + 1));
            }
        }
        
        return keyParts;
    }

    /**
     * Build WHERE conditions for composite key
     */
    public List<String> buildCompositeKeyConditions(List<ColumnInfo> primaryKeyColumns, TableInfo tableInfo) {
        List<String> whereConditions = new ArrayList<>();
        
        for (ColumnInfo pkColumn : primaryKeyColumns) {
            whereConditions.add(q(pkColumn.getName()) + " = " +
                typeConversionService.buildPlaceholderWithCast(pkColumn.getName(), tableInfo));
        }
        
        return whereConditions;
    }

    /**
     * Encode cursor value to base64
     */
    public String encodeCursor(String value) {
        try {
            return java.util.Base64.getEncoder().encodeToString(value.getBytes("UTF-8"));
        } catch (Exception e) {
            return "";
        }
    }
    
    /**
     * Decode cursor value from base64
     */
    public String decodeCursor(String cursor) {
        try {
            return new String(java.util.Base64.getDecoder().decode(cursor), "UTF-8");
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid cursor: " + cursor);
        }
    }
}
