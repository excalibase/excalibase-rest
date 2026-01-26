package io.github.excalibase.service;

import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import io.github.excalibase.service.IValidationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class FilterService {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(FilterService.class);
    private final IValidationService validationService;
    private final TypeConversionService typeConversionService;
    private final ObjectMapper objectMapper;

    public FilterService(IValidationService validationService, TypeConversionService typeConversionService) {
        this.validationService = validationService;
        this.typeConversionService = typeConversionService;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Parse filters in format: column=operator.value
     * Also supports OR conditions: or=(age.gte.18,student.is.true)
     */
    public List<String> parseFilters(MultiValueMap<String, String> filters, List<Object> params, TableInfo tableInfo) {
        List<String> conditions = new ArrayList<>();
        
        Set<String> validColumns = tableInfo.getColumns().stream()
            .map(ColumnInfo::getName)
            .collect(Collectors.toSet());
        
        for (Map.Entry<String, List<String>> filterEntry : filters.entrySet()) {
            String key = filterEntry.getKey();
            List<String> values = filterEntry.getValue();
            
            if (key.equals("or")) {
                // Handle OR conditions: or=(age.gte.18,student.is.true)
                for (String value : values) {
                    String orCondition = parseOrCondition(value, params, validColumns, tableInfo);
                    if (orCondition != null) {
                        conditions.add("(" + orCondition + ")");
                    }
                }
            } else {
                // Handle regular conditions: age=gte.18 (can have multiple values for AND logic)
                for (String value : values) {
                    String condition = parseCondition(key, value, params, validColumns, tableInfo);
                    if (condition != null) {
                        conditions.add(condition);
                    }
                }
            }
        }
        
        return conditions;
    }
    
    /**
     * Parse OR condition: or=(age.gte.18,student.is.true)
     */
    private String parseOrCondition(String orValue, List<Object> params, Set<String> validColumns, TableInfo tableInfo) {
        // Remove parentheses if present
        if (orValue.startsWith("(") && orValue.endsWith(")")) {
            orValue = orValue.substring(1, orValue.length() - 1);
        }
        
        String[] orConditions = orValue.split(",");
        List<String> parsedConditions = new ArrayList<>();
        
        for (String condition : orConditions) {
            condition = condition.trim();
            // Split by first dot to get column and operator.value
            int firstDot = condition.indexOf('.');
            if (firstDot > 0) {
                String column = condition.substring(0, firstDot);
                String operatorValue = condition.substring(firstDot + 1);
                
                String parsedCondition = parseCondition(column, operatorValue, params, validColumns, tableInfo);
                if (parsedCondition != null) {
                    parsedConditions.add(parsedCondition);
                }
            }
        }
        
        return parsedConditions.isEmpty() ? null : String.join(" OR ", parsedConditions);
    }
    
    /**
     * Parse single condition: column=operator.value
     */
    private String parseCondition(String column, String value, List<Object> params, Set<String> validColumns, TableInfo tableInfo) {
        // Validate column exists
        if (!validColumns.contains(column)) {
            throw new IllegalArgumentException("Invalid column for filtering: " + column);
        }
        
        // Basic SQL injection protection
        validationService.validateFilterValue(value);
        
        // Parse operator.value format
        if (!value.contains(".")) {
            // No operator specified, default to equality
            params.add(typeConversionService.convertValueToColumnType(column, value, tableInfo));
            return column + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
        }
        
        int firstDot = value.indexOf('.');
        String operator = value.substring(0, firstDot);
        String operatorValue = value.substring(firstDot + 1);
        
        return buildConditionForOperator(column, operator, operatorValue, params, tableInfo);
    }

    /**
     * Build SQL condition for specific operator
     */
    private String buildConditionForOperator(String column, String operator, String operatorValue, List<Object> params, TableInfo tableInfo) {
        switch (operator.toLowerCase()) {
            case "eq":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
                
            case "neq":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " <> " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
                
            case "gt":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " > " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
                
            case "gte":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " >= " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
                
            case "lt":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " < " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
                
            case "lte":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " <= " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
                
            case "like":
                params.add("%" + operatorValue + "%");
                return column + " LIKE ?";
                
            case "ilike":
                params.add("%" + operatorValue + "%");
                return column + " ILIKE ?";
                
            case "in":
                return buildInCondition(column, operatorValue, params, tableInfo);
                
            case "notin":
                return buildNotInCondition(column, operatorValue, params, tableInfo);
                
            case "is":
                return buildIsCondition(column, operatorValue, params, tableInfo);
                
            case "isnotnull":
                return column + " IS NOT NULL";
                
            case "startswith":
                params.add(operatorValue + "%");
                return column + " LIKE ?";
                
            case "endswith":
                params.add("%" + operatorValue);
                return column + " LIKE ?";
                
            // JSON operators
            case "haskey":
                params.add(operatorValue);
                return "jsonb_exists(" + column + ", ?)";
                
            case "haskeys":
                return buildJsonHasKeysCondition(column, operatorValue, "?&");
                
            case "hasanykeys":
                return buildJsonHasKeysCondition(column, operatorValue, "?|");
                
            case "jsoncontains":
            case "contains":
                return buildJsonContainsCondition(column, operatorValue, params, "@>");
                
            case "jsoncontained":
            case "containedin":
                return buildJsonContainsCondition(column, operatorValue, params, "<@");
                
            case "jsonexists":
            case "exists":
                params.add(operatorValue);
                return column + " ? ?";
                
            case "jsonexistsany":
            case "existsany":
                return buildJsonHasKeysCondition(column, operatorValue, "?|");
                
            case "jsonexistsall":
            case "existsall":
                return buildJsonHasKeysCondition(column, operatorValue, "?&");
                
            case "jsonpath":
                params.add(operatorValue);
                return column + " @? ?::jsonpath";
                
            case "jsonpathexists":
                params.add(operatorValue);
                return column + " @@ ?::jsonpath";
                
            // Array operators
            case "arraycontains":
                params.add(operatorValue);
                return column + " @> ARRAY[?]::" + typeConversionService.getColumnType(column, tableInfo) + "[]";
                
            case "arrayhasany":
                return buildArrayHasCondition(column, operatorValue, "&&", tableInfo);
                
            case "arrayhasall":
                return buildArrayHasAllCondition(column, operatorValue, params);
                
            case "arraylength":
                params.add(Integer.parseInt(operatorValue));
                return "array_length(" + column + ", 1) = ?";
                
            // Full-text search operators
            case "fts":
                params.add(operatorValue);
                return "to_tsvector('english', " + column + ") @@ plainto_tsquery('english', ?)";
                
            case "plfts":
                params.add(operatorValue);
                return "to_tsvector('english', " + column + ") @@ phraseto_tsquery('english', ?)";
                
            case "wfts":
                params.add(operatorValue);
                return "to_tsvector('english', " + column + ") @@ websearch_to_tsquery('english', ?)";
                
            default:
                // Unknown operator, treat as equality
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
        }
    }

    /**
     * Build IN condition with security validation
     */
    private String buildInCondition(String column, String operatorValue, List<Object> params, TableInfo tableInfo) {
        if (operatorValue.startsWith("(") && operatorValue.contains(")")) {
            int closingParen = operatorValue.indexOf(')');
            String inValues = operatorValue.substring(1, closingParen);
            
            validationService.validateInOperatorValues(inValues);
            
            String[] values = inValues.split(",");
            List<String> placeholders = new ArrayList<>();
            for (String val : values) {
                String trimmedVal = val.trim();
                if (!trimmedVal.isEmpty()) {
                    params.add(typeConversionService.convertValueToColumnType(column, trimmedVal, tableInfo));
                    placeholders.add(typeConversionService.buildPlaceholderWithCast(column, tableInfo));
                }
            }
            return column + " IN (" + String.join(",", placeholders) + ")";
        }
        return null;
    }

    /**
     * Build NOT IN condition with security validation
     */
    private String buildNotInCondition(String column, String operatorValue, List<Object> params, TableInfo tableInfo) {
        if (operatorValue.startsWith("(") && operatorValue.contains(")")) {
            int closingParen = operatorValue.indexOf(')');
            String inValues = operatorValue.substring(1, closingParen);
            
            validationService.validateInOperatorValues(inValues);
            
            String[] values = inValues.split(",");
            List<String> placeholders = new ArrayList<>();
            for (String val : values) {
                String trimmedVal = val.trim();
                if (!trimmedVal.isEmpty()) {
                    params.add(typeConversionService.convertValueToColumnType(column, trimmedVal, tableInfo));
                    placeholders.add(typeConversionService.buildPlaceholderWithCast(column, tableInfo));
                }
            }
            return column + " NOT IN (" + String.join(",", placeholders) + ")";
        }
        return null;
    }

    /**
     * Build IS condition (null, true, false)
     */
    private String buildIsCondition(String column, String operatorValue, List<Object> params, TableInfo tableInfo) {
        switch (operatorValue.toLowerCase()) {
            case "null":
                return column + " IS NULL";
            case "true":
                params.add(true);
                return column + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
            case "false":
                params.add(false);
                return column + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
            default:
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return column + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
        }
    }

    /**
     * Build JSON has keys condition
     */
    private String buildJsonHasKeysCondition(String column, String operatorValue, String operator) {
        if (operatorValue.startsWith("[") && operatorValue.endsWith("]")) {
            String keysStr = operatorValue.substring(1, operatorValue.length() - 1);
            String[] keys = keysStr.split(",");
            List<String> cleanKeys = new ArrayList<>();
            for (String key : keys) {
                cleanKeys.add(key.trim().replace("\"", ""));
            }
            String arrayLiteral = "ARRAY[" + cleanKeys.stream()
                .map(key -> "'" + key + "'")
                .collect(Collectors.joining(",")) + "]";
            return column + " " + operator + " " + arrayLiteral;
        } else {
            throw new IllegalArgumentException("JSON keys operator requires array format: [\"key1\",\"key2\"]");
        }
    }

    /**
     * Build JSON contains condition
     */
    private String buildJsonContainsCondition(String column, String operatorValue, List<Object> params, String operator) {
        try {
            // Validate JSON format
            objectMapper.readTree(operatorValue);
            params.add(operatorValue);
            return column + " " + operator + " ?::jsonb";
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON format for contains operator: " + operatorValue);
        }
    }

    /**
     * Build array has condition (overlaps)
     */
    private String buildArrayHasCondition(String column, String operatorValue, String operator, TableInfo tableInfo) {
        if ((operatorValue.startsWith("[") && operatorValue.endsWith("]")) || 
            (operatorValue.startsWith("{") && operatorValue.endsWith("}"))) {
            String valuesStr = operatorValue.substring(1, operatorValue.length() - 1);
            String[] values = valuesStr.split(",");
            List<String> cleanValues = new ArrayList<>();
            for (String val : values) {
                cleanValues.add(val.trim().replace("\"", ""));
            }
            String arrayLiteral = "ARRAY[" + cleanValues.stream()
                .map(val -> "'" + val + "'")
                .collect(Collectors.joining(",")) + "]::" + typeConversionService.getColumnType(column, tableInfo) + "[]";
            return column + " " + operator + " " + arrayLiteral;
        }
        return null;
    }

    /**
     * Build array has all condition
     */
    private String buildArrayHasAllCondition(String column, String operatorValue, List<Object> params) {
        if (operatorValue.startsWith("[") && operatorValue.endsWith("]")) {
            String valuesStr = operatorValue.substring(1, operatorValue.length() - 1);
            String[] values = valuesStr.split(",");
            List<String> cleanValues = new ArrayList<>();
            for (String val : values) {
                cleanValues.add(val.trim().replace("\"", ""));
            }
            params.add(cleanValues.toArray(new String[0]));
            return column + " @> ?";
        }
        return null;
    }
}
