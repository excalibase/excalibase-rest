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
import static io.github.excalibase.util.SqlIdentifier.quoteIdentifier;

@Service
public class FilterService {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(FilterService.class);
    private final IValidationService validationService;
    private final TypeConversionService typeConversionService;
    private final ObjectMapper objectMapper;

    public FilterService(IValidationService validationService, TypeConversionService typeConversionService, ObjectMapper objectMapper) {
        this.validationService = validationService;
        this.typeConversionService = typeConversionService;
        this.objectMapper = objectMapper;
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
     * Also supports nested logic: or=(age.eq.18,not.and(age.gte.30,age.lte.40))
     */
    private String parseOrCondition(String orValue, List<Object> params, Set<String> validColumns, TableInfo tableInfo) {
        // Remove outer parentheses if present
        if (orValue.startsWith("(") && orValue.endsWith(")")) {
            orValue = orValue.substring(1, orValue.length() - 1);
        }

        List<String> tokens = splitLogicTokens(orValue);
        List<String> parsedConditions = new ArrayList<>();

        for (String token : tokens) {
            token = token.trim();
            if (token.isEmpty()) continue;

            String parsedCondition = parseLogicToken(token, params, validColumns, tableInfo);
            if (parsedCondition != null) {
                parsedConditions.add(parsedCondition);
            }
        }

        return parsedConditions.isEmpty() ? null : String.join(" OR ", parsedConditions);
    }

    /**
     * Parse a single logic token inside an or/and group.
     * Handles:
     * - col.op.value              → regular condition
     * - not.and(...)              → NOT (AND group)
     * - not.or(...)               → NOT (OR group)
     * - and(...)                  → AND group
     * - or(...)                   → nested OR group
     */
    private String parseLogicToken(String token, List<Object> params, Set<String> validColumns, TableInfo tableInfo) {
        // not.and(...) or not.or(...)
        if (token.startsWith("not.and(") && token.endsWith(")")) {
            String inner = token.substring("not.and(".length(), token.length() - 1);
            String andCondition = parseAndCondition(inner, params, validColumns, tableInfo);
            return andCondition == null ? null : "NOT (" + andCondition + ")";
        }
        if (token.startsWith("not.or(") && token.endsWith(")")) {
            String inner = token.substring("not.or(".length(), token.length() - 1);
            String orCondition = parseOrCondition(inner, params, validColumns, tableInfo);
            return orCondition == null ? null : "NOT (" + orCondition + ")";
        }
        // and(...) group
        if (token.startsWith("and(") && token.endsWith(")")) {
            String inner = token.substring("and(".length(), token.length() - 1);
            return parseAndCondition(inner, params, validColumns, tableInfo);
        }
        // or(...) nested group
        if (token.startsWith("or(") && token.endsWith(")")) {
            String inner = token.substring("or(".length(), token.length() - 1);
            String orCond = parseOrCondition(inner, params, validColumns, tableInfo);
            return orCond == null ? null : "(" + orCond + ")";
        }

        // Regular: col.op.value
        int firstDot = token.indexOf('.');
        if (firstDot > 0) {
            String column = token.substring(0, firstDot);
            String operatorValue = token.substring(firstDot + 1);
            return parseCondition(column, operatorValue, params, validColumns, tableInfo);
        }
        return null;
    }

    /**
     * Parse AND condition group: col1.op.val1,col2.op.val2
     */
    private String parseAndCondition(String andValue, List<Object> params, Set<String> validColumns, TableInfo tableInfo) {
        List<String> tokens = splitLogicTokens(andValue);
        List<String> parsedConditions = new ArrayList<>();

        for (String token : tokens) {
            token = token.trim();
            if (token.isEmpty()) continue;
            String parsedCondition = parseLogicToken(token, params, validColumns, tableInfo);
            if (parsedCondition != null) {
                parsedConditions.add(parsedCondition);
            }
        }

        return parsedConditions.isEmpty() ? null : String.join(" AND ", parsedConditions);
    }

    /**
     * Split comma-separated tokens while respecting nested parentheses.
     */
    private List<String> splitLogicTokens(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;

        for (char c : input.toCharArray()) {
            if (c == '(') {
                depth++;
                current.append(c);
            } else if (c == ')') {
                depth--;
                current.append(c);
            } else if (c == ',' && depth == 0) {
                tokens.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
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
            return quoteIdentifier(column) + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
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
        String qc = quoteIdentifier(column); // quoted column for SQL
        switch (operator.toLowerCase()) {
            // ── Excalibase `not` prefix ──────────────────────────────────────────────
            case "not": {
                int dot = operatorValue.indexOf('.');
                if (dot > 0) {
                    String innerOp = operatorValue.substring(0, dot);
                    String innerVal = operatorValue.substring(dot + 1);
                    if ("is".equalsIgnoreCase(innerOp) && "null".equalsIgnoreCase(innerVal)) {
                        return qc + " IS NOT NULL";
                    }
                    if ("in".equalsIgnoreCase(innerOp)) {
                        return buildNotInCondition(column, innerVal, params, tableInfo);
                    }
                    String innerCond = buildConditionForOperator(column, innerOp, innerVal, params, tableInfo);
                    return "NOT (" + innerCond + ")";
                }
                return qc + " IS NOT NULL";
            }

            case "eq":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return qc + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
            case "neq":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return qc + " <> " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
            case "gt":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return qc + " > " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
            case "gte":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return qc + " >= " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
            case "lt":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return qc + " < " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
            case "lte":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return qc + " <= " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);

            case "like":
                params.add("%" + operatorValue + "%");
                return qc + " LIKE ?";
            case "ilike":
                params.add("%" + operatorValue + "%");
                return qc + " ILIKE ?";

            case "in":
                return buildInCondition(column, operatorValue, params, tableInfo);
            case "notin":
                return buildNotInCondition(column, operatorValue, params, tableInfo);

            case "is":
                return buildIsCondition(column, operatorValue, params, tableInfo);
            case "isnotnull":
                return qc + " IS NOT NULL";

            case "isdistinct":
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return qc + " IS DISTINCT FROM " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);

            case "match":
                params.add(operatorValue);
                return qc + " ~ ?";
            case "imatch":
                params.add(operatorValue);
                return qc + " ~* ?";
            case "startswith":
                params.add(operatorValue + "%");
                return qc + " LIKE ?";
            case "endswith":
                params.add("%" + operatorValue);
                return qc + " LIKE ?";

            // JSON operators
            case "haskey":
                params.add(operatorValue);
                return "jsonb_exists(" + qc + ", ?)";
            case "haskeys":
                return buildJsonHasKeysCondition(column, operatorValue, "?&", params);
            case "hasanykeys":
                return buildJsonHasKeysCondition(column, operatorValue, "?|", params);
            case "jsoncontains":
            case "contains":
                return buildJsonContainsCondition(column, operatorValue, params, "@>");
            case "jsoncontained":
            case "containedin":
                return buildJsonContainsCondition(column, operatorValue, params, "<@");
            case "jsonexists":
            case "exists":
                params.add(operatorValue);
                return qc + " ? ?";
            case "jsonexistsany":
            case "existsany":
                return buildJsonHasKeysCondition(column, operatorValue, "?|", params);
            case "jsonexistsall":
            case "existsall":
                return buildJsonHasKeysCondition(column, operatorValue, "?&", params);
            case "jsonpath":
                params.add(operatorValue);
                return qc + " @? ?::jsonpath";
            case "jsonpathexists":
                params.add(operatorValue);
                return qc + " @@ ?::jsonpath";

            // Array operators
            case "arraycontains":
                params.add(operatorValue);
                return qc + " @> ARRAY[?]::" + typeConversionService.getColumnType(column, tableInfo) + "[]";
            case "arrayhasany":
                return buildArrayHasCondition(column, operatorValue, "&&", tableInfo, params);
            case "arrayhasall":
                return buildArrayHasAllCondition(column, operatorValue, params);
            case "arraylength":
                params.add(Integer.parseInt(operatorValue));
                return "array_length(" + qc + ", 1) = ?";

            // Full-text search operators
            case "fts":
                params.add(operatorValue);
                return "to_tsvector('english', " + qc + ") @@ to_tsquery('english', ?)";
            case "plfts":
                params.add(operatorValue);
                return "to_tsvector('english', " + qc + ") @@ plainto_tsquery('english', ?)";
            case "phfts":
                params.add(operatorValue);
                return "to_tsvector('english', " + qc + ") @@ phraseto_tsquery('english', ?)";
            case "wfts":
                params.add(operatorValue);
                return "to_tsvector('english', " + qc + ") @@ websearch_to_tsquery('english', ?)";

            // Range / geometric operators
            case "cs":
                params.add(operatorValue);
                return qc + " @> ?";
            case "cd":
                params.add(operatorValue);
                return qc + " <@ ?";
            case "ov":
                params.add(operatorValue);
                return qc + " && ?";
            case "sl":
                params.add(operatorValue);
                return qc + " << ?";
            case "sr":
                params.add(operatorValue);
                return qc + " >> ?";
            case "nxl":
                params.add(operatorValue);
                return qc + " &< ?";
            case "nxr":
                params.add(operatorValue);
                return qc + " &> ?";
            case "adj":
                params.add(operatorValue);
                return qc + " -|- ?";

            default:
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return qc + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
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
            return quoteIdentifier(column) + " IN (" + String.join(",", placeholders) + ")";
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
            return quoteIdentifier(column) + " NOT IN (" + String.join(",", placeholders) + ")";
        }
        return null;
    }

    /**
     * Build IS condition — Excalibase-compatible: null, true, false, unknown.
     */
    private String buildIsCondition(String column, String operatorValue, List<Object> params, TableInfo tableInfo) {
        String qc = quoteIdentifier(column);
        switch (operatorValue.toLowerCase()) {
            case "null":
                return qc + " IS NULL";
            case "true":
                return qc + " IS TRUE";
            case "false":
                return qc + " IS FALSE";
            case "unknown":
                return qc + " IS UNKNOWN";
            default:
                params.add(typeConversionService.convertValueToColumnType(column, operatorValue, tableInfo));
                return qc + " = " + typeConversionService.buildPlaceholderWithCast(column, tableInfo);
        }
    }

    /**
     * Build JSON has keys condition
     */
    private String buildJsonHasKeysCondition(String column, String operatorValue, String operator, List<Object> params) {
        if (operatorValue.startsWith("[") && operatorValue.endsWith("]")) {
            String keysStr = operatorValue.substring(1, operatorValue.length() - 1);
            String[] keys = keysStr.split(",");
            String[] cleanKeys = java.util.Arrays.stream(keys)
                    .map(k -> k.trim().replace("\"", ""))
                    .toArray(String[]::new);
            params.add(cleanKeys);
            return quoteIdentifier(column) + " " + operator + " ?::text[]";
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
            return quoteIdentifier(column) + " " + operator + " ?::jsonb";
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON format for contains operator: " + operatorValue);
        }
    }

    /**
     * Build array has condition (overlaps)
     */
    private String buildArrayHasCondition(String column, String operatorValue, String operator, TableInfo tableInfo, List<Object> params) {
        if ((operatorValue.startsWith("[") && operatorValue.endsWith("]")) ||
            (operatorValue.startsWith("{") && operatorValue.endsWith("}"))) {
            String valuesStr = operatorValue.substring(1, operatorValue.length() - 1);
            String[] values = valuesStr.split(",");
            String[] cleanValues = java.util.Arrays.stream(values)
                    .map(v -> v.trim().replace("\"", ""))
                    .toArray(String[]::new);
            params.add(cleanValues);
            String colType = typeConversionService.getColumnType(column, tableInfo);
            return quoteIdentifier(column) + " " + operator + " ?::" + colType + "[]";
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
            return quoteIdentifier(column) + " @> ?";
        }
        return null;
    }
}
