package io.github.excalibase.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.constant.ColumnTypeConstant;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class TypeConversionService {

    private static final Logger log = LoggerFactory.getLogger(TypeConversionService.class);
    private final ObjectMapper objectMapper;
    private final IValidationService validationService;

    public TypeConversionService(IValidationService validationService, ObjectMapper objectMapper) {
        this.validationService = validationService;
        this.objectMapper = objectMapper;
    }

    /**
     * Convert object value to appropriate type based on column metadata from table schema
     */
    public Object convertValueToColumnType(String columnName, Object value, TableInfo tableInfo) {
        if (value == null) {
            return null;
        }
        
        // Find the column in the table schema
        ColumnInfo column = tableInfo.getColumns().stream()
            .filter(col -> col.getName().equals(columnName))
            .findFirst()
            .orElse(null);
            
        if (column == null) {
            // Column not found, use basic pattern matching with string value
            return convertValueToColumnType(columnName, value.toString());
        }
        
        String columnType = column.getType().toLowerCase();
        
        // Handle JSONB and JSON types specially - serialize objects to JSON strings
        if (columnType.equals(ColumnTypeConstant.JSONB) || columnType.equals(ColumnTypeConstant.JSON)) {
            if (value instanceof Map || value instanceof List) {
                // Serialize Map/List objects to JSON strings using ObjectMapper
                try {
                    return objectMapper.writeValueAsString(value);
                } catch (Exception e) {
                    log.warn("Failed to serialize object to JSON for column {}: {}", columnName, e.getMessage());
                    return value.toString(); // Fallback
                }
            } else if (value instanceof String) {
                // Already a string, validate JSON format if possible
                try {
                    objectMapper.readTree((String) value);
                    return value; // Valid JSON string
                } catch (Exception e) {
                    // Not valid JSON, but return as-is (PostgreSQL will handle validation)
                    return value;
                }
            } else {
                // Other types - convert to string
                return value.toString();
            }
        }
        
        // Handle PostgreSQL array types
        if (columnType.startsWith("_") || columnType.contains("[]")) {
            if (value instanceof List) {
                // Convert List to PostgreSQL array literal format (supports multidimensional)
                return convertToPostgreSQLArrayLiteral((List<?>) value);
            } else if (value instanceof String) {
                // Already a string, might be array format or JSON
                return value;
            } else {
                // Other types - try to convert to string
                return value.toString();
            }
        }
        
        // For non-JSON types, convert to string and use existing string-based method
        return convertValueToColumnType(columnName, value.toString(), tableInfo);
    }
    
    /**
     * Convert List to PostgreSQL array literal format recursively (supports multidimensional arrays)
     */
    private String convertToPostgreSQLArrayLiteral(List<?> list) {
        StringBuilder arrayLiteral = new StringBuilder("{");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
                arrayLiteral.append(",");
            }
            Object item = list.get(i);
            if (item == null) {
                arrayLiteral.append("NULL");
            } else if (item instanceof List) {
                // Nested array - recursively convert
                arrayLiteral.append(convertToPostgreSQLArrayLiteral((List<?>) item));
            } else {
                String itemStr = item.toString();
                // Escape quotes and wrap in quotes if it's a string
                if (item instanceof String) {
                    itemStr = "\"" + itemStr.replace("\"", "\\\"") + "\"";
                }
                arrayLiteral.append(itemStr);
            }
        }
        arrayLiteral.append("}");
        return arrayLiteral.toString();
    }
    
    /**
     * Convert string value to appropriate type based on column metadata from table schema
     */
    public Object convertValueToColumnType(String columnName, String value, TableInfo tableInfo) {
        if (value == null) {
            return null;
        }
        
        try {
            // Find the column in the table schema
            ColumnInfo column = tableInfo.getColumns().stream()
                .filter(col -> col.getName().equals(columnName))
                .findFirst()
                .orElse(null);
            
            if (column == null) {
                // Column not found, use basic pattern matching
                return convertValueToColumnType(columnName, value);
            }
            
            String columnType = column.getType().toLowerCase();
            
            // Handle PostgreSQL enhanced types
            if (columnType.startsWith(ColumnTypeConstant.POSTGRES_ENUM + ":")) {
                // PostgreSQL enum type - PostgreSQL handles validation
                return value;
            } else if (columnType.startsWith(ColumnTypeConstant.POSTGRES_COMPOSITE + ":")) {
                // PostgreSQL composite type - keep as string for now (could be enhanced to parse JSON)
                return value;
            } else if (columnType.equals(ColumnTypeConstant.INET) || columnType.equals(ColumnTypeConstant.CIDR)) {
                // Network types - just return the value
                return value;
            } else if (columnType.equals(ColumnTypeConstant.MACADDR) || columnType.equals(ColumnTypeConstant.MACADDR8)) {
                // MAC address types - just return the value
                return value;
            } else if (columnType.startsWith("bit")) {
                // BIT types - PostgreSQL expects B'binary_string' format, but with casting this is handled
                return value;
            } else if (columnType.equals(ColumnTypeConstant.JSONB) || columnType.equals(ColumnTypeConstant.JSON)) {
                // JSON/JSONB types - ensure proper JSON string format
                // The value should already be a JSON string, but verify and return as-is
                return value;
            } else if (columnType.contains("int") || columnType.equals("serial") || 
                columnType.equals("bigserial") || columnType.equals("smallserial")) {
                // Integer types
                return Integer.parseInt(value);
            } else if (columnType.contains("decimal") || columnType.contains("numeric") || 
                       columnType.contains("real") || columnType.contains("double") || 
                       columnType.equals("float")) {
                // Decimal types
                return Double.parseDouble(value);
            } else if (columnType.equals("boolean") || columnType.equals("bool")) {
                // Boolean types
                return Boolean.parseBoolean(value);
            } else if (columnType.contains("timestamp") || columnType.contains("date") || 
                       columnType.contains("time")) {
                // Date/time types - convert to proper Timestamp object for JDBC binding
                try {
                    if (columnType.contains("timestamp")) {
                        // Handle various timestamp formats
                        if (value.length() == 10) { // "2025-01-01" format
                            return java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(value + "T00:00:00"));
                        } else if (value.endsWith("Z")) { // ISO format with Z
                            return java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(value.substring(0, value.length() - 1)));
                        } else if (value.contains("T")) { // ISO format without Z
                            return java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(value));
                        } else {
                            return java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(value + "T00:00:00"));
                        }
                    } else if (columnType.contains("date")) {
                        // Handle date formats
                        return java.sql.Date.valueOf(java.time.LocalDate.parse(value));
                    } else {
                        // Handle time formats - keep as string for now
                        return value;
                    }
                } catch (Exception e) {
                    // If parsing fails, return as string (fallback)
                    return value;
                }
            } else {
                // For character/text types, always return as string without numeric conversion
                if (columnType.contains("char") || columnType.contains("text") || columnType.equals("varchar")) {
                    return value;
                }
                // Unknown type (possibly a domain type) — try numeric conversion
                // if the value looks like a number, to handle domains over int/numeric
                if (value.matches("-?\\d+")) {
                    try { return Integer.parseInt(value); } catch (NumberFormatException ignored) {}
                } else if (value.matches("-?\\d+\\.\\d+")) {
                    try { return Double.parseDouble(value); } catch (NumberFormatException ignored) {}
                }
                // Default to string for varchar, text, json, etc.
                return value;
            }
        } catch (NumberFormatException e) {
            // If conversion fails, return as string
            return value;
        }
    }
    
    /**
     * Convert string value to appropriate type based on basic pattern matching
     */
    public Object convertValueToColumnType(String columnName, String value) {
        try {
            // Try to convert to number if it looks like a number
            if (value.matches("-?\\d+")) {
                // Integer
                return Integer.parseInt(value);
            } else if (value.matches("-?\\d+\\.\\d+")) {
                // Decimal
                return Double.parseDouble(value);
            } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                // Boolean
                return Boolean.parseBoolean(value);
            } else {
                // String - keep as is
                return value;
            }
        } catch (NumberFormatException e) {
            // If conversion fails, return as string
            return value;
        }
    }

    /**
     * Build placeholder with proper type casting for PostgreSQL custom types
     */
    public String buildPlaceholderWithCast(String columnName, TableInfo tableInfo) {
        ColumnInfo column = tableInfo.getColumns().stream()
            .filter(col -> col.getName().equals(columnName))
            .findFirst()
            .orElse(null);
        
        if (column != null) {
            String columnType = column.getType().toLowerCase();
            
            // Array types (text[], integer[], etc.) - PostgreSQL catalog format
            if (columnType.endsWith("[]")) {
                log.debug("Building cast for array column '{}' with type '{}' -> ?::{}", columnName, columnType, columnType);
                return "?::" + columnType;
            }
            // Network types (INET, CIDR, MACADDR, MACADDR8)
            else if (columnType.equals("inet") || columnType.equals("cidr") || 
                columnType.equals("macaddr") || columnType.equals("macaddr8")) {
                return "?::" + columnType;
            }
            // BIT types
            else if (columnType.startsWith("bit")) {
                log.debug("Building cast for BIT column '{}' with type '{}' -> ?::{}", columnName, columnType, columnType);
                return "?::" + columnType;
            }
            // Enum types
            else if (columnType.startsWith("postgres_enum:")) {
                String enumType = columnType.substring("postgres_enum:".length());
                return "?::" + enumType;
            }
            // JSON types
            else if (columnType.equals("jsonb")) {
                return "?::jsonb";
            }
            else if (columnType.equals("json")) {
                return "?::json";
            }
            // UUID type
            else if (columnType.equals("uuid")) {
                return "?::uuid";
            }
            // Timestamp types - PostgreSQL can usually handle timestamp comparisons without explicit casting
            else if (columnType.equals("timestamptz") || columnType.equals("timestamp with time zone")) {
                return "?";
            }
            else if (columnType.equals("timestamp") || columnType.equals("timestamp without time zone")) {
                return "?";
            }
            else if (columnType.equals("date")) {
                return "?";
            }
            else if (columnType.equals("time") || columnType.equals("time without time zone")) {
                return "?";
            }
            else if (columnType.equals("timetz") || columnType.equals("time with time zone")) {
                return "?";
            }
            // Domain types and other custom types — cast explicitly so PostgreSQL
            // can resolve the domain from a plain literal (e.g. dvdrental.year from int4)
            else if (columnType.contains(".")) {
                return "?::" + columnType;
            }
        }

        return "?";
    }
    
    /**
     * Get base column type for array casting
     */
    public String getColumnType(String columnName, TableInfo tableInfo) {
        ColumnInfo column = tableInfo.getColumns().stream()
            .filter(col -> col.getName().equals(columnName))
            .findFirst()
            .orElse(null);
        
        if (column != null) {
            String columnType = column.getType().toLowerCase();
            // Remove [] suffix for array types
            if (columnType.endsWith("[]")) {
                return columnType.substring(0, columnType.length() - 2);
            }
            return columnType;
        }
        return "text"; // Default fallback
    }
}
