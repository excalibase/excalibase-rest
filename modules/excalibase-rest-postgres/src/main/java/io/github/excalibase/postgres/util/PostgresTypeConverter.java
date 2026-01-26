package io.github.excalibase.postgres.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Comprehensive PostgreSQL type converter for REST API to achieve parity with GraphQL.
 * Handles all advanced PostgreSQL types: Arrays, JSON/JSONB, UUID, BIT, Date/Time, BYTEA, XML, etc.
 */
public class PostgresTypeConverter {
    private static final Logger log = LoggerFactory.getLogger(PostgresTypeConverter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    /**
     * Convert all PostgreSQL types in query results to JSON-serializable formats.
     * This provides full parity with GraphQL type handling.
     */
    public static List<Map<String, Object>> convertPostgresTypes(List<Map<String, Object>> results, TableInfo tableInfo) {
        if (results == null || results.isEmpty()) {
            return results;
        }
        
        return results.stream()
                .map(record -> convertPostgresTypesInRecord(record, tableInfo))
                .collect(Collectors.toList());
    }
    
    /**
     * Convert PostgreSQL types in a single record.
     */
    public static Map<String, Object> convertPostgresTypesInRecord(Map<String, Object> record, TableInfo tableInfo) {
        if (record == null || record.isEmpty()) {
            return record;
        }
        
        Map<String, Object> convertedRecord = new HashMap<>(record);
        
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            String columnName = entry.getKey();
            Object value = entry.getValue();
            
            if (value == null) {
                continue;
            }
            
            // Find column info for type-specific conversion
            ColumnInfo columnInfo = tableInfo.getColumns().stream()
                    .filter(col -> col.getName().equals(columnName))
                    .findFirst()
                    .orElse(null);
            
            String columnType = columnInfo != null ? columnInfo.getType().toLowerCase() : "";
            
            try {
                Object convertedValue = convertSingleValue(value, columnType);
                if (convertedValue != value) { // Only update if conversion occurred
                    convertedRecord.put(columnName, convertedValue);
                    log.debug("Converted PostgreSQL type for column '{}' (type: {})", columnName, columnType);
                }
            } catch (Exception e) {
                log.error("Failed to convert PostgreSQL type for column '{}' (type: {}): {}", 
                         columnName, columnType, e.getMessage());
                // Keep original value on conversion failure
            }
        }
        
        return convertedRecord;
    }
    
    /**
     * Convert a single value based on its PostgreSQL type.
     */
    private static Object convertSingleValue(Object value, String columnType) throws SQLException, JsonProcessingException {
        // 1. PostgreSQL Arrays - Convert to Lists (same as GraphQL)
        if (value instanceof Array) {
            return convertPostgresArray((Array) value);
        }
        
        // 2. JSON/JSONB - Parse to objects (like GraphQL)
        if (isJsonType(columnType)) {
            if (value instanceof String) {
                return convertJsonValue((String) value);
            } else if (isPGobject(value)) {
                // JSONB often comes back as PGobject from PostgreSQL
                return convertJsonValue(value.toString());
            }
        }
        
        // 3. BIT Types - Handle PGobject (like GraphQL)
        if (isBitType(columnType) && isPGobject(value)) {
            return value.toString();
        }
        
        // 4. UUID - Special handling for validation and operations
        if (isUuidType(columnType)) {
            return convertUuidValue(value);
        }
        
        // 5. Date/Time Types - Enhanced parsing (like GraphQL)
        if (isDateTimeType(columnType)) {
            return convertDateTimeValue(value, columnType);
        }
        
        // 6. BYTEA - Binary data handling
        if (isByteaType(columnType)) {
            return convertByteaValue(value);
        }
        
        // 7. XML - XML type support
        if (isXmlType(columnType)) {
            return convertXmlValue(value);
        }
        
        // 8. Network Types (INET/CIDR/MACADDR) - Enhanced validation
        if (isNetworkType(columnType)) {
            return convertNetworkValue(value, columnType);
        }
        
        // 9. Custom Enums and Composite Types
        if (isCustomType(columnType)) {
            return convertCustomTypeValue(value, columnType);
        }
        
        // Return original value if no conversion needed
        return value;
    }
    
    // === PostgreSQL Array Conversion ===
    private static List<Object> convertPostgresArray(Array sqlArray) throws SQLException {
        try {
            Object[] elements = (Object[]) sqlArray.getArray();
            if (elements == null) {
                return Collections.emptyList();
            }
            return Arrays.asList(elements);
        } catch (SQLException e) {
            // Handle closed connection case - parse string representation
            log.debug("Array access failed (connection closed), parsing string representation: {}", e.getMessage());
            String arrayStr = sqlArray.toString();
            return parsePostgresArrayString(arrayStr);
        }
    }
    
    /**
     * Parse PostgreSQL array string representation like "{apple,banana,cherry}".
     */
    private static List<Object> parsePostgresArrayString(String arrayStr) {
        if (arrayStr == null || arrayStr.trim().isEmpty()) {
            return Collections.emptyList();
        }
        
        String trimmed = arrayStr.trim();
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        
        if (trimmed.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Handle quoted string arrays vs unquoted arrays
        if (trimmed.contains("\"")) {
            return parseQuotedArrayElements(trimmed);
        } else {
            return Arrays.stream(trimmed.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
        }
    }
    
    /**
     * Parse quoted array elements like "apple","banana","cherry".
     */
    private static List<Object> parseQuotedArrayElements(String arrayContent) {
        List<Object> elements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean escaped = false;
        
        for (char c : arrayContent.toCharArray()) {
            if (escaped) {
                current.append(c);
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                if (current.length() > 0) {
                    elements.add(current.toString().trim());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }
        
        if (current.length() > 0) {
            elements.add(current.toString().trim());
        }
        
        return elements;
    }
    
    // === JSON/JSONB Conversion ===
    private static Object convertJsonValue(String jsonStr) throws JsonProcessingException {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonStr);
            
            if (jsonNode.isArray()) {
                // Convert JSON array to List
                return objectMapper.convertValue(jsonNode, List.class);
            } else if (jsonNode.isObject()) {
                // Convert JSON object to Map
                return objectMapper.convertValue(jsonNode, Map.class);
            } else {
                // Primitive JSON values (string, number, boolean, null)
                return objectMapper.convertValue(jsonNode, Object.class);
            }
        } catch (JsonProcessingException e) {
            log.debug("JSON parsing failed, returning as string: {}", e.getMessage());
            return jsonStr; // Return as string if JSON parsing fails
        }
    }
    
    // === UUID Conversion ===
    private static Map<String, Object> convertUuidValue(Object value) {
        String uuidStr = value.toString();
        
        // Validate UUID format
        try {
            UUID.fromString(uuidStr);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid UUID format: {}", uuidStr);
        }
        
        // Return enhanced UUID object with additional metadata (like GraphQL)
        Map<String, Object> uuidObj = new HashMap<>();
        uuidObj.put("value", uuidStr);
        uuidObj.put("type", "UUID");
        uuidObj.put("version", getUuidVersion(uuidStr));
        
        return uuidObj;
    }
    
    private static int getUuidVersion(String uuidStr) {
        try {
            UUID uuid = UUID.fromString(uuidStr);
            return uuid.version();
        } catch (Exception e) {
            return -1; // Unknown version
        }
    }
    
    // === Date/Time Conversion ===
    private static Object convertDateTimeValue(Object value, String columnType) {
        String dateStr = value.toString();
        
        try {
            if (columnType.contains("timestamp")) {
                LocalDateTime dateTime = LocalDateTime.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                Map<String, Object> timestampObj = new HashMap<>();
                timestampObj.put("value", dateStr);
                timestampObj.put("parsed", dateTime);
                timestampObj.put("type", "TIMESTAMP");
                return timestampObj;
            } else if (columnType.contains("date")) {
                LocalDate date = LocalDate.parse(dateStr);
                Map<String, Object> dateObj = new HashMap<>();
                dateObj.put("value", dateStr);
                dateObj.put("parsed", date);
                dateObj.put("type", "DATE");
                return dateObj;
            } else if (columnType.contains("time")) {
                Map<String, Object> timeObj = new HashMap<>();
                timeObj.put("value", dateStr);
                timeObj.put("type", "TIME");
                return timeObj;
            } else if (columnType.equals("interval")) {
                Map<String, Object> intervalObj = new HashMap<>();
                intervalObj.put("value", dateStr);
                intervalObj.put("type", "INTERVAL");
                return intervalObj;
            }
        } catch (DateTimeParseException e) {
            log.debug("Date/time parsing failed for '{}', returning as string: {}", dateStr, e.getMessage());
        }
        
        return value; // Return original if parsing fails
    }
    
    // === BYTEA Conversion ===
    private static Map<String, Object> convertByteaValue(Object value) {
        byte[] bytes;
        
        if (value instanceof byte[]) {
            bytes = (byte[]) value;
        } else {
            // Handle hex string format from PostgreSQL
            String hexStr = value.toString();
            if (hexStr.startsWith("\\\\x")) {
                hexStr = hexStr.substring(2); // Remove \\x prefix
            }
            bytes = hexStringToByteArray(hexStr);
        }
        
        Map<String, Object> byteaObj = new HashMap<>();
        byteaObj.put("value", Base64.getEncoder().encodeToString(bytes));
        byteaObj.put("length", bytes.length);
        byteaObj.put("type", "BYTEA");
        
        return byteaObj;
    }
    
    private static byte[] hexStringToByteArray(String hex) {
        int length = hex.length();
        byte[] data = new byte[length / 2];
        for (int i = 0; i < length; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i+1), 16));
        }
        return data;
    }
    
    // === XML Conversion ===
    private static Map<String, Object> convertXmlValue(Object value) {
        String xmlStr = value.toString();
        
        Map<String, Object> xmlObj = new HashMap<>();
        xmlObj.put("value", xmlStr);
        xmlObj.put("type", "XML");
        xmlObj.put("length", xmlStr.length());
        
        return xmlObj;
    }
    
    // === Network Type Conversion ===
    private static Object convertNetworkValue(Object value, String columnType) {
        // Return simple string value for all network types
        // This keeps it simple and functional as per user preference
        return value.toString();
    }
    
    // === Custom Type Conversion ===
    private static Object convertCustomTypeValue(Object value, String columnType) {
        String valueStr = value.toString();
        
        // Handle composite types (tuple format)
        if (valueStr.startsWith("(") && valueStr.endsWith(")")) {
            return parseCompositeType(valueStr, columnType);
        }
        
        // Handle enum types - return as simple string
        if (columnType.startsWith("postgres_enum:")) {
            return valueStr;
        }
        
        // Handle other custom types as objects
        Map<String, Object> customObj = new HashMap<>();
        customObj.put("value", valueStr);
        customObj.put("type", columnType);
        customObj.put("category", "CUSTOM");
        
        return customObj;
    }
    
    private static Map<String, Object> parseCompositeType(String compositeStr, String typeName) {
        Map<String, Object> composite = new HashMap<>();
        composite.put("type", typeName);
        composite.put("category", "COMPOSITE");
        composite.put("raw", compositeStr);
        
        // Basic parsing of (field1,field2,field3) format
        String content = compositeStr.substring(1, compositeStr.length() - 1);
        String[] fields = content.split(",");
        
        List<Object> parsedFields = new ArrayList<>();
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i].trim();
            // Remove quotes if present
            if (field.startsWith("\"") && field.endsWith("\"")) {
                field = field.substring(1, field.length() - 1);
            }
            parsedFields.add(field);
        }
        
        composite.put("fields", parsedFields);
        return composite;
    }
    
    // === Type Detection Helpers ===
    private static boolean isJsonType(String columnType) {
        return columnType.equals("json") || columnType.equals("jsonb");
    }
    
    private static boolean isBitType(String columnType) {
        return columnType.equals("bit") || columnType.equals("varbit") || columnType.equals("bit varying");
    }
    
    private static boolean isUuidType(String columnType) {
        return columnType.equals("uuid");
    }
    
    private static boolean isDateTimeType(String columnType) {
        return columnType.contains("timestamp") || columnType.contains("date") || 
               columnType.contains("time") || columnType.equals("interval");
    }
    
    private static boolean isByteaType(String columnType) {
        return columnType.equals("bytea");
    }
    
    private static boolean isXmlType(String columnType) {
        return columnType.equals("xml");
    }
    
    private static boolean isNetworkType(String columnType) {
        return columnType.equals("inet") || columnType.equals("cidr") || 
               columnType.equals("macaddr") || columnType.equals("macaddr8");
    }
    
    private static boolean isCustomType(String columnType) {
        return columnType.startsWith("postgres_enum:") || columnType.startsWith("postgres_composite:");
    }
    
    private static boolean isPGobject(Object value) {
        return value.getClass().getName().equals("org.postgresql.util.PGobject");
    }
}