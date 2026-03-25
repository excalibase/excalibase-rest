package io.github.excalibase.postgres.service;

import io.github.excalibase.model.SelectField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Service to parse select parameters
 * Examples:
 * - "name,age" -> [SelectField("name"), SelectField("age")]
 * - "name,actors(first_name,last_name)" -> [SelectField("name"), SelectField("actors", [SelectField("first_name"), SelectField("last_name")])]
 * - "*,actors(*)" -> [SelectField("*"), SelectField("actors", [SelectField("*")])]
 */
@Service
public class SelectParserService {

    private static final Logger log = LoggerFactory.getLogger(SelectParserService.class);
    
    // Pattern to match embedded fields: field(subfield1,subfield2)
    // This pattern handles nested parentheses by matching balanced pairs
    private static final Pattern EMBEDDED_PATTERN = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)\\((.*)\\)");
    
    /**
     * Parse select parameter into SelectField objects
     *
     * @param selectParam The select parameter string (e.g., "name,actors(first_name,age)")
     * @return List of parsed SelectField objects
     */
    public List<SelectField> parseSelect(String selectParam) {
        List<SelectField> fields = new ArrayList<>();
        
        if (selectParam == null || selectParam.trim().isEmpty()) {
            // Default to wildcard if no select specified
            fields.add(new SelectField("*"));
            return fields;
        }
        
        // Split by commas, but be careful about commas inside parentheses
        List<String> tokens = splitSelectTokens(selectParam);
        
        for (String token : tokens) {
            token = token.trim();
            if (token.isEmpty()) continue;
            
            SelectField field = parseSelectToken(token);
            if (field != null) {
                fields.add(field);
            }
        }
        
        return fields.isEmpty() ? List.of(new SelectField("*")) : fields;
    }
    
    /**
     * Parse embedded filters from query parameters
     * Example: &amp;actors.age=gt.30 -&gt; adds filter to actors field
     */
    public void parseEmbeddedFilters(List<SelectField> fields, MultiValueMap<String, String> params) {
        if (params == null) return;
        
        for (String key : params.keySet()) {
            if (key.contains(".")) {
                // This is potentially an embedded filter like "actors.age"
                String[] parts = key.split("\\.", 2);
                if (parts.length == 2) {
                    String fieldName = parts[0];
                    String subFilter = parts[1];
                    
                    // Find the matching field and add the filter
                    for (SelectField field : fields) {
                        if (field.getName().equals(fieldName)) {
                            List<String> values = params.get(key);
                            if (!values.isEmpty()) {
                                field.addFilter(subFilter, values.get(0));
                            }
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Split select tokens respecting parentheses
     */
    private List<String> splitSelectTokens(String selectParam) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        
        for (char c : selectParam.toCharArray()) {
            if (c == '(') {
                depth++;
                current.append(c);
            } else if (c == ')') {
                depth--;
                current.append(c);
            } else if (c == ',' && depth == 0) {
                // Top-level comma, split here
                tokens.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        
        // Add the last token
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        
        return tokens;
    }
    
    /**
     * Parse a single select token (could be simple field, embedded field, alias, cast, or JSON path).
     *
     * Supported syntax:
     * - "name"                → simple field
     * - "alias:name"          → alias (response key = alias, SQL column = name)
     * - "age::text"           → type cast
     * - "alias:age::text"     → alias + cast
     * - "data->>key"          → JSON path (used as-is in SQL)
     * - "city:address->>city" → alias + JSON path
     * - "actors(name,age)"    → embedded resource (no alias/cast on these)
     */
    private SelectField parseSelectToken(String token) {
        token = token.trim();

        // Check if token contains parentheses (embedded field) — handle before alias/cast
        int openParen = token.indexOf('(');
        if (openParen > 0 && token.endsWith(")")) {
            String rawFieldName = token.substring(0, openParen);
            // Strip !inner hint if present
            boolean inner = rawFieldName.endsWith("!inner");
            String fieldName = inner ? rawFieldName.substring(0, rawFieldName.length() - 6) : rawFieldName;
            // Embedded resource names must be valid identifiers (no dots, no special chars)
            if (!fieldName.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                throw new IllegalArgumentException("Invalid select field: " + token);
            }
            String subFieldsStr = token.substring(openParen + 1, token.length() - 1);
            List<SelectField> subFields = parseSelect(subFieldsStr);
            return new SelectField(fieldName, subFields, inner);
        }

        // Extract alias prefix: "alias:rest"
        // Alias uses a SINGLE colon; type casts use "::" — must not confuse them.
        // Strategy: find the first ":" that is NOT part of "::"
        String alias = null;
        String rest = token;
        int colonIdx = indexOfSingleColon(token);
        int firstArrow = token.indexOf("->"); // JSON path markers
        if (colonIdx > 0 && (firstArrow < 0 || colonIdx < firstArrow)) {
            alias = token.substring(0, colonIdx);
            rest = token.substring(colonIdx + 1);
        }

        // Extract type cast suffix: "col::type" (only when no JSON path follows)
        String cast = null;
        if (!rest.contains("->") && rest.contains("::")) {
            int castIdx = rest.lastIndexOf("::");
            cast = rest.substring(castIdx + 2);
            rest = rest.substring(0, castIdx);
        }

        return new SelectField(rest, alias, cast);
    }
    
    /**
     * Find the index of the first single colon ":" that is NOT part of "::" (type cast).
     * Returns -1 if no such colon exists.
     */
    private int indexOfSingleColon(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ':') {
                // Check that it is not followed by another ':' (double colon = cast)
                boolean nextIsColon = i + 1 < s.length() && s.charAt(i + 1) == ':';
                // And not preceded by another ':' (would be the second colon of '::')
                boolean prevIsColon = i > 0 && s.charAt(i - 1) == ':';
                if (!nextIsColon && !prevIsColon) {
                    return i;
                }
            }
        }
        return -1;
    }

    /**
     * Check if select contains any embedded fields
     */
    public boolean hasEmbeddedFields(List<SelectField> fields) {
        return fields.stream().anyMatch(SelectField::isEmbedded);
    }
    
    /**
     * Get all simple (non-embedded) column names from select fields
     */
    public List<String> getSimpleColumnNames(List<SelectField> fields) {
        List<String> columns = new ArrayList<>();
        
        for (SelectField field : fields) {
            if (field.isSimpleColumn() || field.isWildcard()) {
                columns.addAll(field.getSimpleColumnNames());
            }
        }
        
        return columns;
    }
    
    /**
     * Get embedded fields for relationship expansion
     */
    public List<SelectField> getEmbeddedFields(List<SelectField> fields) {
        return fields.stream()
                .filter(SelectField::isEmbedded)
                .toList();
    }
}