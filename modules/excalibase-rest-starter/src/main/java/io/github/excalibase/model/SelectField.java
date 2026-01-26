package io.github.excalibase.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a parsed field from select parameter
 * Examples:
 * - "name" -> simple field
 * - "actors(name,age)" -> embedded resource with selected fields
 * - "*" -> all fields
 */
public class SelectField {
    private final String name;
    private final boolean isWildcard;
    private final List<SelectField> subFields;
    private final Map<String, String> filters; // For nested filtering like &actors.age=gt.30
    
    public SelectField(String name) {
        this.name = name;
        this.isWildcard = "*".equals(name);
        this.subFields = new ArrayList<>();
        this.filters = new HashMap<>();
    }
    
    public SelectField(String name, List<SelectField> subFields) {
        this.name = name;
        this.isWildcard = false;
        this.subFields = new ArrayList<>(subFields);
        this.filters = new HashMap<>();
    }
    
    public String getName() {
        return name;
    }
    
    public boolean isWildcard() {
        return isWildcard;
    }
    
    public boolean isEmbedded() {
        return !subFields.isEmpty();
    }
    
    public List<SelectField> getSubFields() {
        return subFields;
    }
    
    public Map<String, String> getFilters() {
        return filters;
    }
    
    public void addFilter(String key, String value) {
        filters.put(key, value);
    }
    
    public void addSubField(SelectField field) {
        subFields.add(field);
    }
    
    @Override
    public String toString() {
        if (!isEmbedded()) {
            return name;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(name).append("(");
        for (int i = 0; i < subFields.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(subFields.get(i).toString());
        }
        sb.append(")");
        return sb.toString();
    }
    
    /**
     * Check if this field represents a simple column (not embedded)
     */
    public boolean isSimpleColumn() {
        return !isEmbedded() && !isWildcard;
    }
    
    /**
     * Get all column names for a simple select (no embedding)
     */
    public List<String> getSimpleColumnNames() {
        List<String> columns = new ArrayList<>();
        if (isWildcard) {
            columns.add("*");
        } else if (isSimpleColumn()) {
            columns.add(name);
        } else if (isEmbedded()) {
            // For embedded fields, we need to include the foreign key
            columns.add(name + "_id"); // Assuming standard naming convention
        }
        return columns;
    }
}