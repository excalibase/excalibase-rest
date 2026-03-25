package io.github.excalibase.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a parsed field from select parameter.
 *
 * Supports extended select syntax:
 * - "name"               → simple field
 * - "alias:name"         → column aliasing (response key = alias)
 * - "age::text"          → type casting
 * - "alias:age::text"    → alias + cast
 * - "data->>key"         → JSON path access
 * - "actors(name,age)"   → embedded resource with selected fields
 * - "*"                  → all fields
 */
public class SelectField {
    private final String name;       // actual column name / JSON path expression
    private final String alias;      // response key alias (null if none)
    private final String cast;       // type cast (null if none)
    private final boolean jsonPath;  // true when name contains -> or ->>
    private final boolean isWildcard;
    private final boolean inner;     // true when !inner hint present (INNER JOIN semantics)
    private final List<SelectField> subFields;
    private final Map<String, String> filters; // For nested filtering like &actors.age=gt.30

    public SelectField(String name) {
        this.name = name;
        this.alias = null;
        this.cast = null;
        this.jsonPath = name.contains("->") || name.contains("->>");
        this.isWildcard = "*".equals(name);
        this.inner = false;
        this.subFields = new ArrayList<>();
        this.filters = new HashMap<>();
    }

    public SelectField(String name, List<SelectField> subFields) {
        this.name = name;
        this.alias = null;
        this.cast = null;
        this.jsonPath = name.contains("->") || name.contains("->>");
        this.isWildcard = false;
        this.inner = false;
        this.subFields = new ArrayList<>(subFields);
        this.filters = new HashMap<>();
    }

    /** Constructor for embedded fields with !inner join hint. */
    public SelectField(String name, List<SelectField> subFields, boolean inner) {
        this.name = name;
        this.alias = null;
        this.cast = null;
        this.jsonPath = name.contains("->") || name.contains("->>");
        this.isWildcard = false;
        this.inner = inner;
        this.subFields = new ArrayList<>(subFields);
        this.filters = new HashMap<>();
    }

    /** Full constructor used by the select parser for alias/cast/jsonPath fields. */
    public SelectField(String name, String alias, String cast) {
        this.name = name;
        this.alias = alias;
        this.cast = cast;
        this.jsonPath = name.contains("->") || name.contains("->>");
        this.isWildcard = "*".equals(name);
        this.inner = false;
        this.subFields = new ArrayList<>();
        this.filters = new HashMap<>();
    }
    
    public String getName() {
        return name;
    }

    /** Returns the alias for the response key, or null if no alias was specified. */
    public String getAlias() {
        return alias;
    }

    /** Returns the PostgreSQL type cast (e.g. "text"), or null if none. */
    public String getCast() {
        return cast;
    }

    /** Returns true when the field name is a JSON path expression (contains -> or ->>). */
    public boolean isJsonPath() {
        return jsonPath;
    }

    /** Returns true when !inner join hint is present (only return parent rows with matches). */
    public boolean isInner() {
        return inner;
    }

    public boolean isWildcard() {
        return isWildcard;
    }

    public boolean isEmbedded() {
        return !subFields.isEmpty();
    }

    /**
     * Builds the SQL expression for this field suitable for use in a SELECT clause.
     * Examples:
     * - "name"           → "name"
     * - alias "fn" on "name" → "name AS \"fn\""
     * - cast to "text"   → "name::text"
     * - alias + cast     → "name::text AS \"fn\""
     * - JSON path        → "data->>key"
     * - JSON path+alias  → "data->>key AS \"city\""
     */
    public String toSqlExpression() {
        StringBuilder sb = new StringBuilder(name);
        if (cast != null) {
            sb.append("::").append(cast);
        }
        if (alias != null) {
            sb.append(" AS \"").append(alias).append("\"");
        }
        return sb.toString();
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