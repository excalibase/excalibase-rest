package io.github.excalibase.postgres.service;

import io.github.excalibase.constant.ColumnTypeConstant;
import io.github.excalibase.constant.DatabaseType;
import io.github.excalibase.model.ColumnInfo;
import io.github.excalibase.model.CompositeTypeAttribute;
import io.github.excalibase.model.CustomCompositeTypeInfo;
import io.github.excalibase.model.CustomEnumTypeInfo;
import io.github.excalibase.model.ForeignKeyInfo;
import io.github.excalibase.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DatabaseSchemaService {

    private static final Logger log = LoggerFactory.getLogger(DatabaseSchemaService.class);
    
    private final JdbcTemplate jdbcTemplate;
    private final String allowedSchema;
    private final DatabaseType databaseType;
    
    // Cache for schema information
    private final Map<String, Map<String, TableInfo>> schemaCache = new ConcurrentHashMap<>();
    private long lastCacheUpdate = 0;
    private static final long CACHE_TTL_MS = 300_000; // 5 minutes
    
    public DatabaseSchemaService(JdbcTemplate jdbcTemplate,
                                 @Value("${app.allowed-schema:public}") String allowedSchema,
                                 @Value("${app.database-type:postgres}") String databaseTypeStr) {
        this.jdbcTemplate = jdbcTemplate;
        this.allowedSchema = allowedSchema;
        this.databaseType = DatabaseType.valueOf(databaseTypeStr.toUpperCase());
    }

    /**
     * Get all table schemas with caching
     */
    public Map<String, TableInfo> getTableSchema() {
        long currentTime = System.currentTimeMillis();
        
        // Check cache first
        if (schemaCache.containsKey(allowedSchema) && 
            (currentTime - lastCacheUpdate) < CACHE_TTL_MS) {
            return schemaCache.get(allowedSchema);
        }
        
        // Refresh cache
        Map<String, TableInfo> schema = reflectSchema();
        schemaCache.put(allowedSchema, schema);
        lastCacheUpdate = currentTime;
        
        return schema;
    }

    /**
     * Reflect database schema to get table and column information
     */
    private Map<String, TableInfo> reflectSchema() {
        log.debug("Reflecting schema for database type: {} and schema: {}", databaseType, allowedSchema);
        
        Map<String, TableInfo> tables = new HashMap<>();
        
        try {
            // Get all tables in the schema
            List<String> tableNames = getTableNames();
            
            for (String tableName : tableNames) {
                // Get columns for this table
                List<ColumnInfo> columns = getTableColumns(tableName);
                
                // Get foreign keys for this table
                List<ForeignKeyInfo> foreignKeys = getTableForeignKeys(tableName);
                
                // Check if it's a view
                boolean isView = isTableView(tableName);
                
                TableInfo tableInfo = new TableInfo(tableName, columns, foreignKeys, isView);
                tables.put(tableName, tableInfo);
            }
            
            log.info("Successfully reflected {} tables from schema '{}'", tables.size(), allowedSchema);
            
        } catch (Exception e) {
            log.error("Error reflecting database schema: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to reflect database schema", e);
        }
        
        return tables;
    }

    /**
     * Get all table names in the schema with role-based filtering
     * Only returns tables that the current database user has SELECT permissions on
     */
    private List<String> getTableNames() {
        String query = """
            SELECT DISTINCT t.table_name 
            FROM information_schema.tables t
            WHERE t.table_schema = ? 
            AND t.table_type IN ('BASE TABLE', 'VIEW')
            AND (
                -- Check if current user has SELECT privilege on this table
                has_table_privilege(current_user, t.table_schema || '.' || t.table_name, 'SELECT')
                OR 
                -- Check if current user is table owner (always has access)
                pg_has_role(current_user, t.table_name::regrole, 'USAGE')
                OR
                -- For public schema, also check if there are any explicit grants
                EXISTS (
                    SELECT 1 FROM information_schema.table_privileges tp
                    WHERE tp.table_schema = t.table_schema 
                    AND tp.table_name = t.table_name
                    AND tp.grantee IN (current_user, 'PUBLIC')
                    AND tp.privilege_type IN ('SELECT', 'ALL PRIVILEGES')
                )
            )
            ORDER BY t.table_name
            """;
            
        List<String> tableAndViewNames;
        try {
            tableAndViewNames = jdbcTemplate.queryForList(query, String.class, allowedSchema);
        } catch (Exception e) {
            log.warn("Role-based table filtering failed, falling back to basic schema query: {}", e.getMessage());
            String fallbackQuery = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ?
                AND table_type IN ('BASE TABLE', 'VIEW')
                ORDER BY table_name
                """;
            tableAndViewNames = jdbcTemplate.queryForList(fallbackQuery, String.class, allowedSchema);
        }

        // Phase 7B: also include materialized views (not in information_schema.tables)
        List<String> matViewNames = getMatViewNames();

        List<String> all = new java.util.ArrayList<>(tableAndViewNames);
        all.addAll(matViewNames);
        return all;
    }

    /**
     * Get materialized view names from pg_matviews (not included in information_schema.tables).
     */
    private List<String> getMatViewNames() {
        try {
            return jdbcTemplate.queryForList(
                "SELECT matviewname FROM pg_catalog.pg_matviews WHERE schemaname = ?",
                String.class, allowedSchema);
        } catch (Exception e) {
            log.warn("Failed to fetch materialized views: {}", e.getMessage());
            return java.util.Collections.emptyList();
        }
    }

    /**
     * Phase 7A: Resolve a column type that may be a domain type to its base PostgreSQL type.
     * If the given type name exists in the domain map, returns the base type; otherwise returns the type as-is.
     *
     * @param typeName  the type name from pg_catalog (may be a domain type name)
     * @param domainMap mapping of domain name → base type (from getDomainTypeToBaseTypeMap)
     * @return the base PostgreSQL type, or typeName if not a domain
     */
    public String resolveDomainType(String typeName, Map<String, String> domainMap) {
        if (typeName == null || domainMap == null) return typeName;
        return domainMap.getOrDefault(typeName, typeName);
    }

    /**
     * Get column information for a specific table with role-based filtering
     * Only returns columns that the current database user has SELECT permissions on
     */
    private List<ColumnInfo> getTableColumns(String tableName) {
        String query = """
            SELECT 
                a.attname as column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) as full_type,
                NOT a.attnotnull as is_nullable,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku 
                    ON tc.constraint_name = ku.constraint_name
                    AND tc.table_schema = ku.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = ?
                AND tc.table_name = ?
            ) pk ON a.attname = pk.column_name
            WHERE n.nspname = ?
            AND c.relname = ?
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND (
                -- Check if current user has SELECT privilege on this column
                has_column_privilege(current_user, n.nspname || '.' || c.relname, a.attname, 'SELECT')
                OR 
                -- Check if current user has SELECT privilege on the whole table
                has_table_privilege(current_user, n.nspname || '.' || c.relname, 'SELECT')
                OR
                -- Check if there are explicit column grants
                EXISTS (
                    SELECT 1 FROM information_schema.column_privileges cp
                    WHERE cp.table_schema = n.nspname 
                    AND cp.table_name = c.relname
                    AND cp.column_name = a.attname
                    AND cp.grantee IN (current_user, 'PUBLIC')
                    AND cp.privilege_type IN ('SELECT', 'ALL PRIVILEGES')
                )
            )
            ORDER BY a.attnum
            """;
            
        return jdbcTemplate.query(query, (rs, rowNum) -> {
            String columnName = rs.getString("column_name");
            String fullType = rs.getString("full_type");
            boolean isNullable = rs.getBoolean("is_nullable");
            boolean isPrimaryKey = rs.getBoolean("is_primary_key");
            
            // Use the full type from PostgreSQL catalog
            String finalType = fullType;
            
            // Debug BIT type detection
            if (finalType != null && finalType.startsWith("bit")) {
                log.debug("BIT type detected - columnName: {}, fullType: {}", columnName, fullType);
            }
            
            // Detect and enhance PostgreSQL custom types
            finalType = enhancePostgreSQLType(finalType, fullType);
            
            return new ColumnInfo(columnName, finalType, isPrimaryKey, isNullable);
        }, allowedSchema, tableName, allowedSchema, tableName);
    }

    /**
     * Get foreign key information for a specific table
     */
    private List<ForeignKeyInfo> getTableForeignKeys(String tableName) {
        // Use pg_catalog instead of information_schema.constraint_column_usage
        // because the latter only shows constraints owned by the current user,
        // which fails when the app connects as a non-owner role (e.g. appuser).
        String query = """
            SELECT
                a_child.attname  AS column_name,
                cl_parent.relname AS referenced_table,
                a_parent.attname AS referenced_column
            FROM pg_constraint con
            JOIN pg_class cl_child ON cl_child.oid = con.conrelid
            JOIN pg_namespace ns ON ns.oid = cl_child.relnamespace
            JOIN pg_class cl_parent ON cl_parent.oid = con.confrelid
            JOIN pg_attribute a_child
                ON a_child.attrelid = con.conrelid
                AND a_child.attnum = ANY(con.conkey)
            JOIN pg_attribute a_parent
                ON a_parent.attrelid = con.confrelid
                AND a_parent.attnum = ANY(con.confkey)
            WHERE con.contype = 'f'
              AND ns.nspname = ?
              AND cl_child.relname = ?
            ORDER BY a_child.attnum
            """;
            
        return jdbcTemplate.query(query, (rs, rowNum) -> {
            String columnName = rs.getString("column_name");
            String referencedTable = rs.getString("referenced_table");
            String referencedColumn = rs.getString("referenced_column");
            
            return new ForeignKeyInfo(columnName, referencedTable, referencedColumn);
        }, allowedSchema, tableName);
    }

    /**
     * Check if a table is a view
     */
    private boolean isTableView(String tableName) {
        String query = """
            SELECT COUNT(*) 
            FROM information_schema.views 
            WHERE table_schema = ? 
            AND table_name = ?
            """;
            
        Integer count = jdbcTemplate.queryForObject(query, Integer.class, allowedSchema, tableName);
        return count != null && count > 0;
    }

    /**
     * Clear the schema cache (useful for testing or when schema changes)
     */
    public void clearCache() {
        schemaCache.clear();
        lastCacheUpdate = 0;
        log.info("Schema cache cleared");
    }

    /**
     * Invalidate schema cache (alias for clearCache for API consistency)
     */
    public void invalidateSchemaCache() {
        clearCache();
    }

    /**
     * Get schema cache statistics
     */
    public Map<String, Object> getSchemaCacheStats() {
        long currentTime = System.currentTimeMillis();
        long cacheAge = lastCacheUpdate > 0 ? currentTime - lastCacheUpdate : 0;
        boolean isCacheValid = schemaCache.containsKey(allowedSchema) && cacheAge < CACHE_TTL_MS;

        return Map.of(
            "totalEntries", schemaCache.size(),
            "validEntries", isCacheValid ? schemaCache.size() : 0,
            "cacheAgeMs", cacheAge,
            "ttlMs", CACHE_TTL_MS,
            "isValid", isCacheValid
        );
    }

    /**
     * Get the current allowed schema
     */
    public String getAllowedSchema() {
        return allowedSchema;
    }

    /**
     * Get the current database type
     */
    public DatabaseType getDatabaseType() {
        return databaseType;
    }
    
    /**
     * Enhance PostgreSQL type detection for custom types (enums, composites, networks, arrays)
     */
    private String enhancePostgreSQLType(String finalType, String dataType) {
        if (databaseType != DatabaseType.POSTGRES) {
            return finalType;
        }
        
        try {
            // PostgreSQL catalog query already gives us proper format like "text[]", "bit(8)", etc.
            // We just need to handle special type mappings
            
            // Handle array types - they already come in correct format from pg_catalog.format_type()
            if (finalType.endsWith("[]")) {
                // Array types are already in correct format, no conversion needed
                return finalType;
            }
            
            // Handle non-array types - check if it's a custom enum type (but not for arrays)
            String baseType = finalType;
            if (isEnumType(baseType)) {
                return ColumnTypeConstant.POSTGRES_ENUM + ":" + baseType;
            }
            
            // Check if it's a composite type (but not for arrays)
            if (isCompositeType(baseType)) {
                return ColumnTypeConstant.POSTGRES_COMPOSITE + ":" + baseType;
            }
            
            // Map known PostgreSQL network types
            return switch (finalType.toLowerCase()) {
                case "inet" -> ColumnTypeConstant.INET;
                case "cidr" -> ColumnTypeConstant.CIDR;
                case "macaddr" -> ColumnTypeConstant.MACADDR;
                case "macaddr8" -> ColumnTypeConstant.MACADDR8;
                default -> finalType;
            };
            
        } catch (Exception e) {
            log.warn("Failed to enhance PostgreSQL type '{}': {}", finalType, e.getMessage());
            return finalType;
        }
    }
    
    /**
     * Check if a type is a PostgreSQL enum
     */
    private boolean isEnumType(String typeName) {
        String query = """
            SELECT COUNT(*) > 0
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            WHERE t.typname = ?
            """;
        
        try {
            Boolean isEnum = jdbcTemplate.queryForObject(query, Boolean.class, typeName);
            return isEnum != null && isEnum;
        } catch (Exception e) {
            log.debug("Error checking if '{}' is enum type: {}", typeName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Check if a type is a PostgreSQL composite type
     */
    private boolean isCompositeType(String typeName) {
        String query = """
            SELECT COUNT(*) > 0
            FROM pg_type t
            WHERE t.typname = ?
            AND t.typtype = 'c'
            """;
        
        try {
            Boolean isComposite = jdbcTemplate.queryForObject(query, Boolean.class, typeName);
            return isComposite != null && isComposite;
        } catch (Exception e) {
            log.debug("Error checking if '{}' is composite type: {}", typeName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Get enum values for a PostgreSQL enum type
     */
    public List<String> getEnumValues(String enumTypeName) {
        String query = """
            SELECT e.enumlabel
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            WHERE t.typname = ?
            ORDER BY e.enumsortorder
            """;
        
        try {
            return jdbcTemplate.queryForList(query, String.class, enumTypeName);
        } catch (Exception e) {
            log.warn("Failed to get enum values for '{}': {}", enumTypeName, e.getMessage());
            return List.of();
        }
    }
    
    /**
     * Get composite type definition
     */
    public Map<String, String> getCompositeTypeDefinition(String compositeTypeName) {
        String query = """
            SELECT a.attname, t.typname
            FROM pg_type pt
            JOIN pg_class c ON pt.typrelid = c.oid
            JOIN pg_attribute a ON c.oid = a.attrelid
            JOIN pg_type t ON a.atttypid = t.oid
            WHERE pt.typname = ?
            AND a.attnum > 0
            AND NOT a.attisdropped
            ORDER BY a.attnum
            """;

        try {
            Map<String, String> definition = new HashMap<>();
            jdbcTemplate.query(query, rs -> {
                String fieldName = rs.getString("attname");
                String fieldType = rs.getString("typname");
                definition.put(fieldName, fieldType);
            }, compositeTypeName);
            return definition;
        } catch (Exception e) {
            log.warn("Failed to get composite type definition for '{}': {}", compositeTypeName, e.getMessage());
            return Map.of();
        }
    }

    /**
     * Get all custom enum types from the database
     */
    public List<CustomEnumTypeInfo> getCustomEnumTypes() {
        String query = """
            SELECT DISTINCT n.nspname as schema_name, t.typname as type_name
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typtype = 'e'
            AND n.nspname = ?
            ORDER BY t.typname
            """;

        try {
            List<CustomEnumTypeInfo> enumTypes = new ArrayList<>();
            jdbcTemplate.query(query, rs -> {
                String schemaName = rs.getString("schema_name");
                String typeName = rs.getString("type_name");

                // Get enum values
                List<String> values = getEnumValues(typeName);

                enumTypes.add(new CustomEnumTypeInfo(schemaName, typeName, values));
            }, allowedSchema);

            return enumTypes;
        } catch (Exception e) {
            log.warn("Failed to get custom enum types: {}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Get all custom composite types from the database
     */
    public List<CustomCompositeTypeInfo> getCustomCompositeTypes() {
        String query = """
            SELECT DISTINCT n.nspname as schema_name, t.typname as type_name
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE t.typtype = 'c'
            AND n.nspname = ?
            ORDER BY t.typname
            """;

        try {
            List<CustomCompositeTypeInfo> compositeTypes = new ArrayList<>();
            jdbcTemplate.query(query, rs -> {
                String schemaName = rs.getString("schema_name");
                String typeName = rs.getString("type_name");

                // Get composite attributes
                List<CompositeTypeAttribute> attributes = getCompositeTypeAttributes(typeName);

                compositeTypes.add(new CustomCompositeTypeInfo(schemaName, typeName, attributes));
            }, allowedSchema);

            return compositeTypes;
        } catch (Exception e) {
            log.warn("Failed to get custom composite types: {}", e.getMessage());
            return List.of();
        }
    }

    /**
     * Get attributes for a specific composite type
     */
    private List<CompositeTypeAttribute> getCompositeTypeAttributes(String compositeTypeName) {
        String query = """
            SELECT a.attname as attribute_name,
                   format_type(a.atttypid, a.atttypmod) as attribute_type,
                   a.attnum as attribute_order
            FROM pg_type pt
            JOIN pg_class c ON pt.typrelid = c.oid
            JOIN pg_attribute a ON c.oid = a.attrelid
            WHERE pt.typname = ?
            AND a.attnum > 0
            AND NOT a.attisdropped
            ORDER BY a.attnum
            """;

        try {
            return jdbcTemplate.query(query, (rs, rowNum) -> {
                String name = rs.getString("attribute_name");
                String type = rs.getString("attribute_type");
                int order = rs.getInt("attribute_order");

                return new CompositeTypeAttribute(name, type, order);
            }, compositeTypeName);
        } catch (Exception e) {
            log.warn("Failed to get attributes for composite type '{}': {}", compositeTypeName, e.getMessage());
            return List.of();
        }
    }
}