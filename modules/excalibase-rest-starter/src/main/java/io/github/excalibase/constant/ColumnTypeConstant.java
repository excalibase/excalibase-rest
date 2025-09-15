package io.github.excalibase.constant;

/**
 * Database column type constants for REST API type mapping.
 */
public class ColumnTypeConstant {
    private ColumnTypeConstant() {
    }

    // UUID and identifier types
    public static final String UUID = "uuid";

    // Integer types
    public static final String INT = "int";
    public static final String INT2 = "int2";
    public static final String INT4 = "int4"; 
    public static final String INT8 = "int8";
    public static final String INTEGER = "integer";
    public static final String BIGINT = "bigint";
    public static final String SMALLINT = "smallint";
    public static final String SERIAL = "serial";
    public static final String SERIAL2 = "serial2";
    public static final String SERIAL4 = "serial4";
    public static final String SERIAL8 = "serial8";
    public static final String SMALLSERIAL = "smallserial";
    public static final String BIGSERIAL = "bigserial";

    // Decimal and floating-point types
    public static final String DECIMAL = "decimal";
    public static final String NUMERIC = "numeric";
    public static final String DOUBLE = "double";
    public static final String FLOAT = "float";
    public static final String FLOAT4 = "float4";
    public static final String FLOAT8 = "float8";
    public static final String REAL = "real";
    public static final String DOUBLE_PRECISION = "double precision";

    // Boolean types
    
    /** Boolean column type (short form) */
    public static final String BOOL = "bool";
    
    /** Boolean column type (full form) */
    public static final String BOOLEAN = "boolean";

    // Date and time types

    /** Timestamp column type */
    public static final String TIMESTAMP = "timestamp";
    
    /** Timestamp with time zone (full form) */
    public static final String TIMESTAMP_WITH_TIME_ZONE = "timestamp with time zone";
    
    /** Timestamp without time zone (full form) */
    public static final String TIMESTAMP_WITHOUT_TIME_ZONE = "timestamp without time zone";
    
    /** Date column type */
    public static final String DATE = "date";
    
    /** Time column type */
    public static final String TIME = "time";
    
    /** Time with time zone (full form) */
    public static final String TIME_WITH_TIME_ZONE = "time with time zone";
    
    /** Time without time zone (full form) */
    public static final String TIME_WITHOUT_TIME_ZONE = "time without time zone";

    // JSON types
    
    /** JSON column type */
    public static final String JSON = "json";
    
    /** JSONB column type (binary JSON) */
    public static final String JSONB = "jsonb";

    // Array and composite types
    
    /** Array type suffix */
    public static final String ARRAY_SUFFIX = "[]";

    // Binary and network types
    
    /** Binary data type */
    public static final String BYTEA = "bytea";
    
    /** Network address type */
    public static final String INET = "inet";
    
    /** Network address type with subnet */
    public static final String CIDR = "cidr";
    
    /** MAC address type */
    public static final String MACADDR = "macaddr";
    
    /** MAC address type (8 bytes) */
    public static final String MACADDR8 = "macaddr8";

    // Enhanced date/time types
    
    /** Timestamp with timezone */
    public static final String TIMESTAMPTZ = "timestamptz";
    
    /** Time with timezone */
    public static final String TIMETZ = "timetz";
    
    /** Interval type */
    public static final String INTERVAL = "interval";

    // Additional numeric types

    /** Bit string type */
    public static final String BIT = "bit";
    
    /** Variable bit string type */
    public static final String VARBIT = "varbit";
    
    /** Bit varying type (full form) */
    public static final String BIT_VARYING = "bit varying";

    // Text and character types
    
    /** Variable character type */
    public static final String VARCHAR = "varchar";
    
    /** Text type */
    public static final String TEXT = "text";
    
    /** Character type */
    public static final String CHAR = "char";
    
    /** Character type (full form) */
    public static final String CHARACTER = "character";
    
    /** Character varying type (PostgreSQL full form) */
    public static final String CHARACTER_VARYING = "character varying";
    
    /** Blank-padded character type */
    public static final String BPCHAR = "bpchar";

    // XML types
    
    /** XML type */
    public static final String XML = "xml";

    public static final String POSTGRES_ENUM = "postgres_enum";
    public static final String POSTGRES_COMPOSITE = "postgres_composite";
}