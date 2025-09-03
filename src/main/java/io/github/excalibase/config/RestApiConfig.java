package io.github.excalibase.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the REST API application
 */
@ConfigurationProperties(prefix = "app")
public class RestApiConfig {

    private String allowedSchema = "public";
    private String databaseType = "postgres";
    private int maxPageSize = 1000;
    private int defaultPageSize = 100;
    private long schemaCacheTtlSeconds = 300; // 5 minutes
    
    // CORS configuration
    private CorsConfig cors = new CorsConfig();
    
    // Security configuration
    private SecurityConfig security = new SecurityConfig();

    public String getAllowedSchema() {
        return allowedSchema;
    }

    public void setAllowedSchema(String allowedSchema) {
        this.allowedSchema = allowedSchema;
    }

    public String getDatabaseType() {
        return databaseType;
    }

    public void setDatabaseType(String databaseType) {
        this.databaseType = databaseType;
    }

    public int getMaxPageSize() {
        return maxPageSize;
    }

    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    public int getDefaultPageSize() {
        return defaultPageSize;
    }

    public void setDefaultPageSize(int defaultPageSize) {
        this.defaultPageSize = defaultPageSize;
    }

    public long getSchemaCacheTtlSeconds() {
        return schemaCacheTtlSeconds;
    }

    public void setSchemaCacheTtlSeconds(long schemaCacheTtlSeconds) {
        this.schemaCacheTtlSeconds = schemaCacheTtlSeconds;
    }

    public CorsConfig getCors() {
        return cors;
    }

    public void setCors(CorsConfig cors) {
        this.cors = cors;
    }

    public SecurityConfig getSecurity() {
        return security;
    }

    public void setSecurity(SecurityConfig security) {
        this.security = security;
    }

    public static class CorsConfig {
        private boolean enabled = true;
        private String[] allowedOrigins = {"*"};
        private String[] allowedMethods = {"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"};
        private String[] allowedHeaders = {"*"};
        private boolean allowCredentials = false;
        private long maxAge = 3600; // 1 hour

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String[] getAllowedOrigins() {
            return allowedOrigins;
        }

        public void setAllowedOrigins(String[] allowedOrigins) {
            this.allowedOrigins = allowedOrigins;
        }

        public String[] getAllowedMethods() {
            return allowedMethods;
        }

        public void setAllowedMethods(String[] allowedMethods) {
            this.allowedMethods = allowedMethods;
        }

        public String[] getAllowedHeaders() {
            return allowedHeaders;
        }

        public void setAllowedHeaders(String[] allowedHeaders) {
            this.allowedHeaders = allowedHeaders;
        }

        public boolean isAllowCredentials() {
            return allowCredentials;
        }

        public void setAllowCredentials(boolean allowCredentials) {
            this.allowCredentials = allowCredentials;
        }

        public long getMaxAge() {
            return maxAge;
        }

        public void setMaxAge(long maxAge) {
            this.maxAge = maxAge;
        }
    }

    public static class SecurityConfig {
        private boolean enableSqlInjectionProtection = true;
        private boolean enableTableNameValidation = true;
        private boolean enableColumnNameValidation = true;
        private int maxRequestBodySize = 1048576; // 1MB
        private int maxQueryComplexity = 100;

        public boolean isEnableSqlInjectionProtection() {
            return enableSqlInjectionProtection;
        }

        public void setEnableSqlInjectionProtection(boolean enableSqlInjectionProtection) {
            this.enableSqlInjectionProtection = enableSqlInjectionProtection;
        }

        public boolean isEnableTableNameValidation() {
            return enableTableNameValidation;
        }

        public void setEnableTableNameValidation(boolean enableTableNameValidation) {
            this.enableTableNameValidation = enableTableNameValidation;
        }

        public boolean isEnableColumnNameValidation() {
            return enableColumnNameValidation;
        }

        public void setEnableColumnNameValidation(boolean enableColumnNameValidation) {
            this.enableColumnNameValidation = enableColumnNameValidation;
        }

        public int getMaxRequestBodySize() {
            return maxRequestBodySize;
        }

        public void setMaxRequestBodySize(int maxRequestBodySize) {
            this.maxRequestBodySize = maxRequestBodySize;
        }

        public int getMaxQueryComplexity() {
            return maxQueryComplexity;
        }

        public void setMaxQueryComplexity(int maxQueryComplexity) {
            this.maxQueryComplexity = maxQueryComplexity;
        }
    }
}