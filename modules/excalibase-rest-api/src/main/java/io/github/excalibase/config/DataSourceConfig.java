package io.github.excalibase.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.zaxxer.hikari.HikariDataSource;

import static io.github.excalibase.util.SqlIdentifier.quoteIdentifier;

/**
 * Sets PostgreSQL {@code search_path} on every new connection so tables
 * in non-public schemas (e.g. {@code dvdrental}) are found without
 * schema-qualifying table names in SQL.
 *
 * <p>Future: per-request schema switching via header for multi-tenant support.
 */
@Configuration
public class DataSourceConfig {

    private static final Logger log = LoggerFactory.getLogger(DataSourceConfig.class);

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariDataSource dataSource(DataSourceProperties properties,
                                        @Value("${app.allowed-schema:public}") String allowedSchema) {
        HikariDataSource ds = properties.initializeDataSourceBuilder()
                .type(HikariDataSource.class)
                .build();

        String initSql = "SET search_path = " + quoteIdentifier(allowedSchema) + ", public";
        ds.setConnectionInitSql(initSql);
        log.info("HikariCP connection-init-sql: {}", initSql);

        return ds;
    }
}