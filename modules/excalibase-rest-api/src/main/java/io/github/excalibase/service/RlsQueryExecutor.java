package io.github.excalibase.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes SQL queries on a raw JDBC connection with RLS context (set_config) applied.
 * Guarantees that SET + query run on the SAME connection by obtaining a raw connection
 * from the DataSource and managing autoCommit manually.
 *
 * This replaces the UserContextFilter approach (which relied on TransactionTemplate
 * to keep SET and query on the same connection through Spring's connection binding).
 */
@Service
public class RlsQueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(RlsQueryExecutor.class);
    private static final String SET_CONFIG_SQL = "SELECT set_config(?, ?, true)";

    private final DataSource dataSource;

    public RlsQueryExecutor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Execute a SELECT query with RLS context, returning results as List of Maps
     * (same shape as JdbcTemplate.queryForList).
     */
    public List<Map<String, Object>> queryForList(String userId, Map<String, String> claims,
                                                   String sql, Object... params) throws SQLException {
        Connection conn = dataSource.getConnection();
        boolean originalAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(false);
            setRlsContext(conn, userId, claims);
            List<Map<String, Object>> result = executeQuery(conn, sql, params);
            conn.commit();
            return result;
        } catch (SQLException e) {
            rollbackQuietly(conn);
            throw e;
        } finally {
            restoreAndClose(conn, originalAutoCommit);
        }
    }

    /**
     * Execute an INSERT/UPDATE/DELETE with RLS context, returning affected row count.
     */
    public int update(String userId, Map<String, String> claims,
                      String sql, Object... params) throws SQLException {
        Connection conn = dataSource.getConnection();
        boolean originalAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(false);
            setRlsContext(conn, userId, claims);
            int affected = executeUpdate(conn, sql, params);
            conn.commit();
            return affected;
        } catch (SQLException e) {
            rollbackQuietly(conn);
            throw e;
        } finally {
            restoreAndClose(conn, originalAutoCommit);
        }
    }

    /**
     * Execute an INSERT/UPDATE/DELETE with RETURNING clause, returning results as List of Maps.
     */
    public List<Map<String, Object>> queryForListReturning(String userId, Map<String, String> claims,
                                                            String sql, Object... params) throws SQLException {
        return queryForList(userId, claims, sql, params);
    }

    /**
     * Set RLS context variables on the connection using parameterized set_config calls.
     */
    private void setRlsContext(Connection conn, String userId, Map<String, String> claims) throws SQLException {
        // Set primary user_id
        try (PreparedStatement ps = conn.prepareStatement(SET_CONFIG_SQL)) {
            ps.setString(1, "request.user_id");
            ps.setString(2, userId);
            ps.execute();
        }
        log.debug("RLS context set: request.user_id={}", userId);

        // Set additional claims
        if (claims != null) {
            for (Map.Entry<String, String> entry : claims.entrySet()) {
                String value = entry.getValue();
                if (value != null && !value.isEmpty()) {
                    String varName = "request.jwt." + sanitizeVariableName(entry.getKey());
                    try (PreparedStatement ps = conn.prepareStatement(SET_CONFIG_SQL)) {
                        ps.setString(1, varName);
                        ps.setString(2, value);
                        ps.execute();
                    }
                    log.debug("RLS context set: {}={}", varName, value);
                }
            }
        }
    }

    /**
     * Execute a query and convert ResultSet to List of Maps (mimics JdbcTemplate.queryForList).
     */
    private List<Map<String, Object>> executeQuery(Connection conn, String sql,
                                                    Object... params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            setParameters(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return resultSetToList(rs);
            }
        }
    }

    /**
     * Execute an update statement and return affected row count.
     */
    private int executeUpdate(Connection conn, String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            setParameters(ps, params);
            return ps.executeUpdate();
        }
    }

    /**
     * Convert a ResultSet into a List of LinkedHashMaps (preserving column order).
     */
    private List<Map<String, Object>> resultSetToList(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        List<Map<String, Object>> results = new ArrayList<>();

        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                String colName = meta.getColumnLabel(i);
                row.put(colName, rs.getObject(i));
            }
            results.add(row);
        }
        return results;
    }

    private void setParameters(PreparedStatement ps, Object... params) throws SQLException {
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
        }
    }

    private String sanitizeVariableName(String name) {
        if (name == null || name.isEmpty()) {
            return "unknown";
        }
        return name.replaceAll("[^a-zA-Z0-9_.]", "_");
    }

    private void rollbackQuietly(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException e) {
            log.debug("Rollback failed (usually harmless): {}", e.getMessage());
        }
    }

    private void restoreAndClose(Connection conn, boolean originalAutoCommit) {
        try {
            conn.setAutoCommit(originalAutoCommit);
        } catch (SQLException e) {
            log.debug("Failed to restore autoCommit: {}", e.getMessage());
        }
        try {
            conn.close();
        } catch (SQLException e) {
            log.debug("Failed to close connection: {}", e.getMessage());
        }
    }
}
