package io.github.excalibase.constant;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTypeTest {

    @Test
    void postgres_hasCorrectName() {
        assertEquals("postgres", DatabaseType.POSTGRES.getName());
    }

    @Test
    void mysql_hasCorrectName() {
        assertEquals("mysql", DatabaseType.MYSQL.getName());
    }

    @Test
    void oracle_hasCorrectName() {
        assertEquals("oracle", DatabaseType.ORACLE.getName());
    }

    @Test
    void sqlServer_hasCorrectName() {
        assertEquals("sqlserver", DatabaseType.SQL_SERVER.getName());
    }

    @Test
    void dynamodb_hasCorrectName() {
        assertEquals("dynamodb", DatabaseType.DYNAMODB.getName());
    }

    @Test
    void allValues_containsFiveTypes() {
        DatabaseType[] types = DatabaseType.values();
        assertEquals(5, types.length);
    }

    @Test
    void valueOf_postgres_returnsCorrectEnum() {
        assertEquals(DatabaseType.POSTGRES, DatabaseType.valueOf("POSTGRES"));
    }

    @Test
    void valueOf_mysql_returnsCorrectEnum() {
        assertEquals(DatabaseType.MYSQL, DatabaseType.valueOf("MYSQL"));
    }
}
