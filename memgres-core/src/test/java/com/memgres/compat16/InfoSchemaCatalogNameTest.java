package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests information_schema.table_catalog behavior.
 *
 * PG 18: information_schema views report the actual database name in
 * table_catalog, table_schema, etc.
 *
 * Memgres: Always reports "memgres" as table_catalog regardless of the actual
 * database name used in the connection.
 *
 * These tests assert PG 18 behavior and are expected to fail on Memgres.
 */
class InfoSchemaCatalogNameTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // -------------------------------------------------------------------------
    // table_catalog should match current_database()
    // -------------------------------------------------------------------------

    @Test
    void tableCatalog_shouldMatchCurrentDatabase() throws SQLException {
        exec("DROP TABLE IF EXISTS is_catalog_test");
        exec("CREATE TABLE is_catalog_test (id int, name text)");

        String currentDb;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT current_database()")) {
            assertTrue(rs.next());
            currentDb = rs.getString(1);
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT table_catalog FROM information_schema.tables "
                             + "WHERE table_name = 'is_catalog_test'")) {
            assertTrue(rs.next());
            String catalog = rs.getString(1);
            assertEquals(currentDb, catalog,
                    "information_schema.tables.table_catalog should be '"
                            + currentDb + "' (from current_database()), got '" + catalog + "'");
        }

        exec("DROP TABLE is_catalog_test");
    }

    // -------------------------------------------------------------------------
    // columns view should also report correct catalog
    // -------------------------------------------------------------------------

    @Test
    void columnsCatalog_shouldMatchCurrentDatabase() throws SQLException {
        exec("DROP TABLE IF EXISTS is_col_catalog_test");
        exec("CREATE TABLE is_col_catalog_test (id int, name text)");

        String currentDb;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT current_database()")) {
            assertTrue(rs.next());
            currentDb = rs.getString(1);
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT table_catalog FROM information_schema.columns "
                             + "WHERE table_name = 'is_col_catalog_test' AND column_name = 'id'")) {
            assertTrue(rs.next());
            String catalog = rs.getString(1);
            assertEquals(currentDb, catalog,
                    "information_schema.columns.table_catalog should be '"
                            + currentDb + "', got '" + catalog + "'");
        }

        exec("DROP TABLE is_col_catalog_test");
    }

    // -------------------------------------------------------------------------
    // schemata view should report correct catalog_name
    // -------------------------------------------------------------------------

    @Test
    void schemataCatalogName_shouldMatchCurrentDatabase() throws SQLException {
        String currentDb;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT current_database()")) {
            assertTrue(rs.next());
            currentDb = rs.getString(1);
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT catalog_name FROM information_schema.schemata "
                             + "WHERE schema_name = 'public'")) {
            assertTrue(rs.next());
            String catalog = rs.getString(1);
            assertEquals(currentDb, catalog,
                    "information_schema.schemata.catalog_name should be '"
                            + currentDb + "', got '" + catalog + "'");
        }
    }

    // -------------------------------------------------------------------------
    // constraint_catalog should match
    // -------------------------------------------------------------------------

    @Test
    void constraintCatalog_shouldMatchCurrentDatabase() throws SQLException {
        exec("DROP TABLE IF EXISTS is_const_catalog_test");
        exec("CREATE TABLE is_const_catalog_test (id int PRIMARY KEY, name text NOT NULL)");

        String currentDb;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT current_database()")) {
            assertTrue(rs.next());
            currentDb = rs.getString(1);
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT constraint_catalog FROM information_schema.table_constraints "
                             + "WHERE table_name = 'is_const_catalog_test' LIMIT 1")) {
            assertTrue(rs.next());
            String catalog = rs.getString(1);
            assertEquals(currentDb, catalog,
                    "information_schema.table_constraints.constraint_catalog should be '"
                            + currentDb + "', got '" + catalog + "'");
        }

        exec("DROP TABLE is_const_catalog_test");
    }
}
