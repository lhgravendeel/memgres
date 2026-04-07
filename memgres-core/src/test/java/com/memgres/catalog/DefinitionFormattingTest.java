package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #56-57: pg_get_viewdef formatting, needs pretty-print with indentation and semicolon
 * Diff #35: COMMENT ON INDEX should validate index exists
 */
class DefinitionFormattingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static String scalar(String sql) throws SQLException { try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) { return rs.next() ? rs.getString(1) : null; } }

    // Diffs #56-57: pg_get_viewdef, exact pattern from 45_definition_and_catalog_helpers.sql
    @Test void pg_get_viewdef_pretty_printed() throws SQLException {
        exec("CREATE SCHEMA def_test"); exec("SET search_path = def_test");
        exec("CREATE TABLE def_help_t(id int PRIMARY KEY, note text DEFAULT 'x')");
        exec("CREATE VIEW def_help_v AS SELECT id, note FROM def_help_t");
        try {
            String def = scalar("SELECT pg_get_viewdef('def_help_v'::regclass, true)");
            assertNotNull(def);
            // PG returns: " SELECT id,\n    note\n   FROM def_help_t;"
            // Must have: indentation (newlines), trailing semicolon, columns on separate lines
            assertTrue(def.contains("\n"), "Pretty-printed viewdef should have newlines: " + def);
            assertTrue(def.trim().endsWith(";"), "Viewdef should end with semicolon: " + def);
        } finally {
            exec("DROP SCHEMA def_test CASCADE"); exec("SET search_path = public");
        }
    }

    @Test void pg_get_viewdef_via_pg_class() throws SQLException {
        exec("CREATE SCHEMA def_test2"); exec("SET search_path = def_test2");
        exec("CREATE TABLE def_help_t(id int PRIMARY KEY, note text DEFAULT 'x')");
        exec("CREATE VIEW def_help_v AS SELECT id, note FROM def_help_t");
        try {
            String def = scalar("""
                SELECT pg_get_viewdef(c.oid, true)
                FROM pg_class c WHERE c.oid = 'def_help_v'::regclass
                """);
            assertNotNull(def);
            assertTrue(def.contains("\n"), "Viewdef via pg_class should have newlines");
            assertTrue(def.trim().endsWith(";"), "Should end with semicolon");
        } finally {
            exec("DROP SCHEMA def_test2 CASCADE"); exec("SET search_path = public");
        }
    }

    // Diff #35: COMMENT ON INDEX should validate index exists
    @Test void comment_on_nonexistent_index_fails() throws SQLException {
        exec("CREATE TABLE com_t(id int PRIMARY KEY)");
        try {
            // COMMENT ON INDEX for a name that is not actually an index should fail
            // admin_t_pkey is a PK constraint name, but PG validates it as an index
            // For a truly nonexistent name:
            assertThrows(SQLException.class,
                () -> exec("COMMENT ON INDEX nonexistent_idx IS 'test'"),
                "COMMENT ON INDEX for nonexistent index should fail");
        } finally {
            exec("DROP TABLE com_t");
        }
    }

    @Test void comment_on_real_index_succeeds() throws SQLException {
        exec("CREATE TABLE com2_t(id int PRIMARY KEY, a int)");
        exec("CREATE INDEX com2_idx ON com2_t(a)");
        try {
            exec("COMMENT ON INDEX com2_idx IS 'my index comment'");
        } finally {
            exec("DROP TABLE com2_t CASCADE");
        }
    }
}
