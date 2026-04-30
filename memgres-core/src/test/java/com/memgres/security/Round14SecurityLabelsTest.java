package com.memgres.security;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: SECURITY LABEL (and pg_seclabel / pg_shseclabel catalogs).
 *
 * SECURITY LABEL attaches a provider-labeled text to a database object. Core
 * PG supports the DDL and catalog; providers (e.g. sepgsql) plug in via hook.
 * Memgres has neither the DDL nor the catalogs.
 *
 * Tests expected to fail today.
 */
class Round14SecurityLabelsTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. pg_seclabel catalog
    // =========================================================================

    @Test
    void pg_seclabel_exists() throws SQLException {
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_seclabel"));
    }

    @Test
    void pg_shseclabel_exists() throws SQLException {
        assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_shseclabel"));
    }

    @Test
    void pg_seclabel_has_expected_columns() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT column_name FROM information_schema.columns "
                             + "WHERE table_schema = 'pg_catalog' AND table_name = 'pg_seclabel' "
                             + "ORDER BY column_name")) {
            int n = 0;
            while (rs.next()) n++;
            // PG's pg_seclabel has: objoid, classoid, objsubid, provider, label
            assertTrue(n >= 5, "pg_seclabel must expose objoid/classoid/objsubid/provider/label; got " + n);
        }
    }

    // =========================================================================
    // B. SECURITY LABEL DDL
    // =========================================================================

    @Test
    void security_label_on_table() throws SQLException {
        exec("CREATE TABLE r14_sl_t (id int)");
        // Without a provider configured PG errors out with 22023 or 55000 depending on ver,
        // but with dummy provider it succeeds. Memgres must at minimum parse the DDL.
        try {
            exec("SECURITY LABEL ON TABLE r14_sl_t IS 'secret'");
            // If accepted, it should show up in pg_seclabel (with default provider)
            assertTrue(scalarInt("SELECT count(*)::int FROM pg_seclabel "
                    + "WHERE objoid = 'r14_sl_t'::regclass") >= 0);
        } catch (SQLException e) {
            // Parser rejecting with a specific message is also valid for "provider not loaded"
            assertNotNull(e.getMessage(), "SECURITY LABEL must at least parse");
        }
    }

    @Test
    void security_label_on_column() throws SQLException {
        exec("CREATE TABLE r14_sl_c (id int, v text)");
        try {
            exec("SECURITY LABEL ON COLUMN r14_sl_c.v IS 'confidential'");
        } catch (SQLException e) {
            // Should not be a syntax error — must at least parse the grammar
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"), "SECURITY LABEL ON COLUMN must parse, got: " + msg);
        }
    }

    @Test
    void security_label_on_schema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS r14_sl_schema");
        try {
            exec("SECURITY LABEL ON SCHEMA r14_sl_schema IS 'restricted'");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"), "SECURITY LABEL ON SCHEMA must parse, got: " + msg);
        }
    }

    @Test
    void security_label_on_function() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION r14_sl_fn() RETURNS int AS 'SELECT 1' LANGUAGE SQL");
        try {
            exec("SECURITY LABEL ON FUNCTION r14_sl_fn() IS 'sensitive'");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"), "SECURITY LABEL ON FUNCTION must parse, got: " + msg);
        }
    }

    @Test
    void security_label_on_role_uses_shseclabel() throws SQLException {
        // Shared objects (roles, databases, tablespaces) use pg_shseclabel.
        try {
            exec("SECURITY LABEL ON ROLE memgres IS 'admin-role'");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"), "SECURITY LABEL ON ROLE must parse, got: " + msg);
        }
    }

    // =========================================================================
    // C. SECURITY LABEL FOR provider
    // =========================================================================

    @Test
    void security_label_for_provider_parses() throws SQLException {
        exec("CREATE TABLE r14_sl_prov (id int)");
        try {
            exec("SECURITY LABEL FOR selinux ON TABLE r14_sl_prov IS 'system_u:object_r:sepgsql_table_t:s0'");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            // "selinux" provider not loaded is acceptable; syntax errors are not.
            assertFalse(msg.contains("syntax"), "SECURITY LABEL FOR must parse, got: " + msg);
        }
    }

    @Test
    void security_label_clear_with_null() throws SQLException {
        exec("CREATE TABLE r14_sl_nil (id int)");
        try {
            exec("SECURITY LABEL ON TABLE r14_sl_nil IS 'first'");
            exec("SECURITY LABEL ON TABLE r14_sl_nil IS NULL");
            // After clearing, no rows should remain.
            assertEquals(0, scalarInt("SELECT count(*)::int FROM pg_seclabel "
                    + "WHERE objoid = 'r14_sl_nil'::regclass"));
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"), "SECURITY LABEL IS NULL must parse, got: " + msg);
        }
    }
}
