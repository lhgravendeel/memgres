package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category Q: Listen / Notify / Large objects.
 *
 * Covers:
 *  - pg_listening_channels() returns the channels the backend is listening on
 *  - lo_truncate64
 *  - lo_import / lo_export (filesystem side-effects — Memgres must at least parse)
 *  - GRANT SELECT/UPDATE ON LARGE OBJECT loid (also touched by N2)
 *  - pg_largeobject table is queryable
 */
class Round16ListenLargeObjectTest {

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

    // =========================================================================
    // Q1. pg_listening_channels()
    // =========================================================================

    @Test
    void pg_listening_channels_returns_subscribed_channel() throws SQLException {
        exec("LISTEN r16_ch");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_listening_channels()")) {
            boolean found = false;
            while (rs.next()) {
                if ("r16_ch".equals(rs.getString(1))) {
                    found = true;
                    break;
                }
            }
            assertTrue(found,
                    "pg_listening_channels() must include the LISTEN channel 'r16_ch'");
        } finally {
            exec("UNLISTEN r16_ch");
        }
    }

    // =========================================================================
    // Q2. lo_truncate64
    // =========================================================================

    @Test
    void lo_truncate64_function_exists() throws SQLException {
        // Create a LO and truncate with 64-bit offset signature.
        long loid;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT lo_create(0)")) {
            assertTrue(rs.next());
            loid = rs.getLong(1);
        }
        // lo_truncate64 takes (fd, bigint) — use server-side form via fd from lo_open
        // Simpler: assert the function is registered.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname='lo_truncate64'")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1,
                    "lo_truncate64 must be registered in pg_proc");
        }
        exec("SELECT lo_unlink(" + loid + ")");
    }

    // =========================================================================
    // Q3. lo_import / lo_export exist
    // =========================================================================

    @Test
    void lo_import_function_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname='lo_import'")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1,
                    "lo_import must be registered in pg_proc");
        }
    }

    @Test
    void lo_export_function_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname='lo_export'")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1,
                    "lo_export must be registered in pg_proc");
        }
    }

    // =========================================================================
    // Q4. GRANT ON LARGE OBJECT
    // =========================================================================

    @Test
    void grant_select_on_large_object_parses() throws SQLException {
        exec("DROP ROLE IF EXISTS r16_lolu");
        exec("CREATE ROLE r16_lolu");
        long loid;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT lo_create(0)")) {
            assertTrue(rs.next());
            loid = rs.getLong(1);
        }
        exec("GRANT SELECT ON LARGE OBJECT " + loid + " TO r16_lolu");
        // Must not be a parser error
    }

    // =========================================================================
    // Q5. pg_largeobject table queryable
    // =========================================================================

    @Test
    void pg_largeobject_table_is_queryable() throws SQLException {
        long loid;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT lo_create(0)")) {
            assertTrue(rs.next());
            loid = rs.getLong(1);
        }
        exec("SELECT lo_put(" + loid + ", 0, 'hello'::bytea)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_largeobject WHERE loid=" + loid)) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1,
                    "pg_largeobject must expose at least one row for a populated LO");
        }
        exec("SELECT lo_unlink(" + loid + ")");
    }
}
