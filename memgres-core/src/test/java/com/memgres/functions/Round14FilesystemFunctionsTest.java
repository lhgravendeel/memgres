package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Filesystem / server-side functions.
 *
 * These are heavily used by admin tools (pgAdmin, Patroni, backup scripts).
 *
 * - pg_ls_dir / pg_read_file / pg_read_binary_file
 * - pg_stat_file
 * - pg_ls_logdir / pg_ls_waldir / pg_ls_tmpdir
 * - pg_current_logfile
 * - current_setting / set_config
 */
class Round14FilesystemFunctionsTest {

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

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. pg_ls_dir / pg_stat_file / pg_read_file
    // =========================================================================

    @Test
    void pg_ls_dir_on_base() throws SQLException {
        // Admin-only but should parse and execute. Any count is fine; we care
        // that the function resolves and returns a setof text.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_ls_dir('base')")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 0);
        }
    }

    @Test
    void pg_stat_file_on_current_log() throws SQLException {
        // Returns record type (size, access, modification, change, creation, isdir)
        // Just verify the function resolves
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT (pg_stat_file('postgresql.conf')).size")) {
            assertTrue(rs.next());
            // size is bigint ≥ 0 (or NULL if missing)
        }
    }

    @Test
    void pg_read_file_exists() throws SQLException {
        // Three-arg variant pg_read_file(path, offset, length)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_read_file('postgresql.conf', 0, 0)")) {
            assertTrue(rs.next());
            // Reading 0 bytes should return empty string
            String v = rs.getString(1);
            assertNotNull(v);
        }
    }

    @Test
    void pg_read_binary_file_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_read_binary_file('postgresql.conf', 0, 0)")) {
            assertTrue(rs.next());
        }
    }

    // =========================================================================
    // B. Directory listing helpers
    // =========================================================================

    @Test
    void pg_ls_logdir_queryable() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_ls_logdir()")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 0);
        }
    }

    @Test
    void pg_ls_waldir_queryable() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_ls_waldir()")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 0);
        }
    }

    @Test
    void pg_ls_tmpdir_queryable() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM pg_ls_tmpdir()")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 0);
        }
    }

    @Test
    void pg_ls_archive_statusdir_queryable() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_ls_archive_statusdir()")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 0);
        }
    }

    // =========================================================================
    // C. Current log & settings helpers
    // =========================================================================

    @Test
    void pg_current_logfile_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_current_logfile()")) {
            assertTrue(rs.next());
            // Result may be NULL when logging_collector is off, but function must resolve
        }
    }

    @Test
    void current_setting_with_missing_ok() throws SQLException {
        // 2-arg form: current_setting(name, missing_ok)
        String v = scalarString("SELECT current_setting('nonexistent.guc', true)");
        assertNull(v, "missing_ok=true should return NULL for unknown GUC");
    }

    @Test
    void current_setting_throws_on_missing() throws SQLException {
        SQLException ex = assertThrows(SQLException.class, () ->
                scalarString("SELECT current_setting('nonexistent.guc')"));
        assertNotNull(ex.getMessage());
    }

    @Test
    void set_config_session_local() throws SQLException {
        // 3-arg form: set_config(name, value, is_local)
        assertEquals("hello",
                scalarString("SELECT set_config('custom.foo', 'hello', false)"));
        assertEquals("hello",
                scalarString("SELECT current_setting('custom.foo')"));
    }

    // =========================================================================
    // D. Size / total-relation-size helpers
    // =========================================================================

    @Test
    void pg_database_size_current() throws SQLException {
        assertTrue(scalarInt("SELECT pg_database_size(current_database())::int") >= 0);
    }

    @Test
    void pg_total_relation_size_for_table() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE r14_fs_size (id int)");
        }
        assertTrue(scalarInt("SELECT pg_total_relation_size('r14_fs_size')::int") >= 0);
    }

    @Test
    void pg_relation_size_forks() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE r14_fs_rs (id int)");
        }
        // fork names: 'main', 'fsm', 'vm', 'init'
        assertTrue(scalarInt(
                "SELECT pg_relation_size('r14_fs_rs', 'main')::int") >= 0);
    }

    @Test
    void pg_size_pretty_function() throws SQLException {
        // pg_size_pretty(bigint) returns human-friendly size string
        String v = scalarString("SELECT pg_size_pretty(1024::bigint)");
        assertNotNull(v);
        assertTrue(v.toLowerCase().contains("kb") || v.toLowerCase().contains("1024"),
                "pg_size_pretty(1024) should mention kB; got " + v);
    }

    @Test
    void pg_size_bytes_function() throws SQLException {
        // PG 9.6+ inverse: pg_size_bytes('1 MB') → bigint
        assertEquals(1024 * 1024,
                scalarInt("SELECT pg_size_bytes('1 MB')::int"));
    }

    // =========================================================================
    // E. Version / uptime helpers
    // =========================================================================

    @Test
    void pg_postmaster_start_time_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_postmaster_start_time()")) {
            assertTrue(rs.next());
            assertNotNull(rs.getTimestamp(1));
        }
    }

    @Test
    void pg_conf_load_time_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_conf_load_time()")) {
            assertTrue(rs.next());
            assertNotNull(rs.getTimestamp(1));
        }
    }

    @Test
    void pg_backend_pid_returns_int() throws SQLException {
        assertTrue(scalarInt("SELECT pg_backend_pid()") > 0);
    }
}
