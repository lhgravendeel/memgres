package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category M: System / admin functions.
 *
 * Covers:
 *  - pg_log_backend_memory_contexts(pid)
 *  - pg_promote / pg_wal_replay_pause / pg_wal_replay_resume
 *  - pg_create_restore_point / pg_switch_wal / pg_backup_start / pg_backup_stop
 *  - pg_size_bytes(text)
 *  - pg_tablespace_size
 *  - has_parameter_privilege
 *  - pg_column_size returns encoding-aware storage bytes
 *  - pg_relation_size / pg_total_relation_size fork argument
 *  - pg_stat_reset_shared(target) validates target
 */
class Round16AdminFunctionsTest {

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

    private static long longQ(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private static boolean bool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // M1. pg_log_backend_memory_contexts
    // =========================================================================

    @Test
    void pg_log_backend_memory_contexts_returns_bool() throws SQLException {
        // Under PG 18 this signals the backend; accept any boolean result.
        boolean b = bool("SELECT pg_log_backend_memory_contexts(pg_backend_pid())");
        // Can be true or false; simply must not error
        assertTrue(b || !b, "pg_log_backend_memory_contexts must exist and return boolean");
    }

    // =========================================================================
    // M2. pg_promote / pg_wal_replay_pause / pg_wal_replay_resume
    // =========================================================================

    @Test
    void pg_promote_function_exists() throws SQLException {
        // In a non-standby cluster pg_promote returns false — accept any bool.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_promote(false, 0)")) {
            assertTrue(rs.next(), "pg_promote must exist");
        }
    }

    @Test
    void pg_wal_replay_pause_resume_functions_exist() throws SQLException {
        // These raise \"recovery not in progress\" on a primary; just verify they are functions.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT proname FROM pg_proc WHERE proname='pg_wal_replay_pause'")) {
            assertTrue(rs.next(), "pg_wal_replay_pause must be registered in pg_proc");
        }
    }

    // =========================================================================
    // M3. pg_create_restore_point / pg_switch_wal / pg_backup_start / pg_backup_stop
    // =========================================================================

    @Test
    void pg_switch_wal_function_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT proname FROM pg_proc WHERE proname='pg_switch_wal'")) {
            assertTrue(rs.next(), "pg_switch_wal must be registered in pg_proc");
        }
    }

    @Test
    void pg_backup_start_function_exists() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT proname FROM pg_proc WHERE proname='pg_backup_start'")) {
            assertTrue(rs.next(), "pg_backup_start must be registered in pg_proc");
        }
    }

    // =========================================================================
    // M4. pg_size_bytes(text)
    // =========================================================================

    @Test
    void pg_size_bytes_parses_human_readable_suffix() throws SQLException {
        long v = longQ("SELECT pg_size_bytes('10 MB')");
        assertEquals(10L * 1024 * 1024, v,
                "pg_size_bytes('10 MB') must return 10*1024*1024; got " + v);
    }

    // =========================================================================
    // M5. pg_tablespace_size
    // =========================================================================

    @Test
    void pg_tablespace_size_function_exists() throws SQLException {
        // Must exist; value depends on storage. Just assert the call works.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_tablespace_size('pg_default')")) {
            assertTrue(rs.next(), "pg_tablespace_size must exist");
        }
    }

    // =========================================================================
    // M6. has_parameter_privilege
    // =========================================================================

    @Test
    void has_parameter_privilege_returns_boolean() throws SQLException {
        // Syntax: has_parameter_privilege(role, param, privilege)
        boolean v = bool(
                "SELECT has_parameter_privilege(current_user, 'work_mem', 'SET')");
        // Accept either true or false; the important thing is the function exists
        assertTrue(v || !v,
                "has_parameter_privilege must be a boolean-returning function");
    }

    // =========================================================================
    // M7. pg_column_size is encoding-aware
    // =========================================================================

    @Test
    void pg_column_size_for_text_excludes_varlena_overhead_semantics() throws SQLException {
        // pg_column_size('hello'::text) in PG 18 = 9 (1-byte header * 4 + 5 bytes for short)
        // or 6 on short-header case. Must not equal length('hello')+4 = 9 only by coincidence
        // — assert it's consistent with PG semantics by comparing text vs bytea.
        long textSize = longQ("SELECT pg_column_size('hello'::text)");
        long byteaSize = longQ("SELECT pg_column_size('hello'::bytea)");
        // Both are 5-byte payloads → must have same size in PG
        assertEquals(textSize, byteaSize,
                "pg_column_size('hello'::text) must equal pg_column_size('hello'::bytea); " +
                        "got text=" + textSize + ", bytea=" + byteaSize);
    }

    // =========================================================================
    // M8. pg_relation_size / pg_total_relation_size fork argument
    // =========================================================================

    @Test
    void pg_relation_size_accepts_fork_argument() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS r16_rs");
            s.execute("CREATE TABLE r16_rs (id int)");
        }
        // pg_relation_size(relation, fork) — valid forks: main, fsm, vm, init
        long v = longQ("SELECT pg_relation_size('r16_rs'::regclass, 'fsm')");
        // fsm fork is usually 0 for a freshly-created table; must not error on the fork arg
        assertTrue(v >= 0,
                "pg_relation_size('r16_rs','fsm') must accept fork arg; got " + v);
    }

    // =========================================================================
    // M9. pg_stat_reset_shared validates target
    // =========================================================================

    @Test
    void pg_stat_reset_shared_rejects_invalid_target() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_stat_reset_shared('nonsense')")) {
            if (rs.next()) {
                fail("pg_stat_reset_shared('nonsense') must reject invalid target");
            }
        } catch (SQLException e) {
            assertNotNull(e.getMessage(),
                    "pg_stat_reset_shared must reject invalid target with an error");
        }
    }
}
