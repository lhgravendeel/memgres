package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category F: LOCK TABLE + advisory locks surface.
 *
 * Covers:
 *  - LOCK TABLE ONLY t
 *  - LOCK TABLE t1, t2, t3
 *  - LOCK TABLE ... NOWAIT
 *  - LOCK TABLE ... IN {ACCESS SHARE, ROW SHARE, ROW EXCLUSIVE,
 *                       SHARE UPDATE EXCLUSIVE, SHARE, SHARE ROW EXCLUSIVE,
 *                       EXCLUSIVE, ACCESS EXCLUSIVE} MODE
 *  - Advisory locks appear in pg_locks
 */
class Round15LockTableTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(false);  // LOCK requires transaction
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try { conn.rollback(); } catch (SQLException ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // A. Basic LOCK TABLE
    // =========================================================================

    @Test
    void lock_table_default_mode() throws SQLException {
        exec("CREATE TABLE r15_lt_def (id int)");
        conn.commit();
        exec("LOCK TABLE r15_lt_def");
        conn.rollback();
    }

    @Test
    void lock_table_in_all_modes() throws SQLException {
        exec("CREATE TABLE r15_lt_modes (id int)");
        conn.commit();
        String[] modes = {
                "ACCESS SHARE",
                "ROW SHARE",
                "ROW EXCLUSIVE",
                "SHARE UPDATE EXCLUSIVE",
                "SHARE",
                "SHARE ROW EXCLUSIVE",
                "EXCLUSIVE",
                "ACCESS EXCLUSIVE"
        };
        for (String m : modes) {
            exec("LOCK TABLE r15_lt_modes IN " + m + " MODE");
            conn.rollback();
        }
    }

    // =========================================================================
    // B. LOCK TABLE ONLY
    // =========================================================================

    @Test
    void lock_table_only_accepted() throws SQLException {
        exec("CREATE TABLE r15_lt_only_p (a int)");
        exec("CREATE TABLE r15_lt_only_c () INHERITS (r15_lt_only_p)");
        conn.commit();
        exec("LOCK TABLE ONLY r15_lt_only_p");
        conn.rollback();
    }

    // =========================================================================
    // C. LOCK TABLE multi-table
    // =========================================================================

    @Test
    void lock_table_multi_table() throws SQLException {
        exec("CREATE TABLE r15_lt_m1 (id int)");
        exec("CREATE TABLE r15_lt_m2 (id int)");
        exec("CREATE TABLE r15_lt_m3 (id int)");
        conn.commit();
        exec("LOCK TABLE r15_lt_m1, r15_lt_m2, r15_lt_m3 IN ACCESS EXCLUSIVE MODE");
        conn.rollback();
    }

    // =========================================================================
    // D. LOCK TABLE NOWAIT
    // =========================================================================

    @Test
    void lock_table_nowait_accepted() throws SQLException {
        exec("CREATE TABLE r15_lt_nw (id int)");
        conn.commit();
        // In-memory engine with no contention: NOWAIT should succeed
        exec("LOCK TABLE r15_lt_nw IN ACCESS EXCLUSIVE MODE NOWAIT");
        conn.rollback();
    }

    // =========================================================================
    // E. pg_locks view for LOCK TABLE
    // =========================================================================

    @Test
    void pg_locks_shows_table_lock() throws SQLException {
        exec("CREATE TABLE r15_lt_pl (id int)");
        conn.commit();
        exec("LOCK TABLE r15_lt_pl IN ACCESS EXCLUSIVE MODE");
        int n = scalarInt(
                "SELECT count(*)::int FROM pg_locks l "
                        + "JOIN pg_class c ON l.relation = c.oid "
                        + "WHERE c.relname='r15_lt_pl' AND l.mode='AccessExclusiveLock'");
        assertTrue(n >= 1,
                "pg_locks should list AccessExclusiveLock for LOCK TABLE IN ACCESS EXCLUSIVE");
        conn.rollback();
    }

    // =========================================================================
    // F. Advisory locks — pg_locks rows for locktype='advisory'
    // =========================================================================

    @Test
    void pg_advisory_lock_appears_in_pg_locks() throws SQLException {
        conn.rollback();  // clear any prior state
        conn.setAutoCommit(true);
        try {
            try (Statement s = conn.createStatement()) {
                s.executeQuery("SELECT pg_advisory_lock(123456)").close();
            }
            int n = scalarInt(
                    "SELECT count(*)::int FROM pg_locks "
                            + "WHERE locktype='advisory' AND objid=123456");
            assertTrue(n >= 1,
                    "pg_advisory_lock(123456) should appear in pg_locks with locktype='advisory'");
        } finally {
            try (Statement s = conn.createStatement()) {
                s.executeQuery("SELECT pg_advisory_unlock(123456)").close();
            }
            conn.setAutoCommit(false);
        }
    }

    @Test
    void pg_advisory_xact_lock_released_at_commit() throws SQLException {
        conn.rollback();
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT pg_advisory_xact_lock(999001)").close();
        }
        int beforeCommit = scalarInt(
                "SELECT count(*)::int FROM pg_locks "
                        + "WHERE locktype='advisory' AND objid=999001");
        assertTrue(beforeCommit >= 1,
                "pg_advisory_xact_lock must register in pg_locks");
        conn.commit();
        int afterCommit = scalarInt(
                "SELECT count(*)::int FROM pg_locks "
                        + "WHERE locktype='advisory' AND objid=999001");
        assertEquals(0, afterCommit,
                "pg_advisory_xact_lock must be released at commit");
    }

    @Test
    void pg_try_advisory_lock_returns_bool() throws SQLException {
        conn.rollback();
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_try_advisory_lock(555777)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1), "pg_try_advisory_lock should return true when free");
        } finally {
            try (Statement s = conn.createStatement()) {
                s.executeQuery("SELECT pg_advisory_unlock(555777)").close();
            }
            conn.setAutoCommit(false);
        }
    }

    // =========================================================================
    // G. pg_locks view shape
    // =========================================================================

    @Test
    void pg_locks_columns() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT locktype, database, relation, page, tuple, virtualxid,"
                             + " transactionid, classid, objid, objsubid, virtualtransaction,"
                             + " pid, mode, granted, fastpath, waitstart"
                             + " FROM pg_locks LIMIT 1")) {
            rs.getMetaData();  // shape only
        }
    }
}
