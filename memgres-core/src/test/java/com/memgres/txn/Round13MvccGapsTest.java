package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 13 gaps: transaction, MVCC, and locking features that PG 18 fully
 * supports but Memgres currently lacks or only stubs.
 *
 * Coverage:
 *   A. Isolation level stubs      — READ UNCOMMITTED behavior
 *   B. Predicate locks            — phantom-read prevention in SERIALIZABLE
 *   C. SET TRANSACTION SNAPSHOT   — import snapshot from another txn
 *   D. pg_export_snapshot         — must return real snapshot IDs
 *   E. LOCK TABLE                 — actually blocks and NOWAIT errors (55P03)
 *   F. Advisory locks             — shared (non-exclusive) advisory locks
 *   G. 2PC                        — COMMIT/ROLLBACK PREPARED unknown GID → 42704
 *   H. txid_* family              — within-txn consistency
 *   I. Deferred constraints       — on RELEASE SAVEPOINT
 *   J. VACUUM / ANALYZE           — populate pg_stats, cluster updates
 */
class Round13MvccGapsTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (memgres != null) memgres.close();
    }

    private static Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static String scalarString(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. READ UNCOMMITTED is a stub in PG (mapped to READ COMMITTED). Memgres
    //    must match that and NOT expose uncommitted writes across sessions.
    // =========================================================================

    @Test
    void read_uncommitted_mapsTo_readCommitted_noDirtyReads() throws Exception {
        try (Connection writer = newConn(); Connection reader = newConn()) {
            try (Statement s = writer.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_ru");
                s.execute("CREATE TABLE r13_ru (id int, v text)");
                s.execute("INSERT INTO r13_ru VALUES (1, 'initial')");
            }

            writer.setAutoCommit(false);
            try (Statement s = writer.createStatement()) {
                s.execute("UPDATE r13_ru SET v = 'uncommitted' WHERE id = 1");
            }

            // Reader sets READ UNCOMMITTED — should still see committed value.
            reader.setAutoCommit(false);
            reader.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            try (Statement s = reader.createStatement();
                 ResultSet rs = s.executeQuery("SELECT v FROM r13_ru WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals("initial", rs.getString(1),
                        "READ UNCOMMITTED must NOT expose uncommitted writes in PG semantics");
            }
            reader.rollback();
            writer.rollback();
        }
    }

    // =========================================================================
    // B. Predicate locks / phantom read prevention in SERIALIZABLE
    // =========================================================================

    @Test
    void serializable_preventsPhantomReads() throws Exception {
        try (Connection a = newConn(); Connection b = newConn()) {
            try (Statement s = a.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_phantom");
                s.execute("CREATE TABLE r13_phantom (v int)");
                s.execute("INSERT INTO r13_phantom VALUES (1), (2), (3)");
            }

            a.setAutoCommit(false);
            b.setAutoCommit(false);
            a.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            b.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            try (Statement sa = a.createStatement()) {
                sa.executeQuery("SELECT count(*) FROM r13_phantom WHERE v > 0").close();
            }
            try (Statement sb = b.createStatement()) {
                sb.execute("INSERT INTO r13_phantom VALUES (99)");
                b.commit();
            }

            // PG SSI: A's commit should fail (40001) because B inserted a row
            // that would have been visible to A's predicate.
            SQLException ex = assertThrows(SQLException.class, () -> {
                try (Statement sa = a.createStatement()) {
                    sa.execute("INSERT INTO r13_phantom VALUES (100)");
                }
                a.commit();
            });
            assertEquals("40001", ex.getSQLState(),
                    "expected 40001 serialization_failure; got " + ex.getSQLState());
            a.rollback();
        }
    }

    // =========================================================================
    // C. SET TRANSACTION SNAPSHOT (import exported snapshot)
    // =========================================================================

    @Test
    void set_transaction_snapshot_importsSnapshot() throws Exception {
        try (Connection a = newConn(); Connection b = newConn()) {
            try (Statement s = a.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_snap CASCADE");
                s.execute("CREATE TABLE r13_snap (id int, v text)");
                s.execute("INSERT INTO r13_snap VALUES (1, 'first')");
            }

            a.setAutoCommit(false);
            a.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            String snap;
            try (Statement s = a.createStatement();
                 ResultSet rs = s.executeQuery("SELECT pg_export_snapshot()")) {
                assertTrue(rs.next());
                snap = rs.getString(1);
                assertNotNull(snap);
            }
            try (Statement s = newConn().createStatement()) {
                s.execute("INSERT INTO r13_snap VALUES (2, 'second')");
            }

            b.setAutoCommit(false);
            b.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            try (Statement s = b.createStatement()) {
                s.execute("SET TRANSACTION SNAPSHOT '" + snap + "'");
            }
            try (Statement s = b.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*)::text FROM r13_snap")) {
                assertTrue(rs.next());
                assertEquals("1", rs.getString(1),
                        "B must see A's snapshot (1 row), not the concurrent insert");
            }
            a.rollback();
            b.rollback();
        }
    }

    @Test
    void pg_export_snapshot_returnsSnapshotId() throws Exception {
        try (Connection c = newConn()) {
            c.setAutoCommit(false);
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            String v = scalarString(c, "SELECT pg_export_snapshot()");
            assertNotNull(v);
            // PG snapshot ID: typically "NNNNNNNN-NNN-N-N" (xid-like).
            assertTrue(v.matches("[0-9A-Fa-f-]+"), "unexpected snapshot id shape: " + v);
            c.rollback();
        }
    }

    // =========================================================================
    // D. LOCK TABLE enforcement
    // =========================================================================

    @Test
    void lock_table_nowait_failsWhenContended() throws Exception {
        try (Connection a = newConn(); Connection b = newConn()) {
            try (Statement s = a.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_lock_nowait");
                s.execute("CREATE TABLE r13_lock_nowait (id int)");
            }
            a.setAutoCommit(false);
            try (Statement sa = a.createStatement()) {
                sa.execute("LOCK TABLE r13_lock_nowait IN ACCESS EXCLUSIVE MODE");
            }
            b.setAutoCommit(false);
            SQLException ex = assertThrows(SQLException.class, () -> {
                try (Statement sb = b.createStatement()) {
                    sb.execute("LOCK TABLE r13_lock_nowait IN ACCESS EXCLUSIVE MODE NOWAIT");
                }
            });
            assertEquals("55P03", ex.getSQLState(),
                    "NOWAIT must raise 55P03 lock_not_available; got " + ex.getSQLState());
            a.rollback();
            b.rollback();
        }
    }

    @Test
    void lock_table_blocksConcurrentWriter() throws Exception {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Connection a = newConn()) {
            try (Statement s = a.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_lock_block");
                s.execute("CREATE TABLE r13_lock_block (id int)");
            }
            a.setAutoCommit(false);
            try (Statement sa = a.createStatement()) {
                sa.execute("LOCK TABLE r13_lock_block IN ACCESS EXCLUSIVE MODE");
            }
            Future<Boolean> fut = pool.submit(() -> {
                try (Connection b = newConn()) {
                    b.setAutoCommit(false);
                    try (Statement sb = b.createStatement()) {
                        sb.execute("INSERT INTO r13_lock_block VALUES (1)");
                    }
                    b.commit();
                    return true;
                }
            });
            Thread.sleep(200);
            assertFalse(fut.isDone(), "concurrent INSERT must block on locked table");
            a.rollback();
            assertTrue(fut.get(3, TimeUnit.SECONDS),
                    "once lock released, waiter must complete");
        } finally {
            pool.shutdownNow();
        }
    }

    // =========================================================================
    // E. Advisory locks — shared mode
    // =========================================================================

    @Test
    void shared_advisory_lock_allowsConcurrentSharedHolders() throws Exception {
        try (Connection a = newConn(); Connection b = newConn()) {
            // Both sessions acquire SHARED advisory lock on same ID.
            assertTrue(Boolean.parseBoolean(
                    scalarString(a, "SELECT pg_try_advisory_lock_shared(49127)::text")));
            assertTrue(Boolean.parseBoolean(
                    scalarString(b, "SELECT pg_try_advisory_lock_shared(49127)::text")),
                    "second shared advisory lock must succeed");
            try (Statement s = a.createStatement()) { s.execute("SELECT pg_advisory_unlock_shared(49127)"); }
            try (Statement s = b.createStatement()) { s.execute("SELECT pg_advisory_unlock_shared(49127)"); }
        }
    }

    @Test
    void exclusive_advisory_lock_blocksShared() throws Exception {
        try (Connection a = newConn(); Connection b = newConn()) {
            assertTrue(Boolean.parseBoolean(
                    scalarString(a, "SELECT pg_try_advisory_lock(49128)::text")));
            // Second session: shared lock should FAIL via try-variant.
            assertFalse(Boolean.parseBoolean(
                    scalarString(b, "SELECT pg_try_advisory_lock_shared(49128)::text")),
                    "shared advisory must fail when another session holds exclusive");
            try (Statement s = a.createStatement()) { s.execute("SELECT pg_advisory_unlock(49128)"); }
        }
    }

    // =========================================================================
    // F. 2PC error codes
    // =========================================================================

    @Test
    void commit_prepared_unknownGid_42704() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Connection c = newConn(); Statement s = c.createStatement()) {
                s.execute("COMMIT PREPARED 'does-not-exist-r13'");
            }
        });
        assertEquals("42704", ex.getSQLState(),
                "expected 42704 undefined_object for unknown prepared txn");
    }

    @Test
    void rollback_prepared_unknownGid_42704() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Connection c = newConn(); Statement s = c.createStatement()) {
                s.execute("ROLLBACK PREPARED 'nope-r13'");
            }
        });
        assertEquals("42704", ex.getSQLState());
    }

    // =========================================================================
    // G. txid_* within-txn consistency
    // =========================================================================

    @Test
    void txid_current_consistent_withinTransaction() throws Exception {
        try (Connection c = newConn()) {
            c.setAutoCommit(false);
            String t1 = scalarString(c, "SELECT txid_current()::text");
            String t2 = scalarString(c, "SELECT txid_current()::text");
            assertEquals(t1, t2, "txid_current must be stable within a txn");
            c.rollback();
        }
    }

    @Test
    void txid_current_differsAcrossAutocommitStatements() throws Exception {
        try (Connection c = newConn()) {
            c.setAutoCommit(true);
            String t1 = scalarString(c, "SELECT txid_current()::text");
            String t2 = scalarString(c, "SELECT txid_current()::text");
            assertNotEquals(t1, t2,
                    "in autocommit, each SELECT txid_current() must open a new xid");
        }
    }

    // =========================================================================
    // H. Deferred constraints
    // =========================================================================

    @Test
    void deferred_fk_validated_at_commit() throws Exception {
        try (Connection c = newConn()) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_def_child CASCADE");
                s.execute("DROP TABLE IF EXISTS r13_def_parent CASCADE");
                s.execute("CREATE TABLE r13_def_parent (id int PRIMARY KEY)");
                s.execute("CREATE TABLE r13_def_child (pid int REFERENCES r13_def_parent(id) "
                        + "DEFERRABLE INITIALLY DEFERRED)");
            }
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO r13_def_child VALUES (999)"); // violates but deferred
                s.execute("INSERT INTO r13_def_parent VALUES (999)"); // fixes it
            }
            c.commit(); // should succeed
            assertEquals("1",
                    scalarString(c, "SELECT count(*)::text FROM r13_def_child"));
        }
    }

    @Test
    void deferred_fk_failsOnCommit_whenUnresolved() throws Exception {
        try (Connection c = newConn()) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_def_child2 CASCADE");
                s.execute("DROP TABLE IF EXISTS r13_def_parent2 CASCADE");
                s.execute("CREATE TABLE r13_def_parent2 (id int PRIMARY KEY)");
                s.execute("CREATE TABLE r13_def_child2 (pid int REFERENCES r13_def_parent2(id) "
                        + "DEFERRABLE INITIALLY DEFERRED)");
            }
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO r13_def_child2 VALUES (999)");
            }
            // Commit must throw 23503
            SQLException ex = assertThrows(SQLException.class, c::commit);
            assertEquals("23503", ex.getSQLState(),
                    "expected 23503 foreign_key_violation; got " + ex.getSQLState());
        }
    }

    // =========================================================================
    // I. VACUUM / ANALYZE populate statistics
    // =========================================================================

    @Test
    void analyze_populatesPgStats() throws Exception {
        try (Connection c = newConn()) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_ana");
                s.execute("CREATE TABLE r13_ana (id int, v text)");
                s.execute("INSERT INTO r13_ana SELECT i, 'x'||i FROM generate_series(1, 100) i");
                s.execute("ANALYZE r13_ana");
            }
            // pg_stats row should now exist for id and v columns
            String n = scalarString(c,
                    "SELECT count(*)::text FROM pg_stats WHERE tablename = 'r13_ana'");
            assertTrue(Integer.parseInt(n) > 0,
                    "ANALYZE must populate pg_stats; got " + n + " rows");
        }
    }

    @Test
    void vacuum_updatesRelpagesReltuples() throws Exception {
        try (Connection c = newConn()) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_vac");
                s.execute("CREATE TABLE r13_vac (id int)");
                s.execute("INSERT INTO r13_vac SELECT i FROM generate_series(1, 500) i");
                s.execute("VACUUM ANALYZE r13_vac");
            }
            String n = scalarString(c,
                    "SELECT reltuples::int::text FROM pg_class WHERE relname = 'r13_vac'");
            assertTrue(Integer.parseInt(n) > 0,
                    "VACUUM ANALYZE must update pg_class.reltuples; got " + n);
        }
    }

    @Test
    void cluster_table_preserves_rows() throws Exception {
        try (Connection c = newConn()) {
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r13_cluster");
                s.execute("CREATE TABLE r13_cluster (id int PRIMARY KEY, v text)");
                s.execute("INSERT INTO r13_cluster VALUES (3,'c'),(1,'a'),(2,'b')");
                s.execute("CLUSTER r13_cluster USING r13_cluster_pkey");
            }
            assertEquals("3",
                    scalarString(c, "SELECT count(*)::text FROM r13_cluster"));
        }
    }
}
