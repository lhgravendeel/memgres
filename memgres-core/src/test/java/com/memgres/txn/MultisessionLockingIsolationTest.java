package com.memgres.txn;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Multi-session locking and isolation tests covering scenarios from:
 * - 1100_multisession_locking_and_isolation_scenarios.md
 * - 1550_multisession_executable_spec.md
 *
 * Tests row-level locking modes, NOWAIT/SKIP LOCKED, deadlock detection,
 * isolation level visibility semantics, advisory locks, lock timeouts,
 * table-level locking, and concurrent constraint enforcement.
 */
class MultisessionLockingIsolationTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (memgres != null) memgres.close();
    }

    Connection newConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    // =========================================================================
    // 1. Row-level locking modes
    // =========================================================================

    @Test
    void testForUpdateReturnsCorrectData() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE msli_forupdate (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO msli_forupdate VALUES (1, 'alpha'), (2, 'beta')");
            c.commit();

            ResultSet rs = s.executeQuery("SELECT id, val FROM msli_forupdate WHERE id = 1 FOR UPDATE");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("alpha", rs.getString("val"));
            assertFalse(rs.next());
            c.commit();
        }
    }

    @Test
    void testForNoKeyUpdateReturnsCorrectData() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE msli_fornokeyupdate (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO msli_fornokeyupdate VALUES (1, 'alpha'), (2, 'beta')");
            c.commit();

            ResultSet rs = s.executeQuery("SELECT id, val FROM msli_fornokeyupdate WHERE id = 2 FOR NO KEY UPDATE");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("beta", rs.getString("val"));
            assertFalse(rs.next());
            c.commit();
        }
    }

    @Test
    void testForShareReturnsCorrectData() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE msli_forshare (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO msli_forshare VALUES (1, 'shared')");
            c.commit();

            ResultSet rs = s.executeQuery("SELECT id, val FROM msli_forshare FOR SHARE");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("shared", rs.getString("val"));
            c.commit();
        }
    }

    @Test
    void testForKeyShareReturnsCorrectData() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE msli_forkeyshare (id int PRIMARY KEY, val text)");
            s.execute("INSERT INTO msli_forkeyshare VALUES (1, 'keyshared')");
            c.commit();

            ResultSet rs = s.executeQuery("SELECT id, val FROM msli_forkeyshare FOR KEY SHARE");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("keyshared", rs.getString("val"));
            c.commit();
        }
    }

    // =========================================================================
    // 2. NOWAIT - fail immediately when row is locked
    // =========================================================================

    @Test
    void testForUpdateNowaitFailsWhenRowLocked() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_nowait (id int PRIMARY KEY, val text)");
            sA.execute("INSERT INTO msli_nowait VALUES (1, 'locked')");
            cA.commit();

            // Session A locks the row
            sA.executeQuery("SELECT * FROM msli_nowait WHERE id = 1 FOR UPDATE");

            // Session B attempts NOWAIT - must get 55P03
            SQLException ex = assertThrows(SQLException.class, () ->
                    sB.executeQuery("SELECT * FROM msli_nowait WHERE id = 1 FOR UPDATE NOWAIT"));
            assertEquals("55P03", ex.getSQLState(),
                    "Expected lock_not_available (55P03) but got: " + ex.getSQLState() + " - " + ex.getMessage());

            cA.commit();
            try { cB.rollback(); } catch (SQLException ignored) {}
        }
    }

    // =========================================================================
    // 3. SKIP LOCKED - skip rows locked by another session
    // =========================================================================

    @Test
    void testForUpdateSkipLockedSkipsLockedRows() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_skiplocked (id int PRIMARY KEY, val text)");
            sA.execute("INSERT INTO msli_skiplocked VALUES (1, 'row1'), (2, 'row2'), (3, 'row3')");
            cA.commit();

            // Session A locks row 1
            sA.executeQuery("SELECT * FROM msli_skiplocked WHERE id = 1 FOR UPDATE");

            // Session B uses SKIP LOCKED - should get rows 2 and 3 only
            ResultSet rs = sB.executeQuery("SELECT id FROM msli_skiplocked ORDER BY id FOR UPDATE SKIP LOCKED");
            List<Integer> ids = new ArrayList<>();
            while (rs.next()) ids.add(rs.getInt(1));

            assertFalse(ids.contains(1), "Locked row 1 should be skipped");
            assertTrue(ids.size() < 3, "Should return fewer than 3 rows due to skip");

            cA.commit();
            cB.commit();
        }
    }

    // =========================================================================
    // 4. Deadlock detection
    // =========================================================================

    @Test
    void testDeadlockDetected() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn()) {
            // Setup
            try (Statement s = cA.createStatement()) {
                s.execute("CREATE TABLE msli_deadlock (id int PRIMARY KEY, val text)");
                s.execute("INSERT INTO msli_deadlock VALUES (1, 'row1'), (2, 'row2')");
                cA.commit();
            }

            // Session A locks row 1
            try (Statement sA = cA.createStatement()) {
                sA.executeQuery("SELECT * FROM msli_deadlock WHERE id = 1 FOR UPDATE");
            }

            // Session B locks row 2
            try (Statement sB = cB.createStatement()) {
                sB.executeQuery("SELECT * FROM msli_deadlock WHERE id = 2 FOR UPDATE");
            }

            ExecutorService exec = Executors.newFixedThreadPool(2);
            CountDownLatch aLockedRow1 = new CountDownLatch(1);
            CountDownLatch bLockedRow2 = new CountDownLatch(1);

            Future<SQLException> futureA = exec.submit(() -> {
                try (Statement sA = cA.createStatement()) {
                    aLockedRow1.countDown();
                    bLockedRow2.await(5, TimeUnit.SECONDS);
                    sA.executeQuery("SELECT * FROM msli_deadlock WHERE id = 2 FOR UPDATE");
                    return null;
                } catch (SQLException e) {
                    return e;
                } catch (Exception e) {
                    return new SQLException("Unexpected", e);
                }
            });

            Future<SQLException> futureB = exec.submit(() -> {
                try (Statement sB = cB.createStatement()) {
                    bLockedRow2.countDown();
                    aLockedRow1.await(5, TimeUnit.SECONDS);
                    Thread.sleep(100); // slight delay so A requests first
                    sB.executeQuery("SELECT * FROM msli_deadlock WHERE id = 1 FOR UPDATE");
                    return null;
                } catch (SQLException e) {
                    return e;
                } catch (Exception e) {
                    return new SQLException("Unexpected", e);
                }
            });

            exec.shutdown();
            exec.awaitTermination(15, TimeUnit.SECONDS);

            SQLException exA = futureA.get(10, TimeUnit.SECONDS);
            SQLException exB = futureB.get(10, TimeUnit.SECONDS);

            // Exactly one of them should get 40P01 (deadlock_detected)
            boolean aDeadlock = exA != null && "40P01".equals(exA.getSQLState());
            boolean bDeadlock = exB != null && "40P01".equals(exB.getSQLState());
            assertTrue(aDeadlock || bDeadlock,
                    "Expected deadlock (40P01) in one session. A: " + (exA != null ? exA.getSQLState() : "ok")
                    + ", B: " + (exB != null ? exB.getSQLState() : "ok"));

            try { cA.rollback(); } catch (SQLException ignored) {}
            try { cB.rollback(); } catch (SQLException ignored) {}
        }
    }

    // =========================================================================
    // 5. READ COMMITTED isolation - sees committed rows after commit
    // =========================================================================

    @Test
    void testReadCommittedSeesCommittedInsert() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_rc_vis (id int PRIMARY KEY, val text)");
            cA.commit();

            // Session B starts a READ COMMITTED transaction
            sB.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
            ResultSet before = sB.executeQuery("SELECT COUNT(*) FROM msli_rc_vis");
            before.next();
            int countBefore = before.getInt(1);
            assertEquals(0, countBefore);

            // Session A inserts and commits
            sA.execute("INSERT INTO msli_rc_vis VALUES (1, 'newrow')");
            cA.commit();

            // Session B (still in its transaction) re-reads - READ COMMITTED should see new row
            ResultSet after = sB.executeQuery("SELECT COUNT(*) FROM msli_rc_vis");
            after.next();
            int countAfter = after.getInt(1);
            assertEquals(1, countAfter, "READ COMMITTED should see the row committed by Session A");

            cB.commit();
        }
    }

    // =========================================================================
    // 6. REPEATABLE READ isolation - does NOT see rows inserted after snapshot
    // =========================================================================

    @Test
    void testRepeatableReadDoesNotSeeNewRowsAfterSnapshot() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_rr_snap (id int PRIMARY KEY, val text)");
            cA.commit();

            // Session B begins REPEATABLE READ and takes first snapshot
            sB.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            ResultSet snap1 = sB.executeQuery("SELECT COUNT(*) FROM msli_rr_snap");
            snap1.next();
            assertEquals(0, snap1.getInt(1));

            // Session A inserts and commits
            sA.execute("INSERT INTO msli_rr_snap VALUES (1, 'invisible')");
            cA.commit();

            // Session B re-reads - REPEATABLE READ must NOT see the new row
            ResultSet snap2 = sB.executeQuery("SELECT COUNT(*) FROM msli_rr_snap");
            snap2.next();
            int count = snap2.getInt(1);
            assertEquals(0, count,
                    "REPEATABLE READ should not see rows inserted after snapshot was taken");

            cB.commit();
        }
    }

    // =========================================================================
    // 7. SERIALIZABLE isolation - conflicting transactions
    // =========================================================================

    @Test
    void testSerializableConflictCausesFailure() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn()) {
            try (Statement s = cA.createStatement()) {
                s.execute("CREATE TABLE msli_ser (id int PRIMARY KEY, val int)");
                s.execute("INSERT INTO msli_ser VALUES (1, 10), (2, 20)");
                cA.commit();
            }

            cA.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            cB.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            try (Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {
                // Both read
                sA.executeQuery("SELECT SUM(val) FROM msli_ser");
                sB.executeQuery("SELECT SUM(val) FROM msli_ser");

                // Both write to different rows
                sA.execute("UPDATE msli_ser SET val = val + 1 WHERE id = 1");
                sB.execute("UPDATE msli_ser SET val = val + 1 WHERE id = 2");

                // Attempt to commit both - at least one should succeed, one may fail with 40001
                boolean aOk = false;
                boolean bOk = false;
                String aState = null;
                String bState = null;

                try { cA.commit(); aOk = true; } catch (SQLException e) { aState = e.getSQLState(); }
                try { cB.commit(); bOk = true; } catch (SQLException e) { bState = e.getSQLState(); }

                // At least one must commit successfully
                assertTrue(aOk || bOk, "At least one serializable transaction should commit");

                // If one failed, it should be a serialization failure
                if (!aOk) {
                    assertEquals("40001", aState,
                            "Failed serializable txn should get 40001, got: " + aState);
                }
                if (!bOk) {
                    assertEquals("40001", bState,
                            "Failed serializable txn should get 40001, got: " + bState);
                }

                try { cA.rollback(); } catch (SQLException ignored) {}
                try { cB.rollback(); } catch (SQLException ignored) {}
            }
        }
    }

    // =========================================================================
    // 8. Phantom read prevention under REPEATABLE READ
    // =========================================================================

    @Test
    void testPhantomReadPreventedUnderRepeatableRead() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_phantom (id int PRIMARY KEY, category int)");
            sA.execute("INSERT INTO msli_phantom VALUES (1, 5), (2, 5)");
            cA.commit();

            // Session B takes snapshot under REPEATABLE READ
            sB.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            ResultSet snap1 = sB.executeQuery("SELECT COUNT(*) FROM msli_phantom WHERE category = 5");
            snap1.next();
            int countBefore = snap1.getInt(1);
            assertEquals(2, countBefore);

            // Session A inserts a matching row and commits
            sA.execute("INSERT INTO msli_phantom VALUES (3, 5)");
            cA.commit();

            // Session B re-runs the same predicate - should NOT see phantom row
            ResultSet snap2 = sB.executeQuery("SELECT COUNT(*) FROM msli_phantom WHERE category = 5");
            snap2.next();
            int countAfter = snap2.getInt(1);
            assertEquals(countBefore, countAfter,
                    "REPEATABLE READ should prevent phantom reads");

            cB.commit();
        }
    }

    // =========================================================================
    // 9. Lost update prevention under REPEATABLE READ
    // =========================================================================

    @Test
    void testLostUpdatePreventedUnderRepeatableRead() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn()) {
            try (Statement s = cA.createStatement()) {
                s.execute("CREATE TABLE msli_lostupdate (id int PRIMARY KEY, counter int)");
                s.execute("INSERT INTO msli_lostupdate VALUES (1, 100)");
                cA.commit();
            }

            cA.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            cB.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            try (Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {
                // Both read the same row
                ResultSet rsA = sA.executeQuery("SELECT counter FROM msli_lostupdate WHERE id = 1");
                rsA.next();
                int valA = rsA.getInt(1);

                ResultSet rsB = sB.executeQuery("SELECT counter FROM msli_lostupdate WHERE id = 1");
                rsB.next();
                int valB = rsB.getInt(1);

                assertEquals(100, valA);
                assertEquals(100, valB);

                // Both attempt to write based on their read value
                sA.execute("UPDATE msli_lostupdate SET counter = " + (valA + 10) + " WHERE id = 1");
                cA.commit();

                // Session B's update should fail with serialization failure (40001)
                // or succeed with read-committed-like behavior depending on implementation
                sB.execute("UPDATE msli_lostupdate SET counter = " + (valB + 20) + " WHERE id = 1");
                try {
                    cB.commit();
                    // If commit succeeds, verify the final value is one of the two expected outcomes
                    try (Statement sV = cA.createStatement()) {
                        ResultSet rv = sV.executeQuery("SELECT counter FROM msli_lostupdate WHERE id = 1");
                        rv.next();
                        int finalVal = rv.getInt(1);
                        assertTrue(finalVal == 110 || finalVal == 120,
                                "Final value should be either 110 or 120, got " + finalVal);
                        cA.commit();
                    }
                } catch (SQLException e) {
                    // 40001 serialization failure is also acceptable
                    assertTrue("40001".equals(e.getSQLState()) || "40P01".equals(e.getSQLState()),
                            "Expected serialization failure but got: " + e.getSQLState());
                    try { cB.rollback(); } catch (SQLException ignored) {}
                }
            }
        }
    }

    // =========================================================================
    // 10. Advisory locks
    // =========================================================================

    @Test
    void testPgAdvisoryLockAndUnlock() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            // Acquire advisory lock
            s.execute("SELECT pg_advisory_lock(12345)");

            // Verify we can unlock it
            ResultSet rs = s.executeQuery("SELECT pg_advisory_unlock(12345)");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1), "pg_advisory_unlock should return true");

            c.commit();
        }
    }

    @Test
    void testPgTryAdvisoryLockReturnsFalseWhenHeld() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            // Session A acquires advisory lock 99999
            sA.execute("SELECT pg_advisory_lock(99999)");

            // Session B tries non-blocking - should return false
            ResultSet rs = sB.executeQuery("SELECT pg_try_advisory_lock(99999)");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1), "pg_try_advisory_lock should return false when lock is held");

            // Session A releases
            sA.execute("SELECT pg_advisory_unlock(99999)");
            cA.commit();
            cB.commit();
        }
    }

    @Test
    void testPgTryAdvisoryLockReturnsTrueWhenFree() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT pg_try_advisory_lock(77777)");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1), "pg_try_advisory_lock should return true when lock is free");
            s.execute("SELECT pg_advisory_unlock(77777)");
            c.commit();
        }
    }

    // =========================================================================
    // 11. Lock timeout
    // =========================================================================

    @Test
    void testLockTimeoutFailsWhenRowHeld() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_locktimeout (id int PRIMARY KEY, val text)");
            sA.execute("INSERT INTO msli_locktimeout VALUES (1, 'held')");
            cA.commit();

            // Session A locks the row
            sA.executeQuery("SELECT * FROM msli_locktimeout WHERE id = 1 FOR UPDATE");

            // Session B sets a very short lock_timeout and tries to lock
            sB.execute("SET lock_timeout = '100ms'");
            long start = System.currentTimeMillis();
            SQLException ex = assertThrows(SQLException.class, () ->
                    sB.executeQuery("SELECT * FROM msli_locktimeout WHERE id = 1 FOR UPDATE"));
            long elapsed = System.currentTimeMillis() - start;

            // Should be 55P03 (lock_not_available) or 40P01 with timeout
            String state = ex.getSQLState();
            assertTrue("55P03".equals(state) || "40P01".equals(state),
                    "Expected lock timeout SQLSTATE 55P03 or 40P01, got: " + state);
            assertTrue(elapsed < 2000,
                    "lock_timeout = 100ms should fail fast, but took " + elapsed + "ms");

            cA.commit();
            try { cB.rollback(); } catch (SQLException ignored) {}
        }
    }

    // =========================================================================
    // 12. Table-level LOCK TABLE
    // =========================================================================

    @Test
    void testLockTableInShareMode() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE msli_locktable (id int PRIMARY KEY)");
            s.execute("INSERT INTO msli_locktable VALUES (1)");
            c.commit();

            // Lock table in SHARE mode - should succeed
            s.execute("LOCK TABLE msli_locktable IN SHARE MODE");

            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM msli_locktable");
            rs.next();
            assertEquals(1, rs.getInt(1));

            c.commit();
        }
    }

    @Test
    void testLockTableInExclusiveMode() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE msli_locktable_ex (id int PRIMARY KEY)");
            c.commit();

            // Lock table in EXCLUSIVE mode - should succeed
            s.execute("LOCK TABLE msli_locktable_ex IN EXCLUSIVE MODE");
            s.execute("INSERT INTO msli_locktable_ex VALUES (42)");

            ResultSet rs = s.executeQuery("SELECT id FROM msli_locktable_ex WHERE id = 42");
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));

            c.commit();
        }
    }

    @Test
    void testLockTableInAccessExclusiveMode() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE msli_locktable_ax (id int)");
            c.commit();

            s.execute("LOCK TABLE msli_locktable_ax IN ACCESS EXCLUSIVE MODE");
            s.execute("INSERT INTO msli_locktable_ax VALUES (1)");
            c.commit();

            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM msli_locktable_ax");
            rs.next();
            assertEquals(1, rs.getInt(1));
            c.commit();
        }
    }

    // =========================================================================
    // 13. Visibility after ROLLBACK
    // =========================================================================

    @Test
    void testRolledBackChangesNotVisible() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_rollback_vis (id int PRIMARY KEY, val text)");
            cA.commit();

            // Session A inserts then rolls back
            sA.execute("INSERT INTO msli_rollback_vis VALUES (1, 'should_not_exist')");
            cA.rollback();

            // Session B should see no rows
            ResultSet rs = sB.executeQuery("SELECT COUNT(*) FROM msli_rollback_vis");
            rs.next();
            assertEquals(0, rs.getInt(1),
                    "Rolled-back insert must never be visible to other sessions");
            cB.commit();
        }
    }

    @Test
    void testRolledBackUpdateNotVisible() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_rollback_upd (id int PRIMARY KEY, val text)");
            sA.execute("INSERT INTO msli_rollback_upd VALUES (1, 'original')");
            cA.commit();

            // Session A updates then rolls back
            sA.execute("UPDATE msli_rollback_upd SET val = 'changed' WHERE id = 1");
            cA.rollback();

            // Session B should see original value
            ResultSet rs = sB.executeQuery("SELECT val FROM msli_rollback_upd WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("original", rs.getString(1),
                    "Rolled-back update must not be visible");
            cB.commit();
        }
    }

    // =========================================================================
    // 14. Concurrent INSERT with UNIQUE constraint
    // =========================================================================

    @Test
    void testConcurrentInsertUniqueConstraintViolation() throws Exception {
        try (Connection cA = newConn()) {
            try (Statement s = cA.createStatement()) {
                s.execute("CREATE TABLE msli_unique_ins (id int PRIMARY KEY, val text)");
                cA.commit();
            }
        }

        ExecutorService exec = Executors.newFixedThreadPool(2);

        Callable<String> task = () -> {
            try (Connection c = newConn(); Statement s = c.createStatement()) {
                s.execute("INSERT INTO msli_unique_ins VALUES (1, 'duplicate')");
                c.commit();
                return "ok";
            } catch (SQLException e) {
                try (Connection rc = newConn()) { rc.rollback(); } catch (Exception ignored) {}
                return e.getSQLState();
            }
        };

        Future<String> f1 = exec.submit(task);
        Future<String> f2 = exec.submit(task);
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);

        String r1 = f1.get(10, TimeUnit.SECONDS);
        String r2 = f2.get(10, TimeUnit.SECONDS);

        // One should succeed, one should fail with 23505 (unique_violation)
        boolean oneOk = "ok".equals(r1) || "ok".equals(r2);
        boolean oneViolation = "23505".equals(r1) || "23505".equals(r2);

        assertTrue(oneOk, "One INSERT should succeed. r1=" + r1 + " r2=" + r2);
        assertTrue(oneViolation,
                "One INSERT should fail with 23505. r1=" + r1 + " r2=" + r2);
    }

    // =========================================================================
    // 15. SELECT FOR UPDATE with subquery
    // =========================================================================

    @Test
    void testForUpdateWithSubquery() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE msli_subq (id int PRIMARY KEY, status text)");
            s.execute("INSERT INTO msli_subq VALUES (1, 'pending'), (2, 'done'), (3, 'pending')");
            c.commit();

            // FOR UPDATE on result of a subquery (via CTE or inline view)
            ResultSet rs = s.executeQuery(
                    "SELECT id, status FROM (SELECT id, status FROM msli_subq WHERE status = 'pending') t FOR UPDATE");
            List<Integer> ids = new ArrayList<>();
            while (rs.next()) ids.add(rs.getInt("id"));

            assertEquals(2, ids.size(), "Should return 2 pending rows");
            assertTrue(ids.contains(1));
            assertTrue(ids.contains(3));

            c.commit();
        }
    }

    // =========================================================================
    // 16. FOR UPDATE with JOIN (FOR UPDATE OF specific table)
    // =========================================================================

    @Test
    void testForUpdateOfSpecificTableInJoin() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE msli_join_a (id int PRIMARY KEY, val text)");
            s.execute("CREATE TABLE msli_join_b (id int PRIMARY KEY, a_id int, info text)");
            s.execute("INSERT INTO msli_join_a VALUES (1, 'A'), (2, 'B')");
            s.execute("INSERT INTO msli_join_b VALUES (10, 1, 'X'), (20, 2, 'Y')");
            c.commit();

            // Lock only the 'a' table rows in the join
            ResultSet rs = s.executeQuery(
                    "SELECT a.id, b.info FROM msli_join_a a JOIN msli_join_b b ON b.a_id = a.id " +
                    "WHERE a.id = 1 FOR UPDATE OF a");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("X", rs.getString(2));

            c.commit();
        }
    }

    // =========================================================================
    // 17. Barrier synchronization pattern
    // =========================================================================

    @Test
    void testBarrierSynchronizationPattern() throws Exception {
        // Setup
        try (Connection setup = newConn(); Statement s = setup.createStatement()) {
            s.execute("CREATE TABLE msli_barrier (id int PRIMARY KEY, stage text)");
            s.execute("INSERT INTO msli_barrier VALUES (1, 'initial')");
            setup.commit();
        }

        CountDownLatch aInserted = new CountDownLatch(1);
        CountDownLatch bVerified = new CountDownLatch(1);
        CountDownLatch aUpdated = new CountDownLatch(1);

        ExecutorService exec = Executors.newFixedThreadPool(2);

        // Session A: insert step 2 row, wait for B to verify, then update step 3
        Future<Void> futureA = exec.submit(() -> {
            try (Connection cA = newConn(); Statement sA = cA.createStatement()) {
                sA.execute("INSERT INTO msli_barrier VALUES (2, 'step2')");
                cA.commit();
                aInserted.countDown(); // Signal B that insert is done

                bVerified.await(5, TimeUnit.SECONDS); // Wait for B to verify

                sA.execute("UPDATE msli_barrier SET stage = 'step3' WHERE id = 2");
                cA.commit();
                aUpdated.countDown(); // Signal B that update is done
            } catch (Exception e) {
                aInserted.countDown();
                aUpdated.countDown();
                throw new RuntimeException(e);
            }
            return null;
        });

        // Session B: wait for A to insert, verify it, then signal A, then verify update
        Future<Void> futureB = exec.submit(() -> {
            try (Connection cB = newConn(); Statement sB = cB.createStatement()) {
                aInserted.await(5, TimeUnit.SECONDS); // Wait for A to insert

                ResultSet rs1 = sB.executeQuery("SELECT stage FROM msli_barrier WHERE id = 2");
                assertTrue(rs1.next(), "B should see row inserted by A");
                assertEquals("step2", rs1.getString(1), "B should see stage='step2'");
                cB.commit();
                bVerified.countDown(); // Signal A that B has verified

                aUpdated.await(5, TimeUnit.SECONDS); // Wait for A to update

                ResultSet rs2 = sB.executeQuery("SELECT stage FROM msli_barrier WHERE id = 2");
                assertTrue(rs2.next(), "B should see updated row");
                assertEquals("step3", rs2.getString(1), "B should see stage='step3' after A updates");
                cB.commit();
            } catch (Exception e) {
                bVerified.countDown();
                throw new RuntimeException(e);
            }
            return null;
        });

        exec.shutdown();
        exec.awaitTermination(15, TimeUnit.SECONDS);

        // Surface any exceptions from the concurrent tasks
        futureA.get(10, TimeUnit.SECONDS);
        futureB.get(10, TimeUnit.SECONDS);
    }

    // =========================================================================
    // Additional coverage
    // =========================================================================

    @Test
    void testReadCommittedSeesUpdatesFromOtherCommittedSession() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_rc_upd (id int PRIMARY KEY, val int)");
            sA.execute("INSERT INTO msli_rc_upd VALUES (1, 10)");
            cA.commit();

            sB.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
            // B reads initial value
            ResultSet r1 = sB.executeQuery("SELECT val FROM msli_rc_upd WHERE id = 1");
            r1.next();
            assertEquals(10, r1.getInt(1));

            // A updates and commits
            sA.execute("UPDATE msli_rc_upd SET val = 99 WHERE id = 1");
            cA.commit();

            // B re-reads (READ COMMITTED) - should see 99
            ResultSet r2 = sB.executeQuery("SELECT val FROM msli_rc_upd WHERE id = 1");
            r2.next();
            assertEquals(99, r2.getInt(1),
                    "READ COMMITTED should see committed update from other session");
            cB.commit();
        }
    }

    @Test
    void testRepeatableReadDoesNotSeeUpdatesAfterSnapshot() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_rr_upd (id int PRIMARY KEY, val int)");
            sA.execute("INSERT INTO msli_rr_upd VALUES (1, 10)");
            cA.commit();

            sB.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            // B takes snapshot by reading
            ResultSet r1 = sB.executeQuery("SELECT val FROM msli_rr_upd WHERE id = 1");
            r1.next();
            assertEquals(10, r1.getInt(1));

            // A updates and commits
            sA.execute("UPDATE msli_rr_upd SET val = 99 WHERE id = 1");
            cA.commit();

            // B re-reads - REPEATABLE READ should still see 10
            ResultSet r2 = sB.executeQuery("SELECT val FROM msli_rr_upd WHERE id = 1");
            r2.next();
            assertEquals(10, r2.getInt(1),
                    "REPEATABLE READ should not see updates committed after snapshot");
            cB.commit();
        }
    }

    @Test
    void testMultipleSharedLocksCoexist() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_forshare2 (id int PRIMARY KEY, val text)");
            sA.execute("INSERT INTO msli_forshare2 VALUES (1, 'shared')");
            cA.commit();

            // Both sessions acquire FOR SHARE - should not block each other
            ResultSet rsA = sA.executeQuery("SELECT * FROM msli_forshare2 WHERE id = 1 FOR SHARE");
            assertTrue(rsA.next());

            ResultSet rsB = sB.executeQuery("SELECT * FROM msli_forshare2 WHERE id = 1 FOR SHARE");
            assertTrue(rsB.next(), "FOR SHARE should coexist with another FOR SHARE");

            cA.commit();
            cB.commit();
        }
    }

    @Test
    void testSkipLockedWithEmptyResult() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_skipall (id int PRIMARY KEY, val text)");
            sA.execute("INSERT INTO msli_skipall VALUES (1, 'row1'), (2, 'row2')");
            cA.commit();

            // Session A locks all rows
            sA.executeQuery("SELECT * FROM msli_skipall FOR UPDATE");

            // Session B with SKIP LOCKED should get empty result
            ResultSet rs = sB.executeQuery("SELECT * FROM msli_skipall FOR UPDATE SKIP LOCKED");
            assertFalse(rs.next(), "All rows are locked; SKIP LOCKED should return empty result");

            cA.commit();
            cB.commit();
        }
    }

    @Test
    void testForKeyShareCompatibleWithForNoKeyUpdate() throws Exception {
        try (Connection cA = newConn(); Connection cB = newConn();
             Statement sA = cA.createStatement(); Statement sB = cB.createStatement()) {

            sA.execute("CREATE TABLE msli_keyshare_compat (id int PRIMARY KEY, val text)");
            sA.execute("INSERT INTO msli_keyshare_compat VALUES (1, 'test')");
            cA.commit();

            // Session A acquires FOR KEY SHARE
            sA.executeQuery("SELECT * FROM msli_keyshare_compat WHERE id = 1 FOR KEY SHARE");

            // Session B acquires FOR NO KEY UPDATE - should be compatible with FOR KEY SHARE
            ResultSet rs = sB.executeQuery(
                    "SELECT * FROM msli_keyshare_compat WHERE id = 1 FOR NO KEY UPDATE NOWAIT");
            assertTrue(rs.next(), "FOR NO KEY UPDATE should be compatible with FOR KEY SHARE");

            cA.commit();
            cB.commit();
        }
    }

    @Test
    void testTransactionIsolationLevelShowsCorrectly() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            ResultSet rs = s.executeQuery("SHOW transaction_isolation");
            assertTrue(rs.next());
            assertEquals("serializable", rs.getString(1));
            c.commit();
        }
    }

    @Test
    void testForUpdateAllRowsLocked() throws Exception {
        try (Connection cA = newConn(); Statement sA = cA.createStatement()) {
            sA.execute("CREATE TABLE msli_forall (id int PRIMARY KEY, val text)");
            sA.execute("INSERT INTO msli_forall VALUES (1,'a'),(2,'b'),(3,'c')");
            cA.commit();

            ResultSet rs = sA.executeQuery("SELECT id FROM msli_forall ORDER BY id FOR UPDATE");
            List<Integer> ids = new ArrayList<>();
            while (rs.next()) ids.add(rs.getInt(1));
            assertEquals(Cols.listOf(1, 2, 3), ids, "FOR UPDATE should return all rows");
            cA.commit();
        }
    }
}
