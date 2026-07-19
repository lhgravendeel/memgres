package com.memgres.locks;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PostgreSQL advisory lock semantics:
 * blocking acquisition, per-(session,key,mode) reference counting, shared vs exclusive
 * mode separation, distinct keyspaces for the 1-arg and 2-arg forms, transaction-scoped
 * auto-release on COMMIT and ROLLBACK, disconnect cleanup, and deadlock detection.
 */
class AdvisoryLockSemanticsTest {

    private static Memgres memgres;
    private static Connection connA;
    private static Connection connB;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        connA = open();
        connB = open();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (connA != null) connA.close();
        if (connB != null) connB.close();
        if (memgres != null) memgres.close();
    }

    private static Connection open() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private static boolean boolQuery(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected one row from: " + sql);
            return rs.getBoolean(1);
        }
    }

    private static int intQuery(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected one row from: " + sql);
            return rs.getInt(1);
        }
    }

    // ---- 1. Blocking acquisition ----

    @Test
    void blocking_lock_waits_until_holder_releases() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(700001)");
        AtomicLong elapsedMs = new AtomicLong(-1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            try {
                started.countDown();
                long t0 = System.nanoTime();
                exec(connB, "SELECT pg_advisory_lock(700001)");
                elapsedMs.set((System.nanoTime() - t0) / 1_000_000L);
            } catch (Exception e) {
                failure.set(e);
            }
        });
        waiter.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        // The waiter must still be blocked well after issuing the lock call
        waiter.join(500);
        assertTrue(waiter.isAlive(), "second pg_advisory_lock must block while the lock is held");
        exec(connA, "SELECT pg_advisory_unlock(700001)");
        waiter.join(5000);
        assertFalse(waiter.isAlive(), "waiter must complete once the lock is released");
        assertNull(failure.get(), "blocking acquisition must succeed after release: " + failure.get());
        assertTrue(elapsedMs.get() >= 300,
                "blocked acquire should have taken >=300ms but took " + elapsedMs.get() + "ms");
        exec(connB, "SELECT pg_advisory_unlock(700001)");
    }

    @Test
    void blocking_shared_lock_waits_for_exclusive_holder() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(700002)");
        AtomicReference<Exception> failure = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                exec(connB, "SELECT pg_advisory_lock_shared(700002)");
            } catch (Exception e) {
                failure.set(e);
            }
        });
        waiter.start();
        waiter.join(400);
        assertTrue(waiter.isAlive(), "pg_advisory_lock_shared must block while exclusive lock is held");
        exec(connA, "SELECT pg_advisory_unlock(700002)");
        waiter.join(5000);
        assertFalse(waiter.isAlive());
        assertNull(failure.get());
        exec(connB, "SELECT pg_advisory_unlock_shared(700002)");
    }

    // ---- 2. Reference counting ----

    @Test
    void reacquired_lock_needs_matching_number_of_unlocks() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(700010)");
        exec(connA, "SELECT pg_advisory_lock(700010)"); // refcount 2
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700010)"));

        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock(700010)")); // refcount 1
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700010)"),
                "lock must still be held after releasing only one of two acquisitions");

        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock(700010)"), // refcount 0
                "second unlock must succeed (return true) because the lock was acquired twice");
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_lock(700010)"));
        exec(connB, "SELECT pg_advisory_unlock(700010)");

        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock(700010)"),
                "unlock of a lock no longer held must return false");
    }

    @Test
    void shared_lock_is_reference_counted_too() throws Exception {
        exec(connA, "SELECT pg_advisory_lock_shared(700011)");
        exec(connA, "SELECT pg_advisory_lock_shared(700011)");
        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock_shared(700011)"));
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700011)"),
                "one shared hold must remain after one unlock");
        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock_shared(700011)"));
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock_shared(700011)"));
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_lock(700011)"));
        exec(connB, "SELECT pg_advisory_unlock(700011)");
    }

    // ---- 3. Mode separation ----

    @Test
    void unlock_shared_does_not_release_exclusive_hold() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(700020)");
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock_shared(700020)"),
                "pg_advisory_unlock_shared must not release an exclusive hold");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700020)"),
                "exclusive hold must survive a mismatched shared unlock");
        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock(700020)"));
    }

    @Test
    void unlock_exclusive_does_not_release_shared_hold() throws Exception {
        exec(connA, "SELECT pg_advisory_lock_shared(700021)");
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock(700021)"),
                "pg_advisory_unlock must not release a shared hold");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700021)"),
                "shared hold must survive a mismatched exclusive unlock");
        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock_shared(700021)"));
    }

    @Test
    void multiple_shared_holders_allowed_but_exclusive_excluded() throws Exception {
        assertTrue(boolQuery(connA, "SELECT pg_try_advisory_lock_shared(700022)"));
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_lock_shared(700022)"),
                "shared locks must be concurrently holdable");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700022)"),
                "exclusive lock must fail while another session holds shared");
        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock_shared(700022)"));
        assertFalse(boolQuery(connA, "SELECT pg_try_advisory_lock(700022)"),
                "exclusive still blocked by connB's shared hold");
        assertTrue(boolQuery(connB, "SELECT pg_advisory_unlock_shared(700022)"));
        assertTrue(boolQuery(connA, "SELECT pg_try_advisory_lock(700022)"));
        exec(connA, "SELECT pg_advisory_unlock(700022)");
    }

    @Test
    void same_session_can_hold_both_modes() throws Exception {
        assertTrue(boolQuery(connA, "SELECT pg_try_advisory_lock(700023)"));
        assertTrue(boolQuery(connA, "SELECT pg_try_advisory_lock_shared(700023)"),
                "a session may stack shared on top of its own exclusive hold");
        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock(700023)"));
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700023)"),
                "shared hold must remain after the exclusive one is released");
        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock_shared(700023)"));
    }

    // ---- 4. Keyspace separation (1-arg bigint vs 2-arg int pair) ----

    @Test
    void one_arg_and_two_arg_forms_use_distinct_keyspaces() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(700030)");
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_lock(0, 700030)"),
                "pg_advisory_lock(0, k) must not conflict with pg_advisory_lock(k)");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700030)"));

        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock(0, 700030)"),
                "session A holds the 1-arg lock, not the 2-arg one");
        assertFalse(boolQuery(connB, "SELECT pg_advisory_unlock(700030)"),
                "session B holds the 2-arg lock, not the 1-arg one");

        assertTrue(boolQuery(connA, "SELECT pg_advisory_unlock(700030)"));
        assertTrue(boolQuery(connB, "SELECT pg_advisory_unlock(0, 700030)"));
    }

    // ---- 5. Transaction-scoped locks ----

    @Test
    void xact_lock_released_on_commit() throws Exception {
        exec(connA, "BEGIN");
        exec(connA, "SELECT pg_advisory_xact_lock(700040)");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700040)"));
        exec(connA, "COMMIT");
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_lock(700040)"),
                "xact advisory lock must be released at COMMIT");
        exec(connB, "SELECT pg_advisory_unlock(700040)");
    }

    @Test
    void xact_lock_released_on_rollback() throws Exception {
        exec(connA, "BEGIN");
        exec(connA, "SELECT pg_advisory_xact_lock(700041)");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_xact_lock(700041)"));
        exec(connA, "ROLLBACK");
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_lock(700041)"),
                "xact advisory lock must be released at ROLLBACK");
        exec(connB, "SELECT pg_advisory_unlock(700041)");
    }

    @Test
    void xact_shared_lock_released_at_transaction_end() throws Exception {
        exec(connA, "BEGIN");
        exec(connA, "SELECT pg_advisory_xact_lock_shared(700042)");
        exec(connB, "BEGIN");
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_xact_lock_shared(700042)"),
                "shared xact locks must be concurrently holdable");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_xact_lock(700042)"));
        exec(connB, "COMMIT");
        exec(connA, "COMMIT");
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_lock(700042)"));
        exec(connB, "SELECT pg_advisory_unlock(700042)");
    }

    @Test
    void manual_unlock_cannot_release_xact_lock() throws Exception {
        exec(connA, "BEGIN");
        exec(connA, "SELECT pg_advisory_xact_lock(700043)");
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock(700043)"),
                "pg_advisory_unlock must not release a transaction-level hold");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700043)"));
        exec(connA, "COMMIT");
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_lock(700043)"));
        exec(connB, "SELECT pg_advisory_unlock(700043)");
    }

    @Test
    void failed_xact_acquisition_is_not_registered() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(700044)");
        try {
            exec(connB, "SET lock_timeout = 200");
            exec(connB, "BEGIN");
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec(connB, "SELECT pg_advisory_xact_lock(700044)"),
                    "blocking xact lock must time out under lock_timeout while lock is held");
            assertEquals("55P03", ex.getSQLState());
            exec(connB, "ROLLBACK");
            // Under the old bug the failed acquisition was still registered and connB's
            // rollback would have released connA's lock.
            assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700044)"),
                    "connA must still hold the lock after connB's failed xact acquisition");
        } finally {
            exec(connB, "SET lock_timeout = 0");
            exec(connA, "SELECT pg_advisory_unlock(700044)");
        }
    }

    // ---- 6. Try variants and unlock-not-held ----

    @Test
    void try_variants_return_false_while_held_by_other_session() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(700050)");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700050)"));
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock_shared(700050)"));
        exec(connB, "BEGIN");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_xact_lock(700050)"));
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_xact_lock_shared(700050)"));
        exec(connB, "COMMIT");
        exec(connA, "SELECT pg_advisory_unlock(700050)");
    }

    @Test
    void unlock_of_never_held_lock_returns_false() throws Exception {
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock(700051)"));
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock_shared(700051)"));
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock(700051, 700051)"));
    }

    // ---- 7. unlock_all ----

    @Test
    void unlock_all_releases_all_session_level_holds() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(700060)");
        exec(connA, "SELECT pg_advisory_lock(700060)");
        exec(connA, "SELECT pg_advisory_lock_shared(700061)");
        exec(connA, "SELECT pg_advisory_lock(1, 700062)");
        exec(connA, "SELECT pg_advisory_unlock_all()");
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock(700060)"));
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock_shared(700061)"));
        assertFalse(boolQuery(connA, "SELECT pg_advisory_unlock(1, 700062)"));
        assertTrue(boolQuery(connB, "SELECT pg_try_advisory_lock(700060)"));
        exec(connB, "SELECT pg_advisory_unlock(700060)");
    }

    // ---- 8. Disconnect cleanup ----

    @Test
    void disconnect_releases_session_level_locks() throws Exception {
        Connection c = open();
        exec(c, "SELECT pg_advisory_lock(700070)");
        assertFalse(boolQuery(connB, "SELECT pg_try_advisory_lock(700070)"));
        c.close();
        // channelInactive processing is asynchronous; poll briefly
        boolean acquired = false;
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            if (boolQuery(connB, "SELECT pg_try_advisory_lock(700070)")) {
                acquired = true;
                break;
            }
            Thread.sleep(50);
        }
        assertTrue(acquired, "disconnect must release the session's advisory locks");
        exec(connB, "SELECT pg_advisory_unlock(700070)");
    }

    // ---- 9. Deadlock detection ----

    @Test
    void deadlock_between_two_advisory_waiters_is_detected() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(700080)");
        exec(connB, "SELECT pg_advisory_lock(700081)");
        AtomicReference<Exception> waiterFailure = new AtomicReference<>();
        Thread waiter = new Thread(() -> {
            try {
                exec(connA, "SELECT pg_advisory_lock(700081)"); // blocks on connB's lock
            } catch (Exception e) {
                waiterFailure.set(e);
            }
        });
        waiter.start();
        waiter.join(300);
        assertTrue(waiter.isAlive(), "connA should be blocked waiting for connB's lock");
        // connB now waits for connA -> cycle -> 40P01 for connB (it detects the cycle)
        SQLException ex = assertThrows(SQLException.class,
                () -> exec(connB, "SELECT pg_advisory_lock(700080)"));
        assertEquals("40P01", ex.getSQLState(), "deadlock must be reported as 40P01");
        // Let connA's wait complete by releasing connB's lock
        exec(connB, "SELECT pg_advisory_unlock(700081)");
        waiter.join(5000);
        assertFalse(waiter.isAlive());
        assertNull(waiterFailure.get(), "the surviving waiter must acquire after release: " + waiterFailure.get());
        exec(connA, "SELECT pg_advisory_unlock(700080)");
        exec(connA, "SELECT pg_advisory_unlock(700081)");
    }

    // ---- 10. pg_locks visibility ----

    @Test
    void pg_locks_shows_key_split_mode_and_keyspace() throws Exception {
        exec(connA, "SELECT pg_advisory_lock(3, 4)");
        assertEquals(1, intQuery(connB, "SELECT count(*)::int FROM pg_locks WHERE locktype='advisory'"
                + " AND classid=3 AND objid=4 AND objsubid=2 AND mode='ExclusiveLock'"),
                "2-arg advisory lock must appear with classid/objid split and objsubid=2");
        exec(connA, "SELECT pg_advisory_unlock(3, 4)");

        long bigKey = (5L << 32) + 6L;
        exec(connA, "SELECT pg_advisory_lock_shared(" + bigKey + ")");
        assertEquals(1, intQuery(connB, "SELECT count(*)::int FROM pg_locks WHERE locktype='advisory'"
                + " AND classid=5 AND objid=6 AND objsubid=1 AND mode='ShareLock'"),
                "1-arg advisory lock must be split into high/low 32-bit halves with objsubid=1");
        exec(connA, "SELECT pg_advisory_unlock_shared(" + bigKey + ")");

        assertEquals(0, intQuery(connB, "SELECT count(*)::int FROM pg_locks WHERE locktype='advisory'"
                + " AND classid=5 AND objid=6"));
    }
}
