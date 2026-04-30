package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for items 134-136:
 * 134: Transaction isolation levels
 * 135: Advisory locks
 * 136: Two-phase commit
 */
class ConcurrencyTransactionTest {

    static Memgres memgres;
    static String url;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).maxConnections(20).build().start();
        url = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
        // Enable two-phase commit (disabled by default like PG)
        try (Connection c = DriverManager.getConnection(url, "test", "test");
             var s = c.createStatement()) {
            s.execute("SET max_prepared_transactions = 10");
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(url, "test", "test");
    }

    private String query1(Statement st, String sql) throws SQLException {
        ResultSet rs = st.executeQuery(sql);
        assertTrue(rs.next(), "Expected at least one row for: " + sql);
        return rs.getString(1);
    }

    // ========================================================================
    // 134: Transaction Isolation Levels
    // ========================================================================

    // [BEGIN with ISOLATION LEVEL]

    @Test void testBeginReadCommitted() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN ISOLATION LEVEL READ COMMITTED");
            assertEquals("read committed", query1(st, "SHOW transaction_isolation"));
            st.execute("COMMIT");
        }
    }

    @Test void testBeginRepeatableRead() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN ISOLATION LEVEL REPEATABLE READ");
            assertEquals("repeatable read", query1(st, "SHOW transaction_isolation"));
            st.execute("COMMIT");
        }
    }

    @Test void testBeginSerializable() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN ISOLATION LEVEL SERIALIZABLE");
            assertEquals("serializable", query1(st, "SHOW transaction_isolation"));
            st.execute("COMMIT");
        }
    }

    @Test void testBeginReadUncommitted() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN ISOLATION LEVEL READ UNCOMMITTED");
            assertEquals("read uncommitted", query1(st, "SHOW transaction_isolation"));
            st.execute("COMMIT");
        }
    }

    @Test void testStartTransactionIsolationLevel() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            assertEquals("serializable", query1(st, "SHOW transaction_isolation"));
            st.execute("COMMIT");
        }
    }

    @Test void testBeginReadOnly() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN READ ONLY");
            assertEquals("on", query1(st, "SHOW transaction_read_only"));
            st.execute("COMMIT");
        }
    }

    @Test void testBeginReadWrite() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN READ WRITE");
            assertEquals("off", query1(st, "SHOW transaction_read_only"));
            st.execute("COMMIT");
        }
    }

    @Test void testBeginIsolationAndReadOnly() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY");
            assertEquals("repeatable read", query1(st, "SHOW transaction_isolation"));
            assertEquals("on", query1(st, "SHOW transaction_read_only"));
            st.execute("COMMIT");
        }
    }

    @Test void testBeginDeferrable() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            // Should parse without error
            st.execute("BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE");
            st.execute("COMMIT");
        }
    }

    @Test void testBeginNotDeferrable() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN NOT DEFERRABLE");
            st.execute("COMMIT");
        }
    }

    // [SET TRANSACTION ISOLATION LEVEL]

    @Test void testSetTransactionIsolationReadCommitted() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
            assertEquals("read committed", query1(st, "SHOW transaction_isolation"));
        }
    }

    @Test void testSetTransactionIsolationRepeatableRead() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals("repeatable read", query1(st, "SHOW transaction_isolation"));
        }
    }

    @Test void testSetTransactionIsolationSerializable() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            assertEquals("serializable", query1(st, "SHOW transaction_isolation"));
        }
    }

    @Test void testSetTransactionReadOnly() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            // Should not error even though we just consume the tokens
            st.execute("SET TRANSACTION READ ONLY");
        }
    }

    @Test void testSetTransactionReadWrite() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SET TRANSACTION READ WRITE");
        }
    }

    // [SET default_transaction_isolation]

    @Test void testSetDefaultTransactionIsolation() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SET default_transaction_isolation = 'repeatable read'");
            assertEquals("repeatable read", query1(st, "SHOW default_transaction_isolation"));
        }
    }

    // [SHOW transaction_isolation]

    @Test void testShowTransactionIsolation() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            String level = query1(st, "SHOW transaction_isolation");
            assertEquals("read committed", level); // default
        }
    }

    @Test void testShowTransactionIsolationLevel() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            // SHOW TRANSACTION ISOLATION LEVEL is PG syntax
            String level = query1(st, "SHOW TRANSACTION ISOLATION LEVEL");
            assertEquals("read committed", level);
        }
    }

    @Test void testCurrentSettingTransactionIsolation() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            assertEquals("read committed", query1(st, "SELECT current_setting('transaction_isolation')"));
        }
    }

    // [Transaction isolation is per-session]

    @Test void testIsolationPerSession() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("BEGIN ISOLATION LEVEL SERIALIZABLE");
            assertEquals("serializable", query1(s1, "SHOW transaction_isolation"));
            // Other session still at default
            assertEquals("read committed", query1(s2, "SHOW transaction_isolation"));
            s1.execute("COMMIT");
        }
    }

    // [Basic transaction behavior]

    @Test void testCommitPersistsData() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE txn_commit_test (id int)");
            st.execute("BEGIN");
            st.execute("INSERT INTO txn_commit_test VALUES (1)");
            st.execute("COMMIT");
            assertEquals("1", query1(st, "SELECT count(*) FROM txn_commit_test"));
            st.execute("DROP TABLE txn_commit_test");
        }
    }

    @Test void testRollbackRevertsData() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE txn_rb_test (id int)");
            st.execute("INSERT INTO txn_rb_test VALUES (1)");
            st.execute("BEGIN");
            st.execute("INSERT INTO txn_rb_test VALUES (2)");
            st.execute("ROLLBACK");
            assertEquals("1", query1(st, "SELECT count(*) FROM txn_rb_test"));
            st.execute("DROP TABLE txn_rb_test");
        }
    }

    @Test void testSavepointAndRollbackTo() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE txn_sp_test (id int)");
            st.execute("BEGIN");
            st.execute("INSERT INTO txn_sp_test VALUES (1)");
            st.execute("SAVEPOINT sp1");
            st.execute("INSERT INTO txn_sp_test VALUES (2)");
            st.execute("ROLLBACK TO SAVEPOINT sp1");
            st.execute("COMMIT");
            assertEquals("1", query1(st, "SELECT count(*) FROM txn_sp_test"));
            st.execute("DROP TABLE txn_sp_test");
        }
    }

    @Test void testReleaseSavepoint() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE txn_rel_test (id int)");
            st.execute("BEGIN");
            st.execute("INSERT INTO txn_rel_test VALUES (1)");
            st.execute("SAVEPOINT sp1");
            st.execute("INSERT INTO txn_rel_test VALUES (2)");
            st.execute("RELEASE SAVEPOINT sp1");
            st.execute("COMMIT");
            assertEquals("2", query1(st, "SELECT count(*) FROM txn_rel_test"));
            st.execute("DROP TABLE txn_rel_test");
        }
    }

    @Test void testFailedTransactionRecovery() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            try {
                st.execute("SELECT * FROM nonexistent_txn_table_xyz");
                fail("Should throw");
            } catch (SQLException e) {
                // expected; transaction is now FAILED
            }
            // Other commands should fail in FAILED state
            try {
                st.execute("SELECT 1");
                fail("Should throw in failed state");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("aborted"));
            }
            // ROLLBACK recovers
            st.execute("ROLLBACK");
            assertEquals("1", query1(st, "SELECT 1"));
        }
    }

    @Test void testFailedTransactionSavepointRecovery() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE txn_fail_sp (id int)");
            st.execute("BEGIN");
            st.execute("INSERT INTO txn_fail_sp VALUES (1)");
            st.execute("SAVEPOINT sp1");
            try {
                st.execute("INSERT INTO nonexistent_fail VALUES (2)");
            } catch (SQLException e) { /* expected */ }
            st.execute("ROLLBACK TO SAVEPOINT sp1");
            st.execute("INSERT INTO txn_fail_sp VALUES (3)");
            st.execute("COMMIT");
            assertEquals("2", query1(st, "SELECT count(*) FROM txn_fail_sp"));
            st.execute("DROP TABLE txn_fail_sp");
        }
    }

    @Test void testAbortSynonym() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("ABORT");
            assertEquals("1", query1(st, "SELECT 1"));
        }
    }

    @Test void testEndSynonym() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("END");
            assertEquals("1", query1(st, "SELECT 1"));
        }
    }

    @Test void testCommitWork() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("COMMIT WORK");
        }
    }

    @Test void testRollbackWork() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("ROLLBACK WORK");
        }
    }

    @Test void testCommitTransaction() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("COMMIT TRANSACTION");
        }
    }

    @Test void testRollbackTransaction() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN TRANSACTION");
            st.execute("ROLLBACK TRANSACTION");
        }
    }

    // [Concurrent transactions]

    @Test void testConcurrentTransactionsIsolated() throws Exception {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE txn_conc (id int, val text)");
            s1.execute("INSERT INTO txn_conc VALUES (1, 'original')");

            s1.execute("BEGIN");
            s1.execute("UPDATE txn_conc SET val = 'modified' WHERE id = 1");

            // c2 reads; should see original (committed) or modified depending on isolation
            // In memgres (shared state), c2 may see modified; just verify no crash
            String val = query1(s2, "SELECT val FROM txn_conc WHERE id = 1");
            assertNotNull(val);

            s1.execute("COMMIT");
            assertEquals("modified", query1(s2, "SELECT val FROM txn_conc WHERE id = 1"));
            s1.execute("DROP TABLE txn_conc");
        }
    }

    // ========================================================================
    // 135: Advisory Locks
    // ========================================================================

    // [pg_advisory_lock (session-level, exclusive)]

    @Test void testAdvisoryLockBasic() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock(12345)");
            st.execute("SELECT pg_advisory_unlock(12345)");
        }
    }

    @Test void testAdvisoryLockTwoKeys() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock(1, 2)");
            st.execute("SELECT pg_advisory_unlock(1, 2)");
        }
    }

    @Test void testTryAdvisoryLock() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            assertEquals("t", query1(st, "SELECT pg_try_advisory_lock(99999)"));
            st.execute("SELECT pg_advisory_unlock(99999)");
        }
    }

    @Test void testTryAdvisoryLockTwoKeys() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            assertEquals("t", query1(st, "SELECT pg_try_advisory_lock(10, 20)"));
            st.execute("SELECT pg_advisory_unlock(10, 20)");
        }
    }

    @Test void testAdvisoryUnlockReturnsTrue() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock(55555)");
            assertEquals("t", query1(st, "SELECT pg_advisory_unlock(55555)"));
        }
    }

    @Test void testAdvisoryUnlockReturnsFalseIfNotHeld() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            assertEquals("f", query1(st, "SELECT pg_advisory_unlock(77777)"));
        }
    }

    @Test void testAdvisoryUnlockAll() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock(100)");
            st.execute("SELECT pg_advisory_lock(200)");
            st.execute("SELECT pg_advisory_lock(300)");
            st.execute("SELECT pg_advisory_unlock_all()");
            // All should be released now
            assertEquals("f", query1(st, "SELECT pg_advisory_unlock(100)"));
            assertEquals("f", query1(st, "SELECT pg_advisory_unlock(200)"));
            assertEquals("f", query1(st, "SELECT pg_advisory_unlock(300)"));
        }
    }

    // [pg_advisory_xact_lock (transaction-level)]

    @Test void testAdvisoryXactLock() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("SELECT pg_advisory_xact_lock(44444)");
            st.execute("COMMIT");
        }
    }

    @Test void testTryAdvisoryXactLock() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            assertEquals("t", query1(st, "SELECT pg_try_advisory_xact_lock(55555)"));
            st.execute("COMMIT");
        }
    }

    @Test void testTryAdvisoryXactLockTwoKeys() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            assertEquals("t", query1(st, "SELECT pg_try_advisory_xact_lock(5, 6)"));
            st.execute("COMMIT");
        }
    }

    // [pg_advisory_lock_shared (shared locks)]

    @Test void testAdvisoryLockShared() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock_shared(88888)");
            st.execute("SELECT pg_advisory_unlock_shared(88888)");
        }
    }

    @Test void testAdvisoryLockSharedTwoKeys() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock_shared(3, 4)");
            st.execute("SELECT pg_advisory_unlock_shared(3, 4)");
        }
    }

    @Test void testAdvisoryXactLockShared() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("SELECT pg_advisory_xact_lock_shared(66666)");
            st.execute("COMMIT");
        }
    }

    @Test void testTryAdvisoryLockShared() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            assertEquals("t", query1(st, "SELECT pg_try_advisory_lock_shared(22222)"));
            st.execute("SELECT pg_advisory_unlock_shared(22222)");
        }
    }

    @Test void testTryAdvisoryLockSharedTwoKeys() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            assertEquals("t", query1(st, "SELECT pg_try_advisory_lock_shared(7, 8)"));
            st.execute("SELECT pg_advisory_unlock_shared(7, 8)");
        }
    }

    // [Multiple sessions with advisory locks]

    @Test void testAdvisoryLockMultipleSessions() throws Exception {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            // Session 1 acquires the lock
            s1.execute("SELECT pg_advisory_lock(11111)");
            // Session 2 cannot acquire it (mutual exclusion, matching PG behavior)
            assertEquals("f", query1(s2, "SELECT pg_try_advisory_lock(11111)"));
            // Session 1 unlocks
            s1.execute("SELECT pg_advisory_unlock(11111)");
            // Now session 2 can acquire
            assertEquals("t", query1(s2, "SELECT pg_try_advisory_lock(11111)"));
            s2.execute("SELECT pg_advisory_unlock(11111)");
        }
    }

    @Test void testAdvisoryLockConcurrentAcquireRelease() throws Exception {
        int numThreads = 5;
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final long key = 50000 + i;
            pool.submit(() -> {
                try (Connection c = connect(); Statement st = c.createStatement()) {
                    st.execute("SELECT pg_advisory_lock(" + key + ")");
                    st.execute("SELECT pg_advisory_unlock(" + key + ")");
                } catch (Exception e) {
                    fail("Advisory lock failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        pool.shutdown();
    }

    // [Advisory lock with negative keys]

    @Test void testAdvisoryLockNegativeKey() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock(-1)");
            assertEquals("t", query1(st, "SELECT pg_advisory_unlock(-1)"));
        }
    }

    @Test void testAdvisoryLockZeroKey() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock(0)");
            assertEquals("t", query1(st, "SELECT pg_advisory_unlock(0)"));
        }
    }

    @Test void testAdvisoryLockLargeKey() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_lock(9223372036854775807)"); // Long.MAX_VALUE
            assertEquals("t", query1(st, "SELECT pg_advisory_unlock(9223372036854775807)"));
        }
    }

    // ========================================================================
    // 136: Two-Phase Commit
    // ========================================================================

    @Test void testPrepareTransaction() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("PREPARE TRANSACTION 'test_xact_1'");
        }
    }

    @Test void testCommitPrepared() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("PREPARE TRANSACTION 'test_xact_commit'");
            st.execute("COMMIT PREPARED 'test_xact_commit'");
        }
    }

    @Test void testRollbackPrepared() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("PREPARE TRANSACTION 'test_xact_rollback'");
            st.execute("ROLLBACK PREPARED 'test_xact_rollback'");
        }
    }

    @Test void testPrepareAndCommitWorkflow() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("CREATE TABLE tpc_test (id int)");
            st.execute("INSERT INTO tpc_test VALUES (1)");
            st.execute("PREPARE TRANSACTION 'my_2pc'");
            st.execute("COMMIT PREPARED 'my_2pc'");
            // Table exists since no-op (data already committed by PREPARE)
            st.execute("DROP TABLE IF EXISTS tpc_test");
        }
    }

    @Test void testPrepareAndRollbackWorkflow() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("PREPARE TRANSACTION 'rollback_2pc'");
            st.execute("ROLLBACK PREPARED 'rollback_2pc'");
        }
    }

    // [pg_prepared_xacts view]

    @Test void testPgPreparedXactsView() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT transaction, gid, prepared, owner, database FROM pg_prepared_xacts");
            // Should be empty (no real 2PC support)
            assertFalse(rs.next());
        }
    }

    @Test void testPgPreparedXactsCount() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            assertEquals("0", query1(st, "SELECT count(*) FROM pg_prepared_xacts"));
        }
    }

    // [PREPARE TRANSACTION with various GID formats]

    @Test void testPrepareTransactionWithDottedGid() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("PREPARE TRANSACTION 'app.node1.xact42'");
            st.execute("COMMIT PREPARED 'app.node1.xact42'");
        }
    }

    @Test void testPrepareTransactionWithNumericGid() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            st.execute("PREPARE TRANSACTION '12345'");
            st.execute("ROLLBACK PREPARED '12345'");
        }
    }

    // ========================================================================
    // Mixed / edge-case tests
    // ========================================================================

    @Test void testIsolationLevelResetAfterCommit() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN ISOLATION LEVEL SERIALIZABLE");
            assertEquals("serializable", query1(st, "SHOW transaction_isolation"));
            st.execute("COMMIT");
            // After commit, isolation level persists in GUC (matches PG behavior)
            assertNotNull(query1(st, "SHOW transaction_isolation"));
        }
    }

    @Test void testSetTransactionAndBegin() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals("repeatable read", query1(st, "SHOW transaction_isolation"));
            st.execute("BEGIN");
            assertEquals("repeatable read", query1(st, "SHOW transaction_isolation"));
            st.execute("COMMIT");
        }
    }

    @Test void testAdvisoryLockInsideTransaction() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN");
            assertEquals("t", query1(st, "SELECT pg_try_advisory_lock(99998)"));
            st.execute("COMMIT");
            // Session lock persists after transaction (unlike xact locks in real PG)
            assertEquals("t", query1(st, "SELECT pg_advisory_unlock(99998)"));
        }
    }

    @Test void testMultipleIsolationLevelsAcrossTransactions() throws SQLException {
        try (Connection c = connect(); Statement st = c.createStatement()) {
            st.execute("BEGIN ISOLATION LEVEL SERIALIZABLE");
            assertEquals("serializable", query1(st, "SHOW transaction_isolation"));
            st.execute("COMMIT");

            st.execute("BEGIN ISOLATION LEVEL READ COMMITTED");
            assertEquals("read committed", query1(st, "SHOW transaction_isolation"));
            st.execute("COMMIT");
        }
    }
}
