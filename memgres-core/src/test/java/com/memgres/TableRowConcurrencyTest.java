package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Table.java row mutation concurrency safety.
 * Exercises the volatile snapshot-swap + writeLock serialization
 * that prevents lost updates during concurrent DML and ROLLBACK.
 */
class TableRowConcurrencyTest {

    static Memgres memgres;
    static String url;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).maxConnections(50).build().start();
        url = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
    }

    @AfterAll
    static void teardown() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        Connection c = DriverManager.getConnection(url, "test", "test");
        c.setAutoCommit(true);
        return c;
    }

    private Connection connectTx() throws SQLException {
        Connection c = DriverManager.getConnection(url, "test", "test");
        c.setAutoCommit(false);
        return c;
    }

    private int queryInt(Statement st, String sql) throws SQLException {
        ResultSet rs = st.executeQuery(sql);
        assertTrue(rs.next());
        return rs.getInt(1);
    }

    // =========================================================================
    // 1. Concurrent non-overlapping DELETEs (original bug: clear-and-rebuild)
    // =========================================================================

    @RepeatedTest(10)
    void testConcurrentNonOverlappingDeletes() throws Exception {
        int numThreads = 5;
        int rowsPerThread = 10;
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_del_concurrent");
            st.execute("CREATE TABLE t_del_concurrent (id int, owner int)");
            for (int t = 0; t < numThreads; t++) {
                for (int i = 0; i < rowsPerThread; i++) {
                    st.execute("INSERT INTO t_del_concurrent VALUES (" + (t * 100 + i) + ", " + t + ")");
                }
            }
        }

        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            final int owner = t;
            futures.add(pool.submit(() -> {
                try (Connection c = connect(); Statement st = c.createStatement()) {
                    st.execute("DELETE FROM t_del_concurrent WHERE owner = " + owner);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            assertEquals(0, queryInt(st, "SELECT COUNT(*) FROM t_del_concurrent"),
                    "All rows should be deleted by concurrent non-overlapping DELETEs");
            st.execute("DROP TABLE t_del_concurrent");
        }
    }

    // =========================================================================
    // 2. Concurrent INSERT + ROLLBACK (Tier 1.1/1.2 — undo lost-update)
    // =========================================================================

    @RepeatedTest(10)
    void testConcurrentInsertAndRollback() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_ins_rb");
            st.execute("CREATE TABLE t_ins_rb (id int, conn_id int)");
        }

        int numConns = 8;
        ExecutorService pool = Executors.newFixedThreadPool(numConns);
        CountDownLatch ready = new CountDownLatch(numConns);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        for (int c = 0; c < numConns; c++) {
            final int connId = c;
            final boolean shouldCommit = (c % 2 == 0);
            futures.add(pool.submit(() -> {
                try (Connection conn = connectTx(); Statement st = conn.createStatement()) {
                    for (int i = 0; i < 5; i++) {
                        st.execute("INSERT INTO t_ins_rb VALUES (" + (connId * 100 + i) + ", " + connId + ")");
                    }
                    ready.countDown();
                    go.await(10, TimeUnit.SECONDS);
                    if (shouldCommit) {
                        conn.commit();
                    } else {
                        conn.rollback();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        ready.await(10, TimeUnit.SECONDS);
        go.countDown(); // all threads commit/rollback simultaneously

        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        // Even connections committed: 0, 2, 4, 6 = 4 connections * 5 rows = 20
        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            int count = queryInt(st, "SELECT COUNT(*) FROM t_ins_rb");
            assertEquals(20, count, "Only committed transactions' rows should persist");
            assertEquals(0, queryInt(st, "SELECT COUNT(*) FROM t_ins_rb WHERE conn_id % 2 = 1"),
                    "No rows from rolled-back transactions should remain");
            st.execute("DROP TABLE t_ins_rb");
        }
    }

    // =========================================================================
    // 3. Concurrent INSERT + DELETE on same table (cross-operation serialization)
    // =========================================================================

    @RepeatedTest(10)
    void testConcurrentInsertAndDelete() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_ins_del");
            st.execute("CREATE TABLE t_ins_del (id int, batch int)");
            // Pre-populate batch 0 (will be deleted)
            for (int i = 0; i < 20; i++) {
                st.execute("INSERT INTO t_ins_del VALUES (" + i + ", 0)");
            }
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);

        // Thread 1: delete all batch=0 rows
        Future<?> deleter = pool.submit(() -> {
            try (Connection c = connect(); Statement st = c.createStatement()) {
                ready.countDown();
                go.await(10, TimeUnit.SECONDS);
                st.execute("DELETE FROM t_ins_del WHERE batch = 0");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Thread 2: insert batch=1 rows
        Future<?> inserter = pool.submit(() -> {
            try (Connection c = connect(); Statement st = c.createStatement()) {
                ready.countDown();
                go.await(10, TimeUnit.SECONDS);
                for (int i = 0; i < 20; i++) {
                    st.execute("INSERT INTO t_ins_del VALUES (" + (100 + i) + ", 1)");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        deleter.get(15, TimeUnit.SECONDS);
        inserter.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            assertEquals(0, queryInt(st, "SELECT COUNT(*) FROM t_ins_del WHERE batch = 0"),
                    "All batch=0 rows should be deleted");
            assertEquals(20, queryInt(st, "SELECT COUNT(*) FROM t_ins_del WHERE batch = 1"),
                    "All batch=1 rows should be inserted");
            st.execute("DROP TABLE t_ins_del");
        }
    }

    // =========================================================================
    // 4. Many concurrent inserters (high contention on insertRow)
    // =========================================================================

    @RepeatedTest(5)
    void testManyConcurrentInserters() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_many_ins");
            st.execute("CREATE TABLE t_many_ins (id int, thread_id int)");
        }

        int numThreads = 10;
        int rowsPerThread = 20;
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < numThreads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                try (Connection c = connect(); Statement st = c.createStatement()) {
                    for (int i = 0; i < rowsPerThread; i++) {
                        st.execute("INSERT INTO t_many_ins VALUES (" + (tid * 1000 + i) + ", " + tid + ")");
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            assertEquals(numThreads * rowsPerThread,
                    queryInt(st, "SELECT COUNT(*) FROM t_many_ins"),
                    "All rows from all threads should be present");
            // Verify each thread's rows are intact
            for (int t = 0; t < numThreads; t++) {
                assertEquals(rowsPerThread,
                        queryInt(st, "SELECT COUNT(*) FROM t_many_ins WHERE thread_id = " + t),
                        "Thread " + t + " should have exactly " + rowsPerThread + " rows");
            }
            st.execute("DROP TABLE t_many_ins");
        }
    }

    // =========================================================================
    // 5. Concurrent ROLLBACK of DELETEs (DeleteUndo → insertRow without lock)
    // =========================================================================

    @RepeatedTest(10)
    void testConcurrentDeleteRollbacks() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_del_rb");
            st.execute("CREATE TABLE t_del_rb (id int)");
            for (int i = 0; i < 50; i++) {
                st.execute("INSERT INTO t_del_rb VALUES (" + i + ")");
            }
        }

        int numConns = 4;
        ExecutorService pool = Executors.newFixedThreadPool(numConns);
        CountDownLatch ready = new CountDownLatch(numConns);
        CountDownLatch go = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        // Each thread deletes a range in a transaction, then rolls back
        for (int c = 0; c < numConns; c++) {
            final int lo = c * 12;
            final int hi = lo + 12;
            futures.add(pool.submit(() -> {
                try (Connection conn = connectTx(); Statement st = conn.createStatement()) {
                    st.execute("DELETE FROM t_del_rb WHERE id >= " + lo + " AND id < " + hi);
                    ready.countDown();
                    go.await(10, TimeUnit.SECONDS);
                    conn.rollback();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        ready.await(10, TimeUnit.SECONDS);
        go.countDown(); // all rollback simultaneously

        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            assertEquals(50, queryInt(st, "SELECT COUNT(*) FROM t_del_rb"),
                    "All rows should be restored after concurrent rollbacks");
            st.execute("DROP TABLE t_del_rb");
        }
    }

    // =========================================================================
    // 6. Mixed DML: concurrent INSERT, UPDATE, DELETE on same table
    // =========================================================================

    @RepeatedTest(5)
    void testMixedConcurrentDml() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_mixed");
            st.execute("CREATE TABLE t_mixed (id int, val int)");
            for (int i = 0; i < 100; i++) {
                st.execute("INSERT INTO t_mixed VALUES (" + i + ", 0)");
            }
        }

        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch go = new CountDownLatch(1);

        // Thread 1: delete id < 30
        Future<?> deleter = pool.submit(() -> {
            try (Connection c = connect(); Statement st = c.createStatement()) {
                go.await(10, TimeUnit.SECONDS);
                st.execute("DELETE FROM t_mixed WHERE id < 30");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Thread 2: insert 20 new rows (id 200-219)
        Future<?> inserter = pool.submit(() -> {
            try (Connection c = connect(); Statement st = c.createStatement()) {
                go.await(10, TimeUnit.SECONDS);
                for (int i = 200; i < 220; i++) {
                    st.execute("INSERT INTO t_mixed VALUES (" + i + ", 1)");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Thread 3: update id 50-69 set val=99
        Future<?> updater = pool.submit(() -> {
            try (Connection c = connect(); Statement st = c.createStatement()) {
                go.await(10, TimeUnit.SECONDS);
                st.execute("UPDATE t_mixed SET val = 99 WHERE id >= 50 AND id < 70");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        go.countDown();
        deleter.get(15, TimeUnit.SECONDS);
        inserter.get(15, TimeUnit.SECONDS);
        updater.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            // Original 100 - 30 deleted + 20 inserted = 90
            assertEquals(90, queryInt(st, "SELECT COUNT(*) FROM t_mixed"),
                    "Should have 100 - 30 deleted + 20 inserted = 90 rows");
            assertEquals(0, queryInt(st, "SELECT COUNT(*) FROM t_mixed WHERE id < 30"),
                    "Rows with id < 30 should be deleted");
            assertEquals(20, queryInt(st, "SELECT COUNT(*) FROM t_mixed WHERE id >= 200"),
                    "20 new rows should be inserted");
            assertEquals(20, queryInt(st, "SELECT COUNT(*) FROM t_mixed WHERE val = 99"),
                    "20 rows should be updated to val=99");
            st.execute("DROP TABLE t_mixed");
        }
    }

    // =========================================================================
    // 7. Concurrent transactions: some commit, some rollback (reproduces
    //    testConcurrentTransactionsWithMixedCommitRollback failure)
    // =========================================================================

    @RepeatedTest(10)
    void testConcurrentCommitRollbackMix() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_commit_rb");
            st.execute("CREATE TABLE t_commit_rb (id int, conn_id int)");
        }

        int numConns = 6;
        ExecutorService pool = Executors.newFixedThreadPool(numConns);
        CountDownLatch latch = new CountDownLatch(numConns);
        List<Future<?>> futures = new ArrayList<>();

        for (int c = 0; c < numConns; c++) {
            final int connId = c;
            final boolean shouldCommit = (c % 2 == 0);
            futures.add(pool.submit(() -> {
                try (Connection conn = connectTx(); Statement st = conn.createStatement()) {
                    for (int i = 0; i < 5; i++) {
                        st.execute("INSERT INTO t_commit_rb VALUES (" + (connId * 100 + i) + ", " + connId + ")");
                    }
                    if (shouldCommit) {
                        conn.commit();
                    } else {
                        conn.rollback();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            }));
        }

        assertTrue(latch.await(15, TimeUnit.SECONDS));
        pool.shutdown();

        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            // Even connections: 0, 2, 4 → 3 * 5 = 15
            assertEquals(15, queryInt(st, "SELECT COUNT(*) FROM t_commit_rb"),
                    "Only committed transactions' rows should persist");
            assertEquals(0, queryInt(st, "SELECT COUNT(*) FROM t_commit_rb WHERE conn_id % 2 = 1"),
                    "No rows from rolled-back transactions");
            st.execute("DROP TABLE t_commit_rb");
        }
    }

    // =========================================================================
    // 8. Stress test: high-contention concurrent inserts and deletes
    // =========================================================================

    @Test
    void testHighContentionInsertDelete() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("DROP TABLE IF EXISTS t_stress");
            st.execute("CREATE TABLE t_stress (id serial, thread_id int)");
        }

        int numThreads = 8;
        int opsPerThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        AtomicInteger totalInserted = new AtomicInteger(0);
        AtomicInteger totalDeleted = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < numThreads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                try (Connection c = connect(); Statement st = c.createStatement()) {
                    for (int i = 0; i < opsPerThread; i++) {
                        st.execute("INSERT INTO t_stress (thread_id) VALUES (" + tid + ")");
                        totalInserted.incrementAndGet();
                    }
                    // Each thread deletes some of its own rows
                    ResultSet rs = st.executeQuery(
                            "SELECT COUNT(*) FROM t_stress WHERE thread_id = " + tid);
                    rs.next();
                    int myRows = rs.getInt(1);
                    if (myRows > 10) {
                        st.execute("DELETE FROM t_stress WHERE thread_id = " + tid
                                + " AND id IN (SELECT id FROM t_stress WHERE thread_id = " + tid + " LIMIT 10)");
                        totalDeleted.addAndGet(10);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
        pool.shutdown();

        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            int expected = totalInserted.get() - totalDeleted.get();
            int actual = queryInt(st, "SELECT COUNT(*) FROM t_stress");
            assertEquals(expected, actual,
                    "Row count should equal inserts minus deletes");
            st.execute("DROP TABLE t_stress");
        }
    }
}
