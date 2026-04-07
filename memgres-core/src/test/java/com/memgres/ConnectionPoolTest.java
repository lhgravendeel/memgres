package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for multiple concurrent connections to a single Memgres instance,
 * simulating a JDBC connection pool (e.g., with 10 connections).
 * Validates thread safety for parallel reads, writes, DDL, and connection limits.
 */
class ConnectionPoolTest {

    static Memgres memgres;
    static String jdbcUrl;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).maxConnections(20).build().start();
        jdbcUrl = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
        // Set up shared schema
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "test", "test");
             Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE pool_test (id serial PRIMARY KEY, value text, counter int DEFAULT 0)");
            st.execute("CREATE TABLE pool_reads (id int, data text)");
            for (int i = 1; i <= 100; i++) {
                st.execute("INSERT INTO pool_reads (id, data) VALUES (" + i + ", 'row" + i + "')");
            }
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (memgres != null) memgres.close();
    }

    // ---- Basic multi-connection ----

    @Test
    void testMultipleConnectionsSimultaneously() throws Exception {
        int numConnections = 10;
        Connection[] connections = new Connection[numConnections];
        try {
            // Open 10 connections at once
            for (int i = 0; i < numConnections; i++) {
                connections[i] = DriverManager.getConnection(jdbcUrl, "test", "test");
                assertNotNull(connections[i]);
                assertFalse(connections[i].isClosed());
            }

            // Verify each connection can independently query
            for (int i = 0; i < numConnections; i++) {
                try (Statement st = connections[i].createStatement();
                     ResultSet rs = st.executeQuery("SELECT 1 + " + i)) {
                    assertTrue(rs.next());
                    assertEquals(1 + i, rs.getInt(1));
                }
            }
        } finally {
            for (Connection conn : connections) {
                if (conn != null) conn.close();
            }
        }
    }

    @Test
    void testConnectionsShareSameDatabase() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(jdbcUrl, "test", "test");
             Connection conn2 = DriverManager.getConnection(jdbcUrl, "test", "test")) {

            // conn1 inserts data
            try (Statement st = conn1.createStatement()) {
                st.execute("INSERT INTO pool_test (value) VALUES ('from_conn1')");
            }

            // conn2 can see it
            try (Statement st = conn2.createStatement();
                 ResultSet rs = st.executeQuery("SELECT value FROM pool_test WHERE value = 'from_conn1'")) {
                assertTrue(rs.next());
                assertEquals("from_conn1", rs.getString(1));
            }

            // Clean up
            try (Statement st = conn1.createStatement()) {
                st.execute("DELETE FROM pool_test WHERE value = 'from_conn1'");
            }
        }
    }

    // ---- Concurrent parallel reads ----

    @Test
    void testParallelReadsFromPool() throws Exception {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Connection> pool = new CopyOnWriteArrayList<>();

        try {
            // Create pool of connections
            for (int i = 0; i < numThreads; i++) {
                pool.add(DriverManager.getConnection(jdbcUrl, "test", "test"));
            }

            // Each thread reads all 100 rows using its own connection
            List<Future<Integer>> futures = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                final Connection conn = pool.get(t);
                futures.add(executor.submit(() -> {
                    int count = 0;
                    try (Statement st = conn.createStatement();
                         ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM pool_reads")) {
                        rs.next();
                        count = rs.getInt(1);
                    }
                    return count;
                }));
            }

            // All should see 100 rows
            for (Future<Integer> f : futures) {
                assertEquals(100, f.get(10, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdown();
            for (Connection conn : pool) conn.close();
        }
    }

    // ---- Concurrent parallel writes ----

    @Test
    void testParallelWritesFromPool() throws Exception {
        int numThreads = 10;
        int insertsPerThread = 20;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Connection> pool = new CopyOnWriteArrayList<>();

        try {
            for (int i = 0; i < numThreads; i++) {
                pool.add(DriverManager.getConnection(jdbcUrl, "test", "test"));
            }

            // Create a dedicated table for this test
            try (Statement st = pool.get(0).createStatement()) {
                st.execute("CREATE TABLE parallel_writes (id serial PRIMARY KEY, thread_id int, seq int)");
            }

            // Each thread inserts 20 rows
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                final Connection conn = pool.get(t);
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < insertsPerThread; i++) {
                        try (Statement st = conn.createStatement()) {
                            st.execute("INSERT INTO parallel_writes (thread_id, seq) VALUES (" + threadId + ", " + i + ")");
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }));
            }

            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            // Verify all rows were inserted
            try (Statement st = pool.get(0).createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM parallel_writes")) {
                rs.next();
                assertEquals(numThreads * insertsPerThread, rs.getInt(1));
            }

            // Verify each thread's rows are present
            for (int t = 0; t < numThreads; t++) {
                try (Statement st = pool.get(0).createStatement();
                     ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM parallel_writes WHERE thread_id = " + t)) {
                    rs.next();
                    assertEquals(insertsPerThread, rs.getInt(1));
                }
            }

            try (Statement st = pool.get(0).createStatement()) {
                st.execute("DROP TABLE parallel_writes");
            }
        } finally {
            executor.shutdown();
            for (Connection conn : pool) conn.close();
        }
    }

    // ---- Concurrent mixed reads and writes ----

    @Test
    void testConcurrentReadersAndWriters() throws Exception {
        int numReaders = 5;
        int numWriters = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numReaders + numWriters);
        List<Connection> pool = new CopyOnWriteArrayList<>();
        AtomicInteger writeCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);

        try {
            for (int i = 0; i < numReaders + numWriters; i++) {
                pool.add(DriverManager.getConnection(jdbcUrl, "test", "test"));
            }

            try (Statement st = pool.get(0).createStatement()) {
                st.execute("CREATE TABLE rw_test (id serial, data text)");
            }

            List<Future<?>> futures = new ArrayList<>();

            // Readers: continuously read counts
            for (int r = 0; r < numReaders; r++) {
                final Connection conn = pool.get(r);
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < 50; i++) {
                            try (Statement st = conn.createStatement();
                                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM rw_test")) {
                                rs.next();
                                int count = rs.getInt(1);
                                assertTrue(count >= 0); // should never be negative
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            // Writers: insert rows
            for (int w = 0; w < numWriters; w++) {
                final Connection conn = pool.get(numReaders + w);
                final int writerId = w;
                futures.add(executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < 20; i++) {
                            try (Statement st = conn.createStatement()) {
                                st.execute("INSERT INTO rw_test (data) VALUES ('writer" + writerId + "_" + i + "')");
                                writeCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown(); // Start all threads simultaneously

            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            // Final count should match total writes
            try (Statement st = pool.get(0).createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM rw_test")) {
                rs.next();
                assertEquals(numWriters * 20, rs.getInt(1));
            }

            try (Statement st = pool.get(0).createStatement()) {
                st.execute("DROP TABLE rw_test");
            }
        } finally {
            executor.shutdown();
            for (Connection conn : pool) conn.close();
        }
    }

    // ---- Independent sessions ----

    @Test
    void testIndependentSessions() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(jdbcUrl, "test", "test");
             Connection conn2 = DriverManager.getConnection(jdbcUrl, "test", "test")) {

            // Each connection has its own session - SET on one doesn't affect the other
            try (Statement st = conn1.createStatement()) {
                st.execute("SET search_path TO 'custom_schema'");
            }

            try (Statement st = conn2.createStatement();
                 ResultSet rs = st.executeQuery("SHOW search_path")) {
                rs.next();
                // conn2 should still have default search_path
                String path = rs.getString(1);
                assertNotEquals("custom_schema", path);
            }
        }
    }

    @Test
    void testIndependentTransactions() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(jdbcUrl, "test", "test");
             Connection conn2 = DriverManager.getConnection(jdbcUrl, "test", "test")) {

            try (Statement st = conn1.createStatement()) {
                st.execute("CREATE TABLE tx_test (id int, val text)");
            }

            // conn1 begins a transaction
            try (Statement st = conn1.createStatement()) {
                st.execute("BEGIN");
                st.execute("INSERT INTO tx_test VALUES (1, 'in_tx')");
            }

            // conn2 can still execute queries (not blocked by conn1's transaction)
            try (Statement st = conn2.createStatement();
                 ResultSet rs = st.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }

            // Rollback conn1's transaction
            try (Statement st = conn1.createStatement()) {
                st.execute("ROLLBACK");
            }

            try (Statement st = conn1.createStatement()) {
                st.execute("DROP TABLE tx_test");
            }
        }
    }

    // ---- Connection limit enforcement ----

    @Test
    void testMaxConnectionsEnforced() throws Exception {
        // Create a memgres instance with a very small limit
        try (Memgres limited = Memgres.builder().port(0).maxConnections(3).build().start()) {
            String url = "jdbc:postgresql://localhost:" + limited.getPort() + "/test";

            Connection[] conns = new Connection[3];
            try {
                // Open 3 connections (at the limit)
                for (int i = 0; i < 3; i++) {
                    conns[i] = DriverManager.getConnection(url, "test", "test");
                    assertNotNull(conns[i]);
                }

                // 4th connection should be rejected
                assertThrows(SQLException.class, () ->
                        DriverManager.getConnection(url, "test", "test"));
            } finally {
                for (Connection c : conns) {
                    if (c != null) c.close();
                }
            }

            // After closing, we should be able to connect again
            Thread.sleep(100); // Give Netty a moment to process the disconnects
            try (Connection conn = DriverManager.getConnection(url, "test", "test")) {
                assertNotNull(conn);
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("SELECT 1")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            }
        }
    }

    @Test
    void testConnectionLimitReleasedOnClose() throws Exception {
        try (Memgres limited = Memgres.builder().port(0).maxConnections(2).build().start()) {
            String url = "jdbc:postgresql://localhost:" + limited.getPort() + "/test";

            // Open and close connections repeatedly to verify slots are released
            for (int round = 0; round < 5; round++) {
                Connection conn1 = DriverManager.getConnection(url, "test", "test");
                Connection conn2 = DriverManager.getConnection(url, "test", "test");
                conn1.close();
                conn2.close();
                Thread.sleep(50); // Wait for cleanup
            }
        }
    }

    // ---- Pool-style usage patterns ----

    @Test
    void testPoolBorrowAndReturn() throws Exception {
        // Simulate a connection pool: borrow a connection, do work, return it
        int poolSize = 5;
        int totalOperations = 50;
        BlockingQueue<Connection> pool = new LinkedBlockingQueue<>();
        ExecutorService executor = Executors.newFixedThreadPool(10);

        try {
            // Initialize pool
            for (int i = 0; i < poolSize; i++) {
                pool.add(DriverManager.getConnection(jdbcUrl, "test", "test"));
            }

            try (Statement st = pool.peek().createStatement()) {
                st.execute("CREATE TABLE pool_borrow (id serial, worker int)");
            }

            // Submit more work than pool size; threads must wait for connections
            List<Future<?>> futures = new ArrayList<>();
            AtomicInteger completed = new AtomicInteger(0);

            for (int i = 0; i < totalOperations; i++) {
                final int opId = i;
                futures.add(executor.submit(() -> {
                    try {
                        Connection conn = pool.poll(10, TimeUnit.SECONDS);
                        assertNotNull(conn, "Failed to borrow connection for op " + opId);
                        try (Statement st = conn.createStatement()) {
                            st.execute("INSERT INTO pool_borrow (worker) VALUES (" + opId + ")");
                        } finally {
                            pool.offer(conn); // return to pool
                        }
                        completed.incrementAndGet();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            assertEquals(totalOperations, completed.get());

            // Verify all rows
            Connection check = pool.poll(5, TimeUnit.SECONDS);
            try (Statement st = check.createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM pool_borrow")) {
                rs.next();
                assertEquals(totalOperations, rs.getInt(1));
            }

            try (Statement st = check.createStatement()) {
                st.execute("DROP TABLE pool_borrow");
            }
            pool.offer(check);
        } finally {
            executor.shutdown();
            Connection c;
            while ((c = pool.poll()) != null) c.close();
        }
    }

    // ---- Concurrent DDL + DML ----

    @Test
    void testConcurrentDdlAndDml() throws Exception {
        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Connection> pool = new CopyOnWriteArrayList<>();

        try {
            for (int i = 0; i < numThreads; i++) {
                pool.add(DriverManager.getConnection(jdbcUrl, "test", "test"));
            }

            // Each thread creates its own table, inserts data, queries, and drops it
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                final Connection conn = pool.get(t);
                futures.add(executor.submit(() -> {
                    try (Statement st = conn.createStatement()) {
                        String tableName = "ddl_test_" + threadId;
                        st.execute("CREATE TABLE " + tableName + " (id serial, val text)");
                        for (int i = 0; i < 10; i++) {
                            st.execute("INSERT INTO " + tableName + " (val) VALUES ('t" + threadId + "_" + i + "')");
                        }
                        try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                            rs.next();
                            assertEquals(10, rs.getInt(1));
                        }
                        st.execute("DROP TABLE " + tableName);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
            for (Connection conn : pool) conn.close();
        }
    }

    // ---- Concurrent updates to same table ----

    @Test
    void testConcurrentUpdatesToSameTable() throws Exception {
        int numThreads = 5;
        int updatesPerThread = 20;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Connection> pool = new CopyOnWriteArrayList<>();

        try {
            for (int i = 0; i < numThreads; i++) {
                pool.add(DriverManager.getConnection(jdbcUrl, "test", "test"));
            }

            try (Statement st = pool.get(0).createStatement()) {
                st.execute("CREATE TABLE update_test (id int PRIMARY KEY, counter int DEFAULT 0)");
                // Each thread will update its own row
                for (int i = 0; i < numThreads; i++) {
                    st.execute("INSERT INTO update_test VALUES (" + i + ", 0)");
                }
            }

            // Each thread updates only its own row
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                final Connection conn = pool.get(t);
                futures.add(executor.submit(() -> {
                    try {
                        for (int i = 0; i < updatesPerThread; i++) {
                            try (Statement st = conn.createStatement()) {
                                st.execute("UPDATE update_test SET counter = counter + 1 WHERE id = " + threadId);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            // Each row should have been updated exactly updatesPerThread times
            try (Statement st = pool.get(0).createStatement();
                 ResultSet rs = st.executeQuery("SELECT id, counter FROM update_test ORDER BY id")) {
                for (int i = 0; i < numThreads; i++) {
                    assertTrue(rs.next());
                    assertEquals(i, rs.getInt(1));
                    assertEquals(updatesPerThread, rs.getInt(2));
                }
            }

            try (Statement st = pool.get(0).createStatement()) {
                st.execute("DROP TABLE update_test");
            }
        } finally {
            executor.shutdown();
            for (Connection conn : pool) conn.close();
        }
    }

    // ---- Concurrent deletes ----

    @Test
    void testConcurrentDeletes() throws Exception {
        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Connection> pool = new CopyOnWriteArrayList<>();

        try {
            for (int i = 0; i < numThreads; i++) {
                pool.add(DriverManager.getConnection(jdbcUrl, "test", "test"));
            }

            try (Statement st = pool.get(0).createStatement()) {
                st.execute("CREATE TABLE delete_test (id int, owner int)");
                // Each thread owns 10 rows
                for (int t = 0; t < numThreads; t++) {
                    for (int i = 0; i < 10; i++) {
                        st.execute("INSERT INTO delete_test VALUES (" + (t * 10 + i) + ", " + t + ")");
                    }
                }
            }

            // Each thread deletes only its own rows
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                final Connection conn = pool.get(t);
                futures.add(executor.submit(() -> {
                    try (Statement st = conn.createStatement()) {
                        st.execute("DELETE FROM delete_test WHERE owner = " + threadId);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            // All rows should be deleted
            try (Statement st = pool.get(0).createStatement();
                 ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM delete_test")) {
                rs.next();
                assertEquals(0, rs.getInt(1));
            }

            try (Statement st = pool.get(0).createStatement()) {
                st.execute("DROP TABLE delete_test");
            }
        } finally {
            executor.shutdown();
            for (Connection conn : pool) conn.close();
        }
    }

    // ---- Sequence thread safety ----

    @Test
    void testConcurrentSequenceAccess() throws Exception {
        int numThreads = 10;
        int callsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Connection> pool = new CopyOnWriteArrayList<>();

        try {
            for (int i = 0; i < numThreads; i++) {
                pool.add(DriverManager.getConnection(jdbcUrl, "test", "test"));
            }

            try (Statement st = pool.get(0).createStatement()) {
                st.execute("CREATE SEQUENCE pool_seq");
            }

            // Each thread calls nextval() concurrently
            Set<Long> allValues = ConcurrentHashMap.newKeySet();
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                final Connection conn = pool.get(t);
                futures.add(executor.submit(() -> {
                    try {
                        for (int i = 0; i < callsPerThread; i++) {
                            try (Statement st = conn.createStatement();
                                 ResultSet rs = st.executeQuery("SELECT nextval('pool_seq')")) {
                                rs.next();
                                long val = rs.getLong(1);
                                assertTrue(allValues.add(val), "Duplicate sequence value: " + val);
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }

            // All values should be unique
            assertEquals(numThreads * callsPerThread, allValues.size());

            try (Statement st = pool.get(0).createStatement()) {
                st.execute("DROP SEQUENCE pool_seq");
            }
        } finally {
            executor.shutdown();
            for (Connection conn : pool) conn.close();
        }
    }

    // ---- High connection count ----

    @Test
    void testManyConnections() throws Exception {
        int numConnections = 15;
        Connection[] connections = new Connection[numConnections];
        try {
            for (int i = 0; i < numConnections; i++) {
                connections[i] = DriverManager.getConnection(jdbcUrl, "test", "test");
            }

            // All connections active and working
            for (int i = 0; i < numConnections; i++) {
                try (Statement st = connections[i].createStatement();
                     ResultSet rs = st.executeQuery("SELECT " + (i * 100))) {
                    assertTrue(rs.next());
                    assertEquals(i * 100, rs.getInt(1));
                }
            }
        } finally {
            for (Connection c : connections) {
                if (c != null) c.close();
            }
        }
    }

    // ---- Connection reuse after close ----

    @Test
    void testConnectionReuseAfterClose() throws Exception {
        // Rapidly open and close connections
        for (int i = 0; i < 20; i++) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, "test", "test");
                 Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT " + i)) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
        }
    }

    // ---- Prepared statements per-session ----

    @Test
    void testPreparedStatementsPerSession() throws Exception {
        try (Connection conn1 = DriverManager.getConnection(jdbcUrl, "test", "test");
             Connection conn2 = DriverManager.getConnection(jdbcUrl, "test", "test")) {

            // conn1 prepares a statement
            try (Statement st = conn1.createStatement()) {
                st.execute("PREPARE myplan AS SELECT 42 AS answer");
            }

            // conn2 should NOT have access to conn1's prepared statement
            try (Statement st = conn2.createStatement()) {
                assertThrows(SQLException.class, () ->
                        st.executeQuery("EXECUTE myplan"));
            }

            // conn1 can execute it
            try (Statement st = conn1.createStatement();
                 ResultSet rs = st.executeQuery("EXECUTE myplan")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
            }

            // Clean up
            try (Statement st = conn1.createStatement()) {
                st.execute("DEALLOCATE myplan");
            }
        }
    }

    // ---- SHOW / current_setting max_connections ----

    @Test
    void testShowMaxConnectionsReflectsConfiguredValue() throws Exception {
        // The instance was configured with maxConnections(20)
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "test", "test");
             Statement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery("SHOW max_connections");
            assertTrue(rs.next());
            assertEquals("20", rs.getString(1));
        }
    }

    @Test
    void testCurrentSettingMaxConnections() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "test", "test");
             Statement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT current_setting('max_connections')");
            assertTrue(rs.next());
            assertEquals("20", rs.getString(1));
        }
    }

    @Test
    void testSetMaxConnectionsRejected() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "test", "test");
             Statement st = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                    () -> st.execute("SET max_connections = 50"));
            assertTrue(ex.getMessage().contains("cannot be changed without restarting"));
        }
    }

    @Test
    void testSetConfigMaxConnectionsRejected() throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "test", "test");
             Statement st = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT set_config('max_connections', '50', false)"));
            assertTrue(ex.getMessage().contains("cannot be changed without restarting"));
        }
    }

    @Test
    void testShowMaxConnectionsDifferentInstance() throws Exception {
        // Create a separate instance with a different max
        try (Memgres db2 = Memgres.builder().port(0).maxConnections(5).build().start()) {
            String url2 = "jdbc:postgresql://localhost:" + db2.getPort() + "/test";
            try (Connection conn = DriverManager.getConnection(url2, "test", "test");
                 Statement st = conn.createStatement()) {
                ResultSet rs = st.executeQuery("SHOW max_connections");
                assertTrue(rs.next());
                assertEquals("5", rs.getString(1));
            }
        }
    }
}
