package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PgWire extended protocol state management.
 *
 * Validates that:
 * - RowDescription is always sent before DataRows (no "no field structure" errors)
 * - PreparedStatement replacement doesn't cause column count mismatches
 * - Concurrent connections using extended protocol don't interfere
 * - Statement close properly cleans up protocol state
 * - Rapid query cycling doesn't leak protocol state
 */
class ProtocolStateManagementTest {

    static Memgres memgres;
    static String url;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).maxConnections(20).build().start();
        url = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
        try (Connection c = DriverManager.getConnection(url, "test", "test");
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS proto_test (id SERIAL PRIMARY KEY, name TEXT, value INT)");
            for (int i = 1; i <= 50; i++) {
                s.execute("INSERT INTO proto_test (name, value) VALUES ('item" + i + "', " + i + ")");
            }
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        try (Connection c = DriverManager.getConnection(url, "test", "test");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS proto_test");
        }
        if (memgres != null) memgres.close();
    }

    private Connection newConn() throws SQLException {
        return DriverManager.getConnection(url, "test", "test");
    }

    // =========================================================================
    // 1. Basic extended protocol: PreparedStatement always returns correct data
    // =========================================================================

    @Test
    void testPreparedSelectReturnsCorrectColumns() throws Exception {
        try (Connection c = newConn();
             PreparedStatement ps = c.prepareStatement("SELECT id, name, value FROM proto_test WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(3, meta.getColumnCount());
                assertEquals("id", meta.getColumnName(1).toLowerCase());
                assertEquals("name", meta.getColumnName(2).toLowerCase());
                assertEquals("value", meta.getColumnName(3).toLowerCase());
                assertNotNull(rs.getString("name"));
                assertEquals(1, rs.getInt("value"));
            }
        }
    }

    // =========================================================================
    // 2. Rapid PreparedStatement reuse: same statement, many executions
    // =========================================================================

    @Test
    void testRapidPreparedStatementReuse() throws Exception {
        try (Connection c = newConn();
             PreparedStatement ps = c.prepareStatement("SELECT id, name FROM proto_test WHERE id = ?")) {
            for (int i = 1; i <= 50; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Should find row for id=" + i);
                    assertEquals(2, rs.getMetaData().getColumnCount(),
                            "Column count must be 2 on iteration " + i);
                    assertEquals(i, rs.getInt(1));
                    assertNotNull(rs.getString(2));
                }
            }
        }
    }

    // =========================================================================
    // 3. Alternating PreparedStatements with different column counts
    // =========================================================================

    @Test
    void testAlternatingDifferentPreparedStatements() throws Exception {
        try (Connection c = newConn();
             PreparedStatement ps2 = c.prepareStatement("SELECT id, name FROM proto_test WHERE id = ?");
             PreparedStatement ps3 = c.prepareStatement("SELECT id, name, value FROM proto_test WHERE id = ?")) {
            for (int i = 1; i <= 20; i++) {
                // Use 2-column query
                ps2.setInt(1, i);
                try (ResultSet rs = ps2.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getMetaData().getColumnCount(),
                            "ps2 must return 2 columns on iteration " + i);
                }
                // Use 3-column query
                ps3.setInt(1, i);
                try (ResultSet rs = ps3.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getMetaData().getColumnCount(),
                            "ps3 must return 3 columns on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // 4. PreparedStatement close + new statement: no stale protocol state
    // =========================================================================

    @Test
    void testCloseAndReopenPreparedStatement() throws Exception {
        try (Connection c = newConn()) {
            for (int round = 0; round < 20; round++) {
                // Create, use, and close a PreparedStatement
                PreparedStatement ps = c.prepareStatement("SELECT id, name FROM proto_test WHERE id = ?");
                ps.setInt(1, round + 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getMetaData().getColumnCount());
                }
                ps.close();

                // Immediately create another with DIFFERENT columns
                ps = c.prepareStatement("SELECT id, name, value FROM proto_test WHERE id = ?");
                ps.setInt(1, round + 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getMetaData().getColumnCount(),
                            "After close+reopen, column count must be 3 on round " + round);
                    assertNotNull(rs.getString("name"));
                    assertEquals(round + 1, rs.getInt("value"));
                }
                ps.close();
            }
        }
    }

    // =========================================================================
    // 5. Interleave Statement (simple query) and PreparedStatement (extended)
    // =========================================================================

    @Test
    void testInterleavedSimpleAndExtendedProtocol() throws Exception {
        try (Connection c = newConn();
             Statement stmt = c.createStatement();
             PreparedStatement ps = c.prepareStatement("SELECT id, name FROM proto_test WHERE value > ?")) {
            for (int i = 0; i < 15; i++) {
                // Simple query
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM proto_test")) {
                    assertTrue(rs.next());
                    assertTrue(rs.getInt(1) >= 50);
                }
                // Extended query
                ps.setInt(1, 40);
                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        assertEquals(2, rs.getMetaData().getColumnCount());
                        count++;
                    }
                    assertTrue(count > 0, "Extended query must return rows on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // 6. Concurrent connections executing the same query pattern
    // =========================================================================

    @Test
    void testConcurrentExtendedProtocolConnections() throws Exception {
        int threadCount = 8;
        int iterations = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        List<Future<Void>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            futures.add(pool.submit(() -> {
                try (Connection c = newConn();
                     PreparedStatement ps = c.prepareStatement(
                             "SELECT id, name, value FROM proto_test WHERE id = ?")) {
                    for (int i = 1; i <= iterations; i++) {
                        ps.setInt(1, i);
                        try (ResultSet rs = ps.executeQuery()) {
                            assertTrue(rs.next(), "Row must exist for id=" + i);
                            assertEquals(3, rs.getMetaData().getColumnCount());
                            assertEquals(i, rs.getInt("id"));
                            assertNotNull(rs.getString("name"));
                            assertEquals(i, rs.getInt("value"));
                        }
                    }
                }
                return null;
            }));
        }

        for (Future<Void> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();
    }

    // =========================================================================
    // 7. Concurrent SELECT + UPDATE on same table (extended protocol)
    // =========================================================================

    @Test
    void testConcurrentSelectAndUpdateExtendedProtocol() throws Exception {
        // Create a separate table for this test to avoid conflicts
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE proto_conc (id SERIAL PRIMARY KEY, counter INT NOT NULL DEFAULT 0)");
            for (int i = 0; i < 100; i++) {
                s.execute("INSERT INTO proto_conc DEFAULT VALUES");
            }
        }

        try {
            int threadCount = 6;
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            List<Future<Void>> futures = new ArrayList<>();

            // 3 readers
            for (int t = 0; t < 3; t++) {
                futures.add(pool.submit(() -> {
                    try (Connection c = newConn();
                         PreparedStatement ps = c.prepareStatement(
                                 "SELECT id, counter FROM proto_conc WHERE id = ?")) {
                        for (int i = 1; i <= 100; i++) {
                            ps.setInt(1, i);
                            try (ResultSet rs = ps.executeQuery()) {
                                assertTrue(rs.next());
                                assertEquals(2, rs.getMetaData().getColumnCount(),
                                        "Reader must see 2 columns for id=" + i);
                                rs.getInt("counter"); // must not throw
                            }
                        }
                    }
                    return null;
                }));
            }

            // 3 writers
            for (int t = 0; t < 3; t++) {
                final int offset = t;
                futures.add(pool.submit(() -> {
                    try (Connection c = newConn();
                         PreparedStatement ps = c.prepareStatement(
                                 "UPDATE proto_conc SET counter = counter + 1 WHERE id = ?")) {
                        for (int i = 1 + offset; i <= 100; i += 3) {
                            ps.setInt(1, i);
                            ps.executeUpdate();
                        }
                    }
                    return null;
                }));
            }

            for (Future<Void> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdown();
        } finally {
            try (Connection c = newConn(); Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS proto_conc");
            }
        }
    }

    // =========================================================================
    // 8. Many short-lived connections with extended protocol, no leaks
    // =========================================================================

    @Test
    void testManyShortLivedConnections() throws Exception {
        for (int i = 0; i < 50; i++) {
            try (Connection c = newConn();
                 PreparedStatement ps = c.prepareStatement(
                         "SELECT id, name, value FROM proto_test WHERE id = ?")) {
                ps.setInt(1, (i % 50) + 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Row must exist on connection " + i);
                    assertEquals(3, rs.getMetaData().getColumnCount());
                }
            }
        }
    }

    // =========================================================================
    // 9. DML via PreparedStatement then SELECT: no state confusion
    // =========================================================================

    @Test
    void testDmlThenSelectViaExtendedProtocol() throws Exception {
        try (Connection c = newConn()) {
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE proto_dml (id SERIAL PRIMARY KEY, label TEXT)");
            }
            c.commit();

            try (PreparedStatement insert = c.prepareStatement(
                         "INSERT INTO proto_dml (label) VALUES (?)");
                 PreparedStatement select = c.prepareStatement(
                         "SELECT id, label FROM proto_dml ORDER BY id")) {

                for (int i = 0; i < 20; i++) {
                    insert.setString(1, "label_" + i);
                    assertEquals(1, insert.executeUpdate());

                    try (ResultSet rs = select.executeQuery()) {
                        int count = 0;
                        while (rs.next()) {
                            assertEquals(2, rs.getMetaData().getColumnCount());
                            assertNotNull(rs.getString("label"));
                            count++;
                        }
                        assertEquals(i + 1, count,
                                "Should see " + (i + 1) + " rows after insert " + i);
                    }
                }
            }
            c.commit();

            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE proto_dml");
            }
            c.commit();
        }
    }

    // =========================================================================
    // 10. Concurrent FOR UPDATE SKIP LOCKED with extended protocol
    //     (Verifies that extended protocol produces valid RowDescription
    //      even under high concurrency with row locking)
    // =========================================================================

    @Test
    void testConcurrentSkipLockedExtendedProtocol() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE proto_queue (id SERIAL PRIMARY KEY, status TEXT DEFAULT 'pending')");
            for (int i = 0; i < 100; i++) {
                s.execute("INSERT INTO proto_queue DEFAULT VALUES");
            }
        }

        try {
            int workerCount = 6;
            ExecutorService pool = Executors.newFixedThreadPool(workerCount);
            java.util.concurrent.atomic.AtomicInteger processed = new java.util.concurrent.atomic.AtomicInteger(0);
            List<Future<Void>> futures = new ArrayList<>();

            for (int w = 0; w < workerCount; w++) {
                futures.add(pool.submit(() -> {
                    try (Connection c = newConn()) {
                        c.setAutoCommit(false);
                        // Use PreparedStatement to exercise extended protocol
                        try (PreparedStatement sel = c.prepareStatement(
                                     "SELECT id FROM proto_queue WHERE status = 'pending' FOR UPDATE SKIP LOCKED LIMIT 5");
                             PreparedStatement upd = c.prepareStatement(
                                     "UPDATE proto_queue SET status = 'done' WHERE id = ?")) {
                            boolean found = true;
                            while (found) {
                                try (ResultSet rs = sel.executeQuery()) {
                                    List<Integer> batch = new ArrayList<>();
                                    while (rs.next()) {
                                        assertEquals(1, rs.getMetaData().getColumnCount(),
                                                "SKIP LOCKED result must have correct column count");
                                        batch.add(rs.getInt(1));
                                    }
                                    if (batch.isEmpty()) {
                                        c.rollback();
                                        found = false;
                                    } else {
                                        for (int id : batch) {
                                            upd.setInt(1, id);
                                            upd.executeUpdate();
                                        }
                                        c.commit();
                                        processed.addAndGet(batch.size());
                                    }
                                }
                            }
                        }
                    }
                    return null;
                }));
            }

            for (Future<Void> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            pool.shutdown();

            assertEquals(100, processed.get(), "All 100 items must be processed exactly once");
        } finally {
            try (Connection c = newConn(); Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS proto_queue");
            }
        }
    }

    // =========================================================================
    // 11. INSERT RETURNING via extended protocol: correct column metadata
    // =========================================================================

    @Test
    void testInsertReturningExtendedProtocol() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE proto_ret (id SERIAL PRIMARY KEY, name TEXT, created_at TIMESTAMP DEFAULT now())");
        }

        try (Connection c = newConn();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO proto_ret (name) VALUES (?) RETURNING id, name, created_at")) {
            for (int i = 0; i < 20; i++) {
                ps.setString(1, "item_" + i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "RETURNING must produce a row on iteration " + i);
                    assertEquals(3, rs.getMetaData().getColumnCount(),
                            "RETURNING must have 3 columns on iteration " + i);
                    assertTrue(rs.getInt("id") > 0);
                    assertEquals("item_" + i, rs.getString("name"));
                    assertNotNull(rs.getTimestamp("created_at"));
                }
            }
        } finally {
            try (Connection c = newConn(); Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS proto_ret");
            }
        }
    }
}
