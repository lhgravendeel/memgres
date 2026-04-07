package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Performance tests verifying that index-accelerated operations complete
 * within reasonable time bounds (indicating O(1) lookups rather than O(N) scans).
 */
class IndexPerformanceTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    void bulkInsertWithPrimaryKey() throws SQLException {
        int count = 10_000;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_pk (id INTEGER PRIMARY KEY, val TEXT)");
        }
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO perf_pk (id, val) VALUES (?, ?)")) {
            for (int i = 1; i <= count; i++) {
                ps.setInt(1, i);
                ps.setString(2, "row" + i);
                ps.addBatch();
                if (i % 1000 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
        long elapsed = System.currentTimeMillis() - start;
        // 10K inserts with PK uniqueness checks should complete well under 10s with indexes
        assertTrue(elapsed < 10_000,
                "10K PK inserts took " + elapsed + "ms (expected < 10s with index acceleration)");
    }

    @Test
    void bulkInsertWithUniqueConstraint() throws SQLException {
        int count = 10_000;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_uniq (id SERIAL PRIMARY KEY, email TEXT UNIQUE)");
        }
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO perf_uniq (email) VALUES (?)")) {
            for (int i = 1; i <= count; i++) {
                ps.setString(1, "user" + i + "@example.com");
                ps.addBatch();
                if (i % 1000 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 10_000,
                "10K UNIQUE inserts took " + elapsed + "ms (expected < 10s with index acceleration)");
    }

    @Test
    void pkLookupPerformance() throws SQLException {
        int count = 5_000;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_lookup (id INTEGER PRIMARY KEY, data TEXT)");
            // Bulk insert
            StringBuilder sb = new StringBuilder("INSERT INTO perf_lookup (id, data) VALUES ");
            for (int i = 1; i <= count; i++) {
                if (i > 1) sb.append(',');
                sb.append("(").append(i).append(",'data").append(i).append("')");
            }
            stmt.execute(sb.toString());
        }
        // Now do 5000 individual PK lookups (SELECT WHERE id = ?)
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("SELECT data FROM perf_lookup WHERE id = ?")) {
            for (int i = 1; i <= count; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                }
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        // 5K individual round-trip lookups, bound by JDBC overhead rather than index performance
        assertTrue(elapsed < 30_000,
                "5K PK lookups took " + elapsed + "ms (expected < 30s)");
    }

    @Test
    void foreignKeyInsertPerformance() throws SQLException {
        int parentCount = 1_000;
        int childCount = 5_000;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_parent (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE perf_child (id SERIAL PRIMARY KEY, parent_id INTEGER REFERENCES perf_parent(id), val TEXT)");
            // Insert parents
            StringBuilder sb = new StringBuilder("INSERT INTO perf_parent (id, name) VALUES ");
            for (int i = 1; i <= parentCount; i++) {
                if (i > 1) sb.append(',');
                sb.append("(").append(i).append(",'parent").append(i).append("')");
            }
            stmt.execute(sb.toString());
        }
        // Insert children with FK validation
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO perf_child (parent_id, val) VALUES (?, ?)")) {
            for (int i = 1; i <= childCount; i++) {
                ps.setInt(1, (i % parentCount) + 1);
                ps.setString(2, "child" + i);
                ps.addBatch();
                if (i % 1000 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 10_000,
                "5K FK inserts took " + elapsed + "ms (expected < 10s with index-accelerated FK checks)");
    }

    @Test
    void onConflictDoNothingPerformance() throws SQLException {
        int count = 5_000;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_conflict (id INTEGER PRIMARY KEY, val TEXT)");
            // Pre-populate with half
            StringBuilder sb = new StringBuilder("INSERT INTO perf_conflict (id, val) VALUES ");
            for (int i = 1; i <= count / 2; i++) {
                if (i > 1) sb.append(',');
                sb.append("(").append(i).append(",'orig").append(i).append("')");
            }
            stmt.execute(sb.toString());
        }
        // Now insert all; half will conflict
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO perf_conflict (id, val) VALUES (?, ?) ON CONFLICT (id) DO NOTHING")) {
            for (int i = 1; i <= count; i++) {
                ps.setInt(1, i);
                ps.setString(2, "new" + i);
                ps.addBatch();
                if (i % 1000 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 10_000,
                "5K ON CONFLICT DO NOTHING took " + elapsed + "ms (expected < 10s)");
    }

    @Test
    void onConflictDoUpdatePerformance() throws SQLException {
        int count = 5_000;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_upsert (id INTEGER PRIMARY KEY, val TEXT, counter INTEGER DEFAULT 0)");
            // Pre-populate
            StringBuilder sb = new StringBuilder("INSERT INTO perf_upsert (id, val) VALUES ");
            for (int i = 1; i <= count; i++) {
                if (i > 1) sb.append(',');
                sb.append("(").append(i).append(",'orig").append(i).append("')");
            }
            stmt.execute(sb.toString());
        }
        // Upsert all; every row will conflict and update
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO perf_upsert (id, val, counter) VALUES (?, ?, 1) " +
                        "ON CONFLICT (id) DO UPDATE SET counter = perf_upsert.counter + 1")) {
            for (int i = 1; i <= count; i++) {
                ps.setInt(1, i);
                ps.setString(2, "updated" + i);
                ps.addBatch();
                if (i % 1000 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 10_000,
                "5K ON CONFLICT DO UPDATE took " + elapsed + "ms (expected < 10s)");
    }

    @Test
    void batchPreparedStatementInsert() throws SQLException {
        int count = 10_000;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_batch (id SERIAL PRIMARY KEY, a TEXT, b INTEGER, c BOOLEAN)");
        }
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO perf_batch (a, b, c) VALUES (?, ?, ?)")) {
            for (int i = 1; i <= count; i++) {
                ps.setString(1, "text" + i);
                ps.setInt(2, i);
                ps.setBoolean(3, i % 2 == 0);
                ps.addBatch();
                if (i % 1000 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 10_000,
                "10K batch inserts took " + elapsed + "ms (expected < 10s)");
    }

    @Test
    void compositePkInsertPerformance() throws SQLException {
        int count = 5_000;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_cpk (a INTEGER, b INTEGER, val TEXT, PRIMARY KEY (a, b))");
        }
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO perf_cpk (a, b, val) VALUES (?, ?, ?)")) {
            for (int i = 1; i <= count; i++) {
                ps.setInt(1, i / 100 + 1);
                ps.setInt(2, i);
                ps.setString(3, "val" + i);
                ps.addBatch();
                if (i % 1000 == 0) ps.executeBatch();
            }
            ps.executeBatch();
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 10_000,
                "5K composite PK inserts took " + elapsed + "ms (expected < 10s)");
    }

    @Test
    void duplicateKeyDetectionAtScale() throws SQLException {
        int count = 5_000;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE perf_dupcheck (id INTEGER PRIMARY KEY)");
            // Insert rows
            StringBuilder sb = new StringBuilder("INSERT INTO perf_dupcheck (id) VALUES ");
            for (int i = 1; i <= count; i++) {
                if (i > 1) sb.append(',');
                sb.append("(").append(i).append(")");
            }
            stmt.execute(sb.toString());
        }
        // Try to insert duplicates; each should fail fast
        long start = System.currentTimeMillis();
        int errors = 0;
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO perf_dupcheck (id) VALUES (?)")) {
            for (int i = 1; i <= 1_000; i++) {
                ps.setInt(1, i);
                try {
                    ps.executeUpdate();
                    fail("Should have thrown duplicate key exception for id=" + i);
                } catch (SQLException e) {
                    assertTrue(e.getMessage().contains("duplicate key") || e.getSQLState().equals("23505"));
                    errors++;
                }
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        assertEquals(1_000, errors);
        assertTrue(elapsed < 5_000,
                "1K duplicate key checks on 5K row table took " + elapsed + "ms (expected < 5s)");
    }
}
