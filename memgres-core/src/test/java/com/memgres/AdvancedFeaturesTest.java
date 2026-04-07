package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 14: Advanced Features tests.
 * LISTEN/NOTIFY, table partitioning, full-text search, LATERAL joins,
 * table inheritance, advisory locks, generated columns, RLS policies,
 * EXPLAIN output.
 */
class AdvancedFeaturesTest {

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

    // ==================== LISTEN / NOTIFY ====================

    @Test
    void testListenNotifyBasic() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("LISTEN test_channel");
            stmt.execute("NOTIFY test_channel, 'hello'");
            // Notifications are delivered before ReadyForQuery,
            // so they should be accessible. We mainly test no errors.
            stmt.execute("UNLISTEN test_channel");
        }
    }

    @Test
    void testNotifyWithPayload() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("LISTEN events");
            stmt.execute("NOTIFY events, 'payload data'");
            stmt.execute("UNLISTEN events");
        }
    }

    @Test
    void testUnlistenAll() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("LISTEN ch1");
            stmt.execute("LISTEN ch2");
            stmt.execute("UNLISTEN *");
        }
    }

    // ==================== Table Partitioning ====================

    @Test
    void testRangePartitioning() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE measurements (id INTEGER, logdate DATE, value DOUBLE PRECISION) PARTITION BY RANGE (logdate)");
            stmt.execute("CREATE TABLE measurements_2023 PARTITION OF measurements FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')");
            stmt.execute("CREATE TABLE measurements_2024 PARTITION OF measurements FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')");

            stmt.execute("INSERT INTO measurements VALUES (1, '2023-06-15', 42.5)");
            stmt.execute("INSERT INTO measurements VALUES (2, '2024-03-20', 99.1)");

            // Query the parent should return all rows
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM measurements");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            stmt.execute("DROP TABLE measurements_2023");
            stmt.execute("DROP TABLE measurements_2024");
            stmt.execute("DROP TABLE measurements");
        }
    }

    @Test
    void testListPartitioning() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE orders (id INTEGER, region TEXT, amount DOUBLE PRECISION) PARTITION BY LIST (region)");
            stmt.execute("CREATE TABLE orders_east PARTITION OF orders FOR VALUES IN ('east', 'northeast')");
            stmt.execute("CREATE TABLE orders_west PARTITION OF orders FOR VALUES IN ('west', 'northwest')");

            stmt.execute("INSERT INTO orders VALUES (1, 'east', 100.0)");
            stmt.execute("INSERT INTO orders VALUES (2, 'west', 200.0)");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM orders");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            stmt.execute("DROP TABLE orders_east");
            stmt.execute("DROP TABLE orders_west");
            stmt.execute("DROP TABLE orders");
        }
    }

    @Test
    void testHashPartitioning() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE data (id INTEGER, value TEXT) PARTITION BY HASH (id)");
            stmt.execute("CREATE TABLE data_p0 PARTITION OF data FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
            stmt.execute("CREATE TABLE data_p1 PARTITION OF data FOR VALUES WITH (MODULUS 2, REMAINDER 1)");

            stmt.execute("INSERT INTO data VALUES (1, 'a')");
            stmt.execute("INSERT INTO data VALUES (2, 'b')");

            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM data");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            stmt.execute("DROP TABLE data_p0");
            stmt.execute("DROP TABLE data_p1");
            stmt.execute("DROP TABLE data");
        }
    }

    // ==================== Full-Text Search ====================

    @Test
    void testToTsvectorAndTsquery() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT to_tsvector('english', 'The quick brown fox') @@ to_tsquery('quick & fox')");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void testTsMatchNoMatch() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT to_tsvector('english', 'The quick brown fox') @@ to_tsquery('cat & dog')");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void testPlaintoTsquery() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT to_tsvector('english', 'cats and dogs') @@ plainto_tsquery('cat dog')");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void testTsRank() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT ts_rank(to_tsvector('english', 'The quick brown fox'), to_tsquery('fox'))");
            assertTrue(rs.next());
            double rank = rs.getDouble(1);
            assertTrue(rank > 0, "Rank should be > 0 for matching terms");
        }
    }

    @Test
    void testFullTextSearchInTable() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE docs (id INTEGER, body TEXT)");
            stmt.execute("INSERT INTO docs VALUES (1, 'The quick brown fox jumps over the lazy dog')");
            stmt.execute("INSERT INTO docs VALUES (2, 'A slow green turtle swims in the pond')");
            stmt.execute("INSERT INTO docs VALUES (3, 'The fox ran quickly through the forest')");

            ResultSet rs = stmt.executeQuery(
                    "SELECT id FROM docs WHERE to_tsvector('english', body) @@ to_tsquery('fox') ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());

            stmt.execute("DROP TABLE docs");
        }
    }

    // ==================== LATERAL Joins ====================

    @Test
    void testLateralJoin() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE departments (id INTEGER, name TEXT)");
            stmt.execute("CREATE TABLE employees (id INTEGER, dept_id INTEGER, name TEXT, salary INTEGER)");
            stmt.execute("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')");
            stmt.execute("INSERT INTO employees VALUES (1, 1, 'Alice', 90000)");
            stmt.execute("INSERT INTO employees VALUES (2, 1, 'Bob', 80000)");
            stmt.execute("INSERT INTO employees VALUES (3, 2, 'Charlie', 70000)");

            ResultSet rs = stmt.executeQuery(
                    "SELECT d.name, e.name FROM departments d " +
                    "JOIN LATERAL (SELECT * FROM employees WHERE dept_id = d.id ORDER BY salary DESC LIMIT 1) e ON true " +
                    "ORDER BY d.name");
            assertTrue(rs.next());
            assertEquals("Engineering", rs.getString(1));
            assertEquals("Alice", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("Sales", rs.getString(1));
            assertEquals("Charlie", rs.getString(2));
            assertFalse(rs.next());

            stmt.execute("DROP TABLE employees");
            stmt.execute("DROP TABLE departments");
        }
    }

    // ==================== Table Inheritance ====================

    @Test
    void testTableInheritance() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE cities (name TEXT, population INTEGER)");
            stmt.execute("CREATE TABLE capitals (name TEXT, population INTEGER, country TEXT) INHERITS (cities)");

            stmt.execute("INSERT INTO cities VALUES ('Amsterdam', 900000)");
            stmt.execute("INSERT INTO capitals VALUES ('Berlin', 3600000, 'Germany')");

            // Query parent includes child rows
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM cities");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            // Query with ONLY excludes child rows
            rs = stmt.executeQuery("SELECT COUNT(*) FROM ONLY cities");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            stmt.execute("DROP TABLE capitals");
            stmt.execute("DROP TABLE cities");
        }
    }

    // ==================== Advisory Locks ====================

    @Test
    void testAdvisoryLock() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT pg_try_advisory_lock(12345)");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));

            rs = stmt.executeQuery("SELECT pg_advisory_unlock(12345)");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void testAdvisoryLockBlocking() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_advisory_lock(99999)");
            // Should succeed (same session)
            ResultSet rs = stmt.executeQuery("SELECT pg_advisory_unlock(99999)");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void testAdvisoryUnlockAll() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_advisory_lock(111)");
            stmt.execute("SELECT pg_advisory_lock(222)");
            stmt.execute("SELECT pg_advisory_unlock_all()");
        }
    }

    // ==================== Generated Columns ====================

    @Test
    void testGeneratedColumnStored() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE products (price DOUBLE PRECISION, tax DOUBLE PRECISION, total DOUBLE PRECISION GENERATED ALWAYS AS (price + tax) STORED)");

            stmt.execute("INSERT INTO products (price, tax) VALUES (100.0, 21.0)");
            ResultSet rs = stmt.executeQuery("SELECT total FROM products");
            assertTrue(rs.next());
            assertEquals(121.0, rs.getDouble(1), 0.01);

            stmt.execute("DROP TABLE products");
        }
    }

    @Test
    void testGeneratedColumnWithExpression() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE people (first_name TEXT, last_name TEXT, full_name TEXT GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)");

            stmt.execute("INSERT INTO people (first_name, last_name) VALUES ('John', 'Doe')");
            ResultSet rs = stmt.executeQuery("SELECT full_name FROM people");
            assertTrue(rs.next());
            assertEquals("John Doe", rs.getString(1));

            stmt.execute("DROP TABLE people");
        }
    }

    // ==================== Row-Level Security ====================

    @Test
    void testEnableDisableRls() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE secure_data (id INTEGER, owner TEXT, data TEXT)");
            stmt.execute("ALTER TABLE secure_data ENABLE ROW LEVEL SECURITY");
            // Create a policy
            stmt.execute("CREATE POLICY owner_policy ON secure_data FOR SELECT USING (owner = 'admin')");
            // Disable RLS
            stmt.execute("ALTER TABLE secure_data DISABLE ROW LEVEL SECURITY");
            stmt.execute("DROP TABLE secure_data");
        }
    }

    @Test
    void testCreatePolicy() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE docs2 (id INTEGER, author TEXT, content TEXT)");
            stmt.execute("ALTER TABLE docs2 ENABLE ROW LEVEL SECURITY");
            stmt.execute("CREATE POLICY author_can_see ON docs2 FOR SELECT USING (author = 'alice')");

            stmt.execute("INSERT INTO docs2 VALUES (1, 'alice', 'secret doc')");
            stmt.execute("INSERT INTO docs2 VALUES (2, 'bob', 'another doc')");

            // Without enforcement, both rows are visible
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM docs2");
            assertTrue(rs.next());
            int count = rs.getInt(1);
            assertTrue(count >= 1, "Should have at least 1 row");

            stmt.execute("DROP TABLE docs2");
        }
    }

    // ==================== EXPLAIN Output ====================

    @Test
    void testExplainSelect() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE explain_test (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO explain_test VALUES (1, 'a'), (2, 'b')");

            ResultSet rs = stmt.executeQuery("EXPLAIN SELECT * FROM explain_test");
            assertTrue(rs.next());
            String plan = rs.getString(1);
            assertTrue(plan.contains("Seq Scan"), "EXPLAIN should show Seq Scan, got: " + plan);

            stmt.execute("DROP TABLE explain_test");
        }
    }

    @Test
    void testExplainAnalyze() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ea_test (id INTEGER, val TEXT)");
            stmt.execute("INSERT INTO ea_test VALUES (1, 'x'), (2, 'y')");

            ResultSet rs = stmt.executeQuery("EXPLAIN ANALYZE SELECT * FROM ea_test");
            assertTrue(rs.next());
            String plan = rs.getString(1);
            assertTrue(plan.contains("Seq Scan"), "Should show Seq Scan, got: " + plan);
            // Collect all lines to check for timing
            StringBuilder allLines = new StringBuilder(plan);
            while (rs.next()) {
                allLines.append("\n").append(rs.getString(1));
            }
            String full = allLines.toString();
            assertTrue(full.contains("actual time") || full.contains("Execution Time"),
                    "EXPLAIN ANALYZE should include timing info, got: " + full);

            stmt.execute("DROP TABLE ea_test");
        }
    }

    @Test
    void testExplainFormatJson() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ej_test (id INTEGER)");

            ResultSet rs = stmt.executeQuery("EXPLAIN (FORMAT JSON) SELECT * FROM ej_test");
            assertTrue(rs.next());
            String json = rs.getString(1);
            assertTrue(json.contains("Plan"), "JSON EXPLAIN should have Plan key, got: " + json);
            assertTrue(json.contains("Node Type"), "JSON EXPLAIN should have Node Type, got: " + json);

            stmt.execute("DROP TABLE ej_test");
        }
    }

    @Test
    void testExplainInsert() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ei_test (id INTEGER)");

            ResultSet rs = stmt.executeQuery("EXPLAIN INSERT INTO ei_test VALUES (1)");
            assertTrue(rs.next());
            String plan = rs.getString(1);
            assertTrue(plan.contains("Insert"), "EXPLAIN INSERT should show Insert, got: " + plan);

            stmt.execute("DROP TABLE ei_test");
        }
    }

    @Test
    void testExplainWithFilter() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ef_test (id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO ef_test VALUES (1, 'a'), (2, 'b')");

            ResultSet rs = stmt.executeQuery("EXPLAIN SELECT * FROM ef_test WHERE id = 1");
            StringBuilder plan = new StringBuilder();
            while (rs.next()) {
                plan.append(rs.getString(1)).append("\n");
            }
            String planStr = plan.toString();
            assertTrue(planStr.contains("Seq Scan"), "Should contain Seq Scan, got: " + planStr);
            assertTrue(planStr.contains("Filter"), "Should show Filter, got: " + planStr);

            stmt.execute("DROP TABLE ef_test");
        }
    }
}
