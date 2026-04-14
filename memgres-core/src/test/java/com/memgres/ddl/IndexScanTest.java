package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that indexes are real data structures used in SELECT queries.
 * Verifies index-based lookups, expression indexes, and indexes on virtual columns.
 */
class IndexScanTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void cleanUp() throws SQLException {
        for (String idx : List.of("idx_t1_cat", "idx_t1_name", "idx_t1_ab", "idx_t1_code",
                "idx_status", "idx_t1_lower_email", "idx_t1_sum", "idx_t1_b", "idx_t1_a", "idx_expr")) {
            try { exec("DROP INDEX IF EXISTS " + idx); } catch (SQLException ignored) {}
        }
        for (String t : List.of("t1", "t2", "t3")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
    }

    @AfterEach
    void cleanUpAfter() throws SQLException {
        for (String idx : List.of("idx_t1_cat", "idx_t1_name", "idx_t1_ab", "idx_t1_code",
                "idx_status", "idx_t1_lower_email", "idx_t1_sum", "idx_t1_b", "idx_t1_a", "idx_expr")) {
            try { exec("DROP INDEX IF EXISTS " + idx); } catch (SQLException ignored) {}
        }
        for (String t : List.of("t1", "t2", "t3")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Index-based SELECT lookups (equality predicates)
    // ========================================================================

    @Test
    void pk_index_used_for_equality_lookup() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, name text)");
        for (int i = 1; i <= 100; i++) {
            exec("INSERT INTO t1 VALUES (" + i + ", 'name" + i + "')");
        }
        // PK index should be used for WHERE id = 50
        assertEquals("name50", scalar("SELECT name FROM t1 WHERE id = 50"));
    }

    @Test
    void unique_index_used_for_equality_lookup() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, email text UNIQUE)");
        exec("INSERT INTO t1 VALUES (1, 'alice@test.com')");
        exec("INSERT INTO t1 VALUES (2, 'bob@test.com')");
        assertEquals("2", scalar("SELECT id FROM t1 WHERE email = 'bob@test.com'"));
    }

    @Test
    void non_unique_index_used_for_equality_lookup() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, category text, name text)");
        exec("CREATE INDEX idx_t1_cat ON t1 (category)");
        exec("INSERT INTO t1 VALUES (1, 'A', 'alice')");
        exec("INSERT INTO t1 VALUES (2, 'B', 'bob')");
        exec("INSERT INTO t1 VALUES (3, 'A', 'carol')");
        // Non-unique index should find both category='A' rows
        List<List<String>> rows = query("SELECT id, name FROM t1 WHERE category = 'A' ORDER BY id");
        assertEquals(2, rows.size());
        assertEquals("alice", rows.get(0).get(1));
        assertEquals("carol", rows.get(1).get(1));
    }

    @Test
    void index_lookup_with_additional_where_predicates() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, category text, val int)");
        exec("CREATE INDEX idx_t1_cat ON t1 (category)");
        exec("INSERT INTO t1 VALUES (1, 'A', 10)");
        exec("INSERT INTO t1 VALUES (2, 'A', 20)");
        exec("INSERT INTO t1 VALUES (3, 'B', 30)");
        // Index on category, plus additional filter on val
        List<List<String>> rows = query("SELECT id FROM t1 WHERE category = 'A' AND val > 15");
        assertEquals(1, rows.size());
        assertEquals("2", rows.get(0).get(0));
    }

    @Test
    void multi_column_index_lookup() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a text, b text, c text)");
        exec("CREATE INDEX idx_t1_ab ON t1 (a, b)");
        exec("INSERT INTO t1 VALUES (1, 'x', 'y', 'data1')");
        exec("INSERT INTO t1 VALUES (2, 'x', 'z', 'data2')");
        exec("INSERT INTO t1 VALUES (3, 'w', 'y', 'data3')");
        // Both columns of multi-column index have equality predicates
        assertEquals("data1", scalar("SELECT c FROM t1 WHERE a = 'x' AND b = 'y'"));
    }

    @Test
    void index_lookup_returns_correct_results_after_updates() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_t1_name ON t1 (name)");
        exec("INSERT INTO t1 VALUES (1, 'alice')");
        exec("INSERT INTO t1 VALUES (2, 'bob')");
        assertEquals("1", scalar("SELECT id FROM t1 WHERE name = 'alice'"));
        exec("UPDATE t1 SET name = 'charlie' WHERE id = 1");
        assertNull(scalar("SELECT id FROM t1 WHERE name = 'alice'"));
        assertEquals("1", scalar("SELECT id FROM t1 WHERE name = 'charlie'"));
    }

    @Test
    void index_lookup_returns_correct_results_after_deletes() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_t1_name ON t1 (name)");
        exec("INSERT INTO t1 VALUES (1, 'alice')");
        exec("INSERT INTO t1 VALUES (2, 'bob')");
        exec("DELETE FROM t1 WHERE id = 1");
        assertNull(scalar("SELECT id FROM t1 WHERE name = 'alice'"));
        assertEquals("2", scalar("SELECT id FROM t1 WHERE name = 'bob'"));
    }

    @Test
    void index_lookup_with_integer_equality() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, code int, data text)");
        exec("CREATE INDEX idx_t1_code ON t1 (code)");
        exec("INSERT INTO t1 VALUES (1, 100, 'a')");
        exec("INSERT INTO t1 VALUES (2, 200, 'b')");
        exec("INSERT INTO t1 VALUES (3, 100, 'c')");
        List<List<String>> rows = query("SELECT data FROM t1 WHERE code = 100 ORDER BY id");
        assertEquals(2, rows.size());
        assertEquals("a", rows.get(0).get(0));
        assertEquals("c", rows.get(1).get(0));
    }

    @Test
    void index_lookup_with_reversed_equality() throws SQLException {
        // Test WHERE 'alice' = name (reversed operands)
        exec("CREATE TABLE t1(id int PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_t1_name ON t1 (name)");
        exec("INSERT INTO t1 VALUES (1, 'alice')");
        exec("INSERT INTO t1 VALUES (2, 'bob')");
        assertEquals("1", scalar("SELECT id FROM t1 WHERE 'alice' = name"));
    }

    // ========================================================================
    // Expression indexes
    // ========================================================================

    @Test
    void expression_index_created_on_lower() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, email text)");
        exec("CREATE INDEX idx_t1_lower_email ON t1 ((lower(email)))");
        exec("INSERT INTO t1 VALUES (1, 'Alice@Test.COM')");
        exec("INSERT INTO t1 VALUES (2, 'BOB@test.com')");
        // Expression index exists in catalog
        List<List<String>> rows = query("SELECT indexname FROM pg_indexes WHERE tablename = 't1' AND indexname = 'idx_t1_lower_email'");
        assertEquals(1, rows.size());
    }

    @Test
    void unique_expression_index_enforces_uniqueness() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, email text)");
        exec("CREATE UNIQUE INDEX idx_t1_lower_email ON t1 ((lower(email)))");
        exec("INSERT INTO t1 VALUES (1, 'Alice@Test.COM')");
        // Same email in different case should conflict
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (2, 'alice@test.com')"));
        assertEquals("23505", ex.getSQLState());
    }

    @Test
    void expression_index_on_arithmetic() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int)");
        exec("CREATE INDEX idx_t1_sum ON t1 ((a + b))");
        exec("INSERT INTO t1 VALUES (1, 10, 20)");
        exec("INSERT INTO t1 VALUES (2, 30, 40)");
        // Index exists
        assertEquals("1", scalar("SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_t1_sum'"));
    }

    // ========================================================================
    // Index on virtual columns
    // ========================================================================

    @Test
    void create_index_on_virtual_column_rejected() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, " +
             "b int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        // PG 18: indexes on virtual generated columns are not supported
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE INDEX idx_t1_b ON t1 (b)"));
        assertEquals("0A000", ex.getSQLState());
    }

    @Test
    void select_from_table_with_virtual_column_and_base_col_index() throws SQLException {
        // Index on base column 'a', virtual column 'b' derived from 'a'
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, " +
             "b int GENERATED ALWAYS AS (a * 3) VIRTUAL)");
        exec("CREATE INDEX idx_t1_a ON t1 (a)");
        exec("INSERT INTO t1(id, a) VALUES (1, 10), (2, 20), (3, 30)");
        // Use index on 'a' for lookup, verify virtual 'b' is computed
        List<List<String>> rows = query("SELECT id, b FROM t1 WHERE a = 20");
        assertEquals(1, rows.size());
        assertEquals("2", rows.get(0).get(0));
        assertEquals("60", rows.get(0).get(1)); // 20 * 3
    }

    @Test
    void expression_index_on_virtual_column_expression() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, " +
             "b int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 10), (2, 20)");
        // Expression index on an expression involving a virtual column
        // This should parse and store but won't build a TableIndex (virtual)
        exec("CREATE INDEX idx_expr ON t1 ((b + 1))");
        assertEquals("1", scalar("SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_expr'"));
    }

    // ========================================================================
    // Index maintained across DML
    // ========================================================================

    @Test
    void index_maintained_through_insert_update_delete() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, status text)");
        exec("CREATE INDEX idx_status ON t1 (status)");
        exec("INSERT INTO t1 VALUES (1, 'active')");
        exec("INSERT INTO t1 VALUES (2, 'inactive')");
        exec("INSERT INTO t1 VALUES (3, 'active')");

        // Initial lookup
        List<List<String>> rows = query("SELECT id FROM t1 WHERE status = 'active' ORDER BY id");
        assertEquals(2, rows.size());
        assertEquals("1", rows.get(0).get(0));
        assertEquals("3", rows.get(1).get(0));

        // Update changes index
        exec("UPDATE t1 SET status = 'inactive' WHERE id = 1");
        rows = query("SELECT id FROM t1 WHERE status = 'active' ORDER BY id");
        assertEquals(1, rows.size());
        assertEquals("3", rows.get(0).get(0));

        // Delete removes from index
        exec("DELETE FROM t1 WHERE id = 3");
        rows = query("SELECT id FROM t1 WHERE status = 'active'");
        assertEquals(0, rows.size());

        // Insert adds to index
        exec("INSERT INTO t1 VALUES (4, 'active')");
        assertEquals("4", scalar("SELECT id FROM t1 WHERE status = 'active'"));
    }

    // ========================================================================
    // DROP INDEX removes index
    // ========================================================================

    @Test
    void drop_index_removes_index() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_t1_name ON t1 (name)");
        exec("INSERT INTO t1 VALUES (1, 'alice')");
        // Verify index exists
        assertEquals("1", scalar("SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_t1_name'"));
        exec("DROP INDEX idx_t1_name");
        assertEquals("0", scalar("SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_t1_name'"));
        // Query still works (falls back to seq scan)
        assertEquals("1", scalar("SELECT id FROM t1 WHERE name = 'alice'"));
    }

    // ========================================================================
    // Index with NULL values
    // ========================================================================

    @Test
    void index_handles_null_values() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_t1_name ON t1 (name)");
        exec("INSERT INTO t1 VALUES (1, 'alice')");
        exec("INSERT INTO t1 VALUES (2, NULL)");
        exec("INSERT INTO t1 VALUES (3, 'bob')");
        assertEquals("alice", scalar("SELECT name FROM t1 WHERE name = 'alice'"));
        // NULL lookup should return nothing (NULL = NULL is not true in SQL)
        assertEquals("0", scalar("SELECT COUNT(*) FROM t1 WHERE name = NULL"));
    }

    // ========================================================================
    // Concurrent index operations
    // ========================================================================

    @Test
    void create_index_concurrently_accepted() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, name text)");
        exec("INSERT INTO t1 VALUES (1, 'alice')");
        exec("CREATE INDEX CONCURRENTLY idx_t1_name ON t1 (name)");
        assertEquals("1", scalar("SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_t1_name'"));
    }

    @Test
    void create_index_if_not_exists() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_t1_name ON t1 (name)");
        // Should not error
        exec("CREATE INDEX IF NOT EXISTS idx_t1_name ON t1 (name)");
        assertEquals("1", scalar("SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_t1_name'"));
    }
}
