package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for protocol-level regressions where the server crashes or returns
 * malformed responses instead of proper result sets or errors.
 *
 * Covers:
 * - Re-EXECUTE of a prepared SELECT statement (second execute crashes)
 * - CTE with FOR UPDATE SKIP LOCKED + UPDATE ... RETURNING (queue pattern)
 * - Various PREPARE/EXECUTE edge cases
 */
class ProtocolRegressionTest {

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
    // Re-EXECUTE of prepared SELECT: second execute must return results
    // ========================================================================

    @Test
    void re_execute_prepared_select_returns_results() throws SQLException {
        exec("CREATE TABLE prep_t(id int PRIMARY KEY, a int)");
        exec("INSERT INTO prep_t VALUES (1, 10), (2, 20), (3, 30)");
        try {
            exec("PREPARE p_limit(int) AS SELECT id, a FROM prep_t ORDER BY id LIMIT $1");

            // First execute should work
            List<List<String>> first = query("EXECUTE p_limit(2)");
            assertEquals(2, first.size(), "First EXECUTE should return 2 rows");
            assertEquals("1", first.get(0).get(0));
            assertEquals("2", first.get(1).get(0));

            // Second execute with different parameter; must not crash
            List<List<String>> second = query("EXECUTE p_limit(1)");
            assertEquals(1, second.size(), "Second EXECUTE should return 1 row");
            assertEquals("1", second.get(0).get(0));

            // Third execute with larger limit
            List<List<String>> third = query("EXECUTE p_limit(10)");
            assertEquals(3, third.size(), "Third EXECUTE should return all 3 rows");

            exec("DEALLOCATE p_limit");
        } finally {
            exec("DROP TABLE prep_t");
        }
    }

    @Test
    void re_execute_prepared_select_no_params() throws SQLException {
        exec("CREATE TABLE prep2_t(id int PRIMARY KEY, val text)");
        exec("INSERT INTO prep2_t VALUES (1, 'a'), (2, 'b')");
        try {
            exec("PREPARE p_all AS SELECT * FROM prep2_t ORDER BY id");

            List<List<String>> first = query("EXECUTE p_all");
            assertEquals(2, first.size());

            // Re-execute without params must also work
            List<List<String>> second = query("EXECUTE p_all");
            assertEquals(2, second.size());
            assertEquals("a", second.get(0).get(1));

            exec("DEALLOCATE p_all");
        } finally {
            exec("DROP TABLE prep2_t");
        }
    }

    @Test
    void execute_prepared_select_after_data_change() throws SQLException {
        exec("CREATE TABLE prep3_t(id int PRIMARY KEY, val int)");
        exec("INSERT INTO prep3_t VALUES (1, 100)");
        try {
            exec("PREPARE p_sel AS SELECT * FROM prep3_t ORDER BY id");

            List<List<String>> before = query("EXECUTE p_sel");
            assertEquals(1, before.size());

            // Insert more data, re-execute should see new rows
            exec("INSERT INTO prep3_t VALUES (2, 200)");
            List<List<String>> after = query("EXECUTE p_sel");
            assertEquals(2, after.size(), "Re-execute should see newly inserted row");

            exec("DEALLOCATE p_sel");
        } finally {
            exec("DROP TABLE prep3_t");
        }
    }

    @Test
    void execute_prepared_insert_then_select() throws SQLException {
        exec("CREATE TABLE prep4_t(id int PRIMARY KEY, val text)");
        try {
            exec("PREPARE p_ins(int, text) AS INSERT INTO prep4_t VALUES ($1, $2)");
            exec("PREPARE p_sel AS SELECT * FROM prep4_t ORDER BY id");

            exec("EXECUTE p_ins(1, 'hello')");
            exec("EXECUTE p_ins(2, 'world')");

            List<List<String>> rows = query("EXECUTE p_sel");
            assertEquals(2, rows.size());
            assertEquals("hello", rows.get(0).get(1));
            assertEquals("world", rows.get(1).get(1));

            exec("DEALLOCATE p_ins");
            exec("DEALLOCATE p_sel");
        } finally {
            exec("DROP TABLE prep4_t");
        }
    }

    // ========================================================================
    // CTE + FOR UPDATE SKIP LOCKED + UPDATE ... RETURNING (queue pattern)
    // ========================================================================

    @Test
    void cte_for_update_skip_locked_update_returning() throws SQLException {
        exec("CREATE TABLE work_item(id serial PRIMARY KEY, payload text, status text DEFAULT 'ready', priority int DEFAULT 0, attempts int DEFAULT 0)");
        exec("INSERT INTO work_item(payload, status, priority) VALUES ('job1', 'ready', 10), ('job2', 'ready', 5), ('job3', 'done', 1)");
        try {
            // Queue-like pattern: grab highest-priority ready item, mark as running, return it
            List<List<String>> result = query("""
                WITH next_item AS (
                    SELECT id FROM work_item
                    WHERE status = 'ready'
                    ORDER BY priority DESC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE work_item w
                SET status = 'running', attempts = attempts + 1
                FROM next_item n
                WHERE w.id = n.id
                RETURNING w.id, w.status, w.attempts, w.payload
                """);

            assertEquals(1, result.size(), "Should return exactly 1 row");
            assertEquals("running", result.get(0).get(1), "Status should be 'running'");
            assertEquals("1", result.get(0).get(2), "Attempts should be 1");
            assertEquals("job1", result.get(0).get(3), "Should pick highest-priority job");
        } finally {
            exec("DROP TABLE work_item");
        }
    }

    @Test
    void cte_for_update_with_returning_multiple_rows() throws SQLException {
        exec("CREATE TABLE task_q(id serial PRIMARY KEY, status text DEFAULT 'pending', data text)");
        exec("INSERT INTO task_q(status, data) VALUES ('pending', 'a'), ('pending', 'b'), ('pending', 'c'), ('done', 'd')");
        try {
            // Grab multiple pending items
            List<List<String>> result = query("""
                WITH batch AS (
                    SELECT id FROM task_q
                    WHERE status = 'pending'
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 2
                )
                UPDATE task_q t
                SET status = 'processing'
                FROM batch b
                WHERE t.id = b.id
                RETURNING t.id, t.status, t.data
                """);

            assertEquals(2, result.size(), "Should return 2 rows");
            assertEquals("processing", result.get(0).get(1));
            assertEquals("processing", result.get(1).get(1));

            // Remaining pending count
            String remaining = scalar("SELECT count(*) FROM task_q WHERE status = 'pending'");
            assertEquals("1", remaining, "1 pending item should remain");
        } finally {
            exec("DROP TABLE task_q");
        }
    }

    @Test
    void simple_for_update_skip_locked() throws SQLException {
        exec("CREATE TABLE lock_test(id int PRIMARY KEY, val text)");
        exec("INSERT INTO lock_test VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        try {
            List<List<String>> rows = query(
                    "SELECT * FROM lock_test ORDER BY id FOR UPDATE SKIP LOCKED");
            assertEquals(3, rows.size(), "FOR UPDATE SKIP LOCKED should return all rows in single session");
        } finally {
            exec("DROP TABLE lock_test");
        }
    }

    @Test
    void for_update_nowait() throws SQLException {
        exec("CREATE TABLE lock_test2(id int PRIMARY KEY, val text)");
        exec("INSERT INTO lock_test2 VALUES (1, 'x')");
        try {
            List<List<String>> rows = query(
                    "SELECT * FROM lock_test2 WHERE id = 1 FOR UPDATE NOWAIT");
            assertEquals(1, rows.size());
        } finally {
            exec("DROP TABLE lock_test2");
        }
    }

    // ========================================================================
    // EXECUTE with bad arguments
    // ========================================================================

    @Test
    void execute_with_wrong_arg_count_fails() throws SQLException {
        exec("CREATE TABLE prep5_t(id int)");
        try {
            exec("PREPARE p_one(int) AS SELECT * FROM prep5_t WHERE id = $1");
            // Wrong number of arguments
            assertThrows(SQLException.class,
                    () -> exec("EXECUTE p_one(1, 2)"),
                    "EXECUTE with too many args should fail");
            exec("DEALLOCATE p_one");
        } finally {
            exec("DROP TABLE prep5_t");
        }
    }

    @Test
    void execute_nonexistent_prepared_statement_fails() {
        assertThrows(SQLException.class,
                () -> exec("EXECUTE no_such_plan(1)"),
                "EXECUTE of non-existent prepared statement should fail");
    }

    @Test
    void deallocate_nonexistent_fails() {
        assertThrows(SQLException.class,
                () -> exec("DEALLOCATE no_such_plan"),
                "DEALLOCATE of non-existent plan should fail");
    }

    @Test
    void deallocate_all() throws SQLException {
        exec("PREPARE p_da1 AS SELECT 1");
        exec("PREPARE p_da2 AS SELECT 2");
        exec("DEALLOCATE ALL");
        // Both should be gone
        assertThrows(SQLException.class, () -> exec("EXECUTE p_da1"));
        assertThrows(SQLException.class, () -> exec("EXECUTE p_da2"));
    }

    // ========================================================================
    // UPDATE ... RETURNING without CTE
    // ========================================================================

    @Test
    void update_returning_basic() throws SQLException {
        exec("CREATE TABLE ret_t(id int PRIMARY KEY, val text)");
        exec("INSERT INTO ret_t VALUES (1, 'old'), (2, 'keep')");
        try {
            List<List<String>> result = query(
                    "UPDATE ret_t SET val = 'new' WHERE id = 1 RETURNING id, val");
            assertEquals(1, result.size());
            assertEquals("1", result.get(0).get(0));
            assertEquals("new", result.get(0).get(1));
        } finally {
            exec("DROP TABLE ret_t");
        }
    }

    @Test
    void delete_returning() throws SQLException {
        exec("CREATE TABLE dret_t(id int PRIMARY KEY, val text)");
        exec("INSERT INTO dret_t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        try {
            List<List<String>> result = query(
                    "DELETE FROM dret_t WHERE id <= 2 RETURNING id, val");
            assertEquals(2, result.size(), "DELETE RETURNING should return deleted rows");
        } finally {
            exec("DROP TABLE dret_t");
        }
    }
}
