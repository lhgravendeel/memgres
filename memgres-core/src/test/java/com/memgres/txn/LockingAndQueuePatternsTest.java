package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for row-level locking clauses, LOCK TABLE modes, and CTE-based queue patterns.
 *
 * Covers known PG 18 vs Memgres compatibility differences:
 *
 * diff 64, FOR UPDATE FOR SHARE (multiple locking clauses):
 *   PG 18 accepts this as valid syntax. Memgres gives syntax error.
 *
 * diff 63, CTE queue pattern crash:
 *   WITH next_item AS (... FOR UPDATE SKIP LOCKED) UPDATE ... WHERE id = (SELECT id FROM next_item) RETURNING *
 *   PG 18 succeeds. Memgres crashes with
 *   "IllegalStateException: Received resultset tuples, but no field structure for them".
 *
 * diff 56, EXECUTE crash:
 *   EXECUTE p_limit(1) after PREPARE gives the same IllegalStateException.
 *
 * Additional locking coverage:
 *   FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, FOR KEY SHARE,
 *   FOR UPDATE OF table, FOR UPDATE SKIP LOCKED, FOR UPDATE NOWAIT,
 *   FOR UPDATE in subquery, LOCK TABLE with all standard modes,
 *   full queue pattern with insert/dequeue/RETURNING.
 */
class LockingAndQueuePatternsTest {

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
    // diff 64: FOR UPDATE FOR SHARE (multiple locking clauses, PG 18 valid)
    // ========================================================================

    /**
     * PG 18: SELECT * FROM work_item FOR UPDATE FOR SHARE is valid syntax with
     * multiple locking clauses. Memgres currently gives a syntax error.
     * This test documents the expected behavior: the statement succeeds and
     * returns rows.
     */
    @Test
    void for_update_for_share_multiple_locking_clauses() throws SQLException {
        exec("CREATE TABLE wi_fuf(id int PRIMARY KEY, status text)");
        exec("INSERT INTO wi_fuf VALUES (1,'pending'),(2,'pending')");
        try {
            conn.setAutoCommit(false);
            try {
                List<List<String>> rows = query("SELECT * FROM wi_fuf FOR UPDATE FOR SHARE");
                assertFalse(rows.isEmpty(), "FOR UPDATE FOR SHARE should return rows");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_fuf");
        }
    }

    // ========================================================================
    // diff 63: CTE queue pattern crash
    // ========================================================================

    /**
     * PG 18: CTE-based dequeue with FOR UPDATE SKIP LOCKED feeding an UPDATE RETURNING
     * succeeds without error and returns the updated row.
     * Memgres currently crashes with:
     *   IllegalStateException: Received resultset tuples, but no field structure for them
     */
    @Test
    void cte_queue_pattern_update_returning() throws SQLException {
        exec("CREATE TABLE wi_cte(id int PRIMARY KEY, status text)");
        exec("INSERT INTO wi_cte VALUES (1,'pending'),(2,'pending')");
        try {
            conn.setAutoCommit(false);
            try {
                List<List<String>> rows = query("""
                    WITH next_item AS (
                      SELECT id FROM wi_cte WHERE status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
                    )
                    UPDATE wi_cte SET status = 'processing'
                    WHERE id = (SELECT id FROM next_item)
                    RETURNING *
                    """);
                assertEquals(1, rows.size(), "CTE queue UPDATE RETURNING should return exactly one row");
                assertEquals("1", rows.get(0).get(0), "Should have dequeued item with id=1");
                assertEquals("processing", rows.get(0).get(1), "Status should be 'processing'");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_cte");
        }
    }

    // ========================================================================
    // diff 56: PREPARE / EXECUTE crash
    // ========================================================================

    /**
     * PG 18: PREPARE + EXECUTE with a parameter used in LIMIT succeeds and returns
     * result rows without error.
     * Memgres currently crashes with:
     *   IllegalStateException: Received resultset tuples, but no field structure for them
     */
    @Test
    void prepare_execute_with_limit_parameter() throws SQLException {
        exec("CREATE TABLE wi_prep(id int PRIMARY KEY, status text)");
        exec("INSERT INTO wi_prep VALUES (1,'pending'),(2,'pending'),(3,'pending')");
        try {
            exec("PREPARE p_limit(int) AS SELECT * FROM wi_prep ORDER BY id LIMIT $1");
            List<List<String>> rows = query("EXECUTE p_limit(1)");
            assertEquals(1, rows.size(), "EXECUTE with LIMIT 1 should return exactly one row");
            assertEquals("1", rows.get(0).get(0), "Should return first row");
        } finally {
            exec("DEALLOCATE p_limit");
            exec("DROP TABLE wi_prep");
        }
    }

    // ========================================================================
    // Basic locking clause variants
    // ========================================================================

    @Test
    void for_update_basic() throws SQLException {
        exec("CREATE TABLE wi_fu(id int PRIMARY KEY, status text)");
        exec("INSERT INTO wi_fu VALUES (1,'pending'),(2,'done')");
        try {
            conn.setAutoCommit(false);
            try {
                List<List<String>> rows = query("SELECT * FROM wi_fu ORDER BY id FOR UPDATE");
                assertEquals(2, rows.size(), "FOR UPDATE should return all rows");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_fu");
        }
    }

    @Test
    void for_no_key_update() throws SQLException {
        exec("CREATE TABLE wi_fnku(id int PRIMARY KEY, status text)");
        exec("INSERT INTO wi_fnku VALUES (1,'pending')");
        try {
            conn.setAutoCommit(false);
            try {
                List<List<String>> rows = query("SELECT * FROM wi_fnku FOR NO KEY UPDATE");
                assertFalse(rows.isEmpty(), "FOR NO KEY UPDATE should return rows");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_fnku");
        }
    }

    @Test
    void for_share() throws SQLException {
        exec("CREATE TABLE wi_fs(id int PRIMARY KEY, status text)");
        exec("INSERT INTO wi_fs VALUES (1,'pending')");
        try {
            conn.setAutoCommit(false);
            try {
                List<List<String>> rows = query("SELECT * FROM wi_fs FOR SHARE");
                assertFalse(rows.isEmpty(), "FOR SHARE should return rows");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_fs");
        }
    }

    @Test
    void for_key_share() throws SQLException {
        exec("CREATE TABLE wi_fks(id int PRIMARY KEY, status text)");
        exec("INSERT INTO wi_fks VALUES (1,'pending')");
        try {
            conn.setAutoCommit(false);
            try {
                List<List<String>> rows = query("SELECT * FROM wi_fks FOR KEY SHARE");
                assertFalse(rows.isEmpty(), "FOR KEY SHARE should return rows");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_fks");
        }
    }

    @Test
    void for_update_of_specific_table() throws SQLException {
        exec("CREATE TABLE wi_fuo(id int PRIMARY KEY, status text)");
        exec("CREATE TABLE wi_fuo_ref(id int PRIMARY KEY, wi_id int)");
        exec("INSERT INTO wi_fuo VALUES (1,'pending')");
        exec("INSERT INTO wi_fuo_ref VALUES (10, 1)");
        try {
            conn.setAutoCommit(false);
            try {
                List<List<String>> rows = query(
                    "SELECT w.*, r.id AS rid FROM wi_fuo w JOIN wi_fuo_ref r ON r.wi_id = w.id FOR UPDATE OF wi_fuo");
                assertFalse(rows.isEmpty(), "FOR UPDATE OF table should return rows");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_fuo_ref");
            exec("DROP TABLE wi_fuo");
        }
    }

    @Test
    void for_update_skip_locked() throws SQLException {
        exec("CREATE TABLE wi_fusl(id int PRIMARY KEY, status text)");
        exec("INSERT INTO wi_fusl VALUES (1,'pending'),(2,'pending')");
        try {
            conn.setAutoCommit(false);
            try {
                List<List<String>> rows = query("SELECT * FROM wi_fusl ORDER BY id FOR UPDATE SKIP LOCKED");
                assertFalse(rows.isEmpty(), "FOR UPDATE SKIP LOCKED should return available rows");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_fusl");
        }
    }

    @Test
    void for_update_nowait() throws SQLException {
        exec("CREATE TABLE wi_funw(id int PRIMARY KEY, status text)");
        exec("INSERT INTO wi_funw VALUES (1,'pending')");
        try {
            conn.setAutoCommit(false);
            try {
                List<List<String>> rows = query("SELECT * FROM wi_funw FOR UPDATE NOWAIT");
                assertFalse(rows.isEmpty(), "FOR UPDATE NOWAIT should return rows when no contention");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_funw");
        }
    }

    @Test
    void for_update_in_subquery() throws SQLException {
        exec("CREATE TABLE wi_fusq(id int PRIMARY KEY, priority int, status text)");
        exec("INSERT INTO wi_fusq VALUES (1,10,'pending'),(2,20,'pending'),(3,5,'pending')");
        try {
            conn.setAutoCommit(false);
            try {
                // FOR UPDATE inside a subquery selects from the locked set
                List<List<String>> rows = query("""
                    SELECT * FROM (
                      SELECT * FROM wi_fusq WHERE status = 'pending' FOR UPDATE
                    ) sub ORDER BY priority
                    """);
                assertEquals(3, rows.size(), "Subquery with FOR UPDATE should return all pending rows");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE wi_fusq");
        }
    }

    // ========================================================================
    // LOCK TABLE: all standard modes
    // ========================================================================

    @Test
    void lock_table_access_share() throws SQLException {
        exec("CREATE TABLE lt_as(id int PRIMARY KEY)");
        exec("INSERT INTO lt_as VALUES (1)");
        try {
            conn.setAutoCommit(false);
            try {
                exec("LOCK TABLE lt_as IN ACCESS SHARE MODE");
                assertEquals("1", scalar("SELECT count(*) FROM lt_as"), "Should read after ACCESS SHARE lock");
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE lt_as");
        }
    }

    @Test
    void lock_table_row_share() throws SQLException {
        exec("CREATE TABLE lt_rs(id int PRIMARY KEY)");
        exec("INSERT INTO lt_rs VALUES (1)");
        try {
            conn.setAutoCommit(false);
            try {
                exec("LOCK TABLE lt_rs IN ROW SHARE MODE");
                assertEquals("1", scalar("SELECT count(*) FROM lt_rs"));
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE lt_rs");
        }
    }

    @Test
    void lock_table_row_exclusive() throws SQLException {
        exec("CREATE TABLE lt_re(id int PRIMARY KEY)");
        exec("INSERT INTO lt_re VALUES (1)");
        try {
            conn.setAutoCommit(false);
            try {
                exec("LOCK TABLE lt_re IN ROW EXCLUSIVE MODE");
                assertEquals("1", scalar("SELECT count(*) FROM lt_re"));
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE lt_re");
        }
    }

    @Test
    void lock_table_share_update_exclusive() throws SQLException {
        exec("CREATE TABLE lt_sue(id int PRIMARY KEY)");
        exec("INSERT INTO lt_sue VALUES (1)");
        try {
            conn.setAutoCommit(false);
            try {
                exec("LOCK TABLE lt_sue IN SHARE UPDATE EXCLUSIVE MODE");
                assertEquals("1", scalar("SELECT count(*) FROM lt_sue"));
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE lt_sue");
        }
    }

    @Test
    void lock_table_share() throws SQLException {
        exec("CREATE TABLE lt_sh(id int PRIMARY KEY)");
        exec("INSERT INTO lt_sh VALUES (1)");
        try {
            conn.setAutoCommit(false);
            try {
                exec("LOCK TABLE lt_sh IN SHARE MODE");
                assertEquals("1", scalar("SELECT count(*) FROM lt_sh"));
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE lt_sh");
        }
    }

    @Test
    void lock_table_share_row_exclusive() throws SQLException {
        exec("CREATE TABLE lt_sre(id int PRIMARY KEY)");
        exec("INSERT INTO lt_sre VALUES (1)");
        try {
            conn.setAutoCommit(false);
            try {
                exec("LOCK TABLE lt_sre IN SHARE ROW EXCLUSIVE MODE");
                assertEquals("1", scalar("SELECT count(*) FROM lt_sre"));
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE lt_sre");
        }
    }

    @Test
    void lock_table_exclusive() throws SQLException {
        exec("CREATE TABLE lt_ex(id int PRIMARY KEY)");
        exec("INSERT INTO lt_ex VALUES (1)");
        try {
            conn.setAutoCommit(false);
            try {
                exec("LOCK TABLE lt_ex IN EXCLUSIVE MODE");
                assertEquals("1", scalar("SELECT count(*) FROM lt_ex"));
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE lt_ex");
        }
    }

    @Test
    void lock_table_access_exclusive() throws SQLException {
        exec("CREATE TABLE lt_ae(id int PRIMARY KEY)");
        exec("INSERT INTO lt_ae VALUES (1)");
        try {
            conn.setAutoCommit(false);
            try {
                exec("LOCK TABLE lt_ae IN ACCESS EXCLUSIVE MODE");
                // After ACCESS EXCLUSIVE, we can still run DML within the same transaction
                exec("INSERT INTO lt_ae VALUES (2)");
                assertEquals("2", scalar("SELECT count(*) FROM lt_ae"));
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } finally {
            exec("DROP TABLE lt_ae");
        }
    }

    // ========================================================================
    // Full queue pattern: insert items → dequeue with CTE + FOR UPDATE SKIP LOCKED + RETURNING
    // ========================================================================

    /**
     * Full round-trip queue pattern:
     * 1. Insert several work items with status='pending'.
     * 2. Use a CTE with FOR UPDATE SKIP LOCKED to atomically pick and claim the next item.
     * 3. Verify the dequeued item transitions to 'processing' and RETURNING delivers the row.
     * 4. Dequeue again; verify a different item is claimed.
     * 5. Verify remaining pending count.
     */
    @Test
    void queue_pattern_insert_dequeue_returning() throws SQLException {
        exec("CREATE TABLE wi_queue(id int PRIMARY KEY, payload text, status text DEFAULT 'pending')");
        exec("INSERT INTO wi_queue(id, payload) VALUES (1,'job-a'),(2,'job-b'),(3,'job-c')");
        try {
            // First dequeue
            conn.setAutoCommit(false);
            List<List<String>> first;
            try {
                first = query("""
                    WITH next_item AS (
                      SELECT id FROM wi_queue WHERE status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
                    )
                    UPDATE wi_queue SET status = 'processing'
                    WHERE id = (SELECT id FROM next_item)
                    RETURNING id, payload, status
                    """);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }

            assertEquals(1, first.size(), "First dequeue should return exactly one item");
            assertEquals("1", first.get(0).get(0), "First dequeue should pick id=1 (lowest)");
            assertEquals("job-a", first.get(0).get(1));
            assertEquals("processing", first.get(0).get(2));

            // Second dequeue
            conn.setAutoCommit(false);
            List<List<String>> second;
            try {
                second = query("""
                    WITH next_item AS (
                      SELECT id FROM wi_queue WHERE status = 'pending' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
                    )
                    UPDATE wi_queue SET status = 'processing'
                    WHERE id = (SELECT id FROM next_item)
                    RETURNING id, payload, status
                    """);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }

            assertEquals(1, second.size(), "Second dequeue should return exactly one item");
            assertEquals("2", second.get(0).get(0), "Second dequeue should pick id=2");
            assertEquals("job-b", second.get(0).get(1));

            // Verify remaining pending
            assertEquals("1", scalar("SELECT count(*) FROM wi_queue WHERE status = 'pending'"),
                "One item should remain pending after two dequeues");
            assertEquals("2", scalar("SELECT count(*) FROM wi_queue WHERE status = 'processing'"),
                "Two items should be processing");
        } finally {
            exec("DROP TABLE wi_queue");
        }
    }
}
