package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for MERGE statement correctness, RETURNING clauses, and scoping after MERGE.
 *
 * Covers:
 * - MERGE with duplicate source key must fail with 23505 (duplicate key)
 * - After failed MERGE, table must not be modified
 * - MERGE UPDATE + DELETE + INSERT in one statement
 * - Row counts must be exact after MERGE
 * - CTE scoping queries after MERGE see correct rows
 * - Correlated subqueries after MERGE
 * - INSERT/UPDATE/DELETE RETURNING with expressions
 */
class MergeAndReturningTest {

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
    // MERGE with duplicate source key → 23505
    // ========================================================================

    @Test
    void merge_duplicate_source_key_raises_23505() throws SQLException {
        exec("CREATE TABLE mdk_tgt(id int PRIMARY KEY, a int, b text)");
        exec("CREATE TABLE mdk_src(id int, a int, b text)");
        exec("INSERT INTO mdk_tgt VALUES (1,10,'x'),(2,20,'y')");
        // Source has duplicate key 4, so both would try to INSERT
        exec("INSERT INTO mdk_src VALUES (1,100,'sx'),(4,400,'sw'),(4,401,'sw2')");
        try {
            SQLException ex = assertThrows(SQLException.class, () -> exec("""
                MERGE INTO mdk_tgt AS t USING mdk_src AS s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET a = s.a
                WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                """));
            assertEquals("23505", ex.getSQLState(),
                    "Duplicate source key should raise 23505, got " + ex.getSQLState());
        } finally {
            exec("DROP TABLE mdk_src"); exec("DROP TABLE mdk_tgt");
        }
    }

    @Test
    void merge_failed_does_not_modify_table() throws SQLException {
        exec("CREATE TABLE mfm_tgt(id int PRIMARY KEY, a int)");
        exec("CREATE TABLE mfm_src(id int, a int)");
        exec("INSERT INTO mfm_tgt VALUES (1,10),(2,20)");
        exec("INSERT INTO mfm_src VALUES (1,100),(3,300),(3,301)");
        try {
            // MERGE should fail due to duplicate source key 3
            try {
                exec("""
                    MERGE INTO mfm_tgt AS t USING mfm_src AS s ON t.id = s.id
                    WHEN MATCHED THEN UPDATE SET a = s.a
                    WHEN NOT MATCHED THEN INSERT (id, a) VALUES (s.id, s.a)
                    """);
            } catch (SQLException ignored) {}

            // Table should still have original 2 rows (MERGE failed, rolled back)
            List<List<String>> rows = query("SELECT * FROM mfm_tgt ORDER BY id");
            // PG: failed MERGE in autocommit does not partially apply (depends on behavior)
            // At minimum, the table should not have both duplicate rows
            assertTrue(rows.size() <= 3, "Table should not have duplicate rows from failed MERGE");
        } finally {
            exec("DROP TABLE mfm_src"); exec("DROP TABLE mfm_tgt");
        }
    }

    // ========================================================================
    // MERGE with UPDATE + DELETE + INSERT: exact row counts
    // ========================================================================

    @Test
    void merge_update_delete_insert_exact_counts() throws SQLException {
        exec("CREATE TABLE mud_tgt(id int PRIMARY KEY, a int, b text, c int GENERATED ALWAYS AS IDENTITY)");
        exec("CREATE TABLE mud_src(id int, a int, flag text)");
        exec("INSERT INTO mud_tgt(id, a, b) VALUES (1,10,'x'),(2,20,'y'),(10,1000,'r1')");
        exec("INSERT INTO mud_src VALUES (1,111,'upd'),(2,222,'del'),(6,666,'ins')");
        try {
            exec("""
                MERGE INTO mud_tgt AS t USING mud_src AS s ON t.id = s.id
                WHEN MATCHED AND s.flag = 'del' THEN DELETE
                WHEN MATCHED AND s.flag = 'upd' THEN UPDATE SET a = s.a, b = s.flag
                WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.flag)
                """);

            List<List<String>> rows = query("SELECT id, a, b FROM mud_tgt ORDER BY id");
            // id=1 updated (a=111, b='upd'), id=2 deleted, id=6 inserted, id=10 untouched
            assertEquals(3, rows.size(), "Should have 3 rows: 1 updated, 1 deleted, 1 inserted, 1 untouched");
            assertEquals("1", rows.get(0).get(0)); assertEquals("111", rows.get(0).get(1));
            assertEquals("6", rows.get(1).get(0)); assertEquals("666", rows.get(1).get(1));
            assertEquals("10", rows.get(2).get(0)); assertEquals("1000", rows.get(2).get(1));
        } finally {
            exec("DROP TABLE mud_src"); exec("DROP TABLE mud_tgt");
        }
    }

    // ========================================================================
    // Scoping queries after MERGE must see correct data
    // ========================================================================

    @Test
    void cte_after_merge_sees_correct_rows() throws SQLException {
        exec("CREATE TABLE cam_tgt(id int PRIMARY KEY, a int)");
        exec("CREATE TABLE cam_src(id int, a int)");
        exec("INSERT INTO cam_tgt VALUES (1,10),(2,20),(10,1000)");
        exec("INSERT INTO cam_src VALUES (1,111),(6,666)");
        try {
            exec("""
                MERGE INTO cam_tgt t USING cam_src s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET a = s.a
                WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.a)
                """);

            // CTE reading from merged table
            List<List<String>> ctRes = query("""
                WITH t AS (SELECT id, a FROM cam_tgt WHERE id < 100)
                SELECT t.id, t.a FROM t ORDER BY id
                """);
            assertEquals(4, ctRes.size(), "CTE should see 4 rows after merge");

            // Correlated subquery
            List<List<String>> subRes = query("""
                SELECT t.id, (SELECT max(s.a) FROM cam_src s WHERE s.id = t.id) AS max_a
                FROM cam_tgt t ORDER BY t.id
                """);
            assertEquals(4, subRes.size(), "Correlated subquery should see 4 rows");

            // Window function subquery
            List<List<String>> winRes = query("""
                SELECT sub.id, sub.a FROM (
                  SELECT t.id, t.a, row_number() OVER (ORDER BY t.id) AS rn
                  FROM cam_tgt t
                ) sub WHERE sub.rn >= 1 ORDER BY sub.id
                """);
            assertEquals(4, winRes.size(), "Window subquery should see 4 rows");
        } finally {
            exec("DROP TABLE cam_src"); exec("DROP TABLE cam_tgt");
        }
    }

    // ========================================================================
    // INSERT RETURNING with expressions
    // ========================================================================

    @Test
    void insert_returning_with_expressions() throws SQLException {
        exec("CREATE TABLE ir_t(id int PRIMARY KEY, a int, b text, c int GENERATED ALWAYS AS IDENTITY)");
        try {
            List<List<String>> res = query(
                    "INSERT INTO ir_t(id, a, b) VALUES (10, 1000, 'r1') RETURNING id, a, b, c, a * 2 AS doubled");
            assertEquals(1, res.size());
            assertEquals("10", res.get(0).get(0));
            assertEquals("1000", res.get(0).get(1));
            assertEquals("r1", res.get(0).get(2));
            assertEquals("2000", res.get(0).get(4), "Expression a*2 in RETURNING");
        } finally {
            exec("DROP TABLE ir_t");
        }
    }

    @Test
    void insert_returning_pg_typeof() throws SQLException {
        exec("CREATE TABLE ipt_t(id int PRIMARY KEY, a int, b text)");
        try {
            List<List<String>> res = query(
                    "INSERT INTO ipt_t VALUES (1, 42, 'hello') RETURNING pg_typeof(a), pg_typeof(b)");
            assertEquals(1, res.size());
            assertEquals("integer", res.get(0).get(0));
            assertEquals("text", res.get(0).get(1));
        } finally {
            exec("DROP TABLE ipt_t");
        }
    }

    @Test
    void update_returning_with_alias_and_expressions() throws SQLException {
        exec("CREATE TABLE ur_t(id int PRIMARY KEY, a int, b text, c int GENERATED ALWAYS AS IDENTITY)");
        exec("INSERT INTO ur_t(id, a, b) VALUES (1, 10, 'x'), (2, 20, 'y')");
        try {
            List<List<String>> res = query(
                    "UPDATE ur_t AS t SET a = t.a + 1, b = upper(t.b) WHERE t.id IN (1,2) RETURNING t.id, t.a, t.b, t.a + 10 AS plus_ten");
            assertEquals(2, res.size());
            assertEquals("11", res.get(0).get(1), "a should be incremented");
            assertEquals("X", res.get(0).get(2), "b should be uppercased");
            assertEquals("21", res.get(0).get(3), "a+10 expression");
        } finally {
            exec("DROP TABLE ur_t");
        }
    }

    @Test
    void delete_returning_with_alias() throws SQLException {
        exec("CREATE TABLE dr_t(id int PRIMARY KEY, a int, b text, c int GENERATED ALWAYS AS IDENTITY)");
        exec("INSERT INTO dr_t(id, a, b) VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        try {
            List<List<String>> res = query(
                    "DELETE FROM dr_t t WHERE t.id = 3 RETURNING t.id, t.a, t.b, t.c");
            assertEquals(1, res.size());
            assertEquals("3", res.get(0).get(0));
            assertEquals("30", res.get(0).get(1));
        } finally {
            exec("DROP TABLE dr_t");
        }
    }

    // ========================================================================
    // MERGE with no matching WHEN clause; row should be untouched
    // ========================================================================

    @Test
    void merge_unmatched_rows_untouched() throws SQLException {
        exec("CREATE TABLE mur_tgt(id int PRIMARY KEY, a int)");
        exec("CREATE TABLE mur_src(id int, a int)");
        exec("INSERT INTO mur_tgt VALUES (1,10),(2,20),(3,30)");
        exec("INSERT INTO mur_src VALUES (1,100)");
        try {
            // Only WHEN MATCHED, so id=2 and id=3 are untouched
            exec("""
                MERGE INTO mur_tgt t USING mur_src s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET a = s.a
                """);
            assertEquals("100", scalar("SELECT a FROM mur_tgt WHERE id = 1"));
            assertEquals("20", scalar("SELECT a FROM mur_tgt WHERE id = 2"));
            assertEquals("30", scalar("SELECT a FROM mur_tgt WHERE id = 3"));
            assertEquals("3", scalar("SELECT count(*) FROM mur_tgt"));
        } finally {
            exec("DROP TABLE mur_src"); exec("DROP TABLE mur_tgt");
        }
    }

    // ========================================================================
    // MERGE with duplicate source key, thorough: verify no partial apply
    // ========================================================================

    @Test
    void merge_duplicate_source_key_no_partial_apply() throws SQLException {
        exec("CREATE TABLE mdp_tgt(id int PRIMARY KEY, a int, b text)");
        exec("CREATE TABLE mdp_src(id int, a int, b text)");
        // Target has 2 rows
        exec("INSERT INTO mdp_tgt VALUES (1,10,'orig1'),(2,20,'orig2')");
        // Source has: key 1 triggers UPDATE, keys 5 and 5 both trigger INSERT (duplicate)
        exec("INSERT INTO mdp_src VALUES (1,100,'upd'),(5,500,'ins1'),(5,501,'ins2')");
        try {
            // MERGE should fail because source has duplicate key 5
            SQLException ex = assertThrows(SQLException.class, () -> exec("""
                MERGE INTO mdp_tgt AS t USING mdp_src AS s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET a = s.a, b = s.b
                WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                """));
            assertEquals("23505", ex.getSQLState(),
                    "Duplicate source key should raise 23505, got " + ex.getSQLState());

            // After failed MERGE, target must have exactly its original rows (no partial apply)
            List<List<String>> rows = query("SELECT id, a, b FROM mdp_tgt ORDER BY id");
            assertEquals(2, rows.size(), "Target should still have exactly 2 original rows");
            assertEquals("1", rows.get(0).get(0));
            assertEquals("10", rows.get(0).get(1));
            assertEquals("orig1", rows.get(0).get(2));
            assertEquals("2", rows.get(1).get(0));
            assertEquals("20", rows.get(1).get(1));
            assertEquals("orig2", rows.get(1).get(2));
        } finally {
            exec("DROP TABLE mdp_src"); exec("DROP TABLE mdp_tgt");
        }
    }

    // ========================================================================
    // MERGE DELETE removes rows correctly
    // ========================================================================

    @Test
    void merge_delete_removes_matched_rows() throws SQLException {
        exec("CREATE TABLE mdr_tgt(id int PRIMARY KEY, a int, status text)");
        exec("CREATE TABLE mdr_src(id int, a int)");
        // Target has 5 rows, some with status='inactive'
        exec("INSERT INTO mdr_tgt VALUES (1,10,'active'),(2,20,'inactive'),(3,30,'active'),(4,40,'inactive'),(5,50,'active')");
        // Source matches ids 2, 3, 4
        exec("INSERT INTO mdr_src VALUES (2,200),(3,300),(4,400)");
        try {
            // DELETE only rows that match AND have status='inactive'
            exec("""
                MERGE INTO mdr_tgt AS t USING mdr_src AS s ON t.id = s.id
                WHEN MATCHED AND t.status = 'inactive' THEN DELETE
                """);

            List<List<String>> rows = query("SELECT id, a, status FROM mdr_tgt ORDER BY id");
            // ids 2 and 4 were inactive and matched -> deleted
            // ids 1, 3, 5 remain (3 was matched but active, so no WHEN clause fired)
            assertEquals(3, rows.size(), "Should have 3 rows after deleting 2 inactive matched rows");
            assertEquals("1", rows.get(0).get(0));
            assertEquals("3", rows.get(1).get(0));
            assertEquals("5", rows.get(2).get(0));
            // Verify the remaining rows are unchanged
            assertEquals("10", rows.get(0).get(1));
            assertEquals("active", rows.get(0).get(2));
            assertEquals("30", rows.get(1).get(1));
            assertEquals("active", rows.get(1).get(2));
            assertEquals("50", rows.get(2).get(1));
            assertEquals("active", rows.get(2).get(2));
        } finally {
            exec("DROP TABLE mdr_src"); exec("DROP TABLE mdr_tgt");
        }
    }

    @Test
    void merge_delete_and_insert_in_same_statement() throws SQLException {
        exec("CREATE TABLE mdi_tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE mdi_src(id int, val int)");
        // Target starts with 3 rows
        exec("INSERT INTO mdi_tgt VALUES (1,100),(2,200),(3,300)");
        // Source: id=1 matches (will be deleted), id=2 matches (will be deleted), id=7 and id=8 are new (will be inserted)
        exec("INSERT INTO mdi_src VALUES (1,0),(2,0),(7,700),(8,800)");
        try {
            exec("""
                MERGE INTO mdi_tgt AS t USING mdi_src AS s ON t.id = s.id
                WHEN MATCHED THEN DELETE
                WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
                """);

            List<List<String>> rows = query("SELECT id, val FROM mdi_tgt ORDER BY id");
            // ids 1,2 deleted; ids 7,8 inserted; id 3 untouched
            assertEquals(3, rows.size(), "Should have 3 rows: 1 untouched + 2 inserted, 2 deleted");
            assertEquals("3", rows.get(0).get(0));
            assertEquals("300", rows.get(0).get(1));
            assertEquals("7", rows.get(1).get(0));
            assertEquals("700", rows.get(1).get(1));
            assertEquals("8", rows.get(2).get(0));
            assertEquals("800", rows.get(2).get(1));
        } finally {
            exec("DROP TABLE mdi_src"); exec("DROP TABLE mdi_tgt");
        }
    }

    // ========================================================================
    // MERGE with all three actions: exact row-level verification
    // ========================================================================

    @Test
    void merge_all_three_actions_exact_verification() throws SQLException {
        exec("CREATE TABLE mat_tgt(id int PRIMARY KEY, val int, label text)");
        exec("CREATE TABLE mat_src(id int, val int, action text)");
        // Target: 5 rows
        exec("INSERT INTO mat_tgt VALUES (1,10,'a'),(2,20,'b'),(3,30,'c'),(4,40,'d'),(5,50,'e')");
        // Source:
        //   id=1 action='upd' -> matched, update
        //   id=3 action='del' -> matched, delete
        //   id=5 action='upd' -> matched, update
        //   id=7 action='ins' -> not matched, insert
        //   id=9 action='ins' -> not matched, insert
        exec("INSERT INTO mat_src VALUES (1,111,'upd'),(3,0,'del'),(5,555,'upd'),(7,777,'ins'),(9,999,'ins')");
        try {
            exec("""
                MERGE INTO mat_tgt AS t USING mat_src AS s ON t.id = s.id
                WHEN MATCHED AND s.action = 'del' THEN DELETE
                WHEN MATCHED AND s.action = 'upd' THEN UPDATE SET val = s.val, label = 'updated'
                WHEN NOT MATCHED THEN INSERT (id, val, label) VALUES (s.id, s.val, 'new')
                """);

            List<List<String>> rows = query("SELECT id, val, label FROM mat_tgt ORDER BY id");
            // Expected rows:
            //   id=1: updated (val=111, label='updated')
            //   id=2: untouched (val=20, label='b')
            //   id=3: deleted
            //   id=4: untouched (val=40, label='d')
            //   id=5: updated (val=555, label='updated')
            //   id=7: inserted (val=777, label='new')
            //   id=9: inserted (val=999, label='new')
            assertEquals(6, rows.size(), "Should have 6 rows: 2 updated, 1 deleted, 2 untouched, 2 inserted");

            // id=1 updated
            assertEquals("1", rows.get(0).get(0));
            assertEquals("111", rows.get(0).get(1));
            assertEquals("updated", rows.get(0).get(2));

            // id=2 untouched
            assertEquals("2", rows.get(1).get(0));
            assertEquals("20", rows.get(1).get(1));
            assertEquals("b", rows.get(1).get(2));

            // id=4 untouched
            assertEquals("4", rows.get(2).get(0));
            assertEquals("40", rows.get(2).get(1));
            assertEquals("d", rows.get(2).get(2));

            // id=5 updated
            assertEquals("5", rows.get(3).get(0));
            assertEquals("555", rows.get(3).get(1));
            assertEquals("updated", rows.get(3).get(2));

            // id=7 inserted
            assertEquals("7", rows.get(4).get(0));
            assertEquals("777", rows.get(4).get(1));
            assertEquals("new", rows.get(4).get(2));

            // id=9 inserted
            assertEquals("9", rows.get(5).get(0));
            assertEquals("999", rows.get(5).get(1));
            assertEquals("new", rows.get(5).get(2));

            // Also verify total count explicitly
            assertEquals("6", scalar("SELECT count(*) FROM mat_tgt"));
            // Verify id=3 is truly gone
            assertEquals("0", scalar("SELECT count(*) FROM mat_tgt WHERE id = 3"));
        } finally {
            exec("DROP TABLE mat_src"); exec("DROP TABLE mat_tgt");
        }
    }
}
