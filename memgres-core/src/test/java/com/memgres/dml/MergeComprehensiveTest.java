package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for SQL MERGE statement behavior.
 *
 * The core bug: MERGE inserts rows for source rows that match the ON condition
 * (should UPDATE instead), producing duplicate rows. This test file exercises
 * every combination of MERGE actions and validates exact row counts.
 */
class MergeComprehensiveTest {

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

    static int count(String table) throws SQLException {
        return query("SELECT count(*) FROM " + table).get(0).get(0).equals("0") ? 0
                : Integer.parseInt(query("SELECT count(*) FROM " + table).get(0).get(0));
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Core: matched rows should UPDATE, not INSERT duplicates
    // ========================================================================

    @Test
    void merge_matched_updates_not_inserts() throws SQLException {
        exec("CREATE TABLE m1_tgt(id int PRIMARY KEY, a int, b text)");
        exec("CREATE TABLE m1_src(id int, a int, b text)");
        exec("INSERT INTO m1_tgt VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')");
        exec("INSERT INTO m1_src VALUES (1,100,'sx'),(4,400,'sw'),(5,NULL,NULL)");
        try {
            exec("""
                MERGE INTO m1_tgt AS t
                USING m1_src AS s
                ON t.id = s.id
                WHEN MATCHED AND s.a IS NOT NULL THEN
                  UPDATE SET a = s.a, b = coalesce(s.b, t.b)
                WHEN NOT MATCHED AND s.id IS NOT NULL THEN
                  INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                """);

            // id=1 matched → UPDATE. ids 2,3 no match in src → untouched.
            // id=4 not matched → INSERT. id=5 not matched, id IS NOT NULL → INSERT.
            // Total: 3 original + 2 inserted = 5
            List<List<String>> rows = query("SELECT * FROM m1_tgt ORDER BY id");
            assertEquals(5, rows.size(), "Should have exactly 5 rows after MERGE");
            assertEquals("100", rows.get(0).get(1), "id=1 should be updated to a=100");
            assertEquals("20", rows.get(1).get(1), "id=2 should be untouched");
            assertEquals("30", rows.get(2).get(1), "id=3 should be untouched");
            assertEquals("400", rows.get(3).get(1), "id=4 should be inserted");
        } finally {
            exec("DROP TABLE m1_src"); exec("DROP TABLE m1_tgt");
        }
    }

    @Test
    void merge_second_pass_with_delete() throws SQLException {
        exec("CREATE TABLE m2_tgt(id int PRIMARY KEY, a int, b text)");
        exec("CREATE TABLE m2_src(id int, a int, flag text)");
        exec("INSERT INTO m2_tgt VALUES (1,10,'x'),(2,20,'y'),(4,400,'sw'),(5,NULL,NULL)");
        exec("INSERT INTO m2_src VALUES (1,111,'upd'),(2,222,'del'),(6,666,'ins')");
        try {
            exec("""
                MERGE INTO m2_tgt AS t
                USING m2_src AS s
                ON t.id = s.id
                WHEN MATCHED AND s.flag = 'del' THEN
                  DELETE
                WHEN MATCHED AND s.flag = 'upd' THEN
                  UPDATE SET a = s.a, b = s.flag
                WHEN NOT MATCHED THEN
                  INSERT (id, a, b) VALUES (s.id, s.a, s.flag)
                """);

            List<List<String>> rows = query("SELECT * FROM m2_tgt ORDER BY id");
            // id=1 updated(a=111,b='upd'), id=2 deleted, id=4 untouched, id=5 untouched, id=6 inserted
            assertEquals(4, rows.size(), "Should be 4 rows: 1 updated, 1 deleted, 2 untouched, 1 inserted");
            assertEquals("111", rows.get(0).get(1), "id=1 updated");
            assertEquals("4", rows.get(1).get(0), "id=4 untouched");
            assertEquals("5", rows.get(2).get(0), "id=5 untouched");
            assertEquals("6", rows.get(3).get(0), "id=6 inserted");
        } finally {
            exec("DROP TABLE m2_src"); exec("DROP TABLE m2_tgt");
        }
    }

    // ========================================================================
    // Row count accuracy after cascading MERGE operations
    // ========================================================================

    @Test
    void merge_row_count_after_two_merges() throws SQLException {
        exec("CREATE TABLE mc_tgt(id int PRIMARY KEY, a int, b text DEFAULT 'd', c int GENERATED ALWAYS AS IDENTITY)");
        exec("CREATE TABLE mc_src(id int, a int, b text)");
        exec("INSERT INTO mc_tgt(id, a, b) VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')");
        try {
            // First MERGE: use non-duplicate source keys so the merge succeeds
            exec("INSERT INTO mc_src VALUES (1,100,'sx'),(4,400,'sw'),(5,NULL,NULL)");
            exec("""
                MERGE INTO mc_tgt AS t USING mc_src AS s ON t.id = s.id
                WHEN MATCHED AND s.a IS NOT NULL THEN UPDATE SET a = s.a, b = coalesce(s.b, t.b)
                WHEN NOT MATCHED AND s.id IS NOT NULL THEN INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                """);

            int afterFirst = count("mc_tgt");
            // src has ids 1,4,5. id=1 matches (UPDATE). id=4,5 NOT MATCHED (INSERT).
            // Result: 3 original + id=4 + id=5 = 5 rows
            assertEquals(5, afterFirst, "Should have 5 rows after first merge");

            // Second MERGE
            exec("DELETE FROM mc_src"); // clean src
            exec("INSERT INTO mc_src VALUES (1,111,'upd'),(2,222,'del'),(6,666,'ins')");
            exec("""
                MERGE INTO mc_tgt AS t USING mc_src AS s ON t.id = s.id
                WHEN MATCHED AND s.b = 'del' THEN DELETE
                WHEN MATCHED AND s.b = 'upd' THEN UPDATE SET a = s.a, b = s.b
                WHEN NOT MATCHED THEN INSERT (id, a, b) VALUES (s.id, s.a, s.b)
                """);

            List<List<String>> finalRows = query("SELECT id FROM mc_tgt ORDER BY id");
            // After second merge: id=1 updated, id=2 deleted, others untouched, id=6 inserted
            assertFalse(finalRows.stream().anyMatch(r -> "2".equals(r.get(0))),
                    "id=2 should have been deleted by MERGE");
            assertTrue(finalRows.stream().anyMatch(r -> "6".equals(r.get(0))),
                    "id=6 should have been inserted by MERGE");
        } finally {
            exec("DROP TABLE mc_src"); exec("DROP TABLE mc_tgt");
        }
    }

    // ========================================================================
    // Subsequent queries after MERGE must see correct row counts
    // ========================================================================

    @Test
    void queries_after_merge_see_correct_counts() throws SQLException {
        exec("CREATE TABLE qa_tgt(id int PRIMARY KEY, a int, b text)");
        exec("CREATE TABLE qa_src(id int, a int, b text)");
        exec("INSERT INTO qa_tgt VALUES (1,10,'x'),(2,20,'y'),(3,30,'z')");
        exec("INSERT INTO qa_src VALUES (1,100,'sx'),(4,400,'sw')");
        try {
            exec("""
                MERGE INTO qa_tgt t USING qa_src s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET a = s.a
                WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.a, s.b)
                """);

            // Exact: 3 + 1 = 4 rows
            assertEquals(4, count("qa_tgt"), "Should be 4 after merge");

            // CTE reading from merged table
            List<List<String>> ctRes = query("""
                WITH tgt AS (SELECT id, a FROM qa_tgt WHERE id < 10)
                SELECT tgt.id, tgt.a FROM tgt ORDER BY id
                """);
            assertEquals(4, ctRes.size(), "CTE on merged table should see 4 rows");

            // Correlated subquery
            List<List<String>> subRes = query("""
                SELECT t.id, (SELECT max(s.a) FROM qa_src s WHERE s.id = t.id) AS max_a
                FROM qa_tgt t ORDER BY t.id
                """);
            assertEquals(4, subRes.size(), "Correlated subquery should see 4 rows");

            // Alias query
            List<List<String>> aliasRes = query("SELECT t.id AS x, t.a FROM qa_tgt t ORDER BY x");
            assertEquals(4, aliasRes.size(), "Alias query should see 4 rows");

            // Subquery with window function
            List<List<String>> winRes = query("""
                SELECT sub.id, sub.a FROM (
                  SELECT t.id, t.a, row_number() OVER (ORDER BY t.id) AS rn
                  FROM qa_tgt t
                ) sub WHERE sub.rn >= 1 ORDER BY sub.id
                """);
            assertEquals(4, winRes.size(), "Window subquery should see 4 rows");
        } finally {
            exec("DROP TABLE qa_src"); exec("DROP TABLE qa_tgt");
        }
    }

    // ========================================================================
    // MERGE with GENERATED ALWAYS AS IDENTITY target column
    // ========================================================================

    @Test
    void merge_with_identity_column_insert_omits_identity() throws SQLException {
        exec("CREATE TABLE mi_tgt(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, a int, b text)");
        exec("CREATE TABLE mi_src(a int, b text)");
        exec("INSERT INTO mi_tgt(a, b) VALUES (10, 'x')");
        exec("INSERT INTO mi_src VALUES (100, 'new')");
        try {
            exec("""
                MERGE INTO mi_tgt t USING mi_src s ON t.a = s.a
                WHEN NOT MATCHED THEN INSERT (a, b) VALUES (s.a, s.b)
                """);
            assertEquals(2, count("mi_tgt"), "MERGE INSERT should work with identity column");
        } finally {
            exec("DROP TABLE mi_src"); exec("DROP TABLE mi_tgt");
        }
    }

    @Test
    void merge_insert_into_identity_column_should_fail() throws SQLException {
        exec("CREATE TABLE mif_tgt(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, a int)");
        exec("CREATE TABLE mif_src(id int, a int)");
        exec("INSERT INTO mif_tgt(a) VALUES (1)");
        exec("INSERT INTO mif_src VALUES (99, 2)");
        try {
            // Explicit identity column in INSERT list should fail
            assertThrows(SQLException.class, () -> exec("""
                MERGE INTO mif_tgt t USING mif_src s ON t.a = s.a
                WHEN NOT MATCHED THEN INSERT (id, a) VALUES (s.id, s.a)
                """));
        } finally {
            exec("DROP TABLE mif_src"); exec("DROP TABLE mif_tgt");
        }
    }

    // ========================================================================
    // MERGE error cases
    // ========================================================================

    @Test
    void merge_update_nonexistent_column() throws SQLException {
        exec("CREATE TABLE me1_tgt(id int PRIMARY KEY, a int)");
        exec("CREATE TABLE me1_src(id int, a int)");
        try {
            assertThrows(SQLException.class, () -> exec("""
                MERGE INTO me1_tgt t USING me1_src s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET nope = 1
                """));
        } finally {
            exec("DROP TABLE me1_src"); exec("DROP TABLE me1_tgt");
        }
    }

    @Test
    void merge_subquery_returning_multiple_rows_in_set() throws SQLException {
        exec("CREATE TABLE me2_tgt(id int PRIMARY KEY, a int)");
        exec("CREATE TABLE me2_src(id int, a int)");
        exec("INSERT INTO me2_tgt VALUES (1, 10)");
        exec("INSERT INTO me2_src VALUES (1, 100), (2, 200)");
        try {
            assertThrows(SQLException.class, () -> exec("""
                MERGE INTO me2_tgt t USING me2_src s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET a = (SELECT id FROM me2_src)
                """));
        } finally {
            exec("DROP TABLE me2_src"); exec("DROP TABLE me2_tgt");
        }
    }

    @Test
    void merge_do_nothing_clauses() throws SQLException {
        exec("CREATE TABLE mdn_tgt(id int PRIMARY KEY, a int)");
        exec("CREATE TABLE mdn_src(id int, a int)");
        exec("INSERT INTO mdn_tgt VALUES (1, 10)");
        exec("INSERT INTO mdn_src VALUES (1, 100), (2, 200)");
        try {
            exec("""
                MERGE INTO mdn_tgt t USING mdn_src s ON t.id = s.id
                WHEN MATCHED THEN DO NOTHING
                WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.a)
                """);
            assertEquals(2, count("mdn_tgt"));
            assertEquals("10", scalar("SELECT a FROM mdn_tgt WHERE id = 1"),
                    "DO NOTHING should leave matched row unchanged");
        } finally {
            exec("DROP TABLE mdn_src"); exec("DROP TABLE mdn_tgt");
        }
    }

    // ========================================================================
    // MERGE with multiple conditional WHEN clauses
    // ========================================================================

    @Test
    void merge_first_matching_when_clause_wins() throws SQLException {
        exec("CREATE TABLE mw_tgt(id int PRIMARY KEY, a int, status text)");
        exec("CREATE TABLE mw_src(id int, a int)");
        exec("INSERT INTO mw_tgt VALUES (1, 10, 'active')");
        exec("INSERT INTO mw_src VALUES (1, 100)");
        try {
            exec("""
                MERGE INTO mw_tgt t USING mw_src s ON t.id = s.id
                WHEN MATCHED AND t.status = 'active' THEN
                  UPDATE SET a = s.a
                WHEN MATCHED THEN
                  DELETE
                """);
            // First clause matches (status='active') → UPDATE, not DELETE
            assertEquals(1, count("mw_tgt"), "Row should be updated, not deleted");
            assertEquals("100", scalar("SELECT a FROM mw_tgt WHERE id = 1"));
        } finally {
            exec("DROP TABLE mw_src"); exec("DROP TABLE mw_tgt");
        }
    }

    @Test
    void merge_only_unmatched_source_rows_trigger_insert() throws SQLException {
        exec("CREATE TABLE mu_tgt(id int PRIMARY KEY, a int)");
        exec("CREATE TABLE mu_src(id int, a int)");
        exec("INSERT INTO mu_tgt VALUES (1, 10), (2, 20)");
        exec("INSERT INTO mu_src VALUES (1, 100), (2, 200), (3, 300)");
        try {
            exec("""
                MERGE INTO mu_tgt t USING mu_src s ON t.id = s.id
                WHEN MATCHED THEN UPDATE SET a = s.a
                WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.a)
                """);
            // 2 matched → UPDATE. 1 not matched → INSERT. Total = 3.
            assertEquals(3, count("mu_tgt"),
                    "2 updates + 1 insert = 3 total rows, not more");
            assertEquals("100", scalar("SELECT a FROM mu_tgt WHERE id = 1"));
            assertEquals("200", scalar("SELECT a FROM mu_tgt WHERE id = 2"));
            assertEquals("300", scalar("SELECT a FROM mu_tgt WHERE id = 3"));
        } finally {
            exec("DROP TABLE mu_src"); exec("DROP TABLE mu_tgt");
        }
    }
}
