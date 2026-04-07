package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SQL MERGE statement (PG 15+).
 * Covers MERGE INTO ... USING ... WHEN MATCHED/NOT MATCHED,
 * with UPDATE, DELETE, INSERT actions, plus RETURNING and CTE interactions.
 */
class MergeStatementTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE tgt(id int PRIMARY KEY, a int, b text)");
        exec("CREATE TABLE src(id int, a int, b text)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void resetData() throws SQLException {
        exec("DELETE FROM tgt");
        exec("DELETE FROM src");
        exec("INSERT INTO tgt VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");
        exec("INSERT INTO src VALUES (1, 100, 'sx'), (4, 400, 'sw'), (5, NULL, NULL)");
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

    static int countRows(String sql) throws SQLException {
        return query(sql).size();
    }

    // ========================================================================
    // Basic MERGE: update matched, insert not matched
    // ========================================================================

    @Test
    void merge_update_and_insert() throws SQLException {
        exec("""
            MERGE INTO tgt AS t
            USING src AS s
            ON t.id = s.id
            WHEN MATCHED AND s.a IS NOT NULL THEN
              UPDATE SET a = s.a, b = coalesce(s.b, t.b)
            WHEN NOT MATCHED AND s.id IS NOT NULL THEN
              INSERT (id, a, b) VALUES (s.id, s.a, s.b)
            """);

        List<List<String>> rows = query("SELECT * FROM tgt ORDER BY id");
        // id=1 should be updated (a=100), ids 2,3 untouched, ids 4,5 inserted
        assertEquals(5, rows.size(), "MERGE should produce exactly 5 rows");
        assertEquals("100", rows.get(0).get(1), "id=1 should have a=100 after MATCHED UPDATE");
        assertEquals("20", rows.get(1).get(1), "id=2 should be untouched");
        assertEquals("400", rows.get(3).get(1), "id=4 should be inserted with a=400");
    }

    @Test
    void merge_does_not_duplicate_inserts() throws SQLException {
        exec("""
            MERGE INTO tgt AS t
            USING src AS s
            ON t.id = s.id
            WHEN MATCHED THEN
              UPDATE SET a = s.a
            WHEN NOT MATCHED THEN
              INSERT (id, a, b) VALUES (s.id, s.a, s.b)
            """);

        // src has ids 1,4,5. id=1 matches tgt, so updated. ids 4,5 inserted.
        // Total should be 3 (original) + 2 (new) = 5, NOT 6 or more.
        assertEquals(5, countRows("SELECT * FROM tgt"),
                "MERGE must not insert duplicate rows for matched source rows");
    }

    // ========================================================================
    // MERGE with DELETE action
    // ========================================================================

    @Test
    void merge_with_delete_action() throws SQLException {
        exec("INSERT INTO src VALUES (2, 222, 'del')");
        exec("""
            MERGE INTO tgt AS t
            USING (SELECT * FROM src WHERE id = 2) AS s
            ON t.id = s.id
            WHEN MATCHED THEN
              DELETE
            """);

        assertNull(query("SELECT * FROM tgt WHERE id = 2").stream().findFirst().orElse(null),
                "MERGE DELETE should remove matched row");
    }

    @Test
    void merge_mixed_update_delete_insert() throws SQLException {
        exec("DELETE FROM src");
        exec("INSERT INTO src VALUES (1, 111, 'upd'), (2, 222, 'del'), (6, 666, 'ins')");

        exec("""
            MERGE INTO tgt AS t
            USING src AS s
            ON t.id = s.id
            WHEN MATCHED AND s.b = 'del' THEN
              DELETE
            WHEN MATCHED AND s.b = 'upd' THEN
              UPDATE SET a = s.a, b = s.b
            WHEN NOT MATCHED THEN
              INSERT (id, a, b) VALUES (s.id, s.a, s.b)
            """);

        List<List<String>> rows = query("SELECT * FROM tgt ORDER BY id");
        // id=1 updated, id=2 deleted, id=3 untouched, id=6 inserted → 3 rows
        assertEquals(3, rows.size(), "Should have 3 rows after mixed MERGE");
        assertEquals("111", rows.get(0).get(1), "id=1 updated to a=111");
        assertEquals("3", rows.get(1).get(0), "id=3 untouched");
        assertEquals("6", rows.get(2).get(0), "id=6 inserted");
    }

    // ========================================================================
    // MERGE with GENERATED ALWAYS AS IDENTITY target column
    // ========================================================================

    @Test
    void merge_respects_identity_column() throws SQLException {
        exec("CREATE TABLE tgt_id(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, a int, b text)");
        exec("INSERT INTO tgt_id(a, b) VALUES (10, 'x')");
        exec("CREATE TABLE src_id(id int, a int, b text)");
        exec("INSERT INTO src_id VALUES (1, 100, 'upd'), (2, 200, 'new')");

        exec("""
            MERGE INTO tgt_id AS t
            USING src_id AS s
            ON t.id = s.id
            WHEN MATCHED THEN
              UPDATE SET a = s.a, b = s.b
            WHEN NOT MATCHED THEN
              INSERT (a, b) VALUES (s.a, s.b)
            """);

        List<List<String>> rows = query("SELECT * FROM tgt_id ORDER BY id");
        assertEquals(2, rows.size());

        exec("DROP TABLE src_id");
        exec("DROP TABLE tgt_id");
    }

    // ========================================================================
    // MERGE error cases
    // ========================================================================

    @Test
    void merge_update_nonexistent_column_fails() {
        assertThrows(SQLException.class, () -> exec("""
            MERGE INTO tgt AS t
            USING src AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET nope = 1
            """));
    }

    @Test
    void merge_insert_into_identity_column_fails() throws SQLException {
        exec("CREATE TABLE tgt_id2(id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, a int)");
        try {
            assertThrows(SQLException.class, () -> exec("""
                MERGE INTO tgt_id2 AS t
                USING src AS s
                ON t.id = s.id
                WHEN NOT MATCHED THEN INSERT (id, a) VALUES (s.id, s.a)
                """));
        } finally {
            exec("DROP TABLE IF EXISTS tgt_id2");
        }
    }

    @Test
    void merge_duplicate_when_matched_clauses_fails() {
        // PG does not allow two WHEN MATCHED THEN UPDATE clauses
        assertThrows(SQLException.class, () -> exec("""
            MERGE INTO tgt AS t
            USING src AS s
            ON t.id = s.id
            WHEN MATCHED THEN DELETE
            WHEN MATCHED THEN UPDATE SET a = 1
            """));
    }

    @Test
    void merge_subquery_in_update_set_fails() {
        // Subquery returning multiple rows in SET clause should fail
        assertThrows(SQLException.class, () -> exec("""
            MERGE INTO tgt AS t
            USING src AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET a = (SELECT id FROM src)
            """));
    }

    // ========================================================================
    // CTE scoping with MERGE target table name
    // ========================================================================

    @Test
    void cte_shadowing_merge_target_table() throws SQLException {
        // CTE named 'tgt' should shadow the real table in a SELECT
        List<List<String>> rows = query("""
            WITH tgt AS (
              SELECT id, a FROM tgt WHERE id < 3
            )
            SELECT tgt.id, tgt.a FROM tgt ORDER BY id
            """);
        assertEquals(2, rows.size(), "CTE should shadow real table, returning only id<3");
    }
}
