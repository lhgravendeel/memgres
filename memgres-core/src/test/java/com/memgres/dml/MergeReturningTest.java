package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for MERGE RETURNING (PG 17+) and WHEN NOT MATCHED BY SOURCE (PG 17+).
 */
class MergeReturningTest {

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
        try { exec("DROP TABLE IF EXISTS src"); } catch (SQLException ignored) {}
        try { exec("DROP TABLE IF EXISTS tgt"); } catch (SQLException ignored) {}
    }

    @AfterEach
    void cleanUpAfter() throws SQLException {
        try { exec("DROP TABLE IF EXISTS src"); } catch (SQLException ignored) {}
        try { exec("DROP TABLE IF EXISTS tgt"); } catch (SQLException ignored) {}
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
    // MERGE RETURNING — basic
    // ========================================================================

    @Test
    void merge_returning_update_only() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            RETURNING t.id, t.val
            """);
        assertEquals(2, res.size());
        // Both rows updated
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("1") && r.get(1).equals("100")));
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("2") && r.get(1).equals("200")));
    }

    @Test
    void merge_returning_insert_only() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, t.val
            """);
        assertEquals(2, res.size());
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("1") && r.get(1).equals("100")));
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("2") && r.get(1).equals("200")));
    }

    @Test
    void merge_returning_delete_only() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20), (3, 30)");
        exec("INSERT INTO src VALUES (1, 0), (3, 0)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN DELETE
            RETURNING t.id, t.val
            """);
        assertEquals(2, res.size());
        // Deleted rows should return their pre-delete values
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("1") && r.get(1).equals("10")));
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("3") && r.get(1).equals("30")));
        // Verify they're actually deleted
        assertEquals("1", scalar("SELECT count(*) FROM tgt"));
    }

    @Test
    void merge_returning_mixed_actions() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int, label text)");
        exec("CREATE TABLE src(id int, val int, action text)");
        exec("INSERT INTO tgt VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c')");
        exec("INSERT INTO src VALUES (1, 100, 'upd'), (2, 0, 'del'), (5, 500, 'ins')");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED AND s.action = 'del' THEN DELETE
            WHEN MATCHED AND s.action = 'upd' THEN UPDATE SET val = s.val, label = 'updated'
            WHEN NOT MATCHED THEN INSERT (id, val, label) VALUES (s.id, s.val, 'new')
            RETURNING t.id, t.val, t.label
            """);
        assertEquals(3, res.size());
        // id=1 updated
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("1") && r.get(1).equals("100") && r.get(2).equals("updated")));
        // id=2 deleted (returns pre-delete values)
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("2") && r.get(1).equals("20") && r.get(2).equals("b")));
        // id=5 inserted
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("5") && r.get(1).equals("500") && r.get(2).equals("new")));
    }

    @Test
    void merge_returning_star() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING *
            """);
        assertEquals(2, res.size());
    }

    @Test
    void merge_returning_expressions() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            RETURNING t.id, t.val, t.val * 2 AS doubled
            """);
        assertEquals(1, res.size());
        assertEquals("100", res.get(0).get(1));
        assertEquals("200", res.get(0).get(2));
    }

    @Test
    void merge_returning_no_affected_rows() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (99, 100)");

        // Source matches nothing, and only WHEN MATCHED clause exists
        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            RETURNING t.id, t.val
            """);
        assertEquals(0, res.size());
    }

    @Test
    void merge_returning_with_generated_identity() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int, seq int GENERATED ALWAYS AS IDENTITY)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, t.val, t.seq
            """);
        assertEquals(2, res.size());
        // Each inserted row should have a seq value
        assertNotNull(res.get(0).get(2));
        assertNotNull(res.get(1).get(2));
    }

    // ========================================================================
    // MERGE RETURNING — edge cases
    // ========================================================================

    @Test
    void merge_returning_do_nothing_returns_nothing() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN DO NOTHING
            RETURNING t.id
            """);
        // DO NOTHING should not produce RETURNING rows
        assertEquals(0, res.size());
    }

    @Test
    void merge_returning_conditional_actions() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int, status text)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10, 'active'), (2, 20, 'inactive'), (3, 30, 'active')");
        exec("INSERT INTO src VALUES (1, 100), (2, 200), (3, 300)");

        // Only update active rows, delete inactive rows
        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED AND t.status = 'inactive' THEN DELETE
            WHEN MATCHED AND t.status = 'active' THEN UPDATE SET val = s.val
            RETURNING t.id, t.val
            """);
        assertEquals(3, res.size());
        // id=1 updated to 100
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("1") && r.get(1).equals("100")));
        // id=2 deleted, returns pre-delete val=20
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("2") && r.get(1).equals("20")));
        // id=3 updated to 300
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("3") && r.get(1).equals("300")));
    }

    @Test
    void merge_returning_with_subquery_source() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING (SELECT * FROM src WHERE val > 50) AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, t.val
            """);
        assertEquals(2, res.size());
    }

    @Test
    void merge_returning_with_values_source() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t
            USING (VALUES (1, 100), (2, 200)) AS s(id, val) ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, t.val
            """);
        assertEquals(2, res.size());
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("1") && r.get(1).equals("100")));
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("2") && r.get(1).equals("200")));
    }

    // ========================================================================
    // WHEN NOT MATCHED BY SOURCE
    // ========================================================================

    @Test
    void merge_not_matched_by_source_delete() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20), (3, 30)");
        exec("INSERT INTO src VALUES (1, 100)");

        exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            """);

        List<List<String>> rows = query("SELECT id, val FROM tgt ORDER BY id");
        // id=1 updated, ids 2,3 deleted (not matched by source)
        assertEquals(1, rows.size());
        assertEquals("1", rows.get(0).get(0));
        assertEquals("100", rows.get(0).get(1));
    }

    @Test
    void merge_not_matched_by_source_update() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int, status text)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10, 'old'), (2, 20, 'old'), (3, 30, 'old')");
        exec("INSERT INTO src VALUES (1, 100)");

        exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET status = 'orphan'
            """);

        List<List<String>> rows = query("SELECT id, val, status FROM tgt ORDER BY id");
        assertEquals(3, rows.size());
        assertEquals("100", rows.get(0).get(1)); // id=1 updated val
        assertEquals("old", rows.get(0).get(2));
        assertEquals("orphan", rows.get(1).get(2)); // id=2 marked orphan
        assertEquals("orphan", rows.get(2).get(2)); // id=3 marked orphan
    }

    @Test
    void merge_not_matched_by_source_conditional() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int, status text)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10, 'active'), (2, 20, 'inactive'), (3, 30, 'active')");
        exec("INSERT INTO src VALUES (1, 100)");

        exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE AND t.status = 'inactive' THEN DELETE
            """);

        List<List<String>> rows = query("SELECT id FROM tgt ORDER BY id");
        // id=1 updated, id=2 deleted (inactive + not in source), id=3 untouched (active, clause didn't fire)
        assertEquals(2, rows.size());
        assertEquals("1", rows.get(0).get(0));
        assertEquals("3", rows.get(1).get(0));
    }

    @Test
    void merge_not_matched_by_source_with_returning() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20), (3, 30)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            RETURNING t.id, t.val
            """);
        assertEquals(3, res.size());
        // id=1 updated
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("1") && r.get(1).equals("100")));
        // ids 2,3 deleted (return pre-delete values)
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("2") && r.get(1).equals("20")));
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("3") && r.get(1).equals("30")));
    }

    @Test
    void merge_not_matched_by_source_do_nothing() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20), (3, 30)");
        exec("INSERT INTO src VALUES (1, 100)");

        exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE THEN DO NOTHING
            """);

        List<List<String>> rows = query("SELECT id, val FROM tgt ORDER BY id");
        assertEquals(3, rows.size());
        assertEquals("100", rows.get(0).get(1)); // id=1 updated
        assertEquals("20", rows.get(1).get(1));  // id=2 untouched
        assertEquals("30", rows.get(2).get(1));  // id=3 untouched
    }

    @Test
    void merge_by_target_explicit() throws SQLException {
        // WHEN NOT MATCHED BY TARGET is equivalent to WHEN NOT MATCHED
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY TARGET THEN INSERT (id, val) VALUES (s.id, s.val)
            """);

        List<List<String>> rows = query("SELECT id, val FROM tgt ORDER BY id");
        assertEquals(2, rows.size());
        assertEquals("100", rows.get(0).get(1));
        assertEquals("200", rows.get(1).get(1));
    }

    // ========================================================================
    // MERGE with all three clause types
    // ========================================================================

    @Test
    void merge_all_three_clause_types() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int, status text)");
        exec("CREATE TABLE src(id int, val int)");
        // Target: 1,2,3,4,5
        exec("INSERT INTO tgt VALUES (1, 10, 'old'), (2, 20, 'old'), (3, 30, 'old'), (4, 40, 'old'), (5, 50, 'old')");
        // Source: 1,2,6
        exec("INSERT INTO src VALUES (1, 100), (2, 200), (6, 600)");

        exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY TARGET THEN INSERT (id, val, status) VALUES (s.id, s.val, 'new')
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET status = 'orphan'
            """);

        List<List<String>> rows = query("SELECT id, val, status FROM tgt ORDER BY id");
        assertEquals(6, rows.size());
        // id=1,2 updated val from source
        assertEquals("100", rows.get(0).get(1));
        assertEquals("200", rows.get(1).get(1));
        // id=3,4,5 marked orphan (not in source)
        assertEquals("orphan", rows.get(2).get(2));
        assertEquals("orphan", rows.get(3).get(2));
        assertEquals("orphan", rows.get(4).get(2));
        // id=6 inserted
        assertEquals("6", rows.get(5).get(0));
        assertEquals("600", rows.get(5).get(1));
        assertEquals("new", rows.get(5).get(2));
    }

    @Test
    void merge_all_three_with_returning() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20), (3, 30)");
        exec("INSERT INTO src VALUES (1, 100), (5, 500)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY TARGET THEN INSERT (id, val) VALUES (s.id, s.val)
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            RETURNING t.id, t.val
            """);
        // id=1 updated, id=5 inserted, ids 2,3 deleted
        assertEquals(4, res.size());
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("1") && r.get(1).equals("100")));
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("5") && r.get(1).equals("500")));
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("2") && r.get(1).equals("20")));
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("3") && r.get(1).equals("30")));

        // After merge, only id=1 and id=5 remain
        List<List<String>> remaining = query("SELECT id FROM tgt ORDER BY id");
        assertEquals(2, remaining.size());
        assertEquals("1", remaining.get(0).get(0));
        assertEquals("5", remaining.get(1).get(0));
    }

    // ========================================================================
    // MERGE RETURNING — column name metadata
    // ========================================================================

    @Test
    void merge_returning_column_names() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100)");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 MERGE INTO tgt AS t USING src AS s ON t.id = s.id
                 WHEN MATCHED THEN UPDATE SET val = s.val
                 RETURNING t.id, t.val, t.val + 1 AS next_val
                 """)) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount());
            assertEquals("id", md.getColumnName(1));
            assertEquals("val", md.getColumnName(2));
            assertEquals("next_val", md.getColumnName(3));
        }
    }

    // ========================================================================
    // Error cases
    // ========================================================================

    @Test
    void merge_returning_invalid_column() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
            exec("CREATE TABLE src(id int, val int)");
            exec("INSERT INTO tgt VALUES (1, 10)");
            exec("INSERT INTO src VALUES (1, 100)");
        });

        SQLException ex = assertThrows(SQLException.class, () -> query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            RETURNING t.nope
            """));
        assertEquals("42703", ex.getSQLState());
    }

    @Test
    void merge_not_matched_by_source_insert_is_parse_error() {
        // WHEN NOT MATCHED BY SOURCE cannot have INSERT
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
            exec("CREATE TABLE src(id int, val int)");
        });

        assertThrows(SQLException.class, () -> exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED BY SOURCE THEN INSERT (id, val) VALUES (1, 1)
            """));
    }

    @Test
    void merge_not_matched_by_source_bad_column() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
            exec("CREATE TABLE src(id int, val int)");
        });

        SQLException ex = assertThrows(SQLException.class, () -> exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET nope = 1
            """));
        assertEquals("42703", ex.getSQLState());
    }

    @Test
    void merge_duplicate_unconditional_not_matched_by_source() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
            exec("CREATE TABLE src(id int, val int)");
            exec("INSERT INTO tgt VALUES (1, 10)");
            exec("INSERT INTO src VALUES (2, 20)");
        });

        SQLException ex = assertThrows(SQLException.class, () -> exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET val = 0
            """));
        assertEquals("42601", ex.getSQLState());
    }

    // ========================================================================
    // MERGE RETURNING with empty source
    // ========================================================================

    @Test
    void merge_returning_empty_source_with_not_matched_by_source() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20)");
        // Empty source

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            RETURNING t.id, t.val
            """);
        // All target rows are "not matched by source" and should be deleted
        assertEquals(2, res.size());
        assertEquals("0", scalar("SELECT count(*) FROM tgt"));
    }

    // ========================================================================
    // MERGE RETURNING with NULL handling
    // ========================================================================

    @Test
    void merge_returning_null_values() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, NULL)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            RETURNING t.id, t.val
            """);
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).get(0));
        assertNull(res.get(0).get(1));
    }

    // ========================================================================
    // MERGE RETURNING with coalesce and function expressions
    // ========================================================================

    @Test
    void merge_returning_with_coalesce() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int, label text)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10, NULL)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            RETURNING t.id, coalesce(t.label, 'no_label') AS label
            """);
        assertEquals(1, res.size());
        assertEquals("no_label", res.get(0).get(1));
    }

    // ========================================================================
    // MERGE without RETURNING still works as before
    // ========================================================================

    @Test
    void merge_without_returning_still_works() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        exec("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            """);

        assertEquals("2", scalar("SELECT count(*) FROM tgt"));
        assertEquals("100", scalar("SELECT val FROM tgt WHERE id = 1"));
        assertEquals("200", scalar("SELECT val FROM tgt WHERE id = 2"));
    }

    // ========================================================================
    // MERGE RETURNING with multiple inserts
    // ========================================================================

    @Test
    void merge_returning_multiple_inserts() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200), (3, 300)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING *
            """);
        assertEquals(3, res.size());
    }

    // ========================================================================
    // MERGE NOT MATCHED BY SOURCE with update returning updated values
    // ========================================================================

    @Test
    void merge_not_matched_by_source_update_returning() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int, status text)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10, 'ok'), (2, 20, 'ok'), (3, 30, 'ok')");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET status = 'stale'
            RETURNING t.id, t.val, t.status
            """);
        assertEquals(3, res.size());
        // id=1 updated
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("1") && r.get(1).equals("100") && r.get(2).equals("ok")));
        // ids 2,3 marked stale (return updated values)
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("2") && r.get(2).equals("stale")));
        assertTrue(res.stream().anyMatch(r -> r.get(0).equals("3") && r.get(2).equals("stale")));
    }
}
