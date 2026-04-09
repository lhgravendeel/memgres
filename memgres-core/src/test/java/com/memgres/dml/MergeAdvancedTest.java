package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for:
 * - merge_action() function in MERGE RETURNING (PG 17+)
 * - WITH (CTE) support for MERGE
 * - RETURNING OLD/NEW for INSERT, UPDATE, DELETE, MERGE (PG 18)
 */
class MergeAdvancedTest {

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
        for (String t : List.of("src", "tgt", "t1", "audit_log")) {
            try { exec("DROP TABLE IF EXISTS " + t); } catch (SQLException ignored) {}
        }
    }

    @AfterEach
    void cleanUpAfter() throws SQLException {
        for (String t : List.of("src", "tgt", "t1", "audit_log")) {
            try { exec("DROP TABLE IF EXISTS " + t); } catch (SQLException ignored) {}
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
    // merge_action() function
    // ========================================================================

    @Test
    void merge_action_returns_insert() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, merge_action()
            """);
        assertEquals(1, res.size());
        assertEquals("INSERT", res.get(0).get(1));
    }

    @Test
    void merge_action_returns_update() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            RETURNING t.id, merge_action()
            """);
        assertEquals(1, res.size());
        assertEquals("UPDATE", res.get(0).get(1));
    }

    @Test
    void merge_action_returns_delete() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 0)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN DELETE
            RETURNING t.id, merge_action()
            """);
        assertEquals(1, res.size());
        assertEquals("DELETE", res.get(0).get(1));
    }

    @Test
    void merge_action_mixed_actions() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int, action text)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20)");
        exec("INSERT INTO src VALUES (1, 100, 'upd'), (2, 0, 'del'), (3, 300, 'ins')");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED AND s.action = 'del' THEN DELETE
            WHEN MATCHED AND s.action = 'upd' THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, merge_action()
            """);
        assertEquals(3, res.size());
        // Check each action
        Map<String, String> actionMap = new HashMap<>();
        for (List<String> r : res) actionMap.put(r.get(0), r.get(1));
        assertEquals("UPDATE", actionMap.get("1"));
        assertEquals("DELETE", actionMap.get("2"));
        assertEquals("INSERT", actionMap.get("3"));
    }

    @Test
    void merge_action_with_not_matched_by_source() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20), (3, 30)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            RETURNING t.id, merge_action()
            """);
        assertEquals(3, res.size());
        Map<String, String> actionMap = new HashMap<>();
        for (List<String> r : res) actionMap.put(r.get(0), r.get(1));
        assertEquals("UPDATE", actionMap.get("1"));
        assertEquals("DELETE", actionMap.get("2"));
        assertEquals("DELETE", actionMap.get("3"));
    }

    // ========================================================================
    // WITH (CTE) support for MERGE
    // ========================================================================

    @Test
    void with_cte_merge_source() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        exec("""
            WITH s AS (SELECT * FROM src WHERE val > 50)
            MERGE INTO tgt AS t USING s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            """);

        List<List<String>> rows = query("SELECT id, val FROM tgt ORDER BY id");
        assertEquals(2, rows.size());
        assertEquals("100", rows.get(0).get(1));
        assertEquals("200", rows.get(1).get(1));
    }

    @Test
    void with_cte_merge_returning() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        List<List<String>> res = query("""
            WITH s AS (SELECT * FROM src)
            MERGE INTO tgt AS t USING s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, t.val
            """);
        assertEquals(2, res.size());
    }

    @Test
    void with_cte_merge_filtering_source() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200), (3, 300)");

        // CTE filters source to only id > 1
        exec("""
            WITH filtered AS (SELECT * FROM src WHERE id > 1)
            MERGE INTO tgt AS t USING filtered AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            """);

        assertEquals("10", scalar("SELECT val FROM tgt WHERE id = 1")); // not matched by filtered source
        assertEquals("200", scalar("SELECT val FROM tgt WHERE id = 2")); // updated
        assertEquals("300", scalar("SELECT val FROM tgt WHERE id = 3")); // inserted
    }

    @Test
    void with_cte_merge_multiple_ctes() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        exec("""
            WITH s1 AS (SELECT * FROM src WHERE id = 1),
                 s2 AS (SELECT * FROM src WHERE id = 2),
                 combined AS (SELECT * FROM s1 UNION ALL SELECT * FROM s2)
            MERGE INTO tgt AS t USING combined AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            """);

        assertEquals("2", scalar("SELECT count(*) FROM tgt"));
    }

    // ========================================================================
    // RETURNING OLD/NEW — INSERT
    // ========================================================================

    @Test
    void insert_returning_new_star() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");

        List<List<String>> res = query(
                "INSERT INTO t1 VALUES (1, 100) RETURNING NEW.*");
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).get(0));
        assertEquals("100", res.get(0).get(1));
    }

    @Test
    void insert_returning_old_star() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");

        // For INSERT, OLD.* should be all NULLs
        List<List<String>> res = query(
                "INSERT INTO t1 VALUES (1, 100) RETURNING OLD.*");
        assertEquals(1, res.size());
        assertNull(res.get(0).get(0));
        assertNull(res.get(0).get(1));
    }

    @Test
    void insert_returning_old_and_new_columns() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");

        List<List<String>> res = query(
                "INSERT INTO t1 VALUES (1, 100) RETURNING OLD.id, OLD.val, NEW.id, NEW.val");
        assertEquals(1, res.size());
        assertNull(res.get(0).get(0));   // OLD.id = NULL
        assertNull(res.get(0).get(1));   // OLD.val = NULL
        assertEquals("1", res.get(0).get(2));    // NEW.id = 1
        assertEquals("100", res.get(0).get(3));  // NEW.val = 100
    }

    @Test
    void insert_returning_bare_star_is_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");

        // Bare * should return NEW (backward compat)
        List<List<String>> bareRes = query("INSERT INTO t1 VALUES (1, 100) RETURNING *");
        exec("DROP TABLE t1");
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        List<List<String>> newRes = query("INSERT INTO t1 VALUES (1, 100) RETURNING NEW.*");

        assertEquals(bareRes.get(0).get(0), newRes.get(0).get(0));
        assertEquals(bareRes.get(0).get(1), newRes.get(0).get(1));
    }

    // ========================================================================
    // RETURNING OLD/NEW — UPDATE
    // ========================================================================

    @Test
    void update_returning_old_star() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10), (2, 20)");

        List<List<String>> res = query(
                "UPDATE t1 SET val = val + 100 WHERE id = 1 RETURNING OLD.*");
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).get(0));
        assertEquals("10", res.get(0).get(1)); // OLD value
    }

    @Test
    void update_returning_new_star() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        List<List<String>> res = query(
                "UPDATE t1 SET val = val + 100 RETURNING NEW.*");
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).get(0));
        assertEquals("110", res.get(0).get(1)); // NEW value
    }

    @Test
    void update_returning_old_and_new_columns() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int, label text)");
        exec("INSERT INTO t1 VALUES (1, 10, 'old_label')");

        List<List<String>> res = query(
                "UPDATE t1 SET val = 999, label = 'new_label' RETURNING OLD.val, NEW.val, OLD.label, NEW.label");
        assertEquals(1, res.size());
        assertEquals("10", res.get(0).get(0));          // OLD.val
        assertEquals("999", res.get(0).get(1));         // NEW.val
        assertEquals("old_label", res.get(0).get(2));   // OLD.label
        assertEquals("new_label", res.get(0).get(3));   // NEW.label
    }

    @Test
    void update_returning_bare_star_is_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        // Bare * returns NEW values (backward compat)
        List<List<String>> res = query("UPDATE t1 SET val = 999 RETURNING *");
        assertEquals("999", res.get(0).get(1)); // NEW
    }

    @Test
    void update_returning_old_new_multiple_rows() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)");

        List<List<String>> res = query(
                "UPDATE t1 SET val = val * 10 RETURNING id, OLD.val AS old_val, NEW.val AS new_val");
        assertEquals(3, res.size());
        // Verify each row has correct old and new
        for (List<String> r : res) {
            int id = Integer.parseInt(r.get(0));
            int oldVal = Integer.parseInt(r.get(1));
            int newVal = Integer.parseInt(r.get(2));
            assertEquals(id * 10, oldVal);
            assertEquals(id * 100, newVal);
        }
    }

    // ========================================================================
    // RETURNING OLD/NEW — DELETE
    // ========================================================================

    @Test
    void delete_returning_old_star() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10), (2, 20)");

        List<List<String>> res = query(
                "DELETE FROM t1 WHERE id = 1 RETURNING OLD.*");
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).get(0));
        assertEquals("10", res.get(0).get(1));
    }

    @Test
    void delete_returning_new_star() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        // For DELETE, NEW.* should be all NULLs
        List<List<String>> res = query(
                "DELETE FROM t1 RETURNING NEW.*");
        assertEquals(1, res.size());
        assertNull(res.get(0).get(0));
        assertNull(res.get(0).get(1));
    }

    @Test
    void delete_returning_old_and_new_columns() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        List<List<String>> res = query(
                "DELETE FROM t1 RETURNING OLD.id, OLD.val, NEW.id, NEW.val");
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).get(0));    // OLD.id
        assertEquals("10", res.get(0).get(1));   // OLD.val
        assertNull(res.get(0).get(2));           // NEW.id = NULL
        assertNull(res.get(0).get(3));           // NEW.val = NULL
    }

    @Test
    void delete_returning_bare_star_is_old() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        // For DELETE, bare * returns the deleted row (= OLD)
        List<List<String>> res = query("DELETE FROM t1 RETURNING *");
        assertEquals("1", res.get(0).get(0));
        assertEquals("10", res.get(0).get(1));
    }

    // ========================================================================
    // RETURNING OLD/NEW — MERGE
    // ========================================================================

    @Test
    void merge_returning_old_new_update() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            RETURNING OLD.val, NEW.val
            """);
        assertEquals(1, res.size());
        assertEquals("10", res.get(0).get(0));   // OLD.val
        assertEquals("100", res.get(0).get(1));  // NEW.val
    }

    @Test
    void merge_returning_old_new_insert() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING OLD.val, NEW.val
            """);
        assertEquals(1, res.size());
        assertNull(res.get(0).get(0));           // OLD.val = NULL (insert)
        assertEquals("100", res.get(0).get(1));  // NEW.val
    }

    @Test
    void merge_returning_old_new_delete() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 0)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN DELETE
            RETURNING OLD.val, NEW.val
            """);
        assertEquals(1, res.size());
        assertEquals("10", res.get(0).get(0));   // OLD.val
        assertNull(res.get(0).get(1));           // NEW.val = NULL (delete)
    }

    @Test
    void merge_returning_old_new_mixed_with_merge_action() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int, action text)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20)");
        exec("INSERT INTO src VALUES (1, 100, 'upd'), (2, 0, 'del'), (3, 300, 'ins')");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED AND s.action = 'del' THEN DELETE
            WHEN MATCHED AND s.action = 'upd' THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, merge_action(), OLD.val AS old_val, NEW.val AS new_val
            """);
        assertEquals(3, res.size());
        Map<String, List<String>> byId = new HashMap<>();
        for (List<String> r : res) byId.put(r.get(0), r);

        // id=1: UPDATE, old=10, new=100
        assertEquals("UPDATE", byId.get("1").get(1));
        assertEquals("10", byId.get("1").get(2));
        assertEquals("100", byId.get("1").get(3));

        // id=2: DELETE, old=20, new=NULL
        assertEquals("DELETE", byId.get("2").get(1));
        assertEquals("20", byId.get("2").get(2));
        assertNull(byId.get("2").get(3));

        // id=3: INSERT, old=NULL, new=300
        assertEquals("INSERT", byId.get("3").get(1));
        assertNull(byId.get("3").get(2));
        assertEquals("300", byId.get("3").get(3));
    }

    // ========================================================================
    // RETURNING OLD/NEW — edge cases
    // ========================================================================

    @Test
    void update_returning_old_new_star_both() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        List<List<String>> res = query("UPDATE t1 SET val = 99 RETURNING OLD.*, NEW.*");
        assertEquals(1, res.size());
        // OLD.id, OLD.val, NEW.id, NEW.val
        assertEquals("1", res.get(0).get(0));
        assertEquals("10", res.get(0).get(1));
        assertEquals("1", res.get(0).get(2));
        assertEquals("99", res.get(0).get(3));
    }

    @Test
    void insert_returning_new_column_only() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");

        List<List<String>> res = query(
                "INSERT INTO t1 VALUES (1, 100) RETURNING NEW.id, NEW.val");
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).get(0));
        assertEquals("100", res.get(0).get(1));
    }

    @Test
    void update_on_conflict_returning_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        List<List<String>> res = query(
                "INSERT INTO t1 VALUES (1, 100) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val RETURNING OLD.val, NEW.val");
        assertEquals(1, res.size());
        assertEquals("10", res.get(0).get(0));   // OLD.val
        assertEquals("100", res.get(0).get(1));  // NEW.val
    }

    // ========================================================================
    // Combined: CTE + MERGE RETURNING + merge_action + OLD/NEW
    // ========================================================================

    @Test
    void with_merge_returning_merge_action_old_new() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20)");
        exec("INSERT INTO src VALUES (1, 100), (3, 300)");

        List<List<String>> res = query("""
            WITH s AS (SELECT * FROM src)
            MERGE INTO tgt AS t USING s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, merge_action(), OLD.val, NEW.val
            """);
        assertEquals(2, res.size());
        Map<String, List<String>> byId = new HashMap<>();
        for (List<String> r : res) byId.put(r.get(0), r);

        assertEquals("UPDATE", byId.get("1").get(1));
        assertEquals("10", byId.get("1").get(2));   // OLD
        assertEquals("100", byId.get("1").get(3));  // NEW

        assertEquals("INSERT", byId.get("3").get(1));
        assertNull(byId.get("3").get(2));           // OLD = NULL
        assertEquals("300", byId.get("3").get(3));  // NEW
    }

    // ========================================================================
    // RETURNING OLD/NEW with expressions
    // ========================================================================

    @Test
    void update_returning_old_new_expressions() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        List<List<String>> res = query(
                "UPDATE t1 SET val = 50 RETURNING NEW.val - OLD.val AS diff");
        assertEquals(1, res.size());
        assertEquals("40", res.get(0).get(0));
    }

    @Test
    void merge_returning_old_new_with_coalesce() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING coalesce(OLD.val, -1) AS old_or_default, NEW.val
            """);
        assertEquals(1, res.size());
        assertEquals("-1", res.get(0).get(0));    // OLD.val is NULL, coalesce to -1
        assertEquals("100", res.get(0).get(1));
    }

    // ========================================================================
    // Error states
    // ========================================================================

    @Test
    void merge_action_outside_merge_context_errors() {
        // merge_action() outside MERGE RETURNING should error
        SQLException ex = assertThrows(SQLException.class, () ->
                query("SELECT merge_action()"));
        assertEquals("42P20", ex.getSQLState());
    }

    @Test
    void returning_old_nonexistent_column_errors() {
        assertDoesNotThrow(() -> exec("CREATE TABLE t1(id int PRIMARY KEY, val int)"));
        SQLException ex = assertThrows(SQLException.class, () ->
                query("INSERT INTO t1 VALUES (1, 100) RETURNING OLD.nope"));
        assertEquals("42703", ex.getSQLState());
    }

    @Test
    void returning_new_nonexistent_column_errors() {
        assertDoesNotThrow(() -> exec("CREATE TABLE t1(id int PRIMARY KEY, val int)"));
        assertDoesNotThrow(() -> exec("INSERT INTO t1 VALUES (1, 10)"));
        SQLException ex = assertThrows(SQLException.class, () ->
                query("UPDATE t1 SET val = 99 RETURNING NEW.nope"));
        assertEquals("42703", ex.getSQLState());
    }

    @Test
    void merge_returning_old_nonexistent_column_errors() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
            exec("CREATE TABLE src(id int, val int)");
        });
        SQLException ex = assertThrows(SQLException.class, () -> query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            RETURNING OLD.bogus
            """));
        assertEquals("42703", ex.getSQLState());
    }

    // ========================================================================
    // Additional RETURNING OLD/NEW edge cases
    // ========================================================================

    @Test
    void insert_multiple_rows_returning_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");

        List<List<String>> res = query(
                "INSERT INTO t1 VALUES (1, 100), (2, 200), (3, 300) RETURNING OLD.id, OLD.val, NEW.id, NEW.val");
        assertEquals(3, res.size());
        for (List<String> r : res) {
            assertNull(r.get(0));  // OLD.id = NULL for INSERT
            assertNull(r.get(1));  // OLD.val = NULL for INSERT
            assertNotNull(r.get(2));  // NEW.id
            assertNotNull(r.get(3));  // NEW.val
        }
    }

    @Test
    void delete_multiple_rows_returning_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)");

        List<List<String>> res = query(
                "DELETE FROM t1 RETURNING OLD.id, OLD.val, NEW.id, NEW.val");
        assertEquals(3, res.size());
        for (List<String> r : res) {
            assertNotNull(r.get(0));  // OLD.id
            assertNotNull(r.get(1));  // OLD.val
            assertNull(r.get(2));     // NEW.id = NULL for DELETE
            assertNull(r.get(3));     // NEW.val = NULL for DELETE
        }
    }

    @Test
    void update_no_matching_rows_returning_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        List<List<String>> res = query(
                "UPDATE t1 SET val = 99 WHERE id = 999 RETURNING OLD.val, NEW.val");
        assertEquals(0, res.size());
    }

    @Test
    void insert_select_returning_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        List<List<String>> res = query(
                "INSERT INTO t1 SELECT * FROM src RETURNING OLD.val, NEW.val");
        assertEquals(2, res.size());
        for (List<String> r : res) {
            assertNull(r.get(0));     // OLD.val = NULL for INSERT
            assertNotNull(r.get(1));  // NEW.val
        }
    }

    @Test
    void update_null_to_nonnull_returning_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, NULL)");

        List<List<String>> res = query(
                "UPDATE t1 SET val = 42 RETURNING OLD.val, NEW.val");
        assertEquals(1, res.size());
        assertNull(res.get(0).get(0));       // OLD.val was NULL
        assertEquals("42", res.get(0).get(1)); // NEW.val = 42
    }

    @Test
    void update_nonnull_to_null_returning_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 42)");

        List<List<String>> res = query(
                "UPDATE t1 SET val = NULL RETURNING OLD.val, NEW.val");
        assertEquals(1, res.size());
        assertEquals("42", res.get(0).get(0)); // OLD.val = 42
        assertNull(res.get(0).get(1));          // NEW.val = NULL
    }

    @Test
    void on_conflict_do_nothing_returning() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        // Conflict row should be skipped entirely — no RETURNING
        List<List<String>> res = query(
                "INSERT INTO t1 VALUES (1, 100), (2, 200) ON CONFLICT DO NOTHING RETURNING *");
        assertEquals(1, res.size()); // Only the non-conflicting row (id=2)
        assertEquals("2", res.get(0).get(0));
        assertEquals("200", res.get(0).get(1));
    }

    @Test
    void merge_not_matched_by_source_delete_returning_old_new() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10), (2, 20), (3, 30)");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            RETURNING t.id, merge_action(), OLD.val, NEW.val
            """);
        assertEquals(3, res.size());
        Map<String, List<String>> byId = new HashMap<>();
        for (List<String> r : res) byId.put(r.get(0), r);

        // id=1 updated: OLD=10, NEW=100
        assertEquals("UPDATE", byId.get("1").get(1));
        assertEquals("10", byId.get("1").get(2));
        assertEquals("100", byId.get("1").get(3));

        // id=2 deleted by source: OLD=20, NEW=NULL
        assertEquals("DELETE", byId.get("2").get(1));
        assertEquals("20", byId.get("2").get(2));
        assertNull(byId.get("2").get(3));

        // id=3 deleted by source: OLD=30, NEW=NULL
        assertEquals("DELETE", byId.get("3").get(1));
        assertEquals("30", byId.get("3").get(2));
        assertNull(byId.get("3").get(3));
    }

    @Test
    void merge_not_matched_by_source_update_returning_old_new() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int, status text)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10, 'active'), (2, 20, 'active'), (3, 30, 'active')");
        exec("INSERT INTO src VALUES (1, 100)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET status = 'orphan'
            RETURNING t.id, OLD.status, NEW.status
            """);
        assertEquals(3, res.size());
        Map<String, List<String>> byId = new HashMap<>();
        for (List<String> r : res) byId.put(r.get(0), r);

        // id=1 updated val (status unchanged): OLD.status='active', NEW.status='active'
        assertEquals("active", byId.get("1").get(1));
        assertEquals("active", byId.get("1").get(2));

        // id=2 status changed: OLD.status='active', NEW.status='orphan'
        assertEquals("active", byId.get("2").get(1));
        assertEquals("orphan", byId.get("2").get(2));

        // id=3 status changed: OLD.status='active', NEW.status='orphan'
        assertEquals("active", byId.get("3").get(1));
        assertEquals("orphan", byId.get("3").get(2));
    }

    // ========================================================================
    // CASE, IS NULL, CAST with OLD/NEW (tests exprReferencesOldNew coverage)
    // ========================================================================

    @Test
    void returning_case_with_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        List<List<String>> res = query(
                "UPDATE t1 SET val = 99 RETURNING CASE WHEN OLD.val < 50 THEN 'small' ELSE 'big' END AS size_label");
        assertEquals(1, res.size());
        assertEquals("small", res.get(0).get(0));
    }

    @Test
    void returning_is_null_with_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");

        // INSERT: OLD.val IS NULL should be true
        List<List<String>> res = query(
                "INSERT INTO t1 VALUES (1, 100) RETURNING CASE WHEN OLD.val IS NULL THEN 'yes' ELSE 'no' END AS was_null");
        assertEquals(1, res.size());
        assertEquals("yes", res.get(0).get(0));
    }

    @Test
    void returning_cast_with_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        List<List<String>> res = query(
                "UPDATE t1 SET val = 99 RETURNING OLD.val::text AS old_text, NEW.val::text AS new_text");
        assertEquals(1, res.size());
        assertEquals("10", res.get(0).get(0));
        assertEquals("99", res.get(0).get(1));
    }

    @Test
    void merge_action_in_case_expression() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO tgt VALUES (1, 10)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET val = s.val
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING t.id, CASE merge_action() WHEN 'INSERT' THEN 'new row' WHEN 'UPDATE' THEN 'changed' ELSE 'other' END AS action_label
            """);
        assertEquals(2, res.size());
        Map<String, String> byId = new HashMap<>();
        for (List<String> r : res) byId.put(r.get(0), r.get(1));
        assertEquals("changed", byId.get("1"));
        assertEquals("new row", byId.get("2"));
    }

    // ========================================================================
    // Column metadata for OLD.*/NEW.*
    // ========================================================================

    @Test
    void returning_old_new_column_metadata() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int, label text)");
        exec("INSERT INTO t1 VALUES (1, 10, 'x')");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "UPDATE t1 SET val = 99 RETURNING OLD.id, OLD.val, NEW.val, NEW.label")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(4, md.getColumnCount());
            assertEquals("id", md.getColumnName(1));
            assertEquals("val", md.getColumnName(2));
            assertEquals("val", md.getColumnName(3));
            assertEquals("label", md.getColumnName(4));
        }
    }

    @Test
    void returning_old_new_star_column_metadata() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("UPDATE t1 SET val = 99 RETURNING OLD.*, NEW.*")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(4, md.getColumnCount());
            // OLD.id, OLD.val, NEW.id, NEW.val
            assertEquals("id", md.getColumnName(1));
            assertEquals("val", md.getColumnName(2));
            assertEquals("id", md.getColumnName(3));
            assertEquals("val", md.getColumnName(4));
        }
    }

    // ========================================================================
    // Mixed bare * with OLD/NEW
    // ========================================================================

    @Test
    void returning_bare_star_plus_old_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        // bare * returns current (NEW), plus OLD.val
        List<List<String>> res = query(
                "UPDATE t1 SET val = 99 RETURNING *, OLD.val AS old_val");
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).get(0));    // id (current/NEW)
        assertEquals("99", res.get(0).get(1));   // val (current/NEW)
        assertEquals("10", res.get(0).get(2));   // OLD.val
    }

    // ========================================================================
    // INSERT OLD.* + NEW.* together (both should work)
    // ========================================================================

    @Test
    void insert_returning_old_and_new_star_together() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");

        List<List<String>> res = query(
                "INSERT INTO t1 VALUES (1, 100) RETURNING OLD.*, NEW.*");
        assertEquals(1, res.size());
        // OLD: NULL, NULL; NEW: 1, 100
        assertNull(res.get(0).get(0));
        assertNull(res.get(0).get(1));
        assertEquals("1", res.get(0).get(2));
        assertEquals("100", res.get(0).get(3));
    }

    @Test
    void delete_returning_old_and_new_star_together() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");

        List<List<String>> res = query(
                "DELETE FROM t1 RETURNING OLD.*, NEW.*");
        assertEquals(1, res.size());
        // OLD: 1, 10; NEW: NULL, NULL
        assertEquals("1", res.get(0).get(0));
        assertEquals("10", res.get(0).get(1));
        assertNull(res.get(0).get(2));
        assertNull(res.get(0).get(3));
    }

    // ========================================================================
    // String concatenation with OLD/NEW
    // ========================================================================

    @Test
    void returning_string_concat_with_old_new() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, label text)");
        exec("INSERT INTO t1 VALUES (1, 'before')");

        List<List<String>> res = query(
                "UPDATE t1 SET label = 'after' RETURNING OLD.label || ' -> ' || NEW.label AS change");
        assertEquals(1, res.size());
        assertEquals("before -> after", res.get(0).get(0));
    }

    // ========================================================================
    // MERGE with empty target table
    // ========================================================================

    @Test
    void merge_empty_target_returning_old_new() throws SQLException {
        exec("CREATE TABLE tgt(id int PRIMARY KEY, val int)");
        exec("CREATE TABLE src(id int, val int)");
        exec("INSERT INTO src VALUES (1, 100), (2, 200)");

        List<List<String>> res = query("""
            MERGE INTO tgt AS t USING src AS s ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
            RETURNING merge_action(), OLD.val, NEW.val
            """);
        assertEquals(2, res.size());
        for (List<String> r : res) {
            assertEquals("INSERT", r.get(0));
            assertNull(r.get(1));        // OLD = NULL for INSERT
            assertNotNull(r.get(2));     // NEW = inserted val
        }
    }

    // ========================================================================
    // merge_action() in non-MERGE DML should error
    // ========================================================================

    @Test
    void merge_action_in_update_errors() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
            exec("INSERT INTO t1 VALUES (1, 10)");
        });
        SQLException ex = assertThrows(SQLException.class, () ->
                query("UPDATE t1 SET val = 99 RETURNING merge_action()"));
        assertEquals("42P20", ex.getSQLState());
    }

    @Test
    void merge_action_in_delete_errors() {
        assertDoesNotThrow(() -> {
            exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
            exec("INSERT INTO t1 VALUES (1, 10)");
        });
        SQLException ex = assertThrows(SQLException.class, () ->
                query("DELETE FROM t1 RETURNING merge_action()"));
        assertEquals("42P20", ex.getSQLState());
    }

    @Test
    void merge_action_in_insert_errors() {
        assertDoesNotThrow(() -> exec("CREATE TABLE t1(id int PRIMARY KEY, val int)"));
        SQLException ex = assertThrows(SQLException.class, () ->
                query("INSERT INTO t1 VALUES (1, 10) RETURNING merge_action()"));
        assertEquals("42P20", ex.getSQLState());
    }
}
