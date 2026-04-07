package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 11-17 (DML Commands).
 *
 * 11. INSERT
 * 12. UPDATE
 * 13. DELETE
 * 14. MERGE
 * 15. COPY
 * 16. TRUNCATE
 * 17. SELECT INTO & CREATE TABLE AS
 */
class DMLCoverageTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 11. INSERT
    // =========================================================================

    @Test
    void insert_single_row() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_single (id INTEGER, name TEXT)");
            s.execute("INSERT INTO ins_single VALUES (1, 'Alice')");
            ResultSet rs = s.executeQuery("SELECT * FROM ins_single");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());
            s.execute("DROP TABLE ins_single");
        }
    }

    @Test
    void insert_multi_row_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_multi (id INTEGER, name TEXT)");
            s.execute("INSERT INTO ins_multi VALUES (1, 'A'), (2, 'B'), (3, 'C')");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ins_multi");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            s.execute("DROP TABLE ins_multi");
        }
    }

    @Test
    void insert_with_column_list() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_cols (id INTEGER, name TEXT, age INTEGER DEFAULT 25)");
            s.execute("INSERT INTO ins_cols (id, name) VALUES (1, 'Bob')");
            ResultSet rs = s.executeQuery("SELECT age FROM ins_cols WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(25, rs.getInt(1)); // default applied
            s.execute("DROP TABLE ins_cols");
        }
    }

    @Test
    void insert_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_src (id INTEGER, val TEXT)");
            s.execute("INSERT INTO ins_src VALUES (1, 'x'), (2, 'y')");
            s.execute("CREATE TABLE ins_dst (id INTEGER, val TEXT)");
            s.execute("INSERT INTO ins_dst SELECT * FROM ins_src WHERE id = 1");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ins_dst");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            s.execute("DROP TABLE ins_src");
            s.execute("DROP TABLE ins_dst");
        }
    }

    @Test
    void insert_select_with_column_list() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_sel_src (a INTEGER, b TEXT, c INTEGER)");
            s.execute("INSERT INTO ins_sel_src VALUES (1, 'hello', 42)");
            s.execute("CREATE TABLE ins_sel_dst (x INTEGER, y TEXT)");
            s.execute("INSERT INTO ins_sel_dst (x, y) SELECT a, b FROM ins_sel_src");
            ResultSet rs = s.executeQuery("SELECT x, y FROM ins_sel_dst");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("x"));
            assertEquals("hello", rs.getString("y"));
            s.execute("DROP TABLE ins_sel_src");
            s.execute("DROP TABLE ins_sel_dst");
        }
    }

    @Test
    void insert_default_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_def (id SERIAL, name TEXT DEFAULT 'unknown')");
            s.execute("INSERT INTO ins_def DEFAULT VALUES");
            ResultSet rs = s.executeQuery("SELECT id, name FROM ins_def");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("unknown", rs.getString("name"));
            s.execute("DROP TABLE ins_def");
        }
    }

    @Test
    void insert_returning_star() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_ret (id SERIAL, name TEXT)");
            ResultSet rs = s.executeQuery("INSERT INTO ins_ret (name) VALUES ('Alice') RETURNING *");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());
            s.execute("DROP TABLE ins_ret");
        }
    }

    @Test
    void insert_returning_specific_columns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_ret2 (id SERIAL, name TEXT, age INTEGER)");
            ResultSet rs = s.executeQuery("INSERT INTO ins_ret2 (name, age) VALUES ('Bob', 30) RETURNING id, name");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Bob", rs.getString("name"));
            s.execute("DROP TABLE ins_ret2");
        }
    }

    @Test
    void insert_returning_expression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_ret_expr (id SERIAL, price NUMERIC(10,2))");
            ResultSet rs = s.executeQuery("INSERT INTO ins_ret_expr (price) VALUES (100.00) RETURNING id, price * 1.1 AS with_tax");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            // price * 1.1 = 110.00
            assertTrue(rs.getDouble("with_tax") > 109.9);
            s.execute("DROP TABLE ins_ret_expr");
        }
    }

    @Test
    void insert_on_conflict_do_nothing() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_conf (id INTEGER PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO ins_conf VALUES (1, 'Alice')");
            // Should not throw - just skip
            s.execute("INSERT INTO ins_conf VALUES (1, 'Bob') ON CONFLICT DO NOTHING");
            ResultSet rs = s.executeQuery("SELECT name FROM ins_conf WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1)); // Not updated
            s.execute("DROP TABLE ins_conf");
        }
    }

    @Test
    void insert_on_conflict_do_nothing_with_target() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_conf2 (id INTEGER PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO ins_conf2 VALUES (1, 'Alice')");
            s.execute("INSERT INTO ins_conf2 VALUES (1, 'Bob') ON CONFLICT (id) DO NOTHING");
            ResultSet rs = s.executeQuery("SELECT name FROM ins_conf2 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            s.execute("DROP TABLE ins_conf2");
        }
    }

    @Test
    void insert_on_conflict_do_update() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_upsert (id INTEGER PRIMARY KEY, name TEXT, counter INTEGER DEFAULT 0)");
            s.execute("INSERT INTO ins_upsert VALUES (1, 'Alice', 1)");
            s.execute("INSERT INTO ins_upsert VALUES (1, 'Alice', 1) ON CONFLICT (id) DO UPDATE SET counter = ins_upsert.counter + 1");
            ResultSet rs = s.executeQuery("SELECT counter FROM ins_upsert WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE ins_upsert");
        }
    }

    @Test
    void insert_on_conflict_do_update_with_excluded() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_excl (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)");
            s.execute("INSERT INTO ins_excl VALUES (1, 'old', 10)");
            s.execute("INSERT INTO ins_excl VALUES (1, 'new', 20) ON CONFLICT (id) DO UPDATE SET name = excluded.name, val = excluded.val");
            ResultSet rs = s.executeQuery("SELECT name, val FROM ins_excl WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("new", rs.getString("name"));
            assertEquals(20, rs.getInt("val"));
            s.execute("DROP TABLE ins_excl");
        }
    }

    @Test
    void insert_on_conflict_on_constraint() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_constr (id INTEGER, name TEXT, CONSTRAINT ins_constr_pk PRIMARY KEY (id))");
            s.execute("INSERT INTO ins_constr VALUES (1, 'Alice')");
            s.execute("INSERT INTO ins_constr VALUES (1, 'Bob') ON CONFLICT ON CONSTRAINT ins_constr_pk DO NOTHING");
            ResultSet rs = s.executeQuery("SELECT name FROM ins_constr WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            s.execute("DROP TABLE ins_constr");
        }
    }

    @Test
    void insert_on_conflict_do_update_returning() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_uret (id INTEGER PRIMARY KEY, val INTEGER)");
            s.execute("INSERT INTO ins_uret VALUES (1, 10)");
            ResultSet rs = s.executeQuery("INSERT INTO ins_uret VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = excluded.val RETURNING id, val");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(20, rs.getInt("val"));
            s.execute("DROP TABLE ins_uret");
        }
    }

    @Test
    void insert_serial_column() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_serial (id SERIAL PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO ins_serial (name) VALUES ('A')");
            s.execute("INSERT INTO ins_serial (name) VALUES ('B')");
            s.execute("INSERT INTO ins_serial (name) VALUES ('C')");
            ResultSet rs = s.executeQuery("SELECT id FROM ins_serial ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            s.execute("DROP TABLE ins_serial");
        }
    }

    @Test
    void insert_null_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_null (id INTEGER, val TEXT)");
            s.execute("INSERT INTO ins_null VALUES (1, NULL)");
            ResultSet rs = s.executeQuery("SELECT val FROM ins_null WHERE id = 1");
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            s.execute("DROP TABLE ins_null");
        }
    }

    @Test
    void insert_with_schema_qualified() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_schema (id INTEGER, val TEXT)");
            s.execute("INSERT INTO public.ins_schema VALUES (1, 'test')");
            ResultSet rs = s.executeQuery("SELECT val FROM ins_schema");
            assertTrue(rs.next());
            assertEquals("test", rs.getString(1));
            s.execute("DROP TABLE ins_schema");
        }
    }

    @Test
    void insert_cte() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ins_cte_src (id INTEGER, val TEXT)");
            s.execute("INSERT INTO ins_cte_src VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            s.execute("CREATE TABLE ins_cte_dst (id INTEGER, val TEXT)");
            s.execute("WITH src AS (SELECT * FROM ins_cte_src WHERE id <= 2) INSERT INTO ins_cte_dst SELECT * FROM src");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ins_cte_dst");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE ins_cte_src");
            s.execute("DROP TABLE ins_cte_dst");
        }
    }

    // =========================================================================
    // 12. UPDATE
    // =========================================================================

    @Test
    void update_single_column() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_single (id INTEGER, name TEXT)");
            s.execute("INSERT INTO upd_single VALUES (1, 'Alice'), (2, 'Bob')");
            s.execute("UPDATE upd_single SET name = 'Charlie' WHERE id = 1");
            ResultSet rs = s.executeQuery("SELECT name FROM upd_single WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Charlie", rs.getString(1));
            s.execute("DROP TABLE upd_single");
        }
    }

    @Test
    void update_multiple_columns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_multi (id INTEGER, name TEXT, age INTEGER)");
            s.execute("INSERT INTO upd_multi VALUES (1, 'Alice', 25)");
            s.execute("UPDATE upd_multi SET name = 'Bob', age = 30 WHERE id = 1");
            ResultSet rs = s.executeQuery("SELECT name, age FROM upd_multi WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("name"));
            assertEquals(30, rs.getInt("age"));
            s.execute("DROP TABLE upd_multi");
        }
    }

    @Test
    void update_with_expression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_expr (id INTEGER, price NUMERIC(10,2))");
            s.execute("INSERT INTO upd_expr VALUES (1, 100.00)");
            s.execute("UPDATE upd_expr SET price = price * 1.1 WHERE id = 1");
            ResultSet rs = s.executeQuery("SELECT price FROM upd_expr WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(110.00, rs.getDouble(1), 0.01);
            s.execute("DROP TABLE upd_expr");
        }
    }

    @Test
    void update_all_rows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_all (id INTEGER, status TEXT)");
            s.execute("INSERT INTO upd_all VALUES (1, 'active'), (2, 'active'), (3, 'active')");
            s.execute("UPDATE upd_all SET status = 'inactive'");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM upd_all WHERE status = 'inactive'");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            s.execute("DROP TABLE upd_all");
        }
    }

    @Test
    void update_from_clause() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_main (id INTEGER, category TEXT, discount NUMERIC(5,2))");
            s.execute("INSERT INTO upd_main VALUES (1, 'A', 0), (2, 'B', 0), (3, 'A', 0)");
            s.execute("CREATE TABLE upd_rates (category TEXT, rate NUMERIC(5,2))");
            s.execute("INSERT INTO upd_rates VALUES ('A', 10.00), ('B', 5.00)");
            s.execute("UPDATE upd_main SET discount = upd_rates.rate FROM upd_rates WHERE upd_main.category = upd_rates.category");
            ResultSet rs = s.executeQuery("SELECT discount FROM upd_main WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(10.00, rs.getDouble(1), 0.01);
            rs = s.executeQuery("SELECT discount FROM upd_main WHERE id = 2");
            assertTrue(rs.next());
            assertEquals(5.00, rs.getDouble(1), 0.01);
            s.execute("DROP TABLE upd_main");
            s.execute("DROP TABLE upd_rates");
        }
    }

    @Test
    void update_returning() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_ret (id INTEGER, val TEXT)");
            s.execute("INSERT INTO upd_ret VALUES (1, 'old')");
            ResultSet rs = s.executeQuery("UPDATE upd_ret SET val = 'new' WHERE id = 1 RETURNING id, val");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("new", rs.getString("val"));
            assertFalse(rs.next());
            s.execute("DROP TABLE upd_ret");
        }
    }

    @Test
    void update_returning_star() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_ret_star (id INTEGER, name TEXT, age INTEGER)");
            s.execute("INSERT INTO upd_ret_star VALUES (1, 'Alice', 25)");
            ResultSet rs = s.executeQuery("UPDATE upd_ret_star SET age = 26 WHERE id = 1 RETURNING *");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertEquals(26, rs.getInt("age"));
            s.execute("DROP TABLE upd_ret_star");
        }
    }

    @Test
    void update_with_cte() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_cte (id INTEGER, val TEXT)");
            s.execute("INSERT INTO upd_cte VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            s.execute("WITH target AS (SELECT id FROM upd_cte WHERE id <= 2) UPDATE upd_cte SET val = 'updated' WHERE id IN (SELECT id FROM target)");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM upd_cte WHERE val = 'updated'");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE upd_cte");
        }
    }

    @Test
    void update_set_from_subquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_sub (id INTEGER, total INTEGER)");
            s.execute("INSERT INTO upd_sub VALUES (1, 0)");
            s.execute("CREATE TABLE upd_sub_items (parent_id INTEGER, amount INTEGER)");
            s.execute("INSERT INTO upd_sub_items VALUES (1, 10), (1, 20), (1, 30)");
            s.execute("UPDATE upd_sub SET total = (SELECT SUM(amount) FROM upd_sub_items WHERE parent_id = upd_sub.id) WHERE id = 1");
            ResultSet rs = s.executeQuery("SELECT total FROM upd_sub WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(60, rs.getInt(1));
            s.execute("DROP TABLE upd_sub_items");
            s.execute("DROP TABLE upd_sub");
        }
    }

    @Test
    void update_where_not_matched() throws SQLException {
        // UPDATE with WHERE that matches no rows should update 0
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE upd_nomatch (id INTEGER, val TEXT)");
            s.execute("INSERT INTO upd_nomatch VALUES (1, 'a')");
            s.execute("UPDATE upd_nomatch SET val = 'b' WHERE id = 999");
            ResultSet rs = s.executeQuery("SELECT val FROM upd_nomatch WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1)); // unchanged
            s.execute("DROP TABLE upd_nomatch");
        }
    }

    // =========================================================================
    // 13. DELETE
    // =========================================================================

    @Test
    void delete_with_where() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE del_where (id INTEGER, name TEXT)");
            s.execute("INSERT INTO del_where VALUES (1, 'A'), (2, 'B'), (3, 'C')");
            s.execute("DELETE FROM del_where WHERE id = 2");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM del_where");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE del_where");
        }
    }

    @Test
    void delete_all_rows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE del_all (id INTEGER)");
            s.execute("INSERT INTO del_all VALUES (1), (2), (3)");
            s.execute("DELETE FROM del_all");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM del_all");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            s.execute("DROP TABLE del_all");
        }
    }

    @Test
    void delete_returning() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE del_ret (id INTEGER, name TEXT)");
            s.execute("INSERT INTO del_ret VALUES (1, 'Alice'), (2, 'Bob')");
            ResultSet rs = s.executeQuery("DELETE FROM del_ret WHERE id = 1 RETURNING *");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());
            // Verify actually deleted
            rs = s.executeQuery("SELECT COUNT(*) FROM del_ret");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            s.execute("DROP TABLE del_ret");
        }
    }

    @Test
    void delete_returning_expression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE del_ret_expr (id INTEGER, price NUMERIC(10,2))");
            s.execute("INSERT INTO del_ret_expr VALUES (1, 100.00)");
            ResultSet rs = s.executeQuery("DELETE FROM del_ret_expr WHERE id = 1 RETURNING id, price * 2 AS doubled");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(200.00, rs.getDouble("doubled"), 0.01);
            s.execute("DROP TABLE del_ret_expr");
        }
    }

    @Test
    void delete_using_clause() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE del_main (id INTEGER, category TEXT)");
            s.execute("INSERT INTO del_main VALUES (1, 'keep'), (2, 'remove'), (3, 'remove')");
            s.execute("CREATE TABLE del_remove (category TEXT)");
            s.execute("INSERT INTO del_remove VALUES ('remove')");
            s.execute("DELETE FROM del_main USING del_remove WHERE del_main.category = del_remove.category");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM del_main");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            rs = s.executeQuery("SELECT id FROM del_main");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            s.execute("DROP TABLE del_main");
            s.execute("DROP TABLE del_remove");
        }
    }

    @Test
    void delete_with_cte() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE del_cte (id INTEGER, val TEXT)");
            s.execute("INSERT INTO del_cte VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            s.execute("WITH to_del AS (SELECT id FROM del_cte WHERE id > 1) DELETE FROM del_cte WHERE id IN (SELECT id FROM to_del)");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM del_cte");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            s.execute("DROP TABLE del_cte");
        }
    }

    @Test
    void delete_complex_where() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE del_complex (id INTEGER, status TEXT, priority INTEGER)");
            s.execute("INSERT INTO del_complex VALUES (1, 'done', 1), (2, 'active', 2), (3, 'done', 3), (4, 'active', 1)");
            s.execute("DELETE FROM del_complex WHERE status = 'done' AND priority < 3");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM del_complex");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1)); // Only id=1 deleted
            s.execute("DROP TABLE del_complex");
        }
    }

    @Test
    void delete_with_in_subquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE del_parent (id INTEGER PRIMARY KEY, name TEXT)");
            s.execute("INSERT INTO del_parent VALUES (1, 'A'), (2, 'B'), (3, 'C')");
            s.execute("CREATE TABLE del_child (parent_id INTEGER, val TEXT)");
            s.execute("INSERT INTO del_child VALUES (1, 'x'), (3, 'y')");
            // Delete parents that have children
            s.execute("DELETE FROM del_parent WHERE id IN (SELECT parent_id FROM del_child)");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM del_parent");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // Only id=2 remains
            s.execute("DROP TABLE del_child");
            s.execute("DROP TABLE del_parent");
        }
    }

    // =========================================================================
    // 14. MERGE
    // =========================================================================

    @Test
    void merge_insert_when_not_matched() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mrg_target (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("CREATE TABLE mrg_source (id INTEGER, val TEXT)");
            s.execute("INSERT INTO mrg_source VALUES (1, 'a'), (2, 'b')");
            s.execute("MERGE INTO mrg_target USING mrg_source ON mrg_target.id = mrg_source.id " +
                       "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (mrg_source.id, mrg_source.val)");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM mrg_target");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE mrg_source");
            s.execute("DROP TABLE mrg_target");
        }
    }

    @Test
    void merge_update_when_matched() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mrg_upd_t (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO mrg_upd_t VALUES (1, 'old'), (2, 'old')");
            s.execute("CREATE TABLE mrg_upd_s (id INTEGER, val TEXT)");
            s.execute("INSERT INTO mrg_upd_s VALUES (1, 'new'), (3, 'new')");
            s.execute("MERGE INTO mrg_upd_t USING mrg_upd_s ON mrg_upd_t.id = mrg_upd_s.id " +
                       "WHEN MATCHED THEN UPDATE SET val = mrg_upd_s.val " +
                       "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (mrg_upd_s.id, mrg_upd_s.val)");
            // id=1 should be updated, id=3 should be inserted
            ResultSet rs = s.executeQuery("SELECT val FROM mrg_upd_t WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("new", rs.getString(1));
            rs = s.executeQuery("SELECT COUNT(*) FROM mrg_upd_t");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            s.execute("DROP TABLE mrg_upd_s");
            s.execute("DROP TABLE mrg_upd_t");
        }
    }

    @Test
    void merge_delete_when_matched() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mrg_del_t (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO mrg_del_t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            s.execute("CREATE TABLE mrg_del_s (id INTEGER)");
            s.execute("INSERT INTO mrg_del_s VALUES (1), (2)");
            s.execute("MERGE INTO mrg_del_t USING mrg_del_s ON mrg_del_t.id = mrg_del_s.id " +
                       "WHEN MATCHED THEN DELETE");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM mrg_del_t");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // Only id=3 remains
            s.execute("DROP TABLE mrg_del_s");
            s.execute("DROP TABLE mrg_del_t");
        }
    }

    @Test
    void merge_with_and_condition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mrg_cond_t (id INTEGER PRIMARY KEY, val TEXT, status TEXT)");
            s.execute("INSERT INTO mrg_cond_t VALUES (1, 'a', 'active'), (2, 'b', 'inactive')");
            s.execute("CREATE TABLE mrg_cond_s (id INTEGER, val TEXT)");
            s.execute("INSERT INTO mrg_cond_s VALUES (1, 'new1'), (2, 'new2')");
            s.execute("MERGE INTO mrg_cond_t USING mrg_cond_s ON mrg_cond_t.id = mrg_cond_s.id " +
                       "WHEN MATCHED AND mrg_cond_t.status = 'active' THEN UPDATE SET val = mrg_cond_s.val " +
                       "WHEN MATCHED AND mrg_cond_t.status = 'inactive' THEN DELETE");
            // id=1 updated (was active), id=2 deleted (was inactive)
            ResultSet rs = s.executeQuery("SELECT val FROM mrg_cond_t WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("new1", rs.getString(1));
            rs = s.executeQuery("SELECT COUNT(*) FROM mrg_cond_t");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            s.execute("DROP TABLE mrg_cond_s");
            s.execute("DROP TABLE mrg_cond_t");
        }
    }

    @Test
    void merge_do_nothing() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mrg_noop_t (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO mrg_noop_t VALUES (1, 'keep')");
            s.execute("CREATE TABLE mrg_noop_s (id INTEGER, val TEXT)");
            s.execute("INSERT INTO mrg_noop_s VALUES (1, 'ignore'), (2, 'ignore')");
            s.execute("MERGE INTO mrg_noop_t USING mrg_noop_s ON mrg_noop_t.id = mrg_noop_s.id " +
                       "WHEN MATCHED THEN DO NOTHING " +
                       "WHEN NOT MATCHED THEN DO NOTHING");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM mrg_noop_t");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // No changes
            rs = s.executeQuery("SELECT val FROM mrg_noop_t WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("keep", rs.getString(1));
            s.execute("DROP TABLE mrg_noop_s");
            s.execute("DROP TABLE mrg_noop_t");
        }
    }

    @Test
    void merge_with_subquery_source() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mrg_sub_t (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("CREATE TABLE mrg_sub_src (id INTEGER, val TEXT, active BOOLEAN)");
            s.execute("INSERT INTO mrg_sub_src VALUES (1, 'a', TRUE), (2, 'b', FALSE), (3, 'c', TRUE)");
            s.execute("MERGE INTO mrg_sub_t " +
                       "USING (SELECT id, val FROM mrg_sub_src WHERE active = TRUE) AS src ON mrg_sub_t.id = src.id " +
                       "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM mrg_sub_t");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1)); // Only active rows inserted
            s.execute("DROP TABLE mrg_sub_src");
            s.execute("DROP TABLE mrg_sub_t");
        }
    }

    @Test
    void merge_with_alias() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mrg_alias_t (id INTEGER PRIMARY KEY, val INTEGER)");
            s.execute("INSERT INTO mrg_alias_t VALUES (1, 10)");
            s.execute("CREATE TABLE mrg_alias_s (id INTEGER, val INTEGER)");
            s.execute("INSERT INTO mrg_alias_s VALUES (1, 20), (2, 30)");
            s.execute("MERGE INTO mrg_alias_t AS t " +
                       "USING mrg_alias_s AS s ON t.id = s.id " +
                       "WHEN MATCHED THEN UPDATE SET val = s.val " +
                       "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)");
            ResultSet rs = s.executeQuery("SELECT val FROM mrg_alias_t WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
            rs = s.executeQuery("SELECT COUNT(*) FROM mrg_alias_t");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE mrg_alias_s");
            s.execute("DROP TABLE mrg_alias_t");
        }
    }

    // =========================================================================
    // 15. COPY
    // =========================================================================

    @Test
    void copy_to_stdout() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE copy_out (id INTEGER, name TEXT)");
            s.execute("INSERT INTO copy_out VALUES (1, 'Alice'), (2, 'Bob')");
            // COPY TO STDOUT requires PG COPY protocol, not available via JDBC
            assertThrows(SQLException.class, () -> s.execute("COPY copy_out TO STDOUT"));
            s.execute("DROP TABLE copy_out");
        }
    }

    @Test
    void copy_to_stdout_with_columns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE copy_cols (id INTEGER, name TEXT, age INTEGER)");
            s.execute("INSERT INTO copy_cols VALUES (1, 'Alice', 30)");
            assertThrows(SQLException.class, () -> s.execute("COPY copy_cols (id, name) TO STDOUT"));
            s.execute("DROP TABLE copy_cols");
        }
    }

    @Test
    void copy_to_stdout_csv_format() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE copy_csv (id INTEGER, name TEXT)");
            s.execute("INSERT INTO copy_csv VALUES (1, 'Alice')");
            assertThrows(SQLException.class, () -> s.execute("COPY copy_csv TO STDOUT WITH (FORMAT csv)"));
            s.execute("DROP TABLE copy_csv");
        }
    }

    @Test
    void copy_to_stdout_csv_header() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE copy_hdr (id INTEGER, name TEXT)");
            s.execute("INSERT INTO copy_hdr VALUES (1, 'Alice')");
            assertThrows(SQLException.class, () -> s.execute("COPY copy_hdr TO STDOUT WITH (FORMAT csv, HEADER)"));
            s.execute("DROP TABLE copy_hdr");
        }
    }

    // =========================================================================
    // 16. TRUNCATE
    // =========================================================================

    @Test
    void truncate_basic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trunc_basic (id INTEGER, val TEXT)");
            s.execute("INSERT INTO trunc_basic VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            s.execute("TRUNCATE trunc_basic");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM trunc_basic");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            s.execute("DROP TABLE trunc_basic");
        }
    }

    @Test
    void truncate_with_table_keyword() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trunc_tbl (id INTEGER)");
            s.execute("INSERT INTO trunc_tbl VALUES (1), (2)");
            s.execute("TRUNCATE TABLE trunc_tbl");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM trunc_tbl");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            s.execute("DROP TABLE trunc_tbl");
        }
    }

    @Test
    void truncate_restart_identity() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trunc_restart (id SERIAL, name TEXT)");
            s.execute("INSERT INTO trunc_restart (name) VALUES ('A'), ('B')");
            s.execute("TRUNCATE trunc_restart RESTART IDENTITY");
            s.execute("INSERT INTO trunc_restart (name) VALUES ('C')");
            ResultSet rs = s.executeQuery("SELECT id FROM trunc_restart");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // Reset to 1
            s.execute("DROP TABLE trunc_restart");
        }
    }

    @Test
    void truncate_continue_identity() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trunc_continue (id SERIAL, name TEXT)");
            s.execute("INSERT INTO trunc_continue (name) VALUES ('A'), ('B')");
            s.execute("TRUNCATE trunc_continue CONTINUE IDENTITY");
            s.execute("INSERT INTO trunc_continue (name) VALUES ('C')");
            ResultSet rs = s.executeQuery("SELECT id FROM trunc_continue");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1)); // Continues from 3
            s.execute("DROP TABLE trunc_continue");
        }
    }

    @Test
    void truncate_default_continues_identity() throws SQLException {
        // Without specifying, TRUNCATE should default to CONTINUE IDENTITY (PG behavior)
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trunc_def (id SERIAL, name TEXT)");
            s.execute("INSERT INTO trunc_def (name) VALUES ('A'), ('B')");
            s.execute("TRUNCATE trunc_def");
            s.execute("INSERT INTO trunc_def (name) VALUES ('C')");
            ResultSet rs = s.executeQuery("SELECT id FROM trunc_def");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1)); // Continues from 3
            s.execute("DROP TABLE trunc_def");
        }
    }

    @Test
    void truncate_multiple_tables() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trunc_m1 (id INTEGER)");
            s.execute("CREATE TABLE trunc_m2 (id INTEGER)");
            s.execute("INSERT INTO trunc_m1 VALUES (1), (2)");
            s.execute("INSERT INTO trunc_m2 VALUES (3), (4)");
            s.execute("TRUNCATE trunc_m1, trunc_m2");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM trunc_m1");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            rs = s.executeQuery("SELECT COUNT(*) FROM trunc_m2");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            s.execute("DROP TABLE trunc_m1");
            s.execute("DROP TABLE trunc_m2");
        }
    }

    @Test
    void truncate_cascade() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trunc_casc (id INTEGER)");
            s.execute("INSERT INTO trunc_casc VALUES (1)");
            s.execute("TRUNCATE trunc_casc CASCADE");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM trunc_casc");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            s.execute("DROP TABLE trunc_casc");
        }
    }

    @Test
    void truncate_restrict() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trunc_restr (id INTEGER)");
            s.execute("INSERT INTO trunc_restr VALUES (1)");
            s.execute("TRUNCATE trunc_restr RESTRICT");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM trunc_restr");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            s.execute("DROP TABLE trunc_restr");
        }
    }

    @Test
    void truncate_multi_table_restart_identity() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE trunc_mr1 (id SERIAL, name TEXT)");
            s.execute("CREATE TABLE trunc_mr2 (id SERIAL, name TEXT)");
            s.execute("INSERT INTO trunc_mr1 (name) VALUES ('A'), ('B')");
            s.execute("INSERT INTO trunc_mr2 (name) VALUES ('X'), ('Y'), ('Z')");
            s.execute("TRUNCATE trunc_mr1, trunc_mr2 RESTART IDENTITY");
            s.execute("INSERT INTO trunc_mr1 (name) VALUES ('C')");
            s.execute("INSERT INTO trunc_mr2 (name) VALUES ('W')");
            ResultSet rs = s.executeQuery("SELECT id FROM trunc_mr1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            rs = s.executeQuery("SELECT id FROM trunc_mr2");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            s.execute("DROP TABLE trunc_mr1");
            s.execute("DROP TABLE trunc_mr2");
        }
    }

    // =========================================================================
    // 17. SELECT INTO & CREATE TABLE AS
    // =========================================================================

    @Test
    void create_table_as_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ctas_src (id INTEGER, name TEXT, active BOOLEAN)");
            s.execute("INSERT INTO ctas_src VALUES (1, 'A', TRUE), (2, 'B', FALSE), (3, 'C', TRUE)");
            s.execute("CREATE TABLE ctas_dst AS SELECT id, name FROM ctas_src WHERE active = TRUE");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ctas_dst");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            rs = s.executeQuery("SELECT name FROM ctas_dst ORDER BY id");
            assertTrue(rs.next()); assertEquals("A", rs.getString(1));
            assertTrue(rs.next()); assertEquals("C", rs.getString(1));
            s.execute("DROP TABLE ctas_dst");
            s.execute("DROP TABLE ctas_src");
        }
    }

    @Test
    void create_table_as_select_if_not_exists() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ctas_ine_src (id INTEGER)");
            s.execute("INSERT INTO ctas_ine_src VALUES (1)");
            s.execute("CREATE TABLE ctas_ine AS SELECT * FROM ctas_ine_src");
            // Should not throw
            s.execute("CREATE TABLE IF NOT EXISTS ctas_ine AS SELECT * FROM ctas_ine_src");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ctas_ine");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // Still 1 row from first create
            s.execute("DROP TABLE ctas_ine");
            s.execute("DROP TABLE ctas_ine_src");
        }
    }

    @Test
    void create_table_as_with_expression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ctas_expr AS SELECT 1 AS num, 'hello' AS greeting, 2 + 3 AS sum");
            ResultSet rs = s.executeQuery("SELECT num, greeting, sum FROM ctas_expr");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));
            assertEquals("hello", rs.getString("greeting"));
            assertEquals(5, rs.getInt("sum"));
            s.execute("DROP TABLE ctas_expr");
        }
    }

    @Test
    void create_table_as_with_aggregation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ctas_agg_src (category TEXT, amount INTEGER)");
            s.execute("INSERT INTO ctas_agg_src VALUES ('A', 10), ('A', 20), ('B', 30)");
            s.execute("CREATE TABLE ctas_agg AS SELECT category, SUM(amount) AS total FROM ctas_agg_src GROUP BY category");
            ResultSet rs = s.executeQuery("SELECT total FROM ctas_agg WHERE category = 'A'");
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
            s.execute("DROP TABLE ctas_agg");
            s.execute("DROP TABLE ctas_agg_src");
        }
    }

    @Test
    void create_temp_table_as() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ctas_tmp_src (id INTEGER, val TEXT)");
            s.execute("INSERT INTO ctas_tmp_src VALUES (1, 'x'), (2, 'y')");
            s.execute("CREATE TEMP TABLE ctas_tmp AS SELECT * FROM ctas_tmp_src");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ctas_tmp");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE ctas_tmp");
            s.execute("DROP TABLE ctas_tmp_src");
        }
    }

    @Test
    void select_into_new_table() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE selinto_src (id INTEGER, name TEXT)");
            s.execute("INSERT INTO selinto_src VALUES (1, 'A'), (2, 'B'), (3, 'C')");
            s.execute("SELECT id, name INTO selinto_dst FROM selinto_src WHERE id <= 2");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM selinto_dst");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE selinto_dst");
            s.execute("DROP TABLE selinto_src");
        }
    }

    @Test
    void select_into_temp_table() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE selinto_tmp_src (id INTEGER, val TEXT)");
            s.execute("INSERT INTO selinto_tmp_src VALUES (1, 'x')");
            s.execute("SELECT * INTO TEMP selinto_tmp FROM selinto_tmp_src");
            ResultSet rs = s.executeQuery("SELECT val FROM selinto_tmp WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
            s.execute("DROP TABLE selinto_tmp");
            s.execute("DROP TABLE selinto_tmp_src");
        }
    }

    @Test
    void create_table_as_with_cte() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ctas_cte_src (id INTEGER, val TEXT)");
            s.execute("INSERT INTO ctas_cte_src VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            s.execute("CREATE TABLE ctas_cte AS WITH sub AS (SELECT * FROM ctas_cte_src WHERE id <= 2) SELECT * FROM sub");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ctas_cte");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE ctas_cte");
            s.execute("DROP TABLE ctas_cte_src");
        }
    }

    @Test
    void create_table_as_with_join() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ctas_j1 (id INTEGER, name TEXT)");
            s.execute("CREATE TABLE ctas_j2 (id INTEGER, score INTEGER)");
            s.execute("INSERT INTO ctas_j1 VALUES (1, 'Alice'), (2, 'Bob')");
            s.execute("INSERT INTO ctas_j2 VALUES (1, 95), (2, 87)");
            s.execute("CREATE TABLE ctas_joined AS SELECT ctas_j1.name, ctas_j2.score FROM ctas_j1 JOIN ctas_j2 ON ctas_j1.id = ctas_j2.id");
            ResultSet rs = s.executeQuery("SELECT name, score FROM ctas_joined ORDER BY name");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(95, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(87, rs.getInt(2));
            s.execute("DROP TABLE ctas_joined");
            s.execute("DROP TABLE ctas_j1");
            s.execute("DROP TABLE ctas_j2");
        }
    }

    @Test
    void create_table_as_preserves_column_names() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ctas_names AS SELECT 1 AS first_col, 'hello' AS second_col");
            ResultSet rs = s.executeQuery("SELECT first_col, second_col FROM ctas_names");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("first_col"));
            assertEquals("hello", rs.getString("second_col"));
            s.execute("DROP TABLE ctas_names");
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void assertRowCount(Statement s, String sql, int expected) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
            assertEquals(expected, count, "Row count mismatch for: " + sql);
        }
    }
}
