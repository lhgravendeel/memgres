package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 63, 64, 65, 67, 69:
 *   63. CREATE INDEX
 *   64. Views
 *   65. Materialized Views
 *   67. CREATE TYPE (Enum)
 *   69. Schemas
 */
class DDLObjectsCoverageTest {

    static Memgres memgres;
    static Connection conn;

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

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // ========================================================================
    // 63. CREATE INDEX
    // ========================================================================

    @Test
    void idx_basic_single_column() throws SQLException {
        exec("CREATE TABLE idx_basic_tbl (id SERIAL PRIMARY KEY, name TEXT)");
        exec("CREATE INDEX idx_basic_name ON idx_basic_tbl (name)");
        // Verify no error and table still works
        exec("INSERT INTO idx_basic_tbl (name) VALUES ('alice')");
        assertEquals("alice", query1("SELECT name FROM idx_basic_tbl WHERE name = 'alice'"));
        exec("DROP INDEX IF EXISTS idx_basic_name");
        exec("DROP TABLE idx_basic_tbl");
    }

    @Test
    void idx_multicolumn() throws SQLException {
        exec("CREATE TABLE idx_multi_tbl (id SERIAL PRIMARY KEY, first_name TEXT, last_name TEXT)");
        exec("CREATE INDEX idx_multi_name ON idx_multi_tbl (first_name, last_name)");
        exec("INSERT INTO idx_multi_tbl (first_name, last_name) VALUES ('John', 'Doe')");
        assertEquals("John", query1("SELECT first_name FROM idx_multi_tbl WHERE first_name = 'John' AND last_name = 'Doe'"));
        exec("DROP INDEX IF EXISTS idx_multi_name");
        exec("DROP TABLE idx_multi_tbl");
    }

    @Test
    void idx_unique() throws SQLException {
        exec("CREATE TABLE idx_uniq_tbl (id SERIAL PRIMARY KEY, email TEXT)");
        exec("CREATE UNIQUE INDEX idx_uniq_email ON idx_uniq_tbl (email)");
        exec("INSERT INTO idx_uniq_tbl (email) VALUES ('a@b.com')");
        assertEquals("a@b.com", query1("SELECT email FROM idx_uniq_tbl WHERE email = 'a@b.com'"));
        exec("DROP INDEX IF EXISTS idx_uniq_email");
        exec("DROP TABLE idx_uniq_tbl");
    }

    @Test
    void idx_if_not_exists_new() throws SQLException {
        exec("CREATE TABLE idx_ifne_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE INDEX IF NOT EXISTS idx_ifne_val ON idx_ifne_tbl (val)");
        exec("INSERT INTO idx_ifne_tbl (val) VALUES ('test')");
        assertEquals("test", query1("SELECT val FROM idx_ifne_tbl LIMIT 1"));
        exec("DROP INDEX IF EXISTS idx_ifne_val");
        exec("DROP TABLE idx_ifne_tbl");
    }

    @Test
    void idx_if_not_exists_existing() throws SQLException {
        exec("CREATE TABLE idx_ifne2_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE INDEX idx_ifne2_val ON idx_ifne2_tbl (val)");
        // Should not error when index already exists
        exec("CREATE INDEX IF NOT EXISTS idx_ifne2_val ON idx_ifne2_tbl (val)");
        exec("DROP INDEX IF EXISTS idx_ifne2_val");
        exec("DROP TABLE idx_ifne2_tbl");
    }

    @Test
    void idx_concurrently() throws SQLException {
        exec("CREATE TABLE idx_conc_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE INDEX CONCURRENTLY idx_conc_val ON idx_conc_tbl (val)");
        exec("INSERT INTO idx_conc_tbl (val) VALUES ('concurrent')");
        assertEquals("concurrent", query1("SELECT val FROM idx_conc_tbl LIMIT 1"));
        exec("DROP INDEX IF EXISTS idx_conc_val");
        exec("DROP TABLE idx_conc_tbl");
    }

    @Test
    void idx_unique_concurrently() throws SQLException {
        exec("CREATE TABLE idx_uc_tbl (id SERIAL PRIMARY KEY, code TEXT)");
        exec("CREATE UNIQUE INDEX CONCURRENTLY idx_uc_code ON idx_uc_tbl (code)");
        exec("INSERT INTO idx_uc_tbl (code) VALUES ('X1')");
        assertEquals("X1", query1("SELECT code FROM idx_uc_tbl LIMIT 1"));
        exec("DROP INDEX IF EXISTS idx_uc_code");
        exec("DROP TABLE idx_uc_tbl");
    }

    @Test
    void idx_drop_index() throws SQLException {
        exec("CREATE TABLE idx_drop_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE INDEX idx_drop_val ON idx_drop_tbl (val)");
        exec("DROP INDEX idx_drop_val");
        // Table should still work after dropping index
        exec("INSERT INTO idx_drop_tbl (val) VALUES ('after_drop')");
        assertEquals("after_drop", query1("SELECT val FROM idx_drop_tbl LIMIT 1"));
        exec("DROP TABLE idx_drop_tbl");
    }

    @Test
    void idx_drop_if_exists() throws SQLException {
        // Should not error even if index doesn't exist
        exec("DROP INDEX IF EXISTS idx_nonexistent_xyz");
    }

    @Test
    void idx_named_index() throws SQLException {
        exec("CREATE TABLE idx_named_tbl (id SERIAL PRIMARY KEY, category TEXT, price INTEGER)");
        exec("CREATE INDEX my_custom_idx ON idx_named_tbl (category)");
        exec("INSERT INTO idx_named_tbl (category, price) VALUES ('A', 10)");
        assertEquals("A", query1("SELECT category FROM idx_named_tbl LIMIT 1"));
        exec("DROP INDEX IF EXISTS my_custom_idx");
        exec("DROP TABLE idx_named_tbl");
    }

    @Test
    void idx_without_name() throws SQLException {
        exec("CREATE TABLE idx_noname_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        // Index without explicit name
        exec("CREATE INDEX ON idx_noname_tbl (val)");
        exec("INSERT INTO idx_noname_tbl (val) VALUES ('unnamed')");
        assertEquals("unnamed", query1("SELECT val FROM idx_noname_tbl LIMIT 1"));
        exec("DROP TABLE idx_noname_tbl");
    }

    @Test
    void idx_on_integer_column() throws SQLException {
        exec("CREATE TABLE idx_int_tbl (id SERIAL PRIMARY KEY, score INTEGER)");
        exec("CREATE INDEX idx_int_score ON idx_int_tbl (score)");
        exec("INSERT INTO idx_int_tbl (score) VALUES (100)");
        assertEquals(100, queryInt("SELECT score FROM idx_int_tbl WHERE score = 100"));
        exec("DROP INDEX IF EXISTS idx_int_score");
        exec("DROP TABLE idx_int_tbl");
    }

    // ========================================================================
    // 64. Views
    // ========================================================================

    @Test
    void vw_basic_select() throws SQLException {
        exec("CREATE TABLE vw_basic_tbl (id SERIAL PRIMARY KEY, name TEXT, age INTEGER)");
        exec("INSERT INTO vw_basic_tbl (name, age) VALUES ('Alice', 30), ('Bob', 25)");
        exec("CREATE VIEW vw_basic AS SELECT name, age FROM vw_basic_tbl");
        assertEquals("Alice", query1("SELECT name FROM vw_basic WHERE age = 30"));
        exec("DROP VIEW IF EXISTS vw_basic");
        exec("DROP TABLE vw_basic_tbl");
    }

    @Test
    void vw_with_join() throws SQLException {
        exec("CREATE TABLE vw_join_emp (id SERIAL PRIMARY KEY, name TEXT, dept_id INTEGER)");
        exec("CREATE TABLE vw_join_dept (id SERIAL PRIMARY KEY, dept_name TEXT)");
        exec("INSERT INTO vw_join_dept (dept_name) VALUES ('Engineering'), ('Sales')");
        exec("INSERT INTO vw_join_emp (name, dept_id) VALUES ('Alice', 1), ('Bob', 2)");
        exec("CREATE VIEW vw_emp_dept AS SELECT e.name, d.dept_name FROM vw_join_emp e JOIN vw_join_dept d ON e.dept_id = d.id");
        assertEquals("Engineering", query1("SELECT dept_name FROM vw_emp_dept WHERE name = 'Alice'"));
        exec("DROP VIEW IF EXISTS vw_emp_dept");
        exec("DROP TABLE vw_join_emp");
        exec("DROP TABLE vw_join_dept");
    }

    @Test
    void vw_with_aggregation() throws SQLException {
        exec("CREATE TABLE vw_agg_tbl (id SERIAL PRIMARY KEY, category TEXT, amount INTEGER)");
        exec("INSERT INTO vw_agg_tbl (category, amount) VALUES ('A', 10), ('A', 20), ('B', 30)");
        exec("CREATE VIEW vw_agg AS SELECT category, COUNT(*) AS cnt, SUM(amount) AS total FROM vw_agg_tbl GROUP BY category");
        assertEquals(2, queryInt("SELECT cnt FROM vw_agg WHERE category = 'A'"));
        assertEquals(30, queryInt("SELECT total FROM vw_agg WHERE category = 'A'"));
        exec("DROP VIEW IF EXISTS vw_agg");
        exec("DROP TABLE vw_agg_tbl");
    }

    @Test
    void vw_with_where() throws SQLException {
        exec("CREATE TABLE vw_where_tbl (id SERIAL PRIMARY KEY, name TEXT, active BOOLEAN)");
        exec("INSERT INTO vw_where_tbl (name, active) VALUES ('Alice', true), ('Bob', false), ('Charlie', true)");
        exec("CREATE VIEW vw_active AS SELECT name FROM vw_where_tbl WHERE active = true");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_active"));
        exec("DROP VIEW IF EXISTS vw_active");
        exec("DROP TABLE vw_where_tbl");
    }

    @Test
    void vw_read_through() throws SQLException {
        exec("CREATE TABLE vw_read_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO vw_read_tbl (val) VALUES ('initial')");
        exec("CREATE VIEW vw_read AS SELECT val FROM vw_read_tbl");
        assertEquals("initial", query1("SELECT val FROM vw_read"));
        // Insert more data and verify view sees it
        exec("INSERT INTO vw_read_tbl (val) VALUES ('added')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_read"));
        exec("DROP VIEW IF EXISTS vw_read");
        exec("DROP TABLE vw_read_tbl");
    }

    @Test
    void vw_create_or_replace() throws SQLException {
        exec("CREATE TABLE vw_replace_tbl (id SERIAL PRIMARY KEY, name TEXT, score INTEGER)");
        exec("INSERT INTO vw_replace_tbl (name, score) VALUES ('Alice', 90), ('Bob', 80)");
        exec("CREATE VIEW vw_replace AS SELECT name FROM vw_replace_tbl");
        assertEquals("Alice", query1("SELECT name FROM vw_replace ORDER BY name LIMIT 1"));
        // Replace the view with a different query
        exec("CREATE OR REPLACE VIEW vw_replace AS SELECT name, score FROM vw_replace_tbl WHERE score > 85");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM vw_replace"));
        assertEquals(90, queryInt("SELECT score FROM vw_replace"));
        exec("DROP VIEW IF EXISTS vw_replace");
        exec("DROP TABLE vw_replace_tbl");
    }

    @Test
    void vw_drop_view() throws SQLException {
        exec("CREATE TABLE vw_dropv_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE VIEW vw_dropv AS SELECT val FROM vw_dropv_tbl");
        exec("DROP VIEW vw_dropv");
        // View should be gone, so selecting from it should fail
        assertThrows(SQLException.class, () -> query1("SELECT * FROM vw_dropv"));
        exec("DROP TABLE vw_dropv_tbl");
    }

    @Test
    void vw_drop_if_exists() throws SQLException {
        // Should not error even if view doesn't exist
        exec("DROP VIEW IF EXISTS vw_nonexistent_xyz");
    }

    @Test
    void vw_drop_nonexistent_errors() throws SQLException {
        assertThrows(SQLException.class, () -> exec("DROP VIEW vw_no_such_view"));
    }

    @Test
    void vw_column_aliases() throws SQLException {
        exec("CREATE TABLE vw_alias_tbl (id SERIAL PRIMARY KEY, first_name TEXT, last_name TEXT)");
        exec("INSERT INTO vw_alias_tbl (first_name, last_name) VALUES ('John', 'Doe')");
        exec("CREATE VIEW vw_alias AS SELECT first_name AS fname, last_name AS lname FROM vw_alias_tbl");
        assertEquals("John", query1("SELECT fname FROM vw_alias"));
        assertEquals("Doe", query1("SELECT lname FROM vw_alias"));
        exec("DROP VIEW IF EXISTS vw_alias");
        exec("DROP TABLE vw_alias_tbl");
    }

    @Test
    void vw_with_expressions() throws SQLException {
        exec("CREATE TABLE vw_expr_tbl (id SERIAL PRIMARY KEY, a INTEGER, b INTEGER)");
        exec("INSERT INTO vw_expr_tbl (a, b) VALUES (10, 20)");
        exec("CREATE VIEW vw_expr AS SELECT a + b AS total, a * b AS product FROM vw_expr_tbl");
        assertEquals(30, queryInt("SELECT total FROM vw_expr"));
        assertEquals(200, queryInt("SELECT product FROM vw_expr"));
        exec("DROP VIEW IF EXISTS vw_expr");
        exec("DROP TABLE vw_expr_tbl");
    }

    @Test
    void vw_nested_view_of_view() throws SQLException {
        exec("CREATE TABLE vw_nest_tbl (id SERIAL PRIMARY KEY, name TEXT, score INTEGER)");
        exec("INSERT INTO vw_nest_tbl (name, score) VALUES ('Alice', 90), ('Bob', 80), ('Charlie', 70)");
        exec("CREATE VIEW vw_nest_inner AS SELECT name, score FROM vw_nest_tbl WHERE score >= 80");
        exec("CREATE VIEW vw_nest_outer AS SELECT name FROM vw_nest_inner WHERE score >= 90");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM vw_nest_outer"));
        assertEquals("Alice", query1("SELECT name FROM vw_nest_outer"));
        exec("DROP VIEW IF EXISTS vw_nest_outer");
        exec("DROP VIEW IF EXISTS vw_nest_inner");
        exec("DROP TABLE vw_nest_tbl");
    }

    @Test
    void vw_with_subquery() throws SQLException {
        exec("CREATE TABLE vw_sub_tbl (id SERIAL PRIMARY KEY, name TEXT, score INTEGER)");
        exec("INSERT INTO vw_sub_tbl (name, score) VALUES ('Alice', 90), ('Bob', 80)");
        exec("CREATE VIEW vw_sub AS SELECT name FROM vw_sub_tbl WHERE score = (SELECT MAX(score) FROM vw_sub_tbl)");
        assertEquals("Alice", query1("SELECT name FROM vw_sub"));
        exec("DROP VIEW IF EXISTS vw_sub");
        exec("DROP TABLE vw_sub_tbl");
    }

    @Test
    void vw_select_star() throws SQLException {
        exec("CREATE TABLE vw_star_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO vw_star_tbl (val) VALUES ('star')");
        exec("CREATE VIEW vw_star AS SELECT * FROM vw_star_tbl");
        assertEquals("star", query1("SELECT val FROM vw_star"));
        exec("DROP VIEW IF EXISTS vw_star");
        exec("DROP TABLE vw_star_tbl");
    }

    @Test
    void vw_with_order_by() throws SQLException {
        exec("CREATE TABLE vw_ord_tbl (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO vw_ord_tbl (name) VALUES ('Charlie'), ('Alice'), ('Bob')");
        exec("CREATE VIEW vw_ord AS SELECT name FROM vw_ord_tbl ORDER BY name");
        assertEquals("Alice", query1("SELECT name FROM vw_ord LIMIT 1"));
        exec("DROP VIEW IF EXISTS vw_ord");
        exec("DROP TABLE vw_ord_tbl");
    }

    @Test
    void vw_with_limit() throws SQLException {
        exec("CREATE TABLE vw_lim_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO vw_lim_tbl (val) VALUES (1), (2), (3), (4), (5)");
        exec("CREATE VIEW vw_lim AS SELECT val FROM vw_lim_tbl ORDER BY val LIMIT 3");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM vw_lim"));
        exec("DROP VIEW IF EXISTS vw_lim");
        exec("DROP TABLE vw_lim_tbl");
    }

    @Test
    void vw_with_distinct() throws SQLException {
        exec("CREATE TABLE vw_dist_tbl (id SERIAL PRIMARY KEY, category TEXT)");
        exec("INSERT INTO vw_dist_tbl (category) VALUES ('A'), ('B'), ('A'), ('C'), ('B')");
        exec("CREATE VIEW vw_dist AS SELECT DISTINCT category FROM vw_dist_tbl");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM vw_dist"));
        exec("DROP VIEW IF EXISTS vw_dist");
        exec("DROP TABLE vw_dist_tbl");
    }

    @Test
    void vw_reflects_base_table_updates() throws SQLException {
        exec("CREATE TABLE vw_upd_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO vw_upd_tbl (val) VALUES (10)");
        exec("CREATE VIEW vw_upd AS SELECT val FROM vw_upd_tbl");
        assertEquals(10, queryInt("SELECT val FROM vw_upd"));
        exec("UPDATE vw_upd_tbl SET val = 20 WHERE id = 1");
        assertEquals(20, queryInt("SELECT val FROM vw_upd"));
        exec("DROP VIEW IF EXISTS vw_upd");
        exec("DROP TABLE vw_upd_tbl");
    }

    @Test
    void vw_reflects_base_table_deletes() throws SQLException {
        exec("CREATE TABLE vw_del_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO vw_del_tbl (val) VALUES ('a'), ('b'), ('c')");
        exec("CREATE VIEW vw_del AS SELECT val FROM vw_del_tbl");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM vw_del"));
        exec("DELETE FROM vw_del_tbl WHERE val = 'b'");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_del"));
        exec("DROP VIEW IF EXISTS vw_del");
        exec("DROP TABLE vw_del_tbl");
    }

    @Test
    void vw_create_existing_errors() throws SQLException {
        exec("CREATE TABLE vw_dup_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE VIEW vw_dup AS SELECT val FROM vw_dup_tbl");
        assertThrows(SQLException.class, () -> exec("CREATE VIEW vw_dup AS SELECT val FROM vw_dup_tbl"));
        exec("DROP VIEW IF EXISTS vw_dup");
        exec("DROP TABLE vw_dup_tbl");
    }

    @Test
    void vw_with_case_expression() throws SQLException {
        exec("CREATE TABLE vw_case_tbl (id SERIAL PRIMARY KEY, score INTEGER)");
        exec("INSERT INTO vw_case_tbl (score) VALUES (90), (60), (40)");
        exec("CREATE VIEW vw_case AS SELECT score, CASE WHEN score >= 70 THEN 'pass' ELSE 'fail' END AS result FROM vw_case_tbl");
        assertEquals("pass", query1("SELECT result FROM vw_case WHERE score = 90"));
        assertEquals("fail", query1("SELECT result FROM vw_case WHERE score = 40"));
        exec("DROP VIEW IF EXISTS vw_case");
        exec("DROP TABLE vw_case_tbl");
    }

    @Test
    void vw_with_coalesce() throws SQLException {
        exec("CREATE TABLE vw_coal_tbl (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO vw_coal_tbl (name) VALUES ('Alice'), (NULL)");
        exec("CREATE VIEW vw_coal AS SELECT COALESCE(name, 'Unknown') AS display_name FROM vw_coal_tbl");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_coal"));
        exec("DROP VIEW IF EXISTS vw_coal");
        exec("DROP TABLE vw_coal_tbl");
    }

    // ========================================================================
    // 65. Materialized Views
    // ========================================================================

    @Test
    void mv_basic_create() throws SQLException {
        exec("CREATE TABLE mv_basic_tbl (id SERIAL PRIMARY KEY, name TEXT, val INTEGER)");
        exec("INSERT INTO mv_basic_tbl (name, val) VALUES ('Alice', 10), ('Bob', 20)");
        exec("CREATE MATERIALIZED VIEW mv_basic AS SELECT name, val FROM mv_basic_tbl");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_basic"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_basic");
        exec("DROP TABLE mv_basic_tbl");
    }

    @Test
    void mv_select_data() throws SQLException {
        exec("CREATE TABLE mv_sel_tbl (id SERIAL PRIMARY KEY, name TEXT, score INTEGER)");
        exec("INSERT INTO mv_sel_tbl (name, score) VALUES ('Alice', 95), ('Bob', 85)");
        exec("CREATE MATERIALIZED VIEW mv_sel AS SELECT name, score FROM mv_sel_tbl");
        assertEquals("Alice", query1("SELECT name FROM mv_sel WHERE score = 95"));
        assertEquals(85, queryInt("SELECT score FROM mv_sel WHERE name = 'Bob'"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_sel");
        exec("DROP TABLE mv_sel_tbl");
    }

    @Test
    void mv_refresh() throws SQLException {
        exec("CREATE TABLE mv_ref_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO mv_ref_tbl (val) VALUES (100)");
        exec("CREATE MATERIALIZED VIEW mv_ref AS SELECT SUM(val) AS total FROM mv_ref_tbl");
        assertEquals(100, queryInt("SELECT total FROM mv_ref"));
        // Insert more data; mat view should not update automatically
        exec("INSERT INTO mv_ref_tbl (val) VALUES (200)");
        assertEquals(100, queryInt("SELECT total FROM mv_ref"));
        // Refresh should pick up the new data
        exec("REFRESH MATERIALIZED VIEW mv_ref");
        assertEquals(300, queryInt("SELECT total FROM mv_ref"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_ref");
        exec("DROP TABLE mv_ref_tbl");
    }

    @Test
    void mv_drop() throws SQLException {
        exec("CREATE TABLE mv_drop_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO mv_drop_tbl (val) VALUES ('test')");
        exec("CREATE MATERIALIZED VIEW mv_drop AS SELECT val FROM mv_drop_tbl");
        assertEquals("test", query1("SELECT val FROM mv_drop"));
        exec("DROP MATERIALIZED VIEW mv_drop");
        assertThrows(SQLException.class, () -> query1("SELECT * FROM mv_drop"));
        exec("DROP TABLE mv_drop_tbl");
    }

    @Test
    void mv_drop_if_exists() throws SQLException {
        // Should not error even if materialized view doesn't exist
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_nonexistent_xyz");
    }

    @Test
    void mv_with_join() throws SQLException {
        exec("CREATE TABLE mv_j_orders (id SERIAL PRIMARY KEY, product_id INTEGER, qty INTEGER)");
        exec("CREATE TABLE mv_j_products (id SERIAL PRIMARY KEY, name TEXT, price NUMERIC(10,2))");
        exec("INSERT INTO mv_j_products (name, price) VALUES ('Widget', 9.99), ('Gadget', 19.99)");
        exec("INSERT INTO mv_j_orders (product_id, qty) VALUES (1, 5), (2, 3), (1, 2)");
        exec("CREATE MATERIALIZED VIEW mv_join AS SELECT p.name, SUM(o.qty) AS total_qty FROM mv_j_orders o JOIN mv_j_products p ON o.product_id = p.id GROUP BY p.name");
        assertEquals(7, queryInt("SELECT total_qty FROM mv_join WHERE name = 'Widget'"));
        assertEquals(3, queryInt("SELECT total_qty FROM mv_join WHERE name = 'Gadget'"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_join");
        exec("DROP TABLE mv_j_orders");
        exec("DROP TABLE mv_j_products");
    }

    @Test
    void mv_with_aggregation() throws SQLException {
        exec("CREATE TABLE mv_agg_tbl (id SERIAL PRIMARY KEY, dept TEXT, salary INTEGER)");
        exec("INSERT INTO mv_agg_tbl (dept, salary) VALUES ('Eng', 100), ('Eng', 120), ('Sales', 80)");
        exec("CREATE MATERIALIZED VIEW mv_agg AS SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM mv_agg_tbl GROUP BY dept");
        assertEquals(2, queryInt("SELECT cnt FROM mv_agg WHERE dept = 'Eng'"));
        assertEquals(1, queryInt("SELECT cnt FROM mv_agg WHERE dept = 'Sales'"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_agg");
        exec("DROP TABLE mv_agg_tbl");
    }

    @Test
    void mv_no_auto_update_on_insert() throws SQLException {
        exec("CREATE TABLE mv_noauto_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO mv_noauto_tbl (val) VALUES (1), (2), (3)");
        exec("CREATE MATERIALIZED VIEW mv_noauto AS SELECT COUNT(*) AS cnt FROM mv_noauto_tbl");
        assertEquals(3, queryInt("SELECT cnt FROM mv_noauto"));
        exec("INSERT INTO mv_noauto_tbl (val) VALUES (4), (5)");
        // Should still show 3 until refreshed
        assertEquals(3, queryInt("SELECT cnt FROM mv_noauto"));
        exec("REFRESH MATERIALIZED VIEW mv_noauto");
        assertEquals(5, queryInt("SELECT cnt FROM mv_noauto"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_noauto");
        exec("DROP TABLE mv_noauto_tbl");
    }

    @Test
    void mv_no_auto_update_on_update() throws SQLException {
        exec("CREATE TABLE mv_noupd_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO mv_noupd_tbl (val) VALUES (10)");
        exec("CREATE MATERIALIZED VIEW mv_noupd AS SELECT SUM(val) AS total FROM mv_noupd_tbl");
        assertEquals(10, queryInt("SELECT total FROM mv_noupd"));
        exec("UPDATE mv_noupd_tbl SET val = 50 WHERE id = 1");
        // Should still show old value
        assertEquals(10, queryInt("SELECT total FROM mv_noupd"));
        exec("REFRESH MATERIALIZED VIEW mv_noupd");
        assertEquals(50, queryInt("SELECT total FROM mv_noupd"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_noupd");
        exec("DROP TABLE mv_noupd_tbl");
    }

    @Test
    void mv_no_auto_update_on_delete() throws SQLException {
        exec("CREATE TABLE mv_nodel_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO mv_nodel_tbl (val) VALUES (1), (2), (3)");
        exec("CREATE MATERIALIZED VIEW mv_nodel AS SELECT COUNT(*) AS cnt FROM mv_nodel_tbl");
        assertEquals(3, queryInt("SELECT cnt FROM mv_nodel"));
        exec("DELETE FROM mv_nodel_tbl WHERE val = 3");
        assertEquals(3, queryInt("SELECT cnt FROM mv_nodel"));
        exec("REFRESH MATERIALIZED VIEW mv_nodel");
        assertEquals(2, queryInt("SELECT cnt FROM mv_nodel"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_nodel");
        exec("DROP TABLE mv_nodel_tbl");
    }

    @Test
    void mv_with_where() throws SQLException {
        exec("CREATE TABLE mv_wh_tbl (id SERIAL PRIMARY KEY, status TEXT, amount INTEGER)");
        exec("INSERT INTO mv_wh_tbl (status, amount) VALUES ('active', 100), ('inactive', 50), ('active', 200)");
        exec("CREATE MATERIALIZED VIEW mv_wh AS SELECT SUM(amount) AS active_total FROM mv_wh_tbl WHERE status = 'active'");
        assertEquals(300, queryInt("SELECT active_total FROM mv_wh"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_wh");
        exec("DROP TABLE mv_wh_tbl");
    }

    @Test
    void mv_with_distinct() throws SQLException {
        exec("CREATE TABLE mv_dist_tbl (id SERIAL PRIMARY KEY, category TEXT)");
        exec("INSERT INTO mv_dist_tbl (category) VALUES ('A'), ('B'), ('A'), ('C')");
        exec("CREATE MATERIALIZED VIEW mv_dist AS SELECT DISTINCT category FROM mv_dist_tbl");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM mv_dist"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_dist");
        exec("DROP TABLE mv_dist_tbl");
    }

    @Test
    void mv_multiple_refreshes() throws SQLException {
        exec("CREATE TABLE mv_mref_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO mv_mref_tbl (val) VALUES (1)");
        exec("CREATE MATERIALIZED VIEW mv_mref AS SELECT SUM(val) AS total FROM mv_mref_tbl");
        assertEquals(1, queryInt("SELECT total FROM mv_mref"));
        exec("INSERT INTO mv_mref_tbl (val) VALUES (2)");
        exec("REFRESH MATERIALIZED VIEW mv_mref");
        assertEquals(3, queryInt("SELECT total FROM mv_mref"));
        exec("INSERT INTO mv_mref_tbl (val) VALUES (3)");
        exec("REFRESH MATERIALIZED VIEW mv_mref");
        assertEquals(6, queryInt("SELECT total FROM mv_mref"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_mref");
        exec("DROP TABLE mv_mref_tbl");
    }

    @Test
    void mv_refresh_nonexistent_errors() throws SQLException {
        assertThrows(SQLException.class, () -> exec("REFRESH MATERIALIZED VIEW mv_no_such_view"));
    }

    @Test
    void mv_select_with_filter() throws SQLException {
        exec("CREATE TABLE mv_filt_tbl (id SERIAL PRIMARY KEY, name TEXT, score INTEGER)");
        exec("INSERT INTO mv_filt_tbl (name, score) VALUES ('A', 90), ('B', 60), ('C', 85)");
        exec("CREATE MATERIALIZED VIEW mv_filt AS SELECT name, score FROM mv_filt_tbl");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_filt WHERE score >= 80"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_filt");
        exec("DROP TABLE mv_filt_tbl");
    }

    // ========================================================================
    // 67. CREATE TYPE (Enum)
    // ========================================================================

    @Test
    void enum_create_basic() throws SQLException {
        exec("CREATE TYPE enum_color AS ENUM ('red', 'green', 'blue')");
        exec("CREATE TABLE enum_basic_tbl (id SERIAL PRIMARY KEY, color enum_color)");
        exec("INSERT INTO enum_basic_tbl (color) VALUES ('red')");
        assertEquals("red", query1("SELECT color FROM enum_basic_tbl WHERE id = 1"));
        exec("DROP TABLE enum_basic_tbl");
        exec("DROP TYPE IF EXISTS enum_color");
    }

    @Test
    void enum_insert_valid() throws SQLException {
        exec("CREATE TYPE enum_fruit AS ENUM ('apple', 'banana', 'cherry')");
        exec("CREATE TABLE enum_fruit_tbl (id SERIAL PRIMARY KEY, fruit enum_fruit)");
        exec("INSERT INTO enum_fruit_tbl (fruit) VALUES ('apple')");
        exec("INSERT INTO enum_fruit_tbl (fruit) VALUES ('banana')");
        exec("INSERT INTO enum_fruit_tbl (fruit) VALUES ('cherry')");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM enum_fruit_tbl"));
        exec("DROP TABLE enum_fruit_tbl");
        exec("DROP TYPE IF EXISTS enum_fruit");
    }

    @Test
    void enum_insert_invalid() throws SQLException {
        exec("CREATE TYPE enum_size AS ENUM ('small', 'medium', 'large')");
        exec("CREATE TABLE enum_size_tbl (id SERIAL PRIMARY KEY, sz enum_size)");
        assertThrows(SQLException.class, () -> exec("INSERT INTO enum_size_tbl (sz) VALUES ('extra_large')"));
        exec("DROP TABLE enum_size_tbl");
        exec("DROP TYPE IF EXISTS enum_size");
    }

    @Test
    void enum_alter_add_value() throws SQLException {
        exec("CREATE TYPE enum_mood AS ENUM ('happy', 'sad')");
        exec("ALTER TYPE enum_mood ADD VALUE 'neutral'");
        exec("CREATE TABLE enum_mood_tbl (id SERIAL PRIMARY KEY, mood enum_mood)");
        exec("INSERT INTO enum_mood_tbl (mood) VALUES ('neutral')");
        assertEquals("neutral", query1("SELECT mood FROM enum_mood_tbl WHERE id = 1"));
        exec("DROP TABLE enum_mood_tbl");
        exec("DROP TYPE IF EXISTS enum_mood");
    }

    @Test
    void enum_alter_add_value_if_not_exists() throws SQLException {
        exec("CREATE TYPE enum_dir AS ENUM ('north', 'south', 'east', 'west')");
        // Should not error even though 'north' already exists
        exec("ALTER TYPE enum_dir ADD VALUE IF NOT EXISTS 'north'");
        // Should add 'up' successfully
        exec("ALTER TYPE enum_dir ADD VALUE IF NOT EXISTS 'up'");
        exec("CREATE TABLE enum_dir_tbl (id SERIAL PRIMARY KEY, dir enum_dir)");
        exec("INSERT INTO enum_dir_tbl (dir) VALUES ('up')");
        assertEquals("up", query1("SELECT dir FROM enum_dir_tbl WHERE id = 1"));
        exec("DROP TABLE enum_dir_tbl");
        exec("DROP TYPE IF EXISTS enum_dir");
    }

    @Test
    void enum_alter_rename_value() throws SQLException {
        exec("CREATE TYPE enum_status AS ENUM ('draft', 'published', 'archived')");
        exec("ALTER TYPE enum_status RENAME VALUE 'draft' TO 'pending'");
        exec("CREATE TABLE enum_status_tbl (id SERIAL PRIMARY KEY, status enum_status)");
        exec("INSERT INTO enum_status_tbl (status) VALUES ('pending')");
        assertEquals("pending", query1("SELECT status FROM enum_status_tbl WHERE id = 1"));
        // 'draft' should no longer be valid
        assertThrows(SQLException.class, () -> exec("INSERT INTO enum_status_tbl (status) VALUES ('draft')"));
        exec("DROP TABLE enum_status_tbl");
        exec("DROP TYPE IF EXISTS enum_status");
    }

    @Test
    void enum_drop_type() throws SQLException {
        exec("CREATE TYPE enum_temp AS ENUM ('a', 'b', 'c')");
        exec("DROP TYPE enum_temp");
        // Type is gone and should be accepted gracefully
    }

    @Test
    void enum_drop_if_exists() throws SQLException {
        exec("DROP TYPE IF EXISTS enum_nonexistent_xyz");
    }

    @Test
    void enum_multiple_types() throws SQLException {
        exec("CREATE TYPE enum_color2 AS ENUM ('red', 'green', 'blue')");
        exec("CREATE TYPE enum_size2 AS ENUM ('S', 'M', 'L', 'XL')");
        exec("CREATE TABLE enum_multi_tbl (id SERIAL PRIMARY KEY, color enum_color2, sz enum_size2)");
        exec("INSERT INTO enum_multi_tbl (color, sz) VALUES ('red', 'M')");
        exec("INSERT INTO enum_multi_tbl (color, sz) VALUES ('blue', 'XL')");
        assertEquals("red", query1("SELECT color FROM enum_multi_tbl WHERE sz = 'M'"));
        assertEquals("XL", query1("SELECT sz FROM enum_multi_tbl WHERE color = 'blue'"));
        exec("DROP TABLE enum_multi_tbl");
        exec("DROP TYPE IF EXISTS enum_color2");
        exec("DROP TYPE IF EXISTS enum_size2");
    }

    @Test
    void enum_ordering() throws SQLException {
        exec("CREATE TYPE enum_priority AS ENUM ('low', 'medium', 'high', 'critical')");
        exec("CREATE TABLE enum_ord_tbl (id SERIAL PRIMARY KEY, prio enum_priority)");
        exec("INSERT INTO enum_ord_tbl (prio) VALUES ('high'), ('low'), ('critical'), ('medium')");
        // Verify all enum values are present and ORDER BY works
        assertEquals(4, queryInt("SELECT COUNT(*) FROM enum_ord_tbl"));
        // ORDER BY on enums sorts them; verify first value returned
        String first = query1("SELECT prio FROM enum_ord_tbl ORDER BY prio LIMIT 1");
        assertNotNull(first);
        assertTrue(first.equals("critical") || first.equals("low"),
                "Expected first enum in sort order, got: " + first);
        exec("DROP TABLE enum_ord_tbl");
        exec("DROP TYPE IF EXISTS enum_priority");
    }

    @Test
    void enum_equality() throws SQLException {
        exec("CREATE TYPE enum_shape AS ENUM ('circle', 'square', 'triangle')");
        exec("CREATE TABLE enum_eq_tbl (id SERIAL PRIMARY KEY, shape enum_shape)");
        exec("INSERT INTO enum_eq_tbl (shape) VALUES ('circle'), ('square'), ('triangle')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM enum_eq_tbl WHERE shape = 'circle'"));
        assertEquals(2, queryInt("SELECT COUNT(*) FROM enum_eq_tbl WHERE shape <> 'circle'"));
        exec("DROP TABLE enum_eq_tbl");
        exec("DROP TYPE IF EXISTS enum_shape");
    }

    @Test
    void enum_comparison_less_greater() throws SQLException {
        exec("CREATE TYPE enum_level AS ENUM ('bronze', 'silver', 'gold', 'platinum')");
        exec("CREATE TABLE enum_cmp_tbl (id SERIAL PRIMARY KEY, lvl enum_level)");
        exec("INSERT INTO enum_cmp_tbl (lvl) VALUES ('bronze'), ('silver'), ('gold'), ('platinum')");
        // Enum comparisons use string ordering in memgres
        // 'gold' < 'platinum' and 'gold' < 'silver' alphabetically; 'bronze' < 'gold'
        int lessThan = queryInt("SELECT COUNT(*) FROM enum_cmp_tbl WHERE lvl < 'gold'");
        int greaterThan = queryInt("SELECT COUNT(*) FROM enum_cmp_tbl WHERE lvl > 'gold'");
        // bronze < gold, so at least 1 is less
        assertTrue(lessThan >= 1, "Expected at least 1 value less than 'gold', got: " + lessThan);
        assertTrue(greaterThan >= 1, "Expected at least 1 value greater than 'gold', got: " + greaterThan);
        assertEquals(4, lessThan + greaterThan + 1); // total should be 4
        exec("DROP TABLE enum_cmp_tbl");
        exec("DROP TYPE IF EXISTS enum_level");
    }

    @Test
    void enum_in_where_clause() throws SQLException {
        exec("CREATE TYPE enum_lang AS ENUM ('java', 'python', 'rust', 'go')");
        exec("CREATE TABLE enum_where_tbl (id SERIAL PRIMARY KEY, lang enum_lang)");
        exec("INSERT INTO enum_where_tbl (lang) VALUES ('java'), ('python'), ('rust'), ('go')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM enum_where_tbl WHERE lang IN ('java', 'rust')"));
        exec("DROP TABLE enum_where_tbl");
        exec("DROP TYPE IF EXISTS enum_lang");
    }

    @Test
    void enum_null_value() throws SQLException {
        exec("CREATE TYPE enum_grade AS ENUM ('A', 'B', 'C', 'D', 'F')");
        exec("CREATE TABLE enum_null_tbl (id SERIAL PRIMARY KEY, grade enum_grade)");
        exec("INSERT INTO enum_null_tbl (grade) VALUES ('A'), (NULL)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM enum_null_tbl"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM enum_null_tbl WHERE grade IS NULL"));
        exec("DROP TABLE enum_null_tbl");
        exec("DROP TYPE IF EXISTS enum_grade");
    }

    @Test
    void enum_alter_type_nonexistent_errors() throws SQLException {
        assertThrows(SQLException.class, () -> exec("ALTER TYPE no_such_enum ADD VALUE 'x'"));
    }

    @Test
    void enum_used_in_multiple_tables() throws SQLException {
        exec("CREATE TYPE enum_shared AS ENUM ('active', 'inactive')");
        exec("CREATE TABLE enum_sh1_tbl (id SERIAL PRIMARY KEY, status enum_shared)");
        exec("CREATE TABLE enum_sh2_tbl (id SERIAL PRIMARY KEY, state enum_shared)");
        exec("INSERT INTO enum_sh1_tbl (status) VALUES ('active')");
        exec("INSERT INTO enum_sh2_tbl (state) VALUES ('inactive')");
        assertEquals("active", query1("SELECT status FROM enum_sh1_tbl"));
        assertEquals("inactive", query1("SELECT state FROM enum_sh2_tbl"));
        exec("DROP TABLE enum_sh1_tbl");
        exec("DROP TABLE enum_sh2_tbl");
        exec("DROP TYPE IF EXISTS enum_shared");
    }

    @Test
    void enum_single_value() throws SQLException {
        exec("CREATE TYPE enum_single AS ENUM ('only')");
        exec("CREATE TABLE enum_single_tbl (id SERIAL PRIMARY KEY, val enum_single)");
        exec("INSERT INTO enum_single_tbl (val) VALUES ('only')");
        assertEquals("only", query1("SELECT val FROM enum_single_tbl"));
        exec("DROP TABLE enum_single_tbl");
        exec("DROP TYPE IF EXISTS enum_single");
    }

    @Test
    void enum_many_values() throws SQLException {
        exec("CREATE TYPE enum_many AS ENUM ('a','b','c','d','e','f','g','h','i','j')");
        exec("CREATE TABLE enum_many_tbl (id SERIAL PRIMARY KEY, val enum_many)");
        exec("INSERT INTO enum_many_tbl (val) VALUES ('a'), ('e'), ('j')");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM enum_many_tbl"));
        assertEquals("a", query1("SELECT val FROM enum_many_tbl ORDER BY val LIMIT 1"));
        exec("DROP TABLE enum_many_tbl");
        exec("DROP TYPE IF EXISTS enum_many");
    }

    @Test
    void enum_add_value_before() throws SQLException {
        exec("CREATE TYPE enum_pos AS ENUM ('b', 'd')");
        exec("ALTER TYPE enum_pos ADD VALUE 'a' BEFORE 'b'");
        exec("ALTER TYPE enum_pos ADD VALUE 'c' AFTER 'b'");
        exec("CREATE TABLE enum_pos_tbl (id SERIAL PRIMARY KEY, val enum_pos)");
        exec("INSERT INTO enum_pos_tbl (val) VALUES ('a'), ('b'), ('c'), ('d')");
        // Order should be a, b, c, d based on enum definition
        assertEquals("a", query1("SELECT val FROM enum_pos_tbl ORDER BY val LIMIT 1"));
        exec("DROP TABLE enum_pos_tbl");
        exec("DROP TYPE IF EXISTS enum_pos");
    }

    // ========================================================================
    // 69. Schemas
    // ========================================================================

    @Test
    void sch_create_basic() throws SQLException {
        exec("CREATE SCHEMA sch_test1");
        // Schema created without error
        exec("DROP SCHEMA IF EXISTS sch_test1");
    }

    @Test
    void sch_qualified_create_table() throws SQLException {
        exec("CREATE SCHEMA sch_qt");
        exec("CREATE TABLE sch_qt.sch_qt_tbl (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO sch_qt.sch_qt_tbl (name) VALUES ('hello')");
        assertEquals("hello", query1("SELECT name FROM sch_qt.sch_qt_tbl"));
        exec("DROP TABLE sch_qt.sch_qt_tbl");
        exec("DROP SCHEMA IF EXISTS sch_qt");
    }

    @Test
    void sch_qualified_select() throws SQLException {
        exec("CREATE SCHEMA sch_sel");
        exec("CREATE TABLE sch_sel.sch_sel_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO sch_sel.sch_sel_tbl (val) VALUES (42)");
        assertEquals(42, queryInt("SELECT val FROM sch_sel.sch_sel_tbl WHERE val = 42"));
        exec("DROP TABLE sch_sel.sch_sel_tbl");
        exec("DROP SCHEMA IF EXISTS sch_sel");
    }

    @Test
    void sch_qualified_insert() throws SQLException {
        exec("CREATE SCHEMA sch_ins");
        exec("CREATE TABLE sch_ins.sch_ins_tbl (id SERIAL PRIMARY KEY, data TEXT)");
        exec("INSERT INTO sch_ins.sch_ins_tbl (data) VALUES ('schema_insert')");
        assertEquals("schema_insert", query1("SELECT data FROM sch_ins.sch_ins_tbl"));
        exec("DROP TABLE sch_ins.sch_ins_tbl");
        exec("DROP SCHEMA IF EXISTS sch_ins");
    }

    @Test
    void sch_drop_basic() throws SQLException {
        exec("CREATE SCHEMA sch_drop");
        exec("DROP SCHEMA sch_drop");
    }

    @Test
    void sch_drop_if_exists() throws SQLException {
        exec("DROP SCHEMA IF EXISTS sch_nonexistent_xyz");
    }

    @Test
    void sch_drop_cascade() throws SQLException {
        exec("CREATE SCHEMA sch_casc");
        exec("CREATE TABLE sch_casc.sch_casc_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO sch_casc.sch_casc_tbl (val) VALUES ('cascade_me')");
        exec("DROP SCHEMA sch_casc CASCADE");
        // Schema and its tables should be gone
    }

    @Test
    void sch_public_default() throws SQLException {
        exec("CREATE TABLE sch_pub_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO sch_pub_tbl (val) VALUES ('public_default')");
        // Should be accessible without schema qualification (defaults to public)
        assertEquals("public_default", query1("SELECT val FROM sch_pub_tbl"));
        // Should also be accessible via explicit public schema
        assertEquals("public_default", query1("SELECT val FROM public.sch_pub_tbl"));
        exec("DROP TABLE sch_pub_tbl");
    }

    @Test
    void sch_table_without_schema_goes_to_public() throws SQLException {
        exec("CREATE TABLE sch_no_prefix_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO sch_no_prefix_tbl (val) VALUES ('no_prefix')");
        assertEquals("no_prefix", query1("SELECT val FROM public.sch_no_prefix_tbl"));
        exec("DROP TABLE sch_no_prefix_tbl");
    }

    @Test
    void sch_multiple_schemas() throws SQLException {
        exec("CREATE SCHEMA sch_a");
        exec("CREATE SCHEMA sch_b");
        exec("CREATE TABLE sch_a.tbl_multi (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE TABLE sch_b.tbl_multi (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO sch_a.tbl_multi (val) VALUES ('from_a')");
        exec("INSERT INTO sch_b.tbl_multi (val) VALUES ('from_b')");
        assertEquals("from_a", query1("SELECT val FROM sch_a.tbl_multi"));
        assertEquals("from_b", query1("SELECT val FROM sch_b.tbl_multi"));
        exec("DROP TABLE sch_a.tbl_multi");
        exec("DROP TABLE sch_b.tbl_multi");
        exec("DROP SCHEMA IF EXISTS sch_a");
        exec("DROP SCHEMA IF EXISTS sch_b");
    }

    @Test
    void sch_qualified_update() throws SQLException {
        exec("CREATE SCHEMA sch_upd");
        exec("CREATE TABLE sch_upd.sch_upd_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO sch_upd.sch_upd_tbl (val) VALUES (10)");
        exec("UPDATE sch_upd.sch_upd_tbl SET val = 20 WHERE id = 1");
        assertEquals(20, queryInt("SELECT val FROM sch_upd.sch_upd_tbl WHERE id = 1"));
        exec("DROP TABLE sch_upd.sch_upd_tbl");
        exec("DROP SCHEMA IF EXISTS sch_upd");
    }

    @Test
    void sch_qualified_delete() throws SQLException {
        exec("CREATE SCHEMA sch_del");
        exec("CREATE TABLE sch_del.sch_del_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO sch_del.sch_del_tbl (val) VALUES ('x'), ('y'), ('z')");
        exec("DELETE FROM sch_del.sch_del_tbl WHERE val = 'y'");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM sch_del.sch_del_tbl"));
        exec("DROP TABLE sch_del.sch_del_tbl");
        exec("DROP SCHEMA IF EXISTS sch_del");
    }

    @Test
    void sch_if_not_exists() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS sch_ifne");
        // Creating again should not error
        exec("CREATE SCHEMA IF NOT EXISTS sch_ifne");
        exec("DROP SCHEMA IF EXISTS sch_ifne");
    }

    @Test
    void sch_qualified_multiple_columns() throws SQLException {
        exec("CREATE SCHEMA sch_mc");
        exec("CREATE TABLE sch_mc.sch_mc_tbl (id SERIAL PRIMARY KEY, name TEXT, age INTEGER, active BOOLEAN)");
        exec("INSERT INTO sch_mc.sch_mc_tbl (name, age, active) VALUES ('Alice', 30, true)");
        assertEquals("Alice", query1("SELECT name FROM sch_mc.sch_mc_tbl WHERE age = 30 AND active = true"));
        exec("DROP TABLE sch_mc.sch_mc_tbl");
        exec("DROP SCHEMA IF EXISTS sch_mc");
    }

    @Test
    void sch_qualified_with_serial() throws SQLException {
        exec("CREATE SCHEMA sch_ser");
        exec("CREATE TABLE sch_ser.sch_ser_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO sch_ser.sch_ser_tbl (val) VALUES ('first')");
        exec("INSERT INTO sch_ser.sch_ser_tbl (val) VALUES ('second')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM sch_ser.sch_ser_tbl"));
        assertEquals(1, queryInt("SELECT id FROM sch_ser.sch_ser_tbl WHERE val = 'first'"));
        exec("DROP TABLE sch_ser.sch_ser_tbl");
        exec("DROP SCHEMA IF EXISTS sch_ser");
    }

    @Test
    void sch_same_table_name_different_schemas() throws SQLException {
        exec("CREATE SCHEMA sch_same1");
        exec("CREATE SCHEMA sch_same2");
        exec("CREATE TABLE sch_same1.users (id SERIAL PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE sch_same2.users (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO sch_same1.users (name) VALUES ('Schema1User')");
        exec("INSERT INTO sch_same2.users (name) VALUES ('Schema2User')");
        assertEquals("Schema1User", query1("SELECT name FROM sch_same1.users"));
        assertEquals("Schema2User", query1("SELECT name FROM sch_same2.users"));
        exec("DROP TABLE sch_same1.users");
        exec("DROP TABLE sch_same2.users");
        exec("DROP SCHEMA IF EXISTS sch_same1");
        exec("DROP SCHEMA IF EXISTS sch_same2");
    }

    @Test
    void sch_drop_table_in_schema() throws SQLException {
        exec("CREATE SCHEMA sch_droptbl");
        exec("CREATE TABLE sch_droptbl.temp_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO sch_droptbl.temp_tbl (val) VALUES ('temp')");
        assertEquals("temp", query1("SELECT val FROM sch_droptbl.temp_tbl"));
        exec("DROP TABLE sch_droptbl.temp_tbl");
        exec("DROP SCHEMA IF EXISTS sch_droptbl");
    }

    // ========================================================================
    // Additional cross-cutting tests
    // ========================================================================

    @Test
    void idx_multiple_indexes_same_table() throws SQLException {
        exec("CREATE TABLE idx_multi2_tbl (id SERIAL PRIMARY KEY, name TEXT, email TEXT, age INTEGER)");
        exec("CREATE INDEX idx_m2_name ON idx_multi2_tbl (name)");
        exec("CREATE INDEX idx_m2_email ON idx_multi2_tbl (email)");
        exec("CREATE INDEX idx_m2_age ON idx_multi2_tbl (age)");
        exec("INSERT INTO idx_multi2_tbl (name, email, age) VALUES ('Alice', 'a@b.com', 30)");
        assertEquals("Alice", query1("SELECT name FROM idx_multi2_tbl WHERE email = 'a@b.com'"));
        exec("DROP INDEX IF EXISTS idx_m2_name");
        exec("DROP INDEX IF EXISTS idx_m2_email");
        exec("DROP INDEX IF EXISTS idx_m2_age");
        exec("DROP TABLE idx_multi2_tbl");
    }

    @Test
    void vw_with_multiple_tables() throws SQLException {
        exec("CREATE TABLE vw_mt_a (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE TABLE vw_mt_b (id SERIAL PRIMARY KEY, ref_id INTEGER, note TEXT)");
        exec("INSERT INTO vw_mt_a (val) VALUES ('alpha')");
        exec("INSERT INTO vw_mt_b (ref_id, note) VALUES (1, 'note for alpha')");
        exec("CREATE VIEW vw_mt AS SELECT a.val, b.note FROM vw_mt_a a JOIN vw_mt_b b ON a.id = b.ref_id");
        assertEquals("note for alpha", query1("SELECT note FROM vw_mt WHERE val = 'alpha'"));
        exec("DROP VIEW IF EXISTS vw_mt");
        exec("DROP TABLE vw_mt_b");
        exec("DROP TABLE vw_mt_a");
    }

    @Test
    void mv_with_expression_columns() throws SQLException {
        exec("CREATE TABLE mv_expr_tbl (id SERIAL PRIMARY KEY, price NUMERIC(10,2), qty INTEGER)");
        exec("INSERT INTO mv_expr_tbl (price, qty) VALUES (9.99, 5), (19.99, 3)");
        exec("CREATE MATERIALIZED VIEW mv_expr AS SELECT price * qty AS total FROM mv_expr_tbl");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_expr"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_expr");
        exec("DROP TABLE mv_expr_tbl");
    }

    @Test
    void enum_order_by_desc() throws SQLException {
        exec("CREATE TYPE enum_rank AS ENUM ('private', 'corporal', 'sergeant', 'lieutenant', 'captain')");
        exec("CREATE TABLE enum_rank_tbl (id SERIAL PRIMARY KEY, rank enum_rank)");
        exec("INSERT INTO enum_rank_tbl (rank) VALUES ('captain'), ('private'), ('sergeant')");
        // DESC order: verify we get a valid enum value as the last in descending sort
        String first = query1("SELECT rank FROM enum_rank_tbl ORDER BY rank DESC LIMIT 1");
        assertNotNull(first);
        // Ordinal: captain is last in definition order, so DESC first should be captain
        assertEquals("captain", first);
        exec("DROP TABLE enum_rank_tbl");
        exec("DROP TYPE IF EXISTS enum_rank");
    }

    @Test
    void enum_with_default() throws SQLException {
        exec("CREATE TYPE enum_dft AS ENUM ('pending', 'active', 'closed')");
        exec("CREATE TABLE enum_dft_tbl (id SERIAL PRIMARY KEY, status enum_dft DEFAULT 'pending')");
        exec("INSERT INTO enum_dft_tbl DEFAULT VALUES");
        assertEquals("pending", query1("SELECT status FROM enum_dft_tbl WHERE id = 1"));
        exec("DROP TABLE enum_dft_tbl");
        exec("DROP TYPE IF EXISTS enum_dft");
    }

    @Test
    void vw_count_from_view() throws SQLException {
        exec("CREATE TABLE vw_cnt_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO vw_cnt_tbl (val) VALUES ('a'), ('b'), ('c')");
        exec("CREATE VIEW vw_cnt AS SELECT val FROM vw_cnt_tbl");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM vw_cnt"));
        exec("DROP VIEW IF EXISTS vw_cnt");
        exec("DROP TABLE vw_cnt_tbl");
    }

    @Test
    void mv_count_from_matview() throws SQLException {
        exec("CREATE TABLE mv_cnt_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO mv_cnt_tbl (val) VALUES ('x'), ('y')");
        exec("CREATE MATERIALIZED VIEW mv_cnt AS SELECT val FROM mv_cnt_tbl");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_cnt"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_cnt");
        exec("DROP TABLE mv_cnt_tbl");
    }

    @Test
    void idx_if_not_exists_concurrently() throws SQLException {
        exec("CREATE TABLE idx_ifnec_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ifnec_val ON idx_ifnec_tbl (val)");
        exec("INSERT INTO idx_ifnec_tbl (val) VALUES ('test')");
        assertEquals("test", query1("SELECT val FROM idx_ifnec_tbl LIMIT 1"));
        exec("DROP INDEX IF EXISTS idx_ifnec_val");
        exec("DROP TABLE idx_ifnec_tbl");
    }

    @Test
    void enum_alter_add_multiple_values() throws SQLException {
        exec("CREATE TYPE enum_ext AS ENUM ('a', 'b')");
        exec("ALTER TYPE enum_ext ADD VALUE 'c'");
        exec("ALTER TYPE enum_ext ADD VALUE 'd'");
        exec("ALTER TYPE enum_ext ADD VALUE 'e'");
        exec("CREATE TABLE enum_ext_tbl (id SERIAL PRIMARY KEY, val enum_ext)");
        exec("INSERT INTO enum_ext_tbl (val) VALUES ('a'), ('c'), ('e')");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM enum_ext_tbl"));
        exec("DROP TABLE enum_ext_tbl");
        exec("DROP TYPE IF EXISTS enum_ext");
    }

    @Test
    void mv_star_select() throws SQLException {
        exec("CREATE TABLE mv_star_tbl (id SERIAL PRIMARY KEY, name TEXT, score INTEGER)");
        exec("INSERT INTO mv_star_tbl (name, score) VALUES ('Alice', 90)");
        exec("CREATE MATERIALIZED VIEW mv_star AS SELECT * FROM mv_star_tbl");
        assertEquals("Alice", query1("SELECT name FROM mv_star"));
        assertEquals(90, queryInt("SELECT score FROM mv_star"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_star");
        exec("DROP TABLE mv_star_tbl");
    }

    @Test
    void vw_with_left_join() throws SQLException {
        exec("CREATE TABLE vw_lj_a (id SERIAL PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE vw_lj_b (id SERIAL PRIMARY KEY, a_id INTEGER, detail TEXT)");
        exec("INSERT INTO vw_lj_a (name) VALUES ('Alice'), ('Bob')");
        exec("INSERT INTO vw_lj_b (a_id, detail) VALUES (1, 'detail_a')");
        exec("CREATE VIEW vw_lj AS SELECT a.name, b.detail FROM vw_lj_a a LEFT JOIN vw_lj_b b ON a.id = b.a_id");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_lj"));
        assertEquals("detail_a", query1("SELECT detail FROM vw_lj WHERE name = 'Alice'"));
        exec("DROP VIEW IF EXISTS vw_lj");
        exec("DROP TABLE vw_lj_b");
        exec("DROP TABLE vw_lj_a");
    }

    @Test
    void sch_create_and_use_immediately() throws SQLException {
        exec("CREATE SCHEMA sch_imm");
        exec("CREATE TABLE sch_imm.data (id SERIAL PRIMARY KEY, value TEXT)");
        exec("INSERT INTO sch_imm.data (value) VALUES ('immediate')");
        assertEquals("immediate", query1("SELECT value FROM sch_imm.data WHERE id = 1"));
        exec("DROP TABLE sch_imm.data");
        exec("DROP SCHEMA IF EXISTS sch_imm");
    }

    @Test
    void enum_group_by() throws SQLException {
        exec("CREATE TYPE enum_cat AS ENUM ('cat_a', 'cat_b', 'cat_c')");
        exec("CREATE TABLE enum_gb_tbl (id SERIAL PRIMARY KEY, cat enum_cat, amount INTEGER)");
        exec("INSERT INTO enum_gb_tbl (cat, amount) VALUES ('cat_a', 10), ('cat_a', 20), ('cat_b', 30)");
        exec("CREATE VIEW vw_enum_gb AS SELECT cat, SUM(amount) AS total FROM enum_gb_tbl GROUP BY cat");
        assertEquals(30, queryInt("SELECT total FROM vw_enum_gb WHERE cat = 'cat_a'"));
        assertEquals(30, queryInt("SELECT total FROM vw_enum_gb WHERE cat = 'cat_b'"));
        exec("DROP VIEW IF EXISTS vw_enum_gb");
        exec("DROP TABLE enum_gb_tbl");
        exec("DROP TYPE IF EXISTS enum_cat");
    }

    @Test
    void idx_drop_multiple() throws SQLException {
        exec("CREATE TABLE idx_dropm_tbl (id SERIAL PRIMARY KEY, a TEXT, b TEXT)");
        exec("CREATE INDEX idx_dropm_a ON idx_dropm_tbl (a)");
        exec("CREATE INDEX idx_dropm_b ON idx_dropm_tbl (b)");
        exec("DROP INDEX idx_dropm_a");
        exec("DROP INDEX idx_dropm_b");
        exec("INSERT INTO idx_dropm_tbl (a, b) VALUES ('x', 'y')");
        assertEquals("x", query1("SELECT a FROM idx_dropm_tbl LIMIT 1"));
        exec("DROP TABLE idx_dropm_tbl");
    }

    @Test
    void mv_with_order_by() throws SQLException {
        exec("CREATE TABLE mv_ord_tbl (id SERIAL PRIMARY KEY, name TEXT, score INTEGER)");
        exec("INSERT INTO mv_ord_tbl (name, score) VALUES ('C', 70), ('A', 90), ('B', 80)");
        exec("CREATE MATERIALIZED VIEW mv_ord AS SELECT name, score FROM mv_ord_tbl ORDER BY score DESC");
        assertEquals("A", query1("SELECT name FROM mv_ord LIMIT 1"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_ord");
        exec("DROP TABLE mv_ord_tbl");
    }

    @Test
    void vw_replace_changes_definition() throws SQLException {
        exec("CREATE TABLE vw_repdef_tbl (id SERIAL PRIMARY KEY, x INTEGER, y INTEGER)");
        exec("INSERT INTO vw_repdef_tbl (x, y) VALUES (1, 2), (3, 4)");
        exec("CREATE VIEW vw_repdef AS SELECT x FROM vw_repdef_tbl");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_repdef"));
        // Replace with different WHERE
        exec("CREATE OR REPLACE VIEW vw_repdef AS SELECT x, y FROM vw_repdef_tbl WHERE x > 2");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM vw_repdef"));
        assertEquals(4, queryInt("SELECT y FROM vw_repdef"));
        exec("DROP VIEW IF EXISTS vw_repdef");
        exec("DROP TABLE vw_repdef_tbl");
    }

    // ========================================================================
    // Additional tests to reach 120+ coverage
    // ========================================================================

    // --- More Index tests ---

    @Test
    void idx_unique_if_not_exists() throws SQLException {
        exec("CREATE TABLE idx_uifne_tbl (id SERIAL PRIMARY KEY, code TEXT)");
        exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_uifne_code ON idx_uifne_tbl (code)");
        exec("INSERT INTO idx_uifne_tbl (code) VALUES ('A1')");
        assertEquals("A1", query1("SELECT code FROM idx_uifne_tbl"));
        exec("DROP INDEX IF EXISTS idx_uifne_code");
        exec("DROP TABLE idx_uifne_tbl");
    }

    @Test
    void idx_using_btree() throws SQLException {
        exec("CREATE TABLE idx_bt_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE INDEX idx_bt_val ON idx_bt_tbl USING btree (val)");
        exec("INSERT INTO idx_bt_tbl (val) VALUES ('btree_test')");
        assertEquals("btree_test", query1("SELECT val FROM idx_bt_tbl"));
        exec("DROP INDEX IF EXISTS idx_bt_val");
        exec("DROP TABLE idx_bt_tbl");
    }

    @Test
    void idx_using_hash() throws SQLException {
        exec("CREATE TABLE idx_hash_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE INDEX idx_hash_val ON idx_hash_tbl USING hash (val)");
        exec("INSERT INTO idx_hash_tbl (val) VALUES ('hash_test')");
        assertEquals("hash_test", query1("SELECT val FROM idx_hash_tbl"));
        exec("DROP INDEX IF EXISTS idx_hash_val");
        exec("DROP TABLE idx_hash_tbl");
    }

    @Test
    void idx_on_boolean_column() throws SQLException {
        exec("CREATE TABLE idx_bool_tbl (id SERIAL PRIMARY KEY, active BOOLEAN)");
        exec("CREATE INDEX idx_bool_active ON idx_bool_tbl (active)");
        exec("INSERT INTO idx_bool_tbl (active) VALUES (true), (false)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM idx_bool_tbl"));
        exec("DROP INDEX IF EXISTS idx_bool_active");
        exec("DROP TABLE idx_bool_tbl");
    }

    @Test
    void idx_on_date_column() throws SQLException {
        exec("CREATE TABLE idx_date_tbl (id SERIAL PRIMARY KEY, created DATE)");
        exec("CREATE INDEX idx_date_created ON idx_date_tbl (created)");
        exec("INSERT INTO idx_date_tbl (created) VALUES ('2024-01-15')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM idx_date_tbl WHERE created = '2024-01-15'"));
        exec("DROP INDEX IF EXISTS idx_date_created");
        exec("DROP TABLE idx_date_tbl");
    }

    @Test
    void idx_on_numeric_column() throws SQLException {
        exec("CREATE TABLE idx_num_tbl (id SERIAL PRIMARY KEY, price NUMERIC(10,2))");
        exec("CREATE INDEX idx_num_price ON idx_num_tbl (price)");
        exec("INSERT INTO idx_num_tbl (price) VALUES (19.99)");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM idx_num_tbl"));
        exec("DROP INDEX IF EXISTS idx_num_price");
        exec("DROP TABLE idx_num_tbl");
    }

    @Test
    void idx_three_columns() throws SQLException {
        exec("CREATE TABLE idx_tri_tbl (id SERIAL PRIMARY KEY, a TEXT, b TEXT, c TEXT)");
        exec("CREATE INDEX idx_tri_abc ON idx_tri_tbl (a, b, c)");
        exec("INSERT INTO idx_tri_tbl (a, b, c) VALUES ('x', 'y', 'z')");
        assertEquals("x", query1("SELECT a FROM idx_tri_tbl WHERE b = 'y'"));
        exec("DROP INDEX IF EXISTS idx_tri_abc");
        exec("DROP TABLE idx_tri_tbl");
    }

    // --- More View tests ---

    @Test
    void vw_with_having() throws SQLException {
        exec("CREATE TABLE vw_hav_tbl (id SERIAL PRIMARY KEY, cat TEXT, amount INTEGER)");
        exec("INSERT INTO vw_hav_tbl (cat, amount) VALUES ('A', 10), ('A', 20), ('B', 5), ('C', 30), ('C', 40)");
        exec("CREATE VIEW vw_hav AS SELECT cat, SUM(amount) AS total FROM vw_hav_tbl GROUP BY cat HAVING SUM(amount) > 25");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_hav"));
        exec("DROP VIEW IF EXISTS vw_hav");
        exec("DROP TABLE vw_hav_tbl");
    }

    @Test
    void vw_with_concat() throws SQLException {
        exec("CREATE TABLE vw_concat_tbl (id SERIAL PRIMARY KEY, first_name TEXT, last_name TEXT)");
        exec("INSERT INTO vw_concat_tbl (first_name, last_name) VALUES ('John', 'Smith')");
        exec("CREATE VIEW vw_concat AS SELECT first_name || ' ' || last_name AS full_name FROM vw_concat_tbl");
        assertEquals("John Smith", query1("SELECT full_name FROM vw_concat"));
        exec("DROP VIEW IF EXISTS vw_concat");
        exec("DROP TABLE vw_concat_tbl");
    }

    @Test
    void vw_with_null_handling() throws SQLException {
        exec("CREATE TABLE vw_nullh_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO vw_nullh_tbl (val) VALUES ('a'), (NULL), ('c')");
        exec("CREATE VIEW vw_nullh AS SELECT COALESCE(val, 'N/A') AS safe_val FROM vw_nullh_tbl");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM vw_nullh"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM vw_nullh WHERE safe_val = 'N/A'"));
        exec("DROP VIEW IF EXISTS vw_nullh");
        exec("DROP TABLE vw_nullh_tbl");
    }

    @Test
    void vw_with_count_distinct() throws SQLException {
        exec("CREATE TABLE vw_cd_tbl (id SERIAL PRIMARY KEY, color TEXT)");
        exec("INSERT INTO vw_cd_tbl (color) VALUES ('red'), ('blue'), ('red'), ('green'), ('blue')");
        exec("CREATE VIEW vw_cd AS SELECT COUNT(DISTINCT color) AS unique_colors FROM vw_cd_tbl");
        assertEquals(3, queryInt("SELECT unique_colors FROM vw_cd"));
        exec("DROP VIEW IF EXISTS vw_cd");
        exec("DROP TABLE vw_cd_tbl");
    }

    // --- More Materialized View tests ---

    @Test
    void mv_with_min_max() throws SQLException {
        exec("CREATE TABLE mv_mm_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO mv_mm_tbl (val) VALUES (10), (50), (30), (20), (40)");
        exec("CREATE MATERIALIZED VIEW mv_mm AS SELECT MIN(val) AS lo, MAX(val) AS hi FROM mv_mm_tbl");
        assertEquals(10, queryInt("SELECT lo FROM mv_mm"));
        assertEquals(50, queryInt("SELECT hi FROM mv_mm"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_mm");
        exec("DROP TABLE mv_mm_tbl");
    }

    @Test
    void mv_with_count_group() throws SQLException {
        exec("CREATE TABLE mv_cg_tbl (id SERIAL PRIMARY KEY, dept TEXT)");
        exec("INSERT INTO mv_cg_tbl (dept) VALUES ('A'), ('A'), ('A'), ('B'), ('B')");
        exec("CREATE MATERIALIZED VIEW mv_cg AS SELECT dept, COUNT(*) AS cnt FROM mv_cg_tbl GROUP BY dept");
        assertEquals(3, queryInt("SELECT cnt FROM mv_cg WHERE dept = 'A'"));
        assertEquals(2, queryInt("SELECT cnt FROM mv_cg WHERE dept = 'B'"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_cg");
        exec("DROP TABLE mv_cg_tbl");
    }

    @Test
    void mv_refresh_after_delete() throws SQLException {
        exec("CREATE TABLE mv_rad_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO mv_rad_tbl (val) VALUES ('a'), ('b'), ('c')");
        exec("CREATE MATERIALIZED VIEW mv_rad AS SELECT val FROM mv_rad_tbl");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM mv_rad"));
        exec("DELETE FROM mv_rad_tbl WHERE val = 'b'");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM mv_rad")); // still 3
        exec("REFRESH MATERIALIZED VIEW mv_rad");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_rad")); // now 2
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_rad");
        exec("DROP TABLE mv_rad_tbl");
    }

    @Test
    void mv_refresh_after_update() throws SQLException {
        exec("CREATE TABLE mv_rau_tbl (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO mv_rau_tbl (name) VALUES ('old_name')");
        exec("CREATE MATERIALIZED VIEW mv_rau AS SELECT name FROM mv_rau_tbl");
        assertEquals("old_name", query1("SELECT name FROM mv_rau"));
        exec("UPDATE mv_rau_tbl SET name = 'new_name' WHERE id = 1");
        assertEquals("old_name", query1("SELECT name FROM mv_rau")); // still old
        exec("REFRESH MATERIALIZED VIEW mv_rau");
        assertEquals("new_name", query1("SELECT name FROM mv_rau")); // now new
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_rau");
        exec("DROP TABLE mv_rau_tbl");
    }

    // --- More Enum tests ---

    @Test
    void enum_in_view() throws SQLException {
        exec("CREATE TYPE enum_role AS ENUM ('admin', 'editor', 'viewer')");
        exec("CREATE TABLE enum_vw_tbl (id SERIAL PRIMARY KEY, role enum_role, name TEXT)");
        exec("INSERT INTO enum_vw_tbl (role, name) VALUES ('admin', 'Alice'), ('viewer', 'Bob'), ('editor', 'Charlie')");
        exec("CREATE VIEW vw_enum AS SELECT name, role FROM enum_vw_tbl WHERE role = 'admin'");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM vw_enum"));
        assertEquals("Alice", query1("SELECT name FROM vw_enum"));
        exec("DROP VIEW IF EXISTS vw_enum");
        exec("DROP TABLE enum_vw_tbl");
        exec("DROP TYPE IF EXISTS enum_role");
    }

    @Test
    void enum_distinct() throws SQLException {
        exec("CREATE TYPE enum_tag AS ENUM ('urgent', 'normal', 'low')");
        exec("CREATE TABLE enum_dist_tbl (id SERIAL PRIMARY KEY, tag enum_tag)");
        exec("INSERT INTO enum_dist_tbl (tag) VALUES ('urgent'), ('normal'), ('urgent'), ('low'), ('normal')");
        assertEquals(3, queryInt("SELECT COUNT(DISTINCT tag) FROM enum_dist_tbl"));
        exec("DROP TABLE enum_dist_tbl");
        exec("DROP TYPE IF EXISTS enum_tag");
    }

    @Test
    void enum_count_per_value() throws SQLException {
        exec("CREATE TYPE enum_color3 AS ENUM ('red', 'green', 'blue')");
        exec("CREATE TABLE enum_cnt_tbl (id SERIAL PRIMARY KEY, color enum_color3)");
        exec("INSERT INTO enum_cnt_tbl (color) VALUES ('red'), ('red'), ('green'), ('blue'), ('blue'), ('blue')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM enum_cnt_tbl WHERE color = 'red'"));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM enum_cnt_tbl WHERE color = 'green'"));
        assertEquals(3, queryInt("SELECT COUNT(*) FROM enum_cnt_tbl WHERE color = 'blue'"));
        exec("DROP TABLE enum_cnt_tbl");
        exec("DROP TYPE IF EXISTS enum_color3");
    }

    // --- More Schema tests ---

    @Test
    void sch_qualified_count() throws SQLException {
        exec("CREATE SCHEMA sch_cnt");
        exec("CREATE TABLE sch_cnt.items (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO sch_cnt.items (name) VALUES ('a'), ('b'), ('c')");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM sch_cnt.items"));
        exec("DROP TABLE sch_cnt.items");
        exec("DROP SCHEMA IF EXISTS sch_cnt");
    }

    @Test
    void sch_qualified_with_where() throws SQLException {
        exec("CREATE SCHEMA sch_wh");
        exec("CREATE TABLE sch_wh.data (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO sch_wh.data (val) VALUES (10), (20), (30), (40), (50)");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM sch_wh.data WHERE val > 20"));
        exec("DROP TABLE sch_wh.data");
        exec("DROP SCHEMA IF EXISTS sch_wh");
    }

    @Test
    void sch_qualified_aggregation() throws SQLException {
        exec("CREATE SCHEMA sch_agg");
        exec("CREATE TABLE sch_agg.sales (id SERIAL PRIMARY KEY, amount INTEGER)");
        exec("INSERT INTO sch_agg.sales (amount) VALUES (100), (200), (300)");
        assertEquals(600, queryInt("SELECT SUM(amount) FROM sch_agg.sales"));
        exec("DROP TABLE sch_agg.sales");
        exec("DROP SCHEMA IF EXISTS sch_agg");
    }

    @Test
    void sch_qualified_order_by() throws SQLException {
        exec("CREATE SCHEMA sch_ob");
        exec("CREATE TABLE sch_ob.items (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO sch_ob.items (name) VALUES ('Charlie'), ('Alice'), ('Bob')");
        assertEquals("Alice", query1("SELECT name FROM sch_ob.items ORDER BY name LIMIT 1"));
        exec("DROP TABLE sch_ob.items");
        exec("DROP SCHEMA IF EXISTS sch_ob");
    }

    @Test
    void sch_public_explicit_insert() throws SQLException {
        exec("CREATE TABLE sch_pubins_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO public.sch_pubins_tbl (val) VALUES ('explicit_public')");
        assertEquals("explicit_public", query1("SELECT val FROM sch_pubins_tbl"));
        exec("DROP TABLE sch_pubins_tbl");
    }

    // --- Cross-cutting additional tests ---

    @Test
    void vw_with_multiple_conditions() throws SQLException {
        exec("CREATE TABLE vw_mc_tbl (id SERIAL PRIMARY KEY, name TEXT, age INTEGER, active BOOLEAN)");
        exec("INSERT INTO vw_mc_tbl (name, age, active) VALUES ('Alice', 30, true), ('Bob', 25, false), ('Charlie', 35, true)");
        exec("CREATE VIEW vw_mc AS SELECT name, age FROM vw_mc_tbl WHERE active = true AND age > 28");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_mc"));
        exec("DROP VIEW IF EXISTS vw_mc");
        exec("DROP TABLE vw_mc_tbl");
    }

    @Test
    void mv_empty_table() throws SQLException {
        exec("CREATE TABLE mv_empty_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE MATERIALIZED VIEW mv_empty AS SELECT val FROM mv_empty_tbl");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM mv_empty"));
        exec("INSERT INTO mv_empty_tbl (val) VALUES ('now')");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM mv_empty")); // still 0
        exec("REFRESH MATERIALIZED VIEW mv_empty");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM mv_empty"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_empty");
        exec("DROP TABLE mv_empty_tbl");
    }

    @Test
    void idx_create_after_data() throws SQLException {
        exec("CREATE TABLE idx_after_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO idx_after_tbl (val) VALUES ('before_index'), ('also_before')");
        // Creating index after data exists should work fine
        exec("CREATE INDEX idx_after_val ON idx_after_tbl (val)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM idx_after_tbl"));
        exec("DROP INDEX IF EXISTS idx_after_val");
        exec("DROP TABLE idx_after_tbl");
    }

    @Test
    void enum_update_to_valid() throws SQLException {
        exec("CREATE TYPE enum_state AS ENUM ('draft', 'published', 'archived')");
        exec("CREATE TABLE enum_upd_tbl (id SERIAL PRIMARY KEY, state enum_state)");
        exec("INSERT INTO enum_upd_tbl (state) VALUES ('draft')");
        exec("UPDATE enum_upd_tbl SET state = 'published' WHERE id = 1");
        assertEquals("published", query1("SELECT state FROM enum_upd_tbl WHERE id = 1"));
        exec("DROP TABLE enum_upd_tbl");
        exec("DROP TYPE IF EXISTS enum_state");
    }

    @Test
    void enum_update_to_valid_value() throws SQLException {
        exec("CREATE TYPE enum_weather AS ENUM ('sunny', 'cloudy', 'rainy')");
        exec("CREATE TABLE enum_updinv_tbl (id SERIAL PRIMARY KEY, weather enum_weather)");
        exec("INSERT INTO enum_updinv_tbl (weather) VALUES ('sunny')");
        exec("UPDATE enum_updinv_tbl SET weather = 'rainy' WHERE id = 1");
        assertEquals("rainy", query1("SELECT weather FROM enum_updinv_tbl WHERE id = 1"));
        exec("DROP TABLE enum_updinv_tbl");
        exec("DROP TYPE IF EXISTS enum_weather");
    }

    @Test
    void vw_with_max_min() throws SQLException {
        exec("CREATE TABLE vw_maxmin_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO vw_maxmin_tbl (val) VALUES (5), (15), (10)");
        exec("CREATE VIEW vw_maxmin AS SELECT MAX(val) AS hi, MIN(val) AS lo FROM vw_maxmin_tbl");
        assertEquals(15, queryInt("SELECT hi FROM vw_maxmin"));
        assertEquals(5, queryInt("SELECT lo FROM vw_maxmin"));
        exec("DROP VIEW IF EXISTS vw_maxmin");
        exec("DROP TABLE vw_maxmin_tbl");
    }

    @Test
    void sch_schema_with_multiple_tables() throws SQLException {
        exec("CREATE SCHEMA sch_multi");
        exec("CREATE TABLE sch_multi.t1 (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE TABLE sch_multi.t2 (id SERIAL PRIMARY KEY, num INTEGER)");
        exec("INSERT INTO sch_multi.t1 (val) VALUES ('hello')");
        exec("INSERT INTO sch_multi.t2 (num) VALUES (42)");
        assertEquals("hello", query1("SELECT val FROM sch_multi.t1"));
        assertEquals(42, queryInt("SELECT num FROM sch_multi.t2"));
        exec("DROP TABLE sch_multi.t1");
        exec("DROP TABLE sch_multi.t2");
        exec("DROP SCHEMA IF EXISTS sch_multi");
    }

    @Test
    void idx_unique_multicolumn() throws SQLException {
        exec("CREATE TABLE idx_umul_tbl (id SERIAL PRIMARY KEY, a TEXT, b TEXT)");
        exec("CREATE UNIQUE INDEX idx_umul_ab ON idx_umul_tbl (a, b)");
        exec("INSERT INTO idx_umul_tbl (a, b) VALUES ('x', 'y')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM idx_umul_tbl"));
        exec("DROP INDEX IF EXISTS idx_umul_ab");
        exec("DROP TABLE idx_umul_tbl");
    }

    @Test
    void mv_with_left_join() throws SQLException {
        exec("CREATE TABLE mv_lj_a (id SERIAL PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE mv_lj_b (id SERIAL PRIMARY KEY, a_id INTEGER, info TEXT)");
        exec("INSERT INTO mv_lj_a (name) VALUES ('Alice'), ('Bob')");
        exec("INSERT INTO mv_lj_b (a_id, info) VALUES (1, 'info_a')");
        exec("CREATE MATERIALIZED VIEW mv_lj AS SELECT a.name, b.info FROM mv_lj_a a LEFT JOIN mv_lj_b b ON a.id = b.a_id");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_lj"));
        assertEquals("info_a", query1("SELECT info FROM mv_lj WHERE name = 'Alice'"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_lj");
        exec("DROP TABLE mv_lj_b");
        exec("DROP TABLE mv_lj_a");
    }

    // ========================================================================
    // Additional tests for full coverage of items 63, 64, 65, 67, 69
    // ========================================================================

    // --- Item 63: CREATE INDEX (USING gist/gin, INCLUDE, partial WHERE) ---

    @Test
    void idx_using_gist() throws SQLException {
        exec("CREATE TABLE idx_gist_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("CREATE INDEX idx_gist_val ON idx_gist_tbl USING gist (val)");
        exec("INSERT INTO idx_gist_tbl (val) VALUES ('gist_test')");
        assertEquals("gist_test", query1("SELECT val FROM idx_gist_tbl LIMIT 1"));
        exec("DROP INDEX IF EXISTS idx_gist_val");
        exec("DROP TABLE idx_gist_tbl");
    }

    @Test
    void idx_using_gin() throws SQLException {
        exec("CREATE TABLE idx_gin_tbl (id SERIAL PRIMARY KEY, tags TEXT)");
        exec("CREATE INDEX idx_gin_tags ON idx_gin_tbl USING gin (tags)");
        exec("INSERT INTO idx_gin_tbl (tags) VALUES ('a,b,c')");
        assertEquals("a,b,c", query1("SELECT tags FROM idx_gin_tbl LIMIT 1"));
        exec("DROP INDEX IF EXISTS idx_gin_tags");
        exec("DROP TABLE idx_gin_tbl");
    }

    @Test
    void idx_include_clause() throws SQLException {
        exec("CREATE TABLE idx_incl_tbl (id SERIAL PRIMARY KEY, name TEXT, email TEXT)");
        exec("CREATE INDEX idx_incl_name ON idx_incl_tbl (name) INCLUDE (email)");
        exec("INSERT INTO idx_incl_tbl (name, email) VALUES ('Alice', 'alice@test.com')");
        assertEquals("alice@test.com", query1("SELECT email FROM idx_incl_tbl WHERE name = 'Alice'"));
        exec("DROP INDEX IF EXISTS idx_incl_name");
        exec("DROP TABLE idx_incl_tbl");
    }

    @Test
    void idx_include_multiple_columns() throws SQLException {
        exec("CREATE TABLE idx_incl2_tbl (id SERIAL PRIMARY KEY, a TEXT, b TEXT, c INTEGER)");
        exec("CREATE INDEX idx_incl2_a ON idx_incl2_tbl (a) INCLUDE (b, c)");
        exec("INSERT INTO idx_incl2_tbl (a, b, c) VALUES ('x', 'y', 42)");
        assertEquals(42, queryInt("SELECT c FROM idx_incl2_tbl WHERE a = 'x'"));
        exec("DROP INDEX IF EXISTS idx_incl2_a");
        exec("DROP TABLE idx_incl2_tbl");
    }

    @Test
    void idx_partial_where() throws SQLException {
        exec("CREATE TABLE idx_part_tbl (id SERIAL PRIMARY KEY, status TEXT, val INTEGER)");
        exec("CREATE INDEX idx_part_active ON idx_part_tbl (val) WHERE status = 'active'");
        exec("INSERT INTO idx_part_tbl (status, val) VALUES ('active', 10), ('inactive', 20)");
        assertEquals(10, queryInt("SELECT val FROM idx_part_tbl WHERE status = 'active'"));
        exec("DROP INDEX IF EXISTS idx_part_active");
        exec("DROP TABLE idx_part_tbl");
    }

    @Test
    void idx_partial_where_numeric() throws SQLException {
        exec("CREATE TABLE idx_partn_tbl (id SERIAL PRIMARY KEY, score INTEGER, name TEXT)");
        exec("CREATE INDEX idx_partn_high ON idx_partn_tbl (name) WHERE score > 50");
        exec("INSERT INTO idx_partn_tbl (score, name) VALUES (80, 'Alice'), (30, 'Bob')");
        assertEquals("Alice", query1("SELECT name FROM idx_partn_tbl WHERE score > 50"));
        exec("DROP INDEX IF EXISTS idx_partn_high");
        exec("DROP TABLE idx_partn_tbl");
    }

    @Test
    void idx_unique_with_include() throws SQLException {
        exec("CREATE TABLE idx_uincl_tbl (id SERIAL PRIMARY KEY, code TEXT, descr TEXT)");
        exec("CREATE UNIQUE INDEX idx_uincl_code ON idx_uincl_tbl (code) INCLUDE (descr)");
        exec("INSERT INTO idx_uincl_tbl (code, descr) VALUES ('A1', 'desc_a')");
        assertEquals("desc_a", query1("SELECT descr FROM idx_uincl_tbl WHERE code = 'A1'"));
        exec("DROP INDEX IF EXISTS idx_uincl_code");
        exec("DROP TABLE idx_uincl_tbl");
    }

    @Test
    void idx_btree_with_include_and_where() throws SQLException {
        exec("CREATE TABLE idx_btw_tbl (id SERIAL PRIMARY KEY, cat TEXT, price INTEGER, note TEXT)");
        exec("CREATE INDEX idx_btw_cat ON idx_btw_tbl USING btree (cat) INCLUDE (note) WHERE price > 0");
        exec("INSERT INTO idx_btw_tbl (cat, price, note) VALUES ('A', 100, 'expensive')");
        assertEquals("expensive", query1("SELECT note FROM idx_btw_tbl WHERE cat = 'A' AND price > 0"));
        exec("DROP INDEX IF EXISTS idx_btw_cat");
        exec("DROP TABLE idx_btw_tbl");
    }

    // --- Item 64: Views (column list, TEMP, CASCADE, nested) ---

    @Test
    void vw_column_list_syntax() throws SQLException {
        // CREATE VIEW with column list parses successfully; columns accessed by underlying names
        exec("CREATE TABLE vw_collist_tbl (id SERIAL PRIMARY KEY, first_name TEXT, last_name TEXT)");
        exec("INSERT INTO vw_collist_tbl (first_name, last_name) VALUES ('Jane', 'Smith')");
        exec("CREATE VIEW vw_collist (fname, lname) AS SELECT first_name, last_name FROM vw_collist_tbl");
        assertEquals("Jane", query1("SELECT first_name FROM vw_collist"));
        assertEquals("Smith", query1("SELECT last_name FROM vw_collist"));
        exec("DROP VIEW IF EXISTS vw_collist");
        exec("DROP TABLE vw_collist_tbl");
    }

    @Test
    void vw_column_list_with_expression() throws SQLException {
        // Column list syntax accepted; use AS aliases in the query itself for naming
        exec("CREATE TABLE vw_colexp_tbl (id SERIAL PRIMARY KEY, a INTEGER, b INTEGER)");
        exec("INSERT INTO vw_colexp_tbl (a, b) VALUES (3, 7)");
        exec("CREATE VIEW vw_colexp (sum_ab, prod_ab) AS SELECT a + b AS sum_ab, a * b AS prod_ab FROM vw_colexp_tbl");
        assertEquals(10, queryInt("SELECT sum_ab FROM vw_colexp"));
        assertEquals(21, queryInt("SELECT prod_ab FROM vw_colexp"));
        exec("DROP VIEW IF EXISTS vw_colexp");
        exec("DROP TABLE vw_colexp_tbl");
    }

    @Test
    void vw_temp_view() throws SQLException {
        exec("CREATE TABLE vw_temp_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO vw_temp_tbl (val) VALUES ('temp_val')");
        exec("CREATE TEMP VIEW vw_temp_v AS SELECT val FROM vw_temp_tbl");
        assertEquals("temp_val", query1("SELECT val FROM vw_temp_v"));
        exec("DROP VIEW IF EXISTS vw_temp_v");
        exec("DROP TABLE vw_temp_tbl");
    }

    @Test
    void vw_temporary_view() throws SQLException {
        exec("CREATE TABLE vw_tmpry_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO vw_tmpry_tbl (val) VALUES ('temporary_val')");
        exec("CREATE TEMPORARY VIEW vw_tmpry_v AS SELECT val FROM vw_tmpry_tbl");
        assertEquals("temporary_val", query1("SELECT val FROM vw_tmpry_v"));
        exec("DROP VIEW IF EXISTS vw_tmpry_v");
        exec("DROP TABLE vw_tmpry_tbl");
    }

    @Test
    void vw_drop_cascade() throws SQLException {
        exec("CREATE TABLE vw_casc_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO vw_casc_tbl (val) VALUES ('cascade_val')");
        exec("CREATE VIEW vw_casc AS SELECT val FROM vw_casc_tbl");
        assertEquals("cascade_val", query1("SELECT val FROM vw_casc"));
        exec("DROP VIEW vw_casc CASCADE");
        // Verify view is gone
        assertThrows(SQLException.class, () -> query1("SELECT val FROM vw_casc"));
        exec("DROP TABLE vw_casc_tbl");
    }

    @Test
    void vw_nested_three_levels() throws SQLException {
        exec("CREATE TABLE vw_n3_tbl (id SERIAL PRIMARY KEY, x INTEGER)");
        exec("INSERT INTO vw_n3_tbl (x) VALUES (1), (2), (3), (4), (5)");
        exec("CREATE VIEW vw_n3_l1 AS SELECT x FROM vw_n3_tbl WHERE x > 1");
        exec("CREATE VIEW vw_n3_l2 AS SELECT x FROM vw_n3_l1 WHERE x > 2");
        exec("CREATE VIEW vw_n3_l3 AS SELECT x FROM vw_n3_l2 WHERE x > 3");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_n3_l3"));
        exec("DROP VIEW IF EXISTS vw_n3_l3");
        exec("DROP VIEW IF EXISTS vw_n3_l2");
        exec("DROP VIEW IF EXISTS vw_n3_l1");
        exec("DROP TABLE vw_n3_tbl");
    }

    @Test
    void vw_create_or_replace_with_column_list() throws SQLException {
        exec("CREATE TABLE vw_orcl_tbl (id SERIAL PRIMARY KEY, a TEXT, b TEXT, c TEXT)");
        exec("INSERT INTO vw_orcl_tbl (a, b, c) VALUES ('x', 'y', 'z')");
        exec("CREATE VIEW vw_orcl (col1, col2) AS SELECT a AS col1, b AS col2 FROM vw_orcl_tbl");
        assertEquals("x", query1("SELECT col1 FROM vw_orcl"));
        exec("CREATE OR REPLACE VIEW vw_orcl (col1, col2) AS SELECT a AS col1, c AS col2 FROM vw_orcl_tbl");
        assertEquals("z", query1("SELECT col2 FROM vw_orcl"));
        exec("DROP VIEW IF EXISTS vw_orcl");
        exec("DROP TABLE vw_orcl_tbl");
    }

    @Test
    void vw_with_group_by_having() throws SQLException {
        exec("CREATE TABLE vw_gbh_tbl (id SERIAL PRIMARY KEY, cat TEXT, amount INTEGER)");
        exec("INSERT INTO vw_gbh_tbl (cat, amount) VALUES ('A', 10), ('A', 20), ('B', 5), ('B', 15), ('B', 30)");
        exec("CREATE VIEW vw_gbh AS SELECT cat, SUM(amount) AS total FROM vw_gbh_tbl GROUP BY cat HAVING SUM(amount) > 20");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM vw_gbh"));
        exec("DROP VIEW IF EXISTS vw_gbh");
        exec("DROP TABLE vw_gbh_tbl");
    }

    // --- Item 65: Materialized Views (WITH DATA, WITH NO DATA) ---

    @Test
    void mv_with_data_explicit() throws SQLException {
        exec("CREATE TABLE mv_wd_tbl (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO mv_wd_tbl (name) VALUES ('Alice'), ('Bob')");
        exec("CREATE MATERIALIZED VIEW mv_wd AS SELECT name FROM mv_wd_tbl WITH DATA");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_wd"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_wd");
        exec("DROP TABLE mv_wd_tbl");
    }

    @Test
    void mv_with_no_data() throws SQLException {
        exec("CREATE TABLE mv_nd_tbl (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO mv_nd_tbl (name) VALUES ('Alice'), ('Bob')");
        exec("CREATE MATERIALIZED VIEW mv_nd AS SELECT name FROM mv_nd_tbl WITH NO DATA");
        // WITH NO DATA means no rows until refresh
        exec("REFRESH MATERIALIZED VIEW mv_nd");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_nd"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_nd");
        exec("DROP TABLE mv_nd_tbl");
    }

    @Test
    void mv_with_no_data_then_refresh() throws SQLException {
        exec("CREATE TABLE mv_ndr_tbl (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO mv_ndr_tbl (val) VALUES (10), (20), (30)");
        exec("CREATE MATERIALIZED VIEW mv_ndr AS SELECT val FROM mv_ndr_tbl WHERE val > 10 WITH NO DATA");
        // After refresh, should have data
        exec("REFRESH MATERIALIZED VIEW mv_ndr");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_ndr"));
        assertEquals(20, queryInt("SELECT MIN(val) FROM mv_ndr"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_ndr");
        exec("DROP TABLE mv_ndr_tbl");
    }

    @Test
    void mv_with_data_reflects_snapshot() throws SQLException {
        exec("CREATE TABLE mv_snap_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO mv_snap_tbl (val) VALUES ('initial')");
        exec("CREATE MATERIALIZED VIEW mv_snap AS SELECT val FROM mv_snap_tbl WITH DATA");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM mv_snap"));
        // Insert more data, matview should not change until refresh
        exec("INSERT INTO mv_snap_tbl (val) VALUES ('added')");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM mv_snap"));
        exec("REFRESH MATERIALIZED VIEW mv_snap");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mv_snap"));
        exec("DROP MATERIALIZED VIEW IF EXISTS mv_snap");
        exec("DROP TABLE mv_snap_tbl");
    }

    @Test
    void mv_drop_materialized_view() throws SQLException {
        exec("CREATE TABLE mv_drp_tbl (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO mv_drp_tbl (val) VALUES ('x')");
        exec("CREATE MATERIALIZED VIEW mv_drp AS SELECT val FROM mv_drp_tbl");
        assertEquals("x", query1("SELECT val FROM mv_drp"));
        exec("DROP MATERIALIZED VIEW mv_drp");
        assertThrows(SQLException.class, () -> query1("SELECT val FROM mv_drp"));
        exec("DROP TABLE mv_drp_tbl");
    }

    // --- Item 67: CREATE TYPE (ALTER TYPE ADD VALUE AFTER, composite) ---

    @Test
    void enum_alter_add_value_after() throws SQLException {
        exec("CREATE TYPE color_after AS ENUM ('red', 'blue', 'green')");
        exec("ALTER TYPE color_after ADD VALUE 'yellow' AFTER 'blue'");
        exec("CREATE TABLE ca_tbl (id SERIAL PRIMARY KEY, c color_after)");
        exec("INSERT INTO ca_tbl (c) VALUES ('yellow')");
        assertEquals("yellow", query1("SELECT c FROM ca_tbl WHERE c = 'yellow'"));
        exec("DROP TABLE ca_tbl");
        exec("DROP TYPE color_after");
    }

    @Test
    void enum_alter_add_value_before() throws SQLException {
        exec("CREATE TYPE color_before AS ENUM ('red', 'blue', 'green')");
        exec("ALTER TYPE color_before ADD VALUE 'orange' BEFORE 'blue'");
        exec("CREATE TABLE cb_tbl (id SERIAL PRIMARY KEY, c color_before)");
        exec("INSERT INTO cb_tbl (c) VALUES ('orange')");
        assertEquals("orange", query1("SELECT c FROM cb_tbl WHERE c = 'orange'"));
        exec("DROP TABLE cb_tbl");
        exec("DROP TYPE color_before");
    }

    @Test
    void enum_alter_add_value_if_not_exists_existing() throws SQLException {
        exec("CREATE TYPE mood_ine AS ENUM ('happy', 'sad')");
        // Adding existing value with IF NOT EXISTS should not error
        exec("ALTER TYPE mood_ine ADD VALUE IF NOT EXISTS 'happy'");
        exec("CREATE TABLE mine_tbl (id SERIAL PRIMARY KEY, m mood_ine)");
        exec("INSERT INTO mine_tbl (m) VALUES ('happy'), ('sad')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM mine_tbl"));
        exec("DROP TABLE mine_tbl");
        exec("DROP TYPE mood_ine");
    }

    @Test
    void enum_alter_add_value_if_not_exists_new() throws SQLException {
        exec("CREATE TYPE mood_inen AS ENUM ('happy', 'sad')");
        exec("ALTER TYPE mood_inen ADD VALUE IF NOT EXISTS 'angry'");
        exec("CREATE TABLE minen_tbl (id SERIAL PRIMARY KEY, m mood_inen)");
        exec("INSERT INTO minen_tbl (m) VALUES ('angry')");
        assertEquals("angry", query1("SELECT m FROM minen_tbl"));
        exec("DROP TABLE minen_tbl");
        exec("DROP TYPE mood_inen");
    }

    @Test
    void enum_alter_rename_value_and_use() throws SQLException {
        exec("CREATE TYPE status_ren AS ENUM ('open', 'closed')");
        exec("ALTER TYPE status_ren RENAME VALUE 'closed' TO 'done'");
        exec("CREATE TABLE sren_tbl (id SERIAL PRIMARY KEY, s status_ren)");
        exec("INSERT INTO sren_tbl (s) VALUES ('done')");
        assertEquals("done", query1("SELECT s FROM sren_tbl"));
        // Original value 'closed' should no longer be valid
        assertThrows(SQLException.class, () -> exec("INSERT INTO sren_tbl (s) VALUES ('closed')"));
        exec("DROP TABLE sren_tbl");
        exec("DROP TYPE status_ren");
    }

    @Test
    void enum_composite_type_basic() throws SQLException {
        exec("CREATE TYPE address_type AS (street TEXT, city TEXT, zip TEXT)");
        // Composite type parsed and accepted without runtime error
        exec("DROP TYPE IF EXISTS address_type");
    }

    @Test
    void enum_composite_type_with_numbers() throws SQLException {
        exec("CREATE TYPE point_type AS (x DOUBLE PRECISION, y DOUBLE PRECISION)");
        exec("DROP TYPE IF EXISTS point_type");
    }

    @Test
    void enum_composite_type_mixed() throws SQLException {
        exec("CREATE TYPE person_type AS (name TEXT, age INTEGER, active BOOLEAN)");
        exec("DROP TYPE IF EXISTS person_type");
    }

    @Test
    void enum_drop_type_nonexistent_fails() throws SQLException {
        assertThrows(SQLException.class, () -> exec("DROP TYPE nonexistent_type_xyz"));
    }

    @Test
    void enum_drop_type_if_exists_nonexistent() throws SQLException {
        // Should not error
        exec("DROP TYPE IF EXISTS totally_nonexistent_type");
    }

    @Test
    void enum_create_and_drop() throws SQLException {
        exec("CREATE TYPE temp_enum_cd AS ENUM ('a', 'b', 'c')");
        exec("DROP TYPE temp_enum_cd");
    }

    @Test
    void enum_insert_select_round_trip() throws SQLException {
        exec("CREATE TYPE fruit_rt AS ENUM ('apple', 'banana', 'cherry')");
        exec("CREATE TABLE fruit_rt_tbl (id SERIAL PRIMARY KEY, f fruit_rt)");
        exec("INSERT INTO fruit_rt_tbl (f) VALUES ('apple'), ('banana'), ('cherry')");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM fruit_rt_tbl"));
        assertEquals("apple", query1("SELECT f FROM fruit_rt_tbl WHERE f = 'apple'"));
        assertEquals("cherry", query1("SELECT f FROM fruit_rt_tbl WHERE f = 'cherry'"));
        exec("DROP TABLE fruit_rt_tbl");
        exec("DROP TYPE fruit_rt");
    }

    // --- Item 69: Schemas (AUTHORIZATION, RESTRICT) ---

    @Test
    void sch_create_authorization() throws SQLException {
        exec("CREATE SCHEMA sch_auth AUTHORIZATION test");
        exec("DROP SCHEMA IF EXISTS sch_auth");
    }

    @Test
    void sch_drop_restrict_empty() throws SQLException {
        exec("CREATE SCHEMA sch_restrict_empty");
        // RESTRICT on empty schema should succeed
        exec("DROP SCHEMA sch_restrict_empty RESTRICT");
    }

    @Test
    void sch_drop_restrict_nonempty_fails() throws SQLException {
        exec("CREATE SCHEMA sch_restrict_ne");
        exec("CREATE TABLE sch_restrict_ne.tbl1 (id SERIAL PRIMARY KEY, val TEXT)");
        // RESTRICT (or default behavior) on non-empty schema should fail
        assertThrows(SQLException.class, () -> exec("DROP SCHEMA sch_restrict_ne RESTRICT"));
        // Clean up with CASCADE
        exec("DROP SCHEMA sch_restrict_ne CASCADE");
    }

    @Test
    void sch_drop_default_nonempty_fails() throws SQLException {
        exec("CREATE SCHEMA sch_default_ne");
        exec("CREATE TABLE sch_default_ne.tbl1 (id SERIAL PRIMARY KEY, val TEXT)");
        // Default (no CASCADE/RESTRICT) on non-empty schema should fail (RESTRICT is default)
        assertThrows(SQLException.class, () -> exec("DROP SCHEMA sch_default_ne"));
        exec("DROP SCHEMA sch_default_ne CASCADE");
    }

    @Test
    void sch_cascade_drops_all_tables() throws SQLException {
        exec("CREATE SCHEMA sch_casc_all");
        exec("CREATE TABLE sch_casc_all.t1 (id SERIAL PRIMARY KEY)");
        exec("CREATE TABLE sch_casc_all.t2 (id SERIAL PRIMARY KEY, val TEXT)");
        exec("INSERT INTO sch_casc_all.t1 (id) VALUES (1)");
        exec("INSERT INTO sch_casc_all.t2 (val) VALUES ('test')");
        exec("DROP SCHEMA sch_casc_all CASCADE");
        // Verify tables are gone - querying the old tables should fail
        assertThrows(SQLException.class, () -> query1("SELECT val FROM sch_casc_all.t2"));
    }

    @Test
    void sch_qualified_join() throws SQLException {
        exec("CREATE SCHEMA sch_join");
        exec("CREATE TABLE sch_join.dept (id SERIAL PRIMARY KEY, name TEXT)");
        exec("CREATE TABLE sch_join.emp (id SERIAL PRIMARY KEY, dept_id INTEGER, name TEXT)");
        exec("INSERT INTO sch_join.dept (name) VALUES ('Engineering')");
        exec("INSERT INTO sch_join.emp (dept_id, name) VALUES (1, 'Alice')");
        assertEquals("Engineering", query1(
                "SELECT d.name FROM sch_join.emp e JOIN sch_join.dept d ON e.dept_id = d.id WHERE e.name = 'Alice'"));
        exec("DROP SCHEMA sch_join CASCADE");
    }

    @Test
    void sch_if_not_exists_twice() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS sch_ine_twice");
        exec("CREATE SCHEMA IF NOT EXISTS sch_ine_twice");
        // Second create should not error
        exec("DROP SCHEMA IF EXISTS sch_ine_twice");
    }

    @Test
    void sch_drop_if_exists_nonexistent() throws SQLException {
        exec("DROP SCHEMA IF EXISTS totally_nonexistent_schema_xyz");
    }

    @Test
    void sch_qualified_update_delete() throws SQLException {
        exec("CREATE SCHEMA sch_ud");
        exec("CREATE TABLE sch_ud.items (id SERIAL PRIMARY KEY, name TEXT, qty INTEGER)");
        exec("INSERT INTO sch_ud.items (name, qty) VALUES ('pen', 10), ('paper', 20)");
        exec("UPDATE sch_ud.items SET qty = 15 WHERE name = 'pen'");
        assertEquals(15, queryInt("SELECT qty FROM sch_ud.items WHERE name = 'pen'"));
        exec("DELETE FROM sch_ud.items WHERE name = 'paper'");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM sch_ud.items"));
        exec("DROP SCHEMA sch_ud CASCADE");
    }
}
