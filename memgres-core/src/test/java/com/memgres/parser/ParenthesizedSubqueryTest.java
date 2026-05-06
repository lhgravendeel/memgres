package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that Memgres matches PG 18 parenthesization behavior:
 * extra parentheses around subqueries/SELECT in all SQL contexts.
 *
 * Covers 11 root causes identified in the parenthesization audit.
 */
class ParenthesizedSubqueryTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE src (id serial PRIMARY KEY, val int, label text)");
        exec("INSERT INTO src (val, label) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')");
        exec("CREATE TABLE dst (id serial PRIMARY KEY, val int, label text)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // Root cause 1: INSERT with multi-level parens around SELECT
    // =========================================================================

    @Test void insert_select_double_parens_with_columns() throws SQLException {
        exec("TRUNCATE dst");
        exec("INSERT INTO dst (val, label) ((SELECT val, label FROM src WHERE id = 1))");
        assertEquals(1, queryInt("SELECT count(*) FROM dst"));
        exec("TRUNCATE dst");
    }

    @Test void insert_select_triple_parens_with_columns() throws SQLException {
        exec("TRUNCATE dst");
        exec("INSERT INTO dst (val, label) (((SELECT val, label FROM src WHERE id = 1)))");
        assertEquals(1, queryInt("SELECT count(*) FROM dst"));
        exec("TRUNCATE dst");
    }

    @Test void insert_select_5_parens_with_columns() throws SQLException {
        exec("TRUNCATE dst");
        exec("INSERT INTO dst (val, label) (((((SELECT val, label FROM src WHERE id = 1)))))");
        assertEquals(1, queryInt("SELECT count(*) FROM dst"));
        exec("TRUNCATE dst");
    }

    @Test void insert_select_double_parens_no_columns() throws SQLException {
        exec("TRUNCATE dst");
        exec("INSERT INTO dst ((SELECT id, val, label FROM src WHERE id = 1))");
        assertEquals(1, queryInt("SELECT count(*) FROM dst"));
        exec("TRUNCATE dst");
    }

    @Test void insert_select_triple_parens_no_columns() throws SQLException {
        exec("TRUNCATE dst");
        exec("INSERT INTO dst (((SELECT id, val, label FROM src WHERE id = 1)))");
        assertEquals(1, queryInt("SELECT count(*) FROM dst"));
        exec("TRUNCATE dst");
    }

    @Test void insert_select_5_parens_no_columns() throws SQLException {
        exec("TRUNCATE dst");
        exec("INSERT INTO dst (((((SELECT id, val, label FROM src WHERE id = 1)))))");
        assertEquals(1, queryInt("SELECT count(*) FROM dst"));
        exec("TRUNCATE dst");
    }

    // =========================================================================
    // Root cause 2: EXISTS with extra parens
    // =========================================================================

    @Test void exists_double_parens() throws SQLException {
        assertEquals("t", querySingle("SELECT EXISTS ((SELECT 1))"));
    }

    @Test void exists_triple_parens() throws SQLException {
        assertEquals("t", querySingle("SELECT EXISTS (((SELECT 1)))"));
    }

    @Test void exists_5_parens() throws SQLException {
        assertEquals("t", querySingle("SELECT EXISTS (((((SELECT 1)))))"));
    }

    @Test void exists_10_parens() throws SQLException {
        assertEquals("t", querySingle("SELECT EXISTS ((((((((((SELECT 1))))))))))"));
    }

    @Test void not_exists_double_parens() throws SQLException {
        assertEquals("t", querySingle("SELECT NOT EXISTS ((SELECT 1 WHERE false))"));
    }

    @Test void exists_in_where_double_parens() throws SQLException {
        assertEquals(5, queryInt("SELECT count(*) FROM src WHERE EXISTS ((SELECT 1 FROM src s2 WHERE s2.id = src.id))"));
    }

    // =========================================================================
    // Root cause 3: ALL with extra parens
    // =========================================================================

    @Test void all_double_parens() throws SQLException {
        assertEquals(3, queryInt("SELECT count(*) FROM src WHERE val > ALL ((SELECT val FROM src WHERE val <= 2))"));
    }

    @Test void all_5_parens() throws SQLException {
        assertEquals(3, queryInt("SELECT count(*) FROM src WHERE val > ALL (((((SELECT val FROM src WHERE val <= 2)))))"));
    }

    // =========================================================================
    // Root cause 4: ARRAY constructor with extra parens
    // =========================================================================

    @Test void array_subquery_double_parens() throws SQLException {
        assertEquals("{1,2,3,4,5}", querySingle("SELECT ARRAY((SELECT val FROM src ORDER BY val))"));
    }

    @Test void array_subquery_triple_parens() throws SQLException {
        assertEquals("{1,2,3,4,5}", querySingle("SELECT ARRAY(((SELECT val FROM src ORDER BY val)))"));
    }

    @Test void array_subquery_5_parens() throws SQLException {
        assertEquals("{1,2,3,4,5}", querySingle("SELECT ARRAY(((((SELECT val FROM src ORDER BY val)))))"));
    }

    // =========================================================================
    // Root cause 5: CREATE VIEW / MATERIALIZED VIEW AS with multi-parens
    // =========================================================================

    @Test void create_view_double_parens() throws SQLException {
        exec("CREATE VIEW ptest_v1 AS ((SELECT val FROM src))");
        try {
            assertEquals(5, queryInt("SELECT count(*) FROM ptest_v1"));
        } finally {
            exec("DROP VIEW ptest_v1");
        }
    }

    @Test void create_view_5_parens() throws SQLException {
        exec("CREATE VIEW ptest_v2 AS (((((SELECT val FROM src)))))");
        try {
            assertEquals(5, queryInt("SELECT count(*) FROM ptest_v2"));
        } finally {
            exec("DROP VIEW ptest_v2");
        }
    }

    @Test void create_materialized_view_double_parens() throws SQLException {
        exec("CREATE MATERIALIZED VIEW ptest_mv1 AS ((SELECT val FROM src))");
        try {
            assertEquals(5, queryInt("SELECT count(*) FROM ptest_mv1"));
        } finally {
            exec("DROP MATERIALIZED VIEW ptest_mv1");
        }
    }

    @Test void create_materialized_view_5_parens() throws SQLException {
        exec("CREATE MATERIALIZED VIEW ptest_mv2 AS (((((SELECT val FROM src)))))");
        try {
            assertEquals(5, queryInt("SELECT count(*) FROM ptest_mv2"));
        } finally {
            exec("DROP MATERIALIZED VIEW ptest_mv2");
        }
    }

    // =========================================================================
    // Root cause 6: CTE body with extra parens
    // =========================================================================

    @Test void cte_body_double_parens() throws SQLException {
        assertEquals(5, queryInt("WITH cte AS ((SELECT val FROM src)) SELECT count(*) FROM cte"));
    }

    @Test void cte_body_5_parens() throws SQLException {
        assertEquals(5, queryInt("WITH cte AS (((((SELECT val FROM src))))) SELECT count(*) FROM cte"));
    }

    @Test void cte_with_column_list_double_parens() throws SQLException {
        assertEquals(5, queryInt("WITH cte (v) AS ((SELECT val FROM src)) SELECT count(*) FROM cte"));
    }

    @Test void cte_multiple_mixed_parens() throws SQLException {
        assertEquals(5, queryInt(
            "WITH cte1 AS ((SELECT val FROM src WHERE val <= 2)), " +
            "cte2 AS (((SELECT val FROM src WHERE val > 2))) " +
            "SELECT count(*) FROM (SELECT * FROM cte1 UNION ALL SELECT * FROM cte2) t"));
    }

    @Test void recursive_cte_parens_on_arms() throws SQLException {
        assertEquals(5, queryInt(
            "WITH RECURSIVE cnt (n) AS (" +
            "  (SELECT 1)" +
            "  UNION ALL" +
            "  (SELECT n + 1 FROM cnt WHERE n < 5)" +
            ") SELECT count(*) FROM cnt"));
    }

    @Test void recursive_cte_double_parens_on_arms() throws SQLException {
        assertEquals(5, queryInt(
            "WITH RECURSIVE cnt (n) AS (" +
            "  ((SELECT 1))" +
            "  UNION ALL" +
            "  ((SELECT n + 1 FROM cnt WHERE n < 5))" +
            ") SELECT count(*) FROM cnt"));
    }

    // =========================================================================
    // Root cause 7: Set operation arms with double+ parens
    // =========================================================================

    @Test void union_double_parens() throws SQLException {
        assertEquals(4, queryInt(
            "SELECT count(*) FROM (" +
            "((SELECT val FROM src WHERE val <= 2)) UNION ((SELECT val FROM src WHERE val > 3))" +
            ") t"));
    }

    @Test void union_triple_parens() throws SQLException {
        assertEquals(4, queryInt(
            "SELECT count(*) FROM (" +
            "(((SELECT val FROM src WHERE val <= 2))) UNION (((SELECT val FROM src WHERE val > 3)))" +
            ") t"));
    }

    @Test void union_5_parens() throws SQLException {
        assertEquals(4, queryInt(
            "SELECT count(*) FROM (" +
            "(((((SELECT val FROM src WHERE val <= 2))))) UNION (((((SELECT val FROM src WHERE val > 3)))))" +
            ") t"));
    }

    @Test void union_all_double_parens() throws SQLException {
        assertEquals(4, queryInt(
            "SELECT count(*) FROM (" +
            "((SELECT val FROM src WHERE val <= 2)) UNION ALL ((SELECT val FROM src WHERE val > 3))" +
            ") t"));
    }

    @Test void intersect_double_parens() throws SQLException {
        assertEquals(3, queryInt(
            "SELECT count(*) FROM (" +
            "((SELECT val FROM src)) INTERSECT ((SELECT val FROM src WHERE val <= 3))" +
            ") t"));
    }

    @Test void except_double_parens() throws SQLException {
        assertEquals(2, queryInt(
            "SELECT count(*) FROM (" +
            "((SELECT val FROM src)) EXCEPT ((SELECT val FROM src WHERE val <= 3))" +
            ") t"));
    }

    @Test void chained_union_double_parens() throws SQLException {
        assertEquals(3, queryInt(
            "SELECT count(*) FROM (" +
            "((SELECT val FROM src WHERE val = 1)) UNION " +
            "((SELECT val FROM src WHERE val = 2)) UNION " +
            "((SELECT val FROM src WHERE val = 3))" +
            ") t"));
    }

    // =========================================================================
    // Root cause 8: EXPLAIN with parenthesized query
    // =========================================================================

    @Test void explain_parenthesized_select() throws SQLException {
        // EXPLAIN (SELECT ...) — PG treats this as EXPLAIN of a parenthesized query
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN (SELECT val FROM src)")) {
            assertTrue(rs.next()); // should return at least one plan row
        }
    }

    @Test void explain_double_parenthesized_select() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN ((SELECT val FROM src))")) {
            assertTrue(rs.next());
        }
    }

    // =========================================================================
    // Root cause 9: DECLARE CURSOR FOR with parens
    // =========================================================================

    @Test void declare_cursor_parenthesized() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE ptest_cur1 CURSOR FOR (SELECT val FROM src)");
            assertEquals("1", querySingle("FETCH 1 FROM ptest_cur1"));
            exec("CLOSE ptest_cur1");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test void declare_cursor_double_parens() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE ptest_cur2 CURSOR FOR ((SELECT val FROM src))");
            assertEquals("1", querySingle("FETCH 1 FROM ptest_cur2"));
            exec("CLOSE ptest_cur2");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // Root cause 10: ON CONFLICT ((id)) — parenthesized column target
    // =========================================================================

    @Test void on_conflict_double_parens_column() throws SQLException {
        exec("CREATE TABLE ptest_upsert (id int PRIMARY KEY, val int)");
        try {
            exec("INSERT INTO ptest_upsert VALUES (1, 10)");
            exec("INSERT INTO ptest_upsert VALUES (1, 20) ON CONFLICT ((id)) DO UPDATE SET val = EXCLUDED.val");
            assertEquals(20, queryInt("SELECT val FROM ptest_upsert WHERE id = 1"));
        } finally {
            exec("DROP TABLE ptest_upsert");
        }
    }

    // =========================================================================
    // Root cause 11: FROM (tablename) — PG rejects, Memgres should too
    // =========================================================================

    @Test void from_parenthesized_table_name_rejected() {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT * FROM (src) WHERE val <= 2"));
        assertEquals("42601", ex.getSQLState());
    }

    @Test void from_double_parens_table_name_rejected() {
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("SELECT * FROM ((src)) WHERE val <= 2"));
        assertEquals("42601", ex.getSQLState());
    }

    // =========================================================================
    // Regression guards: single-paren cases that already work
    // =========================================================================

    @Test void insert_select_single_parens_still_works() throws SQLException {
        exec("TRUNCATE dst");
        exec("INSERT INTO dst (val, label) (SELECT val, label FROM src WHERE id = 1)");
        assertEquals(1, queryInt("SELECT count(*) FROM dst"));
        exec("TRUNCATE dst");
    }

    @Test void exists_single_parens_still_works() throws SQLException {
        assertEquals("t", querySingle("SELECT EXISTS (SELECT 1)"));
    }

    @Test void array_subquery_single_parens_still_works() throws SQLException {
        assertEquals("{1,2,3,4,5}", querySingle("SELECT ARRAY(SELECT val FROM src ORDER BY val)"));
    }

    @Test void cte_single_parens_still_works() throws SQLException {
        assertEquals(5, queryInt("WITH cte AS (SELECT val FROM src) SELECT count(*) FROM cte"));
    }

    @Test void create_view_single_parens_still_works() throws SQLException {
        exec("CREATE VIEW ptest_v_reg AS (SELECT val FROM src)");
        try {
            assertEquals(5, queryInt("SELECT count(*) FROM ptest_v_reg"));
        } finally {
            exec("DROP VIEW ptest_v_reg");
        }
    }

    @Test void from_subquery_still_works() throws SQLException {
        assertEquals(5, queryInt("SELECT count(*) FROM (SELECT val FROM src) sub"));
    }

    @Test void from_double_parens_subquery_still_works() throws SQLException {
        assertEquals(5, queryInt("SELECT count(*) FROM ((SELECT val FROM src)) sub"));
    }

    @Test void union_single_parens_still_works() throws SQLException {
        assertEquals(4, queryInt(
            "SELECT count(*) FROM (" +
            "(SELECT val FROM src WHERE val <= 2) UNION (SELECT val FROM src WHERE val > 3)" +
            ") t"));
    }
}
