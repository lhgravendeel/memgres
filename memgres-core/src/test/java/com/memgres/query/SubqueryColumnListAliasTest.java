package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for subquery aliases with column definition lists.
 *
 * PostgreSQL allows naming the output columns of a subquery in FROM:
 *   SELECT * FROM (SELECT 1, 'a') sub(num, letter)
 *
 * This is different from table function column lists (covered in compat3)
 * because it applies to arbitrary subqueries, not just function calls.
 * It's used heavily in pg_dump output for complex views.
 *
 * Also covers deeply nested subqueries (4+ levels) which stress the
 * parser's parenthesis-tracking at high character positions.
 */
class SubqueryColumnListAliasTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE items (id serial PRIMARY KEY, name text, category text, val numeric)");
            s.execute("INSERT INTO items (name, category, val) VALUES ('x', 'a', 10), ('y', 'b', 20)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // Basic: subquery with column definition alias
    // =========================================================================

    @Test
    void testSubqueryWithColumnAlias() throws SQLException {
        assertEquals("1", query1("""
            SELECT sub.num FROM (SELECT 1, 'hello') sub(num, greeting)
        """));
    }

    @Test
    void testSubqueryWithColumnAliasAndWhere() throws SQLException {
        assertEquals("y", query1("""
            SELECT sub.item_name
            FROM (SELECT id, name, val FROM items) sub(item_id, item_name, item_val)
            WHERE sub.item_val > 15
        """));
    }

    @Test
    void testSubqueryWithManyColumnAliases() throws SQLException {
        // Mirrors pg_dump pattern: many columns renamed in alias list
        assertNotNull(query1("""
            SELECT sub.col_a, sub.col_b, sub.col_c
            FROM (SELECT 1, 'hello', true) sub(col_a, col_b, col_c)
        """));
    }

    // =========================================================================
    // Subquery with column alias inside JOINs (pg_dump pattern)
    // =========================================================================

    @Test
    void testSubqueryColumnAliasJoinedToTable() throws SQLException {
        exec("CREATE TABLE lookup_a (id serial PRIMARY KEY, label text)");
        exec("INSERT INTO lookup_a (label) VALUES ('alpha')");
        assertNotNull(query1("""
            SELECT sub.item_name, t.label
            FROM (SELECT id, name FROM items WHERE id = 1) sub(item_id, item_name)
            JOIN lookup_a t ON t.id = sub.item_id
        """));
    }

    @Test
    void testMultipleParensWithSubqueryAlias() throws SQLException {
        // Pattern: FROM (((( SELECT ... ) alias JOIN ... )))
        exec("CREATE TABLE join_target (id serial PRIMARY KEY, code text)");
        exec("INSERT INTO join_target (code) VALUES ('a')");
        assertNotNull(query1("""
            SELECT sub.item_name
            FROM ((((SELECT id, name, category FROM items) sub(item_id, item_name, item_cat)
                JOIN join_target jt ON jt.code = sub.item_cat)))
        """));
    }

    // =========================================================================
    // Deeply nested subquery aliases in JOINs (4 levels)
    // =========================================================================

    @Test
    void testFourLevelNestedJoinsWithAliases() throws SQLException {
        exec("CREATE TABLE ref_a (id serial PRIMARY KEY, val text)");
        exec("CREATE TABLE ref_b (id serial PRIMARY KEY, a_id int, info text)");
        exec("INSERT INTO ref_a (val) VALUES ('root')");
        exec("INSERT INTO ref_b (a_id, info) VALUES (1, 'detail')");
        assertNotNull(query1("""
            SELECT inner_sub.a_val, inner_sub.b_info
            FROM (((SELECT a.id, a.val, b.info
                    FROM ref_a a
                    JOIN ref_b b ON b.a_id = a.id)
                inner_sub(a_id, a_val, b_info)))
        """));
    }

    // =========================================================================
    // View using subquery column alias
    // =========================================================================

    @Test
    void testViewWithSubqueryColumnAlias() throws SQLException {
        exec("""
            CREATE VIEW aliased_sub_view AS
            SELECT sub.item_name, sub.item_val
            FROM (SELECT name, val FROM items) sub(item_name, item_val)
        """);
    }

    @Test
    void testViewWithSubqueryColumnAliasAndJoins() throws SQLException {
        exec("CREATE TABLE view_lookup (code text PRIMARY KEY, description text)");
        exec("INSERT INTO view_lookup VALUES ('a', 'Category A'), ('b', 'Category B')");
        exec("""
            CREATE VIEW enriched_items_view AS
            SELECT sub.item_name, sub.item_cat, lk.description
            FROM (SELECT name, category FROM items) sub(item_name, item_cat)
            JOIN view_lookup lk ON lk.code = sub.item_cat
        """);
    }

    // =========================================================================
    // Complex nested CASE + casts with subquery aliases (pg_dump pattern)
    // =========================================================================

    @Test
    void testDeeplyCastedExpressionInView() throws SQLException {
        // Simulates the bloat estimate view: CASE with casts at every level
        exec("""
            CREATE VIEW nested_case_view AS
            SELECT sub.item_name,
                (CASE
                    WHEN ((sub.item_val)::double precision > (0)::double precision)
                    THEN ((1)::double precision + ceil(
                        (sub.item_val)::double precision / NULLIF(
                            floor(((100)::double precision * ((4)::numeric + (sub.item_val)::numeric)::double precision)),
                            0
                        )
                    ))
                    ELSE (0)::double precision
                END)::bigint AS estimate
            FROM (SELECT name, val FROM items) sub(item_name, item_val)
        """);
    }

    // =========================================================================
    // Subquery alias with generate_series
    // =========================================================================

    @Test
    void testGenerateSeriesSubqueryAlias() throws SQLException {
        assertNotNull(query1("""
            SELECT s.pos
            FROM (SELECT generate_series(1, 5)) s(pos)
            LIMIT 1
        """));
    }

    // =========================================================================
    // Multiple subquery aliases in a single FROM clause
    // =========================================================================

    @Test
    void testMultipleSubqueryAliasesJoined() throws SQLException {
        assertNotNull(query1("""
            SELECT a.n, b.t
            FROM (SELECT 1) a(n)
            JOIN (SELECT 'hello'::text) b(t) ON true
        """));
    }

    // =========================================================================
    // Subquery alias referencing pg_catalog tables
    // =========================================================================

    @Test
    void testSubqueryAliasOnCatalogQuery() throws SQLException {
        // Pattern from views querying system catalogs
        exec("""
            CREATE VIEW catalog_sub_view AS
            SELECT sub.table_name, sub.table_schema
            FROM (SELECT tablename, schemaname FROM pg_tables) sub(table_name, table_schema)
            WHERE sub.table_schema = 'public'
        """);
    }

    // =========================================================================
    // CASE with char literal comparison inside subquery alias
    // =========================================================================

    @Test
    void testCaseWithCharLiteralInSubquery() throws SQLException {
        // Pattern: CASE strat WHEN 'l'::"char" THEN 'list' ...
        assertNotNull(query1("""
            SELECT sub.label
            FROM (
                SELECT CASE 'r'::"char"
                    WHEN 'l'::"char" THEN 'list'
                    WHEN 'r'::"char" THEN 'range'
                    WHEN 'h'::"char" THEN 'hash'
                    ELSE NULL
                END AS strat_label
            ) sub(label)
        """));
    }
}
