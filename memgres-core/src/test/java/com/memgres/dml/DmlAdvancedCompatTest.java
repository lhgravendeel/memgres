package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DML edge cases that still fail.
 *
 * v1 compat covered basic UPDATE alias, LATERAL, FILTER, ON CONFLICT.
 * These tests cover the remaining patterns:
 * - Aggregate functions with ORDER BY inside (array_agg(x ORDER BY y))
 * - JSONB_AGG with ORDER BY
 * - Nested function calls with multiple levels of parens
 * - Subquery in SET clause of UPDATE
 * - WITH ... UPDATE (CTE wrapping DML)
 * - Complex subquery expressions with pg_dump-style double-parens
 * - Block comment followed by a statement (parser losing track)
 */
class DmlAdvancedCompatTest {

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
            s.execute("CREATE TABLE dml_items (id serial PRIMARY KEY, name text, category text, value numeric, metadata jsonb DEFAULT '{}')");
            s.execute("INSERT INTO dml_items (name, category, value) VALUES ('a', 'x', 10), ('b', 'x', 20), ('c', 'y', 30)");
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
    // Aggregate functions with ORDER BY inside
    // =========================================================================

    @Test
    void testArrayAggWithOrderBy() throws SQLException {
        assertNotNull(query1("SELECT array_agg(name ORDER BY name) FROM dml_items WHERE category = 'x'"));
    }

    @Test
    void testJsonbAggWithOrderBy() throws SQLException {
        assertNotNull(query1("SELECT jsonb_agg(name ORDER BY name) FROM dml_items WHERE category = 'x'"));
    }

    @Test
    void testStringAggWithOrderBy() throws SQLException {
        assertNotNull(query1("SELECT string_agg(name, ', ' ORDER BY name) FROM dml_items WHERE category = 'x'"));
    }

    @Test
    void testJsonbObjectAggWithOrderBy() throws SQLException {
        assertNotNull(query1("SELECT jsonb_object_agg(name, value ORDER BY name) FROM dml_items WHERE category = 'x'"));
    }

    // =========================================================================
    // Nested jsonb function calls
    // =========================================================================

    @Test
    void testNestedJsonbBuildObject() throws SQLException {
        assertNotNull(query1("""
            SELECT jsonb_build_object(
                'type', 'test',
                'details', jsonb_build_object(
                    'count', (SELECT COUNT(*) FROM dml_items),
                    'total', (SELECT SUM(value) FROM dml_items)
                )
            )
        """));
    }

    @Test
    void testJsonbStripNullsWithBuildObject() throws SQLException {
        assertNotNull(query1("""
            SELECT jsonb_strip_nulls(jsonb_build_object(
                'name', 'test',
                'nullable', NULL,
                'nested', jsonb_build_object('a', 1)
            ))
        """));
    }

    // =========================================================================
    // UPDATE with complex SET using subquery and jsonb functions
    // =========================================================================

    @Test
    void testUpdateSetWithJsonbAggSubquery() throws SQLException {
        exec("CREATE TABLE config_store (id serial PRIMARY KEY, config jsonb DEFAULT '{}')");
        exec("INSERT INTO config_store (config) VALUES ('{\"items\": []}')");
        exec("""
            UPDATE config_store SET config = (
                SELECT jsonb_build_object('items', jsonb_agg(jsonb_build_object('name', name, 'value', value)))
                FROM dml_items
            )
            WHERE id = 1
        """);
        assertNotNull(query1("SELECT config FROM config_store WHERE id = 1"));
    }

    // =========================================================================
    // WITH ... UPDATE (CTE wrapping UPDATE)
    // =========================================================================

    @Test
    void testCteWrappingUpdate() throws SQLException {
        exec("CREATE TABLE cte_upd (id serial PRIMARY KEY, category text, rank_val int DEFAULT 0)");
        exec("INSERT INTO cte_upd (category) VALUES ('a'), ('a'), ('b')");
        exec("""
            WITH ranked AS (
                SELECT id, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn
                FROM cte_upd
            )
            UPDATE cte_upd SET rank_val = ranked.rn
            FROM ranked
            WHERE cte_upd.id = ranked.id
        """);
        assertNotNull(query1("SELECT rank_val FROM cte_upd WHERE id = 1"));
    }

    // =========================================================================
    // Block comment followed by statement (parser state issue)
    // =========================================================================

    @Test
    void testBlockCommentBeforeStatement() throws SQLException {
        exec("""
            /* This is a multi-line
               block comment with special chars: () {} [] */
            CREATE TABLE after_comment (id serial PRIMARY KEY, val text)
        """);
        exec("INSERT INTO after_comment (val) VALUES ('ok')");
        assertEquals("ok", query1("SELECT val FROM after_comment"));
    }

    @Test
    void testBlockCommentWithAsteriskInsideStatement() throws SQLException {
        exec("""
            CREATE TABLE bc_test (
                id serial PRIMARY KEY,
                /* this column stores the user's name */
                name text,
                /* and this one stores the age */
                age int
            )
        """);
    }

    @Test
    void testBlockCommentOnlyStatement() throws SQLException {
        // A statement that is only a block comment should be a no-op
        exec("/* nothing to do here */");
    }

    @Test
    void testBlockCommentAsEntireFile() throws SQLException {
        // Some migration files contain only a block comment (no-op)
        exec("""
            /*
             * This migration was intentionally left blank.
             * The relevant changes were applied in a previous release.
             */
        """);
    }

    // =========================================================================
    // UPDATE with jsonb_set and nested path
    // =========================================================================

    @Test
    void testUpdateJsonbSetNestedPath() throws SQLException {
        exec("CREATE TABLE json_nested (id serial PRIMARY KEY, data jsonb)");
        exec("INSERT INTO json_nested (data) VALUES ('{\"a\": {\"b\": {\"c\": 1}}}')");
        exec("UPDATE json_nested SET data = jsonb_set(data, '{a,b,c}', '2') WHERE id = 1");
        assertEquals("2", query1("SELECT data->'a'->'b'->>'c' FROM json_nested WHERE id = 1"));
    }

    // =========================================================================
    // Complex pg_dump-style double-parenthesized expressions in SELECT
    // =========================================================================

    @Test
    void testDoubleParnsCastExpression() throws SQLException {
        // pg_dump wraps everything in extra parens: ((col)::type)
        assertNotNull(query1("SELECT ((value)::double precision * (1.1)::double precision) FROM dml_items LIMIT 1"));
    }

    @Test
    void testTripleParensJoinCondition() throws SQLException {
        // pg_dump: FROM ((a JOIN b ON ((a.id = b.id))))
        exec("CREATE TABLE pp_a (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE pp_b (id serial PRIMARY KEY, a_id int)");
        exec("INSERT INTO pp_a (name) VALUES ('test')");
        exec("INSERT INTO pp_b (a_id) VALUES (1)");
        assertNotNull(query1("""
            SELECT pp_a.name FROM ((pp_a JOIN pp_b ON ((pp_a.id = pp_b.a_id))))
        """));
    }

    @Test
    void testNestedCastsInExpression() throws SQLException {
        // ((schema.nspname)::text || '.'::text || (table.relname)::text)
        assertNotNull(query1("""
            SELECT (('prefix'::text || '.'::text) || ('suffix'::text)) AS result
        """));
    }

    // =========================================================================
    // SELECT with complex FROM clause (multiple parenthesized joins)
    // =========================================================================

    @Test
    void testMultipleParenthesizedJoins() throws SQLException {
        exec("CREATE TABLE j1 (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE j2 (id serial PRIMARY KEY, j1_id int)");
        exec("CREATE TABLE j3 (id serial PRIMARY KEY, j2_id int)");
        exec("INSERT INTO j1 (name) VALUES ('root')");
        exec("INSERT INTO j2 (j1_id) VALUES (1)");
        exec("INSERT INTO j3 (j2_id) VALUES (1)");
        assertNotNull(query1("""
            SELECT j1.name
            FROM ((j1
                JOIN j2 ON ((j2.j1_id = j1.id)))
                JOIN j3 ON ((j3.j2_id = j2.id)))
        """));
    }

    // =========================================================================
    // Aggregate with FILTER + ORDER BY combined
    // =========================================================================

    @Test
    void testAggFilterAndOrderBy() throws SQLException {
        assertNotNull(query1("""
            SELECT json_object_agg(name, value ORDER BY name)
                FILTER (WHERE name IS NOT NULL)
            FROM dml_items
        """));
    }

    // =========================================================================
    // INSERT with DEFAULT VALUES and RETURNING
    // =========================================================================

    @Test
    void testInsertDefaultValuesReturning() throws SQLException {
        exec("CREATE TABLE dv_ret (id serial PRIMARY KEY, ts timestamp DEFAULT now())");
        assertNotNull(query1("INSERT INTO dv_ret DEFAULT VALUES RETURNING id, ts"));
    }

    // =========================================================================
    // Complex subquery in UPDATE SET with lateral reference
    // =========================================================================

    @Test
    void testUpdateWithCorrelatedSubqueryInSet() throws SQLException {
        exec("CREATE TABLE corr_upd (id serial PRIMARY KEY, category text, total numeric DEFAULT 0)");
        exec("INSERT INTO corr_upd (category) VALUES ('x'), ('y')");
        exec("""
            UPDATE corr_upd cu SET total = (
                SELECT COALESCE(SUM(value), 0)
                FROM dml_items i
                WHERE i.category = cu.category
            )
        """);
        assertEquals("30", query1("SELECT total FROM corr_upd WHERE category = 'x'"));
    }
}
