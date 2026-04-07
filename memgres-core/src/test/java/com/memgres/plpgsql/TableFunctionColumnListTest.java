package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for table functions with column definition lists in FROM clauses.
 *
 * PostgreSQL allows aliasing a set-returning function with an explicit column
 * list that names the output columns:
 *   SELECT f.key, f.value FROM jsonb_each(data) f(key, value)
 *   SELECT * FROM unnest(arr) WITH ORDINALITY AS t(val, ord)
 *
 * This syntax is used heavily in pg_dump output and complex views.
 * Also covers WITH ORDINALITY and multiple-paren wrapping of subqueries.
 */
class TableFunctionColumnListTest {

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
            s.execute("CREATE TABLE kv_data (id serial PRIMARY KEY, metadata jsonb DEFAULT '{}', tags text[])");
            s.execute("INSERT INTO kv_data (metadata, tags) VALUES ('{\"a\": 1, \"b\": 2}', ARRAY['x','y','z'])");
            s.execute("CREATE TABLE lookup (code text PRIMARY KEY, label text)");
            s.execute("INSERT INTO lookup VALUES ('a', 'Alpha'), ('b', 'Beta')");
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
    // Basic: function call with column definition list alias
    // =========================================================================

    @Test
    void testJsonbEachWithColumnAlias() throws SQLException {
        assertNotNull(query1("""
            SELECT f.key, f.value
            FROM jsonb_each('{"a": 1, "b": 2}'::jsonb) f(key, value)
            ORDER BY f.key LIMIT 1
        """));
    }

    @Test
    void testJsonbEachTextWithColumnAlias() throws SQLException {
        assertEquals("1", query1("""
            SELECT f.value
            FROM jsonb_each_text('{"a": "1", "b": "2"}'::jsonb) f(key, value)
            WHERE f.key = 'a'
        """));
    }

    @Test
    void testUnnestWithColumnAlias() throws SQLException {
        assertEquals("x", query1("""
            SELECT t.val
            FROM unnest(ARRAY['x','y','z']) t(val)
            LIMIT 1
        """));
    }

    @Test
    void testGenerateSeriesWithColumnAlias() throws SQLException {
        assertNotNull(query1("""
            SELECT s.n FROM generate_series(1, 5) s(n) LIMIT 1
        """));
    }

    // =========================================================================
    // Function with column list in a view definition
    // =========================================================================

    @Test
    void testViewWithJsonbEachColumnAlias() throws SQLException {
        exec("""
            CREATE VIEW expanded_kv AS
            SELECT d.id, f.key, f.value
            FROM kv_data d, jsonb_each(d.metadata) f(key, value)
        """);
    }

    @Test
    void testViewWithFunctionColumnAliasAndJoin() throws SQLException {
        // The exact pattern that fails: function alias + JOIN inside parens
        exec("""
            CREATE VIEW enriched_kv AS
            SELECT d.id, f.key, f.value, l.label
            FROM kv_data d,
                (jsonb_each(d.metadata) f(key, value)
                 LEFT JOIN lookup l ON l.code = f.key)
        """);
    }

    @Test
    void testViewWithCoalesceAndFunctionColumnAlias() throws SQLException {
        // Full pattern: COALESCE wrapping a SELECT with function + column list + JOIN
        exec("""
            CREATE VIEW agg_kv AS
            SELECT d.id,
                COALESCE((
                    SELECT jsonb_object_agg(f.key, jsonb_build_object('label', COALESCE(l.label, 'unknown')))
                    FROM (jsonb_each(d.metadata) f(key, value)
                          LEFT JOIN lookup l ON l.code = f.key)
                ), '{}'::jsonb) AS enriched_metadata
            FROM kv_data d
        """);
    }

    // =========================================================================
    // WITH ORDINALITY
    // =========================================================================

    @Test
    void testUnnestWithOrdinality() throws SQLException {
        assertNotNull(query1("""
            SELECT t.val, t.ord
            FROM unnest(ARRAY['a','b','c']) WITH ORDINALITY t(val, ord)
            ORDER BY t.ord LIMIT 1
        """));
    }

    @Test
    void testUnnestWithOrdinalityAsKeyword() throws SQLException {
        // WITH ORDINALITY using AS keyword
        assertNotNull(query1("""
            SELECT t.val, t.ord
            FROM unnest(ARRAY[10,20,30]) WITH ORDINALITY AS t(val, ord)
            ORDER BY t.ord LIMIT 1
        """));
    }

    @Test
    void testUnnestWithOrdinalityInSubquery() throws SQLException {
        assertNotNull(query1("""
            SELECT sub.val FROM (
                SELECT t.val, t.ord
                FROM unnest(ARRAY['x','y','z']) WITH ORDINALITY t(val, ord)
            ) sub
            ORDER BY sub.ord LIMIT 1
        """));
    }

    @Test
    void testUnnestWithOrdinalityJoinedToTable() throws SQLException {
        // Pattern from real schemas: unnest WITH ORDINALITY joined to pg_attribute
        exec("""
            CREATE VIEW ord_joined AS
            SELECT t.val, t.pos, d.id
            FROM kv_data d,
                unnest(d.tags) WITH ORDINALITY t(val, pos)
        """);
    }

    @Test
    void testUnnestWithOrdinalityInParenthesizedFrom() throws SQLException {
        // Parenthesized FROM with WITH ORDINALITY + JOIN
        assertNotNull(query1("""
            SELECT t.val, t.ord
            FROM (unnest(ARRAY['a','b','c']) WITH ORDINALITY t(val, ord)
                  JOIN (SELECT 'a' AS match_val) m ON m.match_val = t.val)
            LIMIT 1
        """));
    }

    @Test
    void testViewWithUnnestOrdinalityAndAggregate() throws SQLException {
        // Real pattern: array_agg(col ORDER BY ordinality) from unnest WITH ORDINALITY
        exec("""
            CREATE VIEW reordered_tags AS
            SELECT d.id,
                (SELECT array_agg(t.val ORDER BY t.ord)
                 FROM unnest(d.tags) WITH ORDINALITY t(val, ord)) AS sorted_tags
            FROM kv_data d
        """);
    }

    // =========================================================================
    // Multiple levels of parentheses wrapping subqueries
    // =========================================================================

    @Test
    void testDoubleParenSubquery() throws SQLException {
        assertNotNull(query1("""
            SELECT * FROM ((SELECT 1 AS n)) sub
        """));
    }

    @Test
    void testTripleParenSubquery() throws SQLException {
        assertNotNull(query1("""
            SELECT * FROM (((SELECT 1 AS n, 'a' AS label))) sub
        """));
    }

    @Test
    void testQuadrupleParenSubqueryWithJoin() throws SQLException {
        // pg_dump pattern: ((((SELECT ... FROM tbl) alias JOIN ... ON ...)))
        exec("CREATE TABLE mp_a (id serial PRIMARY KEY, val text)");
        exec("CREATE TABLE mp_b (id serial PRIMARY KEY, a_id int)");
        exec("INSERT INTO mp_a (val) VALUES ('test')");
        exec("INSERT INTO mp_b (a_id) VALUES (1)");
        assertNotNull(query1("""
            SELECT sub.val FROM (((
                SELECT a.val FROM mp_a a JOIN mp_b b ON b.a_id = a.id
            ))) sub
        """));
    }

    @Test
    void testViewWithMultiParenSubquery() throws SQLException {
        exec("""
            CREATE VIEW multi_paren_view AS
            SELECT sub.id, sub.val
            FROM (((
                SELECT id, metadata->>'a' AS val
                FROM kv_data
            ))) sub
        """);
    }

    // =========================================================================
    // CROSS JOIN LATERAL with WITH ORDINALITY (combined pattern)
    // =========================================================================

    @Test
    void testCrossJoinLateralWithOrdinality() throws SQLException {
        assertNotNull(query1("""
            SELECT d.id, t.val, t.ord
            FROM kv_data d
            CROSS JOIN LATERAL unnest(d.tags) WITH ORDINALITY t(val, ord)
            LIMIT 1
        """));
    }

    @Test
    void testLeftJoinLateralSubqueryWithLimit() throws SQLException {
        // Pattern: LEFT JOIN LATERAL (SELECT ... LIMIT 1) alias ON true
        assertNotNull(query1("""
            SELECT d.id, top_tag.val
            FROM kv_data d
            LEFT JOIN LATERAL (
                SELECT t.val
                FROM unnest(d.tags) t(val)
                ORDER BY t.val
                LIMIT 1
            ) top_tag ON true
        """));
    }

    // =========================================================================
    // jsonb_each in a FROM clause without explicit column list (baseline)
    // =========================================================================

    @Test
    void testJsonbEachWithoutColumnList() throws SQLException {
        // This should already work; baseline test
        assertNotNull(query1("""
            SELECT key, value FROM jsonb_each('{"a": 1}'::jsonb) LIMIT 1
        """));
    }

    // =========================================================================
    // regexp_matches with column alias
    // =========================================================================

    @Test
    void testRegexpMatchesWithColumnAlias() throws SQLException {
        assertNotNull(query1("""
            SELECT m.match[1]
            FROM regexp_matches('hello world 123', '(\\d+)') m(match)
        """));
    }
}
