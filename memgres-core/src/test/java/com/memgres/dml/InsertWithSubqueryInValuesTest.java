package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for INSERT with subqueries in VALUES clause and CTE-INSERT patterns.
 *
 * Covers:
 * - INSERT with subquery as a value expression: VALUES ((SELECT id FROM ...))
 * - WITH ... INSERT (CTE wrapping INSERT with complex value expressions)
 * - INSERT with very long value lists (parser state at high positions)
 * - INSERT ... ON CONFLICT with complex WHERE clauses
 */
class InsertWithSubqueryInValuesTest {

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
            s.execute("CREATE TABLE apps (id serial PRIMARY KEY, workspace_id text, path text, name text)");
            s.execute("INSERT INTO apps (workspace_id, path, name) VALUES ('ws1', '/app1', 'MyApp')");
            s.execute("CREATE TABLE app_versions (id serial PRIMARY KEY, app_id int REFERENCES apps(id), created_by text, created_at timestamp DEFAULT now(), value jsonb)");
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
    // INSERT with subquery in VALUES
    // =========================================================================

    @Test
    void testInsertWithSubqueryValue() throws SQLException {
        exec("""
            INSERT INTO app_versions (app_id, created_by, value)
            VALUES (
                (SELECT id FROM apps WHERE workspace_id = 'ws1' AND path = '/app1'),
                'system',
                '{"version": "1.0"}'::jsonb
            )
        """);
        assertEquals("1", query1("SELECT app_id FROM app_versions WHERE created_by = 'system'"));
    }

    @Test
    void testInsertWithMultipleSubqueryValues() throws SQLException {
        exec("CREATE TABLE sub_vals (id serial PRIMARY KEY, ref_id int, ref_name text)");
        exec("CREATE TABLE ref_source (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO ref_source (name) VALUES ('source_a')");
        exec("""
            INSERT INTO sub_vals (ref_id, ref_name) VALUES (
                (SELECT id FROM ref_source WHERE name = 'source_a'),
                (SELECT name FROM ref_source WHERE id = 1)
            )
        """);
        assertEquals("source_a", query1("SELECT ref_name FROM sub_vals"));
    }

    // =========================================================================
    // WITH ... INSERT (CTE wrapping INSERT)
    // =========================================================================

    @Test
    void testCteInsertWithSubqueryInValues() throws SQLException {
        exec("""
            WITH target_app AS (
                SELECT id FROM apps WHERE workspace_id = 'ws1' AND path = '/app1'
            )
            INSERT INTO app_versions (app_id, created_by, value)
            SELECT id, 'deployer', '{"version": "2.0"}'::jsonb
            FROM target_app
        """);
    }

    @Test
    void testCteInsertWithComplexSelect() throws SQLException {
        exec("CREATE TABLE summary_log (category text, total_count bigint, recorded_at timestamp DEFAULT now())");
        exec("CREATE TABLE raw_events (id serial PRIMARY KEY, category text, event_time timestamp DEFAULT now())");
        exec("INSERT INTO raw_events (category) VALUES ('click'), ('click'), ('view')");
        exec("""
            WITH aggregated AS (
                SELECT category, COUNT(*) AS cnt
                FROM raw_events
                GROUP BY category
            )
            INSERT INTO summary_log (category, total_count)
            SELECT category, cnt FROM aggregated
        """);
        assertEquals("2", query1("SELECT total_count FROM summary_log WHERE category = 'click'"));
    }

    // =========================================================================
    // INSERT ... ON CONFLICT with complex expressions
    // =========================================================================

    @Test
    void testOnConflictDoUpdateWithSubquery() throws SQLException {
        exec("CREATE TABLE upsert_complex (id int PRIMARY KEY, val text, counter int DEFAULT 0)");
        exec("INSERT INTO upsert_complex VALUES (1, 'first', 1)");
        exec("""
            INSERT INTO upsert_complex (id, val, counter)
            VALUES (1, 'updated', 1)
            ON CONFLICT (id) DO UPDATE
            SET val = EXCLUDED.val,
                counter = upsert_complex.counter + EXCLUDED.counter
        """);
        assertEquals("2", query1("SELECT counter FROM upsert_complex WHERE id = 1"));
    }

    @Test
    void testOnConflictDoNothingWithSubqueryValues() throws SQLException {
        exec("CREATE TABLE upsert_sub (id int PRIMARY KEY, name text)");
        exec("INSERT INTO upsert_sub VALUES (1, 'existing')");
        exec("""
            INSERT INTO upsert_sub (id, name)
            VALUES ((SELECT MAX(id) FROM upsert_sub), 'conflict')
            ON CONFLICT DO NOTHING
        """);
        assertEquals("existing", query1("SELECT name FROM upsert_sub WHERE id = 1"));
    }

    // =========================================================================
    // Very long INSERT with many columns (parser state at high positions)
    // =========================================================================

    @Test
    void testLongInsertManyColumns() throws SQLException {
        exec("""
            CREATE TABLE wide_table (
                id serial PRIMARY KEY,
                col_a text, col_b text, col_c text, col_d text, col_e text,
                col_f text, col_g text, col_h text, col_i text, col_j text,
                col_k int, col_l int, col_m int, col_n int, col_o int,
                config jsonb DEFAULT '{}', tags text[] DEFAULT '{}',
                created_at timestamp DEFAULT now(), updated_at timestamp DEFAULT now()
            )
        """);
        exec("""
            INSERT INTO wide_table (col_a, col_b, col_c, col_d, col_e, col_f, col_g, col_h, col_i, col_j, col_k, col_l, col_m, col_n, col_o, config, tags)
            VALUES ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 1, 2, 3, 4, 5, '{"key": "val"}', ARRAY['tag1', 'tag2'])
        """);
        assertEquals("a", query1("SELECT col_a FROM wide_table WHERE col_k = 1"));
    }

    // =========================================================================
    // INSERT with RETURNING and complex expressions
    // =========================================================================

    @Test
    void testInsertReturningComputedExpression() throws SQLException {
        exec("CREATE TABLE ret_computed (id serial PRIMARY KEY, a int, b int)");
        String result = query1("INSERT INTO ret_computed (a, b) VALUES (3, 7) RETURNING a + b AS sum");
        assertEquals("10", result);
    }

    @Test
    void testInsertReturningWithCoalesce() throws SQLException {
        exec("CREATE TABLE ret_coalesce (id serial PRIMARY KEY, name text, display_name text)");
        String result = query1("""
            INSERT INTO ret_coalesce (name)
            VALUES ('test')
            RETURNING COALESCE(display_name, name) AS effective_name
        """);
        assertEquals("test", result);
    }
}
