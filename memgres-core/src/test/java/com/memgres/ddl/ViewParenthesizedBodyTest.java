package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE VIEW with parenthesized SELECT body.
 *
 * PostgreSQL allows:   CREATE VIEW v AS (SELECT ...)
 * as well as:          CREATE VIEW v AS SELECT ...
 *
 * Some tools generate the parenthesized form: CREATE VIEW v AS (SELECT ...).
 * Also covers CREATE MATERIALIZED VIEW IF NOT EXISTS which has the
 * same parser path issue with IF NOT EXISTS before AS.
 */
class ViewParenthesizedBodyTest {

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
            s.execute("CREATE TABLE base_data (id serial PRIMARY KEY, name text, category text, val int, created_at timestamp DEFAULT now())");
            s.execute("INSERT INTO base_data (name, category, val) VALUES ('a', 'x', 10), ('b', 'x', 20), ('c', 'y', 30)");
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
    // CREATE VIEW AS (SELECT ...): parenthesized body
    // =========================================================================

    @Test
    void testViewAsParenthesizedSelect() throws SQLException {
        exec("CREATE VIEW paren_view1 AS (SELECT id, name FROM base_data)");
        assertEquals("a", query1("SELECT name FROM paren_view1 ORDER BY id LIMIT 1"));
    }

    @Test
    void testViewAsParenthesizedSelectWithWhere() throws SQLException {
        exec("CREATE VIEW paren_view2 AS (SELECT id, name, val FROM base_data WHERE category = 'x')");
        assertEquals("2", query1("SELECT COUNT(*) FROM paren_view2"));
    }

    @Test
    void testViewAsParenthesizedSelectWithJoin() throws SQLException {
        exec("CREATE TABLE paren_categories (id serial PRIMARY KEY, label text)");
        exec("INSERT INTO paren_categories (label) VALUES ('x'), ('y')");
        exec("""
            CREATE VIEW paren_view3 AS (
                SELECT d.id, d.name, c.label
                FROM base_data d
                JOIN paren_categories c ON c.label = d.category
            )
        """);
    }

    @Test
    void testViewAsParenthesizedSelectWithGroupBy() throws SQLException {
        exec("""
            CREATE VIEW paren_agg AS (
                SELECT category, COUNT(*) AS cnt, SUM(val) AS total
                FROM base_data
                GROUP BY category
            )
        """);
    }

    // =========================================================================
    // CREATE OR REPLACE VIEW AS (SELECT ...)
    // =========================================================================

    @Test
    void testCreateOrReplaceViewParenthesized() throws SQLException {
        exec("CREATE VIEW repl_paren AS (SELECT id, name FROM base_data)");
        exec("CREATE OR REPLACE VIEW repl_paren AS (SELECT id, name, val FROM base_data)");
    }

    @Test
    void testCreateOrReplaceViewParenthesizedWithAliases() throws SQLException {
        exec("""
            CREATE OR REPLACE VIEW alias_paren AS (
                SELECT id,
                    name AS display_name,
                    val AS amount,
                    category || '_' || name AS qualified_name
                FROM base_data
            )
        """);
    }

    // =========================================================================
    // CREATE MATERIALIZED VIEW IF NOT EXISTS
    // =========================================================================

    @Test
    void testMaterializedViewIfNotExists() throws SQLException {
        exec("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mat_stats AS
            SELECT category, COUNT(*) AS cnt FROM base_data GROUP BY category
        """);
    }

    @Test
    void testMaterializedViewIfNotExistsIdempotent() throws SQLException {
        exec("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mat_idempotent AS
            SELECT COUNT(*) AS total FROM base_data
        """);
        // Second call should not error
        exec("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mat_idempotent AS
            SELECT COUNT(*) AS total FROM base_data
        """);
    }

    @Test
    void testMaterializedViewIfNotExistsWithTimestampCast() throws SQLException {
        exec("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mat_ts AS
            SELECT id, created_at::date AS day, COUNT(*) AS cnt
            FROM base_data
            GROUP BY id, created_at::date
        """);
    }

    @Test
    void testMaterializedViewIfNotExistsLowercaseAs() throws SQLException {
        // Some codebases use lowercase "as" after IF NOT EXISTS
        exec("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS mat_lower as
            SELECT COUNT(*) as total, COALESCE(SUM(val), 0) as sum_val
            FROM base_data
        """);
    }

    // =========================================================================
    // Views with complex subqueries that were failing (nested parens)
    // =========================================================================

    @Test
    void testViewWithSubqueryInSelectList() throws SQLException {
        exec("""
            CREATE VIEW sub_select_view AS
            SELECT d.id, d.name,
                (SELECT array_agg(b.name) FROM base_data b WHERE b.category = d.category) AS siblings
            FROM base_data d
        """);
    }

    @Test
    void testViewWithSubqueryInFrom() throws SQLException {
        exec("""
            CREATE VIEW sub_from_view AS
            SELECT stats.category, stats.total
            FROM (
                SELECT category, COUNT(*) AS total
                FROM base_data
                GROUP BY category
            ) stats
            WHERE stats.total > 0
        """);
    }

    @Test
    void testViewWithNestedCaseAndCasts() throws SQLException {
        // pg_dump pattern with triple-nested parens and casts
        exec("""
            CREATE VIEW complex_calc_view AS
            SELECT id,
                (CASE
                    WHEN ((val)::double precision > (0)::double precision)
                    THEN (((val)::double precision * (1.1)::double precision))::bigint
                    ELSE (0)::bigint
                END) AS adjusted_val
            FROM base_data
        """);
    }

    @Test
    void testViewWithCoalesceAndConcatCasts() throws SQLException {
        // pg_dump pattern: (((col)::text || '.'::text) || (col2)::text)
        exec("""
            CREATE VIEW concat_cast_view AS
            SELECT id,
                (((category)::text || '.'::text) || (name)::text) AS qualified
            FROM base_data
        """);
    }

    // =========================================================================
    // Views using regex operators
    // =========================================================================

    @Test
    void testViewWithRegexTildeStarOperator() throws SQLException {
        exec("""
            CREATE VIEW regex_view AS
            SELECT id, name
            FROM base_data
            WHERE name ~* '^[a-z]'
        """);
    }

    @Test
    void testViewWithIlikeOperatorForm() throws SQLException {
        // ~~* is the operator form of ILIKE
        exec("""
            CREATE VIEW ilike_op_view AS
            SELECT id, name
            FROM base_data
            WHERE name ~~* '%a%'
        """);
    }

    @Test
    void testViewWithNotIlikeOperatorForm() throws SQLException {
        // !~~* is the operator form of NOT ILIKE
        exec("""
            CREATE VIEW not_ilike_view AS
            SELECT id, name
            FROM base_data
            WHERE name !~~* '%z%'
        """);
    }

    @Test
    void testViewWithLikeOperatorForm() throws SQLException {
        // ~~ is the operator form of LIKE
        exec("""
            CREATE VIEW like_op_view AS
            SELECT id, name
            FROM base_data
            WHERE name ~~ '%a%'
        """);
    }

    // =========================================================================
    // View with array_agg + ORDER BY inside subquery with WITH
    // =========================================================================

    @Test
    void testViewWithArrayAggInSubqueryWithWhere() throws SQLException {
        exec("""
            CREATE VIEW agg_sub_view AS
            SELECT category,
                (SELECT array_agg(name ORDER BY name)
                 FROM base_data sub
                 WHERE sub.category = base_data.category
                 AND sub.val > 0) AS sorted_names
            FROM base_data
            GROUP BY category
        """);
    }
}
