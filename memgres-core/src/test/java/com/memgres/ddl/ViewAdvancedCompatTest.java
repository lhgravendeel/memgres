package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for advanced CREATE VIEW patterns that still fail.
 *
 * v1 compat covered basic CTEs, LATERALs, and parenthesized JOINs.
 * These tests cover the remaining edge cases:
 * - Views with deeply nested subqueries in FROM clause
 * - Views with regex operators (~, ~*, ~~*)
 * - Views with array_agg / unnest in subqueries
 * - Views with complex CASE + cast chains (pg_dump output)
 * - Views with multiple levels of subquery nesting
 * - Views using catalog functions that return TABLE
 * - Views with ORDER BY inside aggregate functions
 * - Views with pg_dump-style double-parenthesized casts: ((col)::type)
 */
class ViewAdvancedCompatTest {

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
            s.execute("CREATE TABLE items (id serial PRIMARY KEY, name text, category text, status text, value numeric, tags text[], metadata jsonb, created_at timestamp DEFAULT now())");
            s.execute("CREATE TABLE categories (id serial PRIMARY KEY, name text, parent_id int)");
            s.execute("CREATE TABLE settings (key text PRIMARY KEY, value text)");
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

    // =========================================================================
    // Views with regex match operators (~, ~*, ~~, ~~*)
    // =========================================================================

    @Test
    void testViewWithRegexMatch() throws SQLException {
        exec("""
            CREATE VIEW pattern_matches AS
            SELECT id, name
            FROM items
            WHERE name ~ '^[A-Z].*'
        """);
    }

    @Test
    void testViewWithRegexMatchCaseInsensitive() throws SQLException {
        exec("""
            CREATE VIEW ci_pattern_matches AS
            SELECT id, name
            FROM items
            WHERE name ~* '^prefix.*suffix$'
        """);
    }

    @Test
    void testViewWithIlikeOperator() throws SQLException {
        // ~~* is the operator form of ILIKE
        exec("""
            CREATE VIEW ilike_view AS
            SELECT id, name
            FROM items
            WHERE name ~~* '%search%'
        """);
    }

    @Test
    void testViewWithNotRegexMatch() throws SQLException {
        exec("""
            CREATE VIEW not_matching AS
            SELECT id, name
            FROM items
            WHERE name !~* 'excluded_pattern'
        """);
    }

    // =========================================================================
    // Views with regexp_matches / regexp_replace
    // =========================================================================

    @Test
    void testViewWithRegexpMatches() throws SQLException {
        exec("""
            CREATE VIEW extracted_parts AS
            SELECT id, name,
                regexp_matches(name, '(\\w+)\\.(\\w+)') AS parts
            FROM items
        """);
    }

    // =========================================================================
    // Views with deeply nested subqueries in FROM clause
    // =========================================================================

    @Test
    void testViewWithNestedSubqueriesInFrom() throws SQLException {
        exec("""
            CREATE VIEW nested_stats AS
            SELECT agg.category, agg.total, agg.avg_value
            FROM (
                SELECT category,
                    COUNT(*) AS total,
                    AVG(value) AS avg_value
                FROM items
                GROUP BY category
            ) agg
            WHERE agg.total > 0
        """);
    }

    @Test
    void testViewWithThreeLevelNesting() throws SQLException {
        exec("""
            CREATE VIEW deep_nesting AS
            SELECT final.name, final.computed
            FROM (
                SELECT mid.name, mid.total * 2 AS computed
                FROM (
                    SELECT category AS name, COALESCE(SUM(value), 0) AS total
                    FROM items
                    GROUP BY category
                ) mid
            ) final
            WHERE final.computed > 0
        """);
    }

    // =========================================================================
    // Views with complex CASE + cast chains (pg_dump-style)
    // =========================================================================

    @Test
    void testViewWithCaseAndCast() throws SQLException {
        // pg_dump wraps everything in extra parens and casts: ((val)::double precision > 0)
        exec("""
            CREATE VIEW cast_chain AS
            SELECT id,
                (CASE
                    WHEN ((value)::double precision > (0)::double precision) THEN ((value)::double precision * (1.1)::double precision)
                    ELSE (0)::double precision
                END)::bigint AS adjusted_value
            FROM items
        """);
    }

    @Test
    void testViewWithPgDumpStyleCasts() throws SQLException {
        // Pattern: ((nspname)::text || '.'::text || (tblname)::text)
        exec("""
            CREATE VIEW qualified_names AS
            SELECT id,
                (((category)::text || '.'::text) || (name)::text) AS qualified_name
            FROM items
        """);
    }

    @Test
    void testViewWithNestedCaseAndCoalesce() throws SQLException {
        exec("""
            CREATE VIEW complex_calc AS
            SELECT id,
                COALESCE(
                    (1::double precision + ceil(
                        (value::double precision / NULLIF(
                            floor(((100::double precision * (4::numeric + value)::double precision))),
                            0
                        ))
                    )),
                    0::double precision
                ) AS estimate
            FROM items
        """);
    }

    // =========================================================================
    // Views with array_agg in subquery + ORDER BY inside aggregate
    // =========================================================================

    @Test
    void testViewWithArrayAggOrderBy() throws SQLException {
        exec("""
            CREATE VIEW ordered_tags AS
            SELECT category,
                array_agg(name ORDER BY name) AS sorted_names
            FROM items
            GROUP BY category
        """);
    }

    @Test
    void testViewWithArrayAggInSubquery() throws SQLException {
        exec("""
            CREATE VIEW subq_agg AS
            SELECT c.id, c.name,
                (SELECT array_agg(i.name ORDER BY i.name)
                 FROM items i
                 WHERE i.category = c.name) AS item_names
            FROM categories c
        """);
    }

    // =========================================================================
    // Views with aggregate FILTER inside subquery
    // =========================================================================

    @Test
    void testViewWithFilterInSubquery() throws SQLException {
        exec("""
            CREATE VIEW filtered_agg AS
            SELECT c.name AS category,
                (SELECT COUNT(*) FILTER (WHERE i.status = 'active')
                 FROM items i WHERE i.category = c.name) AS active_count,
                (SELECT COUNT(*) FILTER (WHERE i.status = 'archived')
                 FROM items i WHERE i.category = c.name) AS archived_count
            FROM categories c
        """);
    }

    // =========================================================================
    // Views with json_object_agg + ORDER BY + FILTER
    // =========================================================================

    @Test
    void testViewWithJsonObjectAggFilterOrderBy() throws SQLException {
        // Pattern: json_object_agg(key, val ORDER BY key) FILTER (WHERE key IS NOT NULL)
        exec("""
            CREATE VIEW config_map AS
            SELECT category,
                json_object_agg(name, COALESCE(value, 0) ORDER BY name)
                    FILTER (WHERE name IS NOT NULL) AS config
            FROM items
            GROUP BY category
        """);
    }

    // =========================================================================
    // Views with COALESCE on array columns
    // =========================================================================

    @Test
    void testViewWithCoalesceArray() throws SQLException {
        exec("""
            CREATE VIEW array_defaults AS
            SELECT id, name,
                COALESCE(tags, '{}'::text[]) AS safe_tags
            FROM items
        """);
    }

    // =========================================================================
    // Views with string_agg
    // =========================================================================

    @Test
    void testViewWithStringAgg() throws SQLException {
        exec("""
            CREATE VIEW csv_items AS
            SELECT category,
                string_agg(name, ', ' ORDER BY name) AS item_list
            FROM items
            GROUP BY category
        """);
    }

    // =========================================================================
    // Views with LATERAL in subquery
    // =========================================================================

    @Test
    void testViewWithLateralSubquery() throws SQLException {
        exec("""
            CREATE VIEW top_per_category AS
            SELECT c.name AS category, top_item.name AS top_item, top_item.value
            FROM categories c
            LEFT JOIN LATERAL (
                SELECT i.name, i.value
                FROM items i
                WHERE i.category = c.name
                ORDER BY i.value DESC NULLS LAST
                LIMIT 1
            ) top_item ON true
        """);
    }

    // =========================================================================
    // Views with DISTINCT ON
    // =========================================================================

    @Test
    void testViewWithDistinctOn() throws SQLException {
        exec("""
            CREATE VIEW latest_per_category AS
            SELECT DISTINCT ON (category)
                id, name, category, created_at
            FROM items
            ORDER BY category, created_at DESC
        """);
    }

    // =========================================================================
    // Views referencing pg_catalog tables (system catalog queries)
    // =========================================================================

    @Test
    void testViewOnPgCatalog() throws SQLException {
        exec("""
            CREATE VIEW table_sizes AS
            SELECT
                pg_class.relname AS table_name,
                pg_class.reltuples AS row_estimate
            FROM pg_catalog.pg_class
            JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE pg_namespace.nspname = 'public'
            AND pg_class.relkind = 'r'
        """);
    }

    // =========================================================================
    // Views with subquery in CASE expression
    // =========================================================================

    @Test
    void testViewWithSubqueryInCase() throws SQLException {
        exec("""
            CREATE VIEW enriched_items AS
            SELECT id, name,
                CASE
                    WHEN (SELECT COUNT(*) FROM items sub WHERE sub.category = items.category) > 5
                    THEN 'popular'
                    ELSE 'niche'
                END AS popularity
            FROM items
        """);
    }

    // =========================================================================
    // Views with jsonb operators in SELECT
    // =========================================================================

    @Test
    void testViewWithJsonbOperators() throws SQLException {
        exec("""
            CREATE VIEW json_extract AS
            SELECT id, name,
                metadata->>'type' AS item_type,
                metadata->'details' AS details_json,
                (metadata->>'score')::int AS score
            FROM items
            WHERE metadata IS NOT NULL
        """);
    }

    // =========================================================================
    // CREATE OR REPLACE VIEW preserving existing
    // =========================================================================

    @Test
    void testCreateOrReplaceViewAddColumn() throws SQLException {
        exec("CREATE VIEW evolving_view AS SELECT id, name FROM items");
        // Replace with more columns; must be superset of original
        exec("CREATE OR REPLACE VIEW evolving_view AS SELECT id, name, category, value FROM items");
    }
}
