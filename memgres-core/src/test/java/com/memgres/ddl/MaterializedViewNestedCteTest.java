package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for materialized views with nested CTEs and complex aggregate patterns.
 *
 * Covers:
 * - CTE (WITH) inside a subquery in FROM clause of another CTE
 * - ROW_TO_JSON(alias) where alias is a subquery name matching keyword ROW
 * - JSON_AGG(ROW_TO_JSON(sub)) pattern
 * - JSON_BUILD_OBJECT with COALESCE wrapping CTE references
 * - Nested WITH inside FROM subquery (PG allows WITH in subqueries)
 */
class MaterializedViewNestedCteTest {

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
            s.execute("CREATE TABLE events (id serial PRIMARY KEY, event_type text, created_at timestamp DEFAULT now())");
            s.execute("CREATE TABLE page_hits (id serial PRIMARY KEY, url text, created_at timestamp DEFAULT now())");
            s.execute("CREATE TABLE items (id serial PRIMARY KEY, name text)");
            s.execute("INSERT INTO events (event_type) VALUES ('click'), ('view'), ('click')");
            s.execute("INSERT INTO page_hits (url) VALUES ('/home'), ('/about')");
            s.execute("INSERT INTO items (name) VALUES ('Widget'), ('Gadget')");
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
    // ROW_TO_JSON with subquery alias named 'row'
    // =========================================================================

    @Test
    void testRowToJsonWithRowAlias() throws SQLException {
        // The exact failing pattern: ROW_TO_JSON(row) FROM (SELECT ...) row
        assertNotNull(query1("""
            SELECT JSON_AGG(ROW_TO_JSON(row))
            FROM (SELECT id, event_type FROM events LIMIT 2) row
        """));
    }

    @Test
    void testRowToJsonWithDifferentAlias() throws SQLException {
        // Baseline: same query with non-keyword alias; should work
        assertNotNull(query1("""
            SELECT JSON_AGG(ROW_TO_JSON(r))
            FROM (SELECT id, event_type FROM events LIMIT 2) r
        """));
    }

    @Test
    void testRowToJsonWithRowAliasInView() throws SQLException {
        exec("""
            CREATE VIEW json_events AS
            SELECT JSON_AGG(ROW_TO_JSON(row))
            FROM (SELECT id, event_type, created_at FROM events) row
        """);
    }

    // =========================================================================
    // Nested WITH inside FROM subquery
    // =========================================================================

    @Test
    void testWithInsideFromSubquery() throws SQLException {
        // PG allows WITH in subqueries: SELECT ... FROM (WITH cte AS (...) SELECT ... FROM cte) sub
        assertNotNull(query1("""
            SELECT sub.cnt
            FROM (
                WITH recent AS (
                    SELECT id FROM events ORDER BY id DESC LIMIT 5
                )
                SELECT COUNT(*) AS cnt FROM recent
            ) sub
        """));
    }

    @Test
    void testNestedWithInMultipleCtes() throws SQLException {
        // Multiple top-level CTEs, each with a nested WITH in FROM subquery
        assertNotNull(query1("""
            WITH event_data AS (
                SELECT JSON_AGG(ROW_TO_JSON(row))
                FROM (
                    WITH dates AS (
                        SELECT created_at::date AS event_date
                        FROM events ORDER BY id DESC LIMIT 1
                    )
                    SELECT COUNT(*) AS count, created_at::date AS date
                    FROM events
                    WHERE created_at::date >= (SELECT event_date FROM dates)
                    GROUP BY date
                ) row
            ),
            hit_data AS (
                SELECT JSON_AGG(ROW_TO_JSON(row))
                FROM (
                    WITH dates AS (
                        SELECT created_at::date AS hit_date
                        FROM page_hits ORDER BY id DESC LIMIT 1
                    )
                    SELECT COUNT(*) AS count, created_at::date AS date
                    FROM page_hits
                    WHERE created_at::date >= (SELECT hit_date FROM dates)
                    GROUP BY date
                ) row
            )
            SELECT JSON_BUILD_OBJECT(
                'events', COALESCE((SELECT * FROM event_data), '[]'),
                'hits', COALESCE((SELECT * FROM hit_data), '[]')
            )
        """));
    }

    // =========================================================================
    // MATERIALIZED VIEW with nested CTE
    // =========================================================================

    @Test
    void testMaterializedViewWithNestedCte() throws SQLException {
        exec("""
            CREATE MATERIALIZED VIEW dashboard_data AS
            WITH summary AS (
                SELECT JSON_AGG(ROW_TO_JSON(row))
                FROM (
                    SELECT event_type, COUNT(*) AS cnt
                    FROM events
                    GROUP BY event_type
                    ORDER BY cnt DESC
                ) row
            )
            SELECT NOW() AS updated_at,
                JSON_BUILD_OBJECT('summary', COALESCE((SELECT * FROM summary), '[]'))
        """);
    }

    @Test
    void testMaterializedViewWithMultipleNestedCtes() throws SQLException {
        exec("""
            CREATE MATERIALIZED VIEW multi_nested_mv AS
            WITH stat_a AS (
                SELECT JSON_AGG(ROW_TO_JSON(row))
                FROM (
                    WITH recent AS (
                        SELECT created_at::date AS recent_date
                        FROM events ORDER BY id DESC LIMIT 1
                    )
                    SELECT COUNT(*) AS count, created_at::date AS date
                    FROM events
                    WHERE created_at >= (SELECT recent_date FROM recent)
                    GROUP BY date ORDER BY date
                ) row
            ),
            stat_b AS (
                SELECT JSON_AGG(ROW_TO_JSON(row))
                FROM (
                    SELECT url, COUNT(*) AS count
                    FROM page_hits
                    GROUP BY url ORDER BY count DESC
                ) row
            )
            SELECT NOW() AS updated_at,
                JSON_BUILD_OBJECT(
                    'events', COALESCE((SELECT * FROM stat_a), '[]'),
                    'hits', COALESCE((SELECT * FROM stat_b), '[]')
                )
        """);
    }

    // =========================================================================
    // INTERVAL arithmetic inside nested CTE
    // =========================================================================

    @Test
    void testNestedCteWithIntervalArithmetic() throws SQLException {
        assertNotNull(query1("""
            SELECT sub.result
            FROM (
                WITH date_range AS (
                    SELECT created_at::date AS end_date,
                           created_at::date - INTERVAL '30 DAY' AS start_date
                    FROM events ORDER BY id DESC LIMIT 1
                )
                SELECT COUNT(*) AS result
                FROM events
                WHERE created_at >= (SELECT start_date FROM date_range)
                  AND created_at < (SELECT end_date FROM date_range) + INTERVAL '1 day'
            ) sub
        """));
    }

    // =========================================================================
    // JSON_BUILD_OBJECT with COALESCE on CTE references
    // =========================================================================

    @Test
    void testJsonBuildObjectWithCoalesceCte() throws SQLException {
        assertNotNull(query1("""
            WITH data AS (
                SELECT json_agg(row_to_json(r)) AS payload
                FROM (SELECT 1 AS id, 'test' AS name) r
            )
            SELECT JSON_BUILD_OBJECT(
                'items', COALESCE((SELECT payload FROM data), '[]'::json)
            )
        """));
    }

    // =========================================================================
    // Baseline: simple ROW_TO_JSON without keyword alias
    // =========================================================================

    @Test
    void testSimpleRowToJson() throws SQLException {
        assertNotNull(query1("SELECT ROW_TO_JSON(ROW(1, 'hello', true))"));
    }

    @Test
    void testRowToJsonWithNamedRecord() throws SQLException {
        assertNotNull(query1("""
            SELECT ROW_TO_JSON(t)
            FROM (SELECT 1 AS id, 'test' AS name) t
        """));
    }

    // =========================================================================
    // SELECT * FROM cte_name (used inside COALESCE)
    // =========================================================================

    @Test
    void testSelectStarFromCteInCoalesce() throws SQLException {
        // Pattern: COALESCE((SELECT * FROM cte), '[]')
        assertNotNull(query1("""
            WITH items_json AS (
                SELECT json_agg(json_build_object('id', id, 'name', name)) AS data
                FROM items
            )
            SELECT COALESCE((SELECT data FROM items_json), '[]'::json)::text
        """));
    }

    // =========================================================================
    // Nested CTE with GROUP BY and ORDER BY
    // =========================================================================

    @Test
    void testNestedCteWithGroupByOrderBy() throws SQLException {
        assertNotNull(query1("""
            WITH aggregated AS (
                SELECT JSON_AGG(ROW_TO_JSON(row))
                FROM (
                    SELECT event_type, COUNT(*) AS cnt
                    FROM events
                    GROUP BY event_type
                    ORDER BY cnt DESC
                ) row
            )
            SELECT COALESCE((SELECT * FROM aggregated), '[]'::json)::text
        """));
    }
}
