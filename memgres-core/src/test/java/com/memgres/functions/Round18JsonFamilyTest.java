package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category W: JSON / JSONB function family siblings.
 *
 * Covers:
 *  - json_populate_record / jsonb_populate_record (R16 touched jsonb only)
 *  - jsonb_to_record typed columns
 *  - record_to_json / record_to_jsonb
 *  - row_to_json 2-arg (pretty)
 *  - jsonb_set create_missing semantics
 *  - json_strip_nulls / jsonb_strip_nulls
 *  - jsonb - text[] operator
 *  - jsonb_path_match / jsonb_path_query_first
 */
class Round18JsonFamilyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // W1. json_populate_record
    // =========================================================================

    @Test
    void json_populate_record_returns_typed_composite() throws SQLException {
        exec("DROP TYPE IF EXISTS r18_jpr CASCADE");
        exec("CREATE TYPE r18_jpr AS (a int, b text)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a, b FROM json_populate_record(null::r18_jpr, '{\"a\":7,\"b\":\"x\"}'::json)")) {
            assertTrue(rs.next(), "json_populate_record must yield a row");
            assertEquals(7, rs.getInt(1), "a must be 7");
            assertEquals("x", rs.getString(2), "b must be 'x'");
        }
    }

    // =========================================================================
    // W2. jsonb_to_record returns typed columns
    // =========================================================================

    @Test
    void jsonb_to_record_integer_column_typed_not_text() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a, b FROM jsonb_to_record('{\"a\":1,\"b\":2}'::jsonb) AS x(a int, b int)")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "a must be int 1");
            assertEquals(2, rs.getInt(2), "b must be int 2");
            // Column type in metadata must be INTEGER.
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(Types.INTEGER, md.getColumnType(1),
                    "jsonb_to_record column 'a int' must be JDBC INTEGER, not VARCHAR");
        }
    }

    // =========================================================================
    // W3. record_to_json / record_to_jsonb
    // =========================================================================

    @Test
    void record_to_json_produces_json() throws SQLException {
        String v = str("SELECT row_to_json(x) FROM (SELECT 1 AS a, 'y' AS b) x");
        // PG 18 returns {"a":1,"b":"y"}
        assertTrue(v != null && v.contains("\"a\":1") && v.contains("\"b\":\"y\""),
                "row_to_json must produce JSON; got '" + v + "'");
    }

    @Test
    void record_to_jsonb_registered() throws SQLException {
        // record_to_jsonb(anyelement) registered in pg_proc
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname='row_to_json' OR proname='to_jsonb'")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 2,
                    "row_to_json + to_jsonb must both be registered");
        }
    }

    // =========================================================================
    // W4. row_to_json 2-arg pretty form
    // =========================================================================

    @Test
    void row_to_json_two_arg_pretty() throws SQLException {
        String v = str("SELECT row_to_json(x, true) FROM (SELECT 1 AS a) x");
        // Pretty must include newlines.
        assertTrue(v != null && v.contains("\n"),
                "row_to_json(rec, true) must pretty-print with newlines; got '" + v + "'");
    }

    // =========================================================================
    // W5. jsonb_set create_missing=false preserves original
    // =========================================================================

    @Test
    void jsonb_set_create_missing_false_leaves_missing_path_intact() throws SQLException {
        String v = str(
                "SELECT jsonb_set('{\"a\":1}'::jsonb, '{b}', '99', false)::text");
        // create_missing=false → path b does not exist → original returned as-is.
        assertEquals("{\"a\": 1}", v,
                "jsonb_set(... create_missing=false) on missing path must return original; got '" + v + "'");
    }

    // =========================================================================
    // W6. json_strip_nulls / jsonb_strip_nulls
    // =========================================================================

    @Test
    void jsonb_strip_nulls_removes_null_fields() throws SQLException {
        String v = str("SELECT jsonb_strip_nulls('{\"a\":1,\"b\":null,\"c\":2}'::jsonb)::text");
        assertTrue(v != null && !v.contains("null") && v.contains("\"a\"") && v.contains("\"c\""),
                "jsonb_strip_nulls must remove null-valued fields; got '" + v + "'");
    }

    @Test
    void json_strip_nulls_registered() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname='json_strip_nulls'")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1, "json_strip_nulls must be registered");
        }
    }

    // =========================================================================
    // W7. jsonb - text[] operator
    // =========================================================================

    @Test
    void jsonb_minus_text_array_removes_keys() throws SQLException {
        String v = str(
                "SELECT ('{\"a\":1,\"b\":2,\"c\":3}'::jsonb - ARRAY['a','c'])::text");
        assertEquals("{\"b\": 2}", v,
                "jsonb - text[] must remove listed keys; got '" + v + "'");
    }

    // =========================================================================
    // W8. jsonb_path_match
    // =========================================================================

    @Test
    void jsonb_path_match_registered() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*)::int FROM pg_proc WHERE proname='jsonb_path_match'")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1, "jsonb_path_match must be registered");
        }
    }

    // =========================================================================
    // W9. jsonb_path_query_first
    // =========================================================================

    @Test
    void jsonb_path_query_first_returns_first_match() throws SQLException {
        String v = str(
                "SELECT jsonb_path_query_first('{\"a\":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 1)')::text");
        assertEquals("2", v,
                "jsonb_path_query_first must return first match; got '" + v + "'");
    }
}
