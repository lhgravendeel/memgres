package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: SQL/JSON standard & jsonb extensions.
 *
 * - JSON_TABLE / JSON_VALUE / JSON_QUERY / JSON_EXISTS
 * - jsonb subscript operator  j['key']  / j[0]      (PG 14+)
 * - jsonb_path_*_tz family                           (PG 17+)
 * - jsonb_set_lax                                    (PG 13+)
 * - @? / @@ jsonpath operators
 * - JSON_ARRAYAGG / JSON_OBJECTAGG / WITH UNIQUE KEYS
 * - jsonb_object array/array constructors
 */
class Round14JsonStandardTest {

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

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. JSON_TABLE (SQL/JSON PG 17+)
    // =========================================================================

    @Test
    void json_table_scalar_columns() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a, b FROM JSON_TABLE("
                             + "'[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"}]'::jsonb, "
                             + "'$[*]' COLUMNS (a int PATH '$.a', b text PATH '$.b')) ORDER BY a")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("a"));
            assertEquals("x", rs.getString("b"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("a"));
            assertEquals("y", rs.getString("b"));
        }
    }

    @Test
    void json_table_nested_path() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT a, tag FROM JSON_TABLE("
                             + "'{\"rows\":[{\"a\":1,\"tags\":[\"x\",\"y\"]}]}'::jsonb, "
                             + "'$.rows[*]' COLUMNS (a int PATH '$.a', "
                             + "NESTED PATH '$.tags[*]' COLUMNS (tag text PATH '$'))) ORDER BY tag")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt("a")); assertEquals("x", rs.getString("tag"));
            assertTrue(rs.next()); assertEquals(1, rs.getInt("a")); assertEquals("y", rs.getString("tag"));
        }
    }

    @Test
    void json_table_with_ordinality() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT n, v FROM JSON_TABLE('[10,20,30]'::jsonb, '$[*]' "
                             + "COLUMNS (n FOR ORDINALITY, v int PATH '$')) ORDER BY n")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt("n")); assertEquals(10, rs.getInt("v"));
            assertTrue(rs.next()); assertEquals(2, rs.getInt("n")); assertEquals(20, rs.getInt("v"));
        }
    }

    // =========================================================================
    // B. JSON_VALUE / JSON_QUERY / JSON_EXISTS (PG 17+)
    // =========================================================================

    @Test
    void json_value_scalar() throws SQLException {
        assertEquals("1",
                scalarString("SELECT JSON_VALUE('{\"a\":1}'::jsonb, '$.a')"));
    }

    @Test
    void json_value_default_on_empty() throws SQLException {
        assertEquals("missing",
                scalarString("SELECT JSON_VALUE('{\"a\":1}'::jsonb, '$.b' DEFAULT 'missing' ON EMPTY)"));
    }

    @Test
    void json_query_returns_json() throws SQLException {
        // JSON_QUERY returns a jsonb value, not scalar text
        String v = scalarString("SELECT JSON_QUERY('{\"a\":[1,2]}'::jsonb, '$.a')::text");
        assertTrue(v.contains("1") && v.contains("2"));
    }

    @Test
    void json_exists_true() throws SQLException {
        assertEquals("true",
                scalarString("SELECT JSON_EXISTS('{\"a\":1}'::jsonb, '$.a')::text"));
    }

    @Test
    void json_exists_false() throws SQLException {
        assertEquals("false",
                scalarString("SELECT JSON_EXISTS('{\"a\":1}'::jsonb, '$.b')::text"));
    }

    // =========================================================================
    // C. jsonb subscript operator (PG 14+)
    // =========================================================================

    @Test
    void jsonb_subscript_read_key() throws SQLException {
        assertEquals("1",
                scalarString("SELECT ('{\"a\":1}'::jsonb)['a']::text"));
    }

    @Test
    void jsonb_subscript_read_index() throws SQLException {
        assertEquals("2",
                scalarString("SELECT ('[1,2,3]'::jsonb)[1]::text"));
    }

    @Test
    void jsonb_subscript_chained() throws SQLException {
        // Chained subscripts
        String v = scalarString(
                "SELECT ('{\"a\":{\"b\":42}}'::jsonb)['a']['b']::text");
        assertEquals("42", v);
    }

    @Test
    void jsonb_subscript_assign_in_update() throws SQLException {
        exec("CREATE TABLE r14_js_sub (j jsonb)");
        exec("INSERT INTO r14_js_sub VALUES ('{\"a\":1}')");
        exec("UPDATE r14_js_sub SET j['a'] = '99'::jsonb");
        assertEquals("99",
                scalarString("SELECT (j->'a')::text FROM r14_js_sub"));
    }

    // =========================================================================
    // D. jsonb_path_*_tz family (PG 17+)
    // =========================================================================

    @Test
    void jsonb_path_exists_tz() throws SQLException {
        // Timezone-aware jsonpath function (comparing timestamps)
        assertEquals("true", scalarString(
                "SELECT jsonb_path_exists_tz('\"2024-01-01T00:00:00Z\"'::jsonb, "
                        + "'$.datetime() < \"2025-01-01\".datetime()')::text"));
    }

    @Test
    void jsonb_path_query_tz() throws SQLException {
        String v = scalarString(
                "SELECT jsonb_path_query_tz('\"2024-01-01T00:00:00Z\"'::jsonb, '$')::text");
        assertNotNull(v);
    }

    @Test
    void jsonb_path_match_tz() throws SQLException {
        assertEquals("true", scalarString(
                "SELECT jsonb_path_match_tz('\"2024-01-01T00:00:00Z\"'::jsonb, "
                        + "'$.datetime() <= \"2030-01-01\".datetime()')::text"));
    }

    // =========================================================================
    // E. jsonb_set_lax (PG 13+)
    // =========================================================================

    @Test
    void jsonb_set_lax_null_as_return_target() throws SQLException {
        // null_value_treatment = 'return_target': NULL value returns the original
        String v = scalarString(
                "SELECT jsonb_set_lax('{\"a\":1}'::jsonb, '{b}', NULL, true, 'return_target')::text");
        assertEquals("{\"a\": 1}", v);
    }

    @Test
    void jsonb_set_lax_null_as_delete_key() throws SQLException {
        // null_value_treatment = 'delete_key'
        String v = scalarString(
                "SELECT jsonb_set_lax('{\"a\":1,\"b\":2}'::jsonb, '{b}', NULL, true, 'delete_key')::text");
        assertEquals("{\"a\": 1}", v);
    }

    @Test
    void jsonb_set_lax_null_as_use_null() throws SQLException {
        String v = scalarString(
                "SELECT jsonb_set_lax('{\"a\":1}'::jsonb, '{a}', NULL, true, 'use_json_null')::text");
        assertEquals("{\"a\": null}", v);
    }

    // =========================================================================
    // F. @? and @@ jsonpath operators (PG 12+; variants through PG 16)
    // =========================================================================

    @Test
    void jsonpath_exists_operator() throws SQLException {
        // @? returns whether any match exists
        assertEquals("true", scalarString(
                "SELECT ('{\"a\":1}'::jsonb @? '$.a')::text"));
    }

    @Test
    void jsonpath_predicate_operator() throws SQLException {
        // @@ returns result of boolean predicate
        assertEquals("true", scalarString(
                "SELECT ('{\"a\":1}'::jsonb @@ '$.a == 1')::text"));
    }

    // =========================================================================
    // G. JSON_ARRAYAGG / JSON_OBJECTAGG (SQL/JSON)
    // =========================================================================

    @Test
    void json_arrayagg_basic() throws SQLException {
        assertEquals("[1, 2, 3]",
                scalarString("SELECT JSON_ARRAYAGG(x) FROM (VALUES (1),(2),(3)) t(x)"));
    }

    @Test
    void json_objectagg_basic() throws SQLException {
        String v = scalarString(
                "SELECT JSON_OBJECTAGG(k VALUE v) FROM (VALUES ('a',1),('b',2)) t(k,v)");
        // Should contain both keys
        assertTrue(v.contains("\"a\"") && v.contains("\"b\""), "got " + v);
    }

    @Test
    void json_objectagg_with_unique_keys() throws SQLException {
        // WITH UNIQUE KEYS errors on duplicate keys
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute(
                        "SELECT JSON_OBJECTAGG(k VALUE v WITH UNIQUE KEYS) "
                                + "FROM (VALUES ('a',1),('a',2)) t(k,v)");
            }
        });
        assertNotNull(ex.getMessage());
    }

    // =========================================================================
    // H. JSON_OBJECT KEY-VALUE syntax
    // =========================================================================

    @Test
    void json_object_key_value_syntax() throws SQLException {
        // PG 18 uses colon syntax, not KEY...VALUE syntax
        assertEquals("{\"a\" : 1, \"b\" : 2}",
                scalarString("SELECT JSON_OBJECT('a' : 1, 'b' : 2)::text"));
    }

    // =========================================================================
    // I. jsonb_object(text[], text[])
    // =========================================================================

    @Test
    void jsonb_object_two_arrays() throws SQLException {
        String v = scalarString(
                "SELECT jsonb_object(ARRAY['a','b']::text[], ARRAY['1','2']::text[])::text");
        assertTrue(v.contains("\"a\"") && v.contains("1"));
    }

    @Test
    void jsonb_object_single_array_alternating() throws SQLException {
        // Single array of alternating k,v
        String v = scalarString(
                "SELECT jsonb_object(ARRAY['a','1','b','2'])::text");
        assertTrue(v.contains("\"a\"") && v.contains("\"b\""));
    }

    // =========================================================================
    // J. json_strip_nulls (json variant)
    // =========================================================================

    @Test
    void json_strip_nulls_json_variant() throws SQLException {
        assertEquals("{\"a\":1}",
                scalarString("SELECT json_strip_nulls('{\"a\":1,\"b\":null}'::json)::text"));
    }
}
