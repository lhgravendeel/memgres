package com.memgres.json;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for jsonb operator semantics against PostgreSQL 18 behavior:
 * containment (@> / <@), text extraction (->> / #>>), concatenation (||),
 * jsonb_set / jsonb_insert path handling, key existence (? / ?| / ?&),
 * element deletion (-), composite literal quoted empty strings, and
 * hstore key-sorted text output.
 */
class JsonbOperatorsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS hstore");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row for: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertBool(boolean expected, String expr) throws SQLException {
        assertEquals(expected ? "t" : "f", q("SELECT " + expr), "for: " + expr);
    }

    // ==== 1. containment @> / <@ ====

    @Test
    void array_contains_scalar_at_top_level() throws SQLException {
        assertBool(true, "'[1,2,3]'::jsonb @> '3'::jsonb");
        assertBool(true, "'[\"a\",\"b\"]'::jsonb @> '\"a\"'::jsonb");
        assertBool(false, "'[1,2,3]'::jsonb @> '4'::jsonb");
    }

    @Test
    void scalar_in_array_special_case_is_top_level_only() throws SQLException {
        // nested: array/scalar mismatch is NOT contained below top level
        assertBool(false, "'{\"a\":[1]}'::jsonb @> '{\"a\":1}'::jsonb");
        assertBool(false, "'[[1]]'::jsonb @> '[1]'::jsonb");
    }

    @Test
    void contained_by_mirrors_contains() throws SQLException {
        assertBool(true, "'[1,2]'::jsonb <@ '[1,2,3]'::jsonb");
        assertBool(true, "'3'::jsonb <@ '[1,2,3]'::jsonb");
        assertBool(false, "'4'::jsonb <@ '[1,2,3]'::jsonb");
        assertBool(false, "'[1]'::jsonb <@ '[[1]]'::jsonb");
    }

    @Test
    void object_and_array_containment_recursive() throws SQLException {
        assertBool(true, "'{\"a\":1,\"b\":2}'::jsonb @> '{\"a\":1}'::jsonb");
        assertBool(true, "'{\"a\":{\"b\":2,\"c\":3}}'::jsonb @> '{\"a\":{\"c\":3}}'::jsonb");
        assertBool(true, "'[{\"a\":1,\"b\":2}]'::jsonb @> '[{\"b\":2}]'::jsonb");
        assertBool(true, "'[1,2,3]'::jsonb @> '[3,1]'::jsonb");
        assertBool(true, "'[[1,2]]'::jsonb @> '[[1]]'::jsonb");
        assertBool(true, "'\"foo\"'::jsonb @> '\"foo\"'::jsonb");
        assertBool(false, "'\"foo\"'::jsonb @> '\"bar\"'::jsonb");
        // ('[1,2]' would be parsed as a range literal by the string-typed heuristics;
        // use three elements so the operand is unambiguously JSON)
        assertBool(true, "'[1,2,3]'::jsonb @> '[]'::jsonb");
        // scalar does not contain an array
        assertBool(false, "'3'::jsonb @> '[3]'::jsonb");
        // array does not contain an object scalar-style
        assertBool(false, "'[{\"a\":1}]'::jsonb @> '{\"a\":1}'::jsonb");
    }

    // ==== 2. ->> and #>> text extraction ====

    @Test
    void text_extraction_maps_json_null_to_sql_null() throws SQLException {
        assertBool(true, "('{\"a\":null}'::jsonb ->> 'a') IS NULL");
        assertBool(true, "('{\"a\":null}'::jsonb #>> '{a}') IS NULL");
        assertBool(true, "('[null]'::jsonb ->> 0) IS NULL");
        // a JSON string "null" is NOT SQL NULL
        assertEquals("null", q("SELECT '[\"null\"]'::jsonb ->> 0"));
        assertEquals("null", q("SELECT '{\"a\":\"null\"}'::jsonb ->> 'a'"));
    }

    @Test
    void text_extraction_unescapes_json_strings() throws SQLException {
        assertEquals("x\"y", q("SELECT '{\"a\":\"x\\\"y\"}'::jsonb ->> 'a'"));
        assertEquals("x\"y", q("SELECT '{\"a\":{\"b\":\"x\\\"y\"}}'::jsonb #>> '{a,b}'"));
        assertEquals("a\\b", q("SELECT '{\"a\":\"a\\\\b\"}'::jsonb ->> 'a'"));
        assertEquals("line1\nline2", q("SELECT '{\"a\":\"line1\\nline2\"}'::jsonb ->> 'a'"));
        assertEquals("tab\there", q("SELECT '{\"a\":\"tab\\there\"}'::jsonb ->> 'a'"));
        assertEquals("A", q("SELECT '{\"a\":\"\\u0041\"}'::jsonb ->> 'a'"));
        assertEquals("a/b", q("SELECT '{\"a\":\"a\\/b\"}'::jsonb ->> 'a'"));
        // array element extraction unescapes too
        assertEquals("x\"y", q("SELECT '[\"x\\\"y\"]'::jsonb ->> 0"));
    }

    @Test
    void text_extraction_of_containers_returns_json_text() throws SQLException {
        assertEquals("{\"b\": 1}", q("SELECT '{\"a\":{\"b\":1}}'::jsonb ->> 'a'"));
        assertEquals("[1, 2]", q("SELECT '{\"a\":[1,2]}'::jsonb #>> '{a}'"));
    }

    // ==== 3. jsonb || concatenation matrix ====

    @Test
    void concat_scalar_scalar_wraps_into_array() throws SQLException {
        assertEquals("[1, 2]", q("SELECT ('1'::jsonb || '2'::jsonb)::text"));
        assertEquals("[\"x\", \"y\"]", q("SELECT ('\"x\"'::jsonb || '\"y\"'::jsonb)::text"));
    }

    @Test
    void concat_array_scalar_appends_and_prepends() throws SQLException {
        assertEquals("[1, 2, 3]", q("SELECT ('[1,2]'::jsonb || '3'::jsonb)::text"));
        assertEquals("[3, 1, 2]", q("SELECT ('3'::jsonb || '[1,2]'::jsonb)::text"));
    }

    @Test
    void concat_object_object_merges_right_wins() throws SQLException {
        assertEquals("{\"a\": 1, \"b\": 2}", q("SELECT ('{\"a\":1}'::jsonb || '{\"b\":2}'::jsonb)::text"));
        assertEquals("{\"a\": 2}", q("SELECT ('{\"a\":1}'::jsonb || '{\"a\":2}'::jsonb)::text"));
    }

    @Test
    void concat_array_array_concatenates() throws SQLException {
        assertEquals("[1, 2, 3]", q("SELECT ('[1]'::jsonb || '[2,3]'::jsonb)::text"));
    }

    @Test
    void concat_object_with_array_or_scalar_wraps_object() throws SQLException {
        assertEquals("[{\"a\": 1}, 1]", q("SELECT ('{\"a\":1}'::jsonb || '[1]'::jsonb)::text"));
        assertEquals("[1, {\"a\": 1}]", q("SELECT ('[1]'::jsonb || '{\"a\":1}'::jsonb)::text"));
        assertEquals("[{\"a\": 1}, 2]", q("SELECT ('{\"a\":1}'::jsonb || '2'::jsonb)::text"));
        assertEquals("[2, {\"a\": 1}]", q("SELECT ('2'::jsonb || '{\"a\":1}'::jsonb)::text"));
    }

    @Test
    void concat_with_null_is_null() throws SQLException {
        assertBool(true, "('1'::jsonb || NULL::jsonb) IS NULL");
    }

    // ==== 4. jsonb_set path semantics ====

    @Test
    void jsonb_set_missing_intermediate_step_returns_target_unchanged() throws SQLException {
        assertEquals("{\"a\": 1}", q("SELECT jsonb_set('{\"a\":1}'::jsonb, '{b,c}', '2'::jsonb)::text"));
        assertEquals("{\"a\": {\"b\": 1}}",
                q("SELECT jsonb_set('{\"a\":{\"b\":1}}'::jsonb, '{x,y,z}', '2'::jsonb)::text"));
    }

    @Test
    void jsonb_set_create_missing_applies_to_last_step_only() throws SQLException {
        assertEquals("{\"a\": 1, \"b\": 2}", q("SELECT jsonb_set('{\"a\":1}'::jsonb, '{b}', '2'::jsonb)::text"));
        assertEquals("{\"a\": 1}", q("SELECT jsonb_set('{\"a\":1}'::jsonb, '{b}', '2'::jsonb, false)::text"));
        assertEquals("{\"a\": 2}", q("SELECT jsonb_set('{\"a\":1}'::jsonb, '{a}', '2'::jsonb, false)::text"));
        assertEquals("{\"a\": {\"b\": 1, \"c\": 2}}",
                q("SELECT jsonb_set('{\"a\":{\"b\":1}}'::jsonb, '{a,c}', '2'::jsonb)::text"));
    }

    @Test
    void jsonb_set_navigates_arrays() throws SQLException {
        assertEquals("{\"a\": [1, 9]}", q("SELECT jsonb_set('{\"a\":[1,2]}'::jsonb, '{a,1}', '9'::jsonb)::text"));
        assertEquals("{\"a\": [{\"b\": 9}]}",
                q("SELECT jsonb_set('{\"a\":[{\"b\":1}]}'::jsonb, '{a,0,b}', '9'::jsonb)::text"));
    }

    @Test
    void jsonb_set_negative_and_out_of_range_indexes() throws SQLException {
        assertEquals("[\"a\", \"c\"]", q("SELECT jsonb_set('[\"a\",\"b\"]'::jsonb, '{-1}', '\"c\"'::jsonb)::text"));
        // out-of-range positive appends; negative prepends (create_missing default true)
        assertEquals("[1, 2]", q("SELECT jsonb_set('[1]'::jsonb, '{5}', '2'::jsonb)::text"));
        assertEquals("[2, 1]", q("SELECT jsonb_set('[1]'::jsonb, '{-5}', '2'::jsonb)::text"));
        // with create_missing false, out-of-range leaves array unchanged
        assertEquals("[1]", q("SELECT jsonb_set('[1]'::jsonb, '{5}', '2'::jsonb, false)::text"));
    }

    // ==== 5. jsonb_insert ====

    @Test
    void jsonb_insert_missing_intermediate_step_returns_target_unchanged() throws SQLException {
        assertEquals("{\"a\": 1}", q("SELECT jsonb_insert('{\"a\":1}'::jsonb, '{b,c}', '2'::jsonb)::text"));
    }

    @Test
    void jsonb_insert_new_object_key_inserts_existing_key_errors() throws SQLException {
        assertEquals("{\"a\": 1, \"b\": 2}", q("SELECT jsonb_insert('{\"a\":1}'::jsonb, '{b}', '2'::jsonb)::text"));
        SQLException e = assertThrows(SQLException.class,
                () -> q("SELECT jsonb_insert('{\"a\":1}'::jsonb, '{a}', '2'::jsonb)::text"));
        assertEquals("22023", e.getSQLState());
    }

    @Test
    void jsonb_insert_array_positions() throws SQLException {
        assertEquals("{\"a\": [0, \"x\", 1, 2]}",
                q("SELECT jsonb_insert('{\"a\":[0,1,2]}'::jsonb, '{a,1}', '\"x\"'::jsonb)::text"));
        assertEquals("{\"a\": [0, 1, \"x\", 2]}",
                q("SELECT jsonb_insert('{\"a\":[0,1,2]}'::jsonb, '{a,1}', '\"x\"'::jsonb, true)::text"));
        // negative index counts from the end
        assertEquals("[1, 2, 9, 3]", q("SELECT jsonb_insert('[1,2,3]'::jsonb, '{-1}', '9'::jsonb)::text"));
        assertEquals("[1, 2, 3, 9]", q("SELECT jsonb_insert('[1,2,3]'::jsonb, '{-1}', '9'::jsonb, true)::text"));
    }

    // ==== 6. ? / ?| / ?& key existence ====

    @Test
    void question_matches_top_level_scalar_string() throws SQLException {
        assertBool(true, "'\"foo\"'::jsonb ? 'foo'");
        assertBool(false, "'\"foo\"'::jsonb ? 'bar'");
    }

    @Test
    void question_on_arrays_matches_string_elements_only() throws SQLException {
        assertBool(true, "'[\"a\",\"b\"]'::jsonb ? 'a'");
        assertBool(false, "'[\"a\",\"b\"]'::jsonb ? 'c'");
        // number elements do not match their text form
        assertBool(false, "'[1,2]'::jsonb ? '1'");
    }

    @Test
    void question_on_objects_matches_keys() throws SQLException {
        assertBool(true, "'{\"a\":1}'::jsonb ? 'a'");
        assertBool(false, "'{\"a\":1}'::jsonb ? 'b'");
    }

    @Test
    void question_any_and_all_follow_same_rules() throws SQLException {
        assertBool(true, "'\"foo\"'::jsonb ?| array['bar','foo']");
        assertBool(false, "'\"foo\"'::jsonb ?| array['bar','baz']");
        assertBool(true, "'\"foo\"'::jsonb ?& array['foo']");
        assertBool(true, "'[\"a\",\"b\"]'::jsonb ?& array['a','b']");
        assertBool(false, "'[\"a\",\"b\"]'::jsonb ?& array['a','c']");
        assertBool(true, "'{\"a\":1,\"b\":2}'::jsonb ?& array['a','b']");
    }

    // ==== 7. jsonb - integer deletion ====

    @Test
    void minus_negative_index_deletes_from_end() throws SQLException {
        assertEquals("[\"a\", \"b\"]", q("SELECT ('[\"a\",\"b\",\"c\"]'::jsonb - -1)::text"));
        assertEquals("[\"b\", \"c\"]", q("SELECT ('[\"a\",\"b\",\"c\"]'::jsonb - -3)::text"));
        assertEquals("[\"a\", \"c\"]", q("SELECT ('[\"a\",\"b\",\"c\"]'::jsonb - 1)::text"));
    }

    @Test
    void minus_out_of_range_index_leaves_array_unchanged() throws SQLException {
        assertEquals("[\"a\", \"b\", \"c\"]", q("SELECT ('[\"a\",\"b\",\"c\"]'::jsonb - 5)::text"));
        assertEquals("[\"a\", \"b\", \"c\"]", q("SELECT ('[\"a\",\"b\",\"c\"]'::jsonb - -5)::text"));
    }

    // ==== 8. composite type quoted empty string ====

    @Test
    void composite_quoted_empty_string_is_empty_not_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE jot_ct AS (a int, b text)");
            try (ResultSet rs = s.executeQuery("SELECT ('(1,\"\")'::jot_ct).b")) {
                assertTrue(rs.next());
                assertEquals("", rs.getString(1));
                assertFalse(rs.wasNull(), "quoted empty string must not be NULL");
            }
            try (ResultSet rs = s.executeQuery("SELECT ('(1,)'::jot_ct).b")) {
                assertTrue(rs.next());
                assertNull(rs.getString(1), "unquoted empty field must be NULL");
            }
            s.execute("DROP TYPE jot_ct");
        }
    }

    // ==== 9. hstore key-sorted output ====

    @Test
    void hstore_output_is_key_sorted() throws SQLException {
        assertEquals("\"a\"=>\"1\", \"b\"=>\"2\", \"c\"=>\"3\"",
                q("SELECT ('c=>3, a=>1, b=>2'::hstore)::text"));
        // hstore sorts like jsonb keys: shorter keys first, then byte order
        assertEquals("\"a\"=>\"1\", \"z\"=>\"3\", \"bb\"=>\"2\"",
                q("SELECT ('bb=>2, z=>3, a=>1'::hstore)::text"));
    }

    @Test
    void hstore_lookup_unchanged_by_sorted_output() throws SQLException {
        assertEquals("3", q("SELECT ('c=>3, a=>1'::hstore) -> 'c'"));
        assertBool(true, "('c=>3, a=>1'::hstore) ? 'a'");
    }
}
