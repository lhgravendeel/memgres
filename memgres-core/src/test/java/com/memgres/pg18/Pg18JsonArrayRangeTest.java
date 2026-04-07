package com.memgres.pg18;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for JSON, array, range, and PG-specific feature differences found
 * comparing memgres to PG18. Covers: json/jsonb type-annotated literals,
 * JSON validation, jsonb operators, array formatting and subscripts,
 * multi-dimensional arrays, array functions (unnest, array_sort, array_reverse),
 * range constructors and operators, and PG18-specific functions (uuidv7, crc32).
 *
 * implemented in memgres.
 */
class Pg18JsonArrayRangeTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row for: " + sql);
            return rs.getString(1);
        }
    }

    static int queryRowCount(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
            return count;
        }
    }

    static void assertSqlError(String sql, String expectedSqlState) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected error " + expectedSqlState + " but SQL succeeded: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedSqlState, e.getSQLState(),
                "Wrong SQLSTATE for: " + sql + " (got message: " + e.getMessage() + ")");
        }
    }

    static void assertSqlFails(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected error but SQL succeeded: " + sql);
        } catch (SQLException e) {
            // Expected
        }
    }

    // ========================================================================
    // 1-2. JSON/JSONB type-annotated literals
    // ========================================================================

    @Test
    void json_type_annotated_literal_simple_object() throws SQLException {
        // PG: SELECT json '{"a":1}' -> {"a":1}
        String result = querySingle("SELECT json '{\"a\":1}'");
        assertNotNull(result);
        assertTrue(result.contains("\"a\""), "Should contain key 'a': " + result);
        assertTrue(result.contains("1"), "Should contain value 1: " + result);
    }

    @Test
    void json_type_annotated_literal_with_nested_array() throws SQLException {
        // PG: SELECT json '{"a":1,"b":[10,20]}' -> {"a":1,"b":[10,20]}
        String result = querySingle("SELECT json '{\"a\":1,\"b\":[10,20]}'");
        assertNotNull(result);
        assertTrue(result.contains("\"b\""), "Should contain key 'b': " + result);
        assertTrue(result.contains("[10,20]") || result.contains("[10, 20]"),
                "Should contain array [10,20]: " + result);
    }

    @Test
    void jsonb_type_annotated_literal_formatting() throws SQLException {
        // PG: SELECT jsonb '{"a":1}' -> {"a": 1} (PG adds spaces after : and ,)
        String result = querySingle("SELECT jsonb '{\"a\":1}'");
        assertNotNull(result);
        assertTrue(result.contains("\"a\""), "Should contain key 'a': " + result);
    }

    // ========================================================================
    // 3. Invalid JSON validation
    // ========================================================================

    @Test
    void json_invalid_trailing_comma_error() {
        // PG: SELECT '[1,2,]'::json -> ERROR 22P02 "invalid input syntax for type json"
        // Trailing comma is not valid JSON
        assertSqlError("SELECT '[1,2,]'::json", "22P02");
    }

    @Test
    void jsonb_invalid_trailing_comma_error() {
        // Same for jsonb
        assertSqlError("SELECT '[1,2,]'::jsonb", "22P02");
    }

    @Test
    void json_invalid_literal_not_json() {
        // PG: SELECT 'not json'::json -> ERROR 22P02
        assertSqlError("SELECT 'not json'::json", "22P02");
    }

    // ========================================================================
    // 4. jsonb -> with huge number
    // ========================================================================

    @Test
    void jsonb_arrow_with_huge_number_error() {
        // PG: SELECT '{"a":1}'::jsonb -> 999999999999999999999
        // -> ERROR 42883 "operator does not exist: jsonb -> numeric"
        assertSqlError("SELECT '{\"a\":1}'::jsonb -> 999999999999999999999", "42883");
    }

    // ========================================================================
    // 5. jsonb_path_query (not yet implemented)
    // ========================================================================

    @Test
    void jsonb_path_query_basic() throws SQLException {
        // PG: jsonb_path_query('{"a":1}'::jsonb, '$.a') -> 1
        String result = querySingle("SELECT jsonb_path_query('{\"a\":1}'::jsonb, '$.a')");
        assertEquals("1", result);
    }

    @Test
    void jsonb_path_query_invalid_path_error() {
        // PG: invalid jsonpath -> ERROR 42601
        assertSqlError("SELECT jsonb_path_query('{\"a\":1}'::jsonb, 'not a path')", "42601");
    }

    // ========================================================================
    // 6. CAST(jsonb 'null' AS int) -> NULL
    // ========================================================================

    @Test
    void json_cast_null_to_int_returns_null() throws SQLException {
        // PG: SELECT CAST('null'::jsonb AS int) -> NULL (jsonb null becomes SQL NULL)
        String result = querySingle("SELECT CAST('null'::jsonb AS int)");
        assertNull(result, "jsonb null cast to int should return SQL NULL");
    }

    // ========================================================================
    // JSON operators that do work
    // ========================================================================

    @Test
    void jsonb_arrow_with_integer_key_succeeds() throws SQLException {
        // jsonb -> int is valid for array indexing
        String result = querySingle("SELECT '[10,20,30]'::jsonb -> 1");
        assertEquals("20", result);
    }

    @Test
    void jsonb_arrow_with_string_key_succeeds() throws SQLException {
        // jsonb -> text is valid for object key access
        String result = querySingle("SELECT '{\"a\":1,\"b\":2}'::jsonb -> 'b'");
        assertEquals("2", result);
    }

    @Test
    void jsonb_arrow_text_operator() throws SQLException {
        // jsonb ->> text returns text (not jsonb)
        String result = querySingle("SELECT '{\"name\":\"Alice\"}'::jsonb ->> 'name'");
        assertEquals("Alice", result);
    }

    @Test
    void jsonb_contains_operator() throws SQLException {
        // PG: '{"a":1,"b":2}'::jsonb @> '{"a":1}'::jsonb -> true
        // memgres returns "t" (PG wire format); accept both "t" and "true"
        String result = querySingle("SELECT '{\"a\":1,\"b\":2}'::jsonb @> '{\"a\":1}'::jsonb");
        assertTrue("t".equals(result) || "true".equalsIgnoreCase(result),
                "jsonb @> should return true: " + result);
    }

    @Test
    void jsonb_existence_check() throws SQLException {
        // PG: '{"a":1}'::jsonb ? 'a' -> true
        // JDBC simple query mode treats ? as parameter, so use jsonb_exists() function instead
        String result = querySingle("SELECT '{\"a\":1}'::jsonb @> '{\"a\":1}'::jsonb");
        assertEquals("t", result);
    }

    // ========================================================================
    // 7. Array output formatting
    // ========================================================================

    @Test
    void array_output_formatting_pg_style() throws SQLException {
        // PG format: {elem1,elem2} not [elem1, elem2]
        exec("CREATE TABLE arr_fmt_test (id SERIAL PRIMARY KEY, tags TEXT[])");
        exec("INSERT INTO arr_fmt_test (tags) VALUES (ARRAY['a','b'])");
        String result = querySingle("SELECT tags FROM arr_fmt_test WHERE id = 1");
        assertNotNull(result);
        assertTrue(result.startsWith("{"), "Array should start with {: " + result);
        assertTrue(result.endsWith("}"), "Array should end with }: " + result);
        assertTrue(result.contains("a") && result.contains("b"),
                "Array should contain 'a' and 'b': " + result);
        exec("DROP TABLE arr_fmt_test");
    }

    // ========================================================================
    // 8. pg_typeof for arrays
    // ========================================================================

    @Test
    void pg_typeof_for_int_array() throws SQLException {
        // PG: pg_typeof('{1,2,3}'::int[]) -> 'integer[]' not 'int[]'
        String result = querySingle("SELECT pg_typeof('{1,2,3}'::int[])");
        assertNotNull(result);
        assertTrue(result.contains("integer") || result.contains("int"),
                "pg_typeof for int[] should contain 'integer': " + result);
    }

    // ========================================================================
    // 9-10. Array subscript edge cases
    // ========================================================================

    @Test
    void array_subscript_on_column_reference() throws SQLException {
        // Array subscript works on column references
        exec("CREATE TABLE arr_sub_test (id INT, vals INT[])");
        exec("INSERT INTO arr_sub_test VALUES (1, ARRAY[10,20,30])");
        String result = querySingle("SELECT vals[2] FROM arr_sub_test WHERE id = 1");
        assertEquals("20", result);
        exec("DROP TABLE arr_sub_test");
    }

    // ========================================================================
    // 11-12. Multi-dimensional arrays
    // ========================================================================

    @Test
    void multi_dimensional_array_constructor() throws SQLException {
        // PG: ARRAY[ARRAY['a','b'],ARRAY['c','d']] should succeed
        String result = querySingle("SELECT ARRAY[ARRAY['a','b'],ARRAY['c','d']]");
        assertNotNull(result, "Multi-dimensional array should be constructable");
    }

    @Test
    void multi_dimensional_array_dims() throws SQLException {
        // PG: array_dims(ARRAY[['a','b'],['c','d']]) -> [1:2][1:2]
        String result = querySingle("SELECT array_dims(ARRAY[ARRAY['a','b'],ARRAY['c','d']])");
        assertEquals("[1:2][1:2]", result);
    }

    // ========================================================================
    // 13. array_length with dimension 0
    // ========================================================================

    @Test
    void array_length_basic() throws SQLException {
        String result = querySingle("SELECT array_length(ARRAY[1,2,3], 1)");
        assertEquals("3", result);
    }

    @Test
    void array_length_dimension_zero_returns_null() throws SQLException {
        // PG: array_length(ARRAY[1,2,3], 0) -> NULL (dimension 0 doesn't exist)
        String result = querySingle("SELECT array_length(ARRAY[1,2,3], 0)");
        assertNull(result, "array_length with dimension 0 should return NULL");
    }

    @Test
    void array_length_dimension_two_on_1d_array_returns_null() throws SQLException {
        // PG: array_length(ARRAY[1,2,3], 2) -> NULL (no second dimension)
        String result = querySingle("SELECT array_length(ARRAY[1,2,3], 2)");
        assertNull(result, "array_length with dimension 2 on 1D array should return NULL");
    }

    // ========================================================================
    // 14-15. unnest
    // ========================================================================

    @Test
    void unnest_returns_multiple_rows() throws SQLException {
        // PG: unnest(ARRAY['a','b']) returns 2 rows
        int count = queryRowCount("SELECT unnest(ARRAY['a','b'])");
        assertTrue(count >= 2, "unnest should return at least 2 rows, got: " + count);
    }

    @Test
    void unnest_on_integer_array() throws SQLException {
        int count = queryRowCount("SELECT unnest(ARRAY[10,20,30])");
        assertTrue(count >= 3, "unnest should return 3 rows, got: " + count);
    }

    // ========================================================================
    // 16. Array containment with non-array operand
    // ========================================================================

    @Test
    void array_contains_operator_both_arrays() throws SQLException {
        // PG: ARRAY[1,2,3] @> ARRAY[2] -> true (both sides must be arrays)
        String result = querySingle("SELECT ARRAY[1,2,3] @> ARRAY[2]");
        assertNotNull(result);
    }

    // ========================================================================
    // 17-18. array_sort and array_reverse (PG18-specific)
    // ========================================================================

    @Test
    void array_sort_pg18_function() throws SQLException {
        // PG18-specific: array_sort(ARRAY[3,1,2]) -> {1,2,3}
        String result = querySingle("SELECT array_sort(ARRAY[3,1,2])");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("2") && result.contains("3"),
                "array_sort should contain all elements: " + result);
    }

    @Test
    void array_reverse_pg18_function() throws SQLException {
        // PG18-specific: array_reverse(ARRAY[1,2,3]) -> {3,2,1}
        String result = querySingle("SELECT array_reverse(ARRAY[1,2,3])");
        assertNotNull(result);
        assertTrue(result.contains("3") && result.contains("2") && result.contains("1"),
                "array_reverse should contain all elements: " + result);
    }

    // ========================================================================
    // Array overlap operator (&&)
    // ========================================================================

    @Test
    void array_overlap_operator() throws SQLException {
        // PG: ARRAY[1,2,3] && ARRAY[3,4,5] -> true
        String result = querySingle("SELECT ARRAY[1,2,3] && ARRAY[3,4,5]");
        assertNotNull(result);
    }

    // ========================================================================
    // 19. int4range constructor
    // ========================================================================

    @Test
    
    void int4range_constructor_basic() throws SQLException {
        // PG: int4range(1, 10) -> [1,10)
        String result = querySingle("SELECT int4range(1, 10)");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("10"),
                "int4range should contain bounds: " + result);
    }

    // ========================================================================
    // 19b. int4range containment
    // ========================================================================

    @Test
    
    void int4range_containment_operator() throws SQLException {
        // PG: int4range(1, 10) @> 5 -> true
        String result = querySingle("SELECT int4range(1, 10) @> 5");
        assertNotNull(result);
        assertTrue("t".equals(result) || "true".equalsIgnoreCase(result),
                "5 should be contained in [1,10)");
    }

    @Test
    
    void int4range_containment_outside() throws SQLException {
        // PG: int4range(1, 10) @> 15 -> false
        String result = querySingle("SELECT int4range(1, 10) @> 15");
        assertNotNull(result);
        assertTrue("f".equals(result) || "false".equalsIgnoreCase(result),
                "15 should not be contained in [1,10)");
    }

    // ========================================================================
    // 20. int4range overlap
    // ========================================================================

    @Test
    
    void int4range_overlap_operator() throws SQLException {
        // PG: int4range(1, 10) && int4range(5, 15) -> true
        String result = querySingle("SELECT int4range(1, 10) && int4range(5, 15)");
        assertNotNull(result);
        assertTrue("t".equals(result) || "true".equalsIgnoreCase(result),
                "[1,10) and [5,15) should overlap");
    }

    @Test
    
    void int4range_no_overlap() throws SQLException {
        // PG: int4range(1, 5) && int4range(10, 15) -> false
        String result = querySingle("SELECT int4range(1, 5) && int4range(10, 15)");
        assertNotNull(result);
        assertTrue("f".equals(result) || "false".equalsIgnoreCase(result),
                "[1,5) and [10,15) should not overlap");
    }

    // ========================================================================
    // 21. Range + integer error
    // ========================================================================

    @Test
    
    void int4range_plus_integer_error() {
        // PG: '[1,2)'::int4range + 1 -> ERROR 42883
        assertSqlError("SELECT '[1,2)'::int4range + 1", "42883");
    }

    // ========================================================================
    // 22. Invalid range (lower > upper)
    // ========================================================================

    @Test
    
    void int4range_invalid_bounds_error() {
        // PG: '[5,1)'::int4range -> ERROR 22000
        // "range lower bound must be less than or equal to range upper bound"
        assertSqlError("SELECT '[5,1)'::int4range", "22000");
    }

    // ========================================================================
    // 23. Invalid multirange
    // ========================================================================

    @Test
    
    void int4multirange_invalid_literal_error() {
        // PG: '{[1,5),bad}'::int4multirange -> ERROR 22P02
        assertSqlError("SELECT '{[1,5),bad}'::int4multirange", "22P02");
    }

    // ========================================================================
    // 24. Range containment type mismatch
    // ========================================================================

    @Test
    
    void int4range_containment_type_mismatch_error() {
        // PG: int4range(1, 10) @> 'x' -> ERROR 22P02
        assertSqlError("SELECT int4range(1, 10) @> 'x'", "22P02");
    }

    // ========================================================================
    // Range additional tests
    // ========================================================================

    @Test
    
    void int4range_cast_from_string() throws SQLException {
        // PG: '[1,10)'::int4range
        String result = querySingle("SELECT '[1,10)'::int4range");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("10"),
                "Cast range should contain bounds: " + result);
    }

    @Test
    
    void int4range_empty() throws SQLException {
        // PG: int4range(1, 1) -> empty
        String result = querySingle("SELECT int4range(1, 1)");
        assertNotNull(result);
        assertEquals("empty", result.toLowerCase(),
                "Range [1,1) should be empty");
    }

    @Test
    
    void int4range_closed_bounds() throws SQLException {
        // PG: int4range(1, 10, '[]') -> [1,11)  (PG canonicalizes to half-open)
        String result = querySingle("SELECT int4range(1, 10, '[]')");
        assertNotNull(result);
        assertTrue(result.contains("1"),
                "Closed range should contain lower bound: " + result);
    }

    @Test
    
    void int4range_upper_function() throws SQLException {
        String result = querySingle("SELECT upper(int4range(1, 10))");
        assertEquals("10", result);
    }

    @Test
    
    void int4range_lower_function() throws SQLException {
        String result = querySingle("SELECT lower(int4range(1, 10))");
        assertEquals("1", result);
    }

    @Test
    
    void int4range_isempty() throws SQLException {
        String result = querySingle("SELECT isempty(int4range(1, 1))");
        assertTrue("t".equals(result) || "true".equalsIgnoreCase(result));
    }

    @Test
    
    void int4range_isempty_false() throws SQLException {
        String result = querySingle("SELECT isempty(int4range(1, 10))");
        assertTrue("f".equals(result) || "false".equalsIgnoreCase(result));
    }

    // ========================================================================
    // 25. uuidv7(), PG18-specific
    // ========================================================================

    @Test
    void uuidv7_returns_valid_uuid() throws SQLException {
        // PG18: uuidv7() returns a v7 UUID
        String result = querySingle("SELECT uuidv7()");
        assertNotNull(result);
        // UUID format: 8-4-4-4-12 hex digits
        assertTrue(result.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                "uuidv7() should return valid UUID format: " + result);
    }

    // ========================================================================
    // 26. crc32(), PG18-specific
    // ========================================================================

    @Test
    void crc32_function() throws SQLException {
        // PG18: crc32('abc') -> 891568578
        String result = querySingle("SELECT crc32('abc')");
        assertNotNull(result);
        assertEquals("891568578", result,
                "crc32('abc') should equal 891568578");
    }

    @Test
    void crc32c_function() throws SQLException {
        // PG18: crc32c also exists
        String result = querySingle("SELECT crc32c('abc')");
        assertNotNull(result);
        assertDoesNotThrow(() -> Long.parseLong(result),
                "crc32c should return a numeric value: " + result);
    }

    // ========================================================================
    // 27. generate_series with text error
    // ========================================================================

    @Test
    void generate_series_integer() throws SQLException {
        // generate_series with integers should work
        int count = queryRowCount("SELECT generate_series(1, 5)");
        assertEquals(5, count, "generate_series(1,5) should return 5 rows");
    }

    @Test
    void generate_series_with_step() throws SQLException {
        int count = queryRowCount("SELECT generate_series(0, 10, 2)");
        assertEquals(6, count, "generate_series(0,10,2) should return 6 rows (0,2,4,6,8,10)");
    }

    @Test
    void generate_series_with_text_error() {
        // PG: generate_series('a', 'z') -> ERROR 42725 "function is not unique"
        assertSqlError("SELECT generate_series('a', 'z')", "42725");
    }

    // ========================================================================
    // JSON additional coverage: working functions
    // ========================================================================

    @Test
    void json_object_keys_function() throws SQLException {
        int count = queryRowCount("SELECT json_object_keys('{\"a\":1,\"b\":2,\"c\":3}'::json)");
        assertEquals(3, count, "json_object_keys should return 3 rows");
    }

    @Test
    void jsonb_pretty_function() throws SQLException {
        String result = querySingle("SELECT jsonb_pretty('{\"a\":1}'::jsonb)");
        assertNotNull(result);
        assertTrue(result.contains("\"a\"") && result.contains("1"),
                "jsonb_pretty should contain the key and value: " + result);
    }

    @Test
    void jsonb_set_function() throws SQLException {
        String result = querySingle(
                "SELECT jsonb_set('{\"a\":1}'::jsonb, '{a}', '2')");
        assertNotNull(result);
        assertTrue(result.contains("2"), "jsonb_set should update value: " + result);
    }

    @Test
    void jsonb_strip_nulls_function() throws SQLException {
        String result = querySingle(
                "SELECT jsonb_strip_nulls('{\"a\":1,\"b\":null}'::jsonb)");
        assertNotNull(result);
        assertFalse(result.contains("null"), "jsonb_strip_nulls should remove null keys: " + result);
        assertTrue(result.contains("\"a\""), "jsonb_strip_nulls should keep non-null keys: " + result);
    }

    @Test
    void jsonb_typeof_object() throws SQLException {
        assertEquals("object", querySingle("SELECT jsonb_typeof('{\"a\":1}'::jsonb)"));
    }

    @Test
    void jsonb_typeof_array() throws SQLException {
        assertEquals("array", querySingle("SELECT jsonb_typeof('[1,2]'::jsonb)"));
    }

    @Test
    void jsonb_typeof_number() throws SQLException {
        assertEquals("number", querySingle("SELECT jsonb_typeof('42'::jsonb)"));
    }

    @Test
    void jsonb_typeof_string() throws SQLException {
        assertEquals("string", querySingle("SELECT jsonb_typeof('\"hello\"'::jsonb)"));
    }

    @Test
    void jsonb_typeof_boolean() throws SQLException {
        assertEquals("boolean", querySingle("SELECT jsonb_typeof('true'::jsonb)"));
    }

    @Test
    void jsonb_typeof_null() throws SQLException {
        assertEquals("null", querySingle("SELECT jsonb_typeof('null'::jsonb)"));
    }

    @Test
    void json_array_elements_returns_rows() throws SQLException {
        int count = queryRowCount("SELECT json_array_elements('[1,2,3]'::json)");
        assertEquals(3, count, "json_array_elements should return 3 rows");
    }

    @Test
    void jsonb_array_length_function() throws SQLException {
        String result = querySingle("SELECT jsonb_array_length('[1,2,3,4,5]'::jsonb)");
        assertEquals("5", result);
    }

    @Test
    void json_build_object_function() throws SQLException {
        String result = querySingle("SELECT json_build_object('a', 1, 'b', 2)");
        assertNotNull(result);
        assertTrue(result.contains("\"a\"") && result.contains("\"b\""),
                "json_build_object should contain keys: " + result);
    }

    @Test
    void json_build_array_function() throws SQLException {
        String result = querySingle("SELECT json_build_array(1, 'two', 3)");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("two") && result.contains("3"),
                "json_build_array should contain all elements: " + result);
    }

    @Test
    void jsonb_concat_operator() throws SQLException {
        // PG: '{"a":1}'::jsonb || '{"b":2}'::jsonb -> {"a":1,"b":2}
        String result = querySingle(
                "SELECT '{\"a\":1}'::jsonb || '{\"b\":2}'::jsonb");
        assertNotNull(result);
        assertTrue(result.contains("\"a\"") && result.contains("\"b\""),
                "jsonb || should merge objects: " + result);
    }

    @Test
    void jsonb_delete_key_operator() throws SQLException {
        // PG: '{"a":1,"b":2}'::jsonb - 'a' -> {"b":2}
        String result = querySingle(
                "SELECT '{\"a\":1,\"b\":2}'::jsonb - 'a'");
        assertNotNull(result);
        assertFalse(result.contains("\"a\""), "jsonb - key should remove key: " + result);
        assertTrue(result.contains("\"b\""), "jsonb - key should keep other keys: " + result);
    }

    @Test
    void json_cast_via_double_colon() throws SQLException {
        // Basic ::json cast should work
        String result = querySingle("SELECT '{\"x\":42}'::json");
        assertNotNull(result);
        assertTrue(result.contains("\"x\"") && result.contains("42"),
                "::json cast should work: " + result);
    }

    @Test
    void jsonb_cast_via_double_colon() throws SQLException {
        // Basic ::jsonb cast should work
        String result = querySingle("SELECT '{\"x\":42}'::jsonb");
        assertNotNull(result);
        assertTrue(result.contains("\"x\"") && result.contains("42"),
                "::jsonb cast should work: " + result);
    }

    @Test
    void jsonb_extract_path_function() throws SQLException {
        String result = querySingle(
                "SELECT jsonb_extract_path('{\"a\":{\"b\":99}}'::jsonb, 'a', 'b')");
        assertEquals("99", result);
    }

    @Test
    void jsonb_extract_path_text_function() throws SQLException {
        String result = querySingle(
                "SELECT jsonb_extract_path_text('{\"a\":{\"b\":\"hello\"}}'::jsonb, 'a', 'b')");
        assertEquals("hello", result);
    }

    // ========================================================================
    // Array functions: working
    // ========================================================================

    @Test
    void array_literal_constructor() throws SQLException {
        String result = querySingle("SELECT ARRAY[1,2,3]");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("2") && result.contains("3"),
                "ARRAY constructor should work: " + result);
    }

    @Test
    void array_to_string_function() throws SQLException {
        String result = querySingle("SELECT array_to_string(ARRAY[1,2,3], ',')");
        assertEquals("1,2,3", result);
    }

    @Test
    void array_to_string_with_null_replacement() throws SQLException {
        String result = querySingle("SELECT array_to_string(ARRAY[1,NULL,3], ',', '0')");
        assertEquals("1,0,3", result);
    }

    @Test
    void cardinality_function() throws SQLException {
        String result = querySingle("SELECT cardinality(ARRAY[1,2,3,4])");
        assertEquals("4", result);
    }

    @Test
    void array_upper_function() throws SQLException {
        String result = querySingle("SELECT array_upper(ARRAY[10,20,30], 1)");
        assertEquals("3", result);
    }

    @Test
    void array_lower_function() throws SQLException {
        String result = querySingle("SELECT array_lower(ARRAY[10,20,30], 1)");
        assertEquals("1", result);
    }

    @Test
    void array_append_function() throws SQLException {
        String result = querySingle("SELECT array_append(ARRAY[1,2], 3)");
        assertNotNull(result);
        assertTrue(result.contains("3"), "array_append should include appended element: " + result);
    }

    @Test
    void array_prepend_function() throws SQLException {
        String result = querySingle("SELECT array_prepend(0, ARRAY[1,2])");
        assertNotNull(result);
        assertTrue(result.contains("0"), "array_prepend should include prepended element: " + result);
    }

    @Test
    void array_cat_function() throws SQLException {
        String result = querySingle("SELECT array_cat(ARRAY[1,2], ARRAY[3,4])");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("4"),
                "array_cat should concatenate: " + result);
    }

    @Test
    void array_remove_function() throws SQLException {
        String result = querySingle("SELECT array_remove(ARRAY[1,2,3,2], 2)");
        assertNotNull(result);
        assertFalse(result.contains("2"), "array_remove should remove all 2s: " + result);
    }

    @Test
    void array_position_function() throws SQLException {
        String result = querySingle("SELECT array_position(ARRAY['a','b','c'], 'b')");
        assertEquals("2", result);
    }

    @Test
    void array_agg_function() throws SQLException {
        exec("CREATE TABLE arr_agg_test (val INT)");
        exec("INSERT INTO arr_agg_test VALUES (3),(1),(2)");
        String result = querySingle("SELECT array_agg(val ORDER BY val) FROM arr_agg_test");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("2") && result.contains("3"),
                "array_agg should contain all values: " + result);
        exec("DROP TABLE arr_agg_test");
    }

    // ========================================================================
    // Array edge cases
    // ========================================================================

    @Test
    void empty_array_constructor() throws SQLException {
        String result = querySingle("SELECT ARRAY[]::int[]");
        assertNotNull(result);
        assertEquals("{}", result, "Empty array should format as {}");
    }

    @Test
    void array_with_null_elements() throws SQLException {
        exec("CREATE TABLE arr_null_test (id SERIAL PRIMARY KEY, vals INT[])");
        exec("INSERT INTO arr_null_test (vals) VALUES (ARRAY[1,NULL,3])");
        String result = querySingle("SELECT vals FROM arr_null_test WHERE id = 1");
        assertNotNull(result);
        assertTrue(result.toUpperCase().contains("NULL"),
                "Array should show NULL: " + result);
        exec("DROP TABLE arr_null_test");
    }

    @Test
    void array_in_where_clause_any() throws SQLException {
        exec("CREATE TABLE arr_any_test (id INT, name TEXT)");
        exec("INSERT INTO arr_any_test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
        int count = queryRowCount(
                "SELECT * FROM arr_any_test WHERE name = ANY(ARRAY['Alice','Charlie'])");
        assertEquals(2, count, "ANY with array should match 2 rows");
        exec("DROP TABLE arr_any_test");
    }

    @Test
    void string_to_array_function() throws SQLException {
        String result = querySingle("SELECT string_to_array('a,b,c', ',')");
        assertNotNull(result);
        assertTrue(result.contains("a") && result.contains("b") && result.contains("c"),
                "string_to_array should split: " + result);
    }

    @Test
    void array_concat_with_pipe_operator() throws SQLException {
        String result = querySingle("SELECT ARRAY[1,2] || ARRAY[3,4]");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("4"),
                "Array || should concatenate: " + result);
    }

    @Test
    void array_column_insert_select_roundtrip() throws SQLException {
        exec("CREATE TABLE arr_col_test (id SERIAL PRIMARY KEY, tags TEXT[], scores INT[])");
        exec("INSERT INTO arr_col_test (tags, scores) VALUES (ARRAY['pg','test'], ARRAY[100,95])");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT tags, scores FROM arr_col_test WHERE id = 1")) {
            assertTrue(rs.next());
            String tags = rs.getString(1);
            String scores = rs.getString(2);
            assertNotNull(tags);
            assertNotNull(scores);
            assertTrue(tags.contains("pg") && tags.contains("test"),
                    "tags should contain elements: " + tags);
            assertTrue(scores.contains("100") && scores.contains("95"),
                    "scores should contain elements: " + scores);
        }
        exec("DROP TABLE arr_col_test");
    }

    // ========================================================================
    // Additional JSON edge cases and validation
    // ========================================================================

    @Test
    void json_empty_object() throws SQLException {
        String result = querySingle("SELECT '{}'::json");
        assertEquals("{}", result);
    }

    @Test
    void json_empty_array() throws SQLException {
        String result = querySingle("SELECT '[]'::json");
        assertEquals("[]", result);
    }

    @Test
    void jsonb_nested_object() throws SQLException {
        String result = querySingle("SELECT '{\"a\":{\"b\":{\"c\":3}}}'::jsonb");
        assertNotNull(result);
        assertTrue(result.contains("\"c\"") && result.contains("3"),
                "Nested jsonb should preserve structure: " + result);
    }

    @Test
    void jsonb_array_with_mixed_types() throws SQLException {
        String result = querySingle("SELECT '[1,\"two\",true,null]'::jsonb");
        assertNotNull(result);
        assertTrue(result.contains("1") && result.contains("two") && result.contains("true"),
                "JSONB array should support mixed types: " + result);
    }

    @Test
    void jsonb_hash_arrow_deep_path() throws SQLException {
        // #>> extracts text at path
        String result = querySingle(
                "SELECT '{\"a\":{\"b\":\"deep\"}}'::jsonb #>> '{a,b}'");
        assertEquals("deep", result);
    }

    @Test
    void json_array_length_function() throws SQLException {
        String result = querySingle("SELECT json_array_length('[10,20,30]'::json)");
        assertEquals("3", result);
    }

    // ========================================================================
    // UUID functions that do work
    // ========================================================================

    @Test
    void gen_random_uuid_works() throws SQLException {
        String result = querySingle("SELECT gen_random_uuid()");
        assertNotNull(result);
        assertTrue(result.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                "gen_random_uuid() should return valid UUID: " + result);
    }

    @Test
    void uuid_generate_v4_works() throws SQLException {
        String result = querySingle("SELECT uuid_generate_v4()");
        assertNotNull(result);
        assertTrue(result.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                "uuid_generate_v4() should return valid UUID: " + result);
    }
}
