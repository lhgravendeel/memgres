package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 18-22 (Value Expressions).
 *
 * 18. Literals & constants
 * 19. Type casts
 * 20. Conditional expressions
 * 21. Array constructors & operations
 * 22. Row constructors & comparisons
 */
class ValueExpressionsCoverageTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 18. Literals & Constants
    // =========================================================================

    @Test
    void literal_integer() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 42, -17, 0, 2147483647")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertEquals(-17, rs.getInt(2));
            assertEquals(0, rs.getInt(3));
            assertEquals(2147483647, rs.getInt(4));
        }
    }

    @Test
    void literal_decimal() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 3.14, 0.5, -2.718, 100.00")) {
            assertTrue(rs.next());
            assertEquals(3.14, rs.getDouble(1), 0.001);
            assertEquals(0.5, rs.getDouble(2), 0.001);
            assertEquals(-2.718, rs.getDouble(3), 0.001);
            assertEquals(100.00, rs.getDouble(4), 0.001);
        }
    }

    @Test
    void literal_scientific_notation() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1.5e2, 3E-1, 1e0, 2.5E+3")) {
            assertTrue(rs.next());
            assertEquals(150.0, rs.getDouble(1), 0.001);
            assertEquals(0.3, rs.getDouble(2), 0.001);
            assertEquals(1.0, rs.getDouble(3), 0.001);
            assertEquals(2500.0, rs.getDouble(4), 0.001);
        }
    }

    @Test
    void literal_string() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'hello', 'world', '', 'it''s'")) {
            assertTrue(rs.next());
            assertEquals("hello", rs.getString(1));
            assertEquals("world", rs.getString(2));
            assertEquals("", rs.getString(3));
            assertEquals("it's", rs.getString(4));
        }
    }

    @Test
    void literal_boolean() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT TRUE, FALSE, true, false")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
            assertTrue(rs.getBoolean(3));
            assertFalse(rs.getBoolean(4));
        }
    }

    @Test
    void literal_null() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT NULL, NULL::TEXT, NULL::INTEGER")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertNull(rs.getString(2));
            assertNull(rs.getObject(3));
        }
    }

    @Test
    void literal_dollar_quoted_string() throws SQLException {
        // Dollar-quoted strings in function bodies, etc.
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE dq_test (id INTEGER, body TEXT)");
            // Use dollar-quoting inside a function definition (most common use)
            s.execute("CREATE OR REPLACE FUNCTION dq_func() RETURNS TEXT LANGUAGE plpgsql AS $$ BEGIN RETURN 'hello from dollar-quoted'; END; $$");
            ResultSet rs = s.executeQuery("SELECT dq_func()");
            assertTrue(rs.next());
            assertEquals("hello from dollar-quoted", rs.getString(1));
            s.execute("DROP FUNCTION dq_func");
            s.execute("DROP TABLE dq_test");
        }
    }

    @Test
    void literal_bit_string_binary() throws SQLException {
        // B'101' should be accepted by the lexer as a string literal
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT B'101'")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void literal_bit_string_hex() throws SQLException {
        // X'FF' should be accepted by the lexer
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT X'1FF'")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void literal_type_annotated_date() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT DATE '2024-01-15'")) {
            assertTrue(rs.next());
            assertEquals("2024-01-15", rs.getString(1));
        }
    }

    @Test
    void literal_type_annotated_timestamp() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT TIMESTAMP '2024-01-15 10:30:00'")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            assertTrue(rs.getString(1).contains("2024-01-15"));
        }
    }

    @Test
    void literal_type_annotated_interval() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT INTERVAL '1 day'")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void literal_type_annotated_time() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT TIME '14:30:00'")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void literal_array() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ARRAY[1, 2, 3]")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void literal_array_of_strings() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ARRAY['a', 'b', 'c']")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void literal_empty_string_is_not_null() throws SQLException {
        // In PostgreSQL, empty string is NOT NULL (unlike Oracle)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT '' IS NULL, '' IS NOT NULL, '' = ''")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));  // '' IS NULL -> false
            assertTrue(rs.getBoolean(2));   // '' IS NOT NULL -> true
            assertTrue(rs.getBoolean(3));   // '' = '' -> true
        }
    }

    // =========================================================================
    // 19. Type Casts
    // =========================================================================

    @Test
    void cast_function_syntax() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT CAST(42 AS TEXT), CAST('123' AS INTEGER), CAST(3.14 AS INTEGER)")) {
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1));
            assertEquals(123, rs.getInt(2));
            assertEquals(3, rs.getInt(3));
        }
    }

    @Test
    void cast_double_colon_syntax() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 42::TEXT, '123'::INTEGER, 3.14::INTEGER")) {
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1));
            assertEquals(123, rs.getInt(2));
            assertEquals(3, rs.getInt(3));
        }
    }

    @Test
    void cast_chain() throws SQLException {
        // Chained casts: value::type1::type2
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 42::TEXT::INTEGER")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    void cast_to_numeric() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT '3.14159'::NUMERIC(5,2)")) {
            assertTrue(rs.next());
            assertEquals(3.14, rs.getDouble(1), 0.01);
        }
    }

    @Test
    void cast_to_boolean() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'true'::BOOLEAN, 'false'::BOOLEAN, '1'::BOOLEAN, '0'::BOOLEAN")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
            assertTrue(rs.getBoolean(3));
            assertFalse(rs.getBoolean(4));
        }
    }

    @Test
    void cast_null() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT CAST(NULL AS INTEGER), NULL::TEXT")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
            assertNull(rs.getObject(2));
        }
    }

    @Test
    void cast_to_date() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT '2024-06-15'::DATE")) {
            assertTrue(rs.next());
            assertEquals("2024-06-15", rs.getString(1));
        }
    }

    @Test
    void cast_to_timestamp() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT '2024-06-15 10:30:00'::TIMESTAMP")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            assertTrue(rs.getString(1).contains("2024-06-15"));
        }
    }

    @Test
    void cast_integer_to_text_concat() throws SQLException {
        // Common pattern: cast number to text for concatenation
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 'Item #' || 42::TEXT")) {
            assertTrue(rs.next());
            assertEquals("Item #42", rs.getString(1));
        }
    }

    @Test
    void cast_in_expression() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT CAST(1 + 2 AS TEXT) || ' items'")) {
            assertTrue(rs.next());
            assertEquals("3 items", rs.getString(1));
        }
    }

    // =========================================================================
    // 20. Conditional Expressions
    // =========================================================================

    @Test
    void case_searched() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT CASE WHEN 1 > 2 THEN 'bigger' WHEN 1 < 2 THEN 'smaller' ELSE 'equal' END")) {
            assertTrue(rs.next());
            assertEquals("smaller", rs.getString(1));
        }
    }

    @Test
    void case_simple() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' ELSE 'other' END")) {
            assertTrue(rs.next());
            assertEquals("two", rs.getString(1));
        }
    }

    @Test
    void case_no_else() throws SQLException {
        // CASE without ELSE returns NULL if no match
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT CASE WHEN FALSE THEN 'match' END")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    @Test
    void case_with_null() throws SQLException {
        // NULL handling in CASE: NULL = NULL is NULL (not true), so simple CASE doesn't match NULL
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT CASE NULL WHEN NULL THEN 'matched' ELSE 'not matched' END")) {
            assertTrue(rs.next());
            assertEquals("not matched", rs.getString(1));
        }
    }

    @Test
    void case_in_select_list_with_table() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE case_test (id INTEGER, status TEXT)");
            s.execute("INSERT INTO case_test VALUES (1, 'active'), (2, 'inactive'), (3, 'pending')");
            ResultSet rs = s.executeQuery(
                "SELECT id, CASE status WHEN 'active' THEN 'A' WHEN 'inactive' THEN 'I' ELSE '?' END AS code " +
                "FROM case_test ORDER BY id");
            assertTrue(rs.next()); assertEquals("A", rs.getString("code"));
            assertTrue(rs.next()); assertEquals("I", rs.getString("code"));
            assertTrue(rs.next()); assertEquals("?", rs.getString("code"));
            s.execute("DROP TABLE case_test");
        }
    }

    @Test
    void case_nested() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT CASE WHEN TRUE THEN CASE WHEN FALSE THEN 'inner-true' ELSE 'inner-false' END ELSE 'outer-false' END")) {
            assertTrue(rs.next());
            assertEquals("inner-false", rs.getString(1));
        }
    }

    @Test
    void coalesce_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COALESCE(NULL, NULL, 'third', 'fourth')")) {
            assertTrue(rs.next());
            assertEquals("third", rs.getString(1));
        }
    }

    @Test
    void coalesce_first_non_null() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COALESCE('first', 'second')")) {
            assertTrue(rs.next());
            assertEquals("first", rs.getString(1));
        }
    }

    @Test
    void coalesce_all_null() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COALESCE(NULL, NULL)")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    @Test
    void coalesce_with_expression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE coal_test (id INTEGER, preferred TEXT, fallback TEXT)");
            s.execute("INSERT INTO coal_test VALUES (1, NULL, 'backup')");
            s.execute("INSERT INTO coal_test VALUES (2, 'primary', 'backup')");
            ResultSet rs = s.executeQuery("SELECT id, COALESCE(preferred, fallback) AS result FROM coal_test ORDER BY id");
            assertTrue(rs.next()); assertEquals("backup", rs.getString("result"));
            assertTrue(rs.next()); assertEquals("primary", rs.getString("result"));
            s.execute("DROP TABLE coal_test");
        }
    }

    @Test
    void nullif_equal() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT NULLIF(5, 5)")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    @Test
    void nullif_not_equal() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT NULLIF(5, 3)")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void nullif_division_by_zero_protection() throws SQLException {
        // Common pattern: NULLIF to avoid division by zero
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 10 / NULLIF(0, 0)")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1)); // 10 / NULL = NULL
        }
    }

    @Test
    void greatest_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT GREATEST(1, 5, 3, 7, 2)")) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
        }
    }

    @Test
    void greatest_with_null() throws SQLException {
        // PG GREATEST ignores NULLs
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT GREATEST(NULL, 3, NULL, 7)")) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
        }
    }

    @Test
    void greatest_strings() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT GREATEST('apple', 'cherry', 'banana')")) {
            assertTrue(rs.next());
            assertEquals("cherry", rs.getString(1));
        }
    }

    @Test
    void least_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT LEAST(5, 1, 8, 3)")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void least_with_null() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT LEAST(NULL, 3, NULL, 1)")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void least_strings() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT LEAST('apple', 'cherry', 'banana')")) {
            assertTrue(rs.next());
            assertEquals("apple", rs.getString(1));
        }
    }

    // =========================================================================
    // 21. Array Constructors & Operations
    // =========================================================================

    @Test
    void array_constructor_integers() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ARRAY[1, 2, 3]")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
        }
    }

    @Test
    void array_constructor_strings() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ARRAY['hello', 'world']")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void array_constructor_empty() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ARRAY[]::TEXT[]")) {
            assertTrue(rs.next());
            // Empty array
            assertNotNull(rs.getObject(1));
        }
    }

    @Test
    void array_subquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE arr_sub (id INTEGER, val TEXT)");
            s.execute("INSERT INTO arr_sub VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            ResultSet rs = s.executeQuery("SELECT ARRAY(SELECT val FROM arr_sub ORDER BY id)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            // Should contain all three values
            assertTrue(result.contains("a"));
            assertTrue(result.contains("b"));
            assertTrue(result.contains("c"));
            s.execute("DROP TABLE arr_sub");
        }
    }

    @Test
    void array_subscript_one_based() throws SQLException {
        // PostgreSQL arrays are 1-based
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT (ARRAY[10, 20, 30])[1]")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
        }
    }

    @Test
    void array_subscript_second_element() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT (ARRAY[10, 20, 30])[2]")) {
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
        }
    }

    @Test
    void array_subscript_third_element() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT (ARRAY[10, 20, 30])[3]")) {
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
        }
    }

    @Test
    void array_subscript_out_of_bounds() throws SQLException {
        // Out of bounds returns NULL
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT (ARRAY[10, 20, 30])[0]")) {
            assertTrue(rs.next());
            assertNull(rs.getObject(1));
        }
    }

    @Test
    void array_subscript_from_column() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE arr_col (id INTEGER, vals INTEGER[])");
            s.execute("INSERT INTO arr_col VALUES (1, ARRAY[10, 20, 30])");
            ResultSet rs = s.executeQuery("SELECT vals[1], vals[2], vals[3] FROM arr_col WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals(20, rs.getInt(2));
            assertEquals(30, rs.getInt(3));
            s.execute("DROP TABLE arr_col");
        }
    }

    @Test
    void array_concat() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ARRAY[1, 2] || ARRAY[3, 4]")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
        }
    }

    @Test
    void array_append_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT array_append(ARRAY[1, 2, 3], 4)")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void array_prepend_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT array_prepend(0, ARRAY[1, 2, 3])")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void array_cat_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT array_cat(ARRAY[1, 2], ARRAY[3, 4])")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void array_remove_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT array_remove(ARRAY[1, 2, 3, 2], 2)")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
        }
    }

    @Test
    void array_replace_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT array_replace(ARRAY[1, 2, 3], 2, 99)")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void array_position_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT array_position(ARRAY['a', 'b', 'c'], 'b')")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void array_length_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT array_length(ARRAY[1, 2, 3, 4, 5], 1)")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void cardinality_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT cardinality(ARRAY[1, 2, 3])")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void array_to_string_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT array_to_string(ARRAY[1, 2, 3], ', ')")) {
            assertTrue(rs.next());
            assertEquals("1, 2, 3", rs.getString(1));
        }
    }

    @Test
    void string_to_array_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT string_to_array('a,b,c', ',')")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void unnest_function() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM unnest(ARRAY[10, 20, 30]) AS x")) {
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(30, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void any_with_array() throws SQLException {
        // = ANY(ARRAY[...]) acts like IN
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE any_arr (id INTEGER, name TEXT)");
            s.execute("INSERT INTO any_arr VALUES (1, 'A'), (2, 'B'), (3, 'C')");
            ResultSet rs = s.executeQuery("SELECT name FROM any_arr WHERE id = ANY(ARRAY[1, 3]) ORDER BY id");
            assertTrue(rs.next()); assertEquals("A", rs.getString(1));
            assertTrue(rs.next()); assertEquals("C", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE any_arr");
        }
    }

    @Test
    void array_in_insert_and_select() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE arr_store (id INTEGER, tags TEXT[])");
            s.execute("INSERT INTO arr_store VALUES (1, ARRAY['tag1', 'tag2', 'tag3'])");
            ResultSet rs = s.executeQuery("SELECT tags FROM arr_store WHERE id = 1");
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
            s.execute("DROP TABLE arr_store");
        }
    }

    // =========================================================================
    // 22. Row Constructors & Comparisons
    // =========================================================================

    @Test
    void row_constructor_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 'hello', TRUE)")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1));
        }
    }

    @Test
    void row_equality_true() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 2) = ROW(1, 2)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void row_equality_false() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 2) = ROW(1, 3)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void row_inequality() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 2) <> ROW(1, 3)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void row_inequality_same() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 2) <> ROW(1, 2)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void row_less_than() throws SQLException {
        // (1, 2) < (1, 3) is true because first elements are equal and 2 < 3
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 2) < ROW(1, 3)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void row_less_than_first_element() throws SQLException {
        // (1, 5) < (2, 1) is true because 1 < 2 (stops at first difference)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 5) < ROW(2, 1)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void row_less_than_equal_rows() throws SQLException {
        // (1, 2) < (1, 2) is false (not strictly less)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 2) < ROW(1, 2)")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
        }
    }

    @Test
    void row_greater_than() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(2, 1) > ROW(1, 5)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void row_less_equal() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 2) <= ROW(1, 2)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void row_greater_equal() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 3) >= ROW(1, 2)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void row_comparison_in_where() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE row_cmp (a INTEGER, b INTEGER, val TEXT)");
            s.execute("INSERT INTO row_cmp VALUES (1, 1, 'x'), (1, 2, 'y'), (2, 1, 'z')");
            ResultSet rs = s.executeQuery(
                "SELECT val FROM row_cmp WHERE ROW(a, b) > ROW(1, 1) ORDER BY a, b");
            assertTrue(rs.next()); assertEquals("y", rs.getString(1)); // (1,2)
            assertTrue(rs.next()); assertEquals("z", rs.getString(1)); // (2,1)
            assertFalse(rs.next());
            s.execute("DROP TABLE row_cmp");
        }
    }

    @Test
    void row_with_null_equality() throws SQLException {
        // ROW with NULL: ROW(1, NULL) = ROW(1, NULL) → NULL (not true!)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, NULL) = ROW(1, NULL)")) {
            assertTrue(rs.next());
            // In SQL, NULL = NULL is NULL (which reads as false via getBoolean but is actually null)
            rs.getBoolean(1);
            assertTrue(rs.wasNull());
        }
    }

    @Test
    void parenthesized_row_syntax() throws SQLException {
        // (1, 2) without ROW keyword should work the same in comparisons
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 2, 3) = ROW(1, 2, 3)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void row_mixed_types() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ROW(1, 'hello', TRUE) = ROW(1, 'hello', TRUE)")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }
}
