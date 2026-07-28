package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A JSON function given the wrong container used to answer with a plausible value — 0 for the
 * length of an object, NULL for the keys of an array, the input unchanged for a path set in a
 * scalar. PostgreSQL names the container it wanted instead, and a caller cannot tell a wrong
 * answer from a right one.
 *
 * <p>Also covers the string escapes jsonb cannot represent, and the arithmetic and error
 * suppression a jsonpath is expected to perform.
 */
class JsonContainerErrorsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE jce_t (id int, j jsonb)");
        exec("INSERT INTO jce_t VALUES (1, '{\"b\":1,\"aa\":2}'), (2, '\"scalar\"'), (3, '[1,2]')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static List<String> column(String sql) throws SQLException {
        List<String> values = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) values.add(rs.getString(1));
        }
        return values;
    }

    private static void assertFails(String state, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(state, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- containers of the wrong shape ----

    @Test
    void theLengthOfSomethingThatIsNotAnArrayIsAnError() {
        assertFails("22023", "cannot get array length of a non-array",
                "SELECT jsonb_array_length('{\"a\":1}'::jsonb)");
        assertFails("22023", "cannot get array length of a scalar",
                "SELECT jsonb_array_length('3'::jsonb)");
        assertFails("22023", "cannot get array length of a scalar",
                "SELECT jsonb_array_length('null'::jsonb)");
        assertFails("22023", "cannot get array length of a non-array",
                "SELECT json_array_length('{\"a\":1}'::json)");
        assertFails("22023", "cannot get array length of a scalar",
                "SELECT json_array_length('3'::json)");
    }

    @Test
    void theKeysOfSomethingThatIsNotAnObjectAreAnError() {
        assertFails("22023", "cannot call jsonb_object_keys on an array",
                "SELECT jsonb_object_keys('[1,2]'::jsonb)");
        assertFails("22023", "cannot call jsonb_object_keys on a scalar",
                "SELECT jsonb_object_keys('3'::jsonb)");
        assertFails("22023", "cannot call json_object_keys on an array",
                "SELECT json_object_keys('[1,2]'::json)");
        assertFails("22023", "cannot call jsonb_object_keys on an array",
                "SELECT * FROM jsonb_object_keys('[1]'::jsonb)");
    }

    @Test
    void theElementsOfSomethingThatIsNotAnArrayAreAnError() {
        assertFails("22023", "cannot extract elements from an object",
                "SELECT jsonb_array_elements('{\"a\":1}'::jsonb)");
        assertFails("22023", "cannot extract elements from a scalar",
                "SELECT jsonb_array_elements('3'::jsonb)");
        assertFails("22023", "cannot extract elements from an object",
                "SELECT jsonb_array_elements_text('{\"a\":1}'::jsonb)");
        assertFails("22023", "cannot call json_array_elements on a non-array",
                "SELECT json_array_elements('{\"a\":1}'::json)");
        assertFails("22023", "cannot call json_array_elements_text on a scalar",
                "SELECT json_array_elements_text('3'::json)");
        assertFails("22023", "cannot extract elements from an object",
                "SELECT * FROM jsonb_array_elements('{\"a\":1}'::jsonb)");
    }

    @Test
    void deconstructingSomethingThatIsNotAnObjectIsAnError() {
        assertFails("22023", "cannot call jsonb_each on a non-object",
                "SELECT * FROM jsonb_each('[1,2]'::jsonb)");
        assertFails("22023", "cannot call jsonb_each on a non-object",
                "SELECT * FROM jsonb_each('\"x\"'::jsonb)");
        assertFails("22023", "cannot call jsonb_each_text on a non-object",
                "SELECT * FROM jsonb_each_text('3'::jsonb)");
        assertFails("22023", "cannot deconstruct an array as an object",
                "SELECT * FROM json_each('[1,2]'::json)");
        assertFails("22023", "cannot deconstruct a scalar",
                "SELECT * FROM json_each('3'::json)");
        assertFails("22023", "cannot deconstruct an array as an object",
                "SELECT * FROM json_each_text('[1,2]'::json)");
    }

    @Test
    void theRightContainerStillAnswers() throws Exception {
        assertEquals("3", scalar("SELECT jsonb_array_length('[1,2,3]'::jsonb)"));
        assertEquals("0", scalar("SELECT jsonb_array_length('[]'::jsonb)"));
        // a comma inside a string is not an element separator
        assertEquals("1", scalar("SELECT jsonb_array_length('[\"a,b\"]'::jsonb)"));
        assertEquals("3", scalar("SELECT jsonb_array_length('[1,[2,3],4]'::jsonb)"));
        assertEquals("2", scalar("SELECT jsonb_array_length(j) FROM jce_t WHERE id = 3"));
        assertEquals(List.of("a", "b"), column("SELECT jsonb_object_keys('{\"a\":1,\"b\":2}'::jsonb)"));
        assertEquals(List.of("a", "bb"), column("SELECT * FROM jsonb_object_keys('{\"bb\":1,\"a\":2}'::jsonb)"));
        assertEquals(List.of("{\"a\": 1}", "2"), column("SELECT * FROM jsonb_array_elements('[{\"a\":1},2]'::jsonb)"));
        // a JSON null becomes a SQL NULL in the text form
        List<String> texts = column("SELECT * FROM jsonb_array_elements_text('[null,\"a\",1]'::jsonb)");
        assertEquals(3, texts.size());
        assertNull(texts.get(0));
        assertEquals("a", texts.get(1));
        assertEquals(List.of("a"), column("SELECT * FROM jsonb_each('{\"a\":1}'::jsonb)"));
    }

    @Test
    void aSetReturningFunctionGivenNullProducesNoRows() throws Exception {
        assertEquals(List.of(), column("SELECT * FROM jsonb_array_elements(NULL::jsonb)"));
        assertEquals(List.of(), column("SELECT * FROM jsonb_object_keys(NULL::jsonb)"));
        assertEquals(List.of(), column("SELECT * FROM jsonb_each(NULL::jsonb)"));
        // the scalar functions stay strict
        assertNull(scalar("SELECT jsonb_array_length(NULL::jsonb)"));
    }

    // ---- deleting ----

    @Test
    void deletingFromTheWrongContainerIsAnError() {
        assertFails("22023", "cannot delete from object using integer index",
                "SELECT '{\"a\":1}'::jsonb - 0");
        assertFails("22023", "cannot delete from scalar", "SELECT '\"x\"'::jsonb - 'a'");
        assertFails("22023", "cannot delete from scalar", "SELECT '3'::jsonb - 'a'");
        assertFails("22023", "cannot delete from scalar", "SELECT '3'::jsonb - '{a}'::text[]");
        assertFails("22023", "cannot delete from scalar", "SELECT j - 'x' FROM jce_t WHERE id = 2");
        assertFails("22023", "cannot delete path in scalar", "SELECT '3'::jsonb #- '{a}'");
        assertFails("22P02", "path element at position 1 is not an integer: \"a\"",
                "SELECT '[1,2]'::jsonb #- '{a}'");
        assertFails("22P02", "path element at position 2 is not an integer: \"x\"",
                "SELECT '{\"a\":[1]}'::jsonb #- '{a,x}'");
    }

    @Test
    void deletionsThatDoApplyStillWork() throws Exception {
        assertEquals("[2]", scalar("SELECT '[1,2]'::jsonb - 0"));
        assertEquals("[1, 2]", scalar("SELECT '[1,2]'::jsonb - 5"));
        assertEquals("[1]", scalar("SELECT '[1,2]'::jsonb - (-1)"));
        assertEquals("{}", scalar("SELECT '{\"a\":1}'::jsonb - 'a'"));
        assertEquals("[\"b\"]", scalar("SELECT '[\"a\",\"b\"]'::jsonb - 'a'"));
        // a text[] deletes keys, so an array only loses matching string elements
        assertEquals("[\"b\"]", scalar("SELECT '[\"a\",\"b\"]'::jsonb - '{a}'::text[]"));
        assertEquals("[1, 2]", scalar("SELECT '[1,2]'::jsonb - '{0}'::text[]"));
        assertEquals("{}", scalar("SELECT '{\"a\":1,\"b\":2}'::jsonb - '{a,b}'::text[]"));
        assertEquals("[1, 3]", scalar("SELECT '[1,2,3]'::jsonb #- '{1}'"));
        assertEquals("{\"a\": {}}", scalar("SELECT '{\"a\":{\"b\":1}}'::jsonb #- '{a,b}'"));
        // a step that runs into a scalar leaves the value alone rather than raising
        assertEquals("{\"a\": 1}", scalar("SELECT '{\"a\":1}'::jsonb #- '{a,0}'"));
        assertEquals("{\"a\": \"s\"}", scalar("SELECT '{\"a\":\"s\"}'::jsonb #- '{a,b}'"));
        assertEquals("{\"a\": 1}", scalar("SELECT '{\"a\":1}'::jsonb #- '{}'"));
    }

    // ---- setting a path ----

    @Test
    void settingAPathInAScalarIsAnError() {
        assertFails("22023", "cannot set path in scalar", "SELECT jsonb_set('1'::jsonb, '{a}', '2')");
        assertFails("22023", "cannot set path in scalar", "SELECT jsonb_set('\"s\"'::jsonb, '{a}', '2')");
        assertFails("22023", "cannot set path in scalar", "SELECT jsonb_set('null'::jsonb, '{a}', '2')");
        assertFails("22023", "cannot set path in scalar", "SELECT jsonb_insert('1'::jsonb, '{a}', '2')");
        assertFails("22023", "cannot set path in scalar", "SELECT jsonb_insert('\"x\"'::jsonb, '{0}', '9')");
    }

    @Test
    void aPathStepIntoAnArrayHasToBeAnInteger() {
        assertFails("22P02", "path element at position 1 is not an integer: \"a\"",
                "SELECT jsonb_set('[1,2]'::jsonb, '{a}', '9')");
        assertFails("22P02", "path element at position 2 is not an integer: \"x\"",
                "SELECT jsonb_set('{\"a\":[1,2]}'::jsonb, '{a,x}', '9')");
        assertFails("22P02", "path element at position 2 is not an integer: \"x\"",
                "SELECT jsonb_set('[[1]]'::jsonb, '{0,x}', '9')");
        assertFails("22P02", "path element at position 1 is not an integer: \"x\"",
                "SELECT jsonb_insert('[1,2]'::jsonb, '{x}', '9')");
        assertFails("22P02", "path element at position 2 is not an integer: \"x\"",
                "SELECT jsonb_insert('{\"a\":[1]}'::jsonb, '{a,x}', '9')");
    }

    @Test
    void pathsThatDoApplyStillWork() throws Exception {
        assertEquals("[9, 2]", scalar("SELECT jsonb_set('[1,2]'::jsonb, '{0}', '9')"));
        assertEquals("{\"a\": 9}", scalar("SELECT jsonb_set('{\"a\":1}'::jsonb, '{a}', '9')"));
        assertEquals("{\"a\": [1, 9]}", scalar("SELECT jsonb_set('{\"a\":[1,2]}'::jsonb, '{a,1}', '9')"));
        assertEquals("{\"a\": {\"b\": [\"z\", 2]}}",
                scalar("SELECT jsonb_set('{\"a\":{\"b\":[1,2]}}'::jsonb, '{a,b,0}', '\"z\"')"));
        assertEquals("{\"a\": 1}", scalar("SELECT jsonb_set('{\"a\":1}'::jsonb, '{}', '9')"));
        assertEquals("{\"a\": 1}", scalar("SELECT jsonb_set('{\"a\":1}'::jsonb, '{b}', '2', false)"));
        assertEquals("{\"a\": 1, \"b\": 2}", scalar("SELECT jsonb_set('{\"a\":1}'::jsonb, '{b}', '2', true)"));
        // a missing or scalar intermediate step leaves the target unchanged
        assertEquals("{\"a\": 1}", scalar("SELECT jsonb_set('{\"a\":1}'::jsonb, '{b,c}', '9')"));
        assertEquals("{\"a\": \"s\"}", scalar("SELECT jsonb_set('{\"a\":\"s\"}'::jsonb, '{a,b}', '9')"));
        assertEquals("{\"a\": [1, 9]}", scalar("SELECT jsonb_set('{\"a\":[1]}'::jsonb, '{a,5}', '9')"));
        assertEquals("[1, 9, 2]", scalar("SELECT jsonb_insert('[1,2]'::jsonb, '{1}', '9')"));
        assertEquals("[1, 2, 9]", scalar("SELECT jsonb_insert('[1,2]'::jsonb, '{1}', '9', true)"));
        assertEquals("{\"a\": [9, 1, 2]}", scalar("SELECT jsonb_insert('{\"a\":[1,2]}'::jsonb, '{a,0}', '9')"));
        assertFails("22023", "cannot replace existing key", "SELECT jsonb_insert('{\"a\":1}'::jsonb, '{a}', '9')");
    }

    // ---- building an object ----

    @Test
    void argumentsThatDoNotPairUpAreAnError() {
        assertFails("22023", "argument list must have even number of elements",
                "SELECT json_build_object('a')");
        assertFails("22023", "argument list must have even number of elements",
                "SELECT jsonb_build_object('a')");
        // the two families report a null key differently, down to the SQLSTATE
        assertFails("22004", "null value not allowed for object key",
                "SELECT json_build_object(NULL, 1)");
        assertFails("22023", "argument 1: key must not be null",
                "SELECT jsonb_build_object(NULL, 1)");
        assertFails("22023", "argument 3: key must not be null",
                "SELECT jsonb_build_object('a', 1, NULL, 2)");
        assertFails("2202E", "array must have even number of elements", "SELECT json_object('{a}')");
        assertFails("2202E", "array must have even number of elements", "SELECT jsonb_object('{a}')");
    }

    @Test
    void argumentsThatDoPairUpBuildTheObject() throws Exception {
        assertEquals("{}", scalar("SELECT json_build_object()"));
        // json prints the object the way its own text output does
        assertEquals("{\"a\" : 1, \"b\" : 2}", scalar("SELECT json_build_object('a', 1, 'b', 2)"));
        assertEquals("{\"a\" : 1, \"b\" : null}", scalar("SELECT json_build_object('a', 1, 'b', NULL)"));
        assertEquals("{\"a\": 1}", scalar("SELECT jsonb_build_object('a', 1)"));
        assertEquals("[1, \"a\"]", scalar("SELECT json_build_array(1, 'a')"));
        // json_object(text[]) is the older function form, not the SQL/JSON constructor
        assertEquals("{\"a\" : \"1\", \"b\" : \"2\"}", scalar("SELECT json_object('{a,1,b,2}')"));
        assertEquals("{\"a\" : \"1\", \"b\" : \"2\"}", scalar("SELECT json_object('{a,b}', '{1,2}')"));
        assertEquals("{\"a\": \"1\", \"b\": \"2\"}", scalar("SELECT jsonb_object('{a,1,b,2}')"));
        assertEquals("{ \"a\" : 1, \"b\" : 2 }",
                scalar("SELECT json_object_agg(k, v) FROM (SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 2) t"));
    }

    // ---- string escapes ----

    @Test
    void jsonbRefusesTheEscapesItCannotRepresent() {
        assertFails("22P05", "unsupported Unicode escape sequence", "SELECT '\"\\u0000\"'::jsonb");
        assertFails("22P05", "unsupported Unicode escape sequence", "SELECT '{\"a\": \"\\u0000\"}'::jsonb");
        assertFails("22P05", "unsupported Unicode escape sequence",
                "INSERT INTO jce_t VALUES (4, '\"\\u0000\"')");
        // half a surrogate pair names no character
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"\\ud834\"'::jsonb");
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"\\udd1e\"'::jsonb");
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"\\ud834A\"'::jsonb");
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"\\ud834\\u0041\"'::jsonb");
        assertFails("22P02", "invalid input syntax for type json",
                "INSERT INTO jce_t VALUES (5, '\"\\ud834\"')");
        // and the escapes that are not escapes at all
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"\\u\"'::jsonb");
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"\\u12\"'::jsonb");
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"\\q\"'::jsonb");
    }

    @Test
    void jsonKeepsTheTextItWasGiven() throws Exception {
        // only jsonb has to decode, so json takes escapes it could not represent
        assertEquals("\"\\u0000\"", scalar("SELECT '\"\\u0000\"'::json"));
        assertEquals("\"\\ud834\"", scalar("SELECT '\"\\ud834\"'::json"));
        assertEquals("\"\\u0041\"", scalar("SELECT '\"\\u0041\"'::json"));
        // but a malformed escape is still not JSON
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"\\q\"'::json");
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"\\u12\"'::json");
    }

    @Test
    void jsonbStoresTheDecodedStringAndEscapesItAgainOnTheWayOut() throws Exception {
        assertEquals("\"A\"", scalar("SELECT '\"\\u0041\"'::jsonb"));
        assertEquals("\"\u00e9\"", scalar("SELECT '\"\\u00e9\"'::jsonb"));
        assertEquals("\"\ud834\udd1e\"", scalar("SELECT '\"\\ud834\\udd1e\"'::jsonb"));
        assertEquals("\"a/b\"", scalar("SELECT '\"a\\/b\"'::jsonb"));
        assertEquals("\"a\\nb\"", scalar("SELECT '\"a\\nb\"'::jsonb"));
        // a backslash that is properly escaped is a value, not a broken escape
        assertEquals("\"a\\\\b\"", scalar("SELECT '\"a\\\\b\"'::jsonb"));
        assertEquals("[\"a\\\\\"]", scalar("SELECT '[\"a\\\\\"]'::jsonb"));
        assertEquals("a\\", scalar("SELECT '[\"a\\\\\"]'::jsonb ->> 0"));
        assertEquals("{\"a\\\\\": 1}", scalar("SELECT '{\"a\\\\\":1}'::jsonb"));
        assertEquals("{\"a\\\\b\": 1}", scalar("SELECT '{\"a\\\\b\":1}'::jsonb"));
        // and the key can be found again by the text it decodes to
        assertEquals("1", scalar("SELECT '{\"a\\\\b\":1}'::jsonb -> 'a\\b'"));
        assertEquals("\"a\\\\b\"", scalar("SELECT to_jsonb('a\\b'::text)"));
        assertEquals("[\n    \"a\\\\\"\n]", scalar("SELECT jsonb_pretty('[\"a\\\\\"]'::jsonb)"));
        // a raw control character is still not allowed inside a string
        assertFails("22P02", "invalid input syntax for type json", "SELECT '\"a\nb\"'::jsonb");
    }

    // ---- jsonpath arithmetic ----

    @Test
    void arithmeticAroundAPathIsEvaluated() throws Exception {
        assertEquals("5", scalar("SELECT jsonb_path_query('[2]', '$[0] + 3')"));
        assertEquals("-1", scalar("SELECT jsonb_path_query('[2]', '$[0] - 3')"));
        assertEquals("6", scalar("SELECT jsonb_path_query('[2]', '$[0] * 3')"));
        assertEquals("1", scalar("SELECT jsonb_path_query('[7]', '$[0] % 3')"));
        assertEquals("-2", scalar("SELECT jsonb_path_query('[-10]', '$[0] % 4')"));
        // an operand may come before the path
        assertEquals("5", scalar("SELECT jsonb_path_query('[2]', '7 - $[0]')"));
        assertEquals("8", scalar("SELECT jsonb_path_query('[4]', '2 * $[0]')"));
        assertEquals("-2", scalar("SELECT jsonb_path_query('[2]', '- $[0]')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('[2]', '+ $[0]')"));
        assertEquals("-1", scalar("SELECT jsonb_path_query('[1]', '-1')"));
        assertEquals("3", scalar("SELECT jsonb_path_query('[1]', '1 + 2')"));
        assertEquals("9", scalar("SELECT jsonb_path_query('[1]', '(1 + 2) * 3')"));
        assertEquals("4", scalar("SELECT jsonb_path_query('[1]', '$[0] + 1 + 2')"));
        assertEquals("3", scalar("SELECT jsonb_path_query('[1]', '$[0] + $[0] * 2')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('[1]', '$[0] - -1')"));
        assertEquals("3", scalar("SELECT jsonb_path_query('[1]', '2 - - 1')"));
        assertEquals("4", scalar("SELECT jsonb_path_query('{\"a\":2}', '$.a + $.a')"));
        assertEquals("5", scalar("SELECT jsonb_path_query('{\"x\":2}', '2 * $.x + 1')"));
        assertEquals("5", scalar("SELECT jsonb_path_query('{\"x\":2}', '$.x + $y', '{\"y\": 3}')"));
        // decimals keep the scale PG's numeric arithmetic gives them
        assertEquals("2.5", scalar("SELECT jsonb_path_query('[1.5]', '$[0] + 1')"));
        assertEquals("2.50", scalar("SELECT jsonb_path_query('[1]', '$[0] + 1.50')"));
        assertEquals("0.3", scalar("SELECT jsonb_path_query('[0.1]', '$[0] + 0.2')"));
        assertEquals("2.0", scalar("SELECT jsonb_path_query('[7]', '$[0] % 2.5')"));
        assertEquals("20000000000", scalar("SELECT jsonb_path_query('[1e10]', '$[0] * 2')"));
    }

    @Test
    void divisionKeepsSixteenSignificantDigits() throws Exception {
        assertEquals("2.0000000000000000", scalar("SELECT jsonb_path_query('[6]', '$[0] / 3')"));
        assertEquals("5.0000000000000000", scalar("SELECT jsonb_path_query('[2]', '10 / $[0]')"));
        assertEquals("0.33333333333333333333", scalar("SELECT jsonb_path_query('[1]', '$[0] / 3')"));
        assertEquals("14.2857142857142857", scalar("SELECT jsonb_path_query('[100]', '$[0] / 7')"));
        assertEquals("0.000033333333333333333333", scalar("SELECT jsonb_path_query('[1]', '$[0] / 30000')"));
        assertEquals("0.50000000000000000000", scalar("SELECT jsonb_path_query('[1.5]', '$[0] / 3')"));
        assertEquals("3333.3333333333333333", scalar("SELECT jsonb_path_query('[10000]', '$[0] / 3')"));
    }

    @Test
    void aUnaryOperatorUnwrapsAnArrayButABinaryOneDoesNot() throws Exception {
        assertEquals("[2, 3, 4]", scalar("SELECT jsonb_path_query_array('{\"x\":[2,3,4]}', '+ $.x')"));
        assertEquals("[-2, -3, -4]", scalar("SELECT jsonb_path_query_array('{\"x\":[2,3,4]}', '- $.x')"));
        assertEquals(List.of("-1", "-2"), column("SELECT jsonb_path_query('[1,2]', '- $')"));
        assertEquals(List.of("-1", "-2"), column("SELECT jsonb_path_query('[[1,2]]', '- $[0]')"));
        // a binary operator wants exactly one numeric value on each side
        assertFails("22038", "left operand of jsonpath operator + is not a single numeric value",
                "SELECT jsonb_path_query('{\"x\":[2,3,4]}', '$.x + 1')");
        assertFails("22038", "left operand of jsonpath operator + is not a single numeric value",
                "SELECT jsonb_path_query_array('[1,2,3]', '$[*] + 1')");
        assertFails("22038", "left operand of jsonpath operator * is not a single numeric value",
                "SELECT jsonb_path_query_array('[1,2]', '$[*] * 2')");
        assertFails("22038", "left operand of jsonpath operator + is not a single numeric value",
                "SELECT jsonb_path_query('\"a\"', '$ + 1')");
        assertFails("22038", "right operand of jsonpath operator + is not a single numeric value",
                "SELECT jsonb_path_query('1', '$ + \"a\"')");
        // and a unary one wants a number, reported under its own SQLSTATE
        assertFails("2203B", "operand of unary jsonpath operator - is not a numeric value",
                "SELECT jsonb_path_query('[\"a\"]', '- $')");
        assertFails("2203B", "operand of unary jsonpath operator - is not a numeric value",
                "SELECT jsonb_path_query('{\"a\":1}', '- $')");
        assertFails("22012", "division by zero", "SELECT jsonb_path_query('[2]', '$[0] / 0')");
        assertFails("42601", "syntax error at end of jsonpath input",
                "SELECT jsonb_path_query('[1]', '$[0] + ')");
    }

    @Test
    void aPathWithNoArithmeticInItIsUntouched() throws Exception {
        assertEquals("1", scalar("SELECT jsonb_path_query('{\"a\":1}', '$.*')"));
        assertEquals(List.of("1", "2", "3"), column("SELECT jsonb_path_query('{\"a\":[1,2,3]}', '$.a[*]')"));
        assertEquals("2", scalar("SELECT jsonb_path_query('{\"a\":{\"b\":2}}', '$.a.b')"));
        assertEquals("[2, 3]", scalar("SELECT jsonb_path_query_array('{\"a\":[1,2,3]}', '$.a[*] ? (@ > 1)')"));
        assertEquals("[]", scalar("SELECT jsonb_path_query_array('[1,2]', '$[*].a')"));
        assertEquals("t", scalar("SELECT jsonb_path_exists('{\"a\":1}', '$.a')"));
        assertEquals("f", scalar("SELECT jsonb_path_exists('{\"a\":1}', '$.b')"));
        assertEquals("t", scalar("SELECT jsonb_path_match('{\"a\":1}', '$.a == 1')"));
    }

    // ---- error suppression ----

    @Test
    void theSilentOperatorsSwallowAStructuralError() throws Exception {
        assertNull(scalar("SELECT '{\"a\":[1,2,3]}'::jsonb @? 'strict $.b'"));
        assertNull(scalar("SELECT '{\"a\":[1,2,3]}'::jsonb @@ 'strict $.b == 1'"));
        assertNull(scalar("SELECT '1'::jsonb @? 'strict $[*]'"));
        assertNull(scalar("SELECT '1'::jsonb @@ 'strict $[*] == 1'"));
        assertNull(scalar("SELECT '[1,2]'::jsonb @? '$[*] + 1'"));
        assertNull(scalar("SELECT '[2]'::jsonb @? '$[0] / 0'"));
        assertNull(scalar("SELECT jsonb_path_exists('{\"a\":[1,2,3]}', 'strict $.b', '{}', true)"));
        assertNull(scalar("SELECT jsonb_path_exists('{\"a\":1}', 'strict $.a.b', '{}', true)"));
        assertNull(scalar("SELECT jsonb_path_query_first('{\"a\":[1,2,3]}', 'strict $.b', '{}', true)"));
        assertNull(scalar("SELECT jsonb_path_match('{\"a\":[1,2,3]}', 'strict $.b == 1', '{}', true)"));
        assertEquals("[]", scalar("SELECT jsonb_path_query_array('{\"a\":[1,2,3]}', 'strict $.b', '{}', true)"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('{\"a\":[1,2,3]}', 'strict $.b', '{}', true)"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('[2]', '$[0] / 0', '{}', true)"));
        assertEquals(List.of(), column("SELECT jsonb_path_query('[1,2]', '$[*] + 1', '{}', true)"));
    }

    @Test
    void withoutSuppressionTheSamePathStillRaises() throws Exception {
        assertFails("2203A", "JSON object does not contain key \"b\"",
                "SELECT jsonb_path_exists('{\"a\":[1,2,3]}', 'strict $.b')");
        assertFails("2203A", "JSON object does not contain key \"b\"",
                "SELECT jsonb_path_exists('{\"a\":[1,2,3]}', 'strict $.b', '{}', false)");
        // and a path that does match answers the same either way
        assertEquals("t", scalar("SELECT '{\"a\":[1,2,3]}'::jsonb @? '$.a'"));
        assertEquals("f", scalar("SELECT '{\"a\":[1,2,3]}'::jsonb @? 'lax $.b'"));
        assertEquals("t", scalar("SELECT '{\"a\":1}'::jsonb @@ '$.a == 1'"));
        assertEquals("f", scalar("SELECT '{\"a\":1}'::jsonb @@ '$.a == 2'"));
        assertEquals("1", scalar("SELECT jsonb_path_query('{\"a\":1}', '$.a', '{}', true)"));
        assertEquals(List.of("1", "2"), column("SELECT jsonb_path_query('{\"a\":[1,2]}', 'strict $.a[*]')"));
    }
}
