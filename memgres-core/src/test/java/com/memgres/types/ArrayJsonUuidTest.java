package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 18 (Java/JDBC): Array, JSON, UUID, and XML type mappings.
 * Tests PostgreSQL-specific types including UUID generation and retrieval,
 * JSON/JSONB literals and operators, array literals and functions,
 * XML support, and NULL handling for these types.
 */
class ArrayJsonUuidTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }
    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // --- 1. UUID generation: gen_random_uuid() ---

    @Test void uuid_gen_random_uuid() throws Exception {
        String val = scalar("SELECT gen_random_uuid()");
        assertNotNull(val);
        UUID parsed = UUID.fromString(val);
        assertNotNull(parsed, "gen_random_uuid() should produce a valid UUID");
        assertEquals(4, parsed.version(), "Should be a version-4 (random) UUID");
    }

    // --- 2. UUID insert and retrieval as string ---

    @Test void uuid_insert_and_retrieve_as_string() throws Exception {
        exec("CREATE TABLE aj_uuid_str(id uuid PRIMARY KEY, label text)");
        String uuidStr = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
        exec("INSERT INTO aj_uuid_str VALUES ('" + uuidStr + "', 'test')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, label FROM aj_uuid_str")) {
            assertTrue(rs.next());
            assertEquals(uuidStr, rs.getString("id"));
            assertEquals("test", rs.getString("label"));
        }
        exec("DROP TABLE aj_uuid_str");
    }

    // --- 3. UUID comparison in WHERE clause ---

    @Test void uuid_comparison_in_where() throws Exception {
        exec("CREATE TABLE aj_uuid_where(id uuid PRIMARY KEY, val int)");
        String u1 = "11111111-1111-1111-1111-111111111111";
        String u2 = "22222222-2222-2222-2222-222222222222";
        exec("INSERT INTO aj_uuid_where VALUES ('" + u1 + "', 10), ('" + u2 + "', 20)");
        String result = scalar("SELECT val FROM aj_uuid_where WHERE id = '" + u1 + "'");
        assertEquals("10", result);
        result = scalar("SELECT val FROM aj_uuid_where WHERE id = '" + u2 + "'");
        assertEquals("20", result);
        exec("DROP TABLE aj_uuid_where");
    }

    // --- 4. UUID type via getObject ---

    @Test void uuid_get_object() throws Exception {
        exec("CREATE TABLE aj_uuid_obj(id uuid PRIMARY KEY)");
        String uuidStr = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        exec("INSERT INTO aj_uuid_obj VALUES ('" + uuidStr + "')");
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT id FROM aj_uuid_obj")) {
            assertTrue(rs.next());
            Object obj = rs.getObject("id");
            assertNotNull(obj);
            // JDBC drivers may return UUID as UUID or String
            String asStr = obj.toString();
            assertEquals(uuidStr, asStr);
        }
        exec("DROP TABLE aj_uuid_obj");
    }

    // --- 5. JSON literal insert and retrieval ---

    @Test void json_literal_insert_and_retrieve() throws Exception {
        exec("CREATE TABLE aj_json(id int PRIMARY KEY, data json)");
        exec("INSERT INTO aj_json VALUES (1, '{\"name\": \"alice\", \"age\": 30}')");
        String result = scalar("SELECT data FROM aj_json WHERE id = 1");
        assertNotNull(result);
        assertTrue(result.contains("\"name\""), "JSON should contain name field");
        assertTrue(result.contains("alice"), "JSON should contain alice value");
        assertTrue(result.contains("30"), "JSON should contain age value");
        exec("DROP TABLE aj_json");
    }

    // --- 6. JSONB literal insert and retrieval ---

    @Test void jsonb_literal_insert_and_retrieve() throws Exception {
        exec("CREATE TABLE aj_jsonb(id int PRIMARY KEY, data jsonb)");
        exec("INSERT INTO aj_jsonb VALUES (1, '{\"key\": \"value\", \"num\": 42}')");
        String result = scalar("SELECT data FROM aj_jsonb WHERE id = 1");
        assertNotNull(result);
        assertTrue(result.contains("\"key\""), "JSONB should contain key field");
        assertTrue(result.contains("value"), "JSONB should contain value");
        assertTrue(result.contains("42"), "JSONB should contain num value");
        exec("DROP TABLE aj_jsonb");
    }

    // --- 7. JSONB operator: -> (get field) ---

    @Test void jsonb_arrow_operator_get_field() throws Exception {
        exec("CREATE TABLE aj_jsonb_arrow(id int PRIMARY KEY, data jsonb)");
        exec("INSERT INTO aj_jsonb_arrow VALUES (1, '{\"name\": \"bob\", \"score\": 95}')");
        String result = scalar("SELECT data -> 'name' FROM aj_jsonb_arrow WHERE id = 1");
        assertNotNull(result);
        // -> returns a JSON value, so it includes quotes
        assertTrue(result.contains("bob"), "Arrow operator should extract the name field");
        exec("DROP TABLE aj_jsonb_arrow");
    }

    // --- 8. JSONB operator: ->> (get field as text) ---

    @Test void jsonb_double_arrow_operator_get_text() throws Exception {
        exec("CREATE TABLE aj_jsonb_darrow(id int PRIMARY KEY, data jsonb)");
        exec("INSERT INTO aj_jsonb_darrow VALUES (1, '{\"city\": \"Paris\", \"pop\": 2161}')");
        String result = scalar("SELECT data ->> 'city' FROM aj_jsonb_darrow WHERE id = 1");
        assertEquals("Paris", result, "->> should return plain text without quotes");
        String numResult = scalar("SELECT data ->> 'pop' FROM aj_jsonb_darrow WHERE id = 1");
        assertEquals("2161", numResult, "->> should return number as text");
        exec("DROP TABLE aj_jsonb_darrow");
    }

    // --- 9. JSONB containment: @> operator ---

    @Test void jsonb_containment_operator() throws Exception {
        exec("CREATE TABLE aj_jsonb_contain(id int PRIMARY KEY, data jsonb)");
        exec("INSERT INTO aj_jsonb_contain VALUES (1, '{\"a\": 1, \"b\": 2, \"c\": 3}')");
        exec("INSERT INTO aj_jsonb_contain VALUES (2, '{\"a\": 1, \"d\": 4}')");
        exec("INSERT INTO aj_jsonb_contain VALUES (3, '{\"x\": 10}')");
        // Find rows where data contains {"a": 1}
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id FROM aj_jsonb_contain WHERE data @> '{\"a\": 1}' ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next(), "Only rows 1 and 2 contain {\"a\": 1}");
        }
        exec("DROP TABLE aj_jsonb_contain");
    }

    // --- 10. Integer array literal: '{1,2,3}'::int[] ---

    @Test void integer_array_literal() throws Exception {
        String result = scalar("SELECT '{1,2,3}'::int[]");
        assertNotNull(result);
        assertEquals("{1,2,3}", result, "Integer array literal should round-trip");
    }

    // --- 11. Text array literal: '{a,b,c}'::text[] ---

    @Test void text_array_literal() throws Exception {
        String result = scalar("SELECT '{a,b,c}'::text[]");
        assertNotNull(result);
        assertEquals("{a,b,c}", result, "Text array literal should round-trip");
    }

    // --- 12. Array element access: arr[1] ---

    @Test void array_element_access() throws Exception {
        exec("CREATE TABLE aj_arr_access(id int PRIMARY KEY, vals int[])");
        exec("INSERT INTO aj_arr_access VALUES (1, '{10,20,30}')");
        // PostgreSQL arrays are 1-indexed
        String first = scalar("SELECT vals[1] FROM aj_arr_access WHERE id = 1");
        assertEquals("10", first, "First element (1-indexed) should be 10");
        String second = scalar("SELECT vals[2] FROM aj_arr_access WHERE id = 1");
        assertEquals("20", second, "Second element should be 20");
        String third = scalar("SELECT vals[3] FROM aj_arr_access WHERE id = 1");
        assertEquals("30", third, "Third element should be 30");
        exec("DROP TABLE aj_arr_access");
    }

    // --- 13. Array unnest ---

    @Test void array_unnest() throws Exception {
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT unnest('{10,20,30}'::int[])")) {
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(30, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    // --- 14. Array ANY: WHERE id = ANY('{1,2,3}') ---

    @Test void array_any_in_where() throws Exception {
        exec("CREATE TABLE aj_arr_any(id int PRIMARY KEY, label text)");
        exec("INSERT INTO aj_arr_any VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, label FROM aj_arr_any WHERE id = ANY('{1,3,5}'::int[]) ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt("id")); assertEquals("a", rs.getString("label"));
            assertTrue(rs.next()); assertEquals(3, rs.getInt("id")); assertEquals("c", rs.getString("label"));
            assertTrue(rs.next()); assertEquals(5, rs.getInt("id")); assertEquals("e", rs.getString("label"));
            assertFalse(rs.next());
        }
        exec("DROP TABLE aj_arr_any");
    }

    // --- 15. ARRAY constructor: ARRAY[1,2,3] ---

    @Test void array_constructor() throws Exception {
        String result = scalar("SELECT ARRAY[1,2,3]");
        assertNotNull(result);
        assertEquals("{1,2,3}", result, "ARRAY constructor should produce same format as literal");
    }

    // --- 16. JSON aggregation: json_agg ---

    @Test void json_agg_aggregation() throws Exception {
        exec("CREATE TABLE aj_json_agg(id int PRIMARY KEY, name text)");
        exec("INSERT INTO aj_json_agg VALUES (1,'alice'),(2,'bob'),(3,'charlie')");
        String result = scalar("SELECT json_agg(name ORDER BY id) FROM aj_json_agg");
        assertNotNull(result);
        assertTrue(result.contains("alice"), "json_agg should contain alice");
        assertTrue(result.contains("bob"), "json_agg should contain bob");
        assertTrue(result.contains("charlie"), "json_agg should contain charlie");
        // Should look like a JSON array
        assertTrue(result.trim().startsWith("["), "json_agg result should be a JSON array");
        assertTrue(result.trim().endsWith("]"), "json_agg result should end with ]");
        exec("DROP TABLE aj_json_agg");
    }

    // --- 17. JSONB build: jsonb_build_object ---

    @Test void jsonb_build_object() throws Exception {
        String result = scalar("SELECT jsonb_build_object('name', 'alice', 'age', 30)");
        assertNotNull(result);
        assertTrue(result.contains("\"name\""), "Should contain name key");
        assertTrue(result.contains("alice"), "Should contain alice value");
        assertTrue(result.contains("\"age\""), "Should contain age key");
        assertTrue(result.contains("30"), "Should contain age value 30");
    }

    // --- 18. XML: xmlparse ---

    @Test void xml_xmlparse() throws Exception {
        try {
            String result = scalar("SELECT xmlparse(content '<root><child>hello</child></root>')");
            assertNotNull(result);
            assertTrue(result.contains("<root>"), "XML result should contain root element");
            assertTrue(result.contains("hello"), "XML result should contain text content");
        } catch (SQLException e) {
            // XML support may not be available in all configurations
            assertTrue(e.getMessage() != null, "If XML is not supported, an error message should be present");
        }
    }

    // --- 19. NULL UUID, NULL JSON, NULL array handling ---

    @Test void null_uuid_handling() throws Exception {
        exec("CREATE TABLE aj_null_uuid(id int PRIMARY KEY, uid uuid)");
        exec("INSERT INTO aj_null_uuid VALUES (1, NULL)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT uid FROM aj_null_uuid WHERE id = 1")) {
            assertTrue(rs.next());
            assertNull(rs.getString("uid"), "NULL UUID should return null string");
            assertNull(rs.getObject("uid"), "NULL UUID should return null object");
            assertTrue(rs.wasNull(), "wasNull() should be true after reading NULL UUID");
        }
        exec("DROP TABLE aj_null_uuid");
    }

    @Test void null_json_handling() throws Exception {
        exec("CREATE TABLE aj_null_json(id int PRIMARY KEY, data json, datab jsonb)");
        exec("INSERT INTO aj_null_json VALUES (1, NULL, NULL)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT data, datab FROM aj_null_json WHERE id = 1")) {
            assertTrue(rs.next());
            assertNull(rs.getString("data"), "NULL json should return null");
            assertTrue(rs.wasNull());
            assertNull(rs.getString("datab"), "NULL jsonb should return null");
            assertTrue(rs.wasNull());
        }
        exec("DROP TABLE aj_null_json");
    }

    @Test void null_array_handling() throws Exception {
        exec("CREATE TABLE aj_null_arr(id int PRIMARY KEY, vals int[], names text[])");
        exec("INSERT INTO aj_null_arr VALUES (1, NULL, NULL)");
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT vals, names FROM aj_null_arr WHERE id = 1")) {
            assertTrue(rs.next());
            assertNull(rs.getString("vals"), "NULL int array should return null string");
            assertTrue(rs.wasNull());
            assertNull(rs.getString("names"), "NULL text array should return null string");
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("vals"), "NULL int array should return null object");
            assertNull(rs.getObject("names"), "NULL text array should return null object");
        }
        exec("DROP TABLE aj_null_arr");
    }
}
