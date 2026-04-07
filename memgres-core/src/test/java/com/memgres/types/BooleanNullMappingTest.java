package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document Java/JDBC section 17: Boolean, null, and empty-string mapping.
 * Tests boolean insert/retrieve, null semantics for primitive getters,
 * empty-string vs null distinction, boolean string representations,
 * three-valued logic, and boolean/text casting.
 */
class BooleanNullMappingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }
    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // --- 1. Boolean true insert and retrieve via getBoolean ---

    @Test void boolean_true_insert_and_retrieve() throws Exception {
        exec("CREATE TABLE bn_true(id int PRIMARY KEY, flag boolean)");
        exec("INSERT INTO bn_true VALUES (1, true)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT flag FROM bn_true WHERE id = 1")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1), "getBoolean should return true for boolean true");
        }
        exec("DROP TABLE bn_true");
    }

    // --- 2. Boolean false insert and retrieve ---

    @Test void boolean_false_insert_and_retrieve() throws Exception {
        exec("CREATE TABLE bn_false(id int PRIMARY KEY, flag boolean)");
        exec("INSERT INTO bn_false VALUES (1, false)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT flag FROM bn_false WHERE id = 1")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1), "getBoolean should return false for boolean false");
        }
        exec("DROP TABLE bn_false");
    }

    // --- 3. Null boolean: getBoolean returns false, wasNull returns true ---

    @Test void null_boolean_getBoolean_returns_false_wasNull_true() throws Exception {
        exec("CREATE TABLE bn_nullbool(id int PRIMARY KEY, flag boolean)");
        exec("INSERT INTO bn_nullbool VALUES (1, NULL)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT flag FROM bn_nullbool WHERE id = 1")) {
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1), "getBoolean on NULL should return false");
            assertTrue(rs.wasNull(), "wasNull should return true after reading NULL boolean");
        }
        exec("DROP TABLE bn_nullbool");
    }

    // --- 4. Boolean via getObject returns Boolean type ---

    @Test void boolean_getObject_returns_Boolean_type() throws Exception {
        exec("CREATE TABLE bn_objtype(id int PRIMARY KEY, flag boolean)");
        exec("INSERT INTO bn_objtype VALUES (1, true)");
        exec("INSERT INTO bn_objtype VALUES (2, false)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT flag FROM bn_objtype ORDER BY id")) {
            assertTrue(rs.next());
            Object val = rs.getObject(1);
            assertInstanceOf(Boolean.class, val, "getObject on boolean column should return Boolean");
            assertEquals(Boolean.TRUE, val);
            assertTrue(rs.next());
            Object valFalse = rs.getObject(1);
            assertInstanceOf(Boolean.class, valFalse);
            assertEquals(Boolean.FALSE, valFalse);
        }
        exec("DROP TABLE bn_objtype");
    }

    // --- 5. Boolean string representations ---

    @Test void boolean_string_representations() throws Exception {
        exec("CREATE TABLE bn_strrep(id int PRIMARY KEY, flag boolean)");
        exec("INSERT INTO bn_strrep VALUES (1, 'true')");
        exec("INSERT INTO bn_strrep VALUES (2, 'false')");
        exec("INSERT INTO bn_strrep VALUES (3, 't')");
        exec("INSERT INTO bn_strrep VALUES (4, 'f')");
        exec("INSERT INTO bn_strrep VALUES (5, 'yes')");
        exec("INSERT INTO bn_strrep VALUES (6, 'no')");
        exec("INSERT INTO bn_strrep VALUES (7, '1')");
        exec("INSERT INTO bn_strrep VALUES (8, '0')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, flag FROM bn_strrep ORDER BY id")) {
            // true, t, yes, 1 -> true
            assertTrue(rs.next()); assertTrue(rs.getBoolean(2), "'true' should map to true");
            assertTrue(rs.next()); assertFalse(rs.getBoolean(2), "'false' should map to false");
            assertTrue(rs.next()); assertTrue(rs.getBoolean(2), "'t' should map to true");
            assertTrue(rs.next()); assertFalse(rs.getBoolean(2), "'f' should map to false");
            assertTrue(rs.next()); assertTrue(rs.getBoolean(2), "'yes' should map to true");
            assertTrue(rs.next()); assertFalse(rs.getBoolean(2), "'no' should map to false");
            assertTrue(rs.next()); assertTrue(rs.getBoolean(2), "'1' should map to true");
            assertTrue(rs.next()); assertFalse(rs.getBoolean(2), "'0' should map to false");
        }
        exec("DROP TABLE bn_strrep");
    }

    // --- 6. Empty string vs null: text column, empty string is NOT null ---

    @Test void empty_string_is_not_null_in_text_column() throws Exception {
        exec("CREATE TABLE bn_empty(id int PRIMARY KEY, val text)");
        exec("INSERT INTO bn_empty VALUES (1, '')");
        exec("INSERT INTO bn_empty VALUES (2, NULL)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, val IS NULL AS is_null FROM bn_empty ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.getBoolean(2), "Empty string should NOT be null");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertTrue(rs.getBoolean(2), "NULL value should be null");
        }
        exec("DROP TABLE bn_empty");
    }

    // --- 7. Empty string getObject returns "" not null ---

    @Test void empty_string_getObject_returns_empty_string() throws Exception {
        exec("CREATE TABLE bn_emptyobj(id int PRIMARY KEY, val text)");
        exec("INSERT INTO bn_emptyobj VALUES (1, '')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM bn_emptyobj WHERE id = 1")) {
            assertTrue(rs.next());
            Object obj = rs.getObject(1);
            assertNotNull(obj, "getObject on empty string should not return null");
            assertEquals("", obj, "getObject on empty string should return empty string");
            assertFalse(rs.wasNull(), "wasNull should be false for empty string");
        }
        exec("DROP TABLE bn_emptyobj");
    }

    // --- 8. Null text getObject returns null ---

    @Test void null_text_getObject_returns_null() throws Exception {
        exec("CREATE TABLE bn_nulltext(id int PRIMARY KEY, val text)");
        exec("INSERT INTO bn_nulltext VALUES (1, NULL)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM bn_nulltext WHERE id = 1")) {
            assertTrue(rs.next());
            Object obj = rs.getObject(1);
            assertNull(obj, "getObject on NULL text should return null");
            assertTrue(rs.wasNull(), "wasNull should be true after reading NULL text");
        }
        exec("DROP TABLE bn_nulltext");
    }

    // --- 9. Integer null: getInt returns 0, wasNull returns true ---

    @Test void null_integer_getInt_returns_zero_wasNull_true() throws Exception {
        exec("CREATE TABLE bn_nullint(id int PRIMARY KEY, val int)");
        exec("INSERT INTO bn_nullint VALUES (1, NULL)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM bn_nullint WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "getInt on NULL should return 0");
            assertTrue(rs.wasNull(), "wasNull should return true after reading NULL int");
        }
        exec("DROP TABLE bn_nullint");
    }

    // --- 10. Double null: getDouble returns 0.0, wasNull returns true ---

    @Test void null_double_getDouble_returns_zero_wasNull_true() throws Exception {
        exec("CREATE TABLE bn_nulldbl(id int PRIMARY KEY, val double precision)");
        exec("INSERT INTO bn_nulldbl VALUES (1, NULL)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM bn_nulldbl WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(0.0, rs.getDouble(1), "getDouble on NULL should return 0.0");
            assertTrue(rs.wasNull(), "wasNull should return true after reading NULL double");
        }
        exec("DROP TABLE bn_nulldbl");
    }

    // --- 11. Boolean in WHERE clause ---

    @Test void boolean_in_where_clause() throws Exception {
        exec("CREATE TABLE bn_where(id int PRIMARY KEY, flag boolean)");
        exec("INSERT INTO bn_where VALUES (1, true)");
        exec("INSERT INTO bn_where VALUES (2, false)");
        exec("INSERT INTO bn_where VALUES (3, true)");
        // WHERE flag = true
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM bn_where WHERE flag = true")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1), "WHERE flag = true should find 2 rows");
        }
        // WHERE flag IS TRUE
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM bn_where WHERE flag IS TRUE")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1), "WHERE flag IS TRUE should find 2 rows");
        }
        exec("DROP TABLE bn_where");
    }

    // --- 12. Boolean NOT: WHERE NOT flag ---

    @Test void boolean_not_in_where() throws Exception {
        exec("CREATE TABLE bn_not(id int PRIMARY KEY, flag boolean)");
        exec("INSERT INTO bn_not VALUES (1, true)");
        exec("INSERT INTO bn_not VALUES (2, false)");
        exec("INSERT INTO bn_not VALUES (3, NULL)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM bn_not WHERE NOT flag")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "WHERE NOT flag should find only the false row (NULL excluded)");
        }
        exec("DROP TABLE bn_not");
    }

    // --- 13. Three-valued logic: NULL AND TRUE, NULL OR FALSE, NOT NULL ---

    @Test void three_valued_logic_null_and_true() throws Exception {
        String result = scalar("SELECT (NULL AND TRUE)::text");
        assertNull(result, "NULL AND TRUE should be NULL");
    }

    @Test void three_valued_logic_null_or_false() throws Exception {
        String result = scalar("SELECT (NULL OR FALSE)::text");
        assertNull(result, "NULL OR FALSE should be NULL");
    }

    @Test void three_valued_logic_not_null() throws Exception {
        String result = scalar("SELECT (NOT NULL)::text");
        assertNull(result, "NOT NULL (boolean expression) should be NULL");
    }

    @Test void three_valued_logic_null_and_false() throws Exception {
        String result = scalar("SELECT (NULL AND FALSE)::text");
        assertEquals("false", result, "NULL AND FALSE should be FALSE");
    }

    @Test void three_valued_logic_null_or_true() throws Exception {
        String result = scalar("SELECT (NULL OR TRUE)::text");
        assertEquals("true", result, "NULL OR TRUE should be TRUE");
    }

    // --- 14. Cast boolean to text ---

    @Test void cast_boolean_to_text() throws Exception {
        assertEquals("true", scalar("SELECT true::text"));
        assertEquals("false", scalar("SELECT false::text"));
    }

    // --- 15. Cast text to boolean ---

    @Test void cast_text_yes_to_boolean() throws Exception {
        assertEquals("true", scalar("SELECT 'yes'::boolean::text"));
    }

    @Test void cast_text_1_to_boolean() throws Exception {
        assertEquals("true", scalar("SELECT '1'::boolean::text"));
    }

    @Test void cast_text_no_to_boolean() throws Exception {
        assertEquals("false", scalar("SELECT 'no'::boolean::text"));
    }

    @Test void cast_text_0_to_boolean() throws Exception {
        assertEquals("false", scalar("SELECT '0'::boolean::text"));
    }

    @Test void cast_text_t_to_boolean() throws Exception {
        assertEquals("true", scalar("SELECT 't'::boolean::text"));
    }

    @Test void cast_text_f_to_boolean() throws Exception {
        assertEquals("false", scalar("SELECT 'f'::boolean::text"));
    }
}
