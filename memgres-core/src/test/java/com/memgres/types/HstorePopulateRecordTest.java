package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for populate_record(record, hstore), hstore(record), and #= operator.
 */
class HstorePopulateRecordTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE EXTENSION IF NOT EXISTS hstore");
        exec("CREATE TYPE test_pop AS (a int, b text, c boolean)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try { exec("DROP TYPE IF EXISTS test_pop CASCADE"); } catch (Exception ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected at least one row for: " + sql);
            String v = rs.getString(1);
            assertFalse(rs.next(), "expected single row");
            return v;
        }
    }

    private static String strOrNull(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected at least one row");
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. populate_record(anyelement, hstore)
    // =========================================================================

    @Test
    void populate_record_null_base() throws SQLException {
        assertEquals("42", str(
            "SELECT (populate_record(NULL::test_pop, 'a=>42, b=>hello'::hstore)).a"));
    }

    @Test
    void populate_record_extracts_text_field() throws SQLException {
        assertEquals("hello", str(
            "SELECT (populate_record(NULL::test_pop, 'a=>42, b=>hello'::hstore)).b"));
    }

    @Test
    void populate_record_extracts_boolean_field() throws SQLException {
        assertEquals("t", str(
            "SELECT (populate_record(NULL::test_pop, 'a=>1, c=>true'::hstore)).c"));
    }

    @Test
    void populate_record_preserves_base_values() throws SQLException {
        // Only 'b' is overridden; 'a' should keep its base value
        assertEquals("99", str(
            "SELECT (populate_record(ROW(99, 'old', true)::test_pop, 'b=>new'::hstore)).a"));
        assertEquals("new", str(
            "SELECT (populate_record(ROW(99, 'old', true)::test_pop, 'b=>new'::hstore)).b"));
    }

    @Test
    void populate_record_ignores_extra_keys() throws SQLException {
        // 'z' doesn't exist in test_pop — should be silently ignored
        assertEquals("42", str(
            "SELECT (populate_record(NULL::test_pop, 'a=>42, z=>extra'::hstore)).a"));
    }

    @Test
    void populate_record_null_hstore_returns_base() throws SQLException {
        assertEquals("99", str(
            "SELECT (populate_record(ROW(99, 'x', false)::test_pop, NULL::hstore)).a"));
    }

    @Test
    void populate_record_null_hstore_value() throws SQLException {
        assertNull(strOrNull(
            "SELECT (populate_record(NULL::test_pop, 'a=>NULL'::hstore)).a"));
    }

    @Test
    void populate_record_with_table_type() throws SQLException {
        exec("CREATE TABLE t_pop_table (x int, y text)");
        try {
            assertEquals("99", str(
                "SELECT (populate_record(NULL::t_pop_table, 'x=>99, y=>hi'::hstore)).x"));
            assertEquals("hi", str(
                "SELECT (populate_record(NULL::t_pop_table, 'x=>99, y=>hi'::hstore)).y"));
        } finally {
            exec("DROP TABLE t_pop_table");
        }
    }

    @Test
    void populate_record_in_from_clause() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT a, b FROM populate_record(NULL::test_pop, 'a=>10, b=>world'::hstore)")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            assertEquals("world", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // B. hstore(record)
    // =========================================================================

    @Test
    void hstore_from_composite_type() throws SQLException {
        String result = str("SELECT hstore(ROW(1, 'hello', true)::test_pop)");
        assertNotNull(result);
        // Verify it contains expected keys
        assertTrue(result.contains("\"a\"=>\"1\""), "should contain a=>1: " + result);
        assertTrue(result.contains("\"b\"=>\"hello\""), "should contain b=>hello: " + result);
        assertTrue(result.contains("\"c\"=>\"true\""), "should contain c=>true: " + result);
    }

    @Test
    void hstore_from_record_null_field() throws SQLException {
        String result = str("SELECT hstore(ROW(1, NULL, false)::test_pop)");
        assertNotNull(result);
        assertTrue(result.contains("\"a\"=>\"1\""), "should contain a=>1: " + result);
        assertTrue(result.contains("\"b\"=>NULL"), "should contain b=>NULL: " + result);
    }

    @Test
    void hstore_from_record_arrow_extraction() throws SQLException {
        // Convert to hstore then extract a key
        assertEquals("hello", str(
            "SELECT hstore(ROW(1, 'hello', true)::test_pop)->'b'"));
    }

    // =========================================================================
    // C. #= operator
    // =========================================================================

    @Test
    void hash_equals_basic() throws SQLException {
        assertEquals("5", str(
            "SELECT (NULL::test_pop #= 'a=>5, b=>hi'::hstore).a"));
    }

    @Test
    void hash_equals_text_field() throws SQLException {
        assertEquals("hi", str(
            "SELECT (NULL::test_pop #= 'a=>5, b=>hi'::hstore).b"));
    }

    @Test
    void hash_equals_preserves_base() throws SQLException {
        assertEquals("99", str(
            "SELECT (ROW(99, 'old', true)::test_pop #= 'b=>new'::hstore).a"));
        assertEquals("new", str(
            "SELECT (ROW(99, 'old', true)::test_pop #= 'b=>new'::hstore).b"));
    }

    // =========================================================================
    // D. Round-trip: hstore(record) -> populate_record
    // =========================================================================

    @Test
    void round_trip_hstore_record() throws SQLException {
        // Convert record to hstore, then back
        assertEquals("42", str(
            "SELECT (populate_record(NULL::test_pop, hstore(ROW(42, 'world', false)::test_pop))).a"));
        assertEquals("world", str(
            "SELECT (populate_record(NULL::test_pop, hstore(ROW(42, 'world', false)::test_pop))).b"));
    }

    // =========================================================================
    // E. Star expansion: (populate_record(...)).*
    // =========================================================================

    @Test
    void populate_record_star_expansion() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "SELECT (populate_record(NULL::test_pop, 'a=>42, b=>hello, c=>true'::hstore)).*")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount());
            assertEquals("a", md.getColumnName(1));
            assertEquals("b", md.getColumnName(2));
            assertEquals("c", md.getColumnName(3));
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
            assertEquals("hello", rs.getString(2));
            assertEquals(true, rs.getBoolean(3));
            assertFalse(rs.next());
        }
    }
}
