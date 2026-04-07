package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 22-23: Dynamic SQL helpers and quoting functions.
 * Tests quote_ident, quote_literal, quote_nullable, format(),
 * object/column/index/constraint existence checks, and pg_typeof.
 */
class DynamicSqlQuotingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE dq_items (id serial PRIMARY KEY, name text NOT NULL, price numeric)");
        exec("CREATE INDEX dq_items_name_idx ON dq_items (name)");
        exec("ALTER TABLE dq_items ADD CONSTRAINT dq_items_price_positive CHECK (price >= 0)");
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

    // --- quote_ident ---

    @Test void quoteIdent_simple_identifier() throws Exception {
        String result = scalar("SELECT quote_ident('simple')");
        assertEquals("simple", result, "Simple lowercase identifier should not be quoted");
    }

    @Test void quoteIdent_mixed_case_needs_quoting() throws Exception {
        String result = scalar("SELECT quote_ident('MixedCase')");
        assertEquals("\"MixedCase\"", result, "Mixed-case identifier should be double-quoted");
    }

    @Test void quoteIdent_identifier_with_spaces() throws Exception {
        String result = scalar("SELECT quote_ident('has space')");
        assertEquals("\"has space\"", result, "Identifier with spaces should be double-quoted");
    }

    @Test void quoteIdent_reserved_word() throws Exception {
        String result = scalar("SELECT quote_ident('select')");
        assertEquals("\"select\"", result, "Reserved word should be double-quoted");
    }

    // --- quote_literal ---

    @Test void quoteLiteral_simple_string() throws Exception {
        String result = scalar("SELECT quote_literal('hello')");
        assertEquals("'hello'", result, "Simple string should be single-quoted");
    }

    @Test void quoteLiteral_string_with_single_quote() throws Exception {
        String result = scalar("SELECT quote_literal('it''s')");
        assertEquals("'it''s'", result, "Single quote inside should be escaped");
    }

    @Test void quoteLiteral_null_returns_string_NULL() throws Exception {
        String result = scalar("SELECT quote_literal(NULL)");
        assertNull(result, "quote_literal(NULL) should return SQL NULL");
    }

    // --- quote_nullable ---

    @Test void quoteNullable_non_null_value() throws Exception {
        String result = scalar("SELECT quote_nullable('hello')");
        assertEquals("'hello'", result, "Non-null value should be single-quoted");
    }

    @Test void quoteNullable_null_returns_NULL_string() throws Exception {
        String result = scalar("SELECT quote_nullable(NULL)");
        assertEquals("NULL", result, "quote_nullable(NULL) should return the string NULL");
    }

    // --- format ---

    @Test void format_identifier_placeholder() throws Exception {
        String result = scalar("SELECT format('SELECT * FROM %I', 'my_table')");
        assertEquals("SELECT * FROM my_table", result);
    }

    @Test void format_literal_placeholder() throws Exception {
        String result = scalar("SELECT format('WHERE name = %L', 'O''Brien')");
        assertEquals("WHERE name = 'O''Brien'", result);
    }

    @Test void format_simple_string_placeholder() throws Exception {
        String result = scalar("SELECT format('hello %s', 'world')");
        assertEquals("hello world", result);
    }

    @Test void format_multiple_placeholders() throws Exception {
        String result = scalar("SELECT format('INSERT INTO %I (%I) VALUES (%L)', 'dq_items', 'name', 'widget')");
        assertEquals("INSERT INTO dq_items (name) VALUES ('widget')", result);
    }

    // --- existence checks ---

    @Test void table_existence_check() throws Exception {
        String result = scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'dq_items')");
        assertTrue(result.equalsIgnoreCase("t") || result.equalsIgnoreCase("true"), "dq_items table should exist");
    }

    @Test void table_existence_check_missing() throws Exception {
        String result = scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'dq_no_such_table')");
        assertTrue(result.equalsIgnoreCase("f") || result.equalsIgnoreCase("false"), "Non-existent table should return false");
    }

    @Test void column_existence_check() throws Exception {
        String result = scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns " +
                "WHERE table_name = 'dq_items' AND column_name = 'name')");
        assertTrue(result.equalsIgnoreCase("t") || result.equalsIgnoreCase("true"), "name column should exist on dq_items");
    }

    @Test void column_existence_check_missing() throws Exception {
        String result = scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns " +
                "WHERE table_name = 'dq_items' AND column_name = 'nonexistent')");
        assertTrue(result.equalsIgnoreCase("f") || result.equalsIgnoreCase("false"), "Non-existent column should return false");
    }

    @Test void index_existence_check_via_pg_indexes() throws Exception {
        String result = scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = 'dq_items_name_idx')");
        assertTrue(result.equalsIgnoreCase("t") || result.equalsIgnoreCase("true"), "dq_items_name_idx should exist");
    }

    @Test void index_existence_check_missing() throws Exception {
        String result = scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname = 'dq_no_such_idx')");
        assertTrue(result.equalsIgnoreCase("f") || result.equalsIgnoreCase("false"), "Non-existent index should return false");
    }

    @Test void constraint_existence_check_via_information_schema() throws Exception {
        String result = scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.table_constraints " +
                "WHERE table_name = 'dq_items' AND constraint_name = 'dq_items_price_positive')");
        assertTrue(result.equalsIgnoreCase("t") || result.equalsIgnoreCase("true"), "dq_items_price_positive constraint should exist");
    }

    @Test void constraint_existence_check_missing() throws Exception {
        String result = scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.table_constraints " +
                "WHERE table_name = 'dq_items' AND constraint_name = 'dq_no_such_constraint')");
        assertTrue(result.equalsIgnoreCase("f") || result.equalsIgnoreCase("false"), "Non-existent constraint should return false");
    }

    // --- pg_typeof ---

    @Test void pgTypeof_integer() throws Exception {
        String result = scalar("SELECT pg_typeof(42)");
        assertEquals("integer", result);
    }

    @Test void pgTypeof_text() throws Exception {
        String result = scalar("SELECT pg_typeof('hello'::text)");
        assertEquals("text", result);
    }

    @Test void pgTypeof_boolean() throws Exception {
        String result = scalar("SELECT pg_typeof(true)");
        assertEquals("boolean", result);
    }

    @Test void pgTypeof_numeric() throws Exception {
        String result = scalar("SELECT pg_typeof(3.14::numeric)");
        assertEquals("numeric", result);
    }

    @Test void pgTypeof_timestamp() throws Exception {
        String result = scalar("SELECT pg_typeof(now())");
        assertEquals("timestamp with time zone", result);
    }
}
