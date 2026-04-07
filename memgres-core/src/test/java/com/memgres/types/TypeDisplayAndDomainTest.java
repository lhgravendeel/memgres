package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #15: Domain array CHECK with NULL; NULL should not violate CHECK
 * Diff #17: ARRAY[ROW(...)::record], record pseudo-type not recognized
 * Diff #18: jsonb_path_match returns "true" vs PG's "t" (boolean display format)
 * Diff #55: Domain CHECK not enforced on INSERT; empty string violates nonempty_text domain
 */
class TypeDisplayAndDomainTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static String scalar(String sql) throws SQLException { try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) { return rs.next() ? rs.getString(1) : null; } }

    // Diff #15: domain array CHECK where array_length returns NULL for empty array
    // PG: CHECK (array_length(VALUE, 1) > 0); empty array returns NULL from array_length,
    // and NULL > 0 is NULL (not false), so CHECK passes.
    @Test void domain_array_check_null_does_not_violate() throws SQLException {
        exec("CREATE DOMAIN posint_array AS int[] CHECK (array_length(VALUE, 1) > 0)");
        exec("CREATE TABLE dom_t(id int PRIMARY KEY, lucky posint_array)");
        try {
            // Non-empty array, should pass
            exec("INSERT INTO dom_t VALUES (1, ARRAY[7,8])");
            // Empty array: array_length returns NULL, and NULL > 0 is NULL (not false), so it passes
            exec("INSERT INTO dom_t VALUES (2, ARRAY[]::int[])");
        } finally {
            exec("DROP TABLE dom_t");
            exec("DROP DOMAIN posint_array");
        }
    }

    // Diff #17: record pseudo-type
    @Test void array_of_record_type() throws SQLException {
        String val = scalar("SELECT ARRAY[ROW(1,'a')::record, ROW(2,'b')::record]");
        assertNotNull(val, "ARRAY of record should succeed");
    }

    // Diff #18: boolean display; PG uses 't'/'f', not 'true'/'false'
    @Test void boolean_display_format_t_f() throws SQLException {
        assertEquals("t", scalar("SELECT true"), "true should display as 't'");
        assertEquals("f", scalar("SELECT false"), "false should display as 'f'");
    }

    @Test void boolean_display_in_jsonb_path_match() throws SQLException {
        String val = scalar("SELECT jsonb_path_match('{\"a\":2}'::jsonb, 'exists($.a ? (@ == 2))')");
        assertEquals("t", val, "jsonb_path_match should return 't' not 'true'");
    }

    @Test void boolean_display_in_expressions() throws SQLException {
        assertEquals("t", scalar("SELECT 1 = 1"));
        assertEquals("f", scalar("SELECT 1 = 2"));
        assertEquals("t", scalar("SELECT true AND true"));
        assertEquals("f", scalar("SELECT true AND false"));
    }

    // Diff #55: domain CHECK enforcement on INSERT
    @Test void domain_check_enforced_on_insert() throws SQLException {
        exec("CREATE DOMAIN nonempty_text AS text CHECK (VALUE <> '')");
        exec("CREATE TABLE dom2_t(id int PRIMARY KEY, tag nonempty_text DEFAULT 'ok')");
        try {
            // Valid insert
            exec("INSERT INTO dom2_t VALUES (1, 'hello')");
            // Empty string violates domain CHECK, should fail
            assertThrows(SQLException.class,
                () -> exec("INSERT INTO dom2_t VALUES (2, '')"),
                "Empty string should violate nonempty_text domain CHECK");
        } finally {
            exec("DROP TABLE dom2_t");
            exec("DROP DOMAIN nonempty_text");
        }
    }

    @Test void domain_check_with_length_constraint() throws SQLException {
        exec("CREATE DOMAIN nonempty_text2 AS text CHECK (VALUE <> '')");
        exec("ALTER DOMAIN nonempty_text2 ADD CONSTRAINT nonempty_text2_len CHECK (char_length(VALUE) <= 10)");
        exec("CREATE TABLE dom3_t(id int PRIMARY KEY, tag nonempty_text2)");
        try {
            exec("INSERT INTO dom3_t VALUES (1, 'short')");
            assertThrows(SQLException.class, () -> exec("INSERT INTO dom3_t VALUES (2, '')"), "Empty string violates CHECK");
            assertThrows(SQLException.class, () -> exec("INSERT INTO dom3_t VALUES (3, 'this is way too long for the constraint')"), "Too long violates CHECK");
        } finally {
            exec("DROP TABLE dom3_t"); exec("DROP DOMAIN nonempty_text2");
        }
    }
}
