package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Advanced regex surfaces.
 *
 * - POSIX regex flags (?x) extended / (?n) newline-sensitive
 * - regexp_substr / regexp_count / regexp_instr full param set
 * - LIKE_REGEX (SQL:2008) predicate
 */
class Round14RegexAdvancedTest {

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
    // A. Embedded regex flags (?x), (?n), (?i), (?s)
    // =========================================================================

    @Test
    void regex_extended_flag_x() throws SQLException {
        // (?x) allows whitespace / comments in pattern
        assertEquals("true", scalarString(
                "SELECT ('hello' ~ '(?x) h e l l o ')::text"));
    }

    @Test
    void regex_newline_sensitive_flag_n() throws SQLException {
        // (?n): . does NOT match newline; ^$ match line boundaries
        // Input 'foo\nbar' — pattern '.' in non-(?n) mode only matches first line
        assertEquals("true", scalarString(
                "SELECT ('line1\nline2' ~ '(?n)^line2$')::text"));
    }

    @Test
    void regex_case_insensitive_flag_i() throws SQLException {
        assertEquals("true", scalarString(
                "SELECT ('HELLO' ~ '(?i)hello')::text"));
    }

    @Test
    void regex_single_line_flag_s() throws SQLException {
        // (?s): . matches newline
        assertEquals("true", scalarString(
                "SELECT ('a\nb' ~ '(?s)a.b')::text"));
    }

    // =========================================================================
    // B. regexp_substr
    // =========================================================================

    @Test
    void regexp_substr_basic() throws SQLException {
        assertEquals("foo", scalarString(
                "SELECT regexp_substr('foobar', 'foo')"));
    }

    @Test
    void regexp_substr_with_start_position() throws SQLException {
        // 4th argument is start_pos; 2nd occurrence
        assertEquals("bar", scalarString(
                "SELECT regexp_substr('foobar foobar', 'foo|bar', 1, 2)"));
    }

    @Test
    void regexp_substr_with_flags() throws SQLException {
        assertEquals("FOO", scalarString(
                "SELECT regexp_substr('FOO', 'foo', 1, 1, 'i')"));
    }

    @Test
    void regexp_substr_with_capture_group() throws SQLException {
        // 6th argument: capture group number
        assertEquals("42", scalarString(
                "SELECT regexp_substr('abc=42', '([a-z]+)=([0-9]+)', 1, 1, 'c', 2)"));
    }

    // =========================================================================
    // C. regexp_count
    // =========================================================================

    @Test
    void regexp_count_basic() throws SQLException {
        assertEquals(3, scalarInt(
                "SELECT regexp_count('aaa', 'a')"));
    }

    @Test
    void regexp_count_with_start_position() throws SQLException {
        assertEquals(2, scalarInt(
                "SELECT regexp_count('aaaa', 'a', 3)"));
    }

    @Test
    void regexp_count_with_flags() throws SQLException {
        assertEquals(3, scalarInt(
                "SELECT regexp_count('AaA', 'a', 1, 'i')"));
    }

    // =========================================================================
    // D. regexp_instr
    // =========================================================================

    @Test
    void regexp_instr_basic() throws SQLException {
        // Returns 1-based start position of first match
        assertEquals(1, scalarInt(
                "SELECT regexp_instr('foobar', 'foo')"));
    }

    @Test
    void regexp_instr_with_occurrence() throws SQLException {
        // Find position of the SECOND occurrence
        assertEquals(5, scalarInt(
                "SELECT regexp_instr('foo foo', 'foo', 1, 2)"));
    }

    @Test
    void regexp_instr_with_return_option() throws SQLException {
        // return_opt = 1 returns position after the match
        assertEquals(4, scalarInt(
                "SELECT regexp_instr('foobar', 'foo', 1, 1, 1)"));
    }

    @Test
    void regexp_instr_with_flags_and_group() throws SQLException {
        // Case-insensitive, capture group 1's position
        int v = scalarInt(
                "SELECT regexp_instr('FOO=bar', '([a-z]+)=([a-z]+)', 1, 1, 0, 'i', 2)");
        assertEquals(5, v);
    }

    // =========================================================================
    // E. regexp_replace occurrence argument
    // =========================================================================

    @Test
    void regexp_replace_with_occurrence() throws SQLException {
        // Replace only the 2nd occurrence
        assertEquals("aXa", scalarString(
                "SELECT regexp_replace('aaa', 'a', 'X', 1, 2)"));
    }

    // =========================================================================
    // F. regexp_like (PG 17+)
    // =========================================================================

    @Test
    void regexp_like_basic() throws SQLException {
        assertEquals("true", scalarString(
                "SELECT regexp_like('abc', 'b')::text"));
    }

    @Test
    void regexp_like_with_flags() throws SQLException {
        assertEquals("true", scalarString(
                "SELECT regexp_like('ABC', 'abc', 'i')::text"));
    }

    // =========================================================================
    // G. LIKE_REGEX (SQL:2008)
    // =========================================================================

    @Test
    void like_regex_predicate() throws SQLException {
        try {
            assertEquals("true", scalarString(
                    "SELECT ('abc' LIKE_REGEX 'b.*')::text"));
        } catch (SQLException e) {
            // LIKE_REGEX is in SQL:2008 but Memgres may not parse — explicit fail.
            fail("LIKE_REGEX must parse: " + e.getMessage());
        }
    }

    // =========================================================================
    // H. SIMILAR TO / SUBSTRING with regex group
    // =========================================================================

    @Test
    void substring_regex_with_group() throws SQLException {
        // SUBSTRING('abc123' FROM '([0-9]+)')
        assertEquals("123", scalarString(
                "SELECT SUBSTRING('abc123' FROM '([0-9]+)')"));
    }
}
