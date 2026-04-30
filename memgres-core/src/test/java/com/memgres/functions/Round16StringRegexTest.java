package com.memgres.functions;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category K: String / regex functions.
 *
 * Covers:
 *  - sha224(text/bytea)
 *  - convert(bytea, src, dest) three-arg form
 *  - regexp_instr / regexp_substr / regexp_count / regexp_replace with
 *    start position + Nth occurrence + subexpr arguments
 *  - regexp_split_to_table set-returning form
 *  - overlay(bytea PLACING bytea FROM int) preserves bytes
 */
class Round16StringRegexTest {

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

    private static String str(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static int intQ(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // K1. sha224
    // =========================================================================

    @Test
    void sha224_text_known_answer() throws SQLException {
        // sha224('abc') = 23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7
        String v = str("SELECT encode(sha224('abc'::bytea), 'hex')");
        assertEquals(
                "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7",
                v,
                "sha224('abc') hex digest must match FIPS known answer");
    }

    // =========================================================================
    // K2. convert(bytea, src, dest) 3-arg form
    // =========================================================================

    @Test
    void convert_three_arg_utf8_to_latin1_roundtrip() throws SQLException {
        // 'é' in UTF8 → LATIN1 should become 0xE9
        String hex = str(
                "SELECT encode(convert('\u00e9'::bytea, 'UTF8', 'LATIN1'), 'hex')");
        assertEquals("e9", hex,
                "convert('é'::bytea, 'UTF8', 'LATIN1') must produce byte 0xe9; got " + hex);
    }

    // =========================================================================
    // K3. regexp_instr
    // =========================================================================

    @Test
    void regexp_instr_with_start_and_nth_match() throws SQLException {
        // Positions of 'a' in 'banana': b=1 a=2 n=3 a=4 n=5 a=6
        // regexp_instr(str, pattern, start=1, N=2) should return 4 (2nd 'a')
        int n = intQ("SELECT regexp_instr('banana', 'a', 1, 2)");
        assertEquals(4, n,
                "regexp_instr('banana','a',1,2) must return 4; got " + n);
    }

    // =========================================================================
    // K4. regexp_substr with N-th occurrence
    // =========================================================================

    @Test
    void regexp_substr_returns_nth_occurrence() throws SQLException {
        // regexp_substr('foobarfoo', 'foo', 1, 2) must return 'foo' starting at pos 7
        String v = str("SELECT regexp_substr('foobarfoo', 'foo', 1, 2)");
        assertEquals("foo", v,
                "regexp_substr('foobarfoo','foo',1,2) must return 'foo'; got " + v);
    }

    // =========================================================================
    // K5. regexp_count
    // =========================================================================

    @Test
    void regexp_count_basic() throws SQLException {
        int n = intQ("SELECT regexp_count('banana', 'a')");
        assertEquals(3, n,
                "regexp_count('banana','a') must return 3; got " + n);
    }

    @Test
    void regexp_count_with_start() throws SQLException {
        // Starting at position 4, 'banana' → 'ana' → 'a' appears twice
        int n = intQ("SELECT regexp_count('banana', 'a', 4)");
        assertEquals(2, n,
                "regexp_count('banana','a',4) must return 2; got " + n);
    }

    // =========================================================================
    // K6. regexp_replace with start position + Nth occurrence
    // =========================================================================

    @Test
    void regexp_replace_with_nth_replaces_only_specified_match() throws SQLException {
        // regexp_replace('banana','a','X',1,2) → replace only 2nd 'a'
        String v = str("SELECT regexp_replace('banana', 'a', 'X', 1, 2)");
        assertEquals("banXna", v,
                "regexp_replace('banana','a','X',1,2) must produce 'banXna'; got " + v);
    }

    // =========================================================================
    // K7. regexp_split_to_table
    // =========================================================================

    @Test
    void regexp_split_to_table_emits_rows() throws SQLException {
        int n = intQ(
                "SELECT count(*)::int FROM regexp_split_to_table('a,b,c,d', ',')");
        assertEquals(4, n,
                "regexp_split_to_table('a,b,c,d', ',') must yield 4 rows; got " + n);
    }

    // =========================================================================
    // K8. overlay(bytea PLACING bytea FROM int)
    // =========================================================================

    @Test
    void overlay_bytea_preserves_non_ascii_bytes() throws SQLException {
        // Start: 0xFF FF FF FF FF, PLACING 0xAA BB FROM position 2
        // Expected: 0xFF AA BB FF FF
        String hex = str(
                "SELECT encode(" +
                        "overlay('\\xffffffffff'::bytea PLACING '\\xaabb'::bytea FROM 2), " +
                        "'hex')");
        assertEquals("ffaabbffff", hex.toLowerCase(),
                "overlay(bytea PLACING bytea FROM 2) must preserve raw bytes; got " + hex);
    }
}
