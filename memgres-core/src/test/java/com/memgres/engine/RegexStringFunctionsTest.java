package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for fix/regex-string-functions: H25, H26, M23.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RegexStringFunctionsTest {

    private static Memgres memgres;
    private static Connection conn;

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

    private String query(String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected a row for: " + sql);
            return rs.getString(1);
        }
    }

    private String getSqlState(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    // ========== H25: POSIX regex ==========

    @Test @Order(1)
    void posixDigitClass() throws SQLException {
        assertEquals("t", query("SELECT 'abc123' ~ '[[:digit:]]{3}'"));
    }

    @Test @Order(2)
    void posixAlphaClass() throws SQLException {
        assertEquals("t", query("SELECT 'hello' ~ '[[:alpha:]]+'"));
    }

    @Test @Order(3)
    void wordBoundaryBegin() throws SQLException {
        assertEquals("t", query("SELECT 'hello world' ~ '\\mworld'"));
    }

    @Test @Order(4)
    void wordBoundaryNoMatch() throws SQLException {
        assertEquals("f", query("SELECT 'helloworld' ~ '\\mworld'"));
    }

    @Test @Order(5)
    void dollarDoesNotMatchBeforeTrailingNewline() throws SQLException {
        assertEquals("f", query("SELECT E'abc\\n' ~ 'c$'"));
    }

    @Test @Order(6)
    void regexpLikeWithFlags() throws SQLException {
        assertEquals("t", query("SELECT regexp_like('ABC', 'abc', 'i')"));
    }

    @Test @Order(7)
    void regexpLikeNewlineSensitive() throws SQLException {
        assertEquals("t", query("SELECT regexp_like(E'a\\nb', '^b', 'n')"));
    }

    // ========== H26: regexp_* functions ==========

    @Test @Order(10)
    void regexpReplaceStartPositionFirstOnly() throws SQLException {
        // PG15+ form: replace first match from position 3
        assertEquals("banXna", query("SELECT regexp_replace('banana', 'a', 'X', 3)"));
    }

    @Test @Order(11)
    void regexpReplaceStartZeroError() {
        assertEquals("22023", getSqlState("SELECT regexp_replace('banana', 'a', 'X', 0)"));
    }

    @Test @Order(12)
    void regexpReplaceWholeMatchBackref() throws SQLException {
        assertEquals("he[ll]o", query("SELECT regexp_replace('hello', '(ll)', '[\\&]')"));
    }

    @Test @Order(13)
    void substringRegexReturnsGroup1() throws SQLException {
        assertEquals("o", query("SELECT substring('foobar' from 'o(.)b')"));
    }

    @Test @Order(14)
    void substringRegexNoGroupReturnsWholeMatch() throws SQLException {
        assertEquals("oob", query("SELECT substring('foobar' from 'o.b')"));
    }

    @Test @Order(15)
    void similarToBoundedQuantifiers() throws SQLException {
        assertEquals("t", query("SELECT 'aab' SIMILAR TO 'a{2}%'"));
    }

    @Test @Order(16)
    void similarToBoundedQuantifiersNoMatch() throws SQLException {
        assertEquals("f", query("SELECT 'ab' SIMILAR TO 'a{2}%'"));
    }

    @Test @Order(17)
    void regexpReplaceInvalidBackrefNocrash() throws SQLException {
        // \1 with no group should not crash — PG treats gracefully
        String result = query("SELECT regexp_replace('hello', 'l', '\\1')");
        assertNotNull(result);
    }

    // ========== M23: format/quoting/string one-offs ==========

    @Test @Order(20)
    void formatWidthRightAlign() throws SQLException {
        assertEquals("     hello", query("SELECT format('%10s', 'hello')"));
    }

    @Test @Order(21)
    void formatWidthLeftAlign() throws SQLException {
        assertEquals("hello     |", query("SELECT format('%-10s|', 'hello')"));
    }

    @Test @Order(22)
    void formatMissingArgError() {
        assertEquals("22023", getSqlState("SELECT format('%s %s', 'only_one')"));
    }

    @Test @Order(23)
    void quoteLiteralWithBackslash() throws SQLException {
        String result = query("SELECT quote_literal(E'a\\\\b')");
        assertEquals("E'a\\\\b'", result);
    }

    @Test @Order(24)
    void concatTrue() throws SQLException {
        assertEquals("t", query("SELECT concat(true)"));
    }

    @Test @Order(25)
    void concatFalse() throws SQLException {
        assertEquals("f", query("SELECT concat(false)"));
    }

    @Test @Order(26)
    void concatWsZeroValueArgs() throws SQLException {
        // concat_ws with only the separator (no value args) does not match any function (PG 42883).
        assertEquals("42883", getSqlState("SELECT concat_ws(',')"));
    }

    @Test @Order(27)
    void splitPartEmptyDelimiter() throws SQLException {
        assertEquals("abc", query("SELECT split_part('abc', '', 1)"));
    }

    @Test @Order(28)
    void splitPartEmptyDelimiterField2() throws SQLException {
        assertEquals("", query("SELECT split_part('abc', '', 2)"));
    }

    @Test @Order(29)
    void lpadNegativeLength() throws SQLException {
        assertEquals("", query("SELECT lpad('hi', -1)"));
    }

    @Test @Order(30)
    void rpadNegativeLength() throws SQLException {
        assertEquals("", query("SELECT rpad('hi', -1)"));
    }

    @Test @Order(31)
    void unistr4Digit() throws SQLException {
        assertEquals("\u00E9", query("SELECT unistr('\\00E9')"));
    }

    @Test @Order(32)
    void unistr6Digit() throws SQLException {
        // U+1F600 = grinning face emoji
        String result = query("SELECT unistr('\\+01F600')");
        assertEquals("\uD83D\uDE00", result);
    }

    @Test @Order(33)
    void formatPositionalWithWidth() throws SQLException {
        assertEquals("     hello", query("SELECT format('%1$10s', 'hello')"));
    }

    @Test @Order(34)
    void quoteLiteralNoBackslash() throws SQLException {
        assertEquals("'hello'", query("SELECT quote_literal('hello')"));
    }

    @Test @Order(35)
    void quoteNullableWithBackslash() throws SQLException {
        String result = query("SELECT quote_nullable(E'a\\\\b')");
        assertEquals("E'a\\\\b'", result);
    }
}
