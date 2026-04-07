package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Checklist items 48-50: String Functions coverage tests.
 * Core string functions, pattern matching, and other string functions.
 */
class StringFunctionsCoverageTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    // ============================================================
    // Item 48: Core String Functions
    // ============================================================

    @Test
    void testLength() throws SQLException {
        assertEquals(5, queryInt("SELECT length('hello')"));
    }

    @Test
    void testCharLength() throws SQLException {
        assertEquals(5, queryInt("SELECT char_length('hello')"));
    }

    @Test
    void testCharacterLength() throws SQLException {
        assertEquals(5, queryInt("SELECT character_length('hello')"));
    }

    @Test
    void testOctetLength() throws SQLException {
        assertEquals(5, queryInt("SELECT octet_length('hello')"));
    }

    @Test
    void testOctetLengthMultibyte() throws SQLException {
        assertEquals(9, queryInt("SELECT octet_length('日本語')"));
    }

    @Test
    void testBitLength() throws SQLException {
        assertEquals(40, queryInt("SELECT bit_length('hello')"));
    }

    @Test
    void testLower() throws SQLException {
        assertEquals("hello", query1("SELECT lower('HELLO')"));
    }

    @Test
    void testUpper() throws SQLException {
        assertEquals("HELLO", query1("SELECT upper('hello')"));
    }

    @Test
    void testInitcap() throws SQLException {
        assertEquals("Hello World", query1("SELECT initcap('hello world')"));
    }

    @Test
    void testTrim() throws SQLException {
        assertEquals("hello", query1("SELECT trim('  hello  ')"));
    }

    @Test
    void testTrimLeading() throws SQLException {
        assertEquals("hello  ", query1("SELECT trim(leading ' ' from '  hello  ')"));
    }

    @Test
    void testTrimTrailing() throws SQLException {
        assertEquals("  hello", query1("SELECT trim(trailing ' ' from '  hello  ')"));
    }

    @Test
    void testTrimBothChar() throws SQLException {
        assertEquals("hello", query1("SELECT trim(both 'x' from 'xxhelloxx')"));
    }

    @Test
    void testLtrim() throws SQLException {
        assertEquals("hello", query1("SELECT ltrim('  hello')"));
    }

    @Test
    void testRtrim() throws SQLException {
        assertEquals("hello", query1("SELECT rtrim('hello  ')"));
    }

    @Test
    void testBtrim() throws SQLException {
        assertEquals("hello", query1("SELECT btrim('xxhelloxx', 'x')"));
    }

    @Test
    void testLpadDefault() throws SQLException {
        assertEquals("   hi", query1("SELECT lpad('hi', 5)"));
    }

    @Test
    void testLpadWithFill() throws SQLException {
        assertEquals("xyxhi", query1("SELECT lpad('hi', 5, 'xy')"));
    }

    @Test
    void testRpadDefault() throws SQLException {
        assertEquals("hi   ", query1("SELECT rpad('hi', 5)"));
    }

    @Test
    void testRpadWithFill() throws SQLException {
        assertEquals("hixyx", query1("SELECT rpad('hi', 5, 'xy')"));
    }

    @Test
    void testRepeat() throws SQLException {
        assertEquals("ababab", query1("SELECT repeat('ab', 3)"));
    }

    @Test
    void testReplace() throws SQLException {
        assertEquals("hello earth", query1("SELECT replace('hello world', 'world', 'earth')"));
    }

    @Test
    void testTranslate() throws SQLException {
        assertEquals("HELLO", query1("SELECT translate('hello', 'helo', 'HELO')"));
    }

    @Test
    void testReverse() throws SQLException {
        assertEquals("olleh", query1("SELECT reverse('hello')"));
    }

    @Test
    void testSubstringFromFor() throws SQLException {
        assertEquals("ell", query1("SELECT substring('hello' from 2 for 3)"));
    }

    @Test
    void testSubstringComma() throws SQLException {
        assertEquals("ell", query1("SELECT substring('hello', 2, 3)"));
    }

    @Test
    void testSubstringFromOnly() throws SQLException {
        assertEquals("ello", query1("SELECT substring('hello' from 2)"));
    }

    @Test
    void testLeft() throws SQLException {
        assertEquals("hel", query1("SELECT left('hello', 3)"));
    }

    @Test
    void testLeftNegative() throws SQLException {
        assertEquals("hel", query1("SELECT left('hello', -2)"));
    }

    @Test
    void testRight() throws SQLException {
        assertEquals("llo", query1("SELECT right('hello', 3)"));
    }

    @Test
    void testRightNegative() throws SQLException {
        assertEquals("llo", query1("SELECT right('hello', -2)"));
    }

    @Test
    void testConcat() throws SQLException {
        assertEquals("hello world", query1("SELECT concat('hello', ' ', 'world')"));
    }

    @Test
    void testConcatWs() throws SQLException {
        assertEquals("a, b, c", query1("SELECT concat_ws(', ', 'a', 'b', 'c')"));
    }

    @Test
    void testConcatWsSkipsNull() throws SQLException {
        assertEquals("a, c", query1("SELECT concat_ws(', ', 'a', NULL, 'c')"));
    }

    @Test
    void testFormatString() throws SQLException {
        assertEquals("Hello World, you are great",
                query1("SELECT format('Hello %s, you are %s', 'World', 'great')"));
    }

    @Test
    void testFormatIdentifier() throws SQLException {
        String result = query1("SELECT format('Value: %I', 'col_name')");
        assertTrue(result.contains("col_name"), "Expected result to contain col_name, got: " + result);
    }

    @Test
    void testEmptyStringIsNotNull() throws SQLException {
        assertTrue(queryBool("SELECT '' IS NOT NULL"));
    }

    @Test
    void testStringConcatenationOperator() throws SQLException {
        assertEquals("hello world", query1("SELECT 'hello' || ' ' || 'world'"));
    }

    // ============================================================
    // Item 49: Pattern Matching
    // ============================================================

    @Test
    void testLikePercent() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' LIKE 'hel%'"));
    }

    @Test
    void testLikeUnderscore() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' LIKE 'h_llo'"));
    }

    @Test
    void testNotLike() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' NOT LIKE 'world%'"));
    }

    @Test
    void testIlike() throws SQLException {
        assertTrue(queryBool("SELECT 'Hello' ILIKE 'hello'"));
    }

    @Test
    void testNotIlike() throws SQLException {
        assertFalse(queryBool("SELECT 'Hello' NOT ILIKE 'hello'"));
    }

    @Test
    void testSimilarTo() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' SIMILAR TO 'h(e|a)llo'"));
    }

    @Test
    void testNotSimilarTo() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' NOT SIMILAR TO 'world'"));
    }

    @Test
    void testPosixRegexMatch() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' ~ 'hel'"));
    }

    @Test
    void testPosixRegexCaseSensitive() throws SQLException {
        assertFalse(queryBool("SELECT 'Hello' ~ 'hello'"));
    }

    @Test
    void testPosixRegexCaseInsensitive() throws SQLException {
        assertTrue(queryBool("SELECT 'Hello' ~* 'hello'"));
    }

    @Test
    void testPosixRegexNegated() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' !~ 'world'"));
    }

    @Test
    void testPosixRegexNegatedCaseInsensitive() throws SQLException {
        assertFalse(queryBool("SELECT 'Hello' !~* 'hello'"));
    }

    @Test
    void testRegexpMatch() throws SQLException {
        String result = query1("SELECT regexp_match('foobarbequebaz', '(bar)(beque)')");
        assertEquals("{bar,beque}", result);
    }

    @Test
    void testRegexpReplace() throws SQLException {
        assertEquals("hello earth", query1("SELECT regexp_replace('hello world', 'world', 'earth')"));
    }

    @Test
    void testRegexpReplaceGlobal() throws SQLException {
        assertEquals("abcNdefN", query1("SELECT regexp_replace('abc123def456', '[0-9]+', 'N', 'g')"));
    }

    @Test
    void testRegexpCount() throws SQLException {
        assertEquals(2, queryInt("SELECT regexp_count('hello world hello', 'hello')"));
    }

    @Test
    void testRegexpLike() throws SQLException {
        assertTrue(queryBool("SELECT regexp_like('hello', '^h.*o$')"));
    }

    @Test
    void testRegexpLikeCaseInsensitive() throws SQLException {
        assertTrue(queryBool("SELECT regexp_like('HELLO', '^h.*o$', 'i')"));
    }

    @Test
    void testRegexpSubstr() throws SQLException {
        assertEquals("hello", query1("SELECT regexp_substr('hello world', '\\w+')"));
    }

    @Test
    void testRegexpInstr() throws SQLException {
        assertEquals(7, queryInt("SELECT regexp_instr('hello world', 'world')"));
    }

    @Test
    void testRegexpSplitToArray() throws SQLException {
        assertEquals("{hello,world,foo}", query1("SELECT regexp_split_to_array('hello world foo', '\\s+')"));
    }

    // ============================================================
    // Item 50: Other String Functions
    // ============================================================

    @Test
    void testPosition() throws SQLException {
        assertEquals(3, queryInt("SELECT position('ll' in 'hello')"));
    }

    @Test
    void testStrpos() throws SQLException {
        assertEquals(3, queryInt("SELECT strpos('hello', 'll')"));
    }

    @Test
    void testAscii() throws SQLException {
        assertEquals(65, queryInt("SELECT ascii('A')"));
    }

    @Test
    void testChrA() throws SQLException {
        assertEquals("A", query1("SELECT chr(65)"));
    }

    @Test
    void testChrSpace() throws SQLException {
        assertEquals(" ", query1("SELECT chr(32)"));
    }

    @Test
    void testEncodeBase64() throws SQLException {
        String result = query1("SELECT encode('hello'::bytea, 'base64')");
        assertNotNull(result);
        assertTrue(result.length() > 0, "Expected non-empty base64 encoding");
    }

    @Test
    void testDecodeBase64() throws SQLException {
        // decode returns bytea; just verify it does not error
        String result = query1("SELECT decode('aGVsbG8=', 'base64')");
        assertNotNull(result);
    }

    @Test
    void testEncodeHex() throws SQLException {
        String result = query1("SELECT encode('hello'::bytea, 'hex')");
        assertNotNull(result);
        assertTrue(result.length() > 0, "Expected non-empty hex encoding");
    }

    @Test
    void testMd5() throws SQLException {
        assertEquals("5d41402abc4b2a76b9719d911017c592", query1("SELECT md5('hello')"));
    }

    @Test
    void testQuoteIdentSimple() throws SQLException {
        assertEquals("hello", query1("SELECT quote_ident('hello')"));
    }

    @Test
    void testQuoteIdentNeedsQuoting() throws SQLException {
        assertEquals("\"Hello World\"", query1("SELECT quote_ident('Hello World')"));
    }

    @Test
    void testQuoteLiteral() throws SQLException {
        assertEquals("'hello'", query1("SELECT quote_literal('hello')"));
    }

    @Test
    void testQuoteLiteralNull() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT quote_literal(NULL)")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }
    }

    @Test
    void testQuoteNullableNull() throws SQLException {
        assertEquals("NULL", query1("SELECT quote_nullable(NULL)"));
    }

    @Test
    void testQuoteNullableValue() throws SQLException {
        assertEquals("'hello'", query1("SELECT quote_nullable('hello')"));
    }

    @Test
    void testSplitPart() throws SQLException {
        assertEquals("b", query1("SELECT split_part('a.b.c', '.', 2)"));
    }

    @Test
    void testSplitPartBeyond() throws SQLException {
        assertEquals("", query1("SELECT split_part('a.b.c', '.', 4)"));
    }

    @Test
    void testStartsWithTrue() throws SQLException {
        assertTrue(queryBool("SELECT starts_with('hello', 'hel')"));
    }

    @Test
    void testStartsWithFalse() throws SQLException {
        assertFalse(queryBool("SELECT starts_with('hello', 'world')"));
    }

    @Test
    void testToHex255() throws SQLException {
        assertEquals("ff", query1("SELECT to_hex(255)"));
    }

    @Test
    void testToHex16() throws SQLException {
        assertEquals("10", query1("SELECT to_hex(16)"));
    }

    @Test
    void testToHex0() throws SQLException {
        assertEquals("0", query1("SELECT to_hex(0)"));
    }

    @Test
    void testNormalize() throws SQLException {
        assertEquals("hello", query1("SELECT normalize('hello')"));
    }

    @Test
    void testUnicode() throws SQLException {
        assertEquals(65, queryInt("SELECT unicode('A')"));
    }

    @Test
    void testUnistr() throws SQLException {
        assertEquals("A", query1("SELECT unistr('\\0041')"));
    }

    // ============================================================
    // Additional Item 48 tests: edge cases and variations
    // ============================================================

    @Test
    void testLengthEmptyString() throws SQLException {
        assertEquals(0, queryInt("SELECT length('')"));
    }

    @Test
    void testLengthMultibyte() throws SQLException {
        assertEquals(3, queryInt("SELECT length('日本語')"));
    }

    @Test
    void testLowerEmpty() throws SQLException {
        assertEquals("", query1("SELECT lower('')"));
    }

    @Test
    void testLtrimWithChars() throws SQLException {
        assertEquals("hello", query1("SELECT ltrim('xyxyhello', 'xy')"));
    }

    @Test
    void testRtrimWithChars() throws SQLException {
        assertEquals("hello", query1("SELECT rtrim('helloxyxy', 'xy')"));
    }

    @Test
    void testRepeatZero() throws SQLException {
        assertEquals("", query1("SELECT repeat('ab', 0)"));
    }

    @Test
    void testRepeatOne() throws SQLException {
        assertEquals("ab", query1("SELECT repeat('ab', 1)"));
    }

    @Test
    void testReplaceNoMatch() throws SQLException {
        assertEquals("hello", query1("SELECT replace('hello', 'xyz', 'abc')"));
    }

    @Test
    void testReverseEmpty() throws SQLException {
        assertEquals("", query1("SELECT reverse('')"));
    }

    @Test
    void testReverseSingleChar() throws SQLException {
        assertEquals("a", query1("SELECT reverse('a')"));
    }

@Test
    void testConcatWithNull() throws SQLException {
        assertEquals("helloworld", query1("SELECT concat('hello', NULL, 'world')"));
    }

    @Test
    void testFormatInteger() throws SQLException {
        String result = query1("SELECT format('Count: %s', 42)");
        assertEquals("Count: 42", result);
    }

    @Test
    void testStringConcatNull() throws SQLException {
        // In PG, NULL || 'x' is NULL
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT NULL || 'hello'")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }
    }

    @Test
    void testSubstringCommaNoLength() throws SQLException {
        assertEquals("ello", query1("SELECT substring('hello', 2)"));
    }

    @Test
    void testLpadTruncate() throws SQLException {
        assertEquals("hel", query1("SELECT lpad('hello', 3)"));
    }

    @Test
    void testRpadTruncate() throws SQLException {
        assertEquals("hel", query1("SELECT rpad('hello', 3)"));
    }

    // ============================================================
    // Additional Item 49 tests: pattern matching edge cases
    // ============================================================

    @Test
    void testLikeExact() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' LIKE 'hello'"));
    }

    @Test
    void testLikeNoMatch() throws SQLException {
        assertFalse(queryBool("SELECT 'hello' LIKE 'world'"));
    }

    @Test
    void testIlikePattern() throws SQLException {
        assertTrue(queryBool("SELECT 'HELLO WORLD' ILIKE '%world'"));
    }

    @Test
    void testSimilarToPercentWildcard() throws SQLException {
        assertTrue(queryBool("SELECT 'hello world' SIMILAR TO '%world'"));
    }

    @Test
    void testPosixRegexAnchored() throws SQLException {
        assertTrue(queryBool("SELECT 'hello' ~ '^hello$'"));
    }

    @Test
    void testPosixRegexNoMatch() throws SQLException {
        assertFalse(queryBool("SELECT 'hello' ~ '^world$'"));
    }

    @Test
    void testRegexpCountZero() throws SQLException {
        assertEquals(0, queryInt("SELECT regexp_count('hello world', 'xyz')"));
    }

    @Test
    void testRegexpLikeFalse() throws SQLException {
        assertFalse(queryBool("SELECT regexp_like('hello', '^world$')"));
    }

    // ============================================================
    // Additional Item 50 tests: other string function edge cases
    // ============================================================

    @Test
    void testPositionNotFound() throws SQLException {
        assertEquals(0, queryInt("SELECT position('xyz' in 'hello')"));
    }

    @Test
    void testStrposNotFound() throws SQLException {
        assertEquals(0, queryInt("SELECT strpos('hello', 'xyz')"));
    }

    @Test
    void testAsciiLower() throws SQLException {
        assertEquals(97, queryInt("SELECT ascii('a')"));
    }

    @Test
    void testMd5Empty() throws SQLException {
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", query1("SELECT md5('')"));
    }

    @Test
    void testSplitPartFirst() throws SQLException {
        assertEquals("a", query1("SELECT split_part('a.b.c', '.', 1)"));
    }

    @Test
    void testSplitPartLast() throws SQLException {
        assertEquals("c", query1("SELECT split_part('a.b.c', '.', 3)"));
    }

    @Test
    void testQuoteLiteralWithQuote() throws SQLException {
        String result = query1("SELECT quote_literal('he''llo')");
        assertNotNull(result);
        assertTrue(result.contains("he"), "Expected result to contain the string");
    }
}
