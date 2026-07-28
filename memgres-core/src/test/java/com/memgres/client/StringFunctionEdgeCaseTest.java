package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The degenerate corners of the string surface. A LIKE pattern ending in its escape character
 * was silently false rather than an error, PostgreSQL's newline-sensitivity option letters
 * reached Java's regex engine unrewritten, POSIX bracket classes went unchecked, and an empty
 * delimiter, separator or format was read as an instruction to split, join or pad rather than
 * as the nothing PostgreSQL takes it for.
 */
class StringFunctionEdgeCaseTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE sfe_t (v text)");
        exec("INSERT INTO sfe_t VALUES ('a'), ('b')");
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

    private static void assertScalar(String expected, String sql) throws SQLException {
        assertEquals(expected, scalar(sql), sql);
    }

    private static void assertError(String sqlState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    private static void assertError(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    @Test
    void aLikePatternMustNotEndWithTheEscapeCharacter() {
        String msg = "LIKE pattern must not end with escape character";
        assertError("22025", msg, "SELECT 'abc' LIKE 'ab\\'");
        assertError("22025", msg, "SELECT 'abc' ILIKE 'ab\\'");
        assertError("22025", msg, "SELECT 'abc' NOT LIKE 'ab\\'");
        assertError("22025", msg, "SELECT 'abc' NOT ILIKE 'ab\\'");
        assertError("22025", msg, "SELECT 'abc' ~~ 'ab\\'");
        assertError("22025", msg, "SELECT 'abc' !~~ 'ab\\'");
        assertError("22025", msg, "SELECT 'abc' ~~* 'AB\\'");
        assertError("22025", msg, "SELECT 'abc' LIKE 'ab!' ESCAPE '!'");
        assertError("22025", msg, "SELECT 'abc' ILIKE 'AB!' ESCAPE '!'");
        assertError("22025", msg, "SELECT 'abc' LIKE '\\'");
        assertError("22025", msg, "SELECT 'abc' LIKE '%\\'");
        assertError("22025", msg, "SELECT 'abc' LIKE '_\\'");
        assertError("22025", msg, "SELECT 'abcd' LIKE '%c\\'");
    }

    @Test
    void theComplaintOnlyComesWhenMatchingReachesTheEscape() throws Exception {
        // the text runs out before the pattern does, so PG never walks onto the escape
        assertScalar("f", "SELECT 'ab' LIKE 'ab\\'");
        assertScalar("f", "SELECT 'a' LIKE 'ab\\'");
        assertScalar("f", "SELECT '' LIKE '\\'");
        assertScalar("f", "SELECT 'abc' LIKE 'abc\\'");
        // matching fails earlier in the pattern
        assertScalar("f", "SELECT 'abc' LIKE 'x\\'");
        assertScalar("f", "SELECT 'abc' LIKE '%x\\'");
        // the first backslash escapes the second, so only a literal one is compared
        assertScalar("f", "SELECT 'abc' LIKE 'ab\\\\\\'");
        // an empty ESCAPE clause turns escaping off entirely
        assertScalar("f", "SELECT 'abc' LIKE 'ab\\' ESCAPE ''");
        assertScalar("t", "SELECT 'ab\\' LIKE 'ab\\' ESCAPE ''");
        assertError("22025", "invalid escape string", "SELECT 'abc' LIKE 'ab' ESCAPE 'xy'");
    }

    @Test
    void ordinaryLikePatternsAreUntouched() throws Exception {
        assertScalar("f", "SELECT 'abc' LIKE ''");
        assertScalar("t", "SELECT '' LIKE ''");
        assertScalar("t", "SELECT 'abc' LIKE '%'");
        assertScalar("t", "SELECT 'abc' LIKE 'a%b%c'");
        assertScalar("t", "SELECT 'aaa' LIKE '%a%a%a%'");
        assertScalar("t", "SELECT 'abc' LIKE '_%_'");
        assertScalar("f", "SELECT 'a' LIKE '_%_'");
        assertScalar("t", "SELECT 'a%c' LIKE 'a\\%c'");
        assertScalar("t", "SELECT 'a_c' LIKE 'a\\_c'");
        assertScalar("t", "SELECT 'a\\b' LIKE 'a\\\\b'");
        assertScalar("t", "SELECT 'ab\\' LIKE '%\\\\'");
        assertScalar("f", "SELECT 'axc' LIKE 'a.c'");
        assertScalar("t", "SELECT 'a*c' LIKE 'a*c'");
        assertScalar("f", "SELECT 'abc' LIKE 'A%'");
        assertScalar("t", "SELECT 'abc' ILIKE 'A%'");
        // a wildcard spans a newline
        assertScalar("t", "SELECT E'a\\nb' LIKE 'a%b'");
        assertScalar("t", "SELECT E'a\\nb' LIKE 'a_b'");
        assertNull(scalar("SELECT 'abc' LIKE NULL"));
    }

    @Test
    void similarToDropsATrailingEscape() throws Exception {
        assertScalar("t", "SELECT 'abc' SIMILAR TO 'abc!' ESCAPE '!'");
        assertScalar("f", "SELECT 'ab\\' SIMILAR TO 'ab\\'");
        assertScalar("f", "SELECT 'abc' SIMILAR TO 'ab\\'");
        assertScalar("t", "SELECT 'abc' NOT SIMILAR TO 'ab\\'");
        // the rest of SIMILAR TO is unaffected
        assertScalar("t", "SELECT 'abc' SIMILAR TO '(a|b)bc'");
        assertScalar("t", "SELECT 'a%c' SIMILAR TO 'a\\%c'");
        assertScalar("f", "SELECT 'abc' SIMILAR TO 'a\\%c'");
        assertScalar("t", "SELECT E'a\\nb' SIMILAR TO 'a%b'");
        assertScalar("b", "SELECT substring('abc' SIMILAR 'a#\"b#\"c' ESCAPE '#')");
    }

    @Test
    void aRegularExpressionIsNewlineInsensitiveByDefault() throws Exception {
        assertScalar("t", "SELECT E'a\\nb' ~ 'a.b'");
        assertScalar("t", "SELECT E'a\\nb' ~* 'A.B'");
        assertScalar("f", "SELECT E'a\\nb' !~ 'a.b'");
        assertScalar("f", "SELECT E'a\\nb' ~ '^b'");
        assertScalar("f", "SELECT E'a\\nb' ~ 'a$'");
        assertScalar("t", "SELECT E'a\\nb' ~ 'b$'");
        assertScalar("f", "SELECT E'a\\nb\\n' ~ 'b$'");
        assertScalar("X", "SELECT regexp_replace(E'a\\nb', 'a.b', 'X')");
        assertScalar("t", "SELECT regexp_like(E'a\\nb', 'a.b')");
        assertScalar("1", "SELECT regexp_count(E'a\\nb', 'a.b')");
        assertScalar("{\"\",\"\",\"\",\"\"}", "SELECT regexp_split_to_array(E'a\\nb', '.')");
    }

    @Test
    void theNewlineSensitivityOptionLetters() throws Exception {
        // p: no dot over a newline, no line anchors
        assertScalar("f", "SELECT E'a\\nb' ~ '(?p)a.b'");
        assertScalar("f", "SELECT E'a\\nb' ~ '(?p)^b'");
        // w: dot over a newline and line anchors both
        assertScalar("t", "SELECT E'a\\nb' ~ '(?w)a.b'");
        assertScalar("t", "SELECT E'a\\nb' ~ '(?w)^b'");
        // n and its synonym m: newline-sensitive
        assertScalar("f", "SELECT E'a\\nb' ~ '(?n)a.b'");
        assertScalar("t", "SELECT E'a\\nb' ~ '(?n)^b'");
        assertScalar("f", "SELECT E'a\\nb' ~ '(?m)a.b'");
        assertScalar("t", "SELECT E'a\\nb' ~ '(?m)^b'");
        // s restates the default
        assertScalar("t", "SELECT E'a\\nb' ~ '(?s)a.b'");
        // the same letters as a flags argument
        assertScalar("a\nb", "SELECT regexp_replace(E'a\\nb', 'a.b', 'X', 'p')");
        assertScalar("a\nb", "SELECT regexp_replace(E'a\\nb', 'a.b', 'X', 'n')");
        assertScalar("X", "SELECT regexp_replace(E'a\\nb', 'a.b', 'X', 'w')");
        assertScalar("X", "SELECT regexp_replace(E'a\\nb', 'a.b', 'X', 's')");
    }

    @Test
    void theRemainingOptionLettersAndDirectors() throws Exception {
        assertScalar("t", "SELECT E'a\\nb' ~ '(?i)A.B'");
        assertScalar("f", "SELECT 'AB' ~ '(?c)ab'");
        assertScalar("t", "SELECT 'ab' ~ '(?t)ab'");
        assertScalar("f", "SELECT 'a b' ~ '(?t) a b '");
        assertScalar("t", "SELECT 'ab' ~ '(?x) a b '");
        assertScalar("t", "SELECT 'ab' ~ '(?ix) A B '");
        // q makes the rest of the pattern a literal string
        assertScalar("t", "SELECT 'a.b' ~ '(?q)a.b'");
        assertScalar("f", "SELECT 'axb' ~ '(?q)a.b'");
        // b and e ask for the older syntaxes and are accepted
        assertScalar("t", "SELECT E'a\\nb' ~ '(?e)a.b'");
        assertScalar("t", "SELECT E'a\\nb' ~ '(?b)a.b'");
        // the *** directors
        assertScalar("t", "SELECT 'a.b' ~ '***=a.b'");
        assertScalar("f", "SELECT 'axb' ~ '***=a.b'");
        assertScalar("t", "SELECT 'axb' ~ '***:a.b'");
        // ordinary Java-style groups are not option directors
        assertScalar("t", "SELECT 'aXb' ~ 'a(?:X)b'");
        assertScalar("t", "SELECT 'ab' ~ 'a(?=b)'");
        assertScalar("f", "SELECT 'ab' ~ 'a(?!b)'");
        // an unknown letter in a director is rejected
        assertError("2201B", "invalid embedded option", "SELECT 'abc' ~ '(?z)abc'");
        assertError("2201B", "invalid embedded option", "SELECT 'ab' ~ '(?g)ab'");
    }

    @Test
    void posixCharacterClasses() throws Exception {
        assertScalar("t", "SELECT 'a1' ~ '[[:alpha:]][[:digit:]]'");
        assertScalar("t", "SELECT 'a' ~ '[[:word:]]'");
        assertScalar("t", "SELECT '_' ~ '[[:word:]]'");
        assertScalar("t", "SELECT 'a' ~ '[[:ascii:]]'");
        assertScalar("t", "SELECT 'ab cd' ~ 'ab[[:>:]]'");
        assertScalar("t", "SELECT 'ab cd' ~ '[[:<:]]cd'");
        assertScalar("t", "SELECT 'a' ~ '[[:alpha:][:digit:]]'");
        assertScalar("f", "SELECT 'a' ~ '[^[:alpha:]]'");
        assertScalar("t", "SELECT 'x' ~ '[[:alpha:]-]'");
        // a bracket in first position and the other literals Java reads differently
        assertScalar("t", "SELECT '[' ~ '[[]'");
        assertScalar("t", "SELECT ']' ~ '[]]'");
        assertScalar("t", "SELECT 'a' ~ '[^]]'");
        assertScalar("t", "SELECT 'a:' ~ '[:]'");
        assertScalar("t", "SELECT '&' ~ '[&]'");
        assertScalar("t", "SELECT 'a' ~ '[[.a.]]'");
        assertScalar("t", "SELECT 'a' ~ '[[=a=]]'");
        assertScalar("t", "SELECT 'a-b' ~ '[a\\-b]'");
        // an unknown class name, and brackets that never close
        assertError("2201B", "invalid character class", "SELECT 'a' ~ '[[:foo:]]'");
        assertError("2201B", "invalid character class", "SELECT 'a' ~ '[[:ALPHA:]]'");
        assertError("2201B", "brackets [] not balanced", "SELECT 'a' ~ '[[:alpha]]'");
        assertError("2201B", "brackets [] not balanced", "SELECT 'a' ~ '[abc'");
        assertError("2201B", "brackets [] not balanced", "SELECT 'a' ~ '[[:alpha:]'");
    }

    @Test
    void anUnknownFlagLetterIsRejected() throws Exception {
        assertError("22023", "invalid regular expression option: \"z\"",
                "SELECT regexp_replace('abc', 'b', 'X', 'z')");
        assertError("22023", "invalid regular expression option: \"G\"",
                "SELECT regexp_replace('abc', 'b', 'X', 'G')");
        assertError("22023", "SELECT regexp_replace('abc', 'b', 'X', 'gz')");
        assertError("22023", "SELECT regexp_replace('abc', 'b', 'X', ' ')");
        assertError("22023", "SELECT regexp_matches('abc', 'b', 'z')");
        assertError("22023", "SELECT regexp_like('abc', 'b', 'z')");
        assertError("22023", "SELECT regexp_count('abc', 'b', 1, 'z')");
        assertError("22023", "SELECT regexp_substr('abc', 'b', 1, 1, 'z')");
        assertError("22023", "SELECT regexp_instr('abc', 'b', 1, 1, 0, 'z')");
        assertError("22023", "SELECT regexp_split_to_array('abc', 'b', 'z')");
        // g belongs only to the functions that can replace or return more than one match
        assertError("22023", "does not support the \"global\" option",
                "SELECT regexp_like('abc', 'b', 'g')");
        // the letters that are valid keep working
        assertScalar("aXc", "SELECT regexp_replace('abc', 'b', 'X', '')");
        assertScalar("aXc", "SELECT regexp_replace('abc', 'b', 'X', 'c')");
        assertScalar("aXcaXc", "SELECT regexp_replace('abcabc', 'b', 'X', 'g')");
        assertScalar("aXcAXC", "SELECT regexp_replace('abcABC', 'b', 'X', 'gi')");
        assertScalar("ABC", "SELECT regexp_replace('ABC', 'b', 'X', 'ic')");
    }

    @Test
    void nullArgumentsKeepTheRegexpFamilyStrict() throws Exception {
        assertNull(scalar("SELECT regexp_replace('abc', NULL, 'X')"));
        assertNull(scalar("SELECT regexp_replace('abc', 'b', NULL)"));
        assertNull(scalar("SELECT regexp_replace('abc', 'b', 'X', NULL)"));
        assertNull(scalar("SELECT regexp_like('abc', NULL)"));
        assertNull(scalar("SELECT regexp_count('abc', NULL)"));
        assertNull(scalar("SELECT regexp_instr('abc', NULL)"));
        assertNull(scalar("SELECT regexp_substr('abc', NULL)"));
        assertNull(scalar("SELECT regexp_split_to_array('abc', NULL)"));
        assertNull(scalar("SELECT regexp_split_to_array('abc', 'b', NULL)"));
        assertNull(scalar("SELECT 'a' ~ NULL"));
    }

    @Test
    void aZeroLengthMatchDoesNotAddAField() throws Exception {
        assertScalar("{a,b,c}", "SELECT regexp_split_to_array('abc', '')");
        assertScalar("3", "SELECT array_length(regexp_split_to_array('abc', ''), 1)");
        assertScalar("{a,b,c}", "SELECT regexp_split_to_array('abc', 'x*')");
        assertScalar("{a,b,\"\"}", "SELECT regexp_split_to_array('abc', 'c*')");
        assertScalar("{a,b,\"\"}", "SELECT regexp_split_to_array('abcc', 'c*')");
        assertScalar("{\"\",b,c}", "SELECT regexp_split_to_array('abc', 'a*')");
        // an ordinary delimiter still keeps its leading and trailing empties
        assertScalar("{a,c}", "SELECT regexp_split_to_array('abc', 'b')");
        assertScalar("{abc}", "SELECT regexp_split_to_array('abc', 'z')");
        assertScalar("{\"\",abc}", "SELECT regexp_split_to_array('Xabc', 'X')");
        assertScalar("{abc,\"\"}", "SELECT regexp_split_to_array('abcX', 'X')");
        assertScalar("{a,b,c}", "SELECT regexp_split_to_array('a1b2c', '[0-9]')");
        // and the table form splits the same way
        assertScalar("3", "SELECT count(*) FROM regexp_split_to_table('abc', '')");
        assertScalar("4", "SELECT count(*) FROM regexp_split_to_table(E'a\\nb', '.')");
        assertScalar("abc|", "SELECT array_to_string(array(SELECT * FROM regexp_split_to_table('abcX','X')), '|')");
    }

    @Test
    void anEmptyMatchIsStillAMatch() throws Exception {
        assertScalar("{\"\"}", "SELECT regexp_matches('abc', 'x*')");
        assertScalar("{\"\"}", "SELECT regexp_matches('abc', '')");
        assertScalar("1", "SELECT array_length(regexp_matches('abc', 'x*'), 1)");
        assertScalar("4", "SELECT count(*) FROM regexp_matches('abc', 'x*', 'g')");
        // an element that holds a separator of its own is quoted, as array_out does
        assertScalar("{a,b}", "SELECT regexp_matches('abc', '(a)(b)')");
        assertScalar("{\"a,b\"}", "SELECT regexp_matches('a,b', '(a,b)')");
        assertScalar("{\"a b\"}", "SELECT regexp_matches('a b', '(a b)')");
        assertScalar("{a,NULL,NULL}", "SELECT regexp_matches('abc', '(a)(x)?(c)?')");
        assertScalar("a b", "SELECT (regexp_matches('a b', '(a b)'))[1]");
        assertScalar("a,b", "SELECT (regexp_match('a,b', '(a,b)'))[1]");
    }

    @Test
    void anEmptyDelimiterIsNotAnInstructionToSplit() throws Exception {
        assertScalar("{abc}", "SELECT string_to_array('abc', '')");
        assertScalar("1", "SELECT array_length(string_to_array('abc', ''), 1)");
        assertScalar("{NULL}", "SELECT string_to_array('abc', '', 'abc')");
        assertScalar("{abc}", "SELECT string_to_array('abc', '', 'x')");
        // a NULL delimiter still means every character
        assertScalar("{a,b,c}", "SELECT string_to_array('abc', NULL)");
        assertScalar("{}", "SELECT string_to_array('', '')");
        assertScalar("{a,b}", "SELECT string_to_array('a,b', ',')");
        assertScalar("{a,a,\"\"}", "SELECT string_to_array('abcabc', 'bc')");
        assertScalar("{NULL,c}", "SELECT string_to_array('abc', 'b', 'a')");
        assertNull(scalar("SELECT string_to_array(NULL, ',')"));
    }

    @Test
    void thereIsNothingToFindInAnEmptyNeedle() throws Exception {
        assertScalar("abc", "SELECT replace('abc', '', 'X')");
        assertScalar("", "SELECT replace('', '', 'X')");
        assertScalar("abc", "SELECT replace('abc', '', '')");
        // the ordinary cases are unchanged
        assertScalar("ac", "SELECT replace('abc', 'b', '')");
        assertScalar("bbbbbb", "SELECT replace('aaa', 'a', 'bb')");
        assertNull(scalar("SELECT replace('abc', NULL, 'X')"));
        // the neighbouring degenerate arguments already behaved
        assertScalar("abc", "SELECT translate('abc', '', 'X')");
        assertScalar("abc", "SELECT split_part('abc', '', 1)");
        assertScalar("abc", "SELECT lpad('abc', 6, '')");
        assertScalar("abc", "SELECT rpad('abc', 6, '')");
        assertScalar("", "SELECT repeat('abc', -1)");
        assertScalar("ab", "SELECT concat_ws('', 'a', 'b')");
    }

    @Test
    void aNullSeparatorJoinsWithNothingAtAll() throws Exception {
        assertScalar("ab", "SELECT string_agg(v, NULL) FROM sfe_t");
        assertScalar("ab", "SELECT string_agg(v, NULL ORDER BY v) FROM sfe_t");
        assertScalar("ab", "SELECT string_agg(v, '') FROM sfe_t");
        assertScalar("a,b", "SELECT string_agg(v, ',') FROM sfe_t");
    }

    @Test
    void anEmptyFormatString() throws Exception {
        assertScalar("", "SELECT to_char(1, '')");
        assertScalar("", "SELECT to_char(1.5, '')");
        assertScalar("", "SELECT to_char(1::bigint, '')");
        assertScalar("", "SELECT to_char(1.5::float8, '')");
        // the date/time form answers NULL instead
        assertNull(scalar("SELECT to_char(now(), '')"));
        assertNull(scalar("SELECT to_char(now()::date, '')"));
        assertNull(scalar("SELECT to_char(1, NULL)"));
        assertNull(scalar("SELECT to_char(NULL::numeric, '')"));
        // a real format is unaffected
        assertScalar(" 1", "SELECT to_char(1, '9')");
        assertScalar(" 1234.5", "SELECT to_char(1234.5, '9999.9')");
    }

    @Test
    void aNegativeRoundingScalePrintsInFull() throws Exception {
        assertScalar("0", "SELECT round(1.5, -1)::text");
        assertScalar("0", "SELECT trunc(1.5, -1)::text");
        assertScalar("20", "SELECT round(15, -1)::text");
        assertScalar("1200", "SELECT round(1234.5, -2)::text");
        assertScalar("1200", "SELECT trunc(1234.5, -2)::text");
        assertScalar("-20", "SELECT round(-15.0, -1)::text");
        assertScalar("0", "SELECT round(1.5::numeric, -5)::text");
        // a large or small exponent is spelled out too
        assertScalar("20", "SELECT (2e1::numeric)::text");
        assertScalar("0.0000000001", "SELECT (1e-10::numeric)::text");
        // and an ordinary scale is untouched
        assertScalar("2", "SELECT round(1.5, 0)::text");
        assertScalar("1.6", "SELECT round(1.55, 1)::text");
        assertScalar("1.50", "SELECT (1.50::numeric)::text");
    }

    @Test
    void aDimensionTheArrayDoesNotHaveYieldsNoSubscripts() throws Exception {
        assertScalar("0", "SELECT count(*) FROM generate_subscripts(ARRAY[1,2], 5)");
        assertScalar("0", "SELECT count(*) FROM generate_subscripts(ARRAY[1,2], 2)");
        assertScalar("0", "SELECT count(*) FROM generate_subscripts(ARRAY[1,2], 0)");
        assertScalar("0", "SELECT count(*) FROM generate_subscripts(ARRAY[1,2], -1)");
        assertScalar("0", "SELECT count(*) FROM generate_subscripts(ARRAY[1,2], NULL)");
        assertScalar("0", "SELECT count(*) FROM generate_subscripts(ARRAY[[1,2],[3,4]], 3)");
        // the dimensions it does have are unchanged
        assertScalar("2", "SELECT count(*) FROM generate_subscripts(ARRAY[1,2], 1)");
        assertScalar("2", "SELECT count(*) FROM generate_subscripts(ARRAY[[1,2],[3,4]], 2)");
        assertScalar("2", "SELECT count(*) FROM generate_subscripts(ARRAY[1,2], 1, true)");
        assertScalar("0", "SELECT count(*) FROM generate_subscripts('{}'::int[], 1)");
    }
}
