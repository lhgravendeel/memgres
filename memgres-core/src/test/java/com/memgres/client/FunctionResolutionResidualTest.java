package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which function was written decides what the answer means.
 *
 * <p><b>A tsquery literal is not a search phrase.</b> {@code 'Cats'::tsquery} names the lexeme
 * {@code Cats} and nothing else — no case folding, no stemming, no stop-word list — while
 * {@code to_tsquery('Cats')} runs the word through a dictionary and yields {@code 'cat'}. Reading a
 * literal through the dictionary silently turned {@code 'a & b'} into a query with a hole where the
 * stop word had been, which then matched rows nobody asked for.
 *
 * <p><b>PostgreSQL declares four {@code @@} operators and they disagree about bare strings.</b>
 * {@code text @@ tsquery} runs the text through {@code to_tsvector}; {@code text @@ text} also runs
 * the right side through {@code plainto_tsquery}, so {@code 'cat dog'} there is two words to find
 * rather than a tsquery with a syntax error in it. Which one was written is not something the
 * values can answer on their own — both sides arrive as strings.
 *
 * <p><b>An empty query matches nothing, and folds away when combined.</b> A stop word makes
 * {@code to_tsquery('a')} empty. Matching everything instead made it match every row, and keeping
 * it as an operand printed a query beginning with an operator.
 */
class FunctionResolutionResidualTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertRejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---------------------------------------------------------------- SECTION A
    // A tsquery literal names lexemes; to_tsquery makes them.

    @Test
    void aTsqueryLiteralIsTakenAsWritten() throws Exception {
        assertEquals("'a'", scalar("SELECT 'a'::tsquery::text"));
        assertEquals("'the'", scalar("SELECT 'the'::tsquery::text"));
        assertEquals("'cats'", scalar("SELECT 'cats'::tsquery::text"));
        assertEquals("'running'", scalar("SELECT 'running'::tsquery::text"));
        // Case is part of the lexeme, so it survives too.
        assertEquals("'Cats'", scalar("SELECT 'Cats'::tsquery::text"));
        assertEquals("'CATS' & 'Dogs'", scalar("SELECT 'CATS & Dogs'::tsquery::text"));
        // The operators and modifiers are still read as operators and modifiers.
        assertEquals("'a' & 'b'", scalar("SELECT 'a & b'::tsquery::text"));
        assertEquals("'a' | 'b'", scalar("SELECT 'a | b'::tsquery::text"));
        assertEquals("!'a'", scalar("SELECT '!a'::tsquery::text"));
        assertEquals("'a' <-> 'b'", scalar("SELECT 'a <-> b'::tsquery::text"));
        assertEquals("'a':*", scalar("SELECT 'a:*'::tsquery::text"));
        assertEquals("'a':A", scalar("SELECT 'a:A'::tsquery::text"));
    }

    @Test
    void toTsqueryRunsTheWordThroughItsConfiguration() throws Exception {
        assertEquals("'cat'", scalar("SELECT to_tsquery('Cats')::text"));
        assertEquals("'run'", scalar("SELECT to_tsquery('running')::text"));
        // simple folds case and no more: no stemming, no stop-word list.
        assertEquals("'cats'", scalar("SELECT to_tsquery('simple','Cats')::text"));
        assertEquals("'a'", scalar("SELECT to_tsquery('simple','a')::text"));
        // english drops the stop word entirely, leaving a query with nothing in it — which
        // renders as the empty string rather than as NULL.
        assertEquals("", scalar("SELECT to_tsquery('a')::text"));
        assertEquals("'cat'", scalar("SELECT plainto_tsquery('Cats')::text"));
    }

    /** The default is english, and it is what an unqualified to_tsvector uses. */
    @Test
    void theDefaultTextSearchConfigurationIsEnglish() throws Exception {
        assertEquals("pg_catalog.english", scalar("SHOW default_text_search_config"));
        assertEquals("'b':2", scalar("SELECT to_tsvector('a b')::text"));
        assertEquals("'cat':1", scalar("SELECT to_tsvector('cats')::text"));
        assertEquals("'a':1 'b':2", scalar("SELECT to_tsvector('simple','a b')::text"));
    }

    // ---------------------------------------------------------------- SECTION B
    // Four @@ operators, four ways to read a bare string.

    @Test
    void textOnTheLeftIsRunThroughToTsvector() throws Exception {
        // 'a' is an English stop word, so the document holds no such lexeme.
        assertEquals("false", scalar("SELECT ('a b'::text @@ 'a'::tsquery)::text"));
        assertEquals("true", scalar("SELECT ('a b'::text @@ 'b'::tsquery)::text"));
        // The document is stemmed and the query is not, so 'cats' matches the query 'cat'...
        assertEquals("true", scalar("SELECT ('cats'::text @@ 'cat'::tsquery)::text"));
        assertEquals("true", scalar("SELECT ('The Cats'::text @@ 'cat'::tsquery)::text"));
        // ...and does not match the query 'cats', which names a lexeme no document holds.
        assertEquals("false", scalar("SELECT ('cats'::text @@ 'cats'::tsquery)::text"));
    }

    @Test
    void textOnBothSidesSearchesForTheWords() throws Exception {
        // plainto_tsquery on the right: 'cat dog' is two words to find, not a query to parse.
        assertEquals("false", scalar("SELECT ('cat sat'::text @@ 'cat dog'::text)::text"));
        assertEquals("true", scalar("SELECT ('cat dog'::text @@ 'cat dog'::text)::text"));
        assertEquals("true", scalar("SELECT ('cat sat'::text @@ 'cats'::text)::text"));
        // ...so the punctuation is punctuation, not an operator.
        assertEquals("false", scalar("SELECT ('cat sat'::text @@ 'cat | zzz'::text)::text"));
        assertEquals("true", scalar("SELECT ('a b'::text @@ 'a & b'::text)::text"));
    }

    @Test
    void aVectorOnEitherSideReadsTheOtherAsAQuery() throws Exception {
        assertEquals("true", scalar("SELECT ('a b'::tsvector @@ 'a')::text"));
        assertEquals("true", scalar("SELECT ('a b'::tsvector @@ 'a & b')::text"));
        assertEquals("true", scalar("SELECT ('cats'::tsvector @@ 'cats')::text"));
        // The vector holds 'cats' as written, so the query 'cat' does not find it.
        assertEquals("false", scalar("SELECT ('cats'::tsvector @@ 'cat')::text"));
        // Opposite a vector a bare string is a query, so a bare pair of words is a syntax error.
        assertRejected("42601", "syntax error in tsquery", "SELECT 'a b'::tsvector @@ 'cat dog'");
        // The commuted form is the same operator.
        assertEquals("true", scalar("SELECT ('a'::tsquery @@ 'a b'::tsvector)::text"));
        assertEquals("true", scalar("SELECT ('a'::tsquery @@ to_tsvector('simple','a b'))::text"));
    }

    /**
     * An untyped literal takes the type the operator resolution leaves it. Beside a tsquery on the
     * right there is only {@code tsquery @@ tsvector} to fit, so it is a vector; on the left
     * {@code text @@ tsquery} also fits and text is the preferred type of its category, so it is
     * text. The same literal, read two ways, and the answers differ because of it.
     */
    @Test
    void anUntypedLiteralTakesTheTypeTheOperatorLeavesIt() throws Exception {
        assertEquals("false", scalar("SELECT ('a b' @@ 'a'::tsquery)::text"));
        assertEquals("true", scalar("SELECT ('a'::tsquery @@ 'a b')::text"));
        assertEquals("true", scalar("SELECT ('a b' @@ 'b'::tsquery)::text"));
    }

    // ---------------------------------------------------------------- SECTION C
    // An empty query names nothing.

    @Test
    void anEmptyQueryMatchesNothing() throws Exception {
        assertEquals("false", scalar("SELECT (''::tsquery @@ 'a'::tsvector)::text"));
        assertEquals("false", scalar("SELECT ('a'::tsvector @@ ''::tsquery)::text"));
        assertEquals("false", scalar("SELECT (to_tsvector('b') @@ to_tsquery('a'))::text"));
        assertEquals("0", scalar("SELECT numnode(''::tsquery)::text"));
    }

    @Test
    void anEmptyOperandFoldsAwayWhenQueriesAreCombined() throws Exception {
        assertEquals("'b'", scalar("SELECT (to_tsquery('a') && to_tsquery('b'))::text"));
        assertEquals("'b'", scalar("SELECT (to_tsquery('a') || to_tsquery('b'))::text"));
        assertEquals("'b'", scalar("SELECT (to_tsquery('a') <-> to_tsquery('b'))::text"));
        assertEquals("'cat'", scalar("SELECT (to_tsquery('cat') <-> to_tsquery('a'))::text"));
        assertEquals("!'z'", scalar("SELECT (to_tsquery('a') && !!to_tsquery('z'))::text"));
        // Two empties leave nothing, and negating nothing is still nothing.
        assertEquals("", scalar("SELECT (to_tsquery('a') && to_tsquery('the'))::text"));
        assertEquals("", scalar("SELECT (!!to_tsquery('a'))::text"));
        // A combination of two real queries is untouched by any of that.
        assertEquals("'cat' & 'dog'", scalar("SELECT (to_tsquery('cat') && to_tsquery('dog'))::text"));
        assertEquals("1", scalar("SELECT numnode(to_tsquery('a') && to_tsquery('b'))::text"));
        assertEquals("3", scalar("SELECT numnode('a & b'::tsquery)::text"));
    }

    // ---------------------------------------------------------------- SECTION D
    // The residuals around them.

    /** A NULL set of characters to trim is not an empty set: there is no answer to give. */
    @Test
    void theTrimFamilyIsStrictOnNull() throws Exception {
        assertNull(scalar("SELECT btrim('abc', NULL)"));
        assertNull(scalar("SELECT ltrim('abc', NULL)"));
        assertNull(scalar("SELECT rtrim('abc', NULL)"));
        assertNull(scalar("SELECT trim(both NULL from 'abc')"));
        assertNull(scalar("SELECT trim(leading NULL from 'abc')"));
        assertNull(scalar("SELECT trim(trailing NULL from 'abc')"));
        assertNull(scalar("SELECT btrim(NULL, 'a')"));
        // With characters to trim they still trim them, and the default is still whitespace.
        assertEquals("abc", scalar("SELECT btrim('xxabcxx', 'x')"));
        assertEquals("ab", scalar("SELECT btrim('  ab  ')"));
        assertEquals("bcx", scalar("SELECT ltrim('xxbcx', 'x')"));
        assertEquals("xxbc", scalar("SELECT rtrim('xxbcxx', 'x')"));
    }

    /**
     * {@code to_ascii} converts from a single-byte encoding, so it refuses the server's UTF8 —
     * which is the only answer it can give for the one-argument form here.
     */
    @Test
    void toAsciiConvertsOnlyFromTheEncodingsItCan() throws Exception {
        assertNull(scalar("SELECT to_ascii(NULL)"));
        assertNull(scalar("SELECT to_ascii('abc', NULL)"));
        assertRejected("0A000", "encoding conversion from UTF8 to ASCII not supported",
                "SELECT to_ascii('abc')");
        assertRejected("0A000", "encoding conversion from UTF8 to ASCII not supported",
                "SELECT to_ascii('abc', 'UTF8')");
        assertRejected("0A000", "encoding conversion from UTF8 to ASCII not supported",
                "SELECT to_ascii('abc', 6)");
        assertEquals("abc", scalar("SELECT to_ascii('abc', 'LATIN1')"));
        assertEquals("abc", scalar("SELECT to_ascii('abc', 'LATIN2')"));
        assertEquals("abc", scalar("SELECT to_ascii('abc', 'LATIN9')"));
        assertEquals("abc", scalar("SELECT to_ascii('abc', 'WIN1250')"));
        assertEquals("abc", scalar("SELECT to_ascii('abc', 8)"));
        // A name or a number that names no encoding is not guessed at.
        assertRejected("42704", "NOSUCHENC is not a valid encoding name",
                "SELECT to_ascii('abc', 'NOSUCHENC')");
        assertRejected("42704", "999 is not a valid encoding code",
                "SELECT to_ascii('abc', 999)");
    }

    /** The flags say which bounds were meant, so a NULL there is refused rather than defaulted. */
    @Test
    void aRangeConstructorRefusesNullFlags() {
        assertRejected("22000", "range constructor flags argument must not be null",
                "SELECT int4range(1, 5, NULL)");
        assertRejected("22000", "range constructor flags argument must not be null",
                "SELECT numrange(1, 5, NULL)");
        assertRejected("22000", "range constructor flags argument must not be null",
                "SELECT daterange(DATE '2020-01-01', DATE '2020-02-01', NULL)");
    }

    /** A NULL bound is still a bound: it means unbounded, and only the flags are refused. */
    @Test
    void aNullBoundIsStillAcceptedAsUnbounded() throws Exception {
        assertEquals("[1,)", scalar("SELECT int4range(1, NULL)::text"));
        assertEquals("(,5)", scalar("SELECT int4range(NULL, 5)::text"));
        // An int4range is canonicalised to the half-open form, so [] reads back as [1,6).
        assertEquals("[1,6)", scalar("SELECT int4range(1, 5, '[]')::text"));
    }

    /** The pretty form of an index definition drops the schema the search path already reaches. */
    @Test
    void pgGetIndexdefPrettyDropsAReachableSchema() throws Exception {
        exec("CREATE TABLE fr_t (a int, b int)");
        exec("CREATE INDEX fr_i ON fr_t (b)");
        exec("CREATE SCHEMA fr_s");
        exec("CREATE TABLE fr_s.t (a int)");
        exec("CREATE INDEX fr_si ON fr_s.t (a)");

        assertEquals("CREATE INDEX fr_i ON public.fr_t USING btree (b)",
                scalar("SELECT pg_get_indexdef('fr_i'::regclass)"));
        assertEquals("CREATE INDEX fr_i ON public.fr_t USING btree (b)",
                scalar("SELECT pg_get_indexdef('fr_i'::regclass, 0, false)"));
        assertEquals("CREATE INDEX fr_i ON fr_t USING btree (b)",
                scalar("SELECT pg_get_indexdef('fr_i'::regclass, 0, true)"));
        // A schema the path does not reach stays qualified, or the reader cannot tell which it is.
        assertEquals("CREATE INDEX fr_si ON fr_s.t USING btree (a)",
                scalar("SELECT pg_get_indexdef('fr_s.fr_si'::regclass, 0, true)"));
        // The column form is unaffected by pretty-printing.
        assertEquals("b", scalar("SELECT pg_get_indexdef('fr_i'::regclass, 1, true)"));
    }
}
