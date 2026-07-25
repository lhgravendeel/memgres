package com.memgres.fts;

import com.memgres.core.Memgres;
import com.memgres.engine.PgFloatFormat;
import com.memgres.engine.fts.EnglishStemmer;
import com.memgres.engine.fts.StopWords;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Full-text search must produce byte-identical lexemes, ranks and float output to
 * PostgreSQL 18. Every expectation here was captured from a live PG 18.0 server.
 *
 * <p>Getting {@code to_tsvector} right needs all three of PG's pieces at once: its
 * default parser's token types, its exact 127-word stop list, and the bundled Snowball
 * (Porter2) stemmer. Any one of them differing changes the lexemes.
 */
class FtsExactnessTest {

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

    private static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ------------------------------------------------------------------
    // Snowball english (Porter2)
    // ------------------------------------------------------------------

    @Test
    void stemmerMatchesSnowball() {
        assertEquals("run", EnglishStemmer.stem("running"));
        assertEquals("run", EnglishStemmer.stem("runs"));
        assertEquals("ran", EnglishStemmer.stem("ran"));
        assertEquals("easili", EnglishStemmer.stem("easily"));
        assertEquals("fair", EnglishStemmer.stem("fairly"));
        assertEquals("happi", EnglishStemmer.stem("happiness"));
        assertEquals("general", EnglishStemmer.stem("generalization"));
        assertEquals("fox", EnglishStemmer.stem("foxes"));
        assertEquals("jump", EnglishStemmer.stem("jumped"));
        assertEquals("lazili", EnglishStemmer.stem("lazily"));
        assertEquals("relat", EnglishStemmer.stem("relational"));
        assertEquals("feudal", EnglishStemmer.stem("feudalism"));
        assertEquals("sensibl", EnglishStemmer.stem("sensibility"));
        assertEquals("oper", EnglishStemmer.stem("operating"));
    }

    /** Snowball never shortens the double in a three-letter stem. */
    @Test
    void stemmerKeepsShortDoubles() {
        assertEquals("add", EnglishStemmer.stem("added"));
        assertEquals("add", EnglishStemmer.stem("adding"));
        assertEquals("egg", EnglishStemmer.stem("egged"));
        assertEquals("pad", EnglishStemmer.stem("padded"));
        assertEquals("hop", EnglishStemmer.stem("hopped"));
        assertEquals("bed", EnglishStemmer.stem("bedded"));
    }

    @Test
    void stemmerHonoursExceptionLists() {
        assertEquals("ski", EnglishStemmer.stem("skis"));
        assertEquals("sky", EnglishStemmer.stem("skies"));
        assertEquals("die", EnglishStemmer.stem("dying"));
        assertEquals("gentl", EnglishStemmer.stem("gently"));
        assertEquals("earli", EnglishStemmer.stem("early"));
        assertEquals("news", EnglishStemmer.stem("news"));
        assertEquals("inning", EnglishStemmer.stem("inning"));
        assertEquals("proceed", EnglishStemmer.stem("proceed"));
    }

    @Test
    void stopListHasPgsExact127Entries() {
        assertEquals(127, StopWords.ENGLISH.size());
        assertTrue(StopWords.isEnglishStopWord("don"));
        assertTrue(StopWords.isEnglishStopWord("s"));
        assertTrue(StopWords.isEnglishStopWord("themselves"));
        assertTrue(StopWords.isEnglishStopWord("further"));
        // Words the previous ad-hoc list wrongly treated as stop words.
        assertTrue(!StopWords.isEnglishStopWord("would"));
        assertTrue(!StopWords.isEnglishStopWord("could"));
        assertTrue(!StopWords.isEnglishStopWord("may"));
        assertTrue(!StopWords.isEnglishStopWord("shall"));
    }

    // ------------------------------------------------------------------
    // The default parser's token types
    // ------------------------------------------------------------------

    @Test
    void toTsvectorMatchesPg() throws Exception {
        assertEquals("'brown':3 'dog':9 'fox':4 'jump':5 'lazili':7 'quick':2 'run':8",
                q("SELECT to_tsvector('english', 'The Quick brown foxes jumped over lazily running dogs')::text"));
    }

    /** Emails, URLs, hosts and paths survive as single lexemes via the simple dictionary. */
    @Test
    void toTsvectorKeepsEmailsAndUrls() throws Exception {
        assertEquals("'contact':1 'john.doe@example.com':2",
                q("SELECT to_tsvector('english', 'Contact john.doe@example.com')::text"));
        assertEquals("'/path?x=1':4 'visit':1 'www.example.com':3 'www.example.com/path?x=1':2",
                q("SELECT to_tsvector('english', 'visit https://www.example.com/path?x=1')::text"));
        assertEquals("'localhost':2 'visit':1", q("SELECT to_tsvector('english', 'visit localhost')::text"));
    }

    /** A hyphenated compound yields the whole word plus each part, all stemmed. */
    @Test
    void toTsvectorSplitsHyphenatedCompounds() throws Exception {
        assertEquals("'art':8 'co':10 'co-oper':9 'known':3 'oper':11 'state':5 "
                        + "'state-of-the-art':4 'well':2 'well-known':1",
                q("SELECT to_tsvector('english', 'well-known state-of-the-art co-operate')::text"));
    }

    @Test
    void toTsvectorTypesNumbersTheWayPgDoes() throws Exception {
        assertEquals("'-01':2 '-15':3 '192.168.1.1':6 '2024':1 '3.14':5 'v1.2.3':4",
                q("SELECT to_tsvector('english', '2024-01-15 v1.2.3 3.14 192.168.1.1')::text"));
        assertEquals("'+2.5':4 '-1.5e+10':3 '1.5e-3':2 '1e10':1",
                q("SELECT to_tsvector('english', '1e10 1.5e-3 -1.5E+10 +2.5')::text"));
    }

    /** Underscore is not a word character to PG's parser. */
    @Test
    void toTsvectorSplitsOnUnderscore() throws Exception {
        assertEquals("'bar':2 'baz':3 'foo':1 'qux':4",
                q("SELECT to_tsvector('english', 'foo_bar baz_qux')::text"));
    }

    /** Markup is typed as tag/entity, which the english configuration maps to no dictionary. */
    @Test
    void toTsvectorHandlesMarkup() throws Exception {
        assertEquals("'bold':2", q("SELECT to_tsvector('english', 'before <b>bold</b> after')::text"));
        assertEquals("'b':2 'c':3", q("SELECT to_tsvector('english', 'a &amp; b &#65; c')::text"));
    }

    /** Text inside script/style is skipped entirely, as PG's SpecialTags handler arranges. */
    @Test
    void toTsvectorSkipsScriptAndStyleBodies() throws Exception {
        assertEquals("'visibl':1",
                q("SELECT to_tsvector('english', '<script>var x=1;</script> visible')::text"));
    }

    /** A dotted name with a :line suffix is one host token, as in a stack trace. */
    @Test
    void toTsvectorKeepsQualifiedNamesWhole() throws Exception {
        assertEquals("'-523':5 'foo.java:494':4 'infoschemabuilder.java:597':2 'see':1",
                q("SELECT to_tsvector('english', 'see InfoSchemaBuilder.java:597 and Foo.java:494-523')::text"));
        assertEquals("'information_schema.table':1 'privileg':2",
                q("SELECT to_tsvector('english', 'information_schema.table_privileges here')::text"));
        assertEquals("'in/out':4 'parser/ast':2 'path':1",
                q("SELECT to_tsvector('english', 'path parser/ast/ and in/out/')::text"));
    }

    @Test
    void toTsvectorTypesFilePaths() throws Exception {
        assertEquals("'/etc/passwd':4 '/rel':2 '/up':3 '~/home/user':1",
                q("SELECT to_tsvector('english', '~/home/user ./rel ../up /etc/passwd')::text"));
    }

    /** A numeric part after a hyphen is a signed integer, not a compound part. */
    @Test
    void toTsvectorTreatsHyphenBeforeDigitsAsSign() throws Exception {
        assertEquals("'-01':5 '-15':6 '-3':2 '2024':4 'row':1",
                q("SELECT to_tsvector('english', 'row-3 and 2024-01-15')::text"));
    }

    /** Stop words consume a position but contribute no lexeme. */
    @Test
    void stopWordsStillTakePositions() throws Exception {
        assertEquals("'cat':2 'mat':5", q("SELECT to_tsvector('english', 'the cat on the mat')::text"));
    }

    // ------------------------------------------------------------------
    // Ranking
    // ------------------------------------------------------------------

    @Test
    void tsRankMatchesPg() throws Exception {
        assertEquals("0.06079271", q("SELECT ts_rank(to_tsvector('english','the quick brown fox "
                + "jumps over the lazy dog'), to_tsquery('english','fox'))::text"));
    }

    /** Cover density: one cover of one lexeme, weight D, scores 1/(1/0.1) = 0.1. */
    @Test
    void tsRankCdMatchesPg() throws Exception {
        assertEquals("0.1", q("SELECT ts_rank_cd(to_tsvector('english','a b c d e f g'), "
                + "to_tsquery('english','a & g'))::text"));
        assertEquals("0.033333335", q("SELECT ts_rank_cd(to_tsvector('english','alpha beta gamma delta'), "
                + "to_tsquery('english','alpha & delta'))::text"));
    }

    /** Normalisation bit 4 divides by the mean harmonic distance between covers. */
    @Test
    void tsRankCdNormalisationBitsMatchPg() throws Exception {
        String doc = "alpha beta gamma delta alpha epsilon delta";
        assertEquals("0.18333334", q("SELECT ts_rank_cd(to_tsvector('english','" + doc + "'), "
                + "to_tsquery('english','alpha & delta'), 0)::text"));
        assertEquals("0.026190476", q("SELECT ts_rank_cd(to_tsvector('english','" + doc + "'), "
                + "to_tsquery('english','alpha & delta'), 2)::text"));
        assertEquals("0.0712963", q("SELECT ts_rank_cd(to_tsvector('english','" + doc + "'), "
                + "to_tsquery('english','alpha & delta'), 4)::text"));
    }

    /** ts_rank's bit 4 is documented as not applicable and must be a no-op. */
    @Test
    void tsRankIgnoresCoverDistanceBit() throws Exception {
        String base = q("SELECT ts_rank(to_tsvector('english','alpha beta gamma'), "
                + "to_tsquery('english','alpha & gamma'), 0)::text");
        assertEquals(base, q("SELECT ts_rank(to_tsvector('english','alpha beta gamma'), "
                + "to_tsquery('english','alpha & gamma'), 4)::text"));
    }

    /** ts_rank/ts_rank_cd return real (OID 700), not text. */
    @Test
    void rankFunctionsAdvertiseFloat4() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT ts_rank(to_tsvector('english','a fox'), "
                     + "to_tsquery('english','fox')) AS r")) {
            assertEquals(Types.REAL, rs.getMetaData().getColumnType(1));
            assertEquals("float4", rs.getMetaData().getColumnTypeName(1));
            assertTrue(rs.next());
            assertEquals(0.06079271f, rs.getFloat(1), 0.0f);
        }
    }

    // ------------------------------------------------------------------
    // float4out / float8out
    // ------------------------------------------------------------------

    @Test
    void floatOutputMatchesPg() {
        assertEquals("0.1", PgFloatFormat.float8out(0.1));
        assertEquals("3", PgFloatFormat.float8out(3.0));
        assertEquals("100000000000000", PgFloatFormat.float8out(1e14));
        assertEquals("1e+15", PgFloatFormat.float8out(1e15));
        assertEquals("1e+20", PgFloatFormat.float8out(1e20));
        assertEquals("0.0001", PgFloatFormat.float8out(1e-4));
        assertEquals("1e-05", PgFloatFormat.float8out(1e-5));
        assertEquals("1e-300", PgFloatFormat.float8out(1e-300));
        assertEquals("5e-324", PgFloatFormat.float8out(Double.MIN_VALUE));
        assertEquals("1.7976931348623157e+308", PgFloatFormat.float8out(Double.MAX_VALUE));
        assertEquals("0.3333333333333333", PgFloatFormat.float8out(1.0 / 3));
        assertEquals("NaN", PgFloatFormat.float8out(Double.NaN));
        assertEquals("Infinity", PgFloatFormat.float8out(Double.POSITIVE_INFINITY));
        assertEquals("-Infinity", PgFloatFormat.float8out(Double.NEGATIVE_INFINITY));
        assertEquals("-0", PgFloatFormat.float8out(-0.0));
    }

    /** real switches to scientific notation six decimal places earlier than double. */
    @Test
    void float4OutputMatchesPg() {
        assertEquals("0.1", PgFloatFormat.float4out(0.1f));
        assertEquals("100000", PgFloatFormat.float4out(1e5f));
        assertEquals("1e+06", PgFloatFormat.float4out(1e6f));
        assertEquals("123456.7", PgFloatFormat.float4out(123456.7f));
        assertEquals("1.234567e+06", PgFloatFormat.float4out(1234567f));
        assertEquals("1e-45", PgFloatFormat.float4out(Float.MIN_VALUE));
        assertEquals("3.4028235e+38", PgFloatFormat.float4out(Float.MAX_VALUE));
        assertEquals("0.33333334", PgFloatFormat.float4out(1f / 3));
        assertEquals("1e-05", PgFloatFormat.float4out(1e-5f));
    }

    /** A decimal on a rounding midpoint reads back unchanged but PG never emits it. */
    @Test
    void floatOutputRejectsMidpointDecimals() {
        assertEquals("2.3277606065118072e+16", PgFloatFormat.float8out(2.327760606511807E16));
        assertEquals("4.2407352e+07", PgFloatFormat.float4out(4.240735306509258E7f));
    }

    /**
     * extra_float_digits above zero selects shortest round-trip output; zero or below
     * selects a fixed FLT_DIG/DBL_DIG precision, which is how a session that sets it to 0
     * sees 0.0607927 where the default shows 0.06079271.
     */
    @Test
    void extraFloatDigitsMatchesPg() {
        assertEquals("0.33333334", PgFloatFormat.float4out(1f / 3, 1));
        assertEquals("0.333333", PgFloatFormat.float4out(1f / 3, 0));
        assertEquals("0.3", PgFloatFormat.float4out(1f / 3, -5));
        assertEquals("0.3333333333333333", PgFloatFormat.float8out(1.0 / 3, 1));
        assertEquals("0.333333333333333", PgFloatFormat.float8out(1.0 / 3, 0));
        assertEquals("0.3333333333", PgFloatFormat.float8out(1.0 / 3, -5));
        // %g switches to exponent form once the exponent reaches the precision
        assertEquals("1000000000000", PgFloatFormat.float8out(1e12, 1));
        assertEquals("1e+12", PgFloatFormat.float8out(1e12, -5));
        assertEquals("123456.789", PgFloatFormat.float8out(123456.789, -5));
    }

    @Test
    void tsRankHonoursExtraFloatDigits() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SET extra_float_digits = 0");
        }
        try {
            assertEquals("0.0607927", q("SELECT ts_rank(to_tsvector('english','The quick brown fox'), "
                    + "to_tsquery('english','quick'))::text"));
            assertEquals("0.333333", q("SELECT ('0.33333334'::float4)::text"));
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("SET extra_float_digits = 1");
            }
        }
        assertEquals("0.06079271", q("SELECT ts_rank(to_tsvector('english','The quick brown fox'), "
                + "to_tsquery('english','quick'))::text"));
    }

    @Test
    void floatTextCastMatchesPg() throws Exception {
        assertEquals("1e+20", q("SELECT (1e20::float8)::text"));
        assertEquals("1e-05", q("SELECT (0.00001::float8)::text"));
        assertEquals("0.33333334", q("SELECT '0.33333334'::float4::text"));
        assertEquals("1e+06", q("SELECT 1000000::float4::text"));
    }
}
