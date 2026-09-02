package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A document and a query are made of the same lexemes, or they never match.
 *
 * <p>Both sides go through the configuration's own parser. The document side always did; the
 * query side cleaned its input with {@code replaceAll("[^a-zA-Z0-9\\s]", " ")} instead, which
 * deleted every letter outside ASCII and shredded every e-mail address, decimal and hyphenated
 * compound. The two then disagreed about what the words even were, and no amount of matching
 * could reconcile them.
 *
 * <p>A tsvector's text form is the value's own: the same lexeme written twice is one lexeme
 * holding both position lists, the positions are in order and each appears once, a quote inside
 * a lexeme is doubled so that the text reads back, and the lexemes are ordered as bytes. A
 * phrase is about where its lexemes sit, so an operand that is not a bare lexeme is answered by
 * position too, and a stop word between two lexemes leaves them further apart rather than side
 * by side.
 */
class TextSearchValueTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) out.add(rs.getString(1));
            return out;
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The same lexeme written twice is one lexeme holding both lists. */
    @Test
    void aRepeatedLexemeKeepsBothPositionLists() throws SQLException {
        assertEquals("'a':1,2", one("SELECT 'a:1 a:2'::tsvector::text"));
        assertEquals("'a' 'b'", one("SELECT 'a b a'::tsvector::text"));
        assertEquals("'a':1", one("SELECT 'a:1 a'::tsvector::text"));
    }

    /** The positions are in order, each once, and the stronger weight is the one kept. */
    @Test
    void positionsAreOrderedAndEachAppearsOnce() throws SQLException {
        assertEquals("'a':1,2,3", one("SELECT 'a:3,1,2'::tsvector::text"));
        assertEquals("'a':1", one("SELECT 'a:1,1'::tsvector::text"));
        assertEquals("'a':1A", one("SELECT 'a:1A,1'::tsvector::text"));
        assertEquals("'a':1A", one("SELECT 'a:1,1A'::tsvector::text"));
    }

    /** A quote inside a lexeme is doubled, so the text form reads back as the value. */
    @Test
    void theTextFormReadsBackAsTheValue() throws SQLException {
        assertEquals("'it''s'", one("SELECT '''it''''s'''::tsvector::text"));
        assertEquals("'it''s'", one("SELECT ('''it''''s'''::tsvector::text)::tsvector::text"));
    }

    /** A backslash carries the next character into an unquoted lexeme. */
    @Test
    void anUnquotedLexemeMayBeEscaped() throws SQLException {
        assertEquals("'a b'", one("SELECT 'a\\ b'::tsvector::text"));
        assertEquals("'ab'", one("SELECT E'a\\\\b'::tsvector::text"));
    }

    /** A colon separates a lexeme from its positions only once there is a lexeme before it. */
    @Test
    void aLeadingColonIsPartOfTheLexeme() throws SQLException {
        assertEquals("':1'", one("SELECT ':1'::tsvector::text"));
    }

    /** A position beyond the last representable one is brought back to it; zero is no position. */
    @Test
    void aPositionIsCheckedAndClamped() throws SQLException {
        assertEquals("'a':16383", one("SELECT 'a:16384'::tsvector::text"));
        assertEquals("42601", stateOf("SELECT 'a:0'::tsvector"));
        assertEquals("42601", stateOf("SELECT 'a:'::tsvector"));
        assertEquals("42601", stateOf("SELECT 'a:1AB'::tsvector"));
        assertEquals("42601", stateOf("SELECT ''''''::tsvector"));
        // Concatenation can push a position past the end, and the value must still read back.
        assertEquals("'a':16383", one("SELECT ('a:16383'::tsvector || 'a:1'::tsvector)::text"));
    }

    /** A weight belongs to a position, so a lexeme with none is left as it is. */
    @Test
    void setweightInventsNoPosition() throws SQLException {
        assertEquals("'a' 'b'", one("SELECT setweight('a b'::tsvector, 'A')::text"));
        assertEquals("'a'", one("SELECT setweight(strip('a:1'::tsvector), 'A')::text"));
        // A weight outside the four is reported the way PostgreSQL reports it. setweight reads
        // the weight as a byte and reports it as an internal condition; ts_filter reads a list of
        // them and reports a bad parameter with the offending one quoted.
        assertEquals("XX000", stateOf("SELECT setweight('a:1'::tsvector, '')"));
        assertEquals("XX000", stateOf("SELECT setweight('a:1'::tsvector, 'X')"));
        assertEquals("22023", stateOf("SELECT ts_filter('a:1A'::tsvector, '{X}')"));
    }

    /** Every element of a lexeme array has to be a lexeme. */
    @Test
    void aLexemeArrayHoldsLexemes() {
        assertEquals("2200F", stateOf("SELECT array_to_tsvector(ARRAY['']::text[])"));
        assertEquals("22004", stateOf("SELECT array_to_tsvector(ARRAY[NULL]::text[])"));
    }

    /** Lexemes are ordered as bytes, which is not the order Java puts them in. */
    @Test
    void lexemesAreOrderedAsBytes() throws SQLException {
        assertEquals("'é' 'Ａ' '\uD83D\uDE00'",
                one("SELECT array_to_tsvector(ARRAY[chr(65313), chr(128512), chr(233)])::text"));
        assertEquals("false", one("SELECT (array_to_tsvector(ARRAY[chr(128512)])"
                + " < array_to_tsvector(ARRAY[chr(57344)]))::text"));
        assertEquals("-1", one("SELECT tsvector_cmp('a'::tsvector,'b'::tsvector)::text"));
    }

    /**
     * A {@code <} always opens a phrase operator, so one that does not spell a whole operator
     * ends the read. Falling through left the character for a loop that excludes it, and nothing
     * was consumed -- the query never finished being read.
     */
    @Test
    void anUnfinishedPhraseOperatorIsASyntaxError() {
        assertEquals("42601", stateOf("SELECT 'a<b'::tsquery"));
        assertEquals("42601", stateOf("SELECT 'a <2 b'::tsquery"));
        assertEquals("42601", stateOf("SELECT 'a<'::tsquery"));
        assertEquals("42601", stateOf("SELECT 'a <> b'::tsquery"));
        assertEquals("42601", stateOf("SELECT 'a <a> b'::tsquery"));
        assertEquals("22023", stateOf("SELECT 'a <16385> b'::tsquery"));
    }

    /** A quoted lexeme may hold whatever it likes, including a bracket. */
    @Test
    void aQuotedLexemeMayHoldAnything() throws SQLException {
        assertEquals("'a<b'", one("SELECT '''a<b'''::tsquery::text"));
    }

    /** Only the four weights exist, and the prefix marker is written before them. */
    @Test
    void aQueryNamesOnlyWeightsThatExist() throws SQLException {
        assertEquals("42601", stateOf("SELECT 'a:E'::tsquery"));
        assertEquals("'a':*A", one("SELECT 'a:A*'::tsquery::text"));
    }

    /** A phrase groups to the left, so a phrase on the right is written as one. */
    @Test
    void aPhraseKeepsItsGrouping() throws SQLException {
        assertEquals("'a' <-> ( 'b' <-> 'c' )", one("SELECT 'a <-> (b <-> c)'::tsquery::text"));
        assertEquals("'a' <-> ( 'b' <-> 'c' )",
                one("SELECT ('a <-> (b <-> c)'::tsquery::text)::tsquery::text"));
        assertEquals("false",
                one("SELECT ('a <-> (b <-> c)'::tsquery = 'a <-> b <-> c'::tsquery)::text"));
        assertEquals("'a' <2> ( 'b' <-> 'c' )", one("SELECT 'a <2> (b <-> c)'::tsquery::text"));
    }

    /**
     * A phrase constrains where its operands are, so each operand answers with the places it
     * was found. Falling back to AND made a phrase true of a document that merely held the
     * words somewhere.
     */
    @Test
    void aPhraseOperandIsAnsweredByPosition() throws SQLException {
        assertEquals("false", one("SELECT (to_tsvector('simple','a c b')"
                + " @@ to_tsquery('simple','a <-> (b & c)'))::text"));
        assertEquals("false", one("SELECT (to_tsvector('simple','a b c')"
                + " @@ to_tsquery('simple','a <-> (b & c)'))::text"));
    }

    /** A lexeme with no positions carries no weights for a restriction to fail against. */
    @Test
    void aPositionlessLexemeAnswersForEveryWeight() throws SQLException {
        assertEquals("true", one("SELECT ('cat dog'::tsvector @@ 'cat:A'::tsquery)::text"));
        assertEquals("true", one("SELECT (strip('a:1A'::tsvector) @@ 'a:A'::tsquery)::text"));
        assertEquals("false", one("SELECT ('a:1B'::tsvector @@ 'a:A'::tsquery)::text"));
    }

    /** One query holds another when it names every lexeme the other does. */
    @Test
    void oneQueryHoldsAnother() throws SQLException {
        assertEquals("true", one("SELECT ('a'::tsquery @> 'a'::tsquery)::text"));
        assertEquals("true", one("SELECT ('a & b'::tsquery @> 'a'::tsquery)::text"));
        assertEquals("true", one("SELECT ('a'::tsquery <@ 'a & b'::tsquery)::text"));
        assertEquals("false", one("SELECT ('a'::tsquery @> 'a & b'::tsquery)::text"));
    }

    /** A stop word takes its place, so the lexemes around it are that much further apart. */
    @Test
    void aStopWordWidensThePhrase() throws SQLException {
        assertEquals("'cat' <2> 'dog'",
                one("SELECT to_tsquery('english', 'the <-> cat <-> the <-> dog')::text"));
        assertEquals("'cat' <4> 'dog'",
                one("SELECT to_tsquery('english', 'cat <2> the <2> dog')::text"));
    }

    /** The ranking is over the lexemes the query names, whether or not a NOT stands over them. */
    @Test
    void rankingCountsTheLexemesTheQueryNames() throws SQLException {
        assertEquals("1e-20", one("SELECT ts_rank('a:1'::tsvector, 'a & !b'::tsquery)::text"));
        assertEquals("0.09910322",
                one("SELECT ts_rank('a:1 b:2'::tsvector, 'a & !b'::tsquery)::text"));
        assertEquals("0.06079271", one("SELECT ts_rank(to_tsvector('simple','a b c d e'),"
                + " to_tsquery('simple','!a'))::text"));
        // A lexeme with no positions is scored as the one place it could be.
        assertEquals("1e-16",
                one("SELECT ts_rank(strip('a:1 b:2'::tsvector), 'a & b'::tsquery)::text"));
    }

    /** A cover has to satisfy the phrase, not merely hold its words. */
    @Test
    void coverDensityAnswersThePhrase() throws SQLException {
        assertEquals("0.2", one("SELECT ts_rank_cd('a:1,10,20 b:2,11'::tsvector,"
                + " 'a <-> b'::tsquery, 0)::text"));
        assertEquals("0", one("SELECT ts_rank_cd('a:1,10,20 b:2,11'::tsvector,"
                + " 'a <2> b'::tsquery, 0)::text"));
    }

    /** The names given to ts_delete are lexemes already, and name themselves and nothing else. */
    @Test
    void deletingALexemeTakesThatLexeme() throws SQLException {
        assertEquals("'cat':1 'dog':3",
                one("SELECT ts_delete(to_tsvector('english','cats and dogs'), 'cats')::text"));
    }

    /** The query side reads what the document side reads. */
    @Test
    void bothSidesReadTheSameWords() throws SQLException {
        assertEquals("'café'", one("SELECT plainto_tsquery('english', 'café')::text"));
        assertEquals("'café':1", one("SELECT to_tsvector('english','café')::text"));
        assertEquals("'foo@bar.com'",
                one("SELECT plainto_tsquery('english', 'foo@bar.com')::text"));
        assertEquals("'a.b.c'", one("SELECT plainto_tsquery('english', 'a.b.c')::text"));
        assertEquals("'well-known' <-> 'well' <-> 'known' <-> 'thing'",
                one("SELECT phraseto_tsquery('english', 'well-known thing')::text"));
    }

    /** The web-search syntax is four things, and everything else belongs to the text. */
    @Test
    void theWebSearchSyntaxIsFourThings() throws SQLException {
        assertEquals("'foo@bar.com'",
                one("SELECT websearch_to_tsquery('english', 'foo@bar.com')::text"));
        assertEquals("'b' | 'c' & 'd'",
                one("SELECT websearch_to_tsquery('english', 'a b OR c d')::text"));
        assertEquals("'x' | 'y'", one("SELECT websearch_to_tsquery('english', 'x OR OR y')::text"));
        assertEquals("!'cat'", one("SELECT websearch_to_tsquery('english', '- cat')::text"));
        assertEquals("!!'cat'", one("SELECT websearch_to_tsquery('english', '--cat')::text"));
        assertEquals("'cat' | 'dog'",
                one("SELECT websearch_to_tsquery('english', 'cat or dog')::text"));
    }

    /** The dictionary named is the one that does the work, and a name that names none is an error. */
    @Test
    void theDictionaryNamedIsTheOneThatWorks() throws SQLException {
        assertEquals("{cats}", one("SELECT ts_lexize('simple', 'Cats')::text"));
        assertEquals("{}", one("SELECT ts_lexize('english_stem', 'the')::text"));
        assertEquals("{cat}", one("SELECT ts_lexize('english_stem', 'cats')::text"));
        assertEquals("42704", stateOf("SELECT ts_lexize('no_such_dict', 'x')"));
    }

    /** A configuration or parser that names none is an error rather than a fall back. */
    @Test
    void aConfigurationThatNamesNoneIsRefused() {
        assertEquals("42704", stateOf("SELECT plainto_tsquery('nonexistent_cfg', 'hello')"));
        assertEquals("42704", stateOf("SELECT ts_headline('nonexistent_cfg','hello','x'::tsquery)"));
        assertEquals("42704", stateOf("SELECT count(*) FROM ts_parse('no_such_parser', 'a b')"));
        assertEquals("22023", stateOf("SELECT count(*) FROM ts_stat('SELECT 1')"));
    }

    /**
     * A headline is the document's own characters with the matches marked: the spacing, the
     * newlines and the punctuation come back as they stand.
     */
    @Test
    void aHeadlineKeepsTheDocumentsOwnCharacters() throws SQLException {
        assertEquals("<b>Cats</b> and Dogs", one(
                "SELECT ts_headline('simple', 'Cats and Dogs', to_tsquery('simple','cats'))"));
        assertEquals("<b>supernovae</b> stars", one(
                "SELECT ts_headline('english','supernovae stars',to_tsquery('english','sup:*'))"));
        assertEquals("The cat. The <b>dog</b>. The bird.", one(
                "SELECT ts_headline('english', 'The cat. The dog. The bird.',"
                        + " to_tsquery('english','dog'))"));
        assertEquals("32", one("SELECT length(ts_headline('english',"
                + " 'Multi  spaced    text fox', to_tsquery('english','fox')))::text"));
        assertEquals("line1 <b>fox</b><NL>line2", one("SELECT replace(ts_headline('english',"
                + " E'line1 fox\\nline2', to_tsquery('english','fox')), E'\\n', '<NL>')"));
        assertEquals("<b>six</b> seven", one("SELECT ts_headline('english',"
                + " 'one two three four five six seven eight nine ten eleven twelve',"
                + " to_tsquery('english','six'),'MaxWords=5, MinWords=2')"));
        // A markup tag stands for a break between words rather than for itself.
        assertEquals("the big <b>cat</b> sat", one(
                "SELECT ts_headline('english','the big<br>cat sat', to_tsquery('cat'))"));
    }

    /** A text search value is of a text search type, whatever characters it prints as. */
    @Test
    void aTextSearchValueIsOfItsOwnType() throws SQLException {
        assertEquals("tsvector", one("SELECT pg_typeof(to_tsvector('english','a'))::text"));
        assertEquals("tsquery", one("SELECT pg_typeof(to_tsquery('english','a'))::text"));
        assertEquals("tsvector", one("SELECT pg_typeof(strip('a'::tsvector))::text"));
        assertEquals("tsquery", one("SELECT pg_typeof(plainto_tsquery('english','a'))::text"));
        assertEquals("tsvector", one("SELECT pg_typeof(setweight('a:1'::tsvector,'A'))::text"));
        assertEquals("regconfig", one("SELECT pg_typeof(get_current_ts_config())::text"));
    }

    /** The function spellings of the operators are functions PostgreSQL declares. */
    @Test
    void theOperatorsHaveTheirFunctionSpellings() throws SQLException {
        assertEquals("true", one("SELECT ('cat'::tsvector @@@ 'cat'::tsquery)::text"));
        assertEquals("true", one("SELECT ts_match_vq('a'::tsvector, 'a'::tsquery)::text"));
        assertEquals("!'a'", one("SELECT tsquery_not('a'::tsquery)::text"));
        // Written in front of a value, @@@ is not an operator at all.
        assertEquals("42883", stateOf("SELECT @@@ 1"));
    }

    /** A lexeme is folded one character at a time, as PostgreSQL's own lowerstr does. */
    @Test
    void aLexemeIsFoldedOneCharacterAtATime() throws SQLException {
        assertEquals("'i':1", one("SELECT to_tsvector('simple', U&'\\0130')::text"));
        assertEquals("{i}", one("SELECT ts_lexize('simple', U&'\\0130')::text"));
    }

    /** The part of a query an index can be searched with, written the way a query is written. */
    @Test
    void aQueryTreeIsPrintedAsAQuery() throws SQLException {
        assertEquals("'a' & 'b'", one("SELECT querytree('a & b'::tsquery)"));
        assertEquals("'a' <-> 'b'", one("SELECT querytree('a <-> b'::tsquery)"));
        assertEquals("'a' & ( 'b' | 'c' )", one("SELECT querytree('a & (b | c)'::tsquery)"));
        assertEquals("T", one("SELECT querytree('!a'::tsquery)"));
        assertEquals("3", one("SELECT numnode('a & b'::tsquery)::text"));
    }
}
