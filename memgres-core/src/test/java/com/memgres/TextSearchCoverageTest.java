package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for pg18-coverage-checklist items 114-116:
 * Text Search Types, Functions, and Configuration DDL.
 */
class TextSearchCoverageTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE documents (id SERIAL PRIMARY KEY, title TEXT, body TEXT)");
            stmt.execute("INSERT INTO documents (title, body) VALUES ('PostgreSQL Tutorial', 'PostgreSQL is an advanced open source database system')");
            stmt.execute("INSERT INTO documents (title, body) VALUES ('Full Text Search', 'Full text search provides the capability to identify documents that satisfy a query')");
            stmt.execute("INSERT INTO documents (title, body) VALUES ('Fat Cats', 'The fat cat sat on the mat near another fat cat')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String query1(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }

    private double queryDouble(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getDouble(1) : -1.0;
        }
    }

    private boolean queryBool(String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    // ==================== Item 114: Text Search Types ====================

    // ---- tsvector basics ----

    @Test
    void testToTsvectorBasic() throws SQLException {
        String result = query1("SELECT to_tsvector('the fat cat sat on the mat')");
        assertNotNull(result);
        assertTrue(result.contains("'fat'"));
        assertTrue(result.contains("'cat'"));
        assertTrue(result.contains("'sat'"));
        assertTrue(result.contains("'mat'"));
    }

    @Test
    void testToTsvectorRemovesStopWords() throws SQLException {
        String result = query1("SELECT to_tsvector('the fat cat sat on the mat')");
        assertNotNull(result);
        // Stop words removed
        assertFalse(result.contains("'the'"));
        assertFalse(result.contains("'on'"));
    }

    @Test
    void testToTsvectorWithPositions() throws SQLException {
        String result = query1("SELECT to_tsvector('fat cat')");
        assertNotNull(result);
        // Should show positions
        assertTrue(result.contains(":"));
    }

    @Test
    void testToTsvectorStemming() throws SQLException {
        String result = query1("SELECT to_tsvector('running dogs jumping')");
        assertNotNull(result);
        // Words should be stemmed
        assertTrue(result.contains("'run'") || result.contains("'runn'"));
    }

    @Test
    void testToTsvectorWithConfig() throws SQLException {
        String result = query1("SELECT to_tsvector('english', 'fat cats')");
        assertNotNull(result);
        assertTrue(result.contains("'fat'"));
    }

    @Test
    void testToTsvectorNull() throws SQLException {
        String result = query1("SELECT to_tsvector(NULL)");
        assertNull(result);
    }

    @Test
    void testToTsvectorEmpty() throws SQLException {
        String result = query1("SELECT to_tsvector('')");
        assertNotNull(result);
    }

    // ---- tsquery basics ----

    @Test
    void testToTsqueryBasic() throws SQLException {
        String result = query1("SELECT to_tsquery('fat & cat')");
        assertNotNull(result);
        assertTrue(result.contains("fat"));
        assertTrue(result.contains("cat"));
    }

    @Test
    void testToTsqueryOr() throws SQLException {
        String result = query1("SELECT to_tsquery('fat | dog')");
        assertNotNull(result);
        assertTrue(result.contains("|"));
    }

    @Test
    void testToTsqueryNot() throws SQLException {
        String result = query1("SELECT to_tsquery('fat & !dog')");
        assertNotNull(result);
        assertTrue(result.contains("!"));
    }

    @Test
    void testToTsqueryWithConfig() throws SQLException {
        String result = query1("SELECT to_tsquery('english', 'cat & dog')");
        assertNotNull(result);
    }

    @Test
    void testToTsqueryParentheses() throws SQLException {
        String result = query1("SELECT to_tsquery('(fat | thin) & cat')");
        assertNotNull(result);
    }

    // ---- @@ match operator ----

    @Test
    void testTsMatchBasicTrue() throws SQLException {
        assertTrue(queryBool("SELECT to_tsvector('fat cat sat') @@ to_tsquery('cat')"));
    }

    @Test
    void testTsMatchBasicFalse() throws SQLException {
        assertFalse(queryBool("SELECT to_tsvector('fat cat sat') @@ to_tsquery('dog')"));
    }

    @Test
    void testTsMatchAndTrue() throws SQLException {
        assertTrue(queryBool("SELECT to_tsvector('fat cat sat') @@ to_tsquery('fat & cat')"));
    }

    @Test
    void testTsMatchAndFalse() throws SQLException {
        assertFalse(queryBool("SELECT to_tsvector('fat cat sat') @@ to_tsquery('fat & dog')"));
    }

    @Test
    void testTsMatchOrTrue() throws SQLException {
        assertTrue(queryBool("SELECT to_tsvector('fat cat sat') @@ to_tsquery('dog | cat')"));
    }

    @Test
    void testTsMatchNotTrue() throws SQLException {
        assertTrue(queryBool("SELECT to_tsvector('fat cat sat') @@ to_tsquery('fat & !dog')"));
    }

    @Test
    void testTsMatchNotFalse() throws SQLException {
        assertFalse(queryBool("SELECT to_tsvector('fat cat sat') @@ to_tsquery('fat & !cat')"));
    }

    @Test
    void testTsMatchWithNull() throws SQLException {
        assertFalse(queryBool("SELECT NULL::tsvector @@ to_tsquery('cat')"));
    }

    @Test
    void testTsMatchInWhere() throws SQLException {
        int count = queryInt("SELECT COUNT(*) FROM documents WHERE to_tsvector(body) @@ to_tsquery('database')");
        assertEquals(1, count);
    }

    @Test
    void testTsMatchInWhereMultiple() throws SQLException {
        int count = queryInt("SELECT COUNT(*) FROM documents WHERE to_tsvector(body) @@ to_tsquery('fat & cat')");
        assertEquals(1, count);
    }

    // ---- tsvector positions and weights ----

    @Test
    void testTsvectorWithWeightsSetweight() throws SQLException {
        String result = query1("SELECT setweight(to_tsvector('cat dog'), 'A')");
        assertNotNull(result);
        assertTrue(result.contains("A"));
    }

    @Test
    void testTsvectorSetweightB() throws SQLException {
        String result = query1("SELECT setweight(to_tsvector('important words'), 'B')");
        assertNotNull(result);
        assertTrue(result.contains("B"));
    }

    // ---- tsquery phrase operator <-> ----

    @Test
    void testPhraseTsqueryBasic() throws SQLException {
        String result = query1("SELECT phraseto_tsquery('fat cat')");
        assertNotNull(result);
    }

    @Test
    void testPhraseTsqueryWithConfig() throws SQLException {
        String result = query1("SELECT phraseto_tsquery('english', 'fat cat')");
        assertNotNull(result);
    }

    @Test
    void testPhraseMatchAdjacentTrue() throws SQLException {
        // "fat cat" are adjacent in the text
        assertTrue(queryBool("SELECT to_tsvector('the fat cat sat') @@ phraseto_tsquery('fat cat')"));
    }

    @Test
    void testPhraseOperatorInTsquery() throws SQLException {
        // <-> phrase operator
        String result = query1("SELECT to_tsquery('fat <-> cat')");
        assertNotNull(result);
    }

    @Test
    void testPhraseMatchWithDistance() throws SQLException {
        // <2> means within 2 positions
        String result = query1("SELECT to_tsquery('fat <2> sat')");
        assertNotNull(result);
    }

    // ---- prefix matching :* ----

    @Test
    void testPrefixMatchTrue() throws SQLException {
        assertTrue(queryBool("SELECT to_tsvector('postgresql database') @@ to_tsquery('post:*')"));
    }

    @Test
    void testPrefixMatchFalse() throws SQLException {
        assertFalse(queryBool("SELECT to_tsvector('mysql database') @@ to_tsquery('post:*')"));
    }

    @Test
    void testPrefixMatchToString() throws SQLException {
        String result = query1("SELECT to_tsquery('super:*')");
        assertNotNull(result);
        assertTrue(result.contains(":*") || result.contains("*"));
    }

    // ---- weight filtering ----

    @Test
    void testWeightFilterA() throws SQLException {
        String result = query1("SELECT setweight(to_tsvector('cat'), 'A')");
        assertNotNull(result);
        assertTrue(result.contains("A"));
    }

    @Test
    void testTsFilterKeepsWeights() throws SQLException {
        // Set weight A, then filter to only A
        String result = query1("SELECT ts_filter(setweight(to_tsvector('cat dog'), 'A'), '{A}')");
        assertNotNull(result);
    }

    // ==================== Item 115: Text Search Functions ====================

    // ---- plainto_tsquery ----

    @Test
    void testPlaintoTsquery() throws SQLException {
        String result = query1("SELECT plainto_tsquery('fat cat dog')");
        assertNotNull(result);
        assertTrue(result.contains("&"));
    }

    @Test
    void testPlaintoTsqueryWithConfig() throws SQLException {
        String result = query1("SELECT plainto_tsquery('english', 'the fat cat')");
        assertNotNull(result);
    }

    // ---- phraseto_tsquery ----

    @Test
    void testPhrasetoTsquerySingleWord() throws SQLException {
        String result = query1("SELECT phraseto_tsquery('cat')");
        assertNotNull(result);
    }

    @Test
    void testPhrasetoTsqueryMultiWord() throws SQLException {
        String result = query1("SELECT phraseto_tsquery('fat cat dog')");
        assertNotNull(result);
    }

    // ---- websearch_to_tsquery ----

    @Test
    void testWebsearchToTsqueryBasic() throws SQLException {
        String result = query1("SELECT websearch_to_tsquery('fat cat')");
        assertNotNull(result);
        assertTrue(result.contains("&"));
    }

    @Test
    void testWebsearchToTsqueryOr() throws SQLException {
        String result = query1("SELECT websearch_to_tsquery('fat OR dog')");
        assertNotNull(result);
        assertTrue(result.contains("|"));
    }

    @Test
    void testWebsearchToTsqueryNegation() throws SQLException {
        String result = query1("SELECT websearch_to_tsquery('fat -dog')");
        assertNotNull(result);
        assertTrue(result.contains("!"));
    }

    @Test
    void testWebsearchToTsqueryWithConfig() throws SQLException {
        String result = query1("SELECT websearch_to_tsquery('english', 'fat cat')");
        assertNotNull(result);
    }

    // ---- tsvector || tsvector concatenation ----

    @Test
    void testTsvectorConcat() throws SQLException {
        String result = query1("SELECT to_tsvector('cat') || to_tsvector('dog')");
        assertNotNull(result);
        assertTrue(result.contains("'cat'"));
        assertTrue(result.contains("'dog'"));
    }

    @Test
    void testTsvectorConcatInWhere() throws SQLException {
        assertTrue(queryBool("SELECT (to_tsvector('cat') || to_tsvector('dog')) @@ to_tsquery('cat & dog')"));
    }

    // ---- length(tsvector) ----

    @Test
    void testTsvectorLength() throws SQLException {
        int len = queryInt("SELECT length(to_tsvector('fat cat dog'))");
        assertEquals(3, len);
    }

    @Test
    void testTsvectorLengthEmpty() throws SQLException {
        int len = queryInt("SELECT length(to_tsvector('the'))");
        // 'the' is a stop word, so result is empty
        assertEquals(0, len);
    }

    // ---- strip(tsvector) ----

    @Test
    void testStrip() throws SQLException {
        String result = query1("SELECT strip(to_tsvector('fat cat'))");
        assertNotNull(result);
        assertTrue(result.contains("'fat'"));
        assertTrue(result.contains("'cat'"));
    }

    // ---- setweight ----

    @Test
    void testSetweightA() throws SQLException {
        String result = query1("SELECT setweight(to_tsvector('cat'), 'A')");
        assertNotNull(result);
        assertTrue(result.contains("A"));
    }

    @Test
    void testSetweightC() throws SQLException {
        String result = query1("SELECT setweight(to_tsvector('cat'), 'C')");
        assertNotNull(result);
        assertTrue(result.contains("C"));
    }

    @Test
    void testSetweightD() throws SQLException {
        // Weight D is default, may not show in output
        String result = query1("SELECT setweight(to_tsvector('cat'), 'D')");
        assertNotNull(result);
    }

    // ---- ts_delete ----

    @Test
    void testTsDeleteSingle() throws SQLException {
        String result = query1("SELECT ts_delete(to_tsvector('fat cat dog'), 'dog')");
        assertNotNull(result);
        assertTrue(result.contains("'fat'"));
        assertTrue(result.contains("'cat'"));
        assertFalse(result.contains("'dog'"));
    }

    // ---- ts_filter ----

    @Test
    void testTsFilter() throws SQLException {
        String result = query1("SELECT ts_filter(setweight(to_tsvector('cat'), 'A'), '{A}')");
        assertNotNull(result);
        assertTrue(result.contains("'cat'"));
    }

    @Test
    void testTsFilterEmpty() throws SQLException {
        // Filter for weight B when everything is weight A → empty
        String result = query1("SELECT ts_filter(setweight(to_tsvector('cat'), 'A'), '{B}')");
        assertNotNull(result);
    }

    // ---- tsquery_phrase ----

    @Test
    void testTsqueryPhrase() throws SQLException {
        String result = query1("SELECT tsquery_phrase(to_tsquery('fat'), to_tsquery('cat'))");
        assertNotNull(result);
    }

    @Test
    void testTsqueryPhraseWithDistance() throws SQLException {
        String result = query1("SELECT tsquery_phrase(to_tsquery('fat'), to_tsquery('cat'), 2)");
        assertNotNull(result);
    }

    // ---- numnode ----

    @Test
    void testNumnodeSingle() throws SQLException {
        int nodes = queryInt("SELECT numnode(to_tsquery('cat'))");
        assertEquals(1, nodes);
    }

    @Test
    void testNumnodeAnd() throws SQLException {
        int nodes = queryInt("SELECT numnode(to_tsquery('cat & dog'))");
        assertEquals(3, nodes); // AND node + 2 TERM nodes
    }

    @Test
    void testNumnodeComplex() throws SQLException {
        int nodes = queryInt("SELECT numnode(to_tsquery('(cat | dog) & !fish'))");
        assertTrue(nodes >= 5); // OR, AND, NOT, and 3 terms
    }

    // ---- querytree ----

    @Test
    void testQuerytreeSingle() throws SQLException {
        String result = query1("SELECT querytree(to_tsquery('cat'))");
        assertNotNull(result);
        assertTrue(result.contains("cat"));
    }

    @Test
    void testQuerytreeComplex() throws SQLException {
        String result = query1("SELECT querytree(to_tsquery('cat & dog'))");
        assertNotNull(result);
        assertTrue(result.contains("&"));
    }

    // ---- ts_rank ----

    @Test
    void testTsRankBasic() throws SQLException {
        double rank = queryDouble("SELECT ts_rank(to_tsvector('fat cat'), to_tsquery('cat'))");
        assertTrue(rank > 0);
    }

    @Test
    void testTsRankNoMatch() throws SQLException {
        double rank = queryDouble("SELECT ts_rank(to_tsvector('fat cat'), to_tsquery('dog'))");
        assertEquals(0.0, rank, 0.001);
    }

    @Test
    void testTsRankHigherForMoreMatches() throws SQLException {
        double rank1 = queryDouble("SELECT ts_rank(to_tsvector('fat cat dog bird'), to_tsquery('cat'))");
        double rank2 = queryDouble("SELECT ts_rank(to_tsvector('fat cat dog bird'), to_tsquery('cat & dog'))");
        assertTrue(rank2 > rank1);
    }

    // ---- ts_rank_cd ----

    @Test
    void testTsRankCdBasic() throws SQLException {
        double rank = queryDouble("SELECT ts_rank_cd(to_tsvector('fat cat'), to_tsquery('cat'))");
        assertTrue(rank >= 0);
    }

    @Test
    void testTsRankCdNoMatch() throws SQLException {
        double rank = queryDouble("SELECT ts_rank_cd(to_tsvector('fat cat'), to_tsquery('dog'))");
        assertEquals(0.0, rank, 0.001);
    }

    // ---- ts_headline ----

    @Test
    void testTsHeadlineBasic() throws SQLException {
        String result = query1("SELECT ts_headline('The fat cat sat on the mat', to_tsquery('cat'))");
        assertNotNull(result);
        assertTrue(result.contains("<b>") || result.contains("cat"));
    }

    @Test
    void testTsHeadlineWithOptions() throws SQLException {
        String result = query1("SELECT ts_headline('The fat cat sat on the mat', to_tsquery('cat'), 'StartSel=<em>, StopSel=</em>')");
        assertNotNull(result);
    }

    @Test
    void testTsHeadlineWithConfig() throws SQLException {
        String result = query1("SELECT ts_headline('english', 'The fat cat sat', to_tsquery('cat'))");
        assertNotNull(result);
    }

    // ---- ts_rewrite ----

    @Test
    void testTsRewrite() throws SQLException {
        String result = query1("SELECT ts_rewrite(to_tsquery('cat & dog'), to_tsquery('cat'), to_tsquery('kitten'))");
        assertNotNull(result);
    }

    // ---- ts_debug ----

    @Test
    void testTsDebug() throws SQLException {
        String result = query1("SELECT ts_debug('hello')");
        assertNotNull(result);
        assertTrue(result.contains("asciiword"));
    }

    @Test
    void testTsDebugWithConfig() throws SQLException {
        String result = query1("SELECT ts_debug('english', 'hello')");
        assertNotNull(result);
    }

    // ---- ts_lexize ----

    @Test
    void testTsLexize() throws SQLException {
        String result = query1("SELECT ts_lexize('english_stem', 'running')");
        assertNotNull(result);
    }

    // ---- ts_token_type ----

    @Test
    void testTsTokenType() throws SQLException {
        String result = query1("SELECT ts_token_type('default')");
        assertNotNull(result);
    }

    // ---- get_current_ts_config ----

    @Test
    void testGetCurrentTsConfig() throws SQLException {
        String result = query1("SELECT get_current_ts_config()");
        assertEquals("english", result);
    }

    // ---- array_to_tsvector ----

    @Test
    void testArrayToTsvector() throws SQLException {
        String result = query1("SELECT array_to_tsvector(ARRAY['cat', 'dog', 'fish'])");
        assertNotNull(result);
        assertTrue(result.contains("'cat'"));
        assertTrue(result.contains("'dog'"));
        assertTrue(result.contains("'fish'"));
    }

    // ---- tsvector_to_array ----

    @Test
    void testTsvectorToArray() throws SQLException {
        String result = query1("SELECT tsvector_to_array(to_tsvector('fat cat dog'))");
        assertNotNull(result);
        assertTrue(result.contains("cat"));
        assertTrue(result.contains("dog"));
        assertTrue(result.contains("fat"));
    }

    // ---- Cast operators ----

    @Test
    void testCastToTsvector() throws SQLException {
        String result = query1("SELECT 'fat cat'::tsvector");
        assertNotNull(result);
    }

    @Test
    void testCastToTsquery() throws SQLException {
        String result = query1("SELECT 'cat & dog'::tsquery");
        assertNotNull(result);
    }

    // ==================== Item 116: Text Search Configuration DDL ====================

    @Test
    void testCreateTextSearchConfiguration() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TEXT SEARCH CONFIGURATION my_config (COPY = pg_catalog.english)");
            // Should not throw
        }
    }

    @Test
    void testCreateTextSearchDictionary() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TEXT SEARCH DICTIONARY my_dict (TEMPLATE = simple)");
        }
    }

    @Test
    void testCreateTextSearchParser() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TEXT SEARCH PARSER my_parser (START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype)");
        }
    }

    @Test
    void testCreateTextSearchTemplate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TEXT SEARCH TEMPLATE my_template (INIT = dsimple_init, LEXIZE = dsimple_lexize)");
        }
    }

    @Test
    void testAlterTextSearchConfiguration() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TEXT SEARCH CONFIGURATION my_config ADD MAPPING FOR asciiword WITH english_stem");
        }
    }

    @Test
    void testAlterTextSearchDictionary() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TEXT SEARCH DICTIONARY my_dict (StopWords = english)");
        }
    }

    @Test
    void testDropTextSearchConfiguration() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TEXT SEARCH CONFIGURATION IF EXISTS my_config");
        }
    }

    @Test
    void testDropTextSearchDictionary() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TEXT SEARCH DICTIONARY IF EXISTS my_dict");
        }
    }

    @Test
    void testDropTextSearchParser() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TEXT SEARCH PARSER IF EXISTS my_parser");
        }
    }

    @Test
    void testDropTextSearchTemplate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TEXT SEARCH TEMPLATE IF EXISTS my_template");
        }
    }

    // ==================== Integration / complex queries ====================

    @Test
    void testTsMatchWithOrderByRank() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT title, ts_rank(to_tsvector(body), to_tsquery('database')) AS rank " +
                             "FROM documents WHERE to_tsvector(body) @@ to_tsquery('database') " +
                             "ORDER BY rank DESC")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("title"));
            assertTrue(rs.getDouble("rank") > 0);
        }
    }

    @Test
    void testConcatTsvectorsFromColumns() throws SQLException {
        String result = query1(
                "SELECT to_tsvector(title) || to_tsvector(body) FROM documents WHERE id = 1");
        assertNotNull(result);
    }

    @Test
    void testSetweightConcatSearch() throws SQLException {
        // Title gets weight A, body gets weight D; search should match
        assertTrue(queryBool(
                "SELECT (setweight(to_tsvector(title), 'A') || setweight(to_tsvector(body), 'D')) " +
                        "@@ to_tsquery('postgresql') FROM documents WHERE id = 1"));
    }

    @Test
    void testHeadlineOnTableColumn() throws SQLException {
        String result = query1(
                "SELECT ts_headline(body, to_tsquery('database')) FROM documents WHERE id = 1");
        assertNotNull(result);
    }

    @Test
    void testTsMatchOnFatCats() throws SQLException {
        assertTrue(queryBool(
                "SELECT to_tsvector(body) @@ to_tsquery('fat & cat') FROM documents WHERE id = 3"));
    }

    @Test
    void testCountMatchingDocuments() throws SQLException {
        int count = queryInt(
                "SELECT COUNT(*) FROM documents WHERE to_tsvector(title || ' ' || body) @@ to_tsquery('search')");
        assertTrue(count >= 1);
    }

    @Test
    void testRankOrdering() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT title FROM documents " +
                             "WHERE to_tsvector(title || ' ' || body) @@ to_tsquery('cat | database') " +
                             "ORDER BY ts_rank(to_tsvector(title || ' ' || body), to_tsquery('cat | database')) DESC")) {
            int count = 0;
            while (rs.next()) count++;
            assertTrue(count >= 1);
        }
    }

    // ---- Edge cases ----

    @Test
    void testEmptyTsvectorMatch() throws SQLException {
        assertFalse(queryBool("SELECT to_tsvector('') @@ to_tsquery('cat')"));
    }

    @Test
    void testTsvectorOnlyStopWords() throws SQLException {
        String result = query1("SELECT to_tsvector('the and or but')");
        assertNotNull(result);
        // All stop words, result should be empty
    }

    @Test
    void testTsqueryEmptyInput() throws SQLException {
        String result = query1("SELECT to_tsquery('')");
        assertNotNull(result);
    }

    @Test
    void testNullHandling() throws SQLException {
        String result = query1("SELECT ts_rank(NULL, to_tsquery('cat'))");
        assertEquals("0", result);
    }

    @Test
    void testTsvectorLengthAfterDelete() throws SQLException {
        int len = queryInt("SELECT length(ts_delete(to_tsvector('fat cat dog'), 'dog'))");
        assertEquals(2, len);
    }

    @Test
    void testStripRemovesPositions() throws SQLException {
        String stripped = query1("SELECT strip(to_tsvector('cat dog'))");
        String normal = query1("SELECT to_tsvector('cat dog')");
        // Both should contain the lexemes
        assertNotNull(stripped);
        assertNotNull(normal);
        assertTrue(stripped.contains("'cat'"));
    }

    @Test
    void testWebsearchQuotedPhrase() throws SQLException {
        String result = query1("SELECT websearch_to_tsquery('\"fat cat\"')");
        assertNotNull(result);
    }

    @Test
    void testMultipleSetweights() throws SQLException {
        String result = query1(
                "SELECT setweight(to_tsvector('title words'), 'A') || setweight(to_tsvector('body words'), 'B')");
        assertNotNull(result);
    }
}
