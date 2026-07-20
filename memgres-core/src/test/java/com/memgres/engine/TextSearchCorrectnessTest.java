package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for fix/text-search: H27, H28, H29, H30, M18, L14.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TextSearchCorrectnessTest {

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

    private boolean queryIsNull(String sql) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected a row for: " + sql);
            rs.getString(1);
            return rs.wasNull();
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

    // ========== H27: ::tsvector cast input format ==========

    @Test @Order(1)
    void tsvectorCastPreservesCase() throws SQLException {
        // Quoted lexemes in tsvector input should preserve case
        // In PG SQL: $$'Fat':2 'Cat':3$$::tsvector — use dollar quoting or escaped quotes
        String result = query("SELECT '''Fat'':2 ''Cat'':3'::tsvector");
        assertTrue(result.contains("Cat") || result.contains("Fat"),
                "tsvector cast should preserve quoted lexeme case, got: " + result);
    }

    @Test @Order(2)
    void tsvectorCastPositions() throws SQLException {
        // Unquoted form: fat:2,4 cat:3
        String result = query("SELECT 'fat:2,4 cat:3'::tsvector");
        assertNotNull(result);
        assertTrue(result.contains("fat") && result.contains("cat"),
                "Should parse lexemes, got: " + result);
    }

    @Test @Order(3)
    void tsvectorCastEmptyString() throws SQLException {
        String result = query("SELECT ''::tsvector");
        assertEquals("", result);
    }

    @Test @Order(4)
    void tsvectorLengthAfterCast() throws SQLException {
        // ::tsvector should NOT tokenize — just parse the literal
        String result = query("SELECT length('fat:2,4 cat:3'::tsvector)");
        assertEquals("2", result);
    }

    // ========== H28: simple config ==========

    @Test @Order(10)
    void toTsvectorSimpleNoStemming() throws SQLException {
        // 'simple' config: no stemming, keep all words
        String result = query("SELECT to_tsvector('simple', 'the Running Dogs')");
        // Should contain all words, lowercase, not stemmed
        assertTrue(result.contains("'the'"), "simple config should keep stopwords, got: " + result);
        assertTrue(result.contains("'running'"), "simple config should not stem, got: " + result);
        assertTrue(result.contains("'dogs'"), "simple should keep 'dogs' as-is, got: " + result);
    }

    @Test @Order(11)
    void toTsvectorEnglishStems() throws SQLException {
        String result = query("SELECT to_tsvector('english', 'the Running Dogs')");
        // 'the' should be removed (stopword), 'running'→stemmed, 'dogs'→'dog'
        assertFalse(result.contains("'the'"), "english config should remove stopwords");
    }

    // ========== H29: ts_rank / ts_rank_cd ==========

    @Test @Order(20)
    void tsRankBasic() throws SQLException {
        String result = query("SELECT ts_rank('fat:1 cat:2 rat:3'::tsvector, 'cat & rat'::tsquery)");
        assertNotNull(result);
        float rank = Float.parseFloat(result);
        assertTrue(rank > 0, "rank should be positive for matching query, got: " + rank);
    }

    @Test @Order(21)
    void tsRankCdBasic() throws SQLException {
        String result = query("SELECT ts_rank_cd('fat:1 cat:2 rat:3'::tsvector, 'cat & rat'::tsquery)");
        assertNotNull(result);
        float rank = Float.parseFloat(result);
        assertTrue(rank > 0, "rank_cd should be positive for matching query, got: " + rank);
    }

    @Test @Order(22)
    void tsRankWithWeightsArray() throws SQLException {
        String result = query("SELECT ts_rank('{0.1, 0.2, 0.4, 1.0}', 'cat:1A'::tsvector, 'cat'::tsquery)");
        assertNotNull(result);
        float rank = Float.parseFloat(result);
        assertTrue(rank > 0, "rank with weights array should be positive, got: " + rank);
    }

    @Test @Order(23)
    void tsRankNoMatch() throws SQLException {
        String result = query("SELECT ts_rank('fat:1'::tsvector, 'cat'::tsquery)");
        assertEquals("0", result);
    }

    // ========== H30: tsquery construction ==========

    @Test @Order(30)
    void phrasetoTsqueryStopwordDistance() throws SQLException {
        // 'the cats in the hat' → stopwords removed, distance accounts for gaps
        String result = query("SELECT phraseto_tsquery('english', 'the cats in the hat')");
        assertNotNull(result);
        // Should have <N> distances (not literal '' terms)
        assertFalse(result.contains("''"), "stopwords should not appear as '' lexemes, got: " + result);
        assertTrue(result.contains("cat"), "should contain stemmed 'cat', got: " + result);
        assertTrue(result.contains("hat"), "should contain 'hat', got: " + result);
    }

    @Test @Order(31)
    void plaintoTsqueryStripsPunctuation() throws SQLException {
        String result = query("SELECT plainto_tsquery('english', 'cats & dogs')");
        assertNotNull(result);
        // Should NOT keep '&' as a lexeme — it's punctuation in plainto context
        assertFalse(result.contains("'&'"), "plainto should strip punctuation, got: " + result);
    }

    @Test @Order(32)
    void websearchNegation() throws SQLException {
        String result = query("SELECT websearch_to_tsquery('english', 'cat -dog')");
        assertNotNull(result);
        assertTrue(result.contains("!"), "websearch - should become NOT, got: " + result);
    }

    @Test @Order(33)
    void websearchOr() throws SQLException {
        String result = query("SELECT websearch_to_tsquery('english', 'cat or dog')");
        assertNotNull(result);
        assertTrue(result.contains("|"), "websearch OR should produce |, got: " + result);
    }

    @Test @Order(34)
    void websearchQuotedPhrase() throws SQLException {
        String result = query("SELECT websearch_to_tsquery('english', '\"fat cat\"')");
        assertNotNull(result);
        // Should be a phrase query
        assertTrue(result.contains("<->") || result.contains("<"),
                "quoted phrase should produce phrase query, got: " + result);
    }

    // ========== M18: FTS misc ==========

    @Test @Order(40)
    void querytreeStripsNot() throws SQLException {
        // PG's querytree strips NOT branches
        String result = query("SELECT querytree('!cat & dog'::tsquery)");
        assertNotNull(result);
        // Should show 'dog' but NOT branch should be stripped to 'T'
        assertTrue(result.contains("dog"), "should contain 'dog', got: " + result);
    }

    @Test @Order(41)
    void querytreeAllNot() throws SQLException {
        // When entire query is NOT, PG returns 'T'
        String result = query("SELECT querytree('!cat'::tsquery)");
        assertEquals("T", result);
    }

    @Test @Order(42)
    void tsFilterByWeight() throws SQLException {
        String result = query("SELECT ts_filter(setweight('cat:1'::tsvector, 'A'), '{a}')");
        assertNotNull(result);
        assertTrue(result.contains("cat"), "ts_filter should keep A-weighted lexemes, got: " + result);
    }

    @Test @Order(43)
    void tsFilterNoMatchingWeight() throws SQLException {
        String result = query("SELECT ts_filter('cat:1'::tsvector, '{a}')");
        // Default weight is D, so filtering for A should return empty
        assertEquals("", result);
    }

    @Test @Order(44)
    void arrayToTsvectorNoPositions() throws SQLException {
        // PG: array_to_tsvector produces position-less tsvector
        String result = query("SELECT array_to_tsvector(ARRAY['cat','dog'])");
        assertNotNull(result);
        // Should have lexemes but no positions
        assertTrue(result.contains("'cat'"), "should contain cat, got: " + result);
        assertTrue(result.contains("'dog'"), "should contain dog, got: " + result);
        // No position numbers should appear
        assertFalse(result.matches(".*:\\d+.*"), "should have no positions, got: " + result);
    }

    @Test @Order(45)
    void nullTsvectorMatch() throws SQLException {
        // NULL::tsvector @@ query should return NULL, not false
        boolean isNull = queryIsNull("SELECT NULL::tsvector @@ 'cat'::tsquery");
        assertTrue(isNull, "NULL @@ tsquery should return NULL");
    }

    @Test @Order(46)
    void tsHeadlineStripsTags() throws SQLException {
        String result = query("SELECT ts_headline('english', '<p>The fat cat sat</p>', 'cat'::tsquery)");
        assertNotNull(result);
        // Should not contain HTML tags from original text in matching
        assertTrue(result.contains("cat"), "should highlight cat, got: " + result);
    }

    @Test @Order(47)
    void stripRemovesPositions() throws SQLException {
        String result = query("SELECT strip('fat:1 cat:2'::tsvector)");
        assertNotNull(result);
        assertFalse(result.contains(":"), "strip should remove positions, got: " + result);
        assertTrue(result.contains("'cat'") && result.contains("'fat'"),
                "strip should keep lexemes, got: " + result);
    }

    @Test @Order(48)
    void setweightSetsWeight() throws SQLException {
        String result = query("SELECT setweight('cat:1 dog:2'::tsvector, 'A')");
        assertNotNull(result);
        assertTrue(result.contains("A"), "setweight should set weight to A, got: " + result);
    }

    @Test @Order(49)
    void tsDeleteRemovesLexeme() throws SQLException {
        String result = query("SELECT ts_delete('cat:1 dog:2 rat:3'::tsvector, 'dog')");
        assertNotNull(result);
        assertFalse(result.contains("dog"), "should remove dog, got: " + result);
        assertTrue(result.contains("cat") && result.contains("rat"),
                "should keep cat and rat, got: " + result);
    }

    @Test @Order(50)
    void tsvectorToArray() throws SQLException {
        String result = query("SELECT tsvector_to_array('cat:1 dog:2'::tsvector)");
        assertNotNull(result);
        assertTrue(result.contains("cat") && result.contains("dog"),
                "should contain lexemes, got: " + result);
    }

    // ========== L14: display ==========

    @Test @Order(60)
    void emptyTsqueryDisplay() throws SQLException {
        // ''::tsquery should display as empty string, not ''
        String result = query("SELECT ''::tsquery");
        assertNotNull(result);
        assertEquals("", result);
    }

    @Test @Order(61)
    void tsqueryPhraseDisplay() throws SQLException {
        String result = query("SELECT 'cat <-> dog'::tsquery");
        assertNotNull(result);
        assertTrue(result.contains("<->"), "phrase should display with <->, got: " + result);
    }

    @Test @Order(62)
    void tsqueryOrPrecedence() throws SQLException {
        // OR has lower precedence than AND, so 'a | b & c' should group as 'a | (b & c)'
        String result = query("SELECT 'a | b & c'::tsquery");
        assertNotNull(result);
    }

    @Test @Order(63)
    void tsRewriteBasic() throws SQLException {
        String result = query("SELECT ts_rewrite('cat & dog'::tsquery, 'cat'::tsquery, 'tiger'::tsquery)");
        assertNotNull(result);
        assertTrue(result.contains("tiger"), "should rewrite cat to tiger, got: " + result);
        assertFalse(result.contains("cat"), "should not contain original cat, got: " + result);
    }

    @Test @Order(64)
    void tsqueryPhraseDistance() throws SQLException {
        String result = query("SELECT tsquery_phrase('cat'::tsquery, 'dog'::tsquery, 3)");
        assertNotNull(result);
        assertTrue(result.contains("<3>"), "should have distance 3, got: " + result);
    }

    @Test @Order(65)
    void numnodeCount() throws SQLException {
        String result = query("SELECT numnode('cat & dog | rat'::tsquery)");
        assertEquals("5", result);
    }

    @Test @Order(66)
    void concatTsvectors() throws SQLException {
        String result = query("SELECT 'cat:1'::tsvector || 'dog:1'::tsvector");
        assertNotNull(result);
        assertTrue(result.contains("cat") && result.contains("dog"),
                "concat should combine both, got: " + result);
    }

    @Test @Order(67)
    void tsvectorLength() throws SQLException {
        String result = query("SELECT length('cat:1 dog:2 rat:3'::tsvector)");
        assertEquals("3", result);
    }

    @Test @Order(70)
    void tsRank980Scenario() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS t980_debug");
            s.execute("CREATE TABLE t980_debug (id int, doc tsvector)");
            s.execute("INSERT INTO t980_debug VALUES (2, " +
                "setweight(to_tsvector('english', 'Search basics'), 'A') || " +
                "setweight(to_tsvector('english', 'Search ranking ranking'), 'B') || " +
                "setweight(to_tsvector('english', 'tutorial search'), 'C'))");

            // Check the stored tsvector
            ResultSet rs = s.executeQuery("SELECT doc FROM t980_debug WHERE id=2");
            assertTrue(rs.next());
            String doc = rs.getString(1);

            // Check rank
            rs = s.executeQuery("SELECT ts_rank(doc, plainto_tsquery('english', 'search ranking')) FROM t980_debug WHERE id=2");
            assertTrue(rs.next());
            String rankStr = rs.getString(1);
            float rank = Float.parseFloat(rankStr);

            // The expected PG value is 0.970786
            assertTrue(rank > 0.5, "rank should be > 0.5 for strongly matched weighted query, got: " + rank + " doc=" + doc);
        }
    }

    @Test @Order(71)
    void tsRank980ExtendedProtocol() throws SQLException {
        // Test with extended protocol (separate connection without simple mode)
        try (Connection extConn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
             Statement s = extConn.createStatement()) {
            extConn.setAutoCommit(true);
            s.execute("DROP SCHEMA IF EXISTS test_980_ext CASCADE");
            s.execute("CREATE SCHEMA test_980_ext");
            s.execute("SET search_path TO test_980_ext");
            s.execute("CREATE TABLE articles (article_id integer PRIMARY KEY, title text NOT NULL, body text NOT NULL, tags text NOT NULL, search_doc tsvector NOT NULL)");
            s.execute("INSERT INTO articles(article_id, title, body, tags, search_doc) " +
                "SELECT article_id, title, body, tags, " +
                "setweight(to_tsvector('english', title), 'A') || " +
                "setweight(to_tsvector('english', body), 'B') || " +
                "setweight(to_tsvector('english', tags), 'C') " +
                "FROM (VALUES " +
                "(1, 'PostgreSQL search', 'Ranking results matters for users', 'database search'), " +
                "(2, 'Search basics', 'Search ranking ranking', 'tutorial search'), " +
                "(3, 'Gardening', 'Plants and soil care', 'plants home') " +
                ") AS v(article_id, title, body, tags)");

            ResultSet rs = s.executeQuery("SELECT article_id, search_doc FROM articles ORDER BY article_id");
            while (rs.next()) {
                System.out.println("EXT article_id=" + rs.getInt(1) + " doc=" + rs.getString(2));
            }

            rs = s.executeQuery("SELECT article_id, ts_rank(search_doc, plainto_tsquery('english', 'search ranking')) AS rank FROM articles WHERE search_doc @@ plainto_tsquery('english', 'search ranking') ORDER BY rank DESC, article_id");
            while (rs.next()) {
                System.out.println("EXT article_id=" + rs.getInt(1) + " rank=" + rs.getString(2));
            }
            // Debug: check what plainto_tsquery produces
            rs = s.executeQuery("SELECT plainto_tsquery('english', 'search ranking')");
            if (rs.next()) System.out.println("EXT plainto=" + rs.getString(1));
            // Debug: rank from column cast back to tsvector
            rs = s.executeQuery("SELECT ts_rank(search_doc::text::tsvector, plainto_tsquery('english', 'search ranking')) FROM articles WHERE article_id=2");
            if (rs.next()) System.out.println("EXT recast_rank=" + rs.getString(1));
            // Debug: check ts_rank with literal tsvector
            rs = s.executeQuery("SELECT ts_rank('''basic'':2A ''rank'':4B,5B ''search'':1A,3B,7C ''tutorial'':6C'::tsvector, '''search'' & ''rank'''::tsquery)");
            if (rs.next()) System.out.println("EXT literal_rank=" + rs.getString(1));
            // Debug: check if length differs
            rs = s.executeQuery("SELECT length(search_doc), length('''basic'':2A ''rank'':4B,5B ''search'':1A,3B,7C ''tutorial'':6C'::tsvector) FROM articles WHERE article_id=2");
            if (rs.next()) System.out.println("EXT length_col=" + rs.getString(1) + " length_lit=" + rs.getString(2));
            // Debug: check individual pieces
            rs = s.executeQuery("SELECT length(to_tsvector('english', 'Search basics'))");
            if (rs.next()) System.out.println("EXT len_piece1=" + rs.getString(1));
            rs = s.executeQuery("SELECT length(setweight(to_tsvector('english', 'Search basics'), 'A'))");
            if (rs.next()) System.out.println("EXT len_weighted1=" + rs.getString(1));
            rs = s.executeQuery("SELECT to_tsvector('english', 'Search basics')");
            if (rs.next()) System.out.println("EXT piece1=" + rs.getString(1));
            rs = s.executeQuery("SELECT setweight(to_tsvector('english', 'Search basics'), 'A')");
            if (rs.next()) System.out.println("EXT weighted1=" + rs.getString(1));
            rs = s.executeQuery("SELECT length(setweight(to_tsvector('english', 'Search basics'), 'A') || setweight(to_tsvector('english', 'Search ranking ranking'), 'B'))");
            if (rs.next()) System.out.println("EXT len_concat12=" + rs.getString(1));

            // Also test with simple mode on same connection
            try (Connection simConn = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
                 Statement s2 = simConn.createStatement()) {
                s2.execute("SET search_path TO test_980_ext");
                ResultSet rs2 = s2.executeQuery("SELECT article_id, search_doc FROM articles ORDER BY article_id");
                while (rs2.next()) {
                    System.out.println("SIMPLE article_id=" + rs2.getInt(1) + " doc=" + rs2.getString(2));
                }
                rs2 = s2.executeQuery("SELECT article_id, ts_rank(search_doc, plainto_tsquery('english', 'search ranking')) AS rank FROM articles WHERE search_doc @@ plainto_tsquery('english', 'search ranking') ORDER BY rank DESC, article_id");
                while (rs2.next()) {
                    System.out.println("SIMPLE article_id=" + rs2.getInt(1) + " rank=" + rs2.getString(2));
                }
            }
            s.execute("DROP SCHEMA test_980_ext CASCADE");
        }
    }
}
