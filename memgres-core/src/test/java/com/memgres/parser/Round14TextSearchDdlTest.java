package com.memgres.parser;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 14 gaps: Text Search DDL — CREATE TEXT SEARCH DICTIONARY /
 * PARSER / TEMPLATE / CONFIGURATION, and related ts_* utilities.
 *
 * Memgres exposes to_tsvector/to_tsquery with hardcoded configs but cannot
 * define new dictionaries, parsers, templates, or configurations. Tests
 * pin down the DDL surface and several runtime ts_* helpers.
 */
class Round14TextSearchDdlTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getInt(1);
        }
    }

    private static String scalarString(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    // =========================================================================
    // A. pg_ts_* catalogs queryable
    // =========================================================================

    @Test
    void pg_ts_config_queryable() throws SQLException {
        // Should have at least 'simple' and 'english' defaults
        assertTrue(scalarInt("SELECT count(*)::int FROM pg_ts_config") >= 2);
    }

    @Test
    void pg_ts_dict_queryable() throws SQLException {
        assertTrue(scalarInt("SELECT count(*)::int FROM pg_ts_dict") >= 1);
    }

    @Test
    void pg_ts_parser_queryable() throws SQLException {
        assertTrue(scalarInt("SELECT count(*)::int FROM pg_ts_parser") >= 1);
    }

    @Test
    void pg_ts_template_queryable() throws SQLException {
        assertTrue(scalarInt("SELECT count(*)::int FROM pg_ts_template") >= 1);
    }

    @Test
    void pg_ts_config_map_queryable() throws SQLException {
        // Maps token_type → dictionary for each config
        assertTrue(scalarInt("SELECT count(*)::int FROM pg_ts_config_map") >= 1);
    }

    // =========================================================================
    // B. CREATE TEXT SEARCH CONFIGURATION
    // =========================================================================

    @Test
    void create_text_search_configuration_copy() throws SQLException {
        exec("CREATE TEXT SEARCH CONFIGURATION r14_ts_cfg1 (COPY = english)");
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_ts_config WHERE cfgname = 'r14_ts_cfg1'"));
    }

    @Test
    void create_text_search_configuration_parser() throws SQLException {
        exec("CREATE TEXT SEARCH CONFIGURATION r14_ts_cfg2 (PARSER = \"default\")");
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_ts_config WHERE cfgname = 'r14_ts_cfg2'"));
    }

    @Test
    void alter_text_search_configuration_add_mapping() throws SQLException {
        exec("CREATE TEXT SEARCH CONFIGURATION r14_ts_cfg3 (COPY = english)");
        exec("ALTER TEXT SEARCH CONFIGURATION r14_ts_cfg3 "
                + "ALTER MAPPING FOR word, asciiword WITH simple");
        // Check that mapping exists
        assertTrue(scalarInt(
                "SELECT count(*)::int FROM pg_ts_config_map "
                        + "WHERE mapcfg = (SELECT oid FROM pg_ts_config WHERE cfgname = 'r14_ts_cfg3')") >= 1);
    }

    @Test
    void drop_text_search_configuration() throws SQLException {
        exec("CREATE TEXT SEARCH CONFIGURATION r14_ts_drop (COPY = english)");
        exec("DROP TEXT SEARCH CONFIGURATION r14_ts_drop");
        assertEquals(0, scalarInt(
                "SELECT count(*)::int FROM pg_ts_config WHERE cfgname = 'r14_ts_drop'"));
    }

    // =========================================================================
    // C. CREATE TEXT SEARCH DICTIONARY
    // =========================================================================

    @Test
    void create_text_search_dictionary_simple_template() throws SQLException {
        exec("CREATE TEXT SEARCH DICTIONARY r14_ts_dict_simple ("
                + "TEMPLATE = pg_catalog.simple, STOPWORDS = english)");
        assertEquals(1, scalarInt(
                "SELECT count(*)::int FROM pg_ts_dict WHERE dictname = 'r14_ts_dict_simple'"));
    }

    @Test
    void create_text_search_dictionary_synonym_template() throws SQLException {
        try {
            exec("CREATE TEXT SEARCH DICTIONARY r14_ts_syn ("
                    + "TEMPLATE = pg_catalog.synonym, SYNONYMS = nothing)");
            // Synonyms file may not exist - but at least parsing should work
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"),
                    "CREATE DICT with synonym TEMPLATE must parse, got: " + msg);
        }
    }

    @Test
    void drop_text_search_dictionary() throws SQLException {
        exec("CREATE TEXT SEARCH DICTIONARY r14_ts_dict_drop ("
                + "TEMPLATE = pg_catalog.simple)");
        exec("DROP TEXT SEARCH DICTIONARY r14_ts_dict_drop");
        assertEquals(0, scalarInt(
                "SELECT count(*)::int FROM pg_ts_dict WHERE dictname = 'r14_ts_dict_drop'"));
    }

    // =========================================================================
    // D. CREATE TEXT SEARCH PARSER / TEMPLATE
    // =========================================================================

    @Test
    void create_text_search_template_parses() throws SQLException {
        // TEMPLATEs require backend C functions — parser must at least accept it.
        try {
            exec("CREATE TEXT SEARCH TEMPLATE r14_ts_tmpl ("
                    + "INIT = dsimple_init, LEXIZE = dsimple_lexize)");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"),
                    "CREATE TEMPLATE must parse, got: " + msg);
        }
    }

    @Test
    void create_text_search_parser_parses() throws SQLException {
        try {
            exec("CREATE TEXT SEARCH PARSER r14_ts_parser ("
                    + "START = prsd_start, GETTOKEN = prsd_nexttoken, "
                    + "END = prsd_end, LEXTYPES = prsd_lextype, HEADLINE = prsd_headline)");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertFalse(msg.contains("syntax"),
                    "CREATE PARSER must parse, got: " + msg);
        }
    }

    // =========================================================================
    // E. ts_* utilities
    // =========================================================================

    @Test
    void ts_lexize_simple_dictionary() throws SQLException {
        // ts_lexize('simple', 'Word') -> {word}
        assertEquals("{word}", scalarString("SELECT ts_lexize('simple', 'Word')::text"));
    }

    @Test
    void ts_parse_default_parser() throws SQLException {
        // ts_parse('default', 'hello world') yields 2 tokens
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM ts_parse('default', 'hello world')")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 2, "ts_parse should yield tokens for default parser");
        }
    }

    @Test
    void ts_token_type_default_parser() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM ts_token_type('default')")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1, "ts_token_type should enumerate token types");
        }
    }

    @Test
    void ts_debug_returns_rows() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::int FROM ts_debug('hello world')")) {
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) >= 1, "ts_debug must return rows");
        }
    }

    // =========================================================================
    // F. ts_headline advanced options
    // =========================================================================

    @Test
    void ts_headline_with_minwords_maxwords() throws SQLException {
        String v = scalarString(
                "SELECT ts_headline('english', "
                        + "'a b c d e f g h the quick brown fox jumps over the lazy dog i j k l', "
                        + "to_tsquery('english', 'fox'), "
                        + "'MinWords=3, MaxWords=5')");
        assertNotNull(v);
        assertTrue(v.contains("<b>") || v.toLowerCase().contains("fox"),
                "ts_headline with options should return highlighted text; got: " + v);
    }

    @Test
    void ts_headline_with_startsel_stopsel() throws SQLException {
        String v = scalarString(
                "SELECT ts_headline('english', 'the quick brown fox', "
                        + "to_tsquery('english', 'fox'), "
                        + "'StartSel=<em>, StopSel=</em>')");
        assertNotNull(v);
        assertTrue(v.contains("<em>") && v.contains("</em>"),
                "ts_headline StartSel/StopSel must be honored; got: " + v);
    }

    @Test
    void ts_headline_with_maxfragments() throws SQLException {
        // MaxFragments > 0 uses fragment-based highlighting
        String v = scalarString(
                "SELECT ts_headline('english', "
                        + "'the quick brown fox jumps over the lazy dog over the river', "
                        + "to_tsquery('english', 'fox | river'), "
                        + "'MaxFragments=2, FragmentDelimiter=|||')");
        assertNotNull(v);
    }

    @Test
    void websearch_to_tsquery_basic() throws SQLException {
        // PG 11+: websearch_to_tsquery parses Google-style queries
        assertEquals("'quick' & 'fox'",
                scalarString("SELECT websearch_to_tsquery('english', 'quick fox')::text"));
    }

    @Test
    void websearch_to_tsquery_or() throws SQLException {
        String v = scalarString("SELECT websearch_to_tsquery('english', 'quick OR fox')::text");
        assertNotNull(v);
        assertTrue(v.contains("|"), "websearch_to_tsquery OR must become |; got: " + v);
    }

    @Test
    void websearch_to_tsquery_phrase() throws SQLException {
        String v = scalarString("SELECT websearch_to_tsquery('english', '\"quick fox\"')::text");
        assertNotNull(v);
        assertTrue(v.contains("<->"), "phrase must become <->; got: " + v);
    }

    @Test
    void websearch_to_tsquery_negation() throws SQLException {
        String v = scalarString("SELECT websearch_to_tsquery('english', 'fox -lazy')::text");
        assertNotNull(v);
        assertTrue(v.contains("!"), "negation must become !; got: " + v);
    }
}
