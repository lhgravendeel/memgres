package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A set produced by a grouped query, and the configuration text is read with.
 *
 * <p>A set-returning function is a set wherever it stands, a grouped query included:
 * {@code unnest(array_agg(v))} answers with the values one after another rather than with the
 * array they were gathered into, and an item written around one is worked out for each of them.
 *
 * <p>Which configuration a statement that names none reads text with is the session's to say, in
 * {@code default_text_search_config}; and a configuration reads words with the dictionary its own
 * mapping sends them to, whatever it was copied from.
 */
class SetsAndConfigurationsTest {

    private static Memgres memgres;
    private static Connection conn;

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
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static List<String> column(String sql) throws SQLException {
        List<String> values = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) values.add(rs.getString(1));
        }
        return values;
    }

    /** Every value of every row, row by row. */
    private static List<String> flat(String sql) throws SQLException {
        List<String> values = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= width; i++) values.add(rs.getString(i));
            }
        }
        return values;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** A set gathered by an aggregate is still a set. */
    @Test
    void aSetAGroupedQueryProduced() throws SQLException {
        exec("CREATE TABLE zsc_g (g text, v int)");
        exec("INSERT INTO zsc_g VALUES ('a',10),('a',20),('b',30)");
        assertEquals(java.util.Arrays.asList("10", "20", "30"),
                column("SELECT unnest(array_agg(v))::text AS u FROM zsc_g ORDER BY u"));
        assertEquals(java.util.Arrays.asList("11", "21", "31"),
                column("SELECT (unnest(array_agg(v)) + 1)::text AS u FROM zsc_g ORDER BY u"));
        assertEquals(java.util.Arrays.asList("a", "10", "a", "20", "b", "30"),
                flat("SELECT g, unnest(array_agg(v))::text AS u FROM zsc_g GROUP BY g"
                        + " ORDER BY g, u"));
        // What the element is, is what the set holds -- not the array it was gathered into.
        assertEquals("integer", one("SELECT pg_typeof(unnest(array_agg(v)))::text FROM zsc_g"));
        // A set of nothing produces no rows at all.
        assertEquals(0, column("SELECT unnest('{}'::int[])::text FROM zsc_g").size());
        exec("DROP TABLE zsc_g");
    }

    /** Which configuration text is read with is the session's to say. */
    @Test
    void theConfigurationASessionReadsWith() throws SQLException {
        assertEquals("english", one("SELECT get_current_ts_config()::text"));
        assertEquals("'cat':2", one("SELECT to_tsvector('The Cats')::text"));
        exec("SET default_text_search_config = 'pg_catalog.simple'");
        assertEquals("simple", one("SELECT get_current_ts_config()::text"));
        assertEquals("'cats':2 'the':1", one("SELECT to_tsvector('The Cats')::text"));
        assertEquals("'the' & 'cats'", one("SELECT plainto_tsquery('The Cats')::text"));
        assertEquals("'cats'", one("SELECT to_tsquery('Cats')::text"));
        // A configuration named in the call is still that configuration.
        assertEquals("'cat':2", one("SELECT to_tsvector('english', 'The Cats')::text"));
        exec("SET default_text_search_config = 'english'");
        assertEquals("'cat':2", one("SELECT to_tsvector('The Cats')::text"));
    }

    /** A configuration reads words with the dictionary its own mapping names. */
    @Test
    void theDictionaryAConfigurationSendsWordsTo() throws SQLException {
        exec("CREATE TEXT SEARCH CONFIGURATION zsc_cfg (COPY = simple)");
        assertEquals("'cats':2 'the':1", one("SELECT to_tsvector('zsc_cfg', 'The Cats')::text"));
        exec("ALTER TEXT SEARCH CONFIGURATION zsc_cfg"
                + " ALTER MAPPING FOR asciiword WITH english_stem");
        assertEquals("'cat':2", one("SELECT to_tsvector('zsc_cfg', 'The Cats')::text"));
        exec("DROP TEXT SEARCH CONFIGURATION zsc_cfg");
    }
}
