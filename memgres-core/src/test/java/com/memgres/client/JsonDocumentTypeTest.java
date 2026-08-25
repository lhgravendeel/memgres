package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A document carries the type it was given, wherever it is passed.
 *
 * <p>json and jsonb are two types and neither is read as the other, so a COALESCE, GREATEST or
 * LEAST whose arguments name both has no type its whole list shares. PostgreSQL settles that type
 * from the arguments' declarations before it evaluates any of them, so an aggregate that found no
 * rows fails the same list an aggregate that found many would — the answer never comes into it.
 *
 * <p>The declared type is also what says whether an array's elements are documents to be written
 * into a larger one or the characters they happen to hold, and what says that a document handed
 * to the text search functions comes back a document with its matches marked inside it rather
 * than the array those braces would spell if the value alone were read.
 */
class JsonDocumentTypeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE jdt_docs (id int, v json, name text)");
        }
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

    private static void run(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
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

    private static String messageOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    // ------------------------------------------------ one type for the whole

    /** A list that names both document types has no type they all share. */
    @Test
    void aListThatNamesBothDocumentTypesIsRefused() {
        assertEquals("42846", stateOf("SELECT coalesce('{}'::json, '[]'::jsonb)"));
        assertTrue(messageOf("SELECT coalesce('{}'::json, '[]'::jsonb)")
                .contains("COALESCE could not convert type jsonb to json"));
        assertTrue(messageOf("SELECT coalesce('{}'::jsonb, '[]'::json)")
                .contains("COALESCE could not convert type json to jsonb"));
        assertTrue(messageOf("SELECT greatest('{}'::json, '[]'::jsonb)")
                .contains("GREATEST could not convert type jsonb to json"));
        assertTrue(messageOf("SELECT least('{}'::json, '[]'::jsonb)")
                .contains("LEAST could not convert type jsonb to json"));
    }

    /** A list that names one of them is answered in it. */
    @Test
    void aListThatNamesOneDocumentTypeIsAnsweredInIt() throws Exception {
        assertEquals("{\"a\":1}", one("SELECT coalesce('{\"a\":1}'::json, '[]'::json)"));
        assertEquals("{\"a\": 1, \"b\": 2}",
                one("SELECT coalesce('{\"b\":2,\"a\":1}'::jsonb, '[]'::jsonb)"));
        assertEquals("[]", one("SELECT coalesce(NULL::jsonb, '[]'::jsonb)"));
        assertEquals("[]", one("SELECT coalesce(NULL::json, NULL::json, '[]'::json)"));
        assertEquals("[2]", one("SELECT greatest('[1]'::jsonb, '[2]'::jsonb)"));
        assertEquals("[1]", one("SELECT least('[1]'::jsonb, '[2]'::jsonb)"));
    }

    /**
     * The aggregates that build a document answer in json unless a RETURNING says otherwise, and
     * the type of the list around them is settled before either is evaluated — so a list that does
     * not go with json is refused whether or not the aggregate found any rows.
     */
    @Test
    void anAggregateOverNoRowsStillHasTheTypeItWasDeclaredWith() throws Exception {
        run("DELETE FROM jdt_docs");
        assertEquals("json", one("SELECT pg_typeof(json_agg(v)) FROM jdt_docs"));
        assertEquals("json", one("SELECT pg_typeof(JSON_ARRAYAGG(v)) FROM jdt_docs"));
        assertEquals("42846", stateOf("SELECT coalesce(json_agg(v), '[]'::jsonb) FROM jdt_docs"));
        assertEquals("42846", stateOf("SELECT coalesce(JSON_ARRAYAGG(v), '[]'::jsonb) FROM jdt_docs"));
        assertEquals("[]", one("SELECT coalesce(json_agg(v), '[]'::json) FROM jdt_docs"));
        run("INSERT INTO jdt_docs VALUES (1, '{\"a\":1}', 'ab'), (2, '{\"a\":2}', NULL),"
                + " (3, '{\"a\":1}', 'ab')");
        assertEquals("42846", stateOf("SELECT coalesce(json_agg(v), '[]'::jsonb) FROM jdt_docs"));
    }

    /** DISTINCT collapses the arguments to the values they are, and a null is one of them. */
    @Test
    void distinctOverAnAggregateKeepsOneNull() throws Exception {
        run("DELETE FROM jdt_docs");
        run("INSERT INTO jdt_docs VALUES (1, '{\"a\":1}', 'ab'), (2, '{\"a\":2}', NULL),"
                + " (3, '{\"a\":1}', 'ab')");
        assertEquals("[\"ab\", null]", one("SELECT json_agg(DISTINCT name ORDER BY name) FROM jdt_docs"));
        assertEquals("[\"ab\", null]", one("SELECT jsonb_agg(DISTINCT name ORDER BY name) FROM jdt_docs"));
        assertEquals("1", one("SELECT count(DISTINCT name) FROM jdt_docs"));
    }

    // ------------------------------------------ the element type of an array

    /** An element is written as what its element type says it is, not as what its text looks like. */
    @Test
    void anArrayElementIsWrittenAsWhatItsTypeSaysItIs() throws Exception {
        assertEquals("[1,2]", one("SELECT array_to_json(ARRAY[1,2])"));
        assertEquals("[[1,2],[3,4]]", one("SELECT array_to_json(ARRAY[[1,2],[3,4]])"));
        assertEquals("[{\"a\":1},[2]]",
                one("SELECT array_to_json(ARRAY['{\"a\":1}'::json,'[2]'::json])"));
        assertEquals("[{\"a\": 1},[2]]",
                one("SELECT array_to_json(ARRAY['{\"a\":1}'::jsonb,'[2]'::jsonb])"));
        assertEquals("[\"{1,2}\",\"x\"]", one("SELECT array_to_json(ARRAY['{1,2}','x'])"));
        assertEquals("[1,2]", one("SELECT array_to_json('{1,2}'::int[])"));
        assertEquals("[\"a\",\"b\"]", one("SELECT array_to_json(ARRAY['a','b'])"));
        assertEquals("[{\"a\":1}]", one("SELECT to_json(ARRAY['{\"a\":1}'::json])"));
        assertEquals("[\"{\\\"a\\\":1}\"]", one("SELECT to_json(ARRAY['{\"a\":1}'::text])"));
    }

    // ---------------------------------------- a document through text search

    /**
     * The parser runs on through the pieces of a document rather than beginning again at one for
     * each, and a piece that held nothing but stop words still moves the next piece along by one,
     * so that no phrase spans the two.
     */
    @Test
    void aDocumentIsReadAsADocumentByTheTextSearchFunctions() throws Exception {
        assertEquals("'cat':1 'zz':3",
                one("SELECT to_tsvector('english', '{\"b\":\"cat\",\"a\":\"zz\",\"c\":1}'::json)"));
        assertEquals("'cat':3 'zz':1",
                one("SELECT to_tsvector('english', '{\"b\":\"cat\",\"a\":\"zz\",\"c\":1}'::jsonb)"));
        assertEquals("'1':10 'b':1 'c':8 'cat':3 'zz':6",
                one("SELECT json_to_tsvector('english', '{\"b\":\"cat\",\"a\":\"zz\",\"c\":1}'::json,"
                        + " '[\"all\"]')"));
        assertEquals("'1':10 'b':4 'c':8 'cat':6 'zz':2",
                one("SELECT jsonb_to_tsvector('english', '{\"b\":\"cat\",\"a\":\"zz\",\"c\":1}'::jsonb,"
                        + " '[\"all\"]')"));
        // "a" is a stop word: it emits nothing and still moves what follows it along by one
        assertEquals("'bb':1 'cat':3 'zz':6",
                one("SELECT json_to_tsvector('english', '{\"bb\":\"cat\",\"a\":\"zz\"}'::json,"
                        + " '[\"all\"]')"));
        assertEquals("'bb':4 'cat':6 'zz':2",
                one("SELECT jsonb_to_tsvector('english', '{\"bb\":\"cat\",\"a\":\"zz\"}'::jsonb,"
                        + " '[\"all\"]')"));
        assertEquals("'cat':2", one("SELECT json_to_tsvector('english', '{\"a\":\"cat\"}'::json,"
                + " '[\"all\"]')"));
        assertEquals("'cat':3", one("SELECT json_to_tsvector('english', '[\"the\",\"a\",\"cat\"]'::json,"
                + " '[\"all\"]')"));
        assertEquals("'bb':1 'cat':2 'zz':4", one("SELECT to_tsvector('english', 'bb cat a zz')"));
    }

    /** What ts_headline hands back is what it was given, with the matches marked inside it. */
    @Test
    void tsHeadlineAnswersInTheTypeItWasGiven() throws Exception {
        assertEquals("{\"b\":\"<b>cat</b>\",\"a\":\"zz\"}",
                one("SELECT ts_headline('english', '{\"b\":\"cat\",\"a\":\"zz\"}'::json,"
                        + " to_tsquery('english','cat'))"));
        assertEquals("{\"a\": \"zz\", \"b\": \"<b>cat</b>\"}",
                one("SELECT ts_headline('english', '{\"b\":\"cat\",\"a\":\"zz\"}'::jsonb,"
                        + " to_tsquery('english','cat'))"));
        assertEquals("json", one("SELECT pg_typeof(ts_headline('english', '{\"b\":\"cat\"}'::json,"
                + " to_tsquery('english','cat')))"));
        assertEquals("jsonb", one("SELECT pg_typeof(ts_headline('english', '{\"b\":\"cat\"}'::jsonb,"
                + " to_tsquery('english','cat')))"));
        assertEquals("text", one("SELECT pg_typeof(ts_headline('english', 'cat',"
                + " to_tsquery('english','cat')))"));
    }
}
