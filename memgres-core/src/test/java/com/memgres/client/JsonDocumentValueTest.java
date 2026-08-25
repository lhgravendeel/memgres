package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A JSON document is the value it spells and not the text it prints as.
 *
 * <p>Deciding what a text spells needs a reader of JSON, and there was none: whether a text was
 * JSON at all was answered by a heuristic that handed its numbers to {@code Double.parseDouble},
 * which reads {@code 1.}, {@code .5} and {@code 1d} that JSON does not. Once read, a jsonb was
 * kept as the text it prints as and compared as that text, so {@code 1} and {@code 1.0} were two
 * values wherever rows are gathered into groups — DISTINCT, GROUP BY, DISTINCT ON, a window's
 * PARTITION BY and the set operations — while {@code =} already said they were one.
 *
 * <p>The type is also what tells a document from a string, since the two are the same characters:
 * a document collected into a larger one was decided by whether its text began with a brace, which
 * made every scalar document a quoted string, so {@code jsonb_agg} of the number 1 answered
 * {@code ["1"]}. And json is not jsonb: it has the operators of a text and none of the operators
 * that read a document, not even an equality, so a query that would gather json values together
 * has no operator to gather them by.
 */
class JsonDocumentValueTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE jdv_t (id int, j jsonb, g json)");
            st.execute("INSERT INTO jdv_t VALUES (1, '1', '1'), (2, '1.0', '1.0'),"
                    + " (3, '{\"a\":1,\"b\":2}', '{\"a\":1}')");
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
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) b.append('|');
                    String v = rs.getString(i);
                    b.append(rs.wasNull() ? "NULL" : v);
                }
                out.add(b.toString());
            }
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

    private static String messageOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    // ---------------------------------------------------------------- reading

    /** JSON's numbers are JSON's, not whatever a Java parse of a double happens to accept. */
    @Test
    void aNumberIsWrittenTheWayJsonWritesOne() {
        assertEquals("22P02", stateOf("SELECT '1.'::jsonb"));
        assertEquals("22P02", stateOf("SELECT '.5'::jsonb"));
        assertEquals("22P02", stateOf("SELECT '1d'::jsonb"));
        assertEquals("22P02", stateOf("SELECT '01'::jsonb"));
        assertEquals("22P02", stateOf("SELECT '+1'::jsonb"));
        assertEquals("22P02", stateOf("SELECT '1e'::jsonb"));
        assertEquals("22P02", stateOf("SELECT '0x10'::jsonb"));
    }

    /** A number JSON does write is read, and read as the number it is. */
    @Test
    void theNumbersJsonDoesWriteAreRead() throws Exception {
        assertEquals("1000", one("SELECT '1e3'::jsonb"));
        assertEquals("-0.005", one("SELECT '-0.5e-2'::jsonb"));
        assertEquals("0", one("SELECT '-0'::jsonb"));
    }

    /** A document is one value, so a second one after it is not part of it. */
    @Test
    void aDocumentEndsWhereItEnds() {
        assertEquals("22P02", stateOf("SELECT '1 2'::jsonb"));
        assertEquals("22P02", stateOf("SELECT '{} {}'::jsonb"));
        assertEquals("22P02", stateOf("SELECT '[1,]'::jsonb"));
    }

    /** Whether a text is JSON is answered by reading it. */
    @Test
    void isJsonIsAnsweredByTheReader() throws Exception {
        assertEquals("t", one("SELECT '{\"a\":1}'::text IS JSON"));
        assertEquals("f", one("SELECT '1.'::text IS JSON"));
        assertEquals("t", one("SELECT '1'::text IS JSON SCALAR"));
        assertEquals("t", one("SELECT '[1]'::text IS JSON ARRAY"));
    }

    /** A key repeated anywhere in the document, not only at its top, is a key repeated. */
    @Test
    void uniqueKeysAreLookedForThroughout() throws Exception {
        assertEquals("f", one("SELECT '{\"a\":1,\"a\":2}'::text IS JSON WITH UNIQUE KEYS"));
        assertEquals("f", one("SELECT '{\"a\":{\"b\":1,\"b\":2}}'::text IS JSON WITH UNIQUE KEYS"));
        assertEquals("t", one("SELECT '{\"a\":{\"b\":1},\"b\":2}'::text IS JSON WITH UNIQUE KEYS"));
    }

    // ------------------------------------------------------- comparing values

    /** Two spellings of one number are one number, wherever in the document they are. */
    @Test
    void aNumberIsTheNumberItIsHoweverItWasWritten() throws Exception {
        assertEquals("t", one("SELECT '1'::jsonb = '1.0'::jsonb"));
        assertEquals("t", one("SELECT '{\"a\":1}'::jsonb = '{\"a\":1.0}'::jsonb"));
        assertEquals("t", one("SELECT '[1]'::jsonb = '[1.00]'::jsonb"));
        assertEquals("t", one("SELECT '{\"a\":1,\"b\":2}'::jsonb = '{\"b\":2,\"a\":1}'::jsonb"));
    }

    /** The gathering clauses gather by that same comparison and not by the printed text. */
    @Test
    void rowsAreGatheredByTheDocumentTheySpell() throws Exception {
        assertEquals("2", one("SELECT count(DISTINCT j) FROM jdv_t"));
        assertEquals(List.of("1", "{\"a\": 1, \"b\": 2}"),
                rows("SELECT j FROM jdv_t GROUP BY j ORDER BY j::text"));
        assertEquals(List.of("1", "{\"a\": 1, \"b\": 2}"),
                rows("SELECT DISTINCT j FROM jdv_t ORDER BY 1"));
        assertEquals(List.of("1", "3"),
                rows("SELECT DISTINCT ON (j) id FROM jdv_t ORDER BY j, id"));
    }

    @Test
    void aSetOperationTellsThemApartTheSameWay() throws Exception {
        assertEquals("2", one("SELECT count(*) FROM (SELECT j FROM jdv_t"
                + " UNION SELECT j FROM jdv_t) s"));
        assertEquals("1", one("SELECT count(*) FROM (SELECT j FROM jdv_t WHERE id IN (1,2)"
                + " INTERSECT SELECT j FROM jdv_t WHERE id = 2) s"));
        assertEquals("1", one("SELECT count(*) FROM (SELECT j FROM jdv_t"
                + " EXCEPT SELECT j FROM jdv_t WHERE id = 1) s"));
    }

    @Test
    void aWindowPartitionsByItTheSameWay() throws Exception {
        assertEquals(List.of("2", "2", "1"),
                rows("SELECT count(*) OVER (PARTITION BY j) FROM jdv_t ORDER BY id"));
    }

    // ------------------------------------------------------ json has no equality

    /**
     * json is the text it was given, kept as written; PostgreSQL registers no equality over it,
     * so a clause that would gather json values together has nothing to gather them by.
     */
    @Test
    void jsonValuesCannotBeGatheredIntoGroups() {
        assertEquals("42883", stateOf("SELECT DISTINCT g FROM jdv_t"));
        assertEquals("42883", stateOf("SELECT g FROM jdv_t GROUP BY g"));
        assertEquals("42883", stateOf("SELECT g FROM jdv_t UNION SELECT g FROM jdv_t"));
        assertEquals("42883", stateOf("SELECT count(DISTINCT g) FROM jdv_t"));
        assertEquals("42883", stateOf("SELECT count(*) OVER (PARTITION BY g) FROM jdv_t"));
        assertTrue(messageOf("SELECT DISTINCT g FROM jdv_t")
                        .contains("could not identify an equality operator for type json"),
                messageOf("SELECT DISTINCT g FROM jdv_t"));
    }

    /** jsonb has that equality, which is why the same queries over it are answerable. */
    @Test
    void jsonbHasTheEqualityJsonLacks() throws Exception {
        assertNull(stateOf("SELECT DISTINCT j FROM jdv_t"));
        assertEquals("2", one("SELECT count(*) FROM (SELECT DISTINCT j FROM jdv_t) s"));
    }

    /** The operators that read a document are jsonb's, and json does not borrow them. */
    @Test
    void theOperatorsThatReadADocumentAreJsonbs() {
        assertEquals("42883", stateOf("SELECT '{\"a\":1}'::json ? 'a'"));
        assertEquals("42883", stateOf("SELECT '{\"a\":1}'::json #- '{a}'"));
        assertEquals("42883", stateOf("SELECT '{\"a\":1}'::json @? '$.a'"));
        assertEquals("42883", stateOf("SELECT '{\"a\":1}'::json @> '{}'::json"));
    }

    /** The ones json does have are still its own. */
    @Test
    void theOnesJsonDoesHaveStillWork() throws Exception {
        assertEquals("1", one("SELECT '{\"a\":1}'::json #> '{a}'"));
        assertEquals("1", one("SELECT '{\"a\":1}'::json -> 'a'"));
        assertEquals("1", one("SELECT '{\"a\":1}'::json ->> 'a'"));
    }

    // --------------------------------------------- documents inside documents

    /**
     * A document put inside a larger one is written where it stands. Told from a string by the
     * text alone, every document that is not an object or an array became a quoted string.
     */
    @Test
    void aDocumentIsWrittenWhereItStands() throws Exception {
        assertEquals("1", one("SELECT to_jsonb(j) FROM jdv_t WHERE id = 1"));
        assertEquals("[1, {\"a\": 1, \"b\": 2}]",
                one("SELECT jsonb_agg(j ORDER BY id) FROM jdv_t WHERE id IN (1,3)"));
        assertEquals("[1, 1]", one("SELECT jsonb_build_array(j, 1) FROM jdv_t WHERE id = 1"));
        assertEquals("[1, {\"a\":1}]",
                one("SELECT json_agg(g ORDER BY id) FROM jdv_t WHERE id IN (1,3)"));
    }

    /** An array of documents holds documents, so gathering them keeps them documents. */
    @Test
    void anArrayOfDocumentsHoldsDocuments() throws Exception {
        assertEquals("[1, {\"a\": 1, \"b\": 2}]",
                one("SELECT to_jsonb(array_agg(j ORDER BY id)) FROM jdv_t WHERE id IN (1,3)"));
    }

    /** A DISTINCT aggregate over documents tells them apart as documents. */
    @Test
    void aDistinctAggregateCountsDocuments() throws Exception {
        assertEquals("[1]", one("SELECT jsonb_agg(DISTINCT j) FROM jdv_t WHERE id IN (1,2)"));
    }

    // ------------------------------------------------------------- writing out

    /** A character JSON spells with an escape is written with that escape, in keys as in values. */
    @Test
    void whatMustBeEscapedIsEscaped() throws Exception {
        assertEquals("[\"a\\\"b\",\"a\\tb\"]", one("SELECT to_json(ARRAY['a\"b', E'a\\tb'])"));
        assertEquals("{\"k\\tey\": \"v\\nal\"}",
                one("SELECT jsonb_build_object(E'k\\tey', E'v\\nal')"));
    }

    /** A float JSON has no spelling for is written as the text naming it. */
    @Test
    void theNumbersJsonCannotWriteAreWrittenAsText() throws Exception {
        assertEquals("\"Infinity\"", one("SELECT to_json('Infinity'::float8)"));
        assertEquals("\"NaN\"", one("SELECT to_jsonb('NaN'::float8)"));
    }

    @Test
    void aTimestampIsWrittenTheWayJsonWritesOne() throws Exception {
        assertEquals("\"2020-01-02T03:04:05\"",
                one("SELECT to_json('2020-01-02 03:04:05'::timestamp)"));
    }

    // -------------------------------------------------------------- strictness

    /** A function given a null where it needs a value answers null rather than guessing. */
    @Test
    void aNullArgumentIsAnsweredWithNull() throws Exception {
        assertEquals("NULL", one("SELECT jsonb_set('{\"a\":1}', NULL, '2')"));
        assertEquals("NULL", one("SELECT jsonb_insert('{\"a\":1}', NULL, '2')"));
        assertEquals("NULL", one("SELECT json_typeof(NULL::json)"));
    }

    /** A key, though, cannot be null: there is no such member to write. */
    @Test
    void aKeyCannotBeNull() {
        assertEquals("22004", stateOf("SELECT jsonb_object(ARRAY[NULL, 'b'])"));
    }

    @Test
    void theTwoArgumentObjectFormPairsKeysWithValues() throws Exception {
        assertEquals("{\"a\": \"1\", \"b\": \"2\"}",
                one("SELECT jsonb_object(ARRAY['a','b'], ARRAY['1','2'])"));
    }

    /** jsonb prints the document back, which is not the text it was given. */
    @Test
    void jsonbPrintsTheDocumentAndJsonPrintsTheText() throws Exception {
        assertEquals("{\"a\": 1}", one("SELECT ('{\"a\":1}'::jsonb)::text"));
        assertEquals("{\"a\":1}", one("SELECT ('{\"a\":1}'::json)::text"));
    }
}
