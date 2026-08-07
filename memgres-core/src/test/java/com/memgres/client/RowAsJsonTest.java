package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A row written out as JSON.
 *
 * <p>{@code row_to_json}, {@code to_json}, {@code to_jsonb} and the aggregates built on them turn
 * a row into a JSON object whose members are the row's fields. memgres wrote the text a composite
 * prints as and then quoted it, so a client asking for a row was handed one string with every
 * field run together inside it — {@code "(1,ab,...)"} where an object of named members was due.
 *
 * <p>The same reading settles what a field is worth once it is in there: an array is a JSON array
 * and not the braces it prints as, a composite field is an object of its own declared names, a
 * bytea is its hex text, and a string of braces that is only text stays quoted text.
 */
class RowAsJsonTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TYPE rj_comp AS (a int, b text)");
            st.execute("CREATE TABLE rj_t (id int, name text, c char(5), arr int[], "
                    + "j json, by bytea, s text)");
            st.execute("INSERT INTO rj_t VALUES (1, 'ab', 'ab', ARRAY[1,2], '{\"k\":1}', "
                    + "'\\x0102', '{1,2}')");
            st.execute("CREATE TABLE rj_b (tags text[], cs rj_comp[], v rj_comp)");
            st.execute("INSERT INTO rj_b VALUES (ARRAY['a','b c'], ARRAY[ROW(1,'x')::rj_comp], "
                    + "ROW(7,'y')::rj_comp)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    // ------------------------------------------------------------- a row is an object

    /** An anonymous row names its fields f1, f2 and so on. */
    @Test
    void anAnonymousRowNamesItsFieldsInOrder() throws Exception {
        assertEquals("{\"f1\":1}", scalar("SELECT row_to_json(row(1))::text"));
        assertEquals("{\"f1\":1,\"f2\":\"a\"}", scalar("SELECT row_to_json(row(1, 'a'))::text"));
        assertEquals("{\"f1\":1,\"f2\":null}", scalar("SELECT row_to_json(row(1, NULL))::text"));
        assertEquals("{\"f1\":1,\"f2\":{\"f1\":2,\"f2\":\"b\"}}",
                scalar("SELECT row_to_json(row(1, row(2, 'b')))::text"));
    }

    /** A row of a relation carries the names its columns were declared with. */
    @Test
    void aRowOfARelationCarriesItsColumnNames() throws Exception {
        String expected = "{\"id\":1,\"name\":\"ab\",\"c\":\"ab   \",\"arr\":[1,2],"
                + "\"j\":{\"k\":1},\"by\":\"\\\\x0102\",\"s\":\"{1,2}\"}";
        assertEquals(expected, scalar("SELECT row_to_json(t)::text FROM rj_t t"));
        assertEquals(expected, scalar("SELECT row_to_json(t.*)::text FROM rj_t t"));
        assertEquals(expected, scalar("SELECT to_json(t)::text FROM rj_t t"));
        assertEquals("{\"a\":1,\"b\":\"b\"}",
                scalar("SELECT row_to_json(x)::text FROM (SELECT 1 AS a, 'b' AS b) x"));
        assertEquals("{\"n\":1,\"t\":\"a\"}",
                scalar("SELECT row_to_json(v)::text FROM (VALUES (1, 'a')) v(n, t)"));
    }

    /** A composite gives its fields the names it was declared with. */
    @Test
    void aCompositeGivesItsFieldsItsOwnNames() throws Exception {
        assertEquals("{\"a\":1,\"b\":\"a\"}",
                scalar("SELECT row_to_json(ROW(1, 'a')::rj_comp)::text"));
        assertEquals("{\"a\":7,\"b\":\"y\"}", scalar("SELECT row_to_json(v)::text FROM rj_b"));
        assertEquals("{\"a\": 7, \"b\": \"y\"}", scalar("SELECT to_jsonb(v)::text FROM rj_b"));
    }

    /** The row itself is still the text a composite prints as. */
    @Test
    void theRowItselfIsStillItsOwnText() throws Exception {
        assertEquals("(1,a)", scalar("SELECT (row(1, 'a'))::text"));
    }

    // ------------------------------------------------------- what a field is worth

    /** An array is a JSON array, whatever it prints as. */
    @Test
    void anArrayIsAnArray() throws Exception {
        assertEquals("{\"tags\":[\"a\",\"b c\"],\"cs\":[{\"a\":1,\"b\":\"x\"}],"
                        + "\"v\":{\"a\":7,\"b\":\"y\"}}",
                scalar("SELECT row_to_json(b)::text FROM rj_b b"));
        assertEquals("[\"a\",\"b c\"]", scalar("SELECT to_json(tags)::text FROM rj_b"));
        assertEquals("[{\"a\":1,\"b\":\"x\"}]", scalar("SELECT to_json(cs)::text FROM rj_b"));
        assertEquals("[1,2]", scalar("SELECT to_json(arr)::text FROM rj_t"));
    }

    /** Text of braces is text; only json is written unquoted. */
    @Test
    void textOfBracesIsStillText() throws Exception {
        assertEquals("\"{1,2}\"", scalar("SELECT to_json(s)::text FROM rj_t"));
        assertEquals("{\"f1\":\"{1,2}\"}", scalar("SELECT row_to_json(row('{1,2}'::text))::text"));
        assertEquals("{\"k\":1}", scalar("SELECT to_json(j)::text FROM rj_t"));
    }

    /** A bytea is the hex text it prints as, not the name of an array object. */
    @Test
    void aByteaIsItsHexText() throws Exception {
        assertEquals("\"\\\\x0102\"", scalar("SELECT to_json('\\x0102'::bytea)::text"));
    }

    // ------------------------------------------------------ pretty, ordered and strict

    /** The line breaks between one field and the next and nowhere else. */
    @Test
    void prettyBreaksBetweenFieldsOnly() throws Exception {
        assertEquals("{\"f1\":1}", scalar("SELECT row_to_json(row(1), true)::text"));
        assertEquals("{\"f1\":1,\n \"f2\":\"a\"}",
                scalar("SELECT row_to_json(row(1, 'a'), true)::text"));
        assertEquals("{\"f1\":1,\"f2\":\"a\"}",
                scalar("SELECT row_to_json(row(1, 'a'), false)::text"));
    }

    /** jsonb orders its keys and keeps the last of any repeated one. */
    @Test
    void jsonbOrdersItsKeys() throws Exception {
        assertEquals("{\"f1\": 1, \"f2\": \"a\"}", scalar("SELECT to_jsonb(row(1, 'a'))::text"));
        assertEquals("{\"a\": 2, \"bb\": 1}",
                scalar("SELECT jsonb_build_object('bb', 1, 'a', 2)::text"));
        assertEquals("{\"a\": 2, \"bb\": 3}",
                scalar("SELECT jsonb_build_object('bb', 1, 'a', 2, 'bb', 3)::text"));
        assertEquals("{\"bb\" : 1, \"a\" : 2, \"bb\" : 3}",
                scalar("SELECT json_build_object('bb', 1, 'a', 2, 'bb', 3)::text"));
    }

    /** Nothing in, nothing out: the whole family is strict. */
    @Test
    void nothingInNothingOut() throws Exception {
        assertNull(scalar("SELECT to_json(NULL::int)::text"));
        assertNull(scalar("SELECT to_jsonb(NULL::int)::text"));
        assertNull(scalar("SELECT row_to_json(NULL::rj_comp)::text"));
    }

    // ------------------------------------------------------ the aggregates built on them

    /** An element with a shape of its own starts a new line; a plain one does not. */
    @Test
    void anAggregatedRowIsTheObjectItIs() throws Exception {
        assertEquals("[{\"a\":1}, \n {\"a\":2}]",
                scalar("SELECT json_agg(x)::text FROM (SELECT 1 AS a UNION ALL SELECT 2) x"));
        assertEquals("[1, 2]",
                scalar("SELECT json_agg(a)::text FROM (SELECT 1 AS a UNION ALL SELECT 2) y"));
        assertEquals("[{\"f1\":1,\"f2\":\"a\"}]", scalar("SELECT json_agg(row(1, 'a'))::text"));
        assertEquals("[[\"a\",\"b c\"]]", scalar("SELECT json_agg(tags)::text FROM rj_b"));
    }

    /** The object aggregates take a row for a value just as readily. */
    @Test
    void anAggregatedObjectTakesARowForAValue() throws Exception {
        assertEquals("{ \"7\" : {\"a\":7,\"b\":\"y\"} }",
                scalar("SELECT json_object_agg((v).a, v)::text FROM rj_b"));
        assertEquals("{\"7\": {\"a\": 7, \"b\": \"y\"}}",
                scalar("SELECT jsonb_object_agg((v).a, v)::text FROM rj_b"));
    }

    /** Both families fill the same record from the same base argument. */
    @Test
    void bothFamiliesFillTheSameRecord() throws Exception {
        for (String fn : new String[]{"json_populate_record", "jsonb_populate_record"}) {
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT (" + fn
                         + "(NULL::rj_comp, '{\"a\":1,\"b\":\"x\"}')).*")) {
                assertTrue(rs.next(), fn + " must expand to the record's own columns");
                assertEquals(1, rs.getInt(1), fn);
                assertEquals("x", rs.getString(2), fn);
            }
        }
    }
}
