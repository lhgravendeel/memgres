package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an array is an array of.
 *
 * <p>An array takes its type from its elements, and each element keeps its own — which is why an
 * array of {@code character(5)} holds values padded to five and prints them that way. memgres read
 * only the composite and the enum element types, so every other written one answered {@code text[]}
 * and the elements were read back as texts: an array of character was not one of character.
 *
 * <p>Two things followed from that and are fixed here beside it. Subscripting an array the
 * statement built answered {@code jsonb} whatever the array was of, since only a column of one was
 * read. And {@code array_to_string} trimmed every element, dropping the spaces a text element
 * really has as readily as the blanks a bpchar element is declared with.
 */
class ArrayElementTypeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ae_t (c char(5), a char(5)[], ta text[])");
            st.execute("INSERT INTO ae_t VALUES ('ab', ARRAY['ab'::char(5)], ARRAY['a  '])");
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

    // ------------------------------------------------------- what it is an array of

    /** The elements say what the array is, whatever type they were written with. */
    @Test
    void theElementsSayWhatTheArrayIs() throws Exception {
        assertEquals("character[]", scalar("SELECT pg_typeof(ARRAY['ab'::char(5)])::text"));
        assertEquals("character varying[]", scalar("SELECT pg_typeof(ARRAY['a'::varchar])::text"));
        assertEquals("text[]", scalar("SELECT pg_typeof(ARRAY['a'::text])::text"));
        assertEquals("integer[]", scalar("SELECT pg_typeof(ARRAY[1])::text"));
        assertEquals("character[]", scalar("SELECT pg_typeof(ARRAY[c])::text FROM ae_t"));
    }

    /** Elements that disagree are left to the widening their values show. */
    @Test
    void elementsThatDisagreeAreLeftToTheirValues() throws Exception {
        assertEquals("bigint[]", scalar("SELECT pg_typeof(ARRAY[1::smallint, 2::bigint])::text"));
        assertEquals("integer[]", scalar("SELECT pg_typeof(ARRAY[1,2,3])::text"));
    }

    // ------------------------------------------------------- what an element keeps

    /** Each element keeps the width its own type was declared with. */
    @Test
    void anElementKeepsItsOwnWidth() throws Exception {
        assertEquals("{\"ab   \"}", scalar("SELECT (ARRAY['ab'::char(5)])::text"));
        assertEquals("{\"c    \"}", scalar("SELECT ('{c}'::char(5)[])::text"));
        assertEquals("{\"ab   \",\"c    \"}",
                scalar("SELECT (ARRAY['ab'::char(5)] || 'c'::char(5))::text"));
        assertEquals("{\"ab   \",\"c    \"}",
                scalar("SELECT (ARRAY['ab'::char(5)] || ARRAY['c'::char(5)])::text"));
    }

    /** An array is written with its braces, whatever its elements are. */
    @Test
    void anArrayIsWrittenWithItsBraces() {
        for (String sql : new String[]{"SELECT 'c'::char(5)[]", "SELECT 'c'::text[]",
                "SELECT 'c'::varchar[]", "SELECT 'c'::int[]", "SELECT 'c'::name[]"}) {
            SQLException e = assertThrows(SQLException.class, () -> scalar(sql), sql);
            assertEquals("22P02", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("malformed array literal"), sql);
        }
    }

    // ------------------------------------------------------- reading one back out

    /** A subscript gives one element, which is of the array's element type. */
    @Test
    void aSubscriptIsOfTheElementType() throws Exception {
        assertEquals("text", scalar("SELECT pg_typeof((ARRAY['a'::text])[1])::text"));
        assertEquals("integer", scalar("SELECT pg_typeof((ARRAY[1])[1])::text"));
        assertEquals("character", scalar("SELECT pg_typeof((ARRAY['ab'::char(5)])[1])::text"));
        assertEquals("character", scalar("SELECT pg_typeof(a[1])::text FROM ae_t"));
        // And read as a text it drops the blanks its own type was padded to.
        assertEquals("ab", scalar("SELECT (ARRAY['ab'::char(5)])[1]::text"));
        assertEquals("ab", scalar("SELECT a[1]::text FROM ae_t"));
    }

    /** array_to_string writes each element as it is held, spaces and all. */
    @Test
    void arrayToStringWritesEachElementAsItIsHeld() throws Exception {
        assertEquals("a  ", scalar("SELECT array_to_string(ARRAY['a  '::text], ',')"));
        assertEquals("  a  ", scalar("SELECT array_to_string(ARRAY['  a  '::text], ',')"));
        assertEquals("a  |b", scalar("SELECT array_to_string(ARRAY['a  '::text, 'b'], '|')"));
        assertEquals("a  ", scalar("SELECT array_to_string(ta, ',') FROM ae_t"));
        assertEquals("ab   ", scalar("SELECT array_to_string(ARRAY['ab'::char(5)], ',')"));
        assertEquals("ab   ", scalar("SELECT array_to_string(a, ',') FROM ae_t"));
        // What had nothing to trim is unchanged.
        assertEquals("1,2", scalar("SELECT array_to_string(ARRAY[1,2], ',')"));
        assertEquals("a,b", scalar("SELECT array_to_string(ARRAY['a','b'], ',')"));
    }
}
