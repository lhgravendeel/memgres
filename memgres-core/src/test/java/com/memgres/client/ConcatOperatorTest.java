package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which concatenation a {@code ||} means.
 *
 * <p>PostgreSQL declares eleven of them and no more. Two take a text on one side and anything that
 * is not an array on the other, which is what makes {@code 'x' || 42} a string and {@code 42 || 1}
 * nothing at all. memgres resolved nothing: it read the two values, decided from their shapes
 * whether they looked like arrays or JSON, and ran them together as strings otherwise. Over the
 * 1600 pairs of operand types this test's types can make, it disagreed with PostgreSQL about 1193
 * — answering for 772 pairs there is no operator for, refusing 28 there is, choosing for itself
 * where PostgreSQL says it cannot choose, and printing the values it did concatenate as Java
 * writes them rather than as their own type does.
 */
class ConcatOperatorTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("SET TimeZone = 'UTC'");
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

    private static void noOperator(String signature, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), "should be refused: " + sql);
        assertEquals("42883", e.getSQLState(), sql);
        assertEquals("ERROR: operator does not exist: " + signature,
                e.getMessage() == null ? null : e.getMessage().split("\n")[0], sql);
    }

    private static void notUnique(String signature, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), "should be refused: " + sql);
        assertEquals("42725", e.getSQLState(), sql);
        assertEquals("ERROR: operator is not unique: " + signature,
                e.getMessage() == null ? null : e.getMessage().split("\n")[0], sql);
    }

    // ------------------------------------------------------- a pair with no operator

    /**
     * Neither operand is a text, so neither of the two concatenations that take one applies —
     * however readily two values can be written one after the other.
     */
    @Test
    void aPairWithNeitherSideTextHasNoOperator() {
        noOperator("integer || integer", "SELECT 1 || 2");
        noOperator("integer || date", "SELECT 1 || '2020-01-01'::date");
        noOperator("uuid || inet", "SELECT '00000000-0000-0000-0000-000000000001'::uuid"
                + " || '10.0.0.1'::inet");
        noOperator("boolean || numeric", "SELECT true || 1.5");
        noOperator("timestamp without time zone || interval",
                "SELECT '2020-01-01'::timestamp || '1 day'::interval");
        noOperator("smallint || bit", "SELECT 1::smallint || B'101'");
        noOperator("integer || text[]", "SELECT 1 || ARRAY['a','b']::text[]");
    }

    /** An array takes an element of its own kind and nothing else. */
    @Test
    void anArrayTakesAnElementOfItsOwnKind() throws Exception {
        assertEquals("{1,2,3}", scalar("SELECT (ARRAY[1,2] || 3)::text"));
        assertEquals("{3,1,2}", scalar("SELECT (3 || ARRAY[1,2])::text"));
        assertEquals("{1,2,3}", scalar("SELECT (ARRAY[1,2] || ARRAY[3])::text"));
        assertEquals("{a,a,b}", scalar("SELECT ('a'::text || ARRAY['a','b'])::text"));
        noOperator("text || integer[]", "SELECT 'a'::text || ARRAY[1,2]");
        noOperator("\"char\" || text[]", "SELECT 'a'::\"char\" || ARRAY['a','b']");
    }

    // ------------------------------------------------------- a pair with more than one

    /**
     * {@code "char"} is the single byte PostgreSQL keeps for its own catalogs, and it puts it in
     * the internal category rather than among the string types. Nothing there holds the preferred
     * type of a category that has none, so the choice between {@code text || text} and
     * {@code text || anynonarray} is one PostgreSQL will not make.
     */
    @Test
    void aCharBesideAStringIsAChoicePostgresWillNotMake() {
        notUnique("text || \"char\"", "SELECT 'a'::text || 'a'::\"char\"");
        notUnique("\"char\" || text", "SELECT 'a'::\"char\" || 'a'::text");
        notUnique("\"char\" || \"char\"", "SELECT 'a'::\"char\" || 'a'::\"char\"");
        notUnique("character varying || \"char\"", "SELECT 'a'::varchar || 'a'::\"char\"");
        notUnique("name || \"char\"", "SELECT 'a'::name || 'a'::\"char\"");
        notUnique("unknown || \"char\"", "SELECT 'a' || 'a'::\"char\"");
        notUnique("\"char\" || unknown", "SELECT 'a'::\"char\" || 'a'");
    }

    /** Beside anything that is not a string there is only one candidate, and it runs. */
    @Test
    void aCharBesideAnythingElseIsOneOperator() throws Exception {
        assertEquals("a1", scalar("SELECT ('a'::\"char\" || 1)::text"));
        assertEquals("a2020-01-01", scalar("SELECT ('a'::\"char\" || '2020-01-01'::date)::text"));
        assertEquals("1a", scalar("SELECT (1 || 'a'::\"char\")::text"));
    }

    /** The type the catalogs are written in is a type memgres has, and names as PostgreSQL does. */
    @Test
    void theCatalogsCharIsTheSameType() throws Exception {
        assertEquals("\"char\"", scalar("SELECT pg_typeof(provolatile)::text FROM pg_proc LIMIT 1"));
        assertEquals("\"char\"", scalar("SELECT pg_typeof('a'::\"char\")::text"));
        // It is one byte: a longer string keeps its first character, a number is that character.
        assertEquals("a", scalar("SELECT ('abc'::\"char\")::text"));
        assertEquals("A", scalar("SELECT (65::\"char\")::text"));
        // And char without the quotes is still the blank-padded string type.
        assertEquals("character", scalar("SELECT pg_typeof('a'::char(3))::text"));
    }

    // ------------------------------------------------------- what the result is

    /**
     * A concatenation that takes a text reads the other operand as text too, with that type's own
     * output function. Running the stored values together printed whatever Java made of them.
     */
    @Test
    void theOtherOperandIsReadAsItsOwnTypePrintsIt() throws Exception {
        assertEquals("a2020-01-01 00:00:00", scalar("SELECT 'a' || '2020-01-01'::timestamp"));
        assertEquals("a2020-01-01 00:00:00+00", scalar("SELECT 'a' || '2020-01-01'::timestamptz"));
        assertEquals("a1", scalar("SELECT 'a' || 1::real"));
        assertEquals("a1", scalar("SELECT 'a' || 1::float8"));
        assertEquals("a10.0.0.1/32", scalar("SELECT 'a' || '10.0.0.1'::inet"));
        assertEquals("a\\x01", scalar("SELECT 'a'::text || '\\x01'::bytea"));
        // Written with no type of its own the left operand is the bytea's input, not a text.
        assertEquals("\\x6101", scalar("SELECT ('a' || '\\x01'::bytea)::text"));
        assertEquals("a1 day", scalar("SELECT 'a' || '1 day'::interval"));
        assertEquals("a{}", scalar("SELECT 'a' || '{}'::json"));
        assertEquals("a[1,5)", scalar("SELECT 'a' || int4range(1,5)"));
    }

    /**
     * A blank-padded string loses its padding when it is read as a text, which is what makes
     * {@code 'a'::char(3) || 'b'} the two characters PostgreSQL answers with.
     */
    @Test
    void aBlankPaddedStringLosesItsPadding() throws Exception {
        assertEquals("ab", scalar("SELECT 'a'::char(3) || 'b'"));
        assertEquals("ba", scalar("SELECT 'b' || 'a'::char(3)"));
        assertEquals("a1", scalar("SELECT 'a'::char(3) || 1"));
        // An element joined onto an array is read as the array's element type, and the array takes
        // that type from its left operand.
        assertEquals("{a,b,a}", scalar("SELECT (ARRAY['a','b'] || 'a'::char(3))::text"));
    }

    /** A concatenation declared over one type reads an untyped operand as that type. */
    @Test
    void anUntypedOperandIsReadAsTheOperatorsType() throws Exception {
        assertEquals("1011", scalar("SELECT ('101'::varbit || '1')::text"));
        assertEquals("\\x0161", scalar("SELECT ('\\x01'::bytea || 'a')::text"));
        assertEquals("'a' 'b'", scalar("SELECT ('a'::tsvector || 'b')::text"));
        // And an operand that type cannot read is not an operand it takes.
        SQLException e = assertThrows(SQLException.class,
                () -> scalar("SELECT ('{}'::jsonb || 'a')::text"));
        assertEquals("22P02", e.getSQLState());
    }

    // ------------------------------------------------------- what must keep working

    /** The concatenations a statement is actually written with. */
    @Test
    void theOrdinaryConcatenationsStillRun() throws Exception {
        assertEquals("ab", scalar("SELECT 'a' || 'b'"));
        assertEquals("ab", scalar("SELECT 'a'::text || 'b'::text"));
        assertEquals("a1", scalar("SELECT 'a' || 1"));
        assertEquals("1a", scalar("SELECT 1 || 'a'"));
        assertEquals("atrue", scalar("SELECT 'a' || true"));
        assertNull(scalar("SELECT 'a' || NULL"));
        assertNull(scalar("SELECT NULL || NULL"));
        assertEquals("{1,2}", scalar("SELECT (NULL || ARRAY[1,2])::text"));
        assertEquals("{\"a\": 1, \"b\": 2}", scalar("SELECT ('{\"a\":1}'::jsonb || '{\"b\":2}'::jsonb)::text"));
        assertEquals("\\x0102", scalar("SELECT ('\\x01'::bytea || '\\x02'::bytea)::text"));
        assertEquals("101101", scalar("SELECT (B'101' || B'101')::text"));
        assertEquals("'a' 'b'", scalar("SELECT ('a'::tsvector || 'b'::tsvector)::text"));
        assertEquals("abc", scalar("SELECT 'a' || 'b' || 'c'"));
    }

    /** A type memgres declares of its own is not judged against PostgreSQL's operators. */
    @Test
    void aTypeOfMemgresOwnIsLeftAlone() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE EXTENSION IF NOT EXISTS hstore");
            st.execute("DROP TABLE IF EXISTS cc_h");
            st.execute("CREATE TABLE cc_h (a hstore, b hstore)");
            st.execute("INSERT INTO cc_h VALUES ('x=>1'::hstore, 'y=>2'::hstore)");
        }
        assertEquals("\"x\"=>\"1\", \"y\"=>\"2\"", scalar("SELECT (a || b)::text FROM cc_h"));
    }
}
