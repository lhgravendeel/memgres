package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Storing a value, and reading one type's value as another's.
 *
 * <p>Storing is an assignment, not a reading of text. PostgreSQL looks for a cast registered
 * implicit or assignment from what the expression produces to what the column holds, and refuses
 * the statement where there is none — so an integer does not become a date and a boolean does not
 * become an integer, however readable one's text would be as the other. A bare quoted literal is
 * the exception: it has no type yet and is read with the column's own reader.
 *
 * <p>Converting between two shapes is structural rather than a text round trip. A box read as a
 * point is its centre and as a polygon its four corners; a length of time read as a time of day is
 * the clock it reaches, with the days dropped and the hours wrapped.
 */
class StoringAndConvertingValuesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zsc (i int, d date, b boolean, t text, n numeric, c char(3),"
                    + " v varchar(3))");
        }
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

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** A value whose type has no cast to the column's is refused, whatever its text looks like. */
    @Test
    void aValueReachesAColumnByACastOrNotAtAll() {
        assertEquals("42804", stateOf("INSERT INTO zsc (i) VALUES (true)"));
        assertEquals("42804", stateOf("INSERT INTO zsc (d) VALUES (20200101)"));
        assertEquals("42804", stateOf("INSERT INTO zsc (b) VALUES (0.5)"));
        assertEquals("42804", stateOf("INSERT INTO zsc (i) VALUES ('1'::varchar)"));
        assertEquals("42804", stateOf("INSERT INTO zsc (i) SELECT '1'::text"));
        assertEquals("42804", stateOf("UPDATE zsc SET i = true"));
        assertEquals("42804", stateOf("UPDATE zsc SET i = '1'::text"));
        assertTrue(messageOf("INSERT INTO zsc (i) VALUES (true)")
                .contains("column \"i\" is of type integer but expression is of type boolean"));
    }

    /** A bare literal has no type yet, and the column's own reader is what reads it. */
    @Test
    void aBareLiteralIsReadByTheColumn() {
        assertNull(stateOf("INSERT INTO zsc (i) VALUES ('1')"));
        assertNull(stateOf("INSERT INTO zsc (d) VALUES ('2020-01-01')"));
        assertNull(stateOf("INSERT INTO zsc (b) VALUES ('t')"));
        assertNull(stateOf("INSERT INTO zsc (n) SELECT 'Infinity'"));
        // And the casts that are registered still work: a number rounds into an integer, and
        // anything at all has a text form.
        assertNull(stateOf("INSERT INTO zsc (i) VALUES (1.5)"));
        assertNull(stateOf("INSERT INTO zsc (i) VALUES (1::bigint)"));
        assertNull(stateOf("INSERT INTO zsc (t) VALUES (1)"));
        assertNull(stateOf("INSERT INTO zsc (d) VALUES (now())"));
    }

    /** Trailing spaces are the one thing a character column drops rather than refuses. */
    @Test
    void spacesPastTheWidthAreDroppedRatherThanRefused() {
        assertNull(stateOf("INSERT INTO zsc (c) VALUES ('abc   ')"));
        assertNull(stateOf("INSERT INTO zsc (v) VALUES ('abc   ')"));
        assertEquals("22001", stateOf("INSERT INTO zsc (c) VALUES ('abcd ')"));
        assertEquals("22001", stateOf("INSERT INTO zsc (v) VALUES ('abcd ')"));
    }

    /** A character type written with no width is one character wide. */
    @Test
    void aCharacterWithNoWidthIsOneWide() throws SQLException {
        assertEquals("a", one("SELECT 'abcdef'::char"));
        assertEquals("a", one("SELECT 'abcdef'::character"));
        assertEquals("1", one("SELECT 1234::char"));
        assertEquals(" ", one("SELECT ''::char"));
        // bpchar is the catalogue's own name for the type and carries no width of its own.
        assertEquals("abcdef", one("SELECT 'abcdef'::bpchar"));
        assertEquals("abc", one("SELECT 'abcdef'::char(3)"));
    }

    /** Between two shapes the conversion is structural, not a writing out and reading back. */
    @Test
    void oneShapeReadAsAnother() throws SQLException {
        assertEquals("(1,2)", one("SELECT (box '(0,0),(2,4)'::point)::text"));
        assertEquals("((0,0),(0,4),(2,4),(2,0))",
                one("SELECT (box '(0,0),(2,4)'::polygon)::text"));
        assertEquals("<(1,2),2.23606797749979>",
                one("SELECT (box '(0,0),(2,4)'::circle)::text"));
        assertEquals("(1,2)", one("SELECT (circle '<(1,2),3>'::point)::text"));
        assertEquals("(1,2)", one("SELECT (lseg '[(0,0),(2,4)]'::point)::text"));
        assertEquals("(1.6666666666666667,1.6666666666666667)",
                one("SELECT (polygon '((0,0),(2,4),(3,1))'::point)::text"));
        assertEquals("(3,4),(0,0)", one("SELECT (polygon '((0,0),(2,4),(3,1))'::box)::text"));
        assertEquals("(1,2),(1,2)", one("SELECT (point '(1,2)'::box)::text"));
        // A bare literal is not a shape yet: it is read by the reader of the type it is cast to,
        // which is what keeps '(1,2),(3,4),(5,6)' from being a box.
        assertEquals("22P02", stateOf("SELECT '(1,2),(3,4),(5,6)'::box"));
    }

    /** A length of time read as a time of day is the clock it reaches. */
    @Test
    void aLengthOfTimeReadAsATimeOfDay() throws SQLException {
        assertEquals("02:03:04", one("SELECT (interval '1 day 02:03:04'::time)::text"));
        assertEquals("01:03:04", one("SELECT (interval '25:03:04'::time)::text"));
        assertEquals("23:00:00", one("SELECT (interval '-1:00:00'::time)::text"));
        assertEquals("00:00:00", one("SELECT (interval '2 days'::time)::text"));
        assertEquals("04:00:00", one("SELECT (interval '100:00:00'::time)::text"));
    }

    /** A moment read as a time of day keeps the zone and drops the date. */
    @Test
    void aMomentReadAsATimeOfDay() throws SQLException {
        assertEquals("20:38:40.5+00",
                one("SELECT (timestamptz '2001-02-16 20:38:40.5+00'::timetz)::text"));
        assertEquals("15:38:40.5+00",
                one("SELECT (timestamptz '2001-02-16 20:38:40.5+05'::timetz)::text"));
    }
}
