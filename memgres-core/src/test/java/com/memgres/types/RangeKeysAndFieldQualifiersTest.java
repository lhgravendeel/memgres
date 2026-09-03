package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * When two values are one value, and what a type's modifier may say.
 *
 * <p>A range is its two bounds and which of them it includes, and each bound is a value of the
 * subtype — so two bounds the subtype calls equal make one range however either was written.
 * Grouped by the text they print as, {@code [1,2)} and {@code [1.0,2.0)} were two values where
 * PostgreSQL has one.
 *
 * <p>An interval's field qualifier is a pair running from a wider field to a narrower one, and the
 * grammar has only seven such pairs: what it does not have is a statement that will not parse
 * rather than a type nobody declared. A precision stands in front of the literal or behind a field
 * name and nowhere else, and where the grammar reads a plain number a sign cannot stand at all.
 */
class RangeKeysAndFieldQualifiersTest {

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

    /** Two spellings of one range are one range, however the rows are gathered. */
    @Test
    void twoSpellingsOfOneRangeAreOneValue() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zrk (r numrange)");
            s.execute("INSERT INTO zrk VALUES ('[1,2)'), ('[1.0,2.0)'), ('[1.00,2.00)')");
        }
        assertEquals("1", one("SELECT count(DISTINCT r)::text FROM zrk"));
        assertEquals("1", one("SELECT count(*)::text FROM (SELECT r FROM zrk GROUP BY r) g"));
        assertEquals("1", one("SELECT count(*)::text FROM (SELECT DISTINCT r FROM zrk) g"));
        assertEquals("1", one("SELECT count(DISTINCT r)::text FROM"
                + " (VALUES ('[1,2)'::numrange), ('[1.0,2.0)'::numrange)) v(r)"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE zrk");
        }
    }

    /** The grammar has seven pairs of interval fields and no others. */
    @Test
    void whichPairsOfIntervalFieldsThereAre() throws SQLException {
        assertEquals("5 mons", one("SELECT (interval '5' year to month)::text"));
        assertEquals("05:00:00", one("SELECT (interval '5' day to hour)::text"));
        assertEquals("00:00:05", one("SELECT (interval '5' minute to second)::text"));
        // A pair the grammar does not have stops where it stops being one.
        assertTrue(messageOf("SELECT interval '5' hour to hour")
                .contains("syntax error at or near \"hour\""));
        assertTrue(messageOf("SELECT interval '5' year to day")
                .contains("syntax error at or near \"day\""));
        // A field that can begin no pair at all stops at the TO.
        assertTrue(messageOf("SELECT interval '5' second to minute")
                .contains("syntax error at or near \"to\""));
        assertTrue(messageOf("SELECT interval '5' month to year")
                .contains("syntax error at or near \"to\""));
    }

    /** A precision stands in front of the literal or behind a field, and nowhere else. */
    @Test
    void whereAnIntervalPrecisionMayStand() throws SQLException {
        assertEquals("00:00:05", one("SELECT (interval (2) '5')::text"));
        assertEquals("00:00:05", one("SELECT (interval '5' second (2))::text"));
        assertEquals("00:00:05", one("SELECT ('5'::interval(2))::text"));
        assertTrue(messageOf("SELECT interval '5' (2)")
                .contains("syntax error at or near \"(\""));
    }

    /** A sign can stand only where the modifier is read as a value rather than as a number. */
    @Test
    void whereASignMayStandInAModifier() {
        assertEquals("22023", stateOf("SELECT '1'::numeric(-1)"));
        assertEquals("22023", stateOf("SELECT '1'::bit(-1)"));
        for (String written : new String[]{"varchar(-1)", "char(-1)", "interval(-1)",
                "timestamp(-1)"}) {
            assertTrue(messageOf("SELECT '1'::" + written)
                    .contains("syntax error at or near \"-\""), written);
        }
    }

    /** A polygon is made of corners, and two is the fewest a shape can have. */
    @Test
    void aPolygonNeedsAtLeastTwoCorners() throws SQLException {
        for (String n : new String[]{"0", "1", "-3"}) {
            assertTrue(messageOf("SELECT polygon(" + n + ", '<(0,0),1>'::circle)")
                    .contains("must request at least 2 points"), n);
        }
        assertEquals("((-1,0),(1,1.2246467991473532e-16))",
                one("SELECT (polygon(2, '<(0,0),1>'::circle))::text"));
    }
}
