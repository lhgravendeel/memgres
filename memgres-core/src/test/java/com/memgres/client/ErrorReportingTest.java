package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A failure inside the engine must reach the client as the SQLSTATE PostgreSQL would use, not as
 * XX000 carrying a Java type name — XX000 tells a client the database is broken rather than that
 * the statement was wrong. These cover the cases that previously surfaced a raw Java exception,
 * plus the strict-NULL argument handling that several of them came from.
 */
class ErrorReportingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
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

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** Assert the statement fails, and with the given SQLSTATE rather than an internal error. */
    private static void assertState(String expectedState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), sql);
        assertEquals(expectedState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
    }

    /** Assert the statement does not fail with an internal-error SQLSTATE. */
    private static void assertNotInternal(String sql) {
        try {
            scalar(sql);
        } catch (SQLException e) {
            assertNotEquals("XX000", e.getSQLState(),
                    "surfaced as an internal error: " + sql + " -> " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------- frames

    @Test
    void rangeFrameOverNumericColumn() throws Exception {
        exec("DROP TABLE IF EXISTS ert_num CASCADE");
        exec("CREATE TABLE ert_num (n numeric)");
        exec("INSERT INTO ert_num VALUES (1.5), (2.5)");
        assertEquals("4.0", scalar(
                "SELECT sum(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)::text"
                        + " FROM ert_num ORDER BY n LIMIT 1"));
        assertEquals("2", scalar(
                "SELECT count(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)::text"
                        + " FROM ert_num ORDER BY n LIMIT 1"));
        assertEquals("1.5", scalar(
                "SELECT first_value(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)::text"
                        + " FROM ert_num ORDER BY n LIMIT 1"));
        assertEquals("2.5", scalar(
                "SELECT last_value(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)::text"
                        + " FROM ert_num ORDER BY n LIMIT 1"));
        // a numeric offset, not only a numeric ordering column
        assertNotInternal("SELECT sum(n) OVER (ORDER BY n RANGE BETWEEN 0.5 PRECEDING"
                + " AND 0.5 FOLLOWING) FROM ert_num");
        // integer and float columns keep working
        exec("DROP TABLE IF EXISTS ert_int CASCADE");
        exec("CREATE TABLE ert_int (n int, f float8)");
        exec("INSERT INTO ert_int VALUES (1, 1.5), (2, 2.5)");
        assertEquals("3", scalar(
                "SELECT sum(n) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)::text"
                        + " FROM ert_int ORDER BY n LIMIT 1"));
        assertNotInternal("SELECT sum(f) OVER (ORDER BY f RANGE BETWEEN 1 PRECEDING"
                + " AND 1 FOLLOWING) FROM ert_int");
        exec("DROP TABLE IF EXISTS ert_int CASCADE");
        exec("DROP TABLE IF EXISTS ert_num CASCADE");
    }

    // ---------------------------------------------------------------- ranges

    @Test
    void rangeStrictlyLeftAndRight() throws Exception {
        assertEquals("true", scalar("SELECT ('[1,3)'::int4range << '[5,8)'::int4range)::text"));
        assertEquals("false", scalar("SELECT ('[1,3)'::int4range << '[2,8)'::int4range)::text"));
        assertEquals("true", scalar("SELECT ('[5,8)'::int4range >> '[1,3)'::int4range)::text"));
        assertEquals("false", scalar("SELECT ('[1,3)'::int4range >> '[5,8)'::int4range)::text"));
        // touching endpoints stay disjoint only when one side is exclusive
        assertEquals("true", scalar("SELECT ('[1,5)'::int4range << '[5,8)'::int4range)::text"));
        assertEquals("false", scalar("SELECT ('[1,5]'::int4range << '[5,8]'::int4range)::text"));
        // an empty range is strictly left of nothing
        assertEquals("false", scalar("SELECT ('empty'::int4range << '[5,8)'::int4range)::text"));
        assertEquals("false", scalar("SELECT ('[1,3)'::int4range << 'empty'::int4range)::text"));
        // only the facing bounds matter, so an unbounded far side is irrelevant
        assertEquals("true", scalar("SELECT ('(,3)'::int4range << '[5,8)'::int4range)::text"));
        assertEquals("true", scalar("SELECT ('[1,3)'::int4range << '[5,)'::int4range)::text"));
        // but an unbounded facing side reaches infinity and can never be to one side
        assertEquals("false", scalar("SELECT ('(,3)'::int4range << '(,8)'::int4range)::text"));
        assertEquals("false", scalar("SELECT ('[1,)'::int4range << '[5,8)'::int4range)::text"));
        // other range types
        assertEquals("true", scalar("SELECT ('[1,3)'::int8range << '[5,8)'::int8range)::text"));
        assertEquals("true", scalar("SELECT ('[1.5,3.5)'::numrange << '[5.5,8.5)'::numrange)::text"));
        assertEquals("true", scalar("SELECT (daterange('2020-01-01','2020-02-01')"
                + " << daterange('2020-03-01','2020-04-01'))::text"));
        // multiranges compare by their outermost parts
        assertEquals("true", scalar(
                "SELECT ('{[1,3)}'::int4multirange << '{[5,8)}'::int4multirange)::text"));
        assertEquals("true", scalar(
                "SELECT ('{[1,3),[4,6)}'::int4multirange << '{[8,9)}'::int4multirange)::text"));
        assertEquals("false", scalar(
                "SELECT ('{[1,3),[4,10)}'::int4multirange << '{[8,9)}'::int4multirange)::text"));
        assertEquals("false", scalar("SELECT ('{}'::int4multirange << '{[5,8)}'::int4multirange)::text"));
    }

    @Test
    void shiftOperatorsKeepTheirOtherMeanings() throws Exception {
        assertEquals("16", scalar("SELECT (1 << 4)::text"));
        assertEquals("16", scalar("SELECT (256 >> 4)::text"));
        assertEquals("0100", scalar("SELECT (B'1101' << 2)::text"));
        assertEquals("0011", scalar("SELECT (B'1101' >> 2)::text"));
        assertEquals("true", scalar("SELECT ('192.168.1.5'::inet << '192.168.1.0/24'::inet)::text"));
        assertEquals("true", scalar("SELECT ('192.168.1.0/24'::inet >> '192.168.1.5'::inet)::text"));
    }

    // ------------------------------------------------------------------- NaN

    @Test
    void notANumberThroughRoundingAndAggregation() throws Exception {
        assertEquals("NaN", scalar("SELECT round('NaN'::numeric, 2)::text"));
        assertEquals("NaN", scalar("SELECT round('NaN'::numeric)::text"));
        assertEquals("NaN", scalar("SELECT trunc('NaN'::numeric, 2)::text"));
        assertEquals("NaN", scalar("SELECT trunc('NaN'::numeric)::text"));
        exec("DROP TABLE IF EXISTS ert_nan CASCADE");
        exec("CREATE TABLE ert_nan (v numeric)");
        exec("INSERT INTO ert_nan VALUES ('NaN'), (1), (2)");
        assertEquals("NaN", scalar("SELECT sum(v)::text FROM ert_nan"));
        assertEquals("NaN", scalar("SELECT avg(v)::text FROM ert_nan"));
        assertEquals("NaN", scalar("SELECT sum(DISTINCT v)::text FROM ert_nan"));
        assertEquals("NaN", scalar("SELECT avg(DISTINCT v)::text FROM ert_nan"));
        // ordinary values are unaffected
        assertEquals("2.57", scalar("SELECT round(2.567::numeric, 2)::text"));
        assertEquals("2.56", scalar("SELECT trunc(2.567::numeric, 2)::text"));
        assertEquals("-3", scalar("SELECT round(-2.5::numeric)::text"));
        // NaN compares equal to itself for numeric, so this filter keeps every row
        assertEquals("NaN", scalar("SELECT sum(v)::text FROM ert_nan WHERE v = v"));
        assertEquals("3", scalar("SELECT sum(v)::text FROM ert_nan WHERE v <> 'NaN'"));
        exec("DROP TABLE IF EXISTS ert_nan CASCADE");
    }

    // -------------------------------------------------------------- interval

    @Test
    void intervalFieldsBeyondRange() {
        assertState("22015", "SELECT '100000000000 years'::interval");
        assertState("22015", "SELECT '100000000000000 days'::interval");
        assertState("22015", "SELECT '99999999999 months'::interval");
    }

    @Test
    void ordinaryIntervalsStillParse() throws Exception {
        assertEquals("1 year 2 mons 3 days", scalar("SELECT '1 year 2 months 3 days'::interval::text"));
        assertEquals("04:05:06", scalar("SELECT '04:05:06'::interval::text"));
        assertEquals("-1 years", scalar("SELECT '-1 year'::interval::text"));
    }

    // ----------------------------------------------------------------- depth

    /**
     * The depth at which reading gives out is a property of the stack it runs on, not of the
     * document, so the depth asked for here is far past any stack rather than near this one's.
     */
    @Test
    void nestingDeeperThanTheParserWillFollow() {
        assertState("54001",
                "SELECT (repeat('[', 100000) || repeat(']', 100000))::jsonb IS NOT NULL");
        assertState("54001",
                "SELECT (repeat('[', 100000) || repeat(']', 100000))::json IS NOT NULL");
    }

    @Test
    void moderateNestingIsAccepted() throws Exception {
        assertEquals("true",
                scalar("SELECT ((repeat('[', 1000) || repeat(']', 1000))::jsonb IS NOT NULL)::text"));
        assertEquals("true",
                scalar("SELECT ((repeat('[', 1000) || repeat(']', 1000))::json IS NOT NULL)::text"));
    }

    // -------------------------------------------------------------- OVERLAPS

    @Test
    void overlapsWithUnknownEndpoints() throws Exception {
        // one endpoint unknown, and the answer is undetermined
        assertNull(scalar("SELECT (DATE '2001-11-30', NULL::date)"
                + " OVERLAPS (DATE '2001-10-30', DATE '2001-11-01')"));
        assertNull(scalar("SELECT (NULL::date, DATE '2001-11-30')"
                + " OVERLAPS (DATE '2001-10-30', DATE '2001-11-01')"));
        assertNull(scalar("SELECT (DATE '2001-10-30', DATE '2001-11-01')"
                + " OVERLAPS (DATE '2001-11-30', NULL::date)"));
        // both endpoints of one period unknown
        assertNull(scalar("SELECT (NULL::date, NULL::date)"
                + " OVERLAPS (DATE '2001-10-30', DATE '2001-11-01')"));
        assertNull(scalar("SELECT (NULL::timestamp, NULL::timestamp)"
                + " OVERLAPS (NULL::timestamp, NULL::timestamp)"));
        // an unknown length
        assertNull(scalar("SELECT (DATE '2001-02-16', NULL::interval)"
                + " OVERLAPS (DATE '2001-02-16', DATE '2001-02-20')"));
        assertNull(scalar("SELECT (DATE '2001-02-16', INTERVAL '1 day')"
                + " OVERLAPS (DATE '2001-02-16', NULL::date)"));
        // an unknown end that cannot change the answer still gives a definite one
        assertEquals("true", scalar("SELECT ((DATE '2001-02-16', DATE '2001-02-20')"
                + " OVERLAPS (DATE '2001-02-17', NULL::date))::text"));
    }

    @Test
    void overlapsWithKnownEndpoints() throws Exception {
        assertEquals("true", scalar("SELECT ((DATE '2001-02-16', DATE '2001-12-21')"
                + " OVERLAPS (DATE '2001-10-30', DATE '2002-10-30'))::text"));
        assertEquals("false", scalar("SELECT ((DATE '2001-02-16', DATE '2001-12-21')"
                + " OVERLAPS (DATE '2002-10-30', DATE '2003-10-30'))::text"));
        // adjacent periods do not overlap
        assertEquals("false", scalar("SELECT ((DATE '2001-02-16', DATE '2001-02-20')"
                + " OVERLAPS (DATE '2001-02-20', DATE '2001-02-25'))::text"));
        // a reversed pair is normalised rather than rejected
        assertEquals("true", scalar("SELECT ((DATE '2001-12-21', DATE '2001-02-16')"
                + " OVERLAPS (DATE '2001-10-30', DATE '2002-10-30'))::text"));
        // the (start, length) spelling
        assertEquals("true", scalar("SELECT ((DATE '2001-02-16', INTERVAL '100 days')"
                + " OVERLAPS (DATE '2001-02-16', INTERVAL '100 days'))::text"));
        // a zero-length period at the other period's start
        assertEquals("true", scalar("SELECT ((DATE '2001-02-16', DATE '2001-02-16')"
                + " OVERLAPS (DATE '2001-02-16', DATE '2001-02-20'))::text"));
    }

    @Test
    void overlapsOverColumnData() throws Exception {
        exec("DROP TABLE IF EXISTS ert_ovl CASCADE");
        exec("CREATE TABLE ert_ovl (s1 date, e1 date, s2 date, e2 date)");
        exec("INSERT INTO ert_ovl VALUES"
                + " ('2001-02-16','2001-02-20','2001-02-18','2001-02-25'),"
                + " ('2001-02-16',NULL,'2001-06-01','2001-06-05'),"
                + " (NULL,NULL,'2001-02-18','2001-02-25')");
        assertEquals("1", scalar("SELECT count(*)::text FROM ert_ovl WHERE (s1,e1) OVERLAPS (s2,e2)"));
        assertEquals("2", scalar("SELECT count(*)::text FROM ert_ovl"
                + " WHERE ((s1,e1) OVERLAPS (s2,e2)) IS NULL"));
        exec("DROP TABLE IF EXISTS ert_ovl CASCADE");
    }

    // ------------------------------------------------- strict NULL arguments

    @Test
    void nullFillDelimiterOrPatternYieldsNull() throws Exception {
        assertNull(scalar("SELECT rpad('abc', 10, NULL)"));
        assertNull(scalar("SELECT lpad('abc', 10, NULL)"));
        assertNull(scalar("SELECT array_to_string(ARRAY[1,2], NULL)"));
        assertNull(scalar("SELECT replace('abc', NULL, 'x')"));
        assertNull(scalar("SELECT replace('abc', 'b', NULL)"));
        assertNull(scalar("SELECT translate('abc', NULL, 'x')"));
        assertNull(scalar("SELECT translate('abc', 'a', NULL)"));
        assertNull(scalar("SELECT split_part('a,b', NULL, 1)"));
        assertNull(scalar("SELECT overlay('abc' placing NULL from 2)"));
    }

    @Test
    void nullArgumentsNeverBecomeTheTextNull() throws Exception {
        // the previous behaviour stored the four-letter string "null" as data
        assertNull(scalar("SELECT rpad('abc', 10, NULL)"));
        assertNull(scalar("SELECT lpad('abc', 10, NULL)"));
        assertNull(scalar("SELECT array_to_string(ARRAY[1,2], NULL)"));
        assertEquals("0", scalar("SELECT count(*)::text FROM (SELECT rpad('abc', 10, NULL) AS v) t"
                + " WHERE v LIKE '%null%'"));
    }

    @Test
    void strictFunctionsKeepWorkingWithRealArguments() throws Exception {
        assertEquals("abcxyx", scalar("SELECT rpad('abc', 6, 'xy')"));
        assertEquals("xyxabc", scalar("SELECT lpad('abc', 6, 'xy')"));
        assertEquals("aXcaXc", scalar("SELECT replace('abcabc', 'b', 'X')"));
        assertEquals("xbydzf", scalar("SELECT translate('abcdef', 'ace', 'xyz')"));
        assertEquals("b", scalar("SELECT split_part('a,b,c', ',', 2)"));
        assertEquals("1,2,3", scalar("SELECT array_to_string(ARRAY[1,2,3], ',')"));
        assertEquals("1,3", scalar("SELECT array_to_string(ARRAY[1,NULL,3], ',')"));
        assertEquals("1,?,3", scalar("SELECT array_to_string(ARRAY[1,NULL,3], ',', '?')"));
        assertEquals("aXYef", scalar("SELECT overlay('abcdef' placing 'XY' from 2 for 3)"));
    }

    @Test
    void nullArgumentsArrivingAsColumnData() throws Exception {
        exec("DROP TABLE IF EXISTS ert_sfn CASCADE");
        exec("CREATE TABLE ert_sfn (s text, fill text, delim text)");
        exec("INSERT INTO ert_sfn VALUES ('abc','x',','), ('abc',NULL,','),"
                + " ('abc','x',NULL), (NULL,'x',',')");
        assertEquals("2", scalar("SELECT count(*)::text FROM ert_sfn WHERE rpad(s, 6, fill) IS NULL"));
        assertEquals("2", scalar("SELECT count(*)::text FROM ert_sfn"
                + " WHERE translate(s, 'a', fill) IS NULL"));
        assertEquals("2", scalar("SELECT count(*)::text FROM ert_sfn"
                + " WHERE split_part(s, delim, 1) IS NULL"));
        exec("DROP TABLE IF EXISTS ert_sfn CASCADE");
    }
}
