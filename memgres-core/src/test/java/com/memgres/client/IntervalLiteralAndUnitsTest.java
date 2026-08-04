package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * How an interval literal is read, and which spellings PostgreSQL refuses.
 *
 * <p>An interval literal is not one grammar but four, tried in turn, and the differences between
 * them are the whole subject.
 *
 * <p>An ISO 8601 duration is read from the text exactly as given: it must begin at the first
 * character with an upper-case {@code P} and end at the last, so {@code ' P1Y'}, {@code 'P1Y '}
 * and {@code 'p1Y'} are not durations at all, and neither is a bare {@code 'P'}. Inside, PG loops
 * over number-and-designator pairs, so a designator may repeat and its total accumulates
 * ({@code 'P1D1D'} is two days), a quantity may be fractional or carry an exponent, and every
 * letter is upper case. Two alternative forms write the fields positionally instead — extended
 * {@code 'P0001-02-03'} and basic {@code 'P00010203'} — chosen by the width of the first number,
 * eight digits for a date and six for a time, and neither may follow a designator field.
 *
 * <p>A unit word is looked up in a keyword table holding each unit under at most ten characters,
 * after truncating the word it is given. That one rule decides the accepted set: 'microseconds'
 * and 'microsecon' name the same field, 'millenniums' shortens to 'millennium' and works, while
 * 'cents', 'millenium' and 'milleniums' name nothing at all. A bare number is a count of seconds,
 * but only when it is nothing but digits — a trailing letter is a unit, so '1d' is a day.
 *
 * <p>Where a fraction goes is decided per unit and never cascades twice: a fraction of a year
 * rounds to a whole month, a fraction of a month or a week becomes whole days and only then part
 * of a day, and a fraction of a day or smaller becomes microseconds. An interval with nothing in
 * it is a syntax error, not a zero.
 *
 * <p>Every expected value below was measured against PostgreSQL 18.
 */
class IntervalLiteralAndUnitsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("SET TimeZone='UTC'");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The first column of the first row, as text; the empty string when the value is null. */
    private static String value(String sql) {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) return "(no rows)";
            String v = rs.getString(1);
            return v == null ? "" : v.replace("\n", "~");
        } catch (SQLException e) {
            return "raised " + e.getSQLState() + " " + e.getMessage().split("\n")[0];
        }
    }

    /** The SQLSTATE and first message line a statement raises, or "OK" when it does not raise. */
    private static String failure(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState() + " " + e.getMessage().split("\n")[0].replace("ERROR: ", "");
        }
    }

    /** ISO 8601 durations: a designator per field, and the field may repeat. */
    @Test
    void iso8601DurationsADesignatorPerFieldAndTheFieldMayRepeat() {
        assertEquals("1 year 2 mons 3 days 04:05:06", value("SELECT (interval 'P1Y2M3DT4H5M6S')::text AS v"));
        assertEquals("1 year", value("SELECT (interval 'P1Y')::text AS v"));
        assertEquals("1 mon", value("SELECT (interval 'P1M')::text AS v"));
        assertEquals("7 days", value("SELECT (interval 'P1W')::text AS v"));
        assertEquals("1 day", value("SELECT (interval 'P1D')::text AS v"));
        assertEquals("01:00:00", value("SELECT (interval 'PT1H')::text AS v"));
        assertEquals("00:01:00", value("SELECT (interval 'PT1M')::text AS v"));
        assertEquals("00:00:01", value("SELECT (interval 'PT1S')::text AS v"));
        assertEquals("00:00:00", value("SELECT (interval 'PT0S')::text AS v"));
        assertEquals("00:00:00", value("SELECT (interval 'PT')::text AS v"));
        assertEquals("1 day", value("SELECT (interval 'P1DT')::text AS v"));
        assertEquals("1 year", value("SELECT (interval 'P1YT')::text AS v"));
        assertEquals("1 year 2 mons 25 days", value("SELECT (interval 'P1Y2M3W4D')::text AS v"));
        assertEquals("-1 years -2 mons -3 days -04:05:06", value("SELECT (interval 'P-1Y-2M-3DT-4H-5M-6S')::text AS v"));
        assertEquals("2 days", value("SELECT (interval 'P1D1D')::text AS v"));
        assertEquals("02:00:00", value("SELECT (interval 'PT1H1H')::text AS v"));
        assertEquals("2 years", value("SELECT (interval 'P1Y1Y')::text AS v"));
        assertEquals("2 mons", value("SELECT (interval 'P1M1M')::text AS v"));
        assertEquals("21 days", value("SELECT (interval 'P1W2W')::text AS v"));
        assertEquals("00:02:00", value("SELECT (interval 'PT1M1M')::text AS v"));
        assertEquals("00:00:02", value("SELECT (interval 'PT1S1S')::text AS v"));
        assertEquals("10 mons", value("SELECT (interval 'P1Y-2M')::text AS v"));
    }

    /** ISO 8601 durations: a quantity may be fractional, and spills as PG's own units do. */
    @Test
    void iso8601DurationsAQuantityMayBeFractionalAndSpillsAsPgSOwnUnitsDo() {
        assertEquals("1 year 6 mons", value("SELECT (interval 'P1.5Y')::text AS v"));
        assertEquals("1 year", value("SELECT (interval 'P0.99Y')::text AS v"));
        assertEquals("1 year 1 mon", value("SELECT (interval 'P1.08333Y')::text AS v"));
        assertEquals("1 mon 15 days", value("SELECT (interval 'P1.5M')::text AS v"));
        assertEquals("1 mon 16 days 12:00:00", value("SELECT (interval 'P1.55M')::text AS v"));
        assertEquals("29 days 16:48:00", value("SELECT (interval 'P0.99M')::text AS v"));
        assertEquals("10 days 12:00:00", value("SELECT (interval 'P1.5W')::text AS v"));
        assertEquals("13 days 22:19:12", value("SELECT (interval 'P1.99W')::text AS v"));
        assertEquals("1 day 12:00:00", value("SELECT (interval 'P1.5D')::text AS v"));
        assertEquals("23:45:36", value("SELECT (interval 'P0.99D')::text AS v"));
        assertEquals("12:00:00", value("SELECT (interval 'P.5D')::text AS v"));
        assertEquals("1 day", value("SELECT (interval 'P1.D')::text AS v"));
        assertEquals("-1 days -12:00:00", value("SELECT (interval 'P-1.5D')::text AS v"));
        assertEquals("1 day 13:30:00", value("SELECT (interval 'P1.5DT1.5H')::text AS v"));
        assertEquals("01:30:00", value("SELECT (interval 'PT1.5H')::text AS v"));
        assertEquals("-01:30:00", value("SELECT (interval 'PT-1.5H')::text AS v"));
        assertEquals("00:01:30", value("SELECT (interval 'PT1.5M')::text AS v"));
        assertEquals("00:00:01.5", value("SELECT (interval 'PT1.5S')::text AS v"));
        assertEquals("00:00:00", value("SELECT (interval 'PT0.0000005S')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval 'PT0.0000015S')::text AS v"));
        assertEquals("00:00:02", value("SELECT (interval 'PT1.9999999S')::text AS v"));
        assertEquals("1 year 7 mons 15 days", value("SELECT (interval 'P1.5Y1.5M')::text AS v"));
        assertEquals("100 days", value("SELECT (interval 'P1e2D')::text AS v"));
        assertEquals("00:00:10", value("SELECT (interval 'PT1e1S')::text AS v"));
        assertEquals("15 days", value("SELECT (interval 'P1.5e1D')::text AS v"));
    }

    /** ISO 8601 durations: the two alternative forms, positional and run together. */
    @Test
    void iso8601DurationsTheTwoAlternativeFormsPositionalAndRunTogether() {
        assertEquals("1 year 2 mons 3 days", value("SELECT (interval 'P0001-02-03')::text AS v"));
        assertEquals("1 year 2 mons 3 days 04:05:06", value("SELECT (interval 'P0001-02-03T04:05:06')::text AS v"));
        assertEquals("1 year 2 mons 3 days 04:05:06.789", value("SELECT (interval 'P0001-02-03T04:05:06.789')::text AS v"));
        assertEquals("1 year 2 mons 3 days 04:05:06", value("SELECT (interval 'P1-2-3T4:5:6')::text AS v"));
        assertEquals("-10 mons +3 days", value("SELECT (interval 'P-0001-02-03')::text AS v"));
        assertEquals("1 year 2 mons 3 days", value("SELECT (interval 'P00010203')::text AS v"));
        assertEquals("1 year 2 mons 3 days 04:05:06", value("SELECT (interval 'P00010203T040506')::text AS v"));
        assertEquals("102 years", value("SELECT (interval 'P000102')::text AS v"));
        assertEquals("00:00:00", value("SELECT (interval 'P00000000')::text AS v"));
        assertEquals("1 year 2 mons 3 days 12:00:00", value("SELECT (interval 'P00010203.5')::text AS v"));
        assertEquals("1 year 2 mons 3 days 16:05:06", value("SELECT (interval 'P00010203.5T040506')::text AS v"));
        assertEquals("405:30:00", value("SELECT (interval 'PT0405.5')::text AS v"));
        assertEquals("04:30:00", value("SELECT (interval 'PT04.5')::text AS v"));
        assertEquals("04:05:06.000001", value("SELECT (interval 'PT040506.789')::text AS v"));
        assertEquals("00:00:01", value("SELECT (interval 'PT000001.5')::text AS v"));
        assertEquals("1 year 2 mons 3 days 405:00:00", value("SELECT (interval 'P00010203T0405')::text AS v"));
    }

    /** ISO 8601 durations: what is not one. */
    @Test
    void iso8601DurationsWhatIsNotOne() {
        assertEquals("22007 invalid input syntax for type interval: \"P\"", failure("SELECT (interval 'P')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"p1Y\"", failure("SELECT (interval 'p1Y')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P1y2m3dT4h5m6s\"", failure("SELECT (interval 'P1y2m3dT4h5m6s')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P1Y2M3Dt4H\"", failure("SELECT (interval 'P1Y2M3Dt4H')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \" P1Y\"", failure("SELECT (interval ' P1Y')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P1Y \"", failure("SELECT (interval 'P1Y ')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \" P1Y \"", failure("SELECT (interval ' P1Y ')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P 1Y\"", failure("SELECT (interval 'P 1Y')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P1Y2M3D4H\"", failure("SELECT (interval 'P1Y2M3D4H')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"PY\"", failure("SELECT (interval 'PY')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"PT1D\"", failure("SELECT (interval 'PT1D')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P1S\"", failure("SELECT (interval 'P1S')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"PTS\"", failure("SELECT (interval 'PTS')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P+1D\"", failure("SELECT (interval 'P+1D')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"PT+1S\"", failure("SELECT (interval 'PT+1S')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P--1D\"", failure("SELECT (interval 'P--1D')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P1D2\"", failure("SELECT (interval 'P1D2')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P1DX\"", failure("SELECT (interval 'P1DX')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P1,5D\"", failure("SELECT (interval 'P1,5D')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"PT1,5S\"", failure("SELECT (interval 'PT1,5S')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P0001-02-03 04:05:06\"", failure("SELECT (interval 'P0001-02-03 04:05:06')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P0001-02-03t04:05:06\"", failure("SELECT (interval 'P0001-02-03t04:05:06')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"P1e400D\"", failure("SELECT (interval 'P1e400D')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"PT1e400S\"", failure("SELECT (interval 'PT1e400S')::text AS v"));
    }

    /** ISO 8601 durations: a field that will not fit. */
    @Test
    void iso8601DurationsAFieldThatWillNotFit() {
        assertEquals("2147483647 days", value("SELECT (interval 'P2147483647D')::text AS v"));
        assertEquals("22015 interval field value out of range: \"P2147483648D\"", failure("SELECT (interval 'P2147483648D')::text AS v"));
        assertEquals("22008 interval out of range", failure("SELECT (interval 'P1000000000Y')::text AS v"));
        assertEquals("99999999 years", value("SELECT (interval 'P99999999Y')::text AS v"));
        assertEquals("32767 years", value("SELECT (interval 'P32767Y')::text AS v"));
    }

    /** Unit words: every spelling PostgreSQL keeps, and the near misses it does not. */
    @Test
    void unitWordsEverySpellingPostgresqlKeepsAndTheNearMissesItDoesNot() {
        assertEquals("200 years", value("SELECT (interval '2 c')::text AS v"));
        assertEquals("200 years", value("SELECT (interval '2 cent')::text AS v"));
        assertEquals("200 years", value("SELECT (interval '2 century')::text AS v"));
        assertEquals("200 years", value("SELECT (interval '2 centuries')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 cents\"", failure("SELECT (interval '2 cents')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 centurie\"", failure("SELECT (interval '2 centurie')::text AS v"));
        assertEquals("2000 years", value("SELECT (interval '2 mil')::text AS v"));
        assertEquals("2000 years", value("SELECT (interval '2 mils')::text AS v"));
        assertEquals("2000 years", value("SELECT (interval '2 millennia')::text AS v"));
        assertEquals("2000 years", value("SELECT (interval '2 millennium')::text AS v"));
        assertEquals("2000 years", value("SELECT (interval '2 millenniums')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 millennias\"", failure("SELECT (interval '2 millennias')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 millenium\"", failure("SELECT (interval '2 millenium')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 milleniums\"", failure("SELECT (interval '2 milleniums')::text AS v"));
        assertEquals("20 years", value("SELECT (interval '2 dec')::text AS v"));
        assertEquals("20 years", value("SELECT (interval '2 decs')::text AS v"));
        assertEquals("20 years", value("SELECT (interval '2 decade')::text AS v"));
        assertEquals("20 years", value("SELECT (interval '2 decades')::text AS v"));
        assertEquals("2 years", value("SELECT (interval '2 y')::text AS v"));
        assertEquals("2 years", value("SELECT (interval '2 yr')::text AS v"));
        assertEquals("2 years", value("SELECT (interval '2 yrs')::text AS v"));
        assertEquals("2 years", value("SELECT (interval '2 year')::text AS v"));
        assertEquals("2 years", value("SELECT (interval '2 years')::text AS v"));
        assertEquals("2 mons", value("SELECT (interval '2 mon')::text AS v"));
        assertEquals("2 mons", value("SELECT (interval '2 mons')::text AS v"));
        assertEquals("2 mons", value("SELECT (interval '2 month')::text AS v"));
        assertEquals("2 mons", value("SELECT (interval '2 months')::text AS v"));
        assertEquals("14 days", value("SELECT (interval '2 w')::text AS v"));
        assertEquals("14 days", value("SELECT (interval '2 week')::text AS v"));
        assertEquals("14 days", value("SELECT (interval '2 weeks')::text AS v"));
        assertEquals("2 days", value("SELECT (interval '2 d')::text AS v"));
        assertEquals("2 days", value("SELECT (interval '2 day')::text AS v"));
        assertEquals("2 days", value("SELECT (interval '2 days')::text AS v"));
        assertEquals("02:00:00", value("SELECT (interval '2 h')::text AS v"));
        assertEquals("02:00:00", value("SELECT (interval '2 hr')::text AS v"));
        assertEquals("02:00:00", value("SELECT (interval '2 hrs')::text AS v"));
        assertEquals("02:00:00", value("SELECT (interval '2 hour')::text AS v"));
        assertEquals("02:00:00", value("SELECT (interval '2 hours')::text AS v"));
        assertEquals("00:02:00", value("SELECT (interval '2 m')::text AS v"));
        assertEquals("00:02:00", value("SELECT (interval '2 min')::text AS v"));
        assertEquals("00:02:00", value("SELECT (interval '2 mins')::text AS v"));
        assertEquals("00:02:00", value("SELECT (interval '2 minute')::text AS v"));
        assertEquals("00:02:00", value("SELECT (interval '2 minutes')::text AS v"));
        assertEquals("00:00:02", value("SELECT (interval '2 s')::text AS v"));
        assertEquals("00:00:02", value("SELECT (interval '2 sec')::text AS v"));
        assertEquals("00:00:02", value("SELECT (interval '2 secs')::text AS v"));
        assertEquals("00:00:02", value("SELECT (interval '2 second')::text AS v"));
        assertEquals("00:00:02", value("SELECT (interval '2 seconds')::text AS v"));
        assertEquals("00:00:00.002", value("SELECT (interval '2 ms')::text AS v"));
        assertEquals("00:00:00.002", value("SELECT (interval '2 msec')::text AS v"));
        assertEquals("00:00:00.002", value("SELECT (interval '2 msecs')::text AS v"));
        assertEquals("00:00:00.002", value("SELECT (interval '2 msecond')::text AS v"));
        assertEquals("00:00:00.002", value("SELECT (interval '2 mseconds')::text AS v"));
        assertEquals("00:00:00.002", value("SELECT (interval '2 millisecon')::text AS v"));
        assertEquals("00:00:00.002", value("SELECT (interval '2 millisecond')::text AS v"));
        assertEquals("00:00:00.002", value("SELECT (interval '2 milliseconds')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2 us')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2 usec')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2 usecs')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2 usecond')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2 useconds')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2 microsecon')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2 microsecond')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2 microseconds')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 quarter\"", failure("SELECT (interval '2 quarter')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 qtr\"", failure("SELECT (interval '2 qtr')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 timezone\"", failure("SELECT (interval '2 timezone')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 epoch\"", failure("SELECT (interval '2 epoch')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 julian\"", failure("SELECT (interval '2 julian')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"2 j\"", failure("SELECT (interval '2 j')::text AS v"));
        assertEquals("00:00:00.002", value("SELECT (interval '2 MS')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2 US')::text AS v"));
        assertEquals("2000 years", value("SELECT (interval '2 MIL')::text AS v"));
        assertEquals("200 years", value("SELECT (interval '2 C')::text AS v"));
    }

    /** A fractional quantity spills into the next smaller field, never further. */
    @Test
    void aFractionalQuantitySpillsIntoTheNextSmallerFieldNeverFurther() {
        assertEquals("10 days 12:00:00", value("SELECT (interval '1.5 weeks')::text AS v"));
        assertEquals("3 days 12:00:00", value("SELECT (interval '0.5 weeks')::text AS v"));
        assertEquals("8 days 18:00:00", value("SELECT (interval '1.25 weeks')::text AS v"));
        assertEquals("10 days 20:24:00", value("SELECT (interval '1.55 weeks')::text AS v"));
        assertEquals("13 days 22:19:12", value("SELECT (interval '1.99 weeks')::text AS v"));
        assertEquals("-10 days -12:00:00", value("SELECT (interval '-1.5 weeks')::text AS v"));
        assertEquals("17 days 12:00:00", value("SELECT (interval '2.5 weeks')::text AS v"));
        assertEquals("10 days 12:00:00", value("SELECT (interval '1.5 w')::text AS v"));
        assertEquals("1 mon 16 days 12:00:00", value("SELECT (interval '1.55 months')::text AS v"));
        assertEquals("1 mon 27 days", value("SELECT (interval '1.9 months')::text AS v"));
        assertEquals("29 days 16:48:00", value("SELECT (interval '0.99 months')::text AS v"));
        assertEquals("-1 mons -16 days -12:00:00", value("SELECT (interval '-1.55 months')::text AS v"));
        assertEquals("2 years", value("SELECT (interval '1.99 years')::text AS v"));
        assertEquals("1 year", value("SELECT (interval '0.99 years')::text AS v"));
        assertEquals("1 year 1 mon", value("SELECT (interval '1.08333 years')::text AS v"));
        assertEquals("1 year 6 mons", value("SELECT (interval '1.5 years')::text AS v"));
        assertEquals("15 years", value("SELECT (interval '1.5 decades')::text AS v"));
        assertEquals("15 years 6 mons", value("SELECT (interval '1.55 dec')::text AS v"));
        assertEquals("150 years", value("SELECT (interval '1.5 centuries')::text AS v"));
        assertEquals("155 years", value("SELECT (interval '1.55 centuries')::text AS v"));
        assertEquals("1500 years", value("SELECT (interval '1.5 millennia')::text AS v"));
        assertEquals("1550 years", value("SELECT (interval '1.55 mil')::text AS v"));
        assertEquals("1 day 12:00:00", value("SELECT (interval '1.5 days')::text AS v"));
        assertEquals("00:30:00", value("SELECT (interval '0.5 hours')::text AS v"));
        assertEquals("00:01:30", value("SELECT (interval '1.5 minutes')::text AS v"));
        assertEquals("00:00:01.5", value("SELECT (interval '1.5 seconds')::text AS v"));
        assertEquals("00:00:00.0015", value("SELECT (interval '1.5 ms')::text AS v"));
        assertEquals("00:00:00.0014", value("SELECT (interval '1.4 ms')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval '0.0015 ms')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval '1.5 us')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2.5 us')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '1.6 us')::text AS v"));
        assertEquals("00:00:00", value("SELECT (interval '0.5 us')::text AS v"));
        assertEquals("-00:00:00.000001", value("SELECT (interval '-1.5 us')::text AS v"));
        assertEquals("00:00:00", value("SELECT (interval '0.0000005 seconds')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval '0.0000015 seconds')::text AS v"));
        assertEquals("00:00:02", value("SELECT (interval '1.9999999 seconds')::text AS v"));
    }

    /** A fraction that lands exactly half way keeps the smaller magnitude, except in months. */
    @Test
    void aFractionThatLandsExactlyHalfWayKeepsTheSmallerMagnitudeExceptInMonths() {
        assertEquals("1 year 2 mons", value("SELECT (interval '1.125 years')::text AS v"));
        assertEquals("1 year 4 mons", value("SELECT (interval '1.375 years')::text AS v"));
        assertEquals("2 mons", value("SELECT (interval '0.125 years')::text AS v"));
        assertEquals("-1 years -2 mons", value("SELECT (interval '-1.125 years')::text AS v"));
        assertEquals("10 years 2 mons", value("SELECT (interval '1.0125 decades')::text AS v"));
        assertEquals("100 years 2 mons", value("SELECT (interval '1.00125 centuries')::text AS v"));
        assertEquals("1000 years 2 mons", value("SELECT (interval '1.000125 millennia')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval '0.0015 ms')::text AS v"));
        assertEquals("00:00:00.000003", value("SELECT (interval '0.0035 ms')::text AS v"));
        assertEquals("00:00:00.000004", value("SELECT (interval '0.0045 ms')::text AS v"));
        assertEquals("00:00:00", value("SELECT (interval '0.0005 ms')::text AS v"));
        assertEquals("00:00:00.001", value("SELECT (interval '1.0005 ms')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval '1.5 us')::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (interval '2.5 us')::text AS v"));
        assertEquals("00:00:00.000003", value("SELECT (interval '3.5 us')::text AS v"));
        assertEquals("00:00:00.000004", value("SELECT (interval '4.5 us')::text AS v"));
        assertEquals("-00:00:00.000002", value("SELECT (interval '-2.5 us')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval '0.0000015 seconds')::text AS v"));
        assertEquals("00:00:00.000003", value("SELECT (interval '0.0000035 seconds')::text AS v"));
        assertEquals("00:00:00.000004", value("SELECT (interval '0.0000045 seconds')::text AS v"));
        assertEquals("00:00:01.000001", value("SELECT (interval '1.0000015 seconds')::text AS v"));
        assertEquals("00:00:01.000002", value("SELECT (interval '1.0000025 seconds')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval '0.000000025 minutes')::text AS v"));
        assertEquals("1 mon 12:00:00", value("SELECT (interval '1.0166666666666667 months')::text AS v"));
        assertEquals("7 days 12:00:00", value("SELECT (interval '1.0714285714285714 weeks')::text AS v"));
        assertEquals("1 year 2 mons", value("SELECT (interval 'P1.125Y')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval 'PT0.0000015S')::text AS v"));
        assertEquals("00:00:00.000003", value("SELECT (interval 'PT0.0000035S')::text AS v"));
        assertEquals("1 mon 12:00:00", value("SELECT (interval 'P1.0166666666666667M')::text AS v"));
        assertEquals("7 days 12:00:00", value("SELECT (interval 'P1.0714285714285714W')::text AS v"));
    }

    /** A bare number is seconds; a trailing letter is a unit, not a numeric suffix. */
    @Test
    void aBareNumberIsSecondsATrailingLetterIsAUnitNotANumericSuffix() {
        assertEquals("00:00:01", value("SELECT (interval '1')::text AS v"));
        assertEquals("00:00:12", value("SELECT (interval '12')::text AS v"));
        assertEquals("-00:00:01", value("SELECT (interval '-1')::text AS v"));
        assertEquals("00:00:01.5", value("SELECT (interval '1.5')::text AS v"));
        assertEquals("00:00:01", value("SELECT (interval '1.')::text AS v"));
        assertEquals("00:00:00.5", value("SELECT (interval '.5')::text AS v"));
        assertEquals("00:00:01", value("SELECT (interval '1.0000005')::text AS v"));
        assertEquals("00:00:00", value("SELECT (interval '0')::text AS v"));
        assertEquals("1 day", value("SELECT (interval '1d')::text AS v"));
        assertEquals("1 day", value("SELECT (interval '1D')::text AS v"));
        assertEquals("01:00:00", value("SELECT (interval '1h')::text AS v"));
        assertEquals("00:01:00", value("SELECT (interval '1m')::text AS v"));
        assertEquals("00:00:01", value("SELECT (interval '1s')::text AS v"));
        assertEquals("7 days", value("SELECT (interval '1w')::text AS v"));
        assertEquals("1 year", value("SELECT (interval '1y')::text AS v"));
        assertEquals("100 years", value("SELECT (interval '1c')::text AS v"));
        assertEquals("00:00:00.001", value("SELECT (interval '1ms')::text AS v"));
        assertEquals("00:00:00.000001", value("SELECT (interval '1us')::text AS v"));
        assertEquals("1 mon", value("SELECT (interval '1mon')::text AS v"));
        assertEquals("1000 years", value("SELECT (interval '1mil')::text AS v"));
        assertEquals("2 days", value("SELECT (interval '2days')::text AS v"));
        assertEquals("1 day 12:00:00", value("SELECT (interval '1.5d')::text AS v"));
        assertEquals("10 days 12:00:00", value("SELECT (interval '1.5w')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"1e3\"", failure("SELECT (interval '1e3')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"1f\"", failure("SELECT (interval '1f')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"1e400\"", failure("SELECT (interval '1e400')::text AS v"));
    }

    /** An interval with nothing in it is not an interval. */
    @Test
    void anIntervalWithNothingInItIsNotAnInterval() {
        assertEquals("22007 invalid input syntax for type interval: \"\"", failure("SELECT (interval '')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"\"", failure("SELECT (''::interval)::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"   \"", failure("SELECT (interval '   ')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"   \"", failure("SELECT ('   '::interval)::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"\t\"", failure("SELECT (interval '\t')::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"\t\"", failure("SELECT ('\t'::interval)::text AS v"));
        assertEquals("", value("SELECT (NULL::interval)::text AS v"));
        assertEquals("1 day", value("SELECT ('1 day'::interval)::text AS v"));
    }

    /** The shapes PostgreSQL itself writes still read back. */
    @Test
    void theShapesPostgresqlItselfWritesStillReadBack() {
        assertEquals("1 year 2 mons 3 days 04:05:06", value("SELECT (interval '1 year 2 mons 3 days 04:05:06')::text AS v"));
        assertEquals("1 year 2 mons -3 days -04:05:06", value("SELECT (interval '1 year 2 mons -3 days -04:05:06')::text AS v"));
        assertEquals("04:05:06", value("SELECT (interval '04:05:06')::text AS v"));
        assertEquals("-04:05:06", value("SELECT (interval '-04:05:06')::text AS v"));
        assertEquals("1 year 2 mons", value("SELECT (interval '1-2')::text AS v"));
        assertEquals("2 days 04:05:06", value("SELECT (interval '2 04:05:06')::text AS v"));
        assertEquals("1 day", value("SELECT (interval '@ 1 day')::text AS v"));
        assertEquals("-1 days", value("SELECT (interval '@ 1 day ago')::text AS v"));
        assertEquals("-1 days", value("SELECT (interval '1 day ago')::text AS v"));
        assertEquals("-1 days", value("SELECT (interval '-1 day')::text AS v"));
        assertEquals("1 mon 2 days", value("SELECT (interval '1 mon 2 days')::text AS v"));
        assertEquals("infinity", value("SELECT (interval 'infinity')::text AS v"));
        assertEquals("-infinity", value("SELECT (interval '-infinity')::text AS v"));
        assertEquals("1 day 01:00:00", value("SELECT (interval '1 day 01:00:00')::text AS v"));
        assertEquals("1 year 1 mon 8 days 01:01:01", value("SELECT (interval '1 year 1 month 1 week 1 day 1 hour 1 minute 1 second')::text AS v"));
    }

    /** Field qualifiers, and the units they read from a literal. */
    @Test
    void fieldQualifiersAndTheUnitsTheyReadFromALiteral() {
        assertEquals("00:00:01.235", value("SELECT (INTERVAL '1.234567 seconds' SECOND(3))::text AS v"));
        assertEquals("00:00:01", value("SELECT (INTERVAL '1.234567 seconds' SECOND(0))::text AS v"));
        assertEquals("00:00:01.234567", value("SELECT (INTERVAL '1.234567 seconds' SECOND)::text AS v"));
        assertEquals("5 days", value("SELECT (INTERVAL '5' DAY)::text AS v"));
        assertEquals("5 years", value("SELECT (INTERVAL '5' YEAR)::text AS v"));
        assertEquals("1 day 02:00:00", value("SELECT (INTERVAL '1 day 2 hours' DAY TO HOUR)::text AS v"));
        assertEquals("1 year 2 mons", value("SELECT (INTERVAL '1-2' YEAR TO MONTH)::text AS v"));
        assertEquals("1 day 02:03:04", value("SELECT (INTERVAL '1 2:03:04' DAY TO SECOND(2))::text AS v"));
        assertEquals("10 days", value("SELECT (INTERVAL '1.5 weeks' DAY)::text AS v"));
        assertEquals("00:00:00.0015", value("SELECT (INTERVAL '1.5 ms' SECOND)::text AS v"));
        assertEquals("200 years", value("SELECT (INTERVAL '2 c' YEAR)::text AS v"));
        assertEquals("00:00:00.000002", value("SELECT (INTERVAL '2 us' SECOND(6))::text AS v"));
        assertEquals("00:00:01.2", value("SELECT (INTERVAL '1.234567 seconds' MINUTE TO SECOND(1))::text AS v"));
        assertEquals("00:00:01.23", value("SELECT ('1.23456789 sec'::interval second(2))::text AS v"));
        assertEquals("00:00:01.23", value("SELECT (CAST('1.23456789 sec' AS interval second(2)))::text AS v"));
        assertEquals("22007 invalid input syntax for type interval: \"\"", failure("SELECT (''::interval day)::text AS v"));
    }

    /** extract from an interval, over every unit PostgreSQL documents. */
    @Test
    void extractFromAnIntervalOverEveryUnitPostgresqlDocuments() {
        assertEquals("2", value("SELECT extract(millennium from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("20", value("SELECT extract(century from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("200", value("SELECT extract(decade from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("2001", value("SELECT extract(year from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("2", value("SELECT extract(quarter from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("3", value("SELECT extract(month from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("1", value("SELECT extract(week from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("13", value("SELECT extract(day from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("24", value("SELECT extract(hour from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("5", value("SELECT extract(minute from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("6.789000", value("SELECT extract(second from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("6789.000", value("SELECT extract(millisecond from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("6789000", value("SELECT extract(microsecond from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("63155743506.789000", value("SELECT extract(epoch from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("0A000 unit \"dow\" not supported for type interval", failure("SELECT extract(dow from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("0A000 unit \"doy\" not supported for type interval", failure("SELECT extract(doy from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("0A000 unit \"isodow\" not supported for type interval", failure("SELECT extract(isodow from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("0A000 unit \"isoyear\" not supported for type interval", failure("SELECT extract(isoyear from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("0A000 unit \"timezone\" not supported for type interval", failure("SELECT extract(timezone from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("0A000 unit \"timezone_hour\" not supported for type interval", failure("SELECT extract(timezone_hour from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("0A000 unit \"timezone_minute\" not supported for type interval", failure("SELECT extract(timezone_minute from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("0A000 unit \"julian\" not supported for type interval", failure("SELECT extract(julian from interval '2001 years 3 mons 13 days 24 hours 5 min 6.789 sec') AS v"));
        assertEquals("1", value("SELECT extract(week from interval '13 days 24 hours') AS v"));
        assertEquals("-20", value("SELECT extract(century from interval '-2001 years') AS v"));
        assertEquals("", value("SELECT extract(century from NULL::interval) AS v"));
        assertEquals("20", value("SELECT date_part('century', interval '2001 years') AS v"));
        assertEquals("22023 unit \"nosuchunit\" not recognized for type interval", failure("SELECT extract(nosuchunit from interval '1 day') AS v"));
    }

    /** date_bin refuses a stride it cannot step with. */
    @Test
    void dateBinRefusesAStrideItCannotStepWith() {
        assertEquals("2020-02-11 15:30:00", value("SELECT date_bin(interval '15 minutes', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v"));
        assertEquals("2020-02-11 00:00:00", value("SELECT date_bin(interval '1 day', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v"));
        assertEquals("22008 stride must be greater than zero", failure("SELECT date_bin(interval '-2 hours', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v"));
        assertEquals("22008 stride must be greater than zero", failure("SELECT date_bin(interval '0 sec', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v"));
        assertEquals("0A000 timestamps cannot be binned into intervals containing months or years", failure("SELECT date_bin(interval '1 mon 1 day', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v"));
        assertEquals("0A000 timestamps cannot be binned into intervals containing months or years", failure("SELECT date_bin(interval '1 year', timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v"));
        assertEquals("", value("SELECT date_bin(NULL::interval, timestamp '2020-02-11 15:44:17', timestamp '2001-01-01')::text AS v"));
    }

    /** An interval literal reads the same wherever it is written. */
    @Test
    void anIntervalLiteralReadsTheSameWhereverItIsWritten() {
        assertEquals("10 days 12:00:00", value("WITH c AS (SELECT interval '1.5 weeks' AS x) SELECT x::text AS v FROM c"));
        assertEquals("1 day 12:00:00", value("SELECT (SELECT interval 'P1.5D')::text AS v"));
        assertEquals("00:00:00.002002", value("SELECT (interval '2 ms' + interval '2 us')::text AS v"));
        assertEquals("t", value("SELECT (interval '1.5 weeks' = interval '10 days 12 hours') AS v"));
        assertEquals("907200.000000", value("SELECT extract(epoch from interval '1.5 weeks') AS v"));
    }
}
