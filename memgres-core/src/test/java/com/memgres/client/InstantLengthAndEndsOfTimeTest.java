package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An instant, a length, and the two ends of time.
 *
 * <p>The date/time types hold two values no instant answers to, and every operation over them has
 * to say what it does with those: an age reckoned across one of them is endless, a bin holding
 * one is that value rather than some instant far out, and a count of days between two dates
 * cannot reach one at all. Read as ordinary instants they came out as the largest date java.time
 * can hold, which is a real date and answers real questions with nonsense.
 *
 * <p>The rest is the reading and the writing. A literal is taken in each of the spellings the
 * type has — three numbers in whichever order the DateStyle puts them, a month written by name, a
 * clock with no separators in it, a half of the day — and refused in the shapes that are not one,
 * with the error PostgreSQL tells the two apart by. A displacement reaches sixteen hours and no
 * further. A fraction finer than the microsecond the type holds is rounded to one rather than cut
 * there, and a tie goes to the even microsecond. A precision may be written on the type itself,
 * and on the four clock functions. An age is subtracted field by field, borrowing the length of
 * the month the earlier instant is in. And a length of time is a value the totals, the means and
 * the percentiles all know.
 */
class InstantLengthAndEndsOfTimeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        exec("SET TimeZone = 'UTC'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
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

    // ---- the ends of time ----------------------------------------------------------------

    /** No bin holds an endless instant, so the value stands for its own bin. */
    @Test
    void aBinHoldingAnEndlessInstantIsThatInstant() throws SQLException {
        assertEquals("infinity",
                one("SELECT date_bin(interval '1 day', timestamp 'infinity',"
                        + " timestamp '2020-01-01')::text"));
        assertEquals("-infinity",
                one("SELECT date_bin(interval '1 day', timestamp '-infinity',"
                        + " timestamp '2020-01-01')::text"));
        assertEquals("infinity",
                one("SELECT date_bin(interval '1 day', timestamptz 'infinity',"
                        + " timestamptz '2020-01-01')::text"));
        // An endless origin is no place to count bins from.
        assertEquals("22008", stateOf("SELECT date_bin(interval '1 day',"
                + " timestamp '2020-01-01', timestamp 'infinity')"));
        assertEquals("origin out of range",
                messageOf("SELECT date_bin(interval '1 day', timestamp '2020-01-01',"
                        + " timestamp 'infinity')").replace("ERROR: ", ""));
    }

    /**
     * An endless first instant carries its own direction and an endless second the opposite one;
     * two pointing the same way leave no distance between them to name.
     */
    @Test
    void anAgeAcrossAnEndlessInstantIsEndless() throws SQLException {
        assertEquals("infinity",
                one("SELECT age(timestamp 'infinity', timestamp '2020-01-01')::text"));
        assertEquals("-infinity",
                one("SELECT age(timestamp '-infinity', timestamp '2020-01-01')::text"));
        assertEquals("-infinity",
                one("SELECT age(timestamp '2020-01-01', timestamp 'infinity')::text"));
        assertEquals("infinity",
                one("SELECT age(timestamp 'infinity', timestamp '-infinity')::text"));
        assertEquals("22008", stateOf("SELECT age(timestamp 'infinity', timestamp 'infinity')"));
    }

    /** One date taken from another is a count of days, and no count reaches an endless date. */
    @Test
    void oneEndlessDateCannotBeTakenFromAnother() throws SQLException {
        for (String sql : new String[]{
                "SELECT date 'infinity' - date '2020-01-01'",
                "SELECT date '2020-01-01' - date 'infinity'",
                "SELECT date 'infinity' - date 'infinity'",
                "SELECT date 'infinity' - date '-infinity'"}) {
            assertEquals("22008", stateOf(sql), sql);
            assertTrue(messageOf(sql).contains("cannot subtract infinite dates"), sql);
        }
        // The same two written as timestamps do have an answer, which is an endless interval.
        assertEquals("infinity",
                one("SELECT (timestamp 'infinity' - timestamp '2020-01-01')::text"));
        assertEquals("infinity",
                one("SELECT (timestamp 'infinity' - timestamp '-infinity')::text"));
    }

    /** Everything else built over one of them keeps it. */
    @Test
    void anEndlessInstantSurvivesTheRestOfTheArithmetic() throws SQLException {
        assertEquals("infinity", one("SELECT (date 'infinity' - 1)::text"));
        assertEquals("infinity", one("SELECT (date 'infinity' + interval '1 day')::text"));
        assertEquals("infinity", one("SELECT date_trunc('day', timestamp 'infinity')::text"));
        assertEquals("Infinity", one("SELECT extract(epoch from timestamp 'infinity')::text"));
        assertEquals("false", one("SELECT isfinite(timestamp 'infinity')::text"));
    }

    // ---- an age --------------------------------------------------------------------------

    /**
     * The month a day count borrows is the one the earlier instant is in, so an age reckoned
     * from a day in June borrows thirty days. Java's own Period borrows the month before the
     * later date, which is a different answer whenever the two months are different lengths.
     */
    @Test
    void anAgeBorrowsTheMonthItStartedIn() throws SQLException {
        assertEquals("43 years 9 mons 27 days",
                one("SELECT age(timestamp '2001-04-10', timestamp '1957-06-13')::text"));
        assertEquals("-43 years -9 mons -27 days",
                one("SELECT age(timestamp '1957-06-13', timestamp '2001-04-10')::text"));
        assertEquals("10 mons 1 day",
                one("SELECT age(timestamp '2021-01-01', timestamp '2020-02-29')::text"));
        assertEquals("1 mon 1 day",
                one("SELECT age(timestamp '2020-02-01', timestamp '2019-12-31')::text"));
        assertEquals("1 mon 2 days",
                one("SELECT age(timestamp '2020-03-31', timestamp '2020-02-29')::text"));
        assertEquals("1 mon 30 days",
                one("SELECT age(timestamp '2020-01-31', timestamp '2019-12-01')::text"));
        assertEquals("3 years 6 mons 16 days",
                one("SELECT age(timestamp '0001-01-01 BC', timestamp '0005-06-15 BC')::text"));
    }

    /** The clock fields borrow the same way, one into the next. */
    @Test
    void theClockFieldsBorrowIntoOneAnother() throws SQLException {
        assertEquals("1 mon 19:29:44.5",
                one("SELECT age(timestamp '2020-03-01 05:00',"
                        + " timestamp '2020-01-31 09:30:15.5')::text"));
        assertEquals("00:00:00.000002",
                one("SELECT age(timestamp '2020-07-01 00:00:00.000001',"
                        + " timestamp '2020-06-30 23:59:59.999999')::text"));
    }

    // ---- reading a literal ---------------------------------------------------------------

    /** A date written three numbers wide is read in the order the DateStyle puts them in. */
    @Test
    void aDateIsReadInEverySpellingTheTypeTakes() throws SQLException {
        assertEquals("2020-06-15", one("SELECT (date '06/15/2020')::text"));
        assertEquals("2020-06-15", one("SELECT (date '06.15.2020')::text"));
        assertEquals("2020-06-15", one("SELECT (date '2020.06.15')::text"));
        assertEquals("2020-01-02", one("SELECT (date '1.2.2020')::text"));
        assertEquals("2020-06-15", one("SELECT (date '20200615')::text"));
    }

    /** A month written by name is read wherever the year stands. */
    @Test
    void aMonthMayBeWrittenByName() throws SQLException {
        assertEquals("2020-06-15", one("SELECT (date 'June 15, 2020')::text"));
        assertEquals("2020-06-15", one("SELECT (date '15 Jun 2020')::text"));
        assertEquals("2020-06-15", one("SELECT (date 'Jun 15 2020')::text"));
        assertEquals("2020-06-15", one("SELECT (date '2020-Jun-15')::text"));
        assertEquals("2020-06-15 12:00:00",
                one("SELECT (timestamp 'Jun 15 12:00:00 2020')::text"));
        assertEquals("2020-06-15 12:00:00", one("SELECT (timestamp '20200615T120000')::text"));
    }

    /**
     * A field outside its own range is the mistake PostgreSQL suspects of being a date written in
     * another order, and it is the only one it offers the DateStyle advice for. A day that does
     * not exist in the month it names is refused with the message alone, and text that is not a
     * date at all never had a field to overflow.
     */
    @Test
    void whichFaultADateThatWillNotReadIs() throws SQLException {
        assertEquals("22008", stateOf("SELECT date '15.06.2020'"));
        assertTrue(messageOf("SELECT date '15.06.2020'").contains("DateStyle"));
        assertEquals("22008", stateOf("SELECT date '2020-15-06'"));
        assertTrue(messageOf("SELECT date '2020-15-06'").contains("DateStyle"));
        assertEquals("22008", stateOf("SELECT date '2020-02-30'"));
        assertFalse(messageOf("SELECT date '2020-02-30'").contains("DateStyle"));
        assertEquals("22007", stateOf("SELECT date '2020-006-005'"));
        assertEquals("22007", stateOf("SELECT date '2001-01-01-05'"));
    }

    /** What follows a date has to be a time or a zone; a word that is neither was never one. */
    @Test
    void whatMayBeWrittenAfterADate() throws SQLException {
        assertEquals("2020-06-15", one("SELECT (date '2020-06-15 12:00:00')::text"));
        assertEquals("2020-06-15", one("SELECT (date '2020-06-15 UTC')::text"));
        assertEquals("2001-01-01", one("SELECT (date '2001-01-01+02')::text"));
        assertEquals("22007", stateOf("SELECT date '2020-06-15 garbage'"));
    }

    /** A clock may be written without separators, and against the twelve-hour clock. */
    @Test
    void aClockIsReadInEverySpellingTheTypeTakes() throws SQLException {
        assertEquals("01:02:03", one("SELECT (time '1:2:3')::text"));
        assertEquals("01:02:03", one("SELECT (time '010203')::text"));
        assertEquals("01:02:00", one("SELECT (time '0102')::text"));
        assertEquals("01:02:00", one("SELECT (time '1:02 AM')::text"));
        assertEquals("13:02:00", one("SELECT (time '1:02 PM')::text"));
        assertEquals("00:00:00", one("SELECT (time '12:00 AM')::text"));
        assertEquals("12:00:00", one("SELECT (time '12:00 PM')::text"));
        assertEquals("00:02:00", one("SELECT (time '0:02 AM')::text"));
        assertEquals("01:02:00", one("SELECT (time '1:02am')::text"));
        assertEquals("13:02:03", one("SELECT (time '1:02:03pm')::text"));
        assertEquals("22008", stateOf("SELECT time '13:02 PM'"));
        // Written with dots it is not the half of the day at all, but a zone by that name.
        assertEquals("22023", stateOf("SELECT time '1:02 a.m.'"));
    }

    /** A zone is at most sixteen hours from Greenwich, exclusive, and its minutes are minutes. */
    @Test
    void aDisplacementReachesSixteenHoursAndNoFurther() throws SQLException {
        assertEquals("2020-06-15 06:29:45+00",
                one("SELECT (timestamptz '2020-06-15 12:00:00+05:30:15')::text"));
        assertEquals("2020-06-14 20:01:00+00",
                one("SELECT (timestamptz '2020-06-15 12:00:00+15:59')::text"));
        for (String sql : new String[]{
                "SELECT timestamptz '2020-06-15 12:00:00+16'",
                "SELECT timestamptz '2020-06-15 12:00:00-16'",
                "SELECT timestamptz '2020-06-15 12:00:00+15:60'",
                "SELECT timetz '12:00:00+16'"}) {
            assertEquals("22009", stateOf(sql), sql);
            assertTrue(messageOf(sql).contains("time zone displacement out of range"), sql);
        }
    }

    /** A fraction finer than a microsecond is rounded to one, and a tie goes to the even one. */
    @Test
    void aFinerFractionIsRoundedToTheMicrosecondTheTypeHolds() throws SQLException {
        assertEquals("2020-01-01 12:00:01+00",
                one("SELECT (timestamptz '2020-01-01 12:00:00.9999995+00')::text"));
        assertEquals("2020-01-01 12:00:00",
                one("SELECT (timestamp '2020-01-01 12:00:00.0000005')::text"));
        assertEquals("2020-01-01 12:00:00.000002",
                one("SELECT (timestamp '2020-01-01 12:00:00.0000015')::text"));
        assertEquals("12:00:01", one("SELECT (time '12:00:00.9999995')::text"));
    }

    // ---- a precision written on the type -------------------------------------------------

    /** A typed literal may carry the type's own precision, which rounds the value. */
    @Test
    void aTypedLiteralMayCarryItsPrecision() throws SQLException {
        assertEquals("2020-01-01 12:00:00.99",
                one("SELECT (timestamp(2) '2020-01-01 12:00:00.987654')::text"));
        assertEquals("2020-01-01 12:00:01",
                one("SELECT (timestamp(0) '2020-01-01 12:00:00.987654')::text"));
        assertEquals("12:00:00.99", one("SELECT (time(2) '12:00:00.987654')::text"));
        assertEquals("2020-01-01 12:00:00.988+00",
                one("SELECT (timestamptz(3) '2020-01-01 12:00:00.987654+00')::text"));
        assertEquals("12:00:01+00", one("SELECT (timetz(1) '12:00:00.987654+00')::text"));
        assertEquals("00:00:01.99", one("SELECT (interval(2) '1.987654 seconds')::text"));
        // A precision past the six digits the type holds is cut back to six, not refused.
        assertEquals("2020-01-01 00:00:00", one("SELECT (timestamp(7) '2020-01-01')::text"));
    }

    /** The four clock functions take a precision too, and each is the type it has always been. */
    @Test
    void theClockFunctionsTakeAPrecision() throws SQLException {
        assertEquals("timestamp with time zone",
                one("SELECT pg_typeof(current_timestamp(0))::text"));
        assertEquals("timestamp without time zone",
                one("SELECT pg_typeof(localtimestamp(2))::text"));
        assertEquals("time with time zone", one("SELECT pg_typeof(current_time)::text"));
        assertEquals("time with time zone", one("SELECT pg_typeof(current_time(2))::text"));
        assertEquals("time without time zone", one("SELECT pg_typeof(localtime)::text"));
        assertEquals("time without time zone", one("SELECT pg_typeof(localtime(2))::text"));
        assertEquals("true", one("SELECT (date_trunc('sec', current_timestamp(0))"
                + " = date_trunc('sec', current_timestamp(0)))::text"));
    }

    // ---- a date and a time of day --------------------------------------------------------

    /** The two halves of a timestamp put together make one, either way round. */
    @Test
    void aDateAndATimeMakeATimestamp() throws SQLException {
        assertEquals("2020-01-01 12:00:00",
                one("SELECT (date '2020-01-01' + time '12:00:00')::text"));
        assertEquals("timestamp without time zone",
                one("SELECT pg_typeof(date '2020-01-01' + time '12:00:00')::text"));
        assertEquals("2020-01-01 12:00:00",
                one("SELECT (time '12:00:00' + date '2020-01-01')::text"));
        assertEquals("2019-12-31 12:00:00",
                one("SELECT (date '2020-01-01' - time '12:00:00')::text"));
        assertEquals("2020-01-01 12:00:00+00",
                one("SELECT (date '2020-01-01' + timetz '12:00:00+00')::text"));
        assertEquals("timestamp with time zone",
                one("SELECT pg_typeof(date '2020-01-01' + timetz '12:00:00+00')::text"));
    }

    /** A clock with no displacement after it is a reading in the session's zone. */
    @Test
    void aClockWithNoDisplacementTakesTheSessionsOwn() throws SQLException {
        exec("SET TimeZone = 'America/New_York'");
        try {
            assertEquals("12:00:00-04", one("SELECT (timetz '12:00:00')::text"));
        } finally {
            exec("SET TimeZone = 'UTC'");
        }
    }

    // ---- made out of fields --------------------------------------------------------------

    /** A negative year names a BC year, and there is no year nought in either era. */
    @Test
    void aTimestampMadeOfFieldsHasNoYearNought() throws SQLException {
        assertEquals("0001-01-01 00:00:00 BC",
                one("SELECT make_timestamp(-1, 1, 1, 0, 0, 0)::text"));
        assertEquals("4713-01-01 00:00:00 BC",
                one("SELECT make_timestamp(-4713, 1, 1, 0, 0, 0)::text"));
        assertEquals("0001-01-01 BC", one("SELECT make_date(-1, 1, 1)::text"));
        assertEquals("22008", stateOf("SELECT make_timestamp(0, 1, 1, 0, 0, 0)"));
        assertEquals("22008", stateOf("SELECT make_date(0, 1, 1)"));
        // A zone that is not a zone is the caller's mistake, not an internal fault.
        assertEquals("22023",
                stateOf("SELECT make_timestamptz(2020, 1, 1, 0, 0, 0, 'Nowhere/Nothing')"));
    }

    // ---- a length is a value ---------------------------------------------------------------

    /** A total and a mean of lengths are lengths, and a time reaches both as one. */
    @Test
    void theAggregatesTotalAndHalveALength() throws SQLException {
        exec("CREATE TABLE zzi_span(i interval, t time)");
        try {
            exec("INSERT INTO zzi_span VALUES (interval '1 day', time '01:00'),"
                    + " (interval '3 days', time '03:00')");
            assertEquals("4 days", one("SELECT sum(i)::text FROM zzi_span"));
            assertEquals("2 days", one("SELECT avg(i)::text FROM zzi_span"));
            assertEquals("04:00:00", one("SELECT sum(t)::text FROM zzi_span"));
            assertEquals("02:00:00", one("SELECT avg(t)::text FROM zzi_span"));
            assertEquals("interval", one("SELECT pg_typeof(sum(i))::text FROM zzi_span"));
            assertEquals("interval", one("SELECT pg_typeof(avg(t))::text FROM zzi_span"));
            assertEquals("2 days",
                    one("SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY i))::text"
                            + " FROM zzi_span"));
            assertEquals("02:00:00",
                    one("SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY t))::text"
                            + " FROM zzi_span"));
            // Nothing to total is nothing, not a length of no time.
            assertEquals("true",
                    one("SELECT (sum(i) IS NULL)::text FROM zzi_span WHERE false"));
        } finally {
            exec("DROP TABLE zzi_span");
        }
    }

    /**
     * Dividing a length carries what is left of each unit down into the one below it, because a
     * month is a whole number of months: a third of a month is ten days, not a third of a month.
     */
    @Test
    void aMeanCarriesWhatIsLeftOfEachUnitDownward() throws SQLException {
        exec("CREATE TABLE zzi_mean(i interval)");
        try {
            exec("INSERT INTO zzi_mean VALUES (interval '1 day'), (interval '3 days'),"
                    + " (interval '1 mon 5 days 6 hours')");
            assertEquals("1 mon 9 days 06:00:00", one("SELECT sum(i)::text FROM zzi_mean"));
            assertEquals("13 days 02:00:00", one("SELECT avg(i)::text FROM zzi_mean"));
            exec("DELETE FROM zzi_mean");
            exec("INSERT INTO zzi_mean VALUES (interval '1 mon'), (interval '2 mons'),"
                    + " (interval '1 day')");
            assertEquals("1 mon 08:00:00", one("SELECT avg(i)::text FROM zzi_mean"));
        } finally {
            exec("DROP TABLE zzi_mean");
        }
    }

    // ---- the declared result type ----------------------------------------------------------

    /** A built-in answers with the type the catalogue declares for it, not with text. */
    @Test
    void aBuiltInAnswersWithItsDeclaredType() throws SQLException {
        assertEquals("timestamp without time zone",
                one("SELECT pg_typeof(date_bin(interval '1 day', timestamp '2020-01-01',"
                        + " timestamp '2020-01-01'))::text"));
        assertEquals("interval",
                one("SELECT pg_typeof(age(timestamp '2020-01-01', timestamp '2019-01-01'))::text"));
        assertEquals("interval", one("SELECT pg_typeof(justify_days(interval '1 day'))::text"));
        assertEquals("interval", one("SELECT pg_typeof(justify_hours(interval '1 day'))::text"));
        assertEquals("interval", one("SELECT pg_typeof(justify_interval(interval '1 day'))::text"));
        assertEquals("timestamp without time zone",
                one("SELECT pg_typeof(make_timestamp(2020,1,1,0,0,0))::text"));
    }

    /** date_trunc takes the zone it is told to truncate in, and refuses one that is not a zone. */
    @Test
    void truncatingHappensInTheZoneItIsGiven() throws SQLException {
        assertEquals("2020-06-15 04:00:00+00",
                one("SELECT date_trunc('day', timestamptz '2020-06-15 12:00:00+00',"
                        + " 'America/New_York')::text"));
        assertEquals("22023",
                stateOf("SELECT date_trunc('day', timestamptz '2020-06-15 12:00:00+00',"
                        + " 'Nowhere/Nothing')"));
    }
}
