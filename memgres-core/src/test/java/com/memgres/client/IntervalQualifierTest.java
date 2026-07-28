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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The field qualifier and precision an interval type may carry, and PG 17's AT LOCAL.
 *
 * <p>An interval type is not just "interval": INTERVAL '3' DAY is three days where a bare
 * INTERVAL '3' is three seconds, INTERVAL '5' DAY TO HOUR is five <em>hours</em> because an
 * unlabelled number takes the qualifier's last field, and SECOND(3) keeps three fractional
 * digits and rounds the rest away. All of that was parsed and then discarded, so every qualified
 * literal read as seconds. AT LOCAL — AT TIME ZONE against the session's own zone — was a syntax
 * error, and timetz did not move under either spelling because memgres holds one as text.
 *
 * <p>Every expectation below was measured against PostgreSQL 18 with TimeZone=UTC.
 */
class IntervalQualifierTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("SET TimeZone = 'UTC'");
        exec("CREATE TABLE ivq_t (id int, d interval, ts timestamptz, tsn timestamp, "
                + "tz timetz, iv3 interval(3))");
        exec("INSERT INTO ivq_t VALUES (1, interval '3 days', "
                + "timestamptz '2001-02-16 20:38:40-05', timestamp '2001-02-16 20:38:40', "
                + "timetz '10:00:00+02', interval '1.234567 seconds')");
        exec("INSERT INTO ivq_t VALUES (2, NULL, NULL, NULL, NULL, NULL)");
        exec("CREATE VIEW ivq_v AS SELECT id, d, ts AT LOCAL AS tsl, iv3 FROM ivq_t");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertScalar(String expected, String sql) throws SQLException {
        assertEquals(expected, scalar(sql), sql);
    }

    private static void assertFails(String sqlState, String message, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> scalar(sql), sql);
        assertEquals(sqlState, e.getSQLState(), sql);
        assertTrue(e.getMessage().contains(message),
                sql + " -> " + e.getMessage() + " (wanted " + message + ")");
    }

    // ---- a single-field qualifier names the unit of an unlabelled number ------------------

    @Test
    void aBareNumberTakesTheUnitTheQualifierNames() throws Exception {
        assertScalar("3 years", "SELECT INTERVAL '3' YEAR");
        assertScalar("3 mons", "SELECT INTERVAL '3' MONTH");
        assertScalar("3 days", "SELECT INTERVAL '3' DAY");
        assertScalar("03:00:00", "SELECT INTERVAL '3' HOUR");
        assertScalar("00:03:00", "SELECT INTERVAL '3' MINUTE");
        assertScalar("00:00:03", "SELECT INTERVAL '3' SECOND");
        assertScalar("00:00:03", "SELECT INTERVAL '3'");
    }

    @Test
    void theSignTravelsWithTheNumberNotTheUnit() throws Exception {
        assertScalar("-3 days", "SELECT INTERVAL '-3' DAY");
        assertScalar("-03:00:00", "SELECT INTERVAL '-3' HOUR");
        assertScalar("-3 years", "SELECT INTERVAL '-3' YEAR");
    }

    @Test
    void aFractionSpillsIntoTheNextFieldAndThenTheQualifierDropsIt() throws Exception {
        // 1.5 years is a year and six months, but a YEAR qualifier cannot hold the months
        assertScalar("1 year", "SELECT INTERVAL '1.5' YEAR");
        assertScalar("1 mon", "SELECT INTERVAL '1.5' MONTH");
        assertScalar("1 day", "SELECT INTERVAL '1.5' DAY");
        assertScalar("01:00:00", "SELECT INTERVAL '1.5' HOUR");
        assertScalar("00:01:00", "SELECT INTERVAL '1.5' MINUTE");
        assertScalar("00:00:01.5", "SELECT INTERVAL '1.5' SECOND");
        assertScalar("00:00:00", "SELECT INTERVAL '0.5' DAY");
    }

    // ---- a ranged qualifier: the number takes the range's LAST field ----------------------

    @Test
    void aRangedQualifierGivesABareNumberItsLeastSignificantField() throws Exception {
        assertScalar("3 mons", "SELECT INTERVAL '3' YEAR TO MONTH");
        assertScalar("1 year 8 mons", "SELECT INTERVAL '20' YEAR TO MONTH");
        assertScalar("05:00:00", "SELECT INTERVAL '5' DAY TO HOUR");
        assertScalar("00:10:00", "SELECT INTERVAL '10' DAY TO MINUTE");
        assertScalar("00:00:10", "SELECT INTERVAL '10' DAY TO SECOND");
        assertScalar("00:10:00", "SELECT INTERVAL '10' HOUR TO MINUTE");
        assertScalar("00:00:10", "SELECT INTERVAL '10' HOUR TO SECOND");
        assertScalar("00:00:10", "SELECT INTERVAL '10' MINUTE TO SECOND");
    }

    @Test
    void twoNumbersSpellTheSqlStandardDayAndHour() throws Exception {
        assertScalar("1 day 02:00:00", "SELECT INTERVAL '1 2' DAY TO HOUR");
        assertScalar("1 day 02:00:00", "SELECT INTERVAL '1 2' HOUR");
        assertScalar("-1 days -02:00:00", "SELECT INTERVAL '-1 -2' DAY TO HOUR");
    }

    @Test
    void aLiteralThatFillsAFieldTwiceIsRejectedTheWayPostgresRejectsIt() {
        assertFails("22007", "invalid input syntax for type interval", "SELECT INTERVAL '1 2' DAY");
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT INTERVAL '1 2 3' DAY TO HOUR");
    }

    @Test
    void minuteToSecondRereadsATwoPartTimeFieldAsMinutesAndSeconds() throws Exception {
        assertScalar("00:02:03", "SELECT INTERVAL '2:03' MINUTE TO SECOND");
        assertScalar("1 day 00:02:03", "SELECT INTERVAL '1 2:03' MINUTE TO SECOND");
        // three parts are already hours, minutes and seconds, so nothing shifts
        assertScalar("02:03:04", "SELECT INTERVAL '2:03:04' MINUTE TO SECOND");
        // and no other qualifier moves the field
        assertScalar("02:03:00", "SELECT INTERVAL '2:03' HOUR TO MINUTE");
        assertScalar("02:03:00", "SELECT INTERVAL '2:03' SECOND");
        assertScalar("02:00:00", "SELECT INTERVAL '2:03' DAY TO HOUR");
    }

    // ---- the qualifier also drops the fields it does not reach ----------------------------

    @Test
    void theQualifierDropsEverythingBelowItsLastField() throws Exception {
        String full = "'1 year 2 months 3 days 4:05:06'";
        assertScalar("1 year", "SELECT INTERVAL " + full + " YEAR");
        assertScalar("1 year 2 mons", "SELECT INTERVAL " + full + " MONTH");
        assertScalar("1 year 2 mons 3 days", "SELECT INTERVAL " + full + " DAY");
        assertScalar("1 year 2 mons 3 days 04:00:00", "SELECT INTERVAL " + full + " HOUR");
        assertScalar("1 year 2 mons 3 days 04:05:00", "SELECT INTERVAL " + full + " MINUTE");
        assertScalar("1 year 2 mons 3 days 04:05:06", "SELECT INTERVAL " + full + " SECOND");
        assertScalar("1 year 2 mons", "SELECT INTERVAL " + full + " YEAR TO MONTH");
        assertScalar("1 year 2 mons 3 days 04:05:00", "SELECT INTERVAL " + full + " HOUR TO MINUTE");
        assertScalar("1 day", "SELECT INTERVAL '1 day 2 hours' DAY");
        assertScalar("1 day 02:00:00", "SELECT INTERVAL '1 day 2 hours' HOUR");
        assertScalar("00:00:00", "SELECT INTERVAL '1 day 2 hours' YEAR");
    }

    @Test
    void aLabelledLiteralKeepsItsOwnUnitsWhateverTheQualifierSays() throws Exception {
        assertScalar("1 day", "SELECT INTERVAL '1 day' MINUTE TO SECOND");
        assertScalar("3 days 04:05:00", "SELECT INTERVAL '3 days 4 hours 5 minutes' MINUTE");
        assertScalar("3 days", "SELECT INTERVAL '3 days 4 hours 5 minutes' DAY");
        assertScalar("1 year 2 mons", "SELECT INTERVAL '1-2' SECOND");
        assertScalar("1 year", "SELECT INTERVAL '1-2' YEAR");
    }

    // ---- fractional-seconds precision ------------------------------------------------------

    @Test
    void secondPrecisionRoundsTheFractionAwayFromZero() throws Exception {
        assertScalar("00:00:01.235", "SELECT INTERVAL '1.234567 seconds' SECOND(3)");
        assertScalar("00:00:01.235", "SELECT INTERVAL '1.234567' SECOND(3)");
        assertScalar("00:00:02", "SELECT INTERVAL '1.5' SECOND(0)");
        assertScalar("00:00:01.5", "SELECT INTERVAL '1.4999' SECOND(2)");
        assertScalar("-00:00:01.5", "SELECT INTERVAL '-1.4999' SECOND(2)");
        assertScalar("-00:00:02", "SELECT INTERVAL '-1.5' SECOND(0)");
        assertScalar("00:00:01.5", "SELECT INTERVAL '1.5' SECOND(3)");
        // six digits is all an interval holds, so a wider request changes nothing
        assertScalar("00:00:01", "SELECT INTERVAL '1' SECOND(7)");
    }

    @Test
    void aRangedQualifierCarriesThePrecisionOnItsLastField() throws Exception {
        assertScalar("1 day 02:03:04.57", "SELECT INTERVAL '1 2:03:04.56789' DAY TO SECOND(2)");
        assertScalar("1 day 02:03:04.6", "SELECT INTERVAL '1 2:03:04.5678' HOUR TO SECOND(1)");
        assertScalar("1 day 02:03:04", "SELECT INTERVAL '1 2:03:04' DAY TO SECOND(2)");
    }

    @Test
    void everySpellingOfAPrecisedIntervalTypeRoundsTheSameWay() throws Exception {
        assertScalar("00:00:01.235", "SELECT CAST('1.234567 seconds' AS interval(3))");
        assertScalar("00:00:01.235", "SELECT '1.234567 seconds'::interval(3)");
        assertScalar("00:00:01.235", "SELECT CAST('1.234567 seconds' AS interval second(3))");
        assertScalar("00:00:01.235", "SELECT interval(3) '1.234567 seconds'");
        assertScalar("00:00:02", "SELECT interval(0) '1.5 seconds'");
    }

    @Test
    void thePrecisionAlsoReachesAValueThatIsAlreadyAnInterval() throws Exception {
        assertScalar("00:00:01.235", "SELECT (INTERVAL '1.234567 seconds')::interval(3)");
        assertScalar("00:00:01", "SELECT CAST(interval '1.234567' AS interval(0))");
        assertScalar("-00:00:02", "SELECT CAST(interval '-1.5' AS interval(0))");
        assertScalar("1 day 02:03:04.57",
                "SELECT CAST(INTERVAL '1 2:03:04.56789' AS interval day to second(2))");
    }

    @Test
    void anIntervalColumnKeepsOnlyTheDigitsItsTypeDeclares() throws Exception {
        assertScalar("00:00:01.235", "SELECT iv3::text FROM ivq_t WHERE id = 1");
    }

    // ---- shapes the qualifier must not disturb ---------------------------------------------

    @Test
    void aQualifierDoesNotDisturbTheShapesThatCarryTheirOwnUnits() throws Exception {
        assertScalar("infinity", "SELECT INTERVAL 'infinity' DAY");
        assertScalar("-infinity", "SELECT INTERVAL '-infinity' SECOND(2)");
        assertScalar("-1 days", "SELECT INTERVAL '1 day ago' DAY");
        assertScalar("1 day 02:00:00", "SELECT INTERVAL '@ 1 day 2 hours' HOUR");
        assertScalar("1 year 2 mons 3 days 04:05:06.79",
                "SELECT INTERVAL 'P1Y2M3DT4H5M6.789S' SECOND(2)");
        assertScalar("1 year 2 mons 3 days", "SELECT INTERVAL 'P0001-02-03T04:05:06' DAY");
        assertScalar("1000000000 days", "SELECT INTERVAL '1000000000 days' DAY");
    }

    @Test
    void wordsThatAreNotIntervalFieldsStayColumnAliases() throws Exception {
        // WEEK and MILLENNIUM are units, but not qualifiers: PG reads them as the output name
        assertScalar("00:00:01.5", "SELECT INTERVAL '1.5' WEEK");
        assertScalar("00:00:05", "SELECT INTERVAL '5' MILLENNIUM");
        assertScalar("1", "SELECT 1 AS second");
        assertScalar("1", "SELECT 1 AS local");
        assertScalar("1 day", "SELECT interval '1 day' AS interval");
    }

    @Test
    void unqualifiedIntervalsAreUnchanged() throws Exception {
        assertScalar("1 day", "SELECT INTERVAL '1 day'");
        assertScalar("1 day 02:03:00", "SELECT INTERVAL '1 day 2 hours 3 minutes'");
        assertScalar("04:05:06", "SELECT INTERVAL '04:05:06'");
        assertScalar("00:00:01.234567", "SELECT INTERVAL '1.234567'");
        assertScalar("1 year 2 mons", "SELECT INTERVAL '1-2'");
        assertScalar("2 days 04:05:06", "SELECT INTERVAL '2 04:05:06'");
        assertScalar("1 year 2 mons 3 days 04:05:06", "SELECT INTERVAL 'P1Y2M3DT4H5M6S'");
        assertScalar("-1 mons +3 days", "SELECT INTERVAL '-1 mons +3 days'");
        assertScalar("1 day", "SELECT '1 day'::interval");
        assertScalar("1 day", "SELECT CAST('1 day' AS interval)");
        assertScalar("1 day 02:00:00", "SELECT INTERVAL '1 day' + INTERVAL '2 hours'");
    }

    @Test
    void aQualifiedIntervalIsStillAnOrdinaryIntervalAfterwards() throws Exception {
        assertScalar("2001-01-04 00:00:00", "SELECT date '2001-01-01' + INTERVAL '3' DAY");
        assertScalar("2001-01-01 03:00:00",
                "SELECT timestamp '2001-01-01 00:00:00' + INTERVAL '3' HOUR");
        assertScalar("3 days 03:00:00", "SELECT INTERVAL '3' DAY + INTERVAL '3' HOUR");
        assertScalar("1 day 06:00:00", "SELECT justify_hours(INTERVAL '30' HOUR)");
        assertScalar("t", "SELECT INTERVAL '3' DAY = INTERVAL '3 days'");
        assertScalar("t", "SELECT INTERVAL '3' DAY > INTERVAL '2 days'");
        assertScalar("3", "SELECT extract(day from INTERVAL '3' DAY)");
        assertScalar("3", "SELECT extract(hour from INTERVAL '3' HOUR)");
        assertScalar("interval", "SELECT pg_typeof(INTERVAL '3' DAY)::text");
        assertScalar("interval", "SELECT pg_typeof(INTERVAL '3' DAY TO SECOND(2))::text");
        assertScalar("interval", "SELECT pg_typeof(CAST('1' AS interval(3)))::text");
    }

    @Test
    void nullSurvivesEveryIntervalSpelling() throws Exception {
        assertEquals(null, scalar("SELECT NULL::interval"));
        assertEquals(null, scalar("SELECT (NULL::text)::interval"));
        assertEquals(null, scalar("SELECT (NULL::text)::interval(3)"));
    }

    // ---- AT LOCAL ---------------------------------------------------------------------------

    @Test
    void atLocalConvertsAgainstTheSessionZone() throws Exception {
        assertScalar("2001-02-17 01:38:40",
                "SELECT timestamptz '2001-02-16 20:38:40-05' AT LOCAL");
        assertScalar("2001-02-16 20:38:40+00",
                "SELECT timestamp '2001-02-16 20:38:40' AT LOCAL");
        assertScalar("08:00:00+00", "SELECT (timetz '10:00:00+02') AT LOCAL");
        assertScalar("10:00:00+00", "SELECT time '10:00:00' AT LOCAL");
        assertScalar("2001-01-01 00:00:00", "SELECT date '2001-01-01' AT LOCAL");
    }

    @Test
    void atLocalReportsTheZonedOrZonelessSpellingOfItsInputType() throws Exception {
        assertScalar("timestamp without time zone",
                "SELECT pg_typeof(timestamptz '2001-02-16 20:38:40-05' AT LOCAL)::text");
        assertScalar("timestamp with time zone",
                "SELECT pg_typeof(timestamp '2001-02-16 20:38:40' AT LOCAL)::text");
        assertScalar("time with time zone",
                "SELECT pg_typeof((timetz '10:00:00+02') AT LOCAL)::text");
    }

    @Test
    void atLocalChains() throws Exception {
        assertScalar("2001-02-17 01:38:40+00",
                "SELECT timestamptz '2001-02-16 20:38:40-05' AT LOCAL AT TIME ZONE 'UTC'");
    }

    @Test
    void atLocalOnNullIsNull() throws Exception {
        assertEquals(null, scalar("SELECT (NULL::timestamptz) AT LOCAL"));
        assertEquals(null, scalar("SELECT (NULL::timestamp) AT LOCAL"));
        assertEquals(null, scalar("SELECT (NULL::timetz) AT LOCAL"));
    }

    @Test
    void atLocalFollowsTheSessionZoneWhenItChanges() throws Exception {
        try {
            exec("SET TimeZone = 'Europe/Amsterdam'");
            assertScalar("2001-02-17 02:38:40",
                    "SELECT timestamptz '2001-02-16 20:38:40-05' AT LOCAL");
            assertScalar("2001-02-16 20:38:40+01",
                    "SELECT timestamp '2001-02-16 20:38:40' AT LOCAL");
        } finally {
            exec("SET TimeZone = 'UTC'");
        }
    }

    @Test
    void atTimeZoneStillMovesTheZonedAndZonelessTimestampsItAlwaysDid() throws Exception {
        assertScalar("2001-02-17 01:38:40",
                "SELECT timestamptz '2001-02-16 20:38:40-05' AT TIME ZONE 'UTC'");
        assertScalar("2001-02-16 20:38:40+00",
                "SELECT timestamp '2001-02-16 20:38:40' AT TIME ZONE 'UTC'");
        assertScalar("2001-02-17 02:38:40",
                "SELECT timestamptz '2001-02-16 20:38:40-05' AT TIME ZONE 'Europe/Amsterdam'");
    }

    @Test
    void atTimeZoneNowMovesATimeToo() throws Exception {
        assertScalar("08:00:00+00", "SELECT (timetz '10:00:00+02') AT TIME ZONE 'UTC'");
        assertScalar("10:00:00+00", "SELECT time '10:00:00' AT TIME ZONE 'UTC'");
    }

    // ---- a date literal may name an offset with no time of day -----------------------------

    @Test
    void aDateOnlyLiteralMayCarryAnOffset() throws Exception {
        assertScalar("2001-01-01 00:00:00+00", "SELECT timestamptz '2001-01-01+00'");
        assertScalar("2000-12-31 22:00:00+00", "SELECT timestamptz '2001-01-01+02'");
        assertScalar("2000-12-31 18:30:00+00", "SELECT timestamptz '2001-01-01+05:30'");
        assertScalar("2000-12-31 22:00:00+00", "SELECT timestamptz '2001-01-01+2'");
        assertScalar("2001-01-01 00:00:00+00", "SELECT timestamptz '2001-01-01 +00'");
        assertScalar("2000-12-31 22:00:00+00", "SELECT timestamptz '2001-01-01 +2'");
        // a negative offset needs the space: without one the '-' is another date field
        assertScalar("2001-01-01 05:00:00+00", "SELECT timestamptz '2001-01-01 -05'");
        assertFails("22007", "invalid input syntax for type timestamp with time zone",
                "SELECT timestamptz '2001-01-01-05'");
    }

    @Test
    void aZonelessTypeReadsThatOffsetAndThenDiscardsIt() throws Exception {
        assertScalar("2001-01-01 00:00:00", "SELECT timestamp '2001-01-01+00'");
        assertScalar("2001-01-01 00:00:00", "SELECT timestamp '2001-01-01+02'");
        assertScalar("2001-01-01 00:00:00", "SELECT timestamp '2001-01-01 +02'");
        assertScalar("2001-01-01", "SELECT date '2001-01-01+02'");
    }

    @Test
    void theOrdinaryTimestampLiteralsAreUnchanged() throws Exception {
        assertScalar("2001-01-01", "SELECT date '2001-01-01'");
        assertScalar("2001-01-01 12:00:00", "SELECT timestamp '2001-01-01 12:00:00+02'");
        assertFails("22008", "date/time field value out of range",
                "SELECT timestamptz '2024-02-30'");
        assertFails("22007", "invalid input syntax for type timestamp with time zone",
                "SELECT timestamptz 'garbage'");
    }

    // ---- the same operators against real columns, a view and a subquery ---------------------

    @Test
    void qualifiedIntervalsAndAtLocalWorkAgainstStoredRows() throws Exception {
        assertScalar("3 days", "SELECT d::text FROM ivq_t WHERE id = 1");
        assertScalar("2001-02-17 01:38:40", "SELECT (ts AT LOCAL)::text FROM ivq_t WHERE id = 1");
        assertScalar("2001-02-16 20:38:40+00",
                "SELECT (tsn AT LOCAL)::text FROM ivq_t WHERE id = 1");
        assertScalar("08:00:00+00", "SELECT (tz AT LOCAL)::text FROM ivq_t WHERE id = 1");
        assertEquals(null, scalar("SELECT (ts AT LOCAL)::text FROM ivq_t WHERE id = 2"));
        assertEquals(null, scalar("SELECT (tz AT LOCAL)::text FROM ivq_t WHERE id = 2"));
    }

    @Test
    void aViewCanBeDefinedOverAtLocal() throws Exception {
        assertScalar("2001-02-17 01:38:40", "SELECT tsl::text FROM ivq_v WHERE id = 1");
        assertScalar("00:00:01.235", "SELECT iv3::text FROM ivq_v WHERE id = 1");
    }

    @Test
    void aDerivedColumnCarriesTheIntervalThroughASubquery() throws Exception {
        assertScalar("3 days",
                "SELECT sub.x::text FROM (SELECT d AS x FROM ivq_t WHERE id = 1) sub");
    }

    @Test
    void aQualifiedIntervalWorksInWhereGroupByAndOrderBy() throws Exception {
        assertScalar("1", "SELECT id FROM ivq_t WHERE d >= INTERVAL '1' DAY ORDER BY id");
        assertScalar("1", "SELECT id FROM ivq_t WHERE ts AT LOCAL > timestamp '2000-01-01'");
        assertScalar("3 days", "SELECT d::text FROM ivq_t GROUP BY d ORDER BY 1");
        assertScalar("2001-02-17 01:38:40",
                "SELECT (ts AT LOCAL)::text FROM ivq_t GROUP BY ts AT LOCAL ORDER BY 1");
        assertScalar("1", "SELECT id FROM ivq_t ORDER BY d NULLS LAST");
        assertScalar("1", "SELECT id FROM ivq_t ORDER BY ts AT LOCAL NULLS LAST");
    }
}
