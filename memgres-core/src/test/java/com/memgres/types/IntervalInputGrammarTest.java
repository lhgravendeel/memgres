package com.memgres.types;

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

/**
 * Everything PostgreSQL prints for an interval has to read back in, which means accepting
 * per-field signs, fractional quantities, bare time fields and the wide unit names.
 * Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>N27 interval input rejects PG's own output grammar.
 */
class IntervalInputGrammarTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** Parse the literal and return PG's text rendering of the resulting interval. */
    private static String iv(String literal) throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT (INTERVAL '" + literal + "')::text")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String expr(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Test
    void bareTimeFieldsParseAndHoursMayExceedADay() throws Exception {
        assertEquals("05:00:00", iv("5:00"));
        assertEquals("27:00:00", iv("27:00"));
        assertEquals("00:05:30", iv("0:05:30"));
    }

    @Test
    void fractionalQuantitiesSpillIntoTheNextSmallerUnit() throws Exception {
        assertEquals("1 year 6 mons", iv("1.5 years"));
        assertEquals("1 mon 15 days", iv("1.5 mons"));
        assertEquals("1 day 12:00:00", iv("1.5 days"));
        assertEquals("01:30:00", iv("1.5 hours"));
    }

    /** This is the shape PG prints for a mixed-sign interval, so it has to round-trip. */
    @Test
    void perFieldSignsRoundTrip() throws Exception {
        assertEquals("-1 mons +3 days", iv("-1 mons +3 days"));
        assertEquals("-1 days -02:00:00", iv("1 day 2 hours ago"));
    }

    @Test
    void isoDurationsMayCarryNegativeComponents() throws Exception {
        assertEquals("-06:00:00", iv("PT-6H"));
        assertEquals("1 year 2 mons 3 days", iv("P1Y2M3D"));
    }

    @Test
    void theWideUnitNamesAreAccepted() throws Exception {
        assertEquals("100 years", iv("1 century"));
        assertEquals("1000 years", iv("1 millennium"));
        assertEquals("20 years", iv("2 decades"));
    }

    @Test
    void arithmeticOnTheParsedValueMatches() throws Exception {
        assertEquals("3 days 08:00:00", expr("SELECT (INTERVAL '10 days' / 3)::text"));
        assertEquals("-1 mons -2 days", expr("SELECT (- INTERVAL '1 mon 2 days')::text"));
        assertEquals("2 days 17:00:00", expr("SELECT (INTERVAL '1 day 2 hours' * 2.5)::text"));
    }

    @Test
    void theSqlStandardShapesStillRead() throws Exception {
        assertEquals("1 year 2 mons", iv("1-2"));
        assertEquals("2 days 04:05:06", iv("2 04:05:06"));
    }

    /** A unit list is not a licence to accept nonsense. */
    @Test
    void nonIntervalTextIsStillRejected() {
        SQLException e = assertThrows(SQLException.class, () -> iv("3 fortnights"));
        assertEquals("22007", e.getSQLState());
    }
}
