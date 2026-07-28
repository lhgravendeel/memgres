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
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Queries that either never came back or grew until the heap did. {@code rpad} padded with a loop
 * that an empty fill string never advanced, so {@code rpad(x, n, sep)} with an empty separator
 * wedged its connection for good — no error, no timeout, and a hung build. The pad and repeat
 * family, the bit length modifier and the numeric paths all built results PostgreSQL refuses
 * outright, and generate_series filled its virtual table a row at a time, which is quadratic.
 * Expectations captured from a live PostgreSQL 18 server.
 */
class NonTerminatingOperationsTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (memgres != null) memgres.close();
    }

    /**
     * Each check gets its own connection. A statement that fails to terminate wedges the
     * connection it runs on, and a shared one would carry that stall into every later check.
     */
    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static String scalar(String sql) throws SQLException {
        try (Connection c = open();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) { s.execute(sql); }
    }

    /** Run {@code sql} under a watchdog, so a non-terminating result fails rather than hangs. */
    private static void assertScalar(String expected, String sql) {
        assertTimeoutPreemptively(Duration.ofSeconds(20), () ->
                assertEquals(expected, scalar(sql), sql));
    }

    private static void assertFails(String expectedState, String expectedMessage, String sql) {
        assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
            SQLException e = assertThrows(SQLException.class, () -> scalar(sql), sql);
            assertEquals(expectedState, e.getSQLState(),
                    "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
            assertTrue(e.getMessage().contains(expectedMessage),
                    "expected \"" + expectedMessage + "\" in: " + e.getMessage());
        });
    }

    // ---- an empty fill string ----

    @Test
    void padWithAnEmptyFillReturnsTheInputRatherThanLooping() {
        assertScalar("abc", "SELECT rpad('abc', 10, '')");
        assertScalar("abc", "SELECT lpad('abc', 10, '')");
        assertScalar("ab", "SELECT rpad('abc', 2, '')");
        assertScalar("ab", "SELECT lpad('abc', 2, '')");
    }

    @Test
    void anyExpressionThatEvaluatesToAnEmptyFillBehavesTheSame() {
        assertScalar("abc", "SELECT rpad('abc', 10, substr('a', 2))");
        assertScalar("abc", "SELECT rpad('abc', 10, '' || '')");
        assertScalar("abc", "SELECT rpad('abc', 10, repeat('z', 0))");
        assertScalar("abc", "SELECT rpad('abc', 10, left('x', 0))");
        assertScalar("abc", "SELECT lpad('abc', 10, left('x', 0))");
    }

    /** Nothing will be built, so the length ceiling does not apply. */
    @Test
    void anEmptyFillAlsoLiftsTheLengthCeiling() {
        assertScalar("abc", "SELECT rpad('abc', 400000000, '')");
        assertScalar("abc", "SELECT lpad('abc', 400000000, '')");
    }

    /** The realistic trigger is a separator column that happens to hold ''. */
    @Test
    void anEmptyFillFromAColumnBehavesTheSame() throws Exception {
        assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
            try (Connection c = open()) {
                exec(c, "CREATE TABLE nto_pad (s text, n int, sep text)");
                exec(c, "INSERT INTO nto_pad VALUES ('abc', 10, ''), ('abc', 10, '-')");
                try (Statement s = c.createStatement();
                     ResultSet rs = s.executeQuery(
                             "SELECT rpad(s, n, sep) FROM nto_pad ORDER BY sep")) {
                    assertTrue(rs.next());
                    assertEquals("abc", rs.getString(1));
                    assertTrue(rs.next());
                    assertEquals("abc-------", rs.getString(1));
                }
                exec(c, "DROP TABLE nto_pad");
            }
        });
    }

    // ---- results wider than a single allocation ----

    @Test
    void aPadWiderThanOneAllocationIsRefused() {
        assertFails("54000", "requested length too large",
                "SELECT length(lpad('abc', 400000000, 'x'))");
        assertFails("54000", "requested length too large",
                "SELECT length(rpad('abc', 400000000, 'x'))");
        assertFails("54000", "requested length too large",
                "SELECT length(lpad('abc', 1500000000, 'x'))");
        assertFails("54000", "requested length too large",
                "SELECT length(rpad('abc', 2000000000, 'x'))");
        // the two-argument form pads with a space, so it has no empty-fill escape
        assertFails("54000", "requested length too large",
                "SELECT lpad('abcdefghij', 400000000)");
    }

    @Test
    void aRepeatWiderThanOneAllocationIsRefused() {
        // the count times the string's own byte width is what has to fit
        assertFails("54000", "requested length too large",
                "SELECT length(repeat('ab', 1500000000))");
        assertFails("54000", "requested length too large",
                "SELECT length(repeat('ab', 600000000))");
        assertFails("54000", "requested length too large",
                "SELECT length(repeat('abcdefghij', 200000000))");
        // a two-byte character counts double, as PostgreSQL counts it
        assertFails("54000", "requested length too large",
                "SELECT length(repeat(chr(233), 600000000))");
    }

    @Test
    void sizesThatFitAreUnaffected() {
        assertScalar("abcxyx", "SELECT rpad('abc', 6, 'xy')");
        assertScalar("xyxabc", "SELECT lpad('abc', 6, 'xy')");
        assertScalar("ab", "SELECT rpad('abc', 2, 'x')");
        assertScalar("ab", "SELECT lpad('abc', 2, 'x')");
        assertScalar("abc", "SELECT lpad('abc', 3, 'x')");
        assertScalar("", "SELECT rpad('abc', 0, 'x')");
        assertScalar("", "SELECT rpad('abc', -1, 'x')");
        assertScalar("", "SELECT lpad('abc', -1, 'x')");
        assertScalar("abc     ", "SELECT rpad('abc', 8)");
        assertScalar("     abc", "SELECT lpad('abc', 8)");
        assertScalar("ababa", "SELECT rpad('', 5, 'ab')");
        assertScalar("abcdcd", "SELECT rpad('ab', 6, 'cd')");
        assertScalar("2000", "SELECT length(repeat('ab', 1000))");
        assertScalar("100000", "SELECT length(lpad('abc', 100000, 'x'))");
        assertScalar("ab", "SELECT repeat('ab', 1)");
        assertScalar("", "SELECT repeat('', 5)");
    }

    @Test
    void aNullFillIsStillNull() throws Exception {
        assertNull(scalar("SELECT rpad('abc', 10, NULL)"));
    }

    // ---- type modifiers ----

    @Test
    void aBitLengthModifierIsBoundedBeforeAnythingIsBuilt() {
        assertFails("22023", "length for type bit cannot exceed 83886080",
                "SELECT '0'::bit(200000000)");
        assertFails("22023", "length for type bit cannot exceed 83886080",
                "SELECT '0'::bit(83886081)");
        assertFails("22023", "length for type bit cannot exceed 83886080",
                "SELECT 42::bit(200000000)");
        // PostgreSQL names varbit by its internal spelling, whichever syntax was written
        assertFails("22023", "length for type varbit cannot exceed 83886080",
                "SELECT '0'::varbit(200000000)");
        assertFails("22023", "length for type varbit cannot exceed 83886080",
                "SELECT '0'::bit varying(200000000)");
        assertFails("22023", "length for type bit must be at least 1", "SELECT '0'::bit(0)");
        // a modifier is an int4, so one too wide is a bad integer before it is a length
        assertFails("22003", "value \"99999999999\" is out of range for type integer",
                "SELECT '0'::bit(99999999999)");
    }

    @Test
    void bitModifiersWithinRangeAreUnaffected() {
        assertScalar("0", "SELECT '0'::bit(1)");
        assertScalar("10100000", "SELECT '101'::bit(8)");
        assertScalar("10", "SELECT '101'::varbit(2)");
        assertScalar("10100", "SELECT B'101'::bit(5)");
        assertScalar("00101010", "SELECT 42::bit(8)");
    }

    // ---- numeric's own limits ----

    @Test
    void factorialStopsAtTheWidestResultNumericHolds() {
        assertFails("22003", "value overflows numeric format", "SELECT factorial(50000)");
        assertFails("22003", "value overflows numeric format", "SELECT factorial(32178)");
        // a negative argument is out of range too, not a data exception of its own
        assertFails("22003", "factorial of a negative number is undefined", "SELECT factorial(-1)");
    }

    @Test
    void factorialsThatFitAreUnaffected() {
        assertScalar("2432902008176640000", "SELECT factorial(20)");
        assertScalar("1", "SELECT factorial(0)");
        assertScalar("2568", "SELECT length(factorial(1000)::text)");
    }

    @Test
    void aCastCannotProduceANumericWiderThanTheFormatAllows() {
        assertFails("22003", "value overflows numeric format", "SELECT '1e200000'::numeric");
        assertFails("22003", "value overflows numeric format", "SELECT '1e200000'::numeric + 1");
        assertFails("22003", "value overflows numeric format", "SELECT '1e131072'::numeric + 1");
        assertFails("22003", "value overflows numeric format", "SELECT (-1e200000)::numeric");
        assertFails("22003", "value overflows numeric format", "SELECT '1e200000'::decimal");
        // the same limit applies below the decimal point
        assertFails("22003", "value overflows numeric format", "SELECT (1e-16384)::numeric");
    }

    @Test
    void numericsWithinRangeAreUnaffected() {
        assertScalar("t", "SELECT '1e131071'::numeric IS NOT NULL");
        assertScalar("t", "SELECT (1e-16383)::numeric IS NOT NULL");
        assertScalar("123.456", "SELECT '123.456'::numeric");
        assertScalar("12", "SELECT '  12  '::numeric");
        assertScalar("1.5", "SELECT 1.5::numeric");
        assertScalar("-0.5", "SELECT (-0.5)::numeric");
        assertScalar("1000000.00", "SELECT 1000000::numeric(10,2)");
        assertScalar("12345.678", "SELECT 12345.678::numeric(8,3)");
        assertScalar("1024.0000000000000", "SELECT 2::numeric ^ 10");
    }

    // ---- generate_series ----

    @Test
    void aWideIntegerSeriesFinishes() {
        assertScalar("5000000", "SELECT count(*) FROM generate_series(1, 5000000)");
        assertScalar("1000000", "SELECT max(g) FROM generate_series(1, 1000000) g");
        assertScalar("200000", "SELECT count(*) FROM generate_series(1.0, 200000.0, 1)");
    }

    /** The last step of a series that reaches bigint's edge wraps; the loop must stop there. */
    @Test
    void aSeriesAtTheEdgeOfBigintTerminates() {
        assertTimeoutPreemptively(Duration.ofSeconds(20), () -> {
            try (Connection c = open();
                 Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM generate_series("
                         + "9223372036854775805, 9223372036854775807)")) {
                assertTrue(rs.next());
                assertEquals("9223372036854775805", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("9223372036854775806", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("9223372036854775807", rs.getString(1));
                assertTrue(!rs.next(), "the series must stop at bigint's edge");
            }
        });
    }

    @Test
    void smallSeriesKeepTheirShape() {
        assertScalar("5050", "SELECT sum(g) FROM generate_series(1, 100) g");
        assertScalar("0", "SELECT count(*) FROM generate_series(1, 0)");
        assertScalar("5", "SELECT count(*) FROM generate_series("
                + "'2020-01-01'::date, '2020-01-05'::date, '1 day')");
        assertScalar("3", "SELECT count(*) FROM generate_series(1.0, 2.0, 0.5)");
        assertFails("22023", "step size cannot equal zero",
                "SELECT count(*) FROM generate_series(1, 3, 0)");
    }
}
