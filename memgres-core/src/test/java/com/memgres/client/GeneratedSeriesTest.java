package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A series is read rather than held.
 *
 * <p>Every row of {@code generate_series} follows from the first one and the step, so the rows can
 * be worked out as they are read and counted without being produced at all. Building the whole
 * list first cost a five-million-row series five million boxed values and five million row arrays
 * before the query had looked at one of them — a {@code LIMIT 1} paid for all five million — and a
 * series longer than memgres was willing to hold was refused outright, where PostgreSQL answers it.
 *
 * <p>A five-million-row fixture is an ordinary thing to write in a test, which is what makes this
 * worth being about time as well as about answers: the timings below are generous enough not to
 * be flaky and tight enough that a series being built up front would fail them.
 */
class GeneratedSeriesTest {

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

    private static String rows(String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                if (sb.length() > 0) sb.append(", ");
                sb.append(rs.getString(1));
            }
        }
        return sb.toString();
    }

    /** How long a statement took, in milliseconds. */
    private static long millisOf(String sql) throws SQLException {
        long start = System.nanoTime();
        scalar(sql);
        return (System.nanoTime() - start) / 1_000_000L;
    }

    // ---------------------------------------------------------------- what it answers

    @Test
    void aSeriesAnswersTheRowsItSpans() throws Exception {
        assertEquals("1, 2, 3", rows("SELECT g::text FROM generate_series(1, 3) g"));
        assertEquals("5, 3, 1", rows("SELECT g::text FROM generate_series(5, 1, -2) g"));
        assertEquals("", rows("SELECT g::text FROM generate_series(1, 0) g"));
        assertEquals("6", scalar("SELECT sum(g)::text FROM generate_series(1, 3) g"));
    }

    /** A series over bigint runs to the edge of the type without running past it. */
    @Test
    void aSeriesStopsAtTheEndOfItsType() throws Exception {
        assertEquals("9223372036854775805, 9223372036854775806, 9223372036854775807",
                rows("SELECT g::text FROM generate_series("
                        + "9223372036854775805::int8, 9223372036854775807) g"));
    }

    /** Each numeric value keeps the scale that adding the step that many times keeps. */
    @Test
    void aNumericSeriesKeepsTheScaleAddingWouldKeep() throws Exception {
        assertEquals("1.00, 1.25, 1.50, 1.75, 2.00",
                rows("SELECT g::text FROM generate_series(1.00::numeric, 2, 0.25) g"));
    }

    /**
     * A step of fixed length is the same length wherever it is applied, and a step of months is
     * not: a month added twice is not two months added once. The second is still worked out a row
     * at a time, and answers what it always did.
     */
    @Test
    void aTemporalSeriesStepsTheWayTheStepIsWritten() throws Exception {
        assertEquals("2020-01-01 00:00:00, 2020-01-01 07:00:00, 2020-01-01 14:00:00, "
                        + "2020-01-01 21:00:00",
                rows("SELECT g::text FROM generate_series("
                        + "'2020-01-01'::timestamp, '2020-01-02', '7 hours') g"));
        assertEquals("2020-01-31 00:00:00, 2020-02-29 00:00:00, 2020-03-29 00:00:00, "
                        + "2020-04-29 00:00:00",
                rows("SELECT g::text FROM generate_series("
                        + "'2020-01-31'::timestamp, '2020-05-01', '1 month') g"));
        assertEquals("3506329", scalar("SELECT count(*)::text FROM generate_series("
                + "'2000-01-01'::timestamp, '2400-01-01', '1 hour')"));
    }

    // ---------------------------------------------------------------- what it costs

    /**
     * A series longer than memgres would have held. The old limit was ten million rows and a
     * refusal past it; PostgreSQL has no such limit, and neither does a series that is never
     * built.
     */
    @Test
    void aSeriesLongerThanCouldHaveBeenHeldIsAnswered() throws Exception {
        assertEquals("20000000", scalar("SELECT count(*)::text FROM generate_series(1, 20000000)"));
        assertEquals("12000000", scalar("SELECT count(*)::text FROM generate_series(1, 12000000)"));
    }

    /** Counting a series is arithmetic on its ends, not a walk of its rows. */
    @Test
    void countingASeriesDoesNotProduceIt() throws Exception {
        assertTrue(millisOf("SELECT count(*)::text FROM generate_series(1, 20000000)") < 2000,
                "counting a twenty-million-row series should not build it");
    }

    /** A row past the limit cannot reach the answer, so the select list is not evaluated for it. */
    @Test
    void aLimitOverALongSeriesCostsTheRowsItAsksFor() throws Exception {
        assertEquals("1", scalar("SELECT g::text FROM generate_series(1, 5000000) g LIMIT 1"));
        assertTrue(millisOf("SELECT g::text FROM generate_series(1, 5000000) g LIMIT 1") < 2000,
                "a LIMIT 1 should not project five million rows");
        // The offset still counts from the same place it always did.
        assertEquals("4999999, 5000000",
                rows("SELECT g::text FROM generate_series(1, 5000000) g OFFSET 4999998"));
    }

    /**
     * A series of more rows than can be addressed at once.
     *
     * <p>PostgreSQL answers this one by running until something stops it — nine quintillion rows
     * is not an answer anybody receives. memgres says so instead, with the SQLSTATE PostgreSQL
     * uses for a limit of its own implementation, rather than beginning an answer it cannot finish.
     */
    @Test
    void aSeriesOfMoreRowsThanCanBeHeldIsRefused() {
        SQLException e = assertThrows(SQLException.class, () ->
                scalar("SELECT count(*) FROM generate_series(1::int8, 9223372036854775807)"));
        assertEquals("54000", e.getSQLState());
    }

    /** Ordering, joining and numbering a series still see every row of it. */
    @Test
    void aSeriesIsStillAWholeRelation() throws Exception {
        assertEquals("6, 5", rows("SELECT g::text FROM generate_series(1, 6) g ORDER BY g DESC LIMIT 2"));
        assertEquals("12", scalar(
                "SELECT count(*)::text FROM generate_series(1, 4) a, generate_series(1, 3) b"));
        assertEquals("14, 21", rows(
                "SELECT g::text FROM generate_series(1, 25) g WHERE g % 7 = 0 OFFSET 1"));
        assertEquals("10, 11, 12", rows(
                "SELECT g::text FROM generate_series(10, 12) WITH ORDINALITY AS t(g, o)"));
    }

    /**
     * The same rule, on the other thing a query can ask to be given more of than it can hold.
     *
     * <p>An array's extents are asked for rather than accumulated, so a request for four hundred
     * million elements is a number to read before anything is allocated for it. Building it until
     * the heap ran out took the whole run with it, where PostgreSQL answers the request itself.
     */
    @Test
    void anArrayLargerThanCanBeBuiltIsRefused() throws Exception {
        SQLException e = assertThrows(SQLException.class,
                () -> scalar("SELECT array_length(array_fill(1, ARRAY[400000000]), 1)"));
        assertEquals("54000", e.getSQLState());
        // The extents multiply, so no one of them has to be large for the request to be.
        assertEquals("54000", assertThrows(SQLException.class,
                () -> scalar("SELECT array_length(array_fill(1, ARRAY[20000, 20000]), 1)"))
                .getSQLState());
        // A size it can hold is still built.
        assertEquals("{{7,7},{7,7}}", scalar("SELECT array_fill(7, ARRAY[2, 2])::text"));
    }

    /**
     * A series named by two untyped literals names no overload: generate_series is declared over
     * three numeric types and over instants, and an untyped literal chooses none of them.
     */
    @Test
    void aSeriesOverUntypedLiteralsIsRefused() {
        SQLException ambiguous = assertThrows(SQLException.class,
                () -> scalar("SELECT count(*) FROM generate_series('1', '3')"));
        assertEquals("42725", ambiguous.getSQLState());
        SQLException wrongKind = assertThrows(SQLException.class,
                () -> scalar("SELECT count(*) FROM generate_series('1'::text, '3'::text)"));
        assertEquals("42883", wrongKind.getSQLState());
    }
}
