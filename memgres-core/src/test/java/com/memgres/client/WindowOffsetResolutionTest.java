package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A window function's offsets are read where they are written.
 *
 * <p>lag, lead, nth_value and ntile each take a count, and every frame bound takes a size. Two
 * things decide what one of those is worth: the type it was written with, which says whether the
 * call exists at all, and the row it is read on, which says what it is worth there. Neither was
 * being asked. An offset was read once for the whole partition, so {@code lag(v, o)} stepped the
 * first row's distance on every row and a column named as the default was not there to resolve;
 * the value was narrowed by casting whatever arrived to a number, so a quoted {@code '1'} left an
 * internal error and a bigint offset silently became an integer one where PostgreSQL has no such
 * function; and a frame size was tested for negativity as it arrived, so {@code ROWS '-1'
 * PRECEDING} was text rather than a negative bigint and went through, while a size past the bigint
 * range wrapped round into a small one and quietly covered the wrong rows.
 */
class WindowOffsetResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE win_off (id int, v int, o int, s smallint, d date, n numeric)");
            st.execute("INSERT INTO win_off VALUES"
                    + " (1,10,1,1,'2020-01-01',1.5), (2,20,2,2,'2020-01-02',2.5),"
                    + " (3,30,0,1,'2020-01-04',3.5), (4,40,1,1,'2020-01-08',4.5)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) b.append('|');
                    String v = rs.getString(i);
                    b.append(rs.wasNull() ? "NULL" : v);
                }
                out.add(b.toString());
            }
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

    /** o is 1, 2, 0, 1, so each row steps its own distance back. */
    @Test
    void anOffsetIsReadOnTheRowItProducesAValueFor() throws Exception {
        assertEquals(List.of("1|NULL", "2|NULL", "3|30", "4|30"),
                rows("SELECT id, lag(v, o) OVER (ORDER BY id) FROM win_off ORDER BY id"));
        assertEquals(List.of("1|20", "2|40", "3|30", "4|NULL"),
                rows("SELECT id, lead(v, o) OVER (ORDER BY id) FROM win_off ORDER BY id"));
    }

    /** The default is read on the same row, so it may name a column too. */
    @Test
    void theDefaultIsReadOnTheSameRow() throws Exception {
        assertEquals(List.of("1|10", "2|10", "3|20", "4|30"),
                rows("SELECT id, lag(v, 1, v) OVER (ORDER BY id) FROM win_off ORDER BY id"));
        assertEquals(List.of("1|-1", "2|-1", "3|30", "4|30"),
                rows("SELECT id, lag(v, o, -1) OVER (ORDER BY id) FROM win_off ORDER BY id"));
    }

    /** A null offset produces null for that row, and the default does not answer for it. */
    @Test
    void aNullOffsetProducesNullRatherThanTheDefault() throws Exception {
        assertEquals(List.of("1|NULL", "2|NULL", "3|NULL", "4|NULL"),
                rows("SELECT id, lag(v, NULL, -1) OVER (ORDER BY id) FROM win_off ORDER BY id"));
    }

    /** smallint widens to integer, so the call is the integer one. */
    @Test
    void smallintWidensToTheIntegerCall() throws Exception {
        assertEquals(List.of("1|NULL", "2|NULL", "3|20", "4|30"),
                rows("SELECT id, lag(v, s) OVER (ORDER BY id) FROM win_off ORDER BY id"));
        assertEquals(List.of("1|1", "2|1", "3|1", "4|1"),
                rows("SELECT id, ntile(s) OVER (ORDER BY id) FROM win_off ORDER BY id"));
    }

    /** bigint does not narrow to integer, and there is no window function taking one. */
    @Test
    void aBigintOffsetNamesNoFunction() {
        assertEquals("42883",
                stateOf("SELECT id, lag(v, 2::bigint) OVER (ORDER BY id) FROM win_off"));
        assertEquals("42883",
                stateOf("SELECT id, ntile(v::bigint) OVER (ORDER BY id) FROM win_off"));
        assertEquals("42883",
                stateOf("SELECT id, nth_value(v, 2::bigint) OVER (ORDER BY id) FROM win_off"));
    }

    /** A whole number is written as the narrowest type that holds it. */
    @Test
    void aNumberPastTheIntegerRangeIsABigint() {
        assertEquals("42883",
                stateOf("SELECT id, lag(v, 2147483648) OVER (ORDER BY id) FROM win_off"));
        assertEquals("42883",
                stateOf("SELECT id, ntile(2147483648) OVER (ORDER BY id) FROM win_off"));
        assertEquals("42883",
                stateOf("SELECT id, nth_value(v, 3000000000) OVER (ORDER BY id) FROM win_off"));
    }

    /** numeric does not narrow either, whether or not it has a fraction. */
    @Test
    void aNumericOffsetNamesNoFunctionEvenWhenWhole() {
        assertEquals("42883", stateOf("SELECT id, lag(v, 1.0) OVER (ORDER BY id) FROM win_off"));
        assertEquals("42883", stateOf("SELECT id, lag(v, 1.5) OVER (ORDER BY id) FROM win_off"));
        assertEquals("42883", stateOf("SELECT id, ntile(2.5) OVER (ORDER BY id) FROM win_off"));
    }

    /** Written as a cast to integer it is an integer, whatever it was before. */
    @Test
    void aCastToIntegerMakesTheIntegerCall() throws Exception {
        assertEquals(List.of("1|NULL", "2|NULL", "3|10", "4|20"),
                rows("SELECT id, lag(v, 2.0::int) OVER (ORDER BY id) FROM win_off ORDER BY id"));
    }

    /** A quoted literal is not yet anything, and becomes what the parameter asks for. */
    @Test
    void aQuotedOffsetBecomesWhatTheParameterAsksFor() throws Exception {
        assertEquals(List.of("1|NULL", "2|10", "3|20", "4|30"),
                rows("SELECT id, lag(v, '1') OVER (ORDER BY id) FROM win_off ORDER BY id"));
        assertEquals(List.of("1|1", "2|1", "3|2", "4|2"),
                rows("SELECT id, ntile('2') OVER (ORDER BY id) FROM win_off ORDER BY id"));
    }

    /** And says so when it does not read as one. */
    @Test
    void aQuotedOffsetThatIsNoNumberSaysSo() {
        assertEquals("22P02", stateOf("SELECT id, ntile('x') OVER (ORDER BY id) FROM win_off"));
    }

    /**
     * How many buckets there are is one answer for the whole partition, read on its first row.
     * o is 1 there, so every row lands in bucket 1 -- not the 2 buckets row 2 asks for.
     */
    @Test
    void ntileCountsBucketsOnceForThePartition() throws Exception {
        assertEquals(List.of("1|1", "2|1", "3|1", "4|1"),
                rows("SELECT id, ntile(o) OVER (ORDER BY id) FROM win_off ORDER BY id"));
        assertEquals(List.of("1|NULL", "2|NULL", "3|NULL", "4|NULL"),
                rows("SELECT id, ntile(NULL) OVER (ORDER BY id) FROM win_off ORDER BY id"));
        assertEquals("22014", stateOf("SELECT id, ntile(0) OVER (ORDER BY id) FROM win_off"));
    }

    /** Which value in the frame is wanted is read on the row it is wanted for. */
    @Test
    void nthValueReadsItsPositionOnEachRow() throws Exception {
        assertEquals(List.of("1|10", "2|20", "3|30", "4|40"),
                rows("SELECT id, nth_value(v, id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED"
                        + " PRECEDING AND UNBOUNDED FOLLOWING) FROM win_off ORDER BY id"));
        assertEquals(List.of("1|10", "2|20", "4|10"),
                rows("SELECT id, nth_value(v, o) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED"
                        + " PRECEDING AND UNBOUNDED FOLLOWING) FROM win_off WHERE o > 0"
                        + " ORDER BY id"));
        // A position of zero is no position at all, and it is the row asking that says so.
        assertEquals("22016", stateOf("SELECT id, nth_value(v, o) OVER (ORDER BY id ROWS BETWEEN"
                + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM win_off"));
    }

    /** ROWS and GROUPS count in bigints, so a quoted size is read as one -- sign and all. */
    @Test
    void aQuotedFrameSizeIsReadAsTheBigintTheFrameCountsIn() throws Exception {
        assertEquals(List.of("1|1", "2|2", "3|2", "4|2"),
                rows("SELECT id, count(*) OVER (ORDER BY id ROWS '1' PRECEDING)"
                        + " FROM win_off ORDER BY id"));
        assertEquals("22013", stateOf("SELECT id, count(*) OVER (ORDER BY id ROWS '-1' PRECEDING)"
                + " FROM win_off"));
        assertEquals("22013", stateOf("SELECT id, count(*) OVER (ORDER BY id GROUPS '-2'"
                + " PRECEDING) FROM win_off"));
        assertEquals("22013", stateOf("SELECT id, count(*) OVER (ORDER BY id ROWS BETWEEN 1"
                + " PRECEDING AND '-1' FOLLOWING) FROM win_off"));
        assertEquals("22P02", stateOf("SELECT id, count(*) OVER (ORDER BY id ROWS 'x' PRECEDING)"
                + " FROM win_off"));
    }

    /** A size past the bigint range is no bigint, rather than a small one wrapped round. */
    @Test
    void aFrameSizePastTheBigintRangeIsNoBigint() throws Exception {
        assertEquals(List.of("1|1", "2|2", "3|3", "4|4"),
                rows("SELECT id, count(*) OVER (ORDER BY id ROWS 2000000000000 PRECEDING)"
                        + " FROM win_off ORDER BY id"));
        assertEquals("22003", stateOf("SELECT id, count(*) OVER (ORDER BY id ROWS"
                + " 9223372036854775808 PRECEDING) FROM win_off"));
        assertEquals("22003", stateOf("SELECT id, count(*) OVER (ORDER BY id ROWS"
                + " 99999999999999999999 PRECEDING) FROM win_off"));
    }

    /** A RANGE size is read in whatever the ordering column is measured in, intervals included. */
    @Test
    void aRangeSizeIsReadInWhatTheOrderingIsMeasuredIn() throws Exception {
        assertEquals(List.of("1|1", "2|2", "3|1", "4|1"),
                rows("SELECT id, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1 day'"
                        + " PRECEDING AND CURRENT ROW) FROM win_off ORDER BY id"));
        assertEquals("22013", stateOf("SELECT id, count(*) OVER (ORDER BY d RANGE BETWEEN"
                + " INTERVAL '-1 day' PRECEDING AND CURRENT ROW) FROM win_off"));
        assertEquals("22013", stateOf("SELECT id, count(*) OVER (ORDER BY v RANGE BETWEEN '-5'"
                + " PRECEDING AND CURRENT ROW) FROM win_off"));
    }

    @Test
    void whatAlreadyWorkedStillDoes() throws Exception {
        assertEquals(List.of("1|NULL", "2|NULL", "3|10", "4|20"),
                rows("SELECT id, lag(v, 2) OVER (ORDER BY id) FROM win_off ORDER BY id"));
        assertEquals(List.of("1|30", "2|40", "3|0", "4|0"),
                rows("SELECT id, lead(v, 2, 0) OVER (ORDER BY id) FROM win_off ORDER BY id"));
        assertEquals(List.of("1|20", "2|20", "3|20", "4|20"),
                rows("SELECT id, nth_value(v, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED"
                        + " PRECEDING AND UNBOUNDED FOLLOWING) FROM win_off ORDER BY id"));
        assertEquals(List.of("1|2", "2|3", "3|3", "4|2"),
                rows("SELECT id, count(*) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1"
                        + " FOLLOWING) FROM win_off ORDER BY id"));
    }
}
