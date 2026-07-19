package com.memgres.window;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Window frame bound resolution fixes:
 *
 * 1. RANGE frames with an offset under ORDER BY ... DESC (the offset moves opposite
 *    to the value axis: N PRECEDING means values GREATER than the current one).
 * 2. first_value()/last_value()/nth_value() must resolve BOTH frame bounds through
 *    the standard frame-resolution path and return NULL for empty frames.
 * 3. GROUPS frames with offsets beyond the partition edge must yield an empty frame
 *    (NULL for sum, 0 for count) instead of an IndexOutOfBoundsException.
 *
 * All expected values verified against PostgreSQL 18.
 */
class WindowFrameBoundsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE wfb_t5 (x int)");
            s.execute("INSERT INTO wfb_t5 VALUES (1),(2),(3),(4),(5)");
            s.execute("CREATE TABLE wfb_t3 (x int)");
            s.execute("INSERT INTO wfb_t3 VALUES (1),(2),(3)");
            s.execute("CREATE TABLE wfb_tg (g int)");
            s.execute("INSERT INTO wfb_tg VALUES (1),(1),(2),(2),(3)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** Runs the query and returns column 2 of every row as strings (null for SQL NULL). */
    private static List<String> col2(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(2));
        }
        return out;
    }

    // =========================================================================
    // 1. RANGE offset frames with ORDER BY ... DESC
    // =========================================================================

    @Test
    void desc_range_preceding_to_current_row() throws SQLException {
        // N PRECEDING under DESC means values in [x, x+1]
        assertEquals(Arrays.asList("3", "5", "7", "9", "5"), col2(
                "SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                "FROM wfb_t5 ORDER BY x"));
    }

    @Test
    void desc_range_current_row_to_following() throws SQLException {
        // N FOLLOWING under DESC means values in [x-1, x]
        assertEquals(Arrays.asList("1", "3", "5", "7", "9"), col2(
                "SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) " +
                "FROM wfb_t5 ORDER BY x"));
    }

    @Test
    void desc_range_preceding_to_following() throws SQLException {
        // Frame covers values in [x-2, x+1]
        assertEquals(Arrays.asList("3", "6", "10", "14", "12"), col2(
                "SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING) " +
                "FROM wfb_t5 ORDER BY x"));
    }

    @Test
    void desc_range_both_bounds_preceding() throws SQLException {
        // Frame covers values in [x+1, x+3]; empty for x=5 -> NULL
        assertEquals(Arrays.asList("9", "12", "9", "5", null), col2(
                "SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING) " +
                "FROM wfb_t5 ORDER BY x"));
    }

    @Test
    void desc_range_both_bounds_following() throws SQLException {
        // Frame covers values in [x-2, x-1]; empty for x=1 -> NULL
        assertEquals(Arrays.asList(null, "1", "3", "5", "7"), col2(
                "SELECT x, sum(x) OVER (ORDER BY x DESC RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM wfb_t5 ORDER BY x"));
    }

    @Test
    void desc_range_count_over_empty_frame_is_zero() throws SQLException {
        assertEquals(Arrays.asList("3", "3", "2", "1", "0"), col2(
                "SELECT x, count(*) OVER (ORDER BY x DESC RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING) " +
                "FROM wfb_t5 ORDER BY x"));
    }

    @Test
    void desc_range_exclude_current_row_still_works() throws SQLException {
        // Same frame as [x, x+1] but the current row is excluded; x=5 frame becomes empty
        assertEquals(Arrays.asList("2", "3", "4", "5", null), col2(
                "SELECT x, sum(x) OVER (ORDER BY x DESC " +
                "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) " +
                "FROM wfb_t5 ORDER BY x"));
    }

    @Test
    void asc_range_offsets_still_correct() throws SQLException {
        assertEquals(Arrays.asList("1", "3", "5", "7", "9"), col2(
                "SELECT x, sum(x) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                "FROM wfb_t5 ORDER BY x"));
        assertEquals(Arrays.asList(null, "1", "3", "6", "9"), col2(
                "SELECT x, sum(x) OVER (ORDER BY x RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING) " +
                "FROM wfb_t5 ORDER BY x"));
        assertEquals(Arrays.asList("3", "5", "7", "9", "5"), col2(
                "SELECT x, sum(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) " +
                "FROM wfb_t5 ORDER BY x"));
    }

    // =========================================================================
    // 2. first_value / last_value / nth_value with explicit frames
    // =========================================================================

    @Test
    void first_value_rows_following_frame() throws SQLException {
        assertEquals(Arrays.asList("2", "3", null), col2(
                "SELECT x, first_value(x) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void last_value_rows_following_frame() throws SQLException {
        assertEquals(Arrays.asList("3", "3", null), col2(
                "SELECT x, last_value(x) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void nth_value_rows_following_frame() throws SQLException {
        assertEquals(Arrays.asList("3", null, null), col2(
                "SELECT x, nth_value(x, 2) OVER (ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void first_value_rows_preceding_frame() throws SQLException {
        assertEquals(Arrays.asList(null, "1", "1"), col2(
                "SELECT x, first_value(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void last_value_rows_preceding_frame() throws SQLException {
        assertEquals(Arrays.asList(null, "1", "2"), col2(
                "SELECT x, last_value(x) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void nth_value_rows_preceding_frame() throws SQLException {
        assertEquals(Arrays.asList(null, null, "2"), col2(
                "SELECT x, nth_value(x, 2) OVER (ORDER BY x ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void first_last_value_range_offset_frame() throws SQLException {
        // RANGE 1 FOLLOWING..2 FOLLOWING: frame covers values in [x+1, x+2]; empty for x=3
        assertEquals(Arrays.asList("2", "3", null), col2(
                "SELECT x, first_value(x) OVER (ORDER BY x RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM wfb_t3 ORDER BY x"));
        assertEquals(Arrays.asList("3", "3", null), col2(
                "SELECT x, last_value(x) OVER (ORDER BY x RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void first_value_desc_range_offset_frame() throws SQLException {
        // DESC frame covers values in [x, x+1]; first (largest) value in frame
        assertEquals(Arrays.asList("2", "3", "4", "5", "5"), col2(
                "SELECT x, first_value(x) OVER (ORDER BY x DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                "FROM wfb_t5 ORDER BY x"));
    }

    @Test
    void last_value_partial_frame_current_to_following() throws SQLException {
        assertEquals(Arrays.asList("2", "3", "3"), col2(
                "SELECT x, last_value(x) OVER (ORDER BY x ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void first_last_value_default_frame_unchanged() throws SQLException {
        // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (with peers)
        assertEquals(Arrays.asList("1", "1", "1"), col2(
                "SELECT x, first_value(x) OVER (ORDER BY x) FROM wfb_t3 ORDER BY x"));
        assertEquals(Arrays.asList("1", "1", "2", "2", "3"), col2(
                "SELECT g, last_value(g) OVER (ORDER BY g) FROM wfb_tg ORDER BY g"));
    }

    // =========================================================================
    // 3. GROUPS frames with offsets beyond the partition edge
    // =========================================================================

    @Test
    void groups_following_beyond_partition_returns_null() throws SQLException {
        assertEquals(Arrays.asList(null, null, null), col2(
                "SELECT x, sum(x) OVER (ORDER BY x GROUPS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void groups_preceding_beyond_partition_returns_null() throws SQLException {
        assertEquals(Arrays.asList(null, null, null), col2(
                "SELECT x, sum(x) OVER (ORDER BY x GROUPS BETWEEN 5 PRECEDING AND 3 PRECEDING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void groups_count_over_empty_frame_is_zero() throws SQLException {
        assertEquals(Arrays.asList("0", "0", "0"), col2(
                "SELECT x, count(*) OVER (ORDER BY x GROUPS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) " +
                "FROM wfb_t3 ORDER BY x"));
        assertEquals(Arrays.asList("0", "0", "0"), col2(
                "SELECT x, count(*) OVER (ORDER BY x GROUPS BETWEEN 5 PRECEDING AND 3 PRECEDING) " +
                "FROM wfb_t3 ORDER BY x"));
    }

    @Test
    void groups_partially_out_of_range() throws SQLException {
        // Groups of wfb_tg: {1,1}, {2,2}, {3}. Frame = groups [current+2, current+3],
        // clamped to the last group; empty once the start passes the last group.
        assertEquals(Arrays.asList("3", "3", null, null, null), col2(
                "SELECT g, sum(g) OVER (ORDER BY g GROUPS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) " +
                "FROM wfb_tg ORDER BY g"));
    }

    @Test
    void groups_in_range_still_correct() throws SQLException {
        assertEquals(Arrays.asList("2", "2", "6", "6", "7"), col2(
                "SELECT g, sum(g) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                "FROM wfb_tg ORDER BY g"));
        assertEquals(Arrays.asList("6", "6", "7", "7", "3"), col2(
                "SELECT g, sum(g) OVER (ORDER BY g GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) " +
                "FROM wfb_tg ORDER BY g"));
    }

    // =========================================================================
    // Empty-frame result convention across aggregates
    // =========================================================================

    @Test
    void empty_rows_frame_aggregate_conventions() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT x, " +
                     "sum(x)   OVER w, " +
                     "avg(x)   OVER w, " +
                     "min(x)   OVER w, " +
                     "max(x)   OVER w, " +
                     "count(x) OVER w " +
                     "FROM wfb_t3 " +
                     "WINDOW w AS (ORDER BY x ROWS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) " +
                     "ORDER BY x")) {
            int rows = 0;
            while (rs.next()) {
                rows++;
                assertNull(rs.getObject(2), "sum over empty frame must be NULL");
                assertNull(rs.getObject(3), "avg over empty frame must be NULL");
                assertNull(rs.getObject(4), "min over empty frame must be NULL");
                assertNull(rs.getObject(5), "max over empty frame must be NULL");
                assertEquals(0, rs.getInt(6), "count over empty frame must be 0");
                assertFalse(rs.wasNull(), "count over empty frame must be 0, not NULL");
            }
            assertEquals(3, rows);
        }
    }
}
