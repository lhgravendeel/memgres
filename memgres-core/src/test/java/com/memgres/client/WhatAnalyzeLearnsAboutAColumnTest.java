package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What ANALYZE learns about a column, and what a planner would read of it.
 *
 * <p>A planner reads these numbers and nothing else about the data: how often the column is null,
 * how wide its values are, how many distinct ones there are, which of them are common enough to be
 * worth naming, where the rest fall, and whether the column's order follows the table's. Left as a
 * distinct count and zeros, {@code pg_stats} said a column of three values repeated a hundred
 * times each had no common values at all, {@code n_distinct} named a count where PostgreSQL names
 * a fraction, and the width of every value was the length of its text.
 *
 * <p>The counts here are PostgreSQL's own. A distinct count above a tenth of the table is stored
 * as the negative fraction it is, because such a count grows with the table rather than staying
 * put; a value is common enough to name when it beats a threshold set from the average, and what
 * is left over becomes the histogram. And a relation's statistics say how many times it has been
 * vacuumed and analysed, which is what a monitor watches rather than the times.
 */
class WhatAnalyzeLearnsAboutAColumnTest {

    static Memgres memgres;
    static Connection conn;

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
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    if (c > 1) row.append('/');
                    row.append(rs.getString(c));
                }
                out.add(row.toString());
            }
            return out;
        }
    }

    /**
     * A count of distinct values that grows with the table is stored as the fraction it is, and
     * one that stays put is stored as the count.
     */
    @Test
    void aDistinctCountThatGrowsWithTheTableIsStoredAsAFraction() throws SQLException {
        exec("CREATE TABLE zan_a (id int, v text)");
        exec("INSERT INTO zan_a SELECT g, 'x'||(g%7) FROM generate_series(1,300) g");
        try {
            exec("ANALYZE zan_a");
            assertEquals(List.of("id/-1", "v/7"),
                    rows("SELECT attname::text, n_distinct FROM pg_stats"
                            + " WHERE tablename='zan_a' ORDER BY 1"));
        } finally {
            exec("DROP TABLE zan_a");
        }
    }

    /** A value's width is what it takes to store, which for a text value counts its length word. */
    @Test
    void aValuesWidthIsWhatItTakesToStore() throws SQLException {
        exec("CREATE TABLE zan_w (i int, b bool, f float8, s text)");
        exec("INSERT INTO zan_w VALUES (1, true, 1.5, 'ab'), (2, false, 2.5, 'cd')");
        exec("CREATE TABLE zan_w2 (t text)");
        exec("INSERT INTO zan_w2 VALUES ('short'), (repeat('y', 200))");
        try {
            exec("ANALYZE zan_w");
            exec("ANALYZE zan_w2");
            assertEquals(List.of("b/1", "f/8", "i/4", "s/3"),
                    rows("SELECT attname::text, avg_width FROM pg_stats"
                            + " WHERE tablename='zan_w' ORDER BY 1"));
            // 'short' is six bytes stored and the long one is 204; the average is 105.
            assertEquals("105", one("SELECT avg_width FROM pg_stats WHERE tablename='zan_w2'"));
        } finally {
            exec("DROP TABLE zan_w, zan_w2");
        }
    }

    /**
     * A value is worth naming when it beats a threshold set from the average, and what the named
     * values leave over becomes the histogram.
     */
    @Test
    void theCommonValuesAndTheHistogramDivideTheColumnBetweenThem() throws SQLException {
        exec("CREATE TABLE zan_m (id int)");
        exec("INSERT INTO zan_m VALUES (1),(1),(2),(3),(4),(5),(6),(7),(8),(9)");
        exec("CREATE TABLE zan_m2 (id int)");
        exec("INSERT INTO zan_m2 SELECT g%40 FROM generate_series(1,300) g");
        exec("CREATE TABLE zan_m3 (id int)");
        exec("INSERT INTO zan_m3 SELECT g FROM generate_series(1,10) g");
        try {
            exec("ANALYZE zan_m");
            exec("ANALYZE zan_m2");
            exec("ANALYZE zan_m3");
            // One value repeats, so it alone is named; the other eight make the histogram.
            assertEquals("-0.9/{1}/{0.2}/{2,3,4,5,6,7,8,9}",
                    one("SELECT n_distinct, most_common_vals::text, most_common_freqs::text,"
                            + " histogram_bounds::text FROM pg_stats WHERE tablename='zan_m'"));
            // Every value repeats often enough to name, so nothing is left for a histogram.
            assertEquals("40/null", one("SELECT array_length(most_common_vals, 1),"
                    + " histogram_bounds::text FROM pg_stats WHERE tablename='zan_m2'"));
            // No value repeats at all, so there is nothing common and the histogram is all of it.
            assertEquals("null/{1,2,3,4,5,6,7,8,9,10}",
                    one("SELECT most_common_vals::text, histogram_bounds::text"
                            + " FROM pg_stats WHERE tablename='zan_m3'"));
        } finally {
            exec("DROP TABLE zan_m, zan_m2, zan_m3");
        }
    }

    /** A column written in order follows the table's order exactly, and one that alternates does not. */
    @Test
    void correlationSaysWhetherTheColumnsOrderFollowsTheTables() throws SQLException {
        exec("CREATE TABLE zan_c (id int, alt bool)");
        exec("INSERT INTO zan_c SELECT g, g%2=0 FROM generate_series(1,300) g");
        try {
            exec("ANALYZE zan_c");
            assertEquals("1", one("SELECT correlation FROM pg_stats"
                    + " WHERE tablename='zan_c' AND attname='id'"));
            assertEquals("0.50498337", one("SELECT correlation FROM pg_stats"
                    + " WHERE tablename='zan_c' AND attname='alt'"));
        } finally {
            exec("DROP TABLE zan_c");
        }
    }

    /** A null takes no place in the order and is counted as the fraction of rows it is. */
    @Test
    void nullsAreCountedAndTakeNoPlaceInTheOrder() throws SQLException {
        exec("CREATE TABLE zan_n (n int)");
        exec("INSERT INTO zan_n SELECT CASE WHEN g%10=0 THEN NULL ELSE g%3 END"
                + " FROM generate_series(1,300) g");
        try {
            exec("ANALYZE zan_n");
            assertEquals("0.1/3/{0,1,2}/{0.3,0.3,0.3}",
                    one("SELECT null_frac, n_distinct, most_common_vals::text,"
                            + " most_common_freqs::text FROM pg_stats WHERE tablename='zan_n'"));
        } finally {
            exec("DROP TABLE zan_n");
        }
    }

    /** The slots hold what they hold in the order PostgreSQL fills them. */
    @Test
    void theNumberedSlotsSayWhichStatisticEachHolds() throws SQLException {
        exec("CREATE TABLE zan_s (id int, v text)");
        exec("INSERT INTO zan_s SELECT g, 'x'||(g%7) FROM generate_series(1,300) g");
        try {
            exec("ANALYZE zan_s");
            // A column with no common values holds the histogram first, then the correlation;
            // one whose values are all common holds the common values and then the correlation.
            assertEquals(List.of("1/2/3/0", "2/1/3/0"),
                    rows("SELECT staattnum, stakind1, stakind2, stakind3 FROM pg_statistic"
                            + " WHERE starelid='zan_s'::regclass ORDER BY staattnum"));
            assertEquals(List.of("0/4/-1", "0/3/7"),
                    rows("SELECT stanullfrac, stawidth, stadistinct FROM pg_statistic"
                            + " WHERE starelid='zan_s'::regclass ORDER BY staattnum"));
        } finally {
            exec("DROP TABLE zan_s");
        }
    }

    /** A relation with no rows has nothing to describe, and no statistics are written for it. */
    @Test
    void anEmptyRelationHasNothingToDescribe() throws SQLException {
        exec("CREATE TABLE zan_e (a int)");
        try {
            exec("ANALYZE zan_e");
            assertEquals("0", one("SELECT count(*)::int FROM pg_stats WHERE tablename='zan_e'"));
        } finally {
            exec("DROP TABLE zan_e");
        }
    }

    /** The counts move with the statements that ran, which is what a monitor watches. */
    @Test
    void theMaintenanceCountsMoveWithTheStatements() throws SQLException {
        exec("CREATE TABLE zan_v (id int)");
        exec("INSERT INTO zan_v VALUES (1)");
        try {
            assertEquals("0/0", one("SELECT vacuum_count, analyze_count"
                    + " FROM pg_stat_user_tables WHERE relname='zan_v'"));
            exec("ANALYZE zan_v");
            assertEquals("0/1", one("SELECT vacuum_count, analyze_count"
                    + " FROM pg_stat_user_tables WHERE relname='zan_v'"));
            exec("VACUUM zan_v");
            assertEquals("1/1", one("SELECT vacuum_count, analyze_count"
                    + " FROM pg_stat_user_tables WHERE relname='zan_v'"));
            exec("VACUUM ANALYZE zan_v");
            // Nothing runs autovacuum here, so the two automatic counts stay where they started.
            assertEquals("2/2/0/0", one("SELECT vacuum_count, analyze_count,"
                    + " autovacuum_count, autoanalyze_count FROM pg_stat_user_tables"
                    + " WHERE relname='zan_v'"));
        } finally {
            exec("DROP TABLE zan_v");
        }
    }

    /** An ANALYZE that names columns gathers those and leaves the rest as it found them. */
    @Test
    void anAnalyzeThatNamesColumnsLeavesTheRestAsItFoundThem() throws SQLException {
        exec("CREATE TABLE zan_p (a int, b int)");
        exec("INSERT INTO zan_p SELECT g, g%5 FROM generate_series(1,50) g");
        try {
            exec("ANALYZE zan_p (a)");
            assertEquals(List.of("a"), rows("SELECT attname::text FROM pg_stats"
                    + " WHERE tablename='zan_p' ORDER BY 1"));
            exec("ANALYZE zan_p (b)");
            assertEquals(List.of("a", "b"), rows("SELECT attname::text FROM pg_stats"
                    + " WHERE tablename='zan_p' ORDER BY 1"));
        } finally {
            exec("DROP TABLE zan_p");
        }
    }
}
