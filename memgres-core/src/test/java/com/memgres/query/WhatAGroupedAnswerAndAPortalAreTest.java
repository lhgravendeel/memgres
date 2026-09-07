package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a grouped answer is ordered by, what a portal is, and how long a wait waits.
 *
 * <p>A sort key the select list does not carry is still a key: {@code ORDER BY a} over
 * {@code GROUPING SETS ((a), ())} sorts by the grouping column, which is NULL for the set that does
 * not group by it. Read only from the select list, every such key was nothing and the rows came
 * back in whatever order the sets were answered in.
 *
 * <p>The extended protocol's unnamed portal is a cursor as far as pg_cursors is concerned, and
 * PostgreSQL lists it under the empty name while it is open. A row invented for it whether or not
 * one was open told a client in simple query mode that a cursor it never declared was there.
 *
 * <p>And {@code pg_sleep_for} waits: read only to check its argument was an interval, it returned
 * at once and a caller timing itself against the clock saw no time pass.
 */
class WhatAGroupedAnswerAndAPortalAreTest {

    private static Memgres memgres;
    private static Connection simple;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        simple = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (simple != null) simple.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = simple.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = simple.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
        }
        return out;
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = simple.createStatement()) {
            s.execute(sql);
        }
    }

    /** A grouped answer is sorted by the key the query names, in or out of the select list. */
    @Test
    void whatAGroupedAnswerIsSortedBy() throws SQLException {
        exec("CREATE TABLE zwg_s (a int, c int)");
        exec("INSERT INTO zwg_s VALUES (1,10),(2,20),(3,120)");
        assertEquals(java.util.Arrays.asList("0", "0", "0", "1"),
                rows("SELECT grouping(a)::text FROM zwg_s"
                        + " GROUP BY GROUPING SETS ((a), ()) ORDER BY a NULLS LAST"));
        assertEquals(java.util.Arrays.asList("10", "20", "120", "150"),
                rows("SELECT sum(c)::text FROM zwg_s GROUP BY ROLLUP (a) ORDER BY a NULLS LAST"));
        assertEquals(java.util.Arrays.asList("null|150", "1|10", "2|20", "3|120"),
                rows("SELECT coalesce(a::text,'null'), sum(c)::text FROM zwg_s"
                        + " GROUP BY GROUPING SETS ((a), ()) ORDER BY a NULLS FIRST"));
        exec("DROP TABLE zwg_s");
    }

    /** With nothing to order by, the set that groups by nothing is answered first. */
    @Test
    void whichGroupingSetIsAnsweredFirst() throws SQLException {
        exec("CREATE TABLE zwg_t (g text, v int)");
        exec("INSERT INTO zwg_t VALUES ('a',3),('b',3)");
        assertEquals(java.util.Arrays.asList("null|6", "a|3", "b|3"),
                rows("SELECT coalesce(g,'null'), sum(v)::text FROM zwg_t"
                        + " GROUP BY GROUPING SETS ((g), ())"));
        assertEquals(java.util.Arrays.asList("null|6", "a|3", "b|3"),
                rows("SELECT coalesce(g,'null'), sum(v)::text FROM zwg_t"
                        + " GROUP BY GROUPING SETS ((), (g))"));
        exec("DROP TABLE zwg_t");
    }

    /** A cursor a simple query never opened is not one pg_cursors lists. */
    @Test
    void whichCursorsAreListed() throws SQLException {
        assertEquals("0", one("SELECT count(*)::text FROM pg_cursors"));
        exec("CREATE TABLE zwg_c (i int)");
        exec("INSERT INTO zwg_c VALUES (1)");
        exec("BEGIN");
        exec("DECLARE zwg_cur CURSOR FOR SELECT i FROM zwg_c");
        assertEquals("1", one("SELECT count(*)::text FROM pg_cursors"));
        exec("COMMIT");
        // A cursor without WITH HOLD closes when the transaction commits.
        assertEquals("0", one("SELECT count(*)::text FROM pg_cursors"));
        exec("DROP TABLE zwg_c");
    }

    /** In the extended protocol the unnamed portal is listed, under the empty name. */
    @Test
    void whatTheUnnamedPortalIsCalled() throws Exception {
        try (Connection extended = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
             PreparedStatement ps = extended.prepareStatement("SELECT name FROM pg_cursors");
             ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next(), "the portal this query runs in is listed");
            assertEquals("", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    /** A wait waits. */
    @Test
    void howLongAWaitWaits() throws SQLException {
        assertEquals("true", one("SELECT (clock_timestamp() - now() > interval '1 second')::text"
                + " FROM (SELECT pg_sleep_for('2 seconds')) t"));
        // A wait of no time at all is over at once, and so is one already past. Both answer
        // with void, which reads back as the empty string.
        assertEquals("", one("SELECT pg_sleep_for('0 seconds')"));
        assertEquals("", one("SELECT pg_sleep_until(now() - interval '1 hour')"));
    }
}
