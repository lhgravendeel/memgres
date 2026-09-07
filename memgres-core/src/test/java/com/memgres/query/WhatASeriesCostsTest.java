package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a series costs to read one row of.
 *
 * <p>A series is worked out where it is read rather than built up front: a caller that wants the
 * first row of a twenty-million-row series should not have to wait for -- or find room for -- the
 * other twenty million. Held as a list of values, the series was built whole before anything could
 * look at it, and a query PostgreSQL answers at once ran the engine out of memory.
 *
 * <p>The same for a subquery under a LIMIT: it need not produce more rows than the LIMIT asks for.
 * That holds only where the reading query cannot look past that many -- it must take its rows from
 * that one item, keep all of them, put none in order, and say how many it wants as a plain number.
 * A WHERE, a GROUP BY, DISTINCT, an ORDER BY or an aggregate decides how many rows come out only
 * once they are all in hand, so under any of those nothing can be cut; and a subquery that orders
 * its own rows decides which come first for itself.
 */
class WhatASeriesCostsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    /** One row of a very long series is one row's worth of work. */
    @Test
    @Timeout(30)
    void whatOneRowOfALongSeriesCosts() throws SQLException {
        assertEquals("1", one("SELECT generate_series(1, 20000000) AS g LIMIT 1"));
        assertEquals("1", one("SELECT g FROM (SELECT generate_series(1, 20000000) AS g) t LIMIT 1"));
        assertEquals(java.util.Arrays.asList("1", "2", "3"),
                rows("SELECT g FROM (SELECT generate_series(1, 20000000) AS g) t LIMIT 3"));
    }

    /** The series is the same series, however much of it is read. */
    @Test
    void whatASeriesHolds() throws SQLException {
        assertEquals(java.util.Arrays.asList("1", "2", "3"),
                rows("SELECT generate_series(1,3) AS g"));
        assertEquals(java.util.Arrays.asList("5", "3", "1"),
                rows("SELECT generate_series(5,1,-2) AS g"));
        assertEquals("4", one("SELECT count(*)::text FROM generate_series(1,10,3) g"));
        assertEquals("500500", one("SELECT sum(g)::text FROM generate_series(1,1000) g"));
        assertEquals("0", one("SELECT count(*)::text FROM generate_series(2,1) g"));
        assertEquals("1", one("SELECT count(*)::text FROM generate_series(1,1) g"));
    }

    /** A cut is only taken where the reading query cannot look past it. */
    @Test
    void whenASubqueryMayBeCutShort() throws SQLException {
        String from = " FROM (SELECT generate_series(1,10) AS g) t";
        assertEquals(java.util.Arrays.asList("4", "5"), rows("SELECT g" + from + " LIMIT 2 OFFSET 3"));
        assertEquals(java.util.Arrays.asList("8", "9", "10"), rows("SELECT g" + from + " OFFSET 7"));
        // A WHERE, an ORDER BY, DISTINCT or an aggregate all read past the first rows.
        assertEquals(java.util.Arrays.asList("9"), rows("SELECT g" + from + " WHERE g > 8 LIMIT 1"));
        assertEquals(java.util.Arrays.asList("10", "9"),
                rows("SELECT g" + from + " ORDER BY g DESC LIMIT 2"));
        assertEquals("55", one("SELECT sum(g)::text" + from + " LIMIT 1"));
        assertEquals("10", one("SELECT count(*)::text" + from));
        // And a subquery that orders its own rows decides which come first.
        assertEquals(java.util.Arrays.asList("10", "9"),
                rows("SELECT g FROM (SELECT generate_series(1,10) AS g ORDER BY 1 DESC) t LIMIT 2"));
    }
}
