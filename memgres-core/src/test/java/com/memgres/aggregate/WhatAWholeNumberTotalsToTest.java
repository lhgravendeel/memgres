package com.memgres.aggregate;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a total of whole numbers is written as.
 *
 * <p>A smallint is a whole number and so is a total of them: {@code sum} over smallints is a
 * bigint, and a bigint has no fractional part to print. Converted through a double on the way into
 * the running total, three smallints adding to seven came back as "7.0" -- and the column said
 * bigint while the value said otherwise, so the two disagreed about the same answer.
 */
class WhatAWholeNumberTotalsToTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE TABLE zwt_t (a int2, b int4, c int8, d numeric, e real, f float8)");
        exec("INSERT INTO zwt_t VALUES (1,1,1,1,1,1),(2,2,2,2,2,2),(4,4,4,4,4,4)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE zwt_t");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static String[] row(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            String[] out = new String[rs.getMetaData().getColumnCount()];
            for (int i = 0; i < out.length; i++) out[i] = rs.getString(i + 1);
            return out;
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** Each total is written the way its own type is written. */
    @Test
    void whatEachTotalIsWrittenAs() throws SQLException {
        assertArrayEquals(new String[]{"7", "7", "7", "7", "7", "7"},
                row("SELECT sum(a)::text, sum(b)::text, sum(c)::text,"
                        + " sum(d)::text, sum(e)::text, sum(f)::text FROM zwt_t"));
        assertArrayEquals(new String[]{"bigint", "bigint", "numeric", "numeric",
                        "real", "double precision"},
                row("SELECT pg_typeof(sum(a))::text, pg_typeof(sum(b))::text,"
                        + " pg_typeof(sum(c))::text, pg_typeof(sum(d))::text,"
                        + " pg_typeof(sum(e))::text, pg_typeof(sum(f))::text FROM zwt_t"));
        // A total that is still a whole number goes on being one when something is done with it.
        assertArrayEquals(new String[]{"8", "7"},
                row("SELECT (sum(a)+1)::text, sum(DISTINCT a)::text FROM zwt_t"));
        // And one smallint on its own totals to itself.
        assertArrayEquals(new String[]{"1"},
                row("SELECT sum(a)::text FROM (SELECT 1::int2 AS a) q"));
    }

    /** The same whole number read as a numeric, which is where it also went through a double. */
    @Test
    void whatASmallintReadsAsANumeric() throws SQLException {
        assertArrayEquals(new String[]{"1", "1", "1", "2", "1"},
                row("SELECT a::numeric::text, a::text, a::int::text, (a*2)::text, abs(a)::text"
                        + " FROM zwt_t WHERE a = 1"));
    }
}
