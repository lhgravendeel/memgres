package com.memgres.aggregate;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an ordered-set aggregate somebody declared is.
 *
 * <p>An ordered-set aggregate a user creates is made of the same pieces PostgreSQL's own are:
 * {@code CREATE AGGREGATE p (float8 ORDER BY float8) (SFUNC = ordered_set_transition, FINALFUNC =
 * percentile_cont_float8_final)} is percentile_cont under another name, and it is the final
 * function that says which. Read as a name of its own, such an aggregate was one this engine had
 * never heard of -- and once it was recognised, its rows still had
 * to arrive as the type it says it sorts over, or a percentile over numeric literals answered 2.0
 * where PostgreSQL answers 2.
 */
class WhatADeclaredOrderedSetAggregateIsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE AGGREGATE zxb_cont (float8 ORDER BY float8) ("
                + " SFUNC = ordered_set_transition, STYPE = internal,"
                + " FINALFUNC = percentile_cont_float8_final)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP AGGREGATE zxb_cont (float8 ORDER BY float8)");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** The catalogue says what kind of aggregate it is, and the call answers as one. */
    @Test
    void whatItAnswers() throws SQLException {
        assertEquals("o|1", one("SELECT aggkind::text || '|' || aggnumdirectargs::text"
                + " FROM pg_aggregate WHERE aggfnoid::regproc::text = 'zxb_cont'"));
        assertEquals("2", one("SELECT zxb_cont(0.5) WITHIN GROUP (ORDER BY v)::text"
                + " FROM (VALUES (1.0),(2.0),(3.0)) t(v)"));
        assertEquals("2", one("SELECT zxb_cont(0.5) WITHIN GROUP (ORDER BY v)::text"
                + " FROM (VALUES (1.0::float8),(2.0),(3.0)) t(v)"));
        assertEquals("1", one("SELECT zxb_cont(0.0) WITHIN GROUP (ORDER BY v)::text"
                + " FROM (VALUES (1.0),(9.0)) t(v)"));
        // And it interpolates, which is what percentile_cont does and percentile_disc does not.
        assertEquals("1.5", one("SELECT zxb_cont(0.5) WITHIN GROUP (ORDER BY v)::text"
                + " FROM (VALUES (1.0),(2.0)) t(v)"));
    }
}
