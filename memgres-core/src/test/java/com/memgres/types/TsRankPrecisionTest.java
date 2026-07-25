package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * ts_rank returns a float4 and PG prints the shortest decimal that reads back as the
 * same value — 0.06079271 here, verified against PG 18. An earlier fix rounded the
 * result to six significant digits, which dropped a digit PG keeps.
 */
class TsRankPrecisionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static String scalar(String sql) throws SQLException { try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) { return rs.next() ? rs.getString(1) : null; } }

    @Test void ts_rank_display_precision() throws SQLException {
        String val = scalar("SELECT ts_rank(to_tsvector('english','The quick brown fox'), to_tsquery('english','fox'))");
        assertEquals("0.06079271", val,
            "ts_rank should display as PG 18 does, got " + val);
    }
}
