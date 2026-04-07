package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #18: ts_rank returns 0.06079271 instead of PG's 0.0607927.
 * Float display precision issue: PG truncates to 7 significant digits.
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
        // PG returns exactly "0.0607927" (7 significant digits for float4)
        assertEquals("0.0607927", val,
            "ts_rank should display as 0.0607927 (PG float4 precision), got " + val);
    }
}
