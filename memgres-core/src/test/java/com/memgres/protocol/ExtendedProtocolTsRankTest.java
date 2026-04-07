package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Issue #3: ts_rank locale-dependent number parsing crash.
 * Float formatting must use Locale.US, not system default.
 * Extended query protocol version.
 */
class ExtendedProtocolTsRankTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    @Test void ts_rank_precision_extended() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT ts_rank(to_tsvector('english','The quick brown fox'), to_tsquery('english','fox'))")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("0.0607927", rs.getString(1),
                    "ts_rank should display as 0.0607927 (PG float4 precision)");
            }
        }
    }

    @Test void ts_rank_cd_precision_extended() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT ts_rank_cd(to_tsvector('english','The quick brown fox'), to_tsquery('english','fox'))")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertNotNull(val, "ts_rank_cd should return a value");
                assertFalse(val.contains(","), "ts_rank_cd should use dot decimal separator, got: " + val);
            }
        }
    }

    @Test void ts_rank_no_crash_on_locale() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT ts_rank(to_tsvector('simple', 'a b c d e f'), to_tsquery('simple', 'a & b'))")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertNotNull(val);
                // Must parse as a valid float (dot separator)
                assertDoesNotThrow(() -> Float.parseFloat(val),
                    "ts_rank result should be parseable as float: " + val);
            }
        }
    }
}
