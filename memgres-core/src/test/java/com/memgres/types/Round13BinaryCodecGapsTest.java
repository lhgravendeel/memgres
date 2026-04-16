package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 13 gaps: wire-protocol binary codecs.
 *
 * These tests exercise the PG JDBC driver's binary-transfer path for types
 * where Memgres currently lacks a binary encoder and falls back to text.
 * When Memgres only emits text bytes under a format=1 RowDescription field,
 * the driver decodes them as raw bytes and produces garbage / exceptions.
 *
 * Strategy:
 *   - Connect WITHOUT "preferQueryMode=simple" → JDBC uses extended protocol.
 *   - Use PreparedStatement reuse (≥5 iterations) to trigger adaptive binary
 *     transfer on result columns.
 *   - Assert that the round-trip value is intact when read as the canonical
 *     JDBC type (getString, getObject, getBytes).
 *
 * Coverage:
 *   A. MACADDR8   — 8-byte binary codec missing
 *   B. TIMETZ     — text-only (PG uses 12-byte binary)
 *   C. INTERVAL   — text-only (PG uses 16-byte binary)
 *   D. JSON/JSONB — text-only (JSONB has 1-byte version header)
 *   E. BIT/VARBIT — binary codec
 *   F. INET/CIDR  — text-only
 *   G. MACADDR    — text-only (6 bytes in PG binary)
 *   H. BOX/POINT/LSEG/CIRCLE — text-only
 *   I. MONEY      — text-only
 */
class Round13BinaryCodecGapsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // Default mode: extended protocol — triggers adaptive binary transfer.
        // binaryTransfer=true is on by default; keep explicit for clarity.
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?binaryTransfer=true",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /**
     * Round-trip a single value via a PreparedStatement reused at least
     * {@code iterations} times — long enough for the JDBC driver to request
     * binary format for the column.
     */
    private static String readColumnViaReusedPreparedStatement(
            String castExpr, int iterations) throws SQLException {
        String lastValue = null;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT " + castExpr + " AS v")) {
            for (int i = 0; i < iterations; i++) {
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    lastValue = rs.getString("v");
                }
            }
        }
        return lastValue;
    }

    // =========================================================================
    // A. MACADDR8 (8-byte PG binary format)
    // =========================================================================

    @Test
    void macaddr8_roundTrip_viaBinaryTransfer() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'08:00:2b:01:02:03:04:05'::macaddr8", 10);
        assertEquals("08:00:2b:01:02:03:04:05", v);
    }

    @Test
    void macaddr8_insertReadBack() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS r13_ma8");
            s.execute("CREATE TABLE r13_ma8 (id int, m macaddr8)");
            s.execute("INSERT INTO r13_ma8 VALUES (1, '08:00:2b:01:02:03:04:05')");
        }
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT m FROM r13_ma8 WHERE id = ?")) {
            for (int i = 0; i < 10; i++) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("08:00:2b:01:02:03:04:05", rs.getString(1));
                }
            }
        }
    }

    // =========================================================================
    // B. TIMETZ (12-byte binary: time + 4-byte offset)
    // =========================================================================

    @Test
    void timetz_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'12:30:00+00'::timetz", 10);
        // Must be intact; PG renders without fractional seconds for whole values.
        assertTrue(v != null && v.startsWith("12:30:00"),
                "expected 12:30:00+/-offset; got " + v);
    }

    @Test
    void timetz_withOffset() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'14:30:00+05:30'::timetz", 10);
        assertTrue(v != null && v.contains("14:30:00"),
                "expected 14:30:00 with +05:30; got " + v);
    }

    // =========================================================================
    // C. INTERVAL (16-byte binary: months int4 + days int4 + micros int8)
    // =========================================================================

    @Test
    void interval_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "interval '1 day 02:03:04'", 10);
        assertNotNull(v);
        // Should contain "1 day" and "02:03:04" in some form
        String lc = v.toLowerCase();
        assertTrue(lc.contains("1 day") && lc.contains("02:03:04"),
                "expected interval '1 day 02:03:04'; got " + v);
    }

    @Test
    void interval_monthsYearsCombined() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "interval '1 year 2 months 3 days'", 10);
        assertNotNull(v);
        String lc = v.toLowerCase();
        // PG canonical: "1 year 2 mons 3 days"
        assertTrue((lc.contains("year") || lc.contains("mon")) && lc.contains("3 day"),
                "expected year/mon/day components; got " + v);
    }

    // =========================================================================
    // D. JSON / JSONB (JSONB has 1-byte binary version prefix)
    // =========================================================================

    @Test
    void jsonb_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'{\"a\":1,\"b\":[1,2,3]}'::jsonb", 10);
        assertNotNull(v);
        // PG canonical JSONB formatting uses space after colon: {"a": 1, ...}
        assertTrue(v.contains("\"a\"") && v.contains("\"b\""), "got " + v);
    }

    @Test
    void json_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'{\"a\":1}'::json", 10);
        assertNotNull(v);
        assertTrue(v.contains("\"a\""));
    }

    // =========================================================================
    // E. BIT / VARBIT
    // =========================================================================

    @Test
    void bit_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "B'101010'", 10);
        assertEquals("101010", v);
    }

    @Test
    void varbit_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "B'11011011'::varbit", 10);
        assertEquals("11011011", v);
    }

    // =========================================================================
    // F. INET / CIDR
    // =========================================================================

    @Test
    void inet_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'192.168.1.1/24'::inet", 10);
        assertEquals("192.168.1.1/24", v);
    }

    @Test
    void cidr_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'192.168.1.0/24'::cidr", 10);
        assertEquals("192.168.1.0/24", v);
    }

    @Test
    void inet_ipv6_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'::1/128'::inet", 10);
        assertEquals("::1/128", v);
    }

    // =========================================================================
    // G. MACADDR (6-byte binary)
    // =========================================================================

    @Test
    void macaddr_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'08:00:2b:01:02:03'::macaddr", 10);
        assertEquals("08:00:2b:01:02:03", v);
    }

    // =========================================================================
    // H. Geometric types
    // =========================================================================

    @Test
    void point_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "point(1.5, 2.5)", 10);
        assertEquals("(1.5,2.5)", v);
    }

    @Test
    void box_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "box '((0,0),(3,4))'", 10);
        // PG canonicalizes box by sorting corners, so it's always "(3,4),(0,0)".
        assertEquals("(3,4),(0,0)", v);
    }

    @Test
    void lseg_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "lseg '((0,0),(1,2))'", 10);
        assertEquals("[(0,0),(1,2)]", v);
    }

    @Test
    void circle_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "circle '<(1,1),5>'", 10);
        assertEquals("<(1,1),5>", v);
    }

    // =========================================================================
    // I. MONEY
    // =========================================================================

    @Test
    void money_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "'$12.34'::money", 10);
        // PG locale-sensitive; default locale formats as "$12.34"
        assertNotNull(v);
        assertTrue(v.contains("12.34"), "expected money to contain 12.34; got " + v);
    }

    // =========================================================================
    // J. Range / Multirange types — binary codecs
    // =========================================================================

    @Test
    void int4range_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "int4range(1, 10)", 10);
        assertEquals("[1,10)", v);
    }

    @Test
    void int4multirange_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "int4multirange(int4range(1,5), int4range(10,20))", 10);
        assertEquals("{[1,5),[10,20)}", v);
    }

    @Test
    void tsrange_roundTrip_binary() throws SQLException {
        String v = readColumnViaReusedPreparedStatement(
                "tsrange('2024-01-01 00:00:00', '2024-12-31 00:00:00')", 10);
        assertNotNull(v);
        assertTrue(v.contains("2024-01-01") && v.contains("2024-12-31"),
                "got " + v);
    }

    // =========================================================================
    // K. UUID (sanity check — already known to work in binary)
    // =========================================================================

    @Test
    void uuid_binaryCodec_sanity() throws SQLException {
        UUID u = UUID.fromString("11111111-2222-3333-4444-555555555555");
        String v = readColumnViaReusedPreparedStatement(
                "'" + u + "'::uuid", 10);
        assertEquals(u.toString(), v);
    }
}
