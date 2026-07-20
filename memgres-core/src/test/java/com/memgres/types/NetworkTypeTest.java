package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for H18-H21, L3: inet/cidr/macaddr/macaddr8 as real types.
 */
class NetworkTypeTest {

    static Memgres memgres;
    Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void connect() throws SQLException {
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterEach
    void disconnect() throws SQLException {
        if (conn != null) conn.close();
    }

    private String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private boolean qBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getBoolean(1);
        }
    }

    private void expectError(String sql, String sqlState) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected error " + sqlState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(sqlState, e.getSQLState(), "Wrong SQLSTATE for: " + sql + " — " + e.getMessage());
        }
    }

    // ========================================================================
    // H18: inet/cidr input validation and normalization
    // ========================================================================

    @Test void inet_rejects_invalid_octet() { assertThrows(SQLException.class, () -> q("SELECT '192.168.1.256'::inet")); }
    @Test void inet_rejects_invalid_mask() { assertThrows(SQLException.class, () -> q("SELECT '1.2.3.4/33'::inet")); }
    @Test void inet_rejects_v6_invalid_mask() { assertThrows(SQLException.class, () -> q("SELECT '::1/129'::inet")); }
    @Test void cidr_rejects_host_bits() throws SQLException { expectError("SELECT '192.168.1.5/24'::cidr", "22P02"); }

    @Test void cidr_zeros_host_bits_on_cast() throws SQLException {
        assertEquals("192.168.1.0/24", q("SELECT ('192.168.1.5/24'::inet::cidr)::text"));
    }

    @Test void cidr_ipv6_mapped() throws SQLException {
        assertEquals("::ffff:0.0.0.0/96", q("SELECT '::ffff:0:0/96'::cidr::text"));
    }

    // ========================================================================
    // H19: inet operators
    // ========================================================================

    @Test void inet_plus_integer() throws SQLException {
        assertEquals("192.168.1.11/32", q("SELECT ('192.168.1.1'::inet + 10)::text"));
    }

    @Test void inet_minus_integer() throws SQLException {
        assertEquals("192.168.1.1/32", q("SELECT ('192.168.1.11'::inet - 10)::text"));
    }

    @Test void inet_minus_inet() throws SQLException {
        assertEquals("10", q("SELECT '192.168.1.11'::inet - '192.168.1.1'::inet"));
    }

    @Test void inet_overlap() throws SQLException {
        assertTrue(qBool("SELECT '192.168.1.0/24'::inet && '192.168.1.128/25'::inet"));
    }

    @Test void inet_no_overlap() throws SQLException {
        assertFalse(qBool("SELECT '192.168.1.0/24'::inet && '10.0.0.0/8'::inet"));
    }

    @Test void inet_equality_with_mask() throws SQLException {
        assertTrue(qBool("SELECT '192.168.1.1'::inet = '192.168.1.1/32'::inet"));
    }

    @Test void inet_containment_strict() throws SQLException {
        assertTrue(qBool("SELECT '192.168.1.5'::inet << '192.168.1.0/24'::inet"));
    }

    @Test void inet_contains_strict() throws SQLException {
        assertTrue(qBool("SELECT '192.168.1.0/24'::inet >> '192.168.1.5/32'::inet"));
    }

    @Test void inet_contains_or_equals() throws SQLException {
        assertTrue(qBool("SELECT '192.168.1.0/24'::inet >>= '192.168.1.0/24'::inet"));
    }

    @Test void inet_contained_by_or_equals() throws SQLException {
        assertTrue(qBool("SELECT '192.168.1.0/24'::inet <<= '192.168.0.0/16'::inet"));
    }

    @Test void inet_ordering() throws SQLException {
        // PG orders: family first, then address bytes, then prefix length
        assertEquals("192.168.1.0/24", q("SELECT min(a)::text FROM (VALUES ('192.168.1.0/24'::inet), ('192.168.1.0/25'::inet)) v(a)"));
    }

    @Test void inet_multiply_rejected() throws SQLException {
        expectError("SELECT '192.168.1.1'::inet * '10.0.0.1'::inet", "42883");
    }

    @Test void cross_family_containment_false() throws SQLException {
        assertFalse(qBool("SELECT '1.2.3.4'::inet << '::/0'::inet"));
    }

    // ========================================================================
    // H20: Network functions
    // ========================================================================

    @Test void netmask_ipv4() throws SQLException {
        assertEquals("255.255.255.0", q("SELECT netmask('192.168.1.0/24'::inet)"));
    }

    @Test void set_masklen() throws SQLException {
        // set_masklen returns inet with prefix 16 (not maxBits), so /16 is shown
        assertEquals("192.168.1.1/16", q("SELECT set_masklen('192.168.1.1/24'::inet, 16)::text"));
    }

    @Test void network_function() throws SQLException {
        assertEquals("192.168.1.0/24", q("SELECT network('192.168.1.5/24'::inet)::text"));
    }

    @Test void inet_merge_same_family() throws SQLException {
        assertEquals("192.168.0.0/22", q("SELECT inet_merge('192.168.1.0/24'::inet, '192.168.2.0/24'::inet)::text"));
    }

    @Test void family_ipv4() throws SQLException {
        assertEquals("4", q("SELECT family('192.168.1.1'::inet)"));
    }

    @Test void family_ipv6() throws SQLException {
        assertEquals("6", q("SELECT family('::1'::inet)"));
    }

    @Test void abbrev_inet_host() throws SQLException {
        assertEquals("192.168.1.1", q("SELECT abbrev('192.168.1.1/32'::inet)"));
    }

    @Test void abbrev_cidr() throws SQLException {
        assertEquals("10/8", q("SELECT abbrev('10.0.0.0/8'::cidr)"));
    }

    @Test void host_function() throws SQLException {
        assertEquals("192.168.1.1", q("SELECT host('192.168.1.1/24'::inet)"));
    }

    @Test void masklen_function() throws SQLException {
        assertEquals("24", q("SELECT masklen('192.168.1.0/24'::inet)"));
    }

    @Test void hostmask_function() throws SQLException {
        assertEquals("0.0.0.255", q("SELECT hostmask('192.168.1.0/24'::inet)"));
    }

    @Test void broadcast_ipv4() throws SQLException {
        assertEquals("192.168.1.255/24", q("SELECT broadcast('192.168.1.0/24'::inet)::text"));
    }

    @Test void inet_same_family_true() throws SQLException {
        assertTrue(qBool("SELECT inet_same_family('192.168.1.1'::inet, '10.0.0.1'::inet)"));
    }

    @Test void inet_same_family_false() throws SQLException {
        assertFalse(qBool("SELECT inet_same_family('192.168.1.1'::inet, '::1'::inet)"));
    }

    // ========================================================================
    // H21: macaddr/macaddr8 normalization and operators
    // ========================================================================

    @Test void macaddr_normalization_colon() throws SQLException {
        assertEquals("12:34:56:78:90:ab", q("SELECT '12:34:56:78:90:AB'::macaddr::text"));
    }

    @Test void macaddr_normalization_dash() throws SQLException {
        assertEquals("12:34:56:78:90:ab", q("SELECT '12-34-56-78-90-AB'::macaddr::text"));
    }

    @Test void macaddr_normalization_dot() throws SQLException {
        assertEquals("12:34:56:78:90:ab", q("SELECT '1234.5678.90AB'::macaddr::text"));
    }

    @Test void macaddr_equality_normalized() throws SQLException {
        assertTrue(qBool("SELECT '12:34:56:78:90:ab'::macaddr = '12-34-56-78-90-AB'::macaddr"));
    }

    @Test void macaddr_invalid_rejected() throws SQLException {
        expectError("SELECT 'zz:zz:zz:zz:zz:zz'::macaddr", "22P02");
    }

    @Test void macaddr_bitwise_not() throws SQLException {
        assertEquals("ed:cb:a9:87:6f:54", q("SELECT (~'12:34:56:78:90:ab'::macaddr)::text"));
    }

    @Test void macaddr_bitwise_and() throws SQLException {
        assertEquals("12:34:56:78:90:ab", q("SELECT ('ff:ff:ff:ff:ff:ff'::macaddr & '12:34:56:78:90:ab'::macaddr)::text"));
    }

    @Test void macaddr_bitwise_or() throws SQLException {
        assertEquals("ff:ff:ff:ff:ff:ff", q("SELECT ('12:34:56:78:90:ab'::macaddr | 'ed:cb:a9:87:6f:54'::macaddr)::text"));
    }

    @Test void macaddr_trunc() throws SQLException {
        assertEquals("12:34:56:00:00:00", q("SELECT trunc('12:34:56:78:90:ab'::macaddr)::text"));
    }

    @Test void macaddr_to_macaddr8() throws SQLException {
        assertEquals("12:34:56:ff:fe:78:90:ab", q("SELECT macaddr8('12:34:56:78:90:ab'::macaddr)::text"));
    }

    @Test void macaddr8_normalization() throws SQLException {
        assertEquals("12:34:56:78:90:ab:cd:ef", q("SELECT '12:34:56:78:90:AB:CD:EF'::macaddr8::text"));
    }

    @Test void macaddr8_set7bit() throws SQLException {
        assertEquals("02:34:56:78:90:ab:cd:ef", q("SELECT macaddr8_set7bit('00:34:56:78:90:ab:cd:ef'::macaddr8)::text"));
    }

    @Test void macaddr8_to_macaddr() throws SQLException {
        assertEquals("12:34:56:78:90:ab", q("SELECT '12:34:56:ff:fe:78:90:ab'::macaddr8::macaddr::text"));
    }

    @Test void macaddr8_trunc() throws SQLException {
        assertEquals("12:34:56:00:00:00:00:00", q("SELECT trunc('12:34:56:78:90:ab:cd:ef'::macaddr8)::text"));
    }

    // ========================================================================
    // L3: inet ordering and display
    // ========================================================================

    @Test void inet_display_with_text_cast() throws SQLException {
        // PG inet::text uses network_show which always includes prefix
        assertEquals("192.168.1.1/32", q("SELECT '192.168.1.1'::inet::text"));
    }

    @Test void inet_display_with_mask() throws SQLException {
        assertEquals("192.168.1.1/24", q("SELECT '192.168.1.1/24'::inet::text"));
    }

    @Test void inet_order_by_correct() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inet_order_test (a inet)");
            s.execute("INSERT INTO inet_order_test VALUES ('192.168.1.0/25'), ('192.168.1.0/24'), ('192.168.1.0/32')");
            ResultSet rs = s.executeQuery("SELECT a::text FROM inet_order_test ORDER BY a");
            assertTrue(rs.next()); assertEquals("192.168.1.0/24", rs.getString(1));
            assertTrue(rs.next()); assertEquals("192.168.1.0/25", rs.getString(1));
            assertTrue(rs.next()); assertEquals("192.168.1.0/32", rs.getString(1));
            s.execute("DROP TABLE inet_order_test");
        }
    }

    @Test void inet_column_storage_roundtrip() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inet_rt (a inet, b cidr, c macaddr, d macaddr8)");
            s.execute("INSERT INTO inet_rt VALUES ('192.168.1.1/24', '10.0.0.0/8', '08:00:2b:01:02:03', '08:00:2b:ff:fe:01:02:03')");
            ResultSet rs = s.executeQuery("SELECT a::text, b::text, c::text, d::text FROM inet_rt");
            assertTrue(rs.next());
            assertEquals("192.168.1.1/24", rs.getString(1));
            assertEquals("10.0.0.0/8", rs.getString(2));
            assertEquals("08:00:2b:01:02:03", rs.getString(3));
            assertEquals("08:00:2b:ff:fe:01:02:03", rs.getString(4));
            s.execute("DROP TABLE inet_rt");
        }
    }
}
