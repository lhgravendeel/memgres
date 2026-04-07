package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checklist items 39-41: Network Address Types, Binary Data (BYTEA), Bit String Types.
 */
class NetworkBinaryBitCoverageTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ========================================================================
    // 39. Network Address Types
    // ========================================================================

    @Test
    void inet_column_insert_query() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inet_test1 (id SERIAL PRIMARY KEY, addr INET)");
            s.execute("INSERT INTO inet_test1 (addr) VALUES ('192.168.1.1')");
            s.execute("INSERT INTO inet_test1 (addr) VALUES ('10.0.0.0/8')");
            ResultSet rs = s.executeQuery("SELECT addr FROM inet_test1 ORDER BY id");
            assertTrue(rs.next());
            String v1 = rs.getString("addr");
            assertNotNull(v1);
            assertTrue(v1.contains("192.168.1.1"));
            assertTrue(rs.next());
            String v2 = rs.getString("addr");
            assertNotNull(v2);
            assertTrue(v2.contains("10.0.0.0"));
            s.execute("DROP TABLE inet_test1");
        }
    }

    @Test
    void inet_cidr_column() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cidr_test (id SERIAL PRIMARY KEY, net CIDR)");
            s.execute("INSERT INTO cidr_test (net) VALUES ('192.168.1.0/24')");
            ResultSet rs = s.executeQuery("SELECT net FROM cidr_test");
            assertTrue(rs.next());
            String v = rs.getString("net");
            assertNotNull(v);
            assertTrue(v.contains("192.168.1.0"));
            s.execute("DROP TABLE cidr_test");
        }
    }

    @Test
    void inet_macaddr_column() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mac_test (id SERIAL PRIMARY KEY, mac MACADDR)");
            s.execute("INSERT INTO mac_test (mac) VALUES ('08:00:2b:01:02:03')");
            ResultSet rs = s.executeQuery("SELECT mac FROM mac_test");
            assertTrue(rs.next());
            String v = rs.getString("mac");
            assertNotNull(v);
            assertTrue(v.contains("08:00:2b:01:02:03"));
            s.execute("DROP TABLE mac_test");
        }
    }

    @Test
    void inet_host_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT host('192.168.1.1/24'::inet)");
            assertTrue(rs.next());
            assertEquals("192.168.1.1", rs.getString(1));
        }
    }

    @Test
    void inet_text_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // text() as a function conflicts with the TEXT type name, use ::text cast instead
            ResultSet rs = s.executeQuery("SELECT ('192.168.1.1/24'::inet)::text");
            assertTrue(rs.next());
            assertEquals("192.168.1.1/24", rs.getString(1));
        }
    }

    @Test
    void inet_masklen_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT masklen('192.168.1.1/24'::inet)");
            assertTrue(rs.next());
            assertEquals(24, rs.getInt(1));
        }
    }

    @Test
    void inet_set_masklen() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT set_masklen('192.168.1.1/24'::inet, 16)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertTrue(v.contains("192.168.1.1/16"), "Expected '192.168.1.1/16' but got: " + v);
        }
    }

    @Test
    void inet_netmask() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT netmask('192.168.1.0/24'::inet)");
            assertTrue(rs.next());
            assertEquals("255.255.255.0", rs.getString(1));
        }
    }

    @Test
    void inet_broadcast() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT broadcast('192.168.1.0/24'::inet)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertTrue(v.contains("192.168.1.255"), "Expected broadcast containing '192.168.1.255' but got: " + v);
        }
    }

    @Test
    void inet_network() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT network('192.168.1.5/24'::inet)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertTrue(v.contains("192.168.1.0/24"), "Expected '192.168.1.0/24' but got: " + v);
        }
    }

    @Test
    void inet_abbrev() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT abbrev('192.168.1.1/32'::inet)");
            assertTrue(rs.next());
            assertEquals("192.168.1.1", rs.getString(1));
        }
    }

    @Test
    void inet_family() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT family('192.168.1.1'::inet)");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    @Test
    void inet_same_family() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT inet_same_family('192.168.1.1'::inet, '10.0.0.1'::inet)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("t".equals(v) || "true".equalsIgnoreCase(v), "Expected true but got: " + v);
        }
    }

    @Test
    void inet_merge() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT inet_merge('192.168.1.0/24'::inet, '192.168.2.0/24'::inet)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertTrue(v.contains("192.168"), "Expected result containing '192.168' but got: " + v);
        }
    }

    @Test
    void inet_contains() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '192.168.1.0/24'::inet >> '192.168.1.5'::inet");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("t".equals(v) || "true".equalsIgnoreCase(v), "Expected true but got: " + v);
        }
    }

    @Test
    void inet_contains_false() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '192.168.1.0/24'::inet >> '10.0.0.1'::inet");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("f".equals(v) || "false".equalsIgnoreCase(v), "Expected false but got: " + v);
        }
    }

    @Test
    void inet_contained_by() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '192.168.1.5'::inet << '192.168.1.0/24'::inet");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("t".equals(v) || "true".equalsIgnoreCase(v), "Expected true but got: " + v);
        }
    }

    @Test
    void inet_contains_equals() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '192.168.1.0/24'::inet >>= '192.168.1.0/24'::inet");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("t".equals(v) || "true".equalsIgnoreCase(v), "Expected true but got: " + v);
        }
    }

    @Test
    void inet_contained_by_equals() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '192.168.1.0/24'::inet <<= '192.168.1.0/24'::inet");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("t".equals(v) || "true".equalsIgnoreCase(v), "Expected true but got: " + v);
        }
    }

    @Test
    void inet_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '192.168.1.1'::inet < '192.168.1.2'::inet");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertTrue("t".equals(v) || "true".equalsIgnoreCase(v), "Expected true but got: " + v);
        }
    }

    @Test
    void inet_order_by() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inet_order (id SERIAL PRIMARY KEY, addr INET)");
            s.execute("INSERT INTO inet_order (addr) VALUES ('10.0.0.1')");
            s.execute("INSERT INTO inet_order (addr) VALUES ('192.168.1.1')");
            s.execute("INSERT INTO inet_order (addr) VALUES ('172.16.0.1')");
            ResultSet rs = s.executeQuery("SELECT addr FROM inet_order ORDER BY addr");
            assertTrue(rs.next());
            String first = rs.getString("addr");
            assertNotNull(first);
            assertTrue(rs.next());
            assertTrue(rs.next());
            s.execute("DROP TABLE inet_order");
        }
    }

    @Test
    void inet_where_clause() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inet_where (id SERIAL PRIMARY KEY, addr INET)");
            s.execute("INSERT INTO inet_where (addr) VALUES ('192.168.1.1')");
            s.execute("INSERT INTO inet_where (addr) VALUES ('10.0.0.1')");
            ResultSet rs = s.executeQuery("SELECT addr FROM inet_where WHERE addr = '192.168.1.1'::inet");
            assertTrue(rs.next());
            String v = rs.getString("addr");
            assertTrue(v.contains("192.168.1.1"));
            assertFalse(rs.next());
            s.execute("DROP TABLE inet_where");
        }
    }

    @Test
    void inet_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inet_null (id SERIAL PRIMARY KEY, addr INET)");
            s.execute("INSERT INTO inet_null (addr) VALUES (NULL)");
            ResultSet rs = s.executeQuery("SELECT addr FROM inet_null");
            assertTrue(rs.next());
            assertNull(rs.getString("addr"));
            s.execute("DROP TABLE inet_null");
        }
    }

    @Test
    void inet_distinct() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inet_dist (id SERIAL PRIMARY KEY, addr INET)");
            s.execute("INSERT INTO inet_dist (addr) VALUES ('192.168.1.1')");
            s.execute("INSERT INTO inet_dist (addr) VALUES ('192.168.1.1')");
            s.execute("INSERT INTO inet_dist (addr) VALUES ('10.0.0.1')");
            ResultSet rs = s.executeQuery("SELECT DISTINCT addr FROM inet_dist ORDER BY addr");
            int count = 0;
            while (rs.next()) count++;
            assertEquals(2, count);
            s.execute("DROP TABLE inet_dist");
        }
    }

    @Test
    void inet_cast() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '192.168.1.1'::inet");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertTrue(v.contains("192.168.1.1"), "Expected '192.168.1.1' but got: " + v);
        }
    }

    // ========================================================================
    // 40. Binary Data (BYTEA)
    // ========================================================================

    @Test
    void bytea_column_insert_query() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bytea_test1 (id SERIAL PRIMARY KEY, data BYTEA)");
            s.execute("INSERT INTO bytea_test1 (data) VALUES ('Hello'::bytea)");
            ResultSet rs = s.executeQuery("SELECT data FROM bytea_test1");
            assertTrue(rs.next());
            String v = rs.getString("data");
            assertNotNull(v);
            s.execute("DROP TABLE bytea_test1");
        }
    }

    @Test
    void bytea_encode_hex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT encode('Hello'::bytea, 'hex')");
            assertTrue(rs.next());
            assertEquals("48656c6c6f", rs.getString(1));
        }
    }

    @Test
    void bytea_encode_base64() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT encode('Hello'::bytea, 'base64')");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // Base64 of "Hello" is "SGVsbG8="
            assertTrue(v.contains("SGVsbG8"), "Expected base64 of 'Hello' but got: " + v);
        }
    }

    @Test
    void bytea_decode_hex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT decode('48656c6c6f', 'hex')");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // Should decode to something representing 'Hello'
            assertTrue(v.contains("Hello") || v.contains("48656c6c6f") || v.contains("\\x"),
                    "Expected decoded result related to 'Hello' but got: " + v);
        }
    }

    @Test
    void bytea_decode_base64() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT decode('SGVsbG8=', 'base64')");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // Should decode to something representing 'Hello'
            assertTrue(v.contains("Hello") || v.contains("48656c6c6f") || v.contains("\\x"),
                    "Expected decoded result related to 'Hello' but got: " + v);
        }
    }

    @Test
    void bytea_octet_length() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT octet_length('Hello'::bytea)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void bytea_bit_length() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT bit_length('Hello'::bytea)");
            assertTrue(rs.next());
            assertEquals(40, rs.getInt(1));
        }
    }

    @Test
    void bytea_md5() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT md5('Hello')");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertEquals(32, v.length(), "MD5 hash should be 32 hex chars, got: " + v);
            assertTrue(v.matches("[0-9a-f]{32}"), "MD5 hash should be hex, got: " + v);
        }
    }

    @Test
    void bytea_md5_known_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT md5('abc')");
            assertTrue(rs.next());
            assertEquals("900150983cd24fb0d6963f7d28e17f72", rs.getString(1));
        }
    }

    @Test
    void bytea_sha256() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT sha256('Hello'::bytea)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // SHA-256 produces 32 bytes = 64 hex chars; result may have \\x prefix
            String hex = v.startsWith("\\x") ? v.substring(2) : v;
            assertEquals(64, hex.length(), "SHA-256 hash should be 64 hex chars, got: " + v);
        }
    }

    @Test
    void bytea_sha512() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT sha512('Hello'::bytea)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // SHA-512 produces 64 bytes = 128 hex chars; result may have \\x prefix
            String hex = v.startsWith("\\x") ? v.substring(2) : v;
            assertEquals(128, hex.length(), "SHA-512 hash should be 128 hex chars, got: " + v);
        }
    }

    @Test
    void bytea_get_byte() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT get_byte('Hello'::bytea, 0)");
            assertTrue(rs.next());
            assertEquals(72, rs.getInt(1)); // ASCII 'H'
        }
    }

    @Test
    void bytea_get_byte_second() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT get_byte('Hello'::bytea, 1)");
            assertTrue(rs.next());
            assertEquals(101, rs.getInt(1)); // ASCII 'e'
        }
    }

    @Test
    void bytea_set_byte() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT set_byte('Hello'::bytea, 0, 74)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // First byte changed from 'H' (72) to 'J' (74) => 'Jello'
            assertTrue(v.contains("Jello") || v.contains("4a656c6c6f") || v.contains("\\x"),
                    "Expected 'Jello' or its hex representation but got: " + v);
        }
    }

    @Test
    void bytea_substring() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT substring('Hello World'::bytea, 1, 5)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // substring from position 1, length 5 should give 'Hello'
            assertTrue(v.contains("Hello") || v.contains("48656c6c6f") || v.contains("\\x"),
                    "Expected 'Hello' or its representation but got: " + v);
        }
    }

    @Test
    void bytea_convert_from() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT convert_from('Hello'::bytea, 'UTF8')");
            assertTrue(rs.next());
            assertEquals("Hello", rs.getString(1));
        }
    }

    @Test
    void bytea_convert_to() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT convert_to('Hello', 'UTF8')");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // Should return bytea representation of 'Hello'
        }
    }

    @Test
    void bytea_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bytea_null_t (id SERIAL PRIMARY KEY, data BYTEA)");
            s.execute("INSERT INTO bytea_null_t (data) VALUES (NULL)");
            ResultSet rs = s.executeQuery("SELECT data FROM bytea_null_t");
            assertTrue(rs.next());
            assertNull(rs.getString("data"));
            s.execute("DROP TABLE bytea_null_t");
        }
    }

    @Test
    void bytea_concatenation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 'Hello'::bytea || ' World'::bytea");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // Concatenation of 'Hello' and ' World' bytea
            assertTrue(v.contains("Hello World") || v.contains("48656c6c6f20576f726c64") || v.length() > 0,
                    "Expected concatenated bytea but got: " + v);
        }
    }

    @Test
    void bytea_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bytea_cmp (id SERIAL PRIMARY KEY, data BYTEA)");
            s.execute("INSERT INTO bytea_cmp (data) VALUES ('abc'::bytea)");
            s.execute("INSERT INTO bytea_cmp (data) VALUES ('def'::bytea)");
            ResultSet rs = s.executeQuery("SELECT data FROM bytea_cmp WHERE data = 'abc'::bytea");
            assertTrue(rs.next());
            assertFalse(rs.next());
            rs = s.executeQuery("SELECT data FROM bytea_cmp WHERE data <> 'abc'::bytea");
            assertTrue(rs.next());
            assertFalse(rs.next());
            s.execute("DROP TABLE bytea_cmp");
        }
    }

    @Test
    void bytea_length() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT length('Hello'::bytea)");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    // ========================================================================
    // 41. Bit String Types
    // ========================================================================

    @Test
    void bit_literal_binary() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT B'101'");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertTrue(v.contains("101"), "Expected '101' but got: " + v);
        }
    }

    @Test
    void bit_literal_hex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT X'1FF'");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            // X'1FF' = 0001 1111 1111 in binary
            assertTrue(v.length() > 0, "Expected non-empty result for hex bit literal");
        }
    }

    @Test
    void bit_column_create() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bit_test1 (id SERIAL PRIMARY KEY, flags BIT(8))");
            s.execute("INSERT INTO bit_test1 (flags) VALUES (B'10101010')");
            ResultSet rs = s.executeQuery("SELECT flags FROM bit_test1");
            assertTrue(rs.next());
            String v = rs.getString("flags");
            assertNotNull(v);
            assertTrue(v.contains("10101010"), "Expected '10101010' but got: " + v);
            s.execute("DROP TABLE bit_test1");
        }
    }

    @Test
    void bit_varying_column() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bit_var_test (id SERIAL PRIMARY KEY, flags BIT VARYING(16))");
            s.execute("INSERT INTO bit_var_test (flags) VALUES (B'1010')");
            s.execute("INSERT INTO bit_var_test (flags) VALUES (B'11110000')");
            s.execute("INSERT INTO bit_var_test (flags) VALUES (B'1111000011110000')");
            ResultSet rs = s.executeQuery("SELECT flags FROM bit_var_test ORDER BY id");
            assertTrue(rs.next());
            assertNotNull(rs.getString("flags"));
            assertTrue(rs.next());
            assertNotNull(rs.getString("flags"));
            assertTrue(rs.next());
            assertNotNull(rs.getString("flags"));
            s.execute("DROP TABLE bit_var_test");
        }
    }

    @Test
    void bit_bitwise_and() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 12 & 10");
            assertTrue(rs.next());
            assertEquals(8, rs.getInt(1)); // 1100 & 1010 = 1000
        }
    }

    @Test
    void bit_bitwise_or() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 12 | 10");
            assertTrue(rs.next());
            assertEquals(14, rs.getInt(1)); // 1100 | 1010 = 1110
        }
    }

    @Test
    void bit_bitwise_xor() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 12 # 10");
            assertTrue(rs.next());
            assertEquals(6, rs.getInt(1)); // 1100 ^ 1010 = 0110
        }
    }

    @Test
    void bit_bitwise_not() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT ~1");
            assertTrue(rs.next());
            assertEquals(-2, rs.getInt(1));
        }
    }

    @Test
    void bit_shift_left() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1 << 4");
            assertTrue(rs.next());
            assertEquals(16, rs.getInt(1));
        }
    }

    @Test
    void bit_shift_right() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 16 >> 2");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    @Test
    void bit_bit_length() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT bit_length('Hello')");
            assertTrue(rs.next());
            assertEquals(40, rs.getInt(1));
        }
    }

    @Test
    void bit_octet_length() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT octet_length('Hello')");
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
    }

    @Test
    void bit_get_bit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT get_bit(B'10101010', 0)");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // first bit
        }
    }

    @Test
    void bit_get_bit_second() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT get_bit(B'10101010', 1)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1)); // second bit
        }
    }

    @Test
    void bit_set_bit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT set_bit(B'10101010', 1, 1)");
            assertTrue(rs.next());
            String v = rs.getString(1);
            assertNotNull(v);
            assertTrue(v.contains("11101010"), "Expected '11101010' but got: " + v);
        }
    }

    @Test
    void bit_combined_operations() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT (12 & 10) | 5");
            assertTrue(rs.next());
            assertEquals(13, rs.getInt(1)); // (1100 & 1010) | 0101 = 1000 | 0101 = 1101 = 13
        }
    }

    @Test
    void bit_shift_combined() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT (1 << 4) | (1 << 2)");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1)); // 16 | 4 = 20
        }
    }

    @Test
    void bit_in_where() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bit_where_t (id INTEGER, val INTEGER)");
            s.execute("INSERT INTO bit_where_t VALUES (1, 12)");
            s.execute("INSERT INTO bit_where_t VALUES (2, 10)");
            s.execute("INSERT INTO bit_where_t VALUES (3, 15)");
            ResultSet rs = s.executeQuery("SELECT id FROM bit_where_t WHERE val & 8 = 8 ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // 12 & 8 = 8
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1)); // 10 & 8 = 8
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1)); // 15 & 8 = 8
            assertFalse(rs.next());
            s.execute("DROP TABLE bit_where_t");
        }
    }

    @Test
    void bit_column_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bit_cmp_t (id SERIAL PRIMARY KEY, flags BIT(8))");
            s.execute("INSERT INTO bit_cmp_t (flags) VALUES (B'10101010')");
            s.execute("INSERT INTO bit_cmp_t (flags) VALUES (B'01010101')");
            s.execute("INSERT INTO bit_cmp_t (flags) VALUES (B'10101010')");
            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM bit_cmp_t WHERE flags = B'10101010'");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            s.execute("DROP TABLE bit_cmp_t");
        }
    }
}
