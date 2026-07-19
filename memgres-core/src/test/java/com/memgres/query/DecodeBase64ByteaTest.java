package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that decode('...', 'base64') returns bytea (byte[]), not a UTF-8 String.
 * PG: decode returns bytea; converting non-UTF-8 bytes to String corrupts them.
 */
class DecodeBase64ByteaTest {

    static Memgres memgres;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }
    @AfterAll static void tearDown() throws Exception { if (memgres != null) memgres.close(); }

    Connection newConn() throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
    }

    @Test void decode_base64_returns_bytea() throws Exception {
        // encode('hello', 'base64') = 'aGVsbG8='
        // decode('aGVsbG8=', 'base64') should return bytea \x68656c6c6f
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT decode('aGVsbG8=', 'base64') AS result");
            assertTrue(rs.next());
            byte[] bytes = rs.getBytes("result");
            assertArrayEquals(new byte[]{'h', 'e', 'l', 'l', 'o'}, bytes);
        }
    }

    @Test void decode_base64_non_utf8_bytes() throws Exception {
        // Base64 for bytes 0xFF 0xFE: '/v4=' (or more precisely hex FFFE = //4=')
        // Actually: FF FE in base64 = //4=
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT decode('//4=', 'base64') AS result");
            assertTrue(rs.next());
            byte[] bytes = rs.getBytes("result");
            assertArrayEquals(new byte[]{(byte) 0xFF, (byte) 0xFE}, bytes);
        }
    }

    @Test void decode_hex_returns_bytea() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT decode('deadbeef', 'hex') AS result");
            assertTrue(rs.next());
            byte[] bytes = rs.getBytes("result");
            assertArrayEquals(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, bytes);
        }
    }

    @Test void encode_decode_roundtrip() throws Exception {
        try (Connection c = newConn(); Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT encode(decode('AQID', 'base64'), 'base64') AS result");
            assertTrue(rs.next());
            assertEquals("AQID", rs.getString("result"));
        }
    }
}
