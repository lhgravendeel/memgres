package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.math.BigDecimal;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 13 (Java/JDBC): Large values and streaming.
 * Tests handling of large text, bytea, varchar values, special characters,
 * edge cases for empty vs null, maximum integer values, precise numerics,
 * large text in WHERE clauses, SQL concatenation, and bytea hex encoding.
 */
class LargeValuesStreamingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // --- 1. Large text value (1MB string) insert and retrieval ---

    @Test void large_text_1mb_insert_and_retrieval() throws Exception {
        exec("CREATE TABLE lv_large_text(id int PRIMARY KEY, content text)");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1024 * 1024; i++) {
            sb.append((char) ('A' + (i % 26)));
        }
        String megaString = sb.toString();
        assertEquals(1024 * 1024, megaString.length());

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_large_text VALUES (1, ?)")) {
            ps.setString(1, megaString);
            assertEquals(1, ps.executeUpdate());
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT content FROM lv_large_text WHERE id = 1")) {
            assertTrue(rs.next());
            String retrieved = rs.getString(1);
            assertEquals(megaString.length(), retrieved.length(), "Retrieved text length should match");
            assertEquals(megaString, retrieved, "Retrieved text content should match exactly");
        }
        exec("DROP TABLE lv_large_text");
    }

    // --- 2. Large bytea value insert and retrieval ---

    @Test void large_bytea_insert_and_retrieval() throws Exception {
        exec("CREATE TABLE lv_large_bytea(id int PRIMARY KEY, data bytea)");
        byte[] largeBytes = new byte[512 * 1024]; // 512KB
        for (int i = 0; i < largeBytes.length; i++) {
            largeBytes[i] = (byte) (i % 256);
        }

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_large_bytea VALUES (1, ?)")) {
            ps.setBytes(1, largeBytes);
            assertEquals(1, ps.executeUpdate());
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT data FROM lv_large_bytea WHERE id = 1")) {
            assertTrue(rs.next());
            byte[] retrieved = rs.getBytes(1);
            assertEquals(largeBytes.length, retrieved.length, "Retrieved bytea length should match");
            assertArrayEquals(largeBytes, retrieved, "Retrieved bytea content should match exactly");
        }
        exec("DROP TABLE lv_large_bytea");
    }

    // --- 3. Very long varchar value ---

    @Test void very_long_varchar_value() throws Exception {
        exec("CREATE TABLE lv_long_varchar(id int PRIMARY KEY, val varchar(100000))");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 50000; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        String longVarchar = sb.toString();

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_long_varchar VALUES (1, ?)")) {
            ps.setString(1, longVarchar);
            assertEquals(1, ps.executeUpdate());
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT val FROM lv_long_varchar WHERE id = 1")) {
            assertTrue(rs.next());
            String retrieved = rs.getString(1);
            assertEquals(longVarchar.length(), retrieved.length());
            assertEquals(longVarchar, retrieved);
        }
        exec("DROP TABLE lv_long_varchar");
    }

    // --- 4. Multiple large rows ---

    @Test void multiple_large_rows() throws Exception {
        exec("CREATE TABLE lv_multi_large(id int PRIMARY KEY, content text)");
        int rowCount = 10;
        int sizePerRow = 100_000; // 100KB each
        String[] values = new String[rowCount];

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_multi_large VALUES (?, ?)")) {
            for (int r = 0; r < rowCount; r++) {
                StringBuilder sb = new StringBuilder(sizePerRow);
                char base = (char) ('A' + r);
                for (int i = 0; i < sizePerRow; i++) {
                    sb.append(base);
                }
                values[r] = sb.toString();
                ps.setInt(1, r);
                ps.setString(2, values[r]);
                ps.executeUpdate();
            }
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, content FROM lv_multi_large ORDER BY id")) {
            for (int r = 0; r < rowCount; r++) {
                assertTrue(rs.next(), "Should have row " + r);
                assertEquals(r, rs.getInt("id"));
                String retrieved = rs.getString("content");
                assertEquals(sizePerRow, retrieved.length(), "Row " + r + " length mismatch");
                assertEquals(values[r], retrieved, "Row " + r + " content mismatch");
            }
            assertFalse(rs.next(), "Should have no more rows");
        }
        exec("DROP TABLE lv_multi_large");
    }

    // --- 5. Text value with special characters (unicode, newlines, tabs, null bytes) ---

    @Test @Timeout(10) void text_with_special_characters() throws Exception {
        exec("CREATE TABLE lv_special_chars(id int PRIMARY KEY, content text)");

        // Unicode characters: emoji, CJK, Arabic, accented
        String unicode = "Hello \u00e9\u00e0\u00fc\u00f1 \u4e16\u754c \u0645\u0631\u062d\u0628\u0627 \ud83d\ude00\ud83c\udf0d";
        // Newlines and tabs
        String whitespace = "line1\nline2\nline3\ttabbed\r\nwindows-line";

        // Test unicode
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_special_chars VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, unicode);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT content FROM lv_special_chars WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(unicode, rs.getString(1));
        }

        // Test whitespace characters
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_special_chars VALUES (?, ?)")) {
            ps.setInt(1, 2);
            ps.setString(2, whitespace);
            ps.executeUpdate();
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT content FROM lv_special_chars WHERE id = 2")) {
            assertTrue(rs.next());
            assertEquals(whitespace, rs.getString(1));
        }

        // Note: null byte (\u0000) in text is rejected by PostgreSQL and can hang
        // the wire protocol, so we skip testing it here. PG returns:
        // ERROR: invalid byte sequence for encoding "UTF8": 0x00

        exec("DROP TABLE lv_special_chars");
    }

    // --- 6. Empty string vs null text ---

    @Test void empty_string_vs_null_text() throws Exception {
        exec("CREATE TABLE lv_empty_null_text(id int PRIMARY KEY, val text)");

        // Insert empty string
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_empty_null_text VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "");
            ps.executeUpdate();
        }

        // Insert null
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_empty_null_text VALUES (?, ?)")) {
            ps.setInt(1, 2);
            ps.setNull(2, Types.VARCHAR);
            ps.executeUpdate();
        }

        // Verify empty string
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT val FROM lv_empty_null_text WHERE id = 1")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val, "Empty string should not be null");
            assertEquals("", val, "Should be empty string");
            assertFalse(rs.wasNull());
        }

        // Verify null
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT val FROM lv_empty_null_text WHERE id = 2")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1), "Null text should return null");
            assertTrue(rs.wasNull());
        }

        // Verify they are distinguishable via IS NULL
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM lv_empty_null_text WHERE val IS NULL")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "Only one row should have NULL");
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM lv_empty_null_text WHERE val = ''")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "Only one row should have empty string");
        }

        exec("DROP TABLE lv_empty_null_text");
    }

    // --- 7. Empty bytea vs null bytea ---

    @Test void empty_bytea_vs_null_bytea() throws Exception {
        exec("CREATE TABLE lv_empty_null_bytea(id int PRIMARY KEY, data bytea)");

        // Insert empty bytea
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_empty_null_bytea VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, new byte[0]);
            ps.executeUpdate();
        }

        // Insert null bytea
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_empty_null_bytea VALUES (?, ?)")) {
            ps.setInt(1, 2);
            ps.setNull(2, Types.BINARY);
            ps.executeUpdate();
        }

        // Verify empty bytea
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT data FROM lv_empty_null_bytea WHERE id = 1")) {
            assertTrue(rs.next());
            byte[] val = rs.getBytes(1);
            assertNotNull(val, "Empty bytea should not be null");
            assertEquals(0, val.length, "Empty bytea should have zero length");
            assertFalse(rs.wasNull());
        }

        // Verify null bytea
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT data FROM lv_empty_null_bytea WHERE id = 2")) {
            assertTrue(rs.next());
            assertNull(rs.getBytes(1), "Null bytea should return null");
            assertTrue(rs.wasNull());
        }

        // Distinguish via IS NULL
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM lv_empty_null_bytea WHERE data IS NULL")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM lv_empty_null_bytea WHERE data IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }

        exec("DROP TABLE lv_empty_null_bytea");
    }

    // --- 8. Maximum integer values (Integer.MAX_VALUE, Long.MAX_VALUE) ---

    @Test void maximum_integer_values() throws Exception {
        exec("CREATE TABLE lv_max_int(id int PRIMARY KEY, int_val int, bigint_val bigint, smallint_val smallint)");

        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO lv_max_int VALUES (?, ?, ?, ?)")) {
            // Max values
            ps.setInt(1, 1);
            ps.setInt(2, Integer.MAX_VALUE);
            ps.setLong(3, Long.MAX_VALUE);
            ps.setShort(4, Short.MAX_VALUE);
            ps.executeUpdate();

            // Min values
            ps.setInt(1, 2);
            ps.setInt(2, Integer.MIN_VALUE);
            ps.setLong(3, Long.MIN_VALUE);
            ps.setShort(4, Short.MIN_VALUE);
            ps.executeUpdate();

            // Zero
            ps.setInt(1, 3);
            ps.setInt(2, 0);
            ps.setLong(3, 0L);
            ps.setShort(4, (short) 0);
            ps.executeUpdate();
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT int_val, bigint_val, smallint_val FROM lv_max_int WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(Integer.MAX_VALUE, rs.getInt("int_val"));
            assertEquals(Long.MAX_VALUE, rs.getLong("bigint_val"));
            assertEquals(Short.MAX_VALUE, rs.getShort("smallint_val"));
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT int_val, bigint_val, smallint_val FROM lv_max_int WHERE id = 2")) {
            assertTrue(rs.next());
            assertEquals(Integer.MIN_VALUE, rs.getInt("int_val"));
            assertEquals(Long.MIN_VALUE, rs.getLong("bigint_val"));
            assertEquals(Short.MIN_VALUE, rs.getShort("smallint_val"));
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT int_val, bigint_val, smallint_val FROM lv_max_int WHERE id = 3")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("int_val"));
            assertEquals(0L, rs.getLong("bigint_val"));
            assertEquals((short) 0, rs.getShort("smallint_val"));
        }

        exec("DROP TABLE lv_max_int");
    }

    // --- 9. Very precise numeric/decimal values ---

    @Test void very_precise_numeric_decimal_values() throws Exception {
        exec("CREATE TABLE lv_precise_num(id int PRIMARY KEY, val numeric(40,20))");

        BigDecimal veryPrecise = new BigDecimal("12345678901234567890.12345678901234567890");
        BigDecimal tiny = new BigDecimal("0.00000000000000000001");
        BigDecimal negative = new BigDecimal("-99999999999999999999.99999999999999999999");
        BigDecimal zero = BigDecimal.ZERO;

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_precise_num VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBigDecimal(2, veryPrecise);
            ps.executeUpdate();

            ps.setInt(1, 2);
            ps.setBigDecimal(2, tiny);
            ps.executeUpdate();

            ps.setInt(1, 3);
            ps.setBigDecimal(2, negative);
            ps.executeUpdate();

            ps.setInt(1, 4);
            ps.setBigDecimal(2, zero);
            ps.executeUpdate();
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT val FROM lv_precise_num WHERE id = 1")) {
            assertTrue(rs.next());
            BigDecimal retrieved = rs.getBigDecimal(1);
            assertEquals(0, veryPrecise.compareTo(retrieved),
                    "Very precise value should round-trip exactly: expected " + veryPrecise + " got " + retrieved);
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT val FROM lv_precise_num WHERE id = 2")) {
            assertTrue(rs.next());
            BigDecimal retrieved = rs.getBigDecimal(1);
            assertEquals(0, tiny.compareTo(retrieved),
                    "Tiny value should round-trip exactly: expected " + tiny + " got " + retrieved);
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT val FROM lv_precise_num WHERE id = 3")) {
            assertTrue(rs.next());
            BigDecimal retrieved = rs.getBigDecimal(1);
            assertEquals(0, negative.compareTo(retrieved),
                    "Negative value should round-trip exactly: expected " + negative + " got " + retrieved);
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT val FROM lv_precise_num WHERE id = 4")) {
            assertTrue(rs.next());
            BigDecimal retrieved = rs.getBigDecimal(1);
            assertEquals(0, BigDecimal.ZERO.compareTo(retrieved),
                    "Zero should round-trip exactly");
        }

        exec("DROP TABLE lv_precise_num");
    }

    // --- 10. Large text in WHERE clause comparison ---

    @Test void large_text_in_where_clause() throws Exception {
        exec("CREATE TABLE lv_where_text(id int PRIMARY KEY, val text)");

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10_000; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        String largeVal = sb.toString();

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_where_text VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, largeVal);
            ps.executeUpdate();

            ps.setInt(1, 2);
            ps.setString(2, "short");
            ps.executeUpdate();
        }

        // Query using large text in WHERE clause with parameter
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM lv_where_text WHERE val = ?")) {
            ps.setString(1, largeVal);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Should find row matching large text");
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next(), "Should find exactly one match");
            }
        }

        // Query with slightly different large text should not match
        String almostSame = largeVal.substring(0, largeVal.length() - 1) + "Z";
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM lv_where_text WHERE val = ?")) {
            ps.setString(1, almostSame);
            try (ResultSet rs = ps.executeQuery()) {
                assertFalse(rs.next(), "Slightly different text should not match");
            }
        }

        // LIKE with large text prefix
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT id FROM lv_where_text WHERE val LIKE ?")) {
            ps.setString(1, largeVal.substring(0, 100) + "%");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "LIKE with large text prefix should match");
                assertEquals(1, rs.getInt(1));
            }
        }

        exec("DROP TABLE lv_where_text");
    }

    // --- 11. Large text concatenation in SQL ---

    @Test void large_text_concatenation_in_sql() throws Exception {
        exec("CREATE TABLE lv_concat(id int PRIMARY KEY, part1 text, part2 text)");

        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            sb1.append('X');
            sb2.append('Y');
        }
        String part1 = sb1.toString();
        String part2 = sb2.toString();

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_concat VALUES (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, part1);
            ps.setString(3, part2);
            ps.executeUpdate();
        }

        // Concatenate in SQL and verify
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT part1 || part2 AS combined, length(part1 || part2) AS combined_len FROM lv_concat WHERE id = 1")) {
            assertTrue(rs.next());
            String combined = rs.getString("combined");
            assertEquals(10000, rs.getInt("combined_len"), "Combined length should be 10000");
            assertEquals(10000, combined.length());
            assertEquals(part1 + part2, combined, "Concatenation should match expected");
        }

        // Concatenation with a literal
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT part1 || '---' || part2 AS with_sep FROM lv_concat WHERE id = 1")) {
            assertTrue(rs.next());
            String result = rs.getString("with_sep");
            assertEquals(10003, result.length(), "Should be 5000 + 3 + 5000");
            assertTrue(result.startsWith(part1));
            assertTrue(result.contains("---"));
            assertTrue(result.endsWith(part2));
        }

        // LENGTH function on large text
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT length(part1), length(part2) FROM lv_concat WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(5000, rs.getInt(1));
            assertEquals(5000, rs.getInt(2));
        }

        exec("DROP TABLE lv_concat");
    }

    // --- 12. Bytea hex encoding round-trip ---

    @Test void bytea_hex_encoding_round_trip() throws Exception {
        exec("CREATE TABLE lv_bytea_hex(id int PRIMARY KEY, data bytea)");

        // Test known byte patterns
        byte[] pattern1 = new byte[]{0x00, 0x01, 0x7F, (byte) 0x80, (byte) 0xFF};
        byte[] pattern2 = new byte[256];
        for (int i = 0; i < 256; i++) {
            pattern2[i] = (byte) i;
        }

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO lv_bytea_hex VALUES (?, ?)")) {
            ps.setInt(1, 1);
            ps.setBytes(2, pattern1);
            ps.executeUpdate();

            ps.setInt(1, 2);
            ps.setBytes(2, pattern2);
            ps.executeUpdate();
        }

        // Retrieve via getBytes and verify exact match
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT data FROM lv_bytea_hex WHERE id = 1")) {
            assertTrue(rs.next());
            assertArrayEquals(pattern1, rs.getBytes(1),
                    "Boundary byte values should round-trip exactly");
        }

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT data FROM lv_bytea_hex WHERE id = 2")) {
            assertTrue(rs.next());
            byte[] retrieved = rs.getBytes(1);
            assertArrayEquals(pattern2, retrieved,
                    "All 256 byte values should round-trip exactly");
        }

        // Verify length via SQL
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT octet_length(data) FROM lv_bytea_hex WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1));
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT octet_length(data) FROM lv_bytea_hex WHERE id = 2")) {
            assertTrue(rs.next());
            assertEquals(256, rs.getInt(1));
        }

        // Insert via hex literal and retrieve
        exec("INSERT INTO lv_bytea_hex VALUES (3, '\\x48656c6c6f')"); // "Hello" in hex
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT data FROM lv_bytea_hex WHERE id = 3")) {
            assertTrue(rs.next());
            byte[] retrieved = rs.getBytes(1);
            assertArrayEquals("Hello".getBytes("UTF-8"), retrieved,
                    "Hex-encoded literal should decode to 'Hello'");
        }

        // Bytea concatenation in SQL
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT data || '\\x4142'::bytea AS combined FROM lv_bytea_hex WHERE id = 3")) {
            assertTrue(rs.next());
            byte[] combined = rs.getBytes(1);
            assertEquals(7, combined.length, "Hello(5) + AB(2) = 7 bytes");
            assertArrayEquals("HelloAB".getBytes("UTF-8"), combined);
        }

        exec("DROP TABLE lv_bytea_hex");
    }
}
