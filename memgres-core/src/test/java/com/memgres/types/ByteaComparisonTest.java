package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for BYTEA storage, comparison, and parameter binding.
 *
 * PostgreSQL supports BYTEA (byte array) columns with several input formats:
 *   - Hex format: '\xDEADBEEF' or E'\\xDEADBEEF'
 *   - Escape format: '\001\002\003'
 *   - JDBC setBytes(): sends as hex-escaped literal in simple query mode
 *
 * The critical scenario: when BYTEA values are stored via INSERT and then
 * compared via WHERE using JDBC PreparedStatement.setBytes(), the PG JDBC
 * driver in simple query mode sends the byte array as a hex-escaped string
 * literal (e.g., '\xDEADBEEF'). The server must correctly match this against
 * the stored binary representation.
 *
 * This is important for:
 *   - Workflow engines (store execution IDs as BYTEA)
 *   - Any app using binary primary keys or tokens
 *   - JDBC frameworks that use setBytes() for BYTEA columns
 */
class ByteaComparisonTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // Basic BYTEA storage and retrieval
    // =========================================================================

    @Test
    void testInsertAndSelectByteaHexFormat() throws SQLException {
        exec("CREATE TABLE ba_hex (id serial PRIMARY KEY, data bytea)");
        exec("INSERT INTO ba_hex (data) VALUES ('\\xDEADBEEF')");
        assertNotNull(query1("SELECT data FROM ba_hex WHERE id = 1"));
    }

    @Test
    void testInsertByteaEscapeFormat() throws SQLException {
        exec("CREATE TABLE ba_esc (id serial PRIMARY KEY, data bytea)");
        exec("INSERT INTO ba_esc (data) VALUES (E'\\\\xCAFEBABE')");
        assertNotNull(query1("SELECT data FROM ba_esc WHERE id = 1"));
    }

    @Test
    void testInsertByteaEmptyArray() throws SQLException {
        exec("CREATE TABLE ba_empty (id serial PRIMARY KEY, data bytea)");
        exec("INSERT INTO ba_empty (data) VALUES ('\\x')");
        assertNotNull(query1("SELECT data FROM ba_empty WHERE id = 1"));
    }

    @Test
    void testInsertByteaNull() throws SQLException {
        exec("CREATE TABLE ba_null (id serial PRIMARY KEY, data bytea)");
        exec("INSERT INTO ba_null (data) VALUES (NULL)");
        assertEquals("1", query1("SELECT COUNT(*) FROM ba_null WHERE data IS NULL"));
    }

    // =========================================================================
    // BYTEA equality comparison with literal
    // =========================================================================

    @Test
    void testByteaEqualityWithHexLiteral() throws SQLException {
        exec("CREATE TABLE ba_eq (id serial PRIMARY KEY, token bytea NOT NULL)");
        exec("INSERT INTO ba_eq (token) VALUES ('\\xDEADBEEF')");
        exec("INSERT INTO ba_eq (token) VALUES ('\\xCAFEBABE')");
        assertEquals("1", query1("SELECT id FROM ba_eq WHERE token = '\\xDEADBEEF'"));
    }

    @Test
    void testByteaEqualityWithEscapeLiteral() throws SQLException {
        exec("CREATE TABLE ba_eq2 (id serial PRIMARY KEY, token bytea NOT NULL)");
        exec("INSERT INTO ba_eq2 (token) VALUES (E'\\\\xABCD')");
        assertEquals("1", query1("SELECT COUNT(*) FROM ba_eq2 WHERE token = E'\\\\xABCD'"));
    }

    @Test
    void testByteaInequalityComparison() throws SQLException {
        exec("CREATE TABLE ba_neq (id serial PRIMARY KEY, data bytea)");
        exec("INSERT INTO ba_neq (data) VALUES ('\\xAA'), ('\\xBB'), ('\\xCC')");
        assertEquals("2", query1("SELECT COUNT(*) FROM ba_neq WHERE data != '\\xBB'"));
    }

    // =========================================================================
    // BYTEA comparison via PreparedStatement.setBytes()
    // =========================================================================

    @Test
    void testPreparedByteaInsertAndSelect() throws SQLException {
        exec("CREATE TABLE ba_prep (id serial PRIMARY KEY, token bytea NOT NULL, label text)");
        byte[] tokenValue = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};

        // Insert via PreparedStatement
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ba_prep (token, label) VALUES (?, ?)")) {
            ps.setBytes(1, tokenValue);
            ps.setString(2, "test_token");
            ps.executeUpdate();
        }

        // Select back and verify
        try (PreparedStatement ps = conn.prepareStatement("SELECT label FROM ba_prep WHERE token = ?")) {
            ps.setBytes(1, tokenValue);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Should find row by BYTEA equality via PreparedStatement");
                assertEquals("test_token", rs.getString(1));
            }
        }
    }

    @Test
    void testPreparedByteaComparisonMultipleRows() throws SQLException {
        exec("CREATE TABLE ba_multi (id serial PRIMARY KEY, key bytea NOT NULL, value text)");

        byte[] key1 = new byte[]{0x01, 0x02, 0x03};
        byte[] key2 = new byte[]{0x04, 0x05, 0x06};
        byte[] key3 = new byte[]{0x07, 0x08, 0x09};

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ba_multi (key, value) VALUES (?, ?)")) {
            ps.setBytes(1, key1); ps.setString(2, "first"); ps.executeUpdate();
            ps.setBytes(1, key2); ps.setString(2, "second"); ps.executeUpdate();
            ps.setBytes(1, key3); ps.setString(2, "third"); ps.executeUpdate();
        }

        // Find specific key
        try (PreparedStatement ps = conn.prepareStatement("SELECT value FROM ba_multi WHERE key = ?")) {
            ps.setBytes(1, key2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("second", rs.getString(1));
                assertFalse(rs.next(), "Should match exactly one row");
            }
        }
    }

    @Test
    void testPreparedByteaNotFound() throws SQLException {
        exec("CREATE TABLE ba_notfound (id serial PRIMARY KEY, token bytea NOT NULL)");
        exec("INSERT INTO ba_notfound (token) VALUES ('\\xAABBCC')");

        byte[] differentToken = new byte[]{(byte) 0xFF, (byte) 0xEE, (byte) 0xDD};
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM ba_notfound WHERE token = ?")) {
            ps.setBytes(1, differentToken);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // =========================================================================
    // BYTEA as primary key with PreparedStatement lookup
    // =========================================================================

    @Test
    void testByteaPrimaryKeyLookup() throws SQLException {
        exec("CREATE TABLE ba_pk (id bytea PRIMARY KEY, name text)");

        byte[] id1 = new byte[]{0x10, 0x20, 0x30, 0x40};
        byte[] id2 = new byte[]{0x50, 0x60, 0x70, (byte) 0x80};

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ba_pk (id, name) VALUES (?, ?)")) {
            ps.setBytes(1, id1); ps.setString(2, "alpha"); ps.executeUpdate();
            ps.setBytes(1, id2); ps.setString(2, "beta"); ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM ba_pk WHERE id = ?")) {
            ps.setBytes(1, id1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("alpha", rs.getString(1));
            }
        }
    }

    @Test
    void testByteaCompositePrimaryKeyLookup() throws SQLException {
        exec("CREATE TABLE ba_cpk (part_a bytea NOT NULL, part_b bytea NOT NULL, data text, PRIMARY KEY (part_a, part_b))");

        byte[] a1 = new byte[]{0x01};
        byte[] b1 = new byte[]{0x02};
        byte[] b2 = new byte[]{0x03};

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ba_cpk (part_a, part_b, data) VALUES (?, ?, ?)")) {
            ps.setBytes(1, a1); ps.setBytes(2, b1); ps.setString(3, "row1"); ps.executeUpdate();
            ps.setBytes(1, a1); ps.setBytes(2, b2); ps.setString(3, "row2"); ps.executeUpdate();
        }

        try (PreparedStatement ps = conn.prepareStatement("SELECT data FROM ba_cpk WHERE part_a = ? AND part_b = ?")) {
            ps.setBytes(1, a1);
            ps.setBytes(2, b2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("row2", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // BYTEA in IN clause with PreparedStatement
    // =========================================================================

    @Test
    void testByteaInClauseWithPrepared() throws SQLException {
        exec("CREATE TABLE ba_in (id serial PRIMARY KEY, token bytea NOT NULL, label text)");
        exec("INSERT INTO ba_in (token, label) VALUES ('\\x0101', 'a'), ('\\x0202', 'b'), ('\\x0303', 'c')");

        try (PreparedStatement ps = conn.prepareStatement("SELECT label FROM ba_in WHERE token = ?")) {
            ps.setBytes(1, new byte[]{0x02, 0x02});
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // BYTEA comparison with decode() function
    // =========================================================================

    @Test
    void testByteaComparisonWithDecode() throws SQLException {
        exec("CREATE TABLE ba_decode (id serial PRIMARY KEY, data bytea)");
        exec("INSERT INTO ba_decode (data) VALUES (decode('DEADBEEF', 'hex'))");
        assertEquals("1", query1("SELECT COUNT(*) FROM ba_decode WHERE data = decode('DEADBEEF', 'hex')"));
    }

    @Test
    void testByteaInsertWithDecodeAndPreparedLookup() throws SQLException {
        exec("CREATE TABLE ba_dec2 (id bytea PRIMARY KEY, value text)");
        exec("INSERT INTO ba_dec2 (id, value) VALUES (decode('CAFEBABE', 'hex'), 'coffee')");

        byte[] cafeBytes = new byte[]{(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
        try (PreparedStatement ps = conn.prepareStatement("SELECT value FROM ba_dec2 WHERE id = ?")) {
            ps.setBytes(1, cafeBytes);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("coffee", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // BYTEA ordering and range comparison
    // =========================================================================

    @Test
    void testByteaOrderBy() throws SQLException {
        exec("CREATE TABLE ba_order (id serial PRIMARY KEY, data bytea)");
        exec("INSERT INTO ba_order (data) VALUES ('\\x03'), ('\\x01'), ('\\x02')");
        assertEquals("\\x01", query1("SELECT data FROM ba_order ORDER BY data ASC LIMIT 1"));
    }

    @Test
    void testByteaGreaterThan() throws SQLException {
        exec("CREATE TABLE ba_gt (id serial PRIMARY KEY, data bytea)");
        exec("INSERT INTO ba_gt (data) VALUES ('\\x01'), ('\\x05'), ('\\x09')");
        assertEquals("2", query1("SELECT COUNT(*) FROM ba_gt WHERE data > '\\x02'"));
    }

    // =========================================================================
    // BYTEA with length and octet_length functions
    // =========================================================================

    @Test
    void testByteaLength() throws SQLException {
        exec("CREATE TABLE ba_len (id serial PRIMARY KEY, data bytea)");
        exec("INSERT INTO ba_len (data) VALUES ('\\xDEADBEEF')");
        assertEquals("4", query1("SELECT octet_length(data) FROM ba_len WHERE id = 1"));
    }

    // =========================================================================
    // Mixed: insert via literal, select via PreparedStatement (the real bug)
    // =========================================================================

    @Test
    void testInsertLiteralSelectPrepared() throws SQLException {
        // This is the core scenario: data inserted as hex literal,
        // then queried via JDBC setBytes()
        exec("CREATE TABLE ba_mixed (id bytea PRIMARY KEY, name text)");
        exec("INSERT INTO ba_mixed (id, name) VALUES ('\\xAABBCCDD', 'found_me')");

        byte[] searchKey = new byte[]{(byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD};
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM ba_mixed WHERE id = ?")) {
            ps.setBytes(1, searchKey);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Literal-inserted BYTEA must match PreparedStatement.setBytes()");
                assertEquals("found_me", rs.getString(1));
            }
        }
    }

    @Test
    void testInsertPreparedSelectLiteral() throws SQLException {
        // Reverse: insert via PreparedStatement, query with literal
        exec("CREATE TABLE ba_mixed2 (id bytea PRIMARY KEY, name text)");

        byte[] key = new byte[]{(byte) 0x11, (byte) 0x22, (byte) 0x33};
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ba_mixed2 (id, name) VALUES (?, ?)")) {
            ps.setBytes(1, key);
            ps.setString(2, "prepped");
            ps.executeUpdate();
        }

        assertEquals("prepped", query1("SELECT name FROM ba_mixed2 WHERE id = '\\x112233'"));
    }

    // =========================================================================
    // BYTEA with UPDATE via PreparedStatement
    // =========================================================================

    @Test
    void testUpdateByteaViaPrepared() throws SQLException {
        exec("CREATE TABLE ba_upd (id bytea PRIMARY KEY, version int DEFAULT 0)");
        exec("INSERT INTO ba_upd (id) VALUES ('\\xABCD')");

        byte[] key = new byte[]{(byte) 0xAB, (byte) 0xCD};
        try (PreparedStatement ps = conn.prepareStatement("UPDATE ba_upd SET version = version + 1 WHERE id = ?")) {
            ps.setBytes(1, key);
            int updated = ps.executeUpdate();
            assertEquals(1, updated, "Should update exactly one row by BYTEA key");
        }

        assertEquals("1", query1("SELECT version FROM ba_upd WHERE id = '\\xABCD'"));
    }

    @Test
    void testDeleteByteaViaPrepared() throws SQLException {
        exec("CREATE TABLE ba_del (id bytea PRIMARY KEY, data text)");
        exec("INSERT INTO ba_del (id, data) VALUES ('\\xFF01', 'delete_me'), ('\\xFF02', 'keep_me')");

        byte[] deleteKey = new byte[]{(byte) 0xFF, 0x01};
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM ba_del WHERE id = ?")) {
            ps.setBytes(1, deleteKey);
            assertEquals(1, ps.executeUpdate());
        }

        assertEquals("1", query1("SELECT COUNT(*) FROM ba_del"));
        assertEquals("keep_me", query1("SELECT data FROM ba_del"));
    }
}
