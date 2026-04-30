package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 16 gap category B: Advanced DML / COPY fidelity.
 *
 * Covers:
 *  - ON CONFLICT DO UPDATE … WHERE predicate must be enforced
 *  - COPY … WITH (ENCODING 'LATIN1') must actually decode bytes with that encoding
 *  - COPY … HEADER MATCH must reject mismatched header column names
 */
class Round16DmlCopyAdvancedTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // =========================================================================
    // B1. ON CONFLICT DO UPDATE … WHERE predicate
    // =========================================================================

    @Test
    void on_conflict_do_update_where_false_must_skip_update() throws SQLException {
        exec("CREATE TABLE r16_onc (id int PRIMARY KEY, v int, touched boolean DEFAULT false)");
        exec("INSERT INTO r16_onc (id, v, touched) VALUES (1, 10, false)");

        // WHERE predicate is always false — UPDATE must NOT run.
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO r16_onc (id, v) VALUES (1, 99) "
                    + "ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v, touched = true "
                    + "WHERE false");
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT v, touched FROM r16_onc WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(10, rs.getInt("v"),
                    "v must remain 10 when ON CONFLICT ... WHERE false (update must be skipped)");
            assertFalse(rs.getBoolean("touched"),
                    "touched must remain false when ON CONFLICT ... WHERE false");
        }
    }

    @Test
    void on_conflict_do_update_where_true_must_run_update() throws SQLException {
        exec("CREATE TABLE r16_onc2 (id int PRIMARY KEY, v int)");
        exec("INSERT INTO r16_onc2 VALUES (1, 10)");
        exec("INSERT INTO r16_onc2 (id, v) VALUES (1, 99) "
                + "ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE true");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT v FROM r16_onc2 WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(99, rs.getInt(1), "WHERE true baseline must update normally");
        }
    }

    // =========================================================================
    // B2. COPY ... WITH (ENCODING 'LATIN1') must honor the encoding
    // =========================================================================

    @Test
    void copy_with_encoding_latin1_decodes_accented_bytes() throws Exception {
        exec("CREATE TABLE r16_copyenc (s text)");
        PGConnection pg = conn.unwrap(PGConnection.class);
        CopyManager cm = pg.getCopyAPI();

        // 0xE9 in Latin-1 = 'é'. In UTF-8 that same byte is the start of an invalid sequence.
        byte[] latin1 = new byte[]{'c', 'a', 'f', (byte) 0xE9, '\n'};

        try {
            cm.copyIn(
                    "COPY r16_copyenc (s) FROM STDIN WITH (FORMAT text, ENCODING 'LATIN1')",
                    new java.io.ByteArrayInputStream(latin1));
        } catch (SQLException ignored) {
            // If the engine rejects ENCODING entirely, that's also a failure:
            fail("COPY ... WITH (ENCODING 'LATIN1') must be supported; got: " + ignored.getMessage());
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT s FROM r16_copyenc")) {
            assertTrue(rs.next());
            assertEquals("café", rs.getString(1),
                    "LATIN1-encoded 0xE9 must decode to 'é' under ENCODING 'LATIN1'");
        }
    }

    // =========================================================================
    // B3. COPY ... HEADER MATCH must reject mismatched column names
    // =========================================================================

    @Test
    void copy_header_match_rejects_mismatched_header() throws Exception {
        exec("CREATE TABLE r16_hdrmatch (a int, b int)");
        PGConnection pg = conn.unwrap(PGConnection.class);
        CopyManager cm = pg.getCopyAPI();

        // Header claims column "wrong_name" instead of "a" — HEADER MATCH must error.
        String csv = "wrong_name,b\n1,2\n";
        try {
            cm.copyIn(
                    "COPY r16_hdrmatch (a, b) FROM STDIN WITH (FORMAT csv, HEADER MATCH)",
                    new StringReader(csv));
            fail("COPY ... HEADER MATCH must reject a header with wrong column names");
        } catch (SQLException e) {
            String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            assertTrue(msg.contains("header") || msg.contains("match") || msg.contains("mismatch")
                            || msg.contains("column"),
                    "HEADER MATCH rejection must mention header/column mismatch; got: " + e.getMessage());
        }
    }
}
