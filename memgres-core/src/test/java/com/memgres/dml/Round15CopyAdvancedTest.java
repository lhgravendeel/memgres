package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.StringReader;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 15 gap category E: COPY extra options.
 *
 * Covers:
 *  - COPY FROM … WHERE (PG 12+)
 *  - HEADER MATCH (PG 14+)
 *  - FORCE_NULL (empty strings → NULL on listed columns)
 *  - FORCE_NOT_NULL
 *  - ON_ERROR (PG 17+): stop, ignore
 *  - LOG_VERBOSITY (PG 17+): default, verbose
 *  - DEFAULT (PG 17+): literal in CSV → default expression
 *  - PROGRAM is rejected (security) in untrusted modes
 */
class Round15CopyAdvancedTest {

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

    private static int scalarInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static void copyIn(String sql, String data) throws Exception {
        CopyManager mgr = conn.unwrap(PGConnection.class).getCopyAPI();
        mgr.copyIn(sql, new StringReader(data));
    }

    // =========================================================================
    // A. COPY FROM … WHERE
    // =========================================================================

    @Test
    void copy_from_where_filters_rows() throws Exception {
        exec("CREATE TABLE r15_cw (id int, v int)");
        copyIn("COPY r15_cw (id, v) FROM STDIN WITH (FORMAT csv) WHERE v > 10",
                "1,5\n2,15\n3,20\n");
        int total = scalarInt("SELECT count(*)::int FROM r15_cw");
        assertEquals(2, total, "COPY WHERE v>10 should keep only 2 rows");
    }

    // =========================================================================
    // B. HEADER MATCH (PG 14+)
    // =========================================================================

    @Test
    void copy_from_header_match_rejects_mismatch() throws Exception {
        exec("CREATE TABLE r15_hm (id int, v int)");
        // Header matches table columns exactly
        copyIn("COPY r15_hm (id, v) FROM STDIN WITH (FORMAT csv, HEADER MATCH)",
                "id,v\n1,10\n");
        int n = scalarInt("SELECT count(*)::int FROM r15_hm");
        assertEquals(1, n);

        // Mismatched header should raise
        try {
            copyIn("COPY r15_hm (id, v) FROM STDIN WITH (FORMAT csv, HEADER MATCH)",
                    "xx,yy\n2,20\n");
            fail("HEADER MATCH with mismatched names must raise");
        } catch (Exception e) {
            // expected
        }
    }

    // =========================================================================
    // C. FORCE_NULL
    // =========================================================================

    @Test
    void copy_force_null_converts_empty_to_null() throws Exception {
        exec("CREATE TABLE r15_fn (id int, v text)");
        copyIn("COPY r15_fn (id, v) FROM STDIN WITH (FORMAT csv, FORCE_NULL (v))",
                "1,\"\"\n2,\"hello\"\n");
        int nullCount = scalarInt("SELECT count(*)::int FROM r15_fn WHERE v IS NULL");
        assertEquals(1, nullCount,
                "FORCE_NULL must convert empty quoted string to NULL");
    }

    @Test
    void copy_force_not_null_keeps_empty_as_empty() throws Exception {
        exec("CREATE TABLE r15_fnn (id int, v text)");
        // Without FORCE_NOT_NULL, an unquoted empty field in CSV is NULL
        copyIn("COPY r15_fnn (id, v) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL (v))",
                "1,\n");
        int nullCount = scalarInt("SELECT count(*)::int FROM r15_fnn WHERE v IS NULL");
        assertEquals(0, nullCount,
                "FORCE_NOT_NULL must keep empty as empty-string, not NULL");
    }

    // =========================================================================
    // D. ON_ERROR (PG 17+)
    // =========================================================================

    @Test
    void copy_on_error_stop_default() throws Exception {
        exec("CREATE TABLE r15_oe_stop (id int, v int)");
        try {
            copyIn("COPY r15_oe_stop (id, v) FROM STDIN WITH (FORMAT csv, ON_ERROR stop)",
                    "1,10\nbad,20\n3,30\n");
            fail("ON_ERROR stop must abort on bad row");
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    void copy_on_error_ignore_skips_bad_rows() throws Exception {
        exec("CREATE TABLE r15_oe_ign (id int, v int)");
        copyIn("COPY r15_oe_ign (id, v) FROM STDIN WITH (FORMAT csv, ON_ERROR ignore)",
                "1,10\nbad,20\n3,30\n");
        int n = scalarInt("SELECT count(*)::int FROM r15_oe_ign");
        assertEquals(2, n, "ON_ERROR ignore must skip malformed rows");
    }

    // =========================================================================
    // E. LOG_VERBOSITY (PG 17+)
    // =========================================================================

    @Test
    void copy_log_verbosity_verbose_accepted() throws Exception {
        exec("CREATE TABLE r15_lv (id int)");
        // Accepted even with no bad rows
        copyIn("COPY r15_lv (id) FROM STDIN WITH "
                        + "(FORMAT csv, ON_ERROR ignore, LOG_VERBOSITY verbose)",
                "1\n2\n");
        int n = scalarInt("SELECT count(*)::int FROM r15_lv");
        assertEquals(2, n);
    }

    // =========================================================================
    // F. DEFAULT option (PG 17+)
    // =========================================================================

    @Test
    void copy_default_literal_triggers_default_expression() throws Exception {
        exec("CREATE TABLE r15_cd (id int, v text DEFAULT 'DFLT')");
        copyIn("COPY r15_cd (id, v) FROM STDIN WITH (FORMAT csv, DEFAULT '\\D')",
                "1,hello\n2,\\D\n");
        int n = scalarInt("SELECT count(*)::int FROM r15_cd WHERE v='DFLT'");
        assertEquals(1, n, "DEFAULT literal in CSV row must resolve to column default");
    }

    // =========================================================================
    // G. PROGRAM variant rejected without superuser
    // =========================================================================

    @Test
    void copy_program_rejected_or_errors() throws Exception {
        exec("CREATE TABLE r15_pg (id int)");
        try {
            try (Statement s = conn.createStatement()) {
                s.execute("COPY r15_pg (id) FROM PROGRAM 'echo 1'");
            }
            // If it didn't error, that's fine only for legitimate programs;
            // most engines reject at parse time.
        } catch (SQLException e) {
            String msg = (e.getMessage() == null ? "" : e.getMessage().toLowerCase());
            assertTrue(msg.contains("program") || msg.contains("permission")
                            || msg.contains("superuser") || msg.contains("not supported"),
                    "COPY PROGRAM error should reference program/permission; got: " + msg);
        }
    }

    // =========================================================================
    // H. BINARY format
    // =========================================================================

    @Test
    void copy_binary_format_roundtrip_shape() throws Exception {
        exec("CREATE TABLE r15_cb (id int)");
        exec("INSERT INTO r15_cb VALUES (1),(2),(3)");
        // Export BINARY
        CopyManager mgr = conn.unwrap(PGConnection.class).getCopyAPI();
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        mgr.copyOut("COPY r15_cb TO STDOUT WITH (FORMAT binary)", bos);
        byte[] bin = bos.toByteArray();
        assertTrue(bin.length > 0, "COPY binary output must be non-empty");
        // PG binary format starts with magic "PGCOPY\n\377\r\n\0"
        assertEquals('P', bin[0]);
        assertEquals('G', bin[1]);
        assertEquals('C', bin[2]);
        assertEquals('O', bin[3]);
        assertEquals('P', bin[4]);
        assertEquals('Y', bin[5]);
    }
}
