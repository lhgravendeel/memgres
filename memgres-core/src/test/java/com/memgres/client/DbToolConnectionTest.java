package com.memgres.client;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the exact queries a database management tool issues on connection.
 * Each query must succeed (or fail gracefully) for the tool to connect.
 */
class DbToolConnectionTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static List<String> columnNames(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> cols = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) cols.add(md.getColumnName(i));
            return cols;
        }
    }

    // ========================================================================
    // 1. SET DateStyle=ISO
    // ========================================================================
    @Test
    void set_datestyle_iso() throws SQLException {
        exec("SET DateStyle=ISO");
    }

    // ========================================================================
    // 2. SET client_min_messages=notice
    // ========================================================================
    @Test
    void set_client_min_messages() throws SQLException {
        exec("SET client_min_messages=notice");
    }

    // ========================================================================
    // 3. SELECT set_config(...) FROM pg_show_all_settings() WHERE ...
    // ========================================================================
    @Test
    void set_config_from_pg_show_all_settings() throws SQLException {
        List<List<String>> rows = query(
            "SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'");
        assertEquals(1, rows.size(), "Should return exactly 1 row");
        assertEquals("hex", rows.get(0).get(0), "Should return 'hex'");
    }

    // ========================================================================
    // 4. SET client_encoding='utf-8'
    // ========================================================================
    @Test
    void set_client_encoding() throws SQLException {
        exec("SET client_encoding='utf-8'");
    }

    // ========================================================================
    // 5. SELECT version()
    // ========================================================================
    @Test
    void select_version() throws SQLException {
        List<List<String>> rows = query("SELECT version()");
        assertEquals(1, rows.size());
        assertTrue(rows.get(0).get(0).contains("PostgreSQL"),
            "version() should contain 'PostgreSQL': " + rows.get(0).get(0));
    }

    // ========================================================================
    // 6. pg_database query: database info lookup
    // ========================================================================
    @Test
    void pg_database_current_database() throws SQLException {
        List<String> cols = columnNames("""
            SELECT
                db.oid as did, db.datname, db.datallowconn,
                pg_encoding_to_char(db.encoding) AS serverencoding,
                has_database_privilege(db.oid, 'CREATE') as cancreate,
                datistemplate
            FROM
                pg_catalog.pg_database db
            WHERE db.datname = current_database()
            """);
        assertEquals(Cols.listOf("did", "datname", "datallowconn", "serverencoding", "cancreate", "datistemplate"), cols,
            "Column names must match");

        List<List<String>> rows = query("""
            SELECT
                db.oid as did, db.datname, db.datallowconn,
                pg_encoding_to_char(db.encoding) AS serverencoding,
                has_database_privilege(db.oid, 'CREATE') as cancreate,
                datistemplate
            FROM
                pg_catalog.pg_database db
            WHERE db.datname = current_database()
            """);
        assertEquals(1, rows.size(), "Should return exactly 1 row for current database");
        assertNotNull(rows.get(0).get(0), "oid should not be null");
        assertNotNull(rows.get(0).get(1), "datname should not be null");
    }

    // ========================================================================
    // 7. pg_stat_gssapi query: checks GSS auth status
    // ========================================================================
    @Test
    void pg_stat_gssapi_query() throws SQLException {
        // PG returns 1 row with gss_authenticated=false, encrypted=false
        // Memgres can return 0 rows or 1 row; the tool handles both.
        // The key is that it must not CRASH or return an error.
        List<List<String>> rows = query("""
            SELECT
                gss_authenticated, encrypted
            FROM
                pg_catalog.pg_stat_gssapi
            WHERE pid = pg_backend_pid()
            """);
        // PG: 1 row with (false, false) typically
        // Acceptable: 0 rows (no GSS) or 1 row
        assertTrue(rows.size() <= 1, "Should return 0 or 1 rows");
    }

    // ========================================================================
    // 8. Replication/BDR detection query
    // ========================================================================
    @Test
    void replication_detection_query() throws SQLException {
        List<List<String>> rows = query("""
            SELECT CASE
                WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0
                THEN 'pgd'
                WHEN (SELECT COUNT(*) FROM pg_replication_slots) > 0
                THEN 'log'
                ELSE NULL
            END as type
            """);
        assertEquals(1, rows.size(), "Should return exactly 1 row");
        // For memgres: no bdr extension, no replication slots → NULL
        assertNull(rows.get(0).get(0), "Should return NULL (no replication)");

        List<String> cols = columnNames("""
            SELECT CASE
                WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0
                THEN 'pgd'
                WHEN (SELECT COUNT(*) FROM pg_replication_slots) > 0
                THEN 'log'
                ELSE NULL
            END as type
            """);
        assertEquals(Cols.listOf("type"), cols, "Column should be named 'type'");
    }

    // ========================================================================
    // Full sequence: all 8 queries in order (simulating tool connect)
    // ========================================================================
    @Test
    void full_connection_sequence() throws SQLException {
        // This simulates the exact sequence a database management tool sends on connect
        exec("SET DateStyle=ISO");
        exec("SET client_min_messages=notice");

        List<List<String>> r1 = query(
            "SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'");
        assertEquals(1, r1.size(), "set_config query should return 1 row");

        exec("SET client_encoding='utf-8'");

        List<List<String>> r2 = query("SELECT version()");
        assertEquals(1, r2.size());

        List<List<String>> r3 = query("""
            SELECT
                db.oid as did, db.datname, db.datallowconn,
                pg_encoding_to_char(db.encoding) AS serverencoding,
                has_database_privilege(db.oid, 'CREATE') as cancreate,
                datistemplate
            FROM
                pg_catalog.pg_database db
            WHERE db.datname = current_database()
            """);
        assertEquals(1, r3.size(), "pg_database query should return 1 row");

        List<List<String>> r4 = query("""
            SELECT
                gss_authenticated, encrypted
            FROM
                pg_catalog.pg_stat_gssapi
            WHERE pid = pg_backend_pid()
            """);
        assertTrue(r4.size() <= 1);

        List<List<String>> r5 = query("""
            SELECT CASE
                WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0
                THEN 'pgd'
                WHEN (SELECT COUNT(*) FROM pg_replication_slots) > 0
                THEN 'log'
                ELSE NULL
            END as type
            """);
        assertEquals(1, r5.size());
    }
}
