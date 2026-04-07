package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs every distinct query from a DB tool's connection sequence and reports
 * exactly what memgres returns or what error it throws.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DbToolConnectionDebugTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }

    @Test @Order(1) void q01_set_datestyle() throws SQLException { exec("SET DateStyle=ISO"); }
    @Test @Order(2) void q02_set_client_min_messages() throws SQLException { exec("SET client_min_messages=notice"); }
    @Test @Order(3) void q03_set_config_bytea() throws SQLException {
        List<List<String>> r = query("SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'");
        assertEquals(1, r.size()); assertEquals("hex", r.get(0).get(0));
    }
    @Test @Order(4) void q04_set_encoding() throws SQLException { exec("SET client_encoding='utf-8'"); }
    @Test @Order(5) void q05_version() throws SQLException {
        List<List<String>> r = query("SELECT version()");
        assertEquals(1, r.size()); assertTrue(r.get(0).get(0).contains("18.0"));
    }
    @Test @Order(6) void q06_database_info() throws SQLException {
        List<List<String>> r = query("""
            SELECT db.oid as did, db.datname, db.datallowconn,
                pg_encoding_to_char(db.encoding) AS serverencoding,
                has_database_privilege(db.oid, 'CREATE') as cancreate, datistemplate
            FROM pg_catalog.pg_database db WHERE db.datname = current_database()
            """);
        assertEquals(1, r.size());
    }
    @Test @Order(7) void q07_gssapi() throws SQLException {
        List<List<String>> r = query("""
            SELECT gss_authenticated, encrypted
            FROM pg_catalog.pg_stat_gssapi WHERE pid = pg_backend_pid()
            """);
        assertTrue(r.size() <= 1);
    }
    @Test @Order(8) void q08_roles_with_recursive_cte() throws SQLException {
        List<List<String>> r = query("""
            SELECT roles.oid as id, roles.rolname as name, roles.rolsuper as is_superuser,
                CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreaterole END as can_create_role,
                CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreatedb END as can_create_db,
                CASE WHEN 'pg_signal_backend'=ANY(ARRAY(WITH RECURSIVE cte AS (
                    SELECT pg_roles.oid,pg_roles.rolname FROM pg_roles WHERE pg_roles.oid = roles.oid
                    UNION ALL
                    SELECT m.roleid,pgr.rolname FROM cte cte_1
                        JOIN pg_auth_members m ON m.member = cte_1.oid
                        JOIN pg_roles pgr ON pgr.oid = m.roleid)
                SELECT rolname FROM cte)) THEN True ELSE False END as can_signal_backend
            FROM pg_catalog.pg_roles as roles WHERE rolname = current_user
            """);
        assertEquals(1, r.size(), "Should return 1 row for current user");
    }

    // NEW query not in previous test: pg_is_in_recovery / pg_is_wal_replay_paused
    @Test @Order(9) void q09_recovery_status() throws SQLException {
        List<List<String>> r = query("""
            SELECT CASE WHEN usesuper
                   THEN pg_catalog.pg_is_in_recovery()
                   ELSE FALSE
                   END as inrecovery,
                   CASE WHEN usesuper AND pg_catalog.pg_is_in_recovery()
                   THEN pg_is_wal_replay_paused()
                   ELSE FALSE
                   END as isreplaypaused
            FROM pg_catalog.pg_user WHERE usename=current_user
            """);
        assertEquals(1, r.size(), "Should return 1 row");
        // PG returns: (false, false), not in recovery
        assertEquals("f", r.get(0).get(0), "inrecovery should be false");
        assertEquals("f", r.get(0).get(1), "isreplaypaused should be false");
    }

    @Test @Order(10) void q10_replication_check() throws SQLException {
        List<List<String>> r = query("""
            SELECT CASE
                WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0 THEN 'pgd'
                WHEN (SELECT COUNT(*) FROM pg_replication_slots) > 0 THEN 'log'
                ELSE NULL
            END as type
            """);
        assertEquals(1, r.size());
    }

    // NEW: pg_cancel_backend, DB tools send this as a keepalive/cancel mechanism
    @Test @Order(11) void q11_pg_cancel_backend() throws SQLException {
        // PG: pg_cancel_backend(pid) returns boolean and cancels a backend process
        // For memgres: should return false (nothing to cancel) or at minimum not crash
        List<List<String>> r = query("SELECT pg_cancel_backend(1)");
        assertEquals(1, r.size());
    }

    // Full sequence test that mimics the exact DB tool connection flow
    @Test @Order(12) void full_connection_sequence() throws SQLException {
        exec("SET DateStyle=ISO");
        exec("SET client_min_messages=notice");
        query("SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'");
        exec("SET client_encoding='utf-8'");
        query("SELECT version()");
        query("""
            SELECT db.oid as did, db.datname, db.datallowconn,
                pg_encoding_to_char(db.encoding) AS serverencoding,
                has_database_privilege(db.oid, 'CREATE') as cancreate, datistemplate
            FROM pg_catalog.pg_database db WHERE db.datname = current_database()
            """);
        query("SELECT gss_authenticated, encrypted FROM pg_catalog.pg_stat_gssapi WHERE pid = pg_backend_pid()");
        query("""
            SELECT roles.oid as id, roles.rolname as name, roles.rolsuper as is_superuser,
                CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreaterole END as can_create_role,
                CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreatedb END as can_create_db,
                CASE WHEN 'pg_signal_backend'=ANY(ARRAY(WITH RECURSIVE cte AS (
                    SELECT pg_roles.oid,pg_roles.rolname FROM pg_roles WHERE pg_roles.oid = roles.oid
                    UNION ALL
                    SELECT m.roleid,pgr.rolname FROM cte cte_1
                        JOIN pg_auth_members m ON m.member = cte_1.oid
                        JOIN pg_roles pgr ON pgr.oid = m.roleid)
                SELECT rolname FROM cte)) THEN True ELSE False END as can_signal_backend
            FROM pg_catalog.pg_roles as roles WHERE rolname = current_user
            """);
        query("""
            SELECT CASE WHEN usesuper THEN pg_catalog.pg_is_in_recovery() ELSE FALSE END as inrecovery,
                   CASE WHEN usesuper AND pg_catalog.pg_is_in_recovery() THEN pg_is_wal_replay_paused() ELSE FALSE END as isreplaypaused
            FROM pg_catalog.pg_user WHERE usename=current_user
            """);
        query("""
            SELECT CASE
                WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0 THEN 'pgd'
                WHEN (SELECT COUNT(*) FROM pg_replication_slots) > 0 THEN 'log'
                ELSE NULL
            END as type
            """);
    }
}
