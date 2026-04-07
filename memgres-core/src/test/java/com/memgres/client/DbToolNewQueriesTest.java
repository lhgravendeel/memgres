package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests new distinct DB tool queries from the latest batch:
 * - Trigger listing (pg_trigger with tgisinternal, tgenabled, tgname)
 * - Rule listing (pg_rewrite with ev_enabled, ev_class, rulename)
 * - Policy listing (pg_policy with polname, polrelid)
 * - Partition listing (pg_inherits with inhdetachpending, pg_get_partkeydef, reltoastrelid, reloptions)
 * - Column introspection (already tested with seq.seqtypid)
 * - DISTINCT ON index listing (known parser bug with correlated subquery)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DbToolNewQueriesTest {

    static Memgres memgres;
    static Connection conn;
    static String tableOid;
    static String nsOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA myapp");
            s.execute("SET search_path = myapp");
            s.execute("CREATE TABLE users(user_id serial PRIMARY KEY, username text NOT NULL)");
            s.execute("CREATE INDEX idx_username ON users(username)");
            try (ResultSet rs = s.executeQuery("SELECT 'myapp.users'::regclass::oid")) { rs.next(); tableOid = rs.getString(1); }
            try (ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'myapp'")) { rs.next(); nsOid = rs.getString(1); }
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP SCHEMA myapp CASCADE"); } conn.close(); }
        if (memgres != null) memgres.close();
    }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }

    // NEW: Trigger listing query
    @Test @Order(1) void trigger_listing() throws SQLException {
        List<List<String>> rows = query("""
            SELECT t.oid, t.tgname as name, t.tgenabled AS is_enable_trigger, des.description
            FROM pg_catalog.pg_trigger t
                LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=t.oid AND des.classoid='pg_trigger'::regclass)
            WHERE NOT tgisinternal
                AND tgrelid = """ + tableOid + "::OID ORDER BY tgname");
        // users table has no triggers
        assertEquals(0, rows.size());
    }

    // NEW: Rule listing query (pg_rewrite)
    @Test @Order(2) void rule_listing() throws SQLException {
        List<List<String>> rows = query("""
            SELECT rw.oid AS oid, rw.rulename AS name,
                CASE WHEN rw.ev_enabled != 'D' THEN True ELSE False END AS enabled,
                rw.ev_enabled AS is_enable_rule,
                description AS comment
            FROM pg_catalog.pg_rewrite rw
                LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=rw.oid AND des.classoid='pg_rewrite'::regclass)
            WHERE rw.ev_class = """ + tableOid + " ORDER BY rw.rulename");
        // Should not crash; pg_rewrite needs ev_class, ev_enabled, rulename columns
        assertNotNull(rows);
    }

    // NEW: Policy listing query (pg_policy)
    @Test @Order(3) void policy_listing() throws SQLException {
        List<List<String>> rows = query("""
            SELECT pl.oid AS oid, pl.polname AS name
            FROM pg_catalog.pg_policy pl
            WHERE pl.polrelid = """ + tableOid + " ORDER BY pl.polname");
        assertEquals(0, rows.size());
    }

    // NEW: Partition listing (complex query with inhdetachpending, reltoastrelid, reloptions, pg_get_partkeydef)
    @Test @Order(4) void partition_listing() throws SQLException {
        List<List<String>> rows = query("""
            SELECT rel.oid, rel.relname AS name,
                (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE) AS triggercount,
                (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE AND tgenabled = 'O') AS has_enable_triggers,
                pg_catalog.pg_get_expr(rel.relpartbound, rel.oid) AS partition_value,
                rel.relnamespace AS schema_id,
                nsp.nspname AS schema_name,
                (CASE WHEN rel.relkind = 'p' THEN true ELSE false END) AS is_partitioned,
                (CASE WHEN rel.relkind = 'p' THEN true ELSE false END) AS is_sub_partitioned,
                (CASE WHEN rel.relkind = 'p' THEN pg_catalog.pg_get_partkeydef(rel.oid::oid) ELSE '' END) AS partition_scheme,
                (CASE WHEN rel.relkind = 'p' THEN pg_catalog.pg_get_partkeydef(rel.oid::oid) ELSE '' END) AS sub_partition_scheme,
                (CASE WHEN rel.relpersistence = 'u' THEN true ELSE false END) AS relpersistence,
                (CASE WHEN length(spc.spcname::text) > 0 THEN spc.spcname ELSE
                    (SELECT sp.spcname FROM pg_catalog.pg_database dtb
                    JOIN pg_catalog.pg_tablespace sp ON dtb.dattablespace=sp.oid
                    WHERE dtb.oid = (SELECT oid FROM pg_database WHERE datname = current_database()))
                END) as spcname,
                substring(pg_catalog.array_to_string(rel.reloptions, ',') FROM 'fillfactor=([0-9]*)') AS fillfactor,
                rel.reloptions AS reloptions, tst.reloptions AS toast_reloptions, rel.reloftype, typ.typname,
                typ.typrelid AS typoid, des.description, pg_catalog.pg_get_userbyid(rel.relowner) AS relowner,
                inh.inhdetachpending, am.amname
            FROM
                (SELECT * FROM pg_catalog.pg_inherits WHERE inhparent = """ + tableOid + """
                ::oid) inh
                LEFT JOIN pg_catalog.pg_class rel ON inh.inhrelid = rel.oid
                LEFT JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid
                LEFT OUTER JOIN pg_catalog.pg_class tst ON tst.oid = rel.reltoastrelid
                LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=rel.oid AND des.objsubid=0 AND des.classoid='pg_class'::regclass)
                LEFT OUTER JOIN pg_catalog.pg_tablespace spc on spc.oid=rel.reltablespace
                LEFT OUTER JOIN pg_catalog.pg_am am ON am.oid = rel.relam
                LEFT JOIN pg_catalog.pg_type typ ON rel.reloftype=typ.oid
            WHERE rel.relispartition
            ORDER BY rel.relname
            """);
        // users is not partitioned, so no partitions
        assertEquals(0, rows.size());
    }

    // pg_rewrite needs ev_class, ev_enabled, rulename columns
    @Test @Order(5) void pg_rewrite_has_required_columns() throws SQLException {
        query("SELECT oid, rulename, ev_class, ev_enabled FROM pg_catalog.pg_rewrite LIMIT 0");
    }

    // pg_policy needs polrelid, polname columns
    @Test @Order(6) void pg_policy_has_required_columns() throws SQLException {
        query("SELECT oid, polname, polrelid FROM pg_catalog.pg_policy LIMIT 0");
    }

    // pg_inherits needs inhdetachpending column
    @Test @Order(7) void pg_inherits_has_inhdetachpending() throws SQLException {
        query("SELECT inhrelid, inhparent, inhdetachpending FROM pg_catalog.pg_inherits LIMIT 0");
    }

    // pg_class needs reltoastrelid, reloptions, reloftype, relpersistence, relpartbound
    @Test @Order(8) void pg_class_partition_columns() throws SQLException {
        query("SELECT reltoastrelid, reloptions, reloftype, relpersistence FROM pg_catalog.pg_class LIMIT 1");
    }

    // pg_get_partkeydef function
    @Test @Order(9) void pg_get_partkeydef_function() throws SQLException {
        // Should return empty string or null for non-partitioned table
        List<List<String>> rows = query("SELECT pg_catalog.pg_get_partkeydef(" + tableOid + "::oid)");
        assertEquals(1, rows.size());
    }

    // pg_get_expr function with relpartbound
    @Test @Order(10) void pg_get_expr_relpartbound() throws SQLException {
        List<List<String>> rows = query("SELECT pg_catalog.pg_get_expr(rel.relpartbound, rel.oid) FROM pg_catalog.pg_class rel WHERE rel.oid = " + tableOid + "::oid");
        assertEquals(1, rows.size());
    }
}
