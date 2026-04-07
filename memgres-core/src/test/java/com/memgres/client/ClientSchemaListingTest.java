package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests distinct database client queries for schema/role/tablespace listing.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientSchemaListingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // txid_current() function
    @Test @Order(1) void txid_current() throws SQLException {
        List<List<String>> rows = query("""
            select case
              when pg_catalog.pg_is_in_recovery() then null
              else (pg_catalog.txid_current() % 4294967296)::varchar::bigint
            end as current_txid
            """);
        assertEquals(1, rows.size());
        assertNotNull(rows.get(0).get(0), "txid_current should return a value");
    }

    // pg_namespace.xmin column (used as state_number)
    @Test @Order(2) void namespace_listing_with_xmin() throws SQLException {
        List<List<String>> rows = query("""
            select N.oid::bigint as id,
                   N.xmin as state_number,
                   nspname as name,
                   D.description,
                   pg_catalog.pg_get_userbyid(N.nspowner) as "owner"
            from pg_catalog.pg_namespace N
              left join pg_catalog.pg_description D on N.oid = D.objoid
            order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end
            """);
        assertFalse(rows.isEmpty(), "Should list namespaces");
    }

    // pg_roles with rolinherit, rolreplication, rolconnlimit, rolvaliduntil, rolbypassrls, rolconfig
    @Test @Order(3) void roles_listing_with_all_columns() throws SQLException {
        List<List<String>> rows = query("""
            select R.oid::bigint as role_id, rolname as role_name,
              rolsuper is_super, rolinherit is_inherit,
              rolcreaterole can_createrole, rolcreatedb can_createdb,
              rolcanlogin can_login, rolreplication is_replication,
              rolconnlimit conn_limit, rolvaliduntil valid_until,
              rolbypassrls bypass_rls, rolconfig config,
              D.description
            from pg_catalog.pg_roles R
              left join pg_catalog.pg_shdescription D on D.objoid = R.oid
            """);
        assertFalse(rows.isEmpty(), "Should list at least 1 role");
    }

    // pg_auth_members with member, roleid, admin_option
    @Test @Order(4) void auth_members_listing() throws SQLException {
        List<List<String>> rows = query("""
            select member id, roleid role_id, admin_option
            from pg_catalog.pg_auth_members order by id, roleid::text
            """);
        assertNotNull(rows); // may be empty
    }

    // pg_tablespace with pg_tablespace_location, xmin, spcoptions
    @Test @Order(5) void tablespace_listing() throws SQLException {
        List<List<String>> rows = query("""
            select T.oid::bigint as id, T.spcname as name,
                   T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner,
                   pg_catalog.pg_tablespace_location(T.oid) as location,
                   T.spcoptions as options,
                   D.description as comment
            from pg_catalog.pg_tablespace T
              left join pg_catalog.pg_shdescription D on D.objoid = T.oid
            """);
        assertFalse(rows.isEmpty(), "Should list at least pg_default tablespace");
    }

    // pg_tablespace.spcacl and pg_database.datacl union
    @Test @Order(6) void acl_union() throws SQLException {
        List<List<String>> rows = query("""
            select T.oid as object_id, T.spcacl as acl
            from pg_catalog.pg_tablespace T
            union all
            select T.oid as object_id, T.datacl as acl
            from pg_catalog.pg_database T
            """);
        assertFalse(rows.isEmpty());
    }

    // pg_locks with transactionid column
    @Test @Order(7) void pg_locks_transaction_query() throws SQLException {
        List<List<String>> rows = query("""
            select L.transactionid::varchar::bigint as transaction_id
            from pg_catalog.pg_locks L
            where L.transactionid is not null
            order by pg_catalog.age(L.transactionid) desc
            limit 1
            """);
        assertNotNull(rows); // likely empty
    }

    // pg_timezone_names and pg_timezone_abbrevs union
    @Test @Order(8) void timezone_listing() throws SQLException {
        List<List<String>> rows = query("""
            select name, is_dst from pg_catalog.pg_timezone_names
            union distinct
            select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs
            """);
        assertFalse(rows.isEmpty(), "Should list timezones");
    }

    // Database listing with shdescription join
    @Test @Order(9) void database_listing() throws SQLException {
        List<List<String>> rows = query("""
            select N.oid::bigint as id, datname as name, D.description,
                   datistemplate as is_template, datallowconn as allow_connections,
                   pg_catalog.pg_get_userbyid(N.datdba) as "owner"
            from pg_catalog.pg_database N
              left join pg_catalog.pg_shdescription D on N.oid = D.objoid
            order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end
            """);
        assertFalse(rows.isEmpty());
    }

    // startup time extraction
    @Test @Order(10) void startup_time() throws SQLException {
        List<List<String>> rows = query(
            "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time");
        assertEquals(1, rows.size());
        assertNotNull(rows.get(0).get(0));
    }
}
