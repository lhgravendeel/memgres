package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that focus on the remaining client errors:
 * 1. "No results were returned by the query": SHOW/SET via extended protocol
 * 2. "Bad value for type int : f/t": boolean columns with wrong type OID
 * Uses EXTENDED protocol (default JDBC mode) to match extended protocol behavior.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientExtendedProtocolErrorsTest {

    static Memgres memgres;
    static Connection conn;
    static int nsOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // Setup with simple protocol
        try (Connection setupConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
            setupConn.setAutoCommit(true);
            try (Statement s = setupConn.createStatement()) {
                s.execute("CREATE TABLE users (user_id serial PRIMARY KEY, username text)");
            }
        }
        // Extended protocol connection (extended protocol)
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
                rs.next(); nsOid = rs.getInt(1);
            }
        }
    }

    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP TABLE users"); } conn.close(); }
        if (memgres != null) memgres.close();
    }

    // === "No results were returned by the query" ===

    @Test @Order(1) void show_transaction_isolation_via_extended() throws SQLException {
        // The client sends SHOW via Parse/Bind/Execute
        try (PreparedStatement ps = conn.prepareStatement("SHOW TRANSACTION ISOLATION LEVEL")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "SHOW should return a row via extended protocol");
                assertNotNull(rs.getString(1));
            }
        }
    }

    @Test @Order(2) void show_datestyle_via_extended() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("show DateStyle")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "show DateStyle should return a row");
            }
        }
    }

    @Test @Order(3) void set_via_extended() throws SQLException {
        // SET should work without error
        try (PreparedStatement ps = conn.prepareStatement("SET extra_float_digits = 3")) {
            ps.execute();
        }
    }

    @Test @Order(4) void empty_query_via_extended() throws SQLException {
        // The client sends empty queries: should not error
        try (PreparedStatement ps = conn.prepareStatement("")) {
            ps.execute();
        } catch (SQLException e) {
            // OK if "No results", but shouldn't crash
            if (!e.getMessage().contains("No results")) throw e;
        }
    }

    // === "Bad value for type int : f/t" ===
    // These tests use getBoolean() and getInt() to check type compatibility

    @Test @Order(10) void pg_database_boolean_columns() throws SQLException {
        // datistemplate and datallowconn are boolean
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 select datistemplate as is_template, datallowconn as allow_connections
                 from pg_catalog.pg_database LIMIT 1""")) {
            ResultSetMetaData md = rs.getMetaData();
            // Column type should be BOOLEAN (OID 16), not INT4 (23) or TEXT (25)
            String type1 = md.getColumnTypeName(1);
            String type2 = md.getColumnTypeName(2);
            assertTrue(rs.next());
            // Should be able to call getBoolean without error
            rs.getBoolean(1);
            rs.getBoolean(2);
            // Verify type names
            assertEquals("bool", type1, "is_template should be bool type");
            assertEquals("bool", type2, "allow_connections should be bool type");
        }
    }

    @Test @Order(11) void pg_roles_boolean_columns() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 select rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls
                 from pg_catalog.pg_roles LIMIT 1""")) {
            ResultSetMetaData md = rs.getMetaData();
            assertTrue(rs.next());
            for (int i = 1; i <= 7; i++) {
                String typeName = md.getColumnTypeName(i);
                assertEquals("bool", typeName, "Column " + md.getColumnLabel(i) + " should be bool, got: " + typeName);
                rs.getBoolean(i); // Should not throw
            }
        }
    }

    @Test @Order(12) void pg_user_usesuper_boolean() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("select usesuper from pg_user where usename = current_user")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("bool", md.getColumnTypeName(1), "usesuper should be bool");
            assertTrue(rs.next());
            rs.getBoolean(1); // Should not throw
        }
    }

    @Test @Order(13) void pg_indexam_has_property_boolean() throws SQLException {
        // This is the CROSS JOIN function that was causing "Bad value for type int : f"
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 SELECT amcanorder can_order
                 FROM pg_catalog.pg_class ind_stor
                 CROSS JOIN pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
                 WHERE ind_stor.relkind IN ('i','I') AND ind_stor.relnamespace = """ + nsOid + "::oid LIMIT 1")) {
            ResultSetMetaData md = rs.getMetaData();
            if (rs.next()) {
                String typeName = md.getColumnTypeName(1);
                assertEquals("bool", typeName, "pg_indexam_has_property result should be bool, got: " + typeName);
                rs.getBoolean(1); // Should not throw "Bad value for type int"
            }
        }
    }

    @Test @Order(14) void timezone_is_dst_boolean() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("select name, is_dst from pg_catalog.pg_timezone_names LIMIT 1")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("bool", md.getColumnTypeName(2), "is_dst should be bool");
            assertTrue(rs.next());
            rs.getBoolean(2); // Should not throw
        }
    }

    @Test @Order(15) void timezone_union_boolean_preserved() throws SQLException {
        // The UNION should preserve boolean type from both branches
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 select name, is_dst from pg_catalog.pg_timezone_names
                 union distinct
                 select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs""")) {
            ResultSetMetaData md = rs.getMetaData();
            String typeName = md.getColumnTypeName(2);
            assertEquals("bool", typeName, "is_dst in UNION should be bool, got: " + typeName);
        }
    }

    // === Extended protocol with parameters, via direct TCP ===
    // The client sends $1, $2 etc. directly in PgWire Parse message.
    // We test the SQL works, then separately verify parameter description.

    @Test @Order(20) void cross_join_index_detail_query() throws SQLException {
        // This is the most complex query: CROSS JOIN unnest WITH ORDINALITY + CROSS JOIN pg_indexam_has_property
        // Run with direct value substitution (extended protocol param handling is tested separately)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 select ind_head.indexrelid index_id,
                        k col_idx, k <= indnkeyatts in_key,
                        ind_head.indkey[k-1] column_position,
                        ind_head.indoption[k-1] column_options,
                        ind_head.indcollation[k-1] as collation,
                        colln.nspname as collation_schema,
                        collname as collation_str,
                        ind_head.indclass[k-1] as opclass,
                        case when opcdefault then null else opcn.nspname end as opclass_schema,
                        case when opcdefault then null else opcname end as opclass_str,
                        case
                            when indexprs is null then null
                            when ind_head.indkey[k-1] = 0 then chr(27) || pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)
                            else pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)
                        end as expression,
                        amcanorder can_order
                 from pg_catalog.pg_index ind_head
                          join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid
                 cross join unnest(ind_head.indkey) with ordinality u(u, k)
                 left join pg_catalog.pg_collation on pg_collation.oid = ind_head.indcollation[k-1]
                 left join pg_catalog.pg_namespace colln on collnamespace = colln.oid
                 cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
                          left join pg_catalog.pg_opclass on pg_opclass.oid = ind_head.indclass[k-1]
                          left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid
                 where ind_stor.relnamespace = """ + nsOid + """
                 ::oid
                   and ind_stor.relkind in ('i','I')
                   and pg_catalog.age(ind_stor.xmin) <= coalesce(nullif(greatest(pg_catalog.age('1'::varchar::xid), -1), -1), 2147483647)
                 order by index_id, k""")) {
            ResultSetMetaData md = rs.getMetaData();
            // Check that can_order (last column) is boolean
            int lastCol = md.getColumnCount();
            String typeName = md.getColumnTypeName(lastCol);
            assertEquals("bool", typeName, "can_order should be bool, got: " + typeName);
            if (rs.next()) {
                rs.getBoolean(lastCol); // Should not throw "Bad value for type int"
            }
        }
    }

    // === Verify column type OIDs match PG expectations ===

    @Test @Order(30) void verify_pg_database_column_oids() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 select N.oid::bigint as id, datname as name, D.description,
                        datistemplate as is_template, datallowconn as allow_connections,
                        pg_catalog.pg_get_userbyid(N.datdba) as "owner"
                 from pg_catalog.pg_database N
                   left join pg_catalog.pg_shdescription D on N.oid = D.objoid
                 order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end""")) {
            ResultSetMetaData md = rs.getMetaData();
            // id should be int8/bigint (OID 20)
            assertTrue(md.getColumnType(1) == Types.BIGINT || md.getColumnType(1) == Types.INTEGER,
                    "id type: " + md.getColumnTypeName(1));
            // is_template should be bool (OID 16)
            assertEquals("bool", md.getColumnTypeName(4), "is_template should be bool");
            // allow_connections should be bool (OID 16)
            assertEquals("bool", md.getColumnTypeName(5), "allow_connections should be bool");
        }
    }
}
