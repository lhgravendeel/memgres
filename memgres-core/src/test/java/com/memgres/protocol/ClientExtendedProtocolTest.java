package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests using PreparedStatement to force extended query protocol (Parse/Bind/Describe/Execute).
 * This is how database clients actually send queries.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientExtendedProtocolTest {

    static Memgres memgres;
    static Connection conn;
    static int nsOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // Use simple mode for setup, extended for actual tests
        try (Connection setupConn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword())) {
            setupConn.setAutoCommit(true);
            try (Statement s = setupConn.createStatement()) {
                try { s.execute("DROP TABLE IF EXISTS ext_t"); } catch (Exception ignored) {}
                s.execute("CREATE TABLE ext_t(id serial PRIMARY KEY, name text)");
            }
        }
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO ext_t(name) VALUES ('test')");
            try (ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
                rs.next(); nsOid = rs.getInt(1);
            }
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP TABLE ext_t"); } conn.close(); }
        if (memgres != null) memgres.close();
    }

    // SHOW via extended protocol: PreparedStatement forces Parse/Describe/Execute
    @Test @Order(1) void show_transaction_isolation_via_prepared() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SHOW TRANSACTION ISOLATION LEVEL")) {
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "SHOW should return 1 row via extended protocol");
                assertNotNull(rs.getString(1));
            }
        }
    }

    // The 7-branch UNION with bound parameters
    @Test @Order(2) void seven_branch_union_with_params() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            select T.oid as oid, relnamespace as schemaId,
                   pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind, relname as name
            from pg_catalog.pg_class T
            where relnamespace in ( ? ) and relkind in ('r','m','v','p','f','S')
            union all
            select T.oid, T.typnamespace, 'T', T.typname
            from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid
            where T.typnamespace in ( ? )
              and (T.typtype in ('d','e') or C.relkind = 'c' or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or T.typtype = 'p' and not T.typisdefined)
            union all
            select oid, collnamespace, 'C', collname from pg_catalog.pg_collation where collnamespace in ( ? )
            union all
            select oid, oprnamespace, 'O', oprname from pg_catalog.pg_operator where oprnamespace in ( ? )
            union all
            select oid, opcnamespace, 'c', opcname from pg_catalog.pg_opclass where opcnamespace in ( ? )
            union all
            select oid, opfnamespace, 'F', opfname from pg_catalog.pg_opfamily where opfnamespace in ( ? )
            union all
            select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end, proname
            from pg_catalog.pg_proc where pronamespace in ( ? )
            order by schemaId
            """)) {
            for (int i = 1; i <= 7; i++) ps.setInt(i, nsOid);
            try (ResultSet rs = ps.executeQuery()) {
                // Should return rows without UNION column count error
                int n = 0; while (rs.next()) n++;
                assertTrue(n >= 0); // 0 or more rows
            }
        }
    }

    // Cross join with pg_indexam_has_property, boolean type via extended protocol
    @Test @Order(3) void cross_join_boolean_via_prepared() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            select ind_head.indexrelid index_id, amcanorder can_order
            from pg_catalog.pg_index ind_head
                join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid
            cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
            where ind_stor.relnamespace = ?::oid and ind_stor.relkind in ('i','I')
            """)) {
            ps.setInt(1, nsOid);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // Should NOT throw "bad value for type int: f"
                    Object val = rs.getObject("can_order");
                    assertNotNull(val);
                }
            }
        }
    }

    // ACL union with age() and xid parameters
    @Test @Order(4) void acl_union_with_age_params() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            select T.oid as object_id, T.relacl as acl
            from pg_catalog.pg_class T
            where relnamespace = ?::oid and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
            union all
            select T.oid as object_id, T.proacl as acl
            from pg_catalog.pg_proc T
            where pronamespace = ?::oid and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
            union all
            select T.oid as object_id, T.typacl as acl
            from pg_catalog.pg_type T
            where typnamespace = ?::oid and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid)
            order by object_id
            """)) {
            ps.setInt(1, nsOid);
            ps.setString(2, "1");
            ps.setInt(3, nsOid);
            ps.setString(4, "1");
            ps.setInt(5, nsOid);
            ps.setString(6, "1");
            try (ResultSet rs = ps.executeQuery()) {
                int n = 0; while (rs.next()) n++;
                assertTrue(n >= 0);
            }
        }
    }

    // Table listing with age() and xid parameter
    @Test @Order(5) void table_listing_with_age_param() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            select T.relkind as table_kind, T.relname as table_name, T.oid as table_id, T.xmin as table_state_number
            from pg_catalog.pg_class T
            where relnamespace = ?::oid and relkind in ('r','m','v','f','p')
            and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age(?::varchar::xid), -1), -1), 2147483647)
            order by table_kind, table_id
            """)) {
            ps.setInt(1, nsOid);
            ps.setString(2, "1");
            try (ResultSet rs = ps.executeQuery()) {
                int n = 0; while (rs.next()) n++;
                assertTrue(n >= 1, "Should list at least ext_t table");
            }
        }
    }

    // Empty Parse message (some database clients send these)
    @Test @Order(6) void empty_parse_message() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("")) {
            // Empty SQL should not crash
            try {
                ps.execute();
            } catch (SQLException e) {
                // Expected: empty query might return error, but should not crash the connection
            }
        }
        // Connection should still work after empty parse
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
    }
}
