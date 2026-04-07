package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests targeting the exact remaining database client errors.
 * Uses EXTENDED protocol (default JDBC mode).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientRemainingErrorsTest {

    static Memgres memgres;
    static Connection conn;
    static String nsOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE fix_t(id serial PRIMARY KEY, name text)");
            s.execute("INSERT INTO fix_t(name) VALUES ('test')");
            try (ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
                rs.next(); nsOid = rs.getString(1);
            }
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP TABLE fix_t"); } conn.close(); }
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

    // Error: "No results were returned" from SHOW via extended protocol
    @Test @Order(1) void show_transaction_isolation_level() throws SQLException {
        // This must return a result set, not a command completion
        List<List<String>> rows = query("SHOW TRANSACTION ISOLATION LEVEL");
        assertEquals(1, rows.size(), "SHOW should return 1 row");
        assertNotNull(rows.get(0).get(0));
    }

    // Error: "Bad value for type int : f/t" from pg_indexam_has_property boolean in cross join
    // The column type must be reported as BOOLEAN (OID 16), not INTEGER
    @Test @Order(2) void cross_join_boolean_type() throws SQLException {
        List<List<String>> rows = query("""
            SELECT amcanorder can_order
            FROM pg_catalog.pg_class ind_stor
            CROSS JOIN pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
            WHERE ind_stor.relkind IN ('i','I') AND ind_stor.relnamespace = """ + nsOid + "::oid LIMIT 1");
        if (!rows.isEmpty()) {
            // The value should be 't' or 'f', NOT throw "bad value for type int"
            String val = rows.get(0).get(0);
            assertTrue("t".equals(val) || "f".equals(val) || "true".equals(val) || "false".equals(val),
                    "Should be boolean, got: " + val);
        }
    }

    // Error: "UNION column count mismatch" from translate() function
    @Test @Order(3) void translate_function() throws SQLException {
        String val = query("SELECT pg_catalog.translate('rmvpfS', 'rmvpfS', 'rmvrfS')").get(0).get(0);
        assertEquals("rmvrfS", val);
    }

    // The actual 7-branch UNION query
    @Test @Order(4) void seven_branch_union() throws SQLException {
        query("""
            select T.oid as oid, relnamespace as schemaId,
                   pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind
            from pg_catalog.pg_class T
            where relnamespace in ( null ) and relkind in ('r','m','v','p','f','S')
            union all
            select T.oid, T.typnamespace, 'T' as kind
            from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid
            where T.typnamespace in ( null )
              and (T.typtype in ('d','e') or C.relkind = 'c' or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or T.typtype = 'p' and not T.typisdefined)
            union all
            select oid, collnamespace, 'C' from pg_catalog.pg_collation where collnamespace in ( null )
            union all
            select oid, oprnamespace, 'O' from pg_catalog.pg_operator where oprnamespace in ( null )
            union all
            select oid, opcnamespace, 'c' from pg_catalog.pg_opclass where opcnamespace in ( null )
            union all
            select oid, opfnamespace, 'F' from pg_catalog.pg_opfamily where opfnamespace in ( null )
            union all
            select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end
            from pg_catalog.pg_proc where pronamespace in ( null )
            """);
    }

    // pg_catalog.upper() function
    @Test @Order(5) void pg_catalog_upper() throws SQLException {
        String val = query("SELECT pg_catalog.upper('hello')").get(0).get(0);
        assertEquals("HELLO", val);
    }

    // The column detail query that uses age(xmin) with parameters
    @Test @Order(6) void age_xmin_with_xid_parameter() throws SQLException {
        // In database clients, this is sent with bound parameters. Test with literals.
        query("""
            SELECT T.oid, pg_catalog.age(T.xmin) as age_val
            FROM pg_catalog.pg_class T
            WHERE relnamespace = """ + nsOid + """
            ::oid AND pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age('1'::varchar::xid), -1), -1), 2147483647)
            LIMIT 1
            """);
    }

    // The ACL union with 3 branches
    @Test @Order(7) void acl_union_with_age() throws SQLException {
        query("""
            select T.oid as object_id, T.relacl as acl
            from pg_catalog.pg_class T
            where relnamespace = """ + nsOid + """
            ::oid and pg_catalog.age(T.xmin) <= pg_catalog.age('1'::varchar::xid)
            union all
            select T.oid as object_id, T.proacl as acl
            from pg_catalog.pg_proc T
            where pronamespace = """ + nsOid + """
            ::oid and pg_catalog.age(T.xmin) <= pg_catalog.age('1'::varchar::xid)
            union all
            select T.oid as object_id, T.typacl as acl
            from pg_catalog.pg_type T
            where typnamespace = """ + nsOid + """
            ::oid and pg_catalog.age(T.xmin) <= pg_catalog.age('1'::varchar::xid)
            order by object_id
            """);
    }
}
