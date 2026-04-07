package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for remaining database client errors:
 * - "Bad value for type int : f/t" (boolean xmin/age issue)
 * - "xmin does not exist" (missing on some catalog table)
 * - "UNION column count mismatch"
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientFinalFixTest {

    static Memgres memgres;
    static Connection conn;
    static String nsOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        // Extended protocol
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE fix_t(id serial PRIMARY KEY, name text)");
            s.execute("INSERT INTO fix_t(name) VALUES ('test')");
            try (ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) { rs.next(); nsOid = rs.getString(1); }
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

    // "Bad value for type int: f": age(xmin) returns int, xmin is stored as int (1),
    // but if xmin is somehow boolean, age() would get 'f'/'t' as input
    @Test @Order(1) void age_of_xmin_on_pg_class() throws SQLException {
        query("SELECT pg_catalog.age(T.xmin) FROM pg_catalog.pg_class T LIMIT 1");
    }

    @Test @Order(2) void age_xmin_comparison() throws SQLException {
        // This is the exact pattern a database client uses for incremental refresh
        query("SELECT T.oid FROM pg_catalog.pg_class T WHERE relnamespace = " + nsOid +
                "::oid AND pg_catalog.age(T.xmin) <= 2147483647");
    }

    @Test @Order(3) void age_xid_cast_chain() throws SQLException {
        // age($2::varchar::xid): the parameter $2 would be a transaction ID string
        // In simple mode, test with literal
        query("SELECT pg_catalog.age('1'::varchar::xid)");
    }

    // xmin on pg_rewrite
    @Test @Order(4) void pg_rewrite_xmin() throws SQLException {
        query("SELECT xmin FROM pg_catalog.pg_rewrite LIMIT 0");
    }

    // xmin on pg_policy
    @Test @Order(5) void pg_policy_xmin() throws SQLException {
        query("SELECT xmin FROM pg_catalog.pg_policy LIMIT 0");
    }

    // xmin on pg_proc
    @Test @Order(6) void pg_proc_xmin() throws SQLException {
        query("SELECT xmin FROM pg_catalog.pg_proc LIMIT 0");
    }

    // xmin on pg_operator
    @Test @Order(7) void pg_operator_xmin() throws SQLException {
        query("SELECT xmin FROM pg_catalog.pg_operator LIMIT 0");
    }

    // xmin on pg_opclass
    @Test @Order(8) void pg_opclass_xmin() throws SQLException {
        query("SELECT xmin FROM pg_catalog.pg_opclass LIMIT 0");
    }

    // xmin on pg_opfamily
    @Test @Order(9) void pg_opfamily_xmin() throws SQLException {
        query("SELECT xmin FROM pg_catalog.pg_opfamily LIMIT 0");
    }

    // xmin on pg_aggregate
    @Test @Order(10) void pg_aggregate_xmin() throws SQLException {
        query("SELECT xmin FROM pg_catalog.pg_aggregate LIMIT 0");
    }

    // The ACL UNION query: all branches must have same column count
    @Test @Order(11) void acl_union_all_branches() throws SQLException {
        query("""
            select T.oid as object_id, T.relacl as acl from pg_catalog.pg_class T WHERE relnamespace = """ + nsOid + """
            ::oid
            union all
            select T.oid as object_id, T.proacl as acl from pg_catalog.pg_proc T WHERE pronamespace = """ + nsOid + """
            ::oid
            union all
            select T.oid as object_id, T.typacl as acl from pg_catalog.pg_type T WHERE typnamespace = """ + nsOid + """
            ::oid
            order by object_id
            """);
    }

    // Column details query with C.xmin (pg_attribute xmin via alias C)
    @Test @Order(12) void column_details_with_xmin() throws SQLException {
        query("""
            select C.attnum as column_position, C.attname as column_name,
                   C.xmin as column_state_number
            from pg_catalog.pg_attribute C
            WHERE C.attrelid = (SELECT oid FROM pg_class WHERE relname = 'fix_t' LIMIT 1)
              AND C.attnum > 0 AND NOT C.attisdropped
            order by C.attnum
            """);
    }
}
