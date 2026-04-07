package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 3 remaining database client errors:
 * 1. "nspname" is ambiguous (from the extension query or cross-join queries)
 * 2. "Bad value for type int : f" (boolean in wrong context)
 * 3. UNION column count mismatch
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientComplexQueryFixTest {

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

    // Test 1: The extension query with nspname; is nspname ambiguous?
    @Test @Order(1) void extension_query_nspname() throws SQLException {
        query("""
            select E.oid as id, E.xmin as state_number, extname as name, extversion as version,
                   extnamespace as schema_id, nspname as schema_name
            from pg_catalog.pg_extension E
                join pg_namespace N on E.extnamespace = N.oid
                left join (select name, array_agg(version) as available_versions
                           from pg_available_extension_versions() group by name) V on E.extname = V.name
            """);
    }

    // Test 2: The ACL union query; column count match?
    @Test @Order(2) void acl_union_query() throws SQLException {
        query("""
            select T.oid as object_id, T.relacl as acl from pg_catalog.pg_class T where relnamespace = """ + nsOid + """
            ::oid
            union all
            select T.oid as object_id, T.proacl as acl from pg_catalog.pg_proc T where pronamespace = """ + nsOid + """
            ::oid
            union all
            select T.oid as object_id, T.typacl as acl from pg_catalog.pg_type T where typnamespace = """ + nsOid + """
            ::oid
            order by object_id
            """);
    }

    // Test 3: Attribute ACL query (attacl)
    @Test @Order(3) void attribute_acl_query() throws SQLException {
        query("""
            select T.oid as object_id, A.attnum as attr_position, A.attacl as acl
            from pg_catalog.pg_attribute A join pg_catalog.pg_class T on T.oid = A.attrelid
            where relnamespace = """ + nsOid + "::oid and attnum > 0 order by object_id, attr_position");
    }

    // Test 4: The complex index detail query with cross join pg_indexam_has_property
    @Test @Order(4) void index_detail_with_cross_join_function() throws SQLException {
        query("""
            select ind_head.indexrelid index_id,
                   k col_idx,
                   ind_head.indkey[k-1] column_position,
                   ind_head.indoption[k-1] column_options,
                   ind_head.indcollation[k-1] as collation,
                   ind_head.indclass[k-1] as opclass,
                   amcanorder can_order
            from pg_catalog.pg_index ind_head
                join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid
            cross join unnest(ind_head.indkey) with ordinality u(u, k)
            cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
            where ind_stor.relnamespace = """ + nsOid + """
            ::oid and ind_stor.relkind in ('i', 'I')
            order by index_id, k
            """);
    }

    // Test 5: The full index detail query with pg_collation and pg_opclass joins
    @Test @Order(5) void full_index_detail_query() throws SQLException {
        query("""
            select ind_head.indexrelid index_id,
                   k col_idx,
                   ind_head.indkey[k-1] column_position,
                   ind_head.indcollation[k-1] as collation,
                   colln.nspname as collation_schema,
                   collname as collation_str,
                   ind_head.indclass[k-1] as opclass,
                   case when opcdefault then null else opcn.nspname end as opclass_schema,
                   case when opcdefault then null else opcname end as opclass_str,
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
            ::oid and ind_stor.relkind in ('i', 'I')
            order by index_id, k
            """);
    }

    // Test 6: pg_indexam_has_property used as a scalar in CROSS JOIN
    @Test @Order(6) void cross_join_scalar_function() throws SQLException {
        query("""
            select t.id, f as result
            from fix_t t
            cross join pg_catalog.pg_indexam_has_property(403, 'can_order') f
            """);
    }

    // Test 7: Simplified extension query, nspname unqualified
    @Test @Order(7) void extension_query_simple() throws SQLException {
        query("""
            select E.oid as id, extname as name, extnamespace as schema_id, nspname as schema_name
            from pg_catalog.pg_extension E
                join pg_namespace N on E.extnamespace = N.oid
            """);
    }

    // Test 7b: Extension query with nspname QUALIFIED
    @Test @Order(9) void extension_query_qualified() throws SQLException {
        query("""
            select E.oid as id, E.extname as name, E.extnamespace as schema_id, N.nspname as schema_name
            from pg_catalog.pg_extension E
                join pg_namespace N on E.extnamespace = N.oid
            """);
    }

    // Test 8: Extension query with SRF subquery
    @Test @Order(8) void extension_query_with_srf() throws SQLException {
        query("""
            select E.oid as id, extname as name, nspname as schema_name
            from pg_catalog.pg_extension E
                join pg_namespace N on E.extnamespace = N.oid
                left join (select name, array_agg(version) as available_versions
                           from pg_available_extension_versions() group by name) V on E.extname = V.name
            """);
    }
}
