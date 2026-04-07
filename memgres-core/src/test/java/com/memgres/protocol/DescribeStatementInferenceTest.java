package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that simulate what describeStatement does internally:
 * replace $N with NULL, add LIMIT 0, execute to get column metadata.
 * Uses simple query mode to execute the same SQL that describeStatement would try.
 */
class DescribeStatementInferenceTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE inf_t(id serial PRIMARY KEY, name text)");
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP TABLE inf_t"); } conn.close(); }
        if (memgres != null) memgres.close();
    }

    /** Simulates replaceParamsWithNull */
    static String replaceParams(String sql) {
        return sql.replaceAll("\\$\\d+", "NULL");
    }

    /** Execute the NULL-substituted + LIMIT 0 query and check columns */
    void assertInferenceWorks(String parameterizedSql) throws SQLException {
        String metaSql = replaceParams(parameterizedSql).replaceAll(";\\s*$", "").trim();
        if (!metaSql.toUpperCase().contains("LIMIT")) {
            metaSql = metaSql + " LIMIT 0";
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(metaSql)) {
            ResultSetMetaData md = rs.getMetaData();
            assertTrue(md.getColumnCount() > 0, "Column inference should return > 0 columns for: " + metaSql.substring(0, Math.min(80, metaSql.length())));
        }
    }

    @Test void infer_simple_select() throws SQLException {
        assertInferenceWorks("SELECT * FROM inf_t WHERE id = $1");
    }

    @Test void infer_7branch_union() throws SQLException {
        assertInferenceWorks("""
            select T.oid as oid, relnamespace as schemaId,
                   pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind, relname as name
            from pg_catalog.pg_class T
            where relnamespace in ( $1 ) and relkind in ('r','m','v','p','f','S')
            union all
            select T.oid, T.typnamespace, 'T', T.typname
            from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid
            where T.typnamespace in ( $2 )
              and (T.typtype in ('d','e') or C.relkind = 'c'::"char" or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or T.typtype = 'p' and not T.typisdefined)
            union all
            select oid, collnamespace, 'C', collname from pg_catalog.pg_collation where collnamespace in ( $3 )
            union all
            select oid, oprnamespace, 'O', oprname from pg_catalog.pg_operator where oprnamespace in ( $4 )
            union all
            select oid, opcnamespace, 'c', opcname from pg_catalog.pg_opclass where opcnamespace in ( $5 )
            union all
            select oid, opfnamespace, 'F', opfname from pg_catalog.pg_opfamily where opfnamespace in ( $6 )
            union all
            select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end, proname
            from pg_catalog.pg_proc where pronamespace in ( $7 )
            order by schemaId
            """);
    }

    @Test void infer_cross_join_function() throws SQLException {
        assertInferenceWorks("""
            select ind_head.indexrelid index_id, amcanorder can_order
            from pg_catalog.pg_index ind_head
                join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid
            cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder
            where ind_stor.relnamespace = $1::oid and ind_stor.relkind in ('i','I')
            """);
    }

    @Test void infer_acl_union() throws SQLException {
        assertInferenceWorks("""
            select T.oid as object_id, T.relacl as acl from pg_catalog.pg_class T
            where relnamespace = $1::oid and pg_catalog.age(T.xmin) <= pg_catalog.age($2::varchar::xid)
            union all
            select T.oid as object_id, T.proacl as acl from pg_catalog.pg_proc T
            where pronamespace = $3::oid and pg_catalog.age(T.xmin) <= pg_catalog.age($4::varchar::xid)
            union all
            select T.oid as object_id, T.typacl as acl from pg_catalog.pg_type T
            where typnamespace = $5::oid and pg_catalog.age(T.xmin) <= pg_catalog.age($6::varchar::xid)
            order by object_id
            """);
    }

    @Test void infer_table_listing_with_age() throws SQLException {
        assertInferenceWorks("""
            select T.relkind as table_kind, T.relname as table_name, T.oid as table_id, T.xmin as table_state_number
            from pg_catalog.pg_class T
            where relnamespace = $1::oid and relkind in ('r','m','v','f','p')
            and pg_catalog.age(T.xmin) <= coalesce(nullif(greatest(pg_catalog.age($2::varchar::xid), -1), -1), 2147483647)
            order by table_kind, table_id
            """);
    }

    // Test the exact NULL substitution: $1::oid becomes NULL::oid
    @Test void null_oid_cast() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT NULL::oid")) {
            assertTrue(rs.next());
        }
    }

    // Test age(NULL::varchar::xid)
    @Test void age_null_xid() throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT pg_catalog.age(NULL::varchar::xid)")) {
            assertTrue(rs.next());
        }
    }
}
