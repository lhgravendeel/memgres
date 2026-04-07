package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the next round of database client errors:
 * 1. "nspname" is ambiguous (JOIN pg_namespace multiple times without alias)
 * 2. "Bad value for type int : f" (boolean stored as 'f' being read as int)
 * 3. indcollation column missing on pg_index
 * 4. xmin missing on pg_description
 * 5. UNION column count mismatch
 * 6. attacl missing on pg_attribute
 * 7. pg_indexam_has_property SRF not supported
 * 8. indoption, indnkeyatts missing on pg_index
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientNextRoundTest {

    static Memgres memgres;
    static Connection conn;
    static String nsOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE test_t(id serial PRIMARY KEY, name text)");
            try (ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) { rs.next(); nsOid = rs.getString(1); }
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP TABLE test_t"); } conn.close(); }
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

    // indcollation column on pg_index
    @Test @Order(1) void pg_index_indcollation() throws SQLException {
        query("SELECT indcollation FROM pg_catalog.pg_index LIMIT 0");
    }

    // indnkeyatts column on pg_index
    @Test @Order(2) void pg_index_indnkeyatts() throws SQLException {
        query("SELECT indnkeyatts FROM pg_catalog.pg_index LIMIT 0");
    }

    // attacl column on pg_attribute
    @Test @Order(3) void pg_attribute_attacl() throws SQLException {
        query("SELECT attacl FROM pg_catalog.pg_attribute LIMIT 0");
    }

    // pg_description xmin
    @Test @Order(4) void pg_description_xmin() throws SQLException {
        query("SELECT xmin FROM pg_catalog.pg_description LIMIT 0");
    }

    // pg_indexam_has_property function
    @Test @Order(5) void pg_indexam_has_property() throws SQLException {
        query("SELECT pg_catalog.pg_indexam_has_property(403, 'can_order')");
    }

    // The complex index column details query (causes multiple errors)
    @Test @Order(6) void index_column_details_query() throws SQLException {
        query("""
            select ind_head.indexrelid index_id,
                   k col_idx,
                   k <= indnkeyatts in_key,
                   ind_head.indkey[k-1] column_position,
                   ind_head.indoption[k-1] column_options,
                   ind_head.indcollation[k-1] as collation,
                   ind_head.indclass[k-1] as opclass
            from pg_catalog.pg_index ind_head
                join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid
            cross join unnest(ind_head.indkey) with ordinality u(u, k)
            where ind_stor.relnamespace = """ + nsOid + """
            ::oid and ind_stor.relkind in ('i', 'I')
            order by index_id, k
            """);
    }

    // nspname ambiguous: language query joins pg_namespace multiple times
    @Test @Order(7) void language_with_multiple_namespace_joins() throws SQLException {
        query("""
            select l.oid as id, lanname as name
            from pg_catalog.pg_language l
                left join pg_catalog.pg_proc h on h.oid = lanplcallfoid
                left join pg_catalog.pg_namespace hs on hs.oid = h.pronamespace
                left join pg_catalog.pg_proc i on i.oid = laninline
                left join pg_catalog.pg_namespace isc on isc.oid = i.pronamespace
            order by lanname
            """);
    }

    // ACL attribute query with attacl
    @Test @Order(8) void attribute_acl_query() throws SQLException {
        query("""
            select T.oid as object_id, A.attnum as attr_position, A.attacl as acl
            from pg_catalog.pg_attribute A join pg_catalog.pg_class T on T.oid = A.attrelid
            where relnamespace = """ + nsOid + "::oid and attnum > 0 order by object_id, attr_position");
    }
}
