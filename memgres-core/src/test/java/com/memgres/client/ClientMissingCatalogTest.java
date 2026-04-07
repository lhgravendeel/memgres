package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for specific errors reported by database clients:
 * 1. pg_available_extension_versions() SRF not supported in FROM
 * 2. nspacl column missing on pg_namespace
 * 3. array_agg(x ORDER BY y), ORDER BY inside aggregate function
 * 4. xmin on pg_attribute
 * 5. relacl on pg_class
 * 6. UNION column count mismatch (related to missing columns)
 * 7. date/time field value out of range: "1" (age() with xid type)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ClientMissingCatalogTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE test_t(id serial PRIMARY KEY, name text)");
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

    // Error: "Set-returning function not supported in FROM: pg_available_extension_versions"
    @Test @Order(1) void pg_available_extension_versions_srf() throws SQLException {
        query("SELECT name, version FROM pg_available_extension_versions() LIMIT 0");
    }

    // Error: "column nspacl does not exist"
    @Test @Order(2) void pg_namespace_nspacl() throws SQLException {
        query("SELECT nspacl FROM pg_catalog.pg_namespace LIMIT 1");
    }

    // Error: "Expected RIGHT_PAREN but found KEYWORD at position ... near 'ORDER'"
    // This is array_agg(x ORDER BY y): aggregate with ORDER BY inside
    @Test @Order(3) void array_agg_with_order_by() throws SQLException {
        query("""
            SELECT pg_catalog.array_agg(inhparent::bigint ORDER BY inhseqno)::varchar
            FROM pg_catalog.pg_inherits WHERE inhrelid = 0
            """);
    }

    // Error: "column xmin does not exist" on pg_attribute
    @Test @Order(4) void pg_attribute_xmin() throws SQLException {
        query("SELECT xmin FROM pg_catalog.pg_attribute LIMIT 1");
    }

    // Error: "column a.xmin does not exist", xmin on pg_attribute via alias
    @Test @Order(5) void pg_attribute_xmin_aliased() throws SQLException {
        query("SELECT A.xmin FROM pg_catalog.pg_attribute A LIMIT 1");
    }

    // Error: "column relacl does not exist"
    @Test @Order(6) void pg_class_relacl() throws SQLException {
        query("SELECT relacl FROM pg_catalog.pg_class LIMIT 1");
    }

    // Error: "column typacl does not exist"
    @Test @Order(7) void pg_type_typacl() throws SQLException {
        query("SELECT typacl FROM pg_catalog.pg_type LIMIT 1");
    }

    // Error: "date/time field value out of range: 1", age() with xid cast
    @Test @Order(8) void age_with_xid_cast() throws SQLException {
        query("SELECT pg_catalog.age(1::varchar::xid)");
    }

    // pg_attribute needs attislocal, attfdwoptions, attidentity, attgenerated
    @Test @Order(9) void pg_attribute_extra_columns() throws SQLException {
        query("SELECT attislocal, attfdwoptions, attidentity, attgenerated FROM pg_catalog.pg_attribute LIMIT 1");
    }

    // pg_collation needs xmin and collcollate, collctype
    @Test @Order(10) void pg_collation_xmin_and_collate() throws SQLException {
        query("SELECT xmin, collcollate, collctype FROM pg_catalog.pg_collation LIMIT 1");
    }

    // pg_index needs indnullsnotdistinct, indclass
    @Test @Order(11) void pg_index_extra_columns() throws SQLException {
        query("SELECT indnullsnotdistinct, indclass FROM pg_catalog.pg_index LIMIT 0");
    }

    // pg_trigger needs tgargs, tgtype, tgdeferrable, tginitdeferred, tgattr, tgconstraint, tgoldtable, tgnewtable
    @Test @Order(12) void pg_trigger_extra_columns() throws SQLException {
        query("SELECT tgargs, tgtype, tgdeferrable, tginitdeferred, tgattr, tgconstraint, tgoldtable, tgnewtable FROM pg_catalog.pg_trigger LIMIT 0");
    }

    // pg_get_triggerdef function
    @Test @Order(13) void pg_get_triggerdef() throws SQLException {
        // Should not crash even with invalid OID
        query("SELECT pg_catalog.pg_get_triggerdef(0, true)");
    }

    // pg_get_ruledef function
    @Test @Order(14) void pg_get_ruledef() throws SQLException {
        query("SELECT pg_catalog.pg_get_ruledef(0, true)");
    }

    // pg_get_function_sqlbody function
    @Test @Order(15) void pg_get_function_sqlbody() throws SQLException {
        query("SELECT pg_catalog.pg_get_function_sqlbody(0)");
    }
}
