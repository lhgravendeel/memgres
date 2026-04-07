package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests a DB tool's index introspection query which JOINs pg_index, pg_class,
 * pg_am, pg_depend, pg_constraint, pg_description, pg_inherits.
 *
 * Key columns/tables needed:
 * - pg_class.relam (access method OID)
 * - pg_class.reltablespace
 * - pg_class.tableoid
 * - pg_am (access method catalog)
 * - pg_depend with classid/objid/refobjid/refobjsubid/refclassid/deptype
 * - pg_inherits with inhrelid
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DbToolIndexIntrospectionTest {

    static Memgres memgres;
    static Connection conn;
    static String tableOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_test(id serial PRIMARY KEY, name text NOT NULL, val int)");
            s.execute("CREATE INDEX idx_test_name ON idx_test(name)");
            s.execute("CREATE UNIQUE INDEX idx_test_val ON idx_test(val)");
            try (ResultSet rs = s.executeQuery("SELECT 'idx_test'::regclass::oid")) {
                rs.next(); tableOid = rs.getString(1);
            }
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP TABLE idx_test"); } conn.close(); }
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

    // Common DB tool index listing query
    @Test @Order(1) void dbtool_index_listing_query() throws SQLException {
        List<List<String>> rows = query("""
            SELECT DISTINCT ON(cls.relname) cls.oid, cls.relname as name,
            (SELECT (CASE WHEN count(i.inhrelid) > 0 THEN true ELSE false END) FROM pg_inherits i WHERE i.inhrelid = cls.oid) as is_inherited,
            CASE WHEN contype IN ('p', 'u', 'x') THEN desp.description ELSE des.description END AS description
            FROM pg_catalog.pg_index idx
                JOIN pg_catalog.pg_class cls ON cls.oid=indexrelid
                JOIN pg_catalog.pg_class tab ON tab.oid=indrelid
                LEFT OUTER JOIN pg_catalog.pg_tablespace ta on ta.oid=cls.reltablespace
                JOIN pg_catalog.pg_namespace n ON n.oid=tab.relnamespace
                JOIN pg_catalog.pg_am am ON am.oid=cls.relam
                LEFT JOIN pg_catalog.pg_depend dep ON (dep.classid = cls.tableoid AND dep.objid = cls.oid AND dep.refobjsubid = '0' AND dep.refclassid=(SELECT oid FROM pg_catalog.pg_class WHERE relname='pg_constraint') AND dep.deptype='i')
                LEFT OUTER JOIN pg_catalog.pg_constraint con ON (con.tableoid = dep.refclassid AND con.oid = dep.refobjid)
                LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=cls.oid AND des.classoid='pg_class'::regclass)
                LEFT OUTER JOIN pg_catalog.pg_description desp ON (desp.objoid=con.oid AND desp.objsubid = 0 AND desp.classoid='pg_constraint'::regclass)
            WHERE indrelid = """ + tableOid + """
            ::OID
                AND conname is NULL
                ORDER BY cls.relname
            """);
        // Should return the non-constraint indexes: idx_test_name, idx_test_val
        // (PK index is linked to a constraint, so it's filtered by conname IS NULL)
        assertFalse(rows.isEmpty(), "Should return at least 1 non-constraint index");
    }

    // Prerequisite: pg_class must have relam column
    @Test @Order(2) void pg_class_has_relam() throws SQLException {
        query("SELECT relam FROM pg_catalog.pg_class LIMIT 1");
    }

    // Prerequisite: pg_class must have reltablespace column
    @Test @Order(3) void pg_class_has_reltablespace() throws SQLException {
        query("SELECT reltablespace FROM pg_catalog.pg_class LIMIT 1");
    }

    // Prerequisite: pg_class must have tableoid column
    @Test @Order(4) void pg_class_has_tableoid() throws SQLException {
        query("SELECT tableoid FROM pg_catalog.pg_class LIMIT 1");
    }

    // Prerequisite: pg_inherits must exist with inhrelid
    @Test @Order(5) void pg_inherits_accessible() throws SQLException {
        query("SELECT inhrelid FROM pg_catalog.pg_inherits LIMIT 0");
    }

    // Prerequisite: pg_am must have oid column and be joinable
    @Test @Order(6) void pg_am_joinable() throws SQLException {
        List<List<String>> rows = query("SELECT oid, amname FROM pg_catalog.pg_am");
        assertTrue(rows.size() >= 1, "pg_am should have at least btree");
    }

    // Prerequisite: pg_depend must have deptype column
    @Test @Order(7) void pg_depend_has_deptype() throws SQLException {
        query("SELECT classid, objid, refobjid, refobjsubid, refclassid, deptype FROM pg_catalog.pg_depend LIMIT 0");
    }

    // Prerequisite: pg_constraint must have tableoid
    @Test @Order(8) void pg_constraint_has_tableoid() throws SQLException {
        query("SELECT tableoid FROM pg_catalog.pg_constraint LIMIT 0");
    }
}
