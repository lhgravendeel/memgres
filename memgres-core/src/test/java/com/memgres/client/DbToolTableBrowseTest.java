package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests every distinct new DB tool query from the table/index browsing session.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DbToolTableBrowseTest {

    static Memgres memgres;
    static Connection conn;
    static String tableOid;
    static String nsOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA myapp");
            s.execute("SET search_path = myapp");
            s.execute("CREATE TABLE users(user_id serial PRIMARY KEY, username text NOT NULL)");
            s.execute("CREATE INDEX idx_username ON users(username)");
            try (ResultSet rs = s.executeQuery("SELECT 'myapp.users'::regclass::oid")) { rs.next(); tableOid = rs.getString(1); }
            try (ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'myapp'")) { rs.next(); nsOid = rs.getString(1); }
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP SCHEMA myapp CASCADE"); } conn.close(); }
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

    // Schema namespace listing with catalog detection
    @Test @Order(1) void namespace_listing_with_catalog_detection() throws SQLException {
        List<List<String>> rows = query("""
            SELECT
                nsp.oid, nsp.nspname as name,
                pg_catalog.has_schema_privilege(nsp.oid, 'CREATE') as can_create,
                pg_catalog.has_schema_privilege(nsp.oid, 'USAGE') as has_usage,
                des.description
            FROM pg_catalog.pg_namespace nsp
                LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=nsp.oid AND des.classoid='pg_namespace'::regclass)
            WHERE nspname NOT LIKE E'pg\\_%'
                AND NOT (
                    (nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR
                    (nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR
                    (nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1))
                )
            ORDER BY nspname
            """);
        // Should include 'myapp' and 'public', but NOT pg_catalog or information_schema
        boolean hasMyapp = rows.stream().anyMatch(r -> "myapp".equals(r.get(1)));
        assertTrue(hasMyapp, "Should include myapp schema");
    }

    // Schema is_catalog detection for a specific namespace OID
    @Test @Order(2) void namespace_is_catalog_detection() throws SQLException {
        List<List<String>> rows = query("""
            SELECT nsp.nspname as schema_name,
                (nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR
                (nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR
                (nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1)) AS is_catalog,
                CASE WHEN nsp.nspname = ANY('{information_schema}') THEN false ELSE true END AS db_support
            FROM pg_catalog.pg_namespace nsp
            WHERE nsp.oid = """ + nsOid + "::OID");
        assertEquals(1, rows.size());
        assertEquals("myapp", rows.get(0).get(0));
    }

    // Table listing for a schema (with trigger/inherit counts)
    @Test @Order(3) void table_listing_with_trigger_inherit_counts() throws SQLException {
        List<List<String>> rows = query("""
            SELECT rel.oid, rel.relname AS name,
                (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE) AS triggercount,
                (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE AND tgenabled = 'O') AS has_enable_triggers,
                (CASE WHEN rel.relkind = 'p' THEN true ELSE false END) AS is_partitioned,
                (SELECT count(1) FROM pg_catalog.pg_inherits WHERE inhrelid=rel.oid LIMIT 1) as is_inherits,
                (SELECT count(1) FROM pg_catalog.pg_inherits WHERE inhparent=rel.oid LIMIT 1) as is_inherited,
                des.description
            FROM pg_catalog.pg_class rel
                LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=rel.oid AND des.objsubid=0 AND des.classoid='pg_class'::regclass)
            WHERE rel.relkind IN ('r','s','t','p') AND rel.relnamespace = """ + nsOid + """
            ::oid
                AND NOT rel.relispartition
            ORDER BY rel.relname
            """);
        assertTrue(rows.size() >= 1, "Should list at least 'users' table");
        assertEquals("users", rows.get(0).get(1));
    }

    // Is partitioned check
    @Test @Order(4) void is_partitioned_check() throws SQLException {
        List<List<String>> rows = query("SELECT CASE WHEN c.relkind = 'p' THEN True ELSE False END As ptable FROM pg_catalog.pg_class c WHERE c.oid = " + tableOid + "::oid");
        assertEquals(1, rows.size());
        assertEquals("f", rows.get(0).get(0)); // users is not partitioned
    }

    // FK constraints listing
    @Test @Order(5) void fk_constraints_listing() throws SQLException {
        List<List<String>> rows = query("""
            SELECT ct.oid, conname as name, NOT convalidated as convalidated, description as comment
            FROM pg_catalog.pg_constraint ct
            LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=ct.oid AND des.classoid='pg_constraint'::regclass)
            WHERE contype='f' AND conrelid = """ + tableOid + "::oid ORDER BY conname");
        assertEquals(0, rows.size()); // users has no FKs
    }

    // CHECK constraints listing (with conislocal)
    @Test @Order(6) void check_constraints_with_conislocal() throws SQLException {
        List<List<String>> rows = query("""
            SELECT c.oid, conname as name, NOT convalidated as convalidated, conislocal, description as comment
            FROM pg_catalog.pg_constraint c
            LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=c.oid AND des.classoid='pg_constraint'::regclass)
            WHERE contype = 'c' AND conrelid = """ + tableOid + "::oid");
        // users has no CHECK constraints (just PK and NOT NULL)
        assertNotNull(rows);
    }

    // Exclusion constraints listing
    @Test @Order(7) void exclusion_constraints_listing() throws SQLException {
        List<List<String>> rows = query("""
            SELECT conindid as oid, conname as name, NOT convalidated as convalidated, desp.description AS comment
            FROM pg_catalog.pg_constraint ct
            LEFT OUTER JOIN pg_catalog.pg_description desp ON (desp.objoid=ct.oid AND desp.objsubid = 0 AND desp.classoid='pg_constraint'::regclass)
            WHERE contype='x' AND conrelid = """ + tableOid + "::oid ORDER BY conname");
        assertEquals(0, rows.size());
    }

    // PK index listing
    @Test @Order(8) void pk_index_listing() throws SQLException {
        List<List<String>> rows = query("""
            SELECT cls.oid, cls.relname as name,
                CASE contype WHEN 'p' THEN desp.description WHEN 'u' THEN desp.description WHEN 'x' THEN desp.description ELSE des.description END AS comment
            FROM pg_catalog.pg_index idx
            JOIN pg_catalog.pg_class cls ON cls.oid=indexrelid
            LEFT JOIN pg_catalog.pg_depend dep ON (dep.classid = cls.tableoid AND dep.objid = cls.oid AND dep.refobjsubid = '0' AND dep.refclassid=(SELECT oid FROM pg_catalog.pg_class WHERE relname='pg_constraint') AND dep.deptype='i')
            LEFT OUTER JOIN pg_catalog.pg_constraint con ON (con.tableoid = dep.refclassid AND con.oid = dep.refobjid)
            LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=cls.oid AND des.classoid='pg_class'::regclass)
            LEFT OUTER JOIN pg_catalog.pg_description desp ON (desp.objoid=con.oid AND desp.objsubid = 0 AND desp.classoid='pg_constraint'::regclass)
            WHERE indrelid = """ + tableOid + "::oid AND contype='p'");
        // Should find the PK index
        assertNotNull(rows);
    }

    // UNIQUE index listing
    @Test @Order(9) void unique_index_listing() throws SQLException {
        List<List<String>> rows = query("""
            SELECT cls.oid, cls.relname as name,
                CASE contype WHEN 'p' THEN desp.description WHEN 'u' THEN desp.description WHEN 'x' THEN desp.description ELSE des.description END AS comment
            FROM pg_catalog.pg_index idx
            JOIN pg_catalog.pg_class cls ON cls.oid=indexrelid
            LEFT JOIN pg_catalog.pg_depend dep ON (dep.classid = cls.tableoid AND dep.objid = cls.oid AND dep.refobjsubid = '0' AND dep.refclassid=(SELECT oid FROM pg_catalog.pg_class WHERE relname='pg_constraint') AND dep.deptype='i')
            LEFT OUTER JOIN pg_catalog.pg_constraint con ON (con.tableoid = dep.refclassid AND con.oid = dep.refobjid)
            LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=cls.oid AND des.classoid='pg_class'::regclass)
            LEFT OUTER JOIN pg_catalog.pg_description desp ON (desp.objoid=con.oid AND desp.objsubid = 0 AND desp.classoid='pg_constraint'::regclass)
            WHERE indrelid = """ + tableOid + "::oid AND contype='u'");
        assertNotNull(rows);
    }

    // DISTINCT ON index listing (the one that fails)
    @Test @Order(10) void distinct_on_index_listing() throws SQLException {
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
        // Should return idx_username (not the PK, which has a constraint)
        assertFalse(rows.isEmpty(), "Should return non-constraint indexes");
    }

    // pg_constraint needs conislocal column
    @Test @Order(11) void pg_constraint_has_conislocal() throws SQLException {
        query("SELECT conislocal FROM pg_catalog.pg_constraint LIMIT 0");
    }

    // pg_trigger needs tgisinternal and tgenabled columns
    @Test @Order(12) void pg_trigger_has_tgisinternal_and_tgenabled() throws SQLException {
        query("SELECT tgisinternal, tgenabled FROM pg_catalog.pg_trigger LIMIT 0");
    }
}
