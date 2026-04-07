package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class DistinctOnDebugTest {
    static Memgres memgres;
    static Connection conn;
    static String tableOid;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE test_tab(id serial PRIMARY KEY, name text)");
            s.execute("CREATE INDEX test_tab_name_idx ON test_tab(name)");
            try (ResultSet rs = s.executeQuery("SELECT 'test_tab'::regclass::oid")) { rs.next(); tableOid = rs.getString(1); }
        }
    }
    @AfterAll static void tearDown() throws Exception {
        if (conn != null) { try (Statement s = conn.createStatement()) { s.execute("DROP TABLE test_tab CASCADE"); } conn.close(); }
        if (memgres != null) memgres.close();
    }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }

    // Without DISTINCT ON: does the base query work?
    @Test void base_query_without_distinct_on() throws SQLException {
        query("""
            SELECT cls.oid, cls.relname as name,
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
            ::OID AND conname is NULL ORDER BY cls.relname
            """);
    }

    // With DISTINCT ON: adds the DISTINCT ON clause
    @Test void with_distinct_on() throws SQLException {
        query("""
            SELECT DISTINCT ON(cls.relname) cls.oid, cls.relname as name,
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
            ::OID AND conname is NULL ORDER BY cls.relname
            """);
    }

    // With correlated subquery in SELECT list (a common DB tool pattern)
    @Test void with_correlated_subquery_in_select() throws SQLException {
        query("""
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
            ::OID AND conname is NULL ORDER BY cls.relname
            """);
    }

    // Minimal: scalar subquery with count() in SELECT list
    @Test void scalar_subquery_with_count_in_select() throws SQLException {
        exec("CREATE TABLE sub_t(id int PRIMARY KEY)");
        exec("INSERT INTO sub_t VALUES (1),(2),(3)");
        try {
            query("SELECT id, (SELECT count(*) FROM sub_t) as total FROM sub_t ORDER BY id");
        } finally {
            exec("DROP TABLE sub_t");
        }
    }

    // Minimal: scalar subquery with CASE + count() in SELECT list
    @Test void scalar_subquery_case_count_in_select() throws SQLException {
        exec("CREATE TABLE sub_t2(id int PRIMARY KEY)");
        exec("INSERT INTO sub_t2 VALUES (1),(2)");
        try {
            query("""
                SELECT id,
                (SELECT (CASE WHEN count(*) > 0 THEN true ELSE false END) FROM sub_t2 WHERE id = sub_t2.id) as has_rows
                FROM sub_t2 ORDER BY id
                """);
        } finally {
            exec("DROP TABLE sub_t2");
        }
    }

    // Simplest version, no subqueries in JOINs
    @Test void simplified_distinct_on() throws SQLException {
        query("""
            SELECT DISTINCT ON(cls.relname) cls.oid, cls.relname as name
            FROM pg_catalog.pg_index idx
                JOIN pg_catalog.pg_class cls ON cls.oid=indexrelid
            WHERE indrelid = """ + tableOid + "::OID ORDER BY cls.relname");
    }
}
