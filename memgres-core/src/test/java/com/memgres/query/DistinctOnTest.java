package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SELECT DISTINCT ON(expr), a PG-specific extension.
 */
class DistinctOnTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }

    @Test void basic_distinct_on() throws SQLException {
        exec("CREATE TABLE t(category text, val int, name text)");
        exec("INSERT INTO t VALUES ('a',1,'x'),('a',2,'y'),('b',3,'z'),('b',4,'w')");
        try {
            List<List<String>> rows = query("SELECT DISTINCT ON(category) category, val, name FROM t ORDER BY category, val");
            assertEquals(2, rows.size(), "DISTINCT ON should return 1 row per category");
            assertEquals("a", rows.get(0).get(0));
            assertEquals("1", rows.get(0).get(1)); // first row for 'a' (ordered by val)
            assertEquals("b", rows.get(1).get(0));
            assertEquals("3", rows.get(1).get(1)); // first row for 'b'
        } finally {
            exec("DROP TABLE t");
        }
    }

    @Test void distinct_on_with_case_expression() throws SQLException {
        exec("CREATE TABLE t2(id int, kind text, note text)");
        exec("INSERT INTO t2 VALUES (1,'a','n1'),(2,'a','n2'),(3,'b','n3')");
        try {
            // This mimics a common DB tool pattern: DISTINCT ON + CASE in select list
            List<List<String>> rows = query("""
                SELECT DISTINCT ON(t2.kind) t2.id, t2.kind,
                    CASE WHEN t2.kind = 'a' THEN 'alpha' ELSE 'other' END AS label
                FROM t2
                ORDER BY t2.kind, t2.id
                """);
            assertEquals(2, rows.size());
        } finally {
            exec("DROP TABLE t2");
        }
    }

    @Test void distinct_on_with_join_and_case() throws SQLException {
        exec("CREATE TABLE idx_tab(oid int, relname text)");
        exec("CREATE TABLE con_tab(oid int, contype text)");
        exec("INSERT INTO idx_tab VALUES (1,'idx_a'),(2,'idx_b'),(3,'idx_c')");
        exec("INSERT INTO con_tab VALUES (1,'p'),(2,NULL)");
        try {
            List<List<String>> rows = query("""
                SELECT DISTINCT ON(i.relname) i.oid, i.relname,
                    CASE WHEN c.contype IN ('p','u') THEN 'constraint' ELSE 'plain' END AS kind
                FROM idx_tab i
                LEFT JOIN con_tab c ON c.oid = i.oid
                ORDER BY i.relname
                """);
            assertEquals(3, rows.size());
        } finally {
            exec("DROP TABLE con_tab"); exec("DROP TABLE idx_tab");
        }
    }

    // Common DB tool pattern: complex multi-join with DISTINCT ON
    @Test void dbtool_index_query_pattern() throws SQLException {
        exec("CREATE TABLE test_tab(id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX test_tab_name_idx ON test_tab(name)");
        try {
            String tableOid;
            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT 'test_tab'::regclass::oid")) {
                rs.next(); tableOid = rs.getString(1);
            }
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
            // Should list test_tab_name_idx (not PK since that has a constraint)
            assertFalse(rows.isEmpty(), "Should return non-constraint indexes");
        } finally {
            exec("DROP TABLE test_tab CASCADE");
        }
    }
}
