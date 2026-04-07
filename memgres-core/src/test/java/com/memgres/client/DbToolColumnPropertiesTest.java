package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CTE alias resolution bug: "missing FROM-clause entry for table 'inh'"
 *
 * The failing query from db-log-10.txt (lines 328-383) uses a CTE named INH_TABLES
 * and aliases it as INH in the main query via LEFT OUTER JOIN. References to inh.inheritedfrom
 * and inh.inheritedid fail because the CTE alias is not being resolved correctly.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DbToolColumnPropertiesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE test_props (id serial PRIMARY KEY, name text NOT NULL)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ========== Simplest CTE alias cases ==========

    @Test
    @Order(1)
    void simplestCteWithAlias() throws Exception {
        // Most basic CTE-with-alias: WITH cte AS (...) SELECT t.val FROM cte AS t
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH cte AS (SELECT 1 AS val) " +
                     "SELECT t.val FROM cte AS t")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("val"));
        }
    }

    @Test
    @Order(2)
    void cteWithUpperCaseNameAndAlias() throws Exception {
        // Case-sensitivity test: uppercase CTE name, lowercase alias
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH MY_CTE AS (SELECT 1 AS x) " +
                     "SELECT c.x FROM MY_CTE AS c")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("x"));
        }
    }

    // ========== CTE with JOIN alias (the failing pattern) ==========

    @Test
    @Order(3)
    void cteAliasedInLeftOuterJoin() throws Exception {
        // Reproduces the core pattern from the failing query:
        // CTE named INH_TABLES, aliased as INH in a LEFT OUTER JOIN,
        // then referenced as inh.inheritedid in the SELECT list.
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH INH_TABLES AS (" +
                     "    SELECT att.attname AS name, ph.inhparent AS inheritedid" +
                     "    FROM pg_catalog.pg_attribute att" +
                     "    JOIN pg_catalog.pg_inherits ph ON ph.inhparent = att.attrelid" +
                     "    WHERE ph.inhrelid = (SELECT oid FROM pg_class WHERE relname = 'test_props')" +
                     ") " +
                     "SELECT att.attname, inh.inheritedid " +
                     "FROM pg_catalog.pg_attribute att " +
                     "LEFT OUTER JOIN INH_TABLES AS inh ON att.attname = inh.name " +
                     "WHERE att.attrelid = (SELECT oid FROM pg_class WHERE relname = 'test_props') " +
                     "AND att.attnum > 0")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("attname"));
            // inheritedid will be null since test_props has no parent table
            rs.getObject("inheritedid");
        }
    }

    @Test
    @Order(4)
    void cteAliasedInLeftJoinWithMultipleColumns() throws Exception {
        // Tests that multiple columns from the aliased CTE are accessible
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH INH_TABLES AS (" +
                     "    SELECT att.attname AS name, ph.inhparent AS inheritedid," +
                     "           pg_catalog.concat(nmsp_parent.nspname, '.', parent.relname) AS inheritedfrom" +
                     "    FROM pg_catalog.pg_attribute att" +
                     "    JOIN pg_catalog.pg_inherits ph ON ph.inhparent = att.attrelid" +
                     "    JOIN pg_catalog.pg_class parent ON ph.inhparent = parent.oid" +
                     "    JOIN pg_catalog.pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace" +
                     "    WHERE ph.inhrelid = (SELECT oid FROM pg_class WHERE relname = 'test_props')" +
                     ") " +
                     "SELECT att.attname, inh.inheritedfrom, inh.inheritedid " +
                     "FROM pg_catalog.pg_attribute att " +
                     "LEFT OUTER JOIN INH_TABLES AS inh ON att.attname = inh.name " +
                     "WHERE att.attrelid = (SELECT oid FROM pg_class WHERE relname = 'test_props') " +
                     "AND att.attnum > 0")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString("attname"));
            // Both CTE columns should be accessible (null since no inheritance)
            rs.getObject("inheritedfrom");
            rs.getObject("inheritedid");
        }
    }

    // ========== aclexplode function ==========

    @Test
    @Order(10)
    void aclexplodeNullReturnsEmpty() throws Exception {
        // aclexplode(NULL) should return 0 rows (no ACLs)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM (SELECT pg_catalog.aclexplode(att.attacl) AS d " +
                     "FROM pg_catalog.pg_attribute att " +
                     "WHERE att.attrelid = (SELECT oid FROM pg_class WHERE relname = 'test_props') " +
                     "AND att.attnum = 1) sub")) {
            assertFalse(rs.next()); // NULL attacl -> 0 rows
        }
    }

    @Test
    @Order(11)
    void aclexplodeInSubquery() throws Exception {
        // aclexplode in a subquery pattern - should not error
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM (SELECT " +
                     "    pg_catalog.aclexplode(attacl) as d FROM pg_catalog.pg_attribute att " +
                     "    WHERE att.attrelid = (SELECT oid FROM pg_class WHERE relname = 'test_props') " +
                     "    AND att.attnum = 1) a")) {
            // Should return 0 rows (NULL attacl -> empty SRF)
            assertFalse(rs.next());
        }
    }

    // ========== Full original query (simplified OIDs) ==========

    @Test
    @Order(5)
    void fullDbToolColumnPropertiesQuery() throws Exception {
        // Simplified version of the exact query from db-log-10.txt lines 328-383
        // with hardcoded OIDs replaced by subqueries
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH INH_TABLES AS " +
                     "(SELECT " +
                     "  at.attname AS name, ph.inhparent AS inheritedid, ph.inhseqno, " +
                     "  pg_catalog.concat(nmsp_parent.nspname, '.', parent.relname) AS inheritedfrom " +
                     " FROM " +
                     "  pg_catalog.pg_attribute at " +
                     " JOIN " +
                     "  pg_catalog.pg_inherits ph ON ph.inhparent = at.attrelid " +
                     "    AND ph.inhrelid = (SELECT oid FROM pg_class WHERE relname = 'test_props') " +
                     " JOIN " +
                     "  pg_catalog.pg_class parent ON ph.inhparent = parent.oid " +
                     " JOIN " +
                     "  pg_catalog.pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace " +
                     " GROUP BY at.attname, ph.inhparent, ph.inhseqno, inheritedfrom " +
                     " ORDER BY at.attname, ph.inhparent, ph.inhseqno, inheritedfrom " +
                     ") " +
                     "SELECT DISTINCT ON (att.attnum) att.attname as name, att.atttypid, att.attnum, " +
                     "  att.attnotnull, " +
                     "  pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS defval, " +
                     "  pg_catalog.format_type(ty.oid, NULL) AS typname, " +
                     "  pg_catalog.format_type(ty.oid, att.atttypmod) AS cltype, " +
                     "  inh.inheritedfrom, " +
                     "  inh.inheritedid, " +
                     "  description, pi.indkey " +
                     "FROM pg_catalog.pg_attribute att " +
                     "  JOIN pg_catalog.pg_type ty ON ty.oid = atttypid " +
                     "  LEFT OUTER JOIN pg_catalog.pg_attrdef def ON adrelid = att.attrelid AND adnum = att.attnum " +
                     "  LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid = att.attrelid AND des.objsubid = att.attnum AND des.classoid = 'pg_class'::regclass) " +
                     "  LEFT OUTER JOIN pg_catalog.pg_index pi ON pi.indrelid = att.attrelid AND indisprimary " +
                     "  LEFT OUTER JOIN pg_catalog.pg_class tab ON tab.oid = att.attrelid " +
                     "  LEFT OUTER JOIN INH_TABLES AS inh ON att.attname = inh.name " +
                     "WHERE att.attrelid = (SELECT oid FROM pg_class WHERE relname = 'test_props') " +
                     "  AND att.attnum > 0 " +
                     "  AND att.attisdropped IS FALSE " +
                     "ORDER BY att.attnum")) {
            assertTrue(rs.next());
            // First column of test_props is 'id'
            String colName = rs.getString("name");
            assertNotNull(colName);
            // inh columns should be null (no inheritance)
            assertNull(rs.getObject("inheritedfrom"));
            assertNull(rs.getObject("inheritedid"));
        }
    }
}
