package com.memgres.pg18;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that column-detail query results match PG 18 behavior.
 * Covers the fixes from issues-10.md:
 *   Issue 1: indkey must be int2vector format ("1") not array format ("{1}")
 *   Issue 2: attlen must match pg_type.typlen (e.g. 4 for integer, not -1)
 *   Issue 3: pg_seclabels must have correct columns (objoid, classoid, objsubid, provider, label)
 */
class Pg18ColumnDetailVerifyTest {

    static Memgres memgres;
    static Connection conn;
    static int tableOid;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE verify_col (id serial PRIMARY KEY, name text NOT NULL, flag boolean, val bigint, price numeric(10,2))");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_class WHERE relname = 'verify_col'")) {
            assertTrue(rs.next());
            tableOid = rs.getInt(1);
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ========== Issue 1: indkey format ==========

    @Test
    void indkey_singleColumn_intVectorFormat() throws Exception {
        // PG 18: indkey for single-column PK is "1" (int2vector, space-separated)
        // NOT "{1}" (array format)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pi.indkey FROM pg_catalog.pg_index pi " +
                     "WHERE pi.indrelid = " + tableOid + " AND pi.indisprimary")) {
            assertTrue(rs.next());
            String indkey = rs.getString("indkey");
            assertEquals("1", indkey, "indkey should be int2vector format '1', not array format '{1}'");
            assertFalse(indkey.contains("{"), "indkey must not contain curly braces");
        }
    }

    @Test
    void indkey_multiColumn_intVectorFormat() throws Exception {
        // Create a table with a multi-column primary key
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE multi_pk (a int, b int, c text, PRIMARY KEY (a, b))");
        }
        int multiOid;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_class WHERE relname = 'multi_pk'")) {
            assertTrue(rs.next());
            multiOid = rs.getInt(1);
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT pi.indkey FROM pg_catalog.pg_index pi " +
                     "WHERE pi.indrelid = " + multiOid + " AND pi.indisprimary")) {
            assertTrue(rs.next());
            String indkey = rs.getString("indkey");
            assertEquals("1 2", indkey, "indkey for (a,b) PK should be '1 2' (int2vector)");
            assertFalse(indkey.contains("{"), "indkey must not contain curly braces");
        }
    }

    @Test
    void indkey_inColumnDetailCteQuery() throws Exception {
        // The pattern used in the column detail CTE: pi.indkey
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT att.attname as name, pi.indkey " +
                     "FROM pg_catalog.pg_attribute att " +
                     "LEFT OUTER JOIN pg_catalog.pg_index pi ON pi.indrelid=att.attrelid AND indisprimary " +
                     "WHERE att.attrelid = " + tableOid + " AND att.attnum = 1 AND att.attnum > 0")) {
            assertTrue(rs.next());
            assertEquals("id", rs.getString("name"));
            String indkey = rs.getString("indkey");
            assertEquals("1", indkey);
        }
    }

    // ========== Issue 2: attlen must match typlen ==========

    @Test
    void attlen_integer() throws Exception {
        // PG 18: integer columns have attlen = 4
        assertAttlen("id", (short) 4);
    }

    @Test
    void attlen_text() throws Exception {
        // PG 18: text columns have attlen = -1 (variable)
        assertAttlen("name", (short) -1);
    }

    @Test
    void attlen_boolean() throws Exception {
        // PG 18: boolean columns have attlen = 1
        assertAttlen("flag", (short) 1);
    }

    @Test
    void attlen_bigint() throws Exception {
        // PG 18: bigint columns have attlen = 8
        assertAttlen("val", (short) 8);
    }

    @Test
    void attlen_numeric() throws Exception {
        // PG 18: numeric columns have attlen = -1 (variable)
        assertAttlen("price", (short) -1);
    }

    private void assertAttlen(String colName, short expectedLen) throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT att.attlen FROM pg_catalog.pg_attribute att " +
                     "WHERE att.attrelid = " + tableOid + " AND att.attname = '" + colName + "'")) {
            assertTrue(rs.next(), "Column " + colName + " should exist in pg_attribute");
            assertEquals(expectedLen, rs.getShort("attlen"),
                    "attlen for " + colName + " should match pg_type.typlen");
        }
    }

    // ========== Issue 3: pg_seclabels columns ==========

    @Test
    void pgSeclabels_hasCorrectColumns() throws Exception {
        // PG 18: pg_seclabels has columns objoid, classoid, objsubid, provider, label (and more)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM pg_catalog.pg_seclabels LIMIT 0")) {
            ResultSetMetaData md = rs.getMetaData();
            // Verify essential columns exist
            boolean hasObjoid = false, hasClassoid = false, hasObjsubid = false;
            boolean hasProvider = false, hasLabel = false;
            for (int i = 1; i <= md.getColumnCount(); i++) {
                switch (md.getColumnName(i)) {
                    case "objoid" -> hasObjoid = true;
                    case "classoid" -> hasClassoid = true;
                    case "objsubid" -> hasObjsubid = true;
                    case "provider" -> hasProvider = true;
                    case "label" -> hasLabel = true;
                }
            }
            assertTrue(hasObjoid, "pg_seclabels must have objoid column");
            assertTrue(hasClassoid, "pg_seclabels must have classoid column");
            assertTrue(hasObjsubid, "pg_seclabels must have objsubid column");
            assertTrue(hasProvider, "pg_seclabels must have provider column");
            assertTrue(hasLabel, "pg_seclabels must have label column");
        }
    }

    @Test
    void pgSeclabels_subqueryInColumnDetailQuery() throws Exception {
        // The subquery pattern used in the column detail CTE
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT (SELECT pg_catalog.array_agg(provider || '=' || label) " +
                     "FROM pg_catalog.pg_seclabels sl1 " +
                     "WHERE sl1.objoid = att.attrelid AND sl1.objsubid = att.attnum) AS seclabels " +
                     "FROM pg_catalog.pg_attribute att " +
                     "WHERE att.attrelid = " + tableOid + " AND att.attnum = 1")) {
            assertTrue(rs.next());
            // Should be null (no security labels), but the query must not error
            assertNull(rs.getObject("seclabels"));
        }
    }

    // ========== Full column detail CTE query ==========

    @Test
    void fullColumnDetailCteQuery() throws Exception {
        // Run the full column detail query from db-log-10.txt and verify it succeeds
        String sql = "WITH INH_TABLES AS " +
                "(SELECT at.attname AS name, ph.inhparent AS inheritedid, ph.inhseqno, " +
                "pg_catalog.concat(nmsp_parent.nspname, '.',parent.relname ) AS inheritedfrom " +
                "FROM pg_catalog.pg_attribute at " +
                "JOIN pg_catalog.pg_inherits ph ON ph.inhparent = at.attrelid AND ph.inhrelid = " + tableOid + "::oid " +
                "JOIN pg_catalog.pg_class parent ON ph.inhparent = parent.oid " +
                "JOIN pg_catalog.pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace " +
                "GROUP BY at.attname, ph.inhparent, ph.inhseqno, inheritedfrom " +
                "ORDER BY at.attname, ph.inhparent, ph.inhseqno, inheritedfrom) " +
                "SELECT DISTINCT ON (att.attnum) att.attname as name, att.atttypid, att.attlen, att.attnum, att.attndims, " +
                "att.atttypmod, att.attacl, att.attnotnull, att.attoptions, att.attfdwoptions, att.attstattarget, " +
                "att.attstorage, att.attidentity, " +
                "pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS defval, " +
                "pg_catalog.format_type(ty.oid,NULL) AS typname, " +
                "pg_catalog.format_type(ty.oid,att.atttypmod) AS displaytypname, " +
                "pg_catalog.format_type(ty.oid,att.atttypmod) AS cltype, " +
                "inh.inheritedfrom, inh.inheritedid, " +
                "CASE WHEN ty.typelem > 0 THEN ty.typelem ELSE ty.oid END as elemoid, " +
                "(SELECT nspname FROM pg_catalog.pg_namespace WHERE oid = ty.typnamespace) as typnspname, " +
                "ty.typstorage AS defaultstorage, " +
                "description, pi.indkey, " +
                "(SELECT count(1) FROM pg_catalog.pg_type t2 WHERE t2.typname=ty.typname) > 1 AS isdup, " +
                "CASE WHEN length(coll.collname::text) > 0 AND length(nspc.nspname::text) > 0 THEN " +
                "pg_catalog.concat(pg_catalog.quote_ident(nspc.nspname),'.',pg_catalog.quote_ident(coll.collname)) " +
                "ELSE '' END AS collspcname, " +
                "EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid=att.attrelid AND contype='f' AND att.attnum=ANY(conkey)) As is_fk, " +
                "(SELECT pg_catalog.array_agg(provider || '=' || label) FROM pg_catalog.pg_seclabels sl1 WHERE sl1.objoid=att.attrelid AND sl1.objsubid=att.attnum) AS seclabels, " +
                "(CASE WHEN (att.attnum < 1) THEN true ElSE false END) AS is_sys_column, " +
                "(CASE WHEN (att.attidentity in ('a', 'd')) THEN 'i' WHEN (att.attgenerated in ('s')) THEN 'g' ELSE 'n' END) AS colconstype, " +
                "(CASE WHEN (att.attgenerated in ('s')) THEN pg_catalog.pg_get_expr(def.adbin, def.adrelid) END) AS genexpr, tab.relname as relname, " +
                "(CASE WHEN tab.relkind = 'v' THEN true ELSE false END) AS is_view_only, " +
                "(CASE WHEN att.attcompression = 'p' THEN 'pglz' WHEN att.attcompression = 'l' THEN 'lz4' END) AS attcompression, " +
                "seq.* " +
                "FROM pg_catalog.pg_attribute att " +
                "JOIN pg_catalog.pg_type ty ON ty.oid=atttypid " +
                "LEFT OUTER JOIN pg_catalog.pg_attrdef def ON adrelid=att.attrelid AND adnum=att.attnum " +
                "LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=att.attrelid AND des.objsubid=att.attnum AND des.classoid='pg_class'::regclass) " +
                "LEFT OUTER JOIN (pg_catalog.pg_depend dep JOIN pg_catalog.pg_class cs ON dep.classid='pg_class'::regclass AND dep.objid=cs.oid AND cs.relkind='S') ON dep.refobjid=att.attrelid AND dep.refobjsubid=att.attnum " +
                "LEFT OUTER JOIN pg_catalog.pg_index pi ON pi.indrelid=att.attrelid AND indisprimary " +
                "LEFT OUTER JOIN pg_catalog.pg_collation coll ON att.attcollation=coll.oid " +
                "LEFT OUTER JOIN pg_catalog.pg_namespace nspc ON coll.collnamespace=nspc.oid " +
                "LEFT OUTER JOIN pg_catalog.pg_sequence seq ON cs.oid=seq.seqrelid " +
                "LEFT OUTER JOIN pg_catalog.pg_class tab on tab.oid = att.attrelid " +
                "LEFT OUTER join INH_TABLES as INH ON att.attname = INH.name " +
                "WHERE att.attrelid = " + tableOid + "::oid AND att.attnum = 1::int " +
                "AND att.attnum > 0 AND att.attisdropped IS FALSE ORDER BY att.attnum";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Should return one row for the id column");

            // Verify key values match PG 18 expectations
            assertEquals("id", rs.getString("name"));
            assertEquals(23, rs.getInt("atttypid"), "integer type OID");
            assertEquals(4, rs.getShort("attlen"), "integer attlen = 4");
            assertEquals(1, rs.getShort("attnum"));
            assertTrue(rs.getBoolean("attnotnull"), "PK column is NOT NULL");
            assertEquals("integer", rs.getString("typname"));
            assertEquals("integer", rs.getString("cltype"));
            assertEquals("1", rs.getString("indkey"), "indkey must be int2vector '1'");
            assertEquals("pg_catalog", rs.getString("typnspname"));
            assertEquals("verify_col", rs.getString("relname"));
            assertFalse(rs.getBoolean("is_fk"));
            assertNull(rs.getObject("seclabels"));
            assertFalse(rs.getBoolean("is_sys_column"));
            assertEquals("n", rs.getString("colconstype"));
            assertFalse(rs.getBoolean("is_view_only"));
        }
    }
}
