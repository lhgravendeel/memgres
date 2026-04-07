package com.memgres.pg18;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

/**
 * Diagnostic test that runs the column-detail queries
 * against Memgres and prints the results for comparison with real PG 18.
 */
class Pg18ColumnDetailDiagnosticTest {

    static Memgres memgres;
    static Connection conn;
    static int tableOid;
    static int nsOid;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE users (user_id serial PRIMARY KEY, username text NOT NULL)");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_class WHERE relname = 'users'")) {
            rs.next();
            tableOid = rs.getInt(1);
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
            rs.next();
            nsOid = rs.getInt(1);
        }
        System.out.println("=== Table OID: " + tableOid + ", Namespace OID: " + nsOid + " ===\n");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    void query1_columnListing() throws Exception {
        System.out.println("=== Query 1: Column listing ===");
        String sql = "SELECT DISTINCT att.attname as name, att.attnum as OID, " +
                "pg_catalog.format_type(ty.oid,NULL) AS datatype, " +
                "att.attnotnull as not_null, " +
                "CASE WHEN att.atthasdef OR att.attidentity != '' OR ty.typdefault IS NOT NULL THEN True " +
                "ELSE False END as has_default_val, des.description, seq.seqtypid " +
                "FROM pg_catalog.pg_attribute att " +
                "JOIN pg_catalog.pg_type ty ON ty.oid=atttypid " +
                "JOIN pg_catalog.pg_namespace tn ON tn.oid=ty.typnamespace " +
                "JOIN pg_catalog.pg_class cl ON cl.oid=att.attrelid " +
                "JOIN pg_catalog.pg_namespace na ON na.oid=cl.relnamespace " +
                "LEFT OUTER JOIN pg_catalog.pg_type et ON et.oid=ty.typelem " +
                "LEFT OUTER JOIN pg_catalog.pg_attrdef def ON adrelid=att.attrelid AND adnum=att.attnum " +
                "LEFT OUTER JOIN (pg_catalog.pg_depend JOIN pg_catalog.pg_class cs ON classid='pg_class'::regclass AND objid=cs.oid AND cs.relkind='S') ON refobjid=att.attrelid AND refobjsubid=att.attnum " +
                "LEFT OUTER JOIN pg_catalog.pg_namespace ns ON ns.oid=cs.relnamespace " +
                "LEFT OUTER JOIN pg_catalog.pg_index pi ON pi.indrelid=att.attrelid AND indisprimary " +
                "LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=att.attrelid AND des.objsubid=att.attnum AND des.classoid='pg_class'::regclass) " +
                "LEFT OUTER JOIN pg_catalog.pg_sequence seq ON cs.oid=seq.seqrelid " +
                "WHERE att.attrelid = " + tableOid + "::oid AND att.attnum > 0 AND att.attisdropped IS FALSE " +
                "ORDER BY att.attnum";
        printQuery(sql);
    }

    @Test
    void query2_columnDetail_withCTE() throws Exception {
        System.out.println("=== Query 2: Column detail CTE (user_id, attnum=1) ===");
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
        printQuery(sql);
    }

    @Test
    void query3_inheritance() throws Exception {
        System.out.println("=== Query 3: Inheritance ===");
        String sql = "SELECT pg_catalog.array_to_string(pg_catalog.array_agg(inhrelname), ', ') inhrelname, attrname " +
                "FROM (SELECT inhparent::regclass AS inhrelname, a.attname AS attrname " +
                "FROM pg_catalog.pg_inherits i LEFT JOIN pg_catalog.pg_attribute a ON (attrelid = inhparent AND attnum > 0) " +
                "WHERE inhrelid = " + tableOid + "::oid ORDER BY inhseqno) a GROUP BY attrname";
        printQuery(sql);
    }

    @Test
    void query4_checkPgSequenceColumns() throws Exception {
        System.out.println("=== Query 4: pg_sequence columns ===");
        String sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_sequence' ORDER BY ordinal_position";
        printQuery(sql);
    }

    @Test
    void query5_checkPgDependColumns() throws Exception {
        System.out.println("=== Query 5: pg_depend columns ===");
        String sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_depend' ORDER BY ordinal_position";
        printQuery(sql);
    }

    @Test
    void query6_seqJoinDirect() throws Exception {
        System.out.println("=== Query 6: seq.* from pg_sequence joined via pg_depend ===");
        // Simplified version of the seq join in the big CTE query
        String sql = "SELECT seq.* " +
                "FROM pg_catalog.pg_attribute att " +
                "LEFT OUTER JOIN (pg_catalog.pg_depend dep JOIN pg_catalog.pg_class cs ON dep.classid='pg_class'::regclass AND dep.objid=cs.oid AND cs.relkind='S') " +
                "ON dep.refobjid=att.attrelid AND dep.refobjsubid=att.attnum " +
                "LEFT OUTER JOIN pg_catalog.pg_sequence seq ON cs.oid=seq.seqrelid " +
                "WHERE att.attrelid = " + tableOid + "::oid AND att.attnum = 1::int AND att.attnum > 0";
        printQuery(sql);
    }

    @Test
    void query7_pgSeclabels() throws Exception {
        System.out.println("=== Query 7: pg_seclabels ===");
        try {
            String sql = "SELECT * FROM pg_catalog.pg_seclabels LIMIT 0";
            printQuery(sql);
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
    }

    @Test
    void query8_distinctOn() throws Exception {
        System.out.println("=== Query 8: DISTINCT ON test ===");
        String sql = "SELECT DISTINCT ON (att.attnum) att.attname, att.attnum " +
                "FROM pg_catalog.pg_attribute att " +
                "WHERE att.attrelid = " + tableOid + "::oid AND att.attnum > 0 AND att.attisdropped IS FALSE " +
                "ORDER BY att.attnum";
        printQuery(sql);
    }

    private void printQuery(String sql) throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();
            System.out.println("Columns (" + colCount + "):");
            for (int i = 1; i <= colCount; i++) {
                System.out.println("  " + i + ": " + md.getColumnName(i) + " (type=" + md.getColumnTypeName(i) + ", nullable=" + md.isNullable(i) + ")");
            }
            int rowNum = 0;
            while (rs.next()) {
                rowNum++;
                System.out.println("Row " + rowNum + ":");
                for (int i = 1; i <= colCount; i++) {
                    Object val = rs.getObject(i);
                    System.out.println("  " + md.getColumnName(i) + " = " + val + (val != null ? " [" + val.getClass().getSimpleName() + "]" : ""));
                }
            }
            if (rowNum == 0) System.out.println("(no rows)");
            System.out.println();
        }
    }
}
