package com.memgres.pg18;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

/**
 * Runs every distinct client query from db-log-10.txt against Memgres,
 * prints column metadata and row values for comparison with PG 18.
 * Queries are parameterized with dynamic OIDs from the test setup.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class Pg18FullQueryComparisonTest {

    static Memgres memgres;
    static Connection conn;
    static int dbOid;
    static int nsOid;
    static int tableOid;

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
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_database WHERE datname = current_database()")) {
            rs.next(); dbOid = rs.getInt(1);
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
            rs.next(); nsOid = rs.getInt(1);
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_class WHERE relname = 'users'")) {
            rs.next(); tableOid = rs.getInt(1);
        }
        System.out.println("=== dbOid=" + dbOid + ", nsOid=" + nsOid + ", tableOid=" + tableOid + " ===\n");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- Q1: Database info ----
    @Test @Order(1)
    void q01_databaseInfo() throws Exception {
        runAndPrint("Q01_db_info", "SELECT db.oid as did, db.datname, db.datallowconn, " +
                "pg_encoding_to_char(db.encoding) AS serverencoding, " +
                "has_database_privilege(db.oid, 'CREATE') as cancreate, datistemplate " +
                "FROM pg_catalog.pg_database db WHERE db.datname = current_database()");
    }

    // ---- Q2: Roles ----
    @Test @Order(2)
    void q02_roles() throws Exception {
        runAndPrint("Q02_roles", "SELECT roles.oid as id, roles.rolname as name, " +
                "roles.rolsuper as is_superuser, " +
                "CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreaterole END as can_create_role, " +
                "CASE WHEN roles.rolsuper THEN true ELSE roles.rolcreatedb END as can_create_db " +
                "FROM pg_catalog.pg_roles as roles WHERE rolname = current_user");
    }

    // ---- Q3: Recovery check ----
    @Test @Order(3)
    void q03_recovery() throws Exception {
        runAndPrint("Q03_recovery", "SELECT CASE WHEN usesuper THEN pg_catalog.pg_is_in_recovery() ELSE FALSE END as inrecovery, " +
                "CASE WHEN usesuper AND pg_catalog.pg_is_in_recovery() THEN pg_is_wal_replay_paused() ELSE FALSE END as isreplaypaused " +
                "FROM pg_catalog.pg_user WHERE usename=current_user");
    }

    // ---- Q4: BDR/replication check ----
    @Test @Order(4)
    void q04_bdrCheck() throws Exception {
        runAndPrint("Q04_bdr_check", "SELECT CASE " +
                "WHEN (SELECT count(extname) FROM pg_catalog.pg_extension WHERE extname='bdr') > 0 THEN 'pgd' " +
                "WHEN (SELECT COUNT(*) FROM pg_replication_slots) > 0 THEN 'log' ELSE NULL END as type");
    }

    // ---- Q5: Database listing with tablespace ----
    @Test @Order(5)
    void q05_databaseListing() throws Exception {
        runAndPrint("Q05_db_listing",
                "SELECT db.oid as did, db.datname as name, ta.spcname as spcname, db.datallowconn, " +
                "db.datistemplate AS is_template, " +
                "pg_catalog.has_database_privilege(db.oid, 'CREATE') as cancreate, datdba as owner, " +
                "descr.description " +
                "FROM pg_catalog.pg_database db " +
                "LEFT OUTER JOIN pg_catalog.pg_tablespace ta ON db.dattablespace = ta.oid " +
                "LEFT OUTER JOIN pg_catalog.pg_shdescription descr ON (db.oid=descr.objoid AND descr.classoid='pg_database'::regclass) " +
                "WHERE db.oid > 16383::OID OR db.datname IN ('postgres', 'edb') ORDER BY datname");
    }

    // ---- Q6: Schema listing ----
    @Test @Order(6)
    void q06_schemaListing() throws Exception {
        runAndPrint("Q06_schema_listing",
                "SELECT nsp.oid, nsp.nspname as name, " +
                "pg_catalog.has_schema_privilege(nsp.oid, 'CREATE') as can_create, " +
                "pg_catalog.has_schema_privilege(nsp.oid, 'USAGE') as has_usage, " +
                "des.description " +
                "FROM pg_catalog.pg_namespace nsp " +
                "LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=nsp.oid AND des.classoid='pg_namespace'::regclass) " +
                "WHERE nspname NOT LIKE E'pg\\\\_%' AND NOT (" +
                "(nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR " +
                "(nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR " +
                "(nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1))) " +
                "ORDER BY nspname");
    }

    // ---- Q7: Schema catalog check ----
    @Test @Order(7)
    void q07_schemaCatalogCheck() throws Exception {
        runAndPrint("Q07_schema_catalog", "SELECT nsp.nspname as schema_name, " +
                "(nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR " +
                "(nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR " +
                "(nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1)) AS is_catalog, " +
                "CASE WHEN nsp.nspname = ANY('{information_schema}') THEN false ELSE true END AS db_support " +
                "FROM pg_catalog.pg_namespace nsp WHERE nsp.oid = " + nsOid + "::OID");
    }

    // ---- Q8: Table listing ----
    @Test @Order(8)
    void q08_tableListing() throws Exception {
        runAndPrint("Q08_table_listing",
                "SELECT rel.oid, rel.relname AS name, " +
                "(SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE) AS triggercount, " +
                "(SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE AND tgenabled = 'O') AS has_enable_triggers, " +
                "(CASE WHEN rel.relkind = 'p' THEN true ELSE false END) AS is_partitioned, " +
                "(SELECT count(1) FROM pg_catalog.pg_inherits WHERE inhrelid=rel.oid LIMIT 1) as is_inherits, " +
                "(SELECT count(1) FROM pg_catalog.pg_inherits WHERE inhparent=rel.oid LIMIT 1) as is_inherited, " +
                "des.description " +
                "FROM pg_catalog.pg_class rel " +
                "LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=rel.oid AND des.objsubid=0 AND des.classoid='pg_class'::regclass) " +
                "WHERE rel.relkind IN ('r','s','t','p') AND rel.relnamespace = " + nsOid + "::oid " +
                "AND NOT rel.relispartition ORDER BY rel.relname");
    }

    // ---- Q9: Partitioned check ----
    @Test @Order(9)
    void q09_partitionedCheck() throws Exception {
        runAndPrint("Q09_partitioned",
                "SELECT CASE WHEN c.relkind = 'p' THEN True ELSE False END As ptable " +
                "FROM pg_catalog.pg_class c WHERE c.oid = " + tableOid + "::oid");
    }

    // ---- Q10: Schema+table name lookup ----
    @Test @Order(10)
    void q10_schemaTableLookup() throws Exception {
        runAndPrint("Q10_schema_table",
                "SELECT nsp.nspname AS schema, rel.relname AS table " +
                "FROM pg_catalog.pg_class rel JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid::oid " +
                "WHERE rel.oid = " + tableOid + "::oid");
    }

    // ---- Q11: Column listing ----
    @Test @Order(11)
    void q11_columnListing() throws Exception {
        runAndPrint("Q11_column_listing",
                "SELECT DISTINCT att.attname as name, att.attnum as OID, pg_catalog.format_type(ty.oid,NULL) AS datatype, " +
                "att.attnotnull as not_null, " +
                "CASE WHEN att.atthasdef OR att.attidentity != '' OR ty.typdefault IS NOT NULL THEN True ELSE False END as has_default_val, " +
                "des.description, seq.seqtypid " +
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
                "WHERE att.attrelid = " + tableOid + "::oid AND att.attnum > 0 AND att.attisdropped IS FALSE ORDER BY att.attnum");
    }

    // ---- Q12: Collation listing ----
    @Test @Order(12)
    void q12_collationListing() throws Exception {
        runAndPrint("Q12_collation",
                "SELECT CASE WHEN length(nspname::text) > 0 AND length(collname::text) > 0 THEN " +
                "pg_catalog.concat(pg_catalog.quote_ident(nspname), '.', pg_catalog.quote_ident(collname)) " +
                "ELSE '' END AS copy_collation " +
                "FROM pg_catalog.pg_collation c, pg_catalog.pg_namespace n " +
                "WHERE c.collnamespace=n.oid ORDER BY nspname, collname");
    }

    // ---- Q13: Column detail CTE (THE BIG ONE) ----
    @Test @Order(13)
    void q13_columnDetailCte() throws Exception {
        runAndPrint("Q13_column_detail_CTE",
                "WITH INH_TABLES AS " +
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
                "AND att.attnum > 0 AND att.attisdropped IS FALSE ORDER BY att.attnum");
    }

    // ---- Q14: Type listing (with ::name cast) ----
    @Test @Order(14)
    void q14_typeListing() throws Exception {
        runAndPrint("Q14_type_listing",
                "SELECT * FROM (SELECT pg_catalog.format_type(t.oid,NULL) AS typname, " +
                "CASE WHEN typelem > 0 THEN typelem ELSE t.oid END as elemoid, " +
                "typlen, typtype, t.oid, nspname, " +
                "(SELECT COUNT(1) FROM pg_catalog.pg_type t2 WHERE t2.typname = t.typname) > 1 AS isdup, " +
                "CASE WHEN t.typcollation != 0 THEN TRUE ELSE FALSE END AS is_collatable " +
                "FROM pg_catalog.pg_type t " +
                "JOIN pg_catalog.pg_namespace nsp ON typnamespace=nsp.oid " +
                "WHERE (NOT (typname = 'unknown' AND nspname = 'pg_catalog')) " +
                "AND typisdefined AND typtype IN ('b', 'c', 'd', 'e', 'r', 'm') " +
                "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relnamespace=typnamespace AND relname = typname AND relkind != 'c') " +
                "AND (typname NOT LIKE '_%' OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relnamespace=typnamespace AND relname = substring(typname FROM 2)::name AND relkind != 'c')) " +
                "AND nsp.nspname != 'information_schema' " +
                "UNION SELECT 'smallserial', 0, 2, 'b', 0, 'pg_catalog', false, false " +
                "UNION SELECT 'bigserial', 0, 8, 'b', 0, 'pg_catalog', false, false " +
                "UNION SELECT 'serial', 0, 4, 'b', 0, 'pg_catalog', false, false " +
                ") AS dummy ORDER BY nspname <> 'pg_catalog', nspname <> 'public', nspname, 1");
    }

    // ---- Q15: Inheritance lookup ----
    @Test @Order(15)
    void q15_inheritance() throws Exception {
        runAndPrint("Q15_inheritance",
                "SELECT pg_catalog.array_to_string(pg_catalog.array_agg(inhrelname), ', ') inhrelname, attrname " +
                "FROM (SELECT inhparent::regclass AS inhrelname, a.attname AS attrname " +
                "FROM pg_catalog.pg_inherits i LEFT JOIN pg_catalog.pg_attribute a ON (attrelid = inhparent AND attnum > 0) " +
                "WHERE inhrelid = " + tableOid + "::oid ORDER BY inhseqno) a GROUP BY attrname");
    }

    // ---- Q16: ACL explode (the last query that uses aclexplode) ----
    @Test @Order(16)
    void q16_aclExplode() throws Exception {
        System.out.println("=== Q16_acl_explode ===");
        String sql = "SELECT 'attacl' as deftype, " +
                "COALESCE(gt.rolname, 'PUBLIC') grantee, " +
                "g.rolname grantor, " +
                "pg_catalog.array_agg(privilege_type order by privilege_type) as privileges, " +
                "pg_catalog.array_agg(is_grantable) as grantable " +
                "FROM (SELECT d.grantee, d.grantor, d.is_grantable, " +
                "CASE d.privilege_type " +
                "WHEN 'CONNECT' THEN 'c' WHEN 'CREATE' THEN 'C' WHEN 'DELETE' THEN 'd' " +
                "WHEN 'EXECUTE' THEN 'X' WHEN 'INSERT' THEN 'a' WHEN 'REFERENCES' THEN 'x' " +
                "WHEN 'SELECT' THEN 'r' WHEN 'TEMPORARY' THEN 'T' WHEN 'TRIGGER' THEN 't' " +
                "WHEN 'TRUNCATE' THEN 'D' WHEN 'UPDATE' THEN 'w' WHEN 'USAGE' THEN 'U' ELSE 'UNKNOWN' END AS privilege_type " +
                "FROM (SELECT attacl FROM pg_catalog.pg_attribute att WHERE att.attrelid = " + tableOid + "::oid AND att.attnum = 1::int) acl, " +
                "(SELECT (d).grantee AS grantee, (d).grantor AS grantor, (d).is_grantable AS is_grantable, (d).privilege_type AS privilege_type " +
                "FROM (SELECT pg_catalog.aclexplode(attacl) as d FROM pg_catalog.pg_attribute att WHERE att.attrelid = " + tableOid + "::oid AND att.attnum = 1::int) a) d) d " +
                "LEFT JOIN pg_catalog.pg_roles g ON (d.grantor = g.oid) " +
                "LEFT JOIN pg_catalog.pg_roles gt ON (d.grantee = gt.oid) " +
                "GROUP BY g.rolname, gt.rolname ORDER BY grantee";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            printResultSet(rs);
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
            System.out.println();
        }
    }

    // ---- Q17: Dashboard stats (simplified) ----
    @Test @Order(17)
    void q17_dashboardStats() throws Exception {
        runAndPrint("Q17_dashboard_stats",
                "SELECT 'session_stats' AS chart_name, pg_catalog.row_to_json(t) AS chart_data " +
                "FROM (SELECT (SELECT count(*) FROM pg_catalog.pg_stat_activity) AS \"Total\", " +
                "(SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE state = 'active') AS \"Active\", " +
                "(SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE state = 'idle') AS \"Idle\") t");
    }

    // ---- Q18: Extension count ----
    @Test @Order(18)
    void q18_extensionCount() throws Exception {
        runAndPrint("Q18_extension_count",
                "SELECT COUNT(*) FROM pg_extension WHERE extname IN ('edb_job_scheduler', 'dbms_scheduler')");
    }

    // ---- Q19: pgagent check ----
    @Test @Order(19)
    void q19_pgagentCheck() throws Exception {
        runAndPrint("Q19_pgagent_check",
                "SELECT has_table_privilege('pgagent.pga_job', 'INSERT, SELECT, UPDATE') has_priviledge " +
                "WHERE EXISTS(SELECT has_schema_privilege('pgagent', 'USAGE') " +
                "WHERE EXISTS(SELECT cl.oid FROM pg_catalog.pg_class cl " +
                "LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid=relnamespace " +
                "WHERE relname='pga_job' AND nspname='pgagent'))");
    }

    private void runAndPrint(String label, String sql) throws Exception {
        System.out.println("=== " + label + " ===");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            printResultSet(rs);
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
            System.out.println();
        }
    }

    private void printResultSet(ResultSet rs) throws Exception {
        ResultSetMetaData md = rs.getMetaData();
        int colCount = md.getColumnCount();
        System.out.println("Columns (" + colCount + "):");
        for (int i = 1; i <= colCount; i++) {
            System.out.println("  " + i + ": " + md.getColumnName(i) + " (" + md.getColumnTypeName(i) + ")");
        }
        int rowNum = 0;
        while (rs.next()) {
            rowNum++;
            System.out.println("Row " + rowNum + ":");
            for (int i = 1; i <= colCount; i++) {
                Object val = rs.getObject(i);
                String repr = val == null ? "NULL" : "'" + val + "' [" + val.getClass().getSimpleName() + "]";
                System.out.println("  " + md.getColumnName(i) + " = " + repr);
            }
        }
        if (rowNum == 0) System.out.println("(no rows)");
        System.out.println();
    }
}
