package com.memgres.pgdump;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simulates the EXACT sequence of queries that pg_dump v18 master sends
 * inside a single serializable transaction. If ANY query fails, the
 * transaction aborts and all subsequent queries fail with 25P02.
 * This test identifies the exact query that breaks the parallel dump.
 */
class PgDumpMasterTransactionTest {

    static Memgres memgres;
    static Connection setupConn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        setupConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        setupConn.setAutoCommit(true);
        PgDumpFromMemgresTest.populateReferenceSchema(setupConn);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (setupConn != null) setupConn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    void master_transaction_all_catalog_queries() throws Exception {
        Connection master = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        master.setAutoCommit(false);

        List<String> failedQueries = new ArrayList<>();

        try (Statement s = master.createStatement()) {
            // pg_dump starts with serializable read-only transaction
            exec(s, "START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE", failedQueries);

            // pg_dump preamble SET commands
            exec(s, "SET statement_timeout = 0", failedQueries);
            exec(s, "SET lock_timeout = 0", failedQueries);
            exec(s, "SET idle_in_transaction_session_timeout = 0", failedQueries);
            exec(s, "SET transaction_timeout = 0", failedQueries);
            exec(s, "SET extra_float_digits TO 3", failedQueries);
            exec(s, "SET synchronize_seqscans TO off", failedQueries);
            exec(s, "SET row_security = off", failedQueries);

            // Server version check
            query(s, "SELECT pg_catalog.current_setting('server_version_num')", failedQueries);
            query(s, "SHOW server_version_num", failedQueries);

            // Database info
            query(s, "SELECT d.oid, d.datname, pg_catalog.pg_encoding_to_char(d.encoding), " +
                    "d.datcollate, d.datctype FROM pg_catalog.pg_database d " +
                    "WHERE d.datname = current_database()", failedQueries);

            // Roles
            query(s, "SELECT oid, rolname FROM pg_catalog.pg_roles ORDER BY oid", failedQueries);

            // Role settings (was missing before Round 29!)
            query(s, "SELECT unnest(setconfig) FROM pg_catalog.pg_db_role_setting " +
                    "WHERE setrole = 0 AND setdatabase = 0::oid", failedQueries);

            // Tablespaces
            query(s, "SELECT oid, spcname FROM pg_catalog.pg_tablespace", failedQueries);

            // Schemas
            query(s, "SELECT oid, nspname FROM pg_catalog.pg_namespace ORDER BY nspname", failedQueries);

            // Extensions
            query(s, "SELECT e.oid, e.extname, e.extnamespace, e.extrelocatable, " +
                    "e.extversion FROM pg_catalog.pg_extension e", failedQueries);

            // All relations (tables, views, indexes, sequences, etc.)
            query(s, "SELECT c.oid, c.relname, c.relnamespace, c.relkind, c.reltype, " +
                    "c.relowner, c.relpersistence, c.relreplident " +
                    "FROM pg_catalog.pg_class c ORDER BY c.oid", failedQueries);

            // Types
            query(s, "SELECT t.oid, t.typname, t.typnamespace, t.typtype, t.typrelid " +
                    "FROM pg_catalog.pg_type t ORDER BY t.oid", failedQueries);

            // Attributes
            query(s, "SELECT a.attrelid, a.attname, a.atttypid, a.attnum, a.attnotnull, " +
                    "a.atthasdef, a.attisdropped FROM pg_catalog.pg_attribute a " +
                    "WHERE a.attnum > 0 ORDER BY a.attrelid, a.attnum", failedQueries);

            // Constraints
            query(s, "SELECT c.oid, c.conname, c.connamespace, c.contype, c.condeferrable, " +
                    "c.condeferred, c.conrelid, c.confrelid " +
                    "FROM pg_catalog.pg_constraint c", failedQueries);

            // Indexes
            query(s, "SELECT i.indexrelid, i.indrelid, i.indisunique, i.indisprimary, " +
                    "i.indisvalid FROM pg_catalog.pg_index i", failedQueries);

            // Triggers
            query(s, "SELECT t.oid, t.tgrelid, t.tgname, t.tgfoid, t.tgtype " +
                    "FROM pg_catalog.pg_trigger t WHERE NOT t.tgisinternal", failedQueries);

            // Descriptions (comments)
            query(s, "SELECT objoid, classoid, objsubid, description " +
                    "FROM pg_catalog.pg_description ORDER BY objoid", failedQueries);

            // Shared descriptions
            query(s, "SELECT objoid, classoid, description " +
                    "FROM pg_catalog.pg_shdescription", failedQueries);

            // Dependencies
            query(s, "SELECT classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype " +
                    "FROM pg_catalog.pg_depend WHERE deptype != 'p' ORDER BY objid", failedQueries);

            // Enums
            query(s, "SELECT e.oid, e.enumtypid, e.enumsortorder, e.enumlabel " +
                    "FROM pg_catalog.pg_enum e ORDER BY e.enumtypid, e.enumsortorder", failedQueries);

            // Procedures/functions
            query(s, "SELECT p.oid, p.proname, p.pronamespace, p.prorettype " +
                    "FROM pg_catalog.pg_proc p ORDER BY p.oid", failedQueries);

            // Publications
            query(s, "SELECT p.oid, p.pubname, p.pubowner FROM pg_catalog.pg_publication p", failedQueries);

            // Subscriptions
            query(s, "SELECT s.oid, s.subname FROM pg_catalog.pg_subscription s", failedQueries);

            // Rewrite rules
            query(s, "SELECT r.oid, r.ev_class, r.rulename FROM pg_catalog.pg_rewrite r", failedQueries);

            // Policies
            query(s, "SELECT p.oid, p.polrelid, p.polname FROM pg_catalog.pg_policy p", failedQueries);

            // Auth members
            query(s, "SELECT roleid, member, admin_option FROM pg_catalog.pg_auth_members", failedQueries);

            // Default ACLs
            query(s, "SELECT oid, defaclrole, defaclnamespace, defaclobjtype " +
                    "FROM pg_catalog.pg_default_acl", failedQueries);

            // Init privs
            query(s, "SELECT objoid, classoid, objsubid FROM pg_catalog.pg_init_privs", failedQueries);

            // Seclabels
            query(s, "SELECT objoid, classoid, objsubid, provider, label " +
                    "FROM pg_catalog.pg_seclabel", failedQueries);

            // Shared seclabels
            query(s, "SELECT objoid, classoid, provider, label " +
                    "FROM pg_catalog.pg_shseclabel", failedQueries);

            // Shared depends
            query(s, "SELECT classid, objid, objsubid, refclassid, refobjid, deptype " +
                    "FROM pg_catalog.pg_shdepend", failedQueries);

            // Casts
            query(s, "SELECT c.oid, c.castsource, c.casttarget, c.castfunc, c.castcontext " +
                    "FROM pg_catalog.pg_cast c", failedQueries);

            // Operators
            query(s, "SELECT o.oid, o.oprname, o.oprnamespace FROM pg_catalog.pg_operator o", failedQueries);

            // Opclasses/opfamilies
            query(s, "SELECT o.oid, o.opcname, o.opcnamespace FROM pg_catalog.pg_opclass o", failedQueries);
            query(s, "SELECT o.oid, o.opfname, o.opfnamespace FROM pg_catalog.pg_opfamily o", failedQueries);

            // Languages
            query(s, "SELECT l.oid, l.lanname FROM pg_catalog.pg_language l", failedQueries);

            // Access methods
            query(s, "SELECT a.oid, a.amname, a.amtype FROM pg_catalog.pg_am a", failedQueries);

            // Collations
            query(s, "SELECT c.oid, c.collname, c.collnamespace FROM pg_catalog.pg_collation c", failedQueries);

            // Conversions
            query(s, "SELECT c.oid, c.conname, c.connamespace FROM pg_catalog.pg_conversion c", failedQueries);

            // Transforms
            query(s, "SELECT t.oid, t.trftype, t.trflang FROM pg_catalog.pg_transform t", failedQueries);

            // Foreign data wrappers
            query(s, "SELECT f.oid, f.fdwname FROM pg_catalog.pg_foreign_data_wrapper f", failedQueries);

            // Foreign servers
            query(s, "SELECT s.oid, s.srvname, s.srvfdw FROM pg_catalog.pg_foreign_server s", failedQueries);

            // User mappings
            query(s, "SELECT u.oid, u.umuser, u.umserver FROM pg_catalog.pg_user_mapping u", failedQueries);

            // Sequences
            query(s, "SELECT s.seqrelid, s.seqtypid, s.seqstart, s.seqincrement, " +
                    "s.seqmax, s.seqmin, s.seqcache, s.seqcycle " +
                    "FROM pg_catalog.pg_sequence s", failedQueries);

            // Inheritance
            query(s, "SELECT inhrelid, inhparent, inhseqno FROM pg_catalog.pg_inherits", failedQueries);

            // Partitioned tables
            query(s, "SELECT partrelid, partstrat FROM pg_catalog.pg_partitioned_table", failedQueries);

            // Ranges
            query(s, "SELECT rngtypid, rngsubtype FROM pg_catalog.pg_range", failedQueries);

            // Large objects
            query(s, "SELECT oid FROM pg_catalog.pg_largeobject_metadata", failedQueries);

            // Event triggers
            query(s, "SELECT e.oid, e.evtname FROM pg_catalog.pg_event_trigger e", failedQueries);

            // Text search
            query(s, "SELECT oid, prsname FROM pg_catalog.pg_ts_parser", failedQueries);
            query(s, "SELECT oid, dictname FROM pg_catalog.pg_ts_dict", failedQueries);
            query(s, "SELECT oid, tmplname FROM pg_catalog.pg_ts_template", failedQueries);
            query(s, "SELECT oid, cfgname FROM pg_catalog.pg_ts_config", failedQueries);

            // Statistic extensions
            query(s, "SELECT oid, stxname, stxnamespace FROM pg_catalog.pg_statistic_ext", failedQueries);

            // Publication relations
            query(s, "SELECT oid, prpubid, prrelid FROM pg_catalog.pg_publication_rel", failedQueries);

            // Publication namespaces
            query(s, "SELECT oid, pnpubid, pnnspid FROM pg_catalog.pg_publication_namespace", failedQueries);

            // Export snapshot (required for parallel mode)
            query(s, "SELECT pg_catalog.pg_export_snapshot()", failedQueries);

            // Now lock ALL user tables with NOWAIT (what pg_dump does)
            List<String> tables = new ArrayList<>();
            try (ResultSet rs = s.executeQuery(
                    "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c " +
                    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
                    "WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') " +
                    "ORDER BY n.nspname, c.relname")) {
                while (rs.next()) {
                    tables.add("\"" + rs.getString(1) + "\".\"" + rs.getString(2) + "\"");
                }
            }

            for (String tbl : tables) {
                exec(s, "LOCK TABLE " + tbl + " IN ACCESS SHARE MODE NOWAIT", failedQueries);
            }

            s.execute("COMMIT");
        }
        master.close();

        assertTrue(failedQueries.isEmpty(),
                "The following queries failed inside the master transaction:\n" +
                String.join("\n", failedQueries));
    }

    private void exec(Statement s, String sql, List<String> failures) {
        try {
            s.execute(sql);
        } catch (SQLException e) {
            failures.add("EXEC: " + sql + " → " + e.getSQLState() + ": " + e.getMessage());
        }
    }

    private void query(Statement s, String sql, List<String> failures) {
        try (ResultSet rs = s.executeQuery(sql)) {
            rs.getMetaData(); // consume metadata
            while (rs.next()) {} // consume all rows
        } catch (SQLException e) {
            failures.add("QUERY: " + sql + " → " + e.getSQLState() + ": " + e.getMessage());
        }
    }
}
