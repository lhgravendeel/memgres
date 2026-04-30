package com.memgres.pgdump;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.StringWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simulates the pg_dump parallel protocol to identify where it fails.
 * pg_dump -Fd -j2 uses:
 *   Master: BEGIN SERIALIZABLE READ ONLY → catalog queries → pg_export_snapshot() → LOCK TABLE NOWAIT
 *   Workers: BEGIN RR READ ONLY → SET TRANSACTION SNAPSHOT → COPY TO stdout
 *
 * This test exercises the actual COPY TO stdout protocol (via CopyManager),
 * which is what pg_dump uses — not SELECT queries.
 */
class ParallelDumpProtocolTest {

    static Memgres memgres;
    static Connection setupConn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        setupConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        setupConn.setAutoCommit(true);
        // Use the same schema as PgDumpFormatsTest
        PgDumpFromMemgresTest.populateReferenceSchema(setupConn);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (setupConn != null) setupConn.close();
        if (memgres != null) memgres.close();
    }

    /**
     * Simulates the full pg_dump parallel protocol using COPY TO stdout.
     * Master acquires locks on all tables, workers import snapshot and COPY data.
     */
    @Test
    void parallel_protocol_with_copy_to_stdout() throws Exception {
        String jdbcUrl = memgres.getJdbcUrl() + "?preferQueryMode=simple";

        // Discover all user tables (what pg_dump does via catalog queries)
        List<String> tables = new ArrayList<>();
        try (Statement s = setupConn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c " +
                     "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
                     "WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') " +
                     "ORDER BY n.nspname, c.relname")) {
            while (rs.next()) {
                tables.add("\"" + rs.getString(1) + "\".\"" + rs.getString(2) + "\"");
            }
        }
        assertFalse(tables.isEmpty(), "Should find user tables");

        // === MASTER connection ===
        Connection master = DriverManager.getConnection(jdbcUrl, memgres.getUser(), memgres.getPassword());
        master.setAutoCommit(false);

        try (Statement ms = master.createStatement()) {
            // pg_dump master: serializable read-only deferrable
            ms.execute("START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE");

            // pg_dump v18 preamble SET commands (all of them)
            ms.execute("SET statement_timeout = 0");
            ms.execute("SET lock_timeout = 0");
            ms.execute("SET idle_in_transaction_session_timeout = 0");
            ms.execute("SET transaction_timeout = 0");
            ms.execute("SET extra_float_digits TO 3");
            ms.execute("SET synchronize_seqscans TO off");
            ms.execute("SET row_security = off");

            // Critical catalog queries pg_dump issues
            assertCatalogQuery(ms, "SELECT oid, nspname FROM pg_catalog.pg_namespace ORDER BY nspname");
            assertCatalogQuery(ms, "SELECT c.oid, c.relname, c.relnamespace, c.relkind, c.reltype, " +
                    "c.relowner, c.relpersistence FROM pg_catalog.pg_class c ORDER BY c.oid");
            assertCatalogQuery(ms, "SELECT unnest(setconfig) FROM pg_catalog.pg_db_role_setting " +
                    "WHERE setrole = 0 AND setdatabase = 0::oid");
            assertCatalogQuery(ms, "SELECT oid, rolname FROM pg_catalog.pg_roles ORDER BY oid");
            assertCatalogQuery(ms, "SELECT d.oid, d.datname, pg_encoding_to_char(d.encoding) " +
                    "FROM pg_catalog.pg_database d");
            assertCatalogQuery(ms, "SELECT spcname FROM pg_catalog.pg_tablespace WHERE spcname <> 'pg_global'");

            // Export snapshot
            ResultSet snapRs = ms.executeQuery("SELECT pg_catalog.pg_export_snapshot()");
            assertTrue(snapRs.next());
            String snapshotId = snapRs.getString(1);
            snapRs.close();

            // Lock ALL user tables with NOWAIT (what pg_dump master does)
            for (String tbl : tables) {
                ms.execute("LOCK TABLE " + tbl + " IN ACCESS SHARE MODE NOWAIT");
            }

            // === WORKER 1: uses COPY TO stdout (actual pg_dump protocol) ===
            Connection w1 = DriverManager.getConnection(jdbcUrl, memgres.getUser(), memgres.getPassword());
            w1.setAutoCommit(false);
            CopyManager cm1 = new CopyManager(w1.unwrap(BaseConnection.class));
            try (Statement ws1 = w1.createStatement()) {
                ws1.execute("START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
                ws1.execute("SET TRANSACTION SNAPSHOT '" + snapshotId + "'");

                int half = tables.size() / 2;
                for (int i = 0; i < half; i++) {
                    // pg_dump uses COPY TO stdout, not SELECT
                    StringWriter sw = new StringWriter();
                    long rows = cm1.copyOut("COPY " + tables.get(i) + " TO stdout", sw);
                    assertTrue(rows >= 0, "COPY TO stdout should succeed for " + tables.get(i));
                }
                ws1.execute("COMMIT");
            }
            w1.close();

            // === WORKER 2: uses COPY TO stdout ===
            Connection w2 = DriverManager.getConnection(jdbcUrl, memgres.getUser(), memgres.getPassword());
            w2.setAutoCommit(false);
            CopyManager cm2 = new CopyManager(w2.unwrap(BaseConnection.class));
            try (Statement ws2 = w2.createStatement()) {
                ws2.execute("START TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY");
                ws2.execute("SET TRANSACTION SNAPSHOT '" + snapshotId + "'");

                int half = tables.size() / 2;
                for (int i = half; i < tables.size(); i++) {
                    StringWriter sw = new StringWriter();
                    long rows = cm2.copyOut("COPY " + tables.get(i) + " TO stdout", sw);
                    assertTrue(rows >= 0, "COPY TO stdout should succeed for " + tables.get(i));
                }
                ws2.execute("COMMIT");
            }
            w2.close();

            ms.execute("COMMIT");
        }
        master.close();
    }

    /**
     * Tests that COPY TO stdout works for every table in the reference schema.
     * If this fails, it identifies the exact table causing the issue.
     */
    @Test
    void copy_to_stdout_all_tables_individually() throws Exception {
        String jdbcUrl = memgres.getJdbcUrl() + "?preferQueryMode=simple";

        List<String> tables = new ArrayList<>();
        try (Statement s = setupConn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT n.nspname, c.relname FROM pg_catalog.pg_class c " +
                     "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
                     "WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') " +
                     "ORDER BY n.nspname, c.relname")) {
            while (rs.next()) {
                tables.add("\"" + rs.getString(1) + "\".\"" + rs.getString(2) + "\"");
            }
        }

        List<String> failures = new ArrayList<>();
        Connection conn = DriverManager.getConnection(jdbcUrl, memgres.getUser(), memgres.getPassword());
        CopyManager cm = new CopyManager(conn.unwrap(BaseConnection.class));

        for (String tbl : tables) {
            try {
                StringWriter sw = new StringWriter();
                cm.copyOut("COPY " + tbl + " TO stdout", sw);
            } catch (Exception e) {
                failures.add("COPY " + tbl + " TO stdout → " + e.getMessage());
            }
        }
        conn.close();

        assertTrue(failures.isEmpty(),
                "COPY TO stdout failed for:\n" + String.join("\n", failures));
    }

    /**
     * Tests that multi-table LOCK TABLE works — pg_dump v18 sends:
     * LOCK TABLE t1, t2, t3 IN ACCESS SHARE MODE
     * (comma-separated list, single statement)
     */
    @Test
    void multi_table_lock_statement() throws Exception {
        String jdbcUrl = memgres.getJdbcUrl() + "?preferQueryMode=simple";

        Connection conn = DriverManager.getConnection(jdbcUrl, memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("BEGIN");

            // Multi-table lock: exactly what pg_dump v18 sends
            s.execute("LOCK TABLE public.customers, public.orders, public.products IN ACCESS SHARE MODE");

            // Verify locks are compatible — a second connection should also get ACCESS SHARE
            Connection conn2 = DriverManager.getConnection(jdbcUrl, memgres.getUser(), memgres.getPassword());
            conn2.setAutoCommit(false);
            try (Statement s2 = conn2.createStatement()) {
                s2.execute("BEGIN");
                // Should NOT throw — AccessShareLock is compatible with AccessShareLock
                s2.execute("LOCK TABLE public.customers IN ACCESS SHARE MODE NOWAIT");
                s2.execute("LOCK TABLE public.orders IN ACCESS SHARE MODE NOWAIT");
                s2.execute("LOCK TABLE public.products IN ACCESS SHARE MODE NOWAIT");
                s2.execute("COMMIT");
            }
            conn2.close();

            s.execute("COMMIT");
        }
        conn.close();
    }

    /**
     * Tests multi-table lock with schema-qualified names (pg_dump sends these).
     */
    @Test
    void multi_table_lock_with_schemas() throws Exception {
        String jdbcUrl = memgres.getJdbcUrl() + "?preferQueryMode=simple";

        // Get all user tables for a realistic multi-table lock
        List<String> tables = new ArrayList<>();
        try (Statement s = setupConn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT n.nspname || '.' || c.relname FROM pg_catalog.pg_class c " +
                     "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
                     "WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema') " +
                     "ORDER BY n.nspname, c.relname")) {
            while (rs.next()) tables.add(rs.getString(1));
        }
        assertFalse(tables.isEmpty());

        Connection conn = DriverManager.getConnection(jdbcUrl, memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("BEGIN");
            // Lock ALL tables in a single statement, like pg_dump does
            s.execute("LOCK TABLE " + String.join(", ", tables) + " IN ACCESS SHARE MODE");
            s.execute("COMMIT");
        }
        conn.close();
    }

    private void assertCatalogQuery(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            rs.getMetaData();
        }
    }
}
