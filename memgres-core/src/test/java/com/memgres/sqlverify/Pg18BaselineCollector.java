package com.memgres.sqlverify;

import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * Collects baseline results by running the pg18_sample_sql verification files
 * against a real PostgreSQL 18 instance.
 *
 * Usage:
 *   mvn -pl memgres-core exec:java \
 *     -Dexec.mainClass="com.memgres.sqlverify.Pg18BaselineCollector" \
 *     -Dexec.classpathScope=test
 *
 * Requires PostgreSQL on localhost:5432, database 'memgrestest',
 * user 'memgres', password 'memgres'.
 *
 * Outputs: memgres-core/src/test/resources/sql-verify-pg18/pg-baseline.txt
 */
public class Pg18BaselineCollector {

    private static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest";
    private static final String PG_USER = "memgres";
    private static final String PG_PASS = "memgres";

    public static void main(String[] args) throws Exception {
        System.out.println("=== PG18 Baseline Collector ===\n");

        // Clean the database first
        cleanDatabase();

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            System.out.println("Connected to: " + conn.getMetaData().getDatabaseProductVersion());
            System.out.println();

            Path resourceDir = SqlVerifyHarness.findResourceDir("sql-verify-pg18");
            List<SqlVerifyHarness.FileResult> results = SqlVerifyHarness.runAll(conn, resourceDir);

            Path outputPath = resourceDir.resolve("pg-baseline.txt");
            SqlVerifyHarness.saveResults(results, outputPath);
            System.out.println("\nBaseline saved to: " + outputPath);

            // Summary
            int totalStmts = 0, totalOk = 0, totalErr = 0;
            for (var fr : results) {
                for (var sr : fr.results()) {
                    totalStmts++;
                    if (sr.success()) totalOk++;
                    else totalErr++;
                }
            }
            System.out.printf("Total: %d statements, %d ok, %d errors%n", totalStmts, totalOk, totalErr);
        }

        // Clean up after ourselves
        cleanDatabase();
    }

    private static void cleanDatabase() {
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            conn.setAutoCommit(true);
            try (Statement s = conn.createStatement()) {
                // Drop non-public schemas
                List<String> schemas = new ArrayList<>();
                try (ResultSet rs = s.executeQuery(
                        "SELECT nspname FROM pg_namespace WHERE nspname NOT IN " +
                        "('pg_catalog','information_schema','pg_toast','public') " +
                        "AND nspname NOT LIKE 'pg_temp%' AND nspname NOT LIKE 'pg_toast_temp%'")) {
                    while (rs.next()) schemas.add(rs.getString(1));
                }
                for (String schema : schemas) {
                    try { s.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE"); } catch (SQLException ignored) {}
                }

                // Drop objects in public schema
                dropByQuery(s, "SELECT 'DROP VIEW IF EXISTS public.' || quote_ident(viewname) || ' CASCADE' FROM pg_views WHERE schemaname = 'public'");
                dropByQuery(s, "SELECT 'DROP MATERIALIZED VIEW IF EXISTS public.' || quote_ident(matviewname) || ' CASCADE' FROM pg_matviews WHERE schemaname = 'public'");
                dropByQuery(s, "SELECT 'DROP TABLE IF EXISTS public.' || quote_ident(tablename) || ' CASCADE' FROM pg_tables WHERE schemaname = 'public'");
                dropByQuery(s, "SELECT 'DROP FUNCTION IF EXISTS ' || oid::regprocedure || ' CASCADE' FROM pg_proc WHERE pronamespace = 'public'::regnamespace");
                dropByQuery(s, "SELECT 'DROP TYPE IF EXISTS public.' || quote_ident(typname) || ' CASCADE' FROM pg_type WHERE typnamespace = 'public'::regnamespace AND typtype IN ('e','c','d') AND typname NOT LIKE '\\_%'");
                dropByQuery(s, "SELECT 'DROP SEQUENCE IF EXISTS public.' || quote_ident(sequencename) || ' CASCADE' FROM pg_sequences WHERE schemaname = 'public'");

                List<String> extensions = new ArrayList<>();
                try (ResultSet rs = s.executeQuery("SELECT extname FROM pg_extension WHERE extname != 'plpgsql'")) {
                    while (rs.next()) extensions.add(rs.getString(1));
                }
                for (String ext : extensions) {
                    try { s.execute("DROP EXTENSION IF EXISTS \"" + ext + "\" CASCADE"); } catch (SQLException ignored) {}
                }

                // Drop roles created by tests
                dropByQuery(s, "SELECT 'DROP ROLE IF EXISTS ' || quote_ident(rolname) FROM pg_roles WHERE rolname NOT IN ('memgres') AND NOT rolsuper AND rolname != current_user AND rolname NOT LIKE 'pg_%'");

                s.execute("SET search_path TO public, pg_catalog");
            }
        } catch (Exception e) {
            System.err.println("Warning: cleanup failed: " + e.getMessage());
        }
    }

    private static void dropByQuery(Statement s, String query) {
        try {
            List<String> stmts = new ArrayList<>();
            try (ResultSet rs = s.executeQuery(query)) {
                while (rs.next()) stmts.add(rs.getString(1));
            }
            for (String drop : stmts) {
                try { s.execute(drop); } catch (SQLException ignored) {}
            }
        } catch (SQLException ignored) {}
    }
}
