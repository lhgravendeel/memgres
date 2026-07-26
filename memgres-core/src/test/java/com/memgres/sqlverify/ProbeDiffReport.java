package com.memgres.sqlverify;

import com.memgres.Pg18SampleSql5Test;
import com.memgres.core.Memgres;
import com.memgres.engine.util.IO;

import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Bug-hunting companion to {@link FeatureComparisonReport}: runs <em>unannotated</em>
 * probe SQL files against both real PostgreSQL 18 and Memgres and reports every
 * statement where the two engines diverge. For each divergence the report includes
 * the PG result in {@code begin-expected} annotation format so a confirmed bug can
 * be promoted into a feature-comparison verification file directly.
 *
 * <h3>Usage</h3>
 * <pre>
 *   mvn -pl memgres-core test-compile exec:java \
 *     -Dexec.mainClass="com.memgres.sqlverify.ProbeDiffReport" \
 *     -Dexec.classpathScope=test \
 *     -Dprobe.dir=path/to/probes -Dprobe.out=path/to/probe-diffs.md
 * </pre>
 */
public class ProbeDiffReport {

    private static final String PG_URL  = System.getProperty("pg.url",  "jdbc:postgresql://localhost:5432/memgrestest");
    private static final String PG_USER = System.getProperty("pg.user", "memgres");
    private static final String PG_PASS = System.getProperty("pg.pass", "memgres");

    public static void main(String[] args) throws Exception {
        Path probeDir = Path.of(System.getProperty("probe.dir"));
        Path outPath = Path.of(System.getProperty("probe.out", probeDir.resolve("probe-diffs.md").toString()));

        List<Path> sqlFiles;
        try (var stream = Files.list(probeDir)) {
            sqlFiles = stream.filter(p -> p.toString().endsWith(".sql")).sorted().collect(Collectors.toList());
        }
        System.out.println("=== Probe Diff: PG 18 vs Memgres ===");
        System.out.println("Probe files: " + sqlFiles.size());

        Memgres memgres = Memgres.builder().port(0).build().start();
        String memUrl = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
        Connection memConn = open(memUrl, "test", "test");
        Connection pgConn = open(PG_URL, PG_USER, PG_PASS);
        System.out.println("PG connected: " + pgConn.getMetaData().getDatabaseProductVersion());

        StringBuilder md = new StringBuilder("# Probe Diff Report\n\n");
        int totalStmts = 0, totalDiffs = 0;

        for (Path sqlFile : sqlFiles) {
            String fileName = sqlFile.getFileName().toString();
            List<Pg18SampleSql5Test.ParsedBlock> blocks = Pg18SampleSql5Test.parseFile(IO.readString(sqlFile));
            memConn = reset(memConn, memUrl, "test", "test");
            pgConn = reset(pgConn, PG_URL, PG_USER, PG_PASS);

            int fileDiffs = 0;
            StringBuilder fileMd = new StringBuilder("## " + fileName + "\n\n");

            for (int bi = 0; bi < blocks.size(); bi++) {
                String sql = blocks.get(bi).sql();
                if (sql.trim().isEmpty()) continue;
                totalStmts++;

                memConn = ensureLive(memConn, memUrl, "test", "test");
                pgConn = ensureLive(pgConn, PG_URL, PG_USER, PG_PASS);
                System.out.printf("  [%s stmt %d] %s%n", fileName, bi + 1,
                        sql.trim().replaceAll("\\s+", " ").substring(0, Math.min(80, sql.trim().length())));
                SqlVerifyHarness.StatementResult memResult = executeWithTimeout(memConn, sql, 10);
                SqlVerifyHarness.StatementResult pgResult = executeWithTimeout(pgConn, sql, 10);
                if (memResult == null || pgResult == null) {
                    totalDiffs++;
                    fileDiffs++;
                    fileMd.append("### stmt ").append(bi + 1).append(" — TIMEOUT\n\n");
                    fileMd.append("```sql\n").append(sql.trim()).append(";\n```\n\n");
                    fileMd.append("**Diff:** ").append(memResult == null ? "Memgres" : "PG").append(" timed out after 10s (possible hang)\n\n");
                    if (memResult == null) memConn = forceReopen(memConn, memUrl, "test", "test");
                    if (pgResult == null) pgConn = forceReopen(pgConn, PG_URL, PG_USER, PG_PASS);
                    continue;
                }

                String diff = FeatureComparisonReport.compareResults(pgResult, memResult, sql);
                if (diff != null) {
                    totalDiffs++;
                    fileDiffs++;
                    fileMd.append("### stmt ").append(bi + 1).append("\n\n");
                    fileMd.append("```sql\n").append(sql.trim()).append(";\n```\n\n");
                    fileMd.append("**Diff:** ").append(diff).append("\n\n");
                    fileMd.append("- **PG:** ").append(FeatureComparisonReport.formatResultBrief(pgResult)).append("\n");
                    fileMd.append("- **Memgres:** ").append(FeatureComparisonReport.formatResultBrief(memResult)).append("\n\n");
                    fileMd.append("PG result as annotation:\n\n```\n").append(toAnnotation(pgResult)).append("```\n\n");
                }
            }
            if (fileDiffs > 0) md.append(fileMd);
            System.out.printf("[%s] %-40s diffs=%d%n", fileDiffs == 0 ? "PASS" : "DIFF", fileName, fileDiffs);
        }

        md.append("\n---\nTotal statements: ").append(totalStmts).append(", diffs: ").append(totalDiffs).append("\n");
        IO.writeString(outPath, md.toString());
        System.out.println("\nTotal statements: " + totalStmts + ", diffs: " + totalDiffs);
        System.out.println("Report: " + outPath);

        memConn.close();
        memgres.close();
        pgConn.close();
    }

    static String toAnnotation(SqlVerifyHarness.StatementResult r) {
        StringBuilder sb = new StringBuilder();
        if (!r.success()) {
            sb.append("-- begin-expected-error\n-- message-like: ").append(r.errorMessage()).append("\n-- end-expected-error\n");
            return sb.toString();
        }
        if (r.columns() == null) {
            sb.append("-- (update count ").append(r.updateCount()).append(")\n");
            return sb.toString();
        }
        sb.append("-- begin-expected\n-- columns: ").append(String.join("|", r.columns())).append("\n");
        for (List<String> row : r.rows()) {
            sb.append("-- row: ");
            for (int c = 0; c < row.size(); c++) {
                if (c > 0) sb.append("|");
                sb.append(row.get(c) == null ? "NULL" : row.get(c));
            }
            sb.append("\n");
        }
        sb.append("-- end-expected\n");
        return sb.toString();
    }

    /** Execute on a worker thread; returns null on timeout (caller must reopen the connection). */
    private static SqlVerifyHarness.StatementResult executeWithTimeout(Connection conn, String sql, int seconds) {
        java.util.concurrent.ExecutorService ex = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        try {
            var fut = ex.submit(() -> SqlVerifyHarness.executeStatement(conn, sql));
            return fut.get(seconds, java.util.concurrent.TimeUnit.SECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            ex.shutdownNow();
        }
    }

    private static Connection forceReopen(Connection conn, String url, String user, String pass) {
        try { conn.close(); } catch (Exception ignored) {}
        try {
            return open(url, user, pass);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static Connection open(String url, String user, String pass) throws SQLException {
        Connection c = DriverManager.getConnection(url, user, pass);
        c.setAutoCommit(true);
        return c;
    }

    private static Connection reset(Connection conn, String url, String user, String pass) {
        conn = ensureLive(conn, url, user, pass);
        try (Statement s = conn.createStatement()) {
            if (!conn.getAutoCommit()) { conn.rollback(); conn.setAutoCommit(true); }
            s.execute("SET search_path = public");
            try { s.execute("SET TIME ZONE 'UTC'"); } catch (SQLException ignored) {}
            try { s.execute("SET lc_monetary = 'C'"); } catch (SQLException ignored) {}
            try { s.execute("SET lc_numeric = 'C'"); } catch (SQLException ignored) {}
            try { s.execute("SET lc_time = 'C'"); } catch (SQLException ignored) {}
            try { s.execute("SET DateStyle = 'ISO, MDY'"); } catch (SQLException ignored) {}
            try { s.execute("SET IntervalStyle = 'postgres'"); } catch (SQLException ignored) {}
        } catch (SQLException ignored) {}
        return conn;
    }

    private static Connection ensureLive(Connection conn, String url, String user, String pass) {
        try {
            if (conn != null && !conn.isClosed() && conn.isValid(2)) return conn;
        } catch (SQLException ignored) {}
        try {
            if (conn != null) { try { conn.close(); } catch (SQLException ignored) {} }
            return open(url, user, pass);
        } catch (SQLException e) {
            throw new RuntimeException("Could not reconnect to " + url + ": " + e.getMessage(), e);
        }
    }
}
