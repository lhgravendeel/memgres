package com.memgres.sqlverify;

import com.memgres.Pg18SampleSql5Test;
import com.memgres.core.Memgres;
import com.memgres.engine.util.IO;
import com.memgres.engine.util.Strs;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Runs all feature-comparison SQL files against both a real PostgreSQL 18
 * instance and Memgres, then produces a {@code differences.md} report
 * listing every query where the two engines diverge.
 *
 * <p>This class reuses:
 * <ul>
 *   <li>{@link Pg18SampleSql5Test#parseFile} — annotation-aware SQL parser
 *       (understands {@code begin-expected}, {@code begin-expected-error}, etc.)</li>
 *   <li>{@link SqlVerifyHarness#executeStatement} — execute + capture result/error</li>
 *   <li>{@link SqlVerifyHarness} normalization/comparison helpers</li>
 * </ul>
 *
 * <h3>Usage</h3>
 * <pre>
 *   # Requires PG 18 on localhost:5432, database 'memgrestest', user/pass 'memgres'
 *   mvn -pl memgres-core exec:java \
 *     -Dexec.mainClass="com.memgres.sqlverify.FeatureComparisonReport" \
 *     -Dexec.classpathScope=test
 * </pre>
 *
 * <p>Override PG connection with system properties:
 * <pre>
 *   -Dpg.url=jdbc:postgresql://host:port/db -Dpg.user=u -Dpg.pass=p
 * </pre>
 *
 * <p>Output: {@code memgres-core/src/test/resources/feature-comparison/differences.md}
 */
public class FeatureComparisonReport {

    // PG defaults — overridable via system properties
    private static final String PG_URL  = System.getProperty("pg.url",  "jdbc:postgresql://localhost:5432/memgrestest");
    private static final String PG_USER = System.getProperty("pg.user", "memgres");
    private static final String PG_PASS = System.getProperty("pg.pass", "memgres");

    public static void main(String[] args) throws Exception {
        Path suiteDir = findFeatureComparisonDir();
        List<Path> sqlFiles;
        try (var stream = Files.list(suiteDir)) {
            sqlFiles = stream.filter(p -> p.toString().endsWith(".sql")).sorted().collect(Collectors.toList());
        }

        System.out.println("=== Feature Comparison: PG 18 vs Memgres ===");
        System.out.println("SQL files: " + sqlFiles.size());
        System.out.println("Suite dir: " + suiteDir);
        System.out.println();

        // --- Start engines ---
        Memgres memgres = Memgres.builder().port(0).build().start();
        Connection memgresConn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test", "test", "test");
        memgresConn.setAutoCommit(true);

        Connection pgConn = null;
        boolean hasPg = true;
        try {
            pgConn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
            pgConn.setAutoCommit(true);
            System.out.println("PG connected: " + pgConn.getMetaData().getDatabaseProductVersion());
        } catch (SQLException e) {
            System.out.println("WARNING: Cannot connect to PostgreSQL (" + PG_URL + "): " + e.getMessage());
            System.out.println("Running Memgres-only mode (comparing against annotations).");
            hasPg = false;
        }
        System.out.println();

        // --- Run all files ---
        List<FileDifferences> allDiffs = new ArrayList<>();
        int totalSections = 0, totalPgMemDiffs = 0, totalPgAnnotDiffs = 0, totalMemAnnotDiffs = 0;

        for (Path sqlFile : sqlFiles) {
            String fileName = sqlFile.getFileName().toString();
            String content = IO.readString(sqlFile);
            List<Pg18SampleSql5Test.ParsedBlock> blocks = Pg18SampleSql5Test.parseFile(content);

            FileDifferences fd = new FileDifferences(fileName);

            // Reset connection state between files
            safeReset(memgresConn);
            if (hasPg) safeReset(pgConn);

            for (int bi = 0; bi < blocks.size(); bi++) {
                Pg18SampleSql5Test.ParsedBlock block = blocks.get(bi);
                if (block.sql().trim().isEmpty()) continue;

                int sectionNum = bi + 1;
                totalSections++;

                // Execute on both engines
                SqlVerifyHarness.StatementResult memResult = SqlVerifyHarness.executeStatement(memgresConn, block.sql());
                SqlVerifyHarness.StatementResult pgResult = hasPg ? SqlVerifyHarness.executeStatement(pgConn, block.sql()) : null;

                // Compare PG vs Memgres
                if (pgResult != null) {
                    String pgMemDiff = compareResults(pgResult, memResult, block.sql());
                    if (pgMemDiff != null) {
                        fd.pgVsMemgres.add(new Difference(sectionNum, block.sql(), pgMemDiff, pgResult, memResult));
                        totalPgMemDiffs++;
                    }
                }

                // Compare each engine against annotations (if present)
                if (block.expectation() != null) {
                    if (pgResult != null) {
                        String pgAnnotDiff = compareToAnnotation(pgResult, block);
                        if (pgAnnotDiff != null) {
                            fd.pgVsAnnotation.add(new Difference(sectionNum, block.sql(), pgAnnotDiff, pgResult, null));
                            totalPgAnnotDiffs++;
                        }
                    }
                    String memAnnotDiff = compareToAnnotation(memResult, block);
                    if (memAnnotDiff != null) {
                        fd.memgresVsAnnotation.add(new Difference(sectionNum, block.sql(), memAnnotDiff, null, memResult));
                        totalMemAnnotDiffs++;
                    }
                }
            }

            allDiffs.add(fd);
            String status = fd.totalDiffs() == 0 ? "PASS" : "FAIL";
            System.out.printf("[%s] %-55s  pg-vs-mem=%d  pg-vs-annot=%d  mem-vs-annot=%d%n",
                    status, fileName, fd.pgVsMemgres.size(), fd.pgVsAnnotation.size(), fd.memgresVsAnnotation.size());
        }

        // --- Summary ---
        System.out.println();
        System.out.println(Strs.repeat("=", 80));
        System.out.printf("TOTAL: %d sections across %d files%n", totalSections, sqlFiles.size());
        System.out.printf("  PG vs Memgres differences:      %d%n", totalPgMemDiffs);
        System.out.printf("  PG vs annotation differences:    %d%n", totalPgAnnotDiffs);
        System.out.printf("  Memgres vs annotation diffs:     %d%n", totalMemAnnotDiffs);
        System.out.println(Strs.repeat("=", 80));

        // --- Write report ---
        Path reportPath = suiteDir.resolve("differences.md");
        writeReport(allDiffs, reportPath, hasPg, totalSections, sqlFiles.size());
        System.out.println("\nReport written to: " + reportPath);

        // --- Cleanup ---
        memgresConn.close();
        memgres.close();
        if (pgConn != null) pgConn.close();
    }

    // =========================================================================
    // Comparison: PG result vs Memgres result
    // =========================================================================

    /**
     * Compare two engine results. Returns null if they match, or a diff description.
     * Reuses {@link SqlVerifyHarness} normalization to handle timestamps, UUIDs, etc.
     */
    static String compareResults(SqlVerifyHarness.StatementResult pg, SqlVerifyHarness.StatementResult mem, String sql) {
        // Success vs error
        if (pg.success() != mem.success()) {
            if (pg.success()) {
                return "PG succeeded, Memgres errored: " + mem.errorMessage();
            } else {
                return "PG errored (" + pg.errorState() + ": " + pg.errorMessage() + "), Memgres succeeded";
            }
        }

        // Both errors — compare SQLSTATE
        if (!pg.success()) {
            if (pg.errorState() != null && mem.errorState() != null && !pg.errorState().equals(mem.errorState())) {
                return "SQLSTATE differs: PG=" + pg.errorState() + " Memgres=" + mem.errorState();
            }
            return null; // both errored with same state
        }

        // Both success — compare structure
        if (pg.columns() != null && mem.columns() == null) return "PG returned result set, Memgres returned update count";
        if (pg.columns() == null && mem.columns() != null) return "PG returned update count, Memgres returned result set";

        if (pg.columns() != null) {
            // Column comparison (case-insensitive)
            List<String> pgCols = pg.columns().stream().map(String::toLowerCase).collect(Collectors.toList());
            List<String> memCols = mem.columns().stream().map(String::toLowerCase).collect(Collectors.toList());
            if (!pgCols.equals(memCols)) {
                return "Columns differ: PG=" + pgCols + " Memgres=" + memCols;
            }

            // Row count
            if (pg.rows().size() != mem.rows().size()) {
                return "Row count differs: PG=" + pg.rows().size() + " Memgres=" + mem.rows().size();
            }

            // Row data (use SqlVerifyHarness normalization)
            for (int r = 0; r < pg.rows().size(); r++) {
                String pgRow = SqlVerifyHarness.formatRow(pg.rows().get(r));
                String memRow = SqlVerifyHarness.formatRow(mem.rows().get(r));
                if (!pgRow.equals(memRow)) {
                    return "Row " + (r + 1) + " differs: PG=[" + pgRow + "] Memgres=[" + memRow + "]";
                }
            }
        } else {
            if (pg.updateCount() != mem.updateCount()) {
                return "Update count differs: PG=" + pg.updateCount() + " Memgres=" + mem.updateCount();
            }
        }

        return null;
    }

    // =========================================================================
    // Comparison: engine result vs SQL annotation
    // =========================================================================

    /**
     * Compare a single engine result against the SQL file annotation.
     * Reuses the value-matching logic from {@link Pg18SampleSql5Test}.
     */
    static String compareToAnnotation(SqlVerifyHarness.StatementResult result, Pg18SampleSql5Test.ParsedBlock block) {
        Object expectation = block.expectation();
        if (expectation == null) return null;

        if (expectation instanceof Pg18SampleSql5Test.ExpectedError ee) {
            if (result.success()) {
                return "Expected error containing \"" + ee.messageLike() + "\" but succeeded";
            }
            String msg = result.errorMessage() != null ? result.errorMessage().toLowerCase() : "";
            if (!msg.contains(ee.messageLike().toLowerCase())) {
                return "Error message mismatch: expected substring \"" + ee.messageLike() + "\" in \"" + result.errorMessage() + "\"";
            }
            return null;
        }

        if (expectation instanceof Pg18SampleSql5Test.ExpectedResult er) {
            if (!result.success()) {
                return "Expected result but got error: " + result.errorMessage();
            }
            if (result.columns() == null) {
                return "Expected result set but got update count";
            }

            // Column check
            List<String> actualCols = result.columns().stream().map(String::toLowerCase).collect(Collectors.toList());
            List<String> expectedCols = er.columns().stream().map(String::toLowerCase).collect(Collectors.toList());
            if (!actualCols.equals(expectedCols)) {
                return "Columns differ: expected=" + expectedCols + " actual=" + actualCols;
            }

            // Row count
            if (result.rows().size() != er.rows().size()) {
                return "Row count differs: expected=" + er.rows().size() + " actual=" + result.rows().size();
            }

            // Row data — use Pg18SampleSql5Test value matching for tolerance
            for (int r = 0; r < er.rows().size(); r++) {
                String expectedRow = er.rows().get(r);
                // Annotation rows may use comma or pipe as separator.
                // Normalize to pipe-delimited for comparison (matching valuesMatch expectations).
                if (!expectedRow.contains("|") && expectedRow.contains(",")) {
                    expectedRow = expectedRow.replace(", ", "|").replace(",", "|");
                }
                // Build actual row in pipe-delimited format
                StringBuilder sb = new StringBuilder();
                List<String> rowData = result.rows().get(r);
                for (int c = 0; c < rowData.size(); c++) {
                    if (c > 0) sb.append("|");
                    String val = rowData.get(c);
                    sb.append(val == null ? "NULL" : val);
                }
                String actualRow = sb.toString();
                // Use the tolerant matcher from test harness
                if (!Pg18SampleSql5Test.valuesMatch(expectedRow, actualRow)) {
                    return "Row " + (r + 1) + " differs: expected=[" + expectedRow + "] actual=[" + actualRow + "]";
                }
            }
            return null;
        }

        return null;
    }

    // =========================================================================
    // Report generation
    // =========================================================================

    static void writeReport(List<FileDifferences> allDiffs, Path reportPath, boolean hasPg,
                            int totalSections, int fileCount) throws IOException {
        StringBuilder md = new StringBuilder();
        md.append("# Feature Comparison: Differences Report\n\n");
        md.append("Generated by `FeatureComparisonReport`\n\n");

        // Summary table
        int totalPgMem = 0, totalPgAnn = 0, totalMemAnn = 0;
        for (FileDifferences fd : allDiffs) {
            totalPgMem += fd.pgVsMemgres.size();
            totalPgAnn += fd.pgVsAnnotation.size();
            totalMemAnn += fd.memgresVsAnnotation.size();
        }

        md.append("## Summary\n\n");
        md.append("| Metric | Count |\n");
        md.append("|--------|-------|\n");
        md.append("| SQL files | ").append(fileCount).append(" |\n");
        md.append("| Total sections | ").append(totalSections).append(" |\n");
        if (hasPg) {
            md.append("| PG vs Memgres differences | ").append(totalPgMem).append(" |\n");
            md.append("| PG vs annotation mismatches | ").append(totalPgAnn).append(" |\n");
        }
        md.append("| Memgres vs annotation mismatches | ").append(totalMemAnn).append(" |\n");
        md.append("\n");

        // Per-file summary
        md.append("## Per-File Summary\n\n");
        md.append("| File | PG vs Memgres | PG vs Annot | Mem vs Annot |\n");
        md.append("|------|:---:|:---:|:---:|\n");
        for (FileDifferences fd : allDiffs) {
            md.append("| ").append(fd.fileName).append(" | ");
            md.append(hasPg ? fd.pgVsMemgres.size() : "-").append(" | ");
            md.append(hasPg ? fd.pgVsAnnotation.size() : "-").append(" | ");
            md.append(fd.memgresVsAnnotation.size()).append(" |\n");
        }
        md.append("\n");

        // Detailed failures grouped by file
        boolean anyDiffs = allDiffs.stream().anyMatch(fd -> fd.totalDiffs() > 0);
        if (!anyDiffs) {
            md.append("## No differences found!\n\n");
            md.append("All annotated expectations match across both engines.\n");
        } else {
            // PG vs Memgres differences
            if (hasPg && totalPgMem > 0) {
                md.append("## PG vs Memgres Differences\n\n");
                md.append("Queries where PostgreSQL and Memgres produce different results.\n\n");
                for (FileDifferences fd : allDiffs) {
                    if (fd.pgVsMemgres.isEmpty()) continue;
                    md.append("### ").append(fd.fileName).append("\n\n");
                    for (Difference diff : fd.pgVsMemgres) {
                        appendDifference(md, diff);
                    }
                }
            }

            // Memgres vs annotation
            if (totalMemAnn > 0) {
                md.append("## Memgres vs Annotation Mismatches\n\n");
                md.append("Queries where Memgres results don't match the SQL file annotations.\n\n");
                for (FileDifferences fd : allDiffs) {
                    if (fd.memgresVsAnnotation.isEmpty()) continue;
                    md.append("### ").append(fd.fileName).append("\n\n");
                    for (Difference diff : fd.memgresVsAnnotation) {
                        appendDifference(md, diff);
                    }
                }
            }

            // PG vs annotation (annotation bugs)
            if (hasPg && totalPgAnn > 0) {
                md.append("## PG vs Annotation Mismatches\n\n");
                md.append("Queries where real PostgreSQL doesn't match the SQL file annotations ");
                md.append("(likely annotation bugs).\n\n");
                for (FileDifferences fd : allDiffs) {
                    if (fd.pgVsAnnotation.isEmpty()) continue;
                    md.append("### ").append(fd.fileName).append("\n\n");
                    for (Difference diff : fd.pgVsAnnotation) {
                        appendDifference(md, diff);
                    }
                }
            }
        }

        IO.writeString(reportPath, md.toString());
    }

    private static void appendDifference(StringBuilder md, Difference diff) {
        md.append("**Stmt ").append(diff.sectionNum).append(":** ").append(diff.description).append("\n\n");
        String sqlPreview = diff.sql.replaceAll("\\s+", " ").trim();
        if (sqlPreview.length() > 200) sqlPreview = sqlPreview.substring(0, 200) + "...";
        md.append("```sql\n").append(sqlPreview).append("\n```\n\n");
        if (diff.pgResult != null) {
            md.append("- **PG:** ").append(formatResultBrief(diff.pgResult)).append("\n");
        }
        if (diff.memResult != null) {
            md.append("- **Memgres:** ").append(formatResultBrief(diff.memResult)).append("\n");
        }
        md.append("\n");
    }

    static String formatResultBrief(SqlVerifyHarness.StatementResult r) {
        if (!r.success()) {
            return "ERROR [" + r.errorState() + "]: " + r.errorMessage();
        }
        if (r.columns() != null) {
            String cols = String.join(", ", r.columns());
            if (r.rows().isEmpty()) return "OK (" + cols + ") 0 rows";
            if (r.rows().size() <= 3) {
                StringBuilder sb = new StringBuilder("OK (" + cols + ") ");
                for (int i = 0; i < r.rows().size(); i++) {
                    if (i > 0) sb.append(" ; ");
                    sb.append("[").append(SqlVerifyHarness.formatRow(r.rows().get(i))).append("]");
                }
                return sb.toString();
            }
            return "OK (" + cols + ") " + r.rows().size() + " rows";
        }
        return "OK " + r.updateCount() + " rows affected";
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static void safeReset(Connection conn) {
        try {
            if (!conn.getAutoCommit()) {
                conn.rollback();
                conn.setAutoCommit(true);
            }
            try (Statement s = conn.createStatement()) {
                s.execute("SET search_path = public");
            }
        } catch (SQLException ignored) {}
    }

    private static Path findFeatureComparisonDir() {
        return SqlVerifyHarness.findResourceDir("feature-comparison");
    }

    // =========================================================================
    // Data classes
    // =========================================================================

    static class Difference {
        final int sectionNum;
        final String sql;
        final String description;
        final SqlVerifyHarness.StatementResult pgResult;
        final SqlVerifyHarness.StatementResult memResult;

        Difference(int sectionNum, String sql, String description,
                   SqlVerifyHarness.StatementResult pgResult, SqlVerifyHarness.StatementResult memResult) {
            this.sectionNum = sectionNum;
            this.sql = sql;
            this.description = description;
            this.pgResult = pgResult;
            this.memResult = memResult;
        }
    }

    static class FileDifferences {
        final String fileName;
        final List<Difference> pgVsMemgres = new ArrayList<>();
        final List<Difference> pgVsAnnotation = new ArrayList<>();
        final List<Difference> memgresVsAnnotation = new ArrayList<>();

        FileDifferences(String fileName) {
            this.fileName = fileName;
        }

        int totalDiffs() {
            return pgVsMemgres.size() + pgVsAnnotation.size() + memgresVsAnnotation.size();
        }
    }
}
