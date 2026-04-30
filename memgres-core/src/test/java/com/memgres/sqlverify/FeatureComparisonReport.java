package com.memgres.sqlverify;

import com.memgres.Pg18SampleSql5Test;
import com.memgres.core.Memgres;
import com.memgres.engine.util.IO;
import com.memgres.engine.util.Strs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
        String memUrl = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
        String memUser = "test", memPass = "test";
        Connection[] memConnHolder = new Connection[1];
        memConnHolder[0] = DriverManager.getConnection(memUrl, memUser, memPass);
        memConnHolder[0].setAutoCommit(true);

        Connection[] pgConnHolder = new Connection[1];
        boolean hasPg = true;
        try {
            // Reset PG test database for repeatable results
            resetPgDatabase();
            pgConnHolder[0] = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
            pgConnHolder[0].setAutoCommit(true);
            System.out.println("PG connected: " + pgConnHolder[0].getMetaData().getDatabaseProductVersion());
        } catch (SQLException e) {
            System.out.println("WARNING: Cannot connect to PostgreSQL (" + PG_URL + "): " + e.getMessage());
            System.out.println("Running Memgres-only mode (comparing against annotations).");
            hasPg = false;
        }
        System.out.println();

        // --- Run all files ---
        List<FileDifferences> allDiffs = new ArrayList<>();
        int totalSections = 0, totalPgMemDiffs = 0, totalPgAnnotDiffs = 0, totalMemAnnotDiffs = 0, totalSetupDivergences = 0, totalPgBugSkips = 0;

        for (Path sqlFile : sqlFiles) {
            String fileName = sqlFile.getFileName().toString();
            String content = IO.readString(sqlFile);
            List<Pg18SampleSql5Test.ParsedBlock> blocks = Pg18SampleSql5Test.parseFile(content);

            FileDifferences fd = new FileDifferences(fileName);

            // Reset connection state between files. If the PG connection has
            // been terminated by a prior fatal error (pgjdbc marks it closed
            // and every subsequent statement fails with 08003), reconnect so
            // we see real PG 18 behavior for this file's statements.
            memConnHolder[0] = ensureLive(memConnHolder[0], memUrl, memUser, memPass);
            safeReset(memConnHolder[0]);
            if (hasPg) {
                pgConnHolder[0] = ensureLive(pgConnHolder[0], PG_URL, PG_USER, PG_PASS);
                safeReset(pgConnHolder[0]);
            }

            for (int bi = 0; bi < blocks.size(); bi++) {
                Pg18SampleSql5Test.ParsedBlock block = blocks.get(bi);
                if (block.sql().trim().isEmpty()) continue;

                int sectionNum = bi + 1;
                totalSections++;

                // Execute on both engines — reconnect if the last statement
                // closed the session (otherwise every subsequent statement in
                // this file would cascade with SQLSTATE 08003).
                memConnHolder[0] = ensureLive(memConnHolder[0], memUrl, memUser, memPass);
                SqlVerifyHarness.StatementResult memResult = SqlVerifyHarness.executeStatement(memConnHolder[0], block.sql());
                SqlVerifyHarness.StatementResult pgResult = null;
                if (hasPg) {
                    pgConnHolder[0] = ensureLive(pgConnHolder[0], PG_URL, PG_USER, PG_PASS);
                    pgResult = SqlVerifyHarness.executeStatement(pgConnHolder[0], block.sql());
                }


                // Skip PG comparisons for known PG bugs
                if (block.pgBugSkip() != null && pgResult != null) {
                    fd.pgBugSkips.add(new Difference(sectionNum, block.sql(), block.pgBugSkip(), pgResult, memResult));
                    totalPgBugSkips++;
                } else {
                    // Compare PG vs Memgres for annotated statements
                    if (pgResult != null && block.expectation() != null) {
                        String pgMemDiff = compareResults(pgResult, memResult, block.sql());
                        if (pgMemDiff != null) {
                            fd.pgVsMemgres.add(new Difference(sectionNum, block.sql(), pgMemDiff, pgResult, memResult));
                            totalPgMemDiffs++;
                        }
                    }

                    // Detect setup statement divergences (unannotated statements
                    // where one engine errors and the other succeeds)
                    if (pgResult != null && block.expectation() == null) {
                        if (pgResult.success() != memResult.success()) {
                            String desc;
                            if (pgResult.success()) {
                                desc = "Setup divergence: PG succeeded, Memgres errored: " + memResult.errorMessage();
                            } else {
                                desc = "Setup divergence: PG errored (" + pgResult.errorState() + ": " + pgResult.errorMessage() + "), Memgres succeeded";
                            }
                            fd.setupDivergences.add(new Difference(sectionNum, block.sql(), desc, pgResult, memResult));
                            totalSetupDivergences++;
                        }
                    }

                    // Compare PG against annotations (if present and not a known PG bug)
                    if (block.expectation() != null && pgResult != null) {
                        String pgAnnotDiff = compareToAnnotation(pgResult, block);
                        if (pgAnnotDiff != null) {
                            fd.pgVsAnnotation.add(new Difference(sectionNum, block.sql(), pgAnnotDiff, pgResult, null));
                            totalPgAnnotDiffs++;
                        }
                    }
                }

                // Always compare Memgres against annotations (if present)
                if (block.expectation() != null) {
                    String memAnnotDiff = compareToAnnotation(memResult, block);
                    if (memAnnotDiff != null) {
                        fd.memgresVsAnnotation.add(new Difference(sectionNum, block.sql(), memAnnotDiff, null, memResult));
                        totalMemAnnotDiffs++;
                    }
                }
            }

            allDiffs.add(fd);
            String status = fd.totalDiffs() == 0 ? "PASS" : "FAIL";
            System.out.printf("[%s] %-55s  pg-vs-mem=%d  pg-vs-annot=%d  mem-vs-annot=%d  setup-div=%d%n",
                    status, fileName, fd.pgVsMemgres.size(), fd.pgVsAnnotation.size(), fd.memgresVsAnnotation.size(), fd.setupDivergences.size());
        }

        // --- Summary ---
        System.out.println();
        System.out.println(Strs.repeat("=", 80));
        System.out.printf("TOTAL: %d sections across %d files%n", totalSections, sqlFiles.size());
        System.out.printf("  PG vs Memgres differences:      %d%n", totalPgMemDiffs);
        System.out.printf("  PG vs annotation differences:    %d%n", totalPgAnnotDiffs);
        System.out.printf("  Memgres vs annotation diffs:     %d%n", totalMemAnnotDiffs);
        System.out.printf("  Setup statement divergences:     %d%n", totalSetupDivergences);
        if (totalPgBugSkips > 0) {
            System.out.printf("  Skipped (known PG bugs):         %d%n", totalPgBugSkips);
        }
        System.out.println(Strs.repeat("=", 80));

        // --- Write report ---
        Path reportPath = suiteDir.resolve("differences.md");
        writeReport(allDiffs, reportPath, hasPg, totalSections, sqlFiles.size());
        System.out.println("\nReport written to: " + reportPath);

        // --- Cleanup ---
        memConnHolder[0].close();
        memgres.close();
        if (pgConnHolder[0] != null) pgConnHolder[0].close();
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

            // Row data — use tolerant cell matching (boolean normalization,
            // numeric tolerance, NULL equivalence) to avoid false positives
            for (int r = 0; r < pg.rows().size(); r++) {
                List<String> pgCells = pg.rows().get(r);
                List<String> memCells = mem.rows().get(r);
                if (pgCells.size() != memCells.size()) {
                    return "Row " + (r + 1) + " column count differs: PG=" + pgCells.size() + " Memgres=" + memCells.size();
                }
                for (int c = 0; c < pgCells.size(); c++) {
                    String pv = pgCells.get(c) == null ? "" : pgCells.get(c);
                    String mv = memCells.get(c) == null ? "" : memCells.get(c);
                    if (!Pg18SampleSql5Test.cellMatches(pv, mv)) {
                        String pgRow = SqlVerifyHarness.formatRow(pgCells);
                        String memRow = SqlVerifyHarness.formatRow(memCells);
                        StringBuilder debug = new StringBuilder();
                        debug.append("Row ").append(r + 1).append(" differs: PG=[").append(pgRow)
                                .append("] Memgres=[").append(memRow).append("]");
                        // Append hex dump and type info for the differing cell
                        debug.append("\n  - Column: ").append(pg.columns().get(c));
                        if (pg.columnTypeNames() != null) {
                            debug.append(" pgType=").append(pg.columnTypeNames().get(c));
                        }
                        if (mem.columnTypeNames() != null) {
                            debug.append(" memType=").append(mem.columnTypeNames().get(c));
                        }
                        debug.append("\n  - PG hex: ").append(toHex(pgCells.get(c)));
                        debug.append("\n  - Memgres hex: ").append(toHex(memCells.get(c)));
                        debug.append("\n  - PG len: ").append(pv.length())
                                .append(", Memgres len: ").append(mv.length());
                        return debug.toString();
                    }
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
                List<String> rowData = result.rows().get(r);
                int colCount = rowData.size();

                // Annotation rows may use comma or pipe as separator.
                // Use column count to split intelligently: split(", ", colCount) ensures
                // commas inside structured values (arrays, composites, JSON) are preserved.
                if (!expectedRow.contains("|") && expectedRow.contains(",") && colCount > 1) {
                    String[] parts = expectedRow.split(", ", colCount);
                    if (parts.length == colCount) {
                        expectedRow = String.join("|", parts);
                    }
                    // else: commas are part of the value, leave as-is
                }

                // Build actual row in pipe-delimited format
                StringBuilder sb = new StringBuilder();
                for (int c = 0; c < colCount; c++) {
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
        int totalPgMem = 0, totalPgAnn = 0, totalMemAnn = 0, totalSetupDiv = 0, totalBugSkips = 0;
        for (FileDifferences fd : allDiffs) {
            totalPgMem += fd.pgVsMemgres.size();
            totalPgAnn += fd.pgVsAnnotation.size();
            totalMemAnn += fd.memgresVsAnnotation.size();
            totalSetupDiv += fd.setupDivergences.size();
            totalBugSkips += fd.pgBugSkips.size();
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
        if (hasPg) {
            md.append("| Setup statement divergences | ").append(totalSetupDiv).append(" |\n");
            if (totalBugSkips > 0) {
                md.append("| Skipped (known PG bugs) | ").append(totalBugSkips).append(" |\n");
            }
        }
        md.append("\n");

        // Per-file summary
        md.append("## Per-File Summary\n\n");
        md.append("| File | PG vs Memgres | PG vs Annot | Mem vs Annot | Setup Div | PG Bugs |\n");
        md.append("|------|:---:|:---:|:---:|:---:|:---:|\n");
        for (FileDifferences fd : allDiffs) {
            md.append("| ").append(fd.fileName).append(" | ");
            md.append(hasPg ? fd.pgVsMemgres.size() : "-").append(" | ");
            md.append(hasPg ? fd.pgVsAnnotation.size() : "-").append(" | ");
            md.append(fd.memgresVsAnnotation.size()).append(" | ");
            md.append(hasPg ? fd.setupDivergences.size() : "-").append(" | ");
            md.append(hasPg ? fd.pgBugSkips.size() : "-").append(" |\n");
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

            // Setup statement divergences (unannotated)
            if (hasPg && totalSetupDiv > 0) {
                md.append("## Setup Statement Divergences\n\n");
                md.append("Unannotated statements (setup/teardown) where one engine errors and the other succeeds.\n");
                md.append("These may cause cascading differences in subsequent annotated statements.\n\n");
                for (FileDifferences fd : allDiffs) {
                    if (fd.setupDivergences.isEmpty()) continue;
                    md.append("### ").append(fd.fileName).append("\n\n");
                    for (Difference diff : fd.setupDivergences) {
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

            // Known PG bugs (informational)
            if (hasPg && totalBugSkips > 0) {
                md.append("## Skipped: Known PG Bugs\n\n");
                md.append("Statements where PG comparison is skipped due to known PostgreSQL bugs.\n\n");
                for (FileDifferences fd : allDiffs) {
                    if (fd.pgBugSkips.isEmpty()) continue;
                    md.append("### ").append(fd.fileName).append("\n\n");
                    for (Difference diff : fd.pgBugSkips) {
                        md.append("**Stmt ").append(diff.sectionNum).append(":** ").append(diff.description).append("\n\n");
                        String sqlPreview = diff.sql.replaceAll("\\s+", " ").trim();
                        if (sqlPreview.length() > 200) sqlPreview = sqlPreview.substring(0, 200) + "...";
                        md.append("```sql\n").append(sqlPreview).append("\n```\n\n");
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

    /**
     * Drops and recreates the PG test database and removes non-system roles
     * for a fully clean slate. Connects to the 'postgres' database to issue
     * DROP/CREATE, then disconnects.
     */
    private static void resetPgDatabase() throws SQLException {
        String dbName = "memgrestest";
        // Extract host/port from PG_URL to connect to 'postgres' DB instead
        String adminUrl = PG_URL.replaceFirst("/[^/]*$", "/postgres");
        try (Connection admin = DriverManager.getConnection(adminUrl, PG_USER, PG_PASS)) {
            admin.setAutoCommit(true);
            // Terminate existing connections to the test database
            admin.createStatement().execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + dbName + "' AND pid <> pg_backend_pid()");
            admin.createStatement().execute("DROP DATABASE IF EXISTS " + dbName);

            // Drop non-system roles (roles are cluster-wide, survive DROP DATABASE).
            // Keep: postgres, the test user, and pg_ system roles.
            try (ResultSet rs = admin.createStatement().executeQuery(
                    "SELECT rolname FROM pg_roles WHERE rolname NOT LIKE 'pg_%' AND rolname <> 'postgres' AND rolname <> '" + PG_USER + "'")) {
                while (rs.next()) {
                    String role = rs.getString(1);
                    try {
                        // DROP OWNED BY revokes cluster-wide grants (e.g., GRANT SET ON PARAMETER)
                        // that survive DROP DATABASE and would otherwise block DROP ROLE.
                        admin.createStatement().execute("DROP OWNED BY " + quoteIdent(role) + " CASCADE");
                        admin.createStatement().execute("DROP ROLE IF EXISTS " + quoteIdent(role));
                    } catch (SQLException e) {
                        // Role may have dependencies in other databases; skip it
                    }
                }
            }

            admin.createStatement().execute("CREATE DATABASE " + dbName + " OWNER " + PG_USER);
        }
        System.out.println("PG database '" + dbName + "' reset (dropped and recreated, stale roles removed).");
    }

    private static String quoteIdent(String ident) {
        return "\"" + ident.replace("\"", "\"\"") + "\"";
    }

    private static void safeReset(Connection conn) {
        try {
            if (!conn.getAutoCommit()) {
                conn.rollback();
                conn.setAutoCommit(true);
            }
            try (Statement s = conn.createStatement()) {
                s.execute("SET search_path = public");
                // Force deterministic locale / TZ so PG and Memgres produce
                // identical locale-dependent output (money formatting,
                // extract(timezone ...), session-TZ-dependent to_char, etc.).
                // The host running the report may have TZ=Europe/X or
                // lc_monetary=de_DE — we don't want those to show up as
                // PG-vs-Memgres diffs.
                try { s.execute("SET TIME ZONE 'UTC'"); } catch (SQLException ignored) {}
                try { s.execute("SET lc_monetary = 'C'"); } catch (SQLException ignored) {}
                try { s.execute("SET lc_numeric = 'C'"); } catch (SQLException ignored) {}
                try { s.execute("SET lc_time = 'C'"); } catch (SQLException ignored) {}
                try { s.execute("SET DateStyle = 'ISO, MDY'"); } catch (SQLException ignored) {}
                try { s.execute("SET IntervalStyle = 'postgres'"); } catch (SQLException ignored) {}
            }
        } catch (SQLException ignored) {}
    }

    /**
     * Return a live connection. If the given connection is closed (pgjdbc
     * marks a connection closed after a fatal / protocol-level error and every
     * subsequent statement fails with 08003 "This connection has been closed"),
     * open a fresh one using the supplied credentials.
     */
    private static Connection ensureLive(Connection conn, String url, String user, String pass) {
        try {
            if (conn != null && !conn.isClosed() && conn.isValid(2)) {
                return conn;
            }
        } catch (SQLException ignored) {}
        try {
            if (conn != null) {
                try { conn.close(); } catch (SQLException ignored) {}
            }
            Connection fresh = DriverManager.getConnection(url, user, pass);
            fresh.setAutoCommit(true);
            return fresh;
        } catch (SQLException e) {
            throw new RuntimeException("Could not reconnect to " + url + ": " + e.getMessage(), e);
        }
    }

    private static Path findFeatureComparisonDir() {
        return SqlVerifyHarness.findResourceDir("feature-comparison");
    }

    /** Convert a string to hex representation for debugging wire-level differences. */
    private static String toHex(String s) {
        if (s == null) return "NULL";
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder(bytes.length * 3);
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) sb.append(' ');
            sb.append(String.format("%02x", bytes[i] & 0xFF));
        }
        return sb.toString() + " (\"" + s.replace("\n", "\\n").replace("\r", "\\r") + "\")";
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
        final List<Difference> setupDivergences = new ArrayList<>();
        final List<Difference> pgBugSkips = new ArrayList<>();

        FileDifferences(String fileName) {
            this.fileName = fileName;
        }

        int totalDiffs() {
            // pg bug skips are informational, not counted as diffs
            return pgVsMemgres.size() + pgVsAnnotation.size() + memgresVsAnnotation.size() + setupDivergences.size();
        }
    }
}
