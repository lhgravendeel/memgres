package com.memgres.sqlverify;

import com.memgres.engine.util.IO;

import com.memgres.engine.util.Strs;

import java.nio.file.*;
import java.util.*;

/**
 * Generates diff reports between PG baseline and memgres results for ALL verification suites.
 *
 * Usage:
 *   mvn exec:java \
 *     -Dexec.mainClass="com.memgres.sqlverify.AllSuitesDiffReport" \
 *     -Dexec.classpathScope=test
 */
public class AllSuitesDiffReport {

    public static void main(String[] args) throws Exception {
        String[] suites = {"sql-verify", "sql-verify-pg18", "sql-verify-pg18-v2"};

        int totalDiffs = 0;
        int totalStatements = 0;
        int totalMatched = 0;

        for (String suite : suites) {
            Path resourceDir = SqlVerifyHarness.findResourceDir(suite);
            Path baselinePath = resourceDir.resolve("pg-baseline.txt");
            Path memgresPath = resourceDir.resolve("memgres-results.txt");

            System.out.println("=== " + suite + " ===");

            if (!Files.exists(baselinePath)) {
                System.out.println("  No PG baseline found. Skipping.\n");
                continue;
            }
            if (!Files.exists(memgresPath)) {
                System.out.println("  No memgres results found. Skipping.\n");
                continue;
            }

            List<SqlVerifyHarness.FileResult> expected = SqlVerifyHarness.loadResults(baselinePath);
            List<SqlVerifyHarness.FileResult> actual = SqlVerifyHarness.loadResults(memgresPath);

            // Count total statements
            int stmtCount = expected.stream().mapToInt(fr -> fr.results().size()).sum();
            totalStatements += stmtCount;

            List<String> diffs = SqlVerifyHarness.compare(expected, actual);
            totalDiffs += diffs.size();
            totalMatched += stmtCount - diffs.size();

            System.out.println("  Statements: " + stmtCount);
            System.out.println("  Differences: " + diffs.size());
            System.out.println("  Match rate: " + String.format("%.1f%%", 100.0 * (stmtCount - diffs.size()) / stmtCount));

            if (!diffs.isEmpty()) {
                // Save diff report
                Path reportPath = resourceDir.resolve("diff-report.txt");
                StringBuilder sb = new StringBuilder();
                sb.append(suite + " Diff Report: ").append(diffs.size()).append(" differences\n");
                sb.append(Strs.repeat("=", 70)).append("\n\n");
                for (String diff : diffs) {
                    sb.append(diff).append("\n");
                }
                IO.writeString(reportPath, sb.toString());
                System.out.println("  Report saved to: " + reportPath);

                // Print first 10 diffs as preview
                System.out.println("  First " + Math.min(10, diffs.size()) + " differences:");
                for (int i = 0; i < Math.min(10, diffs.size()); i++) {
                    System.out.println("    " + diffs.get(i));
                }
                if (diffs.size() > 10) {
                    System.out.println("    ... and " + (diffs.size() - 10) + " more");
                }
            }
            System.out.println();
        }

        System.out.println("=== TOTALS ===");
        System.out.println("  Total statements: " + totalStatements);
        System.out.println("  Total matched: " + totalMatched);
        System.out.println("  Total differences: " + totalDiffs);
        System.out.println("  Overall match rate: " + String.format("%.1f%%", 100.0 * totalMatched / totalStatements));
    }
}
