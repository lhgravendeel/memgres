package com.memgres.sqlverify;

import com.memgres.engine.util.IO;

import com.memgres.engine.util.Strs;

import java.nio.file.*;
import java.util.*;

/**
 * Generates a diff report between PG18 baseline and memgres results for V2 suite.
 *
 * Usage:
 *   mvn -pl memgres-core exec:java \
 *     -Dexec.mainClass="com.memgres.sqlverify.Pg18V2DiffReport" \
 *     -Dexec.classpathScope=test
 */
public class Pg18V2DiffReport {

    public static void main(String[] args) throws Exception {
        Path resourceDir = SqlVerifyHarness.findResourceDir("sql-verify-pg18-v2");
        Path baselinePath = resourceDir.resolve("pg-baseline.txt");
        Path memgresPath = resourceDir.resolve("memgres-results.txt");

        if (!Files.exists(baselinePath)) {
            System.err.println("No PG baseline found. Run Pg18V2BaselineCollector first.");
            return;
        }
        if (!Files.exists(memgresPath)) {
            System.err.println("No memgres results found. Run Pg18V2SqlVerifyTest first.");
            return;
        }

        List<SqlVerifyHarness.FileResult> expected = SqlVerifyHarness.loadResults(baselinePath);
        List<SqlVerifyHarness.FileResult> actual = SqlVerifyHarness.loadResults(memgresPath);

        List<String> diffs = SqlVerifyHarness.compare(expected, actual);

        Path reportPath = resourceDir.resolve("diff-report.txt");
        StringBuilder sb = new StringBuilder();
        sb.append("PG18 V2 Diff Report: ").append(diffs.size()).append(" differences\n");
        sb.append(Strs.repeat("=", 70)).append("\n\n");
        for (String diff : diffs) {
            sb.append(diff).append("\n");
        }
        IO.writeString(reportPath, sb.toString());

        System.out.println("Total differences: " + diffs.size());
        System.out.println("Report saved to: " + reportPath);
        System.out.println();

        // Print all diffs
        for (String diff : diffs) {
            System.out.println(diff);
        }
    }
}
