package com.memgres.sqlverify;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs the pg18_sample_sql verification files against memgres and compares
 * results to the PG 18 baseline captured by Pg18BaselineCollector.
 *
 * The PG baseline must be collected first by running Pg18BaselineCollector.
 * If the baseline file is missing, this test will fail with a clear message.
 */
class Pg18SqlVerifyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    void verifySqlCompatibility() throws Exception {
        Path resourceDir = SqlVerifyHarness.findResourceDir("sql-verify-pg18");
        Path memgresResultsPath = resourceDir.resolve("memgres-results.txt");

        // Run against memgres
        System.out.println("Running PG18 SQL verification against memgres...\n");
        List<SqlVerifyHarness.FileResult> actual = SqlVerifyHarness.runAll(conn, resourceDir);

        // Regenerate mode: save results and return (no comparison)
        if (Boolean.getBoolean("sqlverify.regenerate")) {
            SqlVerifyHarness.saveResults(actual, memgresResultsPath);
            System.out.println("Regenerated: " + memgresResultsPath);
            return;
        }

        // Compare against the saved memgres-results baseline
        assertTrue(Files.exists(memgresResultsPath),
                "Memgres results baseline not found. Run with -Dsqlverify.regenerate=true to generate it.");

        List<SqlVerifyHarness.FileResult> expected = SqlVerifyHarness.loadResults(memgresResultsPath);

        // Save current run for inspection
        Path currentRunPath = resourceDir.resolve("memgres-results-current.txt");
        SqlVerifyHarness.saveResults(actual, currentRunPath);

        // Compare
        List<String> diffs = SqlVerifyHarness.compare(expected, actual);

        if (!diffs.isEmpty()) {
            StringBuilder report = new StringBuilder();
            report.append("PG18 SQL verification found ").append(diffs.size()).append(" differences:\n\n");
            for (String diff : diffs) {
                report.append("  - ").append(diff).append("\n");
            }
            report.append("\nExpected baseline: ").append(memgresResultsPath);
            report.append("\nCurrent run: ").append(currentRunPath);
            report.append("\nTo accept new behavior, run with: -Dsqlverify.regenerate=true");
            fail(report.toString());
        }
    }
}
