package com.memgres.sqlverify;
import com.memgres.core.Memgres;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
public class MemgresVerify {
    public static void main(String[] args) throws Exception {
        Memgres m = Memgres.builder().port(0).build().start();
        Connection conn = DriverManager.getConnection(m.getJdbcUrl(), m.getUser(), m.getPassword());
        System.out.println("Running SQL verification against memgres...\n");
        List<SqlVerifyHarness.FileResult> results = SqlVerifyHarness.runAll(conn);
        Path outputPath = SqlVerifyHarness.findResourceDir().resolve("memgres-results.txt");
        SqlVerifyHarness.saveResults(results, outputPath);
        System.out.println("\nResults saved to: " + outputPath);
        int totalStmts = 0, totalOk = 0, totalErr = 0;
        for (var fr : results) {
            for (var sr : fr.results()) {
                totalStmts++;
                if (sr.success()) totalOk++;
                else totalErr++;
            }
        }
        System.out.printf("Total: %d statements, %d ok, %d errors%n", totalStmts, totalOk, totalErr);
        conn.close(); m.close();
    }
}
