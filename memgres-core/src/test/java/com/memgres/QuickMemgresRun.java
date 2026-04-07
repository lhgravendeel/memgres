package com.memgres;
import com.memgres.core.Memgres;
import com.memgres.sqlverify.SqlVerifyHarness;
import java.nio.file.*;
import java.sql.*;
public class QuickMemgresRun {
    public static void main(String[] args) throws Exception {
        Memgres m = Memgres.builder().port(0).build().start();
        Connection conn = DriverManager.getConnection(m.getJdbcUrl(), m.getUser(), m.getPassword());
        conn.setAutoCommit(true);
        var results = SqlVerifyHarness.runAll(conn);
        SqlVerifyHarness.saveResults(results, Path.of("memgres-core/src/test/resources/sql-verify/memgres-results.txt"));
        int total = 0, ok = 0;
        for (var fr : results) { for (var sr : fr.results()) { total++; if (sr.success()) ok++; } }
        System.out.println("\nTotal: " + total + " stmts, " + ok + " ok, " + (total-ok) + " errors");
        conn.close();
        m.close();
    }
}
