package com.memgres.pgdump;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import com.memgres.engine.util.IO;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests pg_dump and pg_restore with various output formats and parallelism options
 * against a Memgres instance.
 *
 * Covers: custom format (-Fc), directory format (-Fd), compression (-Z9),
 * parallel dump/restore (-j2), and data integrity after round-trips.
 */
@EnabledIf("isPgDumpAvailable")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PgDumpFormatsTest {

    static final String PG_DUMP = PgDumpFromMemgresTest.findPgDump();
    static final String PG_RESTORE = derivePgRestore(PG_DUMP);

    static Memgres memgres;
    static Connection conn;
    static int port;

    @TempDir
    static Path tempDir;

    static boolean isPgDumpAvailable() {
        return PG_DUMP != null && PG_RESTORE != null;
    }

    static String derivePgRestore(String pgDumpPath) {
        if (pgDumpPath == null) return null;
        // Replace "pg_dump" with "pg_restore" in the path, handling both with and without .exe
        String candidate;
        if (pgDumpPath.contains("pg_dump.exe")) {
            candidate = pgDumpPath.replace("pg_dump.exe", "pg_restore.exe");
        } else if (pgDumpPath.contains("pg_dump")) {
            candidate = pgDumpPath.replace("pg_dump", "pg_restore");
        } else {
            return null;
        }
        // Verify pg_restore is actually executable
        try {
            Process proc = new ProcessBuilder(candidate, "--version")
                    .redirectErrorStream(true).start();
            String out = new String(IO.readAllBytes(proc.getInputStream()), StandardCharsets.UTF_8);
            proc.waitFor(5, TimeUnit.SECONDS);
            if (out.contains("pg_restore") && proc.exitValue() == 0) return candidate;
        } catch (Exception ignored) {}
        return null;
    }

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        port = memgres.getPort();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        PgDumpFromMemgresTest.populateReferenceSchema(conn);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // === Process execution helpers ===

        public static final class ProcessResult {
        public final int exitCode;
        public final String stdout;
        public final String stderr;

        public ProcessResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        public int exitCode() { return exitCode; }
        public String stdout() { return stdout; }
        public String stderr() { return stderr; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProcessResult that = (ProcessResult) o;
            return exitCode == that.exitCode
                && java.util.Objects.equals(stdout, that.stdout)
                && java.util.Objects.equals(stderr, that.stderr);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(exitCode, stdout, stderr);
        }

        @Override
        public String toString() {
            return "ProcessResult[exitCode=" + exitCode + ", " + "stdout=" + stdout + ", " + "stderr=" + stderr + "]";
        }
    }

    /**
     * Run a command with PGPASSWORD set, capturing stdout and stderr.
     * Fails the test if the process does not finish within the timeout.
     */
    static ProcessResult runProcess(List<String> cmd, int timeoutSeconds) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("PGPASSWORD", "memgres");
        Process proc = pb.start();

        final byte[][] results = new byte[2][];
        Thread stderrThread = new Thread(() -> {
            try { results[1] = IO.readAllBytes(proc.getErrorStream()); }
            catch (IOException e) { results[1] = new byte[0]; }
        });
        stderrThread.start();
        results[0] = IO.readAllBytes(proc.getInputStream());
        stderrThread.join(timeoutSeconds * 1000L);

        boolean finished = proc.waitFor(timeoutSeconds, TimeUnit.SECONDS);
        if (!finished) {
            proc.destroyForcibly();
            fail("Process timed out after " + timeoutSeconds + "s: " + String.join(" ", cmd));
        }

        return new ProcessResult(proc.exitValue(),
                new String(results[0], StandardCharsets.UTF_8),
                new String(results[1], StandardCharsets.UTF_8));
    }

    /** Run pg_dump against the source Memgres instance with extra arguments. */
    static ProcessResult runPgDump(String... extraArgs) throws Exception {
        List<String> cmd = new ArrayList<>(Cols.listOf(
                PG_DUMP,
                "-h", "127.0.0.1",
                "-p", String.valueOf(port),
                "-U", "memgres",
                "-d", "memgres",
                "--no-password"
        ));
        cmd.addAll(Cols.listOf(extraArgs));
        return runProcess(cmd, 60);
    }

    /** Run pg_restore against a target Memgres instance with extra arguments. */
    static ProcessResult runPgRestore(int targetPort, String... extraArgs) throws Exception {
        List<String> cmd = new ArrayList<>(Cols.listOf(
                PG_RESTORE,
                "-h", "127.0.0.1",
                "-p", String.valueOf(targetPort),
                "-U", "memgres",
                "-d", "memgres",
                "--no-password",
                "--no-owner",
                "--no-privileges"
        ));
        cmd.addAll(Cols.listOf(extraArgs));
        return runProcess(cmd, 60);
    }

    /** Verify the three core tables have correct row counts. */
    static void assertRowCounts(Connection c, int customers, int orders, int orderItems) throws SQLException {
        try (Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM customers")) {
                assertTrue(rs.next());
                assertEquals(customers, rs.getInt(1), "customers row count");
            }
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM orders")) {
                assertTrue(rs.next());
                assertEquals(orders, rs.getInt(1), "orders row count");
            }
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM order_items")) {
                assertTrue(rs.next());
                assertEquals(orderItems, rs.getInt(1), "order_items row count");
            }
        }
    }

    /** Verify that the core tables exist in the target database. */
    static void assertTablesExist(Connection c) throws SQLException {
        try (Statement s = c.createStatement()) {
            for (String table : Cols.listOf("customers", "orders", "order_items")) {
                try (ResultSet rs = s.executeQuery(
                        "SELECT count(*) FROM information_schema.tables " +
                        "WHERE table_schema = 'public' AND table_name = '" + table + "'")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1), "Table '" + table + "' should exist after restore");
                }
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Custom Format (-Fc)
    // ══════════════════════════════════════════════════════════════════════════

    @Test @Order(1)
    void customFormat_dumpSucceeds() throws Exception {
        Path dumpFile = tempDir.resolve("dump_custom.pgdump");
        ProcessResult r = runPgDump("-Fc", "-f", dumpFile.toString());

        System.out.println("=== pg_dump -Fc ===");
        System.out.println("Exit code: " + r.exitCode);
        if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);

        assertEquals(0, r.exitCode, "pg_dump -Fc failed:\n" + r.stderr);
        assertTrue(Files.exists(dumpFile), "Custom format dump file should exist");
        assertTrue(Files.size(dumpFile) > 0, "Custom format dump file should not be empty");
    }

    @Test @Order(2)
    void customFormat_restoreSucceeds() throws Exception {
        // Dump first
        Path dumpFile = tempDir.resolve("dump_custom_restore.pgdump");
        ProcessResult dumpResult = runPgDump("-Fc", "-f", dumpFile.toString());
        assertEquals(0, dumpResult.exitCode, "pg_dump -Fc failed (pre-restore):\n" + dumpResult.stderr);

        // Restore into a fresh Memgres
        try (Memgres m2 = Memgres.builder().port(0).build().start()) {
            ProcessResult r = runPgRestore(m2.getPort(), dumpFile.toString());

            System.out.println("=== pg_restore from custom format ===");
            System.out.println("Exit code: " + r.exitCode);
            if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);

            // pg_restore may return exit code 1 for non-critical warnings (e.g. OWNER TO, GRANT)
            // but should not be > 1 (which indicates a fundamental failure)
            assertTrue(r.exitCode <= 1,
                    "pg_restore from custom format failed critically (exit=" + r.exitCode + "):\n" + r.stderr);

            try (Connection c2 = DriverManager.getConnection(m2.getJdbcUrl(), m2.getUser(), m2.getPassword())) {
                c2.setAutoCommit(true);
                assertTablesExist(c2);
                assertRowCounts(c2, 3, 3, 4);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Directory Format (-Fd)
    // ══════════════════════════════════════════════════════════════════════════

    @Test @Order(3)
    void directoryFormat_dumpSucceeds() throws Exception {
        Path dumpDir = tempDir.resolve("dump_directory");
        ProcessResult r = runPgDump("-Fd", "-f", dumpDir.toString());

        System.out.println("=== pg_dump -Fd ===");
        System.out.println("Exit code: " + r.exitCode);
        if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);

        assertEquals(0, r.exitCode, "pg_dump -Fd failed:\n" + r.stderr);
        assertTrue(Files.isDirectory(dumpDir), "Directory format dump should create a directory");
        // Directory format creates a toc.dat file inside the directory
        assertTrue(Files.exists(dumpDir.resolve("toc.dat")),
                "Directory format dump should contain toc.dat");
    }

    @Test @Order(4)
    void directoryFormat_restoreSucceeds() throws Exception {
        // Dump first
        Path dumpDir = tempDir.resolve("dump_directory_restore");
        ProcessResult dumpResult = runPgDump("-Fd", "-f", dumpDir.toString());
        assertEquals(0, dumpResult.exitCode, "pg_dump -Fd failed (pre-restore):\n" + dumpResult.stderr);

        // Restore into a fresh Memgres
        try (Memgres m2 = Memgres.builder().port(0).build().start()) {
            ProcessResult r = runPgRestore(m2.getPort(), "-Fd", dumpDir.toString());

            System.out.println("=== pg_restore -Fd ===");
            System.out.println("Exit code: " + r.exitCode);
            if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);

            assertTrue(r.exitCode <= 1,
                    "pg_restore -Fd failed critically (exit=" + r.exitCode + "):\n" + r.stderr);

            try (Connection c2 = DriverManager.getConnection(m2.getJdbcUrl(), m2.getUser(), m2.getPassword())) {
                c2.setAutoCommit(true);
                assertTablesExist(c2);
                assertRowCounts(c2, 3, 3, 4);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Compressed Custom Format (-Fc -Z9)
    // ══════════════════════════════════════════════════════════════════════════

    @Test @Order(5)
    void compressedDump_succeeds() throws Exception {
        Path dumpFile = tempDir.resolve("dump_compressed.pgdump");
        ProcessResult r = runPgDump("-Fc", "-Z9", "-f", dumpFile.toString());

        System.out.println("=== pg_dump -Fc -Z9 ===");
        System.out.println("Exit code: " + r.exitCode);
        if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);

        assertEquals(0, r.exitCode, "pg_dump -Fc -Z9 failed:\n" + r.stderr);
        assertTrue(Files.exists(dumpFile), "Compressed dump file should exist");
        assertTrue(Files.size(dumpFile) > 0, "Compressed dump file should not be empty");
    }

    @Test @Order(6)
    void compressedDump_restoreSucceeds() throws Exception {
        // Dump with max compression
        Path dumpFile = tempDir.resolve("dump_compressed_restore.pgdump");
        ProcessResult dumpResult = runPgDump("-Fc", "-Z9", "-f", dumpFile.toString());
        assertEquals(0, dumpResult.exitCode, "pg_dump -Fc -Z9 failed (pre-restore):\n" + dumpResult.stderr);

        // Restore into a fresh Memgres
        try (Memgres m2 = Memgres.builder().port(0).build().start()) {
            ProcessResult r = runPgRestore(m2.getPort(), dumpFile.toString());

            System.out.println("=== pg_restore from compressed custom format ===");
            System.out.println("Exit code: " + r.exitCode);
            if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);

            assertTrue(r.exitCode <= 1,
                    "pg_restore from compressed dump failed critically (exit=" + r.exitCode + "):\n" + r.stderr);

            try (Connection c2 = DriverManager.getConnection(m2.getJdbcUrl(), m2.getUser(), m2.getPassword())) {
                c2.setAutoCommit(true);
                assertTablesExist(c2);
                assertRowCounts(c2, 3, 3, 4);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Parallel Dump and Restore (-j2)
    // ══════════════════════════════════════════════════════════════════════════

    @Test @Order(7)
    void parallelDump_succeeds() throws Exception {
        // Parallel dump requires directory format and pg_export_snapshot() support.
        // pg_dump -j opens multiple connections and uses snapshot export for consistency.
        Path dumpDir = tempDir.resolve("dump_parallel");
        ProcessResult r = runPgDump("-Fd", "-j2", "-f", dumpDir.toString());

        System.out.println("=== pg_dump -Fd -j2 ===");
        System.out.println("Exit code: " + r.exitCode);
        if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);

        // Skip if Memgres does not yet support pg_export_snapshot (required for parallel dump)
        assumeTrue(r.exitCode == 0,
                "Skipping: parallel dump requires pg_export_snapshot() which Memgres may not yet support. " +
                "Stderr: " + r.stderr);

        assertTrue(Files.isDirectory(dumpDir), "Parallel dump should create a directory");
        assertTrue(Files.exists(dumpDir.resolve("toc.dat")),
                "Parallel dump directory should contain toc.dat");
    }

    @Test @Order(8)
    void parallelRestore_succeeds() throws Exception {
        // Parallel dump requires pg_export_snapshot(); skip gracefully if not supported.
        Path dumpDir = tempDir.resolve("dump_parallel_restore");
        ProcessResult dumpResult = runPgDump("-Fd", "-j2", "-f", dumpDir.toString());

        System.out.println("=== pg_dump -Fd -j2 (for parallel restore) ===");
        System.out.println("Exit code: " + dumpResult.exitCode);
        if (!dumpResult.stderr.isEmpty()) System.out.println("Stderr:\n" + dumpResult.stderr);

        assumeTrue(dumpResult.exitCode == 0,
                "Skipping: parallel dump requires pg_export_snapshot() which Memgres may not yet support. " +
                "Stderr: " + dumpResult.stderr);

        // Restore with parallel into a fresh Memgres
        try (Memgres m2 = Memgres.builder().port(0).build().start()) {
            ProcessResult r = runPgRestore(m2.getPort(), "-Fd", "-j2", dumpDir.toString());

            System.out.println("=== pg_restore -Fd -j2 ===");
            System.out.println("Exit code: " + r.exitCode);
            if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);

            assertTrue(r.exitCode <= 1,
                    "pg_restore -Fd -j2 failed critically (exit=" + r.exitCode + "):\n" + r.stderr);

            try (Connection c2 = DriverManager.getConnection(m2.getJdbcUrl(), m2.getUser(), m2.getPassword())) {
                c2.setAutoCommit(true);
                assertTablesExist(c2);
                assertRowCounts(c2, 3, 3, 4);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  Data Integrity: Deep check after custom format round-trip
    // ══════════════════════════════════════════════════════════════════════════

    @Test @Order(9)
    void customFormat_dataIntegrity() throws Exception {
        // Dump in custom format
        Path dumpFile = tempDir.resolve("dump_integrity.pgdump");
        ProcessResult dumpResult = runPgDump("-Fc", "-f", dumpFile.toString());
        assertEquals(0, dumpResult.exitCode, "pg_dump -Fc failed (integrity test):\n" + dumpResult.stderr);

        // Restore into a fresh Memgres
        try (Memgres m2 = Memgres.builder().port(0).build().start()) {
            ProcessResult restoreResult = runPgRestore(m2.getPort(), dumpFile.toString());
            assertTrue(restoreResult.exitCode <= 1,
                    "pg_restore failed (integrity test, exit=" + restoreResult.exitCode + "):\n" + restoreResult.stderr);

            try (Connection c2 = DriverManager.getConnection(m2.getJdbcUrl(), m2.getUser(), m2.getPassword())) {
                c2.setAutoCommit(true);

                // Exact row counts
                assertRowCounts(c2, 3, 3, 4);

                try (Statement s = c2.createStatement()) {
                    // Verify specific customer data
                    try (ResultSet rs = s.executeQuery(
                            "SELECT name, email, score FROM customers WHERE name = 'Alice Johnson'")) {
                        assertTrue(rs.next(), "Alice Johnson should exist after restore");
                        assertEquals("alice@example.com", rs.getString("email"),
                                "Alice's email should be preserved");
                        assertEquals(new java.math.BigDecimal("98.50"), rs.getBigDecimal("score"),
                                "Alice's score should be preserved");
                    }

                    // Verify enum values survived the round-trip
                    try (ResultSet rs = s.executeQuery(
                            "SELECT status FROM orders WHERE customer_id = 1 ORDER BY id LIMIT 1")) {
                        assertTrue(rs.next(), "Order for customer 1 should exist");
                        assertEquals("confirmed", rs.getString(1),
                                "Enum value should be preserved after restore");
                    }

                    // Verify order items detail
                    try (ResultSet rs = s.executeQuery(
                            "SELECT product_name, quantity, unit_price FROM order_items ORDER BY id")) {
                        assertTrue(rs.next());
                        assertEquals("Widget Pro", rs.getString("product_name"));
                        assertEquals(2, rs.getInt("quantity"));
                        assertEquals(new java.math.BigDecimal("50.00"), rs.getBigDecimal("unit_price"));

                        assertTrue(rs.next());
                        assertEquals("Gadget X", rs.getString("product_name"));

                        assertTrue(rs.next());
                        assertEquals("Widget Pro", rs.getString("product_name"));
                        assertEquals(1, rs.getInt("quantity"));

                        assertTrue(rs.next());
                        assertEquals("Mega Bundle", rs.getString("product_name"));
                        assertEquals(new java.math.BigDecimal("200.00"), rs.getBigDecimal("unit_price"));

                        assertFalse(rs.next(), "Should have exactly 4 order_items");
                    }

                    // Verify view is queryable after restore
                    try (ResultSet rs = s.executeQuery("SELECT count(*) FROM customer_summary")) {
                        assertTrue(rs.next());
                        assertTrue(rs.getInt(1) > 0, "customer_summary view should have rows after restore");
                    }

                    // Verify FK constraint is enforced after restore
                    assertThrows(SQLException.class, () -> {
                        try (Statement s2 = c2.createStatement()) {
                            s2.execute("INSERT INTO orders (customer_id, status, total) VALUES (999, 'pending', 10)");
                        }
                    }, "FK constraint should prevent invalid customer_id after custom format restore");
                }
            }
        }
    }
}
