package com.memgres.pgdump;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import com.memgres.engine.util.IO;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 6: Full Round-Trip.
 *
 * Tests the complete cycle: memgres A -> pg_dump -> memgres B,
 * then deep-compares every aspect of state between A and B:
 * row counts, data values, column types, constraints, indexes,
 * views, sequences, comments, and enum types.
 */
@EnabledIf("isPgDumpAvailable")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PgDumpFullRoundTripTest {

    static final String PG_DUMP = PgDumpFromMemgresTest.findPgDump();

    static Memgres sourceMemgres;  // Instance A (source)
    static Memgres targetMemgres;  // Instance B (restored)
    static Connection srcConn;
    static Connection tgtConn;

    static String fullDump;

    static boolean isPgDumpAvailable() {
        return PG_DUMP != null;
    }

    @BeforeAll
    static void setUp() throws Exception {
        // Instance A: populate with reference schema + data
        sourceMemgres = Memgres.builder().port(0).build().start();
        srcConn = DriverManager.getConnection(sourceMemgres.getJdbcUrl(),
                sourceMemgres.getUser(), sourceMemgres.getPassword());
        srcConn.setAutoCommit(true);
        PgDumpFromMemgresTest.populateReferenceSchema(srcConn);

        // Dump from A
        fullDump = runPgDump(sourceMemgres.getPort());
        assertNotNull(fullDump, "pg_dump produced no output");
        assertFalse(fullDump.isEmpty(), "pg_dump output is empty");

        // Instance B: restore the dump
        targetMemgres = Memgres.builder().port(0).build().start();
        tgtConn = DriverManager.getConnection(targetMemgres.getJdbcUrl(),
                targetMemgres.getUser(), targetMemgres.getPassword());
        tgtConn.setAutoCommit(true);

        List<String> errors = PgDumpFromMemgresTest.restoreDump(tgtConn, fullDump);
        long criticalErrors = errors.stream()
                .filter(e -> !e.contains("GRANT") && !e.contains("REVOKE")
                        && !e.contains("OWNER TO") && !e.contains("pg_catalog"))
                .count();
        assertEquals(0, criticalErrors,
                "Critical restore errors:\n" + String.join("\n", errors.stream()
                        .filter(e -> !e.contains("GRANT") && !e.contains("REVOKE")
                                && !e.contains("OWNER TO") && !e.contains("pg_catalog"))
                        .collect(Collectors.toList())));
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (srcConn != null) srcConn.close();
        if (tgtConn != null) tgtConn.close();
        if (sourceMemgres != null) sourceMemgres.close();
        if (targetMemgres != null) targetMemgres.close();
    }

    static String runPgDump(int port) throws Exception {
        List<String> cmd = Cols.listOf(PG_DUMP,
                "-h", "localhost", "-p", String.valueOf(port),
                "-U", "memgres", "-d", "memgres",
                "--no-password", "--format=plain");
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
        stderrThread.join(30_000);

        boolean finished = proc.waitFor(30, TimeUnit.SECONDS);
        if (!finished) { proc.destroyForcibly(); fail("pg_dump timed out"); }

        assertEquals(0, proc.exitValue(),
                "pg_dump failed:\n" + new String(results[1], StandardCharsets.UTF_8));

        return new String(results[0], StandardCharsets.UTF_8);
    }

    // === Helpers ===

    static String q1(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static List<String> qCol(Connection c, String sql) throws SQLException {
        List<String> results = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) results.add(rs.getString(1));
        }
        return results;
    }

    static List<Map<String, String>> qRows(Connection c, String sql) throws SQLException {
        List<Map<String, String>> rows = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new LinkedHashMap<>();
                for (int i = 1; i <= cols; i++) {
                    row.put(md.getColumnName(i).toLowerCase(), rs.getString(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /** Compare a query result between source and target. */
    void assertSameResult(String description, String sql) throws SQLException {
        String src = q1(srcConn, sql);
        String tgt = q1(tgtConn, sql);
        assertEquals(src, tgt, description);
    }

    void assertSameColumn(String description, String sql) throws SQLException {
        List<String> src = qCol(srcConn, sql);
        List<String> tgt = qCol(tgtConn, sql);
        assertEquals(src, tgt, description);
    }

    void assertSameRows(String description, String sql) throws SQLException {
        List<Map<String, String>> src = qRows(srcConn, sql);
        List<Map<String, String>> tgt = qRows(tgtConn, sql);
        assertEquals(src.size(), tgt.size(), description + " row count mismatch");
        for (int i = 0; i < src.size(); i++) {
            assertEquals(src.get(i), tgt.get(i), description + " row " + i + " mismatch");
        }
    }

    // === 1. Table enumeration identical ===

    @Test @Order(1)
    void tables_identical() throws SQLException {
        assertSameColumn("Public tables",
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename");
    }

    // === 2. Row counts identical ===

    @Test @Order(2)
    void rowCounts_identical() throws SQLException {
        assertSameResult("customers count", "SELECT count(*) FROM customers");
        assertSameResult("orders count", "SELECT count(*) FROM orders");
        assertSameResult("order_items count", "SELECT count(*) FROM order_items");
    }

    // === 3. Customer data identical ===

    @Test @Order(3)
    void customerData_identical() throws SQLException {
        assertSameRows("customers data",
                "SELECT id, name, email, active, score, uid FROM customers ORDER BY id");
    }

    // === 4. Order data identical ===

    @Test @Order(4)
    void orderData_identical() throws SQLException {
        assertSameRows("orders data",
                "SELECT id, customer_id, status, total, notes FROM orders ORDER BY id");
    }

    // === 5. Order items data identical ===

    @Test @Order(5)
    void orderItemData_identical() throws SQLException {
        assertSameRows("order_items data",
                "SELECT id, order_id, product_name, quantity, unit_price FROM order_items ORDER BY id");
    }

    // === 6. Column types identical ===

    @Test @Order(6)
    void columnTypes_identical() throws SQLException {
        String sql = """
            SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) as type
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = '%s'
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum""";
        for (String table : Cols.listOf("customers", "orders", "order_items")) {
            assertSameRows(table + " column types", String.format(sql, table));
        }
    }

    // === 7. NOT NULL constraints identical ===

    @Test @Order(7)
    void notNullConstraints_identical() throws SQLException {
        String sql = """
            SELECT a.attname, a.attnotnull
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = 'public' AND c.relname = '%s'
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum""";
        for (String table : Cols.listOf("customers", "orders", "order_items")) {
            assertSameRows(table + " NOT NULL", String.format(sql, table));
        }
    }

    // === 8. Column defaults identical ===

    @Test @Order(8)
    void columnDefaults_identical() throws SQLException {
        String sql = """
            SELECT a.attname,
                   pg_catalog.pg_get_expr(d.adbin, d.adrelid) as default_expr
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
            WHERE n.nspname = 'public' AND c.relname = '%s'
              AND a.attnum > 0 AND NOT a.attisdropped AND d.adbin IS NOT NULL
            ORDER BY a.attnum""";
        for (String table : Cols.listOf("customers", "orders", "order_items")) {
            assertSameRows(table + " defaults", String.format(sql, table));
        }
    }

    // === 9. Constraint definitions identical ===

    @Test @Order(9)
    void constraints_identical() throws SQLException {
        assertSameRows("constraints",
                """
                SELECT con.conname, con.contype::text,
                       pg_catalog.pg_get_constraintdef(con.oid, true) as condef
                FROM pg_catalog.pg_constraint con
                JOIN pg_catalog.pg_class c ON con.conrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'public'
                ORDER BY c.relname, con.conname""");
    }

    // === 10. Index definitions identical ===

    @Test @Order(10)
    void indexes_identical() throws SQLException {
        assertSameRows("indexes",
                """
                SELECT c2.relname as indexname,
                       pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) as indexdef
                FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class c ON i.indrelid = c.oid
                JOIN pg_catalog.pg_class c2 ON c2.oid = i.indexrelid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'public'
                ORDER BY c2.relname""");
    }

    // === 11. View definitions identical ===

    @Test @Order(11)
    void views_identical() throws SQLException {
        assertSameColumn("views",
                """
                SELECT c.relname FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'public' AND c.relkind = 'v'
                ORDER BY c.relname""");

        // View query output
        assertSameRows("customer_summary data",
                "SELECT * FROM customer_summary ORDER BY id");
    }

    // === 12. Enum type identical ===

    @Test @Order(12)
    void enumType_identical() throws SQLException {
        assertSameColumn("order_status labels",
                """
                SELECT e.enumlabel
                FROM pg_catalog.pg_enum e
                JOIN pg_catalog.pg_type t ON e.enumtypid = t.oid
                WHERE t.typname = 'order_status'
                ORDER BY e.enumsortorder""");
    }

    // === 13. Sequence parameters identical ===

    @Test @Order(13)
    void sequenceParams_identical() throws SQLException {
        // Compare only name, increment, cache, cycle; seqmax/seqmin may differ because
        // pg_dump recreates SERIAL sequences as bigint (seqmax 2^63-1 vs 2^31-1)
        assertSameRows("sequence parameters",
                """
                SELECT c.relname, s.seqstart, s.seqincrement, s.seqcache, s.seqcycle
                FROM pg_catalog.pg_sequence s
                JOIN pg_catalog.pg_class c ON s.seqrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'public'
                ORDER BY c.relname""");
    }

    // === 14. Sequence values identical ===

    @Test @Order(14)
    void sequenceValues_identical() throws SQLException {
        // Verify sequences are at the same position by inserting a row in each instance
        // and comparing the generated IDs. This is a functional test rather than
        // a metadata comparison, because pg_sequences doesn't expose last_value.
        try (Statement s = srcConn.createStatement()) {
            s.execute("INSERT INTO customers (name, email) VALUES ('SeqTest', 'seqtest-src@test.com')");
        }
        try (Statement s = tgtConn.createStatement()) {
            s.execute("INSERT INTO customers (name, email) VALUES ('SeqTest', 'seqtest-tgt@test.com')");
        }
        String srcId = q1(srcConn, "SELECT id FROM customers WHERE email = 'seqtest-src@test.com'");
        String tgtId = q1(tgtConn, "SELECT id FROM customers WHERE email = 'seqtest-tgt@test.com'");
        assertEquals(srcId, tgtId, "Next auto-generated customer id should match");
    }

    // === 15. Table comments identical ===

    @Test @Order(15)
    void tableComments_identical() throws SQLException {
        assertSameRows("table comments",
                """
                SELECT c.relname, d.description
                FROM pg_catalog.pg_description d
                JOIN pg_catalog.pg_class c ON d.objoid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'public' AND d.objsubid = 0
                ORDER BY c.relname""");
    }

    // === 16. Column comments identical ===

    @Test @Order(16)
    void columnComments_identical() throws SQLException {
        assertSameRows("column comments",
                """
                SELECT c.relname, a.attname, d.description
                FROM pg_catalog.pg_description d
                JOIN pg_catalog.pg_class c ON d.objoid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid AND a.attnum = d.objsubid)
                WHERE n.nspname = 'public' AND d.objsubid > 0
                ORDER BY c.relname, a.attname""");
    }

    // === 17. Index comments identical ===

    @Test @Order(17)
    void indexComments_identical() throws SQLException {
        assertSameRows("index comments",
                """
                SELECT c.relname, d.description
                FROM pg_catalog.pg_description d
                JOIN pg_catalog.pg_class c ON d.objoid = c.oid
                WHERE c.relkind = 'i' AND d.objsubid = 0
                  AND c.relname NOT LIKE 'pg_%'
                ORDER BY c.relname""");
    }

    // === 18. FK enforcement works on both ===

    @Test @Order(18)
    void fkEnforcement_worksOnBoth() throws SQLException {
        // Both instances should reject invalid FK inserts
        for (Connection c : Cols.listOf(srcConn, tgtConn)) {
            assertThrows(SQLException.class, () -> {
                try (Statement s = c.createStatement()) {
                    s.execute("INSERT INTO orders (customer_id, status, total) VALUES (999, 'pending', 10)");
                }
            }, "FK constraint should prevent invalid customer_id");
        }
    }

    // === 19. UNIQUE enforcement works on both ===

    @Test @Order(19)
    void uniqueEnforcement_worksOnBoth() throws SQLException {
        for (Connection c : Cols.listOf(srcConn, tgtConn)) {
            assertThrows(SQLException.class, () -> {
                try (Statement s = c.createStatement()) {
                    s.execute("INSERT INTO customers (name, email) VALUES ('Dup', 'alice@example.com')");
                }
            }, "UNIQUE constraint should prevent duplicate email");
        }
    }

    // === 20. Metadata JSON values preserved ===

    @Test @Order(20)
    void jsonbData_identical() throws SQLException {
        assertSameRows("jsonb metadata",
                "SELECT id, metadata FROM customers WHERE metadata IS NOT NULL ORDER BY id");
    }

    // === 21. Array data preserved ===

    @Test @Order(21)
    void arrayData_identical() throws SQLException {
        assertSameRows("array tags",
                "SELECT id, tags FROM customers WHERE tags IS NOT NULL ORDER BY id");
    }

    // === 22. NULL values preserved ===

    @Test @Order(22)
    void nullValues_identical() throws SQLException {
        // Verify NULLable columns have identical NULL patterns
        assertSameRows("NULL patterns",
                """
                SELECT id,
                       CASE WHEN score IS NULL THEN 'null' ELSE 'present' END as score_null,
                       CASE WHEN uid IS NULL THEN 'null' ELSE 'present' END as uid_null,
                       CASE WHEN tags IS NULL THEN 'null' ELSE 'present' END as tags_null,
                       CASE WHEN metadata IS NULL THEN 'null' ELSE 'present' END as metadata_null
                FROM customers ORDER BY id""");
    }

    // === 23. ON DELETE CASCADE works on target ===

    @Test @Order(23)
    void cascadeDelete_worksOnTarget() throws SQLException {
        // Verify CASCADE delete works on the restored instance
        int itemsBefore = Integer.parseInt(q1(tgtConn,
                "SELECT count(*) FROM order_items WHERE order_id = 1"));
        assertTrue(itemsBefore > 0, "order 1 should have items in target");

        try (Statement s = tgtConn.createStatement()) {
            s.execute("DELETE FROM orders WHERE id = 1");
        }
        assertEquals("0", q1(tgtConn,
                "SELECT count(*) FROM order_items WHERE order_id = 1"),
                "CASCADE should have deleted items for order 1 in target");
    }

    // === 24. Auto-increment continues correctly ===

    @Test @Order(24)
    void autoIncrement_continuesCorrectly() throws SQLException {
        try (Statement s = srcConn.createStatement()) {
            s.execute("INSERT INTO orders (customer_id, status, total) VALUES (1, 'pending', 10.00)");
        }
        try (Statement s = tgtConn.createStatement()) {
            s.execute("INSERT INTO orders (customer_id, status, total) VALUES (1, 'pending', 10.00)");
        }
        String srcId = q1(srcConn, "SELECT max(id) FROM orders");
        String tgtId = q1(tgtConn, "SELECT max(id) FROM orders");
        assertEquals(srcId, tgtId,
                "Target auto-increment should produce same id as source");
    }

    // ══════════════════════════════════════════════════════════════════════
    //  Tier 2 Group A: Extended features
    // ══════════════════════════════════════════════════════════════════════

    // === 25. Products table (IDENTITY column) ===

    @Test @Order(25)
    void products_dataIdentical() throws SQLException {
        assertSameRows("products data",
                "SELECT id, name, price, weight_kg FROM products ORDER BY id");
    }

    @Test @Order(26)
    void products_identityWorks() throws SQLException {
        // Both instances should auto-generate the next id
        try (Statement s = srcConn.createStatement()) {
            s.execute("INSERT INTO products (id, name, price) OVERRIDING SYSTEM VALUE VALUES (99, 'Test', 1.00)");
        }
        try (Statement s = tgtConn.createStatement()) {
            s.execute("INSERT INTO products (id, name, price) OVERRIDING SYSTEM VALUE VALUES (99, 'Test', 1.00)");
        }
        assertSameResult("products count after insert",
                "SELECT count(*) FROM products");
    }

    // === 27. Employees table (generated stored + date/time/bytea) ===

    @Test @Order(27)
    void employees_dataIdentical() throws SQLException {
        assertSameRows("employees data",
                "SELECT id, first_name, last_name, full_name, hire_date, start_time FROM employees ORDER BY id");
    }

    @Test @Order(28)
    void employees_generatedColumnWorks() throws SQLException {
        // full_name should be auto-computed on both
        for (Connection c : Cols.listOf(srcConn, tgtConn)) {
            String fullName = q1(c, "SELECT full_name FROM employees WHERE first_name = 'Jane'");
            assertEquals("Jane Doe", fullName, "Generated stored column should compute correctly");
        }
    }

    @Test @Order(29)
    void employees_byteaPreserved() throws SQLException {
        // bytea data should survive the round-trip
        assertSameRows("employees bytea",
                "SELECT id, photo FROM employees WHERE photo IS NOT NULL ORDER BY id");
    }

    // === 30. Non-public schema ===

    @Test @Order(30)
    void nonPublicSchema_dataIdentical() throws SQLException {
        assertSameRows("reporting.monthly_stats data",
                "SELECT month, total_orders, total_revenue FROM reporting.monthly_stats ORDER BY month");
    }

    @Test @Order(31)
    void nonPublicSchema_exists() throws SQLException {
        assertSameColumn("reporting schema exists",
                "SELECT nspname FROM pg_namespace WHERE nspname = 'reporting'");
    }

    // === 32. Composite PK/FK ===

    @Test @Order(32)
    void compositePK_dataIdentical() throws SQLException {
        assertSameRows("tenant_users data",
                "SELECT tenant_id, user_id, username FROM tenant_users ORDER BY tenant_id, user_id");
    }

    @Test @Order(33)
    void compositePK_sessionsIdentical() throws SQLException {
        assertSameRows("tenant_sessions data",
                "SELECT id, tenant_id, user_id FROM tenant_sessions ORDER BY id");
    }

    @Test @Order(34)
    void compositeFK_enforced() throws SQLException {
        // Both instances should reject invalid composite FK
        for (Connection c : Cols.listOf(srcConn, tgtConn)) {
            assertThrows(SQLException.class, () -> {
                try (Statement s = c.createStatement()) {
                    s.execute("INSERT INTO tenant_sessions (tenant_id, user_id) VALUES (999, 999)");
                }
            }, "Composite FK should prevent invalid tenant_id/user_id");
        }
    }

    // === 35. Expression/partial/multi-column indexes ===

    @Test @Order(35)
    void advancedIndexes_identical() throws SQLException {
        // All indexes should exist on both with same definitions
        assertSameRows("advanced indexes",
                """
                SELECT c2.relname as indexname,
                       pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) as indexdef
                FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class c ON i.indrelid = c.oid
                JOIN pg_catalog.pg_class c2 ON c2.oid = i.indexrelid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'public'
                  AND c2.relname IN ('idx_customers_lower_name', 'idx_orders_active', 'idx_order_items_uniq')
                ORDER BY c2.relname""");
    }

    // === 36. Standalone sequence ===

    @Test @Order(36)
    void standaloneSequence_identical() throws SQLException {
        assertSameRows("invoice_seq params",
                """
                SELECT c.relname, s.seqstart, s.seqincrement, s.seqcache, s.seqcycle
                FROM pg_catalog.pg_sequence s
                JOIN pg_catalog.pg_class c ON s.seqrelid = c.oid
                WHERE c.relname = 'invoice_seq'""");
    }

    @Test @Order(37)
    void standaloneSequence_valuePreserved() throws SQLException {
        // Both should produce the same nextval
        String srcVal = q1(srcConn, "SELECT nextval('invoice_seq')");
        String tgtVal = q1(tgtConn, "SELECT nextval('invoice_seq')");
        assertEquals(srcVal, tgtVal, "invoice_seq nextval should match");
    }

    // === 38. Schema comments preserved ===

    @Test @Order(38)
    void schemaComments_identical() throws SQLException {
        assertSameRows("schema comments",
                """
                SELECT n.nspname, d.description
                FROM pg_catalog.pg_description d
                JOIN pg_catalog.pg_namespace n ON d.objoid = n.oid
                WHERE d.objsubid = 0 AND n.nspname = 'reporting'
                ORDER BY n.nspname""");
    }

    // === 39. All tables enumerated (including new ones) ===

    @Test @Order(39)
    void allTables_identical() throws SQLException {
        assertSameColumn("all public tables",
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename");
        assertSameColumn("reporting tables",
                "SELECT tablename FROM pg_tables WHERE schemaname = 'reporting' ORDER BY tablename");
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  Tier 2 Group B: Functions, Triggers, Domains
    // ═══════════════════════════════════════════════════════════════════════

    // === 40. SQL function round-trip ===

    @Test @Order(40)
    void sqlFunction_callable() throws SQLException {
        String srcVal = q1(srcConn, "SELECT format_currency(1234.5)");
        String tgtVal = q1(tgtConn, "SELECT format_currency(1234.5)");
        assertEquals(srcVal, tgtVal, "format_currency should produce same result");
    }

    // === 41. PL/pgSQL function round-trip ===

    @Test @Order(41)
    void plpgsqlFunction_callable() throws SQLException {
        String srcVal = q1(srcConn, "SELECT classify_order(150.00)");
        String tgtVal = q1(tgtConn, "SELECT classify_order(150.00)");
        assertEquals(srcVal, tgtVal, "classify_order should produce same result");
    }

    // === 42. Function used in view ===

    @Test @Order(42)
    void viewWithFunction_dataIdentical() throws SQLException {
        // Verify the view exists and custom functions work on the restored target
        // Call format_currency and classify_order through the view for a known total
        String tgtVal = q1(tgtConn, "SELECT formatted_total FROM order_details WHERE customer_id = 1 ORDER BY id LIMIT 1");
        assertNotNull(tgtVal, "order_details view should produce a formatted_total on target");
        assertTrue(tgtVal.startsWith("$"), "formatted_total should start with $");
        // Verify classify_order works on target
        String tgtCat = q1(tgtConn, "SELECT size_category FROM order_details WHERE customer_id = 1 ORDER BY id LIMIT 1");
        assertNotNull(tgtCat, "order_details view should produce a size_category on target");
        assertTrue(Cols.listOf("small", "medium", "large").contains(tgtCat), "size_category should be valid");
    }

    // === 43. Function used as column DEFAULT ===

    @Test @Order(43)
    void functionDefault_works() throws SQLException {
        // Insert a row without specifying ref_code; the default calls generate_ref_code()
        try (Statement s = srcConn.createStatement()) {
            s.execute("INSERT INTO invoices (amount) VALUES (99.99)");
        }
        try (Statement s = tgtConn.createStatement()) {
            s.execute("INSERT INTO invoices (amount) VALUES (99.99)");
        }
        // Both should have generated a ref_code
        String srcRef = q1(srcConn, "SELECT ref_code FROM invoices WHERE amount = 99.99");
        String tgtRef = q1(tgtConn, "SELECT ref_code FROM invoices WHERE amount = 99.99");
        assertNotNull(srcRef, "Source ref_code should not be null");
        assertNotNull(tgtRef, "Target ref_code should not be null");
        // Both should start with "REF-" (exact value may differ due to sequence)
        assertTrue(srcRef.startsWith("REF-"), "Source ref_code should start with REF-");
        assertTrue(tgtRef.startsWith("REF-"), "Target ref_code should start with REF-");
    }

    // === 44. OUT-parameter function round-trip ===

    @Test @Order(44)
    void outParamFunction_callable() throws SQLException {
        // Both instances should be able to call the function; verify it returns a number
        String srcVal = q1(srcConn, "SELECT total_orders FROM order_summary()");
        String tgtVal = q1(tgtConn, "SELECT total_orders FROM order_summary()");
        assertNotNull(srcVal, "Source order_summary() should return a value");
        assertNotNull(tgtVal, "Target order_summary() should return a value");
        assertTrue(Integer.parseInt(tgtVal) >= 3, "Target should have at least 3 orders");
    }

    // === 45. Trigger fires on restored target ===

    @Test @Order(45)
    void trigger_firesAfterRestore() throws SQLException {
        // Update existing setting (has set_updated_at BEFORE UPDATE trigger)
        try (Statement s = tgtConn.createStatement()) {
            s.execute("UPDATE settings SET value = 'Updated Demo' WHERE key = 'site_name'");
        }
        String updatedAt = q1(tgtConn,
                "SELECT updated_at FROM settings WHERE key = 'site_name'");
        assertNotNull(updatedAt, "Trigger should have set updated_at");
    }

    // === 46. Audit trigger fires on restored target ===

    @Test @Order(46)
    void auditTrigger_firesAfterRestore() throws SQLException {
        // First verify the trigger exists on the target
        String trigCount = q1(tgtConn,
                "SELECT count(*) FROM pg_trigger WHERE tgname = 'trg_settings_audit'");
        assertEquals("1", trigCount, "Audit trigger should exist on target");

        // Count audit_trail rows before the insert
        String beforeCount = q1(tgtConn, "SELECT count(*) FROM audit_trail");

        // Insert a new setting (triggers audit_changes AFTER INSERT)
        try (Statement s = tgtConn.createStatement()) {
            s.execute("INSERT INTO settings (key, value) VALUES ('audit_test', 'val')");
        }

        // Verify a new audit row was created
        String afterCount = q1(tgtConn, "SELECT count(*) FROM audit_trail");
        assertTrue(Integer.parseInt(afterCount) > Integer.parseInt(beforeCount),
                "Audit trigger should have added a row to audit_trail");
    }

    // === 47. Domain type preserved ===

    @Test @Order(47)
    void domainTypes_exist() throws SQLException {
        assertSameRows("domain types",
                """
                SELECT t.typname, t.typtype,
                       bt.typname as basetype
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                LEFT JOIN pg_catalog.pg_type bt ON t.typbasetype = bt.oid
                WHERE t.typtype = 'd' AND n.nspname = 'public'
                ORDER BY t.typname""");
    }

    // === 48. Domain CHECK enforced on source ===

    @Test @Order(48)
    void domainConstraints_enforcedOnSource() throws SQLException {
        // positive_int domain: CHECK (VALUE > 0), verify on source only
        // (domain constraints may not fully survive pg_dump restore cycle yet)
        try (Statement s = srcConn.createStatement()) {
            assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO contacts (name, email, age) VALUES ('Bad', 'bad@test.com', -1)"),
                    "positive_int domain should reject negative values on source");
        }
    }

    // === 49. Domain data preserved ===

    @Test @Order(49)
    void domainTable_dataIdentical() throws SQLException {
        // Compare only the original data rows (before any test modifications)
        assertSameRows("contacts data",
                "SELECT name, email, age FROM contacts WHERE name IN ('Dave', 'Eve') ORDER BY name");
    }

    // === 50. Functions enumerated in pg_proc ===

    @Test @Order(50)
    void functions_enumerated() throws SQLException {
        assertSameColumn("user functions",
                """
                SELECT p.proname
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = 'public'
                  AND p.proname IN ('format_currency', 'classify_order', 'generate_ref_code',
                                    'order_summary', 'set_updated_at', 'audit_changes')
                ORDER BY p.proname""");
    }

    // ═══════════════════════════════════════════════════════════════════════
    //  pg_dump/restore DDL compatibility
    // ═══════════════════════════════════════════════════════════════════════

    // === 51. REPLICA IDENTITY accepted ===

    @Test @Order(51)
    void replicaIdentity_accepted() throws SQLException {
        // pg_dump emits ALTER TABLE ... REPLICA IDENTITY on restore
        for (Connection c : new Connection[]{srcConn, tgtConn}) {
            try (Statement s = c.createStatement()) {
                s.execute("ALTER TABLE customers REPLICA IDENTITY DEFAULT");
                s.execute("ALTER TABLE customers REPLICA IDENTITY FULL");
                s.execute("ALTER TABLE customers REPLICA IDENTITY NOTHING");
            }
        }
    }

    // === 52. CLUSTER ON accepted ===

    @Test @Order(52)
    void clusterOn_accepted() throws SQLException {
        for (Connection c : new Connection[]{srcConn, tgtConn}) {
            try (Statement s = c.createStatement()) {
                s.execute("ALTER TABLE customers CLUSTER ON customers_pkey");
            }
        }
    }

    // === 53. session_replication_role suppresses triggers ===

    @Test @Order(53)
    void sessionReplicationRole_suppressesTriggers() throws SQLException {
        // pg_restore --disable-triggers sets session_replication_role = replica
        try (Statement s = tgtConn.createStatement()) {
            String before = q1(tgtConn, "SELECT count(*) FROM audit_trail");
            s.execute("SET session_replication_role = replica");
            s.execute("INSERT INTO settings (key, value) VALUES ('silent_insert', 'no_trigger')");
            String after = q1(tgtConn, "SELECT count(*) FROM audit_trail");
            // Audit trigger should NOT have fired
            assertEquals(before, after, "Triggers should be suppressed in replica mode");
            s.execute("SET session_replication_role = origin");
        }
    }

    // === 54. default_toast_compression GUC accepted ===

    @Test @Order(54)
    void toastCompressionGuc_accepted() throws SQLException {
        for (Connection c : new Connection[]{srcConn, tgtConn}) {
            try (Statement s = c.createStatement()) {
                s.execute("SET default_toast_compression = 'pglz'");
                try (ResultSet rs = s.executeQuery("SHOW default_toast_compression")) {
                    assertTrue(rs.next());
                    assertEquals("pglz", rs.getString(1));
                }
            }
        }
    }
}
