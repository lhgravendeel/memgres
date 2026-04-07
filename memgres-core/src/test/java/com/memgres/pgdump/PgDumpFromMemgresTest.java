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
 * Phase 5: pg_dump Output Generation.
 *
 * Runs the real pg_dump 18 binary against a Memgres instance and verifies:
 * 1. pg_dump completes without errors
 * 2. Output is valid SQL
 * 3. Output restores into a fresh Memgres instance
 * 4. Restored data matches the original
 */
@EnabledIf("isPgDumpAvailable")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PgDumpFromMemgresTest {

    static final String PG_DUMP = findPgDump();

    static Memgres memgres;
    static Connection conn;
    static int port;

    // pg_dump output cached across tests
    static String schemaOnlyDump;
    static String dataOnlyDump;
    static String fullDump;

    static boolean isPgDumpAvailable() {
        return PG_DUMP != null;
    }

    static final int MIN_PG_DUMP_MAJOR = 18;

    static String findPgDump() {
        // Try common locations
        String[] paths = {
            "pg_dump",
            "C:\\Program Files\\PostgreSQL\\18\\bin\\pg_dump.exe",
            "C:\\Program Files\\PostgreSQL\\17\\bin\\pg_dump.exe",
            "/usr/bin/pg_dump",
            "/usr/local/bin/pg_dump"
        };
        for (String p : paths) {
            try {
                Process proc = new ProcessBuilder(p, "--version")
                        .redirectErrorStream(true).start();
                String out = new String(IO.readAllBytes(proc.getInputStream()), StandardCharsets.UTF_8);
                proc.waitFor(5, TimeUnit.SECONDS);
                if (out.contains("pg_dump") && proc.exitValue() == 0 && pgDumpMajorVersion(out) >= MIN_PG_DUMP_MAJOR) return p;
            } catch (Exception ignored) {}
        }
        return null;
    }

    static int pgDumpMajorVersion(String versionOutput) {
        // Parses "pg_dump (PostgreSQL) 18.0" or "pg_dump (PostgreSQL) 16.13"
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(\\d+)\\.").matcher(versionOutput);
        return m.find() ? Integer.parseInt(m.group(1)) : 0;
    }

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        port = memgres.getPort();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        // Populate with the reference schema + data
        populateReferenceSchema(conn);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static void populateReferenceSchema(Connection c) throws Exception {
        try (Statement s = c.createStatement()) {
            // Enum type
            s.execute("CREATE TYPE order_status AS ENUM ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')");

            // Tables
            s.execute("""
                CREATE TABLE customers (
                    id serial PRIMARY KEY,
                    name text NOT NULL,
                    email varchar(255) NOT NULL UNIQUE,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    metadata jsonb DEFAULT '{}',
                    tags text[],
                    active boolean DEFAULT true,
                    score numeric(5,2),
                    uid uuid
                )""");
            s.execute("COMMENT ON TABLE customers IS 'Core customer records'");
            s.execute("COMMENT ON COLUMN customers.metadata IS 'Arbitrary JSON metadata for extensibility'");

            s.execute("""
                CREATE TABLE orders (
                    id bigserial PRIMARY KEY,
                    customer_id integer NOT NULL REFERENCES customers(id) ON DELETE RESTRICT,
                    status order_status NOT NULL DEFAULT 'pending',
                    total numeric(10,2) NOT NULL,
                    notes text,
                    placed_at timestamptz NOT NULL DEFAULT now(),
                    shipped_at timestamptz
                )""");
            s.execute("COMMENT ON TABLE orders IS 'Customer orders with status tracking'");

            s.execute("""
                CREATE TABLE order_items (
                    id serial PRIMARY KEY,
                    order_id bigint NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
                    product_name text NOT NULL,
                    quantity integer NOT NULL DEFAULT 1,
                    unit_price numeric(10,2) NOT NULL
                )""");

            // Indexes
            s.execute("CREATE INDEX idx_customers_email ON customers (email)");
            s.execute("CREATE INDEX idx_orders_customer_id ON orders (customer_id)");
            s.execute("CREATE INDEX idx_orders_status ON orders (status)");
            s.execute("COMMENT ON INDEX idx_customers_email IS 'Fast email lookups'");

            // View
            s.execute("""
                CREATE VIEW customer_summary AS
                SELECT c.id, c.name, c.email,
                       count(o.id) AS order_count,
                       COALESCE(sum(o.total), 0) AS total_spent
                FROM customers c
                LEFT JOIN orders o ON o.customer_id = c.id
                GROUP BY c.id, c.name, c.email""");

            // Data
            s.execute("INSERT INTO customers (name, email, metadata, tags, active, score, uid) VALUES " +
                    "('Alice Johnson', 'alice@example.com', '{\"tier\": \"gold\", \"since\": 2020}', '{loyal,vip}', true, 98.50, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
            s.execute("INSERT INTO customers (name, email, metadata, tags, active, score, uid) VALUES " +
                    "('Bob Smith', 'bob@example.com', '{\"tier\": \"silver\"}', '{new}', true, 75.25, 'b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22')");
            s.execute("INSERT INTO customers (name, email, active) VALUES ('Carol White', 'carol@example.com', false)");

            s.execute("INSERT INTO orders (customer_id, status, total, notes) VALUES (1, 'confirmed', 150.00, 'Rush order')");
            s.execute("INSERT INTO orders (customer_id, status, total) VALUES (1, 'shipped', 75.50)");
            s.execute("INSERT INTO orders (customer_id, status, total, notes) VALUES (2, 'pending', 200.00, 'Gift wrap please')");

            s.execute("INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES (1, 'Widget Pro', 2, 50.00)");
            s.execute("INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES (1, 'Gadget X', 1, 50.00)");
            s.execute("INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES (2, 'Widget Pro', 1, 50.00)");
            s.execute("INSERT INTO order_items (order_id, product_name, quantity, unit_price) VALUES (3, 'Mega Bundle', 1, 200.00)");

            // === Tier 2 Group A: Identity, generated stored, extra types ===

            // GENERATED ALWAYS AS IDENTITY
            s.execute("""
                CREATE TABLE products (
                    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    name text NOT NULL,
                    price numeric(10,2) NOT NULL,
                    weight_kg numeric(6,3)
                )""");

            // Generated stored column + extra types (date, time, bytea)
            s.execute("""
                CREATE TABLE employees (
                    id serial PRIMARY KEY,
                    first_name text NOT NULL,
                    last_name text NOT NULL,
                    full_name text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
                    hire_date date NOT NULL DEFAULT CURRENT_DATE,
                    start_time time,
                    photo bytea
                )""");

            // Non-public schema
            s.execute("CREATE SCHEMA reporting");
            s.execute("COMMENT ON SCHEMA reporting IS 'Aggregated reporting tables'");
            s.execute("""
                CREATE TABLE reporting.monthly_stats (
                    month date NOT NULL,
                    total_orders integer NOT NULL,
                    total_revenue numeric(12,2) NOT NULL,
                    PRIMARY KEY (month)
                )""");

            // Composite PK/FK
            s.execute("""
                CREATE TABLE tenant_users (
                    tenant_id integer NOT NULL,
                    user_id integer NOT NULL,
                    username text NOT NULL,
                    PRIMARY KEY (tenant_id, user_id)
                )""");
            s.execute("""
                CREATE TABLE tenant_sessions (
                    id serial PRIMARY KEY,
                    tenant_id integer NOT NULL,
                    user_id integer NOT NULL,
                    started_at timestamptz DEFAULT now(),
                    FOREIGN KEY (tenant_id, user_id) REFERENCES tenant_users(tenant_id, user_id) ON DELETE CASCADE
                )""");

            // Expression index, partial index, multi-column unique index
            s.execute("CREATE INDEX idx_customers_lower_name ON customers (lower(name))");
            s.execute("CREATE INDEX idx_orders_active ON orders (placed_at) WHERE status <> 'cancelled'");
            s.execute("CREATE UNIQUE INDEX idx_order_items_uniq ON order_items (order_id, product_name)");

            // Standalone sequence with custom params
            s.execute("CREATE SEQUENCE invoice_seq START 1000 INCREMENT BY 5 MINVALUE 1000 MAXVALUE 99999 CYCLE");

            // Comments on new objects
            s.execute("COMMENT ON TABLE products IS 'Product catalog with identity column'");
            s.execute("COMMENT ON TABLE employees IS 'Employee directory with generated columns'");

            // === Data for Tier 2 tables ===

            s.execute("INSERT INTO products (id, name, price, weight_kg) OVERRIDING SYSTEM VALUE VALUES (1, 'Laptop', 999.99, 2.100)");
            s.execute("INSERT INTO products (id, name, price, weight_kg) OVERRIDING SYSTEM VALUE VALUES (2, 'Mouse', 29.99, 0.085)");
            s.execute("INSERT INTO products (id, name, price) OVERRIDING SYSTEM VALUE VALUES (3, 'Keyboard', 79.99)");

            s.execute("INSERT INTO employees (first_name, last_name, hire_date, start_time, photo) VALUES " +
                    "('Jane', 'Doe', '2023-06-15', '09:00:00', '\\x48656c6c6f')");
            s.execute("INSERT INTO employees (first_name, last_name, hire_date) VALUES ('John', 'Smith', '2024-01-10')");

            s.execute("INSERT INTO reporting.monthly_stats VALUES ('2024-01-01', 150, 45000.00)");
            s.execute("INSERT INTO reporting.monthly_stats VALUES ('2024-02-01', 175, 52500.00)");

            s.execute("INSERT INTO tenant_users VALUES (1, 100, 'alice'), (1, 101, 'bob'), (2, 200, 'carol')");
            s.execute("INSERT INTO tenant_sessions (tenant_id, user_id) VALUES (1, 100), (1, 101), (2, 200)");

            s.execute("SELECT setval('invoice_seq', 1025)");

            // === Tier 2 Group B: Functions, triggers, domains ===

            // SQL-language function (immutable, used in views and indexes)
            s.execute("""
                CREATE FUNCTION format_currency(amount numeric) RETURNS text AS $$
                    SELECT '$' || to_char(amount, 'FM999,999,990.00')
                $$ LANGUAGE sql IMMUTABLE""");

            // PL/pgSQL function with IF/ELSE (used in views)
            s.execute("""
                CREATE FUNCTION classify_order(total numeric) RETURNS text AS $$
                BEGIN
                    IF total >= 500 THEN RETURN 'large';
                    ELSIF total >= 100 THEN RETURN 'medium';
                    ELSE RETURN 'small';
                    END IF;
                END;
                $$ LANGUAGE plpgsql IMMUTABLE""");

            // PL/pgSQL function used as column default
            s.execute("""
                CREATE FUNCTION generate_ref_code() RETURNS text AS $$
                BEGIN
                    RETURN 'REF-' || lpad(nextval('invoice_seq')::text, 6, '0');
                END;
                $$ LANGUAGE plpgsql""");

            // Function with OUT parameters
            s.execute("""
                CREATE FUNCTION order_summary(OUT total_orders bigint, OUT total_revenue numeric) AS $$
                BEGIN
                    SELECT count(*), coalesce(sum(total), 0) INTO total_orders, total_revenue FROM orders;
                END;
                $$ LANGUAGE plpgsql""");

            // View that uses custom functions
            s.execute("""
                CREATE VIEW order_details AS
                SELECT o.id, o.customer_id,
                       format_currency(o.total) AS formatted_total,
                       classify_order(o.total) AS size_category
                FROM orders o""");

            // Table with custom function as default
            s.execute("""
                CREATE TABLE invoices (
                    id serial PRIMARY KEY,
                    ref_code text NOT NULL DEFAULT generate_ref_code(),
                    amount numeric(10,2) NOT NULL,
                    created_at timestamptz DEFAULT now()
                )""");

            // Trigger function: auto-set updated_at
            s.execute("""
                CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
                BEGIN
                    NEW.updated_at := now();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql""");

            // Table with trigger
            s.execute("""
                CREATE TABLE settings (
                    key text PRIMARY KEY,
                    value text NOT NULL,
                    updated_at timestamptz DEFAULT now()
                )""");
            s.execute("CREATE TRIGGER trg_settings_updated BEFORE UPDATE ON settings FOR EACH ROW EXECUTE FUNCTION set_updated_at()");

            // Audit trigger with TG_OP
            s.execute("CREATE TABLE audit_trail (id serial, table_name text, operation text, detail text, ts timestamptz DEFAULT now())");
            s.execute("""
                CREATE FUNCTION audit_changes() RETURNS trigger AS $$
                BEGIN
                    IF TG_OP = 'INSERT' THEN
                        INSERT INTO audit_trail (table_name, operation, detail) VALUES (TG_TABLE_NAME, 'INSERT', NEW.key);
                        RETURN NEW;
                    ELSIF TG_OP = 'UPDATE' THEN
                        INSERT INTO audit_trail (table_name, operation, detail) VALUES (TG_TABLE_NAME, 'UPDATE', NEW.key);
                        RETURN NEW;
                    ELSIF TG_OP = 'DELETE' THEN
                        INSERT INTO audit_trail (table_name, operation, detail) VALUES (TG_TABLE_NAME, 'DELETE', OLD.key);
                        RETURN OLD;
                    END IF;
                    RETURN NULL;
                END;
                $$ LANGUAGE plpgsql""");
            s.execute("CREATE TRIGGER trg_settings_audit AFTER INSERT OR UPDATE OR DELETE ON settings FOR EACH ROW EXECUTE FUNCTION audit_changes()");

            // Domain types
            s.execute("CREATE DOMAIN email_address AS text CHECK (VALUE LIKE '%@%.%')");
            s.execute("CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)");
            s.execute("CREATE DOMAIN currency AS numeric(12,2) DEFAULT 0.00");

            // Table using domains
            s.execute("""
                CREATE TABLE contacts (
                    id serial PRIMARY KEY,
                    name text NOT NULL,
                    email email_address NOT NULL,
                    age positive_int,
                    balance currency
                )""");

            // Data for Group B tables
            s.execute("INSERT INTO invoices (amount) VALUES (500.00)");
            s.execute("INSERT INTO invoices (amount) VALUES (1250.00)");

            s.execute("INSERT INTO settings (key, value) VALUES ('site_name', 'Memgres Demo')");
            s.execute("INSERT INTO settings (key, value) VALUES ('max_retries', '3')");

            s.execute("INSERT INTO contacts (name, email, age, balance) VALUES ('Dave', 'dave@example.com', 30, 1500.00)");
            s.execute("INSERT INTO contacts (name, email, age) VALUES ('Eve', 'eve@example.com', 25)");
        }
    }

    // === pg_dump execution helper ===

        public static final class DumpResult {
        public final int exitCode;
        public final String stdout;
        public final String stderr;

        public DumpResult(int exitCode, String stdout, String stderr) {
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
            DumpResult that = (DumpResult) o;
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
            return "DumpResult[exitCode=" + exitCode + ", " + "stdout=" + stdout + ", " + "stderr=" + stderr + "]";
        }
    }

    static DumpResult runPgDump(String... extraArgs) throws Exception {
        List<String> cmd = new ArrayList<>(Cols.listOf(
                PG_DUMP,
                "-h", "localhost",
                "-p", String.valueOf(port),
                "-U", "memgres",
                "-d", "memgres",
                "--no-password",
                "--format=plain"
        ));
        cmd.addAll(Cols.listOf(extraArgs));

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.environment().put("PGPASSWORD", "memgres");
        Process proc = pb.start();

        // Read stdout and stderr in parallel to avoid deadlock
        Thread stderrThread = new Thread(() -> {});
        final byte[][] results = new byte[2][];
        stderrThread = new Thread(() -> {
            try { results[1] = IO.readAllBytes(proc.getErrorStream()); }
            catch (IOException e) { results[1] = new byte[0]; }
        });
        stderrThread.start();
        results[0] = IO.readAllBytes(proc.getInputStream());
        stderrThread.join(30_000);

        boolean finished = proc.waitFor(30, TimeUnit.SECONDS);
        if (!finished) {
            proc.destroyForcibly();
            fail("pg_dump timed out after 30 seconds");
        }

        return new DumpResult(proc.exitValue(),
                new String(results[0], StandardCharsets.UTF_8),
                new String(results[1], StandardCharsets.UTF_8));
    }

    // === Test: Schema-only dump succeeds ===

    @Test @Order(1)
    void pgDump_schemaOnly_succeeds() throws Exception {
        DumpResult r = runPgDump("--schema-only");
        schemaOnlyDump = r.stdout;

        System.out.println("=== pg_dump --schema-only ===");
        System.out.println("Exit code: " + r.exitCode);
        if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);
        System.out.println("Output lines: " + r.stdout.split("\n").length);

        assertEquals(0, r.exitCode, "pg_dump --schema-only failed:\n" + r.stderr);
        assertFalse(r.stdout.isEmpty(), "pg_dump produced no output");

        // Should contain CREATE TABLE statements
        assertTrue(r.stdout.contains("CREATE TABLE"), "Output should contain CREATE TABLE");
    }

    // === Test: Data-only dump succeeds ===

    @Test @Order(2)
    void pgDump_dataOnly_succeeds() throws Exception {
        DumpResult r = runPgDump("--data-only");
        dataOnlyDump = r.stdout;

        System.out.println("=== pg_dump --data-only ===");
        System.out.println("Exit code: " + r.exitCode);
        if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);

        assertEquals(0, r.exitCode, "pg_dump --data-only failed:\n" + r.stderr);

        // Should contain COPY statements with data
        assertTrue(r.stdout.contains("COPY"), "Output should contain COPY statements");
    }

    // === Test: Full dump succeeds ===

    @Test @Order(3)
    void pgDump_fullDump_succeeds() throws Exception {
        DumpResult r = runPgDump();
        fullDump = r.stdout;

        System.out.println("=== pg_dump (full) ===");
        System.out.println("Exit code: " + r.exitCode);
        if (!r.stderr.isEmpty()) System.out.println("Stderr:\n" + r.stderr);
        System.out.println("Output lines: " + fullDump.split("\n").length);

        assertEquals(0, r.exitCode, "pg_dump failed:\n" + r.stderr);

        // Should contain both DDL and COPY
        assertTrue(fullDump.contains("CREATE TABLE"), "Output should contain DDL");
        assertTrue(fullDump.contains("COPY"), "Output should contain COPY data");
    }

    // === Test: Output is valid SQL ===

    @Test @Order(4)
    void pgDump_outputIsValidSql() throws Exception {
        assertNotNull(fullDump, "Full dump not available (did pgDump_fullDump_succeeds run?)");

        // Verify key structures are present
        String upper = fullDump.toUpperCase();
        assertTrue(upper.contains("CREATE TYPE") || upper.contains("ORDER_STATUS"),
                "Should contain enum type");
        assertTrue(upper.contains("CREATE TABLE"), "Should contain tables");
        assertTrue(upper.contains("CREATE INDEX"), "Should contain indexes");
        assertTrue(upper.contains("CREATE VIEW") || upper.contains("CUSTOMER_SUMMARY"),
                "Should contain view");
        assertTrue(upper.contains("ALTER TABLE"), "Should contain constraints");
        assertTrue(upper.contains("COPY") && upper.contains("FROM STDIN"),
                "Should contain COPY FROM STDIN data blocks");

        // No pg_dump error markers in output
        assertFalse(fullDump.contains("-- could not dump"),
                "Dump contains error markers");
    }

    // === Test: Output restores into fresh Memgres ===

    @Test @Order(5)
    void pgDump_outputRestoresIntoMemgres() throws Exception {
        assertNotNull(fullDump, "Full dump not available");

        try (Memgres m2 = Memgres.builder().port(0).build().start();
             Connection c2 = DriverManager.getConnection(m2.getJdbcUrl(), m2.getUser(), m2.getPassword())) {
            c2.setAutoCommit(true);

            // Restore the dump
            List<String> errors = restoreDump(c2, fullDump);
            if (!errors.isEmpty()) {
                System.out.println("=== Restore errors (" + errors.size() + ") ===");
                errors.forEach(e -> System.out.println("  " + e));
            }

            // Verify tables exist and have data
            try (Statement s = c2.createStatement()) {
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM customers")) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getLong(1), "customers count after restore");
                }
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM orders")) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getLong(1), "orders count after restore");
                }
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM order_items")) {
                    assertTrue(rs.next());
                    assertEquals(4, rs.getLong(1), "order_items count after restore");
                }
            }

            // Few critical restore errors allowed (GRANT/REVOKE, some catalogs), but core must work
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
    }

    // === Test: Data integrity after self-restore ===

    @Test @Order(6)
    void pgDump_dataIntegrity_afterSelfRestore() throws Exception {
        assertNotNull(fullDump, "Full dump not available");

        try (Memgres m2 = Memgres.builder().port(0).build().start();
             Connection c2 = DriverManager.getConnection(m2.getJdbcUrl(), m2.getUser(), m2.getPassword())) {
            c2.setAutoCommit(true);
            restoreDump(c2, fullDump);

            try (Statement s = c2.createStatement()) {
                // Verify specific data values
                try (ResultSet rs = s.executeQuery(
                        "SELECT name, email, score FROM customers WHERE name = 'Alice Johnson'")) {
                    assertTrue(rs.next(), "Alice should exist");
                    assertEquals("alice@example.com", rs.getString("email"));
                    assertEquals(new java.math.BigDecimal("98.50"), rs.getBigDecimal("score"));
                }

                // Verify enum values survived
                try (ResultSet rs = s.executeQuery(
                        "SELECT status FROM orders WHERE customer_id = 1 ORDER BY id LIMIT 1")) {
                    assertTrue(rs.next());
                    assertEquals("confirmed", rs.getString(1));
                }

                // Verify view is queryable
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM customer_summary")) {
                    assertTrue(rs.next());
                    assertTrue(rs.getInt(1) > 0, "customer_summary should have rows");
                }

                // Verify FK constraint works after restore
                assertThrows(SQLException.class, () -> {
                    try (Statement s2 = c2.createStatement()) {
                        s2.execute("INSERT INTO orders (customer_id, status, total) VALUES (999, 'pending', 10)");
                    }
                }, "FK constraint should prevent inserting with invalid customer_id");
            }
        }
    }

    // === Dump restore helper ===

    static List<String> restoreDump(Connection c, String rawSql) throws Exception {
        CopyManager cm = new CopyManager(c.unwrap(BaseConnection.class));
        // Normalize Windows line endings
        String[] lines = rawSql.replace("\r\n", "\n").replace("\r", "\n").split("\n", -1);
        StringBuilder currentStmt = new StringBuilder();
        List<String> errors = new ArrayList<>();

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            if (currentStmt.isEmpty() && (line.trim().startsWith("--") || line.trim().isEmpty())) continue;
            // Skip psql meta-commands (e.g. backslash-commands like restrict)
            if (currentStmt.isEmpty() && line.trim().startsWith("\\")) continue;

            currentStmt.append(line).append("\n");
            String soFar = currentStmt.toString().trim();

            // COPY FROM stdin block
            if (soFar.toUpperCase().startsWith("COPY ") && soFar.endsWith(";")
                    && soFar.toUpperCase().contains("FROM STDIN")) {
                String copyCmd = soFar.substring(0, soFar.length() - 1);
                StringBuilder copyData = new StringBuilder();
                i++;
                while (i < lines.length) {
                    if (lines[i].equals("\\.")) break;
                    copyData.append(lines[i]).append("\n");
                    i++;
                }
                try {
                    cm.copyIn(copyCmd, new StringReader(copyData.toString()));
                } catch (Exception e) {
                    errors.add("COPY: " + e.getMessage());
                }
                currentStmt.setLength(0);
                continue;
            }

            if (isStatementComplete(soFar)) {
                try (Statement s = c.createStatement()) {
                    s.execute(soFar);
                } catch (Exception e) {
                    errors.add(e.getMessage() + " => " + soFar.substring(0, Math.min(80, soFar.length())));
                }
                currentStmt.setLength(0);
            }
        }
        return errors;
    }

    static boolean isStatementComplete(String sql) {
        if (!sql.endsWith(";")) return false;
        boolean inSingleQuote = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inSingleQuote) { inSingleQuote = true; }
            else if (c == '\'' && inSingleQuote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') { i++; }
                else { inSingleQuote = false; }
            }
            if (c == '$' && !inSingleQuote) {
                int j = i + 1;
                while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
                if (j < sql.length() && sql.charAt(j) == '$') {
                    String tag = sql.substring(i, j + 1);
                    int end = sql.indexOf(tag, j + 1);
                    if (end < 0) return false;
                    i = end + tag.length() - 1;
                }
            }
        }
        return !inSingleQuote;
    }
}
