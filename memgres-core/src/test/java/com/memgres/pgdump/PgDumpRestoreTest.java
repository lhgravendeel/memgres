package com.memgres.pgdump;

import com.memgres.engine.util.IO;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.memgres.engine.util.Strs;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that real pg_dump output restores into Memgres WITHOUT skipping
 * any statement types. This test executes everything a pg_dump produces.
 */
class PgDumpRestoreTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // === Statement categories for reporting ===

    enum StmtCategory {
        SET, SET_CONFIG, COMMENT_ON, GRANT_REVOKE, OWNER_TO,
        COPY_FROM_STDIN, SETVAL, DDL, OTHER
    }

        public static final class StmtResult {
        public final int index;
        public final String sql;
        public final StmtCategory category;
        public final boolean success;
        public final String error;

        public StmtResult(
                int index,
                String sql,
                StmtCategory category,
                boolean success,
                String error
        ) {
            this.index = index;
            this.sql = sql;
            this.category = category;
            this.success = success;
            this.error = error;
        }

        public int index() { return index; }
        public String sql() { return sql; }
        public StmtCategory category() { return category; }
        public boolean success() { return success; }
        public String error() { return error; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StmtResult that = (StmtResult) o;
            return index == that.index
                && java.util.Objects.equals(sql, that.sql)
                && java.util.Objects.equals(category, that.category)
                && success == that.success
                && java.util.Objects.equals(error, that.error);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(index, sql, category, success, error);
        }

        @Override
        public String toString() {
            return "StmtResult[index=" + index + ", " + "sql=" + sql + ", " + "category=" + category + ", " + "success=" + success + ", " + "error=" + error + "]";
        }
    }

    static StmtCategory categorize(String stmt) {
        String upper = Strs.stripLeading(stmt).toUpperCase();
        // Strip leading comments
        while (upper.startsWith("--")) {
            int nl = upper.indexOf('\n');
            if (nl < 0) return StmtCategory.OTHER;
            upper = Strs.stripLeading(upper.substring(nl + 1));
        }
        if (upper.startsWith("SET ")) return StmtCategory.SET;
        if (upper.startsWith("SELECT PG_CATALOG.SET_CONFIG")) return StmtCategory.SET_CONFIG;
        if (upper.startsWith("COMMENT ON")) return StmtCategory.COMMENT_ON;
        if (upper.startsWith("GRANT ") || upper.startsWith("REVOKE ")) return StmtCategory.GRANT_REVOKE;
        if (upper.contains("OWNER TO")) return StmtCategory.OWNER_TO;
        if (upper.startsWith("COPY ") && upper.contains("FROM") &&
                (upper.contains("STDIN") || upper.contains("stdin"))) return StmtCategory.COPY_FROM_STDIN;
        if (upper.startsWith("SELECT PG_CATALOG.SETVAL")) return StmtCategory.SETVAL;
        if (upper.startsWith("CREATE ") || upper.startsWith("ALTER ") || upper.startsWith("DROP "))
            return StmtCategory.DDL;
        return StmtCategory.OTHER;
    }

    // === Full dump restore with detailed reporting ===

    /**
     * Simulates restoring a pg_dump output into Memgres, exercising all statement
     * categories that pg_dump produces: SET preamble, set_config, schema-qualified
     * CREATE TABLE with defaults/constraints, sequences with OWNED BY, indexes
     * (regular + unique), ALTER TABLE ADD CONSTRAINT (PK, FK, CHECK), and setval.
     */
    @Test
    void restorePgDumpStyle_allStatements() throws Exception {
        String sql = PG_DUMP_STYLE_SQL;

        List<String> statements = splitSqlStatements(sql);
        List<StmtResult> results = new ArrayList<>();

        for (int i = 0; i < statements.size(); i++) {
            String stmt = statements.get(i).trim();
            if (stmt.isEmpty() || isCommentOnly(stmt)) continue;

            StmtCategory cat = categorize(stmt);

            try (Statement s = conn.createStatement()) {
                s.execute(stmt);
                results.add(new StmtResult(i, truncate(stmt), cat, true, null));
            } catch (SQLException e) {
                results.add(new StmtResult(i, truncate(stmt), cat, false, e.getMessage()));
            }
        }

        // Collect all failures for the assertion message
        List<StmtResult> failures = results.stream().filter(r -> !r.success()).collect(Collectors.toList());
        if (!failures.isEmpty()) {
            StringBuilder msg = new StringBuilder();
            msg.append(failures.size()).append(" statement(s) failed:\n");
            for (StmtResult f : failures) {
                msg.append("  [").append(f.category()).append("] ")
                        .append(f.sql(), 0, Math.min(100, f.sql().length()))
                        .append("\n    → ").append(f.error()).append("\n");
            }
            fail(msg.toString());
        }

        // Verify the schema was actually created correctly
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "INSERT INTO public.projects (name, status, config) " +
                "VALUES ('test', 'ACTIVE', '{\"key\": \"val\"}'::jsonb) RETURNING id");
            assertTrue(rs.next());
            assertTrue(rs.getInt(1) > 0);

            s.execute("INSERT INTO public.members (project_id, email, role) " +
                "VALUES (" + rs.getInt(1) + ", 'alice@test.com', 'OWNER')");

            rs = s.executeQuery("SELECT count(*) FROM public.audit_events");
            assertTrue(rs.next());
        }
    }

    /**
     * pg_dump-style SQL covering all statement categories: SET preamble, set_config,
     * schema-qualified DDL, sequences with OWNED BY, defaults (now(), jsonb, text casts),
     * CHECK constraints, primary keys, foreign keys, unique + regular indexes, and setval.
     */
    private static final String PG_DUMP_STYLE_SQL = """
            --
            -- PostgreSQL database dump (simulated)
            --

            SET statement_timeout = 0;
            SET lock_timeout = 0;
            SET idle_in_transaction_session_timeout = 0;
            SET client_encoding = 'UTF8';
            SET standard_conforming_strings = on;
            SELECT pg_catalog.set_config('search_path', '', false);
            SET check_function_bodies = false;
            SET xmloption = content;
            SET client_min_messages = warning;
            SET row_security = off;

            SET default_tablespace = '';
            SET default_table_access_method = heap;

            --
            -- Name: projects; Type: TABLE; Schema: public
            --

            CREATE TABLE public.projects (
                id bigint NOT NULL,
                name text NOT NULL,
                description text DEFAULT ''::text NOT NULL,
                status text DEFAULT 'ACTIVE'::text NOT NULL,
                config jsonb DEFAULT '{}'::jsonb NOT NULL,
                metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
                created_at timestamp with time zone DEFAULT now() NOT NULL,
                updated_at timestamp with time zone DEFAULT now() NOT NULL,
                CONSTRAINT projects_status_check CHECK ((status = ANY (ARRAY['ACTIVE'::text, 'ARCHIVED'::text, 'DELETED'::text])))
            );

            --
            -- Name: projects_id_seq; Type: SEQUENCE; Schema: public
            --

            CREATE SEQUENCE public.projects_id_seq
                START WITH 1
                INCREMENT BY 1
                NO MINVALUE
                NO MAXVALUE
                CACHE 1;

            --
            -- Name: projects_id_seq; Type: SEQUENCE OWNED BY; Schema: public
            --

            ALTER SEQUENCE public.projects_id_seq OWNED BY public.projects.id;

            --
            -- Name: members; Type: TABLE; Schema: public
            --

            CREATE TABLE public.members (
                id bigint NOT NULL,
                project_id bigint NOT NULL,
                email text NOT NULL,
                role text DEFAULT 'VIEWER'::text NOT NULL,
                payload jsonb DEFAULT '{}'::jsonb NOT NULL,
                invited_at timestamp with time zone DEFAULT now() NOT NULL,
                CONSTRAINT members_role_check CHECK ((role = ANY (ARRAY['OWNER'::text, 'EDITOR'::text, 'VIEWER'::text])))
            );

            --
            -- Name: members_id_seq; Type: SEQUENCE; Schema: public
            --

            CREATE SEQUENCE public.members_id_seq
                START WITH 1
                INCREMENT BY 1
                NO MINVALUE
                NO MAXVALUE
                CACHE 1;

            ALTER SEQUENCE public.members_id_seq OWNED BY public.members.id;

            --
            -- Name: audit_events; Type: TABLE; Schema: public
            --

            CREATE TABLE public.audit_events (
                id bigint NOT NULL,
                project_id bigint,
                actor_email text,
                action text NOT NULL,
                details jsonb DEFAULT '{}'::jsonb NOT NULL,
                created_at timestamp with time zone DEFAULT now() NOT NULL
            );

            CREATE SEQUENCE public.audit_events_id_seq
                START WITH 1
                INCREMENT BY 1
                NO MINVALUE
                NO MAXVALUE
                CACHE 1;

            ALTER SEQUENCE public.audit_events_id_seq OWNED BY public.audit_events.id;

            --
            -- Name: settings; Type: TABLE; Schema: public
            --

            CREATE TABLE public.settings (
                id bigint NOT NULL,
                project_id bigint NOT NULL,
                key text NOT NULL,
                value text DEFAULT ''::text NOT NULL,
                updated_at timestamp with time zone DEFAULT now() NOT NULL
            );

            CREATE SEQUENCE public.settings_id_seq
                START WITH 1
                INCREMENT BY 1
                NO MINVALUE
                NO MAXVALUE
                CACHE 1;

            ALTER SEQUENCE public.settings_id_seq OWNED BY public.settings.id;

            --
            -- Name: id defaults; Type: DEFAULT; Schema: public
            --

            ALTER TABLE ONLY public.projects ALTER COLUMN id SET DEFAULT nextval('public.projects_id_seq'::regclass);
            ALTER TABLE ONLY public.members ALTER COLUMN id SET DEFAULT nextval('public.members_id_seq'::regclass);
            ALTER TABLE ONLY public.audit_events ALTER COLUMN id SET DEFAULT nextval('public.audit_events_id_seq'::regclass);
            ALTER TABLE ONLY public.settings ALTER COLUMN id SET DEFAULT nextval('public.settings_id_seq'::regclass);

            --
            -- Name: primary keys; Type: CONSTRAINT; Schema: public
            --

            ALTER TABLE ONLY public.projects ADD CONSTRAINT projects_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY public.members ADD CONSTRAINT members_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY public.audit_events ADD CONSTRAINT audit_events_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY public.settings ADD CONSTRAINT settings_pkey PRIMARY KEY (id);

            --
            -- Name: unique constraints; Type: CONSTRAINT; Schema: public
            --

            ALTER TABLE ONLY public.members ADD CONSTRAINT members_project_email_unique UNIQUE (project_id, email);
            ALTER TABLE ONLY public.settings ADD CONSTRAINT settings_project_key_unique UNIQUE (project_id, key);

            --
            -- Name: foreign keys; Type: FK CONSTRAINT; Schema: public
            --

            ALTER TABLE ONLY public.members
                ADD CONSTRAINT members_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

            ALTER TABLE ONLY public.audit_events
                ADD CONSTRAINT audit_events_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE SET NULL;

            ALTER TABLE ONLY public.settings
                ADD CONSTRAINT settings_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

            --
            -- Name: indexes; Type: INDEX; Schema: public
            --

            CREATE INDEX idx_members_email ON public.members USING btree (email);
            CREATE INDEX idx_audit_events_created_at ON public.audit_events USING btree (created_at);
            CREATE INDEX idx_audit_events_project_id ON public.audit_events USING btree (project_id);
            CREATE UNIQUE INDEX idx_projects_name ON public.projects USING btree (name);

            --
            -- Name: sequence values; Type: SEQUENCE SET; Schema: public
            --

            SELECT pg_catalog.setval('public.projects_id_seq', 100, true);
            SELECT pg_catalog.setval('public.members_id_seq', 500, true);
            SELECT pg_catalog.setval('public.audit_events_id_seq', 1000, true);
            SELECT pg_catalog.setval('public.settings_id_seq', 200, true);
            """;

    // === Individual category tests ===

    @Test
    void pgDumpPreamble_setStatements() throws Exception {
        // Standard pg_dump preamble SET statements
        String[] sets = {
                "SET statement_timeout = 0",
                "SET lock_timeout = 0",
                "SET idle_in_transaction_session_timeout = 0",
                "SET client_encoding = 'UTF8'",
                "SET standard_conforming_strings = on",
                "SET check_function_bodies = false",
                "SET xmloption = content",
                "SET client_min_messages = warning",
                "SET row_security = off",
                "SET default_tablespace = ''",
                "SET default_table_access_method = heap",
        };

        List<String> failures = new ArrayList<>();
        for (String set : sets) {
            try (Statement s = conn.createStatement()) {
                s.execute(set);
            } catch (SQLException e) {
                failures.add(set + " → " + e.getMessage());
            }
        }
        if (!failures.isEmpty()) {
            fail("SET statement failures:\n" + String.join("\n", failures));
        }
    }

    @Test
    void pgDumpPreamble_setConfig() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT pg_catalog.set_config('search_path', '', false)");
        }
        // Restore search_path for subsequent tests
        try (Statement s = conn.createStatement()) {
            s.execute("SET search_path TO public, pg_catalog");
        }
    }

    @Test
    void pgDumpPreamble_setval() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SEQUENCE IF NOT EXISTS test_seq_pgdump START 1");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT pg_catalog.setval('test_seq_pgdump', 42, true)")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getLong(1));
        }
        // Verify the sequence advanced
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT nextval('test_seq_pgdump')")) {
            assertTrue(rs.next());
            assertEquals(43, rs.getLong(1));
        }
    }

    @Test
    void commentOn_table() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS test_comment_tbl (id int PRIMARY KEY, name text)");
            s.execute("COMMENT ON TABLE test_comment_tbl IS 'Test table for pg_dump'");
        }
        // Verify comment is stored
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT description FROM pg_catalog.pg_description d " +
                     "JOIN pg_catalog.pg_class c ON d.objoid = c.oid " +
                     "WHERE c.relname = 'test_comment_tbl' AND d.objsubid = 0")) {
            assertTrue(rs.next(), "Comment should be retrievable from pg_description");
            assertEquals("Test table for pg_dump", rs.getString(1));
        }
    }

    @Test
    void commentOn_column() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS test_comment_col (id int PRIMARY KEY, name text)");
            s.execute("COMMENT ON COLUMN test_comment_col.name IS 'The name field'");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT description FROM pg_catalog.pg_description d " +
                     "JOIN pg_catalog.pg_class c ON d.objoid = c.oid " +
                     "JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid " +
                     "WHERE c.relname = 'test_comment_col' AND a.attname = 'name'")) {
            assertTrue(rs.next(), "Column comment should be retrievable");
            assertEquals("The name field", rs.getString(1));
        }
    }

    @Test
    void commentOn_variousObjects() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS test_comment_idx (id int PRIMARY KEY, name text)");
            s.execute("CREATE INDEX IF NOT EXISTS idx_comment_name ON test_comment_idx(name)");
        }

        // COMMENT ON INDEX
        try (Statement s = conn.createStatement()) {
            s.execute("COMMENT ON INDEX idx_comment_name IS 'Index on name'");
        }

        // COMMENT ON SCHEMA
        try (Statement s = conn.createStatement()) {
            s.execute("COMMENT ON SCHEMA public IS 'Standard public schema'");
        }
    }

    @Test
    void alterTable_ownerTo() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS test_owner_tbl (id int)");
            // pg_dump always emits OWNER TO; should be silently accepted
            s.execute("ALTER TABLE test_owner_tbl OWNER TO memgres");
        }
    }

    @Test
    void grantRevoke_silentlyAccepted() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS test_grant_tbl (id int)");
        }
        // pg_dump emits GRANT/REVOKE; should be silently accepted or at least not error
        List<String> grants = Cols.listOf(
                "GRANT ALL ON TABLE test_grant_tbl TO memgres",
                "GRANT SELECT ON TABLE test_grant_tbl TO memgres",
                "REVOKE ALL ON TABLE test_grant_tbl FROM PUBLIC"
        );
        List<String> failures = new ArrayList<>();
        for (String grant : grants) {
            try (Statement s = conn.createStatement()) {
                s.execute(grant);
            } catch (SQLException e) {
                failures.add(grant + " → " + e.getMessage());
            }
        }
        if (!failures.isEmpty()) {
            fail("GRANT/REVOKE failures:\n" + String.join("\n", failures));
        }
    }

    @Test
    void copyFromStdin_viaProtocol() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS test_copy_restore (id int, name text, active boolean)");
        }

        CopyManager cm = new CopyManager(conn.unwrap(BaseConnection.class));

        // Simulate pg_dump COPY FROM stdin block (tab-delimited, \N for null)
        String copyData = "1\tAlice\tt\n2\tBob\tf\n3\t\\N\t\\N\n";
        long rows = cm.copyIn("COPY test_copy_restore (id, name, active) FROM STDIN", new StringReader(copyData));
        assertEquals(3, rows);

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM test_copy_restore")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
        }

        // Verify null handling
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT name, active FROM test_copy_restore WHERE id = 3")) {
            assertTrue(rs.next());
            assertNull(rs.getString("name"));
            assertNull(rs.getObject("active"));
        }
    }

    @Test
    void copyFromStdin_allPgDumpTypes() throws Exception {
        // Create a table with types commonly seen in pg_dump output
        try (Statement s = conn.createStatement()) {
            s.execute("""
                CREATE TABLE IF NOT EXISTS test_copy_types (
                    id integer,
                    big_id bigint,
                    name text,
                    price numeric(10,2),
                    active boolean,
                    created_at timestamp with time zone,
                    tags text[],
                    metadata jsonb,
                    uid uuid
                )
            """);
        }

        CopyManager cm = new CopyManager(conn.unwrap(BaseConnection.class));
        // Tab-delimited pg_dump style data
        String data = String.join("\n",
                "1\t100\tWidget\t9.99\tt\t2024-01-15 10:30:00+00\t{red,blue}\t{\"key\": \"val\"}\ta0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                "2\t200\tGadget\t19.50\tf\t2024-06-20 14:00:00+00\t{green}\t{}\tb0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22",
                "3\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"
        ) + "\n";

        long rows = cm.copyIn("COPY test_copy_types FROM STDIN", new StringReader(data));
        assertEquals(3, rows);

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM test_copy_types ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Widget", rs.getString("name"));
            assertEquals(true, rs.getBoolean("active"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("Gadget", rs.getString("name"));

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertNull(rs.getString("name"));
            assertNull(rs.getObject("active"));
            assertNull(rs.getString("uid"));
        }
    }

    // === Full pg_dump restore with COPY FROM stdin data ===

    /**
     * Restores the reference-dump.sql fixture (synthetic pg_dump output with
     * DDL, COPY FROM stdin data blocks, constraints, indexes, comments, and
     * sequence setval). Verifies data integrity after restore.
     */
    @Test
    void restoreReferenceDump_withCopyData() throws Exception {
        Path dumpFile = findFixture("pgdump-fixtures/reference-dump.sql");
        String rawSql = IO.readString(dumpFile, StandardCharsets.UTF_8);

        // Use a fresh Memgres for this test to avoid conflicts
        try (Memgres m = Memgres.builder().port(0).build().start();
             Connection c = DriverManager.getConnection(m.getJdbcUrl(), m.getUser(), m.getPassword())) {
            c.setAutoCommit(true);

            List<StmtResult> results = restoreDumpWithCopy(c, rawSql);

            // Print summary
            long passed = results.stream().filter(StmtResult::success).count();
            long failed = results.size() - passed;
            System.out.printf("%n=== Reference Dump Restore: %d passed, %d failed ===%n", passed, failed);

            List<StmtResult> failures = results.stream().filter(r -> !r.success()).collect(Collectors.toList());
            for (StmtResult f : failures) {
                System.out.printf("  FAIL [%s]: %s%n    -> %s%n", f.category(),
                        f.sql().substring(0, Math.min(100, f.sql().length())), f.error());
            }

            // Verify data integrity
            try (Statement s = c.createStatement()) {
                // Customer count
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM public.customers")) {
                    assertTrue(rs.next());
                    assertEquals(5, rs.getLong(1), "customers row count");
                }

                // Order count
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM public.orders")) {
                    assertTrue(rs.next());
                    assertEquals(5, rs.getLong(1), "orders row count");
                }

                // Order items count
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM public.order_items")) {
                    assertTrue(rs.next());
                    assertEquals(7, rs.getLong(1), "order_items row count");
                }

                // Verify specific data values survived the COPY round-trip
                try (ResultSet rs = s.executeQuery(
                        "SELECT name, email, score, uid FROM public.customers WHERE id = 1")) {
                    assertTrue(rs.next());
                    assertEquals("Alice Johnson", rs.getString("name"));
                    assertEquals("alice@example.com", rs.getString("email"));
                    assertEquals(new java.math.BigDecimal("98.50"), rs.getBigDecimal("score"));
                    assertEquals("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", rs.getString("uid"));
                }

                // Verify NULL handling
                try (ResultSet rs = s.executeQuery(
                        "SELECT tags, score, uid FROM public.customers WHERE id = 3")) {
                    assertTrue(rs.next());
                    assertNull(rs.getObject("tags"), "null array");
                    assertNull(rs.getObject("score"), "null numeric");
                    assertNull(rs.getObject("uid"), "null uuid");
                }

                // Verify special characters in data
                try (ResultSet rs = s.executeQuery(
                        "SELECT name FROM public.customers WHERE id = 4")) {
                    assertTrue(rs.next());
                    assertEquals("Dave \"The Dev\" O'Brien", rs.getString("name"));
                }

                // Verify empty string name (id=5)
                try (ResultSet rs = s.executeQuery(
                        "SELECT name FROM public.customers WHERE id = 5")) {
                    assertTrue(rs.next());
                    assertEquals("", rs.getString("name"));
                }

                // Verify enum values survived
                try (ResultSet rs = s.executeQuery(
                        "SELECT status FROM public.orders WHERE id = 5")) {
                    assertTrue(rs.next());
                    assertEquals("cancelled", rs.getString("status"));
                }

                // Verify FK works (order_items -> orders)
                try (ResultSet rs = s.executeQuery(
                        "SELECT count(*) FROM public.order_items WHERE order_id = 1")) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getLong(1), "items for order 1");
                }

                // Verify sequence was set correctly (next value should be 6)
                try (ResultSet rs = s.executeQuery("SELECT nextval('public.customers_id_seq')")) {
                    assertTrue(rs.next());
                    assertEquals(6, rs.getLong(1), "customers_id_seq after setval(5,true)");
                }

                // Verify view works
                try (ResultSet rs = s.executeQuery(
                        "SELECT order_count, total_spent FROM public.customer_summary WHERE id = 1")) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getLong("order_count"));
                }

                // Verify comments persisted
                try (ResultSet rs = s.executeQuery(
                        "SELECT description FROM pg_catalog.pg_description d " +
                        "JOIN pg_catalog.pg_class c ON d.objoid = c.oid " +
                        "WHERE c.relname = 'customers' AND d.objsubid = 0")) {
                    assertTrue(rs.next(), "table comment should exist");
                    assertEquals("Core customer records", rs.getString(1));
                }
            }

            if (!failures.isEmpty()) {
                fail(failures.size() + " statement(s) failed during restore");
            }
        }
    }

    // === Dump restore engine (handles COPY FROM stdin blocks) ===

    /**
     * Restores a pg_dump plain SQL file, properly handling COPY FROM stdin blocks
     * by detecting them and routing data through CopyManager.
     */
    private List<StmtResult> restoreDumpWithCopy(Connection c, String rawSql) throws Exception {
        CopyManager cm = new CopyManager(c.unwrap(BaseConnection.class));
        List<StmtResult> results = new ArrayList<>();
        String[] lines = rawSql.split("\n", -1);
        StringBuilder currentStmt = new StringBuilder();
        boolean inString = false;
        char stringChar = 0;
        int stmtIdx = 0;

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];

            // Skip comment-only lines outside of statements
            if (currentStmt.isEmpty() && (line.trim().startsWith("--") || line.trim().isEmpty())) {
                continue;
            }

            currentStmt.append(line).append("\n");
            String soFar = currentStmt.toString().trim();

            // Check if this completes a COPY FROM stdin statement
            if (soFar.toUpperCase().startsWith("COPY ") && soFar.endsWith(";")
                    && soFar.toUpperCase().contains("FROM STDIN")) {
                String copyCmd = soFar.substring(0, soFar.length() - 1); // strip trailing ;
                // Collect data lines until \.
                StringBuilder copyData = new StringBuilder();
                i++;
                while (i < lines.length) {
                    if (lines[i].equals("\\.")) break;
                    copyData.append(lines[i]).append("\n");
                    i++;
                }
                // Execute via CopyManager
                stmtIdx++;
                try {
                    long rows = cm.copyIn(copyCmd, new StringReader(copyData.toString()));
                    results.add(new StmtResult(stmtIdx, truncate(copyCmd), StmtCategory.COPY_FROM_STDIN,
                            true, rows + " rows"));
                } catch (Exception e) {
                    results.add(new StmtResult(stmtIdx, truncate(copyCmd), StmtCategory.COPY_FROM_STDIN,
                            false, e.getMessage()));
                }
                currentStmt.setLength(0);
                continue;
            }

            // Check for statement terminator (semicolon outside strings/dollar-quotes)
            if (isStatementComplete(soFar)) {
                String stmt = soFar;
                if (stmt.endsWith(";")) stmt = stmt.substring(0, stmt.length() - 1).trim();
                if (stmt.isEmpty() || isCommentOnly(stmt)) {
                    currentStmt.setLength(0);
                    continue;
                }
                stmtIdx++;
                StmtCategory cat = categorize(stmt);
                try (Statement s = c.createStatement()) {
                    s.execute(stmt);
                    results.add(new StmtResult(stmtIdx, truncate(stmt), cat, true, null));
                } catch (SQLException e) {
                    results.add(new StmtResult(stmtIdx, truncate(stmt), cat, false, e.getMessage()));
                }
                currentStmt.setLength(0);
            }
        }

        // Handle any trailing statement without semicolon
        String remaining = currentStmt.toString().trim();
        if (!remaining.isEmpty() && !isCommentOnly(remaining)) {
            stmtIdx++;
            try (Statement s = c.createStatement()) {
                s.execute(remaining);
                results.add(new StmtResult(stmtIdx, truncate(remaining), categorize(remaining), true, null));
            } catch (SQLException e) {
                results.add(new StmtResult(stmtIdx, truncate(remaining), categorize(remaining), false, e.getMessage()));
            }
        }

        return results;
    }

    /**
     * Simple check if a SQL string ends with a semicolon that's not inside a string or dollar-quote.
     */
    private static boolean isStatementComplete(String sql) {
        if (!sql.endsWith(";")) return false;
        // Quick check: count unmatched quotes
        boolean inSingle = false;
        boolean inDouble = false;
        int dollarDepth = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (dollarDepth > 0) {
                if (c == '$') {
                    int j = i + 1;
                    while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
                    if (j < sql.length() && sql.charAt(j) == '$') {
                        dollarDepth--;
                        i = j;
                    }
                }
                continue;
            }
            if (c == '$' && !inSingle && !inDouble) {
                int j = i + 1;
                while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
                if (j < sql.length() && sql.charAt(j) == '$') {
                    dollarDepth++;
                    i = j;
                    continue;
                }
            }
            if (c == '\'' && !inDouble) {
                if (inSingle && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') { i++; continue; }
                inSingle = !inSingle;
            }
            if (c == '"' && !inSingle) inDouble = !inDouble;
        }
        return !inSingle && !inDouble && dollarDepth == 0;
    }

    // === Helpers ===

    private static Path findFixture(String relativePath) {
        // Test resources are on classpath, but also try direct file access
        Path p = Path.of("memgres-core", "src", "test", "resources", relativePath);
        if (Files.exists(p)) return p;
        p = Path.of("src", "test", "resources", relativePath);
        if (Files.exists(p)) return p;
        // Try classpath
        var url = PgDumpRestoreTest.class.getClassLoader().getResource(relativePath);
        if (url != null) {
            try { return Path.of(url.toURI()); } catch (Exception ignored) {}
        }
        fail("Cannot find fixture: " + relativePath);
        return null;
    }

    private static String truncate(String s) {
        s = s.replaceAll("\\s+", " ").trim();
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }

    public static boolean isCommentOnly(String stmt) {
        return Strs.lines(stmt).allMatch(line -> {
            String trimmed = line.trim();
            return trimmed.isEmpty() || trimmed.startsWith("--");
        });
    }

    // === SQL Statement Splitting ===
    // Splits on semicolons, respecting:
    //   - Dollar-quoted strings ($$, $tag$)
    //   - Bare $ delimiters (normalized to $$ in output)
    //   - Single-quoted and double-quoted strings
    //   - Line comments (--)
    //   - Block comments (/* */)
    //   - COPY ... FROM stdin data blocks (consumed until \.)
    public static List<String> splitSqlStatements(String sql) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        char stringChar = 0;
        // Track lines for COPY stdin handling
        String[] lines = sql.split("\n", -1);
        int lineIdx = 0;
        boolean inCopyData = false;

        // Rebuild as character-based but also track whether we just emitted a COPY statement
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            // Dollar quoting: $$, $tag$, or bare $ (followed by whitespace)
            if (!inString && c == '$') {
                int j = i + 1;
                while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) {
                    j++;
                }
                if (j < sql.length() && sql.charAt(j) == '$') {
                    // Standard dollar-quote: $$ or $tag$
                    String delimiter = sql.substring(i, j + 1);
                    current.append(delimiter);
                    i = j + 1;
                    int close = sql.indexOf(delimiter, i);
                    if (close >= 0) {
                        current.append(sql, i, close + delimiter.length());
                        i = close + delimiter.length() - 1;
                    } else {
                        current.append(sql.substring(i));
                        i = sql.length() - 1;
                    }
                    continue;
                }
                // Bare $ followed by whitespace: treat as $$ delimiter (normalize)
                if (j == i + 1 && j < sql.length() && Character.isWhitespace(sql.charAt(j))) {
                    // Emit $$ instead of bare $
                    current.append("$$");
                    i = j;
                    // Find matching bare $ (closing)
                    int close = -1;
                    for (int k = i; k < sql.length(); k++) {
                        if (sql.charAt(k) == '$') {
                            if (k + 1 >= sql.length() || sql.charAt(k + 1) == ';'
                                || Character.isWhitespace(sql.charAt(k + 1))) {
                                close = k;
                                break;
                            }
                        }
                    }
                    if (close >= 0) {
                        current.append(sql, i, close);
                        current.append("$$"); // emit $$ instead of bare $
                        i = close;
                    } else {
                        current.append(sql.substring(i));
                        i = sql.length() - 1;
                    }
                    continue;
                }
                // Lone $ not followed by anything meaningful, so just emit it
                current.append(c);
                continue;
            }

            if (inString) {
                current.append(c);
                if (c == stringChar) {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == stringChar) {
                        current.append(sql.charAt(++i));
                    } else {
                        inString = false;
                    }
                }
            } else if (c == '\'' || c == '"') {
                inString = true;
                stringChar = c;
                current.append(c);
            } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                int eol = sql.indexOf('\n', i);
                if (eol < 0) eol = sql.length();
                current.append(sql, i, Math.min(eol + 1, sql.length()));
                i = eol;
            } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                int close = sql.indexOf("*/", i + 2);
                if (close >= 0) {
                    current.append(sql, i, close + 2);
                    i = close + 1;
                } else {
                    current.append(sql.substring(i));
                    i = sql.length() - 1;
                }
            } else if (c == ';') {
                String stmt = current.toString().trim();
                if (!stmt.isEmpty()) {
                    result.add(stmt);
                    // Check if this was a COPY ... FROM stdin; if so, skip data lines
                    if (isCopyFromStdin(stmt)) {
                        i = skipCopyData(sql, i + 1);
                    }
                }
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) {
            result.add(last);
        }
        return result;
    }

    /**
     * Check if a statement is a COPY ... FROM stdin/STDIN command.
     */
    private static boolean isCopyFromStdin(String stmt) {
        String upper = stmt.toUpperCase();
        return upper.startsWith("COPY ") && upper.contains("FROM") &&
            (upper.contains("STDIN") || upper.contains("stdin"));
    }

    /**
     * Skip past COPY data lines (tab-delimited data terminated by \. on its own line).
     * Returns the index to continue parsing from.
     */
    private static int skipCopyData(String sql, int fromIndex) {
        int i = fromIndex;
        while (i < sql.length()) {
            // Find start of next line
            int lineStart = i;
            int lineEnd = sql.indexOf('\n', i);
            if (lineEnd < 0) lineEnd = sql.length();

            String line = sql.substring(lineStart, lineEnd).trim();
            if (line.equals("\\.")) {
                // End of COPY data; return position after this line
                return Math.min(lineEnd + 1, sql.length()) - 1;
            }
            i = lineEnd + 1;
        }
        return sql.length() - 1;
    }

}
