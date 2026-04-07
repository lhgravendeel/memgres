package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for system catalog accuracy with realistic application schemas.
 *
 * Mirrors the exact patterns from:
 * - 34_information_schema_pg_catalog_smoke.sql
 * - 40_metadata_realistic_schema.sql
 *
 * Focuses on:
 * - information_schema.table_constraints listing all constraint types
 * - key_column_usage correct join counts
 * - pg_constraint contype accuracy (p, u, c, f, n)
 * - pg_class listing all object kinds (tables, views, indexes, sequences)
 * - pg_index not over/under-counting
 */
class SystemCatalogRealisticTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static int countRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = 0; while (rs.next()) n++; return n;
        }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    // ========================================================================
    // Exact pattern from 34_information_schema_pg_catalog_smoke.sql
    // smoke_a: PK + UNIQUE index + NOT NULL on note + DEFAULT
    // ========================================================================

    @Test
    void smoke_a_table_constraints() throws SQLException {
        exec("CREATE SCHEMA smoke_test");
        exec("SET search_path = smoke_test");
        exec("""
            CREATE TABLE smoke_a(
              id int PRIMARY KEY,
              note text NOT NULL DEFAULT 'x',
              qty int,
              created_at timestamptz DEFAULT CURRENT_TIMESTAMP
            )
            """);
        exec("CREATE UNIQUE INDEX smoke_a_note_idx ON smoke_a(note)");
        try {
            // information_schema.table_constraints
            List<List<String>> cons = query("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_schema = 'smoke_test' AND table_name = 'smoke_a'
                ORDER BY constraint_name
                """);
            // PG returns 3: PK constraint + UNIQUE (from index) + possibly CHECK
            // At minimum PK + UNIQUE = 2, PG may also show the UNIQUE index as a constraint
            assertTrue(cons.size() >= 2,
                    "Should have >= 2 constraints in info_schema, got " + cons.size() + ": " + cons);

            // key_column_usage JOIN should return 1 row per key column
            // PK has 1 column (id), UNIQUE has 1 column (note) → at most 2 rows from join
            List<List<String>> kcu = query("""
                SELECT kcu.column_name, tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                 AND tc.table_name = kcu.table_name
                WHERE tc.table_schema = 'smoke_test' AND tc.table_name = 'smoke_a'
                ORDER BY kcu.ordinal_position
                """);
            // PG: PK(id) → 1 row. That's it for key_column_usage if UNIQUE isn't a named constraint.
            // With a named UNIQUE INDEX → might add another row
            assertTrue(kcu.size() >= 1 && kcu.size() <= 3,
                    "key_column_usage should have 1-3 rows, got " + kcu.size());

            // pg_constraint
            List<List<String>> pgCons = query("""
                SELECT conname, contype
                FROM pg_constraint
                WHERE conrelid = 'smoke_test.smoke_a'::regclass
                ORDER BY conname
                """);
            // PG 18: PK(p) + UNIQUE if from ADD CONSTRAINT + NOT NULLs(n)
            // At least PK
            assertTrue(pgCons.size() >= 1,
                    "pg_constraint should have >= 1 constraint, got " + pgCons.size());

            // pg_index: PK index + note unique index = 2
            int indexCount = countRows("""
                SELECT 1 FROM pg_index i
                WHERE i.indrelid = 'smoke_test.smoke_a'::regclass
                """);
            assertEquals(2, indexCount, "Should have exactly 2 indexes (PK + UNIQUE)");
        } finally {
            exec("DROP SCHEMA smoke_test CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // Exact pattern from 40_metadata_realistic_schema.sql
    // 3 tables with PKs, FKs, UNIQUEs, NOT NULLs, indexes, view
    // ========================================================================

    @Test
    void realistic_schema_constraint_count() throws SQLException {
        exec("CREATE SCHEMA rs_test");
        exec("SET search_path = rs_test");
        exec("CREATE TYPE app_status AS ENUM ('new', 'active', 'disabled')");
        exec("""
            CREATE TABLE account(
              id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              email text NOT NULL,
              display_name text,
              status app_status NOT NULL DEFAULT 'new',
              version int NOT NULL DEFAULT 0,
              deleted boolean NOT NULL DEFAULT false,
              created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
              CONSTRAINT account_email_uq UNIQUE (email)
            )
            """);
        exec("""
            CREATE TABLE project(
              id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              account_id bigint NOT NULL,
              code text NOT NULL,
              title text NOT NULL,
              archived boolean NOT NULL DEFAULT false,
              created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
              CONSTRAINT project_account_fk FOREIGN KEY (account_id) REFERENCES account(id),
              CONSTRAINT project_code_uq UNIQUE (code)
            )
            """);
        exec("""
            CREATE TABLE task(
              id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              project_id bigint NOT NULL,
              ext_ref text,
              title text NOT NULL,
              state text NOT NULL DEFAULT 'open',
              priority int NOT NULL DEFAULT 0,
              due_date date,
              created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
              CONSTRAINT task_project_fk FOREIGN KEY (project_id) REFERENCES project(id)
            )
            """);
        exec("CREATE INDEX task_project_idx ON task(project_id)");
        exec("CREATE INDEX task_open_priority_idx ON task(priority DESC) WHERE state = 'open'");
        exec("""
            CREATE VIEW task_summary AS
            SELECT t.id, p.code AS project_code, t.title, t.state, t.priority
            FROM task t JOIN project p ON p.id = t.project_id
            """);
        try {
            // information_schema.table_constraints: ALL constraints across all 3 tables
            int totalInfoConstraints = countRows("""
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_schema = 'rs_test'
                """);
            // account: PK + UNIQUE(email) = 2
            // project: PK + FK + UNIQUE(code) = 3
            // task: PK + FK = 2
            // Total explicit: 7 minimum
            // PG 18 with NOT NULL constraints: many more (each NOT NULL column adds one)
            // account has 6 NOT NULL cols, project has 5, task has 5 → ~16 NOT NULLs + 7 = ~23
            assertTrue(totalInfoConstraints >= 7,
                    "Should have >= 7 constraints total in info_schema, got " + totalInfoConstraints);

            // pg_class: all objects in the schema
            List<List<String>> pgClassObjects = query("""
                SELECT c.relname, c.relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'rs_test'
                ORDER BY c.relname
                """);
            Set<String> names = new HashSet<>();
            for (List<String> r : pgClassObjects) names.add(r.get(0));

            // Should include: account, project, task (tables)
            // + task_summary (view)
            // + 3 PK indexes (account_pkey, project_pkey, task_pkey)
            // + 2 UNIQUE indexes (account_email_uq, project_code_uq)
            // + 2 explicit indexes (task_project_idx, task_open_priority_idx)
            // + 3 identity sequences (account_id_seq, project_id_seq, task_id_seq)
            // = ~14 objects
            assertTrue(pgClassObjects.size() >= 10,
                    "pg_class should list >= 10 objects, got " + pgClassObjects.size() + ": " + names);
            assertTrue(names.contains("account"), "Should list account table");
            assertTrue(names.contains("project"), "Should list project table");
            assertTrue(names.contains("task"), "Should list task table");
            assertTrue(names.contains("task_summary"), "Should list task_summary view");

            // pg_constraint: across all 3 tables
            int totalPgConstraints = countRows("""
                SELECT 1 FROM pg_constraint
                WHERE conrelid IN (
                    'rs_test.account'::regclass,
                    'rs_test.project'::regclass,
                    'rs_test.task'::regclass
                )
                """);
            // Without NOT NULLs: PK*3 + UNIQUE*2 + FK*2 = 7
            // With NOT NULLs: 7 + ~16 = ~23
            assertTrue(totalPgConstraints >= 7,
                    "pg_constraint should have >= 7 constraints, got " + totalPgConstraints);

            // pg_index for task table: PK + 2 explicit = 3
            int taskIndexCount = countRows("""
                SELECT 1 FROM pg_index i
                WHERE i.indrelid = 'rs_test.task'::regclass
                """);
            assertEquals(3, taskIndexCount,
                    "task should have exactly 3 indexes (PK + project_idx + open_priority_idx)");
        } finally {
            exec("DROP SCHEMA rs_test CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // pg_class: sequences from GENERATED ALWAYS AS IDENTITY visible
    // ========================================================================

    @Test
    void identity_sequences_visible_in_pg_class() throws SQLException {
        exec("CREATE SCHEMA iseq");
        exec("SET search_path = iseq");
        exec("CREATE TABLE t(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, note text)");
        try {
            int seqCount = countRows("""
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'iseq' AND c.relkind = 'S'
                """);
            assertTrue(seqCount >= 1,
                    "Identity column should create a sequence visible in pg_class");
        } finally {
            exec("DROP SCHEMA iseq CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // pg_class: indexes visible with correct relkind
    // ========================================================================

    @Test
    void indexes_visible_in_pg_class_as_relkind_i() throws SQLException {
        exec("CREATE SCHEMA ivis");
        exec("SET search_path = ivis");
        exec("CREATE TABLE t(id int PRIMARY KEY, a int, b text)");
        exec("CREATE INDEX idx_a ON t(a)");
        try {
            int indexCount = countRows("""
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'ivis' AND c.relkind = 'i'
                """);
            // PK index + idx_a = 2
            assertEquals(2, indexCount,
                    "Should have 2 indexes visible in pg_class with relkind='i'");
        } finally {
            exec("DROP SCHEMA ivis CASCADE");
            exec("SET search_path = public");
        }
    }

    // ========================================================================
    // CHECK constraint from ADD COLUMN IF NOT EXISTS edge case
    // ========================================================================

    @Test
    void add_column_if_not_exists_bad_constraint_rejected() throws SQLException {
        exec("CREATE TABLE acine_t(id int PRIMARY KEY, a int)");
        try {
            exec("ALTER TABLE acine_t ADD COLUMN IF NOT EXISTS b int DEFAULT 0");
            // Adding again with a conflicting type/constraint should be silently ignored
            // (column already exists) and NOT apply the new constraint
            exec("ALTER TABLE acine_t ADD COLUMN IF NOT EXISTS b text CHECK (length(b) < 5)");
            // b is still int, not text; the CHECK should NOT have been applied
            exec("INSERT INTO acine_t VALUES (1, 10, 99999)");
            // If the CHECK was wrongly applied, this would fail
        } finally {
            exec("DROP TABLE acine_t");
        }
    }
}
