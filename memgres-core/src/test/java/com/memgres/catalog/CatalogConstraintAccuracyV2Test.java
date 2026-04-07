package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #42-44: smoke_a constraint counts (UNIQUE INDEX not in pg_constraint/info_schema)
 * Diff #45: patch_t migration constraint counts
 * Diffs #47-48: realistic schema (account/project/task) constraint counts
 *
 * Root cause: CREATE UNIQUE INDEX does not register as a constraint in pg_constraint.
 * All assertions use EXACT counts matching PG 18 behavior.
 */
class CatalogConstraintAccuracyV2Test {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static int countRows(String sql) throws SQLException { try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) { int n = 0; while (rs.next()) n++; return n; } }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }

    // Diff #42: info_schema.table_constraints: PG returns 3 (PK + UNIQUE from index + NOT NULL)
    @Test void smoke_a_info_schema_table_constraints() throws SQLException {
        exec("CREATE SCHEMA cat_test"); exec("SET search_path = cat_test");
        exec("CREATE TABLE smoke_a(id int PRIMARY KEY, note text NOT NULL DEFAULT 'x', qty int, created_at timestamptz DEFAULT CURRENT_TIMESTAMP)");
        exec("CREATE UNIQUE INDEX smoke_a_note_idx ON smoke_a(note)");
        try {
            int count = countRows("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_schema = 'cat_test' AND table_name = 'smoke_a'
                """);
            // PG 18 returns 3: PK + UNIQUE (from CREATE UNIQUE INDEX) + (UNIQUE from NOT NULL? no)
            // Actually PG returns: smoke_a_pkey(PK), smoke_a_note_idx(UNIQUE), smoke_a_note_not_null(CHECK for NOT NULL)
            // The diff says PG=3, memgres=2. We need exactly 3.
            assertEquals(3, count,
                    "Should have 3 constraints: PK + UNIQUE from index + one more, got " + count);
        } finally {
            exec("DROP SCHEMA cat_test CASCADE"); exec("SET search_path = public");
        }
    }

    // Diff #43: key_column_usage JOIN, PG returns exactly 1 row
    @Test void smoke_a_key_column_usage_join() throws SQLException {
        exec("CREATE SCHEMA cat_test2"); exec("SET search_path = cat_test2");
        exec("CREATE TABLE smoke_a(id int PRIMARY KEY, note text NOT NULL DEFAULT 'x', qty int)");
        exec("CREATE UNIQUE INDEX smoke_a_note_idx ON smoke_a(note)");
        try {
            int count = countRows("""
                SELECT kcu.column_name, tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name
                WHERE tc.table_schema = 'cat_test2' AND tc.table_name = 'smoke_a'
                ORDER BY kcu.ordinal_position
                """);
            // PG returns 1 (PK key column only; UNIQUE INDEX without ADD CONSTRAINT doesn't appear in KCU)
            // The diff says PG=1, memgres=2. We need exactly 1.
            assertEquals(1, count, "key_column_usage should have exactly 1 row (PK only), got " + count);
        } finally {
            exec("DROP SCHEMA cat_test2 CASCADE"); exec("SET search_path = public");
        }
    }

    // Diff #44: pg_constraint, PG returns 3 for smoke_a
    @Test void smoke_a_pg_constraint() throws SQLException {
        exec("CREATE SCHEMA cat_test3"); exec("SET search_path = cat_test3");
        exec("CREATE TABLE smoke_a(id int PRIMARY KEY, note text NOT NULL DEFAULT 'x', qty int)");
        exec("CREATE UNIQUE INDEX smoke_a_note_idx ON smoke_a(note)");
        try {
            int count = countRows("""
                SELECT conname, contype FROM pg_constraint
                WHERE conrelid = 'cat_test3.smoke_a'::regclass
                """);
            // PG returns 3: PK + UNIQUE + NOT NULL on 'note'. The diff says PG=3, memgres=2.
            assertEquals(3, count, "pg_constraint should have 3 entries (PK + UNIQUE + NOT NULL), got " + count);
        } finally {
            exec("DROP SCHEMA cat_test3 CASCADE"); exec("SET search_path = public");
        }
    }

    // Diff #45: migration pattern, PG returns 4 constraints
    @Test void migration_pattern_pg_constraint() throws SQLException {
        exec("CREATE SCHEMA mig_test"); exec("SET search_path = mig_test");
        exec("CREATE TABLE patch_t(id serial PRIMARY KEY, code text NOT NULL, qty text NOT NULL, created_on text NOT NULL)");
        exec("ALTER TABLE patch_t ADD COLUMN IF NOT EXISTS active boolean DEFAULT true");
        exec("ALTER TABLE patch_t RENAME COLUMN code TO ext_code");
        exec("ALTER TABLE patch_t ALTER COLUMN qty TYPE int USING qty::int");
        exec("CREATE UNIQUE INDEX patch_ext_code_idx ON patch_t(ext_code)");
        exec("ALTER TABLE patch_t ADD CONSTRAINT patch_ext_code_uq UNIQUE USING INDEX patch_ext_code_idx");
        try {
            int count = countRows("SELECT 1 FROM pg_constraint WHERE conrelid = 'mig_test.patch_t'::regclass");
            // PG 18 returns 5: PK + UNIQUE via USING INDEX + NOT NULLs on id,qty,created_on
            // (ext_code NOT NULL is absorbed by the UNIQUE constraint added via USING INDEX)
            assertEquals(5, count, "patch_t should have 5 constraints, got " + count);
        } finally {
            exec("DROP SCHEMA mig_test CASCADE"); exec("SET search_path = public");
        }
    }

    // Diffs #47-48: realistic schema, PG returns 26 constraints
    @Test void realistic_schema_constraint_counts() throws SQLException {
        exec("CREATE SCHEMA real_test"); exec("SET search_path = real_test");
        exec("CREATE TYPE app_status AS ENUM ('new','active','disabled')");
        exec("""
            CREATE TABLE account(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, email text NOT NULL,
              display_name text, status app_status NOT NULL DEFAULT 'new', version int NOT NULL DEFAULT 0,
              deleted boolean NOT NULL DEFAULT false, created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP, CONSTRAINT account_email_uq UNIQUE (email))
            """);
        exec("""
            CREATE TABLE project(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, account_id bigint NOT NULL,
              code text NOT NULL, title text NOT NULL, archived boolean NOT NULL DEFAULT false,
              created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
              CONSTRAINT project_account_fk FOREIGN KEY (account_id) REFERENCES account(id),
              CONSTRAINT project_code_uq UNIQUE (code))
            """);
        exec("""
            CREATE TABLE task(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, project_id bigint NOT NULL,
              ext_ref text, title text NOT NULL, state text NOT NULL DEFAULT 'open', priority int NOT NULL DEFAULT 0,
              due_date date, created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
              CONSTRAINT task_project_fk FOREIGN KEY (project_id) REFERENCES project(id))
            """);
        try {
            int isCount = countRows("SELECT 1 FROM information_schema.table_constraints WHERE table_schema = 'real_test'");
            // PG returns 26. The diff says PG=26, memgres=7.
            assertEquals(26, isCount, "info_schema should have 26 constraints, got " + isCount);

            int pgCount = countRows("""
                SELECT 1 FROM pg_constraint WHERE conrelid IN (
                    'real_test.account'::regclass, 'real_test.project'::regclass, 'real_test.task'::regclass)
                """);
            // PG returns 26 in pg_constraint too.
            assertEquals(26, pgCount, "pg_constraint should have 26 constraints, got " + pgCount);
        } finally {
            exec("DROP SCHEMA real_test CASCADE"); exec("SET search_path = public");
        }
    }
}
