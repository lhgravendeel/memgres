package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for system catalog accuracy: pg_constraint, pg_index, pg_class,
 * information_schema.table_constraints, information_schema.key_column_usage.
 *
 * Core issues:
 * - pg_constraint doesn't include NOT NULL constraints (contype='n' in PG 17+)
 * - pg_constraint missing some CHECK constraints
 * - pg_index phantom entries or missing entries
 * - information_schema.table_constraints incomplete
 * - pg_class missing sequences, indexes, composite types
 */
class SystemCatalogConstraintTest {

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

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
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
    // pg_constraint: PK, UNIQUE, CHECK, FK all present
    // ========================================================================

    @Test
    void pg_constraint_pk_unique_check() throws SQLException {
        exec("CREATE TABLE con_t(id int PRIMARY KEY, code text UNIQUE NOT NULL, val int CHECK (val > 0))");
        try {
            List<List<String>> cons = query(
                    "SELECT conname, contype FROM pg_constraint WHERE conrelid = 'con_t'::regclass ORDER BY conname");

            Map<String, Integer> typeCounts = new HashMap<>();
            for (List<String> row : cons) {
                typeCounts.merge(row.get(1), 1, Integer::sum);
            }

            assertEquals(1, typeCounts.getOrDefault("p", 0), "Should have 1 PRIMARY KEY");
            assertEquals(1, typeCounts.getOrDefault("u", 0), "Should have 1 UNIQUE");
            assertEquals(1, typeCounts.getOrDefault("c", 0), "Should have 1 CHECK");
            // PG: PK + UNIQUE + CHECK = at least 3
            assertTrue(cons.size() >= 3, "Should have at least 3 constraints, got " + cons.size());
        } finally {
            exec("DROP TABLE con_t");
        }
    }

    @Test
    void pg_constraint_foreign_key() throws SQLException {
        exec("CREATE TABLE con_parent(id int PRIMARY KEY)");
        exec("CREATE TABLE con_child(id int PRIMARY KEY, pid int REFERENCES con_parent(id))");
        try {
            List<List<String>> cons = query(
                    "SELECT conname, contype FROM pg_constraint WHERE conrelid = 'con_child'::regclass ORDER BY conname");
            long fkCount = cons.stream().filter(r -> "f".equals(r.get(1))).count();
            assertEquals(1, fkCount, "Should have 1 FK constraint");
            // con_child also has a PK
            long pkCount = cons.stream().filter(r -> "p".equals(r.get(1))).count();
            assertEquals(1, pkCount, "Should have 1 PK constraint");
        } finally {
            exec("DROP TABLE con_child"); exec("DROP TABLE con_parent");
        }
    }

    @Test
    void pg_constraint_not_null_in_pg17_plus() throws SQLException {
        // PG 18: NOT NULL constraints appear as contype='n' in pg_constraint
        exec("CREATE TABLE nn_t(id int PRIMARY KEY, name text NOT NULL, val int)");
        try {
            List<List<String>> cons = query(
                    "SELECT conname, contype FROM pg_constraint WHERE conrelid = 'nn_t'::regclass ORDER BY conname");
            // PG 18 stores NOT NULL as contype='n' in pg_constraint for ALL NOT NULL columns including PK
            long nnCount = cons.stream().filter(r -> "n".equals(r.get(1))).count();
            assertEquals(2, nnCount,
                    "pg_constraint should include 2 NOT NULL constraints (contype='n') for 'id' and 'name', found " + nnCount);
        } finally {
            exec("DROP TABLE nn_t");
        }
    }

    // ========================================================================
    // pg_constraint: complex schema with many constraint types
    // ========================================================================

    @Test
    void pg_constraint_complex_schema() throws SQLException {
        exec("""
            CREATE TABLE smoke_a(
              id int PRIMARY KEY,
              code text UNIQUE NOT NULL,
              val int CHECK (val >= 0)
            )
            """);
        try {
            int conCount = countRows(
                    "SELECT 1 FROM pg_constraint WHERE conrelid = 'smoke_a'::regclass");
            // PG: PK(p) + UNIQUE(u) + CHECK(c) + NOT NULL on code(n) + NOT NULL on id(n) = 5
            // At minimum without NOT NULL: PK + UNIQUE + CHECK = 3
            assertTrue(conCount >= 3,
                    "smoke_a should have at least 3 constraints (PK, UNIQUE, CHECK), got " + conCount);
        } finally {
            exec("DROP TABLE smoke_a");
        }
    }

    @Test
    void pg_constraint_realistic_schema() throws SQLException {
        exec("CREATE TABLE account(id serial PRIMARY KEY, email text UNIQUE NOT NULL, name text NOT NULL, active boolean DEFAULT true NOT NULL)");
        exec("CREATE TABLE project(id serial PRIMARY KEY, account_id int NOT NULL REFERENCES account(id), name text NOT NULL, budget numeric CHECK (budget >= 0))");
        exec("CREATE TABLE task(id serial PRIMARY KEY, project_id int NOT NULL REFERENCES project(id), title text NOT NULL, status text DEFAULT 'open' CHECK (status IN ('open','closed','blocked')))");
        try {
            // Count constraints across all three tables
            int acctCons = countRows("SELECT 1 FROM pg_constraint WHERE conrelid = 'account'::regclass");
            int projCons = countRows("SELECT 1 FROM pg_constraint WHERE conrelid = 'project'::regclass");
            int taskCons = countRows("SELECT 1 FROM pg_constraint WHERE conrelid = 'task'::regclass");

            // account: PK + UNIQUE + (NOT NULLs)
            assertTrue(acctCons >= 2, "account should have >= 2 constraints, got " + acctCons);
            // project: PK + FK + CHECK + (NOT NULLs)
            assertTrue(projCons >= 3, "project should have >= 3 constraints, got " + projCons);
            // task: PK + FK + CHECK + (NOT NULLs)
            assertTrue(taskCons >= 3, "task should have >= 3 constraints, got " + taskCons);

            // Total across all tables
            int totalCons = countRows("""
                SELECT 1 FROM pg_constraint
                WHERE conrelid IN ('account'::regclass, 'project'::regclass, 'task'::regclass)
                """);
            assertTrue(totalCons >= 8, "Total constraints should be >= 8, got " + totalCons);
        } finally {
            exec("DROP TABLE task"); exec("DROP TABLE project"); exec("DROP TABLE account");
        }
    }

    // ========================================================================
    // information_schema.table_constraints
    // ========================================================================

    @Test
    void information_schema_table_constraints_complete() throws SQLException {
        exec("CREATE TABLE is_t(id int PRIMARY KEY, code text UNIQUE, val int CHECK (val > 0))");
        try {
            List<List<String>> cons = query("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_name = 'is_t'
                ORDER BY constraint_name
                """);
            Map<String, Integer> types = new HashMap<>();
            for (List<String> r : cons) types.merge(r.get(1), 1, Integer::sum);

            assertEquals(1, types.getOrDefault("PRIMARY KEY", 0), "Should have 1 PK");
            assertEquals(1, types.getOrDefault("UNIQUE", 0), "Should have 1 UNIQUE");
            // PG 18: CHECK(val > 0) + NOT NULL on PK col 'id' (appears as CHECK) = 2 CHECK entries
            assertEquals(2, types.getOrDefault("CHECK", 0), "Should have 2 CHECK (explicit + PK NOT NULL)");
            assertTrue(cons.size() >= 4, "Should have >= 4 constraints in info schema, got " + cons.size());
        } finally {
            exec("DROP TABLE is_t");
        }
    }

    @Test
    void key_column_usage_correct() throws SQLException {
        exec("CREATE TABLE kcu_t(id int PRIMARY KEY, code text UNIQUE NOT NULL)");
        try {
            // PK column
            int pkCols = countRows("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = 'kcu_t' AND tc.constraint_type = 'PRIMARY KEY'
                """);
            assertEquals(1, pkCols, "PK should reference exactly 1 column");

            // UNIQUE column
            int uqCols = countRows("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = 'kcu_t' AND tc.constraint_type = 'UNIQUE'
                """);
            assertEquals(1, uqCols, "UNIQUE should reference exactly 1 column");
        } finally {
            exec("DROP TABLE kcu_t");
        }
    }

    // ========================================================================
    // pg_index accuracy
    // ========================================================================

    @Test
    void pg_index_exact_count() throws SQLException {
        exec("CREATE TABLE idx_t(id int PRIMARY KEY, a int, b text)");
        exec("CREATE INDEX idx_a ON idx_t(a)");
        exec("CREATE UNIQUE INDEX idx_b ON idx_t(b)");
        try {
            List<List<String>> indexes = query("""
                SELECT i.indexrelid::regclass, i.indisunique, i.indisprimary
                FROM pg_index i
                WHERE i.indrelid = 'idx_t'::regclass
                ORDER BY 1
                """);
            // Exactly 3: PK index, idx_a, idx_b
            assertEquals(3, indexes.size(), "Should have exactly 3 indexes, got " + indexes.size());

            long primary = indexes.stream().filter(r -> "t".equals(r.get(2))).count();
            assertEquals(1, primary, "Exactly 1 primary index");

            long unique = indexes.stream().filter(r -> "t".equals(r.get(1))).count();
            assertEquals(2, unique, "2 unique indexes (PK + idx_b)");
        } finally {
            exec("DROP TABLE idx_t CASCADE");
        }
    }

    @Test
    void pg_index_partial_with_predicate() throws SQLException {
        exec("CREATE TABLE pidx_t(id int PRIMARY KEY, email text, status text)");
        exec("CREATE UNIQUE INDEX pidx ON pidx_t(email) WHERE status = 'active'");
        try {
            String hasPred = scalar("""
                SELECT i.indpred IS NOT NULL
                FROM pg_index i
                WHERE i.indexrelid = 'pidx'::regclass
                """);
            assertEquals("t", hasPred, "Partial index should have non-null indpred");
        } finally {
            exec("DROP TABLE pidx_t CASCADE");
        }
    }

    @Test
    void pg_index_no_phantom_entries() throws SQLException {
        exec("CREATE TABLE noph_t(id int PRIMARY KEY, a int UNIQUE, b text)");
        try {
            int indexCount = countRows("""
                SELECT 1 FROM pg_index i WHERE i.indrelid = 'noph_t'::regclass
                """);
            // Exactly 2: PK index + UNIQUE index on 'a'
            assertEquals(2, indexCount, "Should have exactly 2 indexes, not more");
        } finally {
            exec("DROP TABLE noph_t CASCADE");
        }
    }

    // ========================================================================
    // pg_class completeness
    // ========================================================================

    @Test
    void pg_class_includes_all_object_types() throws SQLException {
        exec("CREATE TABLE pgc_t(id serial PRIMARY KEY, note text)");
        exec("CREATE VIEW pgc_v AS SELECT * FROM pgc_t");
        exec("CREATE INDEX pgc_idx ON pgc_t(note)");
        try {
            List<List<String>> objects = query("""
                SELECT c.relname, c.relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public' AND c.relname LIKE 'pgc_%'
                ORDER BY c.relname
                """);

            Set<String> kinds = new HashSet<>();
            Set<String> names = new HashSet<>();
            for (List<String> r : objects) {
                names.add(r.get(0));
                kinds.add(r.get(1));
            }

            assertTrue(names.contains("pgc_t"), "Should list the table");
            assertTrue(names.contains("pgc_v"), "Should list the view");
            assertTrue(names.contains("pgc_idx"), "Should list the explicit index");
            // serial creates pgc_t_id_seq
            assertTrue(names.stream().anyMatch(n -> n.contains("seq")),
                    "Should list the serial sequence: " + names);
            // PK creates an index
            assertTrue(names.stream().anyMatch(n -> n.contains("pkey")),
                    "Should list the PK index: " + names);
        } finally {
            exec("DROP VIEW pgc_v");
            exec("DROP TABLE pgc_t CASCADE");
        }
    }

    // ========================================================================
    // pg_constraint after ALTER TABLE ADD CONSTRAINT
    // ========================================================================

    @Test
    void pg_constraint_after_add_unique_via_index() throws SQLException {
        exec("CREATE TABLE alt_t(id int PRIMARY KEY, code text)");
        exec("CREATE UNIQUE INDEX alt_code_idx ON alt_t(code)");
        exec("ALTER TABLE alt_t ADD CONSTRAINT alt_code_uq UNIQUE USING INDEX alt_code_idx");
        try {
            int conCount = countRows(
                    "SELECT 1 FROM pg_constraint WHERE conrelid = 'alt_t'::regclass");
            // PK + UNIQUE = at least 2
            assertTrue(conCount >= 2,
                    "Should have at least 2 constraints after ADD CONSTRAINT USING INDEX, got " + conCount);
        } finally {
            exec("DROP TABLE alt_t CASCADE");
        }
    }

    // ========================================================================
    // Migration-style schema: constraint counts must match
    // ========================================================================

    @Test
    void migration_schema_constraint_count() throws SQLException {
        exec("""
            CREATE TABLE patch_t(
              id serial PRIMARY KEY,
              code text NOT NULL,
              qty text NOT NULL,
              created_on text NOT NULL
            )
            """);
        exec("ALTER TABLE patch_t ADD COLUMN IF NOT EXISTS active boolean DEFAULT true");
        exec("ALTER TABLE patch_t RENAME COLUMN code TO ext_code");
        exec("ALTER TABLE patch_t ALTER COLUMN qty TYPE int USING qty::int");
        exec("CREATE UNIQUE INDEX patch_ext_code_idx ON patch_t(ext_code)");
        exec("ALTER TABLE patch_t ADD CONSTRAINT patch_ext_code_uq UNIQUE USING INDEX patch_ext_code_idx");
        try {
            int conCount = countRows(
                    "SELECT 1 FROM pg_constraint WHERE conrelid = 'patch_t'::regclass");
            // PK + UNIQUE via index + (NOT NULLs in PG17+)
            // Without NOT NULLs: at least PK + UNIQUE = 2
            // With NOT NULLs: PK + UNIQUE + NOT NULL on code, qty, created_on = 5
            assertTrue(conCount >= 2,
                    "patch_t should have >= 2 constraints after migration, got " + conCount);
        } finally {
            exec("DROP TABLE patch_t CASCADE");
        }
    }

    // ========================================================================
    // Composite index and multi-column PK
    // ========================================================================

    @Test
    void multi_column_pk_index() throws SQLException {
        exec("CREATE TABLE mcpk(a int, b int, c text, PRIMARY KEY (a, b))");
        try {
            int indexCount = countRows(
                    "SELECT 1 FROM pg_index i WHERE i.indrelid = 'mcpk'::regclass");
            assertEquals(1, indexCount, "Multi-column PK should have exactly 1 index");

            String isPrimary = scalar("""
                SELECT i.indisprimary FROM pg_index i WHERE i.indrelid = 'mcpk'::regclass
                """);
            assertEquals("t", isPrimary, "The only index should be the primary key");
        } finally {
            exec("DROP TABLE mcpk");
        }
    }

    // ========================================================================
    // Named CHECK constraints visible in pg_constraint
    // ========================================================================

    @Test
    void named_check_constraint_visible() throws SQLException {
        exec("CREATE TABLE nck_t(id int PRIMARY KEY, val int CONSTRAINT val_positive CHECK (val > 0))");
        try {
            String conname = scalar("""
                SELECT conname FROM pg_constraint
                WHERE conrelid = 'nck_t'::regclass AND contype = 'c'
                """);
            assertEquals("val_positive", conname, "Named CHECK constraint should appear in pg_constraint");
        } finally {
            exec("DROP TABLE nck_t");
        }
    }

    @Test
    void multiple_check_constraints() throws SQLException {
        exec("""
            CREATE TABLE mck_t(
              id int PRIMARY KEY,
              a int CHECK (a > 0),
              b int CHECK (b < 100),
              CONSTRAINT ab_check CHECK (a < b)
            )
            """);
        try {
            int checkCount = countRows("""
                SELECT 1 FROM pg_constraint
                WHERE conrelid = 'mck_t'::regclass AND contype = 'c'
                """);
            assertEquals(3, checkCount, "Should have 3 CHECK constraints");
        } finally {
            exec("DROP TABLE mck_t");
        }
    }
}
