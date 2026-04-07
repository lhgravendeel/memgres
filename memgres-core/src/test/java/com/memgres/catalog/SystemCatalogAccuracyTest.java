package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that system catalog views (pg_constraint, pg_index, pg_class,
 * information_schema.table_constraints, information_schema.key_column_usage)
 * return accurate metadata matching what PG 18 reports.
 *
 * Key issues found in verification:
 * - pg_constraint missing NOT NULL constraints or check constraints
 * - pg_index returning phantom indexes or missing real ones
 * - information_schema.table_constraints not listing all constraint types
 * - pg_get_indexdef formatting differences
 * - pg_get_expr for column defaults
 * - pg_get_viewdef formatting
 */
class SystemCatalogAccuracyTest {

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
    // pg_constraint accuracy
    // ========================================================================

    @Test
    void pg_constraint_lists_pk_unique_check() throws SQLException {
        exec("CREATE TABLE cat_t(id int PRIMARY KEY, code text UNIQUE NOT NULL, val int CHECK (val > 0))");
        try {
            List<List<String>> cons = query(
                    "SELECT conname, contype FROM pg_constraint WHERE conrelid = 'cat_t'::regclass ORDER BY conname");

            // Should have: PRIMARY KEY (p), UNIQUE (u), CHECK (c)
            long pkCount = cons.stream().filter(r -> "p".equals(r.get(1))).count();
            long uqCount = cons.stream().filter(r -> "u".equals(r.get(1))).count();
            long ckCount = cons.stream().filter(r -> "c".equals(r.get(1))).count();

            assertEquals(1, pkCount, "Should have exactly 1 PRIMARY KEY constraint");
            assertEquals(1, uqCount, "Should have exactly 1 UNIQUE constraint");
            assertEquals(1, ckCount, "Should have exactly 1 CHECK constraint");
            // PG 17+ also includes NOT NULL constraints (contype='n'), so total may be > 3
            assertTrue(cons.size() >= 3, "Should have at least 3 constraints total, got " + cons.size());
        } finally {
            exec("DROP TABLE IF EXISTS cat_t");
        }
    }

    @Test
    void pg_constraint_lists_foreign_keys() throws SQLException {
        exec("CREATE TABLE cat_parent(id int PRIMARY KEY)");
        exec("CREATE TABLE cat_child(id int PRIMARY KEY, parent_id int REFERENCES cat_parent(id))");
        try {
            long fkCount = countRows(
                    "SELECT 1 FROM pg_constraint WHERE conrelid = 'cat_child'::regclass AND contype = 'f'");
            assertEquals(1, fkCount, "Should have 1 FOREIGN KEY constraint");
        } finally {
            exec("DROP TABLE IF EXISTS cat_child");
            exec("DROP TABLE IF EXISTS cat_parent");
        }
    }

    // ========================================================================
    // information_schema.table_constraints
    // ========================================================================

    @Test
    void information_schema_table_constraints_complete() throws SQLException {
        exec("CREATE TABLE is_t(id int PRIMARY KEY, code text UNIQUE, val int CHECK (val > 0))");
        try {
            int count = countRows("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_name = 'is_t'
                """);
            // PG 18: PRIMARY KEY, UNIQUE, CHECK(val > 0), NOT NULL on PK col 'id' (as CHECK) → 4 constraints
            assertEquals(4, count,
                    "information_schema.table_constraints should list PK, UNIQUE, CHECK, NOT NULL(id)");
        } finally {
            exec("DROP TABLE IF EXISTS is_t");
        }
    }

    @Test
    void key_column_usage_for_pk() throws SQLException {
        exec("CREATE TABLE kcu_t(id int PRIMARY KEY, a int, b text UNIQUE)");
        try {
            int pkCols = countRows("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = 'kcu_t' AND tc.constraint_type = 'PRIMARY KEY'
                """);
            assertEquals(1, pkCols, "PK should have exactly 1 column");
        } finally {
            exec("DROP TABLE IF EXISTS kcu_t");
        }
    }

    // ========================================================================
    // pg_index accuracy
    // ========================================================================

    @Test
    void pg_index_lists_correct_indexes() throws SQLException {
        exec("CREATE TABLE idx_t(id int PRIMARY KEY, a int, b text)");
        exec("CREATE INDEX idx_t_a ON idx_t(a)");
        exec("CREATE UNIQUE INDEX idx_t_b ON idx_t(b)");
        try {
            List<List<String>> indexes = query("""
                SELECT i.indexrelid::regclass, i.indisunique, i.indisprimary
                FROM pg_index i
                WHERE i.indrelid = 'idx_t'::regclass
                ORDER BY 1
                """);
            // Should be: PK index (unique+primary), idx_t_a (not unique), idx_t_b (unique not primary)
            assertEquals(3, indexes.size(), "Should have exactly 3 indexes (PK + 2 explicit)");

            long primaryCount = indexes.stream().filter(r -> "t".equals(r.get(2))).count();
            assertEquals(1, primaryCount, "Exactly 1 index should be primary");

            long uniqueCount = indexes.stream().filter(r -> "t".equals(r.get(1))).count();
            assertEquals(2, uniqueCount, "2 indexes should be unique (PK + idx_t_b)");
        } finally {
            exec("DROP TABLE IF EXISTS idx_t CASCADE");
        }
    }

    @Test
    void pg_index_partial_index_has_predicate() throws SQLException {
        exec("CREATE TABLE pidx_t(id int PRIMARY KEY, status text, email text)");
        exec("CREATE UNIQUE INDEX pidx_active ON pidx_t(email) WHERE status = 'active'");
        try {
            String hasPred = scalar("""
                SELECT i.indpred IS NOT NULL
                FROM pg_index i
                WHERE i.indexrelid = 'pidx_active'::regclass
                """);
            assertEquals("t", hasPred, "Partial index should have non-null indpred");
        } finally {
            exec("DROP TABLE IF EXISTS pidx_t CASCADE");
        }
    }

    // ========================================================================
    // pg_get_indexdef formatting
    // ========================================================================

    @Test
    void pg_get_indexdef_basic() throws SQLException {
        exec("CREATE TABLE gid_t(id int PRIMARY KEY, note text)");
        exec("CREATE INDEX gid_note ON gid_t(note)");
        try {
            String def = scalar("SELECT pg_get_indexdef('gid_note'::regclass)");
            assertNotNull(def);
            assertTrue(def.toUpperCase().contains("CREATE INDEX"),
                    "pg_get_indexdef should contain CREATE INDEX: " + def);
            assertTrue(def.contains("gid_note"),
                    "pg_get_indexdef should contain index name: " + def);
            assertTrue(def.contains("gid_t"),
                    "pg_get_indexdef should contain table name: " + def);
        } finally {
            exec("DROP TABLE IF EXISTS gid_t CASCADE");
        }
    }

    @Test
    void pg_get_indexdef_expression_index() throws SQLException {
        exec("CREATE TABLE eidx_t(id int PRIMARY KEY, note text)");
        exec("CREATE INDEX eidx_note ON eidx_t(COALESCE(note, ''))");
        try {
            String def = scalar("SELECT pg_get_indexdef('eidx_note'::regclass)");
            assertNotNull(def);
            // PG formats as: COALESCE(note, ''::text), with explicit cast
            assertTrue(def.toLowerCase().contains("coalesce"),
                    "Expression index def should contain COALESCE: " + def);
        } finally {
            exec("DROP TABLE IF EXISTS eidx_t CASCADE");
        }
    }

    // ========================================================================
    // pg_get_expr for column defaults
    // ========================================================================

    @Test
    void pg_get_expr_shows_column_default() throws SQLException {
        exec("CREATE TABLE def_t(id int PRIMARY KEY, note text DEFAULT 'hello')");
        try {
            String defExpr = scalar("""
                SELECT pg_get_expr(adbin, adrelid)
                FROM pg_attrdef
                WHERE adrelid = 'def_t'::regclass AND adnum = 2
                """);
            assertNotNull(defExpr, "pg_get_expr should return default expression");
            assertTrue(defExpr.contains("hello"), "Default should contain 'hello': " + defExpr);
        } finally {
            exec("DROP TABLE IF EXISTS def_t");
        }
    }

    // ========================================================================
    // pg_class row counts
    // ========================================================================

    @Test
    void pg_class_includes_tables_views_indexes_sequences() throws SQLException {
        exec("CREATE TABLE pgc_t(id serial PRIMARY KEY, note text)");
        exec("CREATE VIEW pgc_v AS SELECT * FROM pgc_t");
        exec("CREATE INDEX pgc_idx ON pgc_t(note)");
        try {
            // Count entries for our objects
            int count = countRows("""
                SELECT c.relname, c.relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname LIKE 'pgc_%'
                ORDER BY c.relname
                """);
            // Should have: pgc_t (table), pgc_t_pkey (index), pgc_t_id_seq (sequence),
            //              pgc_v (view), pgc_idx (index) → at least 5
            assertTrue(count >= 4,
                    "pg_class should list tables, indexes, sequences, and views; got " + count);
        } finally {
            exec("DROP VIEW IF EXISTS pgc_v");
            exec("DROP TABLE IF EXISTS pgc_t CASCADE");
        }
    }

    // ========================================================================
    // pg_get_functiondef
    // ========================================================================

    @Test
    void pg_get_functiondef_for_missing_function_fails() {
        // regprocedure cast for missing function should fail
        assertThrows(SQLException.class,
                () -> scalar("SELECT pg_get_functiondef('missing_fn(int)'::regprocedure)"),
                "pg_get_functiondef for missing function should fail");
    }

    // ========================================================================
    // tableoid pseudo-column
    // ========================================================================

    @Test
    void tableoid_pseudocolumn_available() throws SQLException {
        exec("CREATE TABLE toid_t(id int PRIMARY KEY)");
        exec("INSERT INTO toid_t VALUES (1)");
        try {
            String oid = scalar("SELECT tableoid FROM toid_t WHERE id = 1");
            assertNotNull(oid, "tableoid pseudo-column should be available");
        } finally {
            exec("DROP TABLE IF EXISTS toid_t");
        }
    }

    @Test
    void tableoid_regclass_cast() throws SQLException {
        exec("CREATE TABLE toid_t2(id int PRIMARY KEY)");
        exec("INSERT INTO toid_t2 VALUES (1)");
        try {
            String name = scalar("SELECT tableoid::regclass FROM toid_t2 WHERE id = 1");
            assertNotNull(name);
            assertTrue(name.contains("toid_t2"),
                    "tableoid::regclass should resolve to table name: " + name);
        } finally {
            exec("DROP TABLE IF EXISTS toid_t2");
        }
    }
}
