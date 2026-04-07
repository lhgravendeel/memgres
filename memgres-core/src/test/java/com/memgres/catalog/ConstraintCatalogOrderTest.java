package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diffs #31, #32: smoke_a constraint ordering; PK NOT NULL should appear, UNIQUE INDEX should not
 *                 appear in info_schema.table_constraints (only in pg_index).
 * Diff #33: patch_t constraint count (PG=4, memgres=3).
 */
class ConstraintCatalogOrderTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }
    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData(); int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) { List<String> row = new ArrayList<>(); for (int i = 1; i <= cols; i++) row.add(rs.getString(i)); rows.add(row); }
            return rows;
        }
    }
    static int countRows(String sql) throws SQLException { try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) { int n = 0; while (rs.next()) n++; return n; } }

    // Diff #31: info_schema.table_constraints for smoke_a
    // PG: smoke_a_id_not_null(CHECK), smoke_a_note_not_null(CHECK), smoke_a_pkey(PK), 3 entries total
    // UNIQUE INDEX should NOT appear here (only ADD CONSTRAINT UNIQUE does)
    @Test void smoke_a_info_schema_constraint_content() throws SQLException {
        exec("CREATE SCHEMA co_test"); exec("SET search_path = co_test");
        exec("CREATE TABLE smoke_a(id int PRIMARY KEY, note text NOT NULL DEFAULT 'x', qty int, created_at timestamptz DEFAULT CURRENT_TIMESTAMP)");
        exec("CREATE UNIQUE INDEX smoke_a_note_idx ON smoke_a(note)");
        try {
            List<List<String>> cons = query("""
                SELECT constraint_name, constraint_type
                FROM information_schema.table_constraints
                WHERE table_schema = 'co_test' AND table_name = 'smoke_a'
                ORDER BY constraint_name
                """);
            // PG returns 3 rows: smoke_a_id_not_null(CHECK), smoke_a_note_not_null(CHECK), smoke_a_pkey(PRIMARY KEY)
            // A standalone CREATE UNIQUE INDEX should NOT appear in table_constraints
            boolean hasUniqueFromIndex = cons.stream().anyMatch(r -> "smoke_a_note_idx".equals(r.get(0)) && "UNIQUE".equals(r.get(1)));
            assertFalse(hasUniqueFromIndex,
                "CREATE UNIQUE INDEX should NOT appear in information_schema.table_constraints (only ADD CONSTRAINT UNIQUE does)");

            // PK NOT NULL should appear as CHECK
            boolean hasIdNotNull = cons.stream().anyMatch(r -> r.get(0).contains("id_not_null") && "CHECK".equals(r.get(1)));
            assertTrue(hasIdNotNull,
                "PK column 'id' should have a NOT NULL (CHECK) entry in table_constraints, got: " + cons);
        } finally {
            exec("DROP SCHEMA co_test CASCADE"); exec("SET search_path = public");
        }
    }

    // Diff #32: pg_constraint for smoke_a
    // PG: smoke_a_id_not_null(n), smoke_a_note_not_null(n), smoke_a_pkey(p), sorted by conname
    @Test void smoke_a_pg_constraint_content() throws SQLException {
        exec("CREATE SCHEMA co_test2"); exec("SET search_path = co_test2");
        exec("CREATE TABLE smoke_a(id int PRIMARY KEY, note text NOT NULL DEFAULT 'x', qty int)");
        exec("CREATE UNIQUE INDEX smoke_a_note_idx ON smoke_a(note)");
        try {
            List<List<String>> cons = query("""
                SELECT conname, contype FROM pg_constraint
                WHERE conrelid = 'co_test2.smoke_a'::regclass ORDER BY conname
                """);
            // First row alphabetically should be smoke_a_id_not_null with contype=n
            assertFalse(cons.isEmpty(), "Should have constraints");
            // Check that id_not_null exists
            boolean hasIdNotNull = cons.stream().anyMatch(r -> r.get(0).contains("id_not_null") && "n".equals(r.get(1)));
            assertTrue(hasIdNotNull, "PK column 'id' should have NOT NULL constraint (n) in pg_constraint, got: " + cons);
            // UNIQUE INDEX should NOT appear in pg_constraint (only ADD CONSTRAINT creates a pg_constraint entry)
            boolean hasUniqueFromIndex = cons.stream().anyMatch(r -> "smoke_a_note_idx".equals(r.get(0)) && "u".equals(r.get(1)));
            assertFalse(hasUniqueFromIndex,
                "Standalone UNIQUE INDEX should NOT appear in pg_constraint, got: " + cons);
        } finally {
            exec("DROP SCHEMA co_test2 CASCADE"); exec("SET search_path = public");
        }
    }

    // Diff #33: patch_t constraint count = 4
    @Test void migration_pattern_constraint_count() throws SQLException {
        exec("CREATE SCHEMA co_test3"); exec("SET search_path = co_test3");
        exec("CREATE TABLE patch_t(id serial PRIMARY KEY, code text NOT NULL, qty text NOT NULL, created_on text NOT NULL)");
        exec("ALTER TABLE patch_t ADD COLUMN IF NOT EXISTS active boolean DEFAULT true");
        exec("ALTER TABLE patch_t RENAME COLUMN code TO ext_code");
        exec("ALTER TABLE patch_t ALTER COLUMN qty TYPE int USING qty::int");
        exec("CREATE UNIQUE INDEX patch_ext_code_idx ON patch_t(ext_code)");
        exec("ALTER TABLE patch_t ADD CONSTRAINT patch_ext_code_uq UNIQUE USING INDEX patch_ext_code_idx");
        try {
            List<List<String>> cons = query("SELECT conname, contype FROM pg_constraint WHERE conrelid = 'co_test3.patch_t'::regclass ORDER BY conname");
            // PG 18 returns 5: patch_ext_code_uq(u) + patch_t_created_on_not_null(n) + patch_t_id_not_null(n) + patch_t_pkey(p) + patch_t_qty_not_null(n)
            assertEquals(5, cons.size(), "Should have exactly 5 constraints, got " + cons.size() + ": " + cons);
        } finally {
            exec("DROP SCHEMA co_test3 CASCADE"); exec("SET search_path = public");
        }
    }
}
