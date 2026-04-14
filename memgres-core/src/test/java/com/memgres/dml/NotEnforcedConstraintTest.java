package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for NOT ENFORCED constraints (PG 18).
 * CHECK and FOREIGN KEY constraints can be declared NOT ENFORCED —
 * they are stored in the catalog but not validated at DML time.
 */
class NotEnforcedConstraintTest {

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

    @BeforeEach
    void cleanUp() throws SQLException {
        for (String t : List.of("child", "parent", "t1", "t2", "t3")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
    }

    @AfterEach
    void cleanUpAfter() throws SQLException {
        for (String t : List.of("child", "parent", "t1", "t2", "t3")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
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

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // EXCLUDE constraint NOT ENFORCED — rejected by PG 18 with 0A000
    // ========================================================================

    @Test
    void exclude_not_enforced_is_rejected() {
        // PG 18: "EXCLUDE constraints cannot be marked NOT ENFORCED"
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE t1(id int, room int, during tsrange, " +
                     "CONSTRAINT excl_room EXCLUDE USING gist (room WITH =, during WITH &&) NOT ENFORCED)"));
        assertEquals("0A000", ex.getSQLState());
        assertTrue(ex.getMessage().toLowerCase().contains("exclude"));
    }

    @Test
    void exclude_without_not_enforced_is_accepted() throws SQLException {
        // Plain EXCLUDE (without NOT ENFORCED) should still be accepted (as DDL stub)
        exec("CREATE TABLE t1(id int, room int, during tsrange, " +
             "CONSTRAINT excl_room EXCLUDE USING gist (room WITH =, during WITH &&))");
    }

    // ========================================================================
    // CHECK constraint NOT ENFORCED — table level
    // ========================================================================

    @Test
    void check_not_enforced_allows_violating_insert() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CHECK (val > 0) NOT ENFORCED)");
        // Should succeed — constraint is not enforced
        exec("INSERT INTO t1 VALUES (1, -5)");
        assertEquals("-5", scalar("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void check_enforced_rejects_violating_insert() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CHECK (val > 0))");
        // Default is ENFORCED — should reject
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (1, -5)"));
        assertEquals("23514", ex.getSQLState());
    }

    @Test
    void check_explicit_enforced_rejects_violating_insert() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CHECK (val > 0) ENFORCED)");
        // Explicit ENFORCED — should reject
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (1, -5)"));
        assertEquals("23514", ex.getSQLState());
    }

    @Test
    void check_not_enforced_allows_violating_update() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CHECK (val > 0) NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, 10)");
        exec("UPDATE t1 SET val = -99 WHERE id = 1");
        assertEquals("-99", scalar("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void check_not_enforced_table_level_constraint() throws SQLException {
        exec("CREATE TABLE t1(id int, val int, CONSTRAINT chk_positive CHECK (val > 0) NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, -100)");
        assertEquals("-100", scalar("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void check_not_enforced_named_column_constraint() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk_val CHECK (val > 0) NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, -1)");
        assertEquals("-1", scalar("SELECT val FROM t1 WHERE id = 1"));
    }

    // ========================================================================
    // FK constraint NOT ENFORCED — table level
    // ========================================================================

    @Test
    void fk_not_enforced_allows_orphan_insert() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        // No parent row exists — should succeed
        exec("INSERT INTO child VALUES (1, 999)");
        assertEquals("999", scalar("SELECT parent_id FROM child WHERE id = 1"));
    }

    @Test
    void fk_enforced_rejects_orphan_insert() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id))");
        // Default ENFORCED — should reject
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO child VALUES (1, 999)"));
        assertEquals("23503", ex.getSQLState());
    }

    @Test
    void fk_not_enforced_allows_orphan_update() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("INSERT INTO parent VALUES (1)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("UPDATE child SET parent_id = 999 WHERE id = 1");
        assertEquals("999", scalar("SELECT parent_id FROM child WHERE id = 1"));
    }

    @Test
    void fk_not_enforced_no_cascade_on_parent_delete() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("INSERT INTO parent VALUES (1)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE NOT ENFORCED)");
        exec("INSERT INTO child VALUES (1, 1)");
        // Delete parent — NOT ENFORCED means no cascade/restrict check
        exec("DELETE FROM parent WHERE id = 1");
        assertEquals("0", scalar("SELECT count(*) FROM parent"));
        // Child row should still exist (no cascade since not enforced)
        assertEquals("1", scalar("SELECT count(*) FROM child"));
    }

    @Test
    void fk_not_enforced_no_restrict_on_parent_delete() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("INSERT INTO parent VALUES (1)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        exec("INSERT INTO child VALUES (1, 1)");
        // Delete parent — NOT ENFORCED means no restrict check
        exec("DELETE FROM parent WHERE id = 1");
        assertEquals("0", scalar("SELECT count(*) FROM parent"));
    }

    @Test
    void fk_not_enforced_column_level() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int REFERENCES parent(id) NOT ENFORCED)");
        exec("INSERT INTO child VALUES (1, 999)");
        assertEquals("999", scalar("SELECT parent_id FROM child WHERE id = 1"));
    }

    // ========================================================================
    // ALTER TABLE ... ALTER CONSTRAINT ... [NOT] ENFORCED
    // ========================================================================

    @Test
    void alter_constraint_to_not_enforced() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk_val CHECK (val > 0))");
        // Initially enforced — rejects violation
        assertThrows(SQLException.class, () -> exec("INSERT INTO t1 VALUES (1, -1)"));
        // Now make it not enforced
        exec("ALTER TABLE t1 ALTER CONSTRAINT chk_val NOT ENFORCED");
        // Should now accept violations
        exec("INSERT INTO t1 VALUES (1, -1)");
        assertEquals("-1", scalar("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void alter_constraint_to_enforced() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk_val CHECK (val > 0) NOT ENFORCED)");
        // Initially not enforced — allows violation
        exec("INSERT INTO t1 VALUES (1, -1)");
        // PG 18: toggling to ENFORCED validates existing data — should fail with 42809
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE t1 ALTER CONSTRAINT chk_val ENFORCED"));
        assertEquals("42809", ex.getSQLState());
    }

    @Test
    void alter_fk_constraint_to_not_enforced() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent(id))");
        // Initially enforced — rejects orphan
        assertThrows(SQLException.class, () -> exec("INSERT INTO child VALUES (1, 999)"));
        // Now make it not enforced
        exec("ALTER TABLE child ALTER CONSTRAINT fk_parent NOT ENFORCED");
        // Should now accept orphan
        exec("INSERT INTO child VALUES (1, 999)");
        assertEquals("999", scalar("SELECT parent_id FROM child WHERE id = 1"));
    }

    @Test
    void alter_nonexistent_constraint_errors() {
        assertDoesNotThrow(() -> exec("CREATE TABLE t1(id int PRIMARY KEY)"));
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE t1 ALTER CONSTRAINT bogus NOT ENFORCED"));
        assertEquals("42704", ex.getSQLState());
    }

    // ========================================================================
    // ALTER TABLE ADD CONSTRAINT ... NOT ENFORCED
    // ========================================================================

    @Test
    void add_check_constraint_not_enforced() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, -5)");
        // Add NOT ENFORCED constraint — should succeed even with existing violating data
        exec("ALTER TABLE t1 ADD CONSTRAINT chk_pos CHECK (val > 0) NOT ENFORCED");
        // More violations still allowed
        exec("INSERT INTO t1 VALUES (2, -10)");
        assertEquals("2", scalar("SELECT count(*) FROM t1"));
    }

    @Test
    void add_fk_constraint_not_enforced() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int)");
        exec("INSERT INTO child VALUES (1, 999)"); // orphan row
        // Add NOT ENFORCED FK — should succeed even with existing orphan
        exec("ALTER TABLE child ADD CONSTRAINT fk_par FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED");
        // More orphans still allowed
        exec("INSERT INTO child VALUES (2, 888)");
        assertEquals("2", scalar("SELECT count(*) FROM child"));
    }

    // ========================================================================
    // Catalog: pg_constraint.conenforced
    // ========================================================================

    @Test
    void pg_constraint_conenforced_true_by_default() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk CHECK (val > 0))");
        assertEquals("t", scalar(
                "SELECT conenforced FROM pg_constraint WHERE conname = 'chk'"));
    }

    @Test
    void pg_constraint_conenforced_false_when_not_enforced() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk CHECK (val > 0) NOT ENFORCED)");
        assertEquals("f", scalar(
                "SELECT conenforced FROM pg_constraint WHERE conname = 'chk'"));
    }

    @Test
    void pg_constraint_conenforced_updates_on_alter() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk CHECK (val > 0))");
        assertEquals("t", scalar(
                "SELECT conenforced FROM pg_constraint WHERE conname = 'chk'"));
        exec("ALTER TABLE t1 ALTER CONSTRAINT chk NOT ENFORCED");
        assertEquals("f", scalar(
                "SELECT conenforced FROM pg_constraint WHERE conname = 'chk'"));
        exec("ALTER TABLE t1 ALTER CONSTRAINT chk ENFORCED");
        assertEquals("t", scalar(
                "SELECT conenforced FROM pg_constraint WHERE conname = 'chk'"));
    }

    @Test
    void pg_constraint_conenforced_fk() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "CONSTRAINT fk_par FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        assertEquals("f", scalar(
                "SELECT conenforced FROM pg_constraint WHERE conname = 'fk_par'"));
    }

    // ========================================================================
    // Catalog: information_schema.table_constraints.enforced
    // ========================================================================

    @Test
    void info_schema_enforced_yes_by_default() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk CHECK (val > 0))");
        assertEquals("YES", scalar(
                "SELECT enforced FROM information_schema.table_constraints WHERE constraint_name = 'chk'"));
    }

    @Test
    void info_schema_enforced_no_when_not_enforced() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk CHECK (val > 0) NOT ENFORCED)");
        assertEquals("NO", scalar(
                "SELECT enforced FROM information_schema.table_constraints WHERE constraint_name = 'chk'"));
    }

    // ========================================================================
    // Mixed enforced and not-enforced constraints
    // ========================================================================

    @Test
    void mixed_enforced_and_not_enforced_check() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, " +
             "val int CHECK (val > 0) NOT ENFORCED, " +
             "status text CHECK (status IN ('active', 'inactive')))");
        // val check is not enforced — accepts negative
        exec("INSERT INTO t1 VALUES (1, -1, 'active')");
        // status check IS enforced — rejects invalid
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (2, 10, 'bogus')"));
        assertEquals("23514", ex.getSQLState());
    }

    @Test
    void not_enforced_check_with_valid_data() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CHECK (val > 0) NOT ENFORCED)");
        // Valid data still works fine
        exec("INSERT INTO t1 VALUES (1, 100)");
        assertEquals("100", scalar("SELECT val FROM t1 WHERE id = 1"));
    }

    // ========================================================================
    // NOT ENFORCED with ON DELETE/ON UPDATE actions
    // ========================================================================

    @Test
    void fk_not_enforced_on_update_cascade_no_effect() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY, val int)");
        exec("INSERT INTO parent VALUES (1, 10)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) ON UPDATE CASCADE NOT ENFORCED)");
        exec("INSERT INTO child VALUES (1, 1)");
        // Update parent PK — NOT ENFORCED means no cascade
        exec("UPDATE parent SET id = 2 WHERE id = 1");
        assertEquals("2", scalar("SELECT id FROM parent"));
        assertEquals("1", scalar("SELECT parent_id FROM child")); // not cascaded
    }

    @Test
    void fk_not_enforced_on_delete_set_null_no_effect() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("INSERT INTO parent VALUES (1)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL NOT ENFORCED)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("DELETE FROM parent WHERE id = 1");
        assertEquals("1", scalar("SELECT parent_id FROM child")); // not set to null
    }

    // ========================================================================
    // NOT ENFORCED with multiple rows
    // ========================================================================

    @Test
    void check_not_enforced_multiple_violating_rows() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CHECK (val BETWEEN 1 AND 100) NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, -50), (2, 0), (3, 200), (4, 50)");
        assertEquals("4", scalar("SELECT count(*) FROM t1"));
    }

    @Test
    void fk_not_enforced_multiple_orphan_rows() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        exec("INSERT INTO child VALUES (1, 100), (2, 200), (3, 300)");
        assertEquals("3", scalar("SELECT count(*) FROM child"));
    }

    // ========================================================================
    // NOT ENFORCED does not apply to PK/UNIQUE/NOT NULL
    // ========================================================================

    @Test
    void pk_is_always_enforced() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int)");
        exec("INSERT INTO t1 VALUES (1, 10)");
        // PK uniqueness is always enforced
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (1, 20)"));
        assertEquals("23505", ex.getSQLState());
    }

    @Test
    void unique_is_always_enforced() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int UNIQUE)");
        exec("INSERT INTO t1 VALUES (1, 10)");
        // UNIQUE is always enforced
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (2, 10)"));
        assertEquals("23505", ex.getSQLState());
    }

    @Test
    void not_null_is_always_enforced() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int NOT NULL)");
        // NOT NULL is always enforced
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1 VALUES (1, NULL)"));
        assertEquals("23502", ex.getSQLState());
    }

    // ========================================================================
    // MERGE with NOT ENFORCED constraints
    // ========================================================================

    @Test
    void merge_insert_bypasses_not_enforced_check() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CHECK (val > 0) NOT ENFORCED)");
        exec("CREATE TABLE t2(id int, val int)");
        exec("INSERT INTO t2 VALUES (1, -5)");
        exec("MERGE INTO t1 USING t2 ON t1.id = t2.id " +
             "WHEN NOT MATCHED THEN INSERT VALUES (t2.id, t2.val)");
        assertEquals("-5", scalar("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void merge_update_bypasses_not_enforced_check() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CHECK (val > 0) NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, 10)");
        exec("CREATE TABLE t2(id int, val int)");
        exec("INSERT INTO t2 VALUES (1, -99)");
        exec("MERGE INTO t1 USING t2 ON t1.id = t2.id " +
             "WHEN MATCHED THEN UPDATE SET val = t2.val");
        assertEquals("-99", scalar("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void merge_insert_bypasses_not_enforced_fk() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        exec("CREATE TABLE t2(id int, pid int)");
        exec("INSERT INTO t2 VALUES (1, 999)");
        exec("MERGE INTO child USING t2 ON child.id = t2.id " +
             "WHEN NOT MATCHED THEN INSERT VALUES (t2.id, t2.pid)");
        assertEquals("999", scalar("SELECT parent_id FROM child WHERE id = 1"));
    }

    // ========================================================================
    // ON CONFLICT DO UPDATE with NOT ENFORCED
    // ========================================================================

    @Test
    void on_conflict_update_bypasses_not_enforced_check() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CHECK (val > 0) NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, 10)");
        exec("INSERT INTO t1 VALUES (1, -5) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
        assertEquals("-5", scalar("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void on_conflict_update_bypasses_not_enforced_fk() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        exec("INSERT INTO parent VALUES (1)");
        exec("INSERT INTO child VALUES (1, 1)");
        exec("INSERT INTO child VALUES (1, 999) ON CONFLICT (id) DO UPDATE SET parent_id = EXCLUDED.parent_id");
        assertEquals("999", scalar("SELECT parent_id FROM child WHERE id = 1"));
    }

    // ========================================================================
    // DROP CONSTRAINT on NOT ENFORCED constraint
    // ========================================================================

    @Test
    void drop_not_enforced_constraint() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk CHECK (val > 0) NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, -5)");
        exec("ALTER TABLE t1 DROP CONSTRAINT chk");
        // Constraint gone — insert still works
        exec("INSERT INTO t1 VALUES (2, -10)");
        assertEquals("2", scalar("SELECT count(*) FROM t1"));
        // And it's gone from pg_constraint
        assertEquals("0", scalar(
                "SELECT count(*) FROM pg_constraint WHERE conname = 'chk'"));
    }

    @Test
    void drop_not_enforced_fk_constraint() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "CONSTRAINT fk_par FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        exec("INSERT INTO child VALUES (1, 999)");
        exec("ALTER TABLE child DROP CONSTRAINT fk_par");
        exec("INSERT INTO child VALUES (2, 888)");
        assertEquals("2", scalar("SELECT count(*) FROM child"));
    }

    // ========================================================================
    // Self-referencing FK with NOT ENFORCED
    // ========================================================================

    @Test
    void self_referencing_fk_not_enforced() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES t1(id) NOT ENFORCED)");
        // Insert with non-existent parent — allowed because NOT ENFORCED
        exec("INSERT INTO t1 VALUES (1, 999)");
        assertEquals("999", scalar("SELECT parent_id FROM t1 WHERE id = 1"));
    }

    @Test
    void self_referencing_fk_not_enforced_delete_parent() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES t1(id) ON DELETE CASCADE NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, NULL)");
        exec("INSERT INTO t1 VALUES (2, 1)");
        // Delete parent — NOT ENFORCED means no cascade
        exec("DELETE FROM t1 WHERE id = 1");
        assertEquals("1", scalar("SELECT count(*) FROM t1"));
        assertEquals("1", scalar("SELECT parent_id FROM t1 WHERE id = 2"));
    }

    // ========================================================================
    // ALTER CONSTRAINT ENFORCED with existing violations
    // ========================================================================

    @Test
    void alter_to_enforced_does_not_revalidate_existing_violations() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, val int CONSTRAINT chk CHECK (val > 0) NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, -5)");
        // PG 18: toggling to ENFORCED validates existing data — should fail with 42809
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("ALTER TABLE t1 ALTER CONSTRAINT chk ENFORCED"));
        assertEquals("42809", ex.getSQLState());
    }

    @Test
    void alter_fk_to_enforced_does_not_revalidate_existing_orphans() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "CONSTRAINT fk_par FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        exec("INSERT INTO child VALUES (1, 999)"); // orphan
        // Toggle to ENFORCED — does NOT revalidate
        exec("ALTER TABLE child ALTER CONSTRAINT fk_par ENFORCED");
        // Existing orphan remains
        assertEquals("999", scalar("SELECT parent_id FROM child WHERE id = 1"));
        // But new orphans are blocked
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO child VALUES (2, 888)"));
        assertEquals("23503", ex.getSQLState());
    }

    // ========================================================================
    // Multi-column FK NOT ENFORCED
    // ========================================================================

    @Test
    void multi_column_fk_not_enforced() throws SQLException {
        exec("CREATE TABLE parent(a int, b int, PRIMARY KEY (a, b))");
        exec("CREATE TABLE child(id int PRIMARY KEY, a int, b int, " +
             "FOREIGN KEY (a, b) REFERENCES parent(a, b) NOT ENFORCED)");
        // Insert orphan — allowed because NOT ENFORCED
        exec("INSERT INTO child VALUES (1, 10, 20)");
        assertEquals("1", scalar("SELECT count(*) FROM child"));
    }

    // ========================================================================
    // NOT ENFORCED FK with NULL values
    // ========================================================================

    @Test
    void fk_not_enforced_null_value_allowed() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("CREATE TABLE child(id int PRIMARY KEY, parent_id int, " +
             "FOREIGN KEY (parent_id) REFERENCES parent(id) NOT ENFORCED)");
        // NULL FK value is always valid (with or without enforcement)
        exec("INSERT INTO child VALUES (1, NULL)");
        assertNull(scalar("SELECT parent_id FROM child WHERE id = 1"));
    }

    // ========================================================================
    // NOT ENFORCED with DELETE parent having enforced FK on same table
    // ========================================================================

    @Test
    void mixed_enforced_fk_parent_delete() throws SQLException {
        exec("CREATE TABLE parent(id int PRIMARY KEY)");
        exec("INSERT INTO parent VALUES (1), (2)");
        // t1 has enforced FK, t2 has NOT ENFORCED FK — both reference parent
        exec("CREATE TABLE t1(id int PRIMARY KEY, pid int, " +
             "FOREIGN KEY (pid) REFERENCES parent(id))");
        exec("CREATE TABLE t2(id int PRIMARY KEY, pid int, " +
             "FOREIGN KEY (pid) REFERENCES parent(id) NOT ENFORCED)");
        exec("INSERT INTO t1 VALUES (1, 1)");
        exec("INSERT INTO t2 VALUES (1, 2)");
        // Delete parent 2 — t2's FK is not enforced, so no block
        exec("DELETE FROM parent WHERE id = 2");
        assertEquals("1", scalar("SELECT count(*) FROM parent"));
        // Delete parent 1 — t1's FK IS enforced, so blocked
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("DELETE FROM parent WHERE id = 1"));
        assertEquals("23503", ex.getSQLState());
    }
}
