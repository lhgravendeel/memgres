package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 9, 21, 26 (Java/JDBC): Migration-tool behavior and schema evolution.
 * Tests idempotent DDL (IF NOT EXISTS / IF EXISTS), transaction-wrapped migrations,
 * ALTER TABLE variants (add/drop/rename column, rename table, alter type),
 * constraint validation, enum expansion, schema creation, and migration metadata patterns.
 */
class MigrationDdlTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }
    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // --- 1. Idempotent DDL: CREATE TABLE IF NOT EXISTS ---

    @Test void create_table_if_not_exists_is_idempotent() throws Exception {
        exec("CREATE TABLE IF NOT EXISTS mig_idem1(id int PRIMARY KEY, name text)");
        // Running it again should not throw
        exec("CREATE TABLE IF NOT EXISTS mig_idem1(id int PRIMARY KEY, name text)");
        exec("INSERT INTO mig_idem1 VALUES (1, 'a')");
        assertEquals("1", scalar("SELECT count(*) FROM mig_idem1"));
        exec("DROP TABLE mig_idem1");
    }

    // --- 2. Idempotent DDL: CREATE INDEX IF NOT EXISTS ---

    @Test void create_index_if_not_exists_is_idempotent() throws Exception {
        exec("CREATE TABLE mig_idem2(id int PRIMARY KEY, val text)");
        exec("CREATE INDEX IF NOT EXISTS idx_mig_idem2_val ON mig_idem2(val)");
        // Running it again should not throw
        exec("CREATE INDEX IF NOT EXISTS idx_mig_idem2_val ON mig_idem2(val)");
        // Verify the index exists
        String cnt = scalar("SELECT count(*) FROM pg_indexes WHERE indexname = 'idx_mig_idem2_val'");
        assertEquals("1", cnt);
        exec("DROP TABLE mig_idem2");
    }

    // --- 3. Idempotent DDL: DROP TABLE IF EXISTS ---

    @Test void drop_table_if_exists_is_idempotent() throws Exception {
        exec("CREATE TABLE mig_idem3(id int)");
        exec("DROP TABLE IF EXISTS mig_idem3");
        // Running it again on a non-existent table should not throw
        exec("DROP TABLE IF EXISTS mig_idem3");
    }

    // --- 4. Transaction-wrapped migration: DDL + DML commit ---

    @Test void transaction_wrapped_ddl_dml_commit() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CREATE TABLE mig_txc(id serial PRIMARY KEY, label text NOT NULL)");
            exec("INSERT INTO mig_txc(label) VALUES ('row1')");
            exec("INSERT INTO mig_txc(label) VALUES ('row2')");
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals("2", scalar("SELECT count(*) FROM mig_txc"));
        exec("DROP TABLE mig_txc");
    }

    // --- 5. Transaction-wrapped migration: DDL + DML rollback ---

    @Test void transaction_wrapped_ddl_dml_rollback() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("CREATE TABLE mig_txr(id int PRIMARY KEY)");
            exec("INSERT INTO mig_txr VALUES (1)");
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
        // Table should not exist after rollback
        assertThrows(SQLException.class, () -> scalar("SELECT count(*) FROM mig_txr"));
    }

    // --- 6. ALTER TABLE ADD COLUMN with default value ---

    @Test void alter_table_add_column_with_default() throws Exception {
        exec("CREATE TABLE mig_addcol1(id int PRIMARY KEY)");
        exec("INSERT INTO mig_addcol1 VALUES (1), (2)");
        exec("ALTER TABLE mig_addcol1 ADD COLUMN status text DEFAULT 'active'");
        // Existing rows should have the default value
        assertEquals("2", scalar("SELECT count(*) FROM mig_addcol1 WHERE status = 'active'"));
        exec("DROP TABLE mig_addcol1");
    }

    // --- 7. ALTER TABLE ADD COLUMN with NOT NULL and default ---

    @Test void alter_table_add_column_not_null_with_default() throws Exception {
        exec("CREATE TABLE mig_addcol2(id int PRIMARY KEY)");
        exec("INSERT INTO mig_addcol2 VALUES (1)");
        exec("ALTER TABLE mig_addcol2 ADD COLUMN priority int NOT NULL DEFAULT 0");
        assertEquals("0", scalar("SELECT priority FROM mig_addcol2 WHERE id = 1"));
        // Inserting without providing the column should use the default
        exec("INSERT INTO mig_addcol2(id) VALUES (2)");
        assertEquals("0", scalar("SELECT priority FROM mig_addcol2 WHERE id = 2"));
        exec("DROP TABLE mig_addcol2");
    }

    // --- 8. ALTER TABLE DROP COLUMN ---

    @Test void alter_table_drop_column() throws Exception {
        exec("CREATE TABLE mig_dropcol(id int PRIMARY KEY, old_col text, keep_col int)");
        exec("INSERT INTO mig_dropcol VALUES (1, 'gone', 42)");
        exec("ALTER TABLE mig_dropcol DROP COLUMN old_col");
        // Verify the column is gone
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM mig_dropcol WHERE id = 1")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(2, md.getColumnCount());
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("keep_col"));
        }
        exec("DROP TABLE mig_dropcol");
    }

    // --- 9. ALTER TABLE ALTER COLUMN TYPE (varchar to text) ---

    @Test void alter_column_type_varchar_to_text() throws Exception {
        exec("CREATE TABLE mig_alttype(id int PRIMARY KEY, name varchar(50))");
        exec("INSERT INTO mig_alttype VALUES (1, 'hello')");
        exec("ALTER TABLE mig_alttype ALTER COLUMN name TYPE text");
        // Data should be preserved
        assertEquals("hello", scalar("SELECT name FROM mig_alttype WHERE id = 1"));
        // Verify the new type via information_schema
        String dataType = scalar(
                "SELECT data_type FROM information_schema.columns " +
                "WHERE table_name = 'mig_alttype' AND column_name = 'name'");
        assertEquals("text", dataType);
        exec("DROP TABLE mig_alttype");
    }

    // --- 10. ALTER TABLE RENAME COLUMN ---

    @Test void alter_table_rename_column() throws Exception {
        exec("CREATE TABLE mig_rencol(id int PRIMARY KEY, old_name text)");
        exec("INSERT INTO mig_rencol VALUES (1, 'value')");
        exec("ALTER TABLE mig_rencol RENAME COLUMN old_name TO new_name");
        assertEquals("value", scalar("SELECT new_name FROM mig_rencol WHERE id = 1"));
        // Old name should fail
        assertThrows(SQLException.class, () -> scalar("SELECT old_name FROM mig_rencol"));
        exec("DROP TABLE mig_rencol");
    }

    // --- 11. ALTER TABLE RENAME TO ---

    @Test void alter_table_rename_to() throws Exception {
        exec("CREATE TABLE mig_rentbl(id int PRIMARY KEY, val text)");
        exec("INSERT INTO mig_rentbl VALUES (1, 'data')");
        exec("ALTER TABLE mig_rentbl RENAME TO mig_rentbl_new");
        assertEquals("data", scalar("SELECT val FROM mig_rentbl_new WHERE id = 1"));
        // Old name should fail
        assertThrows(SQLException.class, () -> scalar("SELECT * FROM mig_rentbl"));
        exec("DROP TABLE mig_rentbl_new");
    }

    // --- 12. ADD CONSTRAINT with NOT VALID then VALIDATE ---

    @Test void add_constraint_not_valid_then_validate() throws Exception {
        exec("CREATE TABLE mig_constr(id int PRIMARY KEY, amount int)");
        exec("INSERT INTO mig_constr VALUES (1, 10), (2, 20)");
        // Add constraint NOT VALID (does not check existing rows)
        exec("ALTER TABLE mig_constr ADD CONSTRAINT chk_positive CHECK (amount > 0) NOT VALID");
        // New inserts must respect the constraint
        assertThrows(SQLException.class, () -> exec("INSERT INTO mig_constr VALUES (3, -5)"));
        // Validate the constraint against existing rows
        exec("ALTER TABLE mig_constr VALIDATE CONSTRAINT chk_positive");
        // Constraint is now fully validated; verify it still blocks bad inserts
        assertThrows(SQLException.class, () -> exec("INSERT INTO mig_constr VALUES (4, -1)"));
        exec("DROP TABLE mig_constr");
    }

    // --- 13. Backfill data then SET NOT NULL ---

    @Test void backfill_then_set_not_null() throws Exception {
        exec("CREATE TABLE mig_backfill(id int PRIMARY KEY, category text)");
        exec("INSERT INTO mig_backfill VALUES (1, NULL), (2, NULL), (3, 'preset')");
        // Backfill nulls
        exec("UPDATE mig_backfill SET category = 'default' WHERE category IS NULL");
        // Now safe to set NOT NULL
        exec("ALTER TABLE mig_backfill ALTER COLUMN category SET NOT NULL");
        // Verify NULL insert is rejected
        assertThrows(SQLException.class, () -> exec("INSERT INTO mig_backfill VALUES (4, NULL)"));
        assertEquals("3", scalar("SELECT count(*) FROM mig_backfill"));
        exec("DROP TABLE mig_backfill");
    }

    // --- 14. Failed ALTER TABLE rollback leaves table unchanged ---

    @Test void failed_alter_table_rollback_leaves_table_unchanged() throws Exception {
        exec("CREATE TABLE mig_failalt(id int PRIMARY KEY, val text)");
        exec("INSERT INTO mig_failalt VALUES (1, 'original')");
        conn.setAutoCommit(false);
        try {
            exec("ALTER TABLE mig_failalt ADD COLUMN extra int NOT NULL DEFAULT 0");
            // Force a failure by violating a constraint on the same table
            exec("INSERT INTO mig_failalt VALUES (1, 'dup')"); // duplicate PK
            conn.commit();
            fail("Should have thrown due to duplicate key");
        } catch (SQLException e) {
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
        // Table should still have only 2 columns (id, val)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM mig_failalt")) {
            assertEquals(2, rs.getMetaData().getColumnCount(), "Rolled-back ALTER should leave table unchanged");
        }
        exec("DROP TABLE mig_failalt");
    }

    // --- 15. Migration metadata table pattern ---

    @Test void migration_metadata_table_pattern() throws Exception {
        exec("CREATE TABLE IF NOT EXISTS mig_schema_version(" +
             "version int PRIMARY KEY, " +
             "description text NOT NULL, " +
             "applied_at timestamp NOT NULL DEFAULT now(), " +
             "checksum text)");
        // Simulate applying two migrations
        exec("INSERT INTO mig_schema_version(version, description) VALUES (1, 'create users table')");
        exec("INSERT INTO mig_schema_version(version, description) VALUES (2, 'add email column')");
        // Idempotent: running CREATE IF NOT EXISTS again should not lose data
        exec("CREATE TABLE IF NOT EXISTS mig_schema_version(" +
             "version int PRIMARY KEY, " +
             "description text NOT NULL, " +
             "applied_at timestamp NOT NULL DEFAULT now(), " +
             "checksum text)");
        assertEquals("2", scalar("SELECT count(*) FROM mig_schema_version"));
        assertEquals("2", scalar("SELECT max(version) FROM mig_schema_version"));
        exec("DROP TABLE mig_schema_version");
    }

    // --- 16. Enum type expansion: ALTER TYPE ... ADD VALUE ---

    @Test void enum_type_add_value() throws Exception {
        exec("CREATE TYPE mig_status AS ENUM ('draft', 'published')");
        exec("CREATE TABLE mig_enum_tbl(id int PRIMARY KEY, status mig_status)");
        exec("INSERT INTO mig_enum_tbl VALUES (1, 'draft')");
        // Expand the enum
        exec("ALTER TYPE mig_status ADD VALUE 'archived'");
        exec("INSERT INTO mig_enum_tbl VALUES (2, 'archived')");
        assertEquals("archived", scalar("SELECT status FROM mig_enum_tbl WHERE id = 2"));
        // Add with positioning
        exec("ALTER TYPE mig_status ADD VALUE IF NOT EXISTS 'review' BEFORE 'published'");
        exec("INSERT INTO mig_enum_tbl VALUES (3, 'review')");
        assertEquals("review", scalar("SELECT status FROM mig_enum_tbl WHERE id = 3"));
        exec("DROP TABLE mig_enum_tbl");
        exec("DROP TYPE mig_status");
    }

    // --- 17. Schema creation: CREATE SCHEMA IF NOT EXISTS ---

    @Test void create_schema_if_not_exists() throws Exception {
        exec("CREATE SCHEMA IF NOT EXISTS mig_app_schema");
        // Idempotent
        exec("CREATE SCHEMA IF NOT EXISTS mig_app_schema");
        exec("CREATE TABLE mig_app_schema.mig_schematbl(id int PRIMARY KEY)");
        exec("INSERT INTO mig_app_schema.mig_schematbl VALUES (1)");
        assertEquals("1", scalar("SELECT count(*) FROM mig_app_schema.mig_schematbl"));
        exec("DROP TABLE mig_app_schema.mig_schematbl");
        exec("DROP SCHEMA mig_app_schema");
    }

    // --- 18. Multiple ALTER TABLE in one migration transaction ---

    @Test void multiple_alter_table_in_one_transaction() throws Exception {
        exec("CREATE TABLE mig_multi(id int PRIMARY KEY, a text, b text)");
        exec("INSERT INTO mig_multi VALUES (1, 'x', 'y')");
        conn.setAutoCommit(false);
        try {
            exec("ALTER TABLE mig_multi ADD COLUMN c int DEFAULT 0");
            exec("ALTER TABLE mig_multi DROP COLUMN b");
            exec("ALTER TABLE mig_multi RENAME COLUMN a TO label");
            exec("ALTER TABLE mig_multi ALTER COLUMN c SET NOT NULL");
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        // Verify all changes applied atomically
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM mig_multi WHERE id = 1")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals(3, md.getColumnCount());
            // Columns should be id, label, c
            assertEquals("id", md.getColumnName(1));
            assertEquals("label", md.getColumnName(2));
            assertEquals("c", md.getColumnName(3));
            assertTrue(rs.next());
            assertEquals("x", rs.getString("label"));
            assertEquals(0, rs.getInt("c"));
        }
        exec("DROP TABLE mig_multi");
    }

    // --- 19. Column type conversion: text to integer with USING clause ---

    @Test void alter_column_type_text_to_integer_using() throws Exception {
        exec("CREATE TABLE mig_typeconv(id int PRIMARY KEY, num_str text)");
        exec("INSERT INTO mig_typeconv VALUES (1, '42'), (2, '100'), (3, '0')");
        exec("ALTER TABLE mig_typeconv ALTER COLUMN num_str TYPE integer USING num_str::integer");
        // Verify the conversion
        assertEquals("42", scalar("SELECT num_str FROM mig_typeconv WHERE id = 1"));
        // Verify the column type changed
        String dataType = scalar(
                "SELECT data_type FROM information_schema.columns " +
                "WHERE table_name = 'mig_typeconv' AND column_name = 'num_str'");
        assertEquals("integer", dataType);
        // Verify arithmetic works on the converted column
        assertEquals("142", scalar("SELECT sum(num_str) FROM mig_typeconv"));
        exec("DROP TABLE mig_typeconv");
    }
}
