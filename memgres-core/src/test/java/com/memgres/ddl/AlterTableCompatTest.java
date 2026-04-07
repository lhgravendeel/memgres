package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ALTER TABLE syntax variants found in real-world schemas.
 */
class AlterTableCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // ADD COLUMN IF NOT EXISTS
    // =========================================================================

    @Test
    void testAddColumnIfNotExists() throws SQLException {
        exec("CREATE TABLE acine_test (id serial PRIMARY KEY, name text)");
        exec("ALTER TABLE acine_test ADD COLUMN IF NOT EXISTS email text");
        exec("INSERT INTO acine_test (name, email) VALUES ('Alice', 'alice@test.com')");
        assertEquals("alice@test.com", query1("SELECT email FROM acine_test WHERE name = 'Alice'"));
    }

    @Test
    void testAddColumnIfNotExistsAlreadyPresent() throws SQLException {
        exec("CREATE TABLE acine_test2 (id serial PRIMARY KEY, name text)");
        // Adding column that already exists; should not error
        exec("ALTER TABLE acine_test2 ADD COLUMN IF NOT EXISTS name text");
        exec("INSERT INTO acine_test2 (name) VALUES ('Bob')");
        assertEquals("Bob", query1("SELECT name FROM acine_test2"));
    }

    @Test
    void testAddColumnIfNotExistsWithDefault() throws SQLException {
        exec("CREATE TABLE acine_test3 (id serial PRIMARY KEY, name text)");
        exec("ALTER TABLE acine_test3 ADD COLUMN IF NOT EXISTS status text DEFAULT 'active'");
        exec("INSERT INTO acine_test3 (name) VALUES ('test')");
        assertEquals("active", query1("SELECT status FROM acine_test3 WHERE name = 'test'"));
    }

    // =========================================================================
    // ADD multiple columns in one ALTER TABLE
    // =========================================================================

    @Test
    void testAddMultipleColumns() throws SQLException {
        exec("CREATE TABLE multi_add (id serial PRIMARY KEY)");
        exec("ALTER TABLE multi_add ADD COLUMN name text, ADD COLUMN email text, ADD COLUMN age int");
        exec("INSERT INTO multi_add (name, email, age) VALUES ('test', 'test@test.com', 25)");
        assertEquals("test", query1("SELECT name FROM multi_add"));
    }

    @Test
    void testAddMultipleColumnsIfNotExists() throws SQLException {
        exec("CREATE TABLE multi_add2 (id serial PRIMARY KEY, name text)");
        exec("""
            ALTER TABLE multi_add2
            ADD COLUMN IF NOT EXISTS name text,
            ADD COLUMN IF NOT EXISTS phone varchar(15) DEFAULT NULL,
            ADD COLUMN IF NOT EXISTS phone_verified_at timestamptz DEFAULT NULL
        """);
        exec("INSERT INTO multi_add2 (name, phone) VALUES ('test', '+1234567890')");
        assertEquals("+1234567890", query1("SELECT phone FROM multi_add2"));
    }

    // =========================================================================
    // VALIDATE CONSTRAINT (separate from adding NOT VALID)
    // =========================================================================

    @Test
    void testAddConstraintNotValidThenValidate() throws SQLException {
        exec("CREATE TABLE validate_test (id serial PRIMARY KEY, parent_id int)");
        exec("CREATE TABLE validate_parent (id serial PRIMARY KEY)");
        exec("INSERT INTO validate_parent (id) VALUES (1)");
        exec("INSERT INTO validate_test (parent_id) VALUES (1)");
        exec("ALTER TABLE validate_test ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES validate_parent(id) NOT VALID");
        exec("ALTER TABLE validate_test VALIDATE CONSTRAINT fk_parent");
    }

    @Test
    void testAddCheckConstraintNotValidThenValidate() throws SQLException {
        exec("CREATE TABLE validate_check (id serial PRIMARY KEY, val int)");
        exec("INSERT INTO validate_check (val) VALUES (5)");
        exec("ALTER TABLE validate_check ADD CONSTRAINT chk_positive CHECK (val > 0) NOT VALID");
        exec("ALTER TABLE validate_check VALIDATE CONSTRAINT chk_positive");
    }

    // =========================================================================
    // RENAME column / table / constraint
    // =========================================================================

    @Test
    void testRenameColumn() throws SQLException {
        exec("CREATE TABLE rename_col_test (id serial PRIMARY KEY, old_name text)");
        exec("INSERT INTO rename_col_test (old_name) VALUES ('test')");
        exec("ALTER TABLE rename_col_test RENAME COLUMN old_name TO new_name");
        assertEquals("test", query1("SELECT new_name FROM rename_col_test"));
    }

    @Test
    void testRenameTable() throws SQLException {
        exec("CREATE TABLE old_table_name (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO old_table_name (val) VALUES ('keep')");
        exec("ALTER TABLE old_table_name RENAME TO new_table_name");
        assertEquals("keep", query1("SELECT val FROM new_table_name"));
    }

    @Test
    void testRenameConstraint() throws SQLException {
        exec("CREATE TABLE rename_con_test (id serial PRIMARY KEY, val int CONSTRAINT old_con CHECK (val > 0))");
        exec("ALTER TABLE rename_con_test RENAME CONSTRAINT old_con TO new_con");
        // Constraint should still work under new name
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO rename_con_test (val) VALUES (-1)"));
    }

    // =========================================================================
    // ADD CONSTRAINT ... USING INDEX (promotes index to PK/UNIQUE)
    // =========================================================================

    @Test
    void testAddConstraintUsingIndex() throws SQLException {
        exec("CREATE TABLE using_idx_test (id int NOT NULL, name text)");
        exec("CREATE UNIQUE INDEX idx_using_test ON using_idx_test (id)");
        exec("ALTER TABLE using_idx_test ADD CONSTRAINT pk_using_test PRIMARY KEY USING INDEX idx_using_test");
        exec("INSERT INTO using_idx_test (id, name) VALUES (1, 'ok')");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO using_idx_test (id, name) VALUES (1, 'dup')"));
    }

    // =========================================================================
    // ALTER COLUMN TYPE (with USING)
    // =========================================================================

    @Test
    void testAlterColumnTypeSimple() throws SQLException {
        exec("CREATE TABLE alter_type_test (id serial PRIMARY KEY, val varchar(50))");
        exec("INSERT INTO alter_type_test (val) VALUES ('hello')");
        exec("ALTER TABLE alter_type_test ALTER COLUMN val TYPE text");
        assertEquals("hello", query1("SELECT val FROM alter_type_test"));
    }

    @Test
    void testAlterColumnTypeWithUsing() throws SQLException {
        exec("CREATE TABLE alter_using_test (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO alter_using_test (val) VALUES ('42')");
        exec("ALTER TABLE alter_using_test ALTER COLUMN val TYPE int USING val::int");
        assertEquals("42", query1("SELECT val FROM alter_using_test"));
    }

    @Test
    void testAlterColumnTypeToTimestamptz() throws SQLException {
        exec("CREATE TABLE alter_tz_test (id serial PRIMARY KEY, ts timestamp)");
        exec("INSERT INTO alter_tz_test (ts) VALUES ('2024-01-15 10:00:00')");
        exec("ALTER TABLE alter_tz_test ALTER COLUMN ts TYPE timestamptz USING ts AT TIME ZONE 'UTC'");
        assertNotNull(query1("SELECT ts FROM alter_tz_test"));
    }

    // =========================================================================
    // DROP CONSTRAINT
    // =========================================================================

    @Test
    void testDropConstraintByName() throws SQLException {
        exec("CREATE TABLE drop_con_test (id serial PRIMARY KEY, val int, CONSTRAINT val_check CHECK (val > 0))");
        exec("INSERT INTO drop_con_test (val) VALUES (1)");
        exec("ALTER TABLE drop_con_test DROP CONSTRAINT val_check");
        // Now negative values should be allowed
        exec("INSERT INTO drop_con_test (val) VALUES (-1)");
        assertEquals("-1", query1("SELECT val FROM drop_con_test WHERE val < 0"));
    }

    @Test
    void testDropConstraintIfExists() throws SQLException {
        exec("CREATE TABLE drop_con_test2 (id serial PRIMARY KEY)");
        exec("ALTER TABLE drop_con_test2 DROP CONSTRAINT IF EXISTS nonexistent_constraint");
        // Should not error
    }

    // =========================================================================
    // SET/DROP DEFAULT
    // =========================================================================

    @Test
    void testAlterColumnSetDefault() throws SQLException {
        exec("CREATE TABLE set_default_test (id serial PRIMARY KEY, status text)");
        exec("ALTER TABLE set_default_test ALTER COLUMN status SET DEFAULT 'pending'");
        exec("INSERT INTO set_default_test DEFAULT VALUES");
        assertEquals("pending", query1("SELECT status FROM set_default_test"));
    }

    @Test
    void testAlterColumnDropDefault() throws SQLException {
        exec("CREATE TABLE drop_default_test (id serial PRIMARY KEY, status text DEFAULT 'active')");
        exec("ALTER TABLE drop_default_test ALTER COLUMN status DROP DEFAULT");
        exec("INSERT INTO drop_default_test DEFAULT VALUES");
        assertNull(query1("SELECT status FROM drop_default_test"));
    }

    // =========================================================================
    // SET/DROP NOT NULL
    // =========================================================================

    @Test
    void testAlterColumnSetNotNull() throws SQLException {
        exec("CREATE TABLE set_nn_test (id serial PRIMARY KEY, name text)");
        exec("ALTER TABLE set_nn_test ALTER COLUMN name SET NOT NULL");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO set_nn_test (name) VALUES (NULL)"));
    }

    @Test
    void testAlterColumnDropNotNull() throws SQLException {
        exec("CREATE TABLE drop_nn_test (id serial PRIMARY KEY, name text NOT NULL)");
        exec("ALTER TABLE drop_nn_test ALTER COLUMN name DROP NOT NULL");
        exec("INSERT INTO drop_nn_test (name) VALUES (NULL)");
        // Should succeed
    }

    // =========================================================================
    // Schema-qualified ALTER TABLE
    // =========================================================================

    @Test
    void testAlterTableInCustomSchema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS app");
        exec("CREATE TABLE app.config (id serial PRIMARY KEY, val text)");
        exec("ALTER TABLE app.config ADD COLUMN extra text DEFAULT 'none'");
        exec("INSERT INTO app.config (val) VALUES ('test')");
        assertEquals("none", query1("SELECT extra FROM app.config"));
    }

    // =========================================================================
    // ALTER TABLE ... SET (storage parameters)
    // =========================================================================

    @Test
    void testAlterTableSetStorageParams() throws SQLException {
        exec("CREATE TABLE storage_test (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE storage_test SET (fillfactor = 70)");
        exec("ALTER TABLE storage_test SET (autovacuum_enabled = false)");
    }

    // =========================================================================
    // ALTER TABLE ONLY (applies only to parent, not children)
    // =========================================================================

    @Test
    void testAlterTableOnly() throws SQLException {
        exec("CREATE TABLE alt_only_parent (id serial PRIMARY KEY, val int)");
        exec("ALTER TABLE ONLY alt_only_parent ADD CONSTRAINT chk CHECK (val > 0)");
        exec("INSERT INTO alt_only_parent (val) VALUES (1)");
    }

    // =========================================================================
    // ATTACH / DETACH PARTITION (also in CreateTableCompatTest but testing ALTER syntax)
    // =========================================================================

    @Test
    void testAlterTableAttachPartitionRange() throws SQLException {
        exec("CREATE TABLE partitioned_data (id bigserial, ts date NOT NULL, data text) PARTITION BY RANGE (ts)");
        exec("CREATE TABLE partitioned_data_2024 (id bigserial, ts date NOT NULL, data text)");
        exec("ALTER TABLE partitioned_data ATTACH PARTITION partitioned_data_2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')");
    }

    @Test
    void testAlterTableDetachPartitionConcurrently() throws SQLException {
        exec("CREATE TABLE part_detach (id bigserial, region text NOT NULL, v int) PARTITION BY LIST (region)");
        exec("CREATE TABLE part_detach_us PARTITION OF part_detach FOR VALUES IN ('us')");
        exec("ALTER TABLE part_detach DETACH PARTITION part_detach_us CONCURRENTLY");
    }
}
