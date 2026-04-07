package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for connection state recovery after batch execution errors.
 *
 * When a multi-statement batch fails (e.g., one statement references a
 * nonexistent constraint), the connection must remain usable and previously
 * committed objects must still be visible. The failed batch should not
 * roll back work from earlier, independently-committed batches.
 *
 * Also covers ALTER TABLE RENAME CONSTRAINT where the constraint name
 * was specified explicitly in the CREATE TABLE.
 */
class BatchErrorRecoveryTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test?preferQueryMode=simple",
                "test", "test"
        );
        conn.setAutoCommit(true);
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
    // Tables remain visible after a failed batch
    // =========================================================================

    @Test
    void testTablesVisibleAfterFailedBatch() throws SQLException {
        // Step 1: Create tables in one batch (succeeds)
        exec("""
            CREATE TABLE recovery_a (id serial PRIMARY KEY, name text);
            INSERT INTO recovery_a (name) VALUES ('persisted');
        """);

        // Step 2: Send a batch that fails (references nonexistent constraint)
        try {
            exec("""
                ALTER TABLE recovery_a DROP CONSTRAINT nonexistent_constraint_xyz;
                ALTER TABLE recovery_a RENAME TO recovery_a_new;
            """);
        } catch (SQLException expected) {
            // Expected to fail
        }

        // Step 3: Tables from step 1 must still be accessible
        assertEquals("persisted", query1("SELECT name FROM recovery_a"));
    }

    @Test
    void testMultipleTablesVisibleAfterFailedBatch() throws SQLException {
        // Create several tables across separate batches
        exec("CREATE TABLE vis_t1 (id serial PRIMARY KEY, val text)");
        exec("CREATE TABLE vis_t2 (id serial PRIMARY KEY, val text)");
        exec("CREATE TABLE vis_t3 (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO vis_t1 (val) VALUES ('one')");
        exec("INSERT INTO vis_t2 (val) VALUES ('two')");
        exec("INSERT INTO vis_t3 (val) VALUES ('three')");

        // Failed batch
        try {
            exec("ALTER TABLE vis_t1 DROP CONSTRAINT fake_constraint");
        } catch (SQLException expected) {}

        // All tables must be accessible
        assertEquals("one", query1("SELECT val FROM vis_t1"));
        assertEquals("two", query1("SELECT val FROM vis_t2"));
        assertEquals("three", query1("SELECT val FROM vis_t3"));
    }

    // =========================================================================
    // Connection usable after failed multi-statement batch
    // =========================================================================

    @Test
    void testConnectionUsableAfterFailedMultiStatementBatch() throws SQLException {
        exec("CREATE TABLE conn_test (id serial PRIMARY KEY, data text)");

        // Multi-statement batch where second statement fails
        try {
            exec("""
                INSERT INTO conn_test (data) VALUES ('before_fail');
                INSERT INTO nonexistent_table (x) VALUES (1);
            """);
        } catch (SQLException expected) {}

        // Connection should still work
        exec("INSERT INTO conn_test (data) VALUES ('after_fail')");
        assertNotNull(query1("SELECT COUNT(*) FROM conn_test"));
    }

    // =========================================================================
    // CREATE TABLE with named PK, then RENAME
    // =========================================================================

    @Test
    void testCreateWithNamedPkThenDropConstraint() throws SQLException {
        exec("""
            CREATE TABLE "Item" (
                "id" TEXT NOT NULL,
                "name" TEXT NOT NULL,
                "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT "Item_pkey" PRIMARY KEY ("id")
            )
        """);
        // DROP the named constraint should work
        exec("ALTER TABLE \"Item\" DROP CONSTRAINT \"Item_pkey\"");
    }

    @Test
    void testCreateWithNamedPkThenRenameTable() throws SQLException {
        // Step 1: Create table with named PK
        exec("""
            CREATE TABLE "Original" (
                "id" TEXT NOT NULL,
                "value" TEXT,
                CONSTRAINT "Original_pkey" PRIMARY KEY ("id")
            )
        """);
        exec("INSERT INTO \"Original\" (\"id\", \"value\") VALUES ('1', 'test')");

        // Step 2: Rename table
        exec("ALTER TABLE \"Original\" RENAME TO \"renamed\"");

        // Step 3: Data should be in renamed table
        assertEquals("test", query1("SELECT \"value\" FROM \"renamed\" WHERE \"id\" = '1'"));
    }

    @Test
    void testCreateWithNamedPkRenameTableThenRenameConstraint() throws SQLException {
        // Full ORM rename pattern:
        // 1. DROP foreign keys
        // 2. RENAME TABLE
        // 3. RENAME COLUMN (camelCase to snake_case)
        // 4. RENAME CONSTRAINT
        // 5. ADD foreign keys back with new names

        exec("""
            CREATE TABLE "Parent" (
                "id" TEXT NOT NULL,
                CONSTRAINT "Parent_pkey" PRIMARY KEY ("id")
            )
        """);
        exec("""
            CREATE TABLE "Child" (
                "id" TEXT NOT NULL,
                "parentId" TEXT NOT NULL,
                CONSTRAINT "Child_pkey" PRIMARY KEY ("id")
            )
        """);
        exec("ALTER TABLE \"Child\" ADD CONSTRAINT \"Child_parentId_fkey\" FOREIGN KEY (\"parentId\") REFERENCES \"Parent\"(\"id\") ON DELETE CASCADE");

        // Drop FK
        exec("ALTER TABLE \"Child\" DROP CONSTRAINT \"Child_parentId_fkey\"");

        // Rename tables
        exec("ALTER TABLE \"Parent\" RENAME TO \"parents\"");
        exec("ALTER TABLE \"Child\" RENAME TO \"children\"");

        // Rename columns
        exec("ALTER TABLE \"children\" RENAME COLUMN \"parentId\" TO \"parent_id\"");

        // Rename constraints
        exec("ALTER TABLE \"parents\" RENAME CONSTRAINT \"Parent_pkey\" TO \"parents_pkey\"");
        exec("ALTER TABLE \"children\" RENAME CONSTRAINT \"Child_pkey\" TO \"children_pkey\"");

        // Re-add FK with new names
        exec("ALTER TABLE \"children\" ADD CONSTRAINT \"children_parent_id_fkey\" FOREIGN KEY (\"parent_id\") REFERENCES \"parents\"(\"id\") ON DELETE CASCADE");
    }

    // =========================================================================
    // Rename constraint on enum type (ALTER TYPE RENAME)
    // =========================================================================

    @Test
    void testRenameEnumType() throws SQLException {
        exec("CREATE TYPE \"ItemType\" AS ENUM ('widget', 'gadget')");
        exec("ALTER TYPE \"ItemType\" RENAME TO \"item_type\"");
        exec("CREATE TABLE typed (id serial PRIMARY KEY, kind \"item_type\")");
        exec("INSERT INTO typed (kind) VALUES ('widget')");
    }

    // =========================================================================
    // Unique index with quoted column name, then rename
    // =========================================================================

    @Test
    void testCreateUniqueIndexThenRenameTable() throws SQLException {
        exec("""
            CREATE TABLE "Entry" (
                "id" TEXT NOT NULL,
                "code" TEXT NOT NULL,
                CONSTRAINT "Entry_pkey" PRIMARY KEY ("id")
            )
        """);
        exec("CREATE UNIQUE INDEX \"Entry_code_key\" ON \"Entry\"(\"code\")");
        exec("ALTER TABLE \"Entry\" RENAME TO \"entries\"");
        // Index should still work after rename
        exec("INSERT INTO \"entries\" (\"id\", \"code\") VALUES ('1', 'ABC')");
        assertThrows(SQLException.class, () ->
            exec("INSERT INTO \"entries\" (\"id\", \"code\") VALUES ('2', 'ABC')"));
    }

    // =========================================================================
    // Batch failure should not affect previous autocommit statements
    // =========================================================================

    @Test
    void testAutocommitIsolation() throws SQLException {
        // Each exec() is autocommitted independently
        exec("CREATE TABLE iso_a (id serial PRIMARY KEY)");
        exec("INSERT INTO iso_a DEFAULT VALUES");
        exec("CREATE TABLE iso_b (id serial PRIMARY KEY)");
        exec("INSERT INTO iso_b DEFAULT VALUES");

        // This should fail
        try { exec("DROP TABLE nonexistent_xyz"); } catch (SQLException expected) {}

        // Previous work should be intact
        assertEquals("1", query1("SELECT COUNT(*) FROM iso_a"));
        assertEquals("1", query1("SELECT COUNT(*) FROM iso_b"));

        // Should be able to create more tables
        exec("CREATE TABLE iso_c (id serial PRIMARY KEY)");
    }
}
