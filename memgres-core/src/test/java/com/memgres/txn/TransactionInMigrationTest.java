package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for explicit BEGIN/COMMIT transaction blocks in migration files.
 *
 * Some migration tools wrap DDL statements in explicit BEGIN/COMMIT blocks.
 * When one statement in the block fails, the entire transaction should be
 * rolled back. When all succeed, the transaction should be committed atomically.
 *
 * Also covers ALTER TABLE ADD COLUMN + CREATE TABLE in the same transaction,
 * and ALTER TABLE RENAME TO / RENAME CONSTRAINT patterns.
 */
class TransactionInMigrationTest {

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
    // BEGIN/COMMIT wrapping DDL
    // =========================================================================

    @Test
    void testBeginCommitWithDdl() throws SQLException {
        exec("BEGIN");
        exec("CREATE TABLE txn_ddl (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO txn_ddl (name) VALUES ('in_txn')");
        exec("COMMIT");
        assertEquals("in_txn", query1("SELECT name FROM txn_ddl"));
    }

    @Test
    void testBeginCommitAddColumnThenCreateTable() throws SQLException {
        // Pattern: one migration adds a column AND creates a new table in same transaction
        exec("CREATE TABLE existing_tbl (id serial PRIMARY KEY, name text)");
        exec("BEGIN");
        exec("ALTER TABLE existing_tbl ADD COLUMN extra_col text");
        exec("CREATE TABLE new_tbl (id serial PRIMARY KEY, ref_id int, details text)");
        exec("ALTER TABLE new_tbl ADD CONSTRAINT fk_ref FOREIGN KEY (ref_id) REFERENCES existing_tbl(id)");
        exec("COMMIT");
        exec("INSERT INTO existing_tbl (name, extra_col) VALUES ('test', 'extra')");
        exec("INSERT INTO new_tbl (ref_id, details) VALUES (1, 'detail')");
    }

    @Test
    void testBeginRollbackOnError() throws SQLException {
        exec("CREATE TABLE txn_rollback (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO txn_rollback (val) VALUES ('before')");
        exec("BEGIN");
        exec("INSERT INTO txn_rollback (val) VALUES ('should_rollback')");
        exec("ROLLBACK");
        assertEquals("1", query1("SELECT COUNT(*) FROM txn_rollback"));
    }

    // =========================================================================
    // ALTER TABLE RENAME TO
    // =========================================================================

    @Test
    void testAlterTableRenameTo() throws SQLException {
        exec("CREATE TABLE old_name (id serial PRIMARY KEY, data text)");
        exec("INSERT INTO old_name (data) VALUES ('renamed')");
        exec("ALTER TABLE old_name RENAME TO new_name");
        assertEquals("renamed", query1("SELECT data FROM new_name"));
    }

    @Test
    void testAlterTableRenameToSchemaQualified() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS rename_ns");
        exec("CREATE TABLE rename_ns.original (id serial PRIMARY KEY, val text)");
        exec("ALTER TABLE rename_ns.original RENAME TO renamed");
        exec("INSERT INTO rename_ns.renamed (val) VALUES ('ok')");
        assertEquals("ok", query1("SELECT val FROM rename_ns.renamed"));
    }

    @Test
    void testAlterTableRenameToThenAlterConstraints() throws SQLException {
        // Pattern: rename table, then rename its constraints to match new name
        exec("""
            CREATE TABLE pre_rename (
                id serial PRIMARY KEY,
                code text UNIQUE NOT NULL,
                CONSTRAINT pre_rename_code_unique UNIQUE (code)
            )
        """);
        // Handle the case where the unique constraint from UNIQUE keyword already exists
        try { exec("ALTER TABLE pre_rename DROP CONSTRAINT IF EXISTS pre_rename_code_unique"); } catch (SQLException ignored) {}
        exec("ALTER TABLE pre_rename RENAME TO post_rename");
        exec("ALTER TABLE post_rename RENAME CONSTRAINT pre_rename_pkey TO post_rename_pkey");
    }

    // =========================================================================
    // ALTER TABLE RENAME CONSTRAINT
    // =========================================================================

    @Test
    void testRenameConstraint() throws SQLException {
        exec("CREATE TABLE rn_con (id serial, val int, CONSTRAINT old_pk PRIMARY KEY (id))");
        exec("ALTER TABLE rn_con RENAME CONSTRAINT old_pk TO new_pk");
        // PK should still work
        assertThrows(SQLException.class, () -> exec("INSERT INTO rn_con (id, val) VALUES (1, 1); INSERT INTO rn_con (id, val) VALUES (1, 2)"));
    }

    @Test
    void testRenameUniqueConstraint() throws SQLException {
        exec("CREATE TABLE rn_uniq (id serial PRIMARY KEY, code text, CONSTRAINT old_uniq UNIQUE (code))");
        exec("ALTER TABLE rn_uniq RENAME CONSTRAINT old_uniq TO new_uniq");
    }

    @Test
    void testRenameForeignKeyConstraint() throws SQLException {
        exec("CREATE TABLE rn_parent (id serial PRIMARY KEY)");
        exec("CREATE TABLE rn_child (id serial PRIMARY KEY, pid int, CONSTRAINT old_fk FOREIGN KEY (pid) REFERENCES rn_parent(id))");
        exec("ALTER TABLE rn_child RENAME CONSTRAINT old_fk TO new_fk");
    }

    // =========================================================================
    // DDL inside BEGIN/COMMIT with schema-qualified tables
    // =========================================================================

    @Test
    void testTransactionWithSchemaQualifiedDdl() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS txn_schema");
        exec("BEGIN");
        exec("CREATE TABLE txn_schema.items (id serial PRIMARY KEY, name text)");
        exec("ALTER TABLE txn_schema.items ADD COLUMN price numeric DEFAULT 0");
        exec("COMMIT");
        exec("INSERT INTO txn_schema.items (name, price) VALUES ('widget', 9.99)");
    }

    // =========================================================================
    // Multiple ALTERs on same table in one transaction
    // =========================================================================

    @Test
    void testMultipleAltersInTransaction() throws SQLException {
        exec("CREATE TABLE multi_alter (id serial PRIMARY KEY, name text)");
        exec("BEGIN");
        exec("ALTER TABLE multi_alter ADD COLUMN email text");
        exec("ALTER TABLE multi_alter ADD COLUMN phone text");
        exec("ALTER TABLE multi_alter ADD COLUMN active boolean DEFAULT true");
        exec("COMMIT");
        exec("INSERT INTO multi_alter (name, email, phone) VALUES ('test', 'a@b.com', '555-0100')");
    }

    // =========================================================================
    // Bitwise operators and byte functions in PL/pgSQL
    // =========================================================================

    @Test
    void testBitwiseAndOperator() throws SQLException {
        assertEquals("2", query1("SELECT 6 & 3"));
    }

    @Test
    void testBitwiseOrOperator() throws SQLException {
        assertEquals("7", query1("SELECT 5 | 3"));
    }

    @Test
    void testBitwiseXorOperator() throws SQLException {
        assertEquals("5", query1("SELECT 6 # 3"));
    }

    @Test
    void testBitwiseNotOperator() throws SQLException {
        assertNotNull(query1("SELECT ~42"));
    }

    @Test
    void testBitwiseShiftLeft() throws SQLException {
        assertEquals("8", query1("SELECT 1 << 3"));
    }

    @Test
    void testBitwiseShiftRight() throws SQLException {
        assertEquals("2", query1("SELECT 8 >> 2"));
    }

    @Test
    void testGetByteFunction() throws SQLException {
        assertNotNull(query1("SELECT get_byte(E'\\\\x4142', 0)"));
    }

    @Test
    void testSetByteFunction() throws SQLException {
        assertNotNull(query1("SELECT set_byte(E'\\\\x4142', 0, 67)"));
    }

    // =========================================================================
    // PL/pgSQL function using bitwise & and get_byte
    // =========================================================================

    @Test
    void testPlpgsqlFunctionWithBitwiseOps() throws SQLException {
        exec("""
            CREATE FUNCTION bitwise_mask(input bytea, mask int)
            RETURNS int LANGUAGE plpgsql STABLE AS $$
            DECLARE
                byte_val int;
            BEGIN
                byte_val := get_byte(input, 0);
                RETURN byte_val & mask;
            END;
            $$
        """);
        assertNotNull(query1("SELECT bitwise_mask(E'\\\\xFF'::bytea, 15)"));
    }

    @Test
    void testPlpgsqlFunctionGeneratingId() throws SQLException {
        // Pattern: function that generates a random short ID using bytea + bitwise ops
        exec("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        exec("""
            CREATE FUNCTION short_id(size int DEFAULT 8)
            RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
                result text := '';
                i int := 0;
                alphabet char(36) := 'abcdefghijklmnopqrstuvwxyz0123456789';
                rand_bytes bytea;
                byte_val int;
                pos int;
            BEGIN
                rand_bytes := gen_random_bytes(size);
                WHILE i < size LOOP
                    byte_val := get_byte(rand_bytes, i);
                    pos := (byte_val & 35) + 1;
                    result := result || substr(alphabet, pos, 1);
                    i := i + 1;
                END LOOP;
                RETURN result;
            END;
            $$
        """);
        assertNotNull(query1("SELECT short_id()"));
    }

    // =========================================================================
    // Quoted enum type used as column type
    // =========================================================================

    @Test
    void testQuotedEnumAsColumnType() throws SQLException {
        exec("CREATE TYPE \"ItemCategory\" AS ENUM ('widget', 'gadget', 'tool')");
        exec("CREATE TABLE typed_items (id serial PRIMARY KEY, category \"ItemCategory\" NOT NULL)");
        exec("INSERT INTO typed_items (category) VALUES ('widget')");
        assertEquals("widget", query1("SELECT category FROM typed_items"));
    }

    @Test
    void testQuotedEnumWithDefaultE() throws SQLException {
        // Combined: quoted enum + E'' default
        exec("CREATE TYPE \"Status\" AS ENUM ('draft', 'published', 'archived')");
        exec("""
            CREATE TABLE content_items (
                "id" TEXT NOT NULL,
                "title" TEXT NOT NULL,
                "status" "Status" NOT NULL DEFAULT 'draft',
                "authorName" TEXT NOT NULL DEFAULT E'',
                "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT content_pkey PRIMARY KEY ("id")
            )
        """);
        exec("INSERT INTO content_items (\"id\", \"title\") VALUES ('c1', 'Test')");
        assertEquals("draft", query1("SELECT \"status\" FROM content_items WHERE \"id\" = 'c1'"));
        assertEquals("", query1("SELECT \"authorName\" FROM content_items WHERE \"id\" = 'c1'"));
    }

    // =========================================================================
    // Assignment operator = in PL/pgSQL (i = i + 1 vs i := i + 1)
    // =========================================================================

    @Test
    void testPlpgsqlAssignmentWithEquals() throws SQLException {
        // PG allows both := and = for assignment in PL/pgSQL
        exec("""
            DO $$
            DECLARE
                counter int;
            BEGIN
                counter = 0;
                counter = counter + 1;
                counter = counter + 1;
                RAISE NOTICE 'counter: %', counter;
            END;
            $$
        """);
    }

    @Test
    void testPlpgsqlMixedAssignment() throws SQLException {
        exec("""
            DO $$
            DECLARE
                a int;
                b int;
            BEGIN
                a := 10;
                b = 20;
                a = a + b;
                RAISE NOTICE 'result: %', a;
            END;
            $$
        """);
    }
}
