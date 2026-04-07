package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for advanced schema-qualified identifier patterns that still fail.
 *
 * v1 compat covered basic schema.table references.
 * These tests focus on what still fails:
 * - Schema-qualified TYPE references in column definitions (myschema.my_enum)
 * - Schema-qualified FOREIGN KEY targets in CREATE TABLE body
 * - Schema-qualified DEFAULT expressions
 * - Schema-qualified function calls as defaults
 * - CREATE TABLE in schema with FK referencing another schema
 * - Types created in a custom schema used as column types
 */
class SchemaQualifiedAdvancedCompatTest {

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
    // Schema-qualified ENUM type in column definition
    // =========================================================================

    @Test
    void testColumnWithSchemaQualifiedEnum() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS app_types");
        exec("CREATE TYPE app_types.status AS ENUM ('active', 'inactive', 'pending')");
        exec("CREATE TABLE app_types.records (id serial PRIMARY KEY, state app_types.status NOT NULL DEFAULT 'pending')");
        exec("INSERT INTO app_types.records DEFAULT VALUES");
        assertEquals("pending", query1("SELECT state FROM app_types.records"));
    }

    @Test
    void testColumnWithSchemaQualifiedEnumInDifferentSchema() throws SQLException {
        // Type in one schema, table in another
        exec("CREATE SCHEMA IF NOT EXISTS type_defs");
        exec("CREATE SCHEMA IF NOT EXISTS data_store");
        exec("CREATE TYPE type_defs.priority AS ENUM ('low', 'medium', 'high')");
        exec("CREATE TABLE data_store.tasks (id serial PRIMARY KEY, prio type_defs.priority NOT NULL)");
        exec("INSERT INTO data_store.tasks (prio) VALUES ('high')");
        assertEquals("high", query1("SELECT prio FROM data_store.tasks"));
    }

    @Test
    void testColumnWithSchemaQualifiedEnumInPublicTable() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS enums");
        exec("CREATE TYPE enums.color AS ENUM ('red', 'green', 'blue')");
        exec("CREATE TABLE colored_items (id serial PRIMARY KEY, c enums.color)");
        exec("INSERT INTO colored_items (c) VALUES ('green')");
        assertEquals("green", query1("SELECT c FROM colored_items"));
    }

    // =========================================================================
    // Schema-qualified FOREIGN KEY in CREATE TABLE body
    // =========================================================================

    @Test
    void testForeignKeyToSchemaQualifiedTable() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS core");
        exec("CREATE TABLE core.accounts (id serial PRIMARY KEY, name text NOT NULL)");
        exec("""
            CREATE TABLE core.sessions (
                id serial PRIMARY KEY,
                account_id int NOT NULL,
                token text NOT NULL,
                FOREIGN KEY (account_id) REFERENCES core.accounts(id) ON DELETE CASCADE
            )
        """);
        exec("INSERT INTO core.accounts (name) VALUES ('test_user')");
        exec("INSERT INTO core.sessions (account_id, token) VALUES (1, 'abc123')");
    }

    @Test
    void testInlineForeignKeyToSchemaQualifiedTable() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS master_data");
        exec("CREATE TABLE master_data.parents (id serial PRIMARY KEY)");
        exec("""
            CREATE TABLE master_data.children (
                id serial PRIMARY KEY,
                parent_id int NOT NULL REFERENCES master_data.parents(id)
            )
        """);
    }

    @Test
    void testCrossSchemaForeignKeyInline() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS schema_a");
        exec("CREATE SCHEMA IF NOT EXISTS schema_b");
        exec("CREATE TABLE schema_a.base_entities (id serial PRIMARY KEY, name text)");
        exec("""
            CREATE TABLE schema_b.related_entities (
                id serial PRIMARY KEY,
                base_id int NOT NULL REFERENCES schema_a.base_entities(id) ON DELETE CASCADE,
                data text
            )
        """);
    }

    // =========================================================================
    // Schema-qualified function in DEFAULT expression
    // =========================================================================

    @Test
    void testSchemaQualifiedFunctionAsDefault() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS util");
        exec("CREATE FUNCTION util.generate_code() RETURNS text LANGUAGE sql AS $$ SELECT 'CODE-' || gen_random_uuid()::text; $$");
        exec("CREATE TABLE util.entries (id serial PRIMARY KEY, code text DEFAULT util.generate_code())");
        exec("INSERT INTO util.entries DEFAULT VALUES");
        assertNotNull(query1("SELECT code FROM util.entries"));
    }

    // =========================================================================
    // Schema-qualified CHECK constraint referencing schema types
    // =========================================================================

    @Test
    void testCheckConstraintWithLengthBetween() throws SQLException {
        // Pattern: CHECK (LENGTH(col) BETWEEN 1 AND 255)
        exec("CREATE SCHEMA IF NOT EXISTS validated");
        exec("""
            CREATE TABLE validated.entries (
                id serial PRIMARY KEY,
                code text NOT NULL CHECK (LENGTH(code) BETWEEN 1 AND 100),
                name text NOT NULL CHECK (name <> '')
            )
        """);
        exec("INSERT INTO validated.entries (code, name) VALUES ('ABC', 'test')");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO validated.entries (code, name) VALUES ('', 'test')"));
    }

    // =========================================================================
    // Schema-qualified SEQUENCE references
    // =========================================================================

    @Test
    void testSchemaQualifiedSequenceInDefault() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS seq_ns");
        exec("CREATE SEQUENCE seq_ns.item_id_seq START WITH 1");
        exec("CREATE TABLE seq_ns.items (id bigint DEFAULT nextval('seq_ns.item_id_seq') PRIMARY KEY, name text)");
        exec("INSERT INTO seq_ns.items (name) VALUES ('first')");
        assertEquals("1", query1("SELECT id FROM seq_ns.items"));
    }

    @Test
    void testAlterSequenceOwnedBySchemaQualified() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS own_ns");
        exec("CREATE TABLE own_ns.entries (id bigint NOT NULL, data text)");
        exec("CREATE SEQUENCE own_ns.entries_id_seq START WITH 1 INCREMENT BY 1");
        exec("ALTER SEQUENCE own_ns.entries_id_seq OWNED BY own_ns.entries.id");
    }

    // =========================================================================
    // Multiple tables in same schema with inter-table FKs
    // =========================================================================

    @Test
    void testMultiTableSchemaWithFks() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS project");
        exec("CREATE TABLE project.orgs (id text PRIMARY KEY CHECK (id <> ''), name text NOT NULL)");
        exec("CREATE TABLE project.teams (id text PRIMARY KEY, org_id text NOT NULL REFERENCES project.orgs(id) ON DELETE CASCADE, name text)");
        exec("CREATE TABLE project.members (id text PRIMARY KEY, team_id text NOT NULL REFERENCES project.teams(id) ON DELETE CASCADE, user_name text)");
        exec("INSERT INTO project.orgs (id, name) VALUES ('o1', 'Org1')");
        exec("INSERT INTO project.teams (id, org_id, name) VALUES ('t1', 'o1', 'Team1')");
        exec("INSERT INTO project.members (id, team_id, user_name) VALUES ('m1', 't1', 'alice')");
    }

    // =========================================================================
    // Trigger on schema-qualified table with schema-qualified function
    // =========================================================================

    @Test
    void testTriggerBothSchemaQualified() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS trg_ns");
        exec("CREATE TABLE trg_ns.tracked (id serial PRIMARY KEY, updated_at timestamp DEFAULT now())");
        exec("""
            CREATE FUNCTION trg_ns.auto_stamp()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN NEW.updated_at := now(); RETURN NEW; END;
            $$
        """);
        exec("""
            CREATE TRIGGER stamp_trigger
            BEFORE UPDATE ON trg_ns.tracked
            FOR EACH ROW
            EXECUTE FUNCTION trg_ns.auto_stamp()
        """);
    }

    // =========================================================================
    // CREATE INDEX on schema-qualified table
    // =========================================================================

    @Test
    void testIndexOnSchemaQualifiedTableWithExpression() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS idx_ns");
        exec("CREATE TABLE idx_ns.records (id serial PRIMARY KEY, email text, name text)");
        exec("CREATE INDEX idx_lower_email ON idx_ns.records (lower(email))");
        exec("CREATE UNIQUE INDEX idx_name_unique ON idx_ns.records (name) WHERE name IS NOT NULL");
    }

    // =========================================================================
    // DROP objects with schema qualification
    // =========================================================================

    @Test
    void testDropSchemaQualifiedObjects() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS drop_ns");
        exec("CREATE TABLE drop_ns.temp_tbl (id serial PRIMARY KEY)");
        exec("CREATE INDEX idx_drop ON drop_ns.temp_tbl (id)");
        exec("DROP INDEX drop_ns.idx_drop");
        exec("DROP TABLE drop_ns.temp_tbl");
    }

    // =========================================================================
    // ALTER TYPE in schema
    // =========================================================================

    @Test
    void testAlterTypeAddValueInSchema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS typed");
        exec("CREATE TYPE typed.stage AS ENUM ('draft', 'review', 'published')");
        exec("ALTER TYPE typed.stage ADD VALUE IF NOT EXISTS 'archived'");
        exec("CREATE TABLE typed.documents (id serial PRIMARY KEY, s typed.stage DEFAULT 'draft')");
        exec("INSERT INTO typed.documents (s) VALUES ('archived')");
        assertEquals("archived", query1("SELECT s FROM typed.documents"));
    }

    @Test
    void testAlterTypeRenameValueInSchema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS typed2");
        exec("CREATE TYPE typed2.level AS ENUM ('lo', 'mid', 'hi')");
        exec("ALTER TYPE typed2.level RENAME VALUE 'lo' TO 'low'");
    }

    @Test
    void testAlterTypeRenameToInSchema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS typed3");
        exec("CREATE TYPE typed3.old_name AS ENUM ('a', 'b')");
        exec("ALTER TYPE typed3.old_name RENAME TO new_name");
    }
}
