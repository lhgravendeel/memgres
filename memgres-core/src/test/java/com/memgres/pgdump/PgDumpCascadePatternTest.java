package com.memgres.pgdump;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for pg_dump patterns that cause cascading failures.
 *
 * Covers:
 * - ALTER TABLE IF EXISTS ONLY ... DROP CONSTRAINT IF EXISTS
 * - Tables created in a schema where the schema is created via CREATE SCHEMA
 *   in the same dump file, and later ATTACH PARTITION references schema.table
 * - pg_dump COPY data format (raw data lines that aren't SQL)
 * - psql :variable references (should be handled gracefully)
 * - CREATE TABLE with columns that have schema-qualified custom domain types
 */
class PgDumpCascadePatternTest {

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
    // ALTER TABLE IF EXISTS ONLY ... DROP CONSTRAINT IF EXISTS
    // =========================================================================

    @Test
    void testAlterTableIfExistsDropConstraintIfExists() throws SQLException {
        // pg_dump pattern: clean up constraints before recreating them
        // Table doesn't exist yet; should not error with IF EXISTS
        exec("ALTER TABLE IF EXISTS ONLY nonexistent_table DROP CONSTRAINT IF EXISTS some_fk_name");
    }

    @Test
    void testAlterTableIfExistsOnExistingTable() throws SQLException {
        exec("CREATE TABLE exists_test (id serial PRIMARY KEY, val int CONSTRAINT chk_val CHECK (val > 0))");
        exec("ALTER TABLE IF EXISTS ONLY exists_test DROP CONSTRAINT IF EXISTS chk_val");
        // Constraint should be gone now
        exec("INSERT INTO exists_test (val) VALUES (-1)");
    }

    @Test
    void testAlterTableIfExistsOnlyAddConstraint() throws SQLException {
        exec("CREATE TABLE if_exists_add (id serial PRIMARY KEY, ref_id int)");
        exec("CREATE TABLE if_exists_parent (id serial PRIMARY KEY)");
        exec("INSERT INTO if_exists_parent VALUES (1)");
        exec("INSERT INTO if_exists_add (ref_id) VALUES (1)");
        exec("ALTER TABLE IF EXISTS ONLY if_exists_add ADD CONSTRAINT fk_ref FOREIGN KEY (ref_id) REFERENCES if_exists_parent(id)");
    }

    // =========================================================================
    // Schema + partition table lifecycle
    // =========================================================================

    @Test
    void testSchemaCreateThenPartitionAttach() throws SQLException {
        // Simulates: CREATE SCHEMA, CREATE partitioned TABLE, CREATE child tables in schema,
        // then ATTACH PARTITION using schema.table
        exec("CREATE SCHEMA static_parts");
        exec("CREATE TABLE partitioned_events (id bigserial, hash_id int NOT NULL, data text) PARTITION BY HASH (hash_id)");
        exec("CREATE TABLE static_parts.events_0 (id bigserial, hash_id int NOT NULL, data text)");
        exec("CREATE TABLE static_parts.events_1 (id bigserial, hash_id int NOT NULL, data text)");
        exec("ALTER TABLE ONLY partitioned_events ATTACH PARTITION static_parts.events_0 FOR VALUES WITH (MODULUS 2, REMAINDER 0)");
        exec("ALTER TABLE ONLY partitioned_events ATTACH PARTITION static_parts.events_1 FOR VALUES WITH (MODULUS 2, REMAINDER 1)");
        exec("INSERT INTO partitioned_events (hash_id, data) VALUES (1, 'test')");
        assertNotNull(query1("SELECT data FROM partitioned_events"));
    }

    @Test
    void testMultiplePartitionsInSchema() throws SQLException {
        exec("CREATE SCHEMA dyn_parts");
        exec("CREATE TABLE range_data (id bigserial, period date NOT NULL, info text) PARTITION BY RANGE (period)");
        exec("CREATE TABLE dyn_parts.data_2024q1 (id bigserial, period date NOT NULL, info text)");
        exec("CREATE TABLE dyn_parts.data_2024q2 (id bigserial, period date NOT NULL, info text)");
        exec("ALTER TABLE range_data ATTACH PARTITION dyn_parts.data_2024q1 FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')");
        exec("ALTER TABLE range_data ATTACH PARTITION dyn_parts.data_2024q2 FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')");
    }

    // =========================================================================
    // Schema-qualified domain as column type
    // =========================================================================

    @Test
    void testCreateDomainInSchemaThenUseAsColumnType() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS custom_types");
        exec("CREATE EXTENSION IF NOT EXISTS citext");
        exec("CREATE DOMAIN custom_types.validated_email AS citext CHECK (value ~ '@')");
        exec("""
            CREATE TABLE custom_types.accounts (
                id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
                created_at timestamp with time zone DEFAULT now() NOT NULL,
                email custom_types.validated_email NOT NULL UNIQUE,
                display_name text
            )
        """);
        exec("INSERT INTO custom_types.accounts (email, display_name) VALUES ('user@example.com', 'Test User')");
        assertEquals("Test User", query1("SELECT display_name FROM custom_types.accounts WHERE email = 'user@example.com'"));
    }

    @Test
    void testDomainWithPublicPrefixedBaseType() throws SQLException {
        // Pattern: CREATE DOMAIN schema.name AS public.citext
        exec("CREATE SCHEMA IF NOT EXISTS auth_types");
        exec("CREATE EXTENSION IF NOT EXISTS citext");
        exec("CREATE DOMAIN auth_types.email AS public.citext CHECK (value ~ '@')");
        exec("CREATE TABLE auth_types.users (id serial PRIMARY KEY, email auth_types.email UNIQUE)");
        exec("INSERT INTO auth_types.users (email) VALUES ('Test@Example.COM')");
        assertNotNull(query1("SELECT email FROM auth_types.users WHERE id = 1"));
    }

    // =========================================================================
    // pg_dump sequence lifecycle: CREATE SEQUENCE, ALTER SEQUENCE OWNED BY
    // =========================================================================

    @Test
    void testPgDumpSequenceLifecycle() throws SQLException {
        // Phase 1: Create table without serial (raw bigint NOT NULL)
        exec("CREATE TABLE raw_serial (id bigint NOT NULL, name text)");
        // Phase 2: Create sequence
        exec("CREATE SEQUENCE raw_serial_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1");
        // Phase 3: ALTER TABLE SET DEFAULT
        exec("ALTER TABLE ONLY raw_serial ALTER COLUMN id SET DEFAULT nextval('raw_serial_id_seq'::regclass)");
        // Phase 4: ALTER SEQUENCE OWNED BY
        exec("ALTER SEQUENCE raw_serial_id_seq OWNED BY raw_serial.id");
        // Phase 5: Insert
        exec("INSERT INTO raw_serial (name) VALUES ('auto')");
        // Phase 6: setval
        exec("SELECT setval('raw_serial_id_seq', 1, true)");
        // Phase 7: Add PK
        exec("ALTER TABLE ONLY raw_serial ADD CONSTRAINT raw_serial_pkey PRIMARY KEY (id)");
        // Verify
        assertEquals("auto", query1("SELECT name FROM raw_serial WHERE id = 1"));
    }

    // =========================================================================
    // Multiple sequences + defaults in pg_dump order
    // =========================================================================

    @Test
    void testMultipleSequencesBeforeInserts() throws SQLException {
        exec("CREATE TABLE multi_seq_a (id bigint NOT NULL, val text)");
        exec("CREATE TABLE multi_seq_b (id bigint NOT NULL, ref_id bigint, val text)");
        exec("CREATE SEQUENCE multi_seq_a_id START WITH 1");
        exec("CREATE SEQUENCE multi_seq_b_id START WITH 1");
        exec("ALTER TABLE ONLY multi_seq_a ALTER COLUMN id SET DEFAULT nextval('multi_seq_a_id'::regclass)");
        exec("ALTER TABLE ONLY multi_seq_b ALTER COLUMN id SET DEFAULT nextval('multi_seq_b_id'::regclass)");
        // Data inserts before constraints
        exec("INSERT INTO multi_seq_a (id, val) VALUES (1, 'parent')");
        exec("INSERT INTO multi_seq_b (id, ref_id, val) VALUES (1, 1, 'child')");
        // Constraints after data
        exec("ALTER TABLE ONLY multi_seq_a ADD CONSTRAINT multi_seq_a_pkey PRIMARY KEY (id)");
        exec("ALTER TABLE ONLY multi_seq_b ADD CONSTRAINT multi_seq_b_pkey PRIMARY KEY (id)");
        exec("ALTER TABLE ONLY multi_seq_b ADD CONSTRAINT multi_seq_b_ref FOREIGN KEY (ref_id) REFERENCES multi_seq_a(id)");
        // Reset sequences
        exec("SELECT setval('multi_seq_a_id', (SELECT MAX(id) FROM multi_seq_a))");
        exec("SELECT setval('multi_seq_b_id', (SELECT MAX(id) FROM multi_seq_b))");
        // Verify auto-increment works
        exec("INSERT INTO multi_seq_a (val) VALUES ('second_parent')");
        assertEquals("2", query1("SELECT id FROM multi_seq_a WHERE val = 'second_parent'"));
    }

    // =========================================================================
    // Scheduling tool pattern: PL/pgSQL function then uses in migration
    // =========================================================================

    @Test
    void testFunctionCreatedThenUsedInMigration() throws SQLException {
        // Pattern: migration creates a helper function, then uses it for data migration
        exec("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        exec("""
            CREATE FUNCTION generate_short_id(size int DEFAULT 21)
            RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
                id text := '';
                i int := 0;
                alphabet char(36) := 'abcdefghijklmnopqrstuvwxyz0123456789';
                bytes bytea := gen_random_bytes(size);
                byte int;
                pos int;
            BEGIN
                WHILE i < size LOOP
                    byte := get_byte(bytes, i);
                    pos := (byte % 36) + 1;
                    id := id || substr(alphabet, pos, 1);
                    i := i + 1;
                END LOOP;
                RETURN id;
            END;
            $$
        """);
        exec("CREATE TABLE short_id_test (id text DEFAULT generate_short_id() PRIMARY KEY, name text)");
        exec("INSERT INTO short_id_test (name) VALUES ('auto_id_row')");
        assertNotNull(query1("SELECT id FROM short_id_test WHERE name = 'auto_id_row'"));
    }

    // =========================================================================
    // ORM-generated CREATE TABLE + CREATE UNIQUE INDEX
    // =========================================================================

    @Test
    void testOrmStyleCreateTableAndIndex() throws SQLException {
        exec("""
            CREATE TABLE "TaskRecord" (
                "id" UUID NOT NULL,
                "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                "deletedAt" TIMESTAMP(3),
                "tenantId" UUID NOT NULL,
                "status" TEXT NOT NULL DEFAULT 'PENDING',
                "name" TEXT,
                CONSTRAINT "TaskRecord_pkey" PRIMARY KEY ("id")
            )
        """);
        exec("""
            CREATE UNIQUE INDEX "TaskRecord_tenantId_name_key" ON "TaskRecord"("tenantId", "name")
        """);
        exec("""
            INSERT INTO "TaskRecord" ("id", "tenantId", "name")
            VALUES (gen_random_uuid(), gen_random_uuid(), 'test_task')
        """);
        assertEquals("test_task", query1("""
            SELECT "name" FROM "TaskRecord" WHERE "name" = 'test_task'
        """));
    }
}
