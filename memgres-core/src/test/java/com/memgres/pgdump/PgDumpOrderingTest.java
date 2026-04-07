package com.memgres.pgdump;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for pg_dump-style DDL ordering patterns.
 *
 * pg_dump emits SQL in a specific phase order:
 * 1. CREATE TABLE (no constraints beyond NOT NULL)
 * 2. CREATE SEQUENCE + ALTER SEQUENCE OWNED BY
 * 3. ALTER TABLE SET DEFAULT nextval(...)
 * 4. INSERT data (COPY or INSERT)
 * 5. ALTER TABLE ADD PRIMARY KEY
 * 6. ALTER TABLE ADD CONSTRAINT (FK, UNIQUE, CHECK)
 * 7. CREATE INDEX
 *
 * This means INSERTs happen BEFORE primary keys and other constraints
 * are added. Memgres must handle this ordering correctly.
 *
 * Also covers jsonb_to_recordset with inline column type definitions
 * (used in LATERAL joins) and nested aggregate function calls.
 */
class PgDumpOrderingTest {

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
    // Phase 1: CREATE TABLE without constraints, then INSERT, then ADD PK
    // =========================================================================

    @Test
    void testInsertBeforePrimaryKeyAdded() throws SQLException {
        // Phase 1: CREATE TABLE with no PK
        exec("CREATE TABLE pg_dump_log (id bigint NOT NULL, created_at timestamp DEFAULT now() NOT NULL, payload jsonb DEFAULT '{}' NOT NULL)");
        // Phase 2: CREATE SEQUENCE
        exec("CREATE SEQUENCE pg_dump_log_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1");
        // Phase 3: SET DEFAULT
        exec("ALTER TABLE ONLY pg_dump_log ALTER COLUMN id SET DEFAULT nextval('pg_dump_log_id_seq'::regclass)");
        // Phase 4: INSERT (before PK!)
        exec("INSERT INTO pg_dump_log (id, payload) VALUES (1, '{\"action\": \"test\"}')");
        exec("INSERT INTO pg_dump_log (id, payload) VALUES (2, '{\"action\": \"test2\"}')");
        // Phase 5: ADD PK
        exec("ALTER TABLE ONLY pg_dump_log ADD CONSTRAINT pg_dump_log_pkey PRIMARY KEY (id)");
        // Verify
        assertEquals("2", query1("SELECT COUNT(*) FROM pg_dump_log"));
    }

    @Test
    void testInsertBeforeForeignKeyAdded() throws SQLException {
        // Create parent and child without FK
        exec("CREATE TABLE pgd_parent (id bigint NOT NULL)");
        exec("CREATE TABLE pgd_child (id bigint NOT NULL, parent_id bigint)");
        // Insert data
        exec("INSERT INTO pgd_parent VALUES (1), (2)");
        exec("INSERT INTO pgd_child VALUES (1, 1), (2, 2)");
        // Now add PK and FK
        exec("ALTER TABLE ONLY pgd_parent ADD CONSTRAINT pgd_parent_pkey PRIMARY KEY (id)");
        exec("ALTER TABLE ONLY pgd_child ADD CONSTRAINT pgd_child_pkey PRIMARY KEY (id)");
        exec("ALTER TABLE ONLY pgd_child ADD CONSTRAINT pgd_child_fk FOREIGN KEY (parent_id) REFERENCES pgd_parent(id)");
        assertEquals("2", query1("SELECT COUNT(*) FROM pgd_child"));
    }

    @Test
    void testInsertBeforeUniqueConstraintAdded() throws SQLException {
        exec("CREATE TABLE pgd_unique (id bigint NOT NULL, code text NOT NULL)");
        exec("INSERT INTO pgd_unique VALUES (1, 'A'), (2, 'B')");
        exec("ALTER TABLE ONLY pgd_unique ADD CONSTRAINT pgd_unique_pkey PRIMARY KEY (id)");
        exec("ALTER TABLE ONLY pgd_unique ADD CONSTRAINT pgd_unique_code_key UNIQUE (code)");
        assertEquals("2", query1("SELECT COUNT(*) FROM pgd_unique"));
    }

    @Test
    void testInsertBeforeIndexCreated() throws SQLException {
        exec("CREATE TABLE pgd_indexed (id bigint NOT NULL, name text, created_at timestamp)");
        exec("INSERT INTO pgd_indexed VALUES (1, 'test', now())");
        exec("ALTER TABLE ONLY pgd_indexed ADD CONSTRAINT pgd_indexed_pkey PRIMARY KEY (id)");
        exec("CREATE INDEX idx_pgd_name ON pgd_indexed (name)");
        exec("CREATE INDEX idx_pgd_created ON pgd_indexed (created_at DESC NULLS LAST)");
        assertEquals("test", query1("SELECT name FROM pgd_indexed WHERE id = 1"));
    }

    // =========================================================================
    // Full pg_dump lifecycle: multiple related tables
    // =========================================================================

    @Test
    void testFullPgDumpLifecycle() throws SQLException {
        // Phase 1: All CREATE TABLE statements
        exec("CREATE TABLE pgd_orgs (id bigint NOT NULL, name text NOT NULL, active boolean DEFAULT true)");
        exec("CREATE TABLE pgd_users (id bigint NOT NULL, org_id bigint, name text, email text)");
        exec("CREATE TABLE pgd_roles (id bigint NOT NULL, user_id bigint, role_name text)");

        // Phase 2: Sequences
        exec("CREATE SEQUENCE pgd_orgs_id_seq START WITH 1 INCREMENT BY 1");
        exec("CREATE SEQUENCE pgd_users_id_seq START WITH 1 INCREMENT BY 1");
        exec("CREATE SEQUENCE pgd_roles_id_seq START WITH 1 INCREMENT BY 1");

        // Phase 3: Set defaults
        exec("ALTER TABLE ONLY pgd_orgs ALTER COLUMN id SET DEFAULT nextval('pgd_orgs_id_seq'::regclass)");
        exec("ALTER TABLE ONLY pgd_users ALTER COLUMN id SET DEFAULT nextval('pgd_users_id_seq'::regclass)");
        exec("ALTER TABLE ONLY pgd_roles ALTER COLUMN id SET DEFAULT nextval('pgd_roles_id_seq'::regclass)");

        // Phase 4: INSERT data (before any constraints!)
        exec("INSERT INTO pgd_orgs (id, name) VALUES (1, 'Acme Corp')");
        exec("INSERT INTO pgd_users (id, org_id, name, email) VALUES (1, 1, 'Alice', 'alice@acme.com')");
        exec("INSERT INTO pgd_roles (id, user_id, role_name) VALUES (1, 1, 'admin')");

        // Phase 5: Primary keys
        exec("ALTER TABLE ONLY pgd_orgs ADD CONSTRAINT pgd_orgs_pkey PRIMARY KEY (id)");
        exec("ALTER TABLE ONLY pgd_users ADD CONSTRAINT pgd_users_pkey PRIMARY KEY (id)");
        exec("ALTER TABLE ONLY pgd_roles ADD CONSTRAINT pgd_roles_pkey PRIMARY KEY (id)");

        // Phase 6: Foreign keys
        exec("ALTER TABLE ONLY pgd_users ADD CONSTRAINT pgd_users_org_fk FOREIGN KEY (org_id) REFERENCES pgd_orgs(id)");
        exec("ALTER TABLE ONLY pgd_roles ADD CONSTRAINT pgd_roles_user_fk FOREIGN KEY (user_id) REFERENCES pgd_users(id)");

        // Phase 7: Indexes
        exec("CREATE UNIQUE INDEX pgd_users_email_idx ON pgd_users (email)");
        exec("CREATE INDEX pgd_roles_user_idx ON pgd_roles (user_id)");

        // Verify everything works
        assertEquals("Acme Corp", query1("SELECT o.name FROM pgd_orgs o JOIN pgd_users u ON u.org_id = o.id WHERE u.name = 'Alice'"));
    }

    // =========================================================================
    // Sequence set after INSERT
    // =========================================================================

    @Test
    void testSetvalAfterInserts() throws SQLException {
        exec("CREATE TABLE pgd_setval (id bigint NOT NULL, name text)");
        exec("CREATE SEQUENCE pgd_setval_id_seq START WITH 1");
        exec("ALTER TABLE ONLY pgd_setval ALTER COLUMN id SET DEFAULT nextval('pgd_setval_id_seq'::regclass)");
        exec("INSERT INTO pgd_setval (id, name) VALUES (1, 'first'), (2, 'second'), (3, 'third')");
        exec("ALTER TABLE ONLY pgd_setval ADD CONSTRAINT pgd_setval_pkey PRIMARY KEY (id)");
        // pg_dump sets the sequence to the max id value after inserts
        exec("SELECT setval('pgd_setval_id_seq', (SELECT MAX(id) FROM pgd_setval))");
        // Next auto-generated id should be 4
        exec("INSERT INTO pgd_setval (name) VALUES ('fourth')");
        assertEquals("4", query1("SELECT id FROM pgd_setval WHERE name = 'fourth'"));
    }

    // =========================================================================
    // jsonb_to_recordset with inline column type definitions
    // =========================================================================

    @Test
    void testJsonbToRecordsetWithColumnTypes() throws SQLException {
        // Pattern: jsonb_to_recordset(data) AS x(col1 type, col2 type, ...)
        assertNotNull(query1("""
            SELECT x.name, x.age
            FROM jsonb_to_recordset('[{"name":"Alice","age":30},{"name":"Bob","age":25}]'::jsonb)
                AS x(name text, age int)
            ORDER BY x.age LIMIT 1
        """));
    }

    @Test
    void testJsonbToRecordsetInLateralJoin() throws SQLException {
        exec("CREATE TABLE config_holder (id serial PRIMARY KEY, rules jsonb)");
        exec("INSERT INTO config_holder (rules) VALUES ('[{\"type\":\"allow\",\"level\":1},{\"type\":\"deny\",\"level\":2}]')");
        assertNotNull(query1("""
            SELECT c.id, r.type, r.level
            FROM config_holder c
            LEFT JOIN LATERAL jsonb_to_recordset(c.rules) AS r(type text, level int) ON true
            LIMIT 1
        """));
    }

    @Test
    void testJsonbToRecordInView() throws SQLException {
        exec("CREATE TABLE rule_store (id serial PRIMARY KEY, payload jsonb)");
        exec("""
            CREATE VIEW expanded_rules AS
            SELECT rs.id, r.type, r.level
            FROM rule_store rs
            LEFT JOIN LATERAL jsonb_to_recordset(rs.payload) AS r(type text, level int) ON true
        """);
    }

    @Test
    void testJsonbToRecordSingle() throws SQLException {
        // json_to_record (single row, not set)
        assertNotNull(query1("""
            SELECT x.name
            FROM json_to_record('{"name":"test","age":30}') AS x(name text, age int)
        """));
    }

    // =========================================================================
    // Nested aggregate with ORDER BY in function arguments
    // =========================================================================

    @Test
    void testJsonbBuildObjectWithNestedAgg() throws SQLException {
        exec("CREATE TABLE agg_items (id serial PRIMARY KEY, category text, val int)");
        exec("INSERT INTO agg_items (category, val) VALUES ('a', 1), ('a', 2), ('b', 3)");
        assertNotNull(query1("""
            SELECT jsonb_build_object(
                'categories',
                jsonb_agg(jsonb_build_object('name', category, 'total', total) ORDER BY category)
            )
            FROM (
                SELECT category, SUM(val) AS total
                FROM agg_items
                GROUP BY category
            ) sub
        """));
    }

    @Test
    void testUpdateSetWithJsonbAggOrderBy() throws SQLException {
        // Real-world pattern: UPDATE SET col = (SELECT jsonb_build_object(..., JSONB_AGG(... ORDER BY ...)))
        exec("CREATE TABLE config_tbl (id serial PRIMARY KEY, config jsonb DEFAULT '{}')");
        exec("INSERT INTO config_tbl DEFAULT VALUES");
        exec("CREATE TABLE config_entries (id serial PRIMARY KEY, label text, priority int)");
        exec("INSERT INTO config_entries (label, priority) VALUES ('first', 1), ('second', 2)");
        exec("""
            UPDATE config_tbl SET config = (
                SELECT jsonb_build_object(
                    'entries',
                    jsonb_agg(jsonb_build_object('label', label) ORDER BY priority)
                )
                FROM config_entries
            )
            WHERE id = 1
        """);
        assertNotNull(query1("SELECT config FROM config_tbl WHERE id = 1"));
    }

    // =========================================================================
    // INSERT with schema-qualified table (public.tablename)
    // =========================================================================

    @Test
    void testInsertWithPublicSchemaPrefix() throws SQLException {
        exec("CREATE TABLE public.schema_prefix_test (id bigint NOT NULL, data text)");
        exec("INSERT INTO public.schema_prefix_test (id, data) VALUES (1, 'schema-qualified insert')");
        assertEquals("schema-qualified insert", query1("SELECT data FROM public.schema_prefix_test WHERE id = 1"));
    }

    @Test
    void testInsertWithOnConflictDoNothing() throws SQLException {
        // pg_dump pattern: INSERT ... ON CONFLICT DO NOTHING (for idempotent loads)
        exec("CREATE TABLE pgd_idempotent (id bigint NOT NULL, name text)");
        exec("ALTER TABLE ONLY pgd_idempotent ADD CONSTRAINT pgd_idempotent_pkey PRIMARY KEY (id)");
        exec("INSERT INTO pgd_idempotent VALUES (1, 'first') ON CONFLICT DO NOTHING");
        exec("INSERT INTO pgd_idempotent VALUES (1, 'duplicate') ON CONFLICT DO NOTHING");
        assertEquals("first", query1("SELECT name FROM pgd_idempotent WHERE id = 1"));
    }
}
