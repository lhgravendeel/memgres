package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE TABLE edge cases that still fail.
 *
 * v1 compat covered basic timestamp WITH/WITHOUT, GENERATED, PARTITION BY.
 * These tests cover remaining edge cases:
 * - Empty column list: CREATE TABLE t ()
 * - TRIM(BOTH FROM col) in CHECK constraints
 * - Complex CHECK with LENGTH + TRIM
 * - Multiple CHECK constraints on same table
 * - Long tables with many columns + constraints (parser state bug at position 402)
 * - Comma-leading style (each column on new line starting with comma)
 */
class CreateTableEdgeCaseCompatTest {

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
    // Empty column list: CREATE TABLE t ()
    // =========================================================================

    @Test
    void testCreateTableEmptyColumnList() throws SQLException {
        exec("CREATE TABLE empty_tbl ()");
        // PG allows this: table exists but has no columns
    }

    @Test
    void testCreateTableEmptyThenAlterAddColumn() throws SQLException {
        exec("CREATE TABLE empty_then_add ()");
        exec("ALTER TABLE empty_then_add ADD COLUMN id serial PRIMARY KEY");
        exec("ALTER TABLE empty_then_add ADD COLUMN name text");
        exec("INSERT INTO empty_then_add (name) VALUES ('added_later')");
        assertEquals("added_later", query1("SELECT name FROM empty_then_add"));
    }

    @Test
    void testCreateTableEmptyInherits() throws SQLException {
        // Some frameworks create empty tables that inherit from a parent
        exec("CREATE TABLE base_empty (id serial PRIMARY KEY, name text)");
        exec("CREATE TABLE child_empty () INHERITS (base_empty)");
        exec("INSERT INTO child_empty (name) VALUES ('inherited')");
        assertEquals("inherited", query1("SELECT name FROM child_empty"));
    }

    // =========================================================================
    // CHECK constraint with TRIM(BOTH FROM col)
    // =========================================================================

    @Test
    void testCheckWithTrimBothFrom() throws SQLException {
        // pg_dump outputs: CHECK (((length(TRIM(BOTH FROM col)) >= 1) AND (length(TRIM(BOTH FROM col)) <= 1024)))
        exec("""
            CREATE TABLE trim_check (
                id serial PRIMARY KEY,
                description text NOT NULL,
                CONSTRAINT chk_desc CHECK (
                    (length(TRIM(BOTH FROM description)) >= 1)
                    AND (length(TRIM(BOTH FROM description)) <= 1024)
                )
            )
        """);
        exec("INSERT INTO trim_check (description) VALUES ('valid text')");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO trim_check (description) VALUES ('   ')"));
    }

    @Test
    void testCheckWithTrimLeading() throws SQLException {
        exec("""
            CREATE TABLE trim_leading (
                id serial PRIMARY KEY,
                code text NOT NULL,
                CONSTRAINT chk_code CHECK (length(TRIM(LEADING FROM code)) > 0)
            )
        """);
    }

    @Test
    void testCheckWithTrimTrailing() throws SQLException {
        exec("""
            CREATE TABLE trim_trailing (
                id serial PRIMARY KEY,
                val text NOT NULL,
                CONSTRAINT chk_val CHECK (length(TRIM(TRAILING ' ' FROM val)) > 0)
            )
        """);
    }

    // =========================================================================
    // Complex CHECK with nested functions
    // =========================================================================

    @Test
    void testCheckWithNestedFunctionCalls() throws SQLException {
        exec("""
            CREATE TABLE complex_check (
                id bigint NOT NULL,
                name text NOT NULL,
                data text,
                CONSTRAINT check_name CHECK (((length(TRIM(BOTH FROM name)) >= 1) AND (length(TRIM(BOTH FROM name)) <= 255))),
                CONSTRAINT check_data CHECK ((data IS NULL) OR (length(data) <= 10000))
            )
        """);
    }

    // =========================================================================
    // Long table with many columns (parser state test)
    // =========================================================================

    @Test
    void testLongTableManyColumns() throws SQLException {
        // A real-world failure at position 402 was a table with ~10 columns + constraints
        // where the parser lost track after many column definitions
        exec("""
            CREATE TABLE long_table (
                id bigint NOT NULL,
                project_id bigint NOT NULL,
                request_id bigint NOT NULL,
                policy_id bigint NOT NULL,
                user_id bigint,
                created_at timestamp with time zone NOT NULL,
                updated_at timestamp with time zone NOT NULL,
                reason text NOT NULL,
                category text,
                priority int DEFAULT 0,
                CONSTRAINT chk_reason CHECK (((length(TRIM(BOTH FROM reason)) >= 1) AND (length(TRIM(BOTH FROM reason)) <= 1024)))
            )
        """);
    }

    @Test
    void testLongTableWithMultipleTimestampTzColumns() throws SQLException {
        exec("""
            CREATE TABLE multi_ts (
                id bigint NOT NULL,
                created_at timestamp with time zone NOT NULL DEFAULT now(),
                updated_at timestamp with time zone NOT NULL DEFAULT now(),
                deleted_at timestamp with time zone,
                last_seen_at timestamp with time zone,
                expires_at timestamp with time zone,
                started_at timestamp without time zone,
                ended_at timestamp without time zone
            )
        """);
    }

    // =========================================================================
    // Comma-leading column definition style
    // =========================================================================

    @Test
    void testCommaLeadingStyle() throws SQLException {
        exec("""
            CREATE TABLE comma_leading (
                id TEXT NOT NULL CHECK (id <> '') PRIMARY KEY
                , name TEXT NOT NULL CHECK (name <> '')
                , description TEXT
                , is_active BOOLEAN DEFAULT true
                , created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
                , updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
            )
        """);
        exec("INSERT INTO comma_leading (id, name) VALUES ('1', 'test')");
        assertEquals("test", query1("SELECT name FROM comma_leading WHERE id = '1'"));
    }

    @Test
    void testCommaLeadingWithForeignKey() throws SQLException {
        exec("CREATE TABLE cl_parent (id TEXT PRIMARY KEY CHECK (id <> ''))");
        exec("""
            CREATE TABLE cl_child (
                id TEXT NOT NULL PRIMARY KEY
                , parent_id TEXT NOT NULL
                , value TEXT
                , FOREIGN KEY (parent_id) REFERENCES cl_parent(id) ON DELETE CASCADE
            )
        """);
    }

    // =========================================================================
    // pg_dump DEFAULT expressions with regclass cast
    // =========================================================================

    @Test
    void testDefaultNextvalRegclass() throws SQLException {
        exec("CREATE SEQUENCE regclass_seq START WITH 1");
        exec("CREATE TABLE regclass_default (id bigint DEFAULT nextval('regclass_seq'::regclass) NOT NULL, name text)");
        exec("INSERT INTO regclass_default (name) VALUES ('test')");
        assertEquals("1", query1("SELECT id FROM regclass_default"));
    }

    // =========================================================================
    // Multiple constraints on same table (named + unnamed)
    // =========================================================================

    @Test
    void testMultipleNamedConstraints() throws SQLException {
        exec("""
            CREATE TABLE multi_constraint (
                id serial PRIMARY KEY,
                code text NOT NULL CONSTRAINT code_not_empty CHECK (code <> ''),
                val int NOT NULL CONSTRAINT val_positive CHECK (val > 0),
                category text CONSTRAINT valid_category CHECK (category IN ('a', 'b', 'c')),
                CONSTRAINT code_length CHECK (length(code) <= 50)
            )
        """);
    }

    // =========================================================================
    // DEFAULT with complex expression
    // =========================================================================

    @Test
    void testDefaultEmptyJsonbObject() throws SQLException {
        exec("CREATE TABLE json_defaults (id serial PRIMARY KEY, config jsonb DEFAULT '{}'::jsonb NOT NULL, tags jsonb DEFAULT '[]'::jsonb NOT NULL)");
        exec("INSERT INTO json_defaults DEFAULT VALUES");
        assertEquals("{}", query1("SELECT config FROM json_defaults"));
    }

    @Test
    void testDefaultEmptyArray() throws SQLException {
        exec("CREATE TABLE array_defaults (id serial PRIMARY KEY, items text[] DEFAULT '{}'::text[] NOT NULL, scores int[] DEFAULT ARRAY[]::int[])");
        exec("INSERT INTO array_defaults DEFAULT VALUES");
        assertNotNull(query1("SELECT items FROM array_defaults"));
    }

    @Test
    void testDefaultWithGenRandomUuid() throws SQLException {
        exec("CREATE TABLE uuid_defaults (id uuid DEFAULT gen_random_uuid() PRIMARY KEY, name text)");
        exec("INSERT INTO uuid_defaults (name) VALUES ('auto_uuid')");
        assertNotNull(query1("SELECT id FROM uuid_defaults"));
    }

    // =========================================================================
    // BIGINT GENERATED ALWAYS AS IDENTITY (pg_dump output)
    // =========================================================================

    @Test
    void testBigintGeneratedAlwaysAsIdentity() throws SQLException {
        exec("CREATE TABLE identity_tbl (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text)");
        exec("INSERT INTO identity_tbl (name) VALUES ('auto')");
        assertEquals("1", query1("SELECT id FROM identity_tbl"));
    }

    @Test
    void testBigintGeneratedByDefaultAsIdentityWithStartAndIncrement() throws SQLException {
        exec("CREATE TABLE identity_opts (id bigint GENERATED BY DEFAULT AS IDENTITY (START WITH 1000 INCREMENT BY 5) PRIMARY KEY, name text)");
        exec("INSERT INTO identity_opts (name) VALUES ('a')");
        assertEquals("1000", query1("SELECT id FROM identity_opts"));
        exec("INSERT INTO identity_opts (name) VALUES ('b')");
        assertEquals("1005", query1("SELECT id FROM identity_opts WHERE name = 'b'"));
    }

    // =========================================================================
    // SERIAL / BIGSERIAL in schema-qualified table
    // =========================================================================

    @Test
    void testSerialInSchemaQualifiedTable() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS seq_test");
        exec("CREATE TABLE seq_test.data (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO seq_test.data (val) VALUES ('test')");
        assertEquals("1", query1("SELECT id FROM seq_test.data"));
    }

    // =========================================================================
    // Table with LIKE ... INCLUDING ALL
    // =========================================================================

    @Test
    void testCreateTableLikeIncludingAll() throws SQLException {
        exec("CREATE TABLE like_source (id serial PRIMARY KEY, name text NOT NULL, status text DEFAULT 'active' CHECK (status IN ('active', 'inactive')))");
        exec("CREATE TABLE like_target (LIKE like_source INCLUDING ALL)");
        exec("INSERT INTO like_target (name) VALUES ('cloned')");
        assertEquals("active", query1("SELECT status FROM like_target"));
    }

    @Test
    void testCreateTableLikeIncludingConstraints() throws SQLException {
        exec("CREATE TABLE like_src2 (id serial PRIMARY KEY, val int CHECK (val > 0))");
        exec("CREATE TABLE like_tgt2 (LIKE like_src2 INCLUDING CONSTRAINTS)");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO like_tgt2 (val) VALUES (-1)"));
    }

    @Test
    void testCreateTableLikeIncludingIndexes() throws SQLException {
        exec("CREATE TABLE like_src3 (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_like_src3 ON like_src3 (name)");
        exec("CREATE TABLE like_tgt3 (LIKE like_src3 INCLUDING INDEXES)");
    }

    @Test
    void testCreateTableLikeExcludingAll() throws SQLException {
        exec("CREATE TABLE like_src4 (id serial PRIMARY KEY, name text DEFAULT 'default', val int CHECK (val > 0))");
        exec("CREATE TABLE like_tgt4 (LIKE like_src4 EXCLUDING ALL)");
        // Should have columns but no defaults or constraints
    }
}
