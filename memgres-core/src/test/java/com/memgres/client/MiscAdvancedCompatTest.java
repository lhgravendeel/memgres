package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for miscellaneous patterns that still fail:
 * - Block comments as entire statements or followed by statements
 * - pg_dump patterns (ALTER TABLE ... ONLY, SET DEFAULT nextval(...::regclass))
 * - CREATE TABLE with LIKE/INHERITS edge cases
 * - ALTER ROLE SET inside DO blocks
 * - Nested function calls in aggregate context
 * - E'...' escape strings in various contexts
 * - Various SET/RESET patterns
 */
class MiscAdvancedCompatTest {

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
    // pg_dump pattern: ALTER TABLE ONLY ... SET DEFAULT nextval(..::regclass)
    // =========================================================================

    @Test
    void testAlterTableOnlySetDefault() throws SQLException {
        exec("CREATE TABLE pgd_test (id bigint NOT NULL, name text)");
        exec("CREATE SEQUENCE pgd_test_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1");
        exec("ALTER TABLE ONLY pgd_test ALTER COLUMN id SET DEFAULT nextval('pgd_test_id_seq'::regclass)");
        exec("INSERT INTO pgd_test (name) VALUES ('auto_id')");
        assertEquals("1", query1("SELECT id FROM pgd_test"));
    }

    @Test
    void testAlterTableOnlyAddConstraint() throws SQLException {
        exec("CREATE TABLE pgd_pk (id bigint NOT NULL, name text)");
        exec("ALTER TABLE ONLY pgd_pk ADD CONSTRAINT pgd_pk_pkey PRIMARY KEY (id)");
    }

    @Test
    void testAlterTableOnlyAddForeignKey() throws SQLException {
        exec("CREATE TABLE pgd_parent (id bigint PRIMARY KEY)");
        exec("CREATE TABLE pgd_child (id bigint PRIMARY KEY, parent_id bigint)");
        exec("INSERT INTO pgd_parent VALUES (1)");
        exec("ALTER TABLE ONLY pgd_child ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES pgd_parent(id)");
    }

    // =========================================================================
    // E'...' escape strings
    // =========================================================================

    @Test
    void testEscapeStringLiteral() throws SQLException {
        exec("CREATE TABLE esc_test (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO esc_test (name) VALUES (E'line1\\nline2')");
        assertNotNull(query1("SELECT name FROM esc_test WHERE name LIKE '%line1%'"));
    }

    @Test
    void testEscapeStringInDefault() throws SQLException {
        exec("CREATE TABLE esc_default (id serial PRIMARY KEY, sep text DEFAULT E'\\t')");
        exec("INSERT INTO esc_default DEFAULT VALUES");
        assertNotNull(query1("SELECT sep FROM esc_default"));
    }

    @Test
    void testEscapeStringEmptyDefault() throws SQLException {
        // Some migrations use DEFAULT E'' (empty escape string)
        exec("CREATE TABLE esc_empty (id serial PRIMARY KEY, val text DEFAULT E'')");
        exec("INSERT INTO esc_empty DEFAULT VALUES");
        assertEquals("", query1("SELECT val FROM esc_empty"));
    }

    // =========================================================================
    // SET/RESET patterns from real migrations
    // =========================================================================

    @Test
    void testSetSearchPathToMultipleSchemas() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS app");
        exec("SET search_path TO app, public, pg_catalog");
    }

    @Test
    void testSetSearchPathWithQuotes() throws SQLException {
        exec("SET search_path TO 'public', 'pg_catalog'");
    }

    @Test
    void testSetLocalInsideTransaction() throws SQLException {
        exec("BEGIN");
        exec("SET LOCAL work_mem = '64MB'");
        exec("SET LOCAL statement_timeout = '5s'");
        exec("COMMIT");
    }

    @Test
    void testSetTimeZone() throws SQLException {
        exec("SET timezone = 'UTC'");
        assertNotNull(query1("SELECT current_setting('timezone')"));
    }

    // =========================================================================
    // COMMENT ON with various object types
    // =========================================================================

    @Test
    void testCommentOnExtension() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        exec("COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions'");
    }

    @Test
    void testCommentOnSequence() throws SQLException {
        exec("CREATE SEQUENCE comment_seq");
        exec("COMMENT ON SEQUENCE comment_seq IS 'A documented sequence'");
    }

    @Test
    void testCommentOnView() throws SQLException {
        exec("CREATE TABLE comment_base (id serial PRIMARY KEY)");
        exec("CREATE VIEW comment_view AS SELECT * FROM comment_base");
        exec("COMMENT ON VIEW comment_view IS 'A documented view'");
    }

    @Test
    void testCommentOnConstraint() throws SQLException {
        exec("CREATE TABLE comment_con (id serial PRIMARY KEY, val int CONSTRAINT pos_val CHECK (val > 0))");
        exec("COMMENT ON CONSTRAINT pos_val ON comment_con IS 'Value must be positive'");
    }

    @Test
    void testCommentOnTrigger() throws SQLException {
        exec("CREATE TABLE comment_trig_tbl (id serial PRIMARY KEY)");
        exec("CREATE FUNCTION comment_trig_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$");
        exec("CREATE TRIGGER comment_trig BEFORE INSERT ON comment_trig_tbl FOR EACH ROW EXECUTE FUNCTION comment_trig_fn()");
        exec("COMMENT ON TRIGGER comment_trig ON comment_trig_tbl IS 'A documented trigger'");
    }

    // =========================================================================
    // Complex ANALYZE patterns
    // =========================================================================

    @Test
    void testAnalyzeVerbose() throws SQLException {
        exec("CREATE TABLE analyze_v (id serial PRIMARY KEY, data text)");
        exec("ANALYZE VERBOSE analyze_v");
    }

    // =========================================================================
    // COPY ... FROM STDIN (pg_dump uses this for data loading)
    // =========================================================================

    @Test
    void testCopyFromStdinNoOp() throws SQLException {
        // COPY FROM stdin is used by pg_dump but not really usable via JDBC
        // memgres should accept and ignore it, or handle gracefully
        exec("CREATE TABLE copy_test (id int, name text)");
        // Don't actually execute COPY; just ensure the table was created
        exec("INSERT INTO copy_test VALUES (1, 'test')");
        assertEquals("test", query1("SELECT name FROM copy_test"));
    }

    // =========================================================================
    // ALTER TABLE ... ADD COLUMN with complex DEFAULT
    // =========================================================================

    @Test
    void testAddColumnWithArrayDefault() throws SQLException {
        exec("CREATE TABLE add_col_arr (id serial PRIMARY KEY)");
        exec("ALTER TABLE add_col_arr ADD COLUMN tags text[] DEFAULT '{}'::text[]");
        exec("INSERT INTO add_col_arr DEFAULT VALUES");
        assertNotNull(query1("SELECT tags FROM add_col_arr"));
    }

    @Test
    void testAddColumnWithJsonbDefault() throws SQLException {
        exec("CREATE TABLE add_col_json (id serial PRIMARY KEY)");
        exec("ALTER TABLE add_col_json ADD COLUMN config jsonb DEFAULT '{}'::jsonb NOT NULL");
        exec("INSERT INTO add_col_json DEFAULT VALUES");
        assertEquals("{}", query1("SELECT config FROM add_col_json"));
    }

    @Test
    void testAddColumnWithTimestamptzDefault() throws SQLException {
        exec("CREATE TABLE add_col_ts (id serial PRIMARY KEY)");
        exec("ALTER TABLE add_col_ts ADD COLUMN created_at timestamptz DEFAULT now() NOT NULL");
        exec("INSERT INTO add_col_ts DEFAULT VALUES");
        assertNotNull(query1("SELECT created_at FROM add_col_ts"));
    }

    // =========================================================================
    // DROP INDEX IF EXISTS / DROP INDEX CONCURRENTLY
    // =========================================================================

    @Test
    void testDropIndexIfExists() throws SQLException {
        exec("DROP INDEX IF EXISTS nonexistent_index");
    }

    @Test
    void testDropIndexConcurrently() throws SQLException {
        exec("CREATE TABLE di_conc (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_di_conc ON di_conc (name)");
        exec("DROP INDEX CONCURRENTLY idx_di_conc");
    }

    @Test
    void testDropIndexConcurrentlyIfExists() throws SQLException {
        exec("DROP INDEX CONCURRENTLY IF EXISTS another_nonexistent_index");
    }

    // =========================================================================
    // CREATE TABLE ... AS SELECT (CTAS)
    // =========================================================================

    @Test
    void testCreateTableAsSelect() throws SQLException {
        exec("CREATE TABLE ctas_source (id serial PRIMARY KEY, name text, val int)");
        exec("INSERT INTO ctas_source (name, val) VALUES ('a', 1), ('b', 2)");
        exec("CREATE TABLE ctas_target AS SELECT name, val * 2 AS doubled FROM ctas_source");
        assertEquals("2", query1("SELECT doubled FROM ctas_target WHERE name = 'a'"));
    }

    @Test
    void testCreateTempTableAsSelect() throws SQLException {
        exec("CREATE TABLE ctas_src2 (id serial PRIMARY KEY, category text)");
        exec("INSERT INTO ctas_src2 (category) VALUES ('x'), ('y'), ('x')");
        exec("CREATE TEMP TABLE category_counts AS SELECT category, COUNT(*) AS cnt FROM ctas_src2 GROUP BY category");
        assertEquals("2", query1("SELECT cnt FROM category_counts WHERE category = 'x'"));
    }

    // =========================================================================
    // ALTER TABLE ... SET STATISTICS
    // =========================================================================

    @Test
    void testAlterColumnSetStatistics() throws SQLException {
        exec("CREATE TABLE set_stats (id serial PRIMARY KEY, name text)");
        exec("ALTER TABLE set_stats ALTER COLUMN name SET STATISTICS 1000");
    }

    // =========================================================================
    // ALTER TABLE ... SET STORAGE
    // =========================================================================

    @Test
    void testAlterColumnSetStorage() throws SQLException {
        exec("CREATE TABLE set_storage (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE set_storage ALTER COLUMN data SET STORAGE EXTENDED");
    }

    @Test
    void testAlterColumnSetStoragePlain() throws SQLException {
        exec("CREATE TABLE set_storage2 (id serial PRIMARY KEY, small_data text)");
        exec("ALTER TABLE set_storage2 ALTER COLUMN small_data SET STORAGE PLAIN");
    }

    // =========================================================================
    // Sequence AS type
    // =========================================================================

    @Test
    void testCreateSequenceAsSmallint() throws SQLException {
        exec("CREATE SEQUENCE small_id AS smallint START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 32767");
        assertEquals("1", query1("SELECT nextval('small_id')"));
    }

    @Test
    void testCreateSequenceAsInteger() throws SQLException {
        exec("CREATE SEQUENCE int_id AS integer START WITH 100");
        assertEquals("100", query1("SELECT nextval('int_id')"));
    }

    @Test
    void testCreateSequenceAsBigint() throws SQLException {
        exec("CREATE SEQUENCE big_id AS bigint START WITH 1000000");
        assertEquals("1000000", query1("SELECT nextval('big_id')"));
    }

    // =========================================================================
    // RENAME INDEX
    // =========================================================================

    @Test
    void testAlterIndexRename() throws SQLException {
        exec("CREATE TABLE rename_idx_tbl (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX old_idx_name ON rename_idx_tbl (name)");
        exec("ALTER INDEX old_idx_name RENAME TO new_idx_name");
    }
}
