package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE INDEX syntax variants found in real-world schemas.
 */
class CreateIndexCompatTest {

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

    // =========================================================================
    // Schema-qualified table in CREATE INDEX
    // =========================================================================

    @Test
    void testIndexOnSchemaQualifiedTable() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS idx_schema");
        exec("CREATE TABLE idx_schema.entries (id serial PRIMARY KEY, name text, created_at timestamp)");
        exec("CREATE INDEX idx_entries_name ON idx_schema.entries (name)");
        exec("CREATE INDEX idx_entries_created ON idx_schema.entries USING btree (created_at)");
    }

    @Test
    void testIndexOnSchemaQualifiedTableWithDot() throws SQLException {
        // pg_dump outputs schema.table in CREATE INDEX
        exec("CREATE SCHEMA IF NOT EXISTS myschema");
        exec("CREATE TABLE myschema.data_points (id serial PRIMARY KEY, ts timestamp, val numeric)");
        exec("CREATE INDEX idx_dp_ts ON myschema.data_points USING btree (ts)");
    }

    // =========================================================================
    // CREATE INDEX ON ONLY (partitioned tables)
    // =========================================================================

    @Test
    void testIndexOnOnly() throws SQLException {
        // ON ONLY creates index on parent partition only (children get matching indexes)
        exec("CREATE TABLE part_idx (id bigserial, ts date NOT NULL, data text) PARTITION BY RANGE (ts)");
        exec("CREATE INDEX idx_part_ts ON ONLY part_idx USING btree (ts)");
    }

    @Test
    void testIndexOnOnlyWithWhere() throws SQLException {
        exec("CREATE TABLE part_idx2 (id bigserial, ts date NOT NULL, deleted boolean DEFAULT false) PARTITION BY RANGE (ts)");
        exec("CREATE INDEX idx_part_active ON ONLY part_idx2 USING btree (ts, id) WHERE (deleted IS NOT TRUE)");
    }

    // =========================================================================
    // CREATE INDEX CONCURRENTLY
    // =========================================================================

    @Test
    void testCreateIndexConcurrently() throws SQLException {
        exec("CREATE TABLE conc_idx_test (id serial PRIMARY KEY, email text, created_at timestamp)");
        exec("CREATE INDEX CONCURRENTLY idx_conc_email ON conc_idx_test (email)");
    }

    @Test
    void testCreateIndexConcurrentlyIfNotExists() throws SQLException {
        exec("CREATE TABLE conc_idx_test2 (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_conc_name ON conc_idx_test2 (name)");
        // Idempotent
        exec("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_conc_name ON conc_idx_test2 (name)");
    }

    @Test
    void testCreateUniqueIndexConcurrently() throws SQLException {
        exec("CREATE TABLE conc_uniq_test (id serial PRIMARY KEY, code text NOT NULL)");
        exec("CREATE UNIQUE INDEX CONCURRENTLY idx_conc_uniq_code ON conc_uniq_test (code)");
    }

    // =========================================================================
    // Partial indexes with WHERE
    // =========================================================================

    @Test
    void testPartialIndexIsNotNull() throws SQLException {
        exec("CREATE TABLE partial_idx (id serial PRIMARY KEY, email text, confirmed boolean)");
        exec("CREATE INDEX idx_confirmed_emails ON partial_idx (email) WHERE (confirmed IS TRUE)");
    }

    @Test
    void testPartialIndexMultiCondition() throws SQLException {
        exec("CREATE TABLE partial_idx2 (id serial PRIMARY KEY, status text, deleted_at timestamp)");
        exec("CREATE INDEX idx_active ON partial_idx2 (status) WHERE (deleted_at IS NULL AND status != 'archived')");
    }

    @Test
    void testPartialIndexWithIsNotTrue() throws SQLException {
        exec("CREATE TABLE partial_idx3 (id serial PRIMARY KEY, hidden boolean DEFAULT false)");
        exec("CREATE INDEX idx_visible ON partial_idx3 (id) WHERE (hidden IS NOT TRUE)");
    }

    // =========================================================================
    // USING btree / hash / gin / gist
    // =========================================================================

    @Test
    void testIndexUsingBtree() throws SQLException {
        exec("CREATE TABLE idx_method (id serial PRIMARY KEY, name text, tags jsonb)");
        exec("CREATE INDEX idx_btree ON idx_method USING btree (name)");
    }

    @Test
    void testIndexUsingHash() throws SQLException {
        exec("CREATE TABLE idx_hash_test (id serial PRIMARY KEY, code text)");
        exec("CREATE INDEX idx_hash ON idx_hash_test USING hash (code)");
    }

    @Test
    void testIndexUsingGin() throws SQLException {
        exec("CREATE TABLE idx_gin_test (id serial PRIMARY KEY, data jsonb)");
        exec("CREATE INDEX idx_gin ON idx_gin_test USING gin (data)");
    }

    @Test
    void testIndexUsingGinJsonbPathOps() throws SQLException {
        exec("CREATE TABLE idx_gin_path (id serial PRIMARY KEY, metadata jsonb)");
        exec("CREATE INDEX idx_gin_path_ops ON idx_gin_path USING gin (metadata jsonb_path_ops)");
    }

    @Test
    void testIndexUsingGist() throws SQLException {
        exec("CREATE TABLE idx_gist_test (id serial PRIMARY KEY, location point)");
        exec("CREATE INDEX idx_gist ON idx_gist_test USING gist (location)");
    }

    // =========================================================================
    // Expression indexes
    // =========================================================================

    @Test
    void testExpressionIndexLower() throws SQLException {
        exec("CREATE TABLE expr_idx (id serial PRIMARY KEY, email text)");
        exec("CREATE UNIQUE INDEX idx_email_lower ON expr_idx (lower(email))");
    }

    @Test
    void testExpressionIndexCoalesce() throws SQLException {
        exec("CREATE TABLE expr_idx2 (id serial PRIMARY KEY, name text, display_name text)");
        exec("CREATE INDEX idx_display ON expr_idx2 (COALESCE(display_name, name))");
    }

    // =========================================================================
    // Multi-column indexes with mixed ASC/DESC and NULLS FIRST/LAST
    // =========================================================================

    @Test
    void testMultiColumnIndexOrdering() throws SQLException {
        exec("CREATE TABLE multi_idx (id serial PRIMARY KEY, a int, b int, c timestamp)");
        exec("CREATE INDEX idx_multi_order ON multi_idx (a ASC, b DESC NULLS LAST, c DESC NULLS FIRST)");
    }

    // =========================================================================
    // INCLUDE columns (covering index)
    // =========================================================================

    @Test
    void testIndexWithInclude() throws SQLException {
        exec("CREATE TABLE covering_test (id serial PRIMARY KEY, code text, name text, email text)");
        exec("CREATE INDEX idx_covering ON covering_test (code) INCLUDE (name, email)");
    }

    // =========================================================================
    // IF NOT EXISTS
    // =========================================================================

    @Test
    void testCreateIndexIfNotExists() throws SQLException {
        exec("CREATE TABLE idx_ine_test (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX IF NOT EXISTS idx_ine ON idx_ine_test (name)");
        exec("CREATE INDEX IF NOT EXISTS idx_ine ON idx_ine_test (name)");
        // Should not error on second call
    }

    // =========================================================================
    // Index on partition child directly (schema-qualified)
    // =========================================================================

    @Test
    void testIndexOnPartitionChild() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS partitions");
        exec("CREATE TABLE partitions.child_tbl (id serial PRIMARY KEY, ts date, data text)");
        exec("CREATE INDEX idx_child_ts ON partitions.child_tbl USING btree (ts, id)");
    }

    // =========================================================================
    // REINDEX
    // =========================================================================

    @Test
    void testReindexTable() throws SQLException {
        exec("CREATE TABLE reindex_test (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_reindex ON reindex_test (name)");
        exec("REINDEX TABLE reindex_test");
    }

    @Test
    void testReindexIndex() throws SQLException {
        exec("CREATE TABLE reindex_test2 (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_reindex2 ON reindex_test2 (name)");
        exec("REINDEX INDEX idx_reindex2");
    }
}
