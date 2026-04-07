package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for miscellaneous SQL features found in real-world schemas:
 * LISTEN/NOTIFY, LOCK TABLE, VACUUM/ANALYZE, COPY, CREATE RULE,
 * CREATE PUBLICATION, COMMENT ON, and other utility commands.
 */
class MiscCompatTest {

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
    // LISTEN / NOTIFY
    // =========================================================================

    @Test
    void testListen() throws SQLException {
        exec("LISTEN my_channel");
    }

    @Test
    void testNotify() throws SQLException {
        exec("NOTIFY my_channel");
    }

    @Test
    void testNotifyWithPayload() throws SQLException {
        exec("NOTIFY my_channel, 'hello world'");
    }

    @Test
    void testUnlisten() throws SQLException {
        exec("LISTEN test_channel");
        exec("UNLISTEN test_channel");
    }

    @Test
    void testUnlistenAll() throws SQLException {
        exec("LISTEN ch1");
        exec("LISTEN ch2");
        exec("UNLISTEN *");
    }

    @Test
    void testPgNotify() throws SQLException {
        exec("SELECT pg_notify('my_channel', 'payload')");
    }

    // =========================================================================
    // LOCK TABLE
    // =========================================================================

    @Test
    void testLockTable() throws SQLException {
        exec("CREATE TABLE lock_test (id serial PRIMARY KEY)");
        exec("BEGIN");
        exec("LOCK TABLE lock_test IN ACCESS SHARE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableExclusive() throws SQLException {
        exec("CREATE TABLE lock_excl (id serial PRIMARY KEY)");
        exec("BEGIN");
        exec("LOCK TABLE lock_excl IN EXCLUSIVE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableAccessExclusive() throws SQLException {
        exec("CREATE TABLE lock_ae (id serial PRIMARY KEY)");
        exec("BEGIN");
        exec("LOCK TABLE lock_ae IN ACCESS EXCLUSIVE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableShareRowExclusive() throws SQLException {
        exec("CREATE TABLE lock_sre (id serial PRIMARY KEY)");
        exec("BEGIN");
        exec("LOCK TABLE lock_sre IN SHARE ROW EXCLUSIVE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableNowait() throws SQLException {
        exec("CREATE TABLE lock_nw (id serial PRIMARY KEY)");
        exec("BEGIN");
        exec("LOCK TABLE lock_nw IN ACCESS EXCLUSIVE MODE NOWAIT");
        exec("COMMIT");
    }

    // =========================================================================
    // VACUUM / ANALYZE
    // =========================================================================

    @Test
    void testVacuum() throws SQLException {
        exec("CREATE TABLE vacuum_test (id serial PRIMARY KEY, data text)");
        exec("INSERT INTO vacuum_test (data) VALUES ('test')");
        exec("VACUUM vacuum_test");
    }

    @Test
    void testVacuumFull() throws SQLException {
        exec("CREATE TABLE vacuum_full_test (id serial PRIMARY KEY)");
        exec("VACUUM FULL vacuum_full_test");
    }

    @Test
    void testAnalyze() throws SQLException {
        exec("CREATE TABLE analyze_test (id serial PRIMARY KEY, val int)");
        exec("INSERT INTO analyze_test (val) VALUES (1), (2), (3)");
        exec("ANALYZE analyze_test");
    }

    @Test
    void testVacuumAnalyze() throws SQLException {
        exec("CREATE TABLE va_test (id serial PRIMARY KEY)");
        exec("VACUUM ANALYZE va_test");
    }

    @Test
    void testAnalyzeSpecificColumns() throws SQLException {
        exec("CREATE TABLE analyze_cols (id serial PRIMARY KEY, a int, b text)");
        exec("ANALYZE analyze_cols (a, b)");
    }

    // =========================================================================
    // CREATE RULE
    // =========================================================================

    @Test
    void testCreateRuleDoNothing() throws SQLException {
        exec("CREATE TABLE rule_test (id serial PRIMARY KEY, data text)");
        exec("CREATE RULE ignore_inserts AS ON INSERT TO rule_test DO INSTEAD NOTHING");
        exec("INSERT INTO rule_test (data) VALUES ('should be ignored')");
        assertEquals("0", query1("SELECT COUNT(*) FROM rule_test"));
    }

    @Test
    void testCreateRuleAlso() throws SQLException {
        exec("CREATE TABLE rule_source (id serial PRIMARY KEY, val text)");
        exec("CREATE TABLE rule_audit (source_id int, action text, ts timestamp DEFAULT now())");
        exec("""
            CREATE RULE log_inserts AS ON INSERT TO rule_source
            DO ALSO INSERT INTO rule_audit (source_id, action) VALUES (NEW.id, 'INSERT')
        """);
    }

    @Test
    void testCreateRuleInstead() throws SQLException {
        exec("CREATE TABLE rule_view_base (id serial PRIMARY KEY, name text)");
        exec("CREATE VIEW rule_view AS SELECT * FROM rule_view_base");
        exec("""
            CREATE RULE rule_view_insert AS ON INSERT TO rule_view
            DO INSTEAD INSERT INTO rule_view_base (name) VALUES (NEW.name)
        """);
    }

    // =========================================================================
    // COMMENT ON
    // =========================================================================

    @Test
    void testCommentOnTable() throws SQLException {
        exec("CREATE TABLE commented_tbl (id serial PRIMARY KEY)");
        exec("COMMENT ON TABLE commented_tbl IS 'This is a test table'");
    }

    @Test
    void testCommentOnColumn() throws SQLException {
        exec("CREATE TABLE commented_col (id serial PRIMARY KEY, val text)");
        exec("COMMENT ON COLUMN commented_col.val IS 'The value column'");
    }

    @Test
    void testCommentOnFunction() throws SQLException {
        exec("CREATE FUNCTION comment_fn() RETURNS void LANGUAGE plpgsql AS $$ BEGIN END; $$");
        exec("COMMENT ON FUNCTION comment_fn() IS 'A documented function'");
    }

    @Test
    void testCommentOnIndex() throws SQLException {
        exec("CREATE TABLE ci_test (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_ci ON ci_test (name)");
        exec("COMMENT ON INDEX idx_ci IS 'Name lookup index'");
    }

    @Test
    void testCommentOnSchema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS documented_schema");
        exec("COMMENT ON SCHEMA documented_schema IS 'A documented schema'");
    }

    @Test
    void testCommentOnType() throws SQLException {
        exec("CREATE TYPE doc_status AS ENUM ('a', 'b')");
        exec("COMMENT ON TYPE doc_status IS 'Status enum documentation'");
    }

    // =========================================================================
    // REINDEX
    // =========================================================================

    @Test
    void testReindexTable() throws SQLException {
        exec("CREATE TABLE reindex_tbl (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_reindex ON reindex_tbl (name)");
        exec("REINDEX TABLE reindex_tbl");
    }

    @Test
    void testReindexIndex() throws SQLException {
        exec("CREATE TABLE reindex_tbl2 (id serial PRIMARY KEY, name text)");
        exec("CREATE INDEX idx_reindex2 ON reindex_tbl2 (name)");
        exec("REINDEX INDEX idx_reindex2");
    }

    // =========================================================================
    // CREATE PUBLICATION / SUBSCRIPTION (logical replication)
    // =========================================================================

    @Test
    void testCreatePublication() throws SQLException {
        exec("CREATE TABLE pub_test (id serial PRIMARY KEY, data text)");
        exec("CREATE PUBLICATION my_pub FOR TABLE pub_test");
    }

    @Test
    void testCreatePublicationForAllTables() throws SQLException {
        exec("CREATE PUBLICATION all_pub FOR ALL TABLES");
    }

    @Test
    void testAlterPublication() throws SQLException {
        exec("CREATE TABLE pub_alt_test (id serial PRIMARY KEY)");
        exec("CREATE PUBLICATION alt_pub FOR TABLE pub_alt_test");
        exec("ALTER PUBLICATION alt_pub DROP TABLE pub_alt_test");
    }

    // =========================================================================
    // ALTER TABLE ... SET (storage parameters)
    // =========================================================================

    @Test
    void testSetFillfactor() throws SQLException {
        exec("CREATE TABLE ff_test (id serial PRIMARY KEY, data text)");
        exec("ALTER TABLE ff_test SET (fillfactor = 70)");
    }

    @Test
    void testSetAutovacuumEnabled() throws SQLException {
        exec("CREATE TABLE av_test (id serial PRIMARY KEY)");
        exec("ALTER TABLE av_test SET (autovacuum_enabled = false)");
    }

    @Test
    void testSetToastTupleTarget() throws SQLException {
        exec("CREATE TABLE toast_test (id serial PRIMARY KEY, big_data text)");
        exec("ALTER TABLE toast_test SET (toast_tuple_target = 128)");
    }

    // =========================================================================
    // SELECT ... FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE
    // =========================================================================

    @Test
    void testSelectForUpdate() throws SQLException {
        exec("CREATE TABLE for_upd (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO for_upd (val) VALUES ('test')");
        exec("BEGIN");
        query1("SELECT val FROM for_upd WHERE id = 1 FOR UPDATE");
        exec("COMMIT");
    }

    @Test
    void testSelectForShare() throws SQLException {
        exec("CREATE TABLE for_share (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO for_share (val) VALUES ('test')");
        exec("BEGIN");
        query1("SELECT val FROM for_share WHERE id = 1 FOR SHARE");
        exec("COMMIT");
    }

    @Test
    void testSelectForNoKeyUpdate() throws SQLException {
        exec("CREATE TABLE for_nku (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO for_nku (val) VALUES ('test')");
        exec("BEGIN");
        query1("SELECT val FROM for_nku WHERE id = 1 FOR NO KEY UPDATE");
        exec("COMMIT");
    }

    @Test
    void testSelectForUpdateSkipLocked() throws SQLException {
        exec("CREATE TABLE for_skip (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO for_skip (val) VALUES ('test')");
        exec("BEGIN");
        query1("SELECT val FROM for_skip WHERE id = 1 FOR UPDATE SKIP LOCKED");
        exec("COMMIT");
    }

    @Test
    void testSelectForUpdateNowait() throws SQLException {
        exec("CREATE TABLE for_nowait (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO for_nowait (val) VALUES ('test')");
        exec("BEGIN");
        query1("SELECT val FROM for_nowait WHERE id = 1 FOR UPDATE NOWAIT");
        exec("COMMIT");
    }

    // =========================================================================
    // DISCARD
    // =========================================================================

    @Test
    void testDiscardAll() throws SQLException {
        exec("DISCARD ALL");
    }

    @Test
    void testDiscardTemp() throws SQLException {
        exec("DISCARD TEMP");
    }

    @Test
    void testDiscardPlans() throws SQLException {
        exec("DISCARD PLANS");
    }

    // =========================================================================
    // SET / RESET
    // =========================================================================

    @Test
    void testSetLocalVariable() throws SQLException {
        exec("BEGIN");
        exec("SET LOCAL statement_timeout = '5s'");
        exec("COMMIT");
    }

    @Test
    void testResetVariable() throws SQLException {
        exec("SET work_mem = '64MB'");
        exec("RESET work_mem");
    }

    @Test
    void testResetAll() throws SQLException {
        exec("RESET ALL");
    }

    @Test
    void testSetApplicationName() throws SQLException {
        exec("SET application_name = 'my_test_app'");
        assertEquals("my_test_app", query1("SELECT current_setting('application_name')"));
    }

    // =========================================================================
    // PREPARE / EXECUTE / DEALLOCATE
    // =========================================================================

    @Test
    void testPrepareExecuteDeallocate() throws SQLException {
        exec("CREATE TABLE prep_test (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO prep_test (name) VALUES ('Alice'), ('Bob')");
        exec("PREPARE find_by_name(text) AS SELECT id FROM prep_test WHERE name = $1");
        assertEquals("1", query1("EXECUTE find_by_name('Alice')"));
        exec("DEALLOCATE find_by_name");
    }

    @Test
    void testDeallocateAll() throws SQLException {
        exec("PREPARE stmt1 AS SELECT 1");
        exec("PREPARE stmt2 AS SELECT 2");
        exec("DEALLOCATE ALL");
    }
}
