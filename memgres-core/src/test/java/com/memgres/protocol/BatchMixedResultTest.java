package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for multi-statement batches that mix DDL/DML (no result set)
 * with SELECT statements (produce result sets).
 *
 * In the PG simple query protocol, a single execute() call can contain
 * multiple semicolon-separated statements. Each statement is processed
 * sequentially. For each result-producing statement (SELECT, RETURNING),
 * the server must send RowDescription before DataRow messages. For
 * non-result statements (CREATE TABLE, INSERT without RETURNING), the
 * server sends only CommandComplete.
 *
 * pg_dump output commonly interleaves:
 *   CREATE TABLE ...;
 *   CREATE SEQUENCE ...;
 *   ALTER TABLE ... SET DEFAULT nextval(...);
 *   INSERT INTO ... VALUES (...);
 *   SELECT pg_catalog.setval('seq_name', 42, true);  -- returns a result!
 *   ALTER TABLE ... ADD CONSTRAINT ...;
 *
 * If the PgWire handler doesn't properly send RowDescription for the
 * SELECT setval() call, the JDBC driver throws:
 *   "Received resultset tuples, but no field structure for them"
 */
class BatchMixedResultTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test?preferQueryMode=simple",
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
    // Basic: DDL + SELECT in one batch
    // =========================================================================

    @Test
    void testBatchCreateTableThenSelect() throws SQLException {
        exec("""
            CREATE TABLE batch_t1 (id serial PRIMARY KEY, name text);
            SELECT 1;
        """);
    }

    @Test
    void testBatchCreateTableThenSelectSetval() throws SQLException {
        exec("""
            CREATE SEQUENCE batch_seq1 START WITH 1;
            CREATE TABLE batch_t2 (id bigint NOT NULL, name text);
            SELECT setval('batch_seq1', 10, true);
        """);
        assertEquals("11", query1("SELECT nextval('batch_seq1')"));
    }

    // =========================================================================
    // pg_dump pattern: CREATE TABLE + SEQUENCE + SET DEFAULT + INSERT + setval
    // =========================================================================

    @Test
    void testPgDumpStyleBatch() throws SQLException {
        exec("""
            CREATE TABLE batch_orders (
                id bigint NOT NULL,
                customer text,
                total numeric
            );
            CREATE SEQUENCE batch_orders_id_seq
                START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
            ALTER TABLE batch_orders ALTER COLUMN id
                SET DEFAULT nextval('batch_orders_id_seq');
            INSERT INTO batch_orders (id, customer, total) VALUES (1, 'Alice', 100.00);
            INSERT INTO batch_orders (id, customer, total) VALUES (2, 'Bob', 200.00);
            SELECT pg_catalog.setval('batch_orders_id_seq', 2, true);
            ALTER TABLE ONLY batch_orders ADD CONSTRAINT batch_orders_pkey PRIMARY KEY (id);
        """);
        assertEquals("2", query1("SELECT COUNT(*) FROM batch_orders"));
        assertEquals("3", query1("SELECT nextval('batch_orders_id_seq')"));
    }

    // =========================================================================
    // Multiple SELECT setval() calls interleaved with DDL
    // =========================================================================

    @Test
    void testMultipleSetvalInBatch() throws SQLException {
        exec("""
            CREATE SEQUENCE sv_seq_a START WITH 1;
            CREATE SEQUENCE sv_seq_b START WITH 1;
            CREATE SEQUENCE sv_seq_c START WITH 1;
            CREATE TABLE sv_table_a (id bigint NOT NULL, val text);
            CREATE TABLE sv_table_b (id bigint NOT NULL, val text);
            ALTER TABLE sv_table_a ALTER COLUMN id SET DEFAULT nextval('sv_seq_a');
            ALTER TABLE sv_table_b ALTER COLUMN id SET DEFAULT nextval('sv_seq_b');
            INSERT INTO sv_table_a (id, val) VALUES (1, 'first');
            INSERT INTO sv_table_b (id, val) VALUES (1, 'first');
            SELECT pg_catalog.setval('sv_seq_a', 1, true);
            SELECT pg_catalog.setval('sv_seq_b', 1, true);
            SELECT pg_catalog.setval('sv_seq_c', 100, false);
            ALTER TABLE ONLY sv_table_a ADD CONSTRAINT sv_table_a_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY sv_table_b ADD CONSTRAINT sv_table_b_pkey PRIMARY KEY (id);
        """);
    }

    // =========================================================================
    // SELECT pg_catalog.set_config in batch
    // =========================================================================

    @Test
    void testSetConfigInBatch() throws SQLException {
        exec("""
            SELECT pg_catalog.set_config('search_path', 'public', false);
            CREATE TABLE batch_after_config (id serial PRIMARY KEY, val text);
            INSERT INTO batch_after_config (val) VALUES ('ok');
        """);
    }

    // =========================================================================
    // SELECT in middle of DML batch
    // =========================================================================

    @Test
    void testSelectInMiddleOfInserts() throws SQLException {
        exec("CREATE TABLE batch_items (id serial PRIMARY KEY, name text)");
        exec("""
            INSERT INTO batch_items (name) VALUES ('first');
            INSERT INTO batch_items (name) VALUES ('second');
            SELECT COUNT(*) FROM batch_items;
            INSERT INTO batch_items (name) VALUES ('third');
        """);
        assertEquals("3", query1("SELECT COUNT(*) FROM batch_items"));
    }

    // =========================================================================
    // INSERT ... RETURNING in a batch
    // =========================================================================

    @Test
    void testInsertReturningInBatch() throws SQLException {
        exec("""
            CREATE TABLE batch_ret (id serial PRIMARY KEY, name text);
            INSERT INTO batch_ret (name) VALUES ('row1') RETURNING id;
            INSERT INTO batch_ret (name) VALUES ('row2');
        """);
        assertEquals("2", query1("SELECT COUNT(*) FROM batch_ret"));
    }

    // =========================================================================
    // Full pg_dump lifecycle in one batch (many tables + sequences + setvals)
    // =========================================================================

    @Test
    void testFullPgDumpBatch() throws SQLException {
        exec("""
            CREATE TABLE dump_accounts (id bigint NOT NULL, name text NOT NULL, active boolean DEFAULT true);
            CREATE TABLE dump_entries (id bigint NOT NULL, account_id bigint, payload jsonb DEFAULT '{}');
            CREATE TABLE dump_log (id bigint NOT NULL, created_at timestamptz DEFAULT now(), message text);
            CREATE SEQUENCE dump_accounts_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
            CREATE SEQUENCE dump_entries_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
            CREATE SEQUENCE dump_log_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
            ALTER TABLE dump_accounts ALTER COLUMN id SET DEFAULT nextval('dump_accounts_id_seq');
            ALTER TABLE dump_entries ALTER COLUMN id SET DEFAULT nextval('dump_entries_id_seq');
            ALTER TABLE dump_log ALTER COLUMN id SET DEFAULT nextval('dump_log_id_seq');
            INSERT INTO dump_accounts (id, name) VALUES (1, 'Acme Corp');
            INSERT INTO dump_accounts (id, name) VALUES (2, 'Widgets Inc');
            INSERT INTO dump_entries (id, account_id, payload) VALUES (1, 1, '{"type": "order"}');
            INSERT INTO dump_entries (id, account_id, payload) VALUES (2, 1, '{"type": "payment"}');
            INSERT INTO dump_log (id, message) VALUES (1, 'System started');
            SELECT pg_catalog.setval('dump_accounts_id_seq', 2, true);
            SELECT pg_catalog.setval('dump_entries_id_seq', 2, true);
            SELECT pg_catalog.setval('dump_log_id_seq', 1, true);
            ALTER TABLE ONLY dump_accounts ADD CONSTRAINT dump_accounts_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY dump_entries ADD CONSTRAINT dump_entries_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY dump_log ADD CONSTRAINT dump_log_pkey PRIMARY KEY (id);
            ALTER TABLE ONLY dump_entries ADD CONSTRAINT dump_entries_account_fk FOREIGN KEY (account_id) REFERENCES dump_accounts(id);
            CREATE INDEX dump_log_created_idx ON dump_log (created_at);
        """);
        assertEquals("2", query1("SELECT COUNT(*) FROM dump_accounts"));
        assertEquals("2", query1("SELECT COUNT(*) FROM dump_entries"));
        assertEquals("3", query1("SELECT nextval('dump_accounts_id_seq')"));
    }

    // =========================================================================
    // Batch with DO block (no result) + SELECT (result)
    // =========================================================================

    @Test
    void testBatchDoBlockThenSelect() throws SQLException {
        exec("""
            DO $$ BEGIN RAISE NOTICE 'batch test'; END; $$;
            SELECT 1 AS result;
        """);
    }

    // =========================================================================
    // Empty batch / single-statement batch (baseline)
    // =========================================================================

    @Test
    void testSingleStatementBatch() throws SQLException {
        exec("SELECT 1");
    }

    @Test
    void testSingleDdlBatch() throws SQLException {
        exec("CREATE TABLE single_batch (id serial PRIMARY KEY)");
    }
}
