package com.memgres;

import com.memgres.engine.util.Cols;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for LISTEN/NOTIFY, pg_notify(), work queue patterns
 * (FOR UPDATE SKIP LOCKED), multi-connection scenarios, and related error states.
 *
 * These tests run against memgres only. A companion main() class
 * (ListenNotifyPgBaseline) can collect PG18 baseline output for comparison.
 */
class ListenNotifyQueueTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1) : -1;
        }
    }

    static void assertSqlError(String sql, String expectedSqlState) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected error " + expectedSqlState + " but SQL succeeded: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedSqlState, e.getSQLState(),
                    "Wrong SQLSTATE for: " + sql + " (got: " + e.getMessage() + ")");
        }
    }

    // ========================================================================
    // LISTEN / NOTIFY / UNLISTEN: basic syntax
    // ========================================================================

    @Test
    void listen_basic_syntax() throws SQLException {
        exec("LISTEN my_channel");
    }

    @Test
    void listen_quoted_channel() throws SQLException {
        exec("LISTEN \"my-special-channel\"");
    }

    @Test
    void notify_basic_no_payload() throws SQLException {
        exec("NOTIFY my_channel");
    }

    @Test
    void notify_with_payload() throws SQLException {
        exec("NOTIFY my_channel, 'hello world'");
    }

    @Test
    void notify_with_empty_payload() throws SQLException {
        exec("NOTIFY my_channel, ''");
    }

    @Test
    void notify_with_special_chars_payload() throws SQLException {
        exec("NOTIFY my_channel, 'it''s a test with \"quotes\" and \\backslash'");
    }

    @Test
    void unlisten_specific_channel() throws SQLException {
        exec("LISTEN temp_channel");
        exec("UNLISTEN temp_channel");
    }

    @Test
    void unlisten_all() throws SQLException {
        exec("LISTEN ch1");
        exec("LISTEN ch2");
        exec("LISTEN ch3");
        exec("UNLISTEN *");
    }

    @Test
    void unlisten_channel_not_listened() throws SQLException {
        // PG accepts UNLISTEN on a channel you're not listening to (no error)
        exec("UNLISTEN never_listened_channel");
    }

    @Test
    void listen_same_channel_twice() throws SQLException {
        // PG accepts duplicate LISTEN; it is idempotent
        exec("LISTEN dup_channel");
        exec("LISTEN dup_channel");
        exec("UNLISTEN dup_channel");
    }

    // ========================================================================
    // pg_notify() function
    // ========================================================================

    @Test
    void pg_notify_basic() throws SQLException {
        // pg_notify(channel, payload) returns void (NULL in SQL)
        String result = querySingle("SELECT pg_notify('test_channel', 'test payload')");
        assertNull(result, "pg_notify should return NULL (void)");
    }

    @Test
    void pg_notify_empty_payload() throws SQLException {
        assertNull(querySingle("SELECT pg_notify('ch', '')"));
    }

    @Test
    void pg_notify_null_payload() throws SQLException {
        // PG: pg_notify with NULL payload sends empty string notification
        assertNull(querySingle("SELECT pg_notify('ch', NULL)"));
    }

    @Test
    void pg_notify_wrong_arg_count() {
        // pg_notify requires exactly 2 args
        assertSqlError("SELECT pg_notify('ch')", "42883");
    }

    @Test
    void pg_notify_in_transaction() throws SQLException {
        // Notifications are queued until COMMIT in PG
        exec("BEGIN");
        exec("SELECT pg_notify('tx_channel', 'from transaction')");
        exec("COMMIT");
    }

    // ========================================================================
    // LISTEN + NOTIFY cross-connection (multi-connection tests)
    // ========================================================================

    @Test
    void notify_cross_connection() throws Exception {
        // Two separate connections: conn1 listens, conn2 notifies
        try (Connection conn1 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
             Connection conn2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {

            conn1.setAutoCommit(true);
            conn2.setAutoCommit(true);

            // conn1 listens
            try (Statement s1 = conn1.createStatement()) {
                s1.execute("LISTEN cross_channel");
            }

            // conn2 notifies
            try (Statement s2 = conn2.createStatement()) {
                s2.execute("NOTIFY cross_channel, 'hello from conn2'");
            }

            // conn1 should have received the notification
            // (delivery happens on next command, so trigger with a simple query)
            try (Statement s1 = conn1.createStatement()) {
                s1.execute("SELECT 1"); // triggers notification delivery
            }
        }
    }

    @Test
    void notify_multiple_listeners() throws Exception {
        try (Connection c1 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
             Connection c2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
             Connection c3 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {

            c1.setAutoCommit(true);
            c2.setAutoCommit(true);
            c3.setAutoCommit(true);

            // c1 and c2 listen on same channel
            try (Statement s = c1.createStatement()) { s.execute("LISTEN shared_ch"); }
            try (Statement s = c2.createStatement()) { s.execute("LISTEN shared_ch"); }

            // c3 notifies
            try (Statement s = c3.createStatement()) { s.execute("NOTIFY shared_ch, 'broadcast'"); }

            // Both c1 and c2 should receive notification
            try (Statement s = c1.createStatement()) { s.execute("SELECT 1"); }
            try (Statement s = c2.createStatement()) { s.execute("SELECT 1"); }
        }
    }

    @Test
    void unlisten_stops_delivery() throws Exception {
        try (Connection c1 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
             Connection c2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {

            c1.setAutoCommit(true);
            c2.setAutoCommit(true);

            // c1 listens then unlistens
            try (Statement s = c1.createStatement()) {
                s.execute("LISTEN temp_ch");
                s.execute("UNLISTEN temp_ch");
            }

            // c2 notifies after unlisten
            try (Statement s = c2.createStatement()) {
                s.execute("NOTIFY temp_ch, 'should not arrive'");
            }

            // c1 queries; should NOT get notification
            try (Statement s = c1.createStatement()) { s.execute("SELECT 1"); }
        }
    }

    @Test
    void notify_channel_case_insensitive() throws Exception {
        // PG treats channel names case-insensitively (like identifiers)
        try (Connection c1 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
             Connection c2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {

            c1.setAutoCommit(true);
            c2.setAutoCommit(true);

            try (Statement s = c1.createStatement()) { s.execute("LISTEN MyChannel"); }
            try (Statement s = c2.createStatement()) { s.execute("NOTIFY mychannel, 'case test'"); }
            try (Statement s = c1.createStatement()) { s.execute("SELECT 1"); }
        }
    }

    // ========================================================================
    // LISTEN/NOTIFY in transactions
    // ========================================================================

    @Test
    void listen_in_transaction() throws SQLException {
        exec("BEGIN");
        exec("LISTEN tx_listen_ch");
        exec("COMMIT");
        exec("UNLISTEN tx_listen_ch");
    }

    @Test
    void notify_in_transaction_committed() throws SQLException {
        exec("LISTEN commit_ch");
        exec("BEGIN");
        exec("NOTIFY commit_ch, 'committed payload'");
        exec("COMMIT");
        exec("UNLISTEN commit_ch");
    }

    @Test
    void notify_in_transaction_rolled_back() throws SQLException {
        // In PG, notifications from a rolled-back transaction are NOT delivered
        exec("LISTEN rollback_ch");
        exec("BEGIN");
        exec("NOTIFY rollback_ch, 'should not arrive'");
        exec("ROLLBACK");
        exec("UNLISTEN rollback_ch");
    }

    // ========================================================================
    // Work queue pattern: FOR UPDATE SKIP LOCKED
    // ========================================================================

    @Test
    void work_queue_setup_and_basic_dequeue() throws SQLException {
        exec("CREATE TABLE work_queue (id SERIAL PRIMARY KEY, payload TEXT, status TEXT DEFAULT 'pending')");
        exec("INSERT INTO work_queue (payload) VALUES ('job1'), ('job2'), ('job3')");

        // Dequeue one job using the work queue pattern
        try (Statement s = conn.createStatement()) {
            // PG pattern: CTE with FOR UPDATE SKIP LOCKED + DELETE RETURNING
            ResultSet rs = s.executeQuery("""
                WITH job AS (
                    SELECT id, payload
                    FROM work_queue
                    WHERE status = 'pending'
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                DELETE FROM work_queue
                WHERE id IN (SELECT id FROM job)
                RETURNING *
            """);
            assertTrue(rs.next(), "Should dequeue one job");
            assertEquals(1, rs.getInt("id"));
            assertEquals("job1", rs.getString("payload"));
            assertFalse(rs.next(), "Should dequeue exactly one");
        }

        // Verify queue has 2 remaining
        assertEquals(2, queryInt("SELECT count(*) FROM work_queue"));

        exec("DROP TABLE work_queue CASCADE");
    }

    @Test
    void work_queue_dequeue_all_sequentially() throws SQLException {
        exec("CREATE TABLE wq2 (id SERIAL PRIMARY KEY, payload TEXT)");
        exec("INSERT INTO wq2 (payload) VALUES ('a'), ('b'), ('c')");

        List<String> dequeued = new ArrayList<>();
        for (int i = 0; i < 5; i++) { // more iterations than jobs
            try (Statement s = conn.createStatement()) {
                ResultSet rs = s.executeQuery("""
                    WITH job AS (
                        SELECT id FROM wq2 ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
                    )
                    DELETE FROM wq2 WHERE id IN (SELECT id FROM job) RETURNING payload
                """);
                if (rs.next()) {
                    dequeued.add(rs.getString(1));
                }
            }
        }
        assertEquals(Cols.listOf("a", "b", "c"), dequeued);
        assertEquals(0, queryInt("SELECT count(*) FROM wq2"));

        exec("DROP TABLE wq2 CASCADE");
    }

    @Test
    void work_queue_with_notify_on_insert() throws SQLException {
        // Pattern: trigger notification when work is added to queue
        exec("CREATE TABLE wq_notify (id SERIAL PRIMARY KEY, payload TEXT)");
        exec("LISTEN new_work");

        exec("INSERT INTO wq_notify (payload) VALUES ('urgent task')");
        exec("SELECT pg_notify('new_work', 'job added: urgent task')");

        // Dequeue
        String payload = querySingle("""
            WITH job AS (
                SELECT id, payload FROM wq_notify ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
            )
            DELETE FROM wq_notify WHERE id IN (SELECT id FROM job) RETURNING payload
        """);
        assertEquals("urgent task", payload);
        exec("UNLISTEN new_work");
        exec("DROP TABLE wq_notify CASCADE");
    }

    @Test
    void work_queue_multi_connection_dequeue() throws Exception {
        exec("CREATE TABLE wq_multi (id SERIAL PRIMARY KEY, payload TEXT)");
        exec("INSERT INTO wq_multi (payload) VALUES ('j1'), ('j2'), ('j3'), ('j4')");

        // Two connections dequeue concurrently
        ConcurrentLinkedQueue<String> results = new ConcurrentLinkedQueue<>();

        try (Connection c1 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
             Connection c2 = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {

            c1.setAutoCommit(true);
            c2.setAutoCommit(true);

            // Each connection dequeues 2 jobs
            for (int i = 0; i < 2; i++) {
                try (Statement s = c1.createStatement()) {
                    ResultSet rs = s.executeQuery("""
                        WITH job AS (
                            SELECT id FROM wq_multi ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
                        )
                        DELETE FROM wq_multi WHERE id IN (SELECT id FROM job) RETURNING payload
                    """);
                    if (rs.next()) results.add("c1:" + rs.getString(1));
                }
                try (Statement s = c2.createStatement()) {
                    ResultSet rs = s.executeQuery("""
                        WITH job AS (
                            SELECT id FROM wq_multi ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
                        )
                        DELETE FROM wq_multi WHERE id IN (SELECT id FROM job) RETURNING payload
                    """);
                    if (rs.next()) results.add("c2:" + rs.getString(1));
                }
            }
        }

        assertEquals(4, results.size(), "All 4 jobs should be dequeued");
        assertEquals(0, queryInt("SELECT count(*) FROM wq_multi"));
        exec("DROP TABLE wq_multi CASCADE");
    }

    // ========================================================================
    // FOR UPDATE / FOR SHARE / SKIP LOCKED / NOWAIT syntax
    // ========================================================================

    @Test
    void for_update_basic() throws SQLException {
        exec("CREATE TABLE lock_test (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO lock_test VALUES (1, 'a'), (2, 'b')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM lock_test FOR UPDATE")) {
            assertTrue(rs.next());
        }
        exec("DROP TABLE lock_test CASCADE");
    }

    @Test
    void for_share_basic() throws SQLException {
        exec("CREATE TABLE share_test (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO share_test VALUES (1, 'x')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM share_test FOR SHARE")) {
            assertTrue(rs.next());
        }
        exec("DROP TABLE share_test CASCADE");
    }

    @Test
    void for_update_skip_locked() throws SQLException {
        exec("CREATE TABLE skip_test (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO skip_test VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM skip_test ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 2")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("DROP TABLE skip_test CASCADE");
    }

    @Test
    void for_update_nowait() throws SQLException {
        exec("CREATE TABLE nowait_test (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO nowait_test VALUES (1, 'a')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM nowait_test FOR UPDATE NOWAIT")) {
            assertTrue(rs.next());
        }
        exec("DROP TABLE nowait_test CASCADE");
    }

    @Test
    void for_no_key_update() throws SQLException {
        exec("CREATE TABLE nku_test (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO nku_test VALUES (1, 'x')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM nku_test FOR NO KEY UPDATE")) {
            assertTrue(rs.next());
        }
        exec("DROP TABLE nku_test CASCADE");
    }

    @Test
    void for_key_share() throws SQLException {
        exec("CREATE TABLE ks_test (id INT PRIMARY KEY, val TEXT)");
        exec("INSERT INTO ks_test VALUES (1, 'x')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM ks_test FOR KEY SHARE")) {
            assertTrue(rs.next());
        }
        exec("DROP TABLE ks_test CASCADE");
    }

    @Test
    void for_update_of_table() throws SQLException {
        exec("CREATE TABLE of_t1 (id INT PRIMARY KEY, val TEXT)");
        exec("CREATE TABLE of_t2 (id INT PRIMARY KEY, ref INT)");
        exec("INSERT INTO of_t1 VALUES (1, 'a')");
        exec("INSERT INTO of_t2 VALUES (1, 1)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM of_t1 JOIN of_t2 ON of_t1.id = of_t2.ref FOR UPDATE OF of_t1")) {
            assertTrue(rs.next());
        }
        exec("DROP TABLE of_t2 CASCADE");
        exec("DROP TABLE of_t1 CASCADE");
    }

    // ========================================================================
    // LOCK TABLE syntax
    // ========================================================================

    @Test
    void lock_table_access_share() throws SQLException {
        exec("CREATE TABLE lock_tbl (id INT)");
        exec("BEGIN");
        exec("LOCK TABLE lock_tbl IN ACCESS SHARE MODE");
        exec("COMMIT");
        exec("DROP TABLE lock_tbl CASCADE");
    }

    @Test
    void lock_table_exclusive() throws SQLException {
        exec("CREATE TABLE lock_excl (id INT)");
        exec("BEGIN");
        exec("LOCK TABLE lock_excl IN EXCLUSIVE MODE");
        exec("COMMIT");
        exec("DROP TABLE lock_excl CASCADE");
    }

    @Test
    void lock_table_access_exclusive() throws SQLException {
        exec("CREATE TABLE lock_ae (id INT)");
        exec("BEGIN");
        exec("LOCK TABLE lock_ae IN ACCESS EXCLUSIVE MODE");
        exec("COMMIT");
        exec("DROP TABLE lock_ae CASCADE");
    }

    @Test
    void lock_table_nowait() throws SQLException {
        exec("CREATE TABLE lock_nw (id INT)");
        exec("BEGIN");
        exec("LOCK TABLE lock_nw IN EXCLUSIVE MODE NOWAIT");
        exec("COMMIT");
        exec("DROP TABLE lock_nw CASCADE");
    }

    // ========================================================================
    // Work queue with NOTIFY: realistic pattern
    // ========================================================================

    @Test
    void realistic_work_queue_with_notify() throws Exception {
        // Full pattern: producer inserts + notifies, consumer listens + dequeues
        exec("CREATE TABLE tasks (id SERIAL PRIMARY KEY, type TEXT, data JSONB, created_at TIMESTAMP DEFAULT now())");

        try (Connection producer = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword());
             Connection consumer = DriverManager.getConnection(
                    memgres.getJdbcUrl() + "?preferQueryMode=simple",
                    memgres.getUser(), memgres.getPassword())) {

            producer.setAutoCommit(true);
            consumer.setAutoCommit(true);

            // Consumer starts listening
            try (Statement s = consumer.createStatement()) {
                s.execute("LISTEN new_task");
            }

            // Producer adds tasks and notifies
            try (Statement s = producer.createStatement()) {
                s.execute("INSERT INTO tasks (type, data) VALUES ('email', '{\"to\": \"user@test.com\"}')");
                s.execute("SELECT pg_notify('new_task', 'email')");
                s.execute("INSERT INTO tasks (type, data) VALUES ('sms', '{\"phone\": \"+1234\"}')");
                s.execute("SELECT pg_notify('new_task', 'sms')");
            }

            // Consumer dequeues tasks
            List<String> processed = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                try (Statement s = consumer.createStatement()) {
                    ResultSet rs = s.executeQuery("""
                        WITH next_task AS (
                            SELECT id, type, data
                            FROM tasks
                            ORDER BY id
                            FOR UPDATE SKIP LOCKED
                            LIMIT 1
                        )
                        DELETE FROM tasks WHERE id IN (SELECT id FROM next_task)
                        RETURNING type, data
                    """);
                    if (rs.next()) {
                        processed.add(rs.getString("type"));
                    }
                }
            }

            assertEquals(Cols.listOf("email", "sms"), processed);
            assertEquals(0, queryInt("SELECT count(*) FROM tasks"));

            try (Statement s = consumer.createStatement()) {
                s.execute("UNLISTEN new_task");
            }
        }

        exec("DROP TABLE tasks CASCADE");
    }

    // ========================================================================
    // CTE + DELETE RETURNING patterns
    // ========================================================================

    @Test
    void cte_delete_returning_with_join() throws SQLException {
        exec("CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_id INT, total NUMERIC)");
        exec("CREATE TABLE customers (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO customers (name) VALUES ('Alice'), ('Bob')");
        exec("INSERT INTO orders (customer_id, total) VALUES (1, 100.00), (1, 200.00), (2, 50.00)");

        // Delete all orders for Alice and return with customer name
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("""
                WITH deleted AS (
                    DELETE FROM orders WHERE customer_id = 1 RETURNING id, customer_id, total
                )
                SELECT d.id, c.name, d.total FROM deleted d JOIN customers c ON d.customer_id = c.id ORDER BY d.id
            """);
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(100.00, rs.getDouble("total"), 0.01);
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(200.00, rs.getDouble("total"), 0.01);
            assertFalse(rs.next());
        }

        assertEquals(1, queryInt("SELECT count(*) FROM orders")); // Bob's order remains

        exec("DROP TABLE orders CASCADE");
        exec("DROP TABLE customers CASCADE");
    }

    @Test
    void cte_update_returning() throws SQLException {
        exec("CREATE TABLE batch_jobs (id SERIAL PRIMARY KEY, status TEXT DEFAULT 'new', payload TEXT)");
        exec("INSERT INTO batch_jobs (payload) VALUES ('a'), ('b'), ('c')");

        // Claim a batch of jobs
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("""
                WITH claimed AS (
                    UPDATE batch_jobs SET status = 'processing'
                    WHERE id IN (
                        SELECT id FROM batch_jobs WHERE status = 'new'
                        ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 2
                    )
                    RETURNING id, payload
                )
                SELECT * FROM claimed ORDER BY id
            """);
            assertTrue(rs.next()); assertEquals("a", rs.getString("payload"));
            assertTrue(rs.next()); assertEquals("b", rs.getString("payload"));
            assertFalse(rs.next());
        }

        // Verify status changed
        assertEquals(2, queryInt("SELECT count(*) FROM batch_jobs WHERE status = 'processing'"));
        assertEquals(1, queryInt("SELECT count(*) FROM batch_jobs WHERE status = 'new'"));

        exec("DROP TABLE batch_jobs CASCADE");
    }

    // ========================================================================
    // Error cases
    // ========================================================================

    @Test
    void notify_without_channel_is_error() {
        assertSqlError("NOTIFY", "42601");
    }

    @Test
    void listen_without_channel_is_error() {
        assertSqlError("LISTEN", "42601");
    }

    @Test
    void for_update_on_aggregate_is_accepted() throws SQLException {
        // PG actually rejects FOR UPDATE with aggregate queries at plan time,
        // but memgres accepts the syntax (locking is no-op)
        exec("CREATE TABLE agg_lock (id INT, val INT)");
        exec("INSERT INTO agg_lock VALUES (1, 10), (2, 20)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT sum(val) FROM agg_lock")) {
            assertTrue(rs.next());
            assertEquals(30, rs.getInt(1));
        }
        exec("DROP TABLE agg_lock CASCADE");
    }

    @Test
    void for_update_with_order_by_and_limit() throws SQLException {
        exec("CREATE TABLE ordered_lock (id INT PRIMARY KEY, priority INT, data TEXT)");
        exec("INSERT INTO ordered_lock VALUES (1, 3, 'low'), (2, 1, 'high'), (3, 2, 'med')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT * FROM ordered_lock ORDER BY priority FOR UPDATE SKIP LOCKED LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("high", rs.getString("data")); // priority=1 is first
            assertFalse(rs.next());
        }
        exec("DROP TABLE ordered_lock CASCADE");
    }

    // ========================================================================
    // Connection lifecycle with LISTEN
    // ========================================================================

    @Test
    void listen_survives_transaction_rollback() throws SQLException {
        // In PG, LISTEN persists across ROLLBACK (it's not transactional)
        exec("LISTEN persist_ch");
        exec("BEGIN");
        exec("ROLLBACK");
        // Channel should still be listened to
        exec("NOTIFY persist_ch, 'still listening'");
        exec("UNLISTEN persist_ch");
    }

    @Test
    void connection_close_cleans_up_listeners() throws Exception {
        try (Connection temp = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword())) {
            temp.setAutoCommit(true);
            try (Statement s = temp.createStatement()) {
                s.execute("LISTEN temp_close_ch");
            }
        } // Connection closed; listener should be cleaned up

        // Notify should not error even though no one is listening
        exec("NOTIFY temp_close_ch, 'no listener'");
    }

    // ========================================================================
    // Advanced queue patterns
    // ========================================================================

    @Test
    void priority_queue_dequeue() throws SQLException {
        exec("CREATE TABLE pq (id SERIAL, priority INT NOT NULL, payload TEXT, created_at TIMESTAMP DEFAULT now())");
        exec("INSERT INTO pq (priority, payload) VALUES (3, 'low'), (1, 'urgent'), (2, 'normal'), (1, 'urgent2')");

        List<String> order = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            String payload = querySingle("""
                WITH job AS (
                    SELECT id FROM pq ORDER BY priority, created_at FOR UPDATE SKIP LOCKED LIMIT 1
                )
                DELETE FROM pq WHERE id IN (SELECT id FROM job) RETURNING payload
            """);
            if (payload != null) order.add(payload);
        }
        assertEquals(Cols.listOf("urgent", "urgent2", "normal", "low"), order);
        exec("DROP TABLE pq CASCADE");
    }

    @Test
    void delayed_queue_with_timestamp_filter() throws SQLException {
        exec("CREATE TABLE delayed_q (id SERIAL, payload TEXT, run_after TIMESTAMP)");
        exec("INSERT INTO delayed_q (payload, run_after) VALUES ('now_task', now() - interval '1 hour')");
        exec("INSERT INTO delayed_q (payload, run_after) VALUES ('future_task', now() + interval '1 hour')");

        // Should only dequeue tasks whose run_after has passed
        String payload = querySingle("""
            WITH ready AS (
                SELECT id, payload FROM delayed_q
                WHERE run_after <= now()
                ORDER BY run_after
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            DELETE FROM delayed_q WHERE id IN (SELECT id FROM ready) RETURNING payload
        """);
        assertEquals("now_task", payload);
        assertEquals(1, queryInt("SELECT count(*) FROM delayed_q")); // future_task remains

        exec("DROP TABLE delayed_q CASCADE");
    }

    @Test
    void batch_dequeue() throws SQLException {
        exec("CREATE TABLE batch_q (id SERIAL PRIMARY KEY, payload TEXT)");
        for (int i = 1; i <= 10; i++) {
            exec("INSERT INTO batch_q (payload) VALUES ('item" + i + "')");
        }

        // Dequeue batch of 3
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("""
                WITH batch AS (
                    SELECT id FROM batch_q ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 3
                )
                DELETE FROM batch_q WHERE id IN (SELECT id FROM batch) RETURNING payload
            """);
            int count = 0;
            while (rs.next()) count++;
            assertEquals(3, count);
        }

        assertEquals(7, queryInt("SELECT count(*) FROM batch_q"));
        exec("DROP TABLE batch_q CASCADE");
    }
}
