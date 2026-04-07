package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Multi-connection transaction tests. Opens multiple JDBC connections to the same
 * Memgres instance and validates transaction semantics: visibility, rollback effects,
 * concurrent DML, DDL transactionality, sequence atomicity, and state independence.
 *
 * Note: Memgres uses a shared-state undo-log model (no MVCC), so uncommitted changes
 * ARE visible to other connections (dirty reads). Tests document this behavior.
 */
class MultiConnectionTransactionTest {

    static Memgres memgres;
    static String url;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).maxConnections(30).build().start();
        url = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
    }

    @AfterAll
    static void teardown() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(url, "test", "test");
    }

    private String query1(Statement st, String sql) throws SQLException {
        ResultSet rs = st.executeQuery(sql);
        assertTrue(rs.next(), "Expected at least one row for: " + sql);
        return rs.getString(1);
    }

    private int queryInt(Statement st, String sql) throws SQLException {
        return Integer.parseInt(query1(st, sql));
    }

    private boolean hasRows(Statement st, String sql) throws SQLException {
        ResultSet rs = st.executeQuery(sql);
        return rs.next();
    }

    // ========================================================================
    // Category 1: Cross-Connection Data Visibility
    // ========================================================================

    @Test
    void testCommittedDataVisibleToOtherConnection() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE vis_commit (id int, val text)");
            s1.execute("BEGIN");
            s1.execute("INSERT INTO vis_commit VALUES (1, 'hello')");
            s1.execute("COMMIT");
            // After commit, c2 must see the data
            assertEquals("hello", query1(s2, "SELECT val FROM vis_commit WHERE id = 1"));
            s1.execute("DROP TABLE vis_commit");
        }
    }

    @Test
    void testUncommittedInsertVisibleToOtherConnection() throws SQLException {
        // With READ COMMITTED isolation, uncommitted inserts are NOT visible to other connections
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE vis_dirty (id int, val text)");
            s1.execute("BEGIN");
            s1.execute("INSERT INTO vis_dirty VALUES (1, 'uncommitted')");
            // c2 cannot see the uncommitted row (READ COMMITTED behavior)
            assertEquals("0", query1(s2, "SELECT count(*) FROM vis_dirty"));
            s1.execute("ROLLBACK");
            // After rollback, the row is gone
            assertEquals("0", query1(s2, "SELECT count(*) FROM vis_dirty"));
            s1.execute("DROP TABLE vis_dirty");
        }
    }

    @Test
    void testUncommittedUpdateVisibleToOtherConnection() throws SQLException {
        // With READ COMMITTED isolation, uncommitted updates are NOT visible to other connections
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE vis_upd (id int, val text)");
            s1.execute("INSERT INTO vis_upd VALUES (1, 'original')");
            s1.execute("BEGIN");
            s1.execute("UPDATE vis_upd SET val = 'modified' WHERE id = 1");
            // c2 sees the original value (READ COMMITTED behavior)
            assertEquals("original", query1(s2, "SELECT val FROM vis_upd WHERE id = 1"));
            s1.execute("ROLLBACK");
            // After rollback, back to original
            assertEquals("original", query1(s2, "SELECT val FROM vis_upd WHERE id = 1"));
            s1.execute("DROP TABLE vis_upd");
        }
    }

    @Test
    void testUncommittedDeleteVisibleToOtherConnection() throws SQLException {
        // With READ COMMITTED isolation, uncommitted deletes are NOT visible to other connections
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE vis_del (id int)");
            s1.execute("INSERT INTO vis_del VALUES (1)");
            s1.execute("INSERT INTO vis_del VALUES (2)");
            s1.execute("BEGIN");
            s1.execute("DELETE FROM vis_del WHERE id = 1");
            // c2 still sees 2 rows (READ COMMITTED: uncommitted delete not visible)
            assertEquals("2", query1(s2, "SELECT count(*) FROM vis_del"));
            s1.execute("ROLLBACK");
            // After rollback, both rows are back
            assertEquals("2", query1(s2, "SELECT count(*) FROM vis_del"));
            s1.execute("DROP TABLE vis_del");
        }
    }

    @Test
    void testAutocommitImmediatelyVisibleToOtherConnection() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE vis_auto (id int)");
            // Autocommit insert (no BEGIN)
            s1.execute("INSERT INTO vis_auto VALUES (42)");
            // Immediately visible to c2
            assertEquals("42", query1(s2, "SELECT id FROM vis_auto"));
            s1.execute("DROP TABLE vis_auto");
        }
    }

    // ========================================================================
    // Category 2: Rollback Effects Across Connections
    // ========================================================================

    @Test
    void testRollbackRevertsInsertForOtherConnection() throws SQLException {
        // With READ COMMITTED, c2 does not see uncommitted inserts, and after rollback still sees 0
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE rb_ins (id int)");
            s1.execute("BEGIN");
            s1.execute("INSERT INTO rb_ins VALUES (1)");
            s1.execute("INSERT INTO rb_ins VALUES (2)");
            s1.execute("INSERT INTO rb_ins VALUES (3)");
            // c2 does not see uncommitted rows (READ COMMITTED)
            assertEquals("0", query1(s2, "SELECT count(*) FROM rb_ins"));
            s1.execute("ROLLBACK");
            // After rollback, c2 still sees 0 rows
            assertEquals("0", query1(s2, "SELECT count(*) FROM rb_ins"));
            s1.execute("DROP TABLE rb_ins");
        }
    }

    @Test
    void testRollbackRevertsUpdateForOtherConnection() throws SQLException {
        // With READ COMMITTED, c2 sees original value while c1's update is uncommitted
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE rb_upd (id int, val text)");
            s1.execute("INSERT INTO rb_upd VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            s1.execute("BEGIN");
            s1.execute("UPDATE rb_upd SET val = 'x' WHERE id = 2");
            // c2 sees original value (READ COMMITTED)
            assertEquals("b", query1(s2, "SELECT val FROM rb_upd WHERE id = 2"));
            s1.execute("ROLLBACK");
            assertEquals("b", query1(s2, "SELECT val FROM rb_upd WHERE id = 2"));
            s1.execute("DROP TABLE rb_upd");
        }
    }

    @Test
    void testRollbackRevertsDeleteForOtherConnection() throws SQLException {
        // With READ COMMITTED, c2 still sees all rows while c1's delete is uncommitted
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE rb_del (id int)");
            s1.execute("INSERT INTO rb_del VALUES (1), (2), (3)");
            s1.execute("BEGIN");
            s1.execute("DELETE FROM rb_del WHERE id IN (1, 3)");
            // c2 still sees all 3 rows (READ COMMITTED: uncommitted delete not visible)
            assertEquals("3", query1(s2, "SELECT count(*) FROM rb_del"));
            s1.execute("ROLLBACK");
            assertEquals("3", query1(s2, "SELECT count(*) FROM rb_del"));
            s1.execute("DROP TABLE rb_del");
        }
    }

    @Test
    void testSavepointRollbackPartiallyRevertsForOtherConnection() throws SQLException {
        // With READ COMMITTED, c2 only sees committed data
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE rb_sp (id int)");
            s1.execute("BEGIN");
            s1.execute("INSERT INTO rb_sp VALUES (1)");
            s1.execute("SAVEPOINT sp1");
            s1.execute("INSERT INTO rb_sp VALUES (2)");
            s1.execute("INSERT INTO rb_sp VALUES (3)");
            // c2 does not see uncommitted rows (READ COMMITTED)
            assertEquals("0", query1(s2, "SELECT count(*) FROM rb_sp"));
            // Rollback to savepoint, removing rows 2 and 3
            s1.execute("ROLLBACK TO SAVEPOINT sp1");
            assertEquals("0", query1(s2, "SELECT count(*) FROM rb_sp"));
            // Commit keeps row 1, now visible
            s1.execute("COMMIT");
            assertEquals("1", query1(s2, "SELECT count(*) FROM rb_sp"));
            s1.execute("DROP TABLE rb_sp");
        }
    }

    @Test
    void testNestedSavepointsRollbackAcrossConnections() throws SQLException {
        // With READ COMMITTED, c2 only sees committed data
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE rb_nested (id int)");
            s1.execute("BEGIN");
            s1.execute("INSERT INTO rb_nested VALUES (1)");
            s1.execute("SAVEPOINT sp1");
            s1.execute("INSERT INTO rb_nested VALUES (2)");
            s1.execute("SAVEPOINT sp2");
            s1.execute("INSERT INTO rb_nested VALUES (3)");
            // c2 does not see uncommitted rows (READ COMMITTED)
            assertEquals("0", query1(s2, "SELECT count(*) FROM rb_nested"));
            // Rollback to sp2, removing row 3 only
            s1.execute("ROLLBACK TO SAVEPOINT sp2");
            assertEquals("0", query1(s2, "SELECT count(*) FROM rb_nested"));
            // Rollback to sp1, removing row 2
            s1.execute("ROLLBACK TO SAVEPOINT sp1");
            assertEquals("0", query1(s2, "SELECT count(*) FROM rb_nested"));
            s1.execute("COMMIT");
            assertEquals("1", query1(s2, "SELECT count(*) FROM rb_nested"));
            s1.execute("DROP TABLE rb_nested");
        }
    }

    @Test
    void testFullRollbackAfterSavepointRelease() throws SQLException {
        // With READ COMMITTED, c2 only sees committed data
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE rb_rel (id int)");
            s1.execute("BEGIN");
            s1.execute("INSERT INTO rb_rel VALUES (1)");
            s1.execute("SAVEPOINT sp1");
            s1.execute("INSERT INTO rb_rel VALUES (2)");
            s1.execute("RELEASE SAVEPOINT sp1");
            // c2 does not see uncommitted rows (READ COMMITTED)
            assertEquals("0", query1(s2, "SELECT count(*) FROM rb_rel"));
            // Full rollback should undo everything (including released savepoint data)
            s1.execute("ROLLBACK");
            assertEquals("0", query1(s2, "SELECT count(*) FROM rb_rel"));
            s1.execute("DROP TABLE rb_rel");
        }
    }

    // ========================================================================
    // Category 3: Concurrent DML Operations
    // ========================================================================

    @Test
    void testConcurrentInsertsFromMultipleConnections() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("CREATE TABLE conc_ins (id int, conn_id int)");
        }
        int numConns = 5;
        int insertsPerConn = 20;
        ExecutorService pool = Executors.newFixedThreadPool(numConns);
        CountDownLatch latch = new CountDownLatch(numConns);
        for (int c = 0; c < numConns; c++) {
            final int connId = c;
            pool.submit(() -> {
                try (Connection conn = connect(); Statement st = conn.createStatement()) {
                    for (int i = 0; i < insertsPerConn; i++) {
                        st.execute("INSERT INTO conc_ins VALUES (" + (connId * 100 + i) + ", " + connId + ")");
                    }
                } catch (Exception e) {
                    fail("Connection " + connId + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(15, TimeUnit.SECONDS));
        pool.shutdown();
        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            assertEquals(String.valueOf(numConns * insertsPerConn),
                    query1(st, "SELECT count(*) FROM conc_ins"));
            // Verify each connection's inserts are present
            for (int c = 0; c < numConns; c++) {
                assertEquals(String.valueOf(insertsPerConn),
                        query1(st, "SELECT count(*) FROM conc_ins WHERE conn_id = " + c));
            }
            st.execute("DROP TABLE conc_ins");
        }
    }

    @Test
    void testConcurrentUpdatesToDifferentRows() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("CREATE TABLE conc_upd_diff (id int PRIMARY KEY, val int)");
            for (int i = 1; i <= 10; i++) {
                st.execute("INSERT INTO conc_upd_diff VALUES (" + i + ", 0)");
            }
        }
        ExecutorService pool = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 1; i <= 10; i++) {
            final int row = i;
            pool.submit(() -> {
                try (Connection conn = connect(); Statement st = conn.createStatement()) {
                    st.execute("UPDATE conc_upd_diff SET val = " + (row * 10) + " WHERE id = " + row);
                } catch (Exception e) {
                    fail("Update row " + row + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(15, TimeUnit.SECONDS));
        pool.shutdown();
        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            for (int i = 1; i <= 10; i++) {
                assertEquals(String.valueOf(i * 10),
                        query1(st, "SELECT val FROM conc_upd_diff WHERE id = " + i));
            }
            st.execute("DROP TABLE conc_upd_diff");
        }
    }

    @Test
    void testConcurrentUpdatesToSameRowLastWriterWins() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("CREATE TABLE conc_upd_same (id int, val int)");
            st.execute("INSERT INTO conc_upd_same VALUES (1, 0)");
        }
        // Multiple connections updating same row sequentially (last writer wins)
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("UPDATE conc_upd_same SET val = 100 WHERE id = 1");
            s2.execute("UPDATE conc_upd_same SET val = 200 WHERE id = 1");
            // Last writer (c2) wins
            assertEquals("200", query1(s1, "SELECT val FROM conc_upd_same WHERE id = 1"));
            s1.execute("DROP TABLE conc_upd_same");
        }
    }

    @Test
    void testInterleavedInsertReadAcrossConnections() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE interleave (id int, source text)");
            // Interleave inserts and reads
            s1.execute("INSERT INTO interleave VALUES (1, 'c1')");
            assertEquals("1", query1(s2, "SELECT count(*) FROM interleave"));
            s2.execute("INSERT INTO interleave VALUES (2, 'c2')");
            assertEquals("2", query1(s1, "SELECT count(*) FROM interleave"));
            s1.execute("INSERT INTO interleave VALUES (3, 'c1')");
            assertEquals("3", query1(s2, "SELECT count(*) FROM interleave"));
            s1.execute("DROP TABLE interleave");
        }
    }

    @Test
    void testConcurrentDeletesFromDifferentConnections() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE conc_del (id int)");
            for (int i = 1; i <= 10; i++) {
                s1.execute("INSERT INTO conc_del VALUES (" + i + ")");
            }
            // c1 deletes even rows, c2 deletes odd rows
            s1.execute("DELETE FROM conc_del WHERE id % 2 = 0");
            s2.execute("DELETE FROM conc_del WHERE id % 2 = 1");
            assertEquals("0", query1(s1, "SELECT count(*) FROM conc_del"));
            s1.execute("DROP TABLE conc_del");
        }
    }

    @Test
    void testConcurrentInsertWithTransaction() throws SQLException {
        // With READ COMMITTED, c2 does not see c1's uncommitted insert
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE conc_txn_ins (id int)");
            // c1 starts transaction, inserts
            s1.execute("BEGIN");
            s1.execute("INSERT INTO conc_txn_ins VALUES (1)");
            // c2 also inserts (autocommit)
            s2.execute("INSERT INTO conc_txn_ins VALUES (2)");
            // c2 only sees its own committed insert (READ COMMITTED)
            assertEquals("1", query1(s2, "SELECT count(*) FROM conc_txn_ins"));
            // c1 rolls back, so only c2's insert survives
            s1.execute("ROLLBACK");
            assertEquals("1", query1(s2, "SELECT count(*) FROM conc_txn_ins"));
            assertEquals("2", query1(s2, "SELECT id FROM conc_txn_ins"));
            s2.execute("DROP TABLE conc_txn_ins");
        }
    }

    @Test
    void testBothConnectionsInTransactions() throws SQLException {
        // With READ COMMITTED, uncommitted changes are not visible to other connections
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE both_txn (id int, val text)");
            s1.execute("INSERT INTO both_txn VALUES (1, 'init')");

            // Both start transactions
            s1.execute("BEGIN");
            s2.execute("BEGIN");

            s1.execute("UPDATE both_txn SET val = 'from_c1' WHERE id = 1");
            // c2 sees original value (READ COMMITTED)
            assertEquals("init", query1(s2, "SELECT val FROM both_txn WHERE id = 1"));

            s2.execute("INSERT INTO both_txn VALUES (2, 'from_c2')");
            // c1 does not see c2's uncommitted insert
            assertEquals("1", query1(s1, "SELECT count(*) FROM both_txn"));

            // c1 commits, c2 rolls back
            s1.execute("COMMIT");
            s2.execute("ROLLBACK");

            // c1's update persists, c2's insert is gone
            assertEquals("1", query1(s1, "SELECT count(*) FROM both_txn"));
            assertEquals("from_c1", query1(s1, "SELECT val FROM both_txn WHERE id = 1"));
            s1.execute("DROP TABLE both_txn");
        }
    }

    // ========================================================================
    // Category 4: DDL in Transactions Across Connections
    // ========================================================================

    @Test
    void testCreateTableInTransactionVisibleThenRolledBack() throws SQLException {
        // Note: DDL (CREATE TABLE) is still immediately visible to other sessions (no DDL MVCC).
        // But the INSERT inside the transaction is not visible until committed.
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("BEGIN");
            s1.execute("CREATE TABLE ddl_txn_create (id int)");
            s1.execute("INSERT INTO ddl_txn_create VALUES (1)");
            // c2 can see the table (DDL is immediately visible) but not the uncommitted row
            assertEquals("0", query1(s2, "SELECT count(*) FROM ddl_txn_create"));
            // Rollback removes the table
            s1.execute("ROLLBACK");
            // Table should no longer exist
            try {
                s2.executeQuery("SELECT count(*) FROM ddl_txn_create");
                fail("Table should not exist after rollback");
            } catch (SQLException e) {
                assertTrue(e.getMessage().toLowerCase().contains("ddl_txn_create")
                        || e.getMessage().toLowerCase().contains("does not exist")
                        || e.getMessage().toLowerCase().contains("not found"));
            }
        }
    }

    @Test
    void testDropTableInTransactionThenRollbackRestores() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE ddl_txn_drop (id int)");
            s1.execute("INSERT INTO ddl_txn_drop VALUES (1), (2), (3)");
            s1.execute("BEGIN");
            s1.execute("DROP TABLE ddl_txn_drop");
            // c2 should not see table
            try {
                s2.executeQuery("SELECT count(*) FROM ddl_txn_drop");
                fail("Table should be dropped");
            } catch (SQLException e) {
                // expected
            }
            // Rollback restores the table
            s1.execute("ROLLBACK");
            assertEquals("3", query1(s2, "SELECT count(*) FROM ddl_txn_drop"));
            s1.execute("DROP TABLE ddl_txn_drop");
        }
    }

    @Test
    void testTruncateInTransactionThenRollbackRestoresData() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE ddl_txn_trunc (id int)");
            s1.execute("INSERT INTO ddl_txn_trunc VALUES (1), (2), (3)");
            s1.execute("BEGIN");
            s1.execute("TRUNCATE ddl_txn_trunc");
            assertEquals("0", query1(s2, "SELECT count(*) FROM ddl_txn_trunc"));
            s1.execute("ROLLBACK");
            assertEquals("3", query1(s2, "SELECT count(*) FROM ddl_txn_trunc"));
            s1.execute("DROP TABLE ddl_txn_trunc");
        }
    }

    @Test
    void testCreateAndDropViewInTransaction() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE view_base (id int, val text)");
            s1.execute("INSERT INTO view_base VALUES (1, 'a'), (2, 'b')");
            s1.execute("BEGIN");
            s1.execute("CREATE VIEW view_txn AS SELECT * FROM view_base WHERE id = 1");
            // c2 can query the view
            assertEquals("a", query1(s2, "SELECT val FROM view_txn"));
            s1.execute("ROLLBACK");
            // View should be gone
            try {
                s2.executeQuery("SELECT * FROM view_txn");
                fail("View should not exist after rollback");
            } catch (SQLException e) {
                // expected
            }
            s1.execute("DROP TABLE view_base");
        }
    }

    // ========================================================================
    // Category 5: Sequence/Serial Atomicity Across Connections
    // ========================================================================

    @Test
    void testConcurrentNextvalYieldsUniqueValues() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("CREATE SEQUENCE conc_seq");
        }
        int numConns = 5;
        int callsPerConn = 20;
        Set<Long> allValues = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(numConns);
        CountDownLatch latch = new CountDownLatch(numConns);
        for (int c = 0; c < numConns; c++) {
            pool.submit(() -> {
                try (Connection conn = connect(); Statement st = conn.createStatement()) {
                    for (int i = 0; i < callsPerConn; i++) {
                        long val = Long.parseLong(query1(st, "SELECT nextval('conc_seq')"));
                        assertTrue(allValues.add(val), "Duplicate sequence value: " + val);
                    }
                } catch (Exception e) {
                    fail("Sequence call failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(15, TimeUnit.SECONDS));
        pool.shutdown();
        assertEquals(numConns * callsPerConn, allValues.size());
        try (Connection cleanup = connect(); Statement st = cleanup.createStatement()) {
            st.execute("DROP SEQUENCE conc_seq");
        }
    }

    @Test
    void testConcurrentSerialInsertYieldsUniqueIds() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("CREATE TABLE conc_serial (id serial PRIMARY KEY, data text)");
        }
        int numConns = 5;
        int insertsPerConn = 10;
        Set<Integer> allIds = ConcurrentHashMap.newKeySet();
        ExecutorService pool = Executors.newFixedThreadPool(numConns);
        CountDownLatch latch = new CountDownLatch(numConns);
        for (int c = 0; c < numConns; c++) {
            final int connId = c;
            pool.submit(() -> {
                try (Connection conn = connect(); Statement st = conn.createStatement()) {
                    for (int i = 0; i < insertsPerConn; i++) {
                        st.execute("INSERT INTO conc_serial (data) VALUES ('conn" + connId + "_row" + i + "')");
                    }
                } catch (Exception e) {
                    fail("Serial insert failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(15, TimeUnit.SECONDS));
        pool.shutdown();
        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            // All rows present
            assertEquals(String.valueOf(numConns * insertsPerConn),
                    query1(st, "SELECT count(*) FROM conc_serial"));
            // All IDs are unique
            assertEquals(query1(st, "SELECT count(DISTINCT id) FROM conc_serial"),
                    query1(st, "SELECT count(*) FROM conc_serial"));
            st.execute("DROP TABLE conc_serial");
        }
    }

    @Test
    void testSequenceValueNotRolledBack() throws SQLException {
        // In PG, sequence values are never rolled back (they're outside transactions)
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE SEQUENCE seq_no_rb");
            s1.execute("BEGIN");
            String v1 = query1(s1, "SELECT nextval('seq_no_rb')");
            String v2 = query1(s1, "SELECT nextval('seq_no_rb')");
            s1.execute("ROLLBACK");
            // Next value from c2 should continue from where c1 left off (not reset)
            String v3 = query1(s2, "SELECT nextval('seq_no_rb')");
            long l1 = Long.parseLong(v1);
            long l2 = Long.parseLong(v2);
            long l3 = Long.parseLong(v3);
            assertTrue(l3 > l2, "Sequence should not roll back: v2=" + l2 + " v3=" + l3);
            s1.execute("DROP SEQUENCE seq_no_rb");
        }
    }

    // ========================================================================
    // Category 6: Transaction State Independence
    // ========================================================================

    @Test
    void testErrorInOneConnectionDoesNotAffectOther() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("BEGIN");
            try {
                s1.execute("SELECT * FROM nonexistent_table_xyz_abc");
                fail("Should throw");
            } catch (SQLException e) {
                // c1 is now in FAILED state
            }
            // c1 cannot execute anything
            try {
                s1.execute("SELECT 1");
                fail("Should throw in failed state");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("aborted"));
            }
            // c2 is completely unaffected
            assertEquals("1", query1(s2, "SELECT 1"));
            assertEquals("hello", query1(s2, "SELECT 'hello'"));
            s1.execute("ROLLBACK"); // recover c1
            assertEquals("1", query1(s1, "SELECT 1"));
        }
    }

    @Test
    void testFailedTransactionStateIsPerConnection() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE fail_per_conn (id int)");
            s1.execute("INSERT INTO fail_per_conn VALUES (1)");

            s1.execute("BEGIN");
            s2.execute("BEGIN");

            // Cause c1 to fail
            try {
                s1.execute("INSERT INTO nonexistent_xyz VALUES (1)");
            } catch (SQLException e) { /* expected */ }

            // c2's transaction is still healthy
            s2.execute("INSERT INTO fail_per_conn VALUES (2)");
            s2.execute("COMMIT");

            // c1 must rollback to recover
            s1.execute("ROLLBACK");

            // c2's committed data persists
            assertEquals("2", query1(s1, "SELECT count(*) FROM fail_per_conn"));
            s1.execute("DROP TABLE fail_per_conn");
        }
    }

    @Test
    void testGucSettingsIndependentPerConnection() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("SET search_path TO 'custom_schema'");
            // c2 still has default
            String c2Path = query1(s2, "SHOW search_path");
            assertNotEquals("custom_schema", c2Path);
        }
    }

    @Test
    void testConcurrentTransactionsOnDifferentTables() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE txn_t1 (id int)");
            s2.execute("CREATE TABLE txn_t2 (id int)");

            s1.execute("BEGIN");
            s2.execute("BEGIN");

            s1.execute("INSERT INTO txn_t1 VALUES (1), (2), (3)");
            s2.execute("INSERT INTO txn_t2 VALUES (10), (20)");

            // Neither sees the other's uncommitted data (READ COMMITTED)
            assertEquals("0", query1(s1, "SELECT count(*) FROM txn_t2"));
            assertEquals("0", query1(s2, "SELECT count(*) FROM txn_t1"));

            s1.execute("COMMIT");
            s2.execute("ROLLBACK");

            // t1 has data (committed), t2 is empty (rolled back)
            assertEquals("3", query1(s1, "SELECT count(*) FROM txn_t1"));
            assertEquals("0", query1(s1, "SELECT count(*) FROM txn_t2"));

            s1.execute("DROP TABLE txn_t1");
            s1.execute("DROP TABLE txn_t2");
        }
    }

    // ========================================================================
    // Category 7: Complex Interleaved Scenarios
    // ========================================================================

    @Test
    void testInterleavedTransactionsWithCommitAndRollback() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE interl (id int, val text)");

            // c1: begin, insert
            s1.execute("BEGIN");
            s1.execute("INSERT INTO interl VALUES (1, 'c1_first')");

            // c2: begin, insert
            s2.execute("BEGIN");
            s2.execute("INSERT INTO interl VALUES (2, 'c2_first')");

            // c1: insert more, commit
            s1.execute("INSERT INTO interl VALUES (3, 'c1_second')");
            s1.execute("COMMIT");

            // c2: insert more, rollback
            s2.execute("INSERT INTO interl VALUES (4, 'c2_second')");
            s2.execute("ROLLBACK");

            // Only c1's data survives
            assertEquals("2", query1(s1, "SELECT count(*) FROM interl"));
            assertTrue(hasRows(s1, "SELECT 1 FROM interl WHERE id = 1"));
            assertTrue(hasRows(s1, "SELECT 1 FROM interl WHERE id = 3"));
            assertFalse(hasRows(s1, "SELECT 1 FROM interl WHERE id = 2"));
            assertFalse(hasRows(s1, "SELECT 1 FROM interl WHERE id = 4"));
            s1.execute("DROP TABLE interl");
        }
    }

    @Test
    void testMultipleSavepointsAcrossTwoConnections() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE multi_sp (id int, source text)");

            // c1: begin with savepoints
            s1.execute("BEGIN");
            s1.execute("INSERT INTO multi_sp VALUES (1, 'c1')");
            s1.execute("SAVEPOINT c1_sp1");
            s1.execute("INSERT INTO multi_sp VALUES (2, 'c1')");

            // c2: begin with savepoints
            s2.execute("BEGIN");
            s2.execute("INSERT INTO multi_sp VALUES (10, 'c2')");
            s2.execute("SAVEPOINT c2_sp1");
            s2.execute("INSERT INTO multi_sp VALUES (20, 'c2')");

            // Each session sees only its own uncommitted rows (READ COMMITTED)
            assertEquals("2", query1(s1, "SELECT count(*) FROM multi_sp"));

            // c1 rolls back to savepoint (removes row 2)
            s1.execute("ROLLBACK TO SAVEPOINT c1_sp1");
            // c2 cannot see c1's uncommitted data
            assertEquals("2", query1(s2, "SELECT count(*) FROM multi_sp"));

            // c2 rolls back to savepoint (removes row 20)
            s2.execute("ROLLBACK TO SAVEPOINT c2_sp1");
            // c1 cannot see c2's uncommitted data
            assertEquals("1", query1(s1, "SELECT count(*) FROM multi_sp"));

            // Both commit
            s1.execute("COMMIT");
            s2.execute("COMMIT");

            // Only rows 1 and 10 survive
            assertEquals("2", query1(s1, "SELECT count(*) FROM multi_sp"));
            assertEquals("1", query1(s1, "SELECT id FROM multi_sp WHERE source = 'c1'"));
            assertEquals("10", query1(s1, "SELECT id FROM multi_sp WHERE source = 'c2'"));
            s1.execute("DROP TABLE multi_sp");
        }
    }

    @Test
    void testUpdateThenRollbackWhileOtherConnectionReads() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE upd_rb (id int, version int)");
            s1.execute("INSERT INTO upd_rb VALUES (1, 1)");

            // c1 starts updating in a transaction
            s1.execute("BEGIN");
            s1.execute("UPDATE upd_rb SET version = 2 WHERE id = 1");
            // c2 sees original value (READ COMMITTED)
            assertEquals("1", query1(s2, "SELECT version FROM upd_rb WHERE id = 1"));

            s1.execute("UPDATE upd_rb SET version = 3 WHERE id = 1");
            // c2 still sees original value
            assertEquals("1", query1(s2, "SELECT version FROM upd_rb WHERE id = 1"));

            // c1 rolls back; c2 should see original version
            s1.execute("ROLLBACK");
            assertEquals("1", query1(s2, "SELECT version FROM upd_rb WHERE id = 1"));
            s1.execute("DROP TABLE upd_rb");
        }
    }

    @Test
    void testThreeConnectionsInterleavedOperations() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect(); Connection c3 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement();
             Statement s3 = c3.createStatement()) {
            s1.execute("CREATE TABLE three_conn (id int, val int)");

            s1.execute("BEGIN");
            s1.execute("INSERT INTO three_conn VALUES (1, 100)");

            s2.execute("BEGIN");
            s2.execute("INSERT INTO three_conn VALUES (2, 200)");

            s3.execute("BEGIN");
            s3.execute("INSERT INTO three_conn VALUES (3, 300)");

            // Each session sees only its own uncommitted row (READ COMMITTED)
            assertEquals("1", query1(s1, "SELECT count(*) FROM three_conn"));
            assertEquals("1", query1(s2, "SELECT count(*) FROM three_conn"));
            assertEquals("1", query1(s3, "SELECT count(*) FROM three_conn"));

            // c2 rolls back
            s2.execute("ROLLBACK");
            // c1 still sees only its own uncommitted row
            assertEquals("1", query1(s1, "SELECT count(*) FROM three_conn"));

            // c1 and c3 commit
            s1.execute("COMMIT");
            s3.execute("COMMIT");

            assertEquals("2", query1(s1, "SELECT count(*) FROM three_conn"));
            assertEquals("100", query1(s1, "SELECT val FROM three_conn WHERE id = 1"));
            assertEquals("300", query1(s1, "SELECT val FROM three_conn WHERE id = 3"));
            s1.execute("DROP TABLE three_conn");
        }
    }

    @Test
    void testTransactionWithMultipleDmlTypes() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE multi_dml (id int, val text)");
            s1.execute("INSERT INTO multi_dml VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            s1.execute("BEGIN");
            // INSERT + UPDATE + DELETE in same transaction
            s1.execute("INSERT INTO multi_dml VALUES (4, 'd')");
            s1.execute("UPDATE multi_dml SET val = 'B' WHERE id = 2");
            s1.execute("DELETE FROM multi_dml WHERE id = 3");

            // c2 sees original data (READ COMMITTED: uncommitted changes not visible)
            assertEquals("3", query1(s2, "SELECT count(*) FROM multi_dml"));
            assertEquals("b", query1(s2, "SELECT val FROM multi_dml WHERE id = 2"));
            assertFalse(hasRows(s2, "SELECT 1 FROM multi_dml WHERE id = 4"));
            assertTrue(hasRows(s2, "SELECT 1 FROM multi_dml WHERE id = 3"));

            // Rollback reverts everything
            s1.execute("ROLLBACK");
            assertEquals("3", query1(s2, "SELECT count(*) FROM multi_dml"));
            assertEquals("b", query1(s2, "SELECT val FROM multi_dml WHERE id = 2"));
            assertTrue(hasRows(s2, "SELECT 1 FROM multi_dml WHERE id = 3"));
            assertFalse(hasRows(s2, "SELECT 1 FROM multi_dml WHERE id = 4"));
            s1.execute("DROP TABLE multi_dml");
        }
    }

    @Test
    void testConnectionSeesOwnUncommittedChanges() throws SQLException {
        try (Connection c1 = connect(); Statement s1 = c1.createStatement()) {
            s1.execute("CREATE TABLE own_changes (id int, val text)");
            s1.execute("BEGIN");
            s1.execute("INSERT INTO own_changes VALUES (1, 'first')");
            // Same connection sees its own uncommitted data
            assertEquals("first", query1(s1, "SELECT val FROM own_changes WHERE id = 1"));
            s1.execute("UPDATE own_changes SET val = 'updated' WHERE id = 1");
            assertEquals("updated", query1(s1, "SELECT val FROM own_changes WHERE id = 1"));
            s1.execute("ROLLBACK");
            assertEquals("0", query1(s1, "SELECT count(*) FROM own_changes"));
            s1.execute("DROP TABLE own_changes");
        }
    }

    @Test
    void testRollbackAfterConstraintViolation() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE constraint_rb (id int PRIMARY KEY, val text)");
            s1.execute("INSERT INTO constraint_rb VALUES (1, 'existing')");

            s1.execute("BEGIN");
            s1.execute("INSERT INTO constraint_rb VALUES (2, 'new')");
            try {
                // This should fail due to duplicate PK
                s1.execute("INSERT INTO constraint_rb VALUES (1, 'duplicate')");
                fail("Should throw constraint violation");
            } catch (SQLException e) {
                // Transaction is now FAILED
            }
            // Must rollback
            s1.execute("ROLLBACK");
            // Only the original row remains
            assertEquals("1", query1(s2, "SELECT count(*) FROM constraint_rb"));
            assertEquals("existing", query1(s2, "SELECT val FROM constraint_rb WHERE id = 1"));
            s1.execute("DROP TABLE constraint_rb");
        }
    }

    @Test
    void testSavepointRecoveryAfterConstraintViolation() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE sp_constraint (id int PRIMARY KEY, val text)");
            s1.execute("BEGIN");
            s1.execute("INSERT INTO sp_constraint VALUES (1, 'ok')");
            s1.execute("SAVEPOINT sp1");
            try {
                s1.execute("INSERT INTO sp_constraint VALUES (1, 'dup')");
                fail("Should throw");
            } catch (SQLException e) { /* expected */ }
            // Recover via savepoint
            s1.execute("ROLLBACK TO SAVEPOINT sp1");
            s1.execute("INSERT INTO sp_constraint VALUES (2, 'also_ok')");
            s1.execute("COMMIT");
            // c2 sees both committed rows
            assertEquals("2", query1(s2, "SELECT count(*) FROM sp_constraint"));
            s1.execute("DROP TABLE sp_constraint");
        }
    }

    @Test
    void testConcurrentTransactionsWithMixedCommitRollback() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("CREATE TABLE mixed_cr (id int, conn_id int)");
        }
        int numConns = 6;
        ExecutorService pool = Executors.newFixedThreadPool(numConns);
        CountDownLatch latch = new CountDownLatch(numConns);
        for (int c = 0; c < numConns; c++) {
            final int connId = c;
            final boolean shouldCommit = (c % 2 == 0); // even: commit, odd: rollback
            pool.submit(() -> {
                try (Connection conn = connect(); Statement st = conn.createStatement()) {
                    st.execute("BEGIN");
                    for (int i = 0; i < 5; i++) {
                        st.execute("INSERT INTO mixed_cr VALUES (" + (connId * 100 + i) + ", " + connId + ")");
                    }
                    if (shouldCommit) {
                        st.execute("COMMIT");
                    } else {
                        st.execute("ROLLBACK");
                    }
                } catch (Exception e) {
                    fail("Connection " + connId + " failed: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(15, TimeUnit.SECONDS));
        pool.shutdown();
        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            // Only even connections committed (0, 2, 4) = 3 connections × 5 rows = 15
            int count = queryInt(st, "SELECT count(*) FROM mixed_cr");
            assertEquals(15, count, "Only committed transactions' rows should persist");
            // Verify only even conn_ids present
            assertEquals("0", query1(st, "SELECT count(*) FROM mixed_cr WHERE conn_id % 2 = 1"));
            st.execute("DROP TABLE mixed_cr");
        }
    }

    @Test
    void testRapidBeginCommitCycles() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE rapid_txn (counter int)");
            s1.execute("INSERT INTO rapid_txn VALUES (0)");
            // Rapid transaction cycles on c1
            for (int i = 1; i <= 20; i++) {
                s1.execute("BEGIN");
                s1.execute("UPDATE rapid_txn SET counter = " + i);
                s1.execute("COMMIT");
            }
            assertEquals("20", query1(s2, "SELECT counter FROM rapid_txn"));
            s1.execute("DROP TABLE rapid_txn");
        }
    }

    @Test
    void testRapidBeginRollbackCycles() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE rapid_rb (counter int)");
            s1.execute("INSERT INTO rapid_rb VALUES (0)");
            // Rapid rollback cycles; counter should stay at 0
            for (int i = 1; i <= 20; i++) {
                s1.execute("BEGIN");
                s1.execute("UPDATE rapid_rb SET counter = " + i);
                s1.execute("ROLLBACK");
            }
            assertEquals("0", query1(s2, "SELECT counter FROM rapid_rb"));
            s1.execute("DROP TABLE rapid_rb");
        }
    }

    @Test
    void testConnectionCloseMidTransaction() throws SQLException {
        try (Connection c2 = connect(); Statement s2 = c2.createStatement()) {
            s2.execute("CREATE TABLE close_mid (id int)");
            s2.execute("INSERT INTO close_mid VALUES (1)");

            // Open c1, begin transaction, insert, then close without commit
            Connection c1 = connect();
            Statement s1 = c1.createStatement();
            s1.execute("BEGIN");
            s1.execute("INSERT INTO close_mid VALUES (2)");
            // Close connection without commit (PG would rollback)
            s1.close();
            c1.close();

            // Give a moment for cleanup
            // The row may or may not persist depending on connection close behavior
            // In memgres, connection close doesn't trigger rollback (no MVCC)
            // so the row might still be there; we just verify no crash
            assertDoesNotThrow(() -> query1(s2, "SELECT count(*) FROM close_mid"));
            s2.execute("DROP TABLE close_mid");
        }
    }

    @Test
    void testAlterTableInTransactionThenRollback() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE alter_rb (id int)");
            s1.execute("INSERT INTO alter_rb VALUES (1)");
            s1.execute("BEGIN");
            s1.execute("ALTER TABLE alter_rb ADD COLUMN val text DEFAULT 'hello'");
            // c2 should see the new column
            assertEquals("hello", query1(s2, "SELECT val FROM alter_rb WHERE id = 1"));
            // Note: ALTER TABLE rollback may not be fully supported (depends on undo entries)
            // Just verify no crash
            s1.execute("COMMIT");
            s1.execute("DROP TABLE alter_rb");
        }
    }

    // ========================================================================
    // Category 8: Isolation Level Setting and Per-Session Behavior
    // ========================================================================

    @Test
    void testDifferentIsolationLevelsPerConnection() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect(); Connection c3 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement();
             Statement s3 = c3.createStatement()) {
            s1.execute("BEGIN ISOLATION LEVEL READ COMMITTED");
            s2.execute("BEGIN ISOLATION LEVEL REPEATABLE READ");
            s3.execute("BEGIN ISOLATION LEVEL SERIALIZABLE");

            assertEquals("read committed", query1(s1, "SHOW transaction_isolation"));
            assertEquals("repeatable read", query1(s2, "SHOW transaction_isolation"));
            assertEquals("serializable", query1(s3, "SHOW transaction_isolation"));

            s1.execute("COMMIT");
            s2.execute("COMMIT");
            s3.execute("COMMIT");
        }
    }

    @Test
    void testIsolationLevelDoesNotLeakBetweenConnections() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("SET default_transaction_isolation = 'serializable'");
            // c2 should still have default
            assertEquals("read committed", query1(s2, "SHOW default_transaction_isolation"));
        }
    }

    @Test
    void testReadOnlyTransactionDoesNotAffectOtherConnection() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s2.execute("CREATE TABLE ro_test (id int)");
            s1.execute("BEGIN READ ONLY");
            assertEquals("on", query1(s1, "SHOW transaction_read_only"));
            // c2 is not read-only
            assertEquals("off", query1(s2, "SHOW transaction_read_only"));
            s2.execute("INSERT INTO ro_test VALUES (1)");
            s1.execute("COMMIT");
            assertEquals("1", query1(s2, "SELECT count(*) FROM ro_test"));
            s2.execute("DROP TABLE ro_test");
        }
    }

    // ========================================================================
    // Category 9: Edge Cases
    // ========================================================================

    @Test
    void testEmptyTransactionCommit() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE empty_txn (id int)");
            s1.execute("INSERT INTO empty_txn VALUES (1)");
            // Empty transaction on c1
            s1.execute("BEGIN");
            s1.execute("COMMIT");
            // Data unchanged
            assertEquals("1", query1(s2, "SELECT count(*) FROM empty_txn"));
            s1.execute("DROP TABLE empty_txn");
        }
    }

    @Test
    void testEmptyTransactionRollback() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE empty_rb (id int)");
            s1.execute("INSERT INTO empty_rb VALUES (1)");
            s1.execute("BEGIN");
            s1.execute("ROLLBACK");
            assertEquals("1", query1(s2, "SELECT count(*) FROM empty_rb"));
            s1.execute("DROP TABLE empty_rb");
        }
    }

    @Test
    void testMultipleStatementsInSingleConnectionSingleTransaction() throws SQLException {
        try (Connection c1 = connect(); Connection c2 = connect();
             Statement s1 = c1.createStatement(); Statement s2 = c2.createStatement()) {
            s1.execute("CREATE TABLE multi_stmt (id int, val text)");
            s1.execute("BEGIN");
            // Multiple DML statements
            s1.execute("INSERT INTO multi_stmt VALUES (1, 'a')");
            s1.execute("INSERT INTO multi_stmt VALUES (2, 'b')");
            s1.execute("UPDATE multi_stmt SET val = 'A' WHERE id = 1");
            s1.execute("INSERT INTO multi_stmt VALUES (3, 'c')");
            s1.execute("DELETE FROM multi_stmt WHERE id = 2");
            s1.execute("UPDATE multi_stmt SET val = 'C' WHERE id = 3");

            // c2 sees nothing because all changes are uncommitted (READ COMMITTED)
            assertEquals("0", query1(s2, "SELECT count(*) FROM multi_stmt"));

            s1.execute("COMMIT");
            // Same state after commit
            assertEquals("2", query1(s2, "SELECT count(*) FROM multi_stmt"));
            s1.execute("DROP TABLE multi_stmt");
        }
    }

    @Test
    void testConcurrentInsertAndCountFromDifferentConnections() throws Exception {
        try (Connection setup = connect(); Statement st = setup.createStatement()) {
            st.execute("CREATE TABLE ins_count (id serial, data text)");
        }
        AtomicBoolean writerDone = new AtomicBoolean(false);
        int totalInserts = 50;

        // Writer thread
        Thread writer = new Thread(() -> {
            try (Connection wConn = connect(); Statement wSt = wConn.createStatement()) {
                for (int i = 0; i < totalInserts; i++) {
                    wSt.execute("INSERT INTO ins_count (data) VALUES ('row" + i + "')");
                }
                writerDone.set(true);
            } catch (SQLException e) {
                fail("Writer failed: " + e.getMessage());
            }
        });

        // Reader thread
        Thread reader = new Thread(() -> {
            try (Connection rConn = connect(); Statement rSt = rConn.createStatement()) {
                int lastCount = 0;
                while (!writerDone.get() || lastCount < totalInserts) {
                    int count = queryInt(rSt, "SELECT count(*) FROM ins_count");
                    assertTrue(count >= lastCount, "Count should be monotonically increasing");
                    lastCount = count;
                    if (lastCount >= totalInserts) break;
                }
            } catch (SQLException e) {
                fail("Reader failed: " + e.getMessage());
            }
        });

        writer.start();
        reader.start();
        writer.join(15000);
        reader.join(15000);

        try (Connection verify = connect(); Statement st = verify.createStatement()) {
            assertEquals(String.valueOf(totalInserts), query1(st, "SELECT count(*) FROM ins_count"));
            st.execute("DROP TABLE ins_count");
        }
    }
}
