package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 88-93:
 *
 * 88. Basic transactions (BEGIN, COMMIT, ROLLBACK, END, ABORT, autocommit)
 * 89. Transaction options (ISOLATION LEVEL, READ ONLY/WRITE, DEFERRABLE)
 * 90. Savepoints (SAVEPOINT, RELEASE, ROLLBACK TO)
 * 91. Prepared statements (PREPARE, EXECUTE, DEALLOCATE)
 * 92. Cursors (DECLARE, FETCH, MOVE, CLOSE)
 * 93. Locking (LOCK TABLE, SELECT FOR UPDATE/SHARE, SET CONSTRAINTS)
 */
class TransactionCursorLockCoverageTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private boolean hasRows(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next();
        }
    }

    @BeforeEach
    void clean() throws Exception {
        try { exec("ROLLBACK"); } catch (Exception ignored) {}
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'");
            java.util.List<String> tables = new java.util.ArrayList<>();
            while (rs.next()) tables.add(rs.getString(1));
            for (String t : tables) {
                try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (Exception ignored) {}
            }
        }
        try { exec("DEALLOCATE ALL"); } catch (Exception ignored) {}
        try { exec("CLOSE ALL"); } catch (Exception ignored) {}
    }

    // ========================================================================
    // 88. Basic transactions
    // ========================================================================

    @Test
    void testBeginInsertCommit() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, name TEXT)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1, 'Alice')");
        exec("COMMIT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
        assertEquals("Alice", query1("SELECT name FROM t1 WHERE id = 1"));
    }

    @Test
    void testBeginInsertRollback() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, name TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'Before')");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (2, 'During')");
        exec("ROLLBACK");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
        assertEquals("Before", query1("SELECT name FROM t1 WHERE id = 1"));
    }

    @Test
    void testStartTransactionCommit() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("START TRANSACTION");
        exec("INSERT INTO t1 VALUES (1)");
        exec("COMMIT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testBeginWorkCommit() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN WORK");
        exec("INSERT INTO t1 VALUES (1)");
        exec("COMMIT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testBeginTransactionCommit() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN TRANSACTION");
        exec("INSERT INTO t1 VALUES (1)");
        exec("COMMIT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testCommitWithoutBegin() throws SQLException {
        // PG accepts COMMIT outside transaction block (warning, but no error)
        exec("COMMIT");
    }

    @Test
    void testRollbackWithoutBegin() throws SQLException {
        // PG accepts ROLLBACK outside transaction block
        exec("ROLLBACK");
    }

    @Test
    void testEndAsSynonymForCommit() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("END");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testAbortAsSynonymForRollback() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1, 'keep')".replace(", 'keep'", ""));
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (2)");
        exec("ABORT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testImplicitAutocommit() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        // Without BEGIN, data should be auto-committed
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testRollbackUndoesUpdate() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'original')");
        exec("BEGIN");
        exec("UPDATE t1 SET val = 'modified' WHERE id = 1");
        assertEquals("modified", query1("SELECT val FROM t1 WHERE id = 1"));
        exec("ROLLBACK");
        assertEquals("original", query1("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void testRollbackUndoesDelete() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("INSERT INTO t1 VALUES (2)");
        exec("BEGIN");
        exec("DELETE FROM t1 WHERE id = 1");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
        exec("ROLLBACK");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testMultipleOperationsInTransaction() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1, 'a')");
        exec("INSERT INTO t1 VALUES (2, 'b')");
        exec("UPDATE t1 SET val = 'A' WHERE id = 1");
        exec("DELETE FROM t1 WHERE id = 2");
        exec("COMMIT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
        assertEquals("A", query1("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void testRollbackMultipleOperations() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'orig')");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (2, 'new')");
        exec("UPDATE t1 SET val = 'changed' WHERE id = 1");
        exec("ROLLBACK");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
        assertEquals("orig", query1("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void testCommitWorkSynonym() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("COMMIT WORK");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testCommitTransactionSynonym() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("COMMIT TRANSACTION");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testEndWorkSynonym() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("END WORK");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testEndTransactionSynonym() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("END TRANSACTION");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testConsecutiveTransactions() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("COMMIT");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (2)");
        exec("COMMIT");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testTransactionInsertThenSelectInside() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (42)");
        assertEquals(42, queryInt("SELECT id FROM t1"));
        exec("COMMIT");
    }

    // ========================================================================
    // 89. Transaction options (parsed as no-ops)
    // ========================================================================

    @Test
    void testBeginIsolationLevelReadCommitted() throws SQLException {
        exec("BEGIN ISOLATION LEVEL READ COMMITTED");
        exec("COMMIT");
    }

    @Test
    void testBeginIsolationLevelRepeatableRead() throws SQLException {
        exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
        exec("COMMIT");
    }

    @Test
    void testBeginIsolationLevelSerializable() throws SQLException {
        exec("BEGIN ISOLATION LEVEL SERIALIZABLE");
        exec("COMMIT");
    }

    @Test
    void testBeginIsolationLevelReadUncommitted() throws SQLException {
        exec("BEGIN ISOLATION LEVEL READ UNCOMMITTED");
        exec("COMMIT");
    }

    @Test
    void testBeginReadOnly() throws SQLException {
        exec("BEGIN READ ONLY");
        exec("COMMIT");
    }

    @Test
    void testBeginReadWrite() throws SQLException {
        exec("BEGIN READ WRITE");
        exec("COMMIT");
    }

    @Test
    void testBeginNotDeferrable() throws SQLException {
        exec("BEGIN NOT DEFERRABLE");
        exec("COMMIT");
    }

    @Test
    void testBeginDeferrable() throws SQLException {
        exec("BEGIN DEFERRABLE");
        exec("COMMIT");
    }

    @Test
    void testStartTransactionIsolationLevelSerializable() throws SQLException {
        exec("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        exec("COMMIT");
    }

    @Test
    void testStartTransactionReadOnly() throws SQLException {
        exec("START TRANSACTION READ ONLY");
        exec("COMMIT");
    }

    @Test
    void testSetTransactionIsolationLevelReadCommitted() throws SQLException {
        exec("BEGIN");
        exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
        exec("COMMIT");
    }

    @Test
    void testSetTransactionIsolationLevelRepeatableRead() throws SQLException {
        exec("BEGIN");
        exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        exec("COMMIT");
    }

    @Test
    void testSetTransactionIsolationLevelSerializable() throws SQLException {
        exec("BEGIN");
        exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        exec("COMMIT");
    }

    @Test
    void testSetTransactionReadOnly() throws SQLException {
        exec("BEGIN");
        exec("SET TRANSACTION READ ONLY");
        exec("COMMIT");
    }

    @Test
    void testSetTransactionReadWrite() throws SQLException {
        exec("BEGIN");
        exec("SET TRANSACTION READ WRITE");
        exec("COMMIT");
    }

    @Test
    void testSetTransactionDeferrable() throws SQLException {
        exec("BEGIN");
        exec("SET TRANSACTION DEFERRABLE");
        exec("COMMIT");
    }

    @Test
    void testSetTransactionNotDeferrable() throws SQLException {
        exec("BEGIN");
        exec("SET TRANSACTION NOT DEFERRABLE");
        exec("COMMIT");
    }

    @Test
    void testBeginIsolationAndReadOnly() throws SQLException {
        exec("BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY");
        exec("COMMIT");
    }

    @Test
    void testStartTransactionMultipleOptions() throws SQLException {
        exec("START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE, NOT DEFERRABLE");
        exec("COMMIT");
    }

    @Test
    void testBeginWorkIsolationLevel() throws SQLException {
        exec("BEGIN WORK ISOLATION LEVEL REPEATABLE READ");
        exec("COMMIT");
    }

    // ========================================================================
    // 90. Savepoints
    // ========================================================================

    @Test
    void testSavepointAndRollbackTo() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (2)");
        exec("ROLLBACK TO SAVEPOINT sp1");
        exec("COMMIT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
        assertEquals(1, queryInt("SELECT id FROM t1"));
    }

    @Test
    void testReleaseSavepoint() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (2)");
        exec("RELEASE SAVEPOINT sp1");
        exec("COMMIT");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testNestedSavepoints() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (2)");
        exec("SAVEPOINT sp2");
        exec("INSERT INTO t1 VALUES (3)");
        exec("ROLLBACK TO SAVEPOINT sp2");
        // sp2 rolled back, so 3 is gone, but 2 is still there
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
        exec("COMMIT");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testNestedSavepointsBothRolledBack() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (2)");
        exec("SAVEPOINT sp2");
        exec("INSERT INTO t1 VALUES (3)");
        exec("ROLLBACK TO SAVEPOINT sp1");
        // Rolling back to sp1 undoes both sp2 and sp1
        exec("COMMIT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testRollbackToWithoutSavepointKeyword() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (2)");
        exec("ROLLBACK TO sp1");
        exec("COMMIT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testSavepointWithUpdate() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'before')");
        exec("BEGIN");
        exec("SAVEPOINT sp1");
        exec("UPDATE t1 SET val = 'after' WHERE id = 1");
        exec("ROLLBACK TO SAVEPOINT sp1");
        exec("COMMIT");
        assertEquals("before", query1("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void testSavepointWithDelete() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("INSERT INTO t1 VALUES (2)");
        exec("BEGIN");
        exec("SAVEPOINT sp1");
        exec("DELETE FROM t1 WHERE id = 2");
        exec("ROLLBACK TO SAVEPOINT sp1");
        exec("COMMIT");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testSavepointReleaseAndContinue() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (1)");
        exec("RELEASE SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (2)");
        exec("COMMIT");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testMultipleSavepoints() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("SAVEPOINT a");
        exec("INSERT INTO t1 VALUES (2)");
        exec("SAVEPOINT b");
        exec("INSERT INTO t1 VALUES (3)");
        exec("ROLLBACK TO SAVEPOINT b");
        exec("COMMIT");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
    }

    // ========================================================================
    // 91. Prepared statements
    // ========================================================================

    @Test
    void testPrepareSelectLiteral() throws SQLException {
        exec("PREPARE myplan AS SELECT 1");
        assertEquals(1, queryInt("EXECUTE myplan"));
    }

    @Test
    void testPrepareWithIntParam() throws SQLException {
        exec("PREPARE myplan (int) AS SELECT $1");
        assertEquals(42, queryInt("EXECUTE myplan(42)"));
    }

    @Test
    void testPrepareWithTextParam() throws SQLException {
        exec("PREPARE myplan (text) AS SELECT $1");
        assertEquals("hello", query1("EXECUTE myplan('hello')"));
    }

    @Test
    void testPrepareInsertThenExecute() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, name TEXT)");
        exec("PREPARE ins_plan (int, text) AS INSERT INTO t1 VALUES ($1, $2)");
        exec("EXECUTE ins_plan(1, 'Alice')");
        exec("EXECUTE ins_plan(2, 'Bob')");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
        assertEquals("Alice", query1("SELECT name FROM t1 WHERE id = 1"));
        assertEquals("Bob", query1("SELECT name FROM t1 WHERE id = 2"));
    }

    @Test
    void testPrepareSelectFromTable() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, name TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')");
        exec("PREPARE sel_plan (int) AS SELECT name FROM t1 WHERE id = $1");
        assertEquals("Alice", query1("EXECUTE sel_plan(1)"));
        assertEquals("Bob", query1("EXECUTE sel_plan(2)"));
    }

    @Test
    void testPrepareMultipleParams() throws SQLException {
        exec("PREPARE calc (int, int) AS SELECT $1 + $2");
        assertEquals(7, queryInt("EXECUTE calc(3, 4)"));
    }

    @Test
    void testPrepareWithExpressionParams() throws SQLException {
        exec("PREPARE calc (int) AS SELECT $1 * 2");
        assertEquals(10, queryInt("EXECUTE calc(5)"));
    }

    @Test
    void testDeallocatePrepared() throws SQLException {
        exec("PREPARE myplan AS SELECT 1");
        assertEquals(1, queryInt("EXECUTE myplan"));
        exec("DEALLOCATE myplan");
        try {
            exec("EXECUTE myplan");
            fail("Expected error for deallocated plan");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    void testDeallocatePrepareSyntax() throws SQLException {
        exec("PREPARE myplan AS SELECT 1");
        exec("DEALLOCATE PREPARE myplan");
    }

    @Test
    void testDeallocateAll() throws SQLException {
        exec("PREPARE plan1 AS SELECT 1");
        exec("PREPARE plan2 AS SELECT 2");
        exec("DEALLOCATE ALL");
        try {
            exec("EXECUTE plan1");
            fail("Expected error");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
        try {
            exec("EXECUTE plan2");
            fail("Expected error");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    void testExecuteNonExistent() throws SQLException {
        try {
            exec("EXECUTE nonexistent");
            fail("Expected error");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    void testDeallocateNonExistent() throws SQLException {
        try {
            exec("DEALLOCATE nonexistent");
            fail("Expected error");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    void testPrepareUpdate() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'old')");
        exec("PREPARE upd (text, int) AS UPDATE t1 SET val = $1 WHERE id = $2");
        exec("EXECUTE upd('new', 1)");
        assertEquals("new", query1("SELECT val FROM t1 WHERE id = 1"));
    }

    @Test
    void testPrepareDelete() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("PREPARE del (int) AS DELETE FROM t1 WHERE id = $1");
        exec("EXECUTE del(2)");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testPrepareNoParams() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("PREPARE cnt AS SELECT COUNT(*) FROM t1");
        assertEquals(3, queryInt("EXECUTE cnt"));
    }

    @Test
    void testPrepareReusedMultipleTimes() throws SQLException {
        exec("PREPARE addone (int) AS SELECT $1 + 1");
        assertEquals(2, queryInt("EXECUTE addone(1)"));
        assertEquals(11, queryInt("EXECUTE addone(10)"));
        assertEquals(101, queryInt("EXECUTE addone(100)"));
    }

    @Test
    void testPrepareThreeParams() throws SQLException {
        exec("CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)");
        exec("PREPARE ins3 (int, int, int) AS INSERT INTO t1 VALUES ($1, $2, $3)");
        exec("EXECUTE ins3(10, 20, 30)");
        assertEquals(10, queryInt("SELECT a FROM t1"));
        assertEquals(20, queryInt("SELECT b FROM t1"));
        assertEquals(30, queryInt("SELECT c FROM t1"));
    }

    // ========================================================================
    // 92. Cursors
    // ========================================================================

    @Test
    void testDeclareCursorAndFetchNext() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        assertEquals(1, queryInt("FETCH NEXT FROM c1"));
        assertEquals(2, queryInt("FETCH NEXT FROM c1"));
        assertEquals(3, queryInt("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchImplicitNext() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        assertEquals(1, queryInt("FETCH FROM c1"));
        assertEquals(2, queryInt("FETCH FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchFirst() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        assertEquals(1, queryInt("FETCH FIRST FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchLast() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        assertEquals(3, queryInt("FETCH LAST FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchAbsolute() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (10), (20), (30)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        assertEquals(20, queryInt("FETCH ABSOLUTE 2 FROM c1"));
        assertEquals(30, queryInt("FETCH ABSOLUTE 3 FROM c1"));
        assertEquals(10, queryInt("FETCH ABSOLUTE 1 FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchRelative() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (10), (20), (30)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        // Start before first row, RELATIVE 1 = first row
        assertEquals(10, queryInt("FETCH RELATIVE 1 FROM c1"));
        assertEquals(20, queryInt("FETCH RELATIVE 1 FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchForwardN() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3), (4), (5)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH FORWARD 3 FROM c1")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchForwardAll() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH FORWARD ALL FROM c1")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchAll() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH ALL FROM c1")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchPrior() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 SCROLL CURSOR FOR SELECT id FROM t1 ORDER BY id");
        assertEquals(1, queryInt("FETCH NEXT FROM c1"));
        assertEquals(2, queryInt("FETCH NEXT FROM c1"));
        assertEquals(1, queryInt("FETCH PRIOR FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchBackwardN() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 SCROLL CURSOR FOR SELECT id FROM t1 ORDER BY id");
        // Move to end
        exec("FETCH LAST FROM c1");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH BACKWARD 2 FROM c1")) {
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchBackwardAll() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 SCROLL CURSOR FOR SELECT id FROM t1 ORDER BY id");
        exec("FETCH LAST FROM c1");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH BACKWARD ALL FROM c1")) {
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testMoveCursor() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3), (4), (5)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        exec("MOVE NEXT IN c1");
        exec("MOVE NEXT IN c1");
        // After moving forward 2, next fetch should return 3
        assertEquals(3, queryInt("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testMoveForwardN() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3), (4), (5)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        exec("MOVE FORWARD 3 IN c1");
        assertEquals(4, queryInt("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testCloseCursor() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1");
        exec("CLOSE c1");
        try {
            exec("FETCH NEXT FROM c1");
            fail("Expected error for closed cursor");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
        exec("COMMIT");
    }

    @Test
    void testCloseAll() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1");
        exec("DECLARE c2 CURSOR FOR SELECT id FROM t1");
        exec("CLOSE ALL");
        try {
            exec("FETCH NEXT FROM c1");
            fail("Expected error");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
        exec("COMMIT");
    }

    @Test
    void testDeclareScrollCursor() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 SCROLL CURSOR FOR SELECT id FROM t1 ORDER BY id");
        assertEquals(3, queryInt("FETCH LAST FROM c1"));
        assertEquals(1, queryInt("FETCH FIRST FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testDeclareNoScrollCursor() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2)");
        exec("BEGIN");
        exec("DECLARE c1 NO SCROLL CURSOR FOR SELECT id FROM t1 ORDER BY id");
        assertEquals(1, queryInt("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testDeclareWithHoldCursor() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR WITH HOLD FOR SELECT id FROM t1 ORDER BY id");
        assertEquals(1, queryInt("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testMultipleCursors() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("CREATE TABLE t2 (val TEXT)");
        exec("INSERT INTO t1 VALUES (1), (2)");
        exec("INSERT INTO t2 VALUES ('a'), ('b')");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        exec("DECLARE c2 CURSOR FOR SELECT val FROM t2 ORDER BY val");
        assertEquals(1, queryInt("FETCH NEXT FROM c1"));
        assertEquals("a", query1("FETCH NEXT FROM c2"));
        assertEquals(2, queryInt("FETCH NEXT FROM c1"));
        assertEquals("b", query1("FETCH NEXT FROM c2"));
        exec("CLOSE c1");
        exec("CLOSE c2");
        exec("COMMIT");
    }

    @Test
    void testCursorOnEmptyResult() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH NEXT FROM c1")) {
            assertFalse(rs.next());
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchFromNonExistentCursor() throws SQLException {
        exec("BEGIN");
        try {
            exec("FETCH NEXT FROM nonexistent");
            fail("Expected error");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("does not exist"));
        }
        exec("ROLLBACK");
    }

    @Test
    void testDuplicateCursorName() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1");
        try {
            exec("DECLARE c1 CURSOR FOR SELECT id FROM t1");
            fail("Expected error for duplicate cursor");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("already exists"));
        }
        exec("ROLLBACK");
    }

    @Test
    void testCursorWithWhereClause() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT val FROM t1 WHERE id > 1 ORDER BY id");
        assertEquals("b", query1("FETCH NEXT FROM c1"));
        assertEquals("c", query1("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testFetchUsingIN() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        // FETCH NEXT IN c1 (using IN instead of FROM)
        assertEquals(1, queryInt("FETCH NEXT IN c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testCursorFetchAbsoluteNegative() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (10), (20), (30)");
        exec("BEGIN");
        exec("DECLARE c1 SCROLL CURSOR FOR SELECT id FROM t1 ORDER BY id");
        // ABSOLUTE -1 = last row
        assertEquals(30, queryInt("FETCH ABSOLUTE -1 FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testCursorWithMultipleColumns() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, name TEXT, val REAL)");
        exec("INSERT INTO t1 VALUES (1, 'Alice', 3.14), (2, 'Bob', 2.72)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id, name, val FROM t1 ORDER BY id");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH NEXT FROM c1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("Alice", rs.getString(2));
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testMoveAbsolute() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3), (4), (5)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        exec("MOVE ABSOLUTE 3 IN c1");
        assertEquals(4, queryInt("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    // ========================================================================
    // 93. Locking (all no-ops, just verify parsing)
    // ========================================================================

    @Test
    void testLockTableAccessShare() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN ACCESS SHARE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableRowShare() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN ROW SHARE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableRowExclusive() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN ROW EXCLUSIVE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableShareUpdateExclusive() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN SHARE UPDATE EXCLUSIVE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableShare() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN SHARE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableShareRowExclusive() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN SHARE ROW EXCLUSIVE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableExclusive() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN EXCLUSIVE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableAccessExclusive() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN ACCESS EXCLUSIVE MODE");
        exec("COMMIT");
    }

    @Test
    void testLockTableAccessExclusiveNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN ACCESS EXCLUSIVE MODE NOWAIT");
        exec("COMMIT");
    }

    @Test
    void testLockWithoutTableKeyword() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK t1 IN ACCESS SHARE MODE");
        exec("COMMIT");
    }

    @Test
    void testSelectForUpdate() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR UPDATE")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForNoKeyUpdate() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'a')");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR NO KEY UPDATE")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForShare() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'a')");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR SHARE")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForKeyShare() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'a')");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR KEY SHARE")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForUpdateNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR UPDATE NOWAIT")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForUpdateSkipLocked() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR UPDATE SKIP LOCKED")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForUpdateOfTable() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR UPDATE OF t1")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForShareNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR SHARE NOWAIT")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSetConstraintsAllDeferred() throws SQLException {
        exec("SET CONSTRAINTS ALL DEFERRED");
    }

    @Test
    void testSetConstraintsAllImmediate() throws SQLException {
        exec("SET CONSTRAINTS ALL IMMEDIATE");
    }

    @Test
    void testSelectForUpdateWithWhere() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT val FROM t1 WHERE id = 2 FOR UPDATE")) {
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForUpdateWithOrderBy() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (3), (1), (2)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT id FROM t1 ORDER BY id FOR UPDATE")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForUpdateWithLimit() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT id FROM t1 ORDER BY id LIMIT 2 FOR UPDATE")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("COMMIT");
    }

    // ========================================================================
    // Additional edge cases
    // ========================================================================

    @Test
    void testTransactionWithPreparedStmt() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("PREPARE ins (int) AS INSERT INTO t1 VALUES ($1)");
        exec("BEGIN");
        exec("EXECUTE ins(1)");
        exec("EXECUTE ins(2)");
        exec("ROLLBACK");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testTransactionWithPreparedStmtCommit() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("PREPARE ins (int) AS INSERT INTO t1 VALUES ($1)");
        exec("BEGIN");
        exec("EXECUTE ins(1)");
        exec("EXECUTE ins(2)");
        exec("COMMIT");
        assertEquals(2, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testCursorWithOrderByDesc() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id DESC");
        assertEquals(3, queryInt("FETCH NEXT FROM c1"));
        assertEquals(2, queryInt("FETCH NEXT FROM c1"));
        assertEquals(1, queryInt("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testSavepointInsideTransactionWithCursor() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (4)");
        exec("ROLLBACK TO SAVEPOINT sp1");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH ALL FROM c1")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testLockTableRowShareNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN ROW SHARE MODE NOWAIT");
        exec("COMMIT");
    }

    @Test
    void testLockTableExclusiveNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN EXCLUSIVE MODE NOWAIT");
        exec("COMMIT");
    }

    @Test
    void testPrepareSelectWithCondition() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, name TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");
        exec("PREPARE findname (int) AS SELECT name FROM t1 WHERE id = $1");
        assertEquals("Bob", query1("EXECUTE findname(2)"));
        assertEquals("Charlie", query1("EXECUTE findname(3)"));
    }

    @Test
    void testCursorForward2() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3), (4), (5)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH FORWARD 2 FROM c1")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        }
        assertEquals(3, queryInt("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testBeginRollbackBeginCommit() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("ROLLBACK");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (2)");
        exec("COMMIT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
        assertEquals(2, queryInt("SELECT id FROM t1"));
    }

    @Test
    void testAutocommitInsertIsVisible() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("INSERT INTO t1 VALUES (2)");
        exec("INSERT INTO t1 VALUES (3)");
        assertEquals(3, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testAbortAfterInsert() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (2)");
        exec("ABORT");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testRollbackUndoesMultipleInserts() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO t1 VALUES (1)");
        exec("INSERT INTO t1 VALUES (2)");
        exec("INSERT INTO t1 VALUES (3)");
        exec("ROLLBACK");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testSetConstraintsNamedDeferred() throws SQLException {
        exec("SET CONSTRAINTS myconstraint DEFERRED");
    }

    @Test
    void testSetConstraintsNamedImmediate() throws SQLException {
        exec("SET CONSTRAINTS myconstraint IMMEDIATE");
    }

    @Test
    void testSelectForUpdateOfMultipleTables() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, val TEXT)");
        exec("CREATE TABLE t2 (id INTEGER, ref INTEGER)");
        exec("INSERT INTO t1 VALUES (1, 'a')");
        exec("INSERT INTO t2 VALUES (1, 1)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(
                "SELECT t1.val FROM t1 JOIN t2 ON t1.id = t2.ref FOR UPDATE OF t1, t2")) {
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
        }
        exec("COMMIT");
    }

    @Test
    void testStartTransactionReadWrite() throws SQLException {
        exec("START TRANSACTION READ WRITE");
        exec("COMMIT");
    }

    @Test
    void testStartTransactionDeferrable() throws SQLException {
        exec("START TRANSACTION DEFERRABLE");
        exec("COMMIT");
    }

    @Test
    void testStartTransactionNotDeferrable() throws SQLException {
        exec("START TRANSACTION NOT DEFERRABLE");
        exec("COMMIT");
    }

    @Test
    void testStartTransactionIsolationLevelReadUncommitted() throws SQLException {
        exec("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        exec("COMMIT");
    }

    @Test
    void testStartTransactionIsolationLevelRepeatableRead() throws SQLException {
        exec("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        exec("COMMIT");
    }

    @Test
    void testSetTransactionIsolationLevelReadUncommitted() throws SQLException {
        exec("BEGIN");
        exec("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        exec("COMMIT");
    }

    @Test
    void testBeginIsolationReadOnly() throws SQLException {
        exec("BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY");
        exec("COMMIT");
    }

    @Test
    void testBeginIsolationReadWrite() throws SQLException {
        exec("BEGIN ISOLATION LEVEL SERIALIZABLE, READ WRITE");
        exec("COMMIT");
    }

    @Test
    void testBeginIsolationDeferrable() throws SQLException {
        exec("BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE");
        exec("COMMIT");
    }

    @Test
    void testPrepareSelectStar() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, name TEXT)");
        exec("INSERT INTO t1 VALUES (1, 'Alice')");
        exec("PREPARE allrows AS SELECT * FROM t1");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("EXECUTE allrows")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("Alice", rs.getString(2));
        }
    }

    @Test
    void testPrepareSelectCount() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("PREPARE cnt AS SELECT COUNT(*) FROM t1");
        assertEquals(3, queryInt("EXECUTE cnt"));
    }

    @Test
    void testCursorFetchNextBeyondEnd() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1");
        assertEquals(1, queryInt("FETCH NEXT FROM c1"));
        // Beyond end should return empty
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH NEXT FROM c1")) {
            assertFalse(rs.next());
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testCursorWithJoin() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, name TEXT)");
        exec("CREATE TABLE t2 (id INTEGER, tid INTEGER)");
        exec("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')");
        exec("INSERT INTO t2 VALUES (1, 1), (2, 2)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT t1.name FROM t1 JOIN t2 ON t1.id = t2.tid ORDER BY t1.id");
        assertEquals("Alice", query1("FETCH NEXT FROM c1"));
        assertEquals("Bob", query1("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testSavepointReuseName() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (1)");
        exec("RELEASE SAVEPOINT sp1");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO t1 VALUES (2)");
        exec("ROLLBACK TO SAVEPOINT sp1");
        exec("COMMIT");
        // Only the first insert should persist (second was rolled back)
        assertEquals(1, queryInt("SELECT COUNT(*) FROM t1"));
    }

    @Test
    void testSelectForShareSkipLocked() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR SHARE SKIP LOCKED")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForKeyShareNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR KEY SHARE NOWAIT")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testSelectForNoKeyUpdateSkipLocked() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1)");
        exec("BEGIN");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM t1 FOR NO KEY UPDATE SKIP LOCKED")) {
            assertTrue(rs.next());
        }
        exec("COMMIT");
    }

    @Test
    void testMoveFirst() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 SCROLL CURSOR FOR SELECT id FROM t1 ORDER BY id");
        exec("FETCH LAST FROM c1");
        exec("MOVE FIRST IN c1");
        assertEquals(2, queryInt("FETCH NEXT FROM c1"));
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testMoveLast() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("INSERT INTO t1 VALUES (1), (2), (3)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT id FROM t1 ORDER BY id");
        exec("MOVE LAST IN c1");
        // After MOVE LAST, position is at last row; FETCH NEXT goes beyond
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH NEXT FROM c1")) {
            assertFalse(rs.next());
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testCursorWithAggregation() throws SQLException {
        exec("CREATE TABLE t1 (dept TEXT, salary INTEGER)");
        exec("INSERT INTO t1 VALUES ('A', 100), ('A', 200), ('B', 300)");
        exec("BEGIN");
        exec("DECLARE c1 CURSOR FOR SELECT dept, SUM(salary) FROM t1 GROUP BY dept ORDER BY dept");
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH NEXT FROM c1")) {
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
            assertEquals(300, rs.getInt(2));
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("FETCH NEXT FROM c1")) {
            assertTrue(rs.next());
            assertEquals("B", rs.getString(1));
            assertEquals(300, rs.getInt(2));
        }
        exec("CLOSE c1");
        exec("COMMIT");
    }

    @Test
    void testPrepareWithBoolParam() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER, active BOOLEAN)");
        exec("INSERT INTO t1 VALUES (1, TRUE), (2, FALSE), (3, TRUE)");
        exec("PREPARE findactive (boolean) AS SELECT COUNT(*) FROM t1 WHERE active = $1");
        assertEquals(2, queryInt("EXECUTE findactive(TRUE)"));
        assertEquals(1, queryInt("EXECUTE findactive(FALSE)"));
    }

    @Test
    void testLockTableRowExclusiveNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN ROW EXCLUSIVE MODE NOWAIT");
        exec("COMMIT");
    }

    @Test
    void testLockTableShareNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN SHARE MODE NOWAIT");
        exec("COMMIT");
    }

    @Test
    void testLockTableShareRowExclusiveNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN SHARE ROW EXCLUSIVE MODE NOWAIT");
        exec("COMMIT");
    }

    @Test
    void testLockTableShareUpdateExclusiveNowait() throws SQLException {
        exec("CREATE TABLE t1 (id INTEGER)");
        exec("BEGIN");
        exec("LOCK TABLE t1 IN SHARE UPDATE EXCLUSIVE MODE NOWAIT");
        exec("COMMIT");
    }
}
