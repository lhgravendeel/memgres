package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A deadlock ends the refused session's transaction there and then, and it refuses the session that
 * began waiting first. Both of those were wrong here. The victim's writes stayed in the tables until
 * its client ended the block, so the winner's UPDATE went on waiting for rows nobody was entitled to
 * any more, and when the victim finally rolled back its undo put back rows the winner had committed
 * over: PostgreSQL 18 finishes the crosswise-update script with (1,21),(2,20) and this engine
 * finished it with (1,10),(2,11). Victim selection went the other way round too — the session that
 * closed the cycle was refused rather than the one that opened it.
 *
 * <p>The other half is a row-locking read that has to wait. SELECT ... FOR UPDATE naming a child
 * partition never saw the mover holding that row, because an UPDATE through the partitioned parent
 * registers its row lock under the parent's name: the read decided its answer against storage a
 * transaction still in flight was about to change, so a SERIALIZABLE reader was refused before the
 * mover had done anything and a READ COMMITTED reader silently returned no rows whichever way the
 * mover ended.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down. Nothing asserted
 * here concerns advisory-lock or table-lock cycles, which still refuse the session that closes them.
 *
 * <p>These tests are unattended-safe by construction. No statement runs on the test's own thread:
 * every one is submitted to a daemon pool and joined with a deadline, so a wait that never ends
 * fails the test instead of stopping the suite. Waiting is asserted by watching a future for a
 * bounded moment, never by waiting for one to finish. Every session sets lock_timeout, so the
 * server gives up on any lock this file can ask for and reports 55P03 rather than holding a worker.
 * And the timeout below runs each test on a thread of its own, which is interrupted if the test
 * somehow outlasts it: the test thread only ever waits in {@link Future#get(long, TimeUnit)}, which
 * an interrupt ends.
 */
@Timeout(value = 240, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DeadlockVictimAndBlockingReadTest {

    /** A statement with nothing to wait for must finish inside this, or the test fails. */
    private static final long STEP_MS = 20_000;
    /** A statement whose wait has been ended by another session must finish inside this. */
    private static final long WAIT_MS = 25_000;
    /** The winner of a deadlock must be released by the refusal itself, not by a later COMMIT. */
    private static final long RELEASED_MS = 10_000;
    /** How long a statement is watched before it counts as still waiting. */
    private static final long WAITING_MS = 700;

    private static final String ABORTED = "ERR[25P02] ERROR: current transaction is aborted,"
            + " commands ignored until end of transaction block";

    static Memgres memgres;
    static ExecutorService pool;

    private final List<Connection> opened = new ArrayList<>();

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "deadlock-victim-test");
            t.setDaemon(true);
            return t;
        });
    }

    @AfterAll
    static void tearDown() throws Exception {
        // Shutting the server down is bounded as well: a session left waiting by a regression
        // must not turn into a suite that never finishes.
        if (memgres != null) {
            Thread closing = new Thread(() -> {
                try {
                    memgres.close();
                } catch (Exception ignored) {
                    // nothing left to do about it here
                }
            }, "deadlock-victim-test-close");
            closing.setDaemon(true);
            closing.start();
            closing.join(30_000);
        }
        if (pool != null) pool.shutdownNow();
    }

    @AfterEach
    void closeConnections() {
        for (Connection c : opened) {
            // Closing is itself done off the test thread: a connection whose statement is still
            // waiting would block its own close, and that must not reach the suite.
            Future<?> closing = pool.submit(() -> {
                c.close();
                return null;
            });
            try {
                closing.get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
                closing.cancel(true);
            }
        }
        opened.clear();
    }

    private Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        opened.add(c);
        try (Statement st = c.createStatement()) {
            st.execute("SET lock_timeout = '20s'");
        }
        return c;
    }

    /**
     * One line for whatever the statement answered: the rows it returned, the number of rows it
     * reported, or its SQLSTATE and the first line of its message.
     */
    private static String exec(Connection c, String sql) {
        try (Statement st = c.createStatement()) {
            if (st.execute(sql)) {
                try (ResultSet rs = st.getResultSet()) {
                    int cols = rs.getMetaData().getColumnCount();
                    StringBuilder out = new StringBuilder();
                    while (rs.next()) {
                        if (out.length() > 0) out.append(';');
                        for (int i = 1; i <= cols; i++) {
                            if (i > 1) out.append('|');
                            out.append(rs.getString(i));
                        }
                    }
                    return out.length() == 0 ? "(no rows)" : out.toString();
                }
            }
            return "count " + st.getUpdateCount();
        } catch (SQLException e) {
            String m = e.getMessage() == null ? "" : e.getMessage();
            int nl = m.indexOf('\n');
            return "ERR[" + e.getSQLState() + "] " + (nl < 0 ? m : m.substring(0, nl));
        }
    }

    /** Starts a statement that is expected to wait, and hands back the wait itself. */
    private static Future<String> start(Connection c, String sql) {
        return pool.submit(() -> exec(c, sql));
    }

    /** Runs a statement that has nothing to wait for, failing rather than waiting if it does. */
    private static String run(Connection c, String sql) {
        return join(start(c, sql), STEP_MS, sql);
    }

    private static String join(Future<String> f, long millis, String what) {
        try {
            return f.get(millis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            f.cancel(true);
            return fail("still waiting after " + millis + "ms: " + what);
        } catch (Exception e) {
            return fail("could not run: " + what, e);
        }
    }

    private static void assertStillWaiting(Future<String> f, String what) {
        try {
            String answered = f.get(WAITING_MS, TimeUnit.MILLISECONDS);
            fail(what + " should still have been waiting, but it answered: " + answered);
        } catch (TimeoutException expected) {
            // still waiting, which is what PostgreSQL does here
        } catch (Exception e) {
            fail("could not watch: " + what, e);
        }
    }

    /** The two crosswise UPDATEs of a row deadlock, in the order they entered their waits. */
    private static final class Deadlock {
        final Future<String> firstWaiter;
        final Future<String> secondWaiter;

        Deadlock(Future<String> firstWaiter, Future<String> secondWaiter) {
            this.firstWaiter = firstWaiter;
            this.secondWaiter = secondWaiter;
        }
    }

    /**
     * Session {@code a} holds row 1 and session {@code b} holds row 2, and then each asks for the
     * other's row. The session named by {@code aWaitsFirst} is watched into its wait before the
     * other one closes the cycle, so which of the two waits is the older one is not a race.
     */
    private Deadlock stageRowDeadlock(String table, Connection a, Connection b, boolean aWaitsFirst) {
        run(a, "DROP TABLE IF EXISTS " + table);
        run(a, "CREATE TABLE " + table + " (id int PRIMARY KEY, v int)");
        run(a, "INSERT INTO " + table + " VALUES (1,1),(2,2)");
        run(a, "BEGIN");
        run(b, "BEGIN");
        assertEquals("count 1", run(a, "UPDATE " + table + " SET v=10 WHERE id=1"));
        assertEquals("count 1", run(b, "UPDATE " + table + " SET v=20 WHERE id=2"));
        String aAsks = "UPDATE " + table + " SET v=11 WHERE id=2";
        String bAsks = "UPDATE " + table + " SET v=21 WHERE id=1";
        Future<String> first = start(aWaitsFirst ? a : b, aWaitsFirst ? aAsks : bAsks);
        // Which wait is the older one has to be settled, so the cycle is only closed once this
        // one is demonstrably waiting.
        assertStillWaiting(first, "the first waiter's UPDATE, before the cycle was closed");
        Future<String> second = start(aWaitsFirst ? b : a, aWaitsFirst ? bAsks : aAsks);
        return new Deadlock(first, second);
    }

    private static void assertDeadlock(String answer) {
        assertTrue(answer.startsWith("ERR[40P01]"), "expected a deadlock report, got " + answer);
        assertTrue(answer.contains("deadlock detected"), answer);
    }

    private void createPartitioned(Connection c, String table) {
        run(c, "DROP TABLE IF EXISTS " + table);
        run(c, "CREATE TABLE " + table + " (id int, v int) PARTITION BY RANGE (id)");
        run(c, "CREATE TABLE " + table + "1 PARTITION OF " + table
                + " FOR VALUES FROM (0) TO (100)");
        run(c, "CREATE TABLE " + table + "2 PARTITION OF " + table
                + " FOR VALUES FROM (100) TO (200)");
        run(c, "INSERT INTO " + table + " VALUES (1,1),(150,2)");
    }

    // ---- the victim's transaction ----

    @Test
    void victimOfARowDeadlockAnswers25P02UntilItEndsItsTransactionBlock() throws Exception {
        Connection a = open();
        Connection b = open();
        Deadlock d = stageRowDeadlock("zzt4g_dl1", a, b, true);

        assertDeadlock(join(d.firstWaiter, WAIT_MS, "the refused UPDATE"));
        assertEquals("count 1", join(d.secondWaiter, WAIT_MS, "the other session's UPDATE"));

        // The transaction is gone but the block is not: nothing is accepted inside it any more.
        assertEquals(ABORTED, run(a, "SELECT 1"));
        assertEquals(ABORTED, run(a, "SELECT v FROM zzt4g_dl1 ORDER BY id"));
        assertEquals(ABORTED, run(a, "INSERT INTO zzt4g_dl1 VALUES (3,3)"));
        assertEquals(ABORTED, run(a, "SAVEPOINT sp_after"));

        // COMMIT of a transaction that is already gone is accepted without a word.
        assertEquals("count 0", run(a, "COMMIT"));
        assertEquals("1", run(a, "SELECT 1"));

        run(b, "COMMIT");
        run(a, "DROP TABLE zzt4g_dl1");
    }

    @Test
    void aRolledBackVictimIsUsableAgainAndKeptNothingItHadWritten() throws Exception {
        Connection a = open();
        Connection b = open();
        Deadlock d = stageRowDeadlock("zzt4g_dl2", a, b, true);

        assertDeadlock(join(d.firstWaiter, WAIT_MS, "the refused UPDATE"));
        assertEquals("count 1", join(d.secondWaiter, WAIT_MS, "the other session's UPDATE"));
        assertEquals(ABORTED, run(a, "SELECT 1"));

        assertEquals("count 0", run(a, "ROLLBACK"));
        assertEquals("1", run(a, "SELECT 1"));
        run(b, "COMMIT");
        // The victim's v=10 was undone when it was refused, not when its client said so, so
        // nothing of it is left over the row the winner committed.
        assertEquals("1|21;2|20", run(a, "SELECT id, v FROM zzt4g_dl2 ORDER BY id"));
        run(a, "DROP TABLE zzt4g_dl2");
    }

    @Test
    void theWinnersBlockedUpdateFinishesAsSoonAsTheVictimIsRefused() throws Exception {
        Connection a = open();
        Connection b = open();
        Deadlock d = stageRowDeadlock("zzt4g_dl3", a, b, true);

        assertDeadlock(join(d.firstWaiter, WAIT_MS, "the refused UPDATE"));
        // The refusal itself releases the victim's rows: the winner must not have to wait for the
        // victim's client to end the block, which is what it used to have to do.
        assertEquals("count 1", join(d.secondWaiter, RELEASED_MS,
                "the winner's UPDATE of the row the refused session had held"));
        // and the row it now holds carries its own value, not the value the victim wrote
        assertEquals("21;20", run(b, "SELECT v FROM zzt4g_dl3 ORDER BY id"));
        // all of which is true while the victim is still sitting inside its failed block
        assertEquals(ABORTED, run(a, "SELECT 1"));

        run(b, "COMMIT");
        run(a, "ROLLBACK");
        run(a, "DROP TABLE zzt4g_dl3");
    }

    @Test
    void aVictimsRollbackDoesNotResurrectRowsTheWinnerCommittedOver() throws Exception {
        Connection a = open();
        Connection b = open();
        Connection c = open();
        Deadlock d = stageRowDeadlock("zzt4g_dl4", a, b, true);

        assertDeadlock(join(d.firstWaiter, WAIT_MS, "the refused UPDATE"));
        assertEquals("count 1", join(d.secondWaiter, WAIT_MS, "the winner's UPDATE"));
        run(b, "COMMIT");

        // A third session reads the winner's rows while the victim is still inside its block.
        assertEquals("1|21;2|20", run(c, "SELECT id, v FROM zzt4g_dl4 ORDER BY id"));
        assertEquals("count 0", run(a, "ROLLBACK"));
        // The victim has no undo left to apply, so the committed rows stand.
        assertEquals("1|21;2|20", run(c, "SELECT id, v FROM zzt4g_dl4 ORDER BY id"));

        run(a, "DROP TABLE zzt4g_dl4");
    }

    // ---- which of the two is refused ----

    @Test
    void theSessionThatBeganWaitingFirstIsTheOneRefused() throws Exception {
        Connection a = open();
        Connection b = open();
        Deadlock d = stageRowDeadlock("zzt4g_dl5", a, b, true);

        assertDeadlock(join(d.firstWaiter, WAIT_MS, "the first waiter's UPDATE"));
        assertEquals("count 1", join(d.secondWaiter, WAIT_MS, "the second waiter's UPDATE"));
        // the session that closed the cycle keeps a transaction it can go on using
        assertEquals("21;20", run(b, "SELECT v FROM zzt4g_dl5 ORDER BY id"));

        run(b, "COMMIT");
        run(a, "ROLLBACK");
        assertEquals("1|21;2|20", run(a, "SELECT id, v FROM zzt4g_dl5 ORDER BY id"));
        run(a, "DROP TABLE zzt4g_dl5");
    }

    @Test
    void swappingWhichSessionWaitsFirstSwapsWhichOneIsRefused() throws Exception {
        Connection a = open();
        Connection b = open();
        // Same cycle, other order: b enters its wait first, so b is the one refused.
        Deadlock d = stageRowDeadlock("zzt4g_dl6", a, b, false);

        assertDeadlock(join(d.firstWaiter, WAIT_MS, "the first waiter's UPDATE"));
        assertEquals("count 1", join(d.secondWaiter, WAIT_MS, "the second waiter's UPDATE"));
        assertEquals("1", run(a, "SELECT 1"));

        run(a, "COMMIT");
        run(b, "COMMIT");
        // this time it is a's writes that survive, both of them
        assertEquals("1|10;2|11", run(a, "SELECT id, v FROM zzt4g_dl6 ORDER BY id"));
        run(a, "DROP TABLE zzt4g_dl6");
    }

    @Test
    void aVictimHoldingASavepointKeepsItsRowLocksAndRecoversWithRollbackToSavepoint()
            throws Exception {
        Connection a = open();
        Connection b = open();
        run(a, "DROP TABLE IF EXISTS zzt4g_sp");
        run(a, "CREATE TABLE zzt4g_sp (id int PRIMARY KEY, v int)");
        run(a, "INSERT INTO zzt4g_sp VALUES (1,1),(2,2)");
        run(a, "BEGIN");
        run(b, "BEGIN");
        assertEquals("count 1", run(a, "UPDATE zzt4g_sp SET v=10 WHERE id=1"));
        run(a, "SAVEPOINT sp1");
        assertEquals("count 1", run(b, "UPDATE zzt4g_sp SET v=20 WHERE id=2"));
        Future<String> victim = start(a, "UPDATE zzt4g_sp SET v=11 WHERE id=2");
        assertStillWaiting(victim, "the first waiter's UPDATE, before the cycle was closed");
        Future<String> winner = start(b, "UPDATE zzt4g_sp SET v=21 WHERE id=1");

        assertDeadlock(join(victim, WAIT_MS, "the refused UPDATE"));
        // With a savepoint open only the subtransaction goes, so the victim keeps row 1 and the
        // other session goes on waiting for it.
        assertStillWaiting(winner, "the other session's UPDATE of the row the victim holds");
        assertEquals(ABORTED, run(a, "SELECT 7"));

        run(a, "ROLLBACK TO SAVEPOINT sp1");
        assertEquals("7", run(a, "SELECT 7"));
        // the write made before the savepoint is still there
        assertEquals("1|10;2|2", run(a, "SELECT id, v FROM zzt4g_sp ORDER BY id"));
        assertStillWaiting(winner, "the other session's UPDATE, with the victim's block still open");

        run(a, "COMMIT");
        assertEquals("count 1", join(winner, WAIT_MS,
                "the other session's UPDATE once the victim ended its block"));
        run(b, "COMMIT");
        assertEquals("1|21;2|20", run(a, "SELECT id, v FROM zzt4g_sp ORDER BY id"));
        run(a, "DROP TABLE zzt4g_sp");
    }

    // ---- a row-locking read that has to wait ----

    @Test
    void aSerializableChildPartitionReadWaitsForTheMoverAndIsThenRefused() throws Exception {
        Connection a = open();
        Connection b = open();
        createPartitioned(a, "zzt4g_x1");
        run(a, "BEGIN ISOLATION LEVEL SERIALIZABLE");
        run(b, "BEGIN");
        assertEquals("1", run(a, "SELECT v FROM zzt4g_x11"));
        assertEquals("count 1", run(b, "UPDATE zzt4g_x1 SET id=101 WHERE id=1"));

        Future<String> read = start(a, "SELECT id, v FROM zzt4g_x11 FOR UPDATE");
        // The mover has the row out of this partition but has not committed, so there is nothing
        // to decide yet — the read has to wait rather than answer from what it can see.
        assertStillWaiting(read, "the row-locking read of the partition the mover emptied");

        run(b, "COMMIT");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                join(read, WAIT_MS, "the row-locking read after the mover committed"));

        run(a, "ROLLBACK");
        assertEquals("101|1;150|2", run(b, "SELECT id, v FROM zzt4g_x1 ORDER BY id"));
        run(a, "DROP TABLE zzt4g_x1");
    }

    @Test
    void aReadCommittedChildPartitionReadReportsTheTupleMovedToAnotherPartition() throws Exception {
        Connection a = open();
        Connection b = open();
        createPartitioned(a, "zzt4g_x2");
        run(a, "BEGIN");
        run(b, "BEGIN");
        assertEquals("1", run(a, "SELECT v FROM zzt4g_x21"));
        assertEquals("count 1", run(b, "UPDATE zzt4g_x2 SET id=101 WHERE id=1"));

        Future<String> read = start(a, "SELECT id, v FROM zzt4g_x21 FOR UPDATE");
        assertStillWaiting(read, "the row-locking read of the partition the mover emptied");

        run(b, "COMMIT");
        assertEquals("ERR[40001] ERROR: tuple to be locked was already moved to another partition"
                        + " due to concurrent update",
                join(read, WAIT_MS, "the row-locking read after the mover committed"));

        run(a, "ROLLBACK");
        run(a, "DROP TABLE zzt4g_x2");
    }

    @Test
    void aChildPartitionReadReturnsTheRowWhenTheMoverRollsBack() throws Exception {
        Connection a = open();
        Connection b = open();
        createPartitioned(a, "zzt4g_x3");
        run(a, "BEGIN");
        run(b, "BEGIN");
        assertEquals("1", run(a, "SELECT v FROM zzt4g_x31"));
        assertEquals("count 1", run(b, "UPDATE zzt4g_x3 SET id=101 WHERE id=1"));

        Future<String> read = start(a, "SELECT id, v FROM zzt4g_x31 FOR UPDATE");
        assertStillWaiting(read, "the row-locking read of the partition the mover emptied");

        run(b, "ROLLBACK");
        // The move never happened, so the row is the reader's to lock and to return.
        assertEquals("1|1", join(read, WAIT_MS, "the row-locking read after the mover rolled back"));

        run(a, "ROLLBACK");
        assertEquals("1|1;150|2", run(a, "SELECT id, v FROM zzt4g_x3 ORDER BY id"));
        run(a, "DROP TABLE zzt4g_x3");
    }

    @Test
    void theSameReadThroughThePartitionedParentAlsoWaitsForTheMover() throws Exception {
        Connection a = open();
        Connection b = open();
        createPartitioned(a, "zzt4g_x4");
        run(a, "BEGIN");
        run(b, "BEGIN");
        assertEquals("count 1", run(b, "UPDATE zzt4g_x4 SET id=101 WHERE id=1"));

        Future<String> read = start(a, "SELECT id, v FROM zzt4g_x4 WHERE id=1 FOR UPDATE");
        assertStillWaiting(read, "the row-locking read through the parent");

        run(b, "ROLLBACK");
        assertEquals("1|1", join(read, WAIT_MS, "the row-locking read after the mover rolled back"));

        run(a, "ROLLBACK");
        run(a, "DROP TABLE zzt4g_x4");
    }

    @Test
    void aChildPartitionReadWithNoWriterInFlightDoesNotWait() throws Exception {
        Connection a = open();
        createPartitioned(a, "zzt4g_w5");
        run(a, "BEGIN");

        // Nothing is in flight, so waiting for a writer must find none and the read must answer.
        long started = System.currentTimeMillis();
        assertEquals("1|1", join(start(a, "SELECT id, v FROM zzt4g_w51 FOR UPDATE"), STEP_MS,
                "a row-locking read with nothing in its way"));
        long tookMs = System.currentTimeMillis() - started;
        assertTrue(tookMs < 2000, "a read with no writer in flight should not wait, took " + tookMs + "ms");

        run(a, "ROLLBACK");
        run(a, "DROP TABLE zzt4g_w5");
    }

    @Test
    void aRowLockingReadWaitsForAnUncommittedUpdateAndThenSeesItsCommittedValue() throws Exception {
        Connection a = open();
        Connection b = open();
        run(a, "DROP TABLE IF EXISTS zzt4g_w1");
        run(a, "CREATE TABLE zzt4g_w1 (id int PRIMARY KEY, v int)");
        run(a, "INSERT INTO zzt4g_w1 VALUES (1,1),(2,2)");
        run(a, "BEGIN");
        run(b, "BEGIN");
        assertEquals("count 1", run(b, "UPDATE zzt4g_w1 SET v=99 WHERE id=1"));

        Future<String> read = start(a, "SELECT id, v FROM zzt4g_w1 WHERE id=1 FOR UPDATE");
        assertStillWaiting(read, "the row-locking read of a row another session is writing");

        run(b, "COMMIT");
        assertEquals("1|99", join(read, WAIT_MS, "the row-locking read after the writer committed"));

        run(a, "ROLLBACK");
        run(a, "DROP TABLE zzt4g_w1");
    }

    @Test
    void aRowLockingReadThatWaitedOnARolledBackUpdateSeesTheRowUnchanged() throws Exception {
        Connection a = open();
        Connection b = open();
        run(a, "DROP TABLE IF EXISTS zzt4g_w2");
        run(a, "CREATE TABLE zzt4g_w2 (id int PRIMARY KEY, v int)");
        run(a, "INSERT INTO zzt4g_w2 VALUES (1,1),(2,2)");
        run(a, "BEGIN");
        run(b, "BEGIN");
        assertEquals("count 1", run(b, "UPDATE zzt4g_w2 SET v=77 WHERE id=1"));

        Future<String> read = start(a, "SELECT id, v FROM zzt4g_w2 WHERE id=1 FOR UPDATE");
        assertStillWaiting(read, "the row-locking read of a row another session is writing");

        run(b, "ROLLBACK");
        assertEquals("1|1", join(read, WAIT_MS, "the row-locking read after the writer rolled back"));

        run(a, "ROLLBACK");
        run(a, "DROP TABLE zzt4g_w2");
    }

    @Test
    void aRowLockingReadWaitingOnAnUncommittedDeleteFindsNothingOnceItCommits() throws Exception {
        Connection a = open();
        Connection b = open();
        run(a, "DROP TABLE IF EXISTS zzt4g_w3");
        run(a, "CREATE TABLE zzt4g_w3 (id int PRIMARY KEY, v int)");
        run(a, "INSERT INTO zzt4g_w3 VALUES (1,1),(2,2)");
        run(a, "BEGIN");
        run(b, "BEGIN");
        assertEquals("count 1", run(b, "DELETE FROM zzt4g_w3 WHERE id=2"));

        Future<String> read = start(a, "SELECT id, v FROM zzt4g_w3 WHERE id=2 FOR UPDATE");
        assertStillWaiting(read, "the row-locking read of a row another session is deleting");

        run(b, "COMMIT");
        assertEquals("(no rows)", join(read, WAIT_MS,
                "the row-locking read after the delete committed"));

        run(a, "ROLLBACK");
        run(a, "DROP TABLE zzt4g_w3");
    }

    @Test
    void aSerializableRowLockingReadIsRefusedAfterTheWriterCommits() throws Exception {
        Connection a = open();
        Connection b = open();
        run(a, "DROP TABLE IF EXISTS zzt4g_w4");
        run(a, "CREATE TABLE zzt4g_w4 (id int PRIMARY KEY, v int)");
        run(a, "INSERT INTO zzt4g_w4 VALUES (1,1),(2,2)");
        run(a, "BEGIN ISOLATION LEVEL SERIALIZABLE");
        run(b, "BEGIN");
        assertEquals("1", run(a, "SELECT v FROM zzt4g_w4 WHERE id=1"));
        assertEquals("count 1", run(b, "UPDATE zzt4g_w4 SET v=55 WHERE id=1"));

        Future<String> read = start(a, "SELECT id, v FROM zzt4g_w4 WHERE id=1 FOR UPDATE");
        assertStillWaiting(read, "the row-locking read of a row another session is writing");

        run(b, "COMMIT");
        assertEquals("ERR[40001] ERROR: could not serialize access due to concurrent update",
                join(read, WAIT_MS, "the row-locking read after the writer committed"));

        run(a, "ROLLBACK");
        run(a, "DROP TABLE zzt4g_w4");
    }

    @Test
    void aWaitingRowLockingReadGivesUpUnderLockTimeout() throws Exception {
        Connection a = open();
        Connection b = open();
        run(a, "SET lock_timeout = '1s'");
        createPartitioned(a, "zzt4g_l2");
        run(a, "BEGIN");
        run(b, "BEGIN");
        assertEquals("count 1", run(b, "UPDATE zzt4g_l2 SET id=101 WHERE id=1"));

        // The wait the fix introduced is a wait like any other: lock_timeout ends it.
        assertEquals("ERR[55P03] ERROR: canceling statement due to lock timeout",
                join(start(a, "SELECT id, v FROM zzt4g_l21 FOR UPDATE"), WAIT_MS,
                        "the row-locking read under lock_timeout"));

        run(a, "ROLLBACK");
        run(b, "ROLLBACK");
        run(a, "DROP TABLE zzt4g_l2");
    }
}
