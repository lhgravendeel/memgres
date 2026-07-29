package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A second pass over sessions, transactions, cursors and locks, measured against PostgreSQL 18.
 *
 * <p>Three of these are about a statement that must come back at all. A transaction that has
 * already failed can never make its writes permanent, so PostgreSQL treats them as dead the
 * instant the statement errored: another session neither waits for them nor sees their keys. Two
 * sessions that each hold what the other wants are told so rather than left waiting. Every wait
 * here has a client timeout on it, so a test that hangs fails instead of stopping the suite.
 *
 * <p>The rest come in pairs: a rule, and the ordinary shapes around it that PostgreSQL accepts. A
 * rule that fires one shape too wide refuses valid SQL, which costs more than the looseness it
 * removed, so the acceptance assertions outnumber the refusals on purpose.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class TransactionControlRound2Test {

    static Memgres memgres;

    @BeforeAll
    static void setUp() {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() {
        if (memgres != null) memgres.close();
    }

    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.setQueryTimeout(20);
            st.execute(sql);
        }
    }

    /** Run a statement, returning the rows it changed; fails the test if it does not return. */
    private static int update(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
            return st.getUpdateCount();
        }
    }

    private static String scalar(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.setQueryTimeout(20);
            try (ResultSet rs = st.executeQuery(sql)) {
                assertTrue(rs.next(), "expected a row from: " + sql);
                return rs.getString(1);
            }
        }
    }

    private static List<String> column(Connection c, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement()) {
            st.setQueryTimeout(20);
            try (ResultSet rs = st.executeQuery(sql)) {
                while (rs.next()) out.add(rs.getString(1));
            }
        }
        return out;
    }

    private static String state(Connection c, String sql) {
        try {
            exec(c, sql);
            return "no error";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String message(Connection c, String sql) {
        try {
            exec(c, sql);
            return "no error";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    private static void accepted(Connection c, String sql) {
        assertDoesNotThrow(() -> exec(c, sql), "PostgreSQL accepts this: " + sql);
    }

    /** Put a session into the aborted state PostgreSQL reaches when a statement fails. */
    private static void abortTransaction(Connection c) throws SQLException {
        exec(c, "BEGIN");
        assertEquals("22012", state(c, "SELECT 1/0"));
    }

    // ---- 1. A failed transaction's writes are dead, not pending ----

    @Test
    void anUpdateDoesNotWaitForATransactionThatHasAlreadyFailed() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tcr_dead_upd");
            exec(a, "CREATE TABLE tcr_dead_upd (id int primary key, n int)");
            exec(a, "INSERT INTO tcr_dead_upd VALUES (1, 1)");
            exec(a, "BEGIN");
            exec(a, "UPDATE tcr_dead_upd SET n = 2 WHERE id = 1");
            assertEquals("22012", state(a, "SELECT 1/0"));
            // PG answers in milliseconds; the 10s query timeout turns a wait into a failure.
            assertEquals(1, update(b, "UPDATE tcr_dead_upd SET n = 3 WHERE id = 1"));
            exec(a, "ROLLBACK");
            exec(a, "DROP TABLE tcr_dead_upd");
        }
    }

    @Test
    void anInsertIsNotRefusedByAKeyOnlyAFailedTransactionHolds() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tcr_dead_ins");
            exec(a, "CREATE TABLE tcr_dead_ins (id int primary key, n int)");
            exec(a, "BEGIN");
            exec(a, "INSERT INTO tcr_dead_ins VALUES (9, 1)");
            assertEquals("22012", state(a, "SELECT 1/0"));
            assertEquals(1, update(b, "INSERT INTO tcr_dead_ins VALUES (9, 2)"));
            exec(a, "ROLLBACK");
            assertEquals("9|2", scalar(b, "SELECT id || '|' || n FROM tcr_dead_ins ORDER BY id"));
            exec(a, "DROP TABLE tcr_dead_ins");
        }
    }

    @Test
    void aDeleteDoesNotWaitForATransactionThatHasAlreadyFailed() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tcr_dead_del");
            exec(a, "CREATE TABLE tcr_dead_del (id int primary key, n int)");
            exec(a, "INSERT INTO tcr_dead_del VALUES (1, 1)");
            exec(a, "BEGIN");
            exec(a, "UPDATE tcr_dead_del SET n = 5 WHERE id = 1");
            assertEquals("22012", state(a, "SELECT 1/0"));
            assertEquals(1, update(b, "DELETE FROM tcr_dead_del WHERE id = 1"));
            exec(a, "ROLLBACK");
            exec(a, "DROP TABLE tcr_dead_del");
        }
    }

    /**
     * A savepoint is the one way back from a failed statement, so a transaction holding one may
     * still commit the work it did before the savepoint. It is waited for like any live one.
     */
    @Test
    void aFailedSubtransactionInsideASavepointIsStillWaitedFor() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tcr_savepoint_live");
            exec(a, "CREATE TABLE tcr_savepoint_live (id int primary key, n int)");
            exec(a, "BEGIN");
            exec(a, "SAVEPOINT s1");
            exec(a, "INSERT INTO tcr_savepoint_live VALUES (1, 1)");
            exec(a, "RELEASE SAVEPOINT s1");
            exec(a, "SAVEPOINT s2");
            assertEquals("22012", state(a, "SELECT 1/0"));
            // B has to wait on A's row, which A can still commit by winding back to s2. The wait
            // is interruptible, so the client's own timeout is what ends it.
            exec(b, "SET lock_timeout = '1s'");
            long began = System.currentTimeMillis();
            assertEquals("55P03", state(b, "INSERT INTO tcr_savepoint_live VALUES (1, 2)"));
            assertTrue(System.currentTimeMillis() - began >= 900, "the insert must actually have waited");
            exec(b, "RESET lock_timeout");
            exec(a, "ROLLBACK");
            exec(a, "DROP TABLE tcr_savepoint_live");
        }
    }

    /** A live transaction is still waited for, and its rollback frees the key. */
    @Test
    void aLiveTransactionIsStillWaitedForAndItsRollbackFreesTheKey() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tcr_live_key");
            exec(a, "CREATE TABLE tcr_live_key (id int primary key, n int)");
            exec(a, "BEGIN");
            exec(a, "INSERT INTO tcr_live_key VALUES (1, 1)");
            Thread waiter = new Thread(() -> {
                try (Connection w = open()) {
                    exec(w, "INSERT INTO tcr_live_key VALUES (1, 2)");
                } catch (SQLException ignored) {
                    // reported through the row that ends up in the table
                }
            });
            waiter.start();
            Thread.sleep(300);
            assertTrue(waiter.isAlive(), "the second insert must wait while A is still live");
            exec(a, "ROLLBACK");
            waiter.join(20_000);
            assertFalse(waiter.isAlive(), "the wait must end when A rolls back");
            assertEquals("1|2", scalar(b, "SELECT id || '|' || n FROM tcr_live_key"));
            exec(a, "DROP TABLE tcr_live_key");
        }
    }

    // ---- 2. Two sessions waiting for each other are told so ----

    @Test
    void crosswiseTableLocksReportADeadlock() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tcr_dl1");
            exec(a, "DROP TABLE IF EXISTS tcr_dl2");
            exec(a, "CREATE TABLE tcr_dl1 (id int primary key)");
            exec(a, "CREATE TABLE tcr_dl2 (id int primary key)");
            exec(a, "BEGIN");
            exec(b, "BEGIN");
            exec(a, "LOCK TABLE tcr_dl1 IN ACCESS EXCLUSIVE MODE");
            exec(b, "LOCK TABLE tcr_dl2 IN ACCESS EXCLUSIVE MODE");
            final String[] aResult = new String[1];
            Thread aWait = new Thread(() -> aResult[0] = state(a, "LOCK TABLE tcr_dl2 IN ACCESS EXCLUSIVE MODE"));
            aWait.start();
            Thread.sleep(300);
            // B closes the cycle, and PG raises on the waiter that closes it.
            assertEquals("40P01", state(b, "LOCK TABLE tcr_dl1 IN ACCESS EXCLUSIVE MODE"));
            exec(b, "ROLLBACK");
            aWait.join(20_000);
            assertFalse(aWait.isAlive(), "A's wait must end once B has released its lock");
            assertEquals("no error", aResult[0]);
            exec(a, "ROLLBACK");
            exec(a, "DROP TABLE tcr_dl1");
            exec(a, "DROP TABLE tcr_dl2");
        }
    }

    @Test
    void aTableLockThatCannotBeTakenNowNamesTheRelation() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tcr_nowait");
            exec(a, "CREATE TABLE tcr_nowait (id int primary key)");
            exec(a, "BEGIN");
            exec(a, "LOCK TABLE tcr_nowait IN ACCESS EXCLUSIVE MODE");
            exec(b, "BEGIN");
            String msg = message(b, "LOCK TABLE tcr_nowait IN ACCESS EXCLUSIVE MODE NOWAIT");
            assertTrue(msg.contains("could not obtain lock on relation \"tcr_nowait\""), msg);
            exec(b, "ROLLBACK");
            exec(a, "ROLLBACK");
            exec(a, "DROP TABLE tcr_nowait");
        }
    }

    /** Compatible modes do not conflict, and a session never blocks on its own lock. */
    @Test
    void compatibleTableLocksAreTakenWithoutWaiting() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tcr_share");
            exec(a, "CREATE TABLE tcr_share (id int primary key)");
            exec(a, "BEGIN");
            exec(b, "BEGIN");
            accepted(a, "LOCK TABLE tcr_share IN ACCESS SHARE MODE");
            accepted(b, "LOCK TABLE tcr_share IN ACCESS SHARE MODE");
            accepted(a, "LOCK TABLE tcr_share IN ACCESS SHARE MODE");
            // B's ACCESS SHARE does conflict with an ACCESS EXCLUSIVE, so that one is refused.
            assertEquals("55P03", state(a, "LOCK TABLE tcr_share IN ACCESS EXCLUSIVE MODE NOWAIT"));
            exec(a, "ROLLBACK");
            exec(b, "ROLLBACK");
            // with nobody else holding it, the same lock is taken at once
            exec(a, "BEGIN");
            accepted(a, "LOCK TABLE tcr_share IN ACCESS EXCLUSIVE MODE NOWAIT");
            accepted(a, "LOCK TABLE tcr_share IN ACCESS EXCLUSIVE MODE NOWAIT");
            exec(a, "ROLLBACK");
            exec(a, "DROP TABLE tcr_share");
        }
    }

    // ---- 3. Which statements leave the isolation level open ----

    @Test
    void theAsynchronousNotificationCommandsTakeNoSnapshot() throws Exception {
        try (Connection c = open()) {
            for (String stmt : new String[]{"LISTEN tcr_chan", "NOTIFY tcr_chan", "UNLISTEN tcr_chan",
                    "CHECKPOINT", "SET work_mem = '4MB'", "SHOW work_mem", "RESET work_mem"}) {
                exec(c, "BEGIN");
                accepted(c, stmt);
                accepted(c, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                assertEquals("serializable", scalar(c, "SHOW transaction_isolation"), stmt);
                exec(c, "ROLLBACK");
            }
            exec(c, "UNLISTEN *");
        }
    }

    @Test
    void deallocateAndDiscardTakeASnapshotLikeAnyOtherStatement() throws Exception {
        try (Connection c = open()) {
            for (String stmt : new String[]{"DEALLOCATE ALL", "DISCARD PLANS"}) {
                exec(c, "BEGIN");
                accepted(c, stmt);
                assertEquals("25001", state(c, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"), stmt);
                exec(c, "ROLLBACK");
            }
        }
    }

    @Test
    void readingATableStillFixesTheSnapshot() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tcr_snap");
            exec(c, "CREATE TABLE tcr_snap (i int primary key)");
            exec(c, "BEGIN");
            assertEquals("0", scalar(c, "SELECT count(*) FROM tcr_snap"));
            assertEquals("25001", state(c, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
            exec(c, "ROLLBACK");
            // A lock is not a read, so it leaves the choice open.
            exec(c, "BEGIN");
            accepted(c, "LOCK TABLE tcr_snap");
            accepted(c, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tcr_snap");
        }
    }

    // ---- 4. A moved sequence counter invalidates the block a session cached ----

    @Test
    void alterSequenceRestartGivesUpTheCachedBlock() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP SEQUENCE IF EXISTS tcr_seq_restart");
            exec(c, "CREATE SEQUENCE tcr_seq_restart CACHE 3");
            assertEquals("1", scalar(c, "SELECT nextval('tcr_seq_restart')"));
            exec(c, "ALTER SEQUENCE tcr_seq_restart RESTART WITH 100");
            assertEquals("100", scalar(c, "SELECT nextval('tcr_seq_restart')"));
            assertEquals("101", scalar(c, "SELECT nextval('tcr_seq_restart')"));
            exec(c, "DROP SEQUENCE tcr_seq_restart");
        }
    }

    @Test
    void setvalGivesUpTheCachedBlockAndRedefinesCurrval() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP SEQUENCE IF EXISTS tcr_seq_setval");
            exec(c, "CREATE SEQUENCE tcr_seq_setval CACHE 4");
            assertEquals("1", scalar(c, "SELECT nextval('tcr_seq_setval')"));
            assertEquals("50", scalar(c, "SELECT setval('tcr_seq_setval', 50)"));
            assertEquals("51", scalar(c, "SELECT nextval('tcr_seq_setval')"));
            assertEquals("51", scalar(c, "SELECT currval('tcr_seq_setval')"));
            assertEquals("60", scalar(c, "SELECT setval('tcr_seq_setval', 60, false)"));
            assertEquals("60", scalar(c, "SELECT nextval('tcr_seq_setval')"));
            exec(c, "DROP SEQUENCE tcr_seq_setval");
        }
    }

    /** Nothing moved the counter, so the block is still good and still one per session. */
    @Test
    void anUntouchedCacheStillServesItsBlock() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP SEQUENCE IF EXISTS tcr_seq_plain");
            exec(c, "CREATE SEQUENCE tcr_seq_plain CACHE 3");
            assertEquals("1", scalar(c, "SELECT nextval('tcr_seq_plain')"));
            assertEquals("2", scalar(c, "SELECT nextval('tcr_seq_plain')"));
            assertEquals("3", scalar(c, "SELECT nextval('tcr_seq_plain')"));
            assertEquals("4", scalar(c, "SELECT nextval('tcr_seq_plain')"));
            exec(c, "DROP SEQUENCE tcr_seq_plain");
        }
    }

    // ---- 5. A column-level CONSTRAINT clause names the constraint ----

    @Test
    void aColumnLevelConstraintKeepsTheNameItWasGiven() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tcr_named");
            exec(c, "DROP TABLE IF EXISTS tcr_named_parent");
            exec(c, "CREATE TABLE tcr_named_parent (id int primary key)");
            exec(c, "CREATE TABLE tcr_named ("
                    + "id int CONSTRAINT tcr_n_pk PRIMARY KEY,"
                    + " p int CONSTRAINT tcr_n_fk REFERENCES tcr_named_parent(id) DEFERRABLE INITIALLY DEFERRED,"
                    + " q int CONSTRAINT tcr_n_ck CHECK (q > 0),"
                    + " r int CONSTRAINT tcr_n_uq UNIQUE)");
            List<String> names = column(c,
                    "SELECT conname FROM pg_constraint WHERE conrelid = 'tcr_named'::regclass"
                            + " AND contype <> 'n' ORDER BY conname");
            assertEquals("[tcr_n_ck, tcr_n_fk, tcr_n_pk, tcr_n_uq]", names.toString());
            exec(c, "DROP TABLE tcr_named");
            exec(c, "DROP TABLE tcr_named_parent");
        }
    }

    /** Where nothing named them, the generated names are the ones PostgreSQL generates. */
    @Test
    void anUnnamedColumnConstraintStillGetsTheGeneratedName() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tcr_gen");
            exec(c, "DROP TABLE IF EXISTS tcr_gen_parent");
            exec(c, "CREATE TABLE tcr_gen_parent (id int primary key)");
            exec(c, "CREATE TABLE tcr_gen (a int primary key, b int unique,"
                    + " c int references tcr_gen_parent(id), d int check (d > 0))");
            List<String> names = column(c,
                    "SELECT conname FROM pg_constraint WHERE conrelid = 'tcr_gen'::regclass"
                            + " AND contype <> 'n' ORDER BY conname");
            assertEquals("[tcr_gen_b_key, tcr_gen_c_fkey, tcr_gen_d_check, tcr_gen_pkey]", names.toString());
            // and they still enforce what they name
            exec(c, "INSERT INTO tcr_gen VALUES (1, 1, NULL, 1)");
            assertEquals("23505", state(c, "INSERT INTO tcr_gen VALUES (1, 2, NULL, 1)"));
            assertEquals("23505", state(c, "INSERT INTO tcr_gen VALUES (2, 1, NULL, 1)"));
            assertEquals("23514", state(c, "INSERT INTO tcr_gen VALUES (3, 3, NULL, -1)"));
            assertEquals("23503", state(c, "INSERT INTO tcr_gen VALUES (4, 4, 99, 1)"));
            exec(c, "DROP TABLE tcr_gen");
            exec(c, "DROP TABLE tcr_gen_parent");
        }
    }

    @Test
    void setConstraintsFindsAColumnLevelNameAndTakesASchema() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tcr_sc");
            exec(c, "DROP TABLE IF EXISTS tcr_sc_parent");
            exec(c, "CREATE TABLE tcr_sc_parent (id int primary key)");
            exec(c, "CREATE TABLE tcr_sc (id int CONSTRAINT tcr_sc_pk PRIMARY KEY,"
                    + " p int CONSTRAINT tcr_sc_fk REFERENCES tcr_sc_parent(id) DEFERRABLE INITIALLY DEFERRED)");
            exec(c, "BEGIN");
            accepted(c, "SET CONSTRAINTS tcr_sc_fk DEFERRED");
            accepted(c, "SET CONSTRAINTS tcr_sc_fk IMMEDIATE");
            accepted(c, "SET CONSTRAINTS public.tcr_sc_fk DEFERRED");
            accepted(c, "SET CONSTRAINTS public.tcr_sc_fk IMMEDIATE");
            // A key that cannot be deferred says so; a name that is absent is still reported.
            assertEquals("42809", state(c, "SET CONSTRAINTS tcr_sc_pk DEFERRED"));
            exec(c, "ROLLBACK");
            assertEquals("42704", state(c, "SET CONSTRAINTS tcr_no_such DEFERRED"));
            assertEquals("3F000", state(c, "SET CONSTRAINTS tcr_no_schema.tcr_sc_fk DEFERRED"));
            exec(c, "DROP TABLE tcr_sc");
            exec(c, "DROP TABLE tcr_sc_parent");
        }
    }

    @Test
    void aNamedColumnLevelForeignKeyStillDefersToTheEndOfTheTransaction() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tcr_def");
            exec(c, "DROP TABLE IF EXISTS tcr_def_parent");
            exec(c, "CREATE TABLE tcr_def_parent (id int primary key)");
            exec(c, "CREATE TABLE tcr_def (id int primary key,"
                    + " p int CONSTRAINT tcr_def_fk REFERENCES tcr_def_parent(id) DEFERRABLE INITIALLY DEFERRED)");
            exec(c, "BEGIN");
            accepted(c, "INSERT INTO tcr_def VALUES (1, 99)");
            assertEquals("23503", state(c, "SET CONSTRAINTS tcr_def_fk IMMEDIATE"));
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tcr_def");
            exec(c, "DROP TABLE tcr_def_parent");
        }
    }

    // ---- 6. FOR UPDATE: what it may and may not be applied to ----

    @Test
    void forUpdateRefusesADerivedTableOnTheNullableSide() throws Exception {
        try (Connection c = open()) {
            setUpLockTables(c);
            assertEquals("0A000", state(c, "SELECT a.id FROM tcr_l a"
                    + " LEFT JOIN (SELECT * FROM tcr_r) s ON s.l1 = a.id FOR UPDATE"));
            assertEquals("0A000", state(c, "SELECT a.id FROM tcr_l a"
                    + " LEFT JOIN LATERAL (SELECT * FROM tcr_r x WHERE x.l1 = a.id) z ON true FOR UPDATE"));
            assertEquals("0A000", state(c, "SELECT a.id FROM tcr_l a"
                    + " RIGHT JOIN (SELECT * FROM tcr_r) s ON s.l1 = a.id FOR UPDATE"));
            dropLockTables(c);
        }
    }

    @Test
    void forUpdateAcceptsEveryShapeThatStillHasARowBehindIt() throws Exception {
        try (Connection c = open()) {
            setUpLockTables(c);
            accepted(c, "SELECT a.id FROM tcr_l a ORDER BY a.id FOR UPDATE");
            accepted(c, "SELECT s.id FROM (SELECT * FROM tcr_l) s ORDER BY s.id FOR UPDATE");
            accepted(c, "SELECT a.id FROM tcr_l a JOIN (SELECT * FROM tcr_r) s ON s.l1 = a.id FOR UPDATE");
            accepted(c, "SELECT a.id FROM tcr_l a"
                    + " JOIN LATERAL (SELECT * FROM tcr_r x WHERE x.l1 = a.id) z ON true FOR UPDATE");
            accepted(c, "SELECT a.id FROM tcr_l a"
                    + " LEFT JOIN (SELECT * FROM tcr_r) s ON s.l1 = a.id FOR UPDATE OF a");
            accepted(c, "SELECT a.id FROM tcr_l a, tcr_r b WHERE b.l1 = a.id FOR UPDATE");
            // a set-returning function has no row to lock, and a plain FOR UPDATE passes over it
            accepted(c, "SELECT a.id FROM tcr_l a LEFT JOIN generate_series(1, 2) g ON true FOR UPDATE");
            dropLockTables(c);
        }
    }

    @Test
    void forUpdateOfNamesAFromEntryWhichASchemaCannotQualify() throws Exception {
        try (Connection c = open()) {
            setUpLockTables(c);
            String msg = message(c, "SELECT id FROM tcr_l ORDER BY id FOR UPDATE OF public.tcr_l");
            assertTrue(msg.contains("FOR UPDATE must specify unqualified relation names"), msg);
            String share = message(c, "SELECT id FROM tcr_l ORDER BY id FOR SHARE OF public.tcr_l");
            assertTrue(share.contains("FOR SHARE must specify unqualified relation names"), share);
            accepted(c, "SELECT id FROM tcr_l ORDER BY id FOR UPDATE OF tcr_l");
            accepted(c, "SELECT id FROM tcr_l x ORDER BY id FOR UPDATE OF x");
            assertEquals("42P01", state(c, "SELECT id FROM tcr_l ORDER BY id FOR UPDATE OF tcr_nosuch"));
            dropLockTables(c);
        }
    }

    @Test
    void aRowLockIsAWriteAndAReadOnlyTransactionRefusesIt() throws Exception {
        try (Connection c = open()) {
            setUpLockTables(c);
            for (String mode : new String[]{"UPDATE", "SHARE", "KEY SHARE", "NO KEY UPDATE"}) {
                exec(c, "BEGIN READ ONLY");
                String msg = message(c, "SELECT id FROM tcr_l FOR " + mode);
                assertTrue(msg.contains("cannot execute SELECT FOR " + mode
                        + " in a read-only transaction"), msg);
                exec(c, "ROLLBACK");
            }
            // a plain read is still a read, and a read-write transaction still takes the lock
            exec(c, "BEGIN READ ONLY");
            accepted(c, "SELECT id FROM tcr_l ORDER BY id");
            exec(c, "ROLLBACK");
            exec(c, "BEGIN");
            accepted(c, "SELECT id FROM tcr_l ORDER BY id FOR UPDATE");
            exec(c, "COMMIT");
            // the session default reaches an implicit transaction too
            exec(c, "SET default_transaction_read_only = on");
            assertEquals("25006", state(c, "SELECT id FROM tcr_l FOR UPDATE"));
            accepted(c, "SELECT id FROM tcr_l ORDER BY id");
            exec(c, "BEGIN READ WRITE");
            accepted(c, "SELECT id FROM tcr_l ORDER BY id FOR UPDATE");
            exec(c, "COMMIT");
            exec(c, "SET default_transaction_read_only = off");
            dropLockTables(c);
        }
    }

    private static void setUpLockTables(Connection c) throws SQLException {
        exec(c, "DROP TABLE IF EXISTS tcr_l");
        exec(c, "DROP TABLE IF EXISTS tcr_r");
        exec(c, "CREATE TABLE tcr_l (id int primary key, v int)");
        exec(c, "CREATE TABLE tcr_r (id int primary key, l1 int)");
        exec(c, "INSERT INTO tcr_l VALUES (1, 10), (2, 20)");
        exec(c, "INSERT INTO tcr_r VALUES (1, 1)");
    }

    private static void dropLockTables(Connection c) throws SQLException {
        exec(c, "DROP TABLE IF EXISTS tcr_l");
        exec(c, "DROP TABLE IF EXISTS tcr_r");
    }

    // ---- 7. The transaction_mode grammar ----

    @Test
    void aWordThatIsNotATransactionModeIsASyntaxError() throws Exception {
        try (Connection c = open()) {
            assertEquals("42601", state(c, "BEGIN garbage"));
            assertEquals("42601", state(c, "START TRANSACTION garbage"));
            assertEquals("42601", state(c, "SET TRANSACTION"));
            assertEquals("42601", state(c, "SET SESSION CHARACTERISTICS AS TRANSACTION"));
            assertEquals("42601", state(c, "SET SESSION CHARACTERISTICS AS TRANSACTION NONSENSE"));
            assertEquals("42601", state(c, "COMMIT garbage"));
            assertEquals("42601", state(c, "ROLLBACK garbage"));
        }
    }

    @Test
    void aSyntaxErrorQuotesTheWordAsItWasWritten() throws Exception {
        try (Connection c = open()) {
            assertTrue(message(c, "SET TRANSACTION ISOLATION LEVEL NONSENSE")
                    .contains("syntax error at or near \"NONSENSE\""));
            assertTrue(message(c, "BEGIN ISOLATION LEVEL NONSENSE")
                    .contains("syntax error at or near \"NONSENSE\""));
            assertTrue(message(c, "BEGIN ISOLATION LEVEL READ WRITE")
                    .contains("syntax error at or near \"WRITE\""));
            // with nothing left to point at, PG says where it ran out
            assertTrue(message(c, "BEGIN READ").contains("syntax error at end of input"));
            assertTrue(message(c, "BEGIN ISOLATION LEVEL").contains("syntax error at end of input"));
            assertTrue(message(c, "SET TRANSACTION READ").contains("syntax error at end of input"));
        }
    }

    @Test
    void everyRealTransactionModeSpellingStillParses() throws Exception {
        try (Connection c = open()) {
            for (String stmt : new String[]{
                    "BEGIN", "BEGIN WORK", "BEGIN TRANSACTION",
                    "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                    "BEGIN ISOLATION LEVEL REPEATABLE READ, READ WRITE",
                    "BEGIN READ ONLY", "BEGIN READ WRITE, NOT DEFERRABLE", "BEGIN DEFERRABLE",
                    "START TRANSACTION",
                    "START TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY"}) {
                accepted(c, stmt);
                exec(c, "ROLLBACK");
            }
            for (String stmt : new String[]{"COMMIT", "COMMIT WORK", "COMMIT TRANSACTION",
                    "COMMIT AND NO CHAIN", "ROLLBACK", "ROLLBACK WORK", "ROLLBACK TRANSACTION",
                    "ROLLBACK AND NO CHAIN", "ABORT", "END"}) {
                exec(c, "BEGIN");
                accepted(c, stmt);
            }
            exec(c, "BEGIN");
            for (String stmt : new String[]{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                    "SET TRANSACTION READ ONLY", "SET TRANSACTION DEFERRABLE",
                    "SET TRANSACTION NOT DEFERRABLE",
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"}) {
                accepted(c, stmt);
            }
            exec(c, "ROLLBACK");
            accepted(c, "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE");
            accepted(c, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED");
            accepted(c, "SET SESSION CHARACTERISTICS AS TRANSACTION NOT DEFERRABLE");
        }
    }

    // ---- 8. A quoted savepoint name keeps its case ----

    @Test
    void aQuotedSavepointNameIsNotTheSameAsItsFoldedSpelling() throws Exception {
        try (Connection c = open()) {
            exec(c, "BEGIN");
            exec(c, "SAVEPOINT \"TcrSp\"");
            assertEquals("3B001", state(c, "ROLLBACK TO SAVEPOINT tcrsp"));
            exec(c, "ROLLBACK");
            exec(c, "BEGIN");
            exec(c, "SAVEPOINT \"TcrSp\"");
            accepted(c, "ROLLBACK TO SAVEPOINT \"TcrSp\"");
            accepted(c, "RELEASE SAVEPOINT \"TcrSp\"");
            exec(c, "ROLLBACK");
        }
    }

    @Test
    void anUnquotedSavepointNameStillFoldsSoAnySpellingFindsIt() throws Exception {
        try (Connection c = open()) {
            exec(c, "BEGIN");
            exec(c, "SAVEPOINT TcrSp2");
            accepted(c, "ROLLBACK TO SAVEPOINT tcrsp2");
            accepted(c, "ROLLBACK TO SAVEPOINT TCRSP2");
            accepted(c, "RELEASE SAVEPOINT TcrSp2");
            assertEquals("3B001", state(c, "ROLLBACK TO SAVEPOINT tcrsp2"));
            exec(c, "ROLLBACK");
        }
    }

    // ---- 9. Transaction-scoped settings ----

    @Test
    void aTransactionScopedSettingCannotBeReset() throws Exception {
        try (Connection c = open()) {
            assertEquals("0A000", state(c, "RESET transaction_read_only"));
            assertEquals("0A000", state(c, "RESET transaction_isolation"));
            assertEquals("0A000", state(c, "RESET transaction_deferrable"));
            // an ordinary setting resets as before
            exec(c, "SET work_mem = '5MB'");
            assertEquals("5MB", scalar(c, "SHOW work_mem"));
            accepted(c, "RESET work_mem");
            assertNotEquals("5MB", scalar(c, "SHOW work_mem"));
        }
    }

    @Test
    void setConfigFollowsTheSameRulesAsSetTransaction() throws Exception {
        try (Connection c = open()) {
            assertEquals("25001", state(c, "SELECT set_config('transaction_isolation', 'serializable', false)"));
            // with no transaction open there is nothing for the value to belong to
            assertEquals("on", scalar(c, "SELECT set_config('transaction_read_only', 'on', false)"));
            assertEquals("off", scalar(c, "SHOW transaction_read_only"));
            // an ordinary setting is unaffected
            assertEquals("5MB", scalar(c, "SELECT set_config('work_mem', '5MB', false)"));
            assertEquals("5MB", scalar(c, "SHOW work_mem"));
            exec(c, "RESET work_mem");
        }
    }

    // ---- 10. MOVE reports how far it moved ----

    @Test
    void moveReportsTheNumberOfRowsItPassedOver() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tcr_move");
            exec(c, "CREATE TABLE tcr_move (i int primary key)");
            exec(c, "INSERT INTO tcr_move SELECT generate_series(1, 5)");
            exec(c, "BEGIN");
            exec(c, "DECLARE mc CURSOR FOR SELECT i FROM tcr_move ORDER BY i");
            assertEquals(2, update(c, "MOVE FORWARD 2 IN mc"));
            assertEquals("3", scalar(c, "FETCH NEXT FROM mc"));
            assertEquals(1, update(c, "MOVE BACKWARD 1 IN mc"));
            assertEquals("3", scalar(c, "FETCH NEXT FROM mc"));
            assertEquals(2, update(c, "MOVE ALL IN mc"));
            assertEquals(5, update(c, "MOVE BACKWARD ALL IN mc"));
            assertEquals("1", scalar(c, "FETCH NEXT FROM mc"));
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tcr_move");
        }
    }

    // ---- 11. Uncommitted DDL is not there for anyone else ----

    @Test
    void anUncommittedRelationIsNotInTheCatalogsEither() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tcr_uncommitted");
            exec(a, "BEGIN");
            exec(a, "CREATE TABLE tcr_uncommitted (i int primary key)");
            assertEquals("0", scalar(b, "SELECT count(*) FROM pg_class WHERE relname = 'tcr_uncommitted'"));
            assertEquals("0", scalar(b,
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'tcr_uncommitted'"));
            // the session that created it still sees its own work
            assertEquals("1", scalar(a, "SELECT count(*) FROM pg_class WHERE relname = 'tcr_uncommitted'"));
            exec(a, "COMMIT");
            assertEquals("1", scalar(b, "SELECT count(*) FROM pg_class WHERE relname = 'tcr_uncommitted'"));
            exec(a, "DROP TABLE tcr_uncommitted");
            assertEquals("0", scalar(b, "SELECT count(*) FROM pg_class WHERE relname = 'tcr_uncommitted'"));
        }
    }

    @Test
    void aTypeWhoseTransactionRolledBackNeverExisted() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TYPE IF EXISTS tcr_enum");
            exec(a, "BEGIN");
            exec(a, "CREATE TYPE tcr_enum AS ENUM ('a', 'b')");
            assertEquals("42704", state(b, "SELECT 'a'::tcr_enum"));
            exec(a, "ROLLBACK");
            assertEquals("42704", state(b, "SELECT 'a'::tcr_enum"));
            assertEquals("42704", state(a, "SELECT 'a'::tcr_enum"));
            // committing it makes it real
            exec(a, "BEGIN");
            exec(a, "CREATE TYPE tcr_enum AS ENUM ('a', 'b')");
            exec(a, "COMMIT");
            assertEquals("a", scalar(b, "SELECT 'a'::tcr_enum"));
            exec(a, "DROP TYPE tcr_enum");
        }
    }

    // ---- 12. pg_typeof answers void for the blocking advisory lock functions ----

    @Test
    void theBlockingAdvisoryLockFunctionsAreTypedVoid() throws Exception {
        try (Connection c = open()) {
            assertEquals("void", scalar(c, "SELECT pg_typeof(pg_advisory_unlock_all())"));
            assertEquals("void", scalar(c, "SELECT pg_typeof(pg_advisory_lock(9100011))"));
            assertEquals("", scalar(c, "SELECT pg_advisory_lock(9100012)"));
            // the try_ and unlock forms are still booleans
            assertEquals("boolean", scalar(c, "SELECT pg_typeof(pg_try_advisory_lock(9100013))"));
            assertEquals("boolean", scalar(c, "SELECT pg_typeof(pg_advisory_unlock(9100011))"));
            exec(c, "SELECT pg_advisory_unlock_all()");
        }
    }

    // ---- 13. An in-flight change from another session decides nothing here ----

    @Test
    void ordinaryDmlIsUnaffectedByTheCommittedImageRule() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tcr_dml");
            exec(c, "CREATE TABLE tcr_dml (id int primary key, n int)");
            exec(c, "INSERT INTO tcr_dml VALUES (1, 1), (2, 2), (3, 3)");
            assertEquals(2, update(c, "UPDATE tcr_dml SET n = n + 10 WHERE id < 3"));
            assertEquals(2, update(c, "DELETE FROM tcr_dml WHERE n > 10"));
            assertEquals("1", scalar(c, "SELECT count(*) FROM tcr_dml"));
            // a session sees its own uncommitted work within the transaction
            exec(c, "BEGIN");
            assertEquals(1, update(c, "UPDATE tcr_dml SET n = 99 WHERE id = 3"));
            assertEquals(1, update(c, "UPDATE tcr_dml SET n = 100 WHERE n = 99"));
            assertEquals(1, update(c, "DELETE FROM tcr_dml WHERE n = 100"));
            assertEquals("0", scalar(c, "SELECT count(*) FROM tcr_dml"));
            exec(c, "ROLLBACK");
            assertEquals("3|3", scalar(c, "SELECT id || '|' || n FROM tcr_dml"));
            exec(c, "DROP TABLE tcr_dml");
        }
    }
}
