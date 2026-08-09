package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sessions, transactions, cursors and locks, measured against PostgreSQL 18.
 *
 * <p>Two kinds of assertion live here. The rules say what PostgreSQL refuses — a savepoint outside
 * a transaction block, a lock on the nullable side of an outer join, a deferred check forced early.
 * The permissiveness assertions say what it accepts, and there are more of those on purpose: a rule
 * that fires one shape too wide costs more than the looseness it removed.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class TransactionControlTest {

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

    /** Run a statement expected to fail, returning its SQLSTATE. */
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

    /** Assert a statement runs without complaint — the guard against a rule that fires too wide. */
    private static void accepted(Connection c, String sql) {
        assertDoesNotThrow(() -> exec(c, sql), "PostgreSQL accepts this: " + sql);
    }

    // ---- 1. CACHE is an allocation hint, not a claim against the bounds ----

    @Test
    void aCacheWiderThanTheRangeStillHandsOutEveryValue() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP SEQUENCE IF EXISTS tct_cache");
            exec(c, "CREATE SEQUENCE tct_cache MAXVALUE 3 CACHE 10");
            assertEquals("1", scalar(c, "SELECT nextval('tct_cache')"));
            assertEquals("2", scalar(c, "SELECT nextval('tct_cache')"));
            assertEquals("3", scalar(c, "SELECT nextval('tct_cache')"));
            assertEquals("2200H", state(c, "SELECT nextval('tct_cache')"));
            exec(c, "DROP SEQUENCE tct_cache");
        }
    }

    @Test
    void aDescendingSequenceWithATooWideCacheBehavesTheSameWay() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP SEQUENCE IF EXISTS tct_desc");
            exec(c, "CREATE SEQUENCE tct_desc INCREMENT -1 MINVALUE -3 MAXVALUE -1 START -1 CACHE 10");
            assertEquals("-1", scalar(c, "SELECT nextval('tct_desc')"));
            assertEquals("-2", scalar(c, "SELECT nextval('tct_desc')"));
            assertEquals("-3", scalar(c, "SELECT nextval('tct_desc')"));
            assertEquals("2200H", state(c, "SELECT nextval('tct_desc')"));
            exec(c, "DROP SEQUENCE tct_desc");
        }
    }

    @Test
    void aCacheWithRoomStillReservesABlockPerSession() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP SEQUENCE IF EXISTS tct_block");
            exec(a, "CREATE SEQUENCE tct_block CACHE 5");
            assertEquals("1", scalar(a, "SELECT nextval('tct_block')"));
            // the second session gets the block after A's, not A's next value
            assertEquals("6", scalar(b, "SELECT nextval('tct_block')"));
            assertEquals("2", scalar(a, "SELECT nextval('tct_block')"));
            exec(a, "DROP SEQUENCE tct_block");
        }
    }

    @Test
    void aCyclingSequenceStillWrapsRatherThanFailing() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP SEQUENCE IF EXISTS tct_cycle");
            exec(c, "CREATE SEQUENCE tct_cycle MAXVALUE 3 CACHE 2 CYCLE");
            assertEquals("1", scalar(c, "SELECT nextval('tct_cycle')"));
            assertEquals("2", scalar(c, "SELECT nextval('tct_cycle')"));
            assertEquals("3", scalar(c, "SELECT nextval('tct_cycle')"));
            assertEquals("1", scalar(c, "SELECT nextval('tct_cycle')"));
            exec(c, "DROP SEQUENCE tct_cycle");
        }
    }

    // ---- 2. An explicit READ WRITE overrides default_transaction_read_only ----

    @Test
    void beginReadWriteOverridesTheSessionDefault() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_rw CASCADE");
            exec(c, "CREATE TABLE tct_rw (id int PRIMARY KEY)");
            exec(c, "SET default_transaction_read_only = on");
            assertEquals("on", scalar(c, "SHOW transaction_read_only"));
            accepted(c, "BEGIN READ WRITE");
            assertEquals("off", scalar(c, "SHOW transaction_read_only"));
            accepted(c, "CREATE TABLE tct_rw_made (a int)");
            accepted(c, "INSERT INTO tct_rw VALUES (1)");
            exec(c, "COMMIT");
            assertEquals("1", scalar(c, "SELECT count(*) FROM tct_rw"));
            exec(c, "SET default_transaction_read_only = off");
            exec(c, "DROP TABLE tct_rw_made");
            exec(c, "DROP TABLE tct_rw");
        }
    }

    @Test
    void setTransactionReadWriteOverridesTheSessionDefault() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_rw2 CASCADE");
            exec(c, "CREATE TABLE tct_rw2 (id int PRIMARY KEY)");
            exec(c, "SET default_transaction_read_only = on");
            exec(c, "BEGIN");
            accepted(c, "SET TRANSACTION READ WRITE");
            accepted(c, "INSERT INTO tct_rw2 VALUES (1)");
            exec(c, "COMMIT");
            assertEquals("1", scalar(c, "SELECT count(*) FROM tct_rw2"));
            exec(c, "SET default_transaction_read_only = off");
            exec(c, "DROP TABLE tct_rw2");
        }
    }

    @Test
    void theSessionDefaultStillStopsAWriteThatDidNotAskToOverrideIt() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_ro CASCADE");
            exec(c, "CREATE TABLE tct_ro (id int PRIMARY KEY)");
            exec(c, "SET default_transaction_read_only = on");
            exec(c, "BEGIN");
            assertEquals("25006", state(c, "INSERT INTO tct_ro VALUES (1)"));
            exec(c, "ROLLBACK");
            // and an explicit READ ONLY still stops it whatever the default is
            exec(c, "SET default_transaction_read_only = off");
            exec(c, "BEGIN READ ONLY");
            assertEquals("25006", state(c, "INSERT INTO tct_ro VALUES (1)"));
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tct_ro");
        }
    }

    @Test
    void readWriteTurnedReadOnlyMidTransactionStopsLaterWrites() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_ro2 CASCADE");
            exec(c, "CREATE TABLE tct_ro2 (id int PRIMARY KEY)");
            exec(c, "BEGIN READ WRITE");
            accepted(c, "INSERT INTO tct_ro2 VALUES (1)");
            exec(c, "SET TRANSACTION READ ONLY");
            assertEquals("25006", state(c, "INSERT INTO tct_ro2 VALUES (2)"));
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tct_ro2");
        }
    }

    // ---- 3. SET CONSTRAINTS ... IMMEDIATE runs the checks it has postponed ----

    @Test
    void setConstraintsAllImmediateFiresAPendingUniqueCheck() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_dc CASCADE");
            exec(c, "CREATE TABLE tct_dc (id int PRIMARY KEY DEFERRABLE INITIALLY DEFERRED)");
            exec(c, "BEGIN");
            exec(c, "INSERT INTO tct_dc VALUES (1)");
            exec(c, "INSERT INTO tct_dc VALUES (1)");
            assertEquals("23505", state(c, "SET CONSTRAINTS ALL IMMEDIATE"));
            exec(c, "ROLLBACK");
            assertEquals("0", scalar(c, "SELECT count(*) FROM tct_dc"));
            exec(c, "DROP TABLE tct_dc");
        }
    }

    @Test
    void setConstraintsAllImmediateFiresAPendingForeignKey() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_child CASCADE");
            exec(c, "DROP TABLE IF EXISTS tct_parent CASCADE");
            exec(c, "CREATE TABLE tct_parent (id int PRIMARY KEY)");
            exec(c, "CREATE TABLE tct_child (id int PRIMARY KEY,"
                    + " p int REFERENCES tct_parent(id) DEFERRABLE INITIALLY DEFERRED)");
            exec(c, "BEGIN");
            exec(c, "INSERT INTO tct_child VALUES (1, 77)");
            assertEquals("23503", state(c, "SET CONSTRAINTS ALL IMMEDIATE"));
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tct_child");
            exec(c, "DROP TABLE tct_parent");
        }
    }

    @Test
    void aDeferredForeignKeyIsStillDeferredUntilCommit() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_child2 CASCADE");
            exec(c, "DROP TABLE IF EXISTS tct_parent2 CASCADE");
            exec(c, "CREATE TABLE tct_parent2 (id int PRIMARY KEY)");
            exec(c, "CREATE TABLE tct_child2 (id int PRIMARY KEY,"
                    + " p int REFERENCES tct_parent2(id) DEFERRABLE INITIALLY DEFERRED)");
            exec(c, "BEGIN");
            accepted(c, "INSERT INTO tct_child2 VALUES (1, 77)");
            accepted(c, "INSERT INTO tct_parent2 VALUES (77)");
            accepted(c, "COMMIT");
            assertEquals("1", scalar(c, "SELECT count(*) FROM tct_child2"));
            exec(c, "DROP TABLE tct_child2");
            exec(c, "DROP TABLE tct_parent2");
        }
    }

    @Test
    void setConstraintsChecksTheNamesItIsGiven() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_nd CASCADE");
            exec(c, "CREATE TABLE tct_nd (id int, CONSTRAINT tct_nd_pk PRIMARY KEY (id))");
            exec(c, "BEGIN");
            assertEquals("42704", state(c, "SET CONSTRAINTS tct_no_such DEFERRED"));
            exec(c, "ROLLBACK");
            exec(c, "BEGIN");
            assertEquals("42809", state(c, "SET CONSTRAINTS tct_nd_pk DEFERRED"));
            exec(c, "ROLLBACK");
            exec(c, "BEGIN");
            assertTrue(message(c, "SET CONSTRAINTS tct_nd_pk DEFERRED").contains("is not deferrable"));
            exec(c, "ROLLBACK");
            // IMMEDIATE is what a non-deferrable constraint already is, so it is accepted
            exec(c, "BEGIN");
            accepted(c, "SET CONSTRAINTS tct_nd_pk IMMEDIATE");
            accepted(c, "SET CONSTRAINTS ALL IMMEDIATE");
            accepted(c, "SET CONSTRAINTS ALL DEFERRED");
            exec(c, "ROLLBACK");
            // and outside a transaction block both forms are accepted
            accepted(c, "SET CONSTRAINTS ALL IMMEDIATE");
            accepted(c, "SET CONSTRAINTS ALL DEFERRED");
            exec(c, "DROP TABLE tct_nd");
        }
    }

    // ---- 4. Savepoints are a stack of names, not a set of them ----

    @Test
    void aSecondSavepointOfTheSameNameShadowsTheFirst() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_sp CASCADE");
            exec(c, "CREATE TABLE tct_sp (i int PRIMARY KEY)");
            exec(c, "BEGIN");
            exec(c, "INSERT INTO tct_sp VALUES (1)");
            exec(c, "SAVEPOINT s");
            exec(c, "INSERT INTO tct_sp VALUES (2)");
            exec(c, "SAVEPOINT s");
            exec(c, "INSERT INTO tct_sp VALUES (3)");
            exec(c, "RELEASE SAVEPOINT s");     // releases the inner one
            accepted(c, "ROLLBACK TO SAVEPOINT s"); // the outer one is still there
            assertEquals("[1]", column(c, "SELECT i FROM tct_sp ORDER BY i").toString());
            exec(c, "COMMIT");
            assertEquals("[1]", column(c, "SELECT i FROM tct_sp ORDER BY i").toString());
            exec(c, "DROP TABLE tct_sp");
        }
    }

    @Test
    void rollingBackToASavepointTwiceKeepsIt() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_sp2 CASCADE");
            exec(c, "CREATE TABLE tct_sp2 (i int PRIMARY KEY)");
            exec(c, "BEGIN");
            exec(c, "SAVEPOINT s");
            exec(c, "INSERT INTO tct_sp2 VALUES (1)");
            accepted(c, "ROLLBACK TO SAVEPOINT s");
            accepted(c, "ROLLBACK TO SAVEPOINT s");
            assertEquals("[]", column(c, "SELECT i FROM tct_sp2 ORDER BY i").toString());
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tct_sp2");
        }
    }

    @Test
    void releasingASavepointAlsoDestroysTheLaterOnes() throws Exception {
        try (Connection c = open()) {
            exec(c, "BEGIN");
            exec(c, "SAVEPOINT a");
            exec(c, "SAVEPOINT b");
            exec(c, "RELEASE SAVEPOINT a");
            assertEquals("3B001", state(c, "ROLLBACK TO SAVEPOINT b"));
            exec(c, "ROLLBACK");
        }
    }

    @Test
    void anAbortedTransactionTakesRollbackToSavepointButNotSavepoint() throws Exception {
        try (Connection c = open()) {
            exec(c, "BEGIN");
            exec(c, "SAVEPOINT ok1");
            assertEquals("22012", state(c, "SELECT 1/0"));
            assertEquals("25P02", state(c, "SAVEPOINT bad"));
            assertEquals("25P02", state(c, "RELEASE SAVEPOINT ok1"));
            accepted(c, "ROLLBACK TO SAVEPOINT ok1");
            assertEquals("1", scalar(c, "SELECT 1"));
            exec(c, "COMMIT");
        }
    }

    // ---- 5. ROLLBACK TO SAVEPOINT and cursors ----

    @Test
    void aCursorOpenedInsideTheSubtransactionDiesWithIt() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_cur CASCADE");
            exec(c, "CREATE TABLE tct_cur (i int PRIMARY KEY)");
            exec(c, "INSERT INTO tct_cur VALUES (1),(2),(3)");
            exec(c, "BEGIN");
            exec(c, "SAVEPOINT s1");
            exec(c, "DECLARE tct_c CURSOR FOR SELECT i FROM tct_cur ORDER BY i");
            assertEquals("1", scalar(c, "FETCH 1 FROM tct_c"));
            exec(c, "ROLLBACK TO SAVEPOINT s1");
            assertEquals("34000", state(c, "FETCH 1 FROM tct_c"));
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tct_cur");
        }
    }

    @Test
    void aCursorOpenedBeforeTheSavepointSurvivesAtThePositionFetchLeftIt() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_cur2 CASCADE");
            exec(c, "CREATE TABLE tct_cur2 (i int PRIMARY KEY)");
            exec(c, "INSERT INTO tct_cur2 VALUES (1),(2),(3)");
            exec(c, "BEGIN");
            exec(c, "DECLARE tct_c2 CURSOR FOR SELECT i FROM tct_cur2 ORDER BY i");
            exec(c, "SAVEPOINT s1");
            assertEquals("1", scalar(c, "FETCH 1 FROM tct_c2"));
            exec(c, "ROLLBACK TO SAVEPOINT s1");
            // PG does not rewind a cursor that a rolled-back subtransaction only moved
            assertEquals("2", scalar(c, "FETCH 1 FROM tct_c2"));
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tct_cur2");
        }
    }

    @Test
    void aCursorWhoseSubtransactionWasReleasedStaysAlive() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_cur3 CASCADE");
            exec(c, "CREATE TABLE tct_cur3 (i int PRIMARY KEY)");
            exec(c, "INSERT INTO tct_cur3 VALUES (1),(2),(3)");
            exec(c, "BEGIN");
            exec(c, "SAVEPOINT s1");
            exec(c, "DECLARE tct_c3 CURSOR FOR SELECT i FROM tct_cur3 ORDER BY i");
            exec(c, "RELEASE SAVEPOINT s1");
            assertEquals("1", scalar(c, "FETCH 1 FROM tct_c3"));
            exec(c, "ROLLBACK");
            exec(c, "DROP TABLE tct_cur3");
        }
    }

    @Test
    void withHoldStillSurvivesCommitAndAPlainCursorStillDoesNot() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_cur4 CASCADE");
            exec(c, "CREATE TABLE tct_cur4 (i int PRIMARY KEY)");
            exec(c, "INSERT INTO tct_cur4 VALUES (1),(2),(3)");
            exec(c, "BEGIN");
            exec(c, "DECLARE tct_c4 CURSOR WITH HOLD FOR SELECT i FROM tct_cur4 ORDER BY i");
            exec(c, "DECLARE tct_c5 CURSOR FOR SELECT i FROM tct_cur4 ORDER BY i");
            exec(c, "COMMIT");
            assertEquals("1", scalar(c, "FETCH 1 FROM tct_c4"));
            assertEquals("34000", state(c, "FETCH 1 FROM tct_c5"));
            exec(c, "CLOSE tct_c4");
            exec(c, "DROP TABLE tct_cur4");
        }
    }

    // ---- 6. FOR UPDATE legality ----

    @Test
    void forUpdateOfNamesARelationThatIsInTheFromClause() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_fu CASCADE");
            exec(c, "CREATE TABLE tct_fu (id int PRIMARY KEY, v int)");
            exec(c, "INSERT INTO tct_fu VALUES (1,10),(2,20)");
            assertEquals("42P01", state(c, "SELECT id FROM tct_fu FOR UPDATE OF tct_nosuch"));
            // an alias hides the relation name it stands for
            assertEquals("42P01", state(c, "SELECT a.id FROM tct_fu a FOR UPDATE OF tct_fu"));
            // the message names the lock mode that was written
            assertTrue(message(c, "SELECT id FROM tct_fu FOR SHARE OF tct_nosuch")
                    .contains("in FOR SHARE clause not found in FROM clause"));
            assertTrue(message(c, "SELECT id FROM tct_fu FOR NO KEY UPDATE OF tct_nosuch")
                    .contains("in FOR NO KEY UPDATE clause not found in FROM clause"));
            exec(c, "DROP TABLE tct_fu");
        }
    }

    @Test
    void forUpdateRefusesTheNullableSideOfAnOuterJoin() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_fu2 CASCADE");
            exec(c, "CREATE TABLE tct_fu2 (id int PRIMARY KEY, v int)");
            exec(c, "INSERT INTO tct_fu2 VALUES (1,10),(2,20)");
            assertEquals("0A000", state(c,
                    "SELECT a.id FROM tct_fu2 a LEFT JOIN tct_fu2 b ON a.id=b.id FOR UPDATE OF b"));
            assertEquals("0A000", state(c,
                    "SELECT a.id FROM tct_fu2 a RIGHT JOIN tct_fu2 b ON a.id=b.id FOR UPDATE OF a"));
            assertEquals("0A000", state(c,
                    "SELECT a.id FROM tct_fu2 a FULL JOIN tct_fu2 b ON a.id=b.id FOR UPDATE OF a"));
            // a plain FOR UPDATE marks every relation, so an outer join is refused too
            assertEquals("0A000", state(c,
                    "SELECT a.id FROM tct_fu2 a LEFT JOIN tct_fu2 b ON a.id=b.id ORDER BY a.id FOR UPDATE"));
            assertEquals("0A000", state(c, "SELECT * FROM generate_series(1,2) g FOR UPDATE OF g"));
            exec(c, "DROP TABLE tct_fu2");
        }
    }

    @Test
    void theOrdinaryForUpdateShapesAreStillAccepted() throws Exception {
        try (Connection c = open()) {
            exec(c, "DROP TABLE IF EXISTS tct_fu3 CASCADE");
            exec(c, "CREATE TABLE tct_fu3 (id int PRIMARY KEY, v int)");
            exec(c, "INSERT INTO tct_fu3 VALUES (1,10),(2,20)");
            accepted(c, "SELECT * FROM tct_fu3 ORDER BY id FOR UPDATE");
            accepted(c, "SELECT * FROM tct_fu3 WHERE id=1 FOR UPDATE");
            accepted(c, "SELECT * FROM tct_fu3 ORDER BY id LIMIT 1 FOR UPDATE");
            accepted(c, "SELECT * FROM tct_fu3 ORDER BY id FOR SHARE");
            accepted(c, "SELECT * FROM tct_fu3 ORDER BY id FOR NO KEY UPDATE");
            accepted(c, "SELECT * FROM tct_fu3 ORDER BY id FOR KEY SHARE");
            accepted(c, "SELECT * FROM tct_fu3 ORDER BY id FOR UPDATE NOWAIT");
            accepted(c, "SELECT * FROM tct_fu3 ORDER BY id FOR UPDATE SKIP LOCKED");
            accepted(c, "SELECT id FROM tct_fu3 FOR UPDATE OF tct_fu3");
            accepted(c, "SELECT a.id FROM tct_fu3 a FOR UPDATE OF a");
            accepted(c, "SELECT a.id FROM tct_fu3 a JOIN tct_fu3 b ON a.id=b.id ORDER BY a.id FOR UPDATE");
            accepted(c, "SELECT a.id FROM tct_fu3 a LEFT JOIN tct_fu3 b ON a.id=b.id"
                    + " ORDER BY a.id FOR UPDATE OF a");
            accepted(c, "SELECT * FROM (SELECT id FROM tct_fu3) s ORDER BY id FOR UPDATE OF s");
            accepted(c, "SELECT * FROM (SELECT id FROM tct_fu3 FOR UPDATE) s ORDER BY id");
            accepted(c, "SELECT g FROM generate_series(1,2) g ORDER BY g FOR UPDATE");
            accepted(c, "WITH cte AS (SELECT id FROM tct_fu3 FOR UPDATE) SELECT * FROM cte ORDER BY id");
            accepted(c, "SELECT id FROM tct_fu3 WHERE id IN (SELECT id FROM tct_fu3) ORDER BY id FOR UPDATE");
            exec(c, "DROP TABLE tct_fu3");
        }
    }

    // ---- 7. Transaction-control statements outside a transaction block ----

    @Test
    void statementsThatOnlyMeanSomethingInsideABlockAreRefusedOutsideOne() throws Exception {
        try (Connection c = open()) {
            assertEquals("25P01", state(c, "SAVEPOINT tct_outside"));
            assertEquals("25P01", state(c, "RELEASE SAVEPOINT tct_outside"));
            assertEquals("25P01", state(c, "ROLLBACK TO SAVEPOINT tct_outside"));
            assertEquals("25P01", state(c, "COMMIT AND CHAIN"));
            assertEquals("25P01", state(c, "ROLLBACK AND CHAIN"));
            assertEquals("25P01", state(c, "END AND CHAIN"));
            assertEquals("25P01", state(c, "ABORT AND CHAIN"));
            // a plain COMMIT or ROLLBACK outside a block is still only a warning
            accepted(c, "COMMIT");
            accepted(c, "ROLLBACK");
            accepted(c, "END");
            accepted(c, "ABORT");
        }
    }

    @Test
    void andChainInsideABlockStillOpensTheNextTransaction() throws Exception {
        try (Connection c = open()) {
            exec(c, "BEGIN ISOLATION LEVEL REPEATABLE READ");
            accepted(c, "COMMIT AND CHAIN");
            assertEquals("repeatable read", scalar(c, "SHOW transaction_isolation"));
            accepted(c, "ROLLBACK AND CHAIN");
            assertEquals("repeatable read", scalar(c, "SHOW transaction_isolation"));
            exec(c, "ROLLBACK");
            assertEquals("read committed", scalar(c, "SHOW transaction_isolation"));
        }
    }

    @Test
    void setTransactionMustComeBeforeTheTransactionHasTakenItsSnapshot() throws Exception {
        try (Connection c = open()) {
            exec(c, "BEGIN");
            exec(c, "SELECT 1");
            assertEquals("25001", state(c, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
            exec(c, "ROLLBACK");
            // a savepoint puts the work in a subtransaction, which PG refuses as well
            exec(c, "BEGIN");
            exec(c, "SAVEPOINT s");
            assertEquals("25001", state(c, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
            exec(c, "ROLLBACK");
            // before any query it is accepted, and settings do not count as one
            exec(c, "BEGIN");
            accepted(c, "SET LOCAL work_mem = '2MB'");
            accepted(c, "SHOW work_mem");
            accepted(c, "SET CONSTRAINTS ALL DEFERRED");
            accepted(c, "SET TRANSACTION READ ONLY");
            accepted(c, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            assertEquals("serializable", scalar(c, "SHOW transaction_isolation"));
            exec(c, "ROLLBACK");
            // READ ONLY may be chosen at any point; going back to READ WRITE once the
            // transaction is read-only and has run a query would change the mode a statement
            // already ran under, and PostgreSQL refuses it.
            exec(c, "BEGIN");
            exec(c, "SELECT 1");
            accepted(c, "SET TRANSACTION READ WRITE");
            assertEquals("25001", state(c, "SET TRANSACTION DEFERRABLE"));
            exec(c, "ROLLBACK");
            exec(c, "BEGIN");
            exec(c, "SELECT 1");
            accepted(c, "SET TRANSACTION READ ONLY");
            assertEquals("25001", state(c, "SET TRANSACTION READ WRITE"));
            exec(c, "ROLLBACK");
        }
    }

    @Test
    void setTransactionOutsideABlockDoesNotChangeTheSession() throws Exception {
        try (Connection c = open()) {
            assertEquals("read committed", scalar(c, "SHOW transaction_isolation"));
            accepted(c, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals("read committed", scalar(c, "SHOW transaction_isolation"));
            accepted(c, "SET TRANSACTION READ ONLY");
            assertEquals("off", scalar(c, "SHOW transaction_read_only"));
            accepted(c, "SET TRANSACTION DEFERRABLE");
            assertEquals("off", scalar(c, "SHOW transaction_deferrable"));
        }
    }

    @Test
    void transactionDeferrableIsASettingAndBeginCanSetIt() throws Exception {
        try (Connection c = open()) {
            assertEquals("off", scalar(c, "SHOW transaction_deferrable"));
            exec(c, "BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE");
            assertEquals("on", scalar(c, "SHOW transaction_deferrable"));
            assertEquals("serializable", scalar(c, "SHOW transaction_isolation"));
            assertEquals("on", scalar(c, "SHOW transaction_read_only"));
            exec(c, "COMMIT");
            assertEquals("off", scalar(c, "SHOW transaction_deferrable"));
            // SET SESSION CHARACTERISTICS reaches the session default instead
            exec(c, "SET SESSION CHARACTERISTICS AS TRANSACTION DEFERRABLE");
            assertEquals("on", scalar(c, "SHOW default_transaction_deferrable"));
            exec(c, "SET SESSION CHARACTERISTICS AS TRANSACTION NOT DEFERRABLE");
            assertEquals("off", scalar(c, "SHOW default_transaction_deferrable"));
        }
    }

    @Test
    void anIsolationLevelPostgresDoesNotHaveIsRejected() throws Exception {
        try (Connection c = open()) {
            assertEquals("42601", state(c, "SET TRANSACTION ISOLATION LEVEL NONSENSE"));
            assertEquals("42601", state(c, "BEGIN ISOLATION LEVEL NONSENSE"));
            exec(c, "ROLLBACK");
            assertEquals("22023", state(c, "SET default_transaction_isolation = 'nonsense'"));
            // every level PG does have is still accepted, in every spelling
            accepted(c, "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE NOT DEFERRABLE");
            accepted(c, "COMMIT");
            accepted(c, "START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE");
            accepted(c, "COMMIT");
            accepted(c, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            accepted(c, "COMMIT");
            accepted(c, "BEGIN WORK ISOLATION LEVEL READ UNCOMMITTED");
            accepted(c, "COMMIT");
            accepted(c, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            accepted(c, "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE");
            accepted(c, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED");
        }
    }

    // ---- 8. Two sessions ----

    @Test
    void anUncommittedRelationIsNotVisibleToAnotherSession() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tct_ddl CASCADE");
            exec(a, "BEGIN");
            exec(a, "CREATE TABLE tct_ddl (i int)");
            assertEquals("42P01", state(b, "SELECT * FROM tct_ddl"));
            // its own session sees it perfectly well
            assertEquals("0", scalar(a, "SELECT count(*) FROM tct_ddl"));
            exec(a, "COMMIT");
            assertEquals("0", scalar(b, "SELECT count(*) FROM tct_ddl"));
            exec(a, "DROP TABLE tct_ddl");
        }
    }

    @Test
    void anUncommittedSequenceIsNotVisibleToAnotherSession() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP SEQUENCE IF EXISTS tct_seq_iso");
            exec(a, "BEGIN");
            exec(a, "CREATE SEQUENCE tct_seq_iso");
            assertEquals("42P01", state(b, "SELECT nextval('tct_seq_iso')"));
            exec(a, "ROLLBACK");
            assertEquals("42P01", state(b, "SELECT nextval('tct_seq_iso')"));
        }
    }

    @Test
    void currvalIsScopedToTheSessionThatCalledNextval() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP SEQUENCE IF EXISTS tct_cv");
            exec(a, "CREATE SEQUENCE tct_cv");
            assertEquals("1", scalar(a, "SELECT nextval('tct_cv')"));
            assertEquals("55000", state(b, "SELECT currval('tct_cv')"));
            assertEquals("1", scalar(a, "SELECT currval('tct_cv')"));
            exec(a, "DROP SEQUENCE tct_cv");
        }
    }

    @Test
    void anUpdateIsJudgedAgainstWhatTheOtherSessionHasCommitted() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tct_vis CASCADE");
            exec(a, "CREATE TABLE tct_vis (i int PRIMARY KEY)");
            exec(a, "INSERT INTO tct_vis VALUES (1)");
            exec(a, "BEGIN");
            exec(a, "UPDATE tct_vis SET i = 2 WHERE i = 1");
            // B must not be told the row has left its WHERE clause on the strength of
            // a change that may yet be rolled back; it waits instead.
            exec(b, "SET lock_timeout = '600ms'");
            assertEquals("55P03", state(b, "UPDATE tct_vis SET i = 3 WHERE i = 1"));
            exec(a, "ROLLBACK");
            exec(b, "SET lock_timeout = 0");
            // with A gone the row is back where B could see it
            exec(b, "UPDATE tct_vis SET i = 3 WHERE i = 1");
            assertEquals("[3]", column(b, "SELECT i FROM tct_vis ORDER BY i").toString());
            exec(a, "DROP TABLE tct_vis");
        }
    }

    @Test
    void lockTimeoutDecidesHowLongARowLockIsWaitedFor() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tct_lt CASCADE");
            exec(a, "CREATE TABLE tct_lt (i int PRIMARY KEY)");
            exec(a, "INSERT INTO tct_lt VALUES (1)");
            exec(a, "BEGIN");
            exec(a, "SELECT * FROM tct_lt WHERE i=1 FOR UPDATE");
            exec(b, "SET lock_timeout = '400ms'");
            long started = System.currentTimeMillis();
            assertEquals("55P03", state(b, "SELECT * FROM tct_lt WHERE i=1 FOR UPDATE"));
            long waited = System.currentTimeMillis() - started;
            assertTrue(waited >= 300 && waited < 4000,
                    "the wait should follow lock_timeout, not a fixed budget; waited " + waited + "ms");
            // NOWAIT and SKIP LOCKED are unaffected by the setting
            assertEquals("55P03", state(b, "SELECT * FROM tct_lt WHERE i=1 FOR UPDATE NOWAIT"));
            assertEquals("[]", column(b, "SELECT i FROM tct_lt ORDER BY i FOR UPDATE SKIP LOCKED").toString());
            exec(b, "SET lock_timeout = 0");
            exec(a, "ROLLBACK");
            exec(a, "DROP TABLE tct_lt");
        }
    }

    @Test
    void statementTimeoutStillEndsALockWaitWhenLockTimeoutIsOff() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS tct_st CASCADE");
            exec(a, "CREATE TABLE tct_st (i int PRIMARY KEY)");
            exec(a, "INSERT INTO tct_st VALUES (1)");
            exec(a, "BEGIN");
            exec(a, "SELECT * FROM tct_st WHERE i=1 FOR UPDATE");
            exec(b, "SET statement_timeout = '600ms'");
            assertEquals("57014", state(b, "SELECT * FROM tct_st WHERE i=1 FOR UPDATE"));
            exec(b, "SET statement_timeout = 0");
            exec(a, "ROLLBACK");
            exec(a, "DROP TABLE tct_st");
        }
    }

    @Test
    void aBlockingAdvisoryLockReadsBackAsVoidNotNull() throws Exception {
        try (Connection c = open()) {
            try (Statement st = c.createStatement()) {
                st.setQueryTimeout(20);
                try (ResultSet rs = st.executeQuery("SELECT pg_advisory_lock(47110)")) {
                    assertTrue(rs.next());
                    assertEquals("", rs.getString(1));
                    assertFalse(rs.wasNull(), "a void result is an empty string, not SQL NULL");
                }
            }
            try (Statement st = c.createStatement()) {
                st.setQueryTimeout(20);
                try (ResultSet rs = st.executeQuery("SELECT pg_advisory_unlock(47110)")) {
                    assertTrue(rs.next());
                    // the unlock forms answer with a boolean, not void
                    assertEquals(Boolean.TRUE, rs.getObject(1));
                }
            }
            try (Statement st = c.createStatement()) {
                st.setQueryTimeout(20);
                try (ResultSet rs = st.executeQuery("SELECT pg_advisory_unlock_all()")) {
                    assertTrue(rs.next());
                    assertEquals("", rs.getString(1));
                }
            }
        }
    }
}
