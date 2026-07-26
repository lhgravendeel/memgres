package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks a transaction takes must be visible to other sessions and must unwind with the
 * transaction. Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>N16 REPEATABLE READ snapshot vs DDL, N18 plain UPDATE/DELETE row locks,
 * N19 FOR UPDATE over joins and OF targets, N39 ROLLBACK TO SAVEPOINT releasing locks
 * and GUCs, N68 relation locks against concurrent TRUNCATE/ALTER.
 */
class RowAndTableLockingTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        try (Connection c = open()) {
            exec(c, "CREATE TABLE lk_a (id int PRIMARY KEY, v text)");
            exec(c, "CREATE TABLE lk_b (id int PRIMARY KEY, a int, v text)");
            exec(c, "INSERT INTO lk_a VALUES (1,'a1'),(2,'a2'),(3,'a3'),(4,'a4'),(5,'a5'),(6,'a6')");
            exec(c, "INSERT INTO lk_b VALUES (10,1,'b1'),(11,2,'b2'),(13,3,'b3'),(14,4,'b4')");
        }
    }

    @AfterAll
    static void tearDown() {
        if (memgres != null) memgres.close();
    }

    static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    static void exec(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    static List<String> rows(Connection c, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    /** Runs sql on a fresh connection and returns its SQLSTATE, or null when it succeeds. */
    static String otherSession(String sql) throws SQLException {
        try (Connection c = open()) {
            exec(c, sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    // ------------------------------------------------------------------
    // N18 — plain UPDATE/DELETE take a row lock other sessions can see
    // ------------------------------------------------------------------

    @Test
    void uncommittedUpdateBlocksForUpdateNowait() throws Exception {
        try (Connection a = open()) {
            a.setAutoCommit(false);
            exec(a, "UPDATE lk_a SET v='x' WHERE id=1");

            assertEquals("55P03", otherSession("SELECT * FROM lk_a WHERE id=1 FOR UPDATE NOWAIT"));
            assertNull(otherSession("SELECT * FROM lk_a WHERE id=2 FOR UPDATE NOWAIT"),
                    "an untouched row stays lockable");
            assertNull(otherSession("SELECT v FROM lk_a WHERE id=1"),
                    "a plain read is never blocked by a row lock");
            a.rollback();
        }
    }

    @Test
    void uncommittedDeleteBlocksForUpdateNowait() throws Exception {
        try (Connection a = open()) {
            a.setAutoCommit(false);
            exec(a, "DELETE FROM lk_a WHERE id=5");
            assertEquals("55P03", otherSession("SELECT * FROM lk_a WHERE id=5 FOR UPDATE NOWAIT"));
            a.rollback();
        }
    }

    @Test
    void autocommitForUpdateReleasesItsLock() throws Exception {
        try (Connection a = open()) {
            assertEquals(Arrays.asList("6|a6"), rows(a, "SELECT * FROM lk_a WHERE id=6 FOR UPDATE"));
        }
        assertNull(otherSession("SELECT * FROM lk_a WHERE id=6 FOR UPDATE NOWAIT"));
    }

    // ------------------------------------------------------------------
    // N19 — FOR UPDATE over a join locks rows; OF picks which side
    // ------------------------------------------------------------------

    @Test
    void forUpdateOfLocksOnlyTheNamedRelation() throws Exception {
        try (Connection a = open()) {
            a.setAutoCommit(false);
            assertEquals(Arrays.asList("3|13"), rows(a,
                    "SELECT a.id, b.id FROM lk_a a JOIN lk_b b ON b.a = a.id WHERE a.id=3 FOR UPDATE OF a"));

            assertEquals("55P03", otherSession("SELECT * FROM lk_a WHERE id=3 FOR UPDATE NOWAIT"));
            assertNull(otherSession("SELECT * FROM lk_b WHERE id=13 FOR UPDATE NOWAIT"),
                    "the relation not named by OF is left unlocked");
            a.rollback();
        }
    }

    @Test
    void plainForUpdateOverAJoinLocksBothSides() throws Exception {
        try (Connection a = open()) {
            a.setAutoCommit(false);
            assertEquals(Arrays.asList("4|14"), rows(a,
                    "SELECT a.id, b.id FROM lk_a a JOIN lk_b b ON b.a = a.id WHERE a.id=4 FOR UPDATE"));

            assertEquals("55P03", otherSession("SELECT * FROM lk_a WHERE id=4 FOR UPDATE NOWAIT"));
            assertEquals("55P03", otherSession("SELECT * FROM lk_b WHERE id=14 FOR UPDATE NOWAIT"));
            a.rollback();
        }
    }

    // ------------------------------------------------------------------
    // N39 — ROLLBACK TO SAVEPOINT unwinds locks and GUCs
    // ------------------------------------------------------------------

    @Test
    void rollbackToSavepointReleasesLocksTakenAfterIt() throws Exception {
        try (Connection a = open()) {
            a.setAutoCommit(false);
            exec(a, "SAVEPOINT sp1");
            rows(a, "SELECT * FROM lk_a WHERE id=1 FOR UPDATE");
            assertEquals("55P03", otherSession("SELECT * FROM lk_a WHERE id=1 FOR UPDATE NOWAIT"));

            exec(a, "ROLLBACK TO SAVEPOINT sp1");
            assertNull(otherSession("SELECT * FROM lk_a WHERE id=1 FOR UPDATE NOWAIT"));
            a.rollback();
        }
    }

    @Test
    void rollbackToSavepointKeepsLocksTakenBeforeIt() throws Exception {
        try (Connection a = open()) {
            a.setAutoCommit(false);
            rows(a, "SELECT * FROM lk_a WHERE id=2 FOR UPDATE");
            exec(a, "SAVEPOINT sp1");
            exec(a, "ROLLBACK TO SAVEPOINT sp1");

            assertEquals("55P03", otherSession("SELECT * FROM lk_a WHERE id=2 FOR UPDATE NOWAIT"));
            a.rollback();
        }
    }

    @Test
    void rollbackToSavepointRevertsSettingsMadeAfterIt() throws Exception {
        try (Connection a = open()) {
            a.setAutoCommit(false);
            exec(a, "SET LOCAL work_mem = '5MB'");
            assertEquals(Arrays.asList("5MB"), rows(a, "SHOW work_mem"));
            exec(a, "SAVEPOINT sp1");
            exec(a, "SET LOCAL work_mem = '9MB'");
            assertEquals(Arrays.asList("9MB"), rows(a, "SHOW work_mem"));

            exec(a, "ROLLBACK TO SAVEPOINT sp1");
            assertEquals(Arrays.asList("5MB"), rows(a, "SHOW work_mem"));
            a.rollback();
        }
    }

    // ------------------------------------------------------------------
    // N16 — a REPEATABLE READ transaction sees its own DDL and TRUNCATE
    // ------------------------------------------------------------------

    @Test
    void repeatableReadSeesItsOwnAddedColumn() throws Exception {
        try (Connection a = open()) {
            a.setAutoCommit(false);
            a.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            exec(a, "CREATE TABLE lk_ddl (id int PRIMARY KEY)");
            a.commit();

            exec(a, "INSERT INTO lk_ddl VALUES (1),(2)");
            assertEquals(Arrays.asList("2"), rows(a, "SELECT count(*) FROM lk_ddl"));
            exec(a, "ALTER TABLE lk_ddl ADD COLUMN extra text DEFAULT 'x'");
            assertEquals(Arrays.asList("1|x", "2|x"), rows(a, "SELECT id, extra FROM lk_ddl ORDER BY id"));
            a.rollback();
        }
    }

    @Test
    void truncateCascadeIsVisibleToItsOwnRepeatableReadTransaction() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE lk_par (id int PRIMARY KEY)");
            exec(c, "CREATE TABLE lk_kid (id int PRIMARY KEY, p int REFERENCES lk_par(id))");
            exec(c, "CREATE TABLE lk_grandkid (id int PRIMARY KEY, k int REFERENCES lk_kid(id))");
            exec(c, "INSERT INTO lk_par VALUES (1),(2)");
            exec(c, "INSERT INTO lk_kid VALUES (10,1),(11,2)");
            exec(c, "INSERT INTO lk_grandkid VALUES (100,10)");
        }
        try (Connection a = open()) {
            a.setAutoCommit(false);
            a.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            assertEquals(Arrays.asList("2"), rows(a, "SELECT count(*) FROM lk_kid"));

            exec(a, "TRUNCATE lk_par CASCADE");
            assertEquals(Arrays.asList("0"), rows(a, "SELECT count(*) FROM lk_par"));
            assertEquals(Arrays.asList("0"), rows(a, "SELECT count(*) FROM lk_kid"));
            assertEquals(Arrays.asList("0"), rows(a, "SELECT count(*) FROM lk_grandkid"),
                    "CASCADE reaches transitive dependents");
            a.rollback();
        }
        // ROLLBACK restores every cascaded table
        try (Connection c = open()) {
            assertEquals(Arrays.asList("2"), rows(c, "SELECT count(*) FROM lk_par"));
            assertEquals(Arrays.asList("2"), rows(c, "SELECT count(*) FROM lk_kid"));
            assertEquals(Arrays.asList("1"), rows(c, "SELECT count(*) FROM lk_grandkid"));
        }
    }

    @Test
    void cascadedTruncateFiresTheChildsStatementTriggers() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE lk_tp (id int PRIMARY KEY)");
            exec(c, "CREATE TABLE lk_tc (id int PRIMARY KEY, p int REFERENCES lk_tp(id))");
            exec(c, "CREATE TABLE lk_tlog (msg text)");
            exec(c, "CREATE OR REPLACE FUNCTION lk_tlog_fn() RETURNS trigger AS $$ "
                    + "BEGIN INSERT INTO lk_tlog VALUES (TG_TABLE_NAME); RETURN NULL; END $$ LANGUAGE plpgsql");
            exec(c, "CREATE TRIGGER lk_t1 BEFORE TRUNCATE ON lk_tp FOR EACH STATEMENT EXECUTE FUNCTION lk_tlog_fn()");
            exec(c, "CREATE TRIGGER lk_t2 BEFORE TRUNCATE ON lk_tc FOR EACH STATEMENT EXECUTE FUNCTION lk_tlog_fn()");
            exec(c, "INSERT INTO lk_tp VALUES (1)");
            exec(c, "INSERT INTO lk_tc VALUES (5,1)");

            exec(c, "TRUNCATE lk_tp CASCADE");
            assertEquals(Arrays.asList("lk_tc", "lk_tp"), rows(c, "SELECT msg FROM lk_tlog ORDER BY msg"));
        }
    }

    // ------------------------------------------------------------------
    // N68 — an open reader holds a relation lock
    // ------------------------------------------------------------------

    @Test
    void openReaderBlocksConcurrentTruncate() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE lk_rel (id int PRIMARY KEY)");
            exec(c, "INSERT INTO lk_rel VALUES (1),(2)");
        }
        try (Connection a = open()) {
            a.setAutoCommit(false);
            a.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            assertEquals(Arrays.asList("2"), rows(a, "SELECT count(*) FROM lk_rel"));

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch finished = new CountDownLatch(1);
            AtomicReference<String> error = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try (Connection b = open()) {
                    started.countDown();
                    exec(b, "TRUNCATE lk_rel");
                } catch (SQLException e) {
                    error.set(e.getSQLState());
                } finally {
                    finished.countDown();
                }
            });
            t.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertTrue(!finished.await(300, TimeUnit.MILLISECONDS),
                    "TRUNCATE must wait behind the open reader's relation lock");

            a.rollback();
            assertTrue(finished.await(10, TimeUnit.SECONDS), "TRUNCATE proceeds once the reader ends");
            t.join(5000);
            assertNull(error.get());
        }
        try (Connection c = open()) {
            assertEquals(Arrays.asList("0"), rows(c, "SELECT count(*) FROM lk_rel"));
        }
    }

    @Test
    void openReaderBlocksConcurrentAlterTable() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE lk_rel2 (id int PRIMARY KEY)");
            exec(c, "INSERT INTO lk_rel2 VALUES (1)");
        }
        try (Connection a = open()) {
            a.setAutoCommit(false);
            a.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            assertEquals(Arrays.asList("1"), rows(a, "SELECT count(*) FROM lk_rel2"));

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch finished = new CountDownLatch(1);
            AtomicReference<String> error = new AtomicReference<>();
            Thread t = new Thread(() -> {
                try (Connection b = open()) {
                    started.countDown();
                    exec(b, "ALTER TABLE lk_rel2 ADD COLUMN z int");
                } catch (SQLException e) {
                    error.set(e.getSQLState());
                } finally {
                    finished.countDown();
                }
            });
            t.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertTrue(!finished.await(300, TimeUnit.MILLISECONDS),
                    "ALTER TABLE must wait behind the open reader's relation lock");

            a.rollback();
            assertTrue(finished.await(10, TimeUnit.SECONDS));
            t.join(5000);
            assertNull(error.get());
        }
        try (Connection c = open()) {
            assertNotNull(rows(c, "SELECT z FROM lk_rel2"));
        }
    }

    /** An INSERT only needs ROW EXCLUSIVE, which does not conflict with a reader. */
    @Test
    void openReaderDoesNotBlockConcurrentInsert() throws Exception {
        try (Connection c = open()) {
            exec(c, "CREATE TABLE lk_rel3 (id int PRIMARY KEY)");
        }
        try (Connection a = open()) {
            a.setAutoCommit(false);
            a.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            rows(a, "SELECT count(*) FROM lk_rel3");
            assertNull(otherSession("INSERT INTO lk_rel3 VALUES (1)"));
            a.rollback();
        }
    }
}
