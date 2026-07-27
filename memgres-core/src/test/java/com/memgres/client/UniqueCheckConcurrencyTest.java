package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Whether a row that collides with another session's uncommitted insert is really a duplicate
 * depends on that transaction: if it rolls back, the key was never taken. Reporting a violation
 * straight away rejects an insert that should have succeeded, and tells the caller about data it
 * is not allowed to see. The check therefore waits — with the table lock released, because the
 * rollback it waits for needs that lock to remove the row.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class UniqueCheckConcurrencyTest {

    static Memgres memgres;
    static ExecutorService pool;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        pool = Executors.newCachedThreadPool();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (pool != null) pool.shutdownNow();
        if (memgres != null) memgres.close();
    }

    private static Connection open() throws SQLException {
        Connection c = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    private static void exec(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) { st.execute(sql); }
    }

    private static String scalar(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    /** Run a statement on another thread, reporting its SQLSTATE or "ok". */
    private static Future<String> submit(Connection c, String sql) {
        return pool.submit(() -> {
            try {
                exec(c, sql);
                return "ok";
            } catch (SQLException e) {
                return "ERR:" + e.getSQLState();
            }
        });
    }

    private static void freshTable(Connection c, String ddl, String name) throws SQLException {
        exec(c, "DROP TABLE IF EXISTS " + name + " CASCADE");
        exec(c, ddl);
    }

    @Test
    void insertWaitsWhileTheOtherTransactionIsOpen() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            freshTable(a, "CREATE TABLE ucT_a (i int PRIMARY KEY, v int)", "ucT_a");
            a.setAutoCommit(false);
            exec(a, "INSERT INTO ucT_a VALUES (9, 1)");
            Future<String> other = submit(b, "INSERT INTO ucT_a VALUES (9, 2)");
            // it must not decide anything while the first transaction is still open
            assertThrows(TimeoutException.class, () -> other.get(2, TimeUnit.SECONDS),
                    "the second insert should still be waiting");
            a.rollback();
            assertEquals("ok", other.get(15, TimeUnit.SECONDS));
            a.setAutoCommit(true);
            assertEquals("2", scalar(b, "SELECT v::text FROM ucT_a WHERE i = 9"));
        }
    }

    @Test
    void rollbackLetsTheWaitingInsertSucceed() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            freshTable(a, "CREATE TABLE ucT_b (i int PRIMARY KEY, v int)", "ucT_b");
            a.setAutoCommit(false);
            exec(a, "INSERT INTO ucT_b VALUES (9, 1)");
            Future<String> other = submit(b, "INSERT INTO ucT_b VALUES (9, 2)");
            Thread.sleep(300);
            a.rollback();
            assertEquals("ok", other.get(15, TimeUnit.SECONDS));
            a.setAutoCommit(true);
            assertEquals("1", scalar(b, "SELECT count(*)::text FROM ucT_b WHERE i = 9"));
            assertEquals("2", scalar(b, "SELECT v::text FROM ucT_b WHERE i = 9"));
        }
    }

    @Test
    void commitMakesTheWaitingInsertFail() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            freshTable(a, "CREATE TABLE ucT_c (i int PRIMARY KEY, v int)", "ucT_c");
            a.setAutoCommit(false);
            exec(a, "INSERT INTO ucT_c VALUES (9, 1)");
            Future<String> other = submit(b, "INSERT INTO ucT_c VALUES (9, 2)");
            Thread.sleep(300);
            a.commit();
            assertEquals("ERR:23505", other.get(15, TimeUnit.SECONDS));
            a.setAutoCommit(true);
            assertEquals("1", scalar(b, "SELECT v::text FROM ucT_c WHERE i = 9"));
        }
    }

    @Test
    void aUniqueConstraintBehavesLikeAPrimaryKey() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            freshTable(a, "CREATE TABLE ucT_d (i int, k int UNIQUE)", "ucT_d");
            a.setAutoCommit(false);
            exec(a, "INSERT INTO ucT_d VALUES (1, 5)");
            Future<String> other = submit(b, "INSERT INTO ucT_d VALUES (2, 5)");
            Thread.sleep(300);
            a.commit();
            assertEquals("ERR:23505", other.get(15, TimeUnit.SECONDS));
            a.setAutoCommit(true);
        }
    }

    @Test
    void distinctKeysDoNotWaitForEachOther() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            freshTable(a, "CREATE TABLE ucT_e (i int PRIMARY KEY, v int)", "ucT_e");
            a.setAutoCommit(false);
            exec(a, "INSERT INTO ucT_e VALUES (7, 1)");
            // a different key has nothing to wait for and must go straight through
            assertEquals("ok", submit(b, "INSERT INTO ucT_e VALUES (8, 2)").get(5, TimeUnit.SECONDS));
            a.rollback();
            a.setAutoCommit(true);
        }
    }

    @Test
    void nullKeysDoNotCollide() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            freshTable(a, "CREATE TABLE ucT_f (i int, k int UNIQUE)", "ucT_f");
            a.setAutoCommit(false);
            exec(a, "INSERT INTO ucT_f VALUES (1, NULL)");
            // NULLs are distinct, so this is not a conflict and must not wait
            assertEquals("ok", submit(b, "INSERT INTO ucT_f VALUES (2, NULL)").get(5, TimeUnit.SECONDS));
            a.rollback();
            a.setAutoCommit(true);
        }
    }

    @Test
    void twoSessionsWaitingOnEachOtherAreBroken() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            freshTable(a, "CREATE TABLE ucT_g (i int PRIMARY KEY)", "ucT_g");
            a.setAutoCommit(false);
            b.setAutoCommit(false);
            exec(a, "INSERT INTO ucT_g VALUES (1)");
            exec(b, "INSERT INTO ucT_g VALUES (2)");
            // each now wants the key the other is holding uncommitted
            Future<String> fa = submit(a, "INSERT INTO ucT_g VALUES (2)");
            Future<String> fb = submit(b, "INSERT INTO ucT_g VALUES (1)");
            String ra = fa.get(30, TimeUnit.SECONDS);
            String rb = fb.get(30, TimeUnit.SECONDS);
            assertTrue(ra.startsWith("ERR") || rb.startsWith("ERR"),
                    "one side must be aborted rather than both waiting forever, got "
                            + ra + " and " + rb);
            try { a.rollback(); } catch (SQLException ignored) { }
            try { b.rollback(); } catch (SQLException ignored) { }
            a.setAutoCommit(true);
            b.setAutoCommit(true);
        }
    }

    @Test
    void committedDuplicatesStillFailImmediately() throws Exception {
        try (Connection a = open()) {
            freshTable(a, "CREATE TABLE ucT_h (i int PRIMARY KEY, v int)", "ucT_h");
            exec(a, "INSERT INTO ucT_h VALUES (1, 10)");
            // nothing is in flight, so this must be reported without any wait
            long started = System.currentTimeMillis();
            assertEquals("ERR:23505", submit(a, "INSERT INTO ucT_h VALUES (1, 99)")
                    .get(5, TimeUnit.SECONDS));
            assertTrue(System.currentTimeMillis() - started < 3000,
                    "a committed duplicate should be reported without waiting");
        }
    }

    @Test
    void rolledBackRowsAreNotVisibleToOtherSessions() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            freshTable(a, "CREATE TABLE ucT_i (i int PRIMARY KEY, v int)", "ucT_i");
            a.setAutoCommit(false);
            exec(a, "INSERT INTO ucT_i VALUES (5, 50)");
            a.rollback();
            a.setAutoCommit(true);
            // the undo runs before the row stops being marked uncommitted, so there is no
            // window in which another session reads it as committed
            assertEquals("0", scalar(b, "SELECT count(*)::text FROM ucT_i WHERE i = 5"));
        }
    }
}
