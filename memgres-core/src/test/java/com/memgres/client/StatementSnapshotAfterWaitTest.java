package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A statement that waits for another transaction is judged on what it found, not on what it
 * waited for: PostgreSQL re-reads the qualification with the row's new version substituted and
 * everything else still coming from the snapshot the statement started with.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class StatementSnapshotAfterWaitTest {

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
            if (!rs.next()) return "(no rows)";
            String v = rs.getString(1);
            return v == null ? "(null)" : v;
        }
    }

    private static String rows(Connection c, String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                if (sb.length() > 0) sb.append(" / ");
                sb.append(rs.getString(1));
            }
        }
        return sb.length() == 0 ? "(no rows)" : sb.toString();
    }

    /** Run a write on another thread, reporting the row count it ends with. */
    private static Future<String> submitWrite(Connection c, String sql) {
        return pool.submit(() -> {
            try (Statement st = c.createStatement()) {
                return "n=" + st.executeUpdate(sql);
            } catch (SQLException e) {
                return "ERR:" + e.getSQLState();
            }
        });
    }

    private static Future<String> submitQuery(Connection c, String sql) {
        return pool.submit(() -> {
            try {
                return rows(c, sql);
            } catch (SQLException e) {
                return "ERR:" + e.getSQLState();
            }
        });
    }

    /** True while the statement is still waiting after two seconds. */
    private static boolean stillWaiting(Future<String> f) {
        try {
            f.get(2, TimeUnit.SECONDS);
            return false;
        } catch (TimeoutException e) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void threeRows(Connection c, String name) throws SQLException {
        exec(c, "DROP TABLE IF EXISTS " + name + " CASCADE");
        exec(c, "CREATE TABLE " + name + " (i int PRIMARY KEY, v int, s text)");
        exec(c, "INSERT INTO " + name + " VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')");
    }

    @Test
    void aBlockedUpdateReadsItsSubqueryFromTheSnapshotItStartedWith() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            threeRows(a, "zzw5f_bq");
            a.setAutoCommit(false);
            exec(a, "UPDATE zzw5f_bq SET v = 99 WHERE i = 1");
            Future<String> other = submitWrite(b,
                    "UPDATE zzw5f_bq SET s = 'q' WHERE i IN (SELECT i FROM zzw5f_bq WHERE v = 10)");
            assertTrue(stillWaiting(other), "the second update should wait for the first");
            a.commit();
            a.setAutoCommit(true);
            assertEquals("n=1", other.get(15, TimeUnit.SECONDS));
            assertEquals("1:99:q / 2:20:b / 3:30:c",
                    rows(a, "SELECT i||':'||v||':'||s FROM zzw5f_bq ORDER BY i"));
            exec(a, "DROP TABLE zzw5f_bq");
        }
    }

    @Test
    void aBlockedUpdateReadsACorrelatedExistsFromTheSnapshotItStartedWith() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            threeRows(a, "zzw5f_ex");
            a.setAutoCommit(false);
            exec(a, "UPDATE zzw5f_ex SET v = 99 WHERE i = 1");
            Future<String> other = submitWrite(b, "UPDATE zzw5f_ex SET s = 'q' WHERE EXISTS "
                    + "(SELECT 1 FROM zzw5f_ex t2 WHERE t2.i = zzw5f_ex.i AND t2.v = 10)");
            assertTrue(stillWaiting(other), "the second update should wait for the first");
            a.commit();
            a.setAutoCommit(true);
            assertEquals("n=1", other.get(15, TimeUnit.SECONDS));
            assertEquals("1:99:q / 2:20:b / 3:30:c",
                    rows(a, "SELECT i||':'||v||':'||s FROM zzw5f_ex ORDER BY i"));
            exec(a, "DROP TABLE zzw5f_ex");
        }
    }

    @Test
    void aBlockedUpdateStillJudgesTheRowItWaitedForOnItsNewValues() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            threeRows(a, "zzw5f_pq");
            a.setAutoCommit(false);
            exec(a, "UPDATE zzw5f_pq SET v = 99 WHERE i = 1");
            Future<String> other = submitWrite(b, "UPDATE zzw5f_pq SET s = 'q' WHERE v = 10");
            assertTrue(stillWaiting(other), "the second update should wait for the first");
            a.commit();
            a.setAutoCommit(true);
            // The row is the version the committed transaction left, and that version does not
            // answer the qualification any more.
            assertEquals("n=0", other.get(15, TimeUnit.SECONDS));
            assertEquals("1:99:a / 2:20:b / 3:30:c",
                    rows(a, "SELECT i||':'||v||':'||s FROM zzw5f_pq ORDER BY i"));
            // And when that transaction rolls back, the row never changed and is written.
            a.setAutoCommit(false);
            exec(a, "UPDATE zzw5f_pq SET v = 77 WHERE i = 2");
            Future<String> second = submitWrite(b, "UPDATE zzw5f_pq SET s = 'z' WHERE v = 20");
            assertTrue(stillWaiting(second), "the second update should wait for the first");
            a.rollback();
            a.setAutoCommit(true);
            assertEquals("n=1", second.get(15, TimeUnit.SECONDS));
            assertEquals("1:99:a / 2:20:z / 3:30:c",
                    rows(a, "SELECT i||':'||v||':'||s FROM zzw5f_pq ORDER BY i"));
            exec(a, "DROP TABLE zzw5f_pq");
        }
    }

    @Test
    void aBlockedDeleteReadsItsSubqueryFromTheSnapshotItStartedWith() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            threeRows(a, "zzw5f_dl");
            a.setAutoCommit(false);
            exec(a, "UPDATE zzw5f_dl SET v = 99 WHERE i = 1");
            Future<String> other = submitWrite(b,
                    "DELETE FROM zzw5f_dl WHERE i IN (SELECT i FROM zzw5f_dl WHERE v = 10)");
            assertTrue(stillWaiting(other), "the delete should wait for the update");
            a.commit();
            a.setAutoCommit(true);
            assertEquals("n=1", other.get(15, TimeUnit.SECONDS));
            assertEquals("2:20:b / 3:30:c",
                    rows(a, "SELECT i||':'||v||':'||s FROM zzw5f_dl ORDER BY i"));
            exec(a, "DROP TABLE zzw5f_dl");
        }
    }

    @Test
    void aBlockedSelectForUpdateReadsItsSubqueryFromTheSnapshotItStartedWith() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            threeRows(a, "zzw5f_fu");
            a.setAutoCommit(false);
            exec(a, "UPDATE zzw5f_fu SET v = 99 WHERE i = 1");
            b.setAutoCommit(false);
            Future<String> other = submitQuery(b, "SELECT i||':'||v||':'||s FROM zzw5f_fu "
                    + "WHERE i IN (SELECT i FROM zzw5f_fu WHERE v = 10) FOR UPDATE");
            assertTrue(stillWaiting(other), "the locking read should wait for the update");
            a.commit();
            a.setAutoCommit(true);
            // The row still answers the qualification, and it is answered with as the committed
            // transaction left it.
            assertEquals("1:99:a", other.get(15, TimeUnit.SECONDS));
            b.rollback();
            b.setAutoCommit(true);
            // A qualification the locked version no longer answers still drops the row.
            a.setAutoCommit(false);
            exec(a, "UPDATE zzw5f_fu SET v = 88 WHERE i = 2");
            b.setAutoCommit(false);
            Future<String> second = submitQuery(b,
                    "SELECT i||':'||v||':'||s FROM zzw5f_fu WHERE v = 20 FOR UPDATE");
            assertTrue(stillWaiting(second), "the locking read should wait for the update");
            a.commit();
            a.setAutoCommit(true);
            assertEquals("(no rows)", second.get(15, TimeUnit.SECONDS));
            b.rollback();
            b.setAutoCommit(true);
            exec(a, "DROP TABLE zzw5f_fu");
        }
    }

    @Test
    void aRowBeingUpdatedElsewhereKeepsTheTupleIdentityItAlwaysHad() throws Exception {
        try (Connection a = open(); Connection b = open()) {
            exec(a, "DROP TABLE IF EXISTS zzw5f_c8 CASCADE");
            exec(a, "CREATE TABLE zzw5f_c8 (id int PRIMARY KEY, v int)");
            exec(a, "INSERT INTO zzw5f_c8 VALUES (1,1)");
            assertEquals("(0,1)", scalar(b, "SELECT ctid::text FROM zzw5f_c8 WHERE id = 1"));
            a.setAutoCommit(false);
            exec(a, "UPDATE zzw5f_c8 SET v = v + 1 WHERE id = 1");
            // The reader is entitled to the version that write replaced, which lives where it
            // always did; the writer sees the version it wrote, at a place of its own.
            assertEquals("(0,1)", scalar(b, "SELECT ctid::text FROM zzw5f_c8 WHERE id = 1"));
            assertEquals("1", scalar(b, "SELECT v::text FROM zzw5f_c8 WHERE id = 1"));
            assertEquals("false", scalar(b, "SELECT (xmin = 0)::text FROM zzw5f_c8 WHERE id = 1"));
            assertEquals("(0,2)", scalar(a, "SELECT ctid::text FROM zzw5f_c8 WHERE id = 1"));
            a.rollback();
            a.setAutoCommit(true);
            assertEquals("(0,1)", scalar(b, "SELECT ctid::text FROM zzw5f_c8 WHERE id = 1"));
            // And once such a write commits, the row answers from where the new version lives.
            a.setAutoCommit(false);
            exec(a, "UPDATE zzw5f_c8 SET v = v + 1 WHERE id = 1");
            assertEquals("(0,1)", scalar(b, "SELECT ctid::text FROM zzw5f_c8 WHERE id = 1"));
            a.commit();
            a.setAutoCommit(true);
            assertEquals("(0,3)", scalar(b, "SELECT ctid::text FROM zzw5f_c8 WHERE id = 1"));
            exec(a, "DROP TABLE zzw5f_c8");
        }
    }
}