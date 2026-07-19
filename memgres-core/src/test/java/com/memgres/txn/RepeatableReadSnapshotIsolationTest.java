package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that REPEATABLE READ / SERIALIZABLE snapshots never expose other
 * sessions' uncommitted changes (no dirty reads).
 *
 * Memgres mutates tables in place and tracks uncommitted changes per session.
 * The transaction-wide snapshot taken at the first statement of an RR
 * transaction must reconstruct the committed state of every table by
 * reverse-applying other sessions' uncommitted inserts/updates/deletes --
 * both for the table read by the first statement and for tables that are
 * only read by later statements in the same transaction.
 */
class RepeatableReadSnapshotIsolationTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() {
        if (memgres != null) memgres.close();
    }

    Connection connect() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        return c;
    }

    private void setupTables(String anchor, String target, String targetDdl, String targetInsert)
            throws SQLException {
        try (Connection setup = connect()) {
            Statement s = setup.createStatement();
            s.execute("DROP TABLE IF EXISTS " + anchor);
            s.execute("DROP TABLE IF EXISTS " + target);
            s.execute("CREATE TABLE " + anchor + " (id int PRIMARY KEY)");
            s.execute("INSERT INTO " + anchor + " VALUES (1)");
            s.execute(targetDdl);
            s.execute(targetInsert);
            setup.commit();
        }
    }

    private int queryInt(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private String queryString(Connection c, String sql) throws SQLException {
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    // -------------------------------------------------------------------------
    // (a) Uncommitted INSERT by another session is invisible
    // -------------------------------------------------------------------------

    @Test
    void uncommitted_insert_invisible_when_dirty_table_read_first() throws SQLException {
        setupTables("rrsi_a1", "rrsi_t1",
                "CREATE TABLE rrsi_t1 (id int PRIMARY KEY)",
                "INSERT INTO rrsi_t1 VALUES (1)");

        Connection writer = connect();
        Connection reader = connect();
        try {
            writer.createStatement().execute("INSERT INTO rrsi_t1 VALUES (999)");

            reader.createStatement().execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t1"),
                    "RR transaction must not see another session's uncommitted insert");
            reader.commit();
        } finally {
            writer.rollback();
            writer.close();
            reader.close();
        }
    }

    @Test
    void uncommitted_insert_invisible_when_dirty_table_read_after_snapshot() throws SQLException {
        setupTables("rrsi_a2", "rrsi_t2",
                "CREATE TABLE rrsi_t2 (id int PRIMARY KEY)",
                "INSERT INTO rrsi_t2 VALUES (1)");

        Connection writer = connect();
        Connection reader = connect();
        try {
            writer.createStatement().execute("INSERT INTO rrsi_t2 VALUES (999)");

            reader.createStatement().execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            // First statement reads a clean table: this takes the transaction-wide
            // snapshot of ALL tables, including rrsi_t2 which has a dirty row.
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_a2"));
            // Later statement reads the dirty table from the eager snapshot.
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t2"),
                    "Eager transaction-wide snapshot must not contain another session's uncommitted insert");
            reader.commit();
        } finally {
            writer.rollback();
            writer.close();
            reader.close();
        }
    }

    @Test
    void serializable_also_does_not_see_uncommitted_insert() throws SQLException {
        setupTables("rrsi_a3", "rrsi_t3",
                "CREATE TABLE rrsi_t3 (id int PRIMARY KEY)",
                "INSERT INTO rrsi_t3 VALUES (1)");

        Connection writer = connect();
        Connection reader = connect();
        try {
            writer.createStatement().execute("INSERT INTO rrsi_t3 VALUES (999)");

            reader.createStatement().execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_a3"));
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t3"));
            reader.commit();
        } finally {
            writer.rollback();
            writer.close();
            reader.close();
        }
    }

    // -------------------------------------------------------------------------
    // (b) Uncommitted UPDATE by another session shows the OLD committed value
    // -------------------------------------------------------------------------

    @Test
    void uncommitted_update_shows_old_value_in_eager_snapshot() throws SQLException {
        setupTables("rrsi_a4", "rrsi_t4",
                "CREATE TABLE rrsi_t4 (id int PRIMARY KEY, v text)",
                "INSERT INTO rrsi_t4 VALUES (1, 'committed')");

        Connection writer = connect();
        Connection reader = connect();
        try {
            writer.createStatement().execute("UPDATE rrsi_t4 SET v = 'dirty' WHERE id = 1");

            reader.createStatement().execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            // First read a clean table so rrsi_t4 enters the snapshot eagerly.
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_a4"));
            assertEquals("committed", queryString(reader, "SELECT v FROM rrsi_t4 WHERE id = 1"),
                    "RR snapshot must show the old committed value of a row another session is updating");
            reader.commit();
        } finally {
            writer.rollback();
            writer.close();
            reader.close();
        }
    }

    // -------------------------------------------------------------------------
    // (c) Uncommitted DELETE by another session: row still visible
    // -------------------------------------------------------------------------

    @Test
    void uncommitted_delete_still_shows_row_in_eager_snapshot() throws SQLException {
        setupTables("rrsi_a5", "rrsi_t5",
                "CREATE TABLE rrsi_t5 (id int PRIMARY KEY)",
                "INSERT INTO rrsi_t5 VALUES (1)");

        Connection writer = connect();
        Connection reader = connect();
        try {
            writer.createStatement().execute("DELETE FROM rrsi_t5 WHERE id = 1");

            reader.createStatement().execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_a5"));
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t5"),
                    "RR snapshot must still contain a row another session has deleted but not committed");
            reader.commit();
        } finally {
            writer.rollback();
            writer.close();
            reader.close();
        }
    }

    // -------------------------------------------------------------------------
    // (d) The session's OWN uncommitted changes remain visible to itself
    // -------------------------------------------------------------------------

    @Test
    void own_uncommitted_changes_visible_to_self() throws SQLException {
        setupTables("rrsi_a6", "rrsi_t6",
                "CREATE TABLE rrsi_t6 (id int PRIMARY KEY, v text)",
                "INSERT INTO rrsi_t6 VALUES (1, 'orig')");

        Connection conn = connect();
        try {
            conn.createStatement().execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            // Take the snapshot first
            assertEquals(1, queryInt(conn, "SELECT count(*) FROM rrsi_a6"));
            conn.createStatement().execute("INSERT INTO rrsi_t6 VALUES (2, 'mine')");
            conn.createStatement().execute("UPDATE rrsi_t6 SET v = 'updated' WHERE id = 1");
            assertEquals(2, queryInt(conn, "SELECT count(*) FROM rrsi_t6"),
                    "A transaction must see its own uncommitted insert");
            assertEquals("updated", queryString(conn, "SELECT v FROM rrsi_t6 WHERE id = 1"),
                    "A transaction must see its own uncommitted update");
            conn.rollback();
        } finally {
            conn.close();
        }
    }

    // -------------------------------------------------------------------------
    // (e) Snapshot stability across another session's commit; new txn sees it
    // -------------------------------------------------------------------------

    @Test
    void snapshot_stable_after_other_commit_but_new_txn_sees_new_state() throws SQLException {
        setupTables("rrsi_a7", "rrsi_t7",
                "CREATE TABLE rrsi_t7 (id int PRIMARY KEY)",
                "INSERT INTO rrsi_t7 VALUES (1)");

        Connection writer = connect();
        Connection reader = connect();
        try {
            reader.createStatement().execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t7"));

            writer.createStatement().execute("INSERT INTO rrsi_t7 VALUES (2)");
            writer.commit();

            // Already-snapshotted RR transaction keeps seeing the old state
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t7"),
                    "RR transaction must keep its snapshot after another session commits");
            reader.commit();

            // A new RR transaction sees the committed new state
            reader.createStatement().execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals(2, queryInt(reader, "SELECT count(*) FROM rrsi_t7"),
                    "A new transaction must see state committed before it started");
            reader.commit();
        } finally {
            writer.close();
            reader.close();
        }
    }

    // -------------------------------------------------------------------------
    // Rollback after read: data seen never becomes wrong retroactively
    // -------------------------------------------------------------------------

    @Test
    void rollback_by_writer_after_reader_snapshot_keeps_reader_consistent() throws SQLException {
        setupTables("rrsi_a8", "rrsi_t8",
                "CREATE TABLE rrsi_t8 (id int PRIMARY KEY)",
                "INSERT INTO rrsi_t8 VALUES (1)");

        Connection writer = connect();
        Connection reader = connect();
        try {
            writer.createStatement().execute("INSERT INTO rrsi_t8 VALUES (999)");

            reader.createStatement().execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t8"),
                    "Uncommitted insert must be invisible before the writer rolls back");

            writer.rollback();

            // Still 1 after the rollback -- the reader never saw phantom data
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t8"));
            reader.commit();

            // And a fresh transaction also sees 1
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t8"));
            reader.commit();
        } finally {
            writer.close();
            reader.close();
        }
    }

    // -------------------------------------------------------------------------
    // READ COMMITTED must not dirty-read either
    // -------------------------------------------------------------------------

    @Test
    void read_committed_does_not_see_uncommitted_insert() throws SQLException {
        setupTables("rrsi_a9", "rrsi_t9",
                "CREATE TABLE rrsi_t9 (id int PRIMARY KEY, v text)",
                "INSERT INTO rrsi_t9 VALUES (1, 'committed')");

        Connection writer = connect();
        Connection reader = connect();
        try {
            writer.createStatement().execute("INSERT INTO rrsi_t9 VALUES (999, 'dirty')");
            writer.createStatement().execute("UPDATE rrsi_t9 SET v = 'dirty' WHERE id = 1");

            // Default READ COMMITTED
            assertEquals(1, queryInt(reader, "SELECT count(*) FROM rrsi_t9"),
                    "READ COMMITTED must not see another session's uncommitted insert");
            assertEquals("committed", queryString(reader, "SELECT v FROM rrsi_t9 WHERE id = 1"),
                    "READ COMMITTED must see the old committed value of an uncommitted update");
            reader.commit();

            // After the writer commits, READ COMMITTED sees the new state
            writer.commit();
            assertEquals(2, queryInt(reader, "SELECT count(*) FROM rrsi_t9"));
            assertEquals("dirty", queryString(reader, "SELECT v FROM rrsi_t9 WHERE id = 1"));
            reader.commit();
        } finally {
            writer.close();
            reader.close();
        }
    }
}
