package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Residual bug fixes from bugs-review.md.
 *
 * <p><b>C4 residual</b> — ATTACH PARTITION must validate that every parent column has a
 * same-named child column of the SAME type; PG raises 42804 (ERRCODE_DATATYPE_MISMATCH)
 * for a type mismatch, an extra child column, or a missing child column. Previously
 * memgres accepted a wrong-type column (then stored misrouted values) and reported the
 * column-set mismatches under 42P16 instead of 42804.
 *
 * <p><b>M7 residual</b> — under REPEATABLE READ, DELETE of a row that was concurrently
 * updated+committed by another transaction must raise 40001 ("could not serialize access
 * due to concurrent update"), exactly like the UPDATE path already does. READ COMMITTED
 * must still succeed.
 */
class PartitionRrResidualsTest {

    static Memgres memgres;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterAll
    static void tearDown() {
        if (memgres != null) memgres.close();
    }

    private Connection conn(int isolation) throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(false);
        c.setTransactionIsolation(isolation);
        return c;
    }

    private Connection autoConn() throws SQLException {
        Connection c = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        c.setAutoCommit(true);
        return c;
    }

    // =========================================================================
    // C4 residual: ATTACH PARTITION per-column type + column-set validation
    // =========================================================================

    @Test
    void attach_partition_with_different_column_type_rejected_42804() throws SQLException {
        try (Connection c = autoConn(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS c4_parent");
            s.execute("DROP TABLE IF EXISTS c4_badtype");
            s.execute("CREATE TABLE c4_parent (id int, name text) PARTITION BY LIST(id)");
            // 'name' is int here, but text in the parent
            s.execute("CREATE TABLE c4_badtype (id int, name int)");

            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("ALTER TABLE c4_parent ATTACH PARTITION c4_badtype FOR VALUES IN (1)"));
            assertEquals("42804", ex.getSQLState(),
                    "wrong-type column ATTACH must raise 42804; got " + ex.getSQLState());
            assertTrue(ex.getMessage().toLowerCase().contains("different type")
                            && ex.getMessage().contains("name"),
                    "message should mention different type for column name: " + ex.getMessage());

            // The bad table must NOT have become a partition: with the attach rejected,
            // the parent has no partition covering id=1, so the insert has nowhere to
            // route (proving no misrouting into c4_badtype).
            SQLException noPart = assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO c4_parent VALUES (1, 'ok')"));
            assertTrue(noPart.getMessage().toLowerCase().contains("no partition"),
                    "insert should fail: bad table was not attached — " + noPart.getMessage());
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM c4_badtype")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "attach must have been rejected — no rows routed to bad table");
            }
            s.execute("DROP TABLE IF EXISTS c4_parent");
            s.execute("DROP TABLE IF EXISTS c4_badtype");
        }
    }

    @Test
    void attach_partition_with_extra_column_rejected_42804() throws SQLException {
        try (Connection c = autoConn(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS c4_parent2");
            s.execute("DROP TABLE IF EXISTS c4_extra");
            s.execute("CREATE TABLE c4_parent2 (id int, name text) PARTITION BY LIST(id)");
            s.execute("CREATE TABLE c4_extra (id int, name text, junk int)");

            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("ALTER TABLE c4_parent2 ATTACH PARTITION c4_extra FOR VALUES IN (1)"));
            assertEquals("42804", ex.getSQLState(),
                    "extra-column ATTACH must raise 42804; got " + ex.getSQLState());
            assertTrue(ex.getMessage().contains("junk"), ex.getMessage());
            s.execute("DROP TABLE IF EXISTS c4_parent2");
            s.execute("DROP TABLE IF EXISTS c4_extra");
        }
    }

    @Test
    void attach_partition_with_missing_column_rejected_42804() throws SQLException {
        try (Connection c = autoConn(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS c4_parent3");
            s.execute("DROP TABLE IF EXISTS c4_missing");
            s.execute("CREATE TABLE c4_parent3 (id int, name text) PARTITION BY LIST(id)");
            s.execute("CREATE TABLE c4_missing (id int)");

            SQLException ex = assertThrows(SQLException.class, () ->
                    s.execute("ALTER TABLE c4_parent3 ATTACH PARTITION c4_missing FOR VALUES IN (1)"));
            assertEquals("42804", ex.getSQLState(),
                    "missing-column ATTACH must raise 42804; got " + ex.getSQLState());
            assertTrue(ex.getMessage().toLowerCase().contains("missing column"), ex.getMessage());
            s.execute("DROP TABLE IF EXISTS c4_parent3");
            s.execute("DROP TABLE IF EXISTS c4_missing");
        }
    }

    @Test
    void attach_partition_with_matching_columns_succeeds() throws SQLException {
        try (Connection c = autoConn(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS c4_parent4");
            s.execute("DROP TABLE IF EXISTS c4_good");
            s.execute("CREATE TABLE c4_parent4 (id int, name text) PARTITION BY LIST(id)");
            s.execute("CREATE TABLE c4_good (id int, name text)");
            s.execute("ALTER TABLE c4_parent4 ATTACH PARTITION c4_good FOR VALUES IN (1)");
            // Routing works and the row lands in the child.
            s.execute("INSERT INTO c4_parent4 VALUES (1, 'ok')");
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM c4_good")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            s.execute("DROP TABLE IF EXISTS c4_parent4");
        }
    }

    @Test
    void attach_partition_serial_parent_int_child_is_compatible() throws SQLException {
        // serial is not a real type; its stored type is int4, so an int child column
        // must be accepted (no false positive from the type check). serial also carries
        // NOT NULL, and PostgreSQL requires the child to repeat every NOT NULL the parent
        // has, so the child column is declared NOT NULL as well.
        try (Connection c = autoConn(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS c4_ser_parent");
            s.execute("DROP TABLE IF EXISTS c4_ser_child");
            s.execute("CREATE TABLE c4_ser_parent (id serial, name text) PARTITION BY LIST(id)");
            s.execute("CREATE TABLE c4_ser_child (id int NOT NULL, name text)");
            s.execute("ALTER TABLE c4_ser_parent ATTACH PARTITION c4_ser_child FOR VALUES IN (1)");
            // the attach really took, so routing reaches the child
            s.execute("INSERT INTO c4_ser_parent (id, name) VALUES (1, 'ok')");
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM c4_ser_child")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            s.execute("DROP TABLE IF EXISTS c4_ser_parent");
        }
    }

    @Test
    void attach_partition_nullable_child_of_not_null_parent_is_refused() throws SQLException {
        // PostgreSQL 18: a child whose column is nullable cannot be attached under a parent
        // column that is NOT NULL, because the partition would admit rows the parent forbids.
        try (Connection c = autoConn(); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS c4_nn_parent");
            s.execute("DROP TABLE IF EXISTS c4_nn_child");
            s.execute("CREATE TABLE c4_nn_parent (id serial, name text) PARTITION BY LIST(id)");
            s.execute("CREATE TABLE c4_nn_child (id int, name text)");
            SQLException e = assertThrows(SQLException.class, () ->
                    s.execute("ALTER TABLE c4_nn_parent ATTACH PARTITION c4_nn_child FOR VALUES IN (1)"));
            assertEquals("42804", e.getSQLState());
            assertTrue(e.getMessage().contains(
                    "column \"id\" in child table \"c4_nn_child\" must be marked NOT NULL"), e.getMessage());
            s.execute("DROP TABLE IF EXISTS c4_nn_child");
            s.execute("DROP TABLE IF EXISTS c4_nn_parent");
        }
    }

    // =========================================================================
    // M7 residual: RR DELETE-after-concurrent-update must raise 40001
    // =========================================================================

    @Test
    void repeatable_read_delete_after_concurrent_update_raises_40001() throws SQLException {
        try (Connection setup = autoConn(); Statement s = setup.createStatement()) {
            s.execute("DROP TABLE IF EXISTS m7_rr");
            s.execute("CREATE TABLE m7_rr (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO m7_rr VALUES (1, 0)");
        }
        try (Connection a = conn(Connection.TRANSACTION_REPEATABLE_READ);
             Connection b = conn(Connection.TRANSACTION_REPEATABLE_READ)) {
            // A takes its snapshot by reading the row first.
            try (ResultSet rs = a.createStatement().executeQuery("SELECT v FROM m7_rr WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
            // B updates the same row and commits.
            b.createStatement().execute("UPDATE m7_rr SET v = 99 WHERE id = 1");
            b.commit();
            // A now DELETEs the concurrently-updated row → must serialize-fail.
            SQLException ex = assertThrows(SQLException.class, () -> {
                a.createStatement().execute("DELETE FROM m7_rr WHERE id = 1");
                a.commit();
            });
            assertEquals("40001", ex.getSQLState(),
                    "RR DELETE after concurrent committed update must raise 40001; got " + ex.getSQLState());
            a.rollback();
        }
        try (Connection cleanup = autoConn(); Statement s = cleanup.createStatement()) {
            s.execute("DROP TABLE IF EXISTS m7_rr");
        }
    }

    @Test
    void read_committed_delete_after_concurrent_update_succeeds() throws SQLException {
        try (Connection setup = autoConn(); Statement s = setup.createStatement()) {
            s.execute("DROP TABLE IF EXISTS m7_rc");
            s.execute("CREATE TABLE m7_rc (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO m7_rc VALUES (1, 0)");
        }
        try (Connection a = conn(Connection.TRANSACTION_READ_COMMITTED);
             Connection b = conn(Connection.TRANSACTION_READ_COMMITTED)) {
            try (ResultSet rs = a.createStatement().executeQuery("SELECT v FROM m7_rc WHERE id = 1")) {
                assertTrue(rs.next());
            }
            b.createStatement().execute("UPDATE m7_rc SET v = 99 WHERE id = 1");
            b.commit();
            // Under READ COMMITTED the DELETE must succeed (no serialization failure,
            // no over-application of the RR conflict check).
            a.createStatement().execute("DELETE FROM m7_rc WHERE id = 1");
            a.commit();
        }
        try (Connection check = autoConn(); Statement s = check.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM m7_rc")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "READ COMMITTED delete should have removed the row");
            s.execute("DROP TABLE IF EXISTS m7_rc");
        }
    }

    @Test
    void repeatable_read_delete_without_concurrent_change_succeeds() throws SQLException {
        // Guard against over-application: an RR DELETE of a row nobody else touched
        // must not spuriously raise 40001.
        try (Connection setup = autoConn(); Statement s = setup.createStatement()) {
            s.execute("DROP TABLE IF EXISTS m7_rr_clean");
            s.execute("CREATE TABLE m7_rr_clean (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO m7_rr_clean VALUES (1, 0), (2, 0)");
        }
        try (Connection a = conn(Connection.TRANSACTION_REPEATABLE_READ)) {
            try (ResultSet rs = a.createStatement().executeQuery("SELECT count(*) FROM m7_rr_clean")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            a.createStatement().execute("DELETE FROM m7_rr_clean WHERE id = 1");
            a.commit();
        }
        try (Connection check = autoConn(); Statement s = check.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM m7_rr_clean")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            s.execute("DROP TABLE IF EXISTS m7_rr_clean");
        }
    }
}
