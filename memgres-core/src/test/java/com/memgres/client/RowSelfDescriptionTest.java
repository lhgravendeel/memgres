package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a row says about itself is written when it happens.
 *
 * <p>A row's xmax names the transaction that has taken the row for its own, and taking a lock on it
 * is one of the ways that happens: PostgreSQL keeps the locker's id in the same field a deletion
 * writes, and leaves it there whatever becomes of the transaction. memgres wrote xmax on a delete
 * alone, so a row held under {@code SELECT ... FOR UPDATE} went on answering zero — the answer of a
 * row nobody has touched.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class RowSelfDescriptionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE zz_rs1 (id int, k int)");
            st.execute("INSERT INTO zz_rs1 VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void run(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String xmaxOf(int id) throws SQLException {
        return one("SELECT xmax::text FROM zz_rs1 WHERE id = " + id);
    }

    @Test
    void aRowNobodyHasTouchedNamesNoSecondTransaction() throws SQLException {
        assertEquals("0", xmaxOf(6));
    }

    @Test
    void aLockedRowNamesTheTransactionThatLockedIt() throws SQLException {
        run("BEGIN");
        try {
            assertEquals("1", one("SELECT id FROM zz_rs1 WHERE id = 1 FOR UPDATE"));
            assertEquals("t",
                    one("SELECT xmax::text = txid_current()::text FROM zz_rs1 WHERE id = 1"));
            // and only the row the lock reached
            assertEquals("0", xmaxOf(6));
        } finally {
            run("COMMIT");
        }
        // the mark stays after the transaction that made it has finished
        assertNotEquals("0", xmaxOf(1));
    }

    @Test
    void everyLockModeWritesItAndARollbackDoesNotTakeItBack() throws SQLException {
        run("BEGIN");
        try {
            run("SELECT id FROM zz_rs1 WHERE id = 2 FOR NO KEY UPDATE");
            assertEquals("t",
                    one("SELECT xmax::text = txid_current()::text FROM zz_rs1 WHERE id = 2"));
        } finally {
            run("ROLLBACK");
        }
        assertNotEquals("0", xmaxOf(2));

        run("BEGIN");
        try {
            run("SELECT id FROM zz_rs1 WHERE id = 3 FOR SHARE");
            assertEquals("t",
                    one("SELECT xmax::text = txid_current()::text FROM zz_rs1 WHERE id = 3"));
        } finally {
            run("COMMIT");
        }

        run("BEGIN");
        try {
            run("SELECT id FROM zz_rs1 WHERE id = 4 FOR KEY SHARE");
            assertEquals("t",
                    one("SELECT xmax::text = txid_current()::text FROM zz_rs1 WHERE id = 4"));
        } finally {
            run("COMMIT");
        }
    }

    @Test
    void aLockThatReachesNoRowWritesNothing() throws SQLException {
        run("BEGIN");
        try {
            run("SELECT id FROM zz_rs1 WHERE id = 99 FOR UPDATE");
        } finally {
            run("COMMIT");
        }
        assertEquals("0", xmaxOf(6));
    }

    @Test
    void anUpdateWritesAVersionNobodyHasLockedYet() throws SQLException {
        run("BEGIN");
        try {
            run("UPDATE zz_rs1 SET k = k + 1 WHERE id = 5");
            assertEquals("0", xmaxOf(5));
        } finally {
            run("COMMIT");
        }
        assertEquals("0", xmaxOf(5));
    }

    @Test
    void aCommandCounterAdvancesWithinATransaction() throws SQLException {
        run("CREATE TABLE zz_rs2 (id int)");
        run("BEGIN");
        try {
            run("INSERT INTO zz_rs2 VALUES (10)");
            run("INSERT INTO zz_rs2 VALUES (11)");
            assertEquals("2", one(
                    "SELECT count(DISTINCT cmin::text) FROM zz_rs2 WHERE id IN (10,11)"));
        } finally {
            run("COMMIT");
        }
        assertEquals("2",
                one("SELECT count(DISTINCT cmin::text) FROM zz_rs2 WHERE id IN (10,11)"));
        run("DROP TABLE zz_rs2");
    }

    @Test
    void aLinePointerStartsAgainWhereTheRelationDoes() throws SQLException {
        run("CREATE TABLE zz_rs3 (id int)");
        run("INSERT INTO zz_rs3 VALUES (1),(2),(3)");
        assertEquals("(0,1)", one("SELECT ctid::text FROM zz_rs3 ORDER BY id"));
        run("TRUNCATE zz_rs3");
        run("INSERT INTO zz_rs3 VALUES (7),(8)");
        assertEquals("(0,1)", one("SELECT ctid::text FROM zz_rs3 ORDER BY id"));
        assertEquals("(0,2)", one("SELECT ctid::text FROM zz_rs3 WHERE id = 8"));
        run("DROP TABLE zz_rs3");
    }
}
