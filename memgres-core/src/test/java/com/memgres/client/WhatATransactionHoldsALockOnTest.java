package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a transaction holds a lock on, and when it is given an identifier to hold one over.
 *
 * <p>A read takes an AccessShareLock on each relation it opens, and a catalogue relation is a
 * relation like any other — but the locks were noted only for relations the database stores, so
 * a query asking pg_locks what it held was the one read that went unrecorded, and answered
 * without the lock it was taking to ask.
 *
 * <p>A transaction identifier is not handed out at BEGIN. PostgreSQL gives one when the
 * transaction first writes — a row, a catalogue row, or an outright request through
 * txid_current() — and a transaction that only reads never gets one, so it holds no lock over an
 * identifier either. Reported from the transaction being open alone, every read-only transaction
 * claimed a lock over an identifier nothing had assigned. ANALYZE turns on what it wrote rather
 * than on having run: over a relation with no rows it writes no statistics and stays read-only.
 */
class WhatATransactionHoldsALockOnTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void freshTransactionState() throws SQLException {
        conn.setAutoCommit(true);
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    if (c > 1) row.append('/');
                    row.append(rs.getString(c));
                }
                out.add(row.toString());
            }
            return out;
        }
    }

    private static String heldTransactionIds() throws SQLException {
        return one("SELECT count(*)::int FROM pg_locks WHERE locktype='transactionid'");
    }

    /** A query reading a catalogue relation holds a lock on it, and can see that it does. */
    @Test
    void aReadOfACatalogueRelationHoldsALockOnIt() throws SQLException {
        exec("BEGIN");
        try {
            assertEquals(List.of("pg_locks/AccessShareLock"),
                    rows("SELECT relation::regclass::text, mode FROM pg_locks"
                            + " WHERE locktype='relation' ORDER BY 1"));
        } finally {
            exec("COMMIT");
        }
    }

    /** A read of a stored relation takes AccessShareLock, and FOR UPDATE takes a stronger one. */
    @Test
    void aReadTakesAccessShareAndALockingReadTakesMore() throws SQLException {
        exec("CREATE TABLE zlt_t (a int)");
        exec("INSERT INTO zlt_t VALUES (1)");
        try {
            exec("BEGIN");
            assertEquals("0", one("SELECT count(*)::int FROM zlt_t WHERE a = 99"));
            assertEquals("AccessShareLock", one("SELECT mode FROM pg_locks"
                    + " WHERE locktype='relation' AND relation='zlt_t'::regclass"));
            exec("COMMIT");
            exec("BEGIN");
            exec("SELECT a FROM zlt_t FOR UPDATE");
            assertEquals("RowShareLock", one("SELECT mode FROM pg_locks"
                    + " WHERE locktype='relation' AND relation='zlt_t'::regclass"));
            exec("COMMIT");
            // The locks a transaction took go with it, so nothing is held once it has ended.
            assertEquals("0", one("SELECT count(*)::int FROM pg_locks"
                    + " WHERE locktype='relation' AND relation='zlt_t'::regclass"));
        } finally {
            exec("COMMIT");
            exec("DROP TABLE zlt_t");
        }
    }

    /** A transaction that has only read holds no lock over an identifier of its own. */
    @Test
    void aReadOnlyTransactionHoldsNoTransactionIdLock() throws SQLException {
        exec("CREATE TABLE zlt_r (a int)");
        try {
            exec("BEGIN");
            assertEquals("0", heldTransactionIds());
            assertEquals("0", one("SELECT count(*)::int FROM zlt_r"));
            assertEquals("0", heldTransactionIds());
            // Asking for the identifier is enough to be given one.
            assertEquals("t", one("SELECT txid_current() > 0"));
            assertEquals("1", heldTransactionIds());
        } finally {
            exec("COMMIT");
            exec("DROP TABLE zlt_r");
        }
    }

    /** Writing a row gives the transaction an identifier; a stronger lock goes with the write. */
    @Test
    void aWriteGivesTheTransactionAnIdentifier() throws SQLException {
        exec("CREATE TABLE zlt_w (a int)");
        try {
            exec("BEGIN");
            assertEquals("0", heldTransactionIds());
            exec("INSERT INTO zlt_w VALUES (1)");
            assertEquals("1", heldTransactionIds());
            assertEquals("RowExclusiveLock", one("SELECT mode FROM pg_locks"
                    + " WHERE locktype='relation' AND relation='zlt_w'::regclass"));
        } finally {
            exec("COMMIT");
            exec("DROP TABLE zlt_w");
        }
    }

    /** A statement that writes a catalogue row gives one too, and one that does not does not. */
    @Test
    void writingTheCatalogueGivesTheTransactionAnIdentifier() throws SQLException {
        exec("CREATE TABLE zlt_c (a int)");
        try {
            exec("BEGIN");
            exec("SET work_mem = '4MB'");
            assertEquals("0", heldTransactionIds());
            exec("LOCK TABLE zlt_c IN ACCESS SHARE MODE");
            assertEquals("0", heldTransactionIds());
            exec("COMMENT ON TABLE zlt_c IS 'a comment is a catalogue row'");
            assertEquals("1", heldTransactionIds());
            exec("COMMIT");
            exec("BEGIN");
            assertEquals("0", heldTransactionIds());
            exec("GRANT SELECT ON zlt_c TO PUBLIC");
            assertEquals("1", heldTransactionIds());
        } finally {
            exec("COMMIT");
            exec("DROP TABLE zlt_c CASCADE");
        }
    }

    /** ANALYZE writes statistics or it does not, and the identifier follows what it wrote. */
    @Test
    void analyzeGivesAnIdentifierOnlyWhereItWroteStatistics() throws SQLException {
        exec("CREATE TABLE zlt_e (a int)");
        exec("CREATE TABLE zlt_f (a int)");
        exec("INSERT INTO zlt_f VALUES (1)");
        try {
            exec("BEGIN");
            exec("ANALYZE zlt_e");
            assertEquals("0", heldTransactionIds());
            exec("COMMIT");
            exec("BEGIN");
            exec("ANALYZE zlt_f");
            assertEquals("1", heldTransactionIds());
            exec("COMMIT");
            assertEquals("0", one("SELECT count(*)::int FROM pg_statistic"
                    + " WHERE starelid='zlt_e'::regclass"));
            assertEquals("1", one("SELECT count(*)::int FROM pg_statistic"
                    + " WHERE starelid='zlt_f'::regclass"));
        } finally {
            exec("COMMIT");
            exec("DROP TABLE zlt_e, zlt_f");
        }
    }

    /** A session always holds a lock on its own virtual transaction. */
    @Test
    void aSessionHoldsALockOnItsOwnVirtualTransaction() throws SQLException {
        exec("BEGIN");
        try {
            assertEquals("1", one("SELECT count(*)::int FROM pg_locks"
                    + " WHERE locktype='virtualxid'"));
        } finally {
            exec("COMMIT");
        }
    }
}
