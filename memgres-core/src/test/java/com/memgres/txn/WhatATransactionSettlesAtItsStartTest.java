package com.memgres.txn;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a transaction settles when it begins, and what it will not settle again.
 *
 * <p>The level a transaction reads at is taken from the session's default as it begins, so a
 * default changed while it runs belongs to the transactions after it. Once it has read anything
 * the level cannot change — but asking again for the level it is already at asks for nothing, and
 * PostgreSQL allows that.
 *
 * <p>A SET LOCAL is for the transaction and no longer, {@code TO DEFAULT} included: the session's
 * own value comes back when the transaction ends.
 *
 * <p>An identifier is handed out when a transaction first writes, and a transaction that has only
 * read has none to answer with.
 */
class WhatATransactionSettlesAtItsStartTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** A setting made for one transaction is the session's again when it ends. */
    @Test
    void aSettingThatBelongsToOneTransaction() throws SQLException {
        exec("SET work_mem = '13MB'");
        exec("BEGIN");
        exec("SET LOCAL work_mem TO DEFAULT");
        assertEquals("4MB", one("SHOW work_mem"));
        exec("COMMIT");
        assertEquals("13MB", one("SHOW work_mem"));
        exec("BEGIN");
        exec("SET LOCAL work_mem = '9MB'");
        assertEquals("9MB", one("SHOW work_mem"));
        exec("COMMIT");
        assertEquals("13MB", one("SHOW work_mem"));
        exec("SET work_mem TO DEFAULT");
        assertEquals("4MB", one("SHOW work_mem"));
    }

    /** The level a transaction reads at was settled when it began. */
    @Test
    void theLevelATransactionBeganAt() throws SQLException {
        exec("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED");
        exec("BEGIN");
        exec("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        assertEquals("read committed", one("SHOW transaction_isolation"));
        exec("COMMIT");
        // The default it was changed to is the next transaction's.
        assertEquals("serializable", one("SHOW transaction_isolation"));
        exec("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED");
    }

    /** A level may not change once the transaction has read, and need not be asked to. */
    @Test
    void changingTheLevelAfterReading() throws SQLException {
        exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
        assertEquals("1", one("SELECT 1"));
        // The same level again asks for nothing at all.
        assertNull(stateOf("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"));
        assertNull(stateOf("BEGIN ISOLATION LEVEL REPEATABLE READ"));
        assertEquals("25001", stateOf("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
        exec("ROLLBACK");
        exec("BEGIN");
        assertEquals("1", one("SELECT 1"));
        assertEquals("25001", stateOf("BEGIN ISOLATION LEVEL REPEATABLE READ"));
        exec("ROLLBACK");
    }

    /** An identifier is handed out when a transaction first writes. */
    @Test
    void whenATransactionIsGivenAnIdentifier() throws SQLException {
        exec("CREATE TABLE zwt_x (i int)");
        exec("BEGIN");
        assertEquals("1", one("SELECT 1"));
        assertEquals("true", one("SELECT (pg_current_xact_id_if_assigned() IS NULL)::text"));
        exec("INSERT INTO zwt_x VALUES (1)");
        assertEquals("false", one("SELECT (pg_current_xact_id_if_assigned() IS NULL)::text"));
        assertEquals("true",
                one("SELECT (pg_current_xact_id_if_assigned() = pg_current_xact_id())::text"));
        exec("COMMIT");
        assertEquals("true", one("SELECT (pg_current_xact_id_if_assigned() IS NULL)::text"));
        exec("DROP TABLE zwt_x");
    }

    /** Whether a transaction may take another's snapshot is settled before the name is read. */
    @Test
    void takingAnotherTransactionsSnapshot() throws SQLException {
        exec("BEGIN");
        assertEquals("0A000", stateOf("SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"));
        exec("ROLLBACK");
        exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
        // A name spelled the way an exported snapshot is spelled named one that is not there.
        assertEquals("42704", stateOf("SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"));
        exec("ROLLBACK");
        exec("BEGIN ISOLATION LEVEL REPEATABLE READ");
        assertEquals("22023", stateOf("SET TRANSACTION SNAPSHOT 'zzz'"));
        exec("ROLLBACK");
    }
}
