package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category AG: Procedures, savepoints, two-phase commit.
 *
 * Covers:
 *  - CALL proc(..., OUT) returns OUT values
 *  - Savepoint name reuse emits NOTICE and releases prior savepoint
 *  - pg_prepared_xacts is queryable
 *  - PREPARE TRANSACTION / COMMIT PREPARED round-trip
 *  - max_prepared_transactions GUC exists
 */
class Round18ProceduresSavepointsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static int int1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // AG1. CALL proc(..., OUT) returns OUT value
    // =========================================================================

    @Test
    void call_procedure_returns_out_argument() throws SQLException {
        exec("DROP PROCEDURE IF EXISTS r18_outp(int, int)");
        exec("CREATE PROCEDURE r18_outp(IN a int, OUT b int) LANGUAGE plpgsql AS $$ BEGIN b := a * 2; END $$");
        try (CallableStatement cs = conn.prepareCall("{CALL r18_outp(?, ?)}")) {
            cs.setInt(1, 21);
            cs.registerOutParameter(2, Types.INTEGER);
            cs.execute();
            int out = cs.getInt(2);
            assertEquals(42, out,
                    "CALL r18_outp(21) must return OUT b=42; got " + out);
        }
    }

    // =========================================================================
    // AG2. Savepoint name reuse
    // =========================================================================

    @Test
    void savepoint_reuse_releases_previous() throws SQLException {
        conn.setAutoCommit(false);
        try {
            exec("DROP TABLE IF EXISTS r18_sp");
            exec("CREATE TABLE r18_sp(a int)");
            exec("INSERT INTO r18_sp VALUES (1)");
            exec("SAVEPOINT s1");
            exec("INSERT INTO r18_sp VALUES (2)");
            // Reuse name — PG releases the prior s1 then creates a new one.
            // After this the rollback should only undo data after the *new* s1.
            exec("SAVEPOINT s1");
            exec("INSERT INTO r18_sp VALUES (3)");
            exec("ROLLBACK TO SAVEPOINT s1");
            // Prior s1's inserts (1, 2) must persist; only 3 rolls back.
            int n = int1("SELECT count(*)::int FROM r18_sp");
            assertEquals(2, n,
                    "Savepoint reuse must preserve prior savepoint's inserts; got " + n);
            conn.rollback();
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // AG3. pg_prepared_xacts queryable
    // =========================================================================

    @Test
    void pg_prepared_xacts_queryable() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM pg_prepared_xacts")) {
            assertTrue(rs.next(),
                    "pg_prepared_xacts must be queryable (even if empty)");
        }
    }

    // =========================================================================
    // AG4. PREPARE TRANSACTION / COMMIT PREPARED round-trip
    // =========================================================================

    @Test
    void prepare_transaction_round_trip() throws SQLException {
        exec("DROP TABLE IF EXISTS r18_2pc");
        exec("CREATE TABLE r18_2pc(a int)");
        conn.setAutoCommit(false);
        try {
            exec("INSERT INTO r18_2pc VALUES (1)");
            exec("PREPARE TRANSACTION 'r18_pt1'");
            // After PREPARE TRANSACTION the session is no longer in a tx.
            conn.setAutoCommit(true);
            int n = int1("SELECT count(*)::int FROM pg_prepared_xacts WHERE gid='r18_pt1'");
            assertEquals(1, n,
                    "PREPARE TRANSACTION must register gid in pg_prepared_xacts; got " + n);
            exec("COMMIT PREPARED 'r18_pt1'");
            int committed = int1("SELECT count(*)::int FROM r18_2pc");
            assertEquals(1, committed,
                    "COMMIT PREPARED must persist the row; got " + committed);
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // AG5. max_prepared_transactions GUC present
    // =========================================================================

    @Test
    void max_prepared_transactions_guc_exists() throws SQLException {
        assertTrue(int1("SELECT count(*)::int FROM pg_settings WHERE name='max_prepared_transactions'") >= 1,
                "max_prepared_transactions GUC must appear in pg_settings");
    }
}
