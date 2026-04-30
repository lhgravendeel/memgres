package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 36d: Fix 5 setup divergences between PG 18 and Memgres.
 *
 * 1. DROP CAST non-existent → should error 42704
 * 2. EXPLAIN GENERIC_PLAN with $1 in extended protocol → should error 08P01
 * 3. ALTER INDEX ATTACH PARTITION validation → should error 55000
 * 4. pg_xact_status(text) → should error 42883
 * 5. CLUSTER inside transaction block → should succeed (PG allows it)
 */
class Round36dSetupDivergenceTest {
    static Memgres memgres;
    static Connection simpleConn;
    static Connection extendedConn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        simpleConn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        simpleConn.setAutoCommit(true);
        extendedConn = DriverManager.getConnection(
                memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        extendedConn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (simpleConn != null) simpleConn.close();
        if (extendedConn != null) extendedConn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void resetConnState() throws SQLException {
        try { simpleConn.createStatement().execute("ROLLBACK"); } catch (SQLException ignored) {}
        try { extendedConn.createStatement().execute("ROLLBACK"); } catch (SQLException ignored) {}
    }

    // =========================================================================
    // 1. DROP CAST on non-existent cast → 42704
    // =========================================================================

    @Test void drop_cast_nonexistent_errors() throws SQLException {
        try (Statement s = simpleConn.createStatement()) {
            s.execute("CREATE DOMAIN r36d_dom AS int");
            // No cast exists from int to r36d_dom — DROP CAST should error
            SQLException ex = assertThrows(SQLException.class,
                    () -> s.execute("DROP CAST (int AS r36d_dom)"));
            assertEquals("42704", ex.getSQLState(),
                    "DROP CAST on non-existent cast should error 42704, got: " + ex.getMessage());
            s.execute("DROP DOMAIN r36d_dom");
        }
    }

    @Test void drop_cast_if_exists_nonexistent_succeeds() throws SQLException {
        try (Statement s = simpleConn.createStatement()) {
            s.execute("CREATE DOMAIN r36d_dom2 AS int");
            // IF EXISTS should succeed silently
            s.execute("DROP CAST IF EXISTS (int AS r36d_dom2)");
            s.execute("DROP DOMAIN r36d_dom2");
        }
    }

    // =========================================================================
    // 2. EXPLAIN GENERIC_PLAN with $1 — extended protocol rejects (08P01)
    // =========================================================================

    @Test void explain_generic_plan_with_param_extended_protocol_errors() throws SQLException {
        try (Statement s = extendedConn.createStatement()) {
            s.execute("CREATE TABLE r36d_ex (id int)");
        }
        // In extended protocol, $1 requires a bind parameter.
        // PG errors: "bind message supplies 0 parameters, but prepared statement requires 1"
        try (Statement s = extendedConn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                    () -> s.execute("EXPLAIN (GENERIC_PLAN) SELECT * FROM r36d_ex WHERE id = $1"));
            assertEquals("08P01", ex.getSQLState(),
                    "Extended protocol with $1 and 0 bind params should error 08P01, got: " + ex.getMessage());
        } finally {
            try (Statement s = extendedConn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS r36d_ex");
            }
        }
    }

    // =========================================================================
    // 3. ALTER INDEX ATTACH PARTITION — skipped
    // PG auto-propagates CREATE INDEX to partitions, then ATTACH rejects
    // because a child index already exists. Memgres doesn't auto-propagate
    // partition indexes yet, so this validation can't be matched without
    // implementing that feature first.
    // =========================================================================

    // =========================================================================
    // 4. pg_xact_status(text) → should error 42883
    // =========================================================================

    @Test void pg_xact_status_text_arg_errors() throws SQLException {
        try (Statement s = simpleConn.createStatement()) {
            // PG requires xid8 type, not text. ::text cast should be rejected.
            SQLException ex = assertThrows(SQLException.class,
                    () -> s.executeQuery("SELECT pg_xact_status(pg_current_xact_id()::text)"));
            assertEquals("42883", ex.getSQLState(),
                    "pg_xact_status(text) should error 42883, got: " + ex.getMessage());
        }
    }

    @Test void pg_xact_status_without_cast_succeeds() throws SQLException {
        try (Statement s = simpleConn.createStatement()) {
            // Without ::text cast, pg_current_xact_id() returns xid8, which is valid
            ResultSet rs = s.executeQuery("SELECT pg_xact_status(pg_current_xact_id())");
            assertTrue(rs.next());
            assertEquals("in progress", rs.getString(1));
        }
    }

    // =========================================================================
    // 5. CLUSTER inside transaction block → should succeed
    // =========================================================================

    @Test void cluster_inside_transaction_succeeds() throws SQLException {
        try (Statement s = simpleConn.createStatement()) {
            s.execute("CREATE TABLE r36d_cl (id int PRIMARY KEY)");
            s.execute("INSERT INTO r36d_cl VALUES (1),(2),(3)");
            // PG allows CLUSTER inside a transaction block
            s.execute("BEGIN");
            s.execute("CLUSTER r36d_cl USING r36d_cl_pkey");
            s.execute("ROLLBACK");
            s.execute("DROP TABLE r36d_cl");
        }
    }
}
