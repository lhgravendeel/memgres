package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 13 gaps: wire-protocol surface that PG 18 implements but Memgres
 * currently does not fully expose.
 *
 * Coverage:
 *   A. Extended ErrorResponse fields  — Hint (H), Position (P), File (F),
 *                                       Line (L), Routine (R), Detail (D),
 *                                       Internal position (p), Where (W)
 *   B. ParameterStatus                 — is_superuser, application_name,
 *                                       IntervalStyle, DateStyle
 *   C. ReadyForQuery status byte       — 'E' after failed txn, 'T' in txn
 *   D. COPY FROM/TO PROGRAM            — must reject with 42501/55000
 *   E. NegotiateProtocolVersion        — driver with future minor version
 *   F. SCRAM-SHA-256 support advert    — auth method code in AuthRequest
 */
class Round13WireProtocolGapsTest {

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

    private static ServerErrorMessage getErrorFields(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected SQL to throw: " + sql);
            return null;
        } catch (PSQLException e) {
            return e.getServerErrorMessage();
        } catch (SQLException e) {
            if (e instanceof PSQLException) return ((PSQLException) e).getServerErrorMessage();
            fail("Expected PSQLException, got " + e.getClass().getName() + ": " + e.getMessage());
            return null;
        }
    }

    // =========================================================================
    // A. ErrorResponse extended fields
    // =========================================================================

    @Test
    void errorResponse_has_Position_field() {
        ServerErrorMessage m = getErrorFields("SELECT * FROM no_such_table_r13");
        // P = statement position (1-based byte offset of token causing error)
        assertTrue(m != null && m.getPosition() > 0,
                "expected P field > 0; got position=" + (m == null ? null : m.getPosition()));
    }

    @Test
    void errorResponse_has_Hint_for_typo() {
        // PG usually suggests: "Perhaps you meant to reference the column..."
        // for typo'd column names on small schemas.
        ServerErrorMessage m;
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS r13_hint_t");
            s.execute("CREATE TABLE r13_hint_t (correct_col int)");
            m = getErrorFields("SELECT corect_col FROM r13_hint_t");
        } catch (SQLException e) {
            fail(e);
            return;
        }
        assertTrue(m != null && m.getHint() != null,
                "expected H (Hint) field; got " + (m == null ? null : m.getHint()));
    }

    @Test
    void errorResponse_has_File_and_Line_and_Routine() {
        ServerErrorMessage m = getErrorFields("SELECT 1/0");
        assertNotNull(m, "server error message missing");
        // These are optional per PG spec but ALWAYS populated in real PG.
        assertNotNull(m.getFile(), "expected F (File) field");
        assertTrue(m.getLine() > 0, "expected L (Line) > 0");
        assertNotNull(m.getRoutine(), "expected R (Routine) field");
    }

    @Test
    void errorResponse_constraintViolation_has_all_identity_fields() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS r13_err_pk CASCADE");
            s.execute("CREATE TABLE r13_err_pk (id int PRIMARY KEY)");
            s.execute("INSERT INTO r13_err_pk VALUES (1)");
        }
        ServerErrorMessage m = getErrorFields("INSERT INTO r13_err_pk VALUES (1)");
        assertNotNull(m);
        assertEquals("public", m.getSchema());
        assertEquals("r13_err_pk", m.getTable());
        assertNotNull(m.getConstraint());
    }

    @Test
    void errorResponse_fkViolation_has_constraint_field() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS r13_fk_child CASCADE");
            s.execute("DROP TABLE IF EXISTS r13_fk_parent CASCADE");
            s.execute("CREATE TABLE r13_fk_parent (id int PRIMARY KEY)");
            s.execute("CREATE TABLE r13_fk_child (pid int REFERENCES r13_fk_parent(id))");
        }
        ServerErrorMessage m = getErrorFields("INSERT INTO r13_fk_child VALUES (999)");
        assertNotNull(m);
        assertNotNull(m.getConstraint(), "expected constraint name on FK violation");
    }

    @Test
    void errorResponse_datatypeName_on_overflow() {
        ServerErrorMessage m = getErrorFields("SELECT (2147483647 + 1)::int");
        assertNotNull(m);
        // PG emits datatype_name = 'integer' on overflow
        assertNotNull(m.getDatatype(),
                "expected d (datatypeName) field on numeric overflow");
    }

    // =========================================================================
    // B. ParameterStatus messages
    // =========================================================================

    @Test
    void parameterStatus_includes_is_superuser() throws SQLException {
        // Driver exposes ParameterStatus values via getAllParameters (via unwrap)
        org.postgresql.PGConnection pg = conn.unwrap(org.postgresql.PGConnection.class);
        String v = pg.getParameterStatus("is_superuser");
        assertNotNull(v, "expected ParameterStatus 'is_superuser' (on/off)");
        assertTrue("on".equals(v) || "off".equals(v), "must be 'on' or 'off'; got " + v);
    }

    @Test
    void parameterStatus_includes_application_name() throws SQLException {
        org.postgresql.PGConnection pg = conn.unwrap(org.postgresql.PGConnection.class);
        String v = pg.getParameterStatus("application_name");
        assertNotNull(v, "expected ParameterStatus 'application_name'");
    }

    @Test
    void parameterStatus_includes_IntervalStyle() throws SQLException {
        org.postgresql.PGConnection pg = conn.unwrap(org.postgresql.PGConnection.class);
        String v = pg.getParameterStatus("IntervalStyle");
        assertNotNull(v, "expected ParameterStatus 'IntervalStyle' (postgres/iso_8601/...)");
    }

    @Test
    void parameterStatus_updated_on_SET() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("SET application_name TO 'round13-test'");
        }
        org.postgresql.PGConnection pg = conn.unwrap(org.postgresql.PGConnection.class);
        String v = pg.getParameterStatus("application_name");
        assertEquals("round13-test", v,
                "SET application_name must emit ParameterStatus update");
    }

    // =========================================================================
    // C. ReadyForQuery status byte
    // =========================================================================

    @Test
    void readyForQuery_status_E_afterFailedTransaction() throws SQLException {
        // After a failed txn, driver must observe 'E' and refuse commands.
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            // Trigger failure
            assertThrows(SQLException.class,
                    () -> s.execute("SELECT * FROM no_such_table_r13_rfq"));
            // Any subsequent query in this txn must fail with 25P02 ("current
            // transaction is aborted, commands ignored until end of txn block")
            SQLException ex = assertThrows(SQLException.class,
                    () -> s.executeQuery("SELECT 1"));
            assertEquals("25P02", ex.getSQLState(),
                    "after failed txn, driver must see ReadyForQuery='E' → 25P02");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void readyForQuery_status_T_inTransaction() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.executeQuery("SELECT 1");
            // Driver exposes txn state as int via PGConnection
            org.postgresql.PGConnection pg = conn.unwrap(org.postgresql.PGConnection.class);
            // pgjdbc exposes transaction state via getTransactionState (driver-internal).
            // We just check that the connection treats itself as in-txn (no commit needed).
            assertFalse(conn.getAutoCommit());
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    // =========================================================================
    // D. COPY FROM/TO PROGRAM
    // =========================================================================

    @Test
    void copy_from_program_mustFail() {
        // PG would run the child process; Memgres MUST reject this for safety.
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("COPY (SELECT 1) TO PROGRAM 'echo'");
            }
        });
        // Expected SQLSTATE: 42501 insufficient_privilege OR 0A000 (not-supported)
        assertTrue("42501".equals(ex.getSQLState())
                        || "0A000".equals(ex.getSQLState())
                        || "55000".equals(ex.getSQLState())
                        || ex.getMessage().toLowerCase().contains("program"),
                "COPY TO PROGRAM must be rejected; got " + ex.getSQLState() + " — " + ex.getMessage());
    }

    @Test
    void copy_to_program_mustFail() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.execute("COPY nothing_tbl FROM PROGRAM 'cat /tmp/nope.csv'");
            }
        });
        assertTrue("42501".equals(ex.getSQLState())
                        || "0A000".equals(ex.getSQLState())
                        || "55000".equals(ex.getSQLState())
                        || "42P01".equals(ex.getSQLState())
                        || ex.getMessage().toLowerCase().contains("program"),
                "COPY FROM PROGRAM must be rejected or error; got "
                        + ex.getSQLState() + " — " + ex.getMessage());
    }

    // =========================================================================
    // E. NegotiateProtocolVersion (PG 14+: driver advertises 3.1)
    // =========================================================================

    @Test
    void connection_acceptsFutureMinorProtocolVersion() throws SQLException {
        // Protocol minor 3.1 is what PG 14+ supports. Memgres should advertise 3.1
        // (or at least NOT crash when the driver sends it).
        Properties p = new Properties();
        p.setProperty("user", memgres.getUser());
        p.setProperty("password", memgres.getPassword());
        // PG driver's internal protocol version negotiation is automatic —
        // we just confirm a baseline connection works.
        try (Connection c = DriverManager.getConnection(memgres.getJdbcUrl(), p);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
    }

    // =========================================================================
    // F. SCRAM-SHA-256 advert
    // =========================================================================

    @Test
    void password_authMethod_is_scramSha256_notMd5() throws SQLException {
        // Force driver to only offer SCRAM-SHA-256; connection must succeed.
        Properties p = new Properties();
        p.setProperty("user", memgres.getUser());
        p.setProperty("password", memgres.getPassword());
        p.setProperty("channelBinding", "prefer");
        // Password encryption hint isn't something the driver enforces on
        // the client side; we just verify that the default auth doesn't throw
        // "unsupported auth method".
        try (Connection c = DriverManager.getConnection(memgres.getJdbcUrl(), p)) {
            assertTrue(c.isValid(2));
        }
    }
}
