package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Round 18 gap category AC: Wire-protocol ErrorResponse / NoticeResponse / tx-status fields.
 *
 * Covers:
 *  - ServerErrorMessage severity (V field), sqlState (C), detail (D), hint (H)
 *  - file (F), line (L), routine (R)
 *  - position (P)
 *  - tx-status byte 'T'/'E' reflected when inside txn / after error
 *  - pg_stat_activity.query_id non-null when enabled
 */
class Round18WireProtocolFieldsTest {

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

    private static ServerErrorMessage triggerErrorAndGetMessage(String badSql) {
        try (Statement s = conn.createStatement()) {
            s.execute(badSql);
            fail("Expected SQL to fail: " + badSql);
            return null;
        } catch (SQLException e) {
            if (e instanceof PSQLException) {
                return ((PSQLException) e).getServerErrorMessage();
            }
            fail("Expected PSQLException for wire-level diagnostic fields; got " + e.getClass());
            return null;
        }
    }

    // =========================================================================
    // AC1. Severity (V field)
    // =========================================================================

    @Test
    void error_response_includes_severity_V_field() {
        ServerErrorMessage m = triggerErrorAndGetMessage("SELECT * FROM pg_does_not_exist_r18");
        assertNotNull(m, "ServerErrorMessage must not be null");
        String sev = m.getSeverity();
        assertNotNull(sev, "ErrorResponse must include Severity (V field)");
        assertEquals("ERROR", sev.toUpperCase(),
                "Severity must be ERROR for a failed query; got '" + sev + "'");
    }

    // =========================================================================
    // AC2. SQLSTATE (C field)
    // =========================================================================

    @Test
    void error_response_includes_sqlstate_C_field() {
        ServerErrorMessage m = triggerErrorAndGetMessage("SELECT * FROM pg_does_not_exist_r18");
        assertNotNull(m);
        String st = m.getSQLState();
        assertNotNull(st, "ErrorResponse must include SQLSTATE (C field)");
        assertEquals("42P01", st, "Missing relation SQLSTATE must be 42P01; got " + st);
    }

    // =========================================================================
    // AC3. Hint (H field)
    // =========================================================================

    @Test
    void error_response_includes_hint_H_field_when_available() {
        // Provoke undefined column on an existing table to elicit a hint.
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS r18_hint; CREATE TABLE r18_hint(abc int);");
        } catch (SQLException e) { fail(e.getMessage()); }
        ServerErrorMessage m = triggerErrorAndGetMessage("SELECT abcd FROM r18_hint");
        assertNotNull(m);
        String hint = m.getHint();
        assertNotNull(hint, "ErrorResponse H (hint) field must be populated for a typo; got null");
        assertTrue(hint.toLowerCase().contains("abc"),
                "Hint should mention the correct column; got '" + hint + "'");
    }

    // =========================================================================
    // AC4. File (F field)
    // =========================================================================

    @Test
    void error_response_includes_file_F_field() {
        ServerErrorMessage m = triggerErrorAndGetMessage("SELECT * FROM pg_does_not_exist_r18");
        assertNotNull(m);
        assertNotNull(m.getFile(), "ErrorResponse must include File (F field)");
        assertFalse(m.getFile().isEmpty(), "File field must be non-empty");
    }

    // =========================================================================
    // AC5. Line (L field)
    // =========================================================================

    @Test
    void error_response_includes_line_L_field() {
        ServerErrorMessage m = triggerErrorAndGetMessage("SELECT * FROM pg_does_not_exist_r18");
        assertNotNull(m);
        assertNotEquals(0, m.getLine(),
                "ErrorResponse must include Line (L field) as a non-zero integer");
    }

    // =========================================================================
    // AC6. Routine (R field)
    // =========================================================================

    @Test
    void error_response_includes_routine_R_field() {
        ServerErrorMessage m = triggerErrorAndGetMessage("SELECT * FROM pg_does_not_exist_r18");
        assertNotNull(m);
        String r = m.getRoutine();
        assertNotNull(r, "ErrorResponse must include Routine (R field)");
        assertFalse(r.isEmpty(), "Routine field must be non-empty");
    }

    // =========================================================================
    // AC7. Position (P field)
    // =========================================================================

    @Test
    void error_response_includes_position_P_field_on_syntax_error() {
        // Syntax error emits a P field pointing at the bad token.
        ServerErrorMessage m = triggerErrorAndGetMessage("SELECT FROM");
        assertNotNull(m);
        assertNotEquals(0, m.getPosition(),
                "Syntax ErrorResponse must include Position (P field); got 0");
    }

    // =========================================================================
    // AC8. pg_stat_activity.query_id populated
    // =========================================================================

    @Test
    void pg_stat_activity_query_id_populated_when_computed() throws SQLException {
        // PG 14+ provides compute_query_id = on / auto → backend populates query_id.
        try (Statement s = conn.createStatement()) {
            s.execute("SET compute_query_id = on");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT query_id FROM pg_stat_activity WHERE pid = pg_backend_pid()")) {
            assertTrue(rs.next());
            long qid = rs.getLong(1);
            boolean wasNull = rs.wasNull();
            assertFalse(wasNull,
                    "pg_stat_activity.query_id must be non-null with compute_query_id=on");
            assertTrue(qid != 0L,
                    "pg_stat_activity.query_id must be non-zero; got " + qid);
        }
    }
}
