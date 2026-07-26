package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A numeric(p,s) is checked after rounding, to_date rejects an impossible day rather than
 * clamping it, and to_number honours the sign wherever the format puts it. Expectations
 * captured from a live PostgreSQL 18.0 server.
 *
 * <p>N40 numeric typmod overflow, N28 to_date/to_number input handling.
 */
class NumericTypmodAndToNumberTest {

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
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String expr(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ------------------------------------------------------------------
    // N40 — the typmod is checked after rounding
    // ------------------------------------------------------------------

    @Test
    void aValueThatRoundsOutOfItsPrecisionOverflows() {
        assertEquals("22003", state("SELECT 99.995::numeric(4,2)"));
        assertEquals("22003", state("SELECT 123::numeric(2,0)"));
    }

    @Test
    void aValueThatStillFitsAfterRoundingIsKept() throws Exception {
        assertEquals("99.99", expr("SELECT 99.994::numeric(4,2)::text"));
        assertEquals("12.35", expr("SELECT 12.345::numeric(5,2)::text"));
    }

    @Test
    void columnAssignmentEnforcesTheSameBound() throws Exception {
        exec("CREATE TABLE nt_t (id int PRIMARY KEY, v numeric(4,2))");
        exec("INSERT INTO nt_t VALUES (1, 99.99)");
        assertEquals("22003", state("INSERT INTO nt_t VALUES (2, 99.995)"));
        assertEquals("99.99", expr("SELECT v::text FROM nt_t WHERE id = 1"));
    }

    // ------------------------------------------------------------------
    // N28 — to_date and to_number input handling
    // ------------------------------------------------------------------

    @Test
    void toDateRejectsAnImpossibleDay() {
        assertEquals("22008", state("SELECT to_date('2026-02-30','YYYY-MM-DD')"));
    }

    @Test
    void toDateStillParsesARealDate() throws Exception {
        assertEquals("2026-02-28", expr("SELECT to_date('2026-02-28','YYYY-MM-DD')::text"));
        assertEquals("2024-02-29", expr("SELECT to_date('2024-02-29','YYYY-MM-DD')::text"));
    }

    @Test
    void toNumberHonoursALeadingOrTrailingSign() throws Exception {
        assertEquals("-123", expr("SELECT to_number('123-','999S')::text"));
        assertEquals("-123", expr("SELECT to_number('-123','S999')::text"));
        assertEquals("123", expr("SELECT to_number('123','999')::text"));
    }
}
