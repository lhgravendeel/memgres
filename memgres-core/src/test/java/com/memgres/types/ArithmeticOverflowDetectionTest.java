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
 * Intervals, bigints and network addresses have a finite range, and a result that leaves it is
 * an error rather than a wrap. Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>N27 interval overflow wraps silently, N47 inet arithmetic wraps,
 * N65 integer division overflow undetected.
 */
class ArithmeticOverflowDetectionTest {

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
    // N27 — interval field overflow
    // ------------------------------------------------------------------

    @Test
    void intervalFieldOverflowIsReported() {
        assertEquals("22008", state("SELECT (INTERVAL '2147483647 months' + INTERVAL '1 month')"));
        assertEquals("22008", state("SELECT (INTERVAL '100000000 years' * 1000)"));
        assertEquals("22008", state("SELECT (INTERVAL '-2147483647 months' - INTERVAL '2 months')"));
    }

    @Test
    void ordinaryIntervalArithmeticStillWorks() throws Exception {
        assertEquals("2 days", expr("SELECT (INTERVAL '1 day' + INTERVAL '1 day')::text"));
        assertEquals("3 days 08:00:00", expr("SELECT (INTERVAL '10 days' / 3)::text"));
        assertEquals("1 year 6 mons", expr("SELECT (INTERVAL '1.5 years')::text"));
    }

    /** The sentinel months value means infinity, which is not an overflow. */
    @Test
    void anInfiniteIntervalIsNotAnOverflow() throws Exception {
        assertEquals("infinity", expr("SELECT (INTERVAL 'infinity' + INTERVAL '1 day')::text"));
        assertEquals("-infinity", expr("SELECT (INTERVAL '-infinity')::text"));
    }

    // ------------------------------------------------------------------
    // N65 — integer division has exactly one overflow
    // ------------------------------------------------------------------

    @Test
    void theOneIntegerDivisionOverflowIsReported() {
        assertEquals("22003", state("SELECT (-9223372036854775808)::bigint / (-1)::bigint"));
    }

    @Test
    void ordinaryDivisionStillWorks() throws Exception {
        assertEquals("3", expr("SELECT (10::bigint / 3::bigint)::text"));
        assertEquals("-3", expr("SELECT (-10::bigint / 3::bigint)::text"));
    }

    // ------------------------------------------------------------------
    // N47 — the address space does not wrap
    // ------------------------------------------------------------------

    @Test
    void inetArithmeticLeavingTheAddressSpaceIsReported() {
        assertEquals("22003", state("SELECT ('255.255.255.255'::inet + 1)"));
        assertEquals("22003", state("SELECT ('0.0.0.0'::inet - 1)"));
    }

    @Test
    void inetArithmeticInsideTheAddressSpaceStillWorks() throws Exception {
        assertEquals("255.255.255.255/32", expr("SELECT ('255.255.255.255'::inet + 0)::text"));
        assertEquals("10.0.0.6/32", expr("SELECT ('10.0.0.1'::inet + 5)::text"));
        assertEquals("10.0.0.1/32", expr("SELECT ('10.0.0.6'::inet - 5)::text"));
    }
}
