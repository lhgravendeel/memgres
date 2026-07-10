package com.memgres.select;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Group 5 (mtask-8): {@code SelectAggregateEvaluator.evalAggregateExpr} substituted resolved
 * aggregate results as {@code Literal.ofString(val.toString())} when a non-aggregate scalar
 * function/expression wraps aggregate calls (e.g. {@code LEAST(SUM(a), SUM(b))}), so the outer
 * function received string arguments and compared them lexicographically:
 * {@code LEAST('7.0', '10.0')} returns {@code '10.0'} (bigger) because {@code '1' < '7'}
 * character-wise, not {@code '7.0'} (numerically smaller). Silent wrong numbers, not an error.
 *
 * Mirrors the real repro (battery_sets_aggregated materialized view,
 * {@code LEAST(sum(i.discharge_power * bsi.quantity), sum(b.discharge_power * bsb.quantity))}).
 */
class AggregateArgTypePreservationTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE g5_power (grp int, discharge_power numeric)");
            // group 1: sums are 7.0 and 10.0 -> LEAST must be 7.0 (numerically smaller), not 10.0
            // (which sorts lexicographically-smaller as a string: "10.0" < "7.0").
            s.execute("INSERT INTO g5_power VALUES (1, 3), (1, 4), (1, 10)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    void leastOfTwoSumsComparesNumerically_notLexicographically() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT LEAST(SUM(discharge_power) FILTER (WHERE discharge_power < 10), "
                             + "SUM(discharge_power) FILTER (WHERE discharge_power >= 10)) AS m "
                             + "FROM g5_power WHERE grp = 1")) {
            assertTrue(rs.next());
            assertEquals(7.0, rs.getDouble(1), 0.0001);
        }
    }

    @Test
    void greatestOfTwoSumsComparesNumerically() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT GREATEST(SUM(discharge_power) FILTER (WHERE discharge_power < 10), "
                             + "SUM(discharge_power) FILTER (WHERE discharge_power >= 10)) AS m "
                             + "FROM g5_power WHERE grp = 1")) {
            assertTrue(rs.next());
            assertEquals(10.0, rs.getDouble(1), 0.0001);
        }
    }

    @Test
    void joinedGroupByShapeMirroringBatterySetsAggregatedView() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE g5_installations (installation_id int, discharge_power numeric, quantity numeric)");
            s.execute("CREATE TABLE g5_batteries (installation_id int, discharge_power numeric, quantity numeric)");
            s.execute("INSERT INTO g5_installations VALUES (1, 3.5, 2)"); // 7.0
            s.execute("INSERT INTO g5_batteries VALUES (1, 5, 2)"); // 10.0
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT LEAST(SUM(i.discharge_power * i.quantity), SUM(b.discharge_power * b.quantity)) AS effective_discharge_power "
                             + "FROM g5_installations i JOIN g5_batteries b ON b.installation_id = i.installation_id "
                             + "GROUP BY i.installation_id")) {
            assertTrue(rs.next());
            assertEquals(7.0, rs.getDouble(1), 0.0001);
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE g5_installations");
                s.execute("DROP TABLE g5_batteries");
            }
        }
    }

    // Regression guard: plain LEAST/GREATEST over literals (no aggregate args) must still work.
    @Test
    void plainLeastOverLiterals_stillWorks() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT LEAST(7.0, 10.0) AS m")) {
            assertTrue(rs.next());
            assertEquals(7.0, rs.getDouble(1), 0.0001);
        }
    }
}
