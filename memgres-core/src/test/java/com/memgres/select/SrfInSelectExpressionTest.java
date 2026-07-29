package com.memgres.select;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bug 8 (mtask-6): a set-returning function (SRF) nested inside a larger SELECT-list
 * expression previously crashed instead of expanding one row per generated element.
 *
 * memgres already supported a bare/top-level SRF in the select list (e.g.
 * {@code SELECT generate_series(0, 23, 2)}), detected by
 * {@code SelectExecutor.isSrfCall}, which only recognized the target expression being
 * *directly* a (possibly cast) SRF call. When the SRF was nested inside a larger expression —
 * e.g. {@code day_start + (interval '1 hour' * generate_series(0, 23, 2))}, the real shape used
 * by {@code TennetSettlementPricesDao.findMostRecentMissingTimestamp} — evaluating the whole
 * expression handed the raw generate_series {@code List<Object>} to the multiplication operator,
 * which does not understand lists, producing a downstream mapping failure through jdbi.
 *
 * The fix generalizes SRF detection to search the whole expression tree
 * ({@code SelectExecutor.findSrfCall}) and, when found, evaluates the SRF once per row to get
 * its element list, then re-evaluates the *entire* owning expression once per element with that
 * one function-call node bound to the corresponding element (via
 * {@code RowContext.setBoundValue}/{@code ExprEvaluator.evalExpr}), rather than trying to
 * evaluate the raw list as if it were a scalar.
 */
class SrfInSelectExpressionTest {

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

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    /** SRF nested inside arithmetic in the select list expands one row per element. */
    @Test
    void srf_nested_in_arithmetic_expands_one_row_per_element() throws SQLException {
        List<String> results = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT TIMESTAMP '2024-01-01 00:00:00' + " +
                     "(interval '1 hour' * generate_series(0, 23, 2)) AS ts")) {
            while (rs.next()) {
                results.add(rs.getString("ts"));
            }
        }
        // generate_series(0, 23, 2) yields 12 elements: 0,2,4,...,22
        assertEquals(12, results.size(), "expected one row per generated element, got: " + results);
        assertEquals("2024-01-01 00:00:00", results.get(0));
        assertEquals("2024-01-01 02:00:00", results.get(1));
        assertEquals("2024-01-01 22:00:00", results.get(results.size() - 1));
    }

    /** The exact shape from TennetSettlementPricesDao: a CTE with day_start + interval*SRF. */
    @Test
    void srf_nested_in_cte_select_matches_dao_shape() throws SQLException {
        List<String> results = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "WITH time_series AS (" +
                     "  SELECT TIMESTAMP '2024-06-01 00:00:00' AS day_start" +
                     ")" +
                     "SELECT day_start + (interval '1 hour' * generate_series(0, 23, 2)) AS ts " +
                     "FROM time_series")) {
            while (rs.next()) {
                results.add(rs.getString("ts"));
            }
        }
        assertEquals(12, results.size(), "expected 12 expanded rows, got: " + results);
    }

    /** A plain sibling column in the same select list is repeated per generated element (PG ProjectSet). */
    @Test
    void sibling_column_repeats_per_generated_element() throws SQLException {
        exec("CREATE TABLE srf_expr_t (id int, day_start timestamp)");
        exec("INSERT INTO srf_expr_t VALUES (1, '2024-01-01 00:00:00')");
        try {
            List<Integer> ids = new ArrayList<>();
            List<String> timestamps = new ArrayList<>();
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT id, day_start + (interval '1 hour' * generate_series(0, 3)) AS ts " +
                         "FROM srf_expr_t")) {
                while (rs.next()) {
                    ids.add(rs.getInt("id"));
                    timestamps.add(rs.getString("ts"));
                }
            }
            assertEquals(4, ids.size());
            assertTrue(ids.stream().allMatch(i -> i == 1), "id column should repeat across expanded rows: " + ids);
            assertEquals(Arrays.asList(
                    "2024-01-01 00:00:00", "2024-01-01 01:00:00",
                    "2024-01-01 02:00:00", "2024-01-01 03:00:00"), timestamps);
        } finally {
            exec("DROP TABLE IF EXISTS srf_expr_t");
        }
    }

    /** A bare top-level SRF in the select list still works after the generalization. */
    @Test
    void bare_top_level_srf_still_works() throws SQLException {
        List<Integer> results = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT generate_series(1, 3) AS n")) {
            while (rs.next()) results.add(rs.getInt("n"));
        }
        assertEquals(Arrays.asList(1, 2, 3), results);
    }
}
