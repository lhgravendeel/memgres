package com.memgres.aggregate;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers GROUPING() bitmask semantics, select-list expressions over ROLLUP/GROUPING SETS,
 * value-semantic DISTINCT aggregates, and hypothetical-set cume_dist.
 */
class GroupingSetsAggregatesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- GROUPING() bitmask ----

    @Test
    void grouping_single_arg_over_rollup() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE g1 (a int)");
            s.execute("INSERT INTO g1 VALUES (1), (2)");
            ResultSet rs = s.executeQuery(
                    "SELECT a, grouping(a) AS g FROM g1 GROUP BY ROLLUP(a) ORDER BY g, a");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
            assertTrue(rs.next());
            rs.getInt(1);
            assertTrue(rs.wasNull());
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
            s.execute("DROP TABLE g1");
        }
    }

    @Test
    void grouping_two_args_bitmask_over_rollup() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE g2 (region text, product text, amount int)");
            s.execute("INSERT INTO g2 VALUES ('east','widget',10), ('east','gadget',20), "
                    + "('west','widget',30), ('west','gadget',40)");
            ResultSet rs = s.executeQuery(
                    "SELECT region, product, grouping(region, product) AS g, sum(amount) AS total "
                    + "FROM g2 GROUP BY ROLLUP(region, product) "
                    + "ORDER BY g, region, product");
            // Detail rows: bitmask 0
            String[][] detail = {{"east", "gadget"}, {"east", "widget"}, {"west", "gadget"}, {"west", "widget"}};
            for (String[] d : detail) {
                assertTrue(rs.next());
                assertEquals(d[0], rs.getString(1));
                assertEquals(d[1], rs.getString(2));
                assertEquals(0, rs.getInt(3));
            }
            // Region subtotals: product not grouped -> rightmost bit set -> 1
            for (String region : new String[]{"east", "west"}) {
                assertTrue(rs.next());
                assertEquals(region, rs.getString(1));
                assertNull(rs.getString(2));
                assertEquals(1, rs.getInt(3));
            }
            // Grand total: both args not grouped -> 0b11 = 3
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
            assertEquals(3, rs.getInt(3));
            assertEquals(100, rs.getInt(4));
            assertFalse(rs.next());
            s.execute("DROP TABLE g2");
        }
    }

    @Test
    void grouping_two_args_bitmask_over_cube() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE g3 (a text, b text)");
            s.execute("INSERT INTO g3 VALUES ('x','p'), ('y','q')");
            // CUBE(a,b) = sets (a,b), (a), (b), () -> masks 0, 1, 2, 3
            ResultSet rs = s.executeQuery(
                    "SELECT grouping(a, b) AS g, count(*) AS c FROM g3 "
                    + "GROUP BY CUBE(a, b) ORDER BY g, a NULLS LAST, b NULLS LAST");
            List<Integer> masks = new ArrayList<>();
            while (rs.next()) {
                masks.add(rs.getInt(1));
            }
            // 2 detail rows (0), 2 a-subtotals (1), 2 b-subtotals (2), 1 grand total (3)
            assertEquals(List.of(0, 0, 1, 1, 2, 2, 3), masks);
            s.execute("DROP TABLE g3");
        }
    }

    @Test
    void grouping_three_args_bitmask_over_rollup() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE g4 (a int, b int, c int)");
            s.execute("INSERT INTO g4 VALUES (1, 1, 1), (2, 2, 2)");
            // ROLLUP(a,b,c) sets: (a,b,c)->0, (a,b)->1, (a)->3, ()->7
            ResultSet rs = s.executeQuery(
                    "SELECT grouping(a, b, c) AS g FROM g4 GROUP BY ROLLUP(a, b, c) ORDER BY g, a, b, c");
            List<Integer> masks = new ArrayList<>();
            while (rs.next()) {
                masks.add(rs.getInt(1));
            }
            assertEquals(List.of(0, 0, 1, 1, 3, 3, 7), masks);
            s.execute("DROP TABLE g4");
        }
    }

    @Test
    void grouping_bitmask_over_explicit_grouping_sets() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE g5 (a text, b text)");
            s.execute("INSERT INTO g5 VALUES ('x','p'), ('x','q')");
            // Set (a): b not grouped -> grouping(a,b) = 0b01 = 1
            // Set (b): a not grouped -> grouping(a,b) = 0b10 = 2
            ResultSet rs = s.executeQuery(
                    "SELECT grouping(a, b) AS g, count(*) AS c FROM g5 "
                    + "GROUP BY GROUPING SETS ((a), (b)) ORDER BY g, c");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1)); // set (a): one group of 2 rows
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1)); // set (b): 'p'
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1)); // set (b): 'q'
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
            s.execute("DROP TABLE g5");
        }
    }

    @Test
    void grouping_disambiguation_case_idiom() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE g6 (region text, product text, amount int)");
            s.execute("INSERT INTO g6 VALUES ('east','widget',10), ('east','gadget',20), "
                    + "('west','widget',40), ('west','gadget',80)");
            ResultSet rs = s.executeQuery(
                    "SELECT CASE WHEN grouping(region, product) = 3 THEN 'grand total' "
                    + "            WHEN grouping(region, product) = 1 THEN region || ' subtotal' "
                    + "            ELSE region || '/' || product END AS label, "
                    + "       sum(amount) AS total "
                    + "FROM g6 GROUP BY ROLLUP(region, product) ORDER BY sum(amount)");
            String[][] expected = {
                    {"east/widget", "10"},
                    {"east/gadget", "20"},
                    {"east subtotal", "30"},
                    {"west/widget", "40"},
                    {"west/gadget", "80"},
                    {"west subtotal", "120"},
                    {"grand total", "150"}};
            for (String[] e : expected) {
                assertTrue(rs.next());
                assertEquals(e[0], rs.getString(1));
                assertEquals(Integer.parseInt(e[1]), rs.getInt(2));
            }
            assertFalse(rs.next());
            s.execute("DROP TABLE g6");
        }
    }

    // ---- Select-list expressions over ROLLUP/GROUPING SETS ----

    @Test
    void expression_target_over_rollup_evaluates_per_set() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE e1 (a int)");
            s.execute("INSERT INTO e1 VALUES (1), (2)");
            // a+10 must evaluate against the group key in sets containing a,
            // and be NULL only in the grand-total set.
            ResultSet rs = s.executeQuery(
                    "SELECT a + 10 AS ap, count(*) AS c FROM e1 GROUP BY ROLLUP(a) ORDER BY ap");
            assertTrue(rs.next());
            assertEquals(11, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(12, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertTrue(rs.next());
            rs.getInt(1);
            assertTrue(rs.wasNull());
            assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
            s.execute("DROP TABLE e1");
        }
    }

    @Test
    void mixed_expression_and_aggregate_target_over_rollup() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE e2 (a int, b int)");
            s.execute("INSERT INTO e2 VALUES (1, 100), (1, 200), (2, 300)");
            // a + count(b): the column part masks per grouping set, the aggregate
            // still evaluates over the whole group.
            ResultSet rs = s.executeQuery(
                    "SELECT a, a + count(b) AS ac FROM e2 GROUP BY ROLLUP(a) ORDER BY a NULLS LAST");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(3, rs.getInt(2)); // 1 + 2
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(3, rs.getInt(2)); // 2 + 1
            assertTrue(rs.next());
            rs.getInt(1);
            assertTrue(rs.wasNull());
            rs.getInt(2);
            assertTrue(rs.wasNull()); // NULL + 3 = NULL in the grand total
            assertFalse(rs.next());
            s.execute("DROP TABLE e2");
        }
    }

    @Test
    void rollup_two_cols_subtotal_rows_keep_grouped_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE e3 (a int, b int)");
            s.execute("INSERT INTO e3 VALUES (1, 10), (1, 20), (2, 30)");
            ResultSet rs = s.executeQuery(
                    "SELECT a, b, count(*) AS c FROM e3 GROUP BY ROLLUP(a, b) "
                    + "ORDER BY a NULLS LAST, b NULLS LAST");
            int[][] expected = {{1, 10, 1}, {1, 20, 1}, {1, -1, 2}, {2, 30, 1}, {2, -1, 1}, {-1, -1, 3}};
            for (int[] e : expected) {
                assertTrue(rs.next());
                if (e[0] < 0) {
                    rs.getInt(1);
                    assertTrue(rs.wasNull());
                } else {
                    assertEquals(e[0], rs.getInt(1));
                }
                if (e[1] < 0) {
                    rs.getInt(2);
                    assertTrue(rs.wasNull());
                } else {
                    assertEquals(e[1], rs.getInt(2));
                }
                assertEquals(e[2], rs.getInt(3));
            }
            assertFalse(rs.next());
            s.execute("DROP TABLE e3");
        }
    }

    @Test
    void composite_rollup_detail_rows_keep_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE e4 (a int, b int)");
            s.execute("INSERT INTO e4 VALUES (1, 1), (2, 2)");
            // ROLLUP((a,b)) = sets {(a,b)}, {}: a and b are both grouped in the detail set,
            // so their real values must show; NULL only in the grand-total row.
            ResultSet rs = s.executeQuery(
                    "SELECT a, b, count(*) AS c FROM e4 GROUP BY ROLLUP((a, b)) "
                    + "ORDER BY a NULLS LAST, b");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(1, rs.getInt(2));
            assertEquals(1, rs.getInt(3));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(1, rs.getInt(3));
            assertTrue(rs.next());
            rs.getInt(1);
            assertTrue(rs.wasNull());
            rs.getInt(2);
            assertTrue(rs.wasNull());
            assertEquals(2, rs.getInt(3));
            assertFalse(rs.next());
            s.execute("DROP TABLE e4");
        }
    }

    @Test
    void composite_rollup_grouping_of_member_columns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE e5 (a int, b int)");
            s.execute("INSERT INTO e5 VALUES (1, 1), (2, 2)");
            ResultSet rs = s.executeQuery(
                    "SELECT a, b, grouping(a) AS ga, grouping(b) AS gb "
                    + "FROM e5 GROUP BY ROLLUP((a, b)) ORDER BY ga, a");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(0, rs.getInt(3));
            assertEquals(0, rs.getInt(4));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(0, rs.getInt(3));
            assertEquals(0, rs.getInt(4));
            assertTrue(rs.next());
            rs.getInt(1);
            assertTrue(rs.wasNull());
            assertEquals(1, rs.getInt(3));
            assertEquals(1, rs.getInt(4));
            assertFalse(rs.next());
            s.execute("DROP TABLE e5");
        }
    }

    // ---- DISTINCT aggregates with mixed-scale numerics ----

    @Test
    void count_and_sum_distinct_mixed_scale_numerics() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT count(DISTINCT x) AS c, sum(DISTINCT x) AS s "
                    + "FROM (VALUES (1.0::numeric), (1.00::numeric)) v(x)");
            assertTrue(rs.next());
            assertEquals(1, rs.getLong(1));
            assertEquals("1.0", rs.getString(2));
            assertEquals(0, new BigDecimal("1").compareTo(rs.getBigDecimal(2)));
            assertFalse(rs.next());
        }
    }

    @Test
    void distinct_aggregates_mixed_scale_numerics() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE d1 (x numeric)");
            s.execute("INSERT INTO d1 VALUES (1.0), (1.00), (2)");
            ResultSet rs = s.executeQuery(
                    "SELECT count(DISTINCT x) AS c, sum(DISTINCT x) AS s, avg(DISTINCT x) AS a "
                    + "FROM d1");
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertEquals(0, new BigDecimal("3").compareTo(rs.getBigDecimal(2)));
            assertEquals(0, new BigDecimal("1.5").compareTo(rs.getBigDecimal(3)));
            assertFalse(rs.next());
            s.execute("DROP TABLE d1");
        }
    }

    @Test
    void string_agg_and_array_agg_distinct_mixed_scale() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE d2 (x numeric)");
            s.execute("INSERT INTO d2 VALUES (1.0), (1.00), (2)");
            // string_agg is declared over text and over bytea, and a numeric column is neither:
            // PostgreSQL refuses the call rather than reading the number as a string for it.
            SQLException e = assertThrows(SQLException.class, () -> s.executeQuery(
                    "SELECT string_agg(DISTINCT x, ',' ORDER BY x) AS sa FROM d2"));
            assertEquals("42883", e.getSQLState());
            // DISTINCT still has to apply to the numbers -- 1.0 and 1.00 are one value, and it
            // is the first spelling that survives -- so the values are made distinct as numbers
            // and rendered as text after, which is what this test is about.
            ResultSet rs = s.executeQuery(
                    "SELECT array_agg(DISTINCT x) AS aa,"
                    + " (SELECT string_agg(d.x::text, ',' ORDER BY d.x)"
                    + "    FROM (SELECT DISTINCT x FROM d2) d) AS sa "
                    + "FROM d2");
            assertTrue(rs.next());
            assertEquals("{1.0,2}", rs.getString(1));
            assertEquals("1.0,2", rs.getString(2));
            assertFalse(rs.next());
            s.execute("DROP TABLE d2");
        }
    }

    @Test
    void count_distinct_still_distinguishes_unequal_values() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT count(DISTINCT x) AS c, sum(DISTINCT x) AS s "
                    + "FROM (VALUES (1.0::numeric), (1.01::numeric), (1.010::numeric)) v(x)");
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
            assertEquals(0, new BigDecimal("2.01").compareTo(rs.getBigDecimal(2)));
            assertFalse(rs.next());
        }
    }

    // ---- Hypothetical-set cume_dist ----

    @Test
    void cume_dist_hypothetical_includes_hypothetical_row() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT cume_dist(2) WITHIN GROUP (ORDER BY x) AS cd "
                    + "FROM (VALUES (1), (2), (3)) v(x)");
            assertTrue(rs.next());
            assertEquals(0.75, rs.getDouble(1), 1e-9); // (2 + 1) / (3 + 1)
            assertFalse(rs.next());
        }
    }

    @Test
    void cume_dist_hypothetical_with_duplicates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT cume_dist(2) WITHIN GROUP (ORDER BY x) AS cd "
                    + "FROM (VALUES (1), (2), (2), (3)) v(x)");
            assertTrue(rs.next());
            assertEquals(0.8, rs.getDouble(1), 1e-9); // (3 + 1) / (4 + 1)
            assertFalse(rs.next());
        }
    }

    @Test
    void cume_dist_hypothetical_empty_group_is_one() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cd1 (x int)");
            ResultSet rs = s.executeQuery(
                    "SELECT cume_dist(2) WITHIN GROUP (ORDER BY x) AS cd FROM cd1");
            assertTrue(rs.next());
            assertEquals(1.0, rs.getDouble(1), 1e-9); // (0 + 1) / (0 + 1)
            assertFalse(rs.next());
            s.execute("DROP TABLE cd1");
        }
    }

    // ---- rank / dense_rank / percent_rank hypothetical: verified-correct behavior ----

    @Test
    void rank_dense_rank_percent_rank_hypothetical_unchanged() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT rank(2) WITHIN GROUP (ORDER BY x) AS r, "
                    + "       dense_rank(2) WITHIN GROUP (ORDER BY x) AS dr, "
                    + "       percent_rank(2) WITHIN GROUP (ORDER BY x) AS pr "
                    + "FROM (VALUES (1), (2), (3)) v(x)");
            assertTrue(rs.next());
            assertEquals(2L, rs.getLong(1));
            assertEquals(2L, rs.getLong(2));
            assertEquals(1.0 / 3.0, rs.getDouble(3), 1e-9); // (rank - 1) / N
            assertFalse(rs.next());
        }
    }

    @Test
    void rank_hypothetical_with_duplicates_unchanged() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT rank(2) WITHIN GROUP (ORDER BY x) AS r, "
                    + "       dense_rank(2) WITHIN GROUP (ORDER BY x) AS dr "
                    + "FROM (VALUES (1), (1), (2), (3)) v(x)");
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong(1)); // two values below 2
            assertEquals(2L, rs.getLong(2)); // one distinct value below 2
            assertFalse(rs.next());
        }
    }
}
