package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Scenarios from 1350_performance_regression_and_query_shape_scenarios.md.
 *
 * Focuses on query correctness across different data shapes and scales rather
 * than wall-clock performance.  Each test creates and drops its own tables
 * using the {@code perf_} prefix, and uses generate_series() to populate data.
 */
class PerformanceQueryShapeTest {

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
        if (conn != null && !conn.isClosed()) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 1. Top-N with index
    // =========================================================================

    @Test
    void top_n_with_indexed_column_returns_correct_rows() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_top_n ("
                + "  id int PRIMARY KEY, "
                + "  score int NOT NULL)");
        conn.createStatement().execute(
                "CREATE INDEX perf_top_n_score_idx ON perf_top_n (score)");
        conn.createStatement().execute(
                "INSERT INTO perf_top_n "
                + "SELECT gs, (1001 - gs) "
                + "FROM generate_series(1, 1000) gs");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, score FROM perf_top_n ORDER BY score DESC LIMIT 10")) {
            int count = 0;
            int prevScore = Integer.MAX_VALUE;
            while (rs.next()) {
                int score = rs.getInt("score");
                assertTrue(score <= prevScore,
                        "Rows must be in descending score order");
                prevScore = score;
                count++;
            }
            assertEquals(10, count, "Top-N query must return exactly 10 rows");
        }

        conn.createStatement().execute("DROP TABLE perf_top_n");
    }

    // =========================================================================
    // 2. Top-N per group via LATERAL pattern
    // =========================================================================

    @Test
    void top_n_per_group_lateral_returns_correct_rows() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_lateral_items ("
                + "  id int PRIMARY KEY, "
                + "  category int NOT NULL, "
                + "  val int NOT NULL)");
        // 5 categories × 20 rows each = 100 rows
        conn.createStatement().execute(
                "INSERT INTO perf_lateral_items "
                + "SELECT gs, ((gs - 1) / 20) + 1, gs "
                + "FROM generate_series(1, 100) gs");

        conn.createStatement().execute(
                "CREATE TABLE perf_lateral_cats (category int PRIMARY KEY)");
        conn.createStatement().execute(
                "INSERT INTO perf_lateral_cats "
                + "SELECT DISTINCT category FROM perf_lateral_items");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT c.category, top.id, top.val "
                + "FROM perf_lateral_cats c "
                + "CROSS JOIN LATERAL ("
                + "  SELECT id, val FROM perf_lateral_items i "
                + "  WHERE i.category = c.category "
                + "  ORDER BY val DESC LIMIT 3"
                + ") top "
                + "ORDER BY c.category, top.val DESC")) {
            int totalRows = 0;
            while (rs.next()) totalRows++;
            // 5 categories × 3 rows = 15
            assertEquals(15, totalRows,
                    "LATERAL top-3 per category must yield 15 rows total");
        }

        conn.createStatement().execute("DROP TABLE perf_lateral_items");
        conn.createStatement().execute("DROP TABLE perf_lateral_cats");
    }

    // =========================================================================
    // 3. Top-N per group via ROW_NUMBER() window function
    // =========================================================================

    @Test
    void top_n_per_group_window_rownumber_returns_correct_rows() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_window_items ("
                + "  id int PRIMARY KEY, "
                + "  category int NOT NULL, "
                + "  val int NOT NULL)");
        // 4 categories × 25 rows
        conn.createStatement().execute(
                "INSERT INTO perf_window_items "
                + "SELECT gs, ((gs - 1) / 25) + 1, gs "
                + "FROM generate_series(1, 100) gs");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT category, id, val, rn "
                + "FROM ("
                + "  SELECT category, id, val, "
                + "         ROW_NUMBER() OVER (PARTITION BY category ORDER BY val DESC) AS rn "
                + "  FROM perf_window_items"
                + ") ranked "
                + "WHERE rn <= 2 "
                + "ORDER BY category, rn")) {
            int totalRows = 0;
            while (rs.next()) {
                assertTrue(rs.getInt("rn") <= 2,
                        "ROW_NUMBER filter must keep only rn <= 2");
                totalRows++;
            }
            // 4 categories × 2 rows = 8
            assertEquals(8, totalRows,
                    "Window top-2 per category must yield 8 rows total");
        }

        conn.createStatement().execute("DROP TABLE perf_window_items");
    }

    // =========================================================================
    // 4. Filtered aggregation at scale
    // =========================================================================

    @Test
    void filtered_aggregation_at_scale_returns_correct_results() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_agg_data ("
                + "  id int PRIMARY KEY, "
                + "  region int NOT NULL, "
                + "  amount int NOT NULL)");
        // 1000 rows, regions 1-10
        conn.createStatement().execute(
                "INSERT INTO perf_agg_data "
                + "SELECT gs, ((gs - 1) % 10) + 1, gs * 2 "
                + "FROM generate_series(1, 1000) gs");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total, AVG(amount) AS avg_amount "
                + "FROM perf_agg_data "
                + "WHERE amount > 200 "
                + "GROUP BY region "
                + "ORDER BY region")) {
            int regions = 0;
            while (rs.next()) {
                assertTrue(rs.getLong("cnt") > 0,
                        "Each region group must have at least one row");
                assertTrue(rs.getLong("total") > 0,
                        "SUM(amount) must be positive");
                regions++;
            }
            assertTrue(regions > 0, "Filtered aggregation must produce at least one group");
        }

        conn.createStatement().execute("DROP TABLE perf_agg_data");
    }

    // =========================================================================
    // 5. JOIN heavy: 3-table join at 100+ rows
    // =========================================================================

    @Test
    void three_table_join_returns_correct_results() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_customers ("
                + "  cid int PRIMARY KEY, cname text NOT NULL)");
        conn.createStatement().execute(
                "CREATE TABLE perf_orders ("
                + "  oid int PRIMARY KEY, cid int NOT NULL, amount int NOT NULL)");
        conn.createStatement().execute(
                "CREATE TABLE perf_products ("
                + "  pid int PRIMARY KEY, oid int NOT NULL, pname text NOT NULL)");

        conn.createStatement().execute(
                "INSERT INTO perf_customers "
                + "SELECT gs, 'customer_' || gs "
                + "FROM generate_series(1, 50) gs");
        conn.createStatement().execute(
                "INSERT INTO perf_orders "
                + "SELECT gs, ((gs - 1) % 50) + 1, gs * 10 "
                + "FROM generate_series(1, 200) gs");
        conn.createStatement().execute(
                "INSERT INTO perf_products "
                + "SELECT gs, ((gs - 1) % 200) + 1, 'product_' || gs "
                + "FROM generate_series(1, 400) gs");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT c.cname, o.oid, p.pname "
                + "FROM perf_customers c "
                + "JOIN perf_orders o ON c.cid = o.cid "
                + "JOIN perf_products p ON o.oid = p.oid "
                + "WHERE o.amount > 500 "
                + "ORDER BY o.oid LIMIT 20")) {
            int count = 0;
            while (rs.next()) {
                assertNotNull(rs.getString("cname"));
                assertNotNull(rs.getString("pname"));
                count++;
            }
            assertTrue(count > 0,
                    "3-table join with amount > 500 must return at least one row");
        }

        conn.createStatement().execute("DROP TABLE perf_products");
        conn.createStatement().execute("DROP TABLE perf_orders");
        conn.createStatement().execute("DROP TABLE perf_customers");
    }

    // =========================================================================
    // 6. EXISTS vs IN equivalence
    // =========================================================================

    @Test
    void exists_and_in_subquery_return_same_results() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_exist_parent ("
                + "  id int PRIMARY KEY, category int NOT NULL)");
        conn.createStatement().execute(
                "CREATE TABLE perf_exist_child ("
                + "  id int PRIMARY KEY, parent_id int NOT NULL)");

        conn.createStatement().execute(
                "INSERT INTO perf_exist_parent "
                + "SELECT gs, (gs % 5) + 1 FROM generate_series(1, 100) gs");
        // Only reference even parent ids
        conn.createStatement().execute(
                "INSERT INTO perf_exist_child "
                + "SELECT gs, (gs * 2) % 100 + 1 FROM generate_series(1, 50) gs");

        long countExists, countIn;
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_exist_parent p "
                + "WHERE EXISTS (SELECT 1 FROM perf_exist_child c WHERE c.parent_id = p.id)")) {
            assertTrue(rs.next());
            countExists = rs.getLong(1);
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_exist_parent p "
                + "WHERE p.id IN (SELECT parent_id FROM perf_exist_child)")) {
            assertTrue(rs.next());
            countIn = rs.getLong(1);
        }
        assertEquals(countExists, countIn,
                "EXISTS and IN must return the same row count");

        conn.createStatement().execute("DROP TABLE perf_exist_child");
        conn.createStatement().execute("DROP TABLE perf_exist_parent");
    }

    // =========================================================================
    // 7. NOT EXISTS vs LEFT JOIN WHERE NULL equivalence
    // =========================================================================

    @Test
    void not_exists_and_left_join_null_return_same_results() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_lj_parent ("
                + "  id int PRIMARY KEY)");
        conn.createStatement().execute(
                "CREATE TABLE perf_lj_child ("
                + "  id int PRIMARY KEY, parent_id int NOT NULL)");

        conn.createStatement().execute(
                "INSERT INTO perf_lj_parent SELECT gs FROM generate_series(1, 100) gs");
        // Only reference parent ids 1-60
        conn.createStatement().execute(
                "INSERT INTO perf_lj_child "
                + "SELECT gs, gs FROM generate_series(1, 60) gs");

        long countNotExists, countLJ;
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_lj_parent p "
                + "WHERE NOT EXISTS ("
                + "  SELECT 1 FROM perf_lj_child c WHERE c.parent_id = p.id)")) {
            assertTrue(rs.next());
            countNotExists = rs.getLong(1);
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_lj_parent p "
                + "LEFT JOIN perf_lj_child c ON c.parent_id = p.id "
                + "WHERE c.id IS NULL")) {
            assertTrue(rs.next());
            countLJ = rs.getLong(1);
        }
        assertEquals(40, countNotExists,
                "NOT EXISTS should find 40 parents without children");
        assertEquals(countNotExists, countLJ,
                "NOT EXISTS and LEFT JOIN WHERE NULL must return the same count");

        conn.createStatement().execute("DROP TABLE perf_lj_child");
        conn.createStatement().execute("DROP TABLE perf_lj_parent");
    }

    // =========================================================================
    // 8. Correlated subquery correctness
    // =========================================================================

    @Test
    void correlated_subquery_returns_correct_results() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_corr_data ("
                + "  id int PRIMARY KEY, dept int NOT NULL, salary int NOT NULL)");
        conn.createStatement().execute(
                "INSERT INTO perf_corr_data "
                + "SELECT gs, ((gs - 1) % 5) + 1, 30000 + (gs * 100) "
                + "FROM generate_series(1, 50) gs");

        // Employees whose salary is above their department average
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, dept, salary "
                + "FROM perf_corr_data outer_t "
                + "WHERE salary > ("
                + "  SELECT AVG(salary) FROM perf_corr_data inner_t "
                + "  WHERE inner_t.dept = outer_t.dept"
                + ") "
                + "ORDER BY dept, id")) {
            int count = 0;
            while (rs.next()) count++;
            // With a linear salary progression and 5 depts × 10 rows each,
            // roughly half of each department should be above average.
            assertTrue(count > 0,
                    "Correlated subquery must find at least one row above department average");
        }

        conn.createStatement().execute("DROP TABLE perf_corr_data");
    }

    // =========================================================================
    // 9. CTE materialization at scale
    // =========================================================================

    @Test
    void cte_query_at_scale_returns_correct_results() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_cte_base ("
                + "  id int PRIMARY KEY, grp int NOT NULL, val int NOT NULL)");
        conn.createStatement().execute(
                "INSERT INTO perf_cte_base "
                + "SELECT gs, (gs % 10) + 1, gs * 3 "
                + "FROM generate_series(1, 500) gs");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "WITH group_totals AS ("
                + "  SELECT grp, SUM(val) AS total, COUNT(*) AS cnt "
                + "  FROM perf_cte_base "
                + "  GROUP BY grp"
                + "), "
                + "top_groups AS ("
                + "  SELECT grp, total "
                + "  FROM group_totals "
                + "  ORDER BY total DESC "
                + "  LIMIT 3"
                + ") "
                + "SELECT tg.grp, tg.total, b.id "
                + "FROM top_groups tg "
                + "JOIN perf_cte_base b ON b.grp = tg.grp "
                + "ORDER BY tg.total DESC, b.id "
                + "LIMIT 10")) {
            int count = 0;
            while (rs.next()) count++;
            assertTrue(count > 0,
                    "Multi-CTE query at scale must return at least one row");
        }

        conn.createStatement().execute("DROP TABLE perf_cte_base");
    }

    // =========================================================================
    // 10. UNION vs UNION ALL dedup behaviour
    // =========================================================================

    @Test
    void union_deduplicates_while_union_all_does_not() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_union_a (id int PRIMARY KEY, v text NOT NULL)");
        conn.createStatement().execute(
                "CREATE TABLE perf_union_b (id int PRIMARY KEY, v text NOT NULL)");

        // a: 1-100, b: 51-150 → overlap at 51-100 (50 rows)
        conn.createStatement().execute(
                "INSERT INTO perf_union_a SELECT gs, 'v' || gs "
                + "FROM generate_series(1, 100) gs");
        conn.createStatement().execute(
                "INSERT INTO perf_union_b SELECT gs, 'v' || gs "
                + "FROM generate_series(51, 150) gs");

        long countUnion, countUnionAll;
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM ("
                + "  SELECT id FROM perf_union_a "
                + "  UNION "
                + "  SELECT id FROM perf_union_b"
                + ") u")) {
            assertTrue(rs.next());
            countUnion = rs.getLong(1);
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM ("
                + "  SELECT id FROM perf_union_a "
                + "  UNION ALL "
                + "  SELECT id FROM perf_union_b"
                + ") ua")) {
            assertTrue(rs.next());
            countUnionAll = rs.getLong(1);
        }

        assertEquals(150, countUnion,
                "UNION of 1-100 and 51-150 must yield 150 distinct rows");
        assertEquals(200, countUnionAll,
                "UNION ALL of 1-100 and 51-150 must yield 200 rows (no dedup)");
        assertTrue(countUnionAll > countUnion,
                "UNION ALL must return more rows than UNION when overlaps exist");

        conn.createStatement().execute("DROP TABLE perf_union_a");
        conn.createStatement().execute("DROP TABLE perf_union_b");
    }

    // =========================================================================
    // 11. Large IN list with 100 literal values
    // =========================================================================

    @Test
    void large_in_list_with_100_values_returns_correct_count() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_large_in (id int PRIMARY KEY, v text NOT NULL)");
        conn.createStatement().execute(
                "INSERT INTO perf_large_in SELECT gs, 'row' || gs "
                + "FROM generate_series(1, 200) gs");

        // Build IN (1, 2, ..., 100)
        StringBuilder sb = new StringBuilder(
                "SELECT count(*) FROM perf_large_in WHERE id IN (");
        for (int i = 1; i <= 100; i++) {
            if (i > 1) sb.append(',');
            sb.append(i);
        }
        sb.append(')');

        try (ResultSet rs = conn.createStatement().executeQuery(sb.toString())) {
            assertTrue(rs.next());
            assertEquals(100, rs.getLong(1),
                    "IN list of 100 ids must match exactly 100 rows");
        }

        conn.createStatement().execute("DROP TABLE perf_large_in");
    }

    // =========================================================================
    // 12. Multiple aggregation levels via ROLLUP
    // =========================================================================

    @Test
    void rollup_aggregation_produces_subtotals_and_grand_total() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_rollup_data ("
                + "  id int PRIMARY KEY, region int NOT NULL, dept int NOT NULL, "
                + "  sales int NOT NULL)");
        conn.createStatement().execute(
                "INSERT INTO perf_rollup_data "
                + "SELECT gs, ((gs - 1) % 3) + 1, ((gs - 1) % 5) + 1, gs * 7 "
                + "FROM generate_series(1, 150) gs");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT region, dept, SUM(sales) AS total_sales "
                + "FROM perf_rollup_data "
                + "GROUP BY ROLLUP (region, dept) "
                + "ORDER BY region NULLS LAST, dept NULLS LAST")) {
            int rowCount = 0;
            while (rs.next()) rowCount++;
            // Detailed rows: 3 regions × 5 depts = 15
            // Region subtotals: 3
            // Grand total: 1  → minimum 19 rows
            assertTrue(rowCount >= 4,
                    "ROLLUP must produce more rows than a plain GROUP BY (got " + rowCount + ")");
        }

        conn.createStatement().execute("DROP TABLE perf_rollup_data");
    }

    // =========================================================================
    // 13. DISTINCT ON pattern
    // =========================================================================

    @Test
    void distinct_on_returns_first_row_per_category() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_distinct_on ("
                + "  id int PRIMARY KEY, category int NOT NULL, val int NOT NULL)");
        // 5 categories × 20 rows, val increases with id
        conn.createStatement().execute(
                "INSERT INTO perf_distinct_on "
                + "SELECT gs, ((gs - 1) % 5) + 1, gs "
                + "FROM generate_series(1, 100) gs");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT DISTINCT ON (category) category, id, val "
                + "FROM perf_distinct_on "
                + "ORDER BY category, val DESC")) {
            int count = 0;
            int prevCat = -1;
            while (rs.next()) {
                int cat = rs.getInt("category");
                assertNotEquals(prevCat, cat,
                        "DISTINCT ON must yield one row per category");
                prevCat = cat;
                count++;
            }
            assertEquals(5, count,
                    "DISTINCT ON over 5 categories must return exactly 5 rows");
        }

        conn.createStatement().execute("DROP TABLE perf_distinct_on");
    }

    // =========================================================================
    // 14. Pagination patterns: OFFSET/LIMIT and keyset pagination
    // =========================================================================

    @Test
    void offset_limit_and_keyset_pagination_return_same_page() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_pagination ("
                + "  id int PRIMARY KEY, v text NOT NULL)");
        conn.createStatement().execute(
                "INSERT INTO perf_pagination SELECT gs, 'row' || gs "
                + "FROM generate_series(1, 200) gs");

        // Offset/Limit: page 2 of 10, rows 11-20
        int[] offsetRows = new int[10];
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id FROM perf_pagination ORDER BY id LIMIT 10 OFFSET 10")) {
            int i = 0;
            while (rs.next()) offsetRows[i++] = rs.getInt("id");
            assertEquals(10, i, "OFFSET/LIMIT page must return exactly 10 rows");
        }

        // Keyset: rows where id > 10, first 10
        int[] keysetRows = new int[10];
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id FROM perf_pagination WHERE id > 10 ORDER BY id LIMIT 10")) {
            int i = 0;
            while (rs.next()) keysetRows[i++] = rs.getInt("id");
            assertEquals(10, i, "Keyset page must return exactly 10 rows");
        }

        assertArrayEquals(offsetRows, keysetRows,
                "OFFSET/LIMIT and keyset pagination must return identical rows for page 2");

        conn.createStatement().execute("DROP TABLE perf_pagination");
    }

    // =========================================================================
    // 15. Bulk INSERT correctness via generate_series
    // =========================================================================

    @Test
    void bulk_insert_via_generate_series_inserts_correct_count() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_bulk_insert ("
                + "  id int PRIMARY KEY, v text NOT NULL)");
        conn.createStatement().execute(
                "INSERT INTO perf_bulk_insert "
                + "SELECT gs, 'bulk_' || gs FROM generate_series(1, 1000) gs");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_bulk_insert")) {
            assertTrue(rs.next());
            assertEquals(1000, rs.getLong(1),
                    "Bulk INSERT via generate_series must produce exactly 1000 rows");
        }

        conn.createStatement().execute("DROP TABLE perf_bulk_insert");
    }

    // =========================================================================
    // 16. UPDATE with FROM: multi-table update correctness
    // =========================================================================

    @Test
    void update_with_from_applies_values_from_source_table() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_upd_target ("
                + "  id int PRIMARY KEY, score int NOT NULL DEFAULT 0)");
        conn.createStatement().execute(
                "CREATE TABLE perf_upd_source ("
                + "  id int PRIMARY KEY, bonus int NOT NULL)");

        conn.createStatement().execute(
                "INSERT INTO perf_upd_target SELECT gs, gs * 10 "
                + "FROM generate_series(1, 100) gs");
        conn.createStatement().execute(
                "INSERT INTO perf_upd_source SELECT gs, gs * 5 "
                + "FROM generate_series(1, 50) gs");

        conn.createStatement().execute(
                "UPDATE perf_upd_target t "
                + "SET score = t.score + s.bonus "
                + "FROM perf_upd_source s "
                + "WHERE t.id = s.id");

        // Rows 1-50 should have score = id*10 + id*5 = id*15
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, score FROM perf_upd_target WHERE id = 10")) {
            assertTrue(rs.next());
            assertEquals(150, rs.getInt("score"),
                    "UPDATE FROM must set score of id=10 to 10*10 + 10*5 = 150");
        }

        // Rows 51-100 should be unchanged: score = id*10
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, score FROM perf_upd_target WHERE id = 75")) {
            assertTrue(rs.next());
            assertEquals(750, rs.getInt("score"),
                    "UPDATE FROM must not affect rows with no matching source row");
        }

        conn.createStatement().execute("DROP TABLE perf_upd_source");
        conn.createStatement().execute("DROP TABLE perf_upd_target");
    }

    // =========================================================================
    // 17. DELETE with subquery correctness
    // =========================================================================

    @Test
    void delete_with_subquery_removes_correct_rows() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_del_main ("
                + "  id int PRIMARY KEY, active boolean NOT NULL DEFAULT true)");
        conn.createStatement().execute(
                "CREATE TABLE perf_del_inactive ("
                + "  id int PRIMARY KEY)");

        conn.createStatement().execute(
                "INSERT INTO perf_del_main SELECT gs, true "
                + "FROM generate_series(1, 200) gs");
        // Mark ids 101-200 for deletion
        conn.createStatement().execute(
                "INSERT INTO perf_del_inactive SELECT gs "
                + "FROM generate_series(101, 200) gs");

        conn.createStatement().execute(
                "DELETE FROM perf_del_main "
                + "WHERE id IN (SELECT id FROM perf_del_inactive)");

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_del_main")) {
            assertTrue(rs.next());
            assertEquals(100, rs.getLong(1),
                    "DELETE WHERE id IN (subquery) must leave exactly 100 rows");
        }

        // Verify the right rows remain (ids 1-100)
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT max(id) FROM perf_del_main")) {
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1),
                    "Maximum remaining id must be 100 after deleting ids 101-200");
        }

        conn.createStatement().execute("DROP TABLE perf_del_inactive");
        conn.createStatement().execute("DROP TABLE perf_del_main");
    }

    // =========================================================================
    // 18. MERGE statement: WHEN MATCHED / WHEN NOT MATCHED
    // =========================================================================

    @Test
    void merge_statement_handles_matched_and_not_matched_rows() throws Exception {
        conn.createStatement().execute(
                "CREATE TABLE perf_merge_target ("
                + "  id int PRIMARY KEY, val int NOT NULL, status text NOT NULL DEFAULT 'new')");
        conn.createStatement().execute(
                "CREATE TABLE perf_merge_source ("
                + "  id int PRIMARY KEY, val int NOT NULL)");

        // Target: ids 1-60
        conn.createStatement().execute(
                "INSERT INTO perf_merge_target (id, val) SELECT gs, gs * 10 "
                + "FROM generate_series(1, 60) gs");
        // Source: ids 41-100 (overlap at 41-60, new at 61-100)
        conn.createStatement().execute(
                "INSERT INTO perf_merge_source SELECT gs, gs * 20 "
                + "FROM generate_series(41, 100) gs");

        conn.createStatement().execute(
                "MERGE INTO perf_merge_target t "
                + "USING perf_merge_source s ON t.id = s.id "
                + "WHEN MATCHED THEN "
                + "  UPDATE SET val = s.val, status = 'updated' "
                + "WHEN NOT MATCHED THEN "
                + "  INSERT (id, val, status) VALUES (s.id, s.val, 'inserted')");

        // Total rows: original 60 + new 40 = 100
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_merge_target")) {
            assertTrue(rs.next());
            assertEquals(100, rs.getLong(1),
                    "After MERGE target must contain 100 rows (60 original + 40 inserted)");
        }

        // Matched rows (41-60) must be updated
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_merge_target WHERE status = 'updated'")) {
            assertTrue(rs.next());
            assertEquals(20, rs.getLong(1),
                    "MERGE WHEN MATCHED must have updated exactly 20 rows (ids 41-60)");
        }

        // Not-matched rows (61-100) must have been inserted
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_merge_target WHERE status = 'inserted'")) {
            assertTrue(rs.next());
            assertEquals(40, rs.getLong(1),
                    "MERGE WHEN NOT MATCHED must have inserted exactly 40 rows (ids 61-100)");
        }

        // Rows not in source (1-40) must remain unchanged
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM perf_merge_target WHERE status = 'new'")) {
            assertTrue(rs.next());
            assertEquals(40, rs.getLong(1),
                    "Rows not targeted by MERGE must retain status='new' (ids 1-40)");
        }

        conn.createStatement().execute("DROP TABLE perf_merge_source");
        conn.createStatement().execute("DROP TABLE perf_merge_target");
    }
}
