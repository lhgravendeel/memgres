package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class WindowFunctionsCompatTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE wf_data (id integer PRIMARY KEY, dept text, salary integer)");
            stmt.execute("INSERT INTO wf_data VALUES " +
                    "(1, 'eng', 80000), (2, 'eng', 90000), (3, 'eng', 100000), " +
                    "(4, 'sales', 60000), (5, 'sales', 70000), (6, 'sales', 80000), " +
                    "(7, 'hr', 55000), (8, 'hr', 65000)");
            stmt.execute("CREATE TABLE wf_groups (id integer, score integer)");
            stmt.execute("INSERT INTO wf_groups VALUES " +
                    "(1, 10), (2, 10), (3, 20), (4, 30), (5, 30)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("nth_value should return NULL for first row with default frame")
    void testNthValueReturnsNullForFirstRow() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, salary, nth_value(salary, 2) OVER (ORDER BY id) AS second_sal " +
                     "FROM wf_data WHERE dept = 'eng' ORDER BY id")) {
            assertTrue(rs.next());
            rs.getInt("second_sal");
            assertTrue(rs.wasNull(),
                    "nth_value(salary, 2) should be NULL for first row with default frame");
        }
    }

    @Test
    @DisplayName("ntile should assign correct bucket numbers")
    void testNtileCorrectBucketAssignment() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, salary, ntile(5) OVER (ORDER BY salary) AS bucket " +
                     "FROM wf_data WHERE dept = 'sales' ORDER BY salary")) {
            assertTrue(rs.next()); // row 1
            assertTrue(rs.next()); // row 2
            assertTrue(rs.next()); // row 3 (id=6, salary=80000)
            assertEquals(6, rs.getInt("id"));
            assertEquals(3, rs.getInt("bucket"),
                    "Third row (id=6) should be in bucket 3");
        }
    }

    @Test
    @DisplayName("RANGE frame boundary should count correctly with value offsets")
    void testRangeFrameBoundary() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, salary, count(*) OVER (ORDER BY salary " +
                     "RANGE BETWEEN 10000 PRECEDING AND 10000 FOLLOWING)::integer AS cnt " +
                     "FROM wf_data WHERE dept = 'eng' ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("cnt"),
                    "First row (salary=80000) should see 2 rows in range [70000, 90000]");
        }
    }

    @Test
    @DisplayName("GROUPS frame should aggregate across peer groups")
    void testGroupsFrame() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, score, sum(score) OVER (ORDER BY score " +
                     "GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS grp_sum " +
                     "FROM wf_groups ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(40, rs.getInt("grp_sum"),
                    "First row should sum groups {10,10} and {20} = 40");
        }
    }

    @Test
    @DisplayName("Named WINDOW clauses should order correctly")
    void testNamedWindowOrdering() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, dept, rank() OVER dept_w AS dept_rank, " +
                     "rank() OVER global_w AS global_rank " +
                     "FROM wf_data " +
                     "WINDOW dept_w AS (PARTITION BY dept ORDER BY salary), " +
                     "global_w AS (ORDER BY salary) " +
                     "ORDER BY salary, id")) {
            // Skip to 5th row (salary=80000, id=1 is eng, rank 1 in dept)
            for (int i = 0; i < 5; i++) {
                assertTrue(rs.next());
            }
            assertEquals(1, rs.getInt("id"),
                    "5th row by salary,id should be id=1 (eng, 80000)");
            assertEquals("eng", rs.getString("dept"));
        }
    }

    @Test
    @DisplayName("percent_rank should return numeric values not NULL")
    void testPercentRankReturnsNumeric() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, salary, round(percent_rank() OVER (ORDER BY salary)::numeric, 2) AS pct_rank " +
                     "FROM wf_data WHERE dept = 'eng' ORDER BY salary")) {
            assertTrue(rs.next());
            BigDecimal pctRank = rs.getBigDecimal("pct_rank");
            assertNotNull(pctRank, "percent_rank should not be NULL");
            assertEquals(new BigDecimal("0.00"), pctRank);
        }
    }

    @Test
    @DisplayName("cume_dist should return numeric values not NULL")
    void testCumeDistReturnsNumeric() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id, salary, round(cume_dist() OVER (ORDER BY salary)::numeric, 2) AS cum_dist " +
                     "FROM wf_data WHERE dept = 'eng' ORDER BY salary")) {
            assertTrue(rs.next());
            BigDecimal cumDist = rs.getBigDecimal("cum_dist");
            assertNotNull(cumDist, "cume_dist should not be NULL");
            assertEquals(new BigDecimal("0.33"), cumDist);
        }
    }

    @Test
    @DisplayName("named WINDOW with deterministic ORDER BY should produce correct ranks")
    void testNamedWindowDeterministicOrdering() throws Exception {
        String sql = "SELECT id, dept, rank() OVER dept_w AS dept_rank, "
                + "rank() OVER global_w AS global_rank "
                + "FROM wf_data "
                + "WINDOW dept_w AS (PARTITION BY dept ORDER BY salary), "
                + "global_w AS (ORDER BY salary) "
                + "ORDER BY salary, id";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            for (int i = 0; i < 4; i++) assertTrue(rs.next());
            // Row 5: id=1 (eng, 80000)
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("eng", rs.getString("dept"));
            assertEquals(1, rs.getInt("dept_rank"), "id=1 should be rank 1 in eng");
            assertEquals(5, rs.getInt("global_rank"), "salary 80000 should be rank 5 globally");
            // Row 6: id=6 (sales, 80000)
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("id"));
            assertEquals("sales", rs.getString("dept"));
            assertEquals(3, rs.getInt("dept_rank"), "id=6 should be rank 3 in sales");
            assertEquals(5, rs.getInt("global_rank"), "salary 80000 should also be rank 5");
        }
    }
}
