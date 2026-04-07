package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 4: CTEs (WITH clause) and Window Functions tests.
 */
class CTEsAndWindowFunctionsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, department TEXT, salary INTEGER)");
            stmt.execute("INSERT INTO employees (name, department, salary) VALUES ('Alice', 'Engineering', 90000)");
            stmt.execute("INSERT INTO employees (name, department, salary) VALUES ('Bob', 'Engineering', 85000)");
            stmt.execute("INSERT INTO employees (name, department, salary) VALUES ('Charlie', 'Sales', 70000)");
            stmt.execute("INSERT INTO employees (name, department, salary) VALUES ('Diana', 'Sales', 75000)");
            stmt.execute("INSERT INTO employees (name, department, salary) VALUES ('Eve', 'Engineering', 95000)");
            stmt.execute("INSERT INTO employees (name, department, salary) VALUES ('Frank', 'HR', 65000)");

            // Scores table for rank tests
            stmt.execute("CREATE TABLE scores (student TEXT, score INTEGER)");
            stmt.execute("INSERT INTO scores VALUES ('A', 100)");
            stmt.execute("INSERT INTO scores VALUES ('B', 90)");
            stmt.execute("INSERT INTO scores VALUES ('C', 90)");
            stmt.execute("INSERT INTO scores VALUES ('D', 80)");

            // Categories table for recursive CTE tests
            stmt.execute("CREATE TABLE categories (id INTEGER, name TEXT, parent_id INTEGER)");
            stmt.execute("INSERT INTO categories VALUES (1, 'Root', NULL)");
            stmt.execute("INSERT INTO categories VALUES (2, 'Electronics', 1)");
            stmt.execute("INSERT INTO categories VALUES (3, 'Phones', 2)");
            stmt.execute("INSERT INTO categories VALUES (4, 'Laptops', 2)");
            stmt.execute("INSERT INTO categories VALUES (5, 'Clothing', 1)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- CTE Tests ----

    @Test
    void testSimpleCTE() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH eng AS (SELECT name, salary FROM employees WHERE department = 'Engineering') " +
                     "SELECT name, salary FROM eng ORDER BY salary DESC")) {
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertEquals(95000, rs.getInt("salary"));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(90000, rs.getInt("salary"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name")); assertEquals(85000, rs.getInt("salary"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testCTEWithAggregation() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH dept_stats AS (" +
                     "  SELECT department, AVG(salary) AS avg_salary, COUNT(*) AS cnt " +
                     "  FROM employees GROUP BY department" +
                     ") " +
                     "SELECT department, avg_salary FROM dept_stats WHERE cnt > 1 ORDER BY department")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString("department"));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString("department"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testMultipleCTEs() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH eng AS (" +
                     "  SELECT name, salary FROM employees WHERE department = 'Engineering'" +
                     "), sales AS (" +
                     "  SELECT name, salary FROM employees WHERE department = 'Sales'" +
                     ") " +
                     "SELECT name FROM eng WHERE salary > 87000 " +
                     "UNION " +
                     "SELECT name FROM sales WHERE salary > 72000 " +
                     "ORDER BY 1")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Eve", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void testCTEWithColumnAliases() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH data(emp_name, emp_sal) AS (" +
                     "  SELECT name, salary FROM employees WHERE department = 'HR'" +
                     ") " +
                     "SELECT emp_name, emp_sal FROM data")) {
            assertTrue(rs.next());
            assertEquals("Frank", rs.getString("emp_name"));
            assertEquals(65000, rs.getInt("emp_sal"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testCTEReferencedMultipleTimes() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH dept_avg AS (" +
                     "  SELECT department, AVG(salary) AS avg_sal FROM employees GROUP BY department" +
                     ") " +
                     "SELECT d1.department, d1.avg_sal FROM dept_avg d1 " +
                     "WHERE d1.avg_sal = (SELECT MAX(d2.avg_sal) FROM dept_avg d2)")) {
            assertTrue(rs.next());
            assertEquals("Engineering", rs.getString("department"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testRecursiveCTE() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH RECURSIVE tree(id, name, depth) AS (" +
                     "  SELECT id, name, 0 FROM categories WHERE parent_id IS NULL " +
                     "  UNION ALL " +
                     "  SELECT c.id, c.name, t.depth + 1 FROM categories c JOIN tree t ON c.parent_id = t.id" +
                     ") " +
                     "SELECT name, depth FROM tree ORDER BY id")) {
            assertTrue(rs.next()); assertEquals("Root", rs.getString("name")); assertEquals(0, rs.getInt("depth"));
            assertTrue(rs.next()); assertEquals("Electronics", rs.getString("name")); assertEquals(1, rs.getInt("depth"));
            assertTrue(rs.next()); assertEquals("Phones", rs.getString("name")); assertEquals(2, rs.getInt("depth"));
            assertTrue(rs.next()); assertEquals("Laptops", rs.getString("name")); assertEquals(2, rs.getInt("depth"));
            assertTrue(rs.next()); assertEquals("Clothing", rs.getString("name")); assertEquals(1, rs.getInt("depth"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testRecursiveCTENumbers() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH RECURSIVE nums(n) AS (" +
                     "  SELECT 1 " +
                     "  UNION ALL " +
                     "  SELECT n + 1 FROM nums WHERE n < 5" +
                     ") " +
                     "SELECT n FROM nums ORDER BY n")) {
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt("n"));
            }
            assertFalse(rs.next());
        }
    }

    // ---- Window Function Tests ----

    @Test
    void testRowNumber() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn " +
                     "FROM employees ORDER BY rn")) {
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertEquals(1L, rs.getLong("rn"));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(2L, rs.getLong("rn"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name")); assertEquals(3L, rs.getLong("rn"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals(4L, rs.getLong("rn"));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name")); assertEquals(5L, rs.getLong("rn"));
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertEquals(6L, rs.getLong("rn"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testRowNumberPartitioned() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, department, salary, " +
                     "ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank " +
                     "FROM employees ORDER BY department, dept_rank")) {
            // Engineering: Eve(1), Alice(2), Bob(3)
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertEquals(1L, rs.getLong("dept_rank"));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(2L, rs.getLong("dept_rank"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name")); assertEquals(3L, rs.getLong("dept_rank"));
            // HR: Frank(1)
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertEquals(1L, rs.getLong("dept_rank"));
            // Sales: Diana(1), Charlie(2)
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals(1L, rs.getLong("dept_rank"));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name")); assertEquals(2L, rs.getLong("dept_rank"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testRank() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT student, score, RANK() OVER (ORDER BY score DESC) AS rnk " +
                     "FROM scores ORDER BY rnk, student")) {
            assertTrue(rs.next()); assertEquals("A", rs.getString("student")); assertEquals(1L, rs.getLong("rnk"));
            assertTrue(rs.next()); assertEquals("B", rs.getString("student")); assertEquals(2L, rs.getLong("rnk"));
            assertTrue(rs.next()); assertEquals("C", rs.getString("student")); assertEquals(2L, rs.getLong("rnk"));
            assertTrue(rs.next()); assertEquals("D", rs.getString("student")); assertEquals(4L, rs.getLong("rnk"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testDenseRank() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT student, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk " +
                     "FROM scores ORDER BY drnk, student")) {
            assertTrue(rs.next()); assertEquals("A", rs.getString("student")); assertEquals(1L, rs.getLong("drnk"));
            assertTrue(rs.next()); assertEquals("B", rs.getString("student")); assertEquals(2L, rs.getLong("drnk"));
            assertTrue(rs.next()); assertEquals("C", rs.getString("student")); assertEquals(2L, rs.getLong("drnk"));
            assertTrue(rs.next()); assertEquals("D", rs.getString("student")); assertEquals(3L, rs.getLong("drnk"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testLag() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, salary, " +
                     "LAG(salary) OVER (ORDER BY salary) AS prev_salary " +
                     "FROM employees ORDER BY salary")) {
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertNull(rs.getObject("prev_salary"));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name")); assertEquals(65000, rs.getInt("prev_salary"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals(70000, rs.getInt("prev_salary"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name")); assertEquals(75000, rs.getInt("prev_salary"));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(85000, rs.getInt("prev_salary"));
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertEquals(90000, rs.getInt("prev_salary"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testLead() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, salary, " +
                     "LEAD(salary) OVER (ORDER BY salary) AS next_salary " +
                     "FROM employees ORDER BY salary")) {
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertEquals(70000, rs.getInt("next_salary"));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name")); assertEquals(75000, rs.getInt("next_salary"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals(85000, rs.getInt("next_salary"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name")); assertEquals(90000, rs.getInt("next_salary"));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(95000, rs.getInt("next_salary"));
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertNull(rs.getObject("next_salary"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testSumOverWindow() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, salary, " +
                     "SUM(salary) OVER (ORDER BY salary) AS running_total " +
                     "FROM employees ORDER BY salary")) {
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertEquals(65000L, rs.getLong("running_total"));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name")); assertEquals(135000L, rs.getLong("running_total"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals(210000L, rs.getLong("running_total"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name")); assertEquals(295000L, rs.getLong("running_total"));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(385000L, rs.getLong("running_total"));
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertEquals(480000L, rs.getLong("running_total"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testSumOverPartition() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, department, salary, " +
                     "SUM(salary) OVER (PARTITION BY department) AS dept_total " +
                     "FROM employees ORDER BY department, name")) {
            // Engineering total = 90000+85000+95000 = 270000
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(270000L, rs.getLong("dept_total"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name")); assertEquals(270000L, rs.getLong("dept_total"));
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertEquals(270000L, rs.getLong("dept_total"));
            // HR total = 65000
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertEquals(65000L, rs.getLong("dept_total"));
            // Sales total = 70000+75000 = 145000
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name")); assertEquals(145000L, rs.getLong("dept_total"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals(145000L, rs.getLong("dept_total"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testCountOverWindow() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT department, COUNT(*) OVER (PARTITION BY department) AS dept_count " +
                     "FROM employees ORDER BY department, dept_count")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString("department")); assertEquals(3L, rs.getLong("dept_count"));
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString("department")); assertEquals(3L, rs.getLong("dept_count"));
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString("department")); assertEquals(3L, rs.getLong("dept_count"));
            assertTrue(rs.next()); assertEquals("HR", rs.getString("department")); assertEquals(1L, rs.getLong("dept_count"));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString("department")); assertEquals(2L, rs.getLong("dept_count"));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString("department")); assertEquals(2L, rs.getLong("dept_count"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testFirstValue() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, department, salary, " +
                     "FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) AS top_earner " +
                     "FROM employees ORDER BY department, salary DESC")) {
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertEquals("Eve", rs.getString("top_earner"));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals("Eve", rs.getString("top_earner"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name")); assertEquals("Eve", rs.getString("top_earner"));
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertEquals("Frank", rs.getString("top_earner"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals("Diana", rs.getString("top_earner"));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name")); assertEquals("Diana", rs.getString("top_earner"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testNtile() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, salary, NTILE(3) OVER (ORDER BY salary DESC) AS quartile " +
                     "FROM employees ORDER BY salary DESC")) {
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertEquals(1L, rs.getLong("quartile"));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name")); assertEquals(1L, rs.getLong("quartile"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name")); assertEquals(2L, rs.getLong("quartile"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals(2L, rs.getLong("quartile"));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name")); assertEquals(3L, rs.getLong("quartile"));
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertEquals(3L, rs.getLong("quartile"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testWindowWithFrame() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, salary, " +
                     "SUM(salary) OVER (ORDER BY salary ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_sum " +
                     "FROM employees ORDER BY salary")) {
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name"));
            // Frank: 65000+70000 = 135000
            assertEquals(135000L, rs.getLong("moving_sum"));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name"));
            // Charlie: 65000+70000+75000 = 210000
            assertEquals(210000L, rs.getLong("moving_sum"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name"));
            // Diana: 70000+75000+85000 = 230000
            assertEquals(230000L, rs.getLong("moving_sum"));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString("name"));
            // Bob: 75000+85000+90000 = 250000
            assertEquals(250000L, rs.getLong("moving_sum"));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString("name"));
            // Alice: 85000+90000+95000 = 270000
            assertEquals(270000L, rs.getLong("moving_sum"));
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name"));
            // Eve: 90000+95000 = 185000
            assertEquals(185000L, rs.getLong("moving_sum"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testAvgOverWindow() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT department, AVG(salary) OVER (PARTITION BY department) AS dept_avg " +
                     "FROM employees WHERE department = 'Engineering' ORDER BY department LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals(90000.0, rs.getDouble("dept_avg"), 0.01);
            assertFalse(rs.next());
        }
    }

    @Test
    void testWindowFunctionWithCTE() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH ranked AS (" +
                     "  SELECT name, department, salary, " +
                     "  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn " +
                     "  FROM employees" +
                     ") " +
                     "SELECT name, department, salary FROM ranked WHERE rn = 1 ORDER BY department")) {
            assertTrue(rs.next()); assertEquals("Eve", rs.getString("name")); assertEquals("Engineering", rs.getString("department"));
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertEquals("HR", rs.getString("department"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals("Sales", rs.getString("department"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testLagWithDefault() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, salary, " +
                     "LAG(salary, 1, 0) OVER (ORDER BY salary) AS prev_salary " +
                     "FROM employees ORDER BY salary LIMIT 3")) {
            assertTrue(rs.next()); assertEquals("Frank", rs.getString("name")); assertEquals(0, rs.getInt("prev_salary"));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString("name")); assertEquals(65000, rs.getInt("prev_salary"));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString("name")); assertEquals(70000, rs.getInt("prev_salary"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testMultipleWindowFunctions() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name, salary, " +
                     "ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn, " +
                     "SUM(salary) OVER () AS total " +
                     "FROM employees ORDER BY rn LIMIT 3")) {
            assertTrue(rs.next());
            assertEquals("Eve", rs.getString("name"));
            assertEquals(1L, rs.getLong("rn"));
            assertEquals(480000L, rs.getLong("total"));
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertEquals(2L, rs.getLong("rn"));
            assertEquals(480000L, rs.getLong("total"));
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("name"));
            assertEquals(3L, rs.getLong("rn"));
            assertEquals(480000L, rs.getLong("total"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testMinMaxOverWindow() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT department, " +
                     "MIN(salary) OVER (PARTITION BY department) AS min_sal, " +
                     "MAX(salary) OVER (PARTITION BY department) AS max_sal " +
                     "FROM employees WHERE department = 'Engineering' LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals(85000, rs.getInt("min_sal"));
            assertEquals(95000, rs.getInt("max_sal"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testCTEInSubquery() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH high_sal AS (" +
                     "  SELECT name, salary FROM employees WHERE salary > 80000" +
                     ") " +
                     "SELECT COUNT(*) AS cnt FROM high_sal")) {
            assertTrue(rs.next());
            assertEquals(3L, rs.getLong("cnt"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testRecursiveCTEFibonacci() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "WITH RECURSIVE fib(n, a, b) AS (" +
                     "  SELECT 1, 0, 1 " +
                     "  UNION ALL " +
                     "  SELECT n + 1, b, a + b FROM fib WHERE n < 8" +
                     ") " +
                     "SELECT n, a FROM fib ORDER BY n")) {
            int[] expected = {0, 1, 1, 2, 3, 5, 8, 13};
            for (int i = 0; i < expected.length; i++) {
                assertTrue(rs.next());
                assertEquals(i + 1, rs.getInt("n"));
                assertEquals(expected[i], rs.getInt("a"));
            }
            assertFalse(rs.next());
        }
    }
}
