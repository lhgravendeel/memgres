package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive read query tests: views, materialized views, complex joins,
 * subqueries, aggregations over joins, self-joins, and edge cases.
 */
class ViewsJoinsReadTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        seedData(conn);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** Set up a realistic multi-table schema for testing. */
    private static void seedData(Connection conn) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL, budget DOUBLE PRECISION)");
            s.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT NOT NULL, dept_id INTEGER REFERENCES departments(id), salary INTEGER, hire_date DATE, manager_id INTEGER)");
            s.execute("CREATE TABLE projects (id INTEGER PRIMARY KEY, name TEXT NOT NULL, dept_id INTEGER REFERENCES departments(id), deadline DATE)");
            s.execute("CREATE TABLE assignments (employee_id INTEGER REFERENCES employees(id), project_id INTEGER REFERENCES projects(id), role TEXT)");
            s.execute("CREATE TABLE salaries_log (employee_id INTEGER, old_salary INTEGER, new_salary INTEGER, changed_at DATE)");

            s.execute("INSERT INTO departments VALUES (1, 'Engineering', 500000), (2, 'Sales', 200000), (3, 'HR', 100000), (4, 'Research', 300000)");

            s.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 95000, '2020-01-15', NULL)");
            s.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 85000, '2020-06-01', 1)");
            s.execute("INSERT INTO employees VALUES (3, 'Charlie', 2, 70000, '2021-03-10', NULL)");
            s.execute("INSERT INTO employees VALUES (4, 'Diana', 2, 72000, '2021-05-20', 3)");
            s.execute("INSERT INTO employees VALUES (5, 'Eve', 3, 65000, '2022-01-01', NULL)");
            s.execute("INSERT INTO employees VALUES (6, 'Frank', 1, 90000, '2019-08-15', 1)");
            s.execute("INSERT INTO employees VALUES (7, 'Grace', 4, 110000, '2018-03-01', NULL)");
            s.execute("INSERT INTO employees VALUES (8, 'Hank', 4, 105000, '2019-11-10', 7)");

            s.execute("INSERT INTO projects VALUES (1, 'Project Alpha', 1, '2025-06-30')");
            s.execute("INSERT INTO projects VALUES (2, 'Project Beta', 1, '2025-12-31')");
            s.execute("INSERT INTO projects VALUES (3, 'Sales Push', 2, '2025-03-31')");
            s.execute("INSERT INTO projects VALUES (4, 'HR System', 3, '2025-09-30')");
            s.execute("INSERT INTO projects VALUES (5, 'Lab Work', 4, '2026-01-15')");

            s.execute("INSERT INTO assignments VALUES (1, 1, 'Lead'), (2, 1, 'Developer'), (6, 1, 'Developer')");
            s.execute("INSERT INTO assignments VALUES (1, 2, 'Architect'), (2, 2, 'Developer')");
            s.execute("INSERT INTO assignments VALUES (3, 3, 'Lead'), (4, 3, 'Salesperson')");
            s.execute("INSERT INTO assignments VALUES (5, 4, 'Lead')");
            s.execute("INSERT INTO assignments VALUES (7, 5, 'Researcher'), (8, 5, 'Researcher')");

            s.execute("INSERT INTO salaries_log VALUES (1, 90000, 95000, '2023-01-01')");
            s.execute("INSERT INTO salaries_log VALUES (2, 80000, 85000, '2023-06-01')");
            s.execute("INSERT INTO salaries_log VALUES (6, 85000, 90000, '2023-01-01')");
        }
    }

    // ==================== Basic Views ====================

    @Test
    void testCreateAndQueryView() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE VIEW eng_employees AS SELECT e.id, e.name, e.salary FROM employees e WHERE e.dept_id = 1");
            ResultSet rs = s.executeQuery("SELECT name, salary FROM eng_employees ORDER BY salary DESC");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals(95000, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1)); assertEquals(90000, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals(85000, rs.getInt(2));
            assertFalse(rs.next());
            s.execute("DROP VIEW eng_employees");
        }
    }

    @Test
    void testViewReflectsUnderlyingChanges() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE VIEW dept_count AS SELECT dept_id, COUNT(*) AS cnt FROM employees GROUP BY dept_id");

            ResultSet rs = s.executeQuery("SELECT cnt FROM dept_count WHERE dept_id = 1");
            assertTrue(rs.next());
            int before = rs.getInt(1);

            // Add a new employee to dept 1
            s.execute("INSERT INTO employees VALUES (99, 'Newcomer', 1, 60000, '2025-01-01', NULL)");

            rs = s.executeQuery("SELECT cnt FROM dept_count WHERE dept_id = 1");
            assertTrue(rs.next());
            assertEquals(before + 1, rs.getInt(1), "View should reflect newly inserted row");

            s.execute("DELETE FROM employees WHERE id = 99");
            s.execute("DROP VIEW dept_count");
        }
    }

    @Test
    void testViewWithJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE VIEW employee_departments AS " +
                    "SELECT e.name AS emp_name, d.name AS dept_name, e.salary " +
                    "FROM employees e JOIN departments d ON e.dept_id = d.id");

            ResultSet rs = s.executeQuery("SELECT emp_name, dept_name FROM employee_departments WHERE salary > 100000 ORDER BY emp_name");
            assertTrue(rs.next()); assertEquals("Grace", rs.getString(1)); assertEquals("Research", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Hank", rs.getString(1)); assertEquals("Research", rs.getString(2));
            assertFalse(rs.next());

            s.execute("DROP VIEW employee_departments");
        }
    }

    @Test
    void testViewWithAggregation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE VIEW dept_salary_stats AS " +
                    "SELECT d.name AS dept, AVG(e.salary) AS avg_salary, MAX(e.salary) AS max_salary, COUNT(*) AS headcount " +
                    "FROM employees e JOIN departments d ON e.dept_id = d.id GROUP BY d.name");

            ResultSet rs = s.executeQuery("SELECT dept, headcount FROM dept_salary_stats ORDER BY headcount DESC");
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(3, rs.getInt(2));
            assertTrue(rs.next());
            int secondCount = rs.getInt(2);
            assertTrue(secondCount == 2, "Second dept should have 2 employees");

            s.execute("DROP VIEW dept_salary_stats");
        }
    }

    @Test
    void testViewOnView() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE VIEW high_earners AS SELECT name, salary, dept_id FROM employees WHERE salary >= 90000");
            s.execute("CREATE VIEW high_earner_depts AS SELECT d.name AS dept, COUNT(*) AS cnt FROM high_earners h JOIN departments d ON h.dept_id = d.id GROUP BY d.name");

            ResultSet rs = s.executeQuery("SELECT dept, cnt FROM high_earner_depts ORDER BY cnt DESC");
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Research", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());

            s.execute("DROP VIEW high_earner_depts");
            s.execute("DROP VIEW high_earners");
        }
    }

    @Test
    void testOrReplaceView() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE VIEW replaceable AS SELECT 1 AS val");
            ResultSet rs = s.executeQuery("SELECT val FROM replaceable");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));

            s.execute("CREATE OR REPLACE VIEW replaceable AS SELECT 42 AS val");
            rs = s.executeQuery("SELECT val FROM replaceable");
            assertTrue(rs.next()); assertEquals(42, rs.getInt(1));

            s.execute("DROP VIEW replaceable");
        }
    }

    // ==================== Materialized Views ====================

    @Test
    void testMaterializedViewBasic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE MATERIALIZED VIEW mv_dept_totals AS " +
                    "SELECT d.name AS dept, SUM(e.salary) AS total_salary " +
                    "FROM employees e JOIN departments d ON e.dept_id = d.id " +
                    "GROUP BY d.name");

            ResultSet rs = s.executeQuery("SELECT dept, total_salary FROM mv_dept_totals ORDER BY dept");
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(270000.0, rs.getDouble(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals(65000.0, rs.getDouble(2));

            s.execute("DROP VIEW mv_dept_totals");
        }
    }

    @Test
    void testMaterializedViewDoesNotReflectChanges() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE MATERIALIZED VIEW mv_count AS SELECT COUNT(*) AS cnt FROM employees");

            ResultSet rs = s.executeQuery("SELECT cnt FROM mv_count");
            assertTrue(rs.next());
            int originalCount = rs.getInt(1);

            // Add rows; materialized view should NOT update
            s.execute("INSERT INTO employees VALUES (100, 'Phantom1', 1, 50000, '2025-01-01', NULL)");
            s.execute("INSERT INTO employees VALUES (101, 'Phantom2', 1, 50000, '2025-01-01', NULL)");

            rs = s.executeQuery("SELECT cnt FROM mv_count");
            assertTrue(rs.next());
            assertEquals(originalCount, rs.getInt(1), "Materialized view should serve stale cached data");

            // Clean up
            s.execute("DELETE FROM employees WHERE id IN (100, 101)");
            s.execute("DROP VIEW mv_count");
        }
    }

    @Test
    void testRefreshMaterializedView() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE MATERIALIZED VIEW mv_refresh_test AS SELECT COUNT(*) AS cnt FROM employees");

            ResultSet rs = s.executeQuery("SELECT cnt FROM mv_refresh_test");
            assertTrue(rs.next());
            int before = rs.getInt(1);

            s.execute("INSERT INTO employees VALUES (200, 'Temp', 1, 50000, '2025-01-01', NULL)");

            // Still stale
            rs = s.executeQuery("SELECT cnt FROM mv_refresh_test");
            assertTrue(rs.next());
            assertEquals(before, rs.getInt(1));

            // Refresh; now should reflect new data
            s.execute("REFRESH MATERIALIZED VIEW mv_refresh_test");

            rs = s.executeQuery("SELECT cnt FROM mv_refresh_test");
            assertTrue(rs.next());
            assertEquals(before + 1, rs.getInt(1), "After REFRESH, materialized view should have updated data");

            s.execute("DELETE FROM employees WHERE id = 200");
            s.execute("DROP VIEW mv_refresh_test");
        }
    }

    @Test
    void testMaterializedViewWithComplexQuery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE MATERIALIZED VIEW mv_project_summary AS " +
                    "SELECT p.name AS project, d.name AS dept, COUNT(a.employee_id) AS team_size " +
                    "FROM projects p " +
                    "JOIN departments d ON p.dept_id = d.id " +
                    "LEFT JOIN assignments a ON a.project_id = p.id " +
                    "GROUP BY p.name, d.name");

            ResultSet rs = s.executeQuery("SELECT project, team_size FROM mv_project_summary ORDER BY team_size DESC");
            assertTrue(rs.next());
            assertEquals("Project Alpha", rs.getString(1));
            assertEquals(3, rs.getInt(2));

            s.execute("DROP VIEW mv_project_summary");
        }
    }

    // ==================== Complex Joins ====================

    @Test
    void testThreeTableJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT e.name, p.name AS project, a.role " +
                    "FROM employees e " +
                    "JOIN assignments a ON e.id = a.employee_id " +
                    "JOIN projects p ON a.project_id = p.id " +
                    "WHERE p.name = 'Project Alpha' " +
                    "ORDER BY e.name");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Lead", rs.getString(3));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Developer", rs.getString(3));
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1)); assertEquals("Developer", rs.getString(3));
            assertFalse(rs.next());
        }
    }

    @Test
    void testFourTableJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT e.name AS employee, d.name AS dept, p.name AS project, a.role " +
                    "FROM employees e " +
                    "JOIN departments d ON e.dept_id = d.id " +
                    "JOIN assignments a ON e.id = a.employee_id " +
                    "JOIN projects p ON a.project_id = p.id " +
                    "WHERE d.name = 'Engineering' " +
                    "ORDER BY e.name, p.name");
            int count = 0;
            while (rs.next()) {
                assertEquals("Engineering", rs.getString(2));
                count++;
            }
            assertEquals(5, count, "Engineering has 3 people across 2 projects = 5 assignments");
        }
    }

    @Test
    void testSelfJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT e.name AS employee, m.name AS manager " +
                    "FROM employees e " +
                    "JOIN employees m ON e.manager_id = m.id " +
                    "ORDER BY e.name");
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Alice", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString(1)); assertEquals("Charlie", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1)); assertEquals("Alice", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Hank", rs.getString(1)); assertEquals("Grace", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void testLeftJoinWithNulls() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Find employees with no manager (manager_id IS NULL means no join match on self-join)
            ResultSet rs = s.executeQuery(
                    "SELECT e.name, m.name AS manager " +
                    "FROM employees e " +
                    "LEFT JOIN employees m ON e.manager_id = m.id " +
                    "WHERE m.name IS NULL " +
                    "ORDER BY e.name");
            // Alice, Charlie, Eve, Grace have no manager
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertNull(rs.getString(2));
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Eve", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Grace", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void testRightJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Departments with employee count, including departments with 0 employees
            // (though all our depts have employees)
            ResultSet rs = s.executeQuery(
                    "SELECT d.name, COUNT(e.id) AS cnt " +
                    "FROM employees e " +
                    "RIGHT JOIN departments d ON e.dept_id = d.id " +
                    "GROUP BY d.name " +
                    "ORDER BY d.name");
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(3, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Research", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void testCrossJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT COUNT(*) FROM departments d1 CROSS JOIN departments d2");
            assertTrue(rs.next());
            assertEquals(16, rs.getInt(1), "4 departments cross join = 16 rows");
        }
    }

    // ==================== Subqueries in various positions ====================

    @Test
    void testScalarSubquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT name, salary, " +
                    "(SELECT AVG(salary) FROM employees) AS avg_salary " +
                    "FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) " +
                    "ORDER BY salary DESC");
            assertTrue(rs.next()); assertEquals("Grace", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Hank", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
        }
    }

    @Test
    void testSubqueryInFrom() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT sub.dept_name, sub.max_sal " +
                    "FROM (SELECT d.name AS dept_name, MAX(e.salary) AS max_sal " +
                    "      FROM employees e JOIN departments d ON e.dept_id = d.id " +
                    "      GROUP BY d.name) sub " +
                    "WHERE sub.max_sal > 80000 ORDER BY sub.max_sal DESC");
            assertTrue(rs.next()); assertEquals("Research", rs.getString(1)); assertEquals(110000, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(95000, rs.getInt(2));
        }
    }

    @Test
    void testExistsSubquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Employees who are assigned to at least one project
            ResultSet rs = s.executeQuery(
                    "SELECT e.name FROM employees e " +
                    "WHERE EXISTS (SELECT 1 FROM assignments a WHERE a.employee_id = e.id) " +
                    "ORDER BY e.name");
            int count = 0;
            while (rs.next()) count++;
            assertEquals(8, count, "All 8 employees have at least one assignment");
        }
    }

    @Test
    void testNotExistsSubquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Employees NOT assigned to project Alpha (id=1)
            ResultSet rs = s.executeQuery(
                    "SELECT e.name FROM employees e " +
                    "WHERE NOT EXISTS (SELECT 1 FROM assignments a WHERE a.employee_id = e.id AND a.project_id = 1) " +
                    "ORDER BY e.name");
            // Alice(1), Bob(2), Frank(6) ARE on Alpha. Others are not.
            int count = 0;
            while (rs.next()) count++;
            assertEquals(5, count, "5 employees not on Project Alpha");
        }
    }

    @Test
    void testInSubquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT name FROM departments WHERE id IN " +
                    "(SELECT DISTINCT dept_id FROM employees WHERE salary > 90000) " +
                    "ORDER BY name");
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Research", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // ==================== Aggregation edge cases ====================

    @Test
    void testGroupByWithHaving() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT d.name, COUNT(*) AS cnt, AVG(e.salary) AS avg_sal " +
                    "FROM employees e JOIN departments d ON e.dept_id = d.id " +
                    "GROUP BY d.name HAVING COUNT(*) > 1 ORDER BY cnt DESC");
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(3, rs.getInt(2));
            assertTrue(rs.next());
            int cnt2 = rs.getInt(2);
            assertTrue(cnt2 == 2);
        }
    }

    @Test
    void testCountDistinct() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT COUNT(DISTINCT dept_id) FROM employees");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
    }

    @Test
    void testMultipleAggregates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT MIN(salary), MAX(salary), AVG(salary), SUM(salary), COUNT(*) FROM employees");
            assertTrue(rs.next());
            assertEquals(65000, rs.getInt(1));
            assertEquals(110000, rs.getInt(2));
            assertTrue(rs.getDouble(3) > 80000 && rs.getDouble(3) < 90000);
            assertEquals(692000, rs.getInt(4));
            assertEquals(8, rs.getInt(5));
        }
    }

    // ==================== ORDER BY, LIMIT, OFFSET combinations ====================

    @Test
    void testOrderByMultipleColumns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT dept_id, name FROM employees ORDER BY dept_id ASC, salary DESC");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("Alice", rs.getString(2));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("Frank", rs.getString(2));
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("Bob", rs.getString(2));
        }
    }

    @Test
    void testLimitOffset() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT name FROM employees ORDER BY salary DESC LIMIT 3 OFFSET 2");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void testLimitZero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT name FROM employees LIMIT 0");
            assertFalse(rs.next());
        }
    }

    @Test
    void testOffsetBeyondRowCount() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT name FROM employees OFFSET 1000");
            assertFalse(rs.next());
        }
    }

    // ==================== DISTINCT ====================

    @Test
    void testDistinctOnJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Employees on more than one project appear multiple times without DISTINCT
            ResultSet rs = s.executeQuery(
                    "SELECT DISTINCT e.name FROM employees e " +
                    "JOIN assignments a ON e.id = a.employee_id ORDER BY e.name");
            int count = 0;
            while (rs.next()) count++;
            assertEquals(8, count, "8 unique employees with assignments");
        }
    }

    // ==================== UNION / INTERSECT / EXCEPT ====================

    @Test
    void testUnionAll() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT name FROM employees WHERE dept_id = 1 " +
                    "UNION ALL " +
                    "SELECT name FROM employees WHERE salary > 100000 " +
                    "ORDER BY name");
            // Engineering: Alice, Bob, Frank. Salary>100k: Grace, Hank. Alice appears once (not in both).
            // Actually Alice (95k) NOT > 100k. Frank (90k) NOT > 100k. So union: Alice, Bob, Frank, Grace, Hank = 5
            int count = 0;
            while (rs.next()) count++;
            assertEquals(5, count);
        }
    }

    @Test
    void testUnionDistinct() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Same query but employee who is in Engineering AND earns > 90k = Alice (95k), Frank (90k is not > 90k)
            // Only Alice in both. With UNION (not ALL), duplicates removed.
            ResultSet rs = s.executeQuery(
                    "SELECT name FROM employees WHERE dept_id = 1 " +
                    "UNION " +
                    "SELECT name FROM employees WHERE salary > 90000 " +
                    "ORDER BY name");
            // Eng: Alice, Bob, Frank. >90k: Alice, Grace, Hank, 105k=Hank. Union: Alice, Bob, Frank, Grace, Hank = 5
            int count = 0;
            while (rs.next()) count++;
            assertEquals(5, count);
        }
    }

    // ==================== CASE expressions ====================

    @Test
    void testCaseInSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT name, CASE " +
                    "WHEN salary >= 100000 THEN 'senior' " +
                    "WHEN salary >= 80000 THEN 'mid' " +
                    "ELSE 'junior' END AS level " +
                    "FROM employees ORDER BY salary DESC");
            assertTrue(rs.next()); assertEquals("Grace", rs.getString(1)); assertEquals("senior", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Hank", rs.getString(1)); assertEquals("senior", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("mid", rs.getString(2));
        }
    }

    @Test
    void testCaseInWhere() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT name FROM employees " +
                    "WHERE CASE WHEN dept_id = 1 THEN salary > 90000 ELSE salary > 100000 END " +
                    "ORDER BY name");
            // Eng >90k: Alice(95k). Other >100k: Grace(110k), Hank(105k)
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Grace", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Hank", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // ==================== COALESCE and NULL functions ====================

    @Test
    void testCoalesceInJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT e.name, COALESCE(m.name, 'No Manager') AS manager " +
                    "FROM employees e LEFT JOIN employees m ON e.manager_id = m.id " +
                    "ORDER BY e.name");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("No Manager", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1)); assertEquals("Alice", rs.getString(2));
        }
    }

    @Test
    void testNullIfInSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT NULLIF(dept_id, 1) FROM employees WHERE id = 1");
            assertTrue(rs.next());
            rs.getInt(1);
            assertTrue(rs.wasNull(), "NULLIF(1, 1) should be NULL");
        }
    }

    // ==================== String functions in queries ====================

    @Test
    void testUpperLowerInJoin() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT UPPER(e.name), LOWER(d.name) " +
                    "FROM employees e JOIN departments d ON e.dept_id = d.id " +
                    "WHERE e.id = 1");
            assertTrue(rs.next());
            assertEquals("ALICE", rs.getString(1));
            assertEquals("engineering", rs.getString(2));
        }
    }

    @Test
    void testConcatInSelect() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT e.name || ' (' || d.name || ')' AS display " +
                    "FROM employees e JOIN departments d ON e.dept_id = d.id " +
                    "WHERE e.id = 1");
            assertTrue(rs.next());
            assertEquals("Alice (Engineering)", rs.getString(1));
        }
    }

    // ==================== Nested aggregates with joins ====================

    @Test
    void testAvgOfSubquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Average of per-department max salaries
            ResultSet rs = s.executeQuery(
                    "SELECT AVG(max_sal) FROM " +
                    "(SELECT MAX(salary) AS max_sal FROM employees GROUP BY dept_id) sub");
            assertTrue(rs.next());
            double avg = rs.getDouble(1);
            // Max per dept: Eng=95000, Sales=72000, HR=65000, Research=110000. Avg = 85500
            assertEquals(85500.0, avg, 0.01);
        }
    }

    // ==================== Empty result set handling ====================

    @Test
    void testJoinNoMatches() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT e.name FROM employees e " +
                    "JOIN departments d ON e.dept_id = d.id " +
                    "WHERE d.name = 'NonExistent'");
            assertFalse(rs.next());
        }
    }

    @Test
    void testAggregateOnEmptyResult() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT COUNT(*), SUM(salary), AVG(salary) FROM employees WHERE 1 = 0");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            rs.getDouble(2); assertTrue(rs.wasNull());
            rs.getDouble(3); assertTrue(rs.wasNull());
        }
    }

    // ==================== View + Join combinations ====================

    @Test
    void testJoinOnView() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE VIEW active_projects AS SELECT id, name, dept_id FROM projects WHERE deadline > '2025-06-01'");

            ResultSet rs = s.executeQuery(
                    "SELECT d.name AS dept, ap.name AS project " +
                    "FROM active_projects ap JOIN departments d ON ap.dept_id = d.id " +
                    "ORDER BY ap.name");
            int count = 0;
            while (rs.next()) count++;
            assertTrue(count > 0, "Should have active projects after June 2025");

            s.execute("DROP VIEW active_projects");
        }
    }

    @Test
    void testMaterializedViewJoinedWithTable() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE MATERIALIZED VIEW mv_salaries AS " +
                    "SELECT id, name, salary FROM employees WHERE salary > 80000");

            ResultSet rs = s.executeQuery(
                    "SELECT mv.name, d.name AS dept " +
                    "FROM mv_salaries mv " +
                    "JOIN employees e ON mv.id = e.id " +
                    "JOIN departments d ON e.dept_id = d.id " +
                    "ORDER BY mv.salary DESC");
            assertTrue(rs.next()); assertEquals("Grace", rs.getString(1)); assertEquals("Research", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Hank", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));

            s.execute("DROP VIEW mv_salaries");
        }
    }

    // ==================== Multiple statements and data consistency ====================

    @Test
    void testTransactionWithView() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE VIEW tx_view AS SELECT COUNT(*) AS cnt FROM employees");

            ResultSet rs = s.executeQuery("SELECT cnt FROM tx_view");
            assertTrue(rs.next());
            int count = rs.getInt(1);
            assertTrue(count > 0);

            conn.rollback();

            // After rollback, the view should not exist
            assertThrows(SQLException.class, () -> s.executeQuery("SELECT * FROM tx_view"));
        } finally {
            conn.setAutoCommit(true);
        }
    }
}
