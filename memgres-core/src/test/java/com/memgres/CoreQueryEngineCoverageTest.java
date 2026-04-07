package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 1-10 (Core Query Engine).
 *
 * 1. SELECT fundamentals
 * 2. FROM clause & table references
 * 3. WHERE clause
 * 4. JOINs
 * 5. GROUP BY & HAVING
 * 6. ORDER BY
 * 7. LIMIT, OFFSET & FETCH
 * 8. Set operations
 * 9. CTEs (WITH clause)
 * 10. Subquery expressions
 */
class CoreQueryEngineCoverageTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());

        try (Statement stmt = conn.createStatement()) {
            // Common tables used across tests
            stmt.execute("CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT, department TEXT, salary INTEGER, manager_id INTEGER)");
            stmt.execute("INSERT INTO employees (name, department, salary, manager_id) VALUES ('Alice', 'Engineering', 90000, NULL)");
            stmt.execute("INSERT INTO employees (name, department, salary, manager_id) VALUES ('Bob', 'Engineering', 85000, 1)");
            stmt.execute("INSERT INTO employees (name, department, salary, manager_id) VALUES ('Charlie', 'Sales', 70000, 1)");
            stmt.execute("INSERT INTO employees (name, department, salary, manager_id) VALUES ('Diana', 'Sales', 75000, 3)");
            stmt.execute("INSERT INTO employees (name, department, salary, manager_id) VALUES ('Eve', 'HR', 65000, 1)");
            stmt.execute("INSERT INTO employees (name, department, salary, manager_id) VALUES ('Frank', 'HR', 60000, 5)");

            stmt.execute("CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT, budget INTEGER)");
            stmt.execute("INSERT INTO departments (name, budget) VALUES ('Engineering', 500000)");
            stmt.execute("INSERT INTO departments (name, budget) VALUES ('Sales', 300000)");
            stmt.execute("INSERT INTO departments (name, budget) VALUES ('HR', 200000)");
            stmt.execute("INSERT INTO departments (name, budget) VALUES ('Marketing', 150000)");

            stmt.execute("CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, category TEXT, price NUMERIC(10,2), in_stock BOOLEAN)");
            stmt.execute("INSERT INTO products (name, category, price, in_stock) VALUES ('Widget', 'A', 10.50, true)");
            stmt.execute("INSERT INTO products (name, category, price, in_stock) VALUES ('Gadget', 'A', 25.00, true)");
            stmt.execute("INSERT INTO products (name, category, price, in_stock) VALUES ('Doohickey', 'B', 5.75, false)");
            stmt.execute("INSERT INTO products (name, category, price, in_stock) VALUES ('Thingamajig', 'B', 15.00, true)");
            stmt.execute("INSERT INTO products (name, category, price, in_stock) VALUES ('Whatchamacallit', 'C', 50.00, false)");

            stmt.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, employee_id INTEGER, product_id INTEGER, qty INTEGER, order_date DATE)");
            stmt.execute("INSERT INTO orders (employee_id, product_id, qty, order_date) VALUES (1, 1, 5, '2024-01-15')");
            stmt.execute("INSERT INTO orders (employee_id, product_id, qty, order_date) VALUES (1, 2, 3, '2024-01-20')");
            stmt.execute("INSERT INTO orders (employee_id, product_id, qty, order_date) VALUES (2, 1, 10, '2024-02-01')");
            stmt.execute("INSERT INTO orders (employee_id, product_id, qty, order_date) VALUES (3, 3, 2, '2024-02-15')");
            stmt.execute("INSERT INTO orders (employee_id, product_id, qty, order_date) VALUES (4, 4, 7, '2024-03-01')");
            stmt.execute("INSERT INTO orders (employee_id, product_id, qty, order_date) VALUES (4, 5, 1, '2024-03-10')");

            // Table with NULLs for NULL-handling tests
            stmt.execute("CREATE TABLE nullable_data (id INTEGER, val TEXT, num INTEGER)");
            stmt.execute("INSERT INTO nullable_data VALUES (1, 'a', 10)");
            stmt.execute("INSERT INTO nullable_data VALUES (2, NULL, 20)");
            stmt.execute("INSERT INTO nullable_data VALUES (3, 'c', NULL)");
            stmt.execute("INSERT INTO nullable_data VALUES (4, NULL, NULL)");
            stmt.execute("INSERT INTO nullable_data VALUES (5, 'a', 30)");

            // Hierarchy table for recursive CTEs
            stmt.execute("CREATE TABLE tree (id INTEGER, parent_id INTEGER, name TEXT)");
            stmt.execute("INSERT INTO tree VALUES (1, NULL, 'root')");
            stmt.execute("INSERT INTO tree VALUES (2, 1, 'child1')");
            stmt.execute("INSERT INTO tree VALUES (3, 1, 'child2')");
            stmt.execute("INSERT INTO tree VALUES (4, 2, 'grandchild1')");
            stmt.execute("INSERT INTO tree VALUES (5, 2, 'grandchild2')");
            stmt.execute("INSERT INTO tree VALUES (6, 3, 'grandchild3')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 1. SELECT fundamentals
    // =========================================================================

    @Test
    void select_star() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM departments ORDER BY id LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Engineering", rs.getString("name"));
        }
    }

    @Test
    void select_expressions_no_from() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1 + 2, 'hello' || ' world', TRUE")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals("hello world", rs.getString(2));
            assertTrue(rs.getBoolean(3));
        }
    }

    @Test
    void select_column_aliases() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT 1 AS num, 'text' AS str, 2 + 3 AS calc")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("num"));
            assertEquals("text", rs.getString("str"));
            assertEquals(5, rs.getInt("calc"));
        }
    }

    @Test
    void select_distinct() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT DISTINCT department FROM employees ORDER BY department")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void select_distinct_on() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT DISTINCT ON (department) department, name, salary " +
                "FROM employees ORDER BY department, salary DESC")) {
            // Should get highest salary per department
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals("Alice", rs.getString(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals("Eve", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals("Diana", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void select_scalar_subquery_in_select_list() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.employee_id = employees.id) AS order_count " +
                "FROM employees WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertEquals(2, rs.getInt(2));
        }
    }

    // =========================================================================
    // 2. FROM clause & table references
    // =========================================================================

    @Test
    void from_table_alias() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT e.name FROM employees e WHERE e.id = 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
        }
    }

    @Test
    void from_multi_table_cross_join() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT e.name, d.name FROM employees e, departments d " +
                "WHERE e.department = d.name AND e.id = 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertEquals("Engineering", rs.getString(2));
        }
    }

    @Test
    void from_subquery() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT sub.dept, sub.cnt FROM " +
                "(SELECT department AS dept, COUNT(*) AS cnt FROM employees GROUP BY department) sub " +
                "ORDER BY sub.cnt DESC")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void from_lateral_subquery() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT d.name, emp.top_salary " +
                "FROM departments d, " +
                "LATERAL (SELECT MAX(salary) AS top_salary FROM employees WHERE department = d.name) emp " +
                "ORDER BY d.name")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(90000, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals(65000, rs.getInt(2));
            // Marketing has no employees - should still appear with NULL
            assertTrue(rs.next()); assertEquals("Marketing", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(75000, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void from_function_call_generate_series() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM generate_series(1, 5)")) {
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());
        }
    }

    @Test
    void from_values_list() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, letter) ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("a", rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("b", rs.getString(2));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals("c", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void from_schema_qualified() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT name FROM public.employees WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
        }
    }

    // =========================================================================
    // 3. WHERE clause
    // =========================================================================

    @Test
    void where_comparison_operators() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // < > <= >= = <>
            assertRowCount(s, "SELECT id FROM employees WHERE salary > 80000", 2);   // Alice, Bob
            assertRowCount(s, "SELECT id FROM employees WHERE salary >= 85000", 2);  // Alice, Bob
            assertRowCount(s, "SELECT id FROM employees WHERE salary < 65000", 1);   // Frank
            assertRowCount(s, "SELECT id FROM employees WHERE salary <= 65000", 2);  // Eve, Frank
            assertRowCount(s, "SELECT id FROM employees WHERE salary = 70000", 1);   // Charlie
            assertRowCount(s, "SELECT id FROM employees WHERE salary <> 70000", 5);
        }
    }

    @Test
    void where_and_or_not() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT id FROM employees WHERE department = 'Engineering' AND salary > 85000", 1);
            assertRowCount(s, "SELECT id FROM employees WHERE department = 'Engineering' OR department = 'Sales'", 4);
            assertRowCount(s, "SELECT id FROM employees WHERE NOT department = 'Engineering'", 4);
        }
    }

    @Test
    void where_is_null_is_not_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT id FROM nullable_data WHERE val IS NULL", 2);
            assertRowCount(s, "SELECT id FROM nullable_data WHERE val IS NOT NULL", 3);
            assertRowCount(s, "SELECT id FROM nullable_data WHERE num IS NULL", 2);
        }
    }

    @Test
    void where_is_distinct_from() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // IS DISTINCT FROM treats NULL as a comparable value
            // NULL IS DISTINCT FROM NULL → false
            // NULL IS DISTINCT FROM 'a' → true
            // 'a' IS DISTINCT FROM 'a' → false
            assertRowCount(s, "SELECT id FROM nullable_data WHERE val IS DISTINCT FROM 'a'", 3); // ids 2,3,4
            assertRowCount(s, "SELECT id FROM nullable_data WHERE val IS NOT DISTINCT FROM NULL", 2); // ids 2,4
        }
    }

    @Test
    void where_between() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT id FROM employees WHERE salary BETWEEN 70000 AND 85000", 3);
            assertRowCount(s, "SELECT id FROM employees WHERE salary NOT BETWEEN 70000 AND 85000", 3);
        }
    }

    @Test
    void where_in_list() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT id FROM employees WHERE department IN ('Engineering', 'HR')", 4);
            assertRowCount(s, "SELECT id FROM employees WHERE department NOT IN ('Engineering', 'HR')", 2);
        }
    }

    @Test
    void where_in_subquery() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT id FROM employees WHERE id IN (SELECT employee_id FROM orders)", 4);
            assertRowCount(s, "SELECT id FROM employees WHERE id NOT IN (SELECT employee_id FROM orders)", 2);
        }
    }

    @Test
    void where_like_ilike() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT id FROM employees WHERE name LIKE 'A%'", 1);       // Alice
            assertRowCount(s, "SELECT id FROM employees WHERE name LIKE '%a%'", 3);       // Charlie, Diana, Frank
            assertRowCount(s, "SELECT id FROM employees WHERE name LIKE '_o_'", 1);       // Bob
            assertRowCount(s, "SELECT id FROM employees WHERE name NOT LIKE 'A%'", 5);
            assertRowCount(s, "SELECT id FROM employees WHERE name ILIKE 'a%'", 1);       // Alice (case-insensitive)
            assertRowCount(s, "SELECT id FROM employees WHERE name ILIKE 'alice'", 1);
        }
    }

    @Test
    void where_parenthesized_complex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // (Engineering AND salary > 85000) OR (Sales AND salary > 72000)
            assertRowCount(s,
                "SELECT id FROM employees WHERE " +
                "(department = 'Engineering' AND salary > 85000) OR " +
                "(department = 'Sales' AND salary > 72000)", 2); // Alice, Diana
        }
    }

    // =========================================================================
    // 4. JOINs
    // =========================================================================

    @Test
    void join_inner() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT e.name FROM employees e INNER JOIN departments d ON e.department = d.name", 6);
        }
    }

    @Test
    void join_left() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT d.name, e.name FROM departments d LEFT JOIN employees e ON d.name = e.department " +
                "WHERE d.name = 'Marketing'")) {
            assertTrue(rs.next());
            assertEquals("Marketing", rs.getString(1));
            assertNull(rs.getString(2)); // No employees in Marketing
        }
    }

    @Test
    void join_right() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT d.name, COUNT(e.id) FROM departments d " +
                "RIGHT JOIN employees e ON d.name = e.department " +
                "GROUP BY d.name ORDER BY d.name")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(2, rs.getInt(2));
        }
    }

    @Test
    void join_full_outer() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Full outer join should include Marketing (no employees) and all employees
            int count = countRows(s,
                "SELECT d.name, e.name FROM departments d FULL OUTER JOIN employees e ON d.name = e.department");
            assertTrue(count >= 7); // 6 employees + Marketing with null
        }
    }

    @Test
    void join_cross() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT e.name, d.name FROM employees e CROSS JOIN departments d", 24); // 6 * 4
        }
    }

    @Test
    void join_natural() throws SQLException {
        // NATURAL JOIN on departments (matching 'name' column won't work well, let's use a different example)
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE emp_dept (dept_name TEXT, emp_count INTEGER)");
            s.execute("INSERT INTO emp_dept VALUES ('Engineering', 2)");
            s.execute("INSERT INTO emp_dept VALUES ('Sales', 2)");

            s.execute("CREATE TABLE dept_budget (dept_name TEXT, budget INTEGER)");
            s.execute("INSERT INTO dept_budget VALUES ('Engineering', 500000)");
            s.execute("INSERT INTO dept_budget VALUES ('Sales', 300000)");

            ResultSet rs = s.executeQuery("SELECT * FROM emp_dept NATURAL JOIN dept_budget ORDER BY dept_name");
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString("dept_name"));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString("dept_name"));
            assertFalse(rs.next());
            rs.close();

            s.execute("DROP TABLE emp_dept");
            s.execute("DROP TABLE dept_budget");
        }
    }

    @Test
    void join_using() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE t_a (key INTEGER, val_a TEXT)");
            s.execute("CREATE TABLE t_b (key INTEGER, val_b TEXT)");
            s.execute("INSERT INTO t_a VALUES (1, 'a1'), (2, 'a2')");
            s.execute("INSERT INTO t_b VALUES (1, 'b1'), (3, 'b3')");

            ResultSet rs = s.executeQuery("SELECT key, val_a, val_b FROM t_a JOIN t_b USING (key)");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("key"));
            assertEquals("a1", rs.getString("val_a"));
            assertEquals("b1", rs.getString("val_b"));
            assertFalse(rs.next());
            rs.close();

            s.execute("DROP TABLE t_a");
            s.execute("DROP TABLE t_b");
        }
    }

    @Test
    void join_self() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT e.name AS employee, m.name AS manager " +
                "FROM employees e LEFT JOIN employees m ON e.manager_id = m.id " +
                "WHERE e.id = 2")) {
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("employee"));
            assertEquals("Alice", rs.getString("manager"));
        }
    }

    @Test
    void join_multiple_tables() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT e.name, p.name, o.qty " +
                "FROM orders o " +
                "JOIN employees e ON o.employee_id = e.id " +
                "JOIN products p ON o.product_id = p.id " +
                "WHERE e.name = 'Alice' ORDER BY o.id")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Widget", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); assertEquals("Gadget", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // 5. GROUP BY & HAVING
    // =========================================================================

    @Test
    void group_by_single_column() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT department, COUNT(*) as cnt FROM employees GROUP BY department ORDER BY department")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void group_by_multiple_columns() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT department, in_stock, COUNT(*) FROM " +
                "(SELECT e.department, p.in_stock FROM orders o " +
                " JOIN employees e ON o.employee_id = e.id " +
                " JOIN products p ON o.product_id = p.id) sub " +
                "GROUP BY department, in_stock ORDER BY department, in_stock")) {
            assertTrue(rs.next()); // Should have results
        }
    }

    @Test
    void group_by_expression() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT salary / 10000 * 10000 AS salary_band, COUNT(*) " +
                "FROM employees GROUP BY salary / 10000 * 10000 ORDER BY salary_band")) {
            assertTrue(rs.next()); assertEquals(60000, rs.getInt(1)); // 60k, 65k → 60000 band
            assertTrue(rs.next()); assertEquals(70000, rs.getInt(1)); // 70k, 75k → 70000 band
            assertTrue(rs.next()); assertEquals(80000, rs.getInt(1)); // 85k → 80000 band
            assertTrue(rs.next()); assertEquals(90000, rs.getInt(1)); // 90k → 90000 band
        }
    }

    @Test
    void group_by_having() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT department, AVG(salary) AS avg_sal FROM employees " +
                "GROUP BY department HAVING AVG(salary) > 70000 ORDER BY department")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void group_by_with_aggregate_functions() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT department, " +
                "  COUNT(*) AS cnt, " +
                "  SUM(salary) AS total, " +
                "  MIN(salary) AS min_sal, " +
                "  MAX(salary) AS max_sal, " +
                "  AVG(salary) AS avg_sal " +
                "FROM employees GROUP BY department ORDER BY department")) {
            assertTrue(rs.next());
            assertEquals("Engineering", rs.getString(1));
            assertEquals(2, rs.getInt("cnt"));
            assertEquals(175000, rs.getInt("total"));
            assertEquals(85000, rs.getInt("min_sal"));
            assertEquals(90000, rs.getInt("max_sal"));
        }
    }

    @Test
    void group_by_alias() throws SQLException {
        // PG allows GROUP BY alias
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT department AS dept, COUNT(*) FROM employees GROUP BY dept ORDER BY dept")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
        }
    }

    @Test
    void group_by_ordinal() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT department, COUNT(*) FROM employees GROUP BY 1 ORDER BY 1")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
        }
    }

    @Test
    void aggregate_without_group_by() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(*), SUM(salary), AVG(salary) FROM employees")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt(1));
            assertEquals(445000, rs.getInt(2));
        }
    }

    @Test
    void aggregate_count_distinct() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT COUNT(DISTINCT department) FROM employees")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void aggregate_string_agg() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT STRING_AGG(name, ', ' ORDER BY name) FROM employees WHERE department = 'Engineering'")) {
            assertTrue(rs.next());
            assertEquals("Alice, Bob", rs.getString(1));
        }
    }

    @Test
    void aggregate_array_agg() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT ARRAY_AGG(name ORDER BY name) FROM employees WHERE department = 'HR'")) {
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("Eve") && result.contains("Frank"));
        }
    }

    @Test
    void aggregate_bool_and_or() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT BOOL_AND(in_stock), BOOL_OR(in_stock) FROM products");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1)); // Not all in stock
            assertTrue(rs.getBoolean(2));  // Some in stock
            rs.close();
        }
    }

    @Test
    void aggregate_filter_clause() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT COUNT(*) AS total, " +
                "COUNT(*) FILTER (WHERE salary > 80000) AS high_earners " +
                "FROM employees")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt("total"));
            assertEquals(2, rs.getInt("high_earners"));
        }
    }

    // =========================================================================
    // 6. ORDER BY
    // =========================================================================

    @Test
    void order_by_asc_desc() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT name FROM employees ORDER BY salary ASC LIMIT 1");
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1)); rs.close();

            rs = s.executeQuery("SELECT name FROM employees ORDER BY salary DESC LIMIT 1");
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); rs.close();
        }
    }

    @Test
    void order_by_multiple_columns_mixed() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT department, name FROM employees ORDER BY department ASC, salary DESC")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals("Alice", rs.getString(2));
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals("Bob", rs.getString(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals("Eve", rs.getString(2));
        }
    }

    @Test
    void order_by_ordinal() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT name, salary FROM employees ORDER BY 2 DESC LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
        }
    }

    @Test
    void order_by_alias() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT name, salary AS pay FROM employees ORDER BY pay DESC LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
        }
    }

    @Test
    void order_by_expression() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT name FROM employees ORDER BY length(name) ASC LIMIT 1")) {
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString(1)); // 3 chars
        }
    }

    @Test
    void order_by_nulls_first_last() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT id, val FROM nullable_data ORDER BY val ASC NULLS FIRST");
            assertTrue(rs.next()); assertNull(rs.getString("val"));
            rs.close();

            rs = s.executeQuery("SELECT id, val FROM nullable_data ORDER BY val ASC NULLS LAST");
            // First non-null value
            assertTrue(rs.next()); assertNotNull(rs.getString("val"));
            rs.close();
        }
    }

    @Test
    void order_by_desc_nulls_first() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, num FROM nullable_data ORDER BY num DESC NULLS FIRST")) {
            // NULLS FIRST with DESC: NULLs come first
            assertTrue(rs.next()); assertNull(rs.getObject("num"));
            assertTrue(rs.next()); assertNull(rs.getObject("num"));
            assertTrue(rs.next()); assertEquals(30, rs.getInt("num"));
        }
    }

    // =========================================================================
    // 7. LIMIT, OFFSET & FETCH
    // =========================================================================

    @Test
    void limit_basic() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT * FROM employees ORDER BY id LIMIT 3", 3);
        }
    }

    @Test
    void offset_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM employees ORDER BY id OFFSET 4")) {
            assertTrue(rs.next()); assertEquals(5, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(6, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void limit_offset_combined() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM employees ORDER BY id LIMIT 2 OFFSET 2")) {
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void limit_all() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT * FROM employees LIMIT ALL", 6);
        }
    }

    @Test
    void fetch_first_rows() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM employees ORDER BY id FETCH FIRST 3 ROWS ONLY")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void fetch_next_row() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM employees ORDER BY id OFFSET 2 FETCH NEXT 1 ROW ONLY")) {
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void limit_zero() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT * FROM employees LIMIT 0", 0);
        }
    }

    @Test
    void offset_beyond_rows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s, "SELECT * FROM employees OFFSET 100", 0);
        }
    }

    // =========================================================================
    // 8. Set operations
    // =========================================================================

    @Test
    void union_removes_duplicates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT department FROM employees WHERE department = 'Engineering' " +
                "UNION " +
                "SELECT department FROM employees WHERE department = 'Engineering'", 1);
        }
    }

    @Test
    void union_all_keeps_duplicates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT department FROM employees WHERE department = 'Engineering' " +
                "UNION ALL " +
                "SELECT department FROM employees WHERE department = 'Engineering'", 4);
        }
    }

    @Test
    void intersect() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT department FROM employees WHERE salary > 70000 " +
                "INTERSECT " +
                "SELECT department FROM employees WHERE salary < 80000 " +
                "ORDER BY department")) {
            // Departments with both >70k and <80k employees: Sales (75k, 70k), HR (65k - but 65k is not >70k)
            // Actually: >70k = Engineering(90k,85k), Sales(75k), HR(no). <80k = Sales(70k,75k), HR(65k,60k)
            // Intersection of departments: Sales
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void intersect_all() throws SQLException {
        try (Statement s = conn.createStatement()) {
            int count = countRows(s,
                "SELECT department FROM employees " +
                "INTERSECT ALL " +
                "SELECT department FROM employees");
            assertEquals(6, count); // All rows match themselves
        }
    }

    @Test
    void except() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT DISTINCT department FROM employees " +
                "EXCEPT " +
                "SELECT name FROM departments WHERE name = 'Engineering' " +
                "ORDER BY department")) {
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void except_all() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Two Engineering employees EXCEPT ALL one 'Engineering' = one 'Engineering' remaining
            int count = countRows(s,
                "SELECT department FROM employees " +
                "EXCEPT ALL " +
                "SELECT department FROM employees WHERE id <= 3");
            assertEquals(3, count); // ids 4,5,6 remain
        }
    }

    @Test
    void nested_set_operations() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "(SELECT name FROM employees WHERE department = 'Engineering' " +
                " UNION " +
                " SELECT name FROM employees WHERE department = 'Sales') " +
                "EXCEPT " +
                "SELECT name FROM employees WHERE salary < 75000 " +
                "ORDER BY name")) {
            // Engineering + Sales = Alice, Bob, Charlie, Diana
            // Minus salary < 75k (Charlie=70k) = Alice, Bob, Diana
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Diana", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void union_with_order_by_and_limit() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name FROM employees WHERE department = 'Engineering' " +
                "UNION " +
                "SELECT name FROM employees WHERE department = 'Sales' " +
                "ORDER BY name LIMIT 2")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // 9. CTEs (WITH clause)
    // =========================================================================

    @Test
    void cte_basic() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "WITH eng AS (SELECT * FROM employees WHERE department = 'Engineering') " +
                "SELECT name FROM eng ORDER BY name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void cte_multiple() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "WITH eng AS (SELECT * FROM employees WHERE department = 'Engineering'), " +
                "     high_sal AS (SELECT * FROM eng WHERE salary > 85000) " +
                "SELECT name FROM high_sal")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void cte_referencing_another_cte() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "WITH dept_counts AS (" +
                "  SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department" +
                "), " +
                "big_depts AS (" +
                "  SELECT department FROM dept_counts WHERE cnt >= 2" +
                ") " +
                "SELECT department FROM big_depts ORDER BY department")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void cte_recursive_tree() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "WITH RECURSIVE descendants AS (" +
                "  SELECT id, name, 0 AS depth FROM tree WHERE id = 1 " +
                "  UNION ALL " +
                "  SELECT t.id, t.name, d.depth + 1 FROM tree t JOIN descendants d ON t.parent_id = d.id" +
                ") " +
                "SELECT name, depth FROM descendants ORDER BY depth, name")) {
            assertTrue(rs.next()); assertEquals("root", rs.getString(1)); assertEquals(0, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("child1", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("child2", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("grandchild1", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("grandchild2", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("grandchild3", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void cte_recursive_generate_series_like() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "WITH RECURSIVE nums AS (" +
                "  SELECT 1 AS n " +
                "  UNION ALL " +
                "  SELECT n + 1 FROM nums WHERE n < 5" +
                ") " +
                "SELECT n FROM nums ORDER BY n")) {
            for (int i = 1; i <= 5; i++) {
                assertTrue(rs.next());
                assertEquals(i, rs.getInt(1));
            }
            assertFalse(rs.next());
        }
    }

    @Test
    void cte_in_insert() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cte_target (name TEXT, salary INTEGER)");
            s.execute(
                "WITH high_earners AS (SELECT name, salary FROM employees WHERE salary > 80000) " +
                "INSERT INTO cte_target SELECT * FROM high_earners");

            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM cte_target");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1)); // Alice=90k, Bob=85k
            rs.close();

            s.execute("DROP TABLE cte_target");
        }
    }

    @Test
    void cte_in_update() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cte_update_test (id INTEGER, val TEXT)");
            s.execute("INSERT INTO cte_update_test VALUES (1, 'old'), (2, 'old'), (3, 'old')");

            s.execute(
                "WITH to_update AS (SELECT id FROM cte_update_test WHERE id <= 2) " +
                "UPDATE cte_update_test SET val = 'new' WHERE id IN (SELECT id FROM to_update)");

            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM cte_update_test WHERE val = 'new'");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            rs.close();

            s.execute("DROP TABLE cte_update_test");
        }
    }

    @Test
    void cte_in_delete() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cte_del_test (id INTEGER, val TEXT)");
            s.execute("INSERT INTO cte_del_test VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            s.execute(
                "WITH to_delete AS (SELECT id FROM cte_del_test WHERE id > 1) " +
                "DELETE FROM cte_del_test WHERE id IN (SELECT id FROM to_delete)");

            ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM cte_del_test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            rs.close();

            s.execute("DROP TABLE cte_del_test");
        }
    }

    // =========================================================================
    // 10. Subquery expressions
    // =========================================================================

    @Test
    void subquery_exists() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT name FROM employees e WHERE EXISTS " +
                "(SELECT 1 FROM orders o WHERE o.employee_id = e.id)", 4);
        }
    }

    @Test
    void subquery_not_exists() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT name FROM employees e WHERE NOT EXISTS " +
                "(SELECT 1 FROM orders o WHERE o.employee_id = e.id)", 2); // Eve, Frank
        }
    }

    @Test
    void subquery_in() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT name FROM employees WHERE department IN " +
                "(SELECT name FROM departments WHERE budget > 200000)", 4); // Engineering(500k), Sales(300k)
        }
    }

    @Test
    void subquery_not_in() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT name FROM employees WHERE department NOT IN " +
                "(SELECT name FROM departments WHERE budget > 200000)", 2); // HR
        }
    }

    @Test
    void subquery_any() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT name FROM employees WHERE salary > ANY " +
                "(SELECT salary FROM employees WHERE department = 'Sales')", 3); // >70k or >75k: Alice, Bob, Diana(>70k)
        }
    }

    @Test
    void subquery_all() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRowCount(s,
                "SELECT name FROM employees WHERE salary > ALL " +
                "(SELECT salary FROM employees WHERE department = 'Sales')", 2); // >75k: Alice(90), Bob(85)
        }
    }

    @Test
    void subquery_scalar() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name FROM employees WHERE salary = " +
                "(SELECT MAX(salary) FROM employees)")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void subquery_correlated() throws SQLException {
        // Find employees who earn more than the average in their department
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT name FROM employees e WHERE salary > " +
                "(SELECT AVG(salary) FROM employees WHERE department = e.department) " +
                "ORDER BY name")) {
            assertTrue(rs.next()); assertEquals("Alice", rs.getString(1)); // 90k > avg(87.5k)
            assertTrue(rs.next()); assertEquals("Diana", rs.getString(1)); // 75k > avg(72.5k)
            assertTrue(rs.next()); assertEquals("Eve", rs.getString(1));   // 65k > avg(62.5k)
            assertFalse(rs.next());
        }
    }

    @Test
    void subquery_in_from_clause() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT dept, total_sal FROM " +
                "(SELECT department AS dept, SUM(salary) AS total_sal FROM employees GROUP BY department) sub " +
                "WHERE total_sal > 150000 ORDER BY dept")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void subquery_in_select_list_correlated() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT d.name, " +
                "  (SELECT COUNT(*) FROM employees e WHERE e.department = d.name) AS emp_count " +
                "FROM departments d ORDER BY d.name")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("HR", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Marketing", rs.getString(1)); assertEquals(0, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void subquery_exists_with_correlation() throws SQLException {
        // Departments that have at least one employee with salary > 80000
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                "SELECT d.name FROM departments d WHERE EXISTS " +
                "(SELECT 1 FROM employees e WHERE e.department = d.name AND e.salary > 80000) " +
                "ORDER BY d.name")) {
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void assertRowCount(Statement s, String sql, int expected) throws SQLException {
        assertEquals(expected, countRows(s, sql), "Row count mismatch for: " + sql);
    }

    private int countRows(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) count++;
            return count;
        }
    }
}
