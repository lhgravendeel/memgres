package com.memgres;

import com.memgres.core.Memgres;
import com.memgres.engine.util.IO;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 25 complex, diverse SQL scenarios exercising combinations of features:
 * subqueries, joins, CTEs, window functions, triggers, generated columns,
 * CASE expressions, JSON aggregation, correlated subqueries, recursive CTEs,
 * HAVING with expressions, LATERAL joins, MERGE, DO blocks, and more.
 *
 * Each test creates its own schema via the shared setup SQL, runs a complex
 * query, and validates expected results.
 */
class ComplexQueryScenariosTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        // Load and execute the schema setup SQL
        try (InputStream is = ComplexQueryScenariosTest.class.getResourceAsStream("/complex-query-scenarios.sql");
             Statement stmt = conn.createStatement()) {
            String sql = new String(IO.readAllBytes(is), StandardCharsets.UTF_8);
            // Split on semicolons (respecting dollar-quoting)
            for (String s : splitStatements(sql)) {
                String trimmed = s.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("--")) continue;
                stmt.execute(trimmed);
            }
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP SCHEMA IF EXISTS cqs CASCADE");
            } catch (Exception ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // Q1: Correlated subquery, employees earning above their department average
    // =========================================================================
    @Test
    void q01_correlated_subquery_above_dept_avg() throws SQLException {
        ResultSet rs = query("""
            SELECT e.name, e.salary, d.name AS dept
            FROM cqs.employees e
            JOIN cqs.departments d ON d.id = e.department_id
            WHERE e.salary > (
                SELECT AVG(e2.salary) FROM cqs.employees e2
                WHERE e2.department_id = e.department_id AND e2.is_active
            )
            ORDER BY e.salary DESC
            """);
        // Backend avg = (120000+110000+115000)/3 = 115000 → Alice(120000)
        // Frontend avg = (105000+98000)/2 = 101500 → Charlie(105000)
        // Marketing avg = (88000+92000)/2 = 90000 → Ivy(92000) but inactive so not in avg?
        // Actually is_active filter is only in subquery; outer has no filter
        // Backend: avg of active = 115000 → Alice 120000 > 115000
        // Frontend: avg of active = 101500 → Charlie 105000 > 101500
        // Enterprise: only Eve, avg=130000, Eve !> 130000
        // Sales: only Diana, avg=95000, Diana !> 95000
        // Marketing: avg of active(Grace=88000) = 88000, Ivy 92000 > 88000 (Ivy in outer, no filter)
        // Engineering: only Jack, avg=150000, !> 150000
        assertTrue(rs.next());
        assertEquals("Alice", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals("Charlie", rs.getString("name"));
        assertTrue(rs.next());
        assertEquals("Ivy", rs.getString("name"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q2: Window functions, ranking employees by salary within department
    // =========================================================================
    @Test
    void q02_window_rank_per_department() throws SQLException {
        ResultSet rs = query("""
            SELECT name, salary, dept_name,
                   RANK() OVER (PARTITION BY dept_name ORDER BY salary DESC) AS rnk
            FROM (
                SELECT e.name, e.salary, d.name AS dept_name
                FROM cqs.employees e
                JOIN cqs.departments d ON d.id = e.department_id
                WHERE e.is_active
            ) sub
            WHERE dept_name = 'Backend'
            ORDER BY salary DESC
            """);
        assertTrue(rs.next()); assertEquals("Alice",  rs.getString("name")); assertEquals(1, rs.getInt("rnk"));
        assertTrue(rs.next()); assertEquals("Hank",   rs.getString("name")); assertEquals(2, rs.getInt("rnk"));
        assertTrue(rs.next()); assertEquals("Bob",    rs.getString("name")); assertEquals(3, rs.getInt("rnk"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q3: Recursive CTE for full category tree with depth
    // =========================================================================
    @Test
    void q03_recursive_cte_category_tree() throws SQLException {
        ResultSet rs = query("""
            WITH RECURSIVE cat_tree AS (
                SELECT id, name, parent_id, 0 AS depth
                FROM cqs.categories WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, c.parent_id, ct.depth + 1
                FROM cqs.categories c
                JOIN cat_tree ct ON ct.id = c.parent_id
            )
            SELECT name, depth FROM cat_tree ORDER BY depth, name
            """);
        // depth 0: Clothing, Electronics
        assertTrue(rs.next()); assertEquals("Clothing",    rs.getString(1)); assertEquals(0, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Electronics", rs.getString(1)); assertEquals(0, rs.getInt(2));
        // depth 1: Computers, Mens, Phones, Womens
        assertTrue(rs.next()); assertEquals("Computers",   rs.getString(1)); assertEquals(1, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Mens",        rs.getString(1)); assertEquals(1, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Phones",      rs.getString(1)); assertEquals(1, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Womens",      rs.getString(1)); assertEquals(1, rs.getInt(2));
        // depth 2: Desktops, Laptops
        assertTrue(rs.next()); assertEquals("Desktops",    rs.getString(1)); assertEquals(2, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Laptops",     rs.getString(1)); assertEquals(2, rs.getInt(2));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q4: CASE + aggregation, salary bands per department
    // =========================================================================
    @Test
    void q04_case_aggregation_salary_bands() throws SQLException {
        ResultSet rs = query("""
            SELECT d.name AS dept,
                   COUNT(*) FILTER (WHERE e.salary < 100000)  AS under_100k,
                   COUNT(*) FILTER (WHERE e.salary >= 100000 AND e.salary < 130000) AS mid_range,
                   COUNT(*) FILTER (WHERE e.salary >= 130000) AS senior
            FROM cqs.employees e
            JOIN cqs.departments d ON d.id = e.department_id
            WHERE e.is_active
            GROUP BY d.name
            ORDER BY d.name
            """);
        // Backend: Alice 120k(mid), Bob 110k(mid), Hank 115k(mid) → 0,3,0
        assertTrue(rs.next()); assertEquals("Backend", rs.getString(1));
        assertEquals(0, rs.getInt("under_100k"));
        assertEquals(3, rs.getInt("mid_range"));
        assertEquals(0, rs.getInt("senior"));
        // Engineering: Jack 150k → 0,0,1
        assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
        assertEquals(0, rs.getInt("under_100k"));
        assertEquals(0, rs.getInt("mid_range"));
        assertEquals(1, rs.getInt("senior"));
        // Enterprise: Eve 130k → 0,0,1
        assertTrue(rs.next()); assertEquals("Enterprise", rs.getString(1));
        assertEquals(0, rs.getInt("under_100k"));
        assertEquals(0, rs.getInt("mid_range"));
        assertEquals(1, rs.getInt("senior"));
        // Frontend: Charlie 105k(mid), Frank 98k(under) → 1,1,0
        assertTrue(rs.next()); assertEquals("Frontend", rs.getString(1));
        assertEquals(1, rs.getInt("under_100k"));
        assertEquals(1, rs.getInt("mid_range"));
        assertEquals(0, rs.getInt("senior"));
        // Marketing: Grace 88k → 1,0,0
        assertTrue(rs.next()); assertEquals("Marketing", rs.getString(1));
        assertEquals(1, rs.getInt("under_100k"));
        assertEquals(0, rs.getInt("mid_range"));
        assertEquals(0, rs.getInt("senior"));
        // Sales: Diana 95k → 1,0,0
        assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));
        assertEquals(1, rs.getInt("under_100k"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q5: Generated column + aggregation, order totals by customer
    // =========================================================================
    @Test
    void q05_generated_column_order_totals() throws SQLException {
        ResultSet rs = query("""
            SELECT customer,
                   SUM(total) AS total_spend,
                   COUNT(*) AS order_count
            FROM cqs.orders
            GROUP BY customer
            HAVING SUM(total) > 500
            ORDER BY total_spend DESC
            """);
        // Acme: 10*49.99 + 20*49.99 = 499.90 + 999.80 = 1499.70
        // Globex: 5*199 + 1*999 = 995+999 = 1994.00
        // Umbrella: 8*120 = 960.00
        // Initech: 3*75.5 = 226.50 (excluded)
        assertTrue(rs.next()); assertEquals("Globex", rs.getString(1));
        assertEquals(0, rs.getBigDecimal("total_spend").compareTo(new java.math.BigDecimal("1994.00")));
        assertTrue(rs.next()); assertEquals("Acme Corp", rs.getString(1));
        assertEquals(0, rs.getBigDecimal("total_spend").compareTo(new java.math.BigDecimal("1499.70")));
        assertTrue(rs.next()); assertEquals("Umbrella", rs.getString(1));
        assertEquals(0, rs.getBigDecimal("total_spend").compareTo(new java.math.BigDecimal("960.00")));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q6: Multi-join + subquery, project staffing with lead name
    // =========================================================================
    @Test
    void q06_multi_join_project_leads() throws SQLException {
        ResultSet rs = query("""
            SELECT p.name AS project,
                   d.name AS department,
                   lead.name AS lead_name,
                   (SELECT COUNT(*) FROM cqs.employee_projects ep2
                    WHERE ep2.project_id = p.id) AS team_size,
                   (SELECT SUM(ep3.hours) FROM cqs.employee_projects ep3
                    WHERE ep3.project_id = p.id) AS total_hours
            FROM cqs.projects p
            JOIN cqs.departments d ON d.id = p.dept_id
            JOIN cqs.employee_projects ep ON ep.project_id = p.id AND ep.role = 'lead'
            JOIN cqs.employees lead ON lead.id = ep.employee_id
            ORDER BY p.name
            """);
        assertTrue(rs.next()); assertEquals("API v2", rs.getString("project"));
        assertEquals("Alice", rs.getString("lead_name"));
        assertEquals(3, rs.getInt("team_size"));
        assertEquals(800, rs.getInt("total_hours"));

        assertTrue(rs.next()); assertEquals("Dashboard", rs.getString("project"));
        assertEquals("Charlie", rs.getString("lead_name"));
        assertEquals(2, rs.getInt("team_size"));
        assertEquals(500, rs.getInt("total_hours"));

        assertTrue(rs.next()); assertEquals("Data Lake", rs.getString("project"));
        assertEquals("Jack", rs.getString("lead_name"));
        assertEquals(3, rs.getInt("team_size"));
        assertEquals(460, rs.getInt("total_hours"));

        assertTrue(rs.next()); assertEquals("Rebrand", rs.getString("project"));
        assertEquals("Grace", rs.getString("lead_name"));

        assertTrue(rs.next()); assertEquals("Sales Portal", rs.getString("project"));
        assertEquals("Diana", rs.getString("lead_name"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q7: Trigger fires on salary update; verify audit_log
    // =========================================================================
    @Test
    void q07_trigger_audit_salary_change() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("UPDATE cqs.employees SET salary = 125000 WHERE name = 'Bob'");
            ResultSet rs = stmt.executeQuery(
                "SELECT old_data, new_data FROM cqs.audit_log " +
                "WHERE table_name = 'employees' AND row_id = (SELECT id FROM cqs.employees WHERE name = 'Bob') " +
                "ORDER BY id DESC LIMIT 1");
            assertTrue(rs.next());
            assertEquals("110000.00", rs.getString("old_data"));
            assertEquals("125000.00", rs.getString("new_data"));
            // restore
            stmt.execute("UPDATE cqs.employees SET salary = 110000 WHERE name = 'Bob'");
        }
    }

    // =========================================================================
    // Q8: EXISTS + NOT EXISTS, employees on no project
    // =========================================================================
    @Test
    void q08_not_exists_unassigned_employees() throws SQLException {
        ResultSet rs = query("""
            SELECT e.name
            FROM cqs.employees e
            WHERE NOT EXISTS (
                SELECT 1 FROM cqs.employee_projects ep WHERE ep.employee_id = e.id
            )
            ORDER BY e.name
            """);
        // Everyone except: Diana(proj3), Eve(proj3) are assigned
        // Let's check: emp 1(Alice→p1,p5), 2(Bob→p1,p5), 3(Charlie→p2),
        // 4(Diana→p3), 5(Eve→p3), 6(Frank→p2), 7(Grace→p4), 8(Hank→p1),
        // 9(Ivy->p4), 10(Jack->p5); all assigned, so 0 rows
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q9: LATERAL join, top project per employee by hours
    // =========================================================================
    @Test
    void q09_lateral_join_top_project() throws SQLException {
        ResultSet rs = query("""
            SELECT e.name, top_proj.project_name, top_proj.hours
            FROM cqs.employees e
            JOIN LATERAL (
                SELECT p.name AS project_name, ep.hours
                FROM cqs.employee_projects ep
                JOIN cqs.projects p ON p.id = ep.project_id
                WHERE ep.employee_id = e.id
                ORDER BY ep.hours DESC
                LIMIT 1
            ) top_proj ON true
            WHERE e.name IN ('Alice', 'Bob', 'Jack')
            ORDER BY e.name
            """);
        assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
        assertEquals("API v2", rs.getString(2)); assertEquals(320, rs.getInt(3));
        assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
        assertEquals("API v2", rs.getString(2)); assertEquals(280, rs.getInt(3));
        assertTrue(rs.next()); assertEquals("Jack", rs.getString(1));
        assertEquals("Data Lake", rs.getString(2)); assertEquals(250, rs.getInt(3));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q10: Window function, running total of metrics per sensor
    // =========================================================================
    @Test
    void q10_window_running_total() throws SQLException {
        ResultSet rs = query("""
            SELECT sensor_id, measured_at, value,
                   SUM(value) OVER (PARTITION BY sensor_id ORDER BY measured_at
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
            FROM cqs.metrics
            WHERE sensor_id = 1
            ORDER BY measured_at
            """);
        assertTrue(rs.next()); assertEquals(23.5, rs.getDouble("value"), 0.01);
        assertEquals(23.5, rs.getDouble("running_sum"), 0.01);
        assertTrue(rs.next()); assertEquals(24.1, rs.getDouble("value"), 0.01);
        assertEquals(47.6, rs.getDouble("running_sum"), 0.01);
        assertTrue(rs.next()); assertEquals(22.8, rs.getDouble("value"), 0.01);
        assertEquals(70.4, rs.getDouble("running_sum"), 0.01);
        assertTrue(rs.next()); assertEquals(25.0, rs.getDouble("value"), 0.01);
        assertEquals(95.4, rs.getDouble("running_sum"), 0.01);
        assertTrue(rs.next()); assertEquals(23.9, rs.getDouble("value"), 0.01);
        assertEquals(119.3, rs.getDouble("running_sum"), 0.01);
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q11: CTE with UPDATE RETURNING for bulk raise and report
    // =========================================================================
    @Test
    void q11_cte_update_returning() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("""
                WITH raised AS (
                    UPDATE cqs.employees SET salary = salary * 1.10
                    WHERE department_id = (SELECT id FROM cqs.departments WHERE name = 'Frontend')
                    RETURNING id, name, salary
                )
                SELECT name, salary FROM raised ORDER BY name
                """);
            // Charlie was 105000 → 115500, Frank was 98000 → 107800
            assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1));
            assertEquals(0, rs.getBigDecimal(2).compareTo(new java.math.BigDecimal("115500.00")));
            assertTrue(rs.next()); assertEquals("Frank", rs.getString(1));
            assertEquals(0, rs.getBigDecimal(2).compareTo(new java.math.BigDecimal("107800.00")));
            assertFalse(rs.next());
            // restore
            stmt.execute("UPDATE cqs.employees SET salary = 105000 WHERE name = 'Charlie'");
            stmt.execute("UPDATE cqs.employees SET salary = 98000 WHERE name = 'Frank'");
        }
    }

    // =========================================================================
    // Q12: JSONB extraction + aggregation, product metadata stats
    // =========================================================================
    @Test
    void q12_jsonb_extraction_aggregation() throws SQLException {
        ResultSet rs = query("""
            SELECT category,
                   COUNT(*) AS cnt,
                   AVG((metadata->>'weight')::numeric) AS avg_weight
            FROM cqs.products
            WHERE metadata ? 'weight'
            GROUP BY category
            ORDER BY category
            """);
        // hardware: Widget A(0.5), Widget B(1.2), Gadget(3.5) → avg = 1.733..
        assertTrue(rs.next()); assertEquals("hardware", rs.getString(1));
        assertEquals(3, rs.getInt("cnt"));
        double avgW = rs.getDouble("avg_weight");
        assertTrue(avgW > 1.73 && avgW < 1.74, "Expected ~1.733, got " + avgW);
        assertFalse(rs.next()); // software has no weight
    }

    // =========================================================================
    // Q13: UNION ALL + aggregate, combined revenue report
    // =========================================================================
    @Test
    void q13_union_all_aggregate() throws SQLException {
        ResultSet rs = query("""
            SELECT source, SUM(amount) AS total FROM (
                SELECT 'orders' AS source, total AS amount FROM cqs.orders
                UNION ALL
                SELECT 'products' AS source, price AS amount FROM cqs.products
            ) combined
            GROUP BY source
            ORDER BY source
            """);
        // orders totals: 499.90+995.00+226.50+999.80+999.00+960.00 = 4680.20
        // products prices: 29.99+49.99+99.99+149.99+19.99 = 349.95
        assertTrue(rs.next()); assertEquals("orders", rs.getString(1));
        assertEquals(0, rs.getBigDecimal("total").compareTo(new java.math.BigDecimal("4680.20")));
        assertTrue(rs.next()); assertEquals("products", rs.getString(1));
        assertEquals(0, rs.getBigDecimal("total").compareTo(new java.math.BigDecimal("349.95")));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q14: Subquery in SELECT list for department head count and budget utilization
    // =========================================================================
    @Test
    void q14_scalar_subquery_in_select() throws SQLException {
        ResultSet rs = query("""
            SELECT d.name,
                   d.budget,
                   (SELECT COUNT(*) FROM cqs.employees e WHERE e.department_id = d.id AND e.is_active) AS headcount,
                   (SELECT COALESCE(SUM(e.salary), 0) FROM cqs.employees e WHERE e.department_id = d.id AND e.is_active) AS total_salary,
                   CASE WHEN d.budget > 0 THEN
                       ROUND((SELECT COALESCE(SUM(e.salary), 0) FROM cqs.employees e WHERE e.department_id = d.id AND e.is_active) / d.budget * 100, 1)
                   ELSE 0 END AS utilization_pct
            FROM cqs.departments d
            WHERE d.parent_id IS NULL
            ORDER BY d.name
            """);
        // Engineering: budget=500000, headcount=1(Jack), salary=150000, util=30.0%
        assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
        assertEquals(1, rs.getInt("headcount"));
        assertEquals(0, rs.getBigDecimal("utilization_pct").compareTo(new java.math.BigDecimal("30.0")));
        // Marketing: budget=250000, headcount=1(Grace active), salary=88000, util=35.2%
        assertTrue(rs.next()); assertEquals("Marketing", rs.getString(1));
        assertEquals(1, rs.getInt("headcount"));
        // Sales: budget=300000, headcount=1(Diana), salary=95000, util=31.7%
        assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));
        assertEquals(1, rs.getInt("headcount"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q15: COALESCE + LEFT JOIN, all departments with optional project count
    // =========================================================================
    @Test
    void q15_left_join_coalesce() throws SQLException {
        ResultSet rs = query("""
            SELECT d.name, COALESCE(pc.cnt, 0) AS project_count
            FROM cqs.departments d
            LEFT JOIN (
                SELECT dept_id, COUNT(*) AS cnt FROM cqs.projects GROUP BY dept_id
            ) pc ON pc.dept_id = d.id
            ORDER BY d.name
            """);
        assertTrue(rs.next()); assertEquals("Backend", rs.getString(1));    assertEquals(1, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(1, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Enterprise", rs.getString(1)); assertEquals(0, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Frontend", rs.getString(1));   assertEquals(1, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Marketing", rs.getString(1));  assertEquals(1, rs.getInt(2));
        assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));      assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q16: Window LAG/LEAD, sensor value deltas with arithmetic
    // =========================================================================
    @Test
    void q16_window_lag_lead_delta() throws SQLException {
        ResultSet rs = query("""
            SELECT sensor_id, measured_at, value,
                   value - LAG(value) OVER (PARTITION BY sensor_id ORDER BY measured_at) AS delta,
                   LEAD(value) OVER (PARTITION BY sensor_id ORDER BY measured_at) AS next_val
            FROM cqs.metrics
            WHERE sensor_id = 1
            ORDER BY measured_at
            """);
        // Row 1: 23.5, delta=null (no LAG for first row), next=24.1
        assertTrue(rs.next());
        assertEquals(23.5, rs.getDouble("value"), 0.01);
        assertNull(rs.getObject("delta"));
        assertEquals(24.1, rs.getDouble("next_val"), 0.01);

        // Row 2: 24.1, delta = 24.1 - 23.5 = 0.6, next=22.8
        assertTrue(rs.next());
        assertEquals(24.1, rs.getDouble("value"), 0.01);
        assertEquals(0.6, rs.getDouble("delta"), 0.01);
        assertEquals(22.8, rs.getDouble("next_val"), 0.01);

        // Row 3: 22.8, delta = 22.8 - 24.1 = -1.3, next=25.0
        assertTrue(rs.next());
        assertEquals(22.8, rs.getDouble("value"), 0.01);
        assertEquals(-1.3, rs.getDouble("delta"), 0.01);
        assertEquals(25.0, rs.getDouble("next_val"), 0.01);

        // Row 4: 25.0, delta = 25.0 - 22.8 = 2.2, next=23.9
        assertTrue(rs.next());
        assertEquals(25.0, rs.getDouble("value"), 0.01);
        assertEquals(2.2, rs.getDouble("delta"), 0.01);
        assertEquals(23.9, rs.getDouble("next_val"), 0.01);

        // Row 5: 23.9, delta = 23.9 - 25.0 = -1.1, next=null
        assertTrue(rs.next());
        assertEquals(23.9, rs.getDouble("value"), 0.01);
        assertEquals(-1.1, rs.getDouble("delta"), 0.01);
        assertNull(rs.getObject("next_val"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q17: INSERT ... ON CONFLICT (upsert) + RETURNING
    // =========================================================================
    @Test
    void q17_upsert_on_conflict() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("""
                INSERT INTO cqs.departments (name, budget)
                VALUES ('Marketing', 300000)
                ON CONFLICT (name) DO UPDATE SET budget = EXCLUDED.budget
                RETURNING name, budget
                """);
            assertTrue(rs.next());
            assertEquals("Marketing", rs.getString(1));
            assertEquals(0, rs.getBigDecimal(2).compareTo(new java.math.BigDecimal("300000")));
            assertFalse(rs.next());
            // restore
            stmt.execute("UPDATE cqs.departments SET budget = 250000 WHERE name = 'Marketing'");
        }
    }

    // =========================================================================
    // Q18: Multiple CTEs cross-referencing project and employee stats
    // =========================================================================
    @Test
    void q18_multiple_ctes_cross_ref() throws SQLException {
        ResultSet rs = query("""
            WITH dept_stats AS (
                SELECT d.id, d.name,
                       COUNT(e.id) AS emp_count,
                       AVG(e.salary) AS avg_salary
                FROM cqs.departments d
                LEFT JOIN cqs.employees e ON e.department_id = d.id AND e.is_active
                GROUP BY d.id, d.name
            ),
            proj_stats AS (
                SELECT p.dept_id,
                       COUNT(*) AS proj_count,
                       COUNT(*) FILTER (WHERE p.status = 'active') AS active_projs
                FROM cqs.projects p
                GROUP BY p.dept_id
            )
            SELECT ds.name, ds.emp_count, ds.avg_salary,
                   COALESCE(ps.proj_count, 0) AS projects,
                   COALESCE(ps.active_projs, 0) AS active_projects
            FROM dept_stats ds
            LEFT JOIN proj_stats ps ON ps.dept_id = ds.id
            WHERE ds.emp_count > 0
            ORDER BY ds.avg_salary DESC
            """);
        // Jack=150000 in Engineering
        assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
        assertEquals(1, rs.getInt("emp_count"));
        // Eve=130000 in Enterprise
        assertTrue(rs.next()); assertEquals("Enterprise", rs.getString(1));
        assertEquals(1, rs.getInt("emp_count"));
    }

    // =========================================================================
    // Q19: CASE in ORDER BY + NULLS handling
    // =========================================================================
    @Test
    void q19_case_in_order_by_nulls() throws SQLException {
        ResultSet rs = query("""
            SELECT name, end_date, status,
                   CASE status
                       WHEN 'active'    THEN 1
                       WHEN 'planning'  THEN 2
                       WHEN 'completed' THEN 3
                       WHEN 'cancelled' THEN 4
                   END AS sort_order
            FROM cqs.projects
            ORDER BY CASE status
                       WHEN 'active'    THEN 1
                       WHEN 'planning'  THEN 2
                       WHEN 'completed' THEN 3
                       WHEN 'cancelled' THEN 4
                     END,
                     end_date NULLS LAST
            """);
        // active projects first (3): API v2 (end 2024-06-30), Dashboard (2024-09-30), Data Lake (2025-03-31)
        assertTrue(rs.next()); assertEquals("API v2", rs.getString(1));     assertEquals(1, rs.getInt("sort_order"));
        assertTrue(rs.next()); assertEquals("Dashboard", rs.getString(1));  assertEquals(1, rs.getInt("sort_order"));
        assertTrue(rs.next()); assertEquals("Data Lake", rs.getString(1));  assertEquals(1, rs.getInt("sort_order"));
        // then planning: Sales Portal (end null, NULLS LAST)
        assertTrue(rs.next()); assertEquals("Sales Portal", rs.getString(1)); assertEquals(2, rs.getInt("sort_order"));
        // then completed: Rebrand
        assertTrue(rs.next()); assertEquals("Rebrand", rs.getString(1));    assertEquals(3, rs.getInt("sort_order"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q20: Array operations, products with specific tags
    // =========================================================================
    @Test
    void q20_array_operations() throws SQLException {
        ResultSet rs = query("""
            SELECT name, tags, array_length(tags, 1) AS tag_count
            FROM cqs.products
            WHERE tags @> ARRAY['small']
            ORDER BY name
            """);
        // Widget A has ['small','blue'], Plugin has ['small','free']
        assertTrue(rs.next()); assertEquals("Plugin", rs.getString(1));   assertEquals(2, rs.getInt("tag_count"));
        assertTrue(rs.next()); assertEquals("Widget A", rs.getString(1)); assertEquals(2, rs.getInt("tag_count"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q21: Self-join for department hierarchy with parent name
    // =========================================================================
    @Test
    void q21_self_join_hierarchy() throws SQLException {
        ResultSet rs = query("""
            SELECT child.name AS dept, parent.name AS parent_dept
            FROM cqs.departments child
            JOIN cqs.departments parent ON parent.id = child.parent_id
            ORDER BY parent.name, child.name
            """);
        // Engineering → Backend, Frontend; Sales → Enterprise
        assertTrue(rs.next()); assertEquals("Backend",    rs.getString(1)); assertEquals("Engineering", rs.getString(2));
        assertTrue(rs.next()); assertEquals("Frontend",   rs.getString(1)); assertEquals("Engineering", rs.getString(2));
        assertTrue(rs.next()); assertEquals("Enterprise", rs.getString(1)); assertEquals("Sales",       rs.getString(2));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q22: DO block with dynamic SQL to create and populate a summary table
    // =========================================================================
    @Test
    void q22_do_block_dynamic_sql() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                DO $$
                BEGIN
                    EXECUTE 'CREATE TABLE IF NOT EXISTS cqs.dept_summary (
                        dept_name TEXT PRIMARY KEY,
                        emp_count INT
                    )';
                    EXECUTE 'INSERT INTO cqs.dept_summary
                             SELECT d.name, COUNT(e.id)
                             FROM cqs.departments d
                             LEFT JOIN cqs.employees e ON e.department_id = d.id AND e.is_active
                             GROUP BY d.name
                             ON CONFLICT (dept_name) DO UPDATE SET emp_count = EXCLUDED.emp_count';
                END $$
                """);
            ResultSet rs = stmt.executeQuery(
                "SELECT dept_name, emp_count FROM cqs.dept_summary ORDER BY dept_name");
            assertTrue(rs.next()); assertEquals("Backend", rs.getString(1));     assertEquals(3, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Enterprise", rs.getString(1));  assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Frontend", rs.getString(1));    assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Marketing", rs.getString(1));   assertEquals(1, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("Sales", rs.getString(1));       assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // Q23: INTERSECT + EXCEPT, set operations on employee assignments
    // =========================================================================
    @Test
    void q23_set_operations() throws SQLException {
        ResultSet rs = query("""
            SELECT e.name FROM cqs.employees e
            JOIN cqs.employee_projects ep ON ep.employee_id = e.id
            JOIN cqs.projects p ON p.id = ep.project_id
            WHERE p.name = 'API v2'
            INTERSECT
            SELECT e.name FROM cqs.employees e
            JOIN cqs.employee_projects ep ON ep.employee_id = e.id
            JOIN cqs.projects p ON p.id = ep.project_id
            WHERE p.name = 'Data Lake'
            ORDER BY name
            """);
        // API v2: Alice, Bob, Hank; Data Lake: Jack, Alice, Bob
        // Intersection: Alice, Bob
        assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
        assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q24: Nested CASE + string functions for employee classification
    // =========================================================================
    @Test
    void q24_nested_case_string_functions() throws SQLException {
        ResultSet rs = query("""
            SELECT e.name,
                   UPPER(LEFT(d.name, 3)) AS dept_code,
                   CASE
                       WHEN e.salary >= 130000 THEN 'executive'
                       WHEN e.salary >= 100000 THEN
                           CASE WHEN ep.role = 'lead' THEN 'senior-lead'
                                ELSE 'senior'
                           END
                       ELSE 'standard'
                   END AS classification
            FROM cqs.employees e
            JOIN cqs.departments d ON d.id = e.department_id
            LEFT JOIN cqs.employee_projects ep ON ep.employee_id = e.id AND ep.role = 'lead'
            WHERE e.is_active
            ORDER BY e.name
            """);
        assertTrue(rs.next()); assertEquals("Alice", rs.getString(1));
        assertEquals("BAC", rs.getString("dept_code"));
        assertEquals("senior-lead", rs.getString("classification"));

        assertTrue(rs.next()); assertEquals("Bob", rs.getString(1));
        assertEquals("BAC", rs.getString("dept_code"));
        assertEquals("senior", rs.getString("classification"));

        assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1));
        assertEquals("FRO", rs.getString("dept_code"));
        assertEquals("senior-lead", rs.getString("classification"));

        assertTrue(rs.next()); assertEquals("Diana", rs.getString(1));
        assertEquals("SAL", rs.getString("dept_code"));
        assertEquals("standard", rs.getString("classification"));

        assertTrue(rs.next()); assertEquals("Eve", rs.getString(1));
        assertEquals("ENT", rs.getString("dept_code"));
        assertEquals("executive", rs.getString("classification"));

        assertTrue(rs.next()); assertEquals("Frank", rs.getString(1));
        assertEquals("FRO", rs.getString("dept_code"));
        assertEquals("standard", rs.getString("classification"));

        assertTrue(rs.next()); assertEquals("Grace", rs.getString(1));
        assertEquals("MAR", rs.getString("dept_code"));
        assertEquals("standard", rs.getString("classification"));

        assertTrue(rs.next()); assertEquals("Hank", rs.getString(1));
        assertEquals("BAC", rs.getString("dept_code"));
        assertEquals("senior", rs.getString("classification"));

        assertTrue(rs.next()); assertEquals("Jack", rs.getString(1));
        assertEquals("ENG", rs.getString("dept_code"));
        assertEquals("executive", rs.getString("classification"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q25: Window NTILE + percentile, salary distribution
    // =========================================================================
    @Test
    void q25_window_ntile_percentile() throws SQLException {
        ResultSet rs = query("""
            SELECT name, salary,
                   NTILE(4) OVER (ORDER BY salary) AS quartile,
                   PERCENT_RANK() OVER (ORDER BY salary) AS pct_rank
            FROM cqs.employees
            WHERE is_active
            ORDER BY salary
            """);
        // 9 active employees sorted by salary:
        // Grace 88k, Diana 95k, Frank 98k, Charlie 105k, Bob 110k,
        // Hank 115k, Alice 120k, Eve 130k, Jack 150k
        // NTILE(4) over 9: groups of [3,3,2,1] → no, NTILE(4) over 9 = ceil(9/4)=3,2,2,2
        // Actually: 9/4 = 2 rem 1 → first 1 groups get 3, rest get 2
        // → groups: [3, 2, 2, 2]
        assertTrue(rs.next()); assertEquals("Grace",   rs.getString(1)); assertEquals(1, rs.getInt("quartile"));
        assertTrue(rs.next()); assertEquals("Diana",   rs.getString(1)); assertEquals(1, rs.getInt("quartile"));
        assertTrue(rs.next()); assertEquals("Frank",   rs.getString(1)); assertEquals(1, rs.getInt("quartile"));
        assertTrue(rs.next()); assertEquals("Charlie", rs.getString(1)); assertEquals(2, rs.getInt("quartile"));
        assertTrue(rs.next()); assertEquals("Bob",     rs.getString(1)); assertEquals(2, rs.getInt("quartile"));
        assertTrue(rs.next()); assertEquals("Hank",    rs.getString(1)); assertEquals(3, rs.getInt("quartile"));
        assertTrue(rs.next()); assertEquals("Alice",   rs.getString(1)); assertEquals(3, rs.getInt("quartile"));
        assertTrue(rs.next()); assertEquals("Eve",     rs.getString(1)); assertEquals(4, rs.getInt("quartile"));
        assertTrue(rs.next()); assertEquals("Jack",    rs.getString(1)); assertEquals(4, rs.getInt("quartile"));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q26: DELETE with RETURNING + CTE to archive completed projects
    // =========================================================================
    @Test
    void q26_delete_returning_cte() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // First insert a dummy completed project to delete
            stmt.execute("INSERT INTO cqs.projects (name, dept_id, start_date, end_date, status) " +
                         "VALUES ('Temp Project', 1, '2023-01-01', '2023-06-30', 'completed')");
            ResultSet rs = stmt.executeQuery("""
                WITH deleted AS (
                    DELETE FROM cqs.projects
                    WHERE name = 'Temp Project'
                    RETURNING id, name, status
                )
                SELECT name, status FROM deleted
                """);
            assertTrue(rs.next());
            assertEquals("Temp Project", rs.getString(1));
            assertEquals("completed", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // Q27: GROUPING SETS for multi-dimensional aggregation
    // =========================================================================
    @Test
    void q27_grouping_sets() throws SQLException {
        ResultSet rs = query("""
            SELECT d.name AS dept,
                   p.status,
                   COUNT(*) AS cnt
            FROM cqs.projects p
            JOIN cqs.departments d ON d.id = p.dept_id
            GROUP BY GROUPING SETS ((d.name), (p.status), ())
            ORDER BY dept NULLS LAST, status NULLS LAST
            """);
        // By dept: Backend(1), Engineering(1), Frontend(1), Marketing(1), Sales(1)
        List<String> depts = new ArrayList<>();
        List<String> statuses = new ArrayList<>();
        int grandTotal = 0;
        while (rs.next()) {
            String dept = rs.getString("dept");
            String status = rs.getString("status");
            int cnt = rs.getInt("cnt");
            if (dept != null && status == null) depts.add(dept + ":" + cnt);
            else if (dept == null && status != null) statuses.add(status + ":" + cnt);
            else if (dept == null) grandTotal = cnt;
        }
        assertTrue(depts.contains("Backend:1"));
        assertTrue(depts.contains("Engineering:1"));
        assertTrue(statuses.contains("active:3"));
        assertTrue(statuses.contains("completed:1"));
        assertTrue(statuses.contains("planning:1"));
        assertEquals(5, grandTotal);
    }

    // =========================================================================
    // Q28: Subquery with IN + aggregate, departments with above-average salary
    // =========================================================================
    @Test
    void q28_subquery_in_having() throws SQLException {
        ResultSet rs = query("""
            SELECT d.name, ROUND(AVG(e.salary), 2) AS avg_sal
            FROM cqs.departments d
            JOIN cqs.employees e ON e.department_id = d.id AND e.is_active
            GROUP BY d.name
            HAVING AVG(e.salary) > (SELECT AVG(salary) FROM cqs.employees WHERE is_active)
            ORDER BY avg_sal DESC
            """);
        // Overall avg of active: (120k+110k+105k+95k+130k+98k+88k+115k+150k)/9 = 1011000/9 = 112333.33
        // Backend avg = 115000, Engineering = 150000, Enterprise = 130000
        // Frontend avg = 101500 (below), Sales = 95000 (below), Marketing = 88000 (below)
        assertTrue(rs.next()); assertEquals("Engineering", rs.getString(1));
        assertTrue(rs.next()); assertEquals("Enterprise", rs.getString(1));
        assertTrue(rs.next()); assertEquals("Backend", rs.getString(1));
        assertFalse(rs.next());
    }

    // =========================================================================
    // Q29: MERGE statement, upsert with matched/not matched
    // =========================================================================
    @Test
    void q29_merge_upsert() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE cqs.inventory (
                    product_id INT PRIMARY KEY,
                    qty INT NOT NULL,
                    last_restock DATE
                )
                """);
            stmt.execute("INSERT INTO cqs.inventory VALUES (1, 50, '2024-01-01'), (2, 30, '2024-01-15')");

            stmt.execute("""
                MERGE INTO cqs.inventory t
                USING (VALUES (1, 20, '2024-02-01'::date), (3, 100, '2024-02-01'::date)) AS s(pid, qty, dt)
                ON t.product_id = s.pid
                WHEN MATCHED THEN UPDATE SET qty = t.qty + s.qty, last_restock = s.dt
                WHEN NOT MATCHED THEN INSERT (product_id, qty, last_restock) VALUES (s.pid, s.qty, s.dt)
                """);

            ResultSet rs = stmt.executeQuery("SELECT product_id, qty FROM cqs.inventory ORDER BY product_id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals(70, rs.getInt(2));   // merged
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals(30, rs.getInt(2));   // untouched
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1)); assertEquals(100, rs.getInt(2));  // inserted
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // Q30: Complex expression using GREATEST/LEAST + COALESCE + date arithmetic
    // =========================================================================
    @Test
    void q30_complex_expression_dates() throws SQLException {
        ResultSet rs = query("""
            SELECT name,
                   GREATEST(start_date, '2024-02-01'::date) AS effective_start,
                   COALESCE(end_date, CURRENT_DATE) - start_date AS duration_days,
                   LEAST(COALESCE(end_date, '2099-12-31'::date), '2024-12-31'::date) AS capped_end
            FROM cqs.projects
            WHERE status = 'active'
            ORDER BY name
            """);
        // API v2: start 2024-01-01, end 2024-06-30
        assertTrue(rs.next()); assertEquals("API v2", rs.getString(1));
        assertEquals(java.sql.Date.valueOf("2024-02-01"), rs.getDate("effective_start"));
        assertEquals(181, rs.getInt("duration_days")); // 2024-01-01 to 2024-06-30
        assertEquals(java.sql.Date.valueOf("2024-06-30"), rs.getDate("capped_end"));

        // Dashboard: start 2024-02-01, end 2024-09-30
        assertTrue(rs.next()); assertEquals("Dashboard", rs.getString(1));
        assertEquals(java.sql.Date.valueOf("2024-02-01"), rs.getDate("effective_start"));
        assertEquals(242, rs.getInt("duration_days"));
        assertEquals(java.sql.Date.valueOf("2024-09-30"), rs.getDate("capped_end"));

        // Data Lake: start 2024-04-01, end 2025-03-31 (364 days)
        assertTrue(rs.next()); assertEquals("Data Lake", rs.getString(1));
        assertEquals(java.sql.Date.valueOf("2024-04-01"), rs.getDate("effective_start"));
        assertEquals(364, rs.getInt("duration_days"));
        assertEquals(java.sql.Date.valueOf("2024-12-31"), rs.getDate("capped_end")); // capped
        assertFalse(rs.next());
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private ResultSet query(String sql) throws SQLException {
        return conn.createStatement().executeQuery(sql);
    }

    /** Split SQL on semicolons, respecting quotes and dollar-quoting. */
    static List<String> splitStatements(String sql) {
        List<String> stmts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingle = false, inDouble = false;
        String dollarTag = null;
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (dollarTag != null) {
                current.append(c);
                if (c == '$' && sql.indexOf(dollarTag, i) == i) {
                    current.append(dollarTag.substring(1)); // rest of tag
                    i += dollarTag.length();
                    dollarTag = null;
                } else {
                    i++;
                }
                continue;
            }
            if (inSingle) {
                current.append(c);
                if (c == '\'') inSingle = false;
                i++;
                continue;
            }
            if (inDouble) {
                current.append(c);
                if (c == '"') inDouble = false;
                i++;
                continue;
            }
            // Check for dollar-quote start
            if (c == '$') {
                int end = sql.indexOf('$', i + 1);
                if (end > i) {
                    String tag = sql.substring(i, end + 1);
                    if (tag.matches("\\$[a-zA-Z0-9_]*\\$")) {
                        dollarTag = tag;
                        current.append(tag);
                        i = end + 1;
                        continue;
                    }
                }
            }
            if (c == '\'') { inSingle = true; current.append(c); i++; continue; }
            if (c == '"')  { inDouble = true; current.append(c); i++; continue; }
            if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                int eol = sql.indexOf('\n', i);
                if (eol < 0) break;
                i = eol + 1;
                continue;
            }
            if (c == ';') {
                String s = current.toString().trim();
                if (!s.isEmpty()) stmts.add(s);
                current.setLength(0);
                i++;
                continue;
            }
            current.append(c);
            i++;
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) stmts.add(last);
        return stmts;
    }
}
