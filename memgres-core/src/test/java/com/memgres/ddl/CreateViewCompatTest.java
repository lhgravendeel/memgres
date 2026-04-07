package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CREATE VIEW syntax variants found in real-world schemas.
 * Many views use complex subqueries, casts, joins with parenthesized syntax,
 * and aggregate expressions that fail during view creation.
 */
class CreateViewCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        // Set up base tables for views
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE departments (id serial PRIMARY KEY, name text, parent_id int)");
            s.execute("CREATE TABLE employees (id serial PRIMARY KEY, name text, dept_id int REFERENCES departments(id), salary numeric, active boolean DEFAULT true)");
            s.execute("CREATE TABLE projects (id serial PRIMARY KEY, name text, dept_id int REFERENCES departments(id), budget numeric)");
            s.execute("CREATE TABLE assignments (employee_id int REFERENCES employees(id), project_id int REFERENCES projects(id), role text)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // Views with CTE (WITH clause)
    // =========================================================================

    @Test
    void testViewWithCte() throws SQLException {
        exec("""
            CREATE VIEW active_staff AS
            WITH active_emps AS (
                SELECT id, name, dept_id, salary
                FROM employees
                WHERE active = true
            )
            SELECT ae.id, ae.name, d.name AS dept_name, ae.salary
            FROM active_emps ae
            JOIN departments d ON d.id = ae.dept_id
        """);
        // View should be queryable (even if empty)
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM active_staff")) {
            assertNotNull(rs.getMetaData());
        }
    }

    // =========================================================================
    // Views with parenthesized JOINs (pg_dump style)
    // =========================================================================

    @Test
    void testViewWithParenthesizedJoins() throws SQLException {
        // pg_dump wraps joins in extra parentheses: FROM ((a JOIN b ON ...) JOIN c ON ...)
        exec("""
            CREATE VIEW assignment_details AS
            SELECT e.name AS employee_name, p.name AS project_name, a.role, d.name AS dept_name
            FROM ((assignments a
                JOIN employees e ON ((e.id = a.employee_id)))
                JOIN projects p ON ((p.id = a.project_id)))
                JOIN departments d ON ((d.id = e.dept_id))
        """);
    }

    @Test
    void testViewWithDoubleParenthesizedConditions() throws SQLException {
        // pg_dump adds double parens around ON conditions
        exec("""
            CREATE VIEW emp_projects AS
            SELECT e.id, e.name, p.name AS project_name
            FROM (employees e
                LEFT JOIN assignments a ON ((a.employee_id = e.id)))
                LEFT JOIN projects p ON ((p.id = a.project_id))
        """);
    }

    // =========================================================================
    // Views with subqueries in SELECT list
    // =========================================================================

    @Test
    void testViewWithScalarSubquery() throws SQLException {
        exec("""
            CREATE VIEW dept_summary AS
            SELECT d.id, d.name,
                (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS employee_count,
                (SELECT COALESCE(SUM(e.salary), 0) FROM employees e WHERE e.dept_id = d.id) AS total_salary
            FROM departments d
        """);
    }

    // =========================================================================
    // Views with UNION
    // =========================================================================

    @Test
    void testViewWithUnion() throws SQLException {
        exec("""
            CREATE VIEW all_members AS
            SELECT id, name, 'employee' AS member_type FROM employees
            UNION
            SELECT id, name, 'department' AS member_type FROM departments
        """);
    }

    @Test
    void testViewWithUnionAll() throws SQLException {
        exec("""
            CREATE VIEW combined_costs AS
            SELECT 'salary' AS type, SUM(salary) AS amount FROM employees
            UNION ALL
            SELECT 'budget' AS type, SUM(budget) AS amount FROM projects
        """);
    }

    // =========================================================================
    // Views with type casts (pg_dump style)
    // =========================================================================

    @Test
    void testViewWithExplicitCasts() throws SQLException {
        exec("""
            CREATE VIEW casted_view AS
            SELECT id, name,
                (salary)::numeric(10,2) AS formatted_salary,
                (name)::character varying AS varchar_name
            FROM employees
        """);
    }

    @Test
    void testViewWithCastInJoinCondition() throws SQLException {
        exec("""
            CREATE VIEW cast_join_view AS
            SELECT e.name, d.name AS dept
            FROM employees e
            JOIN departments d ON (d.id = (e.dept_id)::integer)
        """);
    }

    // =========================================================================
    // Views with string concatenation using ||
    // =========================================================================

    @Test
    void testViewWithStringConcat() throws SQLException {
        exec("""
            CREATE VIEW named_view AS
            SELECT id,
                (name || ' (' || COALESCE(role, 'unassigned') || ')') AS display_name
            FROM employees e
            LEFT JOIN assignments a ON a.employee_id = e.id
        """);
    }

    @Test
    void testViewWithNamespaceConcatCast() throws SQLException {
        // pg_dump pattern: ((schema)::text || '.'::text || (table)::text)
        exec("""
            CREATE VIEW full_names AS
            SELECT id, (('dept_'::text || (dept_id)::text || '_'::text) || name) AS qualified_name
            FROM employees
        """);
    }

    // =========================================================================
    // Views with CASE expressions
    // =========================================================================

    @Test
    void testViewWithCaseExpression() throws SQLException {
        exec("""
            CREATE VIEW categorized_employees AS
            SELECT id, name,
                CASE
                    WHEN salary > 100000 THEN 'senior'
                    WHEN salary > 50000 THEN 'mid'
                    ELSE 'junior'
                END AS level
            FROM employees
        """);
    }

    // =========================================================================
    // Views with aggregate FILTER (WHERE ...)
    // =========================================================================

    @Test
    void testViewWithAggregateFilter() throws SQLException {
        exec("""
            CREATE VIEW dept_stats AS
            SELECT dept_id,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE active = true) AS active_count,
                AVG(salary) FILTER (WHERE active = true) AS avg_active_salary
            FROM employees
            GROUP BY dept_id
        """);
    }

    // =========================================================================
    // Views with LATERAL join
    // =========================================================================

    @Test
    void testViewWithLateralJoin() throws SQLException {
        exec("""
            CREATE VIEW top_earner_per_dept AS
            SELECT d.id, d.name AS dept_name, t.employee_name, t.salary
            FROM departments d
            LEFT JOIN LATERAL (
                SELECT e.name AS employee_name, e.salary
                FROM employees e
                WHERE e.dept_id = d.id
                ORDER BY e.salary DESC
                LIMIT 1
            ) t ON true
        """);
    }

    // =========================================================================
    // Views with window functions
    // =========================================================================

    @Test
    void testViewWithWindowFunction() throws SQLException {
        exec("""
            CREATE VIEW ranked_employees AS
            SELECT id, name, dept_id, salary,
                ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rank_in_dept,
                DENSE_RANK() OVER (ORDER BY salary DESC) AS global_rank
            FROM employees
        """);
    }

    // =========================================================================
    // CREATE OR REPLACE VIEW
    // =========================================================================

    @Test
    void testCreateOrReplaceView() throws SQLException {
        exec("CREATE VIEW replaceable_view AS SELECT id, name FROM employees");
        exec("CREATE OR REPLACE VIEW replaceable_view AS SELECT id, name, salary FROM employees");
        // Should now include salary column
    }

    // =========================================================================
    // CREATE MATERIALIZED VIEW
    // =========================================================================

    @Test
    void testCreateMaterializedView() throws SQLException {
        exec("""
            CREATE MATERIALIZED VIEW dept_salary_mv AS
            SELECT d.id, d.name, COALESCE(SUM(e.salary), 0) AS total_salary
            FROM departments d
            LEFT JOIN employees e ON e.dept_id = d.id
            GROUP BY d.id, d.name
        """);
    }

    @Test
    void testRefreshMaterializedView() throws SQLException {
        exec("CREATE MATERIALIZED VIEW simple_mv AS SELECT COUNT(*) AS cnt FROM employees");
        exec("REFRESH MATERIALIZED VIEW simple_mv");
    }

    @Test
    void testRefreshMaterializedViewConcurrently() throws SQLException {
        exec("CREATE MATERIALIZED VIEW conc_mv AS SELECT id, name FROM employees");
        exec("CREATE UNIQUE INDEX idx_conc_mv ON conc_mv (id)");
        exec("REFRESH MATERIALIZED VIEW CONCURRENTLY conc_mv");
    }

    // =========================================================================
    // Views with unnest / generate_series
    // =========================================================================

    @Test
    void testViewWithUnnest() throws SQLException {
        exec("CREATE TABLE tag_holder (id serial PRIMARY KEY, tags text[])");
        exec("""
            CREATE VIEW expanded_tags AS
            SELECT id, unnest(tags) AS tag FROM tag_holder
        """);
    }

    @Test
    void testViewWithGenerateSeries() throws SQLException {
        exec("""
            CREATE VIEW date_range AS
            SELECT generate_series('2024-01-01'::date, '2024-12-31'::date, '1 month'::interval) AS month_start
        """);
    }

    // =========================================================================
    // Views in custom schemas
    // =========================================================================

    @Test
    void testCreateViewInSchema() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS reporting");
        exec("""
            CREATE VIEW reporting.headcount AS
            SELECT d.name AS department, COUNT(*) AS employees
            FROM departments d
            JOIN employees e ON e.dept_id = d.id
            GROUP BY d.name
        """);
    }
}
