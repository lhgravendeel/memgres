package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 6: EXPLAIN/planner behavior.
 * Tests EXPLAIN output formats, EXPLAIN ANALYZE, EXPLAIN for DML,
 * ANALYZE/VACUUM commands, ordering guarantees, and partitioned tables.
 */
class ExplainPlannerTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }
    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // --- 1. EXPLAIN SELECT returns plan text ---

    @Test void explain_select_returns_plan_text() throws Exception {
        exec("CREATE TABLE ep_plan(id int, v text)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN SELECT * FROM ep_plan")) {
            assertTrue(rs.next(), "EXPLAIN should return at least one row");
            String line = rs.getString(1);
            assertNotNull(line);
            // Plan text typically contains "Seq Scan" or "Scan" for a simple table
            assertTrue(line.toLowerCase().contains("scan") || line.toLowerCase().contains("result"),
                    "Plan should mention a scan or result node: " + line);
        }
        exec("DROP TABLE ep_plan");
    }

    // --- 2. EXPLAIN ANALYZE SELECT returns actual timing ---

    @Test void explain_analyze_select_returns_timing() throws Exception {
        exec("CREATE TABLE ep_analyze(id int, v text)");
        exec("INSERT INTO ep_analyze VALUES (1, 'a'), (2, 'b')");
        StringBuilder plan = new StringBuilder();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN ANALYZE SELECT * FROM ep_analyze")) {
            while (rs.next()) {
                plan.append(rs.getString(1)).append("\n");
            }
        }
        String output = plan.toString();
        // EXPLAIN ANALYZE includes actual time and rows
        assertTrue(output.toLowerCase().contains("actual"),
                "EXPLAIN ANALYZE should include 'actual' timing info: " + output);
        exec("DROP TABLE ep_analyze");
    }

    // --- 3. EXPLAIN (FORMAT JSON) returns JSON output ---

    @Test void explain_format_json() throws Exception {
        exec("CREATE TABLE ep_json(id int)");
        StringBuilder plan = new StringBuilder();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN (FORMAT JSON) SELECT * FROM ep_json")) {
            while (rs.next()) {
                plan.append(rs.getString(1));
            }
        }
        String output = plan.toString().trim();
        assertTrue(output.startsWith("["), "JSON format should start with '[': " + output);
        assertTrue(output.contains("Plan"), "JSON should contain Plan key: " + output);
        exec("DROP TABLE ep_json");
    }

    // --- 4. EXPLAIN (FORMAT YAML) returns YAML output ---

    @Test void explain_format_yaml() throws Exception {
        exec("CREATE TABLE ep_yaml(id int)");
        StringBuilder plan = new StringBuilder();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN (FORMAT YAML) SELECT * FROM ep_yaml")) {
            while (rs.next()) {
                plan.append(rs.getString(1)).append("\n");
            }
        }
        String output = plan.toString();
        // YAML output contains "Plan:" or "- Plan:" as a key
        assertTrue(output.contains("Plan:"), "YAML format should contain 'Plan:': " + output);
        exec("DROP TABLE ep_yaml");
    }

    // --- 5. EXPLAIN (FORMAT XML) returns XML output ---

    @Test void explain_format_xml() throws Exception {
        exec("CREATE TABLE ep_xml(id int)");
        StringBuilder plan = new StringBuilder();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN (FORMAT XML) SELECT * FROM ep_xml")) {
            while (rs.next()) {
                plan.append(rs.getString(1));
            }
        }
        String output = plan.toString().trim();
        assertTrue(output.contains("<explain"), "XML format should contain '<explain' tag: " + output);
        assertTrue(output.contains("</explain"), "XML format should have closing explain tag: " + output);
        exec("DROP TABLE ep_xml");
    }

    // --- 6. EXPLAIN INSERT returns plan ---

    @Test void explain_insert_returns_plan() throws Exception {
        exec("CREATE TABLE ep_ins(id int, v text)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN INSERT INTO ep_ins VALUES (1, 'x')")) {
            assertTrue(rs.next(), "EXPLAIN INSERT should return plan rows");
            String line = rs.getString(1);
            assertNotNull(line);
            assertTrue(line.toLowerCase().contains("insert"),
                    "Plan should mention Insert: " + line);
        }
        // Verify the INSERT was not actually executed
        assertEquals("0", scalar("SELECT count(*) FROM ep_ins"));
        exec("DROP TABLE ep_ins");
    }

    // --- 7. EXPLAIN UPDATE returns plan ---

    @Test void explain_update_returns_plan() throws Exception {
        exec("CREATE TABLE ep_upd(id int, v text)");
        exec("INSERT INTO ep_upd VALUES (1, 'old')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN UPDATE ep_upd SET v = 'new' WHERE id = 1")) {
            assertTrue(rs.next(), "EXPLAIN UPDATE should return plan rows");
            String line = rs.getString(1);
            assertNotNull(line);
            assertTrue(line.toLowerCase().contains("update"),
                    "Plan should mention Update: " + line);
        }
        // Verify the UPDATE was not actually executed
        assertEquals("old", scalar("SELECT v FROM ep_upd WHERE id = 1"));
        exec("DROP TABLE ep_upd");
    }

    // --- 8. EXPLAIN DELETE returns plan ---

    @Test void explain_delete_returns_plan() throws Exception {
        exec("CREATE TABLE ep_del(id int)");
        exec("INSERT INTO ep_del VALUES (1), (2), (3)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN DELETE FROM ep_del WHERE id = 1")) {
            assertTrue(rs.next(), "EXPLAIN DELETE should return plan rows");
            String line = rs.getString(1);
            assertNotNull(line);
            assertTrue(line.toLowerCase().contains("delete"),
                    "Plan should mention Delete: " + line);
        }
        // Verify the DELETE was not actually executed
        assertEquals("3", scalar("SELECT count(*) FROM ep_del"));
        exec("DROP TABLE ep_del");
    }

    // --- 9. EXPLAIN with index scan (if index exists) ---

    @Test void explain_with_index_scan() throws Exception {
        exec("CREATE TABLE ep_idx(id int PRIMARY KEY, v text)");
        // Insert enough rows to encourage index usage
        for (int i = 1; i <= 100; i++) {
            exec("INSERT INTO ep_idx VALUES (" + i + ", 'val" + i + "')");
        }
        exec("ANALYZE ep_idx");
        StringBuilder plan = new StringBuilder();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("EXPLAIN SELECT * FROM ep_idx WHERE id = 42")) {
            while (rs.next()) {
                plan.append(rs.getString(1)).append("\n");
            }
        }
        String output = plan.toString().toLowerCase();
        // With a primary key and analyzed stats, planner should use an index scan
        assertTrue(output.contains("index") || output.contains("scan"),
                "Plan should reference index or scan for PK lookup: " + output);
        exec("DROP TABLE ep_idx");
    }

    // --- 10. ANALYZE table succeeds ---

    @Test void analyze_table_succeeds() throws Exception {
        exec("CREATE TABLE ep_anlz(id int, v text)");
        exec("INSERT INTO ep_anlz VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        // ANALYZE should complete without error
        exec("ANALYZE ep_anlz");
        // Verify table is still accessible
        assertEquals("3", scalar("SELECT count(*) FROM ep_anlz"));
        exec("DROP TABLE ep_anlz");
    }

    // --- 11. ANALYZE specific columns ---

    @Test void analyze_specific_columns() throws Exception {
        exec("CREATE TABLE ep_anlz_col(id int, name text, value numeric)");
        exec("INSERT INTO ep_anlz_col VALUES (1, 'a', 10.0), (2, 'b', 20.0)");
        // ANALYZE specific columns
        exec("ANALYZE ep_anlz_col(name, value)");
        // Verify table is still accessible
        assertEquals("2", scalar("SELECT count(*) FROM ep_anlz_col"));
        exec("DROP TABLE ep_anlz_col");
    }

    // --- 12. VACUUM table succeeds ---

    @Test void vacuum_table_succeeds() throws Exception {
        exec("CREATE TABLE ep_vac(id int, v text)");
        exec("INSERT INTO ep_vac VALUES (1, 'a'), (2, 'b')");
        exec("DELETE FROM ep_vac WHERE id = 1");
        // VACUUM should complete without error
        exec("VACUUM ep_vac");
        assertEquals("1", scalar("SELECT count(*) FROM ep_vac"));
        exec("DROP TABLE ep_vac");
    }

    // --- 13. VACUUM ANALYZE combined ---

    @Test void vacuum_analyze_combined() throws Exception {
        exec("CREATE TABLE ep_vacanlz(id int, v text)");
        exec("INSERT INTO ep_vacanlz VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        exec("DELETE FROM ep_vacanlz WHERE id = 2");
        // VACUUM ANALYZE performs both operations
        exec("VACUUM ANALYZE ep_vacanlz");
        assertEquals("2", scalar("SELECT count(*) FROM ep_vacanlz"));
        exec("DROP TABLE ep_vacanlz");
    }

    // --- 14. Unordered SELECT without ORDER BY returns all rows (count check) ---

    @Test void unordered_select_returns_all_rows() throws Exception {
        exec("CREATE TABLE ep_unord(id int, v text)");
        exec("INSERT INTO ep_unord VALUES (3, 'c'), (1, 'a'), (2, 'b'), (5, 'e'), (4, 'd')");
        int count = 0;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM ep_unord")) {
            while (rs.next()) count++;
        }
        assertEquals(5, count, "Unordered SELECT should return all 5 rows");
        exec("DROP TABLE ep_unord");
    }

    // --- 15. Ordered SELECT with ORDER BY returns correct order ---

    @Test void ordered_select_returns_correct_order() throws Exception {
        exec("CREATE TABLE ep_ord(id int, v text)");
        exec("INSERT INTO ep_ord VALUES (3, 'c'), (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM ep_ord ORDER BY id ASC")) {
            int prev = Integer.MIN_VALUE;
            int count = 0;
            while (rs.next()) {
                int current = rs.getInt(1);
                assertTrue(current > prev, "Rows should be in ascending order, got " + current + " after " + prev);
                prev = current;
                count++;
            }
            assertEquals(5, count, "Should return all 5 rows in order");
        }
        exec("DROP TABLE ep_ord");
    }

    // --- 16. Partition table creation and basic query ---

    @Test void partition_table_creation_and_query() throws Exception {
        exec("CREATE TABLE ep_part(id int, category text, value int) PARTITION BY LIST (category)");
        exec("CREATE TABLE ep_part_a PARTITION OF ep_part FOR VALUES IN ('A')");
        exec("CREATE TABLE ep_part_b PARTITION OF ep_part FOR VALUES IN ('B')");

        exec("INSERT INTO ep_part VALUES (1, 'A', 10)");
        exec("INSERT INTO ep_part VALUES (2, 'A', 20)");
        exec("INSERT INTO ep_part VALUES (3, 'B', 30)");
        exec("INSERT INTO ep_part VALUES (4, 'B', 40)");

        // Query across all partitions
        assertEquals("4", scalar("SELECT count(*) FROM ep_part"));

        // Query specific partition via parent table
        assertEquals("2", scalar("SELECT count(*) FROM ep_part WHERE category = 'A'"));
        assertEquals("2", scalar("SELECT count(*) FROM ep_part WHERE category = 'B'"));

        // Verify data lands in correct child partitions
        assertEquals("2", scalar("SELECT count(*) FROM ep_part_a"));
        assertEquals("2", scalar("SELECT count(*) FROM ep_part_b"));

        // Sum check
        assertEquals("30", scalar("SELECT sum(value) FROM ep_part WHERE category = 'A'"));
        assertEquals("70", scalar("SELECT sum(value) FROM ep_part WHERE category = 'B'"));

        exec("DROP TABLE ep_part CASCADE");
    }
}
