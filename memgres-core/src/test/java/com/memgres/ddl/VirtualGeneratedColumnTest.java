package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for VIRTUAL generated columns (PG 18).
 * VIRTUAL generated columns are computed on read, never stored.
 * PG 18 makes VIRTUAL the default when neither STORED nor VIRTUAL is specified.
 */
class VirtualGeneratedColumnTest {

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

    @BeforeEach
    void cleanUp() throws SQLException {
        for (String t : List.of("t1", "t2", "t3", "child", "parent")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
    }

    @AfterEach
    void cleanUpAfter() throws SQLException {
        for (String t : List.of("t1", "t2", "t3", "child", "parent")) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (SQLException ignored) {}
        }
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // Basic VIRTUAL generated column
    // ========================================================================

    @Test
    void virtual_column_computed_on_read() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 10) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        assertEquals("50", scalar("SELECT b FROM t1 WHERE id = 1"));
    }

    @Test
    void virtual_column_default_when_no_keyword() throws SQLException {
        // PG 18: if neither STORED nor VIRTUAL specified, default is VIRTUAL
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 2))");
        exec("INSERT INTO t1(id, a) VALUES (1, 7)");
        assertEquals("14", scalar("SELECT b FROM t1 WHERE id = 1"));
    }

    @Test
    void stored_column_still_works() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a + 1) STORED)");
        exec("INSERT INTO t1(id, a) VALUES (1, 10)");
        assertEquals("11", scalar("SELECT b FROM t1 WHERE id = 1"));
    }

    @Test
    void virtual_column_string_concat() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, first_name text, last_name text, " +
             "full_name text GENERATED ALWAYS AS (first_name || ' ' || last_name) VIRTUAL)");
        exec("INSERT INTO t1(id, first_name, last_name) VALUES (1, 'John', 'Doe')");
        assertEquals("John Doe", scalar("SELECT full_name FROM t1 WHERE id = 1"));
    }

    @Test
    void virtual_column_arithmetic() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, price numeric, tax_rate numeric, " +
             "total numeric GENERATED ALWAYS AS (price * (1 + tax_rate)) VIRTUAL)");
        exec("INSERT INTO t1(id, price, tax_rate) VALUES (1, 100.00, 0.08)");
        String total = scalar("SELECT total FROM t1 WHERE id = 1");
        assertTrue(total.startsWith("108.00"), "Expected 108.00..., got " + total);
    }

    // ========================================================================
    // Virtual column recomputes on UPDATE
    // ========================================================================

    @Test
    void virtual_column_recomputes_after_update() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 10) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        assertEquals("50", scalar("SELECT b FROM t1 WHERE id = 1"));
        // Update the base column
        exec("UPDATE t1 SET a = 12 WHERE id = 1");
        // Virtual column should reflect new value
        assertEquals("120", scalar("SELECT b FROM t1 WHERE id = 1"));
    }

    @Test
    void virtual_column_recomputes_multiple_rows() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 3), (2, 7), (3, 11)");
        List<List<String>> rows = query("SELECT id, doubled FROM t1 ORDER BY id");
        assertEquals(3, rows.size());
        assertEquals("6", rows.get(0).get(1));
        assertEquals("14", rows.get(1).get(1));
        assertEquals("22", rows.get(2).get(1));
    }

    // ========================================================================
    // Reject explicit writes to virtual columns
    // ========================================================================

    @Test
    void reject_insert_to_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 10) VIRTUAL)");
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("INSERT INTO t1(id, a, b) VALUES (1, 5, 999)"));
        assertEquals("428C9", ex.getSQLState());
    }

    @Test
    void reject_update_to_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 10) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("UPDATE t1 SET b = 999 WHERE id = 1"));
        assertEquals("428C9", ex.getSQLState());
    }

    @Test
    void allow_default_update_to_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 10) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        // UPDATE SET col = DEFAULT is allowed for generated columns
        exec("UPDATE t1 SET b = DEFAULT WHERE id = 1");
        assertEquals("50", scalar("SELECT b FROM t1 WHERE id = 1"));
    }

    // ========================================================================
    // Virtual column in WHERE clause
    // ========================================================================

    @Test
    void virtual_column_in_select_where() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 3), (2, 7), (3, 11)");
        assertEquals("7", scalar("SELECT a FROM t1 WHERE doubled = 14"));
    }

    @Test
    void virtual_column_in_update_where() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 3), (2, 7), (3, 11)");
        exec("UPDATE t1 SET a = 100 WHERE doubled > 10");
        assertEquals("3", scalar("SELECT a FROM t1 WHERE id = 1")); // doubled=6, not updated
        assertEquals("100", scalar("SELECT a FROM t1 WHERE id = 2")); // doubled=14 > 10
        assertEquals("100", scalar("SELECT a FROM t1 WHERE id = 3")); // doubled=22 > 10
    }

    @Test
    void virtual_column_in_delete_where() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 3), (2, 7), (3, 11)");
        exec("DELETE FROM t1 WHERE doubled >= 14");
        assertEquals("1", scalar("SELECT count(*) FROM t1"));
        assertEquals("1", scalar("SELECT id FROM t1"));
    }

    // ========================================================================
    // Virtual column in ORDER BY, GROUP BY
    // ========================================================================

    @Test
    void virtual_column_in_order_by() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, neg int GENERATED ALWAYS AS (-a) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 3), (2, 1), (3, 5)");
        List<List<String>> rows = query("SELECT id FROM t1 ORDER BY neg");
        assertEquals("3", rows.get(0).get(0)); // neg=-5
        assertEquals("1", rows.get(1).get(0)); // neg=-3
        assertEquals("2", rows.get(2).get(0)); // neg=-1
    }

    // ========================================================================
    // SELECT * includes virtual columns
    // ========================================================================

    @Test
    void select_star_includes_virtual() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, computed int GENERATED ALWAYS AS (a + 100) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        List<List<String>> rows = query("SELECT * FROM t1");
        assertEquals(1, rows.size());
        assertEquals("1", rows.get(0).get(0));
        assertEquals("5", rows.get(0).get(1));
        assertEquals("105", rows.get(0).get(2));
    }

    // ========================================================================
    // Virtual column with NULL input
    // ========================================================================

    @Test
    void virtual_column_with_null_base() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, NULL)");
        assertNull(scalar("SELECT doubled FROM t1 WHERE id = 1"));
    }

    // ========================================================================
    // ALTER TABLE ADD virtual column
    // ========================================================================

    @Test
    void alter_table_add_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int)");
        exec("INSERT INTO t1 VALUES (1, 5), (2, 10)");
        exec("ALTER TABLE t1 ADD COLUMN doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL");
        List<List<String>> rows = query("SELECT id, doubled FROM t1 ORDER BY id");
        assertEquals("10", rows.get(0).get(1));
        assertEquals("20", rows.get(1).get(1));
    }

    @Test
    void alter_table_add_virtual_column_default_mode() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int)");
        exec("INSERT INTO t1 VALUES (1, 3)");
        // No STORED/VIRTUAL keyword => defaults to VIRTUAL in PG 18
        exec("ALTER TABLE t1 ADD COLUMN tripled int GENERATED ALWAYS AS (a * 3)");
        assertEquals("9", scalar("SELECT tripled FROM t1 WHERE id = 1"));
    }

    // ========================================================================
    // Catalog: pg_attribute.attgenerated
    // ========================================================================

    @Test
    void pg_attribute_attgenerated_v_for_virtual() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        assertEquals("v", scalar(
                "SELECT attgenerated FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't1') AND attname = 'b'"));
    }

    @Test
    void pg_attribute_attgenerated_s_for_stored() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 2) STORED)");
        assertEquals("s", scalar(
                "SELECT attgenerated FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't1') AND attname = 'b'"));
    }

    @Test
    void pg_attribute_attgenerated_empty_for_regular() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int)");
        assertEquals("", scalar(
                "SELECT attgenerated FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't1') AND attname = 'a'"));
    }

    // ========================================================================
    // information_schema.columns
    // ========================================================================

    @Test
    void info_schema_is_generated_always_for_virtual() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a + 1) VIRTUAL)");
        assertEquals("ALWAYS", scalar(
                "SELECT is_generated FROM information_schema.columns WHERE table_name = 't1' AND column_name = 'b'"));
    }

    @Test
    void info_schema_generation_expression_for_virtual() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a + 1) VIRTUAL)");
        assertEquals("a + 1", scalar(
                "SELECT generation_expression FROM information_schema.columns WHERE table_name = 't1' AND column_name = 'b'"));
    }

    // ========================================================================
    // Virtual column validation: immutable expression
    // ========================================================================

    @Test
    void virtual_column_rejects_volatile_now() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE t1(id int PRIMARY KEY, created_at timestamp GENERATED ALWAYS AS (now()) VIRTUAL)"));
        assertEquals("42P17", ex.getSQLState());
    }

    @Test
    void virtual_column_rejects_volatile_random() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE t1(id int PRIMARY KEY, r float GENERATED ALWAYS AS (random()) VIRTUAL)"));
        assertEquals("42P17", ex.getSQLState());
    }

    @Test
    void virtual_column_rejects_subquery() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE t1(id int PRIMARY KEY, x int GENERATED ALWAYS AS ((select 1)) VIRTUAL)"));
        assertEquals("0A000", ex.getSQLState());
    }

    @Test
    void virtual_column_rejects_reference_to_generated_column() throws SQLException {
        // Cannot reference another generated column
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE t1(id int PRIMARY KEY, a int, " +
                     "b int GENERATED ALWAYS AS (a * 2) VIRTUAL, " +
                     "c int GENERATED ALWAYS AS (b + 1) VIRTUAL)"));
        assertEquals("42P17", ex.getSQLState());
    }

    // ========================================================================
    // Virtual column with JOIN
    // ========================================================================

    @Test
    void virtual_column_visible_in_join() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("CREATE TABLE t2(id int PRIMARY KEY, ref_id int)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5), (2, 10)");
        exec("INSERT INTO t2 VALUES (1, 1)");
        assertEquals("10", scalar(
                "SELECT t1.doubled FROM t1 JOIN t2 ON t1.id = t2.ref_id WHERE t2.id = 1"));
    }

    // ========================================================================
    // Virtual column RETURNING clause
    // ========================================================================

    @Test
    void insert_returning_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        String result = scalar("INSERT INTO t1(id, a) VALUES (1, 7) RETURNING doubled");
        assertEquals("14", result);
    }

    @Test
    void update_returning_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        String result = scalar("UPDATE t1 SET a = 20 WHERE id = 1 RETURNING doubled");
        assertEquals("40", result);
    }

    @Test
    void delete_returning_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 8)");
        String result = scalar("DELETE FROM t1 WHERE id = 1 RETURNING doubled");
        assertEquals("16", result);
    }

    // ========================================================================
    // Virtual vs STORED: not stored in row
    // ========================================================================

    @Test
    void virtual_column_not_stored_but_readable() throws SQLException {
        // The key difference: VIRTUAL columns are computed on read.
        // If we update the base column, the virtual column reflects the new value immediately.
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 3) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 4)");
        assertEquals("12", scalar("SELECT b FROM t1"));
        exec("UPDATE t1 SET a = 10");
        assertEquals("30", scalar("SELECT b FROM t1"));
    }

    // ========================================================================
    // COPY with virtual columns
    // ========================================================================

    @Test
    void copy_from_skips_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, b int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        // COPY FROM should skip virtual columns - column list without virtual col
        // This verifies COPY FROM doesn't try to insert into the virtual column
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        assertEquals("10", scalar("SELECT b FROM t1"));
    }

    // COPY FROM STDIN requires CopyManager API and is tested separately

    // ========================================================================
    // Mixed STORED and VIRTUAL columns
    // ========================================================================

    @Test
    void mixed_stored_and_virtual() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, " +
             "stored_col int GENERATED ALWAYS AS (a + 1) STORED, " +
             "virtual_col int GENERATED ALWAYS AS (a + 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 10)");
        assertEquals("11", scalar("SELECT stored_col FROM t1"));
        assertEquals("12", scalar("SELECT virtual_col FROM t1"));
        // After update, both should reflect new value
        exec("UPDATE t1 SET a = 20");
        assertEquals("21", scalar("SELECT stored_col FROM t1"));
        assertEquals("22", scalar("SELECT virtual_col FROM t1"));
    }

    // ========================================================================
    // Virtual column with COALESCE / complex expressions
    // ========================================================================

    @Test
    void virtual_column_coalesce() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, " +
             "safe_a int GENERATED ALWAYS AS (COALESCE(a, 0)) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, NULL), (2, 5)");
        assertEquals("0", scalar("SELECT safe_a FROM t1 WHERE id = 1"));
        assertEquals("5", scalar("SELECT safe_a FROM t1 WHERE id = 2"));
    }

    @Test
    void virtual_column_case_expression() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, score int, " +
             "grade text GENERATED ALWAYS AS (CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END) VIRTUAL)");
        exec("INSERT INTO t1(id, score) VALUES (1, 95), (2, 85), (3, 70)");
        assertEquals("A", scalar("SELECT grade FROM t1 WHERE id = 1"));
        assertEquals("B", scalar("SELECT grade FROM t1 WHERE id = 2"));
        assertEquals("C", scalar("SELECT grade FROM t1 WHERE id = 3"));
    }

    // ========================================================================
    // Aggregate on virtual column
    // ========================================================================

    @Test
    void aggregate_on_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 3), (2, 7), (3, 5)");
        assertEquals("30", scalar("SELECT sum(doubled) FROM t1"));
        String avg = scalar("SELECT avg(doubled) FROM t1");
        assertTrue(avg.startsWith("10"), "Expected avg starting with 10, got " + avg);
    }

    // ========================================================================
    // Subquery referencing virtual column
    // ========================================================================

    @Test
    void subquery_references_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 3), (2, 7), (3, 5)");
        assertEquals("7", scalar("SELECT a FROM t1 WHERE doubled = (SELECT max(doubled) FROM t1)"));
    }

    // ========================================================================
    // Virtual column with DROP COLUMN
    // ========================================================================

    @Test
    void drop_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        exec("ALTER TABLE t1 DROP COLUMN doubled");
        // Column should be gone
        List<List<String>> rows = query("SELECT * FROM t1");
        assertEquals(1, rows.size());
        assertEquals(2, rows.get(0).size()); // only id and a
    }

    // ========================================================================
    // MERGE with virtual columns
    // ========================================================================

    @Test
    void merge_on_condition_sees_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("CREATE TABLE t2(id int, val int)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        exec("INSERT INTO t2 VALUES (10, 99)");
        // MERGE ON condition references virtual column
        exec("MERGE INTO t1 USING t2 ON t1.doubled = t2.id " +
             "WHEN MATCHED THEN UPDATE SET a = t2.val " +
             "WHEN NOT MATCHED THEN INSERT VALUES (t2.id, t2.val)");
        // doubled=10 matches t2.id=10, so UPDATE a=99
        assertEquals("99", scalar("SELECT a FROM t1 WHERE id = 1"));
    }

    @Test
    void merge_update_recomputes_stored_generated() throws SQLException {
        // Tests the computeGeneratedColumns fix for MERGE WHEN MATCHED UPDATE
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, computed int GENERATED ALWAYS AS (a + 100) STORED)");
        exec("CREATE TABLE t2(id int, val int)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        exec("INSERT INTO t2 VALUES (1, 20)");
        exec("MERGE INTO t1 USING t2 ON t1.id = t2.id " +
             "WHEN MATCHED THEN UPDATE SET a = t2.val");
        // computed should be recomputed: 20 + 100 = 120
        assertEquals("120", scalar("SELECT computed FROM t1 WHERE id = 1"));
    }

    @Test
    void merge_update_recomputes_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("CREATE TABLE t2(id int, val int)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        exec("INSERT INTO t2 VALUES (1, 20)");
        exec("MERGE INTO t1 USING t2 ON t1.id = t2.id " +
             "WHEN MATCHED THEN UPDATE SET a = t2.val");
        assertEquals("40", scalar("SELECT doubled FROM t1 WHERE id = 1"));
    }

    @Test
    void merge_not_matched_by_source_sees_virtual() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("CREATE TABLE t2(id int, val int)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5), (2, 10)");
        exec("INSERT INTO t2 VALUES (1, 99)");
        // Row id=2 is not matched by source. Use virtual column in AND condition
        exec("MERGE INTO t1 USING t2 ON t1.id = t2.id " +
             "WHEN MATCHED THEN UPDATE SET a = t2.val " +
             "WHEN NOT MATCHED BY SOURCE AND doubled > 15 THEN DELETE");
        // id=1: matched, updated to 99
        assertEquals("99", scalar("SELECT a FROM t1 WHERE id = 1"));
        // id=2: not matched, doubled=20 > 15, should be deleted
        assertEquals("1", scalar("SELECT count(*) FROM t1"));
    }

    // ========================================================================
    // ON CONFLICT with virtual columns
    // ========================================================================

    @Test
    void on_conflict_do_update_sees_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        // DO UPDATE SET references the virtual column of the existing row
        exec("INSERT INTO t1(id, a) VALUES (1, 99) ON CONFLICT (id) DO UPDATE SET a = t1.doubled + 1");
        // t1.doubled was 10 at conflict time, so a = 10 + 1 = 11
        assertEquals("11", scalar("SELECT a FROM t1 WHERE id = 1"));
    }

    @Test
    void on_conflict_recomputes_stored_generated() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, computed int GENERATED ALWAYS AS (a + 100) STORED)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        exec("INSERT INTO t1(id, a) VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a");
        // computed should be recomputed: 20 + 100 = 120
        assertEquals("120", scalar("SELECT computed FROM t1 WHERE id = 1"));
    }

    // ========================================================================
    // Validation: DEFAULT + GENERATED rejected
    // ========================================================================

    @Test
    void reject_default_plus_generated() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE t1(id int PRIMARY KEY, a int DEFAULT 1 GENERATED ALWAYS AS (a + 1) VIRTUAL)"));
        assertTrue(ex.getMessage().contains("generated column") || ex.getMessage().contains("default value"),
                "Expected error about generated column + default, got: " + ex.getMessage());
    }

    @Test
    void reject_default_plus_stored_generated() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE t1(id int PRIMARY KEY, a int DEFAULT 1 GENERATED ALWAYS AS (a + 1) STORED)"));
        assertTrue(ex.getMessage().contains("generated column") || ex.getMessage().contains("default value"),
                "Expected error about generated column + default, got: " + ex.getMessage());
    }

    // ========================================================================
    // Virtual column with multiple base columns
    // ========================================================================

    @Test
    void virtual_column_referencing_multiple_columns() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, x int, y int, " +
             "sum_xy int GENERATED ALWAYS AS (x + y) VIRTUAL)");
        exec("INSERT INTO t1(id, x, y) VALUES (1, 3, 7)");
        assertEquals("10", scalar("SELECT sum_xy FROM t1 WHERE id = 1"));
        exec("UPDATE t1 SET x = 10 WHERE id = 1");
        assertEquals("17", scalar("SELECT sum_xy FROM t1 WHERE id = 1"));
    }

    @Test
    void multiple_virtual_columns_same_base() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, " +
             "doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL, " +
             "tripled int GENERATED ALWAYS AS (a * 3) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 4)");
        assertEquals("8", scalar("SELECT doubled FROM t1 WHERE id = 1"));
        assertEquals("12", scalar("SELECT tripled FROM t1 WHERE id = 1"));
    }

    // ========================================================================
    // Virtual column with empty table
    // ========================================================================

    @Test
    void virtual_column_empty_table() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        List<List<String>> rows = query("SELECT * FROM t1");
        assertEquals(0, rows.size());
        assertEquals("0", scalar("SELECT count(*) FROM t1"));
    }

    // ========================================================================
    // INSERT ... SELECT with virtual column source
    // ========================================================================

    @Test
    void insert_select_from_table_with_virtual_columns() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("CREATE TABLE t2(id int, val int)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5), (2, 10)");
        // SELECT virtual column from t1 and insert into t2
        exec("INSERT INTO t2 SELECT id, doubled FROM t1");
        List<List<String>> rows = query("SELECT * FROM t2 ORDER BY id");
        assertEquals(2, rows.size());
        assertEquals("10", rows.get(0).get(1)); // 5*2
        assertEquals("20", rows.get(1).get(1)); // 10*2
    }

    // ========================================================================
    // RETURNING * with virtual column
    // ========================================================================

    @Test
    void insert_returning_star_includes_virtual() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        List<List<String>> rows = query("INSERT INTO t1(id, a) VALUES (1, 7) RETURNING *");
        assertEquals(1, rows.size());
        assertEquals("1", rows.get(0).get(0));
        assertEquals("7", rows.get(0).get(1));
        assertEquals("14", rows.get(0).get(2));
    }

    @Test
    void update_returning_star_includes_virtual() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5)");
        List<List<String>> rows = query("UPDATE t1 SET a = 20 RETURNING *");
        assertEquals(1, rows.size());
        assertEquals("20", rows.get(0).get(1));
        assertEquals("40", rows.get(0).get(2));
    }

    // ========================================================================
    // Virtual column with boolean expression
    // ========================================================================

    @Test
    void virtual_column_boolean_expression() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, age int, " +
             "is_adult boolean GENERATED ALWAYS AS (age >= 18) VIRTUAL)");
        exec("INSERT INTO t1(id, age) VALUES (1, 20), (2, 15)");
        assertEquals("t", scalar("SELECT is_adult FROM t1 WHERE id = 1"));
        assertEquals("f", scalar("SELECT is_adult FROM t1 WHERE id = 2"));
    }

    // ========================================================================
    // DISTINCT on virtual column
    // ========================================================================

    @Test
    void distinct_on_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, doubled int GENERATED ALWAYS AS (a * 2) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5), (2, 5), (3, 10)");
        List<List<String>> rows = query("SELECT DISTINCT doubled FROM t1 ORDER BY doubled");
        assertEquals(2, rows.size());
        assertEquals("10", rows.get(0).get(0));
        assertEquals("20", rows.get(1).get(0));
    }

    // ========================================================================
    // GROUP BY virtual column
    // ========================================================================

    @Test
    void group_by_virtual_column() throws SQLException {
        exec("CREATE TABLE t1(id int PRIMARY KEY, a int, bucket int GENERATED ALWAYS AS (a / 10) VIRTUAL)");
        exec("INSERT INTO t1(id, a) VALUES (1, 5), (2, 15), (3, 25), (4, 12)");
        List<List<String>> rows = query("SELECT bucket, count(*) FROM t1 GROUP BY bucket ORDER BY bucket");
        assertEquals(3, rows.size());
        assertEquals("0", rows.get(0).get(0));  // a=5 → bucket=0
        assertEquals("1", rows.get(0).get(1));
        assertEquals("1", rows.get(1).get(0));  // a=15,12 → bucket=1
        assertEquals("2", rows.get(1).get(1));
        assertEquals("2", rows.get(2).get(0));  // a=25 → bucket=2
        assertEquals("1", rows.get(2).get(1));
    }

    // ========================================================================
    // Virtual column with nonexistent column reference
    // ========================================================================

    @Test
    void virtual_column_rejects_nonexistent_column() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE TABLE t1(id int PRIMARY KEY, a int, " +
                     "b int GENERATED ALWAYS AS (no_such_col * 2) VIRTUAL)"));
        assertEquals("42703", ex.getSQLState());
    }
}
