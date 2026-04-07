package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document sections 29, 19, 30: Large schema behavior and performance sanity.
 * Tests many-table creation, wide tables, large result sets, pagination,
 * prepared statement reuse, multiple indexes, foreign key chains, views,
 * materialized views, information_schema introspection, cross-schema access,
 * deterministic ordering, and batch inserts at scale.
 */
class LargeSchemaStartupTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }
    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // --- 1. Create 50 tables in a loop, verify all accessible ---

    @Test @Timeout(30) void create_50_tables_all_accessible() throws Exception {
        for (int i = 0; i < 50; i++) {
            exec("CREATE TABLE ls_many_" + i + "(id int PRIMARY KEY, v text)");
        }
        for (int i = 0; i < 50; i++) {
            exec("INSERT INTO ls_many_" + i + " VALUES (1, 'row')");
            String val = scalar("SELECT v FROM ls_many_" + i + " WHERE id = 1");
            assertEquals("row", val, "Table ls_many_" + i + " should be accessible");
        }
        for (int i = 0; i < 50; i++) {
            exec("DROP TABLE ls_many_" + i);
        }
    }

    // --- 2. Create table with 50 columns, verify metadata ---

    @Test void wide_table_50_columns_metadata() throws Exception {
        StringBuilder sb = new StringBuilder("CREATE TABLE ls_wide(id int PRIMARY KEY");
        for (int i = 1; i <= 49; i++) {
            sb.append(", col_").append(i).append(" text");
        }
        sb.append(")");
        exec(sb.toString());

        DatabaseMetaData md = conn.getMetaData();
        int colCount = 0;
        try (ResultSet rs = md.getColumns(null, null, "ls_wide", null)) {
            while (rs.next()) {
                colCount++;
            }
        }
        assertEquals(50, colCount, "Wide table should have 50 columns");
        exec("DROP TABLE ls_wide");
    }

    // --- 3. Large result set: insert 1000 rows, SELECT all, verify count ---

    @Test @Timeout(30) void large_result_set_1000_rows() throws Exception {
        exec("CREATE TABLE ls_large(id int PRIMARY KEY, v text)");
        for (int i = 0; i < 1000; i++) {
            exec("INSERT INTO ls_large VALUES (" + i + ", 'val_" + i + "')");
        }
        int count = Integer.parseInt(scalar("SELECT count(*) FROM ls_large"));
        assertEquals(1000, count);

        // Also verify streaming through full result set
        int rowCount = 0;
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM ls_large")) {
            while (rs.next()) {
                rowCount++;
            }
        }
        assertEquals(1000, rowCount, "Should stream all 1000 rows");
        exec("DROP TABLE ls_large");
    }

    // --- 4. Pagination on indexed column: LIMIT/OFFSET over large dataset ---

    @Test @Timeout(30) void pagination_limit_offset_on_indexed_column() throws Exception {
        exec("CREATE TABLE ls_page(id int PRIMARY KEY, v text)");
        exec("CREATE INDEX ls_page_id_idx ON ls_page(id)");
        for (int i = 0; i < 200; i++) {
            exec("INSERT INTO ls_page VALUES (" + i + ", 'row_" + i + "')");
        }

        // Page through in chunks of 20
        for (int page = 0; page < 10; page++) {
            int offset = page * 20;
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT id, v FROM ls_page ORDER BY id LIMIT 20 OFFSET " + offset)) {
                int rowsInPage = 0;
                while (rs.next()) {
                    int expectedId = offset + rowsInPage;
                    assertEquals(expectedId, rs.getInt("id"),
                            "Page " + page + " row " + rowsInPage + " should have correct id");
                    assertEquals("row_" + expectedId, rs.getString("v"));
                    rowsInPage++;
                }
                assertEquals(20, rowsInPage, "Each page should have 20 rows");
            }
        }
        exec("DROP TABLE ls_page");
    }

    // --- 5. Repeated PreparedStatement execution (100 iterations) performance ---

    @Test @Timeout(30) void repeated_prepared_statement_100_iterations() throws Exception {
        exec("CREATE TABLE ls_prep(id int PRIMARY KEY, v text)");
        for (int i = 0; i < 100; i++) {
            exec("INSERT INTO ls_prep VALUES (" + i + ", 'val_" + i + "')");
        }

        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM ls_prep WHERE id = ?")) {
            for (int i = 0; i < 100; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Row " + i + " should exist");
                    assertEquals("val_" + i, rs.getString(1));
                }
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 15_000,
                "100 prepared statement executions took " + elapsed + "ms, expected < 15s");
        exec("DROP TABLE ls_prep");
    }

    // --- 6. Multiple indexes on same table ---

    @Test void multiple_indexes_on_same_table() throws Exception {
        exec("CREATE TABLE ls_midx(id int PRIMARY KEY, name text, category text, score int)");
        exec("CREATE INDEX ls_midx_name_idx ON ls_midx(name)");
        exec("CREATE INDEX ls_midx_category_idx ON ls_midx(category)");
        exec("CREATE INDEX ls_midx_score_idx ON ls_midx(score)");

        exec("INSERT INTO ls_midx VALUES (1, 'alice', 'A', 95)");
        exec("INSERT INTO ls_midx VALUES (2, 'bob', 'B', 87)");
        exec("INSERT INTO ls_midx VALUES (3, 'charlie', 'A', 92)");

        // Verify queries can use different indexed columns
        assertEquals("alice", scalar("SELECT name FROM ls_midx WHERE name = 'alice'"));
        assertEquals("2", scalar("SELECT count(*) FROM ls_midx WHERE category = 'A'"));
        assertEquals("bob", scalar("SELECT name FROM ls_midx WHERE score = 87"));

        // Verify indexes exist in metadata
        DatabaseMetaData md = conn.getMetaData();
        int indexCount = 0;
        try (ResultSet rs = md.getIndexInfo(null, null, "ls_midx", false, true)) {
            while (rs.next()) {
                if (rs.getString("INDEX_NAME") != null) {
                    indexCount++;
                }
            }
        }
        assertTrue(indexCount >= 4, "Should have at least 4 indexes (PK + 3 created), found: " + indexCount);
        exec("DROP TABLE ls_midx");
    }

    // --- 7. Foreign key chain: A -> B -> C, insert/query across chain ---

    @Test void foreign_key_chain_a_b_c() throws Exception {
        exec("CREATE TABLE ls_fk_c(id int PRIMARY KEY, label text)");
        exec("CREATE TABLE ls_fk_b(id int PRIMARY KEY, c_id int REFERENCES ls_fk_c(id), name text)");
        exec("CREATE TABLE ls_fk_a(id int PRIMARY KEY, b_id int REFERENCES ls_fk_b(id), value text)");

        exec("INSERT INTO ls_fk_c VALUES (1, 'root')");
        exec("INSERT INTO ls_fk_b VALUES (10, 1, 'middle')");
        exec("INSERT INTO ls_fk_a VALUES (100, 10, 'leaf')");

        // Query across the chain with JOINs
        String result = scalar(
                "SELECT a.value || '-' || b.name || '-' || c.label " +
                "FROM ls_fk_a a JOIN ls_fk_b b ON a.b_id = b.id JOIN ls_fk_c c ON b.c_id = c.id " +
                "WHERE a.id = 100");
        assertEquals("leaf-middle-root", result);

        // Verify FK constraint: inserting into A with invalid b_id should fail
        assertThrows(SQLException.class,
                () -> exec("INSERT INTO ls_fk_a VALUES (200, 999, 'invalid')"),
                "FK violation should raise an error");

        exec("DROP TABLE ls_fk_a");
        exec("DROP TABLE ls_fk_b");
        exec("DROP TABLE ls_fk_c");
    }

    // --- 8. View over complex query (JOIN + GROUP BY) ---

    @Test void view_over_join_group_by() throws Exception {
        exec("CREATE TABLE ls_vdept(id int PRIMARY KEY, name text)");
        exec("CREATE TABLE ls_vemp(id int PRIMARY KEY, dept_id int REFERENCES ls_vdept(id), salary int)");

        exec("INSERT INTO ls_vdept VALUES (1, 'Engineering'), (2, 'Sales')");
        exec("INSERT INTO ls_vemp VALUES (1, 1, 100), (2, 1, 120), (3, 2, 90), (4, 2, 95)");

        exec("CREATE VIEW ls_vdept_summary AS " +
             "SELECT d.name AS dept_name, count(e.id) AS emp_count, sum(e.salary) AS total_salary " +
             "FROM ls_vdept d JOIN ls_vemp e ON d.id = e.dept_id GROUP BY d.name");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT dept_name, emp_count, total_salary FROM ls_vdept_summary ORDER BY dept_name")) {
            assertTrue(rs.next());
            assertEquals("Engineering", rs.getString("dept_name"));
            assertEquals(2, rs.getInt("emp_count"));
            assertEquals(220, rs.getInt("total_salary"));
            assertTrue(rs.next());
            assertEquals("Sales", rs.getString("dept_name"));
            assertEquals(2, rs.getInt("emp_count"));
            assertEquals(185, rs.getInt("total_salary"));
            assertFalse(rs.next());
        }

        exec("DROP VIEW ls_vdept_summary");
        exec("DROP TABLE ls_vemp");
        exec("DROP TABLE ls_vdept");
    }

    // --- 9. Materialized view creation and refresh ---

    @Test void materialized_view_creation_and_refresh() throws Exception {
        exec("CREATE TABLE ls_mat_src(id int PRIMARY KEY, val int)");
        exec("INSERT INTO ls_mat_src VALUES (1, 10), (2, 20), (3, 30)");

        exec("CREATE MATERIALIZED VIEW ls_mat_view AS SELECT sum(val) AS total FROM ls_mat_src");

        assertEquals("60", scalar("SELECT total FROM ls_mat_view"));

        // Insert more data and refresh
        exec("INSERT INTO ls_mat_src VALUES (4, 40)");
        // Before refresh, materialized view still shows old data
        assertEquals("60", scalar("SELECT total FROM ls_mat_view"));

        exec("REFRESH MATERIALIZED VIEW ls_mat_view");
        assertEquals("100", scalar("SELECT total FROM ls_mat_view"));

        exec("DROP MATERIALIZED VIEW ls_mat_view");
        exec("DROP TABLE ls_mat_src");
    }

    // --- 10. information_schema.tables count matches expected ---

    @Test void information_schema_tables_count_matches() throws Exception {
        exec("CREATE TABLE ls_is_one(id int PRIMARY KEY)");
        exec("CREATE TABLE ls_is_two(id int PRIMARY KEY)");
        exec("CREATE TABLE ls_is_three(id int PRIMARY KEY)");

        int count = Integer.parseInt(scalar(
                "SELECT count(*) FROM information_schema.tables " +
                "WHERE table_schema = 'public' AND table_name LIKE 'ls_is_%'"));
        assertEquals(3, count, "information_schema should list all 3 ls_is_ tables");

        exec("DROP TABLE ls_is_three");
        int afterDrop = Integer.parseInt(scalar(
                "SELECT count(*) FROM information_schema.tables " +
                "WHERE table_schema = 'public' AND table_name LIKE 'ls_is_%'"));
        assertEquals(2, afterDrop, "After drop, count should be 2");

        exec("DROP TABLE ls_is_one");
        exec("DROP TABLE ls_is_two");
    }

    // --- 11. information_schema.columns for wide table ---

    @Test void information_schema_columns_wide_table() throws Exception {
        StringBuilder sb = new StringBuilder("CREATE TABLE ls_is_wide(id int PRIMARY KEY");
        for (int i = 1; i <= 29; i++) {
            sb.append(", col_").append(i).append(" text");
        }
        sb.append(")");
        exec(sb.toString());

        int colCount = Integer.parseInt(scalar(
                "SELECT count(*) FROM information_schema.columns " +
                "WHERE table_schema = 'public' AND table_name = 'ls_is_wide'"));
        assertEquals(30, colCount, "information_schema should list all 30 columns");

        // Verify specific column exists
        String colName = scalar(
                "SELECT column_name FROM information_schema.columns " +
                "WHERE table_schema = 'public' AND table_name = 'ls_is_wide' AND ordinal_position = 1");
        assertEquals("id", colName, "First column should be 'id'");

        exec("DROP TABLE ls_is_wide");
    }

    // --- 12. Cross-schema table access ---

    @Test void cross_schema_table_access() throws Exception {
        exec("CREATE SCHEMA ls_other");
        exec("CREATE TABLE ls_other.ls_remote(id int PRIMARY KEY, v text)");
        exec("INSERT INTO ls_other.ls_remote VALUES (1, 'cross_schema')");

        String val = scalar("SELECT v FROM ls_other.ls_remote WHERE id = 1");
        assertEquals("cross_schema", val);

        // Verify via information_schema
        String schemaName = scalar(
                "SELECT table_schema FROM information_schema.tables " +
                "WHERE table_name = 'ls_remote'");
        assertEquals("ls_other", schemaName);

        // Create table in public with same name, verify both are distinct
        exec("CREATE TABLE ls_remote(id int PRIMARY KEY, v text)");
        exec("INSERT INTO ls_remote VALUES (1, 'public_schema')");

        assertEquals("public_schema", scalar("SELECT v FROM public.ls_remote WHERE id = 1"));
        assertEquals("cross_schema", scalar("SELECT v FROM ls_other.ls_remote WHERE id = 1"));

        exec("DROP TABLE ls_remote");
        exec("DROP TABLE ls_other.ls_remote");
        exec("DROP SCHEMA ls_other");
    }

    // --- 13. Deterministic ordering: ORDER BY on indexed column is stable ---

    @Test void deterministic_ordering_on_indexed_column() throws Exception {
        exec("CREATE TABLE ls_order(id int PRIMARY KEY, category text, seq int)");
        exec("CREATE INDEX ls_order_seq_idx ON ls_order(seq)");

        // Insert rows in random order
        exec("INSERT INTO ls_order VALUES (5, 'b', 50)");
        exec("INSERT INTO ls_order VALUES (1, 'a', 10)");
        exec("INSERT INTO ls_order VALUES (3, 'a', 30)");
        exec("INSERT INTO ls_order VALUES (4, 'b', 40)");
        exec("INSERT INTO ls_order VALUES (2, 'a', 20)");

        // Run the same ORDER BY query twice and verify identical results
        int[] firstRun = new int[5];
        int[] secondRun = new int[5];

        for (int run = 0; run < 2; run++) {
            int[] target = (run == 0) ? firstRun : secondRun;
            try (Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT id FROM ls_order ORDER BY seq")) {
                int idx = 0;
                while (rs.next()) {
                    target[idx++] = rs.getInt("id");
                }
                assertEquals(5, idx);
            }
        }

        assertArrayEquals(firstRun, secondRun, "ORDER BY on indexed column should be deterministic");
        // Verify actual order is correct
        assertEquals(1, firstRun[0]);
        assertEquals(2, firstRun[1]);
        assertEquals(3, firstRun[2]);
        assertEquals(4, firstRun[3]);
        assertEquals(5, firstRun[4]);

        exec("DROP TABLE ls_order");
    }

    // --- 14. Batch insert 500 rows, verify all present ---

    @Test @Timeout(30) void batch_insert_500_rows_all_present() throws Exception {
        exec("CREATE TABLE ls_batch(id int PRIMARY KEY, v text, num int)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ls_batch VALUES (?, ?, ?)")) {
            for (int i = 0; i < 500; i++) {
                ps.setInt(1, i);
                ps.setString(2, "batch_" + i);
                ps.setInt(3, i * 3);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(500, counts.length, "Should return 500 update counts");
        }

        // Verify total count
        int total = Integer.parseInt(scalar("SELECT count(*) FROM ls_batch"));
        assertEquals(500, total);

        // Spot-check first, middle, and last rows
        assertEquals("batch_0", scalar("SELECT v FROM ls_batch WHERE id = 0"));
        assertEquals("batch_250", scalar("SELECT v FROM ls_batch WHERE id = 250"));
        assertEquals("batch_499", scalar("SELECT v FROM ls_batch WHERE id = 499"));
        assertEquals("750", scalar("SELECT num FROM ls_batch WHERE id = 250"));
        assertEquals("1497", scalar("SELECT num FROM ls_batch WHERE id = 499"));

        // Verify no gaps by checking min/max
        assertEquals("0", scalar("SELECT min(id) FROM ls_batch"));
        assertEquals("499", scalar("SELECT max(id) FROM ls_batch"));

        exec("DROP TABLE ls_batch");
    }
}
