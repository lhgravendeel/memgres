package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document section 5 (Java/JDBC): Batch execution behavior.
 * Tests Statement and PreparedStatement batch operations including addBatch,
 * executeBatch, clearBatch, row counts, error handling, generated keys,
 * and performance with large batches.
 */
class BatchExecutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // --- 1. Basic addBatch/executeBatch with INSERT statements ---

    @Test void statement_batch_insert_basic() throws Exception {
        exec("CREATE TABLE bat_basic(id int PRIMARY KEY, v text)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("INSERT INTO bat_basic VALUES (1, 'a')");
            s.addBatch("INSERT INTO bat_basic VALUES (2, 'b')");
            s.addBatch("INSERT INTO bat_basic VALUES (3, 'c')");
            int[] counts = s.executeBatch();
            assertEquals(3, counts.length);
            for (int c : counts) {
                assertTrue(c == 1 || c == Statement.SUCCESS_NO_INFO,
                        "Each insert should affect 1 row or report SUCCESS_NO_INFO");
            }
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM bat_basic")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
        exec("DROP TABLE bat_basic");
    }

    // --- 2. Batch insert returning row counts array ---

    @Test void batch_insert_row_counts() throws Exception {
        exec("CREATE TABLE bat_counts(id int PRIMARY KEY, v text)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("INSERT INTO bat_counts VALUES (1, 'x')");
            s.addBatch("INSERT INTO bat_counts VALUES (2, 'y')");
            s.addBatch("INSERT INTO bat_counts VALUES (3, 'z')");
            s.addBatch("INSERT INTO bat_counts VALUES (4, 'w')");
            s.addBatch("INSERT INTO bat_counts VALUES (5, 'v')");
            int[] counts = s.executeBatch();
            assertEquals(5, counts.length, "Should return one count per batched statement");
            for (int i = 0; i < counts.length; i++) {
                assertTrue(counts[i] == 1 || counts[i] == Statement.SUCCESS_NO_INFO,
                        "Row count at index " + i + " should be 1 or SUCCESS_NO_INFO, was: " + counts[i]);
            }
        }
        exec("DROP TABLE bat_counts");
    }

    // --- 3. Large batch insert (100+ rows) ---

    @Test void large_batch_insert_100_rows() throws Exception {
        exec("CREATE TABLE bat_large(id int PRIMARY KEY, v text)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO bat_large VALUES (?, ?)")) {
            for (int i = 0; i < 150; i++) {
                ps.setInt(1, i);
                ps.setString(2, "row_" + i);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(150, counts.length);
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM bat_large")) {
            assertTrue(rs.next());
            assertEquals(150, rs.getInt(1));
        }
        // Verify first and last rows
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT v FROM bat_large WHERE id = 0")) {
            assertTrue(rs.next());
            assertEquals("row_0", rs.getString(1));
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT v FROM bat_large WHERE id = 149")) {
            assertTrue(rs.next());
            assertEquals("row_149", rs.getString(1));
        }
        exec("DROP TABLE bat_large");
    }

    // --- 4. Batch with duplicate key violation - partial failure ---

    @Test void batch_duplicate_key_throws_batch_update_exception() throws Exception {
        exec("CREATE TABLE bat_dup(id int PRIMARY KEY, v text)");
        exec("INSERT INTO bat_dup VALUES (5, 'existing')");
        try (Statement s = conn.createStatement()) {
            s.addBatch("INSERT INTO bat_dup VALUES (1, 'ok')");
            s.addBatch("INSERT INTO bat_dup VALUES (5, 'conflict')"); // duplicate
            s.addBatch("INSERT INTO bat_dup VALUES (10, 'also_ok')");
            BatchUpdateException ex = assertThrows(BatchUpdateException.class, s::executeBatch);
            int[] updateCounts = ex.getUpdateCounts();
            assertNotNull(updateCounts, "Update counts should be available from BatchUpdateException");
            assertTrue(updateCounts.length > 0, "Should have at least some update counts");
        }
        exec("DROP TABLE bat_dup");
    }

    @Test void prepared_batch_duplicate_key_throws_batch_update_exception() throws Exception {
        exec("CREATE TABLE bat_pdup(id int PRIMARY KEY)");
        exec("INSERT INTO bat_pdup VALUES (5)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO bat_pdup VALUES (?)")) {
            ps.setInt(1, 1);
            ps.addBatch();
            ps.setInt(1, 5); // duplicate
            ps.addBatch();
            ps.setInt(1, 10);
            ps.addBatch();
            assertThrows(BatchUpdateException.class, ps::executeBatch);
        }
        exec("DROP TABLE bat_pdup");
    }

    // --- 5. Batch UPDATE statements ---

    @Test void statement_batch_update() throws Exception {
        exec("CREATE TABLE bat_upd(id int PRIMARY KEY, v int)");
        exec("INSERT INTO bat_upd VALUES (1, 10), (2, 20), (3, 30)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("UPDATE bat_upd SET v = v + 1 WHERE id = 1");
            s.addBatch("UPDATE bat_upd SET v = v + 1 WHERE id = 2");
            s.addBatch("UPDATE bat_upd SET v = v + 1 WHERE id = 3");
            int[] counts = s.executeBatch();
            assertEquals(3, counts.length);
            for (int c : counts) {
                assertTrue(c == 1 || c == Statement.SUCCESS_NO_INFO,
                        "Each update should affect 1 row");
            }
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT v FROM bat_upd ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(11, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(21, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(31, rs.getInt(1));
        }
        exec("DROP TABLE bat_upd");
    }

    @Test void prepared_batch_update() throws Exception {
        exec("CREATE TABLE bat_pupd(id int PRIMARY KEY, v int)");
        exec("INSERT INTO bat_pupd VALUES (1, 100), (2, 200), (3, 300)");
        try (PreparedStatement ps = conn.prepareStatement("UPDATE bat_pupd SET v = ? WHERE id = ?")) {
            ps.setInt(1, 111);
            ps.setInt(2, 1);
            ps.addBatch();
            ps.setInt(1, 222);
            ps.setInt(2, 2);
            ps.addBatch();
            ps.setInt(1, 333);
            ps.setInt(2, 3);
            ps.addBatch();
            int[] counts = ps.executeBatch();
            assertEquals(3, counts.length);
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT v FROM bat_pupd ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(111, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(222, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(333, rs.getInt(1));
        }
        exec("DROP TABLE bat_pupd");
    }

    // --- 6. Batch DELETE statements ---

    @Test void statement_batch_delete() throws Exception {
        exec("CREATE TABLE bat_del(id int PRIMARY KEY, v text)");
        exec("INSERT INTO bat_del VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')");
        try (Statement s = conn.createStatement()) {
            s.addBatch("DELETE FROM bat_del WHERE id = 1");
            s.addBatch("DELETE FROM bat_del WHERE id = 3");
            int[] counts = s.executeBatch();
            assertEquals(2, counts.length);
            for (int c : counts) {
                assertTrue(c == 1 || c == Statement.SUCCESS_NO_INFO,
                        "Each delete should affect 1 row");
            }
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id FROM bat_del ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("DROP TABLE bat_del");
    }

    @Test void prepared_batch_delete() throws Exception {
        exec("CREATE TABLE bat_pdel(id int PRIMARY KEY)");
        exec("INSERT INTO bat_pdel VALUES (1),(2),(3),(4),(5)");
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM bat_pdel WHERE id = ?")) {
            ps.setInt(1, 2);
            ps.addBatch();
            ps.setInt(1, 4);
            ps.addBatch();
            int[] counts = ps.executeBatch();
            assertEquals(2, counts.length);
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT count(*) FROM bat_pdel")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
        exec("DROP TABLE bat_pdel");
    }

    // --- 7. Batch mixed DML (INSERT then UPDATE) ---

    @Test void statement_batch_mixed_dml() throws Exception {
        exec("CREATE TABLE bat_mix(id int PRIMARY KEY, v int)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("INSERT INTO bat_mix VALUES (1, 10)");
            s.addBatch("INSERT INTO bat_mix VALUES (2, 20)");
            s.addBatch("UPDATE bat_mix SET v = 99 WHERE id = 1");
            s.addBatch("INSERT INTO bat_mix VALUES (3, 30)");
            s.addBatch("DELETE FROM bat_mix WHERE id = 2");
            int[] counts = s.executeBatch();
            assertEquals(5, counts.length);
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, v FROM bat_mix ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(99, rs.getInt("v"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals(30, rs.getInt("v"));
            assertFalse(rs.next(), "Row with id=2 should have been deleted");
        }
        exec("DROP TABLE bat_mix");
    }

    // --- 8. PreparedStatement batch with parameter binding ---

    @Test void prepared_batch_parameter_binding() throws Exception {
        exec("CREATE TABLE bat_bind(id int PRIMARY KEY, name text, score int)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO bat_bind VALUES (?, ?, ?)")) {
            ps.setInt(1, 1);
            ps.setString(2, "alice");
            ps.setInt(3, 95);
            ps.addBatch();

            ps.setInt(1, 2);
            ps.setString(2, "bob");
            ps.setInt(3, 87);
            ps.addBatch();

            ps.setInt(1, 3);
            ps.setString(2, "charlie");
            ps.setInt(3, 92);
            ps.addBatch();

            int[] counts = ps.executeBatch();
            assertEquals(3, counts.length);
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT name, score FROM bat_bind ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("alice", rs.getString("name"));
            assertEquals(95, rs.getInt("score"));
            assertTrue(rs.next());
            assertEquals("bob", rs.getString("name"));
            assertEquals(87, rs.getInt("score"));
            assertTrue(rs.next());
            assertEquals("charlie", rs.getString("name"));
            assertEquals(92, rs.getInt("score"));
        }
        exec("DROP TABLE bat_bind");
    }

    // --- 9. PreparedStatement batch with null values ---

    @Test void prepared_batch_with_null_values() throws Exception {
        exec("CREATE TABLE bat_null(id int PRIMARY KEY, name text, score int)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO bat_null VALUES (?, ?, ?)")) {
            // Row with all non-null values
            ps.setInt(1, 1);
            ps.setString(2, "present");
            ps.setInt(3, 100);
            ps.addBatch();

            // Row with null name
            ps.setInt(1, 2);
            ps.setNull(2, Types.VARCHAR);
            ps.setInt(3, 80);
            ps.addBatch();

            // Row with null score
            ps.setInt(1, 3);
            ps.setString(2, "partial");
            ps.setNull(3, Types.INTEGER);
            ps.addBatch();

            // Row with both nullable columns null
            ps.setInt(1, 4);
            ps.setNull(2, Types.VARCHAR);
            ps.setNull(3, Types.INTEGER);
            ps.addBatch();

            int[] counts = ps.executeBatch();
            assertEquals(4, counts.length);
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, name, score FROM bat_null ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("present", rs.getString("name"));
            assertEquals(100, rs.getInt("score"));

            assertTrue(rs.next());
            assertNull(rs.getObject("name"));
            assertEquals(80, rs.getInt("score"));

            assertTrue(rs.next());
            assertEquals("partial", rs.getString("name"));
            assertNull(rs.getObject("score"));

            assertTrue(rs.next());
            assertNull(rs.getObject("name"));
            assertNull(rs.getObject("score"));
        }
        exec("DROP TABLE bat_null");
    }

    // --- 10. Batch with generated keys (serial column) ---

    @Test void batch_insert_with_generated_keys_serial() throws Exception {
        exec("CREATE TABLE bat_gen(id serial PRIMARY KEY, v text)");
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO bat_gen(v) VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, "first");
            ps.addBatch();
            ps.setString(1, "second");
            ps.addBatch();
            ps.setString(1, "third");
            ps.addBatch();
            int[] counts = ps.executeBatch();
            assertEquals(3, counts.length);

            try (ResultSet keys = ps.getGeneratedKeys()) {
                int keyCount = 0;
                while (keys.next()) {
                    keyCount++;
                    int generatedId = keys.getInt(1);
                    assertTrue(generatedId > 0, "Generated key should be positive");
                }
                // Some drivers may not return keys for batch; at minimum verify no error
                // If keys are returned, verify correct count
                if (keyCount > 0) {
                    assertEquals(3, keyCount, "Should return a key for each batch row");
                }
            }
        }
        // Verify all rows were actually inserted with sequential ids
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id, v FROM bat_gen ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("first", rs.getString("v"));
            int firstId = rs.getInt("id");
            assertTrue(rs.next());
            assertEquals("second", rs.getString("v"));
            assertEquals(firstId + 1, rs.getInt("id"));
            assertTrue(rs.next());
            assertEquals("third", rs.getString("v"));
            assertEquals(firstId + 2, rs.getInt("id"));
        }
        exec("DROP TABLE bat_gen");
    }

    // --- 11. Empty batch execution ---

    @Test void empty_batch_returns_empty_array() throws Exception {
        try (Statement s = conn.createStatement()) {
            int[] counts = s.executeBatch();
            assertEquals(0, counts.length, "Empty batch should return zero-length array");
        }
    }

    @Test void prepared_empty_batch_returns_empty_array() throws Exception {
        exec("CREATE TABLE bat_empty(id int)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO bat_empty VALUES (?)")) {
            int[] counts = ps.executeBatch();
            assertEquals(0, counts.length, "Empty prepared batch should return zero-length array");
        }
        exec("DROP TABLE bat_empty");
    }

    // --- 12. Batch execution after clearBatch ---

    @Test void clear_batch_discards_pending_statements() throws Exception {
        exec("CREATE TABLE bat_clear(id int PRIMARY KEY)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("INSERT INTO bat_clear VALUES (1)");
            s.addBatch("INSERT INTO bat_clear VALUES (2)");
            s.addBatch("INSERT INTO bat_clear VALUES (3)");
            s.clearBatch();
            int[] counts = s.executeBatch();
            assertEquals(0, counts.length, "After clearBatch, executeBatch should return empty array");
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM bat_clear")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "No rows should have been inserted after clearBatch");
        }
        exec("DROP TABLE bat_clear");
    }

    @Test void prepared_clear_batch_discards_pending() throws Exception {
        exec("CREATE TABLE bat_pclear(id int PRIMARY KEY)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO bat_pclear VALUES (?)")) {
            ps.setInt(1, 1);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.addBatch();
            ps.clearBatch();
            int[] counts = ps.executeBatch();
            assertEquals(0, counts.length, "After clearBatch, executeBatch should return empty array");
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM bat_pclear")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
        exec("DROP TABLE bat_pclear");
    }

    @Test void clear_batch_then_add_new_batch() throws Exception {
        exec("CREATE TABLE bat_clearadd(id int PRIMARY KEY)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("INSERT INTO bat_clearadd VALUES (1)");
            s.addBatch("INSERT INTO bat_clearadd VALUES (2)");
            s.clearBatch();
            // Add new batch after clear
            s.addBatch("INSERT INTO bat_clearadd VALUES (10)");
            s.addBatch("INSERT INTO bat_clearadd VALUES (20)");
            int[] counts = s.executeBatch();
            assertEquals(2, counts.length, "Should only execute the post-clear batch");
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT id FROM bat_clearadd ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(20, rs.getInt(1));
            assertFalse(rs.next());
        }
        exec("DROP TABLE bat_clearadd");
    }

    // --- 13. executeBatch row count values (SUCCESS_NO_INFO handling) ---

    @Test void batch_row_counts_are_valid_values() throws Exception {
        exec("CREATE TABLE bat_rc(id int PRIMARY KEY, v int)");
        exec("INSERT INTO bat_rc VALUES (1, 10), (2, 20), (3, 30)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("UPDATE bat_rc SET v = v + 1 WHERE id = 1");
            s.addBatch("UPDATE bat_rc SET v = v + 1 WHERE id IN (2, 3)");
            s.addBatch("UPDATE bat_rc SET v = v + 1 WHERE id = 999"); // no match
            int[] counts = s.executeBatch();
            assertEquals(3, counts.length);
            for (int c : counts) {
                assertTrue(c >= 0 || c == Statement.SUCCESS_NO_INFO || c == Statement.EXECUTE_FAILED,
                        "Row count should be >= 0, SUCCESS_NO_INFO, or EXECUTE_FAILED, was: " + c);
            }
            // If real counts are reported:
            if (counts[0] >= 0) {
                assertEquals(1, counts[0], "First update should affect 1 row");
            }
            if (counts[1] >= 0) {
                assertEquals(2, counts[1], "Second update should affect 2 rows");
            }
            if (counts[2] >= 0) {
                assertEquals(0, counts[2], "Third update should affect 0 rows");
            }
        }
        exec("DROP TABLE bat_rc");
    }

    @Test void batch_update_affecting_multiple_rows() throws Exception {
        exec("CREATE TABLE bat_multi(category text, v int)");
        exec("INSERT INTO bat_multi VALUES ('a', 1),('a', 2),('b', 3),('b', 4),('b', 5)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("UPDATE bat_multi SET v = v * 10 WHERE category = 'a'");
            s.addBatch("UPDATE bat_multi SET v = v * 10 WHERE category = 'b'");
            int[] counts = s.executeBatch();
            assertEquals(2, counts.length);
            if (counts[0] >= 0) assertEquals(2, counts[0], "Category 'a' has 2 rows");
            if (counts[1] >= 0) assertEquals(3, counts[1], "Category 'b' has 3 rows");
        }
        exec("DROP TABLE bat_multi");
    }

    // --- 14. Very large batch (1000 rows) performance sanity ---

    @Test void very_large_batch_1000_rows() throws Exception {
        exec("CREATE TABLE bat_perf(id int PRIMARY KEY, val text, num int)");
        long start = System.currentTimeMillis();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO bat_perf VALUES (?, ?, ?)")) {
            for (int i = 0; i < 1000; i++) {
                ps.setInt(1, i);
                ps.setString(2, "value_" + i);
                ps.setInt(3, i * 10);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(1000, counts.length, "Should return 1000 update counts");
        }
        long elapsed = System.currentTimeMillis() - start;
        // Sanity check: batch of 1000 inserts should complete within 30 seconds
        assertTrue(elapsed < 30_000,
                "1000-row batch took " + elapsed + "ms, expected < 30s");

        // Verify row count
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM bat_perf")) {
            assertTrue(rs.next());
            assertEquals(1000, rs.getInt(1));
        }
        // Spot-check some values
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT val, num FROM bat_perf WHERE id = 500")) {
            assertTrue(rs.next());
            assertEquals("value_500", rs.getString("val"));
            assertEquals(5000, rs.getInt("num"));
        }
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT val, num FROM bat_perf WHERE id = 999")) {
            assertTrue(rs.next());
            assertEquals("value_999", rs.getString("val"));
            assertEquals(9990, rs.getInt("num"));
        }
        exec("DROP TABLE bat_perf");
    }

    // --- Additional edge cases ---

    @Test void batch_delete_affecting_zero_rows() throws Exception {
        exec("CREATE TABLE bat_delz(id int PRIMARY KEY)");
        exec("INSERT INTO bat_delz VALUES (1),(2),(3)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("DELETE FROM bat_delz WHERE id = 999");
            s.addBatch("DELETE FROM bat_delz WHERE id = 888");
            int[] counts = s.executeBatch();
            assertEquals(2, counts.length);
            if (counts[0] >= 0) assertEquals(0, counts[0]);
            if (counts[1] >= 0) assertEquals(0, counts[1]);
        }
        // Original rows untouched
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM bat_delz")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
        exec("DROP TABLE bat_delz");
    }

    @Test void batch_execute_clears_batch_implicitly() throws Exception {
        exec("CREATE TABLE bat_impl(id int PRIMARY KEY)");
        try (Statement s = conn.createStatement()) {
            s.addBatch("INSERT INTO bat_impl VALUES (1)");
            s.addBatch("INSERT INTO bat_impl VALUES (2)");
            int[] first = s.executeBatch();
            assertEquals(2, first.length);
            // After executeBatch, the batch should be cleared per JDBC spec
            int[] second = s.executeBatch();
            assertEquals(0, second.length, "Batch should be cleared after executeBatch");
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM bat_impl")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
        exec("DROP TABLE bat_impl");
    }

    @Test void prepared_batch_reuse_after_execute() throws Exception {
        exec("CREATE TABLE bat_reuse(id int PRIMARY KEY, v text)");
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO bat_reuse VALUES (?, ?)")) {
            // First batch
            ps.setInt(1, 1);
            ps.setString(2, "first");
            ps.addBatch();
            ps.setInt(1, 2);
            ps.setString(2, "second");
            ps.addBatch();
            int[] counts1 = ps.executeBatch();
            assertEquals(2, counts1.length);

            // Second batch using same PreparedStatement
            ps.setInt(1, 3);
            ps.setString(2, "third");
            ps.addBatch();
            ps.setInt(1, 4);
            ps.setString(2, "fourth");
            ps.addBatch();
            int[] counts2 = ps.executeBatch();
            assertEquals(2, counts2.length);
        }
        try (ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM bat_reuse")) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        }
        exec("DROP TABLE bat_reuse");
    }
}
