package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 29 Memgres-vs-Annotation failures from merge-returning.sql.
 *
 * Failure categories:
 * - MERGE RETURNING * / specific columns / expressions (Stmts 16,21,26,54,82,88,114,155)
 * - WHEN NOT MATCHED BY SOURCE: UPDATE/DELETE/DO NOTHING (Stmts 32,38,44,49,70,76,85,99,105,109)
 * - merge_action() in RETURNING clause (Stmts 54,59,123,132)
 * - MERGE with CTE/VALUES/subquery sources (Stmts 93,118)
 * - Multiple WHEN clauses combined (Stmts 76,132)
 * - MERGE as CTE source / writable CTE (Stmts 93,118)
 * - Error handling: non-existent column (Stmt 127), CHECK constraint (Stmts 146,149)
 * - Error message mismatch for MERGE in subquery (Stmts 64,79)
 */
class MergeReturningCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS mr_test CASCADE");
            s.execute("CREATE SCHEMA mr_test");
            s.execute("SET search_path = mr_test, public");

            s.execute("CREATE TABLE mr_target (id integer PRIMARY KEY, val text, score integer)");
            s.execute("CREATE TABLE mr_source (id integer PRIMARY KEY, val text, score integer)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS mr_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    /**
     * Helper: reset mr_target and mr_source with given data.
     */
    private void resetTables(String[] targetRows, String[] sourceRows) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM mr_target");
            s.execute("DELETE FROM mr_source");
            if (targetRows != null) {
                for (String row : targetRows) {
                    s.execute("INSERT INTO mr_target VALUES (" + row + ")");
                }
            }
            if (sourceRows != null) {
                for (String row : sourceRows) {
                    s.execute("INSERT INTO mr_source VALUES (" + row + ")");
                }
            }
        }
    }

    // ========================================================================
    // Stmt 16: MERGE RETURNING * — IllegalStateException
    // PG: (id,val,score,id,val,score) [1|updated|15|1|updated|15], [2|new|20|2|new|20]
    // ========================================================================
    @Test
    void testStmt16_mergeReturningStar() throws SQLException {
        resetTables(
                new String[]{"1, 'old', 10"},
                new String[]{"1, 'updated', 15", "2, 'new', 20"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                "WHEN NOT MATCHED THEN INSERT (id, val, score) VALUES (s.id, s.val, s.score) " +
                "RETURNING *";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next(), "Expected first row");
            assertEquals(1, rs.getInt(1));
            assertEquals("updated", rs.getString(2));
            assertEquals(15, rs.getInt(3));

            assertTrue(rs.next(), "Expected second row");
            assertEquals(2, rs.getInt(1));
            assertEquals("new", rs.getString(2));
            assertEquals(20, rs.getInt(3));

            assertFalse(rs.next(), "Expected only 2 rows");
        }
    }

    // ========================================================================
    // Stmt 21: MERGE RETURNING specific columns — IllegalStateException
    // PG: (id,val) [1|new]
    // ========================================================================
    @Test
    void testStmt21_mergeReturningSpecificColumns() throws SQLException {
        resetTables(
                new String[]{"1, 'old', 10"},
                new String[]{"1, 'new', 99"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                "RETURNING t.id, t.val";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next(), "Expected one row");
            assertEquals(1, rs.getInt("id"));
            assertEquals("new", rs.getString("val"));
            assertFalse(rs.next(), "Expected only 1 row");
        }
    }

    // ========================================================================
    // Stmt 26: MERGE RETURNING expression — IllegalStateException
    // PG: (id,doubled) [1|40]
    // ========================================================================
    @Test
    void testStmt26_mergeReturningExpression() throws SQLException {
        resetTables(
                new String[]{"1, 'a', 10"},
                new String[]{"1, 'b', 20"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                "RETURNING t.id, t.score * 2 AS doubled";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next(), "Expected one row");
            assertEquals(1, rs.getInt("id"));
            assertEquals(40, rs.getInt("doubled"));
            assertFalse(rs.next(), "Expected only 1 row");
        }
    }

    // ========================================================================
    // Stmt 32: WHEN NOT MATCHED BY SOURCE — UPDATE
    // PG: [1|match-src|15], [2|no-source|20], [3|no-source|30]
    // Memgres: [1|updated|15], [2|no-source|20], [3|new|30]
    // ========================================================================
    @Test
    void testStmt32_whenNotMatchedBySourceUpdate() throws SQLException {
        resetTables(
                new String[]{"1, 'match', 10", "2, 'orphan', 20", "3, 'orphan2', 30"},
                new String[]{"1, 'match-src', 15"}
        );

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING mr_source s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                    "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET val = 'no-source'"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM mr_target ORDER BY id")) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("match-src", rs.getString("val"));
            assertEquals(15, rs.getInt("score"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("no-source", rs.getString("val"));
            assertEquals(20, rs.getInt("score"));

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("no-source", rs.getString("val"));
            assertEquals(30, rs.getInt("score"));

            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // Stmt 38: WHEN NOT MATCHED BY SOURCE — DELETE
    // PG: (id,val) [1|src] (1 row)
    // Memgres: [1|updated], [3|new] (2 rows)
    // ========================================================================
    @Test
    void testStmt38_whenNotMatchedBySourceDelete() throws SQLException {
        resetTables(
                new String[]{"1, 'match', 10", "2, 'orphan', 20"},
                new String[]{"1, 'src', 15"}
        );

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING mr_source s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET val = s.val " +
                    "WHEN NOT MATCHED BY SOURCE THEN DELETE"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM mr_target ORDER BY id")) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("src", rs.getString("val"));

            assertFalse(rs.next(), "Expected only 1 row (orphan should be deleted)");
        }
    }

    // ========================================================================
    // Stmt 44: WHEN NOT MATCHED BY SOURCE — DO NOTHING
    // PG: [1|src], [2|orphan]
    // Memgres: [1|updated], [3|new]
    // ========================================================================
    @Test
    void testStmt44_whenNotMatchedBySourceDoNothing() throws SQLException {
        resetTables(
                new String[]{"1, 'match', 10", "2, 'orphan', 20"},
                new String[]{"1, 'src', 15"}
        );

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING mr_source s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET val = s.val " +
                    "WHEN NOT MATCHED BY SOURCE THEN DO NOTHING"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM mr_target ORDER BY id")) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("src", rs.getString("val"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("orphan", rs.getString("val"));

            assertFalse(rs.next(), "Expected exactly 2 rows");
        }
    }

    // ========================================================================
    // Stmt 49: WHEN NOT MATCHED BY TARGET (explicit form)
    // PG: [1|new|10] (1 row)
    // Memgres: [1|updated|15], [3|new|30] (2 rows)
    // ========================================================================
    @Test
    void testStmt49_whenNotMatchedByTarget() throws SQLException {
        resetTables(
                null,
                new String[]{"1, 'new', 10"}
        );

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING mr_source s ON t.id = s.id " +
                    "WHEN NOT MATCHED BY TARGET THEN " +
                    "INSERT (id, val, score) VALUES (s.id, s.val, s.score)"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM mr_target")) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("new", rs.getString("val"));
            assertEquals(10, rs.getInt("score"));

            assertFalse(rs.next(), "Expected only 1 row");
        }
    }

    // ========================================================================
    // Stmt 54: merge_action() in RETURNING — IllegalStateException
    // PG: (id,action) [1|UPDATE], [2|INSERT]
    // ========================================================================
    @Test
    void testStmt54_mergeActionInReturning() throws SQLException {
        resetTables(
                new String[]{"1, 'old', 10"},
                new String[]{"1, 'updated', 15", "2, 'new', 20"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                "WHEN NOT MATCHED THEN INSERT (id, val, score) VALUES (s.id, s.val, s.score) " +
                "RETURNING t.id, merge_action() AS action";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("UPDATE", rs.getString("action"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("INSERT", rs.getString("action"));

            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // Stmt 59: merge_action() with DELETE — IllegalStateException
    // PG: (id,action) [1|UPDATE], [2|DELETE]
    // ========================================================================
    @Test
    void testStmt59_mergeActionWithDelete() throws SQLException {
        resetTables(
                new String[]{"1, 'keep', 10", "2, 'remove', 20"},
                new String[]{"1, 'keep-src', 15"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val " +
                "WHEN NOT MATCHED BY SOURCE THEN DELETE " +
                "RETURNING t.id, merge_action() AS action";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("UPDATE", rs.getString("action"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("DELETE", rs.getString("action"));

            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // Stmt 64: MERGE in subquery — error message mismatch
    // PG: syntax error
    // Memgres: "Expected RIGHT_PAREN but found KEYWORD" (should contain "syntax error")
    // ========================================================================
    @Test
    void testStmt64_mergeInSubquerySyntaxError() throws SQLException {
        resetTables(
                new String[]{"1, 'a', 10"},
                new String[]{"1, 'b', 20"}
        );

        String sql =
                "SELECT count(*) AS cnt FROM ( " +
                "MERGE INTO mr_target t USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN DO NOTHING RETURNING * ) sub";

        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> s.executeQuery(sql));
            String msg = ex.getMessage().toLowerCase();
            assertTrue(msg.contains("syntax error") || msg.contains("syntax"),
                    "Error should mention 'syntax error', got: " + ex.getMessage());
        }
    }

    // ========================================================================
    // Stmt 70: VALUES as source — count mismatch
    // PG: cnt=2
    // Memgres: cnt=3
    // ========================================================================
    @Test
    void testStmt70_mergeWithValuesSource() throws SQLException {
        resetTables(null, null);

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING (VALUES (1, 'a', 10), (2, 'b', 20)) AS s(id, val, score) ON t.id = s.id " +
                    "WHEN NOT MATCHED THEN INSERT (id, val, score) VALUES (s.id, s.val, s.score)"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::integer AS cnt FROM mr_target")) {

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("cnt"));
        }
    }

    // ========================================================================
    // Stmt 76: Multiple WHEN MATCHED clauses
    // PG: [1|was-low], [2|was-high]
    // Memgres: [1|was-high], [2|b], [3|was-high]
    // ========================================================================
    @Test
    void testStmt76_multipleWhenMatchedClauses() throws SQLException {
        resetTables(
                new String[]{"1, 'low', 5", "2, 'high', 50"},
                new String[]{"1, 'src1', 100", "2, 'src2', 200"}
        );

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING mr_source s ON t.id = s.id " +
                    "WHEN MATCHED AND t.score < 10 THEN UPDATE SET val = 'was-low', score = s.score " +
                    "WHEN MATCHED THEN UPDATE SET val = 'was-high', score = s.score"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM mr_target ORDER BY id")) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("was-low", rs.getString("val"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("was-high", rs.getString("val"));

            assertFalse(rs.next(), "Expected exactly 2 rows");
        }
    }

    // ========================================================================
    // Stmt 79: MERGE in subquery (no rows) — error message mismatch
    // PG: syntax error
    // Memgres: "Expected RIGHT_PAREN but found KEYWORD" (should contain "syntax error")
    // ========================================================================
    @Test
    void testStmt79_mergeInSubqueryNoRows() throws SQLException {
        resetTables(null, null);

        String sql =
                "SELECT count(*) AS cnt FROM ( " +
                "MERGE INTO mr_target t USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val RETURNING * ) sub";

        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> s.executeQuery(sql));
            String msg = ex.getMessage().toLowerCase();
            assertTrue(msg.contains("syntax error") || msg.contains("syntax"),
                    "Error should mention 'syntax error', got: " + ex.getMessage());
        }
    }

    // ========================================================================
    // Stmt 82: MERGE RETURNING with NULL / IS NULL — IllegalStateException
    // PG: (id,val_is_null) [10|true]
    // ========================================================================
    @Test
    void testStmt82_mergeReturningWithNullValues() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM mr_target");
            s.execute("DELETE FROM mr_source");
            s.execute("INSERT INTO mr_source VALUES (10, NULL, NULL)");
        }

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, val, score) VALUES (s.id, s.val, s.score) " +
                "RETURNING t.id, t.val IS NULL AS val_is_null";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next());
            assertEquals(10, rs.getInt("id"));
            assertTrue(rs.getBoolean("val_is_null"));
            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // Stmt 85: Subquery source — count mismatch
    // PG: cnt=1
    // Memgres: cnt=4
    // ========================================================================
    @Test
    void testStmt85_mergeWithSubquerySource() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM mr_target");
            // mr_source should still have (10, NULL, NULL) from stmt 82's setup
            // but we reset cleanly
            s.execute("DELETE FROM mr_source");
            s.execute("INSERT INTO mr_source VALUES (10, NULL, NULL)");
        }

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING (SELECT id, val, score FROM mr_source WHERE score IS NULL) s ON t.id = s.id " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.score)"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*)::integer AS cnt FROM mr_target")) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("cnt"));
        }
    }

    // ========================================================================
    // Stmt 88: MERGE RETURNING COALESCE — IllegalStateException
    // PG: (id,safe_val) [10|unknown], [20|unknown]
    // ========================================================================
    @Test
    void testStmt88_mergeReturningCoalesce() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM mr_target");
            s.execute("DELETE FROM mr_source");
            s.execute("INSERT INTO mr_source VALUES (10, NULL, NULL), (20, NULL, 50)");
        }

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val, s.score) " +
                "RETURNING t.id, COALESCE(t.val, 'unknown') AS safe_val";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            List<Integer> ids = new ArrayList<>();
            List<String> vals = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                vals.add(rs.getString("safe_val"));
            }
            assertEquals(2, ids.size(), "Expected 2 rows");
            assertTrue(ids.contains(10));
            assertTrue(ids.contains(20));
            for (String v : vals) {
                assertEquals("unknown", v);
            }
        }
    }

    // ========================================================================
    // Stmt 93: MERGE as CTE source (writable CTE) — parse error
    // PG: (id,val) [1|updated], [2|new]
    // Memgres: ERROR Expected keyword SELECT near 'MERGE'
    // ========================================================================
    @Test
    void testStmt93_mergeAsWritableCte() throws SQLException {
        resetTables(
                new String[]{"1, 'old', 10"},
                new String[]{"1, 'updated', 99", "2, 'new', 50"}
        );

        String sql =
                "WITH merged AS ( " +
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                "WHEN NOT MATCHED THEN INSERT (id, val, score) VALUES (s.id, s.val, s.score) " +
                "RETURNING t.id, t.val " +
                ") SELECT id, val FROM merged ORDER BY id";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("updated", rs.getString("val"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("new", rs.getString("val"));

            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // Stmt 99: Conditional WHEN NOT MATCHED BY SOURCE — row count
    // PG: [1|src] (1 row)
    // Memgres: 5 rows
    // ========================================================================
    @Test
    void testStmt99_conditionalNotMatchedBySourceDelete() throws SQLException {
        resetTables(
                new String[]{"1, 'active', 10", "2, 'inactive', 20", "3, 'inactive', 30"},
                new String[]{"1, 'src', 15"}
        );

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING mr_source s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET val = s.val " +
                    "WHEN NOT MATCHED BY SOURCE AND t.val = 'inactive' THEN DELETE"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM mr_target ORDER BY id")) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("src", rs.getString("val"));

            assertFalse(rs.next(), "Expected only 1 row (inactive rows should be deleted)");
        }
    }

    // ========================================================================
    // Stmt 105: Multiple WHEN NOT MATCHED BY SOURCE clauses
    // PG: [1|src|10], [3|orphan|50] (2 rows)
    // Memgres: 5 rows
    // ========================================================================
    @Test
    void testStmt105_multipleNotMatchedBySourceClauses() throws SQLException {
        resetTables(
                new String[]{"1, 'match', 10", "2, 'low', 5", "3, 'high', 50"},
                new String[]{"1, 'src', 15"}
        );

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING mr_source s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET val = s.val " +
                    "WHEN NOT MATCHED BY SOURCE AND t.score < 10 THEN DELETE " +
                    "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET val = 'orphan'"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT * FROM mr_target ORDER BY id")) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("src", rs.getString("val"));
            assertEquals(10, rs.getInt("score"));

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("orphan", rs.getString("val"));
            assertEquals(50, rs.getInt("score"));

            assertFalse(rs.next(), "Expected exactly 2 rows");
        }
    }

    // ========================================================================
    // Stmt 109: MERGE self-join (source = target)
    // PG: [1|a], [2|self-updated] (2 rows)
    // Memgres: 5 rows
    // ========================================================================
    @Test
    void testStmt109_mergeSelfJoin() throws SQLException {
        resetTables(
                new String[]{"1, 'a', 10", "2, 'b', 20"},
                null
        );

        try (Statement s = conn.createStatement()) {
            s.execute(
                    "MERGE INTO mr_target t " +
                    "USING (SELECT id, val, score FROM mr_target WHERE score > 10) s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET val = 'self-updated'"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, val FROM mr_target ORDER BY id")) {

            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("a", rs.getString("val"));

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("self-updated", rs.getString("val"));

            assertFalse(rs.next(), "Expected exactly 2 rows");
        }
    }

    // ========================================================================
    // Stmt 114: MERGE RETURNING with type cast — IllegalStateException
    // PG: (id_text,score_float) [1|99.0]
    // ========================================================================
    @Test
    void testStmt114_mergeReturningTypeCast() throws SQLException {
        resetTables(
                new String[]{"1, 'old', 10"},
                new String[]{"1, 'new', 99"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                "RETURNING t.id::text AS id_text, t.score::float AS score_float";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next());
            assertEquals("1", rs.getString("id_text"));
            assertEquals(99.0, rs.getDouble("score_float"), 0.001);
            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // Stmt 118: MERGE RETURNING with aggregate via writable CTE — parse error
    // PG: (cnt,total) [3|60]
    // Memgres: ERROR Expected keyword SELECT near 'MERGE'
    // ========================================================================
    @Test
    void testStmt118_mergeReturningAggregateViaCte() throws SQLException {
        resetTables(
                null,
                new String[]{"1, 'a', 10", "2, 'b', 20", "3, 'c', 30"}
        );

        String sql =
                "WITH inserted AS ( " +
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, val, score) VALUES (s.id, s.val, s.score) " +
                "RETURNING t.score " +
                ") SELECT count(*)::integer AS cnt, sum(score)::integer AS total FROM inserted";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("cnt"));
            assertEquals(60, rs.getInt("total"));
            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // Stmt 123: merge_action() with WHEN NOT MATCHED BY SOURCE — IllegalStateException
    // PG: (id,action) [2|UPDATE], [3|DELETE]
    // ========================================================================
    @Test
    void testStmt123_mergeActionNotMatchedBySource() throws SQLException {
        resetTables(
                new String[]{"1, 'keep', 10", "2, 'update-me', 20", "3, 'orphan', 30"},
                new String[]{"2, 'updated', 25"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                "WHEN NOT MATCHED BY SOURCE AND t.id > 1 THEN DELETE " +
                "RETURNING t.id, merge_action() AS action";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("UPDATE", rs.getString("action"));

            assertTrue(rs.next());
            assertEquals(3, rs.getInt("id"));
            assertEquals("DELETE", rs.getString("action"));

            assertFalse(rs.next());
        }
    }

    // ========================================================================
    // Stmt 127: RETURNING non-existent column — should error
    // PG: ERROR column t.nonexistent does not exist
    // Memgres: OK 0 rows affected (incorrectly succeeds)
    // ========================================================================
    @Test
    void testStmt127_returningNonExistentColumn() throws SQLException {
        resetTables(
                null,
                new String[]{"1, 'a', 10"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, val, score) VALUES (s.id, s.val, s.score) " +
                "RETURNING t.nonexistent";

        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> s.execute(sql));
            String msg = ex.getMessage().toLowerCase();
            assertTrue(msg.contains("column") || msg.contains("nonexistent"),
                    "Error should mention 'column', got: " + ex.getMessage());
        }
    }

    // ========================================================================
    // Stmt 132: All three clause types + RETURNING merge_action()
    // PG: (id,action) [1|UPDATE], [2|DELETE], [3|INSERT]
    // Memgres: IllegalStateException
    // ========================================================================
    @Test
    void testStmt132_allThreeClauseTypesReturning() throws SQLException {
        resetTables(
                new String[]{"1, 'match', 10", "2, 'orphan', 20"},
                new String[]{"1, 'updated', 15", "3, 'brand-new', 30"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                "WHEN NOT MATCHED BY TARGET THEN INSERT (id, val, score) VALUES (s.id, s.val, s.score) " +
                "WHEN NOT MATCHED BY SOURCE THEN DELETE " +
                "RETURNING t.id, merge_action() AS action";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            List<Integer> ids = new ArrayList<>();
            List<String> actions = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                actions.add(rs.getString("action"));
            }

            assertEquals(3, ids.size(), "Expected 3 rows");

            // Find each expected row
            int updateIdx = ids.indexOf(1);
            int deleteIdx = ids.indexOf(2);
            int insertIdx = ids.indexOf(3);

            assertTrue(updateIdx >= 0, "Expected row with id=1");
            assertTrue(deleteIdx >= 0, "Expected row with id=2");
            assertTrue(insertIdx >= 0, "Expected row with id=3");

            assertEquals("UPDATE", actions.get(updateIdx));
            assertEquals("DELETE", actions.get(deleteIdx));
            assertEquals("INSERT", actions.get(insertIdx));
        }
    }

    // ========================================================================
    // Stmt 139: Multi-table USING (JOIN source) + RETURNING — IllegalStateException
    // PG: (id,val,score) [1|new|120], [3|new3|330]
    // ========================================================================
    @Test
    void testStmt139_mergeWithJoinSource() throws SQLException {
        resetTables(
                new String[]{"1, 'old', 10"},
                new String[]{"1, 'new', 20", "3, 'new3', 30"}
        );

        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS mr_extra (id integer PRIMARY KEY, bonus integer)");
            s.execute("DELETE FROM mr_extra");
            s.execute("INSERT INTO mr_extra VALUES (1, 100), (3, 300)");
        }

        String sql =
                "MERGE INTO mr_target t " +
                "USING (SELECT s.id, s.val, s.score + e.bonus AS score " +
                "       FROM mr_source s JOIN mr_extra e ON s.id = e.id) src " +
                "ON t.id = src.id " +
                "WHEN MATCHED THEN UPDATE SET val = src.val, score = src.score " +
                "WHEN NOT MATCHED THEN INSERT (id, val, score) VALUES (src.id, src.val, src.score) " +
                "RETURNING t.*";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            List<Integer> ids = new ArrayList<>();
            List<String> vals = new ArrayList<>();
            List<Integer> scores = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                vals.add(rs.getString("val"));
                scores.add(rs.getInt("score"));
            }

            assertEquals(2, ids.size(), "Expected 2 rows");

            int idx1 = ids.indexOf(1);
            int idx3 = ids.indexOf(3);
            assertTrue(idx1 >= 0, "Expected row with id=1");
            assertTrue(idx3 >= 0, "Expected row with id=3");

            assertEquals("new", vals.get(idx1));
            assertEquals(120, scores.get(idx1));
            assertEquals("new3", vals.get(idx3));
            assertEquals(330, scores.get(idx3));
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS mr_extra");
            }
        }
    }

    // ========================================================================
    // Stmt 146: CHECK constraint — valid update score should be 50
    // PG: score=50
    // Memgres: score=15 (MERGE did not apply correctly)
    // ========================================================================
    @Test
    void testStmt146_checkConstraintValidUpdate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS mr_checked (" +
                      "id integer PRIMARY KEY, val text, score integer CHECK (score >= 0))");
            s.execute("DELETE FROM mr_checked");
            s.execute("INSERT INTO mr_checked VALUES (1, 'old', 10)");
            s.execute("DELETE FROM mr_source");
            s.execute("INSERT INTO mr_source VALUES (1, 'updated', 50)");

            s.execute(
                    "MERGE INTO mr_checked t " +
                    "USING mr_source s ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score"
            );
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT score FROM mr_checked WHERE id = 1")) {

            assertTrue(rs.next());
            assertEquals(50, rs.getInt("score"));
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS mr_checked");
            }
        }
    }

    // ========================================================================
    // Stmt 149: CHECK constraint — invalid update should fail
    // PG: ERROR violates check constraint
    // Memgres: OK 1 rows affected (incorrectly succeeds)
    // ========================================================================
    @Test
    void testStmt149_checkConstraintViolation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS mr_checked (" +
                      "id integer PRIMARY KEY, val text, score integer CHECK (score >= 0))");
            s.execute("DELETE FROM mr_checked");
            s.execute("INSERT INTO mr_checked VALUES (1, 'old', 10)");
            s.execute("DELETE FROM mr_source");
            s.execute("INSERT INTO mr_source VALUES (1, 'bad', -10)");
        }

        String sql =
                "MERGE INTO mr_checked t " +
                "USING mr_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score";

        try (Statement s = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class, () -> s.execute(sql));
            String msg = ex.getMessage().toLowerCase();
            assertTrue(msg.contains("violates check constraint") || msg.contains("check"),
                    "Error should mention check constraint violation, got: " + ex.getMessage());
        } finally {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP TABLE IF EXISTS mr_checked");
            }
        }
    }

    // ========================================================================
    // Stmt 155: Expression-based ON condition + RETURNING — IllegalStateException
    // PG: (id,val) [1|src-for-1], [2|src-for-2]
    // ========================================================================
    @Test
    void testStmt155_expressionBasedOnCondition() throws SQLException {
        resetTables(
                new String[]{"1, 'a', 10", "2, 'b', 20"},
                new String[]{"2, 'src-for-1', 99", "3, 'src-for-2', 88"}
        );

        String sql =
                "MERGE INTO mr_target t " +
                "USING mr_source s ON t.id = s.id - 1 " +
                "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score " +
                "RETURNING t.id, t.val";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            List<Integer> ids = new ArrayList<>();
            List<String> vals = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                vals.add(rs.getString("val"));
            }

            assertEquals(2, ids.size(), "Expected 2 rows");

            int idx1 = ids.indexOf(1);
            int idx2 = ids.indexOf(2);
            assertTrue(idx1 >= 0, "Expected row with id=1");
            assertTrue(idx2 >= 0, "Expected row with id=2");

            assertEquals("src-for-1", vals.get(idx1));
            assertEquals("src-for-2", vals.get(idx2));
        }
    }
}
