package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 4 failures from returning-old-new.sql (PG 18 RETURNING OLD/NEW).
 *
 * Stmt 41: MERGE RETURNING OLD/NEW — Memgres throws IllegalStateException instead of returning rows.
 * Stmt 62: INSERT ... ON CONFLICT DO NOTHING RETURNING NEW.* inside subquery — PG rejects with syntax error, Memgres accepts.
 * Stmt 76: MERGE with NOT MATCHED BY SOURCE + RETURNING — Memgres throws IllegalStateException instead of returning rows.
 * Stmt 96: DELETE ... USING RETURNING OLD.* — row order differs (PG: [3,also-remove],[2,remove]; Memgres: [2,remove],[3,also-remove]).
 */
class ReturningOldNewCompat15Test {

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
            // Schema setup
            s.execute("DROP SCHEMA IF EXISTS ron_test CASCADE");
            s.execute("CREATE SCHEMA ron_test");
            s.execute("SET search_path = ron_test, public");

            // Stmt 41 setup: MERGE tables
            s.execute("CREATE TABLE ron_merge_target (id integer PRIMARY KEY, val text)");
            s.execute("CREATE TABLE ron_merge_source (id integer PRIMARY KEY, val text)");
            s.execute("INSERT INTO ron_merge_target VALUES (1, 'old-val')");
            s.execute("INSERT INTO ron_merge_source VALUES (1, 'new-val'), (2, 'inserted')");

            // Stmt 62 setup: ON CONFLICT DO NOTHING table
            s.execute("CREATE TABLE ron_upsert2 (id integer PRIMARY KEY, val text)");
            s.execute("INSERT INTO ron_upsert2 VALUES (1, 'existing')");

            // Stmt 76 setup: MERGE with NOT MATCHED BY SOURCE
            s.execute("CREATE TABLE ron_merge2_target (id integer PRIMARY KEY, val text)");
            s.execute("CREATE TABLE ron_merge2_source (id integer PRIMARY KEY, val text)");
            s.execute("INSERT INTO ron_merge2_target VALUES (1, 'match'), (2, 'orphan')");
            s.execute("INSERT INTO ron_merge2_source VALUES (1, 'updated'), (3, 'new')");

            // Stmt 96 setup: DELETE ... USING
            s.execute("CREATE TABLE ron_items (id integer PRIMARY KEY, val text)");
            s.execute("CREATE TABLE ron_blacklist (val text)");
            s.execute("INSERT INTO ron_items VALUES (1, 'keep'), (2, 'remove'), (3, 'also-remove')");
            s.execute("INSERT INTO ron_blacklist VALUES ('remove'), ('also-remove')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS ron_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 41: MERGE INTO ... RETURNING t.id, merge_action(), OLD.val, NEW.val
     *
     * PG 18 returns:
     *   row 1: (1, UPDATE, old-val, new-val)
     *   row 2: (2, INSERT, NULL, inserted)
     *
     * Memgres throws: IllegalStateException: Received resultset tuples, but no field structure for them
     */
    @Test
    void testMergeReturningOldNew_stmt41() throws SQLException {
        String sql =
                "MERGE INTO ron_merge_target t " +
                "USING ron_merge_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.val) " +
                "RETURNING t.id, merge_action() AS action, OLD.val AS old_val, NEW.val AS new_val";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            // Row 1: UPDATE existing row
            assertTrue(rs.next(), "Expected first row from MERGE RETURNING");
            assertEquals(1, rs.getInt("id"));
            assertEquals("UPDATE", rs.getString("action"));
            assertEquals("old-val", rs.getString("old_val"));
            assertEquals("new-val", rs.getString("new_val"));

            // Row 2: INSERT new row
            assertTrue(rs.next(), "Expected second row from MERGE RETURNING");
            assertEquals(2, rs.getInt("id"));
            assertEquals("INSERT", rs.getString("action"));
            assertNull(rs.getString("old_val"), "OLD.val should be NULL for INSERT action");
            assertEquals("inserted", rs.getString("new_val"));

            assertFalse(rs.next(), "Expected only 2 rows from MERGE RETURNING");
        }
    }

    /**
     * Stmt 62: SELECT count(*)::integer FROM (INSERT ... ON CONFLICT DO NOTHING RETURNING NEW.*) sub
     *
     * PG 18 rejects this with: ERROR [42601]: syntax error at or near "INTO"
     * Memgres incorrectly succeeds with cnt=0.
     */
    @Test
    void testOnConflictDoNothingReturningNewInSubquery_stmt62() throws Exception {
        String sql =
                "SELECT count(*)::integer AS cnt FROM (" +
                "  INSERT INTO ron_upsert2 VALUES (1, 'conflict') " +
                "  ON CONFLICT (id) DO NOTHING " +
                "  RETURNING NEW.*" +
                ") sub";

        // ON CONFLICT DO NOTHING with RETURNING NEW.* — conflicting row returns nothing
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("cnt"),
                    "Conflicting INSERT with DO NOTHING should return 0 rows via RETURNING");
        }
    }

    /**
     * Stmt 76: MERGE with WHEN NOT MATCHED BY SOURCE THEN DELETE + RETURNING OLD/NEW
     *
     * PG 18 returns:
     *   row 1: (1, UPDATE, match, updated)
     *   row 2: (2, DELETE, orphan, NULL)
     *   row 3: (3, INSERT, NULL, new)
     *
     * Memgres throws: IllegalStateException: Received resultset tuples, but no field structure for them
     */
    @Test
    void testMergeNotMatchedBySourceReturning_stmt76() throws SQLException {
        String sql =
                "MERGE INTO ron_merge2_target t " +
                "USING ron_merge2_source s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET val = s.val " +
                "WHEN NOT MATCHED BY TARGET THEN INSERT VALUES (s.id, s.val) " +
                "WHEN NOT MATCHED BY SOURCE THEN DELETE " +
                "RETURNING t.id, merge_action() AS action, OLD.val AS old_val, NEW.val AS new_val";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            // Row 1: UPDATE matched row
            assertTrue(rs.next(), "Expected first row from MERGE RETURNING");
            assertEquals(1, rs.getInt("id"));
            assertEquals("UPDATE", rs.getString("action"));
            assertEquals("match", rs.getString("old_val"));
            assertEquals("updated", rs.getString("new_val"));

            // Row 2: DELETE orphan (not matched by source)
            assertTrue(rs.next(), "Expected second row from MERGE RETURNING");
            assertEquals(2, rs.getInt("id"));
            assertEquals("DELETE", rs.getString("action"));
            assertEquals("orphan", rs.getString("old_val"));
            assertNull(rs.getString("new_val"), "NEW.val should be NULL for DELETE action");

            // Row 3: INSERT new row
            assertTrue(rs.next(), "Expected third row from MERGE RETURNING");
            assertEquals(3, rs.getInt("id"));
            assertEquals("INSERT", rs.getString("action"));
            assertNull(rs.getString("old_val"), "OLD.val should be NULL for INSERT action");
            assertEquals("new", rs.getString("new_val"));

            assertFalse(rs.next(), "Expected only 3 rows from MERGE RETURNING");
        }
    }

    /**
     * Stmt 96: DELETE FROM ron_items USING ron_blacklist WHERE i.val = b.val RETURNING OLD.*
     *
     * PG 18 returns rows in order: (3, also-remove), (2, remove)
     * Memgres returns rows in order: (2, remove), (3, also-remove)
     */
    @Test
    void testDeleteUsingReturningOldRowOrder_stmt96() throws SQLException {
        String sql =
                "DELETE FROM ron_items i " +
                "USING ron_blacklist b " +
                "WHERE i.val = b.val " +
                "RETURNING OLD.*";

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(sql)) {

            List<Integer> ids = new ArrayList<>();
            List<String> vals = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getInt("id"));
                vals.add(rs.getString("val"));
            }

            assertEquals(2, ids.size(), "Expected 2 deleted rows");

            // PG 18 returns (3, also-remove) first, then (2, remove)
            assertEquals(3, ids.get(0), "First deleted row should be id=3");
            assertEquals("also-remove", vals.get(0), "First deleted row should be val=also-remove");
            assertEquals(2, ids.get(1), "Second deleted row should be id=2");
            assertEquals("remove", vals.get(1), "Second deleted row should be val=remove");
        }
    }
}
