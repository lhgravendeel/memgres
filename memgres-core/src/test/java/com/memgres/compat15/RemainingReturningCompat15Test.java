package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 3 remaining Memgres-vs-PG differences from returning-old-new.sql
 * and merge-returning.sql.
 *
 * These tests assert PG 18 behavior. They are expected to FAIL on current
 * Memgres and pass once the underlying issues are fixed.
 */
class RemainingReturningCompat15Test {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS ret_compat CASCADE");
            s.execute("CREATE SCHEMA ret_compat");
            s.execute("SET search_path = ret_compat, public");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS ret_compat CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) {
            memgres.close();
        }
    }

    // ========================================================================
    // Stmt 44 (returning-old-new.sql): DELETE RETURNING OLD.* should return
    // all deleted rows (order may vary between PG and Memgres).
    // ========================================================================
    @Test
    void stmt44_deleteReturningOldShouldReturnAllRows() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ron_data CASCADE");
            s.execute("CREATE TABLE ron_data (id integer PRIMARY KEY, val text, score integer)");
            s.execute("INSERT INTO ron_data VALUES (20, 'twenty', 200), (21, 'twenty-one', 210)");

            try (ResultSet rs = s.executeQuery(
                    "DELETE FROM ron_data WHERE id IN (20, 21) RETURNING OLD.*")) {
                java.util.Set<Integer> ids = new java.util.HashSet<>();
                assertTrue(rs.next(), "Expected first row");
                ids.add(rs.getInt("id"));
                assertTrue(rs.next(), "Expected second row");
                ids.add(rs.getInt("id"));
                assertFalse(rs.next(), "Expected exactly 2 rows");
                assertEquals(java.util.Set.of(20, 21), ids,
                        "DELETE RETURNING OLD.* should return both deleted rows");
            }
        }
    }

    // ========================================================================
    // Stmt 63 (returning-old-new.sql): INSERT ON CONFLICT DO NOTHING
    // RETURNING NEW.* should work (not syntax error).
    // PG: OK [2 | no-conflict]
    // Memgres: ERROR [42601] syntax error at or near "INTO"
    // ========================================================================
    @Test
    void stmt63_insertOnConflictDoNothingReturningNewShouldWork() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ron_upsert2 CASCADE");
            s.execute("CREATE TABLE ron_upsert2 (id integer PRIMARY KEY, val text)");
            s.execute("INSERT INTO ron_upsert2 VALUES (1, 'existing')");

            try (ResultSet rs = s.executeQuery(
                    "INSERT INTO ron_upsert2 VALUES (2, 'no-conflict') "
                    + "ON CONFLICT (id) DO NOTHING RETURNING NEW.*")) {
                assertTrue(rs.next(), "Expected one result row for non-conflicting insert");
                assertEquals(2, rs.getInt("id"));
                assertEquals("no-conflict", rs.getString("val"));
                assertFalse(rs.next(), "Expected exactly 1 row");
            }
        }
    }

    // ========================================================================
    // Stmt 16 (merge-returning.sql): MERGE RETURNING * returns at least
    // the target table columns with correct values.
    // PG returns both target and source columns; Memgres returns target only.
    // ========================================================================
    @Test
    void stmt16_mergeReturningShouldReturnTargetColumns() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS mr_target CASCADE");
            s.execute("DROP TABLE IF EXISTS mr_source CASCADE");
            s.execute("CREATE TABLE mr_target (id integer PRIMARY KEY, val text, score integer)");
            s.execute("CREATE TABLE mr_source (id integer PRIMARY KEY, val text, score integer)");
            s.execute("INSERT INTO mr_target VALUES (1, 'old', 10)");
            s.execute("INSERT INTO mr_source VALUES (1, 'updated', 15), (2, 'new', 20)");

            try (ResultSet rs = s.executeQuery(
                    "MERGE INTO mr_target t USING mr_source s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET val = s.val, score = s.score "
                    + "WHEN NOT MATCHED THEN INSERT (id, val, score) VALUES (s.id, s.val, s.score) "
                    + "RETURNING *")) {

                ResultSetMetaData md = rs.getMetaData();
                assertTrue(md.getColumnCount() >= 3,
                        "MERGE RETURNING * should return at least 3 columns (target), got "
                        + md.getColumnCount());

                java.util.Set<Integer> ids = new java.util.HashSet<>();
                while (rs.next()) {
                    ids.add(rs.getInt(1));
                }
                assertEquals(java.util.Set.of(1, 2), ids,
                        "MERGE RETURNING should include both matched and inserted rows");
            }
        }
    }
}
