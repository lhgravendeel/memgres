package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 3 PG vs Memgres differences from returning-old-new.sql and
 * merge-returning.sql.
 *
 * These tests assert exact PG 18 behavior. They are expected to FAIL on
 * current Memgres, documenting the real gaps.
 *
 * Uses default JDBC (extended query protocol) to match the comparison framework.
 */
class ReturningEdgeCaseTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
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
        if (memgres != null) memgres.close();
    }

    /**
     * Stmt 44 (returning-old-new.sql): DELETE RETURNING OLD.* row ordering.
     *
     * PG returns (20, twenty, 200) first, then (21, twenty-one, 210).
     * Memgres returns them in reverse order.
     */
    @Test
    void stmt44_deleteReturningOldRowOrder() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ron_data CASCADE");
            s.execute("CREATE TABLE ron_data (id integer PRIMARY KEY, val text, score integer)");
            s.execute("INSERT INTO ron_data VALUES (20, 'twenty', 200), (21, 'twenty-one', 210)");

            try (ResultSet rs = s.executeQuery(
                    "DELETE FROM ron_data WHERE id IN (20, 21) RETURNING OLD.*")) {
                List<Integer> ids = new ArrayList<>();
                while (rs.next()) {
                    ids.add(rs.getInt("id"));
                }
                assertEquals(2, ids.size(), "Expected 2 deleted rows");
                assertEquals(20, ids.get(0),
                        "PG returns id=20 as first row, but Memgres returns " + ids.get(0));
                assertEquals(21, ids.get(1),
                        "PG returns id=21 as second row, but Memgres returns " + ids.get(1));
            }
        }
    }

    /**
     * Stmt 62 (returning-old-new.sql): INSERT ... ON CONFLICT DO NOTHING
     * RETURNING NEW.* wrapped in a subquery.
     *
     * PG: ERROR [42601] syntax error at or near "INTO"
     * Memgres: OK (cnt) [0]
     */
    @Test
    void stmt62_onConflictDoNothingReturningNewInSubquery_pgErrors() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS ron_upsert2 CASCADE");
            s.execute("CREATE TABLE ron_upsert2 (id integer PRIMARY KEY, val text)");
            s.execute("INSERT INTO ron_upsert2 VALUES (1, 'existing')");

            try {
                s.executeQuery(
                        "SELECT count(*)::integer AS cnt FROM ("
                        + "  INSERT INTO ron_upsert2 VALUES (1, 'conflict')"
                        + "  ON CONFLICT (id) DO NOTHING"
                        + "  RETURNING NEW.*"
                        + ") sub");
                fail("PG rejects this with ERROR [42601] syntax error at or near \"INTO\", "
                        + "but Memgres succeeded");
            } catch (SQLException e) {
                assertEquals("42601", e.getSQLState(),
                        "SQLSTATE should be 42601 (syntax_error), got: "
                        + e.getSQLState() + " - " + e.getMessage());
            }
        }
    }

    /**
     * Stmt 16 (merge-returning.sql): MERGE RETURNING * should return 6 columns.
     *
     * PG returns (id, val, score, id, val, score) — both target AND source.
     * Memgres returns (id, val, score) — target only.
     */
    @Test
    void stmt16_mergeReturningStar_shouldReturn6Columns() throws Exception {
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
                assertEquals(6, md.getColumnCount(),
                        "PG returns 6 columns for MERGE RETURNING * (target + source), "
                        + "but Memgres returns " + md.getColumnCount());

                assertTrue(rs.next(), "Expected first row");
                assertTrue(rs.next(), "Expected second row");
                assertFalse(rs.next(), "Expected exactly 2 rows");
            }
        }
    }
}
