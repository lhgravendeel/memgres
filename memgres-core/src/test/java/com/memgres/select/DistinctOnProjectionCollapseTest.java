package com.memgres.select;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Group 4 (mtask-8): {@code SelectParser.parseSelectBody} sets {@code distinct = true} for
 * {@code DISTINCT ON (...)} too, and {@code SelectExecutor.applyDistinct} then ran an
 * unconditional full-projection DISTINCT pass whenever {@code stmt.distinct()}, with no guard for
 * {@code stmt.distinctOn()}. DISTINCT ON already deduped correctly on its key expressions
 * (SelectExecutor ~line 407); the extra pass then merged rows that have distinct DISTINCT ON keys
 * but an incidentally-equal projection, silently dropping rows PostgreSQL keeps.
 *
 * Mirrors the real repro (mtask-7-report.md Group 4, R2): a DISTINCT ON per-day subquery whose
 * projected value repeats across two different days.
 */
class DistinctOnProjectionCollapseTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE rd_g4 (d date, typ text, charged numeric)");
            // Two dates ('2026-01-01', '2026-01-02') both end up projecting charged=5 after
            // DISTINCT ON picks the 'manual' row per day; a third date projects a different
            // value (10); a fourth date has only one row (charged=5, ties the collapsed value).
            s.execute("INSERT INTO rd_g4 VALUES ('2026-01-01', 'auto', 1)");
            s.execute("INSERT INTO rd_g4 VALUES ('2026-01-01', 'manual', 5)");
            s.execute("INSERT INTO rd_g4 VALUES ('2026-01-02', 'auto', 2)");
            s.execute("INSERT INTO rd_g4 VALUES ('2026-01-02', 'manual', 5)");
            s.execute("INSERT INTO rd_g4 VALUES ('2026-01-03', 'manual', 10)");
            s.execute("INSERT INTO rd_g4 VALUES ('2026-01-04', 'manual', 5)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    void distinctOnKeepsRowsWithDistinctKeysEvenWhenProjectionRepeats() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT SUM(charged), COUNT(*) FROM ("
                             + "  SELECT DISTINCT ON (d) charged FROM rd_g4"
                             + "  ORDER BY d, CASE typ WHEN 'manual' THEN 0 ELSE 1 END"
                             + ") daily")) {
            assertTrue(rs.next());
            // 4 dates each contribute one row (5, 5, 10, 5) -> SUM 25, COUNT 4.
            assertEquals(25.0, rs.getDouble(1), 0.0001);
            assertEquals(4, rs.getInt(2));
        }
    }

    @Test
    void distinctOnRowsAreExposedDirectly_notCollapsedByProjection() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT DISTINCT ON (d) d, charged FROM rd_g4 "
                             + "ORDER BY d, CASE typ WHEN 'manual' THEN 0 ELSE 1 END")) {
            int count = 0;
            while (rs.next()) count++;
            assertEquals(4, count, "one row per distinct date, regardless of repeated projected values");
        }
    }

    // Regression guard: plain DISTINCT (no ON) must still collapse full-projection duplicates.
    @Test
    void plainDistinctWithoutOn_stillCollapsesDuplicateProjections() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE plain_distinct_g4 (v int)");
            s.execute("INSERT INTO plain_distinct_g4 VALUES (1), (1), (2)");
            try (ResultSet rs = s.executeQuery("SELECT DISTINCT v FROM plain_distinct_g4 ORDER BY v")) {
                int count = 0;
                while (rs.next()) count++;
                assertEquals(2, count);
            }
            s.execute("DROP TABLE plain_distinct_g4");
        }
    }
}
