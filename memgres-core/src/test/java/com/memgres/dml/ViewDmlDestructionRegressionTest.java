package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regressions the previous fix wave re-opened, all of which lose or corrupt data.
 * Every expectation was captured from a live PostgreSQL 18.0 server.
 *
 * <p>N1 view updatability, N4 data-modifying CTE ordering, N13 renamed-view DML with a
 * subquery, N23 nested WITH CHECK OPTION, N24 spurious MERGE 21000.
 */
class ViewDmlDestructionRegressionTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private static String state(Executable r) {
        SQLException e = assertThrows(SQLException.class, r::run);
        return e.getSQLState();
    }

    private interface Executable {
        void run() throws Exception;
    }

    // ------------------------------------------------------------------
    // N1 — a view is not auto-updatable just because its target is not a bare aggregate
    // ------------------------------------------------------------------

    @Test
    void aggregateInExpressionViewIsNotUpdatable() throws Exception {
        exec("CREATE TABLE n1a (id int, val int)");
        exec("INSERT INTO n1a VALUES (1,10),(2,20)");
        exec("CREATE VIEW n1a_v AS SELECT sum(val)+1 AS s FROM n1a");

        SQLException e = assertThrows(SQLException.class, () -> exec("DELETE FROM n1a_v"));
        assertEquals("55000", e.getSQLState());
        assertTrue(e.getMessage().contains("cannot delete from view"), e.getMessage());
        assertEquals(java.util.Arrays.asList("2"), rows("SELECT count(*) FROM n1a"));
    }

    @Test
    void windowFunctionViewIsNotUpdatable() throws Exception {
        exec("CREATE TABLE n1b (id int, val int)");
        exec("INSERT INTO n1b VALUES (1,10),(2,20)");
        exec("CREATE VIEW n1b_v AS SELECT id, row_number() OVER () AS rn FROM n1b");

        assertEquals("55000", state(() -> exec("DELETE FROM n1b_v")));
        assertEquals(java.util.Arrays.asList("2"), rows("SELECT count(*) FROM n1b"));
    }

    @Test
    void distinctAndGroupByViewsAreNotUpdatable() throws Exception {
        exec("CREATE TABLE n1c (id int, val int)");
        exec("INSERT INTO n1c VALUES (1,10),(2,20)");
        exec("CREATE VIEW n1c_d AS SELECT DISTINCT id FROM n1c");
        exec("CREATE VIEW n1c_g AS SELECT id, count(*) AS c FROM n1c GROUP BY id");

        assertEquals("55000", state(() -> exec("DELETE FROM n1c_d")));
        assertEquals("55000", state(() -> exec("DELETE FROM n1c_g")));
        assertEquals(java.util.Arrays.asList("2"), rows("SELECT count(*) FROM n1c"));
    }

    /** An expression column cannot be assigned, but the view's plain columns still can. */
    @Test
    void expressionColumnCannotBeUpdated() throws Exception {
        exec("CREATE TABLE n1d (id int, val int)");
        exec("INSERT INTO n1d VALUES (1,10),(2,20)");
        exec("CREATE VIEW n1d_v AS SELECT id, val*2 AS dbl FROM n1d");

        assertEquals("0A000", state(() -> exec("UPDATE n1d_v SET dbl=5")));
        assertEquals(java.util.Arrays.asList("1|10", "2|20"),
                rows("SELECT id, val FROM n1d ORDER BY id"));

        exec("UPDATE n1d_v SET id=5 WHERE id=1");
        assertEquals(java.util.Arrays.asList("2|20", "5|10"),
                rows("SELECT id, val FROM n1d ORDER BY id"));
    }

    // ------------------------------------------------------------------
    // N4 — data-modifying CTEs run before the main statement
    // ------------------------------------------------------------------

    @Test
    void unreferencedDmlCteRunsBeforeTheMainStatement() throws Exception {
        exec("CREATE TABLE n4t (a int, b int)");
        exec("INSERT INTO n4t VALUES (1,1),(2,2)");

        exec("WITH d AS (DELETE FROM n4t RETURNING *) INSERT INTO n4t VALUES (99,99)");

        assertEquals(java.util.Arrays.asList("99|99"), rows("SELECT a, b FROM n4t ORDER BY a"));
    }

    @Test
    void unreferencedDmlCteRunsOnceForUpdate() throws Exception {
        exec("CREATE TABLE n4u (a int)");
        exec("INSERT INTO n4u VALUES (1),(2)");

        exec("WITH d AS (DELETE FROM n4u WHERE a=1 RETURNING *) UPDATE n4u SET a=a+10");

        assertEquals(java.util.Arrays.asList("12"), rows("SELECT a FROM n4u ORDER BY a"));
    }

    // ------------------------------------------------------------------
    // N13 — a renaming view survives a subquery in the same statement
    // ------------------------------------------------------------------

    @Test
    void renamedViewUpdateWithSubqueryResolves() throws Exception {
        exec("CREATE TABLE n13t (id int, val int)");
        exec("INSERT INTO n13t VALUES (1,10),(2,20)");
        exec("CREATE VIEW n13v (rid, rval) AS SELECT id, val FROM n13t");

        exec("UPDATE n13v SET rval=99 WHERE rid IN (SELECT id FROM n13t WHERE id=1)");
        assertEquals(java.util.Arrays.asList("1|99", "2|20"),
                rows("SELECT id, val FROM n13t ORDER BY id"));
    }

    @Test
    void renamedViewInsertFromSelectResolves() throws Exception {
        exec("CREATE TABLE n13i (id int, val int)");
        exec("INSERT INTO n13i VALUES (1,10)");
        exec("CREATE VIEW n13iv (rid, rval) AS SELECT id, val FROM n13i");

        exec("INSERT INTO n13iv(rid, rval) SELECT 3, 30");
        assertEquals(java.util.Arrays.asList("1|10", "3|30"),
                rows("SELECT id, val FROM n13i ORDER BY id"));
    }

    /** The subquery resolves its own renaming view without clobbering the target's mapping. */
    @Test
    void renamedViewDmlWithSubqueryOverAnotherRenamedView() throws Exception {
        exec("CREATE TABLE n13x (id int, val int)");
        exec("CREATE TABLE n13y (k int)");
        exec("INSERT INTO n13x VALUES (1,10),(2,20)");
        exec("INSERT INTO n13y VALUES (1)");
        exec("CREATE VIEW n13xv (rid, rval) AS SELECT id, val FROM n13x");
        exec("CREATE VIEW n13yv (yk) AS SELECT k FROM n13y");

        exec("UPDATE n13xv SET rval=77 WHERE rid IN (SELECT yk FROM n13yv)");
        assertEquals(java.util.Arrays.asList("1|77", "2|20"),
                rows("SELECT id, val FROM n13x ORDER BY id"));

        exec("DELETE FROM n13xv WHERE rid IN (SELECT yk FROM n13yv)");
        assertEquals(java.util.Arrays.asList("1"), rows("SELECT count(*) FROM n13x"));
    }

    // ------------------------------------------------------------------
    // N23 — an inner view's own check option applies through an outer view
    // ------------------------------------------------------------------

    @Test
    void innerViewCheckOptionEnforcedThroughOuterView() throws Exception {
        exec("CREATE TABLE n23t (id int, val int)");
        exec("CREATE VIEW n23_inner AS SELECT * FROM n23t WHERE val > 0 WITH CHECK OPTION");
        exec("CREATE VIEW n23_outer AS SELECT * FROM n23_inner");

        assertEquals("44000", state(() -> exec("INSERT INTO n23_outer VALUES (1, -5)")));
        assertEquals(java.util.Arrays.asList("0"), rows("SELECT count(*) FROM n23t"));

        exec("INSERT INTO n23_outer VALUES (2, 5)");
        assertEquals(java.util.Arrays.asList("1"), rows("SELECT count(*) FROM n23t"));
    }

    /** A base view with no check option is only checked when a parent cascades. */
    @Test
    void localCheckOptionDoesNotReachUncheckedBaseView() throws Exception {
        exec("CREATE TABLE n23b (id int, val int)");
        exec("CREATE VIEW n23b_inner AS SELECT * FROM n23b WHERE val > 0");
        exec("CREATE VIEW n23b_outer AS SELECT * FROM n23b_inner WHERE id > 0 WITH LOCAL CHECK OPTION");

        exec("INSERT INTO n23b_outer VALUES (1, -5)");
        assertEquals(java.util.Arrays.asList("1"), rows("SELECT count(*) FROM n23b"));
    }

    // ------------------------------------------------------------------
    // N24 — DO NOTHING does not "affect" a row
    // ------------------------------------------------------------------

    @Test
    void mergeDoNothingArmDoesNotBlockLaterUpdate() throws Exception {
        exec("CREATE TABLE n24tgt (id int, tag text, v int)");
        exec("CREATE TABLE n24src (id int, tag text)");
        exec("INSERT INTO n24tgt VALUES (1,'x',0)");
        exec("INSERT INTO n24src VALUES (1,'a'),(1,'b')");

        exec("MERGE INTO n24tgt t USING n24src s ON t.id=s.id "
                + "WHEN MATCHED AND s.tag='a' THEN DO NOTHING "
                + "WHEN MATCHED AND s.tag='b' THEN UPDATE SET v=99");

        assertEquals(java.util.Arrays.asList("1|x|99"), rows("SELECT id, tag, v FROM n24tgt"));
    }

    /** Two real modifications of the same row are still an error. */
    @Test
    void mergeStillRejectsTwoUpdatesOfTheSameRow() throws Exception {
        exec("CREATE TABLE n24t2 (id int, v int)");
        exec("CREATE TABLE n24s2 (id int, v int)");
        exec("INSERT INTO n24t2 VALUES (1,0)");
        exec("INSERT INTO n24s2 VALUES (1,10),(1,20)");

        assertEquals("21000", state(() -> exec(
                "MERGE INTO n24t2 t USING n24s2 s ON t.id=s.id "
                        + "WHEN MATCHED THEN UPDATE SET v=s.v")));
    }

    @Test
    void mergeNonFiringArmDoesNotBlockLaterUpdate() throws Exception {
        exec("CREATE TABLE n24t3 (id int, v int)");
        exec("CREATE TABLE n24s3 (id int, tag text)");
        exec("INSERT INTO n24t3 VALUES (1,0)");
        exec("INSERT INTO n24s3 VALUES (1,'skip'),(1,'go')");

        exec("MERGE INTO n24t3 t USING n24s3 s ON t.id=s.id "
                + "WHEN MATCHED AND s.tag='go' THEN UPDATE SET v=7");

        assertTrue(rows("SELECT v FROM n24t3").contains("7"));
    }
}
