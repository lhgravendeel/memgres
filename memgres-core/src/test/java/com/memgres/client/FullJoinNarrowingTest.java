package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Where the FULL JOIN restriction is asked, and where it is not asked at all.
 *
 * <p>PostgreSQL refuses a full join whose condition it can neither merge nor hash, and memgres
 * reproduces the refusal so that an application cannot tell the two engines apart. But it is a
 * limitation of PostgreSQL's planner and not a rule about what the query means: memgres can compute
 * any full join it is asked for. A refusal PostgreSQL would not have raised is therefore pure loss,
 * while an acceptance PostgreSQL would not have granted costs only that memgres answers a query
 * PostgreSQL declines to plan.
 *
 * <p>Two rounds of trying to decide the question everywhere got that trade the wrong way round. The
 * condition is judged after {@code reduce_outer_joins} has run, and that works on whatever qual has
 * arrived above the join by then — which may have been written a long way from it.
 * {@code pull_up_subqueries} lifts a simple subquery into the query reading it; where it cannot,
 * {@code subquery_is_pushdown_safe} pushes the qual down instead, and it declines to do that only
 * for LIMIT and OFFSET. Reading DISTINCT, GROUP BY, HAVING, window definitions and aggregate
 * targets as barriers — which they are to pull-up and are not to push-down — refused thirty
 * ordinary reporting queries: a non-equi full join inside a grouped or DISTINCT subquery, CTE or
 * view, with an IS NOT NULL or equality filter outside it.
 *
 * <p>So the question is asked in one place: the query the client sent, when the join stands in that
 * query's own FROM clause and that query's own WHERE, HAVING and enclosing ON conditions do not
 * reduce it. As soon as the join sits inside a subquery, a WITH query, a view, an arm of a set
 * operation, the source of a writing statement or a function body, it is accepted unread, because
 * something above it may rescue it and nothing here can tell which.
 *
 * <p>What that qual proves is counted over relations and not columns, as
 * {@code find_nonnullable_rels} counts it: an OR proves what every one of its arms proves, so
 * {@code a.x = 1 OR a.t = 'b'} proves that {@code a} is not null and reduces the join, while
 * {@code a.x IS NOT NULL OR b.y IS NOT NULL} proves nothing and does not. A qual naming a third
 * relation proves nothing about either side.
 *
 * <p>Everything asserted here was measured against PostgreSQL 18. The refusals in
 * {@link StillRefusedAtTheTop} are refusals PostgreSQL raises; the answers in
 * {@link ReportingSqlThatMustNotBeRefused} are answers PostgreSQL gives; and
 * {@link ReachGivenUp} is the price, eight shapes PostgreSQL refuses and memgres now answers.
 */
class FullJoinNarrowingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE fjn_a (x int, t text, n numeric)");
        exec("CREATE TABLE fjn_b (y int, t text, n numeric)");
        exec("CREATE TABLE fjn_c (z int PRIMARY KEY)");
        exec("CREATE TABLE fjn_tgt (id int, k int)");
        exec("INSERT INTO fjn_a VALUES (1,'a',1),(2,'b',2)");
        exec("INSERT INTO fjn_b VALUES (1,'a',1),(3,'c',3)");
        exec("INSERT INTO fjn_c VALUES (1),(3)");
        exec("CREATE VIEW fjn_vv AS SELECT a.x AS ax, a.t AS at, b.y AS by_"
                + " FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    /** One row per entry, columns joined by '|', in the order the query answered. */
    private static String rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                int n = rs.getMetaData().getColumnCount();
                List<String> out = new ArrayList<>();
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) sb.append('|');
                        sb.append(rs.getString(i) == null ? "-" : rs.getString(i));
                    }
                    out.add(sb.toString());
                }
                return String.join(",", out);
            }
        }
    }

    private static final String UNMERGEABLE =
            "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions";

    private static void assertRejected(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql),
                "expected " + sql + " to be rejected");
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> " + e.getMessage() + " (wanted \"" + messagePart + "\")");
    }

    private static void assertUnmergeable(String sql) {
        assertRejected(sql, "0A000", UNMERGEABLE);
    }

    private static final String FULL = "SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y";

    // ---- 1: what the outermost query is still refused for ----

    @Nested
    class StillRefusedAtTheTop {

        @Test
        void theShapeTheWholeRuleExistsFor() {
            assertUnmergeable("SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y");
            assertUnmergeable("SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x > b.y");
            assertUnmergeable("SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x <> b.y");
            assertUnmergeable("SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.x >= b.y");
            assertUnmergeable("SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b"
                    + " ON a.x IS DISTINCT FROM b.y");
            assertUnmergeable("SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b"
                    + " ON a.x IS NOT DISTINCT FROM b.y");
            assertUnmergeable("SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b ON a.t LIKE b.t");
            assertUnmergeable("SELECT count(*) FROM fjn_a a FULL JOIN fjn_b b"
                    + " ON a.x = b.y OR a.t = b.t");
        }

        @Test
        void nothingWrittenAfterTheFromClauseGetsPastIt() {
            assertUnmergeable(FULL + " ORDER BY 1");
            assertUnmergeable(FULL + " LIMIT 0");
            assertUnmergeable(FULL + " OFFSET 0");
            assertUnmergeable(FULL + " GROUP BY GROUPING SETS ((a.x),(b.y))");
            assertUnmergeable(FULL + " GROUP BY a.x HAVING count(*) > 0");
        }

        @Test
        void norAJoinAboveWhoseConditionRejectsNothing() {
            assertUnmergeable(FULL + " CROSS JOIN fjn_c c");
            assertUnmergeable(FULL + " JOIN fjn_c c ON true");
            assertUnmergeable(FULL + ", fjn_c c");
            assertUnmergeable(FULL + " LEFT JOIN fjn_c c ON c.z = a.x");
            assertUnmergeable(FULL + " FULL JOIN fjn_c c ON c.z = a.x");
        }

        @Test
        void norAWhereThatToleratesANullRow() {
            assertUnmergeable(FULL + " WHERE true");
            assertUnmergeable(FULL + " WHERE a.x IS NULL");
            assertUnmergeable(FULL + " WHERE b.y IS NULL");
            assertUnmergeable(FULL + " WHERE coalesce(a.x,0) >= 0");
            assertUnmergeable(FULL + " WHERE a.x IS NULL AND b.y IS NULL");
        }
    }

    // ---- 2: what a qual of that one query proves ----

    @Nested
    class WhatAQualOfThatQueryProves {

        @Test
        void aQualNamingAThirdRelationProvesNothingAboutThisJoin() {
            assertUnmergeable(FULL + " JOIN fjn_c c ON c.z > 0");
            assertUnmergeable(FULL + ", fjn_c c WHERE c.z > 0");
            assertUnmergeable(FULL + " JOIN fjn_c c ON c.z IS NOT NULL");
        }

        @Test
        void anOrProvesWhatEveryArmOfItProves() throws SQLException {
            // Every arm rejects a null row of a, whichever of its columns each one reads.
            assertEquals("2", rows(FULL + " WHERE a.x = 1 OR a.t = 'b'"));
            assertEquals("2", rows(FULL + " WHERE a.x IS NOT NULL OR a.t IS NOT NULL"));
            assertEquals("2", rows(FULL + " WHERE a.x < 9 OR a.n < 9"));
            assertEquals("2", rows(FULL + " WHERE b.y = 3 OR b.t = 'c'"));
            assertEquals("2", rows(FULL + " WHERE a.x = 1 OR a.t = 'b' OR a.n = 2"));
            assertEquals("2", rows(FULL + " WHERE (a.x = 1 AND a.t = 'a') OR (a.x = 2 AND a.n = 2)"));
        }

        @Test
        void andDeMorganRunsBeforeItIsRead() throws SQLException {
            assertEquals("0", rows(FULL + " WHERE NOT (a.x > 0 AND a.t > '')"));
            assertEquals("2", rows(FULL + " WHERE NOT (a.x IS NULL OR a.t IS NULL)"));
        }

        @Test
        void anArmThatProvesNothingLeavesTheOrProvingNothing() {
            assertUnmergeable(FULL + " WHERE a.x IS NOT NULL OR b.y IS NOT NULL");
            assertUnmergeable(FULL + " WHERE a.x IS NOT NULL OR a.t IS NULL");
            assertUnmergeable(FULL + " WHERE a.x = 1 OR coalesce(a.t,'z') = 'z'");
            assertUnmergeable(FULL + " JOIN fjn_c c ON a.x > 0 OR b.y > 0");
        }

        @Test
        void aHavingClauseWithNoAggregateIsReadTheSameWay() throws SQLException {
            assertEquals("1|a,2|b", rows("SELECT a.x, a.t FROM fjn_a a FULL JOIN fjn_b b"
                    + " ON a.x < b.y GROUP BY a.x, a.t HAVING a.x > 0 OR a.t > '' ORDER BY 1"));
        }

        @Test
        void andSoIsTheOnConditionOfAnInnerJoinAbove() throws SQLException {
            assertEquals("4", rows("SELECT count(*) FROM (fjn_a a FULL JOIN fjn_b b ON a.x < b.y)"
                    + " JOIN fjn_c c ON a.x > 0 OR a.t > ''"));
            assertEquals("1", rows(FULL + " JOIN fjn_c c ON c.z = a.x"));
        }

        @Test
        void aNameNoRelationOfTheQueryAnswersToComesFirst() {
            assertRejected(FULL + " WHERE c.z = 1", "42P01",
                    "missing FROM-clause entry for table \"c\"");
            assertRejected(FULL + " WHERE a.nosuch = 1", "42703", "column a.nosuch does not exist");
            assertRejected(FULL + " JOIN fjn_c c ON c.nosuch = 1", "42703",
                    "column c.nosuch does not exist");
            assertRejected(FULL + ", fjn_c c WHERE nosuchcol = 1", "42703",
                    "column \"nosuchcol\" does not exist");
        }
    }

    // ---- 3: the reporting SQL two rounds of this refused ----

    @Nested
    class ReportingSqlThatMustNotBeRefused {

        private static final String INNER =
                "SELECT DISTINCT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y";

        @Test
        void aNonEquiFullJoinInsideASubqueryThatCannotBePulledUp() throws SQLException {
            assertEquals("2", rows("SELECT count(*) FROM (" + INNER + ") s"
                    + " WHERE s.ax IS NOT NULL"));
            assertEquals("2", rows("SELECT count(*) FROM (SELECT DISTINCT ON (a.x) a.x AS ax,"
                    + " b.y AS by_ FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y ORDER BY a.x) s"
                    + " WHERE s.ax IS NOT NULL"));
            assertEquals("2", rows("SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y GROUP BY 1) s WHERE s.ax IS NOT NULL"));
            assertEquals("2", rows("SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y GROUP BY 1 HAVING count(*) > 0) s"
                    + " WHERE s.ax IS NOT NULL"));
            assertEquals("2", rows("SELECT count(*) FROM (SELECT sum(a.n) AS s, a.x AS ax"
                    + " FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y GROUP BY a.x) t"
                    + " WHERE t.ax IS NOT NULL"));
            assertEquals("2", rows("SELECT count(*) FROM (SELECT a.x AS ax,"
                    + " row_number() OVER (PARTITION BY a.x) AS rn FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y) t WHERE t.ax IS NOT NULL"));
            assertEquals("3", rows("SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y UNION ALL SELECT 1) s"
                    + " WHERE s.ax IS NOT NULL"));
            assertEquals("2", rows("SELECT count(*) FROM (SELECT DISTINCT ax FROM"
                    + " (SELECT a.x AS ax FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y"
                    + " GROUP BY 1) u) t WHERE t.ax IS NOT NULL"));
        }

        @Test
        void andTheSameThingUnderAWithQueryAViewAndALateral() throws SQLException {
            assertEquals("2", rows("WITH t AS (" + INNER + ")"
                    + " SELECT count(*) FROM t WHERE t.ax IS NOT NULL"));
            assertEquals("2", rows("SELECT count(*) FROM fjn_vv WHERE ax > 0 OR at > ''"));
            assertEquals("2", rows("SELECT count(*) FROM fjn_vv WHERE ax IS NOT NULL"));
            assertEquals("4", rows("SELECT count(*) FROM fjn_c z, LATERAL (" + INNER + ") t"
                    + " WHERE t.ax IS NOT NULL"));
        }

        @Test
        void anOrdinaryEqualityFilterReducesTheJoinJustAsIsNotNullDoes() throws SQLException {
            assertEquals("1", rows("SELECT count(*) FROM (SELECT DISTINCT a.x AS ax, b.y AS by_"
                    + " FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) t"
                    + " WHERE t.ax = 1 AND t.by_ = 3"));
            assertEquals("2", rows("SELECT count(*) FROM (" + INNER + ") t WHERE t.ax IN (1,2)"));
            assertEquals("1", rows("SELECT count(*) FROM (SELECT DISTINCT a.t AS at FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y) t WHERE t.at LIKE 'a%'"));
        }

        @Test
        void theFilterMayArriveFromAJoinsOnClauseRatherThanAWhere() throws SQLException {
            assertEquals("0", rows("SELECT count(*) FROM fjn_c z JOIN (SELECT DISTINCT a.x AS ax,"
                    + " b.y AS by_ FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) t"
                    + " ON t.ax = z.z AND t.by_ = z.z"));
            assertEquals("2", rows("SELECT count(*) FROM fjn_c z LEFT JOIN (" + INNER + ") t"
                    + " ON t.ax = z.z"));
        }

        @Test
        void aSublinkIsPulledUpToASemijoinThatWasNeverSeenHere() throws SQLException {
            assertEquals("2", rows("SELECT count(*) FROM fjn_c c WHERE c.z IN"
                    + " (SELECT b.y FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y)"));
            assertEquals("1", rows("SELECT count(*) FROM fjn_c z WHERE EXISTS (SELECT 1 FROM ("
                    + INNER + ") t WHERE t.ax IS NOT NULL AND t.ax = z.z)"));
        }

        @Test
        void andAWritingStatementIsPlannedNoDifferently() throws SQLException {
            exec("UPDATE fjn_c SET z = z WHERE z IN"
                    + " (SELECT b.y FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y)");
            exec("DELETE FROM fjn_c WHERE z IN"
                    + " (SELECT b.y FROM fjn_a a FULL JOIN fjn_b b ON a.x < b.y) AND false");
            exec("INSERT INTO fjn_tgt(id, k) SELECT t.ax, 1 FROM (" + INNER + ") t"
                    + " WHERE t.ax IS NOT NULL");
            assertEquals("2", rows("SELECT count(*) FROM fjn_tgt WHERE k = 1"));
            exec("DELETE FROM fjn_tgt WHERE k = 1");
        }
    }

    // ---- 4: the reach given up ----

    @Nested
    class ReachGivenUp {

        /**
         * PostgreSQL refuses every one of these; memgres answers them. Asserted so that the price
         * of the narrowing is written down rather than discovered, and so that a later attempt to
         * win any of it back has to say so here.
         */
        @Test
        void postgresRefusesTheseAndMemgresAnswersThem() throws SQLException {
            assertEquals("2", rows("SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y LIMIT 10) s WHERE s.ax IS NOT NULL"));
            assertEquals("2", rows("SELECT count(*) FROM (SELECT a.x AS ax FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y OFFSET 0) s WHERE s.ax IS NOT NULL"));
            assertEquals("2", rows("WITH q AS MATERIALIZED (SELECT a.x AS ax FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y) SELECT count(*) FROM q"
                    + " WHERE ax IS NOT NULL"));
            assertEquals("6", rows("WITH q AS (SELECT a.x AS ax, b.y AS by_ FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y) SELECT count(*) FROM q p, q r"
                    + " WHERE p.ax IS NOT NULL AND r.by_ IS NOT NULL"));
            assertEquals("3", rows("SELECT (SELECT count(*) FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y)"));
            assertEquals("3", rows("SELECT count(*) FROM fjn_vv"));
            assertEquals("1,3", rows(FULL + " UNION ALL SELECT 1 ORDER BY 1"));
            exec("INSERT INTO fjn_tgt(id, k) SELECT a.x, 2 FROM fjn_a a"
                    + " FULL JOIN fjn_b b ON a.x < b.y");
            assertEquals("3", rows("SELECT count(*) FROM fjn_tgt WHERE k = 2"));
            exec("DELETE FROM fjn_tgt WHERE k = 2");
        }
    }
}
