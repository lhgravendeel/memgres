package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * How far the analysis reaches, and the order it reaches in.
 *
 * <p>Five things, each measured against PostgreSQL 18.
 *
 * <p><b>A SQL function's body is analysed when the function is written.</b> PostgreSQL parses and
 * analyses every statement in a {@code LANGUAGE sql} body at CREATE FUNCTION time, so a body that
 * reads an ungrouped column is refused there and not at the first call. The placement half of that
 * check was already here; the grouping half was not, and it needs the two things a running SELECT
 * has and a stored one does not — the select targets with stars expanded, and the relations the
 * FROM names. Measured, not assumed: a PL/pgSQL body is <em>not</em> analysed, because its
 * statements are strings the PL handler plans when the function runs, and neither kind is analysed
 * with {@code check_function_bodies} off. SECURITY DEFINER changes nothing.
 *
 * <p><b>A partition bound may not read a column.</b> A bound is a value the partition is given
 * once, with no row to read it from, so a name written in one is a column reference — which is also
 * why an enum label has to be quoted there. A bare name took the plain-literal path in the bound
 * parser and came back as a failed cast; only a multi-token bound went through the expression path,
 * which had no column check either. MODULUS and REMAINDER take an integer literal, so a name there
 * is a syntax error rather than a bad bound.
 *
 * <p><b>{@code TABLE t} is a query wherever a query may stand</b> — it is one of PostgreSQL's
 * simple_select productions, alongside SELECT and VALUES. Only the bare statement was read as one.
 *
 * <p><b>Which clause a query is refused for is the clause PostgreSQL reaches first</b>, and it
 * reads the FROM clause, then the select list, then WHERE, then HAVING, the window definitions,
 * ORDER BY, GROUP BY, and last LIMIT and OFFSET. It is <em>not</em> "WHERE first": a bare window
 * call in the select list beats an aggregate in WHERE. What was wrong was that the scan for a
 * window call written without OVER ran over the whole query ahead of the positional walk, so within
 * WHERE it named whichever call it happened to reach.
 *
 * <p><b>Both arms of a join name themselves to the query before its ON condition is read</b>, so a
 * name given twice is reported ahead of anything the condition holds — and a misplaced call in ON
 * is still reported rather than evaluated.
 *
 * <p>The last nested class is the reason to prefer narrow rules to broad ones: every one of these
 * shapes is SQL PostgreSQL runs, and each new refusal here is one more way to refuse it.
 */
class AnalysisReachTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE anr_t (id int PRIMARY KEY, a int, b text)");
        exec("INSERT INTO anr_t VALUES (1,10,'x'),(2,20,'y')");
        exec("CREATE TABLE anr_u (id int PRIMARY KEY, a int, c text)");
        exec("INSERT INTO anr_u VALUES (1,10,'p'),(2,30,'q')");
        exec("CREATE TABLE anr_range (id int, a int) PARTITION BY RANGE (id)");
        exec("CREATE TABLE anr_list (id int, a int) PARTITION BY LIST (a)");
        exec("CREATE TABLE anr_hash (id int, a int) PARTITION BY HASH (id)");
        exec("CREATE TABLE anr_text (id int, b text) PARTITION BY RANGE (b)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** One row per line, columns joined by '|', in the order the query answered. */
    private static String rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
            return String.join(",", out);
        }
    }

    private static void assertRejected(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql),
                "expected " + sql + " to be rejected");
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> " + e.getMessage() + " (wanted \"" + messagePart + "\")");
    }

    private static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    private static final String UNGROUPED = "must appear in the GROUP BY clause";
    private static final String NO_OVER = "window function row_number requires an OVER clause";
    private static final String AGG_WHERE = "aggregate functions are not allowed in WHERE";
    private static final String COLUMN_BOUND = "cannot use column reference in partition bound expression";

    // ---- 1: a SQL function body is analysed when the function is created ----

    @Nested
    class SqlFunctionBody {

        @Test
        void ungroupedColumnInBodyIsRefusedAtCreateTime() {
            assertRejected("CREATE FUNCTION anrf1() RETURNS text"
                    + " AS $$ SELECT b FROM anr_t GROUP BY a $$ LANGUAGE sql", "42803", UNGROUPED);
        }

        @Test
        void theMessageNamesTheRelationTheColumnReads() {
            assertRejected("CREATE FUNCTION anrf2() RETURNS text"
                    + " AS $$ SELECT x.b FROM anr_t x GROUP BY x.a $$ LANGUAGE sql",
                    "42803", "column \"x.b\" " + UNGROUPED);
        }

        @Test
        void havingAloneMakesTheBodyGrouped() {
            assertRejected("CREATE FUNCTION anrf3() RETURNS text"
                    + " AS $$ SELECT b FROM anr_t GROUP BY a HAVING count(*) > 1 $$ LANGUAGE sql",
                    "42803", UNGROUPED);
        }

        @Test
        void eachArmOfASetOperationIsJudgedAsItsOwnQuery() {
            assertRejected("CREATE FUNCTION anrf4() RETURNS text AS $$"
                    + " SELECT b FROM anr_t UNION ALL SELECT b FROM anr_t GROUP BY a $$ LANGUAGE sql",
                    "42803", UNGROUPED);
        }

        @Test
        void aProcedureBodyIsAnalysedTheSameWay() {
            assertRejected("CREATE PROCEDURE anrf5() LANGUAGE sql"
                    + " AS $$ SELECT b FROM anr_t GROUP BY a $$", "42803", UNGROUPED);
        }

        @Test
        void securityDefinerChangesWhoRunsItNotWhetherItIsAnalysed() {
            assertRejected("CREATE FUNCTION anrf6() RETURNS text"
                    + " AS $$ SELECT b FROM anr_t GROUP BY a $$ LANGUAGE sql SECURITY DEFINER",
                    "42803", UNGROUPED);
        }

        @Test
        void aBareWindowCallInABodyIsStillTheMissingOverClause() {
            assertRejected("CREATE FUNCTION anrf7() RETURNS bigint"
                    + " AS $$ SELECT row_number() FROM anr_t $$ LANGUAGE sql", "42809", NO_OVER);
        }

        /** Measured, and not the same answer: PostgreSQL plans a PL/pgSQL body when it runs. */
        @Test
        void aPlpgsqlBodyIsNotAnalysedUntilItRuns() throws Exception {
            exec("CREATE FUNCTION anrf8() RETURNS text LANGUAGE plpgsql"
                    + " AS $$ BEGIN RETURN (SELECT b FROM anr_t GROUP BY a); END $$");
            assertRejected("SELECT anrf8()", "42803", UNGROUPED);
            exec("DROP FUNCTION anrf8()");
        }

        @Test
        void checkFunctionBodiesOffStoresTheBodyUnjudged() throws Exception {
            exec("SET check_function_bodies = off");
            try {
                assertAccepted("CREATE FUNCTION anrf9() RETURNS text"
                        + " AS $$ SELECT b FROM anr_t GROUP BY a $$ LANGUAGE sql");
                exec("DROP FUNCTION anrf9()");
            } finally {
                exec("SET check_function_bodies = on");
            }
        }

        /** A relation that does not exist is still what a body naming one is refused for. */
        @Test
        void aBodyOverAMissingRelationIsRefusedForTheRelation() {
            assertRejected("CREATE FUNCTION anrf10() RETURNS bigint"
                    + " AS $$ SELECT count(*) FROM anr_nosuch $$ LANGUAGE sql",
                    "42P01", "relation \"anr_nosuch\" does not exist");
        }

        /** ...including one inside a CTE the body defines, which the name check has to descend into. */
        @Test
        void aMissingRelationInsideAWithClauseIsRefusedToo() {
            assertRejected("CREATE FUNCTION anrf11() RETURNS bigint AS $$"
                    + " WITH w AS (SELECT a FROM anr_nosuch) SELECT count(*) FROM w $$ LANGUAGE sql",
                    "42P01", "relation \"anr_nosuch\" does not exist");
        }
    }

    // ---- 2: a partition bound may not read a column ----

    @Nested
    class PartitionBound {

        @Test
        void aNameInARangeBoundIsAColumnReference() {
            assertRejected("CREATE TABLE anrp1 PARTITION OF anr_range FOR VALUES FROM (id) TO (10)",
                    "0A000", COLUMN_BOUND);
            assertRejected("CREATE TABLE anrp2 PARTITION OF anr_range FOR VALUES FROM (0) TO (id)",
                    "0A000", COLUMN_BOUND);
        }

        @Test
        void soIsOneWrittenInsideAnExpression() {
            assertRejected("CREATE TABLE anrp3 PARTITION OF anr_range FOR VALUES FROM (id + 1) TO (10)",
                    "0A000", COLUMN_BOUND);
            assertRejected(
                    "CREATE TABLE anrp4 PARTITION OF anr_range FOR VALUES FROM (anr_range.id) TO (10)",
                    "0A000", COLUMN_BOUND);
        }

        @Test
        void aBoundSettledOnceCannotReadAnotherRelationEither() {
            assertRejected("CREATE TABLE anrp5 PARTITION OF anr_range FOR VALUES FROM ((SELECT 1)) TO (10)",
                    "0A000", "cannot use subquery in partition bound");
        }

        @Test
        void aNameInAListBound() {
            assertRejected("CREATE TABLE anrp6 PARTITION OF anr_list FOR VALUES IN (a)",
                    "0A000", COLUMN_BOUND);
            assertRejected("CREATE TABLE anrp7 PARTITION OF anr_list FOR VALUES IN (1, id)",
                    "0A000", COLUMN_BOUND);
        }

        /** A text bound used to accept the name and create the partition, which is worse than a bad cast. */
        @Test
        void aNameInATextBoundIsNotAStringValue() {
            assertRejected("CREATE TABLE anrp8 PARTITION OF anr_text FOR VALUES FROM (b) TO ('m')",
                    "0A000", COLUMN_BOUND);
        }

        @Test
        void modulusAndRemainderTakeAnIntegerLiteral() {
            assertRejected("CREATE TABLE anrp9 PARTITION OF anr_hash"
                    + " FOR VALUES WITH (MODULUS id, REMAINDER 0)", "42601", "syntax error at or near \"id\"");
            assertRejected("CREATE TABLE anrp10 PARTITION OF anr_hash"
                    + " FOR VALUES WITH (MODULUS 4.5, REMAINDER 0)", "42601", "syntax error at or near \"4.5\"");
        }

        @Test
        void attachPartitionReadsTheSameBounds() throws Exception {
            exec("CREATE TABLE anr_ap (id int, a int) PARTITION BY RANGE (id)");
            exec("CREATE TABLE anr_att (id int, a int)");
            assertRejected("ALTER TABLE anr_ap ATTACH PARTITION anr_att FOR VALUES FROM (id) TO (10)",
                    "0A000", COLUMN_BOUND);
            assertAccepted("ALTER TABLE anr_ap ATTACH PARTITION anr_att FOR VALUES FROM (0) TO (10)");
            exec("ALTER TABLE anr_ap DETACH PARTITION anr_att");
            exec("DROP TABLE anr_att");
            exec("DROP TABLE anr_ap");
        }

        /** Every bound spelling that names a value rather than a column. */
        @Test
        void everyValueBoundStillWorks() throws Exception {
            String[][] shapes = {
                {"anrq1", "anr_range", "FOR VALUES FROM (0) TO (10)"},
                {"anrq2", "anr_range", "FOR VALUES FROM (MINVALUE) TO (0)"},
                {"anrq3", "anr_range", "FOR VALUES FROM (100) TO (MAXVALUE)"},
                {"anrq4", "anr_range", "FOR VALUES FROM (11) TO (12)"},
                {"anrq5", "anr_range", "FOR VALUES FROM (1 + 20) TO (30)"},
                {"anrq6", "anr_range", "FOR VALUES FROM (abs(-50)) TO (60)"},
                {"anrq7", "anr_range", "DEFAULT"},
                {"anrq8", "anr_list", "FOR VALUES IN (1, 2)"},
                {"anrq9", "anr_list", "FOR VALUES IN (NULL)"},
                {"anrq10", "anr_list", "FOR VALUES IN (3 + 4)"},
                {"anrq11", "anr_list", "DEFAULT"},
                {"anrq12", "anr_hash", "FOR VALUES WITH (MODULUS 4, REMAINDER 0)"},
                {"anrq13", "anr_text", "FOR VALUES FROM ('a') TO ('m')"},
                {"anrq14", "anr_text", "FOR VALUES FROM ('m'::text) TO ('z')"},
            };
            for (String[] shape : shapes) {
                assertAccepted("CREATE TABLE " + shape[0] + " PARTITION OF " + shape[1] + " " + shape[2]);
            }
            for (String[] shape : shapes) exec("DROP TABLE " + shape[0]);
        }
    }

    // ---- 3: TABLE t is a query wherever a query may stand ----

    @Nested
    class TableAsQuery {

        @Test
        void asAStatement() throws Exception {
            assertEquals("1|10|x,2|20|y", rows("TABLE anr_t ORDER BY id"));
        }

        @Test
        void withTheClausesThatHangOffAQueryRatherThanASelectList() throws Exception {
            assertEquals("1|10|x", rows("TABLE anr_t ORDER BY id LIMIT 1"));
            assertEquals("2|20|y", rows("TABLE anr_t ORDER BY id LIMIT 1 OFFSET 1"));
            assertEquals("1|10|x", rows("TABLE anr_t ORDER BY id FETCH FIRST 1 ROW ONLY"));
            assertEquals("1|10|x,2|20|y", rows("TABLE anr_t ORDER BY id FOR UPDATE"));
        }

        @Test
        void asAViewBody() throws Exception {
            exec("CREATE VIEW anr_vt AS TABLE anr_t");
            assertEquals("1|10|x,2|20|y", rows("SELECT * FROM anr_vt ORDER BY id"));
            exec("DROP VIEW anr_vt");
        }

        @Test
        void asAViewBodyWithAColumnList() throws Exception {
            exec("CREATE VIEW anr_vt2 (i, j, k) AS TABLE anr_t");
            assertEquals("1|10|x,2|20|y", rows("SELECT i, j, k FROM anr_vt2 ORDER BY i"));
            exec("DROP VIEW anr_vt2");
        }

        @Test
        void asAMaterializedViewBody() throws Exception {
            exec("CREATE MATERIALIZED VIEW anr_mv AS TABLE anr_t");
            assertEquals("1|10|x,2|20|y", rows("SELECT * FROM anr_mv ORDER BY id"));
            exec("DROP MATERIALIZED VIEW anr_mv");
        }

        @Test
        void asASetOperationArm() throws Exception {
            assertEquals("1|10|x,2|20|y", rows("SELECT id, a, b FROM anr_t UNION TABLE anr_t ORDER BY id"));
            assertEquals("1|10|x,2|20|y", rows("TABLE anr_t EXCEPT TABLE anr_u ORDER BY id"));
            assertEquals("1|10|x,2|20|y", rows("TABLE anr_t INTERSECT TABLE anr_t ORDER BY id"));
            assertEquals("1|10|x,2|20|y", rows("TABLE anr_t UNION TABLE anr_t ORDER BY id"));
        }

        @Test
        void asACteBody() throws Exception {
            assertEquals("1|10|x,2|20|y", rows("WITH w AS (TABLE anr_t) SELECT * FROM w ORDER BY id"));
            assertEquals("1|10|x,2|20|y",
                    rows("WITH RECURSIVE w AS (TABLE anr_t) SELECT * FROM w ORDER BY id"));
        }

        @Test
        void asASubQuery() throws Exception {
            assertEquals("1|10|x,2|20|y", rows("SELECT * FROM (TABLE anr_t) s ORDER BY id"));
            assertEquals("1|10|x,2|20|y", rows("SELECT * FROM (TABLE anr_t) AS s(i, j, k) ORDER BY i"));
            assertEquals("t", rows("SELECT EXISTS (TABLE anr_t)"));
        }

        @Test
        void asAnInsertSource() throws Exception {
            exec("CREATE TABLE anr_ins (id int, a int, b text)");
            exec("INSERT INTO anr_ins TABLE anr_t");
            assertEquals("2", rows("SELECT count(*) FROM anr_ins"));
            exec("DROP TABLE anr_ins");
        }

        @Test
        void asACursorQuery() throws Exception {
            conn.setAutoCommit(false);
            try {
                exec("DECLARE anr_cur CURSOR FOR TABLE anr_t");
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("FETCH ALL FROM anr_cur")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
                exec("CLOSE anr_cur");
                conn.commit();
            } finally {
                conn.setAutoCommit(true);
            }
        }

        @Test
        void asACreateTableAsBody() throws Exception {
            exec("CREATE TABLE anr_ctas AS TABLE anr_t");
            assertEquals("2", rows("SELECT count(*) FROM anr_ctas"));
            exec("DROP TABLE anr_ctas");
        }

        @Test
        void aTableThatDoesNotExistIsStillReportedAsOne() {
            assertRejected("TABLE anr_nosuch", "42P01", "relation \"anr_nosuch\" does not exist");
        }
    }

    // ---- 4: the clause a query is refused for is the one reached first ----

    @Nested
    class ClauseOrder {

        /** The select list is only a star, so WHERE is next and read left to right. */
        @Test
        void withinWhereTheCallWrittenFirstIsTheOneNamed() {
            assertRejected("SELECT * FROM anr_t WHERE count(*) > 1 AND row_number() = 1",
                    "42803", AGG_WHERE);
        }

        /** And the other way round: the select list is read before WHERE. */
        @Test
        void theSelectListIsReadBeforeWhere() {
            assertRejected("SELECT row_number() FROM anr_t WHERE count(*) > 1", "42809", NO_OVER);
            assertRejected("SELECT nosuchcol FROM anr_t WHERE row_number() = 1",
                    "42703", "column \"nosuchcol\" does not exist");
            assertRejected("SELECT nosuchcol FROM anr_t WHERE count(*) > 1",
                    "42703", "column \"nosuchcol\" does not exist");
        }

        /** An aggregate in the select list is where an aggregate belongs, so WHERE decides. */
        @Test
        void whereIsReadWhenTheSelectListHasNothingWrongWithIt() {
            assertRejected("SELECT count(*) FROM anr_t WHERE row_number() = 1", "42809", NO_OVER);
            assertRejected("SELECT count(*) FROM anr_t WHERE count(*) > 1", "42803", AGG_WHERE);
            assertRejected("SELECT a FROM anr_t WHERE row_number() OVER () = 1",
                    "42P20", "window functions are not allowed in WHERE");
        }

        @Test
        void whereIsReadBeforeEveryClauseThatFollowsIt() {
            assertRejected("SELECT * FROM anr_t WHERE count(*) > 1 GROUP BY a", "42803", AGG_WHERE);
            assertRejected("SELECT * FROM anr_t WHERE count(*) > 1 HAVING count(*) > 1", "42803", AGG_WHERE);
            assertRejected("SELECT * FROM anr_t WHERE count(*) > 1 ORDER BY a", "42803", AGG_WHERE);
            assertRejected("SELECT * FROM anr_t WHERE count(*) > 1 LIMIT count(*)", "42803", AGG_WHERE);
            assertRejected("SELECT * FROM anr_t WHERE count(*) > 1 OFFSET count(*)", "42803", AGG_WHERE);
        }

        /** Those clauses are still judged when WHERE has nothing wrong with it. */
        @Test
        void theClausesAfterWhereAreStillReached() {
            assertRejected("SELECT * FROM anr_t ORDER BY row_number()", "42809", NO_OVER);
            assertRejected("SELECT * FROM anr_t GROUP BY row_number()", "42809", NO_OVER);
            assertRejected("SELECT * FROM anr_t LIMIT row_number()", "42809", NO_OVER);
            assertRejected("SELECT * FROM anr_t OFFSET row_number()", "42809", NO_OVER);
            assertRejected("SELECT * FROM anr_t LIMIT count(*)",
                    "42803", "aggregate functions are not allowed in LIMIT");
            assertRejected("SELECT * FROM anr_t OFFSET count(*)",
                    "42803", "aggregate functions are not allowed in OFFSET");
            assertRejected("SELECT * FROM anr_t GROUP BY count(*)",
                    "42803", "aggregate functions are not allowed in GROUP BY");
            assertRejected("SELECT * FROM anr_t GROUP BY row_number() OVER ()",
                    "42P20", "window functions are not allowed in GROUP BY");
            assertRejected("SELECT count(*) FROM anr_t HAVING row_number() OVER () = 1",
                    "42P20", "window functions are not allowed in HAVING");
        }

        @Test
        void aBareWindowCallInTheSelectListIsStillReached() {
            assertRejected("SELECT row_number() FROM anr_t", "42809", NO_OVER);
            assertRejected("SELECT row_number() FROM anr_t GROUP BY a", "42809", NO_OVER);
            assertRejected("SELECT 1 + row_number() FROM anr_t", "42809", NO_OVER);
        }

        @Test
        void theSameOrderInDml() {
            assertRejected("DELETE FROM anr_t WHERE count(*) = 1", "42803", AGG_WHERE);
            assertRejected("UPDATE anr_t SET a = 1 WHERE count(*) = 1", "42803", AGG_WHERE);
        }
    }

    // ---- 5: a duplicate table name is reported before what the ON clause holds ----

    @Nested
    class DuplicateAlias {

        @Test
        void theDuplicateNameIsFoundBeforeTheOnCondition() {
            assertRejected("SELECT * FROM anr_t x JOIN anr_u x ON count(*) = 1",
                    "42712", "table name \"x\" specified more than once");
            assertRejected("SELECT * FROM anr_t x JOIN anr_u x ON row_number() = 1",
                    "42712", "table name \"x\" specified more than once");
            assertRejected("SELECT * FROM anr_t x JOIN anr_u x ON nosuchcol = 1",
                    "42712", "table name \"x\" specified more than once");
        }

        @Test
        void whateverKindOfJoinItIs() {
            assertRejected("SELECT * FROM anr_t x LEFT JOIN anr_u x ON true",
                    "42712", "table name \"x\" specified more than once");
            assertRejected("SELECT * FROM anr_t x CROSS JOIN anr_u x",
                    "42712", "table name \"x\" specified more than once");
            assertRejected("SELECT * FROM anr_t x JOIN anr_u x USING (id)",
                    "42712", "table name \"x\" specified more than once");
            assertRejected("SELECT * FROM anr_t x NATURAL JOIN anr_u x",
                    "42712", "table name \"x\" specified more than once");
            assertRejected("SELECT * FROM anr_t x JOIN (SELECT * FROM anr_u) x ON true",
                    "42712", "table name \"x\" specified more than once");
            assertRejected("SELECT * FROM anr_t, anr_t",
                    "42712", "table name \"anr_t\" specified more than once");
        }

        /** With distinct names there is no clash, and a misplaced call in ON is still reported. */
        @Test
        void aMisplacedCallInOnIsStillReportedRatherThanEvaluated() {
            assertRejected("SELECT * FROM anr_t x JOIN anr_u y ON count(*) = 1",
                    "42803", "aggregate functions are not allowed in JOIN conditions");
            assertRejected("SELECT * FROM anr_t x JOIN anr_u y ON count(*) OVER () = 1",
                    "42P20", "window functions are not allowed in JOIN conditions");
            assertRejected("SELECT * FROM anr_t x JOIN anr_u y ON row_number() = 1", "42809", NO_OVER);
        }

        /** A relation that does not exist is reported before the duplicate name, as it was. */
        @Test
        void anUnresolvableRelationIsStillReportedFirst() {
            assertRejected("SELECT * FROM anr_nosuch x JOIN anr_nosuch2 x ON true",
                    "42P01", "relation \"anr_nosuch\" does not exist");
        }
    }

    // ---- The shape of ordinary SQL, which has to keep working ----

    @Nested
    class OrdinarySql {

        @Test
        void functionBodiesThatArePerfectlyOrdinaryQueries() throws Exception {
            String[][] fns = {
                {"anro1()", "bigint", "SELECT count(*) FROM anr_t GROUP BY a LIMIT 1"},
                {"anro2()", "bigint", "SELECT row_number() OVER (ORDER BY id) FROM anr_t LIMIT 1"},
                {"anro3()", "text", "SELECT t.b FROM anr_t t JOIN anr_u u ON t.id = u.id LIMIT 1"},
                {"anro4(p int)", "bigint", "SELECT count(*) FROM anr_t WHERE a = p GROUP BY a"},
                {"anro5()", "bigint", "WITH w AS (SELECT a FROM anr_t) SELECT count(*) FROM w"},
                {"anro6()", "bigint", "SELECT count(*) FROM pg_class"},
                {"anro7()", "bigint",
                        "SELECT count(*) FROM anr_t GROUP BY GROUPING SETS ((a), (b)) LIMIT 1"},
                {"anro8()", "bigint",
                        "SELECT count(*) FROM anr_t t1, anr_t t2 WHERE t1.id = t2.id GROUP BY t1.a LIMIT 1"},
                {"anro9()", "bigint", "SELECT count(*) FROM (VALUES (1), (2)) v(x)"},
                {"anro10()", "bigint", "SELECT count(*) FROM generate_series(1, 3) g"},
                {"anro11()", "bigint", "SELECT count(DISTINCT a) FROM anr_t"},
                {"anro12()", "bigint",
                        "SELECT count(*) FROM anr_t t GROUP BY t.a ORDER BY t.a LIMIT 1"},
                {"anro13()", "SETOF text", "SELECT b FROM anr_t"},
                {"anro14()", "TABLE(z text)", "SELECT b FROM anr_t"},
                {"anro15()", "bigint", "SELECT count(*) FROM anr_t x JOIN anr_u y ON x.id = y.id"},
            };
            for (String[] fn : fns) {
                assertAccepted("CREATE FUNCTION " + fn[0] + " RETURNS " + fn[1]
                        + " AS $$ " + fn[2] + " $$ LANGUAGE sql");
            }
            // ...and answer what they should
            assertEquals("1", rows("SELECT anro1()"));
            assertEquals("x", rows("SELECT anro3()"));
            assertEquals("2", rows("SELECT anro5()"));
            assertEquals("2", rows("SELECT anro9()"));
            assertEquals("3", rows("SELECT anro10()"));
            assertEquals("2", rows("SELECT anro11()"));
            assertEquals("x,y", rows("SELECT * FROM anro13()"));
            assertEquals("x,y", rows("SELECT z FROM anro14()"));
            assertEquals("2", rows("SELECT anro15()"));

            for (String[] fn : fns) {
                String sig = fn[0].replace("p int", "int");
                exec("DROP FUNCTION " + sig);
            }
        }

        @Test
        void aPlpgsqlBodyOverAnOrdinaryQuery() throws Exception {
            exec("CREATE FUNCTION anro20() RETURNS bigint LANGUAGE plpgsql"
                    + " AS $$ BEGIN RETURN (SELECT count(*) FROM anr_t); END $$");
            assertEquals("2", rows("SELECT anro20()"));
            exec("DROP FUNCTION anro20()");
        }

        @Test
        void aBodyOverAView() throws Exception {
            exec("CREATE VIEW anr_ov AS SELECT id, a, b FROM anr_t");
            exec("CREATE FUNCTION anro21() RETURNS bigint"
                    + " AS $$ SELECT count(*) FROM anr_ov $$ LANGUAGE sql");
            assertEquals("2", rows("SELECT anro21()"));
            exec("DROP FUNCTION anro21()");
            exec("DROP VIEW anr_ov");
        }

        @Test
        void joinsWithDistinctAliasesSelfJoinsAndNoAliasAtAll() throws Exception {
            assertEquals("x|p", rows("SELECT t.b, u.c FROM anr_t t JOIN anr_u u ON t.a = u.a"));
            assertEquals("1|1,2|2",
                    rows("SELECT x.id, y.id FROM anr_t x JOIN anr_t y ON x.id = y.id ORDER BY x.id"));
            assertEquals("x|p,y|q",
                    rows("SELECT anr_t.b, anr_u.c FROM anr_t JOIN anr_u ON anr_t.id = anr_u.id"
                            + " ORDER BY anr_t.id"));
            assertEquals("1,2",
                    rows("SELECT x.id FROM anr_t x JOIN (SELECT * FROM anr_u) y ON x.id = y.id"
                            + " ORDER BY x.id"));
            assertEquals("1|10|x|1|10|p,2|20|y|2|30|q",
                    rows("SELECT * FROM anr_t LEFT JOIN anr_u ON anr_t.id = anr_u.id ORDER BY anr_t.id"));
            assertEquals("1|10|x,2|20|y",
                    rows("SELECT t.* FROM anr_t t JOIN anr_u u USING (id) ORDER BY t.id"));
        }

        @Test
        void whereClausesHoldingNeitherAnAggregateNorAWindowCall() throws Exception {
            assertEquals("y", rows("SELECT b FROM anr_t WHERE a > 15"));
            assertEquals("x", rows("SELECT b FROM anr_t WHERE a = (SELECT min(a) FROM anr_u)"));
            assertEquals("x,y", rows("SELECT b FROM anr_t WHERE id IN (1, 2) ORDER BY id"));
            assertEquals("2,2", rows("SELECT count(*) OVER () FROM anr_t WHERE a > 0"));
            assertEquals("10|1,20|1",
                    rows("SELECT a, count(*) FROM anr_t WHERE a > 0 GROUP BY a"
                            + " HAVING count(*) > 0 ORDER BY a LIMIT 5"));
        }
    }
}
