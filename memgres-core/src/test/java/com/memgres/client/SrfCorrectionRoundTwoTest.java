package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Where a set may be produced, what the item that produces it exposes, and what a join's merged
 * column is — measured against PostgreSQL 18.
 *
 * <p><b>WITH ORDINALITY is one column, added once.</b> It numbers the rows a FROM item produced,
 * whatever produced them, so it is the same bigint counting from 1 in every case. Each resolver
 * used to add it for itself, which meant the ones nobody had asked about it —
 * {@code string_to_table}, {@code regexp_split_to_table}, {@code json_object_keys},
 * {@code generate_subscripts}, {@code regexp_matches}, a function returning TABLE — dropped it, and
 * the alias list that named it then had one name too many for the columns that were left. It is
 * added in one place now, and the clause is refused beside a column definition list, which
 * describes the same alias list a second way.
 *
 * <p><b>A set expands where rows are still being produced and nowhere else.</b> The clauses that
 * read rows already produced took one: a DELETE's WHERE, the WHERE of an ON CONFLICT DO UPDATE, a
 * FILTER, a VALUES list, and every clause of a MERGE. Each refusal here names the clause
 * PostgreSQL names — except a MERGE's INSERT, which writes one row for the source row that reached
 * it and where PostgreSQL names the value rather than the clause. The one-row VALUES of an INSERT
 * is the shape that does expand, and it still does.
 *
 * <p><b>A declared set-returning function is one too.</b> {@code RETURNS SETOF} and
 * {@code RETURNS TABLE} answer a set for the same reason {@code generate_series} does, and
 * PostgreSQL treats them alike everywhere. Recognising only the built-in names left
 * {@code SELECT setof_fn()} answering one row holding the whole set rendered as an array, and
 * described it as text.
 *
 * <p><b>DISTINCT reads what the expansion produced.</b> Both DISTINCT and DISTINCT ON ran before
 * the sets were expanded, so a duplicate the expansion created survived and a DISTINCT ON key that
 * was a set grouped rows that did not exist yet.
 *
 * <p><b>A join's merged column is neither side's.</b> PostgreSQL resolves one type both sides can
 * be read as — for the numeric types the wider of the two — and the merged column is that type,
 * which is also the type the comparison is made in. Reading the left side's instead described
 * {@code int JOIN bigint USING (k)} as int4 and would not match 1 against 1.0. A pair of types with
 * no {@code =} between them is refused with the hint PostgreSQL attaches, and so is a type with no
 * equality operator of its own.
 *
 * <p><b>A name that is written but out of reach is not a missing one.</b> The relations under
 * {@code (a JOIN b) AS j} and a sibling FROM item read from a subquery that is not LATERAL are both
 * there and unreachable; PostgreSQL says so, and says which word would bring the second into reach.
 * A near-miss column name is suggested qualified by the relation that has it, one suggestion per
 * relation.
 *
 * <p>Every rule added here was measured on the shapes around it first, which is why the ordinary
 * forms are asserted beside each refusal.
 */
class SrfCorrectionRoundTwoTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE sc2_t (a int, b text)");
        exec("INSERT INTO sc2_t VALUES (1,'a'),(2,'b')");
        exec("CREATE TABLE sc2_tgt (id int primary key, name text, dept_id int)");
        exec("CREATE TABLE sc2_i (k int, iv text)");
        exec("INSERT INTO sc2_i VALUES (1,'i'),(2,'i2')");
        exec("CREATE TABLE sc2_l (k bigint, lv text)");
        exec("INSERT INTO sc2_l VALUES (1,'l'),(3,'l3')");
        exec("CREATE TABLE sc2_n (k numeric, nv text)");
        exec("INSERT INTO sc2_n VALUES (1,'n')");
        exec("CREATE TABLE sc2_r (k real, rv text)");
        exec("INSERT INTO sc2_r VALUES (1,'r')");
        exec("CREATE TABLE sc2_tx (k text, xv text)");
        exec("CREATE TABLE sc2_d (k date, dv text)");
        exec("CREATE TABLE sc2_j1 (k int, js json, ar int[])");
        exec("INSERT INTO sc2_j1 VALUES (1,'{\"a\":1}',ARRAY[1,2])");
        exec("CREATE TABLE sc2_j2 (k int, js json, ar int[])");
        exec("INSERT INTO sc2_j2 VALUES (1,'{\"a\":1}',ARRAY[1,2])");
        exec("CREATE TABLE sc2_a (id int primary key, x int, t text)");
        exec("CREATE TABLE sc2_b (id int primary key, y int, t text)");
        exec("CREATE FUNCTION sc2_setofint() RETURNS SETOF int AS $$ "
                + "SELECT 1 UNION ALL SELECT 2 $$ LANGUAGE sql");
        exec("CREATE FUNCTION sc2_tbl() RETURNS TABLE(x int, y text) AS $$ "
                + "SELECT 1, 'p' UNION ALL SELECT 2, 'q' $$ LANGUAGE sql");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            for (String t : new String[]{"sc2_t", "sc2_tgt", "sc2_i", "sc2_l", "sc2_n", "sc2_r",
                    "sc2_tx", "sc2_d", "sc2_j1", "sc2_j2", "sc2_a", "sc2_b"}) {
                exec("DROP TABLE IF EXISTS " + t + " CASCADE");
            }
            exec("DROP FUNCTION IF EXISTS sc2_setofint()");
            exec("DROP FUNCTION IF EXISTS sc2_tbl()");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void reset() throws Exception {
        exec("DELETE FROM sc2_t");
        exec("INSERT INTO sc2_t VALUES (1,'a'),(2,'b')");
        exec("DELETE FROM sc2_tgt");
        exec("INSERT INTO sc2_tgt VALUES (1,'x',9)");
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    /** Every row of a query, as {@code col|col} strings, in the order the query answered them. */
    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
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
        }
        return out;
    }

    /** The column labels and type names a query describes its result with. */
    private static String shape(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                ResultSetMetaData md = rs.getMetaData();
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) sb.append(", ");
                    sb.append(md.getColumnLabel(i)).append(':').append(md.getColumnTypeName(i));
                }
                return sb.toString();
            }
        }
    }

    private static String one(String sql) throws SQLException {
        List<String> r = rows(sql);
        assertEquals(1, r.size(), sql);
        return r.get(0);
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    private static void assertRefused(String sql, String state, String message) {
        SQLException e = refused(sql);
        assertEquals(state, e.getSQLState(), sql);
        assertTrue(e.getMessage().contains(message),
                sql + " -> " + e.getMessage());
    }

    private static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    // ---------------------------------------------------------------- ordinality

    @Nested
    class OrdinalityIsOneColumnAddedOnce {

        @Test
        void everySetReturningItemGetsIt() throws Exception {
            assertEquals("v:text, n:int8",
                    shape("SELECT * FROM string_to_table('x,y', ',') WITH ORDINALITY AS t(v, n)"));
            assertEquals("[x|1, y|2]",
                    rows("SELECT * FROM string_to_table('x,y', ',') WITH ORDINALITY AS t(v, n)")
                            .toString());
            assertEquals("[x|1, y|2]",
                    rows("SELECT * FROM regexp_split_to_table('x,y', ',') WITH ORDINALITY AS t(v, n)")
                            .toString());
            assertEquals("[a|1, b|2]",
                    rows("SELECT * FROM json_object_keys('{\"a\":1,\"b\":2}'::json) "
                            + "WITH ORDINALITY AS t(v, n)").toString());
            assertEquals("[1|1, 2|2]",
                    rows("SELECT * FROM generate_subscripts(ARRAY[5,6], 1) "
                            + "WITH ORDINALITY AS t(v, n)").toString());
            assertEquals("[7|1, 8|2]",
                    rows("SELECT * FROM unnest(ARRAY[7,8]) WITH ORDINALITY AS t(v, n)").toString());
        }

        @Test
        void aFunctionReturningTableGetsItAfterItsOwnColumns() throws Exception {
            assertEquals("x:int4, y:text, ordinality:int8",
                    shape("SELECT * FROM sc2_tbl() WITH ORDINALITY"));
            assertEquals("[1|p|1, 2|q|2]",
                    rows("SELECT * FROM sc2_tbl() WITH ORDINALITY ORDER BY 1").toString());
            assertEquals("[1|p|1, 2|q|2]",
                    rows("SELECT * FROM sc2_tbl() WITH ORDINALITY AS t(a,b,n) ORDER BY 3").toString());
        }

        @Test
        void unnamedItKeepsPostgresName() throws Exception {
            assertEquals("string_to_table:text, ordinality:int8",
                    shape("SELECT * FROM string_to_table('x,y', ',') WITH ORDINALITY"));
            assertEquals("generate_subscripts:int4, ordinality:int8",
                    shape("SELECT * FROM generate_subscripts(ARRAY[5,6],1) WITH ORDINALITY"));
        }

        @Test
        void itIsBigintEvenWhereNoRowShowsIt() throws Exception {
            assertEquals("v:int4, n:int8",
                    shape("SELECT * FROM generate_series(1,0) WITH ORDINALITY AS t(v,n)"));
            assertEquals("v:text, n:int8",
                    shape("SELECT * FROM string_to_table('',',') WITH ORDINALITY AS t(v,n)"));
        }

        @Test
        void aColumnDefinitionListDescribesTheSameThingTwice() {
            SQLException e = refused("SELECT * FROM json_to_recordset('[{\"a\":1}]'::json) "
                    + "WITH ORDINALITY AS t(a int, n bigint)");
            assertEquals("42601", e.getSQLState());
            assertTrue(e.getMessage().contains(
                    "WITH ORDINALITY cannot be used with a column definition list"), e.getMessage());
            assertTrue(e.getMessage().contains("Put the column definition list inside ROWS FROM()"),
                    e.getMessage());
        }

        @Test
        void ordinarySql() throws Exception {
            // A plain alias list, with and without the clause
            assertEquals("[x, y]", rows("SELECT * FROM string_to_table('x,y', ',') AS t(v)").toString());
            assertEquals("[1, 2]", rows("SELECT * FROM generate_series(1,2) AS t(v)").toString());
            assertEquals("[5|1, 6|2]",
                    rows("SELECT * FROM generate_series(5,6) WITH ORDINALITY AS t(v,n) ORDER BY 1")
                            .toString());
            // Several functions side by side, numbered once for the whole item
            assertEquals("[1|x|1, 2|y|2]",
                    rows("SELECT * FROM ROWS FROM (generate_series(1,2), string_to_table('x,y',',')) "
                            + "WITH ORDINALITY AS t(a,b,n) ORDER BY 3").toString());
            assertEquals("[1|a|1, 2|b|2]",
                    rows("SELECT * FROM unnest(ARRAY[1,2], ARRAY['a','b']) "
                            + "WITH ORDINALITY AS t(a,b,n) ORDER BY 3").toString());
            // An alias list longer than the item's columns is still refused
            assertRefused("SELECT * FROM generate_series(1,2) AS t(v,n)", "42P10",
                    "table \"t\" has 1 columns available but 2 columns specified");
        }
    }

    // ---------------------------------------------------------------- placement

    @Nested
    class WhereASetMayNotStand {

        @Test
        void aDeleteWhere() {
            assertRefused("DELETE FROM sc2_t WHERE generate_series(1,2) > 5", "0A000",
                    "set-returning functions are not allowed in WHERE");
        }

        @Test
        void theWhereOfAnOnConflictDoUpdate() {
            assertRefused("INSERT INTO sc2_tgt(id, name) VALUES (1,'a') ON CONFLICT (id) "
                            + "DO UPDATE SET name = 'b' WHERE generate_series(1,2) > 1", "0A000",
                    "set-returning functions are not allowed in WHERE");
        }

        @Test
        void aFilterCondition() {
            assertRefused("SELECT count(*) FILTER (WHERE generate_series(1,2) > 1) FROM sc2_t",
                    "0A000", "set-returning functions are not allowed in FILTER");
            assertRefused("SELECT sum(a) FILTER (WHERE generate_series(1,2) > 0) FROM sc2_t",
                    "0A000", "set-returning functions are not allowed in FILTER");
        }

        @Test
        void aValuesList() {
            assertRefused("VALUES (generate_series(1,3))", "0A000",
                    "set-returning functions are not allowed in VALUES");
            assertRefused("SELECT * FROM (VALUES (generate_series(1,2))) v(x)", "0A000",
                    "set-returning functions are not allowed in VALUES");
        }

        @Test
        void everyClauseOfAMerge() throws Exception {
            assertRefused("MERGE INTO sc2_tgt t USING (SELECT 1 AS id) s "
                            + "ON t.id = s.id AND generate_series(1,2) > 0 "
                            + "WHEN MATCHED THEN UPDATE SET name = 'q'", "0A000",
                    "set-returning functions are not allowed in JOIN conditions");
            assertRefused("MERGE INTO sc2_tgt t USING (SELECT 1 AS id) s ON t.id = s.id "
                            + "WHEN MATCHED AND generate_series(1,2) > 0 THEN UPDATE SET name = 'q'",
                    "0A000", "set-returning functions are not allowed in MERGE WHEN conditions");
            assertRefused("MERGE INTO sc2_tgt t USING (SELECT 1 AS id) s ON t.id = s.id "
                            + "WHEN MATCHED THEN UPDATE SET name = generate_series(1,2)::text",
                    "0A000", "set-returning functions are not allowed in UPDATE");
            exec("DELETE FROM sc2_tgt");
            // A MERGE's INSERT writes one row for the source row that reached it, and PostgreSQL
            // names what the value is rather than which clause holds it.
            assertRefused("MERGE INTO sc2_tgt t USING (SELECT 1 AS id) s ON t.id = s.id "
                            + "WHEN NOT MATCHED THEN INSERT (id, dept_id) "
                            + "VALUES (s.id, generate_series(1,2))", "0A000",
                    "set-valued function called in context that cannot accept a set");
        }

        @Test
        void aSetInAFromFunctionsArguments() {
            assertRefused("SELECT * FROM generate_series(1, generate_series(1,2))", "0A000",
                    "set-returning functions must appear at top level of FROM");
            assertRefused("SELECT * FROM unnest(ARRAY[generate_series(1,2)])", "0A000",
                    "set-returning functions must appear at top level of FROM");
        }

        @Test
        void ordinarySql() throws Exception {
            // The one-row VALUES of an INSERT does expand
            assertAccepted("INSERT INTO sc2_tgt(id,name) VALUES (generate_series(50,51), 'q')");
            assertEquals("[50, 51]",
                    rows("SELECT id FROM sc2_tgt WHERE id >= 50 ORDER BY 1").toString());
            // A set inside a nested query belongs to that query
            assertAccepted("DELETE FROM sc2_t WHERE a IN (SELECT generate_series(1,1))");
            assertAccepted("UPDATE sc2_t SET b = 'z' WHERE a IN (SELECT generate_series(1,1))");
            // One row is left: the DELETE above took the other.
            assertEquals("[1]",
                    rows("SELECT count(*) FILTER (WHERE a IN (SELECT generate_series(1,2))) "
                            + "FROM sc2_t").toString());
            // Plain VALUES, plain FILTER, a plain MERGE INSERT
            assertEquals("[1, 2]", rows("SELECT * FROM (VALUES (1),(2)) v(x) ORDER BY 1").toString());
            assertEquals("[1]", rows("SELECT count(*) FILTER (WHERE a > 1) FROM sc2_t").toString());

            assertAccepted("INSERT INTO sc2_tgt(id,name) VALUES (1,'a') ON CONFLICT (id) "
                    + "DO UPDATE SET name = 'b' WHERE sc2_tgt.id > 0");
            exec("DELETE FROM sc2_tgt");
            assertAccepted("MERGE INTO sc2_tgt t USING (SELECT 1 AS id) s ON t.id = s.id "
                    + "WHEN NOT MATCHED THEN INSERT (id, dept_id) VALUES (s.id, 7)");
            // A function in FROM reading a column of the item to its left, and a sub-query in
            // a FROM function's arguments, are both ordinary
            // One row of sc2_t is left: the DELETE above took the other.
            assertEquals("[2|b|1, 2|b|2]",
                    rows("SELECT * FROM sc2_t, LATERAL generate_series(1, a) g ORDER BY 1,3")
                            .toString());
            assertEquals("[1, 2]",
                    rows("SELECT * FROM generate_series(1, (SELECT 2)) ORDER BY 1").toString());
            assertEquals("[1, 2]",
                    rows("SELECT * FROM unnest(ARRAY(SELECT generate_series(1,2))) ORDER BY 1")
                            .toString());
            // Both items of a ROWS FROM are at top level, so neither is nested in the other
            assertEquals("[1|1, 2|2]",
                    rows("SELECT * FROM ROWS FROM (generate_series(1,2), generate_series(1,2)) "
                            + "ORDER BY 1").toString());
        }
    }

    // ---------------------------------------------------------------- declared SRFs

    @Nested
    class ADeclaredSetReturningFunctionIsOneToo {

        @Test
        void itExpandsInTheSelectList() throws Exception {
            assertEquals("[1, 2]", rows("SELECT sc2_setofint() ORDER BY 1").toString());
            assertEquals("sc2_setofint:int4", shape("SELECT sc2_setofint()"));
        }

        @Test
        void itIsCountedBesideAnotherSet() throws Exception {
            assertEquals("[1|1, 2|2, null|3]",
                    rows("SELECT sc2_setofint() AS a, generate_series(1,3) AS g ORDER BY g")
                            .toString());
            assertEquals("a:int4, g:int4",
                    shape("SELECT sc2_setofint() AS a, generate_series(1,3) AS g"));
        }

        @Test
        void itMultipliesTheRowsItIsWrittenBeside() throws Exception {
            assertEquals("[1|1, 1|2, 2|1, 2|2]",
                    rows("SELECT a, sc2_setofint() FROM sc2_t ORDER BY 1,2").toString());
        }

        @Test
        void oneElementAtATimeReachesTheExpressionAroundIt() throws Exception {
            assertEquals("[11, 12]", rows("SELECT sc2_setofint() + 10 AS v ORDER BY 1").toString());
            assertEquals("[integer, integer]",
                    rows("SELECT pg_typeof(sc2_setofint())::text").toString());
        }

        @Test
        void ordinarySql() throws Exception {
            assertEquals("[1, 2]", rows("SELECT * FROM sc2_setofint() ORDER BY 1").toString());
            assertEquals("[1|1, 2|2]",
                    rows("SELECT * FROM sc2_setofint() WITH ORDINALITY AS t(v,n) ORDER BY 1")
                            .toString());
            assertEquals("[1|1, 2|1, 2|2]",
                    rows("SELECT * FROM sc2_setofint() s, LATERAL generate_series(1, s) g "
                            + "ORDER BY 1,2").toString());
        }
    }

    // ---------------------------------------------------------------- distinct

    @Nested
    class DistinctReadsWhatTheExpansionProduced {

        @Test
        void aDuplicateTheExpansionCreatedIsFolded() throws Exception {
            assertEquals("[1, 2]", rows("SELECT DISTINCT unnest(ARRAY[1,1,2]) ORDER BY 1").toString());
            assertEquals("[1, 2]",
                    rows("SELECT DISTINCT unnest(ARRAY[1,1,2]) AS u ORDER BY u").toString());
        }

        @Test
        void distinctOnGroupsTheRowsTheExpansionMade() throws Exception {
            assertEquals("[1, 1]",
                    rows("SELECT DISTINCT ON (generate_series(1,2)) a FROM sc2_t").toString());
        }

        @Test
        void ordinarySql() throws Exception {
            assertEquals("[1, 2]", rows("SELECT DISTINCT a FROM sc2_t ORDER BY 1").toString());
            assertEquals("[1|a, 2|b]",
                    rows("SELECT DISTINCT ON (a) a, b FROM sc2_t ORDER BY a").toString());
            assertEquals("[1, 2]", rows("SELECT DISTINCT generate_series(1,2) ORDER BY 1").toString());
        }
    }

    // ---------------------------------------------------------------- types

    @Nested
    class WhatAFromItemSaysItsColumnIs {

        @Test
        void generateSeriesAnswersBigintWhenABoundIsOne() throws Exception {
            assertEquals("generate_series:int8", shape("SELECT * FROM generate_series(1::bigint, 2::bigint)"));
            assertEquals("[bigint]",
                    rows("SELECT pg_typeof(g)::text FROM generate_series(1::bigint,2::bigint) g "
                            + "LIMIT 1").toString());
            assertEquals("[bigint]",
                    rows("SELECT pg_typeof(g)::text FROM generate_series(1,2::bigint) g LIMIT 1")
                            .toString());
        }

        @Test
        void andInt4WhenNeitherIs() throws Exception {
            assertEquals("[integer]",
                    rows("SELECT pg_typeof(g)::text FROM generate_series(1,2) g LIMIT 1").toString());
            assertEquals("[integer]",
                    rows("SELECT pg_typeof(g)::text FROM generate_series(1,4,2) g LIMIT 1").toString());
            assertEquals("[numeric]",
                    rows("SELECT pg_typeof(g)::text FROM generate_series(1::numeric,2) g LIMIT 1")
                            .toString());
        }

        @Test
        void jsonArrayElementsAnswersJsonAndJsonbAnswersJsonb() throws Exception {
            assertEquals("v:json, n:int8",
                    shape("SELECT * FROM json_array_elements('[1,2]'::json) WITH ORDINALITY AS t(v,n)"));
            assertEquals("v:jsonb, n:int8",
                    shape("SELECT * FROM jsonb_array_elements('[1,2]'::jsonb) WITH ORDINALITY AS t(v,n)"));
        }
    }

    // ---------------------------------------------------------------- column definition lists

    @Nested
    class WhoMayBeToldWhatItsColumnsAre {

        @Test
        void aBuiltinThatNamesItsOwnColumnsMayNotBeToldAgain() {
            assertRefused("SELECT * FROM ROWS FROM (json_each('{\"a\":1}'::json) AS (k text, v json))",
                    "42601", "a column definition list is redundant for a function with OUT parameters");
            assertRefused("SELECT * FROM json_each('{\"a\":1}'::json) AS t(k text, v json)",
                    "42601", "a column definition list is redundant for a function with OUT parameters");
            assertRefused("SELECT * FROM ROWS FROM (jsonb_each('{\"a\":1}'::jsonb) AS (k text, v jsonb))",
                    "42601", "a column definition list is redundant for a function with OUT parameters");
        }

        @Test
        void aBuiltinReturningRecordHasToBeTold() {
            assertRefused("SELECT * FROM ROWS FROM (json_to_recordset('[{\"a\":1}]'::json))",
                    "42601", "a column definition list is required for functions returning \"record\"");
            assertRefused("SELECT * FROM ROWS FROM (json_to_record('{\"a\":1}'::json))",
                    "42601", "a column definition list is required for functions returning \"record\"");
        }

        @Test
        void ordinarySql() throws Exception {
            // A bare alias list only renames what is there
            assertEquals("[a|1]", rows("SELECT * FROM json_each('{\"a\":1}'::json) AS t(k, v)").toString());
            assertEquals("[1|null]",
                    rows("SELECT * FROM json_to_record('{\"a\":1}'::json) AS t(a int, b text)").toString());
            assertEquals("[1]",
                    rows("SELECT * FROM json_to_recordset('[{\"a\":1}]'::json) AS t(a int) ORDER BY 1")
                            .toString());
            assertEquals("[1|a|1, 2|null|null]",
                    rows("SELECT * FROM ROWS FROM (generate_series(1,2), "
                            + "json_each('{\"a\":1}'::json)) ORDER BY 1").toString());
            assertEquals("[a|1]", rows("SELECT * FROM jsonb_each('{\"a\":1}'::jsonb) ORDER BY 1").toString());
        }
    }

    // ---------------------------------------------------------------- merged join columns

    @Nested
    class AMergedJoinColumnIsNeitherSides {

        @Test
        void itTakesTheTypeBothSidesReadAs() throws Exception {
            assertEquals("[bigint]",
                    rows("SELECT pg_typeof(k)::text FROM sc2_i JOIN sc2_l USING (k)").toString());
            assertEquals("[numeric]",
                    rows("SELECT pg_typeof(k)::text FROM sc2_i JOIN sc2_n USING (k)").toString());
            assertEquals("[real]",
                    rows("SELECT pg_typeof(k)::text FROM sc2_i JOIN sc2_r USING (k)").toString());
            assertEquals("k:int8", shape("SELECT k FROM sc2_i JOIN sc2_l USING (k)"));
        }

        @Test
        void anOuterJoinAndAnAliasedOneToo() throws Exception {
            assertEquals("[bigint]",
                    rows("SELECT pg_typeof(k)::text FROM sc2_i LEFT JOIN sc2_l USING (k) "
                            + "ORDER BY 1 LIMIT 1").toString());
            assertEquals("k:int8", shape("SELECT k FROM sc2_i FULL JOIN sc2_l USING (k)"));
            assertEquals("k:int8", shape("SELECT k FROM (sc2_i JOIN sc2_l USING (k)) AS j"));
            assertEquals("k:int8", shape("SELECT j.k FROM (sc2_i JOIN sc2_l USING (k)) AS j"));
        }

        @Test
        void theComparisonIsMadeInThatTypeToo() throws Exception {
            assertEquals("[1]", rows("SELECT k FROM sc2_i JOIN sc2_l USING (k) ORDER BY 1").toString());
            assertEquals("[1]", rows("SELECT count(*) FROM sc2_i JOIN sc2_r USING (k)").toString());
            assertEquals("[1]", rows("SELECT count(*) FROM sc2_i JOIN sc2_n USING (k)").toString());
        }

        @Test
        void ordinarySql() throws Exception {
            assertEquals("[1|i|i, 2|i2|i2]",
                    rows("SELECT * FROM sc2_i JOIN sc2_i i2 USING (k) ORDER BY 1").toString());
            assertEquals("[1, 2]", rows("SELECT k FROM sc2_i NATURAL JOIN sc2_i i2 ORDER BY 1").toString());
            assertEquals("[1, 2, 3]", rows("SELECT k FROM sc2_i FULL JOIN sc2_l USING (k) ORDER BY 1")
                    .toString());
            // Both columns are merged, so the join exposes exactly those two
            assertEquals("[1|i, 2|i2]",
                    rows("SELECT * FROM sc2_i a JOIN sc2_i b USING (k, iv) ORDER BY 1").toString());
            assertEquals("[1]", rows("SELECT sc2_i.k FROM sc2_i JOIN sc2_l USING (k) ORDER BY 1")
                    .toString());
        }
    }

    // ---------------------------------------------------------------- unequatable keys

    @Nested
    class AKeyPostgresHasNoEqualityFor {

        @Test
        void twoCategoriesWithNoOperatorBetweenThem() {
            for (String[] pair : new String[][]{
                    {"sc2_i JOIN sc2_tx USING (k)", "integer = text"},
                    {"sc2_tx JOIN sc2_i USING (k)", "text = integer"},
                    {"sc2_i JOIN sc2_d USING (k)", "integer = date"},
                    {"sc2_d JOIN sc2_i USING (k)", "date = integer"},
                    {"sc2_tx JOIN sc2_d USING (k)", "text = date"}}) {
                SQLException e = refused("SELECT * FROM " + pair[0]);
                assertEquals("42883", e.getSQLState(), pair[0]);
                assertTrue(e.getMessage().contains("operator does not exist: " + pair[1]),
                        pair[0] + " -> " + e.getMessage());
                assertTrue(e.getMessage().contains(
                        "No operator matches the given name and argument types."), e.getMessage());
            }
        }

        @Test
        void aTypeWithNoEqualityOfItsOwn() {
            for (String sql : new String[]{
                    "SELECT * FROM sc2_j1 NATURAL JOIN sc2_j2",
                    "SELECT * FROM sc2_j1 a JOIN sc2_j2 b USING (js)"}) {
                SQLException e = refused(sql);
                assertEquals("42883", e.getSQLState(), sql);
                assertTrue(e.getMessage().contains("operator does not exist: json = json"),
                        sql + " -> " + e.getMessage());
                assertTrue(e.getMessage().contains(
                        "You might need to add explicit type casts."), e.getMessage());
            }
        }

        @Test
        void ordinarySql() throws Exception {
            // Same type, and two types of one category, are joined
            assertEquals("[1|{\"a\":1}|{1,2}|{\"a\":1}|{1,2}]",
                    rows("SELECT * FROM sc2_j1 a JOIN sc2_j2 b USING (k) ORDER BY 1").toString());
            assertEquals("[1|{1,2}|{\"a\":1}|{\"a\":1}]",
                    rows("SELECT * FROM sc2_j1 a JOIN sc2_j2 b USING (k, ar) ORDER BY 1").toString());
            assertEquals("[1]", rows("SELECT count(*) FROM sc2_i JOIN sc2_l USING (k)").toString());
            // An array containment test in a join condition is an array test, not a geometric one
            assertEquals("[1]",
                    rows("SELECT count(*) FROM sc2_j1 a JOIN sc2_j2 b ON a.ar @> b.ar").toString());
            assertEquals("[t]", rows("SELECT '{1,2}'::int[] @> '{1}'::int[]").toString());
            assertEquals("[t]", rows("SELECT '{1,2,3}'::int[] @> '{1}'::int[]").toString());
            assertEquals("[line]", rows("SELECT pg_typeof('{1,2,3}'::line)::text").toString());
        }
    }

    // ---------------------------------------------------------------- name resolution

    @Nested
    class ANameThatIsWrittenButOutOfReach {

        @Test
        void theRelationsUnderAnAliasedJoin() {
            for (String[] pair : new String[][]{
                    {"SELECT sc2_i.k FROM (sc2_i JOIN sc2_l USING (k)) AS j", "sc2_i"},
                    {"SELECT sc2_l.k FROM (sc2_i JOIN sc2_l USING (k)) AS j", "sc2_l"},
                    {"SELECT sc2_i.k FROM (sc2_i JOIN sc2_l ON sc2_i.k = sc2_l.k) AS j", "sc2_i"}}) {
                SQLException e = refused(pair[0]);
                assertEquals("42P01", e.getSQLState(), pair[0]);
                assertTrue(e.getMessage().contains(
                        "invalid reference to FROM-clause entry for table \"" + pair[1] + "\""),
                        pair[0] + " -> " + e.getMessage());
                assertTrue(e.getMessage().contains("There is an entry for table \"" + pair[1]
                        + "\", but it cannot be referenced from this part of the query."),
                        e.getMessage());
            }
        }

        @Test
        void aSiblingFromItemNotMarkedLateral() {
            for (String sql : new String[]{
                    "SELECT count(*) FROM (SELECT 1 AS a) s, (SELECT s.a) t",
                    "SELECT count(*) FROM sc2_i x, (SELECT x.k) y"}) {
                SQLException e = refused(sql);
                assertEquals("42P01", e.getSQLState(), sql);
                assertTrue(e.getMessage().contains("invalid reference to FROM-clause entry for table"),
                        sql + " -> " + e.getMessage());
                assertTrue(e.getMessage().contains("but it cannot be referenced from this part of the query."),
                        e.getMessage());
                assertTrue(e.getMessage().contains(
                        "To reference that table, you must mark this subquery with LATERAL."),
                        e.getMessage());
            }
        }

        @Test
        void aNameNothingHasIsStillMissing() {
            assertRefused("SELECT nosuch.k FROM sc2_i", "42P01",
                    "missing FROM-clause entry for table \"nosuch\"");
        }

        @Test
        void ordinarySql() throws Exception {
            assertEquals("[1]", rows("SELECT j.k FROM (sc2_i JOIN sc2_l USING (k)) AS j ORDER BY 1")
                    .toString());
            assertEquals("[1]", rows("SELECT k FROM (sc2_i JOIN sc2_l USING (k)) AS j ORDER BY 1")
                    .toString());
            assertEquals("[i]", rows("SELECT j.iv FROM (sc2_i JOIN sc2_l USING (k)) AS j ORDER BY 1")
                    .toString());
            assertEquals("[1]", rows("SELECT count(*) FROM (SELECT 1 AS a) s, LATERAL (SELECT s.a) t")
                    .toString());
            assertEquals("[1]", rows("SELECT count(*) FROM (SELECT 1 AS a) s, (SELECT 2 AS b) t "
                    + "WHERE s.a < t.b").toString());
        }
    }

    @Nested
    class ASuggestedColumnNamesTheRelationThatHasIt {

        @Test
        void oneRelation() {
            for (String sql : new String[]{"SELECT tt FROM sc2_a a", "SELECT a.tt FROM sc2_a a"}) {
                SQLException e = refused(sql);
                assertEquals("42703", e.getSQLState(), sql);
                assertTrue(e.getMessage().contains(
                        "Perhaps you meant to reference the column \"a.t\"."),
                        sql + " -> " + e.getMessage());
            }
            for (String sql : new String[]{"SELECT tt FROM sc2_a", "SELECT sc2_a.tt FROM sc2_a"}) {
                SQLException e = refused(sql);
                assertTrue(e.getMessage().contains(
                        "Perhaps you meant to reference the column \"sc2_a.t\"."),
                        sql + " -> " + e.getMessage());
            }
        }

        @Test
        void aQualifiedReferenceIsAnsweredForThatRelationOnly() {
            SQLException e = refused("SELECT b.yy FROM sc2_a a JOIN sc2_b b ON a.x = b.y");
            assertEquals("42703", e.getSQLState());
            assertTrue(e.getMessage().contains("column b.yy does not exist"), e.getMessage());
            assertTrue(e.getMessage().contains(
                    "Perhaps you meant to reference the column \"b.y\"."), e.getMessage());
        }

        @Test
        void everyRelationWithANearMissIsOffered() {
            SQLException e = refused("SELECT tt FROM sc2_a a JOIN sc2_b b ON a.id = b.id");
            assertEquals("42703", e.getSQLState());
            assertTrue(e.getMessage().contains(
                    "Perhaps you meant to reference the column \"a.t\" or the column \"b.t\"."),
                    e.getMessage());
        }

        @Test
        void ordinarySql() throws Exception {
            assertEquals("[]", rows("SELECT a.x FROM sc2_a a JOIN sc2_b b ON a.x = b.y").toString());
            assertEquals("[]", rows("SELECT b.y FROM sc2_a a JOIN sc2_b b ON a.x = b.y").toString());
            // Nothing near enough to suggest carries no hint at all
            SQLException e = refused("SELECT a.zz FROM sc2_a a");
            assertEquals("42703", e.getSQLState());
            assertFalse(e.getMessage().contains("Perhaps you meant"), e.getMessage());
        }
    }
}
