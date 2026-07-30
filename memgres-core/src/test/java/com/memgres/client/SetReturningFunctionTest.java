package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a set-returning function produces, and where it may be written.
 *
 * <p>A set-returning call in a select list does not return a value: it multiplies the row.
 * Several of them in one query run side by side rather than one inside the other — the query has
 * as many rows as the longest of them, and the shorter ones read NULL past their end. Only one
 * per expression was expanded before, so the second was left as the list it computed and
 * {@code generate_series(1,2) + generate_series(10,12)} failed with "operator does not exist:
 * integer + integer[]". The search for the calls to expand is also a reflective walk now rather
 * than a list of node types to remember, which is what makes it reach one inside {@code IN},
 * {@code IS NULL} or {@code ARRAY[...]}; and a call among another call's arguments runs first,
 * the outer one running once per element it yields.
 *
 * <p>That same walk stops at a nested query, whose set-returning calls produce that query's rows
 * and not this one's. It did not before, and {@code WHERE x IN (SELECT generate_series(1,2))} —
 * ordinary SQL — was refused as a set-returning function in WHERE.
 *
 * <p>Placement follows from what a clause does. GROUP BY produces rows, and a set-returning key
 * expands them before the grouping sees them, which was refused outright. WHERE, a JOIN
 * condition, HAVING, LIMIT and OFFSET read rows already produced, and the last three of those
 * accepted one. Somewhere that wants a single boolean PostgreSQL names the kind of value instead
 * of the clause: a WHEN condition and the arguments of AND, OR and NOT must not return a set. An
 * aggregate or a window call cannot contain one, and OVER written on a function that is neither
 * a window function nor an aggregate is a clause that function has no use for, not a missing
 * function — that answered NULL.
 *
 * <p>ROWS FROM keeps each function's own columns under their own names, where it used to take
 * the first column of each and call them column1, column2; and each function may carry the
 * column definition list a record result needs, which was a syntax error. That list is names
 * with types, and a bare alias list is not one: a record result is not described by an alias
 * list, and a signature that already names its columns is contradicted by a definition list.
 */
class SetReturningFunctionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE srft (x int)");
        exec("INSERT INTO srft VALUES (1),(2)");
        exec("CREATE FUNCTION srft_rec() RETURNS SETOF record AS $$ SELECT 1, 'a'::text $$ LANGUAGE sql");
        exec("CREATE FUNCTION srft_out(OUT x int, OUT y text) RETURNS SETOF record"
                + " AS $$ SELECT 1, 'a'::text $$ LANGUAGE sql");
        exec("CREATE FUNCTION srft_tab() RETURNS TABLE(x int, y text)"
                + " AS $$ SELECT 1, 'a'::text $$ LANGUAGE sql");
        exec("CREATE FUNCTION srft_scalar() RETURNS int AS $$ SELECT 7 $$ LANGUAGE sql");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- helpers ----

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** Every row as "a|b|c", NULL spelled out, in the order the query answered them. */
    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) row.append('|');
                    Object v = rs.getObject(i);
                    row.append(v == null ? "NULL" : String.valueOf(v));
                }
                out.add(row.toString());
            }
        }
        return out;
    }

    private static List<String> columns(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) out.add(md.getColumnLabel(i));
        }
        return out;
    }

    private static SQLException rejects(String sqlState, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        return e;
    }

    private static void rejects(String sqlState, String messagePart, String sql) {
        SQLException e = rejects(sqlState, sql);
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- Several sets in one expression ----

    @Test
    void twoSetsInOneExpressionRunSideBySideToTheLongest() throws Exception {
        assertEquals(List.of("11", "13", "NULL"),
                rows("SELECT generate_series(1,2) + generate_series(10,12) AS a"));
        assertEquals(List.of("11", "22"),
                rows("SELECT unnest(ARRAY[1,2]) + unnest(ARRAY[10,20]) AS a"));
        assertEquals(List.of("2", "6", "NULL"),
                rows("SELECT (generate_series(1,3) + 1) * generate_series(1,2) AS a"));
        assertEquals(List.of("13", "24"),
                rows("SELECT generate_series(1,2)::text || generate_series(3,4)::text AS a"));
        assertEquals(List.of("2", "4"),
                rows("SELECT generate_series(1,2) + generate_series(1,2) AS a"));
    }

    @Test
    void separateSelectItemsFollowTheSameRule() throws Exception {
        assertEquals(List.of("1|10", "2|11", "NULL|12", "NULL|13"),
                rows("SELECT generate_series(1,2) AS a, generate_series(10,13) AS b"));
        assertEquals(List.of("1|10", "2|11", "3|NULL"),
                rows("SELECT generate_series(1,3) AS a, generate_series(10,11) AS b"));
        assertEquals(List.of("1|x", "2|y", "NULL|z"),
                rows("SELECT unnest(ARRAY[1,2]) AS a, unnest(ARRAY['x','y','z']) AS b"));
    }

    @Test
    void aSetAmongAnothersArgumentsRunsFirst() throws Exception {
        assertEquals(List.of("1", "2", "3", "4", "2", "3", "4"),
                rows("SELECT generate_series(generate_series(1,2), 4) AS a"));
    }

    @Test
    void theExpansionReachesThroughWhateverTheExpressionIsMadeOf() throws Exception {
        assertEquals(List.of("2", "3", "4"), rows("SELECT generate_series(1,3) + 1 AS a"));
        assertEquals(List.of("2", "4", "6"), rows("SELECT generate_series(1,3) * 2 AS a"));
        assertEquals(List.of("-1", "-2"), rows("SELECT -generate_series(1,2) AS a"));
        assertEquals(List.of("1", "2"), rows("SELECT (generate_series(1,2))::text AS a"));
        assertEquals(List.of("2", "1", "0"), rows("SELECT abs(generate_series(-2,0)) AS a"));
        assertEquals(List.of("1x", "2x", "3x"), rows("SELECT generate_series(1,3) || 'x' AS a"));
        assertEquals(List.of("true", "true"), rows("SELECT generate_series(1,2) IN (1,2) AS a"));
        assertEquals(List.of("false", "false"), rows("SELECT generate_series(1,2) IS NULL AS a"));
        assertEquals(List.of("{1}", "{2}"), rows("SELECT ARRAY[generate_series(1,2)] AS a"));
        assertEquals(List.of("true", "true"), rows("SELECT generate_series(1,2) > 0 AS a"));
    }

    @Test
    void anEmptySetLeavesNoRow() throws Exception {
        assertEquals(List.of(), rows("SELECT generate_series(1,0) AS a"));
        assertEquals(List.of("0"), rows("SELECT count(*)::text FROM (SELECT generate_series(1,0)) s"));
    }

    @Test
    void aSetInOneItemMultipliesTheRestOfTheRow() throws Exception {
        assertEquals(List.of("1|1", "1|2"), rows("SELECT 1 AS x, generate_series(1,2) AS a"));
        assertEquals(List.of("1|5", "2|5"),
                rows("SELECT unnest(ARRAY[1,2]) AS a, x FROM (VALUES (5)) v(x) ORDER BY a"));
        assertEquals(List.of("1|1", "1|2", "2|1", "2|2"),
                rows("SELECT x, generate_series(1,2) AS a FROM srft ORDER BY x, a"));
    }

    // ---- A nested query keeps its own sets ----

    @Test
    void aSetInsideANestedQueryIsThatQuerysBusiness() throws Exception {
        assertEquals(List.of("1", "1"),
                rows("SELECT 1 AS one FROM srft WHERE x IN (SELECT generate_series(1,2)) ORDER BY x"));
        assertEquals(List.of("1", "1"),
                rows("SELECT 1 AS one FROM srft WHERE EXISTS (SELECT generate_series(1,2)) ORDER BY x"));
        assertEquals(List.of("1"), rows("SELECT (SELECT generate_series(1,1)) AS a"));
        assertEquals(List.of("3"), rows("SELECT count(*) AS n FROM (SELECT generate_series(1,3)) s"));
        assertEquals(List.of("2"), rows("SELECT * FROM (SELECT generate_series(1,2) AS g) s WHERE g > 1"));
        assertEquals(List.of("{1,2,3}"),
                rows("SELECT array_agg(x) AS agg FROM (SELECT generate_series(1,3) AS x) s"));
        assertEquals(List.of("1"),
                rows("SELECT 1 AS one FROM (VALUES (1)) v(x) GROUP BY (SELECT generate_series(1,1))"));
    }

    // ---- Clauses that read rows rather than producing them ----

    @Test
    void aClauseThatReadsRowsHoldsNoSet() {
        rejects("0A000", "set-returning functions are not allowed in WHERE",
                "SELECT 1 FROM srft WHERE generate_series(1,2) > 0");
        rejects("0A000", "set-returning functions are not allowed in HAVING",
                "SELECT 1 FROM srft HAVING generate_series(1,2) > 0");
        rejects("0A000", "set-returning functions are not allowed in LIMIT",
                "SELECT 1 FROM srft LIMIT generate_series(1,2)");
        rejects("0A000", "set-returning functions are not allowed in OFFSET",
                "SELECT 1 FROM srft OFFSET generate_series(1,2)");
        rejects("0A000", "set-returning functions are not allowed in JOIN conditions",
                "SELECT 1 FROM srft JOIN srft u ON generate_series(1,2) > 0");
        rejects("0A000", "set-returning functions are not allowed in JOIN conditions",
                "SELECT 1 FROM srft LEFT JOIN srft u ON generate_series(1,2) > 0");
        rejects("0A000", "set-returning functions are not allowed in COALESCE",
                "SELECT coalesce(generate_series(1,2), 9) AS a");
    }

    @Test
    void aSingleBooleanIsWantedAndASetIsTheWrongKindOfValue() {
        rejects("42804", "argument of CASE/WHEN must not return a set",
                "SELECT CASE WHEN generate_series(1,3) > 1 THEN 'y' ELSE 'n' END AS a");
        rejects("42804", "argument of AND must not return a set",
                "SELECT generate_series(1,2) > 0 AND true AS a");
        rejects("42804", "argument of OR must not return a set",
                "SELECT generate_series(1,2) > 0 OR false AS a");
        rejects("42804", "argument of NOT must not return a set",
                "SELECT NOT (generate_series(1,2) > 0) AS a");
        rejects("42804", "argument of AND must not return a set",
                "SELECT generate_series(1,2) BETWEEN 1 AND 2 AS a");
    }

    @Test
    void aSetElsewhereInACaseIsThePlacementRuleAgain() {
        rejects("0A000", "set-returning functions are not allowed in CASE",
                "SELECT CASE generate_series(1,2) WHEN 1 THEN 'x' ELSE 'y' END AS a");
        rejects("0A000", "set-returning functions are not allowed in CASE",
                "SELECT CASE WHEN true THEN generate_series(1,2) ELSE 0 END AS a");
    }

    @Test
    void anAggregateOrWindowCallReadsOneValueAtATime() {
        rejects("0A000", "aggregate function calls cannot contain set-returning function calls",
                "SELECT array_agg(unnest(ARRAY[1,2]))");
        rejects("0A000", "aggregate function calls cannot contain set-returning function calls",
                "SELECT string_agg(generate_series(1,2)::text, ',')");
        rejects("0A000", "aggregate function calls cannot contain set-returning function calls",
                "SELECT sum(generate_series(1,2))");
        rejects("0A000", "aggregate function calls cannot contain set-returning function calls",
                "SELECT string_agg((srft_tab()).y, ',')");
        rejects("0A000", "window function calls cannot contain set-returning function calls",
                "SELECT count(generate_series(1,2)) OVER () AS a");
    }

    @Test
    void overOnAFunctionThatIsNeitherIsAClauseItHasNoUseFor() {
        rejects("42809", "OVER specified, but generate_series is not a window function nor an aggregate",
                "SELECT generate_series(1,2) OVER ()");
        rejects("42809", "OVER specified, but abs is not a window function nor an aggregate function",
                "SELECT abs(-1) OVER ()");
        rejects("42809", "OVER specified, but now is not a window function nor an aggregate function",
                "SELECT now() OVER ()");
        rejects("42809", "OVER specified, but length is not a window function nor an aggregate function",
                "SELECT length('x') OVER ()");
    }

    /** The window and aggregate shapes around those rules, which must keep working. */
    @Test
    void theOrdinaryWindowShapesAreUntouched() throws Exception {
        assertEquals(List.of("3", "3"), rows("SELECT sum(x) OVER () AS s FROM srft"));
        assertEquals(List.of("1", "2"), rows("SELECT row_number() OVER (ORDER BY x) AS n FROM srft"));
        assertEquals(List.of("NULL", "1"), rows("SELECT lag(x) OVER (ORDER BY x) AS l FROM srft"));
        assertEquals(List.of("1", "2"), rows("SELECT rank() OVER (ORDER BY x) AS r FROM srft"));
        assertEquals(List.of("1", "1"), rows("SELECT ntile(1) OVER (ORDER BY x) AS t FROM srft"));
        assertEquals(List.of("1", "1"), rows("SELECT count(*) OVER (PARTITION BY x) AS c FROM srft"));
        assertEquals(List.of("{1,2,3}"), rows("SELECT array_agg(g) AS agg FROM generate_series(1,3) g"));
        assertEquals(List.of("1|2", "2|4", "3|6"),
                rows("SELECT g, g*2 AS d FROM generate_series(1,3) g ORDER BY g"));
    }

    // ---- GROUP BY ----

    @Test
    void aGroupingKeyThatIsASetExpandsTheRowsFirst() throws Exception {
        assertEquals(List.of("1", "1"),
                rows("SELECT 1 AS one FROM (VALUES (1)) v(x) GROUP BY generate_series(1,2)"));
        assertEquals(List.of("1", "1"),
                rows("SELECT count(*) AS n FROM (VALUES (1)) v(x) GROUP BY generate_series(1,2)"));
        assertEquals(List.of("1", "1", "2", "2"),
                rows("SELECT x FROM srft GROUP BY x, generate_series(1,2) ORDER BY 1"));
        assertEquals(List.of("1", "1", "1"),
                rows("SELECT 1 AS one FROM (VALUES (1)) v(x)"
                        + " GROUP BY generate_series(1,2), generate_series(1,3)"));
        assertEquals(List.of("1", "1"),
                rows("SELECT 1 AS one FROM (VALUES (1)) v(x) GROUP BY unnest(ARRAY[1,2])"));
    }

    @Test
    void aGroupingKeyNamedByOrdinalOrAliasIsTheSameKey() throws Exception {
        assertEquals(List.of("1", "2"),
                rows("SELECT generate_series(1,2) AS g FROM (VALUES (1)) v(x) GROUP BY 1"));
        assertEquals(List.of("1", "2"),
                rows("SELECT generate_series(1,2) AS g FROM (VALUES (1)) v(x) GROUP BY g"));
    }

    /** Grouping without a set in it, which the expansion must leave exactly as it was. */
    @Test
    void anOrdinaryGroupByIsUnaffected() throws Exception {
        assertEquals(List.of("1|1", "2|1"),
                rows("SELECT x, count(*) AS n FROM srft GROUP BY x ORDER BY x"));
        assertEquals(List.of("3"), rows("SELECT sum(x) AS s FROM srft"));
        assertEquals(List.of(), rows("SELECT 1 AS one FROM srft GROUP BY x HAVING count(*) > 1"));
        assertEquals(List.of("1", "1"),
                rows("SELECT count(*) AS n FROM srft GROUP BY x HAVING count(*) > 0"));
    }

    // ---- ROWS FROM ----

    @Test
    void rowsFromKeepsEachFunctionsOwnColumns() throws Exception {
        assertEquals(List.of("generate_series"), columns("SELECT * FROM ROWS FROM (generate_series(1,2))"));
        assertEquals(List.of("1", "2"), rows("SELECT * FROM ROWS FROM (generate_series(1,2))"));

        String twoFunctions = "SELECT * FROM ROWS FROM (generate_series(1,2), json_each('{\"a\":1}'::json))";
        assertEquals(List.of("generate_series", "key", "value"), columns(twoFunctions));
        assertEquals(List.of("1|a|1", "2|NULL|NULL"), rows(twoFunctions));

        assertEquals(List.of("1|1", "2|2", "NULL|3"),
                rows("SELECT * FROM ROWS FROM (generate_series(1,2), generate_series(1,3))"));
    }

    @Test
    void rowsFromTakesAnAliasListAndAnOrdinality() throws Exception {
        String aliased = "SELECT * FROM ROWS FROM (generate_series(1,2), generate_series(1,3)) AS t(a, b)";
        assertEquals(List.of("a", "b"), columns(aliased));
        assertEquals(List.of("1|1", "2|2", "NULL|3"), rows(aliased));

        String ordinal = "SELECT * FROM ROWS FROM (generate_series(1,2)) WITH ORDINALITY";
        assertEquals(List.of("generate_series", "ordinality"), columns(ordinal));
        assertEquals(List.of("1|1", "2|2"), rows(ordinal));
    }

    @Test
    void eachRowsFromFunctionCarriesItsOwnColumnDefinitionList() throws Exception {
        String defined = "SELECT * FROM ROWS FROM (srft_rec() AS (x int, y text))";
        assertEquals(List.of("x", "y"), columns(defined));
        assertEquals(List.of("1|a"), rows(defined));

        assertEquals(List.of("x", "y"), columns("SELECT * FROM ROWS FROM (srft_tab())"));
        rejects("42601", "a column definition list is redundant for a function with OUT parameters",
                "SELECT * FROM ROWS FROM (srft_tab() AS (x int, y text))");
        rejects("42601", "a column definition list is only allowed for functions returning \"record\"",
                "SELECT * FROM ROWS FROM (generate_series(1,2) AS (g int))");
    }

    // ---- Column definition lists ----

    @Test
    void aRecordResultIsDescribedByNamesWithTypesAndNothingElse() throws Exception {
        rejects("42601", "a column definition list is required for functions returning \"record\"",
                "SELECT * FROM srft_rec()");
        rejects("42601", "a column definition list is required for functions returning \"record\"",
                "SELECT * FROM srft_rec() AS t(p, q)");
        assertEquals(List.of("1|a"), rows("SELECT * FROM srft_rec() AS t(x int, y text)"));
        assertEquals(List.of("x", "y"), columns("SELECT * FROM srft_rec() AS t(x int, y text)"));
        // srft_rec is LANGUAGE sql, so PostgreSQL reports the definition rather than a value.
        rejects("42P13", "return type mismatch in function declared to return record",
                "SELECT * FROM srft_rec() AS t(x int, y int, z int)");
    }

    @Test
    void aSignatureThatNamesItsColumnsIsNotDescribedASecondTime() throws Exception {
        rejects("42601", "a column definition list is redundant for a function with OUT parameters",
                "SELECT * FROM srft_out() AS t(x int, y text)");
        rejects("42601", "a column definition list is redundant for a function with OUT parameters",
                "SELECT * FROM srft_tab() AS t(x int, y text)");
        assertEquals(List.of("x", "y"), columns("SELECT * FROM srft_out()"));
        assertEquals(List.of("x", "y"), columns("SELECT * FROM srft_tab()"));
        // but it may still be renamed, which is all an alias list ever does
        assertEquals(List.of("p", "q"), columns("SELECT * FROM srft_tab() AS t(p, q)"));
        assertEquals(List.of("1|a"), rows("SELECT * FROM srft_tab() AS t(p, q)"));
    }

    @Test
    void aNamedResultTypeHasNothingADefinitionListCouldAdd() throws Exception {
        rejects("42601", "a column definition list is only allowed for functions returning \"record\"",
                "SELECT * FROM srft_scalar() AS t(p int)");
        rejects("42601", "a column definition list is only allowed for functions returning \"record\"",
                "SELECT * FROM generate_series(1,2) AS t(g int)");
        // the same names without types are an alias list, and always allowed
        assertEquals(List.of("g"), columns("SELECT * FROM generate_series(1,2) AS t(g)"));
        assertEquals(List.of("1", "2"), rows("SELECT * FROM generate_series(1,2) AS t(g)"));
        assertEquals(List.of("p"), columns("SELECT * FROM srft_scalar() AS t(p)"));
        assertEquals(List.of("7"), rows("SELECT * FROM srft_scalar() AS t(p)"));
    }

    /** A record result whose columns nobody named fails on the column, before the set. */
    @Test
    void anUnnamedRecordFieldIsReportedAsTheColumnItIs() {
        rejects("42703", "could not identify column \"y\" in record data type",
                "SELECT (srft_rec()).y");
        rejects("42703", "could not identify column \"y\" in record data type",
                "SELECT string_agg((srft_rec()).y, ',')");
    }

    // ---- The json_each family ----

    @Test
    void theJsonEachFamilyIsASetReturningFunctionOfTwoColumns() throws Exception {
        String each = "SELECT * FROM json_each('{\"a\":1,\"b\":2}'::json)";
        assertEquals(List.of("key", "value"), columns(each));
        assertEquals(List.of("a|1", "b|2"), rows(each));

        assertEquals(List.of("a|1"), rows("SELECT * FROM jsonb_each_text('{\"a\":\"1\"}'::jsonb)"));

        String ordinal = "SELECT * FROM json_each('{\"a\":1}'::json) WITH ORDINALITY";
        assertEquals(List.of("key", "value", "ordinality"), columns(ordinal));
        assertEquals(List.of("a|1|1"), rows(ordinal));

        assertEquals(List.of("a", "b"), rows("SELECT (jsonb_each('{\"a\":1,\"b\":2}'::jsonb)).key"));
        assertEquals(List.of("1"), rows("SELECT (json_each_text('{\"a\":\"1\"}'::json)).value"));
        assertEquals(List.of("(a,1)"), rows("SELECT jsonb_each('{\"a\":1}'::jsonb)::text"));
    }

    /** A function item in FROM, which none of the placement rules may disturb. */
    @Test
    void anOrdinaryFunctionItemInFromStillResolves() throws Exception {
        assertEquals(List.of("1", "2"), rows("SELECT * FROM generate_series(1,2)"));
        assertEquals(List.of("1|1", "2|2"), rows("SELECT * FROM generate_series(1,2) WITH ORDINALITY"));
        assertEquals(List.of("1|1", "1|2"),
                rows("SELECT x, g FROM (VALUES (1)) v(x), LATERAL generate_series(1,2) g ORDER BY g"));
        assertEquals(List.of("3"), rows("SELECT * FROM abs(-3)"));
        assertEquals(List.of("1", "2"), rows("SELECT * FROM unnest(ARRAY[1,2])"));
    }
}
