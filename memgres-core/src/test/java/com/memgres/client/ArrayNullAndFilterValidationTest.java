package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FILTER belongs to a call that accumulates rows, and what a NULL element means to the array
 * operators.
 *
 * <p>Two things were reported together and only one of them was PostgreSQL. FILTER written on an
 * ordinary function is 42809 "FILTER specified, but <name> is not an aggregate function", and
 * memgres accepted it everywhere but on a set-returning call in a select list. The 22004 "array
 * must not contain nulls" reported for {@code @>}, {@code <@} and {@code &&} is not PostgreSQL's
 * at all: it comes from the intarray contrib extension, which redeclares the three operators over
 * integer[] on the server the corpus measures against. A PostgreSQL without that extension
 * answers a plain t or f, which is what memgres answers, so nothing was changed for it — see
 * {@link #arrayNullElementsAreMatchedAgainst()}. What the same reading did turn up is four real
 * gaps in the array functions, closed here.
 *
 * <p>Every expectation was measured against PostgreSQL 18 before it was written down.
 */
class ArrayNullAndFilterValidationTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            s.execute("CREATE TABLE anf_t (id int PRIMARY KEY, v int, b int)");
            s.execute("INSERT INTO anf_t VALUES (1,10,1),(2,20,2)");
            s.execute("CREATE TABLE anf_empty (id int PRIMARY KEY, v int, b int)");
            s.execute("CREATE FUNCTION anf_double(int) RETURNS int AS 'SELECT $1 * 2' LANGUAGE sql");
            s.execute("CREATE AGGREGATE anf_myagg(int) (SFUNC = int4pl, STYPE = int4, INITCOND = '0')");
            s.execute("CREATE TABLE anf_a (id int PRIMARY KEY, ta text[], ba bigint[])");
            s.execute("INSERT INTO anf_a VALUES (1,'{a,b}','{1,2}'),(2,'{b,NULL}','{3,NULL}'),"
                    + "(3,'{}','{}'),(4,NULL,NULL)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- helpers -----------------------------------------------------------

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            try (ResultSet rs = s.executeQuery(sql)) {
                assertTrue(rs.next(), "no row for " + sql);
                return rs.getString(1);
            }
        }
    }

    private static String row(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            try (ResultSet rs = s.executeQuery(sql)) {
                StringBuilder sb = new StringBuilder();
                int cols = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    if (sb.length() > 0) sb.append(" / ");
                    for (int i = 1; i <= cols; i++) {
                        if (i > 1) sb.append("|");
                        String v = rs.getString(i);
                        sb.append(rs.wasNull() ? "NULL" : v);
                    }
                }
                return sb.toString();
            }
        }
    }

    private static void run(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            s.execute(sql);
        }
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> run(sql), "expected a refusal for " + sql);
    }

    private static void refusedWith(String sql, String sqlState, String message) {
        SQLException e = refused(sql);
        assertEquals(sqlState, e.getSQLState(), sql);
        assertTrue(e.getMessage().contains(message),
                sql + "\n  expected to contain: " + message + "\n  got: " + e.getMessage());
    }

    /** The message PostgreSQL builds for a FILTER on a call that does not accumulate rows. */
    private static void notAnAggregate(String sql, String name) {
        refusedWith(sql, "42809",
                "FILTER specified, but " + name + " is not an aggregate function");
    }

    // ---- SECTION A: FILTER on a call that is not an aggregate ---------------

    @Nested
    @DisplayName("FILTER is refused on a call that does not accumulate rows")
    class FilterRefusedOnPlainCalls {

        @Test
        void a_plain_function_over_a_column_is_refused() {
            notAnAggregate("SELECT abs(v) FILTER (WHERE b = 1) FROM anf_t", "abs");
            notAnAggregate("SELECT length(v::text) FILTER (WHERE b = 1) FROM anf_t", "length");
            notAnAggregate("SELECT upper('x') FILTER (WHERE b = 1) FROM anf_t", "upper");
        }

        @Test
        void no_table_is_needed_because_the_call_itself_is_wrong() {
            notAnAggregate("SELECT abs(1) FILTER (WHERE true)", "abs");
            notAnAggregate("SELECT now() FILTER (WHERE true)", "now");
            notAnAggregate("SELECT random() FILTER (WHERE true)", "random");
        }

        @Test
        void an_unquoted_name_is_folded_and_a_quoted_one_is_not() {
            notAnAggregate("SELECT ABS(v) FILTER (WHERE b = 1) FROM anf_t", "abs");
            notAnAggregate("SELECT AbS(v) FILTER (WHERE b = 1) FROM anf_t", "abs");
            notAnAggregate("SELECT \"abs\"(v) FILTER (WHERE b = 1) FROM anf_t", "abs");
        }

        @Test
        void a_schema_qualifier_is_kept_and_joined_with_a_dot() {
            notAnAggregate("SELECT pg_catalog.abs(v) FILTER (WHERE b = 1) FROM anf_t",
                    "pg_catalog.abs");
            notAnAggregate("SELECT pg_catalog . abs (v) FILTER (WHERE b = 1) FROM anf_t",
                    "pg_catalog.abs");
            notAnAggregate("SELECT pg_catalog.generate_series(1,2) FILTER (WHERE true)",
                    "pg_catalog.generate_series");
        }

        @Test
        void a_set_returning_call_is_refused_under_the_same_message() {
            notAnAggregate("SELECT generate_series(1,2) FILTER (WHERE true)", "generate_series");
            notAnAggregate("SELECT unnest(ARRAY[1,2]) FILTER (WHERE true)", "unnest");
        }

        @Test
        void a_user_declared_function_is_refused_exactly_like_a_builtin() {
            notAnAggregate("SELECT anf_double(v) FILTER (WHERE b = 1) FROM anf_t", "anf_double");
        }

        @Test
        void the_filter_complaint_is_decided_before_the_over_complaint() {
            notAnAggregate("SELECT abs(v) FILTER (WHERE b = 1) OVER () FROM anf_t", "abs");
        }

        @Test
        void the_filter_complaint_is_decided_before_the_grouping_complaint() {
            notAnAggregate("SELECT abs(count(*)) FILTER (WHERE b = 1) FROM anf_t", "abs");
            notAnAggregate("SELECT abs(v) FILTER (WHERE b = 1) FROM anf_t GROUP BY v", "abs");
        }

        @Test
        void the_filter_complaint_is_decided_before_the_srf_placement_complaint() {
            notAnAggregate("SELECT id FROM anf_t WHERE generate_series(1,2) FILTER (WHERE true) > 0",
                    "generate_series");
        }

        @Test
        void every_clause_an_expression_may_stand_in() {
            notAnAggregate("SELECT id FROM anf_t WHERE abs(v) FILTER (WHERE b = 1) > 0", "abs");
            notAnAggregate("SELECT id FROM anf_t ORDER BY abs(v) FILTER (WHERE b = 1)", "abs");
            notAnAggregate("SELECT b FROM anf_t GROUP BY b HAVING abs(b) FILTER (WHERE b = 1) > 0",
                    "abs");
            notAnAggregate("WITH c AS (SELECT abs(v) FILTER (WHERE b = 1) AS x FROM anf_t)"
                    + " SELECT x FROM c", "abs");
            notAnAggregate("SELECT x FROM (SELECT abs(v) FILTER (WHERE b = 1) AS x FROM anf_t) q",
                    "abs");
            notAnAggregate("SELECT id FROM anf_t WHERE id IN"
                    + " (SELECT abs(v) FILTER (WHERE b = 1) FROM anf_t)", "abs");
            notAnAggregate("SELECT count(*) FILTER (WHERE b = 1) AS c FROM anf_t"
                    + " UNION ALL SELECT abs(v) FILTER (WHERE b = 1) AS c FROM anf_t", "abs");
            notAnAggregate("SELECT CASE WHEN false THEN abs(1) FILTER (WHERE true) ELSE 1 END",
                    "abs");
            notAnAggregate("SELECT 'a' AS c WHERE false AND (abs(1) FILTER (WHERE true)) > 0",
                    "abs");
        }

        @Test
        void the_data_modifying_statements() {
            notAnAggregate("UPDATE anf_t SET v = abs(v) FILTER (WHERE b = 1) WHERE id = 1", "abs");
            notAnAggregate("DELETE FROM anf_t WHERE abs(v) FILTER (WHERE b = 1) > 100", "abs");
            notAnAggregate("INSERT INTO anf_t SELECT 3, abs(v) FILTER (WHERE b = 1), 3"
                    + " FROM anf_t LIMIT 1", "abs");
            notAnAggregate("INSERT INTO anf_t VALUES (9, abs(1) FILTER (WHERE true), 9)", "abs");
            notAnAggregate("MERGE INTO anf_t t USING (SELECT 1 AS k) s ON t.id = s.k"
                    + " WHEN MATCHED THEN UPDATE SET v = abs(v) FILTER (WHERE b = 1)", "abs");
        }

        @Test
        void a_plain_call_nested_in_an_aggregate_argument_is_still_refused() {
            notAnAggregate("SELECT sum(abs(v) FILTER (WHERE b = 1)) FROM anf_t", "abs");
        }

        @Test
        void the_predicate_value_and_the_row_count_are_both_irrelevant() {
            notAnAggregate("SELECT abs(v) FILTER (WHERE false) FROM anf_t", "abs");
            notAnAggregate("SELECT abs(v) FILTER (WHERE b = 1) FROM anf_empty", "abs");
            notAnAggregate("SELECT abs(v) FILTER (WHERE b = 1) FROM anf_t LIMIT 0", "abs");
        }

        @Test
        void the_data_the_refused_statements_touched_is_unchanged() throws SQLException {
            assertEquals("1|10|1 / 2|20|2", row("SELECT id, v, b FROM anf_t ORDER BY id"));
        }
    }

    // ---- what must not be refused ------------------------------------------

    @Nested
    @DisplayName("what a FILTER is for keeps answering")
    class FilterKeptOnAggregates {

        @Test
        void the_plain_aggregate_shapes() throws SQLException {
            assertEquals("1", one("SELECT count(*) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("1", one("SELECT count(v) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("10", one("SELECT sum(v) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("10", one("SELECT min(v) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("20", one("SELECT max(v) FILTER (WHERE b = 2) FROM anf_t"));
            assertEquals("10.0000000000000000", one("SELECT avg(v) FILTER (WHERE b = 1) FROM anf_t"));
        }

        @Test
        void distinct_intra_aggregate_order_by_and_two_argument_aggregates() throws SQLException {
            assertEquals("1", one("SELECT count(DISTINCT v) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("10", one("SELECT string_agg(v::text, ',') FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("10", one("SELECT string_agg(DISTINCT v::text, ',' ORDER BY v::text)"
                    + " FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("{10}", one("SELECT array_agg(v ORDER BY v DESC) FILTER (WHERE b = 1)"
                    + " FROM anf_t"));
            assertEquals("[10]", one("SELECT json_agg(v) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("[10]", one("SELECT jsonb_agg(v) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("t", one("SELECT bool_and(v > 5) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("t", one("SELECT every(v > 0) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("10", one("SELECT bit_and(v) FILTER (WHERE b = 1) FROM anf_t"));
        }

        @Test
        void grouping_having_order_by_and_grouping_sets() throws SQLException {
            assertEquals("1|1 / 2|1",
                    row("SELECT b, count(*) FILTER (WHERE v > 5) AS n FROM anf_t"
                            + " GROUP BY b ORDER BY b"));
            assertEquals("1 / 2",
                    row("SELECT b FROM anf_t GROUP BY b"
                            + " HAVING count(*) FILTER (WHERE v > 5) > 0 ORDER BY b"));
            assertEquals("1 / 2",
                    row("SELECT b FROM anf_t GROUP BY b ORDER BY count(*) FILTER (WHERE v > 5), b"));
            assertEquals("1|1 / 2|1 / NULL|2",
                    row("SELECT b, count(*) FILTER (WHERE v > 5) AS n FROM anf_t"
                            + " GROUP BY GROUPING SETS ((b), ()) ORDER BY b"));
            assertEquals("1", one("SELECT count(*) FILTER (WHERE b = 1) FROM anf_t GROUP BY ()"));
        }

        @Test
        void in_a_scalar_subquery_and_as_an_operand() throws SQLException {
            assertEquals("1", one("SELECT (SELECT count(*) FILTER (WHERE b = 1) FROM anf_t) AS c"));
            assertEquals("2", one("SELECT count(*) FILTER (WHERE b = 1)"
                    + " + count(*) FILTER (WHERE b = 2) FROM anf_t"));
        }

        @Test
        void a_plain_function_inside_the_predicate_is_never_looked_at() throws SQLException {
            assertEquals("10", one("SELECT sum(v) FILTER (WHERE abs(b) = 1) FROM anf_t"));
            assertEquals("10", one("SELECT sum(v) FILTER (WHERE length(b::text) = 1 AND b = 1)"
                    + " FROM anf_t"));
            assertEquals("2", one("SELECT count(*) FILTER (WHERE EXISTS (SELECT 1)) FROM anf_t"));
        }

        @Test
        void an_aggregate_used_as_a_window_function_keeps_its_filter() throws SQLException {
            assertEquals("1 / 1", row("SELECT count(*) FILTER (WHERE b = 1) OVER () FROM anf_t"));
            assertEquals("10 / 10",
                    row("SELECT sum(v) FILTER (WHERE b = 1) OVER (ORDER BY id) FROM anf_t"));
            assertEquals("1 / 0",
                    row("SELECT count(*) FILTER (WHERE b = 1) OVER (PARTITION BY b)"
                            + " FROM anf_t ORDER BY id"));
            assertEquals("10 / 10",
                    row("SELECT sum(v) FILTER (WHERE b = 1) OVER (ORDER BY id"
                            + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM anf_t"));
            assertEquals("1 / 1",
                    row("SELECT count(*) FILTER (WHERE b = 1) OVER w FROM anf_t WINDOW w AS ()"));
            assertEquals("{10} / {10}",
                    row("SELECT array_agg(v) FILTER (WHERE b = 1) OVER () FROM anf_t"));
        }

        @Test
        void a_schema_qualified_aggregate_is_still_an_aggregate() throws SQLException {
            assertEquals("1", one("SELECT pg_catalog.count(*) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("10", one("SELECT pg_catalog.sum(v) FILTER (WHERE b = 1) FROM anf_t"));
            assertEquals("1 / 1",
                    row("SELECT pg_catalog.count(*) FILTER (WHERE b = 1) OVER () FROM anf_t"));
        }

        @Test
        void an_aggregate_the_user_declared_is_recognised_as_one() throws SQLException {
            // What CREATE AGGREGATE accumulates is a separate, pre-existing gap -- what matters
            // here is that the call is not refused for carrying a FILTER.
            one("SELECT anf_myagg(v) FILTER (WHERE b = 1) FROM anf_t");
            row("SELECT anf_myagg(v) FILTER (WHERE b = 1) OVER () FROM anf_t");
        }

        @Test
        void a_plain_function_without_a_filter_is_untouched() throws SQLException {
            assertEquals("10 / 20", row("SELECT abs(v) FROM anf_t ORDER BY v"));
            assertEquals("20 / 40", row("SELECT anf_double(v) FROM anf_t ORDER BY v"));
        }
    }

    // ---- the neighbouring answers that were already right -------------------

    @Nested
    @DisplayName("the refusals that were already exact keep their own message")
    class NeighbouringRefusals {

        @Test
        void a_window_function_keeps_its_own_two_answers() {
            refusedWith("SELECT row_number() FILTER (WHERE b = 1) OVER () FROM anf_t", "0A000",
                    "FILTER is not implemented for non-aggregate window functions");
            refusedWith("SELECT row_number() FILTER (WHERE b = 1) FROM anf_t", "42809",
                    "window function row_number requires an OVER clause");
            refusedWith("SELECT rank() FILTER (WHERE b = 1) FROM anf_t", "42809",
                    "window function rank requires an OVER clause");
        }

        @Test
        void an_unrecognised_name_is_a_missing_function_first() {
            refusedWith("SELECT anf_nosuch(v) FILTER (WHERE b = 1) FROM anf_t", "42883",
                    "function anf_nosuch(integer) does not exist");
        }

        @Test
        void an_aggregate_inside_the_filter_predicate_is_named_before_the_call() {
            refusedWith("SELECT abs(v) FILTER (WHERE count(*) > 0) FROM anf_t", "42803",
                    "aggregate functions are not allowed in FILTER");
        }

        @Test
        void filter_after_something_that_is_not_a_call_is_a_syntax_error() {
            refusedWith("SELECT v FILTER (WHERE b = 1) FROM anf_t", "42601",
                    "syntax error at or near \"FILTER\"");
            refusedWith("SELECT 1 FILTER (WHERE true)", "42601",
                    "syntax error at or near \"FILTER\"");
            refusedWith("SELECT coalesce(v,0) FILTER (WHERE b = 1) FROM anf_t", "42601",
                    "syntax error at or near \"FILTER\"");
            refusedWith("SELECT CAST(v AS text) FILTER (WHERE b = 1) FROM anf_t", "42601",
                    "syntax error at or near \"FILTER\"");
            refusedWith("SELECT (abs(v)) FILTER (WHERE b = 1) FROM anf_t", "42601",
                    "syntax error at or near \"FILTER\"");
        }
    }

    // ---- SECTION B: the array side ------------------------------------------

    @Nested
    @DisplayName("the array functions read while checking the reported null rule")
    class ArrayFunctions {

        @Test
        void the_function_spellings_of_the_three_operators_exist() throws SQLException {
            assertEquals("t", one("SELECT arraycontains(ARRAY[1,NULL]::int[], ARRAY[1]::int[])"));
            assertEquals("f", one("SELECT arraycontained(ARRAY[1,NULL]::int[], ARRAY[1,2]::int[])"));
            assertEquals("t", one("SELECT arrayoverlap(ARRAY[1,NULL]::int[], ARRAY[1]::int[])"));
            assertEquals("f", one("SELECT arrayoverlap(ARRAY[NULL]::int[], ARRAY[NULL]::int[])"));
            assertEquals("t", one("SELECT arraycontains(ARRAY['a',NULL]::text[], ARRAY['a']::text[])"));
            assertEquals("f", one("SELECT arraycontains(ARRAY[1,2]::int[], ARRAY[3]::int[])"));
            assertEquals("t", one("SELECT arraycontained(ARRAY[1]::int[], ARRAY[1,2]::int[])"));
            assertEquals("f", one("SELECT arrayoverlap(ARRAY[]::int[], ARRAY[]::int[])"));
            assertEquals("t", one("SELECT arraycontains(ARRAY[]::int[], ARRAY[]::int[])"));
        }

        @Test
        void they_are_strict_and_answer_in_boolean() throws SQLException {
            assertNull(one("SELECT arraycontains(NULL::int[], ARRAY[1]::int[])"));
            assertNull(one("SELECT arraycontains(ARRAY[1]::int[], NULL::int[])"));
            assertNull(one("SELECT arrayoverlap(NULL::int[], NULL::int[])"));
            assertEquals("boolean",
                    one("SELECT pg_typeof(arraycontains(ARRAY[1]::int[], ARRAY[1]::int[]))::text"));
        }

        @Test
        void array_position_has_no_null_rule_and_finds_a_null_when_asked_for_one()
                throws SQLException {
            assertEquals("3", one("SELECT array_position(ARRAY[1,NULL,2]::int[], 2)"));
            assertEquals("2", one("SELECT array_position(ARRAY[1,NULL,2]::int[], NULL::int)"));
            assertNull(one("SELECT array_position(ARRAY[1,NULL,2]::int[], 9)"));
            assertEquals("{1,3}", one("SELECT array_positions(ARRAY[1,NULL,1]::int[], 1)"));
            assertEquals("{2}", one("SELECT array_positions(ARRAY[1,NULL,1]::int[], NULL::int)"));
            assertEquals("{1,2}", one("SELECT array_positions(ARRAY[NULL,NULL]::int[], NULL::int)"));
            assertEquals("2", one("SELECT array_position(ARRAY['a',NULL,'b']::text[], NULL::text)"));
        }

        @Test
        void a_null_array_is_not_an_empty_array() throws SQLException {
            assertNull(one("SELECT array_positions(NULL::int[], 1)"));
            assertNull(one("SELECT array_remove(NULL::int[], 1)"));
            assertNull(one("SELECT array_replace(NULL::int[], 1, 2)"));
            assertNull(one("SELECT array_positions(NULL::text[], 'a')"));
            assertNull(one("SELECT array_position(NULL::int[], 1)"));
            assertEquals("{}", one("SELECT array_positions(ARRAY[]::int[], 1)"));
            assertEquals("{}", one("SELECT array_remove(ARRAY[]::int[], 1)"));
            assertNull(one("SELECT array_position(ARRAY[]::int[], 1)"));
        }

        @Test
        void removing_and_replacing_a_null_is_allowed() throws SQLException {
            assertEquals("{NULL,2}", one("SELECT array_remove(ARRAY[1,NULL,2]::int[], 1)"));
            assertEquals("{1,2}", one("SELECT array_remove(ARRAY[1,NULL,2]::int[], NULL::int)"));
            assertEquals("{a}", one("SELECT array_remove(ARRAY['a',NULL]::text[], NULL::text)"));
            assertEquals("{9,NULL,2}", one("SELECT array_replace(ARRAY[1,NULL,2]::int[], 1, 9)"));
            assertEquals("{1,9,2}",
                    one("SELECT array_replace(ARRAY[1,NULL,2]::int[], NULL::int, 9)"));
            assertEquals("{NULL,NULL,2}",
                    one("SELECT array_replace(ARRAY[1,NULL,2]::int[], 1, NULL::int)"));
        }

    }

    @Nested
    @DisplayName("a NULL element is matched against, never refused")
    class ArrayNullElements {

        /**
         * The 22004 reported for these operators is the intarray extension's, not PostgreSQL's.
         * A server without intarray answers exactly what is asserted here for integer[] too, so
         * the rule was not implemented; the operators are read below over the element types
         * intarray never redeclares, where every PostgreSQL agrees.
         */
        @Test
        void arrayNullElementsAreMatchedAgainst() throws SQLException {
            assertEquals("t", one("SELECT ARRAY['a',NULL]::text[] @> ARRAY['a']::text[]"));
            assertEquals("f", one("SELECT ARRAY['a']::text[] @> ARRAY['a',NULL]::text[]"));
            assertEquals("f", one("SELECT ARRAY['a',NULL]::text[] @> ARRAY[NULL]::text[]"));
            assertEquals("f", one("SELECT ARRAY[NULL]::text[] @> ARRAY[NULL]::text[]"));
            assertEquals("f", one("SELECT ARRAY[NULL]::text[] <@ ARRAY[NULL]::text[]"));
            assertEquals("f", one("SELECT ARRAY['a',NULL]::text[] <@ ARRAY['a',NULL]::text[]"));
            assertEquals("t", one("SELECT ARRAY['a',NULL]::text[] && ARRAY['a']::text[]"));
            assertEquals("f", one("SELECT ARRAY[NULL]::text[] && ARRAY[NULL]::text[]"));
            assertEquals("t", one("SELECT ARRAY['a',NULL]::text[] && ARRAY[NULL,'a']::text[]"));
        }

        @Test
        void an_empty_array_on_the_other_side_is_answered() throws SQLException {
            assertEquals("f", one("SELECT ARRAY[]::text[] @> ARRAY[NULL]::text[]"));
            assertEquals("t", one("SELECT ARRAY[NULL]::text[] @> ARRAY[]::text[]"));
            assertEquals("f", one("SELECT ARRAY[NULL]::text[] && ARRAY[]::text[]"));
        }

        @Test
        void a_null_array_value_is_null_and_the_other_operand_is_never_read() throws SQLException {
            assertNull(one("SELECT NULL::text[] @> ARRAY[NULL]::text[]"));
            assertNull(one("SELECT ARRAY[NULL]::text[] @> NULL::text[]"));
            assertNull(one("SELECT NULL::text[] && NULL::text[]"));
            assertNull(one("SELECT NULL::text[] <@ ARRAY[NULL]::text[]"));
            assertNull(one("SELECT NULL::int[] @> ARRAY[1]::int[]"));
        }

        @Test
        void every_other_element_type_reads_the_same_way() throws SQLException {
            assertEquals("t", one("SELECT ARRAY[1,NULL]::bigint[] @> ARRAY[1]::bigint[]"));
            assertEquals("f", one("SELECT ARRAY[NULL]::bigint[] @> ARRAY[1]::bigint[]"));
            assertEquals("t", one("SELECT ARRAY[1,NULL]::numeric[] @> ARRAY[1]::numeric[]"));
            assertEquals("t", one("SELECT ARRAY['2020-01-01',NULL]::date[]"
                    + " @> ARRAY['2020-01-01']::date[]"));
            assertEquals("t", one("SELECT ARRAY[true,NULL]::boolean[] @> ARRAY[true]::boolean[]"));
            assertEquals("t", one("SELECT ARRAY['a',NULL]::varchar[] && ARRAY['a']::varchar[]"));
        }

        @Test
        void a_null_free_integer_array_is_unaffected() throws SQLException {
            assertEquals("t", one("SELECT ARRAY[1,2,3] @> ARRAY[2,1]"));
            assertEquals("t", one("SELECT ARRAY[1,1] <@ ARRAY[1]"));
            assertEquals("t", one("SELECT ARRAY[1,2] && ARRAY[2]"));
            assertEquals("t", one("SELECT ARRAY[]::int[] @> ARRAY[]::int[]"));
            assertEquals("f", one("SELECT ARRAY[]::int[] && ARRAY[1]::int[]"));
            assertEquals("t", one("SELECT ARRAY[1]::int[] @> ARRAY[]::int[]"));
        }

        @Test
        void nothing_leaks_into_equality_length_or_concatenation() throws SQLException {
            assertEquals("t", one("SELECT ARRAY[1,NULL]::int[] = ARRAY[1,NULL]::int[]"));
            assertEquals("t", one("SELECT ARRAY[1,NULL]::int[] IS NOT NULL"));
            assertEquals("2", one("SELECT array_length(ARRAY[1,NULL]::int[],1)"));
            assertEquals("2", one("SELECT cardinality(ARRAY[1,NULL]::int[])"));
            assertEquals("{1,NULL,3}", one("SELECT ARRAY[1,NULL]::int[] || ARRAY[3]::int[]"));
            assertEquals("{1,NULL,3}",
                    one("SELECT array_cat(ARRAY[1,NULL]::int[], ARRAY[3]::int[])"));
            assertEquals("1,X", one("SELECT array_to_string(ARRAY[1,NULL]::int[], ',', 'X')"));
            assertEquals("1 / NULL", row("SELECT x FROM unnest(ARRAY[1,NULL]::int[]) AS x"
                    + " ORDER BY x NULLS LAST"));
        }

        @Test
        void the_operators_over_other_types_are_untouched() throws SQLException {
            assertEquals("t", one("SELECT '[null]'::jsonb @> '[null]'::jsonb"));
            assertEquals("t", one("SELECT '{\"a\":null}'::jsonb @> '{\"a\":null}'::jsonb"));
            assertEquals("t", one("SELECT int4range(1,5) @> int4range(2,3)"));
            assertEquals("t", one("SELECT int4range(1,5) && int4range(4,9)"));
            assertEquals("t", one("SELECT int4range(2,3) <@ int4range(1,5)"));
            assertEquals("t", one("SELECT inet '192.168.1.0/24' && inet '192.168.1.5/32'"));
            assertEquals("t", one("SELECT box '((0,0),(2,2))' @> point '(1,1)'"));
            assertEquals("t", one("SELECT circle '<(0,0),5>' && circle '<(1,1),1>'"));
        }

        @Test
        void a_column_with_a_null_element_answers_per_row() throws SQLException {
            assertEquals("1 / 2", row("SELECT id FROM anf_a WHERE ta @> ARRAY['b'] ORDER BY id"));
            assertEquals("1 / 2", row("SELECT id FROM anf_a WHERE ta && ARRAY['b'] ORDER BY id"));
            assertEquals("1 / 3",
                    row("SELECT id FROM anf_a WHERE ta <@ ARRAY['a','b'] ORDER BY id"));
            assertEquals("2",
                    row("SELECT id FROM anf_a WHERE ba && ARRAY[3]::bigint[] ORDER BY id"));
            assertEquals("1|t / 2|t / 3|f / 4|NULL",
                    row("SELECT id, ta @> ARRAY['b'] AS c FROM anf_a ORDER BY id"));
        }

        @Test
        void a_multidimensional_array_is_flattened_for_the_operators() throws SQLException {
            assertEquals("t", one("SELECT ARRAY[[1,2],[3,4]]::text[] @> ARRAY['1']::text[]"));
            assertEquals("t", one("SELECT ARRAY[[1,2],[3,4]]::text[] && ARRAY['3']::text[]"));
            assertEquals("t",
                    one("SELECT ARRAY[[1,2],[3,4]]::text[] <@ ARRAY['1','2','3','4']::text[]"));
        }
    }
}
