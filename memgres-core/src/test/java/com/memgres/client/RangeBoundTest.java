package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A range's bounds are values of its element type, and PostgreSQL treats them that way from end to
 * end: it reads them with the element's input function, writes them with its output function,
 * hands them back from {@code lower()}/{@code upper()} as values of it, and compares them against
 * probes on the same scale. Getting one of those without the others produces a range that looks
 * right and compares wrong, so they are all checked here together.
 *
 * <p>The second half covers the hypothetical-set aggregates, whose direct arguments are the row
 * being ranked: they have to match the WITHIN GROUP sort columns one for one, and every sort
 * column has to take part in the comparison.
 */
public class RangeBoundTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement st = conn.createStatement()) {
            st.execute("SET TIME ZONE 'UTC'");
            st.execute("CREATE TABLE rb_r (id int, ts tsrange, dr daterange, nr numrange, ir int4range)");
            st.execute("INSERT INTO rb_r VALUES (1, '[2020-01-01,2020-02-01)',"
                    + " '[2020-01-01,2020-02-01)', '[1.5,9.5)', '[1,10)')");
            st.execute("INSERT INTO rb_r VALUES (2, NULL, NULL, NULL, NULL)");
            st.execute("CREATE VIEW rb_v AS SELECT id, ts FROM rb_r");
            st.execute("CREATE TABLE rb_t (v int)");
            st.execute("INSERT INTO rb_t VALUES (1), (2), (3)");
            st.execute("CREATE TABLE rb_h2 (v int, w int)");
            st.execute("INSERT INTO rb_h2 VALUES (1,1), (1,5), (1,9), (2,1)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- helpers ----

    private static String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertOne(String expected, String sql) throws SQLException {
        assertEquals(expected, one(sql), sql);
    }

    /** JDBC renders a boolean column as "t"/"f" in simple query mode; compare the value itself. */
    private static void assertBool(boolean expected, String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            assertEquals(expected, rs.getBoolean(1), sql);
        }
    }

    private static void assertError(String sqlState, String messagePart, String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            fail("expected " + sqlState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
            assertTrue(e.getMessage().contains(messagePart),
                    sql + " -> expected \"" + messagePart + "\" in \"" + e.getMessage() + "\"");
        }
    }

    // ---- (a) infinite bounds are ordinary bounds ----

    @Test
    void temporalRangesReadAndWriteTheirInfinities() throws Exception {
        assertOne("[\"2020-01-01 00:00:00\",infinity)", "SELECT '[2020-01-01,infinity)'::tsrange");
        assertOne("[\"2020-01-01 00:00:00\",infinity)", "SELECT '[\"2020-01-01\",infinity)'::tsrange");
        assertOne("[\"2020-01-01 00:00:00+00\",infinity)", "SELECT '[2020-01-01,infinity)'::tstzrange");
        assertOne("[2020-01-01,infinity)", "SELECT '[2020-01-01,infinity)'::daterange");
        assertOne("[2020-01-01,infinity]", "SELECT '[2020-01-01,infinity]'::daterange");
        assertOne("[infinity,infinity]", "SELECT '[infinity,infinity]'::tsrange");
        assertOne("(,infinity)", "SELECT '(,infinity)'::tsrange");
        assertOne("[-infinity,infinity]", "SELECT '[-infinity,infinity]'::tsrange");
    }

    @Test
    void anInfiniteBoundIsNotAnUnboundedOne() throws Exception {
        assertBool(false, "SELECT upper_inf('[2020-01-01,infinity)'::tsrange)");
        assertBool(false, "SELECT lower_inf('[-infinity,2020-01-01)'::tsrange)");
        assertBool(false, "SELECT upper_inf('[2020-01-01,infinity)'::daterange)");
        assertBool(false, "SELECT isempty('[2020-01-01,infinity)'::tsrange)");
        assertOne("infinity", "SELECT upper('[2020-01-01,infinity)'::tsrange)");
        assertOne("infinity", "SELECT upper('[2020-01-01,infinity)'::daterange)");
        assertOne("2020-01-01", "SELECT lower('[2020-01-01,infinity)'::daterange)");
        assertBool(true, "SELECT '[2020-01-01,infinity)'::daterange @> '2021-01-01'::date");
    }

    @Test
    void numericRangesTakeNumericsOwnSpecials() throws Exception {
        assertOne("[1,Infinity)", "SELECT '[1,Infinity)'::numrange");
        assertOne("[1,Infinity)", "SELECT '[1,infinity)'::numrange");
        assertOne("(-Infinity,1]", "SELECT '(-Infinity,1]'::numrange");
        assertOne("[1,NaN)", "SELECT '[1,NaN)'::numrange");
        assertOne("Infinity", "SELECT upper('[1,Infinity)'::numrange)");
        assertOne("1", "SELECT lower('[1,Infinity)'::numrange)");
        assertBool(true, "SELECT '[1,Infinity)'::numrange @> 1e10");
    }

    @Test
    void anIntegerHasNoInfinitySoItsRangeTypeRefusesTheWord() {
        assertError("22P02", "invalid input syntax for type integer: \"infinity\"",
                "SELECT '[1,infinity)'::int4range");
    }

    // ---- (a) lower()/upper() answer in the element type ----

    @Test
    void boundsComeBackAsValuesOfTheElementType() throws Exception {
        assertOne("2020-02-01 00:00:00", "SELECT upper('[2020-01-01,2020-02-01)'::tsrange)");
        assertOne("timestamp without time zone",
                "SELECT pg_typeof(upper('[2020-01-01,2020-02-01)'::tsrange))::text");
        assertOne("2020-01-01", "SELECT lower('[2020-01-01,2020-02-01)'::daterange)");
        assertOne("date", "SELECT pg_typeof(lower('[2020-01-01,2020-02-01)'::daterange))::text");
        assertOne("2020-02-01", "SELECT upper('[2020-01-01,2020-02-01)'::daterange)");
        assertOne("2020-01-01 13:00:00",
                "SELECT upper('[\"2020-01-01 12:00:00\",\"2020-01-01 13:00:00\")'::tsrange)");
        assertOne("2020-02-01 00:00:00",
                "SELECT upper(tsrange('2020-01-01'::timestamp,'2020-02-01'::timestamp))");
        assertOne("2020-01-01 12:34:56", "SELECT lower('[2020-01-01 12:34:56,2020-02-01)'::tsrange)");
    }

    @Test
    void containmentComparesTheProbeOnTheBoundsOwnScale() throws Exception {
        assertBool(true, "SELECT '[2020-01-01,2020-02-01)'::tsrange @> '2020-01-15'::timestamp");
        assertBool(false, "SELECT '[2020-01-01,2020-02-01)'::tsrange @> '2020-03-15'::timestamp");
        assertBool(true, "SELECT '[2020-01-01,2020-02-01)'::daterange @> '2020-01-15'::date");
        assertBool(false, "SELECT '[2020-01-01,2020-02-01)'::daterange @> '2020-03-15'::date");
    }

    // ---- (a) the text form is the element type's text form ----

    @Test
    void timestampBoundsAreQuotedAndNormalised() throws Exception {
        assertOne("[\"2020-01-01 00:00:00\",\"2020-02-01 00:00:00\")",
                "SELECT '[2020-01-01,2020-02-01)'::tsrange");
        assertOne("[\"2020-01-01 00:00:00+00\",\"2020-02-01 00:00:00+00\")",
                "SELECT '[2020-01-01,2020-02-01)'::tstzrange");
        assertOne("[\"2020-01-01 00:00:00.5\",\"2020-02-01 00:00:00\")",
                "SELECT '[2020-01-01 00:00:00.5,2020-02-01)'::tsrange");
        assertOne("[\"2020-02-01 00:00:00\",\"2020-03-01 00:00:00\")",
                "SELECT '[2020-01-01,2020-03-01)'::tsrange * '[2020-02-01,2020-04-01)'::tsrange");
        assertOne("[\"2020-01-01 00:00:00\",\"2020-04-01 00:00:00\")",
                "SELECT range_merge('[2020-01-01,2020-02-01)'::tsrange,"
                        + " '[2020-03-01,2020-04-01)'::tsrange)");
        assertOne("{[\"2020-01-01 00:00:00\",\"2020-02-01 00:00:00\")}",
                "SELECT '{[2020-01-01,2020-02-01)}'::tsmultirange");
        assertBool(true, "SELECT '[2020-01-01,2020-02-01)'::tsrange"
                + " = '[\"2020-01-01 00:00:00\",\"2020-02-01 00:00:00\")'::tsrange");
    }

    @Test
    void aDateRangeIsDiscreteAndATimestampRangeIsNot() throws Exception {
        assertOne("[2020-01-01,2020-02-02)", "SELECT '[2020-01-01,2020-02-01]'::daterange");
        assertOne("[2020-01-02,2020-02-01)", "SELECT '(2020-01-01,2020-02-01)'::daterange");
        assertOne("[\"2020-01-01 00:00:00\",\"2020-02-01 00:00:00\"]",
                "SELECT '[2020-01-01,2020-02-01]'::tsrange");
    }

    @Test
    void aRangeLiteralIsNotAMultirangeLiteral() throws Exception {
        assertError("22P02", "malformed multirange literal",
                "SELECT '[2020-01-01,2020-02-01)'::tsmultirange");
        assertError("22P02", "malformed multirange literal", "SELECT '[1,4)'::int4multirange");
        // A range value, as opposed to a written literal, does cast to its multirange type.
        assertOne("{[1,4)}", "SELECT int4range(1,4)::int4multirange");
        assertOne("{[1,5),[10,20)}", "SELECT '{[1,5),[10,20)}'::int4multirange");
    }

    // ---- (a) neighbours: table columns, views, subqueries, NULLs, clauses ----

    @Test
    void aRangeColumnStoresTheElementTypesCanonicalForm() throws Exception {
        assertOne("[\"2020-01-01 00:00:00\",\"2020-02-01 00:00:00\")",
                "SELECT ts FROM rb_r WHERE id = 1");
        assertOne("[2020-01-01,2020-02-01)", "SELECT dr FROM rb_r WHERE id = 1");
        assertOne("[1.5,9.5)", "SELECT nr FROM rb_r WHERE id = 1");
        assertOne("[1,10)", "SELECT ir FROM rb_r WHERE id = 1");
        assertOne("[\"2020-01-01 00:00:00\",\"2020-02-01 00:00:00\")",
                "SELECT ts FROM rb_v WHERE id = 1");
    }

    @Test
    void boundsOfAStoredRangeKeepTheirType() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT lower(ts), upper(ts), lower(dr), upper(dr),"
                     + " lower(nr), lower(ir) FROM rb_r WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals("2020-01-01 00:00:00", rs.getString(1));
            assertEquals("2020-02-01 00:00:00", rs.getString(2));
            assertEquals("2020-01-01", rs.getString(3));
            assertEquals("2020-02-01", rs.getString(4));
            assertEquals("1.5", rs.getString(5));
            assertEquals("1", rs.getString(6));
        }
    }

    @Test
    void aNullRangeHasNoBoundsToReport() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT lower(ts), upper(ts) FROM rb_r WHERE id = 2")) {
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertNull(rs.getString(2));
        }
        assertOne(null, "SELECT lower(NULL::tsrange)");
        assertOne(null, "SELECT upper(NULL::daterange)");
        assertOne(null, "SELECT lower('empty'::int4range)");
    }

    @Test
    void typedBoundsSurviveWhereOrderByGroupByAndSubqueries() throws Exception {
        assertOne("1", "SELECT id FROM rb_r WHERE ts @> '2020-01-15'::timestamp");
        assertOne("1", "SELECT id FROM rb_r WHERE dr @> '2020-01-15'::date");
        assertOne("1", "SELECT id FROM rb_r WHERE ir @> 5");
        assertOne("2020-02-01 00:00:00",
                "SELECT upper(ts) FROM rb_r WHERE id = 1 ORDER BY upper(ts)");
        assertOne("1", "SELECT id FROM rb_r GROUP BY id, ts ORDER BY id");
        // A derived column keeps the type its expression produced; a declared type must not be
        // guessed from the range's text and used to reject the comparison.
        assertOne("2020-01-01 00:00:00",
                "SELECT sub.lo FROM (SELECT lower(ts) AS lo FROM rb_r WHERE id = 1) sub"
                        + " WHERE sub.lo >= '2019-01-01'::timestamp");
        assertOne("{[\"2020-01-01 00:00:00\",\"2020-02-01 00:00:00\")}",
                "SELECT range_agg(ts) FROM rb_r");
        assertOne("{[1,10)}", "SELECT range_agg(ir) FROM rb_r");
    }

    // ---- (a) neighbours: the integer and numeric ranges are unchanged ----

    @Test
    void integerAndNumericRangesKeepWorking() throws Exception {
        assertOne("[1,10)", "SELECT '[1,10)'::int4range");
        assertOne("1", "SELECT lower('[1,10)'::int4range)");
        assertOne("10", "SELECT upper('[1,10)'::int4range)");
        assertBool(true, "SELECT '[1,10)'::int4range @> 5");
        assertBool(false, "SELECT '[1,10)'::int4range @> 10");
        assertOne("[1.5,9.5)", "SELECT '[1.5,9.5)'::numrange");
        assertOne("1.5", "SELECT lower('[1.5,9.5)'::numrange)");
        assertBool(true, "SELECT '[1.5,9.5)'::numrange @> 2.0");
        assertOne("empty", "SELECT '[1,1)'::int4range");
        assertOne("(,5)", "SELECT '[,5)'::int4range");
        assertOne("[5,)", "SELECT '[5,)'::int4range");
        assertBool(true, "SELECT isempty('empty'::int4range)");
    }

    @Test
    void rangeArithmeticStillAnswersInTheSameShapes() throws Exception {
        assertOne("[1,10)", "SELECT '[1,10)'::int4range * '[0,20)'::int4range");
        assertOne("[5,10)", "SELECT '[1,10)'::int4range - '[0,5)'::int4range");
        assertOne("[1,10)", "SELECT '[1,5)'::int4range + '[5,10)'::int4range");
        assertOne("{[1,3)}", "SELECT '{[1,5)}'::int4multirange - '{[3,7)}'::int4multirange");
        assertOne("{[1,5),[10,20)}",
                "SELECT '{[1,20)}'::int4multirange - '{[5,10)}'::int4multirange");
        assertBool(true, "SELECT '[2020-01-01,2020-02-01)'::tsrange"
                + " && '[2020-01-15,2020-03-01)'::tsrange");
        assertBool(true, "SELECT '[2020-01-01,2020-02-01)'::tsrange"
                + " @> '[2020-01-10,2020-01-20)'::tsrange");
        assertBool(true, "SELECT '[2020-01-01,2020-02-01)'::tsrange"
                + " << '[2020-03-01,2020-04-01)'::tsrange");
        assertBool(true, "SELECT '[2020-01-01,2020-02-01)'::tsrange"
                + " -|- '[2020-02-01,2020-03-01)'::tsrange");
    }

    @Test
    void lowerAndUpperOnTextAreStillTheCaseFoldingFunctions() throws Exception {
        assertOne("hello", "SELECT lower('HELLO')");
        assertOne("HELLO", "SELECT upper('hello')");
    }

    @Test
    void aLiteralWithThreeBoundsIsNoRangeAtAll() {
        assertError("22P02", "malformed range literal", "SELECT '[1,2,3)'::int4range");
    }

    // ---- (b) hypothetical-set aggregates compare every sort column ----

    @Test
    void everySortColumnTakesPartInTheComparison() throws Exception {
        assertOne("2", "SELECT rank(1,5) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
        assertOne("3", "SELECT rank(1,9) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
        assertOne("4", "SELECT rank(1,10) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
        assertOne("2", "SELECT dense_rank(1,5) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
        assertOne("0.6", "SELECT cume_dist(1,5) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
        assertOne("0.25", "SELECT percent_rank(1,5) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
    }

    @Test
    void eachSortColumnsDirectionIsHonoured() throws Exception {
        assertOne("2", "SELECT rank(1,5) WITHIN GROUP (ORDER BY v, w DESC) FROM rb_h2");
        assertOne("3", "SELECT rank(1,5) WITHIN GROUP (ORDER BY v DESC, w) FROM rb_h2");
    }

    @Test
    void aNullInTheHypotheticalRowSortsWhereTheClauseSaysItDoes() throws Exception {
        assertOne("4", "SELECT rank(NULL) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertOne("1", "SELECT rank(NULL) WITHIN GROUP (ORDER BY v NULLS FIRST) FROM rb_t");
        assertOne("4", "SELECT rank(1,NULL) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
    }

    // ---- (b) the argument count must match the sort column count ----

    @Test
    void tooManyDirectArgumentsResolveToNoFunction() {
        assertError("42883", "function rank(integer, integer, integer) does not exist",
                "SELECT rank(1,2) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertError("42883", "function dense_rank(integer, integer, integer) does not exist",
                "SELECT dense_rank(1,2) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertError("42883", "function percent_rank(integer, integer, integer) does not exist",
                "SELECT percent_rank(1,2) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertError("42883", "function cume_dist(integer, integer, integer) does not exist",
                "SELECT cume_dist(1,2) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertError("42883", "function rank(integer, integer, integer, integer, integer) does not exist",
                "SELECT rank(2,20,3) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
    }

    @Test
    void tooFewDirectArgumentsResolveToNoFunction() {
        assertError("42883", "function rank(integer, integer, integer) does not exist",
                "SELECT rank(2) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
        assertError("42883", "function rank(integer) does not exist",
                "SELECT rank() WITHIN GROUP (ORDER BY v) FROM rb_t");
    }

    @Test
    void theOtherOrderedSetAggregatesHaveFixedAritiesToo() {
        assertError("42883", "function percentile_cont(numeric, numeric, integer) does not exist",
                "SELECT percentile_cont(0.5,0.9) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertError("42883", "function percentile_disc(numeric, integer, integer) does not exist",
                "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
        assertError("42883", "function mode(integer, integer) does not exist",
                "SELECT mode(1) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertError("42883", "function mode(integer, integer) does not exist",
                "SELECT mode() WITHIN GROUP (ORDER BY v, w) FROM rb_h2");
    }

    // ---- (b) neighbours: the well-formed calls keep working ----

    @Test
    void wellFormedOrderedSetCallsKeepWorking() throws Exception {
        assertOne("2", "SELECT rank(2) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertOne("2", "SELECT dense_rank(2) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertOne("0.3333333333333333", "SELECT percent_rank(2) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertOne("0.75", "SELECT cume_dist(2) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertOne("2", "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertOne("2", "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertOne("1", "SELECT mode() WITHIN GROUP (ORDER BY v) FROM rb_t");
        assertOne("2", "SELECT rank(2) WITHIN GROUP (ORDER BY v DESC) FROM rb_t");
        assertOne("2", "SELECT rank(1.5) WITHIN GROUP (ORDER BY v) FROM rb_t");
    }

    @Test
    void orderedSetAggregatesStillWorkPerGroupAndOverEmptyGroups() throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT v, rank(2) WITHIN GROUP (ORDER BY w) AS r"
                     + " FROM rb_h2 GROUP BY v ORDER BY v")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
        }
        assertOne("1", "SELECT rank(2) WITHIN GROUP (ORDER BY v) FROM rb_t WHERE false");
        assertOne("0", "SELECT percent_rank(2) WITHIN GROUP (ORDER BY v) FROM rb_t WHERE false");
        assertOne("1", "SELECT cume_dist(2) WITHIN GROUP (ORDER BY v) FROM rb_t WHERE false");
    }

    @Test
    void aWithinGroupClauseOnAnOrdinaryAggregateIsStillRejected() {
        assertError("42809", "is not an ordered-set aggregate",
                "SELECT count(*) WITHIN GROUP (ORDER BY v) FROM rb_t");
    }
}
