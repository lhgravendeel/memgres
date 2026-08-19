package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An aggregate call is written the way the grammar writes it, and resolved by its signature.
 *
 * <p>The argument list may be introduced by ALL or by DISTINCT — one of them, and then arguments.
 * ALL is the default written down, so it belongs to every call and not only to the aggregates;
 * memgres read only DISTINCT, which left {@code count(ALL v)} a syntax error and {@code abs(ALL 1)}
 * a column named "all".
 *
 * <p>The star in {@code f(*)} says which rows to accumulate over, not what to accumulate: the call
 * has no arguments and resolves only against a signature declared over none. Reading the first
 * argument of a call that has none made {@code sum(*)} an internal fault. The same fact read the
 * other way is why {@code count()} is not how a parameterless aggregate is written.
 *
 * <p>GROUPING is a production of the grammar in its own right and admits nothing after its closing
 * parenthesis; what may follow an ordinary call is WITHIN GROUP, then FILTER, then OVER, so an
 * ordered-set aggregate may carry a FILTER. And a direct argument written as a literal takes the
 * type its signature declares: a percentile fraction is a double precision, and the value a
 * hypothetical-set aggregate ranks has its sort column's type.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class AggregateCallGrammarTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ag_a (v int, g text)");
            st.execute("INSERT INTO ag_a VALUES (10, 'x'), (20, 'x'), (30, 'y')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The first column of the first row, rendered as text. */
    private static String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            String v = rs.getString(1);
            return rs.wasNull() ? null : v;
        }
    }

    private static String stateOf(String sql) {
        PSQLException e = assertThrows(PSQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected a refusal from: " + sql);
        return e.getSQLState();
    }

    private static String messageOf(String sql) {
        PSQLException e = assertThrows(PSQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected a refusal from: " + sql);
        return e.getServerErrorMessage().getMessage();
    }

    /** ALL is the default written down, so an argument list may say so. */
    @Test
    void allIsTheOtherHalfOfDistinct() throws Exception {
        assertEquals("3", one("SELECT count(ALL v) FROM ag_a"));
        assertEquals("60", one("SELECT sum(ALL v) FROM ag_a"));
        assertEquals("10,20,30", one("SELECT string_agg(ALL v::text, ',' ORDER BY v) FROM ag_a"));
        assertEquals("{10,20,30}", one("SELECT array_agg(ALL v ORDER BY v) FROM ag_a"));
        assertEquals("2", one("SELECT count(ALL v) FILTER (WHERE v > 10) FROM ag_a"));
    }

    /** And it belongs to every call, not only to the aggregates. */
    @Test
    void everyCallMaySayAllNotOnlyAnAggregate() throws Exception {
        assertEquals("1", one("SELECT abs(ALL -1)"));
        assertEquals("A", one("SELECT upper(ALL 'a')"));
        assertEquals("DISTINCT specified, but abs is not an aggregate function",
                messageOf("SELECT abs(DISTINCT -1)"));
    }

    /** One of the two words introduces the list, so the other cannot begin it. */
    @Test
    void oneOfTheTwoWordsAndThenArguments() throws Exception {
        assertEquals("42601", stateOf("SELECT count(ALL DISTINCT v) FROM ag_a"));
        assertEquals("42601", stateOf("SELECT count(DISTINCT ALL v) FROM ag_a"));
        assertEquals("2", one("SELECT count(DISTINCT v) FROM ag_a WHERE v > 10"));
    }

    /** A star is not an argument list, so neither word may introduce one. */
    @Test
    void aStarIsNotAnArgumentList() throws Exception {
        assertEquals("syntax error at or near \"*\"",
                messageOf("SELECT count(DISTINCT *) FROM ag_a"));
        assertEquals("syntax error at or near \"*\"",
                messageOf("SELECT count(ALL *) FROM ag_a"));
        assertEquals("3", one("SELECT count(*) FROM ag_a"));
        assertEquals("2", one("SELECT count(*) FILTER (WHERE v > 10) FROM ag_a"));
    }

    /** The star says which rows to accumulate over, so the call has no arguments at all. */
    @Test
    void aCallWrittenWithAStarResolvesAgainstNoArgumentsAtAll() throws Exception {
        assertEquals("function sum() does not exist", messageOf("SELECT sum(*) FROM ag_a"));
        assertEquals("function max() does not exist", messageOf("SELECT max(*) FROM ag_a"));
        assertEquals("function avg() does not exist", messageOf("SELECT avg(*) FROM ag_a"));
        assertEquals("function string_agg() does not exist",
                messageOf("SELECT string_agg(*) FROM ag_a"));
        assertEquals("42883", stateOf("SELECT bool_and(*) FROM ag_a"));
    }

    /** And read the other way: the list is not what is empty. */
    @Test
    void aParameterlessAggregateIsWrittenWithTheStar() throws Exception {
        assertEquals("count(*) must be used to call a parameterless aggregate function",
                messageOf("SELECT count() FROM ag_a"));
        assertEquals("42809", stateOf("SELECT count() FROM ag_a"));
        assertEquals("function sum() does not exist", messageOf("SELECT sum() FROM ag_a"));
        assertEquals("function abs() does not exist", messageOf("SELECT abs() FROM ag_a"));
    }

    /** GROUPING is spelled like a call but written only one way. */
    @Test
    void groupingIsAProductionOfTheGrammar() throws Exception {
        assertEquals("0", one("SELECT grouping(v) FROM ag_a GROUP BY v ORDER BY 1"));
        assertEquals("syntax error at or near \")\"",
                messageOf("SELECT grouping() FROM ag_a GROUP BY v"));
        assertEquals("syntax error at or near \"DISTINCT\"",
                messageOf("SELECT grouping(DISTINCT v) FROM ag_a GROUP BY v"));
        assertEquals("syntax error at or near \"ALL\"",
                messageOf("SELECT grouping(ALL v) FROM ag_a GROUP BY v"));
    }

    /** And nothing may follow its closing parenthesis. */
    @Test
    void nothingFollowsAGroupingCall() throws Exception {
        assertEquals("syntax error at or near \"OVER\"",
                messageOf("SELECT grouping(v) OVER () FROM ag_a GROUP BY v"));
        assertEquals("syntax error at or near \"FILTER\"",
                messageOf("SELECT grouping(v) FILTER (WHERE true) FROM ag_a GROUP BY v"));
        assertEquals("syntax error at or near \"WITHIN\"",
                messageOf("SELECT grouping(v) WITHIN GROUP (ORDER BY v) FROM ag_a GROUP BY v"));
    }

    /** WITHIN GROUP, then FILTER, then OVER — the order the grammar writes them in. */
    @Test
    void anOrderedSetAggregateMayCarryAFilter() throws Exception {
        assertEquals("25",
                one("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v)"
                        + " FILTER (WHERE v > 10) FROM ag_a"));
        assertEquals("20",
                one("SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY v)"
                        + " FILTER (WHERE v > 10) FROM ag_a"));
        assertEquals("20",
                one("SELECT mode() WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM ag_a"));
        assertEquals("1",
                one("SELECT rank(20) WITHIN GROUP (ORDER BY v) FILTER (WHERE v > 10) FROM ag_a"));
    }

    /** A predicate nothing satisfies leaves it with no rows, as it does any other aggregate. */
    @Test
    void aPredicateNothingSatisfiesLeavesNoRows() throws Exception {
        assertNull(one("SELECT mode() WITHIN GROUP (ORDER BY v) FILTER (WHERE false) FROM ag_a"));
        assertNull(one("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v)"
                + " FILTER (WHERE false) FROM ag_a"));
        assertNull(one("SELECT sum(v) FILTER (WHERE false) FROM ag_a"));
    }

    /** Written the other way round there is nowhere for the WITHIN GROUP to go. */
    @Test
    void aFilterWrittenFirstLeavesNowhereForTheWithinGroup() throws Exception {
        assertEquals("syntax error at or near \"WITHIN\"",
                messageOf("SELECT percentile_cont(0.5) FILTER (WHERE true)"
                        + " WITHIN GROUP (ORDER BY v) FROM ag_a"));
        assertEquals("syntax error at or near \"WITHIN\"",
                messageOf("SELECT count(v) FILTER (WHERE true)"
                        + " WITHIN GROUP (ORDER BY v) FROM ag_a"));
    }

    /** A FILTER on a window call is still read where it always was. */
    @Test
    void aFilterOnAWindowCallIsReadWhereItAlwaysWas() throws Exception {
        assertEquals("2", one("SELECT count(*) FILTER (WHERE v > 10) OVER () FROM ag_a"));
        assertEquals("20", one("SELECT sum(v) FILTER (WHERE v > 10) OVER (ORDER BY v) FROM ag_a"
                + " ORDER BY 1"));
    }

    /** A percentile fraction is a double precision, and is read by that type. */
    @Test
    void aPercentileFractionIsADoublePrecision() throws Exception {
        assertEquals("20", one("SELECT percentile_cont('0.5') WITHIN GROUP (ORDER BY v) FROM ag_a"));
        assertEquals("20", one("SELECT percentile_disc('0.5') WITHIN GROUP (ORDER BY v) FROM ag_a"));
        assertEquals("invalid input syntax for type double precision: \"zz\"",
                messageOf("SELECT percentile_cont('zz') WITHIN GROUP (ORDER BY v) FROM ag_a"));
    }

    /** The value a hypothetical-set aggregate ranks has the type of its sort column. */
    @Test
    void theValueBeingRankedHasItsSortColumnsType() throws Exception {
        assertEquals("2", one("SELECT rank('20') WITHIN GROUP (ORDER BY v) FROM ag_a"));
        assertEquals("invalid input syntax for type integer: \"zz\"",
                messageOf("SELECT rank('zz') WITHIN GROUP (ORDER BY v) FROM ag_a"));
        assertEquals("22P02", stateOf("SELECT dense_rank('zz') WITHIN GROUP (ORDER BY v) FROM ag_a"));
        assertEquals("22P02",
                stateOf("SELECT percent_rank('zz') WITHIN GROUP (ORDER BY v) FROM ag_a"));
        // A sort column that is text reads the same literal as a value of its own.
        assertEquals("4", one("SELECT rank('zz') WITHIN GROUP (ORDER BY g) FROM ag_a"));
        assertEquals("2", one("SELECT rank(20, 'x') WITHIN GROUP (ORDER BY v, g) FROM ag_a"));
    }
}
