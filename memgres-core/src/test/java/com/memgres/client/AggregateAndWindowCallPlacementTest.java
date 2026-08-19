package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An aggregate or window call is found wherever it is written.
 *
 * <p>Whether a query is grouped, and which calls the window pass has to answer for, was decided
 * by naming the expression node types one at a time. That answers only for the shapes somebody
 * remembered to list: a call written under BETWEEN, inside ARRAY[] or ROW(), in an IN list, under
 * COLLATE, or subscripted, was not found. The query then ran ungrouped over rows PostgreSQL folds
 * into one, or took the plain path where such a call has no value and the row answered NULL --
 * and a HAVING written that way kept no rows at all.
 */
class AggregateAndWindowCallPlacementTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE agc_t (g int, v int, s text)");
            st.execute("INSERT INTO agc_t VALUES (1, 10, 'a'), (1, 20, 'b'), (2, 30, 'c')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) b.append('|');
                    String v = rs.getString(i);
                    b.append(rs.wasNull() ? "NULL" : v);
                }
                out.add(b.toString());
            }
            return out;
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** One row per group is what the aggregate asks for, whatever it is written inside. */
    @Test
    void anAggregateUnderBetweenGroupsTheQuery() throws Exception {
        assertEquals(List.of("t"), rows("SELECT max(v) BETWEEN 1 AND 100 FROM agc_t"));
        assertEquals(List.of("t"), rows("SELECT 15 BETWEEN min(v) AND max(v) FROM agc_t"));
    }

    @Test
    void anAggregateInAConstructorGroupsTheQuery() throws Exception {
        assertEquals(List.of("{3}"), rows("SELECT ARRAY[count(*)] FROM agc_t"));
        assertEquals(List.of("(10,30)"), rows("SELECT ROW(min(v), max(v)) FROM agc_t"));
    }

    /** The list an IN reads is folded over the group as much as the value tested against it. */
    @Test
    void anAggregateInAnInListGroupsTheQuery() throws Exception {
        assertEquals(List.of("t"), rows("SELECT 3 IN (count(*), 99) FROM agc_t"));
        assertEquals(List.of("t"), rows("SELECT count(*) IN (1, 2, 3) FROM agc_t"));
        assertEquals(List.of("t"), rows("SELECT count(*) IN (SELECT 3) FROM agc_t"));
    }

    @Test
    void anAggregateUnderCollateOrABooleanTestGroupsTheQuery() throws Exception {
        assertEquals(List.of("c"), rows("SELECT max(s) COLLATE \"C\" FROM agc_t"));
        assertEquals(List.of("t"), rows("SELECT (max(v) > 5) IS TRUE FROM agc_t"));
        assertEquals(List.of("f"), rows("SELECT (min(v) IS NULL) IS NOT FALSE FROM agc_t"));
    }

    /** A subscript reads into the array the aggregates built, and they build it once. */
    @Test
    void aSubscriptReadsTheArrayTheAggregatesBuilt() throws Exception {
        assertEquals(List.of("10"), rows("SELECT (ARRAY[min(v), max(v)])[1] FROM agc_t"));
        assertEquals(List.of("30"), rows("SELECT (ARRAY[min(v), max(v)])[2] FROM agc_t"));
    }

    /** An ordered-set aggregate is an aggregate too. */
    @Test
    void anOrderedSetAggregateIsFoundWhereverItIsWritten() throws Exception {
        assertEquals(List.of("t"), rows(
                "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) BETWEEN 1 AND 100"
                        + " FROM agc_t"));
        assertEquals(List.of("{20}"), rows(
                "SELECT ARRAY[percentile_disc(0.5) WITHIN GROUP (ORDER BY v)] FROM agc_t"));
    }

    /** What HAVING says is read over the group, so a group it kept is a group it answered for. */
    @Test
    void havingReadsTheGroupWhateverTheTestIsWrittenInside() throws Exception {
        assertEquals(List.of("2"),
                rows("SELECT g FROM agc_t GROUP BY g HAVING count(*) BETWEEN 1 AND 1"));
        assertEquals(List.of("2"),
                rows("SELECT g FROM agc_t GROUP BY g HAVING 1 IN (count(*), 5)"));
    }

    /** A window call has its value on the row, and the expression around it is ordinary. */
    @Test
    void aWindowCallUnderBetweenAnswers() throws Exception {
        assertEquals(List.of("t", "t", "t"),
                rows("SELECT rank() OVER () BETWEEN 1 AND 5 FROM agc_t"));
        assertEquals(List.of("f", "f", "f"),
                rows("SELECT v BETWEEN 1 AND rank() OVER () FROM agc_t"));
    }

    @Test
    void aWindowCallInAConstructorOrAnInListAnswers() throws Exception {
        assertEquals(List.of("{1}", "{2}", "{3}"),
                rows("SELECT ARRAY[rank() OVER (ORDER BY v)] FROM agc_t ORDER BY 1"));
        assertEquals(List.of("(1)", "(2)", "(3)"),
                rows("SELECT ROW(rank() OVER (ORDER BY v)) FROM agc_t ORDER BY 1"));
        assertEquals(List.of("f", "f", "t"),
                rows("SELECT 1 IN (rank() OVER (ORDER BY v)) FROM agc_t ORDER BY 1"));
    }

    /** A window named by a WINDOW clause answers the same in either place it is read. */
    @Test
    void aNamedWindowAnswersInsideAConstructorToo() throws Exception {
        assertEquals(List.of("1|{1}", "2|{2}", "3|{3}"), rows(
                "SELECT rank() OVER w, ARRAY[rank() OVER w] FROM agc_t"
                        + " WINDOW w AS (ORDER BY v) ORDER BY 1"));
    }

    /** The value keeps the type the window expression has, not the type of its printed form. */
    @Test
    void aWindowValueKeepsItsOwnType() throws Exception {
        assertEquals(List.of("bigint", "bigint", "bigint"),
                rows("SELECT pg_typeof(sum(v) OVER ()) FROM agc_t"));
        assertEquals(List.of("1", "2", "3"),
                rows("SELECT coalesce(rank() OVER (ORDER BY v), 0) FROM agc_t ORDER BY 1"));
    }

    /** A call written inside a sub-select belongs to that sub-select and groups nothing here. */
    @Test
    void anAggregateInASubSelectDoesNotGroupTheQueryAroundIt() throws Exception {
        assertEquals(List.of("2", "2", "2"), rows(
                "SELECT (SELECT count(*) FROM agc_t WHERE v BETWEEN 1 AND 25) FROM agc_t"
                        + " ORDER BY 1"));
        assertEquals(List.of("2|30"),
                rows("SELECT g, v FROM agc_t WHERE v IN (SELECT max(v) FROM agc_t) ORDER BY g"));
    }

    /** An aggregate is still refused where one may not be written, and a bare column still is. */
    @Test
    void whatMayNotBeWrittenIsStillRefused() {
        assertEquals("42803", stateOf("SELECT g, v FROM agc_t WHERE v BETWEEN 1 AND count(*)"));
        assertEquals("42803", stateOf("SELECT ARRAY[v] FROM agc_t GROUP BY g"));
    }
}
