package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The type a window call answers in, and the names PostgreSQL uses when an aggregate has no
 * overload for what it was handed.
 *
 * <p>A window call's value was threaded back through the expression machinery as a string
 * literal, which is PostgreSQL's {@code unknown}: {@code pg_typeof(sum(x) OVER ())} answered
 * unknown where PostgreSQL answers numeric, the result column was described as text, and anything
 * computed from one resolved against text — {@code sum(sum(total)) OVER () + 1} came back
 * described as an integer, so a client reading it as one saw 256 where PostgreSQL has 256.75. The
 * value now carries the type its own expression has, and both the description and the arithmetic
 * follow from that. Allowing an aggregate under a window function is what this branch opened, so
 * this is the path it opened being given a type.
 *
 * <p>Alongside it, four things measured against PostgreSQL 18 that shared the same neighbourhood:
 *
 * <ul>
 *   <li>The type in "function sum(...) does not exist" was the catalog's spelling — varchar,
 *       bpchar, bool — where PostgreSQL writes the SQL one: character varying, character,
 *       boolean. {@code avg} over one of those did not even get that far and failed as an
 *       internal error.</li>
 *   <li>A RANGE frame offset of {@code INTERVAL '1 month'} over a date column counted thirty
 *       days rather than a calendar month, so the frame started a day late.</li>
 *   <li>GROUPING over a select-list alias reported a misplaced GROUPING (42803) where
 *       PostgreSQL reports the undefined column (42703) it resolves first.</li>
 *   <li>{@code HAVING x IN (SELECT ...)} read the subquery as a single value, so a second row
 *       raised 21000 instead of the membership test answering.</li>
 * </ul>
 */
class AggregateResultTypeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE art_ord (id int PRIMARY KEY, cust_id int, total numeric(10,2))");
        exec("INSERT INTO art_ord VALUES (10,1,100.00),(11,1,50.50),(12,2,75.25),(13,3,10.00),(14,3,20.00)");
        exec("CREATE TABLE art_t2 (id int PRIMARY KEY, s varchar(20), c char(5), b boolean)");
        exec("INSERT INTO art_t2 VALUES (1,'a','b',true),(2,'c','d',false)");
        exec("CREATE TABLE art_d (id int PRIMARY KEY, s text)");
        exec("INSERT INTO art_d VALUES (1,'x'),(2,'y')");
        exec("CREATE TABLE art_ev (id int PRIMARY KEY, d date, amt int)");
        exec("INSERT INTO art_ev VALUES (1,DATE '2024-01-01',10),(2,DATE '2024-01-03',20),"
                + "(3,DATE '2024-01-10',30),(4,DATE '2024-02-01',40)");
        exec("CREATE TABLE art_cust (id int PRIMARY KEY, name text, region text, active boolean)");
        exec("INSERT INTO art_cust VALUES (1,'Ann','EU',true),(2,'Bob','US',false),"
                + "(3,'Cid','EU',true),(4,'Dee','APAC',true)");
        exec("CREATE VIEW art_v AS SELECT cust_id, sum(total) s FROM art_ord GROUP BY cust_id");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- A window result carries the type its argument implies ----

    @Test
    void aWindowOverAnAggregateAnswersInTheAggregatesType() throws Exception {
        assertEquals("[1|numeric, 2|numeric, 3|numeric]",
                rows("SELECT cust_id, pg_typeof(sum(sum(total)) OVER ()) FROM art_ord GROUP BY cust_id"));
        assertEquals("[1|numeric, 2|numeric, 3|numeric]",
                rows("SELECT cust_id, pg_typeof(max(sum(total)) OVER ()) FROM art_ord GROUP BY cust_id"));
        assertEquals("[1|numeric, 2|numeric, 3|numeric]",
                rows("SELECT cust_id, pg_typeof(avg(sum(total)) OVER ()) FROM art_ord GROUP BY cust_id"));
        // sum over a count is sum(bigint), which is numeric and not another bigint
        assertEquals("[1|numeric, 2|numeric, 3|numeric]",
                rows("SELECT cust_id, pg_typeof(sum(count(*)) OVER ()) FROM art_ord GROUP BY cust_id"));
    }

    @Test
    void aPlainWindowCallCarriesItsOwnTypeToo() throws Exception {
        assertEquals("[numeric, numeric, numeric, numeric, numeric]",
                rows("SELECT pg_typeof(sum(total) OVER ()) FROM art_ord"));
        assertEquals("[bigint, bigint, bigint, bigint, bigint]",
                rows("SELECT pg_typeof(sum(id) OVER ()) FROM art_ord"));
        assertEquals("[numeric, numeric, numeric, numeric, numeric]",
                rows("SELECT pg_typeof(avg(id) OVER ()) FROM art_ord"));
        assertEquals("[bigint, bigint, bigint, bigint, bigint]",
                rows("SELECT pg_typeof(count(*) OVER ()) FROM art_ord"));
        assertEquals("[bigint, bigint, bigint, bigint, bigint]",
                rows("SELECT pg_typeof(row_number() OVER ()) FROM art_ord"));
        assertEquals("[bigint, bigint, bigint, bigint, bigint]",
                rows("SELECT pg_typeof(rank() OVER (ORDER BY id)) FROM art_ord"));
        // the value-shifting calls answer in the type of the value they shift, on every row --
        // including the first, where lag has no value at all
        assertEquals("[numeric, numeric, numeric, numeric, numeric]",
                rows("SELECT pg_typeof(lag(total) OVER (ORDER BY id)) FROM art_ord"));
        assertEquals("[numeric, numeric, numeric, numeric, numeric]",
                rows("SELECT pg_typeof(first_value(total) OVER (ORDER BY id)) FROM art_ord"));
    }

    @Test
    void theDescribedColumnTypeFollowsTheWindowsType() throws Exception {
        assertEquals("int4,numeric",
                types("SELECT cust_id, sum(sum(total)) OVER () + 1 FROM art_ord GROUP BY cust_id"));
        assertEquals("int4,numeric,numeric",
                types("SELECT cust_id, sum(total), max(sum(total)) OVER w"
                        + " FROM art_ord GROUP BY cust_id WINDOW w AS ()"));
        assertEquals("int8", types("SELECT count(*) OVER () FROM art_ord"));
        assertEquals("int8", types("SELECT sum(id) OVER () FROM art_ord"));
        assertEquals("numeric", types("SELECT avg(id) OVER () FROM art_ord"));
        assertEquals("int8,int8,int8",
                types("SELECT row_number() OVER (), rank() OVER (ORDER BY id),"
                        + " dense_rank() OVER (ORDER BY id) FROM art_ord"));
        assertEquals("int4,float8,float8",
                types("SELECT ntile(2) OVER (ORDER BY id), percent_rank() OVER (ORDER BY id),"
                        + " cume_dist() OVER (ORDER BY id) FROM art_ord"));
        assertEquals("numeric,int4",
                types("SELECT lag(total) OVER (ORDER BY id), lead(id) OVER (ORDER BY id) FROM art_ord"));
        assertEquals("bool", types("SELECT bool_and(active) OVER () FROM art_cust"));
        // count is a bigint wherever it is written, not only under a window
        assertEquals("int8", types("SELECT count(*) FROM art_ord"));
        assertEquals("int8", types("SELECT count(*) + 1 FROM art_ord"));
    }

    @Test
    void aValueComputedFromAWindowResultKeepsItsFraction() throws Exception {
        assertEquals("[1|256.75, 2|256.75, 3|256.75]",
                rows("SELECT cust_id, sum(sum(total)) OVER () + 1 FROM art_ord GROUP BY cust_id"));
        assertEquals("[1|255.50, 2|255.50, 3|255.50]",
                rows("SELECT cust_id, sum(sum(total)) OVER () - 0.25 FROM art_ord GROUP BY cust_id"));
        assertEquals("[1|150.50|150.50, 2|75.25|150.50, 3|30.00|150.50]",
                rows("SELECT cust_id, sum(total), max(sum(total)) OVER w"
                        + " FROM art_ord GROUP BY cust_id WINDOW w AS ()"));
        assertEquals("[1|85.25, 2|85.25, 3|85.25]",
                rows("SELECT cust_id, round(avg(sum(total)) OVER (), 2) FROM art_ord GROUP BY cust_id"));
    }

    @Test
    void aWindowValueIsStillReadableWhereItIsNull() throws Exception {
        // lag has no value on the first row; the test has to see that, and the rows that do have
        // one have to come out as having one
        assertEquals("[10|t, 11|f, 12|f, 13|f, 14|f]",
                rows("SELECT id, lag(total) OVER (ORDER BY id) IS NULL FROM art_ord"));
        assertEquals("[10|0, 11|100.00, 12|50.50, 13|75.25, 14|10.00]",
                rows("SELECT id, coalesce(lag(total) OVER (ORDER BY id), 0) FROM art_ord"));
        assertEquals("[10|null, 11|100.00, 12|50.50, 13|75.25, 14|10.00]",
                rows("SELECT id, abs(lag(total) OVER (ORDER BY id)) FROM art_ord"));
    }

    // ---- The type name in an aggregate-not-found message ----

    @Test
    void theMissingAggregateNamesItsArgumentTheWaySqlSpellsIt() throws Exception {
        assertError("42883", "function sum(character varying) does not exist",
                "SELECT id, count(*) FROM art_t2 GROUP BY id HAVING sum(s) > 1");
        assertError("42883", "function sum(character) does not exist",
                "SELECT id, count(*) FROM art_t2 GROUP BY id HAVING sum(c) > 1");
        assertError("42883", "function avg(boolean) does not exist",
                "SELECT id, count(*) FROM art_t2 GROUP BY id HAVING avg(b) > 1");
        // and the same names outside HAVING, where the message used to read the value's type
        assertError("42883", "function sum(character varying) does not exist", "SELECT sum(s) FROM art_t2");
        assertError("42883", "function sum(character) does not exist", "SELECT sum(c) FROM art_t2");
        assertError("42883", "function sum(boolean) does not exist", "SELECT sum(b) FROM art_t2");
    }

    @Test
    void anAggregateWithNoOverloadIsAMissingFunctionAndNotAnInternalError() throws Exception {
        // each of these came back as XX000 with a number-parsing complaint
        assertError("42883", "function avg(character varying) does not exist", "SELECT avg(s) FROM art_t2");
        assertError("42883", "function avg(character) does not exist", "SELECT avg(c) FROM art_t2");
        assertError("42883", "function avg(boolean) does not exist", "SELECT avg(b) FROM art_t2");
        assertError("42883", "function bit_and(character varying) does not exist",
                "SELECT bit_and(s) FROM art_t2");
        assertError("42883", "function sum(character varying) does not exist",
                "SELECT sum(s) OVER () FROM art_t2");
    }

    // ---- A RANGE frame offset is an interval, not a count of days ----

    @Test
    void aMonthOffsetOverADateColumnIsACalendarMonth() throws Exception {
        assertEquals("[1|2024-01-01|4, 2|2024-01-03|4, 3|2024-01-10|4, 4|2024-02-01|4]",
                rows("SELECT id, d, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1 month'"
                        + " PRECEDING AND INTERVAL '1 month' FOLLOWING) FROM art_ev"));
        assertEquals("[1|2024-01-01|1, 2|2024-01-03|1, 3|2024-01-10|1, 4|2024-02-01|1]",
                rows("SELECT id, d, count(*) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1 day'"
                        + " PRECEDING AND CURRENT ROW) FROM art_ev"));
        assertEquals("[1|2024-01-01|10, 2|2024-01-03|30, 3|2024-01-10|30, 4|2024-02-01|40]",
                rows("SELECT id, d, sum(amt) OVER (ORDER BY d RANGE BETWEEN INTERVAL '2 days'"
                        + " PRECEDING AND CURRENT ROW) FROM art_ev"));
        // a numeric offset over a numeric ordering column is untouched by that
        assertEquals("[1|10, 2|20, 3|30, 4|40]",
                rows("SELECT id, sum(amt) OVER (ORDER BY id RANGE BETWEEN 0 PRECEDING"
                        + " AND 0 FOLLOWING) FROM art_ev"));
    }

    // ---- GROUPING over a name that is not a column ----

    @Test
    void groupingOverAnUndefinedNameIsAnUndefinedColumn() throws Exception {
        assertError("42703", "column \"k\" does not exist",
                "SELECT s AS k, grouping(k), count(*) FROM art_d GROUP BY ROLLUP(k)");
        assertError("42703", "column \"nosuch\" does not exist",
                "SELECT grouping(nosuch) FROM art_d GROUP BY ROLLUP(s)");
        // a name that is a column but not one the query groups by keeps the GROUPING message
        assertError("42803", "arguments to GROUPING must be grouping expressions",
                "SELECT s, grouping(id) FROM art_d GROUP BY ROLLUP(s)");
        // and a GROUPING over a name the query does group by is simply valid
        assertEquals("[null|1, x|0, y|0]",
                rows("SELECT s, grouping(s) FROM art_d GROUP BY ROLLUP(s)"));
        assertEquals("[null|2, x|1, y|1]",
                rows("SELECT s AS k, count(*) FROM art_d GROUP BY ROLLUP(k)"));
    }

    // ---- A subquery in HAVING is a set, not a scalar ----

    @Test
    void aHavingSubqueryIsReadAsTheSetItIs() throws Exception {
        assertEquals("[1|150.50, 2|75.25]",
                rows("SELECT cust_id, sum(total) FROM art_ord GROUP BY cust_id"
                        + " HAVING cust_id IN (SELECT id FROM art_d)"));
        assertEquals("[1|150.50, 2|75.25, 3|30.00]",
                rows("SELECT cust_id, sum(total) FROM art_ord GROUP BY cust_id"
                        + " HAVING count(*) IN (SELECT id FROM art_d)"));
        assertEquals("[3]",
                rows("SELECT cust_id FROM art_ord GROUP BY cust_id"
                        + " HAVING cust_id NOT IN (SELECT id FROM art_d)"));
        assertEquals("[1, 2, 3]",
                rows("SELECT cust_id FROM art_ord GROUP BY cust_id"
                        + " HAVING sum(total) > ALL (SELECT id FROM art_d)"));
        assertEquals("[1, 2]",
                rows("SELECT cust_id FROM art_ord GROUP BY cust_id"
                        + " HAVING cust_id = ANY (SELECT id FROM art_d)"));
        assertEquals("[APAC|1, EU|2]",
                rows("SELECT region, count(*) FROM art_cust GROUP BY region"
                        + " HAVING region IN (SELECT region FROM art_cust WHERE active)"));
        // an IN over a literal list, and a scalar subquery, both still work
        assertEquals("[1|150.50, 3|30.00]",
                rows("SELECT cust_id, sum(total) FROM art_ord GROUP BY cust_id"
                        + " HAVING cust_id IN (1, 3)"));
        assertEquals("[1, 2, 3]",
                rows("SELECT cust_id FROM art_ord GROUP BY cust_id"
                        + " HAVING (SELECT max(id) FROM art_d) > 1"));
        assertEquals("[1, 2]",
                rows("SELECT cust_id FROM art_ord GROUP BY cust_id"
                        + " HAVING EXISTS (SELECT 1 FROM art_d WHERE id = cust_id)"));
    }

    // ---- The ordinary shapes around every rule touched here ----

    @Test
    void ordinaryGroupedQueriesAreUntouched() throws Exception {
        assertEquals("[APAC|1, EU|2, US|1]",
                rows("SELECT region, count(*) FROM art_cust GROUP BY region"));
        assertEquals("[EU|2, APAC|1, US|1]",
                orderedRows("SELECT region, count(*) c FROM art_cust GROUP BY region ORDER BY c DESC, region"));
        assertEquals("[EU|2, APAC|1, US|1]",
                orderedRows("SELECT region, count(*) FROM art_cust GROUP BY region ORDER BY 2 DESC, 1"));
        assertEquals("[EU|2, APAC|1, US|1]",
                orderedRows("SELECT region, count(*) FROM art_cust GROUP BY region"
                        + " ORDER BY count(*) DESC, region"));
        assertEquals("[EU|2]",
                rows("SELECT region, count(*) FROM art_cust GROUP BY region HAVING count(*) > 1"));
        assertEquals("[APAC, EU, US]", rows("SELECT DISTINCT region FROM art_cust"));
        assertEquals("[APAC|Dee, EU|Ann, US|Bob]",
                rows("SELECT DISTINCT ON (region) region, name FROM art_cust ORDER BY region, name"));
        assertEquals("[APAC|1, EU|2, US|1, null|4]",
                rows("SELECT region, count(*) FROM art_cust GROUP BY ROLLUP(region)"));
    }

    @Test
    void ordinaryWindowAndDerivedQueriesAreUntouched() throws Exception {
        assertEquals("[1|150.50|1, 2|75.25|2, 3|30.00|3]",
                rows("SELECT cust_id, sum(total), row_number() OVER (ORDER BY sum(total) DESC)"
                        + " FROM art_ord GROUP BY cust_id"));
        assertEquals("[10|100.00|100.00, 11|50.50|150.50, 12|75.25|225.75,"
                        + " 13|10.00|235.75, 14|20.00|255.75]",
                rows("SELECT id, total, sum(total) OVER (ORDER BY id) FROM art_ord"));
        assertEquals("[APAC|null, EU|180.50, US|75.25]",
                rows("SELECT c.region, sum(o.total) FROM art_cust c"
                        + " LEFT JOIN art_ord o ON o.cust_id = c.id GROUP BY c.region"));
        assertEquals("[1|150.50, 2|75.25, 3|30.00, 4|null]",
                rows("SELECT c.id, x.s FROM art_cust c JOIN LATERAL"
                        + " (SELECT sum(total) s FROM art_ord o WHERE o.cust_id=c.id) x ON true"));
        assertEquals("[1|150.50, 2|75.25, 3|30.00]", rows("SELECT * FROM art_v"));
        assertEquals("[1|150.50, 2|75.25, 3|30.00]",
                rows("WITH g AS (SELECT cust_id, sum(total) s FROM art_ord GROUP BY cust_id)"
                        + " SELECT * FROM g"));
        assertEquals("[1|150.50, 2|75.25]",
                rows("SELECT * FROM (SELECT cust_id, sum(total) s FROM art_ord GROUP BY cust_id) t"
                        + " WHERE t.s > 40"));
        assertEquals("[1, 2, 3]",
                rows("SELECT cust_id FROM art_ord GROUP BY cust_id UNION SELECT id FROM art_d"));
        assertEquals("[1, 2, 3, 4, 5]",
                rows("SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY id) rn FROM art_ord) sub"
                        + " WHERE sub.rn >= 1"));
    }

    // ---- helpers ----

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** One row per element, columns joined by '|', sorted so an unordered result compares stably. */
    private static String rows(String sql) throws SQLException {
        List<String> out = collect(sql);
        Collections.sort(out);
        return out.toString();
    }

    /** The rows in the order the query asked for them. */
    private static String orderedRows(String sql) throws SQLException {
        return collect(sql).toString();
    }

    private static List<String> collect(String sql) throws SQLException {
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
            return out;
        }
    }

    /** The types the result columns are described as, in order. */
    private static String types(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) sb.append(',');
                sb.append(md.getColumnTypeName(i));
            }
            return sb.toString();
        }
    }

    private static void assertError(String sqlState, String messagePart, String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            fail("expected " + sqlState + " for: " + sql);
        } catch (SQLException e) {
            assertEquals(sqlState, e.getSQLState(), "sqlstate for: " + sql);
            assertTrue(e.getMessage().contains(messagePart),
                    "expected \"" + messagePart + "\" in \"" + e.getMessage() + "\" for: " + sql);
        }
    }
}
