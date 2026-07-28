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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The places the grouping check did not reach, and the errors it reported in the wrong order.
 *
 * <p>Four kinds of gap, each measured against PostgreSQL 18 before and after:
 *
 * <ul>
 *   <li><b>Places.</b> The check stopped at a nested query, so an ungrouped column of ours read
 *       inside a scalar subquery, an EXISTS or a HAVING sub-select was never judged; the DISTINCT
 *       ON list was never walked at all; and CREATE VIEW stored an invalid grouped query that
 *       then returned an arbitrary row on every read.</li>
 *   <li><b>Order.</b> PostgreSQL transforms the sort clause first, then the grouping, then
 *       HAVING, and only checks for ungrouped columns at the end. So a GROUP BY item that names
 *       no column is 42703 and one that names another table is 42P01, an ORDER BY position past
 *       the select list is 42P10, and a HAVING whose operator does not resolve is 42883 — each of
 *       them before "must appear in the GROUP BY clause".</li>
 *   <li><b>Constants.</b> ORDER BY reads a constant as an output-column position exactly as
 *       GROUP BY does: out of range is 42P10, not an integer at all is 42601.</li>
 *   <li><b>Frame offsets.</b> A frame offset's type is resolved before the offset is asked to be
 *       constant: ROWS and GROUPS count in bigint, RANGE counts in the ordering column's own type.</li>
 * </ul>
 *
 * <p>Two things measurement contradicted. A LATERAL item's output was already judged on this
 * branch, so that case needed no change. And the notification-inbox query in the pg18-sql-4
 * corpus grouped by a UNIQUE NOT NULL column while reading the primary key inside an EXISTS:
 * PostgreSQL 18 rejects that (a unique constraint carries no functional dependency), so the
 * corpus query was corrected to group by the key.
 *
 * <p>The second half of this class is the regression half. Adding strictness here touches every
 * grouped query there is, so the shapes that must keep working are asserted next to the ones that
 * must now fail — including the derived column {@code sub.rn >= 1}, which is why the HAVING type
 * check applies to a plain base table only: a derived column's type is inferred from a first
 * result and judging it rejects working SQL.
 */
class GroupByCoverageTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE gbc_nokey (a int, b text)");
        exec("INSERT INTO gbc_nokey VALUES (1,'x'),(1,'y'),(2,'z')");
        exec("CREATE TABLE gbc_other (k int, s text)");
        exec("INSERT INTO gbc_other VALUES (1,'x'),(2,'q')");
        exec("CREATE TABLE gbc_pk (id int PRIMARY KEY, other text, n int)");
        exec("INSERT INTO gbc_pk VALUES (1,'a',5),(2,'b',6)");
        exec("CREATE TABLE gbc_uq (id int PRIMARY KEY, uq text NOT NULL UNIQUE, n int)");
        exec("INSERT INTO gbc_uq VALUES (1,'p',1),(2,'q',2)");
        exec("CREATE TABLE gbc_t (id int, a int)");
        exec("INSERT INTO gbc_t VALUES (1,10),(2,20),(3,30)");
        exec("CREATE TABLE gbc_num (id int, a int, nn numeric, bi bigint, t text)");
        exec("INSERT INTO gbc_num VALUES (1,10,1.5,1,'x'),(2,20,2.5,2,'y')");
        exec("CREATE VIEW gbc_view AS SELECT a, b FROM gbc_nokey");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
            return out;
        }
    }

    private static void assertRows(String sql, String... expected) throws SQLException {
        assertEquals(List.of(expected), rows(sql), sql);
    }

    private static void assertError(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> expected message containing <" + messagePart + "> but was <" + e.getMessage() + ">");
    }

    private static void assertOuterUngrouped(String column, String sql) {
        assertError("42803", "subquery uses ungrouped column \"" + column + "\" from outer query", sql);
    }

    private static void assertUngrouped(String column, String sql) {
        assertError("42803",
                "column \"" + column + "\" must appear in the GROUP BY clause or be used in an aggregate function",
                sql);
    }

    // ---- An ungrouped column read inside a nested query ----

    @Test
    void aSubqueryThatReadsAnUngroupedColumnOfOursIsRejected() {
        assertOuterUngrouped("gbc_nokey.b",
                "SELECT a, (SELECT s FROM gbc_other WHERE gbc_other.s = gbc_nokey.b) FROM gbc_nokey GROUP BY a");
        assertOuterUngrouped("gbc_nokey.b",
                "SELECT a, EXISTS (SELECT 1 FROM gbc_other WHERE s = gbc_nokey.b) FROM gbc_nokey GROUP BY a");
        assertOuterUngrouped("gbc_nokey.b",
                "SELECT a, CASE WHEN true THEN (SELECT s FROM gbc_other WHERE s = gbc_nokey.b) END"
                        + " FROM gbc_nokey GROUP BY a");
        assertOuterUngrouped("gbc_nokey.b",
                "SELECT a FROM gbc_nokey GROUP BY a"
                        + " HAVING (SELECT count(*) FROM gbc_other WHERE s = gbc_nokey.b) > 0");
        assertOuterUngrouped("gbc_nokey.b",
                "SELECT a FROM gbc_nokey GROUP BY a"
                        + " HAVING a IN (SELECT k FROM gbc_other WHERE s = gbc_nokey.b)");
        assertOuterUngrouped("gbc_nokey.b",
                "SELECT a, (SELECT count(*) FROM gbc_other GROUP BY gbc_nokey.b) FROM gbc_nokey GROUP BY a");
    }

    /** The column is named as the query names it: through the alias, when there is one. */
    @Test
    void theOuterColumnIsNamedThroughItsAlias() {
        assertOuterUngrouped("x.b",
                "SELECT x.a FROM gbc_nokey x GROUP BY x.a"
                        + " HAVING EXISTS (SELECT 1 FROM gbc_nokey y WHERE y.b = x.b)");
        assertOuterUngrouped("x.b",
                "SELECT x.a, (SELECT count(*) FROM gbc_other o WHERE o.s = x.b) FROM gbc_nokey x GROUP BY x.a");
    }

    /** A UNIQUE NOT NULL column determines nothing; only the primary key does. */
    @Test
    void onlyThePrimaryKeyLicensesTheRestOfTheRowInASubquery() throws Exception {
        assertRows("SELECT u.other, EXISTS (SELECT 1 FROM gbc_other o WHERE o.k = u.id)"
                + " FROM gbc_pk u GROUP BY u.id ORDER BY 1", "a|t", "b|t");
        assertOuterUngrouped("u.id", "SELECT u.uq, EXISTS (SELECT 1 FROM gbc_other o WHERE o.k = u.id)"
                + " FROM gbc_uq u GROUP BY u.uq");
        assertOuterUngrouped("u.n", "SELECT u.uq, EXISTS (SELECT 1 FROM gbc_other o WHERE o.k = u.n)"
                + " FROM gbc_uq u GROUP BY u.uq");
    }

    /** A nested query that reads its own columns, or ours through the grouping, is untouched. */
    @Test
    void nestedQueriesThatReadNothingUngroupedKeepWorking() throws Exception {
        assertRows("SELECT a, (SELECT count(*) FROM gbc_other) FROM gbc_nokey GROUP BY a ORDER BY 1",
                "1|2", "2|2");
        assertRows("SELECT a, (SELECT count(*) FROM gbc_other WHERE gbc_other.k = gbc_nokey.a)"
                + " FROM gbc_nokey GROUP BY a ORDER BY 1", "1|1", "2|1");
        assertRows("SELECT a FROM gbc_nokey x GROUP BY a"
                + " HAVING a IN (SELECT k FROM gbc_other WHERE k = x.a) ORDER BY 1", "1", "2");
        assertRows("SELECT a, (SELECT count(*) FROM gbc_nokey y WHERE y.b = 'x')"
                + " FROM gbc_nokey GROUP BY a ORDER BY 1", "1|1", "2|1");
        assertRows("SELECT id, (SELECT count(*) FROM gbc_other WHERE s = gbc_pk.other)"
                + " FROM gbc_pk GROUP BY id ORDER BY 1", "1|0", "2|0");
        assertRows("SELECT a, (SELECT count(*) FROM (SELECT k FROM gbc_other) s WHERE s.k = gbc_nokey.a)"
                + " FROM gbc_nokey GROUP BY a ORDER BY 1", "1|1", "2|1");
        assertRows("SELECT a, (SELECT count(*) FROM gbc_view v WHERE v.a = gbc_nokey.a)"
                + " FROM gbc_nokey GROUP BY a ORDER BY 1", "1|2", "2|1");
        // Not grouped at all: the same reference is an ordinary correlated one.
        assertRows("SELECT (SELECT count(*) FROM gbc_other GROUP BY gbc_nokey.b) FROM gbc_nokey ORDER BY 1",
                "2", "2", "2");
    }

    // ---- DISTINCT ON ----

    @Test
    void theDistinctOnListIsJudgedLikeTheSelectList() {
        assertUngrouped("gbc_nokey.b", "SELECT DISTINCT ON (b) a FROM gbc_nokey GROUP BY a");
    }

    @Test
    void distinctOnOverAnAggregateKeepsOneRowPerKey() throws Exception {
        assertRows("SELECT DISTINCT ON (sum(a)) a FROM gbc_nokey GROUP BY a ORDER BY sum(a), a", "1");
        assertRows("SELECT DISTINCT ON (sum(a)) a, sum(a) FROM gbc_nokey GROUP BY a ORDER BY sum(a), a",
                "1|2");
        assertRows("SELECT DISTINCT ON (count(*)) a FROM gbc_nokey GROUP BY a ORDER BY count(*), a",
                "2", "1");
    }

    /** A window function in the key has one value per row, so it eliminates nothing here. */
    @Test
    void distinctOnOverAWindowFunctionIsEvaluatedPerRow() throws Exception {
        assertRows("SELECT DISTINCT ON (row_number() OVER (ORDER BY a)) a FROM gbc_nokey"
                + " ORDER BY row_number() OVER (ORDER BY a)", "1", "1", "2");
    }

    @Test
    void distinctOnKeepsWorkingWhereItAlreadyDid() throws Exception {
        assertRows("SELECT DISTINCT ON (a) a, b FROM gbc_nokey ORDER BY a, b", "1|x", "2|z");
        assertRows("SELECT DISTINCT ON (a) a, b FROM gbc_nokey ORDER BY a, b DESC", "1|y", "2|z");
        assertRows("SELECT DISTINCT ON (a) a, count(*) FROM gbc_nokey GROUP BY a ORDER BY a",
                "1|2", "2|1");
        assertRows("SELECT DISTINCT ON (b) a FROM gbc_nokey GROUP BY a, b ORDER BY b", "1", "1", "2");
        assertRows("SELECT DISTINCT ON (a) a, row_number() OVER (ORDER BY a) FROM gbc_nokey ORDER BY a",
                "1|1", "2|3");
    }

    // ---- SELECT DISTINCT and its sort keys ----

    @Test
    void distinctSortKeysMustBeSelected() {
        assertError("42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                "SELECT DISTINCT a FROM gbc_nokey GROUP BY a ORDER BY count(*)");
        assertError("42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                "SELECT DISTINCT a FROM gbc_nokey GROUP BY a ORDER BY sum(a)");
        assertError("42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
                "SELECT DISTINCT a FROM gbc_nokey ORDER BY row_number() OVER ()");
    }

    @Test
    void distinctOverAnAggregateItSelectsKeepsWorking() throws Exception {
        assertRows("SELECT DISTINCT a, sum(a) FROM gbc_nokey GROUP BY a ORDER BY sum(a)", "1|2", "2|2");
        assertRows("SELECT DISTINCT count(*) FROM gbc_nokey GROUP BY a ORDER BY count(*)", "1", "2");
        assertRows("SELECT DISTINCT a FROM gbc_nokey GROUP BY a ORDER BY a", "1", "2");
        assertRows("SELECT DISTINCT a, count(*) AS c FROM gbc_nokey GROUP BY a ORDER BY c, a",
                "2|1", "1|2");
    }

    // ---- Which error comes first ----

    @Test
    void aGroupByItemThatNamesNothingIsThatErrorAndNoOther() {
        assertError("42703", "column \"nosuchcol\" does not exist",
                "SELECT b FROM gbc_nokey GROUP BY nosuchcol");
        assertError("42703", "column \"nosuchcol\" does not exist",
                "SELECT b FROM gbc_nokey GROUP BY a, nosuchcol");
        assertError("42703", "column gbc_nokey.nosuchcol does not exist",
                "SELECT b FROM gbc_nokey GROUP BY gbc_nokey.nosuchcol");
        assertError("42P01", "missing FROM-clause entry for table \"gbc_other\"",
                "SELECT b FROM gbc_nokey GROUP BY gbc_other.k");
        assertError("42P01", "invalid reference to FROM-clause entry for table \"gbc_nokey\"",
                "SELECT count(*) FROM gbc_nokey n GROUP BY gbc_nokey.a");
    }

    @Test
    void aSortPositionOutsideTheSelectListComesBeforeTheGrouping() {
        assertError("42P10", "ORDER BY position 5 is not in select list",
                "SELECT a, b FROM gbc_nokey GROUP BY a ORDER BY 5");
        assertError("42P10", "ORDER BY position -1 is not in select list",
                "SELECT a, b FROM gbc_nokey GROUP BY a ORDER BY -1");
        assertError("42601", "non-integer constant in ORDER BY",
                "SELECT a, b FROM gbc_nokey GROUP BY a ORDER BY 2.5");
        // ORDER BY is transformed before GROUP BY, so it even beats an unresolvable group item.
        assertError("42P10", "ORDER BY position 9 is not in select list",
                "SELECT a, b FROM gbc_nokey GROUP BY nosuchcol ORDER BY 9");
    }

    @Test
    void aHavingThatDoesNotTypeCheckComesBeforeTheGrouping() {
        assertError("42883", "function sum(text) does not exist",
                "SELECT a, b FROM gbc_nokey GROUP BY a HAVING sum(b) > 1");
        assertError("42883", "operator does not exist: text > integer",
                "SELECT a, b FROM gbc_nokey GROUP BY a HAVING b > 1");
        assertError("22P02", "invalid input syntax for type integer: \"zz\"",
                "SELECT a, b FROM gbc_nokey GROUP BY a HAVING a > 'zz'");
        assertError("22P02", "invalid input syntax for type bigint: \"x\"",
                "SELECT a, b FROM gbc_nokey GROUP BY a HAVING count(*) > 'x'");
    }

    /** The same type errors without a grouping complaint to compete with. */
    @Test
    void aHavingThatDoesNotTypeCheckIsRejectedOnItsOwn() {
        assertError("42883", "function sum(text) does not exist",
                "SELECT a FROM gbc_nokey GROUP BY a HAVING sum(b) > 1");
        assertError("22P02", "invalid input syntax for type integer: \"zz\"",
                "SELECT a FROM gbc_nokey GROUP BY a, b HAVING a > 'zz'");
        assertError("22P02", "invalid input syntax for type bigint: \"x\"",
                "SELECT a FROM gbc_nokey GROUP BY a HAVING count(*) > 'x'");
    }

    /**
     * A derived column carries a type inferred from a first result, not a declared one, so the
     * HAVING type check has to leave it alone: this rejected {@code sub.rn >= 1} once.
     */
    @Test
    void aDerivedColumnIsNotTypeCheckedAgainstALiteral() throws Exception {
        assertRows("SELECT s.rn FROM (SELECT row_number() OVER () AS rn FROM gbc_nokey) s"
                + " GROUP BY s.rn HAVING s.rn >= 1 ORDER BY 1", "1", "2", "3");
        assertRows("SELECT v.a, count(*) FROM gbc_view v GROUP BY v.a HAVING count(*) >= 1 ORDER BY 1",
                "1|2", "2|1");
    }

    @Test
    void havingKeepsWorkingWhereItAlreadyDid() throws Exception {
        assertRows("SELECT a, count(*) FROM gbc_nokey GROUP BY a HAVING count(*) > 1 ORDER BY 1", "1|2");
        assertRows("SELECT count(*) FROM gbc_num GROUP BY id HAVING sum(nn) > 0 ORDER BY 1", "1", "1");
        assertRows("SELECT count(*) FROM gbc_num GROUP BY id HAVING count(*) > '1' ORDER BY 1");
        assertRows("SELECT id, count(*) FROM gbc_num GROUP BY id HAVING max(t) > 'a' ORDER BY 1",
                "1|1", "2|1");
        assertRows("SELECT a FROM gbc_nokey GROUP BY a"
                + " HAVING EXISTS (SELECT 1 FROM gbc_other WHERE k = gbc_nokey.a) ORDER BY 1", "1", "2");
    }

    // ---- Constants in ORDER BY ----

    @Test
    void aSortPositionIsReadAsOneWhetherOrNotTheQueryGroups() {
        assertError("42P10", "ORDER BY position -1 is not in select list",
                "SELECT a FROM gbc_nokey ORDER BY -1");
        assertError("42P10", "ORDER BY position 0 is not in select list",
                "SELECT a FROM gbc_nokey ORDER BY 0");
        assertError("42P10", "ORDER BY position 2 is not in select list",
                "SELECT a FROM gbc_nokey GROUP BY a ORDER BY 2");
        assertError("42P10", "ORDER BY position -1 is not in select list",
                "SELECT a FROM (SELECT a FROM gbc_nokey ORDER BY -1) s");
    }

    @Test
    void aConstantThatIsNoPositionSortsNothing() {
        assertError("42601", "non-integer constant in ORDER BY", "SELECT a FROM gbc_nokey ORDER BY 2.0");
        assertError("42601", "non-integer constant in ORDER BY", "SELECT a FROM gbc_nokey ORDER BY 1.0");
        assertError("42601", "non-integer constant in ORDER BY", "SELECT a FROM gbc_nokey ORDER BY 'x'");
        assertError("42601", "non-integer constant in ORDER BY", "SELECT a FROM gbc_nokey ORDER BY '1'");
        assertError("42601", "non-integer constant in ORDER BY", "SELECT a FROM gbc_nokey ORDER BY true");
        assertError("42601", "non-integer constant in ORDER BY", "SELECT a FROM gbc_nokey ORDER BY NULL");
        assertError("42601", "non-integer constant in ORDER BY",
                "SELECT * FROM (SELECT a FROM gbc_nokey ORDER BY 1.0) s");
    }

    @Test
    void everyOtherSortKeyKeepsWorking() throws Exception {
        assertRows("SELECT a, count(*) FROM gbc_nokey GROUP BY a ORDER BY 2, 1", "2|1", "1|2");
        assertRows("SELECT a AS z FROM gbc_nokey GROUP BY a ORDER BY z", "1", "2");
        assertRows("SELECT a FROM gbc_nokey ORDER BY 1 + 0", "1", "1", "2");
        assertRows("SELECT a FROM gbc_nokey ORDER BY a + 0", "1", "1", "2");
        assertRows("SELECT a FROM gbc_nokey ORDER BY 1 DESC", "2", "1", "1");
        assertRows("SELECT a FROM gbc_nokey UNION SELECT k FROM gbc_other ORDER BY 1", "1", "2");
        assertRows("SELECT array_agg(a ORDER BY 1) FROM gbc_nokey", "{1,1,2}");
        assertRows("SELECT a, row_number() OVER (ORDER BY 1) FROM gbc_nokey ORDER BY 1, 2",
                "1|1", "1|2", "2|3");
        assertRows("WITH c AS (SELECT a FROM gbc_nokey ORDER BY 1) SELECT * FROM c ORDER BY 1",
                "1", "1", "2");
    }

    // ---- CREATE VIEW ----

    @Test
    void aViewOverAnInvalidGroupedQueryIsRefusedWhenItIsWritten() {
        assertUngrouped("gbc_nokey.b",
                "CREATE VIEW gbc_bad AS SELECT a, b FROM gbc_nokey GROUP BY a");
        assertError("42703", "column \"nosuchcol\" does not exist",
                "CREATE VIEW gbc_bad AS SELECT a FROM gbc_nokey GROUP BY nosuchcol");
        assertError("42P10", "ORDER BY position 7 is not in select list",
                "CREATE VIEW gbc_bad AS SELECT a, b FROM gbc_nokey GROUP BY a ORDER BY 7");
        assertUngrouped("gbc_nokey.b",
                "CREATE MATERIALIZED VIEW gbc_badmv AS SELECT a, b FROM gbc_nokey GROUP BY a");
    }

    @Test
    void aViewOverAValidGroupedQueryIsStillCreated() throws Exception {
        exec("CREATE VIEW gbc_good AS SELECT a, count(*) AS c FROM gbc_nokey GROUP BY a");
        assertRows("SELECT a, c FROM gbc_good ORDER BY 1", "1|2", "2|1");
        exec("CREATE VIEW gbc_good_key AS SELECT id, other FROM gbc_pk GROUP BY id");
        assertRows("SELECT id, other FROM gbc_good_key ORDER BY 1", "1|a", "2|b");
        exec("DROP VIEW gbc_good");
        exec("DROP VIEW gbc_good_key");
    }

    // ---- Window frame offsets ----

    @Test
    void aQuotedFrameOffsetIsReadAsTheTypeTheFrameCountsIn() {
        assertError("22P02", "invalid input syntax for type integer: \"x\"",
                "SELECT sum(a) OVER (ORDER BY id RANGE BETWEEN 'x' PRECEDING AND CURRENT ROW) FROM gbc_t");
        assertError("22P02", "invalid input syntax for type numeric: \"x\"",
                "SELECT sum(a) OVER (ORDER BY nn RANGE BETWEEN 'x' PRECEDING AND CURRENT ROW) FROM gbc_num");
        assertError("22P02", "invalid input syntax for type bigint: \"x\"",
                "SELECT sum(a) OVER (ORDER BY id ROWS BETWEEN 'x' PRECEDING AND CURRENT ROW) FROM gbc_t");
        // The offset's type is settled before any row is read, so an empty result still reports it.
        assertError("22P02", "invalid input syntax for type integer: \"x\"",
                "SELECT sum(a) OVER (ORDER BY id RANGE BETWEEN 'x' PRECEDING AND CURRENT ROW)"
                        + " FROM gbc_t WHERE false");
        assertError("22P02", "invalid input syntax for type bigint: \"x\"",
                "SELECT sum(a) OVER (ORDER BY id ROWS BETWEEN 'x' PRECEDING AND CURRENT ROW)"
                        + " FROM gbc_t WHERE false");
        assertError("22P02", "invalid input syntax for type integer: \"x\"",
                "SELECT sum(a) OVER w FROM gbc_t"
                        + " WINDOW w AS (ORDER BY id RANGE BETWEEN 'x' PRECEDING AND CURRENT ROW)");
    }

    @Test
    void aQuotedOffsetThatReadsAsANumberStillFrames() throws Exception {
        assertRows("SELECT sum(a) OVER (ORDER BY id RANGE BETWEEN '1' PRECEDING AND CURRENT ROW)"
                + " FROM gbc_t ORDER BY 1", "10", "30", "50");
        assertRows("SELECT sum(a) OVER (ORDER BY id ROWS BETWEEN '1' PRECEDING AND CURRENT ROW)"
                + " FROM gbc_t ORDER BY 1", "10", "30", "50");
    }

    @Test
    void anOffsetOfTheWrongTypeIsThatErrorRatherThanAVariableOne() {
        assertError("42804", "argument of ROWS must be type bigint, not type text",
                "SELECT a, count(*) OVER (ORDER BY a ROWS BETWEEN b PRECEDING AND CURRENT ROW)"
                        + " FROM gbc_nokey GROUP BY a");
        assertError("42804", "argument of GROUPS must be type bigint, not type text",
                "SELECT sum(a) OVER (ORDER BY id GROUPS BETWEEN t PRECEDING AND CURRENT ROW) FROM gbc_num");
        assertError("42804", "argument of ROWS must be type bigint, not type text",
                "SELECT sum(a) OVER w FROM gbc_num"
                        + " WINDOW w AS (ORDER BY id ROWS BETWEEN t PRECEDING AND CURRENT ROW)");
        assertError("0A000", "RANGE with offset PRECEDING/FOLLOWING is not supported for column type"
                        + " integer and offset type text",
                "SELECT sum(a) OVER (ORDER BY id RANGE BETWEEN t PRECEDING AND CURRENT ROW) FROM gbc_num");
    }

    /** An offset of a type the frame can count in is still refused for reading a row. */
    @Test
    void anOffsetOfTheRightTypeStillMayNotReadARow() {
        assertError("42P10", "argument of ROWS must not contain variables",
                "SELECT sum(a) OVER (ORDER BY id ROWS BETWEEN bi PRECEDING AND CURRENT ROW) FROM gbc_num");
        assertError("42P10", "argument of ROWS must not contain variables",
                "SELECT sum(a) OVER (ORDER BY id ROWS BETWEEN nn PRECEDING AND CURRENT ROW) FROM gbc_num");
        assertError("42P10", "argument of ROWS must not contain variables",
                "SELECT a, count(*) OVER (ORDER BY a ROWS BETWEEN a PRECEDING AND CURRENT ROW)"
                        + " FROM gbc_nokey GROUP BY a");
    }

    @Test
    void frameOffsetsThatAlreadyWorkedKeepWorking() throws Exception {
        assertRows("SELECT sum(a) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)"
                + " FROM gbc_t ORDER BY 1", "10", "30", "50");
        assertRows("SELECT id, sum(a) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
                + " FROM gbc_num ORDER BY 1", "1|30", "2|30");
        assertRows("SELECT id, sum(a) OVER (ORDER BY id GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)"
                + " FROM gbc_num ORDER BY 1", "1|10", "2|30");
        assertRows("SELECT id, sum(a) OVER (ORDER BY nn RANGE BETWEEN 0.5 PRECEDING AND CURRENT ROW)"
                + " FROM gbc_num ORDER BY 1", "1|10", "2|20");
        assertRows("SELECT id, sum(a) OVER (ORDER BY id ROWS BETWEEN (SELECT 1) PRECEDING AND CURRENT ROW)"
                + " FROM gbc_num ORDER BY 1", "1|10", "2|30");
        assertRows("SELECT sum(a) OVER w FROM gbc_t"
                + " WINDOW w AS (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) ORDER BY 1",
                "10", "30", "50");
        assertError("0A000", "RANGE with offset PRECEDING/FOLLOWING is not supported for column type text",
                "SELECT sum(a) OVER (ORDER BY b RANGE BETWEEN 'x' PRECEDING AND CURRENT ROW) FROM gbc_nokey");
    }

    // ---- The shapes every real query is made of ----

    @Test
    void ordinaryGroupedQueriesAreUntouched() throws Exception {
        assertRows("SELECT a, count(*) FROM gbc_nokey GROUP BY a ORDER BY a", "1|2", "2|1");
        assertRows("SELECT a, count(*) FROM gbc_nokey GROUP BY a ORDER BY count(*) DESC, a",
                "1|2", "2|1");
        assertRows("SELECT p.id, p.other, count(n.a) FROM gbc_pk p LEFT JOIN gbc_nokey n ON n.a = p.id"
                + " GROUP BY p.id ORDER BY 1", "1|a|2", "2|b|1");
        assertRows("SELECT id, other FROM gbc_pk GROUP BY id ORDER BY 1", "1|a", "2|b");
        assertRows("SELECT s.x, count(*) FROM (SELECT a AS x, b AS y FROM gbc_nokey) s"
                + " GROUP BY s.x ORDER BY 1", "1|2", "2|1");
        assertRows("WITH c AS (SELECT a, b FROM gbc_nokey) SELECT a, count(*) FROM c GROUP BY a ORDER BY 1",
                "1|2", "2|1");
        assertRows("SELECT n.a, x.c FROM gbc_nokey n,"
                + " LATERAL (SELECT count(*) AS c FROM gbc_other o WHERE o.k = n.a) x"
                + " GROUP BY n.a, x.c ORDER BY 1", "1|1", "2|1");
        assertRows("SELECT n.a, count(x.s) FROM gbc_nokey n,"
                + " LATERAL (SELECT s FROM gbc_other o WHERE o.k = n.a) x GROUP BY n.a ORDER BY 1",
                "1|2", "2|1");
        assertRows("SELECT a, count(*) FROM gbc_nokey GROUP BY GROUPING SETS ((a), ()) ORDER BY 1",
                "1|2", "2|1", "null|3");
        assertRows("SELECT a, count(*) FROM gbc_nokey GROUP BY ROLLUP (a) ORDER BY 1",
                "1|2", "2|1", "null|3");
        assertRows("SELECT a + 0 FROM gbc_nokey GROUP BY a + 0 ORDER BY 1", "1", "2");
        assertRows("SELECT a, string_agg(b, ',' ORDER BY b) FROM gbc_nokey GROUP BY a ORDER BY 1",
                "1|x,y", "2|z");
        assertRows("SELECT a, sum(a) OVER (PARTITION BY a) FROM gbc_nokey ORDER BY 1, 2",
                "1|2", "1|2", "2|2");
        assertRows("SELECT sub.rn FROM (SELECT row_number() OVER (ORDER BY a) AS rn FROM gbc_nokey) sub"
                + " WHERE sub.rn >= 1 ORDER BY 1", "1", "2", "3");
    }

    /** A LATERAL item's output is judged like any other column — this already held. */
    @Test
    void aLateralItemsOutputIsJudgedLikeAnyOtherColumn() {
        assertUngrouped("x.s", "SELECT a, x.s FROM gbc_nokey,"
                + " LATERAL (SELECT s FROM gbc_other o WHERE o.s = gbc_nokey.b) x GROUP BY a");
    }
}
