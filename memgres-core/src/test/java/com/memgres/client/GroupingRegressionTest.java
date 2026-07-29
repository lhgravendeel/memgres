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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Grouped queries PostgreSQL runs that the grouping rules had begun to refuse.
 *
 * <p>Three rules judged the written form of a query rather than what it means, and each of them
 * turned working SQL into an error. All three are measured against PostgreSQL 18:
 *
 * <ul>
 *   <li><b>A sort key was matched by spelling.</b> {@code SELECT DISTINCT s, count(*) ... GROUP BY
 *       s ORDER BY d.s} was refused because the qualified {@code d.s} is not literally the bare
 *       {@code s} of the select list, though both resolve to the same column. Matching is on the
 *       resolved column, so a sort key that names a column the select list does not carry —
 *       {@code ORDER BY u.a} beside a selected {@code t.a} — is still refused.</li>
 *   <li><b>A count compared against a non-integer was looked for in every operator.</b>
 *       {@code HAVING count(*) || 'x' = '2x'} is a concatenation, not a comparison, and
 *       PostgreSQL runs it; only a comparison and the four arithmetic operators read a bare
 *       string literal as the bigint beside them, and only those are checked now.</li>
 *   <li><b>A cast to the column's own type was read as a coercion.</b> PostgreSQL erases such a
 *       cast while it analyses the query, so {@code GROUP BY a::int} over an {@code int} column
 *       is {@code GROUP BY a} — it licenses a bare {@code a}, and a primary key grouped that way
 *       still determines its row. A cast to any other type, {@code b::varchar} over {@code text}
 *       or {@code n::numeric} over {@code numeric(10,2)}, remains a real coercion that leaves
 *       the column ungrouped.</li>
 * </ul>
 */
class GroupingRegressionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE gre_f (id int PRIMARY KEY, a int, b text, d date, n numeric(10,2))");
        exec("INSERT INTO gre_f VALUES (1,10,'x',DATE '2020-01-01',1.5),"
                + "(2,10,'y',DATE '2020-02-01',2.5),(3,20,'z',DATE '2021-01-01',3.5)");
        exec("CREATE TABLE gre_t (id int PRIMARY KEY, a int, b text)");
        exec("INSERT INTO gre_t VALUES (1,10,'x'),(2,20,'y'),(3,10,'z')");
        exec("CREATE TABLE gre_u (id int PRIMARY KEY, a int, c text)");
        exec("INSERT INTO gre_u VALUES (1,10,'p'),(2,30,'q')");
        exec("CREATE TABLE gre_d (id int PRIMARY KEY, s text, n numeric)");
        exec("INSERT INTO gre_d VALUES (1,'a',1.5),(2,'a',2.5),(3,'b',3.5)");
        exec("CREATE TABLE gre_cust (id int PRIMARY KEY, name text, region text)");
        exec("INSERT INTO gre_cust VALUES (1,'Ann','EU'),(2,'Bob','US'),(3,'Cid','EU'),(4,'Dee','APAC')");
        exec("CREATE TABLE gre_ord (id int PRIMARY KEY, cust_id int, total numeric(10,2))");
        exec("INSERT INTO gre_ord VALUES (10,1,100.00),(11,1,50.50),(12,2,75.25),(13,3,10.00),(14,3,20.00)");
        exec("CREATE TABLE gre_w (id serial PRIMARY KEY, v varchar(3), c char(2), ts timestamp)");
        exec("INSERT INTO gre_w (v,c,ts) VALUES ('ab','pq',TIMESTAMP '2024-01-01 10:00:00'),"
                + "('cd','rs',TIMESTAMP '2024-01-02 10:00:00')");
        exec("CREATE TYPE gre_mood AS ENUM ('ok','bad')");
        exec("CREATE TABLE gre_e (id int PRIMARY KEY, m gre_mood)");
        exec("INSERT INTO gre_e VALUES (1,'ok'),(2,'ok'),(3,'bad')");
        exec("CREATE VIEW gre_v AS SELECT s, count(*) AS c FROM gre_d GROUP BY s");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** One row per element, columns joined by '|', in the order the query answered them. */
    private static String rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            List<String> out = new ArrayList<String>();
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

    // ---- a sort key matched on the column, not the spelling ----

    @Test
    void distinctSortKeyMayBeWrittenQualified() throws Exception {
        assertEquals("a|2,b|1", rows("SELECT DISTINCT s, count(*) FROM gre_d d GROUP BY s ORDER BY d.s"));
        assertEquals("b|1,a|2", rows("SELECT DISTINCT s, count(*) FROM gre_d d GROUP BY s ORDER BY d.s DESC"));
        assertEquals("a|2,b|1", rows("SELECT DISTINCT s AS k, count(*) FROM gre_d d GROUP BY s ORDER BY d.s"));
        assertEquals("b|1,a|2", rows("SELECT DISTINCT s AS k, count(*) FROM gre_d d GROUP BY s ORDER BY d.s DESC"));
    }

    /** The relation may be named by its own name as well as by an alias it was not given. */
    @Test
    void distinctSortKeyMayNameTheTable() throws Exception {
        assertEquals("a|2,b|1", rows(
                "SELECT DISTINCT s, count(*) FROM gre_d GROUP BY s ORDER BY gre_d.s"));
        assertEquals("b|1,a|2", rows(
                "SELECT DISTINCT s, count(*) FROM gre_d GROUP BY s ORDER BY gre_d.s DESC"));
    }

    /** A primary key groups the whole row, and the sort key may qualify any column of it. */
    @Test
    void distinctSortKeyOverAFunctionallyDeterminedColumn() throws Exception {
        assertEquals("1|Ann,2|Bob,3|Cid,4|Dee", rows(
                "SELECT DISTINCT id, name FROM gre_cust c GROUP BY 1 ORDER BY c.name"));
        assertEquals("4|Dee,3|Cid,2|Bob,1|Ann", rows(
                "SELECT DISTINCT id, name FROM gre_cust c GROUP BY 1 ORDER BY c.name DESC"));
    }

    /** A sub-select in FROM answers for its own alias, which the sort key may write. */
    @Test
    void distinctSortKeyOverASubqueryColumn() throws Exception {
        String sql = "SELECT DISTINCT rn, count(*) FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn"
                + " FROM gre_ord) sub WHERE rn >= 1 GROUP BY rn ORDER BY sub.rn";
        assertEquals("1|1,2|1,3|1,4|1,5|1", rows(sql));
        assertEquals("5|1,4|1,3|1,2|1,1|1", rows(sql + " DESC"));
    }

    /** Whole expressions match the same way, once each side's columns are resolved. */
    @Test
    void distinctSortKeyMayBeAnExpressionOfTheSelectList() throws Exception {
        assertEquals("A|2,B|1", rows(
                "SELECT DISTINCT upper(d.s), count(*) FROM gre_d d GROUP BY s ORDER BY upper(s)"));
        assertEquals("B|1,A|2", rows(
                "SELECT DISTINCT upper(s), count(*) FROM gre_d d GROUP BY s ORDER BY upper(d.s) DESC"));
    }

    /** The rule that belongs: a sort key naming a column the DISTINCT does not keep. */
    @Test
    void distinctSortKeyOutsideTheSelectListIsStillRejected() {
        assertRejected("SELECT DISTINCT s, count(*) FROM gre_d d GROUP BY s ORDER BY d.n",
                "42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
        assertRejected("SELECT DISTINCT s FROM gre_d GROUP BY s ORDER BY count(*)",
                "42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
        assertRejected("SELECT DISTINCT count(*) FROM gre_d d GROUP BY s ORDER BY d.s",
                "42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
        assertRejected("SELECT DISTINCT s, count(*) FROM gre_d d GROUP BY s ORDER BY d.s || 'x'",
                "42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
        assertRejected("SELECT DISTINCT s FROM gre_d GROUP BY s ORDER BY max(id)",
                "42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
    }

    /** A qualified sort key naming another relation's column of the same name is not a match. */
    @Test
    void distinctSortKeyOfAnotherRelationIsRejected() {
        assertRejected("SELECT DISTINCT t.a, count(*) FROM gre_t t JOIN gre_u u ON t.a = u.a"
                        + " GROUP BY t.a ORDER BY u.a",
                "42P10", "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
    }

    /** Nothing here is about DISTINCT alone: the ordinary sort keys keep working. */
    @Test
    void ordinaryDistinctSortKeysStillWork() throws Exception {
        assertEquals("a|2,b|1", rows("SELECT DISTINCT s, count(*) FROM gre_d GROUP BY s ORDER BY 1"));
        assertEquals("a|2,b|1", rows("SELECT DISTINCT s AS k, count(*) FROM gre_d GROUP BY s ORDER BY k"));
        assertEquals("a,b", rows("SELECT DISTINCT s FROM gre_d ORDER BY s"));
        assertEquals("a|1,a|2,b|3", rows("SELECT DISTINCT ON (s, id) s, id FROM gre_d d ORDER BY d.s, d.id"));
    }

    // ---- a count compared against a non-integer, and nothing else ----

    @Test
    void aCountConcatenatedWithAStringIsOrdinarySql() throws Exception {
        assertEquals("20", rows("SELECT a FROM gre_t GROUP BY a HAVING count(*) || 'x' = '1x' ORDER BY 1"));
        assertEquals("10,20", rows("SELECT a FROM gre_t GROUP BY a HAVING length(count(*) || 'x') = 2 ORDER BY 1"));
        assertEquals("10", rows("SELECT a FROM gre_t GROUP BY a HAVING 'x' || count(*) = 'x2' ORDER BY 1"));
        assertEquals("10", rows("SELECT a FROM gre_t GROUP BY a HAVING count(a) || 'x' = '2x' ORDER BY 1"));
        assertEquals("10,20", rows(
                "SELECT a FROM gre_t GROUP BY a HAVING (count(*) || 'x') IS NOT NULL ORDER BY 1"));
        assertEquals("a|2", rows("SELECT s, count(*) FROM gre_d GROUP BY s HAVING count(*) || 'x' = '2x'"));
        assertEquals("a|2", rows("SELECT s, count(*) FROM gre_d GROUP BY s HAVING 'a' || count(*) || 'b' = 'a2b'"));
        assertEquals("a|2,b|1", rows(
                "SELECT s, count(*) FROM gre_d GROUP BY s HAVING count(*) || '' <> '' ORDER BY 1"));
    }

    /** The rule that belongs: a comparison does read the literal as the count's own type. */
    @Test
    void aCountComparedAgainstANonIntegerIsStillRejected() {
        String[] clauses = {
            "count(*) > 'x'", "count(*) = 'x'", "count(*) < 'abc'", "'x' = count(*)",
            "count(*) <> 'x'", "count(*) >= 'x'", "count(*) <= 'x'",
        };
        for (String clause : clauses) {
            assertRejected("SELECT a FROM gre_t GROUP BY a HAVING " + clause,
                    "22P02", "invalid input syntax for type bigint");
        }
    }

    /** Arithmetic reads it that way too, which is why the message names bigint and not float. */
    @Test
    void aCountInArithmeticWithANonNumberIsStillRejected() {
        String[] clauses = {
            "count(*) + 'x' = 2", "count(*) - 'x' = 2", "count(*) * 'x' = 2",
            "count(*) / 'x' = 2", "count(*) - '1.5' = 2",
        };
        for (String clause : clauses) {
            assertRejected("SELECT a FROM gre_t GROUP BY a HAVING " + clause,
                    "22P02", "invalid input syntax for type bigint");
        }
    }

    /** A literal that reads as an integer is one, whichever operator it stands beside. */
    @Test
    void aCountBesideAnIntegerLiteralIsAccepted() throws Exception {
        assertEquals("10", rows("SELECT a FROM gre_t GROUP BY a HAVING count(*) > '1' ORDER BY 1"));
        assertEquals("10", rows("SELECT a FROM gre_t GROUP BY a HAVING count(*) + '1' = 3 ORDER BY 1"));
        assertEquals("20", rows("SELECT a FROM gre_t GROUP BY a HAVING count(*) * '2' = 2 ORDER BY 1"));
        assertEquals("10,20", rows("SELECT a FROM gre_t GROUP BY a HAVING length(count(*) || '') > 0 ORDER BY 1"));
    }

    /** The other type error HAVING carries is untouched: sum and avg are for numbers. */
    @Test
    void anAggregateOverTheWrongTypeIsStillRejected() {
        assertRejected("SELECT a FROM gre_t GROUP BY a HAVING sum(b) > 1",
                "42883", "function sum(text) does not exist");
        assertRejected("SELECT a FROM gre_t GROUP BY a HAVING avg(b) > 1",
                "42883", "function avg(text) does not exist");
    }

    // ---- a cast to the column's own type ----

    @Test
    void groupingByAColumnCastToItsOwnTypeGroupsByTheColumn() throws Exception {
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM gre_f GROUP BY a::int ORDER BY 1"));
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM gre_f GROUP BY a::int4 ORDER BY 1"));
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM gre_f GROUP BY a::integer ORDER BY 1"));
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM gre_f GROUP BY CAST(a AS integer) ORDER BY 1"));
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM gre_f GROUP BY (a)::int ORDER BY 1"));
        assertEquals("10|2,20|1", rows("SELECT f.a, count(*) FROM gre_f f GROUP BY f.a::int ORDER BY 1"));
        assertEquals("x|1,y|1,z|1", rows("SELECT b, count(*) FROM gre_f GROUP BY b::text ORDER BY 1"));
        assertEquals("2020-01-01|1,2020-02-01|1,2021-01-01|1", rows(
                "SELECT d, count(*) FROM gre_f GROUP BY d::date ORDER BY 1"));
        assertEquals("1.50|1,2.50|1,3.50|1", rows(
                "SELECT n, count(*) FROM gre_f GROUP BY n::numeric(10,2) ORDER BY 1"));
        assertEquals("ab|1,cd|1", rows("SELECT v, count(*) FROM gre_w GROUP BY v::varchar(3) ORDER BY 1"));
        assertEquals("pq|1,rs|1", rows("SELECT c, count(*) FROM gre_w GROUP BY c::char(2) ORDER BY 1"));
        assertEquals("2024-01-01 10:00:00|1,2024-01-02 10:00:00|1", rows(
                "SELECT ts, count(*) FROM gre_w GROUP BY ts::timestamp ORDER BY 1"));
        assertEquals("ok|2,bad|1", rows("SELECT m, count(*) FROM gre_e GROUP BY m::gre_mood ORDER BY 1"));
    }

    /** Two casts are two no-ops, and one buried in an expression is erased where it stands. */
    @Test
    void aNoOpCastIsErasedWhereverItStands() throws Exception {
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM gre_f GROUP BY a::int::int ORDER BY 1"));
        assertEquals("11|2,21|1", rows("SELECT a + 1, count(*) FROM gre_f GROUP BY a::int + 1 ORDER BY 1"));
        assertEquals("11|2,21|1", rows("SELECT a::int + 1, count(*) FROM gre_f GROUP BY a + 1 ORDER BY 1"));
    }

    /** The key erases too, so the whole row it determines stays available. */
    @Test
    void aPrimaryKeyCastToItsOwnTypeStillDeterminesTheRow() throws Exception {
        assertEquals("1|x|1,2|y|1,3|z|1", rows(
                "SELECT id, b, count(*) FROM gre_f GROUP BY id::int ORDER BY 1"));
        assertEquals("1|ab|1,2|cd|1", rows(
                "SELECT id, v, count(*) FROM gre_w GROUP BY id::int ORDER BY 1"));
    }

    /** The rule that belongs: a cast to another type is a value of its own. */
    @Test
    void groupingByARealCoercionStillLeavesTheColumnUngrouped() {
        String[] items = {
            "b::varchar", "n::numeric", "a::bigint", "a::text", "v::varchar", "v::varchar(4)",
            "v::text", "c::char", "ts::date", "m::text",
        };
        String[] tables = {
            "gre_f", "gre_f", "gre_f", "gre_f", "gre_w", "gre_w", "gre_w", "gre_w", "gre_w", "gre_e",
        };
        String[] columns = {"b", "n", "a", "a", "v", "v", "v", "c", "ts", "m"};
        for (int i = 0; i < items.length; i++) {
            assertRejected("SELECT " + columns[i] + ", count(*) FROM " + tables[i]
                            + " GROUP BY " + items[i],
                    "42803", "must appear in the GROUP BY clause");
        }
    }

    /** A cast over something whose type is not declared is left as the coercion it looks like. */
    @Test
    void aCastOverANonColumnIsNotErased() {
        assertRejected("SELECT a, count(*) FROM gre_f GROUP BY (a + 0)::int",
                "42803", "column \"gre_f.a\" must appear in the GROUP BY clause");
        assertRejected("SELECT count(*) FROM gre_f GROUP BY nosuch::int",
                "42703", "column \"nosuch\" does not exist");
    }

    // ---- the shapes around all three rules ----

    @Test
    void ordinaryGroupedQueriesAreUnchanged() throws Exception {
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM gre_t GROUP BY a ORDER BY 1"));
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM gre_t GROUP BY 1 ORDER BY 1"));
        assertEquals("10|2,20|1", rows("SELECT a + 0, count(*) FROM gre_t GROUP BY a + 0 ORDER BY 1"));
        assertEquals("1|10|x,2|20|y,3|10|z", rows("SELECT id, a, b FROM gre_t GROUP BY id ORDER BY 1"));
        assertEquals("10|2", rows("SELECT a, count(*) FROM gre_t GROUP BY a HAVING count(*) > 1 ORDER BY 1"));
        assertEquals("20", rows("SELECT a FROM gre_t GROUP BY a HAVING a > 10 ORDER BY 1"));
        assertEquals("20|1,10|2", rows("SELECT a, count(*) AS c FROM gre_t GROUP BY a ORDER BY c"));
        assertEquals("20,10", rows("SELECT a FROM gre_t GROUP BY a ORDER BY count(*), a"));
        assertEquals("10|2,20|1", rows("SELECT a, count(*) FROM gre_t GROUP BY a ORDER BY 2 DESC, 1"));
        assertEquals("10|2|1,20|1|2", rows(
                "SELECT a, count(*), row_number() OVER (ORDER BY a) FROM gre_t GROUP BY a ORDER BY 1"));
        assertEquals("10|1,20|1", rows(
                "SELECT t.a, count(*) FROM gre_t t JOIN gre_u u ON t.id = u.id GROUP BY t.a ORDER BY 1"));
        assertEquals("10|2,20|1", rows(
                "SELECT t.a, x.c FROM (SELECT DISTINCT a FROM gre_t) t,"
                        + " LATERAL (SELECT count(*) AS c FROM gre_t u WHERE u.a = t.a) x ORDER BY 1"));
        assertEquals("a|2,b|1", rows("SELECT s, c FROM gre_v ORDER BY 1"));
        assertEquals("10|2,20|1", rows("SELECT * FROM (SELECT a, count(*) c FROM gre_t GROUP BY a) q ORDER BY 1"));
        assertEquals("10|2,20|1", rows(
                "WITH g AS (SELECT a, count(*) c FROM gre_t GROUP BY a) SELECT * FROM g ORDER BY 1"));
        assertEquals("10|1,10|2,20|1,30|1", rows(
                "SELECT a, count(*) FROM gre_t GROUP BY a UNION SELECT a, count(*) FROM gre_u GROUP BY a"
                        + " ORDER BY 1, 2"));
        assertEquals("1,2,3,4,5", rows(
                "SELECT rn FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM gre_ord) sub"
                        + " WHERE rn >= 1 ORDER BY 1"));
        assertEquals("null|3,10|2,20|1", rows(
                "SELECT a, count(*) FROM gre_t GROUP BY ROLLUP (a) ORDER BY 1 NULLS FIRST"));
        assertEquals("1|x,2|y,3|z", rows("SELECT id, b FROM gre_t GROUP BY GROUPING SETS ((id)) ORDER BY 1"));
    }

    @Test
    void theGroupingErrorsThatBelongAreUnchanged() {
        assertRejected("SELECT a, b FROM gre_t GROUP BY a",
                "42803", "column \"gre_t.b\" must appear in the GROUP BY clause");
        assertRejected("SELECT a FROM gre_t GROUP BY a HAVING b > 'a'",
                "42803", "column \"gre_t.b\" must appear in the GROUP BY clause");
        assertRejected("SELECT a, count(*) FROM gre_t GROUP BY a ORDER BY b",
                "42803", "column \"gre_t.b\" must appear in the GROUP BY clause");
        assertRejected("SELECT id, b FROM gre_t GROUP BY ROLLUP (id)",
                "42803", "column \"gre_t.b\" must appear in the GROUP BY clause");
        assertRejected("SELECT a, row_number() OVER (ORDER BY b) FROM gre_t GROUP BY a",
                "42803", "column \"gre_t.b\" must appear in the GROUP BY clause");
        assertRejected("SELECT a, count(*) OVER (PARTITION BY b) FROM gre_t GROUP BY a",
                "42803", "column \"gre_t.b\" must appear in the GROUP BY clause");
        assertRejected("SELECT DISTINCT ON (b) a FROM gre_t GROUP BY a",
                "42803", "column \"gre_t.b\" must appear in the GROUP BY clause");
        assertRejected("SELECT a, GROUPING(b) FROM gre_t GROUP BY a",
                "42803", "arguments to GROUPING must be grouping expressions");
        assertRejected("SELECT count(*) FROM gre_t GROUP BY nosuch", "42703", "column \"nosuch\" does not exist");
        assertRejected("SELECT a FROM gre_t GROUP BY 9", "42P10", "GROUP BY position 9 is not in select list");
        assertRejected("SELECT count(*) FROM gre_t GROUP BY 'x'", "42601", "non-integer constant in GROUP BY");
        assertRejected("SELECT a FROM gre_t GROUP BY count(*)",
                "42803", "aggregate functions are not allowed in GROUP BY");
        assertRejected("SELECT a, (SELECT count(*) FROM gre_u u WHERE u.a = t.id) FROM gre_t t GROUP BY a",
                "42803", "subquery uses ungrouped column \"t.id\" from outer query");
    }
}
