package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a WITH clause spells, what it may not spell twice, and what its columns are, measured
 * against PostgreSQL 18.
 *
 * <p>Four subjects, all reached through the same WITH clause.
 *
 * <p><b>The clauses after a WITH item.</b> PostgreSQL's grammar allows one SEARCH and one CYCLE,
 * in that order, attached to the item they follow. Reading them twice — once per item and again
 * after the whole clause — let the second pass rebuild the last item from whichever of the two it
 * found and drop the other. Written {@code CYCLE … SEARCH …} that dropped the CYCLE clause, which
 * was the only thing bounding the recursion, and the query that used to be a syntax error ran out
 * of memory instead. They are read once now, and a second SEARCH or CYCLE is the syntax error
 * PostgreSQL says it is.
 *
 * <p><b>What SEARCH and CYCLE may name.</b> Declaring RECURSIVE does not make an item recursive;
 * naming itself does, and only an item that recurses can be ordered or cut. A column the item does
 * not have cannot be searched or compared, and a column the clause adds under a name the query
 * already uses would leave two columns answering to it.
 *
 * <p><b>What the recursive term's columns may be.</b> The union resolves one type per column from
 * both arms and the seed's rows have to already carry it, so a recursive term that widens a column
 * is refused — but only where the widening can be shown. The character types are not a ladder
 * (PostgreSQL runs a varchar seed with a text recursive term), and a bare string literal has no
 * type of its own, so neither is refused here.
 *
 * <p><b>What a name reaches.</b> A WITH item lives in no schema, so {@code public.c} is always the
 * stored relation; a quoted declaration keeps its case, so {@code WITH "X"} is not {@code x}; and
 * an inner WITH clause declaring the same name means its own item everywhere below, not the one
 * being defined.
 *
 * <p>Position is not asserted anywhere here. This engine derives it by searching the statement
 * text for the first quoted name in the message, which lands on the right offset only by luck —
 * for a one-letter WITH name it finds the "t" of WITH. That approximation is engine-wide and not
 * this area's to fix.
 */
class CteCorrectionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE ctc_t (i int)");
        exec("INSERT INTO ctc_t VALUES (1),(2),(3)");
        exec("CREATE TABLE ctc_w (a int, b text)");
        exec("INSERT INTO ctc_w VALUES (1,'x'),(2,'y'),(2,'z'),(3,'q')");
        exec("CREATE TABLE ctc_e (src int, dst int)");
        exec("INSERT INTO ctc_e VALUES (1,2),(1,3),(2,4),(3,5),(5,1)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE IF EXISTS ctc_t");
            exec("DROP TABLE IF EXISTS ctc_w");
            exec("DROP TABLE IF EXISTS ctc_e");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    /** All rows of a query, each row rendered as pipe-joined column values. */
    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
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
    }

    /** The output column labels of a query. */
    private static List<String> labels(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            try (ResultSet rs = st.executeQuery(sql)) {
                ResultSetMetaData md = rs.getMetaData();
                List<String> out = new ArrayList<>();
                for (int i = 1; i <= md.getColumnCount(); i++) out.add(md.getColumnLabel(i));
                return out;
            }
        }
    }

    private static void assertRows(String sql, String... expected) throws SQLException {
        assertEquals(java.util.Arrays.asList(expected), rows(sql), sql);
    }

    /** Asserts the statement fails with this SQLSTATE and a message containing this text. */
    private static void assertFails(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> rows(sql), sql);
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> expected \"" + messagePart + "\" in \"" + e.getMessage() + "\"");
    }

    // ================================================================
    // 1. SEARCH and CYCLE are read once, in PostgreSQL's order
    // ================================================================

    @Test
    @DisplayName("CYCLE followed by SEARCH is a syntax error, not an unbounded recursion")
    @Timeout(30)
    void cycleThenSearchIsASyntaxError() {
        // Reading these words twice rebuilt the item from the SEARCH clause alone and threw the
        // CYCLE clause away, leaving nothing to stop this recursion: it ran to out of memory.
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (n+1) % 3 FROM r "
                        + "WHERE n < 10) CYCLE n SET is_cycle USING path "
                        + "SEARCH DEPTH FIRST BY n SET ord SELECT n FROM r",
                "42601", "syntax error at or near \"SEARCH\"");
    }

    @Test
    @DisplayName("a second SEARCH or CYCLE clause is a syntax error")
    void oneSearchAndOneCycleOnly() {
        String body = "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) ";
        assertFails(body + "SEARCH DEPTH FIRST BY n SET a SEARCH BREADTH FIRST BY n SET b "
                + "SELECT * FROM r", "42601", "syntax error at or near \"SEARCH\"");
        assertFails(body + "CYCLE n SET a USING p CYCLE n SET b USING q SELECT * FROM r",
                "42601", "syntax error at or near \"CYCLE\"");
        assertFails(body + "SEARCH DEPTH FIRST BY n SET a CYCLE n SET c USING p "
                + "SEARCH BREADTH FIRST BY n SET b SELECT * FROM r",
                "42601", "syntax error at or near \"SEARCH\"");
    }

    @Test
    @DisplayName("one SEARCH then one CYCLE, on any item, still reads")
    void searchThenCycleOnAnyItem() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (n+1)%3 FROM r WHERE n < 10) "
                + "SEARCH DEPTH FIRST BY n SET o CYCLE n SET c USING p "
                + "SELECT n, c FROM r ORDER BY n, c", "0|f", "1|f", "1|t", "2|f");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                + "SEARCH DEPTH FIRST BY n SET ord, s AS (SELECT 9 AS k) "
                + "SELECT r.n, s.k FROM r, s ORDER BY 1", "1|9", "2|9", "3|9");
        assertRows("WITH RECURSIVE s AS (SELECT 9 AS k), r(n) AS "
                + "(SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                + "SEARCH BREADTH FIRST BY n SET ord SELECT n FROM r ORDER BY n", "1", "2", "3");
    }

    // ================================================================
    // 2. SEARCH and CYCLE need a recursion, and columns that exist
    // ================================================================

    @Test
    @DisplayName("SEARCH on an item that never names itself is 42601, even unread")
    void searchOnANonRecursiveItem() {
        // s is never selected from; PostgreSQL refuses the clause all the same.
        assertFails("WITH RECURSIVE s AS (SELECT 9 AS k) SEARCH DEPTH FIRST BY k SET ord, "
                        + "r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                        + "SELECT * FROM r",
                "42601", "WITH query is not recursive");
        assertFails("WITH s AS (SELECT 9 AS k) SEARCH DEPTH FIRST BY k SET ord SELECT * FROM s",
                "42601", "WITH query is not recursive");
    }

    @Test
    @DisplayName("a search or cycle column must be in the WITH query's column list")
    void searchAndCycleColumnsMustExist() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                        + "SEARCH DEPTH FIRST BY zz SET ord SELECT * FROM r",
                "42601", "search column \"zz\" not in WITH query column list");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (n+1)%3 FROM r WHERE n < 10) "
                        + "CYCLE zz SET is_cycle USING path SELECT * FROM r",
                "42601", "cycle column \"zz\" not in WITH query column list");
    }

    @Test
    @DisplayName("the column a clause adds may not collide with one the query has")
    void addedColumnMayNotCollide() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                        + "SEARCH DEPTH FIRST BY n SET n SELECT * FROM r",
                "42702", "column reference \"n\" is ambiguous");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                        + "CYCLE n SET n USING path SELECT * FROM r",
                "42702", "column reference \"n\" is ambiguous");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (n+1) % 3 FROM r "
                        + "WHERE n < 10) CYCLE n SET c USING c SELECT * FROM r",
                "42601", "cycle mark column name and cycle path column name are the same");
    }

    @Test
    @DisplayName("names the query does not already use are added without complaint")
    void freeNamesAreAdded() throws Exception {
        assertEquals(java.util.Arrays.asList("n", "seq"),
                labels("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                        + "SEARCH DEPTH FIRST BY n SET seq SELECT n, seq FROM r"));
        assertEquals(java.util.Arrays.asList("n", "flag", "trail"),
                labels("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                        + "CYCLE n SET flag USING trail SELECT n, flag, trail FROM r"));
    }

    // ================================================================
    // 3. What SEARCH and CYCLE put in the row
    // ================================================================

    @Test
    @DisplayName("the breadth-first ordering column leads with the row's depth")
    void breadthFirstOrderingCarriesDepth() throws Exception {
        assertRows("WITH RECURSIVE r(node, depth) AS (SELECT 1, 0 UNION ALL "
                        + "SELECT e.dst, r.depth+1 FROM r JOIN ctc_e e ON e.src = r.node "
                        + "WHERE r.depth < 3) SEARCH BREADTH FIRST BY node SET ord "
                        + "SELECT node, ord::text FROM r ORDER BY depth, node",
                "1|(0,1)", "2|(1,2)", "3|(1,3)", "4|(2,4)", "5|(2,5)", "1|(3,1)");
    }

    @Test
    @DisplayName("the depth-first ordering column is an array of one record per step")
    void depthFirstOrderingIsAnArrayOfRecords() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                        + "SEARCH DEPTH FIRST BY n SET ord "
                        + "SELECT ord::text, pg_typeof(ord)::text FROM r ORDER BY n",
                "{(1)}|record[]", "{(1),(2)}|record[]", "{(1),(2),(3)}|record[]");
        // A multi-column search key makes each step a two-field record, quoted inside the array.
        assertRows("WITH RECURSIVE r(n,m) AS (SELECT 1,10 UNION ALL SELECT n+1,m-1 FROM r "
                        + "WHERE n < 3) SEARCH DEPTH FIRST BY n,m SET ord "
                        + "SELECT ord::text FROM r ORDER BY n",
                "{\"(1,10)\"}", "{\"(1,10)\",\"(2,9)\"}", "{\"(1,10)\",\"(2,9)\",\"(3,8)\"}");
    }

    @Test
    @DisplayName("the cycle path column is an array of records too")
    void cyclePathIsAnArrayOfRecords() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                        + "CYCLE n SET c USING p SELECT p::text, pg_typeof(p)::text FROM r "
                        + "ORDER BY n",
                "{(1)}|record[]", "{(1),(2)}|record[]", "{(1),(2),(3)}|record[]");
    }

    @Test
    @DisplayName("CYCLE ... TO v DEFAULT d marks with those values, in their own type")
    void cycleMarkValuesAreUsed() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (n+1)%3 FROM r WHERE n < 10) "
                        + "CYCLE n SET c TO 1 DEFAULT 0 USING p "
                        + "SELECT n, c, pg_typeof(c)::text FROM r ORDER BY n, c",
                "0|0|integer", "1|0|integer", "1|1|integer", "2|0|integer");
        // Writing the two the same way marks every row, the seed included, so nothing recurses.
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT (n+1)%3 FROM r WHERE n < 10) "
                        + "CYCLE n SET c TO true DEFAULT true USING p SELECT n FROM r", "1");
    }

    // ================================================================
    // 4. The recursive term's column types
    // ================================================================

    @Test
    @DisplayName("a recursive term that widens a numeric column is 42804")
    void wideningANumericColumnIsRefused() {
        String hint = "has type integer in non-recursive term but type ";
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1.0 FROM r WHERE n < 3) "
                + "SELECT * FROM r", "42804", hint + "numeric overall");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT trunc(n + 1.0) FROM r "
                + "WHERE n < 3) SELECT * FROM r", "42804", hint + "numeric overall");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + row_number() OVER () "
                + "FROM r WHERE n < 3) SELECT * FROM r", "42804", hint + "bigint overall");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT coalesce(n, 0::bigint) + 1 "
                + "FROM r WHERE n < 3) SELECT * FROM r", "42804", hint + "bigint overall");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT CASE WHEN n > 0 THEN n + 1 "
                + "ELSE 0::bigint END FROM r WHERE n < 3) SELECT * FROM r",
                "42804", hint + "bigint overall");
        assertFails("WITH RECURSIVE r(a,b) AS (SELECT 1, 1 UNION ALL SELECT a+1, b + 1.0 FROM r "
                + "WHERE a < 3) SELECT * FROM r", "42804", "column 2 " + hint + "numeric overall");
        assertFails("WITH RECURSIVE r(n) AS (SELECT i FROM ctc_t WHERE i = 1 UNION ALL "
                + "SELECT n + 1.0 FROM r WHERE n < 3) SELECT * FROM r",
                "42804", hint + "numeric overall");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL (SELECT n + 1.0 FROM r WHERE n < 3 "
                + "UNION ALL SELECT 9.5 WHERE false)) SELECT * FROM r",
                "42804", hint + "numeric overall");
    }

    @Test
    @DisplayName("date widened to timestamp, and an untyped seed, are 42804 too")
    void wideningOtherFamilies() {
        assertFails("WITH RECURSIVE r(d) AS (SELECT DATE '2020-01-01' UNION ALL "
                        + "SELECT d + interval '1 day' FROM r WHERE d < DATE '2020-01-03') "
                        + "SELECT * FROM r",
                "42804", "has type date in non-recursive term but type "
                        + "timestamp without time zone overall");
        assertFails("WITH RECURSIVE r(n) AS (SELECT NULL UNION ALL SELECT 1 FROM r "
                        + "WHERE n IS NULL) SELECT * FROM r",
                "42804", "has type text in non-recursive term but type integer overall");
    }

    @Test
    @DisplayName("two arms with no common type are an unmatched union")
    void armsWithNoCommonTypeAreUnmatched() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n::text FROM r "
                        + "WHERE n < 3) SELECT * FROM r",
                "42804", "UNION types integer and text cannot be matched");
        assertFails("WITH RECURSIVE r(n, m) AS (SELECT 1, 'x'::text UNION ALL SELECT n+1, 5 "
                        + "FROM r WHERE n < 3) SELECT * FROM r",
                "42804", "UNION types text and integer cannot be matched");
    }

    @Test
    @DisplayName("narrowing, staying put, and the character types are all allowed")
    void typesPostgresAcceptsAreAccepted() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1::bigint UNION ALL SELECT (n+1)::int FROM r "
                + "WHERE n < 3) SELECT n FROM r ORDER BY n", "1", "2", "3");
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1::bigint UNION ALL SELECT n + 1 FROM r "
                + "WHERE n < 3) SELECT n FROM r ORDER BY n", "1", "2", "3");
        // The character types are not a widening ladder in PostgreSQL: it runs both of these.
        assertRows("WITH RECURSIVE r(s) AS (SELECT 'a'::varchar UNION ALL SELECT (s || 'a')::text "
                + "FROM r WHERE length(s) < 3) SELECT s FROM r ORDER BY s", "a", "aa", "aaa");
        assertRows("WITH RECURSIVE r(s) AS (SELECT 'a'::name UNION ALL SELECT (s || 'a')::text "
                + "FROM r WHERE length(s) < 3) SELECT s FROM r ORDER BY s", "a", "aa", "aaa");
        // A bare string literal has no type of its own; it takes the seed's.
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT '5' FROM r WHERE n < 3) "
                + "SELECT n FROM r ORDER BY n", "1", "5");
        // A cast in a CASE condition decides nothing about the value's type.
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT CASE WHEN n::bigint > 0 "
                + "THEN n + 1 ELSE 0 END FROM r WHERE n < 3) SELECT n FROM r ORDER BY n",
                "1", "2", "3");
    }

    @Test
    @DisplayName("a string that is not a number in an integer column is still bad input")
    void unknownLiteralThatIsNotANumber() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 'x' FROM r WHERE n < 3) "
                        + "SELECT * FROM r",
                "22P02", "invalid input syntax for type integer: \"x\"");
    }

    // ================================================================
    // 5. Where a self-reference may sit
    // ================================================================

    @Test
    @DisplayName("a self-reference under EXCEPT or INTERSECT is 42P19")
    void selfReferenceUnderANonUnionSetOp() {
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL (SELECT 2 EXCEPT SELECT n FROM r))"
                        + " SELECT * FROM r",
                "42P19", "recursive reference to query \"r\" must not appear within EXCEPT");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL (SELECT i FROM ctc_t "
                        + "INTERSECT ALL SELECT n+1 FROM r WHERE n < 3)) SELECT * FROM r",
                "42P19", "recursive reference to query \"r\" must not appear within INTERSECT");
    }

    @Test
    @DisplayName("where the reference sits is reported before how many there are")
    void contextBeforeCount() {
        // Two references, one of them in a sub-select: PostgreSQL names the sub-select.
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3 "
                        + "AND n NOT IN (SELECT n FROM r)) SELECT * FROM r",
                "42P19", "recursive reference to query \"r\" must not appear within a subquery");
    }

    @Test
    @DisplayName("an inner WITH item of the same name is not a self-reference")
    void innerWithItemShadowsTheName() throws Exception {
        // r is defined but never names itself: the inner WITH claims the name, so the body is an
        // ordinary UNION ALL run once.
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT z.n+1 FROM "
                + "(WITH r AS (SELECT 1 AS n) SELECT n FROM r) z WHERE z.n < 3) "
                + "SELECT n FROM r ORDER BY n", "1", "2");
        // An inner WITH of a different name leaves the recursion alone.
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT z.k+1 FROM "
                + "(WITH q AS (SELECT n AS k FROM r) SELECT k FROM q) z WHERE z.k < 3) "
                + "SELECT n FROM r ORDER BY n", "1", "2", "3");
    }

    @Test
    @DisplayName("LIMIT ALL is a LIMIT clause a recursive query has not implemented")
    void limitAllInARecursiveQuery() {
        assertFails("WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 3 "
                        + "LIMIT ALL) SELECT count(*) FROM t",
                "0A000", "LIMIT in a recursive query is not implemented");
    }

    @Test
    @DisplayName("TABLESAMPLE applies to a stored relation, not to a WITH item")
    void tablesampleOnAWithItem() {
        assertFails("WITH c AS (SELECT 1 AS i) SELECT * FROM c TABLESAMPLE SYSTEM (100)",
                "0A000", "TABLESAMPLE clause can only be applied to tables and materialized views");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r "
                        + "TABLESAMPLE SYSTEM (100) WHERE n < 3) SELECT * FROM r",
                "0A000", "TABLESAMPLE clause can only be applied to tables and materialized views");
    }

    // ================================================================
    // 6. What a WITH name reaches
    // ================================================================

    @Test
    @DisplayName("a schema-qualified name is never a WITH item")
    void qualifiedNamesReachTheStoredRelation() throws Exception {
        assertRows("WITH ctc_t AS (SELECT 99 AS i) SELECT * FROM public.ctc_t ORDER BY i",
                "1", "2", "3");
        assertRows("WITH ctc_t AS (SELECT 99 AS i) SELECT * FROM ctc_t ORDER BY i", "99");
        assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM public.r "
                        + "WHERE n < 3) SELECT * FROM r",
                "42P01", "relation \"public.r\" does not exist");
        assertFails("WITH c AS (SELECT 1 AS i) SELECT * FROM public.c",
                "42P01", "relation \"public.c\" does not exist");
    }

    @Test
    @DisplayName("a quoted WITH name keeps its case")
    void quotedNamesAreCaseSensitive() throws Exception {
        assertFails("WITH \"X\" AS (SELECT 1 AS n) SELECT * FROM x",
                "42P01", "relation \"x\" does not exist");
        assertRows("WITH \"X\" AS (SELECT 1 AS n) SELECT * FROM \"X\"", "1");
        assertRows("WITH x AS (SELECT 1 AS n) SELECT * FROM \"x\"", "1");
        assertRows("WITH MyCte AS (SELECT 1 AS n) SELECT * FROM mycte", "1");
    }

    @Test
    @DisplayName("a forward reference says which WITH item it was and how to fix it")
    void forwardReferenceCarriesDetailAndHint() {
        SQLException e = assertThrows(SQLException.class,
                () -> rows("with a as (select 1 x from b), b as (select 2 y) select * from a"));
        assertEquals("42P01", e.getSQLState());
        assertTrue(e.getMessage().contains("relation \"b\" does not exist"), e.getMessage());
        assertTrue(e.getMessage().contains(
                "There is a WITH item named \"b\", but it cannot be referenced from this part "
                        + "of the query."), e.getMessage());
        assertTrue(e.getMessage().contains(
                "Use WITH RECURSIVE, or re-order the WITH items to remove forward references."),
                e.getMessage());
    }

    @Test
    @DisplayName("WITH RECURSIVE lets the same forward reference through")
    void recursiveMakesTheForwardReferenceLegal() throws Exception {
        assertRows("WITH RECURSIVE a AS (SELECT 1 AS x FROM b), b AS (SELECT 2 AS y) "
                + "SELECT * FROM a", "1");
    }

    // ================================================================
    // 7. Column lists on a WITH item and on a FROM item
    // ================================================================

    @Test
    @DisplayName("a WITH item's alias list may be shorter than the query's output")
    void shortAliasListsAreAllowed() throws Exception {
        assertRows("WITH x(a) AS (SELECT 1, 2) SELECT * FROM x", "1|2");
        assertEquals(java.util.Arrays.asList("a", "?column?"),
                labels("WITH x(a) AS (SELECT 1, 2) SELECT * FROM x"));
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1,5 UNION ALL SELECT n+1,6 FROM r WHERE n<3) "
                + "SELECT * FROM r ORDER BY n", "1|5", "2|6", "3|6");
    }

    @Test
    @DisplayName("naming more columns than the query has is a WITH query complaint")
    void tooManyAliasesNameTheWithQuery() {
        assertFails("WITH x(a,b) AS (SELECT 1) SELECT * FROM x",
                "42P10", "WITH query \"x\" has 1 columns available but 2 columns specified");
        assertFails("WITH x(a,b,c,d) AS (SELECT 1, 2, 3) SELECT * FROM x",
                "42P10", "WITH query \"x\" has 3 columns available but 4 columns specified");
    }

    @Test
    @DisplayName("a FROM item's alias list renames its columns")
    void fromItemAliasListRenames() throws Exception {
        assertEquals(java.util.Arrays.asList("m"), labels("SELECT * FROM ctc_t AS z(m)"));
        assertEquals(java.util.Arrays.asList("m", "n"),
                labels("SELECT * FROM ctc_w AS z(m, n)"));
        assertEquals(java.util.Arrays.asList("m", "b"),
                labels("SELECT * FROM ctc_w z(m)"));
        assertRows("SELECT z.m FROM ctc_w AS z(m, n) ORDER BY z.m", "1", "2", "2", "3");
        assertEquals(java.util.Arrays.asList("m"),
                labels("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                        + "SELECT * FROM r AS z(m)"));
        assertFails("SELECT * FROM ctc_w AS z(m, n, o)",
                "42P10", "table \"z\" has 2 columns available but 3 columns specified");
    }

    // ================================================================
    // 8. TABLE, LIMIT and WITH TIES around a WITH clause
    // ================================================================

    @Test
    @DisplayName("TABLE t is a query a WITH clause may hang off")
    void tableAfterAWithClause() throws Exception {
        assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                + "TABLE r", "1", "2", "3");
        assertRows("WITH c AS (SELECT 1 AS i) TABLE c", "1");
    }

    @Test
    @DisplayName("a LIMIT past bigint is out of range, not a negative count")
    void limitPastBigintIsOutOfRange() {
        assertFails("SELECT a FROM ctc_w ORDER BY 1 LIMIT 9999999999999999999",
                "22003", "bigint out of range");
        assertFails("SELECT a FROM ctc_w ORDER BY 1 OFFSET 9999999999999999999",
                "22003", "bigint out of range");
    }

    @Test
    @DisplayName("WITH TIES keeps the rows tied with the last one, on a set operation too")
    void withTiesOnASetOperation() throws Exception {
        assertRows("SELECT a FROM ctc_w UNION ALL SELECT 2 ORDER BY 1 "
                + "FETCH FIRST 2 ROWS WITH TIES", "1", "2", "2", "2");
        assertRows("SELECT a FROM ctc_w ORDER BY a FETCH FIRST 2 ROWS WITH TIES",
                "1", "2", "2");
        assertRows("SELECT a FROM ctc_w ORDER BY a FETCH FIRST 2 ROWS ONLY", "1", "2");
        assertFails("SELECT a FROM ctc_w FETCH FIRST 2 ROWS WITH TIES",
                "42601", "WITH TIES cannot be specified without ORDER BY clause");
    }

    @Test
    @DisplayName("LIMIT ALL outside a recursive query is still no limit")
    void limitAllIsNoLimit() throws Exception {
        assertRows("SELECT i FROM ctc_t ORDER BY 1 LIMIT ALL", "1", "2", "3");
        assertRows("SELECT i FROM ctc_t UNION ALL SELECT 9 ORDER BY 1 LIMIT ALL",
                "1", "2", "3", "9");
    }
}
