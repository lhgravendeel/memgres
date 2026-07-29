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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Where a recursive WITH item's self-reference may sit, which complaint about it comes first, and
 * what the clauses after the item declare — measured against PostgreSQL 18.
 *
 * <p><b>Where a self-reference may sit.</b> PostgreSQL evaluates it as one scan of the rows the
 * previous round produced, so it refuses the places that would need the whole result at once, and
 * only those. A sub-select of an expression and the null-extended side of an outer join are two of
 * them. A set operation is one only where an arm is subtracted or where duplicate counts matter:
 * the right-hand side of any EXCEPT, and either side of an INTERSECT ALL or EXCEPT ALL. Plain
 * INTERSECT and the left side of a plain EXCEPT grow with the previous round's rows, and
 * PostgreSQL runs them — refusing those too turned queries PostgreSQL answers into errors.
 *
 * <p><b>Which complaint comes first.</b> ORDER BY, LIMIT, OFFSET and FOR UPDATE are read off the
 * set operation before the self-reference is looked for at all, so a recursive query that is wrong
 * in both ways is refused for the clause PostgreSQL has not implemented. Among the references,
 * where one sits is decided before how many there are.
 *
 * <p><b>What the clauses after an item say.</b> SEARCH and CYCLE each add a column, so two of them
 * under one name leave the query no way to say which it meant. The value a CYCLE marks a row with
 * is a constant, and the mark and its default resolve to one type between them.
 *
 * <p><b>What the generated columns hold.</b> SEARCH BREADTH FIRST BY p,c orders by depth then p
 * then c, so its column is the record (depth, p, c). A CYCLE clause's two columns count towards a
 * UNION's duplicate removal, so the row that closes a cycle survives beside the one that started
 * it. Subscripting the path array answers with its element type rather than with jsonb.
 *
 * <p>Position is not asserted anywhere here. This engine derives it after the fact by searching the
 * statement text for the first quoted name in the message, which is approximate engine-wide and
 * not this area's to fix; every case in this class was measured with the Position line removed
 * from both engines' output.
 */
class CteCorrectionRoundTwoTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE ctn_edge (a int, b int)");
        exec("INSERT INTO ctn_edge VALUES (1,2),(2,3),(3,1),(3,4)");
        exec("CREATE TABLE ctn_tree (p int, c int)");
        exec("INSERT INTO ctn_tree VALUES (1,2),(1,3),(2,4),(2,5),(3,6)");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE IF EXISTS ctn_edge");
            exec("DROP TABLE IF EXISTS ctn_tree");
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
    // 1. A set operation inside the recursive term
    // ================================================================

    @Nested
    @DisplayName("a set operation the iteration can evaluate is run")
    class SetOperationsThatGrowWithTheRecursion {

        @Test
        @DisplayName("the left side of a plain EXCEPT")
        void exceptLeftArm() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                    + "(SELECT n+1 FROM r WHERE n < 3 EXCEPT SELECT 99)) "
                    + "SELECT * FROM r ORDER BY n", "1", "2", "3");
        }

        @Test
        @DisplayName("either side of a plain INTERSECT")
        void intersectEitherArm() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                    + "(SELECT n+1 FROM r WHERE n < 3 INTERSECT SELECT 2)) "
                    + "SELECT * FROM r ORDER BY n", "1", "2");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                    + "(SELECT 2 INTERSECT SELECT n+1 FROM r WHERE n < 3)) "
                    + "SELECT * FROM r ORDER BY n", "1", "2");
        }

        @Test
        @DisplayName("a FROM subquery is a place the reference may sit, set operation and all")
        void referenceInsideAFromSubquery() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT s.n+1 FROM "
                    + "(SELECT n FROM r EXCEPT SELECT 99) s WHERE s.n < 3) "
                    + "SELECT * FROM r ORDER BY n", "1", "2", "3");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                    + "(SELECT s.n+1 FROM (SELECT n FROM r) s WHERE s.n < 3 EXCEPT SELECT 99)) "
                    + "SELECT * FROM r ORDER BY n", "1", "2", "3");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                    + "SELECT n+1 FROM (SELECT n FROM r) q WHERE n < 3) "
                    + "SELECT * FROM r ORDER BY n", "1", "2", "3");
        }

        @Test
        @DisplayName("a set operation in the non-recursive term is nobody's business")
        void setOperationInTheSeed() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS ((SELECT 1 EXCEPT SELECT 99) UNION ALL "
                    + "SELECT n+1 FROM r WHERE n < 3) SELECT * FROM r ORDER BY n", "1", "2", "3");
            assertRows("WITH RECURSIVE r(n) AS ((SELECT 1 INTERSECT SELECT 1) UNION ALL "
                    + "SELECT n+1 FROM r WHERE n < 3) SELECT * FROM r ORDER BY n", "1", "2", "3");
        }
    }

    @Nested
    @DisplayName("a set operation that subtracts or counts duplicates is 42P19")
    class SetOperationsThatAreRefused {

        @Test
        @DisplayName("the right side of an EXCEPT is subtracted")
        void exceptRightArm() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                            + "(SELECT 99 EXCEPT SELECT n+1 FROM r WHERE n < 3)) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear within EXCEPT");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT * FROM "
                            + "(SELECT 99 EXCEPT SELECT n+1 FROM r WHERE n < 3) t) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear within EXCEPT");
        }

        @Test
        @DisplayName("either side of EXCEPT ALL or INTERSECT ALL counts duplicates")
        void theAllVariantsRefuseBothArms() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                            + "(SELECT n+1 FROM r WHERE n < 3 EXCEPT ALL SELECT 99)) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear within EXCEPT");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                            + "(SELECT 2 INTERSECT ALL SELECT n+1 FROM r WHERE n < 3)) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear within INTERSECT");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                            + "(SELECT n+1 FROM r WHERE n < 3 INTERSECT ALL SELECT 2)) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear within INTERSECT");
        }

        @Test
        @DisplayName("a sub-select is nearer than the EXCEPT around it")
        void theInnermostContextNamesTheReference() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                            + "(SELECT 99 EXCEPT SELECT 1 WHERE 1 IN (SELECT n FROM r))) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear within a subquery");
        }

        @Test
        @DisplayName("a self-reference in the non-recursive term is named for that")
        void referenceInTheSeed() {
            assertFails("WITH RECURSIVE r(n) AS ((SELECT n FROM r EXCEPT SELECT 99) UNION ALL "
                            + "SELECT 1) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" "
                            + "must not appear within its non-recursive term");
        }

        @Test
        @DisplayName("two admissible references are \"more than once\", whatever encloses them")
        void twoReferencesUnderAnIntersect() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL (SELECT n+1 FROM r WHERE n < 3 "
                            + "INTERSECT SELECT n+1 FROM r WHERE n < 3)) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear more than once");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT max(n) FROM r a, r b) "
                            + "SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear more than once");
        }
    }

    // ================================================================
    // 2. The clause PostgreSQL has not implemented is read first
    // ================================================================

    @Nested
    @DisplayName("an unimplemented clause outranks the self-reference rules")
    class TheClauseIsReadBeforeTheReference {

        @Test
        @DisplayName("LIMIT beats two references, a reference in the seed, and one in a subquery")
        void limitFirst() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT a.n+b.n FROM r a, r b "
                            + "WHERE a.n < 3 LIMIT 5) SELECT * FROM r",
                    "0A000", "LIMIT in a recursive query is not implemented");
            assertFails("WITH RECURSIVE r(n) AS (SELECT n FROM r UNION ALL SELECT n+1 FROM r "
                            + "WHERE n < 3 LIMIT 5) SELECT * FROM r",
                    "0A000", "LIMIT in a recursive query is not implemented");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 2 FROM ctn_edge "
                            + "WHERE 2 NOT IN (SELECT n FROM r) LIMIT 1) SELECT * FROM r",
                    "0A000", "LIMIT in a recursive query is not implemented");
        }

        @Test
        @DisplayName("ORDER BY beats a reference on an outer join's nullable side")
        void orderByFirst() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT t.n+1 FROM "
                            + "(VALUES (1)) v LEFT JOIN r t ON true ORDER BY 1) SELECT * FROM r",
                    "0A000", "ORDER BY in a recursive query is not implemented");
        }

        @Test
        @DisplayName("OFFSET beats a reference in the non-recursive term")
        void offsetFirst() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT n FROM r UNION ALL SELECT 1 OFFSET 1) "
                            + "SELECT * FROM r",
                    "0A000", "OFFSET in a recursive query is not implemented");
        }

        @Test
        @DisplayName("with no clause above them the reference rules still speak")
        void theReferenceRulesRemain() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT t.n+1 FROM r t "
                            + "LEFT JOIN r u ON true) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear within an outer join");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r a "
                            + "WHERE n IN (SELECT n FROM r b)) SELECT * FROM r",
                    "42P19", "recursive reference to query \"r\" must not appear within a subquery");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT max(n)+1 FROM r "
                            + "WHERE n < 3) SELECT * FROM r",
                    "42P19", "aggregate functions are not allowed in a recursive query's recursive term");
        }

        @Test
        @DisplayName("the same words inside a parenthesised arm belong to that arm")
        void aParenthesisedArmKeepsItsOwnClauses() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                    + "(SELECT n+1 FROM r WHERE n < 3 ORDER BY 1)) SELECT * FROM r ORDER BY n",
                    "1", "2", "3");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                    + "(SELECT n+1 FROM r WHERE n < 3 LIMIT 5)) SELECT * FROM r ORDER BY n",
                    "1", "2", "3");
        }
    }

    // ================================================================
    // 3. A WITH item the query never reads
    // ================================================================

    @Nested
    @DisplayName("a recursive WITH item is checked whether or not it is read")
    class UnreadItems {

        @Test
        @DisplayName("an unread item of the wrong shape is refused")
        void unreadAndMalformed() {
            assertFails("WITH RECURSIVE unused_c(n) AS (SELECT 1 INTERSECT "
                            + "SELECT n+1 FROM unused_c) SELECT 5 AS m",
                    "42P19", "recursive query \"unused_c\" does not have the form "
                            + "non-recursive-term UNION [ALL] recursive-term");
            assertFails("WITH RECURSIVE unused_c(n) AS (SELECT n FROM unused_c UNION ALL "
                            + "SELECT 1) SELECT 5 AS m",
                    "42P19", "recursive reference to query \"unused_c\" "
                            + "must not appear within its non-recursive term");
        }

        @Test
        @DisplayName("an unread item that is well formed, or never recurses, is no error")
        void unreadAndWellFormed() throws Exception {
            assertRows("WITH RECURSIVE unused_c(n) AS (SELECT 1 UNION ALL "
                    + "SELECT n+1 FROM unused_c WHERE n < 3) SELECT 5 AS m", "5");
            assertRows("WITH unused_c AS (SELECT 1 INTERSECT SELECT 1) SELECT 5 AS m", "5");
            assertRows("WITH RECURSIVE unused_c(n) AS (SELECT 1) SELECT 5 AS m", "5");
            assertRows("WITH RECURSIVE unused_c(n) AS (SELECT 1 UNION ALL "
                    + "SELECT n+1 FROM unused_c WHERE n < 3) SELECT count(*)::text "
                    + "FROM ctn_edge", "4");
        }

        @Test
        @DisplayName("a refusal leaves no WITH scope standing for the next statement")
        void theScopeIsNotLeaked() throws Exception {
            assertFails("WITH RECURSIVE unused_c(n) AS (SELECT n FROM unused_c UNION ALL "
                            + "SELECT 1) SELECT 5 AS m",
                    "42P19", "must not appear within its non-recursive term");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT r.n+1 FROM r "
                    + "JOIN ctn_edge t ON t.a = r.n WHERE r.n < 3) SELECT n FROM r ORDER BY n",
                    "1", "2", "3");
        }
    }

    // ================================================================
    // 4. Parenthesised arms of a set operation
    // ================================================================

    @Nested
    @DisplayName("what may stand as an arm of a set operation")
    class ArmsOfASetOperation {

        @Test
        @DisplayName("a parenthesised set operation is one arm, not two wrappers")
        void aParenthesisedSetOperationAsAnArm() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                    + "((SELECT n+1 FROM r WHERE n < 3) EXCEPT (SELECT 99))) "
                    + "SELECT * FROM r ORDER BY n", "1", "2", "3");
            assertRows("SELECT 1 UNION ALL ((SELECT 2) UNION ALL (SELECT 3))", "1", "2", "3");
            assertRows("SELECT 1 UNION ALL (((SELECT 2) EXCEPT (SELECT 3)))", "1", "2");
            assertRows("SELECT 1 INTERSECT ((SELECT 1) UNION (SELECT 2))", "1");
        }

        @Test
        @DisplayName("plain wrapping parentheses still wrap")
        void ordinaryWrappers() throws Exception {
            assertRows("SELECT 1 UNION ALL ((SELECT 2))", "1", "2");
            assertRows("SELECT 1 UNION ALL (SELECT 2 UNION ALL SELECT 3)", "1", "2", "3");
            assertRows("(SELECT 1) UNION ALL (SELECT 2) EXCEPT (SELECT 3)", "1", "2");
            assertRows("WITH RECURSIVE r(n) AS (((SELECT 1)) UNION ALL "
                    + "((SELECT n+1 FROM r WHERE n < 3))) SELECT * FROM r ORDER BY n",
                    "1", "2", "3");
        }

        @Test
        @DisplayName("a WITH clause is an arm only inside parentheses")
        void aWithClauseIsNotAnArm() throws Exception {
            assertFails("SELECT 1 UNION ALL WITH x AS (SELECT 2) SELECT * FROM x",
                    "42601", "syntax error at or near \"WITH\"");
            assertFails("SELECT 1 INTERSECT WITH x AS (SELECT 1) SELECT * FROM x",
                    "42601", "syntax error at or near \"WITH\"");
            assertRows("SELECT 1 UNION ALL (WITH x AS (SELECT 2) SELECT * FROM x)", "1", "2");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL "
                    + "(WITH i AS (SELECT n FROM r) SELECT n+1 FROM i WHERE n < 3)) "
                    + "SELECT * FROM r ORDER BY n", "1", "2", "3");
            assertRows("WITH x AS (SELECT 1) SELECT * FROM x UNION ALL SELECT 2", "1", "2");
        }
    }

    // ================================================================
    // 5. What a CYCLE clause may mark a row with
    // ================================================================

    @Nested
    @DisplayName("the CYCLE mark is a constant of one resolved type")
    class CycleMarkValues {

        @Test
        @DisplayName("an expression is a syntax error where PostgreSQL stops reading it")
        void onlyConstants() {
            assertFails(cycle("TO random() DEFAULT 0"), "42601", "syntax error at or near \")\"");
            assertFails(cycle("TO 1+1 DEFAULT 0"), "42601", "syntax error at or near \"+\"");
            assertFails(cycle("TO -1 DEFAULT 0"), "42601", "syntax error at or near \"-\"");
            assertFails(cycle("TO 'x'::text DEFAULT 'y'"), "42601", "syntax error at or near \"::\"");
            assertFails(cycle("TO 1 DEFAULT 2*2"), "42601", "syntax error at or near \"*\"");
        }

        @Test
        @DisplayName("the mark and its default resolve to one type between them")
        void oneTypeForBoth() throws Exception {
            assertRows(cycle("TO 1 DEFAULT 0") + " ORDER BY n",
                    "1|0|integer", "2|0|integer", "3|0|integer");
            assertRows(cycle("TO 1 DEFAULT 1.5") + " ORDER BY n",
                    "1|1.5|numeric", "2|1.5|numeric", "3|1.5|numeric");
            assertRows(cycle("TO 1 DEFAULT '2'") + " ORDER BY n",
                    "1|2|integer", "2|2|integer", "3|2|integer");
            assertRows(cycle("TO 'y' DEFAULT 'n'") + " ORDER BY n",
                    "1|n|text", "2|n|text", "3|n|text");
            assertRows(cycle("TO true DEFAULT false") + " ORDER BY n",
                    "1|f|boolean", "2|f|boolean", "3|f|boolean");
            assertRows(cycle("TO DATE '2020-01-01' DEFAULT DATE '2021-01-01'") + " ORDER BY n",
                    "1|2021-01-01|date", "2|2021-01-01|date", "3|2021-01-01|date");
        }

        @Test
        @DisplayName("a string literal is read as the other value's type, or fails as one")
        void theUntypedValueTakesTheOtherType() {
            assertFails("WITH RECURSIVE r(a,b) AS (SELECT a,b FROM ctn_edge WHERE a = 1 UNION ALL "
                            + "SELECT e.a, e.b FROM ctn_edge e, r WHERE e.a = r.b) "
                            + "CYCLE a SET is_cycle TO 1 DEFAULT 'x' USING path "
                            + "SELECT a,b,is_cycle FROM r",
                    "22P02", "invalid input syntax for type integer: \"x\"");
        }

        @Test
        @DisplayName("two values with no common type are 42804")
        void noCommonType() {
            assertFails(cycle("TO true DEFAULT 1"), "42804",
                    "CYCLE types boolean and integer cannot be matched");
        }

        private String cycle(String mark) {
            return "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                    + "CYCLE n SET c " + mark + " USING p "
                    + "SELECT n, c, pg_typeof(c)::text FROM r";
        }
    }

    // ================================================================
    // 6. The columns SEARCH and CYCLE add
    // ================================================================

    @Nested
    @DisplayName("the columns SEARCH and CYCLE add")
    class GeneratedColumns {

        @Test
        @DisplayName("two of them under one name is 42601")
        void namesMayNotCollide() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                            + "SEARCH DEPTH FIRST BY n SET z CYCLE n SET z USING p SELECT n FROM r",
                    "42601", "search sequence column name and cycle mark column name are the same");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                            + "SEARCH DEPTH FIRST BY n SET z CYCLE n SET c USING z SELECT n FROM r",
                    "42601", "search sequence column name and cycle path column name are the same");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                            + "CYCLE n SET c USING c SELECT n FROM r",
                    "42601", "cycle mark column name and cycle path column name are the same");
        }

        @Test
        @DisplayName("distinct names are three more columns beside the query's own")
        void distinctNamesAreFine() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                    + "SEARCH DEPTH FIRST BY n SET o CYCLE n SET c USING p "
                    + "SELECT n, o::text, c, p::text FROM r ORDER BY n",
                    "1|{(1)}|f|{(1)}",
                    "2|{(1),(2)}|f|{(1),(2)}",
                    "3|{(1),(2),(3)}|f|{(1),(2),(3)}");
        }

        @Test
        @DisplayName("breadth first over two columns is the record (depth, p, c)")
        void breadthFirstFlattensTheSearchKey() throws Exception {
            assertRows("WITH RECURSIVE r(p,c) AS (SELECT p,c FROM ctn_tree WHERE p = 1 UNION ALL "
                    + "SELECT t.p, t.c FROM ctn_tree t, r WHERE t.p = r.c) "
                    + "SEARCH BREADTH FIRST BY p,c SET ord "
                    + "SELECT p, c, ord::text FROM r ORDER BY p, c",
                    "1|2|(0,1,2)", "1|3|(0,1,3)", "2|4|(1,2,4)", "2|5|(1,2,5)", "3|6|(1,3,6)");
        }

        @Test
        @DisplayName("depth first over two columns is one record per step of the path")
        void depthFirstIsARecordPerStep() throws Exception {
            assertRows("WITH RECURSIVE r(p,c) AS (SELECT p,c FROM ctn_tree WHERE p = 1 UNION ALL "
                    + "SELECT t.p, t.c FROM ctn_tree t, r WHERE t.p = r.c) "
                    + "SEARCH DEPTH FIRST BY p,c SET ord "
                    + "SELECT p, c, ord::text FROM r ORDER BY p, c",
                    "1|2|{\"(1,2)\"}", "1|3|{\"(1,3)\"}", "2|4|{\"(1,2)\",\"(2,4)\"}",
                    "2|5|{\"(1,2)\",\"(2,5)\"}", "3|6|{\"(1,3)\",\"(3,6)\"}");
        }

        @Test
        @DisplayName("subscripting the path array answers with its element type")
        void subscriptElementType() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                    + "CYCLE n SET c USING p "
                    + "SELECT pg_typeof(p)::text, pg_typeof(p[1])::text FROM r LIMIT 1",
                    "record[]|record");
        }
    }

    // ================================================================
    // 7. A CYCLE clause's columns count towards a UNION's duplicate removal
    // ================================================================

    @Nested
    @DisplayName("what a UNION removes as a duplicate")
    class DuplicateRemoval {

        @Test
        @DisplayName("the row that closes a cycle survives beside the one that started it")
        void theCycleRowIsNotADuplicate() throws Exception {
            assertRows("WITH RECURSIVE r(a,b) AS (SELECT a,b FROM ctn_edge WHERE a = 1 UNION "
                    + "SELECT e.a, e.b FROM ctn_edge e, r WHERE e.a = r.b) "
                    + "CYCLE a SET is_cycle USING path "
                    + "SELECT a, b, is_cycle FROM r ORDER BY a, b, is_cycle",
                    "1|2|f", "1|2|t", "2|3|f", "3|1|f", "3|4|f");
        }

        @Test
        @DisplayName("UNION ALL over the same graph answers the same way")
        void unionAllAgrees() throws Exception {
            assertRows("WITH RECURSIVE r(a,b) AS (SELECT a,b FROM ctn_edge WHERE a = 1 UNION ALL "
                    + "SELECT e.a, e.b FROM ctn_edge e, r WHERE e.a = r.b) "
                    + "CYCLE a SET is_cycle USING path "
                    + "SELECT a, b, is_cycle FROM r ORDER BY a, b, is_cycle",
                    "1|2|f", "1|2|t", "2|3|f", "3|1|f", "3|4|f");
        }

        @Test
        @DisplayName("without a CYCLE clause the removal is over the query's own columns")
        void withoutACycleClause() throws Exception {
            assertRows("WITH RECURSIVE r(a,b) AS (SELECT a,b FROM ctn_edge WHERE a = 1 UNION "
                    + "SELECT e.a, e.b FROM ctn_edge e, r WHERE e.a = r.b) "
                    + "SELECT a, b FROM r ORDER BY a, b",
                    "1|2", "2|3", "3|1", "3|4");
        }
    }

    // ================================================================
    // 8. A recursive term's column type
    // ================================================================

    @Nested
    @DisplayName("a recursive term's column type")
    class ColumnTypes {

        @Test
        @DisplayName("types with no common type at all are named")
        void unmatchedTypes() {
            assertFails("WITH RECURSIVE r(n) AS (SELECT '{}'::jsonb UNION ALL SELECT 1 FROM r "
                            + "WHERE n IS NOT NULL) SELECT * FROM r",
                    "42804", "UNION types jsonb and integer cannot be matched");
            assertFails("WITH RECURSIVE r(n) AS (SELECT true UNION ALL SELECT 1 FROM r "
                            + "WHERE n IS TRUE) SELECT * FROM r",
                    "42804", "UNION types boolean and integer cannot be matched");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT now() FROM r) "
                            + "SELECT * FROM r",
                    "42804", "UNION types integer and timestamp with time zone cannot be matched");
            assertFails("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT '{1}'::int[] FROM r) "
                            + "SELECT * FROM r",
                    "42804", "UNION types integer and integer[] cannot be matched");
        }

        @Test
        @DisplayName("a bare NULL and a bare string literal carry no type of their own")
        void untypedLiteralsAreNotRefused() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT NULL FROM r "
                    + "WHERE n < 3) SELECT * FROM r", "1", "null");
            // A seed written as a bare NULL is text by default, so a recursive term of any
            // other type widens the column and PostgreSQL says so.
            assertFails("WITH RECURSIVE r(n) AS (SELECT NULL UNION ALL SELECT 1 FROM r "
                            + "WHERE n IS NULL) SELECT * FROM r",
                    "42804", "recursive query \"r\" column 1 has type text in non-recursive term "
                            + "but type integer overall");
        }

        @Test
        @DisplayName("types of the same kind still run")
        void sameCategoryStillRuns() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1::bigint UNION ALL SELECT n+1 FROM r "
                    + "WHERE n < 3) SELECT * FROM r ORDER BY n", "1", "2", "3");
            assertRows("WITH RECURSIVE r(s) AS (SELECT 'a'::text UNION ALL SELECT s||'a' FROM r "
                    + "WHERE length(s) < 3) SELECT * FROM r ORDER BY s", "a", "aa", "aaa");
            assertRows("WITH RECURSIVE r(d) AS (SELECT DATE '2020-01-01' UNION ALL "
                    + "SELECT d+1 FROM r WHERE d < DATE '2020-01-03') SELECT * FROM r ORDER BY d",
                    "2020-01-01", "2020-01-02", "2020-01-03");
        }
    }

    // ================================================================
    // 9. Ordinary recursion, unchanged
    // ================================================================

    @Nested
    @DisplayName("ordinary recursion is left alone")
    class OrdinarySql {

        @Test
        @DisplayName("a join, a FROM subquery and a scalar sub-select in the recursive term")
        void theShapesAroundTheRules() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                    + "SELECT * FROM r ORDER BY n", "1", "2", "3");
            assertRows("WITH RECURSIVE r(a,b) AS (SELECT a,b FROM ctn_edge WHERE a = 1 UNION ALL "
                    + "SELECT e.a, e.b FROM ctn_edge e JOIN r ON e.a = r.b WHERE e.b <> 1) "
                    + "SELECT * FROM r ORDER BY a, b", "1|2", "2|3", "3|4");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r "
                    + "WHERE n < (SELECT 3)) SELECT * FROM r ORDER BY n", "1", "2", "3");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT t.n+1 FROM r t "
                    + "LEFT JOIN (VALUES (1)) v(x) ON true WHERE t.n < 3) "
                    + "SELECT * FROM r ORDER BY n", "1", "2", "3");
            assertRows("WITH RECURSIVE r(n) AS (VALUES (1) UNION ALL SELECT n+1 FROM r "
                    + "WHERE n < 3) SELECT * FROM r ORDER BY n", "1", "2", "3");
        }

        @Test
        @DisplayName("SEARCH and CYCLE over one column, as before")
        void searchAndCycleOverOneColumn() throws Exception {
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                    + "SEARCH DEPTH FIRST BY n SET ord SELECT n FROM r ORDER BY ord",
                    "1", "2", "3");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                    + "SEARCH BREADTH FIRST BY n SET ord SELECT n, ord::text FROM r ORDER BY ord",
                    "1|(0,1)", "2|(1,2)", "3|(2,3)");
            assertRows("WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 3) "
                    + "CYCLE n SET c USING p SELECT n, c FROM r ORDER BY n",
                    "1|f", "2|f", "3|f");
        }
    }
}
