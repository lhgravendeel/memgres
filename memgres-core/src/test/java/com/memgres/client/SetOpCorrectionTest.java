package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a set operation and a subquery may be written with, measured against PostgreSQL 18.
 *
 * <p><b>The ORDER BY of a set operation takes an output column, and a cast to the type that
 * column already has is still that column.</b> "Only a result column name" is the shape of what
 * PostgreSQL accepts, not the test it applies: it analyses the item and then looks for the result
 * among the output columns it already has, refusing only when the item added one. A cast to the
 * same type is a relabel PostgreSQL elides, so {@code ORDER BY a::int} over an integer {@code a}
 * sorts — while {@code a::bigint}, {@code b::varchar} over text, {@code +a}, {@code abs(a)} and
 * {@code 1::int} are all refused. Refusing every cast alike refused SQL PostgreSQL runs.
 *
 * <p><b>A name that reaches two FROM entries reaches neither.</b> {@code FROM s1.t, s2.t} is legal
 * — either can still be reached by writing its schema — so PostgreSQL admits the FROM clause and
 * then calls every bare {@code t.n} in it ambiguous. Taking the first match answered from one of
 * them; and because the parser dropped the schema from {@code s2.t.*}, that star expanded both
 * relations and a star qualified by a schema that does not hold the relation expanded it anyway.
 *
 * <p><b>Both sides of a comparison against a subquery have to be the same width</b>, and that is
 * settled from the two select lists before any row is read. Only the narrow half was checked, so
 * {@code 1 IN (SELECT 1, 2)} compared against the first column and answered true. A side whose
 * width the query text does not fix is still not judged: {@code x} in {@code FROM t x} is a whole
 * row, and {@code x IN (SELECT y FROM t y)} is a comparison PostgreSQL makes.
 *
 * <p><b>ORDER BY, LIMIT, OFFSET and FOR UPDATE belong to the set operation and not to an arm</b>,
 * so PostgreSQL's grammar has no production for one on an unparenthesised arm. Parsing the arms as
 * ordinary SELECTs and moving the clauses up afterwards accepted all four: the ORDER BY silently
 * applied to the union, and the LIMIT silently applied to the first arm alone. Parenthesised they
 * are legal and mean the arm, which is the difference the check turns on.
 *
 * <p><b>A prepared body is analysed when it is prepared</b>, not when it is executed, so a FROM
 * clause naming one relation twice and a parameter in a set operation's ORDER BY are both refused
 * at PREPARE.
 *
 * <p><b>A name may be written with four parts</b>, and PostgreSQL reaches the catalog it is
 * connected to and refuses any other as a cross-database reference. A fourth part was a syntax
 * error at the dot.
 *
 * <p>The last nested class is the point of all of it: every shape there is SQL PostgreSQL runs,
 * and each new refusal above is one more way to refuse it.
 */
class SetOpCorrectionTest {

    static Memgres memgres;
    static Connection conn;
    static String db;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE SCHEMA stc_s1");
        exec("CREATE SCHEMA stc_s2");
        exec("CREATE TABLE stc_s1.tw (n int, m int)");
        exec("CREATE TABLE stc_s2.tw (n int)");
        exec("INSERT INTO stc_s1.tw VALUES (1,11)");
        exec("INSERT INTO stc_s2.tw VALUES (2)");
        exec("CREATE TABLE stc_w1 (a int PRIMARY KEY, b text)");
        exec("INSERT INTO stc_w1 VALUES (1,'x'),(2,'y')");
        exec("CREATE TABLE stc_t1 (a int, b text)");
        exec("INSERT INTO stc_t1 VALUES (1,'x'),(2,'y'),(3,'z')");
        exec("CREATE TABLE stc_dpt (id int primary key, name text)");
        exec("INSERT INTO stc_dpt VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')");
        exec("CREATE TABLE stc_emp (id int primary key, dept_id int, name text)");
        exec("INSERT INTO stc_emp VALUES (1,1,'e'),(5,2,'f')");
        exec("CREATE TABLE stc_a (id int, v int)");
        exec("INSERT INTO stc_a VALUES (1,10)");
        exec("CREATE TABLE stc_p (id int primary key, v int)");
        exec("INSERT INTO stc_p VALUES (1,10),(2,20)");
        exec("CREATE TABLE stc_tb (c int, d text)");
        exec("CREATE TABLE stc_proj (id int primary key, code text)");
        db = one("SELECT current_database()");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- helpers ----

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            ResultSet rs = st.executeQuery(sql);
            return rs.next() ? rs.getString(1) : null;
        }
    }

    /** Column labels of {@code sql}, joined with "|". */
    private static String labels(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            ResultSet rs = st.executeQuery(sql);
            ResultSetMetaData md = rs.getMetaData();
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) sb.append('|');
                sb.append(md.getColumnLabel(i));
            }
            return sb.toString();
        }
    }

    /** Rows of {@code sql} in the order returned, each row's columns joined with "|". */
    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            ResultSet rs = st.executeQuery(sql);
            int n = rs.getMetaData().getColumnCount();
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder r = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) r.append('|');
                    Object o = rs.getObject(i);
                    r.append(o == null ? "<null>" : String.valueOf(o));
                }
                out.add(r.toString());
            }
            return out;
        }
    }

    /** Rows of {@code sql} sorted, for queries whose own order is not fixed. */
    private static List<String> sortedRows(String sql) throws SQLException {
        List<String> out = new ArrayList<>(rows(sql));
        Collections.sort(out);
        return out;
    }

    /** Runs {@code sql} expecting failure; asserts the SQLSTATE and returns the exception. */
    private static SQLException fails(String sql, String sqlState) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        return e;
    }

    private static void failsWith(String sql, String sqlState, String message) {
        SQLException e = fails(sql, sqlState);
        assertTrue(e.getMessage().contains(message),
                sql + " -> expected \"" + message + "\" in: " + e.getMessage());
    }

    private static String detailOf(SQLException e) {
        org.postgresql.util.ServerErrorMessage m =
                ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
        return m == null ? null : m.getDetail();
    }

    private static String hintOf(SQLException e) {
        org.postgresql.util.ServerErrorMessage m =
                ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
        return m == null ? null : m.getHint();
    }

    // ------------------------------------------------------------------
    @Nested
    class SetOpOrderByTakesAnOutputColumn {

        @Test
        void aCastToTheColumnsOwnTypeIsStillTheColumn() throws Exception {
            assertEquals(java.util.Arrays.asList("1", "2", "9"),
                    rows("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::int"));
            assertEquals(java.util.Arrays.asList("1", "2", "9"),
                    rows("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::int4"));
            assertEquals(java.util.Arrays.asList("1", "2", "9"),
                    rows("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY CAST(a AS int)"));
            assertEquals(java.util.Arrays.asList("1", "2", "9"),
                    rows("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::integer::int"));
            assertEquals(java.util.Arrays.asList("9", "2", "1"),
                    rows("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::int DESC"));
            assertEquals(java.util.Arrays.asList("1", "2"),
                    rows("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a::int LIMIT 2"));
            assertEquals(java.util.Arrays.asList("1", "2"),
                    rows("SELECT a FROM stc_w1 EXCEPT SELECT 9 ORDER BY a::int"));
            // text as well, and one of each in the same clause
            assertEquals(java.util.Arrays.asList("q", "x", "y"),
                    rows("SELECT b FROM stc_w1 UNION SELECT 'q' ORDER BY b::text"));
            assertEquals(java.util.Arrays.asList("9|q", "1|x", "2|y"),
                    rows("SELECT a, b FROM stc_w1 UNION SELECT 9, 'q' ORDER BY b::text, a::int"));
        }

        @Test
        void aCastToAnyOtherTypeIsAnExpressionTheClauseDoesNotTake() {
            for (String item : new String[]{"a::bigint", "a::numeric", "+a", "abs(a)", "1::int"}) {
                fails("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY " + item, "0A000");
            }
            fails("SELECT b FROM stc_w1 UNION SELECT 'q' ORDER BY b::varchar", "0A000");
            // COLLATE is a node of its own and never peels away, even over a no-op cast
            fails("SELECT b FROM stc_w1 UNION SELECT 'q' ORDER BY b::text COLLATE \"C\"", "0A000");
        }

        @Test
        void aCastOverAMissingNameIsStillTheMissingName() {
            failsWith("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY nosuchcol::int",
                    "42703", "column \"nosuchcol\" does not exist");
        }

        @Test
        void theRefusalCarriesTheDetailAndHintPostgresSends() {
            SQLException e = fails(
                    "select id from stc_dpt union select id from stc_emp order by id + 0", "0A000");
            assertEquals("Only result column names can be used, not expressions or functions.",
                    detailOf(e));
            assertEquals("Add the expression/function to every SELECT, "
                    + "or move the UNION into a FROM clause.", hintOf(e));
        }

        @Test
        void collatingAnIntegerIsStillTheCollationComplaint() {
            failsWith("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a COLLATE \"C\"",
                    "42804", "collations are not supported by type integer");
        }
    }

    // ------------------------------------------------------------------
    @Nested
    class ANameReachingTwoFromEntriesReachesNeither {

        @Test
        void aBareQualifierOverTwoRelationsOfOneNameIsAmbiguous() {
            for (String sql : new String[]{
                    "SELECT tw.n FROM stc_s1.tw, stc_s2.tw",
                    "SELECT tw.n FROM stc_s2.tw, stc_s1.tw",
                    "SELECT tw.* FROM stc_s1.tw, stc_s2.tw",
                    "SELECT 1 FROM stc_s1.tw JOIN stc_s2.tw ON tw.n = 1",
                    "CREATE VIEW stc_wv AS SELECT tw.n FROM stc_s1.tw, stc_s2.tw"}) {
                failsWith(sql, "42P09", "table reference \"tw\" is ambiguous");
            }
        }

        @Test
        void theSchemaPicksTheEntryOut() throws Exception {
            assertEquals(java.util.Arrays.asList("2"),
                    rows("SELECT stc_s2.tw.* FROM stc_s1.tw, stc_s2.tw ORDER BY 1"));
            assertEquals(java.util.Arrays.asList("1|11"),
                    rows("SELECT stc_s1.tw.* FROM stc_s1.tw, stc_s2.tw ORDER BY 1"));
            assertEquals(java.util.Arrays.asList("2"),
                    rows("SELECT stc_s2.tw.n FROM stc_s1.tw, stc_s2.tw ORDER BY 1"));
        }

        @Test
        void aSchemaThatDoesNotHoldTheRelationReachesNothing() {
            SQLException e = fails("SELECT stc_s1.tw.* FROM stc_s2.tw", "42P01");
            assertTrue(e.getMessage().contains(
                    "invalid reference to FROM-clause entry for table \"tw\""), e.getMessage());
            assertEquals("There is an entry for table \"tw\", but it cannot be referenced"
                    + " from this part of the query.", detailOf(e));

            fails("SELECT nosuchschema.stc_a.* FROM stc_a", "42P01");
        }

        @Test
        void anAliasHidesTheRelationsOwnNameFromTheStarToo() {
            SQLException e = fails("SELECT public.stc_a.* FROM stc_a a", "42P01");
            assertEquals("Perhaps you meant to reference the table alias \"a\".", hintOf(e));
        }
    }

    // ------------------------------------------------------------------
    @Nested
    class BothSidesOfASubqueryComparisonAreOneWidth {

        @Test
        void aWiderSubqueryHasNoComparisonToMake() {
            failsWith("SELECT 1 WHERE 1 IN (SELECT 1, 2)", "42601", "subquery has too many columns");
            failsWith("SELECT 1 WHERE 1 NOT IN (SELECT 1, 2)", "42601", "subquery has too many columns");
            failsWith("SELECT 1 WHERE NULL IN (SELECT 1, 2)", "42601", "subquery has too many columns");
            failsWith("select 1 = any (select id, name from stc_dpt)", "42601",
                    "subquery has too many columns");
            failsWith("select 1 = all (select id, name from stc_dpt)", "42601",
                    "subquery has too many columns");
        }

        @Test
        void aNarrowerOneIsTheSameComplaintTheOtherWayRound() {
            failsWith("SELECT 1 WHERE (1,2) IN (SELECT 1)", "42601", "subquery has too few columns");
        }

        @Test
        void aSubqueryWithNoSelectListAtAllHasNoColumn() {
            failsWith("SELECT (SELECT)", "42601", "subquery must return only one column");
            failsWith("SELECT ARRAY(SELECT)", "42601", "subquery must return only one column");
        }
    }

    // ------------------------------------------------------------------
    @Nested
    class TheTrailingClausesBelongToTheSetOperation {

        @Test
        void oneWrittenOnAnUnparenthesisedArmIsASyntaxError() {
            fails("SELECT a FROM stc_t1 ORDER BY 1 UNION SELECT 5", "42601");
            fails("SELECT a FROM stc_t1 LIMIT 1 UNION SELECT 5", "42601");
            fails("SELECT a FROM stc_t1 OFFSET 1 UNION SELECT 5", "42601");
            fails("SELECT a FROM stc_t1 ORDER BY 1 INTERSECT SELECT 1", "42601");
            fails("SELECT a FROM stc_t1 ORDER BY 1 EXCEPT SELECT 1", "42601");
            fails("SELECT id FROM stc_dpt FOR UPDATE UNION SELECT id FROM stc_emp", "42601");
        }

        @Test
        void aRowLockAfterTheWholeSetOperationIsRefusedNotDropped() {
            failsWith("select id from stc_dpt union select id from stc_emp order by 1 for update",
                    "0A000", "FOR UPDATE is not allowed with UNION/INTERSECT/EXCEPT");
            failsWith("select id from stc_dpt union select id from stc_emp for share",
                    "0A000", "FOR SHARE is not allowed with UNION/INTERSECT/EXCEPT");
        }

        @Test
        void anOperatorThatDoesNotOrderIsNotAnOrderingOperator() {
            for (String op : new String[]{"<=", ">=", "=", "<>"}) {
                SQLException e = fails(
                        "SELECT a FROM stc_t1 UNION SELECT 5 ORDER BY 1 USING " + op, "42809");
                assertTrue(e.getMessage().contains("is not a valid ordering operator"),
                        e.getMessage());
                assertEquals("Ordering operators must be \"<\" or \">\""
                        + " members of btree operator families.", hintOf(e));
            }
        }
    }

    // ------------------------------------------------------------------
    @Nested
    class APreparedBodyIsAnalysedWhenItIsPrepared {

        @Test
        void aFromClauseNamingOneRelationTwiceIsRefusedAtPrepareTime() {
            failsWith("PREPARE stc_dp AS SELECT 1 FROM stc_tb a, stc_tb a",
                    "42712", "table name \"a\" specified more than once");
        }

        @Test
        void aParameterIsNeverAnOutputColumnOfASetOperation() {
            SQLException e = fails(
                    "PREPARE stc_p2(int) AS SELECT 1 AS n UNION SELECT 2 ORDER BY $1", "0A000");
            assertEquals("Only result column names can be used, not expressions or functions.",
                    detailOf(e));
        }
    }

    // ------------------------------------------------------------------
    @Nested
    class ANameMayBeWrittenWithFourParts {

        @Test
        void theCatalogTheSessionIsConnectedToIsReached() throws Exception {
            assertEquals(java.util.Arrays.asList("1", "2", "3", "4"),
                    rows("select " + db + ".public.stc_dpt.id from public.stc_dpt order by 1"));
            assertEquals("id|name",
                    labels("select " + db + ".public.stc_dpt.* from public.stc_dpt order by 1"));
            assertEquals(java.util.Arrays.asList("1"),
                    rows("select count(*) from stc_dpt where "
                            + db + ".public.stc_dpt.id = 1"));
        }

        @Test
        void anyOtherCatalogIsACrossDatabaseReference() {
            failsWith("select nosuchdb.public.stc_dpt.id from public.stc_dpt", "0A000",
                    "cross-database references are not implemented:"
                            + " nosuchdb.public.stc_dpt.id");
            failsWith("select nosuchdb.public.stc_dpt.* from public.stc_dpt", "0A000",
                    "cross-database references are not implemented:"
                            + " nosuchdb.public.stc_dpt.*");
        }
    }

    // ------------------------------------------------------------------
    @Nested
    class MessagesThatNameWhatPostgresNames {

        @Test
        void aValueThatCannotBeCoercedNamesTheTypeTheWayPostgresDoes() {
            failsWith("SELECT 1 UNION SELECT 'abc'", "22P02",
                    "invalid input syntax for type integer: \"abc\"");
            failsWith("SELECT 1 UNION ALL SELECT 'abc'", "22P02",
                    "invalid input syntax for type integer: \"abc\"");
        }

        @Test
        void aTargetAndAFromItemOfOneNameAreGivenTwiceNotAmbiguous() {
            failsWith("update stc_proj set code = code from stc_proj where id = -1",
                    "42712", "table name \"stc_proj\" specified more than once");
            failsWith("delete from stc_proj using stc_proj where id = -1",
                    "42712", "table name \"stc_proj\" specified more than once");
            failsWith("update stc_proj p set code = code from stc_a p where p.id = -1",
                    "42712", "table name \"p\" specified more than once");
        }
    }

    // ------------------------------------------------------------------
    /**
     * Every statement here is SQL PostgreSQL 18 runs, and each was measured against it. A rule
     * that reaches one step too far turns one of these into an error, which costs more than the
     * permissiveness it removed.
     */
    @Nested
    class OrdinarySqlThatHasToKeepWorking {

        @Test
        void setOperationsWithTheirClausesWhereTheyBelong() throws Exception {
            assertEquals(java.util.Arrays.asList("1", "2", "3", "5"),
                    rows("SELECT a FROM stc_t1 UNION SELECT 5 ORDER BY 1"));
            assertEquals(java.util.Arrays.asList("1", "2"),
                    rows("SELECT a FROM stc_t1 UNION SELECT 5 ORDER BY 1 LIMIT 2"));
            assertEquals(java.util.Arrays.asList("2", "3", "5"),
                    rows("SELECT a FROM stc_t1 UNION ALL SELECT 5 ORDER BY 1 OFFSET 1"));
            assertEquals(java.util.Arrays.asList("1"),
                    rows("SELECT a FROM stc_t1 INTERSECT SELECT 1 ORDER BY 1"));
            assertEquals(java.util.Arrays.asList("2", "3"),
                    rows("SELECT a FROM stc_t1 EXCEPT SELECT 1 ORDER BY 1"));
            assertEquals(java.util.Arrays.asList("1", "2", "9"),
                    rows("SELECT a FROM stc_w1 UNION SELECT 9 ORDER BY a"));
            assertEquals(java.util.Arrays.asList("1", "2", "9"),
                    rows("SELECT a AS z FROM stc_w1 UNION SELECT 9 ORDER BY z"));
            assertEquals(java.util.Arrays.asList("1", "2", "9"),
                    rows("SELECT a FROM stc_w1 UNION ALL SELECT 9 ORDER BY (a)"));
        }

        @Test
        void parenthesisedArmsKeepTheirOwnClauses() throws Exception {
            assertEquals(2, sortedRows("(SELECT a FROM stc_t1 ORDER BY 1 LIMIT 1)"
                    + " UNION SELECT 5").size());
            assertEquals(2, sortedRows("(SELECT a FROM stc_t1 LIMIT 1) UNION (SELECT 5)").size());
            assertEquals(java.util.Arrays.asList("1", "1"),
                    rows("(SELECT id FROM stc_dpt LIMIT 1) UNION ALL"
                            + " (SELECT id FROM stc_emp LIMIT 1) ORDER BY 1"));
            assertEquals(java.util.Arrays.asList("1", "2", "9"),
                    rows("WITH q AS (SELECT a FROM stc_t1 ORDER BY 1)"
                            + " SELECT a FROM q WHERE a < 3 UNION SELECT 9 ORDER BY 1"));
            assertEquals(java.util.Arrays.asList("1", "2"),
                    rows("SELECT a FROM stc_t1 WHERE a IN"
                            + " (SELECT a FROM stc_t1 ORDER BY 1 LIMIT 2) ORDER BY 1"));
        }

        @Test
        void orderingOperatorsThatDoOrder() throws Exception {
            assertEquals(java.util.Arrays.asList("1", "2", "3"),
                    rows("SELECT a FROM stc_t1 ORDER BY a USING <"));
            assertEquals(java.util.Arrays.asList("3", "2", "1"),
                    rows("SELECT a FROM stc_t1 ORDER BY a USING >"));
            assertEquals(java.util.Arrays.asList("x", "y", "z"),
                    rows("SELECT b FROM stc_t1 ORDER BY b USING <"));
            assertEquals(java.util.Arrays.asList("1", "2", "5"),
                    rows("SELECT a FROM stc_t1 WHERE a < 3 UNION SELECT 5 ORDER BY 1 USING <"));
        }

        @Test
        void subqueryComparisonsOfEqualWidth() throws Exception {
            assertEquals(java.util.Arrays.asList("1"), rows("SELECT 1 WHERE 1 IN (SELECT 1)"));
            assertEquals(java.util.Arrays.asList("1"), rows("SELECT 1 WHERE (1,2) IN (SELECT 1, 2)"));
            assertEquals(java.util.Arrays.asList("1"), rows("SELECT 1 WHERE ROW(1,2) IN (SELECT 1, 2)"));
            assertEquals(java.util.Arrays.asList("1"), rows("SELECT 1 WHERE (SELECT 1) IN (SELECT 1)"));
            assertEquals(java.util.Arrays.asList("1"),
                    rows("SELECT 1 WHERE 1 IN (SELECT id FROM stc_dpt)"));
            assertEquals(java.util.Arrays.asList("1|10", "2|20"),
                    rows("SELECT * FROM stc_p WHERE (id,v) IN (SELECT id,v FROM stc_p) ORDER BY 1"));
            assertEquals(java.util.Arrays.asList("1"),
                    rows("select (1 = any (select id from stc_dpt))::int"));
        }

        @Test
        void aWholeRowComparedAgainstAWholeRow() throws Exception {
            // x is the whole row of stc_p, and so is y: one column each, whatever their width.
            // Counting the elements of either would make this look like a width clash.
            assertEquals(java.util.Arrays.asList("2"),
                    rows("SELECT count(*)::int FROM stc_p x WHERE x IN (SELECT y FROM stc_p y)"));
        }

        @Test
        void twoRelationsOfOneNameEachReachedByItsOwnSchema() throws Exception {
            assertEquals(java.util.Arrays.asList("1"), rows("SELECT tw.n FROM stc_s1.tw"));
            assertEquals(java.util.Arrays.asList("1"),
                    rows("SELECT tw.n FROM stc_s1.tw, stc_s2.tw x"));
            assertEquals(java.util.Arrays.asList("1|11|2"),
                    rows("SELECT * FROM stc_s1.tw, stc_s2.tw"));
            assertEquals("id|v", labels("SELECT public.stc_a.* FROM stc_a"));
            assertEquals("id|v", labels("SELECT public.stc_a.* FROM public.stc_a"));
            assertEquals("id|v", labels("SELECT stc_a.* FROM stc_a"));
        }

        @Test
        void preparedBodiesThatAnalyseCleanly() throws Exception {
            exec("PREPARE stc_ok1(int) AS SELECT $1 AS n UNION SELECT 2 ORDER BY 1");
            exec("PREPARE stc_ok2(int) AS SELECT id FROM stc_dpt WHERE id > $1 ORDER BY $1");
            exec("PREPARE stc_ok3 AS SELECT id FROM stc_dpt UNION SELECT id FROM stc_emp"
                    + " ORDER BY id");
            exec("PREPARE stc_ok4 AS SELECT 1 FROM stc_tb a, stc_tb b");
            exec("PREPARE stc_ok5 AS SELECT 1 FROM stc_tb, stc_tb b");
            exec("PREPARE stc_ok6 AS SELECT 1 FROM stc_s1.tw, stc_s2.tw x");
            exec("PREPARE stc_ok7 AS WITH q AS (SELECT 1 AS z) SELECT z FROM q");
            assertEquals(java.util.Arrays.asList("2", "3", "4"), rows("EXECUTE stc_ok2(1)"));
            exec("DEALLOCATE ALL");
        }

        @Test
        void scalarSubqueriesOfExactlyOneColumn() throws Exception {
            assertEquals(java.util.Arrays.asList("1"), rows("SELECT (SELECT 1)"));
            assertEquals(java.util.Arrays.asList("{1,2,3,4}"),
                    rows("SELECT ARRAY(SELECT id FROM stc_dpt ORDER BY 1)"));
            assertEquals(java.util.Arrays.asList("1"),
                    rows("SELECT (SELECT id FROM stc_dpt WHERE id = 1)"));
            // EXISTS asks only whether a row came back, so the subquery's width is nothing to it
            assertEquals(java.util.Arrays.asList("true"), rows("SELECT EXISTS (SELECT 1, 2)"));
        }
    }
}
