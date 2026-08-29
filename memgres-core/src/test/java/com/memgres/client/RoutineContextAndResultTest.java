package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A routine says where it was, and answers with what it promised.
 *
 * <p>An error raised inside a body reaches the client with the frames the body was inside: the
 * expression or the SQL that failed, quoted as the writer wrote it, and then the line of the body
 * that expression stands on, named by the kind of statement it is. Without those the client is
 * told only that something divided by zero, somewhere.
 *
 * <p>What comes back out is read as the type the routine was declared to return, so a routine
 * returning integer that returns 4.7 returns 5 and one that returns 'notanint' fails against the
 * cast. A set is checked column by column against the shape it promised. A composite answers with
 * its fields, so a query written over it reads them as columns.
 *
 * <p>Inside, an integer FOR loop counts in the range its type has and stops where its bound does
 * rather than wrapping round to the lowest integer and running for ever; a bound that is nothing
 * is named as the bound it is. EXIT and CONTINUE are not exceptions and an enclosing handler does
 * not catch them. A handler names only conditions that are errors — the classes that are not
 * errors have labels but not exception conditions. And a declaration is a type the grammar can
 * read, with the collation it may carry and the name it may be quoted with.
 */
class RoutineContextAndResultTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        exec("CREATE TABLE zzq_t(id int, nm text)");
        exec("INSERT INTO zzq_t VALUES (1, 'a'), (2, 'b')");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = rows(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> rows(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            List<String> out = new ArrayList<>();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int c = 1; c <= rs.getMetaData().getColumnCount(); c++) {
                    if (c > 1) row.append('/');
                    row.append(rs.getString(c));
                }
                out.add(row.toString());
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

    private static String messageOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getMessage();
        }
    }

    /** The frames PostgreSQL reports for the last error, which pgjdbc carries as the server error. */
    private static String contextOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            if (e instanceof org.postgresql.util.PSQLException) {
                org.postgresql.util.ServerErrorMessage server =
                        ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
                if (server != null) return server.getWhere();
            }
            return null;
        }
    }

    private static void define(String name, String body) throws SQLException {
        exec("CREATE OR REPLACE FUNCTION " + name + " AS $$" + body + "$$");
    }

    // ---- where the error was -------------------------------------------------------------

    /** An assignment names itself, and the line it stands on. */
    @Test
    void anAssignmentNamesItselfAndItsLine() throws SQLException {
        define("zzq_assign() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE v int;\nBEGIN\n  v := 1/0;\n  RETURN v;\nEND ");
        assertEquals("PL/pgSQL assignment \"v := 1/0\"\n"
                + "PL/pgSQL function zzq_assign() line 4 at assignment",
                contextOf("SELECT zzq_assign()"));
    }

    /** A statement sent as SQL names the SQL, quoted as the body wrote it. */
    @Test
    void aStatementNamesTheSqlItSent() throws SQLException {
        define("zzq_sql() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  INSERT INTO zzq_t VALUES (1/0);\n  RETURN 1;\nEND ");
        assertEquals("SQL statement \"INSERT INTO zzq_t VALUES (1/0)\"\n"
                + "PL/pgSQL function zzq_sql() line 3 at SQL statement",
                contextOf("SELECT zzq_sql()"));
    }

    /** PERFORM runs its expression as a query, and the query is what the frame names. */
    @Test
    void performNamesTheQueryItRan() throws SQLException {
        define("zzq_perform() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  PERFORM 1/0;\n  RETURN 1;\nEND ");
        assertEquals("SQL statement \"SELECT 1/0\"\n"
                + "PL/pgSQL function zzq_perform() line 3 at PERFORM",
                contextOf("SELECT zzq_perform()"));
    }

    /** EXECUTE names the statement that ran, not the expression that produced it. */
    @Test
    void executeNamesTheStatementThatRan() throws SQLException {
        define("zzq_exec() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  EXECUTE 'SELECT 1/0';\n  RETURN 1;\nEND ");
        assertEquals("SQL statement \"SELECT 1/0\"\n"
                + "PL/pgSQL function zzq_exec() line 3 at EXECUTE",
                contextOf("SELECT zzq_exec()"));
    }

    /** Each kind of statement is named by the word PostgreSQL calls it. */
    @Test
    void eachKindOfStatementIsNamedByItsOwnWord() throws SQLException {
        define("zzq_if() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  IF 1/0 = 1 THEN RETURN 2; END IF;\n  RETURN 1;\nEND ");
        assertEquals("PL/pgSQL expression \"1/0 = 1\"\n"
                + "PL/pgSQL function zzq_if() line 3 at IF", contextOf("SELECT zzq_if()"));

        define("zzq_while() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  WHILE 1/0 = 1 LOOP NULL; END LOOP;\n  RETURN 1;\nEND ");
        assertEquals("PL/pgSQL expression \"1/0 = 1\"\n"
                + "PL/pgSQL function zzq_while() line 3 at WHILE", contextOf("SELECT zzq_while()"));

        define("zzq_forq() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE r record;\nBEGIN\n  FOR r IN SELECT 1/0 LOOP NULL; END LOOP;\n"
                        + "  RETURN 1;\nEND ");
        assertEquals("SQL statement \"SELECT 1/0\"\n"
                + "PL/pgSQL function zzq_forq() line 4 at FOR over SELECT rows",
                contextOf("SELECT zzq_forq()"));

        define("zzq_return() RETURNS int LANGUAGE plpgsql", "\nBEGIN\n  RETURN 1/0;\nEND ");
        assertEquals("PL/pgSQL expression \"1/0\"\n"
                + "PL/pgSQL function zzq_return() line 3 at RETURN",
                contextOf("SELECT zzq_return()"));

        define("zzq_rq() RETURNS SETOF int LANGUAGE plpgsql",
                "\nBEGIN\n  RETURN QUERY SELECT 1/0;\nEND ");
        assertEquals("SQL statement \"SELECT 1/0\"\n"
                + "PL/pgSQL function zzq_rq() line 3 at RETURN QUERY",
                contextOf("SELECT * FROM zzq_rq()"));

        define("zzq_rn() RETURNS SETOF int LANGUAGE plpgsql",
                "\nBEGIN\n  RETURN NEXT 1/0;\nEND ");
        assertEquals("PL/pgSQL expression \"1/0\"\n"
                + "PL/pgSQL function zzq_rn() line 3 at RETURN NEXT",
                contextOf("SELECT * FROM zzq_rn()"));

        define("zzq_case() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  CASE 1/0 WHEN 1 THEN RETURN 1; ELSE RETURN 2; END CASE;\nEND ");
        assertEquals("PL/pgSQL expression \"1/0\"\n"
                + "PL/pgSQL function zzq_case() line 3 at CASE", contextOf("SELECT zzq_case()"));

        define("zzq_open() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE c refcursor;\nBEGIN\n  OPEN c FOR SELECT 1/0;\n  RETURN 1;\nEND ");
        assertEquals("SQL statement \"SELECT 1/0\"\n"
                + "PL/pgSQL function zzq_open() line 4 at OPEN", contextOf("SELECT zzq_open()"));

        define("zzq_raise() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  RAISE EXCEPTION 'e';\nEND ");
        assertEquals("PL/pgSQL function zzq_raise() line 3 at RAISE",
                contextOf("SELECT zzq_raise()"));

        define("zzq_assert() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  ASSERT 1 = 2;\n  RETURN 1;\nEND ");
        assertEquals("PL/pgSQL function zzq_assert() line 3 at ASSERT",
                contextOf("SELECT zzq_assert()"));
    }

    /** The innermost statement to fail is the one that names itself. */
    @Test
    void theInnermostStatementNamesItself() throws SQLException {
        define("zzq_foreach() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE x int;\nBEGIN\n  FOREACH x IN ARRAY ARRAY[1,2] LOOP\n"
                        + "    x := 1/0;\n  END LOOP;\n  RETURN 1;\nEND ");
        assertEquals("PL/pgSQL assignment \"x := 1/0\"\n"
                + "PL/pgSQL function zzq_foreach() line 5 at assignment",
                contextOf("SELECT zzq_foreach()"));

        define("zzq_nested() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  BEGIN\n    RETURN 1/0;\n  END;\nEND ");
        assertEquals("PL/pgSQL expression \"1/0\"\n"
                + "PL/pgSQL function zzq_nested() line 4 at RETURN",
                contextOf("SELECT zzq_nested()"));
    }

    /** A value that will not read as the declared return type fails against the cast. */
    @Test
    void aReturnedValueIsReadAsTheDeclaredType() throws SQLException {
        define("zzq_cast() RETURNS int LANGUAGE plpgsql", "\nBEGIN\n  RETURN 'notanint';\nEND ");
        assertEquals("22P02", stateOf("SELECT zzq_cast()"));
        assertEquals("PL/pgSQL function zzq_cast() while casting return value to"
                + " function's return type", contextOf("SELECT zzq_cast()"));

        define("zzq_rounded() RETURNS int LANGUAGE plpgsql", "\nBEGIN\n  RETURN 4.7;\nEND ");
        assertEquals("5", one("SELECT zzq_rounded()"));

        define("zzq_readnum() RETURNS int LANGUAGE plpgsql", "\nBEGIN\n  RETURN '42';\nEND ");
        assertEquals("42", one("SELECT zzq_readnum()"));
        assertEquals("integer", one("SELECT pg_typeof(zzq_readnum())::text"));
    }

    /**
     * A statement that answers with rows has to say where they go, and PostgreSQL says so when
     * the statement runs rather than when the body is read. The statement never ran, so there is
     * no SQL to quote — only the line it stands on.
     */
    @Test
    void aQueryWithNoDestinationIsRefusedWhenItRuns() throws SQLException {
        define("zzq_nodest() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  SELECT 1;\n  RETURN 2;\nEND ");
        assertEquals("42601", stateOf("SELECT zzq_nodest()"));
        assertTrue(messageOf("SELECT zzq_nodest()").contains("no destination for result data"));
        assertEquals("PL/pgSQL function zzq_nodest() line 3 at SQL statement",
                contextOf("SELECT zzq_nodest()"));
    }

    // ---- a loop --------------------------------------------------------------------------

    /** A loop whose upper bound is the last integer ends there rather than wrapping round. */
    @Test
    void aLoopStopsWhereItsBoundDoes() throws SQLException {
        define("zzq_last() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE n int := 0;\nBEGIN FOR i IN 2147483645..2147483647"
                        + " LOOP n := n + 1; END LOOP; RETURN n; END ");
        assertEquals("3", one("SELECT zzq_last()"));
    }

    /** A bound too wide for the loop variable's type is out of range for it. */
    @Test
    void aBoundTooWideIsOutOfRange() throws SQLException {
        define("zzq_wide() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE n int := 0;\nBEGIN FOR i IN 1..2147483648"
                        + " LOOP n := n + 1; EXIT WHEN n > 2; END LOOP; RETURN n; END ");
        assertEquals("22003", stateOf("SELECT zzq_wide()"));
    }

    /** A bound that is nothing bounds nothing, and PostgreSQL names which of the three it was. */
    @Test
    void aBoundThatIsNothingIsNamedAsTheBoundItIs() throws SQLException {
        define("zzq_lonull() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE n int := 0; lo int := NULL;\nBEGIN FOR i IN lo..3"
                        + " LOOP n := n + 1; END LOOP; RETURN n; END ");
        assertEquals("22004", stateOf("SELECT zzq_lonull()"));
        assertTrue(messageOf("SELECT zzq_lonull()").contains("lower bound of FOR loop cannot be null"));

        define("zzq_hinull() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE n int := 0; hi int := NULL;\nBEGIN FOR i IN 1..hi"
                        + " LOOP n := n + 1; END LOOP; RETURN n; END ");
        assertTrue(messageOf("SELECT zzq_hinull()").contains("upper bound of FOR loop cannot be null"));

        define("zzq_stnull() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE n int := 0; st int := NULL;\nBEGIN FOR i IN 1..3 BY st"
                        + " LOOP n := n + 1; END LOOP; RETURN n; END ");
        assertTrue(messageOf("SELECT zzq_stnull()").contains("BY value of FOR loop cannot be null"));
    }

    /** WHEN says a condition follows; nothing following it is a condition the writer left out. */
    @Test
    void anEmptyWhenIsAMissingExpression() throws SQLException {
        String sql = "CREATE FUNCTION zzq_emptywhen() RETURNS int LANGUAGE plpgsql AS $$"
                + " BEGIN LOOP EXIT WHEN; END LOOP; RETURN 1; END $$";
        assertEquals("42601", stateOf(sql));
        assertTrue(messageOf(sql).contains("missing expression"));
        String other = "CREATE FUNCTION zzq_emptycont() RETURNS int LANGUAGE plpgsql AS $$"
                + " BEGIN LOOP CONTINUE WHEN; END LOOP; RETURN 1; END $$";
        assertEquals("42601", stateOf(other));
    }

    /** EXIT and CONTINUE are not exceptions, so an enclosing handler does not catch them. */
    @Test
    void aHandlerDoesNotCatchTheWayOutOfALoop() throws SQLException {
        define("zzq_exitcross() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE n int := 0;\nBEGIN\n  FOR i IN 1..10 LOOP\n    BEGIN\n"
                        + "      IF i = 3 THEN EXIT; END IF;\n      n := n + 1;\n"
                        + "    EXCEPTION WHEN OTHERS THEN n := -100;\n    END;\n"
                        + "  END LOOP;\n  RETURN n;\nEND ");
        assertEquals("2", one("SELECT zzq_exitcross()"));

        define("zzq_contcross() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE n int := 0;\nBEGIN\n  FOR i IN 1..5 LOOP\n    BEGIN\n"
                        + "      IF i = 3 THEN CONTINUE; END IF;\n      n := n + 1;\n"
                        + "    EXCEPTION WHEN OTHERS THEN n := -100;\n    END;\n"
                        + "  END LOOP;\n  RETURN n;\nEND ");
        assertEquals("4", one("SELECT zzq_contcross()"));

        define("zzq_retcross() RETURNS int LANGUAGE plpgsql",
                "\nBEGIN\n  BEGIN\n    RETURN 7;\n  EXCEPTION WHEN OTHERS THEN RETURN -1;\n"
                        + "  END;\nEND ");
        assertEquals("7", one("SELECT zzq_retcross()"));
    }

    // ---- a handler -----------------------------------------------------------------------

    /** A category catches every member of it, and OTHERS in an OR-list catches everything. */
    @Test
    void aHandlerCatchesByCategoryAndByName() throws SQLException {
        define("zzq_category() RETURNS text LANGUAGE plpgsql",
                "\nBEGIN\n  BEGIN\n    RAISE EXCEPTION 'x' USING ERRCODE = '22012';\n"
                        + "  EXCEPTION WHEN data_exception THEN RETURN 'category';\n  END;\nEND ");
        assertEquals("category", one("SELECT zzq_category()"));

        define("zzq_others_or() RETURNS text LANGUAGE plpgsql",
                "\nBEGIN\n  BEGIN\n    RAISE EXCEPTION 'x' USING ERRCODE = 'P0002';\n"
                        + "  EXCEPTION WHEN OTHERS OR division_by_zero THEN RETURN 'others';\n"
                        + "  END;\nEND ");
        assertEquals("others", one("SELECT zzq_others_or()"));
    }

    /**
     * Only an error is a condition a handler can catch, so the labels of the classes that are not
     * errors — the successful completion, the warnings, the no-data classes — are not conditions.
     */
    @Test
    void onlyAnErrorIsAConditionAHandlerMayName() throws SQLException {
        for (String named : new String[]{"no_data", "warning", "successful_completion",
                "privilege_not_granted", "deprecated_feature", "no_such_condition"}) {
            String sql = "CREATE FUNCTION zzq_cond() RETURNS text LANGUAGE plpgsql AS $$"
                    + " BEGIN BEGIN RETURN 'x'; EXCEPTION WHEN " + named
                    + " THEN RETURN 'y'; END; END $$";
            assertEquals("42704", stateOf(sql), named);
            assertTrue(messageOf(sql).contains("unrecognized exception condition \"" + named + "\""),
                    named);
        }
        // The error is found while the body is read, and PostgreSQL says where it was reading.
        String sql = "CREATE FUNCTION zzq_cond() RETURNS text LANGUAGE plpgsql AS $$\n"
                + "BEGIN\n  BEGIN\n    RETURN 'x';\n"
                + "  EXCEPTION WHEN no_data THEN RETURN 'y';\n  END;\nEND $$";
        assertEquals("compilation of PL/pgSQL function \"zzq_cond\" near line 5", contextOf(sql));
    }

    // ---- a declaration -------------------------------------------------------------------

    /** A length the grammar reads as an integer has to be one. */
    @Test
    void aDeclaredLengthHasToBeOneTheGrammarCanRead() throws SQLException {
        String sql = "CREATE FUNCTION zzq_widelen() RETURNS text LANGUAGE plpgsql AS $$"
                + " DECLARE v varchar(99999999999); BEGIN v := 'x'; RETURN v; END $$";
        assertEquals("42601", stateOf(sql));
        assertEquals("invalid type name \"varchar(99999999999)\"", contextOf(sql));
        assertEquals("42601", stateOf("CREATE FUNCTION zzq_intlen() RETURNS text"
                + " LANGUAGE plpgsql AS $$ DECLARE v varchar(2147483648);"
                + " BEGIN v := 'x'; RETURN v; END $$"));
    }

    /** A local is a typed store, so a value too wide for its declaration is refused. */
    @Test
    void aLocalHoldsItsValueToItsDeclaration() throws SQLException {
        define("zzq_toolong() RETURNS text LANGUAGE plpgsql",
                " DECLARE v varchar(3); BEGIN v := 'abcdef'; RETURN v; END ");
        assertEquals("22001", stateOf("SELECT zzq_toolong()"));
        define("zzq_numscale() RETURNS numeric LANGUAGE plpgsql",
                " DECLARE v numeric(4,2); BEGIN v := 1.23456; RETURN v; END ");
        assertEquals("1.23", one("SELECT zzq_numscale()"));
    }

    /** A declaration may name a collation, and a variable may be named between quotes. */
    @Test
    void aDeclarationMayNameACollationAndAQuotedName() throws SQLException {
        define("zzq_collate() RETURNS text LANGUAGE plpgsql",
                " DECLARE v text COLLATE \"C\" := 'x'; BEGIN RETURN v; END ");
        assertEquals("x", one("SELECT zzq_collate()"));
        define("zzq_quoted() RETURNS int LANGUAGE plpgsql",
                " DECLARE \"my var\" int := 3; BEGIN RETURN \"my var\"; END ");
        assertEquals("3", one("SELECT zzq_quoted()"));
    }

    /**
     * The blanks a character(n) is written out with are how the type is written, not part of the
     * value: it is as long as what was put in it, compares equal to that unpadded, and loses them
     * when it is read as text.
     */
    @Test
    void aCharacterNIsAsLongAsWhatWasPutInIt() throws SQLException {
        define("zzq_bpchar() RETURNS text LANGUAGE plpgsql",
                " DECLARE v char(5) := 'ab';"
                        + " BEGIN RETURN '[' || v || ']' || length(v) || (v = 'ab'); END ");
        assertEquals("[ab]2true", one("SELECT zzq_bpchar()"));
        define("zzq_bpret() RETURNS char(5) LANGUAGE plpgsql",
                " DECLARE v char(5) := 'ab'; BEGIN RETURN v; END ");
        assertEquals("ab   ", one("SELECT zzq_bpret()"));
        assertEquals("2", one("SELECT length(zzq_bpret())::text"));
        assertEquals("character", one("SELECT pg_typeof(zzq_bpret())::text"));
        assertEquals("[ab]", one("SELECT '[' || zzq_bpret() || ']'"));
    }

    // ---- a row and a column ----------------------------------------------------------------

    /** A row variable keeps its declared field names and takes the query's columns in order. */
    @Test
    void aRowVariableHoldsARow() throws SQLException {
        define("zzq_rowtype() RETURNS text LANGUAGE plpgsql",
                "\nDECLARE r zzq_t%ROWTYPE;\nBEGIN\n"
                        + "  SELECT * INTO r FROM zzq_t WHERE id = 1;\n  RETURN r.nm;\nEND ");
        assertEquals("a", one("SELECT zzq_rowtype()"));
        define("zzq_rowfields() RETURNS text LANGUAGE plpgsql",
                "\nDECLARE r zzq_t%ROWTYPE;\nBEGIN\n  r.id := 5; r.nm := 'z';\n"
                        + "  RETURN r.id || '/' || r.nm;\nEND ");
        assertEquals("5/z", one("SELECT zzq_rowfields()"));
        define("zzq_recfields() RETURNS text LANGUAGE plpgsql",
                "\nDECLARE r RECORD;\nBEGIN\n"
                        + "  SELECT id, nm INTO r FROM zzq_t WHERE id = 2;\n"
                        + "  RETURN r.id || '/' || r.nm;\nEND ");
        assertEquals("2/b", one("SELECT zzq_recfields()"));
        define("zzq_unset() RETURNS text LANGUAGE plpgsql",
                "\nDECLARE r zzq_t%ROWTYPE;\nBEGIN RETURN coalesce(r.nm, 'null-field'); END ");
        assertEquals("null-field", one("SELECT zzq_unset()"));
    }

    /** One scalar target takes the query's first column, however many columns the query has. */
    @Test
    void aScalarTargetTakesOneColumn() throws SQLException {
        define("zzq_scalarinto() RETURNS text LANGUAGE plpgsql",
                "\nDECLARE v text;\nBEGIN\n  SELECT id, nm INTO v FROM zzq_t WHERE id = 1;\n"
                        + "  RETURN v;\nEND ");
        assertEquals("1", one("SELECT zzq_scalarinto()"));
        define("zzq_twointo() RETURNS text LANGUAGE plpgsql",
                "\nDECLARE a int; b text;\nBEGIN\n"
                        + "  SELECT id, nm INTO a, b FROM zzq_t WHERE id = 2;\n"
                        + "  RETURN a || '/' || b;\nEND ");
        assertEquals("2/b", one("SELECT zzq_twointo()"));
    }

    /** The INTO of a statement is found in the parse tree, not by searching the text for a word. */
    @Test
    void theWordIntoInsideALiteralIsNotAnIntoClause() throws SQLException {
        define("zzq_intoword() RETURNS text LANGUAGE plpgsql",
                "\nDECLARE v text;\nBEGIN\n  SELECT ' INTO ' INTO v;\n  RETURN v;\nEND ");
        assertEquals(" INTO ", one("SELECT zzq_intoword()"));
    }

    // ---- what comes out ------------------------------------------------------------------

    /** A composite answers with its fields, and it is a type of its own. */
    @Test
    void aCompositeAnswersWithItsFields() throws SQLException {
        define("zzq_composite() RETURNS zzq_t LANGUAGE plpgsql",
                "\nDECLARE r zzq_t%ROWTYPE;\nBEGIN\n"
                        + "  SELECT * INTO r FROM zzq_t WHERE id = 1;\n  RETURN r;\nEND ");
        assertEquals(List.of("1/a"), rows("SELECT * FROM zzq_composite()"));
        assertEquals("(1,a)", one("SELECT zzq_composite()::text"));
        assertEquals("zzq_t", one("SELECT pg_typeof(zzq_composite())::text"));
    }

    /** A set named by OUT parameters is described by them, however the set was declared. */
    @Test
    void outParametersDescribeTheSetTheyName() throws SQLException {
        exec("CREATE OR REPLACE FUNCTION zzq_outrec(OUT a int, OUT b text)"
                + " RETURNS SETOF record LANGUAGE plpgsql AS $$"
                + " BEGIN a := 1; b := 'x'; RETURN NEXT; a := 2; b := 'y'; RETURN NEXT; END $$");
        assertEquals(List.of("1/x", "2/y"), rows("SELECT * FROM zzq_outrec()"));
    }

    /** A set is checked column by column against the shape the declaration promised. */
    @Test
    void aSetIsCheckedAgainstTheShapeItPromised() throws SQLException {
        define("zzq_wrongcol() RETURNS SETOF int LANGUAGE plpgsql",
                " BEGIN RETURN QUERY SELECT nm FROM zzq_t; END ");
        assertEquals("42804", stateOf("SELECT * FROM zzq_wrongcol()"));
        assertTrue(messageOf("SELECT * FROM zzq_wrongcol()")
                .contains("Returned type text does not match expected type integer"));

        define("zzq_widecol() RETURNS SETOF int LANGUAGE plpgsql",
                " BEGIN RETURN QUERY SELECT id::bigint FROM zzq_t; END ");
        assertTrue(messageOf("SELECT * FROM zzq_widecol()")
                .contains("Returned type bigint does not match expected type integer"));

        define("zzq_manycol() RETURNS SETOF int LANGUAGE plpgsql",
                " BEGIN RETURN QUERY SELECT id, id FROM zzq_t; END ");
        assertTrue(messageOf("SELECT * FROM zzq_manycol()")
                .contains("Number of returned columns (2) does not match expected column count (1)"));

        define("zzq_tablecol() RETURNS TABLE(x int, y text) LANGUAGE plpgsql",
                " BEGIN RETURN QUERY SELECT nm, nm FROM zzq_t; END ");
        assertTrue(messageOf("SELECT * FROM zzq_tablecol()")
                .contains("in column \"x\" (position 1)"));

        // And a set that does fit comes back whole.
        define("zzq_tableok() RETURNS TABLE(x int, y text) LANGUAGE plpgsql",
                " BEGIN RETURN QUERY SELECT id, nm FROM zzq_t ORDER BY id; END ");
        assertEquals(List.of("1/a", "2/b"), rows("SELECT * FROM zzq_tableok()"));
        define("zzq_setrows() RETURNS SETOF zzq_t LANGUAGE plpgsql",
                " BEGIN RETURN QUERY SELECT * FROM zzq_t ORDER BY id; END ");
        assertEquals(List.of("1/a", "2/b"), rows("SELECT * FROM zzq_setrows()"));
    }

    // ---- a cursor ------------------------------------------------------------------------

    /** OPEN … FOR EXECUTE names the query with an expression, not with a prepared statement. */
    @Test
    void aCursorMayBeOpenedOnAQueryTheBodyWorksOut() throws SQLException {
        define("zzq_openexec() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE c refcursor; n int;\nBEGIN\n"
                        + "  OPEN c FOR EXECUTE 'SELECT count(*) FROM zzq_t';\n"
                        + "  FETCH c INTO n;\n  CLOSE c;\n  RETURN n;\nEND ");
        assertEquals("2", one("SELECT zzq_openexec()"));
        define("zzq_openusing() RETURNS int LANGUAGE plpgsql",
                "\nDECLARE c refcursor; n int;\nBEGIN\n"
                        + "  OPEN c FOR EXECUTE 'SELECT count(*) FROM zzq_t WHERE id > $1' USING 1;\n"
                        + "  FETCH c INTO n;\n  CLOSE c;\n  RETURN n;\nEND ");
        assertEquals("1", one("SELECT zzq_openusing()"));
    }

    // ---- a routine that is not there -----------------------------------------------------

    /**
     * A DROP names the routine by the signature it was asked for, and there is nothing a cast
     * could change about a name written out in full — so PostgreSQL offers no advice.
     */
    @Test
    void aRoutineThatIsNotThereIsNamedBySignature() throws SQLException {
        assertEquals("function zzq_nosuch() does not exist",
                messageOf("DROP FUNCTION zzq_nosuch()").replace("ERROR: ", ""));
        assertEquals("function zzq_nosuch(integer) does not exist",
                messageOf("DROP FUNCTION zzq_nosuch(int)").replace("ERROR: ", ""));
        assertEquals("function zzq_nosuch(text, numeric) does not exist",
                messageOf("DROP FUNCTION zzq_nosuch(text, numeric)").replace("ERROR: ", ""));
        assertTrue(messageOf("DROP FUNCTION zzq_nosuch")
                .contains("could not find a function named \"zzq_nosuch\""));
        assertEquals("procedure zzq_nosuchproc() does not exist",
                messageOf("DROP PROCEDURE zzq_nosuchproc()").replace("ERROR: ", ""));
    }
}
