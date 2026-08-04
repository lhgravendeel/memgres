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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What CREATE OR REPLACE may change about a routine, and what identifies one in the first place.
 *
 * <p>CREATE OR REPLACE keeps the routine's identity, so everything a caller was compiled against
 * has to survive it: whether it is a function or a procedure, the type a call yields, the names of
 * the input parameters, and which arguments a call may leave out. Change any of those and the
 * result is a different routine wearing the old one's name, which is why PostgreSQL makes you drop
 * the old one first.
 *
 * <p>The subtlety is what "the type a call yields" means, because RETURNS is not the whole story.
 * A single output parameter <em>is</em> the result, so {@code f(IN a int) RETURNS int} and
 * {@code f(INOUT a int)} are the same function seen from outside and either may replace the other;
 * {@code RETURNS TABLE(x int)} is the same as {@code RETURNS SETOF int} for the same reason, and a
 * lone output column may be renamed because its name is not part of a scalar type. Two or more
 * output parameters make a row type, and there the column names <em>are</em> part of it, so
 * renaming one is a changed return type. A procedure yields nothing at all, so what a caller sees
 * is only whether values come back through parameters — changing that has its own message.
 *
 * <p>Every expectation here is PostgreSQL 18's measured answer, taken from the reference server:
 * the SQLSTATE and the first line of the message.
 *
 * <p>The last part is what identifies a function at all: the argument types themselves, not the
 * words they were written with. int, int4 and integer are one type and a precision is not part of
 * the type, so f(int) and f(int4) are the same function — registering both leaves no call able to
 * choose between them.
 */
class FunctionReplacementSignatureTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static void accepted(String sql) {
        try {
            exec(sql);
        } catch (SQLException e) {
            throw new AssertionError(sql + " -> " + e.getSQLState() + " " + e.getMessage(), e);
        }
    }

    private static void rejected(String sql, String sqlState, String messagePart) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql),
                "expected " + sql + " to be rejected");
        assertEquals(sqlState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                sql + " -> " + e.getMessage() + " (wanted \"" + messagePart + "\")");
    }

    // ---- a routine never changes kind ----

    @Test
    void aFunctionIsNotReplacedByAProcedure() throws Exception {
        exec("CREATE FUNCTION fsig_kind1(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE PROCEDURE fsig_kind1(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql",
                "42809", "cannot change routine kind");
        // Even where the function returns nothing, which is the closest a function comes to being
        // a procedure.
        exec("CREATE FUNCTION fsig_kind2(a int) RETURNS void AS $$ SELECT 1 $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE PROCEDURE fsig_kind2(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql",
                "42809", "cannot change routine kind");
    }

    @Test
    void aProcedureIsNotReplacedByAFunction() throws Exception {
        exec("CREATE PROCEDURE fsig_kind3(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql");
        rejected("CREATE OR REPLACE FUNCTION fsig_kind3(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
                "42809", "cannot change routine kind");
        rejected("CREATE OR REPLACE FUNCTION fsig_kind3(a int) RETURNS void AS $$ SELECT 1 $$ LANGUAGE sql",
                "42809", "cannot change routine kind");
        // The kind is decided before anything else about the definition is judged.
        exec("CREATE PROCEDURE fsig_kind4(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql");
        rejected("CREATE OR REPLACE FUNCTION fsig_kind4(b int) RETURNS text AS $$ SELECT 'x' $$ LANGUAGE sql",
                "42809", "cannot change routine kind");
        exec("CREATE PROCEDURE fsig_kind5() AS $$ BEGIN END $$ LANGUAGE plpgsql");
        rejected("CREATE OR REPLACE FUNCTION fsig_kind5() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
                "42809", "cannot change routine kind");
    }

    @Test
    void replacingAProcedureWithAProcedureIsFine() throws Exception {
        exec("CREATE PROCEDURE fsig_kind6(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql");
        accepted("CREATE OR REPLACE PROCEDURE fsig_kind6(a int) AS $$ BEGIN NULL; END $$ LANGUAGE plpgsql");
    }

    // ---- the type a call yields ----

    @Test
    void theDeclaredReturnTypeMayNotChange() throws Exception {
        exec("CREATE FUNCTION fsig_ret1(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_ret1(a int) RETURNS text AS $$ SELECT 'x' $$ LANGUAGE sql",
                "42P13", "cannot change return type of existing function");
        rejected("CREATE OR REPLACE FUNCTION fsig_ret1(a int) RETURNS SETOF int AS $$ SELECT 1 $$ LANGUAGE sql",
                "42P13", "cannot change return type of existing function");
        // A type alias is the same type, so this is a replacement and not a change.
        accepted("CREATE OR REPLACE FUNCTION fsig_ret1(a int4) RETURNS int4 AS $$ SELECT 2 $$ LANGUAGE sql");
        assertEquals("2", scalar("SELECT fsig_ret1(0)"));
        exec("CREATE FUNCTION fsig_ret2(a int) RETURNS varchar AS $$ SELECT 'x' $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_ret2(a int) RETURNS text AS $$ SELECT 'x' $$ LANGUAGE sql",
                "42P13", "cannot change return type of existing function");
    }

    @Test
    void aLoneOutputParameterIsTheResultAndMayMoveInAndOutOfRETURNS() throws Exception {
        // IN a int RETURNS int and INOUT a int are the same function from a caller's side.
        exec("CREATE FUNCTION fsig_out1(IN a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_out1(INOUT a int) AS $$ SELECT a + 10 $$ LANGUAGE sql");
        assertEquals("11", scalar("SELECT fsig_out1(1)"));
        exec("CREATE FUNCTION fsig_out2(INOUT a int) AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_out2(IN a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        // Only one of several parameters turning INOUT still leaves one output, so still int.
        exec("CREATE FUNCTION fsig_out3(IN a int, IN b int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_out3(INOUT a int, IN b int) AS $$ SELECT a $$ LANGUAGE sql");
        // An OUT parameter and a RETURNS of the same type are likewise interchangeable.
        exec("CREATE FUNCTION fsig_out4(a int, OUT r int) AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_out4(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        exec("CREATE FUNCTION fsig_out5(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_out5(a int, OUT r int) AS $$ SELECT a $$ LANGUAGE sql");
        // A scalar result has no column name, so the lone output may be renamed.
        exec("CREATE FUNCTION fsig_out6(a int, OUT r int) AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_out6(a int, OUT s int) AS $$ SELECT a + 1 $$ LANGUAGE sql");
        assertEquals("2", scalar("SELECT fsig_out6(1)"));
    }

    @Test
    void twoOutputParametersMakeARowWhoseColumnNamesAreItsType() throws Exception {
        exec("CREATE FUNCTION fsig_row1(a int, OUT r int, OUT s int) AS $$ SELECT 1, 2 $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_row1(a int, OUT r int, OUT t int) AS $$ SELECT 1, 2 $$ LANGUAGE sql",
                "42P13", "cannot change return type of existing function");
        rejected("CREATE OR REPLACE FUNCTION fsig_row1(a int, OUT r int, OUT s text) AS $$ SELECT 1, 'x' $$ LANGUAGE sql",
                "42P13", "cannot change return type of existing function");
        rejected("CREATE OR REPLACE FUNCTION fsig_row1(a int, OUT r int) AS $$ SELECT 1 $$ LANGUAGE sql",
                "42P13", "cannot change return type of existing function");
        accepted("CREATE OR REPLACE FUNCTION fsig_row1(a int, OUT r int, OUT s int) AS $$ SELECT 3, 4 $$ LANGUAGE sql");
        // Two INOUT parameters make a row as surely as two OUT ones do.
        exec("CREATE FUNCTION fsig_row2(IN a int, IN b int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_row2(INOUT a int, INOUT b int) AS $$ SELECT 1, 2 $$ LANGUAGE sql",
                "42P13", "cannot change return type of existing function");
    }

    @Test
    void aTableOfOneColumnIsASetOfThatColumnsType() throws Exception {
        exec("CREATE FUNCTION fsig_tab1(a int) RETURNS TABLE(x int) AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_tab1(a int) RETURNS SETOF int AS $$ SELECT a + 1 $$ LANGUAGE sql");
        assertEquals("2", scalar("SELECT * FROM fsig_tab1(1)"));
        exec("CREATE FUNCTION fsig_tab2(a int) RETURNS SETOF int AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_tab2(a int) RETURNS TABLE(x int) AS $$ SELECT a $$ LANGUAGE sql");
        // One column, so its name is not part of the type and may change.
        exec("CREATE FUNCTION fsig_tab3(a int) RETURNS TABLE(x int) AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_tab3(a int) RETURNS TABLE(y int) AS $$ SELECT a $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_tab3(a int) RETURNS TABLE(y text) AS $$ SELECT 'x' $$ LANGUAGE sql",
                "42P13", "cannot change return type of existing function");
        // Two columns, so the names are.
        exec("CREATE FUNCTION fsig_tab4(a int) RETURNS TABLE(x int, y int) AS $$ SELECT 1, 2 $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_tab4(a int) RETURNS TABLE(p int, q int) AS $$ SELECT 1, 2 $$ LANGUAGE sql",
                "42P13", "cannot change return type of existing function");
    }

    @Test
    void aProcedureIsJudgedOnWhetherItHandsAnythingBack() throws Exception {
        exec("CREATE PROCEDURE fsig_proc1(INOUT a int) AS $$ BEGIN END $$ LANGUAGE plpgsql");
        rejected("CREATE OR REPLACE PROCEDURE fsig_proc1(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql",
                "42P13", "cannot change whether a procedure has output parameters");
        exec("CREATE PROCEDURE fsig_proc2(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql");
        rejected("CREATE OR REPLACE PROCEDURE fsig_proc2(INOUT a int) AS $$ BEGIN END $$ LANGUAGE plpgsql",
                "42P13", "cannot change whether a procedure has output parameters");
        // A procedure returns no value, so even a single output parameter is a row whose column
        // name is part of it -- unlike a function's.
        exec("CREATE PROCEDURE fsig_proc3(a int, OUT r int) AS $$ BEGIN r := 1; END $$ LANGUAGE plpgsql");
        rejected("CREATE OR REPLACE PROCEDURE fsig_proc3(a int, OUT s int) AS $$ BEGIN s := 1; END $$ LANGUAGE plpgsql",
                "42P13", "cannot change return type of existing function");
    }

    // ---- input parameter names ----

    @Test
    void anInputParameterKeepsItsName() throws Exception {
        exec("CREATE FUNCTION fsig_name1(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_name1(b int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
                "42P13", "cannot change name of input parameter \"a\"");
        rejected("CREATE OR REPLACE FUNCTION fsig_name1(int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
                "42P13", "cannot change name of input parameter \"a\"");
        // An INOUT parameter is an input parameter, so its name is judged the same way -- and a
        // single one leaves the return type alone, so it is the name that is complained about.
        exec("CREATE FUNCTION fsig_name2(INOUT a int) AS $$ SELECT a $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_name2(INOUT b int) AS $$ SELECT b $$ LANGUAGE sql",
                "42P13", "cannot change name of input parameter \"a\"");
        // A parameter that had no name never had one to break, so naming it is allowed.
        exec("CREATE FUNCTION fsig_name3(int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_name3(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
    }

    // ---- defaults ----

    @Test
    void aDefaultMayNotBeTakenAway() throws Exception {
        exec("CREATE FUNCTION fsig_def1(a int DEFAULT 1) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_def1(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql",
                "42P13", "cannot remove parameter defaults from existing function");
        // The refused replacement changed nothing: the old function is still there and still
        // callable with the argument left out.
        assertEquals("1", scalar("SELECT fsig_def1()"));
        exec("CREATE FUNCTION fsig_def2(a int, b int DEFAULT 2) RETURNS int AS $$ SELECT b $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_def2(a int, b int) RETURNS int AS $$ SELECT b $$ LANGUAGE sql",
                "42P13", "cannot remove parameter defaults from existing function");
        exec("CREATE FUNCTION fsig_def3(a int DEFAULT 1, b int DEFAULT 2) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_def3(a int, b int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql",
                "42P13", "cannot remove parameter defaults from existing function");
        // An unnamed parameter's default counts as much as a named one's.
        exec("CREATE FUNCTION fsig_def4(int DEFAULT 1) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
        rejected("CREATE OR REPLACE FUNCTION fsig_def4(int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql",
                "42P13", "cannot remove parameter defaults from existing function");
        // So does a procedure's, and PostgreSQL says "function" either way.
        exec("CREATE PROCEDURE fsig_def5(a int DEFAULT 1) AS $$ BEGIN END $$ LANGUAGE plpgsql");
        rejected("CREATE OR REPLACE PROCEDURE fsig_def5(a int) AS $$ BEGIN END $$ LANGUAGE plpgsql",
                "42P13", "cannot remove parameter defaults from existing function");
    }

    @Test
    void addingOrChangingADefaultIsAllowed() throws Exception {
        exec("CREATE FUNCTION fsig_def6(a int) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_def6(a int DEFAULT 8) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        assertEquals("8", scalar("SELECT fsig_def6()"));
        exec("CREATE FUNCTION fsig_def7(a int DEFAULT 1) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_def7(a int DEFAULT 6) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        assertEquals("6", scalar("SELECT fsig_def7()"));
        // More defaults than before is more calls that resolve, never fewer.
        exec("CREATE FUNCTION fsig_def8(a int, b int DEFAULT 2) RETURNS int AS $$ SELECT a + b $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_def8(a int DEFAULT 1, b int DEFAULT 2) RETURNS int AS $$ SELECT a + b $$ LANGUAGE sql");
        assertEquals("3", scalar("SELECT fsig_def8()"));
    }

    @Test
    void aReplacementThatChangesTheParameterTypesIsANewFunction() throws Exception {
        exec("CREATE FUNCTION fsig_ovl(a int) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_ovl(a text) RETURNS int AS $$ SELECT 2 $$ LANGUAGE sql");
        assertEquals("1", scalar("SELECT fsig_ovl(1)"));
        assertEquals("2", scalar("SELECT fsig_ovl('x')"));
    }

    // ---- the replacements an application actually writes still work ----

    @Test
    void anOrdinaryRedefinitionStillReplacesTheBody() throws Exception {
        exec("CREATE FUNCTION fsig_body(a text, b text) RETURNS text AS $$ SELECT a || b $$ LANGUAGE sql");
        assertEquals("xy", scalar("SELECT fsig_body('x','y')"));
        accepted("CREATE OR REPLACE FUNCTION fsig_body(a text, b text) RETURNS text AS $$ SELECT b || a $$ LANGUAGE sql");
        assertEquals("yx", scalar("SELECT fsig_body('x','y')"));
        // Including one that changes language while keeping everything a caller can see.
        exec("CREATE FUNCTION fsig_lang(a int DEFAULT 2) RETURNS int AS $$ SELECT a $$ LANGUAGE sql");
        accepted("CREATE OR REPLACE FUNCTION fsig_lang(a int DEFAULT 2) RETURNS int AS $$ BEGIN RETURN a * 3; END $$ LANGUAGE plpgsql");
        assertEquals("6", scalar("SELECT fsig_lang()"));
    }
}
