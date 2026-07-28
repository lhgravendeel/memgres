package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A programmable object's definition names other objects: an aggregate's transition function, a
 * cast's function, an event trigger's function, a statistics object's columns. PostgreSQL
 * resolves each of them when the definition is written, so a definition that could never work is
 * refused at that point rather than stored and found broken on first use.
 */
class ProgrammableObjectValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        exec("CREATE FUNCTION pov_sfunc(int, int) RETURNS int LANGUAGE sql AS $$ SELECT $1 + $2 $$");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void assertFails(String expectedState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(expectedState, e.getSQLState(), sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- CREATE AGGREGATE ----

    @Test
    void anAggregateNeedsAStateFunctionAndAStateType() {
        assertFails("42P13", "aggregate sfunc must be specified",
                "CREATE AGGREGATE pov_a1(int) (STYPE = int)");
        assertFails("42P13", "aggregate stype must be specified",
                "CREATE AGGREGATE pov_a2(int) (SFUNC = pov_sfunc)");
    }

    @Test
    void theStateFunctionIsResolvedAgainstStateAndArgumentTypes() {
        assertFails("42883", "function pov_sfunc(integer, text) does not exist",
                "CREATE AGGREGATE pov_a3(text) (SFUNC = pov_sfunc, STYPE = int)");
        // An ordered-set aggregate's direct arguments never reach the transition function
        assertFails("42883", "function pov_sfunc(integer, double precision) does not exist",
                "CREATE AGGREGATE pov_a4(float8 ORDER BY float8)"
                + " (SFUNC = pov_sfunc, STYPE = int, FINALFUNC = pov_nofinal)");
        // The final function takes the state alone
        assertFails("42883", "function pov_nofinal(integer) does not exist",
                "CREATE AGGREGATE pov_a5(int) (SFUNC = pov_sfunc, STYPE = int, FINALFUNC = pov_nofinal)");
    }

    @Test
    void aWellFormedAggregateIsCreatedAndCallable() throws Exception {
        exec("CREATE AGGREGATE pov_sum(int) (SFUNC = pov_sfunc, STYPE = int, INITCOND = '0')");
        assertEquals("1", scalar("SELECT pov_sum(1)"));
        assertEquals("6", scalar("SELECT pov_sum(x) FROM (VALUES (1),(2),(3)) v(x)"));

        assertFails("42723", "already exists with same argument types",
                "CREATE AGGREGATE pov_sum(int) (SFUNC = pov_sfunc, STYPE = int)");

        // A signature that does not match must not take the aggregate down
        assertFails("42883", "aggregate pov_sum(text) does not exist", "DROP AGGREGATE pov_sum(text)");
        assertEquals("1", scalar("SELECT pov_sum(1)"));

        assertFails("42883", "aggregate pov_nosuch(integer) does not exist",
                "ALTER AGGREGATE pov_nosuch(int) RENAME TO pov_x");

        exec("DROP AGGREGATE pov_sum(int)");
    }

    // ---- CREATE OPERATOR ----

    @Test
    void anOperatorNeedsAFunctionAndASymbolicName() {
        assertFails("42P13", "operator function must be specified",
                "CREATE OPERATOR ==== (LEFTARG = int, RIGHTARG = int)");
        assertFails("42P13", "operator argument types must be specified",
                "CREATE OPERATOR ==== (FUNCTION = pov_sfunc)");
        assertFails("42601", "syntax error at or near \"(\"",
                "CREATE OPERATOR povop (LEFTARG = int, RIGHTARG = int, FUNCTION = pov_sfunc)");
        // DROP names an operator the same way, so a word is no more a name there
        assertFails("42601", "syntax error at or near \"(\"",
                "DROP OPERATOR povop (int, int)");
        assertFails("42601", "syntax error at or near \"(\"",
                "DROP OPERATOR IF EXISTS povop (int, int)");
    }

    @Test
    void anOperatorFunctionIsResolvedAgainstItsArgumentTypes() throws Exception {
        assertFails("42883", "function pov_nosuch(integer, integer) does not exist",
                "CREATE OPERATOR #== (LEFTARG = int, RIGHTARG = int, FUNCTION = pov_nosuch)");
        exec("CREATE OPERATOR #== (LEFTARG = int, RIGHTARG = int, FUNCTION = pov_sfunc)");
        exec("DROP OPERATOR #== (int, int)");
    }

    // ---- CREATE CAST ----

    @Test
    void aCastFunctionMustTakeTheSourceAndProduceTheTarget() throws Exception {
        exec("CREATE TYPE pov_ct AS (x int)");
        exec("CREATE FUNCTION pov_cf(int) RETURNS pov_ct LANGUAGE sql AS $$ SELECT ROW($1)::pov_ct $$");
        exec("CREATE CAST (int AS pov_ct) WITH FUNCTION pov_cf(int)");

        assertFails("42710", "cast from type integer to type pov_ct already exists",
                "CREATE CAST (int AS pov_ct) WITH FUNCTION pov_cf(int)");
        assertFails("42P17", "argument of cast function must match",
                "CREATE CAST (text AS pov_ct) WITH FUNCTION pov_cf(int)");
        assertFails("42P17", "return data type of cast function must match",
                "CREATE CAST (int AS int) WITH FUNCTION pov_cf(int)");
        assertFails("42883", "function pov_nosuch(bigint) does not exist",
                "CREATE CAST (bigint AS pov_ct) WITH FUNCTION pov_nosuch(bigint)");

        exec("DROP CAST (int AS pov_ct)");
        assertFails("42704", "cast from type integer to type pov_ct does not exist",
                "DROP CAST (int AS pov_ct)");
    }

    @Test
    void withoutFunctionRequiresTheSameStorage() {
        assertFails("42P17", "source and target data types are not physically compatible",
                "CREATE CAST (int AS text) WITHOUT FUNCTION");
        assertFails("42P17", "source and target data types are not physically compatible",
                "CREATE CAST (bigint AS integer) WITHOUT FUNCTION");
        assertFails("42P17", "source data type and target data type are the same",
                "CREATE CAST (int AS int) WITHOUT FUNCTION");
    }

    // ---- CREATE EVENT TRIGGER ----

    @Test
    void anEventTriggerFunctionMustReturnEventTrigger() throws Exception {
        exec("CREATE FUNCTION pov_etf() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$");
        exec("CREATE FUNCTION pov_etf2() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$");

        assertFails("42P17", "function pov_etf must return type event_trigger",
                "CREATE EVENT TRIGGER pov_et1 ON ddl_command_start EXECUTE FUNCTION pov_etf()");
        assertFails("42883", "function pov_nosuchf() does not exist",
                "CREATE EVENT TRIGGER pov_et2 ON ddl_command_start EXECUTE FUNCTION pov_nosuchf()");
    }

    @Test
    void anEventTriggerFilterIsCheckedAgainstTheCommandTags() throws Exception {
        exec("CREATE OR REPLACE FUNCTION pov_etf3() RETURNS event_trigger"
                + " LANGUAGE plpgsql AS $$ BEGIN END $$");
        assertFails("42601", "unrecognized filter variable \"nosuchvar\"",
                "CREATE EVENT TRIGGER pov_et3 ON ddl_command_start"
                + " WHEN NOSUCHVAR IN ('CREATE TABLE') EXECUTE FUNCTION pov_etf3()");
        assertFails("0A000", "event triggers are not supported for SELECT",
                "CREATE EVENT TRIGGER pov_et4 ON ddl_command_start"
                + " WHEN TAG IN ('SELECT') EXECUTE FUNCTION pov_etf3()");
        assertFails("42601", "unrecognized event name \"nosuchevent\"",
                "CREATE EVENT TRIGGER pov_et5 ON nosuchevent EXECUTE FUNCTION pov_etf3()");

        // A well-formed one is registered, and its name is then taken
        exec("CREATE EVENT TRIGGER pov_et6 ON ddl_command_end"
                + " WHEN TAG IN ('CREATE TABLE') EXECUTE FUNCTION pov_etf3()");
        assertFails("42710", "event trigger \"pov_et6\" already exists",
                "CREATE EVENT TRIGGER pov_et6 ON ddl_command_end EXECUTE FUNCTION pov_etf3()");
        exec("ALTER EVENT TRIGGER pov_et6 DISABLE");
        exec("DROP EVENT TRIGGER pov_et6");

        assertFails("42704", "event trigger \"pov_nosuch\" does not exist",
                "DROP EVENT TRIGGER pov_nosuch");
        assertFails("42704", "event trigger \"pov_nosuch\" does not exist",
                "ALTER EVENT TRIGGER pov_nosuch DISABLE");
        exec("DROP EVENT TRIGGER IF EXISTS pov_nosuch");
    }

    // ---- CREATE STATISTICS ----

    @Test
    void statisticsAreCheckedAgainstTheRelationTheyDescribe() throws Exception {
        exec("CREATE TABLE pov_t(a int, b int)");
        exec("CREATE VIEW pov_v AS SELECT * FROM pov_t");

        assertFails("42P17", "extended statistics require at least 2 columns",
                "CREATE STATISTICS pov_s1 ON a FROM pov_t");
        assertFails("42703", "column \"nosuchcol\" does not exist",
                "CREATE STATISTICS pov_s2 ON a, nosuchcol FROM pov_t");
        assertFails("42601", "unrecognized statistics kind \"nosuchkind\"",
                "CREATE STATISTICS pov_s3 (nosuchkind) ON a, b FROM pov_t");
        assertFails("42701", "duplicate column name in statistics definition",
                "CREATE STATISTICS pov_s4 ON a, a FROM pov_t");
        assertFails("42P01", "relation \"pov_nosuchtable\" does not exist",
                "CREATE STATISTICS pov_s5 ON a, b FROM pov_nosuchtable");
        assertFails("42809", "cannot define statistics for relation \"pov_v\"",
                "CREATE STATISTICS pov_s6 ON a, b FROM pov_v");

        exec("CREATE STATISTICS pov_s7 (ndistinct, dependencies, mcv) ON a, b FROM pov_t");
        assertFails("42710", "statistics object \"pov_s7\" already exists",
                "CREATE STATISTICS pov_s7 (ndistinct) ON a, b FROM pov_t");
        exec("CREATE STATISTICS IF NOT EXISTS pov_s7 ON a, b FROM pov_t");

        assertFails("42704", "statistics object \"pov_nosuch\" does not exist",
                "ALTER STATISTICS pov_nosuch RENAME TO pov_x");
        assertFails("42704", "statistics object \"pov_nosuch\" does not exist",
                "DROP STATISTICS pov_nosuch");
        exec("DROP STATISTICS IF EXISTS pov_nosuch");
        exec("ALTER STATISTICS pov_s7 RENAME TO pov_s7b");
        exec("DROP STATISTICS pov_s7b");
    }

    // ---- LANGUAGE ----

    @Test
    void aFunctionsLanguageMustExist() throws Exception {
        assertFails("42704", "language \"nosuchlang\" does not exist",
                "CREATE FUNCTION pov_f1() RETURNS int LANGUAGE nosuchlang AS $$ x $$");
        exec("CREATE FUNCTION pov_f2() RETURNS int LANGUAGE SQL AS $$ SELECT 1 $$");
        assertEquals("1", scalar("SELECT pov_f2()"));
    }

    // ---- RETURNS void ----

    @Test
    void aVoidFunctionDiscardsWhatItsBodyProduced() throws Exception {
        exec("CREATE FUNCTION pov_vf() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$");
        assertNull(scalar("SELECT pov_vf()"));
        assertEquals("t", scalar("SELECT pov_vf() IS NULL"));

        exec("CREATE FUNCTION pov_vf2() RETURNS void LANGUAGE sql AS $$ SELECT 'abc' $$");
        assertNull(scalar("SELECT pov_vf2()"));
    }

    // ---- CREATE OR REPLACE FUNCTION ----

    @Test
    void replacingMayNotChangeTheReturnTypeOrAParameterName() throws Exception {
        exec("CREATE FUNCTION pov_rf(p int) RETURNS text LANGUAGE sql AS $$ SELECT 'x' $$");

        assertFails("42P13", "cannot change return type of existing function",
                "CREATE OR REPLACE FUNCTION pov_rf(p int) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$");
        assertFails("42P13", "cannot change name of input parameter \"p\"",
                "CREATE OR REPLACE FUNCTION pov_rf(q int) RETURNS text LANGUAGE sql AS $$ SELECT 'y' $$");

        // Replacing only the body keeps working
        exec("CREATE OR REPLACE FUNCTION pov_rf(p int) RETURNS text LANGUAGE sql AS $$ SELECT 'z' $$");
        assertEquals("z", scalar("SELECT pov_rf(1)"));

        // OUT parameters name the result record's columns, so renaming one is a type change
        exec("CREATE FUNCTION pov_rf2(p int, OUT o1 int, OUT o2 int) RETURNS record"
                + " LANGUAGE sql AS $$ SELECT 1, 2 $$");
        assertFails("42P13", "cannot change return type of existing function",
                "CREATE OR REPLACE FUNCTION pov_rf2(p int, OUT o1 int, OUT ox int) RETURNS record"
                + " LANGUAGE sql AS $$ SELECT 1, 2 $$");

        // A parameter that had no name may be given one
        exec("CREATE FUNCTION pov_rf3(int) RETURNS text LANGUAGE sql AS $$ SELECT 'a' $$");
        exec("CREATE OR REPLACE FUNCTION pov_rf3(p int) RETURNS text LANGUAGE sql AS $$ SELECT 'b' $$");
        assertEquals("b", scalar("SELECT pov_rf3(1)"));

        // Overloading on a different argument type is not a replacement
        exec("CREATE FUNCTION pov_rf(p text) RETURNS int LANGUAGE sql AS $$ SELECT 7 $$");
        assertEquals("7", scalar("SELECT pov_rf('s')"));
    }

    // ---- BEGIN ATOMIC dependency tracking ----

    @Test
    void anAtomicBodyDependsOnTheTablesItNames() throws Exception {
        exec("CREATE TABLE pov_dt(a int)");
        exec("CREATE FUNCTION pov_af() RETURNS bigint LANGUAGE sql"
                + " BEGIN ATOMIC SELECT count(*) FROM pov_dt; END");
        assertEquals("0", scalar("SELECT pov_af()"));

        assertFails("2BP01", "cannot drop table pov_dt because other objects depend on it",
                "DROP TABLE pov_dt");
        // The function still works after the refused drop
        assertEquals("0", scalar("SELECT pov_af()"));

        exec("DROP TABLE pov_dt CASCADE");
        assertFails("42883", "function pov_af() does not exist", "SELECT pov_af()");
    }

    @Test
    void aStringBodyRecordsNoDependency() throws Exception {
        exec("CREATE TABLE pov_dt2(a int)");
        exec("CREATE FUNCTION pov_af2() RETURNS bigint LANGUAGE sql"
                + " AS $$ SELECT count(*) FROM pov_dt2 $$");
        exec("DROP TABLE pov_dt2");
    }
}
