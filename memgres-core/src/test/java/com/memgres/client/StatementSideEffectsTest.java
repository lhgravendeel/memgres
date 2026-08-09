package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a statement does, and what it is only asked about.
 *
 * <p>Several statements did their work at the wrong moment, or did it when they were only being
 * read. PREPARE ran the query it was given so that it could learn the shape of the answer, which
 * moved sequences and wrote whatever the query's functions write. A generated column was computed
 * from the row as it was written rather than from the row the BEFORE triggers finished with, so a
 * trigger that changed the column it is generated from left a stored value that never matched it.
 * EXPLAIN of an EXECUTE described a plan for a prepared statement it had not looked for. EXIT and
 * CONTINUE were carried out as exceptions and caught by an enclosing EXCEPTION clause, so a loop
 * left from inside such a block ran the handler and went round again.
 *
 * <p>And several names were read more loosely than PostgreSQL reads them: a prepared statement or
 * a cursor named in quotes could be reached without them, a CALL could leave out the OUT
 * parameters that take a place in its argument list, and ALTER TABLE and DROP TABLE asked nothing
 * about who owns the table.
 */
class StatementSideEffectsTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
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
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    // ---- reading a statement is not running it ----

    /** PREPARE analyses the query; it does not run it, so nothing it would have moved moves. */
    @Test
    void preparingAQueryDoesNotRunIt() throws Exception {
        exec("CREATE SEQUENCE zz_se_seq");
        exec("PREPARE zz_se_p AS SELECT nextval('zz_se_seq') LIMIT 1");
        SQLException e = assertThrows(SQLException.class,
                () -> scalar("SELECT currval('zz_se_seq')"));
        assertEquals("55000", e.getSQLState());
        // The first value the sequence hands out is still its first.
        assertEquals("1", scalar("SELECT nextval('zz_se_seq')"));
        exec("DEALLOCATE zz_se_p");
    }

    /** The shape is still recorded for a query whose columns can be named without running it. */
    @Test
    void aPreparedQueryStillReportsItsResultTypes() throws Exception {
        exec("PREPARE zz_se_shape AS SELECT 1 AS a");
        String types = scalar(
                "SELECT result_types::text FROM pg_prepared_statements WHERE name = 'zz_se_shape'");
        assertNotNull(types, "a query of literals has a shape that can be read");
        assertTrue(types.contains("integer"), "expected integer in: " + types);
        exec("DEALLOCATE zz_se_shape");
    }

    // ---- a generated column reads the row that is stored ----

    /** A BEFORE trigger runs first, and the generated column is computed from what it left. */
    @Test
    void aGeneratedColumnIsComputedAfterTheBeforeTriggers() throws Exception {
        exec("CREATE TABLE zz_se_gen (a int, g int GENERATED ALWAYS AS (a * 2) STORED)");
        exec("CREATE FUNCTION zz_se_bump() RETURNS trigger LANGUAGE plpgsql"
                + " AS $$ BEGIN NEW.a := NEW.a + 10; RETURN NEW; END $$");
        exec("CREATE TRIGGER zz_se_gen_t BEFORE INSERT ON zz_se_gen"
                + " FOR EACH ROW EXECUTE FUNCTION zz_se_bump()");
        exec("INSERT INTO zz_se_gen (a) VALUES (1)");
        assertEquals("11", scalar("SELECT a FROM zz_se_gen"));
        assertEquals("22", scalar("SELECT g FROM zz_se_gen"));
    }

    // ---- EXPLAIN of an EXECUTE looks the prepared statement up ----

    @Test
    void explainingAnExecuteResolvesTheStatementItNames() throws Exception {
        exec("PREPARE zz_se_q(int) AS SELECT $1");
        try {
            SQLException missing = refused("EXPLAIN (COSTS OFF) EXECUTE zz_se_absent");
            assertEquals("26000", missing.getSQLState());
            assertTrue(missing.getMessage().contains("zz_se_absent"), missing.getMessage());

            SQLException wrongCount = refused("EXPLAIN (COSTS OFF) EXECUTE zz_se_q");
            assertEquals("42601", wrongCount.getSQLState());
            assertTrue(wrongCount.getMessage().contains("wrong number of parameters"),
                    wrongCount.getMessage());
            // The count is what the complaint is about; how many were expected is its detail.
            assertFalse(wrongCount.getMessage().contains("expected 1"), wrongCount.getMessage());
        } finally {
            exec("DEALLOCATE zz_se_q");
        }
    }

    /** An argument list is written only when there are arguments in it. */
    @Test
    void anEmptyArgumentListIsNotAWayOfWritingNone() throws Exception {
        exec("PREPARE zz_se_e AS SELECT 1");
        try {
            SQLException e = refused("EXECUTE zz_se_e()");
            assertEquals("42601", e.getSQLState());
            assertTrue(e.getMessage().contains("syntax error"), e.getMessage());
        } finally {
            exec("DEALLOCATE zz_se_e");
        }
    }

    // ---- a quoted name keeps its case ----

    @Test
    void aPreparedStatementIsNamedAsItWasWritten() throws Exception {
        exec("PREPARE \"ZzSeCase\" AS SELECT 42");
        try {
            SQLException e = refused("EXECUTE zzsecase");
            assertEquals("26000", e.getSQLState());
            assertEquals("42", scalar("EXECUTE \"ZzSeCase\""));
        } finally {
            exec("DEALLOCATE \"ZzSeCase\"");
        }
    }

    @Test
    void aCursorIsNamedAsItWasWritten() throws Exception {
        exec("BEGIN");
        try {
            exec("DECLARE \"ZzSeCur\" CURSOR FOR SELECT 7");
            // A FETCH without a direction or a FROM still names a cursor, and answers rows.
            assertEquals("7", scalar("FETCH \"ZzSeCur\""));
        } finally {
            exec("ROLLBACK");
        }
        exec("BEGIN");
        try {
            exec("DECLARE \"ZzSeCur2\" CURSOR FOR SELECT 7");
            SQLException e = refused("FETCH ALL FROM zzsecur2");
            assertEquals("34000", e.getSQLState());
        } finally {
            exec("ROLLBACK");
        }
    }

    // ---- a VALUES list has no rows to lock ----

    @Test
    void forUpdateCannotBeAppliedToValues() {
        for (String sql : new String[]{
                "VALUES (1) FOR UPDATE",
                "DECLARE zz_se_vals CURSOR FOR VALUES (1) FOR UPDATE"}) {
            SQLException e = refused(sql);
            assertEquals("0A000", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("FOR UPDATE cannot be applied to VALUES"),
                    e.getMessage());
        }
    }

    // ---- a CALL lists every parameter ----

    @Test
    void aCallListsTheOutParametersToo() throws Exception {
        exec("CREATE PROCEDURE zz_se_out(a int, OUT b int) LANGUAGE plpgsql"
                + " AS $$ BEGIN b := a * 2; END $$");
        SQLException e = refused("CALL zz_se_out(3)");
        assertEquals("42883", e.getSQLState());
        assertTrue(e.getMessage().contains("zz_se_out(integer)"), e.getMessage());
        assertEquals("6", scalar("CALL zz_se_out(3, NULL)"));
    }

    /** A VARIADIC parameter takes the whole tail, however many arguments that is. */
    @Test
    void aVariadicProcedureTakesTheArgumentsLeftOver() throws Exception {
        exec("CREATE TABLE zz_se_var (v text)");
        exec("CREATE PROCEDURE zz_se_vp(VARIADIC a int[]) LANGUAGE plpgsql"
                + " AS $$ BEGIN INSERT INTO zz_se_var VALUES (a::text); END $$");
        exec("CALL zz_se_vp(1, 2, 3)");
        assertEquals("{1,2,3}", scalar("SELECT v FROM zz_se_var"));
    }

    // ---- EXIT leaves a block; it does not fail in it ----

    @Test
    void exitIsNotCaughtByAnEnclosingExceptionClause() throws Exception {
        exec("CREATE FUNCTION zz_se_exit() RETURNS int LANGUAGE plpgsql AS $$"
                + " DECLARE n int := 0;"
                + " BEGIN FOR i IN 1..3 LOOP"
                + "   BEGIN n := n + 1; EXIT WHEN n >= 2;"
                + "   EXCEPTION WHEN OTHERS THEN n := n + 100; END;"
                + " END LOOP; RETURN n; END $$");
        assertEquals("2", scalar("SELECT zz_se_exit()"));
    }

    // ---- a table is reshaped and removed by whoever owns it ----

    @Test
    void alteringAndDroppingATableNeedsOwnership() throws Exception {
        exec("CREATE TABLE zz_se_own (id int)");
        exec("CREATE ROLE zz_se_role NOLOGIN");
        exec("SET ROLE zz_se_role");
        try {
            for (String sql : new String[]{
                    "ALTER TABLE zz_se_own ADD COLUMN z int",
                    "DROP TABLE zz_se_own"}) {
                SQLException e = refused(sql);
                assertEquals("42501", e.getSQLState(), sql);
                assertTrue(e.getMessage().contains("must be owner of table zz_se_own"),
                        e.getMessage());
            }
        } finally {
            exec("RESET ROLE");
        }
        assertEquals("1", scalar(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'zz_se_own'"));
    }

    // ---- an aborted transaction still reads what it is sent ----

    @Test
    void anAbortedTransactionReportsASyntaxErrorAsOne() throws Exception {
        exec("BEGIN");
        try {
            assertThrows(SQLException.class, () -> scalar("SELECT zz_se_nosuchthing"));
            for (String[] each : new String[][]{
                    {"SAVEPOINT select", "select"},
                    {"RELEASE SAVEPOINT ALL", "ALL"}}) {
                SQLException e = refused(each[0]);
                assertEquals("42601", e.getSQLState(), each[0]);
                assertTrue(e.getMessage().contains("syntax error at or near \"" + each[1] + "\""),
                        e.getMessage());
            }
            // A statement that reads properly is still refused for the transaction it is in.
            SQLException aborted = refused("SELECT 1");
            assertEquals("25P02", aborted.getSQLState());
        } finally {
            exec("ROLLBACK");
        }
    }

    // ---- a grantor is a role before it is a permission ----

    @Test
    void aRoleGrantAnswersForItsGrantor() throws Exception {
        exec("CREATE ROLE zz_se_a");
        exec("CREATE ROLE zz_se_b");
        SQLException missing = refused("GRANT zz_se_a TO zz_se_b GRANTED BY zz_se_nosuch");
        assertEquals("42704", missing.getSQLState());
        assertTrue(missing.getMessage().contains("zz_se_nosuch"), missing.getMessage());

        SQLException denied = refused("GRANT zz_se_a TO zz_se_b GRANTED BY zz_se_a");
        assertEquals("42501", denied.getSQLState());
        assertTrue(denied.getMessage().contains(
                "permission denied to grant privileges as role \"zz_se_a\""), denied.getMessage());
    }

    // ---- a backend's pid is an integer ----

    @Test
    void aBackendPidIsAnInteger() throws Exception {
        assertEquals("integer", scalar("SELECT pg_typeof(pg_backend_pid())::text"));
    }

    // ---- a procedural body runs inside a transaction that has already run a query ----

    @Test
    void aDoBlockCannotSetTheIsolationLevel() {
        SQLException e = refused("DO $$ BEGIN SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; END $$");
        assertEquals("25001", e.getSQLState());
        assertTrue(e.getMessage().contains("must be called before any query"), e.getMessage());
    }

    // ---- an aggregate takes the types it declares, named as PostgreSQL names them ----

    @Test
    void anAggregateNamesItsArgumentTypesAsPostgresDoes() throws Exception {
        exec("CREATE AGGREGATE zz_se_cat (text)"
                + " (SFUNC = textcat, STYPE = text, INITCOND = '')");
        SQLException e = assertThrows(SQLException.class,
                () -> scalar("SELECT zz_se_cat(v, v) FROM (VALUES ('a')) t(v)"));
        assertEquals("42883", e.getSQLState());
        assertTrue(e.getMessage().contains("zz_se_cat(text, text)"), e.getMessage());
    }
}
