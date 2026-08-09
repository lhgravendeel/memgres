package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a statement is allowed to be, and what survives the transaction it ran in.
 *
 * <p>A cursor's WHERE CURRENT OF found its row by comparing the cursor's columns against the
 * table, so a cursor whose select list carried no key updated whichever row happened to share a
 * value. SET ROLE read the word after it without allowing for TO or =, so {@code SET ROLE TO alice}
 * set the role to "to", and a role that did not exist was reported as a bad parameter value rather
 * than a missing role. LISTEN, SET SESSION AUTHORIZATION and transaction-level advisory locks were
 * changed outside the undo log, so a rolled-back transaction left all three behind. A plain SET
 * after SET LOCAL could not be read until the transaction ended. And PREPARE remembered a statement
 * without analysing it, so a missing table was reported at every EXECUTE instead of once.
 */
class StatementContextTest {

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

    private static void state(String expected, String sql) {
        assertEquals(expected, refused(sql).getSQLState(), sql);
    }

    // ---- cursors name a row, not a set of values ----

    /** The row a cursor is on is the row it is on, whatever its select list carries. */
    @Test
    void whereCurrentOfNamesTheRowTheCursorReached() throws Exception {
        exec("CREATE TABLE zz_sc_d (id int primary key, nm text)");
        try {
            exec("INSERT INTO zz_sc_d VALUES (1,'dup'),(2,'dup'),(3,'x')");
            exec("BEGIN");
            try {
                // The cursor selects nm alone and walks the rows backwards, so after two fetches
                // it is on id 2 — the second row with nm 'dup', not the first.
                exec("DECLARE zz_sc_c CURSOR FOR SELECT nm FROM zz_sc_d ORDER BY id DESC");
                exec("FETCH 1 FROM zz_sc_c");
                exec("FETCH 1 FROM zz_sc_c");
                exec("UPDATE zz_sc_d SET nm = 'HIT' WHERE CURRENT OF zz_sc_c");
                assertEquals("dup", scalar("SELECT nm FROM zz_sc_d WHERE id = 1"));
                assertEquals("HIT", scalar("SELECT nm FROM zz_sc_d WHERE id = 2"));
                // And it is still on that row, so the same update runs again.
                exec("UPDATE zz_sc_d SET nm = 'TWICE' WHERE CURRENT OF zz_sc_c");
                assertEquals("TWICE", scalar("SELECT nm FROM zz_sc_d WHERE id = 2"));
            } finally {
                exec("ROLLBACK");
            }
        } finally {
            exec("DROP TABLE zz_sc_d");
        }
    }

    /** A cursor that does not read the table cannot be used to write to it. */
    @Test
    void aCursorHasToScanTheTableItWritesTo() throws Exception {
        exec("CREATE TABLE zz_sc_t (id int, nm text)");
        try {
            exec("INSERT INTO zz_sc_t VALUES (1,'a')");
            exec("CREATE TABLE zz_sc_o (id int, nm text)");
            exec("INSERT INTO zz_sc_o VALUES (1,'a')");
            exec("BEGIN");
            try {
                exec("DECLARE zz_sc_g CURSOR FOR SELECT g FROM generate_series(1,3) g");
                exec("FETCH 1 FROM zz_sc_g");
                SQLException e = refused("UPDATE zz_sc_t SET nm='x' WHERE CURRENT OF zz_sc_g");
                assertEquals("24000", e.getSQLState());
                assertTrue(e.getMessage().contains("is not a simply updatable scan of table"),
                        e.getMessage());
            } finally {
                exec("ROLLBACK");
            }
            // A cursor that locks its rows is told it does not reach this table.
            exec("BEGIN");
            try {
                exec("DECLARE zz_sc_u CURSOR FOR SELECT id FROM zz_sc_t FOR UPDATE");
                exec("FETCH 1 FROM zz_sc_u");
                SQLException other = refused("DELETE FROM zz_sc_o WHERE CURRENT OF zz_sc_u");
                assertEquals("24000", other.getSQLState());
                assertTrue(other.getMessage()
                        .contains("does not have a FOR UPDATE/SHARE reference to table"),
                        other.getMessage());
            } finally {
                exec("ROLLBACK");
            }
        } finally {
            exec("DROP TABLE IF EXISTS zz_sc_o");
            exec("DROP TABLE zz_sc_t");
        }
    }

    /** A cursor before its first row, or past its last, is on no row at all. */
    @Test
    void aCursorHasToBeOnARow() throws Exception {
        exec("CREATE TABLE zz_sc_p (id int)");
        try {
            exec("INSERT INTO zz_sc_p VALUES (1),(2)");
            exec("BEGIN");
            try {
                exec("DECLARE zz_sc_n CURSOR FOR SELECT id FROM zz_sc_p ORDER BY id");
                SQLException before = refused("DELETE FROM zz_sc_p WHERE CURRENT OF zz_sc_n");
                assertEquals("24000", before.getSQLState());
                assertTrue(before.getMessage().contains("is not positioned on a row"));
            } finally {
                exec("ROLLBACK");
            }
            exec("BEGIN");
            try {
                exec("DECLARE zz_sc_n2 CURSOR FOR SELECT id FROM zz_sc_p ORDER BY id");
                exec("FETCH ALL FROM zz_sc_n2");
                SQLException past = refused("DELETE FROM zz_sc_p WHERE CURRENT OF zz_sc_n2");
                assertEquals("24000", past.getSQLState());
                assertTrue(past.getMessage().contains("is not positioned on a row"));
            } finally {
                exec("ROLLBACK");
            }
        } finally {
            exec("DROP TABLE zz_sc_p");
        }
    }

    // ---- roles ----

    /** SET ROLE takes an optional TO or =, and the name after it is the role. */
    @Test
    void setRoleReadsItsTargetName() throws Exception {
        exec("CREATE ROLE zz_sc_r");
        try {
            exec("SET ROLE TO zz_sc_r");
            assertEquals("zz_sc_r", scalar("SELECT current_user"));
            exec("SET ROLE NONE");
            exec("SET ROLE = zz_sc_r");
            assertEquals("zz_sc_r", scalar("SELECT current_user"));
        } finally {
            exec("SET ROLE NONE");
            exec("DROP ROLE zz_sc_r");
        }
    }

    /** A role that is not there is named as missing, not as a bad value for a parameter. */
    @Test
    void aMissingRoleIsNamedAsMissing() {
        for (String sql : new String[]{"SET ROLE zz_sc_nosuch",
                "SET SESSION AUTHORIZATION zz_sc_nosuch"}) {
            SQLException e = refused(sql);
            assertEquals("22023", e.getSQLState(), sql);
            assertTrue(e.getMessage().startsWith("ERROR: role \"zz_sc_nosuch\" does not exist"),
                    e.getMessage());
        }
        // SET ROLE has no DEFAULT spelling; RESET ROLE and SET ROLE NONE are the ways back.
        state("42601", "SET ROLE DEFAULT");
    }

    /** Who the session speaks as goes back when the transaction that changed it does. */
    @Test
    void sessionIdentityRollsBack() throws Exception {
        exec("CREATE ROLE zz_sc_a");
        try {
            String before = scalar("SELECT session_user");
            exec("BEGIN");
            exec("SET SESSION AUTHORIZATION zz_sc_a");
            assertEquals("zz_sc_a", scalar("SELECT session_user"));
            exec("ROLLBACK");
            assertEquals(before, scalar("SELECT session_user"));
        } finally {
            exec("DROP ROLE zz_sc_a");
        }
    }

    // ---- state that belongs to the transaction ----

    /** A channel subscribed to in a transaction that did not commit was never subscribed to. */
    @Test
    void listenRollsBack() throws Exception {
        assertEquals("0", scalar("SELECT count(*)::int FROM pg_listening_channels()"));
        exec("BEGIN");
        exec("LISTEN zz_sc_ch");
        assertEquals("1", scalar("SELECT count(*)::int FROM pg_listening_channels()"));
        exec("ROLLBACK");
        assertEquals("0", scalar("SELECT count(*)::int FROM pg_listening_channels()"));
    }

    /** And a transaction-level advisory lock taken after a savepoint goes with it. */
    @Test
    void advisoryLocksRollBackToTheSavepoint() throws Exception {
        exec("BEGIN");
        try {
            exec("SAVEPOINT zz_sp");
            scalar("SELECT pg_advisory_xact_lock(9060002)");
            assertEquals("1", held());
            exec("ROLLBACK TO SAVEPOINT zz_sp");
            assertEquals("0", held());
        } finally {
            exec("ROLLBACK");
        }
    }

    private static String held() throws SQLException {
        return scalar("SELECT count(*)::int FROM pg_locks"
                + " WHERE locktype = 'advisory' AND objid = 9060002");
    }

    /** A plain SET is the value for the rest of the transaction, over an earlier SET LOCAL. */
    @Test
    void aPlainSetOverridesAnEarlierLocalOne() throws Exception {
        exec("SET work_mem = '10MB'");
        exec("BEGIN");
        try {
            exec("SET LOCAL work_mem = '18MB'");
            exec("SET work_mem = '19MB'");
            assertEquals("19MB", scalar("SHOW work_mem"));
        } finally {
            exec("COMMIT");
            exec("RESET work_mem");
        }
    }

    /** And RESET puts it back now, not when the transaction ends. */
    @Test
    void resetClearsALocalValueToo() throws Exception {
        exec("SET statement_timeout = '6s'");
        exec("BEGIN");
        try {
            exec("SET LOCAL statement_timeout = '7s'");
            exec("RESET statement_timeout");
            assertEquals("0", scalar("SHOW statement_timeout"));
        } finally {
            exec("COMMIT");
            exec("RESET statement_timeout");
        }
    }

    /** A parameter the server fixed at startup refuses RESET as it refuses SET. */
    @Test
    void aFixedParameterCannotBeReset() {
        for (String sql : new String[]{"RESET block_size", "RESET max_connections",
                "RESET wal_level", "SET max_prepared_transactions = 10"}) {
            assertEquals("55P02", refused(sql).getSQLState(), sql);
        }
    }

    // ---- the search path names schemas that are there ----

    /** A path entry that names no schema is not part of the path. */
    @Test
    void theSearchPathReportsWhatExists() throws Exception {
        exec("SET search_path = zz_sc_nosuch, public");
        try {
            assertEquals("{public}", scalar("SELECT current_schemas(false)::text"));
            assertEquals("{pg_catalog,public}", scalar("SELECT current_schemas(true)::text"));
            assertEquals("public", scalar("SELECT current_schema()"));
            exec("SET search_path = zz_sc_nosuch");
            assertEquals("{}", scalar("SELECT current_schemas(false)::text"));
            assertEquals("t", scalar("SELECT current_schema() IS NULL"));
            // With no schema to create in, PostgreSQL says exactly that.
            assertEquals("3F000", refused("CREATE TABLE zz_sc_q (a int)").getSQLState());
        } finally {
            exec("RESET search_path");
        }
    }

    // ---- PREPARE is where the statement is analysed ----

    /** A relation the query names has to be there when the statement is prepared. */
    @Test
    void prepareAnalysesItsQuery() {
        SQLException missing = refused("PREPARE zz_sc_p1 (int) AS SELECT s FROM zz_sc_absent WHERE id = $1");
        assertEquals("42P01", missing.getSQLState());
        // Nothing was remembered, so executing it is executing a statement that is not there.
        assertEquals("26000", refused("EXECUTE zz_sc_p1(1)").getSQLState());
    }

    /** A declared parameter type is a type, not a size to cut the value down to. */
    @Test
    void aDeclaredParameterTypeCarriesNoModifier() throws Exception {
        exec("PREPARE zz_sc_p2 (varchar(3)) AS SELECT $1 AS v");
        try {
            assertEquals("abcdef", scalar("EXECUTE zz_sc_p2('abcdef')"));
        } finally {
            exec("DEALLOCATE zz_sc_p2");
        }
        exec("PREPARE zz_sc_p3 (numeric(2,1)) AS SELECT $1 AS v");
        try {
            assertEquals("1.26", scalar("EXECUTE zz_sc_p3(1.26)"));
        } finally {
            exec("DEALLOCATE zz_sc_p3");
        }
    }

    /** Every parameter up to the highest one written has to be one the query says something about. */
    @Test
    void everyParameterNeedsAType() {
        SQLException gap = refused("PREPARE zz_sc_p4 AS SELECT $2 AS v");
        assertEquals("42P18", gap.getSQLState());
        assertTrue(gap.getMessage().contains("could not determine data type of parameter $1"));
        assertTrue(refused("PREPARE zz_sc_p5 AS SELECT $1 AS a, $3 AS c").getMessage()
                .contains("could not determine data type of parameter $2"));
    }

    /** A parameter list has a type in it, and a declared type has to exist. */
    @Test
    void theParameterListIsWellFormed() {
        state("42601", "PREPARE zz_sc_p6 () AS SELECT 1 AS v");
        assertEquals("42704", refused("PREPARE zz_sc_p7 (zz_sc_notatype) AS SELECT $1 AS v").getSQLState());
    }

    /** The types are reported under the names a reader would write. */
    @Test
    void preparedParameterTypesAreNamed() throws Exception {
        exec("PREPARE zz_sc_p8 (int, text) AS SELECT $1 AS a, $2 AS b");
        try {
            assertEquals("{integer,text}", scalar(
                    "SELECT parameter_types::text FROM pg_prepared_statements WHERE name='zz_sc_p8'"));
            assertEquals("regtype[]", scalar(
                    "SELECT pg_typeof(parameter_types)::text FROM pg_prepared_statements"
                            + " WHERE name='zz_sc_p8'"));
        } finally {
            exec("DEALLOCATE zz_sc_p8");
        }
    }

    // ---- CALL ----

    /** A procedure is its name and the types it takes. */
    @Test
    void callResolvesBySignature() throws Exception {
        exec("CREATE PROCEDURE zz_sc_pr(a int) LANGUAGE plpgsql AS $$ BEGIN NULL; END $$");
        try {
            exec("CALL zz_sc_pr(1)");
            SQLException e = refused("CALL zz_sc_pr(1.5)");
            assertEquals("42883", e.getSQLState());
            assertTrue(e.getMessage().contains("procedure zz_sc_pr(numeric) does not exist"),
                    e.getMessage());
            assertTrue(refused("CALL zz_sc_pr(true)").getMessage()
                    .contains("procedure zz_sc_pr(boolean) does not exist"));
        } finally {
            exec("DROP PROCEDURE zz_sc_pr(int)");
        }
    }

    /** A parameter the call leaves off takes the value its declaration gives it. */
    @Test
    void callFillsInDeclaredDefaults() throws Exception {
        // An OUT parameter takes a place in the argument order, so it comes before any parameter
        // with a default: one written after a default could never be reached.
        exec("CREATE PROCEDURE zz_sc_dp(OUT c text, a int DEFAULT 1, b int DEFAULT 2)"
                + " LANGUAGE plpgsql AS $$ BEGIN c := a::text || b::text; END $$");
        try {
            assertEquals("12", scalar("CALL zz_sc_dp(NULL)"));
            assertEquals("92", scalar("CALL zz_sc_dp(NULL, 9)"));
            assertEquals("98", scalar("CALL zz_sc_dp(NULL, 9, 8)"));
        } finally {
            exec("DROP PROCEDURE zz_sc_dp(int, int)");
        }
        SQLException e = assertThrows(SQLException.class, () -> exec(
                "CREATE PROCEDURE zz_sc_bad(a int DEFAULT 1, OUT c text)"
                        + " LANGUAGE plpgsql AS $$ BEGIN c := 'x'; END $$"));
        assertEquals("42P13", e.getSQLState());
        assertTrue(e.getMessage()
                .contains("procedure OUT parameters cannot appear after one with a default value"));
    }

    /** A CALL argument is a value, not a query and not an aggregate. */
    @Test
    void callArgumentsAreValues() throws Exception {
        exec("CREATE PROCEDURE zz_sc_ar(a int) LANGUAGE plpgsql AS $$ BEGIN NULL; END $$");
        try {
            SQLException sub = refused("CALL zz_sc_ar((SELECT 3))");
            assertEquals("0A000", sub.getSQLState());
            assertTrue(sub.getMessage().contains("cannot use subquery in CALL argument"));
            SQLException agg = refused("CALL zz_sc_ar(count(*))");
            assertEquals("42803", agg.getSQLState());
            assertTrue(agg.getMessage().contains("aggregate functions are not allowed in CALL arguments"));
        } finally {
            exec("DROP PROCEDURE zz_sc_ar(int)");
        }
    }

    // ---- statements that need, or refuse, a transaction ----

    /** A routine body is a transaction block, so LOCK TABLE written in one is inside one. */
    @Test
    void lockTableWorksInsideARoutine() throws Exception {
        exec("CREATE TABLE zz_sc_l (a int)");
        try {
            exec("DO $$ BEGIN LOCK TABLE zz_sc_l IN ACCESS EXCLUSIVE MODE; END $$");
        } finally {
            exec("DROP TABLE zz_sc_l");
        }
        // Outside one it still needs a transaction of its own.
        exec("CREATE TABLE zz_sc_l2 (a int)");
        try {
            assertEquals("25P01", refused("LOCK TABLE zz_sc_l2 IN ACCESS EXCLUSIVE MODE").getSQLState());
        } finally {
            exec("DROP TABLE zz_sc_l2");
        }
    }

    /** Building an index concurrently takes transactions of its own. */
    @Test
    void concurrentIndexBuildsNeedTheirOwnTransaction() throws Exception {
        exec("CREATE TABLE zz_sc_ix (v int)");
        try {
            exec("BEGIN");
            try {
                SQLException e = refused("CREATE INDEX CONCURRENTLY zz_sc_xi ON zz_sc_ix (v)");
                assertEquals("25001", e.getSQLState());
                assertTrue(e.getMessage().contains("cannot run inside a transaction block"));
            } finally {
                exec("ROLLBACK");
            }
            exec("CREATE INDEX CONCURRENTLY zz_sc_xi ON zz_sc_ix (v)");
        } finally {
            exec("DROP TABLE zz_sc_ix");
        }
    }

    // ---- arguments a function cannot use ----

    /** A series with no step never reaches its end. */
    @Test
    void aSeriesNeedsAStep() {
        for (String sql : new String[]{"SELECT generate_series(1, 10, 0)",
                "SELECT generate_series(1::numeric, 5::numeric, 0::numeric)",
                "SELECT generate_series('2000-01-01'::timestamp, '2000-01-05'::timestamp,"
                        + " '0 days'::interval)"}) {
            SQLException e = refused(sql);
            assertEquals("22023", e.getSQLState(), sql);
            assertTrue(e.getMessage().contains("step size cannot equal zero"), sql);
        }
    }

    /** And a series is as long as it is, not as long as a limit written into the engine. */
    @Test
    void aSeriesIsNotTruncated() throws Exception {
        assertEquals("20000", scalar(
                "SELECT count(*) FROM (SELECT generate_series(1::numeric, 20000::numeric,"
                        + " 1::numeric) AS g) t"));
        assertEquals("18264", scalar(
                "SELECT count(*) FROM (SELECT generate_series('2000-01-01'::timestamp,"
                        + " '2050-01-01'::timestamp, '1 day'::interval) AS g) t"));
    }

    /** An enum with no labels has no first or last value. */
    @Test
    void anEmptyEnumHasNoBounds() throws Exception {
        exec("CREATE TYPE zz_sc_e AS ENUM ()");
        try {
            for (String fn : new String[]{"enum_first", "enum_last"}) {
                SQLException e = refused("SELECT " + fn + "(NULL::zz_sc_e)");
                assertEquals("55000", e.getSQLState(), fn);
                assertTrue(e.getMessage().contains("contains no values"), e.getMessage());
                assertFalse(e.getMessage().contains("Internal error"), e.getMessage());
            }
        } finally {
            exec("DROP TYPE zz_sc_e");
        }
    }

    /** A seed is a fraction of the whole range, and a histogram needs a range with width. */
    @Test
    void numericArgumentsAreBounded() {
        SQLException seed = refused("SELECT setseed(2)");
        assertEquals("22023", seed.getSQLState());
        assertTrue(seed.getMessage().contains("is out of allowed range [-1,1]"));
        SQLException bucket = refused("SELECT width_bucket(1,2,2,1)");
        assertEquals("2201G", bucket.getSQLState());
        assertTrue(bucket.getMessage().contains("lower bound cannot equal upper bound"));
    }

    // ---- a comment is one string constant ----

    /** COMMENT ... IS takes a string or NULL, and nothing may follow it. */
    @Test
    void aCommentIsOneStringConstant() throws Exception {
        exec("CREATE TABLE zz_sc_cm (a int)");
        try {
            exec("COMMENT ON TABLE zz_sc_cm IS 'plain'");
            assertEquals("plain", scalar("SELECT obj_description('zz_sc_cm'::regclass)"));
            exec("COMMENT ON TABLE zz_sc_cm IS $$dollar quoted$$");
            assertEquals("dollar quoted", scalar("SELECT obj_description('zz_sc_cm'::regclass)"));
            for (String sql : new String[]{"COMMENT ON TABLE zz_sc_cm IS 'a' || 'b'",
                    "COMMENT ON TABLE zz_sc_cm IS 42",
                    "COMMENT ON TABLE zz_sc_cm IS current_user"}) {
                assertEquals("42601", refused(sql).getSQLState(), sql);
            }
        } finally {
            exec("DROP TABLE zz_sc_cm");
        }
    }
}
