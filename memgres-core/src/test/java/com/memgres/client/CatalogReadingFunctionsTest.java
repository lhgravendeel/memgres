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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The functions that read pg_catalog back out, asserted against what PostgreSQL 18 answers.
 *
 * <p>Every expected value here was measured on the reference server rather than derived: the
 * deparsed definitions were read with {@code replace(pg_get_functiondef(...), chr(10), '~')}, the
 * signatures with {@code ::regprocedure::text}, and the geometry with the {@code close_*} names
 * themselves. Named rows and named columns only -- nothing here counts catalog rows, because a
 * count is the one thing a memgres catalog and a real PostgreSQL catalog have no reason to agree
 * on and a tool never reads.
 *
 * <p>What these cover, all of it measured wrong before: pg_get_functiondef answered the empty
 * string for every OID including ones with no row behind them; the pg_get_function_* family
 * answered the empty string rather than NULL for an OID that names nothing; regprocedure and
 * to_regprocedure could not resolve a signature memgres's own pg_proc carries; and the
 * closest-point operator disagreed with PostgreSQL for two of the shapes it is written over,
 * which is why close_lseg and close_sb were not callable at all.
 */
class CatalogReadingFunctionsTest {

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

    private String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) return null;
            return rs.getString(1);
        }
    }

    /** The SQLSTATE and first message line a statement fails with, as "state|message". */
    private String failure(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            fail("expected " + sql + " to fail");
            return null;
        } catch (SQLException e) {
            String message = e.getMessage() == null ? "" : e.getMessage();
            int newline = message.indexOf('\n');
            if (newline >= 0) message = message.substring(0, newline);
            return e.getSQLState() + "|" + message;
        }
    }

    // ---- pg_get_functiondef, deparsed from the pg_proc row ----

    @Test
    void functiondefOfUpperIsPostgresText() throws SQLException {
        assertEquals("CREATE OR REPLACE FUNCTION pg_catalog.upper(text)\n RETURNS text\n"
                        + " LANGUAGE internal\n IMMUTABLE PARALLEL SAFE STRICT\n"
                        + "AS $function$upper$function$\n",
                one("SELECT pg_get_functiondef('pg_catalog.upper(text)'::regprocedure)"));
    }

    @Test
    void functiondefPrintsStableWhereTheRowSaysStable() throws SQLException {
        assertEquals("CREATE OR REPLACE FUNCTION pg_catalog.now()\n"
                        + " RETURNS timestamp with time zone\n LANGUAGE internal\n"
                        + " STABLE PARALLEL SAFE STRICT\nAS $function$now$function$\n",
                one("SELECT pg_get_functiondef('pg_catalog.now()'::regprocedure)"));
    }

    @Test
    void functiondefOfCurrentDatabaseNamesTheNameType() throws SQLException {
        assertEquals("CREATE OR REPLACE FUNCTION pg_catalog.current_database()\n"
                        + " RETURNS name\n LANGUAGE internal\n STABLE PARALLEL SAFE STRICT\n"
                        + "AS $function$current_database$function$\n",
                one("SELECT pg_get_functiondef('pg_catalog.current_database()'::regprocedure)"));
    }

    @Test
    void functiondefOfChrAndAsciiAndLower() throws SQLException {
        assertEquals("CREATE OR REPLACE FUNCTION pg_catalog.chr(integer)\n RETURNS text\n"
                        + " LANGUAGE internal\n IMMUTABLE PARALLEL SAFE STRICT\n"
                        + "AS $function$chr$function$\n",
                one("SELECT pg_get_functiondef('pg_catalog.chr(integer)'::regprocedure)"));
        assertEquals("CREATE OR REPLACE FUNCTION pg_catalog.ascii(text)\n RETURNS integer\n"
                        + " LANGUAGE internal\n IMMUTABLE PARALLEL SAFE STRICT\n"
                        + "AS $function$ascii$function$\n",
                one("SELECT pg_get_functiondef('pg_catalog.ascii(text)'::regprocedure)"));
        assertEquals("CREATE OR REPLACE FUNCTION pg_catalog.lower(text)\n RETURNS text\n"
                        + " LANGUAGE internal\n IMMUTABLE PARALLEL SAFE STRICT\n"
                        + "AS $function$lower$function$\n",
                one("SELECT pg_get_functiondef('pg_catalog.lower(text)'::regprocedure)"));
    }

    @Test
    void functiondefOfAnOidWithNoRowIsNull() throws SQLException {
        assertNull(one("SELECT pg_get_functiondef(0)"));
        assertNull(one("SELECT pg_get_functiondef(999999999)"));
    }

    @Test
    void functiondefOfAnAggregateIsRefused() {
        assertEquals("42809|ERROR: \"sum\" is an aggregate function",
                failure("SELECT pg_get_functiondef('pg_catalog.sum(integer)'::regprocedure)"));
        assertEquals("42809|ERROR: \"array_agg\" is an aggregate function",
                failure("SELECT pg_get_functiondef('pg_catalog.array_agg(anyarray)'::regprocedure)"));
    }

    // ---- pg_get_function_arguments / _result / _identity_arguments ----

    @Test
    void functionArgumentsAndResultOfLpad() throws SQLException {
        assertEquals("text, integer, text",
                one("SELECT pg_get_function_arguments('pg_catalog.lpad(text,integer,text)'::regprocedure)"));
        assertEquals("text, integer, text",
                one("SELECT pg_get_function_identity_arguments("
                        + "'pg_catalog.lpad(text,integer,text)'::regprocedure)"));
        assertEquals("text",
                one("SELECT pg_get_function_result('pg_catalog.lpad(text,integer,text)'::regprocedure)"));
    }

    @Test
    void functionResultOfASetReturningFunctionSaysSetof() throws SQLException {
        assertEquals("SETOF integer",
                one("SELECT pg_get_function_result("
                        + "'pg_catalog.generate_series(integer,integer)'::regprocedure)"));
    }

    @Test
    void functionArgumentsOfTheByteaOverloadOfMd5() throws SQLException {
        assertEquals("bytea",
                one("SELECT pg_get_function_arguments('pg_catalog.md5(bytea)'::regprocedure)"));
    }

    @Test
    void theFunctionFamilyAnswersNullForAnOidWithNoRow() throws SQLException {
        assertNull(one("SELECT pg_get_function_arguments(0)"));
        assertNull(one("SELECT pg_get_function_result(0)"));
        assertNull(one("SELECT pg_get_function_identity_arguments(0)"));
        assertNull(one("SELECT pg_get_function_arguments(99999)"));
        assertNull(one("SELECT pg_get_function_result(99999)"));
    }

    @Test
    void aJoinOverPgProcStillAnswers() throws SQLException {
        assertEquals("text", one("SELECT pg_get_function_result(p.oid) FROM pg_proc p"
                + " WHERE p.proname = 'initcap' AND p.pronamespace = 'pg_catalog'::regnamespace"));
        assertEquals("integer", one("SELECT pg_get_function_arguments(p.oid)"
                + " FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace"
                + " WHERE p.proname = 'chr' AND n.nspname = 'pg_catalog'"));
    }

    // ---- The rest of the pg_get_*def family ----

    @Test
    void theOtherDefFunctionsAnswerNullForAnOidWithNothingBehindIt() throws SQLException {
        assertNull(one("SELECT pg_get_indexdef(0)"));
        assertNull(one("SELECT pg_get_indexdef(0, 1, true)"));
        assertNull(one("SELECT pg_get_indexdef(999999999)"));
        assertNull(one("SELECT pg_get_constraintdef(0)"));
        assertNull(one("SELECT pg_get_viewdef(0)"));
        assertNull(one("SELECT pg_get_viewdef(0, true)"));
        assertNull(one("SELECT pg_get_ruledef(0)"));
        assertNull(one("SELECT pg_get_triggerdef(0)"));
    }

    @Test
    void theOtherDefFunctionsAnswerNullForANullArgument() throws SQLException {
        assertNull(one("SELECT pg_get_indexdef(NULL)"));
        assertNull(one("SELECT pg_get_constraintdef(NULL)"));
        assertNull(one("SELECT pg_get_viewdef(NULL)"));
        assertNull(one("SELECT pg_get_ruledef(NULL)"));
        assertNull(one("SELECT pg_get_triggerdef(NULL)"));
    }

    @Test
    void aRelationThatIsNeitherIndexNorViewHasNeitherDefinition() throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS b5cr_plain");
            st.execute("CREATE TABLE b5cr_plain (a integer PRIMARY KEY, b text)");
            st.execute("CREATE INDEX b5cr_idx ON b5cr_plain (b)");
        }
        try {
            assertNull(one("SELECT pg_get_indexdef('b5cr_plain'::regclass)"));
            assertNull(one("SELECT pg_get_viewdef('b5cr_plain'::regclass)"));
            // The index itself still deparses, so the NULL above is not the function giving up.
            assertEquals("CREATE INDEX b5cr_idx ON public.b5cr_plain USING btree (b)",
                    one("SELECT pg_get_indexdef('b5cr_idx'::regclass)"));
        } finally {
            try (Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS b5cr_plain");
            }
        }
    }

    // ---- regprocedure, to_regprocedure, to_regproc ----

    @Test
    void regprocedureResolvesASignatureThePgProcCarries() throws SQLException {
        assertEquals("upper(text)", one("SELECT 'pg_catalog.upper(text)'::regprocedure::text"));
        assertEquals("upper(text)", one("SELECT 'upper(text)'::regprocedure::text"));
    }

    @Test
    void regprocedureComparesArgumentTypesByOidNotBySpelling() throws SQLException {
        assertEquals("abs(integer)", one("SELECT 'pg_catalog.abs(int4)'::regprocedure::text"));
        assertEquals("upper(text)", one("SELECT to_regprocedure('pg_catalog.UPPER(TEXT)')::text"));
    }

    @Test
    void toRegprocedureAnswersNullForAnOverloadThatIsNotThere() throws SQLException {
        assertNull(one("SELECT to_regprocedure('pg_catalog.upper(character varying)')::text"));
        assertNull(one("SELECT to_regprocedure('pg_catalog.b5nosuchfn(text)')::text"));
    }

    @Test
    void regprocedureRefusesASignatureNothingAnswersTo() {
        assertEquals("42883|ERROR: function \"b5nosuchfn(text)\" does not exist",
                failure("SELECT 'b5nosuchfn(text)'::regprocedure::text"));
    }

    @Test
    void toRegprocAnswersNullForAnAmbiguousName() throws SQLException {
        assertNull(one("SELECT to_regproc('upper')::text"));
        assertNull(one("SELECT to_regproc('md5')::text"));
        assertNull(one("SELECT to_regproc('b5nosuchfn')::text"));
    }

    @Test
    void regprocRefusesAnAmbiguousName() {
        assertEquals("42725|ERROR: more than one function named \"upper\"",
                failure("SELECT 'upper'::regproc::text"));
    }

    // ---- The closest-point operator and the names behind it ----

    @Test
    void twoSegmentsThatCrossAnswerWithTheCrossing() throws SQLException {
        assertEquals("(1,1)", one("SELECT (lseg '[(0,0),(2,2)]' ## lseg '[(0,2),(2,0)]')::text"));
        assertEquals("(1,0)",
                one("SELECT close_lseg(lseg '[(0,0),(4,0)]', lseg '[(1,-1),(1,1)]')::text"));
    }

    @Test
    void twoParallelSegmentsHaveNoClosestPoint() throws SQLException {
        assertNull(one("SELECT (lseg '[(0,0),(1,1)]' ## lseg '[(2,0),(3,1)]')::text"));
        assertNull(one("SELECT (lseg '[(0,0),(1,0)]' ## lseg '[(0,1),(1,1)]')::text"));
        // Collinear and touching at an endpoint is still parallel.
        assertNull(one("SELECT (lseg '[(0,0),(2,0)]' ## lseg '[(2,0),(4,0)]')::text"));
        assertNull(one("SELECT close_lseg(lseg '[(0,0),(1,1)]', lseg '[(0,1),(1,2)]')::text"));
    }

    @Test
    void theClosestPointLiesOnTheSecondOperand() throws SQLException {
        assertEquals("(5,1)", one("SELECT (lseg '[(0,0),(1,0)]' ## lseg '[(5,1),(5,3)]')::text"));
        assertEquals("(2,1)",
                one("SELECT close_lseg(lseg '[(0,0),(1,0)]', lseg '[(2,1),(3,5)]')::text"));
    }

    @Test
    void aSegmentMeetingTheBoxAnswersWithThePointNearestTheCentre() throws SQLException {
        assertEquals("(2,1)",
                one("SELECT close_sb(lseg '[(-1,1),(5,1)]', box '(0,0),(4,2)')::text"));
        assertEquals("(2,0.5)",
                one("SELECT close_sb(lseg '[(-1,0.5),(5,0.5)]', box '(0,0),(4,2)')::text"));
        assertEquals("(1,0.5)",
                one("SELECT close_sb(lseg '[(-1,0.5),(1,0.5)]', box '(0,0),(4,2)')::text"));
        // Entirely inside the box is the same rule.
        assertEquals("(3,0.2)",
                one("SELECT close_sb(lseg '[(3,0.2),(3.5,0.3)]', box '(0,0),(4,2)')::text"));
        assertEquals("(2,1)",
                one("SELECT (lseg '[(-1,1),(5,1)]' ## box '(0,0),(4,2)')::text"));
    }

    @Test
    void aSegmentClearOfTheBoxAnswersWithAPointOnTheBox() throws SQLException {
        assertEquals("(0,1)",
                one("SELECT close_sb(lseg '[(-3,1),(-1,1)]', box '(0,0),(4,2)')::text"));
        assertEquals("(4,1)",
                one("SELECT close_sb(lseg '[(6,1),(8,1)]', box '(0,0),(4,2)')::text"));
        assertEquals("(2,0)",
                one("SELECT close_sb(lseg '[(2,-3),(2,-1)]', box '(0,0),(4,2)')::text"));
        // Two edges tie at distance one; PostgreSQL keeps the first, so the corner is (0,2).
        assertEquals("(0,2)",
                one("SELECT close_sb(lseg '[(-1,3),(5,3)]', box '(0,0),(4,2)')::text"));
        assertEquals("(4,2)",
                one("SELECT close_sb(lseg '[(6,3),(7,4)]', box '(0,0),(4,2)')::text"));
    }

    @Test
    void theBoxIsNeverTheFirstOperandOfClosestPoint() {
        assertEquals("42883|ERROR: operator does not exist: box ## lseg",
                failure("SELECT (box '(0,0),(4,2)' ## lseg '[(-1,1),(5,1)]')::text"));
    }

    @Test
    void aLineAgainstASegment() throws SQLException {
        assertEquals("(0.5,5)",
                one("SELECT close_ls(line '{0,1,-5}', lseg '[(0,0),(1,10)]')::text"));
        assertEquals("(1,1)",
                one("SELECT close_ls(line '{0,1,-5}', lseg '[(0,0),(1,1)]')::text"));
        assertEquals("(3,0)",
                one("SELECT close_ls(line '{1,-1,0}', lseg '[(3,0),(5,1)]')::text"));
        assertEquals("(2,2)",
                one("SELECT (line '{1,-1,0}' ## lseg '[(0,2),(2,2)]')::text"));
    }

    @Test
    void aLineParallelToTheSegmentHasNoClosestPointOnIt() throws SQLException {
        assertNull(one("SELECT close_ls(line '{0,1,-5}', lseg '[(0,6),(1,6)]')::text"));
        assertNull(one("SELECT close_ls(line '{0,1,-5}', lseg '[(0,4),(1,4)]')::text"));
        assertNull(one("SELECT close_ls(line '{1,-1,0}', lseg '[(0,1),(1,2)]')::text"));
    }

    @Test
    void theFourSpellingsThatAlreadyAgreedStillAgree() throws SQLException {
        assertEquals("(1,1)", one("SELECT close_ps(point '(0,0)', lseg '[(1,1),(2,2)]')::text"));
        assertEquals("(2,2)", one("SELECT close_pb(point '(3,3)', box '(0,0),(2,2)')::text"));
        assertEquals("(0,0)", one("SELECT close_pl(point '(0,0)', line '{1,-1,0}')::text"));
        assertEquals("(0,0)", one("SELECT (point '(0,0)' ## line '{1,-1,0}')::text"));
    }

    // ---- Recovery control names pg_proc listed and the executor could not dispatch ----

    @Test
    void recoveryControlFunctionsSayRecoveryIsNotInProgress() {
        assertEquals("55000|ERROR: recovery is not in progress",
                failure("SELECT pg_wal_replay_pause()"));
        assertEquals("55000|ERROR: recovery is not in progress",
                failure("SELECT pg_wal_replay_resume()"));
    }

    @Test
    void walSwitchAndRestorePointAnswerAnLsn() throws SQLException {
        assertEquals("true", one("SELECT (pg_switch_wal() IS NOT NULL)::text"));
        assertEquals("true",
                one("SELECT (pg_create_restore_point('b5_catalog_reading') IS NOT NULL)::text"));
        assertEquals("true", one("SELECT (pg_create_restore_point(NULL) IS NULL)::text"));
    }

    // ---- The categories the coercion table was missing ----

    @Test
    void theObjectIdentifierAliasesHaveATypeCategory() throws SQLException {
        // categoryOf threw IllegalStateException for regclass, regtype, regproc and xid, which is
        // an internal error reaching the client for a perfectly ordinary catalog value.
        assertEquals("pg_class", one("SELECT 'pg_class'::regclass::text"));
        assertEquals("integer", one("SELECT 'integer'::regtype::text"));
        assertTrue(Integer.parseInt(one("SELECT 'pg_class'::regclass::oid::text")) > 0);
    }
}
