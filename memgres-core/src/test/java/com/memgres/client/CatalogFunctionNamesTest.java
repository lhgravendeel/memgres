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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What pg_proc says this server can do, measured against PostgreSQL 18.
 *
 * <p>Every expected value here was read from the reference server
 * {@code jdbc:postgresql://localhost:5432/memgrestest}, not from memgres. A pg_proc row is a claim
 * a client acts on — pgjdbc answers {@code DatabaseMetaData.getFunctions} from it, {@code
 * ::regproc} resolves through it, psql's \\df reads it — so a name listed there that PostgreSQL
 * has nowhere is worse than a missing one: the client writes a call the real server rejects.
 *
 * <p>The catalog carried seventy-five such names. Fifty-six were type I/O functions built by
 * appending {@code "_in"} to a type name, which is not how PostgreSQL spells any of them; six were
 * geometric aliases memgres invented; nine belonged to extensions that had not been created; and
 * four were one-offs. The other half of the subject is what the surviving rows say: the argument
 * types, the return type, the argument count, and whether the function returns a set.
 */
class CatalogFunctionNamesTest {

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

    // ---- The type I/O names PostgreSQL actually has ----

    /**
     * PostgreSQL does not name these to one rule, which is why deriving them went wrong: the short
     * bootstrap types and the reg* family run the suffix straight on, the longer names take an
     * underscore, and the BRIN summary types drop the {@code pg_} their type name carries.
     */
    @Test
    void theTypeIoFunctionsAreSpelledTheWayPostgresSpellsThem() {
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'char_in'"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'charin'"));
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'tid_in'"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'tidin'"));
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'xid8_in'"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'xid8in'"));
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'regrole_in'"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'regrolein'"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'regoperatorin'"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'regconfigsend'"));
        assertEquals("0", scalar(
                "SELECT count(*)::text FROM pg_proc WHERE proname = 'pg_brin_bloom_summary_in'"));
        assertEquals("1", scalar(
                "SELECT count(*)::text FROM pg_proc WHERE proname = 'brin_bloom_summary_in'"));
        assertEquals("1", scalar(
                "SELECT count(*)::text FROM pg_proc WHERE proname = 'brin_minmax_multi_summary_send'"));
        // The long-named bootstrap types do take the underscore.
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'pg_lsn_in'"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_proc WHERE proname = 'jsonpath_in'"));
        // And no reg* type has one.
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc WHERE proname LIKE 'reg%\\_in'"));
    }

    /** pg_type points at the names pg_proc carries, so following the column reaches a function. */
    @Test
    void pgTypePointsAtNamesPgProcCarries() {
        assertEquals("xid8in", scalar("SELECT typinput::text FROM pg_type WHERE typname = 'xid8'"));
        assertEquals("regroleout",
                scalar("SELECT typoutput::text FROM pg_type WHERE typname = 'regrole'"));
        assertEquals("brin_bloom_summary_recv", scalar(
                "SELECT typreceive::text FROM pg_type WHERE typname = 'pg_brin_bloom_summary'"));
        assertEquals("tidsend", scalar("SELECT typsend::text FROM pg_type WHERE typname = 'tid'"));
        assertEquals("pg_lsn_in",
                scalar("SELECT typinput::text FROM pg_type WHERE typname = 'pg_lsn'"));
        // A handler pseudo-type has no binary I/O at all.
        assertEquals("-", scalar(
                "SELECT typreceive::text FROM pg_type WHERE typname = 'index_am_handler'"));
        assertEquals("charin", scalar("SELECT 'charin'::regproc::text"));
        assertEquals("tidin", scalar("SELECT 'tidin'::regproc::text"));
    }

    /** Nothing in pg_type names a function pg_proc does not carry. */
    @Test
    void noTypeIoColumnDanglesAtAFunctionWithNoRow() {
        List<String> dangling = new ArrayList<>();
        for (String col : new String[]{"typinput", "typoutput", "typreceive", "typsend"}) {
            for (String name : column("SELECT DISTINCT t." + col + "::text FROM pg_type t"
                    + " WHERE t." + col + "::text <> '-'"
                    + " AND NOT EXISTS (SELECT 1 FROM pg_proc p WHERE p.proname = t." + col + "::text)")) {
                dangling.add(col + " -> " + name);
            }
        }
        assertEquals(List.of(), dangling, "pg_type columns naming a function pg_proc has no row for");
    }

    // ---- The names PostgreSQL has nowhere ----

    /**
     * memgres keeps its geometric aliases callable — they are its own extension and code written
     * against them still runs — but the catalog may not advertise a name PostgreSQL lacks.
     */
    @Test
    void theInventedGeometricAliasesAreNotAdvertised() {
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc WHERE proname IN"
                + " ('closest_point','intersects','is_horizontal','is_vertical',"
                + "'is_parallel','is_perpendicular')"));
        // Still callable, and still answering what they answered before.
        assertEquals("true", scalar("SELECT is_horizontal(lseg(point(0,0),point(2,0)))::text"));
        assertEquals("(1,1)", scalar(
                "SELECT closest_point(point(0,0), lseg(point(1,1),point(1,3)))::text"));
    }

    @Test
    void theFourOneOffsAreNotAdvertisedAndAreRefusedTheWayPostgresRefusesThem() {
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc WHERE proname IN"
                + " ('sha1','delete_key','pg_advisory_xact_unlock','merge_action')"));
        assertEquals("42883", stateOf("SELECT sha1('a'::bytea)"));
        assertEquals("42883", stateOf("SELECT pg_advisory_xact_unlock(1)"));
        assertEquals("42883", stateOf("SELECT delete_key('a=>1', 'a')"));
        // The neighbours of those refusals still resolve.
        assertEquals("true", scalar("SELECT (sha256('a'::bytea) IS NOT NULL)::text"));
        assertEquals("4", scalar("SELECT area(box(point(0,0),point(2,2)))::text"));
        assertEquals("true", scalar("SELECT isclosed(path '((0,0),(1,1),(2,0))')::text"));
        assertEquals("OK", stateOf("SELECT pg_advisory_unlock(9987)"));
    }

    /**
     * An extension's functions live in the schema the extension was installed into, and nowhere at
     * all before CREATE EXTENSION runs — which is also when memgres starts dispatching them, so
     * the catalog and the executor now agree on both sides of the line.
     */
    @Test
    void extensionFunctionsAppearOnlyOnceTheExtensionDoes() throws Exception {
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc WHERE proname IN"
                + " ('uuid_generate_v1','uuid_generate_v3','uuid_generate_v4','uuid_generate_v5',"
                + "'uuid_nil','uuid_ns_dns','uuid_ns_url','show_trgm','similarity')"));
        assertEquals("42883", stateOf("SELECT uuid_generate_v4()"));
        assertEquals("42883", stateOf("SELECT similarity('abc','abd')"));

        exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
        try {
            assertEquals("1", scalar(
                    "SELECT count(*)::text FROM pg_proc WHERE proname = 'uuid_generate_v4'"));
            // ... and in the schema the extension was installed into, not in pg_catalog.
            assertEquals("public", scalar("SELECT n.nspname FROM pg_proc p"
                    + " JOIN pg_namespace n ON n.oid = p.pronamespace"
                    + " WHERE p.proname = 'uuid_generate_v4'"));
            assertEquals("true", scalar("SELECT (uuid_generate_v4() IS NOT NULL)::text"));
            assertEquals("0", scalar(
                    "SELECT count(*)::text FROM pg_proc WHERE proname = 'similarity'"));
        } finally {
            exec("DROP EXTENSION IF EXISTS \"uuid-ossp\"");
        }
        assertEquals("0", scalar(
                "SELECT count(*)::text FROM pg_proc WHERE proname = 'uuid_generate_v4'"));
    }

    // ---- The names memgres evaluates and did not list ----

    /**
     * Every aggregate the catalog reports has a pg_aggregate row behind it, which is why the
     * thirteen aggregates memgres evaluates and does not list — any_value, mode, the two
     * percentile_* and the nine regr_* — cannot simply be given a pg_proc row: pg_aggregate is
     * built from BuiltinAggregateSignatures, and a prokind='a' row with nothing behind it drops
     * out of every join a tool makes to find out how the aggregate works.
     */
    @Test
    void noAggregateIsReportedWithoutAPgAggregateRowBehindIt() {
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc p WHERE p.prokind = 'a'"
                + " AND NOT EXISTS (SELECT 1 FROM pg_aggregate a WHERE a.aggfnoid = p.oid)"));
    }

    /** Per overload, not one polymorphic type for the lot. */
    @Test
    void theAggregatesCarryTheirOwnReturnTypes() {
        assertEquals("bigint,bigint", scalar("SELECT string_agg(prorettype::regtype::text, ','"
                + " ORDER BY prorettype::regtype::text) FROM pg_proc WHERE proname = 'count'"));
        assertEquals("bigint,double precision,interval,money,numeric,real",
                scalar("SELECT string_agg(DISTINCT prorettype::regtype::text, ','"
                        + " ORDER BY prorettype::regtype::text)"
                        + " FROM pg_proc WHERE proname = 'sum'"));
        assertEquals("double precision,interval,numeric",
                scalar("SELECT string_agg(DISTINCT prorettype::regtype::text, ','"
                        + " ORDER BY prorettype::regtype::text)"
                        + " FROM pg_proc WHERE proname = 'avg'"));
    }

    @Test
    void theRecordReturningFunctionsThatWorkInFromPositionAreListed() {
        assertEquals("4", scalar("SELECT count(*)::text FROM pg_proc WHERE proname IN"
                + " ('json_to_record','jsonb_to_record','json_to_recordset','jsonb_to_recordset')"));
        assertEquals("3", scalar("SELECT count(*)::text FROM pg_proc WHERE proname IN"
                + " ('pg_options_to_table','pg_show_all_settings','pg_available_extension_versions')"));
        assertEquals("record", scalar(
                "SELECT prorettype::regtype::text FROM pg_proc WHERE proname = 'json_to_record'"));
        // One of the pair returns a set and the other does not, which is all that separates them.
        assertEquals("false", scalar(
                "SELECT proretset::text FROM pg_proc WHERE proname = 'jsonb_to_record'"));
        assertEquals("true", scalar(
                "SELECT proretset::text FROM pg_proc WHERE proname = 'jsonb_to_recordset'"));
    }

    /**
     * The thirteen aggregates the catalog still does not name answer correctly all the same, and
     * so do the seven record-returning functions it now does.
     */
    @Test
    void theNamesTheEngineEvaluatesStillRun() {
        assertEquals("7", scalar("SELECT any_value(x)::text FROM (VALUES (7),(8)) t(x)"));
        assertEquals("1", scalar("SELECT (mode() WITHIN GROUP (ORDER BY x))::text"
                + " FROM (VALUES (1),(1),(2)) t(x)"));
        assertEquals("2", scalar("SELECT (percentile_disc(0.5) WITHIN GROUP (ORDER BY x))::text"
                + " FROM (VALUES (1),(2),(3)) t(x)"));
        assertEquals("2", scalar(
                "SELECT regr_count(y,x)::text FROM (VALUES (1.0,1.0),(2.0,2.0)) t(y,x)"));
        assertEquals("0.3333333333333333", scalar(
                "SELECT regr_slope(y,x)::text FROM (VALUES (1.0,1.0),(2.0,4.0)) t(y,x)"));
        assertEquals("true", scalar("SELECT (count(*) >= 0)::text"
                + " FROM json_to_recordset('[{\"a\":1}]') AS t(a int)"));
        assertEquals("true", scalar("SELECT (count(*) >= 0)::text"
                + " FROM pg_options_to_table(ARRAY['fillfactor=70'])"));
        assertEquals("true", scalar("SELECT (count(*) >= 0)::text FROM pg_show_all_settings()"));
        assertEquals("true", scalar(
                "SELECT (count(*) >= 0)::text FROM pg_available_extension_versions()"));
    }

    // ---- What the surviving rows say ----

    @Test
    void theSignatureColumnsSayWhatPostgresSays() {
        assertEquals("oid", scalar("SELECT prorettype::regtype::text FROM pg_proc"
                + " WHERE proname = 'pg_event_trigger_table_rewrite_oid'"));
        assertEquals("integer", scalar("SELECT prorettype::regtype::text FROM pg_proc"
                + " WHERE proname = 'pg_event_trigger_table_rewrite_reason'"));
        assertEquals("record/true", scalar("SELECT prorettype::regtype::text || '/' || proretset::text"
                + " FROM pg_proc WHERE proname = 'pg_event_trigger_ddl_commands'"));
        assertEquals("record/true", scalar("SELECT prorettype::regtype::text || '/' || proretset::text"
                + " FROM pg_proc WHERE proname = 'pg_event_trigger_dropped_objects'"));
        assertEquals("smallint", scalar(
                "SELECT prorettype::regtype::text FROM pg_proc WHERE proname = 'uuid_extract_version'"));
        assertEquals("table_am_handler", scalar("SELECT prorettype::regtype::text FROM pg_proc"
                + " WHERE proname = 'heap_tableam_handler'"));
        assertEquals("index_am_handler",
                scalar("SELECT prorettype::regtype::text FROM pg_proc WHERE proname = 'bthandler'"));
        assertEquals("false", scalar(
                "SELECT proretset::text FROM pg_proc WHERE proname = 'pg_control_system'"));
        assertEquals("oid", scalar("SELECT prorettype::regtype::text FROM pg_proc WHERE proname = 'oid'"));
        // <-> names only the two-argument tsquery_phrase; PostgreSQL declares both forms.
        assertEquals("2", scalar(
                "SELECT count(*)::text FROM pg_proc WHERE proname = 'tsquery_phrase'"));
    }

    /** A defaulted parameter is one the caller may leave out, and a variadic tail has a type. */
    @Test
    void defaultedAndVariadicParametersAreRecorded() {
        assertEquals("1", scalar(
                "SELECT pronargdefaults::text FROM pg_proc WHERE proname = 'jsonb_set'"));
        assertEquals("2", scalar("SELECT pronargdefaults::text FROM pg_proc"
                + " WHERE proname = 'jsonb_path_query' AND prokind = 'f'"));
        assertEquals("false", scalar(
                "SELECT proisstrict::text FROM pg_proc WHERE proname = 'pg_stat_reset_shared'"));
        assertEquals("1", scalar(
                "SELECT pronargdefaults::text FROM pg_proc WHERE proname = 'pg_stat_reset_shared'"));
        assertEquals("2276", scalar(
                "SELECT provariadic::text FROM pg_proc WHERE proname = 'concat'"));
        // ... and a signature with no variadic tail says so.
        assertEquals("true", scalar("SELECT bool_and(provariadic = 0)::text FROM pg_proc"
                + " WHERE proname = 'upper'"));
    }

    /**
     * The polymorphic I/O functions are declared over the kind of type they serve rather than over
     * the one type whose pg_type row happened to reach them, and they read a type OID and a typmod
     * beside the value.
     */
    @Test
    void thePolymorphicIoFunctionsAreDeclaredPolymorphically() {
        assertEquals("anyarray/3", scalar("SELECT prorettype::regtype::text || '/' || pronargs::text"
                + " FROM pg_proc WHERE proname = 'array_in'"));
        assertEquals("record/3", scalar("SELECT prorettype::regtype::text || '/' || pronargs::text"
                + " FROM pg_proc WHERE proname = 'record_recv'"));
        assertEquals("anyrange/3", scalar("SELECT prorettype::regtype::text || '/' || pronargs::text"
                + " FROM pg_proc WHERE proname = 'range_in'"));
        assertEquals("anymultirange/3",
                scalar("SELECT prorettype::regtype::text || '/' || pronargs::text"
                        + " FROM pg_proc WHERE proname = 'multirange_in'"));
        // ... while a type with neither takes just the cstring.
        assertEquals("\"char\"/1", scalar("SELECT prorettype::regtype::text || '/' || pronargs::text"
                + " FROM pg_proc WHERE proname = 'charin'"));
    }

    /** Whatever a row says it takes, it says how many. */
    @Test
    void pronargsIsTheLengthOfProargtypesOnEveryRow() {
        assertEquals("true", scalar("SELECT bool_and(pronargs = array_length(proargtypes, 1))::text"
                + " FROM pg_proc WHERE pronargs > 0"));
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_proc"
                + " WHERE pronargs = 0 AND array_length(proargtypes, 1) > 0"));
    }

    /** No row claims a return type of nothing at all. */
    @Test
    void everyRowCarriesAReturnType() {
        assertEquals("0", scalar(
                "SELECT count(*)::text FROM pg_proc WHERE prorettype = 0 OR prorettype IS NULL"));
    }

    // ---- What a name written for a reg* type resolves to ----
    //
    // Every expectation below was read from the reference server. A schema written in front of
    // the name is part of what is being named: a function of that name in another schema is not
    // the one asked for. A bare name resolves through the search path, and is answered with
    // nothing where several functions carry it.

    @Test
    void aFunctionOnTheSearchPathResolvesAndOneOffItDoesNot() throws SQLException {
        exec("CREATE SCHEMA zcfn_s");
        exec("CREATE FUNCTION zcfn_s.zcfn_f(integer) RETURNS integer AS $$ SELECT $1 $$ LANGUAGE sql");
        exec("CREATE FUNCTION zcfn_s.zcfn_ov(integer) RETURNS integer AS $$ SELECT 1 $$ LANGUAGE sql");
        exec("CREATE FUNCTION zcfn_s.zcfn_ov(text) RETURNS integer AS $$ SELECT 2 $$ LANGUAGE sql");
        exec("CREATE SCHEMA zcfn_t");
        exec("CREATE FUNCTION zcfn_t.zcfn_h(integer) RETURNS integer AS $$ SELECT $1 $$ LANGUAGE sql");
        exec("SET search_path = pg_catalog, zcfn_s");
        try {
            assertEquals("zcfn_f", scalar("SELECT to_regproc('zcfn_f')::text"));
            assertEquals("zcfn_f", scalar("SELECT to_regproc('zcfn_s.zcfn_f')::text"));
            assertEquals("zcfn_f", scalar("SELECT 'zcfn_f'::regproc::text"));
            assertEquals("zcfn_f(integer)", scalar("SELECT to_regprocedure('zcfn_f(int4)')::text"));

            // A schema that holds no such function, and one that does not exist at all.
            assertNull(scalar("SELECT to_regproc('public.zcfn_f')::text"));
            assertNull(scalar("SELECT to_regproc('nosuchschema.zcfn_f')::text"));
            assertNull(scalar("SELECT to_regprocedure('public.zcfn_f(integer)')::text"));
            assertEquals("42883", stateOf("SELECT 'public.zcfn_f'::regproc"));
            assertEquals("42883", stateOf("SELECT 'public.zcfn_f(integer)'::regprocedure"));

            // A schema off the search path has to be written, and is written back.
            assertNull(scalar("SELECT to_regproc('zcfn_h')::text"));
            assertEquals("42883", stateOf("SELECT 'zcfn_h'::regproc"));
            assertEquals("zcfn_t.zcfn_h", scalar("SELECT to_regproc('zcfn_t.zcfn_h')::text"));
            assertEquals("zcfn_t.zcfn_h(integer)",
                    scalar("SELECT to_regprocedure('zcfn_t.zcfn_h(integer)')::text"));

            // A bare name several functions carry is no function to to_regproc and an error to
            // the cast; the signature tells them apart.
            assertNull(scalar("SELECT to_regproc('zcfn_ov')::text"));
            assertEquals("42725", stateOf("SELECT 'zcfn_ov'::regproc"));
            assertEquals("zcfn_ov(integer)", scalar("SELECT to_regprocedure('zcfn_ov(integer)')::text"));
            assertEquals("zcfn_ov(text)", scalar("SELECT to_regprocedure('zcfn_ov(text)')::text"));

            // regprocedure is a signature: the argument types have to be the ones declared, and a
            // bare name is not a signature at all.
            assertNull(scalar("SELECT to_regprocedure('zcfn_f(text)')::text"));
            assertNull(scalar("SELECT to_regprocedure('zcfn_f')::text"));
        } finally {
            exec("SET search_path = public");
            exec("DROP SCHEMA zcfn_s CASCADE");
            exec("DROP SCHEMA zcfn_t CASCADE");
        }
    }

    @Test
    void aBuiltInResolvesThroughItsSignatureAndItsBareNameOnlyWhenItIsAlone() {
        assertEquals("upper(text)", scalar("SELECT 'upper(text)'::regprocedure::text"));
        assertEquals("upper(text)", scalar("SELECT 'pg_catalog.upper(text)'::regprocedure::text"));
        assertEquals("upper(text)", scalar("SELECT to_regprocedure('upper(text)')::text"));
        assertEquals("sum(integer)", scalar("SELECT to_regprocedure('sum(int4)')::text"));
        assertEquals("pg_sleep(double precision)",
                scalar("SELECT to_regprocedure('pg_sleep(float8)')::text"));
        assertEquals("now()", scalar("SELECT to_regprocedure('now()')::text"));

        // upper is declared over text, over a range and over a multirange, so its bare name names
        // no one function; now and pg_sleep are declared once each.
        assertNull(scalar("SELECT to_regproc('upper')::text"));
        assertNull(scalar("SELECT to_regproc('sum')::text"));
        assertEquals("now", scalar("SELECT to_regproc('now')::text"));
        assertEquals("pg_sleep", scalar("SELECT to_regproc('pg_sleep')::text"));

        // public holds none of them, whatever the search path says.
        assertNull(scalar("SELECT to_regprocedure('public.upper(text)')::text"));
        assertEquals("42883", stateOf("SELECT 'public.upper(text)'::regprocedure"));
        assertEquals("42883", stateOf("SELECT 'nosuchschema.upper(text)'::regprocedure"));

        // A reference is held as its name and its OID together, and answers as the type the call
        // was written for.
        assertEquals("regproc", scalar("SELECT pg_typeof(to_regproc('now'))::text"));
        assertEquals("regprocedure", scalar("SELECT pg_typeof(to_regprocedure('now()'))::text"));
    }

    @Test
    void aWriteAheadLogPositionAnswersAsPgLsn() {
        assertEquals("pg_lsn", scalar("SELECT pg_typeof(pg_switch_wal())::text"));
        assertEquals("pg_lsn", scalar("SELECT pg_typeof(pg_current_wal_lsn())::text"));
        assertEquals("pg_lsn", scalar("SELECT pg_typeof(pg_current_wal_insert_lsn())::text"));
        assertEquals("pg_lsn", scalar("SELECT pg_typeof(pg_current_wal_flush_lsn())::text"));
        // memgres keeps no write-ahead log, so there is no position to advance to -- but the
        // value it answers with is a position, and comparing two of them is a pg_lsn comparison.
        assertEquals("true", scalar("SELECT (pg_switch_wal() >= '0/0'::pg_lsn)::text"));
    }

    // ---- helpers ----

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(20);
            s.execute(sql);
        }
    }

    private static String scalar(String sql) {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row from: " + sql);
            String value = rs.getString(1);
            assertTrue(!rs.next(), "more than one row from: " + sql);
            return value;
        } catch (SQLException e) {
            throw new AssertionError(sql + " -> " + e.getMessage(), e);
        }
    }

    private static List<String> column(String sql) {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        } catch (SQLException e) {
            throw new AssertionError(sql + " -> " + e.getMessage(), e);
        }
        return out;
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(20);
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }
}
