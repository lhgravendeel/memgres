package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What pg_proc says about a built-in's parameters, and what pg_type says about the types the
 * catalogs are built out of.
 *
 * <p>Every expectation below was read off a live PostgreSQL 18 server with the reference set
 * pinned to {@code nspname='pg_catalog' AND oid < 16384} and extension-owned rows excluded
 * through pg_depend, then asserted here by NAME — never by a count, because the reference
 * server carries contrib extensions and scratch objects a count would see and memgres would not.
 *
 * <p>What was wrong: proallargtypes, proargmodes, proargnames and proargdefaults were NULL on
 * all 2338 rows, so json_each had no OUT columns, concat was not marked VARIADIC, jsonb_set's
 * fourth argument claimed a default the row did not carry, and pg_get_function_arguments printed
 * a bare type list where PostgreSQL prints named parameters. pg_sleep carried an argument name
 * PostgreSQL does not give it. procost was 100 where PostgreSQL says 1 and 1 where it says 10,
 * 100 or 1000. provariadic held the array type where PostgreSQL holds its element type. Every
 * aggregate and window function was reported parallel unsafe. Six statistics types reported
 * collation 0 where PostgreSQL says 100, and unknown, refcursor and gtsvector had no pg_type row
 * at all.
 */
class ProcArgumentsAndTypeAttributesTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** The one row a pinned query answers, columns joined with '|', or null when there is none. */
    private static String row(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) return null;
            ResultSetMetaData md = rs.getMetaData();
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) sb.append('|');
                sb.append(rs.getString(i));
            }
            assertTrue(!rs.next(), "expected exactly one row from: " + sql);
            return sb.toString();
        }
    }

    private static String proc(String cols, String name, String argTypes) throws SQLException {
        return row("SELECT " + cols + " FROM pg_proc WHERE proname = '" + name
                + "' AND proargtypes::text = '" + argTypes + "'");
    }

    // ---- proallargtypes / proargmodes / proargnames --------------------------------------

    @Test
    void outParametersAreDescribed() throws Exception {
        assertEquals("{114,25,114}|{i,o,o}|{from_json,key,value}",
                proc("proallargtypes::text, proargmodes::text, proargnames::text",
                        "json_each", "114"));
        assertEquals("{3802,25,3802}|{i,o,o}|{from_json,key,value}",
                proc("proallargtypes::text, proargmodes::text, proargnames::text",
                        "jsonb_each", "3802"));
        assertEquals("{1034,26,26,25,16}|{i,o,o,o,o}"
                        + "|{acl,grantor,grantee,privilege_type,is_grantable}",
                proc("proallargtypes::text, proargmodes::text, proargnames::text",
                        "aclexplode", "1034"));
        assertEquals("{3614,25,1005,1009}|{i,o,o,o}|{tsvector,lexeme,positions,weights}",
                proc("proallargtypes::text, proargmodes::text, proargnames::text",
                        "unnest", "3614"));
        assertEquals("{25,18,16,25,25}|{o,o,o,o,o}|{word,catcode,barelabel,catdesc,baredesc}",
                proc("proallargtypes::text, proargmodes::text, proargnames::text",
                        "pg_get_keywords", ""));
        assertEquals("{23,23,20,1184}|{o,o,o,o}"
                        + "|{pg_control_version,catalog_version_no,system_identifier,"
                        + "pg_control_last_modified}",
                proc("proallargtypes::text, proargmodes::text, proargnames::text",
                        "pg_control_system", ""));
    }

    @Test
    void variadicParametersAreMarked() throws Exception {
        assertEquals("{2276}|{v}", proc("proallargtypes::text, proargmodes::text",
                "concat", "2276"));
        assertEquals("{25,2276}|{i,v}", proc("proallargtypes::text, proargmodes::text",
                "concat_ws", "25 2276"));
        assertEquals("{25,2276}|{i,v}", proc("proallargtypes::text, proargmodes::text",
                "format", "25 2276"));
        assertEquals("{2276}|{v}", proc("proallargtypes::text, proargmodes::text",
                "num_nonnulls", "2276"));
        assertEquals("{114,1009}|{i,v}", proc("proallargtypes::text, proargmodes::text",
                "json_extract_path", "114 1009"));
    }

    /** provariadic names the type the tail collects INTO, not the array it is declared as. */
    @Test
    void provariadicIsTheElementType() throws Exception {
        assertEquals("25", proc("provariadic", "json_extract_path", "114 1009"));
        assertEquals("25", proc("provariadic", "jsonb_extract_path_text", "3802 1009"));
        assertEquals("3912", proc("provariadic", "datemultirange", "3913"));
        assertEquals("3904", proc("provariadic", "int4multirange", "3905"));
        assertEquals("3926", proc("provariadic", "int8multirange", "3927"));
        // "any" is its own element type, and the non-variadic overload has none at all.
        assertEquals("2276", proc("provariadic", "concat", "2276"));
        assertEquals("0", proc("provariadic", "datemultirange", "3912"));
    }

    @Test
    void argumentNamesArePostgresOwn() throws Exception {
        assertEquals("{value,delimiter}", proc("proargnames::text", "string_agg", "25 25"));
        assertEquals("{jsonb_in,path,replacement,create_if_missing}",
                proc("proargnames::text", "jsonb_set", "3802 1009 3802 16"));
        assertEquals("{string,pattern}", proc("proargnames::text", "regexp_matches", "25 25"));
        assertEquals("{years,months,weeks,days,hours,mins,secs}",
                proc("proargnames::text", "make_interval", "23 23 23 23 23 23 701"));
        assertEquals("{array,descending}", proc("proargnames::text", "array_sort", "2277 16"));
        // Invented where PostgreSQL has none: pg_sleep's parameter is unnamed.
        assertEquals("t", proc("proargnames IS NULL", "pg_sleep", "701"));
        assertEquals("t", proc("proargnames IS NULL", "pg_sleep_for", "1186"));
        assertEquals("t", proc("proargnames IS NULL", "pg_sleep_until", "1184"));
    }

    /**
     * A row claiming N defaults has to carry N of them.
     *
     * <p>PostgreSQL stores proargdefaults as a serialized node tree; memgres stores the deparsed
     * expressions, one per defaulted argument, the same way it already stores pg_index.indexprs.
     * What both engines have to agree on is what comes back out — see
     * {@link #functionArgumentsRenderTheWayPostgresRendersThem}, where the DEFAULT clauses are
     * character-for-character PostgreSQL's.
     */
    @Test
    void defaultedArgumentsCarryTheirDefaults() throws Exception {
        assertEquals("1|true", proc("pronargdefaults, proargdefaults::text",
                "jsonb_set", "3802 1009 3802 16"));
        assertEquals("7|0|0|0|0|0|0|0.0", proc("pronargdefaults, proargdefaults::text",
                "make_interval", "23 23 23 23 23 23 701"));
        assertEquals("2|'{}'::jsonb|false", proc("pronargdefaults, proargdefaults::text",
                "jsonb_path_exists", "3802 4072 3802 16"));
        assertEquals("1|true", proc("pronargdefaults, proargdefaults::text",
                "parse_ident", "25 16"));
        assertEquals("2|true|60", proc("pronargdefaults, proargdefaults::text",
                "pg_promote", "16 23"));
        assertEquals("2|0|1", proc("pronargdefaults, proargdefaults::text",
                "random_normal", "701 701"));
        assertNull(row("SELECT proname FROM pg_proc"
                + " WHERE pronargdefaults > 0 AND proargdefaults IS NULL"
                + " AND proname NOT LIKE 'pg_stat_statements%' LIMIT 1"));
    }

    /** The rendering a tool reads back out of those columns. */
    @Test
    void functionArgumentsRenderTheWayPostgresRendersThem() throws Exception {
        assertEquals("jsonb_in jsonb, path text[], replacement jsonb,"
                        + " create_if_missing boolean DEFAULT true",
                row("SELECT pg_get_function_arguments(oid) FROM pg_proc"
                        + " WHERE proname='jsonb_set'"));
        assertEquals("from_json json, OUT key text, OUT value json",
                row("SELECT pg_get_function_arguments(oid) FROM pg_proc"
                        + " WHERE proname='json_each' AND pronargs=1"));
        assertEquals("years integer DEFAULT 0, months integer DEFAULT 0, weeks integer DEFAULT 0,"
                        + " days integer DEFAULT 0, hours integer DEFAULT 0,"
                        + " mins integer DEFAULT 0, secs double precision DEFAULT 0.0",
                row("SELECT pg_get_function_arguments(oid) FROM pg_proc"
                        + " WHERE proname='make_interval'"));
        assertEquals("acl aclitem[], OUT grantor oid, OUT grantee oid,"
                        + " OUT privilege_type text, OUT is_grantable boolean",
                row("SELECT pg_get_function_arguments(oid) FROM pg_proc"
                        + " WHERE proname='aclexplode'"));
        assertEquals("value text, delimiter text",
                row("SELECT pg_get_function_arguments(oid) FROM pg_proc"
                        + " WHERE proname='string_agg' AND proargtypes::text='25 25'"));
        assertEquals("OUT word text, OUT catcode \"char\", OUT barelabel boolean,"
                        + " OUT catdesc text, OUT baredesc text",
                row("SELECT pg_get_function_arguments(oid) FROM pg_proc"
                        + " WHERE proname='pg_get_keywords'"));
    }

    // ---- procost / prorows / provolatile / proparallel / proisstrict ---------------------

    @Test
    void costAndRowsAreThePlannersOwn() throws Exception {
        assertEquals("1|0", proc("procost, prorows", "pg_sleep", "701"));
        assertEquals("1|0", proc("procost, prorows", "pg_sleep_for", "1186"));
        assertEquals("100|0", proc("procost, prorows", "to_tsvector", "25"));
        assertEquals("100|0", proc("procost, prorows", "database_to_xml", "16 16 25"));
        assertEquals("10|500", proc("procost, prorows", "pg_get_keywords", ""));
        assertEquals("10|10000", proc("procost, prorows", "ts_stat", "25"));
        assertEquals("10|0", proc("procost, prorows", "pg_table_is_visible", "26"));
        assertEquals("1|100", proc("procost, prorows", "json_each", "114"));
    }

    @Test
    void aggregatesAndWindowFunctionsAreParallelSafe() throws Exception {
        assertEquals("a|s", proc("prokind, proparallel", "count", ""));
        assertEquals("a|s", proc("prokind, proparallel", "sum", "23"));
        assertEquals("a|s", proc("prokind, proparallel", "array_agg", "2277"));
        assertEquals("w|s", proc("prokind, proparallel", "row_number", ""));
        assertEquals("w|s", proc("prokind, proparallel", "lag", "2283"));
        assertNull(row("SELECT p.proname FROM pg_proc p"
                + " JOIN pg_namespace n ON n.oid = p.pronamespace"
                + " WHERE p.prokind IN ('a','w') AND n.nspname = 'pg_catalog'"
                + " AND p.proparallel <> 's' LIMIT 1"));
    }

    /** A window function is strict exactly when it takes an argument. */
    @Test
    void windowFunctionStrictness() throws Exception {
        assertEquals("t", proc("proisstrict", "lag", "2283"));
        assertEquals("t", proc("proisstrict", "lead", "2283 23"));
        assertEquals("t", proc("proisstrict", "first_value", "2283"));
        assertEquals("t", proc("proisstrict", "nth_value", "2283 23"));
        assertEquals("t", proc("proisstrict", "ntile", "23"));
        assertEquals("f", proc("proisstrict", "row_number", ""));
        assertEquals("f", proc("proisstrict", "rank", ""));
    }

    @Test
    void volatilityAndParallelSafetyOfNamedFunctions() throws Exception {
        assertEquals("i|s", proc("provolatile, proparallel", "pg_sleep", "701"));
        assertEquals("s|s", proc("provolatile, proparallel", "json_agg", "2283"));
        assertEquals("s|s", proc("provolatile, proparallel", "jsonb_agg", "2283"));
        assertEquals("s|r", proc("provolatile, proparallel", "age", "28"));
        assertEquals("s|r", proc("provolatile, proparallel", "database_to_xml", "16 16 25"));
        assertEquals("s|s", proc("provolatile, proparallel", "array_in", "2275 26 23"));
        assertEquals("s|s", proc("provolatile, proparallel", "timestamptz_pl_interval",
                "1184 1186"));
        assertEquals("f", proc("proisstrict", "internal_in", "2275"));
        assertEquals("f", proc("proisstrict", "trigger_in", "2275"));
    }

    // ---- pg_type -------------------------------------------------------------------------

    private static String type(String cols, String name) throws SQLException {
        return row("SELECT " + cols + " FROM pg_type WHERE typname = '" + name + "'");
    }

    @Test
    void statisticsTypesCarryTheDefaultCollation() throws Exception {
        assertEquals("100", type("typcollation", "pg_node_tree"));
        assertEquals("100", type("typcollation", "pg_ndistinct"));
        assertEquals("100", type("typcollation", "pg_dependencies"));
        assertEquals("100", type("typcollation", "pg_mcv_list"));
        assertEquals("100", type("typcollation", "pg_brin_bloom_summary"));
        assertEquals("100", type("typcollation", "pg_brin_minmax_multi_summary"));
        // and the ones PostgreSQL leaves uncollated stay so
        assertEquals("0", type("typcollation", "pg_lsn"));
        assertEquals("0", type("typcollation", "jsonpath"));
    }

    @Test
    void theTypesTheCatalogsAreBuiltOutOfExist() throws Exception {
        assertEquals("-2|f|p|X|c|p|0|0", type(
                "typlen, typbyval, typtype, typcategory, typalign, typstorage, typelem, typarray",
                "unknown"));
        assertEquals("-1|f|b|U|i|x|0|2201", type(
                "typlen, typbyval, typtype, typcategory, typalign, typstorage, typelem, typarray",
                "refcursor"));
        assertEquals("-1|f|b|U|i|p|0|3644", type(
                "typlen, typbyval, typtype, typcategory, typalign, typstorage, typelem, typarray",
                "gtsvector"));
        assertEquals("-1|f|b|A|i|x|1790|0", type(
                "typlen, typbyval, typtype, typcategory, typalign, typstorage, typelem, typarray",
                "_refcursor"));
        assertEquals("-1|f|b|A|i|x|3642|0", type(
                "typlen, typbyval, typtype, typcategory, typalign, typstorage, typelem, typarray",
                "_gtsvector"));
        assertEquals("-1|f|b|A|i|x|2275|0", type(
                "typlen, typbyval, typtype, typcategory, typalign, typstorage, typelem, typarray",
                "_cstring"));
        // refcursor reads and writes as text; gtsvector has no binary I/O at all.
        assertEquals("textin|textout|textrecv|textsend",
                type("typinput::text, typoutput::text, typreceive::text, typsend::text",
                        "refcursor"));
        assertEquals("gtsvectorin|gtsvectorout|-|-",
                type("typinput::text, typoutput::text, typreceive::text, typsend::text",
                        "gtsvector"));
        assertEquals("x", row("SELECT 'x'::unknown"));
    }

    @Test
    void fixedWidthTypesNameWhatTheyAreMadeOf() throws Exception {
        assertEquals("18", type("typelem", "name"));
        assertEquals("701", type("typelem", "point"));
        assertEquals("600", type("typelem", "lseg"));
        assertEquals("600", type("typelem", "box"));
        assertEquals("701", type("typelem", "line"));
        assertEquals("name|char", row("SELECT t.typname, e.typname FROM pg_type t"
                + " JOIN pg_type e ON e.oid = t.typelem WHERE t.typname = 'name'"));
    }

    @Test
    void pseudoTypesPointAtTheirArrayAndBinaryIo() throws Exception {
        assertEquals("2287|record_recv|record_send",
                type("typarray, typreceive::text, typsend::text", "record"));
        assertEquals("1263|cstring_recv|cstring_send",
                type("typarray, typreceive::text, typsend::text", "cstring"));
        assertEquals("0|anyarray_recv|anyarray_send",
                type("typarray, typreceive::text, typsend::text", "anyarray"));
        assertEquals("0|anycompatiblearray_recv|anycompatiblearray_send",
                type("typarray, typreceive::text, typsend::text", "anycompatiblearray"));
        assertEquals("0|void_recv|void_send",
                type("typarray, typreceive::text, typsend::text", "void"));
        // and the ones PostgreSQL leaves without binary I/O keep '-'
        assertEquals("0|-|-", type("typarray, typreceive::text, typsend::text", "internal"));
        assertEquals("0|-|-", type("typarray, typreceive::text, typsend::text", "anyelement"));
    }

    // ---- the joins a tool actually writes over these columns ------------------------------

    @Test
    void everyReferenceTheseColumnsMakeResolves() throws Exception {
        assertEquals("0", row("SELECT count(*) FROM pg_type t"
                + " LEFT JOIN pg_type a ON a.oid = t.typarray"
                + " WHERE t.typarray <> 0 AND a.oid IS NULL"));
        assertEquals("0", row("SELECT count(*) FROM pg_type t"
                + " LEFT JOIN pg_type e ON e.oid = t.typelem"
                + " WHERE t.typelem <> 0 AND e.oid IS NULL"));
        assertEquals("0", row("SELECT count(*) FROM pg_proc p"
                + " LEFT JOIN pg_type t ON t.oid = p.prorettype WHERE t.oid IS NULL"));
        // Every OID a proallargtypes array holds names a type that is there.
        assertEquals("2", row("SELECT count(*) FROM pg_type t"
                + " WHERE t.oid = ANY (SELECT unnest(p.proallargtypes) FROM pg_proc p"
                + "   WHERE p.proname = 'json_each' AND p.pronargs = 1)"));
        // proargmodes and proallargtypes always have the same length.
        assertNull(row("SELECT proname FROM pg_proc WHERE proargmodes IS NOT NULL"
                + " AND proallargtypes IS NOT NULL"
                + " AND array_length(proargmodes, 1) <> array_length(proallargtypes, 1) LIMIT 1"));
        // pronargdefaults never exceeds pronargs.
        assertNull(row("SELECT proname FROM pg_proc WHERE pronargdefaults > pronargs LIMIT 1"));
        // provolatile, proparallel and prokind only ever hold values PostgreSQL defines.
        assertNull(row("SELECT proname FROM pg_proc WHERE provolatile NOT IN ('i','s','v')"
                + " OR proparallel NOT IN ('s','r','u') OR prokind NOT IN ('f','a','w','p')"
                + " LIMIT 1"));
    }
}
