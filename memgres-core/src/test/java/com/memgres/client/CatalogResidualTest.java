package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A catalog row is a claim about what the server will do, and a wrong one misleads whatever reads
 * it more thoroughly than a missing one does. These checks pin the rows that described something
 * other than what the engine performs: the multirange a range converts to, the operators over
 * aclitem, oidvector and jsonpath, the operator classes an index could use, and the built-in
 * collations. Alongside them are the two places the catalog is rendered rather than stored — a
 * function's signature and a view's definition — and the MERGE arm whose whole purpose is to find
 * the rows a source dropped.
 *
 * <p>Every expected value here was measured on PostgreSQL 18.</p>
 */
class CatalogResidualTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- pg_cast: the range a multirange is built over -------------------------------------

    @Test
    void a_range_casts_name_their_own_multirange() throws SQLException {
        assertRows(List.of(
                        "daterange | datemultirange | e | f",
                        "int4range | int4multirange | e | f",
                        "int8range | int8multirange | e | f",
                        "numrange | nummultirange | e | f",
                        "tsrange | tsmultirange | e | f",
                        "tstzrange | tstzmultirange | e | f"),
                "SELECT s.typname::text, t.typname::text, c.castcontext::text, c.castmethod::text "
                        + "FROM pg_cast c JOIN pg_type s ON s.oid = c.castsource "
                        + "JOIN pg_type t ON t.oid = c.casttarget "
                        + "WHERE s.typname::text IN ('int4range','int8range','numrange','tsrange',"
                        + "'tstzrange','daterange') ORDER BY 1, 2");
    }

    @Test
    void a_multirange_types_carry_postgres_oids() throws SQLException {
        assertRows(List.of(
                        "datemultirange | 4535", "int4multirange | 4451", "int8multirange | 4536",
                        "nummultirange | 4532", "tsmultirange | 4533", "tstzmultirange | 4534"),
                "SELECT typname::text, oid::int FROM pg_type WHERE typname::text IN "
                        + "('int4multirange','int8multirange','nummultirange','tsmultirange',"
                        + "'tstzmultirange','datemultirange') ORDER BY 1");
        assertRows(List.of(
                        "_datemultirange | 6155 | 4535", "_int4multirange | 6150 | 4451",
                        "_int8multirange | 6157 | 4536", "_nummultirange | 6151 | 4532",
                        "_tsmultirange | 6152 | 4533", "_tstzmultirange | 6153 | 4534"),
                "SELECT typname::text, oid::int, typelem::int FROM pg_type WHERE typname::text IN "
                        + "('_int4multirange','_int8multirange','_nummultirange','_tsmultirange',"
                        + "'_tstzmultirange','_datemultirange') ORDER BY 1");
    }

    @Test
    void a_pg_range_pairs_each_range_with_its_multirange() throws SQLException {
        assertRows(List.of(
                        "daterange | date | datemultirange",
                        "int4range | integer | int4multirange",
                        "int8range | bigint | int8multirange",
                        "numrange | numeric | nummultirange",
                        "tsrange | timestamp without time zone | tsmultirange",
                        "tstzrange | timestamp with time zone | tstzmultirange"),
                "SELECT rngtypid::regtype::text, rngsubtype::regtype::text, rngmultitypid::regtype::text "
                        + "FROM pg_range WHERE rngtypid::regtype::text IN ('int4range','int8range',"
                        + "'numrange','tsrange','tstzrange','daterange') ORDER BY 1");
    }

    @Test
    void a_range_to_multirange_conversions_still_run() throws SQLException {
        assertRows(List.of("{[1,2)} | {[2020-01-01,2020-02-01)} | {[1,5)} | {[1,5)}"),
                "SELECT numrange(1,2)::nummultirange::text, "
                        + "daterange('2020-01-01','2020-02-01')::datemultirange::text, "
                        + "int8range(1,5)::int8multirange::text, int4range(1,5)::int4multirange::text");
    }

    // ---- pg_operator: aclitem, oidvector and jsonpath ---------------------------------------

    @Test
    void b_jsonpath_operators_answer_and_are_catalogued() throws SQLException {
        assertRows(List.of("true | true | false | false"),
                "SELECT '{\"a\":1}'::jsonb @? '$.a'::jsonpath, "
                        + "'{\"a\":1}'::jsonb @@ '$.a == 1'::jsonpath, "
                        + "'{\"a\":1}'::jsonb @? '$.b'::jsonpath, "
                        + "'{\"a\":1}'::jsonb @@ '$.a == 2'::jsonpath");
        // The same path written with the key already quoted, which is how PG's jsonpath prints.
        assertRows(List.of("yes | true"),
                "SELECT CASE WHEN jsonb_path_exists('{\"a\":1}'::jsonb, '$.\"a\"') "
                        + "THEN 'yes' ELSE 'no' END, "
                        + "'{\"a\":{\"b\":2}}'::jsonb @? '$.a.b'::jsonpath");
    }

    @Test
    void b_oidvector_is_a_type_with_comparisons() throws SQLException {
        assertRows(List.of("1 2 | true | true | true | true | true | true"),
                "SELECT '1 2'::oidvector::text, '1 2'::oidvector = '1 2'::oidvector, "
                        + "'1 2'::oidvector < '1 3'::oidvector, '1 2'::oidvector <> '1 3'::oidvector, "
                        + "'1 2'::oidvector <= '1 2'::oidvector, '2 2'::oidvector > '1 3'::oidvector, "
                        + "'1 2'::oidvector >= '1 2'::oidvector");
        assertRows(List.of("oidvector | int2vector"),
                "SELECT 'oidvector'::regtype::text, 'int2vector'::regtype::text");
    }

    @Test
    void b_operator_catalog_lists_the_aclitem_and_vector_families() throws SQLException {
        assertRows(List.of(
                        "+ | aclitem[] | aclitem | aclitem[]",
                        "- | aclitem[] | aclitem | aclitem[]",
                        "< | oidvector | oidvector | boolean",
                        "<= | oidvector | oidvector | boolean",
                        "<> | oidvector | oidvector | boolean",
                        "= | aclitem | aclitem | boolean",
                        "= | oidvector | oidvector | boolean",
                        "> | oidvector | oidvector | boolean",
                        ">= | oidvector | oidvector | boolean",
                        "@> | aclitem[] | aclitem | boolean",
                        "@? | jsonb | jsonpath | boolean",
                        "@@ | jsonb | jsonpath | boolean"),
                "SELECT o.oprname::text, o.oprleft::regtype::text, o.oprright::regtype::text, "
                        + "o.oprresult::regtype::text FROM pg_operator o "
                        + "WHERE o.oprleft::regtype::text IN ('aclitem[]','aclitem','oidvector') "
                        + "OR o.oprright::regtype::text = 'jsonpath' ORDER BY 1, 2, 3");
    }

    @Test
    void b_every_operator_still_names_a_function_and_two_types() throws SQLException {
        assertRows(List.of("0"),
                "SELECT count(*)::int FROM pg_operator o WHERE NOT EXISTS "
                        + "(SELECT 1 FROM pg_proc p WHERE p.oid = o.oprcode)");
        assertRows(List.of("0"),
                "SELECT count(*)::int FROM pg_operator o WHERE (o.oprleft <> 0 AND NOT EXISTS "
                        + "(SELECT 1 FROM pg_type t WHERE t.oid = o.oprleft)) OR (o.oprright <> 0 "
                        + "AND NOT EXISTS (SELECT 1 FROM pg_type t WHERE t.oid = o.oprright))");
    }

    // ---- the 0-based vectors the catalogs subscript from zero -------------------------------

    @Test
    void c_oidvector_has_one_dimension_even_when_empty() throws SQLException {
        assertRows(List.of("0 | 0 | 0 | -1 | 0"),
                "SELECT pronargs::int, array_length(proargtypes,1), array_lower(proargtypes,1), "
                        + "array_upper(proargtypes,1), cardinality(proargtypes)::int "
                        + "FROM pg_proc WHERE proname = 'now'");
        assertRows(List.of("1 | 1 | 0 | 0"),
                "SELECT DISTINCT pronargs::int, array_length(proargtypes,1), "
                        + "array_lower(proargtypes,1), array_upper(proargtypes,1) "
                        + "FROM pg_proc WHERE proname = 'abs'");
        // PostgreSQL's own opr_sanity query: a zero-argument function is a mismatch, and there
        // are plenty of those.
        assertRows(List.of("true"),
                "SELECT count(*)::int > 0 FROM pg_proc "
                        + "WHERE array_length(proargtypes,1) IS DISTINCT FROM NULLIF(pronargs,0)");
    }

    @Test
    void c_an_ordinary_empty_array_still_has_no_dimensions() throws SQLException {
        assertRows(List.of("3 | NULL | 1 | 2 | 0"),
                "SELECT array_length(ARRAY[1,2,3],1), array_length(ARRAY[]::int[],1), "
                        + "array_lower(ARRAY[1,2],1), array_upper(ARRAY[1,2],1), "
                        + "cardinality(ARRAY[]::int[])::int");
    }

    // ---- pg_opclass / pg_opfamily ------------------------------------------------------------

    @Test
    void d_core_btree_operator_classes_are_all_present() throws SQLException {
        assertRows(List.of(
                        "array_ops | true", "bool_ops | true", "bpchar_ops | true", "bytea_ops | true",
                        "date_ops | true", "float8_ops | true", "inet_ops | true", "int4_ops | true",
                        "int8_ops | true", "interval_ops | true", "jsonb_ops | true", "money_ops | true",
                        "name_ops | true", "numeric_ops | true", "oid_ops | true", "text_ops | true",
                        "time_ops | true", "timestamp_ops | true", "uuid_ops | true", "varchar_ops | false"),
                "SELECT o.opcname::text, o.opcdefault::text FROM pg_opclass o "
                        + "JOIN pg_am a ON a.oid = o.opcmethod WHERE a.amname::text = 'btree' "
                        + "AND o.opcname::text IN ('int4_ops','int8_ops','text_ops','varchar_ops',"
                        + "'bool_ops','date_ops','timestamp_ops','uuid_ops','numeric_ops','bytea_ops',"
                        + "'float8_ops','interval_ops','array_ops','bpchar_ops','oid_ops','name_ops',"
                        + "'jsonb_ops','inet_ops','time_ops','money_ops') ORDER BY 1");
    }

    @Test
    void d_core_btree_operator_families_are_all_present() throws SQLException {
        assertRows(List.of("array_ops", "bool_ops", "bytea_ops", "datetime_ops", "float_ops",
                        "integer_ops", "interval_ops", "jsonb_ops", "money_ops", "network_ops",
                        "numeric_ops", "oid_ops", "text_ops", "time_ops", "uuid_ops"),
                "SELECT f.opfname::text FROM pg_opfamily f JOIN pg_am a ON a.oid = f.opfmethod "
                        + "WHERE a.amname::text = 'btree' AND f.opfname::text IN ('integer_ops',"
                        + "'text_ops','datetime_ops','numeric_ops','bool_ops','uuid_ops','array_ops',"
                        + "'bytea_ops','float_ops','interval_ops','network_ops','jsonb_ops','time_ops',"
                        + "'money_ops','oid_ops') ORDER BY 1");
    }

    @Test
    void d_operator_classes_name_the_type_and_family_postgres_names() throws SQLException {
        assertRows(List.of(
                        "array_ops | anyarray | true | array_ops",
                        "inet_ops | inet | true | network_ops",
                        "name_ops | name | true | text_ops",
                        "oidvector_ops | oidvector | true | oidvector_ops",
                        "varchar_ops | text | false | text_ops",
                        "varchar_pattern_ops | text | false | text_pattern_ops"),
                "SELECT o.opcname::text, o.opcintype::regtype::text, o.opcdefault::text, f.opfname::text "
                        + "FROM pg_opclass o JOIN pg_am a ON a.oid = o.opcmethod "
                        + "JOIN pg_opfamily f ON f.oid = o.opcfamily WHERE a.amname::text = 'btree' "
                        + "AND o.opcname::text IN ('array_ops','inet_ops','name_ops','oidvector_ops',"
                        + "'varchar_ops','varchar_pattern_ops') ORDER BY 1");
    }

    @Test
    void d_no_operator_class_points_at_a_missing_type_family_or_method() throws SQLException {
        assertRows(List.of("0"),
                "SELECT count(*)::int FROM pg_opclass o WHERE NOT EXISTS "
                        + "(SELECT 1 FROM pg_type t WHERE t.oid = o.opcintype) OR NOT EXISTS "
                        + "(SELECT 1 FROM pg_opfamily f WHERE f.oid = o.opcfamily) OR NOT EXISTS "
                        + "(SELECT 1 FROM pg_am a WHERE a.oid = o.opcmethod)");
        // Nothing is registered twice under one OID, which a join by OID would turn into
        // duplicate rows.
        assertRows(List.of("0"),
                "SELECT count(*)::int FROM (SELECT oid FROM pg_opclass GROUP BY oid "
                        + "HAVING count(*) > 1) d");
    }

    // ---- pg_collation ------------------------------------------------------------------------

    @Test
    void e_builtin_collations_report_the_provider_they_belong_to() throws SQLException {
        assertRows(List.of(
                        "C | c | true | C | C | NULL",
                        "POSIX | c | true | POSIX | POSIX | NULL",
                        "default | d | true | NULL | NULL | NULL",
                        "pg_c_utf8 | b | true | NULL | NULL | C.UTF-8",
                        "ucs_basic | b | true | NULL | NULL | C",
                        "unicode | i | true | NULL | NULL | und"),
                "SELECT collname::text, collprovider::text, collisdeterministic::text, "
                        + "collcollate, collctype, colllocale FROM pg_collation WHERE collname::text "
                        + "IN ('default','C','POSIX','ucs_basic','unicode','pg_c_utf8') ORDER BY 1");
    }

    @Test
    void e_every_collation_the_catalog_lists_is_one_collate_accepts() throws SQLException {
        assertRows(List.of("true | true | true | true"),
                "SELECT 'b' COLLATE \"ucs_basic\" < 'c' COLLATE \"ucs_basic\", "
                        + "'b' COLLATE \"unicode\" < 'c' COLLATE \"unicode\", "
                        + "'b' COLLATE \"pg_c_utf8\" < 'c' COLLATE \"pg_c_utf8\", "
                        + "'b' COLLATE \"C\" < 'c' COLLATE \"C\"");
        // A name no collation has is still refused.
        assertError("42704", "does not exist", "SELECT 'b' COLLATE \"cr2_nope\"");
    }

    // ---- pg_get_function_* -------------------------------------------------------------------

    @Test
    void f_function_signatures_render_from_the_catalog_row() throws SQLException {
        assertRows(List.of("bigint", "double precision", "integer", "numeric", "real", "smallint"),
                "SELECT DISTINCT pg_get_function_arguments(oid) FROM pg_proc "
                        + "WHERE proname = 'abs' ORDER BY 1");
        assertRows(List.of("bigint", "double precision", "integer", "numeric", "real", "smallint"),
                "SELECT DISTINCT pg_get_function_result(oid) FROM pg_proc "
                        + "WHERE proname = 'abs' ORDER BY 1");
        assertRows(List.of("jsonb"),
                "SELECT pg_get_function_result(oid) FROM pg_proc WHERE proname = 'jsonb_set'");
        assertRows(List.of("anymultirange | anyelement", "anyrange | anyelement", "text | text"),
                "SELECT pg_get_function_arguments(oid), pg_get_function_result(oid) "
                        + "FROM pg_proc WHERE proname = 'upper' ORDER BY 1");
    }

    @Test
    void f_a_function_with_no_arguments_renders_an_empty_list() throws SQLException {
        assertRows(List.of(" |  | timestamp with time zone"),
                "SELECT pg_get_function_arguments(oid), pg_get_function_identity_arguments(oid), "
                        + "pg_get_function_result(oid) FROM pg_proc WHERE proname = 'now'");
    }

    // ---- MERGE ... WHEN NOT MATCHED BY SOURCE -----------------------------------------------

    @Test
    void g_not_matched_by_source_reaches_only_the_rows_the_source_dropped() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_mt (id int PRIMARY KEY, v int)");
            s.execute("CREATE TABLE cr2_ms (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO cr2_mt VALUES (1,10),(2,20),(3,30)");
            s.execute("INSERT INTO cr2_ms VALUES (2,200),(3,300),(4,400)");
            // No WHEN MATCHED arm at all: the rows the source did match are still matched.
            assertEquals(1, s.executeUpdate("MERGE INTO cr2_mt t USING cr2_ms s ON t.id = s.id "
                    + "WHEN NOT MATCHED BY SOURCE THEN DELETE"));
            assertRows(List.of("2 | 20", "3 | 30"), "SELECT id, v FROM cr2_mt ORDER BY id");

            s.execute("DELETE FROM cr2_mt");
            s.execute("INSERT INTO cr2_mt VALUES (1,10),(2,20),(3,30)");
            assertEquals(1, s.executeUpdate("MERGE INTO cr2_mt t USING cr2_ms s ON t.id = s.id "
                    + "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = -1"));
            assertRows(List.of("1 | -1", "2 | 20", "3 | 30"), "SELECT id, v FROM cr2_mt ORDER BY id");
            s.execute("DROP TABLE cr2_mt");
            s.execute("DROP TABLE cr2_ms");
        }
    }

    @Test
    void g_the_and_condition_narrows_the_unmatched_rows_rather_than_replacing_the_test()
            throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_ct (id int PRIMARY KEY, v int)");
            s.execute("CREATE TABLE cr2_cs (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO cr2_ct VALUES (1,10),(2,20),(3,30),(5,50)");
            s.execute("INSERT INTO cr2_cs VALUES (2,200),(3,300)");
            assertEquals(1, s.executeUpdate("MERGE INTO cr2_ct t USING cr2_cs s ON t.id = s.id "
                    + "WHEN NOT MATCHED BY SOURCE AND t.v > 20 THEN DELETE"));
            assertRows(List.of("1 | 10", "2 | 20", "3 | 30"), "SELECT id, v FROM cr2_ct ORDER BY id");
            s.execute("DROP TABLE cr2_ct");
            s.execute("DROP TABLE cr2_cs");
        }
    }

    @Test
    void g_not_matched_by_source_keeps_working_beside_the_other_arms() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_ft (id int PRIMARY KEY, v int)");
            s.execute("CREATE TABLE cr2_fs (id int PRIMARY KEY, v int)");
            s.execute("INSERT INTO cr2_ft VALUES (1,10),(2,20),(3,30)");
            s.execute("INSERT INTO cr2_fs VALUES (2,200),(3,300),(4,400)");
            s.execute("MERGE INTO cr2_ft t USING cr2_fs s ON t.id = s.id "
                    + "WHEN MATCHED THEN UPDATE SET v = s.v "
                    + "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v) "
                    + "WHEN NOT MATCHED BY SOURCE THEN DELETE");
            assertRows(List.of("2 | 200", "3 | 300", "4 | 400"),
                    "SELECT id, v FROM cr2_ft ORDER BY id");

            // A source that matches every target row leaves the arm with nothing to do.
            s.execute("DELETE FROM cr2_ft");
            s.execute("INSERT INTO cr2_ft VALUES (2,20),(3,30)");
            s.execute("MERGE INTO cr2_ft t USING cr2_fs s ON t.id = s.id "
                    + "WHEN NOT MATCHED BY SOURCE THEN DELETE");
            assertRows(List.of("2 | 20", "3 | 30"), "SELECT id, v FROM cr2_ft ORDER BY id");

            // The ON condition need not be an equality for the arm to read it.
            s.execute("DELETE FROM cr2_ft");
            s.execute("DELETE FROM cr2_fs");
            s.execute("INSERT INTO cr2_ft VALUES (1,10),(2,20),(3,30)");
            s.execute("INSERT INTO cr2_fs VALUES (2,200)");
            s.execute("MERGE INTO cr2_ft t USING cr2_fs s ON t.id < s.id "
                    + "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET v = 0");
            assertRows(List.of("1 | 10", "2 | 0", "3 | 0"), "SELECT id, v FROM cr2_ft ORDER BY id");
            s.execute("DROP TABLE cr2_ft");
            s.execute("DROP TABLE cr2_fs");
        }
    }

    @Test
    void g_a_when_clause_after_an_unconditional_one_of_its_kind_is_refused() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_ut (id int PRIMARY KEY, v int)");
            s.execute("CREATE TABLE cr2_us (id int PRIMARY KEY, v int)");
            String head = "MERGE INTO cr2_ut t USING cr2_us s ON t.id = s.id ";
            assertError("42601", "unreachable WHEN clause specified after unconditional WHEN clause",
                    head + "WHEN MATCHED THEN DELETE WHEN MATCHED AND t.v > 0 THEN DELETE");
            assertError("42601", "unreachable WHEN clause specified after unconditional WHEN clause",
                    head + "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v) "
                            + "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)");
            assertError("42601", "unreachable WHEN clause specified after unconditional WHEN clause",
                    head + "WHEN NOT MATCHED BY SOURCE THEN DELETE "
                            + "WHEN NOT MATCHED BY SOURCE AND t.v > 0 THEN DELETE");
            // A clause of another kind after an unconditional one is reachable, and a conditional
            // arm before an unconditional one is the ordinary way to write a MERGE.
            s.execute(head + "WHEN MATCHED THEN DELETE "
                    + "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)");
            s.execute(head + "WHEN MATCHED THEN DELETE WHEN NOT MATCHED BY SOURCE THEN DELETE");
            s.execute(head + "WHEN MATCHED AND t.v > 0 THEN DELETE WHEN MATCHED THEN DELETE");
            s.execute("DROP TABLE cr2_ut");
            s.execute("DROP TABLE cr2_us");
        }
    }

    @Test
    void g_a_statement_that_simply_stops_is_reported_at_end_of_input() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_et (id int PRIMARY KEY, v int)");
            s.execute("CREATE TABLE cr2_es (id int PRIMARY KEY, v int)");
            assertError("42601", "syntax error at end of input",
                    "MERGE INTO cr2_et t USING cr2_es s ON t.id = s.id");
            assertError("42601", "syntax error at end of input", "SELECT * FROM");
            // A statement that stops on a word is still reported against that word.
            assertError("42601", "syntax error at or near \"FROM\"", "SELECT FROM FROM");
            s.execute("DROP TABLE cr2_et");
            s.execute("DROP TABLE cr2_es");
        }
    }

    @Test
    void g_merge_onto_an_unwritable_view_is_refused_by_its_own_action() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_vt2 (id int PRIMARY KEY, v int)");
            s.execute("CREATE TABLE cr2_vs (id int PRIMARY KEY, v int)");
            s.execute("CREATE VIEW cr2_vd AS SELECT DISTINCT id, v FROM cr2_vt2");
            assertError("55000", "cannot update view \"cr2_vd\"",
                    "MERGE INTO cr2_vd t USING cr2_vs s ON t.id = s.id "
                            + "WHEN MATCHED THEN UPDATE SET v = s.v");
            assertError("55000", "cannot insert into view \"cr2_vd\"",
                    "MERGE INTO cr2_vd t USING cr2_vs s ON t.id = s.id "
                            + "WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)");
            s.execute("DROP VIEW cr2_vd");
            s.execute("DROP TABLE cr2_vt2");
            s.execute("DROP TABLE cr2_vs");
        }
    }

    // ---- pg_get_viewdef ----------------------------------------------------------------------

    @Test
    void h_view_definitions_carry_one_pair_of_parentheses_per_operator() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_dt (a int PRIMARY KEY, b text)");
            s.execute("CREATE VIEW cr2_dv AS SELECT a, b FROM cr2_dt WHERE a > 1");
            assertRows(List.of(" SELECT a,\n    b\n   FROM cr2_dt\n  WHERE (a > 1);"),
                    "SELECT pg_get_viewdef('cr2_dv'::regclass)");
            assertRows(List.of(" SELECT a,\n    b\n   FROM cr2_dt\n  WHERE (a > 1);"),
                    "SELECT definition FROM pg_views WHERE viewname = 'cr2_dv'");
            // The second argument prunes the pairs precedence makes unnecessary...
            assertRows(List.of(" SELECT a,\n    b\n   FROM cr2_dt\n  WHERE a > 1;"),
                    "SELECT pg_get_viewdef('cr2_dv'::regclass, true)");
            // ...and a wrap column keeps the select list on one line when it fits.
            assertRows(List.of(" SELECT a, b\n   FROM cr2_dt\n  WHERE a > 1;"),
                    "SELECT pg_get_viewdef('cr2_dv'::regclass, 80)");
            s.execute("DROP VIEW cr2_dv");
            s.execute("DROP TABLE cr2_dt");
        }
    }

    @Test
    void h_precedence_decides_which_parentheses_the_pretty_form_keeps() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_pt (a int PRIMARY KEY, b int, c int)");
            s.execute("CREATE VIEW cr2_or AS SELECT a FROM cr2_pt WHERE a > 1 OR b < 2");
            s.execute("CREATE VIEW cr2_and AS SELECT a FROM cr2_pt WHERE (a > 1 OR b < 2) AND c = 3");
            s.execute("CREATE VIEW cr2_ar AS SELECT a * 2 + b AS x, a - b AS y FROM cr2_pt");
            assertRows(List.of(" SELECT a\n   FROM cr2_pt\n  WHERE ((a > 1) OR (b < 2));"),
                    "SELECT pg_get_viewdef('cr2_or'::regclass)");
            assertRows(List.of(" SELECT a\n   FROM cr2_pt\n  WHERE a > 1 OR b < 2;"),
                    "SELECT pg_get_viewdef('cr2_or'::regclass, true)");
            assertRows(List.of(" SELECT a\n   FROM cr2_pt\n  WHERE (((a > 1) OR (b < 2)) AND (c = 3));"),
                    "SELECT pg_get_viewdef('cr2_and'::regclass)");
            // The OR keeps its pair because AND binds tighter; nothing else needs one.
            assertRows(List.of(" SELECT a\n   FROM cr2_pt\n  WHERE (a > 1 OR b < 2) AND c = 3;"),
                    "SELECT pg_get_viewdef('cr2_and'::regclass, true)");
            assertRows(List.of(" SELECT a * 2 + b AS x,\n    a - b AS y\n   FROM cr2_pt;"),
                    "SELECT pg_get_viewdef('cr2_ar'::regclass, true)");
            s.execute("DROP VIEW cr2_or");
            s.execute("DROP VIEW cr2_and");
            s.execute("DROP VIEW cr2_ar");
            s.execute("DROP TABLE cr2_pt");
        }
    }

    @Test
    void h_each_clause_starts_its_own_line() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_gt (a int PRIMARY KEY, b int)");
            s.execute("CREATE VIEW cr2_gv AS SELECT b, count(*) AS n FROM cr2_gt "
                    + "GROUP BY b HAVING count(*) > 1");
            assertRows(List.of(" SELECT b,\n    count(*) AS n\n   FROM cr2_gt\n  GROUP BY b\n HAVING (count(*) > 1);"),
                    "SELECT pg_get_viewdef('cr2_gv'::regclass)");
            assertRows(List.of(" SELECT b,\n    count(*) AS n\n   FROM cr2_gt\n  GROUP BY b\n HAVING count(*) > 1;"),
                    "SELECT pg_get_viewdef('cr2_gv'::regclass, true)");
            s.execute("DROP VIEW cr2_gv");
            s.execute("DROP TABLE cr2_gt");
        }
    }

    @Test
    void h_a_clause_word_inside_a_subquery_or_a_literal_stays_where_it_is() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_st (a int PRIMARY KEY, b text)");
            s.execute("INSERT INTO cr2_st VALUES (1, 'from where')");
            s.execute("CREATE VIEW cr2_sv AS SELECT a FROM cr2_st WHERE b = 'from where'");
            String sv = one("SELECT pg_get_viewdef('cr2_sv'::regclass)");
            assertEquals(1, countOf(sv, "\n   FROM"), sv);
            assertEquals(1, countOf(sv, "\n  WHERE"), sv);
            assertTrue(sv.contains("'from where'"), sv);
            // A window function's own ORDER BY is inside parentheses: it is not a clause of the
            // statement and must not be pulled out onto a line of its own.
            s.execute("CREATE VIEW cr2_wv AS SELECT row_number() OVER (ORDER BY a) AS r FROM cr2_st");
            String wv = one("SELECT pg_get_viewdef('cr2_wv'::regclass)");
            assertEquals(0, countOf(wv, "\n  ORDER BY"), wv);
            assertEquals(1, countOf(wv, "\n   FROM cr2_st"), wv);
            assertRows(List.of("1"), "SELECT r::text FROM cr2_wv");
            s.execute("DROP VIEW cr2_wv");
            s.execute("DROP VIEW cr2_sv");
            s.execute("DROP TABLE cr2_st");
        }
    }

    @Test
    void h_an_in_list_renders_as_sql_rather_than_as_a_parse_tree() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cr2_it (a int PRIMARY KEY, b int)");
            s.execute("CREATE VIEW cr2_iv AS SELECT a FROM cr2_it WHERE a IN (1,2)");
            s.execute("CREATE VIEW cr2_nv AS SELECT a FROM cr2_it WHERE a NOT IN (1,2)");
            assertRows(List.of(" SELECT a\n   FROM cr2_it\n  WHERE (a = ANY (ARRAY[1, 2]));"),
                    "SELECT pg_get_viewdef('cr2_iv'::regclass)");
            assertRows(List.of(" SELECT a\n   FROM cr2_it\n  WHERE (a <> ALL (ARRAY[1, 2]));"),
                    "SELECT pg_get_viewdef('cr2_nv'::regclass)");
            s.execute("DROP VIEW cr2_nv");
            s.execute("DROP VIEW cr2_iv");
            s.execute("DROP TABLE cr2_it");
        }
    }

    // ---- helpers -----------------------------------------------------------------------------

    private static int countOf(String haystack, String needle) {
        int n = 0, i = 0;
        while ((i = haystack.indexOf(needle, i)) >= 0) { n++; i += needle.length(); }
        return n;
    }

    private static String one(String sql) throws SQLException {
        List<String> rows = query(sql);
        assertEquals(1, rows.size(), sql);
        return rows.get(0);
    }

    private static List<String> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            try (ResultSet rs = s.executeQuery(sql)) {
                ResultSetMetaData md = rs.getMetaData();
                int n = md.getColumnCount();
                List<String> out = new ArrayList<>();
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) sb.append(" | ");
                        Object v = rs.getObject(i);
                        sb.append(v == null ? "NULL" : String.valueOf(v));
                    }
                    out.add(sb.toString());
                }
                return out;
            }
        }
    }

    private static void assertRows(List<String> expected, String sql) throws SQLException {
        assertEquals(expected, query(sql), sql);
    }

    private static void assertError(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> {
            try (Statement s = conn.createStatement()) {
                s.setQueryTimeout(10);
                s.execute(sql);
            }
        }, sql);
        assertEquals(sqlState, e.getSQLState(), e.getMessage());
        assertTrue(e.getMessage().contains(messagePart), e.getMessage());
    }
}
