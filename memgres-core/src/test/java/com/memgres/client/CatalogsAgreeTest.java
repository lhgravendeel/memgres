package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A catalog is read to find out what a relation is, so two catalogs that describe the same column
 * differently leave a tool believing something the database will not do. These checks pin the
 * places where one catalog used to contradict another: the type modifier pg_attribute records
 * against the precision information_schema reports, the columns a constraint is used by, the
 * schema an object lives in, and the bounds SET is judged against.
 *
 * <p>Every expected value here was measured on PostgreSQL 18.</p>
 */
class CatalogsAgreeTest {

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

    // ---- interval field qualifiers ---------------------------------------------------------

    @Test
    void a_precision_belongs_to_second_alone() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertSyntaxError(s, "CREATE TABLE cg_bad (a interval year(2))");
            assertSyntaxError(s, "CREATE TABLE cg_bad (a interval minute(4))");
            // A range runs from the larger field down to a smaller one, never back up.
            assertSyntaxError(s, "CREATE TABLE cg_bad (a interval second to day)");
            assertSyntaxError(s, "CREATE TABLE cg_bad (a interval hour to year)");
            assertSyntaxError(s, "CREATE TABLE cg_bad (a interval month to day)");
            // The spellings SQL does define are still accepted.
            s.execute("CREATE TABLE cg_ok (a interval second(3), b interval day to second(3), "
                    + "c interval year to month, d interval hour)");
            s.execute("DROP TABLE cg_ok");
        }
    }

    // ---- pg_attribute.atttypmod and format_type --------------------------------------------

    @Test
    void a_columns_type_modifier_carries_its_declaration() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cg_dt (a timestamp, b timestamp(3), c time(2), "
                    + "d timestamptz(1), e timetz(4), f interval(2), g interval day to second(3), "
                    + "h bit(4), i bit varying(8), j varchar(9), k numeric(7,3))");
            assertEquals(List.of(
                            "a=timestamp without time zone/-1",
                            "b=timestamp(3) without time zone/3",
                            "c=time(2) without time zone/2",
                            "d=timestamp(1) with time zone/1",
                            "e=time(4) with time zone/4",
                            "f=interval(2)/2147418114",
                            "g=interval day to second(3)/470286339",
                            "h=bit(4)/4",
                            "i=bit varying(8)/8",
                            "j=character varying(9)/13",
                            "k=numeric(7,3)/458759"),
                    triples(s, "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.atttypmod "
                            + "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                            + "WHERE c.relname = 'cg_dt' AND a.attnum > 0 ORDER BY a.attnum"));
            s.execute("DROP TABLE cg_dt");
        }
    }

    @Test
    void information_schema_reports_the_same_precision_pg_attribute_records() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cg_ts (b timestamp(3), c time(2), n interval day to second(3))");
            assertEquals(List.of("b=3/null", "c=2/null", "n=3/DAY TO SECOND(3)"),
                    triples(s, "SELECT column_name, datetime_precision, interval_type "
                            + "FROM information_schema.columns WHERE table_name = 'cg_ts' "
                            + "ORDER BY ordinal_position"));
            s.execute("DROP TABLE cg_ts");
        }
    }

    @Test
    void a_column_says_whether_its_type_is_passed_by_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cg_bv (a int, b numeric, c bigint, d text)");
            assertEquals(List.of("a=t/4/i/p", "b=f/-1/i/m", "c=t/8/d/p", "d=f/-1/i/x"),
                    join(s, "SELECT a.attname, a.attbyval, a.attlen, a.attalign, a.attstorage "
                            + "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                            + "WHERE c.relname = 'cg_bv' AND a.attnum > 0 ORDER BY a.attnum", 5));
            s.execute("DROP TABLE cg_bv");
        }
    }

    @Test
    void format_type_names_an_array_by_its_element_and_modifier() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT format_type('varchar[]'::regtype, 14)", "character varying(10)[]");
            assertRow(s, "SELECT format_type('numeric[]'::regtype, 655366)", "numeric(10,2)[]");
            assertRow(s, "SELECT format_type(1015, 14)", "character varying(10)[]");
            assertRow(s, "SELECT format_type(1185, 3)", "timestamp(3) with time zone[]");
            // A regtype-cast argument is a type name, not a number to be parsed.
            assertRow(s, "SELECT format_type('int4'::regtype, -1)", "integer");
            // There is no type to name for a NULL OID.
            assertRow(s, "SELECT format_type(NULL, NULL)", (String) null);
        }
    }

    /**
     * format_type has to name the pseudo-types too. A routine's signature is declared over them
     * far more often than a column is — an input function reads a cstring, a receive function is
     * handed internal, a trigger function answers trigger — so a renderer that only knows the
     * types a column can have prints "unknown" over most of pg_proc. Values from PostgreSQL 18.
     */
    @Test
    void format_type_names_the_pseudo_types_and_the_bootstrap_types() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT format_type(2275, NULL)", "cstring");
            assertRow(s, "SELECT format_type(1263, NULL)", "cstring[]");
            assertRow(s, "SELECT format_type(2281, NULL)", "internal");
            assertRow(s, "SELECT format_type(2279, NULL)", "trigger");
            assertRow(s, "SELECT format_type(3838, NULL)", "event_trigger");
            assertRow(s, "SELECT format_type(2280, NULL)", "language_handler");
            assertRow(s, "SELECT format_type(3115, NULL)", "fdw_handler");
            assertRow(s, "SELECT format_type(325, NULL)", "index_am_handler");
            assertRow(s, "SELECT format_type(269, NULL)", "table_am_handler");
            assertRow(s, "SELECT format_type(3310, NULL)", "tsm_handler");
            assertRow(s, "SELECT format_type(2249, NULL)", "record");
            assertRow(s, "SELECT format_type(2278, NULL)", "void");
            assertRow(s, "SELECT format_type(2283, NULL)", "anyelement");
            assertRow(s, "SELECT format_type(2277, NULL)", "anyarray");
            assertRow(s, "SELECT format_type(5077, NULL)", "anycompatible");
            assertRow(s, "SELECT format_type(5078, NULL)", "anycompatiblearray");
            // "any" is a reserved word, so PG's format_type quotes what it prints.
            assertRow(s, "SELECT format_type(2276, NULL)", "\"any\"");
            // The bootstrap types: unknown is what an unadorned literal still is, refcursor what
            // a cursor variable holds, gtsvector what tsvector's GiST opclass stores.
            assertRow(s, "SELECT format_type(705, NULL)", "unknown");
            assertRow(s, "SELECT format_type(1790, NULL)", "refcursor");
            assertRow(s, "SELECT format_type(2201, NULL)", "refcursor[]");
            assertRow(s, "SELECT format_type(3642, NULL)", "gtsvector");
            assertRow(s, "SELECT format_type(3644, NULL)", "gtsvector[]");
            // An unmodified bpchar is named "character": format_type prints the spelling a
            // client could write back, which is why PG's identity form of length(bpchar) reads
            // length(character).
            assertRow(s, "SELECT format_type(1042, NULL)", "character");
            assertRow(s, "SELECT format_type(1042, 9)", "character(5)");
            // No type at all is a dash; an OID with no type behind it is deliberately not a name.
            assertRow(s, "SELECT format_type(0, NULL)", "-");
            assertRow(s, "SELECT format_type(999999, NULL)", "???");
        }
    }

    /**
     * Every type memgres registers has to name itself. A type that reached pg_type without
     * reaching format_type's own table used to print as the word "unknown" wherever a client
     * asked what it was — including through pg_get_function_arguments, which is how a driver
     * lists what the server can do.
     */
    @Test
    void every_registered_type_names_itself() throws SQLException {
        try (Statement s = conn.createStatement()) {
            List<String> nameless = new ArrayList<>();
            try (ResultSet rs = s.executeQuery(
                    "SELECT typname, format_type(oid, NULL) FROM pg_type "
                    + "WHERE typname <> 'unknown' AND format_type(oid, NULL) = 'unknown' "
                    + "ORDER BY typname")) {
                while (rs.next()) nameless.add(rs.getString(1));
            }
            assertEquals(List.of(), nameless, "pg_type rows format_type cannot name");
            // ... and no rendered signature in pg_proc falls back to it either. The type actually
            // named unknown is excluded the same way the query above excludes it: its own four I/O
            // functions are declared over it, so their signatures say "unknown" because that is
            // the type's name and not because the renderer ran out of names. PostgreSQL 18 answers
            // the same four rows for the unfiltered query (unknownin, unknownout, unknownrecv,
            // unknownsend, at OIDs 109, 110, 2416 and 2417) and zero for this one.
            assertRow(s, "SELECT count(*)::text FROM pg_proc "
                    + "WHERE (pg_get_function_arguments(oid) LIKE '%unknown%' "
                    + "OR pg_get_function_result(oid) LIKE '%unknown%') "
                    + "AND proname NOT IN "
                    + "('unknownin', 'unknownout', 'unknownrecv', 'unknownsend')", "0");
        }
    }

    // ---- domains ---------------------------------------------------------------------------

    @Test
    void a_column_of_a_domain_is_a_column_of_the_domains_base_type() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN cg_dv AS varchar(9)");
            s.execute("CREATE DOMAIN cg_dn AS numeric(7,3)");
            s.execute("CREATE DOMAIN cg_dts AS timestamp(2)");
            s.execute("CREATE TABLE cg_dc (a cg_dv, b cg_dn, c cg_dts)");
            assertEquals(List.of("a=9/null/null/null", "b=null/7/3/null", "c=null/null/null/2"),
                    join(s, "SELECT column_name, character_maximum_length, numeric_precision, "
                            + "numeric_scale, datetime_precision FROM information_schema.columns "
                            + "WHERE table_name = 'cg_dc' ORDER BY column_name", 5));
            // The width is enforced, not just reported.
            SQLException tooLong = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO cg_dc (a) VALUES ('0123456789012')"));
            assertEquals("22001", tooLong.getSQLState());
            s.execute("DROP TABLE cg_dc");
            s.execute("DROP DOMAIN cg_dv");
            s.execute("DROP DOMAIN cg_dn");
            s.execute("DROP DOMAIN cg_dts");
        }
    }

    @Test
    void an_interval_domain_keeps_its_field_qualifier() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN cg_divl AS interval day to second(3)");
            s.execute("CREATE DOMAIN cg_divl2 AS interval year to month");
            s.execute("CREATE TABLE cg_ivl (x cg_divl, y cg_divl2)");
            assertEquals(List.of("cg_divl=DAY TO SECOND(3)/null", "cg_divl2=YEAR TO MONTH/null"),
                    triples(s, "SELECT domain_name, interval_type, interval_precision "
                            + "FROM information_schema.domains WHERE domain_name LIKE 'cg_divl%' ORDER BY 1"));
            assertEquals(List.of("x=DAY TO SECOND(3)", "y=YEAR TO MONTH"),
                    pairs(s, "SELECT column_name, interval_type FROM information_schema.columns "
                            + "WHERE table_name = 'cg_ivl' ORDER BY 1"));
            s.execute("DROP TABLE cg_ivl");
            s.execute("DROP DOMAIN cg_divl");
            s.execute("DROP DOMAIN cg_divl2");
        }
    }

    @Test
    void a_domain_over_an_array_is_described_as_the_array() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN cg_darr AS integer[]");
            assertRow(s, "SELECT data_type, udt_schema, udt_name, numeric_precision "
                            + "FROM information_schema.domains WHERE domain_name = 'cg_darr'",
                    "integer[]", "pg_catalog", "_int4", null);
            assertRow(s, "SELECT typbasetype::regtype::text FROM pg_type WHERE typname = 'cg_darr'",
                    "integer[]");
            s.execute("DROP DOMAIN cg_darr");
        }
    }

    @Test
    void a_domain_and_a_sequence_live_in_the_schema_they_were_created_in() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA cg_s1");
            s.execute("CREATE DOMAIN cg_s1.d1 AS varchar(6)");
            s.execute("CREATE SEQUENCE cg_s1.sq1");
            assertRow(s, "SELECT domain_schema, domain_name FROM information_schema.domains "
                    + "WHERE domain_name = 'd1'", "cg_s1", "d1");
            assertRow(s, "SELECT sequence_schema, sequence_name FROM information_schema.sequences "
                    + "WHERE sequence_name = 'sq1'", "cg_s1", "sq1");
            s.execute("DROP SCHEMA cg_s1 CASCADE");
        }
    }

    // ---- constraints -----------------------------------------------------------------------

    @Test
    void a_check_names_the_columns_its_expression_reads() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cg_ck (a int, b int, CONSTRAINT cg_ck_ck CHECK (a > 0))");
            assertRow(s, "SELECT conname, conkey::text FROM pg_constraint WHERE conname = 'cg_ck_ck'",
                    "cg_ck_ck", "{1}");
            assertRow(s, "SELECT table_name, column_name FROM information_schema.constraint_column_usage "
                    + "WHERE constraint_name = 'cg_ck_ck'", "cg_ck", "a");
            s.execute("DROP TABLE cg_ck");
        }
    }

    @Test
    void nulls_not_distinct_survives_into_every_catalog_that_describes_it() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cg_nd (a int, b int)");
            s.execute("ALTER TABLE cg_nd ADD CONSTRAINT cg_nd_a UNIQUE NULLS NOT DISTINCT (a)");
            assertRow(s, "SELECT c.relname, i.indnullsnotdistinct FROM pg_index i "
                    + "JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_class t ON t.oid = i.indrelid "
                    + "WHERE t.relname = 'cg_nd'", "cg_nd_a", "t");
            assertRow(s, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'cg_nd_a'",
                    "UNIQUE NULLS NOT DISTINCT (a)");
            assertRow(s, "SELECT indexdef FROM pg_indexes WHERE tablename = 'cg_nd'",
                    "CREATE UNIQUE INDEX cg_nd_a ON public.cg_nd USING btree (a) NULLS NOT DISTINCT");
            assertRow(s, "SELECT nulls_distinct FROM information_schema.table_constraints "
                    + "WHERE constraint_name = 'cg_nd_a'", "NO");
            s.execute("DROP TABLE cg_nd");
        }
    }

    // ---- triggers --------------------------------------------------------------------------

    @Test
    void pg_trigger_holds_what_a_trigger_was_declared_with() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cg_trg (id int)");
            s.execute("CREATE FUNCTION cg_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql");
            s.execute("CREATE TRIGGER cg_t1 BEFORE INSERT ON cg_trg FOR EACH ROW EXECUTE FUNCTION cg_tf()");
            s.execute("CREATE TRIGGER cg_t2 AFTER UPDATE OR DELETE ON cg_trg "
                    + "FOR EACH STATEMENT EXECUTE FUNCTION cg_tf()");
            assertEquals(List.of("cg_t1=0/7/O", "cg_t2=0/24/O"),
                    quads(s, "SELECT tgname, tgnargs, tgtype, tgenabled FROM pg_trigger "
                            + "WHERE tgname LIKE 'cg_t%' ORDER BY tgname"));
            assertEquals(List.of("cg_t1=0/null", "cg_t2=0/null"),
                    triples(s, "SELECT tgname, tgconstrindid, tgqual FROM pg_trigger "
                            + "WHERE tgname LIKE 'cg_t%' ORDER BY tgname"));
            assertEquals(List.of(
                            "cg_t1=CREATE TRIGGER cg_t1 BEFORE INSERT ON cg_trg "
                                    + "FOR EACH ROW EXECUTE FUNCTION cg_tf()",
                            "cg_t2=CREATE TRIGGER cg_t2 AFTER DELETE OR UPDATE ON cg_trg "
                                    + "FOR EACH STATEMENT EXECUTE FUNCTION cg_tf()"),
                    pairs(s, "SELECT tgname, pg_get_triggerdef(oid) FROM pg_trigger "
                            + "WHERE tgname LIKE 'cg_t%' ORDER BY tgname"));
            s.execute("DROP TABLE cg_trg CASCADE");
            s.execute("DROP FUNCTION cg_tf()");
        }
    }

    // ---- the catalogs describe themselves ---------------------------------------------------

    @Test
    void the_char_type_is_a_type_of_its_own() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertEquals(List.of("_char=A/b/-1", "char=Z/b/1"),
                    quads(s, "SELECT typname, typcategory, typtype, typlen FROM pg_type "
                            + "WHERE typname IN ('char','_char') ORDER BY 1"));
            // No array type points at an element the catalog does not hold.
            assertEquals(List.of(), column(s, "SELECT t.typname FROM pg_type t WHERE t.typelem <> 0 "
                    + "AND NOT EXISTS (SELECT 1 FROM pg_type e WHERE e.oid = t.typelem) ORDER BY 1"));
        }
    }

    @Test
    void information_schema_describes_a_catalog_with_the_columns_it_has() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // attbyval is a column of pg_attribute; xmin and ctid are not described here.
            assertEquals(List.of("attbyval"),
                    column(s, "SELECT column_name FROM information_schema.columns "
                            + "WHERE table_schema = 'pg_catalog' AND table_name = 'pg_attribute' "
                            + "AND column_name IN ('attbyval','xmin','ctid') ORDER BY column_name"));
            assertRow(s, "SELECT count(*) FROM information_schema.columns "
                    + "WHERE table_schema = 'pg_catalog' AND table_name = 'pg_attribute'", "25");
            assertRow(s, "SELECT count(*) FROM information_schema.columns "
                    + "WHERE table_schema = 'pg_catalog' AND table_name = 'pg_trigger'", "19");
            assertRow(s, "SELECT count(*) FROM information_schema.columns "
                    + "WHERE table_schema = 'pg_catalog' AND table_name = 'pg_class'", "34");
        }
    }

    @Test
    void pg_attribute_and_pg_class_hold_rows_for_the_catalogs_themselves() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                    + "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'pg_catalog' "
                    + "AND c.relname = 'pg_type' AND a.attnum > 0", "32");
            assertRow(s, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE n.nspname = 'information_schema' AND c.relname = 'columns'", "1");
        }
    }

    @Test
    void a_catalog_relation_is_described_through_jdbc_metadata() throws SQLException {
        List<String> names = new ArrayList<>();
        try (ResultSet rs = conn.getMetaData().getColumns(null, "pg_catalog", "pg_trigger", null)) {
            while (rs.next()) names.add(rs.getString("COLUMN_NAME"));
        }
        assertEquals(19, names.size(), "pg_trigger's columns through getColumns: " + names);
        assertTrue(names.contains("tgnargs"), "getColumns must list tgnargs: " + names);
        assertTrue(names.contains("tgqual"), "getColumns must list tgqual: " + names);
    }

    // ---- information_schema.tables ----------------------------------------------------------

    @Test
    void tables_and_views_agree_about_an_insertable_view() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE cg_v1 (id int primary key, a int)");
            s.execute("CREATE VIEW cg_vv AS SELECT id, a FROM cg_v1");
            assertRow(s, "SELECT is_insertable_into FROM information_schema.tables "
                    + "WHERE table_name = 'cg_vv'", "YES");
            assertRow(s, "SELECT is_insertable_into FROM information_schema.views "
                    + "WHERE table_name = 'cg_vv'", "YES");
            s.execute("INSERT INTO cg_vv VALUES (1, 2)");
            assertRow(s, "SELECT count(*) FROM cg_v1", "1");
            assertRow(s, "SELECT table_type, is_insertable_into FROM information_schema.tables "
                    + "WHERE table_schema = 'pg_catalog' AND table_name = 'pg_class'",
                    "BASE TABLE", "YES");
            s.execute("DROP VIEW cg_vv");
            s.execute("DROP TABLE cg_v1");
        }
    }

    @Test
    void a_temporary_table_has_a_table_type_of_its_own() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TEMP TABLE cg_tmp (x int)");
            assertRow(s, "SELECT table_type FROM information_schema.tables "
                    + "WHERE table_name = 'cg_tmp'", "LOCAL TEMPORARY");
            s.execute("DROP TABLE cg_tmp");
        }
    }

    // ---- views belong to schemas -------------------------------------------------------------

    @Test
    void two_schemas_may_each_hold_a_view_of_the_same_name() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA cg_s2");
            s.execute("CREATE TABLE cg_base (id int, code text)");
            s.execute("INSERT INTO cg_base VALUES (1,'a'), (2,'b')");
            s.execute("CREATE TABLE cg_s2.t (a int)");
            s.execute("CREATE VIEW public.cg_shared AS SELECT id, code FROM cg_base");
            // The name is free in the other schema.
            s.execute("CREATE VIEW cg_s2.cg_shared AS SELECT a FROM cg_s2.t");
            assertEquals(List.of("cg_s2=cg_shared", "public=cg_shared"),
                    pairs(s, "SELECT table_schema, table_name FROM information_schema.views "
                            + "WHERE table_name = 'cg_shared' ORDER BY table_schema"));
            // A qualified read reaches the view in the schema it names.
            assertEquals(List.of("a"), columnLabels(s, "SELECT * FROM cg_s2.cg_shared"));
            assertEquals(List.of("id", "code"), columnLabels(s, "SELECT * FROM public.cg_shared"));
            s.execute("DROP VIEW cg_s2.cg_shared");
            s.execute("DROP VIEW public.cg_shared");
            s.execute("DROP SCHEMA cg_s2 CASCADE");
            s.execute("DROP TABLE cg_base");
        }
    }

    // ---- SET is judged by what pg_settings reports --------------------------------------------

    @Test
    void a_preset_cannot_be_assigned() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertState(s, "SET block_size = 4096", "55P02");
            assertState(s, "SET wal_segment_size = 1", "55P02");
            assertState(s, "SET data_checksums = on", "55P02");
            assertState(s, "SET server_version = '9.0'", "55P02");
            assertState(s, "SET shared_buffers = '64MB'", "55P02");
            assertState(s, "SET wal_level = 'logical'", "55P02");
            assertState(s, "SET autovacuum = off", "55P02");
        }
    }

    @Test
    void an_enum_takes_one_of_the_values_pg_settings_lists() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertState(s, "SET client_min_messages = 'bogus'", "22023");
            assertState(s, "SET log_min_messages = 'bogus'", "22023");
            assertState(s, "SET default_toast_compression = 'gzip'", "22023");
            assertState(s, "SET session_replication_role = 'bogus'", "22023");
            assertState(s, "SET array_nulls = 'maybe'", "22023");
            // synchronous_commit is an enum, not a boolean, so remote_apply is one of its values.
            s.execute("SET synchronous_commit = 'remote_apply'");
            assertRow(s, "SHOW synchronous_commit", "remote_apply");
            s.execute("RESET synchronous_commit");
            // An enum value reads back in the spelling its own list uses.
            s.execute("SET client_min_messages = 'WARNING'");
            assertRow(s, "SHOW client_min_messages", "warning");
            s.execute("RESET client_min_messages");
        }
    }

    @Test
    void a_number_has_to_fall_inside_the_range_pg_settings_reports() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertState(s, "SET work_mem = '-1'", "22023");
            assertState(s, "SET extra_float_digits = 99", "22023");
            assertState(s, "SET deadlock_timeout = 0", "22023");
            // The values inside the range are still accepted, in any unit.
            s.execute("SET work_mem = '8MB'");
            assertRow(s, "SHOW work_mem", "8MB");
            assertRow(s, "SELECT setting, unit FROM pg_settings WHERE name = 'work_mem'", "8192", "kB");
            s.execute("RESET work_mem");
        }
    }

    // ---- helpers --------------------------------------------------------------------------

    private static void assertSyntaxError(Statement s, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> s.execute(sql), "expected: " + sql);
        assertEquals("42601", e.getSQLState(), "for: " + sql);
    }

    private static void assertState(Statement s, String sql, String sqlState) {
        SQLException e = assertThrows(SQLException.class, () -> s.execute(sql), "expected: " + sql);
        assertEquals(sqlState, e.getSQLState(), "for: " + sql);
    }

    private static void assertRow(Statement s, String sql, String... expected) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], rs.getString(i + 1), "column " + (i + 1) + " of: " + sql);
            }
            assertFalse(rs.next(), "more than one row for: " + sql);
        }
    }

    private static List<String> columnLabels(Statement s, String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData m = rs.getMetaData();
            for (int i = 1; i <= m.getColumnCount(); i++) out.add(m.getColumnLabel(i));
        }
        return out;
    }

    private static List<String> column(Statement s, String sql) throws SQLException {
        return join(s, sql, 1);
    }

    private static List<String> pairs(Statement s, String sql) throws SQLException {
        return join(s, sql, 2);
    }

    private static List<String> triples(Statement s, String sql) throws SQLException {
        return join(s, sql, 3);
    }

    private static List<String> quads(Statement s, String sql) throws SQLException {
        return join(s, sql, 4);
    }

    /** Rows rendered as "first=second/third/..." so a whole result reads as one assertion. */
    private static List<String> join(Statement s, String sql, int width) throws SQLException {
        List<String> out = new ArrayList<>();
        try (ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                StringBuilder sb = new StringBuilder(String.valueOf(rs.getString(1)));
                for (int i = 2; i <= width; i++) {
                    sb.append(i == 2 ? "=" : "/").append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }
}
