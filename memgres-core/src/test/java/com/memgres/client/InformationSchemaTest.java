package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * information_schema is the standard, portable view of a database and is what tooling reads to
 * introspect one, so a column that is missing there is a tool that cannot look. pg_settings is
 * where a client reads how to render a configuration value and whether it may change it, and
 * pg_type is where it reads how a value is physically laid out.
 *
 * <p>Checked against PostgreSQL 18.</p>
 */
class InformationSchemaTest {

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

    // ---- information_schema.domains -------------------------------------------------------

    @Test
    void domains_describes_a_varchar_domain_with_its_width_and_octet_length() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN is_vc AS varchar(12) NOT NULL DEFAULT 'x'");
            assertRow(s, "SELECT data_type, character_maximum_length, character_octet_length, "
                            + "udt_schema, udt_name, dtd_identifier, domain_default "
                            + "FROM information_schema.domains WHERE domain_name = 'is_vc'",
                    "character varying", "12", "48", "pg_catalog", "varchar", "1",
                    "'x'::character varying");
            s.execute("DROP DOMAIN is_vc");
        }
    }

    @Test
    void domains_describes_a_numeric_domain_with_radix_and_scale() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN is_num AS numeric(10,2)");
            assertRow(s, "SELECT numeric_precision, numeric_precision_radix, numeric_scale, "
                            + "character_maximum_length FROM information_schema.domains "
                            + "WHERE domain_name = 'is_num'",
                    "10", "10", "2", null);
            s.execute("DROP DOMAIN is_num");
        }
    }

    @Test
    void domains_reports_a_temporal_domains_typmod_as_its_datetime_precision() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN is_ts AS timestamp(3)");
            assertRow(s, "SELECT data_type, datetime_precision, interval_type, interval_precision "
                            + "FROM information_schema.domains WHERE domain_name = 'is_ts'",
                    "timestamp without time zone", "3", null, null);
            s.execute("DROP DOMAIN is_ts");
        }
    }

    @Test
    void domains_carries_the_scope_and_cardinality_columns_the_standard_declares() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DOMAIN is_scope AS text");
            assertRow(s, "SELECT scope_catalog, scope_schema, scope_name, maximum_cardinality, "
                            + "character_set_name, collation_name, character_octet_length "
                            + "FROM information_schema.domains WHERE domain_name = 'is_scope'",
                    null, null, null, null, null, null, "1073741824");
            s.execute("DROP DOMAIN is_scope");
        }
    }

    // ---- information_schema.columns -------------------------------------------------------

    @Test
    void datetime_precision_follows_the_columns_typmod() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_dt (a timestamp, b timestamp(3), c time(2), "
                    + "d timestamptz(1), e date, f timetz(4), g interval, h interval(2))");
            assertEquals(
                    List.of("a=6", "b=3", "c=2", "d=1", "e=0", "f=4", "g=6", "h=2"),
                    pairs(s, "SELECT column_name, datetime_precision FROM information_schema.columns "
                            + "WHERE table_name = 'is_dt' ORDER BY ordinal_position"));
            s.execute("DROP TABLE is_dt");
        }
    }

    @Test
    void a_timetz_column_does_not_break_the_columns_view() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_ttz (a timetz)");
            assertRow(s, "SELECT data_type, udt_name FROM information_schema.columns "
                            + "WHERE table_name = 'is_ttz'",
                    "time with time zone", "timetz");
            s.execute("DROP TABLE is_ttz");
        }
    }

    @Test
    void an_array_column_reports_ARRAY_and_its_array_udt_name() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_arr (a int[], b timestamptz[], c numeric(8,3)[])");
            assertEquals(List.of("a=ARRAY/_int4", "b=ARRAY/_timestamptz", "c=ARRAY/_numeric"),
                    triples(s, "SELECT column_name, data_type, udt_name FROM information_schema.columns "
                            + "WHERE table_name = 'is_arr' ORDER BY ordinal_position"));
            s.execute("DROP TABLE is_arr");
        }
    }

    @Test
    void plain_numeric_reports_a_decimal_radix_with_no_precision() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_num2 (a numeric, b numeric(6,2))");
            assertEquals(List.of("a=null/10/null", "b=6/10/2"),
                    quads(s, "SELECT column_name, numeric_precision, numeric_precision_radix, "
                            + "numeric_scale FROM information_schema.columns "
                            + "WHERE table_name = 'is_num2' ORDER BY ordinal_position"));
            s.execute("DROP TABLE is_num2");
        }
    }

    // ---- interval field qualifiers --------------------------------------------------------

    @Test
    void an_interval_column_may_declare_which_fields_it_keeps() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_ivl (a interval year to month, b interval day to second(3), "
                    + "c interval hour, d interval minute to second)");
            assertEquals(List.of("a=YEAR TO MONTH/6", "b=DAY TO SECOND(3)/3",
                            "c=HOUR/6", "d=MINUTE TO SECOND/6"),
                    triples(s, "SELECT column_name, interval_type, datetime_precision "
                            + "FROM information_schema.columns WHERE table_name = 'is_ivl' "
                            + "ORDER BY ordinal_position"));
            s.execute("DROP TABLE is_ivl");
        }
    }

    @Test
    void an_interval_qualifier_drops_the_fields_it_does_not_reach() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_ivl2 (a interval year to month, b interval hour)");
            s.execute("INSERT INTO is_ivl2 VALUES ('1 year 2 months 3 days 4:05:06.789', "
                    + "'1 year 2 months 3 days 4:05:06.789')");
            assertRow(s, "SELECT a::text, b::text FROM is_ivl2",
                    "1 year 2 mons", "1 year 2 mons 3 days 04:00:00");
            s.execute("DROP TABLE is_ivl2");
        }
    }

    @Test
    void a_plain_interval_column_reports_no_qualifier() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_ivl3 (a interval, b interval(4))");
            assertEquals(List.of("a=null/6", "b=null/4"),
                    triples(s, "SELECT column_name, interval_type, datetime_precision "
                            + "FROM information_schema.columns WHERE table_name = 'is_ivl3' "
                            + "ORDER BY ordinal_position"));
            s.execute("DROP TABLE is_ivl3");
        }
    }

    // ---- the remaining missing columns ----------------------------------------------------

    @Test
    void tables_carries_commit_action_and_leaves_it_null_for_a_permanent_table() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_ca (a int)");
            assertRow(s, "SELECT table_type, commit_action FROM information_schema.tables "
                    + "WHERE table_name = 'is_ca'", "BASE TABLE", null);
            s.execute("DROP TABLE is_ca");
        }
    }

    @Test
    void table_constraints_answers_nulls_distinct_only_for_a_uniqueness_constraint() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_nd (id int PRIMARY KEY, u int UNIQUE, "
                    + "w int UNIQUE NULLS NOT DISTINCT)");
            assertEquals(List.of("is_nd_pkey=null", "is_nd_u_key=YES", "is_nd_w_key=NO"),
                    pairs(s, "SELECT constraint_name, nulls_distinct "
                            + "FROM information_schema.table_constraints WHERE table_name = 'is_nd' "
                            + "AND constraint_type IN ('PRIMARY KEY','UNIQUE') ORDER BY constraint_name"));
            s.execute("DROP TABLE is_nd");
        }
    }

    @Test
    void sequences_reports_the_radix_and_scale_of_its_counter() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SEQUENCE is_seq");
            assertRow(s, "SELECT data_type, numeric_precision, numeric_precision_radix, numeric_scale "
                            + "FROM information_schema.sequences WHERE sequence_name = 'is_seq'",
                    "bigint", "64", "2", "0");
            s.execute("DROP SEQUENCE is_seq");
        }
    }

    @Test
    void schemata_carries_sql_path() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT sql_path FROM information_schema.schemata "
                    + "WHERE schema_name = 'public'", (String) null);
        }
    }

    @Test
    void triggers_carries_the_row_variable_columns() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION is_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ "
                    + "LANGUAGE plpgsql");
            s.execute("CREATE TABLE is_trg (id int)");
            s.execute("CREATE TRIGGER is_trigger BEFORE INSERT ON is_trg FOR EACH ROW "
                    + "EXECUTE FUNCTION is_tf()");
            assertRow(s, "SELECT action_timing, action_reference_old_row, action_reference_new_row "
                            + "FROM information_schema.triggers WHERE trigger_name = 'is_trigger'",
                    "BEFORE", null, null);
            s.execute("DROP TABLE is_trg CASCADE");
            s.execute("DROP FUNCTION is_tf()");
        }
    }

    // ---- constraint naming ----------------------------------------------------------------

    @Test
    void an_unnamed_check_is_named_after_the_single_column_it_mentions() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_cn (a int, b int, CHECK (a > 0), CHECK (a < b), CHECK (b > 0))");
            assertEquals(List.of("is_cn_a_check", "is_cn_b_check", "is_cn_check"),
                    column(s, "SELECT constraint_name FROM information_schema.table_constraints "
                            + "WHERE table_name = 'is_cn' AND constraint_type = 'CHECK' "
                            + "AND constraint_name NOT LIKE '%not_null' ORDER BY constraint_name"));
            s.execute("DROP TABLE is_cn");
        }
    }

    @Test
    void a_repeated_generated_constraint_name_takes_a_numeric_suffix() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE is_cn2 (a int, b int, CHECK (a < b), CHECK (a <> b))");
            assertEquals(List.of("is_cn2_check", "is_cn2_check1"),
                    column(s, "SELECT constraint_name FROM information_schema.table_constraints "
                            + "WHERE table_name = 'is_cn2' AND constraint_type = 'CHECK' "
                            + "AND constraint_name NOT LIKE '%not_null' ORDER BY constraint_name"));
            s.execute("DROP TABLE is_cn2");
        }
    }

    // ---- pg_settings ----------------------------------------------------------------------

    @Test
    void a_settings_metadata_describes_that_setting_and_not_settings_in_general() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT vartype, context, unit, min_val, max_val, boot_val "
                            + "FROM pg_settings WHERE name = 'work_mem'",
                    "integer", "user", "kB", "64", "2147483647", "4096");
            assertRow(s, "SELECT vartype, context FROM pg_settings WHERE name = 'array_nulls'",
                    "bool", "user");
            assertRow(s, "SELECT vartype, context FROM pg_settings WHERE name = 'archive_mode'",
                    "enum", "postmaster");
            assertRow(s, "SELECT vartype, context FROM pg_settings WHERE name = 'block_size'",
                    "integer", "internal");
            assertRow(s, "SELECT vartype, context FROM pg_settings WHERE name = 'autovacuum'",
                    "bool", "sighup");
            assertRow(s, "SELECT vartype, context FROM pg_settings WHERE name = 'bgwriter_lru_multiplier'",
                    "real", "sighup");
        }
    }

    @Test
    void no_setting_is_left_in_the_catch_all_category() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT count(*)::int FROM pg_settings WHERE category = 'Ungrouped'", "0");
            assertRow(s, "SELECT count(*)::int FROM pg_settings "
                    + "WHERE short_desc IS NULL OR short_desc = ''", "0");
        }
    }

    @Test
    void an_enums_permitted_values_are_quoted_where_a_value_needs_it() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT enumvals::text FROM pg_settings "
                            + "WHERE name = 'default_transaction_isolation'",
                    "{serializable,\"repeatable read\",\"read committed\",\"read uncommitted\"}");
            assertRow(s, "SELECT enumvals::text FROM pg_settings WHERE name = 'bytea_output'",
                    "{escape,hex}");
        }
    }

    @Test
    void a_setting_with_a_unit_stores_its_base_value_and_displays_a_unit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT setting, current_setting('work_mem') FROM pg_settings "
                    + "WHERE name = 'work_mem'", "4096", "4MB");
            assertRow(s, "SELECT current_setting('autovacuum_naptime')", "1min");
            assertRow(s, "SELECT current_setting('segment_size')", "1GB");
            assertRow(s, "SELECT current_setting('wal_segment_size')", "16MB");
            assertRow(s, "SELECT current_setting('track_activity_query_size')", "1kB");
        }
    }

    @Test
    void a_written_unit_is_converted_into_the_unit_the_setting_counts_in() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("SET work_mem = '8MB'");
            assertRow(s, "SELECT setting, boot_val, reset_val, source FROM pg_settings "
                    + "WHERE name = 'work_mem'", "8192", "4096", "4096", "session");
            assertRow(s, "SELECT current_setting('work_mem')", "8MB");
            s.execute("RESET work_mem");
            assertRow(s, "SELECT setting, source FROM pg_settings WHERE name = 'work_mem'",
                    "4096", "default");

            s.execute("SET statement_timeout = '5s'");
            assertRow(s, "SELECT setting FROM pg_settings WHERE name = 'statement_timeout'", "5000");
            assertRow(s, "SELECT current_setting('statement_timeout')", "5s");
            s.execute("RESET statement_timeout");
            assertRow(s, "SELECT current_setting('statement_timeout')", "0");
        }
    }

    @Test
    void the_settings_postgres_keeps_out_of_the_listing_are_not_listed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT count(*)::int FROM pg_settings "
                    + "WHERE name IN ('role','session_authorization','is_superuser')", "0");
            // They are still readable, which is the whole point of the exclusion.
            assertRow(s, "SELECT current_setting('is_superuser')", "on");
        }
    }

    // ---- pg_type --------------------------------------------------------------------------

    @Test
    void an_arrays_alignment_is_recorded_rather_than_guessed() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertEquals(List.of("_float8=d", "_int8=d", "_interval=d", "_text=i", "_timestamptz=d"),
                    pairs(s, "SELECT typname, typalign FROM pg_type WHERE typname IN "
                            + "('_float8','_int8','_interval','_text','_timestamptz') ORDER BY typname"));
        }
    }

    @Test
    void a_box_array_is_delimited_the_way_a_box_is() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT typdelim, typalign FROM pg_type WHERE typname = '_box'", ";", "d");
            assertRow(s, "SELECT typdelim FROM pg_type WHERE typname = 'box'", ";");
        }
    }

    @Test
    void name_and_its_array_sort_under_the_C_collation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT typcollation FROM pg_type WHERE typname = '_name'", "950");
            assertRow(s, "SELECT typcollation FROM pg_type WHERE typname = 'name'", "950");
        }
    }

    @Test
    void aclitem_is_a_sixteen_byte_double_aligned_struct() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertRow(s, "SELECT typlen, typbyval, typalign FROM pg_type WHERE typname = 'aclitem'",
                    "16", "f", "d");
        }
    }

    @Test
    void a_varlena_pseudo_type_is_never_passed_by_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertEquals(List.of("_record=-1/f/p", "anyarray=-1/f/p", "record=-1/f/p"),
                    quads(s, "SELECT typname, typlen, typbyval, typtype FROM pg_type "
                            + "WHERE typname IN ('anyarray','record','_record') ORDER BY typname"));
            assertRow(s, "SELECT typlen, typbyval FROM pg_type WHERE typname = 'cstring'", "-2", "f");
        }
    }

    @Test
    void a_geometric_types_length_and_category_are_its_own() throws SQLException {
        try (Statement s = conn.createStatement()) {
            assertEquals(List.of("box=32/G", "circle=24/G", "line=24/G", "lseg=32/G", "point=16/G"),
                    triples(s, "SELECT typname, typlen, typcategory FROM pg_type WHERE typname IN "
                            + "('box','circle','line','lseg','point') ORDER BY typname"));
        }
    }

    // ---- helpers --------------------------------------------------------------------------

    private static void assertRow(Statement s, String sql, String... expected) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row for: " + sql);
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], rs.getString(i + 1),
                        "column " + (i + 1) + " of: " + sql);
            }
            assertFalse(rs.next(), "more than one row for: " + sql);
        }
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
