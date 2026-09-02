package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Three things a client reads a modifier out of, and used to read wrongly.
 *
 * <p>An interval literal's two-part time field is HOUR:MINUTE only until its second part carries
 * a fraction, at which point it is MINUTE:SECOND — so {@code '3:04'} is three hours and four
 * minutes while {@code '3:04.5'} is three minutes and four and a half seconds. An interval
 * typmod's high half is a field mask, so a number that names no qualifier is not a narrower
 * interval but no type modifier at all. And a trigger's arguments and UPDATE OF column list are
 * catalog content: pg_dump reads them back out of tgargs and tgattr, and a foreign key puts four
 * internal triggers of its own in pg_trigger.
 *
 * <p>Every expected value here was measured on PostgreSQL 18.</p>
 */
class IntervalFieldsAndTriggerCatalogTest {

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

    // ---- a two-part time field with a fraction is MINUTE:SECOND -----------------------------

    @Test
    void a_fraction_makes_a_two_part_time_field_minutes_and_seconds() throws SQLException {
        // Without a fraction the two parts are hours and minutes ...
        assertInterval("03:04:00", "SELECT CAST('3:04' AS interval)::text");
        // ... and with one they are minutes and seconds, because only the seconds field can
        // carry a fraction at all.
        assertInterval("00:03:04.5678", "SELECT CAST('3:04.5678' AS interval)::text");
        assertInterval("00:03:04.5", "SELECT CAST('3:04.5' AS interval)::text");
        // The sign on the first part covers the whole field, fraction included.
        assertInterval("-00:03:04.5678", "SELECT CAST('-3:04.5678' AS interval)::text");
        // A trailing dot is an empty fraction and still switches the reading.
        assertInterval("00:03:04", "SELECT CAST('3:04.' AS interval)::text");
        // Sixty seconds is in range, and carries into the minutes.
        assertInterval("00:04:00.5", "SELECT CAST('3:60.5' AS interval)::text");
        // The fraction rounds to microseconds and may round up a whole second.
        assertInterval("00:12:35", "SELECT CAST('12:34.9999995' AS interval)::text");
        // Three parts are always hours, minutes and seconds.
        assertInterval("03:04:05.5678", "SELECT CAST('3:04:05.5678' AS interval)::text");
    }

    @Test
    void a_time_field_sits_among_the_other_fields() throws SQLException {
        assertInterval("1 day 00:03:04.5678", "SELECT CAST('1 day 3:04.5678' AS interval)::text");
        assertInterval("1 day 00:03:04.5678", "SELECT CAST('3:04.5678 1 day' AS interval)::text");
        assertInterval("-00:03:04.5678", "SELECT CAST('3:04.5678 ago' AS interval)::text");
        assertInterval("00:03:04.5678", "SELECT CAST('@ 3:04.5678' AS interval)::text");
        // An unlabelled number in front of a time of day is a day count ...
        assertInterval("3 days 00:04:05.678", "SELECT CAST('3 4:05.678' AS interval)::text");
        assertInterval("1 year 2 mons 3 days 00:04:05.678",
                "SELECT CAST('1-2 3 4:05.678' AS interval)::text");
        // ... but with no time of day after it, it is a count of seconds.
        assertInterval("1 year 2 mons 00:00:03", "SELECT CAST('1-2 3' AS interval)::text");
        // A unit word to the right of a time field names nothing and is ignored.
        assertInterval("00:03:04.5678", "SELECT CAST('3:04.5678 hour' AS interval)::text");
    }

    @Test
    void a_time_field_is_bounded_field_by_field() throws SQLException {
        // Minutes may not exceed 59, whichever way the field is read.
        assertFails("22015", "interval field value out of range",
                "SELECT CAST('100:04.5' AS interval)");
        assertFails("22015", "interval field value out of range",
                "SELECT CAST('3:60' AS interval)");
        assertFails("22015", "interval field value out of range",
                "SELECT CAST('3:70:05' AS interval)");
        // Seconds may reach 60 but not pass it.
        assertFails("22015", "interval field value out of range",
                "SELECT CAST('1:2:61' AS interval)");
        assertInterval("01:03:00", "SELECT CAST('1:2:60' AS interval)::text");
    }

    @Test
    void no_interval_field_may_be_written_twice() throws SQLException {
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 hour 2 hour' AS interval)");
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 day 1 day' AS interval)");
        // Two different unit words are two different fields, so these stand.
        assertInterval("9 days", "SELECT CAST('1 week 2 days' AS interval)::text");
        assertInterval("00:00:01.5", "SELECT CAST('1 second 500 milliseconds' AS interval)::text");
        assertInterval("12 years", "SELECT CAST('1 decade 2 years' AS interval)::text");
    }

    @Test
    void a_field_qualifier_reads_the_same_literal() throws SQLException {
        assertInterval("00:03:04.5678",
                "SELECT CAST('3:04.5678' AS interval minute to second)::text");
        assertInterval("00:03:04.57",
                "SELECT CAST('3:04.5678' AS interval minute to second(2))::text");
        assertInterval("00:03:00",
                "SELECT CAST('3:04.5678' AS interval hour to minute)::text");
        assertInterval("00:03:04.568",
                "SELECT CAST('3:04.5678' AS interval hour to second(3))::text");
        assertInterval("00:03:04.57",
                "SELECT CAST('3:04.5678' AS interval day to second(2))::text");
        assertInterval("00:00:00",
                "SELECT CAST('3:04.5678' AS interval day to hour)::text");
        assertInterval("1 day 00:03:04.5678",
                "SELECT CAST('1 3:04.5678' AS interval day to second)::text");
        // Without a fraction, MINUTE TO SECOND is the only qualifier that reads two parts as
        // minutes and seconds.
        assertInterval("00:03:04", "SELECT CAST('3:04' AS interval minute to second)::text");
        assertInterval("03:04:00", "SELECT CAST('3:04' AS interval hour to second)::text");
        // The written-literal spelling agrees with the cast.
        assertInterval("00:03:04.57",
                "SELECT (INTERVAL '3:04.5678' MINUTE TO SECOND(2))::text");
    }

    // ---- format_type reads an interval typmod as a field mask -------------------------------

    @Test
    void an_interval_typmod_prints_back_as_the_qualifier_it_names() throws SQLException {
        assertScalar("interval day to second(2)", "SELECT format_type('interval'::regtype, 470286338)");
        assertScalar("interval day to second(2)[]", "SELECT format_type('interval[]'::regtype, 470286338)");
        assertScalar("interval year to month", "SELECT format_type('interval'::regtype, 458751)");
        assertScalar("interval hour to minute", "SELECT format_type('interval'::regtype, 201392127)");
        assertScalar("interval second(2)", "SELECT format_type('interval'::regtype, 268435458)");
        assertScalar("interval(4)", "SELECT format_type('interval'::regtype, 2147418116)");
        assertScalar("interval", "SELECT format_type('interval'::regtype, -1)");
    }

    @Test
    void a_typmod_that_names_no_qualifier_is_not_a_typmod() throws SQLException {
        // Printing 'interval(3)' here would name a type nothing could be declared as.
        assertFails("XX000", "invalid INTERVAL typmod: 0x3",
                "SELECT format_type('interval'::regtype, 3)");
        assertFails("XX000", "invalid INTERVAL typmod: 0x3",
                "SELECT format_type('interval[]'::regtype, 3)");
        assertFails("XX000", "invalid INTERVAL typmod: 0x0",
                "SELECT format_type('interval'::regtype, 0)");
        assertFails("XX000", "invalid INTERVAL typmod: 0x7fff",
                "SELECT format_type('interval'::regtype, 32767)");
        assertFails("XX000", "invalid INTERVAL typmod: 0xffff",
                "SELECT format_type('interval'::regtype, 65535)");
    }

    @Test
    void a_typmod_on_an_array_type_applies_to_its_element() throws SQLException {
        assertScalar("character varying(10)[]", "SELECT format_type('varchar[]'::regtype, 14)");
        assertScalar("numeric(10,2)[]", "SELECT format_type('numeric[]'::regtype, 655366)");
        assertScalar("character varying(10)", "SELECT format_type('varchar'::regtype, 14)");
        assertScalar("numeric(10,2)", "SELECT format_type('numeric'::regtype, 655366)");
        // The bit string array types are types like any other.
        assertScalar("bit(8)[]", "SELECT format_type('bit[]'::regtype, 8)");
        assertScalar("bit varying(8)[]", "SELECT format_type('varbit[]'::regtype, 8)");
        assertScalar("1561", "SELECT ('bit[]'::regtype::oid)::text");
        assertScalar("1563", "SELECT ('varbit[]'::regtype::oid)::text");
        assertScalar("1017", "SELECT ('point[]'::regtype::oid)::text");
        assertScalar("651", "SELECT ('cidr[]'::regtype::oid)::text");
    }

    @Test
    void to_regtype_answers_a_type_and_not_its_name() throws SQLException {
        assertScalar("1561", "SELECT (to_regtype('bit[]')::oid)::text");
        assertScalar("1009", "SELECT (to_regtype('text[]')::oid)::text");
        assertScalar("1015", "SELECT (to_regtype('varchar[]')::oid)::text");
        assertScalar("character varying[]", "SELECT to_regtype('varchar[]')::text");
        assertScalar(null, "SELECT to_regtype('nosuchtype[]')::text");
    }

    // ---- what a trigger records in pg_trigger ------------------------------------------------

    @Test
    void a_triggers_arguments_are_catalog_content() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tc_t (id int, v text, w text)");
            s.execute("CREATE FUNCTION tc_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            s.execute("CREATE TRIGGER tc_1 BEFORE INSERT ON tc_t FOR EACH ROW EXECUTE FUNCTION tc_f()");
            s.execute("CREATE TRIGGER tc_2 BEFORE UPDATE OF v, w ON tc_t FOR EACH ROW EXECUTE FUNCTION tc_f('x')");
            s.execute("CREATE TRIGGER tc_3 BEFORE UPDATE OF v ON tc_t FOR EACH ROW "
                    + "WHEN (OLD.v IS DISTINCT FROM NEW.v) EXECUTE FUNCTION tc_f()");
            s.execute("CREATE TRIGGER tc_4 BEFORE INSERT ON tc_t FOR EACH ROW EXECUTE FUNCTION tc_f('abc','d')");

            // The arguments are one bytea of NUL-terminated strings, and tgnargs counts them.
            assertScalar("0", "SELECT tgnargs::text FROM pg_trigger WHERE tgname = 'tc_1'");
            assertScalar("2", "SELECT tgnargs::text FROM pg_trigger WHERE tgname = 'tc_4'");
            assertScalar("abc\\000d\\000",
                    "SELECT encode(tgargs, 'escape') FROM pg_trigger WHERE tgname = 'tc_4'");
            assertScalar("6", "SELECT length(tgargs)::text FROM pg_trigger WHERE tgname = 'tc_4'");
            assertScalar("0", "SELECT length(tgargs)::text FROM pg_trigger WHERE tgname = 'tc_1'");

            // tgattr holds the attnums of an UPDATE OF list; a trigger without one has an empty
            // vector rather than a null, so array_length needs no null check.
            assertScalar("2 3", "SELECT tgattr::text FROM pg_trigger WHERE tgname = 'tc_2'");
            assertScalar("2", "SELECT tgattr::text FROM pg_trigger WHERE tgname = 'tc_3'");
            assertScalar("", "SELECT tgattr::text FROM pg_trigger WHERE tgname = 'tc_1'");
            assertScalar("2", "SELECT array_length(tgattr, 1)::text FROM pg_trigger WHERE tgname = 'tc_2'");
            assertScalar("0", "SELECT array_length(tgattr, 1)::text FROM pg_trigger WHERE tgname = 'tc_1'");

            // The reconstructed definition carries them, so a restored trigger sees the same
            // TG_NARGS and TG_ARGV the original did.
            assertScalar("CREATE TRIGGER tc_2 BEFORE UPDATE OF v, w ON tc_t "
                            + "FOR EACH ROW EXECUTE FUNCTION tc_f('x')",
                    "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'tc_2'");
            assertScalar("CREATE TRIGGER tc_4 BEFORE INSERT ON tc_t "
                            + "FOR EACH ROW EXECUTE FUNCTION tc_f('abc', 'd')",
                    "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'tc_4'");
            // A WHEN condition comes back in the catalog's spelling: lower-case row references,
            // a dot with no spaces around it, and a second pair of parentheses.
            assertScalar("CREATE TRIGGER tc_3 BEFORE UPDATE OF v ON tc_t FOR EACH ROW "
                            + "WHEN ((old.v IS DISTINCT FROM new.v)) EXECUTE FUNCTION tc_f()",
                    "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'tc_3'");

            s.execute("DROP TABLE tc_t CASCADE");
            s.execute("DROP FUNCTION tc_f()");
        }
    }

    @Test
    void a_foreign_key_installs_four_internal_triggers() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE fkt_p (id int primary key)");
            s.execute("CREATE TABLE fkt_c (id int references fkt_p(id))");

            assertScalar("4", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid IN ('fkt_p'::regclass, 'fkt_c'::regclass) AND tgisinternal");
            // An INSERT (5) and an UPDATE (17) check on the referencing table; a DELETE (9) and
            // an UPDATE action on the referenced one.
            assertScalar("1", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid = 'fkt_c'::regclass AND tgtype = 5");
            assertScalar("1", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid = 'fkt_c'::regclass AND tgtype = 17");
            assertScalar("1", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid = 'fkt_p'::regclass AND tgtype = 9");
            assertScalar("1", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid = 'fkt_p'::regclass AND tgtype = 17");
            assertScalar("4", "SELECT count(*)::text FROM pg_trigger t JOIN pg_constraint c "
                    + "ON c.oid = t.tgconstraint WHERE c.contype = 'f' "
                    + "AND t.tgrelid IN ('fkt_p'::regclass, 'fkt_c'::regclass)");
            assertScalar("4", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid IN ('fkt_p'::regclass, 'fkt_c'::regclass) "
                    + "AND tgname ~ '^RI_ConstraintTrigger_[ac]_[0-9]+$'");
            // Each points at the other table and hangs off the referenced key's index.
            assertScalar("fkt_p", "SELECT DISTINCT tgconstrrelid::regclass::text "
                    + "FROM pg_trigger WHERE tgrelid = 'fkt_c'::regclass");
            assertScalar("0", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid IN ('fkt_p'::regclass, 'fkt_c'::regclass) AND tgconstrindid = 0");
            // They carry no arguments and no condition.
            assertScalar("0", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid IN ('fkt_p'::regclass, 'fkt_c'::regclass) "
                    + "AND (tgnargs <> 0 OR tgqual IS NOT NULL OR length(tgargs) <> 0)");

            // NO ACTION defers with the constraint; a cascade cannot wait, so its action
            // trigger is never deferrable however the constraint was written.
            s.execute("CREATE TABLE fkt_p2 (id int primary key)");
            s.execute("CREATE TABLE fkt_c2 (id int references fkt_p2(id) DEFERRABLE INITIALLY DEFERRED)");
            assertScalar("4", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid IN ('fkt_p2'::regclass, 'fkt_c2'::regclass) "
                    + "AND tgdeferrable AND tginitdeferred");

            s.execute("CREATE TABLE fkt_p3 (id int primary key)");
            s.execute("CREATE TABLE fkt_c3 (id int references fkt_p3(id) "
                    + "ON DELETE CASCADE ON UPDATE SET NULL DEFERRABLE INITIALLY DEFERRED)");
            assertScalar("2", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid IN ('fkt_p3'::regclass, 'fkt_c3'::regclass) AND tgdeferrable");
            assertScalar("0", "SELECT count(*)::text FROM pg_trigger "
                    + "WHERE tgrelid = 'fkt_p3'::regclass AND tgdeferrable");

            s.execute("DROP TABLE fkt_c3");
            s.execute("DROP TABLE fkt_p3");
            s.execute("DROP TABLE fkt_c2");
            s.execute("DROP TABLE fkt_p2");
            s.execute("DROP TABLE fkt_c");
            s.execute("DROP TABLE fkt_p");
        }
    }

    // ---- pg_proc keeps no typmod for a function's arguments -----------------------------------

    @Test
    void a_functions_arguments_carry_no_type_modifier() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE FUNCTION fa_iv(p interval day to second(2)) RETURNS int "
                    + "AS $$ SELECT 1 $$ LANGUAGE sql");
            s.execute("CREATE FUNCTION fa_mod(p varchar(10), q numeric(5,2)) RETURNS int "
                    + "AS $$ SELECT 3 $$ LANGUAGE sql");

            assertScalar("p interval",
                    "SELECT pg_get_function_arguments(oid) FROM pg_proc WHERE proname = 'fa_iv'");
            assertScalar("p character varying, q numeric",
                    "SELECT pg_get_function_arguments(oid) FROM pg_proc WHERE proname = 'fa_mod'");
            assertScalar("p character varying, q numeric",
                    "SELECT pg_get_function_identity_arguments(oid) FROM pg_proc WHERE proname = 'fa_mod'");
            // The qualifier was never part of the argument's type, so the value it would have
            // narrowed is still accepted.
            assertScalar("1", "SELECT fa_iv('1 day 2:03:04.5678')::text");

            s.execute("DROP FUNCTION fa_iv(interval)");
            s.execute("DROP FUNCTION fa_mod(varchar, numeric)");
        }
    }

    // ---- a foreign key's rows belong to the tables the key really names ----------------------

    @Test
    void an_unqualified_reference_files_its_triggers_against_the_table_it_found() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA fkx_a");
            s.execute("CREATE SCHEMA fkx_b");
            s.execute("CREATE TABLE fkx_a.ref (id int CONSTRAINT fkx_ref_pk PRIMARY KEY)");
            s.execute("SET search_path TO fkx_a, public");
            s.execute("CREATE TABLE fkx_b.kid (id int, p int REFERENCES ref(id))");
            try {
                // The action triggers sit on the table the reference found, in the schema that
                // table is in — not on a relation named after the child's schema, which does
                // not exist.
                assertScalar("2", triggerCount("fkx_a", "ref"));
                assertScalar("2", triggerCount("fkx_b", "kid"));
                assertScalar("0", triggerCount("fkx_b", "ref"));
                // Nothing in pg_trigger points at a relation that is not there.
                assertScalar("0", "SELECT count(*)::text FROM pg_trigger t "
                        + "LEFT JOIN pg_class c ON c.oid = t.tgrelid WHERE c.oid IS NULL");
                assertScalar("0", "SELECT count(*)::text FROM pg_trigger t "
                        + "LEFT JOIN pg_class c ON c.oid = t.tgconstrindid "
                        + "WHERE t.tgconstrindid <> 0 AND c.oid IS NULL");
            } finally {
                s.execute("SET search_path TO public");
                s.execute("DROP SCHEMA fkx_b CASCADE");
                s.execute("DROP SCHEMA fkx_a CASCADE");
            }
        }
    }

    /** How many pg_trigger rows are filed against one relation, named by schema and relation. */
    private static String triggerCount(String schema, String relation) {
        return "SELECT count(*)::text FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                + "WHERE n.nspname = '" + schema + "' AND c.relname = '" + relation + "'";
    }

    @Test
    void two_schemas_may_each_hold_a_constraint_of_the_same_name() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA fky_a");
            s.execute("CREATE SCHEMA fky_b");
            s.execute("CREATE TABLE fky_a.par (id int CONSTRAINT fky_pk_x PRIMARY KEY)");
            s.execute("CREATE TABLE fky_b.par (id int CONSTRAINT fky_pk_y PRIMARY KEY)");
            s.execute("CREATE TABLE fky_a.chi (id int, pid int "
                    + "CONSTRAINT fky_fk_z REFERENCES fky_a.par(id))");
            s.execute("CREATE TABLE fky_b.chi (id int, pid int "
                    + "CONSTRAINT fky_fk_z REFERENCES fky_b.par(id))");

            // Two constraints, two OIDs. One OID between them doubled every row of a join.
            assertScalar("2", "SELECT count(DISTINCT oid)::text FROM pg_constraint "
                    + "WHERE conname = 'fky_fk_z'");
            assertScalar("4", "SELECT count(*)::text FROM pg_trigger t "
                    + "JOIN pg_constraint cn ON cn.oid = t.tgconstraint "
                    + "JOIN pg_class c ON c.oid = t.tgrelid "
                    + "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'fky_a'");
            assertScalar("4", "SELECT count(*)::text FROM pg_trigger t "
                    + "JOIN pg_constraint cn ON cn.oid = t.tgconstraint "
                    + "JOIN pg_class c ON c.oid = t.tgrelid "
                    + "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'fky_b'");
            // Each key hangs off the index of the primary key it actually references.
            assertScalar("fky_pk_y", "SELECT DISTINCT i.relname FROM pg_trigger t "
                    + "JOIN pg_class c ON c.oid = t.tgrelid "
                    + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    + "JOIN pg_class i ON i.oid = t.tgconstrindid "
                    + "WHERE n.nspname = 'fky_b' AND c.relname = 'chi'");
            assertScalar("fky_pk_x", "SELECT DISTINCT i.relname FROM pg_trigger t "
                    + "JOIN pg_class c ON c.oid = t.tgrelid "
                    + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    + "JOIN pg_class i ON i.oid = t.tgconstrindid "
                    + "WHERE n.nspname = 'fky_a' AND c.relname = 'chi'");
            assertScalar("0", "SELECT count(*)::text FROM pg_constraint cn "
                    + "LEFT JOIN pg_class c ON c.oid = cn.confrelid "
                    + "WHERE cn.confrelid <> 0 AND c.oid IS NULL");
            // A schema the reader cannot reach by name is written out in full.
            assertScalar("FOREIGN KEY (pid) REFERENCES fky_a.par(id)",
                    "SELECT pg_get_constraintdef(cn.oid) FROM pg_constraint cn "
                            + "JOIN pg_class c ON c.oid = cn.conrelid "
                            + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                            + "WHERE cn.conname = 'fky_fk_z' AND n.nspname = 'fky_a'");

            s.execute("DROP SCHEMA fky_b CASCADE");
            s.execute("DROP SCHEMA fky_a CASCADE");
        }
    }

    // ---- what pg_get_triggerdef prints back ---------------------------------------------------

    @Test
    void a_when_conditions_string_constant_is_printed_as_written() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tgw_t (id int, v text)");
            s.execute("CREATE FUNCTION tgw_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ "
                    + "LANGUAGE plpgsql");
            s.execute("CREATE TRIGGER tgw_1 BEFORE UPDATE ON tgw_t FOR EACH ROW "
                    + "WHEN (NEW.v = 'Old. Faithful') EXECUTE FUNCTION tgw_f()");
            s.execute("CREATE TRIGGER tgw_2 BEFORE UPDATE ON tgw_t FOR EACH ROW "
                    + "WHEN (NEW.v LIKE 'NEW.%') EXECUTE FUNCTION tgw_f()");
            s.execute("CREATE TRIGGER tgw_3 BEFORE UPDATE ON tgw_t FOR EACH ROW "
                    + "WHEN (NEW.v <> OLD.v) EXECUTE FUNCTION tgw_f()");

            // The row references are lower-cased; the value they are compared with is not, and
            // the space after its dot survives. A dump that rewrote it fired on another value.
            assertScalar("yes", "SELECT CASE WHEN pg_get_triggerdef(oid) LIKE '%''Old. Faithful''%' "
                    + "THEN 'yes' ELSE 'no' END FROM pg_trigger WHERE tgname = 'tgw_1'");
            assertScalar("yes", "SELECT CASE WHEN pg_get_triggerdef(oid) LIKE '%WHEN ((new.v %' "
                    + "THEN 'yes' ELSE 'no' END FROM pg_trigger WHERE tgname = 'tgw_1'");
            assertScalar("yes", "SELECT CASE WHEN pg_get_triggerdef(oid) LIKE '%''NEW.%%''%' "
                    + "THEN 'yes' ELSE 'no' END FROM pg_trigger WHERE tgname = 'tgw_2'");
            assertScalar("CREATE TRIGGER tgw_3 BEFORE UPDATE ON tgw_t FOR EACH ROW "
                            + "WHEN ((new.v <> old.v)) EXECUTE FUNCTION tgw_f()",
                    "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'tgw_3'");

            s.execute("DROP TABLE tgw_t CASCADE");
            s.execute("DROP FUNCTION tgw_f()");
        }
    }

    @Test
    void transition_tables_and_deferrability_are_catalog_content() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE tgr_t (id int, a int)");
            s.execute("CREATE FUNCTION tgr_f() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ "
                    + "LANGUAGE plpgsql");
            s.execute("CREATE TRIGGER tgr_1 AFTER UPDATE ON tgr_t "
                    + "REFERENCING OLD TABLE AS oldt NEW TABLE AS newt "
                    + "FOR EACH STATEMENT EXECUTE FUNCTION tgr_f()");
            s.execute("CREATE CONSTRAINT TRIGGER tgr_2 AFTER INSERT ON tgr_t "
                    + "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION tgr_f()");

            assertScalar("oldt|newt", "SELECT tgoldtable || '|' || tgnewtable "
                    + "FROM pg_trigger WHERE tgname = 'tgr_1'");
            assertScalar("CREATE TRIGGER tgr_1 AFTER UPDATE ON tgr_t "
                            + "REFERENCING OLD TABLE AS oldt NEW TABLE AS newt "
                            + "FOR EACH STATEMENT EXECUTE FUNCTION tgr_f()",
                    "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'tgr_1'");
            // A deferrable trigger says so, which is what tells a client SET CONSTRAINTS reaches it.
            assertScalar("true|true", "SELECT tgdeferrable::text || '|' || tginitdeferred::text "
                    + "FROM pg_trigger WHERE tgname = 'tgr_2'");
            assertScalar("false|false", "SELECT tgdeferrable::text || '|' || tginitdeferred::text "
                    + "FROM pg_trigger WHERE tgname = 'tgr_1'");

            s.execute("DROP TABLE tgr_t CASCADE");
            s.execute("DROP FUNCTION tgr_f()");
        }
    }

    // ---- an interval literal has to name a quantity -------------------------------------------

    @Test
    void a_unit_word_on_its_own_is_not_an_interval_of_zero() throws SQLException {
        String[] nothings = {"day", "days", "year", "month", "week", "hours", "second",
                "millennium", "microsecond", "ago", "ago ago", "@", "@ @", "@ ago"};
        for (String text : nothings) {
            assertFails("22007", "invalid input syntax for type interval",
                    "SELECT CAST('" + text + "' AS interval)");
        }
        // Reached the same way through a column, where it used to store a quantity nobody wrote.
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ivz_t (id int, v interval)");
            assertFails("22007", "invalid input syntax for type interval",
                    "INSERT INTO ivz_t VALUES (1, 'day')");
            assertScalar("0", "SELECT count(*)::text FROM ivz_t");
            s.execute("DROP TABLE ivz_t");
        }
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT '2020-01-01'::date + 'day'::interval");
    }

    @Test
    void ago_is_the_last_word_of_a_literal_and_names_no_unit() throws SQLException {
        // 'ago' stands where the unit for the number to its left would have been.
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 ago' AS interval)");
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 day 2 ago' AS interval)");
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 ago' AS interval day to second)");
        // And nothing may follow it.
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('ago 1 day' AS interval)");
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 day ago 2 hours' AS interval)");
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 day ago ago' AS interval)");
        // What it does turn around still turns around.
        assertInterval("-1 days", "SELECT CAST('1 day ago' AS interval)::text");
        assertInterval("-1 days", "SELECT CAST('@ 1 day ago' AS interval)::text");
        assertInterval("-1 years -2 mons", "SELECT CAST('1-2 ago' AS interval)::text");
        assertInterval("-04:05:00", "SELECT CAST('4:05 ago' AS interval)::text");
        assertInterval("-1 days -02:00:00", "SELECT CAST('1 day 2 hours ago' AS interval)::text");
    }

    @Test
    void a_unit_keeps_naming_the_numbers_to_its_left() throws SQLException {
        // '1 2 days' fills DAY twice, which is the duplicate-field error and not a stray second.
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 2 days' AS interval)");
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 2 minutes' AS interval)");
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('5 1 day' AS interval)");
        assertFails("22007", "invalid input syntax for type interval",
                "SELECT CAST('1 2 3 seconds' AS interval)");
        // An hour is the one unit that hands DAY leftwards, because 'D H' is DAY TO HOUR.
        assertInterval("1 day 02:00:00", "SELECT CAST('1 2 hours' AS interval)::text");
        assertInterval("1 day 02:00:00", "SELECT CAST('1 2' AS interval day to hour)::text");
        // A time of day does the same.
        assertInterval("3 days 04:05:00", "SELECT CAST('3 4:05' AS interval)::text");
        assertInterval("1 year 2 mons 00:00:03", "SELECT CAST('1-2 3' AS interval)::text");
        assertInterval("1 year 2 mons 3 days 04:05:00",
                "SELECT CAST('1-2 3 4:05' AS interval)::text");
    }

    // ---- format_type shows the modifier a type has no spelling of its own for ------------------

    @Test
    void a_type_with_no_modifier_output_still_shows_the_number() throws SQLException {
        assertScalar("date(3)", "SELECT format_type('date'::regtype, 3)");
        assertScalar("date(3)[]", "SELECT format_type('date[]'::regtype, 3)");
        assertScalar("text(5)", "SELECT format_type('text'::regtype, 5)");
        assertScalar("text(5)[]", "SELECT format_type('text[]'::regtype, 5)");
        assertScalar("uuid(5)", "SELECT format_type('uuid'::regtype, 5)");
        assertScalar("bytea(5)", "SELECT format_type('bytea'::regtype, 5)");
        assertScalar("inet(5)", "SELECT format_type('inet'::regtype, 5)");
        assertScalar("jsonb(5)", "SELECT format_type('jsonb'::regtype, 5)");
        // The names PostgreSQL writes out by hand never carry one.
        assertScalar("integer", "SELECT format_type('int4'::regtype, 5)");
        assertScalar("smallint", "SELECT format_type('int2'::regtype, 5)");
        assertScalar("bigint", "SELECT format_type('int8'::regtype, 5)");
        assertScalar("boolean", "SELECT format_type('bool'::regtype, 5)");
        assertScalar("real", "SELECT format_type('float4'::regtype, 5)");
        assertScalar("double precision", "SELECT format_type('float8'::regtype, 5)");
        assertScalar("json", "SELECT format_type('json'::regtype, 5)");
        assertScalar("integer[]", "SELECT format_type('int4[]'::regtype, 5)");
        // Nor does a type asked for no modifier at all.
        assertScalar("date", "SELECT format_type('date'::regtype, -1)");
        assertScalar("text[]", "SELECT format_type('text[]'::regtype, NULL)");
    }

    // ---- a field qualifier with a precision, in every position it may be written ---------------

    @Test
    void a_qualifier_with_a_precision_is_read_wherever_a_type_is_written() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ivq_t (a interval day to second(2), b interval second(3), "
                    + "c interval hour to minute, d interval(4), e interval minute to second(0), "
                    + "f interval year to month, g interval hour to second(5))");

            assertScalar("interval day to second(2)", "SELECT format_type(atttypid, atttypmod) "
                    + "FROM pg_attribute WHERE attrelid = 'ivq_t'::regclass AND attname = 'a'");
            assertScalar("interval minute to second(0)", "SELECT format_type(atttypid, atttypmod) "
                    + "FROM pg_attribute WHERE attrelid = 'ivq_t'::regclass AND attname = 'e'");
            // information_schema names the qualifier and the precision separately.
            assertScalar("DAY TO SECOND(2)|2", "SELECT interval_type || '|' || datetime_precision "
                    + "FROM information_schema.columns "
                    + "WHERE table_name = 'ivq_t' AND column_name = 'a'");
            assertScalar("HOUR TO MINUTE|6", "SELECT interval_type || '|' || datetime_precision "
                    + "FROM information_schema.columns "
                    + "WHERE table_name = 'ivq_t' AND column_name = 'c'");
            assertScalar("MINUTE TO SECOND(0)|0", "SELECT interval_type || '|' || datetime_precision "
                    + "FROM information_schema.columns "
                    + "WHERE table_name = 'ivq_t' AND column_name = 'e'");
            // interval_precision is the leading field's precision, which PG never records here.
            assertScalar("0", "SELECT count(*)::text FROM information_schema.columns "
                    + "WHERE table_name = 'ivq_t' AND interval_precision IS NOT NULL");

            // The same qualifier in a cast, and written after the literal.
            assertInterval("1 day 02:03:04.57",
                    "SELECT CAST('1 day 02:03:04.5678' AS interval day to second(2))::text");
            assertInterval("1 day 02:03:04.6",
                    "SELECT CAST('1 day 02:03:04.5678' AS interval second(1))::text");
            assertInterval("1 day 02:03:05",
                    "SELECT CAST('1 day 02:03:04.5678' AS interval minute to second(0))::text");
            assertInterval("1 day 02:03:04.6",
                    "SELECT ('1 day 02:03:04.5678'::interval second(1))::text");

            // In a domain, where information_schema.domains reports it the same way.
            s.execute("CREATE DOMAIN ivq_d AS interval day to second(2)");
            assertScalar("DAY TO SECOND(2)|2", "SELECT interval_type || '|' || datetime_precision "
                    + "FROM information_schema.domains WHERE domain_name = 'ivq_d'");

            // And in a PL/pgSQL declaration, which used to be the one place it would not parse.
            s.execute("CREATE FUNCTION ivq_f() RETURNS text AS $$ "
                    + "DECLARE v interval day to second(2); "
                    + "BEGIN v := '1 day 02:03:04'; RETURN v::text; END $$ LANGUAGE plpgsql");
            assertScalar("1 day 02:03:04", "SELECT ivq_f()");

            s.execute("DROP FUNCTION ivq_f()");
            s.execute("DROP DOMAIN ivq_d");
            s.execute("DROP TABLE ivq_t");
        }
    }

    // ---- helpers ------------------------------------------------------------------------------

    private static void assertInterval(String expected, String sql) throws SQLException {
        assertScalar(expected, sql);
    }

    private static void assertScalar(String expected, String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "no row from: " + sql);
            assertEquals(expected, rs.getString(1), sql);
        }
    }

    private static void assertFails(String sqlstate, String messagePart, String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("expected " + sqlstate + " from: " + sql);
        } catch (SQLException e) {
            assertEquals(sqlstate, e.getSQLState(), sql + " -> " + e.getMessage());
            assertTrue(e.getMessage().contains(messagePart),
                    sql + " -> " + e.getMessage());
        }
    }
}
