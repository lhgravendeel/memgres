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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What ALTER TABLE does to a column it does not own alone.
 *
 * <p>Four things were measured against PostgreSQL 18 and are pinned here. A DEFAULT is judged by
 * its type and not by its value, so a number too big for the column is recorded and reported at
 * the insert that takes it, not at the ALTER. ENABLE and DISABLE reach a rule or a trigger in all
 * four of PostgreSQL's firing modes, REPLICA included — it is not a reserved word, which is why
 * the form did not parse at all. A column a table inherits belongs to its parent, so its type is
 * changed there and from there it reaches every descendant. And ONLY, on a table whose children
 * are required to look like it, is refused for the actions whose effect the children would have
 * to share.
 *
 * <p>Every expected value here was measured on PostgreSQL 18.</p>
 */
class DdlAlterTableResidualsTest {

    private static Memgres memgres;
    private static Connection conn;

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

    // ---- #112 a DEFAULT is judged by its type, not by its value -------------------------------

    @Test
    void a_default_too_big_for_the_column_is_accepted_and_fails_at_the_insert() throws SQLException {
        run("CREATE TABLE atr_dv1 (id int primary key, c1 int)");
        run("ALTER TABLE atr_dv1 ALTER COLUMN c1 SET DEFAULT 2147483648");
        assertError("22003", "integer out of range", "INSERT INTO atr_dv1 (id) VALUES (1)");
    }

    @Test
    void a_default_of_a_numeric_beyond_bigint_is_accepted_too() throws SQLException {
        run("CREATE TABLE atr_dv2 (id int primary key, c1 int, c2 bigint)");
        run("ALTER TABLE atr_dv2 ALTER COLUMN c1 SET DEFAULT 99999999999999999999");
        run("ALTER TABLE atr_dv2 ALTER COLUMN c2 SET DEFAULT 99999999999999999999");
        assertError("22003", "integer out of range", "INSERT INTO atr_dv2 (id) VALUES (1)");
        run("ALTER TABLE atr_dv2 ALTER COLUMN c1 DROP DEFAULT");
        assertError("22003", "bigint out of range", "INSERT INTO atr_dv2 (id) VALUES (1)");
    }

    @Test
    void a_default_too_big_for_a_smallint_or_a_numeric_is_accepted_too() throws SQLException {
        run("CREATE TABLE atr_dv3 (id int primary key, c1 smallint)");
        run("ALTER TABLE atr_dv3 ALTER COLUMN c1 SET DEFAULT 40000");
        assertError("22003", "smallint out of range", "INSERT INTO atr_dv3 (id) VALUES (1)");
        run("CREATE TABLE atr_dv4 (id int primary key, c1 numeric(4,0))");
        run("ALTER TABLE atr_dv4 ALTER COLUMN c1 SET DEFAULT 99999");
        assertError("22003", "numeric field overflow", "INSERT INTO atr_dv4 (id) VALUES (1)");
    }

    @Test
    void a_default_longer_than_the_column_is_accepted_too() throws SQLException {
        run("CREATE TABLE atr_dv5 (id int primary key, c1 varchar(3))");
        run("ALTER TABLE atr_dv5 ALTER COLUMN c1 SET DEFAULT 'abcdefg'");
        assertError("22001", "value too long for type character varying(3)",
                "INSERT INTO atr_dv5 (id) VALUES (1)");
    }

    @Test
    void a_default_of_a_type_the_column_cannot_take_is_still_refused() throws SQLException {
        run("CREATE TABLE atr_dv6 (id int primary key, c1 int, c2 date)");
        assertError("42804", "column \"c1\" is of type integer but default expression is of type"
                + " boolean", "ALTER TABLE atr_dv6 ALTER COLUMN c1 SET DEFAULT true");
        assertError("42804", "column \"c2\" is of type date but default expression is of type"
                + " integer", "ALTER TABLE atr_dv6 ALTER COLUMN c2 SET DEFAULT 5");
        assertError("42804", "column \"c1\" is of type integer but default expression is of type"
                + " text", "ALTER TABLE atr_dv6 ALTER COLUMN c1 SET DEFAULT 'x'::text");
    }

    @Test
    void a_bare_string_default_that_is_not_a_value_of_the_type_is_still_refused() throws SQLException {
        run("CREATE TABLE atr_dv7 (id int primary key, c1 int)");
        assertError("22P02", "invalid input syntax for type integer",
                "ALTER TABLE atr_dv7 ALTER COLUMN c1 SET DEFAULT 'abc'");
    }

    @Test
    void the_ordinary_defaults_are_all_still_accepted() throws SQLException {
        run("CREATE TABLE atr_dv8 (id int primary key, a int, b text, c boolean, d date,"
                + " e timestamptz, f numeric(5,2), g int[], h jsonb, i uuid)");
        run("ALTER TABLE atr_dv8 ALTER COLUMN a SET DEFAULT 7,"
                + " ALTER COLUMN b SET DEFAULT 'x'::text,"
                + " ALTER COLUMN c SET DEFAULT true,"
                + " ALTER COLUMN d SET DEFAULT DATE '2020-01-01',"
                + " ALTER COLUMN e SET DEFAULT now(),"
                + " ALTER COLUMN f SET DEFAULT 10::numeric(5,2),"
                + " ALTER COLUMN g SET DEFAULT ARRAY[1,2],"
                + " ALTER COLUMN h SET DEFAULT '{}'::jsonb,"
                + " ALTER COLUMN i SET DEFAULT gen_random_uuid()");
        run("INSERT INTO atr_dv8 (id) VALUES (1)");
        assertEquals("7 | x | true | 2020-01-01 | 10.00 | {1,2} | {}",
                one("SELECT a, b, c, d::text, f::text, g::text, h::text FROM atr_dv8"));
    }

    @Test
    void an_array_default_is_recorded_rather_than_silently_dropped() throws SQLException {
        run("CREATE TABLE atr_dv9 (id int primary key, g int[] DEFAULT ARRAY[1,2])");
        run("INSERT INTO atr_dv9 (id) VALUES (1)");
        assertEquals("{1,2}", one("SELECT g::text FROM atr_dv9"));
        assertEquals("ARRAY[1, 2]", one("SELECT column_default FROM information_schema.columns"
                + " WHERE table_name = 'atr_dv9' AND column_name = 'g'"));
    }

    @Test
    void a_default_of_null_creates_the_table_rather_than_failing() throws SQLException {
        run("CREATE TABLE atr_dv10 (id int primary key, c1 int DEFAULT NULL)");
        run("INSERT INTO atr_dv10 (id) VALUES (1)");
        assertEquals("NULL", one("SELECT c1 FROM atr_dv10"));
    }

    @Test
    void a_cast_of_a_literal_reads_back_the_way_postgres_normalises_it() throws SQLException {
        run("CREATE TABLE atr_dv11 (id int primary key, a int, b int[], c boolean)");
        run("ALTER TABLE atr_dv11 ALTER COLUMN a SET DEFAULT '7'::int,"
                + " ALTER COLUMN b SET DEFAULT '{1,2}'::int[],"
                + " ALTER COLUMN c SET DEFAULT 'true'::boolean");
        assertRows(Arrays.asList("a | 7", "b | '{1,2}'::integer[]", "c | true"),
                "SELECT column_name, column_default FROM information_schema.columns"
                        + " WHERE table_name = 'atr_dv11' AND column_name <> 'id'"
                        + " ORDER BY ordinal_position");
    }

    // ---- #160 ENABLE and DISABLE in all four firing modes -------------------------------------

    @Test
    void enable_replica_rule_parses_and_records_the_mode() throws SQLException {
        run("CREATE TABLE atr_r1 (a int primary key, b int)");
        run("CREATE RULE atr_r1_r AS ON UPDATE TO atr_r1 DO ALSO NOTHING");
        assertEquals("O", ruleState("atr_r1_r"));
        run("ALTER TABLE atr_r1 ENABLE REPLICA RULE atr_r1_r");
        assertEquals("R", ruleState("atr_r1_r"));
        run("ALTER TABLE atr_r1 ENABLE ALWAYS RULE atr_r1_r");
        assertEquals("A", ruleState("atr_r1_r"));
        run("ALTER TABLE atr_r1 DISABLE RULE atr_r1_r");
        assertEquals("D", ruleState("atr_r1_r"));
        run("ALTER TABLE atr_r1 ENABLE RULE atr_r1_r");
        assertEquals("O", ruleState("atr_r1_r"));
    }

    @Test
    void enable_replica_trigger_parses_and_records_the_mode() throws SQLException {
        run("CREATE TABLE atr_t1 (a int primary key)");
        run("CREATE OR REPLACE FUNCTION atr_trgf() RETURNS trigger LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN NEW; END $$");
        run("CREATE TRIGGER atr_t1_t BEFORE INSERT ON atr_t1 FOR EACH ROW"
                + " EXECUTE FUNCTION atr_trgf()");
        assertEquals("O", triggerState("atr_t1_t"));
        run("ALTER TABLE atr_t1 ENABLE REPLICA TRIGGER atr_t1_t");
        assertEquals("R", triggerState("atr_t1_t"));
        run("ALTER TABLE atr_t1 ENABLE ALWAYS TRIGGER atr_t1_t");
        assertEquals("A", triggerState("atr_t1_t"));
        run("ALTER TABLE atr_t1 DISABLE TRIGGER atr_t1_t");
        assertEquals("D", triggerState("atr_t1_t"));
        run("ALTER TABLE atr_t1 ENABLE TRIGGER ALL");
        assertEquals("O", triggerState("atr_t1_t"));
    }

    @Test
    void a_rule_or_trigger_that_is_not_there_is_still_reported() throws SQLException {
        run("CREATE TABLE atr_r2 (a int primary key)");
        assertError("42704", "rule \"atr_nosuch\" for relation \"atr_r2\" does not exist",
                "ALTER TABLE atr_r2 ENABLE REPLICA RULE atr_nosuch");
        assertError("42704", "trigger \"atr_nosuch\" for table \"atr_r2\" does not exist",
                "ALTER TABLE atr_r2 ENABLE ALWAYS TRIGGER atr_nosuch");
    }

    @Test
    void the_row_security_forms_still_parse_beside_the_rule_ones() throws SQLException {
        run("CREATE TABLE atr_r3 (a int primary key)");
        run("ALTER TABLE atr_r3 ENABLE ROW LEVEL SECURITY");
        run("ALTER TABLE atr_r3 DISABLE ROW LEVEL SECURITY");
        run("ALTER TABLE atr_r3 SET WITHOUT CLUSTER");
    }

    // ---- #114 / #151 an inherited column belongs to its parent --------------------------------

    @Test
    void a_child_cannot_retype_a_column_it_inherits() throws SQLException {
        run("CREATE TABLE atr_p1 (a int, b int)");
        run("CREATE TABLE atr_c1 (c int) INHERITS (atr_p1)");
        assertError("42P16", "cannot alter inherited column \"a\"",
                "ALTER TABLE atr_c1 ALTER COLUMN a TYPE bigint");
        // its own column is still its own
        run("ALTER TABLE atr_c1 ALTER COLUMN c TYPE bigint");
        assertEquals("integer | integer | bigint", one("SELECT string_agg(data_type, ' | '"
                + " ORDER BY ordinal_position) FROM information_schema.columns"
                + " WHERE table_name = 'atr_c1'"));
    }

    @Test
    void a_retype_on_the_parent_reaches_the_child() throws SQLException {
        run("CREATE TABLE atr_p2 (a int, b int)");
        run("CREATE TABLE atr_c2 (c int) INHERITS (atr_p2)");
        run("INSERT INTO atr_c2 VALUES (1, 2, 3)");
        run("ALTER TABLE atr_p2 ALTER COLUMN b TYPE bigint");
        assertEquals("bigint", columnType("atr_c2", "b"));
        assertEquals("bigint", columnType("atr_p2", "b"));
        run("ALTER TABLE atr_p2 ALTER COLUMN a TYPE text");
        assertEquals("1 | text", one("SELECT a, pg_typeof(a)::text FROM atr_c2"));
    }

    @Test
    void a_child_attached_later_is_no_different() throws SQLException {
        run("CREATE TABLE atr_p3 (a int, b int)");
        run("CREATE TABLE atr_c3 (a int, b int)");
        run("ALTER TABLE atr_c3 INHERIT atr_p3");
        assertError("42P16", "cannot alter inherited column \"a\"",
                "ALTER TABLE atr_c3 ALTER COLUMN a TYPE text");
        run("ALTER TABLE atr_p3 ALTER COLUMN b TYPE bigint");
        assertEquals("bigint", columnType("atr_c3", "b"));
    }

    @Test
    void the_column_is_still_inherited_after_the_parent_renamed_it() throws SQLException {
        run("CREATE TABLE atr_p4 (a int, b int)");
        run("CREATE TABLE atr_c4 (c int) INHERITS (atr_p4)");
        run("ALTER TABLE atr_p4 RENAME COLUMN a TO pa");
        assertError("42P16", "cannot alter inherited column \"pa\"",
                "ALTER TABLE atr_c4 ALTER COLUMN pa TYPE bigint");
    }

    @Test
    void the_rule_holds_down_several_levels() throws SQLException {
        run("CREATE TABLE atr_g1 (a int, b int)");
        run("CREATE TABLE atr_g2 () INHERITS (atr_g1)");
        run("CREATE TABLE atr_g3 () INHERITS (atr_g2)");
        assertError("42P16", "cannot alter inherited column \"a\"",
                "ALTER TABLE atr_g3 ALTER COLUMN a TYPE bigint");
        assertError("42P16", "cannot alter inherited column \"a\"",
                "ALTER TABLE atr_g2 ALTER COLUMN a TYPE bigint");
        run("ALTER TABLE atr_g1 ALTER COLUMN a TYPE bigint");
        assertEquals("bigint", columnType("atr_g2", "a"));
        assertEquals("bigint", columnType("atr_g3", "a"));
    }

    @Test
    void every_parent_counts_when_a_table_inherits_from_two() throws SQLException {
        run("CREATE TABLE atr_m1 (a int)");
        run("CREATE TABLE atr_m2 (b int)");
        run("CREATE TABLE atr_m3 (c int) INHERITS (atr_m1, atr_m2)");
        assertError("42P16", "cannot alter inherited column \"a\"",
                "ALTER TABLE atr_m3 ALTER COLUMN a TYPE bigint");
        assertError("42P16", "cannot alter inherited column \"b\"",
                "ALTER TABLE atr_m3 ALTER COLUMN b TYPE bigint");
        run("ALTER TABLE atr_m1 ALTER COLUMN a TYPE bigint");
        assertEquals("bigint", columnType("atr_m3", "a"));
    }

    @Test
    void a_partition_is_an_inheritance_child_too() throws SQLException {
        run("CREATE TABLE atr_pt1 (id int, v text) PARTITION BY RANGE (id)");
        run("CREATE TABLE atr_pt1a PARTITION OF atr_pt1 FOR VALUES FROM (1) TO (100)");
        assertError("42P16", "cannot alter inherited column \"v\"",
                "ALTER TABLE atr_pt1a ALTER COLUMN v TYPE varchar(20)");
        run("ALTER TABLE atr_pt1 ALTER COLUMN v TYPE varchar(20)");
        assertEquals("character varying", columnType("atr_pt1a", "v"));
    }

    @Test
    void only_is_refused_for_a_retype_on_a_table_that_has_children() throws SQLException {
        run("CREATE TABLE atr_o1 (a int, b int)");
        run("CREATE TABLE atr_o1c () INHERITS (atr_o1)");
        assertError("42P16", "type of inherited column \"b\" must be changed in child tables too",
                "ALTER TABLE ONLY atr_o1 ALTER COLUMN b TYPE bigint");
    }

    @Test
    void a_retype_with_no_hierarchy_around_it_is_untouched() throws SQLException {
        run("CREATE TABLE atr_o2 (a int, b int)");
        run("ALTER TABLE atr_o2 ALTER COLUMN a TYPE bigint");
        run("ALTER TABLE ONLY atr_o2 ALTER COLUMN b TYPE bigint");
        assertEquals("bigint", columnType("atr_o2", "a"));
        // a child detached from its parent owns its columns again
        run("CREATE TABLE atr_o3 (a int, b int)");
        run("CREATE TABLE atr_o3c (c int) INHERITS (atr_o3)");
        run("ALTER TABLE atr_o3c NO INHERIT atr_o3");
        run("ALTER TABLE atr_o3c ALTER COLUMN a TYPE bigint");
    }

    @Test
    void the_other_column_actions_are_still_allowed_on_an_inherited_column() throws SQLException {
        run("CREATE TABLE atr_o4 (a int, b int)");
        run("CREATE TABLE atr_o4c (c int) INHERITS (atr_o4)");
        run("ALTER TABLE atr_o4c ALTER COLUMN a SET NOT NULL");
        run("ALTER TABLE atr_o4c ALTER COLUMN a DROP NOT NULL");
        run("ALTER TABLE atr_o4c ALTER COLUMN a SET DEFAULT 3");
        run("ALTER TABLE atr_o4c ALTER COLUMN a DROP DEFAULT");
        run("ALTER TABLE atr_o4c ALTER COLUMN a SET STATISTICS 10");
        run("ALTER TABLE atr_o4c ALTER COLUMN a SET STORAGE PLAIN");
    }

    // ---- #164 ONLY on a table whose children must look like it --------------------------------

    @Test
    void only_set_not_null_on_a_partitioned_parent_is_refused() throws SQLException {
        run("CREATE TABLE atr_q1 (id int, v text) PARTITION BY RANGE (id)");
        run("CREATE TABLE atr_q1a PARTITION OF atr_q1 FOR VALUES FROM (1) TO (100)");
        SQLException e = assertError("42P16", "constraint must be added to child tables too",
                "ALTER TABLE ONLY atr_q1 ALTER COLUMN v SET NOT NULL");
        assertTrue(e.getMessage().contains("Do not specify the ONLY keyword."), e.getMessage());
        assertError("42P16", "constraint must be added to child tables too",
                "ALTER TABLE ONLY atr_q1 ADD CONSTRAINT atr_q1_ck CHECK (id > 0)");
        assertError("42P16", "constraint must be added to child tables too",
                "ALTER TABLE ONLY atr_q1 ADD CHECK (id > 0)");
        // without ONLY the same statements are ordinary
        run("ALTER TABLE atr_q1 ALTER COLUMN v SET NOT NULL");
        run("ALTER TABLE atr_q1 ADD CONSTRAINT atr_q1_ck CHECK (id > 0)");
    }

    @Test
    void only_add_check_on_an_inheritance_parent_is_refused_too() throws SQLException {
        run("CREATE TABLE atr_q2 (id int, v text)");
        run("CREATE TABLE atr_q2c () INHERITS (atr_q2)");
        assertError("42P16", "constraint must be added to child tables too",
                "ALTER TABLE ONLY atr_q2 ADD CONSTRAINT atr_q2_ck CHECK (id > 0)");
        // NO INHERIT says the constraint was never going to travel, so ONLY is not a contradiction
        run("ALTER TABLE ONLY atr_q2 ADD CONSTRAINT atr_q2_ni CHECK (id > 0) NO INHERIT");
        // and SET NOT NULL is allowed on an inheritance parent, unlike a partitioned one
        run("ALTER TABLE ONLY atr_q2 ALTER COLUMN v SET NOT NULL");
    }

    @Test
    void a_no_inherit_constraint_is_refused_on_a_partitioned_table() throws SQLException {
        run("CREATE TABLE atr_q3 (id int, v text) PARTITION BY RANGE (id)");
        run("CREATE TABLE atr_q3a PARTITION OF atr_q3 FOR VALUES FROM (1) TO (100)");
        assertError("42P16", "cannot add NO INHERIT constraint to partitioned table \"atr_q3\"",
                "ALTER TABLE ONLY atr_q3 ADD CONSTRAINT atr_q3_ck CHECK (id > 0) NO INHERIT");
    }

    @Test
    void only_drop_column_on_a_partitioned_parent_is_refused() throws SQLException {
        run("CREATE TABLE atr_q4 (id int, v text) PARTITION BY RANGE (id)");
        run("CREATE TABLE atr_q4a PARTITION OF atr_q4 FOR VALUES FROM (1) TO (100)");
        assertError("42P16",
                "cannot drop column from only the partitioned table when partitions exist",
                "ALTER TABLE ONLY atr_q4 DROP COLUMN v");
        // an inheritance parent is looser, and so is a partitioned table with nothing under it
        run("CREATE TABLE atr_q5 (id int, v text)");
        run("CREATE TABLE atr_q5c () INHERITS (atr_q5)");
        run("ALTER TABLE ONLY atr_q5 DROP COLUMN v");
        run("CREATE TABLE atr_q6 (id int, v text) PARTITION BY RANGE (id)");
        run("ALTER TABLE ONLY atr_q6 DROP COLUMN v");
    }

    @Test
    void only_foreign_key_on_a_partitioned_table_is_refused() throws SQLException {
        run("CREATE TABLE atr_q7ref (id int primary key)");
        run("CREATE TABLE atr_q7 (id int, v text) PARTITION BY RANGE (id)");
        run("CREATE TABLE atr_q7a PARTITION OF atr_q7 FOR VALUES FROM (1) TO (100)");
        assertError("42809", "cannot use ONLY for foreign key on partitioned table \"atr_q7\""
                        + " referencing relation \"atr_q7ref\"",
                "ALTER TABLE ONLY atr_q7 ADD CONSTRAINT atr_q7_fk FOREIGN KEY (id)"
                        + " REFERENCES atr_q7ref(id)");
        run("ALTER TABLE atr_q7 ADD CONSTRAINT atr_q7_fk FOREIGN KEY (id)"
                + " REFERENCES atr_q7ref(id)");
    }

    @Test
    void storage_parameters_are_refused_on_a_partitioned_table() throws SQLException {
        run("CREATE TABLE atr_q8 (id int, v text) PARTITION BY RANGE (id)");
        assertError("42809", "cannot specify storage parameters for a partitioned table",
                "ALTER TABLE atr_q8 SET (fillfactor = 50)");
        assertError("42809", "cannot specify storage parameters for a partitioned table",
                "ALTER TABLE ONLY atr_q8 SET (fillfactor = 50)");
        // an ordinary table takes them, and the other SET forms are not storage parameters
        run("CREATE TABLE atr_q9 (id int, v text)");
        run("ALTER TABLE atr_q9 SET (fillfactor = 50)");
        // atr_q8 is the partitioned one: a partitioned table has no index to mark clustered, so
        // PostgreSQL refuses this form there while taking the other SET spellings.
        assertError("0A000", "cannot mark index clustered in partitioned table",
                "ALTER TABLE atr_q8 SET WITHOUT CLUSTER");
        run("ALTER TABLE atr_q9 SET WITHOUT CLUSTER");
        run("ALTER TABLE atr_q8 SET TABLESPACE pg_default");
        run("ALTER TABLE atr_q8 RESET (fillfactor)");
    }

    @Test
    void only_on_a_childless_or_leaf_relation_is_left_alone() throws SQLException {
        run("CREATE TABLE atr_q10 (id int, v text)");
        run("ALTER TABLE ONLY atr_q10 ALTER COLUMN v SET NOT NULL");
        run("ALTER TABLE ONLY atr_q10 ADD CONSTRAINT atr_q10_ck CHECK (id > 0)");
        run("CREATE TABLE atr_q11 (id int, v text) PARTITION BY RANGE (id)");
        run("ALTER TABLE ONLY atr_q11 ALTER COLUMN v SET NOT NULL");
        run("ALTER TABLE ONLY atr_q11 ADD CONSTRAINT atr_q11_ck CHECK (id > 0)");
        run("CREATE TABLE atr_q12 (id int, v text) PARTITION BY RANGE (id)");
        run("CREATE TABLE atr_q12a PARTITION OF atr_q12 FOR VALUES FROM (1) TO (100)");
        run("ALTER TABLE ONLY atr_q12a ALTER COLUMN id SET NOT NULL");
        run("ALTER TABLE ONLY atr_q12a ADD CONSTRAINT atr_q12a_ck CHECK (id > 0)");
    }

    // ---- helpers -----------------------------------------------------------------------------

    private static String ruleState(String rule) throws SQLException {
        return one("SELECT ev_enabled FROM pg_rewrite WHERE rulename = '" + rule + "'");
    }

    private static String triggerState(String trigger) throws SQLException {
        return one("SELECT tgenabled FROM pg_trigger WHERE tgname = '" + trigger + "'");
    }

    private static String columnType(String table, String column) throws SQLException {
        return one("SELECT data_type FROM information_schema.columns WHERE table_name = '"
                + table + "' AND column_name = '" + column + "'");
    }

    private static void run(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            s.execute(sql);
        }
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

    private static SQLException assertError(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> run(sql), sql);
        assertEquals(sqlState, e.getSQLState(), e.getMessage());
        assertTrue(e.getMessage().contains(messagePart), e.getMessage());
        return e;
    }
}
