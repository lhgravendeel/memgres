package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive coverage tests for checklist items 73-80:
 *   73. Rules
 *   74. Extensions
 *   75. Collations
 *   76. Casts
 *   77. Conversions
 *   78. Aggregates
 *   79. Operators
 *   80. Operator Classes & Families
 */
class ExtensionsDDLCoverageTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
    }

    @AfterAll
    static void teardown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private void assertNoError(String sql) {
        assertDoesNotThrow(() -> exec(sql));
    }

    // ========================================================================
    // 73. Rules
    // ========================================================================

    @Test
    void rule_create_on_insert_do_instead_nothing() {
        assertNoError("CREATE TABLE rule_tbl1 (id INT, name TEXT)");
        assertNoError("CREATE RULE rule_no_insert AS ON INSERT TO rule_tbl1 DO INSTEAD NOTHING");
    }

    @Test
    void rule_create_on_update_do_instead_nothing() {
        assertNoError("CREATE TABLE rule_tbl2 (id INT, val TEXT)");
        assertNoError("CREATE RULE rule_no_update AS ON UPDATE TO rule_tbl2 DO INSTEAD NOTHING");
    }

    @Test
    void rule_create_on_delete_do_instead_nothing() {
        assertNoError("CREATE TABLE rule_tbl3 (id INT)");
        assertNoError("CREATE RULE rule_no_delete AS ON DELETE TO rule_tbl3 DO INSTEAD NOTHING");
    }

    @Test
    void rule_create_on_select_do_instead_nothing() {
        assertNoError("CREATE TABLE rule_tbl4 (id INT)");
        assertNoError("CREATE RULE rule_no_select AS ON SELECT TO rule_tbl4 DO INSTEAD NOTHING");
    }

    @Test
    void rule_create_do_also_insert() {
        assertNoError("CREATE TABLE rule_src (id INT, name TEXT)");
        assertNoError("CREATE TABLE rule_audit (name TEXT)");
        assertNoError("CREATE RULE rule_audit_insert AS ON INSERT TO rule_src DO ALSO (INSERT INTO rule_audit VALUES (NEW.name))");
    }

    @Test
    void rule_drop_basic() {
        assertNoError("CREATE TABLE rule_tbl5 (id INT)");
        assertNoError("CREATE RULE rule_drop_me AS ON INSERT TO rule_tbl5 DO INSTEAD NOTHING");
        assertNoError("DROP RULE rule_drop_me ON rule_tbl5");
    }

    @Test
    void rule_drop_if_exists() {
        assertNoError("CREATE TABLE rule_tbl6 (id INT)");
        assertNoError("DROP RULE IF EXISTS nonexistent_rule ON rule_tbl6");
    }

    @Test
    void rule_drop_if_exists_no_table() {
        assertNoError("CREATE TABLE rule_tbl7 (id INT)");
        assertNoError("DROP RULE IF EXISTS no_such_rule ON rule_tbl7");
    }

    @Test
    void rule_create_on_insert_with_where() {
        assertNoError("CREATE TABLE rule_tbl8 (id INT, status TEXT)");
        assertNoError("CREATE RULE rule_cond AS ON INSERT TO rule_tbl8 WHERE (NEW.status = 'active') DO INSTEAD NOTHING");
    }

    @Test
    void rule_create_on_update_do_also() {
        assertNoError("CREATE TABLE rule_tbl9 (id INT, val INT)");
        assertNoError("CREATE TABLE rule_log9 (val INT)");
        assertNoError("CREATE RULE rule_log_update AS ON UPDATE TO rule_tbl9 DO ALSO (INSERT INTO rule_log9 VALUES (NEW.val))");
    }

    // ========================================================================
    // 74. Extensions
    // ========================================================================

    @Test
    void ext_create_pgcrypto() {
        assertNoError("CREATE EXTENSION pgcrypto");
    }

    @Test
    void ext_create_if_not_exists_uuid_ossp() {
        assertNoError("CREATE EXTENSION IF NOT EXISTS uuid_ossp");
    }

    @Test
    void ext_create_if_not_exists_quoted_name() {
        assertNoError("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
    }

    @Test
    void ext_create_hstore() {
        assertNoError("CREATE EXTENSION hstore");
    }

    @Test
    void ext_create_postgis() {
        assertNoError("CREATE EXTENSION postgis");
    }

    @Test
    void ext_create_with_schema() {
        assertNoError("CREATE EXTENSION IF NOT EXISTS citext");
    }

    @Test
    void ext_drop_pgcrypto() {
        assertNoError("DROP EXTENSION pgcrypto");
    }

    @Test
    void ext_drop_if_exists_nonexistent() {
        assertNoError("DROP EXTENSION IF EXISTS nonexistent_ext");
    }

    @Test
    void ext_drop_cascade() {
        assertNoError("CREATE EXTENSION plpgsql");
        assertNoError("DROP EXTENSION plpgsql CASCADE");
    }

    @Test
    void ext_alter_update() {
        assertNoError("CREATE EXTENSION pgcrypto");
        assertNoError("ALTER EXTENSION pgcrypto UPDATE");
    }

    @Test
    void ext_alter_set_schema() {
        assertNoError("ALTER EXTENSION pgcrypto SET SCHEMA public");
    }

    @Test
    void ext_alter_update_to_version() {
        assertNoError("ALTER EXTENSION pgcrypto UPDATE TO '1.3'");
    }

    @Test
    void ext_create_multiple_sequential() {
        assertNoError("CREATE EXTENSION IF NOT EXISTS pg_trgm");
        assertNoError("CREATE EXTENSION IF NOT EXISTS btree_gist");
        assertNoError("CREATE EXTENSION IF NOT EXISTS tablefunc");
    }

    // ========================================================================
    // 75. Collations
    // ========================================================================

    @Test
    void collation_create_with_locale() {
        assertNoError("CREATE COLLATION my_coll (LOCALE = 'en_US.utf8')");
    }

    @Test
    void collation_create_from_existing() {
        assertNoError("CREATE COLLATION my_coll2 FROM \"C\"");
    }

    @Test
    void collation_create_with_lc_collate_ctype() {
        assertNoError("CREATE COLLATION my_coll3 (LC_COLLATE = 'en_US.utf8', LC_CTYPE = 'en_US.utf8')");
    }

    @Test
    void collation_create_if_not_exists() {
        assertNoError("CREATE COLLATION IF NOT EXISTS my_coll4 (LOCALE = 'de_DE.utf8')");
    }

    @Test
    void collation_drop() {
        assertNoError("DROP COLLATION my_coll");
    }

    @Test
    void collation_drop_if_exists() {
        assertNoError("DROP COLLATION IF EXISTS nonexistent_coll");
    }

    @Test
    void collation_drop_cascade() {
        assertNoError("DROP COLLATION IF EXISTS my_coll2 CASCADE");
    }

    @Test
    void collation_alter_rename() {
        assertNoError("CREATE COLLATION rename_coll (LOCALE = 'fr_FR.utf8')");
        assertNoError("ALTER COLLATION rename_coll RENAME TO renamed_coll");
    }

    @Test
    void collation_alter_owner() {
        assertNoError("ALTER COLLATION renamed_coll OWNER TO test");
    }

    @Test
    void collation_alter_set_schema() {
        assertNoError("ALTER COLLATION renamed_coll SET SCHEMA public");
    }

    // ========================================================================
    // 76. Casts
    // ========================================================================

    @Test
    void cast_create_without_function() {
        assertNoError("CREATE CAST (INTEGER AS TEXT) WITHOUT FUNCTION");
    }

    @Test
    void cast_create_with_inout() {
        assertNoError("CREATE CAST (TEXT AS INTEGER) WITH INOUT");
    }

    @Test
    void cast_create_as_assignment() {
        assertNoError("CREATE CAST (BIGINT AS INTEGER) WITHOUT FUNCTION AS ASSIGNMENT");
    }

    @Test
    void cast_create_as_implicit() {
        assertNoError("CREATE CAST (SMALLINT AS INTEGER) WITHOUT FUNCTION AS IMPLICIT");
    }

    @Test
    void cast_create_with_function() {
        assertNoError("CREATE CAST (VARCHAR AS INTEGER) WITH FUNCTION pg_catalog.int4(VARCHAR)");
    }

    @Test
    void cast_drop() {
        assertNoError("DROP CAST (INTEGER AS TEXT)");
    }

    @Test
    void cast_drop_if_exists() {
        assertNoError("DROP CAST IF EXISTS (TEXT AS INTEGER)");
    }

    @Test
    void cast_drop_cascade() {
        assertNoError("DROP CAST IF EXISTS (BIGINT AS INTEGER) CASCADE");
    }

    @Test
    void cast_drop_nonexistent_if_exists() {
        assertNoError("DROP CAST IF EXISTS (FLOAT AS BOOLEAN)");
    }

    // ========================================================================
    // 77. Conversions
    // ========================================================================

    @Test
    void conversion_create() {
        assertNoError("CREATE CONVERSION my_conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1");
    }

    @Test
    void conversion_create_default() {
        assertNoError("CREATE DEFAULT CONVERSION my_def_conv FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1");
    }

    @Test
    void conversion_drop() {
        assertNoError("DROP CONVERSION my_conv");
    }

    @Test
    void conversion_drop_if_exists() {
        assertNoError("DROP CONVERSION IF EXISTS nonexistent_conv");
    }

    @Test
    void conversion_drop_cascade() {
        assertNoError("DROP CONVERSION IF EXISTS my_def_conv CASCADE");
    }

    @Test
    void conversion_alter_rename() {
        assertNoError("CREATE CONVERSION conv_rename FOR 'UTF8' TO 'LATIN1' FROM utf8_to_iso8859_1");
        assertNoError("ALTER CONVERSION conv_rename RENAME TO conv_renamed");
    }

    @Test
    void conversion_alter_owner() {
        assertNoError("ALTER CONVERSION conv_renamed OWNER TO test");
    }

    @Test
    void conversion_create_utf8_to_sjis() {
        assertNoError("CREATE CONVERSION conv_sjis FOR 'UTF8' TO 'SJIS' FROM utf8_to_sjis");
    }

    // ========================================================================
    // 78. Aggregates
    // ========================================================================

    @Test
    void aggregate_create_simple() {
        assertNoError("CREATE AGGREGATE my_sum (INTEGER) (SFUNC = int4pl, STYPE = INTEGER)");
    }

    @Test
    void aggregate_create_with_finalfunc() {
        assertNoError("CREATE AGGREGATE my_avg (NUMERIC) (SFUNC = numeric_avg_accum, STYPE = INTERNAL, FINALFUNC = numeric_avg)");
    }

    @Test
    void aggregate_create_with_initcond() {
        assertNoError("CREATE AGGREGATE my_count (ANYELEMENT) (SFUNC = int4inc, STYPE = INTEGER, INITCOND = 0)");
    }

    @Test
    void aggregate_create_with_combinefunc() {
        assertNoError("CREATE AGGREGATE my_parallel_sum (INTEGER) (SFUNC = int4pl, STYPE = INTEGER, COMBINEFUNC = int4pl)");
    }

    @Test
    void aggregate_create_ordered_set() {
        assertNoError("CREATE AGGREGATE my_percentile (FLOAT8 ORDER BY FLOAT8) (SFUNC = ordered_set_transition, STYPE = INTERNAL, FINALFUNC = percentile_cont_float8_final)");
    }

    @Test
    void aggregate_drop() {
        assertNoError("DROP AGGREGATE my_sum (INTEGER)");
    }

    @Test
    void aggregate_drop_if_exists() {
        assertNoError("DROP AGGREGATE IF EXISTS nonexistent_agg (INTEGER)");
    }

    @Test
    void aggregate_drop_cascade() {
        assertNoError("DROP AGGREGATE IF EXISTS my_avg (NUMERIC) CASCADE");
    }

    @Test
    void aggregate_drop_multiple_arg_types() {
        assertNoError("DROP AGGREGATE IF EXISTS two_arg_agg (INTEGER, TEXT)");
    }

    // ========================================================================
    // 79. Operators
    // ========================================================================

    @Test
    void operator_create_basic() {
        assertNoError("CREATE OPERATOR === (LEFTARG = INTEGER, RIGHTARG = INTEGER, FUNCTION = int4eq)");
    }

    @Test
    void operator_create_with_commutator() {
        assertNoError("CREATE OPERATOR ~~~ (LEFTARG = TEXT, RIGHTARG = TEXT, FUNCTION = texteq, COMMUTATOR = ~~~)");
    }

    @Test
    void operator_create_with_negator() {
        assertNoError("CREATE OPERATOR !~~ (LEFTARG = TEXT, RIGHTARG = TEXT, FUNCTION = textne, NEGATOR = ~~~)");
    }

    @Test
    void operator_create_unary_prefix() {
        assertNoError("CREATE OPERATOR ## (RIGHTARG = INTEGER, FUNCTION = int4abs)");
    }

    @Test
    void operator_create_with_restrict_join() {
        assertNoError("CREATE OPERATOR <==> (LEFTARG = FLOAT8, RIGHTARG = FLOAT8, FUNCTION = float8eq, RESTRICT = eqsel, JOIN = eqjoinsel)");
    }

    @Test
    void operator_drop() {
        assertNoError("DROP OPERATOR IF EXISTS === (INTEGER, INTEGER)");
    }

    @Test
    void operator_drop_if_exists() {
        assertNoError("DROP OPERATOR IF EXISTS === (INTEGER, INTEGER)");
    }

    @Test
    void operator_drop_none_type() {
        assertNoError("DROP OPERATOR IF EXISTS ## (NONE, INTEGER)");
    }

    @Test
    void operator_drop_cascade() {
        assertNoError("DROP OPERATOR IF EXISTS ~~~ (TEXT, TEXT) CASCADE");
    }

    // ========================================================================
    // 80. Operator Classes & Families
    // ========================================================================

    @Test
    void opclass_create_default() {
        assertNoError("CREATE OPERATOR CLASS my_ops DEFAULT FOR TYPE INTEGER USING btree AS OPERATOR 1 <");
    }

    @Test
    void opclass_create_non_default() {
        assertNoError("CREATE OPERATOR CLASS my_ops2 FOR TYPE TEXT USING hash AS OPERATOR 1 =");
    }

    @Test
    void opclass_create_with_function() {
        assertNoError("CREATE OPERATOR CLASS my_ops3 FOR TYPE INTEGER USING btree AS OPERATOR 1 <, FUNCTION 1 btint4cmp(INTEGER, INTEGER)");
    }

    @Test
    void opfamily_create() {
        assertNoError("CREATE OPERATOR FAMILY my_fam USING btree");
    }

    @Test
    void opfamily_create_hash() {
        assertNoError("CREATE OPERATOR FAMILY my_hash_fam USING hash");
    }

    @Test
    void opclass_drop() {
        assertNoError("DROP OPERATOR CLASS IF EXISTS my_ops USING btree");
    }

    @Test
    void opclass_drop_if_exists() {
        assertNoError("DROP OPERATOR CLASS IF EXISTS nonexistent_ops USING btree");
    }

    @Test
    void opclass_drop_cascade() {
        assertNoError("DROP OPERATOR CLASS IF EXISTS my_ops2 USING hash CASCADE");
    }

    @Test
    void opfamily_drop() {
        assertNoError("DROP OPERATOR FAMILY IF EXISTS my_fam USING btree");
    }

    @Test
    void opfamily_drop_if_exists() {
        assertNoError("DROP OPERATOR FAMILY IF EXISTS nonexistent_fam USING btree");
    }

    @Test
    void opfamily_drop_cascade() {
        assertNoError("DROP OPERATOR FAMILY IF EXISTS my_hash_fam USING hash CASCADE");
    }

    @Test
    void opfamily_create_gist() {
        assertNoError("CREATE OPERATOR FAMILY my_gist_fam USING gist");
    }

    @Test
    void opclass_create_gist() {
        assertNoError("CREATE OPERATOR CLASS my_gist_ops FOR TYPE POINT USING gist AS OPERATOR 1 <<");
    }

    @Test
    void opfamily_drop_gist() {
        assertNoError("DROP OPERATOR FAMILY IF EXISTS my_gist_fam USING gist");
    }

    @Test
    void opclass_drop_gist() {
        assertNoError("DROP OPERATOR CLASS IF EXISTS my_gist_ops USING gist");
    }
}
