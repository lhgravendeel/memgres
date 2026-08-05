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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An OID names an object, not a name. PostgreSQL assigns one when the object is created and never
 * moves it: a tool that reads {@code 't'::regclass::oid}, watches somebody run
 * {@code ALTER TABLE t RENAME TO t2}, and looks the number up again must find {@code t2}; the old
 * name must then be {@code 42P01}; and a number whose object has been dropped must print as the
 * number rather than name something that is gone.
 *
 * <p>Memgres derived an OID from the current (schema, name), so a rename minted a second one and
 * everything filed against the first — the comment, the grants, the index built on the table —
 * was orphaned along with it. These checks pin the answers measured on PostgreSQL 18. No OID
 * value is asserted: PostgreSQL's are arbitrary, so each test stashes the number it read in a
 * table and compares it with what the catalog says afterwards.</p>
 */
class ObjectIdentityAcrossRenameTest {

    private static Memgres memgres;
    private static Connection conn;
    /** A second and a third connection: an OID belongs to the object, not to who is asking. */
    private static Connection other;
    private static Connection third;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        other = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
        third = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (other != null) other.close();
        if (third != null) third.close();
        if (memgres != null) memgres.close();
    }

    // ---- the OID stays with the object -----------------------------------------------------

    @Test
    void an_oid_survives_a_table_rename() throws SQLException {
        run("CREATE TABLE oid_t (a int PRIMARY KEY, b text)");
        run("CREATE TABLE oid_t_keep AS SELECT 'oid_t'::regclass::oid AS o");
        run("ALTER TABLE oid_t RENAME TO oid_t2");

        assertTrue(bool("SELECT (SELECT o FROM oid_t_keep) = 'oid_t2'::regclass::oid"));
        assertEquals("oid_t2", one("SELECT (SELECT o FROM oid_t_keep)::regclass::text"));
        assertEquals("oid_t2", one("SELECT relname FROM pg_class WHERE oid = (SELECT o FROM oid_t_keep)"));
    }

    @Test
    void the_name_a_rename_freed_no_longer_resolves() throws SQLException {
        run("CREATE TABLE oid_free (a int)");
        run("ALTER TABLE oid_free RENAME TO oid_free2");

        assertEquals("42P01", stateOf("SELECT 'oid_free'::regclass::oid"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_class WHERE relname = 'oid_free'"));
    }

    @Test
    void an_oid_survives_a_schema_move() throws SQLException {
        run("CREATE SCHEMA oid_moved");
        run("CREATE TABLE oid_m (a int)");
        run("CREATE TABLE oid_m_keep AS SELECT 'oid_m'::regclass::oid AS o");
        run("ALTER TABLE oid_m SET SCHEMA oid_moved");

        assertTrue(bool("SELECT (SELECT o FROM oid_m_keep) = 'oid_moved.oid_m'::regclass::oid"));
        assertEquals("oid_moved", one("SELECT n.nspname FROM pg_class c JOIN pg_namespace n"
                + " ON n.oid = c.relnamespace WHERE c.oid = (SELECT o FROM oid_m_keep)"));
    }

    @Test
    void a_sequence_view_and_index_keep_their_oid_across_a_rename() throws SQLException {
        run("CREATE SEQUENCE oid_q");
        run("CREATE VIEW oid_v AS SELECT 1 AS x");
        run("CREATE TABLE oid_it (a int, b int)");
        run("CREATE INDEX oid_ix ON oid_it (b)");
        run("CREATE TABLE oid_rel_keep AS SELECT 'oid_q'::regclass::oid AS q,"
                + " 'oid_v'::regclass::oid AS v, 'oid_ix'::regclass::oid AS i");
        run("ALTER SEQUENCE oid_q RENAME TO oid_q2");
        run("ALTER VIEW oid_v RENAME TO oid_v2");
        run("ALTER INDEX oid_ix RENAME TO oid_ix2");

        assertTrue(bool("SELECT (SELECT q FROM oid_rel_keep) = 'oid_q2'::regclass::oid"));
        assertTrue(bool("SELECT (SELECT v FROM oid_rel_keep) = 'oid_v2'::regclass::oid"));
        assertTrue(bool("SELECT (SELECT i FROM oid_rel_keep) = 'oid_ix2'::regclass::oid"));
        assertEquals("oid_ix2", one("SELECT relname FROM pg_class WHERE oid = (SELECT i FROM oid_rel_keep)"));
    }

    @Test
    void an_enum_and_a_domain_keep_their_oid_across_a_rename() throws SQLException {
        run("CREATE TYPE oid_e AS ENUM ('a','b')");
        run("CREATE DOMAIN oid_d AS int CHECK (VALUE > 0)");
        run("CREATE TABLE oid_type_keep AS SELECT 'oid_e'::regtype::oid AS e, 'oid_d'::regtype::oid AS d");
        run("ALTER TYPE oid_e RENAME TO oid_e2");
        run("ALTER DOMAIN oid_d RENAME TO oid_d2");

        assertTrue(bool("SELECT (SELECT e FROM oid_type_keep) = 'oid_e2'::regtype::oid"));
        assertTrue(bool("SELECT (SELECT d FROM oid_type_keep) = 'oid_d2'::regtype::oid"));
    }

    /**
     * A column records the type it was declared with, and PostgreSQL records it as an OID, so a
     * type rename shows through on the column at once.
     */
    @Test
    void a_column_reports_the_renamed_type() throws SQLException {
        run("CREATE TYPE oid_ce AS ENUM ('a','b')");
        run("CREATE TABLE oid_cet (c oid_ce)");
        run("ALTER TYPE oid_ce RENAME TO oid_ce2");

        assertEquals("oid_ce2", one("SELECT atttypid::regtype::text FROM pg_attribute"
                + " WHERE attrelid = 'oid_cet'::regclass AND attname = 'c'"));
        assertEquals("oid_ce2", one("SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                + " WHERE attrelid = 'oid_cet'::regclass AND attname = 'c'"));
    }

    // ---- a dropped OID stops naming anything -----------------------------------------------

    @Test
    void a_dropped_oid_prints_as_its_number() throws SQLException {
        run("CREATE TABLE oid_gone (a int)");
        run("CREATE TABLE oid_gone_keep AS SELECT 'oid_gone'::regclass::oid AS o");
        run("DROP TABLE oid_gone");

        assertTrue(bool("SELECT (SELECT o::regclass::text FROM oid_gone_keep)"
                + " = (SELECT o::text FROM oid_gone_keep)"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_class WHERE oid = (SELECT o FROM oid_gone_keep)"));
    }

    @Test
    void a_table_created_again_under_a_dropped_name_gets_a_new_oid() throws SQLException {
        run("CREATE TABLE oid_again (a int)");
        run("CREATE TABLE oid_again_keep AS SELECT 'oid_again'::regclass::oid AS o");
        run("DROP TABLE oid_again");
        run("CREATE TABLE oid_again (a int)");

        assertFalse(bool("SELECT (SELECT o FROM oid_again_keep) = 'oid_again'::regclass::oid"));
    }

    // ---- what is filed under the name follows the object ------------------------------------

    @Test
    void comments_follow_a_renamed_table() throws SQLException {
        run("CREATE TABLE oid_c (a int, b text)");
        run("COMMENT ON TABLE oid_c IS 'the table'");
        run("COMMENT ON COLUMN oid_c.b IS 'the column'");
        run("ALTER TABLE oid_c RENAME TO oid_c2");

        assertEquals("the table", one("SELECT obj_description('oid_c2'::regclass, 'pg_class')"));
        assertEquals("the column", one("SELECT col_description('oid_c2'::regclass, 2)"));
    }

    @Test
    void a_comment_does_not_outlive_the_object_it_described() throws SQLException {
        run("CREATE TABLE oid_cc (a int)");
        run("COMMENT ON TABLE oid_cc IS 'said of the first one'");
        run("DROP TABLE oid_cc");
        run("CREATE TABLE oid_cc (a int)");

        assertNull(one("SELECT obj_description('oid_cc'::regclass, 'pg_class')"));
    }

    @Test
    void a_grant_follows_a_renamed_table() throws SQLException {
        run("CREATE TABLE oid_p (a int)");
        run("GRANT SELECT ON oid_p TO PUBLIC");
        run("ALTER TABLE oid_p RENAME TO oid_p2");

        assertTrue(bool("SELECT has_table_privilege('public', 'oid_p2', 'SELECT')"));
    }

    @Test
    void an_index_follows_the_table_it_was_built_on() throws SQLException {
        run("CREATE TABLE oid_u (a int, b int)");
        run("CREATE INDEX oid_uix ON oid_u (b)");
        run("ALTER TABLE oid_u RENAME TO oid_u2");

        assertEquals("oid_uix", one("SELECT indexrelid::regclass::text FROM pg_index"
                + " WHERE indrelid = 'oid_u2'::regclass ORDER BY 1"));
        assertEquals("oid_uix", one("SELECT indexname FROM pg_indexes WHERE tablename = 'oid_u2'"));

        run("CREATE SCHEMA oid_uschema");
        run("ALTER TABLE oid_u2 SET SCHEMA oid_uschema");
        assertEquals("1", one("SELECT count(*)::text FROM pg_index"
                + " WHERE indrelid = 'oid_uschema.oid_u2'::regclass"));
    }

    /**
     * A statement that drops one table and creates another is not a rename, however alike the two
     * are, and nothing may be carried across from the one to the other.
     */
    @Test
    void a_drop_beside_a_create_is_not_read_as_a_rename() throws SQLException {
        run("CREATE TABLE oid_x (a int, b text)");
        run("COMMENT ON TABLE oid_x IS 'said of x'");
        run("DO $$ BEGIN DROP TABLE oid_x; CREATE TABLE oid_y (a int, b text); END $$");

        assertNull(one("SELECT obj_description('oid_y'::regclass, 'pg_class')"));
    }

    // ---- an object is not identified by watching names come and go ---------------------------

    /**
     * The identity register used to take a census of every relation's name each statement and read
     * a name that had gone beside a name that had arrived as a rename. A rename run on one
     * connection reached the next connection's census as two unrelated changes at once, which was
     * read as two drops — and both tables' comments were destroyed even though both tables came
     * back under the names they started with.
     */
    @Test
    void renames_from_several_connections_do_not_cost_a_comment() throws SQLException {
        run("CREATE TABLE oid_ma (i int)");
        run("CREATE TABLE oid_mb (j int)");
        run("COMMENT ON TABLE oid_ma IS 'ac'");
        run("COMMENT ON TABLE oid_mb IS 'bc'");
        one("SELECT 1");
        on(other, "ALTER TABLE oid_ma RENAME TO oid_ma9");
        on(third, "ALTER TABLE oid_mb RENAME TO oid_mb9");
        one("SELECT count(*) FROM pg_class WHERE relname LIKE 'oid\\_m%'");
        run("CREATE TABLE oid_mz (k int)");
        one("SELECT count(*) FROM pg_class WHERE relname LIKE 'oid\\_m%'");
        on(other, "ALTER TABLE oid_ma9 RENAME TO oid_ma");
        on(third, "ALTER TABLE oid_mb9 RENAME TO oid_mb");

        assertEquals("ac", one("SELECT obj_description('oid_ma'::regclass)"));
        assertEquals("bc", one("SELECT obj_description('oid_mb'::regclass)"));
    }

    /** Renaming a table back and forth is not a drop, however many times it is done. */
    @Test
    void a_comment_and_an_index_survive_a_run_of_renames() throws SQLException {
        run("CREATE TABLE oid_rr (i int)");
        run("COMMENT ON TABLE oid_rr IS 'still here'");
        run("CREATE INDEX oid_rrix ON oid_rr (i)");
        for (int round = 0; round < 6; round++) {
            on(other, "ALTER TABLE oid_rr RENAME TO oid_rr9");
            one("SELECT count(*) FROM pg_class WHERE relname LIKE 'oid\\_rr%'");
            on(other, "ALTER TABLE oid_rr9 RENAME TO oid_rr");
        }

        assertEquals("still here", one("SELECT obj_description('oid_rr'::regclass)"));
        assertEquals("oid_rrix", one("SELECT indexname FROM pg_indexes WHERE tablename = 'oid_rr'"));
    }

    /**
     * Renaming one schema's table says nothing about the table of the same name in another
     * schema, and must not move what was said about it.
     */
    @Test
    void a_rename_leaves_another_schemas_relation_of_the_same_name_alone() throws SQLException {
        run("CREATE SCHEMA oid_sa");
        run("CREATE SCHEMA oid_sb");
        run("CREATE TABLE oid_sa.t (i int)");
        run("CREATE TABLE oid_sb.t (i int)");
        run("COMMENT ON TABLE oid_sb.t IS 'keepme'");
        run("COMMENT ON COLUMN oid_sb.t.i IS 'colkeep'");
        run("ALTER TABLE oid_sa.t RENAME TO t2");

        assertEquals("keepme", one("SELECT obj_description('oid_sb.t'::regclass)"));
        assertEquals("colkeep", one("SELECT col_description('oid_sb.t'::regclass, 1)"));
        assertNull(one("SELECT obj_description('oid_sa.t2'::regclass)"));
    }

    @Test
    void an_index_rename_leaves_another_schemas_index_comment_alone() throws SQLException {
        run("CREATE SCHEMA oid_ia");
        run("CREATE SCHEMA oid_ib");
        run("CREATE TABLE oid_ia.t (i int)");
        run("CREATE TABLE oid_ib.t (i int)");
        run("CREATE INDEX icx ON oid_ia.t (i)");
        run("CREATE INDEX icx ON oid_ib.t (i)");
        run("COMMENT ON INDEX oid_ib.icx IS 'ickeep'");
        run("ALTER INDEX oid_ia.icx RENAME TO icx2");

        assertEquals("ickeep", one("SELECT obj_description('oid_ib.icx'::regclass)"));
        assertNull(one("SELECT obj_description('oid_ia.icx2'::regclass)"));
    }

    /** The same for an owner, which memgres files against a sequence's bare name. */
    @Test
    void a_sequence_rename_leaves_another_schemas_sequence_owner_alone() throws SQLException {
        run("CREATE ROLE oid_r3");
        run("CREATE SCHEMA oid_qa");
        run("CREATE SCHEMA oid_qb");
        run("CREATE SEQUENCE oid_qa.sq");
        run("CREATE SEQUENCE oid_qb.sq");
        run("ALTER SEQUENCE oid_qb.sq OWNER TO oid_r3");
        run("ALTER SEQUENCE oid_qa.sq RENAME TO sq2");

        assertTrue(bool("SELECT relowner = (SELECT oid FROM pg_authid WHERE rolname = 'oid_r3')"
                + " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE c.relname = 'sq' AND n.nspname = 'oid_qb'"));
        assertFalse(bool("SELECT relowner = (SELECT oid FROM pg_authid WHERE rolname = 'oid_r3')"
                + " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " WHERE c.relname = 'sq2' AND n.nspname = 'oid_qa'"));
    }

    /** A dropped OID is not handed to the table a statement creates beside the drop. */
    @Test
    void a_dropped_table_does_not_lend_its_oid_to_one_created_beside_it() throws SQLException {
        run("CREATE TABLE oid_drp1 (i int)");
        run("CREATE TABLE oid_dkeep AS SELECT 'oid_drp1'::regclass::oid AS o");
        run("DO $$ BEGIN DROP TABLE oid_drp1; CREATE TABLE oid_drp2 (i int); END $$");

        assertFalse(bool("SELECT (SELECT o FROM oid_dkeep) = 'oid_drp2'::regclass::oid"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_class"
                + " WHERE oid = (SELECT o FROM oid_dkeep)"));
    }

    /** An OID belongs to the object, so the connection that did not rename it agrees. */
    @Test
    void another_connection_reads_the_same_oid_after_a_rename() throws SQLException {
        run("CREATE TABLE oid_cc2 (i int)");
        run("CREATE TABLE oid_cc2_keep AS SELECT 'oid_cc2'::regclass::oid AS o");
        run("ALTER TABLE oid_cc2 RENAME TO oid_cc3");

        assertTrue(boolOn(other, "SELECT (SELECT o FROM oid_cc2_keep) = 'oid_cc3'::regclass::oid"));
        assertEquals("0", oneOn(other, "SELECT count(*)::text FROM pg_class"
                + " WHERE relname = 'oid_cc2'"));
    }

    /** A trigger and a rule are on the relation, and PostgreSQL's rename does not disturb them. */
    @Test
    void a_trigger_and_a_rule_follow_a_renamed_table() throws SQLException {
        run("CREATE TABLE oid_tr (a int)");
        run("CREATE TABLE oid_trlog (a int)");
        run("CREATE FUNCTION oid_trf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
        run("CREATE TRIGGER oid_trg BEFORE INSERT ON oid_tr FOR EACH ROW EXECUTE FUNCTION oid_trf()");
        run("CREATE RULE oid_rul AS ON INSERT TO oid_tr DO ALSO INSERT INTO oid_trlog VALUES (1)");
        run("ALTER TABLE oid_tr RENAME TO oid_tr2");

        assertEquals("oid_tr2", one("SELECT tgrelid::regclass::text FROM pg_trigger"
                + " WHERE tgname = 'oid_trg'"));
        assertEquals("oid_tr2", one("SELECT tablename FROM pg_rules WHERE rulename = 'oid_rul'"));
        run("INSERT INTO oid_tr2 VALUES (1)");
        assertEquals("1", one("SELECT count(*)::text FROM oid_trlog"));
    }

    /** Dropping a schema takes its relations with it, and their OIDs stop naming anything. */
    @Test
    void dropping_a_schema_retires_the_oids_of_what_was_in_it() throws SQLException {
        run("CREATE SCHEMA oid_dropme");
        run("CREATE TABLE oid_dropme.k (a int)");
        run("CREATE TABLE oid_dropme_keep AS SELECT 'oid_dropme.k'::regclass::oid AS o");
        run("DROP SCHEMA oid_dropme CASCADE");

        assertTrue(bool("SELECT (SELECT o::regclass::text FROM oid_dropme_keep)"
                + " = (SELECT o::text FROM oid_dropme_keep)"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_class"
                + " WHERE oid = (SELECT o FROM oid_dropme_keep)"));
    }

    /** A view, a sequence and an enum created again under a dropped name are new objects. */
    @Test
    void a_dropped_view_sequence_and_type_do_not_lend_their_oids() throws SQLException {
        run("CREATE VIEW oid_gv AS SELECT 1 AS x");
        run("CREATE SEQUENCE oid_gs");
        run("CREATE TYPE oid_ge AS ENUM ('a')");
        run("CREATE TABLE oid_gk AS SELECT 'oid_gv'::regclass::oid AS v,"
                + " 'oid_gs'::regclass::oid AS s, 'oid_ge'::regtype::oid AS e");
        run("DROP VIEW oid_gv");
        run("DROP SEQUENCE oid_gs");
        run("DROP TYPE oid_ge");
        run("CREATE VIEW oid_gv AS SELECT 1 AS x");
        run("CREATE SEQUENCE oid_gs");
        run("CREATE TYPE oid_ge AS ENUM ('a')");

        assertFalse(bool("SELECT (SELECT v FROM oid_gk) = 'oid_gv'::regclass::oid"));
        assertFalse(bool("SELECT (SELECT s FROM oid_gk) = 'oid_gs'::regclass::oid"));
        assertFalse(bool("SELECT (SELECT e FROM oid_gk) = 'oid_ge'::regtype::oid"));
    }

    /** A materialized view is renamed like any other relation, and keeps its number. */
    @Test
    void a_materialized_view_keeps_its_oid_across_a_rename() throws SQLException {
        run("CREATE MATERIALIZED VIEW oid_mv AS SELECT 1 AS x");
        run("CREATE TABLE oid_mv_keep AS SELECT 'oid_mv'::regclass::oid AS o");
        run("ALTER MATERIALIZED VIEW oid_mv RENAME TO oid_mv2");

        assertTrue(bool("SELECT (SELECT o FROM oid_mv_keep) = 'oid_mv2'::regclass::oid"));
        assertEquals("42P01", stateOf("SELECT 'oid_mv'::regclass::oid"));
    }

    // ---- helpers ----------------------------------------------------------------------------

    private static void run(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static void on(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        return oneOn(conn, sql);
    }

    private static String oneOn(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next()) return null;
            return rs.getString(1);
        }
    }

    private static boolean bool(String sql) throws SQLException {
        return boolOn(conn, sql);
    }

    private static boolean boolOn(Connection c, String sql) throws SQLException {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() && rs.getBoolean(1);
        }
    }

    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }
}
