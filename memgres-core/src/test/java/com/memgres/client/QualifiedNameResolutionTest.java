package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The schema is part of which object a name refers to.
 *
 * <p>The parsers read a qualified name with {@code String x = readIdentifier(); if (match(DOT)) x =
 * readIdentifier();}, which keeps the name and throws the schema away. So {@code ALTER DOMAIN
 * s.d SET NOT NULL} altered whichever d was found first, a trigger written to call {@code s.f()}
 * called the f in public, and rolling back a view created in one schema removed another schema's
 * view of the same name. Reading an unqualified name reached past the search path for the same
 * reason: a type in a schema the session cannot see was still reachable by its bare name.
 */
class QualifiedNameResolutionTest {

    static Memgres memgres;
    static Connection conn;

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

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    /** Rolling back a create in one schema leaves another schema's object alone. */
    @Test
    void aRollbackRemovesOnlyWhatItCreated() throws Exception {
        exec("CREATE SCHEMA zz_qr_vs");
        exec("CREATE VIEW zz_qr_v AS SELECT 1 AS a");
        conn.setAutoCommit(false);
        try {
            exec("CREATE VIEW zz_qr_vs.zz_qr_v AS SELECT 2 AS a");
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
        assertEquals("1", scalar("SELECT a FROM public.zz_qr_v"));
        assertEquals("1", scalar("SELECT a FROM zz_qr_v"));
    }

    /** Only the search path answers an unqualified type name. */
    @Test
    void aTypeOffThePathIsNotReachableUnqualified() throws Exception {
        exec("CREATE SCHEMA zz_qr_n1");
        exec("CREATE TYPE zz_qr_n1.zz_qr_e AS ENUM ('a')");
        assertEquals("42704", refused("SELECT 'a'::zz_qr_e").getSQLState());
        assertEquals("a", scalar("SELECT 'a'::zz_qr_n1.zz_qr_e"));
        // And the column is named after the type, not after where it lives.
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT 'a'::zz_qr_n1.zz_qr_e")) {
            assertEquals("zz_qr_e", rs.getMetaData().getColumnName(1));
        }
    }

    /** Visible means reachable by a bare name, which is what the search path decides. */
    @Test
    void visibilityFollowsTheSearchPath() throws Exception {
        exec("CREATE SCHEMA zz_qr_qv");
        exec("CREATE TABLE zz_qr_qv.t (a int)");
        exec("CREATE TABLE zz_qr_here (a int)");
        assertEquals("f", scalar("SELECT pg_table_is_visible('zz_qr_qv.t'::regclass)"));
        assertEquals("t", scalar("SELECT pg_table_is_visible('zz_qr_here'::regclass)"));
    }

    /** pg_temp stands where the search path names it, and first only when it does not. */
    @Test
    void theTemporarySchemaStandsWhereItIsNamed() throws Exception {
        exec("CREATE TABLE zz_qr_shadow (src text)");
        exec("INSERT INTO zz_qr_shadow VALUES ('permanent')");
        exec("CREATE TEMP TABLE zz_qr_shadow (src text)");
        exec("INSERT INTO zz_qr_shadow VALUES ('temp')");
        assertEquals("temp", scalar("SELECT src FROM zz_qr_shadow"));
        try {
            exec("SET search_path = public, pg_temp");
            assertEquals("permanent", scalar("SELECT src FROM zz_qr_shadow"));
            exec("SET search_path = pg_temp, public");
            assertEquals("temp", scalar("SELECT src FROM zz_qr_shadow"));
        } finally {
            exec("RESET search_path");
        }
    }

    /** A view definition names the view it was asked for, schemas and all. */
    @Test
    void aViewDefinitionKeepsItsSchemas() throws Exception {
        exec("CREATE SCHEMA zz_qr_c1");
        exec("CREATE SCHEMA zz_qr_c2");
        exec("CREATE TABLE zz_qr_c1.b (id int)");
        exec("CREATE TABLE zz_qr_c2.b (id int)");
        exec("CREATE VIEW zz_qr_c1.zz_qr_vw AS SELECT id AS one FROM zz_qr_c1.b");
        exec("CREATE VIEW zz_qr_c2.zz_qr_vw AS SELECT id AS two FROM zz_qr_c2.b");
        String def = scalar("SELECT pg_get_viewdef('zz_qr_c2.zz_qr_vw'::regclass)");
        assertTrue(def.contains("AS two"), def);
        assertTrue(def.contains("zz_qr_c2.b"), def);
    }

    /** ALTER DOMAIN alters the domain it names. */
    @Test
    void alterDomainNamesOneDomain() throws Exception {
        exec("CREATE SCHEMA zz_qr_ds");
        exec("CREATE DOMAIN zz_qr_dm AS int");
        exec("CREATE DOMAIN zz_qr_ds.zz_qr_dm AS int");
        exec("ALTER DOMAIN zz_qr_ds.zz_qr_dm SET NOT NULL");
        assertEquals("t", scalar("SELECT NULL::zz_qr_dm IS NULL"));
    }

    /** A trigger calls the function it names, and counts against its own table. */
    @Test
    void aTriggerCallsTheFunctionItNames() throws Exception {
        exec("CREATE SCHEMA zz_qr_fs");
        exec("CREATE TABLE zz_qr_ft (id int, tag text)");
        exec("CREATE FUNCTION zz_qr_gf() RETURNS trigger AS $$ BEGIN NEW.tag := 'public';"
                + " RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE FUNCTION zz_qr_fs.zz_qr_gf() RETURNS trigger AS $$ BEGIN NEW.tag := 'schema';"
                + " RETURN NEW; END; $$ LANGUAGE plpgsql");
        exec("CREATE TRIGGER zz_qr_trg BEFORE INSERT ON zz_qr_ft"
                + " FOR EACH ROW EXECUTE FUNCTION zz_qr_fs.zz_qr_gf()");
        exec("INSERT INTO zz_qr_ft VALUES (1, 'orig')");
        assertEquals("schema", scalar("SELECT tag FROM zz_qr_ft"));
    }

    /** A table carries its own triggers, not those of a same-named table elsewhere. */
    @Test
    void triggersBelongToTheirOwnTable() throws Exception {
        exec("CREATE SCHEMA zz_qr_s1");
        exec("CREATE TABLE zz_qr_tt (id int)");
        exec("CREATE TABLE zz_qr_s1.zz_qr_tt (id int)");
        exec("CREATE FUNCTION zz_qr_tgf() RETURNS trigger AS 'BEGIN RETURN NEW; END'"
                + " LANGUAGE plpgsql");
        exec("CREATE TRIGGER zz_qr_tr BEFORE INSERT ON zz_qr_tt"
                + " FOR EACH ROW EXECUTE FUNCTION zz_qr_tgf()");
        assertEquals("f", scalar("SELECT hastriggers FROM pg_tables"
                + " WHERE tablename = 'zz_qr_tt' AND schemaname = 'zz_qr_s1'"));
        assertEquals("t", scalar("SELECT hastriggers FROM pg_tables"
                + " WHERE tablename = 'zz_qr_tt' AND schemaname = 'public'"));

        exec("CREATE TABLE zz_qr_ca (id int primary key)");
        exec("CREATE TABLE zz_qr_cb (id int primary key)");
        exec("CREATE TRIGGER zz_qr_t1 BEFORE INSERT ON zz_qr_ca"
                + " FOR EACH ROW EXECUTE FUNCTION zz_qr_tgf()");
        exec("CREATE TRIGGER zz_qr_t1 BEFORE INSERT ON zz_qr_cb"
                + " FOR EACH ROW EXECUTE FUNCTION zz_qr_tgf()");
        assertEquals("2", scalar("SELECT count(DISTINCT event_object_table)"
                + " FROM information_schema.triggers WHERE trigger_name = 'zz_qr_t1'"));
    }

    /** A schema holds one function of a name and argument list. */
    @Test
    void aFunctionCannotMoveOntoAnother() throws Exception {
        exec("CREATE SCHEMA zz_qr_qs");
        exec("CREATE SCHEMA zz_qr_qs2");
        exec("CREATE FUNCTION zz_qr_qs.who() RETURNS text LANGUAGE sql AS $$ SELECT 'one'::text $$");
        exec("CREATE FUNCTION zz_qr_qs2.who() RETURNS text LANGUAGE sql AS $$ SELECT 'two'::text $$");
        SQLException e = refused("ALTER FUNCTION zz_qr_qs.who() SET SCHEMA zz_qr_qs2");
        assertEquals("42723", e.getSQLState());
        assertTrue(e.getMessage().contains("already exists in schema"), e.getMessage());
        assertEquals("two", scalar("SELECT zz_qr_qs2.who()"));
        assertEquals("one", scalar("SELECT zz_qr_qs.who()"));
    }
}
