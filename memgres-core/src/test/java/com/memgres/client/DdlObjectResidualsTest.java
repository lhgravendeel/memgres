package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DDL on objects that are not tables: ALTER and DROP on a name that was never created, the
 * clauses a policy may carry, a domain default that is not a value of the domain, and what a
 * table an inheritance child depends on says when it is dropped.
 *
 * <p>The notices are here rather than in the feature-comparison file because that harness reads
 * only the first line of an error and no notices at all; a NOTICE reaches a JDBC client as a
 * {@link SQLWarning} on the statement.
 */
class DdlObjectResidualsTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** The DETAIL field of the error this statement raises, or null. */
    private static String detailOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            if (e instanceof org.postgresql.util.PSQLException) {
                org.postgresql.util.ServerErrorMessage sem =
                        ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
                return sem == null ? null : sem.getDetail();
            }
            return null;
        }
    }

    /** The HINT field of the error this statement raises, or null. */
    private static String hintOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            if (e instanceof org.postgresql.util.PSQLException) {
                org.postgresql.util.ServerErrorMessage sem =
                        ((org.postgresql.util.PSQLException) e).getServerErrorMessage();
                return sem == null ? null : sem.getHint();
            }
            return null;
        }
    }

    /** Every notice the statement produced, in order. */
    private static List<String> noticesOf(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            SQLWarning w = s.getWarnings();
            while (w != null) {
                out.add(w.getMessage().trim());
                w = w.getNextWarning();
            }
        }
        return out;
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ---- ALTER on a name that was never created ----

    @Test
    void alterOnMissingTextSearchObjectIsRefused() {
        assertEquals("42704", stateOf("ALTER TEXT SEARCH CONFIGURATION dor_no RENAME TO dor_o"));
        assertTrue(messageOf("ALTER TEXT SEARCH CONFIGURATION dor_no RENAME TO dor_o")
                .contains("text search configuration \"dor_no\" does not exist"));
        assertEquals("42704", stateOf("ALTER TEXT SEARCH CONFIGURATION dor_no OWNER TO memgres"));
        assertEquals("42704",
                stateOf("ALTER TEXT SEARCH CONFIGURATION dor_no ADD MAPPING FOR word WITH simple"));
        assertEquals("42704", stateOf("ALTER TEXT SEARCH DICTIONARY dor_no RENAME TO dor_o"));
        assertTrue(messageOf("ALTER TEXT SEARCH DICTIONARY dor_no RENAME TO dor_o")
                .contains("text search dictionary \"dor_no\" does not exist"));
        assertEquals("42704", stateOf("ALTER TEXT SEARCH PARSER dor_no RENAME TO dor_o"));
        assertEquals("42704", stateOf("ALTER TEXT SEARCH TEMPLATE dor_no RENAME TO dor_o"));
    }

    @Test
    void alterOnMissingForeignObjectIsRefused() {
        assertEquals("42704", stateOf("ALTER FOREIGN DATA WRAPPER dor_no RENAME TO dor_o"));
        assertTrue(messageOf("ALTER FOREIGN DATA WRAPPER dor_no OPTIONS (a 'b')")
                .contains("foreign-data wrapper \"dor_no\" does not exist"));
        assertEquals("42704", stateOf("ALTER SERVER dor_no RENAME TO dor_o"));
        assertEquals("42704", stateOf("ALTER SERVER dor_no OPTIONS (a 'b')"));
        assertEquals("42704", stateOf("ALTER SERVER dor_no VERSION '2'"));
        assertTrue(messageOf("ALTER USER MAPPING FOR memgres SERVER dor_no OPTIONS (SET a 'b')")
                .contains("server \"dor_no\" does not exist"));
        // a foreign table is a relation, so it is reported as one
        assertEquals("42P01", stateOf("ALTER FOREIGN TABLE dor_no RENAME TO dor_o"));
    }

    @Test
    void alterOnMissingSubscriptionOrExtensionIsRefused() {
        assertEquals("42704", stateOf("ALTER SUBSCRIPTION dor_no RENAME TO dor_o"));
        assertEquals("42704", stateOf("ALTER SUBSCRIPTION dor_no ENABLE"));
        assertTrue(messageOf("ALTER EXTENSION dor_no UPDATE")
                .contains("extension \"dor_no\" does not exist"));
        assertEquals("42704", stateOf("ALTER EXTENSION dor_no SET SCHEMA public"));
        assertEquals("42704", stateOf("ALTER LARGE OBJECT 987654 OWNER TO memgres"));
    }

    @Test
    void alterOnAnObjectThatIsThereStillWorks() throws Exception {
        exec("DROP TEXT SEARCH CONFIGURATION IF EXISTS dor_cfg");
        exec("DROP TEXT SEARCH CONFIGURATION IF EXISTS dor_cfg2");
        exec("CREATE TEXT SEARCH CONFIGURATION dor_cfg (COPY = simple)");
        exec("ALTER TEXT SEARCH CONFIGURATION dor_cfg RENAME TO dor_cfg2");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_ts_config WHERE cfgname='dor_cfg'"));
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_ts_config WHERE cfgname='dor_cfg2'"));
        // a rename onto a name that is taken is refused rather than silently merging
        exec("CREATE TEXT SEARCH CONFIGURATION dor_cfg (COPY = simple)");
        assertEquals("42710",
                stateOf("ALTER TEXT SEARCH CONFIGURATION dor_cfg RENAME TO dor_cfg2"));
        exec("DROP TEXT SEARCH CONFIGURATION dor_cfg");
        exec("DROP TEXT SEARCH CONFIGURATION dor_cfg2");

        exec("DROP SERVER IF EXISTS dor_srv");
        exec("DROP FOREIGN DATA WRAPPER IF EXISTS dor_fdw");
        exec("CREATE FOREIGN DATA WRAPPER dor_fdw");
        exec("CREATE SERVER dor_srv FOREIGN DATA WRAPPER dor_fdw");
        exec("ALTER SERVER dor_srv OPTIONS (ADD host 'h')");
        exec("ALTER FOREIGN DATA WRAPPER dor_fdw OPTIONS (ADD a 'b')");
        assertEquals("1",
                scalar("SELECT count(*)::text FROM pg_foreign_server WHERE srvname='dor_srv'"));
        exec("DROP SERVER dor_srv");
        exec("DROP FOREIGN DATA WRAPPER dor_fdw");

        // plpgsql is installed without ever being created
        assertEquals("OK", stateOf("ALTER EXTENSION plpgsql UPDATE"));
    }

    // ---- DROP on a name that was never created ----

    @Test
    void dropOnMissingObjectIsRefusedAndIfExistsSaysWhatItSkipped() throws Exception {
        assertEquals("42704", stateOf("DROP TEXT SEARCH CONFIGURATION dor_no"));
        assertEquals("42704", stateOf("DROP TEXT SEARCH DICTIONARY dor_no"));
        assertEquals("42704", stateOf("DROP SERVER dor_no"));
        assertEquals("42704", stateOf("DROP FOREIGN DATA WRAPPER dor_no"));
        assertEquals("42704", stateOf("DROP PUBLICATION dor_no"));
        assertEquals("42704", stateOf("DROP EXTENSION dor_no"));

        assertEquals(one("text search configuration \"dor_no\" does not exist, skipping"),
                noticesOf("DROP TEXT SEARCH CONFIGURATION IF EXISTS dor_no"));
        assertEquals(one("text search dictionary \"dor_no\" does not exist, skipping"),
                noticesOf("DROP TEXT SEARCH DICTIONARY IF EXISTS dor_no"));
        assertEquals(one("text search parser \"dor_no\" does not exist, skipping"),
                noticesOf("DROP TEXT SEARCH PARSER IF EXISTS dor_no"));
        assertEquals(one("text search template \"dor_no\" does not exist, skipping"),
                noticesOf("DROP TEXT SEARCH TEMPLATE IF EXISTS dor_no"));
        assertEquals(one("server \"dor_no\" does not exist, skipping"),
                noticesOf("DROP SERVER IF EXISTS dor_no"));
        assertEquals(one("foreign-data wrapper \"dor_no\" does not exist, skipping"),
                noticesOf("DROP FOREIGN DATA WRAPPER IF EXISTS dor_no"));
        assertEquals(one("publication \"dor_no\" does not exist, skipping"),
                noticesOf("DROP PUBLICATION IF EXISTS dor_no"));
        assertEquals(one("extension \"dor_no\" does not exist, skipping"),
                noticesOf("DROP EXTENSION IF EXISTS dor_no"));
    }

    @Test
    void dropIfExistsNamesTheRelationKindItSkipped() throws Exception {
        assertEquals(one("view \"dor_no\" does not exist, skipping"),
                noticesOf("DROP VIEW IF EXISTS dor_no"));
        assertEquals(one("materialized view \"dor_no\" does not exist, skipping"),
                noticesOf("DROP MATERIALIZED VIEW IF EXISTS dor_no"));
        assertEquals(one("sequence \"dor_no\" does not exist, skipping"),
                noticesOf("DROP SEQUENCE IF EXISTS dor_no"));
        assertEquals(one("index \"dor_no\" does not exist, skipping"),
                noticesOf("DROP INDEX IF EXISTS dor_no"));
        assertEquals(one("type \"dor_no\" does not exist, skipping"),
                noticesOf("DROP TYPE IF EXISTS dor_no"));
        // a domain is a type, and PostgreSQL says so
        assertEquals(one("type \"dor_no\" does not exist, skipping"),
                noticesOf("DROP DOMAIN IF EXISTS dor_no"));
        assertEquals(one("table \"dor_no\" does not exist, skipping"),
                noticesOf("DROP TABLE IF EXISTS dor_no"));
        assertEquals(one("schema \"dor_no\" does not exist, skipping"),
                noticesOf("DROP SCHEMA IF EXISTS dor_no"));
    }

    @Test
    void dropRoutineIfExistsNamesTheArgumentsAsPostgresPrintsThem() throws Exception {
        assertEquals(one("function dor_no() does not exist, skipping"),
                noticesOf("DROP FUNCTION IF EXISTS dor_no()"));
        assertEquals(one("procedure dor_no() does not exist, skipping"),
                noticesOf("DROP PROCEDURE IF EXISTS dor_no()"));
        // the grammar's aliases print as the catalog type they stand for; a plain name does not
        assertEquals(one("function dor_no(pg_catalog.int4) does not exist, skipping"),
                noticesOf("DROP FUNCTION IF EXISTS dor_no(int)"));
        assertEquals(one("function dor_no(pg_catalog.int8) does not exist, skipping"),
                noticesOf("DROP FUNCTION IF EXISTS dor_no(bigint)"));
        assertEquals(one("function dor_no(text) does not exist, skipping"),
                noticesOf("DROP FUNCTION IF EXISTS dor_no(text)"));
        assertEquals(one("function dor_no(pg_catalog.int4,text) does not exist, skipping"),
                noticesOf("DROP FUNCTION IF EXISTS dor_no(int, text)"));
    }

    @Test
    void dropTriggerAndPolicyIfExistsNameTheRelationWhenThatIsWhatIsMissing() throws Exception {
        assertEquals(one("relation \"dor_norel\" does not exist, skipping"),
                noticesOf("DROP TRIGGER IF EXISTS dor_no ON dor_norel"));
        assertEquals(one("relation \"dor_norel\" does not exist, skipping"),
                noticesOf("DROP POLICY IF EXISTS dor_no ON dor_norel"));
        assertEquals("42P01", stateOf("DROP TRIGGER dor_no ON dor_norel"));

        exec("DROP TABLE IF EXISTS dor_tg CASCADE");
        exec("CREATE TABLE dor_tg (i int PRIMARY KEY)");
        assertEquals(one("trigger \"dor_no\" for relation \"dor_tg\" does not exist, skipping"),
                noticesOf("DROP TRIGGER IF EXISTS dor_no ON dor_tg"));
        assertEquals("42704", stateOf("DROP TRIGGER dor_no ON dor_tg"));
        exec("DROP TABLE dor_tg");
    }

    @Test
    void dropCascadeNamesWhatItTookWithIt() throws Exception {
        exec("DROP TABLE IF EXISTS dor_ut CASCADE");
        exec("DROP TYPE IF EXISTS dor_ty CASCADE");
        exec("CREATE TYPE dor_ty AS ENUM ('a','b')");
        exec("CREATE TABLE dor_ut (id int PRIMARY KEY, c dor_ty)");
        assertEquals(one("drop cascades to column c of table dor_ut"),
                noticesOf("DROP TYPE dor_ty CASCADE"));
        exec("DROP TABLE IF EXISTS dor_ut CASCADE");

        exec("DROP VIEW IF EXISTS dor_vv");
        exec("DROP TABLE IF EXISTS dor_vt CASCADE");
        exec("CREATE TABLE dor_vt (i int PRIMARY KEY)");
        exec("CREATE VIEW dor_vv AS SELECT i FROM dor_vt");
        assertEquals(one("drop cascades to view dor_vv"), noticesOf("DROP TABLE dor_vt CASCADE"));

        exec("DROP SEQUENCE IF EXISTS dor_sq CASCADE");
        exec("CREATE SEQUENCE dor_sq");
        exec("CREATE TABLE dor_ut (id int PRIMARY KEY, b int DEFAULT nextval('dor_sq'))");
        assertEquals(one("drop cascades to default value for column b of table dor_ut"),
                noticesOf("DROP SEQUENCE dor_sq CASCADE"));
        exec("DROP TABLE dor_ut");
    }

    // ---- ALTER SCHEMA ----

    @Test
    void alterSchemaOnAMissingSchemaIsRefused() throws Exception {
        assertEquals("3F000", stateOf("ALTER SCHEMA dor_no OWNER TO memgres"));
        assertTrue(messageOf("ALTER SCHEMA dor_no OWNER TO memgres")
                .contains("schema \"dor_no\" does not exist"));
        assertEquals("3F000", stateOf("ALTER SCHEMA dor_no RENAME TO dor_o"));

        exec("DROP SCHEMA IF EXISTS dor_sch CASCADE");
        exec("DROP SCHEMA IF EXISTS dor_sch2 CASCADE");
        exec("CREATE SCHEMA dor_sch");
        exec("ALTER SCHEMA dor_sch OWNER TO memgres");
        exec("ALTER SCHEMA dor_sch RENAME TO dor_sch2");
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.schemata"
                + " WHERE schema_name = 'dor_sch2'"));
        exec("DROP SCHEMA dor_sch2");
    }

    // ---- Domain defaults ----

    @Test
    void aDomainDefaultMustBeAValueOfTheDomain() throws Exception {
        exec("DROP DOMAIN IF EXISTS dor_dom CASCADE");
        exec("CREATE DOMAIN dor_dom AS int");
        assertEquals("22P02", stateOf("ALTER DOMAIN dor_dom SET DEFAULT 'abc'"));
        assertTrue(messageOf("ALTER DOMAIN dor_dom SET DEFAULT 'abc'")
                .contains("invalid input syntax for type integer: \"abc\""));
        // and on CREATE, not only on ALTER
        exec("DROP DOMAIN IF EXISTS dor_dbad CASCADE");
        assertEquals("22P02", stateOf("CREATE DOMAIN dor_dbad AS int DEFAULT 'abc'"));

        exec("DROP DOMAIN IF EXISTS dor_ddate CASCADE");
        exec("CREATE DOMAIN dor_ddate AS date");
        assertEquals("22007", stateOf("ALTER DOMAIN dor_ddate SET DEFAULT 'nope'"));
        exec("DROP DOMAIN IF EXISTS dor_dbool CASCADE");
        exec("CREATE DOMAIN dor_dbool AS boolean");
        assertEquals("22P02", stateOf("ALTER DOMAIN dor_dbool SET DEFAULT 'maybe'"));
        exec("DROP DOMAIN IF EXISTS dor_duuid CASCADE");
        exec("CREATE DOMAIN dor_duuid AS uuid");
        assertEquals("22P02", stateOf("ALTER DOMAIN dor_duuid SET DEFAULT 'zzz'"));

        // what the type does take, it takes -- including the shapes around the refusal
        exec("ALTER DOMAIN dor_dom SET DEFAULT 7");
        exec("ALTER DOMAIN dor_dom SET DEFAULT '7'");
        exec("ALTER DOMAIN dor_dom SET DEFAULT ' 9 '");
        exec("ALTER DOMAIN dor_dom SET DEFAULT 3 + 4");
        exec("ALTER DOMAIN dor_dom SET DEFAULT 99999999999999999999");
        exec("ALTER DOMAIN dor_ddate SET DEFAULT CURRENT_DATE");
        exec("ALTER DOMAIN dor_dbool SET DEFAULT true");
        exec("ALTER DOMAIN dor_duuid SET DEFAULT '00000000-0000-0000-0000-000000000000'");

        exec("DROP DOMAIN dor_ddate");
        exec("DROP DOMAIN dor_dbool");
        exec("DROP DOMAIN dor_duuid");
        exec("DROP DOMAIN dor_dom");
    }

    @Test
    void aDomainDefaultReadsBackAsTheValueThatWasRead() throws Exception {
        exec("DROP DOMAIN IF EXISTS dor_dom CASCADE");
        exec("DROP DOMAIN IF EXISTS dor_dtxt CASCADE");
        exec("CREATE DOMAIN dor_dom AS int");
        exec("CREATE DOMAIN dor_dtxt AS text");

        exec("ALTER DOMAIN dor_dom SET DEFAULT '7'");
        assertEquals("7", domainDefault("dor_dom"));
        exec("ALTER DOMAIN dor_dom SET DEFAULT ' 9 '");
        assertEquals("9", domainDefault("dor_dom"));
        // SET DEFAULT NULL leaves no default at all
        exec("ALTER DOMAIN dor_dom SET DEFAULT NULL");
        assertNull(domainDefault("dor_dom"));
        // a string type keeps the quoted literal it was written as
        exec("ALTER DOMAIN dor_dtxt SET DEFAULT 'abc'");
        assertEquals("'abc'::text", domainDefault("dor_dtxt"));

        // and the default is still what a row without a value gets
        exec("DROP TABLE IF EXISTS dor_dd CASCADE");
        exec("ALTER DOMAIN dor_dom SET DEFAULT 42");
        exec("CREATE TABLE dor_dd (id int PRIMARY KEY, a dor_dom)");
        exec("INSERT INTO dor_dd (id) VALUES (1)");
        assertEquals("42", scalar("SELECT a::text FROM dor_dd WHERE id = 1"));
        exec("DROP TABLE dor_dd");
        exec("DROP DOMAIN dor_dom");
        exec("DROP DOMAIN dor_dtxt");
    }

    private static String domainDefault(String name) throws SQLException {
        return scalar("SELECT domain_default FROM information_schema.domains"
                + " WHERE domain_name = '" + name + "'");
    }

    // ---- ALTER TYPE attributes ----

    @Test
    void anAttributeBelongsToTheRelationACompositeTypeOwns() throws Exception {
        assertEquals("42P01", stateOf("ALTER TYPE dor_notype ADD ATTRIBUTE q int"));
        assertTrue(messageOf("ALTER TYPE dor_notype ADD ATTRIBUTE q int")
                .contains("relation \"dor_notype\" does not exist"));
        assertEquals("42P01", stateOf("ALTER TYPE dor_notype DROP ATTRIBUTE q"));
        assertEquals("42P01", stateOf("ALTER TYPE dor_notype ALTER ATTRIBUTE q TYPE text"));
        assertEquals("42P01", stateOf("ALTER TYPE dor_notype RENAME ATTRIBUTE q TO r"));
        // RENAME TO and SET SCHEMA apply to any type, so they still report a type
        assertEquals("42704", stateOf("ALTER TYPE dor_notype RENAME TO dor_o"));
        assertEquals("42704", stateOf("ALTER DOMAIN dor_nodomain SET NOT NULL"));

        exec("DROP TYPE IF EXISTS dor_en CASCADE");
        exec("CREATE TYPE dor_en AS ENUM ('a')");
        assertEquals("42P01", stateOf("ALTER TYPE dor_en ADD ATTRIBUTE q int"));
        exec("DROP TYPE dor_en");

        exec("DROP TABLE IF EXISTS dor_tt CASCADE");
        exec("CREATE TABLE dor_tt (i int PRIMARY KEY)");
        assertEquals("42809", stateOf("ALTER TYPE dor_tt ADD ATTRIBUTE q int"));
        exec("DROP TABLE dor_tt");

        exec("DROP TYPE IF EXISTS dor_ct CASCADE");
        exec("CREATE TYPE dor_ct AS (a int)");
        exec("ALTER TYPE dor_ct ADD ATTRIBUTE q int");
        assertEquals("a,q", scalar("SELECT string_agg(attname, ',' ORDER BY attnum)"
                + " FROM pg_attribute WHERE attrelid ="
                + " (SELECT typrelid FROM pg_type WHERE typname='dor_ct') AND attnum > 0"));
        exec("DROP TYPE dor_ct");
    }

    // ---- Policy clauses ----

    @Test
    void aPolicyCarriesOnlyTheClausesItsCommandCanUse() throws Exception {
        exec("DROP TABLE IF EXISTS dor_pol CASCADE");
        exec("CREATE TABLE dor_pol (id int PRIMARY KEY)");
        assertEquals("42601", stateOf("CREATE POLICY dor_x ON dor_pol FOR SELECT WITH CHECK (id > 0)"));
        assertEquals("42601", stateOf("CREATE POLICY dor_x ON dor_pol FOR DELETE WITH CHECK (id > 0)"));
        assertEquals("42601", stateOf("CREATE POLICY dor_x ON dor_pol FOR INSERT USING (id > 0)"));

        exec("CREATE POLICY dor_ps ON dor_pol FOR SELECT USING (id > 0)");
        exec("CREATE POLICY dor_pd ON dor_pol FOR DELETE USING (id > 0)");
        exec("CREATE POLICY dor_pi ON dor_pol FOR INSERT WITH CHECK (id > 0)");
        exec("CREATE POLICY dor_pu ON dor_pol FOR UPDATE USING (id > 0) WITH CHECK (id > 1)");
        exec("CREATE POLICY dor_pa ON dor_pol FOR ALL USING (id > 0) WITH CHECK (id > 1)");

        assertEquals("42601", stateOf("ALTER POLICY dor_ps ON dor_pol WITH CHECK (id > 1)"));
        assertTrue(messageOf("ALTER POLICY dor_ps ON dor_pol WITH CHECK (id > 1)")
                .contains("only USING expression allowed for SELECT, DELETE"));
        assertEquals("42601",
                stateOf("ALTER POLICY dor_ps ON dor_pol USING (id > 2) WITH CHECK (id > 2)"));
        assertEquals("42601", stateOf("ALTER POLICY dor_pd ON dor_pol WITH CHECK (id > 1)"));
        assertEquals("42601", stateOf("ALTER POLICY dor_pi ON dor_pol USING (id > 1)"));
        assertTrue(messageOf("ALTER POLICY dor_pi ON dor_pol USING (id > 1)")
                .contains("only WITH CHECK expression allowed for INSERT"));

        // the shapes each command does take are still taken
        exec("ALTER POLICY dor_ps ON dor_pol USING (id > 5)");
        exec("ALTER POLICY dor_pd ON dor_pol USING (id > 5)");
        exec("ALTER POLICY dor_pi ON dor_pol WITH CHECK (id > 5)");
        exec("ALTER POLICY dor_pu ON dor_pol USING (id > 5) WITH CHECK (id > 6)");
        exec("ALTER POLICY dor_pa ON dor_pol USING (id > 5) WITH CHECK (id > 6)");
        exec("ALTER POLICY dor_pu ON dor_pol TO memgres");
        exec("ALTER POLICY dor_ps ON dor_pol RENAME TO dor_ps2");
        assertEquals("5", scalar("SELECT count(*)::text FROM pg_policies WHERE tablename='dor_pol'"));
        exec("DROP TABLE dor_pol");
    }

    // ---- DROP TABLE with dependents ----

    @Test
    void aParentAnInheritanceChildDependsOnCannotBeDropped() throws Exception {
        exec("DROP TABLE IF EXISTS dor_ic CASCADE");
        exec("DROP TABLE IF EXISTS dor_ip CASCADE");
        exec("CREATE TABLE dor_ip (i int PRIMARY KEY)");
        exec("CREATE TABLE dor_ic () INHERITS (dor_ip)");

        assertEquals("2BP01", stateOf("DROP TABLE dor_ip"));
        assertTrue(messageOf("DROP TABLE dor_ip")
                .contains("cannot drop table dor_ip because other objects depend on it"));
        assertEquals("table dor_ic depends on table dor_ip", detailOf("DROP TABLE dor_ip"));
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.",
                hintOf("DROP TABLE dor_ip"));
        // and nothing was dropped
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.tables"
                + " WHERE table_name = 'dor_ip'"));

        assertEquals(one("drop cascades to table dor_ic"), noticesOf("DROP TABLE dor_ip CASCADE"));
        assertEquals("0", scalar("SELECT count(*)::text FROM information_schema.tables"
                + " WHERE table_name IN ('dor_ip','dor_ic')"));
    }

    @Test
    void aViewOverATableSaysWhichViewDependsOnIt() throws Exception {
        exec("DROP VIEW IF EXISTS dor_vv2");
        exec("DROP TABLE IF EXISTS dor_vt2 CASCADE");
        exec("CREATE TABLE dor_vt2 (i int PRIMARY KEY)");
        exec("CREATE VIEW dor_vv2 AS SELECT i FROM dor_vt2");

        assertEquals("2BP01", stateOf("DROP TABLE dor_vt2"));
        assertEquals("view dor_vv2 depends on table dor_vt2", detailOf("DROP TABLE dor_vt2"));
        assertEquals("Use DROP ... CASCADE to drop the dependent objects too.",
                hintOf("DROP TABLE dor_vt2"));
        exec("DROP TABLE dor_vt2 CASCADE");
        assertEquals("0", scalar("SELECT count(*)::text FROM information_schema.views"
                + " WHERE table_name = 'dor_vv2'"));
    }

    @Test
    void aTableWithNothingDependingOnItIsStillDropped() throws Exception {
        exec("DROP TABLE IF EXISTS dor_plain CASCADE");
        exec("CREATE TABLE dor_plain (i int PRIMARY KEY)");
        exec("DROP TABLE dor_plain");
        assertEquals("0", scalar("SELECT count(*)::text FROM information_schema.tables"
                + " WHERE table_name = 'dor_plain'"));

        // a child that no longer inherits is no longer a dependent
        exec("DROP TABLE IF EXISTS dor_ic3 CASCADE");
        exec("DROP TABLE IF EXISTS dor_ip3 CASCADE");
        exec("CREATE TABLE dor_ip3 (i int PRIMARY KEY)");
        exec("CREATE TABLE dor_ic3 () INHERITS (dor_ip3)");
        exec("ALTER TABLE dor_ic3 NO INHERIT dor_ip3");
        exec("DROP TABLE dor_ip3");
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.tables"
                + " WHERE table_name = 'dor_ic3'"));
        exec("DROP TABLE dor_ic3");
    }

    private static List<String> one(String text) {
        List<String> out = new ArrayList<>();
        out.add(text);
        return out;
    }
}
