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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Three rules, all the same shape and all measured against PostgreSQL 18.
 *
 * <p><b>A type belongs to one schema, and each schema has its own.</b> Enums, composites, ranges,
 * domains and shells share one namespace per schema, so {@code CREATE TYPE a.e} and
 * {@code CREATE TYPE b.e} both succeed and pg_type carries two rows called {@code e}. A column
 * declared {@code a.e} keeps reading a.e's labels however many other schemas later hold an
 * {@code e} — which is the whole point, and the thing that would silently give the wrong answer if
 * the two were one entry. A name written bare is the search path's to answer, so the same statement
 * text reads a's type under one search path and b's under another.
 *
 * <p><b>A comment belongs to the object it was made on.</b> a.t and b.t are two tables and
 * {@code obj_description} answers for each of them separately, whatever the other one says. A
 * rename or a SET SCHEMA carries the comment with the object, because PostgreSQL keys a comment by
 * an OID and a rename does not change one.
 *
 * <p><b>pg_description covers every kind that can carry a comment.</b> Table, column, view,
 * materialized view, index, sequence, constraint, function, type, domain and schema all reach it,
 * each under its own OID, and obj_description and col_description read that same table — so what
 * {@code \d+} shows and what those functions answer cannot disagree.
 */
class TypeAndCommentNamespaceTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE SCHEMA tcn_a");
        exec("CREATE SCHEMA tcn_b");
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

    private static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The first line of the message a statement raises, or "OK". */
    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
    }

    // ------------------------------------------------------------------
    // A type belongs to one schema
    // ------------------------------------------------------------------

    @Test
    void twoSchemasEachHoldAnEnumOfTheSameName() throws Exception {
        exec("CREATE TYPE tcn_a.e AS ENUM ('x')");
        exec("CREATE TYPE tcn_b.e AS ENUM ('p')");
        assertEquals(java.util.Arrays.asList("tcn_a|e", "tcn_b|e"),
                rows("SELECT n.nspname, t.typname FROM pg_type t"
                        + " JOIN pg_namespace n ON n.oid = t.typnamespace"
                        + " WHERE t.typname = 'e' ORDER BY 1"));
        assertEquals("{x}", scalar("SELECT enum_range(NULL::tcn_a.e)::text"));
        assertEquals("{p}", scalar("SELECT enum_range(NULL::tcn_b.e)::text"));
    }

    @Test
    void twoSchemasEachHoldADomainCompositeAndRangeOfTheSameName() throws Exception {
        exec("CREATE DOMAIN tcn_a.d AS int CHECK (VALUE > 0)");
        exec("CREATE DOMAIN tcn_b.d AS text");
        exec("CREATE TYPE tcn_a.ct AS (x int)");
        exec("CREATE TYPE tcn_b.ct AS (y text)");
        exec("CREATE TYPE tcn_a.rg AS RANGE (SUBTYPE = int4)");
        exec("CREATE TYPE tcn_b.rg AS RANGE (SUBTYPE = date)");
        for (String name : new String[]{"d", "ct", "rg"}) {
            assertEquals(java.util.Arrays.asList("tcn_a", "tcn_b"),
                    rows("SELECT n.nspname FROM pg_type t"
                            + " JOIN pg_namespace n ON n.oid = t.typnamespace"
                            + " WHERE t.typname = '" + name + "' ORDER BY 1"),
                    name + " is held by both schemas");
        }
        assertEquals("integer", scalar("SELECT data_type FROM information_schema.domains"
                + " WHERE domain_schema = 'tcn_a' AND domain_name = 'd'"));
        assertEquals("text", scalar("SELECT data_type FROM information_schema.domains"
                + " WHERE domain_schema = 'tcn_b' AND domain_name = 'd'"));
    }

    @Test
    void aColumnKeepsReadingItsOwnType() throws Exception {
        exec("CREATE TYPE tcn_a.m AS ENUM ('x')");
        exec("CREATE TABLE tcn_a.t (c tcn_a.m)");
        exec("INSERT INTO tcn_a.t VALUES ('x')");
        // The second m arrives after the column was declared with the first, and does not become
        // what the column reads. That is the failure this whole change exists to avoid.
        exec("CREATE TYPE tcn_b.m AS ENUM ('p')");
        exec("CREATE TABLE tcn_b.t (c tcn_b.m)");
        exec("INSERT INTO tcn_b.t VALUES ('p')");
        assertEquals("x", scalar("SELECT c::text FROM tcn_a.t"));
        assertEquals("p", scalar("SELECT c::text FROM tcn_b.t"));
        assertEquals("22P02", stateOf("INSERT INTO tcn_a.t VALUES ('p')"),
                "a.m has no label p however many labels b.m has");
        assertEquals("invalid input value for enum tcn_a.m: \"p\"",
                messageOf("INSERT INTO tcn_a.t VALUES ('p')"),
                "the type is named as PostgreSQL names it: qualified, being off the search path");
        assertEquals("tcn_a|m", scalar("SELECT udt_schema || '|' || udt_name"
                + " FROM information_schema.columns"
                + " WHERE table_schema = 'tcn_a' AND table_name = 't' AND column_name = 'c'"));
        assertEquals("tcn_b|m", scalar("SELECT udt_schema || '|' || udt_name"
                + " FROM information_schema.columns"
                + " WHERE table_schema = 'tcn_b' AND table_name = 't' AND column_name = 'c'"));
    }

    @Test
    void aBareTypeNameIsResolvedAlongTheSearchPath() throws Exception {
        exec("CREATE TYPE tcn_a.sp AS ENUM ('inA')");
        exec("CREATE TYPE tcn_b.sp AS ENUM ('inB')");
        try {
            exec("SET search_path TO tcn_a");
            assertEquals("{inA}", scalar("SELECT enum_range(NULL::sp)::text"));
            assertEquals("inA", scalar("SELECT 'inA'::sp::text"));
            exec("SET search_path TO tcn_b");
            assertEquals("{inB}", scalar("SELECT enum_range(NULL::sp)::text"));
            assertEquals("inB", scalar("SELECT 'inB'::sp::text"));
            assertEquals("22P02", stateOf("SELECT 'inA'::sp"),
                    "under this search path sp is b's, and b's has no inA");
        } finally {
            exec("SET search_path TO public");
        }
    }

    @Test
    void oneNamespacePerSchemaHoldsEveryKindOfType() throws Exception {
        exec("CREATE TYPE tcn_a.one AS ENUM ('q')");
        assertEquals("42710", stateOf("CREATE TYPE tcn_a.one AS ENUM ('r')"));
        assertEquals("42710", stateOf("CREATE DOMAIN tcn_a.one AS int"));
        assertEquals("42710", stateOf("CREATE TYPE tcn_a.one AS (z int)"));
        assertEquals("type \"one\" already exists", messageOf("CREATE DOMAIN tcn_a.one AS int"));
        assertEquals("OK", stateOf("CREATE DOMAIN tcn_b.one AS int"),
                "the other schema's namespace is its own");
    }

    @Test
    void aQualifiedTypeNameNamesOnlyThatSchemasType() throws Exception {
        exec("CREATE TYPE tcn_a.only AS ENUM ('q')");
        assertEquals("42704", stateOf("SELECT 'q'::tcn_b.only"));
        assertEquals("type \"tcn_b.only\" does not exist", messageOf("SELECT 'q'::tcn_b.only"));
        assertEquals("42704", stateOf("DROP TYPE tcn_b.only"));
        assertEquals("3F000", stateOf("CREATE TYPE tcn_nosuch.q AS ENUM ('x')"));
    }

    @Test
    void alterAndDropReachTheTypeTheyName() throws Exception {
        exec("CREATE TYPE tcn_a.ad AS ENUM ('x')");
        exec("CREATE TYPE tcn_b.ad AS ENUM ('p')");
        exec("ALTER TYPE tcn_a.ad ADD VALUE 'y'");
        assertEquals("{x,y}", scalar("SELECT enum_range(NULL::tcn_a.ad)::text"));
        assertEquals("{p}", scalar("SELECT enum_range(NULL::tcn_b.ad)::text"),
                "the other schema's type is untouched");
        exec("DROP TYPE tcn_b.ad");
        assertEquals("{x,y}", scalar("SELECT enum_range(NULL::tcn_a.ad)::text"),
                "dropping b's ad leaves a's");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_type WHERE typname = 'ad'"));
    }

    @Test
    void aRenamedTypeKeepsTheColumnsDeclaredWithIt() throws Exception {
        exec("CREATE TYPE tcn_a.rn AS ENUM ('x')");
        exec("CREATE TABLE tcn_a.rt (c tcn_a.rn)");
        exec("INSERT INTO tcn_a.rt VALUES ('x')");
        exec("ALTER TYPE tcn_a.rn RENAME TO rn2");
        assertEquals("x", scalar("SELECT c::text FROM tcn_a.rt"));
        assertEquals("rn2", scalar("SELECT udt_name FROM information_schema.columns"
                + " WHERE table_schema = 'tcn_a' AND table_name = 'rt' AND column_name = 'c'"));
        assertEquals("22P02", stateOf("INSERT INTO tcn_a.rt VALUES ('nope')"),
                "the column still reads the type, which still refuses a label it has not got");
    }

    @Test
    void aRenamedDomainKeepsTheColumnsDeclaredWithIt() throws Exception {
        exec("CREATE DOMAIN tcn_a.dn AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE tcn_a.dt (c tcn_a.dn)");
        exec("INSERT INTO tcn_a.dt VALUES (5)");
        exec("ALTER DOMAIN tcn_a.dn RENAME TO dn2");
        assertEquals("dn2", scalar("SELECT domain_name FROM information_schema.columns"
                + " WHERE table_schema = 'tcn_a' AND table_name = 'dt' AND column_name = 'c'"));
        assertEquals("23514", stateOf("INSERT INTO tcn_a.dt VALUES (-1)"),
                "the CHECK the domain carries is still enforced through the new name");
    }

    // ------------------------------------------------------------------
    // A comment belongs to the object it was made on
    // ------------------------------------------------------------------

    @Test
    void twoTablesOfTheSameNameCarryTheirOwnComments() throws Exception {
        exec("CREATE TABLE tcn_a.c1 (x int)");
        exec("CREATE TABLE tcn_b.c1 (x int)");
        exec("COMMENT ON TABLE tcn_a.c1 IS 'acom'");
        exec("COMMENT ON TABLE tcn_b.c1 IS 'bcom'");
        assertEquals("acom", scalar("SELECT obj_description('tcn_a.c1'::regclass)"));
        assertEquals("bcom", scalar("SELECT obj_description('tcn_b.c1'::regclass)"));
        exec("COMMENT ON COLUMN tcn_a.c1.x IS 'acol'");
        exec("COMMENT ON COLUMN tcn_b.c1.x IS 'bcol'");
        assertEquals("acol", scalar("SELECT col_description('tcn_a.c1'::regclass, 1)"));
        assertEquals("bcol", scalar("SELECT col_description('tcn_b.c1'::regclass, 1)"));
    }

    @Test
    void aRenameCarriesTheCommentOfTheObjectBeingRenamed() throws Exception {
        exec("CREATE TABLE tcn_a.c2 (x int)");
        exec("CREATE TABLE tcn_b.c2 (x int)");
        exec("COMMENT ON TABLE tcn_a.c2 IS 'acom2'");
        exec("COMMENT ON TABLE tcn_b.c2 IS 'bcom2'");
        exec("COMMENT ON COLUMN tcn_a.c2.x IS 'acol2'");
        exec("ALTER TABLE tcn_a.c2 RENAME TO c2r");
        assertEquals("acom2", scalar("SELECT obj_description('tcn_a.c2r'::regclass)"));
        assertEquals("acol2", scalar("SELECT col_description('tcn_a.c2r'::regclass, 1)"));
        assertEquals("bcom2", scalar("SELECT obj_description('tcn_b.c2'::regclass)"),
                "the other table's comment is not the one that moved");
    }

    @Test
    void settingASchemaCarriesTheCommentToo() throws Exception {
        exec("CREATE TABLE tcn_a.c3 (x int)");
        exec("COMMENT ON TABLE tcn_a.c3 IS 'moving'");
        exec("COMMENT ON COLUMN tcn_a.c3.x IS 'movingcol'");
        exec("ALTER TABLE tcn_a.c3 SET SCHEMA tcn_b");
        assertEquals("moving", scalar("SELECT obj_description('tcn_b.c3'::regclass)"));
        assertEquals("movingcol", scalar("SELECT col_description('tcn_b.c3'::regclass, 1)"));
    }

    @Test
    void aRenamedTypeKeepsItsComment() throws Exception {
        exec("CREATE TYPE tcn_a.ce AS ENUM ('q')");
        exec("COMMENT ON TYPE tcn_a.ce IS 'typecomment'");
        exec("ALTER TYPE tcn_a.ce RENAME TO ce2");
        assertEquals("typecomment", scalar("SELECT obj_description('tcn_a.ce2'::regtype)"));
    }

    @Test
    void droppingATypeTakesItsCommentWithIt() throws Exception {
        exec("CREATE TYPE tcn_a.cd AS ENUM ('q')");
        exec("COMMENT ON TYPE tcn_a.cd IS 'goes away'");
        exec("DROP TYPE tcn_a.cd");
        assertEquals("0", scalar(
                "SELECT count(*)::text FROM pg_description WHERE description = 'goes away'"));
    }

    // ------------------------------------------------------------------
    // pg_description covers every kind that can carry a comment
    // ------------------------------------------------------------------

    @Test
    void everyKindOfCommentReachesPgDescription() throws Exception {
        exec("CREATE TABLE tcn_a.d1 (id int PRIMARY KEY, v int)");
        exec("CREATE INDEX tcn_di ON tcn_a.d1 (v)");
        exec("CREATE VIEW tcn_a.dv AS SELECT id FROM tcn_a.d1");
        exec("CREATE MATERIALIZED VIEW tcn_a.dm AS SELECT id FROM tcn_a.d1");
        exec("CREATE SEQUENCE tcn_a.ds");
        exec("ALTER TABLE tcn_a.d1 ADD CONSTRAINT tcn_chk CHECK (v > 0)");
        exec("CREATE FUNCTION tcn_fn(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql");
        exec("CREATE TYPE tcn_a.dt2 AS ENUM ('q')");
        exec("CREATE DOMAIN tcn_a.dd AS int");

        exec("COMMENT ON TABLE tcn_a.d1 IS 'k-table'");
        exec("COMMENT ON COLUMN tcn_a.d1.v IS 'k-column'");
        exec("COMMENT ON INDEX tcn_a.tcn_di IS 'k-index'");
        exec("COMMENT ON VIEW tcn_a.dv IS 'k-view'");
        exec("COMMENT ON MATERIALIZED VIEW tcn_a.dm IS 'k-matview'");
        exec("COMMENT ON SEQUENCE tcn_a.ds IS 'k-sequence'");
        exec("COMMENT ON CONSTRAINT tcn_chk ON tcn_a.d1 IS 'k-constraint'");
        exec("COMMENT ON FUNCTION tcn_fn(int) IS 'k-function'");
        exec("COMMENT ON TYPE tcn_a.dt2 IS 'k-type'");
        exec("COMMENT ON DOMAIN tcn_a.dd IS 'k-domain'");
        exec("COMMENT ON SCHEMA tcn_a IS 'k-schema'");

        assertEquals("11", scalar(
                "SELECT count(*)::text FROM pg_description WHERE description LIKE 'k-%'"),
                "every kind reaches pg_description");

        assertEquals("k-table", scalar("SELECT obj_description('tcn_a.d1'::regclass)"));
        assertEquals("k-column", scalar("SELECT col_description('tcn_a.d1'::regclass, 2)"));
        assertEquals("k-index", scalar("SELECT obj_description('tcn_a.tcn_di'::regclass)"));
        assertEquals("k-view", scalar("SELECT obj_description('tcn_a.dv'::regclass)"));
        assertEquals("k-matview", scalar("SELECT obj_description('tcn_a.dm'::regclass)"));
        assertEquals("k-sequence", scalar("SELECT obj_description('tcn_a.ds'::regclass)"));
        assertEquals("k-type", scalar("SELECT obj_description('tcn_a.dt2'::regtype)"));
        assertEquals("k-domain", scalar("SELECT obj_description('tcn_a.dd'::regtype)"));
        assertEquals("k-constraint", scalar("SELECT obj_description(oid, 'pg_constraint')"
                + " FROM pg_constraint WHERE conname = 'tcn_chk'"));
        assertEquals("k-function", scalar("SELECT obj_description(oid, 'pg_proc')"
                + " FROM pg_proc WHERE proname = 'tcn_fn'"));
        assertEquals("k-schema", scalar("SELECT obj_description(oid, 'pg_namespace')"
                + " FROM pg_namespace WHERE nspname = 'tcn_a'"));
    }

    @Test
    void aDescriptionRowNamesTheCatalogTheObjectLivesIn() throws Exception {
        exec("CREATE TABLE tcn_a.d2 (x int)");
        exec("CREATE TYPE tcn_a.d2t AS ENUM ('q')");
        exec("COMMENT ON TABLE tcn_a.d2 IS 'in pg_class'");
        exec("COMMENT ON TYPE tcn_a.d2t IS 'in pg_type'");
        assertEquals("pg_class", scalar("SELECT c.relname FROM pg_description d"
                + " JOIN pg_class c ON c.oid = d.classoid WHERE d.description = 'in pg_class'"));
        assertEquals("pg_type", scalar("SELECT c.relname FROM pg_description d"
                + " JOIN pg_class c ON c.oid = d.classoid WHERE d.description = 'in pg_type'"));
        // A comment is on the object, not on one of its columns.
        assertEquals("0", scalar("SELECT objsubid::text FROM pg_description"
                + " WHERE description = 'in pg_class'"));
    }

    @Test
    void obj_descriptionAnswersOnlyForTheCatalogItIsAsked() throws Exception {
        exec("CREATE TABLE tcn_a.d3 (x int)");
        exec("COMMENT ON TABLE tcn_a.d3 IS 'relation only'");
        assertEquals("relation only",
                scalar("SELECT obj_description('tcn_a.d3'::regclass, 'pg_class')"));
        assertNull(scalar("SELECT obj_description('tcn_a.d3'::regclass, 'pg_proc')"),
                "the OID is a relation's, so pg_proc has nothing to say about it");
    }

    @Test
    void aViewCarriesAColumnCommentToo() throws Exception {
        exec("CREATE TABLE tcn_a.d4 (id int, v int)");
        exec("CREATE VIEW tcn_a.d4v AS SELECT id, v FROM tcn_a.d4");
        exec("COMMENT ON COLUMN tcn_a.d4v.v IS 'view column'");
        assertEquals("view column", scalar("SELECT col_description('tcn_a.d4v'::regclass, 2)"));
    }

    @Test
    void aCommentSetToNullIsRemoved() throws Exception {
        exec("CREATE TABLE tcn_a.d5 (x int)");
        exec("COMMENT ON TABLE tcn_a.d5 IS 'here'");
        assertEquals("here", scalar("SELECT obj_description('tcn_a.d5'::regclass)"));
        exec("COMMENT ON TABLE tcn_a.d5 IS NULL");
        assertNull(scalar("SELECT obj_description('tcn_a.d5'::regclass)"));
    }
}
