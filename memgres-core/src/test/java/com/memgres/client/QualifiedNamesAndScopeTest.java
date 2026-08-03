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

/**
 * Reading a written name: which half of a qualified one failed, and which query level a bare one
 * belongs to.
 *
 * <p>A qualified name is looked up in two steps — find the schema, then find the object in it — and
 * which step failed is what PostgreSQL reports. A bare relation name in a sub-select is read
 * against the levels it is written inside: the relations of one FROM clause are computed side by
 * side, so a sub-select cannot read its neighbour, but the levels the whole query is nested inside
 * are always in reach and so is everything its own FROM brings in.
 *
 * <p>Every answer below was measured on PostgreSQL 18. The companion corpus file
 * qualified-names-and-scope.sql carries the same cases through the differential harness.
 */
class QualifiedNamesAndScopeTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("DROP TABLE IF EXISTS qst_u CASCADE");
        exec("DROP TABLE IF EXISTS qst_t CASCADE");
        exec("DROP DOMAIN IF EXISTS qst_dom CASCADE");
        exec("CREATE TABLE qst_t (id int PRIMARY KEY, v int)");
        exec("CREATE TABLE qst_u (id int PRIMARY KEY, v int)");
        exec("INSERT INTO qst_t VALUES (1,1),(2,2)");
        exec("INSERT INTO qst_u VALUES (1,1),(2,2)");
        exec("CREATE DOMAIN qst_dom AS int");
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
            return e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    /**
     * A relation in a schema that is not there is a relation that is not there, and IF EXISTS says
     * not to mind one of those.
     */
    @Test
    void ifExistsSkipsARelationInASchemaThatIsNotThere() {
        assertEquals("OK", stateOf("ALTER TABLE IF EXISTS qst_noschema.t ADD COLUMN j int"));
        assertEquals("OK", stateOf("ALTER TABLE IF EXISTS qst_noschema.t DROP COLUMN j"));
        assertEquals("OK", stateOf("ALTER TABLE IF EXISTS qst_noschema.t RENAME COLUMN i TO j"));
        assertEquals("OK", stateOf("ALTER TABLE IF EXISTS qst_noschema.t RENAME TO qst_x"));
        assertEquals("OK",
                stateOf("ALTER TABLE IF EXISTS qst_noschema.t ALTER COLUMN i SET DEFAULT 1"));
        assertEquals("OK",
                stateOf("ALTER TABLE IF EXISTS qst_noschema.t ALTER COLUMN i TYPE bigint"));
        assertEquals("OK", stateOf("DROP TABLE IF EXISTS qst_noschema.t"));
        assertEquals("OK", stateOf("DROP INDEX IF EXISTS qst_noschema.ix"));
        assertEquals("3F000", stateOf("ALTER TABLE qst_noschema.t ADD COLUMN j int"),
                "without IF EXISTS the schema is what is reported");
    }

    /** COMMENT and GRANT resolve the schema before they look for the relation. */
    @Test
    void aStatementThatOpensARelationReportsTheSchemaFirst() {
        assertEquals("schema \"qst_noschema\" does not exist",
                messageOf("COMMENT ON TABLE qst_noschema.t IS 'x'"));
        assertEquals("schema \"qst_noschema\" does not exist",
                messageOf("COMMENT ON COLUMN qst_noschema.t.c IS 'x'"));
        assertEquals("schema \"qst_noschema\" does not exist",
                messageOf("GRANT SELECT ON qst_noschema.t TO PUBLIC"));
        assertEquals("42P01", stateOf("COMMENT ON TABLE public.qst_nosuchtable IS 'x'"),
                "a schema that is there and holds no such relation is the relation's own");
    }

    /** The built-in types are pg_catalog's and no other schema's. */
    @Test
    void aQualifiedTypeIsLookedForInThatSchemaOnly() throws Exception {
        assertEquals("1", scalar("SELECT 1::pg_catalog.int4"));
        assertEquals("type \"public.int4\" does not exist", messageOf("SELECT 1::public.int4"));
        assertEquals("type \"pg_toast.int4\" does not exist", messageOf("SELECT 1::pg_toast.int4"));
        assertEquals("type \"information_schema.int4\" does not exist",
                messageOf("SELECT 1::information_schema.int4"));
        assertEquals("schema \"qst_noschema\" does not exist",
                messageOf("SELECT 1::qst_noschema.int4"),
                "a schema that is not there outranks the type written under it");
    }

    /** The SQL spellings the grammar rewrites are not names pg_catalog holds. */
    @Test
    void aSpellingTheGrammarRewritesIsNoPgCatalogType() {
        assertEquals("type \"pg_catalog.integer\" does not exist",
                messageOf("SELECT 1::pg_catalog.integer"));
        assertEquals("type \"pg_catalog.bigint\" does not exist",
                messageOf("SELECT 1::pg_catalog.bigint"));
        assertEquals("type \"pg_catalog.serial\" does not exist",
                messageOf("SELECT 1::pg_catalog.serial"));
        assertEquals("type \"pg_catalog.boolean\" does not exist",
                messageOf("SELECT 1::pg_catalog.boolean"));
    }

    /** A type this database was told about, and the type every relation mints of its own name. */
    @Test
    void aTypeThisDatabaseHasAnswersWhereItWasMade() throws Exception {
        assertEquals("1", scalar("SELECT 1::public.qst_dom"));
        assertEquals("type \"pg_catalog.qst_dom\" does not exist",
                messageOf("SELECT 1::pg_catalog.qst_dom"));
        assertEquals(null, scalar("SELECT NULL::public.qst_t"));
        assertEquals("type \"pg_catalog.qst_t\" does not exist",
                messageOf("SELECT NULL::pg_catalog.qst_t"));
    }

    /** The same reading wherever a type is written, not only in a cast. */
    @Test
    void aTypeIsReadTheSameWayInEveryPlaceOneIsWritten() {
        assertEquals("type \"pg_toast.int4\" does not exist",
                messageOf("CREATE TABLE qst_q1 (i pg_toast.int4)"));
        assertEquals("type \"pg_toast.int4\" does not exist",
                messageOf("SELECT CAST(1 AS pg_toast.int4)"));
    }

    /** A sub-select cannot read the relation entered beside it, however that one was written. */
    @Test
    void aSubSelectCannotReadItsNeighbour() {
        assertEquals("invalid reference to FROM-clause entry for table \"qst_t\"",
                messageOf("SELECT * FROM qst_t, (SELECT qst_t.id) s"));
        assertEquals("invalid reference to FROM-clause entry for table \"qst_t\"",
                messageOf("SELECT * FROM qst_t x, (SELECT qst_t.id) s"));
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT * FROM qst_t a, (SELECT a.v) b"));
        assertEquals("invalid reference to FROM-clause entry for table \"a\"",
                messageOf("SELECT * FROM qst_t a, (SELECT (SELECT a.v)) s"),
                "read from a level below the sub-select, and still its neighbour");
        assertEquals("missing FROM-clause entry for table \"qst_t\"",
                messageOf("SELECT * FROM (SELECT qst_t.id) s, qst_t"),
                "a relation not entered yet is simply missing");
    }

    /** Everything a sub-select's own FROM brings in is its own, however it was brought in. */
    @Test
    void aSubSelectOwnsEveryNameItsFromBringsIn() throws Exception {
        assertEquals("8", scalar(
                "SELECT count(*) FROM qst_t x, (SELECT qst_t.id FROM qst_t JOIN qst_u ON true) s"));
        assertEquals("8", scalar("SELECT count(*) FROM qst_t"
                + " JOIN (SELECT qst_t.id FROM qst_t JOIN qst_u ON true) s ON true"));
        assertEquals("2", scalar("SELECT count(*) FROM qst_t x,"
                + " (WITH qst_t AS (SELECT 1 AS id) SELECT qst_t.id FROM qst_t) s"));
        assertEquals("2", scalar("SELECT count(*) FROM qst_t x,"
                + " (SELECT (SELECT qst_t.id FROM qst_t LIMIT 1)) s"));
        assertEquals("6", scalar(
                "SELECT count(*) FROM qst_t x, (SELECT qst_t.id FROM qst_t UNION SELECT 9) s"));
    }

    /**
     * A level above the whole query is always in reach, and when a neighbour of the same name is
     * there too the outer one is what the name means — the neighbour is not in scope at all.
     */
    @Test
    void aLevelAboveTheQueryIsAlwaysInReach() throws Exception {
        assertEquals("2", scalar("SELECT count(*) FROM qst_t"
                + " WHERE EXISTS (SELECT 1 FROM qst_t z, (SELECT qst_t.v) q)"));
        assertEquals("2", scalar("SELECT count(*) FROM qst_t"
                + " WHERE EXISTS (SELECT 1 FROM qst_t, (SELECT qst_t.v) q)"));
        assertEquals("2", scalar("SELECT (SELECT count(*) FROM qst_t z, (SELECT qst_t.v) q)"
                + " FROM qst_t LIMIT 1"));
        assertEquals("1", scalar("SELECT b.v FROM qst_t a, LATERAL (SELECT a.v) b ORDER BY 1"),
                "LATERAL brings the neighbour itself into reach");
    }

    /** A variadic signature is written one element at a time. */
    @Test
    void aVariadicSignatureTakesTheElementsOfItsLastParameter() throws Exception {
        assertEquals("1", scalar("SELECT jsonb_extract_path('{\"a\":1}'::jsonb, 'a'::text)"));
        assertEquals("1", scalar("SELECT jsonb_extract_path_text('{\"a\":1}'::jsonb, 'a'::text)"));
        assertEquals("1", scalar(
                "SELECT jsonb_extract_path('{\"a\":{\"b\":1}}'::jsonb, 'a'::text, 'b'::text)"));
        assertEquals("1", scalar("SELECT json_extract_path_text('{\"a\":1}'::json, 'a'::text)"));
        assertEquals("1", scalar("SELECT jsonb_extract_path('{\"a\":1}'::jsonb, 'a'::varchar)"));
        assertEquals("1", scalar("SELECT jsonb_extract_path('{\"a\":1}'::jsonb, 'a'::name)"));
    }

    /** A call is named by what it returns, which is not always what its value looks like. */
    @Test
    void aNestedCallIsNamedByTheTypeItReturns() {
        assertEquals("function multirange(int4range, int4range) does not exist",
                messageOf("SELECT multirange(int4range(1,5), int4range(10,15))"));
    }

    /**
     * A catalog column memgres spells differently from PostgreSQL is still one the functions
     * declared over PostgreSQL's spelling may read.
     */
    @Test
    void aCatalogColumnSpelledDifferentlyStillResolves() throws Exception {
        assertEquals("0", scalar("SELECT count(*) FROM pg_catalog.pg_publication_rel pr"
                + " WHERE pg_catalog.pg_get_expr(pr.prqual, pr.prrelid) IS NOT NULL"));
        assertEquals("0", scalar("SELECT count(*) FROM pg_catalog.pg_attrdef ad"
                + " WHERE pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) IS NULL"));
    }
}
