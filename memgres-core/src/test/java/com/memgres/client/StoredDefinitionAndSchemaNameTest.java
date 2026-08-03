package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Three rules, each measured against PostgreSQL 18 and each asserted together with the ordinary
 * shapes around it.
 *
 * <p><b>A stored expression is analysed like any other.</b> A CHECK on a column and on a table, a
 * DEFAULT, an index expression and predicate, a generated column, a policy, a rule's qualification,
 * a domain constraint, a partition bound and a trigger's WHEN are all read the way an expression in
 * a query is read, so a call carrying FILTER, DISTINCT or an aggregate ORDER BY without being an
 * aggregate is refused in every one of them. Which of a definition's faults is reported follows the
 * order the expression is read in rather than the order of the checks, so one walk finds all of
 * them: {@code CHECK (count(*) > 0 AND abs(i) FILTER (…) > 0)} names the aggregate and the same two
 * written the other way round name the FILTER.
 *
 * <p><b>A schema qualifier has to name a schema.</b> On a type name that is 3F000 wherever a type is
 * written, whether or not the expression holding it would ever have been evaluated. On a relation a
 * DDL statement names it is 3F000 too. On a relation a query reads it is not: that is a different
 * lookup, and it reports 42P01 naming the relation it could not find.
 *
 * <p><b>A domain is a type of its own.</b> pg_typeof, format_type, information_schema and the
 * RowDescription a client reads all answer with the domain rather than with the type it is built on.
 */
class StoredDefinitionAndSchemaNameTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("DROP TABLE IF EXISTS sdn_t CASCADE");
        exec("DROP SCHEMA IF EXISTS sdn_s CASCADE");
        exec("DROP DOMAIN IF EXISTS sdn_yn CASCADE");
        exec("DROP DOMAIN IF EXISTS sdn_pos CASCADE");
        exec("DROP DOMAIN IF EXISTS sdn_arr CASCADE");
        exec("DROP TYPE IF EXISTS sdn_mood CASCADE");
        exec("CREATE SCHEMA sdn_s");
        exec("CREATE TYPE sdn_mood AS ENUM ('ok','bad')");
        exec("CREATE DOMAIN sdn_yn AS boolean");
        exec("CREATE DOMAIN sdn_pos AS int");
        exec("CREATE DOMAIN sdn_arr AS int[]");
        exec("CREATE TABLE sdn_t (id int PRIMARY KEY, v int, txt text,"
                + " y sdn_yn, p sdn_pos, a sdn_arr, e sdn_mood)");
        exec("INSERT INTO sdn_t VALUES (1,1,'aa',true,3,'{1,2}','ok'),"
                + "(2,2,'ab',false,4,'{3}','bad')");
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

    private static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    // ---- A clause only an aggregate may carry, inside a stored definition ----

    @Test
    void aCheckConstraintIsJudgedLikeAnyOtherExpression() {
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_g1 (i int CHECK (abs(i) FILTER (WHERE true) > 0))"));
        assertEquals("DISTINCT specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_g2 (i int CHECK (abs(DISTINCT i) > 0))"));
        assertEquals("ORDER BY specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_g3 (i int CHECK (abs(i ORDER BY i) > 0))"));
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_g4 (i int, CHECK (abs(i) FILTER (WHERE true) > 0))"),
                "a table-level CHECK is the same expression in a different place");
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("ALTER TABLE sdn_t ADD CONSTRAINT sdn_g5"
                        + " CHECK (abs(v) FILTER (WHERE true) > 0)"));
    }

    @Test
    void aDefaultAndAGeneratedColumnAreJudgedTheSameWay() {
        assertEquals("ORDER BY specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_d1 (i int DEFAULT (abs(1 ORDER BY 1)))"));
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_d2 (i int,"
                        + " j int GENERATED ALWAYS AS (abs(i) FILTER (WHERE true)) STORED)"));
        assertEquals("ORDER BY specified, but abs is not an aggregate function",
                messageOf("ALTER TABLE sdn_t ALTER COLUMN v SET DEFAULT (abs(1 ORDER BY 1))"));
        assertEquals("ORDER BY specified, but abs is not an aggregate function",
                messageOf("ALTER TABLE sdn_t ADD COLUMN sdn_d3 int DEFAULT (abs(1 ORDER BY 1))"));
    }

    @Test
    void anIndexKeyAndPredicateAreJudgedTheSameWay() {
        assertEquals("ORDER BY specified, but abs is not an aggregate function",
                messageOf("CREATE INDEX sdn_i1 ON sdn_t ((abs(id ORDER BY id)))"));
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE INDEX sdn_i2 ON sdn_t (id)"
                        + " WHERE abs(id) FILTER (WHERE true) > 0"));
    }

    @Test
    void aPolicyARuleAndADomainConstraintAreJudgedTheSameWay() {
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE POLICY sdn_p1 ON sdn_t USING (abs(id) FILTER (WHERE true) > 0)"));
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE POLICY sdn_p2 ON sdn_t FOR INSERT"
                        + " WITH CHECK (abs(id) FILTER (WHERE true) > 0)"));
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE RULE sdn_p3 AS ON DELETE TO sdn_t"
                        + " WHERE abs(old.id) FILTER (WHERE true) > 0 DO INSTEAD NOTHING"));
        assertEquals("DISTINCT specified, but abs is not an aggregate function",
                messageOf("CREATE DOMAIN sdn_p4 AS int CHECK (abs(DISTINCT VALUE) > 0)"));
    }

    @Test
    void aPartitionBoundAndAViewBodyAreJudgedTheSameWay() throws Exception {
        exec("DROP TABLE IF EXISTS sdn_pt CASCADE");
        exec("CREATE TABLE sdn_pt (a int) PARTITION BY RANGE (a)");
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_pt1 PARTITION OF sdn_pt"
                        + " FOR VALUES FROM (abs(1) FILTER (WHERE true)) TO (10)"));
        exec("DROP TABLE sdn_pt CASCADE");
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE VIEW sdn_v1 AS SELECT abs(id) FILTER (WHERE true) FROM sdn_t"));
    }

    /**
     * A trigger's WHEN is analysed against the relation before the function the trigger will call
     * is looked for, so the condition is what is reported when both are wrong.
     */
    @Test
    void aTriggerWhenConditionIsJudgedBeforeTheFunctionIsLookedFor() throws Exception {
        exec("DROP FUNCTION IF EXISTS sdn_tf() CASCADE");
        exec("CREATE FUNCTION sdn_tf() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$"
                + " LANGUAGE plpgsql");
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE TRIGGER sdn_t1 BEFORE INSERT ON sdn_t FOR EACH ROW"
                        + " WHEN (abs(NEW.id) FILTER (WHERE true) > 0)"
                        + " EXECUTE FUNCTION sdn_nofn()"));
        assertEquals("column new.nosuchcol does not exist",
                messageOf("CREATE TRIGGER sdn_t2 BEFORE INSERT ON sdn_t FOR EACH ROW"
                        + " WHEN (NEW.nosuchcol > 0) EXECUTE FUNCTION sdn_tf()"));
        assertEquals("aggregate functions are not allowed in trigger WHEN conditions",
                messageOf("CREATE TRIGGER sdn_t3 BEFORE INSERT ON sdn_t FOR EACH ROW"
                        + " WHEN (count(*) > 0) EXECUTE FUNCTION sdn_tf()"));
        assertEquals("function sdn_nofn() does not exist",
                messageOf("CREATE TRIGGER sdn_t4 BEFORE INSERT ON sdn_t FOR EACH ROW"
                        + " WHEN (NEW.v > 0) EXECUTE FUNCTION sdn_nofn()"),
                "a condition that stands leaves the missing function to be reported");
        exec("CREATE TRIGGER sdn_t5 BEFORE INSERT ON sdn_t FOR EACH ROW"
                + " WHEN (NEW.v > 0) EXECUTE FUNCTION sdn_tf()");
        exec("DROP TRIGGER sdn_t5 ON sdn_t");
        exec("DROP FUNCTION sdn_tf() CASCADE");
    }

    /**
     * All four kinds of fault are found by one walk, so the leftmost is named. Reading them in a
     * fixed order of kinds got two of these three the wrong way round.
     */
    @Test
    void theLeftmostFaultInADefinitionIsTheOneReported() {
        assertEquals("aggregate functions are not allowed in check constraints",
                messageOf("CREATE TABLE sdn_o1 (i int"
                        + " CHECK (count(*) > 0 AND abs(i) FILTER (WHERE true) > 0))"));
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_o2 (i int"
                        + " CHECK (abs(i) FILTER (WHERE true) > 0 AND count(*) > 0))"));
        assertEquals("cannot use subquery in check constraint",
                messageOf("CREATE TABLE sdn_o3 (i int"
                        + " CHECK ((SELECT 1) > 0 AND abs(i) FILTER (WHERE true) > 0))"));
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_o4 (i int CHECK (abs(i) FILTER (WHERE true) > (SELECT 1)))"));
        assertEquals("aggregate functions are not allowed in check constraints",
                messageOf("CREATE TABLE sdn_o5 (i int CHECK (count(*) > 0 AND (SELECT 1) > 0))"));
        assertEquals("FILTER specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_o6 (i int CHECK (abs(i) FILTER (WHERE true)))"),
                "the FILTER is judged before the type of what it stands in");
        assertEquals("FILTER specified, but pg_catalog.abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_o7 (i int"
                        + " CHECK (pg_catalog.abs(i) FILTER (WHERE true) > 0))"),
                "a qualified built-in is named the way it was written");
        assertEquals("DISTINCT specified, but abs is not an aggregate function",
                messageOf("CREATE TABLE sdn_o8 (i int CHECK (abs(DISTINCT i ORDER BY i) > 0))"),
                "DISTINCT is read before an aggregate ORDER BY");
        assertEquals("cannot use column reference in DEFAULT expression",
                messageOf("CREATE TABLE sdn_o9 (i int, j int DEFAULT (abs(i) FILTER (WHERE true)))"));
        assertEquals("aggregate functions are not allowed in DEFAULT expressions",
                messageOf("CREATE TABLE sdn_o10 (i int DEFAULT (count(*) FILTER (WHERE true)))"));
    }

    /** An aggregate is what these clauses are for, and a plain call without one is ordinary. */
    @Test
    void theOrdinaryDefinitionsAreStillAccepted() throws Exception {
        exec("CREATE VIEW sdn_ok1 AS SELECT count(*) FILTER (WHERE v > 1) AS c,"
                + " count(DISTINCT v) AS d, string_agg(txt, ',' ORDER BY id) AS s FROM sdn_t");
        assertEquals("1", scalar("SELECT c::text FROM sdn_ok1"));
        assertEquals("aa,ab", scalar("SELECT s FROM sdn_ok1"));
        exec("DROP VIEW sdn_ok1");

        exec("CREATE TABLE sdn_ok2 (i int CHECK (abs(i) > 0 AND coalesce(i, 0) >= 0),"
                + " j int DEFAULT greatest(1, 2), g int GENERATED ALWAYS AS (abs(i) * 2) STORED)");
        exec("INSERT INTO sdn_ok2 (i) VALUES (5)");
        assertEquals("10", scalar("SELECT g::text FROM sdn_ok2"));
        exec("DROP TABLE sdn_ok2");

        exec("CREATE INDEX sdn_ok3 ON sdn_t ((lower(txt)), (abs(v))) WHERE abs(v) > 0");
        exec("DROP INDEX sdn_ok3");
        exec("CREATE INDEX sdn_ok4 ON sdn_t ((id::bigint))");
        exec("DROP INDEX sdn_ok4");
        exec("CREATE DOMAIN sdn_ok5 AS text CHECK (length(VALUE) > 0)");
        exec("DROP DOMAIN sdn_ok5");
        exec("CREATE POLICY sdn_ok6 ON sdn_t USING (abs(id) > 0 AND txt IS NOT NULL)");
        exec("DROP POLICY sdn_ok6 ON sdn_t");
        exec("CREATE POLICY sdn_ok7 ON sdn_t USING (id IN (SELECT id FROM sdn_t))");
        exec("DROP POLICY sdn_ok7 ON sdn_t");
        exec("CREATE RULE sdn_ok8 AS ON DELETE TO sdn_t WHERE abs(old.id) > 100 DO INSTEAD NOTHING");
        exec("DROP RULE sdn_ok8 ON sdn_t");
    }

    /** A window call and a sub-query keep their own complaints rather than earning the new one. */
    @Test
    void theOtherRefusalsADefinitionAlreadyEarnedAreUnchanged() {
        assertEquals("window functions are not allowed in check constraints",
                messageOf("CREATE TABLE sdn_w1 (i int CHECK (row_number() OVER () > 0))"));
        assertEquals("window function row_number requires an OVER clause",
                messageOf("CREATE TABLE sdn_w2 (i int CHECK (row_number() > 0))"));
        assertEquals("window functions are not allowed in policy expressions",
                messageOf("CREATE POLICY sdn_w3 ON sdn_t USING (row_number() OVER () > 0)"));
        assertEquals("cannot use subquery in check constraint",
                messageOf("CREATE TABLE sdn_w4 (i int CHECK (i IN (SELECT 1)))"));
        assertEquals("cannot use subquery in index predicate",
                messageOf("CREATE INDEX sdn_w5 ON sdn_t (id) WHERE id IN (SELECT 1)"));
    }

    // ---- A schema qualifier ----

    @Test
    void aTypeNamedUnderASchemaThatIsNotThereIsRefused() {
        assertEquals("schema \"sdn_no\" does not exist", messageOf("SELECT 1::sdn_no.int4"));
        assertEquals("3F000", stateOf("SELECT CAST(1 AS sdn_no.int4)"));
        assertEquals("3F000", stateOf("SELECT ARRAY[1]::sdn_no.int4[]"));
        assertEquals("3F000", stateOf("CREATE TABLE sdn_e1 (a sdn_no.int4)"));
        assertEquals("3F000", stateOf("ALTER TABLE sdn_t ADD COLUMN sdn_e2 sdn_no.int4"));
        assertEquals("3F000", stateOf("ALTER TABLE sdn_t ALTER COLUMN v TYPE sdn_no.int4"));
        assertEquals("3F000", stateOf("CREATE DOMAIN sdn_e3 AS sdn_no.int4"));
        assertEquals("3F000", stateOf("CREATE FUNCTION sdn_e4(a sdn_no.int4) RETURNS int"
                + " AS $$ SELECT 1 $$ LANGUAGE sql"));
        assertEquals("3F000", stateOf("CREATE FUNCTION sdn_e5(a int) RETURNS sdn_no.int4"
                + " AS $$ SELECT 1 $$ LANGUAGE sql"));
        assertEquals("3F000", stateOf("PREPARE sdn_e6 (sdn_no.int4) AS SELECT 1"));
    }

    /** The type is resolved while the statement is analysed, not while it runs. */
    @Test
    void aTypeIsResolvedWhereverItStandsAndWhetherOrNotItWouldBeEvaluated() {
        assertEquals("3F000", stateOf("SELECT CASE WHEN false THEN 1::sdn_no.int4 ELSE 0 END"));
        assertEquals("3F000", stateOf("WITH c AS (SELECT 1::sdn_no.int4) SELECT 1"));
        assertEquals("3F000", stateOf("SELECT (SELECT 1::sdn_no.int4)"));
        assertEquals("3F000", stateOf("CREATE INDEX sdn_e7 ON sdn_t ((id::sdn_no.int4))"));
        assertEquals("3F000", stateOf("CREATE TABLE sdn_e8 (i int CHECK (i::sdn_no.int4 > 0))"));
        assertEquals("schema \"sdn_no1\" does not exist",
                messageOf("SELECT 1::sdn_no1.int4, 1::sdn_no2.int4"),
                "the leftmost qualifier is the one named");
    }

    /** The range table is built before the target list, and a column is resolved after it. */
    @Test
    void aRelationThatIsNotThereOutranksAQualifiedTypeButAColumnDoesNot() {
        assertEquals("relation \"sdn_nosuch\" does not exist",
                messageOf("SELECT 1::sdn_no.int4 FROM sdn_nosuch"));
        assertEquals("schema \"sdn_no\" does not exist",
                messageOf("SELECT sdn_nocol::sdn_no.int4 FROM sdn_t"));
    }

    @Test
    void aDdlStatementNamingASchemaThatIsNotThereIsRefused() {
        assertEquals("schema \"sdn_no\" does not exist",
                messageOf("CREATE INDEX sdn_r1 ON sdn_no.sdn_t (id)"));
        assertEquals("3F000", stateOf("ALTER TABLE sdn_no.sdn_t ADD COLUMN c int"));
        assertEquals("3F000", stateOf("CREATE TABLE sdn_no.sdn_r2 (id int)"));
        assertEquals("3F000", stateOf("CREATE VIEW sdn_no.sdn_r3 AS SELECT 1"));
        assertEquals("3F000", stateOf("CREATE SEQUENCE sdn_no.sdn_r4"));
        assertEquals("3F000", stateOf("CREATE DOMAIN sdn_no.sdn_r5 AS int"));
        assertEquals("3F000", stateOf("CREATE FUNCTION sdn_no.sdn_r6() RETURNS int"
                + " AS $$ SELECT 1 $$ LANGUAGE sql"));
        assertEquals("3F000", stateOf("CREATE TABLE sdn_no.sdn_r7 AS SELECT 1 AS a"));
        assertEquals("3F000", stateOf("TRUNCATE sdn_no.sdn_t"));
        assertEquals("3F000", stateOf("DROP TABLE sdn_no.sdn_t"));
        assertEquals("3F000", stateOf("SELECT 1 OPERATOR(sdn_no.+) 1"));
        assertEquals("OK", stateOf("DROP TABLE IF EXISTS sdn_no.sdn_t"),
                "IF EXISTS skips on the schema by name rather than refusing");
    }

    /** Reading a relation is a different lookup, and it reports the relation rather than the schema. */
    @Test
    void aRelationAQueryReadsIsReportedAsARelation() {
        assertEquals("relation \"sdn_no.sdn_t\" does not exist",
                messageOf("SELECT * FROM sdn_no.sdn_t"));
        assertEquals("relation \"sdn_no.sdn_t\" does not exist",
                messageOf("INSERT INTO sdn_no.sdn_t VALUES (9)"));
        assertEquals("relation \"sdn_no.sdn_t\" does not exist",
                messageOf("UPDATE sdn_no.sdn_t SET v = 1"));
        assertEquals("relation \"sdn_no.sdn_t\" does not exist",
                messageOf("DELETE FROM sdn_no.sdn_t"));
        assertEquals("relation \"sdn_no.sdn_t\" does not exist",
                messageOf("CREATE VIEW sdn_r8 AS SELECT * FROM sdn_no.sdn_t"));
        assertEquals("relation \"public.sdn_nosuch\" does not exist",
                messageOf("CREATE INDEX sdn_r9 ON public.sdn_nosuch (id)"),
                "a schema that is there leaves the relation to be reported, qualified as written");
    }

    /** A qualifier that names a schema is ordinary everywhere one may be written. */
    @Test
    void aQualifierThatNamesASchemaIsOrdinary() throws Exception {
        assertEquals("1", scalar("SELECT (1::pg_catalog.int4)::text"));
        assertEquals("x", scalar("SELECT 'x'::pg_catalog.text"));
        assertEquals("1", scalar("SELECT CAST(1 AS pg_catalog.numeric(5,0))::text"));
        assertEquals("{1,2}", scalar("SELECT (ARRAY[1,2]::pg_catalog.int4[])::text"));
        assertEquals("1", scalar("SELECT pg_catalog.abs(-1)::text"));
        assertEquals("2", scalar("SELECT (1 OPERATOR(pg_catalog.+) 1)::text"));

        exec("CREATE TABLE sdn_s.sdn_q1 (i pg_catalog.int4, j pg_catalog.text)");
        exec("INSERT INTO sdn_s.sdn_q1 VALUES (1, 'x')");
        assertEquals("x", scalar("SELECT j FROM sdn_s.sdn_q1"));
        exec("CREATE DOMAIN sdn_s.sdn_q2 AS pg_catalog.int4");
        exec("CREATE TABLE sdn_s.sdn_q3 (k sdn_s.sdn_q2)");
        exec("INSERT INTO sdn_s.sdn_q3 VALUES (7)");
        assertEquals("7", scalar("SELECT k::text FROM sdn_s.sdn_q3"));
        exec("CREATE FUNCTION sdn_s.sdn_q4(a pg_catalog.int4) RETURNS pg_catalog.int4"
                + " AS $$ SELECT a + 1 $$ LANGUAGE sql");
        assertEquals("2", scalar("SELECT sdn_s.sdn_q4(1)::text"));
        exec("CREATE VIEW sdn_s.sdn_q5 AS SELECT id FROM sdn_t");
        assertEquals("1", scalar("SELECT id::text FROM sdn_s.sdn_q5 ORDER BY id"));
        exec("CREATE INDEX sdn_q6 ON public.sdn_t (v)");
        exec("DROP INDEX public.sdn_q6");
        exec("ALTER TABLE public.sdn_t ADD COLUMN sdn_q7 pg_catalog.int4");
        exec("ALTER TABLE public.sdn_t DROP COLUMN sdn_q7");
    }

    // ---- The name a type answers to ----

    @Test
    void aDomainAnswersToItsOwnName() throws Exception {
        assertEquals("sdn_yn", scalar("SELECT pg_typeof(y)::text FROM sdn_t LIMIT 1"));
        assertEquals("sdn_pos", scalar("SELECT pg_typeof(p)::text FROM sdn_t LIMIT 1"));
        assertEquals("sdn_arr", scalar("SELECT pg_typeof(a)::text FROM sdn_t LIMIT 1"));
        assertEquals("sdn_pos", scalar("SELECT pg_typeof(1::sdn_pos)::text"));
    }

    @Test
    void everyOtherKindOfTypeStillAnswersToItsOwnName() throws Exception {
        assertEquals("sdn_mood", scalar("SELECT pg_typeof(e)::text FROM sdn_t LIMIT 1"));
        assertEquals("integer", scalar("SELECT pg_typeof(id)::text FROM sdn_t LIMIT 1"));
        assertEquals("text", scalar("SELECT pg_typeof(txt)::text FROM sdn_t LIMIT 1"));
        assertEquals("integer", scalar("SELECT pg_typeof(p + 1)::text FROM sdn_t LIMIT 1"),
                "an expression over a domain-typed column is of the base type again");
    }

    @Test
    void aDerivedRelationCarriesTheDomainNameThrough() throws Exception {
        assertEquals("sdn_yn",
                scalar("SELECT pg_typeof(b)::text FROM (SELECT y AS b FROM sdn_t) q LIMIT 1"));
        assertEquals("sdn_yn",
                scalar("WITH q AS (SELECT y AS b FROM sdn_t) SELECT pg_typeof(b)::text FROM q LIMIT 1"));
        assertEquals("sdn_pos",
                scalar("SELECT pg_typeof(x.p)::text FROM sdn_t x JOIN sdn_t z ON x.id = z.id LIMIT 1"));
    }

    @Test
    void theCatalogsNameTheDomainToo() throws Exception {
        assertEquals("sdn_yn", scalar("SELECT format_type(at.atttypid, at.atttypmod)"
                + " FROM pg_attribute at JOIN pg_class c ON c.oid = at.attrelid"
                + " WHERE c.relname = 'sdn_t' AND at.attname = 'y'"));
        assertEquals("sdn_arr", scalar("SELECT format_type(at.atttypid, at.atttypmod)"
                + " FROM pg_attribute at JOIN pg_class c ON c.oid = at.attrelid"
                + " WHERE c.relname = 'sdn_t' AND at.attname = 'a'"));
        assertEquals("sdn_yn", scalar("SELECT domain_name FROM information_schema.columns"
                + " WHERE table_name = 'sdn_t' AND column_name = 'y'"));
        assertEquals("boolean", scalar("SELECT data_type FROM information_schema.columns"
                + " WHERE table_name = 'sdn_t' AND column_name = 'y'"));
        assertNull(scalar("SELECT domain_name FROM information_schema.columns"
                + " WHERE table_name = 'sdn_t' AND column_name = 'e'"),
                "an enum is not a domain, so there is no domain to name");
    }

    /** What the client is told a column is, which is the domain's base type on the wire. */
    @Test
    void theRowDescriptionADomainColumnIsSentAsIsItsBaseType() throws Exception {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT y, p, a, e FROM sdn_t WHERE id = 1")) {
            ResultSetMetaData md = rs.getMetaData();
            assertEquals("bool", md.getColumnTypeName(1));
            assertEquals("int4", md.getColumnTypeName(2));
            assertEquals("_int4", md.getColumnTypeName(3));
            assertEquals("sdn_mood", md.getColumnTypeName(4));
            assertTrue(rs.next());
            assertEquals("t", rs.getString(1));
            assertEquals("3", rs.getString(2));
            assertEquals("{1,2}", rs.getString(3));
            assertEquals("ok", rs.getString(4));
        }
    }
}
