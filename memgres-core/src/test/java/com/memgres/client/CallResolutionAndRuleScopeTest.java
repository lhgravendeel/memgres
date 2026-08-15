package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a call names, and what a rule may name.
 *
 * <p>A one-argument call written as a type name is a cast, and only where pg_cast has one: over an
 * integer column PostgreSQL makes {@code text(a)} and {@code int8(a)} and refuses {@code date(a)},
 * {@code uuid(a)} and the geometric constructors with 42883 "function date(integer) does not
 * exist". memgres read every such call as a cast, so a CHECK naming a conversion that does not
 * exist defined a constraint and the call answered a value at run time. The same silence covered
 * two neighbours: a name carrying a schema other than pg_catalog was handed back unjudged, so
 * {@code public.nosuchfunc(a)} was accepted where PostgreSQL says the function does not exist and
 * a schema of no one is 3F000; and a built-in was let past every polymorphic parameter, so
 * {@code lower(true)} reached lower's range signature although anyrange stands for a kind of type
 * rather than for any type.
 *
 * <p>Inside a rule the ruled relation is not a name at all. PostgreSQL enters it twice, as old and
 * as new, so its own name is a range-table entry out of reach: 42P01 'invalid reference to
 * FROM-clause entry for table "t"', offering the alias old as a hint everywhere except an INSERT
 * rule's qualification, where the entry exists but is out of scope and a DETAIL says so instead. A
 * name in no range table is the other 42P01, 'missing FROM-clause entry', with nothing to suggest.
 * On a view old and new hold the view's own columns, so a column of the table underneath is 42703.
 * memgres accepted all of it and stored rules whose bodies named rows nothing would bind.
 *
 * <p>The fields this asserts -- SQLSTATE, message, DETAIL and HINT -- and the rules of more than
 * one action, whose internal semicolons no corpus file can carry, were read off PostgreSQL 18
 * before they were written down.
 */
class CallResolutionAndRuleScopeTest {

    /** The hint PostgreSQL sends with every 42883 raised by resolution. */
    private static final String CAST_HINT =
            "No function matches the given name and argument types. "
                    + "You might need to add explicit type casts.";

    /** The hint offered wherever the ruled relation's own name was written and old is in scope. */
    private static final String OLD_ALIAS_HINT =
            "Perhaps you meant to reference the table alias \"old\".";

    private static final String OLD_ENTRY_DETAIL =
            "There is an entry for table \"old\", but it cannot be referenced from this part of "
                    + "the query.";

    private static final String NEW_ENTRY_DETAIL =
            "There is an entry for table \"new\", but it cannot be referenced from this part of "
                    + "the query.";

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ------------------------------------------------------------------ helpers

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    /** Every row of the query, columns joined with a pipe, in the order the query returned them. */
    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    /** The error fields a statement raises, as a client reads them off the wire. */
    private static ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof PSQLException, "expected a server error from: " + sql);
        return ((PSQLException) thrown).getServerErrorMessage();
    }

    /** Assert every field of the refusal: state, primary message, DETAIL and HINT. */
    private static void refused(String sql, String state, String message, String detail,
                                String hint) {
        ServerErrorMessage m = fieldsOf(sql);
        assertEquals(state + " | " + message + " | " + detail + " | " + hint,
                m.getSQLState() + " | " + m.getMessage() + " | " + m.getDetail() + " | "
                        + m.getHint(),
                sql);
    }

    /** A 42883 carrying the standard hint and no detail. */
    private static void noSuchFunction(String signature, String sql) {
        refused(sql, "42883", "function " + signature + " does not exist", null, CAST_HINT);
    }

    /** A rule refused for naming the ruled relation where old is in scope. */
    private static void namesTheRuledRelation(String relation, String sql) {
        refused(sql, "42P01", "invalid reference to FROM-clause entry for table \"" + relation
                + "\"", null, OLD_ALIAS_HINT);
    }

    /** A rule refused for naming a relation no range table of it holds. */
    private static void namesAMissingRelation(String relation, String sql) {
        refused(sql, "42P01", "missing FROM-clause entry for table \"" + relation + "\"", null,
                null);
    }

    private static void runs(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    /** A table, a second table for a rule to write, and a view over the first. */
    private static void freshRuleRelations() throws SQLException {
        exec("DROP VIEW IF EXISTS zzt4c_v CASCADE");
        exec("DROP TABLE IF EXISTS zzt4c_t, zzt4c_o CASCADE");
        exec("CREATE TABLE zzt4c_t (i int, j text)");
        exec("CREATE TABLE zzt4c_o (i int, j text)");
        exec("CREATE VIEW zzt4c_v AS SELECT i AS total, j AS caption FROM zzt4c_t");
    }

    private static void dropRuleRelations() throws SQLException {
        exec("DROP VIEW IF EXISTS zzt4c_v CASCADE");
        exec("DROP TABLE IF EXISTS zzt4c_t, zzt4c_o CASCADE");
    }

    // ============================================================== a type name, called

    /**
     * A type name called over an argument it has no cast from is a function that does not exist,
     * and the refusal reaches the definition rather than waiting for the constraint to be checked.
     */
    @Test
    void aTypeNameWithNoCastFromItsArgumentIsRefusedWhereTheDefinitionIsRead() throws Exception {
        exec("DROP TABLE IF EXISTS zzt4c_c1 CASCADE");
        for (String type : new String[]{"date", "uuid", "inet", "jsonb", "xml", "macaddr",
                "tsvector", "point", "box", "circle", "polygon", "lseg", "path", "int4multirange",
                "timestamptz", "timetz"}) {
            noSuchFunction(type + "(integer)",
                    "CREATE TABLE zzt4c_c1 (a int, CHECK (" + type + "(a) IS NOT NULL))");
        }
        // The expression's type is what is judged, not the column the expression was read from.
        noSuchFunction("date(integer)",
                "CREATE TABLE zzt4c_c1 (a int, CHECK (date(a + 1) IS NOT NULL))");
        // int4(boolean) is a cast PostgreSQL makes and int8(boolean) is not; the two are told apart.
        noSuchFunction("int8(boolean)",
                "CREATE TABLE zzt4c_c1 (a bool, CHECK (int8(a) IS NOT NULL))");
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname = 'zzt4c_c1'"));
    }

    /** The same names, refused the same way where the call is written to run. */
    @Test
    void aTypeNameWithNoCastFromItsArgumentIsRefusedWhereItIsWrittenToRun() {
        for (String type : new String[]{"date", "uuid", "inet", "jsonb", "xml", "macaddr",
                "tsvector", "point", "box", "circle", "polygon", "lseg", "path", "timestamptz",
                "timetz"}) {
            noSuchFunction(type + "(integer)", "SELECT " + type + "(1)");
        }
        noSuchFunction("int8(boolean)", "SELECT int8(true)");
        noSuchFunction("date(integer)", "SELECT date(1 + 1)");
        // The argument is named by the type it was written with: a bigint is not an integer.
        noSuchFunction("date(bigint)", "SELECT date(1::bigint)");
    }

    /** Every conversion pg_cast does hold still defines its constraint and still answers. */
    @Test
    void theTypeNamesThatHaveACastFromTheirArgumentStillAnswer() throws Exception {
        exec("DROP TABLE IF EXISTS zzt4c_k1, zzt4c_k2, zzt4c_k3, zzt4c_k4 CASCADE");
        runs("CREATE TABLE zzt4c_k1 (a int,"
                + " CHECK (text(a) IS NOT NULL), CHECK (int8(a) IS NOT NULL),"
                + " CHECK (float8(a) IS NOT NULL), CHECK (bool(a) IS NOT NULL),"
                + " CHECK (oid(a) IS NOT NULL), CHECK (money(a) IS NOT NULL),"
                + " CHECK (bytea(a) IS NOT NULL), CHECK (name(a) IS NOT NULL),"
                + " CHECK (regclass(a) IS NOT NULL), CHECK (int4(a) IS NOT NULL),"
                + " CHECK (float4(a) IS NOT NULL), CHECK (int2(a) IS NOT NULL))");
        runs("CREATE TABLE zzt4c_k2 (a json, CHECK (jsonb(a) IS NOT NULL))");
        runs("CREATE TABLE zzt4c_k3 (a cidr, CHECK (inet(a) IS NOT NULL))");
        // A type casts to itself, so a column's own type name is always a call it can make.
        runs("CREATE TABLE zzt4c_k4 (a date, CHECK (date(a) IS NOT NULL),"
                + " CHECK (timestamptz(a) IS NOT NULL))");
        assertEquals("4", scalar("SELECT count(*) FROM pg_class WHERE relname LIKE 'zzt4c_k%'"));

        assertEquals("2|1|1|1", rows("SELECT int4(1.9) AS a, text(1) AS b, int8(1) AS c,"
                + " float8(1) AS d").get(0));
        assertEquals("t|1|1|1|1.5", rows("SELECT bool(1) AS a, oid(1) AS b, name(1) AS c,"
                + " int2(1) AS d, float4(1.5) AS e").get(0));
        assertEquals("1|2|1", rows("SELECT int4(1::bigint) AS a, int8(1.5::numeric) AS b,"
                + " int4(true) AS c").get(0));
        assertEquals("2020-01-02|2020-01-02", rows("SELECT date('2020-01-02'::text) AS a,"
                + " date(TIMESTAMP '2020-01-02 03:04:05') AS b").get(0));
        assertEquals("(0.5,0.5)|{[1.5,5.5)}",
                rows("SELECT point(box '((0,0),(1,1))')::text AS a,"
                        + " nummultirange(numrange(1.5, 5.5))::text AS b").get(0));
        exec("DROP TABLE IF EXISTS zzt4c_k1, zzt4c_k2, zzt4c_k3, zzt4c_k4 CASCADE");
    }

    /** The constraint a definition kept is the constraint that is enforced. */
    @Test
    void aConstraintOverACallThatResolvedIsStillEnforced() throws Exception {
        exec("DROP TABLE IF EXISTS zzt4c_ck CASCADE");
        exec("CREATE TABLE zzt4c_ck (a int, CONSTRAINT zzt4c_ck_ck CHECK (int4(a) > 0))");
        exec("INSERT INTO zzt4c_ck VALUES (1)");
        ServerErrorMessage m = fieldsOf("INSERT INTO zzt4c_ck VALUES (-1)");
        assertEquals("23514", m.getSQLState());
        assertEquals("new row for relation \"zzt4c_ck\" violates check constraint \"zzt4c_ck_ck\"",
                m.getMessage());
        assertEquals("zzt4c_ck_ck", m.getConstraint());
        assertEquals(List.of("1"), rows("SELECT a FROM zzt4c_ck ORDER BY 1"));
        exec("DROP TABLE IF EXISTS zzt4c_ck CASCADE");
    }

    // ========================================================== a call carrying a schema

    /** A qualified name is looked for in that schema, and a schema of no one comes first. */
    @Test
    void aQualifiedNameIsResolvedInTheSchemaItNamesAndNowhereElse() throws Exception {
        exec("DROP TABLE IF EXISTS zzt4c_q1 CASCADE");
        exec("DROP SCHEMA IF EXISTS zzt4c_s CASCADE");
        exec("CREATE SCHEMA zzt4c_s");
        exec("CREATE SCHEMA \"zzt4c_Mix\"");

        noSuchFunction("public.nosuchfunc(integer)",
                "CREATE TABLE zzt4c_q1 (a int, CHECK (public.nosuchfunc(a) > 0))");
        // The quotes belong to the writer, not to the message.
        noSuchFunction("zzt4c_Mix.nosuchfunc(integer)",
                "CREATE TABLE zzt4c_q1 (a int, CHECK (\"zzt4c_Mix\".nosuchfunc(a) > 0))");
        noSuchFunction("zzt4c_s.nosuchfunc(integer)",
                "CREATE TABLE zzt4c_q1 (a int, CHECK (zzt4c_s.nosuchfunc(a) > 0))");
        noSuchFunction("information_schema.nosuchfunc(integer)",
                "CREATE TABLE zzt4c_q1 (a int, CHECK (information_schema.nosuchfunc(a) > 0))");
        // A schema nothing answers to is reported before the function is looked for at all.
        refused("CREATE TABLE zzt4c_q1 (a int, CHECK (zzt4c_nosuch.nosuchfunc(a) > 0))",
                "3F000", "schema \"zzt4c_nosuch\" does not exist", null, null);
        refused("SELECT zzt4c_nosuch.nosuchfunc(1)",
                "3F000", "schema \"zzt4c_nosuch\" does not exist", null, null);
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname = 'zzt4c_q1'"));

        exec("DROP SCHEMA IF EXISTS \"zzt4c_Mix\" CASCADE");
        exec("DROP SCHEMA IF EXISTS zzt4c_s CASCADE");
    }

    /** The built-ins are pg_catalog's, so the same name qualified with public is nobody's. */
    @Test
    void aBuiltinIsPgCatalogsAndPublicDoesNotHoldIt() throws Exception {
        exec("DROP TABLE IF EXISTS zzt4c_q2 CASCADE");
        noSuchFunction("public.lower(text)",
                "CREATE TABLE zzt4c_q2 (a int, CHECK (public.lower(a::text) IS NOT NULL))");
        noSuchFunction("public.date(integer)",
                "CREATE TABLE zzt4c_q2 (a int, CHECK (public.date(a) IS NOT NULL))");
        noSuchFunction("public.int8(integer)",
                "CREATE TABLE zzt4c_q2 (a int, CHECK (public.int8(a) IS NOT NULL))");
        noSuchFunction("public.abs(integer)",
                "CREATE TABLE zzt4c_q2 (a int, CHECK (public.abs(a) > 0))");
        noSuchFunction("public.count(integer)",
                "CREATE TABLE zzt4c_q2 (a int, CHECK (public.count(a) > 0))");
        noSuchFunction("public.date(integer)", "SELECT public.date(1)");
        noSuchFunction("public.int8(integer)", "SELECT public.int8(1)");
        noSuchFunction("public.abs(integer)", "SELECT public.abs(1)");
        noSuchFunction("public.lower(text)", "SELECT public.lower('A'::text)");
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname = 'zzt4c_q2'"));

        // Under pg_catalog the same two calls are the coercion and the built-in.
        assertEquals("1|2", rows("SELECT pg_catalog.int8(1) AS a, pg_catalog.abs(-2) AS b").get(0));
    }

    /** What the user declared without naming a schema is public's, in every spelling of public. */
    @Test
    void aRoutineDeclaredWithoutASchemaIsFoundUnderPublic() throws Exception {
        exec("DROP TABLE IF EXISTS zzt4c_q3, zzt4c_q4, zzt4c_q5, zzt4c_q6 CASCADE");
        exec("DROP SCHEMA IF EXISTS zzt4c_s CASCADE");
        exec("DROP DOMAIN IF EXISTS zzt4c_dom CASCADE");
        exec("DROP FUNCTION IF EXISTS zzt4c_fn(integer)");
        exec("CREATE SCHEMA zzt4c_s");
        exec("CREATE DOMAIN zzt4c_dom AS integer");
        exec("CREATE FUNCTION zzt4c_fn(integer) RETURNS integer AS 'SELECT $1'"
                + " LANGUAGE sql IMMUTABLE");
        exec("CREATE FUNCTION zzt4c_s.zzt4c_sfn(integer) RETURNS integer AS 'SELECT $1'"
                + " LANGUAGE sql IMMUTABLE");

        runs("CREATE TABLE zzt4c_q3 (a int, CHECK (public.zzt4c_fn(a) > 0))");
        runs("CREATE TABLE zzt4c_q4 (a int, CHECK (PUBLIC.zzt4c_fn(a) > 0))");
        runs("CREATE TABLE zzt4c_q5 (a int, CHECK (\"public\".zzt4c_fn(a) > 0))");
        runs("CREATE TABLE zzt4c_q6 (a int, CHECK (zzt4c_s.zzt4c_sfn(a) > 0))");
        assertEquals("1|1|1", rows("SELECT public.zzt4c_fn(1) AS a, zzt4c_s.zzt4c_sfn(1) AS b,"
                + " public.zzt4c_dom(1) AS c").get(0));

        exec("DROP TABLE IF EXISTS zzt4c_q3, zzt4c_q4, zzt4c_q5, zzt4c_q6 CASCADE");
        exec("DROP FUNCTION IF EXISTS zzt4c_fn(integer)");
        exec("DROP DOMAIN IF EXISTS zzt4c_dom CASCADE");
        exec("DROP SCHEMA IF EXISTS zzt4c_s CASCADE");
    }

    // ================================================ a built-in and its argument types

    /**
     * anyrange and anymultirange stand for a kind of type, not for any type, so lower is the
     * string function unless its argument really is a range.
     */
    @Test
    void aBuiltinRefusesAnArgumentNoSignatureOfItsCanStandFor() {
        noSuchFunction("lower(boolean)", "SELECT lower(true)");
        noSuchFunction("lower(integer)", "SELECT lower(1)");
        // The type named is the one written, not the one it would widen to.
        noSuchFunction("lower(numeric)", "SELECT lower(1.5)");
        noSuchFunction("lower(timestamp with time zone)", "SELECT lower(now())");
        noSuchFunction("upper(boolean)", "SELECT upper(true)");
        noSuchFunction("upper(integer)", "SELECT upper(1)");
        noSuchFunction("upper(numeric)", "SELECT upper(1.5)");

        noSuchFunction("isempty(integer)", "SELECT isempty(1)");
        noSuchFunction("isempty(text)", "SELECT isempty('abc'::text)");
        noSuchFunction("lower_inc(integer)", "SELECT lower_inc(1)");
        noSuchFunction("upper_inf(integer)", "SELECT upper_inf(1)");
        noSuchFunction("range_merge(integer, integer)", "SELECT range_merge(1, 2)");
        noSuchFunction("multirange(integer)", "SELECT multirange(1)");

        noSuchFunction("cardinality(integer)", "SELECT cardinality(1)");
        noSuchFunction("cardinality(text)", "SELECT cardinality('abc'::text)");
        noSuchFunction("array_length(integer, integer)", "SELECT array_length(1, 1)");
        noSuchFunction("array_ndims(integer)", "SELECT array_ndims(1)");
        noSuchFunction("array_dims(integer)", "SELECT array_dims(1)");
        noSuchFunction("array_position(integer, integer)", "SELECT array_position(1, 1)");
        noSuchFunction("array_append(integer, integer)", "SELECT array_append(1, 1)");
        noSuchFunction("array_reverse(integer)", "SELECT array_reverse(1)");
        noSuchFunction("trim_array(integer, integer)", "SELECT trim_array(1, 1)");
        noSuchFunction("generate_subscripts(integer, integer)", "SELECT generate_subscripts(1, 1)");
        noSuchFunction("width_bucket(integer, integer)", "SELECT width_bucket(1, 2)");
    }

    /** And every call whose argument is what the signature stands for still answers. */
    @Test
    void thePolymorphicSignaturesStillTakeWhatTheyStandFor() throws Exception {
        assertEquals("abc|ABC",
                rows("SELECT lower('ABC'::text) AS a, upper('abc'::text) AS b").get(0));
        assertEquals("1|5",
                rows("SELECT lower(int4range(1, 5)) AS a, upper(int4range(1, 5)) AS b").get(0));
        assertEquals("1.5|5.5",
                rows("SELECT lower(numrange(1.5, 5.5)) AS a, upper(numrange(1.5, 5.5)) AS b")
                        .get(0));
        assertEquals("t|t|f", rows("SELECT isempty(int4range(1, 1)) AS a,"
                + " lower_inc(int4range(1, 5)) AS b, upper_inf(int4range(1, 5)) AS c").get(0));
        assertEquals("[1,9)|{[1,5)}",
                rows("SELECT range_merge(int4range(1, 5), int4range(7, 9))::text AS a,"
                        + " multirange(int4range(1, 5))::text AS b").get(0));
        assertEquals("1|t", rows("SELECT lower(int4multirange(int4range(1, 5))) AS a,"
                + " isempty(int4multirange()) AS b").get(0));
        assertEquals("3|3|1", rows("SELECT cardinality(ARRAY[1, 2, 3]) AS a,"
                + " array_length(ARRAY[1, 2, 3], 1) AS b, array_ndims(ARRAY[1, 2, 3]) AS c")
                .get(0));
        assertEquals("[1:3]|2", rows("SELECT array_dims(ARRAY[1, 2, 3]) AS a,"
                + " array_position(ARRAY[1, 2, 3], 2) AS b").get(0));
        assertEquals("{1,2,3}|{1,2}", rows("SELECT array_append(ARRAY[1, 2], 3)::text AS a,"
                + " trim_array(ARRAY[1, 2, 3], 1)::text AS b").get(0));
        assertEquals(List.of("1", "2"), rows("SELECT generate_subscripts(ARRAY[1, 2], 1) AS a"));
        assertEquals("2", scalar("SELECT width_bucket(5, ARRAY[1, 4, 9])"));
    }

    // ================================================= what a rule's qualification may name

    /** The ruled relation's own name in a qualification, with the alias old offered for it. */
    @Test
    void aQualificationNamingTheRuledRelationOffersTheOldAlias() throws Exception {
        freshRuleRelations();
        namesTheRuledRelation("zzt4c_t",
                "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t WHERE zzt4c_t.i <> 0 DO ALSO NOTHING");
        namesTheRuledRelation("zzt4c_t",
                "CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_t WHERE zzt4c_t.i <> 0 DO ALSO NOTHING");
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE zzt4c_t.i <> 0 DO INSTEAD NOTHING");
        // However the name was written, and whatever column it carries.
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE public.zzt4c_t.i <> 0 DO ALSO NOTHING");
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE \"zzt4c_t\".i <> 0 DO ALSO NOTHING");
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE zzt4c_t.nope <> 0 DO ALSO NOTHING");
        // A view is a relation like any other.
        namesTheRuledRelation("zzt4c_v", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_v"
                + " WHERE zzt4c_v.total <> 0 DO INSTEAD NOTHING");
        namesTheRuledRelation("zzt4c_v", "CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_v"
                + " WHERE zzt4c_v.total <> 0 DO INSTEAD NOTHING");
        assertEquals("0", scalar("SELECT count(*) FROM pg_rules WHERE rulename = 'zzt4c_r'"));
        dropRuleRelations();
    }

    /**
     * An INSERT rule has an OLD entry but does not put it in scope, so there is no alias to offer:
     * the same 42P01 carries a DETAIL saying the entry cannot be reached from here, and no hint.
     */
    @Test
    void onInsertTheSameQualificationCarriesADetailInsteadOfTheAlias() throws Exception {
        freshRuleRelations();
        refused("CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_t WHERE zzt4c_t.i <> 0 DO ALSO NOTHING",
                "42P01", "invalid reference to FROM-clause entry for table \"zzt4c_t\"",
                OLD_ENTRY_DETAIL, null);
        refused("CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_v WHERE zzt4c_v.total <> 0"
                        + " DO INSTEAD NOTHING",
                "42P01", "invalid reference to FROM-clause entry for table \"zzt4c_v\"",
                OLD_ENTRY_DETAIL, null);
        dropRuleRelations();
    }

    /** A relation in no range table of the rule is missing, and nothing can be suggested. */
    @Test
    void aQualificationNamingAnotherRelationIsMissingRatherThanOutOfReach() throws Exception {
        freshRuleRelations();
        exec("DROP SCHEMA IF EXISTS zzt4c_s2 CASCADE");
        exec("CREATE SCHEMA zzt4c_s2");
        exec("CREATE TABLE zzt4c_s2.zzt4c_far (i int)");

        namesAMissingRelation("zzt4c_o", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE zzt4c_o.i <> 0 DO ALSO NOTHING");
        namesAMissingRelation("zzt4c_o", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE public.zzt4c_o.i <> 0 DO ALSO NOTHING");
        // The schema written does not hold the relation, so the name resolves to nothing.
        namesAMissingRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE zzt4c_s2.zzt4c_t.i <> 0 DO ALSO NOTHING");
        namesAMissingRelation("zzt4c_o", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE zzt4c_nosuchschema.zzt4c_o.i <> 0 DO ALSO NOTHING");
        // The rule is on a relation the search path does not reach, so its bare name is missing.
        namesAMissingRelation("zzt4c_far", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_s2.zzt4c_far"
                + " WHERE zzt4c_far.i <> 0 DO ALSO NOTHING");
        // Written with the schema it really has, it is the ruled relation again.
        namesTheRuledRelation("zzt4c_far", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_s2.zzt4c_far"
                + " WHERE zzt4c_s2.zzt4c_far.i <> 0 DO ALSO NOTHING");

        exec("DROP SCHEMA IF EXISTS zzt4c_s2 CASCADE");
        dropRuleRelations();
    }

    /** old on an INSERT rule and new on a DELETE rule: the entry is there, the scope is not. */
    @Test
    void oldOnInsertAndNewOnDeleteAreOutOfScopeInAQualification() throws Exception {
        freshRuleRelations();
        refused("CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_t WHERE old.i <> 0 DO ALSO NOTHING",
                "42P01", "invalid reference to FROM-clause entry for table \"old\"",
                OLD_ENTRY_DETAIL, null);
        refused("CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_t WHERE new.i <> 0 DO ALSO NOTHING",
                "42P01", "invalid reference to FROM-clause entry for table \"new\"",
                NEW_ENTRY_DETAIL, null);
        refused("CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_v WHERE old.total <> 0"
                        + " DO INSTEAD NOTHING",
                "42P01", "invalid reference to FROM-clause entry for table \"old\"",
                OLD_ENTRY_DETAIL, null);
        refused("CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_v WHERE new.total <> 0"
                        + " DO INSTEAD NOTHING",
                "42P01", "invalid reference to FROM-clause entry for table \"new\"",
                NEW_ENTRY_DETAIL, null);

        // In an action the same reference is refused for the rule's event, not for its scope.
        refused("CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_t"
                        + " DO ALSO INSERT INTO zzt4c_o VALUES (old.i, 'x')",
                "42P17", "ON INSERT rule cannot use OLD", null, null);
        refused("CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_t"
                        + " DO ALSO INSERT INTO zzt4c_o VALUES (new.i, 'x')",
                "42P17", "ON DELETE rule cannot use NEW", null, null);

        // The same rules with the row their event does hold are stored and fire.
        runs("CREATE RULE zzt4c_ok1 AS ON INSERT TO zzt4c_t"
                + " DO ALSO INSERT INTO zzt4c_o VALUES (new.i, 'in')");
        runs("CREATE RULE zzt4c_ok2 AS ON DELETE TO zzt4c_t"
                + " DO ALSO INSERT INTO zzt4c_o VALUES (old.i, 'out')");
        exec("INSERT INTO zzt4c_t VALUES (1, 'one')");
        exec("DELETE FROM zzt4c_t");
        assertEquals(List.of("1|in", "1|out"), rows("SELECT i, j FROM zzt4c_o ORDER BY j"));
        dropRuleRelations();
    }

    // ====================================================== what a rule's action may name

    /**
     * An action has both rows in scope, so the alias old is offered for the relation's own name
     * there even on an INSERT rule -- where the same name in the qualification gets a DETAIL.
     */
    @Test
    void anActionNamingTheRuledRelationOffersTheOldAliasOnEveryEvent() throws Exception {
        freshRuleRelations();
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " DO ALSO INSERT INTO zzt4c_o VALUES (zzt4c_t.i, 'x')");
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_t"
                + " DO ALSO INSERT INTO zzt4c_o VALUES (zzt4c_t.i, 'x')");
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_t"
                + " DO ALSO INSERT INTO zzt4c_o VALUES (zzt4c_t.i, 'x')");
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " DO INSTEAD INSERT INTO zzt4c_o VALUES (zzt4c_t.i, 'x')");
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " DO ALSO INSERT INTO zzt4c_o VALUES (public.zzt4c_t.i, 'x')");
        // Wherever in the action the name stands.
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " DO ALSO UPDATE zzt4c_o SET j = 'x' WHERE zzt4c_t.i = 1");
        namesTheRuledRelation("zzt4c_t",
                "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t DO ALSO SELECT zzt4c_t.i");
        namesTheRuledRelation("zzt4c_v", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_v"
                + " DO INSTEAD INSERT INTO zzt4c_o VALUES (zzt4c_v.total, 'x')");
        // A qualification beside an action that names nothing is judged all the same.
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE zzt4c_t.i <> 0 DO ALSO NOTIFY zzt4c_chan");
        assertEquals("0", scalar("SELECT count(*) FROM pg_rules WHERE rulename = 'zzt4c_r'"));
        dropRuleRelations();
    }

    /** An action that gives the relation a FROM of its own resolves the name there -- unaliased. */
    @Test
    void anActionReadsTheRuledRelationThroughAFromOfItsOwn() throws Exception {
        freshRuleRelations();
        runs("CREATE RULE zzt4c_ok3 AS ON UPDATE TO zzt4c_t"
                + " DO ALSO INSERT INTO zzt4c_o SELECT zzt4c_t.i, 'x' FROM zzt4c_t");
        // A FROM that renames it puts the written name out of reach again.
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " DO ALSO INSERT INTO zzt4c_o SELECT zzt4c_t.i, 'x' FROM zzt4c_t q");
        exec("INSERT INTO zzt4c_t VALUES (1, 'one')");
        exec("UPDATE zzt4c_t SET j = 'two' WHERE i = 1");
        assertEquals(List.of("1|x"), rows("SELECT i, j FROM zzt4c_o ORDER BY j"));
        dropRuleRelations();
    }

    /**
     * A rule of more than one action is judged by each of them: the fault in the second action is
     * the fault of the rule. Such a rule holds a semicolon of its own, so it can only be written
     * here.
     */
    @Test
    void aRuleOfSeveralActionsIsJudgedByEachActionItHolds() throws Exception {
        freshRuleRelations();
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t DO ALSO"
                + " (INSERT INTO zzt4c_o VALUES (old.i, 'a');"
                + " INSERT INTO zzt4c_o VALUES (zzt4c_t.i, 'b'))");
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_t DO ALSO"
                + " (INSERT INTO zzt4c_o VALUES (new.i, 'a');"
                + " INSERT INTO zzt4c_o VALUES (zzt4c_t.i, 'b'))");
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t DO ALSO"
                + " (NOTIFY zzt4c_chan; INSERT INTO zzt4c_o VALUES (zzt4c_t.i, 'x'))");
        // The qualification is read before the actions, and it is read once for all of them.
        namesTheRuledRelation("zzt4c_t", "CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_t"
                + " WHERE zzt4c_t.i <> 0 DO ALSO (NOTIFY zzt4c_chan; NOTIFY zzt4c_chan)");
        // Each action is held to the rows its event has, too.
        refused("CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_t DO ALSO"
                        + " (INSERT INTO zzt4c_o VALUES (old.i, 'g');"
                        + " INSERT INTO zzt4c_o VALUES (new.i, 'h'))",
                "42P17", "ON DELETE rule cannot use NEW", null, null);
        assertEquals("0", scalar("SELECT count(*) FROM pg_rules WHERE rulename = 'zzt4c_r'"));

        // And a rule whose every action names its rows properly runs all of them, in order.
        runs("CREATE RULE zzt4c_ok4 AS ON UPDATE TO zzt4c_t DO ALSO"
                + " (INSERT INTO zzt4c_o VALUES (old.i, 'a');"
                + " INSERT INTO zzt4c_o VALUES (new.i, 'b'))");
        exec("INSERT INTO zzt4c_t VALUES (1, 'one')");
        exec("UPDATE zzt4c_t SET i = 2 WHERE i = 1");
        assertEquals(List.of("1|a", "2|b"), rows("SELECT i, j FROM zzt4c_o ORDER BY j"));
        assertEquals("1", scalar("SELECT count(*) FROM pg_rules WHERE tablename = 'zzt4c_t'"));
        dropRuleRelations();
    }

    // ================================================== old and new in a rule on a view

    /** On a view old and new are the view's own columns, and a near miss is offered as a hint. */
    @Test
    void aViewRuleReadsOldAndNewAsTheViewsOwnColumns() throws Exception {
        exec("DROP VIEW IF EXISTS zzt4c_vv CASCADE");
        exec("DROP TABLE IF EXISTS zzt4c_vbase, zzt4c_vo CASCADE");
        exec("CREATE TABLE zzt4c_vbase (amount int, note text)");
        exec("CREATE TABLE zzt4c_vo (i int, j text)");
        exec("CREATE VIEW zzt4c_vv AS SELECT amount AS total, note AS caption FROM zzt4c_vbase");

        // A column the table under the view holds, which the view does not.
        refused("CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_vv WHERE old.amount <> 0"
                        + " DO INSTEAD NOTHING",
                "42703", "column old.amount does not exist", null, null);
        refused("CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_vv WHERE new.amount <> 0"
                        + " DO INSTEAD NOTHING",
                "42703", "column new.amount does not exist", null, null);
        refused("CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_vv WHERE old.note <> ''"
                        + " DO INSTEAD NOTHING",
                "42703", "column old.note does not exist", null, null);
        // A near miss of a column the view does hold is the same error, carrying that column.
        refused("CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_vv WHERE old.totl <> 0"
                        + " DO INSTEAD NOTHING",
                "42703", "column old.totl does not exist", null,
                "Perhaps you meant to reference the column \"old.total\".");
        refused("CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_vv WHERE new.totl <> 0"
                        + " DO INSTEAD NOTHING",
                "42703", "column new.totl does not exist", null,
                "Perhaps you meant to reference the column \"new.total\".");
        // An action is read the same way as a qualification.
        refused("CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_vv"
                        + " DO INSTEAD INSERT INTO zzt4c_vo VALUES (old.amount, 'x')",
                "42703", "column old.amount does not exist", null, null);
        refused("CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_vv"
                        + " DO INSTEAD INSERT INTO zzt4c_vo VALUES (new.totl, 'x')",
                "42703", "column new.totl does not exist", null,
                "Perhaps you meant to reference the column \"new.total\".");
        assertEquals("0", scalar("SELECT count(*) FROM pg_rules WHERE rulename = 'zzt4c_r'"));

        // The view's own columns resolve, and the action writes what the view's rows hold.
        runs("CREATE RULE zzt4c_vu AS ON UPDATE TO zzt4c_vv"
                + " DO INSTEAD INSERT INTO zzt4c_vo VALUES (new.total, old.caption)");
        exec("INSERT INTO zzt4c_vbase VALUES (3, 'three')");
        exec("UPDATE zzt4c_vv SET total = 5");
        assertEquals(List.of("5|three"), rows("SELECT i, j FROM zzt4c_vo ORDER BY 1"));
        // DO INSTEAD, so the table under the view is untouched.
        assertEquals(List.of("3|three"), rows("SELECT amount, note FROM zzt4c_vbase ORDER BY 1"));

        exec("DROP VIEW IF EXISTS zzt4c_vv CASCADE");
        exec("DROP TABLE IF EXISTS zzt4c_vbase, zzt4c_vo CASCADE");
    }

    /** An unqualified name in a view rule is read against the rows its event puts in scope. */
    @Test
    void anUnqualifiedNameInAViewRuleFollowsTheRowsItsEventHolds() throws Exception {
        exec("DROP VIEW IF EXISTS zzt4c_vv CASCADE");
        exec("DROP TABLE IF EXISTS zzt4c_vbase CASCADE");
        exec("CREATE TABLE zzt4c_vbase (amount int, note text)");
        exec("CREATE VIEW zzt4c_vv AS SELECT amount AS total, note AS caption FROM zzt4c_vbase");

        refused("CREATE RULE zzt4c_r AS ON DELETE TO zzt4c_vv WHERE amount <> 0"
                        + " DO INSTEAD NOTHING",
                "42703", "column \"amount\" does not exist", null, null);
        refused("CREATE RULE zzt4c_r AS ON INSERT TO zzt4c_vv WHERE nope <> 0"
                        + " DO INSTEAD NOTHING",
                "42703", "column \"nope\" does not exist", null, null);
        // On UPDATE both rows are in scope, so a column the view does hold is the ambiguous one.
        refused("CREATE RULE zzt4c_r AS ON UPDATE TO zzt4c_vv WHERE total <> 0"
                        + " DO INSTEAD NOTHING",
                "42702", "column reference \"total\" is ambiguous", null, null);
        // On the two events that hold one row, the same name resolves.
        runs("CREATE RULE zzt4c_vd AS ON DELETE TO zzt4c_vv WHERE total <> 0 DO INSTEAD NOTHING");
        runs("CREATE RULE zzt4c_vi AS ON INSERT TO zzt4c_vv WHERE total <> 0 DO INSTEAD NOTHING");
        runs("CREATE RULE zzt4c_vq AS ON DELETE TO zzt4c_vv WHERE old.total <> 0"
                + " DO INSTEAD NOTHING");
        assertEquals(List.of("zzt4c_vd", "zzt4c_vi", "zzt4c_vq"),
                rows("SELECT rulename FROM pg_rules WHERE tablename = 'zzt4c_vv' ORDER BY 1"));

        exec("DROP VIEW IF EXISTS zzt4c_vv CASCADE");
        exec("DROP TABLE IF EXISTS zzt4c_vbase CASCADE");
    }
}
