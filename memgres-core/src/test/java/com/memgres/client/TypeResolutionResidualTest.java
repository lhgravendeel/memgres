package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Three rules that decide what a value is worth, all measured against PostgreSQL 18.
 *
 * <p><b>A strict function never sees a NULL.</b> Every text-search function PostgreSQL exposes is
 * declared strict, so a NULL argument makes the whole call NULL without the body running. Getting
 * this wrong is not a cosmetic difference: the body either fails on the NULL — which surfaced as
 * {@code XX000}, a fault code no client can act on — or it stringifies it and the query goes on
 * with the four characters {@code null} where a value should be, which is worse because nothing
 * reports it.
 *
 * <p><b>A timestamp has an end.</b> The type holds 4714-11-24 BC to 294276-12-31, narrower at the
 * top than {@code date} because it spends bits on the time of day. Arithmetic landing outside it
 * has no representable answer, and returning one anyway hands back a value no PostgreSQL could
 * store, send, or read back — a number that looks plausible and is not.
 *
 * <p><b>An ALTER that records nothing still has to be refused.</b> Several ALTER forms change
 * nothing memgres keeps, and the temptation is to accept them. PostgreSQL checks the schema, the
 * role, the relation and the option name first, and reporting success for a move that did not
 * happen leaves the next statement failing somewhere unrelated.
 */
class TypeResolutionResidualTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("SET TimeZone = 'UTC'");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(10);
            st.execute(sql);
        }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static List<String> column(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    private static void assertRejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(),
                "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    private static void assertAccepted(String sql) {
        assertDoesNotThrow(() -> exec(sql), sql);
    }

    // ---------------------------------------------------------------- SECTION A
    // A strict function is never entered with a NULL argument.

    @Test
    void everyTextSearchFunctionIsNullInNullOut() throws Exception {
        // One NULL anywhere in the argument list is enough, whichever position it is in.
        assertNull(scalar("SELECT setweight(NULL::tsvector, 'A')"));
        assertNull(scalar("SELECT setweight('a:1'::tsvector, NULL)"));
        assertNull(scalar("SELECT ts_delete(NULL::tsvector, 'a')"));
        assertNull(scalar("SELECT ts_delete('a:1'::tsvector, NULL)"));
        assertNull(scalar("SELECT ts_filter(NULL::tsvector, '{a}')"));
        assertNull(scalar("SELECT tsvector_to_array(NULL::tsvector)"));
        assertNull(scalar("SELECT array_to_tsvector(NULL::text[])"));
        assertNull(scalar("SELECT numnode(NULL::tsquery)"));
        assertNull(scalar("SELECT querytree(NULL::tsquery)"));
        assertNull(scalar("SELECT tsquery_phrase(NULL::tsquery, 'a'::tsquery)"));
        assertNull(scalar("SELECT ts_rewrite(NULL::tsquery, 'a'::tsquery, 'b'::tsquery)"));
        assertNull(scalar("SELECT ts_rank_cd(NULL::tsvector, 'a'::tsquery)"));
        assertNull(scalar("SELECT strip(NULL::tsvector)"));
        assertNull(scalar("SELECT length(NULL::tsvector)"));
        assertNull(scalar("SELECT to_tsvector(NULL::text)"));
        assertNull(scalar("SELECT to_tsquery(NULL::text)"));
        assertNull(scalar("SELECT ts_headline(NULL::text, to_tsquery('a'))"));
        assertNull(scalar("SELECT ts_lexize('simple', NULL)"));
        assertNull(scalar("SELECT NULL::tsvector @@ 'a'::tsquery"));
    }

    /**
     * The quiet direction, and the reason this is worth a test of its own: these three used to
     * render the NULL as the four characters {@code null} and go on to build a query out of it,
     * so the caller got a tsquery matching the word "null" rather than no answer at all.
     */
    @Test
    void aNullIsNotTheWordNull() throws Exception {
        assertNull(scalar("SELECT phraseto_tsquery(NULL::text)"));
        assertNull(scalar("SELECT plainto_tsquery(NULL::text)"));
        assertNull(scalar("SELECT websearch_to_tsquery(NULL::text)"));
        // The same call with a value still works, so strictness did not cost the function.
        assertEquals("'cat' <-> 'sat'", scalar("SELECT phraseto_tsquery('cat sat')::text"));
        assertEquals("'cat' & 'sat'", scalar("SELECT plainto_tsquery('cat sat')::text"));
    }

    /** An argument is evaluated once, so a NULL check cannot cost a sequence a value. */
    @Test
    void aStrictCallEvaluatesItsArgumentsOnce() throws Exception {
        exec("CREATE SEQUENCE tr_seq");
        assertEquals("'1':1", scalar("SELECT to_tsvector(nextval('tr_seq')::text)::text"));
        assertEquals("1", scalar("SELECT currval('tr_seq')::text"));
        exec("DROP SEQUENCE tr_seq");
    }

    // ---------------------------------------------------------------- SECTION B
    // A timestamp that no PostgreSQL could store is refused rather than returned.

    @Test
    void timestampArithmeticStopsAtTheEndOfTheType() {
        assertRejected("22008", "timestamp out of range",
                "SELECT timestamp '294276-12-31 23:59:59' + interval '1 second'");
        assertRejected("22008", "timestamp out of range",
                "SELECT timestamp '294276-12-31 00:00:00' + interval '1 day'");
        assertRejected("22008", "timestamp out of range",
                "SELECT timestamp '294276-01-01' + interval '1 year'");
        assertRejected("22008", "timestamp out of range",
                "SELECT timestamp '2000-01-01' + interval '2147483647 days'");
        assertRejected("22008", "timestamp out of range",
                "SELECT timestamp '2000-01-01' - interval '2147483647 days'");
        // The lower end is a bound too, and it is not the one date stops at.
        assertRejected("22008", "timestamp out of range",
                "SELECT timestamp '4714-11-24 BC' - interval '1 day'");
        assertRejected("22008", "timestamp out of range",
                "SELECT timestamp '4714-11-24 BC' - interval '1 year'");
        // A timestamptz is the same instant seen from an offset, so it has the same end...
        assertRejected("22008", "timestamp out of range",
                "SELECT timestamptz '294276-12-31 23:59:59+00' + interval '1 second'");
        // ...and date + interval is timestamp arithmetic, so it stops where timestamp does rather
        // than where date does.
        assertRejected("22008", "timestamp out of range",
                "SELECT date '2000-01-01' + interval '2147483647 days'");
    }

    /** The values just inside the bound still work: the rule refuses overflow, not arithmetic. */
    @Test
    void arithmeticInsideTheRangeIsUntouched() throws Exception {
        assertEquals("294276-12-31 23:59:59.000001",
                scalar("SELECT (timestamp '294276-12-31 23:59:59' + interval '1 microsecond')::text"));
        assertEquals("294276-02-01 00:00:00",
                scalar("SELECT (timestamp '294276-01-01' + interval '1 month')::text"));
        assertEquals("2000-01-02 00:00:00",
                scalar("SELECT (timestamp '2000-01-01' + interval '1 day')::text"));
        // date has its own, wider end, and adding days rather than an interval stays on it.
        assertEquals("4714-12-31 BC", scalar("SELECT (date '4713-01-01 BC' - 1)::text"));
        assertEquals("298989 years 1 mon 7 days",
                scalar("SELECT age(timestamp '294276-12-31', timestamp '4714-11-24 BC')::text"));
    }

    /**
     * An infinite factor stretches a finite span to an infinite one, which PostgreSQL has had a
     * representation for since 17. Zero times infinity is the indeterminate case it refuses.
     */
    @Test
    void anIntervalTimesInfinityIsInfinite() throws Exception {
        assertEquals("infinity", scalar("SELECT (interval '1 day' * 'Infinity'::float8)::text"));
        assertEquals("-infinity", scalar("SELECT (interval '1 day' * '-Infinity'::float8)::text"));
        assertEquals("-infinity", scalar("SELECT (interval '-1 day' * 'Infinity'::float8)::text"));
        assertRejected("22008", "interval out of range",
                "SELECT interval '0 days' * 'Infinity'::float8");
        assertRejected("22008", "interval out of range",
                "SELECT interval '1 day' * 'NaN'::float8");
        // Dividing by infinity narrows to nothing rather than overflowing.
        assertEquals("00:00:00", scalar("SELECT (interval '1 day' / 'Infinity'::float8)::text"));
    }

    // ---------------------------------------------------------------- SECTION C
    // An ALTER that records nothing still checks what it names.

    @Test
    void setSchemaNamesASchemaThatHasToExist() throws Exception {
        exec("CREATE TABLE tr_t (a int, b int)");
        exec("CREATE AGGREGATE tr_ag (int) (SFUNC = int4pl, STYPE = int)");
        exec("CREATE COLLATION tr_co (LOCALE = 'C')");
        exec("CREATE TEXT SEARCH DICTIONARY tr_dict (TEMPLATE = simple)");
        exec("CREATE TEXT SEARCH CONFIGURATION tr_cfg (COPY = simple)");
        exec("CREATE STATISTICS tr_st ON a, b FROM tr_t");

        assertRejected("3F000", "schema \"nosuchschema\" does not exist",
                "ALTER AGGREGATE tr_ag(int) SET SCHEMA nosuchschema");
        assertRejected("3F000", "schema \"nosuchschema\" does not exist",
                "ALTER COLLATION tr_co SET SCHEMA nosuchschema");
        assertRejected("3F000", "schema \"nosuchschema\" does not exist",
                "ALTER TEXT SEARCH DICTIONARY tr_dict SET SCHEMA nosuchschema");
        assertRejected("3F000", "schema \"nosuchschema\" does not exist",
                "ALTER TEXT SEARCH CONFIGURATION tr_cfg SET SCHEMA nosuchschema");
        assertRejected("3F000", "schema \"nosuchschema\" does not exist",
                "ALTER STATISTICS tr_st SET SCHEMA nosuchschema");

        // A schema that is there is accepted, so the check refuses the missing one and no more.
        exec("CREATE SCHEMA tr_s");
        assertAccepted("ALTER AGGREGATE tr_ag(int) SET SCHEMA tr_s");
        assertAccepted("ALTER STATISTICS tr_st SET SCHEMA tr_s");
    }

    @Test
    void ownerToNamesARoleThatHasToExist() throws Exception {
        exec("CREATE TYPE tr_ty AS ENUM ('a')");
        exec("CREATE DOMAIN tr_dom AS int");
        exec("CREATE AGGREGATE tr_ag2 (int) (SFUNC = int4pl, STYPE = int)");
        exec("CREATE COLLATION tr_co2 (LOCALE = 'C')");
        exec("CREATE TEXT SEARCH DICTIONARY tr_dict2 (TEMPLATE = simple)");
        exec("CREATE TEXT SEARCH CONFIGURATION tr_cfg2 (COPY = simple)");

        assertRejected("42704", "role \"nosuchrole\" does not exist",
                "ALTER TYPE tr_ty OWNER TO nosuchrole");
        assertRejected("42704", "role \"nosuchrole\" does not exist",
                "ALTER DOMAIN tr_dom OWNER TO nosuchrole");
        assertRejected("42704", "role \"nosuchrole\" does not exist",
                "ALTER AGGREGATE tr_ag2(int) OWNER TO nosuchrole");
        assertRejected("42704", "role \"nosuchrole\" does not exist",
                "ALTER COLLATION tr_co2 OWNER TO nosuchrole");
        assertRejected("42704", "role \"nosuchrole\" does not exist",
                "ALTER TEXT SEARCH DICTIONARY tr_dict2 OWNER TO nosuchrole");
        assertRejected("42704", "role \"nosuchrole\" does not exist",
                "ALTER TEXT SEARCH CONFIGURATION tr_cfg2 OWNER TO nosuchrole");

        exec("CREATE ROLE tr_role");
        assertAccepted("ALTER TYPE tr_ty OWNER TO tr_role");
        assertAccepted("ALTER DOMAIN tr_dom OWNER TO tr_role");
    }

    @Test
    void anOperatorIsMovedAndReownedLikeAnythingElse() throws Exception {
        exec("CREATE FUNCTION tr_lt(int, int) RETURNS bool AS $$ SELECT $1 < $2 $$"
                + " LANGUAGE sql IMMUTABLE");
        exec("CREATE OPERATOR <^ (LEFTARG = int, RIGHTARG = int, FUNCTION = tr_lt)");
        assertRejected("3F000", "schema \"nosuchschema\" does not exist",
                "ALTER OPERATOR <^ (int, int) SET SCHEMA nosuchschema");
        assertRejected("42704", "role \"nosuchrole\" does not exist",
                "ALTER OPERATOR <^ (int, int) OWNER TO nosuchrole");
        // The operator is still there and still usable: a refused ALTER changed nothing.
        assertEquals("true", scalar("SELECT (1 <^ 2)::text"));
    }

    @Test
    void aPublicationListsRelationsThatHaveToExist() throws Exception {
        exec("CREATE TABLE tr_pa (i int)");
        exec("CREATE TABLE tr_pb (i int)");
        exec("CREATE PUBLICATION tr_pub FOR TABLE tr_pa");

        assertRejected("42P01", "relation \"nosuchtable\" does not exist",
                "ALTER PUBLICATION tr_pub ADD TABLE nosuchtable");
        assertRejected("42P01", "relation \"nosuchtable\" does not exist",
                "ALTER PUBLICATION tr_pub DROP TABLE nosuchtable");
        assertRejected("42P01", "relation \"nosuchtable\" does not exist",
                "ALTER PUBLICATION tr_pub SET TABLE nosuchtable");
        // A relation already listed would be listed twice; one not listed cannot be removed.
        assertRejected("42710", "relation \"tr_pa\" is already member of publication \"tr_pub\"",
                "ALTER PUBLICATION tr_pub ADD TABLE tr_pa");
        assertRejected("42704", "relation \"tr_pb\" is not part of the publication",
                "ALTER PUBLICATION tr_pub DROP TABLE tr_pb");
        assertRejected("42704", "role \"nosuchrole\" does not exist",
                "ALTER PUBLICATION tr_pub OWNER TO nosuchrole");
        assertRejected("42601", "unrecognized publication parameter: \"nosuchoption\"",
                "ALTER PUBLICATION tr_pub SET (nosuchoption = true)");

        // The options it does take are still taken, and the membership is what it was.
        assertAccepted("ALTER PUBLICATION tr_pub SET (publish = 'insert')");
        assertAccepted("ALTER PUBLICATION tr_pub ADD TABLE tr_pb");
        assertEquals(List.of("tr_pa", "tr_pb"), column(
                "SELECT tablename FROM pg_publication_tables"
                        + " WHERE pubname = 'tr_pub' ORDER BY tablename"));
    }

    @Test
    void aPolicyRenameAndItsRolesAreChecked() throws Exception {
        exec("CREATE TABLE tr_rls (a int)");
        exec("CREATE POLICY tr_p1 ON tr_rls USING (a > 0)");
        exec("CREATE POLICY tr_p2 ON tr_rls USING (a > 0)");

        assertRejected("42710", "policy \"tr_p2\" for table \"tr_rls\" already exists",
                "ALTER POLICY tr_p1 ON tr_rls RENAME TO tr_p2");
        assertRejected("42704", "role \"nosuchrole\" does not exist",
                "ALTER POLICY tr_p1 ON tr_rls TO nosuchrole");
        // Both survive the refusals, and the ordinary alteration still works — which is the point:
        // the rename used to succeed, and everything after it then failed on a name that was gone.
        assertEquals(List.of("tr_p1", "tr_p2"), column(
                "SELECT policyname FROM pg_policies WHERE tablename = 'tr_rls' ORDER BY policyname"));
        assertAccepted("ALTER POLICY tr_p1 ON tr_rls USING (a > 1)");
        assertAccepted("ALTER POLICY tr_p1 ON tr_rls TO PUBLIC");
    }

    @Test
    void aFunctionRenameNamesTheSignatureItClashesWith() throws Exception {
        exec("CREATE FUNCTION tr_f(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql");
        exec("CREATE FUNCTION tr_g(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql");
        // Only an overload of the same argument types in the same schema collides, so the message
        // says which one — a bare name would not tell the reader what to change.
        assertRejected("42723", "function tr_g(integer) already exists in schema \"public\"",
                "ALTER FUNCTION tr_f(int) RENAME TO tr_g");
        // A different argument list is a different function and does not collide.
        exec("CREATE FUNCTION tr_g(text) RETURNS text AS $$ SELECT $1 $$ LANGUAGE sql");
        assertAccepted("ALTER FUNCTION tr_f(int) RENAME TO tr_h");
    }

    @Test
    void dependsOnExtensionNamesAnExtensionThatHasToExist() throws Exception {
        exec("CREATE FUNCTION tr_dep(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql");
        assertRejected("42704", "extension \"nosuchext\" does not exist",
                "ALTER FUNCTION tr_dep(int) DEPENDS ON EXTENSION nosuchext");
        assertRejected("42704", "extension \"nosuchext\" does not exist",
                "ALTER FUNCTION tr_dep(int) NO DEPENDS ON EXTENSION nosuchext");
        // An extension that is installed is accepted, and the function is untouched either way.
        assertAccepted("ALTER FUNCTION tr_dep(int) DEPENDS ON EXTENSION plpgsql");
        assertEquals("7", scalar("SELECT tr_dep(7)::text"));
    }

    // ---------------------------------------------------------------- SECTION D
    // What a value is worth at the edges of its type.

    /**
     * PostgreSQL's {@code time} runs to 24:00:00 inclusive — one value past the last instant of
     * the day, meaning the end of it. It is not a clock reading, so it compares above every real
     * time, its hour is 24, and arithmetic on it counts a whole day.
     */
    @Test
    void timeRunsToTheEndOfTheDay() throws Exception {
        assertEquals("24:00:00", scalar("SELECT '24:00:00'::time::text"));
        assertEquals("24:00:00+00", scalar("SELECT '24:00:00'::timetz::text"));
        assertEquals("true", scalar("SELECT ('24:00:00'::time > '23:59:59'::time)::text"));
        assertEquals("24", scalar("SELECT extract(hour from '24:00:00'::time)::text"));
        assertEquals("00:00:01", scalar("SELECT ('24:00:00'::time + interval '1 second')::text"));
        assertEquals("23:59:59", scalar("SELECT ('24:00:00'::time - interval '1 second')::text"));
        assertEquals("24:00:00", scalar("SELECT '24:00:00'::time::interval::text"));
        // It is the only hour-24 time the type takes, and a value is rounded to it rather than
        // truncated away from it.
        assertRejected("22008", "date/time field value out of range", "SELECT '24:00:01'::time");
        assertRejected("22008", "date/time field value out of range",
                "SELECT '24:00:00.000001'::time");
        assertEquals("24:00:00", scalar("SELECT '23:59:59.9999999'::time::text"));
        assertEquals("23:59:59.999999", scalar("SELECT '23:59:59.999999'::time::text"));
    }

    /** Subtracting one time from another gives an interval, and it may point backwards. */
    @Test
    void timeMinusTimeIsAnInterval() throws Exception {
        assertEquals("interval", scalar("SELECT pg_typeof(time '10:00' - time '11:00')::text"));
        assertEquals("-01:00:00", scalar("SELECT (time '10:00' - time '11:00')::text"));
        assertEquals("01:00:00", scalar("SELECT (time '11:00' - time '10:00')::text"));
        assertEquals("00:00:00", scalar("SELECT (time '10:00' - time '10:00')::text"));
    }

    /** A jsonb number is a numeric, so the exponent it was written with is not part of it. */
    @Test
    void jsonbNumbersAreStoredAsNumeric() throws Exception {
        assertEquals("100", scalar("SELECT '1e2'::jsonb::text"));
        assertEquals("100", scalar("SELECT '1E2'::jsonb::text"));
        assertEquals("-100", scalar("SELECT '-1e2'::jsonb::text"));
        assertEquals("1500", scalar("SELECT '1.5e3'::jsonb::text"));
        assertEquals("0.001", scalar("SELECT '1e-3'::jsonb::text"));
        assertEquals("[100]", scalar("SELECT '[1e2]'::jsonb::text"));
        assertEquals("{\"a\": 100}", scalar("SELECT '{\"a\":1e2}'::jsonb::text"));
        // json keeps the text as it was written, which is the whole difference between them.
        assertEquals("1e2", scalar("SELECT '1e2'::json::text"));
    }

    /** A negative jsonb subscript counts back from the end, with or without parentheses. */
    @Test
    void aNegativeJsonbSubscriptCountsFromTheEnd() throws Exception {
        assertEquals("3", scalar("SELECT '[1,2,3]'::jsonb -> -1"));
        assertEquals("3", scalar("SELECT '[1,2,3]'::jsonb -> (-1)"));
        assertEquals("1", scalar("SELECT '[1,2,3]'::jsonb -> -3"));
        assertNull(scalar("SELECT '[1,2,3]'::jsonb -> -4"));
        assertEquals("1", scalar("SELECT '[1,2,3]'::jsonb -> 0"));
        assertNull(scalar("SELECT '[1,2,3]'::jsonb -> 5"));
    }

    /** jsonb_set answers with a jsonb even when the path reached nothing to change. */
    @Test
    void jsonbSetAnswersWithJsonbEitherWay() throws Exception {
        assertEquals("{\"a\": 1}", scalar("SELECT jsonb_set('{\"a\":1}', '{b}', '2', false)::text"));
        assertEquals("{\"a\": 2}", scalar("SELECT jsonb_set('{\"a\":1}', '{a}', '2', false)::text"));
        assertEquals("{\"a\": 1, \"b\": 2}",
                scalar("SELECT jsonb_set('{\"a\":1}', '{b}', '2', true)::text"));
    }

    /** to_hex is two's complement, and how wide the argument is decides the answer. */
    @Test
    void toHexIsAsWideAsItsArgument() throws Exception {
        assertEquals("ffffffff", scalar("SELECT to_hex((-1)::int4)"));
        assertEquals("ffffffffffffffff", scalar("SELECT to_hex((-1)::int8)"));
        assertEquals("ff", scalar("SELECT to_hex(255)"));
        assertEquals("ff", scalar("SELECT to_hex(255::bigint)"));
    }

    /** There is no bucket to fall into when none were asked for. */
    @Test
    void widthBucketNeedsAPositiveCount() {
        assertRejected("2201G", "count must be greater than zero",
                "SELECT width_bucket(5.0, 1.0, 10.0, 0)");
        assertRejected("2201G", "count must be greater than zero",
                "SELECT width_bucket(5.0, 1.0, 10.0, -1)");
    }

    /** A clock field out of range is the caller's mistake, not an internal fault. */
    @Test
    void makeTimeReportsAFieldOutOfRange() {
        assertRejected("22008", "time field value out of range", "SELECT make_time(25, 0, 0)");
        assertRejected("22008", "time field value out of range", "SELECT make_time(0, 60, 0)");
        assertRejected("22008", "time field value out of range", "SELECT make_time(-1, 0, 0)");
    }

    /** The attribute forms still parse: NO belongs to the attribute when DEPENDS does not follow. */
    @Test
    void aFunctionAttributeStartingWithNoStillParses() throws Exception {
        exec("CREATE FUNCTION tr_at(int) RETURNS int AS $$ SELECT $1 $$ LANGUAGE sql");
        assertAccepted("ALTER FUNCTION tr_at(int) NOT LEAKPROOF");
        assertAccepted("ALTER FUNCTION tr_at(int) LEAKPROOF");
        assertAccepted("ALTER FUNCTION tr_at(int) IMMUTABLE");
        assertEquals("i", scalar(
                "SELECT provolatile::text FROM pg_proc WHERE proname = 'tr_at'"));
    }
}
