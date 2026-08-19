package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TABLESAMPLE settles what it will read before it reads any of it.
 *
 * <p>A sampled scan decides four things from what was written, in that order and before a single
 * page is touched: which relation, which method, how many arguments the method was given, and what
 * types those arguments are. Only then does it read their values. memgres decided all of it from
 * the values instead — the method was matched in the parser, so it was checked before the relation
 * was; a second argument was a syntax error rather than a question for the method; and both
 * arguments were read as whatever number they happened to produce.
 *
 * <p>Deciding from values rather than from what was written showed up as answers nobody asked for:
 * {@code BERNOULLI (true)} sampled no rows, {@code BERNOULLI ('100'::text)} sampled every one, a
 * seed that was not a whole number raised a Java fault, and a column of the sampled relation
 * written as the percentage was read as though the clause could see the rows it is deciding whether
 * to read.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class TablesampleArgumentResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE s10_a (id int, v text)");
            st.execute("INSERT INTO s10_a VALUES (1, 'one'), (2, 'two'), (3, 'three')");
            st.execute("CREATE VIEW s10_v AS SELECT id FROM s10_a");
            st.execute("CREATE MATERIALIZED VIEW s10_m AS SELECT id FROM s10_a");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** How many rows a sampled scan answered with. */
    private static int sampled(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getInt(1);
        }
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static org.postgresql.util.ServerErrorMessage errorOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    private static String messageOf(String sql) {
        return errorOf(sql).getMessage();
    }

    /** Sampling reads a fraction of a relation's own pages, so there has to be one. */
    @Test
    void aRelationWhosePagesAreItsOwn() throws Exception {
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (100)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100)"));
        assertEquals(0, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (0)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_m TABLESAMPLE BERNOULLI (100)"));
        assertEquals("0A000", stateOf("SELECT count(*) FROM s10_v TABLESAMPLE BERNOULLI (100)"));
        assertEquals("42P01", stateOf("SELECT count(*) FROM s10_no TABLESAMPLE BERNOULLI (100)"));
    }

    /** The relation is settled first, so what follows it is never reached. */
    @Test
    void whatFollowsTheRelationIsNotReachedUntilTheRelationIsSettled() {
        assertEquals("0A000", stateOf("SELECT count(*) FROM s10_v TABLESAMPLE nosuch (100)"));
        assertEquals("0A000", stateOf("SELECT count(*) FROM s10_v TABLESAMPLE BERNOULLI (50, 50)"));
        assertEquals("0A000", stateOf("SELECT count(*) FROM s10_v TABLESAMPLE BERNOULLI (true)"));
        assertEquals("42P01", stateOf("SELECT count(*) FROM s10_no TABLESAMPLE nosuch (100)"));
        assertEquals("42P01", stateOf("SELECT count(*) FROM s10_no TABLESAMPLE BERNOULLI (50, 50)"));
        assertEquals("42P01", stateOf("SELECT count(*) FROM s10_no TABLESAMPLE BERNOULLI (true)"));
    }

    /** The method is named by an identifier, so quoting it keeps the case it was written in. */
    @Test
    void theMethodIsAnIdentifierQuotedOrNot() throws Exception {
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE bernoulli (100)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE SySTeM (100)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE \"bernoulli\" (100)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE \"system\" (100)"));
        assertEquals("42704", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE \"SYSTEM\" (100)"));
        assertEquals("42704", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE \"no such\" (100)"));
    }

    /** What was written is an identifier the grammar resolved, so it is named back bare. */
    @Test
    void aMethodThatDoesNotExistIsNamedAsItWasWritten() {
        assertEquals("tablesample method nosuch does not exist",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE nosuch (100)"));
        assertEquals("tablesample method nosuch does not exist",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE NOSUCH (100)"));
        assertEquals("tablesample method Bernoulli does not exist",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE \"Bernoulli\" (100)"));
    }

    /** And the method is looked up before anything at all is asked of its arguments. */
    @Test
    void theMethodIsLookedUpBeforeItsArgumentsAre() {
        assertEquals("42704", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE nosuch (100, 100)"));
        assertEquals("42704", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE nosuch (true)"));
        assertEquals("42704", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE nosuch (id)"));
    }

    /** How many arguments a method takes is the method's own business, not the grammar's. */
    @Test
    void howManyArgumentsTheMethodWantedIsTheMethodsOwnBusiness() {
        assertEquals("tablesample method bernoulli requires 1 argument, not 2",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (50, 50)"));
        assertEquals("tablesample method system requires 1 argument, not 3",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100, 100, 100)"));
        assertEquals("2202H", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (50, true)"));
        assertEquals("2202H", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (id, id)"));
        assertEquals("2202H",
                stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (50, 50) REPEATABLE (true)"));
        // No argument at all is a list the grammar cannot read, and that is a syntax error.
        assertEquals("42601", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ()"));
        assertEquals("42601", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM"));
    }

    /** The percentage is a real: every number reaches one, and nothing else does. */
    @Test
    void thePercentageIsAReal() throws Exception {
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (100::smallint)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (100::bigint)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (100::numeric)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (100::float8)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (50 + 50)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ((SELECT 100))"));
        assertEquals("argument of TABLESAMPLE must be type real, not type boolean",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (true)"));
        assertEquals("argument of TABLESAMPLE must be type real, not type interval",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('1 day'::interval)"));
        assertEquals("42804", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('100'::text)"));
        assertEquals("42804", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('x'::char)"));
        assertEquals("42804", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('{1}'::int[])"));
    }

    /** The seed answers the same question about double precision. */
    @Test
    void theSeedIsADoublePrecision() throws Exception {
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE (5)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE (-5)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE (1e300)"));
        assertEquals(3,
                sampled("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE (100::numeric)"));
        assertEquals("argument of REPEATABLE must be type double precision, not type text",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE ('100'::text)"));
        assertEquals("42804",
                stateOf("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE (true)"));
        assertEquals("42804",
                stateOf("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE ('1 day'::interval)"));
    }

    /** A seed that is not a whole number is still a seed, because it is a double precision. */
    @Test
    void aSeedThatIsNotAWholeNumberIsStillASeed() throws Exception {
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE (0.0)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE (0.5)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (100) REPEATABLE (1.5)"));
    }

    /** Both types are settled before either value is read. */
    @Test
    void bothTypesAreSettledBeforeEitherValueIsRead() {
        assertEquals("42804",
                stateOf("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (NULL) REPEATABLE (true)"));
        assertEquals("42804",
                stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (true) REPEATABLE (true)"));
    }

    /** A value with no type of its own is read by the input function of the type that was wanted. */
    @Test
    void aValueWithNoTypeOfItsOwnIsReadByTheTypeThatWasWanted() throws Exception {
        assertEquals(3, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('100')"));
        assertEquals(0, sampled("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('0')"));
        assertEquals("invalid input syntax for type real: \"abc\"",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('abc')"));
        assertEquals("invalid input syntax for type double precision: \"xyz\"",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE SYSTEM (100) REPEATABLE ('xyz')"));
        // Read as a real, so a magnitude no real reaches is out of range for one.
        assertEquals("22003", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('1e400')"));
    }

    /** Then the values, and only then. */
    @Test
    void thenTheValuesAndOnlyThen() {
        assertEquals("TABLESAMPLE parameter cannot be null",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (NULL)"));
        assertEquals("2202H", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (NULL::float8)"));
        assertEquals("TABLESAMPLE REPEATABLE parameter cannot be null",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (100) REPEATABLE (NULL)"));
        // The percentage is read first, so it is the one that is missing.
        assertEquals("2202H",
                stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (NULL) REPEATABLE (NULL)"));
        // And a percentage that is there is refused before the seed that is not.
        assertEquals("2202G",
                stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (-1) REPEATABLE (NULL)"));
    }

    /** A percentage is a fraction of the whole, so nothing outside 0..100 names one. */
    @Test
    void aPercentageNamesAFractionOfTheWholeOrNoneAtAll() {
        assertEquals("sample percentage must be between 0 and 100",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (-1)"));
        assertEquals("sample percentage must be between 0 and 100",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (101)"));
        // A NaN is neither inside the range nor outside it, and PG says the same words.
        assertEquals("sample percentage must be between 0 and 100",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('nan'::float8)"));
        assertEquals("sample percentage must be between 0 and 100",
                messageOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI ('infinity'::float8)"));
    }

    /** The arguments sit outside the relation the clause is attached to. */
    @Test
    void theArgumentsSitOutsideTheRelationTheClauseIsAttachedTo() {
        org.postgresql.util.ServerErrorMessage bare =
                errorOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (id)");
        assertEquals("column \"id\" does not exist", bare.getMessage());
        assertEquals("There is a column named \"id\" in table \"s10_a\", but it cannot be referenced"
                + " from this part of the query.", bare.getDetail());

        // The alias is what the relation is called here, so that is the name in the detail.
        org.postgresql.util.ServerErrorMessage aliased =
                errorOf("SELECT count(*) FROM s10_a t TABLESAMPLE BERNOULLI (id)");
        assertEquals("There is a column named \"id\" in table \"t\", but it cannot be referenced"
                + " from this part of the query.", aliased.getDetail());

        // Qualifying it asks for the FROM-clause entry itself, which is the other complaint.
        org.postgresql.util.ServerErrorMessage qualified =
                errorOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (s10_a.id)");
        assertEquals("invalid reference to FROM-clause entry for table \"s10_a\"",
                qualified.getMessage());
        assertEquals("42P01", qualified.getSQLState());

        assertEquals("42703", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (v)"));
        assertEquals("42703",
                stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (100) REPEATABLE (id)"));
        // A name the relation does not have at all is unknown for the ordinary reason.
        assertEquals("42703", stateOf("SELECT count(*) FROM s10_a TABLESAMPLE BERNOULLI (nocol)"));
    }

    /** The clause belongs to a relation, not to whatever else produced rows. */
    @Test
    void theClauseBelongsToARelationNotToWhateverProducedRows() throws Exception {
        assertEquals("42601",
                stateOf("SELECT count(*) FROM (SELECT id FROM s10_a) s TABLESAMPLE BERNOULLI (100)"));
        assertEquals("42601",
                stateOf("SELECT count(*) FROM generate_series(1, 10) g TABLESAMPLE BERNOULLI (100)"));
        assertEquals(3, sampled("SELECT count(*) FROM s10_a AS t TABLESAMPLE BERNOULLI (100)"));
        assertEquals(3,
                sampled("SELECT count(*) FROM s10_a t TABLESAMPLE BERNOULLI (100) REPEATABLE (1)"));
    }
}
