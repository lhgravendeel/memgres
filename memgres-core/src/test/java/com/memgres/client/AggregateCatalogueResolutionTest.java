package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An aggregate is a function the catalogue holds.
 *
 * <p>It is declared however the grammar allows it to be, found by its name and the types it was
 * written with, and refused wherever no function of that shape may stand. memgres treated the
 * ordered-set aggregates as a matter of grammar — a call without WITHIN GROUP was a syntax error,
 * and a fraction or a sort column of the wrong type was accepted and evaluated — and it let a
 * transform expression hold a sub-query, an aggregate or a window call, so ALTER ... USING count(*)
 * rewrote the table with a count in it.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class AggregateCatalogueResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE zz_ac0 (i int, t text, d date, iv interval)");
            st.execute("INSERT INTO zz_ac0 VALUES (1, 'x', '2020-01-01', '1 hour')");
            st.execute("INSERT INTO zz_ac0 VALUES (3, 'y', '2020-01-03', '3 hours')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static PSQLException refusalOf(String sql) {
        return assertThrows(PSQLException.class, () -> {
            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        }, "expected a refusal from: " + sql);
    }

    private static String stateOf(String sql) {
        return refusalOf(sql).getSQLState();
    }

    private static String messageOf(String sql) {
        return refusalOf(sql).getServerErrorMessage().getMessage();
    }

    private static void run(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static String one(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    @Test
    void aCallOfTheRightShapeIsOnlyMissingItsClause() {
        assertEquals("42809", stateOf("SELECT percentile_cont(0.5, 1) FROM zz_ac0"));
        assertEquals("WITHIN GROUP is required for ordered-set aggregate percentile_cont",
                messageOf("SELECT percentile_cont(0.5, 1) FROM zz_ac0"));
        assertEquals("WITHIN GROUP is required for ordered-set aggregate percentile_disc",
                messageOf("SELECT percentile_disc(0.5, 1) FROM zz_ac0"));
        assertEquals("WITHIN GROUP is required for ordered-set aggregate mode",
                messageOf("SELECT mode(i) FROM zz_ac0"));
    }

    @Test
    void aCallOfAnyOtherShapeIsNoFunctionAtAll() {
        assertEquals("42883", stateOf("SELECT percentile_cont(0.5) FROM zz_ac0"));
        assertEquals("function percentile_cont(numeric) does not exist",
                messageOf("SELECT percentile_cont(0.5) FROM zz_ac0"));
        assertEquals("function mode() does not exist", messageOf("SELECT mode() FROM zz_ac0"));
        assertEquals("function mode(integer, integer) does not exist",
                messageOf("SELECT mode(1, 2) FROM zz_ac0"));
    }

    @Test
    void anOverClauseIsReadAfterTheCallHasBeenResolved() {
        assertEquals("WITHIN GROUP is required for ordered-set aggregate mode",
                messageOf("SELECT mode(1) OVER () FROM zz_ac0"));
    }

    @Test
    void theHypotheticalSetAggregatesAreToldApartByHavingArgumentsAtAll() {
        assertEquals("window function rank requires an OVER clause",
                messageOf("SELECT rank() FROM zz_ac0"));
        assertEquals("WITHIN GROUP is required for ordered-set aggregate rank",
                messageOf("SELECT rank(1) FROM zz_ac0"));
        assertEquals("WITHIN GROUP is required for ordered-set aggregate cume_dist",
                messageOf("SELECT cume_dist(1) FROM zz_ac0"));
    }

    @Test
    void percentileContIsDeclaredOnlyOverWhatItCanInterpolate() throws Exception {
        assertEquals("2", one("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i) FROM zz_ac0"));
        assertEquals("function percentile_cont(numeric, date) does not exist",
                messageOf("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY d) FROM zz_ac0"));
        // percentile_disc picks a value out rather than making one, so any type at all will do.
        assertEquals("2020-01-01",
                one("SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY d) FROM zz_ac0"));
    }

    @Test
    void aFractionIsADoublePrecisionAndAColumnOfAnotherTypeIsNotOne() {
        assertEquals("function percentile_cont(text, integer) does not exist",
                messageOf("SELECT percentile_cont(t) WITHIN GROUP (ORDER BY i) FROM zz_ac0"));
        assertEquals("function percentile_disc(text, integer) does not exist",
                messageOf("SELECT percentile_disc(t) WITHIN GROUP (ORDER BY i) FROM zz_ac0"));
        assertEquals("function percentile_cont(text[], integer) does not exist",
                messageOf("SELECT percentile_cont(ARRAY['a']) WITHIN GROUP (ORDER BY i)"
                        + " FROM zz_ac0"));
    }

    @Test
    void theCallIsSettledBeforeAPageIsRead() {
        assertEquals("function percentile_cont(numeric, date) does not exist",
                messageOf("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY d)"
                        + " FROM zz_ac0 WHERE false"));
    }

    @Test
    void aFractionOutOfRangeIsNamedToSixSignificantDigits() {
        assertEquals("22003", stateOf(
                "SELECT percentile_disc(1.5) WITHIN GROUP (ORDER BY i) FROM zz_ac0"));
        assertEquals("percentile value 1.5 is not between 0 and 1", messageOf(
                "SELECT percentile_disc(1.5) WITHIN GROUP (ORDER BY i) FROM zz_ac0"));
        assertEquals("percentile value 2 is not between 0 and 1", messageOf(
                "SELECT percentile_disc(2.0) WITHIN GROUP (ORDER BY i) FROM zz_ac0"));
        assertEquals("percentile value 1e+10 is not between 0 and 1", messageOf(
                "SELECT percentile_disc(1e10) WITHIN GROUP (ORDER BY i) FROM zz_ac0"));
        assertEquals("percentile value 1.23457 is not between 0 and 1", messageOf(
                "SELECT percentile_disc(1.234567891) WITHIN GROUP (ORDER BY i) FROM zz_ac0"));
    }

    @Test
    void theFractionIsReadWhetherOrNotAnythingWasAccumulated() {
        // It is a direct argument, taken once for the whole group rather than per row, so an
        // empty group is still handed one and still refuses it.
        assertEquals("percentile value 1.5 is not between 0 and 1", messageOf(
                "SELECT percentile_disc(1.5) WITHIN GROUP (ORDER BY i) FROM zz_ac0 WHERE false"));
    }

    @Test
    void anIntervalIsInterpolatedAsAnInterval() throws Exception {
        assertEquals("02:00:00",
                one("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY iv) FROM zz_ac0"));
        assertEquals("02:30:00",
                one("SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY iv) FROM zz_ac0"));
        // The same cascade a fraction of an interval goes through anywhere: 0.9 of a month is
        // twenty-seven days, not twenty-six days and a day's worth of microseconds.
        assertEquals("30 days", one("SELECT interval '3 mons 10 days' * 0.3"));
    }

    @Test
    void aTransformExpressionIsNotAQuery() throws Exception {
        run("DROP TABLE IF EXISTS zz_ac1");
        run("CREATE TABLE zz_ac1 (a int)");
        assertEquals("0A000", stateOf("ALTER TABLE zz_ac1 ALTER COLUMN a TYPE text"
                + " USING (SELECT 1)"));
        assertEquals("cannot use subquery in transform expression",
                messageOf("ALTER TABLE zz_ac1 ALTER COLUMN a TYPE text USING a IN (SELECT 1)"));
        assertEquals("42803", stateOf("ALTER TABLE zz_ac1 ALTER COLUMN a TYPE text"
                + " USING count(*)"));
        assertEquals("aggregate functions are not allowed in transform expressions",
                messageOf("ALTER TABLE zz_ac1 ALTER COLUMN a TYPE text USING sum(a)"));
        assertEquals("42P20", stateOf("ALTER TABLE zz_ac1 ALTER COLUMN a TYPE text"
                + " USING row_number() OVER ()"));
        assertEquals("grouping operations are not allowed in transform expressions",
                messageOf("ALTER TABLE zz_ac1 ALTER COLUMN a TYPE text USING grouping(a)::text"));
        run("DROP TABLE zz_ac1");
    }

    @Test
    void whatTheExpressionNamesIsResolvedFirstAndTheTargetTypeLast() throws Exception {
        run("DROP TABLE IF EXISTS zz_ac2");
        run("CREATE TABLE zz_ac2 (a int)");
        assertEquals("function nosuch(integer) does not exist",
                messageOf("ALTER TABLE zz_ac2 ALTER COLUMN a TYPE text USING nosuch(a)"));
        assertEquals("column \"nocol\" does not exist",
                messageOf("ALTER TABLE zz_ac2 ALTER COLUMN a TYPE text USING count(nocol)"));
        assertEquals("aggregate functions are not allowed in transform expressions",
                messageOf("ALTER TABLE zz_ac2 ALTER COLUMN a TYPE nosuchtype USING count(*)"));
        assertEquals("aggregate functions are not allowed in transform expressions",
                messageOf("ALTER TABLE zz_ac2 ALTER COLUMN nocol TYPE text USING count(*)"));
        run("DROP TABLE zz_ac2");
    }

    @Test
    void aTransformExpressionThatIsOneStillConvertsTheColumn() throws Exception {
        run("DROP TABLE IF EXISTS zz_ac3");
        run("CREATE TABLE zz_ac3 (a int)");
        run("INSERT INTO zz_ac3 VALUES (7)");
        run("ALTER TABLE zz_ac3 ALTER COLUMN a TYPE text USING a::text");
        assertEquals("7", one("SELECT a FROM zz_ac3"));
        run("DROP TABLE zz_ac3");
    }

    @Test
    void theSpellingThatPredatesArgumentListsDeclaresOneJustAsWell() throws Exception {
        run("DROP AGGREGATE IF EXISTS zz_acsum(int)");
        run("DROP FUNCTION IF EXISTS zz_acadd(int, int)");
        run("CREATE FUNCTION zz_acadd(int, int) RETURNS int AS 'SELECT $1 + $2'"
                + " LANGUAGE sql IMMUTABLE");
        // The parameters may be written in any order, and the argument type is one of them.
        run("CREATE AGGREGATE zz_acsum (SFUNC = zz_acadd, BASETYPE = int, STYPE = int,"
                + " INITCOND = '0')");
        assertEquals("4", one("SELECT zz_acsum(i) FROM zz_ac0"));
        run("DROP AGGREGATE zz_acsum(int)");
        run("DROP FUNCTION zz_acadd(int, int)");
    }

    @Test
    void anAggregateAQueryDefinedIsAPgProcRowLikeAnyOther() throws Exception {
        run("DROP AGGREGATE IF EXISTS zz_accnt(int)");
        run("DROP FUNCTION IF EXISTS zz_acinc(int, int)");
        run("CREATE FUNCTION zz_acinc(int, int) RETURNS int AS 'SELECT $1 + 1'"
                + " LANGUAGE sql IMMUTABLE");
        run("CREATE AGGREGATE zz_accnt (BASETYPE = int, SFUNC = zz_acinc, STYPE = int,"
                + " INITCOND = '0')");
        assertEquals("a", one("SELECT prokind FROM pg_proc WHERE proname = 'zz_accnt'"));
        assertEquals("1", one("SELECT pronargs FROM pg_proc WHERE proname = 'zz_accnt'"));
        assertEquals("1", one("SELECT count(*) FROM pg_aggregate a JOIN pg_proc p"
                + " ON p.oid = a.aggfnoid WHERE p.proname = 'zz_accnt'"));
        assertEquals("zz_accnt(integer)", one("SELECT 'zz_accnt(int)'::regprocedure::text"));
        assertEquals("zz_accnt", one("SELECT 'zz_accnt'::regproc::text"));
        assertEquals("zz_accnt(integer)", one("SELECT to_regprocedure('zz_accnt(int)')::text"));
        // A signature nothing answers to is nothing, rather than the one that is there.
        assertNull(one("SELECT to_regprocedure('zz_accnt(text)')::text"));
        run("DROP AGGREGATE zz_accnt(int)");
        run("DROP FUNCTION zz_acinc(int, int)");
    }
}
