package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A qualifier is in scope or it is not, whatever rows the statement turns out to read.
 *
 * <p>PostgreSQL resolves every name a statement writes against the relations that statement lists,
 * before a page is read. memgres resolved some of them as each row reached the evaluator, so a
 * statement over an empty relation read no row, tripped over nothing and quietly did nothing; and
 * where the relation was written inside a sub-SELECT the two refusals came out the wrong way round.
 *
 * <p>The two are worded differently on purpose: a relation the statement does not list is missing,
 * and one it does list but has renamed is there and out of reach.
 *
 * <p>Every value here was measured against PostgreSQL 18.
 */
class QualifierScopeResolutionTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE zz_sc0 (a int)");
            st.execute("CREATE TABLE zz_sc1 (b int)");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void emptyBoth() throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("DELETE FROM zz_sc0");
            st.execute("DELETE FROM zz_sc1");
        }
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

    private static long count(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getLong(1);
        }
    }

    @Test
    void aRelationWrittenOnlyInsideASubSelectIsMissing() {
        String sql = "SELECT count(*) FROM (SELECT a FROM zz_sc0) s WHERE zz_sc0.a = 1";
        assertEquals("42P01", stateOf(sql));
        assertEquals("missing FROM-clause entry for table \"zz_sc0\"", messageOf(sql));
        assertEquals("missing FROM-clause entry for table \"zz_sc0\"", messageOf(
                "SELECT count(*) FROM (SELECT a FROM zz_sc0) s, zz_sc1 WHERE zz_sc0.a = 1"));
    }

    @Test
    void aParenthesisedJoinCoversItsRelationsWithItsAlias() {
        String sql = "SELECT count(*) FROM (zz_sc0 JOIN zz_sc1 ON zz_sc0.a = zz_sc1.b) j"
                + " WHERE zz_sc0.a = 1";
        PSQLException e = refusalOf(sql);
        assertEquals("42P01", e.getSQLState());
        ServerErrorMessage m = e.getServerErrorMessage();
        assertEquals("invalid reference to FROM-clause entry for table \"zz_sc0\"", m.getMessage());
        assertEquals("There is an entry for table \"zz_sc0\", but it cannot be referenced from"
                + " this part of the query.", m.getDetail());
    }

    @Test
    void whatTheFromItemDoesAnswerToStillResolves() throws Exception {
        assertEquals(0, count("SELECT count(*) FROM (SELECT a FROM zz_sc0) s WHERE s.a = 1"));
        assertEquals(0, count("SELECT count(*) FROM (zz_sc0 JOIN zz_sc1 ON zz_sc0.a = zz_sc1.b) j"
                + " WHERE j.a = 1"));
    }

    @Test
    void aRelationNothingWroteDownIsMissingOverAnEmptyTable() {
        assertEquals("missing FROM-clause entry for table \"zz_sc1\"",
                messageOf("SELECT count(*) FROM zz_sc0 WHERE zz_sc1.b = 1"));
        assertEquals("missing FROM-clause entry for table \"nosuch\"",
                messageOf("SELECT count(*) FROM zz_sc0 WHERE nosuch.a = 1"));
    }

    @Test
    void anAliasHidesTheRelationsOwnNameOverAnEmptyTable() {
        PSQLException e = refusalOf("SELECT count(*) FROM zz_sc0 x WHERE zz_sc0.a = 1");
        assertEquals("42P01", e.getSQLState());
        assertEquals("invalid reference to FROM-clause entry for table \"zz_sc0\"",
                e.getServerErrorMessage().getMessage());
        assertEquals("Perhaps you meant to reference the table alias \"x\".",
                e.getServerErrorMessage().getHint());
        assertEquals("42P01", stateOf("SELECT zz_sc0.a FROM zz_sc0 x"));
        assertEquals("42P01", stateOf("SELECT a FROM zz_sc0 x ORDER BY zz_sc0.a"));
    }

    @Test
    void anUpdateAndADeleteResolveTheirNamesBeforeTheyReadARow() {
        assertEquals("missing FROM-clause entry for table \"zz_sc1\"",
                messageOf("UPDATE zz_sc0 SET a = 1 WHERE zz_sc1.b = 1"));
        assertEquals("missing FROM-clause entry for table \"zz_sc1\"",
                messageOf("DELETE FROM zz_sc0 WHERE zz_sc1.b = 1"));
        assertEquals("missing FROM-clause entry for table \"zz_sc1\"",
                messageOf("UPDATE zz_sc0 SET a = zz_sc1.b"));
        assertEquals("missing FROM-clause entry for table \"nosuch\"",
                messageOf("UPDATE zz_sc0 SET a = 1 WHERE nosuch.a = 1"));
    }

    @Test
    void anAliasHidesTheTargetsOwnNameInAWrite() {
        for (String sql : new String[] {
                "UPDATE zz_sc0 x SET a = 1 WHERE zz_sc0.a = 1",
                "DELETE FROM zz_sc0 x WHERE zz_sc0.a = 1" }) {
            PSQLException e = refusalOf(sql);
            assertEquals("42P01", e.getSQLState());
            assertEquals("invalid reference to FROM-clause entry for table \"zz_sc0\"",
                    e.getServerErrorMessage().getMessage());
            assertEquals("Perhaps you meant to reference the table alias \"x\".",
                    e.getServerErrorMessage().getHint());
        }
    }

    @Test
    void aNameThatIsNoColumnOfTheTargetIsStillRefusedBeforeTheScan() {
        assertEquals("42703", stateOf("UPDATE zz_sc0 SET a = 1 WHERE nope = 1"));
        assertEquals("42703", stateOf("DELETE FROM zz_sc0 WHERE nope = 1"));
    }

    @Test
    void aFromOrAUsingBringsTheOtherRelationIntoScope() throws Exception {
        run("UPDATE zz_sc0 SET a = 1 FROM zz_sc1 WHERE zz_sc1.b = 1");
        run("DELETE FROM zz_sc0 USING zz_sc1 WHERE zz_sc1.b = 1");
    }

    @Test
    void aSubqueryBringsAFromListOfItsOwn() throws Exception {
        run("UPDATE zz_sc0 SET a = 1 WHERE a IN (SELECT b FROM zz_sc1)");
        run("DELETE FROM zz_sc0 WHERE a IN (SELECT b FROM zz_sc1)");
        assertEquals(0, count(
                "SELECT count(*) FROM zz_sc0 WHERE (SELECT count(*) FROM zz_sc1 WHERE b = 1) = 0"));
    }

    @Test
    void theTargetAnswersToItsOwnNameItsSchemaAndItsAlias() throws Exception {
        run("UPDATE zz_sc0 SET a = 1 WHERE zz_sc0.a = 1");
        run("UPDATE zz_sc0 SET a = 1 WHERE public.zz_sc0.a = 1");
        run("UPDATE zz_sc0 t SET a = t.a WHERE t.a = 1");
        run("UPDATE zz_sc0 SET a = 1 WHERE zz_sc0.ctid IS NOT NULL");
    }

    @Test
    void aNameThatIsNoColumnAtAllIsRefusedOverAnEmptyTable() {
        assertEquals("42703", stateOf("SELECT count(*) FROM zz_sc0 WHERE nope = 1"));
        assertEquals("42703", stateOf("SELECT nope FROM zz_sc0"));
        assertEquals("42703", stateOf("SELECT a FROM zz_sc0 ORDER BY nope"));
        assertEquals("42703", stateOf("SELECT count(*) FROM zz_sc0 HAVING nope > 0"));
        assertEquals("42703", stateOf("SELECT a FROM zz_sc0 GROUP BY nope"));
        assertEquals("42703", stateOf("SELECT count(*) FROM zz_sc0 x WHERE x.nope = 1"));
    }

    @Test
    void theSameRefusalsOnceTheTableHasARowInIt() throws Exception {
        run("INSERT INTO zz_sc0 VALUES (1)");
        run("INSERT INTO zz_sc1 VALUES (1)");
        assertEquals("42P01", stateOf("SELECT count(*) FROM zz_sc0 x WHERE zz_sc0.a = 1"));
        assertEquals("42P01", stateOf("UPDATE zz_sc0 SET a = 1 WHERE zz_sc1.b = 1"));
        assertEquals("42P01", stateOf("DELETE FROM zz_sc0 WHERE zz_sc1.b = 1"));
        assertEquals("42P01", stateOf("UPDATE zz_sc0 x SET a = 1 WHERE zz_sc0.a = 1"));
        assertEquals("missing FROM-clause entry for table \"zz_sc0\"", messageOf(
                "SELECT count(*) FROM (SELECT a FROM zz_sc0) s WHERE zz_sc0.a = 1"));
    }

    @Test
    void theOnesThatWereAlwaysGoingToWorkStillDo() throws Exception {
        run("INSERT INTO zz_sc0 VALUES (1)");
        run("UPDATE zz_sc0 SET a = a + 1 WHERE a = 1");
        assertEquals(1, count("SELECT count(*) FROM zz_sc0 WHERE a = 2"));
        run("DELETE FROM zz_sc0 WHERE a = 2");
        assertEquals(0, count("SELECT count(*) FROM zz_sc0"));
    }
}
