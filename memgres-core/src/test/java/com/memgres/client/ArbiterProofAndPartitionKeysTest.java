package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two questions about what a statement is allowed to name, and both were answered by looking at
 * the spelling rather than at the thing spelled.
 *
 * <p>The first is the arbiter proof: a predicate written beside a conflict target reaches a
 * partial unique index only where the index's own predicate follows from it, and three things
 * decide whether it does. A bound over text is settled by the collation, under which a lowercase
 * letter sorts before its uppercase spelling and both before the next letter, so 'a' &lt; 'A' &lt;
 * 'b' &lt; 'B' -- the codepoints, which put every uppercase letter first, would take the wrong
 * index in both directions at once. A bound over a number is settled after the arithmetic over
 * constants has been worked out, so 1 - 1 and 5 % 5 and (1 + 1) * 3 - 6 are one constant and an
 * index whose predicate the catalogue keeps unfolded is still reached. And the type a constant's
 * spelling gives it decides which side of the comparison the cast lands on: over an int column
 * 0.5 widens the column, over a numeric column 0 widens the constant, and two comparisons are
 * only about the same thing where the same cast lands on the column in each.
 *
 * <p>The danger in every one of those is over-accepting -- taking an index whose rows the
 * statement's predicate does not rule in -- so each case below is pinned in the refusing
 * direction as well as the accepting one.
 *
 * <p>The second question is what may stand in a partition key. An element written in parentheses
 * is an expression, and a bare name in an expression resolves to a column of the relation first
 * and to the relation itself second, which stands for the whole row: PARTITION BY RANGE ((t)) is
 * a key over the whole row, recorded under attribute number 0 and read back with the .*
 * PostgreSQL stores it as. Written bare the same name is only ever a column name. The other way
 * about, a parenthesised name that does resolve to a column reduces to that column -- which is
 * why a generated column, refused in a key because its value is worked out only after the row
 * has been routed, is refused where the element is read as a name and accepted where it is read
 * as an expression.
 *
 * <p>Every expectation here was read off PostgreSQL 18 before it was written down.
 */
class ArbiterProofAndPartitionKeysTest {

    /** The sentence PostgreSQL refuses a conflict target with when no index arbitrates. */
    private static final String NO_ARBITER =
            "there is no unique or exclusion constraint matching the ON CONFLICT specification";

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    /** The first column of the first row, as text, or "(no rows)" when the query answers none. */
    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : "(no rows)";
        }
    }

    /** The one value the query returns, read as the number it is. */
    private static long num(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getLong(1);
        }
    }

    /** Every row of the query, its columns joined with a slash and its rows with a semicolon. */
    private static String rowsOf(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('/');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return String.join(";", out);
    }

    /** The SQLSTATE a statement raises, or "OK" when it does not raise at all. */
    private static String stateOf(String sql) {
        try {
            exec(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    /** The fields of the error a statement raises, as a client reads them off the wire. */
    private static org.postgresql.util.ServerErrorMessage fieldsOf(String sql) {
        SQLException thrown = assertThrows(SQLException.class, () -> exec(sql),
                "expected an error from: " + sql);
        assertTrue(thrown instanceof org.postgresql.util.PSQLException,
                "expected a server error from: " + sql);
        return ((org.postgresql.util.PSQLException) thrown).getServerErrorMessage();
    }

    /** The primary message of the error a statement raises, without any severity prefix. */
    private static String messageOf(String sql) {
        return fieldsOf(sql).getMessage();
    }

    private static String detailOf(String sql) {
        return fieldsOf(sql).getDetail();
    }

    /** The strategy, the key width and the attribute numbers a partitioned table records. */
    private static String partKeyOf(String relation) throws SQLException {
        return scalar("SELECT partstrat::text || '/' || partnatts::text || '/'"
                + " || partattrs::text FROM pg_partitioned_table"
                + " WHERE partrelid = '" + relation + "'::regclass");
    }

    /**
     * Runs each predicate as an arbiter predicate for one insert and reports which of them were
     * refused, as predicate=SQLSTATE, in the order given. An empty answer means every predicate
     * reached the index; naming the refused ones pins the proof in both directions at once, and
     * says which case moved when one does.
     */
    private static String refusals(String insert, String target, String... predicates) {
        List<String> out = new ArrayList<>();
        for (String predicate : predicates) {
            String state = stateOf(insert + " ON CONFLICT (" + target + ") WHERE "
                    + predicate + " DO NOTHING");
            if (!"OK".equals(state)) out.add(predicate + "=" + state);
        }
        return String.join(",", out);
    }

    // ------------------------------------------------------------ A text bound is the collation's to settle

    @Test
    void aTextBoundIsProvedUnderTheCollationAndNotUnderTheCodepoints() throws Exception {
        exec("CREATE TABLE zzt4a_g (i int, s text)");
        exec("CREATE UNIQUE INDEX zzt4a_g_u ON zzt4a_g (i) WHERE s > 'a'");
        exec("INSERT INTO zzt4a_g VALUES (1, 'x')");

        // Every uppercase letter sorts after 'a', so each of these rules out everything the index
        // rules out -- and the codepoints, which put 'A' below 'a', would say none of them does.
        assertEquals("", refusals("INSERT INTO zzt4a_g VALUES (1, 'y')", "i",
                "s > 'A'", "s > 'B'", "s > 'Z'", "s >= 'B'", "s > 'A0'",
                "s > 'b'", "s > 'z'", "s > 'aa'", "s >= 'aa'",
                "s = 'A'", "s = 'zz'",
                "s BETWEEN 'B' AND 'z'", "s IN ('A', 'B')"));

        // And the bounds that admit a row the index does not hold reach nothing: the empty string
        // is below every other, 'a' itself is excluded by a strict bound, and a bound from the
        // other side says nothing at all about this one.
        assertEquals("s > ''=42P10,s >= 'a'=42P10,s < 'b'=42P10",
                refusals("INSERT INTO zzt4a_g VALUES (1, 'y')", "i",
                        "s > ''", "s >= 'a'", "s < 'b'"));

        assertEquals(NO_ARBITER, messageOf("INSERT INTO zzt4a_g VALUES (1, 'y')"
                + " ON CONFLICT (i) WHERE s >= 'a' DO NOTHING"));

        // Nothing above wrote a row: each accepted statement found the conflict and did nothing.
        assertEquals("1/x", rowsOf("SELECT i, s FROM zzt4a_g ORDER BY i"));
        exec("DROP TABLE zzt4a_g");
    }

    @Test
    void anUppercaseBoundIsReachedOnlyFromAboveWhereTheCollationPutsIt() throws Exception {
        exec("CREATE TABLE zzt4a_h (i int, s text)");
        exec("CREATE UNIQUE INDEX zzt4a_h_u ON zzt4a_h (i) WHERE s > 'B'");
        exec("INSERT INTO zzt4a_h VALUES (1, 'x')");

        // 'B' sorts after 'a', 'A' and 'b' alike, so of those spellings only a letter past 'B'
        // proves the index's bound. Read by codepoint, 'B' is below all three and every one of
        // them would be accepted -- which would arbitrate on an index the statement has not ruled
        // its rows into.
        assertEquals("s > 'a'=42P10,s > 'A'=42P10,s > 'b'=42P10",
                refusals("INSERT INTO zzt4a_h VALUES (1, 'y')", "i",
                        "s > 'a'", "s > 'A'", "s > 'b'"));

        assertEquals("", refusals("INSERT INTO zzt4a_h VALUES (1, 'y')", "i",
                "s > 'c'", "s > 'C'", "s = 'c'"));

        assertEquals("1/x", rowsOf("SELECT i, s FROM zzt4a_h ORDER BY i"));
        exec("DROP TABLE zzt4a_h");
    }

    @Test
    void aBoundFromBelowRunsTheSameOrderingTheOtherWayRound() throws Exception {
        exec("CREATE TABLE zzt4a_i (i int, s text)");
        exec("CREATE UNIQUE INDEX zzt4a_i_u ON zzt4a_i (i) WHERE s < 'a'");
        exec("INSERT INTO zzt4a_i VALUES (1, 'x')");

        // 'A' and 'B' both sort above 'a', so a bound below either admits rows the index does not
        // hold; and the inclusive form admits 'a' itself.
        assertEquals("s < 'A'=42P10,s < 'B'=42P10,s <= 'a'=42P10",
                refusals("INSERT INTO zzt4a_i VALUES (2, 'y')", "i",
                        "s < 'A'", "s < 'B'", "s <= 'a'"));

        // The index's own bound reaches it. 'x' is no row the index holds, so there is nothing to
        // conflict with and the row goes in.
        assertEquals("", refusals("INSERT INTO zzt4a_i VALUES (2, 'y')", "i", "s < 'a'"));
        assertEquals("1/x;2/y", rowsOf("SELECT i, s FROM zzt4a_i ORDER BY i"));
        exec("DROP TABLE zzt4a_i");
    }

    @Test
    void twoSpellingsOfOneLetterAreTwoValuesToAnEqualityBound() throws Exception {
        exec("CREATE TABLE zzt4a_j (i int, s text)");
        exec("CREATE UNIQUE INDEX zzt4a_j_u ON zzt4a_j (i) WHERE s = 'a'");
        exec("INSERT INTO zzt4a_j VALUES (1, 'x')");

        assertEquals("s = 'A'=42P10",
                refusals("INSERT INTO zzt4a_j VALUES (2, 'y')", "i", "s = 'A'"));
        assertEquals("", refusals("INSERT INTO zzt4a_j VALUES (2, 'y')", "i", "s = 'a'"));

        assertEquals("1/x;2/y", rowsOf("SELECT i, s FROM zzt4a_j ORDER BY i"));
        exec("DROP TABLE zzt4a_j");
    }

    @Test
    void caseDecidesOnlyATieTheLettersLeaveBehind() throws Exception {
        exec("CREATE TABLE zzt4a_k (i int, s text)");
        exec("CREATE UNIQUE INDEX zzt4a_k_u ON zzt4a_k (i) WHERE s > 'A1'");
        exec("INSERT INTO zzt4a_k VALUES (1, 'x')");

        // The letters are compared before the case of them, so 'a1' sorts below 'A1' and 'a2'
        // above it: a bound at 'a1' admits 'A1', which the index does not hold, and a bound at
        // 'a2' does not.
        assertEquals("s > 'a1'=42P10,s > 'a0'=42P10",
                refusals("INSERT INTO zzt4a_k VALUES (1, 'y')", "i", "s > 'a1'", "s > 'a0'"));
        assertEquals("", refusals("INSERT INTO zzt4a_k VALUES (1, 'y')", "i",
                "s > 'a2'", "s > 'B0'"));

        assertEquals("1/x", rowsOf("SELECT i, s FROM zzt4a_k ORDER BY i"));
        exec("DROP TABLE zzt4a_k");
    }

    @Test
    void noBoundAtADigitProvesABoundAtALetter() throws Exception {
        exec("CREATE TABLE zzt4a_l (i int, s text)");
        exec("CREATE UNIQUE INDEX zzt4a_l_u ON zzt4a_l (i) WHERE s > 'b'");
        exec("INSERT INTO zzt4a_l VALUES (1, 'x')");

        assertEquals("s > '0'=42P10,s > '9'=42P10",
                refusals("INSERT INTO zzt4a_l VALUES (1, 'y')", "i", "s > '0'", "s > '9'"));
        assertEquals("", refusals("INSERT INTO zzt4a_l VALUES (1, 'y')", "i",
                "s > 'e'", "s > 'E'", "s > 'Z'"));

        assertEquals("1/x", rowsOf("SELECT i, s FROM zzt4a_l ORDER BY i"));
        exec("DROP TABLE zzt4a_l");
    }

    @Test
    void anInclusiveBoundIsReachedByAnInclusiveBoundAtTheSamePlace() throws Exception {
        exec("CREATE TABLE zzt4a_m (i int, s text)");
        exec("CREATE UNIQUE INDEX zzt4a_m_u ON zzt4a_m (i) WHERE s >= 'b'");
        exec("INSERT INTO zzt4a_m VALUES (1, 'x')");

        assertEquals("", refusals("INSERT INTO zzt4a_m VALUES (1, 'y')", "i",
                "s >= 'b'", "s > 'b'", "s > 'B'", "s >= 'B'", "s = 'B'"));
        assertEquals("s >= 'a'=42P10",
                refusals("INSERT INTO zzt4a_m VALUES (1, 'y')", "i", "s >= 'a'"));

        assertEquals("1/x", rowsOf("SELECT i, s FROM zzt4a_m ORDER BY i"));
        exec("DROP TABLE zzt4a_m");
    }

    // ------------------------------------------------------------ The arithmetic is worked out before the comparison

    @Test
    void arithmeticOverConstantsIsWorkedOutBeforeThePredicatesAreCompared() throws Exception {
        exec("CREATE TABLE zzt4a_a (i int, k int)");
        exec("CREATE UNIQUE INDEX zzt4a_a_u ON zzt4a_a (i) WHERE i > 0");
        exec("INSERT INTO zzt4a_a VALUES (1, 1)");

        // Each of these comes to 0 or to something above it. Integer division truncates toward
        // zero, so 5 / 2 is 2 and not 2.5, and a remainder is an integer too.
        assertEquals("", refusals("INSERT INTO zzt4a_a VALUES (1, 2)", "i",
                "i > 1 - 1", "i > 2 - 1", "i > 1 * 0", "i > 0 + 0", "i > -1 + 1",
                "i > (1 + 1) * 3 - 6", "i > 3 - 1 - 2", "i > - (1 - 1)",
                "i > 5 / 2", "i > 4 / 3", "i > 6 / 2 / 3", "i > 2 / 2",
                "i > 5 % 5", "i > 7 % 3"));

        // Working the arithmetic out is what refuses these: each comes to a bound below the
        // index's, which admits rows the index does not hold.
        assertEquals("i > 0 - 1=42P10,i > 2 - 3=42P10,i > 2 * 3 - 7=42P10",
                refusals("INSERT INTO zzt4a_a VALUES (1, 2)", "i",
                        "i > 0 - 1", "i > 2 - 3", "i > 2 * 3 - 7"));

        // An operand that is not a constant is not worked out at all, on either side.
        assertEquals("i > k - 1=42P10,i > 1 - k=42P10",
                refusals("INSERT INTO zzt4a_a VALUES (1, 2)", "i", "i > k - 1", "i > 1 - k"));

        assertEquals("1/1", rowsOf("SELECT i, k FROM zzt4a_a ORDER BY i"));
        exec("DROP TABLE zzt4a_a");
    }

    @Test
    void anIndexPredicateTheCatalogueKeepsUnfoldedIsStillReached() throws Exception {
        exec("CREATE TABLE zzt4a_b (i int, k int)");
        exec("CREATE UNIQUE INDEX zzt4a_b_u ON zzt4a_b (i) WHERE i > 1 - 1");

        // The catalogue keeps the predicate as it was written; the folding happens when the two
        // predicates are asked of each other, so both sides meet in the middle.
        assertEquals("(i > (1 - 1))",
                scalar("SELECT pg_get_expr(indpred, indrelid) FROM pg_index"
                        + " WHERE indexrelid = 'zzt4a_b_u'::regclass"));

        exec("INSERT INTO zzt4a_b VALUES (1, 1)");
        assertEquals("", refusals("INSERT INTO zzt4a_b VALUES (1, 2)", "i",
                "i > 0", "i > 2 - 2"));
        assertEquals("i > 0 - 2=42P10",
                refusals("INSERT INTO zzt4a_b VALUES (1, 2)", "i", "i > 0 - 2"));

        assertEquals("1/1", rowsOf("SELECT i, k FROM zzt4a_b ORDER BY i"));
        exec("DROP TABLE zzt4a_b");
    }

    // ------------------------------------------------------------ A constant carries the width its spelling gives it

    @Test
    void aNumericConstantAgainstAnIntegerColumnReachesNoIntegerIndex() throws Exception {
        exec("CREATE TABLE zzt4a_c (i int, k int)");
        exec("CREATE UNIQUE INDEX zzt4a_c_u ON zzt4a_c (i) WHERE i > 0");

        // Nothing is cast here: both sides are already integers.
        assertEquals("(i > 0)",
                scalar("SELECT pg_get_expr(indpred, indrelid) FROM pg_index"
                        + " WHERE indexrelid = 'zzt4a_c_u'::regclass"));

        exec("INSERT INTO zzt4a_c VALUES (1, 1)");

        // A number written with a point, an exponent or a cast to a type wider than the column
        // widens the column instead, so the comparison it writes is not the comparison the index
        // made -- 1.0 is above the index's bound by any reading and still proves nothing. The
        // arithmetic is worked out first, so what a subtraction comes to is judged the same way.
        assertEquals("i > 0.5=42P10,i > 1.0=42P10,i > 0.0=42P10,i > 1e0=42P10,"
                        + "i > 1::numeric=42P10,i > 1::float8=42P10,i > 1::real=42P10,"
                        + "i > 1 - 0.5=42P10,i > 1 - 1.0=42P10",
                refusals("INSERT INTO zzt4a_c VALUES (1, 2)", "i",
                        "i > 0.5", "i > 1.0", "i > 0.0", "i > 1e0",
                        "i > 1::numeric", "i > 1::float8", "i > 1::real",
                        "i > 1 - 0.5", "i > 1 - 1.0"));

        // An integer type leaves the column alone, whatever its width, and a bare string has no
        // type of its own to impose.
        assertEquals("", refusals("INSERT INTO zzt4a_c VALUES (1, 2)", "i",
                "i > 1::bigint", "i > 1::smallint", "i > 1.0::int", "i > (0)", "i > '1'"));

        assertEquals("1/1", rowsOf("SELECT i, k FROM zzt4a_c ORDER BY i"));
        exec("DROP TABLE zzt4a_c");
    }

    @Test
    void anIndexOverTheWidenedColumnIsReachedOnlyByTheWidenedBounds() throws Exception {
        exec("CREATE TABLE zzt4a_d (i int, k int)");
        exec("CREATE UNIQUE INDEX zzt4a_d_u ON zzt4a_d (i) WHERE i > 0.5");

        // Over an int column the column is the side the cast lands on.
        assertEquals("((i)::numeric > 0.5)",
                scalar("SELECT pg_get_expr(indpred, indrelid) FROM pg_index"
                        + " WHERE indexrelid = 'zzt4a_d_u'::regclass"));

        exec("INSERT INTO zzt4a_d VALUES (1, 1)");
        assertEquals("", refusals("INSERT INTO zzt4a_d VALUES (1, 2)", "i", "i > 0.6"));

        // 1 is above 0.5 by any reading; it compares the column unwidened, so it is refused.
        assertEquals("i > 1=42P10,i > 0.4=42P10",
                refusals("INSERT INTO zzt4a_d VALUES (1, 2)", "i", "i > 1", "i > 0.4"));

        assertEquals("1/1", rowsOf("SELECT i, k FROM zzt4a_d ORDER BY i"));
        exec("DROP TABLE zzt4a_d");
    }

    @Test
    void aCastDownToAnIntegerTypeRoundsAwayFromZero() throws Exception {
        exec("CREATE TABLE zzt4a_e (i int, k int)");
        exec("CREATE UNIQUE INDEX zzt4a_e_u ON zzt4a_e (i) WHERE i > 1");
        exec("INSERT INTO zzt4a_e VALUES (2, 1)");

        // 0.5::int is 1 and 1.5::int is 2, so both bounds are the index's or above it.
        assertEquals("", refusals("INSERT INTO zzt4a_e VALUES (2, 2)", "i",
                "i > 0.5::int", "i > 1.5::int"));

        // 0.4::int is 0, and away from zero means (-0.5)::int is -1 rather than 0.
        assertEquals("i > 0.4::int=42P10,i > (-0.5)::int=42P10",
                refusals("INSERT INTO zzt4a_e VALUES (2, 2)", "i",
                        "i > 0.4::int", "i > (-0.5)::int"));

        assertEquals("2/1", rowsOf("SELECT i, k FROM zzt4a_e ORDER BY i"));
        exec("DROP TABLE zzt4a_e");
    }

    @Test
    void overANumericColumnEverySpellingOfTheNumberMeetsTheIndex() throws Exception {
        exec("CREATE TABLE zzt4a_f (i int, n numeric)");
        exec("CREATE UNIQUE INDEX zzt4a_f_u ON zzt4a_f (n) WHERE n > 0");

        // Here the constant is what the cast lands on, so no spelling of the number moves the
        // column and all of them are comparable with one another.
        assertEquals("(n > (0)::numeric)",
                scalar("SELECT pg_get_expr(indpred, indrelid) FROM pg_index"
                        + " WHERE indexrelid = 'zzt4a_f_u'::regclass"));

        exec("INSERT INTO zzt4a_f VALUES (1, 5)");
        assertEquals("", refusals("INSERT INTO zzt4a_f VALUES (2, 5)", "n",
                "n > 0.5", "n > 1", "n > 1e0", "n > 1::int",
                "n > 1 - 1", "n > 0.5 + 0.5", "n > 1.0 / 2", "n > 1.5 - 0.5", "n > 0.5 - 0.5"));

        assertEquals("n > -1=42P10,n > 2 - 3=42P10,n > 0.5 - 1.0=42P10",
                refusals("INSERT INTO zzt4a_f VALUES (2, 5)", "n",
                        "n > -1", "n > 2 - 3", "n > 0.5 - 1.0"));

        assertEquals("1/5", rowsOf("SELECT i, n FROM zzt4a_f ORDER BY i"));
        exec("DROP TABLE zzt4a_f");
    }

    @Test
    void aBigintColumnTakesEveryIntegerWidthAndNoNumberWithAPoint() throws Exception {
        exec("CREATE TABLE zzt4a_p (i int, b bigint)");
        exec("CREATE UNIQUE INDEX zzt4a_p_u ON zzt4a_p (b) WHERE b > 0");
        exec("INSERT INTO zzt4a_p VALUES (1, 5)");

        assertEquals("", refusals("INSERT INTO zzt4a_p VALUES (2, 5)", "b",
                "b > 1", "b > 1::int", "b > 1::bigint"));
        assertEquals("b > 1.0=42P10",
                refusals("INSERT INTO zzt4a_p VALUES (2, 5)", "b", "b > 1.0"));

        assertEquals("1/5", rowsOf("SELECT i, b FROM zzt4a_p ORDER BY i"));
        exec("DROP TABLE zzt4a_p");
    }

    @Test
    void aFloatColumnIsTheSideTheConstantIsCastTo() throws Exception {
        exec("CREATE TABLE zzt4a_q (i int, f float8)");
        exec("CREATE UNIQUE INDEX zzt4a_q_u ON zzt4a_q (f) WHERE f > 0");

        assertEquals("(f > (0)::double precision)",
                scalar("SELECT pg_get_expr(indpred, indrelid) FROM pg_index"
                        + " WHERE indexrelid = 'zzt4a_q_u'::regclass"));

        exec("INSERT INTO zzt4a_q VALUES (1, 5)");
        assertEquals("", refusals("INSERT INTO zzt4a_q VALUES (2, 5)", "f",
                "f > 1", "f > 1.0", "f > 1e0", "f > 0.5"));
        assertEquals("f > -1=42P10",
                refusals("INSERT INTO zzt4a_q VALUES (2, 5)", "f", "f > -1"));

        assertEquals("1/5", rowsOf("SELECT i, f FROM zzt4a_q ORDER BY i"));
        exec("DROP TABLE zzt4a_q");
    }

    @Test
    void aBoundIsOnlyABoundAndDoesNotCountTheIntegersBetweenTwoOfThem() throws Exception {
        exec("CREATE TABLE zzt4a_r (i int)");
        exec("CREATE UNIQUE INDEX zzt4a_r_u ON zzt4a_r (i) WHERE i >= 1");
        exec("INSERT INTO zzt4a_r VALUES (1)");

        assertEquals("", refusals("INSERT INTO zzt4a_r VALUES (1)", "i", "i >= 1"));

        // i > 0 admits exactly what i >= 1 admits over the integers, and the proof is over the
        // values the operators admit rather than over the type's steps, so it reaches nothing.
        assertEquals("i > 0=42P10,i > 1 - 1=42P10",
                refusals("INSERT INTO zzt4a_r VALUES (1)", "i", "i > 0", "i > 1 - 1"));

        assertEquals(1, num("SELECT count(*)::int FROM zzt4a_r"));
        exec("DROP TABLE zzt4a_r");
    }

    // ------------------------------------------------------------ What a refused arbiter leaves behind

    @Test
    void aRefusedArbiterAbortsTheTransactionAndASavepointTakesItBack() throws Exception {
        exec("CREATE TABLE zzt4a_t (i int, s text)");
        exec("CREATE UNIQUE INDEX zzt4a_t_u ON zzt4a_t (i) WHERE s > 'a'");
        exec("INSERT INTO zzt4a_t VALUES (1, 'x')");
        try {
            conn.setAutoCommit(false);
            exec("INSERT INTO zzt4a_t VALUES (2, 'y')");
            Savepoint sp = conn.setSavepoint("zzt4a_sp");

            // The refusal is an ordinary error, so it takes the transaction down with it.
            assertEquals("42P10", stateOf("INSERT INTO zzt4a_t VALUES (1, 'z')"
                    + " ON CONFLICT (i) WHERE s >= 'a' DO NOTHING"));
            assertEquals("25P02", stateOf("SELECT count(*) FROM zzt4a_t"));

            // Rolling back to the savepoint takes the transaction back, with the row written
            // before it still there.
            conn.rollback(sp);
            assertEquals(2, num("SELECT count(*)::int FROM zzt4a_t"));

            // And the bound the collation does prove arbitrates the update it was written for.
            exec("INSERT INTO zzt4a_t VALUES (1, 'z')"
                    + " ON CONFLICT (i) WHERE s > 'A' DO UPDATE SET s = 'u'");
            assertEquals("1/u;2/y", rowsOf("SELECT i, s FROM zzt4a_t ORDER BY i"));
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
        }
        assertEquals("1/u;2/y", rowsOf("SELECT i, s FROM zzt4a_t ORDER BY i"));
        exec("DROP TABLE zzt4a_t");
    }

    // ------------------------------------------------------------ A whole row stands in a partition key

    @Test
    void aWholeRowStandsInAPartitionKeyUnderEveryStrategy() throws Exception {
        exec("CREATE TABLE zzt4a_wr (i int, k int) PARTITION BY RANGE ((zzt4a_wr))");
        assertEquals("RANGE ((zzt4a_wr.*))",
                scalar("SELECT pg_get_partkeydef('zzt4a_wr'::regclass)"));
        // Attribute number 0 is how a key element that is no column of the relation is recorded.
        assertEquals("r/1/0", partKeyOf("zzt4a_wr"));

        exec("CREATE TABLE zzt4a_wl (i int, k int) PARTITION BY LIST ((zzt4a_wl))");
        assertEquals("LIST ((zzt4a_wl.*))",
                scalar("SELECT pg_get_partkeydef('zzt4a_wl'::regclass)"));
        assertEquals("l/1/0", partKeyOf("zzt4a_wl"));

        exec("CREATE TABLE zzt4a_wh (i int, k int) PARTITION BY HASH ((zzt4a_wh))");
        assertEquals("HASH ((zzt4a_wh.*))",
                scalar("SELECT pg_get_partkeydef('zzt4a_wh'::regclass)"));
        assertEquals("h/1/0", partKeyOf("zzt4a_wh"));

        exec("DROP TABLE zzt4a_wr");
        exec("DROP TABLE zzt4a_wl");
        exec("DROP TABLE zzt4a_wh");
    }

    @Test
    void aWholeRowKeyKeepsItsPlaceBesideAColumnOfTheRelation() throws Exception {
        exec("CREATE TABLE zzt4a_wm (i int, k int) PARTITION BY RANGE ((zzt4a_wm), i)");
        assertEquals("RANGE ((zzt4a_wm.*), i)",
                scalar("SELECT pg_get_partkeydef('zzt4a_wm'::regclass)"));
        assertEquals("r/2/0 1", partKeyOf("zzt4a_wm"));
        exec("DROP TABLE zzt4a_wm");
    }

    @Test
    void aColumnCarryingTheRelationsNameTakesTheNameBack() throws Exception {
        exec("CREATE TABLE zzt4a_ws (zzt4a_ws int, k int) PARTITION BY RANGE ((zzt4a_ws))");
        // The column wins, so this is a plain column key and reads back as one.
        assertEquals("RANGE (zzt4a_ws)",
                scalar("SELECT pg_get_partkeydef('zzt4a_ws'::regclass)"));
        assertEquals("r/1/1", partKeyOf("zzt4a_ws"));
        exec("DROP TABLE zzt4a_ws");
    }

    @Test
    void aPartitionedTableKeyedOnItsWholeRowTakesPartitionsLikeAnyOther() throws Exception {
        exec("CREATE TABLE zzt4a_wp (i int, k int) PARTITION BY LIST ((zzt4a_wp))");
        exec("CREATE TABLE zzt4a_wp_1 PARTITION OF zzt4a_wp FOR VALUES IN ('(1,1)')");
        assertEquals("zzt4a_wp/p;zzt4a_wp_1/r",
                rowsOf("SELECT relname, relkind::text FROM pg_class"
                        + " WHERE relname LIKE 'zzt4a\\_wp%' ORDER BY relname"));
        exec("DROP TABLE zzt4a_wp");
        assertEquals(0, num("SELECT count(*)::int FROM pg_class"
                + " WHERE relname LIKE 'zzt4a\\_wp%'"));
    }

    @Test
    void aBareRelationNameInAPartitionKeyIsStillOnlyAColumnName() throws Exception {
        // Outside parentheses the element is read as a name, and a name there is a column's.
        assertEquals("column \"zzt4a_bp\" named in partition key does not exist",
                messageOf("CREATE TABLE zzt4a_bp (i int, k int) PARTITION BY RANGE (zzt4a_bp)"));
        assertEquals("column \"zzt4a_bq\" named in partition key does not exist",
                messageOf("CREATE TABLE zzt4a_bq (i int, k int) PARTITION BY LIST (zzt4a_bq)"));
        assertEquals("column \"zzt4a_bh\" named in partition key does not exist",
                messageOf("CREATE TABLE zzt4a_bh (i int, k int) PARTITION BY HASH (zzt4a_bh)"));
        assertEquals("42703",
                stateOf("CREATE TABLE zzt4a_bp (i int, k int) PARTITION BY RANGE (zzt4a_bp)"));

        // Inside parentheses a name that is neither a column nor the relation is reported the way
        // any expression's unknown name is.
        assertEquals("column \"zzt4a_nosuch\" does not exist",
                messageOf("CREATE TABLE zzt4a_bn (i int, k int)"
                        + " PARTITION BY RANGE ((zzt4a_nosuch))"));
        assertEquals("42703", stateOf("CREATE TABLE zzt4a_bn (i int, k int)"
                + " PARTITION BY RANGE ((zzt4a_nosuch))"));

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname"
                + " IN ('zzt4a_bp','zzt4a_bq','zzt4a_bh','zzt4a_bn')"));
    }

    // ------------------------------------------------------------ A parenthesised column reduces to the column

    @Test
    void aParenthesisedColumnReducesToTheColumn() throws Exception {
        exec("CREATE TABLE zzt4a_gh (i int, k int) PARTITION BY RANGE ((i))");
        // PostgreSQL prints a plain column reference as the column, parentheses and all gone.
        assertEquals("RANGE (i)", scalar("SELECT pg_get_partkeydef('zzt4a_gh'::regclass)"));
        assertEquals("r/1/1", partKeyOf("zzt4a_gh"));

        exec("CREATE TABLE zzt4a_gi (i int, k int) PARTITION BY RANGE (i, (k))");
        assertEquals("RANGE (i, k)", scalar("SELECT pg_get_partkeydef('zzt4a_gi'::regclass)"));

        for (String t : Arrays.asList("zzt4a_gh", "zzt4a_gi")) {
            exec("DROP TABLE " + t);
        }
    }

    @Test
    void aParenthesisedGeneratedColumnIsRefusedLikeTheBareOne() throws Exception {
        // The parentheses decide nothing. An element that comes back to a plain column is that
        // column, and is held to everything the same column written bare is held to.
        String stored = "CREATE TABLE zzt4a_ga (i int, k int GENERATED ALWAYS AS (i * 2) STORED)"
                + " PARTITION BY RANGE ((k))";
        assertEquals("42P17", stateOf(stored));
        assertEquals("cannot use generated column in partition key", messageOf(stored));
        assertEquals("Column \"k\" is a generated column.", detailOf(stored));

        // Stored or virtual, and under every strategy.
        String virtual = "CREATE TABLE zzt4a_gb (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL)"
                + " PARTITION BY LIST ((k))";
        assertEquals("42P17", stateOf(virtual));
        assertEquals("cannot use generated column in partition key", messageOf(virtual));

        String hashed = "CREATE TABLE zzt4a_gc (i int, k int GENERATED ALWAYS AS (i * 2) STORED)"
                + " PARTITION BY HASH ((k))";
        assertEquals("42P17", stateOf(hashed));

        // One ordinary column standing beside it does not save it.
        String company = "CREATE TABLE zzt4a_gg (i int, k int GENERATED ALWAYS AS (i * 2) STORED)"
                + " PARTITION BY RANGE (i, (k))";
        assertEquals("42P17", stateOf(company));

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname"
                + " IN ('zzt4a_ga','zzt4a_gb','zzt4a_gc','zzt4a_gg')"));
    }

    @Test
    void aBareGeneratedColumnInAPartitionKeyIsRefusedAndNamedInTheDetail() throws Exception {
        String stored = "CREATE TABLE zzt4a_gd (i int, k int GENERATED ALWAYS AS (i * 2) STORED)"
                + " PARTITION BY RANGE (k)";
        assertEquals("42P17", stateOf(stored));
        assertEquals("cannot use generated column in partition key", messageOf(stored));
        assertEquals("Column \"k\" is a generated column.", detailOf(stored));

        String virtual = "CREATE TABLE zzt4a_ge (i int, k int GENERATED ALWAYS AS (i * 2) VIRTUAL)"
                + " PARTITION BY RANGE (k)";
        assertEquals("42P17", stateOf(virtual));
        assertEquals("cannot use generated column in partition key", messageOf(virtual));
        assertEquals("Column \"k\" is a generated column.", detailOf(virtual));

        // A real expression over it is refused under the same sentence.
        String expression = "CREATE TABLE zzt4a_gf (i int, k int GENERATED ALWAYS AS (i * 2)"
                + " STORED) PARTITION BY RANGE ((k + 1))";
        assertEquals("42P17", stateOf(expression));
        assertEquals("cannot use generated column in partition key", messageOf(expression));
        assertEquals("Column \"k\" is a generated column.", detailOf(expression));

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname"
                + " IN ('zzt4a_gd','zzt4a_ge','zzt4a_gf')"));
    }

    // ------------------------------------------------------------ VALUES reads as a column name

    @Test
    void valuesInAPartitionKeyIsAColumnNameAndTheParenthesisIsTheFault() throws Exception {
        // VALUES is a word the grammar reads as an ordinary column name, so the element parses as
        // far as the name and stops at the call parenthesis after it -- it is never read as a
        // sub-query at all, under one pair of parentheses or two.
        for (String sql : Arrays.asList(
                "CREATE TABLE zzt4a_v1 (i int, k int) PARTITION BY RANGE ((VALUES (1)))",
                "CREATE TABLE zzt4a_v2 (i int, k int) PARTITION BY RANGE (VALUES (1))",
                "CREATE TABLE zzt4a_v3 (i int, k int) PARTITION BY LIST ((VALUES (1)))",
                "CREATE TABLE zzt4a_v4 (i int, k int) PARTITION BY HASH ((VALUES (1)))")) {
            assertEquals("42601", stateOf(sql), sql);
            assertEquals("syntax error at or near \"(\"", messageOf(sql), sql);
        }

        // Standing inside an expression, where the element's own parentheses have been spent
        // already, it is refused for the sub-query it is.
        String inner = "CREATE TABLE zzt4a_v5 (i int, k int)"
                + " PARTITION BY RANGE ((i + (VALUES (1))))";
        assertEquals("0A000", stateOf(inner));
        assertEquals("cannot use subquery in partition key expression", messageOf(inner));

        // The other sub-query spellings are untouched by any of this.
        String doubled = "CREATE TABLE zzt4a_v6 (i int, k int) PARTITION BY RANGE (((SELECT 1)))";
        assertEquals("0A000", stateOf(doubled));
        assertEquals("cannot use subquery in partition key expression", messageOf(doubled));

        String single = "CREATE TABLE zzt4a_v7 (i int, k int) PARTITION BY RANGE ((SELECT 1))";
        assertEquals("42601", stateOf(single));
        assertEquals("syntax error at or near \"SELECT\"", messageOf(single));

        assertEquals(0, num("SELECT count(*)::int FROM pg_class WHERE relname"
                + " IN ('zzt4a_v1','zzt4a_v2','zzt4a_v3','zzt4a_v4','zzt4a_v5','zzt4a_v6',"
                + "'zzt4a_v7')"));
    }
}
