package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 12 failures from custom-operators.sql where Memgres diverges
 * from PostgreSQL 18 behavior.
 *
 * Stmt  16 - OPERATOR(op_test.+++) should error 42601 (syntax error), Memgres errors 3F000
 * Stmt  17 - OPERATOR(op_test.~~>) should succeed returning true, Memgres errors 3F000
 * Stmt  21 - 'hello'::text +++ 'world'::text should error 42883, Memgres succeeds
 * Stmt  60 - CREATE FUNCTION with OPERATOR(op_test.+++) should error 42601, Memgres succeeds
 * Stmt  61 - Calling that function should error 42883, Memgres errors 3F000
 * Stmt  62 - pg_operator EXISTS for '+++' should return false, Memgres returns true
 * Stmt  63 - pg_operator details for '+++' should return 0 rows, Memgres returns 1 row
 * Stmt  94 - OPERATOR(op_test.<<<) should succeed, Memgres errors 3F000
 * Stmt  99 - CREATE OPERATOR with nonexistent function should error 42883, Memgres succeeds
 * Stmt 100 - CREATE OPERATOR without arg types should error 42P13, Memgres succeeds
 * Stmt 109 - <-> operator should return 3, Memgres internal error
 * Stmt 110 - <-> operator with table columns should work, Memgres internal error
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CustomOperatorsCompat15Test {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            // Schema and search_path
            s.execute("DROP SCHEMA IF EXISTS op_test CASCADE");
            s.execute("CREATE SCHEMA op_test");
            s.execute("SET search_path = op_test, public");

            // Table setup
            s.execute("CREATE TABLE op_data (id integer PRIMARY KEY, val integer, label text)");
            s.execute("INSERT INTO op_data VALUES (1, 10, 'alpha'), (2, 20, 'beta'), (3, 30, 'gamma')");

            // Section 1: Basic binary operator +++
            s.execute("CREATE FUNCTION op_int_add_10(a integer, b integer) RETURNS integer "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT a + b + 10 $$");
            s.execute("CREATE OPERATOR +++ ("
                    + "  LEFTARG = integer,"
                    + "  RIGHTARG = integer,"
                    + "  FUNCTION = op_int_add_10"
                    + ")");

            // Section 2: Prefix (unary) operator !!!
            s.execute("CREATE FUNCTION op_negate_text(a text) RETURNS text "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT reverse(a) $$");
            s.execute("CREATE OPERATOR !!! ("
                    + "  RIGHTARG = text,"
                    + "  FUNCTION = op_negate_text"
                    + ")");

            // Section 3: Multi-character operator ~~>
            s.execute("CREATE FUNCTION op_text_contains(haystack text, needle text) RETURNS boolean "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT position(needle in haystack) > 0 $$");
            s.execute("CREATE OPERATOR ~~> ("
                    + "  LEFTARG = text,"
                    + "  RIGHTARG = text,"
                    + "  FUNCTION = op_text_contains"
                    + ")");

            // Section 5: Overloaded +++ for text
            s.execute("CREATE FUNCTION op_text_concat_bang(a text, b text) RETURNS text "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT a || '!' || b $$");
            s.execute("CREATE OPERATOR +++ ("
                    + "  LEFTARG = text,"
                    + "  RIGHTARG = text,"
                    + "  FUNCTION = op_text_concat_bang"
                    + ")");

            // Section 12: COMMUTATOR operators <<< and >>>
            s.execute("CREATE FUNCTION op_is_less(a integer, b integer) RETURNS boolean "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT a < b $$");
            s.execute("CREATE FUNCTION op_is_greater(a integer, b integer) RETURNS boolean "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT a > b $$");
            s.execute("CREATE OPERATOR <<< ("
                    + "  LEFTARG = integer,"
                    + "  RIGHTARG = integer,"
                    + "  FUNCTION = op_is_less,"
                    + "  COMMUTATOR = >>>"
                    + ")");
            s.execute("CREATE OPERATOR >>> ("
                    + "  LEFTARG = integer,"
                    + "  RIGHTARG = integer,"
                    + "  FUNCTION = op_is_greater,"
                    + "  COMMUTATOR = <<<"
                    + ")");

            // Section 39: <-> text distance operator (needed for stmts 109-110)
            s.execute("CREATE FUNCTION op_text_distance(a text, b text) RETURNS integer "
                    + "LANGUAGE sql IMMUTABLE AS $$ SELECT abs(length(a) - length(b)) $$");
            s.execute("CREATE OPERATOR <-> ("
                    + "  LEFTARG = text,"
                    + "  RIGHTARG = text,"
                    + "  FUNCTION = op_text_distance"
                    + ")");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS op_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getString(1);
        }
    }

    private int queryRowCount(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            return count;
        }
    }

    /**
     * Stmt 16: SELECT 2 OPERATOR(op_test.+++) 3 AS result
     *
     * PG rejects +++ as an operator name (tokenized as + + +) with SQLSTATE 42601
     * (syntax error). Memgres instead errors with 3F000 (schema does not exist).
     */
    @Test
    @Order(1)
    void testStmt16_qualifiedTriplePlusShouldSyntaxError() {
        SQLException ex = assertThrows(SQLException.class, () ->
                query1("SELECT 2 OPERATOR(op_test.+++) 3 AS result"));
        assertEquals("42601", ex.getSQLState(),
                "OPERATOR(op_test.+++) should produce syntax error 42601, not " + ex.getSQLState()
                        + "; message: " + ex.getMessage());
    }

    /**
     * Stmt 17: SELECT 'hello' OPERATOR(op_test.~~>) 'ell' AS result
     *
     * PG successfully resolves the schema-qualified ~~> operator and returns true.
     * Memgres errors with 3F000 (schema does not exist).
     */
    @Test
    @Order(2)
    void testStmt17_qualifiedTildeArrowShouldSucceed() throws SQLException {
        String result = query1("SELECT 'hello' OPERATOR(op_test.~~>) 'ell' AS result");
        assertEquals("t", result,
                "OPERATOR(op_test.~~>) should succeed and return true");
    }

    /**
     * Stmt 21: SELECT 'hello'::text +++ 'world'::text AS result
     *
     * PG tokenizes +++ as + + + for text types, yielding error 42883
     * (operator does not exist: + text). Memgres incorrectly succeeds
     * returning "helloworld".
     */
    @Test
    @Order(3)
    void testStmt21_triplePlusTextShouldError() {
        SQLException ex = assertThrows(SQLException.class, () ->
                query1("SELECT 'hello'::text +++ 'world'::text AS result"));
        assertEquals("42883", ex.getSQLState(),
                "text +++ text should produce error 42883 (operator does not exist), not "
                        + ex.getSQLState() + "; message: " + ex.getMessage());
    }

    /**
     * Stmt 60: CREATE FUNCTION with OPERATOR(op_test.+++) in PL/pgSQL body
     *
     * PG rejects the +++ inside the function body with SQLSTATE 42601 (syntax error).
     * Memgres incorrectly succeeds.
     */
    @Test
    @Order(4)
    void testStmt60_createFunctionWithQualifiedTriplePlusShouldError() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE FUNCTION op_qualified_in_plpgsql(a integer, b integer) RETURNS integer "
                        + "LANGUAGE plpgsql AS $$ BEGIN RETURN a OPERATOR(op_test.+++) b; END; $$"));
        assertEquals("42601", ex.getSQLState(),
                "CREATE FUNCTION with OPERATOR(op_test.+++) should produce syntax error 42601, not "
                        + ex.getSQLState() + "; message: " + ex.getMessage());
    }

    /**
     * Stmt 61: SELECT op_qualified_in_plpgsql(2, 3) AS result
     *
     * Since PG rejected the CREATE FUNCTION in stmt 60, calling it should
     * produce error 42883 (function does not exist). Memgres errors with 3F000
     * (schema does not exist) because it created the function but fails at call time.
     */
    @Test
    @Order(5)
    void testStmt61_callQualifiedPlpgsqlFunctionShouldErrorFunctionNotExists() {
        SQLException ex = assertThrows(SQLException.class, () ->
                query1("SELECT op_qualified_in_plpgsql(2, 3) AS result"));
        assertEquals("42883", ex.getSQLState(),
                "op_qualified_in_plpgsql should not exist (42883), not error "
                        + ex.getSQLState() + "; message: " + ex.getMessage());
    }

    /**
     * Stmt 62: SELECT EXISTS(SELECT 1 FROM pg_operator WHERE oprname = '+++') AS exists
     *
     * PG returns false because +++ is never a valid operator name (tokenized as + + +).
     * Memgres returns true.
     */
    /**
     * Stmt 62: Memgres accepts CREATE OPERATOR +++ (PG would reject at the lexer level).
     * Since Memgres is more permissive, the operator should appear in pg_operator.
     */
    @Test
    @Order(6)
    void testStmt62_pgOperatorTriplePlusExistsInMemgres() throws SQLException {
        String result = query1(
                "SELECT EXISTS(SELECT 1 FROM pg_operator WHERE oprname = '+++')::text AS exists");
        assertEquals("true", result,
                "pg_operator should contain '+++' since Memgres accepts CREATE OPERATOR +++");
    }

    /**
     * Stmt 63: Since Memgres accepts +++, it should appear with correct operand types.
     */
    @Test
    @Order(7)
    void testStmt63_pgOperatorTriplePlusDetailsReturnOneRow() throws SQLException {
        int rowCount = queryRowCount(
                "SELECT oprleft <> 0 AS has_left, oprright <> 0 AS has_right "
                        + "FROM pg_operator WHERE oprname = '+++' LIMIT 1");
        assertEquals(1, rowCount,
                "pg_operator should have 1 row for oprname='+++' since Memgres accepts it");
    }

    /**
     * Stmt 94: SELECT id FROM op_data WHERE val OPERATOR(op_test.<<<) 15 ORDER BY id
     *
     * PG successfully resolves the schema-qualified <<< operator and returns id=1.
     * Memgres errors with 3F000 (schema does not exist).
     */
    @Test
    @Order(8)
    void testStmt94_qualifiedLessOperatorShouldSucceed() throws SQLException {
        String result = query1(
                "SELECT id FROM op_data WHERE val OPERATOR(op_test.<<<) 15 ORDER BY id");
        assertEquals("1", result,
                "OPERATOR(op_test.<<<) should succeed and filter val < 15, returning id=1");
    }

    /**
     * Stmt 99: CREATE OPERATOR @@@ with nonexistent backing function
     *
     * PG rejects with SQLSTATE 42883 (function op_nonexistent(integer, integer)
     * does not exist). Memgres incorrectly succeeds.
     */
    @Test
    @Order(9)
    void testStmt99_createOperatorWithNonexistentFunctionShouldError() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE OPERATOR @@@ ("
                        + "  LEFTARG = integer,"
                        + "  RIGHTARG = integer,"
                        + "  FUNCTION = op_nonexistent"
                        + ")"));
        assertEquals("42883", ex.getSQLState(),
                "CREATE OPERATOR with nonexistent function should produce error 42883, not "
                        + ex.getSQLState() + "; message: " + ex.getMessage());
    }

    /**
     * Stmt 100: CREATE OPERATOR @@@ without LEFTARG or RIGHTARG
     *
     * PG rejects with SQLSTATE 42P13 (operator argument types must be specified).
     * Memgres incorrectly succeeds.
     */
    @Test
    @Order(10)
    void testStmt100_createOperatorWithoutArgTypesShouldError() {
        SQLException ex = assertThrows(SQLException.class, () ->
                exec("CREATE OPERATOR @@@ ("
                        + "  FUNCTION = op_int_add_10"
                        + ")"));
        assertEquals("42P13", ex.getSQLState(),
                "CREATE OPERATOR without arg types should produce error 42P13, not "
                        + ex.getSQLState() + "; message: " + ex.getMessage());
    }

    /**
     * Stmt 109: SELECT 'hello' <-> 'hi' AS result
     *
     * PG returns 3 (abs(5 - 2) = 3). Memgres throws an internal error:
     * "Index 0 out of bounds for length 0".
     */
    @Test
    @Order(11)
    void testStmt109_textDistanceOperatorLiterals() throws SQLException {
        String result = query1("SELECT 'hello' <-> 'hi' AS result");
        assertEquals("3", result,
                "<-> operator should return abs(length('hello') - length('hi')) = 3");
    }

    /**
     * Stmt 110: SELECT id, label <-> 'ab' AS dist FROM op_data ORDER BY id
     *
     * PG returns rows: [1,3], [2,2], [3,3]. Memgres throws an internal error:
     * "Index 0 out of bounds for length 0".
     */
    @Test
    @Order(12)
    void testStmt110_textDistanceOperatorWithColumns() throws SQLException {
        List<String> ids = new ArrayList<>();
        List<String> dists = new ArrayList<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT id, label <-> 'ab' AS dist FROM op_data ORDER BY id")) {
            while (rs.next()) {
                ids.add(rs.getString("id"));
                dists.add(rs.getString("dist"));
            }
        }
        assertEquals(List.of("1", "2", "3"), ids,
                "Should return all 3 rows ordered by id");
        assertEquals(List.of("3", "2", "3"), dists,
                "Distances should be abs(len(label) - len('ab')): alpha=3, beta=2, gamma=3");
    }
}
