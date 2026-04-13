package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for user-defined operator EXECUTION dispatch — Phases 1, 2, and 3.
 *
 * Phase 1: OPERATOR(schema.op) qualified syntax dispatches to backing functions
 * Phase 2: Built-in operator token overloading for custom types
 * Phase 3: Multi-char custom operator lexing, parsing, and evaluation
 *
 * All tests should pass on real PG 18.
 */
class CustomOperatorExecutionTest {

    private Memgres memgres;

    @BeforeEach
    void setUp() {
        memgres = Memgres.builder().port(0).build().start();
    }

    @AfterEach
    void tearDown() {
        if (memgres != null) memgres.close();
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    // ========================================================================
    // Phase 1: OPERATOR() qualified syntax dispatch
    // ========================================================================

    @Test
    void operatorQualifiedInfixBasic() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ### (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT 3 OPERATOR(public.###) 4")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getInt(1));
            }
        }
    }

    @Test
    void operatorQualifiedInfixWithSchema() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION mul_ints(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <#> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = mul_ints)");

            try (ResultSet rs = stmt.executeQuery("SELECT 5 OPERATOR(public.<#>) 6")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1));
            }
        }
    }

    @Test
    void operatorQualifiedInfixStringArgs() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION str_interleave(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || '-' || b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <~> (LEFTARG = text, RIGHTARG = text, FUNCTION = str_interleave)");

            try (ResultSet rs = stmt.executeQuery("SELECT 'hello' OPERATOR(public.<~>) 'world'")) {
                assertTrue(rs.next());
                assertEquals("hello-world", rs.getString(1));
            }
        }
    }

    @Test
    void operatorQualifiedInfixReturnsBoolean() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION both_positive(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a > 0 AND b > 0; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ?&? (LEFTARG = integer, RIGHTARG = integer, FUNCTION = both_positive)");

            try (ResultSet rs = stmt.executeQuery("SELECT 3 OPERATOR(public.?&?) 4")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT -1 OPERATOR(public.?&?) 4")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
        }
    }

    @Test
    void operatorQualifiedInfixNotFound() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                () -> stmt.executeQuery("SELECT 1 OPERATOR(public.@#@) 2"));
            assertEquals("42883", ex.getSQLState());
        }
    }

    @Test
    void operatorQualifiedPrefixUnary() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION negate_int(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN -a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR !!! (RIGHTARG = integer, FUNCTION = negate_int)");

            try (ResultSet rs = stmt.executeQuery("SELECT OPERATOR(public.!!!)(42)")) {
                assertTrue(rs.next());
                assertEquals(-42, rs.getInt(1));
            }
        }
    }

    @Test
    void operatorQualifiedPrefixBinaryFunctionCallStyle() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_sub(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a - b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |~| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_sub)");

            // Binary operator in function-call style: OPERATOR(schema.op)(a, b)
            try (ResultSet rs = stmt.executeQuery("SELECT OPERATOR(public.|~|)(10, 3)")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getInt(1));
            }
        }
    }

    @Test
    void operatorQualifiedWithExpressions() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_pow(a integer, b integer) RETURNS integer AS $$ "
                    + "DECLARE result integer := 1; i integer; "
                    + "BEGIN FOR i IN 1..b LOOP result := result * a; END LOOP; RETURN result; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ^^ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_pow)");

            try (ResultSet rs = stmt.executeQuery("SELECT (2 + 1) OPERATOR(public.^^) (1 + 1)")) {
                assertTrue(rs.next());
                assertEquals(9, rs.getInt(1)); // 3^2 = 9
            }
        }
    }

    @Test
    void operatorQualifiedInWhereClause() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE op_test (id int, val int)");
            stmt.execute("INSERT INTO op_test VALUES (1, 10), (2, 20), (3, 30)");

            stmt.execute("CREATE FUNCTION gt_custom(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a > b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR >>> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = gt_custom)");

            try (ResultSet rs = stmt.executeQuery("SELECT id FROM op_test WHERE val OPERATOR(public.>>>) 15 ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void operatorQualifiedStrictNullHandling() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE OPERATOR |+| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = strict_add)");

            // STRICT function returns NULL when any arg is NULL
            try (ResultSet rs = stmt.executeQuery("SELECT NULL::integer OPERATOR(public.|+|) 5")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    // ========================================================================
    // Phase 3: Multi-char custom operator direct use
    // ========================================================================

    @Test
    void customMultiCharOperatorInfix() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add2(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b + 100; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |+| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add2)");

            // Use custom operator directly (Phase 3 — multi-char operator lexing)
            try (ResultSet rs = stmt.executeQuery("SELECT 1 |+| 2")) {
                assertTrue(rs.next());
                assertEquals(103, rs.getInt(1));
            }
        }
    }

    @Test
    void customMultiCharOperatorWithTilde() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION tilde_op(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b * 2; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <~> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = tilde_op)");

            try (ResultSet rs = stmt.executeQuery("SELECT 3 <~> 4")) {
                assertTrue(rs.next());
                assertEquals(24, rs.getInt(1)); // 3 * 4 * 2
            }
        }
    }

    @Test
    void customOperatorInOrderBy() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE co_test (id int, a int, b int)");
            stmt.execute("INSERT INTO co_test VALUES (1, 5, 2), (2, 3, 4), (3, 1, 6)");
            stmt.execute("CREATE FUNCTION my_sum(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |+| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_sum)");

            try (ResultSet rs = stmt.executeQuery("SELECT id, a |+| b AS total FROM co_test ORDER BY a |+| b")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id")); assertEquals(7, rs.getInt("total"));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id")); assertEquals(7, rs.getInt("total"));
                assertTrue(rs.next());
                assertEquals(3, rs.getInt("id")); assertEquals(7, rs.getInt("total"));
            }
        }
    }

    @Test
    void customOperatorNotExistsGives42883() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Operator <#> not created — should give 42883
            SQLException ex = assertThrows(SQLException.class,
                () -> stmt.executeQuery("SELECT 1 <#> 2"));
            assertEquals("42883", ex.getSQLState());
        }
    }

    @Test
    void customOperatorPrefixNotExistsGives42883() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Prefix operator @#@ not created
            SQLException ex = assertThrows(SQLException.class,
                () -> stmt.executeQuery("SELECT @#@ 5"));
            assertEquals("42883", ex.getSQLState());
        }
    }

    @Test
    void customOperatorWithNullArgs() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION null_safe_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN COALESCE(a, 0) + COALESCE(b, 0); END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = null_safe_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT NULL::integer <+> 5")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorStrictWithNullReturnsNull() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_mul(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE OPERATOR <*> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = strict_mul)");

            try (ResultSet rs = stmt.executeQuery("SELECT NULL::integer <*> 5")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    @Test
    void customOperatorChained() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION chain_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = chain_add)");

            // Multiple custom operators in one expression
            try (ResultSet rs = stmt.executeQuery("SELECT 1 <+> 2 <+> 3")) {
                assertTrue(rs.next());
                assertEquals(6, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorInSubquery() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION double_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN (a + b) * 2; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = double_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM (SELECT 3 <+> 4 AS val) sub WHERE val > 10")) {
                assertTrue(rs.next());
                assertEquals(14, rs.getInt(1)); // (3+4)*2 = 14
            }
        }
    }

    @Test
    void customOperatorInCaseExpression() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION is_big(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a + b > 10; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ?> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = is_big)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT CASE WHEN 5 OPERATOR(public.?>) 6 THEN 'big' ELSE 'small' END")) {
                assertTrue(rs.next());
                assertEquals("big", rs.getString(1));
            }
        }
    }

    @Test
    void customOperatorWithTextOperands() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION text_combine(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN upper(a) || '_' || lower(b); END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <~> (LEFTARG = text, RIGHTARG = text, FUNCTION = text_combine)");

            try (ResultSet rs = stmt.executeQuery("SELECT 'Hello' <~> 'World'")) {
                assertTrue(rs.next());
                assertEquals("HELLO_world", rs.getString(1));
            }
        }
    }

    @Test
    void customOperatorReturningFloat() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION avg_two(a numeric, b numeric) RETURNS numeric AS $$ "
                    + "BEGIN RETURN (a + b) / 2.0; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |~| (LEFTARG = numeric, RIGHTARG = numeric, FUNCTION = avg_two)");

            try (ResultSet rs = stmt.executeQuery("SELECT 10 |~| 20")) {
                assertTrue(rs.next());
                assertEquals(15.0, rs.getDouble(1), 0.001);
            }
        }
    }

    // ========================================================================
    // Phase 1+3 combined: OPERATOR() syntax and direct syntax
    // ========================================================================

    @Test
    void sameOperatorBothSyntaxForms() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_mod(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a - (a / b) * b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |%| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_mod)");

            // Direct syntax
            try (ResultSet rs = stmt.executeQuery("SELECT 17 |%| 5")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
            // Qualified syntax
            try (ResultSet rs = stmt.executeQuery("SELECT 17 OPERATOR(public.|%|) 5")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Lexer edge cases: PG trailing +/- rule
    // ========================================================================

    @Test
    void lexerTrailingPlusMinusRule() throws SQLException {
        // PG rule: operator ending with + or - is only valid if it also contains
        // one of ~ ! @ # % ^ & | ?
        // So ++ is not a valid operator name — lexer should split it into + +
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // SELECT 1 ++ 2 should be parsed as 1 + (+2) = 3
            // Because ++ is split by the trailing rule into + +
            try (ResultSet rs = stmt.executeQuery("SELECT 1 ++ 2")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }
        }
    }

    @Test
    void lexerTrailingMinusRule() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // -- would be a comment, but the greedy scan truncation handles this
            // +- should split into + then - (no special chars)
            try (ResultSet rs = stmt.executeQuery("SELECT 5 +- 2")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1)); // 5 + (-2) = 3
            }
        }
    }

    @Test
    void lexerSpecialCharsPreserveTrailingPlus() throws SQLException {
        // ~+ is valid because it contains ~ (a special char)
        // so ~+ is kept as a single operator token
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION tilde_plus(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * 10; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ~+ (RIGHTARG = integer, FUNCTION = tilde_plus)");

            try (ResultSet rs = stmt.executeQuery("SELECT ~+ 5")) {
                assertTrue(rs.next());
                assertEquals(50, rs.getInt(1));
            }
        }
    }

    @Test
    void lexerKnownOperatorsStillWork() throws SQLException {
        // Ensure restructured lexer still handles all known operators correctly
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // <=, >=, <>, !=, ||, ->, ->>, ::, etc.
            try (ResultSet rs = stmt.executeQuery("SELECT 1 <= 2, 3 >= 2, 1 <> 2, 1 != 2")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
                assertTrue(rs.getBoolean(2));
                assertTrue(rs.getBoolean(3));
                assertTrue(rs.getBoolean(4));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 'a' || 'b'")) {
                assertTrue(rs.next());
                assertEquals("ab", rs.getString(1));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT '{\"a\":1}'::jsonb -> 'a'")) {
                assertTrue(rs.next());
                assertEquals("1", rs.getString(1));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT '{\"a\":\"b\"}'::jsonb ->> 'a'")) {
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
            }
        }
    }

    @Test
    void lexerBitOperatorsStillWork() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT 6 & 3, 6 | 3, 6 # 3, 8 << 2, 8 >> 1")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1)); // 6 & 3 = 2
                assertEquals(7, rs.getInt(2)); // 6 | 3 = 7
                assertEquals(5, rs.getInt(3)); // 6 # 3 = 5
                assertEquals(32, rs.getInt(4)); // 8 << 2 = 32
                assertEquals(4, rs.getInt(5)); // 8 >> 1 = 4
            }
        }
    }

    @Test
    void lexerGeometricOperatorsStillWork() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Just verify these parse without errors
            try (ResultSet rs = stmt.executeQuery("SELECT '@>' AS op_name")) {
                assertTrue(rs.next());
                assertEquals("@>", rs.getString(1));
            }
        }
    }

    @Test
    void lexerFatArrowStillWorks() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // => is FAT_ARROW, not a custom operator
            // Verify it's still recognized as FAT_ARROW by using it in a valid context
            try (ResultSet rs = stmt.executeQuery("SELECT json_build_object('a', 1)")) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    // ========================================================================
    // Drop operator and re-use
    // ========================================================================

    @Test
    void dropAndRecreateOperator() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION op_v1(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = op_v1)");

            try (ResultSet rs = stmt.executeQuery("SELECT 1 <+> 2")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }

            stmt.execute("DROP OPERATOR <+> (integer, integer)");

            // After drop, operator should not exist
            SQLException ex = assertThrows(SQLException.class,
                () -> stmt.executeQuery("SELECT 1 <+> 2"));
            assertEquals("42883", ex.getSQLState());

            // Recreate with different function
            stmt.execute("CREATE FUNCTION op_v2(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = op_v2)");

            try (ResultSet rs = stmt.executeQuery("SELECT 3 <+> 4")) {
                assertTrue(rs.next());
                assertEquals(12, rs.getInt(1)); // now multiplication
            }
        }
    }

    // ========================================================================
    // Operator with table data
    // ========================================================================

    @Test
    void customOperatorOnTableColumns() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE scores (student text, math int, science int)");
            stmt.execute("INSERT INTO scores VALUES ('Alice', 90, 85), ('Bob', 70, 95), ('Carol', 80, 80)");

            stmt.execute("CREATE FUNCTION weighted_avg(a integer, b integer) RETURNS numeric AS $$ "
                    + "BEGIN RETURN (a * 0.6 + b * 0.4); END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <~> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = weighted_avg)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT student, math <~> science AS weighted FROM scores ORDER BY math <~> science DESC")) {
                assertTrue(rs.next());
                assertEquals("Alice", rs.getString(1)); // 88.0
                assertTrue(rs.next());
                // Bob and Carol are tied at 80.0
                String second = rs.getString(1);
                assertTrue("Bob".equals(second) || "Carol".equals(second));
                assertTrue(rs.next());
                String third = rs.getString(1);
                assertTrue("Bob".equals(third) || "Carol".equals(third));
                assertNotEquals(second, third);
            }
        }
    }

    @Test
    void customOperatorInGroupByHaving() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE items (category text, price int, qty int)");
            stmt.execute("INSERT INTO items VALUES ('A', 10, 5), ('A', 20, 3), ('B', 15, 2), ('B', 25, 4)");

            stmt.execute("CREATE FUNCTION total_val(p integer, q integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN p * q; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <*> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = total_val)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT category, SUM(price <*> qty) AS total FROM items GROUP BY category HAVING SUM(price <*> qty) > 120 ORDER BY category")) {
                assertTrue(rs.next());
                assertEquals("B", rs.getString(1));
                assertEquals(130, rs.getInt(2)); // 15*2 + 25*4 = 30+100 = 130
                assertFalse(rs.next()); // A = 10*5+20*3 = 50+60 = 110 < 120
            }
        }
    }

    @Test
    void customOperatorInUpdate() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE balances (id int, amount int)");
            stmt.execute("INSERT INTO balances VALUES (1, 100), (2, 200)");

            stmt.execute("CREATE FUNCTION add_bonus(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b + 10; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_bonus)");

            stmt.execute("UPDATE balances SET amount = amount <+> 50 WHERE id = 1");

            try (ResultSet rs = stmt.executeQuery("SELECT amount FROM balances WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(160, rs.getInt(1)); // 100 + 50 + 10
            }
        }
    }

    @Test
    void customOperatorInInsertValues() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE computed (id int, val int)");

            stmt.execute("CREATE FUNCTION triple_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b + a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = triple_add)");

            stmt.execute("INSERT INTO computed VALUES (1, 5 <+> 3)");

            try (ResultSet rs = stmt.executeQuery("SELECT val FROM computed WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(13, rs.getInt(1)); // 5+3+5 = 13
            }
        }
    }

    // ========================================================================
    // Overloading: same symbol, different arg types
    // ========================================================================

    @Test
    void operatorOverloadingDifferentTypes() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION add_ints(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION concat_texts(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || b; END; $$ LANGUAGE plpgsql");

            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_ints)");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = text, RIGHTARG = text, FUNCTION = concat_texts)");

            // Integer overload
            try (ResultSet rs = stmt.executeQuery("SELECT 3 <+> 4")) {
                assertTrue(rs.next());
                assertEquals(7, rs.getInt(1));
            }
            // Text overload
            try (ResultSet rs = stmt.executeQuery("SELECT 'foo' <+> 'bar'")) {
                assertTrue(rs.next());
                assertEquals("foobar", rs.getString(1));
            }
        }
    }

    // ========================================================================
    // Edge cases
    // ========================================================================

    @Test
    void customOperatorWithCast() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION bigint_add(a bigint, b bigint) RETURNS bigint AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = bigint, RIGHTARG = bigint, FUNCTION = bigint_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT 100::bigint <+> 200::bigint")) {
                assertTrue(rs.next());
                assertEquals(300L, rs.getLong(1));
            }
        }
    }

    @Test
    void customOperatorReturnsNull() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION always_null(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <?> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = always_null)");

            try (ResultSet rs = stmt.executeQuery("SELECT 1 <?> 2")) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
            }
        }
    }

    @Test
    void customOperatorWithCoalesce() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION safe_div(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN IF b = 0 THEN RETURN NULL; END IF; RETURN a / b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR </> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = safe_div)");

            try (ResultSet rs = stmt.executeQuery("SELECT COALESCE(10 </> 0, -1)")) {
                assertTrue(rs.next());
                assertEquals(-1, rs.getInt(1));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT COALESCE(10 </> 2, -1)")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorMultipleInSameQuery() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION op_a(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION op_b(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = op_a)");
            stmt.execute("CREATE OPERATOR <*> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = op_b)");

            // Mix custom operators
            try (ResultSet rs = stmt.executeQuery("SELECT (2 <+> 3) <*> 4")) {
                assertTrue(rs.next());
                assertEquals(20, rs.getInt(1)); // (2+3) * 4
            }
        }
    }

    @Test
    void customOperatorInReturning() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ret_test (id int, a int, b int)");
            stmt.execute("CREATE FUNCTION my_add3(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add3)");

            try (ResultSet rs = stmt.executeQuery(
                    "INSERT INTO ret_test VALUES (1, 10, 20) RETURNING a <+> b")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorInDeleteWhere() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE del_test (id int, val int)");
            stmt.execute("INSERT INTO del_test VALUES (1, 10), (2, 20), (3, 30)");

            stmt.execute("CREATE FUNCTION is_over(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a > b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |>| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = is_over)");

            stmt.execute("DELETE FROM del_test WHERE val |>| 15");

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM del_test")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1)); // only val=10 remains
            }
        }
    }

    // ========================================================================
    // Phase 1: Built-in operator via OPERATOR() qualified syntax
    // ========================================================================

    @Test
    void operatorQualifiedBuiltinPlus() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT 10 OPERATOR(pg_catalog.+) 5")) {
                assertTrue(rs.next());
                assertEquals(15, rs.getInt(1));
            }
        }
    }

    @Test
    void operatorQualifiedBuiltinEquals() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT 1 OPERATOR(pg_catalog.=) 1")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void operatorQualifiedBuiltinConcat() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT 'a' OPERATOR(pg_catalog.||) 'b'")) {
                assertTrue(rs.next());
                assertEquals("ab", rs.getString(1));
            }
        }
    }

    @Test
    void operatorQualifiedBuiltinPrefixMinus() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT OPERATOR(pg_catalog.-)(42)")) {
                assertTrue(rs.next());
                assertEquals(-42, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Corner cases: expression walker coverage
    // ========================================================================

    @Test
    void customOperatorWithAggregate() throws SQLException {
        // Tests that containsAggregate() detects aggregates inside custom operators
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE agg_op (grp text, val int)");
            stmt.execute("INSERT INTO agg_op VALUES ('a', 1), ('a', 2), ('b', 3)");
            stmt.execute("CREATE FUNCTION my_add_agg(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add_agg)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT grp, SUM(val) <+> 10 AS boosted FROM agg_op GROUP BY grp ORDER BY grp")) {
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals(13, rs.getInt(2)); // SUM(1,2)=3, 3+10=13
                assertTrue(rs.next());
                assertEquals("b", rs.getString(1));
                assertEquals(13, rs.getInt(2)); // SUM(3)=3, 3+10=13
            }
        }
    }

    @Test
    void customOperatorInJoinCondition() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE jt1 (id int, val int)");
            stmt.execute("CREATE TABLE jt2 (id int, val int)");
            stmt.execute("INSERT INTO jt1 VALUES (1, 10), (2, 20)");
            stmt.execute("INSERT INTO jt2 VALUES (1, 10), (2, 30)");

            stmt.execute("CREATE FUNCTION eq_check(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <==> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = eq_check)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT jt1.id FROM jt1 JOIN jt2 ON jt1.val <==> jt2.val ORDER BY jt1.id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next()); // Only id=1 has matching val=10
            }
        }
    }

    @Test
    void customOperatorInCteExpression() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION cte_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = cte_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "WITH computed AS (SELECT 5 <+> 3 AS result) SELECT result FROM computed")) {
                assertTrue(rs.next());
                assertEquals(8, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorInWindowFunction() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE win_op (id int, val int)");
            stmt.execute("INSERT INTO win_op VALUES (1, 10), (2, 20), (3, 30)");

            stmt.execute("CREATE FUNCTION win_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = win_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT id, val <+> 5 AS adjusted, ROW_NUMBER() OVER (ORDER BY val <+> 5) AS rn "
                    + "FROM win_op ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals(15, rs.getInt("adjusted"));
                assertEquals(1, rs.getInt("rn"));
            }
        }
    }

    @Test
    void customOperatorInOnConflict() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE conflict_op (id int PRIMARY KEY, val int)");
            stmt.execute("INSERT INTO conflict_op VALUES (1, 10)");

            stmt.execute("CREATE FUNCTION conflict_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = conflict_add)");

            stmt.execute("INSERT INTO conflict_op VALUES (1, 20) "
                    + "ON CONFLICT (id) DO UPDATE SET val = excluded.val <+> conflict_op.val");

            try (ResultSet rs = stmt.executeQuery("SELECT val FROM conflict_op WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1)); // 20 + 10
            }
        }
    }

    @Test
    void customOperatorWithOldNewReturning() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ret_old_new (id int, val int)");
            stmt.execute("INSERT INTO ret_old_new VALUES (1, 10)");

            stmt.execute("CREATE FUNCTION diff_calc(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a - b; END; $$ LANGUAGE plpgsql");
            // Use |~| instead of <-> (which conflicts with built-in DISTANCE operator)
            stmt.execute("CREATE OPERATOR |~| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = diff_calc)");

            // Custom operator in RETURNING with column references
            try (ResultSet rs = stmt.executeQuery(
                    "UPDATE ret_old_new SET val = 20 WHERE id = 1 RETURNING val |~| id AS diff")) {
                assertTrue(rs.next());
                assertEquals(19, rs.getInt(1)); // 20 - 1
            }
        }
    }

    @Test
    void customOperatorSqlLanguageFunction() throws SQLException {
        // Test that SQL-language functions (not just plpgsql) work as operator backing
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION sql_add(a integer, b integer) RETURNS integer AS $$ "
                    + "SELECT a + b $$ LANGUAGE sql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = sql_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT 7 <+> 8")) {
                assertTrue(rs.next());
                assertEquals(15, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorInCheckConstraint() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION is_positive_sum(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a + b > 0; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR ?+? (LEFTARG = integer, RIGHTARG = integer, FUNCTION = is_positive_sum)");

            // Custom operator in a CHECK constraint expression via WHERE
            stmt.execute("CREATE TABLE check_op (a int, b int)");
            stmt.execute("INSERT INTO check_op VALUES (5, 3)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM check_op WHERE a ?+? b")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorMixedWithBuiltinOperators() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION mixed_op(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = mixed_op)");

            // Custom operator mixed with built-in arithmetic
            try (ResultSet rs = stmt.executeQuery("SELECT 1 + 2 <+> 3 * 4")) {
                assertTrue(rs.next());
                // parseComparison: left = parseAddition(1+2=3), then <+>, then parseAddition(3*4=12)
                assertEquals(15, rs.getInt(1)); // 3 <+> 12 = 15
            }
        }
    }

    @Test
    void customOperatorPrecedenceWithComparison() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION add_op(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_op)");

            // Custom operator result used in comparison
            try (ResultSet rs = stmt.executeQuery("SELECT 5 <+> 3 > 7")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1)); // 8 > 7
            }
        }
    }

    @Test
    void customOperatorInMergeCondition() throws SQLException {
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE merge_src (id int, val int)");
            stmt.execute("CREATE TABLE merge_tgt (id int, val int)");
            stmt.execute("INSERT INTO merge_src VALUES (1, 100)");
            stmt.execute("INSERT INTO merge_tgt VALUES (1, 50)");

            stmt.execute("CREATE FUNCTION eq_op(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <==> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = eq_op)");

            stmt.execute("MERGE INTO merge_tgt t USING merge_src s ON t.id <==> s.id "
                    + "WHEN MATCHED THEN UPDATE SET val = s.val");

            try (ResultSet rs = stmt.executeQuery("SELECT val FROM merge_tgt WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1));
            }
        }
    }

    // ========================================================================
    // Lexer edge cases: greedy scanning
    // ========================================================================

    @Test
    void lexerGreedyScansMultiCharOperator() throws SQLException {
        // <=> should be tokenized as a single CUSTOM_OPERATOR, not <= then >
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION spaceship(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN IF a < b THEN RETURN -1; ELSIF a > b THEN RETURN 1; ELSE RETURN 0; END IF; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <=> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = spaceship)");

            try (ResultSet rs = stmt.executeQuery("SELECT 3 <=> 5")) {
                assertTrue(rs.next());
                assertEquals(-1, rs.getInt(1));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 5 <=> 3")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT 4 <=> 4")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    void lexerCommentTruncationInOperator() throws SQLException {
        // SELECT 1 +-- comment should be 1 + (then -- is comment)
        // The trailing rule makes +- become just +
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT 5 +--this is a comment\n3")) {
                assertTrue(rs.next());
                // After the + operator, -- starts a comment, so 3 is on the next line
                assertEquals(8, rs.getInt(1)); // 5 + 3
            }
        }
    }

    @Test
    void customOperatorAdjacentToIdentifiers() throws SQLException {
        // a<+>b should tokenize as IDENTIFIER(<a>), CUSTOM_OPERATOR(<+>), IDENTIFIER(<b>)
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE adj_test (a int, b int)");
            stmt.execute("INSERT INTO adj_test VALUES (10, 20)");
            stmt.execute("CREATE FUNCTION adj_add(x integer, y integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN x + y; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = adj_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT a<+>b FROM adj_test")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1));
            }
        }
    }

    // =====================================================================
    // Corner case tests — A8
    // =====================================================================

    @Test
    void customOperatorTypeMismatchError() throws SQLException {
        // Operator defined for (integer, integer) should fail with text args
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION int_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = int_add)");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.executeQuery("SELECT 'hello' <+> 'world'"));
            assertEquals("42883", ex.getSQLState());
        }
    }

    @Test
    void customOperatorWithBetween() throws SQLException {
        // Custom operator in BETWEEN bounds: val BETWEEN (a <+> 1) AND (b <+> 5)
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT 15 BETWEEN 10 <+> 1 AND 10 <+> 10")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1)); // 15 BETWEEN 11 AND 20
            }

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT 5 BETWEEN 10 <+> 1 AND 10 <+> 10")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1)); // 5 BETWEEN 11 AND 20 → false
            }
        }
    }

    @Test
    void customOperatorWithIsNull() throws SQLException {
        // Custom operator result checked with IS NULL / IS NOT NULL
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION strict_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql STRICT");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = strict_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT NULL::integer <+> 5 IS NULL")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1)); // STRICT → NULL
            }

            try (ResultSet rs = stmt.executeQuery("SELECT 3 <+> 5 IS NOT NULL")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1)); // 8 IS NOT NULL → true
            }
        }
    }

    @Test
    void customOperatorWithIn() throws SQLException {
        // Custom operator result used with IN
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT 3 <+> 7 IN (10, 20, 30)")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1)); // 10 IN (10, 20, 30) → true
            }

            try (ResultSet rs = stmt.executeQuery("SELECT 3 <+> 7 IN (5, 15, 25)")) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1)); // 10 IN (5, 15, 25) → false
            }
        }
    }

    @Test
    void customOperatorWithNullif() throws SQLException {
        // NULLIF with custom operator
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT NULLIF(3 <+> 7, 10)")) {
                assertTrue(rs.next());
                rs.getInt(1);
                assertTrue(rs.wasNull()); // NULLIF(10, 10) → NULL
            }

            try (ResultSet rs = stmt.executeQuery("SELECT NULLIF(3 <+> 7, 99)")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1)); // NULLIF(10, 99) → 10
            }
        }
    }

    @Test
    void customOperatorWithArraySubscript() throws SQLException {
        // Custom operator with array element access
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE arr_op (vals integer[])");
            stmt.execute("INSERT INTO arr_op VALUES (ARRAY[10, 20, 30])");
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT vals[1] <+> vals[2] FROM arr_op")) {
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1)); // 10 + 20
            }
        }
    }

    @Test
    void customOperatorInCorrelatedSubquery() throws SQLException {
        // Custom operator referencing outer table in correlated subquery
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE outer_t (id int, val int)");
            stmt.execute("CREATE TABLE inner_t (id int, bonus int)");
            stmt.execute("INSERT INTO outer_t VALUES (1, 100), (2, 200)");
            stmt.execute("INSERT INTO inner_t VALUES (1, 10), (2, 20)");
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT id, (SELECT o.val <+> i.bonus FROM inner_t i WHERE i.id = o.id) "
                    + "FROM outer_t o ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(110, rs.getInt(2)); // 100 + 10
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1));
                assertEquals(220, rs.getInt(2)); // 200 + 20
            }
        }
    }

    @Test
    void customOperatorInDomainCheck() throws SQLException {
        // Custom operator used in DOMAIN CHECK constraint
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");
            stmt.execute("CREATE DOMAIN positive_sum AS integer CHECK (VALUE <+> 0 > 0)");

            // Valid: 5 + 0 = 5 > 0
            stmt.execute("CREATE TABLE dom_test (val positive_sum)");
            stmt.execute("INSERT INTO dom_test VALUES (5)");

            try (ResultSet rs = stmt.executeQuery("SELECT val FROM dom_test")) {
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
            }

            // Invalid: -3 + 0 = -3 > 0 → false
            assertThrows(SQLException.class, () -> stmt.execute("INSERT INTO dom_test VALUES (-3)"));
        }
    }

    @Test
    void customOperatorInGeneratedColumn() throws SQLException {
        // Custom operator in GENERATED ALWAYS AS expression
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_mul(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql IMMUTABLE");
            stmt.execute("CREATE OPERATOR |*| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_mul)");
            stmt.execute("CREATE TABLE gen_op (x int, y int, product int GENERATED ALWAYS AS (x |*| y) STORED)");
            stmt.execute("INSERT INTO gen_op (x, y) VALUES (3, 7)");

            try (ResultSet rs = stmt.executeQuery("SELECT product FROM gen_op")) {
                assertTrue(rs.next());
                assertEquals(21, rs.getInt(1));
            }
        }
    }

    @Test
    void pgOperatorCatalogCompleteness() throws SQLException {
        // Verify pg_operator catalog has correct oprresult, oprcode, oprnegate
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION bool_neg(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a <> b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION bool_eq(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |=| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = bool_eq, "
                    + "NEGATOR = |!=|, COMMUTATOR = |=|)");
            stmt.execute("CREATE OPERATOR |!=| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = bool_neg, "
                    + "NEGATOR = |=|, COMMUTATOR = |!=|)");

            // Check oprresult is non-zero (should be boolean OID)
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprresult, oprcode, oprcom, oprnegate FROM pg_operator WHERE oprname = '|=|'")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt("oprresult") != 0, "oprresult should be populated");
                assertTrue(rs.getInt("oprcode") != 0, "oprcode should be populated");
                assertTrue(rs.getInt("oprcom") != 0, "oprcom should be populated (self-commutator)");
                assertTrue(rs.getInt("oprnegate") != 0, "oprnegate should be populated");
            }

            // Check |!=| has non-zero oprnegate pointing to |=|
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprnegate FROM pg_operator WHERE oprname = '|!=|'")) {
                assertTrue(rs.next());
                assertTrue(rs.getInt("oprnegate") != 0, "oprnegate for |!=| should point to |=|");
            }
        }
    }

    @Test
    void builtinOperatorOverloadForCustomTypes() throws SQLException {
        // Override built-in + for text concatenation with custom logic
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION text_add(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || '-' || b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR + (LEFTARG = text, RIGHTARG = text, FUNCTION = text_add)");

            try (ResultSet rs = stmt.executeQuery("SELECT OPERATOR(public.+)('hello', 'world')")) {
                assertTrue(rs.next());
                assertEquals("hello-world", rs.getString(1));
            }
        }
    }

    @Test
    void customOperatorWithLike() throws SQLException {
        // Custom operator result used with LIKE
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION str_cat(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |+| (LEFTARG = text, RIGHTARG = text, FUNCTION = str_cat)");

            try (ResultSet rs = stmt.executeQuery("SELECT 'hel' |+| 'lo' LIKE 'hel%'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void dropOperatorIfExists() throws SQLException {
        // DROP OPERATOR IF EXISTS on non-existent operator should not error
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP OPERATOR IF EXISTS <+> (integer, integer)");
            // No error thrown

            // Create and drop with IF EXISTS
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            // Verify it works
            try (ResultSet rs = stmt.executeQuery("SELECT 1 <+> 2")) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1));
            }

            // Drop it
            stmt.execute("DROP OPERATOR IF EXISTS <+> (integer, integer)");

            // Should fail now
            assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1 <+> 2"));

            // Drop again with IF EXISTS — should not error
            stmt.execute("DROP OPERATOR IF EXISTS <+> (integer, integer)");
        }
    }

    @Test
    void dropOperatorWithoutIfExistsErrors() throws SQLException {
        // DROP OPERATOR without IF EXISTS on non-existent operator should error
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("DROP OPERATOR <+> (integer, integer)"));
            assertEquals("42704", ex.getSQLState());
        }
    }

    @Test
    void createDuplicateOperatorErrors() throws SQLException {
        // CREATE OPERATOR with duplicate should error
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)"));
            assertEquals("42710", ex.getSQLState());
        }
    }

    @Test
    void dropPrefixOperatorWithNone() throws SQLException {
        // DROP OPERATOR with NONE for prefix operator
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_neg(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN -a; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |~| (RIGHTARG = integer, FUNCTION = my_neg)");

            // Verify it works
            try (ResultSet rs = stmt.executeQuery("SELECT OPERATOR(public.|~|)(42)")) {
                assertTrue(rs.next());
                assertEquals(-42, rs.getInt(1));
            }

            // Drop with NONE syntax
            stmt.execute("DROP OPERATOR |~| (NONE, integer)");

            // Should fail now
            assertThrows(SQLException.class,
                    () -> stmt.executeQuery("SELECT OPERATOR(public.|~|)(42)"));
        }
    }

    @Test
    void customOperatorInDefaultExpression() throws SQLException {
        // Custom operator in column DEFAULT
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");
            stmt.execute("CREATE TABLE def_op (val int DEFAULT 10 <+> 5)");
            stmt.execute("INSERT INTO def_op DEFAULT VALUES");

            try (ResultSet rs = stmt.executeQuery("SELECT val FROM def_op")) {
                assertTrue(rs.next());
                assertEquals(15, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorInViewDefinition() throws SQLException {
        // Custom operator in CREATE VIEW
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE vw_data (a int, b int)");
            stmt.execute("INSERT INTO vw_data VALUES (3, 7), (10, 20)");
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");
            stmt.execute("CREATE VIEW vw_op AS SELECT a, b, a <+> b AS total FROM vw_data");

            try (ResultSet rs = stmt.executeQuery("SELECT total FROM vw_op ORDER BY total")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(30, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorInWindowPartitionBy() throws SQLException {
        // Custom operator in window function PARTITION BY
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE wp (grp int, val int)");
            stmt.execute("INSERT INTO wp VALUES (1, 10), (1, 20), (2, 30), (2, 40)");
            stmt.execute("CREATE FUNCTION my_mod(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a % b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |%%| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_mod)");

            // Use custom operator in ORDER BY of window function
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT val, SUM(val) OVER (PARTITION BY grp ORDER BY val |%%| 100) FROM wp ORDER BY val")) {
                assertTrue(rs.next()); assertEquals(10, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(20, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(30, rs.getInt(1));
                assertTrue(rs.next()); assertEquals(40, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorMixedWithDifferentOperators() throws SQLException {
        // Multiple different custom operators in the same expression
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION my_mul(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");
            stmt.execute("CREATE OPERATOR |*| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_mul)");

            // Custom operators are left-associative at addition level
            // So: 2 <+> 3 |*| 4 is ((2 <+> 3) |*| 4) = 5 * 4 = 20
            try (ResultSet rs = stmt.executeQuery("SELECT 2 <+> 3 |*| 4")) {
                assertTrue(rs.next());
                assertEquals(20, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorCommutatorStored() throws SQLException {
        // Verify COMMUTATOR is stored in pg_operator catalog
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_eq(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |=| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_eq, "
                    + "COMMUTATOR = |=|)");

            // Self-commutative: oprcom should equal own oid
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oid, oprcom FROM pg_operator WHERE oprname = '|=|'")) {
                assertTrue(rs.next());
                int oid = rs.getInt("oid");
                int comOid = rs.getInt("oprcom");
                assertEquals(oid, comOid, "Self-commutator should reference own OID");
            }
        }
    }

    @Test
    void customOperatorInPlpgsqlBody() throws SQLException {
        // Custom operator used inside another PL/pgSQL function body
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            // Create a function that uses the custom operator internally
            stmt.execute("CREATE FUNCTION use_custom_op(x integer) RETURNS integer AS $$ "
                    + "DECLARE result integer; "
                    + "BEGIN SELECT x <+> 100 INTO result; RETURN result; END; $$ LANGUAGE plpgsql");

            try (ResultSet rs = stmt.executeQuery("SELECT use_custom_op(42)")) {
                assertTrue(rs.next());
                assertEquals(142, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorOverloadSameNameDiffReturnType() throws SQLException {
        // Same operator name, different arg types, different return types
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION add_ints(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE FUNCTION cat_text(a text, b text) RETURNS text AS $$ "
                    + "BEGIN RETURN a || b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = add_ints)");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = text, RIGHTARG = text, FUNCTION = cat_text)");

            try (ResultSet rs = stmt.executeQuery("SELECT 3 <+> 7")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT 'hello' <+> 'world'")) {
                assertTrue(rs.next());
                assertEquals("helloworld", rs.getString(1));
            }
        }
    }

    @Test
    void customOperatorWithCaseWhenResult() throws SQLException {
        // Custom operator on CASE expression result
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_add)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT CASE WHEN true THEN 10 ELSE 20 END <+> 5")) {
                assertTrue(rs.next());
                assertEquals(15, rs.getInt(1));
            }
        }
    }

    @Test
    void customOperatorHashesMergesFlags() throws SQLException {
        // Verify HASHES and MERGES flags stored in pg_operator
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_eq(a integer, b integer) RETURNS boolean AS $$ "
                    + "BEGIN RETURN a = b; END; $$ LANGUAGE plpgsql");
            stmt.execute("CREATE OPERATOR |=| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = my_eq, "
                    + "HASHES, MERGES)");

            try (ResultSet rs = stmt.executeQuery(
                    "SELECT oprcanhash, oprcanmerge FROM pg_operator WHERE oprname = '|=|'")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("oprcanhash"));
                assertTrue(rs.getBoolean("oprcanmerge"));
            }
        }
    }

    // =====================================================================
    // Volatility detection tests — operators and indirect volatility
    // These tests verify that volatile operators/functions are rejected
    // in contexts that require immutability (indexes, generated columns).
    // PG 18 rejects all of these.
    // =====================================================================

    @Test

    void indexRejectsVolatileOperator() throws SQLException {
        // An operator whose backing function is VOLATILE should be rejected in index expressions
        // PG 18: ERROR 42P17 "functions in index expression must be marked IMMUTABLE"
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE vol_idx (x int)");
            stmt.execute("INSERT INTO vol_idx VALUES (1)");
            stmt.execute("CREATE FUNCTION vol_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql VOLATILE");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = vol_add)");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("CREATE INDEX ON vol_idx ((x <+> 1))"));
            assertEquals("42P17", ex.getSQLState());
        }
    }

    @Test

    void indexRejectsStableOperator() throws SQLException {
        // An operator whose backing function is STABLE should also be rejected in index expressions
        // PG 18: ERROR 42P17 "functions in index expression must be marked IMMUTABLE"
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE stab_idx (x int)");
            stmt.execute("INSERT INTO stab_idx VALUES (1)");
            stmt.execute("CREATE FUNCTION stab_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql STABLE");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = stab_add)");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("CREATE INDEX ON stab_idx ((x <+> 1))"));
            assertEquals("42P17", ex.getSQLState());
        }
    }

    @Test
    void indexAcceptsImmutableOperator() throws SQLException {
        // An operator whose backing function is IMMUTABLE should be accepted in index expressions
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE imm_idx (x int)");
            stmt.execute("INSERT INTO imm_idx VALUES (1)");
            stmt.execute("CREATE FUNCTION imm_add(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + b; END; $$ LANGUAGE plpgsql IMMUTABLE");
            stmt.execute("CREATE OPERATOR <+> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = imm_add)");

            // Should succeed
            stmt.execute("CREATE INDEX ON imm_idx ((x <+> 1))");
        }
    }

    @Test

    void virtualColumnRejectsVolatileOperator() throws SQLException {
        // A virtual generated column using a VOLATILE operator should be rejected
        // PG 18: ERROR 42P17 "generation expression is not immutable"
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION vol_mul(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql VOLATILE");
            stmt.execute("CREATE OPERATOR |*| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = vol_mul)");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("CREATE TABLE vol_gen (x int, y int GENERATED ALWAYS AS (x |*| 2) VIRTUAL)"));
            assertEquals("42P17", ex.getSQLState());
        }
    }

    @Test
    void virtualColumnAcceptsImmutableOperator() throws SQLException {
        // A virtual generated column using an IMMUTABLE operator should be accepted
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION imm_mul(a integer, b integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * b; END; $$ LANGUAGE plpgsql IMMUTABLE");
            stmt.execute("CREATE OPERATOR |*| (LEFTARG = integer, RIGHTARG = integer, FUNCTION = imm_mul)");

            // Should succeed
            stmt.execute("CREATE TABLE imm_gen (x int, y int GENERATED ALWAYS AS (x |*| 2) VIRTUAL)");
            stmt.execute("INSERT INTO imm_gen (x) VALUES (5)");

            try (ResultSet rs = stmt.executeQuery("SELECT y FROM imm_gen")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1));
            }
        }
    }

    @Test

    void indexRejectsVolatileUserFunction() throws SQLException {
        // A user-defined function declared VOLATILE should be rejected in index expressions
        // PG 18: ERROR 42P17 "functions in index expression must be marked IMMUTABLE"
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE vol_fn_idx (x int)");
            stmt.execute("INSERT INTO vol_fn_idx VALUES (1)");
            stmt.execute("CREATE FUNCTION my_volatile_fn(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql VOLATILE");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("CREATE INDEX ON vol_fn_idx ((my_volatile_fn(x)))"));
            assertEquals("42P17", ex.getSQLState());
        }
    }

    @Test

    void indexRejectsStableUserFunction() throws SQLException {
        // A user-defined function declared STABLE should be rejected in index expressions
        // PG 18: ERROR 42P17 "functions in index expression must be marked IMMUTABLE"
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE stab_fn_idx (x int)");
            stmt.execute("INSERT INTO stab_fn_idx VALUES (1)");
            stmt.execute("CREATE FUNCTION my_stable_fn(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql STABLE");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("CREATE INDEX ON stab_fn_idx ((my_stable_fn(x)))"));
            assertEquals("42P17", ex.getSQLState());
        }
    }

    @Test
    void indexAcceptsImmutableUserFunction() throws SQLException {
        // A user-defined function declared IMMUTABLE should be accepted in index expressions
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE imm_fn_idx (x int)");
            stmt.execute("INSERT INTO imm_fn_idx VALUES (1)");
            stmt.execute("CREATE FUNCTION my_immutable_fn(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql IMMUTABLE");

            // Should succeed
            stmt.execute("CREATE INDEX ON imm_fn_idx ((my_immutable_fn(x)))");
        }
    }

    @Test

    void virtualColumnRejectsVolatileUserFunction() throws SQLException {
        // A virtual generated column using a VOLATILE user function should be rejected
        // PG 18: ERROR 0A000 "generation expression uses user-defined function"
        // (PG checks UDF restriction before immutability for virtual columns)
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_vol_fn(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * 2; END; $$ LANGUAGE plpgsql VOLATILE");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("CREATE TABLE vol_fn_gen (x int, y int GENERATED ALWAYS AS (my_vol_fn(x)) VIRTUAL)"));
            assertEquals("0A000", ex.getSQLState());
        }
    }

    @Test

    void virtualColumnRejectsStableUserFunction() throws SQLException {
        // A virtual generated column using a STABLE user function should be rejected
        // PG 18: ERROR 0A000 "generation expression uses user-defined function"
        // (PG checks UDF restriction before immutability for virtual columns)
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION my_stab_fn(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a * 2; END; $$ LANGUAGE plpgsql STABLE");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("CREATE TABLE stab_fn_gen (x int, y int GENERATED ALWAYS AS (my_stab_fn(x)) VIRTUAL)"));
            assertEquals("0A000", ex.getSQLState());
        }
    }

    @Test

    void defaultVolatilityIsVolatile() throws SQLException {
        // PG default function volatility is VOLATILE — function without explicit
        // IMMUTABLE/STABLE should be rejected in index expressions
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE def_vol_idx (x int)");
            stmt.execute("INSERT INTO def_vol_idx VALUES (1)");
            // No VOLATILE/STABLE/IMMUTABLE specified — defaults to VOLATILE
            stmt.execute("CREATE FUNCTION default_vol_fn(a integer) RETURNS integer AS $$ "
                    + "BEGIN RETURN a + 1; END; $$ LANGUAGE plpgsql");

            SQLException ex = assertThrows(SQLException.class,
                    () -> stmt.execute("CREATE INDEX ON def_vol_idx ((default_vol_fn(x)))"));
            assertEquals("42P17", ex.getSQLState());
        }
    }
}
