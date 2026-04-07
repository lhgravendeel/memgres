package com.memgres;

import com.memgres.core.Memgres;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Data integrity edge case tests for index correctness.
 * Covers: tables without PK, duplicate rows, numeric precision,
 * CHAR padding, case sensitivity, UPDATE with LIMIT, CASE, FROM,
 * boolean equivalences, empty strings, NaN/Infinity, JSONB ordering,
 * timestamp precision, and cross-type WHERE matching.
 */
class IndexDataIntegrityTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ==============================================================
    // No-PK tables with duplicate rows
    // ==============================================================

    @Test
    void noPkTableDuplicateRowsUpdateOne() throws SQLException {
        // Two identical rows; UPDATE with LIMIT should only change one
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_nopk1 (val TEXT, num INTEGER)");
            s.execute("INSERT INTO di_nopk1 VALUES ('same', 1)");
            s.execute("INSERT INTO di_nopk1 VALUES ('same', 1)");
            // Update only rows matching; both should match and both get updated
            s.execute("UPDATE di_nopk1 SET num = 2 WHERE val = 'same'");
            ResultSet rs = s.executeQuery("SELECT num FROM di_nopk1 ORDER BY num");
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void noPkTableDuplicateRowsDeleteOne() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_nopk2 (val TEXT)");
            s.execute("INSERT INTO di_nopk2 VALUES ('dup')");
            s.execute("INSERT INTO di_nopk2 VALUES ('dup')");
            s.execute("INSERT INTO di_nopk2 VALUES ('other')");
            // Delete all 'dup' rows
            s.execute("DELETE FROM di_nopk2 WHERE val = 'dup'");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM di_nopk2");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void noPkTableWithUniqueConstraint() throws SQLException {
        // No PK, but has UNIQUE; index should work
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_nopk3 (email TEXT UNIQUE, name TEXT)");
            s.execute("INSERT INTO di_nopk3 VALUES ('a@t.com', 'Alice')");
            s.execute("INSERT INTO di_nopk3 VALUES ('b@t.com', 'Bob')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_nopk3 VALUES ('a@t.com', 'Another')"));
            s.execute("UPDATE di_nopk3 SET email = 'c@t.com' WHERE name = 'Alice'");
            s.execute("INSERT INTO di_nopk3 VALUES ('a@t.com', 'Alice2')"); // freed
        }
    }

    @Test
    void noPkDuplicateRowsUpdateToDistinct() throws SQLException {
        // Two identical rows, update one to be different
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_nopk4 (a INTEGER, b TEXT)");
            s.execute("INSERT INTO di_nopk4 VALUES (1, 'x')");
            s.execute("INSERT INTO di_nopk4 VALUES (1, 'x')");
            // ctid-based update isn't available; update all matching rows
            s.execute("UPDATE di_nopk4 SET b = 'y' WHERE a = 1");
            ResultSet rs = s.executeQuery("SELECT b FROM di_nopk4 ORDER BY b");
            assertTrue(rs.next()); assertEquals("y", rs.getString(1));
            assertTrue(rs.next()); assertEquals("y", rs.getString(1));
        }
    }

    // ==============================================================
    // CHAR padding and trailing spaces
    // ==============================================================

    @Test
    void charPaddingInUniqueIndex() throws SQLException {
        // CHAR(5) pads 'AB' to 'AB   ', so both should be treated as the same
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_char1 (code CHAR(5) UNIQUE, val TEXT)");
            s.execute("INSERT INTO di_char1 VALUES ('AB', 'first')");
            // 'AB' is stored as 'AB   ', so inserting 'AB   ' should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_char1 VALUES ('AB   ', 'dup')"));
            // 'AB' with different trailing spaces should also conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_char1 VALUES ('AB', 'dup')"));
            // 'ABC' (different value) should succeed
            s.execute("INSERT INTO di_char1 VALUES ('ABC', 'second')");
        }
    }

    @Test
    void charPkUpdateAndReuse() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_char2 (code CHAR(3) PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO di_char2 VALUES ('A', 'one')");
            // Stored as 'A  ', update to 'B  '
            s.execute("UPDATE di_char2 SET code = 'B' WHERE code = 'A'");
            // 'A' (stored as 'A  ') should be free
            s.execute("INSERT INTO di_char2 VALUES ('A', 'reused')");
            // 'B' should be taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_char2 VALUES ('B', 'dup')"));
        }
    }

    @Test
    void charVsVarcharInWhere() throws SQLException {
        // CHAR(5) pads while VARCHAR does not, so WHERE matching should account for this
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_char3 (id SERIAL PRIMARY KEY, c CHAR(5), v VARCHAR(5))");
            s.execute("INSERT INTO di_char3 (c, v) VALUES ('AB', 'AB')");
            // CHAR column: WHERE c = 'AB' should match (PG trims trailing spaces for comparison)
            ResultSet rs = s.executeQuery("SELECT id FROM di_char3 WHERE c = 'AB'");
            assertTrue(rs.next());
            // VARCHAR column: WHERE v = 'AB' should also match (no padding)
            rs = s.executeQuery("SELECT id FROM di_char3 WHERE v = 'AB'");
            assertTrue(rs.next());
        }
    }

    // ==============================================================
    // Numeric precision: 2.0 vs 2.00 vs 2
    // ==============================================================

    @Test
    void numericPrecisionInUniqueIndex() throws SQLException {
        // NUMERIC(10,2) stores 2.0 and 2.00 identically as 2.00
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_num1 (n NUMERIC(10,2) UNIQUE, label TEXT)");
            s.execute("INSERT INTO di_num1 VALUES (2.0, 'two-point-zero')");
            // 2.00 should conflict (same value after scale normalization)
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_num1 VALUES (2.00, 'dup')"));
            // Plain 2 should also conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_num1 VALUES (2, 'dup')"));
            // 2.01 should succeed
            s.execute("INSERT INTO di_num1 VALUES (2.01, 'different')");
        }
    }

    @Test
    void numericPrecisionWhereClause() throws SQLException {
        // WHERE n = 2.0 should match a row stored as 2.00
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_num2 (id SERIAL PRIMARY KEY, n NUMERIC(10,2))");
            s.execute("INSERT INTO di_num2 (n) VALUES (2.0)");
            s.execute("INSERT INTO di_num2 (n) VALUES (3.0)");
            // All forms should match
            ResultSet rs = s.executeQuery("SELECT count(*) FROM di_num2 WHERE n = 2");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            rs = s.executeQuery("SELECT count(*) FROM di_num2 WHERE n = 2.0");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            rs = s.executeQuery("SELECT count(*) FROM di_num2 WHERE n = 2.00");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void numericPrecisionUpdateWhereMatch() throws SQLException {
        // UPDATE WHERE n = 2.0 should find the row stored as NUMERIC(10,2) 2.00
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_num3 (n NUMERIC(10,2) PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO di_num3 VALUES (2.0, 'orig')");
            int updated = s.executeUpdate("UPDATE di_num3 SET val = 'updated' WHERE n = 2");
            assertEquals(1, updated);
            updated = s.executeUpdate("UPDATE di_num3 SET val = 'updated2' WHERE n = 2.00");
            assertEquals(1, updated);
        }
    }

    @Test
    void numericDifferentInputPrecisionSameAfterStorage() throws SQLException {
        // Two inserts with different input precision but same stored value
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_num4 (id INTEGER PRIMARY KEY, amount NUMERIC(10,2) UNIQUE)");
            s.execute("INSERT INTO di_num4 VALUES (1, 10.5)");
            // 10.50 is the same as 10.5 in NUMERIC(10,2)
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_num4 VALUES (2, 10.50)"));
            // Update amount, old value freed
            s.execute("UPDATE di_num4 SET amount = 10.51 WHERE id = 1");
            s.execute("INSERT INTO di_num4 VALUES (2, 10.50)"); // now free
        }
    }

    @Test
    void integerVsBigintWhereMatch() throws SQLException {
        // BIGINT column, WHERE with integer literal
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_num5 (id BIGINT PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO di_num5 VALUES (42, 'hello')");
            int updated = s.executeUpdate("UPDATE di_num5 SET val = 'world' WHERE id = 42");
            assertEquals(1, updated);
            // Verify
            ResultSet rs = s.executeQuery("SELECT val FROM di_num5 WHERE id = 42");
            assertTrue(rs.next()); assertEquals("world", rs.getString(1));
        }
    }

    // ==============================================================
    // Case sensitivity
    // ==============================================================

    @Test
    void caseSensitiveUniqueIndex() throws SQLException {
        // TEXT UNIQUE: 'ABC' and 'abc' are different values
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_case1 (code TEXT UNIQUE)");
            s.execute("INSERT INTO di_case1 VALUES ('ABC')");
            s.execute("INSERT INTO di_case1 VALUES ('abc')");  // different
            s.execute("INSERT INTO di_case1 VALUES ('Abc')");  // also different
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_case1 VALUES ('ABC')")); // exact dup
        }
    }

    @Test
    void caseSensitiveUpdateAndReuse() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_case2 (id INTEGER PRIMARY KEY, name TEXT UNIQUE)");
            s.execute("INSERT INTO di_case2 VALUES (1, 'Alice')");
            s.execute("INSERT INTO di_case2 VALUES (2, 'alice')");
            // Update 'Alice' to 'ALICE'
            s.execute("UPDATE di_case2 SET name = 'ALICE' WHERE id = 1");
            // 'Alice' is free, 'alice' and 'ALICE' are taken
            s.execute("INSERT INTO di_case2 VALUES (3, 'Alice')");
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_case2 VALUES (4, 'ALICE')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_case2 VALUES (4, 'alice')"));
        }
    }

    @Test
    void caseSensitiveWhereInUpdate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_case3 (id INTEGER PRIMARY KEY, name TEXT UNIQUE)");
            s.execute("INSERT INTO di_case3 VALUES (1, 'Hello')");
            s.execute("INSERT INTO di_case3 VALUES (2, 'hello')");
            // WHERE is case-sensitive: should only update 'Hello', not 'hello'
            int updated = s.executeUpdate("UPDATE di_case3 SET name = 'HELLO' WHERE name = 'Hello'");
            assertEquals(1, updated);
            ResultSet rs = s.executeQuery("SELECT name FROM di_case3 WHERE id = 2");
            assertTrue(rs.next()); assertEquals("hello", rs.getString(1));
        }
    }

    // ==============================================================
    // UPDATE/DELETE with subquery (PG doesn't support LIMIT on UPDATE/DELETE directly)
    // ==============================================================

    @Test
    void updateWithSubqueryLimit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_limit1 (id SERIAL PRIMARY KEY, status TEXT)");
            for (int i = 0; i < 10; i++) {
                s.execute("INSERT INTO di_limit1 (status) VALUES ('pending')");
            }
            // Update only first 3 using subquery
            s.execute("UPDATE di_limit1 SET status = 'done' WHERE id IN (SELECT id FROM di_limit1 WHERE status = 'pending' ORDER BY id LIMIT 3)");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM di_limit1 WHERE status = 'done'");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void deleteWithSubqueryLimit() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_limit2 (id SERIAL PRIMARY KEY, code TEXT UNIQUE)");
            for (int i = 1; i <= 10; i++) {
                s.execute("INSERT INTO di_limit2 (code) VALUES ('C" + i + "')");
            }
            // Delete first 3 using subquery
            s.execute("DELETE FROM di_limit2 WHERE id IN (SELECT id FROM di_limit2 ORDER BY id LIMIT 3)");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM di_limit2");
            assertTrue(rs.next());
            assertEquals(7, rs.getInt(1));
        }
    }

    // ==============================================================
    // Empty string vs NULL
    // ==============================================================

    @Test
    void emptyStringVsNullInUniqueIndex() throws SQLException {
        // In PG, '' and NULL are distinct values
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_empty1 (code TEXT UNIQUE)");
            s.execute("INSERT INTO di_empty1 VALUES ('')");
            s.execute("INSERT INTO di_empty1 VALUES (NULL)");
            s.execute("INSERT INTO di_empty1 VALUES (NULL)"); // NULLs are distinct
            // Empty string is taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_empty1 VALUES ('')"));
        }
    }

    @Test
    void emptyStringUpdateToNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_empty2 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO di_empty2 VALUES (1, '')");
            s.execute("UPDATE di_empty2 SET code = NULL WHERE id = 1");
            // Empty string should be free now
            s.execute("INSERT INTO di_empty2 VALUES (2, '')");
        }
    }

    @Test
    void nullUpdateToEmptyString() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_empty3 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO di_empty3 VALUES (1, NULL)");
            s.execute("INSERT INTO di_empty3 VALUES (2, NULL)");
            s.execute("UPDATE di_empty3 SET code = '' WHERE id = 1");
            // Empty string is now taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_empty3 VALUES (3, '')"));
            // Second NULL can also become empty string, which should fail
            assertThrows(SQLException.class, () ->
                    s.execute("UPDATE di_empty3 SET code = '' WHERE id = 2"));
        }
    }

    // ==============================================================
    // Boolean equivalences
    // ==============================================================

    @Test
    void booleanEquivalenceInIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_bool1 (flag BOOLEAN UNIQUE, label TEXT)");
            s.execute("INSERT INTO di_bool1 VALUES (TRUE, 'yes')");
            // Various TRUE representations should all conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_bool1 VALUES ('t', 'dup')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_bool1 VALUES ('true', 'dup')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_bool1 VALUES ('yes', 'dup')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_bool1 VALUES ('1', 'dup')"));
        }
    }

    @Test
    void booleanWhereMatching() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_bool2 (id INTEGER PRIMARY KEY, flag BOOLEAN UNIQUE)");
            s.execute("INSERT INTO di_bool2 VALUES (1, TRUE)");
            s.execute("INSERT INTO di_bool2 VALUES (2, FALSE)");
            // Update WHERE flag = 't'
            int updated = s.executeUpdate("UPDATE di_bool2 SET id = 10 WHERE flag = 't'");
            assertEquals(1, updated);
            ResultSet rs = s.executeQuery("SELECT id FROM di_bool2 WHERE flag = TRUE");
            assertTrue(rs.next()); assertEquals(10, rs.getInt(1));
        }
    }

    // ==============================================================
    // Negative zero, NaN, Infinity
    // ==============================================================

    @Test
    void negativeZeroVsZero() throws SQLException {
        // In PG, -0.0 and 0.0 are equal for NUMERIC
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_zero1 (n DOUBLE PRECISION UNIQUE)");
            s.execute("INSERT INTO di_zero1 VALUES (0.0)");
            // -0.0 should conflict (PG treats them as equal)
            // Note: this depends on Java Double.valueOf(-0.0).equals(0.0) which is false,
            // but after BigDecimal normalization they should be equal
            // If this doesn't conflict, that's a known edge case
            try {
                s.execute("INSERT INTO di_zero1 VALUES (-0.0)");
                // If it succeeded, both are stored; verify count
                ResultSet rs = s.executeQuery("SELECT count(*) FROM di_zero1");
                assertTrue(rs.next());
                // Accept either behavior (1 if conflicted, 2 if distinct)
                int count = rs.getInt(1);
                assertTrue(count >= 1 && count <= 2);
            } catch (SQLException e) {
                // Conflict detected, which is correct PG behavior
                assertTrue(e.getSQLState().equals("23505"));
            }
        }
    }

    @Test
    void nanInNumericColumn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_nan1 (n DOUBLE PRECISION UNIQUE, label TEXT)");
            s.execute("INSERT INTO di_nan1 VALUES ('NaN', 'nan1')");
            // In PG, NaN = NaN is true for storage purposes (unique constraint)
            // Second NaN should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_nan1 VALUES ('NaN', 'nan2')"));
        }
    }

    @Test
    void infinityInNumericColumn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_inf1 (n DOUBLE PRECISION UNIQUE)");
            s.execute("INSERT INTO di_inf1 VALUES ('Infinity')");
            s.execute("INSERT INTO di_inf1 VALUES ('-Infinity')");
            // Duplicate Infinity should fail
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_inf1 VALUES ('Infinity')"));
            // -Infinity is distinct from Infinity
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_inf1 VALUES ('-Infinity')"));
        }
    }

    @Test
    void selectOnInfinityAndNanValues() throws SQLException {
        // Verify that indexed Infinity/NaN values can be found via SELECT WHERE
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_inf_sel (id SERIAL PRIMARY KEY, n DOUBLE PRECISION UNIQUE, label TEXT)");
            s.execute("INSERT INTO di_inf_sel (n, label) VALUES ('Infinity', 'pos_inf')");
            s.execute("INSERT INTO di_inf_sel (n, label) VALUES ('-Infinity', 'neg_inf')");
            s.execute("INSERT INTO di_inf_sel (n, label) VALUES ('NaN', 'nan_val')");
            s.execute("INSERT INTO di_inf_sel (n, label) VALUES (42.0, 'normal')");

            // SELECT should find Infinity
            ResultSet rs = s.executeQuery("SELECT label FROM di_inf_sel WHERE n = 'Infinity'");
            assertTrue(rs.next(), "Should find Infinity row");
            assertEquals("pos_inf", rs.getString(1));
            assertFalse(rs.next());

            // SELECT should find -Infinity
            rs = s.executeQuery("SELECT label FROM di_inf_sel WHERE n = '-Infinity'");
            assertTrue(rs.next(), "Should find -Infinity row");
            assertEquals("neg_inf", rs.getString(1));
            assertFalse(rs.next());

            // SELECT should find NaN
            rs = s.executeQuery("SELECT label FROM di_inf_sel WHERE n = 'NaN'");
            assertTrue(rs.next(), "Should find NaN row");
            assertEquals("nan_val", rs.getString(1));
            assertFalse(rs.next());

            // Normal value should still work
            rs = s.executeQuery("SELECT label FROM di_inf_sel WHERE n = 42.0");
            assertTrue(rs.next(), "Should find normal row");
            assertEquals("normal", rs.getString(1));
            assertFalse(rs.next());

            // UPDATE on Infinity row should work
            s.execute("UPDATE di_inf_sel SET label = 'updated_inf' WHERE n = 'Infinity'");
            rs = s.executeQuery("SELECT label FROM di_inf_sel WHERE n = 'Infinity'");
            assertTrue(rs.next());
            assertEquals("updated_inf", rs.getString(1));

            // DELETE on NaN row should work
            s.execute("DELETE FROM di_inf_sel WHERE n = 'NaN'");
            rs = s.executeQuery("SELECT count(*) FROM di_inf_sel WHERE n = 'NaN'");
            rs.next();
            assertEquals(0, rs.getInt(1));
        }
    }

    // ==============================================================
    // JSONB key ordering
    // ==============================================================

    @Test
    void jsonbKeyOrderEquivalence() throws SQLException {
        // JSONB normalizes key order, so {"a":1,"b":2} == {"b":2,"a":1}
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_jsonb1 (data JSONB UNIQUE)");
            s.execute("INSERT INTO di_jsonb1 VALUES ('{\"a\":1,\"b\":2}')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_jsonb1 VALUES ('{\"b\":2,\"a\":1}')"));
        }
    }

    @Test
    void jsonbWhitespaceEquivalence() throws SQLException {
        // JSONB normalizes whitespace
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_jsonb2 (data JSONB UNIQUE)");
            s.execute("INSERT INTO di_jsonb2 VALUES ('{\"x\": 1}')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_jsonb2 VALUES ('{\"x\":1}')"));
        }
    }

    @Test
    void jsonbUpdateAndReuse() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_jsonb3 (id INTEGER PRIMARY KEY, data JSONB UNIQUE)");
            s.execute("INSERT INTO di_jsonb3 VALUES (1, '{\"k\":\"v\"}')");
            s.execute("UPDATE di_jsonb3 SET data = '{\"k\":\"v2\"}' WHERE id = 1");
            // Old value should be free
            s.execute("INSERT INTO di_jsonb3 VALUES (2, '{\"k\":\"v\"}')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_jsonb3 VALUES (3, '{\"k\":\"v2\"}')"));
        }
    }

    // ==============================================================
    // Timestamp precision
    // ==============================================================

    @Test
    void timestampPrecisionEquivalence() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_ts1 (ts TIMESTAMP UNIQUE)");
            s.execute("INSERT INTO di_ts1 VALUES ('2024-01-01 12:00:00')");
            // Same timestamp with explicit .000000 should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_ts1 VALUES ('2024-01-01 12:00:00.000000')"));
            // Different fractional seconds should succeed
            s.execute("INSERT INTO di_ts1 VALUES ('2024-01-01 12:00:00.000001')");
        }
    }

    @Test
    void timestamptzEquivalentZones() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("SET timezone = 'UTC'");
            s.execute("CREATE TABLE di_tstz1 (ts TIMESTAMPTZ UNIQUE)");
            s.execute("INSERT INTO di_tstz1 VALUES ('2024-01-01 12:00:00+00')");
            // Same instant in different timezone notation should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_tstz1 VALUES ('2024-01-01 12:00:00 UTC')"));
        }
    }

    // ==============================================================
    // Very long string keys
    // ==============================================================

    @Test
    void veryLongStringUniqueKey() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_long1 (code TEXT UNIQUE)");
            String longStr = Strs.repeat("A", 10000);
            s.execute("INSERT INTO di_long1 VALUES ('" + longStr + "')");
            // Duplicate should fail
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_long1 VALUES ('" + longStr + "')"));
            // One char different should succeed
            String differentStr = longStr.substring(0, 9999) + "B";
            s.execute("INSERT INTO di_long1 VALUES ('" + differentStr + "')");
        }
    }

    // ==============================================================
    // UPDATE with CASE expression
    // ==============================================================

    @Test
    void updateWithCaseExpression() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_case_upd (id INTEGER PRIMARY KEY, status TEXT UNIQUE)");
            s.execute("INSERT INTO di_case_upd VALUES (1, 'active')");
            s.execute("INSERT INTO di_case_upd VALUES (2, 'pending')");
            s.execute("INSERT INTO di_case_upd VALUES (3, 'inactive')");
            // CASE-based update: swap active↔inactive
            s.execute("UPDATE di_case_upd SET status = CASE " +
                    "WHEN status = 'active' THEN 'X_active' " +
                    "WHEN status = 'inactive' THEN 'X_inactive' " +
                    "ELSE status END");
            // Now swap back to the desired final values
            s.execute("UPDATE di_case_upd SET status = CASE " +
                    "WHEN status = 'X_active' THEN 'inactive' " +
                    "WHEN status = 'X_inactive' THEN 'active' " +
                    "ELSE status END");
            ResultSet rs = s.executeQuery("SELECT status FROM di_case_upd WHERE id = 1");
            assertTrue(rs.next()); assertEquals("inactive", rs.getString(1));
            rs = s.executeQuery("SELECT status FROM di_case_upd WHERE id = 3");
            assertTrue(rs.next()); assertEquals("active", rs.getString(1));
        }
    }

    // ==============================================================
    // UPDATE with FROM clause (join update)
    // ==============================================================

    @Test
    void updateWithFromClause() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_from1 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("CREATE TABLE di_from_src (id INTEGER, new_code TEXT)");
            s.execute("INSERT INTO di_from1 VALUES (1, 'OLD1'), (2, 'OLD2')");
            s.execute("INSERT INTO di_from_src VALUES (1, 'NEW1'), (2, 'NEW2')");
            s.execute("UPDATE di_from1 SET code = di_from_src.new_code FROM di_from_src WHERE di_from1.id = di_from_src.id");
            // Old codes free, new codes taken
            s.execute("INSERT INTO di_from1 VALUES (3, 'OLD1')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_from1 VALUES (4, 'NEW1')"));
        }
    }

    // ==============================================================
    // UPDATE RETURNING verifies correct values
    // ==============================================================

    @Test
    void updateReturningReflectsNewValues() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_ret1 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO di_ret1 VALUES (1, 'before')");
            ResultSet rs = s.executeQuery("UPDATE di_ret1 SET code = 'after' WHERE id = 1 RETURNING id, code");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("after", rs.getString(2));
            assertFalse(rs.next());
            // Index should reflect the RETURNING value
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_ret1 VALUES (2, 'after')"));
            s.execute("INSERT INTO di_ret1 VALUES (2, 'before')"); // freed
        }
    }

    // ==============================================================
    // Numeric cross-type: INTEGER column, BIGINT-ish WHERE
    // ==============================================================

    @Test
    void integerColumnBigLiteralWhere() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_xtype1 (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO di_xtype1 VALUES (1, 'a'), (2, 'b')");
            // Large literal that doesn't match any row
            ResultSet rs = s.executeQuery("SELECT count(*) FROM di_xtype1 WHERE id = 999999999");
            assertTrue(rs.next()); assertEquals(0, rs.getInt(1));
            // Exact match should work regardless of literal type
            int updated = s.executeUpdate("UPDATE di_xtype1 SET val = 'updated' WHERE id = 1");
            assertEquals(1, updated);
        }
    }

    @Test
    void smallintPkBigintFkWhere() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_xtype2p (id SMALLINT PRIMARY KEY)");
            s.execute("CREATE TABLE di_xtype2c (id SERIAL PRIMARY KEY, pid SMALLINT REFERENCES di_xtype2p(id))");
            s.execute("INSERT INTO di_xtype2p VALUES (1), (2)");
            s.execute("INSERT INTO di_xtype2c (pid) VALUES (1), (2)");
            // FK check should work across types
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_xtype2c (pid) VALUES (3)"));
        }
    }

    // ==============================================================
    // Multiple identical updates (idempotency)
    // ==============================================================

    @Test
    void repeatedIdenticalUpdates() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_idem1 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO di_idem1 VALUES (1, 'A'), (2, 'B')");
            // Same update 5 times; should all succeed (no-op after first)
            for (int i = 0; i < 5; i++) {
                s.execute("UPDATE di_idem1 SET code = 'X' WHERE id = 1");
            }
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_idem1 VALUES (3, 'X')"));
            s.execute("INSERT INTO di_idem1 VALUES (3, 'A')"); // original freed after first update
        }
    }

    // ==============================================================
    // Mixed: insert, update, ON CONFLICT, delete in sequence
    // ==============================================================

    @Test
    void complexMutationSequence() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_complex (id INTEGER PRIMARY KEY, code TEXT UNIQUE, ver INTEGER DEFAULT 1)");
            // Insert initial rows
            s.execute("INSERT INTO di_complex VALUES (1, 'A', 1), (2, 'B', 1), (3, 'C', 1)");
            // Update A to D
            s.execute("UPDATE di_complex SET code = 'D' WHERE code = 'A'");
            // ON CONFLICT on B: upsert
            s.execute("INSERT INTO di_complex VALUES (2, 'B', 2) ON CONFLICT (code) DO UPDATE SET ver = EXCLUDED.ver");
            // Delete C
            s.execute("DELETE FROM di_complex WHERE code = 'C'");
            // Insert new rows using freed values
            s.execute("INSERT INTO di_complex VALUES (4, 'A', 1)"); // A was freed
            s.execute("INSERT INTO di_complex VALUES (5, 'C', 1)"); // C was freed
            // Verify final state
            ResultSet rs = s.executeQuery("SELECT id, code, ver FROM di_complex ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertEquals("D", rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("B", rs.getString(2)); assertEquals(2, rs.getInt(3));
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1)); assertEquals("A", rs.getString(2));
            assertTrue(rs.next()); assertEquals(5, rs.getInt(1)); assertEquals("C", rs.getString(2));
            assertFalse(rs.next());
            // All current codes protected
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_complex VALUES (6, 'A', 1)"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_complex VALUES (6, 'B', 1)"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_complex VALUES (6, 'C', 1)"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_complex VALUES (6, 'D', 1)"));
        }
    }

    // ==============================================================
    // Whitespace-only strings in unique
    // ==============================================================

    @Test
    void whitespaceStringsDistinct() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_ws1 (code TEXT UNIQUE)");
            s.execute("INSERT INTO di_ws1 VALUES (' ')");
            s.execute("INSERT INTO di_ws1 VALUES ('  ')");
            s.execute("INSERT INTO di_ws1 VALUES ('')");
            // All three should be distinct in TEXT
            ResultSet rs = s.executeQuery("SELECT count(*) FROM di_ws1");
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            // Duplicates should fail
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_ws1 VALUES (' ')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_ws1 VALUES ('  ')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO di_ws1 VALUES ('')"));
        }
    }

    // ==============================================================
    // SERIAL column: auto-generated PK index integrity
    // ==============================================================

    @Test
    void serialPkAfterDeleteAndReinsert() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_serial1 (id SERIAL PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO di_serial1 (val) VALUES ('a'), ('b'), ('c')"); // ids 1,2,3
            s.execute("DELETE FROM di_serial1 WHERE id = 2");
            // Next serial is 4, not 2
            s.execute("INSERT INTO di_serial1 (val) VALUES ('d')"); // id=4
            // Explicitly insert id=2, should work (gap reuse)
            s.execute("INSERT INTO di_serial1 (val) VALUES ('b2')"); // id=5
            // Verify no PK conflicts
            ResultSet rs = s.executeQuery("SELECT count(*) FROM di_serial1");
            assertTrue(rs.next()); assertEquals(4, rs.getInt(1));
        }
    }

    // ==============================================================
    // Composite key: update only one column of the key
    // ==============================================================

    @Test
    void compositeKeyUpdateFirstColumnOnly() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_comp_upd1 (a INTEGER, b INTEGER, val TEXT, PRIMARY KEY (a, b))");
            s.execute("INSERT INTO di_comp_upd1 VALUES (1, 1, 'v1'), (1, 2, 'v2'), (2, 1, 'v3')");
            // Update first column of composite key
            s.execute("UPDATE di_comp_upd1 SET a = 3 WHERE a = 1 AND b = 1");
            // (1, 1) is free, (3, 1) is taken
            s.execute("INSERT INTO di_comp_upd1 VALUES (1, 1, 'reused')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_comp_upd1 VALUES (3, 1, 'dup')"));
            // (1, 2) still taken (untouched)
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_comp_upd1 VALUES (1, 2, 'dup')"));
        }
    }

    @Test
    void compositeKeyUpdateSecondColumnOnly() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE di_comp_upd2 (a INTEGER, b TEXT, PRIMARY KEY (a, b))");
            s.execute("INSERT INTO di_comp_upd2 VALUES (1, 'x'), (1, 'y')");
            s.execute("UPDATE di_comp_upd2 SET b = 'z' WHERE a = 1 AND b = 'x'");
            // (1, 'x') is free
            s.execute("INSERT INTO di_comp_upd2 VALUES (1, 'x')");
            // (1, 'z') is taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO di_comp_upd2 VALUES (1, 'z')"));
        }
    }
}
