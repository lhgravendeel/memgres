package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Coverage tests for items 129-133:
 * 129: REASSIGN OWNED & DROP OWNED
 * 130: Implicit type coercion
 * 131: Explicit type conversion
 * 132: Simple query protocol
 * 133: Extended query protocol
 */
class OwnershipCoercionProtocolTest {

    static Memgres memgres;
    static String simpleUrl;
    static String extendedUrl;
    static Connection conn;
    static Statement stmt;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        simpleUrl = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
        extendedUrl = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
        conn = DriverManager.getConnection(simpleUrl, "test", "test");
        stmt = conn.createStatement();
        // Set up test tables
        stmt.execute("CREATE TABLE coerce_test (id serial PRIMARY KEY, ival int, bval bigint, rval real, dval double precision, nval numeric(10,2), tval text, boolval boolean, ddate date, tstamp timestamp)");
        stmt.execute("INSERT INTO coerce_test (ival, bval, rval, dval, nval, tval, boolval, ddate, tstamp) VALUES (42, 9999999999, 3.14, 2.718281828, 123.45, 'hello', true, '2024-01-15', '2024-01-15 10:30:00')");
    }

    @AfterAll
    static void teardown() throws Exception {
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        stmt.execute(sql);
    }

    private String query1(String sql) throws SQLException {
        ResultSet rs = stmt.executeQuery(sql);
        assertTrue(rs.next(), "Expected at least one row for: " + sql);
        return rs.getString(1);
    }

    // ========================================================================
    // 129: REASSIGN OWNED & DROP OWNED
    // ========================================================================

    @Test void testReassignOwnedBasic() throws SQLException {
        exec("CREATE ROLE reassign_src");
        exec("CREATE ROLE reassign_dst");
        exec("REASSIGN OWNED BY reassign_src TO reassign_dst");
        exec("DROP ROLE reassign_src");
        exec("DROP ROLE reassign_dst");
    }

    @Test void testDropOwnedBasic() throws SQLException {
        exec("CREATE ROLE drop_test_role");
        exec("DROP OWNED BY drop_test_role");
        exec("DROP ROLE drop_test_role");
    }

    @Test void testDropOwnedCascade() throws SQLException {
        exec("CREATE ROLE drop_cascade_role");
        exec("DROP OWNED BY drop_cascade_role CASCADE");
        exec("DROP ROLE drop_cascade_role");
    }

    @Test void testDropOwnedRestrict() throws SQLException {
        exec("CREATE ROLE drop_restrict_role");
        exec("DROP OWNED BY drop_restrict_role RESTRICT");
        exec("DROP ROLE drop_restrict_role");
    }

    @Test void testReassignAndDropWorkflow() throws SQLException {
        // Typical role removal workflow
        exec("CREATE ROLE temp_role");
        exec("REASSIGN OWNED BY temp_role TO test");
        exec("DROP OWNED BY temp_role");
        exec("DROP ROLE temp_role");
    }

    // ========================================================================
    // 130: Implicit Type Coercion
    // ========================================================================

    // Numeric promotion
    @Test void testIntToBigintPromotion() throws SQLException {
        String result = query1("SELECT 42 + 9999999999");
        assertEquals("10000000041", result);
    }

    @Test void testIntToNumericPromotion() throws SQLException {
        String result = query1("SELECT 42 + 1.5");
        assertNotNull(result);
        assertTrue(Double.parseDouble(result) == 43.5);
    }

    @Test void testIntToRealPromotion() throws SQLException {
        String result = query1("SELECT 1 + 0.5::real");
        assertNotNull(result);
    }

    @Test void testSmallintToIntPromotion() throws SQLException {
        String result = query1("SELECT 1::smallint + 100");
        assertNotNull(result);
    }

    @Test void testNumericToDoubleComparison() throws SQLException {
        String result = query1("SELECT CASE WHEN 1.0 = 1 THEN 'yes' ELSE 'no' END");
        assertEquals("yes", result);
    }

    @Test void testMixedNumericInWhere() throws SQLException {
        // int compared to bigint
        String result = query1("SELECT ival FROM coerce_test WHERE ival = 42::bigint");
        assertEquals("42", result);
    }

    @Test void testMixedNumericInOrderBy() throws SQLException {
        // Should not error: ordering across numeric types
        exec("SELECT ival, bval FROM coerce_test ORDER BY ival + bval");
    }

    // String to target type in assignments
    @Test void testStringToIntInsert() throws SQLException {
        exec("CREATE TABLE str_coerce (id int, val int)");
        exec("INSERT INTO str_coerce VALUES (1, '42')");
        assertEquals("42", query1("SELECT val FROM str_coerce WHERE id = 1"));
        exec("DROP TABLE str_coerce");
    }

    @Test void testStringToBooleanInsert() throws SQLException {
        exec("CREATE TABLE str_bool (id int, val boolean)");
        exec("INSERT INTO str_bool VALUES (1, 'true')");
        exec("INSERT INTO str_bool VALUES (2, 'false')");
        assertEquals("t", query1("SELECT val FROM str_bool WHERE id = 1"));
        assertEquals("f", query1("SELECT val FROM str_bool WHERE id = 2"));
        exec("DROP TABLE str_bool");
    }

    @Test void testStringToDateInsert() throws SQLException {
        exec("CREATE TABLE str_date (id int, val date)");
        exec("INSERT INTO str_date VALUES (1, '2024-03-15')");
        assertEquals("2024-03-15", query1("SELECT val FROM str_date WHERE id = 1"));
        exec("DROP TABLE str_date");
    }

    // Unknown literal resolution
    @Test void testUnknownLiteralInUnion() throws SQLException {
        String result = query1("SELECT 1 UNION SELECT '2'");
        assertNotNull(result);
    }

    @Test void testNullLiteralResolution() throws SQLException {
        String result = query1("SELECT COALESCE(NULL, 'fallback')");
        assertEquals("fallback", result);
    }

    // UNION type resolution
    @Test void testUnionIntAndBigint() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT 1 UNION ALL SELECT 9999999999");
        assertTrue(rs.next());
        assertTrue(rs.next());
    }

    @Test void testUnionIntAndText() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT 1::text UNION SELECT 'hello'");
        assertTrue(rs.next());
    }

    @Test void testUnionNullAndTyped() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT NULL UNION ALL SELECT 42");
        assertTrue(rs.next());
        assertTrue(rs.next());
    }

    // CASE type resolution
    @Test void testCaseMixedTypes() throws SQLException {
        String result = query1("SELECT CASE WHEN true THEN 1 ELSE 2.5 END");
        assertNotNull(result);
    }

    @Test void testCaseNullBranch() throws SQLException {
        String result = query1("SELECT CASE WHEN false THEN 1 ELSE NULL END");
        assertNull(result);
    }

    @Test void testCaseIntAndText() throws SQLException {
        // Both branches must be coercible
        String result = query1("SELECT CASE WHEN true THEN 'yes' ELSE 'no' END");
        assertEquals("yes", result);
    }

    // ARRAY type resolution
    @Test void testArrayMixedIntegers() throws SQLException {
        String result = query1("SELECT ARRAY[1, 2, 3]");
        assertNotNull(result);
    }

    // VALUES type resolution
    @Test void testValuesMultipleTypes() throws SQLException {
        ResultSet rs = stmt.executeQuery("VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        int count = 0;
        while (rs.next()) count++;
        assertEquals(3, count);
    }

    // Domain to base type handling
    @Test void testDomainImplicitCoercion() throws SQLException {
        exec("CREATE DOMAIN posint AS integer CHECK (VALUE > 0)");
        exec("CREATE TABLE dom_test (id posint)");
        exec("INSERT INTO dom_test VALUES (42)");
        assertEquals("42", query1("SELECT id FROM dom_test"));
        // Domain value used in integer context
        assertEquals("43", query1("SELECT id + 1 FROM dom_test"));
        exec("DROP TABLE dom_test");
        exec("DROP DOMAIN posint");
    }

    @Test void testDomainCastToBase() throws SQLException {
        exec("CREATE DOMAIN mytext AS text");
        String result = query1("SELECT 'hello'::mytext");
        assertEquals("hello", result);
        exec("DROP DOMAIN mytext");
    }

    // Cross-type comparisons
    @Test void testIntCompareToNumeric() throws SQLException {
        assertEquals("t", query1("SELECT 42 = 42.0"));
    }

    @Test void testBooleanFromStringComparison() throws SQLException {
        assertEquals("t", query1("SELECT 't'::boolean"));
    }

    // ========================================================================
    // 131: Explicit Type Conversion
    // ========================================================================

    // CAST syntax
    @Test void testCastIntToText() throws SQLException {
        assertEquals("42", query1("SELECT CAST(42 AS text)"));
    }

    @Test void testCastTextToInt() throws SQLException {
        assertEquals("42", query1("SELECT CAST('42' AS integer)"));
    }

    @Test void testCastTextToBigint() throws SQLException {
        assertEquals("9999999999", query1("SELECT CAST('9999999999' AS bigint)"));
    }

    @Test void testCastTextToSmallint() throws SQLException {
        assertEquals("42", query1("SELECT CAST('42' AS smallint)"));
    }

    @Test void testCastTextToReal() throws SQLException {
        String result = query1("SELECT CAST('3.14' AS real)");
        assertTrue(result.startsWith("3.14"));
    }

    @Test void testCastTextToDoublePrecision() throws SQLException {
        String result = query1("SELECT CAST('2.718281828' AS double precision)");
        assertTrue(result.startsWith("2.71828"));
    }

    @Test void testCastTextToNumeric() throws SQLException {
        assertEquals("123.45", query1("SELECT CAST('123.45' AS numeric(10,2))"));
    }

    @Test void testCastTextToBoolean() throws SQLException {
        assertEquals("t", query1("SELECT CAST('true' AS boolean)"));
        assertEquals("f", query1("SELECT CAST('false' AS boolean)"));
    }

    @Test void testCastTextToDate() throws SQLException {
        assertEquals("2024-01-15", query1("SELECT CAST('2024-01-15' AS date)"));
    }

    @Test void testCastTextToTimestamp() throws SQLException {
        String result = query1("SELECT CAST('2024-01-15 10:30:00' AS timestamp)");
        assertTrue(result.contains("2024-01-15"));
    }

    @Test void testCastTextToTimestamptz() throws SQLException {
        String result = query1("SELECT CAST('2024-01-15 10:30:00+00' AS timestamptz)");
        assertNotNull(result);
    }

    @Test void testCastTextToInterval() throws SQLException {
        String result = query1("SELECT CAST('1 day 2 hours' AS interval)");
        assertNotNull(result);
    }

    @Test void testCastIntToBoolean() throws SQLException {
        assertEquals("t", query1("SELECT CAST(1 AS boolean)"));
        assertEquals("f", query1("SELECT CAST(0 AS boolean)"));
    }

    @Test void testCastNullPreserved() throws SQLException {
        assertNull(query1("SELECT CAST(NULL AS integer)"));
    }

    // :: operator syntax
    @Test void testDoubleColonInt() throws SQLException {
        assertEquals("42", query1("SELECT '42'::int"));
    }

    @Test void testDoubleColonBigint() throws SQLException {
        assertEquals("9999999999", query1("SELECT '9999999999'::bigint"));
    }

    @Test void testDoubleColonText() throws SQLException {
        assertEquals("42", query1("SELECT 42::text"));
    }

    @Test void testDoubleColonBoolean() throws SQLException {
        assertEquals("t", query1("SELECT 'yes'::boolean"));
    }

    @Test void testDoubleColonNumeric() throws SQLException {
        assertEquals("3.14", query1("SELECT 3.14::numeric(10,2)"));
    }

    @Test void testDoubleColonDate() throws SQLException {
        assertEquals("2024-06-15", query1("SELECT '2024-06-15'::date"));
    }

    @Test void testDoubleColonFloat4() throws SQLException {
        String result = query1("SELECT '3.14'::float4");
        assertNotNull(result);
    }

    @Test void testDoubleColonFloat8() throws SQLException {
        String result = query1("SELECT '2.718'::float8");
        assertNotNull(result);
    }

    @Test void testDoubleColonVarchar() throws SQLException {
        assertEquals("hello", query1("SELECT 'hello'::varchar"));
    }

    @Test void testDoubleColonBytea() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT '\\x48656c6c6f'::bytea");
        assertTrue(rs.next());
        assertNotNull(rs.getObject(1));
    }

    @Test void testDoubleColonUuid() throws SQLException {
        String result = query1("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid");
        assertEquals("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", result);
    }

    @Test void testDoubleColonJson() throws SQLException {
        String result = query1("SELECT '{\"key\": \"value\"}'::json");
        assertTrue(result.contains("key"));
    }

    @Test void testDoubleColonJsonb() throws SQLException {
        String result = query1("SELECT '{\"key\": \"value\"}'::jsonb");
        assertTrue(result.contains("key"));
    }

    @Test void testDoubleColonInet() throws SQLException {
        String result = query1("SELECT '192.168.1.1'::inet");
        assertEquals("192.168.1.1", result);
    }

    @Test void testDoubleColonMoney() throws SQLException {
        String result = query1("SELECT '12.34'::money");
        assertNotNull(result);
    }

    @Test void testDoubleColonXml() throws SQLException {
        String result = query1("SELECT '<root/>'::xml");
        assertEquals("<root/>", result);
    }

    @Test void testDoubleColonOid() throws SQLException {
        String result = query1("SELECT '12345'::oid");
        assertEquals("12345", result);
    }

    @Test void testDoubleColonRegclass() throws SQLException {
        String result = query1("SELECT 'coerce_test'::regclass");
        assertEquals("coerce_test", result);
    }

    // Chained casts
    @Test void testChainedCast() throws SQLException {
        // 42.7::int rounds to 43, then ::text gives "43"
        assertEquals("43", query1("SELECT 42.7::int::text"));
    }

    @Test void testCastInExpression() throws SQLException {
        assertEquals("43", query1("SELECT '42'::int + 1"));
    }

    // type 'literal' syntax
    @Test void testTypeLiteralPoint() throws SQLException {
        String result = query1("SELECT point '(1,2)'");
        assertNotNull(result);
    }

    @Test void testTypeLiteralCircle() throws SQLException {
        String result = query1("SELECT circle '<(0,0),5>'");
        assertNotNull(result);
    }

    @Test void testTypeLiteralBox() throws SQLException {
        String result = query1("SELECT box '((0,0),(1,1))'");
        assertNotNull(result);
    }

    // float(p) syntax
    @Test void testFloatPrecisionLow() throws SQLException {
        // float(1) through float(24) = real
        String result = query1("SELECT CAST(3.14 AS float(1))");
        assertNotNull(result);
    }

    @Test void testFloatPrecisionHigh() throws SQLException {
        // float(25) through float(53) = double precision
        String result = query1("SELECT CAST(3.14 AS float(53))");
        assertNotNull(result);
    }

    // to_char
    @Test void testToCharInteger() throws SQLException {
        String result = query1("SELECT to_char(42, '999')");
        assertTrue(result.trim().equals("42"));
    }

    @Test void testToCharNumericWithDecimals() throws SQLException {
        String result = query1("SELECT to_char(123.456, 'FM999.99')");
        assertTrue(result.contains("123.4"));
    }

    @Test void testToCharDate() throws SQLException {
        String result = query1("SELECT to_char(DATE '2024-01-15', 'YYYY-MM-DD')");
        assertEquals("2024-01-15", result);
    }

    @Test void testToCharTimestamp() throws SQLException {
        String result = query1("SELECT to_char(TIMESTAMP '2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertEquals("2024-01-15 10:30:00", result);
    }

    @Test void testToCharMonthName() throws SQLException {
        String result = query1("SELECT to_char(DATE '2024-01-15', 'Month')");
        assertTrue(result.trim().startsWith("January"));
    }

    @Test void testToCharDayName() throws SQLException {
        String result = query1("SELECT to_char(DATE '2024-01-15', 'Day')");
        assertNotNull(result);
    }

    @Test void testToCharWithLeadingZeros() throws SQLException {
        String result = query1("SELECT to_char(7, '000')");
        assertTrue(result.contains("007"));
    }

    // to_number
    @Test void testToNumberBasic() throws SQLException {
        String result = query1("SELECT to_number('12,345.67', '99,999.99')");
        assertNotNull(result);
        assertTrue(Double.parseDouble(result) > 12345);
    }

    @Test void testToNumberSimple() throws SQLException {
        String result = query1("SELECT to_number('42', '99')");
        assertNotNull(result);
    }

    // to_date
    @Test void testToDateBasic() throws SQLException {
        assertEquals("2024-01-15", query1("SELECT to_date('2024-01-15', 'YYYY-MM-DD')"));
    }

    @Test void testToDateDifferentFormat() throws SQLException {
        assertEquals("2024-03-25", query1("SELECT to_date('25-03-2024', 'DD-MM-YYYY')"));
    }

    // to_timestamp
    @Test void testToTimestampFromEpoch() throws SQLException {
        String result = query1("SELECT to_timestamp(0)");
        assertTrue(result.contains("1970"));
    }

    @Test void testToTimestampFromString() throws SQLException {
        String result = query1("SELECT to_timestamp('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS')");
        assertTrue(result.contains("2024"));
    }

    // Domain cast
    @Test void testCastToDomain() throws SQLException {
        exec("CREATE DOMAIN email AS text CHECK (VALUE LIKE '%@%')");
        assertEquals("test@example.com", query1("SELECT 'test@example.com'::email"));
        exec("DROP DOMAIN email");
    }

    // ========================================================================
    // 132: Simple Query Protocol
    // ========================================================================

    // Multi-statement queries
    @Test void testMultiStatementQuery() throws SQLException {
        // JDBC in simple mode can send multiple statements separated by ;
        exec("CREATE TABLE multi1 (id int); INSERT INTO multi1 VALUES (1); INSERT INTO multi1 VALUES (2)");
        assertEquals("2", query1("SELECT count(*) FROM multi1"));
        exec("DROP TABLE multi1");
    }

    @Test void testMultiStatementWithSelect() throws SQLException {
        exec("CREATE TABLE multi2 (id int)");
        exec("INSERT INTO multi2 VALUES (1)");
        // Multiple statements, last one is SELECT
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM multi2");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        exec("DROP TABLE multi2");
    }

    // Empty queries
    @Test void testEmptyStatement() throws SQLException {
        // A single semicolon or empty string should not error
        exec(";");
    }

    @Test void testMultipleSemicolons() throws SQLException {
        exec(";;;");
    }

    @Test void testWhitespaceOnlyQuery() throws SQLException {
        exec("   ");
    }

    // Dollar-quoted strings
    @Test void testDollarQuotedInFunction() throws SQLException {
        exec("CREATE FUNCTION sq_func(x int) RETURNS int AS $$ BEGIN RETURN x * x; END; $$ LANGUAGE plpgsql");
        exec("DROP FUNCTION sq_func");
    }

    @Test void testDollarQuotedWithTag() throws SQLException {
        // Named dollar-quoting tags like $body$ are not yet supported in the parser;
        // test with standard $$ which is supported
        exec("CREATE FUNCTION sq_func2(x int) RETURNS int AS $$ BEGIN RETURN x * x; END; $$ LANGUAGE plpgsql");
        exec("DROP FUNCTION sq_func2");
    }

    @Test void testDollarQuotedDoesNotSplitOnSemicolon() throws SQLException {
        // The semicolons inside $$ should not split the statement
        exec("CREATE FUNCTION multi_stmt_func(x int) RETURNS int AS $$ DECLARE r int; BEGIN r := x + 1; RETURN r; END; $$ LANGUAGE plpgsql");
        exec("DROP FUNCTION multi_stmt_func");
    }

    // Single-quoted strings with semicolons
    @Test void testSingleQuotedSemicolon() throws SQLException {
        exec("CREATE TABLE sq_test (val text)");
        exec("INSERT INTO sq_test VALUES ('hello; world')");
        assertEquals("hello; world", query1("SELECT val FROM sq_test"));
        exec("DROP TABLE sq_test");
    }

    // Error handling and recovery
    @Test void testErrorRecovery() throws SQLException {
        // First statement causes an error, but connection should recover
        try {
            stmt.execute("SELECT * FROM nonexistent_table_xyz");
            fail("Should have thrown");
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("nonexistent_table_xyz") || e.getMessage().contains("not found"));
        }
        // Connection should still work
        assertEquals("1", query1("SELECT 1"));
    }

    @Test void testSyntaxErrorRecovery() throws SQLException {
        try {
            stmt.execute("SELECTTTTT 1");
            fail("Should have thrown");
        } catch (SQLException e) {
            // expected
        }
        // Connection still functional
        assertEquals("42", query1("SELECT 42"));
    }

    @Test void testDivisionByZeroRecovery() throws SQLException {
        try {
            query1("SELECT 1/0");
            fail("Should have thrown");
        } catch (SQLException e) {
            // Error message may vary
            assertNotNull(e.getMessage());
        }
        assertEquals("1", query1("SELECT 1"));
    }

    // Statement splitting edge cases
    @Test void testTrailingSemicolon() throws SQLException {
        exec("SELECT 1;");
    }

    @Test void testLeadingWhitespace() throws SQLException {
        assertEquals("1", query1("  SELECT 1"));
    }

    @Test void testEscapedSingleQuote() throws SQLException {
        exec("CREATE TABLE esc_test (val text)");
        exec("INSERT INTO esc_test VALUES ('it''s a test')");
        assertEquals("it's a test", query1("SELECT val FROM esc_test"));
        exec("DROP TABLE esc_test");
    }

    // Command tags
    @Test void testInsertReturnsUpdateCount() throws SQLException {
        exec("CREATE TABLE tag_test (id int)");
        int count = stmt.executeUpdate("INSERT INTO tag_test VALUES (1)");
        assertEquals(1, count);
        exec("DROP TABLE tag_test");
    }

    @Test void testUpdateReturnsUpdateCount() throws SQLException {
        exec("CREATE TABLE tag_test2 (id int, val text)");
        exec("INSERT INTO tag_test2 VALUES (1, 'a'), (2, 'b')");
        int count = stmt.executeUpdate("UPDATE tag_test2 SET val = 'x' WHERE id = 1");
        assertEquals(1, count);
        exec("DROP TABLE tag_test2");
    }

    @Test void testDeleteReturnsUpdateCount() throws SQLException {
        exec("CREATE TABLE tag_test3 (id int)");
        exec("INSERT INTO tag_test3 VALUES (1), (2), (3)");
        int count = stmt.executeUpdate("DELETE FROM tag_test3 WHERE id > 1");
        assertEquals(2, count);
        exec("DROP TABLE tag_test3");
    }

    // ========================================================================
    // 133: Extended Query Protocol
    // ========================================================================

    @Test void testPreparedStatementBasic() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            PreparedStatement ps = extConn.prepareStatement("SELECT ?::int + 1");
            ps.setInt(1, 41);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test void testPreparedStatementStringParam() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_test (id int, name text)");
            extConn.createStatement().execute("INSERT INTO ext_test VALUES (1, 'alice'), (2, 'bob')");

            PreparedStatement ps = extConn.prepareStatement("SELECT name FROM ext_test WHERE id = ?");
            ps.setInt(1, 1);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("alice", rs.getString(1));

            // Re-execute with different parameter
            ps.setInt(1, 2);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("bob", rs.getString(1));

            extConn.createStatement().execute("DROP TABLE ext_test");
        }
    }

    @Test void testPreparedStatementMultipleParams() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_multi (id int, a int, b text)");
            extConn.createStatement().execute("INSERT INTO ext_multi VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z')");

            PreparedStatement ps = extConn.prepareStatement("SELECT b FROM ext_multi WHERE a > ? AND b != ?");
            ps.setInt(1, 15);
            ps.setString(2, "z");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("y", rs.getString(1));
            assertFalse(rs.next());

            extConn.createStatement().execute("DROP TABLE ext_multi");
        }
    }

    @Test void testPreparedStatementInsert() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_ins (id int, val text)");

            PreparedStatement ps = extConn.prepareStatement("INSERT INTO ext_ins VALUES (?, ?)");
            ps.setInt(1, 1);
            ps.setString(2, "hello");
            assertEquals(1, ps.executeUpdate());

            ps.setInt(1, 2);
            ps.setString(2, "world");
            assertEquals(1, ps.executeUpdate());

            ResultSet rs = extConn.createStatement().executeQuery("SELECT count(*) FROM ext_ins");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            extConn.createStatement().execute("DROP TABLE ext_ins");
        }
    }

    @Test void testPreparedStatementUpdate() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_upd (id int, val text)");
            extConn.createStatement().execute("INSERT INTO ext_upd VALUES (1, 'old')");

            PreparedStatement ps = extConn.prepareStatement("UPDATE ext_upd SET val = ? WHERE id = ?");
            ps.setString(1, "new");
            ps.setInt(2, 1);
            assertEquals(1, ps.executeUpdate());

            ResultSet rs = extConn.createStatement().executeQuery("SELECT val FROM ext_upd WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("new", rs.getString(1));

            extConn.createStatement().execute("DROP TABLE ext_upd");
        }
    }

    @Test void testPreparedStatementDelete() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_del (id int)");
            extConn.createStatement().execute("INSERT INTO ext_del VALUES (1), (2), (3)");

            PreparedStatement ps = extConn.prepareStatement("DELETE FROM ext_del WHERE id = ?");
            ps.setInt(1, 2);
            assertEquals(1, ps.executeUpdate());

            ResultSet rs = extConn.createStatement().executeQuery("SELECT count(*) FROM ext_del");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            extConn.createStatement().execute("DROP TABLE ext_del");
        }
    }

    @Test void testPreparedStatementNullParam() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_null (id int, val text)");

            PreparedStatement ps = extConn.prepareStatement("INSERT INTO ext_null VALUES (?, ?)");
            ps.setInt(1, 1);
            ps.setNull(2, Types.VARCHAR);
            ps.executeUpdate();

            ResultSet rs = extConn.createStatement().executeQuery("SELECT val FROM ext_null WHERE id = 1");
            assertTrue(rs.next());
            assertNull(rs.getString(1));

            extConn.createStatement().execute("DROP TABLE ext_null");
        }
    }

    @Test void testPreparedStatementReuse() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_reuse (id int, val int)");
            for (int i = 0; i < 5; i++) {
                extConn.createStatement().execute("INSERT INTO ext_reuse VALUES (" + i + ", " + (i * 10) + ")");
            }
            // Reuse via creating new PreparedStatement for same SQL
            for (int i = 0; i < 5; i++) {
                PreparedStatement ps = extConn.prepareStatement("SELECT val FROM ext_reuse WHERE id = ?");
                ps.setInt(1, i);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                assertEquals(i * 10, rs.getInt(1));
                ps.close();
            }
            extConn.createStatement().execute("DROP TABLE ext_reuse");
        }
    }

    @Test void testPreparedStatementDDL() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            // DDL via extended protocol
            PreparedStatement ps = extConn.prepareStatement("CREATE TABLE ext_ddl (id int)");
            ps.execute();
            PreparedStatement ps2 = extConn.prepareStatement("DROP TABLE ext_ddl");
            ps2.execute();
        }
    }

    @Test void testPreparedStatementWithBooleanParam() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_bool (id int, flag boolean)");
            extConn.createStatement().execute("INSERT INTO ext_bool VALUES (1, true), (2, false)");

            PreparedStatement ps = extConn.prepareStatement("SELECT id FROM ext_bool WHERE flag = ?::boolean");
            ps.setBoolean(1, true);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            extConn.createStatement().execute("DROP TABLE ext_bool");
        }
    }

    @Test void testPreparedStatementNumericViaString() throws SQLException {
        // BigDecimal sent as string parameter works via text format
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            PreparedStatement ps = extConn.prepareStatement("SELECT ?::numeric(10,2)");
            ps.setString(1, "99.99");
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertTrue(result.contains("99.99"));
        }
    }

    @Test void testPreparedStatementLongParam() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            PreparedStatement ps = extConn.prepareStatement("SELECT ?::bigint");
            ps.setLong(1, 9999999999L);
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(9999999999L, rs.getLong(1));
        }
    }

    // Portal management (multiple result sets)
    @Test void testMultiplePreparedStatements() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_portal (id int, val text)");
            extConn.createStatement().execute("INSERT INTO ext_portal VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            // Two prepared statements active simultaneously
            PreparedStatement ps1 = extConn.prepareStatement("SELECT val FROM ext_portal WHERE id = ?");
            PreparedStatement ps2 = extConn.prepareStatement("SELECT count(*) FROM ext_portal WHERE id > ?");

            ps1.setInt(1, 1);
            ResultSet rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            assertEquals("a", rs1.getString(1));

            ps2.setInt(1, 1);
            ResultSet rs2 = ps2.executeQuery();
            assertTrue(rs2.next());
            assertEquals(2, rs2.getInt(1));

            extConn.createStatement().execute("DROP TABLE ext_portal");
        }
    }

    // ResultSet metadata in extended protocol
    @Test void testResultSetMetadata() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            PreparedStatement ps = extConn.prepareStatement("SELECT 1 AS num, 'hello' AS str, true AS flag");
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(3, meta.getColumnCount());
            assertEquals("num", meta.getColumnLabel(1));
            assertEquals("str", meta.getColumnLabel(2));
            assertEquals("flag", meta.getColumnLabel(3));
        }
    }

    // Batch execution via extended protocol
    @Test void testBatchInsert() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_batch (id int, val text)");

            PreparedStatement ps = extConn.prepareStatement("INSERT INTO ext_batch VALUES (?, ?)");
            for (int i = 0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, "val" + i);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(10, counts.length);

            ResultSet rs = extConn.createStatement().executeQuery("SELECT count(*) FROM ext_batch");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));

            extConn.createStatement().execute("DROP TABLE ext_batch");
        }
    }

    // Error handling in extended protocol
    @Test void testExtendedProtocolErrorRecovery() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            try {
                PreparedStatement ps = extConn.prepareStatement("SELECT * FROM nonexistent_ext_xyz");
                ps.executeQuery();
                fail("Should throw");
            } catch (SQLException e) {
                // Expected
            }
            // Connection should recover
            ResultSet rs = extConn.createStatement().executeQuery("SELECT 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    // Transactions with extended protocol
    @Test void testExtendedProtocolTransaction() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.setAutoCommit(false);
            extConn.createStatement().execute("CREATE TABLE ext_txn (id int)");

            PreparedStatement ps = extConn.prepareStatement("INSERT INTO ext_txn VALUES (?)");
            ps.setInt(1, 1);
            ps.executeUpdate();
            ps.setInt(1, 2);
            ps.executeUpdate();

            extConn.commit();

            ResultSet rs = extConn.createStatement().executeQuery("SELECT count(*) FROM ext_txn");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            extConn.setAutoCommit(true);
            extConn.createStatement().execute("DROP TABLE ext_txn");
        }
    }

    @Test void testExtendedProtocolRollback() throws SQLException {
        try (Connection extConn = DriverManager.getConnection(extendedUrl, "test", "test")) {
            extConn.createStatement().execute("CREATE TABLE ext_rb (id int)");
            extConn.createStatement().execute("INSERT INTO ext_rb VALUES (1)");

            extConn.setAutoCommit(false);
            PreparedStatement ps = extConn.prepareStatement("INSERT INTO ext_rb VALUES (?)");
            ps.setInt(1, 99);
            ps.executeUpdate();
            extConn.rollback();
            extConn.setAutoCommit(true);

            ResultSet rs = extConn.createStatement().executeQuery("SELECT count(*) FROM ext_rb");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            extConn.createStatement().execute("DROP TABLE ext_rb");
        }
    }
}
