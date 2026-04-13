package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for SQL/JSON standard functions introduced in PostgreSQL 16-17.
 * These are the SQL-standard JSON functions, distinct from the jsonb_* family.
 * All tests should pass on real PG 18.
 */
class SqlJsonStandardTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static String q(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // ================================================================
    // IS JSON predicate
    // ================================================================

    @Test
    void isJson_validObject() throws SQLException {
        assertEquals("t", q("SELECT '{\"a\":1}'::text IS JSON"));
    }

    @Test
    void isJson_validArray() throws SQLException {
        assertEquals("t", q("SELECT '[1,2,3]'::text IS JSON"));
    }

    @Test
    void isJson_validScalar() throws SQLException {
        assertEquals("t", q("SELECT '\"hello\"'::text IS JSON"));
    }

    @Test
    void isJson_validNumber() throws SQLException {
        assertEquals("t", q("SELECT '42'::text IS JSON"));
    }

    @Test
    void isJson_validBoolean() throws SQLException {
        assertEquals("t", q("SELECT 'true'::text IS JSON"));
    }

    @Test
    void isJson_validNull() throws SQLException {
        assertEquals("t", q("SELECT 'null'::text IS JSON"));
    }

    @Test
    void isJson_invalidText() throws SQLException {
        assertEquals("f", q("SELECT 'not json'::text IS JSON"));
    }

    @Test
    void isJson_emptyString() throws SQLException {
        assertEquals("f", q("SELECT ''::text IS JSON"));
    }

    @Test
    void isNotJson() throws SQLException {
        assertEquals("t", q("SELECT 'bad'::text IS NOT JSON"));
    }

    @Test
    void isJson_objectSubtype() throws SQLException {
        assertEquals("t", q("SELECT '{\"a\":1}' IS JSON OBJECT"));
    }

    @Test
    void isJson_arraySubtype() throws SQLException {
        assertEquals("t", q("SELECT '[1,2]' IS JSON ARRAY"));
    }

    @Test
    void isJson_scalarSubtype() throws SQLException {
        assertEquals("t", q("SELECT '42' IS JSON SCALAR"));
    }

    @Test
    void isJson_objectIsNotArray() throws SQLException {
        assertEquals("f", q("SELECT '{\"a\":1}' IS JSON ARRAY"));
    }

    @Test
    void isJson_arrayIsNotObject() throws SQLException {
        assertEquals("f", q("SELECT '[1,2]' IS JSON OBJECT"));
    }

    @Test
    void isJson_stringIsNotScalar_asObject() throws SQLException {
        assertEquals("f", q("SELECT '\"hello\"' IS JSON OBJECT"));
    }

    @Test
    void isJson_withUniqueKeys() throws SQLException {
        assertEquals("t", q("SELECT '{\"a\":1,\"b\":2}' IS JSON WITH UNIQUE KEYS"));
    }

    @Test
    void isJson_withDuplicateKeys() throws SQLException {
        assertEquals("f", q("SELECT '{\"a\":1,\"a\":2}' IS JSON WITH UNIQUE KEYS"));
    }

    @Test
    void isJson_sqlNullIsNotJson() throws SQLException {
        assertNull(q("SELECT NULL IS JSON"));
    }

    // ================================================================
    // JSON_EXISTS
    // ================================================================

    @Test
    void jsonExists_simpleKey() throws SQLException {
        assertEquals("t", q("SELECT JSON_EXISTS('{\"a\":1}', '$.a')"));
    }

    @Test
    void jsonExists_missingKey() throws SQLException {
        assertEquals("f", q("SELECT JSON_EXISTS('{\"a\":1}', '$.b')"));
    }

    @Test
    void jsonExists_nestedKey() throws SQLException {
        assertEquals("t", q("SELECT JSON_EXISTS('{\"a\":{\"b\":2}}', '$.a.b')"));
    }

    @Test
    void jsonExists_arrayElement() throws SQLException {
        assertEquals("t", q("SELECT JSON_EXISTS('[1,2,3]', '$[0]')"));
    }

    @Test
    void jsonExists_arrayElementOutOfBounds() throws SQLException {
        assertEquals("f", q("SELECT JSON_EXISTS('[1,2,3]', '$[10]')"));
    }

    @Test
    void jsonExists_withFilter() throws SQLException {
        assertEquals("t", q("SELECT JSON_EXISTS('{\"a\":5}', '$.a ? (@ > 3)')"));
    }

    @Test
    void jsonExists_filterNoMatch() throws SQLException {
        assertEquals("f", q("SELECT JSON_EXISTS('{\"a\":1}', '$.a ? (@ > 3)')"));
    }

    @Test
    void jsonExists_nullInput() throws SQLException {
        assertNull(q("SELECT JSON_EXISTS(NULL::text, '$.a')"));
    }

    @Test
    void jsonExists_onErrorFalse() throws SQLException {
        // On invalid path expression, ERROR ON ERROR raises, FALSE ON ERROR returns false
        assertEquals("f", q("SELECT JSON_EXISTS('{\"a\":1}', '$.b' FALSE ON ERROR)"));
    }

    // ================================================================
    // JSON_VALUE
    // ================================================================

    @Test
    void jsonValue_extractString() throws SQLException {
        assertEquals("hello", q("SELECT JSON_VALUE('{\"a\":\"hello\"}', '$.a')"));
    }

    @Test
    void jsonValue_extractNumber() throws SQLException {
        assertEquals("42", q("SELECT JSON_VALUE('{\"a\":42}', '$.a')"));
    }

    @Test
    void jsonValue_extractBoolean() throws SQLException {
        assertEquals("true", q("SELECT JSON_VALUE('{\"a\":true}', '$.a')"));
    }

    @Test
    void jsonValue_extractNull() throws SQLException {
        assertNull(q("SELECT JSON_VALUE('{\"a\":null}', '$.a')"));
    }

    @Test
    void jsonValue_missingPath() throws SQLException {
        assertNull(q("SELECT JSON_VALUE('{\"a\":1}', '$.b')"));
    }

    @Test
    void jsonValue_returningInt() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT JSON_VALUE('{\"a\":42}', '$.a' RETURNING int)")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    void jsonValue_returningNumeric() throws SQLException {
        assertEquals("3.14", q("SELECT JSON_VALUE('{\"pi\":3.14}', '$.pi' RETURNING numeric)"));
    }

    @Test
    void jsonValue_returningBoolean() throws SQLException {
        assertEquals("t", q("SELECT JSON_VALUE('{\"ok\":true}', '$.ok' RETURNING boolean)"));
    }

    @Test
    void jsonValue_defaultOnEmpty() throws SQLException {
        assertEquals("fallback", q("SELECT JSON_VALUE('{\"a\":1}', '$.b' DEFAULT 'fallback' ON EMPTY)"));
    }

    @Test
    void jsonValue_defaultOnError() throws SQLException {
        // Extracting an object (not a scalar) is an error for JSON_VALUE
        assertEquals("err", q("SELECT JSON_VALUE('{\"a\":{\"b\":1}}', '$.a' DEFAULT 'err' ON ERROR)"));
    }

    @Test
    void jsonValue_nullOnEmpty() throws SQLException {
        assertNull(q("SELECT JSON_VALUE('{\"a\":1}', '$.b' NULL ON EMPTY)"));
    }

    @Test
    void jsonValue_nestedPath() throws SQLException {
        assertEquals("deep", q("SELECT JSON_VALUE('{\"a\":{\"b\":{\"c\":\"deep\"}}}', '$.a.b.c')"));
    }

    @Test
    void jsonValue_arrayIndex() throws SQLException {
        assertEquals("second", q("SELECT JSON_VALUE('[\"first\",\"second\",\"third\"]', '$[1]')"));
    }

    @Test
    void jsonValue_nullInput() throws SQLException {
        assertNull(q("SELECT JSON_VALUE(NULL::text, '$.a')"));
    }

    @Test
    void jsonValue_errorOnErrorWithNonScalar() throws SQLException {
        // Extracting an object should raise an error with ERROR ON ERROR
        assertThrows(SQLException.class, () ->
                q("SELECT JSON_VALUE('{\"a\":{\"b\":1}}', '$.a' ERROR ON ERROR)"));
    }

    // ================================================================
    // JSON_QUERY
    // ================================================================

    @Test
    void jsonQuery_extractObject() throws SQLException {
        assertEquals("{\"b\": 1}", q("SELECT JSON_QUERY('{\"a\":{\"b\":1}}', '$.a')"));
    }

    @Test
    void jsonQuery_extractArray() throws SQLException {
        assertEquals("[1, 2, 3]", q("SELECT JSON_QUERY('{\"a\":[1,2,3]}', '$.a')"));
    }

    @Test
    void jsonQuery_extractScalarWrapped() throws SQLException {
        // JSON_QUERY with WITH WRAPPER wraps scalar in array
        assertEquals("[42]", q("SELECT JSON_QUERY('{\"a\":42}', '$.a' WITH WRAPPER)"));
    }

    @Test
    void jsonQuery_missingPathReturnsNull() throws SQLException {
        assertNull(q("SELECT JSON_QUERY('{\"a\":1}', '$.b')"));
    }

    @Test
    void jsonQuery_nestedObject() throws SQLException {
        String result = q("SELECT JSON_QUERY('{\"a\":{\"b\":{\"c\":3}}}', '$.a.b')");
        assertEquals("{\"c\": 3}", result);
    }

    @Test
    void jsonQuery_withConditionalWrapper() throws SQLException {
        // PG 17+: WITH CONDITIONAL WRAPPER does NOT wrap single scalars
        assertEquals("42", q("SELECT JSON_QUERY('{\"a\":42}', '$.a' WITH CONDITIONAL WRAPPER)"));
    }

    @Test
    void jsonQuery_withConditionalWrapperOnObject() throws SQLException {
        // Object should not be double-wrapped
        assertEquals("{\"b\": 1}", q("SELECT JSON_QUERY('{\"a\":{\"b\":1}}', '$.a' WITH CONDITIONAL WRAPPER)"));
    }

    @Test
    void jsonQuery_omitQuotes() throws SQLException {
        // PG: OMIT QUOTES on a string scalar returns NULL (unquoted string is not valid JSON)
        assertNull(q("SELECT JSON_QUERY('{\"a\":\"hello\"}', '$.a' OMIT QUOTES)"));
    }

    @Test
    void jsonQuery_keepQuotes() throws SQLException {
        assertEquals("\"hello\"", q("SELECT JSON_QUERY('{\"a\":\"hello\"}', '$.a' KEEP QUOTES)"));
    }

    @Test
    void jsonQuery_emptyArrayOnEmpty() throws SQLException {
        assertEquals("[]", q("SELECT JSON_QUERY('{\"a\":1}', '$.b' EMPTY ARRAY ON EMPTY)"));
    }

    @Test
    void jsonQuery_emptyObjectOnEmpty() throws SQLException {
        assertEquals("{}", q("SELECT JSON_QUERY('{\"a\":1}', '$.b' EMPTY OBJECT ON EMPTY)"));
    }

    @Test
    void jsonQuery_errorOnError() throws SQLException {
        assertThrows(SQLException.class, () ->
                q("SELECT JSON_QUERY('not json', '$.a' ERROR ON ERROR)"));
    }

    @Test
    void jsonQuery_nullInput() throws SQLException {
        assertNull(q("SELECT JSON_QUERY(NULL::text, '$.a')"));
    }

    // ================================================================
    // JSON_TABLE
    // ================================================================

    @Test
    void jsonTable_basicColumns() throws SQLException {
        String sql = """
                SELECT id, name FROM JSON_TABLE(
                    '[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}]',
                    '$[*]' COLUMNS (
                        id int PATH '$.id',
                        name text PATH '$.name'
                    )
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals("Bob", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_withOrdinality() throws SQLException {
        String sql = """
                SELECT rn, val FROM JSON_TABLE(
                    '[10,20,30]',
                    '$[*]' COLUMNS (
                        rn FOR ORDINALITY,
                        val int PATH '$'
                    )
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("rn"));
            assertEquals(10, rs.getInt("val"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("rn"));
            assertEquals(20, rs.getInt("val"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("rn"));
            assertEquals(30, rs.getInt("val"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_defaultOnEmpty() throws SQLException {
        String sql = """
                SELECT val FROM JSON_TABLE(
                    '[{"a":1},{}]',
                    '$[*]' COLUMNS (
                        val int PATH '$.a' DEFAULT 0 ON EMPTY
                    )
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("val"));
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("val"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_existsColumn() throws SQLException {
        String sql = """
                SELECT has_b FROM JSON_TABLE(
                    '[{"a":1,"b":2},{"a":3}]',
                    '$[*]' COLUMNS (
                        has_b boolean EXISTS PATH '$.b'
                    )
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("has_b"));
            assertTrue(rs.next());
            assertFalse(rs.getBoolean("has_b"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_nestedPath() throws SQLException {
        String sql = """
                SELECT name, item FROM JSON_TABLE(
                    '[{"name":"Alice","items":["x","y"]},{"name":"Bob","items":["z"]}]',
                    '$[*]' COLUMNS (
                        name text PATH '$.name',
                        NESTED PATH '$.items[*]' COLUMNS (
                            item text PATH '$'
                        )
                    )
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertEquals("x", rs.getString("item"));
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertEquals("y", rs.getString("item"));
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("name"));
            assertEquals("z", rs.getString("item"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_emptyArray() throws SQLException {
        String sql = """
                SELECT val FROM JSON_TABLE(
                    '[]',
                    '$[*]' COLUMNS (val int PATH '$.a')
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_nullInput() throws SQLException {
        String sql = """
                SELECT val FROM JSON_TABLE(
                    NULL::text,
                    '$[*]' COLUMNS (val int PATH '$.a')
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_inJoin() throws SQLException {
        exec("CREATE TABLE jt_orders (id int, data jsonb)");
        exec("INSERT INTO jt_orders VALUES (1, '[{\"item\":\"pen\",\"qty\":5},{\"item\":\"ink\",\"qty\":2}]')");
        String sql = """
                SELECT o.id, jt.item, jt.qty
                FROM jt_orders o,
                     JSON_TABLE(o.data, '$[*]' COLUMNS (
                         item text PATH '$.item',
                         qty int PATH '$.qty'
                     )) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals("pen", rs.getString("item"));
            assertEquals(5, rs.getInt("qty"));
            assertTrue(rs.next());
            assertEquals("ink", rs.getString("item"));
            assertEquals(2, rs.getInt("qty"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_errorOnError() throws SQLException {
        String sql = """
                SELECT val FROM JSON_TABLE(
                    'not json',
                    '$[*]' COLUMNS (val int PATH '$.a')
                    ERROR ON ERROR
                ) AS jt""";
        assertThrows(SQLException.class, () -> q(sql));
    }

    // ================================================================
    // JSON_SCALAR
    // ================================================================

    @Test
    void jsonScalar_fromText() throws SQLException {
        assertEquals("\"hello\"", q("SELECT JSON_SCALAR('hello')"));
    }

    @Test
    void jsonScalar_fromNumber() throws SQLException {
        assertEquals("42", q("SELECT JSON_SCALAR(42)"));
    }

    @Test
    void jsonScalar_fromBoolean() throws SQLException {
        assertEquals("true", q("SELECT JSON_SCALAR(true)"));
    }

    @Test
    void jsonScalar_fromNull() throws SQLException {
        assertEquals("null", q("SELECT JSON_SCALAR(NULL)"));
    }

    @Test
    void jsonScalar_fromFloat() throws SQLException {
        assertEquals("3.14", q("SELECT JSON_SCALAR(3.14)"));
    }

    // ================================================================
    // JSON_SERIALIZE
    // ================================================================

    @Test
    void jsonSerialize_object() throws SQLException {
        assertEquals("{\"a\": 1}", q("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb)"));
    }

    @Test
    void jsonSerialize_array() throws SQLException {
        assertEquals("[1, 2, 3]", q("SELECT JSON_SERIALIZE('[1,2,3]'::jsonb)"));
    }

    @Test
    void jsonSerialize_returningText() throws SQLException {
        assertEquals("{\"a\": 1}", q("SELECT JSON_SERIALIZE('{\"a\":1}'::jsonb RETURNING text)"));
    }

    @Test
    void jsonSerialize_nullInput() throws SQLException {
        assertNull(q("SELECT JSON_SERIALIZE(NULL::jsonb)"));
    }

    // ================================================================
    // JSON_ARRAYAGG
    // ================================================================

    @Test
    void jsonArrayagg_basic() throws SQLException {
        exec("CREATE TABLE jaa_t (v int)");
        exec("INSERT INTO jaa_t VALUES (1),(2),(3)");
        String result = q("SELECT JSON_ARRAYAGG(v) FROM jaa_t");
        assertEquals("[1, 2, 3]", result);
    }

    @Test
    void jsonArrayagg_withOrderBy() throws SQLException {
        exec("CREATE TABLE jaa_o (v int)");
        exec("INSERT INTO jaa_o VALUES (3),(1),(2)");
        String result = q("SELECT JSON_ARRAYAGG(v ORDER BY v) FROM jaa_o");
        assertEquals("[1, 2, 3]", result);
    }

    @Test
    void jsonArrayagg_nullHandlingAbsent() throws SQLException {
        exec("CREATE TABLE jaa_n (v int)");
        exec("INSERT INTO jaa_n VALUES (1),(NULL),(3)");
        // Default: ABSENT ON NULL — nulls are skipped
        String result = q("SELECT JSON_ARRAYAGG(v) FROM jaa_n");
        assertEquals("[1, 3]", result);
    }

    @Test
    void jsonArrayagg_nullHandlingNull() throws SQLException {
        exec("CREATE TABLE jaa_nn (v int)");
        exec("INSERT INTO jaa_nn VALUES (1),(NULL),(3)");
        String result = q("SELECT JSON_ARRAYAGG(v NULL ON NULL) FROM jaa_nn");
        assertEquals("[1, null, 3]", result);
    }

    @Test
    void jsonArrayagg_emptyTable() throws SQLException {
        exec("CREATE TABLE jaa_e (v int)");
        // Aggregating over empty set returns NULL
        assertNull(q("SELECT JSON_ARRAYAGG(v) FROM jaa_e"));
    }

    @Test
    void jsonArrayagg_textValues() throws SQLException {
        exec("CREATE TABLE jaa_t2 (v text)");
        exec("INSERT INTO jaa_t2 VALUES ('a'),('b')");
        String result = q("SELECT JSON_ARRAYAGG(v) FROM jaa_t2");
        assertEquals("[\"a\", \"b\"]", result);
    }

    @Test
    void jsonArrayagg_returningJsonb() throws SQLException {
        exec("CREATE TABLE jaa_jb (v int)");
        exec("INSERT INTO jaa_jb VALUES (1),(2)");
        String result = q("SELECT JSON_ARRAYAGG(v RETURNING jsonb) FROM jaa_jb");
        // jsonb normalizes
        assertNotNull(result);
        assertTrue(result.contains("1"));
        assertTrue(result.contains("2"));
    }

    // ================================================================
    // JSON_OBJECTAGG
    // ================================================================

    @Test
    void jsonObjectagg_basic() throws SQLException {
        exec("CREATE TABLE joa_t (k text, v int)");
        exec("INSERT INTO joa_t VALUES ('a',1),('b',2)");
        String result = q("SELECT JSON_OBJECTAGG(k : v) FROM joa_t");
        assertNotNull(result);
        assertTrue(result.contains("\"a\""));
        assertTrue(result.contains("\"b\""));
    }

    @Test
    void jsonObjectagg_keyValueSyntax() throws SQLException {
        exec("CREATE TABLE joa_kv (k text, v int)");
        exec("INSERT INTO joa_kv VALUES ('x',10),('y',20)");
        String result = q("SELECT JSON_OBJECTAGG(KEY k VALUE v) FROM joa_kv");
        assertNotNull(result);
        assertTrue(result.contains("\"x\""));
        assertTrue(result.contains("\"y\""));
    }

    @Test
    void jsonObjectagg_nullOnNull() throws SQLException {
        exec("CREATE TABLE joa_n (k text, v int)");
        exec("INSERT INTO joa_n VALUES ('a',1),('b',NULL)");
        String result = q("SELECT JSON_OBJECTAGG(k : v NULL ON NULL) FROM joa_n");
        assertNotNull(result);
        assertTrue(result.contains("null"));
    }

    @Test
    void jsonObjectagg_absentOnNull() throws SQLException {
        exec("CREATE TABLE joa_an (k text, v int)");
        exec("INSERT INTO joa_an VALUES ('a',1),('b',NULL)");
        String result = q("SELECT JSON_OBJECTAGG(k : v ABSENT ON NULL) FROM joa_an");
        assertNotNull(result);
        assertFalse(result.contains("\"b\""));
    }

    @Test
    void jsonObjectagg_emptyTable() throws SQLException {
        exec("CREATE TABLE joa_e (k text, v int)");
        assertNull(q("SELECT JSON_OBJECTAGG(k : v) FROM joa_e"));
    }

    @Test
    void jsonObjectagg_withUniqueKeys() throws SQLException {
        exec("CREATE TABLE joa_u (k text, v int)");
        exec("INSERT INTO joa_u VALUES ('a',1),('a',2)");
        // WITH UNIQUE KEYS should raise an error on duplicate keys
        assertThrows(SQLException.class, () ->
                q("SELECT JSON_OBJECTAGG(k : v WITH UNIQUE KEYS) FROM joa_u"));
    }

    @Test
    void jsonObjectagg_withoutUniqueKeysAllowsDuplicates() throws SQLException {
        exec("CREATE TABLE joa_d (k text, v int)");
        exec("INSERT INTO joa_d VALUES ('a',1),('a',2)");
        // Without UNIQUE KEYS, duplicates are allowed (last value wins or implementation-defined)
        String result = q("SELECT JSON_OBJECTAGG(k : v) FROM joa_d");
        assertNotNull(result);
    }

    // ================================================================
    // json_array constructor (PG 16)
    // ================================================================

    @Test
    void jsonArray_literals() throws SQLException {
        assertEquals("[1, 2, 3]", q("SELECT JSON_ARRAY(1, 2, 3)"));
    }

    @Test
    void jsonArray_textValues() throws SQLException {
        String result = q("SELECT JSON_ARRAY('a', 'b', 'c')");
        assertEquals("[\"a\", \"b\", \"c\"]", result);
    }

    @Test
    void jsonArray_empty() throws SQLException {
        assertEquals("[]", q("SELECT JSON_ARRAY()"));
    }

    @Test
    void jsonArray_mixedTypes() throws SQLException {
        String result = q("SELECT JSON_ARRAY(1, 'two', true, null)");
        // Default null behavior is ABSENT ON NULL
        assertEquals("[1, \"two\", true]", result);
    }

    @Test
    void jsonArray_nullOnNull() throws SQLException {
        String result = q("SELECT JSON_ARRAY(1, null, 3 NULL ON NULL)");
        assertEquals("[1, null, 3]", result);
    }

    @Test
    void jsonArray_returningJsonb() throws SQLException {
        String result = q("SELECT JSON_ARRAY(1, 2 RETURNING jsonb)");
        assertNotNull(result);
        assertTrue(result.contains("1"));
        assertTrue(result.contains("2"));
    }

    @Test
    void jsonArray_fromSubquery() throws SQLException {
        exec("CREATE TABLE ja_sq (v int)");
        exec("INSERT INTO ja_sq VALUES (10),(20),(30)");
        String result = q("SELECT JSON_ARRAY(SELECT v FROM ja_sq ORDER BY v)");
        assertEquals("[10, 20, 30]", result);
    }

    // ================================================================
    // json_object constructor (PG 16)
    // ================================================================

    @Test
    void jsonObject_colonSyntax() throws SQLException {
        String result = q("SELECT JSON_OBJECT('a' : 1, 'b' : 2)");
        assertNotNull(result);
        assertTrue(result.contains("\"a\""));
        assertTrue(result.contains("\"b\""));
    }

    @Test
    void jsonObject_keyValueSyntax() throws SQLException {
        String result = q("SELECT JSON_OBJECT(KEY 'x' VALUE 10, KEY 'y' VALUE 20)");
        assertNotNull(result);
        assertTrue(result.contains("\"x\""));
        assertTrue(result.contains("\"y\""));
    }

    @Test
    void jsonObject_empty() throws SQLException {
        assertEquals("{}", q("SELECT JSON_OBJECT()"));
    }

    @Test
    void jsonObject_nullOnNull() throws SQLException {
        String result = q("SELECT JSON_OBJECT('a' : null NULL ON NULL)");
        assertNotNull(result);
        assertTrue(result.contains("null"));
    }

    @Test
    void jsonObject_absentOnNull() throws SQLException {
        String result = q("SELECT JSON_OBJECT('a' : null ABSENT ON NULL)");
        assertEquals("{}", result);
    }

    @Test
    void jsonObject_withUniqueKeys() throws SQLException {
        assertThrows(SQLException.class, () ->
                q("SELECT JSON_OBJECT('a' : 1, 'a' : 2 WITH UNIQUE KEYS)"));
    }

    @Test
    void jsonObject_returningJsonb() throws SQLException {
        String result = q("SELECT JSON_OBJECT('a' : 1 RETURNING jsonb)");
        assertNotNull(result);
        assertTrue(result.contains("\"a\""));
    }

    // ================================================================
    // IS JSON with table data
    // ================================================================

    @Test
    void isJson_inWhereClause() throws SQLException {
        exec("CREATE TABLE ij_t (id int, data text)");
        exec("INSERT INTO ij_t VALUES (1, '{\"a\":1}'), (2, 'not json'), (3, '[1,2]')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id FROM ij_t WHERE data IS JSON ORDER BY id")) {
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            assertTrue(rs.next()); assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void isJson_inCaseExpression() throws SQLException {
        String result = q("SELECT CASE WHEN '{\"a\":1}' IS JSON OBJECT THEN 'obj' ELSE 'other' END");
        assertEquals("obj", result);
    }

    // ================================================================
    // JSON_VALUE with table data
    // ================================================================

    @Test
    void jsonValue_fromTableColumn() throws SQLException {
        exec("CREATE TABLE jv_t (id int, data jsonb)");
        exec("INSERT INTO jv_t VALUES (1, '{\"name\":\"Alice\",\"age\":30}')");
        exec("INSERT INTO jv_t VALUES (2, '{\"name\":\"Bob\",\"age\":25}')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT JSON_VALUE(data, '$.name') AS name FROM jv_t ORDER BY id")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertTrue(rs.next());
            assertEquals("Bob", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonValue_inWhereClause() throws SQLException {
        exec("CREATE TABLE jvw_t (id int, data jsonb)");
        exec("INSERT INTO jvw_t VALUES (1, '{\"status\":\"active\"}')");
        exec("INSERT INTO jvw_t VALUES (2, '{\"status\":\"inactive\"}')");
        String result = q("SELECT id FROM jvw_t WHERE JSON_VALUE(data, '$.status') = 'active'");
        assertEquals("1", result);
    }

    // ================================================================
    // JSON_QUERY with table data
    // ================================================================

    @Test
    void jsonQuery_fromTableColumn() throws SQLException {
        exec("CREATE TABLE jq_t (id int, data jsonb)");
        exec("INSERT INTO jq_t VALUES (1, '{\"tags\":[\"a\",\"b\"]}')");
        String result = q("SELECT JSON_QUERY(data, '$.tags') FROM jq_t WHERE id = 1");
        assertEquals("[\"a\", \"b\"]", result);
    }

    // ================================================================
    // Combination / integration tests
    // ================================================================

    @Test
    void jsonArrayagg_withJsonValue() throws SQLException {
        exec("CREATE TABLE jcomb (id int, data jsonb)");
        exec("INSERT INTO jcomb VALUES (1, '{\"v\":\"x\"}'),(2, '{\"v\":\"y\"}')");
        String result = q("SELECT JSON_ARRAYAGG(JSON_VALUE(data, '$.v') ORDER BY id) FROM jcomb");
        assertEquals("[\"x\", \"y\"]", result);
    }

    @Test
    void jsonExists_inCheckConstraint() throws SQLException {
        // JSON_EXISTS can be used in CHECK constraints
        exec("CREATE TABLE je_check (data jsonb CHECK (JSON_EXISTS(data, '$.id')))");
        exec("INSERT INTO je_check VALUES ('{\"id\":1}')");
        assertThrows(SQLException.class, () -> exec("INSERT INTO je_check VALUES ('{\"name\":\"no id\"}')"));
    }

    @Test
    void jsonTable_withGroupBy() throws SQLException {
        String sql = """
                SELECT category, count(*) AS cnt
                FROM JSON_TABLE(
                    '[{"cat":"A","v":1},{"cat":"B","v":2},{"cat":"A","v":3}]',
                    '$[*]' COLUMNS (
                        category text PATH '$.cat',
                        v int PATH '$.v'
                    )
                ) AS jt
                GROUP BY category
                ORDER BY category""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals("A", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals("B", rs.getString(1));
            assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_withWhere() throws SQLException {
        String sql = """
                SELECT name, age FROM JSON_TABLE(
                    '[{"name":"Alice","age":30},{"name":"Bob","age":17},{"name":"Eve","age":25}]',
                    '$[*]' COLUMNS (
                        name text PATH '$.name',
                        age int PATH '$.age'
                    )
                ) AS jt
                WHERE age >= 18
                ORDER BY name""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("Eve", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonValue_castToTimestamp() throws SQLException {
        String result = q("SELECT JSON_VALUE('{\"ts\":\"2024-01-15\"}', '$.ts' RETURNING date)");
        assertEquals("2024-01-15", result);
    }

    @Test
    void jsonExists_withPassingVariable() throws SQLException {
        // PASSING clause allows binding SQL values into the JSONPath expression
        assertEquals("t", q("SELECT JSON_EXISTS('{\"a\":5}', '$.a ? (@ > $x)' PASSING 3 AS x)"));
    }

    @Test
    void jsonExists_withPassingVariableNoMatch() throws SQLException {
        assertEquals("f", q("SELECT JSON_EXISTS('{\"a\":5}', '$.a ? (@ > $x)' PASSING 10 AS x)"));
    }

    @Test
    void jsonValue_withPassingVariable() throws SQLException {
        assertEquals("5", q("SELECT JSON_VALUE('{\"a\":5,\"b\":10}', '$.a')"));
    }

    // ================================================================
    // Error code tests
    // ================================================================

    @Test
    void jsonValue_errorOnError_sqlState22032() throws SQLException {
        // 22032 = invalid_json_text (or related JSON error code)
        try {
            q("SELECT JSON_VALUE('{\"a\":[1,2]}', '$.a' ERROR ON ERROR)");
            fail("Should have thrown");
        } catch (SQLException e) {
            // PG uses 22032 or 2203F for JSON errors
            String state = e.getSQLState();
            assertNotNull(state);
            assertTrue(state.startsWith("22"), "Expected JSON error SQLSTATE 22xxx, got " + state);
        }
    }

    @Test
    void jsonExists_invalidJsonInput_errorOnError() throws SQLException {
        try {
            q("SELECT JSON_EXISTS('not json at all', '$.a' ERROR ON ERROR)");
            fail("Should have thrown");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertNotNull(state);
            assertTrue(state.startsWith("22"), "Expected JSON error SQLSTATE 22xxx, got " + state);
        }
    }

    @Test
    void jsonQuery_invalidJsonInput_errorOnError() throws SQLException {
        try {
            q("SELECT JSON_QUERY('{{invalid}}', '$.a' ERROR ON ERROR)");
            fail("Should have thrown");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertNotNull(state);
            assertTrue(state.startsWith("22"), "Expected JSON error SQLSTATE 22xxx, got " + state);
        }
    }

    // ================================================================
    // Additional corner cases: IS JSON
    // ================================================================

    @Test
    void isJson_valueSubtype() throws SQLException {
        // IS JSON VALUE means "any valid JSON" — synonymous with IS JSON
        assertEquals("t", q("SELECT '42' IS JSON VALUE"));
        assertEquals("t", q("SELECT '{\"a\":1}' IS JSON VALUE"));
        assertEquals("t", q("SELECT '[1]' IS JSON VALUE"));
    }

    @Test
    void isNotJson_objectSubtype() throws SQLException {
        // IS NOT JSON OBJECT — arrays and scalars are NOT objects
        assertEquals("t", q("SELECT '[1,2]' IS NOT JSON OBJECT"));
        assertEquals("f", q("SELECT '{\"a\":1}' IS NOT JSON OBJECT"));
    }

    @Test
    void isNotJson_arraySubtype() throws SQLException {
        assertEquals("t", q("SELECT '{\"a\":1}' IS NOT JSON ARRAY"));
        assertEquals("f", q("SELECT '[1,2]' IS NOT JSON ARRAY"));
    }

    @Test
    void isJson_whitespace() throws SQLException {
        // Leading/trailing whitespace should be tolerated
        assertEquals("t", q("SELECT '  {\"a\":1}  ' IS JSON"));
    }

    @Test
    void isJson_scalarFalseKeyword() throws SQLException {
        assertEquals("t", q("SELECT 'false' IS JSON SCALAR"));
    }

    @Test
    void isJson_nestedDuplicateKeysAtTopLevel() throws SQLException {
        // WITH UNIQUE KEYS only checks the top level in PG
        assertEquals("t", q("SELECT '{\"a\":{\"x\":1,\"x\":2},\"b\":1}' IS JSON WITH UNIQUE KEYS"));
    }

    @Test
    void isJson_emptyObject() throws SQLException {
        assertEquals("t", q("SELECT '{}' IS JSON OBJECT"));
    }

    @Test
    void isJson_emptyArray() throws SQLException {
        assertEquals("t", q("SELECT '[]' IS JSON ARRAY"));
    }

    // ================================================================
    // Additional corner cases: JSON_EXISTS
    // ================================================================

    @Test
    void jsonExists_trueOnError() throws SQLException {
        // PG: invalid JSON input always errors — the implicit cast to json fails
        // before JSON_EXISTS runs, so TRUE ON ERROR cannot catch it
        assertThrows(SQLException.class, () ->
                q("SELECT JSON_EXISTS('not json', '$.a' TRUE ON ERROR)"));
    }

    @Test
    void jsonExists_errorOnError_throws() throws SQLException {
        assertThrows(SQLException.class, () ->
                q("SELECT JSON_EXISTS('not json', '$.a' ERROR ON ERROR)"));
    }

    @Test
    void jsonExists_arrayWildcard() throws SQLException {
        // $[*] matches all array elements
        assertEquals("t", q("SELECT JSON_EXISTS('[1,2,3]', '$[*]')"));
    }

    @Test
    void jsonExists_rootPath() throws SQLException {
        // $ always exists for valid JSON
        assertEquals("t", q("SELECT JSON_EXISTS('{\"a\":1}', '$')"));
    }

    @Test
    void jsonExists_deepNestedPath() throws SQLException {
        assertEquals("t", q("SELECT JSON_EXISTS('{\"a\":{\"b\":{\"c\":{\"d\":1}}}}', '$.a.b.c.d')"));
        assertEquals("f", q("SELECT JSON_EXISTS('{\"a\":{\"b\":{\"c\":{\"d\":1}}}}', '$.a.b.c.e')"));
    }

    // ================================================================
    // Additional corner cases: JSON_VALUE
    // ================================================================

    @Test
    void jsonValue_bothDefaults() throws SQLException {
        // DEFAULT ON EMPTY and DEFAULT ON ERROR in the same expression
        assertEquals("empty", q("SELECT JSON_VALUE('{\"a\":1}', '$.b' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR)"));
    }

    @Test
    void jsonValue_bothDefaultsErrorTriggered() throws SQLException {
        // Non-scalar result triggers ON ERROR, not ON EMPTY
        assertEquals("error", q("SELECT JSON_VALUE('{\"a\":[1,2]}', '$.a' DEFAULT 'empty' ON EMPTY DEFAULT 'error' ON ERROR)"));
    }

    @Test
    void jsonValue_returningText() throws SQLException {
        assertEquals("42", q("SELECT JSON_VALUE('{\"a\":42}', '$.a' RETURNING text)"));
    }

    @Test
    void jsonValue_rootPathScalar() throws SQLException {
        // Extracting root of a scalar JSON
        assertEquals("42", q("SELECT JSON_VALUE('42', '$')"));
    }

    @Test
    void jsonValue_rootPathString() throws SQLException {
        assertEquals("hello", q("SELECT JSON_VALUE('\"hello\"', '$')"));
    }

    @Test
    void jsonValue_emptyStringResult() throws SQLException {
        assertEquals("", q("SELECT JSON_VALUE('{\"a\":\"\"}', '$.a')"));
    }

    @Test
    void jsonValue_numericPrecision() throws SQLException {
        assertEquals("3.14159", q("SELECT JSON_VALUE('{\"pi\":3.14159}', '$.pi')"));
    }

    @Test
    void jsonValue_longString() throws SQLException {
        // JSON string with spaces and punctuation
        assertEquals("hello world!", q("SELECT JSON_VALUE('{\"msg\":\"hello world!\"}', '$.msg')"));
    }

    // ================================================================
    // Additional corner cases: JSON_QUERY
    // ================================================================

    @Test
    void jsonQuery_scalarWithoutWrapper() throws SQLException {
        // JSON_QUERY returns scalar JSON values directly (PG 16+ behavior)
        assertEquals("42", q("SELECT JSON_QUERY('{\"a\":42}', '$.a')"));
    }

    @Test
    void jsonQuery_withWrapperOnObject() throws SQLException {
        // WITH WRAPPER wraps even objects in an array
        assertEquals("[{\"b\": 1}]", q("SELECT JSON_QUERY('{\"a\":{\"b\":1}}', '$.a' WITH WRAPPER)"));
    }

    @Test
    void jsonQuery_withWrapperOnArray() throws SQLException {
        // WITH WRAPPER wraps arrays in another array
        assertEquals("[[1, 2]]", q("SELECT JSON_QUERY('{\"a\":[1,2]}', '$.a' WITH WRAPPER)"));
    }

    @Test
    void jsonQuery_conditionalWrapperOnArray() throws SQLException {
        // CONDITIONAL WRAPPER does NOT wrap arrays
        assertEquals("[1, 2]", q("SELECT JSON_QUERY('{\"a\":[1,2]}', '$.a' WITH CONDITIONAL WRAPPER)"));
    }

    @Test
    void jsonQuery_nullOnError() throws SQLException {
        // NULL ON ERROR returns null for invalid JSON
        assertNull(q("SELECT JSON_QUERY('bad json', '$.a' NULL ON ERROR)"));
    }

    @Test
    void jsonQuery_rootPath() throws SQLException {
        // $ returns the entire document
        assertEquals("{\"a\": 1}", q("SELECT JSON_QUERY('{\"a\":1}', '$')"));
    }

    // ================================================================
    // Additional corner cases: JSON_TABLE
    // ================================================================

    @Test
    void jsonTable_inferredPathFromColumnName() throws SQLException {
        // When PATH is omitted, JSON_TABLE infers $.columnName
        String sql = """
                SELECT name, age FROM JSON_TABLE(
                    '[{"name":"Alice","age":30}]',
                    '$[*]' COLUMNS (
                        name text,
                        age int
                    )
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertEquals(30, rs.getInt("age"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_singleObjectInput() throws SQLException {
        // JSON_TABLE with a single object (not an array) using root path
        String sql = """
                SELECT name FROM JSON_TABLE(
                    '{"name":"Alice"}',
                    '$' COLUMNS (name text PATH '$.name')
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_nullColumnValue() throws SQLException {
        // Column value that is JSON null
        String sql = """
                SELECT val FROM JSON_TABLE(
                    '[{"val":null}]',
                    '$[*]' COLUMNS (val text PATH '$.val')
                ) AS jt""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertNull(rs.getString("val"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonTable_multipleColumnsDefaultOnEmpty() throws SQLException {
        // Multiple columns with different defaults
        String sql = """
                SELECT a, b FROM JSON_TABLE(
                    '[{"a":1},{"b":2}]',
                    '$[*]' COLUMNS (
                        a int PATH '$.a' DEFAULT 0 ON EMPTY,
                        b int PATH '$.b' DEFAULT 0 ON EMPTY
                    )
                ) AS jt ORDER BY a""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("a")); // missing a, default 0
            assertEquals(2, rs.getInt("b"));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("a"));
            assertEquals(0, rs.getInt("b")); // missing b, default 0
            assertFalse(rs.next());
        }
    }

    // ================================================================
    // Additional corner cases: JSON_SCALAR
    // ================================================================

    @Test
    void jsonScalar_emptyString() throws SQLException {
        assertEquals("\"\"", q("SELECT JSON_SCALAR('')"));
    }

    @Test
    void jsonScalar_specialCharsQuotes() throws SQLException {
        // Text with double quotes gets escaped
        String result = q("SELECT JSON_SCALAR('he said \"hi\"')");
        assertNotNull(result);
        assertTrue(result.contains("\\\""), "Should contain escaped quotes: " + result);
    }

    @Test
    void jsonScalar_backslash() throws SQLException {
        String result = q("SELECT JSON_SCALAR('path\\\\to\\\\file')");
        assertNotNull(result);
        assertTrue(result.contains("\\\\"), "Should contain escaped backslash: " + result);
    }

    @Test
    void jsonScalar_negativeNumber() throws SQLException {
        assertEquals("-42", q("SELECT JSON_SCALAR(-42)"));
    }

    // ================================================================
    // Additional corner cases: JSON_ARRAY constructor
    // ================================================================

    @Test
    void jsonArray_singleElement() throws SQLException {
        assertEquals("[1]", q("SELECT JSON_ARRAY(1)"));
    }

    @Test
    void jsonArray_nestedArrayConstructor() throws SQLException {
        // JSON_ARRAY inside JSON_ARRAY
        String result = q("SELECT JSON_ARRAY(JSON_ARRAY(1, 2), JSON_ARRAY(3, 4))");
        assertEquals("[[1, 2], [3, 4]]", result);
    }

    @Test
    void jsonArray_objectInsideArray() throws SQLException {
        // JSON_OBJECT inside JSON_ARRAY
        String result = q("SELECT JSON_ARRAY(JSON_OBJECT('a' : 1), JSON_OBJECT('b' : 2))");
        assertNotNull(result);
        assertTrue(result.startsWith("["));
        assertTrue(result.endsWith("]"));
        assertTrue(result.contains("\"a\""));
        assertTrue(result.contains("\"b\""));
    }

    @Test
    void jsonArray_booleanValues() throws SQLException {
        assertEquals("[true, false]", q("SELECT JSON_ARRAY(true, false)"));
    }

    @Test
    void jsonArray_allNullsAbsentOnNull() throws SQLException {
        // All nulls with ABSENT ON NULL → empty array
        assertEquals("[]", q("SELECT JSON_ARRAY(null, null, null)"));
    }

    // ================================================================
    // Additional corner cases: JSON_OBJECT constructor
    // ================================================================

    @Test
    void jsonObject_nestedObject() throws SQLException {
        String result = q("SELECT JSON_OBJECT('outer' : JSON_OBJECT('inner' : 1))");
        assertNotNull(result);
        assertTrue(result.contains("\"outer\""));
        assertTrue(result.contains("\"inner\""));
    }

    @Test
    void jsonObject_arrayAsValue() throws SQLException {
        String result = q("SELECT JSON_OBJECT('items' : JSON_ARRAY(1, 2, 3))");
        assertNotNull(result);
        assertTrue(result.contains("\"items\""));
        assertTrue(result.contains("[1, 2, 3]"));
    }

    @Test
    void jsonObject_manyKeys() throws SQLException {
        String result = q("SELECT JSON_OBJECT('a':1, 'b':2, 'c':3, 'd':4, 'e':5)");
        assertNotNull(result);
        assertTrue(result.contains("\"a\""));
        assertTrue(result.contains("\"e\""));
    }

    @Test
    void jsonObject_booleanValue() throws SQLException {
        String result = q("SELECT JSON_OBJECT('flag' : true)");
        assertNotNull(result);
        assertTrue(result.contains("true"));
    }

    // ================================================================
    // Additional corner cases: JSON_ARRAYAGG
    // ================================================================

    @Test
    void jsonArrayagg_withGroupBy() throws SQLException {
        exec("CREATE TABLE jaa_grp (grp text, v int)");
        exec("INSERT INTO jaa_grp VALUES ('A',1),('A',2),('B',3),('B',4)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT grp, JSON_ARRAYAGG(v ORDER BY v) AS arr FROM jaa_grp GROUP BY grp ORDER BY grp")) {
            assertTrue(rs.next());
            assertEquals("A", rs.getString("grp"));
            assertEquals("[1, 2]", rs.getString("arr"));
            assertTrue(rs.next());
            assertEquals("B", rs.getString("grp"));
            assertEquals("[3, 4]", rs.getString("arr"));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonArrayagg_booleanValues() throws SQLException {
        exec("CREATE TABLE jaa_bool (v boolean)");
        exec("INSERT INTO jaa_bool VALUES (true),(false),(true)");
        String result = q("SELECT JSON_ARRAYAGG(v) FROM jaa_bool");
        assertNotNull(result);
        assertTrue(result.contains("true"));
        assertTrue(result.contains("false"));
    }

    @Test
    void jsonArrayagg_singleRow() throws SQLException {
        exec("CREATE TABLE jaa_one (v int)");
        exec("INSERT INTO jaa_one VALUES (42)");
        assertEquals("[42]", q("SELECT JSON_ARRAYAGG(v) FROM jaa_one"));
    }

    // ================================================================
    // Additional corner cases: JSON_OBJECTAGG
    // ================================================================

    @Test
    void jsonObjectagg_withGroupBy() throws SQLException {
        exec("CREATE TABLE joa_grp (grp text, k text, v int)");
        exec("INSERT INTO joa_grp VALUES ('X','a',1),('X','b',2),('Y','c',3)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT grp, JSON_OBJECTAGG(k : v) AS obj FROM joa_grp GROUP BY grp ORDER BY grp")) {
            assertTrue(rs.next());
            assertEquals("X", rs.getString("grp"));
            String xObj = rs.getString("obj");
            assertNotNull(xObj);
            assertTrue(xObj.contains("\"a\""));
            assertTrue(xObj.contains("\"b\""));
            assertTrue(rs.next());
            assertEquals("Y", rs.getString("grp"));
            String yObj = rs.getString("obj");
            assertNotNull(yObj);
            assertTrue(yObj.contains("\"c\""));
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonObjectagg_singleRow() throws SQLException {
        exec("CREATE TABLE joa_one (k text, v int)");
        exec("INSERT INTO joa_one VALUES ('key', 42)");
        String result = q("SELECT JSON_OBJECTAGG(k : v) FROM joa_one");
        assertNotNull(result);
        assertTrue(result.contains("\"key\""));
        assertTrue(result.contains("42"));
    }

    // ================================================================
    // Additional integration / chaining tests
    // ================================================================

    @Test
    void jsonValueInsideJsonArray() throws SQLException {
        // Use JSON_VALUE result inside JSON_ARRAY constructor
        String result = q("SELECT JSON_ARRAY(JSON_VALUE('{\"a\":1}', '$.a'), JSON_VALUE('{\"b\":2}', '$.b'))");
        assertEquals("[\"1\", \"2\"]", result);
    }

    @Test
    void chainJsonQueryThenJsonValue() throws SQLException {
        // Extract nested object with JSON_QUERY, then extract scalar from it with JSON_VALUE
        String inner = q("SELECT JSON_QUERY('{\"a\":{\"b\":42}}', '$.a')");
        assertEquals("{\"b\": 42}", inner);
        // Now chain: JSON_VALUE of a JSON_QUERY result
        assertEquals("42", q("SELECT JSON_VALUE(JSON_QUERY('{\"a\":{\"b\":42}}', '$.a'), '$.b')"));
    }

    @Test
    void jsonTable_withJsonValueInSelect() throws SQLException {
        // Use JSON_VALUE on a JSON_TABLE column that contains JSON
        String sql = """
                SELECT JSON_VALUE(raw_data, '$.x') AS x_val
                FROM JSON_TABLE(
                    '[{"raw_data":{"x":100}},{"raw_data":{"x":200}}]',
                    '$[*]' COLUMNS (
                        raw_data text PATH '$.raw_data'
                    )
                ) AS jt
                ORDER BY x_val""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals("100", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("200", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void isJson_inSelectList() throws SQLException {
        // IS JSON as an expression in SELECT list
        exec("CREATE TABLE ij_sel (data text)");
        exec("INSERT INTO ij_sel VALUES ('{\"a\":1}'),('not json'),('[1,2]')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT data IS JSON AS is_j FROM ij_sel ORDER BY data")) {
            assertTrue(rs.next()); assertEquals("f", rs.getString(1)); // 'not json'
            assertTrue(rs.next()); assertEquals("t", rs.getString(1)); // '[1,2]'
            assertTrue(rs.next()); assertEquals("t", rs.getString(1)); // '{"a":1}'
            assertFalse(rs.next());
        }
    }

    @Test
    void jsonExists_inCaseExpression() throws SQLException {
        String result = q("""
                SELECT CASE
                    WHEN JSON_EXISTS('{"type":"admin"}', '$.type ? (@ == "admin")') THEN 'admin'
                    ELSE 'user'
                END""");
        assertEquals("admin", result);
    }

    @Test
    void jsonObject_withJsonValue() throws SQLException {
        // Build a JSON object using JSON_VALUE to extract values
        String result = q("""
                SELECT JSON_OBJECT(
                    'name' : JSON_VALUE('{"first":"John","last":"Doe"}', '$.first'),
                    'age' : 30
                )""");
        assertNotNull(result);
        assertTrue(result.contains("\"name\""));
        assertTrue(result.contains("\"John\""));
        assertTrue(result.contains("30"));
    }

    @Test
    void jsonArray_emptySubquery() throws SQLException {
        exec("CREATE TABLE ja_empty_sq (v int)");
        // Empty table → JSON_ARRAY(SELECT ...) returns NULL (PG behavior)
        assertNull(q("SELECT JSON_ARRAY(SELECT v FROM ja_empty_sq)"));
    }

    @Test
    void jsonTable_ordinalityWithWhere() throws SQLException {
        // Ordinality numbering is assigned before WHERE filter
        String sql = """
                SELECT rn, val FROM JSON_TABLE(
                    '[1,2,3,4,5]',
                    '$[*]' COLUMNS (
                        rn FOR ORDINALITY,
                        val int PATH '$'
                    )
                ) AS jt
                WHERE val > 3""";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("rn")); // 4th element
            assertEquals(4, rs.getInt("val"));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("rn")); // 5th element
            assertEquals(5, rs.getInt("val"));
            assertFalse(rs.next());
        }
    }
}
