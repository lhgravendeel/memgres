package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for DEFAULT expression evaluation on INSERT.
 *
 * PostgreSQL evaluates DEFAULT expressions at insert time. Complex expressions
 * involving function calls, casts, arithmetic, and string operations must all
 * be evaluated, not stored as raw text.
 *
 * The typical failure mode: the expression string itself (e.g. "(floor(random() * 60))::INT")
 * gets stored as the column value instead of the evaluated result (e.g. 42).
 */
class DefaultExpressionEvalTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getString(1);
        }
    }

    // =========================================================================
    // 1. Function-based volatile defaults
    // =========================================================================

    @Test
    void testRandomDefault() throws SQLException {
        exec("CREATE TABLE def_random (id serial PRIMARY KEY, val double precision DEFAULT random())");
        exec("INSERT INTO def_random DEFAULT VALUES");
        double val = Double.parseDouble(query1("SELECT val FROM def_random WHERE id = 1"));
        assertTrue(val >= 0.0 && val < 1.0, "random() must be in [0, 1), got " + val);
    }

    @Test
    void testFloorRandomIntDefault() throws SQLException {
        exec("CREATE TABLE def_floor_random (id serial PRIMARY KEY, seconds_after_minute INT NOT NULL DEFAULT (floor(random() * 60))::INT)");
        exec("INSERT INTO def_floor_random DEFAULT VALUES");
        int val = Integer.parseInt(query1("SELECT seconds_after_minute FROM def_floor_random WHERE id = 1"));
        assertTrue(val >= 0 && val < 60, "floor(random()*60)::INT must be in [0, 60), got " + val);
    }

    @Test
    void testFloorRandomSimpler() throws SQLException {
        exec("CREATE TABLE def_floor_simple (id serial PRIMARY KEY, bucket INT DEFAULT floor(random() * 10)::int)");
        exec("INSERT INTO def_floor_simple DEFAULT VALUES");
        int val = Integer.parseInt(query1("SELECT bucket FROM def_floor_simple WHERE id = 1"));
        assertTrue(val >= 0 && val < 10, "floor(random()*10)::int must be in [0, 10), got " + val);
    }

    @Test
    void testRandomTimesHundredCast() throws SQLException {
        exec("CREATE TABLE def_rand100 (id serial PRIMARY KEY, pct INT DEFAULT (random() * 100)::int)");
        exec("INSERT INTO def_rand100 DEFAULT VALUES");
        int val = Integer.parseInt(query1("SELECT pct FROM def_rand100 WHERE id = 1"));
        assertTrue(val >= 0 && val <= 100, "(random()*100)::int must be in [0, 100], got " + val);
    }

    @Test
    void testGenRandomUuidDefault() throws SQLException {
        exec("CREATE TABLE def_genuuid (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text)");
        exec("INSERT INTO def_genuuid (name) VALUES ('test')");
        String val = query1("SELECT id FROM def_genuuid WHERE name = 'test'");
        assertDoesNotThrow(() -> UUID.fromString(val), "Default must be a valid UUID, got: " + val);
    }

    @Test
    void testUuidGenerateV4Default() throws SQLException {
        exec("CREATE TABLE def_uuidv4 (id uuid PRIMARY KEY DEFAULT uuid_generate_v4(), name text)");
        exec("INSERT INTO def_uuidv4 (name) VALUES ('test')");
        String val = query1("SELECT id FROM def_uuidv4 WHERE name = 'test'");
        assertDoesNotThrow(() -> UUID.fromString(val), "Default must be a valid UUID, got: " + val);
    }

    @Test
    void testNowDefault() throws SQLException {
        exec("CREATE TABLE def_now (id serial PRIMARY KEY, created_at timestamp DEFAULT now())");
        exec("INSERT INTO def_now DEFAULT VALUES");
        assertNotNull(query1("SELECT created_at FROM def_now WHERE id = 1"));
    }

    @Test
    void testCurrentTimestampDefault() throws SQLException {
        exec("CREATE TABLE def_curts (id serial PRIMARY KEY, ts timestamptz DEFAULT CURRENT_TIMESTAMP)");
        exec("INSERT INTO def_curts DEFAULT VALUES");
        assertNotNull(query1("SELECT ts FROM def_curts WHERE id = 1"));
    }

    // =========================================================================
    // 2. Cast expression defaults
    // =========================================================================

    @Test
    void testEmptyJsonbDefault() throws SQLException {
        exec("CREATE TABLE def_jsonb (id serial PRIMARY KEY, data jsonb DEFAULT '{}'::jsonb)");
        exec("INSERT INTO def_jsonb DEFAULT VALUES");
        assertEquals("{}", query1("SELECT data FROM def_jsonb WHERE id = 1"));
    }

    @Test
    void testEmptyJsonbArrayDefault() throws SQLException {
        exec("CREATE TABLE def_jsonb_arr (id serial PRIMARY KEY, items jsonb DEFAULT '[]'::jsonb)");
        exec("INSERT INTO def_jsonb_arr DEFAULT VALUES");
        assertEquals("[]", query1("SELECT items FROM def_jsonb_arr WHERE id = 1"));
    }

    @Test
    void testEmptyTextDefault() throws SQLException {
        exec("CREATE TABLE def_emptytext (id serial PRIMARY KEY, notes text DEFAULT ''::text)");
        exec("INSERT INTO def_emptytext DEFAULT VALUES");
        assertEquals("", query1("SELECT notes FROM def_emptytext WHERE id = 1"));
    }

    @Test
    void testCastIntegerDefault() throws SQLException {
        exec("CREATE TABLE def_castint (id serial PRIMARY KEY, val bigint DEFAULT 0::bigint)");
        exec("INSERT INTO def_castint DEFAULT VALUES");
        assertEquals("0", query1("SELECT val FROM def_castint WHERE id = 1"));
    }

    @Test
    void testBooleanTrueDefault() throws SQLException {
        exec("CREATE TABLE def_bool (id serial PRIMARY KEY, active boolean DEFAULT true)");
        exec("INSERT INTO def_bool DEFAULT VALUES");
        String val = query1("SELECT active FROM def_bool WHERE id = 1");
        assertTrue("t".equals(val) || "true".equals(val));
    }

    @Test
    void testBooleanFalseDefault() throws SQLException {
        exec("CREATE TABLE def_boolfalse (id serial PRIMARY KEY, deleted boolean DEFAULT false)");
        exec("INSERT INTO def_boolfalse DEFAULT VALUES");
        String val = query1("SELECT deleted FROM def_boolfalse WHERE id = 1");
        assertTrue("f".equals(val) || "false".equals(val));
    }

    @Test
    void testNumericLiteralDefault() throws SQLException {
        exec("CREATE TABLE def_numlit (id serial PRIMARY KEY, rate numeric(5,2) DEFAULT 0.0)");
        exec("INSERT INTO def_numlit DEFAULT VALUES");
        BigDecimal val = new BigDecimal(query1("SELECT rate FROM def_numlit WHERE id = 1"));
        assertEquals(0, val.compareTo(BigDecimal.ZERO));
    }

    // =========================================================================
    // 3. Arithmetic expression defaults
    // =========================================================================

    @Test
    void testNegativeLiteralDefault() throws SQLException {
        exec("CREATE TABLE def_neg (id serial PRIMARY KEY, offset_val int DEFAULT (-1))");
        exec("INSERT INTO def_neg DEFAULT VALUES");
        assertEquals("-1", query1("SELECT offset_val FROM def_neg WHERE id = 1"));
    }

    @Test
    void testArithmeticDefault() throws SQLException {
        exec("CREATE TABLE def_arith (id serial PRIMARY KEY, val int DEFAULT 2 + 3)");
        exec("INSERT INTO def_arith DEFAULT VALUES");
        assertEquals("5", query1("SELECT val FROM def_arith WHERE id = 1"));
    }

    @Test
    void testNowPlusIntervalDefault() throws SQLException {
        exec("CREATE TABLE def_future (id serial PRIMARY KEY, expires_at timestamp DEFAULT now() + interval '30 days')");
        exec("INSERT INTO def_future DEFAULT VALUES");
        String val = query1("SELECT expires_at FROM def_future WHERE id = 1");
        assertNotNull(val, "now() + interval must evaluate to a timestamp");
        // The timestamp should be in the future
        Timestamp ts = Timestamp.valueOf(val);
        assertTrue(ts.after(new Timestamp(System.currentTimeMillis())),
                "Expiry must be in the future, got: " + val);
    }

    @Test
    void testCurrentDatePlusIntDefault() throws SQLException {
        exec("CREATE TABLE def_dateplus (id serial PRIMARY KEY, due_date date DEFAULT CURRENT_DATE + 7)");
        exec("INSERT INTO def_dateplus DEFAULT VALUES");
        assertNotNull(query1("SELECT due_date FROM def_dateplus WHERE id = 1"));
    }

    // =========================================================================
    // 4. Sequence defaults (pg_dump pattern)
    // =========================================================================

    @Test
    void testNextvalRegclassDefault() throws SQLException {
        exec("CREATE SEQUENCE def_seq_rc START WITH 100");
        exec("CREATE TABLE def_seqrc (id bigint DEFAULT nextval('def_seq_rc'::regclass), name text)");
        exec("INSERT INTO def_seqrc (name) VALUES ('first')");
        assertEquals("100", query1("SELECT id FROM def_seqrc WHERE name = 'first'"));
        exec("INSERT INTO def_seqrc (name) VALUES ('second')");
        assertEquals("101", query1("SELECT id FROM def_seqrc WHERE name = 'second'"));
    }

    @Test
    void testNextvalStringDefault() throws SQLException {
        exec("CREATE SEQUENCE def_seq_str START WITH 1");
        exec("CREATE TABLE def_seqstr (id bigint DEFAULT nextval('def_seq_str'), name text)");
        exec("INSERT INTO def_seqstr (name) VALUES ('a')");
        assertEquals("1", query1("SELECT id FROM def_seqstr WHERE name = 'a'"));
    }

    // =========================================================================
    // 5. String concatenation defaults
    // =========================================================================

    @Test
    void testStringConcatDefault() throws SQLException {
        exec("CREATE TABLE def_concat (id serial PRIMARY KEY, code text DEFAULT 'item_' || gen_random_uuid()::text)");
        exec("INSERT INTO def_concat DEFAULT VALUES");
        String val = query1("SELECT code FROM def_concat WHERE id = 1");
        assertTrue(val.startsWith("item_"), "Default must start with 'item_', got: " + val);
        assertTrue(val.length() > 10, "Default must include UUID suffix, got: " + val);
    }

    @Test
    void testUpperFunctionDefault() throws SQLException {
        exec("CREATE TABLE def_upper (id serial PRIMARY KEY, label text DEFAULT upper('hello'))");
        exec("INSERT INTO def_upper DEFAULT VALUES");
        assertEquals("HELLO", query1("SELECT label FROM def_upper WHERE id = 1"));
    }

    // =========================================================================
    // 6. Nested function defaults
    // =========================================================================

    @Test
    void testMd5RandomDefault() throws SQLException {
        exec("CREATE TABLE def_md5 (id serial PRIMARY KEY, hash text DEFAULT md5(random()::text))");
        exec("INSERT INTO def_md5 DEFAULT VALUES");
        String val = query1("SELECT hash FROM def_md5 WHERE id = 1");
        assertEquals(32, val.length(), "md5 hash must be 32 hex chars, got: " + val);
        assertTrue(val.matches("[0-9a-f]{32}"), "md5 must be hex, got: " + val);
    }

    @Test
    void testLengthFunctionDefault() throws SQLException {
        exec("CREATE TABLE def_len (id serial PRIMARY KEY, size int DEFAULT length('hello world'))");
        exec("INSERT INTO def_len DEFAULT VALUES");
        assertEquals("11", query1("SELECT size FROM def_len WHERE id = 1"));
    }

    @Test
    void testCoalesceDefault() throws SQLException {
        exec("CREATE TABLE def_coalesce (id serial PRIMARY KEY, val text DEFAULT COALESCE(NULL, 'fallback'))");
        exec("INSERT INTO def_coalesce DEFAULT VALUES");
        assertEquals("fallback", query1("SELECT val FROM def_coalesce WHERE id = 1"));
    }

    // =========================================================================
    // 7. Multiple rows: volatile defaults must produce different values per row
    // =========================================================================

    @Test
    void testVolatileDefaultProducesDifferentValues() throws SQLException {
        exec("CREATE TABLE def_volatile (id serial PRIMARY KEY, token uuid DEFAULT gen_random_uuid())");
        for (int i = 0; i < 5; i++) {
            exec("INSERT INTO def_volatile DEFAULT VALUES");
        }
        // All 5 UUIDs must be distinct
        Set<String> uuids = new HashSet<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT token FROM def_volatile")) {
            while (rs.next()) uuids.add(rs.getString(1));
        }
        assertEquals(5, uuids.size(), "Each row must get a unique UUID default");
    }

    @Test
    void testRandomDefaultProducesDifferentValues() throws SQLException {
        exec("CREATE TABLE def_rand_multi (id serial PRIMARY KEY, val double precision DEFAULT random())");
        for (int i = 0; i < 10; i++) {
            exec("INSERT INTO def_rand_multi DEFAULT VALUES");
        }
        // At least some values should differ (probability of all 10 being identical is negligible)
        Set<String> vals = new HashSet<>();
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT val FROM def_rand_multi")) {
            while (rs.next()) vals.add(rs.getString(1));
        }
        assertTrue(vals.size() > 1, "random() defaults must not all be identical");
    }

    // =========================================================================
    // 8. INSERT with partial columns, some explicit, some defaults
    // =========================================================================

    @Test
    void testPartialInsertWithComplexDefaults() throws SQLException {
        exec("""
            CREATE TABLE def_partial (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                name text NOT NULL,
                score int DEFAULT (floor(random() * 100))::int,
                metadata jsonb DEFAULT '{}'::jsonb,
                created_at timestamp DEFAULT now()
            )
        """);
        exec("INSERT INTO def_partial (name) VALUES ('partial_test')");

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name, score, metadata, created_at FROM def_partial")) {
            assertTrue(rs.next());
            // id: must be a valid UUID
            String id = rs.getString("id");
            assertDoesNotThrow(() -> UUID.fromString(id), "id must be UUID, got: " + id);
            // name: the explicit value
            assertEquals("partial_test", rs.getString("name"));
            // score: must be an integer 0-99
            int score = rs.getInt("score");
            assertFalse(rs.wasNull(), "score must not be null");
            assertTrue(score >= 0 && score < 100, "score must be in [0,100), got " + score);
            // metadata: empty JSON
            assertEquals("{}", rs.getString("metadata"));
            // created_at: must be a timestamp
            assertNotNull(rs.getTimestamp("created_at"));
        }
    }

    // =========================================================================
    // 9. TRUNCATE then re-insert; defaults still work after truncate
    // =========================================================================

    @Test
    void testDefaultsAfterTruncate() throws SQLException {
        exec("CREATE TABLE def_trunc (id serial PRIMARY KEY, val int DEFAULT (floor(random() * 1000))::int)");
        exec("INSERT INTO def_trunc DEFAULT VALUES");
        int before = Integer.parseInt(query1("SELECT val FROM def_trunc WHERE id = 1"));
        assertTrue(before >= 0 && before < 1000);

        exec("TRUNCATE def_trunc RESTART IDENTITY CASCADE");
        exec("INSERT INTO def_trunc DEFAULT VALUES");
        int after = Integer.parseInt(query1("SELECT val FROM def_trunc WHERE id = 1"));
        assertTrue(after >= 0 && after < 1000, "Default must still work after TRUNCATE, got: " + after);
    }

    // =========================================================================
    // 10. ALTER TABLE ADD COLUMN with complex default
    // =========================================================================

    @Test
    void testAlterAddColumnWithExpressionDefault() throws SQLException {
        exec("CREATE TABLE def_alter (id serial PRIMARY KEY, name text)");
        exec("INSERT INTO def_alter (name) VALUES ('existing')");
        exec("ALTER TABLE def_alter ADD COLUMN priority int DEFAULT (floor(random() * 10))::int");
        // Existing row should get the default value
        String val = query1("SELECT priority FROM def_alter WHERE name = 'existing'");
        int priority = Integer.parseInt(val);
        assertTrue(priority >= 0 && priority < 10,
                "Existing row's new column default must be in [0,10), got " + priority);
        // New row should also get a default
        exec("INSERT INTO def_alter (name) VALUES ('new_row')");
        int newPriority = Integer.parseInt(query1("SELECT priority FROM def_alter WHERE name = 'new_row'"));
        assertTrue(newPriority >= 0 && newPriority < 10);
    }

    // =========================================================================
    // 11. Multi-row INSERT with defaults
    // =========================================================================

    @Test
    void testMultiRowInsertWithDefaults() throws SQLException {
        exec("""
            CREATE TABLE def_multi (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                name text NOT NULL,
                seq_val int DEFAULT (floor(random() * 1000))::int
            )
        """);
        exec("INSERT INTO def_multi (name) VALUES ('a'), ('b'), ('c')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, seq_val FROM def_multi ORDER BY name")) {
            Set<String> ids = new HashSet<>();
            int count = 0;
            while (rs.next()) {
                count++;
                String id = rs.getString("id");
                ids.add(id);
                assertDoesNotThrow(() -> UUID.fromString(id));
                int seqVal = rs.getInt("seq_val");
                assertTrue(seqVal >= 0 && seqVal < 1000);
            }
            assertEquals(3, count);
            assertEquals(3, ids.size(), "Each row must get a unique UUID");
        }
    }

    // =========================================================================
    // 12. PreparedStatement INSERT with defaults
    // =========================================================================

    @Test
    void testPreparedInsertWithDefaults() throws SQLException {
        exec("""
            CREATE TABLE def_prepared (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                name text NOT NULL,
                created_at timestamp DEFAULT now(),
                score int DEFAULT (floor(random() * 100))::int
            )
        """);
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO def_prepared (name) VALUES (?) RETURNING id, score, created_at")) {
            for (int i = 0; i < 5; i++) {
                ps.setString(1, "item_" + i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    String id = rs.getString("id");
                    assertDoesNotThrow(() -> UUID.fromString(id));
                    int score = rs.getInt("score");
                    assertTrue(score >= 0 && score < 100, "score must be in [0,100), got " + score);
                    assertNotNull(rs.getTimestamp("created_at"));
                }
            }
        }
    }

    // =========================================================================
    // 13. Edge case: default with nested parenthesized cast
    // =========================================================================

    @Test
    void testDeepNestedCastDefault() throws SQLException {
        exec("CREATE TABLE def_deepcast (id serial PRIMARY KEY, val int DEFAULT ((((floor(random() * 50))))::int))");
        exec("INSERT INTO def_deepcast DEFAULT VALUES");
        int val = Integer.parseInt(query1("SELECT val FROM def_deepcast WHERE id = 1"));
        assertTrue(val >= 0 && val < 50, "Deeply nested cast default must evaluate, got " + val);
    }

    @Test
    void testGreatestFunctionDefault() throws SQLException {
        exec("CREATE TABLE def_greatest (id serial PRIMARY KEY, val int DEFAULT greatest(1, 2, 3))");
        exec("INSERT INTO def_greatest DEFAULT VALUES");
        assertEquals("3", query1("SELECT val FROM def_greatest WHERE id = 1"));
    }

    @Test
    void testLeastFunctionDefault() throws SQLException {
        exec("CREATE TABLE def_least (id serial PRIMARY KEY, val int DEFAULT least(10, 20, 5))");
        exec("INSERT INTO def_least DEFAULT VALUES");
        assertEquals("5", query1("SELECT val FROM def_least WHERE id = 1"));
    }

    @Test
    void testAbsFunctionDefault() throws SQLException {
        exec("CREATE TABLE def_abs (id serial PRIMARY KEY, val int DEFAULT abs(-42))");
        exec("INSERT INTO def_abs DEFAULT VALUES");
        assertEquals("42", query1("SELECT val FROM def_abs WHERE id = 1"));
    }
}
