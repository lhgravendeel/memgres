package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that SERIAL columns create real, usable sequences that are
 * tightly coupled with default value generation, matching PG 18 behavior.
 *
 * Covers: implicit sequence creation, nextval/currval/setval, ALTER SEQUENCE
 * affecting INSERT, DROP TABLE cascade, DROP SEQUENCE behavior, BIGSERIAL,
 * SMALLSERIAL, multiple serial columns, TRUNCATE RESTART IDENTITY.
 */
class SerialSequenceLifecycleTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private static String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static long queryLong(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    // =========================================================================
    // 1. Implicit sequence is created and discoverable
    // =========================================================================

    @Test void serial_creates_discoverable_sequence() throws SQLException {
        exec("CREATE TABLE slc_t1 (id serial PRIMARY KEY, val text)");
        try {
            String seqName = querySingle("SELECT pg_get_serial_sequence('slc_t1', 'id')");
            assertNotNull(seqName, "pg_get_serial_sequence should return a name");
            assertTrue(seqName.contains("slc_t1_id_seq"), "sequence name should follow tablename_colname_seq pattern");
        } finally {
            exec("DROP TABLE slc_t1");
        }
    }

    // =========================================================================
    // 2. INSERT uses the sequence (nextval called implicitly)
    // =========================================================================

    @Test void insert_uses_sequence_and_currval_tracks() throws SQLException {
        exec("CREATE TABLE slc_t2 (id serial PRIMARY KEY, val text)");
        try {
            exec("INSERT INTO slc_t2 (val) VALUES ('first')");
            assertEquals(1, queryInt("SELECT id FROM slc_t2 WHERE val = 'first'"));
            assertEquals(1, queryLong("SELECT currval('slc_t2_id_seq')"));

            exec("INSERT INTO slc_t2 (val) VALUES ('second')");
            assertEquals(2, queryInt("SELECT id FROM slc_t2 WHERE val = 'second'"));
            assertEquals(2, queryLong("SELECT currval('slc_t2_id_seq')"));
        } finally {
            exec("DROP TABLE slc_t2");
        }
    }

    // =========================================================================
    // 3. setval changes the next INSERT's id
    // =========================================================================

    @Test void setval_affects_next_insert() throws SQLException {
        exec("CREATE TABLE slc_t3 (id serial PRIMARY KEY, val text)");
        try {
            exec("INSERT INTO slc_t3 (val) VALUES ('before')");
            assertEquals(1, queryInt("SELECT id FROM slc_t3 WHERE val = 'before'"));

            exec("SELECT setval('slc_t3_id_seq', 100)");

            exec("INSERT INTO slc_t3 (val) VALUES ('after_setval')");
            assertEquals(101, queryInt("SELECT id FROM slc_t3 WHERE val = 'after_setval'"));
        } finally {
            exec("DROP TABLE slc_t3");
        }
    }

    // =========================================================================
    // 4. ALTER SEQUENCE ... RESTART WITH affects INSERT
    // =========================================================================

    @Test void alter_sequence_restart_affects_insert() throws SQLException {
        exec("CREATE TABLE slc_t4 (id serial PRIMARY KEY, val text)");
        try {
            exec("INSERT INTO slc_t4 (val) VALUES ('a')");
            assertEquals(1, queryInt("SELECT id FROM slc_t4 WHERE val = 'a'"));

            exec("ALTER SEQUENCE slc_t4_id_seq RESTART WITH 50");

            exec("INSERT INTO slc_t4 (val) VALUES ('b')");
            assertEquals(50, queryInt("SELECT id FROM slc_t4 WHERE val = 'b'"));
        } finally {
            exec("DROP TABLE slc_t4");
        }
    }

    // =========================================================================
    // 5. Manual nextval advances the sequence, skipping a value
    // =========================================================================

    @Test void manual_nextval_skips_value() throws SQLException {
        exec("CREATE TABLE slc_t5 (id serial PRIMARY KEY, val text)");
        try {
            exec("INSERT INTO slc_t5 (val) VALUES ('row1')");
            assertEquals(1, queryInt("SELECT id FROM slc_t5 WHERE val = 'row1'"));

            // Manually advance (this consumes value 2)
            queryLong("SELECT nextval('slc_t5_id_seq')");

            exec("INSERT INTO slc_t5 (val) VALUES ('row2')");
            assertEquals(3, queryInt("SELECT id FROM slc_t5 WHERE val = 'row2'"));
        } finally {
            exec("DROP TABLE slc_t5");
        }
    }

    // =========================================================================
    // 6. Sequence last_value matches after multiple inserts
    // =========================================================================

    @Test void sequence_last_value_tracks_inserts() throws SQLException {
        exec("CREATE TABLE slc_t6 (id serial PRIMARY KEY, val text)");
        try {
            exec("INSERT INTO slc_t6 (val) VALUES ('x'), ('y'), ('z')");
            assertEquals(3, queryLong("SELECT last_value FROM slc_t6_id_seq"));
            assertEquals(3, queryInt("SELECT count(*) FROM slc_t6"));
        } finally {
            exec("DROP TABLE slc_t6");
        }
    }

    // =========================================================================
    // 7. BIGSERIAL creates and uses a sequence
    // =========================================================================

    @Test void bigserial_creates_and_uses_sequence() throws SQLException {
        exec("CREATE TABLE slc_t7 (id bigserial PRIMARY KEY, val text)");
        try {
            String seqName = querySingle("SELECT pg_get_serial_sequence('slc_t7', 'id')");
            assertNotNull(seqName);
            assertTrue(seqName.contains("slc_t7_id_seq"));

            exec("INSERT INTO slc_t7 (val) VALUES ('big1')");
            assertEquals(1L, queryLong("SELECT id FROM slc_t7 WHERE val = 'big1'"));
            assertEquals(1L, queryLong("SELECT last_value FROM slc_t7_id_seq"));
        } finally {
            exec("DROP TABLE slc_t7");
        }
    }

    // =========================================================================
    // 8. SMALLSERIAL creates and uses a sequence
    // =========================================================================

    @Test void smallserial_creates_and_uses_sequence() throws SQLException {
        exec("CREATE TABLE slc_t8 (id smallserial PRIMARY KEY, val text)");
        try {
            String seqName = querySingle("SELECT pg_get_serial_sequence('slc_t8', 'id')");
            assertNotNull(seqName);
            assertTrue(seqName.contains("slc_t8_id_seq"));

            exec("INSERT INTO slc_t8 (val) VALUES ('small1')");
            assertEquals(1, queryInt("SELECT id FROM slc_t8 WHERE val = 'small1'"));
        } finally {
            exec("DROP TABLE slc_t8");
        }
    }

    // =========================================================================
    // 9. DROP TABLE cascades to implicit sequence
    // =========================================================================

    @Test void drop_table_drops_implicit_sequence() throws SQLException {
        exec("CREATE TABLE slc_t9 (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO slc_t9 (val) VALUES ('x')");

        // Sequence should exist
        assertEquals(1, queryInt(
                "SELECT count(*) FROM pg_sequences WHERE sequencename = 'slc_t9_id_seq'"));

        exec("DROP TABLE slc_t9");

        // Sequence should be gone
        assertEquals(0, queryInt(
                "SELECT count(*) FROM pg_sequences WHERE sequencename = 'slc_t9_id_seq'"));
    }

    // =========================================================================
    // 10. DROP SEQUENCE on serial's sequence — rejected without CASCADE
    // =========================================================================

    @Test void drop_serial_sequence_without_cascade_rejected() throws SQLException {
        exec("CREATE TABLE slc_t10 (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO slc_t10 (val) VALUES ('keep')");
        try {
            // PG rejects because column depends on it
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("DROP SEQUENCE slc_t10_id_seq"));
            assertTrue(ex.getMessage().contains("cannot drop") || ex.getMessage().contains("depend"),
                    "Error should mention dependency: " + ex.getMessage());

            // Table still works
            assertEquals(1, queryInt("SELECT count(*) FROM slc_t10"));
        } finally {
            exec("DROP TABLE slc_t10");
        }
    }

    // =========================================================================
    // 11. DROP SEQUENCE ... CASCADE drops default but keeps table
    // =========================================================================

    @Test void drop_serial_sequence_cascade_removes_default() throws SQLException {
        exec("CREATE TABLE slc_t11 (id serial PRIMARY KEY, val text)");
        exec("INSERT INTO slc_t11 (val) VALUES ('keep')");
        try {
            exec("DROP SEQUENCE slc_t11_id_seq CASCADE");

            // Table still exists with its data
            assertEquals(1, queryInt("SELECT count(*) FROM slc_t11"));

            // Insert without explicit id should fail — no default anymore
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO slc_t11 (val) VALUES ('no_default')"));
            assertTrue(ex.getMessage().contains("null") || ex.getMessage().contains("not-null"),
                    "Error should be about null/not-null: " + ex.getMessage());
        } finally {
            exec("DROP TABLE slc_t11");
        }
    }

    // =========================================================================
    // 12. Multiple serial columns on the same table
    // =========================================================================

    @Test void multiple_serial_columns_independent_sequences() throws SQLException {
        exec("CREATE TABLE slc_t12 (a serial, b serial, val text)");
        try {
            exec("INSERT INTO slc_t12 (val) VALUES ('multi')");
            assertEquals(1, queryInt("SELECT a FROM slc_t12 WHERE val = 'multi'"));
            assertEquals(1, queryInt("SELECT b FROM slc_t12 WHERE val = 'multi'"));

            // Advance only sequence b
            exec("SELECT setval('slc_t12_b_seq', 10)");

            exec("INSERT INTO slc_t12 (val) VALUES ('multi2')");
            assertEquals(2, queryInt("SELECT a FROM slc_t12 WHERE val = 'multi2'"));
            assertEquals(11, queryInt("SELECT b FROM slc_t12 WHERE val = 'multi2'"));
        } finally {
            exec("DROP TABLE slc_t12");
        }
    }

    // =========================================================================
    // 13. setval with is_called=false — next value is exactly that value
    // =========================================================================

    @Test void setval_is_called_false_returns_exact_value() throws SQLException {
        exec("CREATE TABLE slc_t13 (id serial PRIMARY KEY, val text)");
        try {
            exec("SELECT setval('slc_t13_id_seq', 42, false)");

            exec("INSERT INTO slc_t13 (val) VALUES ('exact')");
            assertEquals(42, queryInt("SELECT id FROM slc_t13 WHERE val = 'exact'"));

            exec("INSERT INTO slc_t13 (val) VALUES ('next')");
            assertEquals(43, queryInt("SELECT id FROM slc_t13 WHERE val = 'next'"));
        } finally {
            exec("DROP TABLE slc_t13");
        }
    }

    // =========================================================================
    // 14. TRUNCATE ... RESTART IDENTITY resets sequence
    // =========================================================================

    @Test void truncate_restart_identity_resets_sequence() throws SQLException {
        exec("CREATE TABLE slc_t14 (id serial PRIMARY KEY, val text)");
        try {
            exec("INSERT INTO slc_t14 (val) VALUES ('a'), ('b'), ('c')");
            assertEquals(3L, queryLong("SELECT last_value FROM slc_t14_id_seq"));

            exec("TRUNCATE slc_t14 RESTART IDENTITY");

            exec("INSERT INTO slc_t14 (val) VALUES ('fresh')");
            assertEquals(1, queryInt("SELECT id FROM slc_t14 WHERE val = 'fresh'"));
        } finally {
            exec("DROP TABLE slc_t14");
        }
    }

    // =========================================================================
    // 15. pg_get_serial_sequence returns schema-qualified name matching table's schema
    // =========================================================================

    @Test void pg_get_serial_sequence_respects_schema() throws SQLException {
        exec("CREATE SCHEMA slc_schema");
        try {
            exec("CREATE TABLE slc_schema.st (id serial PRIMARY KEY, val text)");
            String seqName = querySingle("SELECT pg_get_serial_sequence('slc_schema.st', 'id')");
            assertNotNull(seqName);
            // PG returns schema-qualified name in the table's schema, not 'public'
            assertTrue(seqName.startsWith("slc_schema."),
                    "sequence should be in slc_schema, got: " + seqName);
            exec("DROP TABLE slc_schema.st");
        } finally {
            exec("DROP SCHEMA slc_schema CASCADE");
        }
    }

    // =========================================================================
    // 16. Sequence exists as a queryable relation immediately after CREATE TABLE
    // =========================================================================

    @Test void sequence_queryable_as_relation_after_create_table() throws SQLException {
        exec("CREATE TABLE slc_t16 (id serial PRIMARY KEY, val text)");
        try {
            // PG creates the sequence at CREATE TABLE time — it should be queryable
            // without any prior INSERT, nextval, or pg_get_serial_sequence call
            long lastVal = queryLong("SELECT last_value FROM slc_t16_id_seq");
            assertEquals(1L, lastVal, "initial last_value should be 1");
        } finally {
            exec("DROP TABLE slc_t16");
        }
    }

    // =========================================================================
    // 17. Sequence visible in pg_sequences immediately after CREATE TABLE
    // =========================================================================

    @Test void sequence_visible_in_pg_sequences_after_create_table() throws SQLException {
        exec("CREATE TABLE slc_t17 (id serial PRIMARY KEY, val text)");
        try {
            // Without any INSERT or nextval — sequence should be in pg_sequences
            assertEquals(1, queryInt(
                    "SELECT count(*) FROM pg_sequences WHERE sequencename = 'slc_t17_id_seq'"));
        } finally {
            exec("DROP TABLE slc_t17");
        }
    }

    // =========================================================================
    // 18. After DROP SEQUENCE CASCADE, INSERT without id fails (no default)
    // =========================================================================

    @Test void insert_fails_after_drop_sequence_cascade() throws SQLException {
        exec("CREATE TABLE slc_t18 (id serial NOT NULL, val text)");
        try {
            exec("INSERT INTO slc_t18 (val) VALUES ('before')");
            assertEquals(1, queryInt("SELECT id FROM slc_t18 WHERE val = 'before'"));

            // DROP SEQUENCE CASCADE should remove the column default
            exec("DROP SEQUENCE slc_t18_id_seq CASCADE");

            // INSERT without explicit id should now fail — no default, NOT NULL constraint
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO slc_t18 (val) VALUES ('after')"));
            assertTrue(ex.getMessage().contains("null") || ex.getMessage().contains("not-null")
                    || ex.getSQLState().equals("23502"),
                    "Should get not-null violation, got: " + ex.getMessage());
        } finally {
            exec("DROP TABLE IF EXISTS slc_t18");
        }
    }

    // =========================================================================
    // 19. setval syncs with serial counter — INSERT after setval uses sequence value
    //     This specifically tests that the column's DEFAULT calls nextval(),
    //     not an independent internal counter.
    // =========================================================================

    @Test void column_default_is_nextval_not_internal_counter() throws SQLException {
        exec("CREATE TABLE slc_t19 (id serial PRIMARY KEY, val text)");
        try {
            // Insert two rows to advance to id=2
            exec("INSERT INTO slc_t19 (val) VALUES ('a'), ('b')");
            assertEquals(2, queryInt("SELECT max(id) FROM slc_t19"));

            // Advance sequence via nextval (should consume id=3)
            long consumed = queryLong("SELECT nextval('slc_t19_id_seq')");
            assertEquals(3L, consumed, "nextval should return 3");

            // currval should reflect the nextval call
            assertEquals(3L, queryLong("SELECT currval('slc_t19_id_seq')"));

            // Next INSERT should get id=4 (skipping 3 which was consumed)
            exec("INSERT INTO slc_t19 (val) VALUES ('c')");
            assertEquals(4, queryInt("SELECT id FROM slc_t19 WHERE val = 'c'"));
        } finally {
            exec("DROP TABLE slc_t19");
        }
    }

    // =========================================================================
    // 20. setval with NULL is a no-op (PG 18 behavior)
    // =========================================================================

    @Test void setval_null_is_noop() throws SQLException {
        exec("CREATE TABLE slc_t21 (id serial PRIMARY KEY)");
        try {
            exec("INSERT INTO slc_t21 DEFAULT VALUES"); // id = 1
            assertEquals(1, queryInt("SELECT id FROM slc_t21"));

            // setval with NULL literal — PG returns NULL, sequence unchanged
            String setvalResult = querySingle("SELECT setval('slc_t21_id_seq', NULL)");
            assertNull(setvalResult, "setval(seq, NULL) should return NULL");

            // Next insert should still get id = 2
            exec("INSERT INTO slc_t21 DEFAULT VALUES");
            assertEquals(2, queryInt("SELECT max(id) FROM slc_t21"));
        } finally {
            exec("DROP TABLE slc_t21");
        }
    }

    // =========================================================================
    // 21. setval with NULL from subquery (empty table MAX pattern)
    // =========================================================================

    @Test void setval_null_from_empty_max_subquery() throws SQLException {
        exec("CREATE TABLE slc_t22 (id serial PRIMARY KEY)");
        try {
            // Empty table — max returns NULL
            assertNull(querySingle("SELECT max(id) FROM slc_t22"));

            // setval with NULL subquery — should be a no-op
            String setvalResult = querySingle(
                    "SELECT setval('slc_t22_id_seq', (SELECT max(id) FROM slc_t22))");
            assertNull(setvalResult, "setval(seq, NULL subquery) should return NULL");

            // First insert should get id = 1
            exec("INSERT INTO slc_t22 DEFAULT VALUES");
            assertEquals(1, queryInt("SELECT id FROM slc_t22"));
        } finally {
            exec("DROP TABLE slc_t22");
        }
    }

    // =========================================================================
    // 22. setval NULL after real inserts — sequence position preserved
    // =========================================================================

    @Test void setval_null_after_inserts_preserves_position() throws SQLException {
        exec("CREATE TABLE slc_t23 (id serial PRIMARY KEY)");
        try {
            exec("INSERT INTO slc_t23 DEFAULT VALUES"); // 1
            exec("INSERT INTO slc_t23 DEFAULT VALUES"); // 2
            exec("INSERT INTO slc_t23 DEFAULT VALUES"); // 3

            // setval NULL should not reset the sequence
            String setvalResult = querySingle("SELECT setval('slc_t23_id_seq', NULL)");
            assertNull(setvalResult, "setval(seq, NULL) should return NULL");

            // Next insert should get id = 4
            exec("INSERT INTO slc_t23 DEFAULT VALUES");
            assertEquals(4, queryInt("SELECT max(id) FROM slc_t23"));
        } finally {
            exec("DROP TABLE slc_t23");
        }
    }

    // =========================================================================
    // 23. Verify column default expression is nextval('seq'::regclass)
    // =========================================================================

    @Test void column_default_expression_references_sequence() throws SQLException {
        exec("CREATE TABLE slc_t20 (id serial PRIMARY KEY, val text)");
        try {
            // PG stores the default as nextval('tablename_colname_seq'::regclass)
            String colDefault = querySingle(
                    "SELECT column_default FROM information_schema.columns " +
                    "WHERE table_name = 'slc_t20' AND column_name = 'id'");
            assertNotNull(colDefault, "serial column should have a default");
            assertTrue(colDefault.contains("nextval") && colDefault.contains("slc_t20_id_seq"),
                    "Default should reference nextval and sequence name, got: " + colDefault);
        } finally {
            exec("DROP TABLE slc_t20");
        }
    }
}
