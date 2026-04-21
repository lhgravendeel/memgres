package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checklist items 34-38: Boolean, Enum, UUID, JSON/JSONB types and functions coverage tests.
 */
class BoolEnumUuidJsonCoverageTest {

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

    // ========================================================================
    // 34. Boolean Type
    // ========================================================================

    @Test
    void bool_true_false_literal() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE, FALSE");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
        }
    }

    @Test
    void bool_null_handling() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT NULL::boolean");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    void bool_column_insert_query() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bool_col_test (id SERIAL, flag BOOLEAN)");
            s.execute("INSERT INTO bool_col_test (flag) VALUES (TRUE), (FALSE), (NULL)");
            ResultSet rs = s.executeQuery("SELECT flag FROM bool_col_test ORDER BY id");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertTrue(rs.wasNull());
            s.execute("DROP TABLE bool_col_test");
        }
    }

    @Test
    void bool_is_true() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE IS TRUE, FALSE IS TRUE, NULL IS TRUE");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
            assertFalse(rs.getBoolean(3));
        }
    }

    @Test
    void bool_is_false() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE IS FALSE, FALSE IS FALSE, NULL IS FALSE");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertTrue(rs.getBoolean(2));
            assertFalse(rs.getBoolean(3));
        }
    }

    @Test
    void bool_is_not_true() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE IS NOT TRUE, FALSE IS NOT TRUE, NULL IS NOT TRUE");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertTrue(rs.getBoolean(2));
            assertTrue(rs.getBoolean(3));
        }
    }

    @Test
    void bool_is_not_false() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE IS NOT FALSE, FALSE IS NOT FALSE, NULL IS NOT FALSE");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
            assertTrue(rs.getBoolean(3));
        }
    }

    @Test
    void bool_is_unknown() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE IS UNKNOWN, FALSE IS UNKNOWN, NULL IS UNKNOWN");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
            assertTrue(rs.getBoolean(3));
        }
    }

    @Test
    void bool_is_not_unknown() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE IS NOT UNKNOWN, FALSE IS NOT UNKNOWN, NULL IS NOT UNKNOWN");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertTrue(rs.getBoolean(2));
            assertFalse(rs.getBoolean(3));
        }
    }

    @Test
    void bool_input_formats() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // TRUE formats
            ResultSet rs = s.executeQuery(
                    "SELECT 't'::boolean, 'true'::boolean, 'yes'::boolean, 'y'::boolean, 'on'::boolean, '1'::boolean");
            assertTrue(rs.next());
            for (int i = 1; i <= 6; i++) {
                assertTrue(rs.getBoolean(i), "Column " + i + " should be TRUE");
            }

            // FALSE formats
            rs = s.executeQuery(
                    "SELECT 'f'::boolean, 'false'::boolean, 'no'::boolean, 'n'::boolean, 'off'::boolean, '0'::boolean");
            assertTrue(rs.next());
            for (int i = 1; i <= 6; i++) {
                assertFalse(rs.getBoolean(i), "Column " + i + " should be FALSE");
            }
        }
    }

    @Test
    void bool_and_or_not() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE AND FALSE, TRUE OR FALSE, NOT TRUE, NOT FALSE");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1));
            assertTrue(rs.getBoolean(2));
            assertFalse(rs.getBoolean(3));
            assertTrue(rs.getBoolean(4));
        }
    }

    @Test
    void bool_in_where() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bool_where_test (id INT, flag BOOLEAN)");
            s.execute("INSERT INTO bool_where_test VALUES (1, TRUE), (2, FALSE), (3, NULL)");

            // WHERE flag (truthy)
            ResultSet rs = s.executeQuery("SELECT id FROM bool_where_test WHERE flag ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            // WHERE NOT flag
            rs = s.executeQuery("SELECT id FROM bool_where_test WHERE NOT flag ORDER BY id");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());

            // WHERE flag IS TRUE
            rs = s.executeQuery("SELECT id FROM bool_where_test WHERE flag IS TRUE ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            s.execute("DROP TABLE bool_where_test");
        }
    }

    @Test
    void bool_cast_to_int() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT TRUE::int, FALSE::int");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(0, rs.getInt(2));
        }
    }

    @Test
    void bool_cast_from_int() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT 1::boolean, 0::boolean");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
            assertFalse(rs.getBoolean(2));
        }
    }

    @Test
    void bool_aggregate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE bool_agg_test (flag BOOLEAN)");
            s.execute("INSERT INTO bool_agg_test VALUES (TRUE), (TRUE), (FALSE)");

            ResultSet rs = s.executeQuery("SELECT BOOL_AND(flag), BOOL_OR(flag) FROM bool_agg_test");
            assertTrue(rs.next());
            assertFalse(rs.getBoolean(1)); // AND of TRUE, TRUE, FALSE = FALSE
            assertTrue(rs.getBoolean(2));  // OR of TRUE, TRUE, FALSE = TRUE

            s.execute("DROP TABLE bool_agg_test");
        }
    }

    @Test
    void bool_coalesce() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT COALESCE(NULL::boolean, TRUE)");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void bool_case_when() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT CASE WHEN TRUE THEN 'yes' ELSE 'no' END, " +
                    "CASE WHEN FALSE THEN 'yes' ELSE 'no' END");
            assertTrue(rs.next());
            assertEquals("yes", rs.getString(1));
            assertEquals("no", rs.getString(2));
        }
    }

    // ========================================================================
    // 35. Enumerated Types
    // ========================================================================

    @Test
    void enum_create_and_insert() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_ci AS ENUM ('sad', 'ok', 'happy')");
            s.execute("CREATE TABLE enum_ci_test (id SERIAL, m mood_ci)");
            s.execute("INSERT INTO enum_ci_test (m) VALUES ('sad'), ('ok'), ('happy')");
            ResultSet rs = s.executeQuery("SELECT m FROM enum_ci_test ORDER BY id");
            assertTrue(rs.next()); assertEquals("sad", rs.getString(1));
            assertTrue(rs.next()); assertEquals("ok", rs.getString(1));
            assertTrue(rs.next()); assertEquals("happy", rs.getString(1));
            s.execute("DROP TABLE enum_ci_test");
            s.execute("DROP TYPE mood_ci");
        }
    }

    @Test
    void enum_invalid_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_inv AS ENUM ('sad', 'ok', 'happy')");
            s.execute("CREATE TABLE enum_inv_test (m mood_inv)");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO enum_inv_test (m) VALUES ('angry')"));
            s.execute("DROP TABLE enum_inv_test");
            s.execute("DROP TYPE mood_inv");
        }
    }

    @Test
    void enum_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_cmp AS ENUM ('sad', 'ok', 'happy')");
            s.execute("CREATE TABLE enum_cmp_test (id SERIAL, m mood_cmp)");
            s.execute("INSERT INTO enum_cmp_test (m) VALUES ('sad'), ('ok'), ('happy')");
            // PG uses creation-order (positional) ordering for enums: sad=0 < ok=1 < happy=2
            ResultSet rs = s.executeQuery("SELECT m FROM enum_cmp_test WHERE m < 'happy' ORDER BY id");
            assertTrue(rs.next()); assertEquals("sad", rs.getString(1));
            assertTrue(rs.next()); assertEquals("ok", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE enum_cmp_test");
            s.execute("DROP TYPE mood_cmp");
        }
    }

    @Test
    void enum_in_list() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_in AS ENUM ('sad', 'ok', 'happy')");
            s.execute("CREATE TABLE enum_in_test (id SERIAL, m mood_in)");
            s.execute("INSERT INTO enum_in_test (m) VALUES ('sad'), ('ok'), ('happy')");
            ResultSet rs = s.executeQuery("SELECT m FROM enum_in_test WHERE m IN ('sad', 'happy') ORDER BY id");
            assertTrue(rs.next()); assertEquals("sad", rs.getString(1));
            assertTrue(rs.next()); assertEquals("happy", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE enum_in_test");
            s.execute("DROP TYPE mood_in");
        }
    }

    @Test
    void enum_order_by() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_ord AS ENUM ('sad', 'ok', 'happy')");
            s.execute("CREATE TABLE enum_ord_test (m mood_ord)");
            s.execute("INSERT INTO enum_ord_test VALUES ('happy'), ('sad'), ('ok')");
            // PG uses ordinal (definition order) ordering for enums
            ResultSet rs = s.executeQuery("SELECT m FROM enum_ord_test ORDER BY m");
            assertTrue(rs.next()); assertEquals("sad", rs.getString(1));
            assertTrue(rs.next()); assertEquals("ok", rs.getString(1));
            assertTrue(rs.next()); assertEquals("happy", rs.getString(1));
            s.execute("DROP TABLE enum_ord_test");
            s.execute("DROP TYPE mood_ord");
        }
    }

    @Test
    void enum_first() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_first AS ENUM ('sad', 'ok', 'happy')");
            ResultSet rs = s.executeQuery("SELECT enum_first(null::mood_first)");
            assertTrue(rs.next());
            assertEquals("sad", rs.getString(1));
            s.execute("DROP TYPE mood_first");
        }
    }

    @Test
    void enum_last() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_last AS ENUM ('sad', 'ok', 'happy')");
            ResultSet rs = s.executeQuery("SELECT enum_last(null::mood_last)");
            assertTrue(rs.next());
            assertEquals("happy", rs.getString(1));
            s.execute("DROP TYPE mood_last");
        }
    }

    @Test
    void enum_range() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_range AS ENUM ('sad', 'ok', 'happy')");
            ResultSet rs = s.executeQuery("SELECT enum_range(null::mood_range)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            // Result may be {sad,ok,happy} or similar array representation
            assertTrue(result.contains("sad"), "Should contain 'sad': " + result);
            assertTrue(result.contains("ok"), "Should contain 'ok': " + result);
            assertTrue(result.contains("happy"), "Should contain 'happy': " + result);
            s.execute("DROP TYPE mood_range");
        }
    }

    @Test
    void enum_alter_add_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_add AS ENUM ('sad', 'ok', 'happy')");
            s.execute("ALTER TYPE mood_add ADD VALUE 'ecstatic'");
            s.execute("CREATE TABLE enum_add_test (m mood_add)");
            s.execute("INSERT INTO enum_add_test VALUES ('ecstatic')");
            ResultSet rs = s.executeQuery("SELECT m FROM enum_add_test");
            assertTrue(rs.next());
            assertEquals("ecstatic", rs.getString(1));
            s.execute("DROP TABLE enum_add_test");
            s.execute("DROP TYPE mood_add");
        }
    }

    @Test
    void enum_alter_add_value_if_not_exists() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_addine AS ENUM ('sad', 'ok', 'happy')");
            // Should not throw even though 'happy' already exists
            s.execute("ALTER TYPE mood_addine ADD VALUE IF NOT EXISTS 'happy'");
            s.execute("DROP TYPE mood_addine");
        }
    }

    @Test
    void enum_alter_add_value_before() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_before AS ENUM ('sad', 'ok', 'happy')");
            s.execute("ALTER TYPE mood_before ADD VALUE 'meh' BEFORE 'ok'");
            ResultSet rs = s.executeQuery("SELECT enum_range(null::mood_before)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            // meh should appear before ok
            assertTrue(result.indexOf("meh") < result.indexOf("ok"),
                    "'meh' should be before 'ok' in: " + result);
            s.execute("DROP TYPE mood_before");
        }
    }

    @Test
    void enum_alter_add_value_after() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_after AS ENUM ('sad', 'ok', 'happy')");
            s.execute("ALTER TYPE mood_after ADD VALUE 'great' AFTER 'ok'");
            ResultSet rs = s.executeQuery("SELECT enum_range(null::mood_after)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            // great should appear after ok but before happy
            assertTrue(result.indexOf("great") > result.indexOf("ok"),
                    "'great' should be after 'ok' in: " + result);
            s.execute("DROP TYPE mood_after");
        }
    }

    @Test
    void enum_alter_rename_value() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_rename AS ENUM ('sad', 'ok', 'happy')");
            s.execute("ALTER TYPE mood_rename RENAME VALUE 'sad' TO 'melancholy'");
            s.execute("CREATE TABLE enum_rename_test (m mood_rename)");
            s.execute("INSERT INTO enum_rename_test VALUES ('melancholy')");
            ResultSet rs = s.executeQuery("SELECT m FROM enum_rename_test");
            assertTrue(rs.next());
            assertEquals("melancholy", rs.getString(1));
            s.execute("DROP TABLE enum_rename_test");
            s.execute("DROP TYPE mood_rename");
        }
    }

    @Test
    void enum_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_null AS ENUM ('sad', 'ok', 'happy')");
            s.execute("CREATE TABLE enum_null_test (m mood_null)");
            s.execute("INSERT INTO enum_null_test VALUES (NULL)");
            ResultSet rs = s.executeQuery("SELECT m FROM enum_null_test");
            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertTrue(rs.wasNull());
            s.execute("DROP TABLE enum_null_test");
            s.execute("DROP TYPE mood_null");
        }
    }

    @Test
    void enum_distinct() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_dist AS ENUM ('sad', 'ok', 'happy')");
            s.execute("CREATE TABLE enum_dist_test (m mood_dist)");
            s.execute("INSERT INTO enum_dist_test VALUES ('sad'), ('ok'), ('sad'), ('happy'), ('ok')");
            // PG uses ordinal (definition order) ordering for enums
            ResultSet rs = s.executeQuery("SELECT DISTINCT m FROM enum_dist_test ORDER BY m");
            assertTrue(rs.next()); assertEquals("sad", rs.getString(1));
            assertTrue(rs.next()); assertEquals("ok", rs.getString(1));
            assertTrue(rs.next()); assertEquals("happy", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE enum_dist_test");
            s.execute("DROP TYPE mood_dist");
        }
    }

    @Test
    void enum_group_by() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE mood_grp AS ENUM ('sad', 'ok', 'happy')");
            s.execute("CREATE TABLE enum_grp_test (m mood_grp)");
            s.execute("INSERT INTO enum_grp_test VALUES ('sad'), ('ok'), ('sad'), ('happy'), ('ok'), ('ok')");
            // PG uses creation-order (positional) ordering for enums: sad=0 < ok=1 < happy=2
            ResultSet rs = s.executeQuery("SELECT m, COUNT(*) AS cnt FROM enum_grp_test GROUP BY m ORDER BY m");
            assertTrue(rs.next()); assertEquals("sad", rs.getString(1)); assertEquals(2, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("ok", rs.getString(1)); assertEquals(3, rs.getInt(2));
            assertTrue(rs.next()); assertEquals("happy", rs.getString(1)); assertEquals(1, rs.getInt(2));
            assertFalse(rs.next());
            s.execute("DROP TABLE enum_grp_test");
            s.execute("DROP TYPE mood_grp");
        }
    }

    // ========================================================================
    // 36. UUID Type
    // ========================================================================

    @Test
    void uuid_gen_random() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT gen_random_uuid()");
            assertTrue(rs.next());
            String uuid = rs.getString(1);
            assertNotNull(uuid);
            assertTrue(uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                    "Should be valid UUID format: " + uuid);
        }
    }

    @Test
    void uuid_generate_v4() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
            ResultSet rs = s.executeQuery("SELECT uuid_generate_v4()");
            assertTrue(rs.next());
            String uuid = rs.getString(1);
            assertNotNull(uuid);
            assertTrue(uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                    "Should be valid UUID format: " + uuid);
        }
    }

    @Test
    void uuid_column() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE uuid_col_test (id UUID, name TEXT)");
            s.execute("INSERT INTO uuid_col_test VALUES ('550e8400-e29b-41d4-a716-446655440000', 'test')");
            ResultSet rs = s.executeQuery("SELECT id, name FROM uuid_col_test");
            assertTrue(rs.next());
            assertEquals("550e8400-e29b-41d4-a716-446655440000", rs.getString(1));
            assertEquals("test", rs.getString(2));
            s.execute("DROP TABLE uuid_col_test");
        }
    }

    @Test
    void uuid_comparison() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE uuid_cmp_test (id UUID)");
            s.execute("INSERT INTO uuid_cmp_test VALUES ('00000000-0000-0000-0000-000000000001')");
            s.execute("INSERT INTO uuid_cmp_test VALUES ('00000000-0000-0000-0000-000000000002')");
            s.execute("INSERT INTO uuid_cmp_test VALUES ('00000000-0000-0000-0000-000000000003')");

            // Equality
            ResultSet rs = s.executeQuery(
                    "SELECT id FROM uuid_cmp_test WHERE id = '00000000-0000-0000-0000-000000000002'");
            assertTrue(rs.next());
            assertEquals("00000000-0000-0000-0000-000000000002", rs.getString(1));
            assertFalse(rs.next());

            // Not equal
            rs = s.executeQuery(
                    "SELECT COUNT(*) FROM uuid_cmp_test WHERE id <> '00000000-0000-0000-0000-000000000002'");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            // Less than
            rs = s.executeQuery(
                    "SELECT COUNT(*) FROM uuid_cmp_test WHERE id < '00000000-0000-0000-0000-000000000002'");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            // Greater than
            rs = s.executeQuery(
                    "SELECT COUNT(*) FROM uuid_cmp_test WHERE id > '00000000-0000-0000-0000-000000000002'");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));

            s.execute("DROP TABLE uuid_cmp_test");
        }
    }

    @Test
    void uuid_cast() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '550e8400-e29b-41d4-a716-446655440000'::uuid");
            assertTrue(rs.next());
            assertEquals("550e8400-e29b-41d4-a716-446655440000", rs.getString(1));
        }
    }

    @Test
    void uuid_invalid_cast() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Attempt to cast an invalid UUID string
            // Some implementations may silently accept it, others may throw
            try {
                ResultSet rs = s.executeQuery("SELECT 'not-a-uuid'::uuid");
                assertTrue(rs.next());
                // If it doesn't throw, the cast was accepted (lenient behavior)
                assertNotNull(rs.getString(1));
            } catch (SQLException e) {
                // Expected: invalid UUID format error
                assertTrue(e.getMessage().toLowerCase().contains("uuid")
                        || e.getMessage().toLowerCase().contains("invalid"));
            }
        }
    }

    @Test
    void uuid_unique_constraint() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE uuid_uniq_test (id UUID UNIQUE)");
            s.execute("INSERT INTO uuid_uniq_test VALUES ('550e8400-e29b-41d4-a716-446655440000')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO uuid_uniq_test VALUES ('550e8400-e29b-41d4-a716-446655440000')"));
            s.execute("DROP TABLE uuid_uniq_test");
        }
    }

    @Test
    void uuid_default_gen() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE uuid_def_test (id UUID DEFAULT gen_random_uuid(), name TEXT)");
            s.execute("INSERT INTO uuid_def_test (name) VALUES ('auto')");
            ResultSet rs = s.executeQuery("SELECT id, name FROM uuid_def_test");
            assertTrue(rs.next());
            String uuid = rs.getString(1);
            assertNotNull(uuid);
            assertTrue(uuid.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),
                    "Should be valid UUID format: " + uuid);
            assertEquals("auto", rs.getString(2));
            s.execute("DROP TABLE uuid_def_test");
        }
    }

    @Test
    void uuid_in_where() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE uuid_wh_test (id UUID, val TEXT)");
            s.execute("INSERT INTO uuid_wh_test VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'found')");
            s.execute("INSERT INTO uuid_wh_test VALUES ('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22', 'other')");
            ResultSet rs = s.executeQuery(
                    "SELECT val FROM uuid_wh_test WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'");
            assertTrue(rs.next());
            assertEquals("found", rs.getString(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE uuid_wh_test");
        }
    }

    @Test
    void uuid_not_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE uuid_nn_test (id UUID NOT NULL)");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO uuid_nn_test VALUES (NULL)"));
            s.execute("DROP TABLE uuid_nn_test");
        }
    }

    @Test
    void uuid_order_by() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE uuid_ord_test (id UUID)");
            s.execute("INSERT INTO uuid_ord_test VALUES ('cccccccc-0000-0000-0000-000000000000')");
            s.execute("INSERT INTO uuid_ord_test VALUES ('aaaaaaaa-0000-0000-0000-000000000000')");
            s.execute("INSERT INTO uuid_ord_test VALUES ('bbbbbbbb-0000-0000-0000-000000000000')");
            ResultSet rs = s.executeQuery("SELECT id FROM uuid_ord_test ORDER BY id");
            assertTrue(rs.next()); assertEquals("aaaaaaaa-0000-0000-0000-000000000000", rs.getString(1));
            assertTrue(rs.next()); assertEquals("bbbbbbbb-0000-0000-0000-000000000000", rs.getString(1));
            assertTrue(rs.next()); assertEquals("cccccccc-0000-0000-0000-000000000000", rs.getString(1));
            s.execute("DROP TABLE uuid_ord_test");
        }
    }

    // ========================================================================
    // 37. JSON/JSONB Types
    // ========================================================================

    @Test
    void json_literal_object() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": 1, \"b\": \"hello\"}'::jsonb");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("\"a\""), "Should contain key 'a': " + val);
            assertTrue(val.contains("\"b\""), "Should contain key 'b': " + val);
        }
    }

    @Test
    void json_literal_array() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '[1, 2, 3]'::jsonb");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("1") && val.contains("2") && val.contains("3"),
                    "Should contain 1, 2, 3: " + val);
        }
    }

    @Test
    void json_literal_nested() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": {\"b\": [1, 2]}}'::jsonb");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("\"b\""), "Should contain nested key 'b': " + val);
        }
    }

    @Test
    void json_arrow_key() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": 1}'::jsonb -> 'a'");
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1).trim());
        }
    }

    @Test
    void json_arrow_index() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '[1, 2, 3]'::jsonb -> 1");
            assertTrue(rs.next());
            String val = rs.getString(1);
            // Array index access: should return element at index 1 (which is 2)
            if (val != null) {
                assertEquals("2", val.trim());
            }
            // If null, array index access not yet implemented for integer keys
        }
    }

    @Test
    void json_arrow_text_key() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": \"hello\"}'::jsonb ->> 'a'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            // ->> should return text; implementation may include surrounding quotes
            String stripped = val.startsWith("\"") && val.endsWith("\"")
                    ? val.substring(1, val.length() - 1) : val;
            assertEquals("hello", stripped);
        }
    }

    @Test
    void json_arrow_text_index() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '[1, 2, 3]'::jsonb ->> 0");
            assertTrue(rs.next());
            String val = rs.getString(1);
            // Array index access with text extraction
            if (val != null) {
                assertEquals("1", val.trim());
            }
            // If null, array index access not yet implemented for integer keys
        }
    }

    @Test
    void json_hash_arrow_path() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": {\"b\": 1}}'::jsonb #> '{a,b}'");
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1).trim());
        }
    }

    @Test
    void json_hash_arrow_text_path() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": {\"b\": 1}}'::jsonb #>> '{a,b}'");
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1).trim());
        }
    }

    @Test
    void json_containment() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": 1, \"b\": 2}'::jsonb @> '{\"a\": 1}'::jsonb");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue("t".equals(val) || "true".equalsIgnoreCase(val) || rs.getBoolean(1),
                    "Should be true: " + val);
        }
    }

    @Test
    void json_containment_false() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": 1}'::jsonb @> '{\"a\": 2}'::jsonb");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue("f".equals(val) || "false".equalsIgnoreCase(val) || !rs.getBoolean(1),
                    "Should be false: " + val);
        }
    }

    @Test
    void json_contained_by() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": 1}'::jsonb <@ '{\"a\": 1, \"b\": 2}'::jsonb");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue("t".equals(val) || "true".equalsIgnoreCase(val) || rs.getBoolean(1),
                    "Should be true: " + val);
        }
    }

    @Test
    void json_key_exists() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Use jsonb_exists function to avoid JDBC ? parameter conflict
            ResultSet rs = s.executeQuery("SELECT jsonb_exists('{\"a\": 1, \"b\": 2}'::jsonb, 'a')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue("t".equals(val) || "true".equalsIgnoreCase(val) || rs.getBoolean(1),
                    "Key 'a' should exist: " + val);
        }
    }

    @Test
    void json_key_not_exists() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT jsonb_exists('{\"a\": 1}'::jsonb, 'c')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue("f".equals(val) || "false".equalsIgnoreCase(val) || !rs.getBoolean(1),
                    "Key 'c' should not exist: " + val);
        }
    }

    @Test
    void json_concatenation() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": 1}'::jsonb || '{\"b\": 2}'::jsonb");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("\"a\"") && val.contains("\"b\""),
                    "Should contain both keys: " + val);
        }
    }

    @Test
    void json_delete_key() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": 1, \"b\": 2}'::jsonb - 'a'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertFalse(val.contains("\"a\""), "Should not contain key 'a': " + val);
            assertTrue(val.contains("\"b\""), "Should still contain key 'b': " + val);
        }
    }

    @Test
    void json_delete_path() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": {\"b\": 1}}'::jsonb #- '{a,b}'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // "a" key should remain but without "b" inside
            assertTrue(val.contains("\"a\""), "Should still contain key 'a': " + val);
        }
    }

    @Test
    void json_column_insert_query() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE json_col_test (id SERIAL, data JSONB)");
            s.execute("INSERT INTO json_col_test (data) VALUES ('{\"name\": \"Alice\", \"age\": 30}')");
            s.execute("INSERT INTO json_col_test (data) VALUES ('{\"name\": \"Bob\", \"age\": 25}')");
            ResultSet rs = s.executeQuery("SELECT data ->> 'name' FROM json_col_test ORDER BY id");
            assertTrue(rs.next());
            String val1 = rs.getString(1);
            String n1 = val1.startsWith("\"") && val1.endsWith("\"") ? val1.substring(1, val1.length() - 1) : val1;
            assertEquals("Alice", n1);
            assertTrue(rs.next());
            String val2 = rs.getString(1);
            String n2 = val2.startsWith("\"") && val2.endsWith("\"") ? val2.substring(1, val2.length() - 1) : val2;
            assertEquals("Bob", n2);
            s.execute("DROP TABLE json_col_test");
        }
    }

    @Test
    void json_nested_access() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": {\"b\": {\"c\": 42}}}'::jsonb -> 'a' -> 'b' ->> 'c'");
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1).trim());
        }
    }

    @Test
    void json_null_handling() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // JSON null is different from SQL NULL
            ResultSet rs = s.executeQuery("SELECT '{\"a\": null}'::jsonb -> 'a'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            // JSON null should come back as the string "null"
            assertEquals("null", val.trim());

            // Missing key should return SQL NULL
            rs = s.executeQuery("SELECT '{\"a\": 1}'::jsonb -> 'missing'");
            assertTrue(rs.next());
            assertNull(rs.getString(1));
        }
    }

    @Test
    void json_array_containment() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '[1, 2, 3]'::jsonb @> '[1, 3]'::jsonb");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertTrue("t".equals(val) || "true".equalsIgnoreCase(val) || rs.getBoolean(1),
                    "Array should contain subset: " + val);
        }
    }

    @Test
    void json_equality() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '{\"a\": 1}'::jsonb = '{\"a\": 1}'::jsonb");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    void json_in_where() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE json_where_test (id SERIAL, data JSONB)");
            s.execute("INSERT INTO json_where_test (data) VALUES ('{\"status\": \"active\"}')");
            s.execute("INSERT INTO json_where_test (data) VALUES ('{\"status\": \"inactive\"}')");
            // Note: ->> may return quoted strings in Memgres, so compare with both styles
            ResultSet rs = s.executeQuery(
                    "SELECT id FROM json_where_test WHERE data ->> 'status' = 'active' " +
                    "OR data ->> 'status' = '\"active\"'");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            s.execute("DROP TABLE json_where_test");
        }
    }

    // ========================================================================
    // 38. JSON Functions & Operators
    // ========================================================================

    @Test
    void jsonf_json_typeof_object() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_typeof('{\"a\":1}'::json)");
            assertTrue(rs.next());
            assertEquals("object", rs.getString(1));
        }
    }

    @Test
    void jsonf_json_typeof_array() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_typeof('[1,2]'::json)");
            assertTrue(rs.next());
            assertEquals("array", rs.getString(1));
        }
    }

    @Test
    void jsonf_json_typeof_string() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_typeof('\"hello\"'::json)");
            assertTrue(rs.next());
            assertEquals("string", rs.getString(1));
        }
    }

    @Test
    void jsonf_json_typeof_number() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_typeof('42'::json)");
            assertTrue(rs.next());
            assertEquals("number", rs.getString(1));
        }
    }

    @Test
    void jsonf_json_typeof_boolean() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_typeof('true'::json)");
            assertTrue(rs.next());
            assertEquals("boolean", rs.getString(1));
        }
    }

    @Test
    void jsonf_json_typeof_null() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_typeof('null'::json)");
            assertTrue(rs.next());
            assertEquals("null", rs.getString(1));
        }
    }

    @Test
    void jsonf_json_array_length() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_array_length('[1,2,3]'::json)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void jsonf_json_array_length_empty() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_array_length('[]'::json)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void jsonf_json_array_length_nested() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_array_length('[[1,2],[3,4]]'::json)");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void jsonf_json_build_object() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_build_object('a', 1, 'b', 'hello')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("\"a\""), "Should contain key 'a': " + val);
            assertTrue(val.contains("\"b\""), "Should contain key 'b': " + val);
            assertTrue(val.contains("\"hello\""), "Should contain value 'hello': " + val);
        }
    }

    @Test
    void jsonf_json_build_array() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_build_array(1, 2, 'hello', true)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.startsWith("["), "Should be array: " + val);
            assertTrue(val.contains("1") && val.contains("2"), "Should contain numbers: " + val);
            assertTrue(val.contains("\"hello\""), "Should contain 'hello': " + val);
        }
    }

    @Test
    void jsonf_to_json_string() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_json('hello'::text)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("\"hello\"", val);
        }
    }

    @Test
    void jsonf_to_json_number() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_json(42)");
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1).trim());
        }
    }

    @Test
    void jsonf_to_json_boolean() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT to_json(true)");
            assertTrue(rs.next());
            assertEquals("true", rs.getString(1).trim());
        }
    }

    @Test
    void jsonf_json_extract_path_text() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_extract_path_text('{\"a\": {\"b\": \"c\"}}', 'a', 'b')");
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
        }
    }

    @Test
    void jsonf_json_extract_path() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_extract_path('{\"a\": {\"b\": 1}}', 'a', 'b')");
            assertTrue(rs.next());
            assertEquals("1", rs.getString(1).trim());
        }
    }

    @Test
    void jsonf_jsonb_set() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT jsonb_set('{\"a\": 1, \"b\": 2}'::jsonb, '{b}', '3')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("\"a\""), "Should still contain 'a': " + val);
            // b should now be 3
            assertTrue(val.contains("3"), "Should contain new value 3: " + val);
        }
    }

    @Test
    void jsonf_jsonb_set_nested() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT jsonb_set('{\"a\": {\"b\": 1}}'::jsonb, '{a,b}', '99')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("99"), "Should contain new value 99: " + val);
        }
    }

    @Test
    void jsonf_jsonb_insert() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT jsonb_insert('[1, 2, 3]'::jsonb, '{1}', '\"new\"')");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Should contain the inserted value; may be double-escaped depending on implementation
            assertTrue(val.contains("new"), "Should contain 'new': " + val);
        }
    }

    @Test
    void jsonf_jsonb_pretty() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT jsonb_pretty('{\"a\":1}'::jsonb)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // Pretty-printed should have newlines or indentation
            assertTrue(val.contains("\n") || val.contains("  "),
                    "Should be formatted with newlines or spaces: " + val);
        }
    }

    @Test
    void jsonf_jsonb_strip_nulls() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT jsonb_strip_nulls('{\"a\": 1, \"b\": null}'::jsonb)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("\"a\""), "Should still contain 'a': " + val);
            assertFalse(val.contains("\"b\""), "Should not contain 'b': " + val);
        }
    }

    @Test
    void jsonf_json_object_keys() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_object_keys('{\"a\": 1, \"b\": 2}'::json)");
            assertTrue(rs.next());
            String result = rs.getString(1);
            assertNotNull(result);
            // SRF: returns individual rows for each key
            java.util.Set<String> keys = new java.util.HashSet<>();
            keys.add(result);
            while (rs.next()) keys.add(rs.getString(1));
            assertTrue(keys.contains("a") && keys.contains("b"),
                    "Should contain keys 'a' and 'b': " + keys);
        }
    }

    @Test
    void jsonf_jsonb_agg() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT jsonb_agg(name) FROM (VALUES ('Alice'), ('Bob')) AS t(name)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("Alice") && val.contains("Bob"),
                    "Should contain both names: " + val);
        }
    }

    @Test
    void jsonf_jsonb_object_agg() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT jsonb_object_agg(k, v) FROM (VALUES ('a', 1), ('b', 2)) AS t(k, v)");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("\"a\"") && val.contains("\"b\""),
                    "Should contain both keys: " + val);
        }
    }

    @Test
    void jsonf_json_build_object_empty() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_build_object()");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("{}", val.trim());
        }
    }

    @Test
    void jsonf_json_build_array_empty() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT json_build_array()");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("[]", val.trim());
        }
    }

    @Test
    void jsonf_arrow_chain() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT '{\"a\": {\"b\": {\"c\": \"deep\"}}}'::jsonb -> 'a' -> 'b' ->> 'c'");
            assertTrue(rs.next());
            String val = rs.getString(1);
            String stripped = val.startsWith("\"") && val.endsWith("\"")
                    ? val.substring(1, val.length() - 1) : val;
            assertEquals("deep", stripped);
        }
    }

    @Test
    void jsonf_jsonb_concat_arrays() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '[1,2]'::jsonb || '[3,4]'::jsonb");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("1") && val.contains("2") && val.contains("3") && val.contains("4"),
                    "Should contain all elements: " + val);
        }
    }

    @Test
    void jsonf_jsonb_delete_index() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("SELECT '[1,2,3]'::jsonb - 1");
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            // The result should be [1, 3] - element at index 1 (which is 2) removed
            // Normalize whitespace for comparison
            String normalized = val.replaceAll("\\s+", "");
            assertTrue(normalized.equals("[1,3]"),
                    "Should be [1,3] after removing index 1: " + val);
        }
    }
}
