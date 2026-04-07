package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for expression (functional) indexes and partial indexes.
 * Covers:
 * - CREATE UNIQUE INDEX on lower(col), upper(col), coalesce(), abs(), etc.
 * - Partial indexes with WHERE predicates
 * - Combined expression + partial indexes
 * - INSERT, UPDATE, DELETE, ON CONFLICT with expression/partial indexes
 * - NULL handling in expression indexes
 * - Multi-column expression indexes
 */
class ExpressionAndPartialIndexTest {

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
    // Expression index: lower(email) for case-insensitive uniqueness
    // ==============================================================

    @Test
    void lowerEmailUniqueIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_email (id SERIAL PRIMARY KEY, email TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_email_lower ON ei_email(lower(email))");
            s.execute("INSERT INTO ei_email (email) VALUES ('Alice@Example.com')");

            // Same email in different case should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_email (email) VALUES ('alice@example.com')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_email (email) VALUES ('ALICE@EXAMPLE.COM')"));

            // Different email should succeed
            s.execute("INSERT INTO ei_email (email) VALUES ('Bob@Example.com')");

            // Verify both exist
            ResultSet rs = s.executeQuery("SELECT count(*) FROM ei_email");
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void lowerEmailSelectRoundTrip() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_email2 (id SERIAL PRIMARY KEY, email TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_email2_lower ON ei_email2(lower(email))");
            s.execute("INSERT INTO ei_email2 (email) VALUES ('User@Host.COM')");

            // SELECT with lower() should find the row
            ResultSet rs = s.executeQuery("SELECT email FROM ei_email2 WHERE lower(email) = 'user@host.com'");
            assertTrue(rs.next());
            assertEquals("User@Host.COM", rs.getString(1)); // original case preserved
            assertFalse(rs.next());
        }
    }

    @Test
    void lowerEmailUpdateReindex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_email3 (id SERIAL PRIMARY KEY, email TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_email3_lower ON ei_email3(lower(email))");
            s.execute("INSERT INTO ei_email3 (email) VALUES ('alice@test.com')");
            s.execute("INSERT INTO ei_email3 (email) VALUES ('bob@test.com')");

            // Update alice to a new email, which should free the old lower() value
            s.execute("UPDATE ei_email3 SET email = 'charlie@test.com' WHERE email = 'alice@test.com'");

            // Now 'alice@test.com' should be available
            s.execute("INSERT INTO ei_email3 (email) VALUES ('ALICE@TEST.COM')");

            // 'charlie@test.com' should be taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_email3 (email) VALUES ('Charlie@Test.Com')"));
        }
    }

    @Test
    void lowerEmailNullHandling() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_email4 (id SERIAL PRIMARY KEY, email TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_email4_lower ON ei_email4(lower(email))");

            // Multiple NULLs should be allowed (lower(NULL) is NULL, NULLs are distinct)
            s.execute("INSERT INTO ei_email4 (email) VALUES (NULL)");
            s.execute("INSERT INTO ei_email4 (email) VALUES (NULL)");

            ResultSet rs = s.executeQuery("SELECT count(*) FROM ei_email4 WHERE email IS NULL");
            rs.next();
            assertEquals(2, rs.getInt(1));

            // Update NULL to a value
            s.execute("UPDATE ei_email4 SET email = 'test@test.com' WHERE id = 1");
            rs = s.executeQuery("SELECT count(*) FROM ei_email4 WHERE email IS NULL");
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    // ==============================================================
    // Expression index: upper(name)
    // ==============================================================

    @Test
    void upperNameUniqueIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_upper (id SERIAL PRIMARY KEY, name TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_upper_name ON ei_upper(upper(name))");

            s.execute("INSERT INTO ei_upper (name) VALUES ('hello')");
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_upper (name) VALUES ('HELLO')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_upper (name) VALUES ('Hello')"));

            s.execute("INSERT INTO ei_upper (name) VALUES ('world')");
            assertEquals(2, count(s, "SELECT count(*) FROM ei_upper"));
        }
    }

    // ==============================================================
    // Expression index: coalesce(note, '') to treat NULL as empty
    // ==============================================================

    @Test
    void coalesceUniqueIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_coal (id SERIAL PRIMARY KEY, note TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_coal ON ei_coal(coalesce(note, ''))");

            s.execute("INSERT INTO ei_coal (note) VALUES (NULL)");
            // Another NULL should conflict because coalesce(NULL,'') = '' for both
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_coal (note) VALUES (NULL)"));
            // Explicit empty string should also conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_coal (note) VALUES ('')"));

            s.execute("INSERT INTO ei_coal (note) VALUES ('hello')");
            assertEquals(2, count(s, "SELECT count(*) FROM ei_coal"));
        }
    }

    @Test
    void coalesceUpdateRoundTrip() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_coal2 (id SERIAL PRIMARY KEY, note TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_coal2 ON ei_coal2(coalesce(note, ''))");

            s.execute("INSERT INTO ei_coal2 (note) VALUES (NULL)");
            s.execute("INSERT INTO ei_coal2 (note) VALUES ('a')");

            // Update NULL to 'b', freeing the coalesce '' slot
            s.execute("UPDATE ei_coal2 SET note = 'b' WHERE note IS NULL");

            // Now NULL (coalesced to '') should be available
            s.execute("INSERT INTO ei_coal2 (note) VALUES (NULL)");

            // Empty string should conflict with the new NULL
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_coal2 (note) VALUES ('')"));
        }
    }

    // ==============================================================
    // Expression index: abs(value), numeric expression
    // ==============================================================

    @Test
    void absUniqueIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_abs (id SERIAL PRIMARY KEY, val INTEGER)");
            s.execute("CREATE UNIQUE INDEX idx_abs ON ei_abs(abs(val))");

            s.execute("INSERT INTO ei_abs (val) VALUES (5)");
            // -5 has same abs() as 5, so it should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_abs (val) VALUES (-5)"));

            s.execute("INSERT INTO ei_abs (val) VALUES (3)");
            s.execute("INSERT INTO ei_abs (val) VALUES (-4)");
            // 4 conflicts with -4
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_abs (val) VALUES (4)"));

            assertEquals(3, count(s, "SELECT count(*) FROM ei_abs"));
        }
    }

    // ==============================================================
    // Expression index: length(text)
    // ==============================================================

    @Test
    void lengthUniqueIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_len (id SERIAL PRIMARY KEY, code TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_len ON ei_len(length(code))");

            s.execute("INSERT INTO ei_len (code) VALUES ('ab')");    // length 2
            s.execute("INSERT INTO ei_len (code) VALUES ('abc')");   // length 3

            // Another length-2 string should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_len (code) VALUES ('xy')"));

            s.execute("INSERT INTO ei_len (code) VALUES ('a')");     // length 1
            assertEquals(3, count(s, "SELECT count(*) FROM ei_len"));
        }
    }

    // ==============================================================
    // Expression index: (a + b), arithmetic expression
    // ==============================================================

    @Test
    void arithmeticExpressionIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_arith (id SERIAL PRIMARY KEY, a INTEGER, b INTEGER)");
            s.execute("CREATE UNIQUE INDEX idx_sum ON ei_arith((a + b))");

            s.execute("INSERT INTO ei_arith (a, b) VALUES (1, 2)");   // sum = 3
            s.execute("INSERT INTO ei_arith (a, b) VALUES (5, 5)");   // sum = 10

            // (2, 1) has same sum as (1, 2), so it should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_arith (a, b) VALUES (2, 1)"));

            s.execute("INSERT INTO ei_arith (a, b) VALUES (1, 3)");   // sum = 4
            assertEquals(3, count(s, "SELECT count(*) FROM ei_arith"));
        }
    }

    // ==============================================================
    // Expression index: substr/substring
    // ==============================================================

    @Test
    void substrExpressionIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_sub (id SERIAL PRIMARY KEY, code TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_prefix ON ei_sub(substr(code, 1, 3))");

            s.execute("INSERT INTO ei_sub (code) VALUES ('ABC123')");
            // Same prefix 'ABC', should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_sub (code) VALUES ('ABC999')"));

            s.execute("INSERT INTO ei_sub (code) VALUES ('DEF123')");
            assertEquals(2, count(s, "SELECT count(*) FROM ei_sub"));
        }
    }

    // ==============================================================
    // Expression index: trim()
    // ==============================================================

    @Test
    void trimExpressionIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_trim (id SERIAL PRIMARY KEY, val TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_trim ON ei_trim(trim(val))");

            s.execute("INSERT INTO ei_trim (val) VALUES ('  hello  ')");
            // 'hello' without spaces has same trim(), should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_trim (val) VALUES ('hello')"));
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_trim (val) VALUES ('hello   ')"));

            s.execute("INSERT INTO ei_trim (val) VALUES ('world')");
            assertEquals(2, count(s, "SELECT count(*) FROM ei_trim"));
        }
    }

    // ==============================================================
    // Partial index: WHERE active = true
    // ==============================================================

    @Test
    void partialIndexWhereActive() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_active (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN)");
            s.execute("CREATE UNIQUE INDEX idx_active_email ON pi_active(email) WHERE active = true");

            s.execute("INSERT INTO pi_active (email, active) VALUES ('alice@test.com', true)");
            // Same email, active=true: should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO pi_active (email, active) VALUES ('alice@test.com', true)"));

            // Same email, active=false: should be fine (outside partial index scope)
            s.execute("INSERT INTO pi_active (email, active) VALUES ('alice@test.com', false)");

            // Another inactive row with same email, also fine
            s.execute("INSERT INTO pi_active (email, active) VALUES ('alice@test.com', false)");

            assertEquals(3, count(s, "SELECT count(*) FROM pi_active"));
            assertEquals(1, count(s, "SELECT count(*) FROM pi_active WHERE active = true"));
        }
    }

    @Test
    void partialIndexActivateDeactivateRoundTrip() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_act2 (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN)");
            s.execute("CREATE UNIQUE INDEX idx_act2_email ON pi_act2(email) WHERE active = true");

            s.execute("INSERT INTO pi_act2 (email, active) VALUES ('a@test.com', true)");
            s.execute("INSERT INTO pi_act2 (email, active) VALUES ('b@test.com', false)");

            // Deactivate 'a', removing it from partial index
            s.execute("UPDATE pi_act2 SET active = false WHERE email = 'a@test.com'");

            // Now 'a@test.com' active should be available
            s.execute("INSERT INTO pi_act2 (email, active) VALUES ('a@test.com', true)");

            // Activate 'b', entering partial index
            s.execute("UPDATE pi_act2 SET active = true WHERE email = 'b@test.com'");

            // Another 'b@test.com' active should now conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO pi_act2 (email, active) VALUES ('b@test.com', true)"));

            assertEquals(3, count(s, "SELECT count(*) FROM pi_act2"));
        }
    }

    // ==============================================================
    // Partial index: WHERE deleted_at IS NULL (soft delete pattern)
    // ==============================================================

    @Test
    void partialIndexSoftDelete() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_soft (id SERIAL PRIMARY KEY, code TEXT, deleted_at TIMESTAMP)");
            s.execute("CREATE UNIQUE INDEX idx_soft_code ON pi_soft(code) WHERE deleted_at IS NULL");

            s.execute("INSERT INTO pi_soft (code, deleted_at) VALUES ('ABC', NULL)");

            // Same code, not deleted: should conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO pi_soft (code, deleted_at) VALUES ('ABC', NULL)"));

            // Same code, deleted: outside partial index, allowed
            s.execute("INSERT INTO pi_soft (code, deleted_at) VALUES ('ABC', '2024-01-01 00:00:00')");

            // "Soft delete" the active row
            s.execute("UPDATE pi_soft SET deleted_at = '2024-06-01 00:00:00' WHERE code = 'ABC' AND deleted_at IS NULL");

            // Now 'ABC' non-deleted should be available again
            s.execute("INSERT INTO pi_soft (code, deleted_at) VALUES ('ABC', NULL)");

            assertEquals(3, count(s, "SELECT count(*) FROM pi_soft WHERE code = 'ABC'"));
            assertEquals(1, count(s, "SELECT count(*) FROM pi_soft WHERE code = 'ABC' AND deleted_at IS NULL"));
        }
    }

    // ==============================================================
    // Partial index: WHERE status = 'active' (string predicate)
    // ==============================================================

    @Test
    void partialIndexStatusString() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_status (id SERIAL PRIMARY KEY, email TEXT, status TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_status_email ON pi_status(email) WHERE status = 'active'");

            s.execute("INSERT INTO pi_status (email, status) VALUES ('a@t.com', 'active')");
            s.execute("INSERT INTO pi_status (email, status) VALUES ('a@t.com', 'inactive')");
            s.execute("INSERT INTO pi_status (email, status) VALUES ('a@t.com', 'suspended')");

            // Second active with same email: conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO pi_status (email, status) VALUES ('a@t.com', 'active')"));

            assertEquals(3, count(s, "SELECT count(*) FROM pi_status WHERE email = 'a@t.com'"));
        }
    }

    // ==============================================================
    // Partial index: WHERE col > value (range predicate)
    // ==============================================================

    @Test
    void partialIndexRangePredicate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_range (id SERIAL PRIMARY KEY, score INTEGER, name TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_high_score ON pi_range(name) WHERE score > 100");

            s.execute("INSERT INTO pi_range (score, name) VALUES (150, 'Alice')");
            s.execute("INSERT INTO pi_range (score, name) VALUES (50, 'Alice')");   // below threshold, ok

            // High scorer with same name: conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO pi_range (score, name) VALUES (200, 'Alice')"));

            // Low scorers can share names freely
            s.execute("INSERT INTO pi_range (score, name) VALUES (80, 'Alice')");
            assertEquals(3, count(s, "SELECT count(*) FROM pi_range WHERE name = 'Alice'"));
        }
    }

    // ==============================================================
    // Combined: expression + partial index
    // ==============================================================

    @Test
    void lowerEmailWhereActive() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE epi_combo (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN)");
            s.execute("CREATE UNIQUE INDEX idx_combo ON epi_combo(lower(email)) WHERE active = true");

            s.execute("INSERT INTO epi_combo (email, active) VALUES ('Alice@Test.com', true)");

            // Same lower() email, active: conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO epi_combo (email, active) VALUES ('ALICE@TEST.COM', true)"));

            // Same lower() email, inactive: ok
            s.execute("INSERT INTO epi_combo (email, active) VALUES ('alice@test.com', false)");

            // Different email, active: ok
            s.execute("INSERT INTO epi_combo (email, active) VALUES ('Bob@Test.com', true)");

            assertEquals(3, count(s, "SELECT count(*) FROM epi_combo"));
        }
    }

    @Test
    void coalesceWhereNotDeleted() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE epi_combo2 (id SERIAL PRIMARY KEY, code TEXT, deleted BOOLEAN DEFAULT false)");
            s.execute("CREATE UNIQUE INDEX idx_combo2 ON epi_combo2(coalesce(code, '')) WHERE deleted = false");

            s.execute("INSERT INTO epi_combo2 (code) VALUES (NULL)");  // coalesce -> ''

            // Another NULL, not deleted: conflict (both coalesce to '')
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO epi_combo2 (code) VALUES (NULL)"));

            // Same NULL but deleted: ok
            s.execute("INSERT INTO epi_combo2 (code, deleted) VALUES (NULL, true)");

            // "Delete" the first row, freeing the slot
            s.execute("UPDATE epi_combo2 SET deleted = true WHERE id = 1");

            // Now NULL non-deleted should be available
            s.execute("INSERT INTO epi_combo2 (code) VALUES (NULL)");
            assertEquals(3, count(s, "SELECT count(*) FROM epi_combo2"));
        }
    }

    // ==============================================================
    // ON CONFLICT with expression index
    // ==============================================================

    @Test
    void onConflictWithExpressionIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_oc (id SERIAL PRIMARY KEY, email TEXT, name TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_oc_email ON ei_oc(lower(email))");

            s.execute("INSERT INTO ei_oc (email, name) VALUES ('Alice@Test.com', 'Alice')");

            // ON CONFLICT on the expression index
            s.execute("INSERT INTO ei_oc (email, name) VALUES ('alice@test.com', 'Updated Alice') "
                    + "ON CONFLICT ((lower(email))) DO UPDATE SET name = EXCLUDED.name");

            ResultSet rs = s.executeQuery("SELECT name FROM ei_oc WHERE lower(email) = 'alice@test.com'");
            assertTrue(rs.next());
            assertEquals("Updated Alice", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    void onConflictDoNothingWithExpressionIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_oc2 (id SERIAL PRIMARY KEY, email TEXT, name TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_oc2_email ON ei_oc2(lower(email))");

            s.execute("INSERT INTO ei_oc2 (email, name) VALUES ('alice@test.com', 'Alice')");

            // DO NOTHING on conflict
            s.execute("INSERT INTO ei_oc2 (email, name) VALUES ('ALICE@TEST.COM', 'Should Not Appear') "
                    + "ON CONFLICT ((lower(email))) DO NOTHING");

            assertEquals(1, count(s, "SELECT count(*) FROM ei_oc2"));
            ResultSet rs = s.executeQuery("SELECT name FROM ei_oc2");
            rs.next();
            assertEquals("Alice", rs.getString(1));
        }
    }

    @Test
    void onConflictWithPartialExpressionIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_oc3 (id SERIAL PRIMARY KEY, email TEXT, status TEXT, name TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_oc3 ON ei_oc3(lower(email)) WHERE status = 'active'");

            s.execute("INSERT INTO ei_oc3 (email, status, name) VALUES ('alice@test.com', 'active', 'Alice')");

            // ON CONFLICT on partial expression index, must include WHERE
            s.execute("INSERT INTO ei_oc3 (email, status, name) VALUES ('ALICE@TEST.COM', 'active', 'Updated') "
                    + "ON CONFLICT ((lower(email))) WHERE status = 'active' DO UPDATE SET name = EXCLUDED.name");

            ResultSet rs = s.executeQuery("SELECT name FROM ei_oc3 WHERE status = 'active'");
            assertTrue(rs.next());
            assertEquals("Updated", rs.getString(1));
        }
    }

    // ==============================================================
    // DELETE with expression/partial indexes
    // ==============================================================

    @Test
    void deleteAndReinsertWithExpressionIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_del (id SERIAL PRIMARY KEY, email TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_del_email ON ei_del(lower(email))");

            s.execute("INSERT INTO ei_del (email) VALUES ('Alice@Test.com')");
            s.execute("DELETE FROM ei_del WHERE lower(email) = 'alice@test.com'");

            // Should be available again
            s.execute("INSERT INTO ei_del (email) VALUES ('ALICE@test.com')");
            assertEquals(1, count(s, "SELECT count(*) FROM ei_del"));
        }
    }

    @Test
    void deleteFromPartialIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_del (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN)");
            s.execute("CREATE UNIQUE INDEX idx_pi_del ON pi_del(email) WHERE active = true");

            s.execute("INSERT INTO pi_del (email, active) VALUES ('a@t.com', true)");
            s.execute("INSERT INTO pi_del (email, active) VALUES ('a@t.com', false)");

            // Delete the active one, freeing the partial index slot
            s.execute("DELETE FROM pi_del WHERE email = 'a@t.com' AND active = true");

            // Can re-insert as active
            s.execute("INSERT INTO pi_del (email, active) VALUES ('a@t.com', true)");
            assertEquals(2, count(s, "SELECT count(*) FROM pi_del WHERE email = 'a@t.com'"));
        }
    }

    // ==============================================================
    // Multiple expression indexes on same table
    // ==============================================================

    @Test
    void multipleExpressionIndexes() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_multi (id SERIAL PRIMARY KEY, name TEXT, code TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_multi_name ON ei_multi(lower(name))");
            s.execute("CREATE UNIQUE INDEX idx_multi_code ON ei_multi(upper(code))");

            s.execute("INSERT INTO ei_multi (name, code) VALUES ('Alice', 'abc')");

            // lower(name) conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_multi (name, code) VALUES ('alice', 'def')"));

            // upper(code) conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_multi (name, code) VALUES ('Bob', 'ABC')"));

            // Both different, ok
            s.execute("INSERT INTO ei_multi (name, code) VALUES ('Bob', 'def')");
            assertEquals(2, count(s, "SELECT count(*) FROM ei_multi"));
        }
    }

    // ==============================================================
    // Expression index with UPDATE changing expression result
    // ==============================================================

    @Test
    void updateChangingExpressionResult() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_upd (id SERIAL PRIMARY KEY, val INTEGER)");
            s.execute("CREATE UNIQUE INDEX idx_upd_abs ON ei_upd(abs(val))");

            s.execute("INSERT INTO ei_upd (val) VALUES (5)");    // abs = 5
            s.execute("INSERT INTO ei_upd (val) VALUES (-3)");   // abs = 3

            // Change 5 to -5, same abs() but should be ok (same row)
            s.execute("UPDATE ei_upd SET val = -5 WHERE val = 5");

            // Change -5 to 3; abs(3) conflicts with existing abs(-3)=3
            assertThrows(SQLException.class, () ->
                    s.execute("UPDATE ei_upd SET val = 3 WHERE val = -5"));

            // Change -3 to 7, freeing abs(3) since abs(7) is new
            s.execute("UPDATE ei_upd SET val = 7 WHERE val = -3");

            // Now abs(3) is free
            s.execute("INSERT INTO ei_upd (val) VALUES (3)");
            assertEquals(3, count(s, "SELECT count(*) FROM ei_upd"));
        }
    }

    // ==============================================================
    // Partial index: NULL predicate column
    // ==============================================================

    @Test
    void partialIndexNullPredicateColumn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_null (id SERIAL PRIMARY KEY, code TEXT, category TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_pi_null ON pi_null(code) WHERE category IS NOT NULL");

            // No category (NULL), outside partial index so no uniqueness check
            s.execute("INSERT INTO pi_null (code, category) VALUES ('A', NULL)");
            s.execute("INSERT INTO pi_null (code, category) VALUES ('A', NULL)");

            // With category, inside partial index
            s.execute("INSERT INTO pi_null (code, category) VALUES ('A', 'cat1')");

            // Same code, different category but still NOT NULL: conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO pi_null (code, category) VALUES ('A', 'cat2')"));

            assertEquals(3, count(s, "SELECT count(*) FROM pi_null WHERE code = 'A'"));
        }
    }

    // ==============================================================
    // Partial index with compound WHERE (AND)
    // ==============================================================

    @Test
    void partialIndexCompoundWhere() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_comp (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN, tier TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_pi_comp ON pi_comp(email) WHERE active = true AND tier = 'premium'");

            s.execute("INSERT INTO pi_comp (email, active, tier) VALUES ('a@t.com', true, 'premium')");

            // Same email, active + premium: conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO pi_comp (email, active, tier) VALUES ('a@t.com', true, 'premium')"));

            // Same email, active + free: ok (doesn't match full WHERE)
            s.execute("INSERT INTO pi_comp (email, active, tier) VALUES ('a@t.com', true, 'free')");

            // Same email, inactive + premium: ok
            s.execute("INSERT INTO pi_comp (email, active, tier) VALUES ('a@t.com', false, 'premium')");

            assertEquals(3, count(s, "SELECT count(*) FROM pi_comp WHERE email = 'a@t.com'"));
        }
    }

    // ==============================================================
    // Expression index: lower() with non-ASCII / unicode
    // ==============================================================

    @Test
    void lowerWithUnicode() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_uni (id SERIAL PRIMARY KEY, name TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_uni_lower ON ei_uni(lower(name))");

            s.execute("INSERT INTO ei_uni (name) VALUES ('café')");
            // Same in different case patterns
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_uni (name) VALUES ('café')"));

            s.execute("INSERT INTO ei_uni (name) VALUES ('naïve')");
            assertEquals(2, count(s, "SELECT count(*) FROM ei_uni"));
        }
    }

    // ==============================================================
    // Expression index: concat or string concatenation
    // ==============================================================

    @Test
    void concatExpressionIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_concat (id SERIAL PRIMARY KEY, first_name TEXT, last_name TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_full_name ON ei_concat(lower(first_name || ' ' || last_name))");

            s.execute("INSERT INTO ei_concat (first_name, last_name) VALUES ('John', 'Doe')");

            // Same full name, different case: conflict
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_concat (first_name, last_name) VALUES ('JOHN', 'DOE')"));

            // Different name: ok
            s.execute("INSERT INTO ei_concat (first_name, last_name) VALUES ('Jane', 'Doe')");
            assertEquals(2, count(s, "SELECT count(*) FROM ei_concat"));
        }
    }

    // ==============================================================
    // Transaction rollback with expression index
    // ==============================================================

    @Test
    void rollbackExpressionIndexInsert() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_rb (id SERIAL PRIMARY KEY, email TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_rb_email ON ei_rb(lower(email))");
            conn.commit();

            s.execute("INSERT INTO ei_rb (email) VALUES ('Alice@Test.com')");
            conn.rollback();

            // After rollback, the email should be available
            s.execute("INSERT INTO ei_rb (email) VALUES ('alice@test.com')");
            conn.commit();

            assertEquals(1, count(s, "SELECT count(*) FROM ei_rb"));
        } finally {
            conn.setAutoCommit(true);
        }
    }

    @Test
    void rollbackPartialIndexUpdate() throws SQLException {
        conn.setAutoCommit(false);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_rb (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN)");
            s.execute("CREATE UNIQUE INDEX idx_rb_active ON pi_rb(email) WHERE active = true");
            s.execute("INSERT INTO pi_rb (email, active) VALUES ('a@t.com', true)");
            conn.commit();

            // Deactivate then rollback
            s.execute("UPDATE pi_rb SET active = false WHERE email = 'a@t.com'");

            // During transaction, slot appears free, but this insert will also be rolled back
            s.execute("INSERT INTO pi_rb (email, active) VALUES ('a@t.com', true)");

            conn.rollback();

            // After rollback, original active row should still be there
            assertEquals(1, count(s, "SELECT count(*) FROM pi_rb WHERE email = 'a@t.com' AND active = true"));
        } finally {
            conn.setAutoCommit(true);
        }
    }

    // ==============================================================
    // Expression index with prepared statements
    // ==============================================================

    @Test
    void preparedStatementWithExpressionIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_ps (id SERIAL PRIMARY KEY, email TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_ps_email ON ei_ps(lower(email))");
        }

        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO ei_ps (email) VALUES (?)")) {
            ps.setString(1, "Alice@Test.com");
            ps.executeUpdate();

            // Same lower(), should conflict
            ps.setString(1, "alice@test.com");
            assertThrows(SQLException.class, ps::executeUpdate);

            // Different email
            ps.setString(1, "Bob@Test.com");
            ps.executeUpdate();
        }

        try (Statement s = conn.createStatement()) {
            assertEquals(2, count(s, "SELECT count(*) FROM ei_ps"));
        }
    }

    // ==============================================================
    // Partial index: only active rows, verify inactive can be mass-inserted
    // ==============================================================

    @Test
    void bulkInactiveInsertWithPartialIndex() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE pi_bulk (id SERIAL PRIMARY KEY, email TEXT, active BOOLEAN)");
            s.execute("CREATE UNIQUE INDEX idx_bulk ON pi_bulk(email) WHERE active = true");

            // Bulk insert many inactive duplicates; all should succeed
            for (int i = 0; i < 50; i++) {
                s.execute("INSERT INTO pi_bulk (email, active) VALUES ('same@test.com', false)");
            }

            // One active entry
            s.execute("INSERT INTO pi_bulk (email, active) VALUES ('same@test.com', true)");

            assertEquals(51, count(s, "SELECT count(*) FROM pi_bulk WHERE email = 'same@test.com'"));
            assertEquals(1, count(s, "SELECT count(*) FROM pi_bulk WHERE email = 'same@test.com' AND active = true"));
        }
    }

    // ==============================================================
    // Expression index: interaction with regular unique constraint
    // ==============================================================

    @Test
    void expressionIndexPlusRegularUnique() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE ei_plus (id SERIAL PRIMARY KEY, email TEXT UNIQUE, display_name TEXT)");
            s.execute("CREATE UNIQUE INDEX idx_plus_lower ON ei_plus(lower(email))");

            s.execute("INSERT INTO ei_plus (email, display_name) VALUES ('Alice@Test.com', 'Alice')");

            // Exact duplicate, fails on regular UNIQUE
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_plus (email, display_name) VALUES ('Alice@Test.com', 'Other')"));

            // Case variant, fails on expression UNIQUE
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO ei_plus (email, display_name) VALUES ('alice@test.com', 'Other')"));

            // Completely different, ok
            s.execute("INSERT INTO ei_plus (email, display_name) VALUES ('Bob@Other.com', 'Bob')");
            assertEquals(2, count(s, "SELECT count(*) FROM ei_plus"));
        }
    }

    // ==============================================================
    // Helpers
    // ==============================================================

    private int count(Statement s, String sql) throws SQLException {
        ResultSet rs = s.executeQuery(sql);
        rs.next();
        return rs.getInt(1);
    }
}
