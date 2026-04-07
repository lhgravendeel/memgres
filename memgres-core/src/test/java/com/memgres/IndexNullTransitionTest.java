package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for index correctness when indexed values transition to/from NULL.
 */
class IndexNullTransitionTest {

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

    // ---- Non-NULL to NULL ----

    @Test
    void updateUniqueColumnToNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n1 (id SERIAL PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n1 (code) VALUES ('AAA')");
            s.execute("INSERT INTO idx_n1 (code) VALUES ('BBB')");
            // Set AAA to NULL
            s.execute("UPDATE idx_n1 SET code = NULL WHERE code = 'AAA'");
            // AAA should now be available
            s.execute("INSERT INTO idx_n1 (code) VALUES ('AAA')");
            // BBB still taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_n1 (code) VALUES ('BBB')"));
        }
    }

    @Test
    void updatePkColumnToNull_shouldFail() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n2 (id INTEGER PRIMARY KEY, val TEXT)");
            s.execute("INSERT INTO idx_n2 VALUES (1, 'a')");
            // PKs can't be null
            assertThrows(SQLException.class, () ->
                    s.execute("UPDATE idx_n2 SET id = NULL WHERE id = 1"));
        }
    }

    // ---- NULL to non-NULL ----

    @Test
    void updateNullToValue() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n3 (id SERIAL PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n3 (code) VALUES (NULL)");
            s.execute("INSERT INTO idx_n3 (code) VALUES (NULL)");
            // Update first NULL to 'X'
            s.execute("UPDATE idx_n3 SET code = 'X' WHERE id = 1");
            // 'X' should now be taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_n3 (code) VALUES ('X')"));
            // Update second NULL to 'Y', which should work
            s.execute("UPDATE idx_n3 SET code = 'Y' WHERE id = 2");
            // Both X and Y taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_n3 (code) VALUES ('Y')"));
            // NULL still allowed
            s.execute("INSERT INTO idx_n3 (code) VALUES (NULL)");
        }
    }

    @Test
    void updateMultipleNullsToSameValue_shouldFail() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n4 (id SERIAL PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n4 (code) VALUES (NULL)");
            s.execute("INSERT INTO idx_n4 (code) VALUES (NULL)");
            s.execute("UPDATE idx_n4 SET code = 'A' WHERE id = 1");
            // Second NULL to same value should fail
            assertThrows(SQLException.class, () ->
                    s.execute("UPDATE idx_n4 SET code = 'A' WHERE id = 2"));
        }
    }

    // ---- Round-trip: value → NULL → different value ----

    @Test
    void roundTripThroughNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n5 (id SERIAL PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n5 (code) VALUES ('A')");
            s.execute("INSERT INTO idx_n5 (code) VALUES ('B')");
            // A → NULL
            s.execute("UPDATE idx_n5 SET code = NULL WHERE code = 'A'");
            // A is free now, insert new row with A
            s.execute("INSERT INTO idx_n5 (code) VALUES ('A')");
            // Now set the NULL row to C
            s.execute("UPDATE idx_n5 SET code = 'C' WHERE code IS NULL");
            // Verify state: A, B, C all present
            ResultSet rs = s.executeQuery("SELECT code FROM idx_n5 ORDER BY code");
            assertTrue(rs.next()); assertEquals("A", rs.getString(1));
            assertTrue(rs.next()); assertEquals("B", rs.getString(1));
            assertTrue(rs.next()); assertEquals("C", rs.getString(1));
            assertFalse(rs.next());
            // All three should be protected
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n5 (code) VALUES ('A')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n5 (code) VALUES ('B')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n5 (code) VALUES ('C')"));
        }
    }

    // ---- Swap via NULL ----

    @Test
    void swapValuesViaNullIntermediate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n6 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n6 VALUES (1, 'X')");
            s.execute("INSERT INTO idx_n6 VALUES (2, 'Y')");
            // Swap X and Y using NULL as intermediate
            s.execute("UPDATE idx_n6 SET code = NULL WHERE id = 1");  // (1, NULL), (2, Y)
            s.execute("UPDATE idx_n6 SET code = 'X' WHERE id = 2");   // (1, NULL), (2, X)
            s.execute("UPDATE idx_n6 SET code = 'Y' WHERE id = 1");   // (1, Y), (2, X)
            // Verify swapped
            ResultSet rs = s.executeQuery("SELECT code FROM idx_n6 WHERE id = 1");
            assertTrue(rs.next()); assertEquals("Y", rs.getString(1));
            rs = s.executeQuery("SELECT code FROM idx_n6 WHERE id = 2");
            assertTrue(rs.next()); assertEquals("X", rs.getString(1));
            // Both still protected
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n6 VALUES (3, 'X')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n6 VALUES (3, 'Y')"));
        }
    }

    // ---- Delete rows with NULL in indexed column ----

    @Test
    void deleteRowWithNullUniqueColumn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n7 (id SERIAL PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n7 (code) VALUES (NULL)");
            s.execute("INSERT INTO idx_n7 (code) VALUES ('A')");
            s.execute("DELETE FROM idx_n7 WHERE code IS NULL");
            // Only one row left
            ResultSet rs = s.executeQuery("SELECT count(*) FROM idx_n7");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
            // A still protected
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n7 (code) VALUES ('A')"));
        }
    }

    @Test
    void deleteAllNullRows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n8 (id SERIAL PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n8 (code) VALUES (NULL)");
            s.execute("INSERT INTO idx_n8 (code) VALUES (NULL)");
            s.execute("INSERT INTO idx_n8 (code) VALUES ('A')");
            s.execute("DELETE FROM idx_n8 WHERE code IS NULL");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM idx_n8");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
        }
    }

    // ---- Multiple NULLs then update one ----

    @Test
    void multipleNullsThenUpdateOneByOne() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n9 (id SERIAL PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n9 (code) VALUES (NULL)"); // id=1
            s.execute("INSERT INTO idx_n9 (code) VALUES (NULL)"); // id=2
            s.execute("INSERT INTO idx_n9 (code) VALUES (NULL)"); // id=3
            // Assign values one by one
            s.execute("UPDATE idx_n9 SET code = 'A' WHERE id = 1");
            s.execute("UPDATE idx_n9 SET code = 'B' WHERE id = 2");
            s.execute("UPDATE idx_n9 SET code = 'C' WHERE id = 3");
            // All three protected
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n9 (code) VALUES ('A')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n9 (code) VALUES ('B')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n9 (code) VALUES ('C')"));
            // NULL still allowed
            s.execute("INSERT INTO idx_n9 (code) VALUES (NULL)");
        }
    }

    // ---- ON CONFLICT with NULL transition ----

    @Test
    void onConflictAfterNullToValue() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n10 (id INTEGER PRIMARY KEY, code TEXT UNIQUE, val TEXT)");
            s.execute("INSERT INTO idx_n10 VALUES (1, NULL, 'orig')");
            s.execute("UPDATE idx_n10 SET code = 'X' WHERE id = 1");
            // ON CONFLICT should detect 'X'
            s.execute("INSERT INTO idx_n10 VALUES (2, 'X', 'new') ON CONFLICT (code) DO UPDATE SET val = 'upserted'");
            ResultSet rs = s.executeQuery("SELECT val FROM idx_n10 WHERE id = 1");
            assertTrue(rs.next()); assertEquals("upserted", rs.getString(1));
            // Only one row
            rs = s.executeQuery("SELECT count(*) FROM idx_n10");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void onConflictWithNullDoesNotConflict() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n11 (id SERIAL PRIMARY KEY, code TEXT UNIQUE, val TEXT)");
            s.execute("INSERT INTO idx_n11 (code, val) VALUES (NULL, 'a')");
            // Inserting another NULL should NOT conflict because NULLs are distinct
            s.execute("INSERT INTO idx_n11 (code, val) VALUES (NULL, 'b') ON CONFLICT (code) DO UPDATE SET val = 'should not happen'");
            ResultSet rs = s.executeQuery("SELECT count(*) FROM idx_n11");
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));
        }
    }

    // ---- FK SET NULL cascade: non-null → null in indexed child column ----

    @Test
    void fkSetNullOnIndexedChildColumn() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n12_p (id INTEGER PRIMARY KEY)");
            s.execute("CREATE TABLE idx_n12_c (id SERIAL PRIMARY KEY, pid INTEGER UNIQUE REFERENCES idx_n12_p(id) ON DELETE SET NULL)");
            s.execute("INSERT INTO idx_n12_p VALUES (1), (2)");
            s.execute("INSERT INTO idx_n12_c (pid) VALUES (1)"); // id=1, pid=1
            s.execute("INSERT INTO idx_n12_c (pid) VALUES (2)"); // id=2, pid=2
            // Delete parent 1 → child pid set to NULL
            s.execute("DELETE FROM idx_n12_p WHERE id = 1");
            // pid=1 is now free (set to NULL). Can we insert a new child with pid=1? No parent exists.
            // But the UNIQUE index on pid should no longer block pid=1 conceptually.
            // Let's re-insert parent 1 and a new child
            s.execute("INSERT INTO idx_n12_p VALUES (1)");
            s.execute("INSERT INTO idx_n12_c (pid) VALUES (1)");
            // pid=2 still unique
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_n12_c (pid) VALUES (2)"));
        }
    }

    // ---- Composite key with NULL transitions ----

    @Test
    void compositeKeyNullTransition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n13 (a INTEGER, b TEXT, val TEXT, UNIQUE(a, b))");
            s.execute("INSERT INTO idx_n13 VALUES (1, NULL, 'x')");
            s.execute("INSERT INTO idx_n13 VALUES (1, NULL, 'y')"); // allowed: NULL is distinct
            // Update one NULL to 'Z'
            s.execute("UPDATE idx_n13 SET b = 'Z' WHERE val = 'x'");
            // (1, Z) now taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_n13 VALUES (1, 'Z', 'dup')"));
            // (1, NULL) still allowed
            s.execute("INSERT INTO idx_n13 VALUES (1, NULL, 'z')");
            // Update remaining NULLs to different values
            s.execute("UPDATE idx_n13 SET b = 'A' WHERE val = 'y'");
            s.execute("UPDATE idx_n13 SET b = 'B' WHERE val = 'z'");
            // Verify all protected
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n13 VALUES (1, 'Z', 'dup')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n13 VALUES (1, 'A', 'dup')"));
            assertThrows(SQLException.class, () -> s.execute("INSERT INTO idx_n13 VALUES (1, 'B', 'dup')"));
        }
    }

    // ---- Bulk: many nulls then assign values ----

    @Test
    void bulkNullToValueTransition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n14 (id SERIAL PRIMARY KEY, code TEXT UNIQUE)");
            // Insert 50 NULL rows
            for (int i = 0; i < 50; i++) {
                s.execute("INSERT INTO idx_n14 (code) VALUES (NULL)");
            }
            // Assign unique codes to all of them
            for (int i = 1; i <= 50; i++) {
                s.execute("UPDATE idx_n14 SET code = 'C" + i + "' WHERE id = " + i);
            }
            // All 50 codes should be protected
            for (int i = 1; i <= 50; i++) {
                String code = "C" + i;
                assertThrows(SQLException.class, () ->
                        s.execute("INSERT INTO idx_n14 (code) VALUES ('" + code + "')"),
                        "Code " + code + " should be protected by unique constraint");
            }
        }
    }

    // ---- NULLS NOT DISTINCT with transitions ----

    @Test
    void nullsNotDistinctTransition() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n15 (id SERIAL PRIMARY KEY, code TEXT UNIQUE NULLS NOT DISTINCT)");
            s.execute("INSERT INTO idx_n15 (code) VALUES (NULL)");
            // Second NULL should fail with NULLS NOT DISTINCT
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_n15 (code) VALUES (NULL)"));
            // Set to non-NULL
            s.execute("UPDATE idx_n15 SET code = 'A' WHERE id = 1");
            // NULL should now be available
            s.execute("INSERT INTO idx_n15 (code) VALUES (NULL)");
            // A is taken
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_n15 (code) VALUES ('A')"));
        }
    }

    // ---- Rollback of NULL transitions ----

    @Test
    void rollbackNullToValue() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n16 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n16 VALUES (1, NULL)");
            conn.setAutoCommit(false);
            s.execute("UPDATE idx_n16 SET code = 'X' WHERE id = 1");
            // X should be taken within the transaction
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_n16 VALUES (2, 'X')"));
            conn.rollback();
            conn.setAutoCommit(true);
            // After rollback, X should be free again
            s.execute("INSERT INTO idx_n16 VALUES (2, 'X')");
            // Verify
            ResultSet rs = s.executeQuery("SELECT id, code FROM idx_n16 ORDER BY id");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1)); assertNull(rs.getString(2));
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1)); assertEquals("X", rs.getString(2));
        }
    }

    @Test
    void rollbackValueToNull() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE idx_n17 (id INTEGER PRIMARY KEY, code TEXT UNIQUE)");
            s.execute("INSERT INTO idx_n17 VALUES (1, 'X')");
            conn.setAutoCommit(false);
            s.execute("UPDATE idx_n17 SET code = NULL WHERE id = 1");
            // X should be free within the transaction
            s.execute("INSERT INTO idx_n17 VALUES (2, 'X')");
            conn.rollback();
            conn.setAutoCommit(true);
            // After rollback, X should be taken again (by id=1, restored)
            assertThrows(SQLException.class, () ->
                    s.execute("INSERT INTO idx_n17 VALUES (3, 'X')"));
        }
    }
}
