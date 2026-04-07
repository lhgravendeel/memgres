package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Index integrity tests: verify that hash indexes remain consistent
 * through all mutation paths (INSERT, UPDATE, DELETE, ON CONFLICT, FK CASCADE).
 */
class IndexIntegrityTest {

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

    // --- UPDATE changing PK ---

    @Test
    void updatePrimaryKeyAllowsReuse() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_upd_pk (id INTEGER PRIMARY KEY, val TEXT)");
            stmt.execute("INSERT INTO idx_upd_pk VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            // Move id=1 to id=10
            stmt.execute("UPDATE idx_upd_pk SET id = 10 WHERE id = 1");
            // Now id=1 should be free, so insert it
            stmt.execute("INSERT INTO idx_upd_pk VALUES (1, 'reused')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM idx_upd_pk WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("reused", rs.getString(1));
            // id=10 should still exist
            rs = stmt.executeQuery("SELECT val FROM idx_upd_pk WHERE id = 10");
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
        }
    }

    @Test
    void updatePrimaryKeyDetectsDuplicate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_upd_pk_dup (id INTEGER PRIMARY KEY, val TEXT)");
            stmt.execute("INSERT INTO idx_upd_pk_dup VALUES (1, 'a'), (2, 'b')");
            // Trying to set id=1 to id=2 should fail
            assertThrows(SQLException.class, () ->
                    stmt.execute("UPDATE idx_upd_pk_dup SET id = 2 WHERE id = 1"));
        }
    }

    // --- DELETE then re-insert ---

    @Test
    void deleteAndReinsertPK() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_del_pk (id INTEGER PRIMARY KEY, val TEXT)");
            stmt.execute("INSERT INTO idx_del_pk VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            stmt.execute("DELETE FROM idx_del_pk WHERE id = 2");
            // id=2 should be available
            stmt.execute("INSERT INTO idx_del_pk VALUES (2, 'new')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM idx_del_pk WHERE id = 2");
            assertTrue(rs.next());
            assertEquals("new", rs.getString(1));
            // Duplicate should still fail
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_del_pk VALUES (1, 'dup')"));
        }
    }

    @Test
    void deleteAllAndReinsert() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_del_all (id INTEGER PRIMARY KEY)");
            stmt.execute("INSERT INTO idx_del_all VALUES (1), (2), (3)");
            stmt.execute("DELETE FROM idx_del_all");
            // All PKs should be available again
            stmt.execute("INSERT INTO idx_del_all VALUES (1), (2), (3)");
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM idx_del_all");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    // --- ON CONFLICT DO UPDATE ---

    @Test
    void onConflictDoUpdateIndexConsistency() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_oc (id INTEGER PRIMARY KEY, val TEXT, ver INTEGER DEFAULT 1)");
            stmt.execute("INSERT INTO idx_oc VALUES (1, 'orig', 1)");
            // Upsert: should update existing
            stmt.execute("INSERT INTO idx_oc VALUES (1, 'updated', 2) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, ver = EXCLUDED.ver");
            ResultSet rs = stmt.executeQuery("SELECT val, ver FROM idx_oc WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("updated", rs.getString("val"));
            assertEquals(2, rs.getInt("ver"));
            // Still only one row
            rs = stmt.executeQuery("SELECT count(*) FROM idx_oc");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            // Insert a new row, should work
            stmt.execute("INSERT INTO idx_oc VALUES (2, 'new', 1) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
            rs = stmt.executeQuery("SELECT count(*) FROM idx_oc");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void onConflictDoNothingIndexConsistency() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_oc_dn (id INTEGER PRIMARY KEY, val TEXT)");
            stmt.execute("INSERT INTO idx_oc_dn VALUES (1, 'orig')");
            // DO NOTHING should leave index intact
            stmt.execute("INSERT INTO idx_oc_dn VALUES (1, 'ignored') ON CONFLICT DO NOTHING");
            ResultSet rs = stmt.executeQuery("SELECT val FROM idx_oc_dn WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("orig", rs.getString(1));
            // New insert should still work
            stmt.execute("INSERT INTO idx_oc_dn VALUES (2, 'second')");
            // Duplicate without ON CONFLICT should still fail
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_oc_dn VALUES (2, 'dup')"));
        }
    }

    // --- FK CASCADE DELETE ---

    @Test
    void fkCascadeDeleteIndexIntegrity() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_fk_parent (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE idx_fk_child (id SERIAL PRIMARY KEY, parent_id INTEGER REFERENCES idx_fk_parent(id) ON DELETE CASCADE, val TEXT)");
            stmt.execute("INSERT INTO idx_fk_parent VALUES (1, 'p1'), (2, 'p2')");
            stmt.execute("INSERT INTO idx_fk_child (parent_id, val) VALUES (1, 'c1'), (1, 'c2'), (2, 'c3')");
            // Delete parent; children should cascade
            stmt.execute("DELETE FROM idx_fk_parent WHERE id = 1");
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM idx_fk_child");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            // Child table PK index should still work; insert new child
            stmt.execute("INSERT INTO idx_fk_child (parent_id, val) VALUES (2, 'c4')");
            rs = stmt.executeQuery("SELECT count(*) FROM idx_fk_child");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    // --- FK CASCADE UPDATE ---

    @Test
    void fkCascadeUpdateIndexIntegrity() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_fku_parent (id INTEGER PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE idx_fku_child (id SERIAL PRIMARY KEY, parent_id INTEGER REFERENCES idx_fku_parent(id) ON UPDATE CASCADE, val TEXT)");
            stmt.execute("INSERT INTO idx_fku_parent VALUES (1, 'p1'), (2, 'p2')");
            stmt.execute("INSERT INTO idx_fku_child (parent_id, val) VALUES (1, 'c1'), (1, 'c2'), (2, 'c3')");
            // Update parent PK; children FK should cascade
            stmt.execute("UPDATE idx_fku_parent SET id = 10 WHERE id = 1");
            ResultSet rs = stmt.executeQuery("SELECT parent_id FROM idx_fku_child WHERE val = 'c1'");
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            // Parent PK index should be consistent; old id=1 is gone
            rs = stmt.executeQuery("SELECT count(*) FROM idx_fku_parent WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
            // Can now insert id=1
            stmt.execute("INSERT INTO idx_fku_parent VALUES (1, 'new_p1')");
        }
    }

    // --- FK SET NULL ---

    @Test
    void fkSetNullIndexIntegrity() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_fksn_parent (id INTEGER PRIMARY KEY)");
            stmt.execute("CREATE TABLE idx_fksn_child (id SERIAL PRIMARY KEY, pid INTEGER REFERENCES idx_fksn_parent(id) ON DELETE SET NULL)");
            stmt.execute("INSERT INTO idx_fksn_parent VALUES (1), (2)");
            stmt.execute("INSERT INTO idx_fksn_child (pid) VALUES (1), (1), (2)");
            stmt.execute("DELETE FROM idx_fksn_parent WHERE id = 1");
            // Children FK should be null
            ResultSet rs = stmt.executeQuery("SELECT pid FROM idx_fksn_child ORDER BY id LIMIT 1");
            assertTrue(rs.next());
            assertNull(rs.getObject("pid"));
            // Child PK index should be fine; all rows still exist
            rs = stmt.executeQuery("SELECT count(*) FROM idx_fksn_child");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    // --- NULLs in UNIQUE columns ---

    @Test
    void nullsAreDistinctInUniqueColumns() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_null_uniq (id SERIAL PRIMARY KEY, email TEXT UNIQUE)");
            // Multiple NULLs should be allowed in UNIQUE columns (standard SQL behavior)
            stmt.execute("INSERT INTO idx_null_uniq (email) VALUES (NULL)");
            stmt.execute("INSERT INTO idx_null_uniq (email) VALUES (NULL)");
            stmt.execute("INSERT INTO idx_null_uniq (email) VALUES (NULL)");
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM idx_null_uniq");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            // Non-null duplicate should still fail
            stmt.execute("INSERT INTO idx_null_uniq (email) VALUES ('unique@test.com')");
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_null_uniq (email) VALUES ('unique@test.com')"));
        }
    }

    // --- Composite key edge cases ---

    @Test
    void compositeKeyPartialNullAllowed() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_comp_null (a INTEGER, b INTEGER, UNIQUE(a, b))");
            // (1, NULL) and (1, NULL) should both be allowed (NULLs are distinct)
            stmt.execute("INSERT INTO idx_comp_null VALUES (1, NULL)");
            stmt.execute("INSERT INTO idx_comp_null VALUES (1, NULL)");
            // (1, 1) should only be allowed once
            stmt.execute("INSERT INTO idx_comp_null VALUES (1, 1)");
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_comp_null VALUES (1, 1)"));
        }
    }

    // --- Numeric type coercion in index keys ---

    @Test
    void integerAndBigintCompareEqual() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_type_coerce (id BIGINT PRIMARY KEY, val TEXT)");
            stmt.execute("INSERT INTO idx_type_coerce VALUES (1, 'one')");
            // Inserting integer 1 should conflict with bigint 1
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_type_coerce VALUES (1, 'dup')"));
        }
    }

    // --- Stress: interleaved inserts/updates/deletes ---

    @Test
    void interleavedMutationsIndexConsistency() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_interleave (id INTEGER PRIMARY KEY, val TEXT)");
            // Insert 100 rows
            for (int i = 1; i <= 100; i++) {
                stmt.execute("INSERT INTO idx_interleave VALUES (" + i + ", 'v" + i + "')");
            }
            // Delete even ids
            stmt.execute("DELETE FROM idx_interleave WHERE id % 2 = 0");
            // Update odd ids to negative
            stmt.execute("UPDATE idx_interleave SET id = -id");
            // Re-insert the even ids
            for (int i = 2; i <= 100; i += 2) {
                stmt.execute("INSERT INTO idx_interleave VALUES (" + i + ", 'new" + i + "')");
            }
            // Verify counts
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM idx_interleave");
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
            // Verify no duplicate PKs; insert of existing should fail
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_interleave VALUES (2, 'dup')"));
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_interleave VALUES (-1, 'dup')"));
        }
    }

    // --- TRUNCATE resets index ---

    @Test
    void truncateResetsIndex() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_trunc (id INTEGER PRIMARY KEY)");
            stmt.execute("INSERT INTO idx_trunc VALUES (1), (2), (3)");
            stmt.execute("TRUNCATE idx_trunc");
            // All PKs should be reusable
            stmt.execute("INSERT INTO idx_trunc VALUES (1), (2), (3)");
            ResultSet rs = stmt.executeQuery("SELECT count(*) FROM idx_trunc");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        }
    }

    // --- Multiple unique constraints ---

    @Test
    void multipleUniqueConstraints() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_multi_uniq (id INTEGER PRIMARY KEY, email TEXT UNIQUE, code INTEGER UNIQUE)");
            stmt.execute("INSERT INTO idx_multi_uniq VALUES (1, 'a@test.com', 100)");
            stmt.execute("INSERT INTO idx_multi_uniq VALUES (2, 'b@test.com', 200)");
            // Each unique constraint should be checked independently
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_multi_uniq VALUES (3, 'a@test.com', 300)")); // email dup
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_multi_uniq VALUES (3, 'c@test.com', 100)")); // code dup
            assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO idx_multi_uniq VALUES (1, 'c@test.com', 300)")); // pk dup
            // Valid insert should work
            stmt.execute("INSERT INTO idx_multi_uniq VALUES (3, 'c@test.com', 300)");
        }
    }

    // --- ON CONFLICT with composite key ---

    @Test
    void onConflictCompositeKey() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_oc_comp (a INTEGER, b INTEGER, val TEXT, PRIMARY KEY(a, b))");
            stmt.execute("INSERT INTO idx_oc_comp VALUES (1, 1, 'orig')");
            stmt.execute("INSERT INTO idx_oc_comp VALUES (1, 1, 'updated') ON CONFLICT (a, b) DO UPDATE SET val = EXCLUDED.val");
            ResultSet rs = stmt.executeQuery("SELECT val FROM idx_oc_comp WHERE a = 1 AND b = 1");
            assertTrue(rs.next());
            assertEquals("updated", rs.getString(1));
            rs = stmt.executeQuery("SELECT count(*) FROM idx_oc_comp");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }
}
