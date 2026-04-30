package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that leftover FOR ALL TABLES publications block UPDATE/DELETE
 * on tables without a primary key, and that dropping the publication
 * restores normal operation. This mirrors the scenario where
 * IndexComparisonTest.cleanPg() must drop publications to avoid
 * replica identity errors on a shared PG instance.
 */
class ReplicaIdentityPublicationTest {

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

    @Test
    void leftover_publication_blocks_update_on_no_pk_table() throws SQLException {
        // Setup: create a FOR ALL TABLES publication (simulates leftover from prior test)
        exec("CREATE TABLE ri_test (id int, val text)");
        exec("CREATE UNIQUE INDEX ri_test_id ON ri_test(id)");
        exec("INSERT INTO ri_test VALUES (1, 'a'), (2, 'b')");
        exec("CREATE PUBLICATION ri_pub FOR ALL TABLES");

        // UPDATE should fail — no PK, default replica identity needs PK
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("UPDATE ri_test SET val = 'c' WHERE id = 1"));
        assertEquals("55000", ex.getSQLState(),
                "UPDATE on published table without PK must fail with 55000");

        // DELETE should also fail
        ex = assertThrows(SQLException.class,
                () -> exec("DELETE FROM ri_test WHERE id = 2"));
        assertEquals("55000", ex.getSQLState(),
                "DELETE on published table without PK must fail with 55000");

        // Fix: drop the publication (what cleanPg should do)
        exec("DROP PUBLICATION ri_pub");

        // Now UPDATE and DELETE should work
        exec("UPDATE ri_test SET val = 'c' WHERE id = 1");
        exec("DELETE FROM ri_test WHERE id = 2");

        // Cleanup
        exec("DROP TABLE ri_test");
    }

    @Test
    void replica_identity_full_allows_update_with_publication() throws SQLException {
        exec("CREATE TABLE ri_full (id int, val text)");
        exec("INSERT INTO ri_full VALUES (1, 'a')");
        exec("CREATE PUBLICATION ri_full_pub FOR ALL TABLES");

        // Default identity + no PK → fail
        assertThrows(SQLException.class, () -> exec("UPDATE ri_full SET val = 'b' WHERE id = 1"));

        // Set REPLICA IDENTITY FULL → should succeed
        exec("ALTER TABLE ri_full REPLICA IDENTITY FULL");
        exec("UPDATE ri_full SET val = 'b' WHERE id = 1");

        // Cleanup
        exec("DROP PUBLICATION ri_full_pub");
        exec("DROP TABLE ri_full");
    }
}
