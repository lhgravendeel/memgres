package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests cross-schema (cross-keyspace) foreign key behavior:
 * - FK referencing a table in a different schema via CREATE TABLE
 * - FK referencing a table in a different schema via ALTER TABLE
 * - Data validation (INSERT, UPDATE, DELETE cascade) across schemas
 * - Ambiguity when the referenced table exists in both schemas
 */
class CrossSchemaForeignKeyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @BeforeEach
    void cleanSchemas() throws SQLException {
        exec("DROP SCHEMA IF EXISTS ks1 CASCADE");
        exec("DROP SCHEMA IF EXISTS ks2 CASCADE");
        exec("CREATE SCHEMA ks1");
        exec("CREATE SCHEMA ks2");
    }

    @AfterEach
    void dropSchemas() throws SQLException {
        exec("DROP SCHEMA IF EXISTS ks1 CASCADE");
        exec("DROP SCHEMA IF EXISTS ks2 CASCADE");
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    private String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    // =========================================================================
    // 1. CREATE TABLE with FK referencing another schema (schema-qualified)
    // =========================================================================

    @Test void create_table_fk_references_other_schema() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        // FK should exist
        String fkCount = querySingle(
                "SELECT count(*) FROM information_schema.table_constraints " +
                "WHERE table_schema = 'ks2' AND table_name = 'child' AND constraint_type = 'FOREIGN KEY'");
        assertEquals("1", fkCount);
    }

    // =========================================================================
    // 2. ALTER TABLE ADD FK referencing another schema
    // =========================================================================

    @Test void alter_table_add_fk_references_other_schema() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int)");
        exec("ALTER TABLE ks2.child ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES ks1.parent(id)");

        String fkCount = querySingle(
                "SELECT count(*) FROM information_schema.table_constraints " +
                "WHERE table_schema = 'ks2' AND table_name = 'child' AND constraint_type = 'FOREIGN KEY'");
        assertEquals("1", fkCount);
    }

    // =========================================================================
    // 3. INSERT valid data across schemas
    // =========================================================================

    @Test void insert_valid_cross_schema_fk() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1), (2), (3)");
        exec("INSERT INTO ks2.child VALUES (10, 1), (20, 2), (30, 3)");

        assertEquals(3, queryInt("SELECT count(*) FROM ks2.child"));
    }

    // =========================================================================
    // 4. INSERT invalid data — FK violation across schemas
    // =========================================================================

    @Test void insert_invalid_cross_schema_fk_rejected() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1)");

        // parent_id=99 does not exist in ks1.parent
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks2.child VALUES (10, 99)"));
        assertEquals("23503", ex.getSQLState(), "Expected FK violation, got: " + ex.getMessage());
    }

    // =========================================================================
    // 5. INSERT NULL FK value — should be allowed (NULL is not checked)
    // =========================================================================

    @Test void insert_null_fk_cross_schema_allowed() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks2.child VALUES (10, NULL)");
        assertEquals(1, queryInt("SELECT count(*) FROM ks2.child"));
        assertNull(querySingle("SELECT parent_id FROM ks2.child WHERE id = 10"));
    }

    // =========================================================================
    // 6. DELETE parent — FK violation when child exists
    // =========================================================================

    @Test void delete_parent_blocked_by_cross_schema_child() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        SQLException ex = assertThrows(SQLException.class,
                () -> exec("DELETE FROM ks1.parent WHERE id = 1"));
        assertEquals("23503", ex.getSQLState(), "Expected FK violation on delete, got: " + ex.getMessage());
    }

    // =========================================================================
    // 7. DELETE parent with ON DELETE CASCADE across schemas
    // =========================================================================

    @Test void delete_cascade_across_schemas() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id) ON DELETE CASCADE)");

        exec("INSERT INTO ks1.parent VALUES (1), (2)");
        exec("INSERT INTO ks2.child VALUES (10, 1), (20, 1), (30, 2)");

        exec("DELETE FROM ks1.parent WHERE id = 1");

        // Children referencing parent 1 should be gone
        assertEquals(1, queryInt("SELECT count(*) FROM ks2.child"));
        assertEquals("30", querySingle("SELECT id FROM ks2.child"));
    }

    // =========================================================================
    // 8. DELETE parent with ON DELETE SET NULL across schemas
    // =========================================================================

    @Test void delete_set_null_across_schemas() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id) ON DELETE SET NULL)");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        exec("DELETE FROM ks1.parent WHERE id = 1");

        assertEquals(1, queryInt("SELECT count(*) FROM ks2.child"));
        assertNull(querySingle("SELECT parent_id FROM ks2.child WHERE id = 10"));
    }

    // =========================================================================
    // 9. UPDATE parent PK — FK violation when child references old value
    // =========================================================================

    @Test void update_parent_pk_blocked_by_cross_schema_child() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        SQLException ex = assertThrows(SQLException.class,
                () -> exec("UPDATE ks1.parent SET id = 99 WHERE id = 1"));
        assertEquals("23503", ex.getSQLState(), "Expected FK violation on update, got: " + ex.getMessage());
    }

    // =========================================================================
    // 10. UPDATE parent with ON UPDATE CASCADE across schemas
    // =========================================================================

    @Test void update_cascade_across_schemas() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id) ON UPDATE CASCADE)");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        exec("UPDATE ks1.parent SET id = 99 WHERE id = 1");

        assertEquals("99", querySingle("SELECT parent_id FROM ks2.child WHERE id = 10"));
    }

    // =========================================================================
    // 11. UPDATE child FK to invalid value — rejected
    // =========================================================================

    @Test void update_child_fk_to_invalid_value_rejected() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        SQLException ex = assertThrows(SQLException.class,
                () -> exec("UPDATE ks2.child SET parent_id = 999 WHERE id = 10"));
        assertEquals("23503", ex.getSQLState());
    }

    // =========================================================================
    // 12. FK from ks1 to ks2 (reverse direction)
    // =========================================================================

    @Test void fk_reverse_direction_ks1_references_ks2() throws SQLException {
        exec("CREATE TABLE ks2.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks1.child (id int PRIMARY KEY, parent_id int REFERENCES ks2.parent(id))");

        exec("INSERT INTO ks2.parent VALUES (1)");
        exec("INSERT INTO ks1.child VALUES (10, 1)");

        // Invalid reference should fail
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks1.child VALUES (20, 99)"));
        assertEquals("23503", ex.getSQLState());
    }

    // =========================================================================
    // 13. Bidirectional FKs between schemas
    // =========================================================================

    @Test void bidirectional_fks_between_schemas() throws SQLException {
        exec("CREATE TABLE ks1.a (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.b (id int PRIMARY KEY, a_id int REFERENCES ks1.a(id))");
        // Add FK from ks1.a back to ks2.b (deferred-style — nullable column)
        exec("ALTER TABLE ks1.a ADD COLUMN b_id int");
        exec("ALTER TABLE ks1.a ADD CONSTRAINT fk_b FOREIGN KEY (b_id) REFERENCES ks2.b(id)");

        exec("INSERT INTO ks1.a (id) VALUES (1)");
        exec("INSERT INTO ks2.b VALUES (10, 1)");
        exec("UPDATE ks1.a SET b_id = 10 WHERE id = 1");

        assertEquals("10", querySingle("SELECT b_id FROM ks1.a WHERE id = 1"));
        assertEquals("1", querySingle("SELECT a_id FROM ks2.b WHERE id = 10"));
    }

    // =========================================================================
    // 14. Referenced table exists in BOTH schemas — unqualified REFERENCES
    //     should resolve to search_path (default: public, not ks1 or ks2)
    // =========================================================================

    @Test void ambiguous_table_name_unqualified_uses_search_path() throws SQLException {
        // Create "parent" in both schemas
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.parent (id int PRIMARY KEY)");

        // Set search_path to ks2, ks1 — unqualified "parent" should resolve to ks2.parent
        exec("SET search_path = ks2, ks1");

        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES parent(id))");

        // Insert into ks2.parent (the one search_path resolves to)
        exec("INSERT INTO ks2.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        // Value in ks1.parent should NOT satisfy the FK
        exec("INSERT INTO ks1.parent VALUES (99)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks2.child VALUES (20, 99)"));
        // 99 exists in ks1.parent but FK points to ks2.parent
        assertEquals("23503", ex.getSQLState(),
                "FK should reference ks2.parent (search_path), not ks1.parent. Got: " + ex.getMessage());

        exec("SET search_path = public");
    }

    // =========================================================================
    // 15. Schema-qualified REFERENCES resolves correctly despite search_path
    // =========================================================================

    @Test void schema_qualified_fk_ignores_search_path() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.parent (id int PRIMARY KEY)");

        // search_path has ks2 first, but FK explicitly references ks1.parent
        exec("SET search_path = ks2, ks1");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        // Insert into ks1.parent — this should satisfy the FK
        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        // Insert into ks2.parent — should NOT satisfy the FK
        exec("INSERT INTO ks2.parent VALUES (99)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks2.child VALUES (20, 99)"));
        assertEquals("23503", ex.getSQLState(),
                "FK references ks1.parent explicitly, 99 only in ks2.parent. Got: " + ex.getMessage());

        exec("SET search_path = public");
    }

    // =========================================================================
    // 16. ALTER TABLE ADD FK — ambiguous table, schema-qualified
    // =========================================================================

    @Test void alter_table_add_fk_schema_qualified_with_ambiguity() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int)");

        // Explicitly reference ks1.parent
        exec("ALTER TABLE ks2.child ADD CONSTRAINT fk_p FOREIGN KEY (parent_id) REFERENCES ks1.parent(id)");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        // ks2.parent value should not satisfy the FK
        exec("INSERT INTO ks2.parent VALUES (50)");
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks2.child VALUES (20, 50)"));
        assertEquals("23503", ex.getSQLState());
    }

    // =========================================================================
    // 17. DROP SCHEMA CASCADE drops dependent FKs
    // =========================================================================

    @Test void drop_schema_cascade_removes_cross_schema_fk() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        // Drop the schema containing the parent — should CASCADE to FK on child
        exec("DROP SCHEMA ks1 CASCADE");

        // child table should still exist but FK should be gone
        // inserting any parent_id should work now (no FK to enforce)
        exec("INSERT INTO ks2.child VALUES (20, 999)");
        assertEquals(2, queryInt("SELECT count(*) FROM ks2.child"));
    }

    // =========================================================================
    // 18. DROP referenced table — blocked without CASCADE
    // =========================================================================

    @Test void drop_referenced_table_blocked_without_cascade() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        SQLException ex = assertThrows(SQLException.class,
                () -> exec("DROP TABLE ks1.parent"));
        // PG: 2BP01 (dependent_objects_still_exist)
        assertTrue(ex.getSQLState().equals("2BP01") || ex.getMessage().contains("depends on"),
                "Expected dependency error, got: [" + ex.getSQLState() + "] " + ex.getMessage());
    }

    // =========================================================================
    // 19. DROP referenced table CASCADE — FK removed, child survives
    // =========================================================================

    @Test void drop_referenced_table_cascade_removes_fk() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        exec("DROP TABLE ks1.parent CASCADE");

        // child should still exist, FK gone — any value accepted
        exec("INSERT INTO ks2.child VALUES (20, 999)");
        assertEquals(2, queryInt("SELECT count(*) FROM ks2.child"));
    }

    // =========================================================================
    // 20. Composite FK across schemas
    // =========================================================================

    @Test void composite_fk_across_schemas() throws SQLException {
        exec("CREATE TABLE ks1.parent (a int, b int, PRIMARY KEY (a, b))");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, pa int, pb int, " +
                "FOREIGN KEY (pa, pb) REFERENCES ks1.parent(a, b))");

        exec("INSERT INTO ks1.parent VALUES (1, 2), (3, 4)");
        exec("INSERT INTO ks2.child VALUES (10, 1, 2)");

        // Invalid composite reference
        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks2.child VALUES (20, 1, 4)"));
        assertEquals("23503", ex.getSQLState());
    }

    // =========================================================================
    // 21. FK referencing UNIQUE (not PK) across schemas
    // =========================================================================

    @Test void fk_references_unique_column_across_schemas() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY, code text UNIQUE)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_code text REFERENCES ks1.parent(code))");

        exec("INSERT INTO ks1.parent VALUES (1, 'A'), (2, 'B')");
        exec("INSERT INTO ks2.child VALUES (10, 'A')");

        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks2.child VALUES (20, 'Z')"));
        assertEquals("23503", ex.getSQLState());
    }

    // =========================================================================
    // 22. ALTER TABLE ADD FK fails validation on existing data
    // =========================================================================

    @Test void alter_table_add_fk_rejects_existing_invalid_data() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int)");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1), (20, 99)"); // 99 is invalid

        SQLException ex = assertThrows(SQLException.class,
                () -> exec("ALTER TABLE ks2.child ADD CONSTRAINT fk_p FOREIGN KEY (parent_id) REFERENCES ks1.parent(id)"));
        assertEquals("23503", ex.getSQLState(),
                "Should reject FK creation due to existing invalid data. Got: " + ex.getMessage());
    }

    // =========================================================================
    // 23. Multiple FKs from one child to different parent schemas
    // =========================================================================

    @Test void child_with_fks_to_multiple_schemas() throws SQLException {
        exec("CREATE TABLE ks1.users (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.products (id int PRIMARY KEY)");
        exec("CREATE TABLE ks1.orders (id int PRIMARY KEY, " +
                "user_id int REFERENCES ks1.users(id), " +
                "product_id int REFERENCES ks2.products(id))");

        exec("INSERT INTO ks1.users VALUES (1)");
        exec("INSERT INTO ks2.products VALUES (100)");
        exec("INSERT INTO ks1.orders VALUES (1000, 1, 100)");

        // Invalid user
        SQLException ex1 = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks1.orders VALUES (1001, 99, 100)"));
        assertEquals("23503", ex1.getSQLState());

        // Invalid product
        SQLException ex2 = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks1.orders VALUES (1002, 1, 999)"));
        assertEquals("23503", ex2.getSQLState());
    }

    // =========================================================================
    // 24. Self-referencing FK in non-public schema
    // =========================================================================

    @Test void self_referencing_fk_in_non_public_schema() throws SQLException {
        exec("CREATE TABLE ks1.tree (id int PRIMARY KEY, parent_id int REFERENCES ks1.tree(id))");

        exec("INSERT INTO ks1.tree VALUES (1, NULL)");
        exec("INSERT INTO ks1.tree VALUES (2, 1)");
        exec("INSERT INTO ks1.tree VALUES (3, 1)");

        SQLException ex = assertThrows(SQLException.class,
                () -> exec("INSERT INTO ks1.tree VALUES (4, 99)"));
        assertEquals("23503", ex.getSQLState());
    }

    // =========================================================================
    // 25. TRUNCATE child table — should work even with cross-schema FK
    // =========================================================================

    @Test void truncate_child_with_cross_schema_fk() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        exec("TRUNCATE ks2.child");
        assertEquals(0, queryInt("SELECT count(*) FROM ks2.child"));
    }

    // =========================================================================
    // 26. TRUNCATE parent — should fail if child has rows referencing it
    // =========================================================================

    @Test void truncate_parent_blocked_by_cross_schema_child() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1)");
        exec("INSERT INTO ks2.child VALUES (10, 1)");

        SQLException ex = assertThrows(SQLException.class,
                () -> exec("TRUNCATE ks1.parent"));
        // PG uses 0A000 for TRUNCATE FK violation
        assertTrue(ex.getSQLState().equals("0A000") || ex.getSQLState().equals("23503"),
                "Expected truncate FK error, got: [" + ex.getSQLState() + "] " + ex.getMessage());
    }

    // =========================================================================
    // 27. TRUNCATE CASCADE across schemas
    // =========================================================================

    @Test void truncate_cascade_across_schemas() throws SQLException {
        exec("CREATE TABLE ks1.parent (id int PRIMARY KEY)");
        exec("CREATE TABLE ks2.child (id int PRIMARY KEY, parent_id int REFERENCES ks1.parent(id))");

        exec("INSERT INTO ks1.parent VALUES (1), (2)");
        exec("INSERT INTO ks2.child VALUES (10, 1), (20, 2)");

        exec("TRUNCATE ks1.parent CASCADE");

        assertEquals(0, queryInt("SELECT count(*) FROM ks1.parent"));
        assertEquals(0, queryInt("SELECT count(*) FROM ks2.child"));
    }
}
