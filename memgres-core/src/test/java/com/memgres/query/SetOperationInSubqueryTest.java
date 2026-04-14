package com.memgres.query;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for UNION / UNION ALL / INTERSECT / EXCEPT inside subquery contexts.
 *
 * PostgreSQL allows set operations anywhere a subquery is valid:
 * EXISTS(...), IN(...), scalar subquery, FROM subquery, CTE body,
 * INSERT...SELECT, CREATE VIEW, etc.
 *
 * Migration tools use EXISTS with UNION ALL
 * to check schema state across multiple catalog tables in a single query.
 * ORM frameworks and web frameworks generate UNION queries in various contexts.
 */
class SetOperationInSubqueryTest {

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
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE products (id serial PRIMARY KEY, name text, category text, price numeric)");
            s.execute("CREATE TABLE services (id serial PRIMARY KEY, name text, category text, rate numeric)");
            s.execute("CREATE TABLE archive (id serial PRIMARY KEY, name text, source text)");
            s.execute("INSERT INTO products (name, category, price) VALUES ('Widget', 'hardware', 9.99), ('Gadget', 'electronics', 19.99), ('Tool', 'hardware', 14.99)");
            s.execute("INSERT INTO services (name, category, rate) VALUES ('Consulting', 'professional', 100), ('Support', 'technical', 50), ('Widget', 'hardware', 25)");
            s.execute("INSERT INTO archive (name, source) VALUES ('OldWidget', 'products'), ('OldService', 'services')");
        }
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

    private boolean queryBool(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next()); return rs.getBoolean(1);
        }
    }

    // =========================================================================
    // 1. EXISTS context
    // =========================================================================

    @Test
    void testExistsWithUnionAll() throws SQLException {
        assertTrue(queryBool("""
            SELECT EXISTS (
                SELECT id FROM products WHERE category = 'hardware'
                UNION ALL
                SELECT id FROM services WHERE category = 'technical'
            )
        """));
    }

    @Test
    void testExistsWithUnionAllThreeBranches() throws SQLException {
        // 3 UNION ALL branches for schema introspection
        assertTrue(queryBool("""
            SELECT EXISTS (
                SELECT id FROM products WHERE category = 'hardware'
                UNION ALL
                SELECT id FROM services WHERE category = 'professional'
                UNION ALL
                SELECT id FROM archive WHERE source = 'products'
            )
        """));
    }

    @Test
    void testExistsWithUnionDistinct() throws SQLException {
        assertTrue(queryBool("""
            SELECT EXISTS (
                SELECT name FROM products
                UNION
                SELECT name FROM services
            )
        """));
    }

    @Test
    void testExistsWithIntersect() throws SQLException {
        // INTERSECT inside EXISTS: finds names that appear in both tables
        // 'Widget' exists in both products and services (added in setUp)
        assertTrue(queryBool("""
            SELECT EXISTS (
                SELECT name FROM products
                INTERSECT
                SELECT name FROM services
            )
        """));
    }

    @Test
    void testExistsWithExcept() throws SQLException {
        // EXCEPT inside EXISTS: finds names in products but not services
        assertTrue(queryBool("""
            SELECT EXISTS (
                SELECT name FROM products
                EXCEPT
                SELECT name FROM services
            )
        """));
    }

    @Test
    void testNotExistsWithUnionAll() throws SQLException {
        assertFalse(queryBool("""
            SELECT NOT EXISTS (
                SELECT id FROM products
                UNION ALL
                SELECT id FROM services
            )
        """));
    }

    @Test
    void testExistsWithCorrelatedUnionAll() throws SQLException {
        // Correlated: outer query references in both UNION branches
        assertNotNull(query1("""
            SELECT p.name FROM products p
            WHERE EXISTS (
                SELECT 1 FROM archive a WHERE a.name = p.name
                UNION ALL
                SELECT 1 FROM services s WHERE s.name = p.name
            )
            LIMIT 1
        """));
    }

    // =========================================================================
    // 2. IN context
    // =========================================================================

    @Test
    void testInWithUnionAll() throws SQLException {
        assertNotNull(query1("""
            SELECT COUNT(*) FROM products
            WHERE name IN (
                SELECT name FROM services
                UNION ALL
                SELECT name FROM archive
            )
        """));
    }

    @Test
    void testNotInWithUnion() throws SQLException {
        assertNotNull(query1("""
            SELECT COUNT(*) FROM products
            WHERE name NOT IN (
                SELECT name FROM services
                UNION
                SELECT name FROM archive
            )
        """));
    }

    @Test
    void testInWithIntersect() throws SQLException {
        assertNotNull(query1("""
            SELECT COUNT(*) FROM archive
            WHERE name IN (
                SELECT name FROM products
                INTERSECT
                SELECT name FROM services
            )
        """));
    }

    // =========================================================================
    // 3. Scalar subquery context
    // =========================================================================

    @Test
    void testScalarSubqueryWithUnionAllAndLimit() throws SQLException {
        assertNotNull(query1("""
            SELECT (
                SELECT name FROM (
                    SELECT name FROM products
                    UNION ALL
                    SELECT name FROM services
                ) combined
                ORDER BY name LIMIT 1
            )
        """));
    }

    @Test
    void testScalarSubqueryInSelectList() throws SQLException {
        assertNotNull(query1("""
            SELECT
                (SELECT COUNT(*) FROM products) +
                (SELECT COUNT(*) FROM services)
            AS total
        """));
    }

    @Test
    void testComparisonWithUnionSubquery() throws SQLException {
        assertNotNull(query1("""
            SELECT name FROM products
            WHERE price > (
                SELECT MIN(val) FROM (
                    SELECT price AS val FROM products
                    UNION ALL
                    SELECT rate AS val FROM services
                ) combined
            )
            LIMIT 1
        """));
    }

    // =========================================================================
    // 4. FROM subquery context
    // =========================================================================

    @Test
    void testFromSubqueryWithUnionAll() throws SQLException {
        assertEquals("6", query1("""
            SELECT COUNT(*) FROM (
                SELECT name, category FROM products
                UNION ALL
                SELECT name, category FROM services
            ) combined
        """));
    }

    @Test
    void testFromSubqueryWithUnionAllAndAlias() throws SQLException {
        assertNotNull(query1("""
            SELECT combined.item_name, combined.item_category
            FROM (
                SELECT name AS item_name, category AS item_category FROM products
                UNION ALL
                SELECT name, category FROM services
            ) combined
            ORDER BY combined.item_name LIMIT 1
        """));
    }

    @Test
    void testFromSubqueryWithUnionJoinedToTable() throws SQLException {
        assertNotNull(query1("""
            SELECT combined.name, a.source
            FROM (
                SELECT name FROM products
                UNION
                SELECT name FROM services
            ) combined
            LEFT JOIN archive a ON a.name = combined.name
            LIMIT 1
        """));
    }

    // =========================================================================
    // 5. CTE context
    // =========================================================================

    @Test
    void testCteWithUnionAll() throws SQLException {
        assertEquals("6", query1("""
            WITH all_items AS (
                SELECT name, category FROM products
                UNION ALL
                SELECT name, category FROM services
            )
            SELECT COUNT(*) FROM all_items
        """));
    }

    @Test
    void testCteWithUnionUsedInExists() throws SQLException {
        assertTrue(queryBool("""
            WITH combined AS (
                SELECT name FROM products
                UNION
                SELECT name FROM services
            )
            SELECT EXISTS (SELECT 1 FROM combined WHERE name = 'Widget')
        """));
    }

    @Test
    void testMultipleCtesBothWithUnion() throws SQLException {
        assertNotNull(query1("""
            WITH hw AS (
                SELECT name FROM products WHERE category = 'hardware'
                UNION ALL
                SELECT name FROM archive WHERE source = 'products'
            ),
            tech AS (
                SELECT name FROM services WHERE category = 'technical'
                UNION ALL
                SELECT name FROM archive WHERE source = 'services'
            )
            SELECT (SELECT COUNT(*) FROM hw) + (SELECT COUNT(*) FROM tech)
        """));
    }

    // =========================================================================
    // 6. INSERT ... SELECT context
    // =========================================================================

    @Test
    void testInsertSelectWithUnionAll() throws SQLException {
        exec("CREATE TABLE combined_log (name text, source text)");
        exec("""
            INSERT INTO combined_log (name, source)
            SELECT name, 'product' FROM products
            UNION ALL
            SELECT name, 'service' FROM services
        """);
        assertEquals("6", query1("SELECT COUNT(*) FROM combined_log"));
    }

    // =========================================================================
    // 7. CREATE VIEW context
    // =========================================================================

    @Test
    void testCreateViewWithUnionAll() throws SQLException {
        exec("""
            CREATE VIEW all_offerings AS
            SELECT name, category, price AS amount, 'product' AS type FROM products
            UNION ALL
            SELECT name, category, rate, 'service' FROM services
        """);
        assertEquals("6", query1("SELECT COUNT(*) FROM all_offerings"));
    }

    @Test
    void testCreateViewWithUnionAndOrderBy() throws SQLException {
        exec("""
            CREATE VIEW sorted_names AS
            SELECT name FROM products
            UNION
            SELECT name FROM services
            ORDER BY name
        """);
    }

    // =========================================================================
    // 8. System catalog introspection patterns
    // =========================================================================

    @Test
    void testMigrationToolStyleSchemaIntrospection() throws SQLException {
        // EXISTS with UNION ALL across pg_class, pg_type, pg_proc
        assertNotNull(query1("""
            SELECT EXISTS (
                SELECT c.oid FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public' AND c.relkind IN ('r', 'v', 'S', 't')
              UNION ALL
                SELECT t.oid FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname = 'public' AND t.typcategory NOT IN ('A', 'C')
              UNION ALL
                SELECT p.oid FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'public'
            )
        """));
    }

    @Test
    void testMigrationToolStyleWithLeftJoinPgDepend() throws SQLException {
        // Full schema introspection pattern including pg_depend LEFT JOIN
        assertNotNull(query1("""
            SELECT EXISTS (
                SELECT c.oid FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_depend d ON d.objid = c.oid AND d.deptype = 'e'
                WHERE n.nspname = 'public' AND d.objid IS NULL AND c.relkind IN ('r', 'v', 'S', 't')
              UNION ALL
                SELECT t.oid FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                LEFT JOIN pg_catalog.pg_depend d ON d.objid = t.oid AND d.deptype = 'e'
                WHERE n.nspname = 'public' AND d.objid IS NULL AND t.typcategory NOT IN ('A', 'C')
              UNION ALL
                SELECT p.oid FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                LEFT JOIN pg_catalog.pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
                WHERE n.nspname = 'public' AND d.objid IS NULL
            )
        """));
    }

    @Test
    void testSchemaEmptinessCheck() throws SQLException {
        // Common pattern: check if a schema has any objects
        assertNotNull(query1("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables WHERE schemaname = 'public'
                UNION ALL
                SELECT 1 FROM pg_views WHERE schemaname = 'public'
                UNION ALL
                SELECT 1 FROM pg_sequences WHERE schemaname = 'public'
            )
        """));
    }

    @Test
    void testChangelogStyleTableCheck() throws SQLException {
        // Check for migration tracking tables
        assertNotNull(query1("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables WHERE tablename = 'products' AND schemaname = 'public'
                UNION ALL
                SELECT 1 FROM pg_tables WHERE tablename = 'services' AND schemaname = 'public'
            )
        """));
    }

    // =========================================================================
    // 9. Edge cases
    // =========================================================================

    @Test
    void testUnionAllFourBranches() throws SQLException {
        assertEquals("9", query1("""
            SELECT COUNT(*) FROM (
                SELECT name FROM products
                UNION ALL
                SELECT name FROM services
                UNION ALL
                SELECT name FROM archive
                UNION ALL
                SELECT 'extra' AS name
            ) combined
        """));
    }

    @Test
    void testUnionWithOrderByAndLimitInsideExists() throws SQLException {
        // ORDER BY and LIMIT on the UNION result inside EXISTS
        assertTrue(queryBool("""
            SELECT EXISTS (
                SELECT * FROM (
                    SELECT name FROM products
                    UNION ALL
                    SELECT name FROM services
                    ORDER BY name
                    LIMIT 1
                ) limited
            )
        """));
    }

    @Test
    void testMixedUnionAndExcept() throws SQLException {
        assertNotNull(query1("""
            SELECT COUNT(*) FROM (
                SELECT name FROM products
                UNION ALL
                SELECT name FROM services
                EXCEPT
                SELECT name FROM archive
            ) result
        """));
    }

    @Test
    void testUnionInCaseWhenExists() throws SQLException {
        assertNotNull(query1("""
            SELECT CASE
                WHEN EXISTS (
                    SELECT 1 FROM products WHERE category = 'hardware'
                    UNION ALL
                    SELECT 1 FROM services WHERE category = 'technical'
                ) THEN 'found'
                ELSE 'empty'
            END
        """));
    }

    @Test
    void testUnionInPlpgsqlIfExists() throws SQLException {
        exec("CREATE TABLE union_check_result (found boolean)");
        exec("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM products WHERE category = 'hardware'
                    UNION ALL
                    SELECT 1 FROM services WHERE category = 'nonexistent'
                ) THEN
                    INSERT INTO union_check_result VALUES (true);
                ELSE
                    INSERT INTO union_check_result VALUES (false);
                END IF;
            END;
            $$
        """);
        assertEquals("true", query1("SELECT found::text FROM union_check_result"));
    }

    @Test
    void testUnionAllInAnySubquery() throws SQLException {
        assertNotNull(query1("""
            SELECT COUNT(*) FROM products
            WHERE name = ANY (
                SELECT name FROM services
                UNION ALL
                SELECT name FROM archive
            )
        """));
    }

    @Test
    void testExistsWithUnionAllAndParameterPlaceholderStyle() throws SQLException {
        // Simulates prepared statement style (though we use literal values here)
        // The key pattern: WHERE uses = comparison with ? parameters
        String schema = "public";
        assertTrue(queryBool(String.format("""
            SELECT EXISTS (
                SELECT c.oid FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = '%s' AND c.relkind IN ('r', 'v')
              UNION ALL
                SELECT t.oid FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE n.nspname = '%s'
            )
        """, schema, schema)));
    }

    // =========================================================================
    // 10. PreparedStatement with ? parameter binding
    // =========================================================================

    @Test
    void testPreparedExistsWithUnionAll() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT id FROM products WHERE category = ?
                UNION ALL
                SELECT id FROM services WHERE category = ?
            )
        """)) {
            ps.setString(1, "hardware");
            ps.setString(2, "technical");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testPreparedExistsWithUnionAllThreeBranches() throws SQLException {
        // Schema introspection query pattern with ? parameters
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT id FROM products WHERE category = ?
                UNION ALL
                SELECT id FROM services WHERE category = ?
                UNION ALL
                SELECT id FROM archive WHERE source = ?
            )
        """)) {
            ps.setString(1, "hardware");
            ps.setString(2, "professional");
            ps.setString(3, "products");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testPreparedMigrationToolSchemaIntrospection() throws SQLException {
        // Schema check query with ? parameters across pg_catalog tables
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT c.oid FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_catalog.pg_depend d ON d.objid = c.oid AND d.deptype = 'e'
                WHERE n.nspname = ? AND d.objid IS NULL AND c.relkind IN ('r', 'v', 'S', 't')
              UNION ALL
                SELECT t.oid FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                LEFT JOIN pg_catalog.pg_depend d ON d.objid = t.oid AND d.deptype = 'e'
                WHERE n.nspname = ? AND d.objid IS NULL AND t.typcategory NOT IN ('A', 'C')
              UNION ALL
                SELECT p.oid FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
                LEFT JOIN pg_catalog.pg_depend d ON d.objid = p.oid AND d.deptype = 'e'
                WHERE n.nspname = ? AND d.objid IS NULL
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "public");
            ps.setString(3, "public");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testPreparedInWithUnionAll() throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT COUNT(*) FROM products
            WHERE category IN (
                SELECT category FROM products WHERE price > ?
                UNION ALL
                SELECT category FROM services WHERE rate > ?
            )
        """)) {
            ps.setBigDecimal(1, new java.math.BigDecimal("10"));
            ps.setBigDecimal(2, new java.math.BigDecimal("60"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
            }
        }
    }

    @Test
    void testPreparedExistsReturningFalse() throws SQLException {
        // Ensure it correctly returns false when no rows match
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT id FROM products WHERE category = ?
                UNION ALL
                SELECT id FROM services WHERE category = ?
            )
        """)) {
            ps.setString(1, "nonexistent_category_xyz");
            ps.setString(2, "also_nonexistent_abc");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertFalse(rs.getBoolean(1));
            }
        }
    }

    @Test
    void testPreparedSameParameterReusedAcrossBranches() throws SQLException {
        // Same schema name parameter used in all branches
        try (PreparedStatement ps = conn.prepareStatement("""
            SELECT EXISTS (
                SELECT 1 FROM pg_tables WHERE schemaname = ?
                UNION ALL
                SELECT 1 FROM pg_views WHERE schemaname = ?
            )
        """)) {
            ps.setString(1, "public");
            ps.setString(2, "public");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean(1));
            }
        }
    }
}
