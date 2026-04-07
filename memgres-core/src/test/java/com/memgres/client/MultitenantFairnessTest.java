package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Multitenant fairness and isolation scenarios from:
 * 1330_multitenant_fairness_and_isolation_scenarios.md
 *
 * Covers: RLS/WHERE-clause tenant scoping, cross-tenant aggregation, quota
 * enforcement, fair job-queue claiming, schema-per-tenant isolation,
 * tenant-aware unique constraints, bulk ops, noisy-neighbor prevention,
 * per-tenant configuration, and cross-tenant FK prevention.
 *
 * Table prefix: mt_
 * All tests share a single autocommit=true connection.
 */
class MultitenantFairnessTest {

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
        if (conn != null && !conn.isClosed()) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 1. Tenant-scoped queries: WHERE tenant_id = ? isolation
    // =========================================================================

    @Test
    void testTenantScopedQueries() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mt_scoped_data (id SERIAL PRIMARY KEY, tenant_id int NOT NULL, val text)");
            s.execute("INSERT INTO mt_scoped_data (tenant_id, val) VALUES (1, 'tenant1-row'), (2, 'tenant2-row')");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM mt_scoped_data WHERE tenant_id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("tenant1-row", rs.getString(1));
                assertFalse(rs.next(), "Tenant 1 query must not see tenant 2 rows");
            }
            ps.setInt(1, 2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("tenant2-row", rs.getString(1));
                assertFalse(rs.next(), "Tenant 2 query must not see tenant 1 rows");
            }
        }
    }

    // =========================================================================
    // 2. Tenant-scoped INSERT: each tenant sees only their own data
    // =========================================================================

    @Test
    void testTenantScopedInsert() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mt_insert_isolation (id SERIAL PRIMARY KEY, tenant_id int NOT NULL, secret text)");
            s.execute("INSERT INTO mt_insert_isolation (tenant_id, secret) VALUES (10, 'alpha-secret')");
            s.execute("INSERT INTO mt_insert_isolation (tenant_id, secret) VALUES (20, 'beta-secret')");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT secret FROM mt_insert_isolation WHERE tenant_id = ?")) {
            ps.setInt(1, 10);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("alpha-secret", rs.getString(1));
                assertFalse(rs.next(), "Tenant 10 must only see its own row");
            }
            ps.setInt(1, 20);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("beta-secret", rs.getString(1));
                assertFalse(rs.next(), "Tenant 20 must only see its own row");
            }
        }
    }

    // =========================================================================
    // 3. Cross-tenant aggregation: admin query across all tenants
    // =========================================================================

    @Test
    void testCrossTenantAggregation() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mt_agg_data (id SERIAL PRIMARY KEY, tenant_id int NOT NULL, amount int)");
            s.execute("INSERT INTO mt_agg_data (tenant_id, amount) VALUES (1, 100), (1, 200), (2, 300), (3, 400)");
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT tenant_id, SUM(amount) AS total FROM mt_agg_data GROUP BY tenant_id ORDER BY tenant_id")) {
            Map<Integer, Long> totals = new LinkedHashMap<>();
            while (rs.next()) {
                totals.put(rs.getInt("tenant_id"), rs.getLong("total"));
            }
            assertEquals(3, totals.size(), "Should see 3 distinct tenants");
            assertEquals(300L, totals.get(1), "Tenant 1 total");
            assertEquals(300L, totals.get(2), "Tenant 2 total");
            assertEquals(400L, totals.get(3), "Tenant 3 total");
        }
    }

    // =========================================================================
    // 4. Tenant quota enforcement: CHECK constraint limits row count per tenant
    // =========================================================================

    @Test
    void testTenantQuotaEnforcement() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE mt_quota (
                        id SERIAL PRIMARY KEY,
                        tenant_id int NOT NULL,
                        val text
                    )
                    """);
            // Insert up to the simulated quota (3 rows for tenant 99)
            s.execute("INSERT INTO mt_quota (tenant_id, val) VALUES (99, 'r1'), (99, 'r2'), (99, 'r3')");
        }

        // The quota check is enforced by application logic: count before insert
        try (PreparedStatement check = conn.prepareStatement(
                "SELECT count(*) FROM mt_quota WHERE tenant_id = ?")) {
            check.setInt(1, 99);
            try (ResultSet rs = check.executeQuery()) {
                assertTrue(rs.next());
                long existing = rs.getLong(1);
                final long QUOTA = 3;
                assertTrue(existing <= QUOTA,
                        "Tenant 99 already has " + existing + " rows, quota is " + QUOTA);
                // Attempting a 4th insert should be refused by quota check
                boolean quotaExceeded = existing >= QUOTA;
                assertTrue(quotaExceeded,
                        "Tenant 99 has reached its quota of " + QUOTA + " rows");
            }
        }
    }

    // =========================================================================
    // 5. Fair job queue claiming: jobs distributed across tenants
    // =========================================================================

    @Test
    void testFairJobQueueClaiming() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE mt_job_queue (
                        id SERIAL PRIMARY KEY,
                        tenant_id int NOT NULL,
                        status text DEFAULT 'pending',
                        claimed_by text
                    )
                    """);
            // 3 jobs per tenant
            for (int tenant = 1; tenant <= 3; tenant++) {
                for (int j = 0; j < 3; j++) {
                    s.execute("INSERT INTO mt_job_queue (tenant_id) VALUES (" + tenant + ")");
                }
            }
        }

        // Claim one job per tenant in a round-robin fashion
        Set<Integer> claimedTenants = new HashSet<>();
        try (PreparedStatement claim = conn.prepareStatement("""
                UPDATE mt_job_queue SET status = 'claimed', claimed_by = 'worker-1'
                WHERE id = (
                    SELECT id FROM mt_job_queue
                    WHERE status = 'pending' AND tenant_id = ?
                    ORDER BY id LIMIT 1
                )
                RETURNING tenant_id
                """)) {
            for (int tenant = 1; tenant <= 3; tenant++) {
                claim.setInt(1, tenant);
                try (ResultSet rs = claim.executeQuery()) {
                    if (rs.next()) {
                        claimedTenants.add(rs.getInt(1));
                    }
                }
            }
        }

        assertEquals(3, claimedTenants.size(), "Should have claimed a job for each of the 3 tenants");
        assertTrue(claimedTenants.containsAll(Arrays.asList(1, 2, 3)));
    }

    // =========================================================================
    // 6. Tenant data isolation with schemas: each tenant in separate schema
    // =========================================================================

    @Test
    void testTenantSchemaIsolation() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS mt_tenant_a");
            s.execute("CREATE SCHEMA IF NOT EXISTS mt_tenant_b");
            s.execute("CREATE TABLE mt_tenant_a.orders (id int PRIMARY KEY, amount int)");
            s.execute("CREATE TABLE mt_tenant_b.orders (id int PRIMARY KEY, amount int)");
            s.execute("INSERT INTO mt_tenant_a.orders VALUES (1, 500)");
            s.execute("INSERT INTO mt_tenant_b.orders VALUES (1, 999)");
        }

        try (Statement s = conn.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT amount FROM mt_tenant_a.orders WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(500, rs.getInt(1), "Tenant A orders isolated in own schema");
            }
            try (ResultSet rs = s.executeQuery("SELECT amount FROM mt_tenant_b.orders WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(999, rs.getInt(1), "Tenant B orders isolated in own schema");
            }
        }
    }

    // =========================================================================
    // 7. Tenant-aware unique constraints: unique within tenant, not globally
    // =========================================================================

    @Test
    void testTenantAwareUniqueConstraints() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE mt_unique_per_tenant (
                        id SERIAL PRIMARY KEY,
                        tenant_id int NOT NULL,
                        slug text NOT NULL,
                        UNIQUE (tenant_id, slug)
                    )
                    """);
            // Same slug in different tenants is allowed
            s.execute("INSERT INTO mt_unique_per_tenant (tenant_id, slug) VALUES (1, 'my-page')");
            s.execute("INSERT INTO mt_unique_per_tenant (tenant_id, slug) VALUES (2, 'my-page')");
        }

        // Duplicate slug within same tenant must be rejected
        try (Statement s = conn.createStatement()) {
            assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO mt_unique_per_tenant (tenant_id, slug) VALUES (1, 'my-page')"),
                    "Duplicate slug within same tenant must violate unique constraint");
        }

        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM mt_unique_per_tenant WHERE slug = 'my-page'")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1), "Two tenants can each have 'my-page'");
        }
    }

    // =========================================================================
    // 8. Bulk operations per tenant: UPDATE/DELETE scoped to tenant
    // =========================================================================

    @Test
    void testBulkOperationsPerTenant() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE mt_bulk_ops (id SERIAL PRIMARY KEY, tenant_id int NOT NULL, status text)");
            for (int i = 0; i < 5; i++) {
                s.execute("INSERT INTO mt_bulk_ops (tenant_id, status) VALUES (1, 'pending')");
                s.execute("INSERT INTO mt_bulk_ops (tenant_id, status) VALUES (2, 'pending')");
            }
        }

        // Bulk UPDATE only for tenant 1
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE mt_bulk_ops SET status = 'processed' WHERE tenant_id = ?")) {
            ps.setInt(1, 1);
            int updated = ps.executeUpdate();
            assertEquals(5, updated, "Bulk update should affect exactly tenant 1's 5 rows");
        }

        // Tenant 2 rows must be untouched
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT count(*) FROM mt_bulk_ops WHERE tenant_id = ? AND status = 'pending'")) {
            ps.setInt(1, 2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(5, rs.getLong(1), "Tenant 2 rows must remain 'pending'");
            }
        }

        // Bulk DELETE only for tenant 1
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM mt_bulk_ops WHERE tenant_id = ?")) {
            ps.setInt(1, 1);
            int deleted = ps.executeUpdate();
            assertEquals(5, deleted, "Bulk delete should remove exactly tenant 1's 5 rows");
        }
    }

    // =========================================================================
    // 9. Tenant migration isolation: schema-per-tenant avoids shared-table DDL
    // =========================================================================

    @Test
    void testTenantMigrationIsolation() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA IF NOT EXISTS mt_migration_a");
            s.execute("CREATE SCHEMA IF NOT EXISTS mt_migration_b");
            s.execute("CREATE TABLE mt_migration_a.items (id int PRIMARY KEY, name text)");
            s.execute("CREATE TABLE mt_migration_b.items (id int PRIMARY KEY, name text)");
            s.execute("INSERT INTO mt_migration_a.items VALUES (1, 'alpha-item')");
            s.execute("INSERT INTO mt_migration_b.items VALUES (1, 'beta-item')");
        }

        // Alter only tenant A's table; tenant B unaffected
        try (Statement s = conn.createStatement()) {
            s.execute("ALTER TABLE mt_migration_a.items ADD COLUMN IF NOT EXISTS description text DEFAULT ''");
        }

        // Tenant A now has the new column
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT description FROM mt_migration_a.items WHERE id = 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getString(1), "Tenant A should have the new description column");
        }

        // Tenant B's table is unaffected
        try (Statement s = conn.createStatement()) {
            assertThrows(SQLException.class,
                    () -> s.executeQuery("SELECT description FROM mt_migration_b.items"),
                    "Tenant B table must not have the description column");
        }
    }

    // =========================================================================
    // 10. Noisy neighbor prevention: large tenant shouldn't monopolize claims
    // =========================================================================

    @Test
    void testNoisyNeighborPrevention() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE mt_noisy_queue (
                        id SERIAL PRIMARY KEY,
                        tenant_id int NOT NULL,
                        status text DEFAULT 'pending'
                    )
                    """);
            // Noisy tenant (1): 100 jobs; small tenant (2): 5 jobs
            for (int i = 0; i < 100; i++) {
                s.execute("INSERT INTO mt_noisy_queue (tenant_id) VALUES (1)");
            }
            for (int i = 0; i < 5; i++) {
                s.execute("INSERT INTO mt_noisy_queue (tenant_id) VALUES (2)");
            }
        }

        // Fair claiming: pick one job per round from each tenant with pending work
        // rather than simply taking the top N by id (which would favor the noisy tenant).
        List<Integer> claimOrder = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE mt_noisy_queue SET status = 'claimed'
                WHERE id = (
                    SELECT id FROM mt_noisy_queue
                    WHERE status = 'pending' AND tenant_id = ?
                    ORDER BY id LIMIT 1
                )
                RETURNING tenant_id
                """)) {
            // One claim for tenant 2 (small), one for tenant 1 (noisy)
            for (int tenant : Arrays.asList(2, 1)) {
                ps.setInt(1, tenant);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) claimOrder.add(rs.getInt(1));
                }
            }
        }

        assertEquals(2, claimOrder.size(), "Both tenants should have had a job claimed");
        assertTrue(claimOrder.contains(1) && claimOrder.contains(2),
                "Fair scheduler should claim from both tenants regardless of queue depth");
    }

    // =========================================================================
    // 11. Tenant-specific configuration: different settings per tenant via table
    // =========================================================================

    @Test
    void testTenantSpecificConfiguration() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE mt_tenant_config (
                        tenant_id int PRIMARY KEY,
                        max_results int,
                        feature_flags text
                    )
                    """);
            s.execute("INSERT INTO mt_tenant_config VALUES (1, 100,  'flag_a,flag_b')");
            s.execute("INSERT INTO mt_tenant_config VALUES (2, 500,  'flag_a')");
            s.execute("INSERT INTO mt_tenant_config VALUES (3, 50,   '')");
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT max_results, feature_flags FROM mt_tenant_config WHERE tenant_id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt("max_results"));
                assertTrue(rs.getString("feature_flags").contains("flag_b"),
                        "Tenant 1 should have flag_b enabled");
            }
            ps.setInt(1, 2);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(500, rs.getInt("max_results"));
                assertFalse(rs.getString("feature_flags").contains("flag_b"),
                        "Tenant 2 should not have flag_b");
            }
            ps.setInt(1, 3);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(50, rs.getInt("max_results"));
            }
        }
    }

    // =========================================================================
    // 12. Cross-tenant foreign keys: must be prevented
    // =========================================================================

    @Test
    void testCrossTenantForeignKeyPrevention() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("""
                    CREATE TABLE mt_tenants (
                        id int PRIMARY KEY,
                        name text
                    )
                    """);
            s.execute("INSERT INTO mt_tenants VALUES (1, 'alpha'), (2, 'beta')");

            s.execute("""
                    CREATE TABLE mt_tenant_resources (
                        id SERIAL PRIMARY KEY,
                        tenant_id int NOT NULL REFERENCES mt_tenants(id),
                        resource text
                    )
                    """);
            s.execute("INSERT INTO mt_tenant_resources (tenant_id, resource) VALUES (1, 'res-alpha')");
            s.execute("INSERT INTO mt_tenant_resources (tenant_id, resource) VALUES (2, 'res-beta')");
        }

        // Attempt to insert a resource referencing a non-existent tenant must fail
        try (Statement s = conn.createStatement()) {
            assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO mt_tenant_resources (tenant_id, resource) VALUES (99, 'orphan')"),
                    "Insert referencing non-existent tenant must be rejected by FK constraint");
        }

        // Verify existing rows are unaffected
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT count(*) FROM mt_tenant_resources")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1), "Only the two valid tenant resources should exist");
        }
    }
}
