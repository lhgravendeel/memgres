package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Data quality invariant checks (1380_data_quality_invariant_check_scenarios.md).
 *
 * Each test is self-contained: it creates the tables it needs (prefixed {@code dq_}),
 * exercises the invariant-check pattern, then drops the objects. The shared connection
 * runs with {@code autoCommit=true} so independent DDL statements are immediately visible.
 */
class DataQualityInvariantTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void start() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test");
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void stop() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void exec(String sql) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private long scalarLong(String sql) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getLong(1);
        }
    }

    private String scalarString(String sql) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getString(1);
        }
    }

    /** Drop the given tables if they exist (CASCADE). */
    private void drop(String... tables) {
        for (String t : tables) {
            try { exec("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (Exception ignored) {}
        }
    }

    /** Drop the given views/materialized views if they exist. */
    private void dropViews(String... views) {
        for (String v : views) {
            try { exec("DROP MATERIALIZED VIEW IF EXISTS " + v + " CASCADE"); } catch (Exception ignored) {}
            try { exec("DROP VIEW IF EXISTS " + v + " CASCADE"); } catch (Exception ignored) {}
        }
    }

    // =========================================================================
    // 1. Orphan row detection
    // =========================================================================

    @Test
    @DisplayName("1. LEFT JOIN + IS NULL detects orphaned child rows with no matching parent")
    void orphanRowDetection() throws Exception {
        drop("dq_orders", "dq_customers");
        try {
            exec("CREATE TABLE dq_customers (id INT PRIMARY KEY, name TEXT)");
            exec("CREATE TABLE dq_orders   (id INT PRIMARY KEY, cust_id INT, amount NUMERIC)");

            exec("INSERT INTO dq_customers VALUES (1, 'Alice'), (2, 'Bob')");
            // Order 10 references non-existent customer 99; it is an orphan.
            exec("INSERT INTO dq_orders VALUES (10, 1, 100), (11, 99, 50)");

            long orphans = scalarLong(
                    "SELECT COUNT(*) FROM dq_orders o " +
                    "LEFT JOIN dq_customers c ON c.id = o.cust_id " +
                    "WHERE c.id IS NULL");
            assertEquals(1, orphans, "Exactly one orphan order should be detected");
        } finally {
            drop("dq_orders", "dq_customers");
        }
    }

    // =========================================================================
    // 2. Duplicate business key detection
    // =========================================================================

    @Test
    @DisplayName("2. GROUP BY + HAVING COUNT(*) > 1 finds duplicate business keys")
    void duplicateBusinessKeyDetection() throws Exception {
        drop("dq_products");
        try {
            exec("CREATE TABLE dq_products (id SERIAL, sku TEXT, price NUMERIC)");
            exec("INSERT INTO dq_products (sku, price) VALUES " +
                 "('SKU-A', 10), ('SKU-B', 20), ('SKU-A', 15)");  // SKU-A duplicated

            long dupeCount = scalarLong(
                    "SELECT COUNT(*) FROM (" +
                    "  SELECT sku FROM dq_products " +
                    "  GROUP BY sku HAVING COUNT(*) > 1" +
                    ") dupes");
            assertEquals(1, dupeCount, "Exactly one duplicate SKU should be found");
        } finally {
            drop("dq_products");
        }
    }

    // =========================================================================
    // 3. FK integrity verification
    // =========================================================================

    @Test
    @DisplayName("3. Count of child FK values not present in parent PK identifies referential violations")
    void fkIntegrityVerification() throws Exception {
        drop("dq_line_items", "dq_invoice");
        try {
            exec("CREATE TABLE dq_invoice    (id INT PRIMARY KEY, total NUMERIC)");
            exec("CREATE TABLE dq_line_items (id SERIAL, inv_id INT, qty INT)");

            exec("INSERT INTO dq_invoice VALUES (1, 300), (2, 150)");
            // inv_id = 99 has no matching invoice row.
            exec("INSERT INTO dq_line_items (inv_id, qty) VALUES (1, 3), (2, 1), (99, 5)");

            long violations = scalarLong(
                    "SELECT COUNT(*) FROM dq_line_items li " +
                    "WHERE NOT EXISTS (SELECT 1 FROM dq_invoice i WHERE i.id = li.inv_id)");
            assertEquals(1, violations, "One FK violation should be detected");
        } finally {
            drop("dq_line_items", "dq_invoice");
        }
    }

    // =========================================================================
    // 4. Valid state transitions: CHECK constraint on status column
    // =========================================================================

    @Test
    @DisplayName("4. CHECK constraint rejects invalid status values and accepts valid ones")
    void validStateTransitions() throws Exception {
        drop("dq_tickets");
        try {
            exec("CREATE TABLE dq_tickets (" +
                 "  id SERIAL PRIMARY KEY," +
                 "  status TEXT CHECK (status IN ('open','in_progress','closed'))" +
                 ")");
            exec("INSERT INTO dq_tickets (status) VALUES ('open')");
            exec("INSERT INTO dq_tickets (status) VALUES ('in_progress')");
            exec("INSERT INTO dq_tickets (status) VALUES ('closed')");

            long valid = scalarLong("SELECT COUNT(*) FROM dq_tickets");
            assertEquals(3, valid, "Three valid status rows must be inserted");

            assertThrows(SQLException.class,
                    () -> exec("INSERT INTO dq_tickets (status) VALUES ('deleted')"),
                    "CHECK constraint must reject invalid status 'deleted'");
        } finally {
            drop("dq_tickets");
        }
    }

    // =========================================================================
    // 5. Effective-date overlap detection using a self-join
    // =========================================================================

    @Test
    @DisplayName("5. Self-join detects overlapping effective-date ranges for the same entity")
    void effectiveDateOverlapDetection() throws Exception {
        drop("dq_contracts");
        try {
            exec("CREATE TABLE dq_contracts (" +
                 "  id        SERIAL PRIMARY KEY," +
                 "  entity_id INT," +
                 "  eff_from  DATE," +
                 "  eff_to    DATE" +
                 ")");

            // Non-overlapping ranges for entity 1.
            exec("INSERT INTO dq_contracts (entity_id, eff_from, eff_to) VALUES " +
                 "(1, '2024-01-01', '2024-06-30'), " +
                 "(1, '2024-07-01', '2024-12-31')");

            // Overlapping ranges for entity 2.
            exec("INSERT INTO dq_contracts (entity_id, eff_from, eff_to) VALUES " +
                 "(2, '2024-01-01', '2024-09-30'), " +
                 "(2, '2024-06-01', '2024-12-31')");

            // Detect overlaps: two rows for the same entity whose ranges intersect.
            long overlaps = scalarLong(
                    "SELECT COUNT(*) FROM (" +
                    "  SELECT a.entity_id FROM dq_contracts a " +
                    "  JOIN dq_contracts b ON a.entity_id = b.entity_id AND a.id < b.id " +
                    "  WHERE a.eff_from <= b.eff_to AND a.eff_to >= b.eff_from" +
                    ") x");
            assertEquals(1, overlaps, "One overlap pair should be found (entity 2)");
        } finally {
            drop("dq_contracts");
        }
    }

    // =========================================================================
    // 6. Denormalized count freshness
    // =========================================================================

    @Test
    @DisplayName("6. Stored counter column is stale when it differs from actual COUNT(*) in detail table")
    void denormalizedCountFreshness() throws Exception {
        drop("dq_order_lines", "dq_order_header");
        try {
            exec("CREATE TABLE dq_order_header (id INT PRIMARY KEY, line_count INT)");
            exec("CREATE TABLE dq_order_lines  (id SERIAL, order_id INT, sku TEXT)");

            exec("INSERT INTO dq_order_header VALUES (1, 3)");  // claims 3 lines
            exec("INSERT INTO dq_order_lines (order_id, sku) VALUES (1,'A'), (1,'B')"); // only 2

            long stale = scalarLong(
                    "SELECT COUNT(*) FROM dq_order_header h " +
                    "WHERE h.line_count != (SELECT COUNT(*) FROM dq_order_lines l WHERE l.order_id = h.id)");
            assertEquals(1, stale, "One stale counter must be detected");
        } finally {
            drop("dq_order_lines", "dq_order_header");
        }
    }

    // =========================================================================
    // 7. Materialized view staleness
    // =========================================================================

    @Test
    @DisplayName("7. Materialized view row count diverges from source after source inserts until refreshed")
    void materializedViewStaleness() throws Exception {
        drop("dq_sales");
        dropViews("dq_sales_mv");
        try {
            exec("CREATE TABLE dq_sales (id SERIAL PRIMARY KEY, amount NUMERIC)");
            exec("INSERT INTO dq_sales (amount) VALUES (10), (20), (30)");
            exec("CREATE MATERIALIZED VIEW dq_sales_mv AS SELECT * FROM dq_sales");

            // MV and source are in sync.
            long mvCount  = scalarLong("SELECT COUNT(*) FROM dq_sales_mv");
            long srcCount = scalarLong("SELECT COUNT(*) FROM dq_sales");
            assertEquals(srcCount, mvCount, "MV count must match source before any new inserts");

            // Insert into source without refreshing the MV.
            exec("INSERT INTO dq_sales (amount) VALUES (40)");

            long mvAfter  = scalarLong("SELECT COUNT(*) FROM dq_sales_mv");
            long srcAfter = scalarLong("SELECT COUNT(*) FROM dq_sales");
            assertTrue(srcAfter > mvAfter, "Source count must exceed stale MV count");

            // After refresh they should be equal again.
            exec("REFRESH MATERIALIZED VIEW dq_sales_mv");
            long mvRefreshed = scalarLong("SELECT COUNT(*) FROM dq_sales_mv");
            assertEquals(srcAfter, mvRefreshed, "MV count must match source after REFRESH");
        } finally {
            dropViews("dq_sales_mv");
            drop("dq_sales");
        }
    }

    // =========================================================================
    // 8. NULL percentage monitoring
    // =========================================================================

    @Test
    @DisplayName("8. NULL percentage query correctly identifies columns with excessive NULLs")
    void nullPercentageMonitoring() throws Exception {
        drop("dq_profiles");
        try {
            exec("CREATE TABLE dq_profiles (id SERIAL, email TEXT, phone TEXT)");
            exec("INSERT INTO dq_profiles (email, phone) VALUES " +
                 "('a@x.com', '111'), " +
                 "('b@x.com', NULL), " +
                 "(NULL,      NULL), " +
                 "('d@x.com', NULL)");

            // phone is NULL in 3 of 4 rows → 75 %.
            long nullPct = scalarLong(
                    "SELECT ROUND(100.0 * SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) / COUNT(*)) " +
                    "FROM dq_profiles");
            assertEquals(75, nullPct, "phone NULL percentage should be 75%");

            // email is NULL in 1 of 4 rows → 25 %.
            long emailNullPct = scalarLong(
                    "SELECT ROUND(100.0 * SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) / COUNT(*)) " +
                    "FROM dq_profiles");
            assertEquals(25, emailNullPct, "email NULL percentage should be 25%");
        } finally {
            drop("dq_profiles");
        }
    }

    // =========================================================================
    // 9. Range constraint validation
    // =========================================================================

    @Test
    @DisplayName("9. Query identifies values outside the expected business range")
    void rangeConstraintValidation() throws Exception {
        drop("dq_ratings");
        try {
            exec("CREATE TABLE dq_ratings (id SERIAL, score NUMERIC)");
            exec("INSERT INTO dq_ratings (score) VALUES (1), (5), (10), (-1), (11)");

            // Valid range for score: 1-10.
            long outOfRange = scalarLong(
                    "SELECT COUNT(*) FROM dq_ratings WHERE score < 1 OR score > 10");
            assertEquals(2, outOfRange, "Two out-of-range scores should be detected (-1 and 11)");
        } finally {
            drop("dq_ratings");
        }
    }

    // =========================================================================
    // 10. Referential consistency after bulk update
    // =========================================================================

    @Test
    @DisplayName("10. Children remain valid after a bulk update on the parent PK when FK ON UPDATE CASCADE is set")
    void referentialConsistencyAfterBulkUpdate() throws Exception {
        drop("dq_items", "dq_categories");
        try {
            exec("CREATE TABLE dq_categories (id INT PRIMARY KEY, label TEXT)");
            exec("CREATE TABLE dq_items (id SERIAL, cat_id INT " +
                 "  REFERENCES dq_categories(id) ON UPDATE CASCADE ON DELETE CASCADE," +
                 "  name TEXT)");

            exec("INSERT INTO dq_categories VALUES (1,'Electronics'), (2,'Books')");
            exec("INSERT INTO dq_items (cat_id, name) VALUES (1,'Laptop'), (2,'Novel'), (1,'Phone')");

            // Bulk update parent PK; children must follow via CASCADE.
            exec("UPDATE dq_categories SET id = id + 100");

            long orphans = scalarLong(
                    "SELECT COUNT(*) FROM dq_items i " +
                    "WHERE NOT EXISTS (SELECT 1 FROM dq_categories c WHERE c.id = i.cat_id)");
            assertEquals(0, orphans, "No orphaned items should remain after cascaded parent update");
        } finally {
            drop("dq_items", "dq_categories");
        }
    }

    // =========================================================================
    // 11. Unique invariant across (simulated) partitions
    // =========================================================================

    @Test
    @DisplayName("11. UNION ALL across partition-like tables reveals cross-partition duplicate keys")
    void uniqueInvariantAcrossPartitions() throws Exception {
        drop("dq_part_q2", "dq_part_q1");
        try {
            exec("CREATE TABLE dq_part_q1 (id INT, val TEXT)");
            exec("CREATE TABLE dq_part_q2 (id INT, val TEXT)");

            exec("INSERT INTO dq_part_q1 VALUES (1,'a'), (2,'b')");
            exec("INSERT INTO dq_part_q2 VALUES (2,'c'), (3,'d')"); // id=2 duplicated across partitions

            long crossDupes = scalarLong(
                    "SELECT COUNT(*) FROM (" +
                    "  SELECT id FROM (" +
                    "    SELECT id FROM dq_part_q1 UNION ALL SELECT id FROM dq_part_q2" +
                    "  ) all_parts GROUP BY id HAVING COUNT(*) > 1" +
                    ") x");
            assertEquals(1, crossDupes, "id=2 must appear as a cross-partition duplicate");
        } finally {
            drop("dq_part_q2", "dq_part_q1");
        }
    }

    // =========================================================================
    // 12. Business rule: no future dates
    // =========================================================================

    @Test
    @DisplayName("12. No created_at timestamps should be in the future")
    void noFutureDates() throws Exception {
        drop("dq_events");
        try {
            exec("CREATE TABLE dq_events (id SERIAL, name TEXT, created_at TIMESTAMPTZ DEFAULT NOW())");
            exec("INSERT INTO dq_events (name) VALUES ('past_event')");
            exec("INSERT INTO dq_events (name, created_at) VALUES ('future_event', NOW() + INTERVAL '1 year')");

            long futureRows = scalarLong(
                    "SELECT COUNT(*) FROM dq_events WHERE created_at > NOW()");
            assertEquals(1, futureRows, "Exactly one future-dated row should be detected");
        } finally {
            drop("dq_events");
        }
    }

    // =========================================================================
    // 13. Monetary precision: no more than 2 decimal places
    // =========================================================================

    @Test
    @DisplayName("13. Monetary values with more than 2 decimal places are identified as precision violations")
    void monetaryPrecision() throws Exception {
        drop("dq_payments");
        try {
            exec("CREATE TABLE dq_payments (id SERIAL, amount NUMERIC(12,4))");
            exec("INSERT INTO dq_payments (amount) VALUES " +
                 "(10.00), (5.50), (3.141), (99.999)");

            // A value has excess precision if (amount * 100) is not an integer.
            long violations = scalarLong(
                    "SELECT COUNT(*) FROM dq_payments " +
                    "WHERE amount <> ROUND(amount, 2)");
            assertEquals(2, violations, "Two rows have more than 2 decimal places");
        } finally {
            drop("dq_payments");
        }
    }

    // =========================================================================
    // 14. Email format validation
    // =========================================================================

    @Test
    @DisplayName("14. SIMILAR TO / ~ regex check detects malformed email addresses")
    void emailFormatValidation() throws Exception {
        drop("dq_users_email");
        try {
            exec("CREATE TABLE dq_users_email (id SERIAL, email TEXT)");
            exec("INSERT INTO dq_users_email (email) VALUES " +
                 "('alice@example.com'), " +
                 "('bob@domain.org'), " +
                 "('not-an-email'), " +
                 "('@missinglocal.com'), " +
                 "('no-at-sign')");

            // Basic regex: must contain exactly one '@' with non-empty local and domain parts.
            long invalid = scalarLong(
                    "SELECT COUNT(*) FROM dq_users_email " +
                    "WHERE email !~ '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'");
            assertEquals(3, invalid, "Three invalid email addresses should be flagged");
        } finally {
            drop("dq_users_email");
        }
    }

    // =========================================================================
    // 15. Audit trail completeness
    // =========================================================================

    @Test
    @DisplayName("15. Every entity has at least one audit row; entities without an audit row are flagged")
    void auditTrailCompleteness() throws Exception {
        drop("dq_audit_log", "dq_entities");
        try {
            exec("CREATE TABLE dq_entities  (id INT PRIMARY KEY, name TEXT)");
            exec("CREATE TABLE dq_audit_log (id SERIAL, entity_id INT, action TEXT)");

            exec("INSERT INTO dq_entities VALUES (1,'Alpha'), (2,'Beta'), (3,'Gamma')");
            exec("INSERT INTO dq_audit_log (entity_id, action) VALUES (1,'CREATE'), (2,'UPDATE')");
            // Entity 3 has no audit row.

            long unaudited = scalarLong(
                    "SELECT COUNT(*) FROM dq_entities e " +
                    "WHERE NOT EXISTS (SELECT 1 FROM dq_audit_log a WHERE a.entity_id = e.id)");
            assertEquals(1, unaudited, "Exactly one entity must be missing from the audit log");
        } finally {
            drop("dq_audit_log", "dq_entities");
        }
    }

    // =========================================================================
    // 16. Soft-delete consistency
    // =========================================================================

    @Test
    @DisplayName("16. Rows with is_deleted=true must have a non-NULL deleted_at timestamp")
    void softDeleteConsistency() throws Exception {
        drop("dq_records");
        try {
            exec("CREATE TABLE dq_records (" +
                 "  id         SERIAL PRIMARY KEY," +
                 "  name       TEXT," +
                 "  is_deleted BOOLEAN DEFAULT FALSE," +
                 "  deleted_at TIMESTAMPTZ" +
                 ")");

            exec("INSERT INTO dq_records (name, is_deleted, deleted_at) VALUES " +
                 "('live',             false, NULL), " +
                 "('properly_deleted', true,  NOW()), " +
                 "('bad_deleted',      true,  NULL)");  // inconsistent: flagged deleted but no timestamp

            long inconsistent = scalarLong(
                    "SELECT COUNT(*) FROM dq_records WHERE is_deleted = TRUE AND deleted_at IS NULL");
            assertEquals(1, inconsistent, "One soft-delete inconsistency (missing deleted_at) should be found");

            // Also verify that live rows do not carry a deleted_at timestamp.
            long livePolluted = scalarLong(
                    "SELECT COUNT(*) FROM dq_records WHERE is_deleted = FALSE AND deleted_at IS NOT NULL");
            assertEquals(0, livePolluted, "No live row should have a deleted_at timestamp");
        } finally {
            drop("dq_records");
        }
    }

    // =========================================================================
    // 17. Cross-table totals: header sum must equal sum of detail lines
    // =========================================================================

    @Test
    @DisplayName("17. Header total must equal the sum of its detail lines; mismatches are flagged")
    void crossTableTotals() throws Exception {
        drop("dq_detail_lines", "dq_invoice_hdr");
        try {
            exec("CREATE TABLE dq_invoice_hdr  (id INT PRIMARY KEY, total NUMERIC)");
            exec("CREATE TABLE dq_detail_lines (id SERIAL, inv_id INT, amount NUMERIC)");

            // Invoice 1: header total matches detail sum.
            exec("INSERT INTO dq_invoice_hdr  VALUES (1, 300)");
            exec("INSERT INTO dq_detail_lines (inv_id, amount) VALUES (1, 100), (1, 200)");

            // Invoice 2: header total does NOT match detail sum (mismatch).
            exec("INSERT INTO dq_invoice_hdr  VALUES (2, 500)");
            exec("INSERT INTO dq_detail_lines (inv_id, amount) VALUES (2, 200), (2, 250)"); // sum=450 ≠ 500

            long mismatches = scalarLong(
                    "SELECT COUNT(*) FROM dq_invoice_hdr h " +
                    "WHERE h.total != (" +
                    "  SELECT COALESCE(SUM(d.amount), 0) " +
                    "  FROM dq_detail_lines d WHERE d.inv_id = h.id" +
                    ")");
            assertEquals(1, mismatches, "Exactly one header/detail total mismatch should be found");
        } finally {
            drop("dq_detail_lines", "dq_invoice_hdr");
        }
    }

    // =========================================================================
    // 18. Bonus: Composite uniqueness, same (entity, period) should not repeat
    // =========================================================================

    @Test
    @DisplayName("18. Composite uniqueness check catches rows that violate a business-level unique constraint")
    void compositeUniquenessCheck() throws Exception {
        drop("dq_budget");
        try {
            exec("CREATE TABLE dq_budget (id SERIAL, dept_id INT, year INT, amount NUMERIC)");
            exec("INSERT INTO dq_budget (dept_id, year, amount) VALUES " +
                 "(10, 2024, 5000), " +
                 "(10, 2025, 6000), " +
                 "(20, 2024, 3000), " +
                 "(10, 2024, 4500)");  // duplicate (dept=10, year=2024)

            long compositeDupes = scalarLong(
                    "SELECT COUNT(*) FROM (" +
                    "  SELECT dept_id, year FROM dq_budget " +
                    "  GROUP BY dept_id, year HAVING COUNT(*) > 1" +
                    ") x");
            assertEquals(1, compositeDupes, "One composite duplicate (dept_id=10, year=2024) should be found");
        } finally {
            drop("dq_budget");
        }
    }

    // =========================================================================
    // 19. Bonus: Window-function based deduplication ranking
    // =========================================================================

    @Test
    @DisplayName("19. ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) identifies the newest row per group")
    void windowFunctionDeduplication() throws Exception {
        drop("dq_snapshots");
        try {
            exec("CREATE TABLE dq_snapshots (id SERIAL, entity_id INT, captured_at TIMESTAMPTZ, state TEXT)");
            exec("INSERT INTO dq_snapshots (entity_id, captured_at, state) VALUES " +
                 "(1, '2024-01-01', 'v1'), " +
                 "(1, '2024-06-01', 'v2'), " +
                 "(1, '2024-12-01', 'v3'), " +
                 "(2, '2024-03-01', 'v1'), " +
                 "(2, '2024-09-01', 'v2')");

            // Latest snapshot per entity (rn = 1).
            long latestCount = scalarLong(
                    "SELECT COUNT(*) FROM (" +
                    "  SELECT entity_id, state, " +
                    "         ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY captured_at DESC) AS rn " +
                    "  FROM dq_snapshots" +
                    ") ranked WHERE rn = 1");
            assertEquals(2, latestCount, "Two entities should each have exactly one latest snapshot");

            // Confirm the states of the latest snapshots.
            long correctState = scalarLong(
                    "SELECT COUNT(*) FROM (" +
                    "  SELECT entity_id, state, " +
                    "         ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY captured_at DESC) AS rn " +
                    "  FROM dq_snapshots" +
                    ") ranked WHERE rn = 1 AND state IN ('v3','v2')");
            assertEquals(2, correctState, "Latest snapshots must be v3 for entity 1 and v2 for entity 2");
        } finally {
            drop("dq_snapshots");
        }
    }

    // =========================================================================
    // 20. Bonus: Negative numeric invariant, balances must not go negative
    // =========================================================================

    @Test
    @DisplayName("20. CHECK constraint and invariant query together enforce non-negative account balances")
    void negativeBalanceInvariant() throws Exception {
        drop("dq_accounts");
        try {
            exec("CREATE TABLE dq_accounts (" +
                 "  id      SERIAL PRIMARY KEY," +
                 "  holder  TEXT," +
                 "  balance NUMERIC CHECK (balance >= 0)" +
                 ")");
            exec("INSERT INTO dq_accounts (holder, balance) VALUES ('Alice', 100), ('Bob', 0)");

            // Valid inserts must work.
            long valid = scalarLong("SELECT COUNT(*) FROM dq_accounts");
            assertEquals(2, valid);

            // CHECK constraint must reject a negative balance.
            assertThrows(SQLException.class,
                    () -> exec("INSERT INTO dq_accounts (holder, balance) VALUES ('Eve', -50)"),
                    "CHECK constraint must prevent negative balance");

            // Post-insert invariant: no negative balances should exist.
            long negative = scalarLong("SELECT COUNT(*) FROM dq_accounts WHERE balance < 0");
            assertEquals(0, negative, "No account should have a negative balance");
        } finally {
            drop("dq_accounts");
        }
    }
}
