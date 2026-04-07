package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers safe online schema migration patterns (1310_online_migration_expand_contract_scenarios).
 *
 * Tests expand-contract techniques: nullable column addition, chunked backfills,
 * dual-write, NOT NULL promotion, column rename, default migration, index creation,
 * constraint validation, enum expansion, table swap, view-based cutover,
 * foreign key addition, and data-type widening.
 *
 * Each test is self-contained and uses the table prefix om_.
 * Connection autocommit is true.
 */
class OnlineMigrationExpandContractTest {

    static Memgres memgres;
    static String url;

    @BeforeAll
    static void setup() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        url = "jdbc:postgresql://localhost:" + memgres.getPort() + "/test";
    }

    @AfterAll
    static void teardown() throws Exception {
        if (memgres != null) memgres.close();
    }

    private Connection conn() throws SQLException {
        Connection c = DriverManager.getConnection(url, "test", "test");
        c.setAutoCommit(true);
        return c;
    }

    // =========================================================================
    // 1. Nullable column addition: existing rows unaffected, new column is NULL
    // =========================================================================

    @Test
    void testNullableColumnAddition() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_nullable");
            st.execute("CREATE TABLE om_nullable (id SERIAL PRIMARY KEY, name TEXT NOT NULL)");
            st.execute("INSERT INTO om_nullable (name) VALUES ('alice'), ('bob')");

            // Expand: add nullable column
            st.execute("ALTER TABLE om_nullable ADD COLUMN email TEXT");

            // Existing rows must have NULL for the new column
            ResultSet rs = st.executeQuery("SELECT id, name, email FROM om_nullable ORDER BY id");
            assertTrue(rs.next()); assertEquals("alice", rs.getString("name")); assertNull(rs.getString("email"));
            assertTrue(rs.next()); assertEquals("bob",   rs.getString("name")); assertNull(rs.getString("email"));
            assertFalse(rs.next());

            // New inserts can populate both columns
            st.execute("INSERT INTO om_nullable (name, email) VALUES ('carol', 'carol@example.com')");
            ResultSet rs2 = st.executeQuery("SELECT email FROM om_nullable WHERE name = 'carol'");
            assertTrue(rs2.next());
            assertEquals("carol@example.com", rs2.getString(1));
        }
    }

    // =========================================================================
    // 2. Chunked backfill using CTEs with LIMIT and RETURNING
    // =========================================================================

    @Test
    void testChunkedBackfill() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_backfill");
            st.execute("CREATE TABLE om_backfill (id SERIAL PRIMARY KEY, value INT, computed INT)");
            for (int i = 1; i <= 20; i++) {
                st.execute("INSERT INTO om_backfill (value) VALUES (" + i + ")");
            }

            // Backfill in chunks of 5 using a CTE
            int totalUpdated = 0;
            int batchSize = 5;
            int offset = 0;
            while (true) {
                ResultSet rs = st.executeQuery(
                        "WITH batch AS (" +
                        "  SELECT id FROM om_backfill WHERE computed IS NULL ORDER BY id LIMIT " + batchSize +
                        ") " +
                        "UPDATE om_backfill SET computed = value * 2 " +
                        "WHERE id IN (SELECT id FROM batch) " +
                        "RETURNING id");
                int count = 0;
                while (rs.next()) count++;
                totalUpdated += count;
                if (count == 0) break;
                offset += count;
            }

            assertEquals(20, totalUpdated, "All 20 rows must be backfilled");

            // Verify correctness
            ResultSet verify = st.executeQuery("SELECT COUNT(*) FROM om_backfill WHERE computed != value * 2");
            assertTrue(verify.next());
            assertEquals(0, verify.getInt(1), "All computed values must equal value * 2");
        }
    }

    // =========================================================================
    // 3. Dual-write validation: INSERT writes to both old and new columns
    // =========================================================================

    @Test
    void testDualWriteValidation() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_dualwrite");
            st.execute("""
                    CREATE TABLE om_dualwrite (
                        id SERIAL PRIMARY KEY,
                        username TEXT,
                        username_new TEXT
                    )""");

            // Dual-write: populate both columns on insert
            st.execute("INSERT INTO om_dualwrite (username, username_new) VALUES ('alice', 'alice')");
            st.execute("INSERT INTO om_dualwrite (username, username_new) VALUES ('bob', 'bob')");

            // Verify they match
            ResultSet rs = st.executeQuery(
                    "SELECT COUNT(*) FROM om_dualwrite WHERE username != username_new OR username_new IS NULL");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "Old and new columns must be consistent after dual-write");
        }
    }

    // =========================================================================
    // 4. Read from new column with COALESCE fallback
    // =========================================================================

    @Test
    void testReadWithCoalesceFallback() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_coalesce");
            st.execute("CREATE TABLE om_coalesce (id SERIAL PRIMARY KEY, old_name TEXT, new_name TEXT)");
            // Row 1: only old_name populated (pre-migration)
            st.execute("INSERT INTO om_coalesce (old_name) VALUES ('legacy-value')");
            // Row 2: both populated (dual-write period)
            st.execute("INSERT INTO om_coalesce (old_name, new_name) VALUES ('old-value', 'new-value')");
            // Row 3: only new_name populated (post-migration)
            st.execute("INSERT INTO om_coalesce (new_name) VALUES ('newest-value')");

            ResultSet rs = st.executeQuery(
                    "SELECT id, COALESCE(new_name, old_name) AS effective_name FROM om_coalesce ORDER BY id");

            assertTrue(rs.next()); assertEquals("legacy-value", rs.getString("effective_name"));
            assertTrue(rs.next()); assertEquals("new-value",    rs.getString("effective_name"));
            assertTrue(rs.next()); assertEquals("newest-value", rs.getString("effective_name"));
        }
    }

    // =========================================================================
    // 5. Column rename pattern: add, backfill, verify, drop old
    // =========================================================================

    @Test
    void testColumnRenamePattern() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_rename");
            st.execute("CREATE TABLE om_rename (id SERIAL PRIMARY KEY, fname TEXT)");
            st.execute("INSERT INTO om_rename (fname) VALUES ('alice'), ('bob')");

            // Expand: add new column
            st.execute("ALTER TABLE om_rename ADD COLUMN first_name TEXT");
            // Backfill
            st.execute("UPDATE om_rename SET first_name = fname WHERE first_name IS NULL");
            // Verify consistency
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM om_rename WHERE first_name IS DISTINCT FROM fname");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "first_name and fname must match after backfill");

            // Contract: drop old column
            st.execute("ALTER TABLE om_rename DROP COLUMN fname");

            // Verify old column gone, new column present
            ResultSet rs2 = st.executeQuery("SELECT first_name FROM om_rename ORDER BY id");
            assertTrue(rs2.next()); assertEquals("alice", rs2.getString(1));
            assertTrue(rs2.next()); assertEquals("bob",   rs2.getString(1));

            // Accessing dropped column must fail
            assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT fname FROM om_rename"),
                    "Dropped column fname must no longer be accessible");
        }
    }

    // =========================================================================
    // 6. NOT NULL constraint addition: backfill first, then enforce
    // =========================================================================

    @Test
    void testNotNullConstraintAddition() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_notnull");
            st.execute("CREATE TABLE om_notnull (id SERIAL PRIMARY KEY, label TEXT)");
            st.execute("INSERT INTO om_notnull (label) VALUES ('a'), (NULL), ('c')");

            // Step 1: backfill NULLs
            st.execute("UPDATE om_notnull SET label = 'default' WHERE label IS NULL");

            // Step 2: add NOT NULL constraint, which should succeed since no NULLs remain
            assertDoesNotThrow(() -> st.execute("ALTER TABLE om_notnull ALTER COLUMN label SET NOT NULL"));

            // Verify constraint is enforced on new insert
            assertThrows(SQLException.class,
                    () -> st.execute("INSERT INTO om_notnull (label) VALUES (NULL)"),
                    "NOT NULL constraint must reject NULL after being added");
        }
    }

    // =========================================================================
    // 7. Default value migration: set default then backfill existing rows
    // =========================================================================

    @Test
    void testDefaultValueMigration() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_default");
            st.execute("CREATE TABLE om_default (id SERIAL PRIMARY KEY, status TEXT)");
            st.execute("INSERT INTO om_default (status) VALUES (NULL), (NULL)");

            // Step 1: set a default for new rows
            st.execute("ALTER TABLE om_default ALTER COLUMN status SET DEFAULT 'pending'");

            // New row should pick up default
            st.execute("INSERT INTO om_default DEFAULT VALUES");
            ResultSet newRow = st.executeQuery("SELECT status FROM om_default WHERE id = 3");
            assertTrue(newRow.next()); assertEquals("pending", newRow.getString(1));

            // Step 2: backfill existing NULLs
            st.execute("UPDATE om_default SET status = 'pending' WHERE status IS NULL");

            ResultSet all = st.executeQuery("SELECT COUNT(*) FROM om_default WHERE status IS NULL");
            assertTrue(all.next()); assertEquals(0, all.getInt(1), "No NULLs should remain after backfill");
        }
    }

    // =========================================================================
    // 8. Index creation on new column (CREATE INDEX, no CONCURRENTLY)
    // =========================================================================

    @Test
    void testIndexCreationOnNewColumn() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_index");
            st.execute("CREATE TABLE om_index (id SERIAL PRIMARY KEY, tag TEXT)");
            for (int i = 0; i < 100; i++) {
                st.execute("INSERT INTO om_index (tag) VALUES ('tag-" + (i % 10) + "')");
            }

            // Create index (Memgres does not support CONCURRENTLY; plain CREATE INDEX)
            st.execute("CREATE INDEX om_index_tag_idx ON om_index (tag)");

            // Verify index exists in pg_indexes
            ResultSet rs = st.executeQuery(
                    "SELECT indexname FROM pg_indexes WHERE tablename = 'om_index' AND indexname = 'om_index_tag_idx'");
            assertTrue(rs.next(), "Index om_index_tag_idx should exist after creation");

            // Queries using the indexed column must still work correctly
            ResultSet count = st.executeQuery("SELECT COUNT(*) FROM om_index WHERE tag = 'tag-0'");
            assertTrue(count.next()); assertEquals(10, count.getInt(1));
        }
    }

    // =========================================================================
    // 9. Constraint addition: NOT VALID + VALIDATE CONSTRAINT
    // =========================================================================

    @Test
    void testConstraintAdditionWithValidation() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_constraint");
            st.execute("CREATE TABLE om_constraint (id SERIAL PRIMARY KEY, age INT)");
            // Insert some rows that all satisfy the constraint
            st.execute("INSERT INTO om_constraint (age) VALUES (25), (30), (18)");

            // Add constraint NOT VALID (does not scan existing rows)
            st.execute("ALTER TABLE om_constraint ADD CONSTRAINT chk_age CHECK (age >= 0) NOT VALID");

            // Validate the constraint (scans existing rows)
            assertDoesNotThrow(() -> st.execute("ALTER TABLE om_constraint VALIDATE CONSTRAINT chk_age"));

            // Constraint must be enforced on new inserts
            assertThrows(SQLException.class,
                    () -> st.execute("INSERT INTO om_constraint (age) VALUES (-1)"),
                    "Negative age must violate CHECK constraint");
        }
    }

    // =========================================================================
    // 10. Rollback at each migration stage
    // =========================================================================

    @Test
    void testRollbackAtEachMigrationStage() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_rollback");
            st.execute("CREATE TABLE om_rollback (id SERIAL PRIMARY KEY, value TEXT)");
            st.execute("INSERT INTO om_rollback (value) VALUES ('original')");
        }

        // Stage 1: add column, then rollback via DROP COLUMN
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("ALTER TABLE om_rollback ADD COLUMN new_value TEXT");
            // Simulate rollback by contracting
            st.execute("ALTER TABLE om_rollback DROP COLUMN new_value");

            // Verify column is gone
            assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT new_value FROM om_rollback"),
                    "Rolled-back column must not be accessible");
        }

        // Stage 2: add column + partial backfill, then rollback
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("ALTER TABLE om_rollback ADD COLUMN partial TEXT");
            st.execute("UPDATE om_rollback SET partial = 'filled' WHERE id = 1");

            // Contract (rollback)
            st.execute("ALTER TABLE om_rollback DROP COLUMN partial");
        }

        // Stage 3: full backfill; original data must still be intact
        try (Connection c = conn(); Statement st = c.createStatement()) {
            ResultSet rs = st.executeQuery("SELECT value FROM om_rollback WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("original", rs.getString(1), "Original data must be intact after all rollback stages");
        }
    }

    // =========================================================================
    // 11. Enum type expansion: ALTER TYPE ADD VALUE
    // =========================================================================

    @Test
    void testEnumTypeExpansion() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_enum");
            st.execute("DROP TYPE IF EXISTS om_status_enum");
            st.execute("CREATE TYPE om_status_enum AS ENUM ('pending', 'active', 'completed')");
            st.execute("CREATE TABLE om_enum (id SERIAL PRIMARY KEY, status om_status_enum NOT NULL DEFAULT 'pending')");
            st.execute("INSERT INTO om_enum DEFAULT VALUES");

            // Expand enum with a new value
            st.execute("ALTER TYPE om_status_enum ADD VALUE 'cancelled'");

            // New value should be usable immediately
            st.execute("INSERT INTO om_enum (status) VALUES ('cancelled')");
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM om_enum WHERE status = 'cancelled'");
            assertTrue(rs.next()); assertEquals(1, rs.getInt(1));

            // Existing rows must be unaffected
            ResultSet pending = st.executeQuery("SELECT COUNT(*) FROM om_enum WHERE status = 'pending'");
            assertTrue(pending.next()); assertEquals(1, pending.getInt(1));
        }
    }

    // =========================================================================
    // 12. Table swap pattern: populate new table, rename old, rename new
    // =========================================================================

    @Test
    void testTableSwapPattern() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_swap_old");
            st.execute("DROP TABLE IF EXISTS om_swap_new");
            st.execute("DROP TABLE IF EXISTS om_swap_archived");

            // Create and populate old table
            st.execute("CREATE TABLE om_swap_old (id SERIAL PRIMARY KEY, data TEXT)");
            st.execute("INSERT INTO om_swap_old (data) VALUES ('row1'), ('row2')");

            // Create new table with updated schema (e.g. added column)
            st.execute("CREATE TABLE om_swap_new (id SERIAL PRIMARY KEY, data TEXT, extra TEXT DEFAULT 'default')");
            // Populate from old
            st.execute("INSERT INTO om_swap_new (id, data) SELECT id, data FROM om_swap_old");

            // Swap: archive old, promote new
            st.execute("ALTER TABLE om_swap_old RENAME TO om_swap_archived");
            st.execute("ALTER TABLE om_swap_new RENAME TO om_swap_old");

            // Verify new table has all original rows plus the new column
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM om_swap_old");
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));

            ResultSet extra = st.executeQuery("SELECT extra FROM om_swap_old LIMIT 1");
            assertTrue(extra.next()); assertEquals("default", extra.getString(1));

            // Archived table still has original data
            ResultSet archived = st.executeQuery("SELECT COUNT(*) FROM om_swap_archived");
            assertTrue(archived.next()); assertEquals(2, archived.getInt(1));
        }
    }

    // =========================================================================
    // 13. View-based cutover: swap view definition from old to new schema
    // =========================================================================

    @Test
    void testViewBasedCutover() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP VIEW  IF EXISTS om_view_current");
            st.execute("DROP TABLE IF EXISTS om_view_v2");
            st.execute("DROP TABLE IF EXISTS om_view_v1");

            // V1 schema
            st.execute("CREATE TABLE om_view_v1 (id SERIAL PRIMARY KEY, name TEXT)");
            st.execute("INSERT INTO om_view_v1 (name) VALUES ('v1-row')");

            // Create view pointing at V1
            st.execute("CREATE VIEW om_view_current AS SELECT id, name FROM om_view_v1");

            // Confirm view returns V1 data
            ResultSet rs1 = st.executeQuery("SELECT name FROM om_view_current");
            assertTrue(rs1.next()); assertEquals("v1-row", rs1.getString(1));

            // V2 schema (with additional column)
            st.execute("CREATE TABLE om_view_v2 (id SERIAL PRIMARY KEY, name TEXT, email TEXT)");
            st.execute("INSERT INTO om_view_v2 (name, email) VALUES ('v2-row', 'v2@example.com')");

            // Cutover: replace view definition to point at V2
            st.execute("CREATE OR REPLACE VIEW om_view_current AS SELECT id, name FROM om_view_v2");

            // View now returns V2 data
            ResultSet rs2 = st.executeQuery("SELECT name FROM om_view_current");
            assertTrue(rs2.next()); assertEquals("v2-row", rs2.getString(1));
            assertFalse(rs2.next(), "View should only expose V2 rows after cutover");
        }
    }

    // =========================================================================
    // 14. Foreign key addition: NOT VALID then VALIDATE CONSTRAINT
    // =========================================================================

    @Test
    void testForeignKeyAdditionNotValid() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_fk_child");
            st.execute("DROP TABLE IF EXISTS om_fk_parent");

            st.execute("CREATE TABLE om_fk_parent (id SERIAL PRIMARY KEY, label TEXT)");
            st.execute("INSERT INTO om_fk_parent (label) VALUES ('parent1')"); // id=1

            st.execute("CREATE TABLE om_fk_child (id SERIAL PRIMARY KEY, parent_id INT, note TEXT)");
            st.execute("INSERT INTO om_fk_child (parent_id, note) VALUES (1, 'valid child')");

            // Add FK NOT VALID (does not validate existing rows)
            st.execute("""
                    ALTER TABLE om_fk_child
                    ADD CONSTRAINT fk_child_parent
                    FOREIGN KEY (parent_id) REFERENCES om_fk_parent(id) NOT VALID""");

            // VALIDATE CONSTRAINT: all existing rows satisfy the FK
            assertDoesNotThrow(() -> st.execute("ALTER TABLE om_fk_child VALIDATE CONSTRAINT fk_child_parent"));

            // New insert with non-existent parent must fail
            assertThrows(SQLException.class,
                    () -> st.execute("INSERT INTO om_fk_child (parent_id, note) VALUES (999, 'orphan')"),
                    "FK constraint must reject orphaned child row");
        }
    }

    // =========================================================================
    // 15. Data type widening: INT to BIGINT
    // =========================================================================

    @Test
    void testDataTypeWideningIntToBigint() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_widen");
            st.execute("CREATE TABLE om_widen (id SERIAL PRIMARY KEY, counter INT)");
            st.execute("INSERT INTO om_widen (counter) VALUES (100), (200), (2147483647)"); // max int

            // Widen INT -> BIGINT (safe widening, no data loss)
            st.execute("ALTER TABLE om_widen ALTER COLUMN counter TYPE BIGINT");

            // Existing data must be preserved
            ResultSet rs = st.executeQuery("SELECT counter FROM om_widen WHERE id = 3");
            assertTrue(rs.next());
            assertEquals(2147483647L, rs.getLong(1), "Max int value must survive widening to bigint");

            // Can now store values beyond INT range
            st.execute("INSERT INTO om_widen (counter) VALUES (9999999999)");
            ResultSet big = st.executeQuery("SELECT counter FROM om_widen WHERE id = 4");
            assertTrue(big.next());
            assertEquals(9999999999L, big.getLong(1));
        }
    }

    // =========================================================================
    // 16. Partial backfill with predicate: only fill rows matching a condition
    // =========================================================================

    @Test
    void testPartialBackfillWithPredicate() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_partial");
            st.execute("CREATE TABLE om_partial (id SERIAL PRIMARY KEY, category TEXT, score INT, normalized FLOAT)");
            st.execute("INSERT INTO om_partial (category, score) VALUES ('A', 80), ('B', 60), ('A', 90), ('B', 70)");

            // Backfill only category 'A' rows with a normalized score (score / 100.0)
            st.execute("UPDATE om_partial SET normalized = score / 100.0 WHERE category = 'A' AND normalized IS NULL");

            ResultSet aRows = st.executeQuery("SELECT COUNT(*) FROM om_partial WHERE category = 'A' AND normalized IS NOT NULL");
            assertTrue(aRows.next()); assertEquals(2, aRows.getInt(1));

            ResultSet bRows = st.executeQuery("SELECT COUNT(*) FROM om_partial WHERE category = 'B' AND normalized IS NULL");
            assertTrue(bRows.next()); assertEquals(2, bRows.getInt(1), "Category B rows must still be NULL");
        }
    }

    // =========================================================================
    // 17. Multi-step migration with intermediate validation
    // =========================================================================

    @Test
    void testMultiStepMigrationWithIntermediateValidation() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_multistep");
            st.execute("CREATE TABLE om_multistep (id SERIAL PRIMARY KEY, raw_phone TEXT)");
            st.execute("INSERT INTO om_multistep (raw_phone) VALUES ('+1-555-0100'), ('+1-555-0200'), ('+1-555-0300')");

            // Step 1: add normalized column
            st.execute("ALTER TABLE om_multistep ADD COLUMN phone_e164 TEXT");

            // Step 2: backfill (strip dashes as a simplistic normalization)
            st.execute("UPDATE om_multistep SET phone_e164 = REPLACE(REPLACE(raw_phone, '-', ''), '+', '+')");

            // Intermediate validation: all rows backfilled
            ResultSet check = st.executeQuery("SELECT COUNT(*) FROM om_multistep WHERE phone_e164 IS NULL");
            assertTrue(check.next()); assertEquals(0, check.getInt(1));

            // Step 3: add NOT NULL
            st.execute("ALTER TABLE om_multistep ALTER COLUMN phone_e164 SET NOT NULL");

            // Step 4: add unique index
            st.execute("CREATE UNIQUE INDEX om_multistep_phone_idx ON om_multistep (phone_e164)");

            // Step 5: remove old column (contract)
            st.execute("ALTER TABLE om_multistep DROP COLUMN raw_phone");

            // Final state: phone_e164 is the only phone column, NOT NULL, unique
            assertThrows(SQLException.class,
                    () -> st.execute("INSERT INTO om_multistep (phone_e164) VALUES (NULL)"),
                    "phone_e164 must be NOT NULL");

            assertThrows(SQLException.class,
                    () -> st.execute("INSERT INTO om_multistep (phone_e164) VALUES ('+15550100')"),
                    "Duplicate phone_e164 must violate unique index");
        }
    }

    // =========================================================================
    // 18. Shadow column write-through: new column stays in sync via UPDATE triggers
    //     (Implemented without triggers: verify manual dual-write sync protocol)
    // =========================================================================

    @Test
    void testShadowColumnWriteThrough() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_shadow");
            st.execute("CREATE TABLE om_shadow (id SERIAL PRIMARY KEY, price_cents INT, price_dollars NUMERIC(10,2))");
            st.execute("INSERT INTO om_shadow (price_cents) VALUES (1999), (4999), (9900)");

            // Backfill shadow column
            st.execute("UPDATE om_shadow SET price_dollars = price_cents / 100.0 WHERE price_dollars IS NULL");

            // Dual-write update: change price_cents, keep price_dollars in sync
            st.execute("UPDATE om_shadow SET price_cents = 2500, price_dollars = 2500 / 100.0 WHERE id = 1");

            ResultSet rs = st.executeQuery("SELECT price_cents, price_dollars FROM om_shadow WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(2500,  rs.getInt("price_cents"));
            assertEquals(25.00, rs.getDouble("price_dollars"), 0.001);

            // Verify all rows are in sync
            ResultSet sync = st.executeQuery(
                    "SELECT COUNT(*) FROM om_shadow WHERE ABS(price_dollars - price_cents / 100.0) > 0.001");
            assertTrue(sync.next()); assertEquals(0, sync.getInt(1), "All shadow columns must stay in sync");
        }
    }

    // =========================================================================
    // 19. Zero-downtime column drop: mark deprecated, stop writing, then drop
    // =========================================================================

    @Test
    void testZeroDowntimeColumnDrop() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS om_coldrop");
            st.execute("""
                    CREATE TABLE om_coldrop (
                        id SERIAL PRIMARY KEY,
                        active_col TEXT,
                        deprecated_col TEXT
                    )""");
            st.execute("INSERT INTO om_coldrop (active_col, deprecated_col) VALUES ('keep', 'drop-me')");

            // Phase 1: stop writing to deprecated_col (reads still possible)
            ResultSet rs1 = st.executeQuery("SELECT deprecated_col FROM om_coldrop WHERE id = 1");
            assertTrue(rs1.next()); assertEquals("drop-me", rs1.getString(1));

            // Phase 2: null out deprecated data (optional data cleanup)
            st.execute("UPDATE om_coldrop SET deprecated_col = NULL");

            // Phase 3: drop column
            st.execute("ALTER TABLE om_coldrop DROP COLUMN deprecated_col");

            // Verify active column still has its data
            ResultSet rs2 = st.executeQuery("SELECT active_col FROM om_coldrop WHERE id = 1");
            assertTrue(rs2.next()); assertEquals("keep", rs2.getString(1));

            // Accessing dropped column must fail
            assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT deprecated_col FROM om_coldrop"),
                    "Dropped column must be inaccessible");
        }
    }

    // =========================================================================
    // 20. Rename table and update references atomically
    // =========================================================================

    @Test
    void testRenameTableAndUpdateReferences() throws Exception {
        try (Connection c = conn(); Statement st = c.createStatement()) {
            // Drop both possible names to ensure clean state across test re-runs
            st.execute("DROP TABLE IF EXISTS om_renamed_table");
            st.execute("DROP TABLE IF EXISTS om_original_table");

            // Use explicit IDs instead of SERIAL to avoid sequence conflicts on re-runs
            st.execute("CREATE TABLE om_original_table (id INT PRIMARY KEY, payload TEXT)");
            st.execute("INSERT INTO om_original_table (id, payload) VALUES (1, 'data1'), (2, 'data2')");

            // Rename the table
            st.execute("ALTER TABLE om_original_table RENAME TO om_renamed_table");

            // Original name must no longer be accessible
            assertThrows(SQLException.class,
                    () -> st.executeQuery("SELECT * FROM om_original_table"),
                    "Original table name must be inaccessible after rename");

            // New name must expose all data
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM om_renamed_table");
            assertTrue(rs.next()); assertEquals(2, rs.getInt(1));

            // New inserts work via new name
            st.execute("INSERT INTO om_renamed_table (id, payload) VALUES (3, 'data3')");
            ResultSet rs2 = st.executeQuery("SELECT COUNT(*) FROM om_renamed_table");
            assertTrue(rs2.next()); assertEquals(3, rs2.getInt(1));
        }
    }
}
