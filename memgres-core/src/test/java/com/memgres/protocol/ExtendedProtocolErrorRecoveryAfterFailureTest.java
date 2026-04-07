package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for connection state recovery after errors in the PG extended query protocol.
 *
 * When a query fails in the extended protocol (Parse→Bind→Execute→Sync),
 * the PG protocol spec requires the server to:
 *   1. Send ErrorResponse
 *   2. Discard all subsequent messages until Sync
 *   3. On Sync, send ReadyForQuery
 *
 * If the server doesn't skip messages after an error, subsequent queries on
 * the same connection can execute with corrupted state (wrong parameters,
 * wrong portal, or wrong prepared statement), causing data corruption like
 * null values appearing in columns that should have data.
 */
class ExtendedProtocolErrorRecoveryAfterFailureTest {

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

    // =========================================================================
    // 1. FK violation then INSERT on same connection must work correctly
    // =========================================================================

    @Test
    void testFkViolationThenInsertSameConnection() throws SQLException {
        exec("CREATE TABLE err_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL)");
        exec("CREATE TABLE err_child (id serial PRIMARY KEY, parent_id UUID NOT NULL REFERENCES err_parent(id), info TEXT)");

        // Insert a parent
        UUID parentId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "parent");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                parentId = (UUID) rs.getObject(1);
            }
        }

        // Attempt FK-violating insert (nonexistent parent), must fail
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_child (parent_id, info) VALUES (?, ?)")) {
            ps.setObject(1, UUID.randomUUID()); // nonexistent parent
            ps.setString(2, "bad_child");
            assertThrows(SQLException.class, ps::executeUpdate);
        }

        // Connection must still work; insert a valid child
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_child (parent_id, info) VALUES (?, ?) RETURNING id")) {
            ps.setObject(1, parentId);
            ps.setString(2, "good_child");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Valid INSERT must succeed after FK violation on same connection");
                assertNotNull(rs.getObject(1));
            }
        }

        // Verify the child was inserted correctly
        assertEquals("good_child", query1("SELECT info FROM err_child WHERE parent_id = '" + parentId + "'"));
    }

    // =========================================================================
    // 2. FK violation then SELECT on same connection: data must not be corrupt
    // =========================================================================

    @Test
    void testFkViolationThenSelectSameConnection() throws SQLException {
        exec("CREATE TABLE err_sel_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT)");
        exec("CREATE TABLE err_sel_child (id serial PRIMARY KEY, parent_id UUID REFERENCES err_sel_parent(id), data TEXT)");

        UUID parentId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_sel_parent (name) VALUES (?) RETURNING id")) {
            ps.setString(1, "test_parent");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); parentId = (UUID) rs.getObject(1); }
        }

        // Cause an FK violation
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_sel_child (parent_id, data) VALUES (?, ?)")) {
            ps.setObject(1, UUID.randomUUID());
            ps.setString(2, "orphan");
            assertThrows(SQLException.class, ps::executeUpdate);
        }

        // SELECT must still work and return correct data
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT name FROM err_sel_parent WHERE id = ?")) {
            ps.setObject(1, parentId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "SELECT must work after FK violation");
                assertEquals("test_parent", rs.getString(1),
                        "Data must not be corrupted after FK violation");
            }
        }
    }

    // =========================================================================
    // 3. Multiple errors in sequence; connection must recover each time
    // =========================================================================

    @Test
    void testMultipleErrorsInSequence() throws SQLException {
        exec("CREATE TABLE err_multi (id serial PRIMARY KEY, val TEXT NOT NULL UNIQUE)");
        exec("INSERT INTO err_multi (val) VALUES ('existing')");

        for (int i = 0; i < 10; i++) {
            // Cause a unique violation
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO err_multi (val) VALUES (?)")) {
                ps.setString(1, "existing"); // duplicate
                assertThrows(SQLException.class, ps::executeUpdate,
                        "Unique violation expected on iteration " + i);
            }

            // Immediately after: successful insert
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO err_multi (val) VALUES (?) RETURNING id")) {
                ps.setString(1, "new_" + i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "INSERT must succeed on iteration " + i);
                    int id = rs.getInt(1);
                    assertTrue(id > 0, "ID must be positive on iteration " + i);
                }
            }

            // Verify
            assertEquals("new_" + i, query1("SELECT val FROM err_multi WHERE val = 'new_" + i + "'"));
        }
    }

    // =========================================================================
    // 4. Error during INSERT RETURNING: verify connection usable after
    // =========================================================================

    @Test
    void testErrorDuringInsertReturning() throws SQLException {
        exec("CREATE TABLE err_ret (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), email TEXT NOT NULL UNIQUE, hash TEXT NOT NULL)");

        // Insert a row
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_ret (email, hash) VALUES (?, ?) RETURNING id")) {
            ps.setString(1, "first@test.com");
            ps.setString(2, "hash_first");
            try (ResultSet rs = ps.executeQuery()) { assertTrue(rs.next()); }
        }

        // Try to insert duplicate email, which fails
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_ret (email, hash) VALUES (?, ?) RETURNING id")) {
            ps.setString(1, "first@test.com"); // duplicate
            ps.setString(2, "hash_dup");
            assertThrows(SQLException.class, () -> {
                try (ResultSet rs = ps.executeQuery()) { rs.next(); }
            });
        }

        // Insert a different email; must succeed with all columns intact
        UUID newId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_ret (email, hash) VALUES (?, ?) RETURNING id, email, hash")) {
            ps.setString(1, "second@test.com");
            ps.setString(2, "hash_second");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "INSERT must succeed after duplicate violation");
                newId = (UUID) rs.getObject("id");
                assertNotNull(newId);
                assertEquals("second@test.com", rs.getString("email"));
                assertEquals("hash_second", rs.getString("hash"),
                        "hash must NOT be null or corrupted after error recovery");
            }
        }

        // Verify via separate SELECT
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT email, hash FROM err_ret WHERE id = ?")) {
            ps.setObject(1, newId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("second@test.com", rs.getString("email"));
                assertEquals("hash_second", rs.getString("hash"),
                        "hash must match in separate SELECT after error");
            }
        }
    }

    // =========================================================================
    // 5. Interleaved errors with different PreparedStatements
    // =========================================================================

    @Test
    void testInterleavedErrorsDifferentStatements() throws SQLException {
        exec("""
            CREATE TABLE err_accounts (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                balance NUMERIC(10,2) NOT NULL DEFAULT 0
            )
        """);
        exec("""
            CREATE TABLE err_transactions (
                id serial PRIMARY KEY,
                account_id UUID NOT NULL REFERENCES err_accounts(id),
                amount NUMERIC(10,2) NOT NULL,
                description TEXT
            )
        """);

        // Create account
        UUID accountId;
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_accounts (name, balance) VALUES (?, ?) RETURNING id")) {
            ps.setString(1, "Test Account");
            ps.setBigDecimal(2, new BigDecimal("1000.00"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                accountId = (UUID) rs.getObject(1);
            }
        }

        try (PreparedStatement insertTx = conn.prepareStatement(
                     "INSERT INTO err_transactions (account_id, amount, description) VALUES (?, ?, ?) RETURNING id");
             PreparedStatement selectAccount = conn.prepareStatement(
                     "SELECT name, balance FROM err_accounts WHERE id = ?")) {

            for (int i = 0; i < 20; i++) {
                if (i % 3 == 0) {
                    // Every 3rd iteration: cause an FK violation
                    try (PreparedStatement badInsert = conn.prepareStatement(
                            "INSERT INTO err_transactions (account_id, amount, description) VALUES (?, ?, ?)")) {
                        badInsert.setObject(1, UUID.randomUUID()); // nonexistent account
                        badInsert.setBigDecimal(2, new BigDecimal("50.00"));
                        badInsert.setString(3, "bad_tx_" + i);
                        assertThrows(SQLException.class, badInsert::executeUpdate);
                    }
                }

                // Valid transaction insert
                insertTx.setObject(1, accountId);
                insertTx.setBigDecimal(2, new BigDecimal("10.00"));
                insertTx.setString(3, "tx_" + i);
                try (ResultSet rs = insertTx.executeQuery()) {
                    assertTrue(rs.next(), "Valid INSERT must succeed on iteration " + i);
                    int txId = rs.getInt(1);
                    assertTrue(txId > 0);
                }

                // Verify account is still readable with correct data
                selectAccount.setObject(1, accountId);
                try (ResultSet rs = selectAccount.executeQuery()) {
                    assertTrue(rs.next(), "SELECT must find account on iteration " + i);
                    assertEquals("Test Account", rs.getString("name"),
                            "Account name corrupted on iteration " + i);
                    BigDecimal balance = rs.getBigDecimal("balance");
                    assertNotNull(balance, "Balance null on iteration " + i);
                    assertEquals(0, new BigDecimal("1000.00").compareTo(balance),
                            "Balance corrupted on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // 6. NOT NULL violation then verify correct INSERT
    // =========================================================================

    @Test
    void testNotNullViolationRecovery() throws SQLException {
        exec("""
            CREATE TABLE err_notnull (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                display_name TEXT,
                active BOOLEAN DEFAULT true
            )
        """);

        // Violate NOT NULL on password_hash
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_notnull (email, password_hash) VALUES (?, ?)")) {
            ps.setString(1, "user@test.com");
            ps.setNull(2, Types.VARCHAR); // NOT NULL violation
            assertThrows(SQLException.class, ps::executeUpdate);
        }

        // Now insert correctly; all columns must be populated
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_notnull (email, password_hash, display_name) VALUES (?, ?, ?) RETURNING *")) {
            ps.setString(1, "good@test.com");
            ps.setString(2, "bcrypt_hash_abc");
            ps.setString(3, "Good User");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertNotNull(rs.getObject("id"), "id must not be null");
                assertEquals("good@test.com", rs.getString("email"));
                assertEquals("bcrypt_hash_abc", rs.getString("password_hash"),
                        "password_hash MUST be 'bcrypt_hash_abc', not null or corrupted");
                assertEquals("Good User", rs.getString("display_name"));
                assertTrue(rs.getBoolean("active"));
            }
        }
    }

    // =========================================================================
    // 7. Parse error recovery: invalid SQL then valid SQL
    // =========================================================================

    @Test
    void testParseErrorRecovery() throws SQLException {
        exec("CREATE TABLE err_parse (id serial PRIMARY KEY, val TEXT)");
        exec("INSERT INTO err_parse (val) VALUES ('test_data')");

        // Send invalid SQL to trigger a parse error
        assertThrows(SQLException.class, () -> {
            try (PreparedStatement ps = conn.prepareStatement("SELECTX FROM err_parse")) {
                ps.executeQuery();
            }
        });

        // Connection must still work
        try (PreparedStatement ps = conn.prepareStatement("SELECT val FROM err_parse WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "SELECT must work after parse error");
                assertEquals("test_data", rs.getString(1));
            }
        }
    }

    // =========================================================================
    // 8. CHECK constraint violation then correct insert
    // =========================================================================

    @Test
    void testCheckViolationRecovery() throws SQLException {
        exec("CREATE TABLE err_check (id serial PRIMARY KEY, age INT CHECK (age >= 0 AND age <= 150), name TEXT NOT NULL)");

        // Violate CHECK
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_check (age, name) VALUES (?, ?)")) {
            ps.setInt(1, -5);
            ps.setString(2, "Invalid");
            assertThrows(SQLException.class, ps::executeUpdate);
        }

        // Valid insert
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO err_check (age, name) VALUES (?, ?) RETURNING id, age, name")) {
            ps.setInt(1, 25);
            ps.setString(2, "Valid");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(25, rs.getInt("age"));
                assertEquals("Valid", rs.getString("name"));
            }
        }
    }

    // =========================================================================
    // 9. High-frequency error/success alternation (stress test)
    // =========================================================================

    @Test
    void testHighFrequencyErrorSuccessAlternation() throws SQLException {
        exec("CREATE TABLE err_stress (id serial PRIMARY KEY, val TEXT NOT NULL UNIQUE, data TEXT NOT NULL)");

        try (PreparedStatement insert = conn.prepareStatement(
                     "INSERT INTO err_stress (val, data) VALUES (?, ?) RETURNING id, val, data");
             PreparedStatement badInsert = conn.prepareStatement(
                     "INSERT INTO err_stress (val, data) VALUES (?, ?)");
             PreparedStatement select = conn.prepareStatement(
                     "SELECT val, data FROM err_stress WHERE id = ?")) {

            for (int i = 0; i < 50; i++) {
                // Successful insert
                String val = "val_" + i;
                String data = "data_" + UUID.randomUUID();
                insert.setString(1, val);
                insert.setString(2, data);
                int id;
                try (ResultSet rs = insert.executeQuery()) {
                    assertTrue(rs.next(), "INSERT must succeed on iteration " + i);
                    id = rs.getInt("id");
                    assertEquals(val, rs.getString("val"));
                    assertEquals(data, rs.getString("data"),
                            "data in RETURNING must match on iteration " + i);
                }

                // Failed insert (duplicate val)
                badInsert.setString(1, val); // duplicate
                badInsert.setString(2, "should_fail");
                assertThrows(SQLException.class, badInsert::executeUpdate,
                        "Duplicate violation expected on iteration " + i);

                // Verify via SELECT
                select.setInt(1, id);
                try (ResultSet rs = select.executeQuery()) {
                    assertTrue(rs.next(), "SELECT must work on iteration " + i);
                    assertEquals(val, rs.getString("val"));
                    assertEquals(data, rs.getString("data"),
                            "data in SELECT must match on iteration " + i);
                }
            }
        }
    }

    // =========================================================================
    // 10. TRUNCATE between error cycles (the real-world pattern)
    // =========================================================================

    @Test
    void testTruncateBetweenErrorCycles() throws SQLException {
        exec("""
            CREATE TABLE err_trunc_parent (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL);
            CREATE TABLE err_trunc_child (id serial PRIMARY KEY, parent_id UUID NOT NULL REFERENCES err_trunc_parent(id), info TEXT NOT NULL)
        """);

        try (PreparedStatement insertParent = conn.prepareStatement(
                     "INSERT INTO err_trunc_parent (name) VALUES (?) RETURNING id");
             PreparedStatement insertChild = conn.prepareStatement(
                     "INSERT INTO err_trunc_child (parent_id, info) VALUES (?, ?) RETURNING id");
             PreparedStatement badChild = conn.prepareStatement(
                     "INSERT INTO err_trunc_child (parent_id, info) VALUES (?, ?)");
             PreparedStatement selectChild = conn.prepareStatement(
                     "SELECT info FROM err_trunc_child WHERE id = ?")) {

            for (int cycle = 0; cycle < 5; cycle++) {
                // Create parent
                insertParent.setString(1, "parent_cycle_" + cycle);
                UUID parentId;
                try (ResultSet rs = insertParent.executeQuery()) {
                    assertTrue(rs.next());
                    parentId = (UUID) rs.getObject(1);
                }

                // FK violation
                badChild.setObject(1, UUID.randomUUID());
                badChild.setString(2, "orphan");
                assertThrows(SQLException.class, badChild::executeUpdate);

                // Valid child insert
                insertChild.setObject(1, parentId);
                insertChild.setString(2, "child_cycle_" + cycle);
                int childId;
                try (ResultSet rs = insertChild.executeQuery()) {
                    assertTrue(rs.next(), "Child INSERT must succeed on cycle " + cycle);
                    childId = rs.getInt(1);
                }

                // Verify
                selectChild.setInt(1, childId);
                try (ResultSet rs = selectChild.executeQuery()) {
                    assertTrue(rs.next(), "SELECT must find child on cycle " + cycle);
                    assertEquals("child_cycle_" + cycle, rs.getString("info"),
                            "Child info corrupted on cycle " + cycle);
                }

                // Truncate (order matters for FK)
                exec("TRUNCATE err_trunc_child, err_trunc_parent CASCADE");
            }
        }
    }
}
