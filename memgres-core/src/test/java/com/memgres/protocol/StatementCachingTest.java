package com.memgres.protocol;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Document Java/JDBC section 16: Statement caching and server-side prepare thresholds.
 * Covers JDBC PreparedStatement reuse, SQL-level PREPARE/EXECUTE/DEALLOCATE,
 * and behavior across schema changes, transaction boundaries, and driver thresholds.
 */
class StatementCachingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // --- 1. Repeated execution of same PreparedStatement produces correct results ---

    @Test void repeated_execution_same_prepared_statement() throws Exception {
        exec("CREATE TABLE sc_repeat(id int PRIMARY KEY, v text)");
        exec("INSERT INTO sc_repeat VALUES (1,'alpha'),(2,'beta'),(3,'gamma')");
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM sc_repeat WHERE id = ?")) {
            for (int run = 0; run < 5; run++) {
                ps.setInt(1, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Expected a row on run " + run);
                    assertEquals("beta", rs.getString(1));
                    assertFalse(rs.next());
                }
            }
        } finally {
            exec("DROP TABLE sc_repeat");
        }
    }

    // --- 2. PreparedStatement with different parameter values across executions ---

    @Test void prepared_statement_different_params() throws Exception {
        exec("CREATE TABLE sc_diffparam(id int PRIMARY KEY, v int)");
        exec("INSERT INTO sc_diffparam VALUES (1,10),(2,20),(3,30)");
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM sc_diffparam WHERE id = ?")) {
            int[] ids = {1, 2, 3, 1, 3};
            int[] expected = {10, 20, 30, 10, 30};
            for (int i = 0; i < ids.length; i++) {
                ps.setInt(1, ids[i]);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(expected[i], rs.getInt(1));
                }
            }
        } finally {
            exec("DROP TABLE sc_diffparam");
        }
    }

    // --- 3. PreparedStatement reuse after schema change (DROP/CREATE same table) ---

    @Test void prepared_statement_after_drop_create_same_table() throws Exception {
        exec("CREATE TABLE sc_schema(id int, v text)");
        exec("INSERT INTO sc_schema VALUES (1,'first')");
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM sc_schema WHERE id = ?")) {
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("first", rs.getString(1));
            }
            // Drop and recreate the table with same structure
            exec("DROP TABLE sc_schema");
            exec("CREATE TABLE sc_schema(id int, v text)");
            exec("INSERT INTO sc_schema VALUES (1,'second')");

            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("second", rs.getString(1));
            }
        } finally {
            exec("DROP TABLE IF EXISTS sc_schema");
        }
    }

    // --- 4. Multiple PreparedStatements on same connection ---

    @Test void multiple_prepared_statements_same_connection() throws Exception {
        exec("CREATE TABLE sc_multi_a(id int PRIMARY KEY, v text)");
        exec("CREATE TABLE sc_multi_b(id int PRIMARY KEY, v text)");
        exec("INSERT INTO sc_multi_a VALUES (1,'a1'),(2,'a2')");
        exec("INSERT INTO sc_multi_b VALUES (1,'b1'),(2,'b2')");
        try (PreparedStatement psA = conn.prepareStatement("SELECT v FROM sc_multi_a WHERE id = ?");
             PreparedStatement psB = conn.prepareStatement("SELECT v FROM sc_multi_b WHERE id = ?")) {
            // Interleave executions
            psA.setInt(1, 1);
            psB.setInt(1, 2);
            try (ResultSet rsA = psA.executeQuery()) {
                assertTrue(rsA.next());
                assertEquals("a1", rsA.getString(1));
            }
            try (ResultSet rsB = psB.executeQuery()) {
                assertTrue(rsB.next());
                assertEquals("b2", rsB.getString(1));
            }
            // Swap parameters and re-execute
            psA.setInt(1, 2);
            psB.setInt(1, 1);
            try (ResultSet rsA = psA.executeQuery()) {
                assertTrue(rsA.next());
                assertEquals("a2", rsA.getString(1));
            }
            try (ResultSet rsB = psB.executeQuery()) {
                assertTrue(rsB.next());
                assertEquals("b1", rsB.getString(1));
            }
        } finally {
            exec("DROP TABLE sc_multi_a");
            exec("DROP TABLE sc_multi_b");
        }
    }

    // --- 5. PreparedStatement after connection autocommit toggle ---

    @Test void prepared_statement_after_autocommit_toggle() throws Exception {
        exec("CREATE TABLE sc_autocommit(id int PRIMARY KEY, v text)");
        exec("INSERT INTO sc_autocommit VALUES (1,'hello')");
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM sc_autocommit WHERE id = ?")) {
            // Execute with autocommit on
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("hello", rs.getString(1));
            }
            // Toggle autocommit off then back on
            conn.setAutoCommit(false);
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("hello", rs.getString(1));
            }
            conn.commit();
            conn.setAutoCommit(true);

            // Execute again with autocommit restored
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("hello", rs.getString(1));
            }
        } finally {
            conn.setAutoCommit(true);
            exec("DROP TABLE sc_autocommit");
        }
    }

    // --- 6. PREPARE/EXECUTE via SQL (server-side prepared statements) ---

    @Test void sql_prepare_execute() throws Exception {
        exec("CREATE TABLE sc_sqlprep(id int PRIMARY KEY, v text)");
        exec("INSERT INTO sc_sqlprep VALUES (1,'one'),(2,'two')");
        exec("PREPARE sc_plan AS SELECT v FROM sc_sqlprep WHERE id = $1");
        try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_plan(1)")) {
            assertTrue(rs.next());
            assertEquals("one", rs.getString(1));
        }
        try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_plan(2)")) {
            assertTrue(rs.next());
            assertEquals("two", rs.getString(1));
        }
        exec("DEALLOCATE sc_plan");
        exec("DROP TABLE sc_sqlprep");
    }

    // --- 7. PREPARE with explicit types ---

    @Test void sql_prepare_with_explicit_types() throws Exception {
        exec("CREATE TABLE sc_typed(id int PRIMARY KEY, name text, amount numeric)");
        exec("INSERT INTO sc_typed VALUES (1,'item',99.95)");
        exec("PREPARE sc_typed_plan(int, text) AS SELECT amount FROM sc_typed WHERE id = $1 AND name = $2");
        try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_typed_plan(1, 'item')")) {
            assertTrue(rs.next());
            assertEquals(99.95, rs.getDouble(1), 0.001);
        }
        // Wrong type usage should still work via implicit cast or fail gracefully
        try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_typed_plan(1, 'nonexistent')")) {
            assertFalse(rs.next());
        }
        exec("DEALLOCATE sc_typed_plan");
        exec("DROP TABLE sc_typed");
    }

    // --- 8. DEALLOCATE and re-prepare same name ---

    @Test void deallocate_and_reprepare_same_name() throws Exception {
        exec("CREATE TABLE sc_reprep(id int PRIMARY KEY, v text)");
        exec("INSERT INTO sc_reprep VALUES (1,'original')");
        exec("PREPARE sc_reuse_plan AS SELECT v FROM sc_reprep WHERE id = $1");
        try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_reuse_plan(1)")) {
            assertTrue(rs.next());
            assertEquals("original", rs.getString(1));
        }
        // Deallocate and re-prepare with different query
        exec("DEALLOCATE sc_reuse_plan");
        exec("UPDATE sc_reprep SET v = 'modified' WHERE id = 1");
        exec("PREPARE sc_reuse_plan AS SELECT v || '_suffix' FROM sc_reprep WHERE id = $1");
        try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_reuse_plan(1)")) {
            assertTrue(rs.next());
            assertEquals("modified_suffix", rs.getString(1));
        }
        exec("DEALLOCATE sc_reuse_plan");
        exec("DROP TABLE sc_reprep");
    }

    // --- 9. DEALLOCATE ALL ---

    @Test void deallocate_all() throws Exception {
        exec("CREATE TABLE sc_dealloc(id int PRIMARY KEY, v text)");
        exec("INSERT INTO sc_dealloc VALUES (1,'x'),(2,'y')");
        exec("PREPARE sc_da_plan1 AS SELECT v FROM sc_dealloc WHERE id = $1");
        exec("PREPARE sc_da_plan2 AS SELECT id FROM sc_dealloc WHERE v = $1");
        // Verify both work
        try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_da_plan1(1)")) {
            assertTrue(rs.next());
            assertEquals("x", rs.getString(1));
        }
        try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_da_plan2('y')")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
        // Deallocate all
        exec("DEALLOCATE ALL");
        // Both should fail after DEALLOCATE ALL
        assertThrows(SQLException.class, () -> {
            try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_da_plan1(1)")) {}
        });
        assertThrows(SQLException.class, () -> {
            try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_da_plan2('y')")) {}
        });
        exec("DROP TABLE sc_dealloc");
    }

    // --- 10. Execute prepared statement with wrong number of params fails ---

    @Test void execute_prepared_wrong_param_count_fails() throws Exception {
        exec("CREATE TABLE sc_wrongp(id int PRIMARY KEY, v text)");
        exec("INSERT INTO sc_wrongp VALUES (1,'val')");
        exec("PREPARE sc_wrong_plan AS SELECT v FROM sc_wrongp WHERE id = $1");
        // Too many parameters
        assertThrows(SQLException.class, () -> {
            try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_wrong_plan(1, 2)")) {}
        });
        // No parameters
        assertThrows(SQLException.class, () -> {
            try (ResultSet rs = conn.createStatement().executeQuery("EXECUTE sc_wrong_plan()")) {}
        });
        exec("DEALLOCATE sc_wrong_plan");
        exec("DROP TABLE sc_wrongp");
    }

    // --- 11. Prepared statement survives across transaction boundaries ---

    @Test void prepared_statement_survives_transaction_boundaries() throws Exception {
        exec("CREATE TABLE sc_txbound(id int PRIMARY KEY, v text)");
        exec("INSERT INTO sc_txbound VALUES (1,'start')");
        conn.setAutoCommit(false);
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM sc_txbound WHERE id = ?")) {
            // Execute in first transaction
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("start", rs.getString(1));
            }
            conn.commit();

            // Modify data in new transaction
            exec("UPDATE sc_txbound SET v = 'committed' WHERE id = 1");
            conn.commit();

            // Execute same PreparedStatement in a new transaction
            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("committed", rs.getString(1));
            }
            conn.commit();

            // Also survives a rollback
            exec("UPDATE sc_txbound SET v = 'rolled_back' WHERE id = 1");
            conn.rollback();

            ps.setInt(1, 1);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("committed", rs.getString(1));
            }
            conn.commit();
        } finally {
            conn.setAutoCommit(true);
            exec("DROP TABLE sc_txbound");
        }
    }

    // --- 12. Multiple executions crossing driver prepare threshold ---

    @Test void multiple_executions_crossing_prepare_threshold() throws Exception {
        exec("CREATE TABLE sc_threshold(id int PRIMARY KEY, v int)");
        try (PreparedStatement ins = conn.prepareStatement("INSERT INTO sc_threshold VALUES (?, ?)")) {
            for (int i = 1; i <= 20; i++) {
                ins.setInt(1, i);
                ins.setInt(2, i * 10);
                ins.executeUpdate();
            }
        }
        // Execute the same PreparedStatement 10 times to cross typical driver prepare threshold (default 5)
        try (PreparedStatement ps = conn.prepareStatement("SELECT v FROM sc_threshold WHERE id = ?")) {
            for (int i = 1; i <= 10; i++) {
                ps.setInt(1, i);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), "Expected row for id=" + i);
                    assertEquals(i * 10, rs.getInt(1), "Wrong value for id=" + i);
                    assertFalse(rs.next(), "Expected single row for id=" + i);
                }
            }
        } finally {
            exec("DROP TABLE sc_threshold");
        }
    }
}
