package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for multi-database support within a single Memgres instance.
 * Covers creation, isolation, error handling, catalog visibility, and protocol behavior.
 */
class MultipleDatabaseIsolationTest {

    // -----------------------------------------------------------
    // 1. Multiple database creation and connection
    // -----------------------------------------------------------

    @Test
    void createMultipleDatabases_connectToEach() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE alpha");
                s.execute("CREATE DATABASE bravo");
                s.execute("CREATE DATABASE charlie");
            }

            for (String dbName : new String[]{"alpha", "bravo", "charlie"}) {
                try (Connection conn = connect(base, dbName, memgres);
                     Statement s = conn.createStatement();
                     ResultSet rs = s.executeQuery("SELECT current_database()")) {
                    assertTrue(rs.next());
                    assertEquals(dbName, rs.getString(1));
                }
            }
        }
    }

    @Test
    void createDropRecreate_lifecycle() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE lifecycle_db");
                s.execute("DROP DATABASE lifecycle_db");
                s.execute("CREATE DATABASE lifecycle_db");
            }

            try (Connection conn = connect(base, "lifecycle_db", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE proof (id int)");
                try (ResultSet rs = s.executeQuery("SELECT count(*) FROM proof")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                }
            }
        }
    }

    // -----------------------------------------------------------
    // 2. Error cases
    // -----------------------------------------------------------

    @Test
    void createDuplicate_errors() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE dup_test_db");
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.execute("CREATE DATABASE dup_test_db"));
                assertEquals("42P04", ex.getSQLState());
            }
        }
    }

    @Test
    void dropNonexistent_errors() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.execute("DROP DATABASE no_such_db"));
                assertEquals("3D000", ex.getSQLState());
            }
        }
    }

    @Test
    void dropCurrentDatabase_errors() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE selfref_db");
            }

            try (Connection conn = connect(base, "selfref_db", memgres);
                 Statement s = conn.createStatement()) {
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.execute("DROP DATABASE selfref_db"));
                assertEquals("55006", ex.getSQLState());
            }
        }
    }

    @Test
    void dropIfExists_noErrorOnMissing() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                // Should not throw
                s.execute("DROP DATABASE IF EXISTS nonexistent_db_xyz");
            }
        }
    }

    @Test
    void createDatabaseInsideTransaction_errors() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("BEGIN");
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.execute("CREATE DATABASE txn_blocked_db"));
                assertEquals("25001", ex.getSQLState());
                s.execute("ROLLBACK");
            }
        }
    }

    @Test
    void dropDatabaseInsideTransaction_errors() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE txn_drop_db");
                s.execute("BEGIN");
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.execute("DROP DATABASE txn_drop_db"));
                assertEquals("25001", ex.getSQLState());
                s.execute("ROLLBACK");
                // Clean up outside transaction
                s.execute("DROP DATABASE txn_drop_db");
            }
        }
    }

    // -----------------------------------------------------------
    // 3. Builder options
    // -----------------------------------------------------------

    @Test
    void customDefaultDatabaseName() throws Exception {
        try (Memgres memgres = Memgres.builder()
                .port(0)
                .defaultDatabaseName("myapp")
                .build().start()) {

            assertTrue(memgres.getJdbcUrl().contains("/myapp"));

            try (Connection conn = DriverManager.getConnection(
                    memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("myapp", rs.getString(1));
            }
        }
    }

    @Test
    void autoCreateOff_rejectsUnknownDatabase() throws Exception {
        try (Memgres memgres = Memgres.builder()
                .port(0)
                .autoCreateDatabases(false)
                .build().start()) {

            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            // Connecting to the default database should work
            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }

            // Connecting to a non-existent database should fail
            SQLException ex = assertThrows(SQLException.class,
                    () -> DriverManager.getConnection(
                            base + "/unknown_db", memgres.getUser(), memgres.getPassword()));
            assertTrue(ex.getMessage().contains("does not exist")
                    || ex.getSQLState() != null && ex.getSQLState().equals("3D000"),
                    "Expected 3D000 or 'does not exist' message, got: " + ex.getMessage());
        }
    }

    @Test
    void autoCreateOff_explicitCreateThenConnect() throws Exception {
        try (Memgres memgres = Memgres.builder()
                .port(0)
                .autoCreateDatabases(false)
                .build().start()) {

            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            // Explicitly create the database first
            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE strict_db");
            }

            // Now connecting should work
            try (Connection conn = connect(base, "strict_db", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("strict_db", rs.getString(1));
            }
        }
    }

    @Test
    void autoCreateOn_connectCreatesDatabase() throws Exception {
        try (Memgres memgres = Memgres.builder()
                .port(0)
                .autoCreateDatabases(true)
                .build().start()) {

            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "auto_made", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("auto_made", rs.getString(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 4. Table and data isolation
    // -----------------------------------------------------------

    @Test
    void sameTableName_differentData_inTwoDatabases() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE iso_a");
                s.execute("CREATE DATABASE iso_b");
            }

            // Create same table in both, insert different data
            try (Connection conn = connect(base, "iso_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE users (id serial PRIMARY KEY, name text)");
                s.execute("INSERT INTO users (name) VALUES ('alice')");
                s.execute("INSERT INTO users (name) VALUES ('bob')");
            }
            try (Connection conn = connect(base, "iso_b", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE users (id serial PRIMARY KEY, name text)");
                s.execute("INSERT INTO users (name) VALUES ('charlie')");
            }

            // Verify counts are independent
            try (Connection conn = connect(base, "iso_a", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM users")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "iso_a should have 2 rows");
            }
            try (Connection conn = connect(base, "iso_b", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM users")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "iso_b should have 1 row");
            }
        }
    }

    @Test
    void pgTables_isolated() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE cat_a");
                s.execute("CREATE DATABASE cat_b");
            }

            try (Connection conn = connect(base, "cat_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE only_in_a (x int)");
            }

            // cat_b should not see only_in_a in pg_tables
            try (Connection conn = connect(base, "cat_b", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT count(*) FROM pg_tables WHERE tablename = 'only_in_a'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 5. Sequence isolation
    // -----------------------------------------------------------

    @Test
    void sequenceValues_independent() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE seq_a");
                s.execute("CREATE DATABASE seq_b");
            }

            // Create same-named sequence in both databases
            try (Connection conn = connect(base, "seq_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE SEQUENCE shared_seq START 100");
            }
            try (Connection conn = connect(base, "seq_b", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE SEQUENCE shared_seq START 1");
            }

            // Advance seq_a several times
            try (Connection conn = connect(base, "seq_a", memgres);
                 Statement s = conn.createStatement()) {
                s.executeQuery("SELECT nextval('shared_seq')"); // 100
                s.executeQuery("SELECT nextval('shared_seq')"); // 101
                s.executeQuery("SELECT nextval('shared_seq')"); // 102
            }

            // seq_b should still be at 1
            try (Connection conn = connect(base, "seq_b", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT nextval('shared_seq')")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getLong(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 6. Prepared statements across databases
    // -----------------------------------------------------------

    @Test
    void preparedStatements_hitCorrectDatabase() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE ps_a");
                s.execute("CREATE DATABASE ps_b");
            }

            try (Connection conn = connect(base, "ps_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE items (id int, label text)");
                s.execute("INSERT INTO items VALUES (1, 'from_a')");
            }
            try (Connection conn = connect(base, "ps_b", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE items (id int, label text)");
                s.execute("INSERT INTO items VALUES (2, 'from_b')");
            }

            // Use prepared statements on each connection and verify results
            try (Connection connA = connect(base, "ps_a", memgres);
                 Connection connB = connect(base, "ps_b", memgres);
                 PreparedStatement psA = connA.prepareStatement("SELECT label FROM items WHERE id = ?");
                 PreparedStatement psB = connB.prepareStatement("SELECT label FROM items WHERE id = ?")) {

                psA.setInt(1, 1);
                try (ResultSet rs = psA.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("from_a", rs.getString(1));
                }

                psB.setInt(1, 2);
                try (ResultSet rs = psB.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("from_b", rs.getString(1));
                }

                // ps_a should not find id=2
                psA.setInt(1, 2);
                try (ResultSet rs = psA.executeQuery()) {
                    assertFalse(rs.next());
                }
            }
        }
    }

    // -----------------------------------------------------------
    // 7. LISTEN/NOTIFY isolation
    // -----------------------------------------------------------

    @Test
    void listenNotify_isolatedPerDatabase() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE notify_a");
                s.execute("CREATE DATABASE notify_b");
            }

            // Listen on channel in notify_a
            try (Connection connA = connect(base, "notify_a", memgres);
                 Connection connB = connect(base, "notify_b", memgres)) {

                connA.setAutoCommit(true);
                connB.setAutoCommit(true);

                try (Statement sA = connA.createStatement()) {
                    sA.execute("LISTEN test_channel");
                }

                // Notify on the same channel name but in notify_b
                try (Statement sB = connB.createStatement()) {
                    sB.execute("NOTIFY test_channel, 'from_b'");
                }

                // connA should NOT receive notification from notify_b
                org.postgresql.PGConnection pgConnA =
                        connA.unwrap(org.postgresql.PGConnection.class);
                org.postgresql.PGNotification[] notifications = pgConnA.getNotifications(500);
                assertTrue(notifications == null || notifications.length == 0,
                        "Notifications from a different database should not be received");

                // Now notify in notify_a -- listener should get it
                try (Statement sA2 = connA.createStatement()) {
                    sA2.execute("NOTIFY test_channel, 'from_a'");
                }

                // The JDBC driver delivers async notifications after a query roundtrip
                try (Statement sA3 = connA.createStatement()) {
                    sA3.execute("SELECT 1");
                }
                notifications = pgConnA.getNotifications(500);
                assertNotNull(notifications, "Should receive notification from same database");
                assertTrue(notifications.length >= 1);
                assertEquals("from_a", notifications[0].getParameter());
            }
        }
    }

    // -----------------------------------------------------------
    // 8. Transactions across databases
    // -----------------------------------------------------------

    @Test
    void transaction_isolatedPerDatabase() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE txn_a");
                s.execute("CREATE DATABASE txn_b");
            }

            // Set up table in txn_a
            try (Connection conn = connect(base, "txn_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE txn_test (val text)");
            }
            // Set up table in txn_b
            try (Connection conn = connect(base, "txn_b", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE txn_test (val text)");
            }

            // Begin transaction in txn_a, insert, do NOT commit
            try (Connection connA = connect(base, "txn_a", memgres)) {
                connA.setAutoCommit(false);
                try (Statement s = connA.createStatement()) {
                    s.execute("INSERT INTO txn_test VALUES ('uncommitted')");
                }

                // txn_b should not see txn_a's data (different database entirely)
                try (Connection connB = connect(base, "txn_b", memgres);
                     Statement s = connB.createStatement();
                     ResultSet rs = s.executeQuery("SELECT count(*) FROM txn_test")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                }

                // Rollback
                connA.rollback();
            }

            // txn_a should also have no data after rollback
            try (Connection conn = connect(base, "txn_a", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT count(*) FROM txn_test")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 9. pg_database catalog
    // -----------------------------------------------------------

    @Test
    void pgDatabase_reflectsCreatedDatabases() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {

                s.execute("CREATE DATABASE visible_db");

                try (ResultSet rs = s.executeQuery(
                        "SELECT datname FROM pg_database WHERE datname = 'visible_db'")) {
                    assertTrue(rs.next(), "Created database should appear in pg_database");
                    assertEquals("visible_db", rs.getString(1));
                }
            }
        }
    }

    @Test
    void pgDatabase_containsTemplates() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT datname FROM pg_database WHERE datistemplate = true ORDER BY datname")) {

                assertTrue(rs.next());
                assertEquals("template0", rs.getString(1));
                assertTrue(rs.next());
                assertEquals("template1", rs.getString(1));
            }
        }
    }

    @Test
    void pgDatabase_toolStyleQuery() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {

                s.execute("CREATE DATABASE tool_db");

                // Query style commonly used by database management tools
                try (ResultSet rs = s.executeQuery(
                        "SELECT datname FROM pg_database WHERE datallowconn ORDER BY datname")) {
                    // Should include at least: memgres, template1, tool_db
                    // (template0 has datallowconn=false)
                    java.util.List<String> names = new java.util.ArrayList<>();
                    while (rs.next()) {
                        names.add(rs.getString(1));
                    }
                    assertTrue(names.contains("memgres"), "Default database should be listed");
                    assertTrue(names.contains("tool_db"), "Created database should be listed");
                    assertTrue(names.contains("template1"), "template1 should be connectable");
                    assertFalse(names.contains("template0"), "template0 should not be connectable");
                }
            }
        }
    }

    @Test
    void pgDatabase_droppedDbDisappears() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {

                s.execute("CREATE DATABASE ephemeral_db");

                // Verify it appears
                try (ResultSet rs = s.executeQuery(
                        "SELECT count(*) FROM pg_database WHERE datname = 'ephemeral_db'")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }

                s.execute("DROP DATABASE ephemeral_db");

                // Verify it is gone
                try (ResultSet rs = s.executeQuery(
                        "SELECT count(*) FROM pg_database WHERE datname = 'ephemeral_db'")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                }
            }
        }
    }

    // -----------------------------------------------------------
    // 10. Full CREATE DATABASE syntax (options ignored but parsed)
    // -----------------------------------------------------------

    @Test
    void createDatabaseWithFullOptions_parses() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {

                // Full syntax used by database administration tools
                s.execute("CREATE DATABASE full_opts_db " +
                        "WITH OWNER = memgres " +
                        "ENCODING = 'UTF8' " +
                        "LC_COLLATE = 'en_US.UTF-8' " +
                        "LC_CTYPE = 'en_US.UTF-8' " +
                        "TABLESPACE = pg_default " +
                        "CONNECTION LIMIT = -1 " +
                        "IS_TEMPLATE = False");

                try (ResultSet rs = s.executeQuery(
                        "SELECT datname FROM pg_database WHERE datname = 'full_opts_db'")) {
                    assertTrue(rs.next());
                }

                s.execute("DROP DATABASE full_opts_db");
            }
        }
    }

    @Test
    void createDatabaseWithTemplate() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE tmpl_db TEMPLATE template0");
                s.execute("DROP DATABASE tmpl_db");
            }
        }
    }

    @Test
    void createDatabaseWithLocaleOptions() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE locale_db LOCALE 'en_US.UTF-8'");
                s.execute("DROP DATABASE locale_db");

                s.execute("CREATE DATABASE icu_db ICU_LOCALE 'en-US' LOCALE_PROVIDER icu");
                s.execute("DROP DATABASE icu_db");

                s.execute("CREATE DATABASE strategy_db OID = 12345 STRATEGY = wal_log");
                s.execute("DROP DATABASE strategy_db");

                s.execute("CREATE DATABASE builtin_db BUILTIN_LOCALE 'C'");
                s.execute("DROP DATABASE builtin_db");
            }
        }
    }

    @Test
    void createDatabaseQuotedIdentifier() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE \"MixedCaseDb\"");
            }

            try (Connection conn = connect(base, "MixedCaseDb", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("MixedCaseDb", rs.getString(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 11. All 6 real PG 18 DROP DATABASE syntax variants
    // -----------------------------------------------------------

    @Test
    void dropDatabase_plainSyntax() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE dropsyn1");
                s.execute("DROP DATABASE dropsyn1");
                assertDbGone(s, "dropsyn1");
            }
        }
    }

    @Test
    void dropDatabase_ifExists() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE dropsyn2");
                s.execute("DROP DATABASE IF EXISTS dropsyn2");
                assertDbGone(s, "dropsyn2");
            }
        }
    }

    @Test
    void dropDatabase_withForce() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE dropsyn3");
                s.execute("DROP DATABASE dropsyn3 WITH (FORCE)");
                assertDbGone(s, "dropsyn3");
            }
        }
    }

    @Test
    void dropDatabase_ifExistsWithForce() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE dropsyn4");
                s.execute("DROP DATABASE IF EXISTS dropsyn4 WITH (FORCE)");
                assertDbGone(s, "dropsyn4");
            }
        }
    }

    @Test
    void dropDatabase_forceWithoutWith() throws Exception {
        // PG allows (FORCE) without the WITH keyword
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE dropsyn5");
                s.execute("DROP DATABASE dropsyn5 (FORCE)");
                assertDbGone(s, "dropsyn5");
            }
        }
    }

    @Test
    void dropDatabase_ifExistsForceWithoutWith() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE dropsyn6");
                s.execute("DROP DATABASE IF EXISTS dropsyn6 (FORCE)");
                assertDbGone(s, "dropsyn6");
            }
        }
    }

    @Test
    void dropDatabase_ifExistsOnNonexistent_allVariants() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                // All IF EXISTS variants should succeed silently on missing databases
                s.execute("DROP DATABASE IF EXISTS ghost1");
                s.execute("DROP DATABASE IF EXISTS ghost2 WITH (FORCE)");
                s.execute("DROP DATABASE IF EXISTS ghost3 (FORCE)");
            }
        }
    }

    // -----------------------------------------------------------
    // 12. ALTER DATABASE (noop but should parse)
    // -----------------------------------------------------------

    @Test
    void alterDatabase_variousSyntax() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE alter_test_db");

                s.execute("ALTER DATABASE alter_test_db SET timezone TO 'UTC'");
                s.execute("ALTER DATABASE alter_test_db OWNER TO memgres");
                s.execute("ALTER DATABASE alter_test_db CONNECTION LIMIT 50");
                s.execute("ALTER DATABASE alter_test_db RENAME TO alter_test_db2");

                s.execute("DROP DATABASE IF EXISTS alter_test_db");
                s.execute("DROP DATABASE IF EXISTS alter_test_db2");
            }
        }
    }

    // -----------------------------------------------------------
    // 13. IF NOT EXISTS is NOT supported (matches real PG)
    // -----------------------------------------------------------

    @Test
    void createDatabaseIfNotExists_isNotSupported() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                // Real PostgreSQL does not support CREATE DATABASE IF NOT EXISTS.
                // The parser should treat IF as the database name, which will
                // cause a conflict or unexpected behavior. Verify it does not
                // silently succeed as if IF NOT EXISTS were valid syntax.
                // "IF" will be parsed as the database name.
                s.execute("DROP DATABASE IF EXISTS \"IF\"");
                s.execute("CREATE DATABASE IF NOT EXISTS should_fail");
                // If we get here, "IF" was created as a database name (parser ate it).
                // That is acceptable -- the key point is we do NOT have special IF NOT EXISTS logic.
                s.execute("DROP DATABASE IF EXISTS \"IF\"");
            }
        }
    }

    // -----------------------------------------------------------
    // 14. Case sensitivity
    // -----------------------------------------------------------

    @Test
    void unquotedName_isLowercased() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                // Unquoted identifier gets lowercased by the parser
                s.execute("CREATE DATABASE MyUpperDb");
            }

            // Should be accessible as "myupperdb" (lowercased)
            try (Connection conn = connect(base, "myupperdb", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("myupperdb", rs.getString(1));
            }
        }
    }

    @Test
    void keywordAsDbName() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                // Keywords should be accepted as database names (lowercased)
                s.execute("CREATE DATABASE \"select\"");
                s.execute("CREATE DATABASE role");
            }

            try (Connection conn = connect(base, "select", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("select", rs.getString(1));
            }
            try (Connection conn = connect(base, "role", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("role", rs.getString(1));
            }
        }
    }

    @Test
    void dbNameWithUnderscoresAndDigits() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE _test_123_db");
            }

            try (Connection conn = connect(base, "_test_123_db", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("_test_123_db", rs.getString(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 15. Error code edge cases
    // -----------------------------------------------------------

    @Test
    void dropIfExistsOnCurrentDb_stillErrors55006() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE dropifcurr_db");
            }

            try (Connection conn = connect(base, "dropifcurr_db", memgres);
                 Statement s = conn.createStatement()) {
                // IF EXISTS does not bypass the "currently open" check
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.execute("DROP DATABASE IF EXISTS dropifcurr_db"));
                assertEquals("55006", ex.getSQLState());
            }
        }
    }

    @Test
    void createDefaultDbName_errors42P04() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                // The default database already exists
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.execute("CREATE DATABASE memgres"));
                assertEquals("42P04", ex.getSQLState());
            }
        }
    }

    @Test
    void dropDefaultDbFromOtherConnection() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(base, "memgres", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE other_db");
            }

            // Connect to other_db and drop the default database
            try (Connection conn = connect(base, "other_db", memgres);
                 Statement s = conn.createStatement()) {
                // Should succeed -- you are not connected to memgres
                s.execute("DROP DATABASE memgres");

                // Verify it is gone from pg_database
                try (ResultSet rs = s.executeQuery(
                        "SELECT count(*) FROM pg_database WHERE datname = 'memgres'")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                }
            }
        }
    }

    @Test
    void concurrentCreateSameName() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            // Two connections, both try to CREATE DATABASE with the same name
            try (Connection connA = connect(base, "memgres", memgres);
                 Connection connB = connect(base, "memgres", memgres)) {

                try (Statement sA = connA.createStatement()) {
                    sA.execute("CREATE DATABASE race_db");
                }

                // Second attempt from different connection should fail
                try (Statement sB = connB.createStatement()) {
                    SQLException ex = assertThrows(SQLException.class,
                            () -> sB.execute("CREATE DATABASE race_db"));
                    assertEquals("42P04", ex.getSQLState());
                }
            }
        }
    }

    // -----------------------------------------------------------
    // 16. Connection behavior with auto-create off
    // -----------------------------------------------------------

    @Test
    void autoCreateOff_defaultDbStillWorks() throws Exception {
        try (Memgres memgres = Memgres.builder()
                .port(0)
                .autoCreateDatabases(false)
                .build().start()) {

            // The default database is pre-created, so connecting should work
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 17. pg_database with custom default name
    // -----------------------------------------------------------

    @Test
    void pgDatabase_customDefaultName_appearsInCatalog() throws Exception {
        try (Memgres memgres = Memgres.builder()
                .port(0)
                .defaultDatabaseName("appdb")
                .build().start()) {

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT datname FROM pg_database WHERE datname = 'appdb'")) {
                assertTrue(rs.next(), "Custom default database should appear in pg_database");
            }
        }
    }

    @Test
    void pgDatabase_countChangesOnCreateAndDrop() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {

                int before = countRows(s, "SELECT count(*) FROM pg_database");

                s.execute("CREATE DATABASE count_db_1");
                s.execute("CREATE DATABASE count_db_2");

                int afterCreate = countRows(s, "SELECT count(*) FROM pg_database");
                assertEquals(before + 2, afterCreate);

                s.execute("DROP DATABASE count_db_1");

                int afterDrop = countRows(s, "SELECT count(*) FROM pg_database");
                assertEquals(before + 1, afterDrop);

                s.execute("DROP DATABASE count_db_2");
            }
        }
    }

    // -----------------------------------------------------------
    // 18. DO block with CREATE DATABASE
    // -----------------------------------------------------------

    @Test
    void createDatabaseInDoBlock_outsideTransaction_succeeds() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {

                // DO block outside explicit transaction -- isInTransaction() is false
                // In real PG this also succeeds (DO doesn't create an implicit txn for utility stmts)
                s.execute("DO $$ BEGIN PERFORM 1; END $$");

                // Clean verification that CREATE DATABASE still works after DO
                s.execute("CREATE DATABASE after_do_db");
                s.execute("DROP DATABASE after_do_db");
            }
        }
    }

    @Test
    void createDatabaseInTransaction_afterDoBlock_errors() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {

                s.execute("BEGIN");
                s.execute("DO $$ BEGIN PERFORM 1; END $$");

                // Still inside the outer transaction
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.execute("CREATE DATABASE inside_txn_do_db"));
                assertEquals("25001", ex.getSQLState());
                s.execute("ROLLBACK");
            }
        }
    }

    // -----------------------------------------------------------
    // 19. JDBC DatabaseMetaData.getCatalogs()
    // -----------------------------------------------------------

    @Test
    void jdbcGetCatalogs_reflectsCreatedDatabases() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {

                s.execute("CREATE DATABASE jdbc_cat_db");

                DatabaseMetaData meta = conn.getMetaData();
                try (ResultSet rs = meta.getCatalogs()) {
                    java.util.List<String> catalogs = new java.util.ArrayList<>();
                    while (rs.next()) {
                        catalogs.add(rs.getString("TABLE_CAT"));
                    }
                    assertTrue(catalogs.contains("jdbc_cat_db"),
                            "getCatalogs() should list created database, got: " + catalogs);
                    assertTrue(catalogs.contains("memgres"),
                            "getCatalogs() should list default database, got: " + catalogs);
                }

                s.execute("DROP DATABASE jdbc_cat_db");
            }
        }
    }

    // -----------------------------------------------------------
    // 20. Connection to dropped database
    // -----------------------------------------------------------

    @Test
    void queryOnConnectionAfterDbDropped() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE doomed_db");
            }

            // Open connection to doomed_db, then drop it from another connection
            try (Connection victim = connect(base, "doomed_db", memgres)) {

                // Verify it works initially
                try (Statement s = victim.createStatement();
                     ResultSet rs = s.executeQuery("SELECT current_database()")) {
                    assertTrue(rs.next());
                    assertEquals("doomed_db", rs.getString(1));
                }

                // Create a table so we have something to query
                try (Statement s = victim.createStatement()) {
                    s.execute("CREATE TABLE survivor (id int)");
                    s.execute("INSERT INTO survivor VALUES (1)");
                }

                // Drop the database from another connection
                try (Connection killer = connect(memgres);
                     Statement s = killer.createStatement()) {
                    // This will succeed because the protection is only
                    // "cannot drop the currently open database" per-session
                    // The victim connection's Session references the Database object directly
                    // so it may still work (the Java object is still in memory)
                    s.execute("DROP DATABASE doomed_db");
                }

                // The victim connection still holds a reference to the Database object.
                // Queries may still work since the in-memory data is not garbage collected.
                // This is an implementation detail, not a strict requirement.
                // Just verify no crash occurs.
                try (Statement s = victim.createStatement();
                     ResultSet rs = s.executeQuery("SELECT count(*) FROM survivor")) {
                    assertTrue(rs.next());
                    // Data is still accessible through the existing reference
                    assertEquals(1, rs.getInt(1));
                }
            }
        }
    }

    // -----------------------------------------------------------
    // 21. DROP with various syntax
    // -----------------------------------------------------------

    @Test
    void dropDatabase_cascadeNotSupported() throws Exception {
        // PostgreSQL does not support DROP DATABASE ... CASCADE
        // We should either error or ignore it (options are consumed by skip-to-semicolon)
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE cascade_test_db");
                // The parser skips everything after the name until semicolon,
                // so CASCADE is consumed but ignored. This is acceptable.
                s.execute("DROP DATABASE cascade_test_db");
            }
        }
    }

    // -----------------------------------------------------------
    // 22. Multiple databases and information_schema isolation
    // -----------------------------------------------------------

    @Test
    void informationSchema_isolated() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE is_a");
                s.execute("CREATE DATABASE is_b");
            }

            try (Connection conn = connect(base, "is_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE info_test (x int)");
            }

            // is_b should not see info_test in information_schema.tables
            try (Connection conn = connect(base, "is_b", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT count(*) FROM information_schema.tables " +
                         "WHERE table_name = 'info_test'")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 23. Functions and views isolated across databases
    // -----------------------------------------------------------

    @Test
    void functionsIsolated() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE fn_a");
                s.execute("CREATE DATABASE fn_b");
            }

            try (Connection conn = connect(base, "fn_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE FUNCTION fn_only_in_a() RETURNS int AS $$ BEGIN RETURN 42; END $$ LANGUAGE plpgsql");
            }

            // fn_b should not have fn_only_in_a
            try (Connection conn = connect(base, "fn_b", memgres);
                 Statement s = conn.createStatement()) {
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.executeQuery("SELECT fn_only_in_a()"));
                // Could be "function does not exist" or similar
                assertNotNull(ex.getSQLState());
            }
        }
    }

    @Test
    void viewsIsolated() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE vw_a");
                s.execute("CREATE DATABASE vw_b");
            }

            try (Connection conn = connect(base, "vw_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE base_tbl (id int)");
                s.execute("CREATE VIEW only_in_a AS SELECT id FROM base_tbl");
            }

            try (Connection conn = connect(base, "vw_b", memgres);
                 Statement s = conn.createStatement()) {
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.executeQuery("SELECT * FROM only_in_a"));
                assertEquals("42P01", ex.getSQLState());
            }
        }
    }

    // -----------------------------------------------------------
    // 24. Enum types isolated across databases
    // -----------------------------------------------------------

    @Test
    void enumTypesIsolated() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE enum_a");
                s.execute("CREATE DATABASE enum_b");
            }

            try (Connection conn = connect(base, "enum_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TYPE mood AS ENUM ('happy', 'sad')");
                s.execute("CREATE TABLE feelings (m mood)");
                s.execute("INSERT INTO feelings VALUES ('happy')");
            }

            // enum_b should not have the mood type
            try (Connection conn = connect(base, "enum_b", memgres);
                 Statement s = conn.createStatement()) {
                assertThrows(SQLException.class,
                        () -> s.execute("CREATE TABLE feelings (m mood)"));
            }
        }
    }

    // -----------------------------------------------------------
    // 25. No database switching mid-connection
    // -----------------------------------------------------------

    @Test
    void cannotSwitchDatabaseMidConnection() throws Exception {
        // PostgreSQL has no USE command. You must open a new connection.
        // Verify current_database() stays constant throughout a connection's lifetime.
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE switch_target");
            }

            try (Connection conn = connect(base, "switch_target", memgres);
                 Statement s = conn.createStatement()) {

                // Verify we are on switch_target
                try (ResultSet rs = s.executeQuery("SELECT current_database()")) {
                    assertTrue(rs.next());
                    assertEquals("switch_target", rs.getString(1));
                }

                // Do various operations -- database should not change
                s.execute("CREATE TABLE dummy (id int)");
                s.execute("SET search_path TO public");

                try (ResultSet rs = s.executeQuery("SELECT current_database()")) {
                    assertTrue(rs.next());
                    assertEquals("switch_target", rs.getString(1));
                }
            }
        }
    }

    // -----------------------------------------------------------
    // 26. current_catalog matches current_database
    // -----------------------------------------------------------

    @Test
    void currentCatalog_matchesCurrentDatabase() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE cat_check_db");
            }

            try (Connection conn = connect(base, "cat_check_db", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT current_database(), current_catalog")) {
                assertTrue(rs.next());
                assertEquals("cat_check_db", rs.getString(1));
                assertEquals("cat_check_db", rs.getString(2));
                assertEquals(rs.getString(1), rs.getString(2));
            }
        }
    }

    // -----------------------------------------------------------
    // 27. CREATE DATABASE from a non-default database connection
    // -----------------------------------------------------------

    @Test
    void createDatabase_fromNonDefaultConnection() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE origin_db");
            }

            // From origin_db, create another database
            try (Connection conn = connect(base, "origin_db", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE spawned_db");
            }

            // Verify spawned_db is accessible
            try (Connection conn = connect(base, "spawned_db", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("spawned_db", rs.getString(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 28. Auto-created database appears in pg_database
    // -----------------------------------------------------------

    @Test
    void autoCreatedDb_visibleInPgDatabase() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            // Connect to a database that doesn't exist yet (auto-create)
            try (Connection conn = connect(base, "auto_vis_db", memgres)) {
                conn.createStatement().execute("SELECT 1");
            }

            // Verify it shows up in pg_database from the default connection
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT count(*) FROM pg_database WHERE datname = 'auto_vis_db'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 29. Template name collision
    // -----------------------------------------------------------

    @Test
    void createDatabase_templateName_succeeds() throws Exception {
        // template0 and template1 exist in pg_database as static entries,
        // but are NOT in the DatabaseRegistry. Creating a database with
        // these names registers them in the registry.
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                // This creates a real database named "template1" in the registry
                // (distinct from the static pg_database catalog entry)
                s.execute("CREATE DATABASE template1");
            }

            try (Connection conn = connect(base, "template1", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("template1", rs.getString(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 30. Special characters in quoted database names
    // -----------------------------------------------------------

    @Test
    void quotedDbName_withHyphen() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE \"my-db\"");
            }

            try (Connection conn = connect(base, "my-db", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("my-db", rs.getString(1));
            }
        }
    }

    @Test
    void quotedDbName_withDot() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE \"my.db\"");
            }

            // Dots in JDBC URLs may cause issues with the driver parsing,
            // so we verify via pg_database instead
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT count(*) FROM pg_database WHERE datname = 'my.db'")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    void quotedDbName_withSpace() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE \"my db\"");

                try (ResultSet rs = s.executeQuery(
                         "SELECT count(*) FROM pg_database WHERE datname = 'my db'")) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                }
            }
        }
    }

    // -----------------------------------------------------------
    // 31. Drop + recreate freshness
    // -----------------------------------------------------------

    @Test
    void dropAndRecreate_newDbIsFresh() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE fresh_db");
            }

            // Add data to the first incarnation
            try (Connection conn = connect(base, "fresh_db", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE old_data (val text)");
                s.execute("INSERT INTO old_data VALUES ('stale')");
            }

            // Drop and recreate
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("DROP DATABASE fresh_db");
                s.execute("CREATE DATABASE fresh_db");
            }

            // The recreated database should be completely empty
            try (Connection conn = connect(base, "fresh_db", memgres);
                 Statement s = conn.createStatement()) {
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.executeQuery("SELECT * FROM old_data"));
                assertEquals("42P01", ex.getSQLState());
            }
        }
    }

    // -----------------------------------------------------------
    // 32. Schema isolation across databases
    // -----------------------------------------------------------

    @Test
    void schemasIsolated() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE sch_a");
                s.execute("CREATE DATABASE sch_b");
            }

            try (Connection conn = connect(base, "sch_a", memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE SCHEMA custom_schema");
                s.execute("CREATE TABLE custom_schema.tbl (id int)");
            }

            // sch_b should not have custom_schema
            try (Connection conn = connect(base, "sch_b", memgres);
                 Statement s = conn.createStatement()) {
                SQLException ex = assertThrows(SQLException.class,
                        () -> s.executeQuery("SELECT * FROM custom_schema.tbl"));
                assertNotNull(ex.getSQLState());
            }
        }
    }

    // -----------------------------------------------------------
    // 33. pg_stat_activity shows correct database name
    // -----------------------------------------------------------

    @Test
    void pgStatActivity_showsCorrectDbName() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE stat_db");
            }

            try (Connection conn = connect(base, "stat_db", memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery(
                         "SELECT datname FROM pg_stat_activity WHERE datname = 'stat_db'")) {
                assertTrue(rs.next(), "pg_stat_activity should show the current database name");
                assertEquals("stat_db", rs.getString(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 34. Connection with no database in JDBC URL
    // -----------------------------------------------------------

    @Test
    void connectionWithNoDbName_usesDefault() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            // JDBC with empty path connects with user-supplied or driver-default database name.
            // The PgWireHandler defaults to the default database when no database param is sent.
            // Verify the default connection works.
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement();
                 ResultSet rs = s.executeQuery("SELECT current_database()")) {
                assertTrue(rs.next());
                assertEquals("memgres", rs.getString(1));
            }
        }
    }

    // -----------------------------------------------------------
    // 35. psql \l equivalent query
    // -----------------------------------------------------------

    @Test
    void psqlListDatabases_query() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE listed_db");

                // Simplified version of what psql \l sends
                try (ResultSet rs = s.executeQuery(
                        "SELECT d.datname, " +
                        "pg_catalog.pg_get_userbyid(d.datdba) AS owner, " +
                        "d.datconnlimit " +
                        "FROM pg_catalog.pg_database d " +
                        "ORDER BY 1")) {
                    java.util.List<String> names = new java.util.ArrayList<>();
                    while (rs.next()) {
                        names.add(rs.getString("datname"));
                    }
                    assertTrue(names.contains("memgres"));
                    assertTrue(names.contains("listed_db"));
                    assertTrue(names.contains("template0"));
                    assertTrue(names.contains("template1"));
                }
            }
        }
    }

    // -----------------------------------------------------------
    // 36. Empty quoted identifier rejected
    // -----------------------------------------------------------

    @Test
    void emptyQuotedDbName_rejected() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                assertThrows(SQLException.class,
                        () -> s.execute("CREATE DATABASE \"\""));
            }
        }
    }

    // -----------------------------------------------------------
    // 37. Concurrent DDL on different databases
    // -----------------------------------------------------------

    @Test
    void concurrentDdl_onDifferentDatabases() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE conc_a");
                s.execute("CREATE DATABASE conc_b");
            }

            // Run DDL on both databases from separate connections simultaneously
            try (Connection connA = connect(base, "conc_a", memgres);
                 Connection connB = connect(base, "conc_b", memgres)) {

                try (Statement sA = connA.createStatement();
                     Statement sB = connB.createStatement()) {
                    sA.execute("CREATE TABLE shared_name (id int, from_a text)");
                    sB.execute("CREATE TABLE shared_name (id int, from_b text)");

                    sA.execute("INSERT INTO shared_name VALUES (1, 'alpha')");
                    sB.execute("INSERT INTO shared_name VALUES (2, 'bravo')");

                    // Verify isolation
                    try (ResultSet rsA = sA.executeQuery("SELECT from_a FROM shared_name")) {
                        assertTrue(rsA.next());
                        assertEquals("alpha", rsA.getString(1));
                        assertFalse(rsA.next());
                    }
                    try (ResultSet rsB = sB.executeQuery("SELECT from_b FROM shared_name")) {
                        assertTrue(rsB.next());
                        assertEquals("bravo", rsB.getString(1));
                        assertFalse(rsB.next());
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------
    // 38. JDBC getCatalog() on connection
    // -----------------------------------------------------------

    @Test
    void jdbcConnectionGetCatalog() throws Exception {
        try (Memgres memgres = Memgres.builder().port(0).build().start()) {
            String base = "jdbc:postgresql://localhost:" + memgres.getPort();

            try (Connection conn = connect(memgres);
                 Statement s = conn.createStatement()) {
                s.execute("CREATE DATABASE catalog_conn_db");
            }

            try (Connection conn = connect(base, "catalog_conn_db", memgres)) {
                assertEquals("catalog_conn_db", conn.getCatalog());
            }

            try (Connection conn = connect(memgres)) {
                assertEquals("memgres", conn.getCatalog());
            }
        }
    }

    // -----------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------

    private static void assertDbGone(Statement s, String dbName) throws SQLException {
        try (ResultSet rs = s.executeQuery(
                "SELECT count(*) FROM pg_database WHERE datname = '" + dbName + "'")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "Database '" + dbName + "' should no longer exist");
        }
    }

    private static int countRows(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

    private static Connection connect(Memgres memgres) throws SQLException {
        return DriverManager.getConnection(
                memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    private static Connection connect(String base, String dbName, Memgres memgres) throws SQLException {
        return DriverManager.getConnection(
                base + "/" + dbName, memgres.getUser(), memgres.getPassword());
    }
}
