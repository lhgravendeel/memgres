package com.memgres.compat15;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for 4 ALTER INDEX failures from alter-index.sql where Memgres
 * diverges from PostgreSQL 18 behavior.
 *
 * Stmt 16 - Renaming a PK index should be reflected in pg_class
 * Stmt 48 - COMMENT ON INDEX ... IS NULL should remove the comment
 * Stmt 67 - ALTER INDEX SET (fillfactor=70) should appear in reloptions
 * Stmt 88 - COMMENT ON INDEX ... IS NULL should remove the comment (via regclass)
 */
class AlterIndexCompat15Test {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA IF EXISTS ai_test CASCADE");
            s.execute("CREATE SCHEMA ai_test");
            s.execute("SET search_path = ai_test, public");
            s.execute("CREATE TABLE ai_data (id integer PRIMARY KEY, val integer, label text)");
            s.execute("INSERT INTO ai_data VALUES (1, 10, 'alpha'), (2, 20, 'beta'), (3, 30, 'gamma')");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS ai_test CASCADE");
                s.execute("SET search_path = public");
            } catch (SQLException ignored) {
            }
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row from: " + sql);
            return rs.getString(1);
        }
    }

    /**
     * Stmt 16: Renaming the PK index (ai_data_pkey -> ai_data_pk_renamed)
     * should be visible in pg_class. PG returns true, Memgres returns false.
     */
    @Test
    void testRenamePkIndexVisibleInPgClass() throws SQLException {
        // Verify the PK index exists first
        assertEquals("true", query1(
                "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = 'ai_data_pkey' AND relkind = 'i')::text AS exists"));

        exec("ALTER INDEX ai_data_pkey RENAME TO ai_data_pk_renamed");
        try {
            String result = query1(
                    "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = 'ai_data_pk_renamed' AND relkind = 'i')::text AS exists");
            assertEquals("true", result,
                    "After renaming PK index, ai_data_pk_renamed should exist in pg_class");
        } finally {
            // Rename back for other tests / cleanup
            try {
                exec("ALTER INDEX ai_data_pk_renamed RENAME TO ai_data_pkey");
            } catch (SQLException ignored) {
            }
        }
    }

    /**
     * Stmt 48: After setting COMMENT ON INDEX to NULL, the comment row should
     * be removed from pg_description. PG returns 0, Memgres returns 1.
     */
    @Test
    void testCommentOnIndexSetNullRemovesDescription() throws SQLException {
        exec("CREATE INDEX idx_ai_comment_s48 ON ai_data (val)");
        try {
            exec("COMMENT ON INDEX idx_ai_comment_s48 IS 'This is a test index'");

            // Verify comment was set
            assertEquals("1", query1(
                    "SELECT count(*)::integer FROM pg_description d "
                            + "JOIN pg_class c ON d.objoid = c.oid "
                            + "WHERE c.relname = 'idx_ai_comment_s48'"));

            // Remove comment
            exec("COMMENT ON INDEX idx_ai_comment_s48 IS NULL");

            // PG returns 0 — the description row should be gone
            String cnt = query1(
                    "SELECT count(*)::integer AS cnt FROM pg_description d "
                            + "JOIN pg_class c ON d.objoid = c.oid "
                            + "WHERE c.relname = 'idx_ai_comment_s48'");
            assertEquals("0", cnt,
                    "After COMMENT ON INDEX ... IS NULL, pg_description should have no rows for the index");
        } finally {
            try {
                exec("DROP INDEX IF EXISTS idx_ai_comment_s48");
            } catch (SQLException ignored) {
            }
        }
    }

    /**
     * Stmt 67: ALTER INDEX SET (fillfactor = 70) should store the option in
     * reloptions. PG returns true, Memgres returns NULL.
     */
    @Test
    void testAlterIndexSetFillfactorReflectedInReloptions() throws SQLException {
        exec("CREATE INDEX idx_ai_storage ON ai_data (val)");
        try {
            exec("ALTER INDEX idx_ai_storage SET (fillfactor = 70)");

            String hasOption = query1(
                    "SELECT (reloptions @> ARRAY['fillfactor=70'])::text AS has_option "
                            + "FROM pg_class WHERE relname = 'idx_ai_storage'");
            assertEquals("true", hasOption,
                    "reloptions should contain 'fillfactor=70' after ALTER INDEX SET");
        } finally {
            try {
                exec("DROP INDEX IF EXISTS idx_ai_storage");
            } catch (SQLException ignored) {
            }
        }
    }

    /**
     * Stmt 88: After set/change/remove cycle on COMMENT ON INDEX, the final
     * IS NULL should remove the row from pg_description (queried via regclass).
     * PG returns 0, Memgres returns 1.
     */
    @Test
    void testCommentOnIndexSetNullRemovesDescriptionViaRegclass() throws SQLException {
        exec("CREATE INDEX idx_ai_comment_s88 ON ai_data (val)");
        try {
            exec("COMMENT ON INDEX idx_ai_comment_s88 IS 'initial comment'");

            // Verify initial comment
            assertEquals("initial comment", query1(
                    "SELECT description FROM pg_description "
                            + "WHERE objoid = 'idx_ai_comment_s88'::regclass"));

            // Update comment
            exec("COMMENT ON INDEX idx_ai_comment_s88 IS 'updated comment'");
            assertEquals("updated comment", query1(
                    "SELECT description FROM pg_description "
                            + "WHERE objoid = 'idx_ai_comment_s88'::regclass"));

            // Remove comment
            exec("COMMENT ON INDEX idx_ai_comment_s88 IS NULL");

            // PG returns 0 — the description row should be gone
            String cnt = query1(
                    "SELECT count(*)::integer AS cnt FROM pg_description "
                            + "WHERE objoid = 'idx_ai_comment_s88'::regclass");
            assertEquals("0", cnt,
                    "After COMMENT ON INDEX ... IS NULL, pg_description should have no rows (via regclass)");
        } finally {
            try {
                exec("DROP INDEX IF EXISTS idx_ai_comment_s88");
            } catch (SQLException ignored) {
            }
        }
    }
}
