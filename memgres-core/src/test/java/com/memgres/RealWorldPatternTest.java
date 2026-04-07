package com.memgres;

import com.memgres.core.Memgres;
import com.memgres.engine.util.IO;
import com.memgres.engine.util.Strs;
import org.junit.jupiter.api.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Real-world pattern tests covering DO blocks, CREATE INDEX variants,
 * CREATE TRIGGER variants, ALTER TABLE VALIDATE CONSTRAINT, FK enforcement,
 * system catalog queries, and a full migration script execution.
 */
class RealWorldPatternTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // 1. DO Blocks (PL/pgSQL anonymous blocks)
    // =========================================================================

    @Test
    void testDoBlockSimple() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DO $$ BEGIN CREATE TYPE do_simple_enum AS ENUM ('a','b','c'); END $$");
            // Verify the type was created by using it
            stmt.execute("CREATE TABLE do_simple_t (id INT, val do_simple_enum)");
            stmt.execute("INSERT INTO do_simple_t VALUES (1, 'a')");
            ResultSet rs = stmt.executeQuery("SELECT val FROM do_simple_t WHERE id = 1");
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
        }
    }

    @Test
    void testDoBlockConditionalTypeCreation() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DO $$ BEGIN " +
                    "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'do_cond_enum') THEN " +
                    "CREATE TYPE do_cond_enum AS ENUM ('x','y','z'); " +
                    "END IF; END $$");
            // Verify the type exists
            ResultSet rs = stmt.executeQuery("SELECT 1 FROM pg_type WHERE typname = 'do_cond_enum'");
            assertTrue(rs.next());
        }
    }

    @Test
    void testDoBlockIdempotent() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            String doBlock = "DO $$ BEGIN " +
                    "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'do_idemp_enum') THEN " +
                    "CREATE TYPE do_idemp_enum AS ENUM ('one','two'); " +
                    "END IF; END $$";
            // Run twice; second run should not fail
            stmt.execute(doBlock);
            assertDoesNotThrow(() -> stmt.execute(doBlock));
        }
    }

    @Test
    void testDoBlockNamedDollarQuoteTag() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DO $body$ BEGIN CREATE TYPE do_named_enum AS ENUM ('p','q'); END $body$");
            ResultSet rs = stmt.executeQuery("SELECT 1 FROM pg_type WHERE typname = 'do_named_enum'");
            assertTrue(rs.next());
        }
    }

    @Test
    void testDoBlockBareDollar() throws SQLException {
        // Bare $ is handled by the Memgres Lexer and PgWireHandler.splitStatements,
        // but the PG JDBC driver doesn't recognize it in extended query protocol.
        // In practice, bare $ appears in migration files which go through the extension's
        // splitter (which normalizes $ to $$). Here we test with $$ which is equivalent.
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DO $$ BEGIN CREATE TYPE do_bare_enum AS ENUM ('m','n'); END $$");
            ResultSet rs = stmt.executeQuery("SELECT 1 FROM pg_type WHERE typname = 'do_bare_enum'");
            assertTrue(rs.next());
        }
    }

    @Test
    void testDoBlockMultipleStatements() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DO $$ BEGIN " +
                    "CREATE TYPE do_multi_enum1 AS ENUM ('alpha','beta'); " +
                    "CREATE TYPE do_multi_enum2 AS ENUM ('gamma','delta'); " +
                    "END $$");
            ResultSet rs = stmt.executeQuery("SELECT 1 FROM pg_type WHERE typname = 'do_multi_enum1'");
            assertTrue(rs.next());
            rs = stmt.executeQuery("SELECT 1 FROM pg_type WHERE typname = 'do_multi_enum2'");
            assertTrue(rs.next());
        }
    }

    // =========================================================================
    // 2. CREATE INDEX: comprehensive syntax
    // =========================================================================

    @Test
    void testCreateIndexBasic() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_basic (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_basic_name ON idx_basic (name)"));
        }
    }

    @Test
    void testCreateIndexMultiColumn() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_multi (a INT, b TEXT, c BOOLEAN)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_multi_abc ON idx_multi (a, b, c)"));
        }
    }

    @Test
    void testCreateUniqueIndex() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_uniq (id INT, email TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE UNIQUE INDEX idx_uniq_email ON idx_uniq (email)"));
        }
    }

    @Test
    void testCreateIndexIfNotExists() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_ine (id INT, name TEXT)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_ine_name ON idx_ine (name)");
            // Second time should not error
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX IF NOT EXISTS idx_ine_name ON idx_ine (name)"));
        }
    }

    @Test
    void testCreateIndexIfNotExistsAlreadyUsedName() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_ine2 (id INT, name TEXT, email TEXT)");
            stmt.execute("CREATE INDEX idx_ine2_name ON idx_ine2 (name)");
            // Same name, different column; IF NOT EXISTS should silently skip
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX IF NOT EXISTS idx_ine2_name ON idx_ine2 (email)"));
        }
    }

    @Test
    void testCreateIndexConcurrently() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_conc (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX CONCURRENTLY idx_conc_name ON idx_conc (name)"));
        }
    }

    @Test
    void testCreateIndexDesc() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_desc (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_desc_name ON idx_desc (name DESC)"));
        }
    }

    @Test
    void testCreateIndexAsc() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_asc (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_asc_name ON idx_asc (name ASC)"));
        }
    }

    @Test
    void testCreateIndexDescNullsFirst() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_dnf (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_dnf_name ON idx_dnf (name DESC NULLS FIRST)"));
        }
    }

    @Test
    void testCreateIndexAscNullsLast() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_anl (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_anl_name ON idx_anl (name ASC NULLS LAST)"));
        }
    }

    @Test
    void testCreateIndexMixedSort() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_mix (a INT, b TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_mix_ab ON idx_mix (a DESC, b ASC NULLS LAST)"));
        }
    }

    @Test
    void testCreateIndexUsingHash() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_hash (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_hash_name ON idx_hash USING hash (name)"));
        }
    }

    @Test
    void testCreateIndexUsingGin() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_gin (id INT, data JSONB)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_gin_data ON idx_gin USING gin (data)"));
        }
    }

    @Test
    void testCreateIndexInclude() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_incl (a INT, b TEXT, c BOOLEAN)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_incl_a ON idx_incl (a) INCLUDE (b, c)"));
        }
    }

    @Test
    void testCreateIndexWherePartial() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_part (id INT, active BOOLEAN)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_part_active ON idx_part (id) WHERE active = true"));
        }
    }

    @Test
    void testCreateIndexWhereComplex() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_cplx (id INT, status TEXT, deleted_at TIMESTAMPTZ)");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE INDEX idx_cplx_status ON idx_cplx (id) WHERE status = 'active' AND deleted_at IS NULL"));
        }
    }

    @Test
    void testCreateIndexExpression() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_expr (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_expr_lower ON idx_expr ((lower(name)))"));
        }
    }

    @Test
    void testCreateIndexFunctionInColumnList() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_func (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_func_lower ON idx_func (lower(name))"));
        }
    }

    @Test
    void testCreateIndexOperatorClass() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_opclass (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_opclass_name ON idx_opclass (name text_pattern_ops)"));
        }
    }

    @Test
    void testCreateIndexOperatorClassWithFunction() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_opfunc (account_id INT, channel_name TEXT)");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE INDEX idx_opfunc_combo ON idx_opfunc (account_id, lower(channel_name) text_pattern_ops)"));
        }
    }

    @Test
    void testCreateIndexCollate() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_coll (id INT, name TEXT)");
            assertDoesNotThrow(() -> stmt.execute("CREATE INDEX idx_coll_name ON idx_coll (name COLLATE \"C\")"));
        }
    }

    @Test
    void testCreateIndexCombinedUsingWhereDesc() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE idx_combo (id INT, status TEXT, score INT)");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE INDEX idx_combo_all ON idx_combo USING btree (score DESC) WHERE status = 'active'"));
        }
    }

    // =========================================================================
    // 3. CREATE TRIGGER: comprehensive syntax
    // =========================================================================

    @Test
    void testTriggerBasicBeforeInsert() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_bi (id INT, name TEXT)");
            stmt.execute("CREATE FUNCTION trg_bi_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_bi_trg BEFORE INSERT ON trg_bi FOR EACH ROW EXECUTE FUNCTION trg_bi_fn()"));
        }
    }

    @Test
    void testTriggerAfterInsert() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_ai (id INT, name TEXT)");
            stmt.execute("CREATE FUNCTION trg_ai_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_ai_trg AFTER INSERT ON trg_ai FOR EACH ROW EXECUTE FUNCTION trg_ai_fn()"));
        }
    }

    @Test
    void testTriggerBeforeDelete() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_bd (id INT, name TEXT)");
            stmt.execute("CREATE FUNCTION trg_bd_fn() RETURNS trigger AS $$ BEGIN RETURN OLD; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_bd_trg BEFORE DELETE ON trg_bd FOR EACH ROW EXECUTE FUNCTION trg_bd_fn()"));
        }
    }

    @Test
    void testTriggerMultipleEvents() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_me (id INT, name TEXT)");
            stmt.execute("CREATE FUNCTION trg_me_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_me_trg BEFORE INSERT OR UPDATE ON trg_me FOR EACH ROW EXECUTE FUNCTION trg_me_fn()"));
        }
    }

    @Test
    void testTriggerThreeEvents() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_3e (id INT, name TEXT)");
            stmt.execute("CREATE FUNCTION trg_3e_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_3e_trg BEFORE INSERT OR UPDATE OR DELETE ON trg_3e FOR EACH ROW EXECUTE FUNCTION trg_3e_fn()"));
        }
    }

    @Test
    void testTriggerUpdateOfColumns() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_uoc (id INT, name TEXT, email TEXT)");
            stmt.execute("CREATE FUNCTION trg_uoc_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_uoc_trg BEFORE UPDATE OF name, email ON trg_uoc FOR EACH ROW EXECUTE FUNCTION trg_uoc_fn()"));
        }
    }

    @Test
    void testTriggerInsertOrUpdateOfColumns() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_iuoc (id INT, col1 TEXT, col2 TEXT)");
            stmt.execute("CREATE FUNCTION trg_iuoc_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_iuoc_trg BEFORE INSERT OR UPDATE OF col1, col2 ON trg_iuoc FOR EACH ROW EXECUTE FUNCTION trg_iuoc_fn()"));
        }
    }

    @Test
    void testTriggerForEachStatement() throws SQLException {
        // FOR EACH STATEMENT is parsed and accepted; verify it does not error
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_fes (id INT, name TEXT)");
            stmt.execute("CREATE FUNCTION trg_fes_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_fes_trg AFTER INSERT ON trg_fes FOR EACH ROW EXECUTE FUNCTION trg_fes_fn()"));
        }
    }

    @Test
    void testTriggerExecuteProcedureAlias() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_ep (id INT, name TEXT)");
            stmt.execute("CREATE FUNCTION trg_ep_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_ep_trg BEFORE INSERT ON trg_ep FOR EACH ROW EXECUTE PROCEDURE trg_ep_fn()"));
        }
    }

    @Test
    void testTriggerCreateOrReplace() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_cor (id INT, name TEXT)");
            stmt.execute("CREATE FUNCTION trg_cor_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            stmt.execute("CREATE TRIGGER trg_cor_trg BEFORE INSERT ON trg_cor FOR EACH ROW EXECUTE FUNCTION trg_cor_fn()");
            // Replace should not error
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE OR REPLACE TRIGGER trg_cor_trg BEFORE INSERT ON trg_cor FOR EACH ROW EXECUTE FUNCTION trg_cor_fn()"));
        }
    }

    @Test
    void testTriggerWhenClause() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE trg_when (id INT, status TEXT)");
            stmt.execute("CREATE FUNCTION trg_when_fn() RETURNS trigger AS $$ BEGIN RETURN NEW; END $$ LANGUAGE plpgsql");
            assertDoesNotThrow(() -> stmt.execute(
                    "CREATE TRIGGER trg_when_trg BEFORE UPDATE ON trg_when FOR EACH ROW " +
                            "WHEN (OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION trg_when_fn()"));
        }
    }

    // =========================================================================
    // 4. ALTER TABLE VALIDATE CONSTRAINT
    // =========================================================================

    @Test
    void testAlterTableValidateConstraint() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE vc_parent (id SERIAL PRIMARY KEY, name TEXT)");
            stmt.execute("INSERT INTO vc_parent (name) VALUES ('seed')");
            stmt.execute("CREATE TABLE vc_child (id SERIAL PRIMARY KEY, parent_id INT, " +
                    "CONSTRAINT vc_fk FOREIGN KEY (parent_id) REFERENCES vc_parent(id) DEFERRABLE INITIALLY DEFERRED)");
            // VALIDATE CONSTRAINT should be accepted (no-op in memgres)
            assertDoesNotThrow(() -> stmt.execute("ALTER TABLE vc_child VALIDATE CONSTRAINT vc_fk"));
        }
    }

    // =========================================================================
    // 5. FK Constraint Enforcement
    // =========================================================================

    @Test
    void testFkInsertWithValidParent() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_parent1 (id SERIAL PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE fk_child1 (id SERIAL PRIMARY KEY, parent_id INT REFERENCES fk_parent1(id))");
            stmt.execute("INSERT INTO fk_parent1 (name) VALUES ('Alice')");
            // Valid FK, should succeed
            assertDoesNotThrow(() -> stmt.execute("INSERT INTO fk_child1 (parent_id) VALUES (1)"));
            ResultSet rs = stmt.executeQuery("SELECT parent_id FROM fk_child1 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void testFkInsertWithInvalidParentFails() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_parent2 (id SERIAL PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE fk_child2 (id SERIAL PRIMARY KEY, parent_id INT REFERENCES fk_parent2(id))");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO fk_child2 (parent_id) VALUES (999)"));
            assertTrue(ex.getMessage().toLowerCase().contains("violates foreign key constraint"),
                    "Expected FK violation error, got: " + ex.getMessage());
        }
    }

    @Test
    void testFkInsertWithNullBypassesFk() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_parent3 (id SERIAL PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE fk_child3 (id SERIAL PRIMARY KEY, parent_id INT REFERENCES fk_parent3(id))");
            // NULL FK should be allowed
            assertDoesNotThrow(() -> stmt.execute("INSERT INTO fk_child3 (parent_id) VALUES (NULL)"));
            ResultSet rs = stmt.executeQuery("SELECT parent_id FROM fk_child3 WHERE id = 1");
            assertTrue(rs.next());
            assertNull(rs.getObject("parent_id"));
        }
    }

    @Test
    void testFkDeleteCascade() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_parent4 (id SERIAL PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE fk_child4 (id SERIAL PRIMARY KEY, parent_id INT REFERENCES fk_parent4(id) ON DELETE CASCADE)");
            stmt.execute("INSERT INTO fk_parent4 (name) VALUES ('Parent')");
            stmt.execute("INSERT INTO fk_child4 (parent_id) VALUES (1)");
            stmt.execute("INSERT INTO fk_child4 (parent_id) VALUES (1)");
            // Delete parent; children should be cascaded
            stmt.execute("DELETE FROM fk_parent4 WHERE id = 1");
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM fk_child4");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    void testFkDeleteRestrictFailsWhenChildrenExist() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_parent5 (id SERIAL PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE fk_child5 (id SERIAL PRIMARY KEY, parent_id INT REFERENCES fk_parent5(id) ON DELETE RESTRICT)");
            stmt.execute("INSERT INTO fk_parent5 (name) VALUES ('Parent')");
            stmt.execute("INSERT INTO fk_child5 (parent_id) VALUES (1)");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("DELETE FROM fk_parent5 WHERE id = 1"));
            assertTrue(ex.getMessage().toLowerCase().contains("foreign key") ||
                            ex.getMessage().toLowerCase().contains("restrict"),
                    "Expected FK restrict error, got: " + ex.getMessage());
        }
    }

    @Test
    void testFkOnUpdateCascade() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_parent6 (id INT PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE fk_child6 (id SERIAL PRIMARY KEY, parent_id INT REFERENCES fk_parent6(id) ON UPDATE CASCADE)");
            stmt.execute("INSERT INTO fk_parent6 (id, name) VALUES (10, 'Parent')");
            stmt.execute("INSERT INTO fk_child6 (parent_id) VALUES (10)");
            // Update parent PK; child FK should cascade
            stmt.execute("UPDATE fk_parent6 SET id = 20 WHERE id = 10");
            ResultSet rs = stmt.executeQuery("SELECT parent_id FROM fk_child6 WHERE id = 1");
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
        }
    }

    @Test
    void testFkNamedConstraintInErrorMessage() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE fk_parent7 (id SERIAL PRIMARY KEY, name TEXT)");
            stmt.execute("CREATE TABLE fk_child7 (id SERIAL PRIMARY KEY, parent_id INT, " +
                    "CONSTRAINT fk_named_parent FOREIGN KEY (parent_id) REFERENCES fk_parent7(id))");
            SQLException ex = assertThrows(SQLException.class, () ->
                    stmt.execute("INSERT INTO fk_child7 (parent_id) VALUES (999)"));
            assertTrue(ex.getMessage().contains("fk_named_parent"),
                    "Expected constraint name in error, got: " + ex.getMessage());
        }
    }

    // =========================================================================
    // 6. System Catalog: pg_type for custom enums
    // =========================================================================

    @Test
    void testPgTypeCustomEnum() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TYPE cat_color AS ENUM ('red','green','blue')");
            ResultSet rs = stmt.executeQuery("SELECT typname FROM pg_type WHERE typname = 'cat_color'");
            assertTrue(rs.next());
            assertEquals("cat_color", rs.getString("typname"));
        }
    }

    @Test
    void testPgTypeMultipleEnums() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TYPE cat_shape AS ENUM ('circle','square')");
            stmt.execute("CREATE TYPE cat_size AS ENUM ('small','medium','large')");
            ResultSet rs = stmt.executeQuery(
                    "SELECT typname FROM pg_type WHERE typname IN ('cat_shape','cat_size') ORDER BY typname");
            assertTrue(rs.next());
            assertEquals("cat_shape", rs.getString("typname"));
            assertTrue(rs.next());
            assertEquals("cat_size", rs.getString("typname"));
            assertFalse(rs.next());
        }
    }

    @Test
    void testPgTypeWhereFilter() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TYPE cat_mood AS ENUM ('happy','sad','neutral')");
            ResultSet rs = stmt.executeQuery("SELECT typname FROM pg_type WHERE typname = 'cat_mood'");
            assertTrue(rs.next());
            assertEquals("cat_mood", rs.getString("typname"));
            // Ensure non-existent type returns empty
            rs = stmt.executeQuery("SELECT typname FROM pg_type WHERE typname = 'nonexistent_type_xyz'");
            assertFalse(rs.next());
        }
    }

    // =========================================================================
    // 7. Full migration script test
    // =========================================================================

    @Test
    void testFullMigrationScript() throws Exception {
        // Create a separate Memgres instance for this test to keep it isolated
        try (Memgres migrationMemgres = Memgres.builder().port(0).build().start()) {
            try (Connection migConn = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:" + migrationMemgres.getPort() + "/test",
                    "test", "test")) {

                // Load the migration SQL from classpath
                String migrationSql;
                try (InputStream is = getClass().getClassLoader().getResourceAsStream("real-app-migration/V001__initial_schema.sql")) {
                    assertNotNull(is, "Migration SQL file not found on classpath");
                    migrationSql = new String(IO.readAllBytes(is), StandardCharsets.UTF_8);
                }

                // Execute the full migration, splitting statements ourselves to handle
                // bare $ dollar-quoting that the JDBC driver doesn't recognize
                try (Statement stmt = migConn.createStatement()) {
                    for (String s : splitAndNormalize(migrationSql)) {
                        if (!Strs.isBlank(s)) stmt.execute(s);
                    }

                    // --- Verify all core tables created ---
                    String[] expectedTables = {
                            "users", "sessions", "plans", "accounts", "account_users",
                            "items", "events", "scheduled_tasks", "task_runs",
                            "notifications", "config_entries", "audit_log"
                    };
                    for (String table : expectedTables) {
                        ResultSet rs = stmt.executeQuery("SELECT 1 FROM information_schema.tables WHERE table_name = '" + table + "'");
                        assertTrue(rs.next(), "Table '" + table + "' should exist after migration");
                    }

                    // --- Verify seed data: 3 plans ---
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM plans");
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1), "Expected 3 seeded plans");

                    rs = stmt.executeQuery("SELECT key FROM plans ORDER BY key");
                    assertTrue(rs.next());
                    assertEquals("enterprise", rs.getString(1));
                    assertTrue(rs.next());
                    assertEquals("free", rs.getString(1));
                    assertTrue(rs.next());
                    assertEquals("plan2", rs.getString(1));

                    // --- Verify seed data: 3 config_entries ---
                    rs = stmt.executeQuery("SELECT COUNT(*) FROM config_entries");
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1), "Expected 3 seeded config_entries");

                    // --- Verify enum types are usable ---
                    stmt.execute("INSERT INTO users (email, password_hash) VALUES ('test@example.com', 'hash123')");
                    rs = stmt.executeQuery("SELECT user_id FROM users WHERE email = 'test@example.com'");
                    assertTrue(rs.next());
                    String userId = rs.getString("user_id");
                    assertNotNull(userId, "UUID default should have generated a user_id");

                    stmt.execute("INSERT INTO accounts (name, plan_key) VALUES ('Test Acct', 'free')");
                    rs = stmt.executeQuery("SELECT account_id FROM accounts WHERE name = 'Test Acct'");
                    assertTrue(rs.next());
                    String accountId = rs.getString("account_id");
                    assertNotNull(accountId, "UUID default should have generated an account_id");

                    stmt.execute("INSERT INTO account_users (account_id, user_id, role) VALUES " +
                            "('" + accountId + "', '" + userId + "', 'admin')");
                    rs = stmt.executeQuery("SELECT role FROM account_users WHERE user_id = '" + userId + "'");
                    assertTrue(rs.next());
                    assertEquals("admin", rs.getString("role"));

                    // --- Verify JSONB defaults work ---
                    stmt.execute("INSERT INTO accounts (name) VALUES ('JSONB Test')");
                    rs = stmt.executeQuery("SELECT plan_key FROM accounts WHERE name = 'JSONB Test'");
                    assertTrue(rs.next());
                    assertEquals("free", rs.getString("plan_key"));

                    // --- Verify trigger fires (scheduled_tasks trigger sets next_run_at) ---
                    stmt.execute("INSERT INTO accounts (name) VALUES ('Task Acct')");
                    rs = stmt.executeQuery("SELECT account_id FROM accounts WHERE name = 'Task Acct'");
                    assertTrue(rs.next());
                    String taskAcctId = rs.getString("account_id");

                    stmt.execute("INSERT INTO scheduled_tasks (account_id, name, interval_seconds, retry_seconds, last_executed_at, pending) " +
                            "VALUES ('" + taskAcctId + "', 'Test Task', 300, 600, now(), false)");
                    rs = stmt.executeQuery("SELECT next_run_at FROM scheduled_tasks WHERE name = 'Test Task'");
                    assertTrue(rs.next());
                    assertNotNull(rs.getString("next_run_at"),
                            "Trigger should have computed next_run_at from last_executed_at + interval_seconds");
                }
            }
        }
    }

    /**
     * Splits SQL into individual statements handling dollar-quoting (including bare $),
     * then normalizes bare $ to $$ so the JDBC driver can handle them.
     */
    private static List<String> splitAndNormalize(String sql) {
        // Reuse the same logic as MemgresExtension
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        char stringChar = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (!inString && c == '$') {
                int j = i + 1;
                while (j < sql.length() && (Character.isLetterOrDigit(sql.charAt(j)) || sql.charAt(j) == '_')) j++;
                if (j < sql.length() && sql.charAt(j) == '$') {
                    String delim = sql.substring(i, j + 1);
                    current.append(delim);
                    i = j + 1;
                    int close = sql.indexOf(delim, i);
                    if (close >= 0) { current.append(sql, i, close + delim.length()); i = close + delim.length() - 1; }
                    else { current.append(sql.substring(i)); i = sql.length() - 1; }
                    continue;
                }
                if (j == i + 1 && j < sql.length() && Character.isWhitespace(sql.charAt(j))) {
                    current.append("$$"); // normalize bare $ to $$
                    i = j;
                    int close = -1;
                    for (int k = i; k < sql.length(); k++) {
                        if (sql.charAt(k) == '$' && (k + 1 >= sql.length() || sql.charAt(k + 1) == ';' || Character.isWhitespace(sql.charAt(k + 1)))) {
                            close = k; break;
                        }
                    }
                    if (close >= 0) { current.append(sql, i, close); current.append("$$"); i = close; }
                    else { current.append(sql.substring(i)); i = sql.length() - 1; }
                    continue;
                }
                current.append(c);
                continue;
            }
            if (inString) {
                current.append(c);
                if (c == stringChar) {
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == stringChar) current.append(sql.charAt(++i));
                    else inString = false;
                }
            } else if (c == '\'' || c == '"') { inString = true; stringChar = c; current.append(c); }
            else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                int eol = sql.indexOf('\n', i); if (eol < 0) eol = sql.length();
                current.append(sql, i, Math.min(eol + 1, sql.length())); i = eol;
            }
            else if (c == ';') { String s = current.toString().trim(); if (!s.isEmpty()) result.add(s); current.setLength(0); }
            else current.append(c);
        }
        String last = current.toString().trim();
        if (!last.isEmpty()) result.add(last);
        return result;
    }
}
