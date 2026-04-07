package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Diff #35: INSERT with empty string into nonempty_text domain column succeeds
 * in memgres but PG rejects it. Domain CHECK (VALUE <> '') is not enforced.
 */
class DomainCheckEnforcementTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }
    @AfterAll static void tearDown() throws Exception { if (conn != null) conn.close(); if (memgres != null) memgres.close(); }
    static void exec(String sql) throws SQLException { try (Statement s = conn.createStatement()) { s.execute(sql); } }

    // Exact pattern from 43_schema_evolution_lifecycle.sql
    @Test void domain_check_enforced_on_insert() throws SQLException {
        exec("CREATE SCHEMA dom_test"); exec("SET search_path = dom_test");
        exec("CREATE TABLE evo_t(id int PRIMARY KEY, code text, qty text, created_on text)");
        exec("INSERT INTO evo_t VALUES (1,'A','1','2024-01-01'),(2,'B','2','2024-01-02')");
        exec("ALTER TABLE evo_t ADD COLUMN IF NOT EXISTS active boolean DEFAULT true");
        exec("ALTER TABLE evo_t ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT CURRENT_TIMESTAMP");
        exec("ALTER TABLE evo_t RENAME COLUMN code TO ext_code");
        exec("ALTER TABLE evo_t ALTER COLUMN qty TYPE int USING qty::int");
        exec("ALTER TABLE evo_t ALTER COLUMN created_on TYPE date USING created_on::date");
        exec("CREATE UNIQUE INDEX evo_t_ext_code_uq_idx ON evo_t(ext_code)");
        exec("ALTER TABLE evo_t ADD CONSTRAINT evo_t_ext_code_uq UNIQUE USING INDEX evo_t_ext_code_uq_idx");
        exec("ALTER TABLE evo_t DROP COLUMN IF EXISTS updated_at");
        exec("ALTER TABLE evo_t ADD COLUMN version int DEFAULT 0 NOT NULL");
        exec("ALTER TABLE evo_t ADD COLUMN deleted_at timestamptz");
        exec("CREATE DOMAIN nonempty_text AS text CHECK (VALUE <> '')");
        exec("ALTER TABLE evo_t ADD COLUMN tag nonempty_text DEFAULT 'ok'");
        exec("ALTER DOMAIN nonempty_text ADD CONSTRAINT nonempty_text_len CHECK (char_length(VALUE) <= 10)");
        exec("CREATE TYPE evo_state AS ENUM ('new','active','closed')");
        exec("ALTER TYPE evo_state ADD VALUE 'paused' AFTER 'active'");
        exec("ALTER TABLE evo_t ADD COLUMN state evo_state DEFAULT 'new'");
        exec("UPDATE evo_t SET state = 'paused' WHERE id = 1");
        exec("ALTER TABLE evo_t ADD COLUMN archived boolean NOT NULL DEFAULT false");
        exec("ALTER TABLE evo_t ADD COLUMN updated_by text DEFAULT 'system'");
        try {
            // tag column has domain nonempty_text with CHECK (VALUE <> '')
            // Empty string '' should violate the domain CHECK
            assertThrows(SQLException.class, () -> exec(
                "INSERT INTO evo_t(id, ext_code, qty, created_on, version, tag, state, archived, updated_by) " +
                "VALUES (3, 'C', 3, DATE '2024-01-03', 0, '', 'new', false, 'u')"),
                "Empty string should violate nonempty_text domain CHECK");
        } finally {
            exec("DROP SCHEMA dom_test CASCADE"); exec("SET search_path = public");
        }
    }
}
