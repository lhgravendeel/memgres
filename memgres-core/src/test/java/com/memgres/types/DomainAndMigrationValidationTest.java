package com.memgres.types;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for domain types, sequences, and ALTER TABLE migration patterns.
 *
 * Covers known PG 18 vs Memgres compatibility differences:
 *
 * diff 22, Domain CHECK constraint on INSERT:
 *   The baseline defines a domain posint_array with a CHECK constraint. When inserting
 *   a value that violates the domain check, PG 18 reports:
 *     ERROR: value for domain posint_array violates check constraint "posint_array_check"
 *   with SQLSTATE 23514 (check_violation). Memgres was observed to succeed (false pass).
 *   Test that a violating INSERT raises 23514.
 *
 * diff 40, setval beyond MAXVALUE:
 *   SELECT setval('seq1', 1000, true) on a sequence with MAXVALUE < 1000.
 *   PG 18 gives SQLSTATE 22003 (numeric_value_out_of_range). Memgres succeeds.
 *   Test that setval to a value exceeding MAXVALUE raises 22003.
 *
 * diff 62, ALTER TABLE ADD COLUMN IF NOT EXISTS with bad default type:
 *   ALTER TABLE patch_t ADD COLUMN IF NOT EXISTS bad_col int DEFAULT 'x'
 *   PG 18 gives 22P02 (invalid_text_representation). Memgres succeeds.
 *   Test that a type-incompatible DEFAULT causes 22P02.
 *
 * Additional coverage:
 *   - CREATE DOMAIN basic, DROP DOMAIN
 *   - CREATE DOMAIN with NOT NULL
 *   - Domain used as column type
 *   - ALTER DOMAIN ADD CONSTRAINT, DROP CONSTRAINT
 *   - Domain with DEFAULT value
 *   - CREATE SEQUENCE, ALTER SEQUENCE, DROP SEQUENCE
 *   - nextval, currval, setval
 *   - Sequence CYCLE vs NO CYCLE behavior
 *   - ALTER TABLE ADD COLUMN various types
 *   - ALTER TABLE DROP COLUMN, DROP COLUMN IF EXISTS
 *   - ALTER TABLE ALTER COLUMN TYPE (type migration)
 *   - ALTER TABLE ALTER COLUMN SET DEFAULT, DROP DEFAULT
 *   - ALTER TABLE ADD CONSTRAINT CHECK
 *   - Migration patterns: rename column, add nullable column, backfill, set NOT NULL
 */
class DomainAndMigrationValidationTest {

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
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static List<List<String>> query(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) row.add(rs.getString(i));
                rows.add(row);
            }
            return rows;
        }
    }

    static String scalar(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    // ========================================================================
    // diff 22: Domain CHECK constraint, violating INSERT must raise 23514
    // ========================================================================

    @Test
    void domain_check_violation_on_insert_raises_23514() throws SQLException {
        exec("CREATE DOMAIN posint AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE dom_chk_t(id int PRIMARY KEY, n posint)");
        try {
            // Satisfying value must succeed
            exec("INSERT INTO dom_chk_t VALUES (1, 5)");
            assertEquals("5", scalar("SELECT n FROM dom_chk_t WHERE id = 1"));

            // Violating value must raise 23514
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO dom_chk_t VALUES (2, -1)"));
            assertEquals("23514", ex.getSQLState(),
                    "Domain check violation should raise 23514, got: " + ex.getSQLState());
        } finally {
            exec("DROP TABLE dom_chk_t");
            exec("DROP DOMAIN posint");
        }
    }

    @Test
    void domain_check_violation_zero_raises_23514() throws SQLException {
        exec("CREATE DOMAIN posint2 AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE dom_chk2_t(id int PRIMARY KEY, n posint2)");
        try {
            exec("INSERT INTO dom_chk2_t VALUES (1, 1)");

            // Zero also violates VALUE > 0
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO dom_chk2_t VALUES (2, 0)"));
            assertEquals("23514", ex.getSQLState(),
                    "Zero violates posint domain check, should raise 23514, got: " + ex.getSQLState());
        } finally {
            exec("DROP TABLE dom_chk2_t");
            exec("DROP DOMAIN posint2");
        }
    }

    @Test
    void domain_check_update_violation_raises_23514() throws SQLException {
        exec("CREATE DOMAIN posint3 AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE dom_upd_t(id int PRIMARY KEY, n posint3)");
        try {
            exec("INSERT INTO dom_upd_t VALUES (1, 10)");
            // UPDATE to violating value must also raise 23514
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("UPDATE dom_upd_t SET n = -5 WHERE id = 1"));
            assertEquals("23514", ex.getSQLState(),
                    "Domain check violation on UPDATE should raise 23514, got: " + ex.getSQLState());
            // Original value must be unchanged
            assertEquals("10", scalar("SELECT n FROM dom_upd_t WHERE id = 1"));
        } finally {
            exec("DROP TABLE dom_upd_t");
            exec("DROP DOMAIN posint3");
        }
    }

    // ========================================================================
    // diff 40: setval beyond MAXVALUE must raise 22003
    // ========================================================================

    @Test
    void setval_beyond_maxvalue_raises_22003() throws SQLException {
        exec("CREATE SEQUENCE dmv_seq1 MAXVALUE 100 START 1");
        try {
            // setval within range must succeed
            String r = scalar("SELECT setval('dmv_seq1', 50, true)");
            assertEquals("50", r, "setval within range should return the new value");

            // setval beyond MAXVALUE must raise 22003
            SQLException ex = assertThrows(SQLException.class,
                    () -> scalar("SELECT setval('dmv_seq1', 1000, true)"));
            assertEquals("22003", ex.getSQLState(),
                    "setval beyond MAXVALUE should raise 22003, got: " + ex.getSQLState());
        } finally {
            exec("DROP SEQUENCE dmv_seq1");
        }
    }

    @Test
    void setval_below_minvalue_raises_22003() throws SQLException {
        exec("CREATE SEQUENCE dmv_seq2 MINVALUE 10 START 10");
        try {
            // setval to a value below MINVALUE must raise 22003
            SQLException ex = assertThrows(SQLException.class,
                    () -> scalar("SELECT setval('dmv_seq2', 1, true)"));
            assertEquals("22003", ex.getSQLState(),
                    "setval below MINVALUE should raise 22003, got: " + ex.getSQLState());
        } finally {
            exec("DROP SEQUENCE dmv_seq2");
        }
    }

    // ========================================================================
    // diff 62: ALTER TABLE ADD COLUMN IF NOT EXISTS with bad default, expects 22P02
    // ========================================================================

    @Test
    void alter_add_column_if_not_exists_bad_default_raises_22P02() throws SQLException {
        exec("CREATE TABLE patch_t1(id int PRIMARY KEY)");
        try {
            // Valid default; must succeed
            exec("ALTER TABLE patch_t1 ADD COLUMN IF NOT EXISTS good_col int DEFAULT 42");
            // The table may have no rows; scalar returns null if empty
            // Just verify the column was added by not throwing
            query("SELECT good_col FROM patch_t1 LIMIT 1");

            // Column already exists with IF NOT EXISTS, so it must silently succeed even if default is bad,
            // because the column already exists and nothing is added
            exec("ALTER TABLE patch_t1 ADD COLUMN IF NOT EXISTS good_col int DEFAULT 'x'");

            // New column name with bad default type must raise 22P02
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("ALTER TABLE patch_t1 ADD COLUMN IF NOT EXISTS bad_col int DEFAULT 'x'"));
            assertEquals("22P02", ex.getSQLState(),
                    "ADD COLUMN IF NOT EXISTS with invalid default should raise 22P02, got: " + ex.getSQLState());
        } finally {
            exec("DROP TABLE patch_t1");
        }
    }

    @Test
    void alter_add_column_bad_default_raises_22P02() throws SQLException {
        exec("CREATE TABLE patch_t2(id int PRIMARY KEY)");
        try {
            // Without IF NOT EXISTS, must also raise 22P02 for bad default
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("ALTER TABLE patch_t2 ADD COLUMN bad_col int DEFAULT 'x'"));
            assertEquals("22P02", ex.getSQLState(),
                    "ADD COLUMN with invalid default should raise 22P02, got: " + ex.getSQLState());
        } finally {
            exec("DROP TABLE patch_t2");
        }
    }

    // ========================================================================
    // Domain: basic CREATE / DROP
    // ========================================================================

    @Test
    void create_and_drop_domain() throws SQLException {
        exec("CREATE DOMAIN simple_dom AS text");
        exec("DROP DOMAIN simple_dom");
        // After DROP, the domain must no longer exist; recreating should succeed
        exec("CREATE DOMAIN simple_dom AS text");
        exec("DROP DOMAIN simple_dom");
    }

    @Test
    void create_domain_if_not_exists_idempotent() throws SQLException {
        exec("CREATE DOMAIN idem_dom AS int");
        try {
            // Duplicate without IF NOT EXISTS must give 42710
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("CREATE DOMAIN idem_dom AS int"));
            assertEquals("42710", ex.getSQLState(),
                    "Duplicate domain should raise 42710, got: " + ex.getSQLState());
        } finally {
            exec("DROP DOMAIN idem_dom");
        }
    }

    @Test
    void drop_domain_if_exists_no_error_when_missing() throws SQLException {
        exec("DROP DOMAIN IF EXISTS nonexistent_dom");
        // No exception; test passes if we reach here
    }

    // ========================================================================
    // Domain: NOT NULL constraint
    // ========================================================================

    @Test
    void domain_not_null_rejects_null_on_insert() throws SQLException {
        exec("CREATE DOMAIN notnull_dom AS int NOT NULL");
        exec("CREATE TABLE dom_nn_t(id int PRIMARY KEY, v notnull_dom)");
        try {
            exec("INSERT INTO dom_nn_t VALUES (1, 7)");
            assertEquals("7", scalar("SELECT v FROM dom_nn_t WHERE id = 1"));

            // NULL must be rejected with 23502 (not_null_violation)
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO dom_nn_t VALUES (2, NULL)"));
            assertTrue(
                    ex.getSQLState().equals("23502") || ex.getSQLState().equals("23514"),
                    "NULL in NOT NULL domain should raise 23502 or 23514, got: " + ex.getSQLState());
        } finally {
            exec("DROP TABLE dom_nn_t");
            exec("DROP DOMAIN notnull_dom");
        }
    }

    // ========================================================================
    // Domain: used as column type
    // ========================================================================

    @Test
    void domain_as_column_type_accepts_valid_values() throws SQLException {
        exec("CREATE DOMAIN email_dom AS text CHECK (VALUE LIKE '%@%')");
        exec("CREATE TABLE dom_col_t(id int PRIMARY KEY, email email_dom)");
        try {
            exec("INSERT INTO dom_col_t VALUES (1, 'user@example.com')");
            assertEquals("user@example.com", scalar("SELECT email FROM dom_col_t WHERE id = 1"));

            // Invalid email must raise 23514
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO dom_col_t VALUES (2, 'notanemail')"));
            assertEquals("23514", ex.getSQLState(),
                    "Invalid email domain should raise 23514, got: " + ex.getSQLState());
        } finally {
            exec("DROP TABLE dom_col_t");
            exec("DROP DOMAIN email_dom");
        }
    }

    // ========================================================================
    // Domain: ALTER DOMAIN ADD / DROP CONSTRAINT
    // ========================================================================

    @Test
    void alter_domain_add_and_drop_constraint() throws SQLException {
        exec("CREATE DOMAIN altdom AS int");
        exec("CREATE TABLE altdom_t(id int PRIMARY KEY, v altdom)");
        try {
            // Insert a row that will later violate the new constraint
            exec("INSERT INTO altdom_t VALUES (1, -5)");

            // Adding a constraint that the existing row violates must fail
            assertThrows(SQLException.class,
                    () -> exec("ALTER DOMAIN altdom ADD CONSTRAINT altdom_pos CHECK (VALUE > 0)"));

            // Clean up the offending row and retry
            exec("DELETE FROM altdom_t WHERE id = 1");
            exec("ALTER DOMAIN altdom ADD CONSTRAINT altdom_pos CHECK (VALUE > 0)");

            // Now inserting a valid value must succeed
            exec("INSERT INTO altdom_t VALUES (2, 10)");

            // And a violating value must fail
            SQLException ex2 = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO altdom_t VALUES (3, -1)"));
            assertEquals("23514", ex2.getSQLState(),
                    "Constraint violation should be 23514 after ADD CONSTRAINT, got: " + ex2.getSQLState());

            // DROP CONSTRAINT; inserting negative should succeed again
            exec("ALTER DOMAIN altdom DROP CONSTRAINT altdom_pos");
            exec("INSERT INTO altdom_t VALUES (4, -99)");
            assertEquals("-99", scalar("SELECT v FROM altdom_t WHERE id = 4"));
        } finally {
            exec("DROP TABLE altdom_t");
            exec("DROP DOMAIN altdom");
        }
    }

    // ========================================================================
    // Domain: DEFAULT value
    // ========================================================================

    @Test
    void domain_with_default_value() throws SQLException {
        exec("CREATE DOMAIN score_dom AS int DEFAULT 0 CHECK (VALUE >= 0)");
        exec("CREATE TABLE dom_def_t(id int PRIMARY KEY, score score_dom)");
        try {
            // Insert without providing the domain column; default should apply
            exec("INSERT INTO dom_def_t(id) VALUES (1)");
            String v = scalar("SELECT score FROM dom_def_t WHERE id = 1");
            // Default from domain should be 0
            assertEquals("0", v, "Domain DEFAULT should supply 0 when column omitted");

            // Override with explicit value
            exec("INSERT INTO dom_def_t VALUES (2, 42)");
            assertEquals("42", scalar("SELECT score FROM dom_def_t WHERE id = 2"));
        } finally {
            exec("DROP TABLE dom_def_t");
            exec("DROP DOMAIN score_dom");
        }
    }

    // ========================================================================
    // Sequences: CREATE, ALTER, DROP
    // ========================================================================

    @Test
    void create_alter_drop_sequence() throws SQLException {
        exec("CREATE SEQUENCE seq_basic START 1 INCREMENT 1");
        try {
            String v1 = scalar("SELECT nextval('seq_basic')");
            assertEquals("1", v1, "First nextval should be 1");

            String v2 = scalar("SELECT nextval('seq_basic')");
            assertEquals("2", v2, "Second nextval should be 2");

            exec("ALTER SEQUENCE seq_basic INCREMENT BY 5");
            String v3 = scalar("SELECT nextval('seq_basic')");
            assertEquals("7", v3, "After INCREMENT BY 5, next value should be 7");
        } finally {
            exec("DROP SEQUENCE seq_basic");
        }
    }

    @Test
    void drop_sequence_if_exists_no_error_when_missing() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS nonexistent_seq");
    }

    @Test
    void create_sequence_with_explicit_bounds() throws SQLException {
        exec("CREATE SEQUENCE seq_bounded MINVALUE 10 MAXVALUE 20 START 10");
        try {
            assertEquals("10", scalar("SELECT nextval('seq_bounded')"));
            assertEquals("11", scalar("SELECT nextval('seq_bounded')"));
        } finally {
            exec("DROP SEQUENCE seq_bounded");
        }
    }

    // ========================================================================
    // Sequences: nextval, currval, setval
    // ========================================================================

    @Test
    void nextval_currval_setval_basic() throws SQLException {
        exec("CREATE SEQUENCE seq_ncs START 100");
        try {
            assertEquals("100", scalar("SELECT nextval('seq_ncs')"));
            assertEquals("100", scalar("SELECT currval('seq_ncs')"));

            scalar("SELECT setval('seq_ncs', 200, true)");
            assertEquals("200", scalar("SELECT currval('seq_ncs')"));

            // setval with called=false, so nextval should return the set value
            scalar("SELECT setval('seq_ncs', 300, false)");
            assertEquals("300", scalar("SELECT nextval('seq_ncs')"));
        } finally {
            exec("DROP SEQUENCE seq_ncs");
        }
    }

    @Test
    void currval_before_nextval_raises_error() throws SQLException {
        exec("CREATE SEQUENCE seq_cv_err START 1");
        try {
            // currval before any nextval in this session must raise an error
            assertThrows(SQLException.class,
                    () -> scalar("SELECT currval('seq_cv_err')"),
                    "currval before nextval should raise an error");
        } finally {
            exec("DROP SEQUENCE seq_cv_err");
        }
    }

    // ========================================================================
    // Sequences: CYCLE vs NO CYCLE behavior
    // ========================================================================

    @Test
    void sequence_no_cycle_exceeding_maxvalue_raises_error() throws SQLException {
        exec("CREATE SEQUENCE seq_nocycle MAXVALUE 3 START 1 NO CYCLE");
        try {
            assertEquals("1", scalar("SELECT nextval('seq_nocycle')"));
            assertEquals("2", scalar("SELECT nextval('seq_nocycle')"));
            assertEquals("3", scalar("SELECT nextval('seq_nocycle')"));
            // Beyond MAXVALUE with NO CYCLE must error
            assertThrows(SQLException.class,
                    () -> scalar("SELECT nextval('seq_nocycle')"),
                    "nextval beyond MAXVALUE with NO CYCLE should raise an error");
        } finally {
            exec("DROP SEQUENCE seq_nocycle");
        }
    }

    @Test
    void sequence_cycle_wraps_around() throws SQLException {
        exec("CREATE SEQUENCE seq_cycle MINVALUE 1 MAXVALUE 3 START 1 CYCLE");
        try {
            assertEquals("1", scalar("SELECT nextval('seq_cycle')"));
            assertEquals("2", scalar("SELECT nextval('seq_cycle')"));
            assertEquals("3", scalar("SELECT nextval('seq_cycle')"));
            // Should wrap back to MINVALUE
            assertEquals("1", scalar("SELECT nextval('seq_cycle')"),
                    "CYCLE sequence should wrap back to MINVALUE after MAXVALUE");
        } finally {
            exec("DROP SEQUENCE seq_cycle");
        }
    }

    // ========================================================================
    // ALTER TABLE: ADD COLUMN various types
    // ========================================================================

    @Test
    void alter_add_column_various_types() throws SQLException {
        exec("CREATE TABLE acv_t(id int PRIMARY KEY)");
        try {
            exec("INSERT INTO acv_t VALUES (1)");

            exec("ALTER TABLE acv_t ADD COLUMN c_text text");
            exec("ALTER TABLE acv_t ADD COLUMN c_bool boolean DEFAULT false");
            exec("ALTER TABLE acv_t ADD COLUMN c_ts timestamptz DEFAULT now()");
            exec("ALTER TABLE acv_t ADD COLUMN c_num numeric(10,2) DEFAULT 0.00");

            List<List<String>> rows = query("SELECT id, c_text, c_bool, c_num FROM acv_t");
            assertEquals(1, rows.size());
            assertNull(rows.get(0).get(1), "c_text should be NULL for existing row");
            // PG text protocol returns 'f' for false
            assertTrue("false".equals(rows.get(0).get(2)) || "f".equals(rows.get(0).get(2)),
                    "c_bool should default to false, got: " + rows.get(0).get(2));
            assertEquals("0.00", rows.get(0).get(3), "c_num should default to 0.00");
        } finally {
            exec("DROP TABLE acv_t");
        }
    }

    @Test
    void alter_add_column_if_not_exists_idempotent() throws SQLException {
        exec("CREATE TABLE acif_t(id int PRIMARY KEY, existing_col text)");
        try {
            // Adding existing column with IF NOT EXISTS must silently succeed
            exec("ALTER TABLE acif_t ADD COLUMN IF NOT EXISTS existing_col text");
            // Adding new column with IF NOT EXISTS must succeed
            exec("ALTER TABLE acif_t ADD COLUMN IF NOT EXISTS new_col int");

            List<List<String>> rows = query("SELECT column_name FROM information_schema.columns WHERE table_name = 'acif_t' ORDER BY ordinal_position");
            // id, existing_col, new_col
            assertEquals(3, rows.size(), "Table should have exactly 3 columns");
        } finally {
            exec("DROP TABLE acif_t");
        }
    }

    // ========================================================================
    // ALTER TABLE: DROP COLUMN, DROP COLUMN IF EXISTS
    // ========================================================================

    @Test
    void alter_drop_column() throws SQLException {
        exec("CREATE TABLE drc_t(id int PRIMARY KEY, a int, b text, c boolean)");
        exec("INSERT INTO drc_t VALUES (1, 10, 'hello', true)");
        try {
            exec("ALTER TABLE drc_t DROP COLUMN b");

            List<List<String>> rows = query("SELECT id, a, c FROM drc_t");
            assertEquals(1, rows.size());
            assertEquals("1", rows.get(0).get(0));
            assertEquals("10", rows.get(0).get(1));
            assertTrue("true".equals(rows.get(0).get(2)) || "t".equals(rows.get(0).get(2)),
                    "c should be true, got: " + rows.get(0).get(2));

            // Trying to SELECT the dropped column must fail
            assertThrows(SQLException.class,
                    () -> query("SELECT b FROM drc_t"),
                    "Selecting dropped column should raise an error");
        } finally {
            exec("DROP TABLE drc_t");
        }
    }

    @Test
    void alter_drop_column_if_exists_no_error_when_missing() throws SQLException {
        exec("CREATE TABLE drcif_t(id int PRIMARY KEY, a int)");
        try {
            exec("ALTER TABLE drcif_t DROP COLUMN IF EXISTS nonexistent_col");
            // No exception expected
        } finally {
            exec("DROP TABLE drcif_t");
        }
    }

    @Test
    void alter_drop_nonexistent_column_without_if_exists_raises_error() throws SQLException {
        exec("CREATE TABLE drcne_t(id int PRIMARY KEY, a int)");
        try {
            assertThrows(SQLException.class,
                    () -> exec("ALTER TABLE drcne_t DROP COLUMN nonexistent_col"),
                    "Dropping non-existent column without IF EXISTS should raise an error");
        } finally {
            exec("DROP TABLE drcne_t");
        }
    }

    // ========================================================================
    // ALTER TABLE: ALTER COLUMN TYPE (type migration)
    // ========================================================================

    @Test
    void alter_column_type_int_to_bigint() throws SQLException {
        exec("CREATE TABLE act_t(id int PRIMARY KEY, val int)");
        exec("INSERT INTO act_t VALUES (1, 42)");
        try {
            exec("ALTER TABLE act_t ALTER COLUMN val TYPE bigint");
            assertEquals("42", scalar("SELECT val FROM act_t WHERE id = 1"));
        } finally {
            exec("DROP TABLE act_t");
        }
    }

    @Test
    void alter_column_type_with_using_clause() throws SQLException {
        exec("CREATE TABLE actu_t(id int PRIMARY KEY, val text)");
        exec("INSERT INTO actu_t VALUES (1, '123')");
        try {
            exec("ALTER TABLE actu_t ALTER COLUMN val TYPE int USING val::int");
            assertEquals("123", scalar("SELECT val FROM actu_t WHERE id = 1"));
        } finally {
            exec("DROP TABLE actu_t");
        }
    }

    @Test
    void alter_column_type_incompatible_without_using_raises_error() throws SQLException {
        exec("CREATE TABLE acti_t(id int PRIMARY KEY, val text)");
        exec("INSERT INTO acti_t VALUES (1, 'not_a_number')");
        try {
            // Without USING, casting text → int for non-numeric data must fail
            assertThrows(SQLException.class,
                    () -> exec("ALTER TABLE acti_t ALTER COLUMN val TYPE int"),
                    "Incompatible type change without USING should raise an error");
        } finally {
            exec("DROP TABLE acti_t");
        }
    }

    // ========================================================================
    // ALTER TABLE: ALTER COLUMN SET DEFAULT, DROP DEFAULT
    // ========================================================================

    @Test
    void alter_column_set_and_drop_default() throws SQLException {
        exec("CREATE TABLE acd_t(id int PRIMARY KEY, val int)");
        exec("INSERT INTO acd_t VALUES (1, 10)");
        try {
            exec("ALTER TABLE acd_t ALTER COLUMN val SET DEFAULT 99");
            // New row should pick up the default
            exec("INSERT INTO acd_t(id) VALUES (2)");
            assertEquals("99", scalar("SELECT val FROM acd_t WHERE id = 2"));

            exec("ALTER TABLE acd_t ALTER COLUMN val DROP DEFAULT");
            // New row without explicit value; should be NULL now
            exec("INSERT INTO acd_t(id) VALUES (3)");
            assertNull(scalar("SELECT val FROM acd_t WHERE id = 3"),
                    "After DROP DEFAULT, omitted column should be NULL");
        } finally {
            exec("DROP TABLE acd_t");
        }
    }

    // ========================================================================
    // ALTER TABLE: ADD CONSTRAINT CHECK
    // ========================================================================

    @Test
    void alter_add_check_constraint() throws SQLException {
        exec("CREATE TABLE acc_t(id int PRIMARY KEY, age int)");
        exec("INSERT INTO acc_t VALUES (1, 25)");
        try {
            exec("ALTER TABLE acc_t ADD CONSTRAINT age_positive CHECK (age > 0)");

            // Valid insert must succeed
            exec("INSERT INTO acc_t VALUES (2, 30)");

            // Violating insert must raise 23514
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO acc_t VALUES (3, -1)"));
            assertEquals("23514", ex.getSQLState(),
                    "Check constraint violation should raise 23514, got: " + ex.getSQLState());
        } finally {
            exec("DROP TABLE acc_t");
        }
    }

    @Test
    void alter_add_check_constraint_fails_on_existing_violation() throws SQLException {
        exec("CREATE TABLE accv_t(id int PRIMARY KEY, age int)");
        exec("INSERT INTO accv_t VALUES (1, -5)");
        try {
            // Adding constraint that existing row violates must fail
            assertThrows(SQLException.class,
                    () -> exec("ALTER TABLE accv_t ADD CONSTRAINT age_pos CHECK (age > 0)"),
                    "Adding check constraint that existing row violates should fail");
        } finally {
            exec("DROP TABLE accv_t");
        }
    }

    // ========================================================================
    // Migration pattern: rename column
    // ========================================================================

    @Test
    void migration_rename_column() throws SQLException {
        exec("CREATE TABLE mc_rename_t(id int PRIMARY KEY, old_name text)");
        exec("INSERT INTO mc_rename_t VALUES (1, 'hello')");
        try {
            exec("ALTER TABLE mc_rename_t RENAME COLUMN old_name TO new_name");
            assertEquals("hello", scalar("SELECT new_name FROM mc_rename_t WHERE id = 1"));

            // Old name must no longer be accessible
            assertThrows(SQLException.class,
                    () -> scalar("SELECT old_name FROM mc_rename_t"),
                    "Old column name should not be accessible after RENAME");
        } finally {
            exec("DROP TABLE mc_rename_t");
        }
    }

    // ========================================================================
    // Migration pattern: add nullable column, backfill, set NOT NULL
    // ========================================================================

    @Test
    void migration_add_nullable_backfill_set_not_null() throws SQLException {
        exec("CREATE TABLE mc_nn_t(id int PRIMARY KEY, name text)");
        exec("INSERT INTO mc_nn_t VALUES (1, 'Alice'), (2, 'Bob')");
        try {
            // Step 1: add nullable column
            exec("ALTER TABLE mc_nn_t ADD COLUMN score int");
            // Existing rows should have NULL
            assertNull(scalar("SELECT score FROM mc_nn_t WHERE id = 1"),
                    "Newly added column should be NULL for existing rows");

            // Step 2: backfill
            exec("UPDATE mc_nn_t SET score = 100 WHERE score IS NULL");
            assertEquals("100", scalar("SELECT score FROM mc_nn_t WHERE id = 1"));
            assertEquals("100", scalar("SELECT score FROM mc_nn_t WHERE id = 2"));

            // Step 3: set NOT NULL
            exec("ALTER TABLE mc_nn_t ALTER COLUMN score SET NOT NULL");

            // Inserting NULL now must fail (23502)
            SQLException ex = assertThrows(SQLException.class,
                    () -> exec("INSERT INTO mc_nn_t VALUES (3, 'Charlie', NULL)"));
            assertEquals("23502", ex.getSQLState(),
                    "NOT NULL violation should raise 23502, got: " + ex.getSQLState());

            // Valid insert must succeed
            exec("INSERT INTO mc_nn_t VALUES (3, 'Charlie', 200)");
            assertEquals("200", scalar("SELECT score FROM mc_nn_t WHERE id = 3"));
        } finally {
            exec("DROP TABLE mc_nn_t");
        }
    }

    // ========================================================================
    // Migration pattern: full column lifecycle in a single table
    // ========================================================================

    @Test
    void migration_full_column_lifecycle() throws SQLException {
        exec("CREATE TABLE mc_full_t(id int PRIMARY KEY, a text)");
        exec("INSERT INTO mc_full_t VALUES (1, 'v1'), (2, 'v2')");
        try {
            // Add column with default
            exec("ALTER TABLE mc_full_t ADD COLUMN b int DEFAULT 0");
            assertEquals("0", scalar("SELECT b FROM mc_full_t WHERE id = 1"));

            // Change default
            exec("ALTER TABLE mc_full_t ALTER COLUMN b SET DEFAULT 999");
            exec("INSERT INTO mc_full_t(id, a) VALUES (3, 'v3')");
            assertEquals("999", scalar("SELECT b FROM mc_full_t WHERE id = 3"));

            // Rename the column
            exec("ALTER TABLE mc_full_t RENAME COLUMN b TO b_renamed");
            assertEquals("999", scalar("SELECT b_renamed FROM mc_full_t WHERE id = 3"));

            // Change type
            exec("ALTER TABLE mc_full_t ALTER COLUMN b_renamed TYPE bigint");
            assertEquals("999", scalar("SELECT b_renamed FROM mc_full_t WHERE id = 3"));

            // Drop default
            exec("ALTER TABLE mc_full_t ALTER COLUMN b_renamed DROP DEFAULT");
            exec("INSERT INTO mc_full_t(id, a) VALUES (4, 'v4')");
            assertNull(scalar("SELECT b_renamed FROM mc_full_t WHERE id = 4"),
                    "Column should be NULL after DROP DEFAULT");

            // Drop column
            exec("ALTER TABLE mc_full_t DROP COLUMN b_renamed");
            List<List<String>> rows = query("SELECT id, a FROM mc_full_t ORDER BY id");
            assertEquals(4, rows.size());
        } finally {
            exec("DROP TABLE mc_full_t");
        }
    }
}
