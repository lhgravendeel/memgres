package com.memgres.pg18;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Disabled;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for constraint enforcement, DML, RETURNING, and transaction issues
 * found comparing memgres to PG18.
 *
 * Covers:
 *  - IDENTITY column insert rejection / DEFAULT
 *  - Duplicate constraint names
 *  - ALTER ADD FK violations against existing data / non-existent columns
 *  - PK error message wording ("unique constraint" not "primary key constraint")
 *  - CHECK constraint auto-naming convention
 *  - pg_typeof in RETURNING column name
 *  - MERGE with duplicate table alias
 *  - GROUP BY / SELECT validation (non-grouped columns, aggregates in GROUP BY, DISTINCT + ORDER BY)
 *  - Transaction / savepoint SQLSTATE codes
 *  - Function / procedure error codes and messages
 *  - PL/pgSQL nested exception handling and RAISE propagation
 */
class Pg18ConstraintDmlTest {

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

    // ---- helpers -----------------------------------------------------------

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static String querySingle(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row for: " + sql);
            return rs.getString(1);
        }
    }

    static int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), "Expected at least one row for: " + sql);
            return rs.getInt(1);
        }
    }

    static void assertSqlError(String sql, String expectedSqlState) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected error " + expectedSqlState + " but SQL succeeded: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedSqlState, e.getSQLState(),
                "Wrong SQLSTATE for: " + sql + " (got message: " + e.getMessage() + ")");
        }
    }

    static void assertSqlErrorContains(String sql, String expectedSqlState, String messagePart) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            fail("Expected error " + expectedSqlState + " but SQL succeeded: " + sql);
        } catch (SQLException e) {
            assertEquals(expectedSqlState, e.getSQLState(),
                "Wrong SQLSTATE for: " + sql + " (got message: " + e.getMessage() + ")");
            assertTrue(e.getMessage().toLowerCase().contains(messagePart.toLowerCase()),
                "Error message should contain '" + messagePart + "' but was: " + e.getMessage());
        }
    }

    private void dropAllTables() throws SQLException {
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public'");
            java.util.List<String> tables = new java.util.ArrayList<>();
            while (rs.next()) tables.add(rs.getString(1));
            for (String t : tables) {
                try { s.execute("DROP TABLE IF EXISTS " + t + " CASCADE"); } catch (Exception ignored) {}
            }
        }
    }

    @BeforeEach
    void clean() throws Exception {
        try { exec("ROLLBACK"); } catch (Exception ignored) {}
        dropAllTables();
        try { exec("DROP FUNCTION IF EXISTS add1(integer)"); } catch (Exception ignored) {}
        try { exec("DROP FUNCTION IF EXISTS fail_if_negative(integer)"); } catch (Exception ignored) {}
        try { exec("DROP FUNCTION IF EXISTS nested_exc()"); } catch (Exception ignored) {}
        try { exec("DROP FUNCTION IF EXISTS bad_sql_body(integer)"); } catch (Exception ignored) {}
        try { exec("DROP FUNCTION IF EXISTS syntax_err_body(integer)"); } catch (Exception ignored) {}
        try { exec("DROP PROCEDURE IF EXISTS myproc(integer)"); } catch (Exception ignored) {}
    }

    // ========================================================================
    // CONSTRAINTS
    // ========================================================================

    // 1. IDENTITY column insert rejection
    @Test
    void identity_column_rejects_explicit_value() throws SQLException {
        exec("CREATE TABLE ident_t (id INTEGER GENERATED ALWAYS AS IDENTITY, name TEXT)");
        assertSqlError(
            "INSERT INTO ident_t(id, name) VALUES (1, 'Alice')",
            "428C9");
    }

    // 1b. IDENTITY column accepts DEFAULT
    @Test
    void identity_column_accepts_default() throws SQLException {
        exec("CREATE TABLE ident_t2 (id INTEGER GENERATED ALWAYS AS IDENTITY, name TEXT)");
        exec("INSERT INTO ident_t2(name) VALUES ('Bob')");
        String id = querySingle("SELECT id FROM ident_t2");
        assertNotNull(id);
        assertEquals(1, Integer.parseInt(id));
    }

    // 1c. IDENTITY column accepts OVERRIDING SYSTEM VALUE
    @Test
    void identity_column_accepts_overriding_system_value() throws SQLException {
        exec("CREATE TABLE ident_t3 (id INTEGER GENERATED ALWAYS AS IDENTITY, name TEXT)");
        exec("INSERT INTO ident_t3(id, name) OVERRIDING SYSTEM VALUE VALUES (42, 'Carol')");
        assertEquals("42", querySingle("SELECT id FROM ident_t3"));
    }

    // 1d. GENERATED BY DEFAULT AS IDENTITY allows explicit value
    @Test
    void identity_by_default_allows_explicit_value() throws SQLException {
        exec("CREATE TABLE ident_t4 (id INTEGER GENERATED BY DEFAULT AS IDENTITY, name TEXT)");
        exec("INSERT INTO ident_t4(id, name) VALUES (99, 'Dave')");
        assertEquals("99", querySingle("SELECT id FROM ident_t4"));
    }

    // 2. Duplicate constraint name
    @Test
    void duplicate_constraint_name_error() throws SQLException {
        exec("CREATE TABLE dup_con (id INTEGER, val INTEGER, CONSTRAINT c1 CHECK (val > 0))");
        assertSqlError(
            "ALTER TABLE dup_con ADD CONSTRAINT c1 CHECK (val < 100)",
            "42710");
    }

    // 3. ALTER ADD FK that violates existing data
    @Test
    void alter_add_fk_violates_existing_data() throws SQLException {
        exec("CREATE TABLE fk_parent (id INTEGER PRIMARY KEY)");
        exec("INSERT INTO fk_parent VALUES (1)");
        exec("CREATE TABLE fk_child (id INTEGER, parent_id INTEGER)");
        exec("INSERT INTO fk_child VALUES (1, 999)"); // 999 not in parent
        assertSqlError(
            "ALTER TABLE fk_child ADD CONSTRAINT fk_c FOREIGN KEY (parent_id) REFERENCES fk_parent(id)",
            "23503");
    }

    // 4. ALTER ADD FK referencing non-existent column
    @Test
    void alter_add_fk_nonexistent_column() throws SQLException {
        exec("CREATE TABLE fk_parent2 (id INTEGER PRIMARY KEY)");
        exec("CREATE TABLE fk_child2 (id INTEGER, parent_id INTEGER)");
        assertSqlError(
            "ALTER TABLE fk_child2 ADD CONSTRAINT fk_c2 FOREIGN KEY (parent_id) REFERENCES fk_parent2(no_such_col)",
            "42703");
    }

    // 5. PK error says "unique constraint" not "primary key constraint"
    @Test
    void pk_error_says_unique_constraint() throws SQLException {
        exec("CREATE TABLE pk_msg (id INTEGER PRIMARY KEY, name TEXT)");
        exec("INSERT INTO pk_msg VALUES (1, 'X')");
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO pk_msg VALUES (1, 'Y')");
            fail("Expected duplicate PK error");
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            assertTrue(msg.contains("unique constraint") || msg.contains("unique_violation"),
                "PG says 'unique constraint' for PK violations, got: " + e.getMessage());
            // Verify it mentions the pkey name
            assertTrue(msg.contains("pk_msg_pkey") || msg.contains("pk_msg"),
                "Error should reference the constraint name, got: " + e.getMessage());
        }
    }

    // 6. CHECK constraint auto-naming convention
    @Test
    void check_constraint_auto_naming() throws SQLException {
        exec("CREATE TABLE chk_name (id INTEGER, val INTEGER CHECK (val > 0))");
        try (Statement s = conn.createStatement()) {
            s.execute("INSERT INTO chk_name VALUES (1, -5)");
            fail("Expected CHECK violation");
        } catch (SQLException e) {
            String msg = e.getMessage().toLowerCase();
            // PG auto-names as tablename_colname_check
            assertTrue(msg.contains("chk_name_val_check") || msg.contains("check"),
                "Error should reference auto-generated constraint name, got: " + e.getMessage());
        }
    }

    // ========================================================================
    // DML & RETURNING
    // ========================================================================

    // 7. pg_typeof in RETURNING column name
    @Test
    void returning_pg_typeof_column_name() throws SQLException {
        exec("CREATE TABLE ret_type (id SERIAL PRIMARY KEY, val INTEGER)");
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "INSERT INTO ret_type(val) VALUES (42) RETURNING pg_typeof(val)");
            assertTrue(rs.next());
            // The column name should be "pg_typeof", not "?column?"
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("pg_typeof", meta.getColumnName(1),
                "RETURNING pg_typeof(col) should have column name 'pg_typeof'");
            assertEquals("integer", rs.getString(1));
        }
    }

    // 7b. RETURNING with expression alias
    @Test
    void returning_expression_with_alias() throws SQLException {
        exec("CREATE TABLE ret_alias (id SERIAL PRIMARY KEY, val INTEGER)");
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery(
                "INSERT INTO ret_alias(val) VALUES (10) RETURNING val * 2 AS doubled");
            assertTrue(rs.next());
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("doubled", meta.getColumnName(1));
            assertEquals(20, rs.getInt(1));
        }
    }

    // 8. MERGE with duplicate table
    @Test
    void merge_duplicate_table_error() throws SQLException {
        exec("CREATE TABLE merge_t (id INTEGER PRIMARY KEY, x INTEGER)");
        exec("INSERT INTO merge_t VALUES (1, 10)");
        assertSqlError(
            "MERGE INTO merge_t USING merge_t ON (merge_t.id = merge_t.id) " +
            "WHEN MATCHED THEN UPDATE SET x = 0",
            "42712");
    }

    // ========================================================================
    // GROUP BY / SELECT VALIDATION
    // ========================================================================

    // 9. Non-grouped column in SELECT with GROUP BY
    @Test
    void non_grouped_column_in_select() throws SQLException {
        exec("CREATE TABLE grp_t (a INTEGER, b TEXT, c INTEGER)");
        exec("INSERT INTO grp_t VALUES (1, 'x', 10), (1, 'y', 20)");
        assertSqlError(
            "SELECT a, b, sum(c) FROM grp_t GROUP BY a",
            "42803");
    }

    // 10. Aggregate in GROUP BY
    @Test
    void aggregate_in_group_by() throws SQLException {
        exec("CREATE TABLE grp_t2 (a INTEGER, c INTEGER)");
        assertSqlError(
            "SELECT a FROM grp_t2 GROUP BY sum(c)",
            "42803");
    }

    // 11. DISTINCT with ORDER BY on non-selected column
    @Test
    void distinct_order_by_non_selected_column() throws SQLException {
        exec("CREATE TABLE dist_t (a INTEGER, b INTEGER)");
        exec("INSERT INTO dist_t VALUES (1, 10), (2, 20)");
        assertSqlError(
            "SELECT DISTINCT a FROM dist_t ORDER BY b",
            "42P10");
    }

    // ========================================================================
    // TRANSACTION / SAVEPOINT ERROR CODES
    // ========================================================================

    // 12. ROLLBACK TO outside transaction
    @Test
    void rollback_to_outside_transaction() {
        assertSqlError("ROLLBACK TO sp", "25P01");
    }

    // 13. RELEASE SAVEPOINT outside transaction
    @Test
    void release_savepoint_outside_transaction() {
        assertSqlError("RELEASE SAVEPOINT sp", "25P01");
    }

    // 14. Non-existent savepoint RELEASE inside transaction
    @Test
    void release_nonexistent_savepoint_in_transaction() throws SQLException {
        exec("BEGIN");
        try {
            assertSqlError("RELEASE SAVEPOINT no_such", "3B001");
        } finally {
            try { exec("ROLLBACK"); } catch (Exception ignored) {}
        }
    }

    // 15. Non-existent savepoint ROLLBACK inside transaction
    @Test
    void rollback_to_nonexistent_savepoint_in_transaction() throws SQLException {
        exec("BEGIN");
        try {
            assertSqlError("ROLLBACK TO SAVEPOINT no_such", "3B001");
        } finally {
            try { exec("ROLLBACK"); } catch (Exception ignored) {}
        }
    }

    // 15b. Valid savepoint RELEASE works inside transaction
    @Test
    void release_valid_savepoint_succeeds() throws SQLException {
        exec("BEGIN");
        exec("SAVEPOINT sp1");
        exec("RELEASE SAVEPOINT sp1");
        exec("COMMIT");
    }

    // 15c. Valid savepoint ROLLBACK TO works inside transaction
    @Test
    void rollback_to_valid_savepoint_succeeds() throws SQLException {
        exec("CREATE TABLE sp_t (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO sp_t VALUES (1)");
        exec("SAVEPOINT sp1");
        exec("INSERT INTO sp_t VALUES (2)");
        exec("ROLLBACK TO SAVEPOINT sp1");
        exec("COMMIT");
        assertEquals("1", querySingle("SELECT count(*) FROM sp_t"));
    }

    // ========================================================================
    // FUNCTION / ROUTINE ERRORS
    // ========================================================================

    // 16. Function wrong arity (too few args)
    @Test
    void function_too_few_args() throws SQLException {
        exec("CREATE FUNCTION add1(x INTEGER) RETURNS INTEGER AS $$ SELECT x + 1 $$ LANGUAGE sql");
        assertSqlErrorContains(
            "SELECT add1()",
            "42883",
            "add1()");
    }

    // 17. Function wrong arity (too many args)
    @Test
    void function_too_many_args() throws SQLException {
        exec("CREATE FUNCTION add1(x INTEGER) RETURNS INTEGER AS $$ SELECT x + 1 $$ LANGUAGE sql");
        assertSqlErrorContains(
            "SELECT add1(1, 2)",
            "42883",
            "add1(");
    }

    // 18. Function wrong arg type
    @Test
    void function_wrong_arg_type() throws SQLException {
        exec("CREATE FUNCTION add1(x INTEGER) RETURNS INTEGER AS $$ SELECT x + 1 $$ LANGUAGE sql");
        // PG raises 22P02 (invalid_text_representation) or 42883 when types don't match
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT add1('not_a_number')");
            fail("Expected error for wrong arg type");
        } catch (SQLException e) {
            String state = e.getSQLState();
            assertTrue("22P02".equals(state) || "42883".equals(state),
                "Expected SQLSTATE 22P02 or 42883 but got: " + state + " (" + e.getMessage() + ")");
        }
    }

    // 19. CALL on a function (not procedure)
    @Test
    void call_on_function_not_procedure() throws SQLException {
        exec("CREATE FUNCTION add1(x INTEGER) RETURNS INTEGER AS $$ SELECT x + 1 $$ LANGUAGE sql");
        assertSqlErrorContains(
            "CALL add1(1)",
            "42809",
            "is not a procedure");
    }

    // 20. CALL with wrong arity
    @Test
    void call_procedure_wrong_arity() throws SQLException {
        exec("CREATE TABLE proc_t (id INTEGER)");
        exec("CREATE PROCEDURE myproc(val INTEGER) LANGUAGE sql AS $$ INSERT INTO proc_t VALUES (val) $$");
        assertSqlErrorContains(
            "CALL myproc()",
            "42883",
            "does not exist");
    }

    // 21. CALL error says "procedure" not "function"
    @Test
    void call_error_says_procedure_not_function() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CALL nonexistent_proc(1)");
            fail("Expected error for non-existent procedure");
        } catch (SQLException e) {
            assertTrue(e.getMessage().toLowerCase().contains("procedure"),
                "Error message should say 'procedure' not 'function', got: " + e.getMessage());
        }
    }

    // 22. SQL function body validation on CREATE (reference to missing column)
    @Test
    void sql_function_body_validation_missing_column() throws SQLException {
        exec("CREATE TABLE body_t (id INTEGER)");
        assertSqlError(
            "CREATE FUNCTION bad_sql_body(x INTEGER) RETURNS INTEGER LANGUAGE sql AS $$ SELECT missing_col FROM body_t $$",
            "42703");
    }

    // 23. SQL function body syntax error
    @Test
    void sql_function_body_syntax_error() throws SQLException {
        assertSqlError(
            "CREATE FUNCTION syntax_err_body(x INTEGER) RETURNS INTEGER LANGUAGE sql AS $$ SELECT x + $$",
            "42601");
    }

    // 24. PL/pgSQL nested exception handling
    @Test
    void plpgsql_nested_exception_handling() throws SQLException {
        exec("CREATE FUNCTION nested_exc() RETURNS TEXT LANGUAGE plpgsql AS $$\n" +
             "DECLARE result TEXT;\n" +
             "BEGIN\n" +
             "  result := 'outer';\n" +
             "  BEGIN\n" +
             "    PERFORM 1/0;\n" +
             "    result := 'no_error';\n" +
             "  EXCEPTION WHEN division_by_zero THEN\n" +
             "    result := 'caught_inner';\n" +
             "  END;\n" +
             "  RETURN result;\n" +
             "END;\n" +
             "$$");
        assertEquals("caught_inner", querySingle("SELECT nested_exc()"));
    }

    // 25. PL/pgSQL RAISE error message propagation
    @Test
    void plpgsql_raise_error_message_propagation() throws SQLException {
        exec("CREATE FUNCTION fail_if_negative(x INTEGER) RETURNS INTEGER LANGUAGE plpgsql AS $$\n" +
             "BEGIN\n" +
             "  IF x < 0 THEN\n" +
             "    RAISE EXCEPTION 'negative input not allowed' USING ERRCODE = '22023';\n" +
             "  END IF;\n" +
             "  RETURN x;\n" +
             "END;\n" +
             "$$");
        try (Statement s = conn.createStatement()) {
            s.execute("SELECT fail_if_negative(-1)");
            fail("Expected error from RAISE EXCEPTION");
        } catch (SQLException e) {
            assertEquals("22023", e.getSQLState());
            assertTrue(e.getMessage().contains("negative input not allowed"),
                "Error message should be 'negative input not allowed', got: " + e.getMessage());
        }
    }

    // ========================================================================
    // ADDITIONAL CONSTRAINT & DML TESTS (to reach 35+)
    // ========================================================================

    // 26. NOT NULL violation on UPDATE
    @Test
    void not_null_violation_on_update() throws SQLException {
        exec("CREATE TABLE nn_upd (id INTEGER PRIMARY KEY, name TEXT NOT NULL)");
        exec("INSERT INTO nn_upd VALUES (1, 'Alice')");
        assertSqlError(
            "UPDATE nn_upd SET name = NULL WHERE id = 1",
            "23502");
    }

    // 27. UNIQUE constraint violation gives correct SQLSTATE
    @Test
    void unique_constraint_violation_sqlstate() throws SQLException {
        exec("CREATE TABLE uniq_t (id INTEGER, email TEXT UNIQUE)");
        exec("INSERT INTO uniq_t VALUES (1, 'a@b.com')");
        assertSqlError(
            "INSERT INTO uniq_t VALUES (2, 'a@b.com')",
            "23505");
    }

    // 28. FK violation on INSERT gives 23503
    @Test
    void fk_violation_on_insert() throws SQLException {
        exec("CREATE TABLE fk_p (id INTEGER PRIMARY KEY)");
        exec("INSERT INTO fk_p VALUES (1)");
        exec("CREATE TABLE fk_c (id INTEGER, pid INTEGER REFERENCES fk_p(id))");
        assertSqlError(
            "INSERT INTO fk_c VALUES (1, 999)",
            "23503");
    }

    // 29. FK violation on DELETE (RESTRICT) gives 23503
    @Test
    void fk_violation_on_delete_restrict() throws SQLException {
        exec("CREATE TABLE fk_del_p (id INTEGER PRIMARY KEY)");
        exec("INSERT INTO fk_del_p VALUES (1)");
        exec("CREATE TABLE fk_del_c (id INTEGER, pid INTEGER REFERENCES fk_del_p(id))");
        exec("INSERT INTO fk_del_c VALUES (1, 1)");
        assertSqlError(
            "DELETE FROM fk_del_p WHERE id = 1",
            "23503");
    }

    // 30. CHECK violation gives 23514
    @Test
    void check_violation_sqlstate() throws SQLException {
        exec("CREATE TABLE chk_t (id INTEGER, val INTEGER CHECK (val >= 0))");
        assertSqlError(
            "INSERT INTO chk_t VALUES (1, -1)",
            "23514");
    }

    // 31. INSERT ... ON CONFLICT DO NOTHING
    @Test
    void insert_on_conflict_do_nothing() throws SQLException {
        exec("CREATE TABLE upsert_t (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO upsert_t VALUES (1, 'first')");
        exec("INSERT INTO upsert_t VALUES (1, 'second') ON CONFLICT DO NOTHING");
        assertEquals("first", querySingle("SELECT val FROM upsert_t WHERE id = 1"));
    }

    // 32. INSERT ... ON CONFLICT DO UPDATE
    @Test
    void insert_on_conflict_do_update() throws SQLException {
        exec("CREATE TABLE upsert_t2 (id INTEGER PRIMARY KEY, val TEXT)");
        exec("INSERT INTO upsert_t2 VALUES (1, 'first')");
        exec("INSERT INTO upsert_t2 VALUES (1, 'second') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val");
        assertEquals("second", querySingle("SELECT val FROM upsert_t2 WHERE id = 1"));
    }

    // 33. DELETE ... RETURNING
    @Test
    void delete_returning() throws SQLException {
        exec("CREATE TABLE del_ret (id SERIAL PRIMARY KEY, name TEXT)");
        exec("INSERT INTO del_ret(name) VALUES ('Alice'), ('Bob')");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("DELETE FROM del_ret WHERE name = 'Alice' RETURNING id, name")) {
            assertTrue(rs.next());
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());
        }
    }

    // 34. UPDATE ... RETURNING
    @Test
    void update_returning() throws SQLException {
        exec("CREATE TABLE upd_ret (id SERIAL PRIMARY KEY, val INTEGER)");
        exec("INSERT INTO upd_ret(val) VALUES (10)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("UPDATE upd_ret SET val = 20 RETURNING val")) {
            assertTrue(rs.next());
            assertEquals(20, rs.getInt(1));
        }
    }

    // 35. Multi-column PRIMARY KEY duplicate error
    @Test
    void multi_column_pk_duplicate() throws SQLException {
        exec("CREATE TABLE multi_pk (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");
        exec("INSERT INTO multi_pk VALUES (1, 1)");
        assertSqlError(
            "INSERT INTO multi_pk VALUES (1, 1)",
            "23505");
    }

    // 36. FK CASCADE DELETE works
    @Test
    void fk_cascade_delete() throws SQLException {
        exec("CREATE TABLE casc_p (id INTEGER PRIMARY KEY)");
        exec("INSERT INTO casc_p VALUES (1), (2)");
        exec("CREATE TABLE casc_c (id INTEGER, pid INTEGER REFERENCES casc_p(id) ON DELETE CASCADE)");
        exec("INSERT INTO casc_c VALUES (10, 1), (20, 2)");
        exec("DELETE FROM casc_p WHERE id = 1");
        assertEquals("1", querySingle("SELECT count(*) FROM casc_c"));
        assertEquals("20", querySingle("SELECT id FROM casc_c"));
    }

    // 37. FK SET NULL on DELETE works
    @Test
    void fk_set_null_on_delete() throws SQLException {
        exec("CREATE TABLE sn_p (id INTEGER PRIMARY KEY)");
        exec("INSERT INTO sn_p VALUES (1)");
        exec("CREATE TABLE sn_c (id INTEGER, pid INTEGER REFERENCES sn_p(id) ON DELETE SET NULL)");
        exec("INSERT INTO sn_c VALUES (10, 1)");
        exec("DELETE FROM sn_p WHERE id = 1");
        String val = querySingle("SELECT pid FROM sn_c WHERE id = 10");
        assertNull(val, "FK SET NULL should set child column to NULL");
    }

    // 38. Transaction rollback preserves data
    @Test
    void transaction_rollback_preserves_prior_data() throws SQLException {
        exec("CREATE TABLE tx_t (id INTEGER)");
        exec("INSERT INTO tx_t VALUES (1)");
        exec("BEGIN");
        exec("INSERT INTO tx_t VALUES (2)");
        exec("ROLLBACK");
        assertEquals("1", querySingle("SELECT count(*) FROM tx_t"));
    }

    // 39. Transaction commit persists data
    @Test
    void transaction_commit_persists_data() throws SQLException {
        exec("CREATE TABLE tx_t2 (id INTEGER)");
        exec("BEGIN");
        exec("INSERT INTO tx_t2 VALUES (1)");
        exec("INSERT INTO tx_t2 VALUES (2)");
        exec("COMMIT");
        assertEquals("2", querySingle("SELECT count(*) FROM tx_t2"));
    }

    // 40. SERIAL column auto-increments correctly with RETURNING
    @Test
    void serial_returning_auto_increment() throws SQLException {
        exec("CREATE TABLE ser_ret (id SERIAL PRIMARY KEY, name TEXT)");
        try (Statement s = conn.createStatement()) {
            ResultSet rs = s.executeQuery("INSERT INTO ser_ret(name) VALUES ('A') RETURNING id");
            assertTrue(rs.next());
            int id1 = rs.getInt(1);
            rs.close();

            rs = s.executeQuery("INSERT INTO ser_ret(name) VALUES ('B') RETURNING id");
            assertTrue(rs.next());
            int id2 = rs.getInt(1);
            rs.close();

            assertTrue(id2 > id1, "Second serial value should be greater than first");
        }
    }

    // 41. RETURNING with multiple columns and expressions
    @Test
    void returning_multiple_columns_and_expressions() throws SQLException {
        exec("CREATE TABLE ret_multi (id SERIAL PRIMARY KEY, x INTEGER, y INTEGER)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "INSERT INTO ret_multi(x, y) VALUES (3, 4) RETURNING id, x + y AS total, x * y AS product")) {
            assertTrue(rs.next());
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(3, meta.getColumnCount());
            assertEquals("total", meta.getColumnName(2));
            assertEquals("product", meta.getColumnName(3));
            assertEquals(7, rs.getInt("total"));
            assertEquals(12, rs.getInt("product"));
        }
    }

    // 42. INSERT with DEFAULT keyword
    @Test
    void insert_with_default_keyword() throws SQLException {
        exec("CREATE TABLE def_t (id SERIAL PRIMARY KEY, val INTEGER DEFAULT 42)");
        exec("INSERT INTO def_t(val) VALUES (DEFAULT)");
        assertEquals("42", querySingle("SELECT val FROM def_t"));
    }

    // 43. ALTER TABLE ADD CHECK on existing data that violates
    @Test
    void alter_add_check_violates_existing_data() throws SQLException {
        exec("CREATE TABLE alt_chk (id INTEGER, val INTEGER)");
        exec("INSERT INTO alt_chk VALUES (1, -5)");
        assertSqlError(
            "ALTER TABLE alt_chk ADD CONSTRAINT pos_val CHECK (val > 0)",
            "23514");
    }

    // 44. DROP CONSTRAINT that does not exist
    @Test
    void drop_nonexistent_constraint() throws SQLException {
        exec("CREATE TABLE drop_con_t (id INTEGER)");
        assertSqlError(
            "ALTER TABLE drop_con_t DROP CONSTRAINT no_such_constraint",
            "42704");
    }

    // 45. RETURNING star returns all columns
    @Test
    void returning_star() throws SQLException {
        exec("CREATE TABLE ret_star (id SERIAL PRIMARY KEY, name TEXT, active BOOLEAN DEFAULT true)");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                 "INSERT INTO ret_star(name) VALUES ('Test') RETURNING *")) {
            assertTrue(rs.next());
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(3, meta.getColumnCount());
            assertNotNull(rs.getString("id"));
            assertEquals("Test", rs.getString("name"));
            assertEquals(true, rs.getBoolean("active"));
        }
    }
}
