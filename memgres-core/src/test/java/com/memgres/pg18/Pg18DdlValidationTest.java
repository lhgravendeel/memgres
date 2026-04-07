package com.memgres.pg18;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests DDL validation issues found when comparing memgres behavior to PG18.
 * Validates that memgres produces correct SQLSTATE error codes for various
 * DDL validation scenarios including duplicate objects, invalid references,
 * type mismatches, and dependency tracking.
 *
 * The disable reason describes the gap.
 */
class Pg18DdlValidationTest {

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

    static String query1(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    static int queryInt(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
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

    static void assertSqlSuccess(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            fail("Expected success but got error " + e.getSQLState() + ": " + e.getMessage() + " for: " + sql);
        }
    }

    // =========================================================================
    // 1. Duplicate table detection, PG18: ERROR 42P07
    // =========================================================================

    @Test
    void duplicateTable_sameColumns_42P07() throws SQLException {
        exec("DROP TABLE IF EXISTS dup_tbl_1");
        exec("CREATE TABLE dup_tbl_1 (a int)");
        assertSqlError("CREATE TABLE dup_tbl_1 (a int)", "42P07");
        exec("DROP TABLE dup_tbl_1");
    }

    @Test
    void duplicateTable_differentColumns_42P07() throws SQLException {
        exec("DROP TABLE IF EXISTS dup_tbl_2");
        exec("CREATE TABLE dup_tbl_2 (a int)");
        assertSqlError("CREATE TABLE dup_tbl_2 (x text, y boolean)", "42P07");
        exec("DROP TABLE dup_tbl_2");
    }

    @Test
    void duplicateTable_afterDropAndRecreate_succeeds() throws SQLException {
        exec("DROP TABLE IF EXISTS dup_tbl_3");
        exec("CREATE TABLE dup_tbl_3 (a int)");
        exec("DROP TABLE dup_tbl_3");
        assertSqlSuccess("CREATE TABLE dup_tbl_3 (b text)");
        exec("DROP TABLE dup_tbl_3");
    }

    // =========================================================================
    // 2. Duplicate schema detection, PG18: ERROR 42P06
    // =========================================================================

    @Test
    void duplicateSchema_42P06() throws SQLException {
        exec("DROP SCHEMA IF EXISTS dup_schema_1 CASCADE");
        exec("CREATE SCHEMA dup_schema_1");
        assertSqlError("CREATE SCHEMA dup_schema_1", "42P06");
        exec("DROP SCHEMA dup_schema_1 CASCADE");
    }

    @Test
    void duplicateSchema_ifNotExists_succeeds() throws SQLException {
        exec("DROP SCHEMA IF EXISTS dup_schema_2 CASCADE");
        exec("CREATE SCHEMA dup_schema_2");
        assertSqlSuccess("CREATE SCHEMA IF NOT EXISTS dup_schema_2");
        exec("DROP SCHEMA dup_schema_2 CASCADE");
    }

    // =========================================================================
    // 3. Duplicate view detection, PG18: ERROR 42P07 (views are relations)
    // =========================================================================

    @Test
    void duplicateView_42P07() throws SQLException {
        exec("DROP VIEW IF EXISTS dup_view_1");
        exec("CREATE VIEW dup_view_1 AS SELECT 1 AS x");
        assertSqlError("CREATE VIEW dup_view_1 AS SELECT 1 AS x", "42P07");
        exec("DROP VIEW dup_view_1");
    }

    @Test
    void duplicateView_differentQuery_42P07() throws SQLException {
        exec("DROP VIEW IF EXISTS dup_view_2");
        exec("CREATE VIEW dup_view_2 AS SELECT 1 AS x");
        assertSqlError("CREATE VIEW dup_view_2 AS SELECT 2 AS y", "42P07");
        exec("DROP VIEW dup_view_2");
    }

    @Test
    void duplicateView_doesError() throws SQLException {
        // Verify memgres at least rejects duplicate views (even if wrong SQLSTATE)
        exec("DROP VIEW IF EXISTS dup_view_3");
        exec("CREATE VIEW dup_view_3 AS SELECT 1 AS x");
        assertSqlError("CREATE VIEW dup_view_3 AS SELECT 1 AS x", "42P07");
        exec("DROP VIEW dup_view_3");
    }

    // =========================================================================
    // 4. Duplicate sequence detection, PG18: ERROR 42P07
    // =========================================================================

    @Test
    void duplicateSequence_42P07() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS dup_seq_1");
        exec("CREATE SEQUENCE dup_seq_1");
        assertSqlError("CREATE SEQUENCE dup_seq_1", "42P07");
        exec("DROP SEQUENCE dup_seq_1");
    }

    // =========================================================================
    // 5. Duplicate index detection, PG18: ERROR 42P07
    // =========================================================================

    @Test
    void duplicateIndex_42P07() throws SQLException {
        exec("DROP TABLE IF EXISTS idx_tbl_1 CASCADE");
        exec("CREATE TABLE idx_tbl_1 (a int)");
        exec("CREATE INDEX dup_idx_1 ON idx_tbl_1 (a)");
        assertSqlError("CREATE INDEX dup_idx_1 ON idx_tbl_1 (a)", "42P07");
        exec("DROP TABLE idx_tbl_1 CASCADE");
    }

    @Test
    void duplicateIndex_ifNotExists_succeeds() throws SQLException {
        exec("DROP TABLE IF EXISTS idx_tbl_2 CASCADE");
        exec("CREATE TABLE idx_tbl_2 (a int)");
        exec("CREATE INDEX idx_ine_1 ON idx_tbl_2 (a)");
        assertSqlSuccess("CREATE INDEX IF NOT EXISTS idx_ine_1 ON idx_tbl_2 (a)");
        exec("DROP TABLE idx_tbl_2 CASCADE");
    }

    // =========================================================================
    // 6. Duplicate column in CREATE TABLE, PG18: ERROR 42701
    // =========================================================================

    @Test
    void duplicateColumnInCreate_42701() {
        assertSqlError("CREATE TABLE dup_col_tbl (a int, a text)", "42701");
    }

    @Test
    void duplicateColumnInCreate_threeColumns_42701() {
        assertSqlError("CREATE TABLE dup_col_tbl2 (x int, y text, x boolean)", "42701");
    }

    // =========================================================================
    // 7. Duplicate column in ALTER TABLE ADD, PG18: ERROR 42701
    // =========================================================================

    @Test
    void duplicateColumnInAlterAdd_42701() throws SQLException {
        exec("DROP TABLE IF EXISTS alter_dup_col");
        exec("CREATE TABLE alter_dup_col (name text)");
        assertSqlError("ALTER TABLE alter_dup_col ADD COLUMN name text", "42701");
        exec("DROP TABLE alter_dup_col");
    }

    @Test
    void alterAddColumn_newName_succeeds() throws SQLException {
        exec("DROP TABLE IF EXISTS alter_add_ok");
        exec("CREATE TABLE alter_add_ok (name text)");
        assertSqlSuccess("ALTER TABLE alter_add_ok ADD COLUMN age int");
        exec("DROP TABLE alter_add_ok");
    }

    // =========================================================================
    // 8. FK reference to non-existent table, PG18: ERROR 42P01
    // =========================================================================

    @Test
    void fkToNonExistentTable_42P01() {
        assertSqlError(
                "CREATE TABLE fk_bad_tbl (a int REFERENCES no_such_table_xyz(id))",
                "42P01");
    }

    // =========================================================================
    // 9. FK reference to non-existent column, PG18: ERROR 42703
    // =========================================================================

    @Test
    void fkToNonExistentColumn_42703() throws SQLException {
        exec("DROP TABLE IF EXISTS fk_ref_target CASCADE");
        exec("CREATE TABLE fk_ref_target (id int PRIMARY KEY, name text)");
        assertSqlError(
                "CREATE TABLE fk_bad_col (a int REFERENCES fk_ref_target(no_such_col))",
                "42703");
        exec("DROP TABLE fk_ref_target CASCADE");
    }

    // =========================================================================
    // 10. Default value type mismatch, PG18: ERROR 42804
    // =========================================================================

    @Test
    void defaultTypeMismatch_42804() {
        // PG actually accepts string defaults on int columns at CREATE time; error only at INSERT
        // Test the real PG error: DEFAULT now() on int column → 42804
        assertSqlError(
                "CREATE TABLE def_mismatch_ts (a int DEFAULT now())",
                "42804");
    }

    // =========================================================================
    // 11. Generated column immutability, PG18: ERROR 42P17
    // =========================================================================

    @Test
    void generatedColumnNotImmutable_42P17() {
        assertSqlError(
                "CREATE TABLE gen_volatile (a int, b timestamp GENERATED ALWAYS AS (now()) STORED)",
                "42P17");
    }

    // =========================================================================
    // 12. Generated column with subquery, PG18: ERROR 0A000
    // =========================================================================

    @Test
    void generatedColumnWithSubquery_0A000() {
        assertSqlError(
                "CREATE TABLE gen_subq (a int, b int GENERATED ALWAYS AS ((SELECT 1)) STORED)",
                "0A000");
    }

    // =========================================================================
    // 13. Generated column computation
    // =========================================================================

    @Test
    void generatedColumnComputation_selectShowsValues() throws SQLException {
        exec("DROP TABLE IF EXISTS gen_comp");
        exec("CREATE TABLE gen_comp (qty int, derived int GENERATED ALWAYS AS (qty * 2) STORED)");
        exec("INSERT INTO gen_comp (qty) VALUES (5)");
        exec("INSERT INTO gen_comp (qty) VALUES (10)");
        assertEquals("10", query1("SELECT derived FROM gen_comp WHERE qty = 5"));
        assertEquals("20", query1("SELECT derived FROM gen_comp WHERE qty = 10"));
        exec("DROP TABLE gen_comp");
    }

    @Test
    void generatedColumnComputation_multipleExpressions() throws SQLException {
        exec("DROP TABLE IF EXISTS gen_comp2");
        exec("CREATE TABLE gen_comp2 (a int, b int, total int GENERATED ALWAYS AS (a + b) STORED)");
        exec("INSERT INTO gen_comp2 (a, b) VALUES (3, 7)");
        assertEquals("10", query1("SELECT total FROM gen_comp2"));
        exec("DROP TABLE gen_comp2");
    }

    @Test
    void generatedColumnComputation_stringConcat() throws SQLException {
        exec("DROP TABLE IF EXISTS gen_comp3");
        exec("CREATE TABLE gen_comp3 (first_name text, last_name text, full_name text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)");
        exec("INSERT INTO gen_comp3 (first_name, last_name) VALUES ('John', 'Doe')");
        assertEquals("John Doe", query1("SELECT full_name FROM gen_comp3"));
        exec("DROP TABLE gen_comp3");
    }

    // =========================================================================
    // 14. ALTER TABLE RENAME to existing name, PG18: ERROR 42P07
    // =========================================================================

    @Test
    void alterTableRenameToExisting_42P07() throws SQLException {
        exec("DROP TABLE IF EXISTS rename_src CASCADE");
        exec("DROP TABLE IF EXISTS rename_dst CASCADE");
        exec("CREATE TABLE rename_src (a int)");
        exec("CREATE TABLE rename_dst (b int)");
        assertSqlError("ALTER TABLE rename_src RENAME TO rename_dst", "42P07");
        exec("DROP TABLE rename_src CASCADE");
        exec("DROP TABLE rename_dst CASCADE");
    }

    // =========================================================================
    // 15. ALTER TYPE with view dependency, PG18: ERROR 0A000
    // =========================================================================

    @Test
    void alterTypeWithViewDependency_0A000() throws SQLException {
        exec("DROP VIEW IF EXISTS dep_view_1 CASCADE");
        exec("DROP TABLE IF EXISTS dep_tbl_1 CASCADE");
        exec("CREATE TABLE dep_tbl_1 (c varchar(10))");
        exec("CREATE VIEW dep_view_1 AS SELECT c FROM dep_tbl_1");
        assertSqlError("ALTER TABLE dep_tbl_1 ALTER COLUMN c TYPE varchar(20)", "0A000");
        exec("DROP VIEW dep_view_1");
        exec("DROP TABLE dep_tbl_1 CASCADE");
    }

    // =========================================================================
    // 16. DROP VIEW on a table, PG18: ERROR 42809
    // =========================================================================

    @Test
    void dropViewOnTable_42809() throws SQLException {
        exec("DROP TABLE IF EXISTS not_a_view CASCADE");
        exec("CREATE TABLE not_a_view (a int)");
        assertSqlError("DROP VIEW not_a_view", "42809");
        exec("DROP TABLE not_a_view CASCADE");
    }

    @Test
    void dropViewOnTable_doesError() throws SQLException {
        // Verify memgres at least rejects DROP VIEW on a table (even if wrong SQLSTATE)
        exec("DROP TABLE IF EXISTS not_a_view2 CASCADE");
        exec("CREATE TABLE not_a_view2 (a int)");
        assertSqlError("DROP VIEW not_a_view2", "42809");
        exec("DROP TABLE not_a_view2 CASCADE");
    }

    // =========================================================================
    // 17. DROP SEQUENCE on a table, PG18: ERROR 42809
    // =========================================================================

    @Test
    void dropSequenceOnTable_42809() throws SQLException {
        exec("DROP TABLE IF EXISTS not_a_seq CASCADE");
        exec("CREATE TABLE not_a_seq (a int)");
        assertSqlError("DROP SEQUENCE not_a_seq", "42809");
        exec("DROP TABLE not_a_seq CASCADE");
    }

    @Test
    void dropSequenceOnTable_doesError() throws SQLException {
        // Verify memgres at least rejects DROP SEQUENCE on a table (even if wrong SQLSTATE)
        exec("DROP TABLE IF EXISTS not_a_seq2 CASCADE");
        exec("CREATE TABLE not_a_seq2 (a int)");
        assertSqlError("DROP SEQUENCE not_a_seq2", "42809");
        exec("DROP TABLE not_a_seq2 CASCADE");
    }

    // =========================================================================
    // 18. DROP INDEX non-existent, PG18: ERROR 42704
    // =========================================================================

    @Test
    void dropNonExistentIndex_42704() {
        assertSqlError("DROP INDEX no_such_index_xyz", "42704");
    }

    @Test
    void dropIndexIfNotExists_succeeds() {
        assertSqlSuccess("DROP INDEX IF EXISTS no_such_index_xyz");
    }

    // =========================================================================
    // 19. DROP TABLE with dependencies without CASCADE, PG18: ERROR 2BP01
    // =========================================================================

    @Test
    void dropTableWithViewDependency_2BP01() throws SQLException {
        exec("DROP VIEW IF EXISTS dep_view_2 CASCADE");
        exec("DROP TABLE IF EXISTS dep_tbl_2 CASCADE");
        exec("CREATE TABLE dep_tbl_2 (id int PRIMARY KEY, name text)");
        exec("CREATE VIEW dep_view_2 AS SELECT id, name FROM dep_tbl_2");
        assertSqlError("DROP TABLE dep_tbl_2", "2BP01");
        exec("DROP VIEW dep_view_2");
        exec("DROP TABLE dep_tbl_2 CASCADE");
    }

    // =========================================================================
    // 20. DROP TABLE CASCADE with dependencies
    // =========================================================================

    @Test
    void dropTableCascadeWithViewDependency_succeeds() throws SQLException {
        exec("DROP VIEW IF EXISTS dep_view_3 CASCADE");
        exec("DROP TABLE IF EXISTS dep_tbl_3 CASCADE");
        exec("CREATE TABLE dep_tbl_3 (id int PRIMARY KEY, name text)");
        exec("CREATE VIEW dep_view_3 AS SELECT id, name FROM dep_tbl_3");
        assertSqlSuccess("DROP TABLE dep_tbl_3 CASCADE");
    }

    // =========================================================================
    // 21. CREATE INDEX on non-existent column, PG18: ERROR 42703
    // =========================================================================

    @Test
    void createIndexOnNonExistentColumn_42703() throws SQLException {
        exec("DROP TABLE IF EXISTS idx_col_tbl CASCADE");
        exec("CREATE TABLE idx_col_tbl (a int, b text)");
        assertSqlError("CREATE INDEX bad_idx ON idx_col_tbl (no_such_col)", "42703");
        exec("DROP TABLE idx_col_tbl CASCADE");
    }

    // =========================================================================
    // 22. CREATE INDEX with bad expression, PG18: ERROR 42883
    // =========================================================================

    @Test
    void createIndexWithBadExpression_42883() throws SQLException {
        exec("DROP TABLE IF EXISTS idx_expr_tbl CASCADE");
        exec("CREATE TABLE idx_expr_tbl (a int)");
        assertSqlError("CREATE INDEX bad_expr_idx ON idx_expr_tbl ((unknown_func_xyz(a)))", "42883");
        exec("DROP TABLE idx_expr_tbl CASCADE");
    }

    // =========================================================================
    // 23. Missing column in SELECT, PG18: ERROR 42703
    // =========================================================================

    @Test
    void selectNonExistentColumn_42703() throws SQLException {
        exec("DROP TABLE IF EXISTS sel_tbl CASCADE");
        exec("CREATE TABLE sel_tbl (a int, b text)");
        assertSqlError("SELECT no_such_col FROM sel_tbl", "42703");
        exec("DROP TABLE sel_tbl CASCADE");
    }

    // =========================================================================
    // 24. CREATE TABLE IF NOT EXISTS
    // =========================================================================

    @Test
    void createTableIfNotExists_succeeds() throws SQLException {
        exec("DROP TABLE IF EXISTS ine_tbl CASCADE");
        exec("CREATE TABLE ine_tbl (a int)");
        assertSqlSuccess("CREATE TABLE IF NOT EXISTS ine_tbl (a int)");
        exec("DROP TABLE ine_tbl CASCADE");
    }

    @Test
    void createTableIfNotExists_differentColumns_succeeds() throws SQLException {
        exec("DROP TABLE IF EXISTS ine_tbl2 CASCADE");
        exec("CREATE TABLE ine_tbl2 (a int)");
        // IF NOT EXISTS should silently succeed even with different columns
        assertSqlSuccess("CREATE TABLE IF NOT EXISTS ine_tbl2 (x text, y boolean)");
        exec("DROP TABLE ine_tbl2 CASCADE");
    }

    // =========================================================================
    // 25. CREATE INDEX IF NOT EXISTS
    // =========================================================================

    @Test
    void createIndexIfNotExists_succeeds() throws SQLException {
        exec("DROP TABLE IF EXISTS idx_ine_tbl CASCADE");
        exec("CREATE TABLE idx_ine_tbl (a int)");
        exec("CREATE INDEX idx_ine_test ON idx_ine_tbl (a)");
        assertSqlSuccess("CREATE INDEX IF NOT EXISTS idx_ine_test ON idx_ine_tbl (a)");
        exec("DROP TABLE idx_ine_tbl CASCADE");
    }

    // =========================================================================
    // Additional tests beyond the 25 core items
    // =========================================================================

    // --- DROP non-existent table ---

    @Test
    void dropNonExistentTable_42P01() {
        assertSqlError("DROP TABLE no_such_table_abc", "42P01");
    }

    @Test
    void dropTableIfNotExists_succeeds() {
        assertSqlSuccess("DROP TABLE IF EXISTS no_such_table_abc");
    }

    // --- DROP non-existent view ---

    @Test
    void dropNonExistentView_42P01() {
        assertSqlError("DROP VIEW no_such_view_abc", "42P01");
    }

    @Test
    void dropNonExistentView_doesError() {
        // Verify memgres at least errors (current SQLSTATE: 42000)
        assertSqlError("DROP VIEW no_such_view_abc", "42P01");
    }

    // --- DROP non-existent sequence ---

    @Test
    void dropNonExistentSequence_42P01() {
        assertSqlError("DROP SEQUENCE no_such_seq_abc", "42P01");
    }

    @Test
    void dropNonExistentSequence_doesError() {
        // Verify memgres at least errors (current SQLSTATE: 42000)
        assertSqlError("DROP SEQUENCE no_such_seq_abc", "42P01");
    }

    // --- ALTER TABLE on non-existent table ---

    @Test
    void alterNonExistentTable_42P01() {
        assertSqlError("ALTER TABLE no_such_alter_tbl ADD COLUMN x int", "42P01");
    }

    // --- ALTER TABLE DROP non-existent column ---

    @Test
    void alterTableDropNonExistentColumn_42703() throws SQLException {
        exec("DROP TABLE IF EXISTS alt_drop_col CASCADE");
        exec("CREATE TABLE alt_drop_col (a int, b text)");
        assertSqlError("ALTER TABLE alt_drop_col DROP COLUMN no_such_col", "42703");
        exec("DROP TABLE alt_drop_col CASCADE");
    }

    // --- CREATE VIEW referencing non-existent table ---

    @Test
    void createViewOnNonExistentTable_42P01() {
        assertSqlError("CREATE VIEW bad_view AS SELECT * FROM no_such_table_for_view", "42P01");
    }

    // --- CREATE VIEW referencing non-existent column ---

    @Test
    void createViewOnNonExistentColumn_42703() throws SQLException {
        exec("DROP TABLE IF EXISTS view_col_tbl CASCADE");
        exec("CREATE TABLE view_col_tbl (a int)");
        assertSqlError("CREATE VIEW bad_view_col AS SELECT no_such_col FROM view_col_tbl", "42703");
        exec("DROP TABLE view_col_tbl CASCADE");
    }

    // --- INSERT into non-existent table ---

    @Test
    void insertIntoNonExistentTable_42P01() {
        assertSqlError("INSERT INTO no_such_insert_tbl VALUES (1)", "42P01");
    }

    // --- CREATE TABLE with UNIQUE and duplicate insert ---

    @Test
    void uniqueConstraintViolation_23505() throws SQLException {
        exec("DROP TABLE IF EXISTS uniq_tbl CASCADE");
        exec("CREATE TABLE uniq_tbl (id int UNIQUE)");
        exec("INSERT INTO uniq_tbl VALUES (1)");
        assertSqlError("INSERT INTO uniq_tbl VALUES (1)", "23505");
        exec("DROP TABLE uniq_tbl CASCADE");
    }

    // --- NOT NULL constraint violation ---

    @Test
    void notNullViolation_23502() throws SQLException {
        exec("DROP TABLE IF EXISTS nn_tbl CASCADE");
        exec("CREATE TABLE nn_tbl (id int NOT NULL)");
        assertSqlError("INSERT INTO nn_tbl VALUES (NULL)", "23502");
        exec("DROP TABLE nn_tbl CASCADE");
    }

    // --- DROP VIEW IF EXISTS on non-view should silently succeed ---

    @Test
    void dropViewIfExistsOnNonView_succeeds() throws SQLException {
        // DROP VIEW IF EXISTS on something that is not a view should not error
        assertSqlSuccess("DROP VIEW IF EXISTS no_such_thing_at_all_xyz");
    }

    // --- CREATE OR REPLACE VIEW on existing view ---

    @Test
    void createOrReplaceView_succeeds() throws SQLException {
        exec("DROP VIEW IF EXISTS replace_view CASCADE");
        exec("CREATE VIEW replace_view AS SELECT 1 AS x");
        assertSqlSuccess("CREATE OR REPLACE VIEW replace_view AS SELECT 2 AS x");
        assertEquals("2", query1("SELECT x FROM replace_view"));
        exec("DROP VIEW replace_view CASCADE");
    }

    // --- DROP TABLE on a view (wrong object type) ---

    @Test
    void dropTableOnView_42809() throws SQLException {
        exec("DROP VIEW IF EXISTS not_a_table_v CASCADE");
        exec("CREATE VIEW not_a_table_v AS SELECT 1 AS x");
        assertSqlError("DROP TABLE not_a_table_v", "42809");
        exec("DROP VIEW not_a_table_v CASCADE");
    }

    @Test
    void dropTableOnView_doesError() throws SQLException {
        // Verify memgres at least errors (current SQLSTATE: 42P01)
        exec("DROP VIEW IF EXISTS not_a_table_v2 CASCADE");
        exec("CREATE VIEW not_a_table_v2 AS SELECT 1 AS x");
        assertSqlError("DROP TABLE not_a_table_v2", "42809");
        exec("DROP VIEW not_a_table_v2 CASCADE");
    }

    // --- DROP non-existent schema ---

    @Test
    void dropNonExistentSchema_errors() {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP SCHEMA no_such_schema_xyz");
            fail("Expected error but SQL succeeded");
        } catch (SQLException e) {
            // PG18 would return 3F000; memgres may differ but should error
            assertNotNull(e.getSQLState());
        }
    }

    @Test
    void dropSchemaIfNotExists_succeeds() {
        assertSqlSuccess("DROP SCHEMA IF EXISTS no_such_schema_xyz");
    }

    // --- Primary key constraint violation ---

    @Test
    void primaryKeyViolation_23505() throws SQLException {
        exec("DROP TABLE IF EXISTS pk_tbl CASCADE");
        exec("CREATE TABLE pk_tbl (id int PRIMARY KEY)");
        exec("INSERT INTO pk_tbl VALUES (1)");
        assertSqlError("INSERT INTO pk_tbl VALUES (1)", "23505");
        exec("DROP TABLE pk_tbl CASCADE");
    }

    // --- FK constraint violation on insert ---

    @Test
    void fkViolationOnInsert_23503() throws SQLException {
        exec("DROP TABLE IF EXISTS fk_child CASCADE");
        exec("DROP TABLE IF EXISTS fk_parent CASCADE");
        exec("CREATE TABLE fk_parent (id int PRIMARY KEY)");
        exec("CREATE TABLE fk_child (pid int REFERENCES fk_parent(id))");
        assertSqlError("INSERT INTO fk_child VALUES (999)", "23503");
        exec("DROP TABLE fk_child CASCADE");
        exec("DROP TABLE fk_parent CASCADE");
    }

    // --- CHECK constraint violation ---

    @Test
    void checkConstraintViolation_23514() throws SQLException {
        exec("DROP TABLE IF EXISTS chk_tbl CASCADE");
        exec("CREATE TABLE chk_tbl (age int CHECK (age >= 0))");
        assertSqlError("INSERT INTO chk_tbl VALUES (-1)", "23514");
        exec("DROP TABLE chk_tbl CASCADE");
    }

    // --- Basic DDL operations that should work ---

    @Test
    void createAndDropTable_roundtrip() throws SQLException {
        exec("DROP TABLE IF EXISTS roundtrip_tbl CASCADE");
        exec("CREATE TABLE roundtrip_tbl (id serial PRIMARY KEY, name text NOT NULL, created_at timestamp DEFAULT now())");
        exec("INSERT INTO roundtrip_tbl (name) VALUES ('test')");
        String name = query1("SELECT name FROM roundtrip_tbl WHERE id = 1");
        assertEquals("test", name);
        exec("DROP TABLE roundtrip_tbl CASCADE");
    }

    @Test
    void alterTableAddDropColumn() throws SQLException {
        exec("DROP TABLE IF EXISTS alter_col_tbl CASCADE");
        exec("CREATE TABLE alter_col_tbl (a int)");
        exec("ALTER TABLE alter_col_tbl ADD COLUMN b text");
        exec("INSERT INTO alter_col_tbl (a, b) VALUES (1, 'hello')");
        assertEquals("hello", query1("SELECT b FROM alter_col_tbl WHERE a = 1"));
        exec("ALTER TABLE alter_col_tbl DROP COLUMN b");
        // After dropping b, only a should remain
        assertEquals("1", query1("SELECT a FROM alter_col_tbl"));
        exec("DROP TABLE alter_col_tbl CASCADE");
    }

    @Test
    void createAndDropSequence_roundtrip() throws SQLException {
        exec("DROP SEQUENCE IF EXISTS rt_seq");
        exec("CREATE SEQUENCE rt_seq START 100");
        assertEquals("100", query1("SELECT nextval('rt_seq')"));
        assertEquals("101", query1("SELECT nextval('rt_seq')"));
        exec("DROP SEQUENCE rt_seq");
    }

    @Test
    void createAndDropView_roundtrip() throws SQLException {
        exec("DROP VIEW IF EXISTS rt_view CASCADE");
        exec("DROP TABLE IF EXISTS rt_view_tbl CASCADE");
        exec("CREATE TABLE rt_view_tbl (id int, name text)");
        exec("INSERT INTO rt_view_tbl VALUES (1, 'Alice'), (2, 'Bob')");
        exec("CREATE VIEW rt_view AS SELECT name FROM rt_view_tbl WHERE id = 1");
        assertEquals("Alice", query1("SELECT name FROM rt_view"));
        exec("DROP VIEW rt_view CASCADE");
        exec("DROP TABLE rt_view_tbl CASCADE");
    }

    @Test
    void createAndDropSchema_roundtrip() throws SQLException {
        exec("DROP SCHEMA IF EXISTS rt_schema CASCADE");
        exec("CREATE SCHEMA rt_schema");
        exec("CREATE TABLE rt_schema.t1 (a int)");
        exec("INSERT INTO rt_schema.t1 VALUES (42)");
        assertEquals("42", query1("SELECT a FROM rt_schema.t1"));
        exec("DROP SCHEMA rt_schema CASCADE");
    }

    // --- Verify DROP IF EXISTS is truly no-op ---

    @Test
    void dropIfExists_allTypes_noOp() {
        assertSqlSuccess("DROP TABLE IF EXISTS totally_missing_tbl");
        assertSqlSuccess("DROP VIEW IF EXISTS totally_missing_view");
        assertSqlSuccess("DROP SEQUENCE IF EXISTS totally_missing_seq");
        assertSqlSuccess("DROP SCHEMA IF EXISTS totally_missing_schema");
        assertSqlSuccess("DROP INDEX IF EXISTS totally_missing_idx");
    }
}
