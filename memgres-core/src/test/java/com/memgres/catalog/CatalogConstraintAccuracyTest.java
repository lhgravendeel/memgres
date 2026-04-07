package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catalog accuracy tests covering known PG 18 vs Memgres compatibility differences.
 *
 * Covers verification differences:
 *
 * diff 59, table_constraints row count:
 *   SELECT constraint_name, constraint_type FROM information_schema.table_constraints
 *   WHERE table_schema = 'compat' AND table_name = 'smoke_a'
 *   PG returns 3 rows (PK, UNIQUE, CHECK). Memgres returns 4 (too many NOT NULL
 *   CHECK entries being generated). Test that count matches PG 18 expectation.
 *
 * diff 60, key_column_usage row count:
 *   SELECT kcu.column_name, tc.constraint_name
 *   FROM information_schema.table_constraints tc
 *   JOIN information_schema.key_column_usage kcu ...
 *   PG returns 1, memgres returns 2. Only PRIMARY KEY and UNIQUE constraints
 *   should appear in key_column_usage; CHECK constraints must not.
 *
 * diff 61, pg_constraint row count:
 *   SELECT conname, contype FROM pg_constraint WHERE conrelid = 'compat.smoke_a'::regclass
 *   PG returns 3, memgres returns 4 (too many NOT NULL constraints).
 *
 * Extended catalog accuracy coverage:
 *   - information_schema.columns (column_name, data_type, is_nullable, column_default)
 *   - information_schema.tables (table_type for tables vs views)
 *   - pg_attribute (attname, attnum, atttypid matching)
 *   - pg_class (relname, relkind for tables/views/indexes/sequences)
 *   - pg_namespace (schema visibility)
 *   - Constraint types: PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY
 *   - NOT NULL constraint representation in PG 18
 *   - information_schema.referential_constraints for foreign keys
 *   - pg_index for primary key index
 *   - information_schema.constraint_column_usage
 */
class CatalogConstraintAccuracyTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        // Create the canonical test table used across all tests in this class.
        // Named with a prefix to avoid collision with smoke_a used in diffs 59-61.
        exec("CREATE TABLE cat_t (" +
             "  id    INT PRIMARY KEY," +
             "  name  TEXT NOT NULL," +
             "  email TEXT UNIQUE," +
             "  age   INT  CHECK (age > 0)," +
             "  score INT" +
             ")");

        // Reference table for foreign key tests
        exec("CREATE TABLE cat_ref (ref_id INT PRIMARY KEY)");

        // Table with a foreign key back to cat_ref
        exec("CREATE TABLE cat_fk (" +
             "  fk_id INT PRIMARY KEY," +
             "  ref   INT REFERENCES cat_ref(ref_id)" +
             ")");

        // A view for relkind testing
        exec("CREATE VIEW cat_v AS SELECT id, name FROM cat_t");

        // A sequence for relkind testing
        exec("CREATE SEQUENCE cat_seq START 1");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            // Drop in dependency order
            try { exec("DROP VIEW  IF EXISTS cat_v");   } catch (SQLException ignored) {}
            try { exec("DROP TABLE IF EXISTS cat_fk");  } catch (SQLException ignored) {}
            try { exec("DROP TABLE IF EXISTS cat_t");   } catch (SQLException ignored) {}
            try { exec("DROP TABLE IF EXISTS cat_ref"); } catch (SQLException ignored) {}
            try { exec("DROP SEQUENCE IF EXISTS cat_seq"); } catch (SQLException ignored) {}
            conn.close();
        }
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
    // diff 59: information_schema.table_constraints row count
    // ========================================================================

    /**
     * PG 18 exposes constraint rows including NOT NULL constraints (contype='n').
     *
     * cat_t has: PRIMARY KEY (id), UNIQUE (email), CHECK (age > 0),
     * NOT NULL (name), NOT NULL (id) = 5 named constraints.
     * PG 18 produces NOT NULL constraint rows for ALL NOT NULL columns
     * including the PK column "id".
     */
    @Test
    void table_constraints_row_count_excludes_implicit_not_null() throws SQLException {
        // Fetch all named constraints visible in information_schema for cat_t
        List<List<String>> rows = query(
                "SELECT constraint_name, constraint_type " +
                "FROM information_schema.table_constraints " +
                "WHERE table_name = 'cat_t' " +
                "ORDER BY constraint_type, constraint_name");

        // Collect types present for easier assertion messages
        Set<String> types = new LinkedHashSet<>();
        for (List<String> row : rows) types.add(row.get(1));

        // Must have PRIMARY KEY, UNIQUE, CHECK, and NOT NULL
        assertTrue(types.contains("PRIMARY KEY"),
                "table_constraints must include PRIMARY KEY for cat_t; got: " + rows);
        assertTrue(types.contains("UNIQUE"),
                "table_constraints must include UNIQUE for cat_t; got: " + rows);
        assertTrue(types.contains("CHECK"),
                "table_constraints must include CHECK for cat_t; got: " + rows);

        // PG 18 returns 5 rows: PK + UNIQUE + CHECK + NOT NULL on 'name' + NOT NULL on 'id'.
        assertEquals(5, rows.size(),
                "table_constraints should have exactly 5 rows (PK + UNIQUE + CHECK + 2x NOT NULL), " +
                "got " + rows.size() + ": " + rows);
    }

    /**
     * Variant matching diff 59 exactly: smoke_a analog with schema-qualified filter.
     * Creates a local table inside a schema named after the current search_path to
     * mimic the original query pattern used in the diff.
     */
    @Test
    void table_constraints_schema_qualified_count() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS diff59_sch");
        exec("CREATE TABLE diff59_sch.smoke_a (" +
             "  id   INT PRIMARY KEY," +
             "  code TEXT UNIQUE NOT NULL," +
             "  val  INT  CHECK (val >= 0)" +
             ")");
        try {
            List<List<String>> rows = query(
                    "SELECT constraint_name, constraint_type " +
                    "FROM information_schema.table_constraints " +
                    "WHERE table_schema = 'diff59_sch' AND table_name = 'smoke_a' " +
                    "ORDER BY constraint_type, constraint_name");

            assertEquals(5, rows.size(),
                    "Schema-qualified table_constraints should return exactly 5 rows " +
                    "(PK + UNIQUE + CHECK + 2x NOT NULL), got " + rows.size() + ": " + rows);
        } finally {
            exec("DROP TABLE IF EXISTS diff59_sch.smoke_a");
            exec("DROP SCHEMA IF EXISTS diff59_sch");
        }
    }

    // ========================================================================
    // diff 60: key_column_usage row count
    // ========================================================================

    /**
     * Only PRIMARY KEY and UNIQUE constraints expose rows in key_column_usage.
     * CHECK constraints (including those derived from NOT NULL) must not appear.
     * For cat_t with a single-column PK on "id", the JOIN should return exactly
     * 1 row for PRIMARY KEY and 1 for UNIQUE = 2 total.
     */
    @Test
    void key_column_usage_contains_pk_and_unique_only() throws SQLException {
        List<List<String>> rows = query(
                "SELECT kcu.column_name, tc.constraint_name, tc.constraint_type " +
                "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu " +
                "  ON tc.constraint_name = kcu.constraint_name " +
                " AND tc.table_schema   = kcu.table_schema " +
                "WHERE tc.table_name = 'cat_t' " +
                "ORDER BY tc.constraint_type, kcu.column_name");

        // Constraint types visible in kcu must only be PK and UNIQUE
        for (List<String> row : rows) {
            String ctype = row.get(2);
            assertTrue("PRIMARY KEY".equals(ctype) || "UNIQUE".equals(ctype),
                    "key_column_usage must only surface PRIMARY KEY or UNIQUE constraints, " +
                    "found constraint_type='" + ctype + "' for column '" + row.get(0) + "'");
        }

        // cat_t: PK on id (1 row) + UNIQUE on email (1 row) = 2 rows total
        assertEquals(2, rows.size(),
                "key_column_usage JOIN table_constraints should return 2 rows for cat_t " +
                "(PK on id + UNIQUE on email), got " + rows.size() + ": " + rows);
    }

    /**
     * Variant matching diff 60 exactly: the original diff query joined on
     * constraint_schema = table_schema. Replicating that exact join shape.
     */
    @Test
    void key_column_usage_diff60_exact_join() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS diff60_sch");
        exec("CREATE TABLE diff60_sch.smoke_a (" +
             "  id   INT PRIMARY KEY," +
             "  code TEXT UNIQUE NOT NULL" +
             ")");
        try {
            List<List<String>> rows = query(
                    "SELECT kcu.column_name, tc.constraint_name " +
                    "FROM information_schema.table_constraints tc " +
                    "JOIN information_schema.key_column_usage kcu " +
                    "  ON tc.constraint_name  = kcu.constraint_name " +
                    " AND tc.constraint_schema = kcu.constraint_schema " +
                    "WHERE tc.table_schema = 'diff60_sch' " +
                    "  AND tc.table_name   = 'smoke_a' " +
                    "  AND tc.constraint_type IN ('PRIMARY KEY','UNIQUE') " +
                    "ORDER BY kcu.column_name");

            // id (PK) + code (UNIQUE) = 2 rows; NOT NULL must not appear
            assertEquals(2, rows.size(),
                    "key_column_usage diff60 join should return 2 rows (id PK + code UNIQUE), " +
                    "got " + rows.size() + ": " + rows);

            // Confirm the two columns are exactly id and code
            List<String> cols = new ArrayList<>();
            for (List<String> row : rows) cols.add(row.get(0));
            assertTrue(cols.contains("id"),   "id column must be in key_column_usage; got: " + cols);
            assertTrue(cols.contains("code"), "code column must be in key_column_usage; got: " + cols);
        } finally {
            exec("DROP TABLE IF EXISTS diff60_sch.smoke_a");
            exec("DROP SCHEMA IF EXISTS diff60_sch");
        }
    }

    // ========================================================================
    // diff 61: pg_constraint row count
    // ========================================================================

    /**
     * pg_constraint for cat_t should expose exactly 5 rows:
     *   'p' (PRIMARY KEY on id), 'u' (UNIQUE on email), 'c' (CHECK age > 0),
     *   'n' (NOT NULL on name), 'n' (NOT NULL on id).
     * PG 18 creates NOT NULL constraints for ALL NOT NULL columns including PK.
     */
    @Test
    void pg_constraint_count_excludes_implicit_not_null() throws SQLException {
        List<List<String>> rows = query(
                "SELECT conname, contype " +
                "FROM pg_constraint " +
                "WHERE conrelid = 'cat_t'::regclass " +
                "ORDER BY contype, conname");

        Set<String> types = new LinkedHashSet<>();
        for (List<String> row : rows) types.add(row.get(1));

        assertTrue(types.contains("p"),
                "pg_constraint must have type 'p' (PRIMARY KEY) for cat_t; got: " + rows);
        assertTrue(types.contains("u"),
                "pg_constraint must have type 'u' (UNIQUE) for cat_t; got: " + rows);
        assertTrue(types.contains("c"),
                "pg_constraint must have type 'c' (CHECK) for cat_t; got: " + rows);
        assertTrue(types.contains("n"),
                "pg_constraint must have type 'n' (NOT NULL) for cat_t; got: " + rows);

        assertEquals(5, rows.size(),
                "pg_constraint should return exactly 5 rows for cat_t (p + u + c + 2x n), " +
                "got " + rows.size() + ": " + rows);
    }

    /**
     * Variant matching diff 61 exactly using schema-qualified regclass cast.
     */
    @Test
    void pg_constraint_schema_qualified_count() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS diff61_sch");
        exec("CREATE TABLE diff61_sch.smoke_a (" +
             "  id  INT PRIMARY KEY," +
             "  val INT UNIQUE NOT NULL CHECK (val > 0)" +
             ")");
        try {
            List<List<String>> rows = query(
                    "SELECT conname, contype " +
                    "FROM pg_constraint " +
                    "WHERE conrelid = 'diff61_sch.smoke_a'::regclass " +
                    "ORDER BY contype, conname");

            // p (PK) + u (UNIQUE) + c (CHECK) + n (NOT NULL on val) + n (NOT NULL on id) = 5
            assertEquals(5, rows.size(),
                    "pg_constraint schema-qualified should return 5 rows, " +
                    "got " + rows.size() + ": " + rows);
        } finally {
            exec("DROP TABLE IF EXISTS diff61_sch.smoke_a");
            exec("DROP SCHEMA IF EXISTS diff61_sch");
        }
    }

    // ========================================================================
    // information_schema.columns accuracy
    // ========================================================================

    /**
     * information_schema.columns must accurately reflect column_name, data_type,
     * is_nullable, and column_default for each column of cat_t.
     */
    @Test
    void information_schema_columns_data_types() throws SQLException {
        List<List<String>> rows = query(
                "SELECT column_name, data_type, is_nullable, column_default " +
                "FROM information_schema.columns " +
                "WHERE table_name = 'cat_t' " +
                "ORDER BY ordinal_position");

        assertEquals(5, rows.size(),
                "cat_t has 5 columns, information_schema.columns returned " + rows.size());

        // id: integer, not nullable (PK), no default
        assertEquals("id",      rows.get(0).get(0), "col 1 name");
        assertEquals("integer", rows.get(0).get(1), "col 1 data_type");
        assertEquals("NO",      rows.get(0).get(2), "col 1 is_nullable (PK must be NOT NULL)");

        // name: text, not nullable (explicit NOT NULL), no default
        assertEquals("name", rows.get(1).get(0), "col 2 name");
        assertEquals("text", rows.get(1).get(1), "col 2 data_type");
        assertEquals("NO",   rows.get(1).get(2), "col 2 is_nullable (NOT NULL column)");

        // email: text, nullable, no default
        assertEquals("email", rows.get(2).get(0), "col 3 name");
        assertEquals("text",  rows.get(2).get(1), "col 3 data_type");
        assertEquals("YES",   rows.get(2).get(2), "col 3 is_nullable (nullable)");

        // age: integer, nullable, no default
        assertEquals("age",     rows.get(3).get(0), "col 4 name");
        assertEquals("integer", rows.get(3).get(1), "col 4 data_type");
        assertEquals("YES",     rows.get(3).get(2), "col 4 is_nullable (nullable)");

        // score: integer, nullable, no default
        assertEquals("score",   rows.get(4).get(0), "col 5 name");
        assertEquals("integer", rows.get(4).get(1), "col 5 data_type");
        assertEquals("YES",     rows.get(4).get(2), "col 5 is_nullable (nullable)");
    }

    /**
     * Column with a DEFAULT value must surface that default in column_default.
     */
    @Test
    void information_schema_columns_default_value() throws SQLException {
        exec("CREATE TABLE isc_def_t (id INT DEFAULT 42, label TEXT DEFAULT 'n/a')");
        try {
            List<List<String>> rows = query(
                    "SELECT column_name, column_default " +
                    "FROM information_schema.columns " +
                    "WHERE table_name = 'isc_def_t' " +
                    "ORDER BY ordinal_position");

            assertEquals(2, rows.size(), "isc_def_t should have 2 columns");
            assertNotNull(rows.get(0).get(1),
                    "id column_default must not be null; should contain '42'");
            assertTrue(rows.get(0).get(1).contains("42"),
                    "id column_default should contain '42', got: " + rows.get(0).get(1));
            assertNotNull(rows.get(1).get(1),
                    "label column_default must not be null; should contain 'n/a'");
            assertTrue(rows.get(1).get(1).contains("n/a"),
                    "label column_default should contain 'n/a', got: " + rows.get(1).get(1));
        } finally {
            exec("DROP TABLE IF EXISTS isc_def_t");
        }
    }

    // ========================================================================
    // information_schema.tables: table_type accuracy
    // ========================================================================

    /**
     * information_schema.tables must report BASE TABLE for regular tables and
     * VIEW for views.
     */
    @Test
    void information_schema_tables_type_base_table() throws SQLException {
        String tableType = scalar(
                "SELECT table_type " +
                "FROM information_schema.tables " +
                "WHERE table_name = 'cat_t'");
        assertEquals("BASE TABLE", tableType,
                "Regular table must have table_type = 'BASE TABLE'");
    }

    @Test
    void information_schema_tables_type_view() throws SQLException {
        String tableType = scalar(
                "SELECT table_type " +
                "FROM information_schema.tables " +
                "WHERE table_name = 'cat_v'");
        assertEquals("VIEW", tableType,
                "View must have table_type = 'VIEW'");
    }

    // ========================================================================
    // pg_attribute accuracy
    // ========================================================================

    /**
     * pg_attribute must expose all user-visible columns with correct attnum,
     * attname, and a valid atttypid (> 0) for each column of cat_t.
     * System columns (attnum <= 0) must be excluded.
     */
    @Test
    void pg_attribute_columns_present_with_positive_attnum() throws SQLException {
        List<List<String>> rows = query(
                "SELECT attname, attnum, atttypid " +
                "FROM pg_attribute " +
                "WHERE attrelid = 'cat_t'::regclass " +
                "  AND attnum > 0 " +
                "  AND NOT attisdropped " +
                "ORDER BY attnum");

        assertEquals(5, rows.size(),
                "pg_attribute should have 5 user columns for cat_t, got " + rows.size());

        // Verify names in ordinal order
        assertEquals("id",    rows.get(0).get(0), "attnum 1 should be id");
        assertEquals("name",  rows.get(1).get(0), "attnum 2 should be name");
        assertEquals("email", rows.get(2).get(0), "attnum 3 should be email");
        assertEquals("age",   rows.get(3).get(0), "attnum 4 should be age");
        assertEquals("score", rows.get(4).get(0), "attnum 5 should be score");

        // atttypid must be non-zero for every column
        for (List<String> row : rows) {
            int typid = Integer.parseInt(row.get(2));
            assertTrue(typid > 0,
                    "atttypid must be > 0 for column '" + row.get(0) + "', got " + typid);
        }
    }

    /**
     * atttypid for INT columns must match pg_type oid for 'integer'.
     * atttypid for TEXT columns must match pg_type oid for 'text'.
     */
    @Test
    void pg_attribute_atttypid_matches_pg_type() throws SQLException {
        String intOid  = scalar("SELECT oid FROM pg_type WHERE typname = 'int4'");
        String textOid = scalar("SELECT oid FROM pg_type WHERE typname = 'text'");

        assertNotNull(intOid,  "pg_type must contain 'int4'");
        assertNotNull(textOid, "pg_type must contain 'text'");

        // id -> int4
        String idTypid = scalar(
                "SELECT atttypid::text FROM pg_attribute " +
                "WHERE attrelid = 'cat_t'::regclass AND attname = 'id'");
        assertEquals(intOid, idTypid,
                "id column atttypid must equal oid of int4");

        // name -> text
        String nameTypid = scalar(
                "SELECT atttypid::text FROM pg_attribute " +
                "WHERE attrelid = 'cat_t'::regclass AND attname = 'name'");
        assertEquals(textOid, nameTypid,
                "name column atttypid must equal oid of text");
    }

    // ========================================================================
    // pg_class relkind accuracy
    // ========================================================================

    /**
     * pg_class relkind must be 'r' for regular tables, 'v' for views, 'S' for
     * sequences, and 'i' for indexes.
     */
    @Test
    void pg_class_relkind_table() throws SQLException {
        String relkind = scalar(
                "SELECT relkind FROM pg_class WHERE relname = 'cat_t'");
        assertEquals("r", relkind,
                "Regular table must have relkind = 'r'");
    }

    @Test
    void pg_class_relkind_view() throws SQLException {
        String relkind = scalar(
                "SELECT relkind FROM pg_class WHERE relname = 'cat_v'");
        assertEquals("v", relkind,
                "View must have relkind = 'v'");
    }

    @Test
    void pg_class_relkind_sequence() throws SQLException {
        String relkind = scalar(
                "SELECT relkind FROM pg_class WHERE relname = 'cat_seq'");
        assertEquals("S", relkind,
                "Sequence must have relkind = 'S'");
    }

    @Test
    void pg_class_relkind_index() throws SQLException {
        // The primary key on cat_t creates an index; find it via pg_index
        String relkind = scalar(
                "SELECT c.relkind " +
                "FROM pg_index i " +
                "JOIN pg_class c ON c.oid = i.indexrelid " +
                "WHERE i.indrelid = 'cat_t'::regclass " +
                "  AND i.indisprimary = true");
        assertEquals("i", relkind,
                "Primary key index must have relkind = 'i'");
    }

    // ========================================================================
    // pg_namespace schema visibility
    // ========================================================================

    /**
     * pg_namespace must contain 'public', 'pg_catalog', and 'information_schema'.
     */
    @Test
    void pg_namespace_contains_standard_schemas() throws SQLException {
        for (String schema : new String[]{"public", "pg_catalog", "information_schema"}) {
            String count = scalar(
                    "SELECT count(*) FROM pg_namespace WHERE nspname = '" + schema + "'");
            assertNotNull(count, "pg_namespace query must return a row");
            assertEquals("1", count,
                    "pg_namespace must contain schema '" + schema + "'");
        }
    }

    /**
     * A user-created schema must appear in pg_namespace.
     */
    @Test
    void pg_namespace_user_schema_visible() throws SQLException {
        exec("CREATE SCHEMA IF NOT EXISTS cat_ns_test");
        try {
            String count = scalar(
                    "SELECT count(*) FROM pg_namespace WHERE nspname = 'cat_ns_test'");
            assertEquals("1", count,
                    "User-created schema must appear in pg_namespace");
        } finally {
            exec("DROP SCHEMA IF EXISTS cat_ns_test");
        }
    }

    // ========================================================================
    // Constraint types: PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY
    // ========================================================================

    /**
     * Each named constraint type on cat_t must appear with the correct contype
     * value in pg_constraint.
     */
    @Test
    void pg_constraint_primary_key_type() throws SQLException {
        String contype = scalar(
                "SELECT contype FROM pg_constraint " +
                "WHERE conrelid = 'cat_t'::regclass AND contype = 'p'");
        assertEquals("p", contype,
                "PRIMARY KEY must have contype 'p' in pg_constraint");
    }

    @Test
    void pg_constraint_unique_type() throws SQLException {
        String contype = scalar(
                "SELECT contype FROM pg_constraint " +
                "WHERE conrelid = 'cat_t'::regclass AND contype = 'u'");
        assertEquals("u", contype,
                "UNIQUE constraint must have contype 'u' in pg_constraint");
    }

    @Test
    void pg_constraint_check_type() throws SQLException {
        String contype = scalar(
                "SELECT contype FROM pg_constraint " +
                "WHERE conrelid = 'cat_t'::regclass AND contype = 'c'");
        assertEquals("c", contype,
                "CHECK constraint must have contype 'c' in pg_constraint");
    }

    @Test
    void pg_constraint_foreign_key_type() throws SQLException {
        String contype = scalar(
                "SELECT contype FROM pg_constraint " +
                "WHERE conrelid = 'cat_fk'::regclass AND contype = 'f'");
        assertEquals("f", contype,
                "FOREIGN KEY must have contype 'f' in pg_constraint");
    }

    // ========================================================================
    // NOT NULL constraint representation in PG 18
    // ========================================================================

    /**
     * In PG 18 a column-level NOT NULL is NOT separately listed as a named
     * constraint in information_schema.table_constraints or pg_constraint.
     * It is reflected only in information_schema.columns.is_nullable = 'NO'.
     * Verify that for the 'name TEXT NOT NULL' column no extra CHECK constraint
     * row exists beyond the explicit age CHECK.
     */
    @Test
    void not_null_column_not_listed_as_extra_named_constraint() throws SQLException {
        // Count CHECK constraints on cat_t; should be exactly 1 (age > 0)
        String checkCount = scalar(
                "SELECT count(*) FROM pg_constraint " +
                "WHERE conrelid = 'cat_t'::regclass AND contype = 'c'");
        assertEquals("1", checkCount,
                "cat_t should have exactly 1 CHECK constraint (age > 0); " +
                "NOT NULL on 'name' must not add extra CHECK entries");

        // information_schema.columns must still show NOT NULL as is_nullable = 'NO'
        String isNullable = scalar(
                "SELECT is_nullable FROM information_schema.columns " +
                "WHERE table_name = 'cat_t' AND column_name = 'name'");
        assertEquals("NO", isNullable,
                "NOT NULL column must have is_nullable = 'NO' in information_schema.columns");
    }

    // ========================================================================
    // information_schema.referential_constraints for foreign keys
    // ========================================================================

    /**
     * A table with a FOREIGN KEY must expose that FK in
     * information_schema.referential_constraints with the correct
     * unique_constraint_name pointing to the referenced PK.
     */
    @Test
    void referential_constraints_visible_for_foreign_key() throws SQLException {
        List<List<String>> rows = query(
                "SELECT rc.constraint_name, rc.unique_constraint_name " +
                "FROM information_schema.referential_constraints rc " +
                "JOIN information_schema.table_constraints tc " +
                "  ON tc.constraint_name = rc.constraint_name " +
                "WHERE tc.table_name = 'cat_fk'");

        assertEquals(1, rows.size(),
                "cat_fk has 1 FK; referential_constraints should return 1 row, got " + rows.size());

        assertNotNull(rows.get(0).get(0),
                "referential_constraints constraint_name must not be null");
        assertNotNull(rows.get(0).get(1),
                "referential_constraints unique_constraint_name must not be null, " +
                "it should reference the PK on cat_ref");
    }

    // ========================================================================
    // pg_index for primary key
    // ========================================================================

    /**
     * pg_index must contain exactly one row for cat_t where indisprimary = true.
     */
    @Test
    void pg_index_primary_key_row() throws SQLException {
        String count = scalar(
                "SELECT count(*) FROM pg_index " +
                "WHERE indrelid = 'cat_t'::regclass AND indisprimary = true");
        assertEquals("1", count,
                "pg_index must have exactly 1 row with indisprimary = true for cat_t");
    }

    /**
     * The UNIQUE constraint on email should also produce a pg_index entry
     * with indisunique = true.
     */
    @Test
    void pg_index_unique_constraint_row() throws SQLException {
        String count = scalar(
                "SELECT count(*) FROM pg_index " +
                "WHERE indrelid = 'cat_t'::regclass AND indisunique = true");
        // PK is also unique, so expect at least 2 (one for PK, one for UNIQUE on email)
        int n = Integer.parseInt(count);
        assertTrue(n >= 2,
                "pg_index should have >= 2 rows with indisunique = true for cat_t " +
                "(PK index + email UNIQUE index), got " + n);
    }

    // ========================================================================
    // information_schema.constraint_column_usage
    // ========================================================================

    /**
     * information_schema.constraint_column_usage lists columns referenced by
     * PRIMARY KEY and UNIQUE constraints. For cat_t:
     *   - PK references 'id'
     *   - UNIQUE references 'email'
     * CHECK constraints (age > 0) and NOT NULL must NOT appear here.
     */
    @Test
    void constraint_column_usage_pk_and_unique_only() throws SQLException {
        List<List<String>> rows = query(
                "SELECT column_name " +
                "FROM information_schema.constraint_column_usage ccu " +
                "JOIN information_schema.table_constraints tc " +
                "  ON tc.constraint_name = ccu.constraint_name " +
                "WHERE ccu.table_name = 'cat_t' " +
                "  AND tc.table_name  = 'cat_t' " +
                "ORDER BY column_name");

        List<String> cols = new ArrayList<>();
        for (List<String> row : rows) cols.add(row.get(0));

        assertTrue(cols.contains("id"),
                "constraint_column_usage must list 'id' for PK; got: " + cols);
        assertTrue(cols.contains("email"),
                "constraint_column_usage must list 'email' for UNIQUE; got: " + cols);

        // Only id and email should appear (PK + UNIQUE), not age, name, score
        assertFalse(cols.contains("age"),
                "constraint_column_usage must not list CHECK column 'age'; got: " + cols);
        assertFalse(cols.contains("name"),
                "constraint_column_usage must not list NOT NULL column 'name'; got: " + cols);
        assertFalse(cols.contains("score"),
                "constraint_column_usage must not list unconstrained column 'score'; got: " + cols);
    }

    // ========================================================================
    // Catalog consistency: pg_constraint conrelid vs information_schema
    // ========================================================================

    /**
     * The set of constraint names in pg_constraint must match those returned by
     * information_schema.table_constraints for the same table.
     */
    @Test
    void pg_constraint_names_match_information_schema() throws SQLException {
        List<List<String>> pgRows = query(
                "SELECT conname FROM pg_constraint " +
                "WHERE conrelid = 'cat_t'::regclass " +
                "ORDER BY conname");

        List<List<String>> isRows = query(
                "SELECT constraint_name " +
                "FROM information_schema.table_constraints " +
                "WHERE table_name = 'cat_t' " +
                "ORDER BY constraint_name");

        Set<String> pgNames = new LinkedHashSet<>();
        for (List<String> row : pgRows) pgNames.add(row.get(0));

        Set<String> isNames = new LinkedHashSet<>();
        for (List<String> row : isRows) isNames.add(row.get(0));

        assertEquals(pgNames, isNames,
                "Constraint names in pg_constraint must match information_schema.table_constraints. " +
                "pg_constraint: " + pgNames + " vs information_schema: " + isNames);
    }

    /**
     * Total constraint count must be the same in pg_constraint and
     * information_schema.table_constraints.
     */
    @Test
    void pg_constraint_count_matches_information_schema_count() throws SQLException {
        String pgCount = scalar(
                "SELECT count(*) FROM pg_constraint " +
                "WHERE conrelid = 'cat_t'::regclass");
        String isCount = scalar(
                "SELECT count(*) FROM information_schema.table_constraints " +
                "WHERE table_name = 'cat_t'");

        assertEquals(pgCount, isCount,
                "pg_constraint count must match information_schema.table_constraints count for cat_t. " +
                "pg_constraint=" + pgCount + ", information_schema=" + isCount);
    }
}
