package com.memgres.compat16;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class PlpgsqlRaiseCompatTest {
    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP SCHEMA IF EXISTS raise_test CASCADE");
            stmt.execute("CREATE SCHEMA raise_test");
            stmt.execute("SET search_path = raise_test, public");

            // 1. raise_with_detail: raises error with DETAIL, returns PG_EXCEPTION_DETAIL
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_with_detail() RETURNS text LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_detail text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'main error'\n" +
                    "    USING DETAIL = 'here are the details of what went wrong';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS v_detail = PG_EXCEPTION_DETAIL;\n" +
                    "  RETURN v_detail;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 2. raise_with_hint: raises error with HINT, returns PG_EXCEPTION_HINT
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_with_hint() RETURNS text LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_hint text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'something failed'\n" +
                    "    USING HINT = 'Try running VACUUM first';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS v_hint = PG_EXCEPTION_HINT;\n" +
                    "  RETURN v_hint;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 3. raise_all_text_fields: raises with MESSAGE, DETAIL, HINT; returns all three
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_all_text_fields() RETURNS TABLE(msg text, detail text, hint text)\n" +
                    "LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_msg text; v_detail text; v_hint text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'the main message'\n" +
                    "    USING DETAIL = 'the detail text',\n" +
                    "          HINT = 'the hint text';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS\n" +
                    "    v_msg = MESSAGE_TEXT,\n" +
                    "    v_detail = PG_EXCEPTION_DETAIL,\n" +
                    "    v_hint = PG_EXCEPTION_HINT;\n" +
                    "  msg := v_msg; detail := v_detail; hint := v_hint;\n" +
                    "  RETURN NEXT;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 4. raise_full_combo: raises with ERRCODE, MESSAGE, DETAIL, HINT; returns all four
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_full_combo() RETURNS TABLE(state text, msg text, detail text, hint text)\n" +
                    "LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_state text; v_msg text; v_detail text; v_hint text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'combo message'\n" +
                    "    USING ERRCODE = 'P0099',\n" +
                    "          DETAIL = 'full detail',\n" +
                    "          HINT = 'full hint';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS\n" +
                    "    v_state = RETURNED_SQLSTATE,\n" +
                    "    v_msg = MESSAGE_TEXT,\n" +
                    "    v_detail = PG_EXCEPTION_DETAIL,\n" +
                    "    v_hint = PG_EXCEPTION_HINT;\n" +
                    "  state := v_state; msg := v_msg; detail := v_detail; hint := v_hint;\n" +
                    "  RETURN NEXT;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 5. raise_with_column: raises with COLUMN option
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_with_column() RETURNS text LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_column text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'column error'\n" +
                    "    USING COLUMN = 'email_address';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS v_column = COLUMN_NAME;\n" +
                    "  RETURN v_column;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 6. raise_with_constraint: raises with CONSTRAINT option
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_with_constraint() RETURNS text LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_constraint text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'constraint error'\n" +
                    "    USING CONSTRAINT = 'users_email_unique';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS v_constraint = CONSTRAINT_NAME;\n" +
                    "  RETURN v_constraint;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 7. raise_with_datatype: raises with DATATYPE option
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_with_datatype() RETURNS text LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_datatype text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'datatype error'\n" +
                    "    USING DATATYPE = 'integer';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS v_datatype = PG_DATATYPE_NAME;\n" +
                    "  RETURN v_datatype;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 8. raise_with_table: raises with TABLE option
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_with_table() RETURNS text LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_table text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'table error'\n" +
                    "    USING TABLE = 'users';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS v_table = TABLE_NAME;\n" +
                    "  RETURN v_table;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 9. raise_with_schema: raises with SCHEMA option
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_with_schema() RETURNS text LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_schema text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'schema error'\n" +
                    "    USING SCHEMA = 'public';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS v_schema = SCHEMA_NAME;\n" +
                    "  RETURN v_schema;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 10. raise_all_options: raises with ALL options, returns all via GET STACKED DIAGNOSTICS
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_all_options() RETURNS TABLE(\n" +
                    "  state text, msg text, detail text, hint text,\n" +
                    "  col text, constr text, dtype text, tbl text, sch text\n" +
                    ") LANGUAGE plpgsql AS $$\n" +
                    "DECLARE\n" +
                    "  v_state text; v_msg text; v_detail text; v_hint text;\n" +
                    "  v_col text; v_constr text; v_dtype text; v_tbl text; v_sch text;\n" +
                    "BEGIN\n" +
                    "  RAISE EXCEPTION 'all options message'\n" +
                    "    USING ERRCODE = 'P0001',\n" +
                    "          DETAIL = 'all detail',\n" +
                    "          HINT = 'all hint',\n" +
                    "          COLUMN = 'col_name',\n" +
                    "          CONSTRAINT = 'constr_name',\n" +
                    "          DATATYPE = 'text',\n" +
                    "          TABLE = 'tbl_name',\n" +
                    "          SCHEMA = 'sch_name';\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS\n" +
                    "    v_state = RETURNED_SQLSTATE,\n" +
                    "    v_msg = MESSAGE_TEXT,\n" +
                    "    v_detail = PG_EXCEPTION_DETAIL,\n" +
                    "    v_hint = PG_EXCEPTION_HINT,\n" +
                    "    v_col = COLUMN_NAME,\n" +
                    "    v_constr = CONSTRAINT_NAME,\n" +
                    "    v_dtype = PG_DATATYPE_NAME,\n" +
                    "    v_tbl = TABLE_NAME,\n" +
                    "    v_sch = SCHEMA_NAME;\n" +
                    "  state := v_state; msg := v_msg; detail := v_detail; hint := v_hint;\n" +
                    "  col := v_col; constr := v_constr; dtype := v_dtype; tbl := v_tbl; sch := v_sch;\n" +
                    "  RETURN NEXT;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 11. raise_reraise: catches division_by_zero, then re-raises with bare RAISE
            try {
                stmt.execute(
                    "CREATE FUNCTION raise_reraise() RETURNS text LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_state text;\n" +
                    "BEGIN\n" +
                    "  BEGIN\n" +
                    "    BEGIN\n" +
                    "      PERFORM 1/0;\n" +
                    "    EXCEPTION WHEN division_by_zero THEN\n" +
                    "      RAISE;  -- re-raise the caught exception\n" +
                    "    END;\n" +
                    "  EXCEPTION WHEN division_by_zero THEN\n" +
                    "    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE;\n" +
                    "    RETURN 'caught re-raised ' || v_state;\n" +
                    "  END;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}

            // 12. raise_real_constraint_diag: actual constraint violation, returns TABLE_NAME and CONSTRAINT_NAME
            try {
                stmt.execute("CREATE TABLE raise_test.diag_pk_table (id integer PRIMARY KEY)");
                stmt.execute("INSERT INTO raise_test.diag_pk_table VALUES (1)");
            } catch (SQLException ignored) {}

            try {
                stmt.execute(
                    "CREATE FUNCTION raise_real_constraint_diag() RETURNS TABLE(has_table boolean, has_constraint boolean)\n" +
                    "LANGUAGE plpgsql AS $$\n" +
                    "DECLARE v_table text; v_constraint text;\n" +
                    "BEGIN\n" +
                    "  INSERT INTO raise_test.diag_pk_table VALUES (1);\n" +
                    "EXCEPTION WHEN OTHERS THEN\n" +
                    "  GET STACKED DIAGNOSTICS\n" +
                    "    v_table = TABLE_NAME,\n" +
                    "    v_constraint = CONSTRAINT_NAME;\n" +
                    "  has_table := (v_table IS NOT NULL AND v_table <> '');\n" +
                    "  has_constraint := (v_constraint IS NOT NULL AND v_constraint <> '');\n" +
                    "  RETURN NEXT;\n" +
                    "END;\n" +
                    "$$"
                );
            } catch (SQLException ignored) {}
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP SCHEMA IF EXISTS raise_test CASCADE");
            } catch (Exception ignored) {}
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    // =========================================================================
    // Test 1: RAISE with DETAIL option
    // PG returns 'here are the details of what went wrong'
    // Memgres returns 'main error' (message instead of detail) - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE USING DETAIL populates PG_EXCEPTION_DETAIL")
    void raise_with_detail() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT raise_with_detail()")) {
            assertTrue(rs.next());
            assertEquals("here are the details of what went wrong", rs.getString(1));
        }
    }

    // =========================================================================
    // Test 2: RAISE with HINT option
    // PG returns 'Try running VACUUM first'
    // Memgres returns empty string - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE USING HINT populates PG_EXCEPTION_HINT")
    void raise_with_hint() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT raise_with_hint()")) {
            assertTrue(rs.next());
            String hint = rs.getString(1);
            assertNotNull(hint);
            assertFalse(hint.isEmpty(), "PG_EXCEPTION_HINT should not be empty");
        }
    }

    // =========================================================================
    // Test 3: RAISE with MESSAGE, DETAIL, HINT - returns all three
    // PG returns correct msg/detail/hint
    // Memgres returns msg/msg/empty - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE with MESSAGE, DETAIL, HINT returns distinct values for each")
    void raise_all_text_fields() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM raise_all_text_fields()")) {
            assertTrue(rs.next());
            String msg = rs.getString("msg");
            String detail = rs.getString("detail");
            String hint = rs.getString("hint");

            assertEquals("the main message", msg);
            assertNotEquals(msg, detail, "DETAIL should differ from MESSAGE");
            assertNotNull(hint);
            assertFalse(hint.isEmpty(), "HINT should not be empty");
        }
    }

    // =========================================================================
    // Test 4: RAISE with ERRCODE, MESSAGE, DETAIL, HINT
    // PG returns P0099/combo message/full detail/full hint
    // Memgres returns detail=msg, hint=empty - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE with ERRCODE, MESSAGE, DETAIL, HINT returns all correctly")
    void raise_full_combo() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM raise_full_combo()")) {
            assertTrue(rs.next());
            assertEquals("P0099", rs.getString("state"));
            assertEquals("full detail", rs.getString("detail"));
            assertEquals("full hint", rs.getString("hint"));
        }
    }

    // =========================================================================
    // Test 5: RAISE with COLUMN option
    // PG returns 'email_address', Memgres returns NULL - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE USING COLUMN populates COLUMN_NAME in GET STACKED DIAGNOSTICS")
    void raise_with_column() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT raise_with_column()")) {
            assertTrue(rs.next());
            assertEquals("email_address", rs.getString(1));
        }
    }

    // =========================================================================
    // Test 6: RAISE with CONSTRAINT option
    // PG returns 'users_email_unique', Memgres returns NULL - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE USING CONSTRAINT populates CONSTRAINT_NAME in GET STACKED DIAGNOSTICS")
    void raise_with_constraint() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT raise_with_constraint()")) {
            assertTrue(rs.next());
            assertEquals("users_email_unique", rs.getString(1));
        }
    }

    // =========================================================================
    // Test 7: RAISE with DATATYPE option
    // PG returns 'integer', Memgres returns NULL - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE USING DATATYPE populates PG_DATATYPE_NAME in GET STACKED DIAGNOSTICS")
    void raise_with_datatype() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT raise_with_datatype()")) {
            assertTrue(rs.next());
            assertEquals("integer", rs.getString(1));
        }
    }

    // =========================================================================
    // Test 8: RAISE with TABLE option
    // PG returns 'users', Memgres returns NULL - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE USING TABLE populates TABLE_NAME in GET STACKED DIAGNOSTICS")
    void raise_with_table() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT raise_with_table()")) {
            assertTrue(rs.next());
            assertEquals("users", rs.getString(1));
        }
    }

    // =========================================================================
    // Test 9: RAISE with SCHEMA option
    // PG returns 'public', Memgres returns NULL - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE USING SCHEMA populates SCHEMA_NAME in GET STACKED DIAGNOSTICS")
    void raise_with_schema() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT raise_with_schema()")) {
            assertTrue(rs.next());
            assertEquals("public", rs.getString(1));
        }
    }

    // =========================================================================
    // Test 10: RAISE with ALL options
    // PG returns all fields correctly
    // Memgres returns NULLs for most - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("RAISE with all USING options populates all GET STACKED DIAGNOSTICS fields")
    void raise_all_options() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM raise_all_options()")) {
            assertTrue(rs.next());
            assertEquals("P0001", rs.getString("state"));
            assertEquals("all options message", rs.getString("msg"));
            assertEquals("all detail", rs.getString("detail"));
            assertEquals("all hint", rs.getString("hint"));
            assertEquals("col_name", rs.getString("col"));
            assertEquals("constr_name", rs.getString("constr"));
            assertEquals("text", rs.getString("dtype"));
            assertEquals("tbl_name", rs.getString("tbl"));
            assertEquals("sch_name", rs.getString("sch"));
        }
    }

    // =========================================================================
    // Test 11: RAISE with no arguments (re-raise)
    // PG catches re-raised division_by_zero
    // Memgres errors with generic PL/pgSQL exception - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("Bare RAISE re-raises the current exception preserving SQLSTATE")
    void raise_reraise() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT raise_reraise()")) {
            assertTrue(rs.next());
            assertEquals("caught re-raised 22012", rs.getString(1));
        }
    }

    // =========================================================================
    // Test 12: Real constraint violation returns TABLE_NAME and CONSTRAINT_NAME
    // PG returns non-empty for both
    // Memgres returns empty for both - INTENDED TO FAIL
    // =========================================================================

    @Test
    @DisplayName("Real PK violation populates TABLE_NAME and CONSTRAINT_NAME in GET STACKED DIAGNOSTICS")
    void raise_real_constraint_diag() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM raise_real_constraint_diag()")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("has_table"), "TABLE_NAME should be non-empty for real constraint violation");
            assertTrue(rs.getBoolean("has_constraint"), "CONSTRAINT_NAME should be non-empty for real constraint violation");
        }
    }
}
