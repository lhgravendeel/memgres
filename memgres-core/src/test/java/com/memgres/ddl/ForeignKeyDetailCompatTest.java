package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FK DELETE violation should include DETAIL field in error response.
 */
class ForeignKeyDetailCompatTest {
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
            stmt.execute("CREATE SCHEMA IF NOT EXISTS fkdetail_test");
            stmt.execute("SET search_path = fkdetail_test, public");
            stmt.execute("CREATE TABLE fkdetail_test.fkdetail_parent (id integer PRIMARY KEY)");
            stmt.execute("CREATE TABLE fkdetail_test.fkdetail_child (id integer PRIMARY KEY, parent_id integer REFERENCES fkdetail_test.fkdetail_parent(id))");
            stmt.execute("INSERT INTO fkdetail_test.fkdetail_parent VALUES (1)");
            stmt.execute("INSERT INTO fkdetail_test.fkdetail_child VALUES (1, 1)");

            stmt.execute(
                "CREATE FUNCTION fkdetail_diag(sql_text text) RETURNS TABLE(\n" +
                "  err_sqlstate text, detail text\n" +
                ") LANGUAGE plpgsql AS $$\n" +
                "DECLARE v_state text; v_detail text;\n" +
                "BEGIN\n" +
                "  EXECUTE sql_text;\n" +
                "  err_sqlstate := 'OK'; detail := '';\n" +
                "  RETURN NEXT;\n" +
                "EXCEPTION WHEN OTHERS THEN\n" +
                "  GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_detail = PG_EXCEPTION_DETAIL;\n" +
                "  err_sqlstate := v_state; detail := v_detail;\n" +
                "  RETURN NEXT;\n" +
                "END; $$");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("FK violation DELETE should include detail field")
    void testFkDeleteDetail() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                "SELECT err_sqlstate, (detail IS NOT NULL AND detail <> '') AS has_detail "
                + "FROM fkdetail_diag('DELETE FROM fkdetail_test.fkdetail_parent WHERE id = 1')")) {
            assertTrue(rs.next());
            assertEquals("23503", rs.getString("err_sqlstate"));
            assertTrue(rs.getBoolean("has_detail"),
                    "FK violation on DELETE should include a detail field");
        }
    }
}
