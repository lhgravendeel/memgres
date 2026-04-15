package com.memgres.plpgsql;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class VariadicCompatTest {
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
            stmt.execute(
                    "CREATE FUNCTION var_with_prefix(prefix text, VARIADIC items text[]) RETURNS text LANGUAGE SQL AS $$\n" +
                    "  SELECT prefix || ':' || array_to_string(items, ',');\n" +
                    "$$;");

            stmt.execute("CREATE FUNCTION var_concat_all(VARIADIC parts text[]) RETURNS text "
                    + "LANGUAGE plpgsql AS $$ DECLARE result text := ''; p text; BEGIN "
                    + "FOREACH p IN ARRAY parts LOOP result := result || p; END LOOP; "
                    + "RETURN result; END; $$");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("single unknown-typed arg should not resolve to VARIADIC function (42883)")
    void testVariadicSingleArgReject() {
        SQLException ex = assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT var_with_prefix('hello') AS result");
            }
        });
        assertEquals("42883", ex.getSQLState(),
                "Should get 'function does not exist' error, got: " + ex.getMessage());
    }

    @Test
    @DisplayName("VARIADIC function with explicit empty array resolves correctly")
    void testVariadicEmptyArray() throws Exception {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT var_concat_all(VARIADIC ARRAY[]::text[]) AS result")) {
            assertTrue(rs.next());
            assertEquals("", rs.getString("result"));
        }
    }
}
