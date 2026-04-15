package com.memgres.compat16;

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
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    @Test
    @DisplayName("Variadic function with single unknown-typed arg should error like PG")
    void testVariadicFunctionSingleUnknownArgNotResolved() {
        // PG errors: "function var_with_prefix(unknown) does not exist"
        // A single unknown-typed argument cannot match the signature (text, VARIADIC text[]).
        // Memgres incorrectly succeeds, so this test is INTENDED TO FAIL.
        assertThrows(SQLException.class, () -> {
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT var_with_prefix('hello') AS result")) {
                rs.next();
            }
        });
    }
}
