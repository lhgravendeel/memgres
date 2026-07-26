package com.memgres.cursor;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A cursor can be declared for any query, and a refcursor returned from a function leaves a
 * portal the caller can FETCH from. Expectations captured from a live PostgreSQL 18.0 server.
 *
 * <p>N57 DECLARE CURSOR FOR VALUES, N36 refcursor returned from PL/pgSQL.
 */
class CursorValuesAndRefcursorTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE cvr_t (id int PRIMARY KEY)");
        exec("INSERT INTO cvr_t VALUES (1),(2)");
        exec("CREATE FUNCTION cvr_open() RETURNS refcursor AS $$ "
                + "DECLARE c refcursor := 'cvrcur'; "
                + "BEGIN OPEN c FOR SELECT id FROM cvr_t ORDER BY id; RETURN c; END $$ LANGUAGE plpgsql");
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }

    /** A refcursor's value is its portal name, and the portal outlives the function call. */
    @Test
    void aRefcursorReturnedFromAFunctionIsUsable() throws Exception {
        conn.setAutoCommit(false);
        try {
            assertEquals(Arrays.asList("cvrcur"), rows("SELECT cvr_open()::text"));
            assertEquals(Arrays.asList("1", "2"), rows("FETCH ALL FROM cvrcur"));
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void aCursorMayBeDeclaredForValues() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE cvr_c CURSOR FOR VALUES (1),(2)");
            assertEquals(Arrays.asList("1", "2"), rows("FETCH ALL FROM cvr_c"));
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void aCursorMayBeDeclaredForASetOperation() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE cvr_u CURSOR FOR SELECT 1 UNION ALL SELECT 2");
            assertEquals(Arrays.asList("1", "2"), rows("FETCH ALL FROM cvr_u"));
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void anOrdinarySelectCursorStillWorks() throws Exception {
        conn.setAutoCommit(false);
        try {
            exec("DECLARE cvr_s CURSOR FOR SELECT id FROM cvr_t ORDER BY id");
            assertEquals(Arrays.asList("1"), rows("FETCH 1 FROM cvr_s"));
            assertEquals(Arrays.asList("2"), rows("FETCH 1 FROM cvr_s"));
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }
}
