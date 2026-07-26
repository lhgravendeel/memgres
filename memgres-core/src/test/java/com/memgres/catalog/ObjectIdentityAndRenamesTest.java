package com.memgres.catalog;

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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A relation keeps its identity across a rename or a schema move: its foreign keys keep
 * enforcing, its views keep reading it, and comments stay reachable. Expectations captured
 * from a live PostgreSQL 18.0 server.
 *
 * <p>N20 rename/SET SCHEMA dependents, N21 duplicate enum labels, N37 temp object identity,
 * N43 COMMENT retrieval beyond table columns.
 */
class ObjectIdentityAndRenamesTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private static String state(String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        return e.getSQLState();
    }

    // ------------------------------------------------------------------
    // N20 — RENAME and SET SCHEMA keep dependents attached
    // ------------------------------------------------------------------

    @Test
    void renameKeepsForeignKeysAndViewsAttached() throws Exception {
        exec("CREATE TABLE oi_p (id int PRIMARY KEY, v text)");
        exec("CREATE TABLE oi_c (id int PRIMARY KEY, p int REFERENCES oi_p(id))");
        exec("INSERT INTO oi_p VALUES (1,'x'),(2,'y')");
        exec("INSERT INTO oi_c VALUES (10,1)");
        exec("CREATE VIEW oi_v AS SELECT id, v FROM oi_p");

        exec("ALTER TABLE oi_p RENAME TO oi_p2");

        assertEquals(Arrays.asList("2"), rows("SELECT count(*) FROM oi_v"),
                "the view still reads the renamed relation");
        assertEquals(Arrays.asList("oi_p2"), rows(
                "SELECT confrelid::regclass::text FROM pg_constraint"
                        + " WHERE conrelid = 'oi_c'::regclass AND contype = 'f'"));

        exec("INSERT INTO oi_c VALUES (11,2)");
        assertEquals("23503", state("INSERT INTO oi_c VALUES (12,99)"),
                "the FK still rejects a missing parent key");
        assertEquals("23503", state("DELETE FROM oi_p2 WHERE id = 1"),
                "the FK still restricts the parent delete");
    }

    @Test
    void setSchemaKeepsDependentsAndFreesTheOldName() throws Exception {
        exec("CREATE TABLE oi_mp (id int PRIMARY KEY)");
        exec("CREATE TABLE oi_mc (id int PRIMARY KEY, p int REFERENCES oi_mp(id))");
        exec("INSERT INTO oi_mp VALUES (1),(2)");
        exec("CREATE VIEW oi_mv AS SELECT id FROM oi_mp");
        exec("CREATE SCHEMA oi_s");

        exec("ALTER TABLE oi_mp SET SCHEMA oi_s");

        assertEquals(Arrays.asList("2"), rows("SELECT count(*) FROM oi_mv"));
        exec("INSERT INTO oi_mc VALUES (5,2)");
        assertEquals("23503", state("INSERT INTO oi_mc VALUES (6,99)"));
        assertEquals("42P01", state("SELECT count(*) FROM oi_mp"),
                "the relation is gone from its old schema");
        assertEquals(Arrays.asList("1", "2"), rows("SELECT id FROM oi_s.oi_mp ORDER BY id"));
    }

    // ------------------------------------------------------------------
    // N21 — enum label renames are validated
    // ------------------------------------------------------------------

    @Test
    void renamingAnEnumLabelOntoAnExistingOneIsRejected() throws Exception {
        exec("CREATE TYPE oi_e AS ENUM ('a','b','c')");

        assertEquals("42710", state("ALTER TYPE oi_e RENAME VALUE 'a' TO 'b'"));
        assertEquals(Arrays.asList("{a,b,c}"), rows("SELECT enum_range(NULL::oi_e)::text"),
                "the rejected rename left the type untouched");

        exec("ALTER TYPE oi_e RENAME VALUE 'a' TO 'z'");
        assertEquals(Arrays.asList("{z,b,c}"), rows("SELECT enum_range(NULL::oi_e)::text"));
    }

    @Test
    void renamingAMissingEnumLabelIsRejected() throws Exception {
        exec("CREATE TYPE oi_e2 AS ENUM ('a','b')");
        assertEquals("22023", state("ALTER TYPE oi_e2 RENAME VALUE 'nope' TO 'q'"));
    }

    // ------------------------------------------------------------------
    // N43 — comments on non-table objects come back
    // ------------------------------------------------------------------

    @Test
    void commentsOnNonTableObjectsAreRetrievable() throws Exception {
        exec("CREATE TABLE oi_ct (id int PRIMARY KEY, p int)");
        exec("CREATE TABLE oi_cp (id int PRIMARY KEY)");
        exec("ALTER TABLE oi_ct ADD CONSTRAINT oi_ct_fk FOREIGN KEY (p) REFERENCES oi_cp(id)");
        exec("CREATE VIEW oi_cv AS SELECT id, p FROM oi_ct");
        exec("CREATE DOMAIN oi_dom AS int CHECK (VALUE > 0)");
        exec("CREATE FUNCTION oi_fn(int) RETURNS int AS $$ SELECT $1 + 1 $$ LANGUAGE sql");
        exec("CREATE TYPE oi_enum AS ENUM ('q')");

        exec("COMMENT ON COLUMN oi_cv.p IS 'view column comment'");
        exec("COMMENT ON CONSTRAINT oi_ct_fk ON oi_ct IS 'fk comment'");
        exec("COMMENT ON FUNCTION oi_fn(int) IS 'function comment'");
        exec("COMMENT ON DOMAIN oi_dom IS 'domain comment'");
        exec("COMMENT ON TYPE oi_enum IS 'enum comment'");

        assertEquals(Arrays.asList("view column comment"),
                rows("SELECT col_description('oi_cv'::regclass, 2)"));
        assertEquals(Arrays.asList("fk comment"), rows(
                "SELECT obj_description(oid, 'pg_constraint') FROM pg_constraint WHERE conname = 'oi_ct_fk'"));
        assertEquals(Arrays.asList("function comment"), rows(
                "SELECT obj_description(oid, 'pg_proc') FROM pg_proc WHERE proname = 'oi_fn'"));
        assertEquals(Arrays.asList("domain comment"), rows(
                "SELECT obj_description(oid, 'pg_type') FROM pg_type WHERE typname = 'oi_dom'"));
        assertEquals(Arrays.asList("enum comment"), rows(
                "SELECT obj_description(oid, 'pg_type') FROM pg_type WHERE typname = 'oi_enum'"));
    }

    // ------------------------------------------------------------------
    // N37 — temp objects have a pg_temp identity
    // ------------------------------------------------------------------

    @Test
    void tempTableIsMarkedTemporaryAndResolvesAsRegclass() throws Exception {
        exec("CREATE TEMP TABLE oi_tt (id int)");
        exec("INSERT INTO oi_tt VALUES (1)");

        assertEquals(Arrays.asList("t|t"), rows(
                "SELECT relpersistence, relnamespace::regnamespace::text LIKE 'pg\\_temp%'"
                        + " FROM pg_class WHERE relname = 'oi_tt'"));
        assertEquals(Arrays.asList("oi_tt"), rows("SELECT 'oi_tt'::regclass::text"));
        assertEquals(Arrays.asList("t|f"), rows(
                "SELECT pg_my_temp_schema() <> 0, pg_is_other_temp_schema(pg_my_temp_schema())"));
    }

    @Test
    void viewOverATempTableIsItselfTemporary() throws Exception {
        exec("CREATE TEMP TABLE oi_tt2 (id int)");
        exec("CREATE VIEW oi_tv AS SELECT * FROM oi_tt2");

        assertEquals(Arrays.asList("t|t"), rows(
                "SELECT relpersistence, relnamespace::regnamespace::text LIKE 'pg\\_temp%'"
                        + " FROM pg_class WHERE relname = 'oi_tv'"));
    }
}
