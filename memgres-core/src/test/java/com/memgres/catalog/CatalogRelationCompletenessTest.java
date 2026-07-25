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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Relations and attributes PostgreSQL registers but memgres used to omit: composite-type
 * pg_class rows, the index backing an EXCLUDE constraint, pg_attribute rows for sequences
 * and indexes, and information_schema.parameters.
 *
 * <p>All expectations were captured from a live PostgreSQL 18.0 server.
 */
class CatalogRelationCompletenessTest {

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

    // ------------------------------------------------------------------
    // Composite types are relations too (relkind 'c')
    // ------------------------------------------------------------------

    @Test
    void compositeTypeHasPgClassRow() throws Exception {
        exec("CREATE TYPE cr_comp AS (a int, b text)");
        assertEquals(java.util.Arrays.asList("cr_comp|c|2"),
                rows("SELECT relname, relkind, relnatts FROM pg_class WHERE relname = 'cr_comp'"));
    }

    /** pg_type.typrelid must resolve to that pg_class row, and on to its attributes. */
    @Test
    void compositeTypeJoinsThroughTyprelid() throws Exception {
        exec("CREATE TYPE cr_comp2 AS (x int, y text)");
        assertEquals(java.util.Arrays.asList("x|int4", "y|text"),
                rows("SELECT a.attname, ty.typname FROM pg_type t "
                        + "JOIN pg_class c ON c.oid = t.typrelid "
                        + "JOIN pg_attribute a ON a.attrelid = c.oid "
                        + "JOIN pg_type ty ON ty.oid = a.atttypid "
                        + "WHERE t.typname = 'cr_comp2' AND a.attnum > 0 ORDER BY a.attnum"));
    }

    // ------------------------------------------------------------------
    // EXCLUDE constraints are backed by a real index
    // ------------------------------------------------------------------

    @Test
    void excludeConstraintHasBackingIndex() throws Exception {
        exec("CREATE TABLE cr_t (id int primary key, during tsrange, "
                + "EXCLUDE USING gist (during WITH &&))");
        // PG names it <table>_<cols>_excl and registers a gist index relation for it.
        assertEquals(java.util.Arrays.asList("cr_t_during_excl|i|1"),
                rows("SELECT relname, relkind, relnatts FROM pg_class "
                        + "WHERE relname = 'cr_t_during_excl'"));
        assertEquals(java.util.Arrays.asList("cr_t_during_excl|f|t|1"),
                rows("SELECT c.relname, i.indisunique, i.indisexclusion, i.indnkeyatts "
                        + "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
                        + "WHERE c.relname = 'cr_t_during_excl'"));
    }

    @Test
    void excludeConstraintPointsAtItsIndex() throws Exception {
        exec("CREATE TABLE cr_t2 (during tsrange, "
                + "EXCLUDE USING gist (during WITH &&))");
        assertEquals(java.util.Arrays.asList(
                        "cr_t2_during_excl|x|t|EXCLUDE USING gist (during WITH &&)"),
                rows("SELECT conname, contype, conindid <> 0, pg_get_constraintdef(oid) "
                        + "FROM pg_constraint WHERE conname = 'cr_t2_during_excl'"));
    }

    /** Without USING, PG builds the exclusion index with the default access method, btree. */
    @Test
    void excludeDefaultsToBtree() throws Exception {
        exec("CREATE TABLE cr_e (a int, b int, EXCLUDE (a WITH =))");
        assertEquals(java.util.Arrays.asList("cr_e_a_excl|EXCLUDE USING btree (a WITH =)"),
                rows("SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
                        + "WHERE conname = 'cr_e_a_excl'"));
    }

    // ------------------------------------------------------------------
    // Sequence attributes
    // ------------------------------------------------------------------

    @Test
    void sequenceHasItsThreeAttributes() throws Exception {
        exec("CREATE SEQUENCE cr_seq");
        assertEquals(java.util.Arrays.asList(
                        "last_value|1|int8|t", "log_cnt|2|int8|t", "is_called|3|bool|t"),
                rows("SELECT a.attname, a.attnum, t.typname, a.attnotnull "
                        + "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                        + "JOIN pg_type t ON t.oid = a.atttypid "
                        + "WHERE c.relname = 'cr_seq' AND a.attnum > 0 ORDER BY a.attnum"));
    }

    /** pg_class already claims relnatts=3 for a sequence; pg_attribute must agree. */
    @Test
    void sequenceRelnattsMatchesAttributeCount() throws Exception {
        exec("CREATE SEQUENCE cr_seq2");
        assertEquals(java.util.Arrays.asList("3|3"),
                rows("SELECT c.relnatts, (SELECT count(*) FROM pg_attribute a "
                        + "WHERE a.attrelid = c.oid AND a.attnum > 0) FROM pg_class c "
                        + "WHERE c.relname = 'cr_seq2'"));
    }

    // ------------------------------------------------------------------
    // Index attributes
    // ------------------------------------------------------------------

    @Test
    void indexHasOneAttributePerKeyColumn() throws Exception {
        exec("CREATE TABLE cr_ix (id int, name text, vc varchar(10), qty int)");
        exec("CREATE INDEX crx1 ON cr_ix (lower(name))");
        exec("CREATE INDEX crx2 ON cr_ix ((qty + 1))");
        exec("CREATE INDEX crx3 ON cr_ix ((qty::text))");
        exec("CREATE INDEX crx4 ON cr_ix (name, qty)");

        // PG names an expression column after its top-level function, after the underlying
        // column for a cast, and "expr" otherwise.
        assertEquals(java.util.Arrays.asList(
                        "crx1|lower|1|text",
                        "crx2|expr|1|int4",
                        "crx3|qty|1|text",
                        "crx4|name|1|text",
                        "crx4|qty|2|int4"),
                rows("SELECT c.relname, a.attname, a.attnum, t.typname "
                        + "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                        + "JOIN pg_type t ON t.oid = a.atttypid "
                        + "WHERE c.relname LIKE 'crx%' AND a.attnum > 0 "
                        + "ORDER BY c.relname, a.attnum"));
    }

    @Test
    void primaryKeyIndexHasAttributes() throws Exception {
        exec("CREATE TABLE cr_pk (id int primary key, v text)");
        assertEquals(java.util.Arrays.asList("id|1|int4"),
                rows("SELECT a.attname, a.attnum, t.typname "
                        + "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                        + "JOIN pg_type t ON t.oid = a.atttypid "
                        + "WHERE c.relname = 'cr_pk_pkey' AND a.attnum > 0 ORDER BY a.attnum"));
    }

    // ------------------------------------------------------------------
    // information_schema.parameters
    // ------------------------------------------------------------------

    @Test
    void parametersListsEveryArgument() throws Exception {
        exec("CREATE FUNCTION cr_f1(x int, y text) RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql");

        assertEquals(java.util.Arrays.asList(
                        "1|IN|x|integer|int4", "2|IN|y|text|text"),
                rows("SELECT p.ordinal_position, p.parameter_mode, p.parameter_name, p.data_type, p.udt_name "
                        + "FROM information_schema.parameters p "
                        + "JOIN information_schema.routines r USING (specific_name) "
                        + "WHERE r.routine_name = 'cr_f1' ORDER BY p.ordinal_position"));
    }

    @Test
    void parametersReportsOutAndInoutModes() throws Exception {
        exec("CREATE FUNCTION cr_f2(a int, OUT b int, INOUT c text) AS $$ BEGIN b := 1; END; $$ LANGUAGE plpgsql");

        assertEquals(java.util.Arrays.asList("1|IN|a", "2|OUT|b", "3|INOUT|c"),
                rows("SELECT p.ordinal_position, p.parameter_mode, p.parameter_name "
                        + "FROM information_schema.parameters p "
                        + "JOIN information_schema.routines r USING (specific_name) "
                        + "WHERE r.routine_name = 'cr_f2' ORDER BY p.ordinal_position"));
    }

    @Test
    void parametersJoinsRoutinesOnSpecificName() throws Exception {
        exec("CREATE FUNCTION cr_f3(q int) RETURNS int AS $$ BEGIN RETURN q; END; $$ LANGUAGE plpgsql");
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT count(*) FROM information_schema.parameters p "
                             + "JOIN information_schema.routines r USING (specific_name) "
                             + "WHERE r.routine_name = 'cr_f3'")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1), "parameters must join routines on specific_name");
        }
    }
}
