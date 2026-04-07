package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EXCLUDE constraints using GiST indexes.
 *
 * v1 compat had a single basic EXCLUDE test. The real-world schemas use
 * EXCLUDE with function calls (tstzrange, daterange) inside the exclusion
 * elements, plus WITH operators (=, &&), and WHERE clauses.
 */
class ExcludeConstraintCompatTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                "jdbc:postgresql://localhost:" + memgres.getPort() + "/test",
                "test", "test"
        );
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE EXTENSION IF NOT EXISTS btree_gist");
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    // =========================================================================
    // EXCLUDE with tstzrange() function call
    // =========================================================================

    @Test
    void testExcludeWithTstzrangeFunction() throws SQLException {
        // The exact pattern from real schemas:
        // EXCLUDE USING gist (room_id WITH =, tstzrange(starts_at, ends_at, '[)') WITH &&)
        exec("""
            CREATE TABLE shifts (
                id serial PRIMARY KEY,
                rotation_id int NOT NULL,
                starts_at timestamptz NOT NULL,
                ends_at timestamptz NOT NULL,
                CONSTRAINT no_overlap EXCLUDE USING gist (
                    rotation_id WITH =,
                    tstzrange(starts_at, ends_at, '[)'::text) WITH &&
                )
            )
        """);
    }

    @Test
    void testExcludeWithTstzrangeAndCast() throws SQLException {
        exec("""
            CREATE TABLE slots (
                id serial PRIMARY KEY,
                resource_id int NOT NULL,
                start_time timestamptz NOT NULL,
                end_time timestamptz NOT NULL,
                EXCLUDE USING gist (
                    resource_id WITH =,
                    tstzrange(start_time, end_time, '[)'::text) WITH &&
                )
            )
        """);
    }

    // =========================================================================
    // EXCLUDE with daterange() function call
    // =========================================================================

    @Test
    void testExcludeWithDaterangeFunction() throws SQLException {
        exec("""
            CREATE TABLE date_exclusions (
                id serial PRIMARY KEY,
                group_id int NOT NULL,
                start_date date NOT NULL,
                end_date date NOT NULL,
                CONSTRAINT no_date_overlap EXCLUDE USING gist (
                    group_id WITH =,
                    daterange(start_date, end_date, '[]'::text) WITH &&
                )
            )
        """);
    }

    @Test
    void testExcludeWithDaterangeAndWhereClause() throws SQLException {
        // Real-world: EXCLUDE ... WHERE (group_id IS NOT NULL) DEFERRABLE INITIALLY DEFERRED
        exec("""
            CREATE TABLE conditional_exclusions (
                id serial PRIMARY KEY,
                group_id int,
                start_date date NOT NULL,
                end_date date NOT NULL,
                CONSTRAINT cond_no_overlap EXCLUDE USING gist (
                    group_id WITH =,
                    daterange(start_date, end_date, '[]'::text) WITH &&
                ) WHERE (group_id IS NOT NULL)
                  DEFERRABLE INITIALLY DEFERRED
            )
        """);
    }

    // =========================================================================
    // EXCLUDE via ALTER TABLE ADD CONSTRAINT
    // =========================================================================

    @Test
    void testAlterTableAddExcludeWithTstzrange() throws SQLException {
        exec("""
            CREATE TABLE alt_excl (
                id serial PRIMARY KEY,
                rotation_id int NOT NULL,
                starts_at timestamptz NOT NULL,
                ends_at timestamptz NOT NULL
            )
        """);
        exec("""
            ALTER TABLE ONLY alt_excl ADD CONSTRAINT no_overlap_alt
            EXCLUDE USING gist (rotation_id WITH =, tstzrange(starts_at, ends_at, '[)'::text) WITH &&)
        """);
    }

    @Test
    void testAlterTableAddExcludeWithDaterangeAndWhere() throws SQLException {
        exec("""
            CREATE TABLE alt_excl_where (
                id serial PRIMARY KEY,
                cadence_id int,
                start_date date NOT NULL,
                end_date date NOT NULL
            )
        """);
        exec("""
            ALTER TABLE ONLY alt_excl_where ADD CONSTRAINT date_excl
            EXCLUDE USING gist (cadence_id WITH =, daterange(start_date, end_date, '[]'::text) WITH &&)
            WHERE ((cadence_id IS NOT NULL))
            DEFERRABLE INITIALLY DEFERRED
        """);
    }

    // =========================================================================
    // Simple EXCLUDE (no function calls)
    // =========================================================================

    @Test
    void testSimpleExcludeEquality() throws SQLException {
        exec("""
            CREATE TABLE simple_excl (
                id serial PRIMARY KEY,
                room int NOT NULL,
                slot int NOT NULL,
                EXCLUDE USING gist (room WITH =, slot WITH =)
            )
        """);
        exec("INSERT INTO simple_excl (room, slot) VALUES (1, 1)");
        assertThrows(SQLException.class, () ->
                exec("INSERT INTO simple_excl (room, slot) VALUES (1, 1)"));
    }

    // =========================================================================
    // EXCLUDE with int4range
    // =========================================================================

    @Test
    void testExcludeWithInt4range() throws SQLException {
        exec("""
            CREATE TABLE range_excl (
                id serial PRIMARY KEY,
                group_id int NOT NULL,
                range_start int NOT NULL,
                range_end int NOT NULL,
                EXCLUDE USING gist (
                    group_id WITH =,
                    int4range(range_start, range_end) WITH &&
                )
            )
        """);
    }

    // =========================================================================
    // Named EXCLUDE constraint with NOT VALID + VALIDATE
    // =========================================================================

    @Test
    void testExcludeNotValidThenValidate() throws SQLException {
        exec("""
            CREATE TABLE excl_validate (
                id serial PRIMARY KEY,
                room int NOT NULL,
                period daterange NOT NULL
            )
        """);
        exec("ALTER TABLE excl_validate ADD CONSTRAINT no_room_overlap EXCLUDE USING gist (room WITH =, period WITH &&) NOT VALID");
        exec("ALTER TABLE excl_validate VALIDATE CONSTRAINT no_room_overlap");
    }
}
