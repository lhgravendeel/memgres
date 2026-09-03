package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the catalogue knows about the objects it holds.
 *
 * <p>A range is not a pair of values: it has an operator class that orders its bounds, a way of
 * folding a discrete range to its canonical form, and a way of measuring the distance between two
 * bounds. A planner reads all three, and only a discrete range has the middle one.
 *
 * <p>A prepared statement remembers the whole statement — what would have to be sent again to
 * prepare the same thing — and the type of every parameter, which for one the PREPARE did not
 * declare is the type the statement casts it to.
 *
 * <p>{@code pg_get_expr} prints the tree that was stored rather than the text that was written, so
 * an index expression comes back with the parentheses the unpretty form puts round an operator.
 */
class WhatTheCatalogueKnowsTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(),
                memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static String one(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getString(1);
        }
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** Each shipped range knows how its bounds are ordered, folded and measured. */
    @Test
    void whatARangeTypeKnowsAboutItsBounds() throws SQLException {
        assertEquals("1978|int4range_canonical|int4range_subdiff",
                one("SELECT rngsubopc::text || '|' || rngcanonical::text || '|'"
                        + " || rngsubdiff::text FROM pg_range"
                        + " WHERE rngtypid = 'int4range'::regtype"));
        // A continuous range has no canonical form to fold to.
        assertEquals("3128|-|tsrange_subdiff",
                one("SELECT rngsubopc::text || '|' || rngcanonical::text || '|'"
                        + " || rngsubdiff::text FROM pg_range"
                        + " WHERE rngtypid = 'tsrange'::regtype"));
        assertEquals("3122|daterange_canonical|daterange_subdiff",
                one("SELECT rngsubopc::text || '|' || rngcanonical::text || '|'"
                        + " || rngsubdiff::text FROM pg_range"
                        + " WHERE rngtypid = 'daterange'::regtype"));
    }

    /** A prepared statement remembers itself whole, and what each parameter is. */
    @Test
    void whatAPreparedStatementRemembers() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("PREPARE zwk_a AS SELECT $1::int");
            s.execute("PREPARE zwk_b (text) AS SELECT $1");
            s.execute("PREPARE zwk_c AS SELECT $1::int + $2::bigint");
            s.execute("PREPARE zwk_d AS SELECT 1");
        }
        assertEquals("PREPARE zwk_a AS SELECT $1::int|{integer}",
                one("SELECT statement || '|' || parameter_types::text"
                        + " FROM pg_prepared_statements WHERE name='zwk_a'"));
        assertEquals("PREPARE zwk_b (text) AS SELECT $1|{text}",
                one("SELECT statement || '|' || parameter_types::text"
                        + " FROM pg_prepared_statements WHERE name='zwk_b'"));
        assertEquals("{integer,bigint}",
                one("SELECT parameter_types::text FROM pg_prepared_statements WHERE name='zwk_c'"));
        assertEquals("{}",
                one("SELECT parameter_types::text FROM pg_prepared_statements WHERE name='zwk_d'"));
        try (Statement s = conn.createStatement()) {
            s.execute("DEALLOCATE ALL");
        }
    }

    /** An index expression comes back as the tree, brackets and all. */
    @Test
    void howAnIndexExpressionIsWrittenBack() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE zwk_t (n int, s text)");
            s.execute("CREATE INDEX zwk_a ON zwk_t ((n + 1))");
            s.execute("CREATE INDEX zwk_b ON zwk_t ((lower(s)))");
            s.execute("CREATE INDEX zwk_c ON zwk_t ((n * 2), (upper(s)))");
        }
        assertEquals("(n + 1)", one("SELECT pg_get_expr(indexprs, indrelid) FROM pg_index"
                + " WHERE indexrelid='zwk_a'::regclass"));
        // A call needs no brackets of its own; only an operator gets them.
        assertEquals("lower(s)", one("SELECT pg_get_expr(indexprs, indrelid) FROM pg_index"
                + " WHERE indexrelid='zwk_b'::regclass"));
        assertEquals("(n * 2), upper(s)", one("SELECT pg_get_expr(indexprs, indrelid) FROM pg_index"
                + " WHERE indexrelid='zwk_c'::regclass"));
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE zwk_t");
        }
    }

    /**
     * A parameter nobody supplied is not there to read.
     *
     * <p>Sent through the extended protocol a {@code $1} never reaches the server as one — the
     * driver reads it as a placeholder of its own — so this is asked in simple query mode, which
     * is where PostgreSQL itself answers the question.
     */
    @Test
    void aParameterNobodySupplied() throws SQLException {
        try (Connection simple = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword())) {
            for (String written : new String[]{"$1", "$70000", "$2000000000"}) {
                try (Statement s = simple.createStatement()) {
                    s.execute("SELECT " + written);
                    fail("expected no parameter " + written);
                } catch (SQLException e) {
                    assertEquals("42P02", e.getSQLState(), written);
                    assertTrue(e.getMessage().contains("there is no parameter " + written), written);
                }
            }
        }
    }

    /** A slot that is not there is not a slot to drop. */
    @Test
    void droppingAReplicationSlotNobodyHas() {
        assertEquals("42704", stateOf("SELECT pg_drop_replication_slot('zwk_noslot')"));
        assertTrue(messageOf("SELECT pg_drop_replication_slot('zwk_noslot')")
                .contains("replication slot \"zwk_noslot\" does not exist"));
    }
}
