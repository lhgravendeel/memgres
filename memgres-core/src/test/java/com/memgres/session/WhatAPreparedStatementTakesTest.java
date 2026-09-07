package com.memgres.session;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What a prepared statement says it takes, and how a second key of one shape is named.
 *
 * <p>PostgreSQL settles a prepared statement's parameters from how the statement uses them, not
 * from casts alone: a parameter compared with a column takes that column's type, one assigned to a
 * column takes its, and one standing where a column of an INSERT goes takes that column's. Read
 * from casts only, a statement whose parameters were all used plainly reported that it took no
 * parameters at all, so a client asking what to send was told to send nothing.
 *
 * <p>A parameter standing on its own as an output column has nothing to settle it and nothing that
 * needs settling, so it is read as text. Anywhere else -- under IS NULL, inside a call -- it stays
 * indeterminate and PostgreSQL refuses the statement, which is why text is not the answer
 * everywhere.
 *
 * <p>And two foreign keys over one column are two constraints, which PostgreSQL numbers apart:
 * given one name, the second replaced the first in a catalogue keyed by name.
 */
class WhatAPreparedStatementTakesTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
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

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
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

    private static String typesOf(String name) throws SQLException {
        return one("SELECT parameter_types::text FROM pg_prepared_statements"
                + " WHERE name = '" + name + "'");
    }

    /** A parameter takes the type of the column it is used with. */
    @Test
    void whatAStatementSaysItTakes() throws SQLException {
        exec("CREATE TABLE zwt_t (id int, s text, d date)");
        exec("PREPARE zwt_a AS SELECT id FROM zwt_t WHERE id = $1");
        assertEquals("{integer}", typesOf("zwt_a"));
        exec("PREPARE zwt_b AS SELECT id FROM zwt_t WHERE s = $1 AND d > $2");
        assertEquals("{text,date}", typesOf("zwt_b"));
        exec("PREPARE zwt_c AS INSERT INTO zwt_t VALUES ($1, $2, $3)");
        assertEquals("{integer,text,date}", typesOf("zwt_c"));
        exec("PREPARE zwt_d AS UPDATE zwt_t SET s = $1 WHERE id = $2");
        assertEquals("{text,integer}", typesOf("zwt_d"));
        // A declared type is still the declared type, and a cast still settles one.
        exec("PREPARE zwt_e (bigint) AS SELECT id FROM zwt_t WHERE id = $1");
        assertEquals("{bigint}", typesOf("zwt_e"));
        exec("PREPARE zwt_f AS SELECT id FROM zwt_t WHERE id = $1::bigint");
        assertEquals("{bigint}", typesOf("zwt_f"));
        // And the statement still runs with what it says it takes.
        exec("INSERT INTO zwt_t VALUES (1,'x','2020-01-01')");
        assertEquals("1", one("EXECUTE zwt_a (1)"));
        exec("DEALLOCATE ALL");
        exec("DROP TABLE zwt_t");
    }

    /** A parameter written as an output column and nowhere else is text. */
    @Test
    void whatABareParameterIs() throws SQLException {
        exec("PREPARE zwt_g AS SELECT $1");
        assertEquals("{text}", typesOf("zwt_g"));
        exec("DEALLOCATE zwt_g");
        // Written anywhere else, it stays indeterminate and the statement is refused.
        assertEquals("42P18", stateOf("PREPARE zwt_h AS SELECT $1 IS NULL, pg_typeof($1)"));
        assertEquals("42P18", stateOf("PREPARE zwt_i AS SELECT $1 IS NULL"));
        assertEquals("42P18", stateOf("PREPARE zwt_j AS SELECT pg_typeof($1)"));
        // A parameter the statement never writes is one nothing can settle.
        assertEquals("42P18", stateOf("PREPARE zwt_k AS SELECT $2"));
    }

    /** Two foreign keys over one column are named apart. */
    @Test
    void howASecondKeyOfOneShapeIsNamed() throws SQLException {
        exec("CREATE TABLE zwt_pk (a int PRIMARY KEY)");
        exec("CREATE TABLE zwt_fk (a int REFERENCES zwt_pk(a),"
                + " FOREIGN KEY (a) REFERENCES zwt_pk(a))");
        assertEquals("zwt_fk_a_fkey|zwt_fk_a_fkey1",
                one("SELECT string_agg(conname, '|' ORDER BY conname) FROM pg_constraint"
                        + " WHERE conrelid='zwt_fk'::regclass AND contype='f'"));
        exec("DROP TABLE zwt_fk");
        exec("DROP TABLE zwt_pk");
    }
}
