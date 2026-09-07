package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Who a policy is for, and what a sequence belongs to once it has been let go.
 *
 * <p>PUBLIC is every role there is, so naming it alongside others says no more than naming it
 * alone: PostgreSQL records the one entry. Listing the others beside it said the policy was for
 * some roles and for everybody at once, which is not a thing a reader can act on.
 *
 * <p>{@code ALTER SEQUENCE ... OWNED BY NONE} takes away the dependency as well as the ownership,
 * so the sequence outlives the table it was made with. Only the ownership was let go, and the
 * sequence was still recognised as the one behind the column's default -- so it went with the
 * table after all, and every later call on it said there was no such relation.
 */
class WhatAPolicyAndASequenceBelongToTest {

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

    /** A policy for PUBLIC and somebody else is a policy for PUBLIC. */
    @Test
    void whoAPolicyIsFor() throws SQLException {
        exec("CREATE TABLE zwq_pol (id int)");
        exec("CREATE ROLE zwq_polr");
        exec("CREATE POLICY zwq_both ON zwq_pol FOR SELECT TO zwq_polr, PUBLIC USING (true)");
        assertEquals("{public}",
                one("SELECT roles::text FROM pg_policies WHERE policyname='zwq_both'"));
        assertEquals("{0}",
                one("SELECT polroles::text FROM pg_policy WHERE polname='zwq_both'"));
        // A policy naming only real roles lists them.
        exec("CREATE POLICY zwq_named ON zwq_pol FOR SELECT TO zwq_polr USING (true)");
        assertEquals("{zwq_polr}",
                one("SELECT roles::text FROM pg_policies WHERE policyname='zwq_named'"));
        // And one naming nobody is for everybody.
        exec("CREATE POLICY zwq_open ON zwq_pol FOR SELECT USING (true)");
        assertEquals("{public}",
                one("SELECT roles::text FROM pg_policies WHERE policyname='zwq_open'"));
        exec("DROP TABLE zwq_pol");
        exec("DROP ROLE zwq_polr");
    }

    /** A sequence let go outlives the table it was made with. */
    @Test
    void whatALetGoSequenceOutlives() throws SQLException {
        exec("CREATE TABLE zwq_seqt (id serial, v int)");
        exec("ALTER SEQUENCE zwq_seqt_id_seq OWNED BY NONE");
        exec("DROP TABLE zwq_seqt");
        assertEquals("1", one("SELECT nextval('zwq_seqt_id_seq')::text"));
        exec("DROP SEQUENCE zwq_seqt_id_seq");
        // One that was never let go goes with its table.
        exec("CREATE TABLE zwq_kept (id serial, v int)");
        exec("DROP TABLE zwq_kept");
        assertEquals("0", one("SELECT count(*)::text FROM pg_class"
                + " WHERE relname='zwq_kept_id_seq'"));
    }
}
