package com.memgres.catalog;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The numbers and names the catalogue records about a key, a cast and an extension.
 *
 * <p>An index's {@code indclass} and a partitioned table's {@code partclass} are the numbers
 * PostgreSQL pins its operator classes to, and a reader joins them back to pg_opclass by them.
 * Minted per database instead, they agreed with nothing a real server reports; reported as zero,
 * a partition key said it was compared by nothing at all.
 *
 * <p>A cast written WITH FUNCTION names the routine that performs it, and a {@code regprocedure}
 * names that routine's whole signature -- which is what tells two overloads apart.
 *
 * <p>An extension owns its routines, and that ownership is what a reader follows from pg_depend to
 * find what an extension brought.
 *
 * <p>A comment is written into the catalogue, so it changes the database as much as any DDL does.
 * Extended statistics are collected, not declared, so nothing shows until an ANALYZE has run. And a
 * relation's triggers fire in name order, which is what {@code action_order} says.
 */
class WhatTheCatalogueRecordsAboutAKeyTest {

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

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int width = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder line = new StringBuilder();
                for (int i = 1; i <= width; i++) {
                    if (i > 1) line.append('|');
                    line.append(rs.getString(i));
                }
                out.add(line.toString());
            }
        }
        return out;
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

    /** An operator class carries the number PostgreSQL pins it to. */
    @Test
    void whichNumberAnOperatorClassCarries() throws SQLException {
        assertEquals(java.util.Arrays.asList("10006|bytea_ops", "3122|date_ops", "1978|int4_ops",
                        "3125|numeric_ops", "3126|text_ops"),
                rows("SELECT o.oid::text, o.opcname FROM pg_opclass o"
                        + " JOIN pg_type t ON t.oid=o.opcintype JOIN pg_am a ON a.oid=o.opcmethod"
                        + " WHERE o.opcdefault AND a.amname='btree'"
                        + " AND t.typname IN ('int4','bytea','text','numeric','date')"
                        + " ORDER BY t.typname"));
        // A partition key resolves through the default class of its column's type.
        exec("CREATE TABLE zwk_p (a int, b text) PARTITION BY RANGE (a, b)");
        assertEquals("2|1 2|1978 3126|0 0",
                one("SELECT partnatts || '|' || partattrs::text || '|' || partclass::text"
                        + " || '|' || partcollation::text FROM pg_partitioned_table"
                        + " WHERE partrelid = 'zwk_p'::regclass"));
        exec("DROP TABLE zwk_p");
        // An index's class numbers reach rows pg_opclass really has.
        exec("CREATE TABLE zwk_t (a int, b text)");
        exec("CREATE INDEX zwk_bi ON zwk_t (a)");
        exec("CREATE INDEX zwk_bs ON zwk_t (b)");
        assertEquals(java.util.Arrays.asList("zwk_bi|int4_ops", "zwk_bs|text_ops"),
                rows("SELECT c.relname, (SELECT opcname FROM pg_opclass o"
                        + " WHERE o.oid = i.indclass[0])"
                        + " FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid"
                        + " WHERE i.indrelid='zwk_t'::regclass AND c.relname LIKE 'zwk_b%'"
                        + " ORDER BY 1"));
        exec("DROP TABLE zwk_t");
    }

    /** A cast names the routine that performs it, by its whole signature. */
    @Test
    void whichRoutineACastNames() throws SQLException {
        exec("CREATE TYPE zwk_e AS ENUM ('a','b')");
        exec("CREATE FUNCTION zwk_ef(zwk_e) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$");
        exec("CREATE CAST (zwk_e AS int4) WITH FUNCTION zwk_ef(zwk_e) AS ASSIGNMENT");
        assertEquals("a|f|zwk_ef(zwk_e)",
                one("SELECT castcontext::text || '|' || castmethod::text || '|'"
                        + " || castfunc::regprocedure::text FROM pg_cast"
                        + " WHERE castsource='zwk_e'::regtype AND casttarget='int4'::regtype"));
        exec("DROP CAST (zwk_e AS int4)");
        exec("DROP FUNCTION zwk_ef(zwk_e)");
        exec("DROP TYPE zwk_e");
    }

    /** An extension owns what it brought, and pg_depend records it. */
    @Test
    void whatAnExtensionBrought() throws SQLException {
        exec("CREATE EXTENSION IF NOT EXISTS pgcrypto");
        assertEquals("true", one("SELECT (count(*) > 0)::text FROM pg_depend d"
                + " JOIN pg_extension e ON e.oid=d.refobjid"
                + " WHERE e.extname='pgcrypto' AND d.deptype='e'"));
        assertEquals("true", one("SELECT (count(*) > 0)::text FROM pg_depend d"
                + " JOIN pg_extension e ON e.oid=d.refobjid"
                + " WHERE e.extname='plpgsql' AND d.deptype='e'"));
        // An extension nobody installed brought nothing.
        assertEquals("false", one("SELECT (count(*) > 0)::text FROM pg_depend d"
                + " JOIN pg_extension e ON e.oid=d.refobjid"
                + " WHERE e.extname='unaccent' AND d.deptype='e'"));
    }

    /** A comment changes the database, so a read-only transaction refuses it. */
    @Test
    void whatAReadOnlyTransactionRefuses() throws SQLException {
        exec("CREATE TABLE zwk_ro (i int)");
        // Each refusal aborts the transaction, so each is asked for in one of its own.
        for (String refused : new String[]{"COMMENT ON TABLE zwk_ro IS 'hi'",
                "SECURITY LABEL ON TABLE zwk_ro IS 'x'", "CREATE TABLE zwk_ro2 (i int)"}) {
            exec("BEGIN READ ONLY");
            try {
                assertEquals("25006", stateOf(refused), refused);
            } finally {
                exec("ROLLBACK");
            }
        }
        assertNull(stateOf("COMMENT ON TABLE zwk_ro IS 'hi'"));
        exec("DROP TABLE zwk_ro");
    }

    /** Extended statistics show what has been gathered, not what was declared. */
    @Test
    void whenExtendedStatisticsAppear() throws SQLException {
        exec("CREATE TABLE zwk_st (a int, b int)");
        exec("INSERT INTO zwk_st VALUES (1,1),(2,2)");
        exec("CREATE STATISTICS zwk_ks1 ON a, b FROM zwk_st");
        assertEquals("0",
                one("SELECT count(*)::text FROM pg_stats_ext WHERE statistics_name='zwk_ks1'"));
        exec("ANALYZE zwk_st");
        assertEquals("{a,b}",
                one("SELECT attnames::text FROM pg_stats_ext WHERE statistics_name='zwk_ks1'"));
        exec("DROP STATISTICS zwk_ks1");
        exec("DROP TABLE zwk_st");
    }

    /** A relation's triggers are numbered in the order they fire. */
    @Test
    void inWhatOrderTriggersFire() throws SQLException {
        exec("CREATE TABLE zwk_ca (i int)");
        exec("CREATE FUNCTION zwk_tf() RETURNS trigger LANGUAGE plpgsql AS"
                + " $$ BEGIN RETURN NEW; END $$");
        exec("CREATE TRIGGER zwk_t1 BEFORE INSERT ON zwk_ca FOR EACH ROW"
                + " EXECUTE FUNCTION zwk_tf()");
        exec("CREATE TRIGGER zwk_t2 BEFORE INSERT ON zwk_ca FOR EACH ROW"
                + " EXECUTE FUNCTION zwk_tf()");
        assertEquals(java.util.Arrays.asList("zwk_t1|1", "zwk_t2|2"),
                rows("SELECT trigger_name, action_order::text FROM information_schema.triggers"
                        + " WHERE event_object_table='zwk_ca' ORDER BY trigger_name"));
        exec("DROP TABLE zwk_ca");
        exec("DROP FUNCTION zwk_tf()");
    }

    /** A membership a session may use is one applicable_roles lists. */
    @Test
    void whichMembershipsAreApplicable() throws SQLException {
        exec("CREATE ROLE zwk_r");
        exec("CREATE ROLE zwk_r2");
        exec("CREATE ROLE zwk_r3");
        exec("GRANT zwk_r2 TO zwk_r");
        exec("GRANT zwk_r3 TO zwk_r WITH ADMIN OPTION");
        assertEquals(java.util.Arrays.asList("zwk_r2|NO", "zwk_r3|YES"),
                rows("SELECT role_name, is_grantable FROM information_schema.applicable_roles"
                        + " WHERE grantee='zwk_r' ORDER BY 1"));
        assertEquals("3", one("SELECT count(*)::text FROM information_schema.enabled_roles"
                + " WHERE role_name LIKE 'zwk%'"));
        exec("DROP ROLE zwk_r");
        exec("DROP ROLE zwk_r2");
        exec("DROP ROLE zwk_r3");
    }
}
