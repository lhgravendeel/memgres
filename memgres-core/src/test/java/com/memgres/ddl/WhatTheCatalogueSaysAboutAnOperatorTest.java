package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What is read first when an operator is declared, and what the catalogue says about it.
 *
 * <p>An operator's operand types are read before its function is looked for: an operator over a
 * type nobody declared names no type, and there is no signature to look for until both sides name
 * one. And RESTRICT and JOIN name the routines a planner asks how selective the operator is;
 * reported as absent whatever was written, an operator declared with eqsel said the planner had
 * nothing to estimate with.
 *
 * <p>An operator the reader declared may stand in front of ANY or ALL like any other, and a view
 * of the catalogue has no system columns -- there is no row of its own to have a ctid.
 *
 * <p>A missing type is quoted wherever PostgreSQL looks one up, except in a routine's parameter
 * list, where the list is written back as it was read.
 */
class WhatTheCatalogueSaysAboutAnOperatorTest {

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

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return "";
        } catch (SQLException e) {
            return e.getMessage() == null ? "" : e.getMessage();
        }
    }

    /** An operator's operand types are read before its function is looked for. */
    @Test
    void whatIsReadFirstWhenAnOperatorIsDeclared() throws SQLException {
        exec("CREATE FUNCTION zwo_add(int, int) RETURNS int LANGUAGE sql IMMUTABLE"
                + " AS $$ SELECT $1 + $2 $$");
        assertEquals("42704", stateOf("CREATE OPERATOR ###? (LEFTARG = zwo_notype,"
                + " RIGHTARG = int, FUNCTION = zwo_add)"));
        assertTrue(messageOf("CREATE OPERATOR ###? (LEFTARG = zwo_notype,"
                + " RIGHTARG = int, FUNCTION = zwo_add)")
                .contains("type \"zwo_notype\" does not exist"));
        exec("DROP FUNCTION zwo_add(int, int)");
    }

    /** A routine's parameter list names a missing type without quotes. */
    @Test
    void howAMissingTypeIsNamed() {
        assertTrue(messageOf("CREATE FUNCTION zwo_a(zwo_no) RETURNS int LANGUAGE sql"
                + " AS $$ SELECT 1 $$").contains("type zwo_no does not exist"));
        // Everywhere else it is quoted.
        assertTrue(messageOf("CREATE FUNCTION zwo_b() RETURNS zwo_no LANGUAGE sql"
                + " AS $$ SELECT 1 $$").contains("type \"zwo_no\" does not exist"));
        assertTrue(messageOf("CREATE TABLE zwo_t (a zwo_no)")
                .contains("type \"zwo_no\" does not exist"));
        assertTrue(messageOf("CREATE DOMAIN zwo_d AS zwo_no")
                .contains("type \"zwo_no\" does not exist"));
    }

    /** The estimators an operator was declared with are the ones the catalogue reports. */
    @Test
    void whichEstimatorsTheCatalogueReports() throws SQLException {
        exec("CREATE FUNCTION zwo_eq(int,int) RETURNS bool LANGUAGE sql IMMUTABLE"
                + " AS $$ SELECT $1 = $2 $$");
        exec("CREATE OPERATOR ###@ (LEFTARG = int, RIGHTARG = int, FUNCTION = zwo_eq,"
                + " RESTRICT = eqsel, JOIN = eqjoinsel)");
        assertEquals(java.util.Arrays.asList("zwo_eq|eqsel|eqjoinsel"),
                rows("SELECT oprcode::text, oprrest::text, oprjoin::text FROM pg_operator"
                        + " WHERE oprname='###@'"));
        // One declared with neither still prints a dash rather than a zero.
        exec("CREATE OPERATOR ###! (LEFTARG = int, RIGHTARG = int, FUNCTION = zwo_eq)");
        assertEquals(java.util.Arrays.asList("zwo_eq|-|-"),
                rows("SELECT oprcode::text, oprrest::text, oprjoin::text FROM pg_operator"
                        + " WHERE oprname='###!'"));
        exec("DROP OPERATOR ###! (int,int)");
        exec("DROP OPERATOR ###@ (int,int)");
        exec("DROP FUNCTION zwo_eq(int,int)");
    }

    /** An operator the reader declared may stand in front of ANY and ALL. */
    @Test
    void aDeclaredOperatorInFrontOfASet() throws SQLException {
        exec("CREATE FUNCTION zwo_same(int,int) RETURNS bool LANGUAGE sql IMMUTABLE"
                + " AS $$ SELECT $1 = $2 $$");
        exec("CREATE OPERATOR ###= (LEFTARG = int, RIGHTARG = int, FUNCTION = zwo_same)");
        exec("CREATE TABLE zwo_t2 (a int, b int)");
        exec("INSERT INTO zwo_t2 VALUES (1,1),(2,3)");
        assertEquals(java.util.Arrays.asList("1"),
                rows("SELECT a FROM zwo_t2 WHERE a ###= ANY (SELECT b FROM zwo_t2) ORDER BY a"));
        assertEquals(java.util.Arrays.asList(),
                rows("SELECT a FROM zwo_t2 WHERE a ###= ALL (SELECT b FROM zwo_t2) ORDER BY a"));
        assertEquals(java.util.Arrays.asList("2"),
                rows("SELECT a FROM zwo_t2 WHERE a ###= ANY (ARRAY[2,3]) ORDER BY a"));
        // Written between two values it means what it always meant.
        assertEquals("true", one("SELECT (1 ###= 1)::text"));
        exec("DROP TABLE zwo_t2");
        exec("DROP OPERATOR ###= (int,int)");
        exec("DROP FUNCTION zwo_same(int,int)");
    }

    /** A view of the catalogue has no system columns, and names its names as names. */
    @Test
    void whatAViewOfTheCatalogueHas() throws SQLException {
        assertEquals("0", one("SELECT count(*)::text FROM pg_attribute"
                + " WHERE attrelid='pg_policies'::regclass AND attnum < 0"));
        assertEquals("0", one("SELECT count(*)::text FROM pg_attribute"
                + " WHERE attrelid='pg_roles'::regclass AND attnum < 0"));
        // A stored catalogue relation has all six.
        assertEquals("6", one("SELECT count(*)::text FROM pg_attribute"
                + " WHERE attrelid='pg_class'::regclass AND attnum < 0"));
        assertEquals(java.util.Arrays.asList("policyname|name", "schemaname|name",
                        "tablename|name"),
                rows("SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute"
                        + " WHERE attrelid='pg_policies'::regclass"
                        + " AND attname IN ('schemaname','tablename','policyname')"
                        + " ORDER BY attname"));
    }
}
