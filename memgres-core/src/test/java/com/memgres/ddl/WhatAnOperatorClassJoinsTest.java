package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What an operator class joins, and what an index may be written over.
 *
 * <p>A class's operators belong to the family it joins, which is where pg_amop reads them from.
 * Recorded on neither, a family a class had just filled reported itself empty -- so a reader
 * asking what comparisons an index built on it could use was told there were none.
 *
 * <p>And a class a reader defined is a class the access method has, with the type it was declared
 * FOR as the type it accepts. Only the classes PostgreSQL ships were looked for, so an index
 * written over a class the same session had just created was refused as naming one that does not
 * exist.
 */
class WhatAnOperatorClassJoinsTest {

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

    /** The comparisons a class was written with reach the family it joined. */
    @Test
    void whatAClassGivesItsFamily() throws SQLException {
        exec("CREATE FUNCTION zwo_cmp(int,int) RETURNS int LANGUAGE sql IMMUTABLE"
                + " AS $$ SELECT CASE WHEN $1<$2 THEN -1 WHEN $1>$2 THEN 1 ELSE 0 END $$");
        exec("CREATE OPERATOR FAMILY zwo_fam USING btree");
        assertEquals("0", amopCount());
        exec("CREATE OPERATOR CLASS zwo_oc FOR TYPE int USING btree FAMILY zwo_fam AS"
                + " OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >,"
                + " FUNCTION 1 zwo_cmp(int,int)");
        // Five comparisons; the support function is not one of them.
        assertEquals("5", amopCount());
        // And the class is one an index may be written over.
        exec("CREATE TABLE zwo_it (i int, s text)");
        assertNull(stateOf("CREATE INDEX zwo_i ON zwo_it (i zwo_oc)"));
        // It accepts the type it was declared for, and no other.
        assertEquals("42804", stateOf("CREATE INDEX zwo_bad ON zwo_it (s zwo_oc)"));
        // A class nobody defined is still one that does not exist.
        assertEquals("42704", stateOf("CREATE INDEX zwo_none ON zwo_it (i zwo_nosuch)"));
        exec("DROP TABLE zwo_it");
        exec("DROP OPERATOR CLASS zwo_oc USING btree");
        exec("DROP OPERATOR FAMILY zwo_fam USING btree");
        exec("DROP FUNCTION zwo_cmp(int,int)");
    }

    private static String amopCount() throws SQLException {
        return one("SELECT count(*)::text FROM pg_amop WHERE amopfamily ="
                + " (SELECT oid FROM pg_opfamily WHERE opfname = 'zwo_fam')");
    }
}
