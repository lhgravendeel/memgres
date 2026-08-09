package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Storing a value applies the column's type to it, modifier and all.
 *
 * <p>Storage used to be the target type's input function followed by a pass that applied the type
 * modifier — and the pass was guarded so narrowly that most modifiers were never applied. Worse,
 * the input function's error was thrown away for any text beginning with a brace, whatever the
 * column was: a date column took the string {@code {foo}} and kept it.
 */
class AssignmentCastTest {

    static Memgres memgres;
    static Connection conn;

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

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    /** Text that looks like an array literal is still read with the column's own type. */
    @Test
    void braceTextIsNotStoredRaw() throws Exception {
        exec("CREATE TEMP TABLE zz_ac_brace (id int, d date, i int, u uuid, a int[])");
        assertEquals("22007", refused("INSERT INTO zz_ac_brace (id, d) VALUES (1, '{foo}')").getSQLState());
        assertEquals("22P02", refused("INSERT INTO zz_ac_brace (id, i) VALUES (2, '{1,2}')").getSQLState());
        assertEquals("22P02", refused("INSERT INTO zz_ac_brace (id, u) VALUES (3, '{zzz}')").getSQLState());
        assertEquals("0", scalar("SELECT count(*) FROM zz_ac_brace"));
        // An array column still takes one.
        exec("INSERT INTO zz_ac_brace (id, a) VALUES (4, '{1,2}')");
        assertEquals("{1,2}", scalar("SELECT a FROM zz_ac_brace WHERE id = 4"));
    }

    /** A numeric precision with no scale is a scale of zero, and has no room for a special. */
    @Test
    void numericModifiersAreApplied() throws Exception {
        exec("CREATE TEMP TABLE zz_ac_num (id int, n numeric(5))");
        assertEquals("22003", refused("INSERT INTO zz_ac_num VALUES (1, 123456789)").getSQLState());
        exec("INSERT INTO zz_ac_num VALUES (2, 1.7)");
        assertEquals("2", scalar("SELECT n FROM zz_ac_num WHERE id = 2"));

        exec("CREATE TEMP TABLE zz_ac_nn (id int, n numeric(5,2))");
        SQLException e = refused("INSERT INTO zz_ac_nn VALUES (1, 'Infinity')");
        assertEquals("22003", e.getSQLState());
        assertTrue(e.getMessage().contains("numeric field overflow"));
    }

    /** timestamp(0) and time(0) keep the seconds they declare, rounded. */
    @Test
    void datetimePrecisionIsApplied() throws Exception {
        exec("CREATE TEMP TABLE zz_ac_tp (id int, ts timestamp(0), tm time(0))");
        exec("INSERT INTO zz_ac_tp VALUES (1, '2024-01-01 01:02:03.987', '01:02:03.987')");
        assertEquals("2024-01-01 01:02:04", scalar("SELECT ts::text FROM zz_ac_tp"));
        assertEquals("01:02:04", scalar("SELECT tm::text FROM zz_ac_tp"));
    }

    /** A bit(n) column takes exactly n bits, however the value was written. */
    @Test
    void bitLengthIsExact() throws Exception {
        exec("CREATE TEMP TABLE zz_ac_b (a bit(4), b varbit(4))");
        SQLException tooShort = refused("INSERT INTO zz_ac_b VALUES ('101', '101')");
        assertEquals("22026", tooShort.getSQLState());
        assertTrue(tooShort.getMessage().contains("does not match type bit(4)"));
        assertEquals("22026", refused("INSERT INTO zz_ac_b VALUES ('10101', '10101')").getSQLState());
        exec("INSERT INTO zz_ac_b VALUES ('1010', '1010')");
        assertEquals("1", scalar("SELECT count(*) FROM zz_ac_b"));
    }

    /** A log sequence number is checked, and written with capital digits. */
    @Test
    void pgLsnIsCheckedAndNormalised() throws Exception {
        exec("CREATE TEMP TABLE zz_ac_lsn (id int, l pg_lsn)");
        exec("INSERT INTO zz_ac_lsn VALUES (1, '0/16b374d')");
        assertEquals("0/16B374D", scalar("SELECT l::text FROM zz_ac_lsn"));
        assertEquals("22P02", refused("INSERT INTO zz_ac_lsn VALUES (2, 'garbage')").getSQLState());
        assertThrows(SQLException.class, () -> scalar("SELECT '16B374D'::pg_lsn"));
        assertThrows(SQLException.class, () -> scalar("SELECT 'zz/1'::pg_lsn"));
        assertEquals("0/16B374D", scalar("SELECT '0/16b374d'::pg_lsn::text"));
    }

    /** A name holds 63 bytes, so a longer string cast to one is truncated. */
    @Test
    void aNameIsTruncated() throws Exception {
        assertEquals("63", scalar("SELECT length(repeat('a',100)::name)"));
        assertEquals("3", scalar("SELECT length('abc'::name)"));
    }
}
