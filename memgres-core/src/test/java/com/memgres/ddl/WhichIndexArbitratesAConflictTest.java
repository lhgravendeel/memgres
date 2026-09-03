package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Which index an ON CONFLICT clause arbitrates on, and what may be written to name it.
 *
 * <p>A conflict target names the keys of an index, so a key may carry the operator class it was
 * indexed under -- and a key indexed under one class is not the same key under another. A direction
 * may not be written at all: the arbiter is an index that has one already.
 *
 * <p>Arbitration happens as the row is written, so a constraint whose test may be put off to the
 * end of the transaction cannot arbitrate. An exclusion constraint has no row for an update to act
 * on, but a write it refuses is still a conflict, and DO NOTHING leaves that row out.
 */
class WhichIndexArbitratesAConflictTest {

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

    /** A key may carry the class it was indexed under. */
    @Test
    void theClassAKeyIsComparedUnder() throws SQLException {
        exec("CREATE TABLE zwa_o (i int, t text, v int, UNIQUE (i))");
        exec("INSERT INTO zwa_o VALUES (1,'a',1)");
        assertEquals("3", one("INSERT INTO zwa_o VALUES (3,'q',3)"
                + " ON CONFLICT (i int4_ops) DO NOTHING RETURNING i::text"));
        assertEquals("9", one("INSERT INTO zwa_o VALUES (1,'q',4)"
                + " ON CONFLICT (i int4_ops) DO UPDATE SET v = 9 RETURNING v::text"));
        // A class written under the schema everything the server ships lives in is that class.
        assertNull(stateOf("INSERT INTO zwa_o VALUES (1,'q',4)"
                + " ON CONFLICT (i pg_catalog.int4_ops) DO NOTHING"));
        // A class that is not the one the key is compared under names no index.
        assertEquals("42P10", stateOf("INSERT INTO zwa_o VALUES (1,'q',4)"
                + " ON CONFLICT (i text_pattern_ops) DO NOTHING"));
        // A class nobody has is a name that reaches nothing.
        assertEquals("42704", stateOf("INSERT INTO zwa_o VALUES (1,'q',4)"
                + " ON CONFLICT (i zwa_nosuch_ops) DO NOTHING"));
        assertTrue(messageOf("INSERT INTO zwa_o VALUES (1,'q',4)"
                + " ON CONFLICT (i zwa_nosuch_ops) DO NOTHING")
                .contains("operator class \"zwa_nosuch_ops\" does not exist"
                        + " for access method \"btree\""));
        assertEquals("3F000", stateOf("INSERT INTO zwa_o VALUES (1,'q',4)"
                + " ON CONFLICT (i zwa_nosch.int4_ops) DO NOTHING"));
        exec("DROP TABLE zwa_o");
    }

    /** A direction is no part of a conflict target. */
    @Test
    void aDirectionIsNotWrittenHere() throws SQLException {
        exec("CREATE TABLE zwa_d (i int UNIQUE)");
        assertEquals("42P10", stateOf("INSERT INTO zwa_d VALUES (1) ON CONFLICT (i DESC)"
                + " DO NOTHING"));
        assertTrue(messageOf("INSERT INTO zwa_d VALUES (1) ON CONFLICT (i ASC) DO NOTHING")
                .contains("ASC/DESC is not allowed in ON CONFLICT clause"));
        assertTrue(messageOf("INSERT INTO zwa_d VALUES (1) ON CONFLICT (i NULLS FIRST)"
                + " DO NOTHING")
                .contains("NULLS FIRST/LAST is not allowed in ON CONFLICT clause"));
        // A collation belongs to the key itself and is read with it.
        assertEquals("42P10", stateOf("INSERT INTO zwa_d VALUES (1)"
                + " ON CONFLICT (i COLLATE \"C\") DO NOTHING"));
        exec("DROP TABLE zwa_d");
    }

    /** A constraint whose test may be put off cannot arbitrate. */
    @Test
    void aConstraintThatMayBePutOff() throws SQLException {
        exec("CREATE TABLE zwa_f (i int UNIQUE DEFERRABLE, j int UNIQUE)");
        exec("INSERT INTO zwa_f VALUES (1,1)");
        assertEquals("55000", stateOf("INSERT INTO zwa_f VALUES (1,2) ON CONFLICT DO NOTHING"));
        assertEquals("55000", stateOf("INSERT INTO zwa_f VALUES (1,3) ON CONFLICT (i) DO NOTHING"));
        assertEquals("55000", stateOf("INSERT INTO zwa_f VALUES (1,4)"
                + " ON CONFLICT ON CONSTRAINT zwa_f_i_key DO NOTHING"));
        assertTrue(messageOf("INSERT INTO zwa_f VALUES (1,2) ON CONFLICT DO NOTHING")
                .contains("ON CONFLICT does not support deferrable unique"
                        + " constraints/exclusion constraints as arbiters"));
        // Nothing conflicts here either: which constraints would arbitrate is settled first.
        assertEquals("55000", stateOf("INSERT INTO zwa_f VALUES (5,5) ON CONFLICT DO NOTHING"));
        // A target naming the other constraint arbitrates on that one alone.
        assertNull(stateOf("INSERT INTO zwa_f VALUES (2,1) ON CONFLICT (j) DO NOTHING"));
        assertEquals("1", one("SELECT count(*)::text FROM zwa_f"));
        exec("DROP TABLE zwa_f");
    }

    /** A row an exclusion constraint refuses is a conflict DO NOTHING leaves out. */
    @Test
    void aRowNoOtherRowMayStandBeside() throws SQLException {
        exec("CREATE TABLE zwa_e (id int, r int4range, EXCLUDE USING gist (r WITH &&))");
        exec("INSERT INTO zwa_e VALUES (1, '[1,5)')");
        assertNull(stateOf("INSERT INTO zwa_e VALUES (2, '[3,7)') ON CONFLICT DO NOTHING"));
        assertNull(stateOf("INSERT INTO zwa_e VALUES (3, '[3,7)')"
                + " ON CONFLICT ON CONSTRAINT zwa_e_r_excl DO NOTHING"));
        // A row that stands beside every other still goes in.
        assertNull(stateOf("INSERT INTO zwa_e VALUES (4, '[9,11)') ON CONFLICT DO NOTHING"));
        assertEquals("2", one("SELECT count(*)::text FROM zwa_e"));
        // There is no row for an update to act on.
        assertEquals("42809", stateOf("INSERT INTO zwa_e VALUES (5, '[3,7)')"
                + " ON CONFLICT ON CONSTRAINT zwa_e_r_excl DO UPDATE SET id = 9"));
        assertTrue(messageOf("INSERT INTO zwa_e VALUES (5, '[3,7)')"
                + " ON CONFLICT ON CONSTRAINT zwa_e_r_excl DO UPDATE SET id = 9")
                .contains("ON CONFLICT DO UPDATE not supported with exclusion constraints"));
        // Without ON CONFLICT the same write is refused.
        assertEquals("23P01", stateOf("INSERT INTO zwa_e VALUES (6, '[3,7)')"));
        exec("DROP TABLE zwa_e");
    }
}
