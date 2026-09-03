package com.memgres.ddl;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * When two rows have the same key, and when a null is a key at all.
 *
 * <p>A key over several columns is the columns together, and where one column's value ends and the
 * next begins cannot depend on what is in them: joined by a separator alone, two rows whose values
 * differed only in where that character fell made the same key, and a unique index over them could
 * not be built.
 *
 * <p>A null is unlike every other value and so conflicts with nothing — unless the key was declared
 * NULLS NOT DISTINCT, which is the whole of what that clause says, and which an ON CONFLICT over
 * that key has to see too.
 */
class UniqueKeysAndNullConflictsTest {

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

    /** Where one column of a key ends does not depend on what is in it. */
    @Test
    void aKeyOverSeveralColumnsIsTheColumnsTogether() throws SQLException {
        exec("CREATE TABLE zuk_n3 (a text, b text)");
        exec("INSERT INTO zuk_n3 VALUES ('a' || chr(1) || 'b', 'c'), ('a', 'b' || chr(1) || 'c')");
        // The two rows differ only in where the chr(1) falls, and they are two rows.
        assertNull(stateOf("CREATE UNIQUE INDEX zuk_i3 ON zuk_n3 (a, b)"));
        exec("DROP TABLE zuk_n3");
        // Two rows that really do share a key still cannot be indexed uniquely.
        exec("CREATE TABLE zuk_n4 (a text, b text)");
        exec("INSERT INTO zuk_n4 VALUES ('x', 'y'), ('x', 'y')");
        assertEquals("23505", stateOf("CREATE UNIQUE INDEX zuk_i4 ON zuk_n4 (a, b)"));
        exec("DROP TABLE zuk_n4");
    }

    /** A key declared NULLS NOT DISTINCT sees one null as the same key as another. */
    @Test
    void aNullThatIsAKey() throws SQLException {
        exec("CREATE TABLE zuk_f8 (i int, j text, UNIQUE NULLS NOT DISTINCT (i))");
        exec("INSERT INTO zuk_f8 VALUES (NULL, 'a')");
        assertEquals("c", one("INSERT INTO zuk_f8 VALUES (NULL, 'b')"
                + " ON CONFLICT (i) DO UPDATE SET j = 'c' RETURNING j"));
        assertNull(stateOf("INSERT INTO zuk_f8 VALUES (NULL, 'd') ON CONFLICT DO NOTHING"));
        assertEquals("c", one("SELECT j FROM zuk_f8"));
        exec("DROP TABLE zuk_f8");
    }

    /** Without that clause a null still conflicts with nothing. */
    @Test
    void aNullThatIsNotAKey() throws SQLException {
        exec("CREATE TABLE zuk_d (i int UNIQUE, j text)");
        exec("INSERT INTO zuk_d VALUES (NULL, 'a')");
        assertNull(stateOf("INSERT INTO zuk_d VALUES (NULL, 'b')"));
        assertEquals("2", one("SELECT count(*)::text FROM zuk_d"));
        exec("DROP TABLE zuk_d");
    }
}
