package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ON DELETE SET DEFAULT writes an ordinary value into the referencing column, and nothing about a
 * column default guarantees the referenced table holds it. The value therefore has to be checked
 * against the parent table — but against the parent as it will be once the statement is over, not
 * as it stands while the action runs.
 *
 * <p>The gap these tests close left a declared foreign key dangling: a default pointing at a row
 * the same DELETE was about to remove passed the check, because that row was still stored at the
 * moment it was looked for. The delete then took it, and the child was left referencing a row that
 * did not exist.
 *
 * <p>The rest pins the shapes around it, because a referential action that refuses a delete
 * PostgreSQL performs is worse than the permissiveness it removes: a default whose parent survives,
 * a NULL default, SET NULL, CASCADE, and a parent nothing references.
 */
class ForeignKeySetDefaultTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
        }
    }

    /** Rebuild the pair, with the child's default column spelled by the caller. */
    private static void freshPair(String childDefault, String action) throws SQLException {
        exec("DROP TABLE IF EXISTS fsd_c CASCADE");
        exec("DROP TABLE IF EXISTS fsd_p CASCADE");
        exec("CREATE TABLE fsd_p (id int PRIMARY KEY)");
        exec("INSERT INTO fsd_p VALUES (1),(2)");
        exec("CREATE TABLE fsd_c (id int PRIMARY KEY, pid int " + childDefault
                + " REFERENCES fsd_p(id) ON DELETE " + action + ")");
    }

    private static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<String>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    private static String errorOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.execute(sql);
            return null;
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    @Test
    void aDefaultWhoseOwnParentRowIsDeletedTooIsRefused() throws Exception {
        freshPair("DEFAULT 1", "SET DEFAULT");
        exec("INSERT INTO fsd_c VALUES (10,2)");

        assertEquals("23503", errorOf("DELETE FROM fsd_p"),
                "the default is 1, and the same DELETE takes row 1");

        assertEquals(List.of("1", "2"), rows("SELECT id FROM fsd_p ORDER BY id"),
                "a refused statement leaves the parent as it was");
        assertEquals(List.of("10|2"), rows("SELECT id, pid FROM fsd_c ORDER BY id"),
                "and leaves the child pointing where it did");
    }

    @Test
    void aDefaultWhoseParentRowSurvivesStillActs() throws Exception {
        freshPair("DEFAULT 1", "SET DEFAULT");
        exec("INSERT INTO fsd_c VALUES (10,2)");

        exec("DELETE FROM fsd_p WHERE id = 2");

        assertEquals(List.of("10|1"), rows("SELECT id, pid FROM fsd_c ORDER BY id"),
                "row 1 is still there, so the default may be written");
    }

    @Test
    void aDefaultNoParentRowEverHeldIsRefused() throws Exception {
        freshPair("DEFAULT 99", "SET DEFAULT");
        exec("INSERT INTO fsd_c VALUES (10,2)");

        assertEquals("23503", errorOf("DELETE FROM fsd_p WHERE id = 2"));
        assertEquals(List.of("10|2"), rows("SELECT id, pid FROM fsd_c ORDER BY id"));
    }

    @Test
    void aNullDefaultReferencesNothingAndIsAlwaysAllowed() throws Exception {
        freshPair("", "SET DEFAULT");
        exec("INSERT INTO fsd_c VALUES (10,2)");

        exec("DELETE FROM fsd_p");

        assertEquals(List.of("10|null"), rows("SELECT id, coalesce(pid::text,'null') FROM fsd_c"),
                "NULL satisfies the key by referencing nothing");
    }

    @Test
    void setNullAndCascadeAreUntouched() throws Exception {
        freshPair("DEFAULT 1", "SET NULL");
        exec("INSERT INTO fsd_c VALUES (10,2)");
        exec("DELETE FROM fsd_p");
        assertEquals(List.of("10|null"), rows("SELECT id, coalesce(pid::text,'null') FROM fsd_c"));

        freshPair("DEFAULT 1", "CASCADE");
        exec("INSERT INTO fsd_c VALUES (10,2)");
        exec("DELETE FROM fsd_p");
        assertEquals(List.of("0"), rows("SELECT count(*) FROM fsd_c"));
    }

    @Test
    void aParentNothingReferencesIsDeletedFreely() throws Exception {
        freshPair("DEFAULT 1", "SET DEFAULT");

        exec("DELETE FROM fsd_p");

        assertEquals(List.of("0"), rows("SELECT count(*) FROM fsd_p"));
    }

    @Test
    void theSameQuestionIsAskedOfACompositeKey() throws Exception {
        exec("DROP TABLE IF EXISTS fsd_c CASCADE");
        exec("DROP TABLE IF EXISTS fsd_p CASCADE");
        exec("CREATE TABLE fsd_p (a int, b int, PRIMARY KEY (a,b))");
        exec("INSERT INTO fsd_p VALUES (1,1),(2,2)");
        exec("CREATE TABLE fsd_c (id int PRIMARY KEY, x int DEFAULT 1, y int DEFAULT 1, "
                + "FOREIGN KEY (x,y) REFERENCES fsd_p(a,b) ON DELETE SET DEFAULT)");
        exec("INSERT INTO fsd_c VALUES (10,2,2)");

        assertEquals("23503", errorOf("DELETE FROM fsd_p"),
                "(1,1) is the default and the same DELETE takes it");
        assertEquals(List.of("10|2|2"), rows("SELECT id, x, y FROM fsd_c ORDER BY id"));

        // Removing only the row the child does not point at leaves the child alone.
        exec("DELETE FROM fsd_p WHERE a = 1 AND b = 1");
        assertEquals(List.of("10|2|2"), rows("SELECT id, x, y FROM fsd_c ORDER BY id"));
    }

    @Test
    void anUpdateOfTheParentKeyStillFindsItsChildren() throws Exception {
        freshPair("DEFAULT 1", "SET DEFAULT");
        exec("INSERT INTO fsd_c VALUES (10,2)");

        // ON UPDATE is not given here, so it is NO ACTION: changing a referenced key must be
        // refused rather than quietly repointing the child at the default.
        assertEquals("23503", errorOf("UPDATE fsd_p SET id = 7 WHERE id = 2"));
        assertEquals(List.of("10|2"), rows("SELECT id, pid FROM fsd_c ORDER BY id"));
    }
}
