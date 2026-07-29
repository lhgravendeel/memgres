package com.memgres;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A comma join over relations big enough that the engine pairs their rows by key rather than by
 * trying every combination. The answer may not depend on which of the two it chose, so every
 * case here is one where keying the rows could plausibly lose a row or invent one: null keys,
 * whole-number types of different widths, text whose type compares by rules of its own, and
 * conjuncts that name one relation, both relations, or neither unambiguously.
 *
 * <p>Both relations hold 60 rows, so a pairing is 3600 combinations — past the point where the
 * engine stops pairing them one by one.
 */
class JoinPairingTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE jp_left(id int, k2 smallint, k8 bigint, kt text, kc char(6),"
                + " kn numeric(12,2))");
        exec("CREATE TABLE jp_right(id int, k2 smallint, k8 bigint, kt text, kc char(6),"
                + " kn numeric(12,2))");
        for (int i = 0; i < 60; i++) {
            // Ten distinct keys, and every seventh row's text key is null.
            fill("jp_left", i, i % 10, i % 7 == 0 ? null : ("k" + (i % 10)));
            fill("jp_right", i, i % 10, i % 7 == 0 ? null : ("k" + (i % 10)));
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) {
            exec("DROP TABLE IF EXISTS jp_left");
            exec("DROP TABLE IF EXISTS jp_right");
            conn.close();
        }
        if (memgres != null) memgres.close();
    }

    static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static void fill(String table, int id, int key, String text) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO " + table + " VALUES (?,?,?,?,?,?)")) {
            ps.setInt(1, id);
            ps.setShort(2, (short) key);
            ps.setLong(3, key);
            if (text == null) ps.setNull(4, Types.VARCHAR); else ps.setString(4, text);
            if (text == null) ps.setNull(5, Types.CHAR); else ps.setString(5, text);
            ps.setBigDecimal(6, new BigDecimal(key + ".00"));
            ps.executeUpdate();
        }
    }

    static long count(String sql) throws SQLException {
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    static List<String> rows(String sql) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= cols; i++) {
                    if (i > 1) sb.append('|');
                    Object v = rs.getObject(i);
                    sb.append(v == null ? "<null>" : String.valueOf(v).trim());
                }
                out.add(sb.toString());
            }
        }
        return out;
    }

    // --- Keys of the same and of different whole-number types ---

    @Test void equalityOnOneWholeNumberType() throws Exception {
        // Ten keys, six rows of each on either side.
        assertEquals(10 * 6 * 6, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.k2 = r.k2"));
    }

    @Test void equalityAcrossWholeNumberWidths() throws Exception {
        assertEquals(10 * 6 * 6, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.k2 = r.k8"));
        assertEquals(10 * 6 * 6, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.k8 = r.k2"));
    }

    @Test void equalityAgainstANonKeyedType() throws Exception {
        // numeric is not keyed, so these pair the plain way and must still agree.
        assertEquals(10 * 6 * 6, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.kn = r.kn"));
        assertEquals(10 * 6 * 6, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.kn = r.k8"));
    }

    // --- Null keys ---

    @Test void nullKeysMatchNothing() throws Exception {
        // 9 rows a side are null; the other 51 hold "k0".."k9" in the same proportions.
        long expected = count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.kt IS NOT NULL AND r.kt IS NOT NULL AND l.kt = r.kt");
        assertEquals(expected, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.kt = r.kt"));
        assertEquals(0, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.kt = r.kt AND l.kt IS NULL"));
    }

    // --- Text whose type does not compare as written ---

    @Test void blankPaddedTextStillCompensatesForItsPadding() throws Exception {
        // 'k0' stored in char(6) is 'k0    '; comparing it with text ignores the padding.
        assertEquals(count("SELECT count(*) FROM jp_left l, jp_right r"
                        + " WHERE l.kt IS NOT NULL AND l.kt = r.kt"),
                count("SELECT count(*) FROM jp_left l, jp_right r WHERE l.kc = r.kt"));
        assertEquals(count("SELECT count(*) FROM jp_left l, jp_right r WHERE l.kc = r.kt"),
                count("SELECT count(*) FROM jp_left l, jp_right r WHERE l.kc = r.kc"));
    }

    // --- Conjuncts beside the equality ---

    @Test void aRestrictionNamingOneRelationOnly() throws Exception {
        assertEquals(10 * 6, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.k2 = r.k2 AND l.id < 10"));
        assertEquals(6 * 6, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.k2 = r.k2 AND r.k2 = 3"));
    }

    @Test void aFurtherConjunctOverBothRelations() throws Exception {
        assertEquals(10 * 15, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.k2 = r.k2 AND l.id > r.id"));
    }

    @Test void aDisjunctionIsNotSplitApart() throws Exception {
        // Equal ids are equal keys too, so the second arm adds nothing to the first.
        assertEquals(10 * 6 * 6, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE l.k2 = r.k2 OR l.id = r.id"));
        assertEquals(60 * 60 - 10 * 6 * 6, count("SELECT count(*) FROM jp_left l, jp_right r"
                + " WHERE NOT (l.k2 = r.k2)"));
    }

    // --- What the reference is allowed to reach ---

    @Test void aBareNameBothRelationsHoldIsRefused() {
        assertThrows(SQLException.class, () -> count(
                "SELECT count(*) FROM jp_left l, jp_right r WHERE k2 = 4"));
        assertThrows(SQLException.class, () -> count(
                "SELECT count(*) FROM jp_left l, jp_right r WHERE l.k2 = r.k2 AND id < 3"));
    }

    // --- Three relations, and a lateral beside them ---

    @Test void threeRelationsChained() throws Exception {
        exec("CREATE TABLE jp_third(id int, note text)");
        try {
            for (int i = 0; i < 40; i++) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO jp_third VALUES (?, ?)")) {
                    ps.setInt(1, i % 10);
                    ps.setString(2, "n" + i);
                    ps.executeUpdate();
                }
            }
            assertEquals(10 * 6 * 6 * 4, count("SELECT count(*) FROM jp_left l, jp_right r,"
                    + " jp_third t WHERE l.k2 = r.k2 AND r.k2 = t.id"));
        } finally {
            exec("DROP TABLE IF EXISTS jp_third");
        }
    }

    @Test void aLateralItemStillReadsTheRowToItsLeft() throws Exception {
        assertEquals(10 * 6 * 6, count("SELECT count(*) FROM jp_left l,"
                + " LATERAL (SELECT * FROM jp_right r WHERE r.k2 = l.k2) s"));
    }

    @Test void aSetReturningItemBesideTheRelations() throws Exception {
        assertEquals(10 * 6 * 6, count("SELECT count(*) FROM jp_left l, jp_right r,"
                + " generate_series(1,4) g(n) WHERE l.k2 = r.k2 AND g.n = 2"));
    }

    // --- The rows themselves, not just how many ---

    @Test void theRowsAndTheirOrder() throws Exception {
        List<String> paired = rows("SELECT l.id, r.id FROM jp_left l, jp_right r"
                + " WHERE l.k2 = r.k2 AND l.id < 2 AND r.id < 25");
        assertEquals(java.util.Arrays.asList(
                "0|0", "0|10", "0|20", "1|1", "1|11", "1|21"), paired);
    }
}
