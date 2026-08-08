package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Values compare as the types they are, not as the Java objects that carry them.
 *
 * <p>Comparison used to promote any two numbers through {@code double}, strip trailing spaces from
 * every string, and order UUIDs by {@code java.util.UUID.compareTo}. Each of those is a rule that
 * belongs somewhere narrower: {@code double} to the float types, the trailing spaces to bpchar, and
 * signed longs nowhere at all. Applied to everything they answered wrongly and quietly — two
 * distinct bigints compared equal, {@code 'abc ' = 'abc'} was true, and the greatest UUID sorted
 * below the least.
 */
class ComparisonByTypeTest {

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

    /** Trailing blanks count in text, and do not count in bpchar. */
    @Test
    void blanksCountExceptInBpchar() throws Exception {
        assertEquals("f", scalar("SELECT 'abc ' = 'abc'"));
        assertEquals("t", scalar("SELECT 'a ' > 'a'"));
        assertEquals("f", scalar("SELECT '' = ' '"));
        assertEquals("t", scalar("SELECT 'ab'::char(5) = 'ab'"));
        assertEquals("t", scalar("SELECT 'ab'::char(5) = 'ab   '"));
    }

    /** And a column of text keeps them apart, in a WHERE and in a join alike. */
    @Test
    void aTextColumnKeepsItsBlanks() throws Exception {
        exec("CREATE TEMP TABLE zz_cbt_txt (t text)");
        exec("INSERT INTO zz_cbt_txt VALUES ('a'),('a '),('a  '),('b')");
        assertEquals("1", scalar("SELECT count(*) FROM zz_cbt_txt WHERE t = 'a'"));
        assertEquals("4", scalar("SELECT count(*) FROM zz_cbt_txt x JOIN zz_cbt_txt y ON x.t = y.t"));
        assertEquals("4", scalar("SELECT count(DISTINCT t) FROM zz_cbt_txt"));
    }

    /** Whole numbers compare as whole numbers, past what a double can hold. */
    @Test
    void bigintsCompareExactly() throws Exception {
        assertEquals("f", scalar("SELECT 9007199254740993::bigint = 9007199254740992::bigint"));
        exec("CREATE TEMP TABLE zz_cbt_big (b bigint)");
        exec("INSERT INTO zz_cbt_big VALUES (9007199254740992),(9007199254740993)");
        assertEquals("9007199254740993", scalar("SELECT max(b) FROM zz_cbt_big"));
        assertEquals("1", scalar("SELECT count(*) FROM zz_cbt_big WHERE b > 9007199254740992"));
    }

    /** A UUID is sixteen unsigned bytes, so the greatest of them is the greatest. */
    @Test
    void uuidsCompareUnsigned() throws Exception {
        assertEquals("t", scalar("SELECT 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid"
                + " > '7fffffff-ffff-ffff-ffff-ffffffffffff'::uuid"));
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT u FROM (VALUES ('80000000-0000-0000-0000-000000000000'::uuid),"
                             + "('00000000-0000-0000-0000-000000000001'::uuid)) t(u) ORDER BY u")) {
            assertTrue(rs.next());
            assertEquals("00000000-0000-0000-0000-000000000001", rs.getString(1));
        }
    }

    /** mode() counts a value however it is written, so 1.0 and 1.00 are one value twice. */
    @Test
    void modeCountsByEquality() throws Exception {
        exec("CREATE TEMP TABLE zz_cbt_md (n numeric)");
        exec("INSERT INTO zz_cbt_md VALUES (1.0),(1.00),(1.000),(2.0),(2.0)");
        assertEquals("1.0", scalar("SELECT mode() WITHIN GROUP (ORDER BY n) FROM zz_cbt_md"));
    }

    /** A comparison function answers with the sign of the difference. */
    @Test
    void enumCmpAnswersASign() throws Exception {
        exec("CREATE TYPE zz_cbt_e AS ENUM ('lo','hi')");
        try {
            exec("CREATE TEMP TABLE zz_cbt_ec (a zz_cbt_e, b zz_cbt_e)");
            exec("INSERT INTO zz_cbt_ec VALUES ('lo','hi')");
            assertEquals("-1", scalar("SELECT enum_cmp(a, b) FROM zz_cbt_ec"));
            assertEquals("1", scalar("SELECT enum_cmp(b, a) FROM zz_cbt_ec"));
            assertEquals("0", scalar("SELECT enum_cmp(a, a) FROM zz_cbt_ec"));
        } finally {
            exec("DROP TABLE IF EXISTS zz_cbt_ec");
            exec("DROP TYPE IF EXISTS zz_cbt_e");
        }
    }

    /**
     * A foreign key is checked against the values, and under MATCH SIMPLE a NULL anywhere in the
     * referencing columns satisfies it.
     */
    @Test
    void addingAForeignKeyChecksValuesNotText() throws Exception {
        try {
            exec("CREATE TABLE zz_cbt_f1 (a int, b int, PRIMARY KEY (a, b))");
            exec("INSERT INTO zz_cbt_f1 VALUES (1, 1)");
            exec("CREATE TABLE zz_cbt_f2 (x int, y int)");
            exec("INSERT INTO zz_cbt_f2 VALUES (7, NULL)");
            exec("ALTER TABLE zz_cbt_f2 ADD CONSTRAINT zz_cbt_f2_fk"
                    + " FOREIGN KEY (x, y) REFERENCES zz_cbt_f1 (a, b)");

            exec("CREATE TABLE zz_cbt_n1 (a numeric PRIMARY KEY)");
            exec("INSERT INTO zz_cbt_n1 VALUES (1.0)");
            exec("CREATE TABLE zz_cbt_n2 (x numeric)");
            exec("INSERT INTO zz_cbt_n2 VALUES (1.00)");
            exec("ALTER TABLE zz_cbt_n2 ADD CONSTRAINT zz_cbt_n2_fk"
                    + " FOREIGN KEY (x) REFERENCES zz_cbt_n1 (a)");
        } finally {
            exec("DROP TABLE IF EXISTS zz_cbt_f2, zz_cbt_f1, zz_cbt_n2, zz_cbt_n1");
        }
    }
}
