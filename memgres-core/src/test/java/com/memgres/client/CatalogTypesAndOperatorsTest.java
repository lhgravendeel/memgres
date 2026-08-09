package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * What the catalogues say their columns hold, and which operator a call reaches.
 *
 * <p>A user-defined operator was chosen by name alone once the argument types were near enough:
 * every numeric type matched every other, so an operator declared for two integers answered for
 * two numerics, and the value handed to its backing function was whatever the caller wrote rather
 * than what the operator declares. The catalogue relations declared OID columns as integer, name
 * columns as text and single-byte codes as character, so a client reading pg_typeof learned types
 * PostgreSQL does not have there. Writing to a catalogue view said the relation did not exist, and
 * an aggregate called with no arguments reached the client as an internal error about an index.
 */
class CatalogTypesAndOperatorsTest {

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

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    private static SQLException refused(String sql) {
        return assertThrows(SQLException.class, () -> exec(sql), sql);
    }

    // ---- a user-defined operator takes the types it declares ----

    /** A value reaches a wider parameter and not a narrower one. */
    @Test
    void anOperatorIsChosenByType() throws Exception {
        exec("CREATE FUNCTION zz_ct_add(a integer, b integer) RETURNS integer LANGUAGE sql"
                + " AS $$ SELECT a * 100 + b $$");
        exec("CREATE OPERATOR ##@ (LEFTARG = integer, RIGHTARG = integer, FUNCTION = zz_ct_add)");
        try {
            assertEquals("102", scalar("SELECT 1 ##@ 2"));
            // A smallint is an integer without losing anything, so it reaches the operator.
            assertEquals("102", scalar("SELECT 1::smallint ##@ 2::smallint"));

            // A numeric or a bigint is not an integer, and there is no operator for either.
            for (String[] each : new String[][]{
                    {"SELECT 1.5 ##@ 2.5", "numeric ##@ numeric"},
                    {"SELECT 1::bigint ##@ 2::bigint", "bigint ##@ bigint"}}) {
                SQLException e = refused(each[0]);
                assertEquals("42883", e.getSQLState(), each[0]);
                assertTrue(e.getMessage().contains("operator does not exist: " + each[1]),
                        each[0] + " -> " + e.getMessage());
            }

            // A bare constant has no type of its own, so it takes the one the operator declares
            // and is refused as that type rather than reported as a missing text operator.
            SQLException untyped = refused("SELECT 'a' ##@ 'b'");
            assertEquals("22P02", untyped.getSQLState());
            assertTrue(untyped.getMessage()
                    .contains("invalid input syntax for type integer: \"a\""), untyped.getMessage());
        } finally {
            exec("DROP OPERATOR ##@ (integer, integer)");
            exec("DROP FUNCTION zz_ct_add(integer, integer)");
        }
    }

    /** A column keeps the type it was declared with, whatever its value is held in. */
    @Test
    void aColumnOffersItsDeclaredType() throws Exception {
        exec("CREATE FUNCTION zz_ct_add2(a integer, b integer) RETURNS integer LANGUAGE sql"
                + " AS $$ SELECT a + b $$");
        exec("CREATE OPERATOR #+# (LEFTARG = integer, RIGHTARG = integer, FUNCTION = zz_ct_add2)");
        exec("CREATE TABLE zz_ct_t (x int)");
        try {
            exec("INSERT INTO zz_ct_t VALUES (1)");
            assertEquals("2", scalar("SELECT x #+# 1 FROM zz_ct_t"));
        } finally {
            exec("DROP TABLE zz_ct_t");
            exec("DROP OPERATOR #+# (integer, integer)");
            exec("DROP FUNCTION zz_ct_add2(integer, integer)");
        }
    }

    // ---- the catalogues describe themselves ----

    /** An OID column is an oid, a name column is a name, and a code column is "char". */
    @Test
    void catalogColumnsCarryTheirOwnTypes() throws Exception {
        assertEquals("oid,name,oid", types(
                "SELECT pg_typeof(oid), pg_typeof(datname), pg_typeof(datdba)"
                        + " FROM pg_database LIMIT 1"));
        assertEquals("oid,name,oid,oid[],text[]", types(
                "SELECT pg_typeof(oid), pg_typeof(extname), pg_typeof(extowner),"
                        + " pg_typeof(extconfig), pg_typeof(extcondition)"
                        + " FROM pg_extension WHERE extname='plpgsql'"));
        assertEquals("oid,name,oid", types(
                "SELECT pg_typeof(oid), pg_typeof(lanname), pg_typeof(lanplcallfoid)"
                        + " FROM pg_language WHERE lanname='plpgsql'"));
        assertEquals("text[]", types(
                "SELECT pg_typeof(rolconfig) FROM pg_roles WHERE rolname='pg_monitor'"));
    }

    private static String types(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                if (i > 1) sb.append(',');
                sb.append(rs.getString(i));
            }
            return sb.toString();
        }
    }

    /** A catalogue view is a view, and a view of that shape cannot be written through. */
    @Test
    void catalogViewsCannotBeWrittenTo() {
        for (String[] each : new String[][]{
                {"INSERT INTO pg_cursors VALUES ('x','y',false,false,false,now())",
                 "cannot insert into view \"pg_cursors\""},
                {"DELETE FROM pg_cursors", "cannot delete from view \"pg_cursors\""},
                {"UPDATE pg_cursors SET name = 'z'", "cannot update view \"pg_cursors\""},
                {"DELETE FROM pg_settings", "cannot delete from view \"pg_settings\""}}) {
            SQLException e = refused(each[0]);
            assertEquals("55000", e.getSQLState(), each[0]);
            assertTrue(e.getMessage().contains(each[1]), each[0] + " -> " + e.getMessage());
        }
    }

    /** An advisory lock's key halves are OIDs, and an OID is unsigned. */
    @Test
    void advisoryKeysReadAsUnsigned() throws Exception {
        exec("BEGIN");
        try {
            scalar("SELECT pg_advisory_xact_lock(-1, -1)");
            assertEquals("4294967295,4294967295", types(
                    "SELECT classid::text, objid::text FROM pg_locks"
                            + " WHERE locktype = 'advisory' AND objsubid = 2"));
        } finally {
            exec("ROLLBACK");
        }
    }

    // ---- other things that named nothing ----

    /** An aggregate declared over an argument has to be given one. */
    @Test
    void anAggregateNeedsItsArgument() throws Exception {
        exec("CREATE FUNCTION zz_ct_sf(int, int) RETURNS int LANGUAGE sql IMMUTABLE STRICT"
                + " AS $$ SELECT $1 * 10 + $2 $$");
        exec("CREATE AGGREGATE zz_ct_ag (int) (SFUNC = zz_ct_sf, STYPE = int)");
        try {
            SQLException e = refused("SELECT zz_ct_ag() FROM (VALUES (1),(2)) t(v)");
            assertEquals("42883", e.getSQLState());
            assertTrue(e.getMessage().contains("function zz_ct_ag() does not exist"),
                    e.getMessage());
            assertFalse(e.getMessage().contains("Internal error"), e.getMessage());
        } finally {
            exec("DROP AGGREGATE zz_ct_ag(int)");
            exec("DROP FUNCTION zz_ct_sf(int, int)");
        }
    }

    /** A collation that was dropped is a collation nobody has. */
    @Test
    void aDroppedCollationIsGone() throws Exception {
        exec("CREATE COLLATION zz_ct_coll (LOCALE = 'C')");
        SQLException duplicate = refused("CREATE COLLATION zz_ct_coll (LOCALE = 'C')");
        assertEquals("42710", duplicate.getSQLState());
        assertTrue(duplicate.getMessage()
                .contains("collation \"zz_ct_coll\" for encoding \"UTF8\" already exists"),
                duplicate.getMessage());
        exec("DROP COLLATION zz_ct_coll");
        assertEquals("0", scalar(
                "SELECT count(*)::int FROM pg_collation WHERE collname = 'zz_ct_coll'"));
        SQLException gone = refused("SELECT 'a' COLLATE zz_ct_coll");
        assertEquals("42704", gone.getSQLState());
    }

    /** A column list tells VACUUM which columns to gather statistics for, so it needs ANALYZE. */
    @Test
    void aVacuumColumnListNeedsAnalyze() throws Exception {
        exec("CREATE TABLE zz_ct_v (id int)");
        try {
            for (String sql : new String[]{"VACUUM zz_ct_v (id)", "VACUUM zz_ct_v (nosuchcol)",
                    "VACUUM (ANALYZE FALSE) zz_ct_v (id)"}) {
                SQLException e = refused(sql);
                assertEquals("0A000", e.getSQLState(), sql);
                assertTrue(e.getMessage()
                        .contains("ANALYZE option must be specified when a column list is provided"),
                        sql);
            }
            assertEquals("42601", refused("VACUUM zz_ct_v ()").getSQLState());
            exec("VACUUM (ANALYZE) zz_ct_v (id)");
        } finally {
            exec("DROP TABLE zz_ct_v");
        }
    }

    /** DEFAULT NULL adds nothing a column did not already do, so no default is recorded. */
    @Test
    void anExplicitNullDefaultIsNoDefault() throws Exception {
        exec("CREATE TABLE zz_ct_d (id int, d_null int DEFAULT NULL, d_val int DEFAULT 7)");
        try {
            assertEquals("false", scalar(
                    "SELECT atthasdef::text FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid"
                            + " WHERE c.relname='zz_ct_d' AND attname='d_null'"));
            assertEquals("true", scalar(
                    "SELECT atthasdef::text FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid"
                            + " WHERE c.relname='zz_ct_d' AND attname='d_val'"));
            assertEquals("1", scalar("SELECT count(*)::int FROM pg_attrdef d"
                    + " JOIN pg_class c ON c.oid=d.adrelid WHERE c.relname='zz_ct_d'"));
        } finally {
            exec("DROP TABLE zz_ct_d");
        }
    }

    /** A prepared statement named with quotes is the statement of that name. */
    @Test
    void aQuotedPlanNameIsRead() throws Exception {
        exec("CREATE TABLE zz_ct_r (id int)");
        exec("PREPARE zz_ct_pr AS INSERT INTO zz_ct_r VALUES (9) RETURNING id AS a");
        try {
            assertEquals("9", scalar("EXECUTE \"zz_ct_pr\""));
        } finally {
            exec("DEALLOCATE zz_ct_pr");
            exec("DROP TABLE zz_ct_r");
        }
    }
}
