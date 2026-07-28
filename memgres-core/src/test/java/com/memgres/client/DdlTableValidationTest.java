package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PostgreSQL refuses a table or column definition it considers incoherent — a column that would
 * shadow a system column, a DEFAULT that can never be evaluated, a CHECK that is not a predicate,
 * an identity column with no sequence behind its type. Accepting such a definition records
 * something PostgreSQL could not have produced, and the contradiction then surfaces at some later
 * statement (an INSERT, a catalog read) that looks like the culprit but is not.
 *
 * <p>Two of the statements here go the other way: {@code DROP EXPRESSION} and
 * {@code SET WITHOUT CLUSTER} are valid SQL that used to be a syntax error.
 */
class DdlTableValidationTest {

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

    // ---- helpers -------------------------------------------------------

    private static void exec(String sql) throws SQLException {
        try (Statement st = conn.createStatement()) { st.execute(sql); }
    }

    private static String scalar(String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), "expected a row from: " + sql);
            return rs.getString(1);
        }
    }

    /** All rows of a query as {@code a|b} strings, in order. */
    private static String rows(String sql) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                if (sb.length() > 0) sb.append(';');
                for (int i = 1; i <= n; i++) {
                    if (i > 1) sb.append('|');
                    sb.append(rs.getString(i));
                }
            }
        }
        return sb.toString();
    }

    private static void assertRejected(String sqlState, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(messagePart),
                "expected \"" + messagePart + "\" in: " + e.getMessage());
    }

    // ---- system column names -------------------------------------------

    @Test
    void aColumnMayNotShadowASystemColumn() throws Exception {
        for (String name : new String[]{"ctid", "xmin", "cmin", "xmax", "cmax", "tableoid"}) {
            assertRejected("42701", "column name \"" + name + "\" conflicts with a system column name",
                    "CREATE TABLE dtv_sys (" + name + " int)");
        }
        exec("CREATE TABLE dtv_sys (i int)");
        assertRejected("42701", "column name \"xmin\" conflicts with a system column name",
                "ALTER TABLE dtv_sys ADD COLUMN xmin integer");
        assertRejected("42701", "column name \"ctid\" conflicts with a system column name",
                "ALTER TABLE dtv_sys RENAME COLUMN i TO ctid");
        assertRejected("42701", "column name \"tableoid\" conflicts with a system column name",
                "CREATE TABLE dtv_sysc AS SELECT 1 AS tableoid");
    }

    @Test
    void onlyTheSystemNamesThemselvesAreRefused() throws Exception {
        // A quoted name with capitals is a different identifier, and oid has been an ordinary
        // column name since PG 12.
        exec("CREATE TABLE dtv_sysq (\"XMAX\" int, oid int)");
        exec("ALTER TABLE dtv_sysq ADD COLUMN \"Ctid\" int");
        exec("CREATE TABLE dtv_sysr (i int)");
        exec("ALTER TABLE dtv_sysr RENAME COLUMN i TO j");
        exec("ALTER TABLE dtv_sysr ADD COLUMN k int");
        assertEquals("", rows("SELECT j, k FROM dtv_sysr"));
    }

    @Test
    void renamingAColumnThatIsNotThereNamesTheColumn() throws Exception {
        exec("CREATE TABLE dtv_ren (i int)");
        assertRejected("42703", "column \"nosuchcol\" does not exist",
                "ALTER TABLE dtv_ren RENAME COLUMN nosuchcol TO z");
    }

    // ---- DEFAULT expressions -------------------------------------------

    @Test
    void aDefaultThatCanNeverBeEvaluatedIsRefused() throws Exception {
        exec("CREATE TABLE dtv_def (c1 int, c2 int, c3 text, c4 numeric)");
        assertRejected("22P02", "invalid input syntax for type integer: \"wrong_datatype\"",
                "ALTER TABLE dtv_def ALTER COLUMN c1 SET DEFAULT 'wrong_datatype'");
        assertRejected("22P02", "invalid input syntax for type numeric: \"zz\"",
                "ALTER TABLE dtv_def ALTER COLUMN c4 SET DEFAULT 'zz'");
        assertRejected("0A000", "cannot use subquery in DEFAULT expression",
                "ALTER TABLE dtv_def ALTER COLUMN c1 SET DEFAULT (SELECT 1)");
        assertRejected("0A000", "cannot use column reference in DEFAULT expression",
                "ALTER TABLE dtv_def ALTER COLUMN c1 SET DEFAULT c2");
        assertRejected("0A000", "cannot use column reference in DEFAULT expression",
                "ALTER TABLE dtv_def ALTER COLUMN c1 SET DEFAULT dtv_def.c2");
        assertRejected("0A000", "cannot use column reference in DEFAULT expression",
                "ALTER TABLE dtv_def ALTER COLUMN c1 SET DEFAULT nosuchcol");
        assertRejected("42703", "column \"nosuchcol\" of relation \"dtv_def\" does not exist",
                "ALTER TABLE dtv_def ALTER COLUMN nosuchcol SET DEFAULT 1");
    }

    @Test
    void aTypedDefaultExpressionOfTheWrongTypeIsAMismatch() throws Exception {
        // A bare string literal is of type unknown, so a bad one is invalid input; an expression
        // that already has a type of its own is a type mismatch instead.
        exec("CREATE TABLE dtv_deft (c1 int, c3 text)");
        assertRejected("42804", "column \"c1\" is of type integer but default expression"
                + " is of type timestamp with time zone",
                "ALTER TABLE dtv_deft ALTER COLUMN c1 SET DEFAULT now()");
        assertRejected("42804", "column \"c1\" is of type integer but default expression is of type text",
                "ALTER TABLE dtv_deft ALTER COLUMN c1 SET DEFAULT 'abc'::text");
        assertRejected("42804", "column \"c1\" is of type integer but default expression is of type date",
                "ALTER TABLE dtv_deft ALTER COLUMN c1 SET DEFAULT CURRENT_DATE");
        // any of those is storable in a text column
        exec("ALTER TABLE dtv_deft ALTER COLUMN c3 SET DEFAULT now()");
    }

    @Test
    void defaultsThatCanBeEvaluatedAreUnaffected() throws Exception {
        exec("CREATE TABLE dtv_defok (c1 int, c2 int, c3 text)");
        exec("ALTER TABLE dtv_defok ALTER COLUMN c1 SET DEFAULT 5");
        exec("ALTER TABLE dtv_defok ALTER COLUMN c1 SET DEFAULT '7'");
        exec("ALTER TABLE dtv_defok ALTER COLUMN c1 SET DEFAULT (1+2)*3");
        exec("ALTER TABLE dtv_defok ALTER COLUMN c1 SET DEFAULT CASE WHEN true THEN 1 ELSE 2 END");
        exec("ALTER TABLE dtv_defok ALTER COLUMN c1 SET DEFAULT random()");
        exec("ALTER TABLE dtv_defok ALTER COLUMN c1 SET DEFAULT NULL");
        exec("ALTER TABLE dtv_defok ALTER COLUMN c3 SET DEFAULT 5");
        exec("ALTER TABLE dtv_defok ALTER COLUMN c1 DROP DEFAULT");
        exec("INSERT INTO dtv_defok (c2) VALUES (1)");
        assertEquals("null|1|5", rows("SELECT c1, c2, c3 FROM dtv_defok"));
    }

    @Test
    void createTableAndAddColumnValidateDefaultsToo() throws Exception {
        assertRejected("22P02", "invalid input syntax for type integer: \"zzz\"",
                "CREATE TABLE dtv_defc (a int DEFAULT 'zzz')");
        assertRejected("0A000", "cannot use subquery in DEFAULT expression",
                "CREATE TABLE dtv_defc (a int DEFAULT (SELECT 1))");
        assertRejected("0A000", "cannot use column reference in DEFAULT expression",
                "CREATE TABLE dtv_defc (a int, b int DEFAULT a)");
        exec("CREATE TABLE dtv_defc (a int DEFAULT 3, b text DEFAULT 'x')");
        exec("INSERT INTO dtv_defc DEFAULT VALUES");
        assertEquals("3|x", rows("SELECT a, b FROM dtv_defc"));

        exec("CREATE TABLE dtv_defa (a int)");
        assertRejected("0A000", "cannot use subquery in DEFAULT expression",
                "ALTER TABLE dtv_defa ADD COLUMN c int DEFAULT (SELECT 1)");
        assertRejected("0A000", "cannot use column reference in DEFAULT expression",
                "ALTER TABLE dtv_defa ADD COLUMN d int DEFAULT a");
        exec("ALTER TABLE dtv_defa ADD COLUMN e int DEFAULT 4");
    }

    // ---- inheritance ---------------------------------------------------

    @Test
    void anInheritedColumnBelongsToTheParent() throws Exception {
        exec("CREATE TABLE dtv_par (a int, b int)");
        exec("CREATE TABLE dtv_chi (c int) INHERITS (dtv_par)");
        assertRejected("42P16", "cannot drop inherited column \"a\"",
                "ALTER TABLE dtv_chi DROP COLUMN a");
        assertRejected("42P16", "cannot rename inherited column \"a\"",
                "ALTER TABLE dtv_chi RENAME COLUMN a TO z");
        assertRejected("42P16", "column must be added to child tables too",
                "ALTER TABLE ONLY dtv_par ADD COLUMN d int");
        assertRejected("42P16", "inherited column \"a\" must be renamed in child tables too",
                "ALTER TABLE ONLY dtv_par RENAME COLUMN a TO aa");
        // the child's own column is its own business
        exec("ALTER TABLE dtv_chi RENAME COLUMN c TO cc");
        exec("ALTER TABLE dtv_chi DROP COLUMN cc");
    }

    @Test
    void aParentsColumnListReachesItsChildren() throws Exception {
        exec("CREATE TABLE dtv_par2 (a int, b int)");
        exec("CREATE TABLE dtv_chi2 () INHERITS (dtv_par2)");
        exec("ALTER TABLE dtv_par2 ADD COLUMN e int");
        assertEquals("", rows("SELECT e FROM dtv_chi2"));
        exec("ALTER TABLE dtv_par2 RENAME COLUMN e TO ee");
        assertEquals("", rows("SELECT ee FROM dtv_chi2"));
        exec("ALTER TABLE dtv_par2 DROP COLUMN ee");
        SQLException e = assertThrows(SQLException.class, () -> rows("SELECT ee FROM dtv_chi2"));
        assertEquals("42703", e.getSQLState());
        // ONLY leaves the child holding the column as one of its own
        exec("ALTER TABLE ONLY dtv_par2 DROP COLUMN b");
        assertEquals("", rows("SELECT b FROM dtv_chi2"));
    }

    @Test
    void inheritOnlyAttachesATableThatMatches() throws Exception {
        exec("CREATE TABLE dtv_par3 (a int, b int)");
        exec("CREATE TABLE dtv_miss (a int)");
        assertRejected("42804", "child table is missing column \"b\"",
                "ALTER TABLE dtv_miss INHERIT dtv_par3");
        exec("CREATE TABLE dtv_wrong (a text, b int)");
        assertRejected("42804", "child table \"dtv_wrong\" has different type for column \"a\"",
                "ALTER TABLE dtv_wrong INHERIT dtv_par3");
        exec("CREATE TABLE dtv_match (a int, b int)");
        exec("ALTER TABLE dtv_match INHERIT dtv_par3");
        assertRejected("42P07", "would be inherited from more than once",
                "ALTER TABLE dtv_match INHERIT dtv_par3");
        exec("ALTER TABLE dtv_match NO INHERIT dtv_par3");
        assertRejected("42P01", "relation \"dtv_par3\" is not a parent of relation \"dtv_match\"",
                "ALTER TABLE dtv_match NO INHERIT dtv_par3");
    }

    @Test
    void aPartitionsColumnListFollowsItsParent() throws Exception {
        exec("CREATE TABLE dtv_prt (i int, t text) PARTITION BY RANGE (i)");
        exec("CREATE TABLE dtv_prt_p0 PARTITION OF dtv_prt FOR VALUES FROM (0) TO (10)");
        assertRejected("42P16", "column must be added to child tables too",
                "ALTER TABLE ONLY dtv_prt ADD COLUMN extra2 text");
        exec("ALTER TABLE dtv_prt ADD COLUMN extra3 text");
        assertEquals("", rows("SELECT extra3 FROM dtv_prt_p0"));
        assertRejected("42P16", "cannot drop inherited column \"t\"",
                "ALTER TABLE dtv_prt_p0 DROP COLUMN t");
        assertRejected("42P16", "cannot rename inherited column \"t\"",
                "ALTER TABLE dtv_prt_p0 RENAME COLUMN t TO tt");
    }

    // ---- NOT NULL and the primary key ----------------------------------

    @Test
    void aPrimaryKeyColumnKeepsItsNotNull() throws Exception {
        exec("CREATE TABLE dtv_nn (i int PRIMARY KEY, j int NOT NULL, k int UNIQUE NOT NULL)");
        assertRejected("42P16", "column \"i\" is in a primary key",
                "ALTER TABLE dtv_nn ALTER COLUMN i DROP NOT NULL");
        // only the primary key is special
        exec("ALTER TABLE dtv_nn ALTER COLUMN j DROP NOT NULL");
        exec("ALTER TABLE dtv_nn ALTER COLUMN k DROP NOT NULL");
        assertRejected("42703", "column \"nosuchcol\" of relation \"dtv_nn\" does not exist",
                "ALTER TABLE dtv_nn ALTER COLUMN nosuchcol DROP NOT NULL");

        exec("CREATE TABLE dtv_nn2 (i int, j int, PRIMARY KEY (i, j))");
        assertRejected("42P16", "column \"j\" is in a primary key",
                "ALTER TABLE dtv_nn2 ALTER COLUMN j DROP NOT NULL");
    }

    // ---- CHECK and POLICY ----------------------------------------------

    @Test
    void aCheckOrPolicyExpressionMustBeAPredicate() throws Exception {
        exec("CREATE TABLE dtv_chk (i int, t text, n numeric, b boolean)");
        assertRejected("42804", "argument of CHECK must be type boolean, not type integer",
                "ALTER TABLE dtv_chk ADD CONSTRAINT dtv_ck1 CHECK (i)");
        assertRejected("42804", "argument of CHECK must be type boolean, not type text",
                "ALTER TABLE dtv_chk ADD CONSTRAINT dtv_ck2 CHECK (t)");
        assertRejected("42804", "argument of CHECK must be type boolean, not type numeric",
                "ALTER TABLE dtv_chk ADD CONSTRAINT dtv_ck3 CHECK (n)");
        assertRejected("42804", "argument of CHECK must be type boolean, not type text",
                "ALTER TABLE dtv_chk ADD CONSTRAINT dtv_ck4 CHECK (i::text)");
        assertRejected("42804", "argument of POLICY must be type boolean, not type integer",
                "CREATE POLICY dtv_pol1 ON dtv_chk FOR SELECT USING (i)");
        assertRejected("42804", "argument of CHECK must be type boolean, not type integer",
                "CREATE TABLE dtv_chk2 (i int CHECK (i))");
        assertRejected("42804", "argument of CHECK must be type boolean, not type integer",
                "CREATE TABLE dtv_chk2 (i int, CHECK (i))");
    }

    @Test
    void predicatesAndUntypedExpressionsStillPass() throws Exception {
        exec("CREATE TABLE dtv_chkok (i int, b boolean)");
        exec("ALTER TABLE dtv_chkok ADD CONSTRAINT dtv_ok1 CHECK (i > 0)");
        exec("ALTER TABLE dtv_chkok ADD CONSTRAINT dtv_ok2 CHECK (b)");
        exec("ALTER TABLE dtv_chkok ADD CONSTRAINT dtv_ok3 CHECK (NULL)");
        exec("ALTER TABLE dtv_chkok ADD CONSTRAINT dtv_ok4 CHECK ('t')");
        exec("CREATE POLICY dtv_pol2 ON dtv_chkok FOR SELECT USING (i > 0)");
        exec("INSERT INTO dtv_chkok VALUES (1, true)");
        SQLException e = assertThrows(SQLException.class,
                () -> exec("INSERT INTO dtv_chkok VALUES (-1, true)"));
        assertEquals("23514", e.getSQLState());
    }

    // ---- COLLATE -------------------------------------------------------

    @Test
    void collateNeedsATypeThatCarriesACollation() throws Exception {
        exec("CREATE TABLE dtv_col (i int, t text)");
        assertRejected("42804", "collations are not supported by type bigint",
                "ALTER TABLE dtv_col ALTER COLUMN i TYPE bigint COLLATE \"C\"");
        assertRejected("42804", "collations are not supported by type integer",
                "CREATE TABLE dtv_col2 (i int COLLATE \"C\")");
        assertRejected("42804", "collations are not supported by type date",
                "CREATE TABLE dtv_col2 (d date COLLATE \"C\")");
        // the collatable types are unaffected
        exec("ALTER TABLE dtv_col ALTER COLUMN t TYPE varchar(20) COLLATE \"C\"");
        exec("ALTER TABLE dtv_col ALTER COLUMN t TYPE text COLLATE \"C\"");
        exec("CREATE TABLE dtv_col2 (t text COLLATE \"C\", v varchar(4) COLLATE \"C\")");
        exec("INSERT INTO dtv_col2 VALUES ('a', 'b')");
        assertEquals("a|b", rows("SELECT t, v FROM dtv_col2"));
    }

    // ---- identity ------------------------------------------------------

    @Test
    void identityNeedsAnIntegerTypeAndNoCompetingDefault() {
        for (String type : new String[]{"text", "numeric", "date"}) {
            assertRejected("22023", "identity column type must be smallint, integer, or bigint",
                    "CREATE TABLE dtv_idbad (i " + type + " GENERATED ALWAYS AS IDENTITY)");
        }
        assertRejected("42601", "both default and identity specified for column \"i\" of table \"dtv_idbad\"",
                "CREATE TABLE dtv_idbad (i int GENERATED ALWAYS AS IDENTITY DEFAULT 1)");
        assertRejected("42601", "both default and identity specified for column \"i\" of table \"dtv_idbad\"",
                "CREATE TABLE dtv_idbad (i int DEFAULT 1 GENERATED ALWAYS AS IDENTITY)");
    }

    @Test
    void dropIdentityComplainsWhenThereIsNoneToDrop() throws Exception {
        exec("CREATE TABLE dtv_idt (i bigint GENERATED ALWAYS AS IDENTITY, j int)");
        exec("INSERT INTO dtv_idt (j) VALUES (1)");
        assertEquals("1|1", rows("SELECT i, j FROM dtv_idt"));
        assertRejected("55000", "column \"j\" of relation \"dtv_idt\" is not an identity column",
                "ALTER TABLE dtv_idt ALTER COLUMN j DROP IDENTITY");
        exec("ALTER TABLE dtv_idt ALTER COLUMN j DROP IDENTITY IF EXISTS");
        assertRejected("42703", "column \"nosuchcol\" of relation \"dtv_idt\" does not exist",
                "ALTER TABLE dtv_idt ALTER COLUMN nosuchcol DROP IDENTITY IF EXISTS");
        exec("ALTER TABLE dtv_idt ALTER COLUMN i DROP IDENTITY");
        assertRejected("55000", "column \"i\" of relation \"dtv_idt\" is not an identity column",
                "ALTER TABLE dtv_idt ALTER COLUMN i DROP IDENTITY");
    }

    @Test
    void addGeneratedNeedsANotNullColumnWithoutAnIdentity() throws Exception {
        exec("CREATE TABLE dtv_idt2 (i int, j int NOT NULL, k int GENERATED ALWAYS AS IDENTITY)");
        assertRejected("55000", "column \"i\" of relation \"dtv_idt2\" must be declared NOT NULL"
                + " before identity can be added",
                "ALTER TABLE dtv_idt2 ALTER COLUMN i ADD GENERATED ALWAYS AS IDENTITY");
        assertRejected("55000", "column \"k\" of relation \"dtv_idt2\" is already an identity column",
                "ALTER TABLE dtv_idt2 ALTER COLUMN k ADD GENERATED ALWAYS AS IDENTITY");
        exec("ALTER TABLE dtv_idt2 ALTER COLUMN j ADD GENERATED ALWAYS AS IDENTITY");
        exec("INSERT INTO dtv_idt2 (i) VALUES (5)");
        assertEquals("5|1|1", rows("SELECT i, j, k FROM dtv_idt2"));
    }

    @Test
    void identityBehaviourItselfIsUnchanged() throws Exception {
        exec("CREATE TABLE dtv_idt3 (i smallint GENERATED BY DEFAULT AS IDENTITY, j int)");
        exec("INSERT INTO dtv_idt3 (j) VALUES (5)");
        exec("INSERT INTO dtv_idt3 (i, j) VALUES (9, 6)");
        assertEquals("1|5;9|6", rows("SELECT i, j FROM dtv_idt3 ORDER BY j"));
    }

    // ---- generated columns ---------------------------------------------

    @Test
    void aVirtualGeneratedColumnCannotBeKeyed() throws Exception {
        assertRejected("0A000", "primary keys on virtual generated columns are not supported",
                "CREATE TABLE dtv_vg (a int, b int GENERATED ALWAYS AS (a) VIRTUAL PRIMARY KEY)");
        assertRejected("0A000", "unique constraints on virtual generated columns are not supported",
                "CREATE TABLE dtv_vg (a int, b int GENERATED ALWAYS AS (a) VIRTUAL UNIQUE)");
        assertRejected("0A000", "primary keys on virtual generated columns are not supported",
                "CREATE TABLE dtv_vg (a int, b int GENERATED ALWAYS AS (a) VIRTUAL, PRIMARY KEY (b))");
        // a stored one has values for an index to hold
        exec("CREATE TABLE dtv_vg (a int, b int GENERATED ALWAYS AS (a) STORED PRIMARY KEY)");
        exec("INSERT INTO dtv_vg (a) VALUES (3)");
        assertEquals("3|3", rows("SELECT a, b FROM dtv_vg"));
        // and a virtual column with no key on it is fine
        exec("CREATE TABLE dtv_vg2 (a int, b int GENERATED ALWAYS AS (a*2) VIRTUAL)");
        exec("INSERT INTO dtv_vg2 (a) VALUES (3)");
        assertEquals("3|6", rows("SELECT a, b FROM dtv_vg2"));
    }

    @Test
    void dropExpressionMakesAStoredGeneratedColumnOrdinary() throws Exception {
        exec("CREATE TABLE dtv_dex (a int, b int GENERATED ALWAYS AS (a*2) STORED,"
                + " c int GENERATED ALWAYS AS (a+1) VIRTUAL)");
        exec("INSERT INTO dtv_dex (a) VALUES (4)");
        exec("ALTER TABLE dtv_dex ALTER COLUMN b DROP EXPRESSION");
        // the values already computed are kept
        assertEquals("4|8|5", rows("SELECT a, b, c FROM dtv_dex"));
        assertEquals("", scalar("SELECT attgenerated FROM pg_attribute"
                + " WHERE attrelid='dtv_dex'::regclass AND attname='b'"));
        // and the column now takes a written value
        exec("INSERT INTO dtv_dex (a, b) VALUES (1, 9)");
        assertEquals("1|9|2;4|8|5", rows("SELECT a, b, c FROM dtv_dex ORDER BY a"));
        // the virtual column still refuses one
        SQLException e = assertThrows(SQLException.class,
                () -> exec("INSERT INTO dtv_dex (a, c) VALUES (2, 9)"));
        assertEquals("428C9", e.getSQLState());
    }

    @Test
    void dropExpressionReportsWhatItCannotDo() throws Exception {
        exec("CREATE TABLE dtv_dex2 (a int, b int GENERATED ALWAYS AS (a*2) STORED,"
                + " c int GENERATED ALWAYS AS (a+1) VIRTUAL)");
        assertRejected("0A000", "DROP EXPRESSION is not supported for virtual generated columns",
                "ALTER TABLE dtv_dex2 ALTER COLUMN c DROP EXPRESSION");
        exec("ALTER TABLE dtv_dex2 ALTER COLUMN b DROP EXPRESSION");
        assertRejected("55000", "column \"b\" of relation \"dtv_dex2\" is not a generated column",
                "ALTER TABLE dtv_dex2 ALTER COLUMN b DROP EXPRESSION");
        // IF EXISTS makes that silent, but never covers a column that is not there
        exec("ALTER TABLE dtv_dex2 ALTER COLUMN b DROP EXPRESSION IF EXISTS");
        assertRejected("42703", "column \"nosuchcol\" of relation \"dtv_dex2\" does not exist",
                "ALTER TABLE dtv_dex2 ALTER COLUMN nosuchcol DROP EXPRESSION IF EXISTS");
    }

    // ---- storage and compression ---------------------------------------

    @Test
    void storageAndCompressionOptionsAreValidated() throws Exception {
        exec("CREATE TABLE dtv_sto (i int, c text, n numeric, u uuid, ia int[])");
        assertRejected("22023", "invalid storage type \"nonsense\"",
                "ALTER TABLE dtv_sto ALTER COLUMN c SET STORAGE NONSENSE");
        assertRejected("0A000", "column data type integer can only have storage PLAIN",
                "ALTER TABLE dtv_sto ALTER COLUMN i SET STORAGE EXTERNAL");
        assertRejected("0A000", "column data type uuid can only have storage PLAIN",
                "ALTER TABLE dtv_sto ALTER COLUMN u SET STORAGE MAIN");
        assertRejected("22023", "invalid compression method \"nosuchmethod\"",
                "ALTER TABLE dtv_sto ALTER COLUMN c SET COMPRESSION nosuchmethod");
        assertRejected("0A000", "column data type integer does not support compression",
                "ALTER TABLE dtv_sto ALTER COLUMN i SET COMPRESSION pglz");
        assertRejected("42703", "column \"nosuchcol\" of relation \"dtv_sto\" does not exist",
                "ALTER TABLE dtv_sto ALTER COLUMN nosuchcol SET STORAGE PLAIN");
    }

    @Test
    void theStorageOptionsThatDoApplyAreRecorded() throws Exception {
        exec("CREATE TABLE dtv_sto2 (i int, c text, n numeric, u uuid, ia int[])");
        exec("ALTER TABLE dtv_sto2 ALTER COLUMN i SET STORAGE PLAIN");
        exec("ALTER TABLE dtv_sto2 ALTER COLUMN c SET STORAGE MAIN");
        exec("ALTER TABLE dtv_sto2 ALTER COLUMN n SET STORAGE EXTENDED");
        exec("ALTER TABLE dtv_sto2 ALTER COLUMN ia SET STORAGE EXTERNAL");
        exec("ALTER TABLE dtv_sto2 ALTER COLUMN c SET COMPRESSION pglz");
        exec("ALTER TABLE dtv_sto2 ALTER COLUMN c SET COMPRESSION DEFAULT");
        exec("ALTER TABLE dtv_sto2 ALTER COLUMN n SET COMPRESSION pglz");
        assertEquals("i|p;c|m;n|x;u|p;ia|e",
                rows("SELECT attname, attstorage FROM pg_attribute"
                        + " WHERE attrelid='dtv_sto2'::regclass AND attnum>0 ORDER BY attnum"));
    }

    // ---- SET CONSTRAINTS -----------------------------------------------

    @Test
    void setConstraintsNamesAConstraintThatExists() throws Exception {
        assertRejected("42704", "constraint \"dtv_no_such_constraint\" does not exist",
                "SET CONSTRAINTS dtv_no_such_constraint DEFERRED");
        exec("SET CONSTRAINTS ALL DEFERRED");
        exec("SET CONSTRAINTS ALL IMMEDIATE");
        exec("CREATE TABLE dtv_cons (i int, CONSTRAINT dtv_uq1 UNIQUE (i) DEFERRABLE)");
        exec("SET CONSTRAINTS dtv_uq1 DEFERRED");
        exec("SET CONSTRAINTS dtv_uq1 IMMEDIATE");
        assertRejected("42704", "constraint \"dtv_no_such_constraint\" does not exist",
                "SET CONSTRAINTS dtv_uq1, dtv_no_such_constraint IMMEDIATE");
    }

    // ---- temporary and permanent tables --------------------------------

    @Test
    void onCommitIsForTemporaryTablesOnly() {
        assertRejected("42P16", "ON COMMIT can only be used on temporary tables",
                "CREATE TABLE dtv_oc (i int) ON COMMIT DELETE ROWS");
        assertRejected("42P16", "ON COMMIT can only be used on temporary tables",
                "CREATE TABLE dtv_oc (i int) ON COMMIT PRESERVE ROWS");
        assertRejected("42P16", "ON COMMIT can only be used on temporary tables",
                "CREATE TABLE dtv_oc (i int) ON COMMIT DROP");
    }

    @Test
    void aForeignKeyMayNotCrossTheTemporaryBoundary() throws Exception {
        exec("CREATE TABLE dtv_perm (i int PRIMARY KEY)");
        assertRejected("42P16", "constraints on temporary tables may reference only temporary tables",
                "CREATE TEMP TABLE dtv_tmp0 (i int REFERENCES dtv_perm(i))");
        exec("CREATE TEMP TABLE dtv_tmp1 (i int PRIMARY KEY)");
        assertRejected("42P16", "constraints on permanent tables may reference only permanent tables",
                "CREATE TABLE dtv_perm2 (i int REFERENCES dtv_tmp1(i))");
        assertRejected("42P16", "constraints on permanent tables may reference only permanent tables",
                "CREATE TABLE dtv_perm2 (i int, FOREIGN KEY (i) REFERENCES dtv_tmp1(i))");
        assertRejected("42P16", "constraints on permanent tables may reference only permanent tables",
                "ALTER TABLE dtv_perm ADD CONSTRAINT dtv_fk9 FOREIGN KEY (i) REFERENCES dtv_tmp1(i)");
        // temp to temp, and permanent to permanent, are both fine
        exec("CREATE TEMP TABLE dtv_tmp2 (i int REFERENCES dtv_tmp1(i)) ON COMMIT DELETE ROWS");
        exec("CREATE TABLE dtv_perm3 (i int REFERENCES dtv_perm(i))");
    }

    // ---- CREATE TABLE AS and LIKE --------------------------------------

    @Test
    void createTableAsRejectsARepeatedOutputName() throws Exception {
        assertRejected("42701", "column \"x\" specified more than once",
                "CREATE TABLE dtv_cta AS SELECT 1 AS x, 2 AS x");
        exec("CREATE TABLE dtv_cta AS SELECT 1 AS x, 2 AS y");
        assertEquals("1|2", rows("SELECT x, y FROM dtv_cta"));
    }

    @Test
    void likeRejectsAnOptionThatIsNotOne() throws Exception {
        exec("CREATE TABLE dtv_like_src (a int, b text DEFAULT 'z')");
        SQLException e = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE dtv_like1 (LIKE dtv_like_src INCLUDING NOSUCHOPT)"));
        assertEquals("42601", e.getSQLState());
        SQLException e2 = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE dtv_like1 (LIKE dtv_like_src EXCLUDING NOSUCHOPT)"));
        assertEquals("42601", e2.getSQLState());
        // the real options still work
        exec("CREATE TABLE dtv_like1 (LIKE dtv_like_src INCLUDING ALL)");
        exec("INSERT INTO dtv_like1 (a) VALUES (1)");
        assertEquals("1|z", rows("SELECT a, b FROM dtv_like1"));
        exec("CREATE TABLE dtv_like2 (LIKE dtv_like_src INCLUDING DEFAULTS EXCLUDING INDEXES)");
        exec("INSERT INTO dtv_like2 (a) VALUES (1)");
        assertEquals("1|z", rows("SELECT a, b FROM dtv_like2"));
    }

    // ---- valid SQL that used to be a syntax error ----------------------

    @Test
    void setWithoutClusterAndWithoutOidsParse() throws Exception {
        exec("CREATE TABLE dtv_misc (i int)");
        exec("ALTER TABLE dtv_misc SET WITHOUT CLUSTER");
        exec("ALTER TABLE dtv_misc SET WITHOUT OIDS");
        // the neighbouring SET forms still work
        exec("ALTER TABLE dtv_misc SET (fillfactor = 70)");
        exec("ALTER TABLE dtv_misc SET UNLOGGED");
        exec("ALTER TABLE dtv_misc SET LOGGED");
    }
}
