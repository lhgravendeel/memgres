package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A type belongs to a schema, and a schema holds one name across its types and its relations.
 *
 * <p>Every expectation here was measured against PostgreSQL 18. A composite type owns a
 * {@code pg_class} row as well as a {@code pg_type} row, so its name is taken for tables, views,
 * sequences and indexes too, and {@code DROP TABLE} over one is the wrong kind of statement
 * rather than a statement about something that is not there. A table, a view and a materialized
 * view own a row type, so their names are taken for types and {@code DROP TYPE} may not take that
 * row type away on its own. A sequence and an index own no row type, so an enum or a domain may
 * share a name with either.
 *
 * <p>Which check a statement reports first decides the SQLSTATE: a statement making a type looks
 * at the type name first ({@code 42710}) and at the relation name after ({@code 42P07}); a
 * statement making a relation looks the other way round.
 */
class TypeNamespacePerSchemaTest {

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

    @BeforeEach
    void freshSchemas() throws Exception {
        exec("SET search_path = public");
        exec("DROP SCHEMA IF EXISTS tns_a CASCADE");
        exec("DROP SCHEMA IF EXISTS tns_b CASCADE");
        exec("CREATE SCHEMA tns_a");
        exec("CREATE SCHEMA tns_b");
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

    /** Asserts the SQLSTATE and that the first line of the message reads as PostgreSQL's does. */
    private static void assertError(String state, String messagePart, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(state, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage() != null && e.getMessage().contains(messagePart),
                "wrong message for: " + sql + " -> " + e.getMessage());
    }

    private static String typeSchema(String typeName) throws SQLException {
        return scalar("SELECT n.nspname FROM pg_type t JOIN pg_namespace n"
                + " ON n.oid = t.typnamespace WHERE t.typname = '" + typeName + "'");
    }

    // ------------------------------------------------------------- placement

    @Test
    void aQualifiedCreateTypePutsTheTypeInTheSchemaItNames() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int, b text)");
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE DOMAIN tns_a.tns_dom AS int");
        exec("CREATE TYPE tns_a.tns_rng AS RANGE (subtype = int4)");
        assertEquals("tns_a", typeSchema("tns_comp"));
        assertEquals("tns_a", typeSchema("tns_enum"));
        assertEquals("tns_a", typeSchema("tns_dom"));
        assertEquals("tns_a", typeSchema("tns_rng"));
    }

    @Test
    void anUnqualifiedCreateTypeLandsWhereTheSearchPathPutsACreate() throws Exception {
        exec("SET search_path = tns_a, public");
        exec("CREATE TYPE tns_sp AS (a int)");
        exec("SET search_path = public");
        assertEquals("tns_a", typeSchema("tns_sp"));
    }

    @Test
    void aCompositeTypeOwnsARelationInItsOwnSchema() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int, b text)");
        assertEquals("tns_a", scalar("SELECT n.nspname FROM pg_class c JOIN pg_namespace n"
                + " ON n.oid = c.relnamespace WHERE c.relname = 'tns_comp'"));
        assertEquals("c", scalar("SELECT c.relkind::text FROM pg_class c"
                + " WHERE c.relname = 'tns_comp'"));
        assertEquals("true", scalar("SELECT (t.typrelid = c.oid)::text FROM pg_type t"
                + " JOIN pg_class c ON c.relname = t.typname WHERE t.typname = 'tns_comp'"));
        assertEquals("tns_a", scalar("SELECT udt_schema FROM information_schema.attributes"
                + " WHERE udt_name = 'tns_comp' AND attribute_name = 'a'"));
    }

    @Test
    void aTypeNameIsFreeAgainInAnotherSchema() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        exec("CREATE TABLE tns_b.tns_comp (a int)");
        assertEquals("2", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'tns_comp'"));
        exec("INSERT INTO tns_b.tns_comp VALUES (3)");
        assertEquals("3", scalar("SELECT a::text FROM tns_b.tns_comp"));
    }

    // ------------------------------- a composite type sits in both namespaces

    @Test
    void aCompositeTypeBlocksEveryKindOfRelationOfThatName() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        exec("CREATE TABLE tns_a.tns_base (a int)");
        assertError("42P07", "relation \"tns_comp\" already exists",
                "CREATE TABLE tns_a.tns_comp (a int)");
        assertError("42P07", "relation \"tns_comp\" already exists",
                "CREATE SEQUENCE tns_a.tns_comp");
        assertError("42P07", "relation \"tns_comp\" already exists",
                "CREATE VIEW tns_a.tns_comp AS SELECT 1");
        assertError("42P07", "relation \"tns_comp\" already exists",
                "CREATE MATERIALIZED VIEW tns_a.tns_comp AS SELECT 1");
        assertError("42P07", "relation \"tns_comp\" already exists",
                "CREATE INDEX tns_comp ON tns_a.tns_base (a)");
        assertError("42P07", "relation \"tns_comp\" already exists",
                "ALTER TABLE tns_a.tns_base RENAME TO tns_comp");
    }

    @Test
    void theWrongDropOverACompositeTypeIsTheWrongKindNotAMissingObject() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        assertError("42809", "\"tns_comp\" is not a table", "DROP TABLE tns_a.tns_comp");
        // IF EXISTS says what to skip; it does not make DROP TABLE the right statement.
        assertError("42809", "\"tns_comp\" is not a table", "DROP TABLE IF EXISTS tns_a.tns_comp");
        assertError("42809", "\"tns_comp\" is not a view", "DROP VIEW tns_a.tns_comp");
        assertError("42809", "\"tns_comp\" is not an index", "DROP INDEX tns_a.tns_comp");
        assertError("42809", "\"tns_a.tns_comp\" is not a domain", "DROP DOMAIN tns_a.tns_comp");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_type WHERE typname = 'tns_comp'"));
    }

    // ------------------------------------- a relation's row type takes a name

    @Test
    void aTablesRowTypeTakesTheNameForEveryKindOfType() throws Exception {
        exec("CREATE TABLE tns_a.tns_tab (a int)");
        assertError("42710", "type \"tns_tab\" already exists",
                "CREATE TYPE tns_a.tns_tab AS (a int)");
        assertError("42710", "type \"tns_tab\" already exists",
                "CREATE TYPE tns_a.tns_tab AS ENUM ('x')");
        assertError("42710", "type \"tns_tab\" already exists",
                "CREATE DOMAIN tns_a.tns_tab AS int");
        assertError("42710", "type \"tns_tab\" already exists",
                "CREATE TYPE tns_a.tns_tab AS RANGE (subtype = int4)");
        assertError("42710", "type \"tns_tab\" already exists", "CREATE TYPE tns_a.tns_tab");
    }

    @Test
    void dropTypeMayNotTakeAwayARelationsRowType() throws Exception {
        exec("CREATE TABLE tns_a.tns_tab (a int)");
        exec("CREATE VIEW tns_a.tns_vw AS SELECT 1 AS a");
        assertError("2BP01", "cannot drop type tns_a.tns_tab because table tns_a.tns_tab requires it",
                "DROP TYPE tns_a.tns_tab");
        assertError("2BP01", "cannot drop type tns_a.tns_tab because table tns_a.tns_tab requires it",
                "DROP TYPE IF EXISTS tns_a.tns_tab");
        assertError("2BP01", "cannot drop type tns_a.tns_vw because view tns_a.tns_vw requires it",
                "DROP TYPE tns_a.tns_vw");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_class"
                + " WHERE relname = 'tns_tab' AND relkind = 'r'"));
    }

    @Test
    void aSequenceAndAnIndexOwnNoRowTypeSoATypeMayShareTheirName() throws Exception {
        exec("CREATE SEQUENCE tns_a.tns_seq");
        exec("CREATE TYPE tns_a.tns_seq AS ENUM ('x')");
        assertEquals("e", scalar("SELECT typtype::text FROM pg_type WHERE typname = 'tns_seq'"));
        exec("DROP TYPE tns_a.tns_seq");
        // A composite type does own a relation, so the sequence's name is not free for one.
        assertError("42P07", "relation \"tns_seq\" already exists",
                "CREATE TYPE tns_a.tns_seq AS (a int)");

        exec("CREATE TABLE tns_a.tns_base (a int)");
        exec("CREATE INDEX tns_idx ON tns_a.tns_base (a)");
        exec("CREATE DOMAIN tns_a.tns_idx AS int");
        assertEquals("tns_a", typeSchema("tns_idx"));
    }

    @Test
    void aLabelBelongsToAnEnumAndToNothingElse() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        exec("CREATE TABLE tns_a.tns_tab (a int)");
        assertError("42809", "tns_a.tns_comp is not an enum",
                "ALTER TYPE tns_a.tns_comp ADD VALUE 'q'");
        assertError("42809", "tns_a.tns_comp is not an enum",
                "ALTER TYPE tns_a.tns_comp RENAME VALUE 'q' TO 'r'");
        assertError("42809", "tns_a.tns_tab is not an enum",
                "ALTER TYPE tns_a.tns_tab ADD VALUE 'q'");
    }

    // ------------------------------------------- a qualifier reaches one schema

    @Test
    void aWrongQualifierLeavesTheOtherSchemasTypeAlone() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        assertError("42704", "type \"tns_b.tns_enum\" does not exist", "DROP TYPE tns_b.tns_enum");
        assertError("42704", "type \"tns_b.tns_enum\" does not exist",
                "ALTER TYPE tns_b.tns_enum RENAME TO tns_enum2");
        assertError("42704", "type \"tns_b.tns_enum\" does not exist",
                "ALTER TYPE tns_b.tns_enum ADD VALUE 'q'");
        assertEquals("tns_a", typeSchema("tns_enum"));
    }

    @Test
    void anUnqualifiedDropTypeReachesOnlyTheSearchPath() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("SET search_path = tns_b");
        assertError("42704", "type \"tns_enum\" does not exist", "DROP TYPE tns_enum");
        exec("DROP TYPE IF EXISTS tns_enum");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_type WHERE typname = 'tns_enum'"));
        exec("SET search_path = tns_b, tns_a");
        exec("DROP TYPE tns_enum");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_type WHERE typname = 'tns_enum'"));
        exec("SET search_path = public");
    }

    // --------------------------------------------------- rename and set schema

    @Test
    void aRenameKeepsTheTypeWhereItWasAndSetSchemaMovesIt() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("ALTER TYPE tns_a.tns_enum RENAME TO tns_enum2");
        assertEquals("tns_a", typeSchema("tns_enum2"));
        exec("ALTER TYPE tns_a.tns_enum2 SET SCHEMA tns_b");
        assertEquals("tns_b", typeSchema("tns_enum2"));

        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        exec("ALTER TYPE tns_a.tns_comp SET SCHEMA tns_b");
        assertEquals("tns_b", scalar("SELECT n.nspname FROM pg_class c JOIN pg_namespace n"
                + " ON n.oid = c.relnamespace WHERE c.relname = 'tns_comp'"));
    }

    // ----------------------------------------------------------- dependencies

    @Test
    void aColumnDeclaredAsATypeBlocksTheDrop() throws Exception {
        exec("CREATE DOMAIN tns_a.tns_dom AS int CHECK (VALUE > 0)");
        exec("CREATE TABLE tns_a.tns_uses (d tns_a.tns_dom)");
        assertError("2BP01", "cannot drop type tns_a.tns_dom because other objects depend on it",
                "DROP DOMAIN tns_a.tns_dom");
        exec("DROP TABLE tns_a.tns_uses");
        // DROP TYPE names a domain too: PostgreSQL has one type namespace.
        exec("DROP TYPE tns_a.tns_dom");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_type WHERE typname = 'tns_dom'"));
    }

    @Test
    void aQualifiedCompositeTypeIsUsableAsAColumnType() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int, b text)");
        exec("CREATE TABLE tns_a.tns_holder (v tns_a.tns_comp)");
        exec("INSERT INTO tns_a.tns_holder VALUES (ROW(4, 'four'))");
        assertEquals("4", scalar("SELECT (v).a::text FROM tns_a.tns_holder"));
        assertEquals("four", scalar("SELECT (v).b FROM tns_a.tns_holder"));
        assertError("2BP01", "cannot drop type tns_a.tns_comp because other objects depend on it",
                "DROP TYPE tns_a.tns_comp");
    }

    // ------------------------------------------------------ dropping a schema

    @Test
    void droppingASchemaTakesItsTypesWithIt() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE DOMAIN tns_a.tns_dom AS int");
        exec("CREATE TYPE tns_a.tns_rng AS RANGE (subtype = int4)");
        exec("DROP SCHEMA tns_a CASCADE");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_type WHERE typname IN"
                + " ('tns_comp', 'tns_enum', 'tns_dom', 'tns_rng')"));
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_class WHERE relname = 'tns_comp'"));
        // The name is free again in a schema of the same name.
        exec("CREATE SCHEMA tns_a");
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        assertEquals("tns_a", scalar("SELECT n.nspname FROM pg_class c JOIN pg_namespace n"
                + " ON n.oid = c.relnamespace WHERE c.relname = 'tns_comp'"));
    }

    // ------------------------------------------------ nothing changed in public

    @Test
    void unqualifiedNamesInPublicStillBehave() throws Exception {
        exec("DROP TABLE IF EXISTS tns_pub_t");
        exec("DROP TYPE IF EXISTS tns_pub");
        exec("CREATE TYPE tns_pub AS ENUM ('a', 'b')");
        assertEquals("public", typeSchema("tns_pub"));
        exec("CREATE TABLE tns_pub_t (c tns_pub)");
        exec("INSERT INTO tns_pub_t VALUES ('a')");
        assertEquals("a", scalar("SELECT c::text FROM tns_pub_t"));
        exec("DROP TABLE tns_pub_t");
        exec("DROP TYPE tns_pub");
    }

    // --------------------------------- a relation's row type takes a type name

    /**
     * A table carries a row type of its own name, so a name an enum, a domain or a range already
     * answers to is taken for it — {@code 42710} naming the type, with the hint PostgreSQL adds
     * to say why a name nothing seems to hold is refused.
     */
    @Test
    void createTableOverAnEnumDomainOrRangeIsADuplicateType() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE DOMAIN tns_a.tns_dom AS int");
        exec("CREATE TYPE tns_a.tns_rng AS RANGE (subtype = int4)");
        assertError("42710", "type \"tns_enum\" already exists",
                "CREATE TABLE tns_a.tns_enum (i int)");
        assertError("42710", "type \"tns_dom\" already exists",
                "CREATE TABLE tns_a.tns_dom (i int)");
        assertError("42710", "type \"tns_rng\" already exists",
                "CREATE TABLE tns_a.tns_rng (i int)");
        SQLException e = assertThrows(SQLException.class,
                () -> exec("CREATE TABLE tns_a.tns_enum (i int)"));
        assertTrue(e.getMessage().contains("A relation has an associated type of the same name"),
                "expected PostgreSQL's hint, got: " + e.getMessage());
    }

    /** The same name in another schema is free: a type takes a name in its own schema only. */
    @Test
    void aTypeInOneSchemaDoesNotBlockATableInAnother() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE TABLE tns_b.tns_enum (i int)");
        assertEquals("tns_b", scalar("SELECT n.nspname FROM pg_class c JOIN pg_namespace n"
                + " ON n.oid = c.relnamespace WHERE c.relname = 'tns_enum' AND c.relkind = 'r'"));
    }

    /**
     * An index carries no row type, so it may share a name with an enum; a shell type is a
     * reservation the new relation's row type fills in rather than a name it collides with.
     */
    @Test
    void anIndexAndAShellTypeDoNotBlockATableOfThatName() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE TABLE tns_a.tns_base (i int)");
        exec("CREATE INDEX tns_enum ON tns_a.tns_base (i)");
        exec("CREATE TYPE tns_a.tns_shell");
        exec("CREATE TABLE tns_a.tns_shell (i int)");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_class c JOIN pg_namespace n"
                + " ON n.oid = c.relnamespace WHERE n.nspname = 'tns_a'"
                + " AND c.relname = 'tns_shell' AND c.relkind = 'r'"));
    }

    /**
     * A rename has no new type to write into a reserved name, so a shell blocks it where it did
     * not block a CREATE — and the relation namespace is still looked at first, which is what
     * separates {@code 42P07} from {@code 42710}.
     */
    @Test
    void renamingATableOntoATypeNameIsADuplicateTypeAfterTheRelationCheck() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE DOMAIN tns_a.tns_dom AS int");
        exec("CREATE TYPE tns_a.tns_shell");
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        exec("CREATE TABLE tns_a.tns_other (i int)");
        exec("CREATE TABLE tns_a.tns_t (i int)");
        assertError("42710", "type \"tns_enum\" already exists",
                "ALTER TABLE tns_a.tns_t RENAME TO tns_enum");
        assertError("42710", "type \"tns_dom\" already exists",
                "ALTER TABLE tns_a.tns_t RENAME TO tns_dom");
        assertError("42710", "type \"tns_shell\" already exists",
                "ALTER TABLE tns_a.tns_t RENAME TO tns_shell");
        // A composite and another table both own a relation, so the relation check reports first.
        assertError("42P07", "relation \"tns_comp\" already exists",
                "ALTER TABLE tns_a.tns_t RENAME TO tns_comp");
        assertError("42P07", "relation \"tns_other\" already exists",
                "ALTER TABLE tns_a.tns_t RENAME TO tns_other");
        // Nothing was renamed by any of them.
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_class c JOIN pg_namespace n"
                + " ON n.oid = c.relnamespace WHERE n.nspname = 'tns_a' AND c.relname = 'tns_t'"));
    }

    // ------------------------------------- a composite type is not a table

    /**
     * A composite type owns a {@code pg_class} row, so a query reaches it and is refused for what
     * it is. Reporting it missing sent the reader looking for a relation that is there.
     */
    @Test
    void readingOrWritingACompositeTypeIsTheWrongKindNotAMissingRelation() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        exec("SET search_path = tns_a, public");
        for (String sql : new String[]{
                "SELECT * FROM tns_comp",
                "SELECT count(*) FROM tns_comp",
                "INSERT INTO tns_comp VALUES (1)",
                "UPDATE tns_comp SET a = 1",
                "DELETE FROM tns_comp"}) {
            SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
            assertEquals("42809", e.getSQLState(), sql + " -> " + e.getMessage());
            assertTrue(e.getMessage().contains("cannot open relation \"tns_comp\""),
                    sql + " -> " + e.getMessage());
            assertTrue(e.getMessage().contains(
                            "This operation is not supported for composite types."),
                    "expected PostgreSQL's detail line for: " + sql + " -> " + e.getMessage());
        }
        exec("SET search_path = public");
        // PostgreSQL names the bare relation even when the statement qualified it.
        assertError("42809", "cannot open relation \"tns_comp\"", "SELECT * FROM tns_a.tns_comp");
        assertError("42809", "\"tns_comp\" is not a table", "TRUNCATE tns_a.tns_comp");
    }

    /** What a composite is made of is ALTER TYPE's business, and ALTER TABLE says so. */
    @Test
    void alterTableOverACompositeTypePointsAtAlterType() throws Exception {
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        for (String sql : new String[]{
                "ALTER TABLE tns_a.tns_comp ADD COLUMN y int",
                "ALTER TABLE tns_a.tns_comp DROP COLUMN a",
                "ALTER TABLE tns_a.tns_comp RENAME TO tns_comp2"}) {
            SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
            assertEquals("42809", e.getSQLState(), sql + " -> " + e.getMessage());
            assertTrue(e.getMessage().contains("\"tns_comp\" is a composite type"),
                    sql + " -> " + e.getMessage());
            assertTrue(e.getMessage().contains("Use ALTER TYPE instead."),
                    "expected PostgreSQL's hint for: " + sql + " -> " + e.getMessage());
        }
        // The type still has exactly the attribute it was created with.
        assertEquals("1", scalar("SELECT count(*)::text FROM information_schema.attributes"
                + " WHERE udt_schema = 'tns_a' AND udt_name = 'tns_comp'"));
    }

    // ------------------------------------------ naming a type in a message

    /**
     * A message names the type the way a reader could have written it: bare when the search path
     * reaches its schema, schema-qualified when it does not. Without the qualifier the reader is
     * told about a type they cannot find.
     */
    @Test
    void aMessageQualifiesATypeTheSearchPathDoesNotReach() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE DOMAIN tns_a.tns_dom AS int CHECK (VALUE > 0)");
        exec("CREATE DOMAIN tns_a.tns_nn AS int NOT NULL");
        assertError("22P02", "invalid input value for enum tns_a.tns_enum: \"q\"",
                "SELECT 'q'::tns_a.tns_enum");
        assertError("23514", "value for domain tns_a.tns_dom violates check constraint"
                + " \"tns_dom_check\"", "SELECT (-1)::tns_a.tns_dom");
        assertError("23502", "domain tns_a.tns_nn does not allow null values",
                "SELECT NULL::tns_a.tns_nn");
        // The same values written into a column of those types are blamed the same way.
        exec("CREATE TABLE tns_b.tns_holder (e tns_a.tns_enum, d tns_a.tns_dom)");
        assertError("22P02", "invalid input value for enum tns_a.tns_enum: \"q\"",
                "INSERT INTO tns_b.tns_holder VALUES ('q', 1)");
        assertError("23514", "value for domain tns_a.tns_dom violates check constraint"
                + " \"tns_dom_check\"", "INSERT INTO tns_b.tns_holder VALUES ('x', -1)");
    }

    /** Put the schema on the path and the qualifier goes away, however the cast was spelled. */
    @Test
    void aMessageLeavesOffAQualifierTheSearchPathMakesUnnecessary() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE DOMAIN tns_a.tns_dom AS int CHECK (VALUE > 0)");
        exec("SET search_path = tns_a, public");
        try {
            assertError("22P02", "invalid input value for enum tns_enum: \"q\"",
                    "SELECT 'q'::tns_enum");
            assertError("22P02", "invalid input value for enum tns_enum: \"q\"",
                    "SELECT 'q'::tns_a.tns_enum");
            assertError("23514", "value for domain tns_dom violates check constraint",
                    "SELECT (-1)::tns_a.tns_dom");
        } finally {
            exec("SET search_path = public");
        }
    }

    // ---------------------------------------------- udt_schema names the type

    /**
     * {@code udt_schema} names the TYPE's schema, not the table's. Reading it off the table made
     * every user type look local, and a client resolving the name got a type that is not there.
     */
    @Test
    void udtSchemaReportsTheTypesSchemaNotTheTables() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE TYPE tns_a.tns_comp AS (a int)");
        exec("CREATE TABLE tns_b.tns_holder (e tns_a.tns_enum, c tns_a.tns_comp, i int)");
        assertEquals("tns_a", scalar("SELECT udt_schema FROM information_schema.columns"
                + " WHERE table_schema = 'tns_b' AND table_name = 'tns_holder'"
                + " AND column_name = 'e'"));
        assertEquals("tns_a", scalar("SELECT udt_schema FROM information_schema.columns"
                + " WHERE table_schema = 'tns_b' AND table_name = 'tns_holder'"
                + " AND column_name = 'c'"));
        assertEquals("pg_catalog", scalar("SELECT udt_schema FROM information_schema.columns"
                + " WHERE table_schema = 'tns_b' AND table_name = 'tns_holder'"
                + " AND column_name = 'i'"));
        assertEquals("tns_a", scalar("SELECT udt_schema FROM information_schema.column_udt_usage"
                + " WHERE table_schema = 'tns_b' AND table_name = 'tns_holder'"
                + " AND column_name = 'e'"));
        // And the other way round: a table in tns_a with a column of a type in public.
        exec("CREATE TYPE public.tns_pubenum AS ENUM ('y')");
        exec("CREATE TABLE tns_a.tns_holder2 (e tns_pubenum)");
        assertEquals("public", scalar("SELECT udt_schema FROM information_schema.columns"
                + " WHERE table_schema = 'tns_a' AND table_name = 'tns_holder2'"
                + " AND column_name = 'e'"));
        exec("DROP TABLE tns_a.tns_holder2");
        exec("DROP TYPE public.tns_pubenum");
    }

    /** An attribute's type has a schema of its own, which need not be the composite's. */
    @Test
    void anAttributesUdtSchemaIsItsOwnTypesSchema() throws Exception {
        exec("CREATE TYPE tns_a.tns_enum AS ENUM ('x')");
        exec("CREATE TYPE tns_b.tns_comp AS (a tns_a.tns_enum, b int)");
        assertEquals("tns_a", scalar("SELECT attribute_udt_schema FROM"
                + " information_schema.attributes WHERE udt_schema = 'tns_b'"
                + " AND udt_name = 'tns_comp' AND attribute_name = 'a'"));
        assertEquals("pg_catalog", scalar("SELECT attribute_udt_schema FROM"
                + " information_schema.attributes WHERE udt_schema = 'tns_b'"
                + " AND udt_name = 'tns_comp' AND attribute_name = 'b'"));
    }

    // ---------------------------------------- a type name with a dot in it

    /**
     * {@code "a.b"} is one quoted name, not a schema and an object. Reading its own name as a
     * qualifier made the type undroppable by any statement — it stayed in pg_type for good.
     */
    @Test
    void aQuotedTypeNameWithADotInItIsOneName() throws Exception {
        exec("CREATE DOMAIN \"tns_a.dotted\" AS int");
        assertEquals("1", scalar("SELECT count(*)::text FROM pg_type"
                + " WHERE typname = 'tns_a.dotted'"));
        // The schema it went into is the one a CREATE lands in, not the half before the dot.
        assertEquals("public", typeSchema("tns_a.dotted"));
        exec("DROP DOMAIN \"tns_a.dotted\"");
        assertEquals("0", scalar("SELECT count(*)::text FROM pg_type"
                + " WHERE typname = 'tns_a.dotted'"));
    }
}
