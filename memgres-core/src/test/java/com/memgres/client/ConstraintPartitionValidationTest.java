package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A foreign key whose referenced columns are not unique, or whose two sides hold values that can
 * never compare equal, cannot enforce anything. Accepting the definition is worse than refusing
 * it: the constraint appears in the catalog and a reader of the schema concludes the data is
 * protected while nothing is checking it. The same reasoning covers a partition attached with
 * bounds no row could satisfy, and a constraint altered with attributes its kind does not accept.
 */
class ConstraintPartitionValidationTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        exec("CREATE TABLE dcp_pk (ptest1 int, ptest2 text, primary key(ptest1, ptest2))");
        exec("CREATE TABLE dcp_fk (ftest1 int, ftest2 text)");
        exec("CREATE TABLE dcp_pk1 (id int primary key, u int unique, plain int)");
        exec("CREATE TABLE dcp_nopk (a int, b text)");
        exec("CREATE VIEW dcp_v AS SELECT id FROM dcp_pk1");
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

    /** Asserts the statement fails with the given SQLSTATE and message, as PostgreSQL words it. */
    private static SQLException assertFails(String sqlState, String message, String sql) {
        SQLException e = assertThrows(SQLException.class, () -> exec(sql), sql);
        assertEquals(sqlState, e.getSQLState(), "wrong SQLSTATE for: " + sql + " -> " + e.getMessage());
        assertTrue(e.getMessage().contains(message),
                "expected \"" + message + "\" in: " + e.getMessage());
        return e;
    }

    // ---- the referenced key ----

    @Test
    void aKeyMustReferenceAUniqueOrPrimaryKey() {
        assertFails("42830", "there is no unique constraint matching given keys for referenced table \"dcp_pk\"",
                "ALTER TABLE dcp_fk ADD FOREIGN KEY (ftest1) REFERENCES dcp_pk(ptest2)");
        assertFails("42830", "there is no unique constraint matching given keys for referenced table \"dcp_pk1\"",
                "CREATE TABLE dcp_c1 (a int REFERENCES dcp_pk1(plain))");
        assertFails("42830", "there is no unique constraint matching given keys for referenced table \"dcp_pk1\"",
                "CREATE TABLE dcp_c2 (a int, FOREIGN KEY (a) REFERENCES dcp_pk1(plain))");
        // a partial key of a composite primary key is not itself unique
        assertFails("42830", "there is no unique constraint matching given keys",
                "ALTER TABLE dcp_fk ADD FOREIGN KEY (ftest1, ftest2) REFERENCES dcp_pk(ptest1)");
    }

    @Test
    void aSelfReferenceIsCheckedAgainstTheTableBeingCreated() throws Exception {
        assertFails("42830", "there is no unique constraint matching given keys for referenced table \"dcp_self2\"",
                "CREATE TABLE dcp_self2 (a int primary key, b int REFERENCES dcp_self2(b))");
        assertFails("42830", "there is no unique constraint matching given keys for referenced table \"dcp_self3\"",
                "CREATE TABLE dcp_self3 (a int, b int, FOREIGN KEY (b) REFERENCES dcp_self3(a))");
        exec("CREATE TABLE dcp_self1 (a int primary key, b int REFERENCES dcp_self1)");
        exec("DROP TABLE dcp_self1");
    }

    @Test
    void aUniqueConstraintIsEnoughAndTheTableIsNotLeftBehind() throws Exception {
        exec("CREATE TABLE dcp_ok1 (a int REFERENCES dcp_pk1(u))");
        exec("DROP TABLE dcp_ok1");
        // a rejected CREATE TABLE leaves no trace
        assertFails("42830", "no unique constraint", "CREATE TABLE dcp_gone (a int REFERENCES dcp_pk1(plain))");
        assertEquals("0", scalar("SELECT count(*) FROM pg_class WHERE relname = 'dcp_gone'"));
    }

    // ---- column lists ----

    @Test
    void theTwoColumnListsMustAgree() {
        assertFails("42830", "number of referencing and referenced columns for foreign key disagree",
                "CREATE TABLE dcp_c3 (a int, b text, FOREIGN KEY (a, b) REFERENCES dcp_pk1(id))");
        assertFails("42830", "number of referencing and referenced columns for foreign key disagree",
                "CREATE TABLE dcp_c4 (a int, FOREIGN KEY (a) REFERENCES dcp_pk(ptest1, ptest2))");
    }

    @Test
    void theReferencedListMustNotRepeatAColumn() {
        assertFails("42830", "foreign key referenced-columns list must not contain duplicates",
                "CREATE TABLE dcp_c5 (a int, b text, FOREIGN KEY (a, b) REFERENCES dcp_pk(ptest1, ptest1))");
    }

    @Test
    void everyKeyColumnMustExist() {
        assertFails("42703", "column \"nosuchcol\" referenced in foreign key constraint does not exist",
                "CREATE TABLE dcp_c7 (a int, FOREIGN KEY (a) REFERENCES dcp_pk1(nosuchcol))");
        assertFails("42703", "column \"nosuchcol\" referenced in foreign key constraint does not exist",
                "CREATE TABLE dcp_c8 (a int REFERENCES dcp_pk1(nosuchcol))");
        assertFails("42703", "column \"nosuchcol\" referenced in foreign key constraint does not exist",
                "CREATE TABLE dcp_c9 (a int, FOREIGN KEY (nosuchcol) REFERENCES dcp_pk1(id))");
        assertFails("42703", "column \"nosuchcol\" referenced in foreign key constraint does not exist",
                "ALTER TABLE dcp_fk ADD FOREIGN KEY (nosuchcol) REFERENCES dcp_pk1(id)");
    }

    @Test
    void aSystemColumnCannotBeAKeyColumn() {
        assertFails("0A000", "system columns cannot be used in foreign keys",
                "CREATE TABLE dcp_c10 (a int, FOREIGN KEY (tableoid) REFERENCES dcp_pk1(id))");
        assertFails("0A000", "system columns cannot be used in foreign keys",
                "ALTER TABLE dcp_fk ADD FOREIGN KEY (ftest1) REFERENCES dcp_pk1(tableoid)");
    }

    // ---- the referenced relation ----

    @Test
    void theReferencedRelationMustExistBeATableAndHaveAKey() {
        assertFails("42704", "there is no primary key for referenced table \"dcp_nopk\"",
                "CREATE TABLE dcp_c11 (a int REFERENCES dcp_nopk)");
        assertFails("42704", "there is no primary key for referenced table \"dcp_nopk\"",
                "CREATE TABLE dcp_c12 (a int, FOREIGN KEY (a) REFERENCES dcp_nopk)");
        assertFails("42P01", "relation \"dcp_nosuchtable\" does not exist",
                "CREATE TABLE dcp_c13 (a int REFERENCES dcp_nosuchtable)");
        assertFails("42P01", "relation \"dcp_nosuchtable\" does not exist",
                "CREATE TABLE dcp_c14 (a int, FOREIGN KEY (a) REFERENCES dcp_nosuchtable(x))");
        assertFails("42809", "referenced relation \"dcp_v\" is not a table",
                "CREATE TABLE dcp_c15 (a int REFERENCES dcp_v)");
        assertFails("42809", "referenced relation \"dcp_v\" is not a table",
                "CREATE TABLE dcp_c16 (a int, FOREIGN KEY (a) REFERENCES dcp_v(id))");
    }

    // ---- comparable types ----

    @Test
    void keyColumnsMustHoldComparableValues() {
        SQLException e = assertFails("42804", "foreign key constraint \"dcp_c17_a_fkey\" cannot be implemented",
                "CREATE TABLE dcp_c17 (a inet REFERENCES dcp_pk1)");
        assertTrue(e.getMessage().contains(
                        "Key columns \"a\" of the referencing table and \"id\" of the referenced table"
                                + " are of incompatible types: inet and integer."),
                "missing the incompatible-types detail: " + e.getMessage());
        assertFails("42804", "cannot be implemented",
                "CREATE TABLE dcp_c19 (a cidr, b timestamp, FOREIGN KEY (a, b) REFERENCES dcp_pk(ptest1, ptest2))");
        assertFails("42804", "cannot be implemented",
                "CREATE TABLE dcp_c20 (a text, b int, FOREIGN KEY (a, b) REFERENCES dcp_pk(ptest1, ptest2))");
        // a swapped-order composite key reports the first pair that cannot compare
        assertFails("42804", "foreign key constraint \"dcp_c6_a_a_fkey\" cannot be implemented",
                "CREATE TABLE dcp_c6 (a int, b text, FOREIGN KEY (a, a) REFERENCES dcp_pk(ptest1, ptest2))");
    }

    @Test
    void widerAndImplicitlyCastableTypesStillBuildAKey() throws Exception {
        exec("CREATE TABLE dcp_bigpk (id bigint primary key)");
        exec("CREATE TABLE dcp_ok2 (a int REFERENCES dcp_bigpk)");
        exec("DROP TABLE dcp_ok2");
        exec("CREATE TABLE dcp_vpk (id varchar(10) primary key)");
        exec("CREATE TABLE dcp_ok3 (a text REFERENCES dcp_vpk)");
        exec("DROP TABLE dcp_ok3");
    }

    // ---- MATCH PARTIAL and the SET action column list ----

    @Test
    void matchPartialIsRefusedButMatchFullIsNot() throws Exception {
        assertFails("0A000", "MATCH PARTIAL not yet implemented",
                "CREATE TABLE dcp_c21 (a int, b text, FOREIGN KEY (a, b) REFERENCES dcp_pk MATCH PARTIAL)");
        assertFails("0A000", "MATCH PARTIAL not yet implemented",
                "ALTER TABLE dcp_fk ADD FOREIGN KEY (ftest1, ftest2) REFERENCES dcp_pk MATCH PARTIAL");
        exec("CREATE TABLE dcp_ok4 (a int, b text, FOREIGN KEY (a, b) REFERENCES dcp_pk MATCH FULL)");
        exec("DROP TABLE dcp_ok4");
    }

    @Test
    void theSetActionColumnListIsChecked() throws Exception {
        assertFails("42703", "column \"bar\" referenced in foreign key constraint does not exist",
                "CREATE TABLE dcp_c22 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET NULL (bar))");
        assertFails("42P10", "column \"b\" referenced in ON DELETE SET action must be part of foreign key",
                "CREATE TABLE dcp_c23 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET NULL (b))");
        assertFails("0A000", "a column list with SET NULL is only supported for ON DELETE actions",
                "CREATE TABLE dcp_c24 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON UPDATE SET NULL (a))");
        assertFails("0A000", "a column list with SET DEFAULT is only supported for ON DELETE actions",
                "CREATE TABLE dcp_c25 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON UPDATE SET DEFAULT (a))");
        assertFails("42703", "column \"bar\" referenced in foreign key constraint does not exist",
                "CREATE TABLE dcp_c26 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET DEFAULT (bar))");
        assertFails("42P10", "column \"b\" referenced in ON DELETE SET action must be part of foreign key",
                "CREATE TABLE dcp_c27 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET DEFAULT (b))");
        exec("CREATE TABLE dcp_ok5 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET NULL (a))");
        exec("DROP TABLE dcp_ok5");
        exec("CREATE TABLE dcp_ok6 (a int, b int, FOREIGN KEY (a) REFERENCES dcp_pk1(id) ON DELETE SET DEFAULT (a))");
        exec("DROP TABLE dcp_ok6");
    }

    /**
     * The value ON DELETE SET DEFAULT writes is an ordinary default: nothing guarantees the
     * referenced table holds it, so the key has to be checked again afterwards.
     */
    @Test
    void aSetDefaultThatLeavesNoParentIsRejected() throws Exception {
        exec("CREATE TABLE dcp_sdp (id int primary key)");
        exec("INSERT INTO dcp_sdp VALUES (1)");
        exec("CREATE TABLE dcp_sdc (a int default 42 REFERENCES dcp_sdp ON DELETE SET DEFAULT)");
        exec("INSERT INTO dcp_sdc VALUES (1)");
        SQLException e = assertFails("23503",
                "insert or update on table \"dcp_sdc\" violates foreign key constraint \"dcp_sdc_a_fkey\"",
                "DELETE FROM dcp_sdp WHERE id = 1");
        assertTrue(e.getMessage().contains("Key (a)=(42) is not present in table \"dcp_sdp\"."),
                "missing the missing-key detail: " + e.getMessage());
        assertEquals("1", scalar("SELECT a FROM dcp_sdc"));
        exec("DROP TABLE dcp_sdc");
        exec("DROP TABLE dcp_sdp");
    }

    // ---- TRUNCATE ----

    @Test
    void truncateAcceptsBothHalvesOfAReferencePair() throws Exception {
        exec("CREATE TABLE dcp_tp (id int primary key)");
        exec("CREATE TABLE dcp_tc (id int primary key, p int REFERENCES dcp_tp)");
        exec("INSERT INTO dcp_tp VALUES (1)");
        exec("INSERT INTO dcp_tc VALUES (1, 1)");
        // the parent alone is still refused
        assertFails("0A000", "cannot truncate a table referenced in a foreign key constraint",
                "TRUNCATE dcp_tp");
        exec("TRUNCATE dcp_tp, dcp_tc");
        assertEquals("0", scalar("SELECT count(*) FROM dcp_tp"));
        assertEquals("0", scalar("SELECT count(*) FROM dcp_tc"));
        // the order the two are named in does not matter
        exec("INSERT INTO dcp_tp VALUES (2)");
        exec("INSERT INTO dcp_tc VALUES (2, 2)");
        exec("TRUNCATE dcp_tc, dcp_tp");
        // the referencing table alone was always allowed, and CASCADE still is
        exec("TRUNCATE dcp_tc");
        exec("INSERT INTO dcp_tp VALUES (3)");
        exec("INSERT INTO dcp_tc VALUES (3, 3)");
        exec("TRUNCATE dcp_tp CASCADE");
        assertEquals("0", scalar("SELECT count(*) FROM dcp_tc"));
        // a self-referencing table is the whole graph by itself
        exec("CREATE TABLE dcp_tself (id int primary key, p int REFERENCES dcp_tself)");
        exec("TRUNCATE dcp_tself");
    }

    // ---- ALTER CONSTRAINT ----

    @Nested
    class AlterConstraint {

        @BeforeEach
        void createTable() throws Exception {
            exec("DROP TABLE IF EXISTS dcp_ac");
            exec("DROP TABLE IF EXISTS dcp_ac_p");
            exec("CREATE TABLE dcp_ac_p (id int primary key)");
            exec("CREATE TABLE dcp_ac (id int primary key, q int, p int,"
                    + " CONSTRAINT dcp_ac_fk FOREIGN KEY (p) REFERENCES dcp_ac_p,"
                    + " CONSTRAINT dcp_ac_ck CHECK (id > 0),"
                    + " CONSTRAINT dcp_ac_uq UNIQUE (q))");
        }

        @Test
        void selfContradictoryAttributesAreRefused() {
            assertFails("42601", "constraint declared INITIALLY DEFERRED must be DEFERRABLE",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NOT DEFERRABLE INITIALLY DEFERRED");
            assertFails("42601", "conflicting constraint properties",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk ENFORCED NOT ENFORCED");
            assertFails("0A000", "constraints cannot be altered to be NOT VALID",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NOT VALID");
        }

        @Test
        void eachAttributeBelongsToOneConstraintKind() {
            assertFails("42809", "constraint \"dcp_ac_fk\" of relation \"dcp_ac\" is not a not-null constraint",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NO INHERIT");
            assertFails("42809", "constraint \"dcp_ac_fk\" of relation \"dcp_ac\" is not a not-null constraint",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk INHERIT");
            assertFails("42809", "constraint \"dcp_ac_ck\" of relation \"dcp_ac\" is not a foreign key constraint",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_ck DEFERRABLE");
            assertFails("42809", "constraint \"dcp_ac_uq\" of relation \"dcp_ac\" is not a foreign key constraint",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_uq DEFERRABLE");
            assertFails("42809", "constraint \"dcp_ac_pkey\" of relation \"dcp_ac\" is not a foreign key constraint",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_pkey DEFERRABLE");
            assertFails("42809", "cannot alter enforceability of constraint \"dcp_ac_ck\" of relation \"dcp_ac\"",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_ck NOT ENFORCED");
            assertFails("42704", "constraint \"dcp_nosuch\" of relation \"dcp_ac\" does not exist",
                    "ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_nosuch DEFERRABLE");
        }

        @Test
        void aForeignKeyTakesTheDeferrabilityItIsGiven() throws Exception {
            exec("ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk DEFERRABLE INITIALLY DEFERRED");
            assertEquals("t", deferrable());
            assertEquals("t", deferred());
            exec("ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NOT DEFERRABLE");
            assertEquals("f", deferrable());
            assertEquals("f", deferred());
            // INITIALLY DEFERRED on its own implies DEFERRABLE
            exec("ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk INITIALLY DEFERRED");
            assertEquals("t", deferrable());
            assertEquals("t", deferred());
            exec("ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk NOT ENFORCED");
            exec("ALTER TABLE dcp_ac ALTER CONSTRAINT dcp_ac_fk ENFORCED");
        }

        private String deferrable() throws SQLException {
            return scalar("SELECT condeferrable FROM pg_constraint"
                    + " WHERE conrelid = 'dcp_ac'::regclass AND conname = 'dcp_ac_fk'");
        }

        private String deferred() throws SQLException {
            return scalar("SELECT condeferred FROM pg_constraint"
                    + " WHERE conrelid = 'dcp_ac'::regclass AND conname = 'dcp_ac_fk'");
        }
    }

    // ---- ATTACH / DETACH PARTITION ----

    @Nested
    class PartitionAttachment {

        @BeforeEach
        void createTables() throws Exception {
            exec("DROP TABLE IF EXISTS dcp_att");
            exec("DROP TABLE IF EXISTS dcp_rp CASCADE");
            exec("DROP TABLE IF EXISTS dcp_lp CASCADE");
            exec("DROP TABLE IF EXISTS dcp_hp CASCADE");
            exec("CREATE TABLE dcp_rp (a int, b text) PARTITION BY RANGE (a)");
            exec("CREATE TABLE dcp_rp1 PARTITION OF dcp_rp FOR VALUES FROM (0) TO (10)");
            exec("CREATE TABLE dcp_att (a int, b text)");
        }

        @Test
        void aRangeThatNoRowCouldSatisfyIsRefused() {
            SQLException e = assertFails("42P17", "empty range bound specified for partition \"dcp_att\"",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (30) TO (20)");
            assertTrue(e.getMessage().contains(
                            "Specified lower bound (30) is greater than or equal to upper bound (20)."),
                    "missing the bound detail: " + e.getMessage());
            assertFails("42P17", "empty range bound specified for partition \"dcp_rev\"",
                    "CREATE TABLE dcp_rev PARTITION OF dcp_rp FOR VALUES FROM (30) TO (20)");
            assertFails("42P17", "empty range bound specified for partition \"dcp_eq\"",
                    "CREATE TABLE dcp_eq PARTITION OF dcp_rp FOR VALUES FROM (30) TO (30)");
        }

        @Test
        void theBoundMustMatchTheParentsStrategy() throws Exception {
            assertFails("42P16", "invalid bound specification for a range partition",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES IN (100)");
            assertFails("42P16", "invalid bound specification for a range partition",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES WITH (MODULUS 4, REMAINDER 0)");
            exec("CREATE TABLE dcp_lp (a int, b text) PARTITION BY LIST (a)");
            assertFails("42P16", "invalid bound specification for a list partition",
                    "ALTER TABLE dcp_lp ATTACH PARTITION dcp_att FOR VALUES FROM (0) TO (10)");
            exec("CREATE TABLE dcp_hp (a int, b text) PARTITION BY HASH (a)");
            assertFails("42P16", "invalid bound specification for a hash partition",
                    "ALTER TABLE dcp_hp ATTACH PARTITION dcp_att FOR VALUES IN (1)");
            assertFails("42601", "syntax error at end of input",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_att");
        }

        @Test
        void theBoundMustHaveTheKeysArityAndType() {
            assertFails("42P16", "FROM must specify exactly one value per partitioning column",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (1, 2) TO (5, 6)");
            assertFails("22P02", "invalid input syntax for type integer: \"abc\"",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM ('abc') TO ('def')");
        }

        @Test
        void boundsMayNotOverlapAnExistingPartition() {
            assertFails("42P17", "partition \"dcp_att\" would overlap partition \"dcp_rp1\"",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (5) TO (15)");
            assertFails("42P17", "partition \"dcp_ov\" would overlap partition \"dcp_rp1\"",
                    "CREATE TABLE dcp_ov PARTITION OF dcp_rp FOR VALUES FROM (5) TO (15)");
        }

        @Test
        void onlyAPartitionedTableAttachesAndOnlyATableIsAttached() throws Exception {
            exec("CREATE TABLE dcp_plain (a int, b text)");
            exec("CREATE TABLE dcp_plain2 (a int, b text)");
            SQLException e = assertFails("42809",
                    "ALTER action ATTACH PARTITION cannot be performed on relation \"dcp_plain\"",
                    "ALTER TABLE dcp_plain ATTACH PARTITION dcp_plain2 FOR VALUES FROM (0) TO (10)");
            assertTrue(e.getMessage().contains("This operation is not supported for tables."),
                    "missing the relation-kind detail: " + e.getMessage());
            assertFails("42809", "ALTER action DETACH PARTITION cannot be performed on relation \"dcp_plain\"",
                    "ALTER TABLE dcp_plain DETACH PARTITION dcp_plain2");
            assertFails("42809", "ALTER action ATTACH PARTITION cannot be performed on relation \"dcp_v\"",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_v FOR VALUES FROM (20) TO (30)");
            exec("DROP TABLE dcp_plain2");
            exec("DROP TABLE dcp_plain");
        }

        @Test
        void aTableBelongsToOneParentOnly() throws Exception {
            assertFails("42809", "\"dcp_rp1\" is already a partition",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_rp1 FOR VALUES FROM (20) TO (30)");
            exec("CREATE TABLE dcp_rp2 (a int, b text) PARTITION BY RANGE (a)");
            assertFails("42809", "\"dcp_rp1\" is already a partition",
                    "ALTER TABLE dcp_rp2 ATTACH PARTITION dcp_rp1 FOR VALUES FROM (20) TO (30)");
            exec("DROP TABLE dcp_rp2");
            SQLException e = assertFails("42P07", "circular inheritance not allowed",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_rp FOR VALUES FROM (20) TO (30)");
            assertTrue(e.getMessage().contains("\"dcp_rp\" is already a child of \"dcp_rp\"."),
                    "missing the circularity detail: " + e.getMessage());
        }

        @Test
        void detachingSomethingThatIsNotAPartitionIsRefused() throws Exception {
            assertFails("42P01", "relation \"dcp_att\" is not a partition of relation \"dcp_rp\"",
                    "ALTER TABLE dcp_rp DETACH PARTITION dcp_att");
            assertFails("42P01", "relation \"dcp_nosuch\" does not exist",
                    "ALTER TABLE dcp_rp DETACH PARTITION dcp_nosuch");
        }

        @Test
        void aSecondDefaultPartitionIsRefused() throws Exception {
            exec("CREATE TABLE dcp_d1 PARTITION OF dcp_rp DEFAULT");
            assertFails("42P17", "partition \"dcp_d2\" conflicts with existing default partition \"dcp_d1\"",
                    "CREATE TABLE dcp_d2 PARTITION OF dcp_rp DEFAULT");
            assertFails("42P17", "partition \"dcp_att\" conflicts with existing default partition \"dcp_d1\"",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_att DEFAULT");
        }

        @Test
        void aRowTheDefaultHoldsBlocksAPartitionThatWouldClaimIt() throws Exception {
            exec("CREATE TABLE dcp_d1 PARTITION OF dcp_rp DEFAULT");
            exec("INSERT INTO dcp_rp VALUES (500, 'x')");
            assertFails("23514",
                    "updated partition constraint for default partition \"dcp_d1\" would be violated by some row",
                    "ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (400) TO (600)");
            // a range the default holds no row in still attaches
            exec("ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (600) TO (700)");
            exec("ALTER TABLE dcp_rp DETACH PARTITION dcp_att");
        }

        @Test
        void aValidAttachDetachRoundTripStillWorks() throws Exception {
            exec("INSERT INTO dcp_att VALUES (25, 'x')");
            exec("ALTER TABLE dcp_rp ATTACH PARTITION dcp_att FOR VALUES FROM (20) TO (30)");
            exec("INSERT INTO dcp_rp VALUES (5, 'lo'), (22, 'hi')");
            assertEquals("3", scalar("SELECT count(*) FROM dcp_rp"));
            exec("ALTER TABLE dcp_rp DETACH PARTITION dcp_att");
            assertEquals("1", scalar("SELECT count(*) FROM dcp_rp"));
            assertEquals("2", scalar("SELECT count(*) FROM dcp_att"));
        }
    }
}
