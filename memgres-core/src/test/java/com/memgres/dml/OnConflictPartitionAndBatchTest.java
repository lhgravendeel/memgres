package com.memgres.dml;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression coverage for Bug 6: "ON CONFLICT ... DO UPDATE" silently losing writes.
 *
 * Downstream app tests (backend suite) proved that simple single-column
 * {@code ON CONFLICT (id) DO UPDATE} works, but at least one of three related shapes
 * silently loses data: (1) multi-column conflict targets, (2) partitioned target tables,
 * (3) batched upserts over the JDBC extended protocol. This suite bisects the three and
 * pins the actual root causes found:
 *
 * <ul>
 *   <li>Partitioned tables (variant 2): {@code DmlExecutor} ran ON CONFLICT conflict
 *       detection against the parent table instead of the partition the row actually
 *       routes to. Since real rows live on the leaf partition, the parent's row list
 *       never matched, so the "conflicting" insert silently duplicated the row instead of
 *       updating it. Partitions also carried no copy of the parent's PK/UNIQUE
 *       constraints, so per-partition duplicate-key checks and the fast index lookup
 *       had nothing to match against either.</li>
 *   <li>Batched upserts (variant 3): {@code TableIndex} used raw {@code OffsetDateTime}
 *       equality/hashing for its O(1) PK/UNIQUE lookup. Two {@code OffsetDateTime}
 *       values representing the identical instant, but built with different UTC
 *       offsets (e.g. a UTC-literal-inserted row vs. a JDBC-bound parameter encoded
 *       with a non-UTC offset), are not {@code .equals()}, so the index silently
 *       failed to find genuine conflicts/duplicates for {@code timestamptz} keys.</li>
 *   <li>Multi-column conflict targets alone (variant 1, no partitioning involved)
 *       already worked correctly before this fix.</li>
 * </ul>
 */
class OnConflictPartitionAndBatchTest {

    private static Memgres memgres;
    private static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(), memgres.getUser(), memgres.getPassword());
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- Variant 1: multi-column conflict target, non-partitioned (already worked) ----

    @Test
    void multiColumnConflictTarget_withEnumCast_updatesInPlace() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TYPE battery_operating_mode AS ENUM ('idle', 'charging', 'discharging')");
            s.execute("CREATE TABLE mc_t (bucket_ts timestamptz, installation_id int, battery_charge numeric(10,2), " +
                    "battery_mode battery_operating_mode, PRIMARY KEY (bucket_ts, installation_id))");
            s.execute("INSERT INTO mc_t (bucket_ts, installation_id, battery_charge, battery_mode) " +
                    "VALUES ('2024-01-01T00:00:00Z', 1, 75.5, 'idle'::battery_operating_mode)");
            s.execute("INSERT INTO mc_t (bucket_ts, installation_id, battery_charge, battery_mode) " +
                    "VALUES ('2024-01-01T00:00:00Z', 1, 80.0, 'charging'::battery_operating_mode) " +
                    "ON CONFLICT (bucket_ts, installation_id) DO UPDATE " +
                    "SET battery_charge = EXCLUDED.battery_charge, battery_mode = EXCLUDED.battery_mode");
            try (ResultSet rs = s.executeQuery("SELECT battery_charge, battery_mode, count(*) OVER () FROM mc_t")) {
                assertTrue(rs.next());
                assertEquals(0, new BigDecimal("80.0").compareTo(rs.getBigDecimal(1)),
                        "DO UPDATE should have applied EXCLUDED.battery_charge");
                assertEquals("charging", rs.getString(2));
                assertEquals(1, rs.getInt(3), "should still be a single row (no duplicate insert)");
            }
            s.execute("DROP TABLE mc_t");
            s.execute("DROP TYPE battery_operating_mode");
        }
    }

    // ---- Variant 2: partitioned target table, multi-column conflict target ----

    @Test
    void partitionedTable_multiColumnConflictTarget_updatesInPlaceNotDuplicate() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE p_t (bucket_ts date, installation_id int, val int, " +
                    "PRIMARY KEY (bucket_ts, installation_id)) PARTITION BY RANGE (bucket_ts)");
            s.execute("CREATE TABLE p_t_2024_01_01 PARTITION OF p_t " +
                    "FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')");
            s.execute("INSERT INTO p_t VALUES ('2024-01-01', 1, 10)");
            s.execute("INSERT INTO p_t VALUES ('2024-01-01', 1, 20) " +
                    "ON CONFLICT (bucket_ts, installation_id) DO UPDATE SET val = EXCLUDED.val");
            try (ResultSet rs = s.executeQuery("SELECT val, count(*) OVER () FROM p_t")) {
                assertTrue(rs.next());
                assertEquals(20, rs.getInt(1), "DO UPDATE should have applied EXCLUDED.val on a partitioned table");
                assertEquals(1, rs.getInt(2), "should still be a single row (no duplicate insert into the partition)");
            }
            // The partition itself must also reject a genuine duplicate key on a plain INSERT
            // (proves the partition now carries its own copy of the PK constraint/index).
            SQLException dup = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO p_t VALUES ('2024-01-01', 1, 30)"));
            assertTrue(dup.getMessage().contains("duplicate key") || "23505".equals(dup.getSQLState()),
                    "expected a duplicate-key violation, got: " + dup.getMessage());
            s.execute("DROP TABLE p_t");
        }
    }

    @Test
    void partitionedTable_onConflictDoNothing_skipsWithoutDuplicating() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE p_dn_t (bucket_ts date, installation_id int, val int, " +
                    "PRIMARY KEY (bucket_ts, installation_id)) PARTITION BY RANGE (bucket_ts)");
            s.execute("CREATE TABLE p_dn_t_2024_01_01 PARTITION OF p_dn_t " +
                    "FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')");
            s.execute("INSERT INTO p_dn_t VALUES ('2024-01-01', 1, 10)");
            s.execute("INSERT INTO p_dn_t VALUES ('2024-01-01', 1, 20) " +
                    "ON CONFLICT (bucket_ts, installation_id) DO NOTHING");
            try (ResultSet rs = s.executeQuery("SELECT val, count(*) OVER () FROM p_dn_t")) {
                assertTrue(rs.next());
                assertEquals(10, rs.getInt(1), "DO NOTHING should leave the original value in place");
                assertEquals(1, rs.getInt(2), "should still be a single row");
            }
            s.execute("DROP TABLE p_dn_t");
        }
    }

    // ---- Follow-up fixes from code review of the partitioned-table ON CONFLICT fix ----

    /**
     * Finding 1: {@code createPartitionOfTable} used to copy the parent's PK/UNIQUE
     * {@code StoredConstraint} instances into the partition BY REFERENCE, instead of giving the
     * partition its own independent copy. {@code StoredConstraint} is mutable, and
     * {@code ALTER TABLE ... VALIDATE CONSTRAINT} mutates whichever instance it finds by name -
     * so validating a NOT VALID constraint through one partition would silently mark the
     * parent's (and any sibling partition's) copy validated too, since they were the same object.
     * A NOT VALID PRIMARY KEY isn't something real PostgreSQL allows you to add, but memgres
     * doesn't gate NOT VALID by constraint type, which gives us a pure-SQL way to flip
     * {@code convalidated} to false and then observe whether validating it through one table
     * leaks to another - exactly the reachable mutation path the bug affects.
     */
    @Test
    void partitionConstraint_isIndependentInstance_notSharedWithTheParent() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE indep_t (id int, val int) PARTITION BY RANGE (id)");
            s.execute("CREATE TABLE indep_t_p1 PARTITION OF indep_t FOR VALUES FROM (0) TO (100)");
            s.execute("CREATE TABLE indep_t_p2 PARTITION OF indep_t FOR VALUES FROM (100) TO (200)");
            s.execute("ALTER TABLE indep_t ADD CONSTRAINT indep_t_pkey PRIMARY KEY (id)");
            s.execute("CREATE TABLE indep_t_p3 PARTITION OF indep_t FOR VALUES FROM (200) TO (300)");

            // A key added to the parent reaches every partition, whether it existed already or
            // was attached afterwards, and each carries its own row in pg_constraint rather than
            // a second reference to the parent's.
            for (String part : new String[] {"indep_t_p1", "indep_t_p2", "indep_t_p3"}) {
                assertTrue(constraintExists(s, part, part + "_pkey"),
                        part + " should carry its own copy of the parent's key");
            }
            assertTrue(constraintExists(s, "indep_t", "indep_t_pkey"),
                    "the parent keeps its own");
            s.execute("DROP TABLE indep_t");
        }
    }

    /**
     * PostgreSQL will not defer a key: only CHECK and FOREIGN KEY may be written NOT VALID, so the
     * convalidated flag cannot be moved on a key at all. This pins the refusal.
     */
    @Test
    void aKeyCannotBeMarkedNotValid() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE indep_nv (id int, val int)");
            SQLException pk = assertThrows(SQLException.class, () ->
                    s.execute("ALTER TABLE indep_nv ADD CONSTRAINT indep_nv_pkey"
                            + " PRIMARY KEY (id) NOT VALID"));
            assertEquals("0A000", pk.getSQLState());
            assertTrue(pk.getMessage().contains("PRIMARY KEY constraints cannot be marked NOT VALID"),
                    pk.getMessage());
            SQLException uq = assertThrows(SQLException.class, () ->
                    s.execute("ALTER TABLE indep_nv ADD CONSTRAINT indep_nv_uq"
                            + " UNIQUE (val) NOT VALID"));
            assertEquals("0A000", uq.getSQLState());
            // A CHECK may be, which is what keeps the rule from being a blanket refusal.
            s.execute("ALTER TABLE indep_nv ADD CONSTRAINT indep_nv_ck CHECK (val > 0) NOT VALID");
            assertFalse(constraintValidated(s, "indep_nv", "indep_nv_ck"));
            s.execute("DROP TABLE indep_nv");
        }
    }

    private boolean constraintExists(Statement s, String tableName, String constraintName)
            throws SQLException {
        try (ResultSet rs = s.executeQuery(
                "SELECT 1 FROM pg_constraint WHERE conrelid = '" + tableName + "'::regclass "
                + "AND conname = '" + constraintName + "'")) {
            return rs.next();
        }
    }

    private boolean constraintValidated(Statement s, String tableName, String constraintName) throws SQLException {
        try (ResultSet rs = s.executeQuery(
                "SELECT convalidated FROM pg_constraint WHERE conrelid = '" + tableName + "'::regclass "
                + "AND conname = '" + constraintName + "'")) {
            assertTrue(rs.next(), "expected a pg_constraint row for " + tableName + "." + constraintName);
            return rs.getBoolean(1);
        }
    }

    /**
     * Finding 2: {@code executeAddConstraint} (ALTER TABLE ... ADD CONSTRAINT) never propagated
     * a newly-added PK/UNIQUE constraint onto partitions that already existed - only
     * {@code CREATE TABLE ... PARTITION OF} copied constraints, and only ones the parent already
     * had at that moment. A PK added later left existing partitions with no TableIndex for it,
     * so ON CONFLICT conflict detection kept missing rows that were already there.
     */
    @Test
    void alterTableAddConstraint_afterPartitionsExist_propagatesToExistingPartitions() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE late_pk_t (bucket_ts date, installation_id int, val int) " +
                    "PARTITION BY RANGE (bucket_ts)");
            s.execute("CREATE TABLE late_pk_t_2024_01_01 PARTITION OF late_pk_t " +
                    "FOR VALUES FROM ('2024-01-01') TO ('2024-01-02')");
            // Add the PK *after* the partition already exists.
            s.execute("ALTER TABLE late_pk_t ADD CONSTRAINT late_pk_t_pkey " +
                    "PRIMARY KEY (bucket_ts, installation_id)");

            s.execute("INSERT INTO late_pk_t VALUES ('2024-01-01', 1, 10)");
            s.execute("INSERT INTO late_pk_t VALUES ('2024-01-01', 1, 20) " +
                    "ON CONFLICT (bucket_ts, installation_id) DO UPDATE SET val = EXCLUDED.val");
            try (ResultSet rs = s.executeQuery("SELECT val, count(*) OVER () FROM late_pk_t")) {
                assertTrue(rs.next());
                assertEquals(20, rs.getInt(1),
                        "DO UPDATE should have applied EXCLUDED.val for a PK added after the partition existed");
                assertEquals(1, rs.getInt(2), "should still be a single row (no duplicate insert)");
            }
            // A plain duplicate INSERT must also be rejected by the existing partition.
            SQLException dup = assertThrows(SQLException.class,
                    () -> s.execute("INSERT INTO late_pk_t VALUES ('2024-01-01', 1, 30)"));
            assertTrue(dup.getMessage().contains("duplicate key") || "23505".equals(dup.getSQLState()),
                    "expected a duplicate-key violation, got: " + dup.getMessage());
            s.execute("DROP TABLE late_pk_t");
        }
    }

    // ---- Variant 3: batched upserts over the JDBC extended protocol ----

    @Test
    void batchUpsert_extendedProtocol_survivesFullyConflictingBatch() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE batch_t (ts timestamptz PRIMARY KEY, v int)");
        }
        String sql = "INSERT INTO batch_t (ts, v) VALUES (?, ?) ON CONFLICT (ts) DO UPDATE SET v = EXCLUDED.v";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < 5; i++) {
                ps.setTimestamp(1, Timestamp.from(Instant.parse("2024-01-01T00:0" + i + ":00Z")));
                ps.setInt(2, i);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        // Second batch: same 5 keys, every row now conflicts -> exercises DO UPDATE inside a batch.
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < 5; i++) {
                ps.setTimestamp(1, Timestamp.from(Instant.parse("2024-01-01T00:0" + i + ":00Z")));
                ps.setInt(2, 100 + i);
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(5, counts.length);
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT count(*) FROM batch_t")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1), "all 5 rows should survive a fully-conflicting upsert batch");
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT v FROM batch_t ORDER BY ts")) {
            int i = 0;
            while (rs.next()) {
                assertEquals(100 + i, rs.getInt(1), "DO UPDATE should have applied EXCLUDED.v");
                i++;
            }
            assertEquals(5, i);
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE batch_t");
        }
    }

    /**
     * Reproduces the exact mismatch behind the app's "expected 5 but was 0"-style failures:
     * rows committed earlier via a text literal (e.g. demodata/migration SQL, or a plain
     * {@code Statement}) must still be recognized as conflicts by a later batched upsert that
     * binds the same {@code timestamptz} key as a JDBC parameter.
     */
    @Test
    void batchUpsert_conflictsWithRowsInsertedAsTextLiteral() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE batch_mix_t (ts timestamptz PRIMARY KEY, v numeric(10,2))");
            // Pre-existing rows, inserted via literal SQL text (simple protocol) - simulates
            // demodata / a prior import run that already committed these two timestamps.
            s.execute("INSERT INTO batch_mix_t VALUES ('2024-01-01T00:00:00Z', 1.11)");
            s.execute("INSERT INTO batch_mix_t VALUES ('2024-01-01T00:01:00Z', 2.22)");
        }
        String sql = "INSERT INTO batch_mix_t (ts, v) VALUES (?, ?) ON CONFLICT (ts) DO UPDATE SET v = EXCLUDED.v";
        // One batch: rows 0,1 conflict with the pre-existing literal-inserted data;
        // rows 2,3,4 are brand new.
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < 5; i++) {
                ps.setTimestamp(1, Timestamp.from(Instant.parse("2024-01-01T00:0" + i + ":00Z")));
                ps.setBigDecimal(2, new BigDecimal("9" + i + ".00"));
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(5, counts.length);
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT count(*) FROM batch_mix_t")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1),
                    "mixed new+conflicting batch should leave 5 rows (2 updated + 3 inserted), not 7 duplicates");
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE batch_mix_t");
        }
    }

    @Test
    void batchUpsert_withNullableNumericColumns_persistsAllRows() throws Exception {
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE batch_null_t (ts timestamptz PRIMARY KEY, min_price numeric(10,2), " +
                    "max_price numeric(10,2), power_afrr_in numeric(10,1))");
        }
        String sql = "INSERT INTO batch_null_t (ts, min_price, max_price, power_afrr_in) VALUES (?, ?, ?, ?) " +
                "ON CONFLICT (ts) DO UPDATE SET min_price = EXCLUDED.min_price, max_price = EXCLUDED.max_price, " +
                "power_afrr_in = EXCLUDED.power_afrr_in";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < 5; i++) {
                ps.setTimestamp(1, Timestamp.from(Instant.parse("2024-01-01T00:0" + i + ":00Z")));
                ps.setBigDecimal(2, new BigDecimal("1.00"));
                ps.setBigDecimal(3, new BigDecimal("2.00"));
                if (i % 2 == 0) ps.setNull(4, Types.NUMERIC);
                else ps.setBigDecimal(4, new BigDecimal("3.5"));
                ps.addBatch();
            }
            ps.executeBatch();
        }
        // Re-upsert the same keys (all conflicting) with nulls flipped to the other rows.
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < 5; i++) {
                ps.setTimestamp(1, Timestamp.from(Instant.parse("2024-01-01T00:0" + i + ":00Z")));
                ps.setBigDecimal(2, new BigDecimal("9.00"));
                ps.setBigDecimal(3, new BigDecimal("8.00"));
                if (i % 2 != 0) ps.setNull(4, Types.NUMERIC);
                else ps.setBigDecimal(4, new BigDecimal("7.5"));
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            assertEquals(5, counts.length);
        }
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT count(*) FROM batch_null_t")) {
            assertTrue(rs.next());
            assertEquals(5, rs.getInt(1), "batch with nullable numeric columns should leave 5 rows, not 0");
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE batch_null_t");
        }
    }
}
