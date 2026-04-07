package com.memgres.ddl;

import com.memgres.engine.util.IO;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * Collects PG 18 baseline results for the index SQL file comparison tests.
 *
 * Usage:
 *   mvn -pl memgres-core exec:java \
 *     -Dexec.mainClass="com.memgres.ddl.IndexSqlFileBaselineCollector" \
 *     -Dexec.classpathScope=test
 *
 * Requires PostgreSQL 18 on localhost:5432, database 'memgrestest',
 * user 'memgres', password 'memgres'.
 *
 * Outputs: memgres-core/src/test/resources/pg-baselines/index-sql-file-baseline.txt
 */
public class IndexSqlFileBaselineCollector {

    private static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest?preferQueryMode=simple";
    private static final String PG_USER = "memgres";
    private static final String PG_PASS = "memgres";

    /** All SQL test groups; must stay in sync with IndexSqlFileComparisonTest. */
    static final LinkedHashMap<String, String[]> TEST_GROUPS = new LinkedHashMap<>();
    static {
        TEST_GROUPS.put("1600", new String[]{
                "CREATE TABLE t_basic (id int, val int, extra text)",
                "INSERT INTO t_basic VALUES (1,10,'a'),(2,10,'b'),(3,NULL,'c')",
                "CREATE UNIQUE INDEX idx_basic_unique ON t_basic(id)",
                "CREATE INDEX idx_basic_val ON t_basic(val)",
                "CREATE INDEX idx_basic_include ON t_basic(val) INCLUDE (extra)",
                "SELECT COUNT(*) FROM t_basic"
        });
        TEST_GROUPS.put("1610", new String[]{
                "CREATE TABLE users (id int, email text, deleted_at timestamp)",
                "CREATE UNIQUE INDEX idx_users_email_active ON users(lower(email)) WHERE deleted_at IS NULL",
                "INSERT INTO users VALUES (1,'a@test.com',NULL),(2,'a@test.com','2024-01-01')",
                "SELECT COUNT(*) FROM users"
        });
        TEST_GROUPS.put("1620", new String[]{
                "CREATE TABLE expr_test (email text, payload jsonb)",
                "CREATE INDEX idx_lower_email ON expr_test(lower(email))",
                "CREATE INDEX idx_json_sku ON expr_test((payload->>'sku'))",
                "INSERT INTO expr_test VALUES ('Alice@test.com','{\"sku\":\"A1\"}'),('alice@test.com','{\"sku\":\"A2\"}')",
                "SELECT COUNT(*) FROM expr_test"
        });
        TEST_GROUPS.put("1630", new String[]{
                "CREATE TABLE bad_idx (id int, created_at timestamp)",
                "CREATE INDEX idx_bad ON bad_idx((random()))"
        });
        TEST_GROUPS.put("1640", new String[]{
                "CREATE TABLE num_test (val numeric, fval double precision)",
                "CREATE UNIQUE INDEX idx_num_unique ON num_test(val)",
                "INSERT INTO num_test VALUES (2.00, 1.0)",
                "INSERT INTO num_test VALUES (2.0, 2.0)",
                "INSERT INTO num_test VALUES (NULL, 'NaN')",
                "INSERT INTO num_test VALUES (NULL, 'Infinity')",
                "SELECT COUNT(*) FROM num_test"
        });
        TEST_GROUPS.put("1650", new String[]{
                "CREATE TABLE docs (id int, payload jsonb, tags text[], doc tsvector)",
                "CREATE INDEX idx_json ON docs USING gin(payload)",
                "CREATE INDEX idx_tags ON docs USING gin(tags)",
                "CREATE INDEX idx_doc ON docs USING gin(doc)",
                "INSERT INTO docs VALUES (1,'{\"a\":1}',ARRAY['x','y'],to_tsvector('hello world')),(2,'{\"b\":2}',ARRAY['y'],to_tsvector('world test'))",
                "SELECT id FROM docs WHERE payload @> '{\"a\":1}'"
        });
        TEST_GROUPS.put("1660", new String[]{
                "CREATE TABLE bookings (room int, period tsrange)",
                "CREATE EXTENSION IF NOT EXISTS btree_gist",
                "ALTER TABLE bookings ADD CONSTRAINT no_overlap EXCLUDE USING gist (room WITH =, period WITH &&)",
                "INSERT INTO bookings VALUES (1, tsrange('2024-01-01','2024-01-02'))",
                "INSERT INTO bookings VALUES (1, tsrange('2024-01-01 12:00','2024-01-03'))"
        });
        TEST_GROUPS.put("1670", new String[]{
                "CREATE TABLE texts (t text)",
                "CREATE INDEX idx_text_pattern ON texts(t text_pattern_ops)",
                "INSERT INTO texts VALUES ('abc'),('abcd'),('xyz')",
                "SELECT t FROM texts WHERE t LIKE 'abc%'"
        });
        TEST_GROUPS.put("1680", new String[]{
                "CREATE TABLE introspect (id int)",
                "CREATE INDEX idx_i ON introspect(id)",
                "SELECT indexname FROM pg_indexes WHERE tablename='introspect'"
        });
        TEST_GROUPS.put("1690", new String[]{
                "CREATE TABLE mix_idx (a int, b text, c numeric)",
                "CREATE INDEX idx_mix ON mix_idx(a,b,c)",
                "INSERT INTO mix_idx VALUES (1,'x',1.0),(1,'x',1.00),(2,NULL,NULL)",
                "SELECT COUNT(*) FROM mix_idx"
        });
        TEST_GROUPS.put("1700 (MD: strategy/correctness)", new String[]{
                "CREATE INDEX IF NOT EXISTS idx_mix ON mix_idx(a,b,c)",
                "DROP INDEX IF EXISTS nonexistent_index_xyz",
                "CREATE INDEX idx_mix ON mix_idx(a)",
                "CREATE INDEX idx_bad_col ON mix_idx(nonexistent_col)",
                "CREATE INDEX idx_bad_tbl ON nonexistent_table(id)",
                "UPDATE t_basic SET id = 99 WHERE id = 4",
                "INSERT INTO t_basic VALUES (99, 1, 'dup')",
                "UPDATE t_basic SET id = 4 WHERE id = 99",
                "DELETE FROM t_basic WHERE id = 3",
                "INSERT INTO t_basic VALUES (3, 30, 'reinserted')",
                "INSERT INTO t_basic VALUES (3, 31, 'dup')"
        });
        TEST_GROUPS.put("1710 (MD: capability matrix)", new String[]{
                "CREATE TABLE idx_cap_btree (id int, val text)",
                "CREATE INDEX idx_btree_default ON idx_cap_btree(id)",
                "CREATE INDEX idx_btree_explicit ON idx_cap_btree USING btree(val)",
                "INSERT INTO idx_cap_btree VALUES (1,'a'),(2,'b'),(3,'c')",
                "SELECT COUNT(*) FROM idx_cap_btree WHERE id > 1",
                "CREATE TABLE idx_cap_hash (id int, name text)",
                "CREATE INDEX idx_hash ON idx_cap_hash USING hash(name)",
                "INSERT INTO idx_cap_hash VALUES (1,'alice'),(2,'bob')",
                "SELECT id FROM idx_cap_hash WHERE name = 'alice'",
                "CREATE TABLE idx_cap_gin (id int, data jsonb)",
                "CREATE INDEX idx_gin_data ON idx_cap_gin USING gin(data)",
                "INSERT INTO idx_cap_gin VALUES (1,'{\"key\":\"val\"}'),(2,'{\"other\":1}')",
                "SELECT id FROM idx_cap_gin WHERE data @> '{\"key\":\"val\"}'",
                "CREATE TABLE idx_cap_gist (id int, r tsrange)",
                "CREATE INDEX idx_gist_r ON idx_cap_gist USING gist(r)",
                "INSERT INTO idx_cap_gist VALUES (1, tsrange('2024-01-01','2024-01-05'))",
                "INSERT INTO idx_cap_gist VALUES (2, tsrange('2024-02-01','2024-02-05'))",
                "SELECT id FROM idx_cap_gist WHERE r && tsrange('2024-01-03','2024-01-10') ORDER BY id",
                "CREATE TABLE idx_cap_brin (id int, ts timestamp)",
                "CREATE INDEX idx_brin_ts ON idx_cap_brin USING brin(ts)",
                "INSERT INTO idx_cap_brin VALUES (1, '2024-01-01'),(2, '2024-06-01')",
                "SELECT COUNT(*) FROM idx_cap_brin"
        });
        TEST_GROUPS.put("1720 (MD: concurrency/reindex)", new String[]{
                "CREATE TABLE conc_test (id int, val text)",
                "INSERT INTO conc_test VALUES (1,'a'),(2,'b'),(3,'c')",
                "CREATE INDEX CONCURRENTLY idx_conc ON conc_test(id)",
                "SELECT COUNT(*) FROM conc_test WHERE id = 2",
                "CREATE UNIQUE INDEX CONCURRENTLY idx_conc_unique ON conc_test(val)",
                "INSERT INTO conc_test VALUES (4,'a')",
                "CREATE TABLE reindex_test (id int PRIMARY KEY, val text)",
                "INSERT INTO reindex_test VALUES (1,'a'),(2,'b')",
                "REINDEX TABLE reindex_test",
                "SELECT COUNT(*) FROM reindex_test",
                "REINDEX INDEX reindex_test_pkey",
                "SELECT id FROM reindex_test WHERE id = 1",
                "DROP INDEX CONCURRENTLY idx_conc",
                "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'idx_conc'"
        });
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=== Index SQL File Baseline Collector ===\n");

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            conn.setAutoCommit(true);
            System.out.println("Connected to: " + conn.getMetaData().getDatabaseProductVersion());

            // Clean
            String[] tables = {"t_basic", "users", "expr_test", "bad_idx", "num_test",
                    "docs", "bookings", "texts", "introspect", "mix_idx",
                    "idx_cap_btree", "idx_cap_hash", "idx_cap_gin", "idx_cap_gist",
                    "idx_cap_brin", "conc_test", "reindex_test"};
            for (String t : tables) {
                try (Statement s = conn.createStatement()) { s.execute("DROP TABLE IF EXISTS " + t + " CASCADE"); }
            }
            try (Statement s = conn.createStatement()) { s.execute("DROP EXTENSION IF EXISTS btree_gist CASCADE"); }

            // Collect results
            StringBuilder sb = new StringBuilder();
            int totalStmts = 0;
            for (var entry : TEST_GROUPS.entrySet()) {
                sb.append("=== GROUP: ").append(entry.getKey()).append(" ===\n");
                for (String sql : entry.getValue()) {
                    totalStmts++;
                    sb.append("--- SQL: ").append(sql).append("\n");
                    try (Statement s = conn.createStatement()) {
                        boolean hasRs = s.execute(sql);
                        if (hasRs) {
                            ResultSet rs = s.getResultSet();
                            ResultSetMetaData md = rs.getMetaData();
                            int cc = md.getColumnCount();
                            List<String> cols = new ArrayList<>();
                            for (int i = 1; i <= cc; i++) cols.add(md.getColumnName(i));
                            sb.append("COLUMNS: ").append(String.join("|", cols)).append("\n");
                            List<String> rowStrs = new ArrayList<>();
                            while (rs.next()) {
                                List<String> vals = new ArrayList<>();
                                for (int i = 1; i <= cc; i++) {
                                    String v = rs.getString(i);
                                    vals.add(v == null ? "NULL" : v);
                                }
                                rowStrs.add(String.join("|", vals));
                            }
                            Collections.sort(rowStrs);
                            for (String row : rowStrs) sb.append("ROW: ").append(row).append("\n");
                            rs.close();
                        } else {
                            sb.append("OK: ").append(s.getUpdateCount()).append(" rows affected\n");
                        }
                    } catch (SQLException e) {
                        sb.append("ERROR: ").append(e.getSQLState()).append(": ").append(e.getMessage().split("\n")[0]).append("\n");
                    }
                }
            }

            // Write baseline
            Path outputDir = Path.of("memgres-core/src/test/resources/pg-baselines");
            Files.createDirectories(outputDir);
            Path outputPath = outputDir.resolve("index-sql-file-baseline.txt");
            IO.writeString(outputPath, sb.toString());
            System.out.println("\nBaseline saved to: " + outputPath);
            System.out.println("Total statements: " + totalStmts);
        }
    }
}
