package com.memgres.dml;

import com.memgres.engine.util.IO;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * Collects PG 18 baseline COPY output for CopyOutputComparisonTest.
 *
 * Usage:
 *   mvn -pl memgres-core test-compile exec:java \
 *     -Dexec.mainClass="com.memgres.dml.CopyOutputBaselineCollector" \
 *     -Dexec.classpathScope=test
 *
 * Outputs: memgres-core/src/test/resources/pg-baselines/copy-output-baseline.txt
 */
public class CopyOutputBaselineCollector {

    static final String PG_URL = "jdbc:postgresql://localhost:5432/memgrestest?preferQueryMode=simple";
    static final String PG_USER = "memgres";
    static final String PG_PASS = "memgres";

    public static void main(String[] args) throws Exception {
        System.out.println("=== COPY Output Baseline Collector ===\n");

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            conn.setAutoCommit(true);
            CopyManager cm = new CopyManager(conn.unwrap(BaseConnection.class));
            System.out.println("Connected to: " + conn.getMetaData().getDatabaseProductVersion());

            try (Statement s = conn.createStatement()) { s.execute("SET timezone = 'UTC'"); }

            // Clean tables
            String[] tables = {"copy_integers", "copy_floats", "copy_strings", "copy_temporal",
                    "copy_bool", "copy_uuid", "copy_json", "copy_arrays", "copy_bytea",
                    "copy_network", "copy_enum", "copy_mixed", "copy_nulls"};
            for (String t : tables) exec(conn, "DROP TABLE IF EXISTS " + t + " CASCADE");
            exec(conn, "DROP TYPE IF EXISTS test_mood CASCADE");

            StringBuilder sb = new StringBuilder();
            int entryCount = 0;

            // === 1: integers ===
            exec(conn, "CREATE TABLE copy_integers (a smallint, b integer, c bigint)");
            exec(conn, "INSERT INTO copy_integers VALUES (1, 100, 10000000000)");
            exec(conn, "INSERT INTO copy_integers VALUES (-32768, -2147483648, -9223372036854775808)");
            exec(conn, "INSERT INTO copy_integers VALUES (32767, 2147483647, 9223372036854775807)");
            exec(conn, "INSERT INTO copy_integers VALUES (0, 0, 0)");
            exec(conn, "INSERT INTO copy_integers VALUES (NULL, NULL, NULL)");
            entryCount += writeTextCopy(sb, cm, "integers TEXT", "COPY copy_integers TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "integers CSV", "COPY copy_integers TO STDOUT (FORMAT csv)");
            entryCount += writeTextCopy(sb, cm, "integers CSV+HEADER", "COPY copy_integers TO STDOUT (FORMAT csv, HEADER true)");

            // === 2: floats ===
            exec(conn, "CREATE TABLE copy_floats (a real, b double precision, c numeric(10,2), d numeric)");
            exec(conn, "INSERT INTO copy_floats VALUES (1.5, 2.5, 123.45, 99999999999999999.12345)");
            exec(conn, "INSERT INTO copy_floats VALUES (0.0, 0.0, 0.00, 0)");
            exec(conn, "INSERT INTO copy_floats VALUES (-1.5, -2.5, -123.45, -1)");
            exec(conn, "INSERT INTO copy_floats VALUES ('NaN'::real, 'NaN'::double precision, NULL, NULL)");
            exec(conn, "INSERT INTO copy_floats VALUES ('Infinity'::real, 'Infinity'::double precision, NULL, NULL)");
            exec(conn, "INSERT INTO copy_floats VALUES ('-Infinity'::real, '-Infinity'::double precision, NULL, NULL)");
            exec(conn, "INSERT INTO copy_floats VALUES (NULL, NULL, NULL, NULL)");
            entryCount += writeTextCopy(sb, cm, "floats TEXT", "COPY copy_floats TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "floats CSV", "COPY copy_floats TO STDOUT (FORMAT csv)");

            // === 3: strings ===
            exec(conn, "CREATE TABLE copy_strings (a text, b varchar(100), c char(5))");
            exec(conn, "INSERT INTO copy_strings VALUES ('hello', 'world', 'abc')");
            exec(conn, "INSERT INTO copy_strings VALUES ('', '', '     ')");
            exec(conn, "INSERT INTO copy_strings VALUES ('tab\there', 'new\nline', 'bk\\sl')");
            exec(conn, "INSERT INTO copy_strings VALUES ('single''quote', 'double\"quote', 'combo')");
            exec(conn, "INSERT INTO copy_strings VALUES (NULL, NULL, NULL)");
            exec(conn, "INSERT INTO copy_strings VALUES ('emoji: \uD83D\uDE00', 'CJK: \u4e16\u754c', 'ok   ')");
            entryCount += writeTextCopy(sb, cm, "strings TEXT", "COPY copy_strings TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "strings CSV", "COPY copy_strings TO STDOUT (FORMAT csv)");
            entryCount += writeTextCopy(sb, cm, "strings CSV custom quote", "COPY copy_strings TO STDOUT (FORMAT csv, QUOTE '''')");

            // === 4: temporal ===
            exec(conn, "CREATE TABLE copy_temporal (a date, b time, c timestamp, d timestamptz, e interval)");
            exec(conn, "INSERT INTO copy_temporal VALUES ('2024-01-15', '10:30:00', '2024-01-15 10:30:00', '2024-01-15 10:30:00+00', '1 year 2 months 3 days')");
            exec(conn, "INSERT INTO copy_temporal VALUES ('1970-01-01', '00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00+00', '0 seconds')");
            exec(conn, "INSERT INTO copy_temporal VALUES ('2099-12-31', '23:59:59.999999', '2099-12-31 23:59:59.999999', '2099-12-31 23:59:59.999999+00', '-1 day')");
            exec(conn, "INSERT INTO copy_temporal VALUES (NULL, NULL, NULL, NULL, NULL)");
            entryCount += writeTextCopy(sb, cm, "temporal TEXT", "COPY copy_temporal TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "temporal CSV", "COPY copy_temporal TO STDOUT (FORMAT csv)");

            // === 5: boolean ===
            exec(conn, "CREATE TABLE copy_bool (a boolean)");
            exec(conn, "INSERT INTO copy_bool VALUES (true)");
            exec(conn, "INSERT INTO copy_bool VALUES (false)");
            exec(conn, "INSERT INTO copy_bool VALUES (NULL)");
            entryCount += writeTextCopy(sb, cm, "bool TEXT", "COPY copy_bool TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "bool CSV", "COPY copy_bool TO STDOUT (FORMAT csv)");

            // === 6: uuid ===
            exec(conn, "CREATE TABLE copy_uuid (a uuid)");
            exec(conn, "INSERT INTO copy_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
            exec(conn, "INSERT INTO copy_uuid VALUES ('00000000-0000-0000-0000-000000000000')");
            exec(conn, "INSERT INTO copy_uuid VALUES (NULL)");
            entryCount += writeTextCopy(sb, cm, "uuid TEXT", "COPY copy_uuid TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "uuid CSV", "COPY copy_uuid TO STDOUT (FORMAT csv)");

            // === 7: json ===
            exec(conn, "CREATE TABLE copy_json (a json, b jsonb)");
            exec(conn, "INSERT INTO copy_json VALUES ('{\"key\": \"value\"}', '{\"key\": \"value\"}')");
            exec(conn, "INSERT INTO copy_json VALUES ('[]', '[]')");
            exec(conn, "INSERT INTO copy_json VALUES ('null', 'null')");
            exec(conn, "INSERT INTO copy_json VALUES ('{\"nested\": {\"arr\": [1,2,3]}}', '{\"nested\": {\"arr\": [1,2,3]}}')");
            exec(conn, "INSERT INTO copy_json VALUES (NULL, NULL)");
            entryCount += writeTextCopy(sb, cm, "json TEXT", "COPY copy_json TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "json CSV", "COPY copy_json TO STDOUT (FORMAT csv)");

            // === 8: arrays ===
            exec(conn, "CREATE TABLE copy_arrays (a int[], b text[], c boolean[])");
            exec(conn, "INSERT INTO copy_arrays VALUES ('{1,2,3}', '{hello,world}', '{t,f,t}')");
            exec(conn, "INSERT INTO copy_arrays VALUES ('{}', '{}', '{}')");
            exec(conn, "INSERT INTO copy_arrays VALUES ('{NULL,1,NULL}', '{NULL,\"has space\",NULL}', '{NULL}')");
            exec(conn, "INSERT INTO copy_arrays VALUES (NULL, NULL, NULL)");
            entryCount += writeTextCopy(sb, cm, "arrays TEXT", "COPY copy_arrays TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "arrays CSV", "COPY copy_arrays TO STDOUT (FORMAT csv)");

            // === 9: bytea ===
            exec(conn, "CREATE TABLE copy_bytea (a bytea)");
            exec(conn, "INSERT INTO copy_bytea VALUES ('\\x48656c6c6f')");
            exec(conn, "INSERT INTO copy_bytea VALUES ('\\x00ff01fe')");
            exec(conn, "INSERT INTO copy_bytea VALUES ('\\x')");
            exec(conn, "INSERT INTO copy_bytea VALUES (NULL)");
            entryCount += writeTextCopy(sb, cm, "bytea TEXT", "COPY copy_bytea TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "bytea CSV", "COPY copy_bytea TO STDOUT (FORMAT csv)");

            // === 10: network ===
            exec(conn, "CREATE TABLE copy_network (a inet, b cidr)");
            exec(conn, "INSERT INTO copy_network VALUES ('192.168.1.1', '10.0.0.0/8')");
            exec(conn, "INSERT INTO copy_network VALUES ('::1', '::ffff:0:0/96')");
            exec(conn, "INSERT INTO copy_network VALUES (NULL, NULL)");
            entryCount += writeTextCopy(sb, cm, "network TEXT", "COPY copy_network TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "network CSV", "COPY copy_network TO STDOUT (FORMAT csv)");

            // === 11: enum ===
            exec(conn, "CREATE TYPE test_mood AS ENUM ('happy', 'sad', 'neutral')");
            exec(conn, "CREATE TABLE copy_enum (a test_mood)");
            exec(conn, "INSERT INTO copy_enum VALUES ('happy')");
            exec(conn, "INSERT INTO copy_enum VALUES ('sad')");
            exec(conn, "INSERT INTO copy_enum VALUES (NULL)");
            entryCount += writeTextCopy(sb, cm, "enum TEXT", "COPY copy_enum TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "enum CSV", "COPY copy_enum TO STDOUT (FORMAT csv)");

            // === 12: mixed ===
            exec(conn, "CREATE TABLE copy_mixed (id serial PRIMARY KEY, name text NOT NULL, email varchar(255), score numeric(5,2), active boolean DEFAULT true, created_at timestamptz DEFAULT '2024-01-01 00:00:00+00', tags text[], metadata jsonb, uid uuid)");
            exec(conn, "INSERT INTO copy_mixed (name, email, score, active, tags, metadata, uid) VALUES ('Alice', 'alice@test.com', 95.5, true, '{admin,user}', '{\"role\": \"admin\"}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')");
            exec(conn, "INSERT INTO copy_mixed (name, email, score, active, tags, metadata, uid) VALUES ('Bob', NULL, NULL, false, '{}', 'null', NULL)");
            exec(conn, "INSERT INTO copy_mixed (name, email, score, active, tags, metadata, uid) VALUES ('Carol', 'carol@test.com', 0.00, true, NULL, NULL, NULL)");
            entryCount += writeTextCopy(sb, cm, "mixed TEXT", "COPY copy_mixed TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "mixed CSV", "COPY copy_mixed TO STDOUT (FORMAT csv)");
            entryCount += writeTextCopy(sb, cm, "mixed CSV+HEADER", "COPY copy_mixed TO STDOUT (FORMAT csv, HEADER true)");

            // === 13: nulls ===
            exec(conn, "CREATE TABLE copy_nulls (a text, b int, c boolean, d jsonb, e text[])");
            exec(conn, "INSERT INTO copy_nulls VALUES (NULL, NULL, NULL, NULL, NULL)");
            exec(conn, "INSERT INTO copy_nulls VALUES ('', 0, false, 'null', '{}')");
            exec(conn, "INSERT INTO copy_nulls VALUES ('\\N', 0, false, '\"\\\\N\"', '{\"\\\\N\"}')");
            entryCount += writeTextCopy(sb, cm, "nulls TEXT default", "COPY copy_nulls TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "nulls TEXT custom NULL", "COPY copy_nulls TO STDOUT (NULL 'NULL')");
            entryCount += writeTextCopy(sb, cm, "nulls CSV", "COPY copy_nulls TO STDOUT (FORMAT csv)");
            entryCount += writeTextCopy(sb, cm, "nulls CSV custom NULL", "COPY copy_nulls TO STDOUT (FORMAT csv, NULL 'NULL')");

            // === 14-16: options ===
            entryCount += writeTextCopy(sb, cm, "integers pipe delimiter", "COPY copy_integers TO STDOUT (DELIMITER '|')");
            entryCount += writeTextCopy(sb, cm, "strings FORCE_QUOTE *", "COPY copy_strings TO STDOUT (FORMAT csv, FORCE_QUOTE *)");
            entryCount += writeTextCopy(sb, cm, "strings FORCE_QUOTE (a)", "COPY copy_strings TO STDOUT (FORMAT csv, FORCE_QUOTE (a))");
            entryCount += writeTextCopy(sb, cm, "integers HEADER", "COPY copy_integers TO STDOUT (FORMAT csv, HEADER true)");
            entryCount += writeTextCopy(sb, cm, "strings TEXT HEADER", "COPY copy_strings TO STDOUT (HEADER true)");

            // === 17: subquery ===
            entryCount += writeTextCopy(sb, cm, "subquery TEXT", "COPY (SELECT a, b FROM copy_integers WHERE a IS NOT NULL ORDER BY a) TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "subquery CSV", "COPY (SELECT a, b FROM copy_integers WHERE a IS NOT NULL ORDER BY a) TO STDOUT (FORMAT csv)");

            // === 18: column list ===
            entryCount += writeTextCopy(sb, cm, "column list TEXT", "COPY copy_mixed (name, score) TO STDOUT");
            entryCount += writeTextCopy(sb, cm, "column list CSV", "COPY copy_mixed (name, score) TO STDOUT (FORMAT csv)");

            // === 20-23: binary ===
            entryCount += writeBinaryCopy(sb, cm, "integers BINARY", "COPY copy_integers TO STDOUT (FORMAT binary)");
            entryCount += writeBinaryCopy(sb, cm, "strings BINARY", "COPY copy_strings TO STDOUT (FORMAT binary)");
            entryCount += writeBinaryCopy(sb, cm, "bool BINARY", "COPY copy_bool TO STDOUT (FORMAT binary)");
            entryCount += writeBinaryCopy(sb, cm, "mixed BINARY", "COPY copy_mixed TO STDOUT (FORMAT binary)");

            // Write file
            Path outputDir = Path.of("memgres-core/src/test/resources/pg-baselines");
            Files.createDirectories(outputDir);
            Path outputPath = outputDir.resolve("copy-output-baseline.txt");
            IO.writeString(outputPath, sb.toString());
            System.out.println("\nBaseline saved to: " + outputPath);
            System.out.println("Total COPY entries: " + entryCount);
        }
    }

    static void exec(Connection conn, String sql) throws SQLException {
        try (Statement s = conn.createStatement()) { s.execute(sql); }
    }

    static int writeTextCopy(StringBuilder sb, CopyManager cm, String label, String copyCmd) throws Exception {
        StringWriter sw = new StringWriter();
        cm.copyOut(copyCmd, sw);
        String output = sw.toString();
        sb.append("=== COPY: ").append(label).append(" ===\n");
        sb.append("CMD: ").append(copyCmd).append("\n");
        sb.append("TEXT_OUTPUT_START\n");
        sb.append(output);
        if (!output.endsWith("\n")) sb.append("\n");
        sb.append("TEXT_OUTPUT_END\n");
        return 1;
    }

    static int writeBinaryCopy(StringBuilder sb, CopyManager cm, String label, String copyCmd) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        cm.copyOut(copyCmd, baos);
        byte[] bytes = baos.toByteArray();
        sb.append("=== COPY: ").append(label).append(" ===\n");
        sb.append("CMD: ").append(copyCmd).append("\n");
        sb.append("BINARY_OUTPUT_START\n");
        sb.append(Base64.getEncoder().encodeToString(bytes)).append("\n");
        sb.append("BINARY_OUTPUT_END\n");
        return 1;
    }
}
