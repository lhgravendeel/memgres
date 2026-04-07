package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checks for duplicate OIDs and duplicate (name+kind) pairs in the big UNION
 * query that a database client uses for object listing, run against both pg_catalog
 * and public schemas.
 */
class ClientDuplicateCheckTest {

    static Memgres memgres;
    static Connection conn;
    static int publicNsOid;
    static int pgCatalogNsOid;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple",
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);

        // Create a users table with serial primary key
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE users (user_id serial PRIMARY KEY, username text)");
        }

        // Get pg_catalog namespace OID
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'")) {
            assertTrue(rs.next(), "pg_catalog namespace must exist");
            pgCatalogNsOid = rs.getInt(1);
            System.out.println("pg_catalog namespace OID: " + pgCatalogNsOid);
        }

        // Get public namespace OID
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
            assertTrue(rs.next(), "public namespace must exist");
            publicNsOid = rs.getInt(1);
            System.out.println("public namespace OID: " + publicNsOid);
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /**
     * The exact big UNION query a database client uses for object listing,
     * parameterized by namespace OID.
     */
    private static String bigUnionQuery(int nsOid) {
        return String.format("""
            select T.oid as oid,
                   relnamespace as schemaId,
                   pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind,
                   relname as name
            from pg_catalog.pg_class T
            where relnamespace = %1$d
              and relkind in ('r', 'm', 'v', 'p', 'f', 'S')
            union all
            select T.oid,
                   T.typnamespace,
                   'T',
                   T.typname
            from pg_catalog.pg_type T
                 left outer join pg_catalog.pg_class C on T.typrelid = C.oid
            where T.typnamespace = %1$d
              and ( T.typtype in ('d','e')
                    or C.relkind = 'c'::"char"
                    or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A'))
                    or T.typtype = 'p' and not T.typisdefined )
            union all
            select oid,
                   collnamespace,
                   'C',
                   collname
            from pg_catalog.pg_collation
            where collnamespace = %1$d
            union all
            select oid,
                   oprnamespace,
                   'O',
                   oprname
            from pg_catalog.pg_operator
            where oprnamespace = %1$d
            union all
            select oid,
                   opcnamespace,
                   'c',
                   opcname
            from pg_catalog.pg_opclass
            where opcnamespace = %1$d
            union all
            select oid,
                   opfnamespace,
                   'F',
                   opfname
            from pg_catalog.pg_opfamily
            where opfnamespace = %1$d
            union all
            select oid,
                   pronamespace,
                   case when prokind != 'a' then 'R' else 'a' end,
                   proname
            from pg_catalog.pg_proc
            where pronamespace = %1$d
            """, nsOid);
    }

    @Test
    void checkDuplicates_pgCatalog() throws SQLException {
        runDuplicateCheck(pgCatalogNsOid, "pg_catalog");
    }

    @Test
    void checkDuplicates_public() throws SQLException {
        runDuplicateCheck(publicNsOid, "public");
    }

    private void runDuplicateCheck(int nsOid, String schemaName) throws SQLException {
        String sql = bigUnionQuery(nsOid);

        // Collect all rows
        List<String[]> allRows = new ArrayList<>(); // [oid, kind, name]
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            while (rs.next()) {
                String oid = rs.getString("oid");
                String kind = rs.getString("kind");
                String name = rs.getString("name");
                allRows.add(new String[]{oid, kind, name});
            }
        }
        System.out.println("=== Schema: " + schemaName + " (nsOid=" + nsOid + ") ===");
        System.out.println("Total rows returned: " + allRows.size());

        // --- Check 1: Duplicate OIDs ---
        Map<String, List<String>> byOid = new LinkedHashMap<>();
        for (String[] row : allRows) {
            String oid = row[0];
            String kind = row[1];
            String name = row[2];
            byOid.computeIfAbsent(oid, k -> new ArrayList<>())
                    .add("kind=" + kind + ", name=" + name);
        }

        List<String> oidDuplicates = new ArrayList<>();
        for (var entry : byOid.entrySet()) {
            if (entry.getValue().size() > 1) {
                oidDuplicates.add("  OID " + entry.getKey() + " appears " +
                        entry.getValue().size() + " times: " + entry.getValue());
            }
        }
        if (!oidDuplicates.isEmpty()) {
            System.out.println("DUPLICATE OIDs found (" + oidDuplicates.size() + "):");
            oidDuplicates.forEach(System.out::println);
        } else {
            System.out.println("No duplicate OIDs found.");
        }

        // --- Check 2: Duplicate (name + kind) pairs ---
        Map<String, List<String>> byNameKind = new LinkedHashMap<>();
        for (String[] row : allRows) {
            String oid = row[0];
            String kind = row[1];
            String name = row[2];
            String key = name + " [kind=" + kind + "]";
            byNameKind.computeIfAbsent(key, k -> new ArrayList<>()).add("oid=" + oid);
        }

        List<String> nameKindDuplicates = new ArrayList<>();
        for (var entry : byNameKind.entrySet()) {
            if (entry.getValue().size() > 1) {
                nameKindDuplicates.add("  " + entry.getKey() + " appears " +
                        entry.getValue().size() + " times: " + entry.getValue());
            }
        }
        if (!nameKindDuplicates.isEmpty()) {
            System.out.println("DUPLICATE (name+kind) pairs found (" + nameKindDuplicates.size() + "):");
            nameKindDuplicates.forEach(System.out::println);
        } else {
            System.out.println("No duplicate (name+kind) pairs found.");
        }

        System.out.println();

        // --- Fail assertions ---
        // OID duplicates are always a bug
        assertTrue(oidDuplicates.isEmpty(),
                "Found " + oidDuplicates.size() + " duplicate OIDs in " + schemaName + ":\n" +
                        String.join("\n", oidDuplicates));

        // Name+kind duplicates: exclude 'c' (opclass) and 'F' (opfamily) which can
        // legitimately have same-named entries for different access methods, and 'O' (operators)
        // which are overloaded by argument types, and 'R' (functions) which are overloaded
        List<String> realNameDuplicates = new ArrayList<>();
        for (var entry : byNameKind.entrySet()) {
            if (entry.getValue().size() > 1) {
                String key = entry.getKey();
                // These kinds legitimately have same-name entries
                if (key.contains("[kind=c]") || key.contains("[kind=F]") ||
                        key.contains("[kind=O]") || key.contains("[kind=R]")) {
                    continue;
                }
                realNameDuplicates.add("  " + key + " -> " + entry.getValue());
            }
        }
        assertTrue(realNameDuplicates.isEmpty(),
                "Found " + realNameDuplicates.size() +
                        " unexpected duplicate (name+kind) pairs in " + schemaName + ":\n" +
                        String.join("\n", realNameDuplicates));
    }
}
