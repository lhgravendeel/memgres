package com.memgres.client;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Runs client introspection queries and checks for duplicate entries
 * that would cause a client to report "X duplicate reports".
 */
class ClientDuplicateDetectionTest {

    static Memgres memgres;
    static Connection conn;
    static int publicNsOid;
    static int pgCatalogNsOid;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(
                memgres.getJdbcUrl() + "?preferQueryMode=simple", memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE users (user_id serial PRIMARY KEY, username text)");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
            rs.next();
            publicNsOid = rs.getInt(1);
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'pg_catalog'")) {
            rs.next();
            pgCatalogNsOid = rs.getInt(1);
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    /** Check big UNION object listing query for duplicate (oid, kind) pairs */
    @Test
    void checkObjectListingDuplicates_public() throws SQLException {
        checkObjectListingDuplicates(publicNsOid, "public");
    }

    @Test
    void checkObjectListingDuplicates_pgCatalog() throws SQLException {
        checkObjectListingDuplicates(pgCatalogNsOid, "pg_catalog");
    }

    private void checkObjectListingDuplicates(int nsOid, String schemaName) throws SQLException {
        String sql = String.format("""
            select T.oid as oid,
                   relnamespace as schemaId,
                   pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind,
                   relname as name
            from pg_catalog.pg_class T
            where relnamespace in ( %d )
              and relkind in ('r', 'm', 'v', 'p', 'f', 'S')
            union all
            select T.oid,
                   T.typnamespace,
                   'T',
                   T.typname
            from pg_catalog.pg_type T
                 left outer join pg_catalog.pg_class C on T.typrelid = C.oid
            where T.typnamespace in ( %d )
              and ( T.typtype in ('d','e') or
                    C.relkind = 'c'::"char" or
                    (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or
                    T.typtype = 'p' and not T.typisdefined )
            union all
            select oid,
                   collnamespace,
                   'C',
                   collname
            from pg_catalog.pg_collation
            where collnamespace in ( %d )
            union all
            select oid,
                   oprnamespace,
                   'O',
                   oprname
            from pg_catalog.pg_operator
            where oprnamespace in ( %d )
            union all
            select oid,
                   opcnamespace,
                   'c',
                   opcname
            from pg_catalog.pg_opclass
            where opcnamespace in ( %d )
            union all
            select oid,
                   opfnamespace,
                   'F',
                   opfname
            from pg_catalog.pg_opfamily
            where opfnamespace in ( %d )
            union all
            select oid,
                   pronamespace,
                   case when prokind != 'a' then 'R' else 'a' end,
                   proname
            from pg_catalog.pg_proc
            where pronamespace in ( %d )
            order by schemaId
            """, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid, nsOid);

        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Map<String, List<String>> byOid = new LinkedHashMap<>();
            Map<String, List<String>> byName = new LinkedHashMap<>();
            while (rs.next()) {
                int oid = rs.getInt("oid");
                String kind = rs.getString("kind");
                String name = rs.getString("name");
                String key = oid + ":" + kind;
                byOid.computeIfAbsent(key, k -> new ArrayList<>()).add(name);
                String nameKey = name + ":" + kind;
                byName.computeIfAbsent(nameKey, k -> new ArrayList<>()).add(String.valueOf(oid));
            }
            // Check for duplicate OIDs with same kind
            for (var entry : byOid.entrySet()) {
                if (entry.getValue().size() > 1) {
                    fail("Duplicate OID+kind in " + schemaName + ": " + entry.getKey() + " -> " + entry.getValue());
                }
            }
            // Check for duplicate names with same kind (which a client might flag)
            // Note: pg_opclass ('c') and pg_opfamily ('F') have same-named entries for different AMs;
            // pg_proc ('R','a') legitimately has overloaded function names
            for (var entry : byName.entrySet()) {
                if (entry.getValue().size() > 1 && !entry.getKey().endsWith(":c") && !entry.getKey().endsWith(":F")
                        && !entry.getKey().endsWith(":R") && !entry.getKey().endsWith(":a")) {
                    fail("Duplicate name+kind in " + schemaName + ": " + entry.getKey() + " -> OIDs " + entry.getValue());
                }
            }
        }
    }

    /** Check table listing for duplicates */
    @Test
    void checkTableListingDuplicates() throws SQLException {
        String sql = String.format("""
            select T.relkind as table_kind, T.relname as table_name, T.oid as table_id
            from pg_catalog.pg_class T
            where relnamespace = %d
              and relkind in ('r', 'm', 'v', 'f', 'p')
            order by table_kind, table_id
            """, publicNsOid);
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Set<String> seen = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("table_name");
                if (!seen.add(name)) {
                    fail("Duplicate table name: " + name);
                }
            }
        }
    }

    /** Check column listing for duplicates */
    @Test
    void checkColumnListingDuplicates() throws SQLException {
        String sql = String.format("""
            with T as ( select distinct T.oid as table_id, T.relname as table_name
                        from pg_catalog.pg_class T, pg_catalog.pg_attribute A
                        where T.relnamespace = %d
                          and T.relkind in ('r', 'm', 'v', 'f', 'p')
                          and A.attrelid = T.oid )
            select T.table_id, T.table_name,
                   C.attnum as column_position,
                   C.attname as column_name
            from T
              join pg_catalog.pg_attribute C on T.table_id = C.attrelid
            where attnum > 0
            order by table_id, attnum
            """, publicNsOid);
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Map<Integer, Set<String>> colsByTable = new HashMap<>();
            while (rs.next()) {
                int tableId = rs.getInt("table_id");
                String colName = rs.getString("column_name");
                String tableName = rs.getString("table_name");
                Set<String> cols = colsByTable.computeIfAbsent(tableId, k -> new HashSet<>());
                if (!cols.add(colName)) {
                    fail("Duplicate column " + colName + " in table " + tableName);
                }
            }
        }
    }

    /** Check index listing for duplicates (with LEFT JOIN pg_opclass) */
    @Test
    void checkIndexListingDuplicates() throws SQLException {
        String sql = String.format("""
            select tab.oid table_id,
                   tab.relkind table_kind,
                   ind_stor.relname index_name,
                   ind_head.indexrelid index_id,
                   ind_head.indisunique is_unique,
                   ind_head.indisprimary is_primary,
                   opcmethod as access_method_id
            from pg_catalog.pg_class tab
                 join pg_catalog.pg_index ind_head on ind_head.indrelid = tab.oid
                 join pg_catalog.pg_class ind_stor
                      on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid
                 left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)
            where tab.relnamespace = %d
              and tab.relkind in ('r', 'm', 'v', 'p')
              and ind_stor.relkind in ('i', 'I')
            """, publicNsOid);
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Set<Integer> seenIds = new HashSet<>();
            Set<String> seenNames = new HashSet<>();
            while (rs.next()) {
                int indexId = rs.getInt("index_id");
                String indexName = rs.getString("index_name");
                if (!seenIds.add(indexId)) {
                    fail("Duplicate index_id: " + indexId + " (name: " + indexName + ")");
                }
                if (!seenNames.add(indexName)) {
                    fail("Duplicate index_name: " + indexName);
                }
            }
        }
    }

    /** Check constraint listing for duplicates */
    @Test
    void checkConstraintListingDuplicates() throws SQLException {
        String sql = String.format("""
            select T.oid table_id,
                   relkind table_kind,
                   C.oid::bigint con_id,
                   conname con_name,
                   contype con_kind
            from pg_catalog.pg_constraint C
                 join pg_catalog.pg_class T on C.conrelid = T.oid
            where relkind in ('r', 'v', 'f', 'p')
              and relnamespace = %d
              and contype in ('p', 'u', 'f', 'c', 'x')
              and connamespace = %d
            """, publicNsOid, publicNsOid);
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Set<String> seenNames = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("con_name");
                if (!seenNames.add(name)) {
                    fail("Duplicate constraint: " + name);
                }
            }
        }
    }

    /** Check sequence listing for duplicates */
    @Test
    void checkSequenceListingDuplicates() throws SQLException {
        String sql = String.format("""
            select cls.relname as sequence_name, sq.seqrelid as sequence_id
            from pg_catalog.pg_sequence sq
                 join pg_catalog.pg_class cls on sq.seqrelid = cls.oid
            where cls.relnamespace = %d
            """, publicNsOid);
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Set<String> seenNames = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("sequence_name");
                if (!seenNames.add(name)) {
                    fail("Duplicate sequence: " + name);
                }
            }
        }
    }

    /** Check pg_class directly for duplicate relname within a namespace */
    @Test
    void checkPgClassDuplicates_public() throws SQLException {
        checkPgClassDuplicates(publicNsOid, "public");
    }

    @Test
    void checkPgClassDuplicates_pgCatalog() throws SQLException {
        checkPgClassDuplicates(pgCatalogNsOid, "pg_catalog");
    }

    private void checkPgClassDuplicates(int nsOid, String schemaName) throws SQLException {
        String sql = "SELECT relname, relkind, oid FROM pg_class WHERE relnamespace = " + nsOid + " ORDER BY relname";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Map<String, List<String>> nameToKinds = new LinkedHashMap<>();
            while (rs.next()) {
                String name = rs.getString("relname");
                String kind = rs.getString("relkind");
                int oid = rs.getInt("oid");
                nameToKinds.computeIfAbsent(name, k -> new ArrayList<>()).add(kind + ":" + oid);
            }
            for (var entry : nameToKinds.entrySet()) {
                if (entry.getValue().size() > 1) {
                    fail("Duplicate relname in pg_class for " + schemaName + ": " + entry.getKey() + " -> " + entry.getValue());
                }
            }
        }
    }

    /** Check pg_type for duplicates within a namespace */
    @Test
    void checkPgTypeDuplicates_pgCatalog() throws SQLException {
        String sql = "SELECT typname, oid, typtype FROM pg_type WHERE typnamespace = " + pgCatalogNsOid + " ORDER BY typname";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Map<String, List<String>> nameToOids = new LinkedHashMap<>();
            while (rs.next()) {
                String name = rs.getString("typname");
                int oid = rs.getInt("oid");
                String typtype = rs.getString("typtype");
                nameToOids.computeIfAbsent(name, k -> new ArrayList<>()).add(oid + ":" + typtype);
            }
            for (var entry : nameToOids.entrySet()) {
                if (entry.getValue().size() > 1) {
                    fail("Duplicate typname in pg_type for pg_catalog: " + entry.getKey() + " -> " + entry.getValue());
                }
            }
        }
    }

    /** Check pg_proc for duplicate function names+argtypes (same signature) */
    @Test
    void checkPgProcDuplicates_pgCatalog() throws SQLException {
        String sql = "SELECT proname, oid FROM pg_proc WHERE pronamespace = " + pgCatalogNsOid + " ORDER BY proname";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            // pg_proc can have overloaded functions (same name, different args), so just check OID uniqueness
            Set<Integer> seenOids = new HashSet<>();
            while (rs.next()) {
                int oid = rs.getInt("oid");
                String name = rs.getString("proname");
                if (!seenOids.add(oid)) {
                    fail("Duplicate OID in pg_proc: " + oid + " (name: " + name + ")");
                }
            }
        }
    }

    /** Check namespace/schema listing for duplicates */
    @Test
    void checkNamespaceDuplicates() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT nspname, oid FROM pg_namespace ORDER BY nspname")) {
            Map<String, List<Integer>> nameToOids = new LinkedHashMap<>();
            while (rs.next()) {
                String name = rs.getString("nspname");
                int oid = rs.getInt("oid");
                nameToOids.computeIfAbsent(name, k -> new ArrayList<>()).add(oid);
            }
            for (var entry : nameToOids.entrySet()) {
                if (entry.getValue().size() > 1) {
                    fail("Duplicate namespace: " + entry.getKey() + " -> OIDs " + entry.getValue());
                }
            }
        }
    }

    /** Check pg_roles for duplicates */
    @Test
    void checkRolesDuplicates() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("SELECT rolname, oid FROM pg_roles ORDER BY rolname")) {
            Map<String, List<Integer>> nameToOids = new LinkedHashMap<>();
            while (rs.next()) {
                String name = rs.getString("rolname");
                int oid = rs.getInt("oid");
                nameToOids.computeIfAbsent(name, k -> new ArrayList<>()).add(oid);
            }
            for (var entry : nameToOids.entrySet()) {
                if (entry.getValue().size() > 1) {
                    fail("Duplicate role: " + entry.getKey() + " -> OIDs " + entry.getValue());
                }
            }
        }
    }

    /** Check pg_am JOIN pg_proc for duplicates (was previously broken) */
    @Test
    void checkAccessMethodDuplicates() throws SQLException {
        String sql = """
            select A.oid as access_method_id,
                   A.amname as access_method_name,
                   A.amhandler::oid as handler_id,
                   A.amtype as access_method_type
            from pg_am A
                 join pg_proc P on A.amhandler::oid = P.oid
                 join pg_namespace N on P.pronamespace = N.oid
            """;
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Set<String> seenNames = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("access_method_name");
                if (!seenNames.add(name)) {
                    fail("Duplicate access method: " + name);
                }
            }
        }
    }

    /** Check pg_extension for duplicates */
    @Test
    void checkExtensionDuplicates() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 select E.oid as id, extname as name, extversion as version
                 from pg_catalog.pg_extension E
                      join pg_namespace N on E.extnamespace = N.oid""")) {
            Set<String> seenNames = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("name");
                if (!seenNames.add(name)) {
                    fail("Duplicate extension: " + name);
                }
            }
        }
    }

    /** Check pg_language for duplicates */
    @Test
    void checkLanguageDuplicates() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 select l.oid as id, lanname as name
                 from pg_catalog.pg_language l
                 order by lanname""")) {
            Set<String> seenNames = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("name");
                if (!seenNames.add(name)) {
                    fail("Duplicate language: " + name);
                }
            }
        }
    }

    /** Check pg_tablespace for duplicates */
    @Test
    void checkTablespaceDuplicates() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 select T.oid::bigint as id, T.spcname as name
                 from pg_catalog.pg_tablespace T""")) {
            Set<String> seenNames = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("name");
                if (!seenNames.add(name)) {
                    fail("Duplicate tablespace: " + name);
                }
            }
        }
    }

    /** Check pg_database for duplicates */
    @Test
    void checkDatabaseDuplicates() throws SQLException {
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 select N.oid::bigint as id, datname as name
                 from pg_catalog.pg_database N""")) {
            Set<String> seenNames = new HashSet<>();
            while (rs.next()) {
                String name = rs.getString("name");
                if (!seenNames.add(name)) {
                    fail("Duplicate database: " + name);
                }
            }
        }
    }

    /** Test that LEFT JOIN with = ANY(array) doesn't multiply rows */
    @Test
    void testAnyArrayInJoinDoesNotMultiplyRows() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Create test tables
            s.execute("CREATE TABLE test_main (id int, arr int[])");
            s.execute("CREATE TABLE test_lookup (lid int, lname text)");
            s.execute("INSERT INTO test_main VALUES (1, '{10,10}')");  // duplicate in array
            s.execute("INSERT INTO test_main VALUES (2, '{20,30}')");  // two different values
            s.execute("INSERT INTO test_lookup VALUES (10, 'ten')");
            s.execute("INSERT INTO test_lookup VALUES (20, 'twenty')");
            s.execute("INSERT INTO test_lookup VALUES (30, 'thirty')");
        }
        try (Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery("""
                 SELECT m.id, l.lname
                 FROM test_main m
                 LEFT JOIN test_lookup l ON l.lid = ANY(m.arr)
                 ORDER BY m.id""")) {
            List<String> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(rs.getInt("id") + ":" + rs.getString("lname"));
            }
            // Row 1: arr={10,10}, lookup has lid=10 -> should produce 1 row (not 2!)
            // Row 2: arr={20,30}, lookup has lid=20 and lid=30 -> should produce 2 rows
            // In PostgreSQL: = ANY(array) in JOIN produces one row per matching lookup row
            // For arr={10,10}: only 1 lookup row matches (lid=10), so 1 output row
            // For arr={20,30}: 2 lookup rows match (lid=20, lid=30), so 2 output rows
            // Total: 3 rows
            System.out.println("ANY-JOIN rows: " + rows);
            assertEquals(3, rows.size(), "Expected 3 rows, got: " + rows);
        }
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE test_main");
            s.execute("DROP TABLE test_lookup");
        }
    }

    /** Check pg_cast for duplicate (castsource, casttarget) pairs */
    @Test
    void checkCastDuplicates() throws SQLException {
        String sql = "SELECT oid, castsource, casttarget, castcontext FROM pg_cast ORDER BY castsource, casttarget";
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Map<String, List<String>> pairToOids = new LinkedHashMap<>();
            while (rs.next()) {
                int src = rs.getInt("castsource");
                int tgt = rs.getInt("casttarget");
                int oid = rs.getInt("oid");
                String ctx = rs.getString("castcontext");
                String key = src + "->" + tgt;
                pairToOids.computeIfAbsent(key, k -> new ArrayList<>()).add(oid + ":" + ctx);
            }
            for (var entry : pairToOids.entrySet()) {
                if (entry.getValue().size() > 1) {
                    fail("Duplicate cast (castsource, casttarget) pair: " + entry.getKey() + " -> " + entry.getValue());
                }
            }
        }
    }

    /** Run exact client index query and check for row duplication */
    @Test
    void checkIndexQueryWithOpclassJoin() throws SQLException {
        // First populate pg_opclass to simulate what would happen with real data
        String sql = String.format("""
            select tab.oid table_id,
                   tab.relkind table_kind,
                   ind_stor.relname index_name,
                   ind_head.indexrelid index_id,
                   opcmethod as access_method_id
            from pg_catalog.pg_class tab
                 join pg_catalog.pg_index ind_head on ind_head.indrelid = tab.oid
                 join pg_catalog.pg_class ind_stor
                      on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid
                 left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass)
            where tab.relnamespace = %d
              and tab.relkind in ('r', 'm', 'v', 'p')
              and ind_stor.relkind in ('i', 'I')
            """, publicNsOid);
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            Map<Integer, Integer> indexIdCounts = new LinkedHashMap<>();
            while (rs.next()) {
                int indexId = rs.getInt("index_id");
                String indexName = rs.getString("index_name");
                indexIdCounts.merge(indexId, 1, Integer::sum);
                if (indexIdCounts.get(indexId) > 1) {
                    fail("Index query produced duplicate row for index: " + indexName + " (id=" + indexId + ")");
                }
            }
        }
    }
}
