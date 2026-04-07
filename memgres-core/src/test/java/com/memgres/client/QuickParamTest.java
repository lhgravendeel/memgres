package com.memgres.client;

import com.memgres.core.Memgres;

import java.sql.*;

/**
 * Focused diagnostic for 'No results were returned' errors with extended protocol clients errors.
 * Prints exact column metadata (type OID, type name) and row data for the two failing queries.
 */
public class QuickParamTest {
    public static void main(String[] args) throws Exception {
        Memgres.logAllStatements = true;
        Memgres m = Memgres.builder().port(0).build().start();
        Connection c = DriverManager.getConnection(m.getJdbcUrl(), m.getUser(), m.getPassword());

        System.out.println("\n========== Q0: pg_type lookup for aclitem ==========");
        runAndDump(c, "SELECT oid, typname, typelem, typarray, typcategory, typlen FROM pg_type WHERE typname IN ('aclitem', '_aclitem', 'aclitem[]') OR oid IN (1033, 1034) ORDER BY oid");

        System.out.println("\n========== Q1: Extension Query ==========");
        runAndDump(c, """
                select E.oid        as id,
                       E.xmin       as state_number,
                       extname      as name,
                       extversion   as version,
                       extnamespace as schema_id,
                       nspname      as schema_name,
                       array(select unnest
                             from unnest(available_versions)
                             where unnest > extversion) as available_updates
                from pg_catalog.pg_extension E
                       join pg_namespace N on E.extnamespace = N.oid
                       left join (select name, array_agg(version) as available_versions
                                  from pg_available_extension_versions()
                                  group by name) V on E.extname = V.name""");

        System.out.println("\n========== Q2: ACL Union (fdw/lang/ns/srv) ==========");
        runAndDump(c, """
                select T.oid as object_id,
                       T.fdwacl as acl
                from pg_catalog.pg_foreign_data_wrapper T
                union all
                select T.oid as object_id,
                       T.lanacl as acl
                from pg_catalog.pg_language T
                union all
                select T.oid as object_id,
                       T.nspacl as acl
                from pg_catalog.pg_namespace T
                union all
                select T.oid as object_id,
                       T.srvacl as acl
                from pg_catalog.pg_foreign_server T""");

        System.out.println("\n========== Q3: ACL Union (spcacl/datacl) - FIXED ==========");
        runAndDump(c, """
                select T.oid as object_id,
                       T.spcacl as acl
                from pg_catalog.pg_tablespace T
                union all
                select T.oid as object_id,
                       T.datacl as acl
                from pg_catalog.pg_database T""");

        c.close();
        m.close();
    }

    private static void runAndDump(Connection c, String sql) {
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();
            System.out.println("Column count: " + colCount);
            for (int i = 1; i <= colCount; i++) {
                System.out.printf("  col[%d] name=%-25s typeName=%-15s typeId=%-6d className=%s%n",
                        i, meta.getColumnName(i), meta.getColumnTypeName(i),
                        meta.getColumnType(i), meta.getColumnClassName(i));
            }
            int row = 0;
            while (rs.next()) {
                row++;
                StringBuilder sb = new StringBuilder("  row " + row + ": ");
                for (int i = 1; i <= colCount; i++) {
                    Object val = rs.getObject(i);
                    String repr = val == null ? "NULL" : val.getClass().getSimpleName() + "=" + val;
                    sb.append(meta.getColumnName(i)).append("=").append(repr).append("  ");
                }
                System.out.println(sb);
            }
            System.out.println("Total rows: " + row);
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            e.printStackTrace(System.out);
        }
    }
}
