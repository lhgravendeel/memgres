package com.memgres.client;
import com.memgres.core.Memgres;
import java.sql.*;
public class QuickKTest {
    public static void main(String[] args) throws Exception {
        Memgres m = Memgres.builder().port(0).build().start();
        Connection c = DriverManager.getConnection(m.getJdbcUrl(), m.getUser(), m.getPassword());

        // Get public nsOid
        int nsOid;
        ResultSet oidRs = c.createStatement().executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'");
        oidRs.next();
        nsOid = oidRs.getInt(1);
        oidRs.close();
        System.out.println("public nsOid = " + nsOid);

        // Test: Exact Q49 via PreparedStatement (no tables created, empty database)
        String sql = "select ind_head.indexrelid index_id,\n" +
                "       k col_idx,\n" +
                "       k <= indnkeyatts in_key,\n" +
                "       ind_head.indkey[k-1] column_position,\n" +
                "       ind_head.indoption[k-1] column_options,\n" +
                "       ind_head.indcollation[k-1] as collation,\n" +
                "       colln.nspname as collation_schema,\n" +
                "       collname as collation_str,\n" +
                "       ind_head.indclass[k-1] as opclass,\n" +
                "       case when opcdefault then null else opcn.nspname end as opclass_schema,\n" +
                "       case when opcdefault then null else opcname end as opclass_str,\n" +
                "       case\n" +
                "           when indexprs is null then null\n" +
                "           when ind_head.indkey[k-1] = 0 then chr(27) || pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)\n" +
                "           else pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true)\n" +
                "       end as expression,\n" +
                "       amcanorder can_order\n" +
                "from pg_catalog.pg_index ind_head\n" +
                "         join pg_catalog.pg_class ind_stor\n" +
                "              on ind_stor.oid = ind_head.indexrelid\n" +
                "cross join unnest(ind_head.indkey) with ordinality u(u, k)\n" +
                "left join pg_catalog.pg_collation\n" +
                "on pg_collation.oid = ind_head.indcollation[k-1]\n" +
                "left join pg_catalog.pg_namespace colln on collnamespace = colln.oid\n" +
                "cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder\n" +
                "         left join pg_catalog.pg_opclass\n" +
                "                   on pg_opclass.oid = ind_head.indclass[k-1]\n" +
                "         left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid\n" +
                "where ind_stor.relnamespace = ?::oid\n" +
                "  and ind_stor.relkind in ('i', 'I')\n" +
                "order by index_id, k";

        try {
            PreparedStatement ps = c.prepareStatement(sql);
            ps.setInt(1, nsOid);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            System.out.println("Q49 PreparedStatement OK, " + meta.getColumnCount() + " columns:");
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.println("  col " + i + ": " + meta.getColumnLabel(i));
            }
            int rows = 0;
            while (rs.next()) rows++;
            System.out.println("  " + rows + " rows");
            rs.close();
            ps.close();
        } catch (Exception e) {
            System.out.println("Q49 PreparedStatement FAIL: " + e.getMessage());
        }

        m.close();
    }
}
