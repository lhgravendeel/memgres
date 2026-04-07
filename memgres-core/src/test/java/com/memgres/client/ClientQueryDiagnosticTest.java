package com.memgres.client;

import com.memgres.core.Memgres;

import java.sql.*;
import java.util.*;

/**
 * Runs all database client introspection queries against Memgres to find which ones fail.
 * Uses JDBC extended query mode (default), with no preferQueryMode=simple.
 */
public class ClientQueryDiagnosticTest {
    public static void main(String[] args) throws Exception {
        Memgres.logAllStatements = false;
        Memgres m = Memgres.builder().port(0).build().start();
        // Extended query mode (JDBC default)
        Connection c = DriverManager.getConnection(m.getJdbcUrl(), m.getUser(), m.getPassword());

        int pass = 0, fail = 0;

        // Get the public namespace OID
        int publicNsOid;
        try (Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT oid FROM pg_namespace WHERE nspname = 'public'")) {
            rs.next();
            publicNsOid = rs.getInt(1);
        }
        System.out.println("public namespace OID = " + publicNsOid);

        // ============================================================
        // Phase 1: Non-parameterized queries
        // ============================================================
        String[][] phase1 = {
            {"Q01-set-float", "SET extra_float_digits = 3"},
            {"Q02-set-appname", "SET application_name = 'test_client'"},
            {"Q03-version", "select version()"},
            {"Q04-current-db-schemas", "select current_database() as a, current_schemas(false) as b"},
            {"Q05-current-all", "select current_database(), current_schema(), current_user"},
            {"Q06-startup-time", "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time"},
            {"Q07-locks-txid", "select L.transactionid::varchar::bigint as transaction_id from pg_catalog.pg_locks L where L.transactionid is not null order by pg_catalog.age(L.transactionid) desc limit 1"},
            {"Q08-current-txid", "select case when pg_catalog.pg_is_in_recovery() then null else (pg_catalog.txid_current() % 4294967296)::varchar::bigint end as current_txid"},
            {"Q09-databases", "select N.oid::bigint as id, datname as name, D.description, datistemplate as is_template, datallowconn as allow_connections, pg_catalog.pg_get_userbyid(N.datdba) as \"owner\" from pg_catalog.pg_database N left join pg_catalog.pg_shdescription D on N.oid = D.objoid order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end"},
            {"Q10-datestyle", "show DateStyle"},
            {"Q11-timezones", "select name, is_dst from pg_catalog.pg_timezone_names union distinct select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs"},
            {"Q12-roles", "select R.oid::bigint as role_id, rolname as role_name, rolsuper is_super, rolinherit is_inherit, rolcreaterole can_createrole, rolcreatedb can_createdb, rolcanlogin can_login, rolreplication is_replication, rolconnlimit conn_limit, rolvaliduntil valid_until, rolbypassrls bypass_rls, rolconfig config, D.description from pg_catalog.pg_roles R left join pg_catalog.pg_shdescription D on D.objoid = R.oid"},
            {"Q13-auth-members", "select member id, roleid role_id, admin_option from pg_catalog.pg_auth_members order by id, roleid::text"},
            {"Q14-tablespaces", "select T.oid::bigint as id, T.spcname as name, T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner, pg_catalog.pg_tablespace_location(T.oid) as location, T.spcoptions as options, D.description as comment from pg_catalog.pg_tablespace T left join pg_catalog.pg_shdescription D on D.objoid = T.oid"},
            {"Q15-tblspc-db-acl", "select T.oid as object_id, T.spcacl as acl from pg_catalog.pg_tablespace T union all select T.oid as object_id, T.datacl as acl from pg_catalog.pg_database T"},
            {"Q16-namespaces", "select N.oid::bigint as id, N.xmin as state_number, nspname as name, D.description, pg_catalog.pg_get_userbyid(N.nspowner) as \"owner\" from pg_catalog.pg_namespace N left join pg_catalog.pg_description D on N.oid = D.objoid order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end"},
            {"Q17-is-super", "select usesuper from pg_user where usename = current_user"},
            {"Q18-event-triggers", "select t.oid as id, t.xmin as state_number, t.evtname as name, t.evtevent as event, t.evtfoid as routine_id, pg_catalog.pg_get_userbyid(t.evtowner) as owner, t.evttags as tags, case when t.evtenabled = 'D' then 1 else 0 end as is_disabled from pg_catalog.pg_event_trigger t"},
            {"Q19-fdw", "select fdw.oid as id, fdw.xmin as state_number, fdw.fdwname as name, pr.proname as handler, nspc.nspname as handler_schema, pr2.proname as validator, nspc2.nspname as validator_schema, fdw.fdwoptions as options, pg_catalog.pg_get_userbyid(fdw.fdwowner) as \"owner\" from pg_catalog.pg_foreign_data_wrapper fdw left outer join pg_catalog.pg_proc pr on fdw.fdwhandler = pr.oid left outer join pg_catalog.pg_namespace nspc on pr.pronamespace = nspc.oid left outer join pg_catalog.pg_proc pr2 on fdw.fdwvalidator = pr2.oid left outer join pg_catalog.pg_namespace nspc2 on pr2.pronamespace = nspc2.oid"},
            {"Q20-foreign-server", "select srv.oid as id, srv.srvfdw as fdw_id, srv.xmin as state_number, srv.srvname as name, srv.srvtype as type, srv.srvversion as version, srv.srvoptions as options, pg_catalog.pg_get_userbyid(srv.srvowner) as \"owner\" from pg_catalog.pg_foreign_server srv"},
            {"Q21-user-mapping", "select oid as id, umserver as server_id, case when umuser = 0 then null else pg_catalog.pg_get_userbyid(umuser) end as \"user\", umoptions as options from pg_catalog.pg_user_mapping order by server_id"},
            {"Q22-access-methods", "select A.oid as access_method_id, A.xmin as state_number, A.amname as access_method_name, A.amhandler::oid as handler_id, pg_catalog.quote_ident(N.nspname) || '.' || pg_catalog.quote_ident(P.proname) as handler_name, A.amtype as access_method_type from pg_am A join pg_proc P on A.amhandler::oid = P.oid join pg_namespace N on P.pronamespace = N.oid"},
            {"Q23-extensions", "select E.oid as id, E.xmin as state_number, extname as name, extversion as version, extnamespace as schema_id, nspname as schema_name, array(select unnest from unnest(available_versions) where unnest > extversion) as available_updates from pg_catalog.pg_extension E join pg_namespace N on E.extnamespace = N.oid left join (select name, array_agg(version) as available_versions from pg_available_extension_versions() group by name) V on E.extname = V.name"},
            {"Q24-languages", "select l.oid as id, l.xmin state_number, lanname as name, lanpltrusted as trusted, h.proname as handler, hs.nspname as handlerSchema, i.proname as inline, isc.nspname as inlineSchema, v.proname as validator, vs.nspname as validatorSchema from pg_catalog.pg_language l left join pg_catalog.pg_proc h on h.oid = lanplcallfoid left join pg_catalog.pg_namespace hs on hs.oid = h.pronamespace left join pg_catalog.pg_proc i on i.oid = laninline left join pg_catalog.pg_namespace isc on isc.oid = i.pronamespace left join pg_catalog.pg_proc v on v.oid = lanvalidator left join pg_catalog.pg_namespace vs on vs.oid = v.pronamespace order by lanname"},
            {"Q25-misc-descriptions", "select D.objoid id, case when 'pg_catalog.pg_event_trigger'::regclass = classoid then 'T' when 'pg_catalog.pg_am'::regclass = classoid then 'A' when 'pg_catalog.pg_cast'::regclass = classoid then 'C' when 'pg_catalog.pg_foreign_data_wrapper'::regclass = classoid then 'W' when 'pg_catalog.pg_foreign_server'::regclass = classoid then 'S' when 'pg_catalog.pg_language'::regclass = classoid then 'L' when 'pg_catalog.pg_extension'::regclass = classoid then 'E' end as kind, D.objsubid sub_id, D.description from pg_catalog.pg_description D where classoid in ('pg_catalog.pg_event_trigger'::regclass, 'pg_catalog.pg_am'::regclass, 'pg_catalog.pg_cast'::regclass, 'pg_catalog.pg_foreign_data_wrapper'::regclass, 'pg_catalog.pg_foreign_server'::regclass, 'pg_catalog.pg_language'::regclass, 'pg_catalog.pg_extension'::regclass)"},
            {"Q26-acl-union", "select T.oid as object_id, T.fdwacl as acl from pg_catalog.pg_foreign_data_wrapper T union all select T.oid as object_id, T.lanacl as acl from pg_catalog.pg_language T union all select T.oid as object_id, T.nspacl as acl from pg_catalog.pg_namespace T union all select T.oid as object_id, T.srvacl as acl from pg_catalog.pg_foreign_server T"},
            {"Q27-casts", "select C.oid, C.xmin as state_number, C.castsource as castsource_id, pg_catalog.quote_ident(SN.nspname) || '.' || pg_catalog.quote_ident(S.typname) as castsource_name, C.casttarget as casttarget_id, pg_catalog.quote_ident(TN.nspname) || '.' || pg_catalog.quote_ident(T.typname) as casttarget_name, C.castfunc as castfunc_id, pg_catalog.quote_ident(FN.nspname) || '.' || pg_catalog.quote_ident(F.proname) as castfunc_name, C.castcontext, C.castmethod from pg_cast C left outer join pg_proc F on C.castfunc::oid = F.oid left outer join pg_namespace FN on F.pronamespace = FN.oid join pg_type S on C.castsource::oid = S.oid join pg_namespace SN on S.typnamespace = SN.oid join pg_type T on C.casttarget::oid = T.oid join pg_namespace TN on T.typnamespace = TN.oid"},
            {"Q28-ext-depends", "select E.oid as extension_id, D.objid as member_id from pg_extension E join pg_depend D on E.oid = D.refobjid and D.refclassid = 'pg_extension'::regclass::oid where D.deptype = 'e' order by extension_id"},
            {"Q29-null-template", "select T.oid as oid, relnamespace as schemaId, pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind from pg_catalog.pg_class T where relnamespace in ( null ) and relkind in ('r', 'm', 'v', 'p', 'f', 'S') union all select T.oid, T.typnamespace, 'T' as kind from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid where T.typnamespace in ( null ) and ( T.typtype in ('d','e') or C.relkind = 'c'::\"char\" or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or T.typtype = 'p' and not T.typisdefined ) union all select oid, collnamespace, 'C' as kind from pg_catalog.pg_collation where collnamespace in ( null ) union all select oid, oprnamespace, 'O' as kind from pg_catalog.pg_operator where oprnamespace in ( null ) union all select oid, opcnamespace, 'c' as kind from pg_catalog.pg_opclass where opcnamespace in ( null ) union all select oid, opfnamespace, 'F' as kind from pg_catalog.pg_opfamily where opfnamespace in ( null ) union all select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end as kind from pg_catalog.pg_proc where pronamespace in ( null )"},
        };

        for (String[] entry : phase1) {
            String label = entry[0], sql = entry[1];
            try (Statement s = c.createStatement()) {
                boolean hasRS = s.execute(sql);
                if (hasRS) {
                    ResultSet rs = s.getResultSet();
                    int rows = 0;
                    while (rs.next()) rows++;
                    System.out.println("OK   " + label + " (" + rows + " rows)");
                } else {
                    System.out.println("OK   " + label + " (no RS)");
                }
                pass++;
            } catch (Exception e) {
                System.out.println("FAIL " + label + ": " + firstLine(e));
                fail++;
            }
        }

        // ============================================================
        // Phase 2: Parameterized queries (convert $N -> ? for JDBC)
        // ============================================================
        System.out.println("\n--- Phase 2: Parameterized queries ---");

        final class PQ {
            public final String label;
            public final String sql;
            public final Object[] params;

            public PQ(String label, String sql, Object[] params) {
                this.label = label;
                this.sql = sql;
                this.params = params;
            }

            public String label() { return label; }
            public String sql() { return sql; }
            public Object[] params() { return params; }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                PQ that = (PQ) o;
                return java.util.Objects.equals(label, that.label)
                    && java.util.Objects.equals(sql, that.sql)
                    && java.util.Arrays.equals(params, that.params);
            }

            @Override
            public int hashCode() {
                return java.util.Objects.hash(label, sql, java.util.Arrays.hashCode(params));
            }

            @Override
            public String toString() {
                return "PQ[label=" + label + ", " + "sql=" + sql + ", " + "params=" + java.util.Arrays.toString(params) + "]";
            }
        }
        List<PQ> pqs = new ArrayList<>();

        pqs.add(new PQ("P01-schema-objects", "select T.oid as oid, relnamespace as schemaId, pg_catalog.translate(relkind, 'rmvpfS', 'rmvrfS') as kind, relname as name from pg_catalog.pg_class T where relnamespace in ( ?::oid ) and relkind in ('r', 'm', 'v', 'p', 'f', 'S') union all select T.oid, T.typnamespace, 'T', T.typname from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid where T.typnamespace in ( ?::oid ) and ( T.typtype in ('d','e') or C.relkind = 'c'::\"char\" or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or T.typtype = 'p' and not T.typisdefined ) union all select oid, collnamespace, 'C', collname from pg_catalog.pg_collation where collnamespace in ( ?::oid ) union all select oid, oprnamespace, 'O', oprname from pg_catalog.pg_operator where oprnamespace in ( ?::oid ) union all select oid, opcnamespace, 'c', opcname from pg_catalog.pg_opclass where opcnamespace in ( ?::oid ) union all select oid, opfnamespace, 'F', opfname from pg_catalog.pg_opfamily where opfnamespace in ( ?::oid ) union all select oid, pronamespace, case when prokind != 'a' then 'R' else 'a' end, proname from pg_catalog.pg_proc where pronamespace in ( ?::oid ) order by schemaId",
            new Object[]{publicNsOid, publicNsOid, publicNsOid, publicNsOid, publicNsOid, publicNsOid, publicNsOid}));

        pqs.add(new PQ("P02-proc-args", "select pronamespace as schemaId, oid as majorOid, proargnames as argNames, proargmodes as argModes, array_length(proargtypes, 1) as nArgs from pg_catalog.pg_proc where pronamespace in ( ?::oid ) order by schemaId",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P03-table-columns", "with T as ( select T.oid as oid, T.relkind as kind, T.relnamespace as schemaId from pg_catalog.pg_class T where T.relnamespace in ( ?::oid ) and T.relkind in ('r', 'm', 'v', 'f', 'p') ) select T.schemaId as schemaId, T.oid as majorOid, pg_catalog.translate(T.kind, 'rmvpf', 'rmvrf') as kind, C.attnum as position, C.attname as name from T join pg_catalog.pg_attribute C on T.oid = C.attrelid where C.attnum > 0 and not C.attisdropped order by schemaId, majorOid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P04-sequences", "select cls.xmin as sequence_state_number, sq.seqrelid as sequence_id, cls.relname as sequence_name, pg_catalog.format_type(sq.seqtypid, null) as data_type, sq.seqstart as start_value, sq.seqincrement as inc_value, sq.seqmin as min_value, sq.seqmax as max_value, sq.seqcache as cache_size, sq.seqcycle as cycle_option, pg_catalog.pg_get_userbyid(cls.relowner) as \"owner\" from pg_catalog.pg_sequence sq join pg_class cls on sq.seqrelid = cls.oid where cls.relnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P05-types", "select T.oid as type_id, T.xmin as type_state_number, T.typname as type_name, T.typtype as type_sub_kind, T.typcategory as type_category, T.typrelid as class_id, T.typbasetype as base_type_id, case when T.typtype in ('c','e') then null else pg_catalog.format_type(T.typbasetype, T.typtypmod) end as type_def, T.typndims as dimensions_number, T.typdefault as default_expression, T.typnotnull as mandatory, pg_catalog.pg_get_userbyid(T.typowner) as \"owner\" from pg_catalog.pg_type T left outer join pg_catalog.pg_class C on T.typrelid = C.oid where T.typnamespace = ?::oid and (T.typtype in ('d','e') or C.relkind = 'c'::\"char\" or (T.typtype = 'b' and (T.typelem = 0 OR T.typcategory <> 'A')) or T.typtype = 'p' and not T.typisdefined) order by 1",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P06-tables", "select T.relkind as table_kind, T.relname as table_name, T.oid as table_id, T.xmin as table_state_number, false as table_with_oids, T.reltablespace as tablespace_id, T.reloptions as options, T.relpersistence as persistence, (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where T.oid = inhrelid) as ancestors, (select pg_catalog.array_agg(inhrelid::bigint order by inhrelid)::varchar from pg_catalog.pg_inherits where T.oid = inhparent) as successors, T.relispartition as is_partition, pg_catalog.pg_get_partkeydef(T.oid) as partition_key, pg_catalog.pg_get_expr(T.relpartbound, T.oid) as partition_expression, T.relam am_id, pg_catalog.pg_get_userbyid(T.relowner) as \"owner\" from pg_catalog.pg_class T where relnamespace = ?::oid and relkind in ('r', 'm', 'v', 'f', 'p') order by table_kind, table_id",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P07-foreign-tables", "select ft.ftrelid as table_id, srv.srvname as table_server, ft.ftoptions as table_options, pg_catalog.pg_get_userbyid(cls.relowner) as \"owner\" from pg_catalog.pg_foreign_table ft left outer join pg_catalog.pg_foreign_server srv on ft.ftserver = srv.oid join pg_catalog.pg_class cls on ft.ftrelid = cls.oid where cls.relnamespace = ?::oid order by table_id",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P08-arg-types", "with schema_procs as (select prorettype, proargtypes, proallargtypes from pg_catalog.pg_proc where pronamespace = ?::oid), schema_opers as (select oprleft, oprright, oprresult from pg_catalog.pg_operator where oprnamespace = ?::oid), schema_aggregates as (select A.aggtranstype, A.aggmtranstype from pg_catalog.pg_aggregate A join pg_catalog.pg_proc P on A.aggfnoid = P.oid where P.pronamespace = ?::oid), schema_arg_types as ( select prorettype as type_id from schema_procs union select distinct unnest(proargtypes) as type_id from schema_procs union select distinct unnest(proallargtypes) as type_id from schema_procs union select oprleft as type_id from schema_opers where oprleft is not null union select oprright as type_id from schema_opers where oprright is not null union select oprresult as type_id from schema_opers where oprresult is not null union select aggtranstype::oid as type_id from schema_aggregates union select aggmtranstype::oid as type_id from schema_aggregates ) select type_id, pg_catalog.format_type(type_id, null) as type_spec from schema_arg_types where type_id <> 0",
            new Object[]{publicNsOid, publicNsOid, publicNsOid}));

        pqs.add(new PQ("P09-routines", "with languages as (select oid as lang_oid, lanname as lang from pg_catalog.pg_language), routines as (select proname as r_name, prolang as lang_oid, oid as r_id, xmin as r_state_number, proargnames as arg_names, proargmodes as arg_modes, proargtypes::int[] as in_arg_types, proallargtypes::int[] as all_arg_types, pg_catalog.pg_get_expr(proargdefaults, 0) as arg_defaults, provariadic as arg_variadic_id, prorettype as ret_type_id, proretset as ret_set, prokind as kind, provolatile as volatile_kind, proisstrict as is_strict, prosecdef as is_security_definer, proconfig as configuration_parameters, procost as cost, pg_catalog.pg_get_userbyid(proowner) as \"owner\", prorows as rows, proleakproof as is_leakproof, proparallel as concurrency_kind from pg_catalog.pg_proc where pronamespace = ?::oid and not (prokind = 'a') ) select * from routines natural join languages",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P10-aggregates", "select P.oid as aggregate_id, P.xmin as state_number, P.proname as aggregate_name, P.proargnames as arg_names, P.proargmodes as arg_modes, P.proargtypes::int[] as in_arg_types, P.proallargtypes::int[] as all_arg_types, A.aggtransfn::oid as transition_function_id, A.aggtransfn::regproc::text as transition_function_name, A.aggtranstype as transition_type, A.aggfinalfn::oid as final_function_id, case when A.aggfinalfn::oid = 0 then null else A.aggfinalfn::regproc::varchar end as final_function_name, case when A.aggfinalfn::oid = 0 then 0 else P.prorettype end as final_return_type, A.agginitval as initial_value, A.aggsortop as sort_operator_id, case when A.aggsortop = 0 then null else A.aggsortop::regoper::varchar end as sort_operator_name, pg_catalog.pg_get_userbyid(P.proowner) as \"owner\", A.aggfinalextra as final_extra, A.aggtransspace as state_size, A.aggmtransfn::oid as moving_transition_id, case when A.aggmtransfn::oid = 0 then null else A.aggmtransfn::regproc::varchar end as moving_transition_name, A.aggminvtransfn::oid as inverse_transition_id, case when A.aggminvtransfn::oid = 0 then null else A.aggminvtransfn::regproc::varchar end as inverse_transition_name, A.aggmtranstype::oid as moving_state_type, A.aggmtransspace as moving_state_size, A.aggmfinalfn::oid as moving_final_id, case when A.aggmfinalfn::oid = 0 then null else A.aggmfinalfn::regproc::varchar end as moving_final_name, A.aggmfinalextra as moving_final_extra, A.aggminitval as moving_initial_value, A.aggkind as aggregate_kind, A.aggnumdirectargs as direct_args, A.aggcombinefn::oid as combine_function_id, case when A.aggcombinefn::oid = 0 then null else A.aggcombinefn::regproc::varchar end as combine_function_name, A.aggserialfn::oid as serialization_function_id, case when A.aggserialfn::oid = 0 then null else A.aggserialfn::regproc::varchar end as serialization_function_name, A.aggdeserialfn::oid as deserialization_function_id, case when A.aggdeserialfn::oid = 0 then null else A.aggdeserialfn::regproc::varchar end as deserialization_function_name, P.proparallel as concurrency_kind from pg_catalog.pg_aggregate A join pg_catalog.pg_proc P on A.aggfnoid = P.oid where P.pronamespace = ?::oid order by P.oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P11-operators", "select O.oid as op_id, O.xmin as state_number, oprname as op_name, oprkind as op_kind, oprleft as arg_left_type_id, oprright as arg_right_type_id, oprresult as arg_result_type_id, oprcode::oid as main_id, oprcode::varchar as main_name, oprrest::oid as restrict_id, oprrest::varchar as restrict_name, oprjoin::oid as join_id, oprjoin::varchar as join_name, oprcom::oid as com_id, oprcom::regoper::varchar as com_name, oprnegate::oid as neg_id, oprnegate::regoper::varchar as neg_name, oprcanmerge as merges, oprcanhash as hashes, pg_catalog.pg_get_userbyid(O.oprowner) as \"owner\" from pg_catalog.pg_operator O where oprnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P12-collations", "select oid as id, xmin as state_number, collname as name, collcollate as lc_collate, collctype as lc_ctype, pg_catalog.pg_get_userbyid(collowner) as \"owner\" from pg_catalog.pg_collation where collnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P13-opclass", "select O.oid as id, O.xmin as state_number, opcname as name, opcintype::regtype::varchar as in_type, case when opckeytype = 0 then null else opckeytype::regtype::varchar end as key_type, opcdefault as is_default, opcfamily as family_id, opfname as family, opcmethod as access_method_id, pg_catalog.pg_get_userbyid(O.opcowner) as \"owner\" from pg_catalog.pg_opclass O join pg_catalog.pg_opfamily F on F.oid = opcfamily where opcnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P14-opfamily", "select O.oid as id, O.xmin as state_number, opfname as name, opfmethod as access_method_id, pg_catalog.pg_get_userbyid(O.opfowner) as \"owner\" from pg_catalog.pg_opfamily O where opfnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P15-index-ids", "select IX.indexrelid from pg_catalog.pg_index IX, pg_catalog.pg_class IC where IC.oid = IX.indrelid and IC.relnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P16-indexes", "select tab.oid table_id, tab.relkind table_kind, ind_stor.relname index_name, ind_head.indexrelid index_id, ind_stor.xmin state_number, ind_head.indisunique is_unique, ind_head.indisprimary is_primary, ind_head.indnullsnotdistinct nulls_not_distinct, pg_catalog.pg_get_expr(ind_head.indpred, ind_head.indrelid) as condition, (select pg_catalog.array_agg(inhparent::bigint order by inhseqno)::varchar from pg_catalog.pg_inherits where ind_stor.oid = inhrelid) as ancestors, ind_stor.reltablespace tablespace_id, opcmethod as access_method_id from pg_catalog.pg_class tab join pg_catalog.pg_index ind_head on ind_head.indrelid = tab.oid join pg_catalog.pg_class ind_stor on tab.relnamespace = ind_stor.relnamespace and ind_stor.oid = ind_head.indexrelid left join pg_catalog.pg_opclass on pg_opclass.oid = ANY(indclass) where tab.relnamespace = ?::oid and tab.relkind in ('r', 'm', 'v', 'p') and ind_stor.relkind in ('i', 'I')",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P17-index-cols", "select ind_head.indexrelid index_id, k col_idx, k <= indnkeyatts in_key, ind_head.indkey[k-1] column_position, ind_head.indoption[k-1] column_options, ind_head.indcollation[k-1] as collation, colln.nspname as collation_schema, collname as collation_str, ind_head.indclass[k-1] as opclass, case when opcdefault then null else opcn.nspname end as opclass_schema, case when opcdefault then null else opcname end as opclass_str, case when indexprs is null then null when ind_head.indkey[k-1] = 0 then chr(27) || pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true) else pg_catalog.pg_get_indexdef(ind_head.indexrelid, k::int, true) end as expression, amcanorder can_order from pg_catalog.pg_index ind_head join pg_catalog.pg_class ind_stor on ind_stor.oid = ind_head.indexrelid cross join unnest(ind_head.indkey) with ordinality u(u, k) left join pg_catalog.pg_collation on pg_collation.oid = ind_head.indcollation[k-1] left join pg_catalog.pg_namespace colln on collnamespace = colln.oid cross join pg_catalog.pg_indexam_has_property(ind_stor.relam, 'can_order') amcanorder left join pg_catalog.pg_opclass on pg_opclass.oid = ind_head.indclass[k-1] left join pg_catalog.pg_namespace opcn on opcnamespace = opcn.oid where ind_stor.relnamespace = ?::oid and ind_stor.relkind in ('i', 'I') order by index_id, k",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P18-constraint-ids", "select oid from pg_catalog.pg_constraint where conrelid != 0 and connamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P19-constraints", "select T.oid table_id, relkind table_kind, C.oid::bigint con_id, C.xmin::varchar::bigint con_state_id, conname con_name, contype con_kind, conkey con_columns, conindid index_id, confrelid ref_table_id, condeferrable is_deferrable, condeferred is_init_deferred, confupdtype on_update, confdeltype on_delete, connoinherit no_inherit, pg_catalog.pg_get_expr(conbin, T.oid) con_expression, confkey ref_columns, conexclop::int[] excl_operators, array(select unnest::regoper::varchar from unnest(conexclop)) excl_operators_str from pg_catalog.pg_constraint C join pg_catalog.pg_class T on C.conrelid = T.oid where relkind in ('r', 'v', 'f', 'p') and relnamespace = ?::oid and contype in ('p', 'u', 'f', 'c', 'x') and connamespace = ?::oid",
            new Object[]{publicNsOid, publicNsOid}));

        pqs.add(new PQ("P20-rules", "select RU.oid from pg_catalog.pg_rewrite RU, pg_catalog.pg_class RC where RC.oid = RU.ev_class and RC.relnamespace = ?::oid and not RU.rulename = '_RETURN'",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P21-policies", "select P.oid from pg_catalog.pg_policy P join pg_catalog.pg_class C on polrelid = C.oid where relnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P22-triggers", "select TG.oid from pg_catalog.pg_trigger TG, pg_catalog.pg_class TC where TC.oid = TG.tgrelid and TC.relnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P23-seq-depends", "select D.objid as dependent_id, D.refobjid as owner_id, D.refobjsubid as owner_subobject_id from pg_depend D join pg_class C_SEQ on D.objid = C_SEQ.oid and D.classid = 'pg_class'::regclass::oid join pg_class C_TAB on D.refobjid = C_TAB.oid and D.refclassid = 'pg_class'::regclass::oid where C_SEQ.relkind = 'S' and C_TAB.relkind = 'r' and D.refobjsubid <> 0 and (D.deptype = 'a' or D.deptype = 'i') and C_TAB.relnamespace = ?::oid order by owner_id",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P24-view-defs", "select T.relkind as view_kind, T.oid as view_id, pg_catalog.pg_get_viewdef(T.oid, true) as source_text from pg_catalog.pg_class T join pg_catalog.pg_namespace N on T.relnamespace = N.oid where N.oid = ?::oid and T.relkind in ('m','v')",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P25-func-source", "with system_languages as ( select oid as lang from pg_catalog.pg_language where lanname in ('c','internal') ) select oid as id, pg_catalog.pg_get_function_arguments(oid) as arguments_def, pg_catalog.pg_get_function_result(oid) as result_def, pg_catalog.pg_get_function_sqlbody(oid) as sqlbody_def, prosrc as source_text from pg_catalog.pg_proc where pronamespace = ?::oid and not (prokind = 'a') and prolang not in (select lang from system_languages) and prosrc is not null",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P26-acl-age", "select T.oid as object_id, T.relacl as acl from pg_catalog.pg_class T where relnamespace = ?::oid and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid) union all select T.oid as object_id, T.proacl as acl from pg_catalog.pg_proc T where pronamespace = ?::oid and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid) union all select T.oid as object_id, T.typacl as acl from pg_catalog.pg_type T where typnamespace = ?::oid and pg_catalog.age(T.xmin) <= pg_catalog.age(?::varchar::xid) order by object_id",
            new Object[]{publicNsOid, "1", publicNsOid, "1", publicNsOid, "1"}));

        pqs.add(new PQ("P27-attr-acl", "select T.oid as object_id, A.attnum as attr_position, A.attacl as acl from pg_catalog.pg_attribute A join pg_catalog.pg_class T on T.oid = A.attrelid where relnamespace = ?::oid and attnum > 0 and pg_catalog.age(A.xmin) <= pg_catalog.age(?::varchar::xid) order by object_id, attr_position",
            new Object[]{publicNsOid, "1"}));

        pqs.add(new PQ("P28-amop-ids", "select pg_amop.oid from pg_catalog.pg_amop join pg_catalog.pg_opfamily on pg_opfamily.oid = amopfamily where opfnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P29-amproc-ids", "select pg_amproc.oid from pg_catalog.pg_amproc join pg_catalog.pg_opfamily on pg_opfamily.oid = amprocfamily where opfnamespace = ?::oid",
            new Object[]{publicNsOid}));

        pqs.add(new PQ("P30-show-txn", "SHOW TRANSACTION ISOLATION LEVEL", new Object[]{}));

        for (PQ pq : pqs) {
            try {
                if (pq.params().length == 0) {
                    try (Statement s = c.createStatement()) {
                        boolean hasRS = s.execute(pq.sql());
                        if (hasRS) {
                            ResultSet rs = s.getResultSet();
                            int rows = 0;
                            while (rs.next()) rows++;
                            System.out.println("OK   " + pq.label() + " (" + rows + " rows)");
                        } else {
                            System.out.println("OK   " + pq.label() + " (no RS)");
                        }
                    }
                } else {
                    PreparedStatement ps = c.prepareStatement(pq.sql());
                    for (int i = 0; i < pq.params().length; i++) {
                        Object p = pq.params()[i];
                        if (p instanceof Integer) ps.setInt(i + 1, (Integer) p);
                        else if (p instanceof String) ps.setString(i + 1, (String) p);
                        else if (p instanceof Long) ps.setLong(i + 1, (Long) p);
                        else ps.setObject(i + 1, p);
                    }
                    boolean hasRS = ps.execute();
                    if (hasRS) {
                        ResultSet rs = ps.getResultSet();
                        int rows = 0;
                        while (rs.next()) rows++;
                        System.out.println("OK   " + pq.label() + " (" + rows + " rows)");
                    } else {
                        System.out.println("OK   " + pq.label() + " (no RS)");
                    }
                    ps.close();
                }
                pass++;
            } catch (Exception e) {
                System.out.println("FAIL " + pq.label() + ": " + firstLine(e));
                fail++;
            }
        }

        System.out.println("\n=== SUMMARY: " + pass + " passed, " + fail + " failed ===");
        c.close();
        m.close();
    }

    static String firstLine(Exception e) {
        String msg = e.getMessage();
        if (msg == null) return e.getClass().getSimpleName();
        int nl = msg.indexOf('\n');
        return nl > 0 ? msg.substring(0, nl) : msg;
    }
}
