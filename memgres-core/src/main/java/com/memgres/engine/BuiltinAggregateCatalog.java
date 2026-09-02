package com.memgres.engine;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * What pg_aggregate holds for each built-in aggregate, measured from PostgreSQL 18.
 *
 * <p>An aggregate row is more than a name: it says which routine accumulates the rows, which one
 * turns the accumulation into the answer, what the accumulator is made of, and what it starts out
 * as. Every one of those was reported as "none" here, so a client asking how {@code count} is
 * computed — or whether an aggregate has a moving-window form it could use for a frame — was told
 * nothing, and the ordered-set and hypothetical-set aggregates had no row at all.
 *
 * <p>Each entry is the columns of one row separated by {@code ~}, keyed by the aggregate's name
 * and the OIDs of its arguments; {@code @} stands for a value that is not there.
 */
final class BuiltinAggregateCatalog {
    private BuiltinAggregateCatalog() {}

    /** One aggregate's row, as the catalogue reports it. */
    static final class Row {
        final String kind;
        final short directArgs;
        final String transFn;
        final String finalFn;
        final int transType;
        final String initVal;
        final int sortOp;
        final String mTransFn;
        final String mInvTransFn;
        final String mFinalFn;
        final int mTransType;
        final String mInitVal;
        final boolean finalExtra;
        final boolean mFinalExtra;
        final String finalModify;
        final String mFinalModify;
        final int transSpace;
        final int mTransSpace;

        Row(String[] c) {
            this.kind = c[2];
            this.directArgs = Short.parseShort(c[3]);
            this.transFn = c[4];
            this.finalFn = c[5];
            this.transType = Integer.parseInt(c[6]);
            this.initVal = "@".equals(c[7]) ? null : c[7];
            this.sortOp = Integer.parseInt(c[8]);
            this.mTransFn = c[9];
            this.mInvTransFn = c[10];
            this.mFinalFn = c[11];
            this.mTransType = Integer.parseInt(c[12]);
            this.mInitVal = "@".equals(c[13]) ? null : c[13];
            this.finalExtra = Boolean.parseBoolean(c[14]);
            this.mFinalExtra = Boolean.parseBoolean(c[15]);
            this.finalModify = c[16];
            this.mFinalModify = c[17];
            this.transSpace = Integer.parseInt(c[18]);
            this.mTransSpace = Integer.parseInt(c[19]);
        }
    }

    private static final String[] MEASURED = {
            "any_value~2283~n~0~any_value_transfn~-~2283~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "array_agg~2277~n~0~array_agg_array_transfn~array_agg_array_finalfn~2281~@~0~-~-~-~0~@~true~false~r~r~0~0",
            "array_agg~2776~n~0~array_agg_transfn~array_agg_finalfn~2281~@~0~-~-~-~0~@~true~false~r~r~0~0",
            "avg~1186~n~0~interval_avg_accum~interval_avg~2281~@~0~interval_avg_accum~interval_avg_accum_inv~interval_avg~2281~@~false~false~r~r~40~40",
            "avg~1700~n~0~numeric_avg_accum~numeric_avg~2281~@~0~numeric_avg_accum~numeric_accum_inv~numeric_avg~2281~@~false~false~r~r~128~128",
            "avg~20~n~0~int8_avg_accum~numeric_poly_avg~2281~@~0~int8_avg_accum~int8_avg_accum_inv~numeric_poly_avg~2281~@~false~false~r~r~48~48",
            "avg~21~n~0~int2_avg_accum~int8_avg~1016~{0,0}~0~int2_avg_accum~int2_avg_accum_inv~int8_avg~1016~{0,0}~false~false~r~r~0~0",
            "avg~23~n~0~int4_avg_accum~int8_avg~1016~{0,0}~0~int4_avg_accum~int4_avg_accum_inv~int8_avg~1016~{0,0}~false~false~r~r~0~0",
            "avg~700~n~0~float4_accum~float8_avg~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "avg~701~n~0~float8_accum~float8_avg~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_and~1560~n~0~bitand~-~1560~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_and~20~n~0~int8and~-~20~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_and~21~n~0~int2and~-~21~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_and~23~n~0~int4and~-~23~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_or~1560~n~0~bitor~-~1560~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_or~20~n~0~int8or~-~20~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_or~21~n~0~int2or~-~21~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_or~23~n~0~int4or~-~23~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_xor~1560~n~0~bitxor~-~1560~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_xor~20~n~0~int8xor~-~20~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_xor~21~n~0~int2xor~-~21~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bit_xor~23~n~0~int4xor~-~23~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "bool_and~16~n~0~booland_statefunc~-~16~@~58~bool_accum~bool_accum_inv~bool_alltrue~2281~@~false~false~r~r~0~16",
            "bool_or~16~n~0~boolor_statefunc~-~16~@~59~bool_accum~bool_accum_inv~bool_anytrue~2281~@~false~false~r~r~0~16",
            "corr~701 701~n~0~float8_regr_accum~float8_corr~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "count~~n~0~int8inc~-~20~0~0~int8inc~int8dec~-~20~0~false~false~r~r~0~0",
            "count~2276~n~0~int8inc_any~-~20~0~0~int8inc_any~int8dec_any~-~20~0~false~false~r~r~0~0",
            "covar_pop~701 701~n~0~float8_regr_accum~float8_covar_pop~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "covar_samp~701 701~n~0~float8_regr_accum~float8_covar_samp~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "cume_dist~2276~h~1~ordered_set_transition_multi~cume_dist_final~2281~@~0~-~-~-~0~@~true~false~w~w~0~0",
            "dense_rank~2276~h~1~ordered_set_transition_multi~dense_rank_final~2281~@~0~-~-~-~0~@~true~false~w~w~0~0",
            "every~16~n~0~booland_statefunc~-~16~@~58~bool_accum~bool_accum_inv~bool_alltrue~2281~@~false~false~r~r~0~16",
            "json_agg~2283~n~0~json_agg_transfn~json_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "json_agg_strict~2283~n~0~json_agg_strict_transfn~json_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "json_object_agg~2276 2276~n~0~json_object_agg_transfn~json_object_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "json_object_agg_strict~2276 2276~n~0~json_object_agg_strict_transfn~json_object_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "json_object_agg_unique~2276 2276~n~0~json_object_agg_unique_transfn~json_object_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "json_object_agg_unique_strict~2276 2276~n~0~json_object_agg_unique_strict_transfn~json_object_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "jsonb_agg~2283~n~0~jsonb_agg_transfn~jsonb_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "jsonb_agg_strict~2283~n~0~jsonb_agg_strict_transfn~jsonb_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "jsonb_object_agg~2276 2276~n~0~jsonb_object_agg_transfn~jsonb_object_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "jsonb_object_agg_strict~2276 2276~n~0~jsonb_object_agg_strict_transfn~jsonb_object_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "jsonb_object_agg_unique~2276 2276~n~0~jsonb_object_agg_unique_transfn~jsonb_object_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "jsonb_object_agg_unique_strict~2276 2276~n~0~jsonb_object_agg_unique_strict_transfn~jsonb_object_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "max~1042~n~0~bpchar_larger~-~1042~@~1060~-~-~-~0~@~false~false~r~r~0~0",
            "max~1082~n~0~date_larger~-~1082~@~1097~-~-~-~0~@~false~false~r~r~0~0",
            "max~1083~n~0~time_larger~-~1083~@~1112~-~-~-~0~@~false~false~r~r~0~0",
            "max~1114~n~0~timestamp_larger~-~1114~@~2064~-~-~-~0~@~false~false~r~r~0~0",
            "max~1184~n~0~timestamptz_larger~-~1184~@~1324~-~-~-~0~@~false~false~r~r~0~0",
            "max~1186~n~0~interval_larger~-~1186~@~1334~-~-~-~0~@~false~false~r~r~0~0",
            "max~1266~n~0~timetz_larger~-~1266~@~1554~-~-~-~0~@~false~false~r~r~0~0",
            "max~17~n~0~bytea_larger~-~17~@~1959~-~-~-~0~@~false~false~r~r~0~0",
            "max~1700~n~0~numeric_larger~-~1700~@~1756~-~-~-~0~@~false~false~r~r~0~0",
            "max~20~n~0~int8larger~-~20~@~413~-~-~-~0~@~false~false~r~r~0~0",
            "max~21~n~0~int2larger~-~21~@~520~-~-~-~0~@~false~false~r~r~0~0",
            "max~2249~n~0~record_larger~-~2249~@~2991~-~-~-~0~@~false~false~r~r~0~0",
            "max~2277~n~0~array_larger~-~2277~@~1073~-~-~-~0~@~false~false~r~r~0~0",
            "max~23~n~0~int4larger~-~23~@~521~-~-~-~0~@~false~false~r~r~0~0",
            "max~25~n~0~text_larger~-~25~@~666~-~-~-~0~@~false~false~r~r~0~0",
            "max~26~n~0~oidlarger~-~26~@~610~-~-~-~0~@~false~false~r~r~0~0",
            "max~27~n~0~tidlarger~-~27~@~2800~-~-~-~0~@~false~false~r~r~0~0",
            "max~3220~n~0~pg_lsn_larger~-~3220~@~3225~-~-~-~0~@~false~false~r~r~0~0",
            "max~3500~n~0~enum_larger~-~3500~@~3519~-~-~-~0~@~false~false~r~r~0~0",
            "max~5069~n~0~xid8_larger~-~5069~@~5074~-~-~-~0~@~false~false~r~r~0~0",
            "max~700~n~0~float4larger~-~700~@~623~-~-~-~0~@~false~false~r~r~0~0",
            "max~701~n~0~float8larger~-~701~@~674~-~-~-~0~@~false~false~r~r~0~0",
            "max~790~n~0~cashlarger~-~790~@~903~-~-~-~0~@~false~false~r~r~0~0",
            "max~869~n~0~network_larger~-~869~@~1205~-~-~-~0~@~false~false~r~r~0~0",
            "min~1042~n~0~bpchar_smaller~-~1042~@~1058~-~-~-~0~@~false~false~r~r~0~0",
            "min~1082~n~0~date_smaller~-~1082~@~1095~-~-~-~0~@~false~false~r~r~0~0",
            "min~1083~n~0~time_smaller~-~1083~@~1110~-~-~-~0~@~false~false~r~r~0~0",
            "min~1114~n~0~timestamp_smaller~-~1114~@~2062~-~-~-~0~@~false~false~r~r~0~0",
            "min~1184~n~0~timestamptz_smaller~-~1184~@~1322~-~-~-~0~@~false~false~r~r~0~0",
            "min~1186~n~0~interval_smaller~-~1186~@~1332~-~-~-~0~@~false~false~r~r~0~0",
            "min~1266~n~0~timetz_smaller~-~1266~@~1552~-~-~-~0~@~false~false~r~r~0~0",
            "min~17~n~0~bytea_smaller~-~17~@~1957~-~-~-~0~@~false~false~r~r~0~0",
            "min~1700~n~0~numeric_smaller~-~1700~@~1754~-~-~-~0~@~false~false~r~r~0~0",
            "min~20~n~0~int8smaller~-~20~@~412~-~-~-~0~@~false~false~r~r~0~0",
            "min~21~n~0~int2smaller~-~21~@~95~-~-~-~0~@~false~false~r~r~0~0",
            "min~2249~n~0~record_smaller~-~2249~@~2990~-~-~-~0~@~false~false~r~r~0~0",
            "min~2277~n~0~array_smaller~-~2277~@~1072~-~-~-~0~@~false~false~r~r~0~0",
            "min~23~n~0~int4smaller~-~23~@~97~-~-~-~0~@~false~false~r~r~0~0",
            "min~25~n~0~text_smaller~-~25~@~664~-~-~-~0~@~false~false~r~r~0~0",
            "min~26~n~0~oidsmaller~-~26~@~609~-~-~-~0~@~false~false~r~r~0~0",
            "min~27~n~0~tidsmaller~-~27~@~2799~-~-~-~0~@~false~false~r~r~0~0",
            "min~3220~n~0~pg_lsn_smaller~-~3220~@~3224~-~-~-~0~@~false~false~r~r~0~0",
            "min~3500~n~0~enum_smaller~-~3500~@~3518~-~-~-~0~@~false~false~r~r~0~0",
            "min~5069~n~0~xid8_smaller~-~5069~@~5073~-~-~-~0~@~false~false~r~r~0~0",
            "min~700~n~0~float4smaller~-~700~@~622~-~-~-~0~@~false~false~r~r~0~0",
            "min~701~n~0~float8smaller~-~701~@~672~-~-~-~0~@~false~false~r~r~0~0",
            "min~790~n~0~cashsmaller~-~790~@~902~-~-~-~0~@~false~false~r~r~0~0",
            "min~869~n~0~network_smaller~-~869~@~1203~-~-~-~0~@~false~false~r~r~0~0",
            "mode~2283~o~0~ordered_set_transition~mode_final~2281~@~0~-~-~-~0~@~true~false~s~s~0~0",
            "percent_rank~2276~h~1~ordered_set_transition_multi~percent_rank_final~2281~@~0~-~-~-~0~@~true~false~w~w~0~0",
            "percentile_cont~1022 1186~o~1~ordered_set_transition~percentile_cont_interval_multi_final~2281~@~0~-~-~-~0~@~false~false~s~s~0~0",
            "percentile_cont~1022 701~o~1~ordered_set_transition~percentile_cont_float8_multi_final~2281~@~0~-~-~-~0~@~false~false~s~s~0~0",
            "percentile_cont~701 1186~o~1~ordered_set_transition~percentile_cont_interval_final~2281~@~0~-~-~-~0~@~false~false~s~s~0~0",
            "percentile_cont~701 701~o~1~ordered_set_transition~percentile_cont_float8_final~2281~@~0~-~-~-~0~@~false~false~s~s~0~0",
            "percentile_disc~1022 2283~o~1~ordered_set_transition~percentile_disc_multi_final~2281~@~0~-~-~-~0~@~true~false~s~s~0~0",
            "percentile_disc~701 2283~o~1~ordered_set_transition~percentile_disc_final~2281~@~0~-~-~-~0~@~true~false~s~s~0~0",
            "range_agg~3831~n~0~range_agg_transfn~range_agg_finalfn~2281~@~0~-~-~-~0~@~true~false~r~r~0~0",
            "range_agg~4537~n~0~multirange_agg_transfn~multirange_agg_finalfn~2281~@~0~-~-~-~0~@~true~false~r~r~0~0",
            "range_intersect_agg~3831~n~0~range_intersect_agg_transfn~-~3831~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "range_intersect_agg~4537~n~0~multirange_intersect_agg_transfn~-~4537~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "rank~2276~h~1~ordered_set_transition_multi~rank_final~2281~@~0~-~-~-~0~@~true~false~w~w~0~0",
            "regr_avgx~701 701~n~0~float8_regr_accum~float8_regr_avgx~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "regr_avgy~701 701~n~0~float8_regr_accum~float8_regr_avgy~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "regr_count~701 701~n~0~int8inc_float8_float8~-~20~0~0~-~-~-~0~@~false~false~r~r~0~0",
            "regr_intercept~701 701~n~0~float8_regr_accum~float8_regr_intercept~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "regr_r2~701 701~n~0~float8_regr_accum~float8_regr_r2~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "regr_slope~701 701~n~0~float8_regr_accum~float8_regr_slope~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "regr_sxx~701 701~n~0~float8_regr_accum~float8_regr_sxx~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "regr_sxy~701 701~n~0~float8_regr_accum~float8_regr_sxy~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "regr_syy~701 701~n~0~float8_regr_accum~float8_regr_syy~1022~{0,0,0,0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "stddev~1700~n~0~numeric_accum~numeric_stddev_samp~2281~@~0~numeric_accum~numeric_accum_inv~numeric_stddev_samp~2281~@~false~false~r~r~128~128",
            "stddev~20~n~0~int8_accum~numeric_stddev_samp~2281~@~0~int8_accum~int8_accum_inv~numeric_stddev_samp~2281~@~false~false~r~r~128~128",
            "stddev~21~n~0~int2_accum~numeric_poly_stddev_samp~2281~@~0~int2_accum~int2_accum_inv~numeric_poly_stddev_samp~2281~@~false~false~r~r~48~48",
            "stddev~23~n~0~int4_accum~numeric_poly_stddev_samp~2281~@~0~int4_accum~int4_accum_inv~numeric_poly_stddev_samp~2281~@~false~false~r~r~48~48",
            "stddev~700~n~0~float4_accum~float8_stddev_samp~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "stddev~701~n~0~float8_accum~float8_stddev_samp~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "stddev_pop~1700~n~0~numeric_accum~numeric_stddev_pop~2281~@~0~numeric_accum~numeric_accum_inv~numeric_stddev_pop~2281~@~false~false~r~r~128~128",
            "stddev_pop~20~n~0~int8_accum~numeric_stddev_pop~2281~@~0~int8_accum~int8_accum_inv~numeric_stddev_pop~2281~@~false~false~r~r~128~128",
            "stddev_pop~21~n~0~int2_accum~numeric_poly_stddev_pop~2281~@~0~int2_accum~int2_accum_inv~numeric_poly_stddev_pop~2281~@~false~false~r~r~48~48",
            "stddev_pop~23~n~0~int4_accum~numeric_poly_stddev_pop~2281~@~0~int4_accum~int4_accum_inv~numeric_poly_stddev_pop~2281~@~false~false~r~r~48~48",
            "stddev_pop~700~n~0~float4_accum~float8_stddev_pop~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "stddev_pop~701~n~0~float8_accum~float8_stddev_pop~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "stddev_samp~1700~n~0~numeric_accum~numeric_stddev_samp~2281~@~0~numeric_accum~numeric_accum_inv~numeric_stddev_samp~2281~@~false~false~r~r~128~128",
            "stddev_samp~20~n~0~int8_accum~numeric_stddev_samp~2281~@~0~int8_accum~int8_accum_inv~numeric_stddev_samp~2281~@~false~false~r~r~128~128",
            "stddev_samp~21~n~0~int2_accum~numeric_poly_stddev_samp~2281~@~0~int2_accum~int2_accum_inv~numeric_poly_stddev_samp~2281~@~false~false~r~r~48~48",
            "stddev_samp~23~n~0~int4_accum~numeric_poly_stddev_samp~2281~@~0~int4_accum~int4_accum_inv~numeric_poly_stddev_samp~2281~@~false~false~r~r~48~48",
            "stddev_samp~700~n~0~float4_accum~float8_stddev_samp~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "stddev_samp~701~n~0~float8_accum~float8_stddev_samp~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "string_agg~17 17~n~0~bytea_string_agg_transfn~bytea_string_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "string_agg~25 25~n~0~string_agg_transfn~string_agg_finalfn~2281~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "sum~1186~n~0~interval_avg_accum~interval_sum~2281~@~0~interval_avg_accum~interval_avg_accum_inv~interval_sum~2281~@~false~false~r~r~40~40",
            "sum~1700~n~0~numeric_avg_accum~numeric_sum~2281~@~0~numeric_avg_accum~numeric_accum_inv~numeric_sum~2281~@~false~false~r~r~128~128",
            "sum~20~n~0~int8_avg_accum~numeric_poly_sum~2281~@~0~int8_avg_accum~int8_avg_accum_inv~numeric_poly_sum~2281~@~false~false~r~r~48~48",
            "sum~21~n~0~int2_sum~-~20~@~0~int2_avg_accum~int2_avg_accum_inv~int2int4_sum~1016~{0,0}~false~false~r~r~0~0",
            "sum~23~n~0~int4_sum~-~20~@~0~int4_avg_accum~int4_avg_accum_inv~int2int4_sum~1016~{0,0}~false~false~r~r~0~0",
            "sum~700~n~0~float4pl~-~700~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "sum~701~n~0~float8pl~-~701~@~0~-~-~-~0~@~false~false~r~r~0~0",
            "sum~790~n~0~cash_pl~-~790~@~0~cash_pl~cash_mi~-~790~@~false~false~r~r~0~0",
            "var_pop~1700~n~0~numeric_accum~numeric_var_pop~2281~@~0~numeric_accum~numeric_accum_inv~numeric_var_pop~2281~@~false~false~r~r~128~128",
            "var_pop~20~n~0~int8_accum~numeric_var_pop~2281~@~0~int8_accum~int8_accum_inv~numeric_var_pop~2281~@~false~false~r~r~128~128",
            "var_pop~21~n~0~int2_accum~numeric_poly_var_pop~2281~@~0~int2_accum~int2_accum_inv~numeric_poly_var_pop~2281~@~false~false~r~r~48~48",
            "var_pop~23~n~0~int4_accum~numeric_poly_var_pop~2281~@~0~int4_accum~int4_accum_inv~numeric_poly_var_pop~2281~@~false~false~r~r~48~48",
            "var_pop~700~n~0~float4_accum~float8_var_pop~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "var_pop~701~n~0~float8_accum~float8_var_pop~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "var_samp~1700~n~0~numeric_accum~numeric_var_samp~2281~@~0~numeric_accum~numeric_accum_inv~numeric_var_samp~2281~@~false~false~r~r~128~128",
            "var_samp~20~n~0~int8_accum~numeric_var_samp~2281~@~0~int8_accum~int8_accum_inv~numeric_var_samp~2281~@~false~false~r~r~128~128",
            "var_samp~21~n~0~int2_accum~numeric_poly_var_samp~2281~@~0~int2_accum~int2_accum_inv~numeric_poly_var_samp~2281~@~false~false~r~r~48~48",
            "var_samp~23~n~0~int4_accum~numeric_poly_var_samp~2281~@~0~int4_accum~int4_accum_inv~numeric_poly_var_samp~2281~@~false~false~r~r~48~48",
            "var_samp~700~n~0~float4_accum~float8_var_samp~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "var_samp~701~n~0~float8_accum~float8_var_samp~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "variance~1700~n~0~numeric_accum~numeric_var_samp~2281~@~0~numeric_accum~numeric_accum_inv~numeric_var_samp~2281~@~false~false~r~r~128~128",
            "variance~20~n~0~int8_accum~numeric_var_samp~2281~@~0~int8_accum~int8_accum_inv~numeric_var_samp~2281~@~false~false~r~r~128~128",
            "variance~21~n~0~int2_accum~numeric_poly_var_samp~2281~@~0~int2_accum~int2_accum_inv~numeric_poly_var_samp~2281~@~false~false~r~r~48~48",
            "variance~23~n~0~int4_accum~numeric_poly_var_samp~2281~@~0~int4_accum~int4_accum_inv~numeric_poly_var_samp~2281~@~false~false~r~r~48~48",
            "variance~700~n~0~float4_accum~float8_var_samp~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "variance~701~n~0~float8_accum~float8_var_samp~1022~{0,0,0}~0~-~-~-~0~@~false~false~r~r~0~0",
            "xmlagg~142~n~0~xmlconcat2~-~142~@~0~-~-~-~0~@~false~false~r~r~0~0"
    };

    private static final Map<String, Row> BY_SIGNATURE = bySignature();

    private static Map<String, Row> bySignature() {
        Map<String, Row> m = new HashMap<String, Row>();
        for (String entry : MEASURED) {
            String[] c = entry.split("~", -1);
            m.put(key(c[0], c[1]), new Row(c));
        }
        return Collections.unmodifiableMap(m);
    }

    private static String key(String name, String argTypeOids) {
        return name.toLowerCase(java.util.Locale.ROOT) + "(" + argTypeOids.trim() + ")";
    }

    /** The row for one overload, or null when PostgreSQL has no aggregate of that signature. */
    static Row of(String name, String argTypeOids) {
        return BY_SIGNATURE.get(key(name, argTypeOids));
    }

    /** Every signature there is, so the catalogue can carry the ones nothing else names. */
    static Iterable<String> signatures() {
        return BY_SIGNATURE.keySet();
    }

    static Row bySignatureKey(String signature) {
        return BY_SIGNATURE.get(signature);
    }
}
