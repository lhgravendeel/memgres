package com.memgres.engine;

/**
 * The signatures PostgreSQL gives the aggregates memgres implements.
 *
 * <p>An aggregate registered as returning anyelement tells a caller nothing: PostgreSQL has a
 * separate row per input type, and the result type differs between them — sum(int8) returns
 * numeric while sum(int4) returns bigint. A client deciding how to read the result, or an
 * overload resolution done against the catalog, needs the concrete type.
 *
 * <p>Columns: proname, prorettype, proargtypes (space-separated OIDs).
 */
final class BuiltinAggregateSignatures {

    private BuiltinAggregateSignatures() {
    }

    static final String[][] AGGREGATES = {
            {"array_agg", "2277", "2277"}, // array_agg(anyarray) -> anyarray
            {"array_agg", "2277", "2776"}, // array_agg(anynonarray) -> anyarray
            {"avg", "1186", "1186"}, // avg(interval) -> interval
            {"avg", "1700", "1700"}, // avg(numeric) -> numeric
            {"avg", "1700", "20"}, // avg(bigint) -> numeric
            {"avg", "1700", "21"}, // avg(smallint) -> numeric
            {"avg", "1700", "23"}, // avg(integer) -> numeric
            {"avg", "701", "700"}, // avg(real) -> double precision
            {"avg", "701", "701"}, // avg(double precision) -> double precision
            {"bit_and", "1560", "1560"}, // bit_and(bit) -> bit
            {"bit_and", "20", "20"}, // bit_and(bigint) -> bigint
            {"bit_and", "21", "21"}, // bit_and(smallint) -> smallint
            {"bit_and", "23", "23"}, // bit_and(integer) -> integer
            {"bit_or", "1560", "1560"}, // bit_or(bit) -> bit
            {"bit_or", "20", "20"}, // bit_or(bigint) -> bigint
            {"bit_or", "21", "21"}, // bit_or(smallint) -> smallint
            {"bit_or", "23", "23"}, // bit_or(integer) -> integer
            {"bit_xor", "1560", "1560"}, // bit_xor(bit) -> bit
            {"bit_xor", "20", "20"}, // bit_xor(bigint) -> bigint
            {"bit_xor", "21", "21"}, // bit_xor(smallint) -> smallint
            {"bit_xor", "23", "23"}, // bit_xor(integer) -> integer
            {"bool_and", "16", "16"}, // bool_and(boolean) -> boolean
            {"bool_or", "16", "16"}, // bool_or(boolean) -> boolean
            {"corr", "701", "701 701"}, // corr(double precision, double precision) -> double precision
            {"count", "20", ""}, // count() -> bigint
            {"count", "20", "2276"}, // count("any") -> bigint
            {"covar_pop", "701", "701 701"}, // covar_pop(double precision, double precision) -> double precision
            {"covar_samp", "701", "701 701"}, // covar_samp(double precision, double precision) -> double precision
            {"every", "16", "16"}, // every(boolean) -> boolean
            {"json_agg", "114", "2283"}, // json_agg(anyelement) -> json
            {"json_object_agg", "114", "2276 2276"}, // json_object_agg(key "any", value "any") -> json
            {"jsonb_agg", "3802", "2283"}, // jsonb_agg(anyelement) -> jsonb
            {"jsonb_object_agg", "3802", "2276 2276"}, // jsonb_object_agg(key "any", value "any") -> jsonb
            {"max", "1042", "1042"}, // max(character) -> character
            {"max", "1082", "1082"}, // max(date) -> date
            {"max", "1083", "1083"}, // max(time without time zone) -> time without time zone
            {"max", "1114", "1114"}, // max(timestamp without time zone) -> timestamp without time zone
            {"max", "1184", "1184"}, // max(timestamp with time zone) -> timestamp with time zone
            {"max", "1186", "1186"}, // max(interval) -> interval
            {"max", "1266", "1266"}, // max(time with time zone) -> time with time zone
            {"max", "17", "17"}, // max(bytea) -> bytea
            {"max", "1700", "1700"}, // max(numeric) -> numeric
            {"max", "20", "20"}, // max(bigint) -> bigint
            {"max", "21", "21"}, // max(smallint) -> smallint
            {"max", "2249", "2249"}, // max(record) -> record
            {"max", "2277", "2277"}, // max(anyarray) -> anyarray
            {"max", "23", "23"}, // max(integer) -> integer
            {"max", "25", "25"}, // max(text) -> text
            {"max", "26", "26"}, // max(oid) -> oid
            {"max", "27", "27"}, // max(tid) -> tid
            {"max", "3220", "3220"}, // max(pg_lsn) -> pg_lsn
            {"max", "3500", "3500"}, // max(anyenum) -> anyenum
            {"max", "5069", "5069"}, // max(xid8) -> xid8
            {"max", "700", "700"}, // max(real) -> real
            {"max", "701", "701"}, // max(double precision) -> double precision
            {"max", "790", "790"}, // max(money) -> money
            {"max", "869", "869"}, // max(inet) -> inet
            {"min", "1042", "1042"}, // min(character) -> character
            {"min", "1082", "1082"}, // min(date) -> date
            {"min", "1083", "1083"}, // min(time without time zone) -> time without time zone
            {"min", "1114", "1114"}, // min(timestamp without time zone) -> timestamp without time zone
            {"min", "1184", "1184"}, // min(timestamp with time zone) -> timestamp with time zone
            {"min", "1186", "1186"}, // min(interval) -> interval
            {"min", "1266", "1266"}, // min(time with time zone) -> time with time zone
            {"min", "17", "17"}, // min(bytea) -> bytea
            {"min", "1700", "1700"}, // min(numeric) -> numeric
            {"min", "20", "20"}, // min(bigint) -> bigint
            {"min", "21", "21"}, // min(smallint) -> smallint
            {"min", "2249", "2249"}, // min(record) -> record
            {"min", "2277", "2277"}, // min(anyarray) -> anyarray
            {"min", "23", "23"}, // min(integer) -> integer
            {"min", "25", "25"}, // min(text) -> text
            {"min", "26", "26"}, // min(oid) -> oid
            {"min", "27", "27"}, // min(tid) -> tid
            {"min", "3220", "3220"}, // min(pg_lsn) -> pg_lsn
            {"min", "3500", "3500"}, // min(anyenum) -> anyenum
            {"min", "5069", "5069"}, // min(xid8) -> xid8
            {"min", "700", "700"}, // min(real) -> real
            {"min", "701", "701"}, // min(double precision) -> double precision
            {"min", "790", "790"}, // min(money) -> money
            {"min", "869", "869"}, // min(inet) -> inet
            {"range_agg", "4537", "3831"}, // range_agg(anyrange) -> anymultirange
            {"range_agg", "4537", "4537"}, // range_agg(anymultirange) -> anymultirange
            {"range_intersect_agg", "3831", "3831"}, // range_intersect_agg(anyrange) -> anyrange
            {"range_intersect_agg", "4537", "4537"}, // range_intersect_agg(anymultirange) -> anymultirange
            {"stddev", "1700", "1700"}, // stddev(numeric) -> numeric
            {"stddev", "1700", "20"}, // stddev(bigint) -> numeric
            {"stddev", "1700", "21"}, // stddev(smallint) -> numeric
            {"stddev", "1700", "23"}, // stddev(integer) -> numeric
            {"stddev", "701", "700"}, // stddev(real) -> double precision
            {"stddev", "701", "701"}, // stddev(double precision) -> double precision
            {"stddev_pop", "1700", "1700"}, // stddev_pop(numeric) -> numeric
            {"stddev_pop", "1700", "20"}, // stddev_pop(bigint) -> numeric
            {"stddev_pop", "1700", "21"}, // stddev_pop(smallint) -> numeric
            {"stddev_pop", "1700", "23"}, // stddev_pop(integer) -> numeric
            {"stddev_pop", "701", "700"}, // stddev_pop(real) -> double precision
            {"stddev_pop", "701", "701"}, // stddev_pop(double precision) -> double precision
            {"stddev_samp", "1700", "1700"}, // stddev_samp(numeric) -> numeric
            {"stddev_samp", "1700", "20"}, // stddev_samp(bigint) -> numeric
            {"stddev_samp", "1700", "21"}, // stddev_samp(smallint) -> numeric
            {"stddev_samp", "1700", "23"}, // stddev_samp(integer) -> numeric
            {"stddev_samp", "701", "700"}, // stddev_samp(real) -> double precision
            {"stddev_samp", "701", "701"}, // stddev_samp(double precision) -> double precision
            {"string_agg", "17", "17 17"}, // string_agg(value bytea, delimiter bytea) -> bytea
            {"string_agg", "25", "25 25"}, // string_agg(value text, delimiter text) -> text
            {"sum", "1186", "1186"}, // sum(interval) -> interval
            {"sum", "1700", "1700"}, // sum(numeric) -> numeric
            {"sum", "1700", "20"}, // sum(bigint) -> numeric
            {"sum", "20", "21"}, // sum(smallint) -> bigint
            {"sum", "20", "23"}, // sum(integer) -> bigint
            {"sum", "700", "700"}, // sum(real) -> real
            {"sum", "701", "701"}, // sum(double precision) -> double precision
            {"sum", "790", "790"}, // sum(money) -> money
            {"var_pop", "1700", "1700"}, // var_pop(numeric) -> numeric
            {"var_pop", "1700", "20"}, // var_pop(bigint) -> numeric
            {"var_pop", "1700", "21"}, // var_pop(smallint) -> numeric
            {"var_pop", "1700", "23"}, // var_pop(integer) -> numeric
            {"var_pop", "701", "700"}, // var_pop(real) -> double precision
            {"var_pop", "701", "701"}, // var_pop(double precision) -> double precision
            {"var_samp", "1700", "1700"}, // var_samp(numeric) -> numeric
            {"var_samp", "1700", "20"}, // var_samp(bigint) -> numeric
            {"var_samp", "1700", "21"}, // var_samp(smallint) -> numeric
            {"var_samp", "1700", "23"}, // var_samp(integer) -> numeric
            {"var_samp", "701", "700"}, // var_samp(real) -> double precision
            {"var_samp", "701", "701"}, // var_samp(double precision) -> double precision
            {"variance", "1700", "1700"}, // variance(numeric) -> numeric
            {"variance", "1700", "20"}, // variance(bigint) -> numeric
            {"variance", "1700", "21"}, // variance(smallint) -> numeric
            {"variance", "1700", "23"}, // variance(integer) -> numeric
            {"variance", "701", "700"}, // variance(real) -> double precision
            {"variance", "701", "701"}, // variance(double precision) -> double precision
            {"xmlagg", "142", "142"}, // xmlagg(xml) -> xml
    };
}
