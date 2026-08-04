package com.memgres.engine;

/**
 * The casts PostgreSQL registers between the types memgres models.
 *
 * <p>pg_cast is not decoration: an implicit entry is what makes PG apply a conversion on its own,
 * and the *absence* of an entry is what makes {@code '5'::text = 5} an error rather than a
 * comparison. Listing a cast PG does not have therefore claims a conversion that changes what the
 * server accepts, so this table holds exactly what PostgreSQL 18 holds for these types — no more.
 *
 * <p>Columns: castsource, casttarget, castfunc (the function's name, empty when the conversion
 * needs none), castcontext ('i' implicit / 'a' assignment / 'e' explicit), castmethod
 * ('f' via function / 'b' binary-coercible / 'i' I/O conversion) and the cast's own OID. The
 * function is named rather than numbered so pg_cast.castfunc resolves against the pg_proc row
 * memgres actually assigns it, instead of pointing at an OID that exists on some other server.
 *
 * <p>The OID is PostgreSQL's own. A cast is referred to by OID and not by name — pg_depend and
 * pg_describe_object have no other handle on one — so numbering them from an unrelated counter
 * made every such reference mean a different cast here than there.
 */
final class PgCastTable {

    private PgCastTable() {
    }

    static final Object[][] CASTS = {
            {16, 23, "int4", "e", "f", 10035}, // bool -> int4
            {16, 25, "text", "a", "f", 10197}, // bool -> text
            {16, 1042, "text", "a", "f", 10207}, // bool -> bpchar
            {16, 1043, "text", "a", "f", 10202}, // bool -> varchar
            {17, 20, "int8", "e", "f", 10148}, // bytea -> int8
            {17, 21, "int2", "e", "f", 10146}, // bytea -> int2
            {17, 23, "int4", "e", "f", 10147}, // bytea -> int4
            {18, 23, "int4", "e", "f", 10149}, // char -> int4
            {18, 25, "text", "i", "f", 10131}, // char -> text
            {18, 1042, "bpchar", "a", "f", 10132}, // char -> bpchar
            {18, 1043, "text", "a", "f", 10133}, // char -> varchar
            {19, 25, "text", "i", "f", 10134}, // name -> text
            {19, 1042, "bpchar", "a", "f", 10135}, // name -> bpchar
            {19, 1043, "varchar", "a", "f", 10136}, // name -> varchar
            {20, 17, "bytea", "e", "f", 10145}, // int8 -> bytea
            {20, 21, "int2", "a", "f", 10000}, // int8 -> int2
            {20, 23, "int4", "a", "f", 10001}, // int8 -> int4
            {20, 24, "oid", "i", "f", 10044}, // int8 -> regproc
            {20, 26, "oid", "i", "f", 10037}, // int8 -> oid
            {20, 700, "float4", "i", "f", 10002}, // int8 -> float4
            {20, 701, "float8", "i", "f", 10003}, // int8 -> float8
            {20, 790, "money", "a", "f", 10033}, // int8 -> money
            {20, 1560, "bit", "e", "f", 10191}, // int8 -> bit
            {20, 1700, "numeric", "i", "f", 10004}, // int8 -> numeric
            {20, 2202, "oid", "i", "f", 10053}, // int8 -> regprocedure
            {20, 2203, "oid", "i", "f", 10060}, // int8 -> regoper
            {20, 2204, "oid", "i", "f", 10069}, // int8 -> regoperator
            {20, 2205, "oid", "i", "f", 10076}, // int8 -> regclass
            {20, 2206, "oid", "i", "f", 10090}, // int8 -> regtype
            {20, 3734, "oid", "i", "f", 10097}, // int8 -> regconfig
            {20, 3769, "oid", "i", "f", 10104}, // int8 -> regdictionary
            {20, 4089, "oid", "i", "f", 10120}, // int8 -> regnamespace
            {20, 4096, "oid", "i", "f", 10113}, // int8 -> regrole
            {20, 4191, "oid", "i", "f", 10083}, // int8 -> regcollation
            {21, 17, "bytea", "e", "f", 10143}, // int2 -> bytea
            {21, 20, "int8", "i", "f", 10005}, // int2 -> int8
            {21, 23, "int4", "i", "f", 10006}, // int2 -> int4
            {21, 24, "int4", "i", "f", 10045}, // int2 -> regproc
            {21, 26, "int4", "i", "f", 10038}, // int2 -> oid
            {21, 700, "float4", "i", "f", 10007}, // int2 -> float4
            {21, 701, "float8", "i", "f", 10008}, // int2 -> float8
            {21, 1700, "numeric", "i", "f", 10009}, // int2 -> numeric
            {21, 2202, "int4", "i", "f", 10054}, // int2 -> regprocedure
            {21, 2203, "int4", "i", "f", 10061}, // int2 -> regoper
            {21, 2204, "int4", "i", "f", 10070}, // int2 -> regoperator
            {21, 2205, "int4", "i", "f", 10077}, // int2 -> regclass
            {21, 2206, "int4", "i", "f", 10091}, // int2 -> regtype
            {21, 3734, "int4", "i", "f", 10098}, // int2 -> regconfig
            {21, 3769, "int4", "i", "f", 10105}, // int2 -> regdictionary
            {21, 4089, "int4", "i", "f", 10121}, // int2 -> regnamespace
            {21, 4096, "int4", "i", "f", 10114}, // int2 -> regrole
            {21, 4191, "int4", "i", "f", 10084}, // int2 -> regcollation
            {23, 16, "bool", "e", "f", 10034}, // int4 -> bool
            {23, 17, "bytea", "e", "f", 10144}, // int4 -> bytea
            {23, 18, "char", "e", "f", 10150}, // int4 -> char
            {23, 20, "int8", "i", "f", 10010}, // int4 -> int8
            {23, 21, "int2", "a", "f", 10011}, // int4 -> int2
            {23, 24, "", "i", "b", 10046}, // int4 -> regproc
            {23, 26, "", "i", "b", 10039}, // int4 -> oid
            {23, 700, "float4", "i", "f", 10012}, // int4 -> float4
            {23, 701, "float8", "i", "f", 10013}, // int4 -> float8
            {23, 790, "money", "a", "f", 10032}, // int4 -> money
            {23, 1560, "bit", "e", "f", 10192}, // int4 -> bit
            {23, 1700, "numeric", "i", "f", 10014}, // int4 -> numeric
            {23, 2202, "", "i", "b", 10055}, // int4 -> regprocedure
            {23, 2203, "", "i", "b", 10062}, // int4 -> regoper
            {23, 2204, "", "i", "b", 10071}, // int4 -> regoperator
            {23, 2205, "", "i", "b", 10078}, // int4 -> regclass
            {23, 2206, "", "i", "b", 10092}, // int4 -> regtype
            {23, 3734, "", "i", "b", 10099}, // int4 -> regconfig
            {23, 3769, "", "i", "b", 10106}, // int4 -> regdictionary
            {23, 4089, "", "i", "b", 10122}, // int4 -> regnamespace
            {23, 4096, "", "i", "b", 10115}, // int4 -> regrole
            {23, 4191, "", "i", "b", 10085}, // int4 -> regcollation
            {24, 20, "int8", "a", "f", 10047}, // regproc -> int8
            {24, 23, "", "a", "b", 10048}, // regproc -> int4
            {24, 26, "", "i", "b", 10043}, // regproc -> oid
            {24, 2202, "", "i", "b", 10049}, // regproc -> regprocedure
            {25, 18, "char", "a", "f", 10137}, // text -> char
            {25, 19, "name", "i", "f", 10140}, // text -> name
            {25, 142, "xml", "e", "f", 10199}, // text -> xml
            {25, 1042, "", "i", "b", 10125}, // text -> bpchar
            {25, 1043, "", "i", "b", 10126}, // text -> varchar
            {25, 2205, "regclass", "i", "f", 10109}, // text -> regclass
            {26, 20, "int8", "a", "f", 10040}, // oid -> int8
            {26, 23, "", "a", "b", 10041}, // oid -> int4
            {26, 24, "", "i", "b", 10042}, // oid -> regproc
            {26, 2202, "", "i", "b", 10051}, // oid -> regprocedure
            {26, 2203, "", "i", "b", 10058}, // oid -> regoper
            {26, 2204, "", "i", "b", 10067}, // oid -> regoperator
            {26, 2205, "", "i", "b", 10074}, // oid -> regclass
            {26, 2206, "", "i", "b", 10088}, // oid -> regtype
            {26, 3734, "", "i", "b", 10095}, // oid -> regconfig
            {26, 3769, "", "i", "b", 10102}, // oid -> regdictionary
            {26, 4089, "", "i", "b", 10118}, // oid -> regnamespace
            {26, 4096, "", "i", "b", 10111}, // oid -> regrole
            {26, 4191, "", "i", "b", 10081}, // oid -> regcollation
            {114, 3802, "", "a", "i", 10220}, // json -> jsonb
            {142, 25, "", "a", "b", 10198}, // xml -> text
            {142, 1042, "", "a", "b", 10208}, // xml -> bpchar
            {142, 1043, "", "a", "b", 10203}, // xml -> varchar
            {194, 25, "", "i", "b", 10151}, // pg_node_tree -> text
            {600, 603, "box", "a", "f", 10171}, // point -> box
            {601, 600, "point", "e", "f", 10172}, // lseg -> point
            {602, 604, "polygon", "a", "f", 10173}, // path -> polygon
            {603, 600, "point", "e", "f", 10174}, // box -> point
            {603, 601, "lseg", "e", "f", 10175}, // box -> lseg
            {603, 604, "polygon", "a", "f", 10176}, // box -> polygon
            {603, 718, "circle", "e", "f", 10177}, // box -> circle
            {604, 600, "point", "e", "f", 10178}, // polygon -> point
            {604, 602, "path", "a", "f", 10179}, // polygon -> path
            {604, 603, "box", "e", "f", 10180}, // polygon -> box
            {604, 718, "circle", "e", "f", 10181}, // polygon -> circle
            {650, 25, "text", "a", "f", 10195}, // cidr -> text
            {650, 869, "", "i", "b", 10187}, // cidr -> inet
            {650, 1042, "text", "a", "f", 10205}, // cidr -> bpchar
            {650, 1043, "text", "a", "f", 10200}, // cidr -> varchar
            {700, 20, "int8", "a", "f", 10015}, // float4 -> int8
            {700, 21, "int2", "a", "f", 10016}, // float4 -> int2
            {700, 23, "int4", "a", "f", 10017}, // float4 -> int4
            {700, 701, "float8", "i", "f", 10018}, // float4 -> float8
            {700, 1700, "numeric", "a", "f", 10019}, // float4 -> numeric
            {701, 20, "int8", "a", "f", 10020}, // float8 -> int8
            {701, 21, "int2", "a", "f", 10021}, // float8 -> int2
            {701, 23, "int4", "a", "f", 10022}, // float8 -> int4
            {701, 700, "float4", "a", "f", 10023}, // float8 -> float4
            {701, 1700, "numeric", "a", "f", 10024}, // float8 -> numeric
            {718, 600, "point", "e", "f", 10182}, // circle -> point
            {718, 603, "box", "e", "f", 10183}, // circle -> box
            {718, 604, "polygon", "e", "f", 10184}, // circle -> polygon
            {774, 829, "macaddr", "i", "f", 10186}, // macaddr8 -> macaddr
            {790, 1700, "numeric", "a", "f", 10030}, // money -> numeric
            {829, 774, "macaddr8", "i", "f", 10185}, // macaddr -> macaddr8
            {869, 25, "text", "a", "f", 10196}, // inet -> text
            {869, 650, "cidr", "a", "f", 10188}, // inet -> cidr
            {869, 1042, "text", "a", "f", 10206}, // inet -> bpchar
            {869, 1043, "text", "a", "f", 10201}, // inet -> varchar
            {1042, 18, "char", "a", "f", 10138}, // bpchar -> char
            {1042, 19, "name", "i", "f", 10141}, // bpchar -> name
            {1042, 25, "text", "i", "f", 10127}, // bpchar -> text
            {1042, 142, "xml", "e", "f", 10209}, // bpchar -> xml
            {1042, 1042, "bpchar", "i", "f", 10210}, // bpchar -> bpchar
            {1042, 1043, "text", "i", "f", 10128}, // bpchar -> varchar
            {1043, 18, "char", "a", "f", 10139}, // varchar -> char
            {1043, 19, "name", "i", "f", 10142}, // varchar -> name
            {1043, 25, "", "i", "b", 10129}, // varchar -> text
            {1043, 142, "xml", "e", "f", 10204}, // varchar -> xml
            {1043, 1042, "", "i", "b", 10130}, // varchar -> bpchar
            {1043, 1043, "varchar", "i", "f", 10211}, // varchar -> varchar
            {1043, 2205, "regclass", "i", "f", 10110}, // varchar -> regclass
            {1082, 1114, "timestamp", "i", "f", 10158}, // date -> timestamp
            {1082, 1184, "timestamptz", "i", "f", 10159}, // date -> timestamptz
            {1083, 1083, "time", "i", "f", 10212}, // time -> time
            {1083, 1186, "interval", "i", "f", 10160}, // time -> interval
            {1083, 1266, "timetz", "i", "f", 10161}, // time -> timetz
            {1114, 1082, "date", "a", "f", 10162}, // timestamp -> date
            {1114, 1083, "time", "a", "f", 10163}, // timestamp -> time
            {1114, 1114, "timestamp", "i", "f", 10213}, // timestamp -> timestamp
            {1114, 1184, "timestamptz", "i", "f", 10164}, // timestamp -> timestamptz
            {1184, 1082, "date", "a", "f", 10165}, // timestamptz -> date
            {1184, 1083, "time", "a", "f", 10166}, // timestamptz -> time
            {1184, 1114, "timestamp", "a", "f", 10167}, // timestamptz -> timestamp
            {1184, 1184, "timestamptz", "i", "f", 10214}, // timestamptz -> timestamptz
            {1184, 1266, "timetz", "a", "f", 10168}, // timestamptz -> timetz
            {1186, 1083, "time", "a", "f", 10169}, // interval -> time
            {1186, 1186, "interval", "i", "f", 10215}, // interval -> interval
            {1266, 1083, "time", "a", "f", 10170}, // timetz -> time
            {1266, 1266, "timetz", "i", "f", 10216}, // timetz -> timetz
            {1560, 20, "int8", "e", "f", 10193}, // bit -> int8
            {1560, 23, "int4", "e", "f", 10194}, // bit -> int4
            {1560, 1560, "bit", "i", "f", 10217}, // bit -> bit
            {1560, 1562, "", "i", "b", 10189}, // bit -> varbit
            {1562, 1560, "", "i", "b", 10190}, // varbit -> bit
            {1562, 1562, "varbit", "i", "f", 10218}, // varbit -> varbit
            {1700, 20, "int8", "a", "f", 10025}, // numeric -> int8
            {1700, 21, "int2", "a", "f", 10026}, // numeric -> int2
            {1700, 23, "int4", "a", "f", 10027}, // numeric -> int4
            {1700, 700, "float4", "i", "f", 10028}, // numeric -> float4
            {1700, 701, "float8", "i", "f", 10029}, // numeric -> float8
            {1700, 790, "money", "a", "f", 10031}, // numeric -> money
            {1700, 1700, "numeric", "i", "f", 10219}, // numeric -> numeric
            {2202, 20, "int8", "a", "f", 10056}, // regprocedure -> int8
            {2202, 23, "", "a", "b", 10057}, // regprocedure -> int4
            {2202, 24, "", "i", "b", 10050}, // regprocedure -> regproc
            {2202, 26, "", "i", "b", 10052}, // regprocedure -> oid
            {2203, 20, "int8", "a", "f", 10063}, // regoper -> int8
            {2203, 23, "", "a", "b", 10064}, // regoper -> int4
            {2203, 26, "", "i", "b", 10059}, // regoper -> oid
            {2203, 2204, "", "i", "b", 10065}, // regoper -> regoperator
            {2204, 20, "int8", "a", "f", 10072}, // regoperator -> int8
            {2204, 23, "", "a", "b", 10073}, // regoperator -> int4
            {2204, 26, "", "i", "b", 10068}, // regoperator -> oid
            {2204, 2203, "", "i", "b", 10066}, // regoperator -> regoper
            {2205, 20, "int8", "a", "f", 10079}, // regclass -> int8
            {2205, 23, "", "a", "b", 10080}, // regclass -> int4
            {2205, 26, "", "i", "b", 10075}, // regclass -> oid
            {2206, 20, "int8", "a", "f", 10093}, // regtype -> int8
            {2206, 23, "", "a", "b", 10094}, // regtype -> int4
            {2206, 26, "", "i", "b", 10089}, // regtype -> oid
            {3361, 17, "", "i", "b", 10152}, // pg_ndistinct -> bytea
            {3361, 25, "", "i", "i", 10153}, // pg_ndistinct -> text
            {3402, 17, "", "i", "b", 10154}, // pg_dependencies -> bytea
            {3402, 25, "", "i", "i", 10155}, // pg_dependencies -> text
            {3734, 20, "int8", "a", "f", 10100}, // regconfig -> int8
            {3734, 23, "", "a", "b", 10101}, // regconfig -> int4
            {3734, 26, "", "i", "b", 10096}, // regconfig -> oid
            {3769, 20, "int8", "a", "f", 10107}, // regdictionary -> int8
            {3769, 23, "", "a", "b", 10108}, // regdictionary -> int4
            {3769, 26, "", "i", "b", 10103}, // regdictionary -> oid
            {3802, 16, "bool", "e", "f", 10222}, // jsonb -> bool
            {3802, 20, "int8", "e", "f", 10226}, // jsonb -> int8
            {3802, 21, "int2", "e", "f", 10224}, // jsonb -> int2
            {3802, 23, "int4", "e", "f", 10225}, // jsonb -> int4
            {3802, 114, "", "a", "i", 10221}, // jsonb -> json
            {3802, 700, "float4", "e", "f", 10227}, // jsonb -> float4
            {3802, 701, "float8", "e", "f", 10228}, // jsonb -> float8
            {3802, 1700, "numeric", "e", "f", 10223}, // jsonb -> numeric
            {3904, 4451, "int4multirange", "e", "f", 10229}, // int4range -> int4multirange
            {3906, 4532, "nummultirange", "e", "f", 10231}, // numrange -> nummultirange
            {3908, 4533, "tsmultirange", "e", "f", 10233}, // tsrange -> tsmultirange
            {3910, 4534, "tstzmultirange", "e", "f", 10234}, // tstzrange -> tstzmultirange
            {3912, 4535, "datemultirange", "e", "f", 10232}, // daterange -> datemultirange
            {3926, 4536, "int8multirange", "e", "f", 10230}, // int8range -> int8multirange
            {4089, 20, "int8", "a", "f", 10123}, // regnamespace -> int8
            {4089, 23, "", "a", "b", 10124}, // regnamespace -> int4
            {4089, 26, "", "i", "b", 10119}, // regnamespace -> oid
            {4096, 20, "int8", "a", "f", 10116}, // regrole -> int8
            {4096, 23, "", "a", "b", 10117}, // regrole -> int4
            {4096, 26, "", "i", "b", 10112}, // regrole -> oid
            {4191, 20, "int8", "a", "f", 10086}, // regcollation -> int8
            {4191, 23, "", "a", "b", 10087}, // regcollation -> int4
            {4191, 26, "", "i", "b", 10082}, // regcollation -> oid
            {5017, 17, "", "i", "b", 10156}, // pg_mcv_list -> bytea
            {5017, 25, "", "i", "i", 10157}, // pg_mcv_list -> text
            {5069, 28, "xid", "e", "f", 10036}, // xid8 -> xid
    };
}
