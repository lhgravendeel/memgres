package com.memgres.engine;

/**
 * The casts PostgreSQL registers between the types memgres models.
 *
 * <p>pg_cast is not decoration: an implicit entry is what makes PG apply a conversion on its own,
 * and the *absence* of an entry is what makes {@code '5'::text = 5} an error rather than a
 * comparison. Listing a cast PG does not have therefore claims a conversion that changes what the
 * server accepts, so this table holds exactly what PostgreSQL 18 holds for these types — no more.
 *
 * <p>Columns: castsource, casttarget, castfunc (0 when the conversion needs no function),
 * castcontext ('i' implicit / 'a' assignment / 'e' explicit) and castmethod ('f' via function /
 * 'b' binary-coercible / 'i' I/O conversion).
 */
final class PgCastTable {

    private PgCastTable() {
    }

    static final Object[][] CASTS = {
            {16, 23, 2558, "e", "f"}, // boolean -> integer
            {16, 25, 2971, "a", "f"}, // boolean -> text
            {16, 1042, 2971, "a", "f"}, // boolean -> character
            {16, 1043, 2971, "a", "f"}, // boolean -> character varying
            {17, 20, 6372, "e", "f"}, // bytea -> bigint
            {17, 21, 6370, "e", "f"}, // bytea -> smallint
            {17, 23, 6371, "e", "f"}, // bytea -> integer
            {18, 23, 77, "e", "f"}, // "char" -> integer
            {18, 25, 946, "i", "f"}, // "char" -> text
            {18, 1042, 860, "a", "f"}, // "char" -> character
            {18, 1043, 946, "a", "f"}, // "char" -> character varying
            {19, 25, 406, "i", "f"}, // name -> text
            {19, 1042, 408, "a", "f"}, // name -> character
            {19, 1043, 1401, "a", "f"}, // name -> character varying
            {20, 17, 6369, "e", "f"}, // bigint -> bytea
            {20, 21, 714, "a", "f"}, // bigint -> smallint
            {20, 23, 480, "a", "f"}, // bigint -> integer
            {20, 24, 1287, "i", "f"}, // bigint -> regproc
            {20, 26, 1287, "i", "f"}, // bigint -> oid
            {20, 700, 652, "i", "f"}, // bigint -> real
            {20, 701, 482, "i", "f"}, // bigint -> double precision
            {20, 790, 3812, "a", "f"}, // bigint -> money
            {20, 1560, 2075, "e", "f"}, // bigint -> bit
            {20, 1700, 1781, "i", "f"}, // bigint -> numeric
            {20, 2202, 1287, "i", "f"}, // bigint -> regprocedure
            {20, 2203, 1287, "i", "f"}, // bigint -> regoper
            {20, 2204, 1287, "i", "f"}, // bigint -> regoperator
            {20, 2205, 1287, "i", "f"}, // bigint -> regclass
            {20, 2206, 1287, "i", "f"}, // bigint -> regtype
            {20, 3734, 1287, "i", "f"}, // bigint -> regconfig
            {20, 3769, 1287, "i", "f"}, // bigint -> regdictionary
            {20, 4089, 1287, "i", "f"}, // bigint -> regnamespace
            {20, 4096, 1287, "i", "f"}, // bigint -> regrole
            {21, 17, 6367, "e", "f"}, // smallint -> bytea
            {21, 20, 754, "i", "f"}, // smallint -> bigint
            {21, 23, 313, "i", "f"}, // smallint -> integer
            {21, 24, 313, "i", "f"}, // smallint -> regproc
            {21, 26, 313, "i", "f"}, // smallint -> oid
            {21, 700, 236, "i", "f"}, // smallint -> real
            {21, 701, 235, "i", "f"}, // smallint -> double precision
            {21, 1700, 1782, "i", "f"}, // smallint -> numeric
            {21, 2202, 313, "i", "f"}, // smallint -> regprocedure
            {21, 2203, 313, "i", "f"}, // smallint -> regoper
            {21, 2204, 313, "i", "f"}, // smallint -> regoperator
            {21, 2205, 313, "i", "f"}, // smallint -> regclass
            {21, 2206, 313, "i", "f"}, // smallint -> regtype
            {21, 3734, 313, "i", "f"}, // smallint -> regconfig
            {21, 3769, 313, "i", "f"}, // smallint -> regdictionary
            {21, 4089, 313, "i", "f"}, // smallint -> regnamespace
            {21, 4096, 313, "i", "f"}, // smallint -> regrole
            {23, 16, 2557, "e", "f"}, // integer -> boolean
            {23, 17, 6368, "e", "f"}, // integer -> bytea
            {23, 18, 78, "e", "f"}, // integer -> "char"
            {23, 20, 481, "i", "f"}, // integer -> bigint
            {23, 21, 314, "a", "f"}, // integer -> smallint
            {23, 24, 0, "i", "b"}, // integer -> regproc
            {23, 26, 0, "i", "b"}, // integer -> oid
            {23, 700, 318, "i", "f"}, // integer -> real
            {23, 701, 316, "i", "f"}, // integer -> double precision
            {23, 790, 3811, "a", "f"}, // integer -> money
            {23, 1560, 1683, "e", "f"}, // integer -> bit
            {23, 1700, 1740, "i", "f"}, // integer -> numeric
            {23, 2202, 0, "i", "b"}, // integer -> regprocedure
            {23, 2203, 0, "i", "b"}, // integer -> regoper
            {23, 2204, 0, "i", "b"}, // integer -> regoperator
            {23, 2205, 0, "i", "b"}, // integer -> regclass
            {23, 2206, 0, "i", "b"}, // integer -> regtype
            {23, 3734, 0, "i", "b"}, // integer -> regconfig
            {23, 3769, 0, "i", "b"}, // integer -> regdictionary
            {23, 4089, 0, "i", "b"}, // integer -> regnamespace
            {23, 4096, 0, "i", "b"}, // integer -> regrole
            {24, 20, 1288, "a", "f"}, // regproc -> bigint
            {24, 23, 0, "a", "b"}, // regproc -> integer
            {24, 26, 0, "i", "b"}, // regproc -> oid
            {24, 2202, 0, "i", "b"}, // regproc -> regprocedure
            {25, 18, 944, "a", "f"}, // text -> "char"
            {25, 19, 407, "i", "f"}, // text -> name
            {25, 142, 2896, "e", "f"}, // text -> xml
            {25, 1042, 0, "i", "b"}, // text -> character
            {25, 1043, 0, "i", "b"}, // text -> character varying
            {25, 2205, 1079, "i", "f"}, // text -> regclass
            {26, 20, 1288, "a", "f"}, // oid -> bigint
            {26, 23, 0, "a", "b"}, // oid -> integer
            {26, 24, 0, "i", "b"}, // oid -> regproc
            {26, 2202, 0, "i", "b"}, // oid -> regprocedure
            {26, 2203, 0, "i", "b"}, // oid -> regoper
            {26, 2204, 0, "i", "b"}, // oid -> regoperator
            {26, 2205, 0, "i", "b"}, // oid -> regclass
            {26, 2206, 0, "i", "b"}, // oid -> regtype
            {26, 3734, 0, "i", "b"}, // oid -> regconfig
            {26, 3769, 0, "i", "b"}, // oid -> regdictionary
            {26, 4089, 0, "i", "b"}, // oid -> regnamespace
            {26, 4096, 0, "i", "b"}, // oid -> regrole
            {114, 3802, 0, "a", "i"}, // json -> jsonb
            {142, 25, 0, "a", "b"}, // xml -> text
            {142, 1042, 0, "a", "b"}, // xml -> character
            {142, 1043, 0, "a", "b"}, // xml -> character varying
            {600, 603, 4091, "a", "f"}, // point -> box
            {601, 600, 1532, "e", "f"}, // lseg -> point
            {602, 604, 1449, "a", "f"}, // path -> polygon
            {603, 600, 1534, "e", "f"}, // box -> point
            {603, 601, 1541, "e", "f"}, // box -> lseg
            {603, 604, 1448, "a", "f"}, // box -> polygon
            {603, 718, 1479, "e", "f"}, // box -> circle
            {604, 600, 1540, "e", "f"}, // polygon -> point
            {604, 602, 1447, "a", "f"}, // polygon -> path
            {604, 603, 1446, "e", "f"}, // polygon -> box
            {604, 718, 1474, "e", "f"}, // polygon -> circle
            {650, 25, 730, "a", "f"}, // cidr -> text
            {650, 869, 0, "i", "b"}, // cidr -> inet
            {650, 1042, 730, "a", "f"}, // cidr -> character
            {650, 1043, 730, "a", "f"}, // cidr -> character varying
            {700, 20, 653, "a", "f"}, // real -> bigint
            {700, 21, 238, "a", "f"}, // real -> smallint
            {700, 23, 319, "a", "f"}, // real -> integer
            {700, 701, 311, "i", "f"}, // real -> double precision
            {700, 1700, 1742, "a", "f"}, // real -> numeric
            {701, 20, 483, "a", "f"}, // double precision -> bigint
            {701, 21, 237, "a", "f"}, // double precision -> smallint
            {701, 23, 317, "a", "f"}, // double precision -> integer
            {701, 700, 312, "a", "f"}, // double precision -> real
            {701, 1700, 1743, "a", "f"}, // double precision -> numeric
            {718, 600, 1416, "e", "f"}, // circle -> point
            {718, 603, 1480, "e", "f"}, // circle -> box
            {718, 604, 1544, "e", "f"}, // circle -> polygon
            {774, 829, 4124, "i", "f"}, // macaddr8 -> macaddr
            {790, 1700, 3823, "a", "f"}, // money -> numeric
            {829, 774, 4123, "i", "f"}, // macaddr -> macaddr8
            {869, 25, 730, "a", "f"}, // inet -> text
            {869, 650, 1715, "a", "f"}, // inet -> cidr
            {869, 1042, 730, "a", "f"}, // inet -> character
            {869, 1043, 730, "a", "f"}, // inet -> character varying
            {1042, 18, 944, "a", "f"}, // character -> "char"
            {1042, 19, 409, "i", "f"}, // character -> name
            {1042, 25, 401, "i", "f"}, // character -> text
            {1042, 142, 2896, "e", "f"}, // character -> xml
            {1042, 1042, 668, "i", "f"}, // character -> character
            {1042, 1043, 401, "i", "f"}, // character -> character varying
            {1043, 18, 944, "a", "f"}, // character varying -> "char"
            {1043, 19, 1400, "i", "f"}, // character varying -> name
            {1043, 25, 0, "i", "b"}, // character varying -> text
            {1043, 142, 2896, "e", "f"}, // character varying -> xml
            {1043, 1042, 0, "i", "b"}, // character varying -> character
            {1043, 1043, 669, "i", "f"}, // character varying -> character varying
            {1043, 2205, 1079, "i", "f"}, // character varying -> regclass
            {1082, 1114, 2024, "i", "f"}, // date -> timestamp without time zone
            {1082, 1184, 1174, "i", "f"}, // date -> timestamp with time zone
            {1083, 1083, 1968, "i", "f"}, // time without time zone -> time without time zone
            {1083, 1186, 1370, "i", "f"}, // time without time zone -> interval
            {1083, 1266, 2047, "i", "f"}, // time without time zone -> time with time zone
            {1114, 1082, 2029, "a", "f"}, // timestamp without time zone -> date
            {1114, 1083, 1316, "a", "f"}, // timestamp without time zone -> time without time zone
            {1114, 1114, 1961, "i", "f"}, // timestamp without time zone -> timestamp without time zone
            {1114, 1184, 2028, "i", "f"}, // timestamp without time zone -> timestamp with time zone
            {1184, 1082, 1178, "a", "f"}, // timestamp with time zone -> date
            {1184, 1083, 2019, "a", "f"}, // timestamp with time zone -> time without time zone
            {1184, 1114, 2027, "a", "f"}, // timestamp with time zone -> timestamp without time zone
            {1184, 1184, 1967, "i", "f"}, // timestamp with time zone -> timestamp with time zone
            {1184, 1266, 1388, "a", "f"}, // timestamp with time zone -> time with time zone
            {1186, 1083, 1419, "a", "f"}, // interval -> time without time zone
            {1186, 1186, 1200, "i", "f"}, // interval -> interval
            {1266, 1083, 2046, "a", "f"}, // time with time zone -> time without time zone
            {1266, 1266, 1969, "i", "f"}, // time with time zone -> time with time zone
            {1560, 20, 2076, "e", "f"}, // bit -> bigint
            {1560, 23, 1684, "e", "f"}, // bit -> integer
            {1560, 1560, 1685, "i", "f"}, // bit -> bit
            {1560, 1562, 0, "i", "b"}, // bit -> bit varying
            {1562, 1560, 0, "i", "b"}, // bit varying -> bit
            {1562, 1562, 1687, "i", "f"}, // bit varying -> bit varying
            {1700, 20, 1779, "a", "f"}, // numeric -> bigint
            {1700, 21, 1783, "a", "f"}, // numeric -> smallint
            {1700, 23, 1744, "a", "f"}, // numeric -> integer
            {1700, 700, 1745, "i", "f"}, // numeric -> real
            {1700, 701, 1746, "i", "f"}, // numeric -> double precision
            {1700, 790, 3824, "a", "f"}, // numeric -> money
            {1700, 1700, 1703, "i", "f"}, // numeric -> numeric
            {2202, 20, 1288, "a", "f"}, // regprocedure -> bigint
            {2202, 23, 0, "a", "b"}, // regprocedure -> integer
            {2202, 24, 0, "i", "b"}, // regprocedure -> regproc
            {2202, 26, 0, "i", "b"}, // regprocedure -> oid
            {2203, 20, 1288, "a", "f"}, // regoper -> bigint
            {2203, 23, 0, "a", "b"}, // regoper -> integer
            {2203, 26, 0, "i", "b"}, // regoper -> oid
            {2203, 2204, 0, "i", "b"}, // regoper -> regoperator
            {2204, 20, 1288, "a", "f"}, // regoperator -> bigint
            {2204, 23, 0, "a", "b"}, // regoperator -> integer
            {2204, 26, 0, "i", "b"}, // regoperator -> oid
            {2204, 2203, 0, "i", "b"}, // regoperator -> regoper
            {2205, 20, 1288, "a", "f"}, // regclass -> bigint
            {2205, 23, 0, "a", "b"}, // regclass -> integer
            {2205, 26, 0, "i", "b"}, // regclass -> oid
            {2206, 20, 1288, "a", "f"}, // regtype -> bigint
            {2206, 23, 0, "a", "b"}, // regtype -> integer
            {2206, 26, 0, "i", "b"}, // regtype -> oid
            {3734, 20, 1288, "a", "f"}, // regconfig -> bigint
            {3734, 23, 0, "a", "b"}, // regconfig -> integer
            {3734, 26, 0, "i", "b"}, // regconfig -> oid
            {3769, 20, 1288, "a", "f"}, // regdictionary -> bigint
            {3769, 23, 0, "a", "b"}, // regdictionary -> integer
            {3769, 26, 0, "i", "b"}, // regdictionary -> oid
            {3802, 16, 3556, "e", "f"}, // jsonb -> boolean
            {3802, 20, 3452, "e", "f"}, // jsonb -> bigint
            {3802, 21, 3450, "e", "f"}, // jsonb -> smallint
            {3802, 23, 3451, "e", "f"}, // jsonb -> integer
            {3802, 114, 0, "a", "i"}, // jsonb -> json
            {3802, 700, 3453, "e", "f"}, // jsonb -> real
            {3802, 701, 2580, "e", "f"}, // jsonb -> double precision
            {3802, 1700, 3449, "e", "f"}, // jsonb -> numeric
            {3904, 4451, 4281, "e", "f"}, // int4range -> int4multirange
            {3906, 4532, 4284, "e", "f"}, // numrange -> nummultirange
            {3908, 4533, 4287, "e", "f"}, // tsrange -> tsmultirange
            {3910, 4534, 4290, "e", "f"}, // tstzrange -> tstzmultirange
            {3912, 4535, 4293, "e", "f"}, // daterange -> datemultirange
            {3926, 4536, 4296, "e", "f"}, // int8range -> int8multirange
            {4089, 20, 1288, "a", "f"}, // regnamespace -> bigint
            {4089, 23, 0, "a", "b"}, // regnamespace -> integer
            {4089, 26, 0, "i", "b"}, // regnamespace -> oid
            {4096, 20, 1288, "a", "f"}, // regrole -> bigint
            {4096, 23, 0, "a", "b"}, // regrole -> integer
            {4096, 26, 0, "i", "b"}, // regrole -> oid
    };
}
