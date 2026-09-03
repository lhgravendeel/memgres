package com.memgres.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Every word the grammar keeps for itself, and what each one may still be used as.
 *
 * <p>This is what {@code pg_get_keywords()} reports. The parser's own list holds only the words it
 * has to tell apart to parse — the four categories are what decide where a word may stand — but a
 * client asking the server which words it must quote is asking about all of them, so the whole set
 * is recorded here, measured from PostgreSQL 18.
 *
 * <p>Each entry is {@code word|catcode|barelabel}: U unreserved, C unreserved but no function or
 * type name, T reserved but may be one, R reserved.
 */
public final class PgKeywordTable {
    private PgKeywordTable() {}

    /** One row of the report. */
    static final class Keyword {
        final String word;
        final String category;
        final boolean bareLabel;

        Keyword(String word, String category, boolean bareLabel) {
            this.word = word;
            this.category = category;
            this.bareLabel = bareLabel;
        }

        /** What the category letter stands for, spelled the way the report spells it. */
        String categoryDescription() {
            if ("R".equals(category)) return "reserved";
            if ("T".equals(category)) return "reserved (can be function or type name)";
            if ("C".equals(category)) return "unreserved (cannot be function or type name)";
            return "unreserved";
        }

        /** Whether the word may stand alone as a column label, without AS in front of it. */
        String bareLabelDescription() {
            return bareLabel ? "can be bare label" : "requires AS";
        }
    }

    private static final String[] MEASURED = {
            "abort|U|t", "absent|U|t", "absolute|U|t", "access|U|t",
            "action|U|t", "add|U|t", "admin|U|t", "after|U|t",
            "aggregate|U|t", "all|R|t", "also|U|t", "alter|U|t",
            "always|U|t", "analyse|R|t", "analyze|R|t", "and|R|t",
            "any|R|t", "array|R|f", "as|R|f", "asc|R|t",
            "asensitive|U|t", "assertion|U|t", "assignment|U|t", "asymmetric|R|t",
            "at|U|t", "atomic|U|t", "attach|U|t", "attribute|U|t",
            "authorization|T|t", "backward|U|t", "before|U|t", "begin|U|t",
            "between|C|t", "bigint|C|t", "binary|T|t", "bit|C|t",
            "boolean|C|t", "both|R|t", "breadth|U|t", "by|U|t",
            "cache|U|t", "call|U|t", "called|U|t", "cascade|U|t",
            "cascaded|U|t", "case|R|t", "cast|R|t", "catalog|U|t",
            "chain|U|t", "char|C|f", "character|C|f", "characteristics|U|t",
            "check|R|t", "checkpoint|U|t", "class|U|t", "close|U|t",
            "cluster|U|t", "coalesce|C|t", "collate|R|t", "collation|T|t",
            "column|R|t", "columns|U|t", "comment|U|t", "comments|U|t",
            "commit|U|t", "committed|U|t", "compression|U|t", "concurrently|T|t",
            "conditional|U|t", "configuration|U|t", "conflict|U|t", "connection|U|t",
            "constraint|R|t", "constraints|U|t", "content|U|t", "continue|U|t",
            "conversion|U|t", "copy|U|t", "cost|U|t", "create|R|f",
            "cross|T|t", "csv|U|t", "cube|U|t", "current|U|t",
            "current_catalog|R|t", "current_date|R|t", "current_role|R|t", "current_schema|T|t",
            "current_time|R|t", "current_timestamp|R|t", "current_user|R|t", "cursor|U|t",
            "cycle|U|t", "data|U|t", "database|U|t", "day|U|f",
            "deallocate|U|t", "dec|C|t", "decimal|C|t", "declare|U|t",
            "default|R|t", "defaults|U|t", "deferrable|R|t", "deferred|U|t",
            "definer|U|t", "delete|U|t", "delimiter|U|t", "delimiters|U|t",
            "depends|U|t", "depth|U|t", "desc|R|t", "detach|U|t",
            "dictionary|U|t", "disable|U|t", "discard|U|t", "distinct|R|t",
            "do|R|t", "document|U|t", "domain|U|t", "double|U|t",
            "drop|U|t", "each|U|t", "else|R|t", "empty|U|t",
            "enable|U|t", "encoding|U|t", "encrypted|U|t", "end|R|t",
            "enforced|U|t", "enum|U|t", "error|U|t", "escape|U|t",
            "event|U|t", "except|R|f", "exclude|U|t", "excluding|U|t",
            "exclusive|U|t", "execute|U|t", "exists|C|t", "explain|U|t",
            "expression|U|t", "extension|U|t", "external|U|t", "extract|C|t",
            "false|R|t", "family|U|t", "fetch|R|f", "filter|U|f",
            "finalize|U|t", "first|U|t", "float|C|t", "following|U|t",
            "for|R|f", "force|U|t", "foreign|R|t", "format|U|t",
            "forward|U|t", "freeze|T|t", "from|R|f", "full|T|t",
            "function|U|t", "functions|U|t", "generated|U|t", "global|U|t",
            "grant|R|f", "granted|U|t", "greatest|C|t", "group|R|f",
            "grouping|C|t", "groups|U|t", "handler|U|t", "having|R|f",
            "header|U|t", "hold|U|t", "hour|U|f", "identity|U|t",
            "if|U|t", "ilike|T|t", "immediate|U|t", "immutable|U|t",
            "implicit|U|t", "import|U|t", "in|R|t", "include|U|t",
            "including|U|t", "increment|U|t", "indent|U|t", "index|U|t",
            "indexes|U|t", "inherit|U|t", "inherits|U|t", "initially|R|t",
            "inline|U|t", "inner|T|t", "inout|C|t", "input|U|t",
            "insensitive|U|t", "insert|U|t", "instead|U|t", "int|C|t",
            "integer|C|t", "intersect|R|f", "interval|C|t", "into|R|f",
            "invoker|U|t", "is|T|t", "isnull|T|f", "isolation|U|t",
            "join|T|t", "json|C|t", "json_array|C|t", "json_arrayagg|C|t",
            "json_exists|C|t", "json_object|C|t", "json_objectagg|C|t", "json_query|C|t",
            "json_scalar|C|t", "json_serialize|C|t", "json_table|C|t", "json_value|C|t",
            "keep|U|t", "key|U|t", "keys|U|t", "label|U|t",
            "language|U|t", "large|U|t", "last|U|t", "lateral|R|t",
            "leading|R|t", "leakproof|U|t", "least|C|t", "left|T|t",
            "level|U|t", "like|T|t", "limit|R|f", "listen|U|t",
            "load|U|t", "local|U|t", "localtime|R|t", "localtimestamp|R|t",
            "location|U|t", "lock|U|t", "locked|U|t", "logged|U|t",
            "mapping|U|t", "match|U|t", "matched|U|t", "materialized|U|t",
            "maxvalue|U|t", "merge|U|t", "merge_action|C|t", "method|U|t",
            "minute|U|f", "minvalue|U|t", "mode|U|t", "month|U|f",
            "move|U|t", "name|U|t", "names|U|t", "national|C|t",
            "natural|T|t", "nchar|C|t", "nested|U|t", "new|U|t",
            "next|U|t", "nfc|U|t", "nfd|U|t", "nfkc|U|t",
            "nfkd|U|t", "no|U|t", "none|C|t", "normalize|C|t",
            "normalized|U|t", "not|R|t", "nothing|U|t", "notify|U|t",
            "notnull|T|f", "nowait|U|t", "null|R|t", "nullif|C|t",
            "nulls|U|t", "numeric|C|t", "object|U|t", "objects|U|t",
            "of|U|t", "off|U|t", "offset|R|f", "oids|U|t",
            "old|U|t", "omit|U|t", "on|R|f", "only|R|t",
            "operator|U|t", "option|U|t", "options|U|t", "or|R|t",
            "order|R|f", "ordinality|U|t", "others|U|t", "out|C|t",
            "outer|T|t", "over|U|f", "overlaps|T|f", "overlay|C|t",
            "overriding|U|t", "owned|U|t", "owner|U|t", "parallel|U|t",
            "parameter|U|t", "parser|U|t", "partial|U|t", "partition|U|t",
            "passing|U|t", "password|U|t", "path|U|t", "period|U|t",
            "placing|R|t", "plan|U|t", "plans|U|t", "policy|U|t",
            "position|C|t", "preceding|U|t", "precision|C|f", "prepare|U|t",
            "prepared|U|t", "preserve|U|t", "primary|R|t", "prior|U|t",
            "privileges|U|t", "procedural|U|t", "procedure|U|t", "procedures|U|t",
            "program|U|t", "publication|U|t", "quote|U|t", "quotes|U|t",
            "range|U|t", "read|U|t", "real|C|t", "reassign|U|t",
            "recursive|U|t", "ref|U|t", "references|R|t", "referencing|U|t",
            "refresh|U|t", "reindex|U|t", "relative|U|t", "release|U|t",
            "rename|U|t", "repeatable|U|t", "replace|U|t", "replica|U|t",
            "reset|U|t", "restart|U|t", "restrict|U|t", "return|U|t",
            "returning|R|f", "returns|U|t", "revoke|U|t", "right|T|t",
            "role|U|t", "rollback|U|t", "rollup|U|t", "routine|U|t",
            "routines|U|t", "row|C|t", "rows|U|t", "rule|U|t",
            "savepoint|U|t", "scalar|U|t", "schema|U|t", "schemas|U|t",
            "scroll|U|t", "search|U|t", "second|U|f", "security|U|t",
            "select|R|t", "sequence|U|t", "sequences|U|t", "serializable|U|t",
            "server|U|t", "session|U|t", "session_user|R|t", "set|U|t",
            "setof|C|t", "sets|U|t", "share|U|t", "show|U|t",
            "similar|T|t", "simple|U|t", "skip|U|t", "smallint|C|t",
            "snapshot|U|t", "some|R|t", "source|U|t", "sql|U|t",
            "stable|U|t", "standalone|U|t", "start|U|t", "statement|U|t",
            "statistics|U|t", "stdin|U|t", "stdout|U|t", "storage|U|t",
            "stored|U|t", "strict|U|t", "string|U|t", "strip|U|t",
            "subscription|U|t", "substring|C|t", "support|U|t", "symmetric|R|t",
            "sysid|U|t", "system|U|t", "system_user|R|t", "table|R|t",
            "tables|U|t", "tablesample|T|t", "tablespace|U|t", "target|U|t",
            "temp|U|t", "template|U|t", "temporary|U|t", "text|U|t",
            "then|R|t", "ties|U|t", "time|C|t", "timestamp|C|t",
            "to|R|f", "trailing|R|t", "transaction|U|t", "transform|U|t",
            "treat|C|t", "trigger|U|t", "trim|C|t", "true|R|t",
            "truncate|U|t", "trusted|U|t", "type|U|t", "types|U|t",
            "uescape|U|t", "unbounded|U|t", "uncommitted|U|t", "unconditional|U|t",
            "unencrypted|U|t", "union|R|f", "unique|R|t", "unknown|U|t",
            "unlisten|U|t", "unlogged|U|t", "until|U|t", "update|U|t",
            "user|R|t", "using|R|t", "vacuum|U|t", "valid|U|t",
            "validate|U|t", "validator|U|t", "value|U|t", "values|C|t",
            "varchar|C|t", "variadic|R|t", "varying|U|f", "verbose|T|t",
            "version|U|t", "view|U|t", "views|U|t", "virtual|U|t",
            "volatile|U|t", "when|R|t", "where|R|f", "whitespace|U|t",
            "window|R|f", "with|R|f", "within|U|f", "without|U|f",
            "work|U|t", "wrapper|U|t", "write|U|t", "xml|U|t",
            "xmlattributes|C|t", "xmlconcat|C|t", "xmlelement|C|t", "xmlexists|C|t",
            "xmlforest|C|t", "xmlnamespaces|C|t", "xmlparse|C|t", "xmlpi|C|t",
            "xmlroot|C|t", "xmlserialize|C|t", "xmltable|C|t", "year|U|f",
            "yes|U|t", "zone|U|t"
    };

    private static final List<Keyword> ALL = all();

    private static List<Keyword> all() {
        List<Keyword> out = new ArrayList<Keyword>(MEASURED.length);
        for (String entry : MEASURED) {
            int first = entry.indexOf('|');
            int second = entry.indexOf('|', first + 1);
            out.add(new Keyword(entry.substring(0, first),
                    entry.substring(first + 1, second),
                    "t".equals(entry.substring(second + 1))));
        }
        return Collections.unmodifiableList(out);
    }

    /** Every keyword, in the order the report gives them: alphabetical. */
    static List<Keyword> keywords() {
        return ALL;
    }

    /**
     * Whether PostgreSQL's grammar names this word at all, in any of the four categories.
     *
     * <p>Some productions take a bare identifier and nothing else — the one that reports an
     * unrecognized role option is one — so what matters there is not how tightly a word is
     * reserved but whether the grammar has a use for it.
     */
    public static boolean isKeyword(String word) {
        if (word == null) return false;
        String lower = word.toLowerCase(java.util.Locale.ROOT);
        for (Keyword k : ALL) {
            if (k.word.equals(lower)) return true;
        }
        return false;
    }

}
