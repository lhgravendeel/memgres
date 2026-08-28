package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.*;

/**
 * Text search function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class TextSearchFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    /** What to_tsvector indexes when it is given a document: the strings it stores, and no more. */
    private static final Set<String> STRINGS_ONLY = Cols.setOf("string");

    /**
     * Whichever of json and jsonb an argument is written in, or null where it is neither.
     *
     * <p>Read from the expression rather than from the value, because a document reaches here as
     * the characters it is written as and text holding the same characters is a different thing:
     * {@code to_tsvector('{"a": "cats"}')} indexes the braces and the key, and
     * {@code to_tsvector('{"a": "cats"}'::json)} indexes neither.
     */
    private DataType documentTypeOf(FunctionCallExpr fn, int at, RowContext ctx) {
        if (fn == null || at >= fn.args().size()) return null;
        List<RowContext.TableBinding> bindings = ctx == null
                ? Collections.<RowContext.TableBinding>emptyList() : ctx.getBindings();
        DataType type;
        try {
            type = executor.exprEvaluator.inferTypeFromContext(fn.args().get(at), bindings);
        } catch (RuntimeException e) {
            return null;
        }
        return type == DataType.JSON || type == DataType.JSONB ? type : null;
    }

    /**
     * The weight a {@code setweight} argument names.
     *
     * <p>There are four weights and nothing else is one. Reading the first character and using
     * it whatever it was gave {@code 'a':1X} for a weight of {@code 'X'} -- text no reader of a
     * tsvector accepts -- and read past the end of an empty argument.
     *
     * <p>An argument that names no weight is the caller's, so it is reported as a bad parameter
     * with the character quoted, the way every other weight-taking function reports one. An
     * empty argument has no first character, and the one PostgreSQL reads there is the byte that
     * ends the string.
     */
    private static char weightGiven(String given) {
        char c = given.isEmpty() ? 0 : Character.toUpperCase(given.charAt(0));
        if (c < 'A' || c > 'D') {
            throw new MemgresException("unrecognized weight: \""
                    + (given.isEmpty() ? "\\000" : given.substring(0, 1)) + "\"", "22023");
        }
        return c;
    }

    /** The weight an element of a weight array names. */
    private static char weightNamed(String given) {
        char c = given.isEmpty() ? 0 : Character.toUpperCase(given.charAt(0));
        if (c < 'A' || c > 'D' || given.length() > 1) {
            throw new MemgresException("unrecognized weight: \"" + given + "\"", "22023");
        }
        return c;
    }

    /**
     * A document read from its text, as jsonb keeps it where jsonb is what it was written as.
     *
     * <p>Which of the two it is decides the order the members come in, and so the positions the
     * lexemes get: json keeps the order they were written in, jsonb the order it stores them in.
     */
    private static JsonValue parseDocument(String text, boolean asJsonb) {
        return asJsonb ? JsonParser.parseJsonb(text) : JsonParser.parse(text);
    }

    /**
     * The pieces of text a document offers a text search, in the order they are met.
     *
     * <p>The filter names which kinds count. A string is indexed as the text it holds; a number or
     * a boolean as the way it prints; a key as the name it is. A JSON null is never indexed,
     * having nothing to index, and neither is the structure itself.
     */
    private static List<String> indexedTexts(JsonValue document, Set<String> filter) {
        List<String> texts = new ArrayList<String>();
        collectIndexedTexts(document, filter, texts);
        return texts;
    }

    private static void collectIndexedTexts(JsonValue value, Set<String> filter, List<String> out) {
        switch (value.kind()) {
            case JsonValue.OBJECT:
                for (int i = 0; i < value.size(); i++) {
                    if (filter.contains("key")) out.add(value.keyAt(i));
                    collectIndexedTexts(value.at(i), filter, out);
                }
                return;
            case JsonValue.ARRAY:
                for (JsonValue element : value.elements()) {
                    collectIndexedTexts(element, filter, out);
                }
                return;
            case JsonValue.STRING:
                if (filter.contains("string")) out.add(value.asString());
                return;
            case JsonValue.NUMBER:
                if (filter.contains("numeric")) out.add(value.numberText());
                return;
            case JsonValue.BOOLEAN:
                if (filter.contains("boolean")) out.add(value.asBoolean() ? "true" : "false");
                return;
            default:
        }
    }

    /** The kinds the two named to_tsvector forms take a filter over. */
    private static final Set<String> TS_FILTER_KINDS =
            Cols.setOf("string", "numeric", "boolean", "key", "all");

    /**
     * The filter argument of json_to_tsvector, which is a document holding either the one word
     * "all" or an array of the kinds to index.
     */
    private static Set<String> parseTsFilter(Object arg) {
        JsonValue filter = JsonParser.parseJsonb(arg.toString());
        List<JsonValue> named = filter.isArray()
                ? filter.elements() : Collections.singletonList(filter);
        Set<String> kinds = new HashSet<String>();
        for (JsonValue kind : named) {
            if (kind.kind() != JsonValue.STRING || !TS_FILTER_KINDS.contains(kind.asString())) {
                throw new MemgresException("wrong flag in flag array: \""
                        + (kind.kind() == JsonValue.STRING ? kind.asString() : kind.typeName())
                        + "\"", "22023");
            }
            kinds.add(kind.asString());
        }
        if (kinds.contains("all")) return TS_FILTER_KINDS;
        return kinds;
    }

    /** The same document with every string it holds replaced by the headline of that string. */
    private JsonValue headlineOf(JsonValue value, TsQuery query, String options,
                                 String config) {
        switch (value.kind()) {
            case JsonValue.OBJECT: {
                List<JsonValue> values = new ArrayList<JsonValue>(value.size());
                for (JsonValue member : value.elements()) {
                    values.add(headlineOf(member, query, options, config));
                }
                return JsonValue.object(value.keys(), values);
            }
            case JsonValue.ARRAY: {
                List<JsonValue> elements = new ArrayList<JsonValue>(value.size());
                for (JsonValue element : value.elements()) {
                    elements.add(headlineOf(element, query, options, config));
                }
                return JsonValue.array(elements);
            }
            case JsonValue.STRING:
                return JsonValue.string(TextSearchOperations.tsHeadline(
                        value.asString(), query, options, config));
            default:
                return value;
        }
    }

    /** Parse a string as tsvector literal first; fall back to prose tokenization. */
    private static TsVector toTsVector(String s) {
        TsVector parsed = TsVector.parseLiteral(s);
        return parsed != null ? parsed : TsVector.fromText(s);
    }

    /** The configurations PG ships with, each backed by its own snowball stemmer. */
    private static final Set<String> BUILTIN_TS_CONFIGS = Cols.setOf(
            "simple", "english", "german", "french", "spanish", "italian",
            "portuguese", "dutch", "finnish", "swedish", "danish", "norwegian",
            "russian", "romanian", "hungarian", "turkish", "arabic", "nepali",
            "irish", "indonesian", "lithuanian", "greek", "hindi", "basque");

    private final AstExecutor executor;

    TextSearchFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    /**
     * Resolve a text search configuration name to the built-in that does the actual work. A
     * configuration created with COPY behaves as its source does, so following copyFrom lands on
     * a stemmer; a name that names nothing at all is an error, not a silent fallback.
     */
    /** The configuration a name stands for, for callers outside this class. */
    String namedConfig(String rawName) {
        return resolveTsConfig(rawName);
    }

    private String resolveTsConfig(String rawName) {
        String name = rawName == null ? "" : rawName.toLowerCase();
        // Strip any schema qualification: configurations live in one namespace here.
        int dot = name.lastIndexOf('.');
        if (dot >= 0) name = name.substring(dot + 1);
        if (BUILTIN_TS_CONFIGS.contains(name)) return name;

        Map<String, Database.TsConfigDef> defined = executor.database.getTsConfigs();
        String current = name;
        // A chain of copies has to end somewhere; stop if it loops back on itself.
        for (int hops = 0; hops < 16; hops++) {
            Database.TsConfigDef def = defined.get(current);
            if (def == null) break;
            String next = def.copyFrom != null ? def.copyFrom.toLowerCase() : null;
            if (next == null) {
                // Declared with a PARSER rather than COPY: tokenizes without stemming.
                return "simple";
            }
            int nextDot = next.lastIndexOf('.');
            if (nextDot >= 0) next = next.substring(nextDot + 1);
            if (BUILTIN_TS_CONFIGS.contains(next)) return next;
            if (next.equals(current)) break;
            current = next;
        }
        throw new MemgresException(
                "text search configuration \"" + rawName + "\" does not exist", "42704");
    }

    /**
     * The names this class answers to. Checked before anything is evaluated, so a call this class
     * does not handle is handed on untouched rather than having its arguments run twice.
     */
    private static final Set<String> HANDLED = Cols.setOf(
            "__tsquery_not__", "array_to_tsvector", "get_current_ts_config", "numnode",
            "phraseto_tsquery", "plainto_tsquery", "querytree", "setweight", "strip",
            "to_tsquery", "to_tsvector", "ts_debug", "ts_delete", "ts_filter", "ts_headline",
            "ts_lexize", "ts_parse", "ts_rank", "ts_rank_cd", "ts_rewrite", "ts_stat",
            "ts_token_type", "tsquery_phrase", "tsvector_to_array", "websearch_to_tsquery",
            "json_to_tsvector", "jsonb_to_tsvector",
            "ts_match_vq", "ts_match_qv", "ts_match_tq", "ts_match_tt",
            "tsvector_cmp", "tsquery_cmp", "tsquery_not", "tsquery_and", "tsquery_or",
            "tsvector_concat");

    /**
     * Every text-search function PostgreSQL exposes is declared strict, so a NULL argument makes
     * the call NULL without the body ever running. {@code get_current_ts_config} takes no
     * argument and {@code __tsquery_not__} is memgres's own spelling of the {@code !!} operator,
     * which is strict as well; the exceptions in pg_proc are all internal support functions that
     * are not reachable from SQL.
     */
    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        if (!HANDLED.contains(name)) return NOT_HANDLED;
        // Evaluated once, here: reading an argument twice would run a volatile expression twice,
        // and nextval() in an argument would burn two values.
        List<Object> argv = new ArrayList<Object>(fn.args().size());
        for (Expression arg : fn.args()) argv.add(executor.evalExpr(arg, ctx));
        for (Object v : argv) {
            if (v == null) return null;
        }
        return eval(name, argv, fn, ctx);
    }

    private Object eval(String name, List<Object> argv, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "__tsquery_not__": {
                Object operand = argv.get(0);
                if (operand == null) return null;
                TsQuery q = operand instanceof TsQuery ? (TsQuery) operand : TsQuery.parse(operand.toString());
                return TsQuery.not(q);
            }
            case "to_tsvector": {
                int docAt = argv.size() == 2 ? 1 : 0;
                String configName = argv.size() == 2
                        ? resolveTsConfig(String.valueOf(argv.get(0))) : "english";
                Object text = argv.get(docAt);
                if (text == null) return null;
                // A document is indexed by what it holds, not by the characters it is written as:
                // its keys and its punctuation are structure rather than prose. Only the strings
                // it stores are indexed, which is the "string" filter the two named forms spell.
                DataType documentType = documentTypeOf(fn, docAt, ctx);
                if (documentType != null) {
                    JsonValue document = parseDocument(text.toString(),
                            documentType == DataType.JSONB);
                    return TsVector.fromTexts(indexedTexts(document, STRINGS_ONLY), configName);
                }
                return TsVector.fromText(text.toString(), configName);
            }
            case "json_to_tsvector":
            case "jsonb_to_tsvector": {
                // json_to_tsvector([config,] document, filter)
                int docAt = argv.size() == 3 ? 1 : 0;
                String configName = argv.size() == 3
                        ? resolveTsConfig(String.valueOf(argv.get(0))) : "english";
                JsonValue document = parseDocument(argv.get(docAt).toString(),
                        name.startsWith("jsonb"));
                Set<String> filter = parseTsFilter(argv.get(docAt + 1));
                return TsVector.fromTexts(indexedTexts(document, filter), configName);
            }
            case "to_tsquery": {
                String config = "english";
                Object tsqText;
                if (argv.size() == 2) {
                    config = resolveTsConfig(String.valueOf(argv.get(0)));
                    tsqText = argv.get(1);
                } else {
                    tsqText = argv.get(0);
                }
                if (tsqText == null) return null;
                String tsqStr = tsqText.toString().trim();
                if (tsqStr.isEmpty()) return TsQuery.emptyQuery();
                // Validate: to_tsquery requires operators between words
                // Check for adjacent words without operators (PG rejects with 42601)
                if (tsqStr.matches("(?s).*[&|]\\s*[&|].*")) {
                    throw new MemgresException("syntax error in tsquery: \"" + tsqStr + "\"", "42601");
                }
                // Unlike a literal, to_tsquery runs each word through the configuration's
                // dictionary, so 'Cats' becomes 'cat' under english and 'cats' under simple.
                return TsQuery.parse(tsqStr, config);
            }
            case "plainto_tsquery": {
                // The configuration says how the text is read, so a name that names none is an
                // error rather than a quiet fall back to english.
                String config = "english";
                String input;
                if (argv.size() == 2) {
                    config = resolveTsConfig(String.valueOf(argv.get(0)));
                    input = String.valueOf(argv.get(1));
                } else {
                    input = String.valueOf(argv.get(0));
                }
                return TextSearchOperations.plainToTsQuery(input, config);
            }
            case "phraseto_tsquery": {
                // The configuration says how the text is read, so a name that names none is an
                // error rather than a quiet fall back to english.
                String config = "english";
                String input;
                if (argv.size() == 2) {
                    config = resolveTsConfig(String.valueOf(argv.get(0)));
                    input = String.valueOf(argv.get(1));
                } else {
                    input = String.valueOf(argv.get(0));
                }
                return TextSearchOperations.phraseToTsQuery(input, config);
            }
            case "websearch_to_tsquery": {
                // The configuration says how the text is read, so a name that names none is an
                // error rather than a quiet fall back to english.
                String config = "english";
                String input;
                if (argv.size() == 2) {
                    config = resolveTsConfig(String.valueOf(argv.get(0)));
                    input = String.valueOf(argv.get(1));
                } else {
                    input = String.valueOf(argv.get(0));
                }
                return TextSearchOperations.websearchToTsQuery(input, config);
            }
            case "ts_rank": {
                // ts_rank([weights,] tsvector, tsquery [, normalization])
                int argIdx = 0;
                float[] weights = null;
                // Check if first arg is a weights array
                if (argv.size() >= 3) {
                    Object first = argv.get(0);
                    if (first != null) {
                        float[] parsed = parseWeightsArray(first);
                        if (parsed != null) {
                            weights = parsed;
                            argIdx = 1;
                        }
                    }
                }
                Object vecObj = argv.get(argIdx);
                Object queryObj = argv.get(argIdx + 1);
                // A rank over a null document or a null query is unknown, not zero: zero is a real
                // answer meaning "no match", and a caller ordering by rank cannot tell them apart.
                if (vecObj == null || queryObj == null) return null;
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : toTsVector(vecObj.toString());
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                int norm = 0;
                if (argIdx + 2 < argv.size()) {
                    Object normObj = argv.get(argIdx + 2);
                    if (normObj != null) norm = executor.toInt(normObj);
                }
                // PG returns the raw float4; rounding here would lose a significant digit.
                return (float) vec.rank(query, weights, norm);
            }
            case "ts_rank_cd": {
                int argIdx = 0;
                float[] weights = null;
                if (argv.size() >= 3) {
                    Object first = argv.get(0);
                    if (first != null) {
                        float[] parsed = parseWeightsArray(first);
                        if (parsed != null) {
                            weights = parsed;
                            argIdx = 1;
                        }
                    }
                }
                Object vecObj = argv.get(argIdx);
                Object queryObj = argv.get(argIdx + 1);
                // A rank over a null document or a null query is unknown, not zero: zero is a real
                // answer meaning "no match", and a caller ordering by rank cannot tell them apart.
                if (vecObj == null || queryObj == null) return null;
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : toTsVector(vecObj.toString());
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                int norm = 0;
                if (argIdx + 2 < argv.size()) {
                    Object normObj = argv.get(argIdx + 2);
                    if (normObj != null) norm = executor.toInt(normObj);
                }
                return (float) vec.rankCd(query, weights, norm);
            }
            case "ts_headline": {
                // Every argument is strict: a null anywhere makes the headline unknown. Reaching
                // toString() on one instead is what turned a null query into an XX000 and a null
                // document into the four-character string "null".
                String config = null;
                String document;
                int documentAt;
                TsQuery query;
                String options = null;
                if (argv.size() >= 4) {
                    Object cfg = argv.get(0);
                    Object doc = argv.get(1);
                    Object q = argv.get(2);
                    Object opt = argv.get(3);
                    if (cfg == null || doc == null || q == null || opt == null) return null;
                    config = resolveTsConfig(cfg.toString());
                    document = doc.toString();
                    documentAt = 1;
                    query = q instanceof TsQuery ? ((TsQuery) q) : TsQuery.parse(q.toString());
                    options = opt.toString();
                } else if (argv.size() == 3) {
                    Object first = argv.get(0);
                    Object second = argv.get(1);
                    Object third = argv.get(2);
                    if (first == null || second == null || third == null) return null;
                    if (third instanceof TsQuery) {
                        config = resolveTsConfig(first.toString());
                        document = second.toString();
                        documentAt = 1;
                        query = (TsQuery) third;
                    } else {
                        document = first.toString();
                        documentAt = 0;
                        query = second instanceof TsQuery ? ((TsQuery) second) : TsQuery.parse(second.toString());
                        options = third.toString();
                    }
                } else {
                    Object doc = argv.get(0);
                    Object q = argv.get(1);
                    if (doc == null || q == null) return null;
                    document = doc.toString();
                    documentAt = 0;
                    query = q instanceof TsQuery ? ((TsQuery) q) : TsQuery.parse(q.toString());
                }
                // A headline over a document keeps the document's shape: each string it holds is
                // marked up on its own and everything else is left as it stands. Marking up the
                // written characters instead put the tags wherever the word happened to fall,
                // inside the quotes that delimit a string or across the punctuation between two.
                DataType documentType = documentTypeOf(fn, documentAt, ctx);
                if (documentType != null) {
                    JsonValue headlined = headlineOf(
                            parseDocument(document, documentType == DataType.JSONB),
                            query, options, config == null ? "english" : config);
                    return documentType == DataType.JSONB
                            ? JsonWriter.jsonb(headlined) : JsonWriter.json(headlined);
                }
                return TextSearchOperations.tsHeadline(document, query, options,
                        config == null ? "english" : config);
            }
            case "ts_rewrite": {
                Object queryObj = argv.get(0);
                if (queryObj == null) return null;
                // The two-argument form takes the pairs to rewrite with from a query rather than
                // from its own arguments. Routing it to the three-argument code read past the
                // end of the argument list and reported that as an internal error.
                if (argv.size() == 2) {
                    Object sqlObj = argv.get(1);
                    if (sqlObj == null) return null;
                    TsQuery rewritten = queryObj instanceof TsQuery
                            ? (TsQuery) queryObj : TsQuery.parse(queryObj.toString());
                    QueryResult pairs = executor.execute(sqlObj.toString());
                    if (pairs != null && pairs.getRows() != null) {
                        for (Object[] row : pairs.getRows()) {
                            if (row.length < 2 || row[0] == null || row[1] == null) continue;
                            TsQuery target = row[0] instanceof TsQuery
                                    ? (TsQuery) row[0] : TsQuery.parse(row[0].toString());
                            TsQuery sub = row[1] instanceof TsQuery
                                    ? (TsQuery) row[1] : TsQuery.parse(row[1].toString());
                            rewritten = TextSearchOperations.tsRewrite(rewritten, target, sub);
                        }
                    }
                    return rewritten;
                }
                Object targetObj = argv.get(1);
                Object subObj = argv.get(2);
                if (targetObj == null || subObj == null) return null;
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                TsQuery target = targetObj instanceof TsQuery ? ((TsQuery) targetObj) : TsQuery.parse(targetObj.toString());
                TsQuery sub = subObj instanceof TsQuery ? ((TsQuery) subObj) : TsQuery.parse(subObj.toString());
                return TextSearchOperations.tsRewrite(query, target, sub);
            }
            case "strip": {
                Object arg = argv.get(0);
                if (arg instanceof TsVector) return ((TsVector) arg).strip();
                return arg != null ? arg.toString() : null;
            }
            case "setweight": {
                Object vecObj = argv.get(0);
                Object weightObj = argv.get(1);
                if (vecObj == null || weightObj == null) return null;
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : toTsVector(vecObj.toString());
                char weight = weightGiven(weightObj.toString());
                if (argv.size() >= 3) {
                    Object lexArr = argv.get(2);
                    List<String> filterLexemes = new ArrayList<>();
                    if (lexArr instanceof List<?>) {
                        for (Object o : (List<?>) lexArr) filterLexemes.add(o.toString());
                    } else if (lexArr instanceof String) {
                        String s = (String) lexArr;
                        if (s.startsWith("{") && s.endsWith("}")) {
                            for (String w : s.substring(1, s.length() - 1).split(",")) {
                                filterLexemes.add(w.trim());
                            }
                        }
                    }
                    return vec.setWeight(weight, filterLexemes);
                }
                return vec.setWeight(weight);
            }
            case "ts_delete": {
                Object vecObj = argv.get(0);
                Object toDelete = argv.get(1);
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : toTsVector(vecObj.toString());
                List<String> deleteList = new ArrayList<>();
                if (toDelete instanceof List<?>) {
                    for (Object o : (List<?>) toDelete) deleteList.add(o.toString());
                } else if (toDelete instanceof String) {
                    String s = (String) toDelete;
                    if (s.startsWith("{") && s.endsWith("}")) {
                        for (String w : s.substring(1, s.length() - 1).split(",")) {
                            deleteList.add(w.trim());
                        }
                    } else {
                        deleteList.add(s);
                    }
                }
                return vec.delete(deleteList);
            }
            case "ts_filter": {
                Object vecObj = argv.get(0);
                Object weightsObj = argv.get(1);
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : toTsVector(vecObj.toString());
                Set<Character> filterWeights = new HashSet<>();
                if (weightsObj instanceof List<?>) {
                    for (Object o : (List<?>) weightsObj) {
                        String ws = o.toString().trim().replace("\"", "");
                        if (!ws.isEmpty()) filterWeights.add(weightNamed(ws));
                    }
                } else {
                    String ws = weightsObj.toString();
                    if (ws.startsWith("{") && ws.endsWith("}")) ws = ws.substring(1, ws.length() - 1);
                    for (String w : ws.split(",")) {
                        w = w.trim().replace("\"", "");
                        if (!w.isEmpty()) filterWeights.add(weightNamed(w));
                    }
                }
                return vec.filter(filterWeights);
            }
            case "tsquery_phrase": {
                Object leftObj = argv.get(0);
                Object rightObj = argv.get(1);
                TsQuery left = leftObj instanceof TsQuery ? ((TsQuery) leftObj) : TsQuery.parse(leftObj.toString());
                TsQuery right = rightObj instanceof TsQuery ? ((TsQuery) rightObj) : TsQuery.parse(rightObj.toString());
                int dist = argv.size() >= 3 ? executor.toInt(argv.get(2)) : 1;
                return TextSearchOperations.tsqueryPhrase(left, right, dist);
            }
            case "numnode": {
                Object queryObj = argv.get(0);
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                return query.numNode();
            }
            case "querytree": {
                Object queryObj = argv.get(0);
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                return query.queryTree();
            }
            case "ts_debug": {
                String config = "english";
                String input;
                if (argv.size() == 2) {
                    config = resolveTsConfig(String.valueOf(argv.get(0)));
                    input = String.valueOf(argv.get(1));
                } else {
                    input = String.valueOf(argv.get(0));
                }
                List<Object[]> debug = TextSearchOperations.tsDebug(config, input);
                if (debug.isEmpty()) return "";
                Object[] first = debug.get(0);
                return "(" + first[0] + ",\"" + first[1] + "\"," + first[2] + "," + first[3] + ",{" + first[5] + "})";
            }
            case "ts_lexize": {
                // The dictionary is what does the work: the simple one folds the case and keeps
                // the word, and a stemmer stems it and drops the words on its stop list.
                // Ignoring the argument answered for the english stemmer whatever was asked for.
                String dict = String.valueOf(argv.get(0));
                String token = String.valueOf(argv.get(1));
                List<String> result = TextSearchOperations.tsLexize(dict, token);
                return result == null ? null : "{" + String.join(",", result) + "}";
            }
            case "ts_token_type": {
                String parser = argv.isEmpty() ? "default" : String.valueOf(argv.get(0));
                List<Object[]> types = TextSearchOperations.tsTokenType(parser);
                StringBuilder sb = new StringBuilder();
                for (Object[] t : types) {
                    if (sb.length() > 0) sb.append(",");
                    sb.append("(").append(t[0]).append(",").append(t[1]).append(",\"").append(t[2]).append("\")");
                }
                return "(" + sb + ")";
            }
            case "ts_parse": {
                String tsParser = String.valueOf(argv.get(0));
                String text = String.valueOf(argv.get(1));
                List<Object[]> tokens = TextSearchOperations.tsParse(tsParser, text);
                StringBuilder sb = new StringBuilder();
                for (Object[] t : tokens) {
                    if (sb.length() > 0) sb.append(",");
                    sb.append("(").append(t[0]).append(",\"").append(t[1]).append("\")");
                }
                return "(" + sb + ")";
            }
            case "ts_stat": {
                return null;
            }
            // The function spellings of the operators. PostgreSQL declares each of these in
            // pg_proc, so SQL that names one directly resolves; they were missing entirely.
            case "ts_match_vq":
            case "ts_match_qv":
            case "ts_match_tq":
            case "ts_match_tt": {
                Object first = argv.get(0);
                Object second = argv.get(1);
                if (first == null || second == null) return null;
                boolean queryFirst = name.equals("ts_match_qv");
                Object document = queryFirst ? second : first;
                Object query = queryFirst ? first : second;
                TsVector vector = document instanceof TsVector
                        ? (TsVector) document : toTsVector(document.toString());
                TsQuery parsed = query instanceof TsQuery
                        ? (TsQuery) query
                        : name.endsWith("tt") ? TextSearchOperations.plainToTsQuery(
                                query.toString(), "english")
                        : TsQuery.parse(query.toString());
                return vector.matches(parsed);
            }
            case "tsvector_cmp": {
                Object first = argv.get(0);
                Object second = argv.get(1);
                if (first == null || second == null) return null;
                return TextSearchOperations.compareVectors(
                        first instanceof TsVector ? (TsVector) first : toTsVector(first.toString()),
                        second instanceof TsVector ? (TsVector) second : toTsVector(second.toString()));
            }
            case "tsquery_cmp": {
                Object first = argv.get(0);
                Object second = argv.get(1);
                if (first == null || second == null) return null;
                return Integer.valueOf(first.toString().compareTo(second.toString()));
            }
            case "tsquery_not": {
                Object only = argv.get(0);
                if (only == null) return null;
                return TsQuery.not(only instanceof TsQuery
                        ? (TsQuery) only : TsQuery.parse(only.toString()));
            }
            case "tsquery_and":
            case "tsquery_or": {
                Object first = argv.get(0);
                Object second = argv.get(1);
                if (first == null || second == null) return null;
                TsQuery l = first instanceof TsQuery
                        ? (TsQuery) first : TsQuery.parse(first.toString());
                TsQuery r = second instanceof TsQuery
                        ? (TsQuery) second : TsQuery.parse(second.toString());
                return name.equals("tsquery_and") ? TsQuery.and(l, r) : TsQuery.or(l, r);
            }
            case "tsvector_concat": {
                Object first = argv.get(0);
                Object second = argv.get(1);
                if (first == null || second == null) return null;
                TsVector l = first instanceof TsVector
                        ? (TsVector) first : toTsVector(first.toString());
                TsVector r = second instanceof TsVector
                        ? (TsVector) second : toTsVector(second.toString());
                return l.concat(r);
            }
            case "get_current_ts_config": {
                return TextSearchOperations.getCurrentTsConfig();
            }
            case "array_to_tsvector": {
                Object arrObj = argv.get(0);
                List<String> words = new ArrayList<>();
                if (arrObj instanceof List<?>) {
                    List<?> list = (List<?>) arrObj;
                    for (Object o : list) {
                        // The reader of the array says what an element has to be, so the
                        // complaint about one is worded there and once.
                        words.add(o == null ? null : o.toString());
                    }
                } else if (arrObj instanceof String && ((String) arrObj).startsWith("{") && ((String) arrObj).endsWith("}")) {
                    String s = (String) arrObj;
                    for (String w : s.substring(1, s.length() - 1).split(",")) {
                        words.add(w.trim());
                    }
                }
                return TsVector.fromArray(words);
            }
            case "tsvector_to_array": {
                Object vecObj = argv.get(0);
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : toTsVector(vecObj.toString());
                List<String> arr = vec.toArray();
                return "{" + String.join(",", arr) + "}";
            }
            default:
                return NOT_HANDLED;
        }
    }

    /** Parse a float4 weights array like '{0.1, 0.2, 0.4, 1.0}' or a List/array. */
    private float[] parseWeightsArray(Object obj) {
        if (obj instanceof float[]) return (float[]) obj;
        String s = obj.toString().trim();
        if (s.startsWith("{") && s.endsWith("}")) {
            String inner = s.substring(1, s.length() - 1);
            String[] parts = inner.split(",");
            if (parts.length == 4) {
                try {
                    float[] w = new float[4];
                    for (int i = 0; i < 4; i++) {
                        w[i] = Float.parseFloat(parts[i].trim());
                    }
                    return w;
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
        if (obj instanceof List<?>) {
            List<?> list = (List<?>) obj;
            if (list.size() == 4) {
                try {
                    float[] w = new float[4];
                    for (int i = 0; i < 4; i++) {
                        w[i] = ((Number) list.get(i)).floatValue();
                    }
                    return w;
                } catch (Exception e) {
                    return null;
                }
            }
        }
        return null;
    }
}
