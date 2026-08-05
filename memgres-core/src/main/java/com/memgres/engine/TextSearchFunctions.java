package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.*;

/**
 * Text search function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class TextSearchFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

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
            "ts_token_type", "tsquery_phrase", "tsvector_to_array", "websearch_to_tsquery");

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
        return eval(name, argv, ctx);
    }

    private Object eval(String name, List<Object> argv, RowContext ctx) {
        switch (name) {
            case "__tsquery_not__": {
                Object operand = argv.get(0);
                if (operand == null) return null;
                TsQuery q = operand instanceof TsQuery ? (TsQuery) operand : TsQuery.parse(operand.toString());
                return TsQuery.not(q);
            }
            case "to_tsvector": {
                if (argv.size() == 2) {
                    String configName = resolveTsConfig(
                            String.valueOf(argv.get(0)));
                    Object text = argv.get(1);
                    return text == null ? null : TsVector.fromText(text.toString(), configName);
                }
                Object text = argv.get(0);
                return text == null ? null : TsVector.fromText(text.toString());
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
                return TsQuery.parse(tsqStr);
            }
            case "plainto_tsquery": {
                String config = "english";
                String input;
                if (argv.size() == 2) {
                    config = String.valueOf(argv.get(0)).toLowerCase();
                    input = String.valueOf(argv.get(1));
                } else {
                    input = String.valueOf(argv.get(0));
                }
                return TextSearchOperations.plainToTsQuery(input, config);
            }
            case "phraseto_tsquery": {
                String config = "english";
                String input;
                if (argv.size() == 2) {
                    config = String.valueOf(argv.get(0)).toLowerCase();
                    input = String.valueOf(argv.get(1));
                } else {
                    input = String.valueOf(argv.get(0));
                }
                return TextSearchOperations.phraseToTsQuery(input, config);
            }
            case "websearch_to_tsquery": {
                String config = "english";
                String input;
                if (argv.size() == 2) {
                    config = String.valueOf(argv.get(0)).toLowerCase();
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
                TsQuery query;
                String options = null;
                if (argv.size() >= 4) {
                    Object cfg = argv.get(0);
                    Object doc = argv.get(1);
                    Object q = argv.get(2);
                    Object opt = argv.get(3);
                    if (cfg == null || doc == null || q == null || opt == null) return null;
                    config = cfg.toString();
                    document = doc.toString();
                    query = q instanceof TsQuery ? ((TsQuery) q) : TsQuery.parse(q.toString());
                    options = opt.toString();
                } else if (argv.size() == 3) {
                    Object first = argv.get(0);
                    Object second = argv.get(1);
                    Object third = argv.get(2);
                    if (first == null || second == null || third == null) return null;
                    if (third instanceof TsQuery) {
                        config = first.toString();
                        document = second.toString();
                        query = (TsQuery) third;
                    } else {
                        document = first.toString();
                        query = second instanceof TsQuery ? ((TsQuery) second) : TsQuery.parse(second.toString());
                        options = third.toString();
                    }
                } else {
                    Object doc = argv.get(0);
                    Object q = argv.get(1);
                    if (doc == null || q == null) return null;
                    document = doc.toString();
                    query = q instanceof TsQuery ? ((TsQuery) q) : TsQuery.parse(q.toString());
                }
                return TextSearchOperations.tsHeadline(document, query, options);
            }
            case "ts_rewrite": {
                Object queryObj = argv.get(0);
                Object targetObj = argv.get(1);
                Object subObj = argv.get(2);
                if (queryObj == null || targetObj == null || subObj == null) return null;
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
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : toTsVector(vecObj.toString());
                char weight = weightObj.toString().charAt(0);
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
                        if (!ws.isEmpty()) filterWeights.add(Character.toUpperCase(ws.charAt(0)));
                    }
                } else {
                    String ws = weightsObj.toString();
                    if (ws.startsWith("{") && ws.endsWith("}")) ws = ws.substring(1, ws.length() - 1);
                    for (String w : ws.split(",")) {
                        w = w.trim().replace("\"", "");
                        if (!w.isEmpty()) filterWeights.add(Character.toUpperCase(w.charAt(0)));
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
                    config = String.valueOf(argv.get(0));
                    input = String.valueOf(argv.get(1));
                } else {
                    input = String.valueOf(argv.get(0));
                }
                List<Object[]> debug = TextSearchOperations.tsDebug(input);
                if (debug.isEmpty()) return "";
                Object[] first = debug.get(0);
                return "(" + first[0] + ",\"" + first[1] + "\"," + first[2] + "," + first[3] + ",{" + first[5] + "})";
            }
            case "ts_lexize": {
                String dict = String.valueOf(argv.get(0));
                String token = String.valueOf(argv.get(1));
                List<String> result = TextSearchOperations.tsLexize(dict, token);
                return "{" + String.join(",", result) + "}";
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
            case "get_current_ts_config": {
                return TextSearchOperations.getCurrentTsConfig();
            }
            case "array_to_tsvector": {
                Object arrObj = argv.get(0);
                List<String> words = new ArrayList<>();
                if (arrObj instanceof List<?>) {
                    List<?> list = (List<?>) arrObj;
                    for (Object o : list) {
                        if (o == null) {
                            throw new MemgresException("null value not allowed for array_to_tsvector", "22004");
                        }
                        words.add(o.toString());
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
