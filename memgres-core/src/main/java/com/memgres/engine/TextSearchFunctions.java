package com.memgres.engine;

import com.memgres.engine.util.Cols;

import com.memgres.engine.parser.ast.*;
import java.util.*;

/**
 * Text search function evaluation, extracted from FunctionEvaluator to reduce class size.
 */
class TextSearchFunctions {
    private static final Object NOT_HANDLED = FunctionEvaluator.NOT_HANDLED;

    private final AstExecutor executor;

    TextSearchFunctions(AstExecutor executor) {
        this.executor = executor;
    }

    Object eval(String name, FunctionCallExpr fn, RowContext ctx) {
        switch (name) {
            case "to_tsvector": {
                if (fn.args().size() == 2) {
                    String configName = String.valueOf(executor.evalExpr(fn.args().get(0), ctx)).toLowerCase();
                    java.util.Set<String> validConfigs = Cols.setOf(
                            "simple", "english", "german", "french", "spanish", "italian",
                            "portuguese", "dutch", "finnish", "swedish", "danish", "norwegian",
                            "russian", "romanian", "hungarian", "turkish", "arabic", "nepali",
                            "irish", "indonesian", "lithuanian", "greek", "hindi", "basque");
                    if (!validConfigs.contains(configName)) {
                        throw new MemgresException("text search configuration \"" + configName + "\" does not exist", "42704");
                    }
                    Object text = executor.evalExpr(fn.args().get(1), ctx);
                    return text == null ? null : TsVector.fromText(text.toString());
                }
                Object text = executor.evalExpr(fn.args().get(0), ctx);
                return text == null ? null : TsVector.fromText(text.toString());
            }
            case "to_tsquery": {
                Object tsqText;
                if (fn.args().size() == 2) {
                    tsqText = executor.evalExpr(fn.args().get(1), ctx);
                } else {
                    tsqText = executor.evalExpr(fn.args().get(0), ctx);
                }
                if (tsqText == null) return null;
                String tsqStr = tsqText.toString();
                if (tsqStr.matches("(?s).*[&|]\\s*[&|].*")) {
                    throw new MemgresException("syntax error in tsquery: \"" + tsqStr + "\"", "42601");
                }
                return TsQuery.parse(tsqStr);
            }
            case "plainto_tsquery": {
                String input;
                if (fn.args().size() == 2) {
                    input = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                } else {
                    input = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                }
                String[] words = input.split("\\s+");
                return TsQuery.parse(String.join(" & ", words));
            }
            case "phraseto_tsquery": {
                String input;
                if (fn.args().size() == 2) {
                    input = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                } else {
                    input = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                }
                return TextSearchOperations.phraseToTsQuery(input);
            }
            case "websearch_to_tsquery": {
                String input;
                if (fn.args().size() == 2) {
                    input = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                } else {
                    input = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                }
                return TextSearchOperations.websearchToTsQuery(input);
            }
            case "ts_rank": {
                // ts_rank([weights,] tsvector, tsquery [, normalization])
                int argStart = 0;
                if (fn.args().size() >= 3) {
                    Object first = executor.evalExpr(fn.args().get(0), ctx);
                    if (first instanceof String && ((String) first).startsWith("{")) argStart = 1;
                }
                Object vecObj = executor.evalExpr(fn.args().get(argStart), ctx);
                Object queryObj = executor.evalExpr(fn.args().get(argStart + 1), ctx);
                if (vecObj == null || queryObj == null) return 0.0f;
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : TsVector.fromText(vecObj.toString());
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                float rankVal = (float) vec.rank(query);
                // Round to 6 significant digits to match PG float4 display
                return Float.parseFloat(String.format(java.util.Locale.US, "%.6g", rankVal));
            }
            case "ts_rank_cd": {
                int argStart = 0;
                if (fn.args().size() >= 3) {
                    Object first = executor.evalExpr(fn.args().get(0), ctx);
                    if (first instanceof String && ((String) first).startsWith("{")) argStart = 1;
                }
                Object vecObj = executor.evalExpr(fn.args().get(argStart), ctx);
                Object queryObj = executor.evalExpr(fn.args().get(argStart + 1), ctx);
                if (vecObj == null || queryObj == null) return 0.0f;
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : TsVector.fromText(vecObj.toString());
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                float rankCdVal = (float) vec.rankCd(query);
                // Round to 6 significant digits to match PG float4 display
                return Float.parseFloat(String.format(java.util.Locale.US, "%.6g", rankCdVal));
            }
            case "ts_headline": {
                // ts_headline([config,] document, tsquery [, options])
                String config = null;
                String document;
                TsQuery query;
                String options = null;
                if (fn.args().size() >= 4) {
                    config = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                    document = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                    Object q = executor.evalExpr(fn.args().get(2), ctx);
                    query = q instanceof TsQuery ? ((TsQuery) q) : TsQuery.parse(q.toString());
                    options = String.valueOf(executor.evalExpr(fn.args().get(3), ctx));
                } else if (fn.args().size() == 3) {
                    Object first = executor.evalExpr(fn.args().get(0), ctx);
                    Object second = executor.evalExpr(fn.args().get(1), ctx);
                    Object third = executor.evalExpr(fn.args().get(2), ctx);
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
                    document = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                    Object q = executor.evalExpr(fn.args().get(1), ctx);
                    query = q instanceof TsQuery ? ((TsQuery) q) : TsQuery.parse(q.toString());
                }
                return TextSearchOperations.tsHeadline(document, query, options);
            }
            case "ts_rewrite": {
                Object queryObj = executor.evalExpr(fn.args().get(0), ctx);
                Object targetObj = executor.evalExpr(fn.args().get(1), ctx);
                Object subObj = executor.evalExpr(fn.args().get(2), ctx);
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                TsQuery target = targetObj instanceof TsQuery ? ((TsQuery) targetObj) : TsQuery.parse(targetObj.toString());
                TsQuery sub = subObj instanceof TsQuery ? ((TsQuery) subObj) : TsQuery.parse(subObj.toString());
                return TextSearchOperations.tsRewrite(query, target, sub);
            }
            case "strip": {
                Object arg = executor.evalExpr(fn.args().get(0), ctx);
                if (arg instanceof TsVector) return ((TsVector) arg).strip();
                return arg != null ? arg.toString() : null;
            }
            case "setweight": {
                Object vecObj = executor.evalExpr(fn.args().get(0), ctx);
                Object weightObj = executor.evalExpr(fn.args().get(1), ctx);
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : TsVector.fromText(vecObj.toString());
                char weight = weightObj.toString().charAt(0);
                if (fn.args().size() >= 3) {
                    // setweight(tsvector, weight, lexemes[])
                    Object lexArr = executor.evalExpr(fn.args().get(2), ctx);
                    List<String> filterLexemes = new ArrayList<>();
                    if (lexArr instanceof List<?>) {
                        for (Object o : (List<?>) lexArr) filterLexemes.add(o.toString());
                    } else if (lexArr instanceof String) {
                        String s = (String) lexArr;
                        // Parse array literal {word1,word2}
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
                Object vecObj = executor.evalExpr(fn.args().get(0), ctx);
                Object toDelete = executor.evalExpr(fn.args().get(1), ctx);
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : TsVector.fromText(vecObj.toString());
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
                Object vecObj = executor.evalExpr(fn.args().get(0), ctx);
                Object weightsObj = executor.evalExpr(fn.args().get(1), ctx);
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : TsVector.fromText(vecObj.toString());
                Set<Character> filterWeights = new HashSet<>();
                String ws = weightsObj.toString();
                if (ws.startsWith("{") && ws.endsWith("}")) ws = ws.substring(1, ws.length() - 1);
                for (String w : ws.split(",")) {
                    w = w.trim().replace("\"", "");
                    if (!w.isEmpty()) filterWeights.add(w.charAt(0));
                }
                return vec.filter(filterWeights);
            }
            case "tsquery_phrase": {
                Object leftObj = executor.evalExpr(fn.args().get(0), ctx);
                Object rightObj = executor.evalExpr(fn.args().get(1), ctx);
                TsQuery left = leftObj instanceof TsQuery ? ((TsQuery) leftObj) : TsQuery.parse(leftObj.toString());
                TsQuery right = rightObj instanceof TsQuery ? ((TsQuery) rightObj) : TsQuery.parse(rightObj.toString());
                int dist = fn.args().size() >= 3 ? executor.toInt(executor.evalExpr(fn.args().get(2), ctx)) : 1;
                return TextSearchOperations.tsqueryPhrase(left, right, dist);
            }
            case "numnode": {
                Object queryObj = executor.evalExpr(fn.args().get(0), ctx);
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                return query.numNode();
            }
            case "querytree": {
                Object queryObj = executor.evalExpr(fn.args().get(0), ctx);
                TsQuery query = queryObj instanceof TsQuery ? ((TsQuery) queryObj) : TsQuery.parse(queryObj.toString());
                return query.queryTree();
            }
            case "ts_debug": {
                String config = "english";
                String input;
                if (fn.args().size() == 2) {
                    config = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                    input = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                } else {
                    input = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                }
                List<Object[]> debug = TextSearchOperations.tsDebug(input);
                // Return first result as a string representation
                if (debug.isEmpty()) return "";
                Object[] first = debug.get(0);
                return "(" + first[0] + ",\"" + first[1] + "\"," + first[2] + "," + first[3] + ",{" + first[5] + "})";
            }
            case "ts_lexize": {
                String dict = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                String token = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                List<String> result = TextSearchOperations.tsLexize(dict, token);
                return "{" + String.join(",", result) + "}";
            }
            case "ts_token_type": {
                String parser = fn.args().isEmpty() ? "default" : String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                List<Object[]> types = TextSearchOperations.tsTokenType(parser);
                StringBuilder sb = new StringBuilder();
                for (Object[] t : types) {
                    if (sb.length() > 0) sb.append(",");
                    sb.append("(").append(t[0]).append(",").append(t[1]).append(",\"").append(t[2]).append("\")");
                }
                return "(" + sb + ")";
            }
            case "ts_parse": {
                String tsParser = String.valueOf(executor.evalExpr(fn.args().get(0), ctx));
                String text = String.valueOf(executor.evalExpr(fn.args().get(1), ctx));
                List<Object[]> tokens = TextSearchOperations.tsParse(tsParser, text);
                StringBuilder sb = new StringBuilder();
                for (Object[] t : tokens) {
                    if (sb.length() > 0) sb.append(",");
                    sb.append("(").append(t[0]).append(",\"").append(t[1]).append("\")");
                }
                return "(" + sb + ")";
            }
            case "ts_stat": {
                return null; // Cannot execute sub-queries
            }
            case "get_current_ts_config": {
                return TextSearchOperations.getCurrentTsConfig();
            }
            case "array_to_tsvector": {
                Object arrObj = executor.evalExpr(fn.args().get(0), ctx);
                List<String> words = new ArrayList<>();
                if (arrObj instanceof List<?>) {
                    List<?> list = (List<?>) arrObj;
                    for (Object o : list) words.add(o.toString());
                } else if (arrObj instanceof String && ((String) arrObj).startsWith("{") && ((String) arrObj).endsWith("}")) {
                    String s = (String) arrObj;
                    for (String w : s.substring(1, s.length() - 1).split(",")) {
                        words.add(w.trim());
                    }
                }
                return TsVector.fromArray(words);
            }
            case "tsvector_to_array": {
                Object vecObj = executor.evalExpr(fn.args().get(0), ctx);
                TsVector vec = vecObj instanceof TsVector ? ((TsVector) vecObj) : TsVector.fromText(vecObj.toString());
                List<String> arr = vec.toArray();
                return "{" + String.join(",", arr) + "}";
            }
            default:
                return NOT_HANDLED;
        }
    }
}
