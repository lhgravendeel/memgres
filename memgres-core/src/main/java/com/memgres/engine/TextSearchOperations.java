package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Full-text search operations for PostgreSQL compatibility.
 * Implements tsvector/tsquery functions extracted from FunctionEvaluator.
 */
public class TextSearchOperations {

    /**
     * The {@code @@} match. PostgreSQL declares it both ways round — tsvector @@ tsquery and
     * tsquery @@ tsvector — so which operand is the document and which is the query follows from
     * what they are, not from the side they were written on. Reading them in written order made
     * {@code 'a'::tsquery @@ to_tsvector(...)} try to parse a vector as a query.
     *
     * <p>An operand that declares neither type is PostgreSQL's unknown and is read as whatever the
     * other side leaves it: a vector opposite a query, a query opposite a vector.
     */
    public static Object matches(Object left, Object right) {
        if (left == null || right == null) return null;
        // One of each is the only pairing PG declares; two of a kind matches nothing
        if (left instanceof TsQuery && right instanceof TsQuery) {
            throw new MemgresException("operator does not exist: tsquery @@ tsquery", "42883");
        }
        if (left instanceof TsVector && right instanceof TsVector) {
            throw new MemgresException("operator does not exist: tsvector @@ tsvector", "42883");
        }
        boolean queryOnLeft = left instanceof TsQuery
                || (right instanceof TsVector && !(left instanceof TsVector));
        Object documentSide = queryOnLeft ? right : left;
        Object querySide = queryOnLeft ? left : right;
        TsVector vector;
        if (documentSide instanceof TsVector) {
            vector = (TsVector) documentSide;
        } else {
            String text = documentSide.toString();
            TsVector parsed = TsVector.parseLiteral(text);
            vector = parsed != null ? parsed : TsVector.fromText(text);
        }
        TsQuery query = querySide instanceof TsQuery
                ? (TsQuery) querySide : TsQuery.parse(querySide.toString());
        return vector.matches(query);
    }

    /** phraseto_tsquery: treats input as a phrase (words connected by <N> where N accounts for stopwords).
     *  Stopwords are removed and their positions are accounted for by increasing the distance. */
    public static TsQuery phraseToTsQuery(String input) {
        return phraseToTsQuery(input, "english");
    }

    public static TsQuery phraseToTsQuery(String input, String config) {
        if (input == null || input.trim().isEmpty()) return TsQuery.emptyQuery();
        boolean isSimple = "simple".equalsIgnoreCase(config);

        // Strip punctuation and tokenize
        String cleaned = input.replaceAll("[^a-zA-Z0-9\\s]", " ");
        String[] words = cleaned.trim().split("\\s+");

        List<String> stems = new ArrayList<>();
        List<Integer> origPositions = new ArrayList<>(); // original word index (1-based)
        int wordIdx = 0;
        for (String w : words) {
            if (w.isEmpty()) continue;
            wordIdx++;
            String lower = w.toLowerCase();
            if (isSimple) {
                stems.add(lower);
                origPositions.add(wordIdx);
            } else {
                if (TsVector.isStopWord(lower)) continue;
                stems.add(TsVector.simpleStem(lower));
                origPositions.add(wordIdx);
            }
        }
        if (stems.isEmpty()) return TsQuery.emptyQuery();
        if (stems.size() == 1) return TsQuery.termRaw(stems.get(0));

        // Build phrase chain with correct distances accounting for removed stopwords
        TsQuery result = TsQuery.termRaw(stems.get(0));
        for (int i = 1; i < stems.size(); i++) {
            int distance = origPositions.get(i) - origPositions.get(i - 1);
            result = TsQuery.phrase(result, TsQuery.termRaw(stems.get(i)), distance);
        }
        return result;
    }

    /** plainto_tsquery: words joined by AND, no special chars, strip punctuation. */
    public static TsQuery plainToTsQuery(String input, String config) {
        if (input == null || input.trim().isEmpty()) return TsQuery.emptyQuery();
        boolean isSimple = "simple".equalsIgnoreCase(config);

        // Strip punctuation
        String cleaned = input.replaceAll("[^a-zA-Z0-9\\s]", " ");
        String[] words = cleaned.trim().split("\\s+");
        List<TsQuery> terms = new ArrayList<>();
        for (String w : words) {
            if (w.isEmpty()) continue;
            String lower = w.toLowerCase();
            if (isSimple) {
                terms.add(TsQuery.termRaw(lower));
            } else {
                if (TsVector.isStopWord(lower)) continue;
                terms.add(TsQuery.termRaw(TsVector.simpleStem(lower)));
            }
        }
        if (terms.isEmpty()) return TsQuery.emptyQuery();
        TsQuery result = terms.get(0);
        for (int i = 1; i < terms.size(); i++) {
            result = TsQuery.and(result, terms.get(i));
        }
        return result;
    }

    /** websearch_to_tsquery: Google-style query parsing. Quoted = phrase, - = NOT, OR = OR, rest = AND. */
    public static TsQuery websearchToTsQuery(String input) {
        return websearchToTsQuery(input, "english");
    }

    public static TsQuery websearchToTsQuery(String input, String config) {
        if (input == null || input.trim().isEmpty()) return TsQuery.emptyQuery();
        List<TsQuery> parts = new ArrayList<>();
        int i = 0;
        String s = input.trim();

        while (i < s.length()) {
            // Skip whitespace
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
            if (i >= s.length()) break;

            char c = s.charAt(i);

            if (c == '"') {
                // Quoted phrase
                int end = s.indexOf('"', i + 1);
                if (end < 0) end = s.length();
                String phrase = s.substring(i + 1, end);
                i = end + 1;
                TsQuery pq = phraseToTsQuery(phrase, config);
                if (!pq.isEmpty()) {
                    addWebPart(parts, pq, false);
                }
            } else if (c == '-' && i + 1 < s.length()) {
                i++;
                // Negation
                if (i < s.length() && s.charAt(i) == '"') {
                    // -"phrase"
                    int end = s.indexOf('"', i + 1);
                    if (end < 0) end = s.length();
                    String phrase = s.substring(i + 1, end);
                    i = end + 1;
                    TsQuery pq = phraseToTsQuery(phrase, config);
                    if (!pq.isEmpty()) {
                        addWebPart(parts, TsQuery.not(pq), false);
                    }
                } else {
                    // -word
                    StringBuilder sb = new StringBuilder();
                    while (i < s.length() && !Character.isWhitespace(s.charAt(i)) && s.charAt(i) != '"') {
                        sb.append(s.charAt(i));
                        i++;
                    }
                    String word = sb.toString();
                    if (!word.isEmpty()) {
                        String cleaned = word.replaceAll("[^a-zA-Z0-9]", "");
                        if (!cleaned.isEmpty()) {
                            TsQuery tq = "simple".equalsIgnoreCase(config)
                                ? TsQuery.termRaw(cleaned.toLowerCase())
                                : TsQuery.term(cleaned);
                            if (!tq.isEmpty()) {
                                addWebPart(parts, TsQuery.not(tq), false);
                            }
                        }
                    }
                }
            } else {
                // Regular word or OR
                StringBuilder sb = new StringBuilder();
                while (i < s.length() && !Character.isWhitespace(s.charAt(i)) && s.charAt(i) != '"') {
                    sb.append(s.charAt(i));
                    i++;
                }
                String tok = sb.toString();
                if (tok.equalsIgnoreCase("or")) {
                    // Mark last part as needing OR with next
                    if (!parts.isEmpty()) {
                        // Set flag for OR
                        addWebPart(parts, null, true);
                    }
                } else {
                    String cleaned = tok.replaceAll("[^a-zA-Z0-9]", "");
                    if (!cleaned.isEmpty()) {
                        TsQuery tq = "simple".equalsIgnoreCase(config)
                            ? TsQuery.termRaw(cleaned.toLowerCase())
                            : TsQuery.term(cleaned);
                        if (!tq.isEmpty()) {
                            addWebPart(parts, tq, false);
                        }
                    }
                }
            }
        }

        // Build final query: process OR markers
        List<TsQuery> finalParts = new ArrayList<>();
        for (int j = 0; j < parts.size(); j++) {
            TsQuery p = parts.get(j);
            if (p == null) continue; // OR marker
            if (j + 2 < parts.size() && parts.get(j + 1) == null) {
                // This part OR next part
                TsQuery next = parts.get(j + 2);
                if (next != null) {
                    finalParts.add(TsQuery.or(p, next));
                    j += 2;
                    continue;
                }
            }
            finalParts.add(p);
        }

        if (finalParts.isEmpty()) return TsQuery.emptyQuery();
        TsQuery result = finalParts.get(0);
        for (int j = 1; j < finalParts.size(); j++) {
            result = TsQuery.and(result, finalParts.get(j));
        }
        return result;
    }

    private static void addWebPart(List<TsQuery> parts, TsQuery q, boolean isOr) {
        if (isOr) {
            parts.add(null); // OR marker
        } else {
            parts.add(q);
        }
    }

    /** tsquery_phrase: create a phrase query from two tsqueries with given distance. */
    public static TsQuery tsqueryPhrase(TsQuery left, TsQuery right, int distance) {
        return TsQuery.phrase(left, right, distance);
    }

    /** ts_headline: generate a headline with matching terms highlighted. */
    public static String tsHeadline(String document, TsQuery query, String options) {
        if (document == null || query == null) return "";
        // Parse options
        String startSel = "<b>";
        String stopSel = "</b>";
        int maxWords = 35;
        int minWords = 15;
        boolean highlightAll = false;

        if (options != null) {
            for (String opt : options.split(",")) {
                String[] kv = opt.trim().split("=", 2);
                if (kv.length == 2) {
                    String key = kv[0].trim().toLowerCase();
                    String val = kv[1].trim();
                    switch (key) {
                        case "startsel":
                            startSel = val;
                            break;
                        case "stopsel":
                            stopSel = val;
                            break;
                        case "maxwords":
                            maxWords = Integer.parseInt(val);
                            break;
                        case "minwords":
                            minWords = Integer.parseInt(val);
                            break;
                        case "highlightall":
                            highlightAll = val.equalsIgnoreCase("true") || val.equals("1");
                            break;
                    }
                }
            }
        }

        // Strip HTML tags from the document for matching. PG replaces each tag
        // with a space so adjacent words don't run together (e.g. big<br>cat).
        String stripped = document.replaceAll("<[^>]+>", " ");

        // Collect terms from query
        List<String> terms = query.collectTerms();
        Set<String> termSet = new HashSet<>(terms);

        String[] words = stripped.split("\\s+");
        StringBuilder sb = new StringBuilder();
        int shown = 0;
        boolean inFragment = false;
        int lastMatchIdx = -1;
        boolean outputtingMinWords = true;

        for (int idx = 0; idx < words.length && (highlightAll || shown < maxWords); idx++) {
            String word = words[idx];
            String stem = TsVector.simpleStem(word.toLowerCase().replaceAll("[^a-zA-Z0-9]", ""));
            boolean isMatch = termSet.contains(stem);
            if (isMatch) {
                lastMatchIdx = idx;
                if (!inFragment && sb.length() > 0 && !outputtingMinWords) sb.append("... ");
                inFragment = true;
            }
            if (inFragment || shown < minWords || highlightAll) {
                if (isMatch) {
                    sb.append(startSel).append(word).append(stopSel);
                } else {
                    sb.append(word);
                }
                if (idx < words.length - 1) sb.append(" ");
                shown++;
            } else {
                outputtingMinWords = false;
            }
            if (inFragment && !isMatch && idx - lastMatchIdx > 5) {
                inFragment = false;
                outputtingMinWords = false;
            }
        }
        return sb.toString().trim();
    }

    /** ts_headline with default options. */
    public static String tsHeadline(String document, TsQuery query) {
        return tsHeadline(document, query, null);
    }

    /** ts_rewrite: replace occurrences of target query with substitute in query tree. */
    public static TsQuery tsRewrite(TsQuery query, TsQuery target, TsQuery substitute) {
        if (queryEquals(query, target)) return substitute;
        if (query.getOp() == TsQuery.Op.TERM) return query;
        if (query.getOp() == TsQuery.Op.NOT) {
            TsQuery rewritten = tsRewrite(query.getLeft(), target, substitute);
            return TsQuery.not(rewritten);
        }
        TsQuery newLeft = tsRewrite(query.getLeft(), target, substitute);
        TsQuery newRight = query.getRight() != null ? tsRewrite(query.getRight(), target, substitute) : null;
        if (newRight == null) return newLeft;
        switch (query.getOp()) {
            case AND:
                return TsQuery.and(newLeft, newRight);
            case OR:
                return TsQuery.or(newLeft, newRight);
            case PHRASE:
                return TsQuery.phrase(newLeft, newRight, query.getPhraseDistance());
            default:
                return query;
        }
    }

    private static boolean queryEquals(TsQuery a, TsQuery b) {
        if (a.getOp() != b.getOp()) return false;
        if (a.getOp() == TsQuery.Op.TERM) {
            return Objects.equals(a.getTerm(), b.getTerm());
        }
        if (a.getLeft() != null && b.getLeft() != null && !queryEquals(a.getLeft(), b.getLeft())) return false;
        if (a.getRight() != null && b.getRight() != null && !queryEquals(a.getRight(), b.getRight())) return false;
        return true;
    }

    /** ts_debug: one row per parser token, with the dictionary that handled it. */
    public static List<Object[]> tsDebug(String text) {
        List<Object[]> result = new ArrayList<>();
        if (text == null) return result;
        for (com.memgres.engine.fts.TsParser.Token token
                : com.memgres.engine.fts.TsParser.parse(text)) {
            com.memgres.engine.fts.TsParser.Dict dict =
                    com.memgres.engine.fts.TsParser.dictionaryFor(token.type());
            String dictName;
            String lexemes;
            String lower = token.text().toLowerCase();
            switch (dict) {
                case STEM:
                    dictName = "english_stem";
                    lexemes = TsVector.isStopWord(lower)
                            ? "{}" : "{" + TsVector.simpleStem(lower) + "}";
                    break;
                case SIMPLE:
                    dictName = "simple";
                    lexemes = "{" + lower + "}";
                    break;
                default:
                    // No dictionary is configured for this token type.
                    dictName = null;
                    lexemes = null;
                    break;
            }
            result.add(new Object[]{
                    token.type().alias(), token.type().description(), token.text(),
                    dictName == null ? "{}" : "{" + dictName + "}", dictName, lexemes
            });
        }
        return result;
    }

    /** ts_lexize: return lexemes for a word using a dictionary. */
    public static List<String> tsLexize(String dict, String token) {
        if (token == null) return Cols.listOf();
        String stem = TsVector.simpleStem(token.toLowerCase());
        return Cols.listOf(stem);
    }

    /** ts_parse: the parser's own token stream, as (tokid, token) pairs. */
    public static List<Object[]> tsParse(String parserName, String text) {
        List<Object[]> result = new ArrayList<>();
        if (text == null) return result;
        for (com.memgres.engine.fts.TsParser.Token token
                : com.memgres.engine.fts.TsParser.parse(text)) {
            result.add(new Object[]{token.type().id(), token.text()});
        }
        return result;
    }

    /** ts_token_type: the token types the default parser can emit. */
    public static List<Object[]> tsTokenType(String parserName) {
        List<Object[]> result = new ArrayList<>();
        for (com.memgres.engine.fts.TsParser.Type t : com.memgres.engine.fts.TsParser.tokenTypes()) {
            result.add(new Object[]{t.id(), t.alias(), t.description()});
        }
        return result;
    }

    /** ts_stat: statistics for a tsvector column query. */
    public static List<Object[]> tsStat(String sqlResult) {
        return Cols.listOf();
    }

    /** get_current_ts_config: return current text search configuration. */
    public static String getCurrentTsConfig() {
        return "english";
    }

    /** unnest(tsvector): returns rows of (lexeme, positions, weights). */
    public static List<Object[]> unnestTsVector(TsVector vec) {
        List<Object[]> result = new ArrayList<>();
        for (Map.Entry<String, List<TsVector.PosEntry>> entry : vec.getLexemeMap().entrySet()) {
            List<Integer> positions = new ArrayList<>();
            List<Character> weights = new ArrayList<>();
            for (TsVector.PosEntry pe : entry.getValue()) {
                positions.add(pe.position());
                weights.add(pe.weight());
            }
            String posArr = "{" + positions.stream().map(String::valueOf).reduce((a, b) -> a + "," + b).orElse("") + "}";
            String wArr = "{" + weights.stream().map(String::valueOf).reduce((a, b) -> a + "," + b).orElse("") + "}";
            result.add(new Object[]{entry.getKey(), posArr, wArr});
        }
        return result;
    }
}
