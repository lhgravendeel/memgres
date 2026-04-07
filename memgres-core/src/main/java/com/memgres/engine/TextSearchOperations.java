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

    /** phraseto_tsquery: treats input as a phrase (words connected by <->). */
    public static TsQuery phraseToTsQuery(String input) {
        if (input == null || input.trim().isEmpty()) return TsQuery.term("");
        String[] words = input.trim().split("\\s+");
        List<String> filtered = new ArrayList<>();
        for (String w : words) {
            if (!w.isEmpty()) filtered.add(w);
        }
        if (filtered.isEmpty()) return TsQuery.term("");
        if (filtered.size() == 1) return TsQuery.term(filtered.get(0));
        TsQuery result = TsQuery.term(filtered.get(0));
        for (int i = 1; i < filtered.size(); i++) {
            result = TsQuery.phrase(result, TsQuery.term(filtered.get(i)), 1);
        }
        return result;
    }

    /** websearch_to_tsquery: Google-style query parsing. Quoted = phrase, - = NOT, OR = OR, rest = AND. */
    public static TsQuery websearchToTsQuery(String input) {
        if (input == null || input.trim().isEmpty()) return TsQuery.term("");
        List<TsQuery> parts = new ArrayList<>();
        Matcher quoteMatcher = Pattern.compile("\"([^\"]+)\"").matcher(input);
        String remaining = input;
        int lastEnd = 0;

        while (quoteMatcher.find()) {
            // Process unquoted text before this quote
            String before = remaining.substring(lastEnd, quoteMatcher.start());
            addWebsearchTerms(before, parts);
            // Process quoted phrase
            String phrase = quoteMatcher.group(1);
            TsQuery phraseQuery = phraseToTsQuery(phrase);
            parts.add(phraseQuery);
            lastEnd = quoteMatcher.end();
        }
        // Process remaining text after last quote
        if (lastEnd < remaining.length()) {
            addWebsearchTerms(remaining.substring(lastEnd), parts);
        }
        if (parts.isEmpty()) return TsQuery.term("");
        TsQuery result = parts.get(0);
        for (int i = 1; i < parts.size(); i++) {
            result = TsQuery.and(result, parts.get(i));
        }
        return result;
    }

    private static void addWebsearchTerms(String text, List<TsQuery> parts) {
        String[] tokens = text.trim().split("\\s+");
        for (int i = 0; i < tokens.length; i++) {
            String tok = tokens[i].trim();
            if (tok.isEmpty()) continue;
            if (tok.equalsIgnoreCase("OR") && !parts.isEmpty() && i + 1 < tokens.length) {
                // OR: combine previous with next
                String next = tokens[++i].trim();
                if (next.isEmpty()) continue;
                TsQuery prev = parts.remove(parts.size() - 1);
                parts.add(TsQuery.or(prev, TsQuery.term(next)));
            } else if (tok.startsWith("-") && tok.length() > 1) {
                parts.add(TsQuery.not(TsQuery.term(tok.substring(1))));
            } else {
                parts.add(TsQuery.term(tok));
            }
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
        String shortWord = "";
        String maxFragments = "0";

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
                    }
                }
            }
        }

        // Collect terms from query
        List<String> terms = query.collectTerms();
        Set<String> termSet = new HashSet<>(terms);

        String[] words = document.split("\\s+");
        StringBuilder sb = new StringBuilder();
        int shown = 0;
        boolean inFragment = false;
        int lastMatchIdx = -1;

        boolean outputtingMinWords = true; // track whether we're in the initial minWords segment
        for (int i = 0; i < words.length && shown < maxWords; i++) {
            String word = words[i];
            String stem = TsVector.simpleStem(word.toLowerCase().replaceAll("[^a-zA-Z0-9]", ""));
            boolean isMatch = termSet.contains(stem);
            if (isMatch) {
                lastMatchIdx = i;
                // Only add "..." separator when there's an actual gap (not during initial output)
                if (!inFragment && sb.length() > 0 && !outputtingMinWords) sb.append("... ");
                inFragment = true;
            }
            if (inFragment || shown < minWords) {
                if (isMatch) {
                    sb.append(startSel).append(word).append(stopSel);
                } else {
                    sb.append(word);
                }
                if (i < words.length - 1) sb.append(" ");
                shown++;
            } else {
                outputtingMinWords = false;
            }
            if (inFragment && !isMatch && i - lastMatchIdx > 5) {
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
        // Simple: if query matches target structure, replace
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

    /** ts_debug: returns debug info about text analysis. */
    public static List<Object[]> tsDebug(String text) {
        List<Object[]> result = new ArrayList<>();
        if (text == null) return result;
        String[] words = text.split("\\s+");
        for (String word : words) {
            String clean = word.toLowerCase().replaceAll("[^a-zA-Z0-9]", "");
            if (clean.isEmpty()) continue;
            String token = clean;
            String dictName = "english_stem";
            String lexeme = TsVector.simpleStem(clean);
            result.add(new Object[]{
                    "asciiword", "Word, all ASCII", token, dictName,
                    dictName, lexeme
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

    /** ts_parse: parse text into tokens. */
    public static List<Object[]> tsParse(String parserName, String text) {
        List<Object[]> result = new ArrayList<>();
        if (text == null) return result;
        String[] words = text.split("\\s+");
        for (String word : words) {
            if (!word.isEmpty()) {
                // tokid 1 = word, 12 = blank
                result.add(new Object[]{1, word});
            }
        }
        return result;
    }

    /** ts_token_type: list token types for a parser. */
    public static List<Object[]> tsTokenType(String parserName) {
        return Cols.listOf(
                new Object[]{1, "asciiword", "Word, all ASCII"},
                new Object[]{2, "word", "Word, all letters"},
                new Object[]{3, "numword", "Word, letters and digits"},
                new Object[]{4, "email", "Email address"},
                new Object[]{5, "url", "URL"},
                new Object[]{6, "host", "Host"},
                new Object[]{7, "sfloat", "Scientific notation"},
                new Object[]{8, "version", "Version number"},
                new Object[]{9, "hyphword", "Hyphenated word"},
                new Object[]{10, "numhword", "Hyphenated word, letters and digits"},
                new Object[]{11, "asciihword", "Hyphenated word, all ASCII"},
                new Object[]{12, "blank", "Space symbols"},
                new Object[]{13, "tag", "XML Tag"},
                new Object[]{14, "protocol", "Protocol head"},
                new Object[]{15, "int", "Signed integer"},
                new Object[]{16, "float", "Decimal notation"},
                new Object[]{17, "uint", "Unsigned integer"},
                new Object[]{18, "entity", "XML entity"},
                new Object[]{19, "hword_asciipart", "Hyphenated word part, all ASCII"},
                new Object[]{20, "hword_part", "Hyphenated word part, all letters"},
                new Object[]{21, "hword_numpart", "Hyphenated word part, letters and digits"},
                new Object[]{22, "url_path", "URL path"},
                new Object[]{23, "file", "File or path name"}
        );
    }

    /** ts_stat: statistics for a tsvector column query. */
    public static List<Object[]> tsStat(String sqlResult) {
        // Returns word | ndoc | nentry
        // Since we can't execute SQL here, return empty
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
            // Format as arrays
            String posArr = "{" + positions.stream().map(String::valueOf).reduce((a, b) -> a + "," + b).orElse("") + "}";
            String wArr = "{" + weights.stream().map(String::valueOf).reduce((a, b) -> a + "," + b).orElse("") + "}";
            result.add(new Object[]{entry.getKey(), posArr, wArr});
        }
        return result;
    }
}
