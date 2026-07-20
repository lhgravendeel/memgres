package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * PostgreSQL-compatible tsvector implementation for full-text search.
 * Stores normalized lexemes with positions and optional weights (A/B/C/D).
 */
public class TsVector {

    /** Max position value per lexeme (PG caps at 16383, we use 256 for position list length cap). */
    static final int MAX_POSITION = 16383;

    static final Set<String> STOP_WORDS_SET = Cols.setOf(
            "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "do", "does", "did", "will", "would", "could",
            "should", "may", "might", "can", "shall", "to", "of", "in", "for",
            "on", "with", "at", "by", "from", "as", "into", "about", "between",
            "through", "during", "before", "after", "above", "below",
            "and", "but", "or", "nor", "not", "so", "yet",
            "it", "its", "this", "that", "these", "those"
    );

    static boolean isStopWord(String word) {
        return STOP_WORDS_SET.contains(word.toLowerCase());
    }

    private static final Set<String> STOP_WORDS = STOP_WORDS_SET;

    /** A position entry: position number + weight character (D is default/lowest). */
    public static final class PosEntry {
        public final int position;
        public final char weight;

        public PosEntry(int position, char weight) {
            this.position = position;
            this.weight = weight;
        }

        public PosEntry(int position) {
            this(position, 'D');
        }

        public int position() { return position; }
        public char weight() { return weight; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PosEntry that = (PosEntry) o;
            return position == that.position
                && weight == that.weight;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(position, weight);
        }

        @Override
        public String toString() {
            return "PosEntry[position=" + position + ", " + "weight=" + weight + "]";
        }
    }

    /** lexeme -> list of (position, weight) */
    private final Map<String, List<PosEntry>> lexemes;

    public TsVector(Map<String, List<PosEntry>> lexemes) {
        this.lexemes = new TreeMap<>(lexemes);
    }

    /** Create an empty tsvector. */
    public static TsVector empty() {
        return new TsVector(new TreeMap<>());
    }

    /** Build a tsvector from plain text using english config (tokenize, stem, remove stop words). */
    public static TsVector fromText(String text) {
        return fromText(text, "english");
    }

    /** Build a tsvector from plain text with specified config. */
    public static TsVector fromText(String text, String config) {
        Map<String, List<PosEntry>> lexemes = new TreeMap<>();
        if (text == null || text.isEmpty()) return new TsVector(lexemes);

        boolean isSimple = "simple".equalsIgnoreCase(config);

        String[] words = text.toLowerCase().split("[^a-zA-Z0-9]+");
        int position = 0;
        for (String word : words) {
            if (word.isEmpty()) continue;
            position++;  // count position for every word including stop words
            if (isSimple) {
                // simple config: no stemming, no stopword removal, just lowercase
                addPosition(lexemes, word, position);
            } else {
                // english config: stem and remove stop words
                if (STOP_WORDS.contains(word)) continue;
                String stem = simpleStem(word);
                addPosition(lexemes, stem, position);
            }
        }
        return new TsVector(lexemes);
    }

    private static void addPosition(Map<String, List<PosEntry>> lexemes, String lexeme, int position) {
        if (position > MAX_POSITION) position = MAX_POSITION;
        List<PosEntry> entries = lexemes.computeIfAbsent(lexeme, k -> new ArrayList<>());
        // PG caps at 256 positions per lexeme
        if (entries.size() < 256) {
            entries.add(new PosEntry(position));
        }
    }

    /** Parse a tsvector literal. Supports both quoted and unquoted lexemes:
     *  'cat':1A,2B 'dog':3 fat:4C word
     *  PG preserves case of quoted lexemes. Position 0 is rejected (42601). */
    public static TsVector parseLiteral(String input) {
        if (input == null || input.isEmpty()) return null;
        Map<String, List<PosEntry>> lexemes = new TreeMap<>();
        int i = 0;
        int len = input.length();
        while (i < len) {
            // Skip whitespace
            while (i < len && Character.isWhitespace(input.charAt(i))) i++;
            if (i >= len) break;

            String lexeme;
            if (input.charAt(i) == '\'') {
                // Quoted lexeme: 'word' or 'word''s'
                i++; // skip opening quote
                StringBuilder sb = new StringBuilder();
                while (i < len) {
                    char c = input.charAt(i);
                    if (c == '\'' && i + 1 < len && input.charAt(i + 1) == '\'') {
                        sb.append('\'');
                        i += 2;
                    } else if (c == '\'') {
                        i++; // skip closing quote
                        break;
                    } else if (c == '\\' && i + 1 < len) {
                        sb.append(input.charAt(i + 1));
                        i += 2;
                    } else {
                        sb.append(c);
                        i++;
                    }
                }
                lexeme = sb.toString(); // preserve case for quoted
            } else {
                // Unquoted lexeme: read until whitespace or colon
                StringBuilder sb = new StringBuilder();
                while (i < len && !Character.isWhitespace(input.charAt(i)) && input.charAt(i) != ':') {
                    sb.append(input.charAt(i));
                    i++;
                }
                if (sb.length() == 0) { i++; continue; }
                lexeme = sb.toString(); // unquoted: keep as-is (PG lowercases but we match behavior)
            }

            // Parse optional positions: :1A,2B,3
            List<PosEntry> entries = new ArrayList<>();
            if (i < len && input.charAt(i) == ':') {
                i++; // skip ':'
                while (i < len && !Character.isWhitespace(input.charAt(i))) {
                    // Parse one position entry
                    StringBuilder numSb = new StringBuilder();
                    while (i < len && Character.isDigit(input.charAt(i))) {
                        numSb.append(input.charAt(i));
                        i++;
                    }
                    char weight = 'D';
                    if (i < len && Character.isLetter(input.charAt(i)) && "AaBbCcDd".indexOf(input.charAt(i)) >= 0) {
                        weight = Character.toUpperCase(input.charAt(i));
                        i++;
                    }
                    if (numSb.length() > 0) {
                        int pos = Integer.parseInt(numSb.toString());
                        if (pos == 0) {
                            throw new MemgresException("wrong position info in tsvector: \"" + input + "\"", "42601");
                        }
                        if (pos > MAX_POSITION) pos = MAX_POSITION;
                        if (entries.size() < 256) {
                            entries.add(new PosEntry(pos, weight));
                        }
                    }
                    // Skip comma separator
                    if (i < len && input.charAt(i) == ',') {
                        i++;
                    } else {
                        break;
                    }
                }
            }
            lexemes.put(lexeme, entries);
        }
        return lexemes.isEmpty() ? null : new TsVector(lexemes);
    }

    public boolean matches(TsQuery query) {
        return query.matches(this);
    }

    /** Check if lexeme exists. For tsvector produced via parseLiteral, exact match; for stemmed, stem first. */
    public boolean containsLexeme(String lexeme) {
        // Try exact match first (for literal tsvectors)
        if (lexemes.containsKey(lexeme)) return true;
        // Then try stemmed match
        return lexemes.containsKey(simpleStem(lexeme.toLowerCase()));
    }

    /** Check if lexeme exists (exact match only, no stemming). */
    public boolean containsLexemeExact(String lexeme) {
        return lexemes.containsKey(lexeme);
    }

    public Set<String> getLexemes() {
        return lexemes.keySet();
    }

    public Map<String, List<PosEntry>> getLexemeMap() {
        return lexemes;
    }

    /** Get all positions for a lexeme. */
    public List<Integer> getPositions(String lexeme) {
        List<PosEntry> entries = lexemes.get(lexeme);
        if (entries == null) {
            entries = lexemes.get(simpleStem(lexeme.toLowerCase()));
        }
        if (entries == null) return Cols.listOf();
        return entries.stream().map(PosEntry::position).collect(Collectors.toList());
    }

    /** Check if a lexeme exists with any of the given weights. */
    public boolean containsLexemeWithWeight(String lexeme, Set<Character> weights) {
        List<PosEntry> entries = lexemes.get(lexeme);
        if (entries == null) {
            entries = lexemes.get(simpleStem(lexeme.toLowerCase()));
        }
        if (entries == null) return false;
        if (weights == null || weights.isEmpty()) return true;
        return entries.stream().anyMatch(e -> weights.contains(e.weight()));
    }

    /** Number of distinct lexemes. */
    public int length() {
        return lexemes.size();
    }

    /** Remove positions and weights, keeping only lexemes. */
    public TsVector strip() {
        Map<String, List<PosEntry>> stripped = new TreeMap<>();
        for (String key : lexemes.keySet()) {
            stripped.put(key, Cols.listOf());
        }
        return new TsVector(stripped);
    }

    /** Set weight on all positions of all lexemes. */
    public TsVector setWeight(char weight) {
        Map<String, List<PosEntry>> result = new TreeMap<>();
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            List<PosEntry> newEntries = new ArrayList<>();
            for (PosEntry pe : entry.getValue()) {
                newEntries.add(new PosEntry(pe.position(), weight));
            }
            if (newEntries.isEmpty()) {
                newEntries.add(new PosEntry(0, weight));
            }
            result.put(entry.getKey(), newEntries);
        }
        return new TsVector(result);
    }

    /** Set weight only for specified lexemes. */
    public TsVector setWeight(char weight, List<String> filterLexemes) {
        Map<String, List<PosEntry>> result = new TreeMap<>();
        Set<String> filterSet = new HashSet<>(filterLexemes);
        // Also add stemmed forms
        for (String l : filterLexemes) filterSet.add(simpleStem(l.toLowerCase()));
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            if (filterSet.contains(entry.getKey())) {
                List<PosEntry> newEntries = new ArrayList<>();
                for (PosEntry pe : entry.getValue()) {
                    newEntries.add(new PosEntry(pe.position(), weight));
                }
                result.put(entry.getKey(), newEntries);
            } else {
                result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
            }
        }
        return new TsVector(result);
    }

    /** Delete specified lexemes from the vector. */
    public TsVector delete(List<String> toDelete) {
        Map<String, List<PosEntry>> result = new TreeMap<>(lexemes);
        for (String l : toDelete) {
            result.remove(l);
            result.remove(simpleStem(l.toLowerCase()));
        }
        return new TsVector(result);
    }

    /** Filter: keep only lexemes that have any of the given weights. */
    public TsVector filter(Set<Character> weights) {
        if (weights == null || weights.isEmpty()) return empty();
        Map<String, List<PosEntry>> result = new TreeMap<>();
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            List<PosEntry> filtered = new ArrayList<>();
            for (PosEntry pe : entry.getValue()) {
                if (weights.contains(pe.weight())) {
                    filtered.add(pe);
                }
            }
            if (!filtered.isEmpty()) {
                result.put(entry.getKey(), filtered);
            }
        }
        return new TsVector(result);
    }

    /** Concatenate two tsvectors. Positions in other are shifted. */
    public TsVector concat(TsVector other) {
        Map<String, List<PosEntry>> result = new TreeMap<>();
        // Copy this vector's entries
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            result.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        // Find max position in this vector
        int maxPos = 0;
        for (List<PosEntry> entries : lexemes.values()) {
            for (PosEntry pe : entries) {
                maxPos = Math.max(maxPos, pe.position());
            }
        }
        // Merge other vector's entries with shifted positions
        for (Map.Entry<String, List<PosEntry>> entry : other.lexemes.entrySet()) {
            List<PosEntry> existing = result.computeIfAbsent(entry.getKey(), k -> new ArrayList<>());
            for (PosEntry pe : entry.getValue()) {
                existing.add(new PosEntry(pe.position() + maxPos, pe.weight()));
            }
        }
        return new TsVector(result);
    }

    /** Convert to an array of lexeme strings. */
    public List<String> toArray() {
        return new ArrayList<>(lexemes.keySet());
    }

    /** Build a tsvector from an array of strings (PG: no positions assigned). */
    public static TsVector fromArray(List<String> words) {
        Map<String, List<PosEntry>> lexemes = new TreeMap<>();
        for (String word : words) {
            if (word != null && !word.isEmpty()) {
                // PG array_to_tsvector: no positions, preserve case
                lexemes.put(word, new ArrayList<>());
            }
        }
        return new TsVector(lexemes);
    }

    public double rank(TsQuery query) {
        return rank(query, null, 0);
    }

    public double rank(TsQuery query, float[] weights, int normalization) {
        if (lexemes.isEmpty()) return 0.0;
        double[] w = weights != null && weights.length == 4
            ? new double[]{weights[0], weights[1], weights[2], weights[3]}
            : new double[]{0.1, 0.2, 0.4, 1.0}; // D, C, B, A

        // Collect matching terms with their positions
        List<String> matchedTerms = new ArrayList<>();
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            if (query.containsTerm(entry.getKey())) {
                matchedTerms.add(entry.getKey());
            }
        }
        if (matchedTerms.isEmpty()) return 0.0;

        double res;
        if (isAndQuery(query) && matchedTerms.size() >= 2) {
            res = calcRankAnd(w, matchedTerms);
        } else {
            res = calcRankOr(w, matchedTerms, countQueryTerms(query));
        }

        // Apply normalization
        if (normalization != 0) {
            int docLen = 0;
            for (List<PosEntry> entries : lexemes.values()) docLen += entries.size();
            if (docLen == 0) docLen = 1;
            if ((normalization & 1) != 0) {
                // divide by 1 + log(doc length)
                res /= 1.0 + Math.log(docLen);
            }
            if ((normalization & 2) != 0) {
                // divide by doc length
                res /= docLen;
            }
            if ((normalization & 4) != 0) {
                // divide by mean harmonic distance between extents
                res /= (1.0 + matchedTerms.size());
            }
            if ((normalization & 8) != 0) {
                // divide by number of unique words in doc
                res /= lexemes.size();
            }
            if ((normalization & 16) != 0) {
                // divide by 1 + log(num unique words)
                res /= 1.0 + Math.log(lexemes.size());
            }
            if ((normalization & 32) != 0) {
                // rank / (1 + rank)
                res = res / (1.0 + res);
            }
        }

        return (float) res;
    }

    /** PG's calc_rank_and: proximity-based ranking for AND queries. */
    private double calcRankAnd(double[] w, List<String> matchedTerms) {
        double res = 0.0;
        for (int i = 0; i < matchedTerms.size(); i++) {
            List<PosEntry> posI = lexemes.get(matchedTerms.get(i));
            for (int j = i + 1; j < matchedTerms.size(); j++) {
                List<PosEntry> posJ = lexemes.get(matchedTerms.get(j));
                for (PosEntry pi : posI) {
                    for (PosEntry pj : posJ) {
                        int dist = Math.abs(pi.position() - pj.position());
                        double wd = wordDistance(dist);
                        double wI = w[weightIndex(pi.weight())];
                        double wJ = w[weightIndex(pj.weight())];
                        double curw = Math.sqrt(wI * wJ * wd);
                        res = 1.0 - (1.0 - res) * (1.0 - curw);
                    }
                }
            }
        }
        return res;
    }

    /** PG's word_distance: exponential decay for term proximity. */
    private static double wordDistance(int dist) {
        if (dist > 100) return 1e-30;
        return 1.0 / (1.005 + 0.05 * Math.exp(((double) dist) / 1.5 - 2));
    }

    /** PG's calc_rank_or: sum-based ranking for OR queries and single terms. */
    private double calcRankOr(double[] w, List<String> matchedTerms, int queryTermCount) {
        final double PI_SQ_OVER_6 = 1.64493406685;
        double res = 0.0;
        for (String term : matchedTerms) {
            List<PosEntry> positions = lexemes.get(term);
            double resj = 0.0;
            double wjm = -1.0;
            int jm = 0;
            for (int j = 0; j < positions.size(); j++) {
                double wt = w[weightIndex(positions.get(j).weight())];
                resj += wt / ((double)(j + 1) * (j + 1));
                if (wt > wjm) { wjm = wt; jm = j; }
            }
            res += (wjm + resj - wjm / ((double)(jm + 1) * (jm + 1))) / PI_SQ_OVER_6;
        }
        if (queryTermCount > 1) res /= queryTermCount;
        return res;
    }

    private static int weightIndex(char weight) {
        switch (weight) {
            case 'A': return 3;
            case 'B': return 2;
            case 'C': return 1;
            default: return 0;
        }
    }

    private static boolean isAndQuery(TsQuery query) {
        if (query == null) return false;
        TsQuery.Op op = query.getOp();
        return op == TsQuery.Op.AND || op == TsQuery.Op.PHRASE;
    }

    private static int countQueryTerms(TsQuery query) {
        Set<String> terms = new HashSet<>();
        collectQueryTerms(query, terms);
        return Math.max(terms.size(), 1);
    }

    private static void collectQueryTerms(TsQuery query, Set<String> terms) {
        if (query == null) return;
        if (query.getOp() == TsQuery.Op.TERM) {
            String t = query.getTerm();
            if (t != null && !t.isEmpty()) terms.add(t);
        } else if (query.getOp() == TsQuery.Op.NOT) {
            // Do not count terms inside NOT branches
        } else {
            collectQueryTerms(query.getLeft(), terms);
            collectQueryTerms(query.getRight(), terms);
        }
    }

    /** Cover density ranking. */
    public double rankCd(TsQuery query) {
        return rankCd(query, null, 0);
    }

    public double rankCd(TsQuery query, float[] weights, int normalization) {
        if (lexemes.isEmpty()) return 0.0;
        double[] w = weights != null && weights.length == 4
            ? new double[]{weights[0], weights[1], weights[2], weights[3]}
            : new double[]{0.1, 0.2, 0.4, 1.0}; // D, C, B, A

        // Collect all matching positions with their weights
        List<int[]> matchPosWeights = new ArrayList<>(); // [position, weightIndex]
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            if (query.containsTerm(entry.getKey())) {
                for (PosEntry pe : entry.getValue()) {
                    matchPosWeights.add(new int[]{pe.position(), weightIndex(pe.weight())});
                }
            }
        }
        if (matchPosWeights.isEmpty()) return 0.0;
        matchPosWeights.sort(Comparator.comparingInt(a -> a[0]));

        // PG's cover density algorithm:
        // For each pair of distinct query terms found, compute 1/(distance between their positions)
        // weighted by their weights
        int nMatched = matchPosWeights.size();
        if (nMatched == 1) {
            double res = w[matchPosWeights.get(0)[1]];
            return applyNormCd((float) res, normalization);
        }

        double score = 0.0;
        for (int i = 0; i < nMatched - 1; i++) {
            int posI = matchPosWeights.get(i)[0];
            int posJ = matchPosWeights.get(i + 1)[0];
            int dist = posJ - posI;
            if (dist <= 0) dist = 1;
            double wI = w[matchPosWeights.get(i)[1]];
            double wJ = w[matchPosWeights.get(i + 1)[1]];
            score += wI * wJ / (double) (dist * dist);
        }

        double res = score / (double) countQueryTerms(query);
        return applyNormCd((float) res, normalization);
    }

    private float applyNormCd(float res, int normalization) {
        if (normalization != 0) {
            int docLen = 0;
            for (List<PosEntry> entries : lexemes.values()) docLen += entries.size();
            if (docLen == 0) docLen = 1;
            if ((normalization & 2) != 0) res /= docLen;
            if ((normalization & 8) != 0) res /= lexemes.size();
            if ((normalization & 32) != 0) res = res / (1.0f + res);
        }
        return res;
    }

    static String simpleStem(String word) {
        if (word.length() > 4 && word.endsWith("ful")) return word.substring(0, word.length() - 3);
        if (word.length() > 4 && word.endsWith("ing")) return word.substring(0, word.length() - 3);
        if (word.length() > 3 && word.endsWith("ed")) return word.substring(0, word.length() - 2);
        if (word.length() > 3 && word.endsWith("es")) return word.substring(0, word.length() - 2);
        if (word.length() > 2 && word.endsWith("s") && !word.endsWith("ss")) return word.substring(0, word.length() - 1);
        return word;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            if (sb.length() > 0) sb.append(" ");
            sb.append("'").append(entry.getKey()).append("'");
            List<PosEntry> positions = entry.getValue();
            if (!positions.isEmpty()) {
                // Build position string, only emit if there are actual positions > 0
                StringBuilder posSb = new StringBuilder();
                for (int i = 0; i < positions.size(); i++) {
                    PosEntry pe = positions.get(i);
                    if (pe.position() == 0 && pe.weight() == 'D') continue;
                    if (posSb.length() > 0) posSb.append(",");
                    if (pe.position() > 0) {
                        posSb.append(pe.position());
                    }
                    if (pe.weight() != 'D') {
                        posSb.append(pe.weight());
                    }
                }
                if (posSb.length() > 0) {
                    sb.append(":").append(posSb);
                }
            }
        }
        return sb.toString();
    }
}
