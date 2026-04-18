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

    private static final Set<String> STOP_WORDS = Cols.setOf(
            "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "do", "does", "did", "will", "would", "could",
            "should", "may", "might", "can", "shall", "to", "of", "in", "for",
            "on", "with", "at", "by", "from", "as", "into", "about", "between",
            "through", "during", "before", "after", "above", "below",
            "and", "but", "or", "nor", "not", "so", "yet",
            "it", "its", "this", "that", "these", "those"
    );

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

    /** Build a tsvector from plain text (tokenize, stem, remove stop words). */
    public static TsVector fromText(String text) {
        Map<String, List<PosEntry>> lexemes = new TreeMap<>();
        if (text == null || text.isEmpty()) return new TsVector(lexemes);

        // Check if it looks like a tsvector literal: 'word':1A,2B 'word2':3
        if (text.contains("'") && text.contains(":")) {
            TsVector parsed = parseLiteral(text);
            if (parsed != null) return parsed;
        }

        String[] words = text.toLowerCase().split("[^a-zA-Z0-9]+");
        int position = 0;
        for (String word : words) {
            if (word.isEmpty()) continue;
            position++;  // count position for every word including stop words
            if (STOP_WORDS.contains(word)) continue;
            String stem = simpleStem(word);
            lexemes.computeIfAbsent(stem, k -> new ArrayList<>()).add(new PosEntry(position));
        }
        return new TsVector(lexemes);
    }

    /** Parse a tsvector literal like: 'cat':1A,2B 'dog':3 'fat':4C */
    private static final Pattern LEXEME_PAT = Pattern.compile("'([^']*)'(?::([\\d,A-Da-d]+))?");

    public static TsVector parseLiteral(String input) {
        Map<String, List<PosEntry>> lexemes = new TreeMap<>();
        Matcher m = LEXEME_PAT.matcher(input);
        while (m.find()) {
            String lexeme = m.group(1).toLowerCase();
            String posStr = m.group(2);
            List<PosEntry> entries = new ArrayList<>();
            if (posStr != null) {
                for (String part : posStr.split(",")) {
                    part = part.trim();
                    if (part.isEmpty()) continue;
                    char weight = 'D';
                    String numStr = part;
                    char last = part.charAt(part.length() - 1);
                    if (Character.isLetter(last)) {
                        weight = Character.toUpperCase(last);
                        numStr = part.substring(0, part.length() - 1);
                    }
                    if (!numStr.isEmpty()) {
                        try {
                            entries.add(new PosEntry(Integer.parseInt(numStr), weight));
                        } catch (NumberFormatException e) {
                            // skip
                        }
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

    public boolean containsLexeme(String lexeme) {
        return lexemes.containsKey(simpleStem(lexeme.toLowerCase()));
    }

    public Set<String> getLexemes() {
        return lexemes.keySet();
    }

    public Map<String, List<PosEntry>> getLexemeMap() {
        return lexemes;
    }

    /** Get all positions for a lexeme. */
    public List<Integer> getPositions(String lexeme) {
        List<PosEntry> entries = lexemes.get(simpleStem(lexeme.toLowerCase()));
        if (entries == null) return Cols.listOf();
        return entries.stream().map(PosEntry::position).collect(Collectors.toList());
    }

    /** Check if a lexeme exists with any of the given weights. */
    public boolean containsLexemeWithWeight(String lexeme, Set<Character> weights) {
        List<PosEntry> entries = lexemes.get(simpleStem(lexeme.toLowerCase()));
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
        Set<String> filterSet = new HashSet<>();
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
        for (String l : toDelete) result.remove(simpleStem(l.toLowerCase()));
        return new TsVector(result);
    }

    /** Filter: keep only lexemes that have any of the given weights. */
    public TsVector filter(Set<Character> weights) {
        Map<String, List<PosEntry>> result = new TreeMap<>();
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            List<PosEntry> filtered = entry.getValue().stream()
                    .filter(pe -> weights.contains(pe.weight()))
                    .collect(Collectors.toList());
            if (!filtered.isEmpty()) {
                result.put(entry.getKey(), new ArrayList<>(filtered));
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

    /** Build a tsvector from an array of strings. */
    public static TsVector fromArray(List<String> words) {
        Map<String, List<PosEntry>> lexemes = new TreeMap<>();
        int pos = 1;
        for (String word : words) {
            if (word != null && !word.isEmpty()) {
                lexemes.put(word.toLowerCase(), Cols.listOf(new PosEntry(pos++)));
            }
        }
        return new TsVector(lexemes);
    }

    public double rank(TsQuery query) {
        if (lexemes.isEmpty()) return 0.0;
        // PG-compatible ts_rank algorithm (calc_rank_or from tsrank.c).
        // Default weights: {0.1, 0.2, 0.4, 1.0} for categories D, C, B, A.
        double[] defaultWeights = {0.1, 0.2, 0.4, 1.0}; // D, C, B, A
        // pi^2/6, the theoretical maximum of sum(1/i^2, i=1..inf), used as normalizer
        final double PI_SQ_OVER_6 = 1.64493406685;

        double res = 0.0;
        int matchCount = 0;
        // Count total query terms for normalization (PG divides by query size)
        int queryTermCount = countQueryTerms(query);
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            if (query.containsTerm(entry.getKey())) {
                matchCount++;
                List<PosEntry> positions = entry.getValue();
                int dimt = positions.size();
                // Compute resj = sum of wpos(post[j]) / (j+1)^2
                double resj = 0.0;
                double wjm = -1.0;
                int jm = 0;
                for (int j = 0; j < dimt; j++) {
                    PosEntry pe = positions.get(j);
                    int widx;
                    switch (pe.weight()) {
                        case 'A':
                            widx = 3;
                            break;
                        case 'B':
                            widx = 2;
                            break;
                        case 'C':
                            widx = 1;
                            break;
                        default:
                            widx = 0;
                            break;
                    }
                    double wt = defaultWeights[widx];
                    resj += wt / ((double)(j + 1) * (j + 1));
                    if (wt > wjm) {
                        wjm = wt;
                        jm = j;
                    }
                }
                // Per-word score: (wjm + resj - wjm/(jm+1)^2) / (pi^2/6)
                double wordScore = (wjm + resj - wjm / ((double)(jm + 1) * (jm + 1))) / PI_SQ_OVER_6;
                res += wordScore;
            }
        }
        if (matchCount == 0) return 0.0;
        // PG's calc_rank_or divides by the number of unique query terms
        if (queryTermCount > 1) {
            res /= queryTermCount;
        }
        return (float) res; // PG returns float4 precision
    }

    /** Count the number of unique terms in a TsQuery (excluding NOT branches). */
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

    /** Cover density ranking that considers proximity of matched terms. */
    public double rankCd(TsQuery query) {
        if (lexemes.isEmpty()) return 0.0;
        // Collect positions of all matching lexemes
        List<Integer> matchPositions = new ArrayList<>();
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            if (query.containsTerm(entry.getKey())) {
                for (PosEntry pe : entry.getValue()) {
                    matchPositions.add(pe.position());
                }
            }
        }
        if (matchPositions.isEmpty()) return 0.0;
        Collections.sort(matchPositions);
        // Density-based: closer matches score higher
        if (matchPositions.size() == 1) return 1.0 / (1.0 + matchPositions.get(0));
        double score = 0.0;
        for (int i = 1; i < matchPositions.size(); i++) {
            int dist = matchPositions.get(i) - matchPositions.get(i - 1);
            score += 1.0 / dist;
        }
        return score / matchPositions.size();
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
            if (!positions.isEmpty() && !(positions.size() == 1 && positions.get(0).position() == 0)) {
                sb.append(":");
                for (int i = 0; i < positions.size(); i++) {
                    if (i > 0) sb.append(",");
                    PosEntry pe = positions.get(i);
                    if (pe.position() > 0) {
                        sb.append(pe.position());
                    }
                    if (pe.weight() != 'D') {
                        sb.append(pe.weight());
                    }
                }
            }
        }
        return sb.toString();
    }
}
