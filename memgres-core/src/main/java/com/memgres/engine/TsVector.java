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

    static final Set<String> STOP_WORDS_SET = com.memgres.engine.fts.StopWords.ENGLISH;

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

    /**
     * Build a tsvector from plain text with the specified config.
     *
     * <p>Follows PG's pipeline: the default parser assigns each token a type, the
     * configuration routes that type to a dictionary, and every token that reaches a
     * dictionary consumes a position — including stop words, which take a position but
     * contribute no lexeme.
     */
    public static TsVector fromText(String text, String config) {
        Map<String, List<PosEntry>> lexemes = new TreeMap<>();
        if (text == null || text.isEmpty()) return new TsVector(lexemes);

        boolean isSimple = "simple".equalsIgnoreCase(config);
        int position = 0;
        for (com.memgres.engine.fts.TsParser.Token token
                : com.memgres.engine.fts.TsParser.parse(text)) {
            com.memgres.engine.fts.TsParser.Dict dict =
                    com.memgres.engine.fts.TsParser.dictionaryFor(token.type());
            if (dict == com.memgres.engine.fts.TsParser.Dict.NONE) continue;
            position++;
            String lower = token.text().toLowerCase();
            if (isSimple || dict == com.memgres.engine.fts.TsParser.Dict.SIMPLE) {
                addPosition(lexemes, lower, position);
                continue;
            }
            // The snowball dictionary drops stop words before stemming.
            if (STOP_WORDS.contains(lower)) continue;
            addPosition(lexemes, com.memgres.engine.fts.EnglishStemmer.stem(lower), position);
        }
        return new TsVector(lexemes);
    }

    private static void addPosition(Map<String, List<PosEntry>> lexemes, String lexeme, int position) {
        if (position > MAX_POSITION) position = MAX_POSITION;
        List<PosEntry> entries = lexemes.computeIfAbsent(lexeme, k -> new ArrayList<>());
        // PG caps at 255 positions per lexeme (MAXNUMPOS)
        if (entries.size() < 255) {
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
                    // A position entry ends at a comma or at the end of the token; anything
                    // else (a stray letter, say) means the literal is malformed, and PG says so
                    if (i < len && input.charAt(i) == ',') {
                        i++;
                    } else {
                        if (i < len && !Character.isWhitespace(input.charAt(i))) {
                            throw new MemgresException(
                                    "syntax error in tsvector: \"" + input + "\"", "42601");
                        }
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

    /**
     * Whether this vector holds the lexeme. A tsvector holds lexemes and a tsquery holds lexemes,
     * so the comparison is exact: whatever normalization either side needed was done when it was
     * built. Stemming here instead made {@code 'cats'::tsquery} match a vector holding
     * {@code 'cat'}, which PostgreSQL does not.
     */
    public boolean containsLexeme(String lexeme) {
        return lexemes.containsKey(lexeme);
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

    /** Get all positions for a lexeme, matched exactly as {@link #containsLexeme} does. */
    public List<Integer> getPositions(String lexeme) {
        List<PosEntry> entries = lexemes.get(lexeme);
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
        query = prune(query);
        if (query == null) return 0.0;
        double[] w = weights != null && weights.length == 4
            ? new double[]{weights[0], weights[1], weights[2], weights[3]}
            : new double[]{0.1f, 0.2f, 0.4f, 1.0f}; // D, C, B, A (float4, as PG stores them)

        // Collect matching terms with their positions
        List<String> matchedTerms = new ArrayList<>();
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            if (query.containsTerm(entry.getKey())) {
                matchedTerms.add(entry.getKey());
            }
        }
        // No early return on an empty match set: PG's calc_rank_and yields -1, which the
        // clamp below turns into 1e-20 — an AND query that matches nothing is not 0.
        float res;
        // PG's calc_rank_and falls back to calc_rank_or when the query has fewer than two
        // distinct operands, so "a & a" ranks like a single term rather than as no pair.
        if (isAndQuery(query) && countQueryTerms(query) >= 2) {
            res = calcRankAnd(w, matchedTerms);
        } else {
            res = calcRankOr(w, matchedTerms, countQueryTerms(query));
        }

        // PG's calc_rank normalisation. Note the base-2 logarithms, and that the
        // cover-distance bit (4) is documented as "not applicable" to ts_rank and is a
        // no-op there — only ts_rank_cd has covers to measure.
        if (res < 0) res = 1e-20f;
        int docLen = 0;
        for (List<PosEntry> entries : lexemes.values()) {
            docLen += entries.isEmpty() ? 1 : entries.size();
        }
        int uniq = lexemes.size();
        if ((normalization & 1) != 0 && uniq > 0) {
            res = (float) (res / (Math.log((double) (docLen + 1)) / Math.log(2.0)));
        }
        if ((normalization & 2) != 0 && docLen > 0) res = res / (float) docLen;
        if ((normalization & 8) != 0 && uniq > 0) res = res / (float) uniq;
        if ((normalization & 16) != 0 && uniq > 0) {
            res = (float) (res / (Math.log((double) (uniq + 1)) / Math.log(2.0)));
        }
        if ((normalization & 32) != 0) res = res / (res + 1);

        return res;
    }

    /** PG's calc_rank_and: proximity-based ranking for AND queries. */
    private float calcRankAnd(double[] w, List<String> matchedTerms) {
        float res = -1.0f;
        for (int i = 0; i < matchedTerms.size(); i++) {
            List<PosEntry> posI = lexemes.get(matchedTerms.get(i));
            for (int j = i + 1; j < matchedTerms.size(); j++) {
                List<PosEntry> posJ = lexemes.get(matchedTerms.get(j));
                for (PosEntry pi : posI) {
                    for (PosEntry pj : posJ) {
                        int dist = Math.abs(pi.position() - pj.position());
                        // Two different operands at the same position contribute nothing.
                        if (dist == 0) continue;
                        float wd = wordDistance(dist);
                        float wI = (float) w[weightIndex(pi.weight())];
                        float wJ = (float) w[weightIndex(pj.weight())];
                        float curw = (float) Math.sqrt(wI * wJ * wd);
                        res = res < 0 ? curw : (float) (1.0 - (1.0 - res) * (1.0 - curw));
                    }
                }
            }
        }
        return res;
    }

    /** PG's word_distance: exponential decay for term proximity, computed in float. */
    private static float wordDistance(int dist) {
        if (dist > 100) return 1e-30f;
        return (float) (1.0 / (1.005 + 0.05 * Math.exp(((double) (float) dist) / 1.5 - 2)));
    }

    /** PG's calc_rank_or: sum-based ranking for OR queries and single terms. */
    private float calcRankOr(double[] w, List<String> matchedTerms, int queryTermCount) {
        final double PI_SQ_OVER_6 = 1.64493406685;
        float res = 0.0f;
        for (String term : matchedTerms) {
            List<PosEntry> positions = lexemes.get(term);
            float resj = 0.0f;
            float wjm = -1.0f;
            int jm = 0;
            for (int j = 0; j < positions.size(); j++) {
                float wt = (float) w[weightIndex(positions.get(j).weight())];
                resj = resj + wt / ((j + 1) * (j + 1));
                if (wt > wjm) { wjm = wt; jm = j; }
            }
            res = (float) (res + (wjm + resj - wjm / ((jm + 1) * (jm + 1))) / PI_SQ_OVER_6);
        }
        if (queryTermCount > 0) res = res / queryTermCount;
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

    /**
     * PG's {@code calc_rank_cd} from tsrank.c: cover density.
     *
     * <p>A "cover" is a minimal span of the document that satisfies the query. PG walks
     * the document finding successive non-overlapping covers, scores each by its weighted
     * density and the number of non-matching words it contains, and then applies the
     * normalisation bits — including bit 4, which divides by the mean harmonic distance
     * between covers and therefore only exists once covers do.
     */
    public double rankCd(TsQuery query, float[] weights, int normalization) {
        if (lexemes.isEmpty() || query == null) return 0.0;
        query = prune(query);
        if (query == null) return 0.0;
        double[] w = weights != null && weights.length == 4
                ? new double[]{weights[0], weights[1], weights[2], weights[3]}
                : new double[]{0.1f, 0.2f, 0.4f, 1.0f}; // D, C, B, A (float4, as PG stores them)
        double[] invws = new double[4];
        for (int i = 0; i < 4; i++) invws[i] = 1.0 / w[i];

        List<String> terms = new ArrayList<>(new LinkedHashSet<>(query.collectTerms()));
        if (terms.isEmpty()) return 0.0;

        // Document representation: one entry per position of every lexeme the query
        // mentions, ordered by position.
        List<int[]> doc = new ArrayList<>(); // [position, weightIndex, termIndex]
        for (int t = 0; t < terms.size(); t++) {
            List<PosEntry> entries = lexemes.get(terms.get(t));
            if (entries == null) continue;
            for (PosEntry pe : entries) {
                doc.add(new int[]{pe.position(), weightIndex(pe.weight()), t});
            }
        }
        if (doc.isEmpty()) return 0.0;
        doc.sort((a, b) -> a[0] != b[0] ? Integer.compare(a[0], b[0]) : Integer.compare(a[2], b[2]));

        double wdoc = 0.0;
        double sumDist = 0.0;
        double prevExtPos = 0.0;
        int nExtent = 0;
        int pos = 0;
        int len = doc.size();

        while (pos < len) {
            int[] cover = nextCover(doc, len, query, terms, pos);
            if (cover == null) break;
            int begin = cover[0], end = cover[1], p = cover[2], q = cover[3];
            pos = cover[4];

            double invSum = 0.0;
            for (int i = begin; i <= end; i++) invSum += invws[doc.get(i)[1]];
            double cpos = ((double) (end - begin + 1)) / invSum;

            int nNoise = (q - p) - (end - begin);
            if (nNoise < 0) nNoise = (end - begin) / 2;
            wdoc += cpos / ((double) (1 + nNoise));

            double curExtPos = ((double) (q + p)) / 2.0;
            if (nExtent > 0 && curExtPos > prevExtPos) sumDist += 1.0 / (curExtPos - prevExtPos);
            prevExtPos = curExtPos;
            nExtent++;
        }

        int docLen = 0;
        for (List<PosEntry> entries : lexemes.values()) {
            docLen += entries.isEmpty() ? 1 : entries.size();
        }
        int uniq = lexemes.size();

        if ((normalization & 1) != 0 && uniq > 0) wdoc /= Math.log((double) (docLen + 1));
        if ((normalization & 2) != 0 && docLen > 0) wdoc /= (double) docLen;
        if ((normalization & 4) != 0 && nExtent > 0 && sumDist > 0) {
            wdoc /= ((double) nExtent) / sumDist;
        }
        if ((normalization & 8) != 0 && uniq > 0) wdoc /= (double) uniq;
        if ((normalization & 16) != 0 && uniq > 0) wdoc /= Math.log((double) (uniq + 1)) / Math.log(2.0);
        if ((normalization & 32) != 0) wdoc /= (wdoc + 1);

        return (float) wdoc;
    }

    /**
     * PG's {@code Cover}: the next minimal extent [p,q] satisfying the query, starting at
     * {@code from}. Returns {beginIndex, endIndex, p, q, nextFrom} or null when exhausted.
     */
    private static int[] nextCover(List<int[]> doc, int len, TsQuery query,
                                   List<String> terms, int from) {
        while (from < len) {
            boolean[] seen = new boolean[terms.size()];
            int end = -1, q = 0;
            for (int ptr = from; ptr < len; ptr++) {
                seen[doc.get(ptr)[2]] = true;
                if (satisfied(query, terms, seen)) {
                    q = doc.get(ptr)[0];
                    end = ptr;
                    break;
                }
            }
            if (end < 0) return null;

            // Now shrink from the right-hand end back down to the smallest span.
            java.util.Arrays.fill(seen, false);
            int begin = -1, p = Integer.MAX_VALUE;
            for (int ptr = end; ptr >= from; ptr--) {
                seen[doc.get(ptr)[2]] = true;
                if (satisfied(query, terms, seen)) {
                    begin = ptr;
                    p = doc.get(ptr)[0];
                    break;
                }
            }
            if (begin >= 0 && p <= q) {
                return new int[]{begin, end, p, q, begin + 1};
            }
            from++;
        }
        return null;
    }

    /**
     * Drops stop-word operands, which PG removes from the query outright rather than
     * leaving as empty terms — an AND with one of them is just the remaining branch.
     */
    private static TsQuery prune(TsQuery q) {
        if (q == null) return null;
        if (q.getOp() == TsQuery.Op.TERM) {
            String t = q.getTerm();
            return t == null || t.isEmpty() ? null : q;
        }
        if (q.getOp() == TsQuery.Op.NOT) {
            TsQuery inner = prune(q.getLeft() != null ? q.getLeft() : q.getRight());
            return inner == null ? null : q;
        }
        TsQuery l = prune(q.getLeft());
        TsQuery r = prune(q.getRight());
        if (l == null) return r;
        if (r == null) return l;
        if (l == q.getLeft() && r == q.getRight()) return q;
        return q.getOp() == TsQuery.Op.OR ? TsQuery.or(l, r) : TsQuery.and(l, r);
    }

    /** Evaluates the query with each operand true iff it has been seen in the span. */
    private static boolean satisfied(TsQuery query, List<String> terms, boolean[] seen) {
        if (query == null) return false;
        switch (query.getOp()) {
            case TERM: {
                int idx = terms.indexOf(query.getTerm());
                return idx >= 0 && seen[idx];
            }
            case NOT:
                return !satisfied(query.getLeft() != null ? query.getLeft() : query.getRight(),
                        terms, seen);
            case OR:
                return satisfied(query.getLeft(), terms, seen)
                        || satisfied(query.getRight(), terms, seen);
            default: // AND and PHRASE both require every operand within the span
                return satisfied(query.getLeft(), terms, seen)
                        && satisfied(query.getRight(), terms, seen);
        }
    }

    /** The english_stem dictionary: PG's bundled Snowball (Porter2) stemmer. */
    static String simpleStem(String word) {
        return com.memgres.engine.fts.EnglishStemmer.stem(word);
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
