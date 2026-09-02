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

    /** The most positions one lexeme carries; further ones are dropped. */
    private static final int MAX_POSITIONS_PER_LEXEME = 256;

    /**
     * The order lexemes are held in, which is the order they are written in and the order two
     * vectors are compared in.
     *
     * <p>A lexeme is a string of bytes to PostgreSQL and it orders them as bytes. Java orders
     * strings by UTF-16 code unit, and the two disagree wherever a character sits outside the
     * basic plane: a surrogate pair begins {@code 0xD800}, which is above every three-byte
     * character in UTF-16 and below every four-byte one in UTF-8. Ordering by the bytes puts
     * {@code 'Ａ'} before {@code '😀'} the way PostgreSQL does rather than after it.
     */
    static final Comparator<String> LEXEME_ORDER = new Comparator<String>() {
        @Override
        public int compare(String a, String b) {
            byte[] x = a.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] y = b.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            int n = Math.min(x.length, y.length);
            for (int i = 0; i < n; i++) {
                int diff = (x[i] & 0xFF) - (y[i] & 0xFF);
                if (diff != 0) return diff;
            }
            return x.length - y.length;
        }
    };

    /** A map of lexemes in the order PostgreSQL holds them. */
    static TreeMap<String, List<PosEntry>> newLexemeMap() {
        return new TreeMap<String, List<PosEntry>>(LEXEME_ORDER);
    }

    /**
     * The positions of one lexeme, as PostgreSQL keeps them: in order, each position once, none
     * past the last representable one, and no more of them than a lexeme can carry.
     *
     * <p>Where the same position is given twice the stronger weight is the one kept, so
     * {@code 'a:1,1A'} and {@code 'a:1A,1'} are the same value — which they have to be, because
     * neither says anything the other does not.
     */
    static List<PosEntry> normalisePositions(List<PosEntry> entries) {
        if (entries == null || entries.isEmpty()) return new ArrayList<PosEntry>();
        TreeMap<Integer, Character> byPosition = new TreeMap<Integer, Character>();
        for (PosEntry pe : entries) {
            int position = Math.min(pe.position(), MAX_POSITION);
            if (position <= 0) continue;
            Character held = byPosition.get(position);
            if (held == null || pe.weight() < held.charValue()) {
                byPosition.put(position, Character.valueOf(pe.weight()));
            }
        }
        List<PosEntry> out = new ArrayList<PosEntry>();
        for (Map.Entry<Integer, Character> e : byPosition.entrySet()) {
            if (out.size() >= MAX_POSITIONS_PER_LEXEME) break;
            out.add(new PosEntry(e.getKey().intValue(), e.getValue().charValue()));
        }
        return out;
    }

    /** Every lexeme's positions normalised, in place. */
    private static Map<String, List<PosEntry>> normaliseAll(Map<String, List<PosEntry>> lexemes) {
        for (Map.Entry<String, List<PosEntry>> e : lexemes.entrySet()) {
            e.setValue(normalisePositions(e.getValue()));
        }
        return lexemes;
    }

    static final Set<String> STOP_WORDS_SET = com.memgres.engine.fts.StopWords.ENGLISH;

    static boolean isStopWord(String word) {
        return STOP_WORDS_SET.contains(lowerstr(word));
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
        this.lexemes = newLexemeMap();
        this.lexemes.putAll(lexemes);
    }

    /** Create an empty tsvector. */
    public static TsVector empty() {
        return new TsVector(newLexemeMap());
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
        Map<String, List<PosEntry>> lexemes = newLexemeMap();
        addText(lexemes, text, config, new int[2]);
        return new TsVector(lexemes);
    }

    /**
     * A tsvector over the several pieces of text a document holds.
     *
     * <p>The parser runs on through them: it does not begin again at one for each piece, so a
     * piece of nothing but stop words still moves everything after it along. A piece ends where
     * its last lexeme was, or where it began when it had none, and the next piece starts one
     * past that -- which leaves a gap of at least one between the last word of one piece and the
     * first of the next, so that no phrase spans the two.
     */
    public static TsVector fromTexts(List<String> texts, String config) {
        Map<String, List<PosEntry>> lexemes = newLexemeMap();
        int[] cursor = new int[2];
        for (String text : texts) {
            int began = cursor[REACHED];
            addText(lexemes, text, config, cursor);
            cursor[REACHED] = Math.max(cursor[LAST_LEXEME], began) + 1;
        }
        return new TsVector(lexemes);
    }

    /** Where the parser has reached, which the next token follows. */
    private static final int REACHED = 0;
    /** Where the last lexeme was put, or zero while there has been none. */
    private static final int LAST_LEXEME = 1;

    /** Adds one piece of text, moving the cursor on over the positions the piece takes. */
    private static void addText(Map<String, List<PosEntry>> lexemes, String text, String config,
                                int[] cursor) {
        if (text == null || text.isEmpty()) return;
        boolean isSimple = "simple".equalsIgnoreCase(config);
        for (com.memgres.engine.fts.TsParser.Token token
                : com.memgres.engine.fts.TsParser.parse(text)) {
            com.memgres.engine.fts.TsParser.Dict dict =
                    com.memgres.engine.fts.TsParser.dictionaryFor(token.type());
            if (dict == com.memgres.engine.fts.TsParser.Dict.NONE) continue;
            int position = ++cursor[REACHED];
            String lower = lowerstr(token.text());
            if (isSimple || dict == com.memgres.engine.fts.TsParser.Dict.SIMPLE) {
                addPosition(lexemes, lower, position);
                cursor[LAST_LEXEME] = position;
                continue;
            }
            // The snowball dictionary drops stop words before stemming.
            if (STOP_WORDS.contains(lower)) continue;
            addPosition(lexemes, com.memgres.engine.fts.EnglishStemmer.stem(lower), position);
            cursor[LAST_LEXEME] = position;
        }
    }

    private static void addPosition(Map<String, List<PosEntry>> lexemes, String lexeme, int position) {
        if (position > MAX_POSITION) position = MAX_POSITION;
        List<PosEntry> entries = lexemes.computeIfAbsent(lexeme, k -> new ArrayList<>());
        // PG caps at 255 positions per lexeme (MAXNUMPOS)
        if (entries.size() < 255) {
            entries.add(new PosEntry(position));
        }
    }

    /**
     * Read a tsvector literal: {@code 'cat':1A,2B 'dog':3 fat:4C word}.
     *
     * <p>The same lexeme written twice names one lexeme holding both position lists, not the
     * second list alone; the reading is what makes the written form the value's own, so a value
     * this writes has to come back through here unchanged. Positions are put in order, each kept
     * once, and any beyond the last representable one brought back to it — all of which the
     * writer relies on, since it prints what it is given.
     */
    public static TsVector parseLiteral(String input) {
        if (input == null || input.isEmpty()) return null;
        Map<String, List<PosEntry>> lexemes = newLexemeMap();
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
                boolean closed = false;
                while (i < len) {
                    char c = input.charAt(i);
                    if (c == '\'' && i + 1 < len && input.charAt(i + 1) == '\'') {
                        sb.append('\'');
                        i += 2;
                    } else if (c == '\'') {
                        i++; // skip closing quote
                        closed = true;
                        break;
                    } else if (c == '\\' && i + 1 < len) {
                        sb.append(input.charAt(i + 1));
                        i += 2;
                    } else {
                        sb.append(c);
                        i++;
                    }
                }
                if (!closed || sb.length() == 0) throw syntaxErrorIn(input);
                lexeme = sb.toString(); // preserve case for quoted
            } else {
                // An unquoted lexeme runs to the next space or colon, and a backslash carries
                // whichever character follows it into the lexeme -- that is the only way a
                // space or a colon can be part of one without quotes.
                StringBuilder sb = new StringBuilder();
                while (i < len) {
                    char c = input.charAt(i);
                    if (c == '\\' && i + 1 < len) {
                        sb.append(input.charAt(i + 1));
                        i += 2;
                        continue;
                    }
                    if (Character.isWhitespace(c)) break;
                    // A colon separates a lexeme from its positions, so it can only do that
                    // once there is a lexeme for it to follow; leading, it is an ordinary
                    // character and ':1' names a lexeme rather than position 1 of nothing.
                    if (c == ':' && sb.length() > 0) break;
                    sb.append(c);
                    i++;
                }
                if (sb.length() == 0) { i++; continue; }
                lexeme = sb.toString(); // unquoted: keep as-is (PG lowercases but we match behavior)
            }

            // Parse optional positions: :1A,2B,3
            List<PosEntry> entries = new ArrayList<>();
            if (i < len && input.charAt(i) == ':') {
                i++; // skip ':'
                boolean any = false;
                while (i < len && !Character.isWhitespace(input.charAt(i))) {
                    // Parse one position entry
                    StringBuilder numSb = new StringBuilder();
                    while (i < len && Character.isDigit(input.charAt(i))) {
                        numSb.append(input.charAt(i));
                        i++;
                    }
                    char weight = 'D';
                    if (i < len && weightOf(input.charAt(i)) != 0) {
                        weight = weightOf(input.charAt(i));
                        i++;
                    }
                    if (numSb.length() == 0) throw syntaxErrorIn(input);
                    any = true;
                    // A position is written in decimal and read as a number, so a spelling
                    // too long for one is still the number it spells: bringing it back to
                    // the last representable position is what the documented rule asks for.
                    int pos = clampPosition(numSb.toString());
                    if (pos == 0) {
                        throw new MemgresException(
                                "wrong position info in tsvector: \"" + input + "\"", "42601");
                    }
                    entries.add(new PosEntry(pos, weight));
                    // A position entry ends at a comma or at the end of the token; anything
                    // else (a stray letter, say) means the literal is malformed, and PG says so
                    if (i < len && input.charAt(i) == ',') {
                        i++;
                    } else {
                        if (i < len && !Character.isWhitespace(input.charAt(i))) {
                            throw syntaxErrorIn(input);
                        }
                        break;
                    }
                }
                if (!any) throw syntaxErrorIn(input);
            }
            List<PosEntry> held = lexemes.get(lexeme);
            if (held == null) {
                lexemes.put(lexeme, entries);
            } else {
                held.addAll(entries);
            }
        }
        if (lexemes.isEmpty()) return null;
        return new TsVector(normaliseAll(lexemes));
    }

    private static MemgresException syntaxErrorIn(String input) {
        return new MemgresException("syntax error in tsvector: \"" + input + "\"", "42601");
    }

    /** The weight a character names, or {@code 0} if it names none. */
    private static char weightOf(char c) {
        char upper = Character.toUpperCase(c);
        if (upper >= 'A' && upper <= 'D') return upper;
        // A star stands for the strongest weight in this position, as PostgreSQL reads it.
        if (c == '*') return 'A';
        return 0;
    }

    /** The position a run of digits names, brought back to the last representable one. */
    /**
     * A written position, brought back to the last one a tsvector can hold.
     *
     * <p>The digits are read into a machine integer before anything is done with them, so a
     * spelling wider than one keeps only the bits that fit: 4294967297 is position 1, and
     * 2147483648 keeps nothing at all and is no position. What survives is then brought back to
     * the last position a tsvector has room for. Clamping the spelling instead made every wide
     * number the last position, whatever it said.
     */
    private static int clampPosition(String digits) {
        java.math.BigInteger written;
        try {
            written = new java.math.BigInteger(digits);
        } catch (NumberFormatException notANumber) {
            return 0;
        }
        // A number too wide even for the widest machine integer stops at that integer's largest
        // value rather than wrapping, so 2^63 is the same position 9223372036854775807 is.
        java.math.BigInteger widest = java.math.BigInteger.valueOf(Long.MAX_VALUE);
        if (written.compareTo(widest) > 0) written = widest;
        int kept = written.mod(java.math.BigInteger.ONE.shiftLeft(31)).intValue();
        if (kept == 0) return 0;
        return kept > MAX_POSITION ? MAX_POSITION : kept;
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
            entries = lexemes.get(simpleStem(lowerstr(lexeme)));
        }
        if (entries == null) return false;
        if (weights == null || weights.isEmpty()) return true;
        // A lexeme with no positions carries no weights either, so there is no weight for the
        // restriction to fail against and the lexeme answers for every one of them. Requiring a
        // positional entry made a stripped document match no weighted query at all.
        if (entries.isEmpty()) return true;
        return entries.stream().anyMatch(e -> weights.contains(e.weight()));
    }

    /** Number of distinct lexemes. */
    public int length() {
        return lexemes.size();
    }

    /** Remove positions and weights, keeping only lexemes. */
    public TsVector strip() {
        Map<String, List<PosEntry>> stripped = newLexemeMap();
        for (String key : lexemes.keySet()) {
            stripped.put(key, Cols.listOf());
        }
        return new TsVector(stripped);
    }

    /**
     * Set the weight on every position of every lexeme.
     *
     * <p>A weight belongs to a position, so a lexeme that has no positions has nothing to carry
     * one and is left as it is. Inventing a position to hang the weight on wrote text that this
     * reader then refuses -- {@code setweight(strip(v),'A')} could not be read back.
     */
    public TsVector setWeight(char weight) {
        Map<String, List<PosEntry>> result = newLexemeMap();
        for (Map.Entry<String, List<PosEntry>> entry : lexemes.entrySet()) {
            List<PosEntry> newEntries = new ArrayList<>();
            for (PosEntry pe : entry.getValue()) {
                newEntries.add(new PosEntry(pe.position(), weight));
            }
            result.put(entry.getKey(), newEntries);
        }
        return new TsVector(result);
    }

    /** Set weight only for specified lexemes. */
    public TsVector setWeight(char weight, List<String> filterLexemes) {
        Map<String, List<PosEntry>> result = newLexemeMap();
        Set<String> filterSet = new HashSet<>(filterLexemes);
        // Also add stemmed forms
        for (String l : filterLexemes) filterSet.add(simpleStem(lowerstr(l)));
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

    /**
     * Remove the named lexemes.
     *
     * <p>The names are lexemes already, so each one names itself and nothing else. Removing the
     * stemmed form as well took out a second lexeme the caller never named:
     * {@code ts_delete(to_tsvector('english','cats and dogs'), 'cats')} lost {@code 'cat'},
     * which is the lexeme the document actually holds and not the one that was asked for.
     */
    public TsVector delete(List<String> toDelete) {
        Map<String, List<PosEntry>> result = newLexemeMap();
        result.putAll(lexemes);
        for (String l : toDelete) {
            result.remove(l);
        }
        return new TsVector(result);
    }

    /** Filter: keep only lexemes that have any of the given weights. */
    public TsVector filter(Set<Character> weights) {
        if (weights == null || weights.isEmpty()) return empty();
        Map<String, List<PosEntry>> result = newLexemeMap();
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
        Map<String, List<PosEntry>> result = newLexemeMap();
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
        // Merge other vector's entries with shifted positions. Shifting can carry a position
        // past the last representable one, and normalising brings it back and drops the
        // duplicate that then makes -- a position no document can hold is not one to write.
        for (Map.Entry<String, List<PosEntry>> entry : other.lexemes.entrySet()) {
            List<PosEntry> existing = result.computeIfAbsent(entry.getKey(), k -> new ArrayList<>());
            for (PosEntry pe : entry.getValue()) {
                existing.add(new PosEntry(Math.min(pe.position() + maxPos, MAX_POSITION),
                        pe.weight()));
            }
        }
        return new TsVector(normaliseAll(result));
    }

    /** Convert to an array of lexeme strings. */
    public List<String> toArray() {
        return new ArrayList<>(lexemes.keySet());
    }

    /**
     * Build a tsvector from an array of strings (PG: no positions assigned).
     *
     * <p>Every element has to be a lexeme. Neither nothing nor an empty string is one, and
     * passing over them quietly built a vector from an array that did not describe one.
     */
    public static TsVector fromArray(List<String> words) {
        Map<String, List<PosEntry>> lexemes = newLexemeMap();
        for (String word : words) {
            if (word == null) {
                throw new MemgresException("lexeme array may not contain nulls", "22004");
            }
            if (word.isEmpty()) {
                throw new MemgresException("lexeme array may not contain empty strings", "2200F");
            }
            {
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

    /**
     * A lexeme with no positions still takes part in the ranking, as the one place it could be.
     *
     * <p>PostgreSQL scores such an entry against a stand-in position of zero with the weakest
     * weight, and treats the distance between two of them as the largest there is. Skipping
     * them scored a stripped document as though its lexemes were not there at all.
     */
    private static final List<PosEntry> POSNULL =
            Collections.singletonList(new PosEntry(0, 'D'));

    /** PG's calc_rank_and: proximity-based ranking for AND queries. */
    private float calcRankAnd(double[] w, List<String> matchedTerms) {
        float res = -1.0f;
        for (int i = 0; i < matchedTerms.size(); i++) {
            List<PosEntry> posI = positionsOrNull(matchedTerms.get(i));
            for (int j = i + 1; j < matchedTerms.size(); j++) {
                List<PosEntry> posJ = positionsOrNull(matchedTerms.get(j));
                boolean standIn = posI == POSNULL || posJ == POSNULL;
                for (PosEntry pi : posI) {
                    for (PosEntry pj : posJ) {
                        int dist = Math.abs(pi.position() - pj.position());
                        // Two different operands at the same position contribute nothing --
                        // unless one of them is only there as a stand-in, in which case they
                        // are as far apart as two lexemes can be.
                        if (dist == 0 && !standIn) continue;
                        if (dist == 0) dist = MAX_POSITION + 1;
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
    /** The positions a lexeme holds, or the single stand-in where it holds none. */
    private List<PosEntry> positionsOrNull(String lexeme) {
        List<PosEntry> held = lexemes.get(lexeme);
        return held == null || held.isEmpty() ? POSNULL : held;
    }

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
            int end = -1, q = 0;
            for (int ptr = from; ptr < len; ptr++) {
                if (satisfied(query, doc, terms, from, ptr)) {
                    q = doc.get(ptr)[0];
                    end = ptr;
                    break;
                }
            }
            if (end < 0) return null;

            // Now shrink from the right-hand end back down to the smallest span.
            int begin = -1, p = Integer.MAX_VALUE;
            for (int ptr = end; ptr >= from; ptr--) {
                if (satisfied(query, doc, terms, ptr, end)) {
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

    /**
     * Whether a span of the document satisfies the query.
     *
     * <p>The span is read as a document of its own and the query asked about it, so a phrase
     * inside the query is answered by where its lexemes actually sit. Recording only which
     * lexemes had been seen made every phrase behave as an AND, and a cover was found wherever
     * the words appeared at all rather than where they appear next to each other.
     */
    private static boolean satisfied(TsQuery query, List<int[]> doc, List<String> terms,
                                     int from, int to) {
        Map<String, List<PosEntry>> span = newLexemeMap();
        for (int i = from; i <= to; i++) {
            int[] entry = doc.get(i);
            span.computeIfAbsent(terms.get(entry[2]), k -> new ArrayList<PosEntry>())
                    .add(new PosEntry(entry[0], WEIGHT_LETTERS[entry[1]]));
        }
        return new TsVector(span).matches(query);
    }

    /** The weights in the order their indexes run, weakest first. */
    private static final char[] WEIGHT_LETTERS = {'D', 'C', 'B', 'A'};

    /**
     * A lexeme folded to lower case.
     *
     * <p>PostgreSQL folds a text-search token with its own {@code lowerstr}, which maps each
     * character on its own. Java's full mapping turns one character into several -- folding
     * {@code İ} gave a two-character lexeme -- so a document and a query built from the same
     * word could end up holding different lexemes and no longer match.
     */
    static String lowerstr(String word) {
        return StringFunctions.foldCase(word, false);
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
            // A lexeme is written between quotes, so a quote inside one is written twice --
            // otherwise the text closes the lexeme early and cannot be read back as what it
            // was written from, which is the whole point of having a text form.
            sb.append("'").append(entry.getKey().replace("'", "''")).append("'");
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
