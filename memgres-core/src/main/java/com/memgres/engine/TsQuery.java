package com.memgres.engine;

import java.util.*;

/**
 * PostgreSQL-compatible tsquery implementation for full-text search.
 * Supports &amp; (AND), | (OR), ! (NOT), &lt;-&gt; (PHRASE), &lt;N&gt; (FOLLOWED BY) operators,
 * prefix matching (:*), and weight filtering (:ABC).
 */
public class TsQuery {

    public enum Op { AND, OR, NOT, TERM, PHRASE }

    private final Op op;
    private final String term;
    private final boolean prefix;          // :* prefix matching
    private final Set<Character> weights;  // :ABC weight filter
    private final TsQuery left;
    private final TsQuery right;
    private final int phraseDistance;       // for PHRASE: <-> is 1, <N> is N

    private TsQuery(Op op, String term, boolean prefix, Set<Character> weights,
                    TsQuery left, TsQuery right, int phraseDistance) {
        this.op = op;
        this.term = term;
        this.prefix = prefix;
        this.weights = weights;
        this.left = left;
        this.right = right;
        this.phraseDistance = phraseDistance;
    }

    public static TsQuery term(String t) {
        return new TsQuery(Op.TERM, TsVector.simpleStem(t.toLowerCase()), false, null, null, null, 0);
    }

    public static TsQuery term(String t, boolean prefix, Set<Character> weights) {
        return new TsQuery(Op.TERM, TsVector.simpleStem(t.toLowerCase()), prefix, weights, null, null, 0);
    }

    public static TsQuery and(TsQuery l, TsQuery r) {
        return new TsQuery(Op.AND, null, false, null, l, r, 0);
    }

    public static TsQuery or(TsQuery l, TsQuery r) {
        return new TsQuery(Op.OR, null, false, null, l, r, 0);
    }

    public static TsQuery not(TsQuery operand) {
        return new TsQuery(Op.NOT, null, false, null, operand, null, 0);
    }

    public static TsQuery phrase(TsQuery l, TsQuery r, int distance) {
        return new TsQuery(Op.PHRASE, null, false, null, l, r, distance);
    }

    public Op getOp() { return op; }
    public String getTerm() { return term; }
    public TsQuery getLeft() { return left; }
    public TsQuery getRight() { return right; }
    public int getPhraseDistance() { return phraseDistance; }
    public boolean isPrefix() { return prefix; }
    public Set<Character> getWeights() { return weights; }

    /**
     * Parse a tsquery string like 'word1 & word2 | !word3' or 'fat <-> cat' or 'pre:*A'.
     */
    public static TsQuery parse(String input) {
        if (input == null || input.trim().isEmpty()) return term("");
        List<String> tokens = tokenize(input);
        int[] pos = {0};
        return parseOr(tokens, pos);
    }

    private static TsQuery parseOr(List<String> tokens, int[] pos) {
        TsQuery left = parseAnd(tokens, pos);
        while (pos[0] < tokens.size() && tokens.get(pos[0]).equals("|")) {
            pos[0]++;
            TsQuery right = parseAnd(tokens, pos);
            left = or(left, right);
        }
        return left;
    }

    private static TsQuery parseAnd(List<String> tokens, int[] pos) {
        TsQuery left = parsePhrase(tokens, pos);
        while (pos[0] < tokens.size() && tokens.get(pos[0]).equals("&")) {
            pos[0]++;
            TsQuery right = parsePhrase(tokens, pos);
            left = and(left, right);
        }
        return left;
    }

    private static TsQuery parsePhrase(List<String> tokens, int[] pos) {
        TsQuery left = parsePrimary(tokens, pos);
        while (pos[0] < tokens.size()) {
            String tok = tokens.get(pos[0]);
            if (tok.equals("<->")) {
                pos[0]++;
                TsQuery right = parsePrimary(tokens, pos);
                left = phrase(left, right, 1);
            } else if (tok.startsWith("<") && tok.endsWith(">")) {
                // <N> distance operator
                try {
                    int dist = Integer.parseInt(tok.substring(1, tok.length() - 1));
                    pos[0]++;
                    TsQuery right = parsePrimary(tokens, pos);
                    left = phrase(left, right, dist);
                } catch (NumberFormatException e) {
                    break;
                }
            } else {
                break;
            }
        }
        return left;
    }

    private static TsQuery parsePrimary(List<String> tokens, int[] pos) {
        if (pos[0] >= tokens.size()) return term("");
        String t = tokens.get(pos[0]);
        if (t.equals("!") || t.equals("!!")) {
            pos[0]++;
            return not(parsePrimary(tokens, pos));
        }
        if (t.equals("(")) {
            pos[0]++;
            TsQuery result = parseOr(tokens, pos);
            if (pos[0] < tokens.size() && tokens.get(pos[0]).equals(")")) pos[0]++;
            return result;
        }
        pos[0]++;
        // Remove surrounding quotes if present
        if (t.startsWith("'") && t.endsWith("'") && t.length() > 1) {
            t = t.substring(1, t.length() - 1);
        }
        // Check for weight/prefix modifiers: word:*AB or word:AB or word:*
        boolean isPrefix = false;
        Set<Character> ws = null;
        int colonIdx = t.indexOf(':');
        if (colonIdx > 0) {
            String modifier = t.substring(colonIdx + 1);
            t = t.substring(0, colonIdx);
            if (modifier.contains("*")) {
                isPrefix = true;
                modifier = modifier.replace("*", "");
            }
            if (!modifier.isEmpty()) {
                ws = new HashSet<>();
                for (char c : modifier.toUpperCase().toCharArray()) {
                    if (c >= 'A' && c <= 'D') ws.add(c);
                }
                if (ws.isEmpty()) ws = null;
            }
        }
        return term(t, isPrefix, ws);
    }

    private static List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        int i = 0;
        while (i < input.length()) {
            char c = input.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            // <-> or <N>
            if (c == '<') {
                int end = input.indexOf('>', i);
                if (end > i) {
                    tokens.add(input.substring(i, end + 1));
                    i = end + 1;
                    continue;
                }
            }
            if (c == '&' || c == '|' || c == '(' || c == ')') {
                tokens.add(String.valueOf(c));
                i++;
                continue;
            }
            if (c == '!') {
                if (i + 1 < input.length() && input.charAt(i + 1) == '!') {
                    tokens.add("!!");
                    i += 2;
                } else {
                    tokens.add("!");
                    i++;
                }
                continue;
            }
            if (c == '\'') {
                int end = input.indexOf('\'', i + 1);
                if (end < 0) end = input.length() - 1;
                String lexeme = input.substring(i, end + 1);
                i = end + 1;
                // Check for trailing :*AB
                if (i < input.length() && input.charAt(i) == ':') {
                    i++; // skip ':'
                    StringBuilder mod = new StringBuilder();
                    while (i < input.length() && (input.charAt(i) == '*'
                            || Character.isLetter(input.charAt(i)))) {
                        mod.append(input.charAt(i));
                        i++;
                    }
                    // Strip quotes and add modifier
                    String inner = lexeme.substring(1, lexeme.length() - 1);
                    tokens.add(inner + ":" + mod);
                } else {
                    tokens.add(lexeme);
                }
                continue;
            }
            StringBuilder sb = new StringBuilder();
            while (i < input.length() && !Character.isWhitespace(input.charAt(i))
                    && input.charAt(i) != '&' && input.charAt(i) != '|'
                    && input.charAt(i) != '!' && input.charAt(i) != '('
                    && input.charAt(i) != ')' && input.charAt(i) != '<') {
                sb.append(input.charAt(i));
                i++;
            }
            if (!sb.isEmpty()) tokens.add(sb.toString());
        }
        return tokens;
    }

    public boolean matches(TsVector vector) {
        switch (op) {
            case TERM: {
                if (term == null || term.isEmpty()) return true;
                if (prefix) {
                    // Prefix match: check if any lexeme starts with term
                    boolean found = vector.getLexemes().stream()
                            .anyMatch(l -> l.startsWith(term));
                    if (found && weights != null) {
                        return vector.getLexemes().stream()
                                .filter(l -> l.startsWith(term))
                                .anyMatch(l -> vector.containsLexemeWithWeight(l, weights));
                    }
                    return found;
                }
                if (weights != null) {
                    return vector.containsLexemeWithWeight(term, weights);
                }
                return vector.containsLexeme(term);
            }
            case AND:
                return left.matches(vector) && right.matches(vector);
            case OR:
                return left.matches(vector) || right.matches(vector);
            case NOT:
                return !left.matches(vector);
            case PHRASE: {
                // Phrase matching: left and right must appear with correct distance
                if (left.op != Op.TERM || right.op != Op.TERM) {
                    // For complex phrase subexpressions, fall back to AND
                    return left.matches(vector) && right.matches(vector);
                }
                List<Integer> leftPositions = vector.getPositions(left.term);
                List<Integer> rightPositions = vector.getPositions(right.term);
                if (leftPositions.isEmpty() || rightPositions.isEmpty()) return false;
                // Check if any right position = left position + phraseDistance
                for (int lp : leftPositions) {
                    for (int rp : rightPositions) {
                        if (rp - lp == phraseDistance) return true;
                    }
                }
                return false;
            }
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }

    public boolean containsTerm(String lexeme) {
        switch (op) {
            case TERM: {
                if (prefix) return lexeme.startsWith(term);
                return term.equals(lexeme);
            }
            case AND:
            case OR:
                return left.containsTerm(lexeme) || right.containsTerm(lexeme);
            case PHRASE:
                return left.containsTerm(lexeme) || right.containsTerm(lexeme);
            case NOT:
                return false;
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }

    /** Count the number of nodes in the query tree. */
    public int numNode() {
        switch (op) {
            case TERM:
                return 1;
            case NOT:
                return 1 + left.numNode();
            case AND:
            case OR:
            case PHRASE:
                return 1 + left.numNode() + right.numNode();
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }

    /** Return a text representation of the query tree (like querytree()). */
    public String queryTree() {
        switch (op) {
            case TERM: {
                if (term == null || term.isEmpty()) return "T";
                return "'" + term + "'";
            }
            case AND:
                return "( " + left.queryTree() + " & " + right.queryTree() + " )";
            case OR:
                return "( " + left.queryTree() + " | " + right.queryTree() + " )";
            case NOT:
                return "!( " + left.queryTree() + " )";
            case PHRASE:
                return "( " + left.queryTree() + " <" + phraseDistance + "> " + right.queryTree() + " )";
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }

    /** Collect all terms from the query. */
    public List<String> collectTerms() {
        List<String> result = new ArrayList<>();
        collectTermsInto(result);
        return result;
    }

    private void collectTermsInto(List<String> result) {
        if (op == Op.TERM && term != null && !term.isEmpty()) {
            result.add(term);
        }
        if (left != null) left.collectTermsInto(result);
        if (right != null) right.collectTermsInto(result);
    }

    @Override
    public String toString() {
        switch (op) {
            case TERM: {
                StringBuilder sb = new StringBuilder("'").append(term != null ? term : "").append("'");
                if (prefix || weights != null) {
                    sb.append(":");
                    if (prefix) sb.append("*");
                    if (weights != null) {
                        List<Character> sorted = new ArrayList<>(weights);
                        Collections.sort(sorted);
                        for (char w : sorted) sb.append(w);
                    }
                }
                return sb.toString();
            }
            case AND:
                return left + " & " + right;
            case OR:
                return left + " | " + right;
            case NOT:
                return "!" + left;
            case PHRASE:
                return left + " " + (phraseDistance == 1 ? "<->" : "<" + phraseDistance + ">") + " " + right;
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }
}
