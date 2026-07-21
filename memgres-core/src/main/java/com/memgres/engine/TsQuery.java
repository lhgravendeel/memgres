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
        String stemmed = TsVector.simpleStem(t.toLowerCase());
        if (TsVector.isStopWord(stemmed)) stemmed = "";
        return new TsQuery(Op.TERM, stemmed, false, null, null, null, 0);
    }

    /** Create a term with explicit stemming control. */
    public static TsQuery termRaw(String t) {
        return new TsQuery(Op.TERM, t, false, null, null, null, 0);
    }

    public static TsQuery term(String t, boolean prefix, Set<Character> weights) {
        String stemmed = TsVector.simpleStem(t.toLowerCase());
        if (TsVector.isStopWord(stemmed)) stemmed = "";
        return new TsQuery(Op.TERM, stemmed, prefix, weights, null, null, 0);
    }

    /** Create a term without stemming (for 'simple' config). */
    public static TsQuery termSimple(String t, boolean prefix, Set<Character> weights) {
        return new TsQuery(Op.TERM, t.toLowerCase(), prefix, weights, null, null, 0);
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

    /** Create an empty query (represents the empty tsquery value). */
    public static TsQuery emptyQuery() {
        return new TsQuery(Op.TERM, "", false, null, null, null, 0);
    }

    public Op getOp() { return op; }
    public String getTerm() { return term; }
    public TsQuery getLeft() { return left; }
    public TsQuery getRight() { return right; }
    public int getPhraseDistance() { return phraseDistance; }
    public boolean isPrefix() { return prefix; }
    public Set<Character> getWeights() { return weights; }

    /** Check if this query is empty (represents ''::tsquery or stopword-only result). */
    public boolean isEmpty() {
        if (op == Op.TERM && (term == null || term.isEmpty()) && !prefix) return true;
        return false;
    }

    /** Internal signal used while parsing a tsquery string; converted to a
     *  MemgresException (SQLSTATE 42601) carrying the original input at the top level. */
    private static class TsqParseError extends RuntimeException {
        final boolean noOperand;
        TsqParseError(boolean noOperand) { this.noOperand = noOperand; }
    }

    /**
     * Parse a tsquery string like 'word1 & word2 | !word3' or 'fat <-> cat' or 'pre:*A'.
     * Invalid input raises SQLSTATE 42601 the way PostgreSQL does: two adjacent
     * operands with no operator is a "syntax error in tsquery", and an operator
     * missing an operand is "no operand in tsquery".
     */
    public static TsQuery parse(String input) {
        if (input == null || input.trim().isEmpty()) return emptyQuery();
        List<String> tokens = tokenize(input);
        if (tokens.isEmpty()) return emptyQuery();
        try {
            int[] pos = {0};
            TsQuery result = parseOr(tokens, pos);
            // Leftover tokens mean two operands with no operator between them.
            if (pos[0] < tokens.size()) throw new TsqParseError(false);
            return result;
        } catch (TsqParseError e) {
            if (e.noOperand) {
                throw new MemgresException("no operand in tsquery: \"" + input + "\"", "42601");
            }
            throw new MemgresException("syntax error in tsquery: \"" + input + "\"", "42601");
        }
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
        // A primary (operand) is required here; running out of tokens or hitting a
        // binary operator / close-paren means the preceding operator has no operand.
        if (pos[0] >= tokens.size()) throw new TsqParseError(true);
        String t = tokens.get(pos[0]);
        if (t.equals("&") || t.equals("|") || t.equals(")")
                || t.equals("<->") || (t.startsWith("<") && t.endsWith(">"))) {
            throw new TsqParseError(true);
        }
        if (t.equals("!")) {
            pos[0]++;
            return not(parsePrimary(tokens, pos));
        }
        if (t.equals("(")) {
            pos[0]++;
            TsQuery result = parseOr(tokens, pos);
            if (pos[0] < tokens.size() && tokens.get(pos[0]).equals(")")) pos[0]++;
            else throw new TsqParseError(false); // missing ')'
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
                // Each '!' is a separate negation: '!!cat' parses as !(!'cat'),
                // matching PG (which prints it as !!'cat').
                tokens.add("!");
                i++;
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
            if (sb.length() > 0) tokens.add(sb.toString());
        }
        return tokens;
    }

    public boolean matches(TsVector vector) {
        switch (op) {
            case TERM: {
                if (term == null || term.isEmpty()) return true;
                if (prefix) {
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
                if (left.term == null || left.term.isEmpty() || right.term == null || right.term.isEmpty()) {
                    // Empty terms (from stopwords) match anything
                    return left.matches(vector) && right.matches(vector);
                }
                List<Integer> leftPositions = vector.getPositions(left.term);
                List<Integer> rightPositions = vector.getPositions(right.term);
                if (leftPositions.isEmpty() || rightPositions.isEmpty()) return false;
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

    /** Return a text representation of the query tree (like PG's querytree()).
     *  PG's querytree() strips NOT branches entirely and shows 'T' for them. */
    public String queryTree() {
        // If the entire query is just NOT, return 'T'
        if (op == Op.NOT) return "T";
        return queryTreeInner();
    }

    private String queryTreeInner() {
        switch (op) {
            case TERM: {
                if (term == null || term.isEmpty()) return "T";
                return "'" + term + "'";
            }
            case AND: {
                String l = stripNot(left);
                String r = stripNot(right);
                if ("T".equals(l) && "T".equals(r)) return "T";
                if ("T".equals(l)) return r;
                if ("T".equals(r)) return l;
                return "( " + l + " & " + r + " )";
            }
            case OR: {
                String l = stripNot(left);
                String r = stripNot(right);
                if ("T".equals(l) && "T".equals(r)) return "T";
                if ("T".equals(l)) return r;
                if ("T".equals(r)) return l;
                return "( " + l + " | " + r + " )";
            }
            case NOT:
                return "T";
            case PHRASE: {
                String l = stripNot(left);
                String r = stripNot(right);
                if ("T".equals(l) || "T".equals(r)) return "T";
                return "( " + l + " <" + phraseDistance + "> " + r + " )";
            }
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }

    private static String stripNot(TsQuery q) {
        if (q.op == Op.NOT) return "T";
        return q.queryTreeInner();
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
        // PG displays empty tsquery as empty string
        if (isEmpty()) return "";
        return toStringInner(false, null);
    }

    private String toStringInner(boolean parentNeedsParens, Op parentOp) {
        switch (op) {
            case TERM: {
                if (term == null || term.isEmpty()) return "";
                StringBuilder sb = new StringBuilder("'").append(term).append("'");
                if (prefix || weights != null) {
                    sb.append(":");
                    if (weights != null) {
                        List<Character> sorted = new ArrayList<>(weights);
                        Collections.sort(sorted);
                        for (char w : sorted) sb.append(w);
                    }
                    if (prefix) sb.append("*");
                }
                return sb.toString();
            }
            case AND: {
                String l = left.toStringInner(true, Op.AND);
                String r = right.toStringInner(true, Op.AND);
                String result = l + " & " + r;
                // Need parens if parent is a phrase or if this is inside a NOT
                if (parentOp == Op.PHRASE) return "( " + result + " )";
                return result;
            }
            case OR: {
                String l = left.toStringInner(true, Op.OR);
                String r = right.toStringInner(true, Op.OR);
                String result = l + " | " + r;
                // OR has lower precedence than AND and PHRASE
                if (parentOp == Op.AND || parentOp == Op.PHRASE) return "( " + result + " )";
                return result;
            }
            case NOT: {
                String inner = left.toStringInner(true, Op.NOT);
                // If the child is a compound expr, it will have its own parens
                if (left.op == Op.AND || left.op == Op.OR || left.op == Op.PHRASE) {
                    return "!( " + left.toStringInner(false, null) + " )";
                }
                return "!" + inner;
            }
            case PHRASE: {
                String l = left.toStringInner(true, Op.PHRASE);
                String r = right.toStringInner(true, Op.PHRASE);
                String distStr = phraseDistance == 1 ? "<->" : "<" + phraseDistance + ">";
                return l + " " + distStr + " " + r;
            }
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }
}
