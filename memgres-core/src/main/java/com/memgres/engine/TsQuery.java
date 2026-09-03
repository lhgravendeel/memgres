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
        // PG's snowball dictionary tests the stop list before stemming.
        String lower = TsVector.lowerstr(t);
        String stemmed = TsVector.isStopWord(lower) ? "" : TsVector.simpleStem(lower);
        return new TsQuery(Op.TERM, stemmed, false, null, null, null, 0);
    }

    /** Create a term with explicit stemming control. */
    public static TsQuery termRaw(String t) {
        return new TsQuery(Op.TERM, t, false, null, null, null, 0);
    }

    public static TsQuery term(String t, boolean prefix, Set<Character> weights) {
        String lower = TsVector.lowerstr(t);
        String stemmed = TsVector.isStopWord(lower) ? "" : TsVector.simpleStem(lower);
        return new TsQuery(Op.TERM, stemmed, prefix, weights, null, null, 0);
    }

    /** Create a term without stemming (for 'simple' config). */
    public static TsQuery termSimple(String t, boolean prefix, Set<Character> weights) {
        return new TsQuery(Op.TERM, TsVector.lowerstr(t), prefix, weights, null, null, 0);
    }

    /**
     * Combining with an empty query drops the empty side rather than keeping a hole in the tree.
     * A stop word makes {@code to_tsquery('a')} empty, and PostgreSQL folds it away, so
     * {@code to_tsquery('a') && to_tsquery('b')} is simply {@code 'b'} — keeping the empty operand
     * printed a query beginning with an operator and matched nothing.
     */
    public static TsQuery and(TsQuery l, TsQuery r) {
        if (l == null || l.isEmpty()) return r == null ? emptyQuery() : r;
        if (r == null || r.isEmpty()) return l;
        return new TsQuery(Op.AND, null, false, null, l, r, 0);
    }

    public static TsQuery or(TsQuery l, TsQuery r) {
        if (l == null || l.isEmpty()) return r == null ? emptyQuery() : r;
        if (r == null || r.isEmpty()) return l;
        return new TsQuery(Op.OR, null, false, null, l, r, 0);
    }

    /** Negating nothing is still nothing: there is no lexeme for the NOT to be about. */
    public static TsQuery not(TsQuery operand) {
        if (operand == null || operand.isEmpty()) return emptyQuery();
        return new TsQuery(Op.NOT, null, false, null, operand, null, 0);
    }

    public static TsQuery phrase(TsQuery l, TsQuery r, int distance) {
        if (l == null || l.isEmpty()) return r == null ? emptyQuery() : r;
        if (r == null || r.isEmpty()) return l;
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
    /**
     * Read a tsquery the way a literal is read: the lexemes are taken exactly as written.
     *
     * <p>This is the whole difference between {@code 'Cats'::tsquery} and
     * {@code to_tsquery('Cats')}. A literal already names the lexemes to look for, so nothing is
     * lowercased, stemmed or dropped — {@code 'Cats'} stays {@code 'Cats'} and {@code 'a'} stays
     * {@code 'a'} even though the English dictionary would call it a stop word. Running a literal
     * through a dictionary turned {@code 'a & b'} into a query with a hole in it.
     */
    public static TsQuery parse(String input) {
        return parse(input, null);
    }

    /**
     * Read a tsquery and normalize each lexeme through a text search configuration, which is what
     * {@code to_tsquery} does. {@code simple} folds case and no more; every other configuration
     * also drops stop words and stems what is left.
     */
    public static TsQuery parse(String input, String config) {
        if (input == null || input.trim().isEmpty()) return emptyQuery();
        try {
            List<String> tokens = tokenize(input);
            if (tokens.isEmpty()) return emptyQuery();
            int[] pos = {0};
            TsQuery result = parseOr(tokens, pos, config);
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

    private static TsQuery parseOr(List<String> tokens, int[] pos, String config) {
        TsQuery left = parseAnd(tokens, pos, config);
        while (pos[0] < tokens.size() && tokens.get(pos[0]).equals("|")) {
            pos[0]++;
            TsQuery right = parseAnd(tokens, pos, config);
            left = or(left, right);
        }
        return left;
    }

    private static TsQuery parseAnd(List<String> tokens, int[] pos, String config) {
        TsQuery left = parsePhrase(tokens, pos, config);
        while (pos[0] < tokens.size() && tokens.get(pos[0]).equals("&")) {
            pos[0]++;
            TsQuery right = parsePhrase(tokens, pos, config);
            left = and(left, right);
        }
        return left;
    }

    /**
     * A run of lexemes joined by phrase operators.
     *
     * <p>A stop word still takes up a place in the document, so a phrase written across one is
     * not the two remaining lexemes side by side but the two of them that much further apart:
     * {@code the <-> cat <-> the <-> dog} is {@code 'cat' <2> 'dog'}. The dropped operand's
     * distance used to be dropped with it, which named a phrase the document never holds.
     */
    private static TsQuery parsePhrase(List<String> tokens, int[] pos, String config) {
        TsQuery left = parsePrimary(tokens, pos, config);
        int gap = 0;
        while (pos[0] < tokens.size()) {
            String tok = tokens.get(pos[0]);
            int dist;
            if (tok.equals("<->")) dist = 1;
            else if (tok.startsWith("<") && tok.endsWith(">")) dist = phraseDistance(tok);
            else break;
            pos[0]++;
            TsQuery right = parsePrimary(tokens, pos, config);
            if (left == null || left.isEmpty()) {
                // Nothing yet to be before anything: the phrase begins at the next lexeme.
                left = right;
                gap = 0;
            } else if (right == null || right.isEmpty()) {
                gap += dist;
            } else {
                left = phrase(left, right, dist + gap);
                gap = 0;
            }
        }
        return left;
    }

    private static TsQuery parsePrimary(List<String> tokens, int[] pos, String config) {
        // A primary (operand) is required here; running out of tokens or hitting a
        // binary operator / close-paren means the preceding operator has no operand.
        if (pos[0] >= tokens.size()) throw new TsqParseError(true);
        String t = tokens.get(pos[0]);
        // An operator standing where an operand belongs is a mis-written query, not a query
        // whose last operator was left dangling: only running off the end is "no operand".
        if (t.equals("&") || t.equals("|") || t.equals(")")
                || t.equals("<->") || (t.startsWith("<") && t.endsWith(">"))) {
            throw new TsqParseError(false);
        }
        if (t.equals("!")) {
            pos[0]++;
            return not(parsePrimary(tokens, pos, config));
        }
        if (t.equals("(")) {
            pos[0]++;
            TsQuery result = parseOr(tokens, pos, config);
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
                for (char c : modifier.toCharArray()) {
                    char upper = Character.toUpperCase(c);
                    // Only the four weights exist. A letter outside them is not a weight the
                    // query can be about, so the query is mis-written; dropping it silently
                    // answered a different question from the one that was asked.
                    if (upper < 'A' || upper > 'D') throw new TsqParseError(false);
                    ws.add(upper);
                }
                if (ws.isEmpty()) ws = null;
            }
        }
        return lexeme(t, isPrefix, ws, config);
    }

    /**
     * The lexeme a written word stands for. With no configuration the word is the lexeme, which is
     * how a literal is read; with one, the word goes through that configuration's dictionary.
     */
    private static TsQuery lexeme(String word, boolean prefix, Set<Character> weights, String config) {
        if (config == null) return new TsQuery(Op.TERM, word, prefix, weights, null, null, 0);
        // With a configuration the word goes through the parser as well as the dictionary, and
        // the parser may find several words in it: a quoted lexeme holding a space asks for the
        // two words next to each other, which is a phrase. Read as one lexeme it asked for a
        // lexeme with a space in it, which nothing indexes.
        String[] words = word.trim().split("\\s+");
        if (words.length > 1) {
            TsQuery joined = null;
            for (String part : words) {
                if (part.isEmpty()) continue;
                TsQuery one = oneLexeme(part, prefix, weights, config);
                joined = joined == null ? one : phrase(joined, one, 1);
            }
            if (joined != null) return joined;
        }
        return oneLexeme(word, prefix, weights, config);
    }

    private static TsQuery oneLexeme(String word, boolean prefix, Set<Character> weights,
                                     String config) {
        if ("simple".equalsIgnoreCase(config)) return termSimple(word, prefix, weights);
        return term(word, prefix, weights);
    }

    private static List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        int i = 0;
        while (i < input.length()) {
            char c = input.charAt(i);
            if (Character.isWhitespace(c)) { i++; continue; }
            // <-> or <N>. A '<' always begins a phrase operator, so one that does not spell a
            // whole operator ends the read here. Falling through instead left the character for
            // the bare-word loop below, which excludes '<' -- nothing was consumed and the outer
            // loop came round again on the same character, for as long as the client waited.
            if (c == '<') {
                int end = input.indexOf('>', i);
                if (end < 0) throw new TsqParseError(false);
                String inner = input.substring(i + 1, end);
                if (!inner.equals("-") && !isAllDigits(inner)) throw new TsqParseError(false);
                tokens.add(input.substring(i, end + 1));
                i = end + 1;
                continue;
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
                // Two quotes inside a quoted lexeme stand for one quote in the lexeme, so the
                // first of a pair does not end it: 'it''s' is one lexeme with a quote in it.
                int end = i + 1;
                StringBuilder quoted = new StringBuilder();
                while (end < input.length()) {
                    if (input.charAt(end) == '\'') {
                        if (end + 1 < input.length() && input.charAt(end + 1) == '\'') {
                            quoted.append('\'');
                            end += 2;
                            continue;
                        }
                        break;
                    }
                    quoted.append(input.charAt(end));
                    end++;
                }
                if (end >= input.length()) end = input.length() - 1;
                String lexeme = "'" + quoted + "'";
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

    private static boolean isAllDigits(String s) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) < '0' || s.charAt(i) > '9') return false;
        }
        return true;
    }

    /** The furthest apart a phrase operator can say two lexemes are. */
    private static final int MAX_PHRASE_DISTANCE = 16384;

    /**
     * The distance a {@code <N>} operator names. Out of range is a value error rather than a
     * syntax error: the operator is spelled correctly and only says something unrepresentable.
     */
    private static int phraseDistance(String token) {
        String digits = token.substring(1, token.length() - 1);
        long value;
        try {
            value = Long.parseLong(digits);
        } catch (NumberFormatException e) {
            value = MAX_PHRASE_DISTANCE + 1L;
        }
        if (value < 0 || value > MAX_PHRASE_DISTANCE) {
            throw new MemgresException("distance in phrase operator must be an integer value "
                    + "between zero and " + MAX_PHRASE_DISTANCE + " inclusive", "22023");
        }
        return (int) value;
    }

    public boolean matches(TsVector vector) {
        switch (op) {
            case TERM: {
                // An empty query names no lexeme, so there is nothing for a document to hold.
                // Matching everything instead made to_tsquery('a') -- a stop word, and so empty --
                // match every row.
                if (term == null || term.isEmpty()) return false;
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
                // A phrase is about where its operands are, so it is answered by working out
                // the positions each side matches at rather than by asking each side whether it
                // matches anywhere. Falling back to AND for an operand that was not a bare
                // lexeme made 'a <-> (b & c)' true of a document holding a, c and b in that
                // order, where the phrase says b and c both have to sit right after a.
                return !matchPositions(vector).isEmpty();
            }
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }

    /**
     * The positions this query matches at, which is what a phrase around it is asked about.
     *
     * <p>A phrase constrains where its operands sit, so each operand has to answer with the
     * places it was found and not merely with whether it was found at all. Two lexemes joined by
     * {@code &} inside a phrase are both wanted at the same place, and joined by {@code |} at
     * either; a phrase of its own contributes the places its own right-hand side ends.
     */
    private Set<Integer> matchPositions(TsVector vector) {
        switch (op) {
            case TERM: {
                Set<Integer> found = new TreeSet<Integer>();
                if (term == null || term.isEmpty()) return found;
                for (String lexeme : vector.getLexemes()) {
                    boolean names = prefix ? lexeme.startsWith(term) : lexeme.equals(term);
                    if (!names) continue;
                    if (weights != null && !vector.containsLexemeWithWeight(lexeme, weights)) {
                        continue;
                    }
                    found.addAll(vector.getPositions(lexeme));
                }
                return found;
            }
            case AND: {
                Set<Integer> found = left.matchPositions(vector);
                found.retainAll(right.matchPositions(vector));
                return found;
            }
            case OR: {
                Set<Integer> found = left.matchPositions(vector);
                found.addAll(right.matchPositions(vector));
                return found;
            }
            case NOT:
                // Inside a phrase there is no position at which "not this" is found.
                return new TreeSet<Integer>();
            default: {
                Set<Integer> before = left.matchPositions(vector);
                Set<Integer> found = new TreeSet<Integer>();
                for (Integer at : right.matchPositions(vector)) {
                    if (before.contains(Integer.valueOf(at.intValue() - phraseDistance))) {
                        found.add(at);
                    }
                }
                return found;
            }
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
                // A lexeme under a NOT is still one the query names, and the ranking is over
                // the lexemes a query names. Answering no here scored a query of nothing but a
                // NOT as though the document held none of its words.
                return left.containsTerm(lexeme);
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }

    /** Count the number of nodes in the query tree. */
    public int numNode() {
        switch (op) {
            case TERM:
                // An empty query has no nodes at all.
                return term == null || term.isEmpty() ? 0 : 1;
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
    /**
     * The part of the query an index can be searched with, written out.
     *
     * <p>A NOT branch names what must not be there, which no index lookup can supply, so it is
     * written as {@code T} -- "anything". What is left is printed the way the query itself is
     * printed, brackets and all: the outermost operator is not bracketed, because there is
     * nothing around it for the brackets to separate it from.
     */
    public String queryTree() {
        // A query with nothing in it has nothing to write, not even the word for "anything".
        if (isEmpty()) return "";
        // If the entire query is just NOT, return 'T'
        if (op == Op.NOT) return "T";
        return queryTreeInner(false, null);
    }

    private String queryTreeInner(boolean nested, Op parentOp) {
        switch (op) {
            case TERM: {
                if (term == null || term.isEmpty()) return "T";
                // A lexeme is written with everything that says which lexemes it names: the
                // prefix star and the weights are part of what would be looked up.
                return toStringInner(false, null);
            }
            case AND:
            case OR:
            case PHRASE: {
                String l = stripNot(left, Op.AND == op || Op.OR == op || Op.PHRASE == op, op);
                String r = stripNot(right, true, op);
                String operator = op == Op.AND ? " & "
                        : op == Op.OR ? " | "
                        : phraseDistance == 1 ? " <-> " : " <" + phraseDistance + "> ";
                // A phrase and an OR are only as searchable as their weakest branch: a phrase
                // needs both lexemes and an OR is satisfied by either, so a branch that names
                // nothing to look up leaves the whole node naming nothing. An AND still has the
                // other branch to look up, which is why only there is a branch dropped.
                if (op != Op.AND && ("T".equals(l) || "T".equals(r))) return "T";
                if ("T".equals(l) && "T".equals(r)) return "T";
                if ("T".equals(l)) return r;
                if ("T".equals(r)) return l;
                String joined = l + operator + r;
                return nested ? "( " + joined + " )" : joined;
            }
            case NOT:
                return "T";
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }

    private static String stripNot(TsQuery q, boolean nested, Op parentOp) {
        if (q.op == Op.NOT) return "T";
        // An operand of the same precedence needs no brackets of its own.
        boolean bracket = q.op != Op.TERM && q.op != parentOp;
        return q.queryTreeInner(bracket, parentOp);
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
                // A quote inside a lexeme is written twice, so that what is printed reads back
                // as the lexeme it was printed from rather than closing it early.
                StringBuilder sb = new StringBuilder("'").append(term.replace("'", "''"))
                        .append("'");
                if (prefix || weights != null) {
                    sb.append(":");
                    // The star says how much of the lexeme has to match and the letters say
                    // which weights count, and PostgreSQL writes them in that order. Writing
                    // the star last spelled a query this reader then read as a weight list
                    // followed by nothing.
                    if (prefix) sb.append("*");
                    if (weights != null) {
                        List<Character> sorted = new ArrayList<>(weights);
                        Collections.sort(sorted);
                        for (char w : sorted) sb.append(w);
                    }
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
                // The phrase operator groups to the left, so a phrase on the right is a
                // different query from the same lexemes run together and has to be written as
                // one: 'a' <-> ( 'b' <-> 'c' ) puts b and c next to each other and a one before
                // b, where 'a' <-> 'b' <-> 'c' puts a before b and b before c. Printing them
                // alike made a value that read back as the other grouping.
                if (right.op == Op.PHRASE) r = "( " + right.toStringInner(false, null) + " )";
                String distStr = phraseDistance == 1 ? "<->" : "<" + phraseDistance + ">";
                return l + " " + distStr + " " + r;
            }
            default:
                throw new IllegalStateException("Unknown op: " + op);
        }
    }
}
