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
        return matches(left, right, null, null);
    }

    /**
     * The same, told what each side was written as. PostgreSQL declares four {@code @@} operators
     * and they do not agree on how to read a bare string, so which one was written decides the
     * answer:
     *
     * <ul>
     *   <li>{@code tsvector @@ tsquery} — both sides already lexemes.</li>
     *   <li>{@code text @@ tsquery} — the text becomes a vector through {@code to_tsvector}.</li>
     *   <li>{@code text @@ text} — and the right becomes a query through
     *       {@code plainto_tsquery}, so {@code 'cat dog'} is two words to find rather than a
     *       tsquery with a syntax error in it.</li>
     *   <li>{@code tsquery @@ tsvector} — the commuted form of the first.</li>
     * </ul>
     *
     * <p>{@code operandType} is null for an untyped literal, which the other side settles.
     */
    public static Object matches(Object left, Object right, String leftType, String rightType) {
        if (left == null || right == null) return null;
        // One of each is the only pairing PG declares; two of a kind matches nothing
        if (left instanceof TsQuery && right instanceof TsQuery) {
            throw new MemgresException("operator does not exist: tsquery @@ tsquery", "42883");
        }
        if (left instanceof TsVector && right instanceof TsVector) {
            throw new MemgresException("operator does not exist: tsvector @@ tsvector", "42883");
        }
        // A query on the left is the commuted form; there is no tsquery @@ text at all.
        if (left instanceof TsQuery && "text".equals(rightType)) {
            throw new MemgresException("operator does not exist: tsquery @@ text", "42883");
        }
        boolean queryOnLeft = left instanceof TsQuery
                || (right instanceof TsVector && !(left instanceof TsVector));
        Object documentSide = queryOnLeft ? right : left;
        Object querySide = queryOnLeft ? left : right;
        String documentType = queryOnLeft ? rightType : leftType;

        TsVector vector;
        if (documentSide instanceof TsVector) {
            vector = (TsVector) documentSide;
        } else if (queryOnLeft && querySide instanceof TsQuery && documentType == null) {
            // An untyped literal to the right of a query is the document, and PostgreSQL reads it
            // as a vector written out: there is no tsquery @@ text to prefer instead. To the left
            // of one it is text, because @@(text, tsquery) exists and text is the preferred type
            // of its category -- which is why 'a b' @@ 'a'::tsquery is false and the commuted form
            // is true.
            TsVector parsed = TsVector.parseLiteral(documentSide.toString());
            vector = parsed != null ? parsed : TsVector.fromText(documentSide.toString());
        } else {
            // Written as text: the words go through the default configuration rather than being
            // read as a vector already written out.
            vector = TsVector.fromText(documentSide.toString());
        }

        TsQuery query;
        if (querySide instanceof TsQuery) {
            query = (TsQuery) querySide;
        } else if (documentSide instanceof TsVector || "tsvector".equals(documentType)) {
            // Opposite a vector, a bare string is a tsquery as written.
            query = TsQuery.parse(querySide.toString());
        } else {
            // text @@ text: the right side is a phrase to look for, not a query to parse.
            query = phraseWordsToQuery(querySide.toString());
        }
        return vector.matches(query);
    }

    /** {@code plainto_tsquery} over the default configuration, which is what text @@ text uses. */
    private static TsQuery phraseWordsToQuery(String text) {
        return plainToTsQuery(text, "english");
    }

    /** phraseto_tsquery: treats input as a phrase (words connected by <N> where N accounts for stopwords).
     *  Stopwords are removed and their positions are accounted for by increasing the distance. */
    public static TsQuery phraseToTsQuery(String input) {
        return phraseToTsQuery(input, "english");
    }

    /**
     * The lexemes a piece of text yields under a configuration, with the place each one sits.
     *
     * <p>This is the same reading {@code to_tsvector} does, through the configuration's own
     * parser: an e-mail address is one token, a decimal is one token, and a word outside ASCII
     * is a word. The query builders used to clean their input with
     * {@code replaceAll("[^a-zA-Z0-9\\s]", " ")} instead, which deleted every non-ASCII letter
     * and shredded every compound -- so the query side and the document side disagreed about
     * what the words even were, and no amount of matching could reconcile them.
     */
    private static List<Object[]> analyse(String text, String config) {
        List<Object[]> out = new ArrayList<Object[]>();
        if (text == null) return out;
        boolean isSimple = "simple".equalsIgnoreCase(config);
        int position = 0;
        for (com.memgres.engine.fts.TsParser.Token token
                : com.memgres.engine.fts.TsParser.parse(text)) {
            com.memgres.engine.fts.TsParser.Dict dict =
                    com.memgres.engine.fts.TsParser.dictionaryFor(token.type());
            if (dict == com.memgres.engine.fts.TsParser.Dict.NONE) continue;
            position++;
            String lower = TsVector.lowerstr(token.text());
            if (isSimple || dict == com.memgres.engine.fts.TsParser.Dict.SIMPLE) {
                out.add(new Object[]{lower, Integer.valueOf(position)});
                continue;
            }
            // A stop word takes its place and contributes no lexeme.
            if (TsVector.isStopWord(lower)) continue;
            out.add(new Object[]{com.memgres.engine.fts.EnglishStemmer.stem(lower),
                    Integer.valueOf(position)});
        }
        return out;
    }

    public static TsQuery phraseToTsQuery(String input, String config) {
        List<Object[]> found = analyse(input, config);
        if (found.isEmpty()) return TsQuery.emptyQuery();
        TsQuery result = TsQuery.termRaw((String) found.get(0)[0]);
        for (int i = 1; i < found.size(); i++) {
            int distance = ((Integer) found.get(i)[1]).intValue()
                    - ((Integer) found.get(i - 1)[1]).intValue();
            result = TsQuery.phrase(result, TsQuery.termRaw((String) found.get(i)[0]), distance);
        }
        return result;
    }
    /** plainto_tsquery: words joined by AND, no special chars, strip punctuation. */
    public static TsQuery plainToTsQuery(String input, String config) {
        List<Object[]> found = analyse(input, config);
        if (found.isEmpty()) return TsQuery.emptyQuery();
        TsQuery result = TsQuery.termRaw((String) found.get(0)[0]);
        for (int i = 1; i < found.size(); i++) {
            result = TsQuery.and(result, TsQuery.termRaw((String) found.get(i)[0]));
        }
        return result;
    }
    /** websearch_to_tsquery: Google-style query parsing. Quoted = phrase, - = NOT, OR = OR, rest = AND. */
    public static TsQuery websearchToTsQuery(String input) {
        return websearchToTsQuery(input, "english");
    }

    /**
     * The web-search syntax: quoted runs are phrases, a leading {@code -} negates, the bare word
     * {@code OR} joins with an alternation and everything else is joined with an AND.
     *
     * <p>Only those four things are syntax; every other character belongs to the text and is
     * given to the configuration's parser, which is what makes an e-mail address one lexeme.
     * Deleting the punctuation first ran {@code foo@bar.com} together into one meaningless word.
     * OR binds more loosely than the implicit AND, an {@code OR} with nothing before or after it
     * is an ordinary word, and each {@code -} negates once, so {@code --cat} is negated twice.
     */
    public static TsQuery websearchToTsQuery(String input, String config) {
        if (input == null) return TsQuery.emptyQuery();
        String s = input;
        // Read the input into a list of operands, remembering where an OR was written.
        List<TsQuery> operands = new ArrayList<TsQuery>();
        List<Boolean> orBefore = new ArrayList<Boolean>();
        boolean pendingOr = false;
        int i = 0;
        while (i < s.length()) {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
            if (i >= s.length()) break;
            int negations = 0;
            while (i < s.length() && s.charAt(i) == '-') {
                negations++;
                i++;
                // A negation may stand apart from what it negates.
                while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
            }
            TsQuery operand;
            if (i < s.length() && s.charAt(i) == '"') {
                int end = s.indexOf('"', i + 1);
                if (end < 0) end = s.length();
                operand = phraseToTsQuery(s.substring(i + 1, Math.min(end, s.length())), config);
                i = end + 1;
            } else {
                int begin = i;
                while (i < s.length() && !Character.isWhitespace(s.charAt(i))
                        && s.charAt(i) != '"') {
                    i++;
                }
                String word = s.substring(begin, i);
                if (word.isEmpty()) continue;
                if (negations == 0 && word.equalsIgnoreCase("or")) {
                    // An OR only joins something to something: written first or twice over, it
                    // is a word like any other.
                    if (!operands.isEmpty() && !pendingOr) {
                        pendingOr = true;
                        continue;
                    }
                }
                operand = plainToTsQuery(word, config);
            }
            if (operand == null || operand.isEmpty()) continue;
            for (int n = 0; n < negations; n++) operand = TsQuery.not(operand);
            operands.add(operand);
            orBefore.add(Boolean.valueOf(pendingOr));
            pendingOr = false;
        }
        if (operands.isEmpty()) return TsQuery.emptyQuery();
        // AND binds tighter than OR, so the ANDed runs are built first and joined after.
        List<TsQuery> alternatives = new ArrayList<TsQuery>();
        TsQuery current = operands.get(0);
        for (int k = 1; k < operands.size(); k++) {
            if (orBefore.get(k).booleanValue()) {
                alternatives.add(current);
                current = operands.get(k);
            } else {
                current = TsQuery.and(current, operands.get(k));
            }
        }
        alternatives.add(current);
        TsQuery result = alternatives.get(0);
        for (int k = 1; k < alternatives.size(); k++) {
            result = TsQuery.or(result, alternatives.get(k));
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
        return tsHeadline(document, query, options, "english");
    }

    /**
     * The document with the query's matches marked in it.
     *
     * <p>What comes back is the document's own characters: the marks go around the tokens the
     * configuration's parser found, and everything between them -- the spacing, the newlines,
     * the punctuation -- is carried through as it stands. The document used to be split on
     * whitespace and rejoined with single spaces, which rewrote text nobody asked to have
     * rewritten and put the marks around whatever punctuation happened to be stuck to the word.
     */
    public static String tsHeadline(String document, TsQuery query, String options,
                                    String config) {
        if (document == null || query == null) return "";
        String startSel = "<b>";
        String stopSel = "</b>";
        int maxWords = 35;
        int minWords = 15;
        int shortWord = 3;
        int maxFragments = 0;
        String fragmentDelimiter = " ... ";
        boolean highlightAll = false;

        if (options != null) {
            for (String opt : options.split(",")) {
                String[] kv = opt.trim().split("=", 2);
                if (kv.length != 2) continue;
                String written = kv[0].trim();
                String key = TsVector.lowerstr(written);
                String val = kv[1].trim();
                if (val.length() > 1 && val.startsWith("\"") && val.endsWith("\"")) {
                    val = val.substring(1, val.length() - 1);
                }
                switch (key) {
                    case "startsel": startSel = val; break;
                    case "stopsel": stopSel = val; break;
                    // The counts are integers, and a value that is not one is that type's own
                    // complaint rather than a failure of this reader.
                    case "maxwords": maxWords = TypeCoercion.toInteger(val); break;
                    case "minwords": minWords = TypeCoercion.toInteger(val); break;
                    case "shortword": shortWord = TypeCoercion.toInteger(val); break;
                    case "maxfragments": maxFragments = TypeCoercion.toInteger(val); break;
                    case "fragmentdelimiter": fragmentDelimiter = val; break;
                    case "highlightall":
                        highlightAll = val.equalsIgnoreCase("true") || val.equals("1");
                        break;
                    default:
                        // A name the parser does not know is a misspelling, and silently
                        // dropping it left the caller with defaults they thought they had
                        // changed. The word is quoted back as it was written, so it can be
                        // found in the caller's own source.
                        throw new MemgresException("unrecognized headline parameter: \""
                                + written + "\"", "22023");
                }
            }
        }

        // Every character of the document belongs to exactly one token, so walking the tokens
        // walks the document and each one's place in it is where the last one ended.
        List<com.memgres.engine.fts.TsParser.Token> tokens =
                com.memgres.engine.fts.TsParser.parse(document);
        int count = tokens.size();
        int[] starts = new int[count];
        int[] ends = new int[count];
        boolean[] isWord = new boolean[count];
        boolean[] selected = new boolean[count];
        boolean isSimple = "simple".equalsIgnoreCase(config);
        int at = 0;
        for (int i = 0; i < count; i++) {
            com.memgres.engine.fts.TsParser.Token token = tokens.get(i);
            starts[i] = at;
            at += token.text().length();
            ends[i] = at;
            com.memgres.engine.fts.TsParser.Dict dict =
                    com.memgres.engine.fts.TsParser.dictionaryFor(token.type());
            if (dict == com.memgres.engine.fts.TsParser.Dict.NONE) continue;
            isWord[i] = true;
            String lower = TsVector.lowerstr(token.text());
            String lexeme = isSimple || dict == com.memgres.engine.fts.TsParser.Dict.SIMPLE
                    ? lower
                    : TsVector.isStopWord(lower) ? null
                    : com.memgres.engine.fts.EnglishStemmer.stem(lower);
            selected[i] = lexeme != null && query.containsTerm(lexeme);
        }
        if (at != document.length()) {
            // The parser did not account for the whole document; marking by offset would put
            // the marks in the wrong places, so the document is returned as it stands.
            return document;
        }

        int first = 0;
        int last = count - 1;
        if (!highlightAll) {
            int[] window = headlineWindow(selected, isWord, tokens, count,
                    minWords, maxWords, shortWord, maxFragments > 0);
            first = window[0];
            last = window[1];
        }

        StringBuilder out = new StringBuilder();
        for (int i = first; i <= last; i++) {
            if (selected[i]) {
                out.append(startSel).append(document, starts[i], ends[i]).append(stopSel);
            } else if (tokens.get(i).type() == com.memgres.engine.fts.TsParser.Type.TAG) {
                // A markup tag stands for a break between words rather than for itself, so it
                // comes back as a space: without one, big<br>cat would read as a single word.
                out.append(' ');
            } else {
                out.append(document, starts[i], ends[i]);
            }
        }
        return out.toString();
    }

    /**
     * Which run of tokens the headline is taken from: the first run holding a match, grown to
     * the smallest number of words asked for and no larger than the largest.
     *
     * @param trimShort whether a short word at either edge is dropped, which is what asking for
     *     fragments does
     * @return the first and last token index, inclusive
     */
    private static int[] headlineWindow(boolean[] selected, boolean[] isWord,
                                        List<com.memgres.engine.fts.TsParser.Token> tokens,
                                        int count, int minWords, int maxWords, int shortWord,
                                        boolean trimShort) {
        int firstMatch = -1;
        int lastMatch = -1;
        for (int i = 0; i < count; i++) {
            if (!selected[i]) continue;
            if (firstMatch < 0) firstMatch = i;
            lastMatch = i;
        }
        if (firstMatch < 0) {
            // Nothing matched: the opening of the document stands for it.
            return new int[]{0, lastWordIndex(isWord, count, 0, minWords)};
        }
        int words = countWords(isWord, firstMatch, lastMatch);
        int begin = firstMatch;
        int end = lastMatch;
        // Grow forwards first, then backwards, the way PostgreSQL fills a headline out.
        while (words < minWords && end + 1 < count) {
            end++;
            if (isWord[end]) words++;
        }
        while (words < minWords && begin > 0) {
            begin--;
            if (isWord[begin]) words++;
        }
        while (words > maxWords && end > lastMatch) {
            if (isWord[end]) words--;
            end--;
        }
        while (words > maxWords && begin < firstMatch) {
            if (isWord[begin]) words--;
            begin++;
        }
        if (trimShort) {
            while (begin < firstMatch && (!isWord[begin]
                    || tokens.get(begin).text().length() <= shortWord)) {
                begin++;
            }
            while (end > lastMatch && (!isWord[end]
                    || tokens.get(end).text().length() <= shortWord)) {
                end--;
            }
        }
        // A headline taken from the middle of a document begins at a word rather than in the
        // spacing before one. One that begins where the document begins keeps what is there.
        if (begin > 0) {
            while (begin < end && !isWord[begin]) begin++;
        }
        return new int[]{begin, end};
    }

    private static int countWords(boolean[] isWord, int from, int to) {
        int words = 0;
        for (int i = from; i <= to; i++) {
            if (isWord[i]) words++;
        }
        return words;
    }

    /** The index of the token that completes the given number of words from here. */
    private static int lastWordIndex(boolean[] isWord, int count, int from, int words) {
        int seen = 0;
        int last = from;
        for (int i = from; i < count; i++) {
            if (!isWord[i]) continue;
            seen++;
            last = i;
            if (seen >= words) break;
        }
        return last;
    }

    /** ts_headline with default options. */
    public static String tsHeadline(String document, TsQuery query) {
        return tsHeadline(document, query, null);
    }

    /**
     * The order two documents stand in.
     *
     * <p>PostgreSQL orders a tsvector by how many lexemes it holds before looking at any of
     * them, and then lexeme by lexeme as bytes. Comparing the text the two print as put them in
     * the order their punctuation happened to fall in.
     */
    public static int compareVectors(TsVector a, TsVector b) {
        List<String> left = a.toArray();
        List<String> right = b.toArray();
        if (left.size() != right.size()) return left.size() < right.size() ? -1 : 1;
        for (int i = 0; i < left.size(); i++) {
            int cmp = TsVector.LEXEME_ORDER.compare(left.get(i), right.get(i));
            if (cmp != 0) return cmp < 0 ? -1 : 1;
        }
        return 0;
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

    /**
     * Whether two queries are the same query.
     *
     * <p>Everything a node carries is part of what it is: a phrase's distance says how far apart
     * the lexemes have to be, and a lexeme's prefix marker and weights say which lexemes it
     * names. Comparing the operator and the term alone made {@code 'a' <2> 'b'} equal to
     * {@code 'a' <-> 'b'}, so a rewrite for one replaced the other.
     */
    private static boolean queryEquals(TsQuery a, TsQuery b) {
        if (a.getOp() != b.getOp()) return false;
        if (a.getOp() == TsQuery.Op.TERM) {
            return Objects.equals(a.getTerm(), b.getTerm())
                    && a.isPrefix() == b.isPrefix()
                    && Objects.equals(a.getWeights(), b.getWeights());
        }
        if (a.getOp() == TsQuery.Op.PHRASE && a.getPhraseDistance() != b.getPhraseDistance()) {
            return false;
        }
        if ((a.getLeft() == null) != (b.getLeft() == null)) return false;
        if ((a.getRight() == null) != (b.getRight() == null)) return false;
        if (a.getLeft() != null && !queryEquals(a.getLeft(), b.getLeft())) return false;
        if (a.getRight() != null && !queryEquals(a.getRight(), b.getRight())) return false;
        return true;
    }

    /**
     * Whether every lexeme one query names is named by another.
     *
     * <p>This is what {@code @>} asks. It used to answer false for every pair, including a query
     * and itself.
     */
    public static boolean queryContains(TsQuery container, TsQuery contained) {
        Set<String> held = new java.util.HashSet<String>();
        collectTerms(container, held);
        Set<String> wanted = new java.util.HashSet<String>();
        collectTerms(contained, wanted);
        return held.containsAll(wanted);
    }

    private static void collectTerms(TsQuery query, Set<String> into) {
        if (query == null) return;
        if (query.getOp() == TsQuery.Op.TERM) {
            if (query.getTerm() != null && !query.getTerm().isEmpty()) into.add(query.getTerm());
            return;
        }
        collectTerms(query.getLeft(), into);
        collectTerms(query.getRight(), into);
    }

    /** ts_debug: one row per parser token, with the dictionary that handled it. */
    public static List<Object[]> tsDebug(String text) {
        return tsDebug("english", text);
    }

    /**
     * The same, read under a named configuration. A configuration is what routes a token type to
     * a dictionary, so under {@code simple} every type that reaches a dictionary at all reaches
     * the simple one, which lowercases the token and keeps the words the stemmer drops.
     */
    public static List<Object[]> tsDebug(String config, String text) {
        List<Object[]> result = new ArrayList<>();
        if (text == null) return result;
        boolean isSimple = "simple".equalsIgnoreCase(config);
        for (com.memgres.engine.fts.TsParser.Token token
                : com.memgres.engine.fts.TsParser.parse(text)) {
            com.memgres.engine.fts.TsParser.Dict dict =
                    com.memgres.engine.fts.TsParser.dictionaryFor(token.type());
            if (isSimple && dict == com.memgres.engine.fts.TsParser.Dict.STEM) {
                dict = com.memgres.engine.fts.TsParser.Dict.SIMPLE;
            }
            String dictName;
            String lexemes;
            String lower = TsVector.lowerstr(token.text());
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
    /** The dictionaries PostgreSQL ships and this engine implements. */
    private static final Set<String> BUILTIN_DICTIONARIES = new java.util.HashSet<String>(
            java.util.Arrays.asList("simple", "english_stem", "danish_stem", "dutch_stem",
                    "finnish_stem", "french_stem", "german_stem", "hungarian_stem",
                    "italian_stem", "norwegian_stem", "portuguese_stem", "romanian_stem",
                    "russian_stem", "spanish_stem", "swedish_stem", "turkish_stem"));

    /**
     * What one dictionary makes of one token.
     *
     * <p>The simple dictionary folds the case and keeps the word, whatever it is. A stemmer
     * drops the words on its stop list -- answering the empty array, which is the dictionary
     * saying "this word is not worth indexing" -- and stems the rest. The argument used to be
     * read and thrown away, so every dictionary answered as the english stemmer with no stop
     * list, and a name that names no dictionary answered as well.
     */
    public static List<String> tsLexize(String dict, String token) {
        if (token == null || dict == null) return null;
        String name = dict.toLowerCase(java.util.Locale.ROOT);
        int dot = name.lastIndexOf('.');
        if (dot >= 0) name = name.substring(dot + 1);
        if (!BUILTIN_DICTIONARIES.contains(name)) {
            throw new MemgresException(
                    "text search dictionary \"" + dict + "\" does not exist", "42704");
        }
        String lower = TsVector.lowerstr(token);
        if (name.equals("simple")) return Cols.listOf(lower);
        if (TsVector.isStopWord(lower)) return Cols.listOf();
        return Cols.listOf(TsVector.simpleStem(lower));
    }

    /** ts_parse: the parser's own token stream, as (tokid, token) pairs. */
    public static List<Object[]> tsParse(String parserName, String text) {
        // There is one parser, and a name that is not its name names none.
        if (parserName != null && !parserName.equalsIgnoreCase("default")
                && !parserName.equalsIgnoreCase("pg_catalog.default")) {
            throw new MemgresException(
                    "text search parser \"" + parserName + "\" does not exist", "42704");
        }
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
