package com.memgres.engine;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * A jsonpath, read once into the expression it is.
 *
 * <p>A path used to be walked as text: the filter was found with {@code rest.indexOf('?')}, the
 * steps were split on dots, and each step was matched against a handful of shapes. That answers
 * the shapes it was written for and silently selects nothing for everything else — a second filter,
 * a filter that is not the last step, a parenthesised operand, an {@code &&} inside a comparison,
 * a key with a dot in it. There is no way to add a case to a reader like that without adding
 * another shape it does not have.
 *
 * <p>So the path is parsed instead. The grammar is PostgreSQL's own:
 *
 * <pre>
 *   path       := [strict|lax] (expr | predicate)
 *   predicate  := predicate ('||'|'&amp;&amp;') predicate | '!' delimited
 *               | expr ('=='|'!='|'&lt;&gt;'|'&lt;'|'&lt;='|'&gt;'|'&gt;=') expr
 *               | expr 'starts with' (string|$var) | expr 'like_regex' string ['flag' string]
 *               | '(' predicate ')' 'is unknown'
 *   delimited  := 'exists' '(' expr ')' | '(' predicate ')'
 *   expr       := expr ('+'|'-') expr | expr ('*'|'/'|'%') expr | ('+'|'-') expr | accessor
 *   accessor   := primary accessor_op*
 *   primary    := '$' | '@' | '$name' | 'last' | number | string
 *               | 'true' | 'false' | 'null' | '(' expr_or_predicate ')'
 *   accessor_op:= '.' key | '.' '*' | '.' '**' ['{' level '}'] | '.' method '(' args ')'
 *               | '[' (subscript (',' subscript)* | '*') ']' | '?' '(' predicate ')'
 * </pre>
 *
 * <p>The whole path is parsed before any of it is evaluated, so a path that does not parse is a
 * syntax error whatever document it was going to be applied to — which is what PostgreSQL does,
 * jsonpath being a type whose input function runs before the query does.
 */
final class JsonPath {

    /** Whether the path opened with {@code strict}; lax is the default. */
    final boolean strict;
    /** The expression or predicate the path is, with the mode word consumed. */
    final Node body;

    private JsonPath(boolean strict, Node body) {
        this.strict = strict;
        this.body = body;
    }

    /** The item methods PostgreSQL 18 knows, and how many arguments each takes. */
    private static final Set<String> METHODS = new HashSet<String>(Arrays.asList(
            "type", "size", "abs", "floor", "ceiling", "double", "keyvalue", "datetime",
            "bigint", "boolean", "date", "decimal", "integer", "number", "string",
            "time", "time_tz", "timestamp", "timestamp_tz"));

    /** The methods that take arguments: a template for datetime, a precision and scale otherwise. */
    private static final Set<String> METHODS_WITH_ARGS = new HashSet<String>(Arrays.asList(
            "datetime", "date", "decimal", "time", "time_tz", "timestamp", "timestamp_tz"));

    // ----------------------------------------------------------------- the tree

    abstract static class Node {
        /**
         * Whether this node answers true, false or unknown rather than a sequence of items. The
         * two are separate languages that meet only at the top of the path and inside a filter,
         * so telling them apart is what stops {@code $.a && $.b} from parsing.
         */
        boolean isPredicate() {
            return false;
        }
    }

    /** The document the path was applied to, written {@code $}. */
    static final class Root extends Node {
    }

    /** The item a filter is deciding about, written {@code @}. */
    static final class Current extends Node {
    }

    /** The last subscript of the array being subscripted, written {@code last}. */
    static final class Last extends Node {
    }

    /** A {@code $name} reference to the vars object the call was given. */
    static final class Variable extends Node {
        final String name;

        Variable(String name) {
            this.name = name;
        }
    }

    static final class Literal extends Node {
        final JsonValue value;

        Literal(JsonValue value) {
            this.value = value;
        }
    }

    /** {@code .key} and {@code ."key"} — the same node, the quotes being only how it was written. */
    static final class Member extends Node {
        final Node input;
        final String key;

        Member(Node input, String key) {
            this.input = input;
            this.key = key;
        }
    }

    /** {@code .*} — every member of an object. */
    static final class MemberAll extends Node {
        final Node input;

        MemberAll(Node input) {
            this.input = input;
        }
    }

    /**
     * {@code .**} — the item and everything under it. The levels wanted are {@code from} to
     * {@code to}, where {@code to} is {@link Integer#MAX_VALUE} for an open {@code last}.
     */
    static final class AnyLevel extends Node {
        final Node input;
        final int from;
        final int to;

        AnyLevel(Node input, int from, int to) {
            this.input = input;
            this.from = from;
            this.to = to;
        }
    }

    /** {@code [*]} — every element of an array. */
    static final class IndexAll extends Node {
        final Node input;

        IndexAll(Node input) {
            this.input = input;
        }
    }

    /**
     * {@code [a, b to c]} — the subscripts wanted, each either a single position or a range. A
     * subscript is an expression, so {@code $[last-1]} and {@code $[$i]} are subscripts too.
     */
    static final class Index extends Node {
        final Node input;
        final List<Node> from;
        /** The end of each range, or null where that subscript is a single position. */
        final List<Node> to;

        Index(Node input, List<Node> from, List<Node> to) {
            this.input = input;
            this.from = from;
            this.to = to;
        }
    }

    /** {@code .name()} — an item method, with the arguments it was written with. */
    static final class Method extends Node {
        final Node input;
        final String name;
        final List<Node> args;

        Method(Node input, String name, List<Node> args) {
            this.input = input;
            this.name = name;
            this.args = args;
        }
    }

    /** {@code ?(...)} — the items of the input the predicate is true of. */
    static final class Filter extends Node {
        final Node input;
        final Node predicate;

        Filter(Node input, Node predicate) {
            this.input = input;
            this.predicate = predicate;
        }
    }

    static final class Binary extends Node {
        final char op;
        final Node left;
        final Node right;

        Binary(char op, Node left, Node right) {
            this.op = op;
            this.left = left;
            this.right = right;
        }
    }

    static final class Unary extends Node {
        final char op;
        final Node operand;

        Unary(char op, Node operand) {
            this.op = op;
            this.operand = operand;
        }
    }

    static final class Logic extends Node {
        final boolean and;
        final Node left;
        final Node right;

        Logic(boolean and, Node left, Node right) {
            this.and = and;
            this.left = left;
            this.right = right;
        }

        @Override
        boolean isPredicate() {
            return true;
        }
    }

    static final class Not extends Node {
        final Node arg;

        Not(Node arg) {
            this.arg = arg;
        }

        @Override
        boolean isPredicate() {
            return true;
        }
    }

    static final class IsUnknown extends Node {
        final Node arg;

        IsUnknown(Node arg) {
            this.arg = arg;
        }

        @Override
        boolean isPredicate() {
            return true;
        }
    }

    static final class Compare extends Node {
        final String op;
        final Node left;
        final Node right;

        Compare(String op, Node left, Node right) {
            this.op = op;
            this.left = left;
            this.right = right;
        }

        @Override
        boolean isPredicate() {
            return true;
        }
    }

    static final class Exists extends Node {
        final Node arg;

        Exists(Node arg) {
            this.arg = arg;
        }

        @Override
        boolean isPredicate() {
            return true;
        }
    }

    static final class StartsWith extends Node {
        final Node left;
        final Node right;

        StartsWith(Node left, Node right) {
            this.left = left;
            this.right = right;
        }

        @Override
        boolean isPredicate() {
            return true;
        }
    }

    static final class LikeRegex extends Node {
        final Node left;
        final Pattern pattern;
        /** The pattern as written, which the compiled form cannot be read back out of. */
        final String regex;
        /** The flag letters that were set, in the order PostgreSQL lists them. */
        final String flags;

        LikeRegex(Node left, Pattern pattern, String regex, String flags) {
            this.left = left;
            this.pattern = pattern;
            this.regex = regex;
            this.flags = flags;
        }

        @Override
        boolean isPredicate() {
            return true;
        }
    }

    // --------------------------------------------------------------- the parser

    static JsonPath parse(String text) {
        return new Parser(text).path();
    }

    // -------------------------------------------------------------- writing it back

    /**
     * The path as PostgreSQL prints one, which is not the text it was written as.
     *
     * <p>Every key is quoted, {@code lax} is dropped because it is the default, {@code <>} becomes
     * {@code !=}, a number is printed as a numeric is, and an operator expression is parenthesised
     * wherever the parentheses are not implied by precedence -- so the outermost one always is,
     * having no operator around it to be implied by.
     */
    String text() {
        StringBuilder sb = new StringBuilder();
        if (strict) sb.append("strict ");
        print(sb, body, true);
        return sb.toString();
    }

    /**
     * How tightly an operator binds. An operand is parenthesised when it binds no more tightly
     * than the operator above it, which is the only case where dropping the parentheses would
     * read as something else. Everything that is not an operator binds tightest of all.
     */
    private static int priority(Node node) {
        if (node instanceof Logic) return ((Logic) node).and ? 1 : 0;
        if (node instanceof Compare || node instanceof StartsWith) return 2;
        if (node instanceof Binary) {
            char op = ((Binary) node).op;
            return op == '+' || op == '-' ? 3 : 4;
        }
        if (node instanceof Unary) return 5;
        return 6;
    }

    /** Prints an operand of {@code parent}, bracketing it where precedence would not imply it. */
    private static void operand(StringBuilder sb, Node node, Node parent) {
        print(sb, node, priority(node) <= priority(parent));
    }

    private static void print(StringBuilder sb, Node node, boolean brackets) {
        if (node instanceof Root) {
            sb.append('$');
        } else if (node instanceof Current) {
            sb.append('@');
        } else if (node instanceof Last) {
            sb.append("last");
        } else if (node instanceof Variable) {
            sb.append('$').append(JsonWriter.quote(((Variable) node).name));
        } else if (node instanceof Literal) {
            JsonValue value = ((Literal) node).value;
            // A number is printed as the numeric it was read into, not as the path spelled it,
            // so an exponent is multiplied out and trailing zeroes are kept.
            sb.append(value.kind() == JsonValue.NUMBER
                    ? value.asNumber().toPlainString() : JsonWriter.jsonb(value));
        } else if (node instanceof Member) {
            Member m = (Member) node;
            print(sb, m.input, false);
            sb.append('.').append(JsonWriter.quote(m.key));
        } else if (node instanceof MemberAll) {
            print(sb, ((MemberAll) node).input, false);
            sb.append(".*");
        } else if (node instanceof AnyLevel) {
            printAnyLevel(sb, (AnyLevel) node);
        } else if (node instanceof IndexAll) {
            print(sb, ((IndexAll) node).input, false);
            sb.append("[*]");
        } else if (node instanceof Index) {
            printIndex(sb, (Index) node);
        } else if (node instanceof Method) {
            printMethod(sb, (Method) node);
        } else if (node instanceof Filter) {
            Filter f = (Filter) node;
            print(sb, f.input, false);
            sb.append("?(");
            print(sb, f.predicate, false);
            sb.append(')');
        } else if (node instanceof Not) {
            sb.append("!(");
            print(sb, ((Not) node).arg, false);
            sb.append(')');
        } else if (node instanceof IsUnknown) {
            sb.append('(');
            print(sb, ((IsUnknown) node).arg, false);
            sb.append(") is unknown");
        } else if (node instanceof Exists) {
            sb.append("exists (");
            print(sb, ((Exists) node).arg, false);
            sb.append(')');
        } else if (node instanceof Unary) {
            Unary u = (Unary) node;
            if (brackets) sb.append('(');
            sb.append(u.op);
            operand(sb, u.operand, u);
            if (brackets) sb.append(')');
        } else {
            printInfix(sb, node, brackets);
        }
    }

    /** The operators written between their operands, which share one shape and one bracketing. */
    private static void printInfix(StringBuilder sb, Node node, boolean brackets) {
        Node left;
        Node right;
        String op;
        if (node instanceof Binary) {
            left = ((Binary) node).left;
            right = ((Binary) node).right;
            op = String.valueOf(((Binary) node).op);
        } else if (node instanceof Logic) {
            left = ((Logic) node).left;
            right = ((Logic) node).right;
            op = ((Logic) node).and ? "&&" : "||";
        } else if (node instanceof Compare) {
            left = ((Compare) node).left;
            right = ((Compare) node).right;
            op = ((Compare) node).op;
        } else if (node instanceof StartsWith) {
            left = ((StartsWith) node).left;
            right = ((StartsWith) node).right;
            op = "starts with";
        } else {
            LikeRegex r = (LikeRegex) node;
            if (brackets) sb.append('(');
            print(sb, r.left, false);
            sb.append(" like_regex ").append(JsonWriter.quote(r.regex));
            if (!r.flags.isEmpty()) sb.append(" flag ").append(JsonWriter.quote(r.flags));
            if (brackets) sb.append(')');
            return;
        }
        if (brackets) sb.append('(');
        operand(sb, left, node);
        sb.append(' ').append(op).append(' ');
        operand(sb, right, node);
        if (brackets) sb.append(')');
    }

    private static void printAnyLevel(StringBuilder sb, AnyLevel a) {
        print(sb, a.input, false);
        sb.append(".**");
        if (a.from == 0 && a.to == Integer.MAX_VALUE) return;
        sb.append('{').append(a.from == Integer.MAX_VALUE ? "last" : String.valueOf(a.from));
        if (a.to != a.from) {
            sb.append(" to ").append(a.to == Integer.MAX_VALUE ? "last" : String.valueOf(a.to));
        }
        sb.append('}');
    }

    private static void printIndex(StringBuilder sb, Index index) {
        print(sb, index.input, false);
        sb.append('[');
        for (int i = 0; i < index.from.size(); i++) {
            if (i > 0) sb.append(',');
            print(sb, index.from.get(i), false);
            if (index.to.get(i) != null) {
                sb.append(" to ");
                print(sb, index.to.get(i), false);
            }
        }
        sb.append(']');
    }

    private static void printMethod(StringBuilder sb, Method method) {
        print(sb, method.input, false);
        sb.append('.').append(method.name).append('(');
        for (int i = 0; i < method.args.size(); i++) {
            if (i > 0) sb.append(',');
            print(sb, method.args.get(i), false);
        }
        sb.append(')');
    }

    private static final class Parser {
        private final String src;
        private int pos;
        /**
         * Whether the expression just parsed was a parenthesised one. {@code is unknown} attaches
         * only to that, so the flag says whether the operand is the shape the grammar wants.
         */
        private boolean lastWasDelimited;
        /**
         * How many subscripts enclose the position being parsed. {@code last} is the subscript one
         * past the end of the array being subscripted, so outside a subscript there is no array
         * for it to be the end of and writing it is a mistake in the path rather than something
         * that could evaluate to nothing.
         */
        private int subscriptDepth;

        Parser(String src) {
            this.src = src;
        }

        JsonPath path() {
            if (src.trim().isEmpty()) {
                throw new MemgresException("invalid input syntax for type jsonpath: \"" + src + "\"",
                        "22P02");
            }
            boolean strict = false;
            skipSpace();
            if (word("strict")) {
                strict = true;
            } else if (word("lax")) {
                strict = false;
            }
            Node body = parseOr();
            skipSpace();
            if (pos < src.length()) throw errorAt();
            return new JsonPath(strict, body);
        }

        // -- predicates, loosest binding first

        private Node parseOr() {
            Node left = parseAnd();
            while (true) {
                skipSpace();
                // The operator is what a non-predicate operand is refused at: it is the word that
                // made the operand wrong, the operand having been fine on its own.
                int op = pos;
                if (!symbol("||")) return left;
                left = new Logic(false, requirePredicate(left, op), requirePredicate(parseAnd(), op));
            }
        }

        private Node parseAnd() {
            Node left = parseNot();
            while (true) {
                skipSpace();
                int op = pos;
                if (!symbol("&&")) return left;
                left = new Logic(true, requirePredicate(left, op), requirePredicate(parseNot(), op));
            }
        }

        private Node parseNot() {
            skipSpace();
            if (peek() == '!' && peek(1) != '=') {
                pos++;
                return new Not(requirePredicate(parseNot()));
            }
            return parseComparison();
        }

        /**
         * A comparison, or the expression itself where there is no operator after it. The
         * comparison operators do not chain, so this reads at most one of them.
         */
        private Node parseComparison() {
            Node left = parseAdditive();
            boolean delimited = lastWasDelimited;
            skipSpace();
            String op = comparisonOp();
            if (op != null) {
                return new Compare(op, requireExpression(left), requireExpression(parseAdditive()));
            }
            if (word("starts")) {
                skipSpace();
                if (!word("with")) throw errorAt();
                return new StartsWith(requireExpression(left), parseStartsWithOperand());
            }
            if (word("like_regex")) {
                return parseLikeRegex(requireExpression(left));
            }
            if (delimited && left.isPredicate() && word("is")) {
                skipSpace();
                if (!word("unknown")) throw errorAt();
                return new IsUnknown(left);
            }
            return left;
        }

        private String comparisonOp() {
            if (symbol("==")) return "==";
            if (symbol("!=")) return "!=";
            if (symbol("<>")) return "!=";
            if (symbol("<=")) return "<=";
            if (symbol(">=")) return ">=";
            if (peek() == '<') { pos++; return "<"; }
            if (peek() == '>') { pos++; return ">"; }
            return null;
        }

        /** {@code starts with} takes a string or a variable, and nothing else. */
        private Node parseStartsWithOperand() {
            skipSpace();
            if (peek() == '"') return new Literal(JsonValue.string(readQuoted()));
            if (peek() == '$' && isNameStart(peek(1))) {
                pos++;
                return new Variable(readName());
            }
            throw errorAt();
        }

        private Node parseLikeRegex(Node left) {
            skipSpace();
            if (peek() != '"') throw errorAt();
            String regex = readQuoted();
            int options = 0;
            boolean literal = false;
            skipSpace();
            if (word("flag")) {
                skipSpace();
                if (peek() != '"') throw errorAt();
                String flags = readQuoted();
                for (int i = 0; i < flags.length(); i++) {
                    char f = flags.charAt(i);
                    switch (f) {
                        case 'i': options |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE; break;
                        case 's': options |= Pattern.DOTALL; break;
                        case 'm': options |= Pattern.MULTILINE; break;
                        case 'x': options |= Pattern.COMMENTS; break;
                        case 'q': literal = true; break;
                        default:
                            MemgresException e = new MemgresException(
                                    "invalid input syntax for type jsonpath", "42601");
                            e.setDetail("Unrecognized flag character \"" + f
                                    + "\" in LIKE_REGEX predicate.");
                            throw e;
                    }
                }
            }
            if (literal) options |= Pattern.LITERAL;
            // The flags are held as the letters that were set rather than as written: PostgreSQL
            // keeps a bitmask, so a repeated or reordered flag string comes back out in one order.
            StringBuilder set = new StringBuilder();
            if ((options & Pattern.CASE_INSENSITIVE) != 0) set.append('i');
            if ((options & Pattern.DOTALL) != 0) set.append('s');
            if ((options & Pattern.MULTILINE) != 0) set.append('m');
            if ((options & Pattern.COMMENTS) != 0) set.append('x');
            if (literal) set.append('q');
            try {
                return new LikeRegex(left, Pattern.compile(regex, options), regex, set.toString());
            } catch (PatternSyntaxException e) {
                throw new MemgresException("invalid regular expression: " + e.getDescription(),
                        "2201B");
            }
        }

        // -- expressions

        private Node parseAdditive() {
            Node left = parseMultiplicative();
            while (true) {
                skipSpace();
                char c = peek();
                if (c != '+' && c != '-') return left;
                pos++;
                left = new Binary(c, requireExpression(left), requireExpression(parseMultiplicative()));
                lastWasDelimited = false;
            }
        }

        private Node parseMultiplicative() {
            Node left = parseUnary();
            while (true) {
                skipSpace();
                char c = peek();
                if (c != '*' && c != '/' && c != '%') return left;
                // A comment opens with the same character a multiplication does
                if (c == '/' && peek(1) == '*') return left;
                pos++;
                left = new Binary(c, requireExpression(left), requireExpression(parseUnary()));
                lastWasDelimited = false;
            }
        }

        private Node parseUnary() {
            skipSpace();
            char c = peek();
            if (c == '+' || c == '-') {
                pos++;
                Node operand = requireExpression(parseUnary());
                lastWasDelimited = false;
                return new Unary(c, operand);
            }
            return parseAccessors(parsePrimary());
        }

        private Node parsePrimary() {
            skipSpace();
            lastWasDelimited = false;
            char c = peek();
            if (c == '\0') throw endOfInput();
            if (c == '$') {
                pos++;
                if (isNameStart(peek())) return new Variable(readName());
                if (peek() == '"') return new Variable(readQuoted());
                return new Root();
            }
            if (c == '@') {
                pos++;
                return new Current();
            }
            if (c == '"') return new Literal(JsonValue.string(readQuoted()));
            if (isDigit(c) || (c == '.' && isDigit(peek(1)))) return new Literal(readNumber());
            if (c == '(') {
                pos++;
                Node inner = parseOr();
                skipSpace();
                if (peek() != ')') throw errorAt();
                pos++;
                lastWasDelimited = true;
                return inner;
            }
            if (isNameStart(c)) {
                int start = pos;
                String name = readName();
                if (name.equalsIgnoreCase("true")) return new Literal(JsonValue.TRUE);
                if (name.equalsIgnoreCase("false")) return new Literal(JsonValue.FALSE);
                if (name.equalsIgnoreCase("null")) return new Literal(JsonValue.JSON_NULL);
                if (name.equalsIgnoreCase("last")) {
                    if (subscriptDepth == 0) {
                        throw new MemgresException(
                                "LAST is allowed only in array subscripts", "42601");
                    }
                    return new Last();
                }
                if (name.equalsIgnoreCase("exists")) {
                    skipSpace();
                    if (peek() != '(') throw errorAt();
                    pos++;
                    Node arg = parseOr();
                    skipSpace();
                    if (peek() != ')') throw errorAt();
                    pos++;
                    lastWasDelimited = true;
                    return new Exists(arg);
                }
                pos = start;
                throw errorAt();
            }
            throw errorAt();
        }

        private Node parseAccessors(Node input) {
            while (true) {
                skipSpace();
                char c = peek();
                if (c == '.') {
                    input = parseDotAccessor(input);
                } else if (c == '[') {
                    input = parseSubscript(input);
                } else if (c == '?') {
                    pos++;
                    skipSpace();
                    if (peek() != '(') throw errorAt();
                    pos++;
                    Node predicate = requirePredicate(parseOr());
                    skipSpace();
                    if (peek() != ')') throw errorAt();
                    pos++;
                    input = new Filter(input, predicate);
                } else {
                    // Nothing was consumed, so whether the primary was parenthesised still
                    // stands: it is the only thing "is unknown" may be written after.
                    return input;
                }
                lastWasDelimited = false;
            }
        }

        private Node parseDotAccessor(Node input) {
            pos++;                                       // the dot itself
            skipSpace();
            char c = peek();
            if (c == '*') {
                if (peek(1) == '*') {
                    pos += 2;
                    return parseAnyLevel(input);
                }
                pos++;
                return new MemberAll(input);
            }
            if (c == '"') return new Member(input, readQuoted());
            if (!isNameStart(c)) throw c == '\0' ? endOfInput() : errorAt();
            String name = readName();
            int save = pos;
            skipSpace();
            if (peek() != '(') {
                pos = save;
                return new Member(input, name);
            }
            // A name with a call after it has to be one of the methods; PostgreSQL's lexer knows
            // the method names, so an unknown one is a syntax error at the parenthesis rather than
            // a member read that happens to be followed by something.
            String method = name.toLowerCase();
            if (!METHODS.contains(method)) throw errorAt();
            pos++;
            List<Node> args = new ArrayList<Node>();
            skipSpace();
            if (peek() != ')') {
                if (!METHODS_WITH_ARGS.contains(method)) throw errorAt();
                while (true) {
                    args.add(parseAdditive());
                    skipSpace();
                    if (peek() == ',') { pos++; continue; }
                    break;
                }
            }
            skipSpace();
            if (peek() != ')') throw errorAt();
            pos++;
            return new Method(input, method, args);
        }

        /** The {@code {n}}, {@code {n to m}} or {@code {n to last}} after a {@code .**}. */
        private Node parseAnyLevel(Node input) {
            int save = pos;
            skipSpace();
            if (peek() != '{') {
                pos = save;
                return new AnyLevel(input, 0, Integer.MAX_VALUE);
            }
            pos++;
            skipSpace();
            int from = word("last") ? Integer.MAX_VALUE : (int) readInteger();
            int to = from;
            skipSpace();
            if (word("to")) {
                skipSpace();
                to = word("last") ? Integer.MAX_VALUE : (int) readInteger();
            }
            skipSpace();
            if (peek() != '}') throw errorAt();
            pos++;
            return new AnyLevel(input, from, to);
        }

        private Node parseSubscript(Node input) {
            pos++;                                       // the bracket itself
            skipSpace();
            if (peek() == '*') {
                pos++;
                skipSpace();
                if (peek() != ']') throw errorAt();
                pos++;
                return new IndexAll(input);
            }
            List<Node> from = new ArrayList<Node>();
            List<Node> to = new ArrayList<Node>();
            subscriptDepth++;
            while (true) {
                Node start = parseAdditive();
                Node end = null;
                skipSpace();
                if (word("to")) end = parseAdditive();
                from.add(start);
                to.add(end);
                skipSpace();
                if (peek() == ',') { pos++; continue; }
                break;
            }
            subscriptDepth--;
            skipSpace();
            if (peek() != ']') throw peek() == '\0' ? endOfInput() : errorAt();
            pos++;
            return new Index(input, from, to);
        }

        // -- the characters themselves

        private void skipSpace() {
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (Character.isWhitespace(c)) {
                    pos++;
                } else if (c == '/' && pos + 1 < src.length() && src.charAt(pos + 1) == '*') {
                    int end = src.indexOf("*/", pos + 2);
                    if (end < 0) throw endOfInput();
                    pos = end + 2;
                } else {
                    return;
                }
            }
        }

        private char peek() {
            return pos < src.length() ? src.charAt(pos) : '\0';
        }

        private char peek(int ahead) {
            return pos + ahead < src.length() ? src.charAt(pos + ahead) : '\0';
        }

        /** Consumes a fixed run of punctuation, or leaves the position where it was. */
        private boolean symbol(String s) {
            if (!src.startsWith(s, pos)) return false;
            pos += s.length();
            return true;
        }

        /**
         * Consumes a keyword, or leaves the position where it was. A keyword only matches a whole
         * word, so the {@code to} in a path step named {@code total} is not a range.
         */
        private boolean word(String w) {
            skipSpace();
            if (pos + w.length() > src.length()) return false;
            if (!src.regionMatches(true, pos, w, 0, w.length())) return false;
            char after = peek(w.length());
            if (isNameChar(after)) return false;
            pos += w.length();
            return true;
        }

        private static boolean isNameStart(char c) {
            return Character.isLetter(c) || c == '_';
        }

        private static boolean isNameChar(char c) {
            return Character.isLetterOrDigit(c) || c == '_' || c == '$';
        }

        private static boolean isDigit(char c) {
            return c >= '0' && c <= '9';
        }

        private String readName() {
            int start = pos;
            while (pos < src.length() && isNameChar(src.charAt(pos))) pos++;
            if (pos == start) throw errorAt();
            return src.substring(start, pos);
        }

        /** A double-quoted string, read with the escapes JSON writes. */
        private String readQuoted() {
            pos++;                                       // the opening quote
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (pos >= src.length()) throw endOfInput();
                char c = src.charAt(pos++);
                if (c == '"') return sb.toString();
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                if (pos >= src.length()) throw endOfInput();
                char esc = src.charAt(pos++);
                switch (esc) {
                    case 'n': sb.append('\n'); break;
                    case 't': sb.append('\t'); break;
                    case 'r': sb.append('\r'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case '/': sb.append('/'); break;
                    case '\\': sb.append('\\'); break;
                    case '"': sb.append('"'); break;
                    case 'u':
                        if (pos + 4 > src.length()) throw endOfInput();
                        sb.append((char) Integer.parseInt(src.substring(pos, pos + 4), 16));
                        pos += 4;
                        break;
                    default:
                        throw errorAt();
                }
            }
        }

        /**
         * A number as jsonpath writes one, which is not quite as JSON does: a decimal point may
         * open or close the digits, so both {@code .5} and {@code 1.} are numbers here.
         */
        private JsonValue readNumber() {
            int start = pos;
            while (isDigit(peek())) pos++;
            if (peek() == '.') {
                pos++;
                while (isDigit(peek())) pos++;
            }
            if (peek() == 'e' || peek() == 'E') {
                int save = pos;
                pos++;
                if (peek() == '+' || peek() == '-') pos++;
                if (!isDigit(peek())) {
                    pos = save;
                } else {
                    while (isDigit(peek())) pos++;
                }
            }
            String text = src.substring(start, pos);
            try {
                return JsonValue.number(new BigDecimal(text));
            } catch (NumberFormatException e) {
                pos = start;
                throw errorAt();
            }
        }

        private long readInteger() {
            int start = pos;
            while (isDigit(peek())) pos++;
            if (pos == start) throw errorAt();
            return Long.parseLong(src.substring(start, pos));
        }

        // -- refusing what does not parse

        private Node requirePredicate(Node node) {
            if (!node.isPredicate()) throw errorAt();
            return node;
        }

        /** As {@link #requirePredicate(Node)}, but complaining at a place already gone past. */
        private Node requirePredicate(Node node, int at) {
            if (node.isPredicate()) return node;
            pos = at;
            throw errorAt();
        }

        private Node requireExpression(Node node) {
            if (node.isPredicate()) throw errorAt();
            return node;
        }

        private MemgresException errorAt() {
            skipSpaceQuietly();
            if (pos >= src.length()) return endOfInput();
            // PostgreSQL names the token the parser stopped at -- except for a word, whose
            // characters its scanner has already consumed into a buffer of their own by the time
            // the parser objects, leaving it with nothing to name and only the end to point at.
            if (isNameStart(src.charAt(pos))) return endOfInput();
            return new MemgresException("syntax error at or near \"" + punctuation()
                    + "\" of jsonpath input", "42601");
        }

        /** The punctuation token at the position, which the two-character operators are one of. */
        private String punctuation() {
            for (String s : new String[] {"&&", "||", "==", "!=", "<>", "<=", ">=", "**"}) {
                if (src.startsWith(s, pos)) return s;
            }
            return String.valueOf(src.charAt(pos));
        }

        /** The whitespace skip an error message needs, which must not raise an error of its own. */
        private void skipSpaceQuietly() {
            while (pos < src.length() && Character.isWhitespace(src.charAt(pos))) pos++;
        }

        private static MemgresException endOfInput() {
            return new MemgresException("syntax error at end of jsonpath input", "42601");
        }
    }
}
