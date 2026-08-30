package com.memgres.engine;

import com.memgres.engine.util.Cols;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * PostgreSQL-compatible geometric type operations.
 * Handles parsing, formatting, arithmetic, measurement, predicates, and conversions
 * for POINT, LINE, LSEG, BOX, PATH, POLYGON, and CIRCLE types.
 */
public final class GeometricOperations {
    private GeometricOperations() {}

    /**
     * How near two coordinates have to be before PostgreSQL calls them the same.
     *
     * <p>This is its own {@code EPSILON}, and every predicate over these types is written in
     * terms of it rather than over exact equality -- two shapes are as close as the arithmetic
     * that built them can tell. A tighter tolerance is not a stricter version of the same
     * answers: it is a different set of answers, and it made points PostgreSQL calls equal
     * unequal here, lines it calls vertical sloped, and shapes it calls touching apart.
     */
    private static final double EPSILON = 1.0E-06;

    // ========================================================================
    // Inner record types
    // ========================================================================

        public static final class PgPoint {
        public final double x;
        public final double y;

        public PgPoint(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double x() { return x; }
        public double y() { return y; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgPoint that = (PgPoint) o;
            return Double.compare(x, that.x) == 0
                && Double.compare(y, that.y) == 0;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(x, y);
        }

        @Override
        public String toString() {
            return "PgPoint[x=" + x + ", " + "y=" + y + "]";
        }
    }
        public static final class PgLine {
        public final double a;
        public final double b;
        public final double c;

        public PgLine(double a, double b, double c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        public double a() { return a; }
        public double b() { return b; }
        public double c() { return c; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgLine that = (PgLine) o;
            return Double.compare(a, that.a) == 0
                && Double.compare(b, that.b) == 0
                && Double.compare(c, that.c) == 0;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(a, b, c);
        }

        @Override
        public String toString() {
            return "PgLine[a=" + a + ", " + "b=" + b + ", " + "c=" + c + "]";
        }
    }
        public static final class PgLseg {
        public final PgPoint p1;
        public final PgPoint p2;

        public PgLseg(PgPoint p1, PgPoint p2) {
            this.p1 = p1;
            this.p2 = p2;
        }

        public PgPoint p1() { return p1; }
        public PgPoint p2() { return p2; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgLseg that = (PgLseg) o;
            return java.util.Objects.equals(p1, that.p1)
                && java.util.Objects.equals(p2, that.p2);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(p1, p2);
        }

        @Override
        public String toString() {
            return "PgLseg[p1=" + p1 + ", " + "p2=" + p2 + "]";
        }
    }
        public static final class PgBox {
        public final PgPoint high;
        public final PgPoint low;

        public PgBox(PgPoint high, PgPoint low) {
            this.high = high;
            this.low = low;
        }

        public PgPoint high() { return high; }
        public PgPoint low() { return low; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgBox that = (PgBox) o;
            return java.util.Objects.equals(high, that.high)
                && java.util.Objects.equals(low, that.low);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(high, low);
        }

        @Override
        public String toString() {
            return "PgBox[high=" + high + ", " + "low=" + low + "]";
        }
    }
        public static final class PgPath {
        public final List<PgPoint> points;
        public final boolean closed;

        public PgPath(List<PgPoint> points, boolean closed) {
            this.points = points;
            this.closed = closed;
        }

        public List<PgPoint> points() { return points; }
        public boolean closed() { return closed; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgPath that = (PgPath) o;
            return java.util.Objects.equals(points, that.points)
                && closed == that.closed;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(points, closed);
        }

        @Override
        public String toString() {
            return "PgPath[points=" + points + ", " + "closed=" + closed + "]";
        }
    }
        public static final class PgPolygon {
        public final List<PgPoint> points;

        public PgPolygon(List<PgPoint> points) {
            this.points = points;
        }

        public List<PgPoint> points() { return points; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgPolygon that = (PgPolygon) o;
            return java.util.Objects.equals(points, that.points);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(points);
        }

        @Override
        public String toString() {
            return "PgPolygon[points=" + points + "]";
        }
    }
        public static final class PgCircle {
        public final PgPoint center;
        public final double radius;

        public PgCircle(PgPoint center, double radius) {
            this.center = center;
            this.radius = radius;
        }

        public PgPoint center() { return center; }
        public double radius() { return radius; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PgCircle that = (PgCircle) o;
            return java.util.Objects.equals(center, that.center)
                && Double.compare(radius, that.radius) == 0;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(center, radius);
        }

        @Override
        public String toString() {
            return "PgCircle[center=" + center + ", " + "radius=" + radius + "]";
        }
    }

    // ========================================================================
    // Parsing helpers
    // ========================================================================

    private static String strip(String s, char open, char close) {
        s = s.trim();
        if (s.length() >= 2 && s.charAt(0) == open && s.charAt(s.length() - 1) == close) {
            return s.substring(1, s.length() - 1).trim();
        }
        return s;
    }

    /**
     * Validate that parentheses and brackets are balanced in a geometry literal.
     */
    private static void validateBalancedParens(String s, String typeName) {
        int parenDepth = 0;
        int bracketDepth = 0;
        int angleDepth = 0;
        for (char ch : s.toCharArray()) {
            switch (ch) {
                case '(':
                    parenDepth++;
                    break;
                case ')': {
                    parenDepth--; if (parenDepth < 0) throw new MemgresException("invalid input syntax for type " + typeName + ": \"" + s + "\"", "22P02"); 
                    break;
                }
                case '[':
                    bracketDepth++;
                    break;
                case ']': {
                    bracketDepth--; if (bracketDepth < 0) throw new MemgresException("invalid input syntax for type " + typeName + ": \"" + s + "\"", "22P02"); 
                    break;
                }
                case '<':
                    angleDepth++;
                    break;
                case '>': {
                    angleDepth--; if (angleDepth < 0) throw new MemgresException("invalid input syntax for type " + typeName + ": \"" + s + "\"", "22P02"); 
                    break;
                }
            }
        }
        if (parenDepth != 0 || bracketDepth != 0 || angleDepth != 0) {
            throw new MemgresException("invalid input syntax for type " + typeName + ": \"" + s + "\"", "22P02");
        }
    }

    /**
     * Extract all doubles from a string. Used as fallback for lenient parsing.
     * Handles NaN, Infinity, -Infinity, scientific notation, and negative zero.
     */
    private static List<Double> extractDoubles(String s) {
        List<Double> result = new ArrayList<>();
        // A number may open with its decimal point, and may be written with a leading plus:
        // PostgreSQL reads "+.5" and ".5" as one half, where requiring a leading digit read the
        // dot as a separator and the value as five.
        Matcher m = Pattern.compile(
                "[-+]?(?:NaN|Infinity|(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][+-]?\\d+)?)").matcher(s);
        while (m.find()) {
            result.add(Double.parseDouble(m.group()));
        }
        return result;
    }

    /** The geometric types PostgreSQL lets a subscript reach into. */
    private static final java.util.Set<String> SUBSCRIPTABLE = new java.util.HashSet<String>(
            java.util.Arrays.asList("point", "box", "lseg", "line", "path", "polygon"));

    /** True when a subscript on a value of this type selects one of its parts. */
    public static boolean isSubscriptableType(String typeName) {
        return typeName != null && SUBSCRIPTABLE.contains(typeName);
    }

    /**
     * One part of a geometric value, counted from zero. A point is its two coordinates; everything
     * built out of points is those points. Anything past the end is simply not there.
     */
    public static Object partAt(String typeName, String text, int index) {
        if (index < 0) return null;
        if ("point".equals(typeName)) {
            PgPoint p = parsePoint(text);
            if (index == 0) return p.x();
            if (index == 1) return p.y();
            return null;
        }
        List<PgPoint> points = parsePointList(text);
        return index < points.size() ? formatPoint(points.get(index)) : null;
    }

    /**
     * Parse a list of points from a string like "(x1,y1),(x2,y2),..."
     */
    private static List<PgPoint> parsePointList(String s) {
        List<PgPoint> points = new ArrayList<>();
        Matcher m = Pattern.compile("\\(\\s*(-?(?:NaN|Infinity|[\\d.eE+-]+))\\s*,\\s*(-?(?:NaN|Infinity|[\\d.eE+-]+))\\s*\\)").matcher(s);
        while (m.find()) {
            points.add(new PgPoint(Double.parseDouble(m.group(1)), Double.parseDouble(m.group(2))));
        }
        if (points.isEmpty()) {
            // Fallback: try extracting pairs of doubles
            List<Double> nums = extractDoubles(s);
            for (int i = 0; i + 1 < nums.size(); i += 2) {
                points.add(new PgPoint(nums.get(i), nums.get(i + 1)));
            }
        }
        return points;
    }

    // ========================================================================
    // Parsing methods
    // ========================================================================

    /**
     * A reader for the geometric literal forms.
     *
     * <p>These used to be scraped with a regular expression that found the components it needed
     * and stopped, so a literal naming more than the type holds was read as its first few parts
     * and the rest thrown away -- {@code '(1,2),(3,4),(5,6)'::box} became a box, and
     * {@code '<(1,2),3,4>'} a circle. A scrape also cannot say where it stopped, so a
     * near-number like {@code 1e} went to the JDK's parser and its complaint reached the client
     * as an internal error. This reads the literal through from one end to the other and
     * requires all of it to be spoken for.
     */
    private static final class GeoReader {
        private static final Pattern NUMBER = Pattern.compile(
                "[-+]?(?:0[xX][0-9a-fA-F]+"
                        + "|(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?"
                        + "|[Ii][Nn][Ff](?:[Ii][Nn][Ii][Tt][Yy])?"
                        + "|[Nn][Aa][Nn])");

        private final String literal;
        private final String typeName;
        private int at;

        GeoReader(String literal, String typeName) {
            this.literal = literal;
            this.typeName = typeName;
        }

        MemgresException bad() {
            return new MemgresException(
                    "invalid input syntax for type " + typeName + ": \"" + literal + "\"", "22P02");
        }

        void spaces() {
            while (at < literal.length() && Character.isWhitespace(literal.charAt(at))) at++;
        }

        boolean peek(char c) {
            spaces();
            return at < literal.length() && literal.charAt(at) == c;
        }

        boolean accept(char c) {
            if (!peek(c)) return false;
            at++;
            return true;
        }

        void expect(char c) {
            if (!accept(c)) throw bad();
        }

        double number() {
            spaces();
            Matcher m = NUMBER.matcher(literal);
            m.region(at, literal.length());
            if (!m.lookingAt()) throw bad();
            String token = m.group();
            at = m.end();
            try {
                if (token.length() > 2 && (token.charAt(0) == '0' || token.charAt(1) == '0')) {
                    int sign = token.charAt(0) == '-' ? -1 : 1;
                    String digits = token.charAt(0) == '-' || token.charAt(0) == '+'
                            ? token.substring(1) : token;
                    if (digits.length() > 2 && digits.charAt(0) == '0'
                            && (digits.charAt(1) == 'x' || digits.charAt(1) == 'X')) {
                        return sign * (double) Long.parseLong(digits.substring(2), 16);
                    }
                }
                return Double.parseDouble(token.replaceAll("(?i)^([-+]?)inf$", "$1Infinity"));
            } catch (NumberFormatException e) {
                throw bad();
            }
        }

        /** A point, whose parentheses are optional where the surrounding form has its own. */
        PgPoint point() {
            boolean parens = accept('(');
            double x = number();
            expect(',');
            double y = number();
            if (parens) expect(')');
            return new PgPoint(x, y);
        }

        /** As many points as are written, each separated from the last by a comma. */
        List<PgPoint> points() {
            List<PgPoint> out = new ArrayList<PgPoint>();
            out.add(point());
            while (accept(',')) out.add(point());
            return out;
        }

        int mark() {
            spaces();
            return at;
        }

        void reset(int to) {
            at = to;
        }

        /** Nothing may be left over: a literal is the whole of what it says. */
        void end() {
            spaces();
            if (at != literal.length()) throw bad();
        }
    }

    /**
     * The points a literal names, with the opening and closing marks the type allows.
     *
     * @param open the characters that may open the list, each paired with its closing character
     */
    private static List<PgPoint> readPoints(String literal, String typeName, String open,
                                            String close, int least, int most) {
        GeoReader r = new GeoReader(literal, typeName);
        char closer = 0;
        for (int i = 0; i < open.length(); i++) {
            int before = r.mark();
            if (!r.accept(open.charAt(i))) continue;
            // A round bracket opens both the list and the first point in it, so it wraps the
            // list only when another follows: '((0,0),(1,1))' is a wrapped list of two points
            // and '(0,0),(1,1)' is a bare one. A square bracket is never a point's.
            if (open.charAt(i) == '(' && !r.peek('(')) {
                r.reset(before);
                continue;
            }
            closer = close.charAt(i);
            break;
        }
        List<PgPoint> points = r.points();
        if (closer != 0) r.expect(closer);
        r.end();
        if (points.size() < least || points.size() > most) throw r.bad();
        return points;
    }

    /**
     * Whether two geometric literals name the same value, by the rule the type has for it.
     *
     * <p>Each type says what "the same" means for it, and only some of them say anything: two
     * circles are equal when they enclose the same area, two lines when one's coefficients are
     * the other's scaled, and two paths when they have the same number of points. Comparing the
     * text these print as answered a different question for every one of them.
     *
     * @return {@code null} where the two are not both shapes of one kind, and the caller should
     *     go on to whatever it would otherwise have done
     */
    public static Boolean equalShapes(String a, String b) {
        if (a == null || b == null) return null;
        String left = a.trim();
        String right = b.trim();
        if (!isGeometricString(left) || !isGeometricString(right)) return null;
        try {
            if (left.startsWith("<") && right.startsWith("<")) {
                PgCircle c1 = parseCircle(left);
                PgCircle c2 = parseCircle(right);
                return Boolean.valueOf(fpEq(c1.radius * c1.radius, c2.radius * c2.radius));
            }
            if (left.startsWith("{") && right.startsWith("{")) {
                return Boolean.valueOf(sameLine(parseLine(left), parseLine(right)));
            }
            if (left.startsWith("[") && right.startsWith("[")) {
                // A square-bracketed literal is an open path or a segment; a segment holds two
                // points and is equal when both agree, a path when the counts do.
                List<PgPoint> p1 = parsePath(left).points;
                List<PgPoint> p2 = parsePath(right).points;
                if (p1.size() == 2 && p2.size() == 2) {
                    return Boolean.valueOf(fpEq(p1.get(0).x, p2.get(0).x)
                            && fpEq(p1.get(0).y, p2.get(0).y)
                            && fpEq(p1.get(1).x, p2.get(1).x)
                            && fpEq(p1.get(1).y, p2.get(1).y));
                }
                return Boolean.valueOf(p1.size() == p2.size());
            }
        } catch (MemgresException e) {
            return null;
        }
        return null;
    }

    /**
     * How two paths stand in order, which PostgreSQL decides by how many points they hold and
     * by nothing else. Two paths of the same length are neither before nor after each other,
     * however far apart the points are.
     *
     * @return {@code null} where the two are not both paths
     */
    public static Integer comparePaths(String a, String b) {
        if (a == null || b == null) return null;
        String left = a.trim();
        String right = b.trim();
        if (!isGeometricString(left) || !isGeometricString(right)) return null;
        boolean leftIsPath = left.startsWith("[") || left.startsWith("((");
        boolean rightIsPath = right.startsWith("[") || right.startsWith("((");
        if (!leftIsPath || !rightIsPath) return null;
        try {
            return Integer.valueOf(Integer.compare(
                    parsePath(left).points.size(), parsePath(right).points.size()));
        } catch (MemgresException e) {
            return null;
        }
    }

    /** Whether values of this type are shapes this class reads and writes. */
    public static boolean isGeometricType(DataType type) {
        switch (type) {
            case POINT: case LINE: case LSEG: case BOX: case PATH: case POLYGON: case CIRCLE:
                return true;
            default:
                return false;
        }
    }

    /** The shape a literal names, read as the named type's own reader reads it. */
    public static Object parseAs(String typeName, String literal) {
        String name = typeName == null ? "" : typeName.toLowerCase(java.util.Locale.ROOT);
        if (name.equals("point")) return parsePoint(literal);
        if (name.equals("line")) return parseLine(literal);
        if (name.equals("lseg")) return parseLseg(literal);
        if (name.equals("box")) return parseBox(literal);
        if (name.equals("path")) return parsePath(literal);
        if (name.equals("polygon")) return parsePolygon(literal);
        if (name.equals("circle")) return parseCircle(literal);
        throw new MemgresException("Not a geometric type: " + typeName, "42883");
    }

    public static PgPoint parsePoint(String s) {
        return readPoints(s, "point", "", "", 1, 1).get(0);
    }

    public static PgLine parseLine(String s) {
        // A line is written either as its three coefficients or as two points it passes through.
        if (s.trim().startsWith("{")) {
            GeoReader r = new GeoReader(s, "line");
            r.expect('{');
            double a = r.number();
            r.expect(',');
            double b = r.number();
            r.expect(',');
            double c = r.number();
            r.expect('}');
            r.end();
            return new PgLine(a, b, c);
        }
        List<PgPoint> pts = readPoints(s, "line", "[(", "])", 2, 2);
        // Two points that are the same point name no line, and PostgreSQL treats points nearer
        // than its own tolerance as the same: a line built from them is rounding residue
        // amplified into coefficients rather than the line anyone meant.
        if (fpEq(pts.get(0).x(), pts.get(1).x()) && fpEq(pts.get(0).y(), pts.get(1).y())) {
            throw new MemgresException(
                    "invalid line specification: must be two distinct points", "22P02");
        }
        return lineFromPoints(pts.get(0), pts.get(1));
    }

    public static PgLseg parseLseg(String s) {
        List<PgPoint> pts = readPoints(s, "lseg", "[(", "])", 2, 2);
        return new PgLseg(pts.get(0), pts.get(1));
    }

    public static PgBox parseBox(String s) {
        // A box is the only one of these that is not written between brackets.
        List<PgPoint> pts = readPoints(s, "box", "(", ")", 2, 2);
        return normalizeBox(pts.get(0), pts.get(1));
    }

    public static PgPath parsePath(String s) {
        // Square brackets say the path is open and everything else says it is closed.
        boolean closed = !s.trim().startsWith("[");
        List<PgPoint> pts = readPoints(s, "path", "[(", "])", 1, Integer.MAX_VALUE);
        return new PgPath(Cols.listCopyOf(pts), closed);
    }

    public static PgPolygon parsePolygon(String s) {
        List<PgPoint> pts = readPoints(s, "polygon", "(", ")", 1, Integer.MAX_VALUE);
        return new PgPolygon(Cols.listCopyOf(pts));
    }

    public static PgCircle parseCircle(String s) {
        GeoReader r = new GeoReader(s, "circle");
        char closer = 0;
        if (r.accept('<')) {
            closer = '>';
        } else {
            // A leading parenthesis belongs to the centre point unless another follows it, in
            // which case it is the one wrapping the whole circle.
            int before = r.mark();
            if (r.accept('(')) {
                if (r.peek('(')) closer = ')';
                else r.reset(before);
            }
        }
        PgPoint centre = r.point();
        r.expect(',');
        double radius = r.number();
        if (closer != 0) r.expect(closer);
        r.end();
        // A circle of negative radius is not a smaller circle: there is no such shape.
        if (!(radius >= 0)) throw r.bad();
        return new PgCircle(centre, radius);
    }

    // ========================================================================
    // Formatting methods
    // ========================================================================

    public static String format(Object geom) {
        if (geom instanceof PgPoint) return formatPoint(((PgPoint) geom));
        if (geom instanceof PgLine) return formatLine(((PgLine) geom));
        if (geom instanceof PgLseg) return formatLseg(((PgLseg) geom));
        if (geom instanceof PgBox) return formatBox(((PgBox) geom));
        if (geom instanceof PgPath) return formatPath(((PgPath) geom));
        if (geom instanceof PgPolygon) return formatPolygon(((PgPolygon) geom));
        if (geom instanceof PgCircle) return formatCircle(((PgCircle) geom));
        throw new MemgresException("Not a geometric type: " + pgTypeName(geom), "42883");
    }

    public static String formatPoint(PgPoint p) {
        // No point at all is written as no value at all: the closest-point operator has shapes
        // for which PostgreSQL answers NULL rather than naming a point.
        if (p == null) return null;
        return "(" + fmtD(p.x) + "," + fmtD(p.y) + ")";
    }

    public static String formatLine(PgLine l) {
        return "{" + fmtD(l.a) + "," + fmtD(l.b) + "," + fmtD(l.c) + "}";
    }

    public static String formatLseg(PgLseg l) {
        return "[" + formatPoint(l.p1) + "," + formatPoint(l.p2) + "]";
    }

    public static String formatBox(PgBox b) {
        return formatPoint(b.high) + "," + formatPoint(b.low);
    }

    public static String formatPath(PgPath p) {
        StringBuilder sb = new StringBuilder();
        sb.append(p.closed ? '(' : '[');
        for (int i = 0; i < p.points.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(formatPoint(p.points.get(i)));
        }
        sb.append(p.closed ? ')' : ']');
        return sb.toString();
    }

    public static String formatPolygon(PgPolygon p) {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < p.points.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append(formatPoint(p.points.get(i)));
        }
        sb.append(')');
        return sb.toString();
    }

    public static String formatCircle(PgCircle c) {
        return "<" + formatPoint(c.center) + "," + fmtD(c.radius) + ">";
    }

    /**
     * Format a double the way PostgreSQL's float8out does: shortest round-trip
     * decimal (as produced by {@link Double#toString}), reformatted with PG's
     * notation rules. Scientific notation ({@code e+NN}/{@code e-NN}, signed,
     * exponent zero-padded to at least two digits) is used when the decimal
     * exponent of the leading digit is &lt; -4 or &gt;= 15; otherwise plain
     * decimal. NaN/Infinity and the sign of -0 are preserved.
     */
    static String fmtD(double d) {
        return PgFloatFormat.float8out(d);
    }

    // ========================================================================
    // Box normalization
    // ========================================================================

    private static PgBox normalizeBox(PgPoint p1, PgPoint p2) {
        double hx = Math.max(p1.x, p2.x);
        double hy = Math.max(p1.y, p2.y);
        double lx = Math.min(p1.x, p2.x);
        double ly = Math.min(p1.y, p2.y);
        return new PgBox(new PgPoint(hx, hy), new PgPoint(lx, ly));
    }

    // ========================================================================
    // Line from two points
    // ========================================================================

    /**
     * The line through two points, written the way PostgreSQL writes it.
     *
     * <p>There are three cases and each has its own spelling: a vertical line is {@code x = C}
     * and so has A of minus one, a horizontal line is {@code y = C} and has B of minus one, and
     * everything else is {@code mx - y + c = 0}. Computing the coefficients from the
     * determinant instead and then forcing A positive gave the same line a different sign --
     * {@code [(0,0),(0,1)]} came out {@code {1,0,0}} where PostgreSQL writes {@code {-1,0,0}}.
     */
    public static PgLine lineFromPoints(PgPoint p1, PgPoint p2) {
        // The constructor names this a bad argument where the input function names it bad
        // syntax, and PostgreSQL keeps the two apart.
        if (fpEq(p1.x, p2.x) && fpEq(p1.y, p2.y)) {
            throw new MemgresException(
                    "invalid line specification: must be two distinct points", "22023");
        }
        if (fpEq(p1.x, p2.x)) return new PgLine(-1.0, 0.0, p1.x);
        if (fpEq(p1.y, p2.y)) return new PgLine(0.0, -1.0, p1.y);
        double a = (p2.y - p1.y) / (p2.x - p1.x);
        return new PgLine(a, -1.0, p1.y - a * p1.x);
    }

    /**
     * Whether two lines are the same line, which they are when one set of coefficients is the
     * other scaled: {@code {1,1,0}} and {@code {2,2,0}} both say that x and y are opposite.
     */
    public static boolean sameLine(PgLine l1, PgLine l2) {
        double ratio;
        if (!fpZero(l2.a)) ratio = l1.a / l2.a;
        else if (!fpZero(l2.b)) ratio = l1.b / l2.b;
        else if (!fpZero(l2.c)) ratio = l1.c / l2.c;
        else ratio = 1.0;
        return fpEq(l1.a, ratio * l2.a) && fpEq(l1.b, ratio * l2.b) && fpEq(l1.c, ratio * l2.c);
    }

    // ========================================================================
    // Point arithmetic (complex number operations)
    // ========================================================================

    public static PgPoint pointAdd(PgPoint a, PgPoint b) {
        return new PgPoint(a.x + b.x, a.y + b.y);
    }

    public static PgPoint pointSub(PgPoint a, PgPoint b) {
        return new PgPoint(a.x - b.x, a.y - b.y);
    }

    public static PgPoint pointMul(PgPoint a, PgPoint b) {
        // Complex multiply: (a.x + a.y*i) * (b.x + b.y*i)
        return new PgPoint(a.x * b.x - a.y * b.y, a.x * b.y + a.y * b.x);
    }

    public static PgPoint pointDiv(PgPoint a, PgPoint b) {
        // Complex divide: (a.x + a.y*i) / (b.x + b.y*i)
        double denom = b.x * b.x + b.y * b.y;
        // Only a divisor that is exactly zero is a division by zero. The tolerance is for
        // deciding whether two shapes coincide, not for deciding whether a number is zero, and
        // applying it here refused a perfectly good division by a very small point.
        if (denom == 0.0) {
            throw new MemgresException("division by zero");
        }
        return new PgPoint((a.x * b.x + a.y * b.y) / denom, (a.y * b.x - a.x * b.y) / denom);
    }

    // ========================================================================
    // Box arithmetic
    // ========================================================================

    public static PgBox boxAdd(PgBox box, PgPoint p) {
        return normalizeBox(pointAdd(box.high, p), pointAdd(box.low, p));
    }

    public static PgBox boxSub(PgBox box, PgPoint p) {
        return normalizeBox(pointSub(box.high, p), pointSub(box.low, p));
    }

    public static PgBox boxMul(PgBox box, PgPoint p) {
        return normalizeBox(pointMul(box.high, p), pointMul(box.low, p));
    }

    public static PgBox boxDiv(PgBox box, PgPoint p) {
        return normalizeBox(pointDiv(box.high, p), pointDiv(box.low, p));
    }

    // ========================================================================
    // Path arithmetic
    // ========================================================================

    public static PgPath pathAdd(PgPath path, PgPoint p) {
        List<PgPoint> pts = new ArrayList<>();
        for (PgPoint pt : path.points) pts.add(pointAdd(pt, p));
        return new PgPath(Cols.listCopyOf(pts), path.closed);
    }

    public static PgPath pathSub(PgPath path, PgPoint p) {
        List<PgPoint> pts = new ArrayList<>();
        for (PgPoint pt : path.points) pts.add(pointSub(pt, p));
        return new PgPath(Cols.listCopyOf(pts), path.closed);
    }

    public static PgPath pathMul(PgPath path, PgPoint p) {
        List<PgPoint> pts = new ArrayList<>();
        for (PgPoint pt : path.points) pts.add(pointMul(pt, p));
        return new PgPath(Cols.listCopyOf(pts), path.closed);
    }

    public static PgPath pathDiv(PgPath path, PgPoint p) {
        List<PgPoint> pts = new ArrayList<>();
        for (PgPoint pt : path.points) pts.add(pointDiv(pt, p));
        return new PgPath(Cols.listCopyOf(pts), path.closed);
    }

    // ========================================================================
    // Circle arithmetic
    // ========================================================================

    public static PgCircle circleAdd(PgCircle c, PgPoint p) {
        return new PgCircle(pointAdd(c.center, p), c.radius);
    }

    public static PgCircle circleSub(PgCircle c, PgPoint p) {
        return new PgCircle(pointSub(c.center, p), c.radius);
    }

    public static PgCircle circleMul(PgCircle c, PgPoint p) {
        double scale = Math.sqrt(p.x * p.x + p.y * p.y);
        return new PgCircle(pointMul(c.center, p), c.radius * scale);
    }

    public static PgCircle circleDiv(PgCircle c, PgPoint p) {
        double scale = Math.sqrt(p.x * p.x + p.y * p.y);
        if (Math.abs(scale) < EPSILON) {
            throw new MemgresException("division by zero");
        }
        return new PgCircle(pointDiv(c.center, p), c.radius / scale);
    }

    // ========================================================================
    // Distance functions (<-> operator)
    // ========================================================================

    public static double distancePointPoint(PgPoint a, PgPoint b) {
        double dx = a.x - b.x;
        double dy = a.y - b.y;
        return Math.sqrt(dx * dx + dy * dy);
    }

    public static double distancePointLine(PgPoint p, PgLine l) {
        double denom = Math.sqrt(l.a * l.a + l.b * l.b);
        if (denom < EPSILON) return 0;
        return Math.abs(l.a * p.x + l.b * p.y + l.c) / denom;
    }

    public static double distancePointLseg(PgPoint p, PgLseg seg) {
        PgPoint closest = closestPointOnLseg(p, seg);
        return distancePointPoint(p, closest);
    }

    public static double distancePointBox(PgPoint p, PgBox box) {
        // If inside, distance is 0
        if (p.x >= box.low.x && p.x <= box.high.x && p.y >= box.low.y && p.y <= box.high.y) {
            return 0;
        }
        double cx = Math.max(box.low.x, Math.min(p.x, box.high.x));
        double cy = Math.max(box.low.y, Math.min(p.y, box.high.y));
        return distancePointPoint(p, new PgPoint(cx, cy));
    }

    public static double distanceLsegLseg(PgLseg a, PgLseg b) {
        // If they intersect, distance is 0
        if (lsegIntersects(a, b)) return 0;
        double d1 = distancePointLseg(a.p1, b);
        double d2 = distancePointLseg(a.p2, b);
        double d3 = distancePointLseg(b.p1, a);
        double d4 = distancePointLseg(b.p2, a);
        return Math.min(Math.min(d1, d2), Math.min(d3, d4));
    }

    public static double distanceLsegLine(PgLseg seg, PgLine line) {
        double d1 = distancePointLine(seg.p1, line);
        double d2 = distancePointLine(seg.p2, line);
        // If they're on opposite sides, the segment crosses the line
        double v1 = line.a * seg.p1.x + line.b * seg.p1.y + line.c;
        double v2 = line.a * seg.p2.x + line.b * seg.p2.y + line.c;
        if (v1 * v2 <= 0) return 0; // segment crosses line
        return Math.min(d1, d2);
    }

    public static double distanceLineLine(PgLine a, PgLine b) {
        // If not parallel, they intersect (distance 0)
        double det = a.a * b.b - a.b * b.a;
        if (Math.abs(det) > EPSILON) return 0;
        // Parallel lines
        double denom = Math.sqrt(a.a * a.a + a.b * a.b);
        if (denom < EPSILON) return 0;
        return Math.abs(a.c - b.c * (Math.sqrt(a.a * a.a + a.b * a.b) / Math.sqrt(b.a * b.a + b.b * b.b))) / denom;
    }

    public static double distanceCircleCircle(PgCircle a, PgCircle b) {
        double centerDist = distancePointPoint(a.center, b.center);
        double d = centerDist - a.radius - b.radius;
        return Math.max(0, d);
    }

    public static double distancePolygonPolygon(PgPolygon a, PgPolygon b) {
        // Approximate: min distance between all edge pairs
        // Also check containment
        if (polygonContainsPoint(a, b.points.get(0)) || polygonContainsPoint(b, a.points.get(0))) {
            return 0;
        }
        double minDist = Double.MAX_VALUE;
        for (int i = 0; i < a.points.size(); i++) {
            PgLseg edgeA = new PgLseg(a.points.get(i), a.points.get((i + 1) % a.points.size()));
            for (int j = 0; j < b.points.size(); j++) {
                PgLseg edgeB = new PgLseg(b.points.get(j), b.points.get((j + 1) % b.points.size()));
                minDist = Math.min(minDist, distanceLsegLseg(edgeA, edgeB));
            }
        }
        return minDist;
    }

    public static double distancePointPath(PgPoint p, PgPath path) {
        if (path.closed) {
            // For closed path, check containment first
            if (pathContainsPoint(path, p)) return 0;
        }
        double minDist = Double.MAX_VALUE;
        int n = path.points.size();
        int limit = path.closed ? n : n - 1;
        for (int i = 0; i < limit; i++) {
            PgLseg edge = new PgLseg(path.points.get(i), path.points.get((i + 1) % n));
            minDist = Math.min(minDist, distancePointLseg(p, edge));
        }
        return minDist;
    }

    public static double distancePointPolygon(PgPoint p, PgPolygon poly) {
        if (polygonContainsPoint(poly, p)) return 0;
        double minDist = Double.MAX_VALUE;
        int n = poly.points.size();
        for (int i = 0; i < n; i++) {
            PgLseg edge = new PgLseg(poly.points.get(i), poly.points.get((i + 1) % n));
            minDist = Math.min(minDist, distancePointLseg(p, edge));
        }
        return minDist;
    }

    public static double distanceLsegBox(PgLseg seg, PgBox box) {
        // If segment intersects or is contained by box, distance is 0
        if (boxContainsPoint(box, seg.p1) || boxContainsPoint(box, seg.p2)) return 0;
        PgLseg[] edges = boxEdges(box);
        double minDist = Double.MAX_VALUE;
        for (PgLseg edge : edges) {
            if (lsegIntersects(seg, edge)) return 0;
            minDist = Math.min(minDist, distanceLsegLseg(seg, edge));
        }
        return minDist;
    }

    public static double distancePathPath(PgPath a, PgPath b) {
        double minDist = Double.MAX_VALUE;
        int na = a.points.size(), nb = b.points.size();
        int limA = a.closed ? na : na - 1;
        int limB = b.closed ? nb : nb - 1;
        for (int i = 0; i < limA; i++) {
            PgLseg ea = new PgLseg(a.points.get(i), a.points.get((i + 1) % na));
            for (int j = 0; j < limB; j++) {
                PgLseg eb = new PgLseg(b.points.get(j), b.points.get((j + 1) % nb));
                double d = distanceLsegLseg(ea, eb);
                if (d == 0) return 0;
                minDist = Math.min(minDist, d);
            }
        }
        return minDist;
    }

    public static double distanceCirclePolygon(PgCircle c, PgPolygon poly) {
        double d = distancePointPolygon(c.center, poly);
        return Math.max(0, d - c.radius);
    }

    /**
     * General distance dispatch.
     */
    public static double distance(Object a, Object b) {
        if (a instanceof PgPoint && b instanceof PgPoint) return distancePointPoint(((PgPoint) a), ((PgPoint) b));
        if (a instanceof PgPoint && b instanceof PgLine) return distancePointLine(((PgPoint) a), ((PgLine) b));
        if (a instanceof PgLine && b instanceof PgPoint) return distancePointLine(((PgPoint) b), ((PgLine) a));
        if (a instanceof PgPoint && b instanceof PgLseg) return distancePointLseg(((PgPoint) a), ((PgLseg) b));
        if (a instanceof PgLseg && b instanceof PgPoint) return distancePointLseg(((PgPoint) b), ((PgLseg) a));
        if (a instanceof PgPoint && b instanceof PgBox) return distancePointBox(((PgPoint) a), ((PgBox) b));
        if (a instanceof PgBox && b instanceof PgPoint) return distancePointBox(((PgPoint) b), ((PgBox) a));
        if (a instanceof PgPoint && b instanceof PgPath) return distancePointPath(((PgPoint) a), ((PgPath) b));
        if (a instanceof PgPath && b instanceof PgPoint) return distancePointPath(((PgPoint) b), ((PgPath) a));
        if (a instanceof PgPoint && b instanceof PgPolygon) return distancePointPolygon(((PgPoint) a), ((PgPolygon) b));
        if (a instanceof PgPolygon && b instanceof PgPoint) return distancePointPolygon(((PgPoint) b), ((PgPolygon) a));
        if (a instanceof PgPoint && b instanceof PgCircle) return Math.max(0, distancePointPoint(((PgPoint) a), ((PgCircle) b).center) - ((PgCircle) b).radius);
        if (a instanceof PgCircle && b instanceof PgPoint) return Math.max(0, distancePointPoint(((PgPoint) b), ((PgCircle) a).center) - ((PgCircle) a).radius);
        if (a instanceof PgLseg && b instanceof PgLseg) return distanceLsegLseg(((PgLseg) a), ((PgLseg) b));
        if (a instanceof PgLseg && b instanceof PgLine) return distanceLsegLine(((PgLseg) a), ((PgLine) b));
        if (a instanceof PgLine && b instanceof PgLseg) return distanceLsegLine(((PgLseg) b), ((PgLine) a));
        if (a instanceof PgLseg && b instanceof PgBox) return distanceLsegBox(((PgLseg) a), ((PgBox) b));
        if (a instanceof PgBox && b instanceof PgLseg) return distanceLsegBox(((PgLseg) b), ((PgBox) a));
        if (a instanceof PgLine && b instanceof PgLine) return distanceLineLine(((PgLine) a), ((PgLine) b));
        if (a instanceof PgCircle && b instanceof PgCircle) return distanceCircleCircle(((PgCircle) a), ((PgCircle) b));
        if (a instanceof PgPolygon && b instanceof PgPolygon) return distancePolygonPolygon(((PgPolygon) a), ((PgPolygon) b));
        if (a instanceof PgCircle && b instanceof PgPolygon) return distanceCirclePolygon(((PgCircle) a), ((PgPolygon) b));
        if (a instanceof PgPolygon && b instanceof PgCircle) return distanceCirclePolygon(((PgCircle) b), ((PgPolygon) a));
        if (a instanceof PgPath && b instanceof PgPath) return distancePathPath(((PgPath) a), ((PgPath) b));
        if (a instanceof PgBox && b instanceof PgBox) {
            PgPoint ca = center((PgBox) a);
            PgPoint cb = center((PgBox) b);
            return distancePointPoint(ca, cb);
        }
        throw new MemgresException("operator does not exist: " + pgTypeName(a) + " <-> " + pgTypeName(b), "42883");
    }

    private static PgLseg[] boxEdges(PgBox b) {
        PgPoint tl = new PgPoint(b.low.x, b.high.y);
        PgPoint tr = b.high;
        PgPoint bl = b.low;
        PgPoint br = new PgPoint(b.high.x, b.low.y);
        return new PgLseg[]{
                new PgLseg(tl, tr), new PgLseg(tr, br),
                new PgLseg(br, bl), new PgLseg(bl, tl)
        };
    }

    // ========================================================================
    // Measurement functions
    // ========================================================================

    /** @-@ operator: length of lseg, perimeter/length of path */
    public static double length(Object geom) {
        if (geom instanceof PgLseg) {
            PgLseg seg = (PgLseg) geom;
            return distancePointPoint(seg.p1, seg.p2);
        }
        if (geom instanceof PgPath) {
            PgPath path = (PgPath) geom;
            double total = 0;
            for (int i = 0; i < path.points.size() - 1; i++) {
                total += distancePointPoint(path.points.get(i), path.points.get(i + 1));
            }
            if (path.closed && path.points.size() > 1) {
                total += distancePointPoint(path.points.get(path.points.size() - 1), path.points.get(0));
            }
            return total;
        }
        throw new MemgresException("length not supported for " + pgTypeName(geom), "42883");
    }

    /** @@ operator: center */
    public static PgPoint center(Object geom) {
        if (geom instanceof PgBox) {
            PgBox b = (PgBox) geom;
            return new PgPoint((b.high.x + b.low.x) / 2, (b.high.y + b.low.y) / 2);
        }
        if (geom instanceof PgCircle) return ((PgCircle) geom).center;
        if (geom instanceof PgLseg) {
            PgLseg seg = (PgLseg) geom;
            return new PgPoint((seg.p1.x + seg.p2.x) / 2, (seg.p1.y + seg.p2.y) / 2);
        }
        if (geom instanceof PgPolygon) {
            PgPolygon poly = (PgPolygon) geom;
            double sx = 0, sy = 0;
            for (PgPoint p : poly.points) { sx += p.x; sy += p.y; }
            return new PgPoint(sx / poly.points.size(), sy / poly.points.size());
        }
        if (geom instanceof PgPath) {
            PgPath path = (PgPath) geom;
            double sx = 0, sy = 0;
            for (PgPoint p : path.points) { sx += p.x; sy += p.y; }
            return new PgPoint(sx / path.points.size(), sy / path.points.size());
        }
        throw new MemgresException("center not supported for " + pgTypeName(geom), "42883");
    }

    public static int npoints(Object geom) {
        if (geom instanceof PgPath) return ((PgPath) geom).points.size();
        if (geom instanceof PgPolygon) return ((PgPolygon) geom).points.size();
        throw new MemgresException("npoints not supported for " + pgTypeName(geom), "42883");
    }

    /**
     * Compute area. Returns null for open paths (PG returns NULL).
     */
    public static Double area(Object geom) {
        if (geom instanceof PgBox) {
            PgBox b = (PgBox) geom;
            return (b.high.x - b.low.x) * (b.high.y - b.low.y);
        }
        if (geom instanceof PgCircle) {
            PgCircle c = (PgCircle) geom;
            return Math.PI * c.radius * c.radius;
        }
        if (geom instanceof PgPolygon) {
            PgPolygon poly = (PgPolygon) geom;
            return shoelaceArea(poly.points);
        }
        if (geom instanceof PgPath) {
            PgPath path = (PgPath) geom;
            if (!path.closed) return null; // PG: area(open path) = NULL
            return shoelaceArea(path.points);
        }
        throw new MemgresException("area not supported for " + pgTypeName(geom), "42883");
    }

    private static double shoelaceArea(List<PgPoint> points) {
        double sum = 0;
        int n = points.size();
        for (int i = 0; i < n; i++) {
            PgPoint curr = points.get(i);
            PgPoint next = points.get((i + 1) % n);
            sum += curr.x * next.y - next.x * curr.y;
        }
        return Math.abs(sum) / 2;
    }

    public static double diameter(PgCircle c) {
        return 2 * c.radius;
    }

    public static double radius(PgCircle c) {
        return c.radius;
    }

    public static double height(PgBox b) {
        return b.high.y - b.low.y;
    }

    public static double width(PgBox b) {
        return b.high.x - b.low.x;
    }

    /**
     * The slope of the line through two points.
     *
     * <p>The difference is taken first minus second, as PostgreSQL takes it. Taking it the other
     * way round is the same number except in sign, and for a pair on the same horizontal it is
     * the difference between zero and minus zero -- which prints.
     */
    public static double slope(PgPoint p1, PgPoint p2) {
        if (fpEq(p1.x, p2.x)) return Double.POSITIVE_INFINITY;
        return (p1.y - p2.y) / (p1.x - p2.x);
    }

    public static double slope(PgLseg seg) {
        return slope(seg.p1, seg.p2);
    }

    public static PgLseg diagonal(PgBox b) {
        return new PgLseg(b.high, b.low);
    }

    public static PgBox boundBox(Object geom) {
        List<PgPoint> pts;
        if (geom instanceof PgPolygon) {
            PgPolygon poly = (PgPolygon) geom;
            pts = poly.points;
        } else if (geom instanceof PgPath) {
            PgPath path = (PgPath) geom;
            pts = path.points;
        } else if (geom instanceof PgLseg) {
            PgLseg seg = (PgLseg) geom;
            pts = Cols.listOf(seg.p1, seg.p2);
        } else if (geom instanceof PgBox) {
            PgBox b = (PgBox) geom;
            return b;
        } else if (geom instanceof PgCircle) {
            PgCircle c = (PgCircle) geom;
            return new PgBox(
                new PgPoint(c.center.x + c.radius, c.center.y + c.radius),
                new PgPoint(c.center.x - c.radius, c.center.y - c.radius)
            );
        } else {
            throw new MemgresException("bound_box not supported for " + pgTypeName(geom), "42883");
        }
        double minX = Double.MAX_VALUE, minY = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE, maxY = -Double.MAX_VALUE;
        for (PgPoint p : pts) {
            minX = Math.min(minX, p.x); minY = Math.min(minY, p.y);
            maxX = Math.max(maxX, p.x); maxY = Math.max(maxY, p.y);
        }
        return new PgBox(new PgPoint(maxX, maxY), new PgPoint(minX, minY));
    }

    public static boolean isclosed(PgPath p) { return p.closed; }
    public static boolean isopen(PgPath p) { return !p.closed; }

    public static PgPath pclose(PgPath p) {
        return new PgPath(p.points, true);
    }

    public static PgPath popen(PgPath p) {
        return new PgPath(p.points, false);
    }

    // ========================================================================
    // Containment predicates (@> and <@)
    // ========================================================================

    public static boolean boxContainsPoint(PgBox box, PgPoint p) {
        return p.x >= box.low.x && p.x <= box.high.x && p.y >= box.low.y && p.y <= box.high.y;
    }

    public static boolean boxContainsBox(PgBox outer, PgBox inner) {
        return inner.low.x >= outer.low.x && inner.high.x <= outer.high.x
            && inner.low.y >= outer.low.y && inner.high.y <= outer.high.y;
    }

    public static boolean circleContainsPoint(PgCircle c, PgPoint p) {
        return distancePointPoint(c.center, p) <= c.radius + EPSILON;
    }

    public static boolean circleContainsCircle(PgCircle outer, PgCircle inner) {
        return distancePointPoint(outer.center, inner.center) + inner.radius <= outer.radius + EPSILON;
    }

    /**
     * Test if point lies on a line segment (within EPSILON tolerance).
     */
    private static boolean pointOnLseg(PgPoint p, PgPoint a, PgPoint b) {
        // Check collinearity
        double cross = (b.x - a.x) * (p.y - a.y) - (b.y - a.y) * (p.x - a.x);
        if (Math.abs(cross) > EPSILON) return false;
        // Check within bounding box
        return p.x >= Math.min(a.x, b.x) - EPSILON && p.x <= Math.max(a.x, b.x) + EPSILON
            && p.y >= Math.min(a.y, b.y) - EPSILON && p.y <= Math.max(a.y, b.y) + EPSILON;
    }

    /**
     * Polygon contains point. Uses ray-casting with boundary inclusion (PG behavior).
     */
    public static boolean polygonContainsPoint(PgPolygon poly, PgPoint p) {
        int n = poly.points.size();
        // Check if point is on any edge (boundary → contained in PG)
        for (int i = 0; i < n; i++) {
            PgPoint pi = poly.points.get(i);
            PgPoint pj = poly.points.get((i + 1) % n);
            if (pointOnLseg(p, pi, pj)) return true;
        }
        // Standard ray-casting for interior
        boolean inside = false;
        for (int i = 0, j = n - 1; i < n; j = i++) {
            PgPoint pi = poly.points.get(i);
            PgPoint pj = poly.points.get(j);
            if ((pi.y > p.y) != (pj.y > p.y) &&
                p.x < (pj.x - pi.x) * (p.y - pi.y) / (pj.y - pi.y) + pi.x) {
                inside = !inside;
            }
        }
        return inside;
    }

    /** lseg contained in line (collinear check). */
    public static boolean lineContainsLseg(PgLine line, PgLseg seg) {
        double v1 = Math.abs(line.a * seg.p1.x + line.b * seg.p1.y + line.c);
        double v2 = Math.abs(line.a * seg.p2.x + line.b * seg.p2.y + line.c);
        double norm = Math.sqrt(line.a * line.a + line.b * line.b);
        if (norm < EPSILON) return false;
        return v1 / norm < EPSILON && v2 / norm < EPSILON;
    }

    /** lseg contained in box. */
    public static boolean boxContainsLseg(PgBox box, PgLseg seg) {
        return boxContainsPoint(box, seg.p1) && boxContainsPoint(box, seg.p2);
    }

    /** point contained in lseg. */
    public static boolean lsegContainsPoint(PgLseg seg, PgPoint p) {
        return pointOnLseg(p, seg.p1, seg.p2);
    }

    /** point contained in line. */
    public static boolean lineContainsPoint(PgLine line, PgPoint p) {
        double v = Math.abs(line.a * p.x + line.b * p.y + line.c);
        double norm = Math.sqrt(line.a * line.a + line.b * line.b);
        if (norm < EPSILON) return false;
        return v / norm < EPSILON;
    }

    /** point contained in path. */
    public static boolean pathContainsPoint(PgPath path, PgPoint p) {
        if (path.closed) {
            return polygonContainsPoint(new PgPolygon(path.points), p);
        }
        // Open path: point on any segment
        for (int i = 0; i < path.points.size() - 1; i++) {
            if (pointOnLseg(p, path.points.get(i), path.points.get(i + 1))) return true;
        }
        return false;
    }

    /**
     * General contains dispatch (@>).
     */
    public static boolean contains(Object a, Object b) {
        // PG's geometric @> operator set: box@>box, box@>point, circle@>circle,
        // circle@>point, path@>point, polygon@>point, polygon@>polygon.
        if (a instanceof PgBox && b instanceof PgPoint) return boxContainsPoint(((PgBox) a), ((PgPoint) b));
        if (a instanceof PgBox && b instanceof PgBox) return boxContainsBox(((PgBox) a), ((PgBox) b));
        if (a instanceof PgCircle && b instanceof PgPoint) return circleContainsPoint(((PgCircle) a), ((PgPoint) b));
        if (a instanceof PgCircle && b instanceof PgCircle) return circleContainsCircle(((PgCircle) a), ((PgCircle) b));
        if (a instanceof PgPolygon && b instanceof PgPoint) return polygonContainsPoint(((PgPolygon) a), ((PgPoint) b));
        if (a instanceof PgPolygon && b instanceof PgPolygon) {
            PgPolygon pb = (PgPolygon) b;
            PgPolygon pa = (PgPolygon) a;
            for (PgPoint p : pb.points) {
                if (!polygonContainsPoint(pa, p)) return false;
            }
            return true;
        }
        if (a instanceof PgPath && b instanceof PgPoint) return pathContainsPoint(((PgPath) a), ((PgPoint) b));
        // A bracketed two-point value reads back as an lseg, but an lseg has no @> operator at
        // all -- the only reading under which this containment exists is an open path, which is
        // what PG's path @> point does.
        if (a instanceof PgLseg && b instanceof PgPoint) {
            PgLseg seg = (PgLseg) a;
            return pathContainsPoint(new PgPath(java.util.Arrays.asList(seg.p1, seg.p2), false),
                    ((PgPoint) b));
        }
        throw new MemgresException("operator does not exist: " + pgTypeName(a) + " @> " + pgTypeName(b), "42883");
    }

    /**
     * General contained-by dispatch (a &lt;@ b). PG's &lt;@ operator set is NOT simply the reverse
     * of @>: point&lt;@line, point&lt;@lseg, lseg&lt;@box and lseg&lt;@line are valid &lt;@ operators
     * with no @> counterpart (so they must not route through {@link #contains}, which correctly
     * rejects those pairs as phantom @> operators).
     */
    public static boolean containedBy(Object a, Object b) {
        if (a instanceof PgPoint) {
            PgPoint p = (PgPoint) a;
            if (b instanceof PgBox) return boxContainsPoint(((PgBox) b), p);
            if (b instanceof PgCircle) return circleContainsPoint(((PgCircle) b), p);
            if (b instanceof PgPolygon) return polygonContainsPoint(((PgPolygon) b), p);
            if (b instanceof PgPath) return pathContainsPoint(((PgPath) b), p);
            if (b instanceof PgLine) return distancePointLine(p, ((PgLine) b)) < EPSILON;
            if (b instanceof PgLseg) return distancePointLseg(p, ((PgLseg) b)) < EPSILON;
        }
        if (a instanceof PgLseg) {
            PgLseg s = (PgLseg) a;
            if (b instanceof PgBox) return boxContainsPoint(((PgBox) b), s.p1) && boxContainsPoint(((PgBox) b), s.p2);
            if (b instanceof PgLine) {
                PgLine ln = (PgLine) b;
                return distancePointLine(s.p1, ln) < EPSILON && distancePointLine(s.p2, ln) < EPSILON;
            }
        }
        if (a instanceof PgBox && b instanceof PgBox) return boxContainsBox(((PgBox) b), ((PgBox) a));
        if (a instanceof PgCircle && b instanceof PgCircle) return circleContainsCircle(((PgCircle) b), ((PgCircle) a));
        if (a instanceof PgPolygon && b instanceof PgPolygon) return contains(b, a);
        throw new MemgresException("operator does not exist: " + pgTypeName(a) + " <@ " + pgTypeName(b), "42883");
    }

    public static boolean containedBy(String a, String b) {
        return containedBy((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static String pgTypeName(Object geom) {
        if (geom instanceof PgPoint) return "point";
        if (geom instanceof PgLine) return "line";
        if (geom instanceof PgLseg) return "lseg";
        if (geom instanceof PgBox) return "box";
        if (geom instanceof PgPath) return "path";
        if (geom instanceof PgPolygon) return "polygon";
        if (geom instanceof PgCircle) return "circle";
        return "unknown";
    }

    // ========================================================================
    // Overlap predicate (&&)
    // ========================================================================

    public static boolean overlapsBoxBox(PgBox a, PgBox b) {
        return a.low.x <= b.high.x && a.high.x >= b.low.x
            && a.low.y <= b.high.y && a.high.y >= b.low.y;
    }

    public static boolean overlapsCircleCircle(PgCircle a, PgCircle b) {
        return distancePointPoint(a.center, b.center) <= a.radius + b.radius + EPSILON;
    }

    public static boolean overlapsPolygonPolygon(PgPolygon a, PgPolygon b) {
        // Check if any vertex of a is inside b or vice versa, or if any edges intersect
        for (PgPoint p : a.points) {
            if (polygonContainsPoint(b, p)) return true;
        }
        for (PgPoint p : b.points) {
            if (polygonContainsPoint(a, p)) return true;
        }
        // Check edge intersections
        for (int i = 0; i < a.points.size(); i++) {
            PgLseg ea = new PgLseg(a.points.get(i), a.points.get((i + 1) % a.points.size()));
            for (int j = 0; j < b.points.size(); j++) {
                PgLseg eb = new PgLseg(b.points.get(j), b.points.get((j + 1) % b.points.size()));
                if (lsegIntersects(ea, eb)) return true;
            }
        }
        return false;
    }

    public static boolean overlaps(Object a, Object b) {
        if (a instanceof PgBox && b instanceof PgBox) return overlapsBoxBox(((PgBox) a), ((PgBox) b));
        if (a instanceof PgCircle && b instanceof PgCircle) return overlapsCircleCircle(((PgCircle) a), ((PgCircle) b));
        if (a instanceof PgPolygon && b instanceof PgPolygon) return overlapsPolygonPolygon(((PgPolygon) a), ((PgPolygon) b));
        throw new MemgresException("overlap not supported between " + pgTypeName(a) + " and " + pgTypeName(b), "42883");
    }

    // ========================================================================
    // Same as (~=), approximate equality
    // ========================================================================

    public static boolean sameAs(Object a, Object b) {
        if (a instanceof PgPoint && b instanceof PgPoint) {
            PgPoint pb = (PgPoint) b;
            PgPoint pa = (PgPoint) a;
            return fpEq(pa.x, pb.x) && fpEq(pa.y, pb.y);
        }
        if (a instanceof PgBox && b instanceof PgBox) {
            PgBox bb = (PgBox) b;
            PgBox ba = (PgBox) a;
            return sameAs(ba.high, bb.high) && sameAs(ba.low, bb.low);
        }
        if (a instanceof PgCircle && b instanceof PgCircle) {
            PgCircle cb = (PgCircle) b;
            PgCircle ca = (PgCircle) a;
            return sameAs(ca.center, cb.center) && fpEq(ca.radius, cb.radius);
        }
        if (a instanceof PgLine && b instanceof PgLine) {
            PgLine lb = (PgLine) b;
            PgLine la = (PgLine) a;
            // Lines are the same if coefficients are proportional
            double r1 = 0, r2 = 0;
            if (Math.abs(lb.a) > EPSILON) r1 = la.a / lb.a;
            else if (Math.abs(la.a) > EPSILON) return false;
            if (Math.abs(lb.b) > EPSILON) r2 = la.b / lb.b;
            else if (Math.abs(la.b) > EPSILON) return false;
            double r3 = 0;
            if (Math.abs(lb.c) > EPSILON) r3 = la.c / lb.c;
            else if (Math.abs(la.c) > EPSILON) return false;
            else return Math.abs(r1 - r2) < EPSILON; // c both zero
            if (Math.abs(la.a) < EPSILON && Math.abs(la.b) < EPSILON) return Math.abs(lb.a) < EPSILON && Math.abs(lb.b) < EPSILON;
            // Check all nonzero ratios are equal
            double r = 0;
            boolean set = false;
            for (double ri : new double[]{r1, r2, r3}) {
                if (!set) { r = ri; set = true; }
                else if (Math.abs(r - ri) > EPSILON) return false;
            }
            return true;
        }
        if (a instanceof PgLseg && b instanceof PgLseg) {
            PgLseg lb = (PgLseg) b;
            PgLseg la = (PgLseg) a;
            return (sameAs(la.p1, lb.p1) && sameAs(la.p2, lb.p2))
                || (sameAs(la.p1, lb.p2) && sameAs(la.p2, lb.p1));
        }
        if (a instanceof PgPolygon && b instanceof PgPolygon) {
            PgPolygon pb = (PgPolygon) b;
            PgPolygon pa = (PgPolygon) a;
            if (pa.points.size() != pb.points.size()) return false;
            // Same area and same bounding box as approximation
            return Math.abs(area(pa) - area(pb)) < EPSILON
                && sameAs(boundBox(pa), boundBox(pb));
        }
        if (a instanceof PgPath && b instanceof PgPath) {
            return a.equals(b);
        }
        return a.equals(b);
    }

    // ========================================================================
    // Intersection tests (?#)
    // ========================================================================

    public static boolean lsegIntersects(PgLseg a, PgLseg b) {
        return lsegIntersectionPoint(a, b) != null || lsegsOverlap(a, b);
    }

    private static boolean lsegsOverlap(PgLseg a, PgLseg b) {
        // Collinear overlap check
        if (!collinear(a.p1, a.p2, b.p1)) return false;
        // Project onto the longer axis
        double ax1 = Math.min(a.p1.x, a.p2.x), ax2 = Math.max(a.p1.x, a.p2.x);
        double bx1 = Math.min(b.p1.x, b.p2.x), bx2 = Math.max(b.p1.x, b.p2.x);
        double ay1 = Math.min(a.p1.y, a.p2.y), ay2 = Math.max(a.p1.y, a.p2.y);
        double by1 = Math.min(b.p1.y, b.p2.y), by2 = Math.max(b.p1.y, b.p2.y);
        return ax1 <= bx2 + EPSILON && bx1 <= ax2 + EPSILON
            && ay1 <= by2 + EPSILON && by1 <= ay2 + EPSILON;
    }

    private static boolean collinear(PgPoint a, PgPoint b, PgPoint c) {
        return Math.abs((b.x - a.x) * (c.y - a.y) - (c.x - a.x) * (b.y - a.y)) < EPSILON;
    }

    /**
     * Find the intersection point of two line segments, or null if none.
     */
    public static PgPoint lsegIntersectionPoint(PgLseg a, PgLseg b) {
        double x1 = a.p1.x, y1 = a.p1.y, x2 = a.p2.x, y2 = a.p2.y;
        double x3 = b.p1.x, y3 = b.p1.y, x4 = b.p2.x, y4 = b.p2.y;
        double denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
        if (Math.abs(denom) < EPSILON) return null; // parallel
        double t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom;
        double u = -((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3)) / denom;
        if (t >= -EPSILON && t <= 1 + EPSILON && u >= -EPSILON && u <= 1 + EPSILON) {
            return new PgPoint(x1 + t * (x2 - x1), y1 + t * (y2 - y1));
        }
        return null;
    }

    public static boolean lineIntersectsLseg(PgLine line, PgLseg seg) {
        double v1 = line.a * seg.p1.x + line.b * seg.p1.y + line.c;
        double v2 = line.a * seg.p2.x + line.b * seg.p2.y + line.c;
        return v1 * v2 <= EPSILON; // different sides or on the line
    }

    public static boolean boxIntersectsBox(PgBox a, PgBox b) {
        return overlapsBoxBox(a, b);
    }

    public static boolean lineIntersectsBox(PgLine line, PgBox box) {
        // Check if line intersects any edge of the box
        PgPoint tl = new PgPoint(box.low.x, box.high.y);
        PgPoint tr = box.high;
        PgPoint bl = box.low;
        PgPoint br = new PgPoint(box.high.x, box.low.y);
        return lineIntersectsLseg(line, new PgLseg(bl, br))
            || lineIntersectsLseg(line, new PgLseg(br, tr))
            || lineIntersectsLseg(line, new PgLseg(tr, tl))
            || lineIntersectsLseg(line, new PgLseg(tl, bl));
    }

    public static boolean lsegIntersectsBox(PgLseg seg, PgBox box) {
        if (boxContainsPoint(box, seg.p1) || boxContainsPoint(box, seg.p2)) return true;
        for (PgLseg edge : boxEdges(box)) {
            if (lsegIntersects(seg, edge)) return true;
        }
        return false;
    }

    public static boolean pathIntersectsPath(PgPath a, PgPath b) {
        int na = a.points.size(), nb = b.points.size();
        int limA = a.closed ? na : na - 1;
        int limB = b.closed ? nb : nb - 1;
        for (int i = 0; i < limA; i++) {
            PgLseg ea = new PgLseg(a.points.get(i), a.points.get((i + 1) % na));
            for (int j = 0; j < limB; j++) {
                PgLseg eb = new PgLseg(b.points.get(j), b.points.get((j + 1) % nb));
                if (lsegIntersects(ea, eb)) return true;
            }
        }
        return false;
    }

    public static boolean intersects(Object a, Object b) {
        if (a instanceof PgLseg && b instanceof PgLseg) return lsegIntersects(((PgLseg) a), ((PgLseg) b));
        // PG has lseg ?# line but NOT line ?# lseg.
        if (a instanceof PgLseg && b instanceof PgLine) return lineIntersectsLseg(((PgLine) b), ((PgLseg) a));
        if (a instanceof PgBox && b instanceof PgBox) return boxIntersectsBox(((PgBox) a), ((PgBox) b));
        if (a instanceof PgLine && b instanceof PgBox) return lineIntersectsBox(((PgLine) a), ((PgBox) b));
        if (a instanceof PgBox && b instanceof PgLine) return lineIntersectsBox(((PgLine) b), ((PgBox) a));
        if (a instanceof PgLseg && b instanceof PgBox) return lsegIntersectsBox(((PgLseg) a), ((PgBox) b));
        if (a instanceof PgBox && b instanceof PgLseg) return lsegIntersectsBox(((PgLseg) b), ((PgBox) a));
        if (a instanceof PgPath && b instanceof PgPath) return pathIntersectsPath(((PgPath) a), ((PgPath) b));
        throw new MemgresException("operator does not exist: " + pgTypeName(a) + " ?# " + pgTypeName(b), "42883");
    }

    // ========================================================================
    // Closest point (## operator)
    // ========================================================================

    public static PgPoint closestPointOnLseg(PgPoint p, PgLseg seg) {
        double dx = seg.p2.x - seg.p1.x;
        double dy = seg.p2.y - seg.p1.y;
        double lenSq = dx * dx + dy * dy;
        if (lenSq < EPSILON) return seg.p1;
        double t = ((p.x - seg.p1.x) * dx + (p.y - seg.p1.y) * dy) / lenSq;
        t = Math.max(0, Math.min(1, t));
        return new PgPoint(seg.p1.x + t * dx, seg.p1.y + t * dy);
    }

    public static PgPoint closestPointOnLine(PgPoint p, PgLine line) {
        double denom = line.a * line.a + line.b * line.b;
        if (denom < EPSILON) return p;
        double x = (line.b * (line.b * p.x - line.a * p.y) - line.a * line.c) / denom;
        double y = (line.a * (line.a * p.y - line.b * p.x) - line.b * line.c) / denom;
        return new PgPoint(x, y);
    }

    public static PgPoint closestPointOnBox(PgPoint p, PgBox box) {
        if (boxContainsPoint(box, p)) return p;
        double cx = Math.max(box.low.x, Math.min(p.x, box.high.x));
        double cy = Math.max(box.low.y, Math.min(p.y, box.high.y));
        return new PgPoint(cx, cy);
    }

    /**
     * The closest point on {@code on} to the segment {@code to}, and the distance to it, following
     * the order PostgreSQL's own lseg_closept_lseg tests its four candidates in. The order decides
     * the answer whenever two candidates are the same distance away, which is why it is kept: a box
     * edge that ties with a later one has to win, or {@code lseg ## box} names a different corner
     * than PostgreSQL does.
     */
    private static double closestOnLsegToLseg(PgLseg on, PgLseg to, PgPoint[] result) {
        PgPoint ip = lsegIntersectionPoint(on, to);
        if (ip != null) {
            result[0] = ip;
            return 0.0;
        }
        PgPoint best = closestPointOnLseg(to.p1, on);
        double dist = distancePointPoint(to.p1, best);
        PgPoint cand = closestPointOnLseg(to.p2, on);
        double d = distancePointPoint(to.p2, cand);
        if (d < dist) { dist = d; best = cand; }
        d = distancePointLseg(on.p1, to);
        if (d < dist) { dist = d; best = on.p1; }
        d = distancePointLseg(on.p2, to);
        if (d < dist) { dist = d; best = on.p2; }
        result[0] = best;
        return dist;
    }

    /** Whether two segments run in the same direction, which is what makes {@code ##} answer NULL. */
    private static boolean lsegsParallel(PgLseg a, PgLseg b) {
        double cross = (a.p2.x - a.p1.x) * (b.p2.y - b.p1.y)
                - (a.p2.y - a.p1.y) * (b.p2.x - b.p1.x);
        return Math.abs(cross) < EPSILON;
    }

    /**
     * {@code lseg ## lseg} — close_lseg. PostgreSQL has no answer for two segments that run
     * parallel, whether they are apart, touching or lying on top of each other, and says NULL;
     * otherwise it answers with the point of {@code on} nearest {@code to}.
     */
    private static PgPoint closestOnLsegFromLseg(PgLseg on, PgLseg to) {
        if (lsegsParallel(on, to)) return null;
        PgPoint[] result = new PgPoint[1];
        closestOnLsegToLseg(on, to, result);
        return result[0];
    }

    /**
     * {@code lseg ## box} — close_sb. A segment that meets the box at all is answered with the
     * point of the segment nearest the box's centre, which is what PostgreSQL does once it has
     * decided the distance is zero; a segment clear of the box is answered with a point on the box,
     * found by walking its four edges in PostgreSQL's order and keeping the first strict minimum.
     */
    private static PgPoint closestOnBoxFromLseg(PgBox box, PgLseg seg) {
        if (lsegIntersectsBox(seg, box)) {
            PgPoint centre = new PgPoint((box.low.x + box.high.x) / 2, (box.low.y + box.high.y) / 2);
            return closestPointOnLseg(centre, seg);
        }
        PgLseg[] edges = new PgLseg[]{
                new PgLseg(box.low, new PgPoint(box.low.x, box.high.y)),
                new PgLseg(box.low, new PgPoint(box.high.x, box.low.y)),
                new PgLseg(box.high, new PgPoint(box.low.x, box.high.y)),
                new PgLseg(box.high, new PgPoint(box.high.x, box.low.y))
        };
        PgPoint best = null;
        double bestDist = Double.MAX_VALUE;
        PgPoint[] result = new PgPoint[1];
        for (PgLseg edge : edges) {
            double d = closestOnLsegToLseg(edge, seg, result);
            if (d < bestDist) { bestDist = d; best = result[0]; }
        }
        return best;
    }

    /**
     * {@code line ## lseg} — close_ls. A line parallel to the segment has no nearest point on it in
     * PostgreSQL and answers NULL; a line that crosses the segment answers with the crossing, and
     * one that does not answers with the segment endpoint nearer the line.
     */
    private static PgPoint closestOnLsegFromLine(PgLseg seg, PgLine line) {
        double dx = seg.p2.x - seg.p1.x;
        double dy = seg.p2.y - seg.p1.y;
        if (Math.abs(line.a * dx + line.b * dy) < EPSILON) return null;
        double v1 = line.a * seg.p1.x + line.b * seg.p1.y + line.c;
        double v2 = line.a * seg.p2.x + line.b * seg.p2.y + line.c;
        if ((v1 <= 0 && v2 >= 0) || (v1 >= 0 && v2 <= 0)) {
            double t = v1 / (v1 - v2);
            return new PgPoint(seg.p1.x + t * dx, seg.p1.y + t * dy);
        }
        return Math.abs(v1) < Math.abs(v2) ? seg.p1 : seg.p2;
    }

    public static PgPoint closestPoint(Object a, Object b) {
        // PG's ## operator never accepts a point as the second operand: the
        // result is the closest point that lies ON the second object. It has no
        // reversed spellings either -- box ## lseg is no operator at all.
        if (a instanceof PgPoint && b instanceof PgLseg) return closestPointOnLseg(((PgPoint) a), ((PgLseg) b));
        if (a instanceof PgPoint && b instanceof PgLine) return closestPointOnLine(((PgPoint) a), ((PgLine) b));
        if (a instanceof PgPoint && b instanceof PgBox) return closestPointOnBox(((PgPoint) a), ((PgBox) b));
        if (a instanceof PgLseg && b instanceof PgLseg) return closeLseg((PgLseg) a, (PgLseg) b);
        if (a instanceof PgLseg && b instanceof PgBox) return closeSb((PgLseg) a, (PgBox) b);
        if (a instanceof PgLine && b instanceof PgLseg) return closeLs((PgLine) a, (PgLseg) b);
        throw new MemgresException("operator does not exist: " + pgTypeName(a) + " ## " + pgTypeName(b), "42883");
    }

    // ========================================================================
    // PostgreSQL's own closest-point arithmetic
    //
    // The two-segment and segment-box cases are not "the nearest point of the
    // shapes" in the way geometry would define it, and the way PostgreSQL
    // arrives at its answer decides both which point it names when several are
    // equally near and the last bits of the coordinates it prints. So these
    // follow geo_ops.c step for step: the same epsilon comparisons, the same
    // line coefficients, and the same order over the sides of a box.
    // ========================================================================

    private static boolean fpZero(double a) { return Math.abs(a) <= EPSILON; }

    /**
     * Whether two coordinates are the same coordinate.
     *
     * <p>Two not-a-numbers are the same coordinate here, which is not what subtracting them
     * says: a shape built from a value that is not a number is still that shape, and asking
     * whether it is itself has to answer yes.
     */
    private static boolean fpEq(double a, double b) {
        if (Double.isNaN(a) || Double.isNaN(b)) return Double.isNaN(a) && Double.isNaN(b);
        return a == b || Math.abs(a - b) <= EPSILON;
    }

    /** A point together with how far it was from whatever was measured to it. */
    private static final class PointDist {
        final PgPoint point;
        final double dist;
        PointDist(PgPoint point, double dist) { this.point = point; this.dist = dist; }
    }

    /** PostgreSQL's pg_hypot: the distance between two points. */
    private static double pgHypot(double x, double y) {
        double yx;
        x = Math.abs(x);
        y = Math.abs(y);
        if (x < y) { double swap = x; x = y; y = swap; }
        if (Double.isInfinite(x) || Double.isInfinite(y)) return Double.POSITIVE_INFINITY;
        if (Double.isNaN(x) || Double.isNaN(y)) return Double.NaN;
        if (x == 0) return 0;
        yx = y / x;
        return x * Math.sqrt(1.0 + (yx * yx));
    }

    private static double pointDt(PgPoint a, PgPoint b) { return pgHypot(a.x - b.x, a.y - b.y); }

    /** The slope of the line through two points; a vertical line is DBL_MAX. */
    private static double pointSl(PgPoint p1, PgPoint p2) {
        if (fpEq(p1.x, p2.x)) return Double.MAX_VALUE;
        if (fpEq(p1.y, p2.y)) return 0.0;
        return (p1.y - p2.y) / (p1.x - p2.x);
    }

    /** The slope of the perpendicular to the line through two points. */
    private static double pointInvsl(PgPoint p1, PgPoint p2) {
        if (fpEq(p1.x, p2.x)) return 0.0;
        if (fpEq(p1.y, p2.y)) return Double.MAX_VALUE;
        return (p1.x - p2.x) / (p2.y - p1.y);
    }

    private static double lsegSl(PgLseg l) { return pointSl(l.p1, l.p2); }

    /** The line through a point with a given slope, as PostgreSQL writes its coefficients. */
    private static PgLine lineConstruct(PgPoint pt, double m) {
        if (m == Double.MAX_VALUE) return new PgLine(-1.0, 0.0, pt.x);
        if (m == 0) return new PgLine(0.0, -1.0, pt.y);
        double c = pt.y - m * pt.x;
        if (c == 0.0) c = 0.0;
        return new PgLine(m, -1.0, c);
    }

    /** Where two lines meet, or null when they are parallel. */
    private static PgPoint lineInterptLine(PgLine l1, PgLine l2) {
        double x;
        double y;
        if (!fpZero(l1.b)) {
            if (fpEq(l2.a, l1.a / l1.b * l2.b)) return null;
            x = (l1.b * l2.c - l2.b * l1.c) / (l1.a * l2.b - l2.a * l1.b);
            y = -(l1.a * x + l1.c) / l1.b;
        } else if (!fpZero(l2.b)) {
            if (fpEq(l1.a, l2.a / l2.b * l1.b)) return null;
            x = (l2.b * l1.c - l1.b * l2.c) / (l2.a * l1.b - l1.a * l2.b);
            y = -(l2.a * x + l2.c) / l2.b;
        } else {
            return null;
        }
        // The arithmetic above tends to produce a negative zero, which PostgreSQL writes back
        // as a plain zero before it hands the point out.
        if (x == 0.0) x = 0.0;
        if (y == 0.0) y = 0.0;
        return new PgPoint(x, y);
    }

    /** Whether a point lies on a segment: the two part lengths add up to the whole. */
    private static boolean lsegContainPoint(PgLseg lseg, PgPoint pt) {
        return fpEq(pointDt(lseg.p1, pt) + pointDt(lseg.p2, pt), pointDt(lseg.p1, lseg.p2));
    }

    private static boolean pointEqPoint(PgPoint a, PgPoint b) {
        return fpEq(a.x, b.x) && fpEq(a.y, b.y);
    }

    /** Where a line crosses a segment, or null when it misses it. */
    private static PgPoint lsegInterptLine(PgLseg lseg, PgLine line) {
        PgPoint interpt = lineInterptLine(lineConstruct(lseg.p1, lsegSl(lseg)), line);
        if (interpt == null) return null;
        if (!lsegContainPoint(lseg, interpt)) return null;
        // An endpoint that is only an epsilon away is the endpoint itself, not the
        // rounding residue the arithmetic produced.
        if (pointEqPoint(lseg.p1, interpt)) return lseg.p1;
        if (pointEqPoint(lseg.p2, interpt)) return lseg.p2;
        return interpt;
    }

    /** Where two segments cross, as a point on the first, or null when they do not. */
    private static PgPoint lsegInterptLseg(PgLseg l1, PgLseg l2) {
        PgPoint interpt = lsegInterptLine(l1, lineConstruct(l2.p1, lsegSl(l2)));
        if (interpt == null) return null;
        if (!lsegContainPoint(l2, interpt)) return null;
        return interpt;
    }

    /** The point of a segment nearest a given point, with the distance to it. */
    private static PointDist lsegCloseptPoint(PgLseg lseg, PgPoint pt) {
        PgPoint closept = lsegInterptLine(lseg, lineConstruct(pt, pointInvsl(lseg.p1, lseg.p2)));
        if (closept == null) {
            // The perpendicular misses the segment, so an endpoint is nearest.
            closept = pointDt(lseg.p1, pt) < pointDt(lseg.p2, pt) ? lseg.p1 : lseg.p2;
        }
        return new PointDist(closept, pointDt(pt, closept));
    }

    /** The point of {@code on} nearest the segment {@code to}, with the distance to it. */
    private static PointDist lsegCloseptLseg(PgLseg on, PgLseg to) {
        PgPoint crossing = lsegInterptLseg(on, to);
        if (crossing != null) return new PointDist(crossing, 0.0);

        PointDist best = lsegCloseptPoint(on, to.p1);
        PointDist other = lsegCloseptPoint(on, to.p2);
        if (other.dist < best.dist) best = other;

        // The nearest point can still be an endpoint of the segment we are on.
        double d = lsegCloseptPoint(to, on.p1).dist;
        if (d < best.dist) best = new PointDist(on.p1, d);
        d = lsegCloseptPoint(to, on.p2).dist;
        if (d < best.dist) best = new PointDist(on.p2, d);
        return best;
    }

    /**
     * {@code lseg ## lseg}: the point of the second segment nearest the first. Two segments of
     * the same slope have no answer at all in PostgreSQL, which counts a segment whose endpoints
     * coincide as vertical, so two of those are parallel to each other and to any vertical.
     */
    private static PgPoint closeLseg(PgLseg l1, PgLseg l2) {
        if (lsegSl(l1) == lsegSl(l2)) return null;
        return lsegCloseptLseg(l2, l1).point;
    }

    /**
     * The four sides of a box as close_sb walks them: round the perimeter from the low corner,
     * left first. Both the order of the sides and the order of the two ends of each side decide
     * which point is named when two of them are equally near.
     */
    private static PgLseg[] pgBoxSides(PgBox box) {
        PgPoint topLeft = new PgPoint(box.low.x, box.high.y);
        PgPoint bottomRight = new PgPoint(box.high.x, box.low.y);
        return new PgLseg[]{
                new PgLseg(box.low, topLeft),        // left
                new PgLseg(topLeft, box.high),       // top
                new PgLseg(box.high, bottomRight),   // right
                new PgLseg(bottomRight, box.low)     // bottom
        };
    }

    /** The slope of a line written as its coefficients; a vertical line is DBL_MAX. */
    private static double lineSl(PgLine line) {
        if (fpZero(line.a)) return 0.0;
        if (fpZero(line.b)) return Double.MAX_VALUE;
        return line.a / -line.b;
    }

    /** The slope of the perpendicular to a line. */
    private static double lineInvsl(PgLine line) {
        if (fpZero(line.a)) return Double.MAX_VALUE;
        if (fpZero(line.b)) return 0.0;
        return line.b / line.a;
    }

    /** How far a point is from a line, the perpendicular never being parallel to it. */
    private static double lineCloseptDist(PgLine line, PgPoint point) {
        PgPoint closept = lineInterptLine(lineConstruct(point, lineInvsl(line)), line);
        if (closept == null) return pointDt(point, point);
        return pointDt(point, closept);
    }

    /**
     * {@code line ## lseg}: the point of the segment nearest the line, or no point at all when
     * the two run at the same slope. PostgreSQL has no operator the other way round.
     */
    private static PgPoint closeLs(PgLine line, PgLseg lseg) {
        if (lsegSl(lseg) == lineSl(line)) return null;
        PgPoint crossing = lsegInterptLine(lseg, line);
        if (crossing != null) return crossing;
        return lineCloseptDist(line, lseg.p1) < lineCloseptDist(line, lseg.p2) ? lseg.p1 : lseg.p2;
    }

    /** Whether a segment meets a box at all: PostgreSQL's inter_sb. */
    private static boolean lsegInterBox(PgLseg lseg, PgBox box) {
        double lowX = Math.min(lseg.p1.x, lseg.p2.x);
        double lowY = Math.min(lseg.p1.y, lseg.p2.y);
        double highX = Math.max(lseg.p1.x, lseg.p2.x);
        double highY = Math.max(lseg.p1.y, lseg.p2.y);
        if (lowX > box.high.x || highX < box.low.x || lowY > box.high.y || highY < box.low.y) {
            return false;
        }
        if (boxContainsPoint(box, lseg.p1) || boxContainsPoint(box, lseg.p2)) return true;
        for (PgLseg side : pgBoxSides(box)) {
            if (lsegInterptLseg(side, lseg) != null) return true;
        }
        return false;
    }

    /**
     * {@code lseg ## box}: a segment that meets the box is answered with its own point nearest
     * the centre of the box -- a point inside the box, not on it -- and one that misses with the
     * point of the nearest side. Sides at the same distance are settled by the order above.
     */
    private static PgPoint closeSb(PgLseg lseg, PgBox box) {
        if (lsegInterBox(lseg, box)) {
            PgPoint centre = new PgPoint((box.high.x + box.low.x) / 2.0, (box.high.y + box.low.y) / 2.0);
            return lsegCloseptPoint(lseg, centre).point;
        }
        PgPoint result = null;
        double dist = -1;
        for (PgLseg side : pgBoxSides(box)) {
            PointDist candidate = lsegCloseptLseg(side, lseg);
            if (dist < 0 || candidate.dist < dist) {
                dist = candidate.dist;
                result = candidate.point;
            }
        }
        return result;
    }

    // ========================================================================
    // Intersection point (# operator)
    // ========================================================================

    /**
     * Where two lines cross, worked out the way PostgreSQL works it out.
     *
     * <p>Solving by the determinant gives the same point, and gives it with different signs on
     * the zeros: the crossing of {@code {1,-1,0}} and {@code {1,1,0}} came out
     * {@code (-0,0)} where a real server names {@code (0,0)}. A negative zero is a value a
     * client can see, so the arithmetic is done in the same order.
     */
    public static PgPoint lineLineIntersection(PgLine a, PgLine b) {
        if (!fpZero(a.b)) {
            if (fpEq(b.a * a.b, a.a * b.b)) return null; // parallel
            double x = (a.b * b.c - b.b * a.c) / (b.b * a.a - a.b * b.a);
            double y = -(a.a * x + a.c) / a.b;
            return new PgPoint(x, y);
        }
        if (!fpZero(b.b)) {
            if (fpEq(a.a * b.b, b.a * a.b)) return null;
            double x = (b.b * a.c - a.b * b.c) / (a.b * b.a - b.b * a.a);
            double y = -(b.a * x + b.c) / b.b;
            return new PgPoint(x, y);
        }
        // Both are vertical, so they are parallel or the same line.
        return null;
    }

    public static Object intersectionGeneral(Object a, Object b) {
        if (a instanceof PgBox && b instanceof PgBox) return boxIntersection(((PgBox) a), ((PgBox) b));
        return intersection(a, b);
    }

    /** Box intersection: returns the overlapping region as a box, or null if no overlap. */
    public static PgBox boxIntersection(PgBox a, PgBox b) {
        double lowX = Math.max(a.low.x, b.low.x);
        double lowY = Math.max(a.low.y, b.low.y);
        double highX = Math.min(a.high.x, b.high.x);
        double highY = Math.min(a.high.y, b.high.y);
        if (lowX > highX || lowY > highY) return null;
        return new PgBox(new PgPoint(highX, highY), new PgPoint(lowX, lowY));
    }

    public static PgPoint intersection(Object a, Object b) {
        if (a instanceof PgLseg && b instanceof PgLseg) return lsegIntersectionPoint(((PgLseg) a), ((PgLseg) b));
        if (a instanceof PgLine && b instanceof PgLine) return lineLineIntersection(((PgLine) a), ((PgLine) b));
        if (a instanceof PgLine && b instanceof PgLseg) {
            PgLseg lb = (PgLseg) b;
            PgLine la = (PgLine) a;
            PgLine lb2 = lineFromPoints(lb.p1, lb.p2);
            PgPoint ip = lineLineIntersection(la, lb2);
            if (ip == null) return null;
            // Check if ip is on the segment
            if (onSegment(ip, lb)) return ip;
            return null;
        }
        if (a instanceof PgLseg && b instanceof PgLine) return intersection(b, a);
        throw new MemgresException("intersection not supported between " + pgTypeName(a) + " and " + pgTypeName(b), "42883");
    }

    private static boolean onSegment(PgPoint p, PgLseg seg) {
        double minX = Math.min(seg.p1.x, seg.p2.x) - EPSILON;
        double maxX = Math.max(seg.p1.x, seg.p2.x) + EPSILON;
        double minY = Math.min(seg.p1.y, seg.p2.y) - EPSILON;
        double maxY = Math.max(seg.p1.y, seg.p2.y) + EPSILON;
        return p.x >= minX && p.x <= maxX && p.y >= minY && p.y <= maxY;
    }

    // ========================================================================
    // Positional predicates (strict left/right/above/below, extends)
    // ========================================================================

    private static PgBox toBBox(Object geom) {
        if (geom instanceof PgBox) return ((PgBox) geom);
        if (geom instanceof PgPoint) return new PgBox(((PgPoint) geom), ((PgPoint) geom));
        if (geom instanceof PgCircle) return boundBox(((PgCircle) geom));
        return boundBox(geom);
    }

    /** << strictly left */
    public static boolean isStrictlyLeft(Object a, Object b) {
        PgBox ba = toBBox(a), bb = toBBox(b);
        return ba.high.x < bb.low.x;
    }

    /** >> strictly right */
    public static boolean isStrictlyRight(Object a, Object b) {
        PgBox ba = toBBox(a), bb = toBBox(b);
        return ba.low.x > bb.high.x;
    }

    /** <<| strictly below */
    public static boolean isStrictlyBelow(Object a, Object b) {
        PgBox ba = toBBox(a), bb = toBBox(b);
        return ba.high.y < bb.low.y;
    }

    /** |>> strictly above */
    public static boolean isStrictlyAbove(Object a, Object b) {
        PgBox ba = toBBox(a), bb = toBBox(b);
        return ba.low.y > bb.high.y;
    }

    /** &< does not extend to the right of */
    public static boolean doesNotExtendRight(Object a, Object b) {
        PgBox ba = toBBox(a), bb = toBBox(b);
        return ba.high.x <= bb.high.x;
    }

    /** &> does not extend to the left of */
    public static boolean doesNotExtendLeft(Object a, Object b) {
        PgBox ba = toBBox(a), bb = toBBox(b);
        return ba.low.x >= bb.low.x;
    }

    /** &<| does not extend above */
    public static boolean doesNotExtendAbove(Object a, Object b) {
        PgBox ba = toBBox(a), bb = toBBox(b);
        return ba.high.y <= bb.high.y;
    }

    /** |&> does not extend below */
    public static boolean doesNotExtendBelow(Object a, Object b) {
        PgBox ba = toBBox(a), bb = toBBox(b);
        return ba.low.y >= bb.low.y;
    }

    // ========================================================================
    // Orientation predicates
    // ========================================================================

    public static boolean isHorizontal(PgLseg seg) {
        return Math.abs(seg.p1.y - seg.p2.y) < EPSILON;
    }

    public static boolean isHorizontal(PgPoint a, PgPoint b) {
        return Math.abs(a.y - b.y) < EPSILON;
    }

    public static boolean isVertical(PgLseg seg) {
        return Math.abs(seg.p1.x - seg.p2.x) < EPSILON;
    }

    public static boolean isVertical(PgPoint a, PgPoint b) {
        return Math.abs(a.x - b.x) < EPSILON;
    }

    /** Does the string look like a point literal, e.g. "(1,2)"? */
    public static boolean isPointString(String s) {
        if (s == null) return false;
        s = s.trim();
        if (!s.startsWith("(")) return false;
        try { return autoDetectPublic(s) instanceof PgPoint; } catch (RuntimeException e) { return false; }
    }

    /** point ?- point : true if the two points are horizontally aligned (same y). */
    public static boolean pointsHorizontallyAligned(String a, String b) {
        Object oa = autoDetectPublic(a), ob = autoDetectPublic(b);
        if (oa instanceof PgPoint && ob instanceof PgPoint) return isHorizontal((PgPoint) oa, (PgPoint) ob);
        throw new MemgresException("operator does not exist: " + pgTypeName(oa) + " ?- " + pgTypeName(ob), "42883");
    }

    /** point ?| point : true if the two points are vertically aligned (same x). */
    public static boolean pointsVerticallyAligned(String a, String b) {
        Object oa = autoDetectPublic(a), ob = autoDetectPublic(b);
        if (oa instanceof PgPoint && ob instanceof PgPoint) return isVertical((PgPoint) oa, (PgPoint) ob);
        throw new MemgresException("operator does not exist: " + pgTypeName(oa) + " ?| " + pgTypeName(ob), "42883");
    }

    public static boolean isPerpendicular(PgLseg a, PgLseg b) {
        double dx1 = a.p2.x - a.p1.x, dy1 = a.p2.y - a.p1.y;
        double dx2 = b.p2.x - b.p1.x, dy2 = b.p2.y - b.p1.y;
        return Math.abs(dx1 * dx2 + dy1 * dy2) < EPSILON;
    }

    public static boolean isParallel(PgLseg a, PgLseg b) {
        double dx1 = a.p2.x - a.p1.x, dy1 = a.p2.y - a.p1.y;
        double dx2 = b.p2.x - b.p1.x, dy2 = b.p2.y - b.p1.y;
        return Math.abs(dx1 * dy2 - dy1 * dx2) < EPSILON;
    }

    public static boolean isParallel(PgLine a, PgLine b) {
        // Lines {a1,b1,c1} and {a2,b2,c2} are parallel if a1*b2 - a2*b1 == 0
        return Math.abs(a.a * b.b - b.a * a.b) < EPSILON;
    }

    // ========================================================================
    // Type conversion functions
    // ========================================================================

    // toPoint

    public static PgPoint toPoint(PgCircle c) { return c.center; }
    public static PgPoint toPoint(PgBox b) { return center(b); }
    public static PgPoint toPoint(PgLseg s) { return center(s); }
    public static PgPoint toPoint(PgPolygon p) { return center(p); }

    // toBox

    public static PgBox toBox(PgPoint p1, PgPoint p2) {
        return normalizeBox(p1, p2);
    }

    public static PgBox toBox(PgCircle c) {
        // PG: box(circle) returns the inscribed square, not the bounding box.
        // The inscribed square has side = r * sqrt(2), half-side = r / sqrt(2).
        double half = c.radius / Math.sqrt(2.0);
        return new PgBox(
            new PgPoint(c.center.x + half, c.center.y + half),
            new PgPoint(c.center.x - half, c.center.y - half)
        );
    }

    public static PgBox toBox(PgPolygon p) {
        return boundBox(p);
    }

    // toCircle

    public static PgCircle toCircle(PgPoint center, double radius) {
        return new PgCircle(center, radius);
    }

    /** Circumscribed circle of a box (PG behavior: half-diagonal). */
    public static PgCircle toCircle(PgBox b) {
        PgPoint c = center(b);
        double r = distancePointPoint(b.high, b.low) / 2;
        return new PgCircle(c, r);
    }

    /**
     * The circle a polygon stands for: centred on the mean of its vertices, with the mean
     * distance from there to them as the radius.
     *
     * <p>The furthest vertex is not the radius PostgreSQL computes, and using it made the circle
     * larger than the one a real server names for every polygon that is not a regular one.
     */
    public static PgCircle toCircle(PgPolygon poly) {
        PgPoint c = center(poly);
        double total = 0;
        for (PgPoint p : poly.points) {
            total += distancePointPoint(c, p);
        }
        return new PgCircle(c, poly.points.isEmpty() ? 0 : total / poly.points.size());
    }

    // toLseg

    public static PgLseg toLseg(PgBox b) { return diagonal(b); }
    public static PgLseg toLseg(PgPoint p1, PgPoint p2) { return new PgLseg(p1, p2); }

    // toLine

    public static PgLine toLine(PgLseg seg) {
        return lineFromPoints(seg.p1, seg.p2);
    }

    // toPolygon

    public static PgPolygon toPolygon(PgBox b) {
        return new PgPolygon(Cols.listOf(
            b.low,
            new PgPoint(b.low.x, b.high.y),
            b.high,
            new PgPoint(b.high.x, b.low.y)
        ));
    }

    /** Approximate circle as a 12-point polygon. */
    public static PgPolygon toPolygon(PgCircle c) {
        return toPolygon(c, 12);
    }

    public static PgPolygon toPolygon(PgCircle c, int npts) {
        // Match PG's circle_poly: first vertex at (cx - r, cy), winding as
        // x = cx - cos(i*step)*r, y = cy + sin(i*step)*r.
        List<PgPoint> pts = new ArrayList<>();
        double anglestep = (2.0 * Math.PI) / npts;
        for (int i = 0; i < npts; i++) {
            double angle = i * anglestep;
            pts.add(new PgPoint(
                c.center.x - Math.cos(angle) * c.radius,
                c.center.y + Math.sin(angle) * c.radius
            ));
        }
        return new PgPolygon(Cols.listCopyOf(pts));
    }

    public static PgPolygon toPolygon(PgPath path) {
        if (!path.closed) {
            throw new MemgresException("open path cannot be converted to polygon", "22P02");
        }
        return new PgPolygon(path.points);
    }

    // toPath

    public static PgPath toPath(PgPolygon poly) {
        return new PgPath(poly.points, true);
    }

    // ========================================================================
    // String-based bridge methods (called from AstExecutor/FunctionEvaluator)
    // ========================================================================

    /**
     * Detect if a string looks like a geometric literal.
     * Geometric strings start with '(' for point/box/polygon/closed-path,
     * '[' for lseg/open-path, '<' for circle, '{' for line.
     */
    /** One coefficient of a line, in any spelling PostgreSQL's float8 input accepts. */
    private static final String NUMBER = "[-+]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][-+]?\\d+)?";

    public static boolean isGeometricString(String s) {
        if (s == null || s.isEmpty()) return false;
        s = s.trim();
        if (s.isEmpty()) return false;
        char c = s.charAt(0);
        // '(' could be point, box, polygon, or closed path; check for numeric content
        if (c == '(') {
            // Geometric: ((x,y),...) or (x,y)
            return s.matches("\\(\\s*[(-]?[\\d.].*");
        }
        if (c == '[') {
            // lseg or open path: [(x,y),(x,y)]
            return s.contains("(");
        }
        if (c == '<') {
            // circle: <(x,y),r>
            return s.contains("(") && s.endsWith(">");
        }
        if (c == '{') {
            // A line is {A,B,C} — three coefficients, no more and no fewer. Accepting any
            // brace-wrapped list of numbers claimed every integer array as a line, so a join on
            // "a.ar @> b.ar" over two int[] columns tried to read {1,2} as one and refused it.
            return s.matches("\\{\\s*" + NUMBER + "\\s*,\\s*" + NUMBER + "\\s*,\\s*" + NUMBER + "\\s*\\}");
        }
        return false;
    }

    /**
     * Auto-detect geometric type from string and parse it.
     */
    public static Object autoDetectPublic(String s) {
        return autoDetect(s);
    }

    private static Object autoDetect(String s) {
        s = s.trim();
        if (s.startsWith("<")) return parseCircle(s);
        if (s.startsWith("{")) return parseLine(s);
        if (s.startsWith("[")) {
            // lseg has exactly 2 points, open path has more
            List<PgPoint> pts = parsePointList(s);
            if (pts.size() == 2) return new PgLseg(pts.get(0), pts.get(1));
            return new PgPath(pts, false);
        }
        // '(' could be point, box, polygon, or closed path
        List<PgPoint> pts = parsePointList(s);
        if (pts.size() == 1) return pts.get(0);
        if (pts.size() == 2) {
            // Could be box or lseg, default to box (PG convention for '((x,y),(x,y))')
            return normalizeBox(pts.get(0), pts.get(1));
        }
        // 3+ points = polygon or closed path
        return new PgPolygon(pts);
    }

    /**
     * Two paths joined end to end. PostgreSQL's path concatenation refuses a closed path -- a
     * loop has no free end to attach anything to -- and answers NULL rather than a shape, so a
     * closed path plus anything is NULL and never the translation a path-plus-point would give.
     */
    public static String pathConcat(String left, String right) {
        PgPath a = parsePath(left);
        PgPath b = parsePath(right);
        if (a.closed || b.closed) return null;
        List<PgPoint> pts = new ArrayList<>(a.points);
        pts.addAll(b.points);
        return format(new PgPath(pts, false));
    }

    // String-based arithmetic: geom + point, geom - point, geom * point, geom / point
    public static String add(String left, String right) {
        Object lGeom = autoDetect(left);
        PgPoint rPt = parsePoint(right);
        return format(addGeom(lGeom, rPt));
    }

    public static String subtract(String left, String right) {
        Object lGeom = autoDetect(left);
        PgPoint rPt = parsePoint(right);
        return format(subtractGeom(lGeom, rPt));
    }

    public static String multiply(String left, String right) {
        Object lGeom = autoDetect(left);
        PgPoint rPt = parsePoint(right);
        return format(multiplyGeom(lGeom, rPt));
    }

    public static String divide(String left, String right) {
        Object lGeom = autoDetect(left);
        PgPoint rPt = parsePoint(right);
        return format(divideGeom(lGeom, rPt));
    }

    private static Object addGeom(Object geom, PgPoint p) {
        if (geom instanceof PgPoint) return pointAdd(((PgPoint) geom), p);
        if (geom instanceof PgBox) return boxAdd(((PgBox) geom), p);
        if (geom instanceof PgCircle) return circleAdd(((PgCircle) geom), p);
        if (geom instanceof PgPath) return pathAdd(((PgPath) geom), p);
        if (geom instanceof PgPolygon) return new PgPolygon(((PgPolygon) geom).points.stream()
                .map(pt -> pointAdd(pt, p)).collect(Collectors.toList()));
        if (geom instanceof PgLseg) return new PgLseg(pointAdd(((PgLseg) geom).p1, p), pointAdd(((PgLseg) geom).p2, p));
        return geom;
    }

    private static Object subtractGeom(Object geom, PgPoint p) {
        if (geom instanceof PgPoint) return pointSub(((PgPoint) geom), p);
        if (geom instanceof PgBox) return boxSub(((PgBox) geom), p);
        if (geom instanceof PgCircle) return circleSub(((PgCircle) geom), p);
        if (geom instanceof PgPath) return pathSub(((PgPath) geom), p);
        if (geom instanceof PgPolygon) return new PgPolygon(((PgPolygon) geom).points.stream()
                .map(pt -> pointSub(pt, p)).collect(Collectors.toList()));
        if (geom instanceof PgLseg) return new PgLseg(pointSub(((PgLseg) geom).p1, p), pointSub(((PgLseg) geom).p2, p));
        return geom;
    }

    private static Object multiplyGeom(Object geom, PgPoint p) {
        if (geom instanceof PgPoint) return pointMul(((PgPoint) geom), p);
        if (geom instanceof PgBox) return boxMul(((PgBox) geom), p);
        if (geom instanceof PgCircle) return circleMul(((PgCircle) geom), p);
        if (geom instanceof PgPath) return pathMul(((PgPath) geom), p);
        if (geom instanceof PgPolygon) return new PgPolygon(((PgPolygon) geom).points.stream()
                .map(pt -> pointMul(pt, p)).collect(Collectors.toList()));
        if (geom instanceof PgLseg) return new PgLseg(pointMul(((PgLseg) geom).p1, p), pointMul(((PgLseg) geom).p2, p));
        return geom;
    }

    private static Object divideGeom(Object geom, PgPoint p) {
        if (geom instanceof PgPoint) return pointDiv(((PgPoint) geom), p);
        if (geom instanceof PgBox) return boxDiv(((PgBox) geom), p);
        if (geom instanceof PgCircle) return circleDiv(((PgCircle) geom), p);
        if (geom instanceof PgPath) return pathDiv(((PgPath) geom), p);
        if (geom instanceof PgPolygon) return new PgPolygon(((PgPolygon) geom).points.stream()
                .map(pt -> pointDiv(pt, p)).collect(Collectors.toList()));
        if (geom instanceof PgLseg) return new PgLseg(pointDiv(((PgLseg) geom).p1, p), pointDiv(((PgLseg) geom).p2, p));
        return geom;
    }

    // String-based function bridges
    public static double length(String s) {
        // Try as lseg first (2 points in brackets), then as path
        s = s.trim();
        if (s.startsWith("[") || s.startsWith("(")) {
            // Try path (length is defined for lseg and path, not polygon)
            try {
                PgPath path = parsePath(s);
                return length(path);
            } catch (Exception e) {
                // try lseg
            }
            try {
                PgLseg seg = parseLseg(s);
                return length(seg);
            } catch (Exception e) {
                // fallback
            }
        }
        Object geom = autoDetect(s);
        return length(geom);
    }
    public static double diameter(String s) { return diameter(parseCircle(s)); }
    public static double radius(String s) { return radius(parseCircle(s)); }
    public static double height(String s) { return height(parseBox(s)); }
    public static double width(String s) { return width(parseBox(s)); }
    public static boolean isclosed(String s) { return isclosed(parsePath(s)); }
    public static boolean isopen(String s) { return isopen(parsePath(s)); }
    public static String pclose(String s) { return format(pclose(parsePath(s))); }
    public static String popen(String s) { return format(popen(parsePath(s))); }
    public static String diagonal(String s) { return format(diagonal(parseBox(s))); }
    public static PgPoint center(String s) {
        Object geom = autoDetect(s);
        return center(geom);
    }
    public static int npoints(String s) {
        Object geom = autoDetect(s);
        return npoints(geom);
    }
    public static Double area(String s) {
        Object geom = autoDetect(s);
        return area(geom);
    }
    public static double slope(String s1, String s2) {
        if (s2 != null) {
            return slope(parsePoint(s1), parsePoint(s2));
        }
        return slope(parseLseg(s1));
    }
    public static String boundBox(String s1, String s2) {
        if (s2 != null) {
            Object g1 = autoDetect(s1);
            Object g2 = autoDetect(s2);
            PgBox b1 = boundBox(g1);
            PgBox b2 = boundBox(g2);
            double hx = Math.max(b1.high.x, b2.high.x);
            double hy = Math.max(b1.high.y, b2.high.y);
            double lx = Math.min(b1.low.x, b2.low.x);
            double ly = Math.min(b1.low.y, b2.low.y);
            return format(new PgBox(new PgPoint(hx, hy), new PgPoint(lx, ly)));
        }
        return format(boundBox(autoDetect(s1)));
    }

    // String-based type conversion bridges
    public static PgPoint toPoint(String s) {
        Object geom = autoDetect(s);
        if (geom instanceof PgPoint) return ((PgPoint) geom);
        if (geom instanceof PgCircle) return toPoint(((PgCircle) geom));
        if (geom instanceof PgBox) return toPoint(((PgBox) geom));
        if (geom instanceof PgLseg) return toPoint(((PgLseg) geom));
        if (geom instanceof PgPolygon) return toPoint(((PgPolygon) geom));
        return parsePoint(s);
    }

    public static PgBox toBox(String s1, String s2) {
        if (s2 != null) {
            PgPoint p1 = parsePoint(s1);
            PgPoint p2 = parsePoint(s2);
            return toBox(p1, p2);
        }
        Object geom = autoDetect(s1);
        if (geom instanceof PgBox) return ((PgBox) geom);
        if (geom instanceof PgCircle) return toBox(((PgCircle) geom));
        if (geom instanceof PgPolygon) return toBox(((PgPolygon) geom));
        return parseBox(s1);
    }

    public static PgCircle toCircle(String s) {
        Object geom = autoDetect(s);
        if (geom instanceof PgCircle) return ((PgCircle) geom);
        if (geom instanceof PgBox) return toCircle(((PgBox) geom));
        if (geom instanceof PgPolygon) return toCircle(((PgPolygon) geom));
        return parseCircle(s);
    }

    public static String toPolygon(String s) {
        Object geom = autoDetect(s);
        if (geom instanceof PgPolygon) return format(((PgPolygon) geom));
        if (geom instanceof PgBox) return format(toPolygon(((PgBox) geom)));
        if (geom instanceof PgCircle) return format(toPolygon(((PgCircle) geom)));
        if (geom instanceof PgPath) return format(toPolygon(((PgPath) geom)));
        return format(parsePolygon(s));
    }

    public static String toPath(String s) {
        Object geom = autoDetect(s);
        if (geom instanceof PgPath) return format(((PgPath) geom));
        if (geom instanceof PgPolygon) return format(toPath(((PgPolygon) geom)));
        return format(parsePath(s));
    }

    // String-based distance (for operator <->)
    public static double distance(String a, String b) {
        return distance((Object) autoDetect(a), (Object) autoDetect(b));
    }

    // String-based predicates
    public static boolean sameAs(String a, String b) {
        return sameAs((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean isStrictlyBelow(String a, String b) {
        return isStrictlyBelow((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean isStrictlyAbove(String a, String b) {
        return isStrictlyAbove((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean isStrictlyLeft(String a, String b) {
        return isStrictlyLeft((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean isStrictlyRight(String a, String b) {
        return isStrictlyRight((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean contains(String a, String b) {
        return contains((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean overlaps(String a, String b) {
        return overlaps((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean doesNotExtendRight(String a, String b) {
        return doesNotExtendRight((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean doesNotExtendLeft(String a, String b) {
        return doesNotExtendLeft((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean doesNotExtendAbove(String a, String b) {
        return doesNotExtendAbove((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean doesNotExtendBelow(String a, String b) {
        return doesNotExtendBelow((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static PgPoint intersection(String a, String b) {
        return intersection((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static Object intersectionGeneral(String a, String b) {
        return intersectionGeneral((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static boolean intersects(String a, String b) {
        return intersects((Object) autoDetect(a), (Object) autoDetect(b));
    }

    public static PgPoint closestPoint(String a, String b) {
        return closestPoint((Object) autoDetect(a), (Object) autoDetect(b));
    }
}
