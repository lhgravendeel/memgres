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

    private static final double EPSILON = 1e-10;

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
     */
    private static List<Double> extractDoubles(String s) {
        List<Double> result = new ArrayList<>();
        Matcher m = Pattern.compile("-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?").matcher(s);
        while (m.find()) {
            result.add(Double.parseDouble(m.group()));
        }
        return result;
    }

    /**
     * Parse a list of points from a string like "(x1,y1),(x2,y2),..."
     */
    private static List<PgPoint> parsePointList(String s) {
        List<PgPoint> points = new ArrayList<>();
        Matcher m = Pattern.compile("\\(\\s*(-?[\\d.eE+-]+)\\s*,\\s*(-?[\\d.eE+-]+)\\s*\\)").matcher(s);
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

    public static PgPoint parsePoint(String s) {
        s = s.trim();
        // Validate balanced parentheses
        if (s.contains("(") || s.contains(")")) {
            int depth = 0;
            for (char ch : s.toCharArray()) {
                if (ch == '(') depth++;
                else if (ch == ')') depth--;
                if (depth < 0) throw new MemgresException("invalid input syntax for type point: \"" + s + "\"", "22P02");
            }
            if (depth != 0) throw new MemgresException("invalid input syntax for type point: \"" + s + "\"", "22P02");
        }
        List<Double> nums = extractDoubles(s);
        if (nums.size() < 2) {
            throw new MemgresException("invalid input syntax for type point: \"" + s + "\"", "22P02");
        }
        if (nums.size() > 2) {
            throw new MemgresException("invalid input syntax for type point: \"" + s + "\"", "22P02");
        }
        return new PgPoint(nums.get(0), nums.get(1));
    }

    public static PgLine parseLine(String s) {
        s = s.trim();
        // {A,B,C} format
        if (s.startsWith("{")) {
            String inner = strip(s, '{', '}');
            List<Double> nums = extractDoubles(inner);
            if (nums.size() < 3) {
                throw new MemgresException("invalid input syntax for type line: \"" + s + "\"", "22P02");
            }
            return new PgLine(nums.get(0), nums.get(1), nums.get(2));
        }
        // Two-point format: [(x1,y1),(x2,y2)] or ((x1,y1),(x2,y2))
        List<PgPoint> pts = parsePointList(s);
        if (pts.size() >= 2) {
            return lineFromPoints(pts.get(0), pts.get(1));
        }
        throw new MemgresException("invalid input syntax for type line: \"" + s + "\"", "22P02");
    }

    public static PgLseg parseLseg(String s) {
        s = s.trim();
        List<PgPoint> pts = parsePointList(s);
        if (pts.size() != 2) {
            throw new MemgresException("invalid input syntax for type lseg: \"" + s + "\"", "22P02");
        }
        return new PgLseg(pts.get(0), pts.get(1));
    }

    public static PgBox parseBox(String s) {
        s = s.trim();
        List<PgPoint> pts = parsePointList(s);
        if (pts.size() < 2) {
            throw new MemgresException("invalid input syntax for type box: \"" + s + "\"", "22P02");
        }
        return normalizeBox(pts.get(0), pts.get(1));
    }

    public static PgPath parsePath(String s) {
        s = s.trim();
        // Validate balanced parentheses/brackets
        validateBalancedParens(s, "path");
        boolean closed;
        if (s.startsWith("[")) {
            closed = false;
            s = strip(s, '[', ']');
        } else if (s.startsWith("(")) {
            closed = true;
            // Remove outer parens if present: ((x,y),(x,y)) -> (x,y),(x,y)
            // But we need to be careful; try stripping outer parens
            String inner = s.substring(1, s.length() - 1).trim();
            // Check if the inner string starts with (, if so it's a point list
            if (inner.startsWith("(")) {
                s = inner;
            }
        } else {
            closed = true;
        }
        List<PgPoint> pts = parsePointList(s);
        if (pts.isEmpty()) {
            throw new MemgresException("invalid input syntax for type path: \"" + s + "\"", "22P02");
        }
        return new PgPath(Cols.listCopyOf(pts), closed);
    }

    public static PgPolygon parsePolygon(String s) {
        s = s.trim();
        // Validate balanced parentheses
        validateBalancedParens(s, "polygon");
        // Remove outer parens if double-wrapped: ((x,y),(x,y))
        if (s.startsWith("(") && s.endsWith(")")) {
            String inner = s.substring(1, s.length() - 1).trim();
            if (inner.startsWith("(")) {
                s = inner;
            }
        }
        List<PgPoint> pts = parsePointList(s);
        if (pts.isEmpty()) {
            throw new MemgresException("invalid input syntax for type polygon: \"" + s + "\"", "22P02");
        }
        return new PgPolygon(Cols.listCopyOf(pts));
    }

    public static PgCircle parseCircle(String s) {
        s = s.trim();
        // <(x,y),r> format
        if (s.startsWith("<")) {
            s = strip(s, '<', '>');
        }
        // Find center point and radius
        Matcher m = Pattern.compile("\\(\\s*(-?[\\d.eE+-]+)\\s*,\\s*(-?[\\d.eE+-]+)\\s*\\)\\s*,\\s*(-?[\\d.eE+-]+)").matcher(s);
        if (m.find()) {
            double cx = Double.parseDouble(m.group(1));
            double cy = Double.parseDouble(m.group(2));
            double r = Double.parseDouble(m.group(3));
            return new PgCircle(new PgPoint(cx, cy), r);
        }
        // Fallback
        List<Double> nums = extractDoubles(s);
        if (nums.size() >= 3) {
            return new PgCircle(new PgPoint(nums.get(0), nums.get(1)), nums.get(2));
        }
        throw new MemgresException("invalid input syntax for type circle: \"" + s + "\"", "22P02");
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
        throw new MemgresException("Not a geometric type: " + geom.getClass().getSimpleName(), "42883");
    }

    public static String formatPoint(PgPoint p) {
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

    private static String fmtD(double d) {
        if (d == Math.floor(d) && !Double.isInfinite(d)) {
            return String.valueOf((long) d);
        }
        // Remove trailing zeros
        String s = String.valueOf(d);
        if (s.contains(".") && !s.contains("E") && !s.contains("e")) {
            s = s.replaceAll("0+$", "").replaceAll("\\.$", "");
        }
        return s;
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

    public static PgLine lineFromPoints(PgPoint p1, PgPoint p2) {
        if (Math.abs(p1.x - p2.x) < EPSILON && Math.abs(p1.y - p2.y) < EPSILON) {
            throw new MemgresException("invalid line specification: must be two distinct points", "22023");
        }
        double a = p1.y - p2.y;
        double b = p2.x - p1.x;
        double c = p1.x * p2.y - p2.x * p1.y;
        // PG normalizes so that A > 0, or if A == 0 then B > 0
        if (a < -EPSILON || (Math.abs(a) < EPSILON && b < -EPSILON)) {
            a = -a;
            b = -b;
            c = -c;
        }
        return new PgLine(a, b, c);
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
        if (Math.abs(denom) < EPSILON) {
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
        if (a instanceof PgLseg && b instanceof PgLseg) return distanceLsegLseg(((PgLseg) a), ((PgLseg) b));
        if (a instanceof PgLseg && b instanceof PgLine) return distanceLsegLine(((PgLseg) a), ((PgLine) b));
        if (a instanceof PgLine && b instanceof PgLseg) return distanceLsegLine(((PgLseg) b), ((PgLine) a));
        if (a instanceof PgLine && b instanceof PgLine) return distanceLineLine(((PgLine) a), ((PgLine) b));
        if (a instanceof PgCircle && b instanceof PgCircle) return distanceCircleCircle(((PgCircle) a), ((PgCircle) b));
        if (a instanceof PgPolygon && b instanceof PgPolygon) return distancePolygonPolygon(((PgPolygon) a), ((PgPolygon) b));
        if (a instanceof PgPoint && b instanceof PgCircle) return Math.max(0, distancePointPoint(((PgPoint) a), ((PgCircle) b).center) - ((PgCircle) b).radius);
        if (a instanceof PgCircle && b instanceof PgPoint) return Math.max(0, distancePointPoint(((PgPoint) b), ((PgCircle) a).center) - ((PgCircle) a).radius);
        if (a instanceof PgBox && b instanceof PgBox) {
            // PG: box <-> box computes distance between the centers of the boxes
            PgPoint ca = center((PgBox) a);
            PgPoint cb = center((PgBox) b);
            return distancePointPoint(ca, cb);
        }
        throw new MemgresException("distance not supported between " + a.getClass().getSimpleName() + " and " + b.getClass().getSimpleName(), "42883");
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
        throw new MemgresException("length not supported for " + geom.getClass().getSimpleName(), "42883");
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
        throw new MemgresException("center not supported for " + geom.getClass().getSimpleName(), "42883");
    }

    public static int npoints(Object geom) {
        if (geom instanceof PgPath) return ((PgPath) geom).points.size();
        if (geom instanceof PgPolygon) return ((PgPolygon) geom).points.size();
        throw new MemgresException("npoints not supported for " + geom.getClass().getSimpleName(), "42883");
    }

    public static double area(Object geom) {
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
            if (!path.closed) return 0;
            return shoelaceArea(path.points);
        }
        throw new MemgresException("area not supported for " + geom.getClass().getSimpleName(), "42883");
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

    public static double slope(PgPoint p1, PgPoint p2) {
        double dx = p2.x - p1.x;
        if (Math.abs(dx) < EPSILON) {
            return Double.POSITIVE_INFINITY;
        }
        return (p2.y - p1.y) / dx;
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
            throw new MemgresException("bound_box not supported for " + geom.getClass().getSimpleName(), "42883");
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
     * Ray-casting algorithm for polygon contains point.
     */
    public static boolean polygonContainsPoint(PgPolygon poly, PgPoint p) {
        int n = poly.points.size();
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

    /**
     * General contains dispatch (@>).
     */
    public static boolean contains(Object a, Object b) {
        if (a instanceof PgBox && b instanceof PgPoint) return boxContainsPoint(((PgBox) a), ((PgPoint) b));
        if (a instanceof PgBox && b instanceof PgBox) return boxContainsBox(((PgBox) a), ((PgBox) b));
        if (a instanceof PgCircle && b instanceof PgPoint) return circleContainsPoint(((PgCircle) a), ((PgPoint) b));
        if (a instanceof PgCircle && b instanceof PgCircle) return circleContainsCircle(((PgCircle) a), ((PgCircle) b));
        if (a instanceof PgPolygon && b instanceof PgPoint) return polygonContainsPoint(((PgPolygon) a), ((PgPoint) b));
        if (a instanceof PgPolygon && b instanceof PgPolygon) {
            PgPolygon pb = (PgPolygon) b;
            PgPolygon pa = (PgPolygon) a;
            // All points of b inside a
            for (PgPoint p : pb.points) {
                if (!polygonContainsPoint(pa, p)) return false;
            }
            return true;
        }
        throw new MemgresException("contains not supported between " + a.getClass().getSimpleName() + " and " + b.getClass().getSimpleName(), "42883");
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
        throw new MemgresException("overlap not supported between " + a.getClass().getSimpleName() + " and " + b.getClass().getSimpleName(), "42883");
    }

    // ========================================================================
    // Same as (~=), approximate equality
    // ========================================================================

    public static boolean sameAs(Object a, Object b) {
        if (a instanceof PgPoint && b instanceof PgPoint) {
            PgPoint pb = (PgPoint) b;
            PgPoint pa = (PgPoint) a;
            return Math.abs(pa.x - pb.x) < EPSILON && Math.abs(pa.y - pb.y) < EPSILON;
        }
        if (a instanceof PgBox && b instanceof PgBox) {
            PgBox bb = (PgBox) b;
            PgBox ba = (PgBox) a;
            return sameAs(ba.high, bb.high) && sameAs(ba.low, bb.low);
        }
        if (a instanceof PgCircle && b instanceof PgCircle) {
            PgCircle cb = (PgCircle) b;
            PgCircle ca = (PgCircle) a;
            return sameAs(ca.center, cb.center) && Math.abs(ca.radius - cb.radius) < EPSILON;
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

    public static boolean intersects(Object a, Object b) {
        if (a instanceof PgLseg && b instanceof PgLseg) return lsegIntersects(((PgLseg) a), ((PgLseg) b));
        if (a instanceof PgLine && b instanceof PgLseg) return lineIntersectsLseg(((PgLine) a), ((PgLseg) b));
        if (a instanceof PgLseg && b instanceof PgLine) return lineIntersectsLseg(((PgLine) b), ((PgLseg) a));
        if (a instanceof PgBox && b instanceof PgBox) return boxIntersectsBox(((PgBox) a), ((PgBox) b));
        if (a instanceof PgLine && b instanceof PgBox) return lineIntersectsBox(((PgLine) a), ((PgBox) b));
        if (a instanceof PgBox && b instanceof PgLine) return lineIntersectsBox(((PgLine) b), ((PgBox) a));
        throw new MemgresException("intersects not supported between " + a.getClass().getSimpleName() + " and " + b.getClass().getSimpleName(), "42883");
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

    public static PgPoint closestPoint(Object a, Object b) {
        if (a instanceof PgPoint && b instanceof PgLseg) return closestPointOnLseg(((PgPoint) a), ((PgLseg) b));
        if (a instanceof PgLseg && b instanceof PgPoint) return closestPointOnLseg(((PgPoint) b), ((PgLseg) a));
        if (a instanceof PgPoint && b instanceof PgLine) return closestPointOnLine(((PgPoint) a), ((PgLine) b));
        if (a instanceof PgLine && b instanceof PgPoint) return closestPointOnLine(((PgPoint) b), ((PgLine) a));
        if (a instanceof PgPoint && b instanceof PgBox) return closestPointOnBox(((PgPoint) a), ((PgBox) b));
        if (a instanceof PgBox && b instanceof PgPoint) return closestPointOnBox(((PgPoint) b), ((PgBox) a));
        if (a instanceof PgLseg && b instanceof PgLseg) {
            PgLseg lb = (PgLseg) b;
            PgLseg la = (PgLseg) a;
            PgPoint ip = lsegIntersectionPoint(la, lb);
            if (ip != null) return ip;
            // Find the closest endpoint pair
            double d1 = distancePointLseg(la.p1, lb);
            double d2 = distancePointLseg(la.p2, lb);
            double d3 = distancePointLseg(lb.p1, la);
            double d4 = distancePointLseg(lb.p2, la);
            double min = Math.min(Math.min(d1, d2), Math.min(d3, d4));
            if (min == d1) return closestPointOnLseg(la.p1, lb);
            if (min == d2) return closestPointOnLseg(la.p2, lb);
            if (min == d3) return closestPointOnLseg(lb.p1, la);
            return closestPointOnLseg(lb.p2, la);
        }
        throw new MemgresException("closest_point not supported between " + a.getClass().getSimpleName() + " and " + b.getClass().getSimpleName(), "42883");
    }

    // ========================================================================
    // Intersection point (# operator)
    // ========================================================================

    public static PgPoint lineLineIntersection(PgLine a, PgLine b) {
        double det = a.a * b.b - b.a * a.b;
        if (Math.abs(det) < EPSILON) return null; // parallel
        double x = (a.b * b.c - b.b * a.c) / det;
        double y = (b.a * a.c - a.a * b.c) / det;
        return new PgPoint(x, y);
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
        throw new MemgresException("intersection not supported between " + a.getClass().getSimpleName() + " and " + b.getClass().getSimpleName(), "42883");
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

    /** Inscribed circle of a box (fits inside). */
    public static PgCircle toCircle(PgBox b) {
        PgPoint c = center(b);
        double r = Math.min(width(b), height(b)) / 2;
        return new PgCircle(c, r);
    }

    /** Circumscribed circle of polygon (approximate, uses avg distance from centroid). */
    public static PgCircle toCircle(PgPolygon poly) {
        PgPoint c = center(poly);
        double maxR = 0;
        for (PgPoint p : poly.points) {
            maxR = Math.max(maxR, distancePointPoint(c, p));
        }
        return new PgCircle(c, maxR);
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
        List<PgPoint> pts = new ArrayList<>();
        for (int i = 0; i < npts; i++) {
            double angle = 2 * Math.PI * i / npts;
            pts.add(new PgPoint(
                c.center.x + c.radius * Math.cos(angle),
                c.center.y + c.radius * Math.sin(angle)
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
            // line: {A,B,C}, must be numbers only, not JSON
            return s.matches("\\{\\s*-?[\\d.].*") && !s.contains("\"") && !s.contains(":");
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
    public static double area(String s) {
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
