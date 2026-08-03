package com.memgres.engine;

import com.memgres.core.Memgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * That the register of callable names is complete, in both directions.
 *
 * <p>A rule reading "this name is no function" is only as sound as the list it reads. Deciding it
 * from the catalog's list would have refused SQL that works, because the engine dispatches some
 * five hundred names that list does not carry — sin, cos, coalesce, greatest, least, nullif,
 * radians, pow, gcd, cbrt among them. {@link BuiltinFunctionNames#isCallable} answers from a
 * register built for the purpose, and these two tests are what make it safe to read:
 *
 * <ul>
 *   <li>the first sweeps the engine's own dispatch — every case label of every switch on a folded
 *       function name, every name the parser writes as a call, and every name the signature,
 *       window and aggregate tables record — and fails if one of them is missing from the
 *       register;</li>
 *   <li>the second calls every name in the register in a position where only the register and the
 *       arity table can refuse it, and fails if any of them answers 42883.</li>
 * </ul>
 */
class FunctionRegisterTest {

    static Memgres memgres;
    static Connection conn;

    @BeforeAll
    static void setUp() throws Exception {
        memgres = Memgres.builder().port(0).build().start();
        conn = DriverManager.getConnection(memgres.getJdbcUrl(),
                memgres.getUser(), memgres.getPassword());
        conn.setAutoCommit(true);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (conn != null) conn.close();
        if (memgres != null) memgres.close();
    }

    // ---- The register holds everything the engine dispatches ----

    @Test
    void everyNameTheEngineDispatchesIsInTheRegister() throws IOException {
        Set<String> dispatched = new TreeSet<>();
        for (Path source : engineSources()) {
            String src = blankOutCommentsAndStrings(
                    new String(Files.readAllBytes(source), StandardCharsets.UTF_8));
            collectCaseLabels(src, dispatched);
            collectSynthesisedCalls(src, dispatched);
        }
        for (String[] sig : BuiltinFunctionSignatures.SIGNATURES) dispatched.add(sig[0]);
        for (String[] win : BuiltinFunctionSignatures.WINDOW_FUNCTIONS) dispatched.add(win[0]);
        for (String[] agg : BuiltinAggregateSignatures.AGGREGATES) dispatched.add(agg[0]);

        // The sweep has to find something, or it is passing by reading nothing.
        assertTrue(dispatched.size() > 500,
                "the dispatch sweep found only " + dispatched.size() + " names");

        List<String> missing = new ArrayList<>();
        for (String name : dispatched) {
            if (!BuiltinFunctionNames.isCallable(name.toLowerCase(Locale.ROOT))) missing.add(name);
        }
        assertEquals(List.of(), missing,
                "names the engine can dispatch that the register does not hold");
    }

    /** The engine's sources, which is where every call is dispatched from. */
    private static List<Path> engineSources() throws IOException {
        Path dir = Paths.get("src", "main", "java", "com", "memgres", "engine");
        assertTrue(Files.isDirectory(dir), "engine sources not found at " + dir.toAbsolutePath());
        List<Path> sources = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir, "*.java")) {
            for (Path entry : entries) sources.add(entry);
        }
        Path parser = dir.resolve("parser");
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(parser, "*.java")) {
            for (Path entry : entries) sources.add(entry);
        }
        return sources;
    }

    /** The variables the engine folds a call's name into before switching on it. */
    private static final Set<String> NAME_SUBJECTS =
            new HashSet<>(Arrays.asList("name", "fnName", "functionName", "bare", "fname"));

    private static final Pattern SWITCH =
            Pattern.compile("switch\\s*\\(\\s*([A-Za-z0-9_.()]+)\\s*\\)\\s*\\{");
    private static final Pattern CASE = Pattern.compile("case\\s+\"([A-Za-z0-9_]+)\"");
    private static final Pattern SYNTHESISED =
            Pattern.compile("new (?:FunctionCallExpr|WindowFuncExpr)\\(\"([A-Za-z0-9_]+)\"");

    /**
     * Every case label of a switch whose subject is the folded function name. Labels of switches
     * on anything else — the field EXTRACT takes, the form NORMALIZE takes, a hash algorithm — are
     * not function names and are deliberately left out.
     */
    private static void collectCaseLabels(String src, Set<String> out) {
        Map<Integer, String> switchAt = new HashMap<>();
        Map<Integer, Integer> switchEnd = new HashMap<>();
        Matcher sw = SWITCH.matcher(src);
        while (sw.find()) {
            switchAt.put(sw.start(), sw.group(1));
            switchEnd.put(sw.start(), sw.end());
        }
        Map<Integer, String> caseAt = new HashMap<>();
        Map<Integer, Integer> caseEnd = new HashMap<>();
        Matcher cs = CASE.matcher(src);
        while (cs.find()) {
            caseAt.put(cs.start(), cs.group(1));
            caseEnd.put(cs.start(), cs.end());
        }
        Map<Integer, String> subjects = new HashMap<>();
        int depth = 0;
        int i = 0;
        while (i < src.length()) {
            if (switchAt.containsKey(i)) {
                depth++;
                subjects.put(depth, switchAt.get(i));
                i = switchEnd.get(i);
                continue;
            }
            if (caseAt.containsKey(i)) {
                String subject = subjects.get(depth);
                if (subject != null && NAME_SUBJECTS.contains(subject)) out.add(caseAt.get(i));
                i = caseEnd.get(i);
                continue;
            }
            char c = src.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                subjects.remove(depth);
                depth--;
            }
            i++;
        }
    }

    /** The names the parser and the executors write as a call of their own. */
    private static void collectSynthesisedCalls(String src, Set<String> out) {
        Matcher m = SYNTHESISED.matcher(src);
        while (m.find()) out.add(m.group(1));
    }

    /**
     * Braces inside a comment or a string body would throw the depth count off, so they are blanked
     * out; the quotes around a string are kept so a case label still reads.
     */
    private static String blankOutCommentsAndStrings(String src) {
        StringBuilder out = new StringBuilder(src.length());
        int i = 0;
        while (i < src.length()) {
            char c = src.charAt(i);
            if (c == '/' && i + 1 < src.length() && src.charAt(i + 1) == '/') {
                while (i < src.length() && src.charAt(i) != '\n') {
                    out.append(' ');
                    i++;
                }
            } else if (c == '/' && i + 1 < src.length() && src.charAt(i + 1) == '*') {
                while (i + 1 < src.length()
                        && !(src.charAt(i) == '*' && src.charAt(i + 1) == '/')) {
                    out.append(src.charAt(i) == '\n' ? '\n' : ' ');
                    i++;
                }
                out.append("  ");
                i += 2;
            } else if (c == '\'') {
                out.append("' '");
                i++;
                while (i < src.length() && src.charAt(i) != '\'') {
                    if (src.charAt(i) == '\\') i++;
                    i++;
                }
                i++;
            } else if (c == '"') {
                out.append('"');
                i++;
                while (i < src.length() && src.charAt(i) != '"') {
                    char d = src.charAt(i);
                    if (d == '\\') {
                        out.append("  ");
                        i += 2;
                        continue;
                    }
                    out.append(d == '{' || d == '}' ? ' ' : d);
                    i++;
                }
                out.append('"');
                i++;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    // ---- No name in the register is refused for being one ----

    /**
     * Every name in the register, called with an argument list its recorded signatures accept and
     * arguments no type can be read from, beside a column that is not there.
     *
     * <p>That position is what makes the answer decisive. A name the register does not hold is
     * resolved to nothing before the rest of the select list is looked at, so the answer would be
     * 42883; a name it does hold leaves the resolution walk silent and the missing column is
     * reported instead, 42703. Whatever the implementation would have said is never reached,
     * because the column is judged before anything is evaluated — which matters, since a good many
     * of these names answer 42883 themselves when the extension they belong to is not installed.
     * The arguments are bare NULLs, which PostgreSQL leaves untyped, so no signature's argument
     * types are consulted either.
     *
     * <p>Every argument count from none to seven is tried and the name has to survive one of them,
     * because a count no signature takes is 42883 in its own right and this is not the test of
     * that. A name the register does not hold is refused at every count, which is what this
     * catches.
     */
    @Test
    void noNameInTheRegisterAnswersThatItIsNoFunction() throws SQLException {
        exec("DROP TABLE IF EXISTS fr_t CASCADE");
        exec("CREATE TABLE fr_t (id int PRIMARY KEY)");
        exec("INSERT INTO fr_t VALUES (1)");
        List<String> refused = new ArrayList<>();
        for (String name : new TreeSet<>(BuiltinFunctionNames.register())) {
            // A name the lexer reads as something else would have to be quoted, and quoting it
            // makes it a different name; the sweep above is what covers those.
            if (!name.matches("[a-z_][a-z0-9_]*")) continue;
            String lastMessage = null;
            boolean resolved = false;
            for (int count = 0; count <= 7 && !resolved; count++) {
                String sql = "SELECT " + name + "(" + nulls(count) + "), fr_nosuchcolumn FROM fr_t";
                if ("42883".equals(stateOf(sql))) {
                    if (lastMessage == null) lastMessage = messageOf(sql);
                } else {
                    resolved = true;
                }
            }
            if (!resolved) refused.add(name + " -> " + lastMessage);
        }
        exec("DROP TABLE IF EXISTS fr_t CASCADE");
        assertEquals(List.of(), refused,
                "names in the register that answer that they are no function");
    }

    private static void exec(String sql) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            s.execute(sql);
        }
    }

    private static String nulls(int count) {
        StringBuilder args = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) args.append(", ");
            args.append("NULL");
        }
        return args.toString();
    }

    private static String stateOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getSQLState();
        }
    }

    private static String messageOf(String sql) {
        try (Statement s = conn.createStatement()) {
            s.setQueryTimeout(10);
            s.execute(sql);
            return "OK";
        } catch (SQLException e) {
            return e.getMessage().split("\n")[0].replace("ERROR: ", "").trim();
        }
    }
}
