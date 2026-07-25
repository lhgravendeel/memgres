package com.memgres.engine.fts;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The Snowball "english" (Porter2) stemmer, as bundled with PostgreSQL's
 * {@code english_stem} dictionary.
 *
 * <p>This is a direct port of the published algorithm — the same one PG's generated
 * Snowball code implements — so {@code to_tsvector('english', …)} produces identical
 * lexemes. The algorithm is fully deterministic: regions R1/R2, a short-syllable test,
 * two exception lists, and steps 0 through 5.
 */
public final class EnglishStemmer {

    private EnglishStemmer() {
    }

    /** Words with an irregular stem, checked before any step runs. */
    private static final Map<String, String> EXCEPTIONS;

    /** Words left exactly as they are. */
    private static final Set<String> INVARIANTS = new HashSet<String>(java.util.Arrays.asList(
            "sky", "news", "howe", "atlas", "cosmos", "bias", "andes"));

    /** Words that must not be touched after step 1a. */
    private static final Set<String> EXCEPTIONS_2 = new HashSet<String>(java.util.Arrays.asList(
            "inning", "outing", "canning", "herring", "earring",
            "proceed", "exceed", "succeed"));

    /** Letters a "li" may follow for step 2's li-deletion. */
    private static final String LI_ENDINGS = "cdeghkmnrt";

    static {
        Map<String, String> m = new HashMap<String, String>();
        m.put("skis", "ski");
        m.put("skies", "sky");
        m.put("dying", "die");
        m.put("lying", "lie");
        m.put("tying", "tie");
        m.put("idly", "idl");
        m.put("gently", "gentl");
        m.put("ugly", "ugli");
        m.put("early", "earli");
        m.put("only", "onli");
        m.put("singly", "singl");
        EXCEPTIONS = Collections.unmodifiableMap(m);
    }

    /** Stems one already-lower-cased word. */
    public static String stem(String input) {
        if (input == null) return null;
        String word = input;
        if (word.length() <= 2) return word;

        if (word.charAt(0) == '\'') word = word.substring(1);
        if (word.isEmpty()) return input;

        String exception = EXCEPTIONS.get(word);
        if (exception != null) return exception;
        if (INVARIANTS.contains(word)) return word;

        // Mark consonantal y as Y so it is not treated as a vowel.
        StringBuilder sb = new StringBuilder(word);
        if (sb.charAt(0) == 'y') sb.setCharAt(0, 'Y');
        for (int i = 1; i < sb.length(); i++) {
            if (sb.charAt(i) == 'y' && isVowel(sb.charAt(i - 1))) sb.setCharAt(i, 'Y');
        }
        word = sb.toString();

        int r1 = computeR1(word);
        int r2 = computeR(word, r1);

        word = step0(word);
        word = step1a(word);
        if (EXCEPTIONS_2.contains(word)) return word;
        word = step1b(word, r1);
        word = step1c(word);
        word = step2(word, r1);
        word = step3(word, r1, r2);
        word = step4(word, r2);
        word = step5(word, r1, r2);

        return word.replace('Y', 'y');
    }

    // ------------------------------------------------------------------
    // Regions
    // ------------------------------------------------------------------

    private static int computeR1(String word) {
        // Snowball's special prefixes force R1 rather than deriving it.
        if (word.startsWith("gener") || word.startsWith("arsen")) return 5;
        if (word.startsWith("commun")) return 6;
        return computeR(word, 0);
    }

    /** Position after the first non-vowel that follows a vowel, starting at {@code from}. */
    private static int computeR(String word, int from) {
        for (int i = from; i < word.length() - 1; i++) {
            if (isVowel(word.charAt(i)) && !isVowel(word.charAt(i + 1))) return i + 2;
        }
        return word.length();
    }

    private static boolean isVowel(char c) {
        return c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u' || c == 'y';
    }

    /** True when the word ends in a short syllable. */
    private static boolean endsShortSyllable(String word) {
        int n = word.length();
        if (n == 2) {
            return isVowel(word.charAt(0)) && !isVowel(word.charAt(1));
        }
        if (n < 3) return false;
        char a = word.charAt(n - 3), b = word.charAt(n - 2), c = word.charAt(n - 1);
        return !isVowel(a) && isVowel(b) && !isVowel(c) && c != 'w' && c != 'x' && c != 'Y';
    }

    private static boolean isShort(String word, int r1) {
        return r1 >= word.length() && endsShortSyllable(word);
    }

    private static boolean inR(String word, int r, String suffix) {
        return word.length() - suffix.length() >= r;
    }

    // ------------------------------------------------------------------
    // Steps
    // ------------------------------------------------------------------

    private static String step0(String word) {
        if (word.endsWith("'s'")) return word.substring(0, word.length() - 3);
        if (word.endsWith("'s")) return word.substring(0, word.length() - 2);
        if (word.endsWith("'")) return word.substring(0, word.length() - 1);
        return word;
    }

    private static String step1a(String word) {
        if (word.endsWith("sses")) return word.substring(0, word.length() - 2);
        if (word.endsWith("ied") || word.endsWith("ies")) {
            return word.length() > 4
                    ? word.substring(0, word.length() - 2)
                    : word.substring(0, word.length() - 1);
        }
        if (word.endsWith("us") || word.endsWith("ss")) return word;
        if (word.endsWith("s")) {
            // Delete only when a vowel precedes, and not immediately before the s.
            for (int i = 0; i < word.length() - 2; i++) {
                if (isVowel(word.charAt(i))) return word.substring(0, word.length() - 1);
            }
        }
        return word;
    }

    private static String step1b(String word, int r1) {
        if (word.endsWith("eedly")) {
            return inR(word, r1, "eedly") ? word.substring(0, word.length() - 3) : word;
        }
        if (word.endsWith("eed")) {
            return inR(word, r1, "eed") ? word.substring(0, word.length() - 1) : word;
        }
        String suffix = null;
        if (word.endsWith("ingly")) suffix = "ingly";
        else if (word.endsWith("edly")) suffix = "edly";
        else if (word.endsWith("ing")) suffix = "ing";
        else if (word.endsWith("ed")) suffix = "ed";
        if (suffix == null) return word;

        String stem = word.substring(0, word.length() - suffix.length());
        boolean hasVowel = false;
        for (int i = 0; i < stem.length(); i++) {
            if (isVowel(stem.charAt(i))) { hasVowel = true; break; }
        }
        if (!hasVowel) return word;

        if (stem.endsWith("at") || stem.endsWith("bl") || stem.endsWith("iz")) return stem + "e";
        if (endsDouble(stem)) return stem.substring(0, stem.length() - 1);
        if (isShort(stem, computeR1(stem))) return stem + "e";
        return stem;
    }

    private static boolean endsDouble(String s) {
        // Snowball works backwards only as far as the third character (the algorithm hops
        // three before reversing), so a double in a three-letter stem stays put: "added"
        // stems to "add", not "ad", while the longer "padded" does become "pad".
        if (s.length() < 4) return false;
        char a = s.charAt(s.length() - 2), b = s.charAt(s.length() - 1);
        if (a != b) return false;
        return "bdfgmnprt".indexOf(b) >= 0;
    }

    private static String step1c(String word) {
        int n = word.length();
        if (n < 3) return word;
        char last = word.charAt(n - 1);
        if (last != 'y' && last != 'Y') return word;
        if (isVowel(word.charAt(n - 2))) return word;
        return word.substring(0, n - 1) + "i";
    }

    private static final String[][] STEP2 = {
            {"ization", "ize"}, {"ational", "ate"}, {"fulness", "ful"}, {"ousness", "ous"},
            {"iveness", "ive"}, {"tional", "tion"}, {"biliti", "ble"}, {"lessli", "less"},
            {"entli", "ent"}, {"ation", "ate"}, {"alism", "al"}, {"aliti", "al"},
            {"ousli", "ous"}, {"iviti", "ive"}, {"fulli", "ful"}, {"enci", "ence"},
            {"anci", "ance"}, {"abli", "able"}, {"izer", "ize"}, {"ator", "ate"},
            {"alli", "al"}, {"bli", "ble"},
    };

    private static String step2(String word, int r1) {
        for (String[] rule : STEP2) {
            if (word.endsWith(rule[0])) {
                if (!inR(word, r1, rule[0])) return word;
                return word.substring(0, word.length() - rule[0].length()) + rule[1];
            }
        }
        if (word.endsWith("ogi")) {
            if (!inR(word, r1, "ogi")) return word;
            if (word.length() >= 4 && word.charAt(word.length() - 4) == 'l') {
                return word.substring(0, word.length() - 1);
            }
            return word;
        }
        if (word.endsWith("li")) {
            if (!inR(word, r1, "li")) return word;
            if (word.length() >= 3 && LI_ENDINGS.indexOf(word.charAt(word.length() - 3)) >= 0) {
                return word.substring(0, word.length() - 2);
            }
        }
        return word;
    }

    private static final String[][] STEP3 = {
            {"ational", "ate"}, {"tional", "tion"}, {"alize", "al"}, {"icate", "ic"},
            {"iciti", "ic"}, {"ical", "ic"}, {"ness", ""}, {"ful", ""},
    };

    private static String step3(String word, int r1, int r2) {
        for (String[] rule : STEP3) {
            if (word.endsWith(rule[0])) {
                if (!inR(word, r1, rule[0])) return word;
                return word.substring(0, word.length() - rule[0].length()) + rule[1];
            }
        }
        if (word.endsWith("ative")) {
            if (inR(word, r2, "ative")) return word.substring(0, word.length() - 5);
        }
        return word;
    }

    private static final String[] STEP4 = {
            "ement", "ance", "ence", "able", "ible", "ment",
            "ant", "ent", "ism", "ate", "iti", "ous", "ive", "ize",
            "al", "er", "ic",
    };

    private static String step4(String word, int r2) {
        for (String suffix : STEP4) {
            if (word.endsWith(suffix)) {
                return inR(word, r2, suffix)
                        ? word.substring(0, word.length() - suffix.length()) : word;
            }
        }
        if (word.endsWith("ion")) {
            if (!inR(word, r2, "ion")) return word;
            if (word.length() >= 4) {
                char before = word.charAt(word.length() - 4);
                if (before == 's' || before == 't') return word.substring(0, word.length() - 3);
            }
        }
        return word;
    }

    private static String step5(String word, int r1, int r2) {
        if (word.endsWith("e")) {
            String stem = word.substring(0, word.length() - 1);
            if (inR(word, r2, "e")) return stem;
            if (inR(word, r1, "e") && !endsShortSyllable(stem)) return stem;
            return word;
        }
        if (word.endsWith("l") && word.length() >= 2 && word.charAt(word.length() - 2) == 'l') {
            if (inR(word, r2, "l")) return word.substring(0, word.length() - 1);
        }
        return word;
    }
}
