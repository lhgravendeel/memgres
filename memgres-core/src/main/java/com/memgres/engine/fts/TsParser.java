package com.memgres.engine.fts;

import java.util.ArrayList;
import java.util.List;

/**
 * PostgreSQL's default text-search parser, ported from
 * {@code src/backend/tsearch/wparser_def.c} (PostgreSQL 18, PostgreSQL License).
 *
 * <p>PG does not split text on "everything that is not a letter or a digit". It runs a
 * pushdown state machine that assigns each token one of 23 <em>types</em>, and the
 * text-search configuration then routes each type to a different dictionary. That is why
 * {@code john.doe@example.com} survives as one lexeme (type {@code email}, routed to
 * {@code simple}) while {@code well-known} yields three ({@code asciihword} plus its two
 * parts, all stemmed), and why {@code Foo.java:597} stays whole (a {@code host} with a
 * port) but {@code foo_bar} is two words.
 *
 * <p>This is a direct transcription: the state enumeration, the per-state action tables,
 * the character-class predicates and the {@code TParserGet} driver all keep PG's
 * structure and ordering, because the behaviour is emergent from the table order —
 * several states deliberately list the same character twice, the second entry reachable
 * only after a POP resumes at the following rule.
 *
 * <p>Two deliberate departures. PG's parallel byte and character offsets collapse into a
 * single code-point index, since Java strings are already Unicode — this also avoids the
 * Windows quirk where PG's 16-bit {@code wchar_t} counts an astral character as two.
 * And the character classes come from the JDK's Unicode tables rather than from
 * {@code iswalpha} and friends: PG delegates classification to the platform's C library,
 * so which code points count as letters depends on the host's locale data. Against a
 * server whose tables stop at Unicode 5.1, letters added later (Samaritan, Georgian
 * Mtavruli, recent Cyrillic) are classified differently here — deliberately, since the
 * JDK's answer is the one a PG built against current locale data or ICU gives.
 * ASCII, on which the token types actually turn, is identical either way.
 */
public final class TsParser {

    private TsParser() {
    }

    // ------------------------------------------------------------------
    // Token types (wparser_def.c output token categories)
    // ------------------------------------------------------------------

    /** Token types PG's default parser assigns, in PG's own numbering order. */
    public enum Type {
        ASCIIWORD("asciiword", "Word, all ASCII"),
        WORD("word", "Word, all letters"),
        NUMWORD("numword", "Word, letters and digits"),
        EMAIL("email", "Email address"),
        URL("url", "URL"),
        HOST("host", "Host"),
        SFLOAT("sfloat", "Scientific notation"),
        VERSION("version", "Version number"),
        HWORD_NUMPART("hword_numpart", "Hyphenated word part, letters and digits"),
        HWORD_PART("hword_part", "Hyphenated word part, all letters"),
        HWORD_ASCIIPART("hword_asciipart", "Hyphenated word part, all ASCII"),
        BLANK("blank", "Space symbols"),
        TAG("tag", "XML tag"),
        PROTOCOL("protocol", "Protocol head"),
        NUMHWORD("numhword", "Hyphenated word, letters and digits"),
        ASCIIHWORD("asciihword", "Hyphenated word, all ASCII"),
        HWORD("hword", "Hyphenated word, all letters"),
        URL_PATH("url_path", "URL path"),
        FILE("file", "File or path name"),
        FLOAT("float", "Decimal notation"),
        INT("int", "Signed integer"),
        UINT("uint", "Unsigned integer"),
        ENTITY("entity", "XML entity");

        private final String alias;
        private final String description;

        Type(String alias, String description) {
            this.alias = alias;
            this.description = description;
        }

        /** The name ts_debug and ts_token_type report for this type. */
        public String alias() {
            return alias;
        }

        /** The description ts_token_type reports. */
        public String description() {
            return description;
        }

        /** PG's tokid for this type. */
        public int id() {
            return ordinal() + 1;
        }
    }

    private static final Type[] TYPES = Type.values();

    // Numeric type ids matching wparser_def.c, so the tables read like the original.
    private static final int ASCIIWORD = 1;
    private static final int WORD_T = 2;
    private static final int NUMWORD = 3;
    private static final int EMAIL = 4;
    private static final int URL_T = 5;
    private static final int HOST = 6;
    private static final int SCIENTIFIC = 7;
    private static final int VERSIONNUMBER = 8;
    private static final int NUMPARTHWORD = 9;
    private static final int PARTHWORD = 10;
    private static final int ASCIIPARTHWORD = 11;
    private static final int SPACE = 12;
    private static final int TAG_T = 13;
    private static final int PROTOCOL = 14;
    private static final int NUMHWORD = 15;
    private static final int ASCIIHWORD = 16;
    private static final int HWORD = 17;
    private static final int URLPATH = 18;
    private static final int FILEPATH = 19;
    private static final int DECIMAL_T = 20;
    private static final int SIGNEDINT = 21;
    private static final int UNSIGNEDINT = 22;
    private static final int XMLENTITY = 23;

    /** Which dictionary the stock {@code english} configuration routes a type to. */
    public enum Dict { STEM, SIMPLE, NONE }

    public static Dict dictionaryFor(Type type) {
        switch (type) {
            case ASCIIWORD:
            case WORD:
            case ASCIIHWORD:
            case HWORD:
            case HWORD_ASCIIPART:
            case HWORD_PART:
                return Dict.STEM;
            case NUMWORD:
            case NUMHWORD:
            case HWORD_NUMPART:
            case EMAIL:
            case URL:
            case HOST:
            case URL_PATH:
            case FILE:
            case UINT:
            case INT:
            case FLOAT:
            case SFLOAT:
            case VERSION:
                return Dict.SIMPLE;
            default:
                // blank, tag, protocol and entity have no dictionary in the stock
                // configuration, so they take no position either.
                return Dict.NONE;
        }
    }

    public static final class Token {
        public final Type type;
        public final String text;

        Token(Type type, String text) {
            this.type = type;
            this.text = text;
        }

        public Type type() { return type; }

        public String text() { return text; }

        @Override
        public String toString() {
            return type.alias() + "(" + text + ")";
        }
    }

    // ------------------------------------------------------------------
    // Public entry point
    // ------------------------------------------------------------------

    /** All token types, in PG's tokid order. */
    public static Type[] tokenTypes() {
        return TYPES.clone();
    }

    public static List<Token> parse(String text) {
        List<Token> out = new ArrayList<Token>();
        if (text == null || text.isEmpty()) return out;
        TParser prs = TParser.init(text);
        while (prs.get()) {
            out.add(new Token(TYPES[prs.type - 1], prs.tokenText()));
        }
        return out;
    }

    // ------------------------------------------------------------------
    // Parser states
    // ------------------------------------------------------------------

    private enum S {
        TPS_Base,
        TPS_InNumWord,
        TPS_InAsciiWord,
        TPS_InWord,
        TPS_InUnsignedInt,
        TPS_InSignedIntFirst,
        TPS_InSignedInt,
        TPS_InSpace,
        TPS_InUDecimalFirst,
        TPS_InUDecimal,
        TPS_InDecimalFirst,
        TPS_InDecimal,
        TPS_InVerVersion,
        TPS_InSVerVersion,
        TPS_InVersionFirst,
        TPS_InVersion,
        TPS_InMantissaFirst,
        TPS_InMantissaSign,
        TPS_InMantissa,
        TPS_InXMLEntityFirst,
        TPS_InXMLEntity,
        TPS_InXMLEntityNumFirst,
        TPS_InXMLEntityNum,
        TPS_InXMLEntityHexNumFirst,
        TPS_InXMLEntityHexNum,
        TPS_InXMLEntityEnd,
        TPS_InTagFirst,
        TPS_InXMLBegin,
        TPS_InTagCloseFirst,
        TPS_InTagName,
        TPS_InTagBeginEnd,
        TPS_InTag,
        TPS_InTagEscapeK,
        TPS_InTagEscapeKK,
        TPS_InTagBackSleshed,
        TPS_InTagEnd,
        TPS_InCommentFirst,
        TPS_InCommentLast,
        TPS_InComment,
        TPS_InCloseCommentFirst,
        TPS_InCloseCommentLast,
        TPS_InCommentEnd,
        TPS_InHostFirstDomain,
        TPS_InHostDomainSecond,
        TPS_InHostDomain,
        TPS_InPortFirst,
        TPS_InPort,
        TPS_InHostFirstAN,
        TPS_InHost,
        TPS_InEmail,
        TPS_InFileFirst,
        TPS_InFileTwiddle,
        TPS_InPathFirst,
        TPS_InPathFirstFirst,
        TPS_InPathSecond,
        TPS_InFile,
        TPS_InFileNext,
        TPS_InURLPathFirst,
        TPS_InURLPathStart,
        TPS_InURLPath,
        TPS_InFURL,
        TPS_InProtocolFirst,
        TPS_InProtocolSecond,
        TPS_InProtocolEnd,
        TPS_InHyphenAsciiWordFirst,
        TPS_InHyphenAsciiWord,
        TPS_InHyphenWordFirst,
        TPS_InHyphenWord,
        TPS_InHyphenNumWordFirst,
        TPS_InHyphenNumWord,
        TPS_InHyphenDigitLookahead,
        TPS_InParseHyphen,
        TPS_InParseHyphenHyphen,
        TPS_InHyphenWordPart,
        TPS_InHyphenAsciiWordPart,
        TPS_InHyphenNumWordPart,
        TPS_InHyphenUnsignedInt,
        TPS_Null
    }

    // Action flags
    private static final int A_NEXT = 0x0000;
    private static final int A_BINGO = 0x0001;
    private static final int A_POP = 0x0002;
    private static final int A_PUSH = 0x0004;
    private static final int A_RERUN = 0x0008;
    private static final int A_CLEAR = 0x0010;
    private static final int A_MERGE = 0x0020;
    private static final int A_CLRALL = 0x0040;

    private interface CharTest {
        boolean test(TParser prs);
    }

    private interface Special {
        void apply(TParser prs);
    }

    private static final class Item {
        final CharTest isclass;   // null marks the catch-all rule
        final char c;
        final int flags;
        final S tostate;
        final int type;
        final Special special;

        Item(CharTest isclass, char c, int flags, S tostate, int type, Special special) {
            this.isclass = isclass;
            this.c = c;
            this.flags = flags;
            this.tostate = tostate;
            this.type = type;
            this.special = special;
        }
    }

    private static Item i(CharTest isclass, char c, int flags, S tostate, int type, Special special) {
        return new Item(isclass, c, flags, tostate, type, special);
    }

    // ------------------------------------------------------------------
    // Character-class predicates
    // ------------------------------------------------------------------

    private static final CharTest p_isEOF = new CharTest() {
        public boolean test(TParser p) { return p.state.pos == p.len || p.state.charlen == 0; }
    };
    private static final CharTest p_iseqC = new CharTest() {
        public boolean test(TParser p) { return p.state.charlen == 1 && p.cur() == p.c; }
    };
    private static final CharTest p_isalnum = new CharTest() {
        public boolean test(TParser p) { int c = p.cur(); return c >= 0 && Character.isLetterOrDigit(c); }
    };
    private static final CharTest p_isnotalnum = new CharTest() {
        public boolean test(TParser p) { return !p_isalnum.test(p); }
    };
    private static final CharTest p_isalpha = new CharTest() {
        public boolean test(TParser p) { int c = p.cur(); return c >= 0 && Character.isLetter(c); }
    };
    private static final CharTest p_isdigit = new CharTest() {
        public boolean test(TParser p) { int c = p.cur(); return c >= '0' && c <= '9'; }
    };
    private static final CharTest p_isxdigit = new CharTest() {
        public boolean test(TParser p) {
            int c = p.cur();
            return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        }
    };
    private static final CharTest p_isspace = new CharTest() {
        public boolean test(TParser p) { int c = p.cur(); return c >= 0 && Character.isWhitespace(c); }
    };
    private static final CharTest p_isasclet = new CharTest() {
        public boolean test(TParser p) {
            int c = p.cur();
            return c >= 0 && c < 128 && Character.isLetter(c);
        }
    };

    /** PG's p_isurlchar: printable ASCII minus the characters RFC 3986 disallows. */
    private static final CharTest p_isurlchar = new CharTest() {
        public boolean test(TParser p) {
            int ch = p.cur();
            if (ch <= 0x20 || ch >= 0x7F) return false;
            switch (ch) {
                case '"': case '<': case '>': case '\\':
                case '^': case '`': case '{': case '|': case '}':
                    return false;
                default:
                    return true;
            }
        }
    };

    private static final CharTest p_isignore = new CharTest() {
        public boolean test(TParser p) { return p.ignore; }
    };

    private static final CharTest p_isstophost = new CharTest() {
        public boolean test(TParser p) {
            if (p.wanthost) {
                p.wanthost = false;
                return true;
            }
            return false;
        }
    };

    /**
     * Zero-width or combining characters: not letters, but not word breakers either.
     * PG tests {@code pg_dsplen() == 0} plus a table of spacing combining marks.
     */
    private static final CharTest p_isspecial = new CharTest() {
        public boolean test(TParser p) {
            int c = p.cur();
            if (c < 0) return false;
            if (c == 0) return true;
            int type = Character.getType(c);
            if (type == Character.NON_SPACING_MARK
                    || type == Character.ENCLOSING_MARK
                    || type == Character.FORMAT) {
                return true;
            }
            return isStrangeLetter(c);
        }
    };

    /** Recursive sub-parse: does a host name start here? */
    private static final CharTest p_ishost = new CharTest() {
        public boolean test(TParser p) {
            TParser tmp = TParser.copyInit(p);
            tmp.wanthost = true;
            if (tmp.get() && tmp.type == HOST) {
                p.state.pos += tmp.lenTok;
                p.state.lenTok += tmp.lenTok;
                p.state.charlen = tmp.state.charlen;
                return true;
            }
            return false;
        }
    };

    /** Recursive sub-parse: does a URL path start here? */
    private static final CharTest p_isURLPath = new CharTest() {
        public boolean test(TParser p) {
            TParser tmp = TParser.copyInit(p);
            tmp.state = new Pos(tmp.state);
            tmp.state.state = S.TPS_InURLPathFirst;
            if (tmp.get() && tmp.type == URLPATH) {
                p.state.pos += tmp.lenTok;
                p.state.lenTok += tmp.lenTok;
                p.state.charlen = tmp.state.charlen;
                return true;
            }
            return false;
        }
    };

    // ------------------------------------------------------------------
    // Special handlers
    // ------------------------------------------------------------------

    private static final Special SpecialTags = new Special() {
        public void apply(TParser p) {
            switch (p.state.lenTok) {
                case 8:  // </script
                    if (p.pendingEqualsIgnoreCase("</script")) p.ignore = false;
                    break;
                case 7:  // <script || </style
                    if (p.pendingEqualsIgnoreCase("</style")) p.ignore = false;
                    else if (p.pendingEqualsIgnoreCase("<script")) p.ignore = true;
                    break;
                case 6:  // <style
                    if (p.pendingEqualsIgnoreCase("<style")) p.ignore = true;
                    break;
                default:
                    break;
            }
        }
    };

    private static final Special SpecialFURL = new Special() {
        public void apply(TParser p) {
            p.wanthost = true;
            p.state.pos -= p.state.lenTok;
        }
    };

    private static final Special SpecialHyphen = new Special() {
        public void apply(TParser p) {
            p.state.pos -= p.state.lenTok;
        }
    };

    private static final Special SpecialVerVersion = new Special() {
        public void apply(TParser p) {
            p.state.pos -= p.state.lenTok;
            p.state.lenTok = 0;
        }
    };

    // ------------------------------------------------------------------
    // Table of state/action of parser
    // ------------------------------------------------------------------

    private static final Item[] actionTPS_Base = {
            i(p_isEOF, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '<', A_PUSH, S.TPS_InTagFirst, 0, null),
            i(p_isignore, (char) 0, A_NEXT, S.TPS_InSpace, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InAsciiWord, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InWord, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InUnsignedInt, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InSignedIntFirst, 0, null),
            i(p_iseqC, '+', A_PUSH, S.TPS_InSignedIntFirst, 0, null),
            i(p_iseqC, '&', A_PUSH, S.TPS_InXMLEntityFirst, 0, null),
            i(p_iseqC, '~', A_PUSH, S.TPS_InFileTwiddle, 0, null),
            i(p_iseqC, '/', A_PUSH, S.TPS_InFileFirst, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InPathFirstFirst, 0, null),
            i(null, (char) 0, A_NEXT, S.TPS_InSpace, 0, null)
    };

    private static final Item[] actionTPS_InNumWord = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, NUMWORD, null),
            i(p_isalnum, (char) 0, A_NEXT, S.TPS_InNumWord, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InNumWord, 0, null),
            i(p_iseqC, '@', A_PUSH, S.TPS_InEmail, 0, null),
            i(p_iseqC, '/', A_PUSH, S.TPS_InFileFirst, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InFileNext, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHyphenNumWordFirst, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, NUMWORD, null)
    };

    private static final Item[] actionTPS_InAsciiWord = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, ASCIIWORD, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InHostFirstDomain, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InFileNext, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHyphenAsciiWordFirst, 0, null),
            i(p_iseqC, '_', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(p_iseqC, '@', A_PUSH, S.TPS_InEmail, 0, null),
            i(p_iseqC, ':', A_PUSH, S.TPS_InProtocolFirst, 0, null),
            i(p_iseqC, '/', A_PUSH, S.TPS_InFileFirst, 0, null),
            i(p_isdigit, (char) 0, A_PUSH, S.TPS_InHost, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InNumWord, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InWord, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InWord, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, ASCIIWORD, null)
    };

    private static final Item[] actionTPS_InWord = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, WORD_T, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InNumWord, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHyphenWordFirst, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, WORD_T, null)
    };

    private static final Item[] actionTPS_InUnsignedInt = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, UNSIGNEDINT, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InHostFirstDomain, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InUDecimalFirst, 0, null),
            i(p_iseqC, 'e', A_PUSH, S.TPS_InMantissaFirst, 0, null),
            i(p_iseqC, 'E', A_PUSH, S.TPS_InMantissaFirst, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(p_iseqC, '_', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(p_iseqC, '@', A_PUSH, S.TPS_InEmail, 0, null),
            i(p_isasclet, (char) 0, A_PUSH, S.TPS_InHost, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InNumWord, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InNumWord, 0, null),
            i(p_iseqC, '/', A_PUSH, S.TPS_InFileFirst, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, UNSIGNEDINT, null)
    };

    private static final Item[] actionTPS_InSignedIntFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_NEXT | A_CLEAR, S.TPS_InSignedInt, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InSignedInt = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, SIGNEDINT, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InDecimalFirst, 0, null),
            i(p_iseqC, 'e', A_PUSH, S.TPS_InMantissaFirst, 0, null),
            i(p_iseqC, 'E', A_PUSH, S.TPS_InMantissaFirst, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, SIGNEDINT, null)
    };

    private static final Item[] actionTPS_InSpace = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, SPACE, null),
            i(p_iseqC, '<', A_BINGO, S.TPS_Base, SPACE, null),
            i(p_isignore, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '-', A_BINGO, S.TPS_Base, SPACE, null),
            i(p_iseqC, '+', A_BINGO, S.TPS_Base, SPACE, null),
            i(p_iseqC, '&', A_BINGO, S.TPS_Base, SPACE, null),
            i(p_iseqC, '/', A_BINGO, S.TPS_Base, SPACE, null),
            i(p_isnotalnum, (char) 0, A_NEXT, S.TPS_InSpace, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, SPACE, null)
    };

    private static final Item[] actionTPS_InUDecimalFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_CLEAR, S.TPS_InUDecimal, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InUDecimal = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, DECIMAL_T, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InUDecimal, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InVersionFirst, 0, null),
            i(p_iseqC, 'e', A_PUSH, S.TPS_InMantissaFirst, 0, null),
            i(p_iseqC, 'E', A_PUSH, S.TPS_InMantissaFirst, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, DECIMAL_T, null)
    };

    private static final Item[] actionTPS_InDecimalFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_CLEAR, S.TPS_InDecimal, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InDecimal = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, DECIMAL_T, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InDecimal, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InVerVersion, 0, null),
            i(p_iseqC, 'e', A_PUSH, S.TPS_InMantissaFirst, 0, null),
            i(p_iseqC, 'E', A_PUSH, S.TPS_InMantissaFirst, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, DECIMAL_T, null)
    };

    private static final Item[] actionTPS_InVerVersion = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_RERUN, S.TPS_InSVerVersion, 0, SpecialVerVersion),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InSVerVersion = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_BINGO | A_CLRALL, S.TPS_InUnsignedInt, SPACE, null),
            i(null, (char) 0, A_NEXT, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InVersionFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_CLEAR, S.TPS_InVersion, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InVersion = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, VERSIONNUMBER, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InVersion, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InVersionFirst, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, VERSIONNUMBER, null)
    };

    private static final Item[] actionTPS_InMantissaFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_CLEAR, S.TPS_InMantissa, 0, null),
            i(p_iseqC, '+', A_NEXT, S.TPS_InMantissaSign, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_InMantissaSign, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InMantissaSign = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_CLEAR, S.TPS_InMantissa, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InMantissa = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, SCIENTIFIC, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InMantissa, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, SCIENTIFIC, null)
    };

    private static final Item[] actionTPS_InXMLEntityFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '#', A_NEXT, S.TPS_InXMLEntityNumFirst, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InXMLEntity, 0, null),
            i(p_iseqC, ':', A_NEXT, S.TPS_InXMLEntity, 0, null),
            i(p_iseqC, '_', A_NEXT, S.TPS_InXMLEntity, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InXMLEntity = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isalnum, (char) 0, A_NEXT, S.TPS_InXMLEntity, 0, null),
            i(p_iseqC, ':', A_NEXT, S.TPS_InXMLEntity, 0, null),
            i(p_iseqC, '_', A_NEXT, S.TPS_InXMLEntity, 0, null),
            i(p_iseqC, '.', A_NEXT, S.TPS_InXMLEntity, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_InXMLEntity, 0, null),
            i(p_iseqC, ';', A_NEXT, S.TPS_InXMLEntityEnd, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InXMLEntityNumFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, 'x', A_NEXT, S.TPS_InXMLEntityHexNumFirst, 0, null),
            i(p_iseqC, 'X', A_NEXT, S.TPS_InXMLEntityHexNumFirst, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InXMLEntityNum, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InXMLEntityHexNumFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isxdigit, (char) 0, A_NEXT, S.TPS_InXMLEntityHexNum, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InXMLEntityNum = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InXMLEntityNum, 0, null),
            i(p_iseqC, ';', A_NEXT, S.TPS_InXMLEntityEnd, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InXMLEntityHexNum = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isxdigit, (char) 0, A_NEXT, S.TPS_InXMLEntityHexNum, 0, null),
            i(p_iseqC, ';', A_NEXT, S.TPS_InXMLEntityEnd, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InXMLEntityEnd = {
            i(null, (char) 0, A_BINGO | A_CLEAR, S.TPS_Base, XMLENTITY, null)
    };

    private static final Item[] actionTPS_InTagFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '/', A_PUSH, S.TPS_InTagCloseFirst, 0, null),
            i(p_iseqC, '!', A_PUSH, S.TPS_InCommentFirst, 0, null),
            i(p_iseqC, '?', A_PUSH, S.TPS_InXMLBegin, 0, null),
            i(p_isasclet, (char) 0, A_PUSH, S.TPS_InTagName, 0, null),
            i(p_iseqC, ':', A_PUSH, S.TPS_InTagName, 0, null),
            i(p_iseqC, '_', A_PUSH, S.TPS_InTagName, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InXMLBegin = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            // <?xml ... — PG accepts <?x followed by anything
            i(p_iseqC, 'x', A_NEXT, S.TPS_InTag, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InTagCloseFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InTagName, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InTagName = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            // <br/> case
            i(p_iseqC, '/', A_NEXT, S.TPS_InTagBeginEnd, 0, null),
            i(p_iseqC, '>', A_NEXT, S.TPS_InTagEnd, 0, SpecialTags),
            i(p_isspace, (char) 0, A_NEXT, S.TPS_InTag, 0, SpecialTags),
            i(p_isalnum, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, ':', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '_', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '.', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_Null, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InTagBeginEnd = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '>', A_NEXT, S.TPS_InTagEnd, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InTag = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '>', A_NEXT, S.TPS_InTagEnd, 0, SpecialTags),
            i(p_iseqC, '\'', A_NEXT, S.TPS_InTagEscapeK, 0, null),
            i(p_iseqC, '"', A_NEXT, S.TPS_InTagEscapeKK, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '=', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '_', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '#', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '/', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, ':', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '.', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '&', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '?', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '%', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '~', A_NEXT, S.TPS_Null, 0, null),
            i(p_isspace, (char) 0, A_NEXT, S.TPS_Null, 0, SpecialTags),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InTagEscapeK = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '\\', A_PUSH, S.TPS_InTagBackSleshed, 0, null),
            i(p_iseqC, '\'', A_NEXT, S.TPS_InTag, 0, null),
            i(null, (char) 0, A_NEXT, S.TPS_InTagEscapeK, 0, null)
    };

    private static final Item[] actionTPS_InTagEscapeKK = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '\\', A_PUSH, S.TPS_InTagBackSleshed, 0, null),
            i(p_iseqC, '"', A_NEXT, S.TPS_InTag, 0, null),
            i(null, (char) 0, A_NEXT, S.TPS_InTagEscapeKK, 0, null)
    };

    private static final Item[] actionTPS_InTagBackSleshed = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(null, (char) 0, A_MERGE, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InTagEnd = {
            i(null, (char) 0, A_BINGO | A_CLRALL, S.TPS_Base, TAG_T, null)
    };

    private static final Item[] actionTPS_InCommentFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_InCommentLast, 0, null),
            // <!DOCTYPE ...>
            i(p_iseqC, 'D', A_NEXT, S.TPS_InTag, 0, null),
            i(p_iseqC, 'd', A_NEXT, S.TPS_InTag, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InCommentLast = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_InComment, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InComment = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_InCloseCommentFirst, 0, null),
            i(null, (char) 0, A_NEXT, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InCloseCommentFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_InCloseCommentLast, 0, null),
            i(null, (char) 0, A_NEXT, S.TPS_InComment, 0, null)
    };

    private static final Item[] actionTPS_InCloseCommentLast = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_Null, 0, null),
            i(p_iseqC, '>', A_NEXT, S.TPS_InCommentEnd, 0, null),
            i(null, (char) 0, A_NEXT, S.TPS_InComment, 0, null)
    };

    private static final Item[] actionTPS_InCommentEnd = {
            i(null, (char) 0, A_BINGO | A_CLRALL, S.TPS_Base, TAG_T, null)
    };

    private static final Item[] actionTPS_InHostFirstDomain = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InHostDomainSecond, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHost, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InHostDomainSecond = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InHostDomain, 0, null),
            i(p_isdigit, (char) 0, A_PUSH, S.TPS_InHost, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(p_iseqC, '_', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InHostFirstDomain, 0, null),
            i(p_iseqC, '@', A_PUSH, S.TPS_InEmail, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InHostDomain = {
            i(p_isEOF, (char) 0, A_BINGO | A_CLRALL, S.TPS_Base, HOST, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InHostDomain, 0, null),
            i(p_isdigit, (char) 0, A_PUSH, S.TPS_InHost, 0, null),
            i(p_iseqC, ':', A_PUSH, S.TPS_InPortFirst, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(p_iseqC, '_', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InHostFirstDomain, 0, null),
            i(p_iseqC, '@', A_PUSH, S.TPS_InEmail, 0, null),
            i(p_isdigit, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isstophost, (char) 0, A_BINGO | A_CLRALL, S.TPS_InURLPathStart, HOST, null),
            i(p_iseqC, '/', A_PUSH, S.TPS_InFURL, 0, null),
            i(null, (char) 0, A_BINGO | A_CLRALL, S.TPS_Base, HOST, null)
    };

    private static final Item[] actionTPS_InPortFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InPort, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InPort = {
            i(p_isEOF, (char) 0, A_BINGO | A_CLRALL, S.TPS_Base, HOST, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InPort, 0, null),
            i(p_isstophost, (char) 0, A_BINGO | A_CLRALL, S.TPS_InURLPathStart, HOST, null),
            i(p_iseqC, '/', A_PUSH, S.TPS_InFURL, 0, null),
            i(null, (char) 0, A_BINGO | A_CLRALL, S.TPS_Base, HOST, null)
    };

    private static final Item[] actionTPS_InHostFirstAN = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHost, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InHost, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InHost = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHost, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InHost, 0, null),
            i(p_iseqC, '@', A_PUSH, S.TPS_InEmail, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InHostFirstDomain, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(p_iseqC, '_', A_PUSH, S.TPS_InHostFirstAN, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InEmail = {
            i(p_isstophost, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_ishost, (char) 0, A_BINGO | A_CLRALL, S.TPS_Base, EMAIL, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InFileFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InFile, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InFile, 0, null),
            i(p_iseqC, '.', A_NEXT, S.TPS_InPathFirst, 0, null),
            i(p_iseqC, '_', A_NEXT, S.TPS_InFile, 0, null),
            i(p_iseqC, '~', A_PUSH, S.TPS_InFileTwiddle, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InFileTwiddle = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InFile, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InFile, 0, null),
            i(p_iseqC, '_', A_NEXT, S.TPS_InFile, 0, null),
            i(p_iseqC, '/', A_NEXT, S.TPS_InFileFirst, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InPathFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InFile, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InFile, 0, null),
            i(p_iseqC, '_', A_NEXT, S.TPS_InFile, 0, null),
            i(p_iseqC, '.', A_NEXT, S.TPS_InPathSecond, 0, null),
            i(p_iseqC, '/', A_NEXT, S.TPS_InFileFirst, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InPathFirstFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '.', A_NEXT, S.TPS_InPathSecond, 0, null),
            i(p_iseqC, '/', A_NEXT, S.TPS_InFileFirst, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InPathSecond = {
            i(p_isEOF, (char) 0, A_BINGO | A_CLEAR, S.TPS_Base, FILEPATH, null),
            i(p_iseqC, '/', A_NEXT | A_PUSH, S.TPS_InFileFirst, 0, null),
            i(p_iseqC, '/', A_BINGO | A_CLEAR, S.TPS_Base, FILEPATH, null),
            i(p_isspace, (char) 0, A_BINGO | A_CLEAR, S.TPS_Base, FILEPATH, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InFile = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, FILEPATH, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InFile, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InFile, 0, null),
            i(p_iseqC, '.', A_PUSH, S.TPS_InFileNext, 0, null),
            i(p_iseqC, '_', A_NEXT, S.TPS_InFile, 0, null),
            i(p_iseqC, '-', A_NEXT, S.TPS_InFile, 0, null),
            i(p_iseqC, '/', A_PUSH, S.TPS_InFileFirst, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, FILEPATH, null)
    };

    private static final Item[] actionTPS_InFileNext = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isasclet, (char) 0, A_CLEAR, S.TPS_InFile, 0, null),
            i(p_isdigit, (char) 0, A_CLEAR, S.TPS_InFile, 0, null),
            i(p_iseqC, '_', A_CLEAR, S.TPS_InFile, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InURLPathFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isurlchar, (char) 0, A_NEXT, S.TPS_InURLPath, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InURLPathStart = {
            i(null, (char) 0, A_NEXT, S.TPS_InURLPath, 0, null)
    };

    private static final Item[] actionTPS_InURLPath = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, URLPATH, null),
            i(p_isurlchar, (char) 0, A_NEXT, S.TPS_InURLPath, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_Base, URLPATH, null)
    };

    private static final Item[] actionTPS_InFURL = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isURLPath, (char) 0, A_BINGO | A_CLRALL, S.TPS_Base, URL_T, SpecialFURL),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InProtocolFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '/', A_NEXT, S.TPS_InProtocolSecond, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InProtocolSecond = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_iseqC, '/', A_NEXT, S.TPS_InProtocolEnd, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InProtocolEnd = {
            i(null, (char) 0, A_BINGO | A_CLRALL, S.TPS_Base, PROTOCOL, null)
    };

    private static final Item[] actionTPS_InHyphenAsciiWordFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InHyphenAsciiWord, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InHyphenWord, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHyphenDigitLookahead, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InHyphenAsciiWord = {
            i(p_isEOF, (char) 0, A_BINGO | A_CLRALL, S.TPS_InParseHyphen, ASCIIHWORD, SpecialHyphen),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InHyphenAsciiWord, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InHyphenWord, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InHyphenWord, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHyphenNumWord, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHyphenAsciiWordFirst, 0, null),
            i(null, (char) 0, A_BINGO | A_CLRALL, S.TPS_InParseHyphen, ASCIIHWORD, SpecialHyphen)
    };

    private static final Item[] actionTPS_InHyphenWordFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InHyphenWord, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHyphenDigitLookahead, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InHyphenWord = {
            i(p_isEOF, (char) 0, A_BINGO | A_CLRALL, S.TPS_InParseHyphen, HWORD, SpecialHyphen),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InHyphenWord, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InHyphenWord, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHyphenNumWord, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHyphenWordFirst, 0, null),
            i(null, (char) 0, A_BINGO | A_CLRALL, S.TPS_InParseHyphen, HWORD, SpecialHyphen)
    };

    private static final Item[] actionTPS_InHyphenNumWordFirst = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InHyphenNumWord, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHyphenDigitLookahead, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InHyphenNumWord = {
            i(p_isEOF, (char) 0, A_BINGO | A_CLRALL, S.TPS_InParseHyphen, NUMHWORD, SpecialHyphen),
            i(p_isalnum, (char) 0, A_NEXT, S.TPS_InHyphenNumWord, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InHyphenNumWord, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InHyphenNumWordFirst, 0, null),
            i(null, (char) 0, A_BINGO | A_CLRALL, S.TPS_InParseHyphen, NUMHWORD, SpecialHyphen)
    };

    private static final Item[] actionTPS_InHyphenDigitLookahead = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHyphenDigitLookahead, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InHyphenNumWord, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InHyphenNumWord, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InParseHyphen = {
            i(p_isEOF, (char) 0, A_RERUN, S.TPS_Base, 0, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InHyphenAsciiWordPart, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InHyphenWordPart, 0, null),
            i(p_isdigit, (char) 0, A_PUSH, S.TPS_InHyphenUnsignedInt, 0, null),
            i(p_iseqC, '-', A_PUSH, S.TPS_InParseHyphenHyphen, 0, null),
            i(null, (char) 0, A_RERUN, S.TPS_Base, 0, null)
    };

    private static final Item[] actionTPS_InParseHyphenHyphen = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isalnum, (char) 0, A_BINGO | A_CLEAR, S.TPS_InParseHyphen, SPACE, null),
            i(p_isspecial, (char) 0, A_BINGO | A_CLEAR, S.TPS_InParseHyphen, SPACE, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    private static final Item[] actionTPS_InHyphenWordPart = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, PARTHWORD, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InHyphenWordPart, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InHyphenWordPart, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHyphenNumWordPart, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_InParseHyphen, PARTHWORD, null)
    };

    private static final Item[] actionTPS_InHyphenAsciiWordPart = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, ASCIIPARTHWORD, null),
            i(p_isasclet, (char) 0, A_NEXT, S.TPS_InHyphenAsciiWordPart, 0, null),
            i(p_isalpha, (char) 0, A_NEXT, S.TPS_InHyphenWordPart, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InHyphenWordPart, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_InHyphenNumWordPart, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_InParseHyphen, ASCIIPARTHWORD, null)
    };

    private static final Item[] actionTPS_InHyphenNumWordPart = {
            i(p_isEOF, (char) 0, A_BINGO, S.TPS_Base, NUMPARTHWORD, null),
            i(p_isalnum, (char) 0, A_NEXT, S.TPS_InHyphenNumWordPart, 0, null),
            i(p_isspecial, (char) 0, A_NEXT, S.TPS_InHyphenNumWordPart, 0, null),
            i(null, (char) 0, A_BINGO, S.TPS_InParseHyphen, NUMPARTHWORD, null)
    };

    private static final Item[] actionTPS_InHyphenUnsignedInt = {
            i(p_isEOF, (char) 0, A_POP, S.TPS_Null, 0, null),
            i(p_isdigit, (char) 0, A_NEXT, S.TPS_Null, 0, null),
            i(p_isalpha, (char) 0, A_CLEAR, S.TPS_InHyphenNumWordPart, 0, null),
            i(p_isspecial, (char) 0, A_CLEAR, S.TPS_InHyphenNumWordPart, 0, null),
            i(null, (char) 0, A_POP, S.TPS_Null, 0, null)
    };

    /** Main table of per-state parser actions, indexed by state ordinal. */
    private static final Item[][] ACTIONS = {
            actionTPS_Base,
            actionTPS_InNumWord,
            actionTPS_InAsciiWord,
            actionTPS_InWord,
            actionTPS_InUnsignedInt,
            actionTPS_InSignedIntFirst,
            actionTPS_InSignedInt,
            actionTPS_InSpace,
            actionTPS_InUDecimalFirst,
            actionTPS_InUDecimal,
            actionTPS_InDecimalFirst,
            actionTPS_InDecimal,
            actionTPS_InVerVersion,
            actionTPS_InSVerVersion,
            actionTPS_InVersionFirst,
            actionTPS_InVersion,
            actionTPS_InMantissaFirst,
            actionTPS_InMantissaSign,
            actionTPS_InMantissa,
            actionTPS_InXMLEntityFirst,
            actionTPS_InXMLEntity,
            actionTPS_InXMLEntityNumFirst,
            actionTPS_InXMLEntityNum,
            actionTPS_InXMLEntityHexNumFirst,
            actionTPS_InXMLEntityHexNum,
            actionTPS_InXMLEntityEnd,
            actionTPS_InTagFirst,
            actionTPS_InXMLBegin,
            actionTPS_InTagCloseFirst,
            actionTPS_InTagName,
            actionTPS_InTagBeginEnd,
            actionTPS_InTag,
            actionTPS_InTagEscapeK,
            actionTPS_InTagEscapeKK,
            actionTPS_InTagBackSleshed,
            actionTPS_InTagEnd,
            actionTPS_InCommentFirst,
            actionTPS_InCommentLast,
            actionTPS_InComment,
            actionTPS_InCloseCommentFirst,
            actionTPS_InCloseCommentLast,
            actionTPS_InCommentEnd,
            actionTPS_InHostFirstDomain,
            actionTPS_InHostDomainSecond,
            actionTPS_InHostDomain,
            actionTPS_InPortFirst,
            actionTPS_InPort,
            actionTPS_InHostFirstAN,
            actionTPS_InHost,
            actionTPS_InEmail,
            actionTPS_InFileFirst,
            actionTPS_InFileTwiddle,
            actionTPS_InPathFirst,
            actionTPS_InPathFirstFirst,
            actionTPS_InPathSecond,
            actionTPS_InFile,
            actionTPS_InFileNext,
            actionTPS_InURLPathFirst,
            actionTPS_InURLPathStart,
            actionTPS_InURLPath,
            actionTPS_InFURL,
            actionTPS_InProtocolFirst,
            actionTPS_InProtocolSecond,
            actionTPS_InProtocolEnd,
            actionTPS_InHyphenAsciiWordFirst,
            actionTPS_InHyphenAsciiWord,
            actionTPS_InHyphenWordFirst,
            actionTPS_InHyphenWord,
            actionTPS_InHyphenNumWordFirst,
            actionTPS_InHyphenNumWord,
            actionTPS_InHyphenDigitLookahead,
            actionTPS_InParseHyphen,
            actionTPS_InParseHyphenHyphen,
            actionTPS_InHyphenWordPart,
            actionTPS_InHyphenAsciiWordPart,
            actionTPS_InHyphenNumWordPart,
            actionTPS_InHyphenUnsignedInt
    };

    // ------------------------------------------------------------------
    // Parser state
    // ------------------------------------------------------------------

    /** One entry of the parser's position stack (PG's TParserPosition). */
    private static final class Pos {
        int pos;          // position in code points, relative to the parser's start
        int charlen;      // 1 normally, 0 at end of input
        int lenTok;       // length of the token so far, in code points
        S state;
        Pos prev;
        int pushedAtAction = -1;

        Pos(Pos prev) {
            if (prev != null) {
                this.pos = prev.pos;
                this.charlen = prev.charlen;
                this.lenTok = prev.lenTok;
                this.state = prev.state;
            } else {
                this.state = S.TPS_Base;
            }
            this.prev = prev;
        }
    }

    private static final class TParser {
        final String str;
        final int[] cps;     // code points of str
        final int[] offs;    // code point index -> char offset in str; length cps.length+1
        final int start;     // code point index this parser begins at
        final int len;       // number of code points available to this parser

        Pos state;
        boolean ignore;
        boolean wanthost;
        char c;

        int tokStart;        // code point index (relative) where the current token began
        int lenTok;          // out: token length in code points
        int type;            // out: token type id

        private TParser(String str, int[] cps, int[] offs, int start, int len) {
            this.str = str;
            this.cps = cps;
            this.offs = offs;
            this.start = start;
            this.len = len;
            this.state = new Pos(null);
        }

        static TParser init(String s) {
            int n = s.codePointCount(0, s.length());
            int[] cps = new int[n];
            int[] offs = new int[n + 1];
            int ci = 0;
            for (int k = 0; k < n; k++) {
                offs[k] = ci;
                int cp = s.codePointAt(ci);
                cps[k] = cp;
                ci += Character.charCount(cp);
            }
            offs[n] = s.length();
            return new TParser(s, cps, offs, 0, n);
        }

        /**
         * A parser over the remainder of another parser's input, starting at its current
         * position. PG uses this so the recursive host / URL-path probes need not copy
         * the string.
         */
        static TParser copyInit(TParser orig) {
            return new TParser(orig.str, orig.cps, orig.offs,
                    orig.start + orig.state.pos, orig.len - orig.state.pos);
        }

        /** The code point under the cursor, or -1 at end of input. */
        int cur() {
            if (state.charlen == 0 || state.pos >= len) return -1;
            return cps[start + state.pos];
        }

        String tokenText() {
            return str.substring(offs[start + tokStart], offs[start + tokStart + lenTok]);
        }

        /** The token accumulated so far, as SpecialTags inspects it. */
        boolean pendingEqualsIgnoreCase(String s) {
            int from = offs[start + state.pos - state.lenTok];
            int to = offs[start + state.pos];
            return to - from == s.length() && str.regionMatches(true, from, s, 0, s.length());
        }

        /** PG's TParserGet: advance to the next token, returning false at end of input. */
        boolean get() {
            if (state.pos >= len) return false;

            tokStart = state.pos;
            state.pushedAtAction = -1;
            Item item = null;

            while (state.pos <= len) {
                state.charlen = (state.pos == len) ? 0 : 1;

                Item[] actions = ACTIONS[state.state.ordinal()];
                int idx;
                if (state.pushedAtAction >= 0) {
                    // After a POP, pick up at the next test
                    idx = state.pushedAtAction + 1;
                    state.pushedAtAction = -1;
                } else {
                    idx = 0;
                }

                // find action by character class
                while (idx < actions.length && actions[idx].isclass != null) {
                    c = actions[idx].c;
                    if (actions[idx].isclass.test(this)) break;
                    idx++;
                }
                item = actions[idx];

                // call special handler if exists
                if (item.special != null) item.special.apply(this);

                // BINGO, token is found
                if ((item.flags & A_BINGO) != 0) {
                    lenTok = state.lenTok;
                    state.lenTok = 0;
                    type = item.type;
                }

                // do various actions by flags
                if ((item.flags & A_POP) != 0) {
                    state = state.prev;
                } else if ((item.flags & A_PUSH) != 0) {
                    state.pushedAtAction = idx;
                    state = new Pos(state);
                } else if ((item.flags & A_CLEAR) != 0) {
                    state.prev = state.prev.prev;
                } else if ((item.flags & A_CLRALL) != 0) {
                    while (state.prev != null) state.prev = state.prev.prev;
                } else if ((item.flags & A_MERGE) != 0) {
                    Pos ptr = state;
                    state = state.prev;
                    state.pos = ptr.pos;
                    state.charlen = ptr.charlen;
                    state.lenTok = ptr.lenTok;
                }

                // set new state if pointed
                if (item.tostate != S.TPS_Null) state.state = item.tostate;

                // check for go away
                if ((item.flags & A_BINGO) != 0
                        || (state.pos >= len && (item.flags & A_RERUN) == 0)) {
                    break;
                }

                // rerun, or just restored state
                if ((item.flags & (A_RERUN | A_POP)) != 0) continue;

                // move forward
                if (state.charlen != 0) {
                    state.pos += state.charlen;
                    state.lenTok += state.charlen;
                }
            }

            return item != null && (item.flags & A_BINGO) != 0;
        }
    }

    // ------------------------------------------------------------------
    // Spacing combining marks (PG's strange_letter table)
    // ------------------------------------------------------------------

    private static boolean isStrangeLetter(int c) {
        int lo = 0, hi = STRANGE_LETTER.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (STRANGE_LETTER[mid] == c) return true;
            if (STRANGE_LETTER[mid] < c) lo = mid + 1;
            else hi = mid - 1;
        }
        return false;
    }

    private static final int[] STRANGE_LETTER = {
            0x0903, 0x093E, 0x093F, 0x0940, 0x0949, 0x094A, 0x094B, 0x094C,
            0x0982, 0x0983, 0x09BE, 0x09BF, 0x09C0, 0x09C7, 0x09C8, 0x09CB,
            0x09CC, 0x09D7, 0x0A03, 0x0A3E, 0x0A3F, 0x0A40, 0x0A83, 0x0ABE,
            0x0ABF, 0x0AC0, 0x0AC9, 0x0ACB, 0x0ACC, 0x0B02, 0x0B03, 0x0B3E,
            0x0B40, 0x0B47, 0x0B48, 0x0B4B, 0x0B4C, 0x0B57, 0x0BBE, 0x0BBF,
            0x0BC1, 0x0BC2, 0x0BC6, 0x0BC7, 0x0BC8, 0x0BCA, 0x0BCB, 0x0BCC,
            0x0BD7, 0x0C01, 0x0C02, 0x0C03, 0x0C41, 0x0C42, 0x0C43, 0x0C44,
            0x0C82, 0x0C83, 0x0CBE, 0x0CC0, 0x0CC1, 0x0CC2, 0x0CC3, 0x0CC4,
            0x0CC7, 0x0CC8, 0x0CCA, 0x0CCB, 0x0CD5, 0x0CD6, 0x0D02, 0x0D03,
            0x0D3E, 0x0D3F, 0x0D40, 0x0D46, 0x0D47, 0x0D48, 0x0D4A, 0x0D4B,
            0x0D4C, 0x0D57, 0x0D82, 0x0D83, 0x0DCF, 0x0DD0, 0x0DD1, 0x0DD8,
            0x0DD9, 0x0DDA, 0x0DDB, 0x0DDC, 0x0DDD, 0x0DDE, 0x0DDF, 0x0DF2,
            0x0DF3, 0x0F3E, 0x0F3F, 0x0F7F, 0x102B, 0x102C, 0x1031, 0x1038,
            0x103B, 0x103C, 0x1056, 0x1057, 0x1062, 0x1063, 0x1064, 0x1067,
            0x1068, 0x1069, 0x106A, 0x106B, 0x106C, 0x106D, 0x1083, 0x1084,
            0x1087, 0x1088, 0x1089, 0x108A, 0x108B, 0x108C, 0x108F, 0x109A,
            0x109B, 0x109C, 0x17B6, 0x17BE, 0x17BF, 0x17C0, 0x17C1, 0x17C2,
            0x17C3, 0x17C4, 0x17C5, 0x17C7, 0x17C8, 0x1923, 0x1924, 0x1925,
            0x1926, 0x1929, 0x192A, 0x192B, 0x1930, 0x1931, 0x1933, 0x1934,
            0x1935, 0x1936, 0x1937, 0x1938, 0x19B0, 0x19B1, 0x19B2, 0x19B3,
            0x19B4, 0x19B5, 0x19B6, 0x19B7, 0x19B8, 0x19B9, 0x19BA, 0x19BB,
            0x19BC, 0x19BD, 0x19BE, 0x19BF, 0x19C0, 0x19C8, 0x19C9, 0x1A19,
            0x1A1A, 0x1A1B, 0x1B04, 0x1B35, 0x1B3B, 0x1B3D, 0x1B3E, 0x1B3F,
            0x1B40, 0x1B41, 0x1B43, 0x1B44, 0x1B82, 0x1BA1, 0x1BA6, 0x1BA7,
            0x1BAA, 0x1C24, 0x1C25, 0x1C26, 0x1C27, 0x1C28, 0x1C29, 0x1C2A,
            0x1C2B, 0x1C34, 0x1C35, 0x1CE1, 0x1CF2, 0xA823, 0xA824, 0xA827,
            0xA880, 0xA881, 0xA8B4, 0xA8B5, 0xA8B6, 0xA8B7, 0xA8B8, 0xA8B9,
            0xA8BA, 0xA8BB, 0xA8BC, 0xA8BD, 0xA8BE, 0xA8BF, 0xA8C0, 0xA8C1,
            0xA8C2, 0xA8C3, 0xA952, 0xA953, 0xAA2F, 0xAA30, 0xAA33, 0xAA34,
            0xAA4D
    };
}
