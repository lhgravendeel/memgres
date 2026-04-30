package com.memgres.engine.parser;

/**
 * Token types for the SQL lexer.
 */
public enum TokenType {
    // Literals
    INTEGER_LITERAL,
    FLOAT_LITERAL,
    STRING_LITERAL,
    DOLLAR_STRING_LITERAL,
    BIT_STRING_LITERAL,  // B'1010' or X'1F' (stored as binary string of 0s and 1s)

    // Identifiers and keywords
    IDENTIFIER,
    QUOTED_IDENTIFIER,
    KEYWORD,

    // Operators
    EQUALS,          // =
    NOT_EQUALS,      // != or <>
    LESS_THAN,       // <
    GREATER_THAN,    // >
    LESS_EQUALS,     // <=
    GREATER_EQUALS,  // >=
    PLUS,            // +
    MINUS,           // -
    STAR,            // *
    SLASH,           // /
    PERCENT,         // %
    CARET,           // ^
    AMPERSAND,       // &
    PIPE,            // |
    TILDE,           // ~
    DOUBLE_TILDE,    // ~~ (LIKE operator form)
    DOUBLE_TILDE_STAR, // ~~* (ILIKE operator form)
    NOT_DOUBLE_TILDE, // !~~ (NOT LIKE operator form)
    NOT_DOUBLE_TILDE_STAR, // !~~* (NOT ILIKE operator form)
    HASH,            // #
    SHIFT_LEFT,      // <<
    SHIFT_RIGHT,     // >>
    CONCAT,          // ||
    CAST,            // ::
    JSON_ARROW,      // ->
    JSON_ARROW_TEXT, // ->>
    JSON_HASH_ARROW, // #>
    JSON_HASH_ARROW_TEXT, // #>>
    AT_SIGN,         // @
    CONTAINS,        // @>
    CONTAINED_BY,    // <@
    OVERLAP,         // &&
    TS_MATCH,        // @@
    JSONB_PATH_EXISTS_OP, // @?
    JSONB_EXISTS,    // ?
    JSONB_EXISTS_ANY, // ?|
    JSONB_EXISTS_ALL, // ?&
    JSON_DELETE_PATH, // #-
    INET_CONTAINS_EQUALS, // >>=
    INET_CONTAINED_BY_EQUALS, // <<=
    DISTANCE,        // <->
    APPROX_EQUAL,    // ~=
    TILDE_STAR,      // ~*
    EXCL_TILDE,      // !~
    EXCL_TILDE_STAR, // !~*
    GEO_BELOW,       // <<|
    GEO_ABOVE,       // |>>
    GEO_NOT_EXTEND_RIGHT,  // &<
    GEO_NOT_EXTEND_LEFT,   // &>
    GEO_NOT_EXTEND_ABOVE,  // &<|
    GEO_NOT_EXTEND_BELOW,  // |&>
    RANGE_ADJACENT,  // -|-
    GEO_INTERSECTS,  // ?#
    GEO_CLOSEST_POINT, // ##
    GEO_PARALLEL,    // ?||
    GEO_PERPENDICULAR, // ?-|
    GEO_IS_HORIZONTAL, // ?- (prefix)
    PARAM,           // $1, $2, etc.

    // Punctuation
    LEFT_PAREN,      // (
    RIGHT_PAREN,     // )
    LEFT_BRACKET,    // [
    RIGHT_BRACKET,   // ]
    COMMA,           // ,
    SEMICOLON,       // ;
    DOT,             // .
    COLON,           // :
    COLON_EQUALS,    // :=
    FAT_ARROW,       // =>

    // User-defined operator (multi-char operator not matching any built-in token)
    CUSTOM_OPERATOR, // e.g. +++, <=>, ~~>, etc.

    // Special
    EOF,
    ERROR
}
