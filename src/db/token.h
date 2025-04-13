#ifndef TOKEN_H
#define TOKEN_H

#include <stdint.h>

#define NO_OF_KEYWORDS 63
#define KEYWORDS keywords

#define MAX_KEYWORD_LEN 9
#define MAX_IDENTIFIER_LEN 256
#define MAX_FLOAT_LIT_DIGITS 7
#define MAX_DOUBLE_LIT_DIGITS 15
#define WRITE_ERRORS_TO 0

typedef struct RowID { uint32_t page_id; uint16_t row_id;} RowID;

typedef struct Token {
  unsigned int line, col;
  char *value;
  enum TokenType {
    // Data Types
    TOK_T_INT,           // INTEGER 
    TOK_T_VARCHAR,       // VARCHAR
    TOK_T_CHAR,          // CHAR
    TOK_T_TEXT,          // TEXT
    TOK_T_BOOL,          // BOOLEAN
    TOK_T_FLOAT,         // FLOAT
    TOK_T_DOUBLE,        // DOUBLE
    TOK_T_DECIMAL,       // DECIMAL
    TOK_T_DATE,          // DATE
    TOK_T_TIME,          // TIME
    TOK_T_DATETIME,      // DATETIME
    TOK_T_TIMESTAMP,     // TIMESTAMP
    TOK_T_BLOB,          // BLOB
    TOK_T_JSON,          // JSON
    TOK_T_UUID,          // UUID
    TOK_T_SERIAL,          // SERIAL
    TOK_T_UINT,            // UINT

    // Special tokens
    TOK_ERR,      // Error token
    TOK_EOF,      // End-of-file (\0)
    TOK_ID,       // Identifier (table/column names)

    // Symbols
    TOK_LP,       // (
    TOK_RP,       // )
    TOK_LB,       // [
    TOK_RB,       // ]
    TOK_COM,      // ,
    TOK_SC,       // ;
    TOK_EQ,       // =
    TOK_LT,       // <
    TOK_GT,       // >
    TOK_LE,       // <=
    TOK_GE,       // >=
    TOK_NE,       // !=
    TOK_ADD,      // +
    TOK_SUB,      // -
    TOK_MUL,      // *
    TOK_DIV,      // /
    TOK_MOD,      // %
    TOK_DOT,      // .
    TOK_COL,      // :
    TOK_PP,       // ||
    TOK_AA,       // &&

    // Slightly Shortened SQL Keywords
    TOK_SEL,      // SELECT
    TOK_INS,      // INSERT
    TOK_UPD,      // UPDATE
    TOK_DEL,      // DELETE
    TOK_CRT,      // CREATE
    TOK_DRP,      // DROP
    TOK_ALT,      // ALTER
    TOK_TBL,      // TABLE
    TOK_FRM,      // FROM
    TOK_WR,       // WHERE
    TOK_AND,      // AND
    TOK_OR,       // OR
    TOK_NOT,      // NOT
    TOK_ODR,      // ORDER
    TOK_BY,       // BY
    TOK_GRP,      // GROUP
    TOK_HAV,      // HAVING
    TOK_LIM,      // LIMIT
    TOK_OFF,      // OFFSET
    TOK_VAL,      // VALUES
    TOK_SET,      // SET
    TOK_INTO,      // INTO
    TOK_AS,       // AS (alias)
    TOK_JN,       // JOIN
    TOK_ON,       // ON
    TOK_IN,       // IN
    TOK_IS,       // IS
    TOK_NL,       // NULL
    TOK_DST,      // DISTINCT
    TOK_PK,       // PRIMARY KEY
    TOK_FK,       // FOREIGN KEY
    TOK_REF,      // REFERENCES
    TOK_IDX,      // INDEX
    TOK_CST,      // CAST
    TOK_CSE,      // CASE
    TOK_WHEN,     // WHEN
    TOK_THEN,     // THEN
    TOK_ELS,      // ELSE
    TOK_END,      // END
    TOK_DEF,      // DEFAULT
    TOK_CHK,      // CHECK
    TOK_UNQ,      // UNIQUE
    TOK_CNST,     // CONSTRAINT
    TOK_FNSUM,     // fSUM

    // Sorting & Transactions
    TOK_ASC,      // ASC (Ascending Sort)
    TOK_DESC,     // DESC (Descending Sort)
    TOK_BEG,      // BEGIN (Transaction)
    TOK_CMT,      // COMMIT
    TOK_RBK,      // ROLLBACK

    // Literals
    TOK_L_UINT,
    TOK_L_INT,
    TOK_L_FLOAT,            // 32-bit floating-point
    TOK_L_DOUBLE,           // 64-bit double-precision floating-point
    TOK_L_CHAR,             // 8-bit character
    TOK_L_STRING,           // Dynamic array of characters (string)
    TOK_L_BOOL,             // 8-bit boolean
  } type;
} Token;

#define VALID_TYPES_MASK ( \
  (1 << TOK_T_INT) |      \
  (1 << TOK_T_VARCHAR) |  \
  (1 << TOK_T_CHAR) |     \
  (1 << TOK_T_TEXT) |     \
  (1 << TOK_T_BOOL) |     \
  (1 << TOK_T_FLOAT) |    \
  (1 << TOK_T_DOUBLE) |   \
  (1 << TOK_T_DECIMAL) |  \
  (1 << TOK_T_DATE) |     \
  (1 << TOK_T_TIME) |     \
  (1 << TOK_T_DATETIME) | \
  (1 << TOK_T_TIMESTAMP) |\
  (1 << TOK_T_BLOB) |     \
  (1 << TOK_T_JSON) |     \
  (1 << TOK_T_UUID) |     \
  (1 << TOK_T_SERIAL) |      \
  (1 << TOK_T_UINT)       \
)

#endif // TOKEN_H
