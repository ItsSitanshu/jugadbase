#include "lexer.h"

#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>


char* keywords[NO_OF_KEYWORDS] = {
  "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "FROM", "WHERE",
  "AND", "OR", "NOT", "ORDER", "BY", "GROUP", "HAVING", "LIM", "OFFSET", "VALUES",
  "SET", "INTO", "AS", "JOIN", "ON", "IN", "IS", "NULL", "DISCT", "PRIMKEY",
  "FRNKEY", "REF", "INDEX", "CAST", "CASE", "WHEN", "THEN", "ELSE", "END", "DEFAULT",
  "CHECK", "UNIQUE", "CONSTR", "fSUM", "INT", "VARCHAR", "CHAR", "TEXT",
  "BOOLEAN", "FLOAT", "DOUBLE", "DECIMAL", "DATE", "TIME", "DATETIME", "TIMESTAMP", "BLOB", "JSON",
  "UUID", "SERIAL"
};



Lexer* lexer_init() {
  Lexer* lexer = calloc(1, sizeof(Lexer));
  if (!lexer) {
    exit(EXIT_FAILURE);
  }
  
  lexer->buf = NULL;
  lexer->buf_size = 0;
  lexer->i = 0;
  lexer->c = '\0';
  lexer->cc = 1;
  lexer->cl = 1;
  
  return lexer;
}


void lexer_set_buffer(Lexer* lexer, char* buffer) {
  if (!lexer) return;

  free(lexer->buf); 

  lexer->buf = strdup(buffer);
  if (!lexer->buf) {
    exit(EXIT_FAILURE);
  }

  lexer->buf_size = strlen(lexer->buf);
  lexer->buf[lexer->buf_size] = '\0';

  lexer->i = 0;
  lexer->c = lexer->buf[lexer->i];
  lexer->cc = 1;
  lexer->cl = 1;
}


void lexer_free(Lexer* lexer) {
  if (!lexer) {
    return;
    exit(EXIT_FAILURE);
  }

  free(lexer->buf);
  free(lexer);

  lexer = NULL;
}

Token* lexer_token_init(Lexer* lexer, char* value, uint8_t type) {
  /*
  Initializes token with provided values 
  return: pointer to a propperly initalized token struct
  */

  Token* token = calloc(1, sizeof(Token));

  if (!token) {
    exit(EXIT_FAILURE);
  }

  token->type = type;  
  token->col = lexer->cc;
  token->line = lexer->cl;

  token->value = strdup(value);

  return (Token*)token;
}

void token_free(Token* token) {
  /*
  De-initializes provided token
  */

  if (!token) {
    exit(EXIT_FAILURE);
    return;
  }

  free(token->value);
  free(token);
}

Token* lexer_next_token(Lexer* lexer) {
  Token* token;
  lexer_handle_fillers(lexer);
  

  if (lexer->c == '\0' || lexer->i >= lexer->buf_size) {
    token = lexer_token_init(lexer, "\0", TOK_EOF);
  } else if (isalpha(lexer->c) || lexer->c == '_') {
    token = lexer_handle_alpha(lexer);
  } else if (isdigit(lexer->c)) {
    token = lexer_handle_numeric(lexer, false);
  } else {
    token = lexer_handle_1char(lexer);
    if (token->type == TOK_ERR) {
      lexer_handle_error(lexer);
      return lexer_next_token(lexer);
    }
  }

  return token;
}

char* lexer_peek(Lexer* lexer, int8_t offset) {
  /*
  Extracts provided # of characters ahead of the current lexer position
  Returns: NULL (EOF), EXIT_FAILURE (alloc error), char* (valid peek)
  */

  if (offset < 0 && (size_t)(-offset) > lexer->i) {
    return NULL;
  }

  size_t peek_start = (size_t)(offset < 0 ? lexer->i + offset : lexer->i);
  size_t peek_length = (size_t)(offset < 0 ? -offset : offset);

  if (peek_start >= lexer->buf_size || peek_start + peek_length > lexer->buf_size) {
    return NULL;
  }

  size_t mem = peek_length + 1;
  char* info = calloc(mem, sizeof(char));
  if (!info) {
    free(info);
    perror("Failed to allocate memory for peek");
    exit(EXIT_FAILURE);
  }

  strncpy(info, lexer->buf + peek_start, peek_length);
  info[peek_length] = '\0';

  return info;
}


char lexer_peep(Lexer* lexer, int8_t offset) {
  /*
  Extracts character of position of the provided # from the current lexer position
  Returns: '\0' (EOF), char (valid peek)
  */

  size_t peep_index = (size_t)(lexer->i + offset);

  if (peep_index >= lexer->buf_size || peep_index < 0) {
      return '\0';
  }

  return lexer->buf[peep_index];
}


void lexer_advance(Lexer* lexer, uint8_t offset) {
  if (lexer->i + offset >= lexer->buf_size) {
    lexer->c = '\0';
    return;
  }
  lexer->i += offset;
  lexer->cc += 1;
  lexer->c = lexer->buf[lexer->i];
}

void lexer_handle_error(Lexer* lexer) {
  while (lexer->c != ';' && lexer->c != '\0' && lexer->c != ' ' && lexer->c != '\n' &&
      lexer->c != '\v' && lexer->c != '\t' && lexer->buf[lexer->i + 1] != '\0' &&
      lexer->i + 1 < lexer->buf_size) {
    lexer_advance(lexer, 1);
  }
}

void lexer_handle_fillers(Lexer* lexer) {
  /*
  Skips un-importaint values (whitespace, newline, horizontal/vertical tab)
  */

  switch (lexer->c) {
    case ' ':
      lexer_advance(lexer, 1);
      break;
    case '\n':
    case '\v':
      lexer->cl += 1;
      lexer_advance(lexer, 1);
      lexer->cc = 1;
      break;
    case '\t':
      lexer_advance(lexer, 2);
      break;
    default:
      break;
  }
}


bool lexer_handle_comments(Lexer* lexer) {
  /*
  Skips / * ..  until it finds  * /
  Skips // .. until new line

  return: true if it was a comment else, false 
  */

  char* peek = lexer_peek(lexer, 2);

  if (strcmp(peek, "//") == 0) {
    while (lexer->c != '\n') {
      lexer_advance(lexer, 1);
    }
    lexer_advance(lexer, 1);
    free(peek);
    return true;
  }


  if (strcmp(peek, "/*") == 0) {
    while (strcmp(peek, "*/") != 0) {
      lexer_advance(lexer, 1);
    }
    lexer_advance(lexer, 2);
    free(peek);
    return true;
  }

  free(peek);
  return false;
}

Token* lexer_handle_alpha(Lexer* lexer) {
  /* 
  Identifies and creates TOK_ID or keyword tokens [TOK_IMPORT -> TOK_AS]
  return: identifiers and keywords
  */

  char* buf = calloc(1, sizeof(char));
  if (!buf) {
    exit(EXIT_FAILURE);
  }

  while (isalnum(lexer->c) || lexer->c == '_') {
    size_t len = strlen(buf);
    buf = realloc(buf, (len + 2) * sizeof(char));
    
    if (!buf) {
      exit(EXIT_FAILURE);
    }
    
    if (len > MAX_IDENTIFIER_LEN) {
      REPORT_ERROR(lexer, "E_SHORTER_LENIDEN", MAX_IDENTIFIER_LEN);
      lexer_handle_error(lexer);
      free(buf);
      return lexer_next_token(lexer);
    }

    buf[len] = lexer->c;
    buf[len + 1] = '\0';
    lexer_advance(lexer, 1);
  } 

  if (strlen(buf) > MAX_KEYWORD_LEN) {
    Token* token = lexer_token_init(lexer, buf, TOK_ID);
    free(buf);
    return token;
  }

  uint8_t KWCHAR_TYPE_MAP[NO_OF_KEYWORDS] = {
    TOK_SEL, TOK_INS, TOK_UPD, TOK_DEL, TOK_CRT, TOK_DRP, TOK_ALT, TOK_TBL, TOK_FRM, TOK_WR,
    TOK_AND, TOK_OR, TOK_NOT, TOK_ODR, TOK_BY, TOK_GRP, TOK_HAV, TOK_LIM, TOK_OFF, TOK_VAL,
    TOK_SET, TOK_INTO, TOK_AS, TOK_JN, TOK_ON, TOK_IN, TOK_IS, TOK_NL, TOK_DST, TOK_PK,
    TOK_FK, TOK_REF, TOK_IDX, TOK_CST, TOK_CSE, TOK_WHEN, TOK_THEN, TOK_ELS, TOK_END, TOK_DEF,
    TOK_CHK, TOK_UNQ, TOK_CNST, TOK_FNSUM, TOK_T_INT, TOK_T_VARCHAR, TOK_T_CHAR, TOK_T_TEXT,
    TOK_T_BOOL, TOK_T_FLOAT, TOK_T_DOUBLE, TOK_T_DECIMAL, TOK_T_DATE, TOK_T_TIME, TOK_T_DATETIME,
    TOK_T_TIMESTAMP, TOK_T_BLOB, TOK_T_JSON, TOK_T_UUID, TOK_T_SERIAL
  };

  for (uint8_t i = 0; i < NO_OF_KEYWORDS; i++) {
    if (strcmp(KEYWORDS[i], buf) == 0) {
      Token* token = lexer_token_init(lexer, buf, KWCHAR_TYPE_MAP[i]);
      free(buf);
      return token;
    }
  }


  Token* token = lexer_token_init(lexer, buf, TOK_ID);
  free(buf);
  return token;
}


Token* lexer_handle_numeric(Lexer* lexer, bool is_negative) {
  /*
  Identifies and creates numeric tokens 
  return: numeric tokens [TOK_L_I8-> TOK_L_DOUBLE]
  */
  
  int type = TOK_ERR;
  __uint8_t npre_decimal = 0;
  char* buf = calloc(2, sizeof(char));

  if (!buf) {
    perror("Failed to allocate memory for buffer");
    exit(EXIT_FAILURE);
  }

  if (is_negative) {
    strcat(buf, (char[]){'-', '\0'}); 
    lexer_advance(lexer, 1);
  }

  lexer_process_digits(lexer, &buf, false);
  type = lexer_process_int_type(buf);

  if (lexer->c == '.') {
    size_t new_size = strlen(buf) + 2;
    char* new_buf = realloc(buf, new_size * sizeof(char));
    
    if (!new_buf) {
        perror("Failed to reallocate memory for buffer");
        free(buf);
        exit(EXIT_FAILURE);
    }
    buf = new_buf;

    strcat(buf, (char[]){lexer->c, '\0'});
    lexer_advance(lexer, 1);

    npre_decimal = strlen(buf); // # of digits before decimal

    lexer_process_digits(lexer, &buf, true);

    type = lexer_process_decimal_type(buf, (strlen(buf) - npre_decimal));
  }

  if (type == TOK_ERR) {
    REPORT_ERROR(lexer, "U_NUM_LIT_TYPE");
    lexer_handle_error(lexer);
    free(buf);
    return lexer_next_token(lexer);
  }

  Token* token = lexer_token_init(lexer, buf, type);
  free(buf);
  return token;
}



Token* lexer_handle_1char(Lexer* lexer) {
  /*
  Identifies and creates tokens for op-erator and checks for possible operator combinations
  return: operators[TOK_LP -> TOK_AA] or, char/string/int literal tokens
  */

  char next_char = lexer_peep(lexer, 1);

  switch (lexer->c) {
    case '<':
      return lexer_process_pos_singlechar(lexer, next_char,
        '=', TOK_LT, TOK_LE);
      break;
    case '>':
      return lexer_process_pos_singlechar(lexer, next_char,
        '=', TOK_GT, TOK_GE);
      break;
    case '|':
      return lexer_process_pos_singlechar(lexer, next_char,
        '|', TOK_ERR, TOK_PP);
      break;
    case '!':
      return lexer_process_pos_singlechar(lexer, next_char,
        '=', TOK_ERR, TOK_NE);
      break;
    case '&':
      return lexer_process_pos_singlechar(lexer, next_char,
        '&', TOK_ERR, TOK_AA);
      break;
    case ':':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, ":", TOK_COL);
      break;
    case '%':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "%", TOK_MOD);
      break;
    case '+':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "+", TOK_ADD);
      break;
    case '*':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "*", TOK_MUL);
      break;
    case '/':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "/", TOK_DIV);
      break;
    case '.':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, ".", TOK_DOT);
      break;
    case ',':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, ",", TOK_COM);
      break;
    case ';':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, ";", TOK_SC);
      break;
    case '(':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "(", TOK_LP);
      break;
    case ')':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, ")", TOK_RP);
      break;
    case '[':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "[", TOK_LB);
      break;
    case ']':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "]", TOK_RB);
      break;
    case '-':
      return lexer_process_minus_op(lexer, next_char);
      break;
    case '\"':
      return lexer_process_double_quote(lexer);
      break;
    default:
      break;
  }

  return lexer_next_token(lexer);
}

void lexer_process_digits(Lexer* lexer, char** buf, bool has_decimal) {
  while (isdigit(lexer->c) ||
      ((lexer->c == 'f' || lexer->c == 'F' || lexer->c == 'd' || lexer->c == 'D') && has_decimal == true)) {
    size_t len = strlen(*buf);
    *buf = realloc(*buf, (len + 2) * sizeof(char));

    if (!*buf) {
        exit(EXIT_FAILURE);
    }

    (*buf)[len] = lexer->c;
    (*buf)[len + 1] = '\0';


    lexer_advance(lexer, 1);
  }

  if (lexer->c == '_' || lexer->c == '\'') {
    if (isdigit(lexer_peep(lexer, -1)) && isdigit(lexer_peep(lexer, 1))) {
      lexer_advance(lexer, 1);
      lexer_process_digits(lexer, buf, has_decimal);
    } else {
      free(*buf);
      lexer_handle_error(lexer);
    }
  } else if (lexer->c == '.' && has_decimal) {
    free(*buf);
    lexer_handle_error(lexer);
  }
}


uint8_t lexer_process_decimal_type(char* buf, uint8_t diadc) {
  /*
  Processes provided buffer and resolves type; TOK_ERR if invalid
  return: TOK_ERR, TOK_L_FLOAT (32-bit float), or TOK_L_DOUBLE (64-bit float)
  */

  bool has_Fpfx = false; // has float prefix
  bool has_Dpfx = false; // has double prefix

  for (uint8_t i = 0; i < strlen(buf); i++) {
    if (buf[i] == 'f' || buf[i] == 'F') {
      if (has_Fpfx) { 
        return TOK_ERR;
      }
      has_Fpfx = true;
    } else if (buf[i] == 'd' || buf[i] == 'D') {
      if (has_Dpfx) { 
        return TOK_ERR;
      }
      has_Dpfx = true;
    }
  }

  if (has_Fpfx) {
      return TOK_L_FLOAT;
  } else if (has_Dpfx) {
      return TOK_L_DOUBLE;
  }

  if (diadc <= MAX_FLOAT_LIT_DIGITS) {
      return TOK_L_FLOAT;
  } else if (diadc <= MAX_DOUBLE_LIT_DIGITS) {
      return TOK_L_DOUBLE;
  }

  return TOK_ERR;
}


int is_within_int_range(int128_t val, int128_t min, int128_t max) {
  if (val.high > max.high || (val.high == max.high && val.low > max.low)) {
    return 0;
  }
  if (val.high < min.high || (val.high == min.high && val.low < min.low)) {
    return 0;
  }
  return 1;
}

int is_within_uint_range(uint128_t val, uint128_t max) {
  if (val.high > max.high || (val.high == max.high && val.low > max.low)) {
    return 0;
  }
  return 1;
}

uint8_t lexer_process_int_type(char* buf) {
  char *endptr;
  int64_t signed_val;
  uint64_t usigned_val;

  buf[strlen(buf)] = '\0';

  if (*buf == '-') {
    int128_t lsigned_val;
    strtoint128(buf, lsigned_val);

    int128_t int128_max = { .high = 0x7FFFFFFFFFFFFFFF, .low = 0xFFFFFFFFFFFFFFFF };
    int128_t int128_min = { .high = 0x8000000000000000, .low = 0 };

    signed_val = strtoll(buf, &endptr, 10) - 1;

    if (endptr == buf || *endptr != '\0') {
      return TOK_ERR;
    }

    if (is_within_int_range(lsigned_val, int128_min, int128_max)) {
      if (lsigned_val.high <= 0) {
        if (signed_val >= INT8_MIN && signed_val <= INT8_MAX) {
          return TOK_L_I8;
        } else if (signed_val >= INT16_MIN && signed_val <= INT16_MAX) {
          return TOK_L_I16;
        } else if (signed_val >= INT32_MIN && signed_val <= INT32_MAX) {
          return TOK_L_I32;
        } else if (signed_val >= INT64_MIN && signed_val <= INT64_MAX) {
          return TOK_L_I64;
        }
      } else if (lsigned_val.high > 0) {
        return TOK_L_I128;
      }
    }        
    return TOK_ERR;
  } else {
    uint128_t unsigned_val;
    strtouint128(buf, unsigned_val);

    usigned_val = strtoull(buf, &endptr, 10);
    signed_val = strtoll(buf, &endptr, 10);

    if (endptr == buf || *endptr != '\0') {
        return TOK_ERR;
    }

    uint128_t uint128_max = { .high = 0xFFFFFFFFFFFFFFFF, .low = 0xFFFFFFFFFFFFFFFF };

    if (is_within_uint_range(unsigned_val, uint128_max)) {
      if (unsigned_val.low > 0) {
        return TOK_L_U128;
      } else if (usigned_val >= 0 && usigned_val <= UINT8_MAX) {
        return TOK_L_U8;
      } else if (usigned_val >= 0 && usigned_val <= UINT16_MAX) {
        return TOK_L_U16;
      } else if (usigned_val >= 0 && usigned_val <= UINT32_MAX) {
        return TOK_L_U32;
      } else if (usigned_val >= 0 && usigned_val <= UINT64_MAX) {
        return TOK_L_U64;
      }
    } else if (signed_val >= INT8_MIN && signed_val <= INT8_MAX) {
      return TOK_L_I8;
    } else if (signed_val >= INT16_MIN && signed_val <= INT16_MAX) {
      return TOK_L_I16;
    } else if (signed_val >= INT32_MIN && signed_val <= INT32_MAX) {
      return TOK_L_I32;
    } else if (signed_val >= INT64_MIN && signed_val <= INT64_MAX) {
      return TOK_L_I64;
    }
  }

  return TOK_ERR;
}

Token* lexer_process_pos_singlechar(Lexer* lexer, char next_char,
  char c_pos1, uint8_t t_pos0, uint8_t t_pos1) {

  char c_pos0 = lexer->c;

  if (next_char == c_pos1) {
    lexer_advance(lexer, 2);
    return lexer_token_init(lexer, (char*)(&(char[]){c_pos0, c_pos1, '\0'}), t_pos1);
  }

  lexer_advance(lexer, 1);
  return lexer_token_init(lexer, (char*)(&(char[]){c_pos0, '\0'}), t_pos0);
}
   
Token* lexer_process_minus_op(Lexer* lexer, char next_char) {
  if isdigit(next_char) {
    return lexer_handle_numeric(lexer, true);
  }

  lexer_advance(lexer, 1);
  return lexer_token_init(lexer, "-", TOK_SUB);
}

Token* lexer_process_double_quote(Lexer* lexer) {
  lexer_advance(lexer, 1); // consume "

  char* value = calloc(1, sizeof(char));

  if (!value) {
    exit(EXIT_FAILURE);
  }

  while (lexer->c != '"' && lexer->c != '\0') {
    size_t len = strlen(value);
    value = realloc(value, (len + 2) * sizeof(char));

    if (!value) {
      exit(EXIT_FAILURE);
    }

    value[len] = lexer->c;
    value[len + 1] = '\0';
    lexer_advance(lexer, 1); // consume char
  }

  if (lexer->c == '"') {
    lexer_advance(lexer, 1);
    Token* token = lexer_token_init(lexer, value, TOK_L_STRING);
    free(value);
    return token;
  }

  REPORT_ERROR(lexer, "E_STRING_TERMINATOR");
  lexer_handle_error(lexer);
  free(value);
  return lexer_next_token(lexer);
}


struct ErrorTemplate templates[] = {
  {"SYE_UNSUPPORTED", "Unsupported syntax"},
  {"SYE_E_CNA", "Expected a column name"},
  {"SYE_E_MEM", "Memory allocation error"},
  {"SYE_E_CTYPE", "Invalid column type"},
  {"SYE_E_TAFCR", "Expected 'TABLE' after 'CREATE'"},
  {"SYE_E_TNAFTA", "Expected table name after 'TABLE'"},
  {"SYE_E_PRNAFDYNA", "Expected opening parenthesis after table name"},
  {"SYE_E_CPRORCOM", "Expected closing parenthesis or comma"},
  {"SYE_E_CPR", "Expected closing parenthesis"},
};

char* lexer_get_reference(Lexer* lexer) {
  size_t line_start = lexer->i;

  while (line_start > 0 && lexer->buf[line_start - 1] != '\n') {
      line_start--;
  }

  size_t line_end = lexer->i;

  while (lexer->buf[line_end] != '\0' && lexer->buf[line_end] != '\n') {
      line_end++;
  }

  size_t line_length = line_end - line_start;
  char* line_content = (char*)malloc((line_length + 2) * sizeof(char));

  if (!line_content) {
      free(line_content);
      exit(EXIT_FAILURE);
  }

  strncpy(line_content, lexer->buf + line_start, line_length);

  line_content[line_length] = '\0';

  return line_content;
}

void lexer_report_error(Lexer* lexer, char* error_code, ...) {
  // if (error_code[0] != 'U' && error_code[0] != 'E') {
  //   return;
  // }

  for (size_t i = 0; i < (sizeof(templates) / sizeof(templates[0])); i++) {        
    if (strcmp(templates[i].code, error_code) == 0) {
      va_list args;
      va_start(args, error_code);

      char* final_content = malloc(
        (strlen(templates[i].content) + sizeof(args)) * sizeof(char)
      );

      if (final_content != NULL) {
        vsprintf(final_content, templates[i].content, args);
      }

      va_end(args);

      char* refrence = lexer_get_reference(lexer);

      printf("%d:%d > %s\n\t%d | %s\n", lexer->cl, lexer->cc, final_content, lexer->cl, refrence);
      free(final_content);
      return;
    }
  }

  char* refrence = lexer_get_reference(lexer);
  printf("%d:%d > %s\n\t%d | %s\n", lexer->cl, lexer->cc, error_code, lexer->cl, refrence);
}