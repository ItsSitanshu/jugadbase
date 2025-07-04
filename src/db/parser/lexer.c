#include "parser/lexer.h"

#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>


char* keywords[NO_OF_KEYWORDS] = {
  "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE",
  "FROM", "WHERE", "AND", "OR", "NOT", "ORDER", "BY", "GROUP",
  "HAVING", "LIM", "OFFSET", "VALUES", "SET", "INTO", "AS", "JOIN",
  "ON", "IN", "IS", "NULL", "DISCT", "PRIMKEY", "FRNKEY", "REFERENCES",
  "INDEX", "CAST", "CASE", "WHEN", "THEN", "ELSE", "END", "DEFAULT",
  "CHECK", "UNIQUE", "CONSTRAINT", "INT", "VARCHAR", "CHAR", "TEXT", "BOOL",
  "FLOAT", "DOUBLE", "DECIMAL", "DATE", "TIME", "TIMETZ", "DATETIME", "DATETIMETZ",
  "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL", "BLOB", "JSON", "UUID", "SERIAL", "true",
  "false", "UINT", "LIKE", "BETWEEN", "ASC", "DESC", "IF", "EXISTS",
  "CASCADE", "RESTRICT", "RETURNING", "TO", "RENAME", "TABLESPACE", "OWNER", "ADD",
  "COLUMN", "_unsafecon"
};

uint8_t KWCHAR_TYPE_MAP[NO_OF_KEYWORDS] = {
  TOK_SEL, TOK_INS, TOK_UPD, TOK_DEL, TOK_CRT, TOK_DRP, TOK_ALT, TOK_TBL,
  TOK_FRM, TOK_WR, TOK_AND, TOK_OR, TOK_NOT, TOK_ODR, TOK_BY, TOK_GRP,
  TOK_HAV, TOK_LIM, TOK_OFF, TOK_VAL, TOK_SET, TOK_INTO, TOK_AS, TOK_JN,
  TOK_ON, TOK_IN, TOK_IS, TOK_NL, TOK_DST, TOK_PK, TOK_FK, TOK_REF,
  TOK_IDX, TOK_CST, TOK_CSE, TOK_WHEN, TOK_THEN, TOK_ELS, TOK_END, TOK_DEF,
  TOK_CHK, TOK_UNQ, TOK_CNST, TOK_T_INT, TOK_T_VARCHAR, TOK_T_CHAR, TOK_T_TEXT, TOK_T_BOOL,
  TOK_T_FLOAT, TOK_T_DOUBLE, TOK_T_DECIMAL, TOK_T_DATE, TOK_T_TIME, TOK_T_TIME_TZ, TOK_T_DATETIME, TOK_T_DATETIME_TZ,
  TOK_T_TIMESTAMP, TOK_T_TIMESTAMP_TZ, TOK_T_INTERVAL, TOK_T_BLOB, TOK_T_JSON, TOK_T_UUID, TOK_T_SERIAL, TOK_L_BOOL,
  TOK_L_BOOL, TOK_T_UINT, TOK_LIKE, TOK_BETWEEN, TOK_ASC, TOK_DESC, TOK_IF, TOK_EXISTS,
  TOK_CASCADE, TOK_RESTRICT, TOK_RETURNING, TOK_TO, TOK_RENAME, TOK_TABLESPACE, TOK_OWNER, TOK_KW_ADD,
  TOK_KW_COL, TOK_NO_CONSTRAINTS
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

  if (lexer->buf) {
    free(lexer->buf);
  }

  lexer->buf = strdup(buffer);
  if (!lexer->buf) {
    exit(EXIT_FAILURE);
  }

  lexer->buf_size = strlen(lexer->buf); 

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

Token* token_clone(Token* src) {
  if (!src) return NULL;

  Token* copy = malloc(sizeof(Token));
  if (!copy) return NULL;

  copy->line = src->line;
  copy->col = src->col;
  copy->type = src->type;

  if (src->value) {
    copy->value = strdup(src->value); 
  } else {
    copy->value = NULL;
  }

  return copy;

}

void token_free(Token* token) {
  /*
  De-initializes provided token
  */

  if (!token || !token->value) {
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
  if (!lexer) return NULL;

  if (offset < 0 && (size_t)(-offset) > lexer->i) {
    return NULL;
  }

  size_t peek_start = (size_t)(offset < 0 ? lexer->i + offset : lexer->i);
  size_t peek_length = (size_t)(offset < 0 ? -offset : offset);

  if (peek_start >= lexer->buf_size || peek_start + peek_length > lexer->buf_size) {
    return NULL;
  }

  return lexer->buf + peek_start;
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
  while (true) {
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
      case '-':
        lexer_advance(lexer, 1);
        if (lexer->c == '-') {
          lexer_advance(lexer, 1);
          while (lexer->c != '\0' && lexer->c != '\n') {
            lexer_advance(lexer, 1);
          }
        } else {
          return;
        }
        break;
      case '/':
        lexer_advance(lexer, 1);
        if (lexer->c == '*') {
          lexer_advance(lexer, 1);
          while (lexer->c != '\0') {
            if (lexer->c == '*') {
              lexer_advance(lexer, 1);
              if (lexer->c == '/') {
                lexer_advance(lexer, 1);
                break;
              }
            }
            if (lexer->c == '\n') {
              lexer->cl += 1;
              lexer->cc = 1;
            }
            lexer_advance(lexer, 1);
          }
        } else {
          return;
        }
        break;
      default:
        return;
    }
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

      Token* token = lexer_next_token(lexer);
      if (token) token_free(token); 

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
    case '=':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "=", TOK_EQ);
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
    case '{':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "{", TOK_LBR);
      break;
    case '}':
      lexer_advance(lexer, 1); return lexer_token_init(lexer, "}", TOK_RBR);
      break;
    case '-':
      return lexer_process_minus_op(lexer, next_char);
      break;
    case '\'':
      return lexer_process_single_quote(lexer);
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
  return: TOK_ERR, TOK_T_FLOAT (32-bit float), or TOK_T_DOUBLE (64-bit float)
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

uint8_t lexer_process_int_type(char* buf) {
  buf[strlen(buf)] = '\0';

  if (*buf == '-') {
    return TOK_L_INT;
  } else {
    return TOK_L_UINT;
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
  if (isdigit(next_char)) {
    return lexer_handle_numeric(lexer, true);
  }

  lexer_advance(lexer, 1);
  return lexer_token_init(lexer, "-", TOK_SUB);
}

Token* lexer_process_single_quote(Lexer* lexer) {
  lexer_advance(lexer, 1);

  char* value = calloc(1, sizeof(char));
  if (!value) exit(EXIT_FAILURE);

  while (lexer->c != '\0') {
    if (lexer->c == '\'') {
      if (lexer_peek(lexer, 2)[1] == '\'') {
        lexer_advance(lexer, 1); 
        lexer_advance(lexer, 1);

        size_t len = strlen(value);
        value = realloc(value, len + 2);
        if (!value) exit(EXIT_FAILURE);
        value[len] = '\'';
        value[len + 1] = '\0';
        continue;
      } else {
        lexer_advance(lexer, 1);
        Token* token = lexer_token_init(lexer, value, TOK_L_STRING);
        free(value);
        return token;
      }
    }

    size_t len = strlen(value);
    value = realloc(value, len + 2);
    if (!value) exit(EXIT_FAILURE);
    value[len] = lexer->c;
    value[len + 1] = '\0';
    lexer_advance(lexer, 1);
  }

  REPORT_ERROR(lexer, "E_STRING_TERMINATOR");
  lexer_handle_error(lexer);
  free(value);
  return lexer_next_token(lexer);
}

Token* lexer_process_double_quote(Lexer* lexer) {
  lexer_advance(lexer, 1);

  char* value = calloc(1, sizeof(char));
  if (!value) exit(EXIT_FAILURE);

  while (lexer->c != '\0') {
    if (lexer->c == '"') {
      if (lexer_peek(lexer, 1)[1] == '"') {
        lexer_advance(lexer, 1); 
        lexer_advance(lexer, 1);

        size_t len = strlen(value);
        value = realloc(value, len + 2);
        if (!value) exit(EXIT_FAILURE);
        value[len] = '"';
        value[len + 1] = '\0';
        continue;
      } else {
        lexer_advance(lexer, 1);
        Token* token = lexer_token_init(lexer, value, TOK_L_STRING);
        free(value);
        return token;
      }
    }

    size_t len = strlen(value);
    value = realloc(value, len + 2);
    if (!value) exit(EXIT_FAILURE);
    value[len] = lexer->c;
    value[len + 1] = '\0';
    lexer_advance(lexer, 1);
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
  {"SYE_E_CPR", "Expected closing parenthesis"},
  {"SYE_E_VARCHAR_VALUE", "Expected a value > 0 and <= 255 to specify number of charachters, not VARCHAR(%s)"},
  {"SYE_U_COLDEF", "Expected a proper column definition, not '%s'"},
  {"SYE_E_CDTYPE", "Expected a correct data type but got %s"},
  {"SYE_E_INVALID_VALUES", "Unexpected token '%s' (type %d), expected ',' or ')' while parsing VALUES list."}
};

char* lexer_get_reference(Lexer* lexer) {
  if (!lexer || !lexer->buf) return NULL;

  size_t i = lexer->i;

  size_t buf_len = lexer->buf_size;
  if (i >= buf_len) i = buf_len - 1;

  size_t line_start = i;
  while (line_start > 0 && lexer->buf[line_start - 1] != '\n') {
    line_start--;
  }

  size_t line_end = i;
  while (line_end < buf_len && lexer->buf[line_end] != '\n') {
    line_end++;
  }

  size_t line_length = line_end - line_start;
  char* line_content = malloc(line_length + 1);
  if (!line_content) {
    perror("malloc failed");
    exit(EXIT_FAILURE);
  }

  memcpy(line_content, lexer->buf + line_start, line_length);
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