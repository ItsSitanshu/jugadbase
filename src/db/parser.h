#ifndef JQL_COMMAND_H
#define JQL_COMMAND_H

#include "lexer.h"

#define MAX_COLUMNS 256
#define MAX_TEXT_SIZE 256
#define MAX_DECIMAL_LEN 256
#define MAX_BLOB_SIZE 512
#define MAX_JSON_SIZE 512
#define MAX_VARCHAR_SIZE 255

typedef enum {
  AST_COMMAND,
  AST_TABLE,
  AST_COLUMN,
  AST_VALUE,
  AST_CONDITION,
  AST_OPERATOR,
  AST_FUNCTION,
  AST_JOIN,
  AST_ORDER,
  AST_GROUP,
  AST_LIMIT,
  AST_TRANSACTION
} ASTNodeType;

typedef enum {
  CMD_SELECT,
  CMD_INSERT,
  CMD_UPDATE,
  CMD_DELETE,
  CMD_CREATE,
  CMD_DROP,
  CMD_ALTER,
  CMD_UNKNOWN 
} JQLCommandType;

typedef enum {
  CLAUSE_NONE = 0,
  CLAUSE_WHERE,
  CLAUSE_GROUP_BY,
  CLAUSE_ORDER_BY,
  CLAUSE_HAVING,
  CLAUSE_LIMIT,
  CLAUSE_OFFSET,
  CLAUSE_JOIN
} SQLClauseType;

typedef enum {
  CONSTRAINT_NONE = 0,
  CONSTRAINT_NOT_NULL,
  CONSTRAINT_PRIMARY_KEY,
  CONSTRAINT_FOREIGN_KEY
} ConstraintType;

typedef enum {
  FUNC_NONE = 0,
  FUNC_COUNT,
  FUNC_AVG,
  FUNC_SUM
} FunctionType;

typedef struct ASTNode {
  ASTNodeType type;
  char value[MAX_IDENTIFIER_LEN];
  struct ASTNode *left;
  struct ASTNode *right;
} ASTNode;

typedef struct ExecutionOrder {
  int step;
  char operation[MAX_IDENTIFIER_LEN];
  struct ExecutionOrder *next;
} ExecutionOrder;


typedef struct {
  uint8_t name_length;
  char name[MAX_IDENTIFIER_LEN]; 
  uint32_t offset; 
} TableCatalogEntry;

typedef struct {
  uint8_t column_index;
  uint8_t type; 
  
  union {  
    int int_value;
    float float_value;
    double double_value;
    bool bool_value;
    char str_value[MAX_IDENTIFIER_LEN]; 
    uint8_t blob_value[MAX_BLOB_SIZE]; 
    uint8_t json_value[MAX_JSON_SIZE]; 
    struct { int precision; int scale; char decimal_value[MAX_DECIMAL_LEN]; } decimal;
    struct { int year, month, day; } date;
    struct { int hours, minutes, seconds; } time;
    struct { int year, month, day, hours, minutes, seconds; } datetime;
    struct { int timestamp; } timestamp;
  };
} ColumnValue;

typedef struct {
  char name[MAX_IDENTIFIER_LEN];

  int type;  
  uint8_t type_varchar;  
  uint8_t type_decimal_precision;
  uint8_t type_decimal_scale;

  bool is_primary_key;
  bool is_unique;
  bool is_not_null;
  bool is_index;
  bool is_auto_increment;

  bool has_default;
  char default_value[MAX_IDENTIFIER_LEN];

  bool has_check;
  char check_expr[MAX_IDENTIFIER_LEN];

  bool is_foreign_key;
  char foreign_table[MAX_IDENTIFIER_LEN];
  char foreign_column[MAX_IDENTIFIER_LEN];
} ColumnDefinition;

typedef struct {
  char table_name[MAX_IDENTIFIER_LEN]; 
  uint8_t column_count;
  ColumnDefinition* columns;
} TableSchema;

typedef struct {
  JQLCommandType type;
  TableSchema schema;

  int value_count;
  ColumnValue* values;

  char conditions[MAX_IDENTIFIER_LEN]; // WHERE conditions
  char order_by[MAX_IDENTIFIER_LEN];  // ORDER BY clause
  char group_by[MAX_IDENTIFIER_LEN];  // GROUP BY clause
  char having[MAX_IDENTIFIER_LEN];    // HAVING clause
  char limit[MAX_IDENTIFIER_LEN];     // LIMIT clause
  char offset[MAX_IDENTIFIER_LEN];    // OFFSET clause

  char join_table[MAX_IDENTIFIER_LEN]; 
  char join_condition[MAX_IDENTIFIER_LEN];

  ConstraintType* constraints; 
  int constraint_count;

  FunctionType* functions; 
  int function_count;

  char transaction[MAX_IDENTIFIER_LEN];
} JQLCommand;

typedef struct {
  Lexer* lexer;
  Token* cur;
  JQLCommand** exec;
} Parser;

Parser* parser_init(Lexer* lexer);
JQLCommand* jql_command_init(JQLCommandType type);

void parser_reset(Parser* parser);
void parser_free(Parser* parser);
void jql_command_free(JQLCommand* cmd);

JQLCommand parser_parse(Parser* parser);

JQLCommand parser_parse_create_table(Parser *parser);
JQLCommand parser_parse_insert(Parser *parser);

void parser_consume(Parser* parser);

bool is_valid_data_type(Parser *parser);
bool is_valid_default(Parser* parser, int column_type, int literal_type);
bool parse_value(Parser* parser, ColumnValue* col_val);
ASTNode* parse_expression(Parser* parser);


#endif // JQL_COMMAND_H
