#ifndef JQL_COMMAND_H
#define JQL_COMMAND_H

#include "lexer.h"

#define MAX_COLUMNS 100 

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
  char** values;  

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

void parser_consume(Parser* parser);

bool is_valid_data_type(Parser *parser);
ASTNode* parse_expression(Parser* parser);


#endif // JQL_COMMAND_H
