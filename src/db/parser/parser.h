#ifndef JQL_COMMAND_H
#define JQL_COMMAND_H

#include "parser/lexer.h"
#include "utils/io.h"

#include "internal/btree.h"
#include "internal/datetime.h"

#include "utils/security.h"

#define MAX_COLUMNS 256
#define MAX_OPERATIONS 128
#define MAX_TEXT_SIZE 256
#define MAX_DECIMAL_LEN 256
#define MAX_BLOB_SIZE 512
#define MAX_JSON_SIZE 512
#define MAX_VARCHAR_SIZE 255
#define MAX_FN_ARGS 40
#define MAX_LIKE_PATTERNS 32
#define MAX_ARRAY_SIZE 2048

struct Database;
typedef struct Database Database;
typedef struct ExprNode ExprNode;
typedef struct ColumnValue ColumnValue;

typedef enum {
  FK_NO_ACTION,
  FK_CASCADE,
  FK_RESTRICT,
  FK_SET_NULL
} FKAction;

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

typedef struct ExecutionOrder {
  int step;
  char operation[MAX_IDENTIFIER_LEN];
  struct ExecutionOrder *next;
} ExecutionOrder;

typedef struct ColumnValue {
  uint8_t column_index;
  uint8_t type; 

  bool is_null;
  bool is_array;
  bool is_toast;
  
  union {  
    int64_t int_value;
    float float_value;
    double double_value;
    bool bool_value;
    char* str_value; 
    uint32_t toast_object;
    struct { int precision; int scale; char decimal_value[MAX_DECIMAL_LEN]; } decimal;
    Date date_value;
    TimeStored time_value;
    Time_TZ time_tz_value;    
    DateTime datetime_value;
    DateTime_TZ datetime_tz_value;
    Timestamp timestamp_value;
    Timestamp_TZ timestamp_tz_value;
    Interval interval_value;
    struct cv__Array { 
      ColumnValue* array_value;
      uint16_t array_size;
      uint16_t array_type;
    } array;
    struct cv__Column {
      uint16_t index;
      int16_t array_idx;
    } column;
  };
} ColumnValue;

typedef enum {
  EXPR_COLUMN,
  EXPR_ARRAY_ACCESS,
  EXPR_LITERAL,
  EXPR_UNARY_OP,
  EXPR_BINARY_OP,
  EXPR_FUNCTION,
  EXPR_COMPARISON,
  EXPR_IN,
  EXPR_BETWEEN,
  EXPR_LIKE,
  EXPR_LOGICAL_NOT,
  EXPR_LOGICAL_AND,
  EXPR_LOGICAL_OR,
} ExprType;

typedef struct ExprNode {
  ExprType type;

  union {
    ColumnValue literal;
    struct column {
      uint16_t index;
      ExprNode* array_idx;
    } column;
    
    struct {
      struct ExprNode* left;
      struct ExprNode* right;
      uint16_t op;
    } binary; 

    ExprNode* unary;
    struct arth_unary {
      struct ExprNode* expr;
      uint16_t op;
    } arth_unary;

    struct like {
      ExprNode* left;
      char* pattern;
    } like;

    struct {
      struct ExprNode* value;
      struct ExprNode* lower;
      struct ExprNode* upper;
    } between;

    struct {
      struct ExprNode* value;
      struct ExprNode** list; 
      size_t count;
    } in;

    struct function_expr {
      char* name;
      ExprNode** args;
      uint8_t arg_count;
    } fn;  
  };
} ExprNode;

typedef struct {
  char name[MAX_IDENTIFIER_LEN];

  int type;  
  uint8_t type_varchar;  
  uint8_t type_decimal_precision;
  uint8_t type_decimal_scale;

  bool has_sequence;
  int64_t sequence_id;

  bool has_constraints;
  bool is_primary_key;
  bool is_unique;
  bool is_not_null;
  bool is_index;
  bool is_auto_increment;

  bool has_default;
  ColumnValue* default_value;

  bool has_check;
  char check_expr[MAX_IDENTIFIER_LEN];

  bool is_array;

  bool is_foreign_key;
  char foreign_table[MAX_IDENTIFIER_LEN];
  char foreign_column[MAX_IDENTIFIER_LEN];
  FKAction on_delete, on_update;
} ColumnDefinition;


typedef struct {
  char table_name[MAX_IDENTIFIER_LEN]; 
  
  uint8_t column_count;
  uint8_t prim_column_count;
  uint8_t not_null_count;
  
  ColumnDefinition* columns;
} TableSchema;

typedef struct {
  uint8_t name_length;
  char name[MAX_IDENTIFIER_LEN]; 
  uint32_t offset;
  TableSchema* schema;
  BTree* btree[MAX_COLUMNS];
  bool is_populated;
} TableCatalogEntry;

typedef struct SelectColumn {
  ExprNode* expr;
  char* alias;
} SelectColumn;

typedef struct UpdateColumn {
  uint16_t index;
  ExprNode* array_idx;
} UpdateColumn;

typedef struct {
  ColumnValue* value;
  uint8_t expected_type;
} __c;

typedef struct {
  JQLCommandType type;
  TableSchema* schema;
  char* schema_name;

  uint8_t* bitmap;
  uint8_t value_counts[MAX_OPERATIONS];
  ExprNode** (*values);
  uint8_t row_count;
  uint8_t col_count;
  uint8_t ret_col_count;

  char** returning_columns;
  char** columns;

  bool specified_order;

  SelectColumn* sel_columns;
  bool select_all;

  UpdateColumn* update_columns;

  ExprNode* where;
  bool has_where;

  bool has_limit;
  bool has_offset;
  uint32_t limit;
  uint32_t offset;

  bool has_order_by;
  uint8_t order_by_count;
  struct order_by {
    uint8_t col;
    uint8_t type;
    bool decend;
  }* order_by; 

  char conditions[MAX_IDENTIFIER_LEN]; // WHERE conditions
  char group_by[MAX_IDENTIFIER_LEN];  // GROUP BY clause
  char having[MAX_IDENTIFIER_LEN];    // HAVING clause

  char join_table[MAX_IDENTIFIER_LEN]; 
  char join_condition[MAX_IDENTIFIER_LEN];

  int constraint_count;

  int function_count;

  char transaction[MAX_IDENTIFIER_LEN];
  bool is_invalid;
} JQLCommand;

typedef struct {
  size_t lexer_position;
  int lexer_line;
  int lexer_column;

  char* buffer_copy;
  size_t buffer_size;

  Token* current_token;
} ParserState;

typedef struct {
  Lexer* lexer;
  Token* cur;
  ParserState state;
} Parser;

Parser* parser_init(Lexer* lexer);
JQLCommand* jql_command_init(JQLCommandType type);

void parser_reset(Parser* parser);
void parser_free(Parser* parser);
void jql_command_free(JQLCommand* cmd);

JQLCommand parser_parse(Database* db);

JQLCommand parser_parse_create_table(Parser* parser, Database* db);
JQLCommand parser_parse_insert(Parser *parser, Database* db);
JQLCommand parser_parse_select(Parser* parser, Database* db);
void parse_where_clause(Parser* parser, Database* db, JQLCommand* command, uint32_t idx);
void parse_limit_clause(Parser* parser, JQLCommand* command);
void parse_offset_clause(Parser* parser, JQLCommand* command);
void parse_order_by_clause(Parser* parser, Database* db, JQLCommand* command, uint32_t idx);

JQLCommand parser_parse_update(Parser* parser, Database* db);
JQLCommand parser_parse_delete(Parser* parser, Database* db);

void parser_consume(Parser* parser);

bool is_valid_data_type(Parser* parser);
bool is_valid_default(Parser* parser, int column_type, int literal_type);
bool parser_parse_value(Parser* parser, ColumnValue* col_val);
bool parser_parse_uuid_string(const char* uuid_str, uint8_t* output);

ParserState parser_save_state(Parser* parser);
void parser_restore_state(Parser* parser, ParserState state);
Token* parser_peek_ahead(Parser* parser, int offset);

ExprNode* parser_parse_expression(Parser* parser, TableSchema* schema);
ExprNode* parser_parse_logical_and(Parser* parser, TableSchema* schema);
ExprNode* parser_parse_logical_not(Parser* parser, TableSchema* schema);
ExprNode* parser_parse_comparison(Parser* parser, TableSchema* schema);
ExprNode* parser_parse_term(Parser* parser, TableSchema* schema);
ExprNode* parser_parse_unary(Parser* parser, TableSchema* schema);
ExprNode* parser_parse_arithmetic(Parser* parser, TableSchema* schema);
ExprNode* parser_parse_primary(Parser* parser, TableSchema* schema);

ExprNode* parser_parse_like(Parser* parser, TableSchema* schema, ExprNode* left);
ExprNode* parser_parse_between(Parser* parser, TableSchema* schema, ExprNode* left);
ExprNode* parser_parse_in(Parser* parser, TableSchema* schema, ExprNode* left);

int find_column_index(TableSchema* schema, const char* name);
bool is_primary_key_column(TableSchema* schema, int column_index);
void print_column_value(ColumnValue* val);
char* str_column_value(ColumnValue* val);
void format_column_value(char* out, size_t out_size, ColumnValue* val);
bool verify_select_col(SelectColumn* col, ColumnValue* evaluated_expr);

bool infer_and_cast_va(size_t count, ...);
bool infer_and_cast_value(ColumnValue* col_val, ColumnDefinition* def);
bool infer_and_cast_value_raw(ColumnValue* col_val, uint8_t target_type);

void free_expr_node(ExprNode* node);
void free_column_value(ColumnValue* val);
void free_column_definition(ColumnDefinition* col_def);
void free_table_schema(TableSchema* schema);
void free_jql_command(JQLCommand* cmd);

#endif // JQL_COMMAND_H
