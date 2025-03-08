#include "parser.h"

Parser* parser_init(Lexer* lexer) {
  Parser* parser = malloc(sizeof(Parser));
  
  parser->lexer = lexer;
  parser->cur = NULL;
  parser->exec = calloc(1, sizeof(JQLCommand));

  return parser;
}

JQLCommand* jql_command_init(JQLCommandType type) {
  JQLCommand* cmd = malloc(sizeof(JQLCommand));
  if (!cmd) return NULL;

  cmd->type = type;
  cmd->column_count = 0;
  cmd->value_count = 0;
  cmd->constraint_count = 0;
  cmd->function_count = 0;
  cmd->columns = NULL;
  cmd->values = NULL;
  cmd->column_types = NULL;  // Initialize column types as NULL
  cmd->constraints = NULL;
  cmd->functions = NULL;
  memset(cmd->table, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->conditions, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->order_by, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->group_by, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->having, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->limit, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->offset, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->join_table, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->join_condition, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->transaction, 0, MAX_IDENTIFIER_LEN);

  return cmd;
}

void parser_reset(Parser* parser) {
  parser->cur = lexer_next_token(parser->lexer);
}

void parser_free(Parser* parser) {
  if (!parser) {
    return;
  }

  lexer_free(parser->lexer);

  parser = NULL;
}

void jql_command_free(JQLCommand* cmd) {
  if (!cmd) return;

  free(cmd->columns);
  free(cmd->values);
  free(cmd->column_types);
  free(cmd->constraints);
  free(cmd->functions);

  free(cmd);
}


JQLCommand parser_parse(Parser* parser) {
  JQLCommand command;
  command.type = CMD_UNKNOWN;
  memset(&command, 0, sizeof(JQLCommand));

  switch (parser->cur->type) {
    case TOK_CRT:
      return parser_parse_create_table(parser);
    default:
      REPORT_ERROR(parser->lexer, "SYE_UNSUPPORTED");
      return command;
  }
}

bool parse_column_definition(Parser *parser, JQLCommand *command) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_CNA");
    return false;
  }

  command->columns = (char **)realloc(command->columns, (command->column_count + 1) * sizeof(char *));
  if (!command->columns) {
    REPORT_ERROR(parser->lexer, "SYE_E_MEM");
    return false;
  }

  command->column_types = (int *)realloc(command->column_types, (command->column_count + 1) * sizeof(int));
  if (!command->column_types) {
    REPORT_ERROR(parser->lexer, "SYE_E_MEM");
    return false;
  }

  command->columns[command->column_count] = strdup(parser->cur->value);
  if (!command->columns[command->column_count]) {
    REPORT_ERROR(parser->lexer, "SYE_E_MEM");
    return false;
  }

  parser_consume(parser);

  printf("%s %d %d\n", parser->cur->value, parser->cur->type, VALID_TYPES_MASK & (1 << parser->cur->type));

  if (!is_valid_data_type(parser)) {
    REPORT_ERROR(parser->lexer, "SYE_E_CTYPE");
    return false;
  }

  command->column_types[command->column_count] = parser->cur->type;

  parser_consume(parser);

  command->column_count++;
  
  return true;
}

JQLCommand parser_parse_create_table(Parser *parser) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_CREATE;


  parser_consume(parser);

  if (parser->cur->type != TOK_TBL) {
    // SYNTAX ERROR [EXPECTED] TABLE AFTER CREATE
    REPORT_ERROR(parser->lexer, "SYE_E_TAFCR");
    return command;
  }
  
  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    // SYNTAX ERROR [EXPECTED] TABLE NAME AFTER TABLE
    REPORT_ERROR(parser->lexer, "SYE_E_TNAFTA");
    return command;
  }

  strcpy(command.table, parser->cur->value);
  parser_consume(parser);

  if (parser->cur->type != TOK_LP) {
    // SYNTAX ERROR [EXPECTED] PAREN AFTER "TABLE"
    REPORT_ERROR(parser->lexer, "SYE_E_PRNAFDYNA");
    return command;
  }
  
  parser_consume(parser);

  while (parser->cur->type != TOK_RP && parser->cur->type != TOK_EOF) {
    if (!parse_column_definition(parser, &command)) {
      return command;
    }

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
    } else if (parser->cur->type == TOK_RP) {
      break;
    } else {
      // SYNTAX ERROR [EXPECTED] CLOSING PAREN OR COMMA
      REPORT_ERROR(parser->lexer, "SYE_E_CPRORCOM");
      return command;
    }
  }

  if (parser->cur->type != TOK_RP) {
    // SYNTAX ERROR [EXPECTED] CLOSING PAREN
    REPORT_ERROR(parser->lexer, "SYE_E_CPR");
    return command;
  }

  parser_consume(parser);

  return command;
}

void parser_consume(Parser* parser) {
  if (parser->cur->type == TOK_EOF) {
    exit(0);
  }

  token_free(parser->cur);

  parser->cur = lexer_next_token(parser->lexer);
}

bool is_valid_data_type(Parser *parser) {
  if (VALID_TYPES_MASK & (1 << parser->cur->type)) {
    return true;
  }
  
  return false;
}