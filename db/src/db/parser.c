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
  cmd->schema.column_count = 0;
  cmd->schema.columns = NULL;
  cmd->value_count = 0;
  cmd->constraint_count = 0;
  cmd->function_count = 0;
  cmd->values = NULL;
  cmd->constraints = NULL;
  cmd->functions = NULL;

  memset(cmd->schema.table_name, 0, MAX_IDENTIFIER_LEN);
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

  free(cmd->schema.columns);
  
  free(cmd->values);
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
    REPORT_ERROR(parser->lexer, "SYE_E_CNAME");
    return false;
  }

  ColumnDefinition column;
  memset(&column, 0, sizeof(ColumnDefinition));

  strcpy(column.name, parser->cur->value);
  parser_consume(parser);

  if (!is_valid_data_type(parser)) {
    REPORT_ERROR(parser->lexer, "SYE_E_CDTYPE");
    return false;
  }

  column.type = parser->cur->type;
  parser_consume(parser);

  if (column.type == TOK_T_VARCHAR && parser->cur->type == TOK_LP) {
    parser_consume(parser);
    if (parser->cur->type != TOK_L_U8 || parser->cur->value[0] == '0') {
      REPORT_ERROR(parser->lexer, "SYE_E_VARCHAR_VALUE", parser->cur->value);
      return false;
    }

    column.type_varchar = (uint8_t)atoi(parser->cur->value);
    parser_consume(parser);

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_CPR");
      return false;
    }

    parser_consume(parser);
  }

  if (column.type == TOK_T_DECIMAL && parser->cur->type == TOK_LP) {
    parser_consume(parser);
    if (parser->cur->type != TOK_L_U8 || parser->cur->value[0] == '0') {
      REPORT_ERROR(parser->lexer, "SYE_E_PRECISION_VALUE", parser->cur->value);
      return false;
    }

    column.type_decimal_precision = (uint8_t)atoi(parser->cur->value);
    parser_consume(parser);

    if (parser->cur->type != TOK_COM) {
      REPORT_ERROR(parser->lexer, "SYE_E_COMMA");
      return false;
    }

    parser_consume(parser);

    if (parser->cur->type != TOK_L_U8 || parser->cur->value[0] == '0') {
      REPORT_ERROR(parser->lexer, "SYE_E_PRECISION_VALUE", parser->cur->value);
      return false;
    }

    column.type_decimal_scale = (uint8_t)atoi(parser->cur->value);
    parser_consume(parser);

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_CPR");
      return false;
    }

    parser_consume(parser);
  }

  while (parser->cur->type != TOK_COM && parser->cur->type != TOK_RP) {
    switch (parser->cur->type) {
      case TOK_PK:
        column.is_primary_key = true;
        break;
      case TOK_UNQ:
        column.is_unique = true;
        break;
      case TOK_NOT:
        parser_consume(parser); 

        if (parser->cur->type != TOK_NL) {
          REPORT_ERROR(parser->lexer, "SYE_E_NLAFNOT");
          return false;
        }

        column.is_not_null = true;
        break;
      case TOK_DEF:
        parser_consume(parser);
        if (parser->cur->type != TOK_VAL) {
          REPORT_ERROR(parser->lexer, "SYE_E_DEFVAL");
          return false;
        }
        strcpy(column.default_value, parser->cur->value);
        column.has_default = true;
        break;
      case TOK_CHK:
        break;
      case TOK_FK:
        parser_consume(parser);
        if (parser->cur->type != TOK_REF) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_REF");
          return false;
        }
        parser_consume(parser);
        if (parser->cur->type != TOK_ID) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_TBL");
          return false;
        }
        strcpy(column.foreign_table, parser->cur->value);
        parser_consume(parser);

        if (parser->cur->type != TOK_LP) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_LP");
          return false;
        }
        parser_consume(parser);

        if (parser->cur->type != TOK_ID) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_COL");
          return false;
        }
        strcpy(column.foreign_column, parser->cur->value);
        parser_consume(parser);

        if (parser->cur->type != TOK_RP) {
          REPORT_ERROR(parser->lexer, "SYE_E_FK_RP");
          return false;
        }
        break;
      case TOK_IDX:
        column.is_index = true;
        break;
      default:
        REPORT_ERROR(parser->lexer, "SYE_U_COLDEF", parser->cur->value);
        return false;
    }

    parser_consume(parser);
  }

  command->schema.columns[command->schema.column_count] = column;
  command->schema.column_count += 1;
  command->schema.columns = realloc(command->schema.columns, ((command->schema.column_count + 1) * sizeof(ColumnDefinition)));

  return true;
}

JQLCommand parser_parse_create_table(Parser *parser) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_CREATE;

  parser_consume(parser);

  if (parser->cur->type != TOK_TBL) {
    REPORT_ERROR(parser->lexer, "SYE_E_TAFCR");
    return command;
  }

  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_TNAFTA");
    return command;
  }

  strcpy(command.schema.table_name, parser->cur->value);
  parser_consume(parser);

  if (parser->cur->type != TOK_LP) {
    REPORT_ERROR(parser->lexer, "SYE_E_PRNAFDYNA");
    return command;
  }

  parser_consume(parser);

  command.schema.columns = calloc(1, sizeof(ColumnDefinition));
  command.schema.column_count = 0;

  while (parser->cur->type != TOK_RP && parser->cur->type != TOK_EOF) {
    if (!parse_column_definition(parser, &command)) {
      return command;
    }

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
    } else if (parser->cur->type == TOK_RP) {
      break;
    } else {
      REPORT_ERROR(parser->lexer, "SYE_E_CPRORCOM");
      return command;
    }
  }

  if (parser->cur->type != TOK_RP) {
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