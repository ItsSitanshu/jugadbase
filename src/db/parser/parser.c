#include "parser/parser.h"
#include "storage/database.h"

Parser* parser_init(Lexer* lexer) {
  Parser* parser = malloc(sizeof(Parser));
  
  parser->lexer = lexer;
  parser->cur = NULL;

  return parser;
}

JQLCommand* jql_command_init(JQLCommandType type) {
  JQLCommand* cmd = malloc(sizeof(JQLCommand));
  if (!cmd) return NULL;

  cmd->type = type;
  cmd->schema = malloc(sizeof(TableSchema));
  if (!cmd->schema) {
    free(cmd);
    return NULL;
  }

  cmd->schema->column_count = 0;
  cmd->schema->columns = NULL;
  memset(cmd->schema->table_name, 0, MAX_IDENTIFIER_LEN);

  cmd->row_count = 0;
  cmd->values = NULL;

  cmd->constraint_count = 0;

  cmd->function_count = 0;

  memset(cmd->value_counts, 0, MAX_OPERATIONS);
  memset(cmd->conditions, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->order_by, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->group_by, 0, MAX_IDENTIFIER_LEN);
  memset(cmd->having, 0, MAX_IDENTIFIER_LEN);
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

  free(cmd->schema->columns);
  
  free(cmd->values);

  free(cmd);
}

JQLCommand parser_parse(Database* db) {
  JQLCommand command = {0};
  memset(&command, 0, sizeof(JQLCommand));

  while (db->parser->cur->type == TOK_SC) {
    parser_consume(db->parser);
  }

  switch (db->parser->cur->type) {
    case TOK_CRT:
      command = parser_parse_create_table(db->parser, db);
      break;
    case TOK_ALT:
      command = parser_parse_alter_table(db->parser, db);
      break;
    case TOK_INS:
      command = parser_parse_insert(db->parser, db);
      break;
    case TOK_SEL: 
      command = parser_parse_select(db->parser, db);
      break;
    case TOK_UPD:
      command = parser_parse_update(db->parser, db);
      break;
    case TOK_DEL:
      command = parser_parse_delete(db->parser, db);
      break;
    case TOK_EOF:
      return command;
    default:
      REPORT_ERROR(db->parser->lexer, "SYE_UNSUPPORTED");
      return command;
  }

  if (command.is_invalid) {
    while (db->parser->cur->type != TOK_SC) {      
      parser_consume(db->parser);
    }
  } 
  
  if (db->parser->cur->type != TOK_SC && db->parser->cur->type != TOK_EOF && !command.is_invalid) {
    REPORT_ERROR(db->parser->lexer, "SYE_UE_SEMICOLON");
    command.is_invalid = true;
  }

  parser_consume(db->parser);
  return command;
}


bool parser_parse_column_definition(Parser *parser, JQLCommand *command) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_CNAME");
    return false;
  }

  ColumnDefinition column;
  memset(&column, 0, sizeof(ColumnDefinition));

  strcpy(column.name, parser->cur->value);
  parser_consume(parser);

  if (!is_valid_data_type(parser)) {
    REPORT_ERROR(parser->lexer, "SYE_E_CDTYPE", parser->cur->value);
    return false;
  }

  column.type = parser->cur->type;

  if (column.type == TOK_T_SERIAL) {
    column.has_sequence = true;
  }

  parser_consume(parser);

  if (column.type == TOK_T_VARCHAR && parser->cur->type == TOK_LP) {
    parser_consume(parser);
    if (parser->cur->type != TOK_L_UINT || parser->cur->value[0] == '0') {
      REPORT_ERROR(parser->lexer, "SYE_E_VARCHAR_VALUE", parser->cur->value);
      return false;
    }

    column.type_varchar = (uint8_t)atoi(parser->cur->value);
    if (column.type_varchar > 255) {
      LOG_ERROR("Can not define a VARCHAR(>255) characters, use TEXT instead");
      return false;
    }
    
    parser_consume(parser);

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_CPR");
      return false;
    }

    parser_consume(parser);
  }

  if (column.type == TOK_T_DECIMAL && parser->cur->type == TOK_LP) {
    parser_consume(parser);
    if (parser->cur->type != TOK_L_UINT || parser->cur->value[0] == '0') {
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

    if (parser->cur->type != TOK_L_UINT || parser->cur->value[0] == '0') {
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

  if (parser->cur->type == TOK_LB) {
    parser_consume(parser);

    if (parser->cur->type != TOK_RB) {
      REPORT_ERROR(parser->lexer, "SYE_E_COMP_BRAC");
      return false;
    }
    parser_consume(parser);

    column.is_array = true;
  }

  while (parser->cur->type != TOK_COM && parser->cur->type != TOK_RP) {
    switch (parser->cur->type) {
      case TOK_PK:
        column.is_primary_key = true;
        column.has_constraints = true;
        column.is_unique = true;
        column.is_not_null = true;
        parser_consume(parser);
        break;
      case TOK_FK: {
        column.has_constraints = true;
        column.is_foreign_key = true;
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

        parser_consume(parser); 
        while (parser->cur->type == TOK_ON) {
          parser_consume(parser);
          
          bool is_delete = false;
          if (parser->cur->type == TOK_DEL) {
            is_delete = true;
          } else if (parser->cur->type == TOK_UPD) {
            is_delete = false;
          } else {
            REPORT_ERROR(parser->lexer, "SYE_E_EXPECT_DELETE_OR_UPDATE");
            return false;
          }
          parser_consume(parser);
          
          FKAction action = FK_NO_ACTION;
          if (parser->cur->type == TOK_CASCADE) {
            action = FK_CASCADE;
            parser_consume(parser);
          } else if (parser->cur->type == TOK_RESTRICT) {
            action = FK_RESTRICT;
            parser_consume(parser);
          } else if (parser->cur->type == TOK_SET) {
            parser_consume(parser);
            if (parser->cur->type == TOK_NL) {
              action = FK_SET_NULL;
              parser_consume(parser);
            } else {
              REPORT_ERROR(parser->lexer, "SYE_E_EXPECT_NULL");
              return false;
            }
          } else {
            REPORT_ERROR(parser->lexer, "SYE_E_INVALID_ACTION");
            return false;
          }
          
          if (is_delete) {
            column.on_delete = action;
          } else {
            column.on_update = action;
          }
        }
        break;
      }
      case TOK_UNQ:
        column.has_constraints = true;
        column.is_unique = true;
        parser_consume(parser);
        break;
      case TOK_NOT:
        parser_consume(parser); 

        if (parser->cur->type != TOK_NL) {
          REPORT_ERROR(parser->lexer, "SYE_E_NLAFNOT");
          return false;
        }

        column.is_not_null = true;
        parser_consume(parser);
        break;
      case TOK_DEF:
        parser_consume(parser);

        column.default_value = calloc(1, sizeof(ColumnValue));
        parser_parse_value(parser, column.default_value);
        column.has_default = true;

        break;
      case TOK_CHK:
        parser_consume(parser);
        column.has_check = true;
        memset(column.check_expr, 0, MAX_IDENTIFIER_LEN);

        size_t check_expr_len = 0;
        while (parser->cur->type != TOK_COM && parser->cur->type != TOK_RP) {
          if (parser->cur->type != TOK_RP && parser->cur->type != TOK_LP) {
            if (check_expr_len + strlen(parser->cur->value) < MAX_IDENTIFIER_LEN - 1) {
              strcat(column.check_expr, parser->cur->value);
              check_expr_len += strlen(parser->cur->value);
            }
          }
          parser_consume(parser);
        }

        parser_consume(parser);
        break;
      case TOK_IDX:
        column.is_index = true;
        parser_consume(parser);
        break;
      default:
        REPORT_ERROR(parser->lexer, "SYE_U_COLDEF", parser->cur->value);
        return false;
    }

  }

  command->schema->columns[command->schema->column_count] = column;
  command->schema->column_count += 1;

  return true;
}

JQLCommand parser_parse_create_table(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_CREATE;
  command.is_invalid = true;

  command.schema = calloc(1, sizeof(TableSchema));

  parser_consume(parser);

  if (parser->cur->type != TOK_TBL) {
    REPORT_ERROR(parser->lexer, "SYE_E_TAFCR");
    return command;
  }

  parser_consume(parser);

  bool if_not_exists = false;
  if (parser->cur->type == TOK_IF) {
    parser_consume(parser);

    if (parser->cur->type != TOK_NOT) {
      REPORT_ERROR(parser->lexer, "SYE_NOT_AFIF_CT");
      return command;
    }
    parser_consume(parser);

    if (parser->cur->type != TOK_EXISTS) {
      REPORT_ERROR(parser->lexer, "SYE_NOT_AFIF_CT");
      return command;
    }
    parser_consume(parser);

    if_not_exists = true;
  }

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_TNAFTA");
    return command;
  }

  strcpy(command.schema->table_name, parser->cur->value);
  parser_consume(parser);

  int idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);

  if (!is_struct_zeroed(&db->tc[idx], sizeof(TableCatalogEntry))) {
    if (!if_not_exists) {
      LOG_ERROR("Table `%s` already exists", command.schema->table_name);
    }
    return command;
  }

  if (parser->cur->type != TOK_LP) {
    REPORT_ERROR(parser->lexer, "SYE_E_PRNAFDYNA");
    return command;
  }

  parser_consume(parser);

  command.schema->columns = calloc(MAX_COLUMNS, sizeof(ColumnDefinition));
  command.schema->column_count = 0;

  while (parser->cur->type != TOK_RP && parser->cur->type != TOK_EOF) {
    if (!parser_parse_column_definition(parser, &command)) {
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
  
  command.is_invalid = false;
  return command;
}

JQLCommand parser_parse_alter_table(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_ALTER;
  command.is_invalid = true;

  AlterTableCommand* alter_cmd = calloc(1, sizeof(AlterTableCommand));

  parser_consume(parser);

  if (parser->cur->type != TOK_TBL) {
    REPORT_ERROR(parser->lexer, "Expected TABLE keyword after ALTER");
    free(alter_cmd);
    return command;
  }
  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected table name after ALTER TABLE");
    free(alter_cmd);
    return command;
  }
  strcpy(alter_cmd->table_name, parser->cur->value);
  parser_consume(parser);

  switch (parser->cur->type) {
    case TOK_KW_ADD:
      parser_consume(parser);
      if (parser->cur->type == TOK_ID) {
        parser_consume(parser);
        if (!parse_alter_add_column(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else if (parser->cur->type == TOK_CNST) {
        parser_consume(parser);

        if (!parse_alter_add_constraint(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else {
        REPORT_ERROR(parser->lexer, "Expected COLUMN or CONSTRAINT after ADD");
        free(alter_cmd);
        return command;
      }
      break;
    case TOK_DRP:
        parser_consume(parser);
        if (parser->cur->type == TOK_ID) {
          parser_consume(parser);
          if (!parse_alter_drop_column(parser, alter_cmd)) {
            free(alter_cmd);
            return command;
          }
        } else if (parser->cur->type == TOK_CNST) {
          parser_consume(parser);
          if (!parse_alter_drop_constraint(parser, alter_cmd)) {
            free(alter_cmd);
            return command;
          }
        } else {
          REPORT_ERROR(parser->lexer, "Expected COLUMN or CONSTRAINT after DROP");
          free(alter_cmd);
          return command;
        }
        break;

    case TOK_RENAME:
      parser_consume(parser);
      if (parser->cur->type == TOK_ID) {
        parser_consume(parser);
        if (!parse_alter_rename_column(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else if (parser->cur->type == TOK_CNST) {
        parser_consume(parser);
        if (!parse_alter_rename_constraint(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else if (parser->cur->type == TOK_TO) {
        if (!parse_alter_rename_table(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else {
        REPORT_ERROR(parser->lexer, "Expected COLUMN, CONSTRAINT, or TO after RENAME");
        free(alter_cmd);
        return command;
      }
      break;

    case TOK_ALT:
      parser_consume(parser);

      if (parser->cur->type == TOK_KW_COL) {
        parser_consume(parser);
        if (!parse_alter_alter_column(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else {
        REPORT_ERROR(parser->lexer, "Expected COLUMN after ALTER");
        free(alter_cmd);
        return command;
      }
      break;

    case TOK_SET:
      parser_consume(parser);
      if (parser->cur->type == TOK_TABLESPACE) {
        parser_consume(parser);
        if (!parse_alter_set_tablespace(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else if (parser->cur->type == TOK_OWNER) {
        parser_consume(parser);
        if (!parse_alter_set_owner(parser, alter_cmd)) {
          free(alter_cmd);
          return command;
        }
      } else {
        REPORT_ERROR(parser->lexer, "Expected TABLESPACE or OWNER after SET");
        free(alter_cmd);
        return command;
      }
      break;

    default:
      REPORT_ERROR(parser->lexer, "Unknown ALTER TABLE operation");
      free(alter_cmd);
      return command;
  }

  command.alter = alter_cmd;
  command.is_invalid = false;
  return command;
}

bool parse_alter_add_column(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected column name after ADD COLUMN");
    return false;
  }
  cmd->operation = ALTER_ADD_COLUMN;
  strcpy(cmd->add_column.column_name, parser->cur->value);
  parser_consume(parser);

  if (!is_valid_data_type(parser)) {
    REPORT_ERROR(parser->lexer, "SYE_E_CDTYPE", parser->cur->value);
    return false;
  }

  cmd->add_column.data_type = parser->cur->type;
  parser_consume(parser);

  if (parser->cur->type == TOK_NOT) {
    parser_consume(parser);
    if (parser->cur->type == TOK_NL) {
      cmd->add_column.not_null = true;
      parser_consume(parser);
    }
  }

  if (parser->cur->type == TOK_DEF) {
    parser_consume(parser);
    sprintf(cmd->add_column.default_expr, "%s", parser->cur->value);
    
    ColumnValue val;
    if (!parser_parse_value(parser, &val)) { 
      REPORT_ERROR(parser->lexer, "Expected default value");
      return false;
    }

    strcpy(cmd->add_column.default_expr, parser->cur->value);
    cmd->add_column.has_default = true;
    parser_consume(parser);
  }

  return true;
}

bool parse_alter_drop_column(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected column name after DROP COLUMN");
    return false;
  }
  cmd->operation = ALTER_DROP_COLUMN;
  strcpy(cmd->column.column_name, parser->cur->value);
  parser_consume(parser);
  return true;
}

bool parse_alter_rename_column(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected column name to rename");
    return false;
  }
  strcpy(cmd->column.column_name, parser->cur->value);
  parser_consume(parser);

  if (parser->cur->type != TOK_TO) {
    REPORT_ERROR(parser->lexer, "Expected TO in RENAME COLUMN");
    return false;
  }
  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected new column name");
    return false;
  }
  strcpy(cmd->column.new_column_name, parser->cur->value);
  cmd->operation = ALTER_RENAME_COLUMN;
  parser_consume(parser);

  return true;
}

bool parse_alter_alter_column(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected column name in ALTER COLUMN");
    return false;
  }

  strcpy(cmd->column.column_name, parser->cur->value);
  parser_consume(parser);

  if (is_valid_data_type(parser)) {
    cmd->column.data_type = parser->cur->type;
    parser_consume(parser);
  }

  if (parser->cur->type == TOK_SET) {
    parser_consume(parser);
    if (parser->cur->type == TOK_DEF) {
      parser_consume(parser);
      cmd->operation = ALTER_SET_DEFAULT;

      strcpy(cmd->column.default_expr, parser->cur->value);
      if (!is_valid_data_type(parser)) {
        REPORT_ERROR(parser->lexer, "SYE_E_CDTYPE", parser->cur->value);
        return false;
      }

      parser_consume(parser);
    } else if (parser->cur->type == TOK_NOT) {
      parser_consume(parser);
      if (parser->cur->type == TOK_NL) {
        parser_consume(parser);
        cmd->operation = ALTER_SET_NOT_NULL;
        cmd->column.not_null = true;
      } else {
        REPORT_ERROR(parser->lexer, "Expected NULL after NOT");
        return false;
      }
    }
  } else if (parser->cur->type == TOK_DRP) {
    parser_consume(parser);
    if (parser->cur->type == TOK_DEF) {
      parser_consume(parser);
      cmd->operation = ALTER_DROP_DEFAULT;
    } else if (parser->cur->type == TOK_NOT) {
      parser_consume(parser);
      if (parser->cur->type == TOK_NL) {
        parser_consume(parser);
        cmd->operation = ALTER_DROP_NOT_NULL;
      } else {
        REPORT_ERROR(parser->lexer, "Expected NULL after NOT");
        return false;
      }
    }
  } else {
    REPORT_ERROR(parser->lexer, "Unknown ALTER COLUMN sub-operation");
    return false;
  }

  LOG_DEBUG("parser->2-> %s", parser->cur->value);

  return true;
}

bool parse_alter_add_constraint(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected constraint name after ADD CONSTRAINT");
    return false;
  }
  strcpy(cmd->constraint.constraint_name, parser->cur->value);
  parser_consume(parser);

  if (parser->cur->type == TOK_PK) {
    cmd->constraint.constraint_type = 1;  // PRIMARY KEY
    parser_consume(parser);

    if (parser->cur->type != TOK_LP) {
      REPORT_ERROR(parser->lexer, "Expected ( after PRIMARY KEY");
      return false;
    }
    parser_consume(parser);

    int i = 0;
    while (parser->cur->type == TOK_ID) {
      if (i >= MAX_COLUMNS) {
        REPORT_ERROR(parser->lexer, "Too many columns in constraint");
        return false;
      }
      strcpy(cmd->constraint.ref_columns[i++], parser->cur->value);
      parser_consume(parser);
      if (parser->cur->type == TOK_COM) parser_consume(parser);
      else break;
    }

    cmd->constraint.ref_columns_count = i;

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "Expected ) at end of constraint column list");
      return false;
    }
    parser_consume(parser);
  }

  else if (parser->cur->type == TOK_UNQ) {
    cmd->constraint.constraint_type = 2;  // UNIQUE
    parser_consume(parser);

    if (parser->cur->type != TOK_LP) {
      REPORT_ERROR(parser->lexer, "Expected ( after UNIQUE");
      return false;
    }
    parser_consume(parser);

    int i = 0;
    while (parser->cur->type == TOK_ID) {
      if (i >= MAX_COLUMNS) {
        REPORT_ERROR(parser->lexer, "Too many columns in UNIQUE constraint");
        return false;
      }

      strcpy(cmd->constraint.ref_columns[i], parser->cur->value);
      parser_consume(parser);
      i += 1;

      if (parser->cur->type == TOK_COM) parser_consume(parser);
      else break;
    }

    cmd->constraint.ref_columns_count = i;

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "Expected ) after UNIQUE column list");
      return false;
    }
    parser_consume(parser);
  }

  else if (parser->cur->type == TOK_FK) {
    cmd->constraint.constraint_type = 3;  // FOREIGN KEY
    parser_consume(parser);

    if (parser->cur->type != TOK_LP) {
      REPORT_ERROR(parser->lexer, "Expected ( after FOREIGN KEY");
      return false;
    }
    parser_consume(parser);

    int i = 0;
    while (parser->cur->type == TOK_ID) {
      if (i >= MAX_COLUMNS) {
        REPORT_ERROR(parser->lexer, "Too many columns in FK constraint");
        return false;
      }
      strcpy(cmd->constraint.ref_columns[i++], parser->cur->value);
      parser_consume(parser);
      if (parser->cur->type == TOK_COM) parser_consume(parser);
      else break;
    }

    cmd->constraint.ref_columns_count = i;

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "Expected ) after FK column list");
      return false;
    }
    parser_consume(parser);

    if (parser->cur->type != TOK_REF) {
      REPORT_ERROR(parser->lexer, "Expected REFERENCES after FOREIGN KEY");
      return false;
    }
    parser_consume(parser);

    if (parser->cur->type != TOK_ID) {
      REPORT_ERROR(parser->lexer, "Expected foreign table name");
      return false;
    }
    strcpy(cmd->constraint.ref_table, parser->cur->value);
    parser_consume(parser);

    if (parser->cur->type != TOK_LP) {
      REPORT_ERROR(parser->lexer, "Expected ( before FK referenced column list");
      return false;
    }
    parser_consume(parser);

    i = 0;
    while (parser->cur->type == TOK_ID) {
      if (i >= MAX_COLUMNS) {
        REPORT_ERROR(parser->lexer, "Too many referenced columns in FK");
        return false;
      }

      strcpy(cmd->constraint.ref_columns[i++], parser->cur->value);
      parser_consume(parser);
      if (parser->cur->type == TOK_COM) parser_consume(parser);
      else break;
    }
    cmd->constraint.ref_columns_count = i;

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "Expected ) after FK referenced column list");
      return false;
    }
    parser_consume(parser);
  }

  else if (parser->cur->type == TOK_CHK) {
    cmd->constraint.constraint_type = 4;  // CHECK
    parser_consume(parser);

    if (parser->cur->type != TOK_LP) {
      REPORT_ERROR(parser->lexer, "Expected ( after CHECK");
      return false;
    }
    parser_consume(parser);

    memset(cmd->constraint.constraint_expr, 0, MAX_IDENTIFIER_LEN);
    size_t expr_len = 0;

    while (parser->cur->type != TOK_RP && parser->cur->type != TOK_EOF) {
      if (expr_len + strlen(parser->cur->value) >= MAX_IDENTIFIER_LEN - 1) {
        REPORT_ERROR(parser->lexer, "Check expression too long");
        return false;
      }
      strcat(cmd->constraint.constraint_expr, parser->cur->value);
      expr_len += strlen(parser->cur->value);
      parser_consume(parser);
    }

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "Expected ) to close CHECK expression");
      return false;
    }
    parser_consume(parser);
  }

  else {
    REPORT_ERROR(parser->lexer, "Unsupported constraint type in ADD CONSTRAINT");
    return false;
  }

  cmd->operation = ALTER_ADD_CONSTRAINT;
  return true;
}


bool parse_alter_drop_constraint(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected constraint name after DROP CONSTRAINT");
    return false;
  }
  strcpy(cmd->constraint.constraint_name, parser->cur->value);
  cmd->operation = ALTER_DROP_CONSTRAINT;
  parser_consume(parser);
  return true;
}

bool parse_alter_rename_constraint(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected constraint name after RENAME CONSTRAINT");
    return false;
  }
  strcpy(cmd->constraint.constraint_name, parser->cur->value);
  parser_consume(parser);

  if (parser->cur->type != TOK_TO) {
    REPORT_ERROR(parser->lexer, "Expected TO in RENAME CONSTRAINT");
    return false;
  }
  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected new constraint name");
    return false;
  }

  strcpy(cmd->constraint.constraint_expr, parser->cur->value);
  cmd->operation = ALTER_RENAME_CONSTRAINT;
  parser_consume(parser);

  return true;
}

bool parse_alter_rename_table(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_TO) {
    REPORT_ERROR(parser->lexer, "Expected TO after RENAME");
    return false;
  }
  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected new table name");
    return false;
  }
  strcpy(cmd->rename_table.new_table_name, parser->cur->value);
  cmd->operation = ALTER_RENAME_TABLE;
  parser_consume(parser);

  return true;
}

bool parse_alter_set_owner(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_TO) {
    REPORT_ERROR(parser->lexer, "Expected TO after SET OWNER");
    return false;
  }
  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected new owner identifier");
    return false;
  }
  strcpy(cmd->set_owner.new_owner, parser->cur->value);
  cmd->operation = ALTER_SET_OWNER;
  parser_consume(parser);

  return true;
}

bool parse_alter_set_tablespace(Parser* parser, AlterTableCommand* cmd) {
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "Expected tablespace name");
    return false;
  }
  strcpy(cmd->set_tablespace.new_tablespace, parser->cur->value);
  cmd->operation = ALTER_SET_TABLESPACE;
  parser_consume(parser);
  return true;
}

JQLCommand parser_parse_insert(Parser *parser, Database* db) {
  JQLCommand command;  
  
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_INSERT;
  command.is_invalid = true;

  parser_consume(parser); 

  if (parser->cur->type != TOK_INTO) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_INTO");
    return command;
  }

  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }

  command.col_count = 0;
 
  uint32_t idx = hash_fnv1a(parser->cur->value, MAX_TABLES);
  parser_consume(parser); 
  command.schema = db->tc[idx].schema;

  if (parser->cur->type == TOK_LP) { 
    parser_consume(parser);
    
    command.columns = calloc(MAX_COLUMNS, sizeof(char *));

    while (parser->cur->type == TOK_ID) {
      command.columns[command.col_count] = strdup(parser->cur->value);
      command.col_count++;

      parser_consume(parser);

      if (parser->cur->type == TOK_COM) {
        parser_consume(parser);
      } else if (parser->cur->type == TOK_RP) {
        break;
      } else {
        REPORT_ERROR(parser->lexer, "SYE_E_INVALID_COLUMN_LIST");
        return command;
      }
    }

    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP");
      return command;
    }

    parser_consume(parser);
  }

  if (parser->cur->type != TOK_VAL) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_VALUES");
    return command;
  }

  parser_consume(parser);

  command.values = calloc(MAX_OPERATIONS, sizeof(ExprNode*));
  command.row_count = 0;

  command.specified_order = command.col_count == 0;
  command.col_count = command.col_count == 0 ? 
    db->tc[idx].schema->column_count 
    : command.col_count;

  while (parser->cur->type == TOK_LP) {
    ExprNode** row = calloc(command.col_count, sizeof(ExprNode*));

    parser_consume(parser); 

    uint8_t value_count = 0; 

    while (value_count < command.col_count) {
      int row_idx = command.specified_order ? value_count 
        : find_column_index(db->tc[idx].schema, command.columns[value_count]);

      if (row_idx < 0) {
        LOG_DEBUG("Internal: Row index for '%s' was evaluated incorrectly", command.columns[value_count]);
        return command;
      }

      row[row_idx] = parser_parse_expression(parser, db->tc[idx].schema);
      if (!row[row_idx]) return command;
      value_count++;

      if (parser->cur->type == TOK_COM) {
        parser_consume(parser);
      } else if (parser->cur->type != TOK_RP) {
        REPORT_ERROR(parser->lexer, "SYE_E_INVALID_VALUES", 
          parser->cur->value, parser->cur->type);
        return command;
      }

      if (parser->cur->type == TOK_EOF) {
        REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP_VALUES");
        return command;  
      }

    }

    if (parser->cur->type != TOK_RP) {
      LOG_ERROR("Mismatch in number of expected attributes %d and actual attributes %d",
        command.col_count, value_count - 1);
      return command;
    }

    parser_consume(parser);   

    LOG_DEBUG("!! %s", parser->cur->value);

    command.value_counts[command.row_count] = value_count;
    command.values[command.row_count] = row;
    command.row_count++;

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);  
    } else {
      break;
    }
    
    if (command.row_count >= MAX_OPERATIONS) {
      LOG_ERROR("Too many rows in INSERT");
      return command;
    }
  }

  if (parser->cur->type != TOK_RETURNING) {
    command.is_invalid = false;
    return command;
  }

  if (command.col_count <= 0 || command.col_count > MAX_COLUMNS) {
    LOG_ERROR("Invalid number of columns expected %d <= 0 and > %d", command.col_count, MAX_COLUMNS);
    return command;
  }

  parser_consume(parser); 
  command.returning_columns = calloc(command.col_count, sizeof(char *));
  if (!command.returning_columns) {
    REPORT_ERROR(parser->lexer, "Memory allocation failed");
    return command;
  }

  while (parser->cur->type == TOK_ID) {
    if (command.ret_col_count > command.col_count) {
      REPORT_ERROR(parser->lexer, "Too many RETURNING columns");
      return command;
    }

    command.returning_columns[command.ret_col_count] = strdup(parser->cur->value);
    command.ret_col_count += 1;
    parser_consume(parser);

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
    } else if (parser->cur->type == TOK_SC || parser->cur->type == TOK_EOF) {
      break; 
    } else {
      REPORT_ERROR(parser->lexer, "SYE_E_INVALID_RET_COLUMN_LIST");
      return command;
    }
  }


  command.is_invalid = false;
  return command;
}

JQLCommand parser_parse_select(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_SELECT;
  command.is_invalid = true;
  
  parser_consume(parser);
  
  ParserState state = parser_save_state(parser);
  
  while (parser->cur->type != TOK_FRM) {
    parser_consume(parser);
    if (parser->cur->type == TOK_EOF || parser->cur->type == TOK_SC) {
      return command;
    }
  }
  parser_consume(parser);

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }
  
  command.schema = calloc(1, sizeof(TableSchema));
  strcpy(command.schema->table_name, parser->cur->value);

  uint32_t idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);
  
  if (is_struct_zeroed(&db->tc[idx].schema, sizeof(TableSchema))) {
    LOG_ERROR("Couldn't fetch rows from table '%s', doesn't exist", command.schema->table_name);
    return command;
  }
    
  parser_restore_state(parser, state);
  
  uint8_t value_count = 0;
  
  if (parser->cur->type == TOK_MUL) {   
    value_count = db->tc[idx].schema->column_count;

    command.sel_columns = calloc(MAX_COLUMNS, sizeof(SelectColumn));  // 🛠️ fix added

    for (int j = 0; j < value_count; j++) {
      ExprNode* id_expr = malloc(sizeof(ExprNode));
      id_expr->type = EXPR_COLUMN;
      id_expr->column.index = j;
      
      command.sel_columns[j].expr = id_expr;
    }
    parser_consume(parser);
  } else {
    int column_count = 0;
    command.sel_columns = calloc(MAX_COLUMNS, sizeof(SelectColumn));
    
    while (true) {
      ExprNode* expr = parser_parse_expression(parser, db->tc[idx].schema);
      
      if (expr == NULL) {
        REPORT_ERROR(parser->lexer, "E_INVALID_COLUMN_EXPR");
        return command;
      }
      
      char* alias = NULL;
      if (parser->cur->type == TOK_AS) {
        parser_consume(parser);
        if (parser->cur->type == TOK_ID) {
          alias = strdup(parser->cur->value);
          parser_consume(parser);
        } else {
          REPORT_ERROR(parser->lexer, "E_IDEN_AF_ALIAS_KW");
          free_expr_node(expr);
          return command;
        }
      }
      
      command.sel_columns[column_count].expr = expr;
      command.sel_columns[column_count].alias = alias;
      column_count++;
      
      if (parser->cur->type != TOK_COM) break;
      parser_consume(parser);
    }
    value_count = column_count;
  }

  
  if (parser->cur->type != TOK_FRM) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_FROM");
    return command;
  }
  parser_consume(parser); 
  
  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }
  parser_consume(parser); 
  
  command.value_counts[0] = value_count;

  parse_where_clause(parser, db, &command, idx);
  parse_order_by_clause(parser, db, &command, idx);
  parse_limit_clause(parser, &command);
  parse_offset_clause(parser, &command);

  command.is_invalid = false;
  return command;
}

void parse_where_clause(Parser* parser, Database* db, JQLCommand* command, uint32_t idx) {
  if (parser->cur->type == TOK_WR) {
    parser_consume(parser);
    command->has_where = true;
    command->where = parser_parse_expression(parser, db->tc[idx].schema);
  }
}

void parse_limit_clause(Parser* parser, JQLCommand* command) {
  char* endptr;

  if (parser->cur->type == TOK_LIM) {
    parser_consume(parser);
    if (parser->cur->type == TOK_L_UINT) {
      command->has_limit = true;
      command->limit = (uint32_t)strtoul(parser->cur->value, &endptr, 10);
      parser_consume(parser);
    } else {
      REPORT_ERROR(parser->lexer, "E_INVALID_LIM_VALUE");
    }
  }
}

void parse_offset_clause(Parser* parser, JQLCommand* command) {
  char* endptr;

  if (parser->cur->type == TOK_OFF) {
    parser_consume(parser);
    if (parser->cur->type == TOK_L_UINT) {
      command->has_offset = true;
      command->offset = (uint32_t)strtoul(parser->cur->value, &endptr, 10);
      parser_consume(parser);
    } else {
      REPORT_ERROR(parser->lexer, "E_INVALID_OFFSET_VALUE");
    }
  }
}

void parse_order_by_clause(Parser* parser, Database* db, JQLCommand* command, uint32_t idx) {
  if (parser->cur->type == TOK_ODR) {
    parser_consume(parser);
    if (parser->cur->type != TOK_BY) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_BY_AFTER_ORDER");
      return;
    }

    parser_consume(parser);

    command->has_order_by = true;
    command->order_by_count = 0;
    command->order_by = calloc(db->tc[idx].schema->column_count, (sizeof(bool) + (2 * sizeof(uint8_t))));

    while (true) {
      ExprNode* ord_expr = parser_parse_expression(parser, db->tc[idx].schema);
      if (!ord_expr || ord_expr->type != EXPR_COLUMN) {
        REPORT_ERROR(parser->lexer, "E_INVALID_ORDER_EXPRESSION");
        return;
      }

      command->order_by[command->order_by_count].decend = false;
      if (parser->cur->type == TOK_ASC) {
        parser_consume(parser);
      } else if (parser->cur->type == TOK_DESC) {
        parser_consume(parser);
        command->order_by[command->order_by_count].decend = true;      
      }

      command->order_by[command->order_by_count].col = ord_expr->column.index;
      command->order_by[command->order_by_count].type = ord_expr->type;
      command->order_by_count++;

      if (parser->cur->type != TOK_COM) break;
      if (command->order_by_count > db->tc[idx].schema->column_count) {
        LOG_ERROR("Got more ORDER basises (%d) than existing columns (%d)", command->order_by_count, db->tc[idx].schema->column_count);
        return;
      }
      parser_consume(parser);
    }
  }
}

JQLCommand parser_parse_update(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_UPDATE;
  command.is_invalid = true;

  parser_consume(parser); 

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }

  uint32_t idx = hash_fnv1a(parser->cur->value, MAX_TABLES);
  command.schema = db->tc[idx].schema;
  
  if (is_struct_zeroed(command.schema, sizeof(TableSchema))) {
    return command;
  }

  parser_consume(parser); 

  if (parser->cur->type != TOK_SET) {
    REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_SET");
    return command;
  }

  parser_consume(parser); 

  uint8_t value_count = 0;
  command.values = calloc(1, sizeof(ExprNode*));
  command.row_count = 1;

  command.update_columns = calloc(command.schema->column_count, sizeof(UpdateColumn));
  command.values[0] = calloc(command.schema->column_count, sizeof(ExprNode));

  uint8_t null_bitmap_size = (command.schema->column_count + 7) / 8;
  command.bitmap = (uint8_t*)malloc(null_bitmap_size);
  if (!command.bitmap) {
    return command;
  }
  memset(command.bitmap, 0, null_bitmap_size);

  ExprNode* expr = NULL;
  while (parser->cur->type == TOK_ID) {
    expr = parser_parse_primary(parser, command.schema);
    if (!expr || !(expr->type == EXPR_COLUMN || expr->type == EXPR_ARRAY_ACCESS)) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_EQUAL_IN_SET");
      return command;
    }

    int col_index = expr->column.index;
  
    if (parser->cur->type != TOK_EQ) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_EQUAL_IN_SET");
      return command;
    }
    parser_consume(parser); 
    
    ExprNode* value = parser_parse_expression(parser, command.schema);
    if (!value) {
      REPORT_ERROR(parser->lexer, "SYE_E_INVALID_VALUE_IN_SET");
      return command;
    }
  
    command.update_columns[value_count].index = col_index;
    command.update_columns[value_count].array_idx = expr->type == EXPR_ARRAY_ACCESS ? 
                         expr->column.array_idx : NULL;
    command.values[0][value_count] = value;

    free_expr_node(expr);
    value_count++;
  
    if (parser->cur->type == TOK_COM) {
      parser_consume(parser); 
    } else {
      break;
    }
  }

  for (int i = 0; i < value_count; ++i) {
    ExprNode* val = command.values[0][i];
  
    if (command.schema->columns[command.update_columns[i].index].is_not_null /* && val->is_null */) {
      LOG_ERROR("Column is NOT NULL but attempted to set NULL");
      return command;
    }
  }  
  
  command.value_counts[0] = value_count;

  parse_where_clause(parser, db, &command, idx);

  command.is_invalid = false;
  return command;
}

JQLCommand parser_parse_delete(Parser* parser, Database* db) {
  JQLCommand command;
  memset(&command, 0, sizeof(JQLCommand));
  command.type = CMD_DELETE;
  command.is_invalid = true;

  parser_consume(parser);  

  if (parser->cur->type != TOK_FRM) {
    REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_FROM");
    return command;
  }

  parser_consume(parser);  

  if (parser->cur->type != TOK_ID) {
    REPORT_ERROR(parser->lexer, "SYE_E_MISSING_TABLE_NAME");
    return command;
  }

  command.schema = malloc(sizeof(TableSchema));
  if (!command.schema) {
    REPORT_ERROR(parser->lexer, "SYE_E_ALLOC_FAILED");
    return command;
  }

  strcpy(command.schema->table_name, parser->cur->value);
  uint32_t idx = hash_fnv1a(command.schema->table_name, MAX_TABLES);

  if (is_struct_zeroed(&db->tc[idx].schema, sizeof(TableSchema))) {
    LOG_ERROR("Couldn't update row in table '%s', doesn't exist", command.schema->table_name);
    return command;
  }

  parser_consume(parser);

  parse_where_clause(parser, db, &command, idx);

  command.is_invalid = false;
  return command;
}

void parser_consume(Parser* parser) {
  if (parser->cur->type == TOK_EOF) {
    return;
  }

  token_free(parser->cur);

  parser->cur = lexer_next_token(parser->lexer);
}

bool is_valid_data_type(Parser *parser) {
  if (parser->cur->type > TOK_T_UINT) return false;

  if (VALID_TYPES_MASK & (1 << parser->cur->type)) {
    return true;
  }
  
  return false;
}

bool is_valid_default(Parser* parser, int column_type, int literal_type) {
  switch (column_type) {
    case TOK_T_INT:
    case TOK_T_SERIAL:
      return (literal_type == TOK_L_INT || TOK_L_UINT);

    case TOK_T_FLOAT:
    case TOK_T_DOUBLE:
    case TOK_T_DECIMAL:
      return (literal_type == TOK_L_FLOAT || literal_type == TOK_L_DOUBLE);

    case TOK_T_BOOL:
      return (literal_type == TOK_L_BOOL);

    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
    case TOK_T_TEXT:
    case TOK_T_BLOB:
    case TOK_T_JSON:
    case TOK_T_UUID:
      return (literal_type == TOK_L_STRING);

    case TOK_T_DATE:
    case TOK_T_TIME:
    case TOK_T_DATETIME:
    case TOK_T_TIMESTAMP:
      return (literal_type == TOK_L_STRING); 
    default:
      return false;
  }
}

bool parser_parse_value(Parser* parser, ColumnValue* col_val) {
  memset(col_val, 0, sizeof(ColumnValue)); 

  col_val->is_array = false;

  switch (parser->cur->type) {
    case TOK_NL:
      col_val->is_null = true;
      break;
    case TOK_L_INT:
      col_val->type = TOK_T_INT;
      col_val->int_value = strtol(parser->cur->value, NULL, 10);
      break;
    case TOK_L_UINT:
      col_val->type = TOK_T_UINT;
      col_val->int_value = strtoul(parser->cur->value, NULL, 10);
      break;
    case TOK_L_FLOAT:
      col_val->type = TOK_T_FLOAT;
      col_val->float_value = strtof(parser->cur->value, NULL);
      break;
    case TOK_L_DOUBLE:
      col_val->type = TOK_T_DOUBLE;
      col_val->double_value = strtod(parser->cur->value, NULL);
      break;
    case TOK_L_BOOL:
      col_val->type = TOK_T_BOOL;
      col_val->bool_value = (strcmp(parser->cur->value, "true") == 0);
      break;
    case TOK_L_STRING:
      char temp_str[MAX_IDENTIFIER_LEN] = {0};
      strncpy(temp_str, parser->cur->value, MAX_IDENTIFIER_LEN - 1);
      temp_str[MAX_IDENTIFIER_LEN - 1] = '\0'; 

      size_t value_len = strlen(parser->cur->value);

      DateTime dt;
      DateTime_TZ dt_tz;
      __dt temp_dt;
      
      bool has_time_component = false;
      bool has_date_component = false;
      bool has_timezone = false;
      
      has_timezone = (strpbrk(temp_str, "+-") != NULL);

      if (temp_str[0] == '{' && temp_str[value_len - 1] == '}') {
        char* array_contents = temp_str;  
        array_contents[value_len] = '\0';

        size_t count = 0;
        col_val->array.array_value = calloc(MAX_ARRAY_SIZE, sizeof(ColumnValue));

        uint32_t array_type = -1; 

        Lexer* tmp_lexer = lexer_init();
        Parser* tmp_parser = parser_init(tmp_lexer);
        lexer_set_buffer(tmp_parser->lexer, strdup(array_contents));
        tmp_parser->cur = lexer_next_token(tmp_parser->lexer);
        parser_consume(tmp_parser);

        if (tmp_parser->cur->type == TOK_RBR) {
          parser_consume(tmp_parser);
          array_type = TOK_NL;
        } 

        while (tmp_parser->cur->type != TOK_EOF
          && tmp_parser->cur->type != TOK_RBK
          && count < MAX_ARRAY_SIZE) {

          ColumnValue element_val;
          if (!parser_parse_value(tmp_parser, &element_val)) {
            free(col_val->array.array_value);
            return false;
          }

          if (array_type == -1) {
            array_type = element_val.type;
          }

          col_val->array.array_value[count] = element_val;
          count++;

          if (tmp_parser->cur->type != TOK_EOF 
            || tmp_parser->cur->type == TOK_COM) {
            parser_consume(tmp_parser);
          } else {
            break;
          }
        }

        parser_free(tmp_parser);

        col_val->array.array_size = count;
        col_val->array.array_type = array_type;

        col_val->is_array = true;
        col_val->type = col_val->array.array_type;
      } else if (has_timezone && parse_to_datetime_TZ(temp_str, &dt_tz)) {
        col_val->type = TOK_T_DATETIME_TZ;
        memcpy(&col_val->datetime_tz_value, &dt_tz, sizeof(DateTime_TZ));
      } else if (parse_to_datetime(temp_str, &dt)) {
        has_time_component = (dt.hour != 0 || dt.minute != 0 || dt.second != 0);
        
        bool contains_time_separator = (strchr(temp_str, ':') != NULL);
        bool contains_date_separator = (strchr(temp_str, '-') != NULL);
        
        if (contains_date_separator && contains_time_separator) {
          col_val->type = TOK_T_DATETIME;
          memcpy(&col_val->datetime_value, &dt, sizeof(DateTime));
        } else if (contains_date_separator) {
          col_val->type = TOK_T_DATE;
          col_val->date_value = encode_date(dt.year, dt.month, dt.day);
        } else if (contains_time_separator) {
          col_val->type = TOK_T_TIME;
          col_val->time_value = encode_time(dt.hour, dt.minute, dt.second);
        } else {
          col_val->type = TOK_T_STRING;
          strncpy(col_val->str_value, parser->cur->value, MAX_IDENTIFIER_LEN - 1);
          col_val->str_value[MAX_IDENTIFIER_LEN - 1] = '\0';
        }

        if (parse_datetime(temp_str, &temp_dt)) {
          has_time_component = (temp_dt.hour != 0 || temp_dt.minute != 0 || temp_dt.second != 0);
          has_date_component = (temp_dt.year != JUGADBASE_EPOCH_YEAR || temp_dt.month != 1 || temp_dt.day != 1);
          
          if (has_date_component && has_time_component) {
            col_val->type = TOK_T_TIMESTAMP;
            col_val->timestamp_value = encode_timestamp(&temp_dt);
          } else if (has_date_component) {
            col_val->type = TOK_T_DATE;
            col_val->date_value = encode_date(temp_dt.year, temp_dt.month, temp_dt.day);
          } else if (has_time_component) {
            col_val->type = TOK_T_TIME;
            col_val->time_value = encode_time(temp_dt.hour, temp_dt.minute, temp_dt.second);
          }
        }

        char* time_start = strchr(temp_str, ' '); 
        if (time_start != NULL) {
          time_start++; 
        } else {
          break;
        }


        if (has_timezone && strchr(time_start, ':') != NULL) {
          int hours = 0, minutes = 0, seconds = 0;
          int tz_hours = 0, tz_minutes = 0;
          char tz_sign = '+';
          char* tz_part = strpbrk(time_start, "+-");
          
          if (tz_part) {
            if (sscanf(time_start, "%d:%d:%d", &hours, &minutes, &seconds) >= 2) {
              tz_sign = *tz_part;
              tz_part++;
              
              if (sscanf(tz_part, "%d:%d", &tz_hours, &tz_minutes) != 2) {
                sscanf(tz_part, "%2d%2d", &tz_hours, &tz_minutes);
              }
              
              int32_t offset = tz_hours * 60 + tz_minutes;
              if (tz_sign == '-') {
                offset = -offset;
              }
              
              col_val->type = col_val->type == TOK_T_TIMESTAMP ? TOK_T_TIMESTAMP_TZ : TOK_T_TIME_TZ;
              col_val->time_tz_value = encode_time_TZ(hours, minutes, seconds, offset);
            }
          }
        }
      } else {
        col_val->type = TOK_T_STRING;
        col_val->str_value = malloc(strlen(parser->cur->value) + 1);
        strcpy(col_val->str_value, parser->cur->value);
        col_val->str_value[strlen(col_val->str_value)] = '\0';
      }
      break;
    case TOK_L_CHAR:
      col_val->type = TOK_T_CHAR;
      col_val->str_value[0] = parser->cur->value[0];  
      col_val->str_value[1] = '\0';
      break;
    default:
      REPORT_ERROR(parser->lexer, "SYE_E_UNSUPPORTED_LITERAL_TYPE");
      return false;
  }

  parser_consume(parser);
  return true;
}

bool parser_parse_uuid_string(const char* uuid_str, uint8_t* output) {
  if (strlen(uuid_str) != 36) return false; 

  static const char hex_map[] = "0123456789abcdef";
  size_t j = 0;

  for (size_t i = 0; i < 36; i++) {
    if (uuid_str[i] == '-') continue;

    const char* p = strchr(hex_map, tolower(uuid_str[i]));
    if (!p) return false;

    output[j / 2] = (output[j / 2] << 4) | (p - hex_map);
    j++;
  }

  return j == 32;
}

ParserState parser_save_state(Parser* parser) {
  ParserState state;
  state.lexer_position = parser->lexer->i;
  state.lexer_line = parser->lexer->cl;
  state.lexer_column = parser->lexer->cc;

  state.buffer_size = parser->lexer->buf_size;
  state.buffer_copy = malloc(state.buffer_size);
  memcpy(state.buffer_copy, parser->lexer->buf, state.buffer_size);

  state.current_token = token_clone(parser->cur);

  return state;
}

void parser_restore_state(Parser* parser, ParserState state) {
  parser->lexer->i = state.lexer_position;
  parser->lexer->cl = state.lexer_line;
    parser->lexer->cc = state.lexer_column;

  free(parser->lexer->buf);

  parser->lexer->buf_size = state.buffer_size;
  parser->lexer->buf = malloc(state.buffer_size);
  memcpy(parser->lexer->buf, state.buffer_copy, state.buffer_size);

  parser->lexer->c = (parser->lexer->i < parser->lexer->buf_size) ?
                     parser->lexer->buf[parser->lexer->i] : '\0';

  if (parser->cur) token_free(parser->cur);
  parser->cur = token_clone(state.current_token);

  token_free(state.current_token);
  free(state.buffer_copy);
}

Token* parser_peek_ahead(Parser* parser, int offset) {
  ParserState state = parser_save_state(parser);
  
  Token* token = parser->cur;
  
  for (int i = 0; i < offset; i++) {
    token = lexer_next_token(parser->lexer);
    if (token->type == TOK_EOF) {
      break;
    }
  }
  
  Token* result = token;
  
  parser_restore_state(parser, state);
  
  return result;
}

ExprNode* parser_parse_expression(Parser* parser, TableSchema* schema) {
  ExprNode* node = parser_parse_logical_and(parser, schema);
  
  while (parser->cur->type == TOK_OR) {
    uint16_t op = parser->cur->type;
    parser_consume(parser);
    
    ExprNode* right = parser_parse_logical_and(parser, schema);
    
    ExprNode* new_node = calloc(1, sizeof(ExprNode));
    new_node->type = EXPR_LOGICAL_OR;
    new_node->binary.left = node;
    new_node->binary.right = right;
    new_node->binary.op = op;
    node = new_node;
  }
  
  return node;
}

ExprNode* parser_parse_logical_and(Parser* parser, TableSchema* schema) {
  ExprNode* node = parser_parse_logical_not(parser, schema);
  
  while (parser->cur->type == TOK_AND) {
    uint16_t op = parser->cur->type;
    parser_consume(parser);
    
    ExprNode* right = parser_parse_logical_not(parser, schema);
    
    ExprNode* new_node = calloc(1, sizeof(ExprNode));
    new_node->type = EXPR_LOGICAL_AND;
    new_node->binary.left = node;
    new_node->binary.right = right;
    new_node->binary.op = op;
    node = new_node;
  }
  
  return node;
}

ExprNode* parser_parse_logical_not(Parser* parser, TableSchema* schema) {
  if (parser->cur->type == TOK_NOT) {
    parser_consume(parser);
    
    ExprNode* node = calloc(1, sizeof(ExprNode));
    node->type = EXPR_LOGICAL_NOT;    
    node->unary = parser_parse_logical_not(parser, schema);
    
    return node;
  }

  return parser_parse_comparison(parser, schema);
}

ExprNode* parser_parse_comparison(Parser* parser, TableSchema* schema) {
  ExprNode* left = parser_parse_arithmetic(parser, schema);

  switch (parser->cur->type) {
    case TOK_LIKE:
      return parser_parse_like(parser, schema, left);
    case TOK_BETWEEN:
      return parser_parse_between(parser, schema, left);
    case TOK_IN:
      return parser_parse_in(parser, schema, left);
    case TOK_EQ:
    case TOK_NE:
    case TOK_LT:
    case TOK_GT:
    case TOK_LE:
    case TOK_GE: {
      uint16_t op = parser->cur->type;
      parser_consume(parser);
      ExprNode* right = parser_parse_arithmetic(parser, schema);

      ExprNode* node = calloc(1, sizeof(ExprNode));
      node->type = EXPR_COMPARISON;
      node->binary.left = left;
      node->binary.right = right;
      node->binary.op = op;

      return node;
    }
    default:
      return left;
  }
}


ExprNode* parser_parse_arithmetic(Parser* parser, TableSchema* schema) {
  ExprNode* node = parser_parse_term(parser, schema);

  while (parser->cur->type == TOK_ADD || parser->cur->type == TOK_SUB) {
    uint16_t op = parser->cur->type;
    parser_consume(parser);
    ExprNode* right = parser_parse_term(parser, schema);

    ExprNode* new_node = calloc(1, sizeof(ExprNode));
    new_node->type = EXPR_BINARY_OP;
    new_node->binary.op = op;
    new_node->binary.left = node;
    new_node->binary.right = right;
    node = new_node;
  }

  return node;
}

ExprNode* parser_parse_term(Parser* parser, TableSchema* schema) {
  ExprNode* node = parser_parse_unary(parser, schema);

  while (parser->cur->type == TOK_MUL || parser->cur->type == TOK_DIV || parser->cur->type == TOK_MOD) {
    uint16_t op = parser->cur->type;

    parser_consume(parser);
    ExprNode* right = parser_parse_unary(parser, schema);

    ExprNode* new_node = calloc(1, sizeof(ExprNode));
    new_node->type = EXPR_BINARY_OP;
    new_node->binary.op = op;
    new_node->binary.left = node;
    new_node->binary.right = right;
    node = new_node;
  }

  return node;
}

ExprNode* parser_parse_unary(Parser* parser, TableSchema* schema) {
  if (parser->cur->type == TOK_ADD || parser->cur->type == TOK_SUB) {
    uint16_t op = parser->cur->type;
    parser_consume(parser);
    
    ExprNode* operand = parser_parse_unary(parser, schema); 
    if (!operand) return NULL;

    ExprNode* node = calloc(1, sizeof(ExprNode));
    node->type = EXPR_UNARY_OP;
    node->arth_unary.op = op;
    node->arth_unary.expr = operand;
    return node;
  }

  return parser_parse_primary(parser, schema); 
}

ExprNode* parser_parse_primary(Parser* parser, TableSchema* schema) {
  if (parser->cur->type == TOK_LP) {
    parser_consume(parser);
    ExprNode* node = parser_parse_expression(parser, schema);
    if (parser->cur->type != TOK_RP) {
      REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP");
      return NULL;
    }
    parser_consume(parser);
    return node;
  }

  if (parser->cur->type == TOK_ID) {
    char* ident = strdup(parser->cur->value);
    parser_consume(parser);

    if (parser->cur->type == TOK_LP) {
      ExprNode* node = calloc(1, sizeof(ExprNode));
      node->fn.args = calloc(MAX_FN_ARGS, sizeof(ExprNode));
      node->type = EXPR_FUNCTION;
      node->fn.name = ident;
      node->fn.arg_count = 0;

      parser_consume(parser);

      if (parser->cur->type != TOK_RP) {
        while (true) {
          if (node->fn.arg_count >= MAX_FN_ARGS) {
            REPORT_ERROR(parser->lexer, "SYE_E_TOO_MANY_FN_ARGS");
            return NULL;
          }

          ExprNode* arg = parser_parse_expression(parser, schema);
          if (!arg) return NULL;

          node->fn.args[node->fn.arg_count++] = arg;

          if (parser->cur->type == TOK_COM) {
            parser_consume(parser);
          } else {
            break;
          }
        }
      }

      if (parser->cur->type != TOK_RP) {
        REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RP");
        return NULL;
      }

      parser_consume(parser);
      return node;
    }

    int col_index = find_column_index(schema, ident);
    if (col_index == -1) {
      REPORT_ERROR(parser->lexer, "SYE_E_UNKNOWN_COLUMN", ident);
      return NULL;
    }

    ExprNode* base = calloc(1, sizeof(ExprNode));
    base->type = EXPR_COLUMN;
    base->column.index = col_index;

    while (parser->cur->type == TOK_LB) {
      parser_consume(parser);  

      ExprNode* index_expr = parser_parse_expression(parser, schema);
      if (!index_expr) return NULL;

      if (parser->cur->type != TOK_RB) {
        REPORT_ERROR(parser->lexer, "SYE_E_EXPECTED_RBRACK");
        return NULL;
      }
      parser_consume(parser);

      base->type = EXPR_ARRAY_ACCESS;
      base->column.index = base->column.index;
      base->column.array_idx = index_expr;
    }

    return base;
  }

  ColumnValue val;
  if (!parser_parse_value(parser, &val)) return NULL;

  ExprNode* node = calloc(1, sizeof(ExprNode));
  node->type = EXPR_LITERAL;
  node->literal = val;

  return node;
}

ExprNode* parser_parse_like(Parser* parser, TableSchema* schema, ExprNode* left) {
  parser_consume(parser);

  if (parser->cur->type != TOK_L_STRING) {
    REPORT_ERROR(parser->lexer, "E_VALID_PATTERN_LIKE");
  }

  ExprNode* node = calloc(1, sizeof(ExprNode));
  node->type = EXPR_LIKE;
  node->like.left = left;
  node->like.pattern = strdup(parser->cur->value);

  parser_consume(parser);
  return node;
}

ExprNode* parser_parse_between(Parser* parser, TableSchema* schema, ExprNode* value) {
  parser_consume(parser);

  ExprNode* lower = parser_parse_arithmetic(parser, schema);

  if (parser->cur->type != TOK_AND) {
    REPORT_ERROR(parser->lexer, "E_EXPECTED_AND_BETWEEN");
  }
  parser_consume(parser);

  ExprNode* upper = parser_parse_arithmetic(parser, schema);

  ExprNode* node = calloc(1, sizeof(ExprNode));
  node->type = EXPR_BETWEEN;
  node->between.value = value;
  node->between.lower = lower;
  node->between.upper = upper;

  return node;
}

ExprNode* parser_parse_in(Parser* parser, TableSchema* schema, ExprNode* value) {
  parser_consume(parser);

  if (parser->cur->type != TOK_LP) {
    REPORT_ERROR(parser->lexer, "E_EXPECTED_PAREN_IN");
  }
  parser_consume(parser);

  ExprNode** values = NULL;
  size_t count = 0;

  while (1) {
    ExprNode* val = parser_parse_arithmetic(parser, schema);
    values = realloc(values, sizeof(ExprNode*) * (count + 1));
    values[count++] = val;

    if (parser->cur->type == TOK_COM) {
      parser_consume(parser);
    } else {
      break;
    }
  }

  if (parser->cur->type != TOK_RP) {
    REPORT_ERROR(parser->lexer, "E_EXPECTED_CLOSING_PAREN_IN");
  }
  parser_consume(parser);

  ExprNode* node = calloc(1, sizeof(ExprNode));
  node->type = EXPR_IN;
  node->in.value = value;
  node->in.list = values;
  node->in.count = count;

  return node;
}


int find_column_index(TableSchema* schema, const char* name) {
  for (uint8_t i = 0; i < schema->column_count; i++) {
    if (strcmp(schema->columns[i].name, name) == 0) {
      return i;
    }
  }
  return -1;
}

bool is_primary_key_column(TableSchema* schema, int column_index) {
  if (column_index < 0 || column_index >= schema->column_count) return false;
  return schema->columns[column_index].is_primary_key;
}

void print_column_value(ColumnValue* val) {
  if (val->is_null) {
    return;
  }

  if (val->is_array) {
    char buffer[1024];
    size_t offset = 0;
    
    for (size_t i = 0; i < val->array.array_size; i++) {
      char* elem_str = str_column_value(&val->array.array_value[i]);
      if (i > 0) {
        offset += snprintf(buffer + offset, sizeof(buffer) - offset, ", ");
      }
      offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%s", elem_str);
    }
      
    printf("arr<%s>[%s]", get_token_type(val->type), buffer);
    return;
  }
  
  printf("<%s>[", get_token_type(val->type));

  
  switch (val->type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL:
      printf("%ld", val->int_value);
      break;

    case TOK_T_FLOAT:
      printf("%f", val->float_value);
      break;

    case TOK_T_DOUBLE:
      printf("%lf", val->double_value);
      break;

    case TOK_T_BOOL:
      printf(val->bool_value ? "true" : "false");
      break;

    case TOK_T_STRING:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
      printf("\"%s\"", val->str_value);
      break;

    case TOK_T_DATE: {
      int year, month, day;
      decode_date(val->date_value, &year, &month, &day);
      printf("%04d-%02d-%02d", year, month, day);
      break;
    }

    case TOK_T_TIME: {
      int hour, minute, second;
      decode_time(val->time_value, &hour, &minute, &second);
      printf("%02d:%02d:%02d", hour, minute, second);
      break;
    }

    case TOK_T_DATETIME: {
      DateTime dt = val->datetime_value;
      printf("%04d-%02d-%02dT%02d:%02d:%02d", dt.year, dt.month, dt.day,
             dt.hour, dt.minute, dt.second);
      break;
    }

    case TOK_T_TIMESTAMP: {
      __dt ts;
      decode_timestamp(val->timestamp_value, &ts);
      printf("%04d-%02d-%02dT%02d:%02d:%02d", ts.year, ts.month, ts.day,
             ts.hour, ts.minute, ts.second);
      break;
    }

    case TOK_T_TIME_TZ: {
      int hour, minute, second, offset_minutes;
      decode_time_TZ(val->time_tz_value, &hour, &minute, &second, &offset_minutes);
      int abs_offset = abs(offset_minutes);
      int offset_hours = abs_offset / 60;
      int offset_min = abs_offset % 60;
      printf("%02d:%02d:%02d%c%02d:%02d", hour, minute, second,
             (offset_minutes >= 0 ? '+' : '-'), offset_hours, offset_min);
      break;
    }

    case TOK_T_DATETIME_TZ: {
      DateTime_TZ dt = val->datetime_tz_value;
      int abs_offset = abs(dt.time_zone_offset);
      int offset_hours = abs_offset / 60;
      int offset_min = abs_offset % 60;
      printf("%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d", dt.year, dt.month, dt.day,
             dt.hour, dt.minute, dt.second,
             (dt.time_zone_offset >= 0 ? '+' : '-'),
             offset_hours, offset_min);
      break;
    }

    case TOK_T_INTERVAL: {
      printf("%s", interval_to_string(&val->interval_value));
      break;
    }

    case TOK_T_TEXT:
    case TOK_T_BLOB:
    case TOK_T_JSON: {
      if (val->is_toast) {
        printf("<%s>(%u)", token_type_strings[val->type], val->toast_object);
      } else {
        const char* s = val->str_value;
        size_t len = strlen(s);
      
        if (len <= 20) {
          printf("\"%s\"", s);
        }
      
        char preview[12]; 
        memcpy(preview, s, 8);
        strcpy(preview + 8, ".");
        printf("\"%s +%zu\"", preview, len - 8);
      }      
      break;
    }
    default:
      printf("unprintable type: %d", val->type);
      break;
  }

  printf("]");
}


char* str_column_value(ColumnValue* val) {
  if (val->is_null) {
    return "NULL";
  }

  if (val->is_array) {
    char buffer[1024];
    size_t offset = 0;
    
    for (size_t i = 0; i < val->array.array_size; i++) {
      char* elem_str = str_column_value(&val->array.array_value[i]);
      if (i > 0) {
        offset += snprintf(buffer + offset, sizeof(buffer) - offset, ", ");
      }
      offset += snprintf(buffer + offset, sizeof(buffer) - offset, "%s", elem_str);
    }

    return strdup(buffer) ;
  }

  char buffer[256];

  switch (val->type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL:
      snprintf(buffer, sizeof(buffer), "%ld", val->int_value);
      break;

    case TOK_T_FLOAT:
      snprintf(buffer, sizeof(buffer), "%f", val->float_value);
      break;

    case TOK_T_DOUBLE:
      snprintf(buffer, sizeof(buffer), "%lf", val->double_value);
      break;

    case TOK_T_BOOL:
      snprintf(buffer, sizeof(buffer), "%s", val->bool_value ? "true" : "false");
      break;

    case TOK_T_STRING:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
      snprintf(buffer, sizeof(buffer), "\"%s\"", val->str_value);
      break;

    case TOK_T_DATE: {
      int y, m, d;
      decode_date(val->date_value, &y, &m, &d);
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", y, m, d);
      break;
    }

    case TOK_T_TIME: {
      int h, m, s;
      decode_time(val->time_value, &h, &m, &s);
      snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d", h, m, s);
      break;
    }

    case TOK_T_DATETIME: {
      DateTime dt = val->datetime_value;
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d",
               dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
      break;
    }

    case TOK_T_TIMESTAMP: {
      __dt ts;
      decode_timestamp(val->timestamp_value, &ts);
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d",
               ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second);
      break;
    }

    case TOK_T_TIME_TZ: {
      int h, m, s, offset_minutes;
      decode_time_TZ(val->time_tz_value, &h, &m, &s, &offset_minutes);
      int abs_offset = abs(offset_minutes);
      snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d%c%02d:%02d",
               h, m, s, (offset_minutes >= 0 ? '+' : '-'),
               abs_offset / 60, abs_offset % 60);
      break;
    }

    case TOK_T_DATETIME_TZ: {
      DateTime_TZ dt = val->datetime_tz_value;
      int abs_offset = abs(dt.time_zone_offset);
      snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d",
               dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,
               (dt.time_zone_offset >= 0 ? '+' : '-'),
               abs_offset / 60, abs_offset % 60);
      break;
    }

    case TOK_T_INTERVAL: {
      const char* s = interval_to_string(&val->interval_value);
      return strdup(s);
    }

    case TOK_T_TEXT:
    case TOK_T_BLOB:
    case TOK_T_JSON: {
      if (val->is_toast) {
        snprintf(buffer, sizeof(buffer), "<%s>(%u)", token_type_strings[val->type], val->toast_object);
      } else {
        const char* s = val->str_value;
        size_t len = strlen(s);
        if (len <= 8) {
          snprintf(buffer, sizeof(buffer), "\"%s\"", s);
        } else {
          char preview[12];
          memcpy(preview, s, 8);
          strcpy(preview + 8, "...");
          snprintf(buffer, sizeof(buffer), "\"%s (%zu chars)\"", preview, len - 8);
        }
      }
      break;
    }

    default:
      snprintf(buffer, sizeof(buffer), "unprintable type: %d", val->type);
      break;
  }

  return strdup(buffer);
}

void format_column_value(char* out, size_t out_size, ColumnValue* val) {
  if (val->is_null) {
    snprintf(out, out_size, "NULL");
    return;
  }

  switch (val->type) {
    case TOK_T_INT:
    case TOK_T_UINT:
    case TOK_T_SERIAL:
      snprintf(out, out_size, "%ld", val->int_value);
      break;

    case TOK_T_FLOAT:
      snprintf(out, out_size, "%f", val->float_value);
      break;

    case TOK_T_DOUBLE:
      snprintf(out, out_size, "%lf", val->double_value);
      break;

    case TOK_T_BOOL:
      snprintf(out, out_size, "%s", val->bool_value ? "true" : "false");
      break;

    case TOK_T_STRING:
    case TOK_T_VARCHAR:
    case TOK_T_CHAR:
      snprintf(out, out_size, "\"%s\"", val->str_value);
      break;

    case TOK_T_DATE: {
      int y, m, d;
      decode_date(val->date_value, &y, &m, &d);
      snprintf(out, out_size, "%04d-%02d-%02d", y, m, d);
      break;
    }

    case TOK_T_TIME: {
      int h, m, s;
      decode_time(val->time_value, &h, &m, &s);
      snprintf(out, out_size, "%02d:%02d:%02d", h, m, s);
      break;
    }

    case TOK_T_DATETIME: {
      DateTime dt = val->datetime_value;
      snprintf(out, out_size, "%04d-%02d-%02dT%02d:%02d:%02d",
               dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
      break;
    }

    case TOK_T_TIMESTAMP: {
      __dt ts;
      decode_timestamp(val->timestamp_value, &ts);
      snprintf(out, out_size, "%04d-%02d-%02dT%02d:%02d:%02d",
               ts.year, ts.month, ts.day, ts.hour, ts.minute, ts.second);
      break;
    }

    case TOK_T_TIME_TZ: {
      int h, m, s, offset_minutes;
      decode_time_TZ(val->time_tz_value, &h, &m, &s, &offset_minutes);
      int abs_offset = abs(offset_minutes);
      snprintf(out, out_size,
               "%02d:%02d:%02d%c%02d:%02d",
               h, m, s, (offset_minutes >= 0 ? '+' : '-'),
               abs_offset / 60, abs_offset % 60);
      break;
    }

    case TOK_T_DATETIME_TZ: {
      DateTime_TZ dt = val->datetime_tz_value;
      int abs_offset = abs(dt.time_zone_offset);
      snprintf(out, out_size,
               "%04d-%02d-%02dT%02d:%02d:%02d%c%02d:%02d",
               dt.year, dt.month, dt.day,
               dt.hour, dt.minute, dt.second,
               (dt.time_zone_offset >= 0 ? '+' : '-'),
               abs_offset / 60, abs_offset % 60);
      break;
    }

    case TOK_T_INTERVAL: {
      const char* s = interval_to_string(&val->interval_value);
      snprintf(out, out_size, "%s", s);
      break;
    }

    // case TOK_T_TEXT:
    // case TOK_T_BLOB:
    // // case TOK_T_JSON: {
    // //   if (val->is_toast) {
    // //     snprintf(out, out_size, "<%s>(%u)]", token_type_strings[val->type], val->toast_object);
    // //   } else {
    // //     const char* s = val->str_value;
    // //     size_t len = strlen(s);

    // //     if (len <= 8) {
    // //       snprintf(out, out_size, "\"%s\"", s);
    // //     } else {
    // //       char preview[12];
    // //       memcpy(preview, s, 8);
    // //       strcpy(preview + 8, "...");
    // //       snprintf(out, out_size, "\"%s (%zu chars)\"]", preview, len - 8);
    // //     }
    // //   }
    // //   break;
    // // }

    default:
      snprintf(out, out_size, "%s", "NULL");
      break;
  }
}

bool verify_select_col(SelectColumn* col, ColumnValue* evaluated_expr) {
  if (!col->expr || !evaluated_expr || is_struct_zeroed(evaluated_expr, sizeof(ColumnValue))) {
    return false;
  }

  if (evaluated_expr->type == EXPR_COMPARISON || 
    evaluated_expr->type == EXPR_LOGICAL_AND ||
    evaluated_expr->type == EXPR_LOGICAL_NOT ||
    evaluated_expr->type == EXPR_LOGICAL_OR
  ) {
    LOG_WARN("Latest query expects literals, functions, columns and binary operations between them, not logical comparisons. Query not processed.");
    return false;
  }

  return true;
}

void free_expr_node(ExprNode* node) {
  if (!node) return;
  if (node == NULL) return;

  switch (node->type) {
    case EXPR_LITERAL:
      if (node->literal.is_array && node->literal.array.array_value) {
        for (uint16_t i = 0; i < node->literal.array.array_size; i++) {
          free_expr_node((ExprNode*)&node->literal.array.array_value[i]);
        }
        free(node->literal.array.array_value);
      }
      break;

    case EXPR_COLUMN:
      break;
    case EXPR_ARRAY_ACCESS:
      free_expr_node(node->column.array_idx);
      break;
    case EXPR_UNARY_OP:
      free_expr_node(node->unary);
      free_expr_node(node->arth_unary.expr);
      break;

    case EXPR_BINARY_OP:
      free_expr_node(node->binary.left);
      free_expr_node(node->binary.right);
      break;

    case EXPR_FUNCTION:
      if (node->fn.name) free(node->fn.name);
      for (uint8_t i = 0; i < node->fn.arg_count; i++) {
        free_expr_node(node->fn.args[i]);
      }
      free(node->fn.args);
      break;

    case EXPR_LIKE:
      free_expr_node(node->like.left);
      if (node->like.pattern) free(node->like.pattern);
      break;

    case EXPR_BETWEEN:
      free_expr_node(node->between.value);
      free_expr_node(node->between.lower);
      free_expr_node(node->between.upper);
      break;

    case EXPR_IN:
      free_expr_node(node->in.value);
      for (size_t i = 0; i < node->in.count; i++) {
        free_expr_node(node->in.list[i]);
      }
      free(node->in.list);
      break;

    case EXPR_LOGICAL_NOT:
      free_expr_node(node->arth_unary.expr);
      break;

    case EXPR_LOGICAL_AND:
    case EXPR_LOGICAL_OR:
    case EXPR_COMPARISON:
      free_expr_node(node->binary.left);
      free_expr_node(node->binary.right);
      break;

    default:
      break;
  }
}

void free_column_value(ColumnValue* val) {
  if (!val) return;

  if (val->is_array && val->array.array_value) {
    for (uint16_t i = 0; i < val->array.array_size; i++) {
      free_column_value(&val->array.array_value[i]);
    }
    free(val->array.array_value);
  } else if (val->str_value) {
    free(val->str_value);
  } 
}

void free_column_definition(ColumnDefinition* col_def) {
  if (!col_def || col_def == NULL) return;

  // if (col_def->has_default) {
  //   free_column_value(col_def->default_value);
  //   free(col_def->default_value);
  // }
}

void free_table_schema(TableSchema* schema) {
  if (schema == NULL) return;

  for (uint8_t i = 0; i < MAX_COLUMNS; i++) {
    // ColumnDefinition* col_def = &(schema->columns[i]); 
    // if (!col_def) continue;

    if (i >= schema->column_count) break;

    // free_column_definition(col_def);
  }

  free(schema->columns);
  schema->columns = NULL;

  schema->column_count = 0;
  schema->prim_column_count = 0;
  schema->not_null_count = 0;
}


void free_jql_command(JQLCommand* cmd) {
  if (!cmd) return;

  // if (cmd->bitmap) free(cmd->bitmap);

  if (cmd->has_where) {
    free_expr_node(cmd->where);
  }

  // if (cmd->values) {
  //   for (uint8_t i = 0; i < cmd->row_count; i++) {
  //     if (cmd->values[i]) {
  //       // for (uint8_t j = 0; j < MAX_COLUMNS; j++) {
  //       //   if (cmd->values[i][j]) free_expr_node(cmd->values[i][j]);
  //       // }
  //       // free(cmd->values[i]);
  //     }
  //   }
  //   free(cmd->values);
  // }

  // if (cmd->returning_columns) {
  //   for (uint8_t i = 0; i < cmd->ret_col_count; i++) {
  //     if (cmd->returning_columns[i]) free(cmd->returning_columns[i]);
  //   }
  //   free(cmd->returning_columns);
  // }

  // if (cmd->columns) {
  //   for (uint8_t i = 0; i < cmd->col_count; i++) {
  //     // if (cmd->columns[i]) free(cmd->columns[i]);
  //   }
  //   free(cmd->columns);
  // }

  if (cmd->sel_columns) {
    for (uint8_t i = 0; i < MAX_COLUMNS; i++) {
      if (i >= cmd->col_count) break;
      if (cmd->sel_columns[i].alias) free(cmd->sel_columns[i].alias);
      if (cmd->sel_columns[i].expr) free_expr_node(cmd->sel_columns[i].expr);
      // free(&cmd->sel_columns[i]);
    }
    free(cmd->sel_columns);
  }

  if (cmd->update_columns) {
    for (uint8_t i = 0; i < cmd->col_count; i++) {
      free_expr_node(cmd->update_columns[i].array_idx);
    }
    free(cmd->update_columns);
  }

  if (cmd->where) {
    free_expr_node(cmd->where);
  }

  if (cmd->order_by) {
    free(cmd->order_by);
  }
}