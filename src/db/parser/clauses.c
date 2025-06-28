#include "parser/parser.h"
#include "storage/database.h"

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
      ColumnValue val;
      if (!parser_parse_value(parser, &val)) {
        REPORT_ERROR(parser->lexer, "Expected a proper default value after the `DEFAULT` keyword");
        return false;
      }
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
        cmd->constraint.on_delete = action;
      } else {
        cmd->constraint.on_update = action;
      }
    }

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