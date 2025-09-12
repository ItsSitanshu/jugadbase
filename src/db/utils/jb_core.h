#ifndef JB_CORE_H
#define JB_CORE_H

#include "parser/parser.h"

TableSchema* jb_tables_schema();
TableSchema* jb_sequences_schema();
TableSchema* jb_attribute_schema();
TableSchema* jb_attrdef_schema();

bool load_jb_attrdef_hardcoded(Database* db);
bool load_jb_sequences_hardcoded(Database* db);
bool load_jb_attributes_hardcoded(Database* db);
bool load_jb_tables_hardcoded(Database* db);


#endif // JB_CORE_H