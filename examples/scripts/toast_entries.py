import random
import json
import nltk
from nltk.corpus import gutenberg
import os

nltk.download("gutenberg", quiet=True)

def generate_text(words, min_bytes):
    current_text = []
    byte_size = 0
    while byte_size < min_bytes:
        chunk = " ".join(random.choices(words, k=30))
        current_text.append(chunk)
        byte_size = len(" ".join(current_text).encode("utf-8"))
    return " ".join(current_text).replace('"', '""')

def generate_random_json(words, min_bytes):
    obj = {}
    byte_size = 0
    while byte_size < min_bytes:
        key = random.choice(words).lower()
        if random.random() < 0.5:
            value = random.choice(words)
        else:
            value = random.randint(0, 1000)
        obj[key] = value
        byte_size = len(json.dumps(obj).encode("utf-8"))
    return obj

def prompt_for_fields():
    fields = []
    
    fields.append({
        "name": "id",
        "type": "INTEGER",
        "sql_type": "INTEGER",
        "gen_type": "index"
    })
    
    while True:
        field_name = input(f"Enter field name (or press Enter to finish): ").strip()
        if not field_name:
            break
            
        print(f"Field types: ")
        print("1. TEXT - Text content")
        print("2. JSON - JSON object")
        print("3. BLOB - Binary data")
        
        field_type = input("Choose field type (1-3): ").strip()
        
        field = {"name": field_name}
        
        if field_type in ("1", "2", "3"):
            size_option = input("Size unit (K for KB, M for MB): ").upper().strip()
            size_value = float(input(f"Size in {size_option}B: "))
            
            if size_option == "K":
                size_bytes = int(size_value * 1024)
            elif size_option == "M":
                size_bytes = int(size_value * 1024 * 1024)
            else:
                size_bytes = int(size_value)
            
            field["size"] = size_bytes
        
        if field_type == "1":  # TEXT
            field["type"] = "TEXT"
            field["sql_type"] = "TEXT"
            field["gen_type"] = "text"
            
        elif field_type == "2":  # JSON
            field["type"] = "JSON"
            field["sql_type"] = "TEXT"
            field["gen_type"] = "json"
            
        elif field_type == "3":  # BLOB
            field["type"] = "BLOB"
            field["sql_type"] = "BLOB"
            field["gen_type"] = "blob"
        
        fields.append(field)
    
    return fields

def prompt_user():
    print("=== TOAST Data Generator ===")
    
    table_name = input("Enter table name (default: big_texts): ").strip() or "big_texts"
    
    row_count = int(input("How many rows do you want? "))
    
    print("\nDefine your table fields:")
    fields = prompt_for_fields()
    
    file_path = input("\nEnter output file path (default=../test/texts.jql): ").strip() or "../test/texts.jql"
    
    book_options = list(gutenberg.fileids())
    print("\nAvailable books for text generation:")
    for i, book in enumerate(book_options):
        print(f"{i+1}. {book}")
    book_idx = int(input(f"Choose book (1-{len(book_options)}, default=1): ") or "1") - 1
    book_id = book_options[book_idx]
    
    return table_name, row_count, fields, file_path, book_id

def generate_create_table(table_name, fields):
    field_defs = []
    for field in fields:
        field_defs.append(f"{field['name']} {field['sql_type']}")
    
    create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    create_stmt += ",\n".join(f"    {field_def}" for field_def in field_defs)
    create_stmt += "\n);"
    
    return create_stmt

def generate_field_value(field, words, row_idx=None):
    if field["gen_type"] == "index":
        return str(row_idx)
        
    elif field["gen_type"] == "text":
        text_val = generate_text(words, field["size"])
        return f'"{text_val}"'
        
    elif field["gen_type"] == "json":
        obj = generate_random_json(words, field["size"])
        jstr = json.dumps(obj).replace('"', '""')
        return f'"{jstr}"'
        
    elif field["gen_type"] == "blob":
        raw = os.urandom(field["size"])
        hexstr = raw.hex()
        return f"X'{hexstr}'"
    
    return "NULL"

def generate_sql(table_name, row_count, fields, book_id):
    words = [w for w in gutenberg.words(book_id) if w.isalpha()]
    
    stmts = [generate_create_table(table_name, fields)]
    
    for i in range(row_count):
        field_names = [f["name"] for f in fields]
        field_values = [generate_field_value(f, words, i) for f in fields]
        
        stmt = f"INSERT INTO {table_name} ({', '.join(field_names)}) VALUES ({', '.join(field_values)});"
        stmts.append(stmt)
    
    return stmts

if __name__ == '__main__':
    table_name, row_count, fields, file_path, book_id = prompt_user()
    sqls = generate_sql(table_name, row_count, fields, book_id)
    
    os.makedirs(os.path.dirname(file_path) if os.path.dirname(file_path) else '.', exist_ok=True)
    
    with open(file_path, "w", encoding="utf-8") as f:
        for s in sqls:
            f.write(s + "\n")
    
    print(f"\n✅ Generated {file_path} with {row_count} rows in table {table_name}.")
    print(f"Fields: {', '.join(f['name'] for f in fields)}")