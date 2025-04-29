import random
import nltk
from nltk.corpus import gutenberg
import os

nltk.download('gutenberg')

def generate_text(words, min_bytes):
    current_text = []
    byte_size = 0
    while byte_size < min_bytes:
        chunk = ' '.join(random.choices(words, k=30))
        current_text.append(chunk)
        byte_size = len(' '.join(current_text).encode('utf-8'))
    return ' '.join(current_text).replace("'", "''")

def prompt_user():
    row_count = int(input("How many rows do you want? "))
    uniform_size = input("Same size for all rows? (y/n): ").lower().startswith('y')
    if uniform_size:
        min_kb = int(input("Size in KB for each TEXT field: "))
        sizes = [min_kb * 1024] * row_count
    else:
        min_kb_list = input("Comma-separated sizes per row (in KB): ")
        sizes = [int(x.strip()) * 1024 for x in min_kb_list.split(",")]

    same_content = input("Same TEXT content for all rows? (y/n): ").lower().startswith('y')

    custom_fields = []
    while input("Add another field? (y/n): ").lower().startswith('y'):
        field = input("Field name: ")
        value_type = input("Value type — fixed or index-based (fixed/index): ")
        if value_type == "fixed":
            value = input("Enter the fixed value (will be quoted in SQL): ")
            custom_fields.append((field, lambda i: f"'{value}'"))
        elif value_type == "index":
            custom_fields.append((field, lambda i: str(i)))

    file_path = input("Enter output file path (e.g. ./texts.sql): ").strip()
    if not file_path:
        file_path = "./texts.sql"

    return row_count, sizes, same_content, custom_fields, file_path

def generate_sql(row_count, sizes, same_content, custom_fields, book_id='austen-emma.txt'):
    words = gutenberg.words(book_id)
    words = [w for w in words if w.isalpha()]

    if same_content:
        shared_text = generate_text(words, sizes[0])

    output = []
    for i in range(row_count):
        text_content = shared_text if same_content else generate_text(words, sizes[i])
        fields = ["content"] + [name for name, _ in custom_fields]
        values = [f"'{text_content}'"] + [fn(i) for _, fn in custom_fields]
        sql = f"INSERT INTO big_texts ({', '.join(fields)}) VALUES ({', '.join(values)});"
        output.append(sql)
    return output

if __name__ == "__main__":
    row_count, sizes, same_content, custom_fields, file_path = prompt_user()
    sql_statements = generate_sql(row_count, sizes, same_content, custom_fields)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        for stmt in sql_statements:
            f.write(stmt + "\n")

    print(f"✅ Generated {file_path} with your settings.")
