import struct

def read_table_schema(file_path):
    with open(file_path, 'rb') as file:
        table_name_length = struct.unpack('B', file.read(1))[0]
        table_name = file.read(table_name_length).decode('utf-8')

        print(f"Table Name: {table_name}")

        column_count = struct.unpack('B', file.read(1))[0]
        print(f"Number of columns: {column_count}")

        columns = []
        for _ in range(column_count):
            col_name_length = struct.unpack('B', file.read(1))[0]
            col_name = file.read(col_name_length).decode('utf-8')

            col_type = struct.unpack('I', file.read(4))[0]  # 4 bytes for type

            columns.append({
                'name': col_name,
                'length': col_name_length,
                'type': col_type
            })
        
        for idx, col in enumerate(columns):
            print(f"Column {idx}:")
            print(f"  Name: {col['name']}")
            print(f"  Length: {col['length']}")
            print(f"  Type: {col['type']}")

file_path = 'default.jdb'

read_table_schema(file_path)
