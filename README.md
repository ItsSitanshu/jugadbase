![Jugadbase](/.github/banner.png)


## To-do List

- ~~Implement proper parsing~~
- ~~FIX BUG: offset from [create_table()](https://github.com/ItsSitanshu/jugadbase/blob/5ec3f58/src/db/executor.c#L39-L139) doesn't work as expected~~
- ~~FIX BUG: offset from create_table is [caluclated incorrectly](https://github.com/ItsSitanshu/jugadbase/blob/main/src/db/context.c#L362-L371)~~
- ~~Design and implement a directory-based storage system for database files~~
- Create a structure to handle multiple files for tables, indexes, and metadata
- Implement logic for linking tables with their corresponding .idx files
- Implement `CREATE TABLE` with `PRIM KEY` (Primary Key) 
- Support 
  - `INSERT`
  - `SELECT`
  - `UPDATE`
  - `DELETE`
- Implement foreign constraints
- Design casading  
- Add indexing for performance optimization  
- Implement `JOIN` operations  
- Support transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)  
- Use `KP` for `PRIMARY KEY`  
- Use `FRN` for `FOREIGN KEY`  
- Use `LIM` for `LIMIT`  
- Use `ODR` for `ORDER BY`  
- Use `ASRT` and `DSRT` for sorting (`ASC` / `DESC`)  
- Write unit tests for table creation  
- Test `INSERT` and `SELECT` queries  
- Optimize `JOIN` performance  
- Benchmark query execution speed  
- Add support for stored procedures  
- Implement views and triggers  
- Add replication and sharding for scalability  
- Add support for database-specific file management, including backing up and restoring individual files
- Implement locking mechanisms for files to handle concurrency across multiple files (tables/indexes)
- Improve disk I/O by updating b-trees partially
- Implement composite primary keys