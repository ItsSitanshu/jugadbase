![Jugadbase](/.github/banner.png)

A database built for JugadInnovations. Why go for complex systems when a little clever "jugad" can get the job done faster and smarter? It's simple, efficient, and just works — because sometimes, the best solutions are the ones that skip the fluff.

## To-do List

- ~~Implement proper parsing~~
- ~~FIX BUG: offset from [create_table()](https://github.com/ItsSitanshu/jugadbase/blob/5ec3f58/src/db/executor.c#L39-L139) doesn't work as expected~~
- ~~FIX BUG: offset from create_table is [caluclated incorrectly](https://github.com/ItsSitanshu/jugadbase/blob/main/src/db/context.c#L362-L371)~~
- ~~Design and implement a directory-based storage system for database files~~
- ~~Create a structure to handle multiple files for tables, indexes, and metadata~~
- ~~Implement logic for linking tables with their corresponding .idx files~~
- ~~FIX BUG: table names aren't extracted properly when there are [>1 table](https://github.com/ItsSitanshu/jugadbase/blob/09e26a5bdc37c8e009e4c28deef58f035df3f5d8/src/db/context.c#L462-L728)~~   
- ~~Implement `CREATE TABLE` with `PRIM KEY` (Primary Key)~~
- ~~Implement a `BufferPool` system~~ 
- ~~Implement file command execution and polish CLI~~
- Support 
  - ~~`INSERT`~~
  - ~~`SELECT`~~
    - ~~`*`~~ 
    - ~~`WHERE`~~
  - ~~`UPDATE`~~
  - ~~`DELETE`~~
- ~~Data from [load_pages()](https://github.com/ItsSitanshu/jugadbase/blob/4c0066d1993d3ae9dd8b39eb77508cc41a76551c/src/db/context.c#L739-L782) isn't being loaded / read properly~~   
- Support advanced queries
  - ~~Mathematical operations~~
    - ~~basic~~
    - ~~~~functions~~
  - ~~Regex / `LIKE` and string functions~~
  - `IN`
  - Limits `LIM`
  - Ordering `ODR` (`ASRT` and `DSRT`)
- Important logic and functions for dynamic types
  - Datetime
  - Json
  - Text
  - Blob
- Compacting in actual storage of rows (storage.c)
- Implement foreign constraints
- Design casading  
- Implement `$variables` for cleaner SDK support
- Implement `DROP` to remove tables
- Add indexing for performance optimization  
- Implement `JOIN` operations  
- Support transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)  
- Optimize `JOIN` performance  
- Benchmark query execution speed  
- Implement locking mechanisms for files to handle concurrency across multiple files (tables/indexes)
- Add support for stored procedures  
- Implement views and triggers  
- Add replication and sharding for scalability  
- Add support for database-specific file management, including backing up and restoring individual files
- Implement composite primary keys
- Custom importable C-written functions with <dlfcn.h>
- Batch mass inserts