# COCO (Cloud Object and Content Organizer) - To-do List


## To-do List
- Design bucket and object models
- Plan directory and file-based storage layout
- Define metadata schema (e.g., object size, type, created_at)
- CLI
  - `coco init` – Initialize a new storage space
  - `coco put` – Add object to a bucket
  - `coco get` – Retrieve object from a bucket
  - `coco rm` – Delete an object or bucket
  - `coco list` – List contents of a bucket
- Implement `BucketManager` module
- Support bucket creation and deletion
- Store and manage bucket metadata
- Implement `ObjectStore` module
- Handle object read/write/delete
- Add support for versioning (optional)
- Efficiently handle binary and blob data
- Design file index structure (JSON or binary)
- Optional: use `sled` or `rocksdb` for faster indexing
- Implement basic ACLs (Access Control Lists)
- Add checksum/hash verification on upload
- Support optional compression for large files
- Log all file and bucket operations
- Support config file `.cocorc` (JSON/TOML/YAML)
- Add `DROP` functionality for buckets and storage