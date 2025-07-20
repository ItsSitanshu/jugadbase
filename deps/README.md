>  All external dependencies are vendored in the `deps/` directory for version control and reproducibility.  

>  `FetchContent` (cmake) is preferred over system package managers to avoid mismatched dependency versions across environments.


## libsodium @ 9511c98

**Repository:** [jedisct1/libsodium](https://github.com/jedisct1/libsodium)  
**Purpose:** Cryptography (password hashing, signing, encryption)

`libsodium` is used for secure password storage and authentication mechanisms. It provides modern cryptographic primitives.

It is included via CMake's `FetchContent` module using a specific tag for reproducibility:

```cmake
FetchContent_Declare(
  libsodium
  GIT_REPOSITORY https://github.com/jedisct1/libsodium.git
  GIT_TAG 1.0.20-RELEASE
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/deps/libsodium
)
````

The library is cloned into `deps/libsodium`, built automatically, and linked statically to the project.

## uthash @ 41c357f

**Repository:** [troydhanson/uthash](https://github.com/troydhanson/uthash)  
**Purpose:** Lightweight hash tables in C

`uthash` is a single-header, zero-dependency C library providing hash table support. It is used for in-memory caches (system cache for constraints and other )

Located at: `deps/uthash.h `

It is not compiled, only included as a header (`#include "uthash.h"`).
