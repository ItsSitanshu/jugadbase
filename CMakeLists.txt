cmake_minimum_required(VERSION 3.14)
project(JugadBase C)

set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose build type (Debug, Release, RelWithDebInfo, MinSizeRel)" FORCE)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wno-format-truncation")

add_definitions(-DDB_ROOT_DIRECTORY="${CMAKE_BINARY_DIR}")

if (DEFINED CMAKE_EXE_LINKER_FLAGS)
  message(STATUS "Custom linker flags: ${CMAKE_EXE_LINKER_FLAGS}")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")
endif()

# Source files
set(DB_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/parser/lexer.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/parser/core.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/parser/statements.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/parser/expression.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/parser/clauses.c

  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/kernel/commands.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/kernel/constraints.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/kernel/expression.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/kernel/kernel.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/kernel/schema.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/kernel/sequence.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/kernel/utils.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/kernel/wal.c

  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/internal/btree.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/internal/datetime.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/internal/functions.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/internal/roles.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/internal/toast.c

  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/storage/cluster.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/storage/database.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/storage/fs.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/storage/storage.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/storage/wal.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/storage/syscache.c

)

set(UTILS_SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/utils/io.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/utils/jb_core.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/utils/setup.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/utils/jugadline.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db/utils/log.c
)

add_library(jugadbase STATIC ${UTILS_SOURCES} ${DB_SOURCES})

target_include_directories(jugadbase PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src/db
  ${CMAKE_CURRENT_SOURCE_DIR}/deps
)

find_package(PkgConfig REQUIRED)

include(cmake/SetupLibsodium.cmake)
include(cmake/SetupTesting.cmake)

target_link_libraries(jugadbase PUBLIC m sodium_static)

add_executable(jugad-cli ${CMAKE_CURRENT_SOURCE_DIR}/src/db/cli.c)
target_link_libraries(jugad-cli PRIVATE jugadbase)

add_executable(jugad-svr ${CMAKE_CURRENT_SOURCE_DIR}/src/db/server.c)
target_link_libraries(jugad-svr PRIVATE jugadbase)
