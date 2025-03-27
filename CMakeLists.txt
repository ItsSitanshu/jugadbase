cmake_minimum_required(VERSION 3.10)
project(JugadBase C)

# Set build type and C standard
set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose build type (Debug, Release, RelWithDebInfo, MinSizeRel)" FORCE)
set(CMAKE_C_STANDARD 11)

set(CMAKE_C_FLAGS_DEBUG "-g")
add_definitions(-DDB_ROOT_DIRECTORY="${CMAKE_BINARY_DIR}")

include_directories(src/db src/utils src/tests)

file(GLOB DB_SOURCES src/db/*.c)
file(GLOB UTILS_SOURCES src/utils/*.c)

add_library(jugadbase STATIC ${DB_SOURCES} ${UTILS_SOURCES})

add_executable(jugad-cli src/cli.c)
target_link_libraries(jugad-cli PRIVATE jugadbase)

add_executable(jugad-svr src/server.c)
target_link_libraries(jugad-svr PRIVATE jugadbase)

enable_testing()

find_package(PkgConfig REQUIRED)
pkg_check_modules(CHECK REQUIRED check)

include_directories(${CHECK_INCLUDE_DIRS})
file(GLOB TEST_SOURCES test/*.c)

add_executable(tests ${TEST_SOURCES})
target_link_libraries(tests PRIVATE jugadbase ${CHECK_LIBRARIES} rt m pthread)

add_test(NAME tests COMMAND tests)