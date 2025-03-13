cmake_minimum_required(VERSION 3.10)
project(JugadDB C)

set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose the type of build (Debug, Release, RelWithDebInfo, MinSizeRel)" FORCE)

set(CMAKE_C_STANDARD 11)

set(CMAKE_C_FLAGS_DEBUG "-g")

include_directories(src/db src/utils)

file(GLOB DB_SOURCES src/db/*.c)
file(GLOB UTILS_SOURCES src/utils/*.c)

add_library(jugaddb STATIC ${DB_SOURCES} ${UTILS_SOURCES})

add_executable(jugad-cli src/cli.c)
target_link_libraries(jugad-cli PRIVATE jugaddb)

add_executable(jugad-svr src/server.c)
target_link_libraries(jugad-svr PRIVATE jugaddb)