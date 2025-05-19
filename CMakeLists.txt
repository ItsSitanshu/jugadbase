cmake_minimum_required(VERSION 3.10)
project(JugadBase C)

# Set build type and C standard
set(CMAKE_BUILD_TYPE Debug CACHE STRING "Choose build type (Debug, Release, RelWithDebInfo, MinSizeRel)" FORCE)
set(CMAKE_C_STANDARD 11)

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wno-format-truncation")
add_definitions(-DDB_ROOT_DIRECTORY="${CMAKE_BINARY_DIR}")

if (DEFINED CMAKE_EXE_LINKER_FLAGS)
  message(STATUS "Custom linker flags: ${CMAKE_EXE_LINKER_FLAGS}")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")
endif()

include_directories(src/db src/utils src/tests)

file(GLOB DB_SOURCES src/db/*.c)
file(GLOB UTILS_SOURCES src/utils/*.c)

add_library(jugadbase STATIC ${DB_SOURCES} ${UTILS_SOURCES})

add_executable(jugad-cli src/cli.c)
target_link_libraries(jugad-cli PRIVATE jugadbase m) 

add_executable(jugad-svr src/server.c)
target_link_libraries(jugad-svr PRIVATE jugadbase m)

enable_testing()

find_package(PkgConfig REQUIRED)
pkg_check_modules(CHECK REQUIRED check)

include_directories(${CHECK_INCLUDE_DIRS})
file(GLOB TEST_SOURCES test/*.c)

set(TEST_UNIT_SOURCES
  test/unit/test_create_table.c
  test/unit/test_simple_select.c
  test/unit/test_combined_select.c
  test/unit/test_select_in_between.c
  test/unit/test_select_like.c
  test/unit/test_select_limit.c
  test/unit/test_select_offset.c
  test/unit/test_select_order.c
  test/unit/test_update.c
  test/unit/test_delete.c
  test/unit/test_array.c
)

foreach(test_src IN LISTS TEST_UNIT_SOURCES)
  get_filename_component(test_name ${test_src} NAME_WE)
  file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${test_name})
  add_executable(${test_name}_runner ${test_src})
  target_link_libraries(${test_name}_runner PRIVATE jugadbase ${CHECK_LIBRARIES} m)
  add_test(NAME ${test_name}_runner COMMAND ${test_name}_runner)
endforeach()

add_executable(test_runner
  test/test_runner.c
)

target_link_libraries(test_runner PRIVATE jugadbase ${CHECK_LIBRARIES} m)

add_test(NAME test_runner COMMAND test_runner)