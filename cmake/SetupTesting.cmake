# cmake/SetupTesting.cmake

pkg_check_modules(CHECK REQUIRED check)

enable_testing()
include_directories(${CHECK_INCLUDE_DIRS})

file(GLOB TEST_UNIT_SOURCES ${CMAKE_SOURCE_DIR}/test/unit/*.c)

foreach(test_src IN LISTS TEST_UNIT_SOURCES)
  get_filename_component(test_name ${test_src} NAME_WE)
  file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${test_name})

  add_executable(${test_name}_runner ${test_src})
  target_link_libraries(${test_name}_runner PRIVATE jugadbase ${CHECK_LIBRARIES} m)
  add_test(NAME ${test_name}_runner COMMAND ${test_name}_runner)
endforeach()

add_executable(test_runner ${CMAKE_SOURCE_DIR}/test/test_runner.c)
target_link_libraries(test_runner PRIVATE jugadbase ${CHECK_LIBRARIES} m)
add_test(NAME test_runner COMMAND test_runner)
