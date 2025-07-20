# cmake/SetupLibsodium.cmake

find_package(libsodium QUIET)

set(LIBSODIUM_STATIC_LIB "${CMAKE_SOURCE_DIR}/deps/libsodium/src/libsodium/.libs/libsodium.a")

if(EXISTS ${LIBSODIUM_STATIC_LIB})
  message(STATUS "Found vendored libsodium static library at ${LIBSODIUM_STATIC_LIB}, skipping build.")

  add_library(sodium_static STATIC IMPORTED GLOBAL)
  set_target_properties(sodium_static PROPERTIES
    IMPORTED_LOCATION ${LIBSODIUM_STATIC_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_SOURCE_DIR}/deps/libsodium/src/libsodium/include
  )

  target_include_directories(jugadbase PUBLIC
    ${CMAKE_SOURCE_DIR}/deps/libsodium/src/libsodium/include
  )

else()
  message(STATUS "Vendored libsodium static library not found, fetching and building...")

  include(FetchContent)
  FetchContent_Declare(
    libsodium
    GIT_REPOSITORY https://github.com/jedisct1/libsodium.git
    GIT_TAG 1.0.20-RELEASE
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/deps/libsodium
  )

  FetchContent_GetProperties(libsodium)
  if(NOT libsodium_POPULATED)
    FetchContent_Populate(libsodium)

    execute_process(COMMAND ./configure --disable-shared --enable-static
      WORKING_DIRECTORY ${libsodium_SOURCE_DIR}
      RESULT_VARIABLE res)
    if(NOT res EQUAL 0)
      message(FATAL_ERROR "Failed to configure libsodium")
    endif()

    execute_process(COMMAND make
      WORKING_DIRECTORY ${libsodium_SOURCE_DIR}
      RESULT_VARIABLE res)
    if(NOT res EQUAL 0)
      message(FATAL_ERROR "Failed to build libsodium")
    endif()
  endif()

  add_library(sodium_static STATIC IMPORTED GLOBAL)
  set_target_properties(sodium_static PROPERTIES
    IMPORTED_LOCATION ${LIBSODIUM_STATIC_LIB}
    INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_SOURCE_DIR}/deps/libsodium/src/libsodium/include
  )

  target_include_directories(jugadbase PUBLIC
    ${CMAKE_SOURCE_DIR}/deps/libsodium/src/libsodium/include
  )
endif()
