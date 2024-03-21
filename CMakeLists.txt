cmake_minimum_required(VERSION 3.5)
project(PTHASH)

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release")
endif ()
MESSAGE(STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE})

if(NOT TARGET PTHASH)
  add_library(PTHASH INTERFACE)
  target_include_directories(PTHASH INTERFACE .)

  target_compile_features(PTHASH INTERFACE cxx_std_17)
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    target_compile_options(PTHASH INTERFACE -stdlib=libc++)
  endif()

  MESSAGE(STATUS "Compiling for processor: " ${CMAKE_HOST_SYSTEM_PROCESSOR})

  if (UNIX AND (CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "x86_64"))
    MESSAGE(STATUS "Compiling with flags: -march=native -mbmi2 -msse4.2")
    target_compile_options(PTHASH INTERFACE -march=native)
    # Flags for PTHASH:
    target_compile_options(PTHASH INTERFACE -mbmi2 -msse4.2) # for hardware popcount and pdep
  endif()

  if (PTHASH_ENABLE_ALL_ENCODERS)
    MESSAGE(STATUS "compiling with all encoders")
    target_compile_options(PTHASH INTERFACE -DPTHASH_ENABLE_ALL_ENCODERS)
  endif()

  if (PTHASH_ENABLE_LARGE_BUCKET_ID_TYPE)
    MESSAGE(STATUS "bucket_id_type is uint64_t")
    target_compile_options(PTHASH INTERFACE -DPTHASH_ENABLE_LARGE_BUCKET_ID_TYPE)
  endif()

  if (UNIX)
    MESSAGE(STATUS "Compiling with flags: -std=c++17 -ggdb -pthread -Wall -Wextra -Wno-missing-braces -Wno-unknown-attributes -Wno-unused-function")

    target_compile_options(PTHASH INTERFACE -pthread)
    target_compile_options(PTHASH INTERFACE -ggdb)
    target_compile_options(PTHASH INTERFACE -Wall -Wextra -Wno-missing-braces -Wno-unknown-attributes -Wno-unused-function)

    if (PTHASH_USE_SANITIZERS)
      MESSAGE(STATUS "Using sanitizers. Compiling with flags: -fsanitize=address -fno-omit-frame-pointer")
      target_compile_options(PTHASH INTERFACE -fsanitize=address -fno-omit-frame-pointer)
    endif()
  endif()

  add_subdirectory(external/essentials)
  target_link_libraries(PTHASH INTERFACE ESSENTIALS)
endif()

# Only add benchmarks and tests when compiling PTHash itself, not when added as a dependency
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
  add_executable(build src/build.cpp)
  target_link_libraries(build PRIVATE PTHASH)
  add_executable(example src/example.cpp)
  target_link_libraries(example PRIVATE PTHASH)

  file(GLOB TEST_SOURCES test/test_*.cpp)
  foreach(TEST_SRC ${TEST_SOURCES})
    get_filename_component (TEST_SRC_NAME ${TEST_SRC} NAME_WE) # without extension
    add_executable(${TEST_SRC_NAME} ${TEST_SRC})
    add_test(${TEST_SRC_NAME} ${TEST_SRC_NAME})
    target_link_libraries(${TEST_SRC_NAME} PRIVATE PTHASH)
  endforeach(TEST_SRC)
endif()
