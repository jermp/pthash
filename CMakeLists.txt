cmake_minimum_required(VERSION 3.5)
project(PTHASH)

set(CMAKE_CXX_STANDARD 17)
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release")
endif ()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif()

MESSAGE(STATUS "CMAKE_BUILD_TYPE: " ${CMAKE_BUILD_TYPE})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

MESSAGE(STATUS "Compiling for processor: " ${CMAKE_HOST_SYSTEM_PROCESSOR})

if (UNIX AND (CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "x86_64"))
  MESSAGE(STATUS "Compiling with flags: -march=native -mbmi2 -msse4.2")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
  # Flags for PTHash:
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mbmi2 -msse4.2") # for hardware popcount and pdep
endif()

if (PTHASH_ENABLE_ALL_ENCODERS)
  MESSAGE(STATUS "compiling with all encoders")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DPTHASH_ENABLE_ALL_ENCODERS")
endif()

if (UNIX)

  MESSAGE(STATUS "Compiling with flags: -std=c++17 -O3 -ggdb -pthread -Wall -Wextra -Wno-missing-braces -Wno-unknown-attributes -Wno-unused-function")

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-missing-braces -Wno-unknown-attributes -Wno-unused-function")

  if (PTHASH_USE_SANITIZERS)
    MESSAGE(STATUS "Using sanitizers. Compiling with flags: -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
  endif()

endif()

add_executable(build src/build.cpp)
add_executable(example src/example.cpp)

file(GLOB TEST_SOURCES test/test_*.cpp)
foreach(TEST_SRC ${TEST_SOURCES})
  get_filename_component (TEST_SRC_NAME ${TEST_SRC} NAME_WE) # without extension
  add_executable(${TEST_SRC_NAME} ${TEST_SRC})
  add_test(${TEST_SRC_NAME} ${TEST_SRC_NAME})
endforeach(TEST_SRC)