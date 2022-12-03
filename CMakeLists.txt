cmake_minimum_required (VERSION 3.10)

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-maybe-uninitialized" CACHE STRING "Set C++ Compiler Flags" FORCE)

project (pika)

# aux_source_directory(./src  DIR_SRCS)

# add_library(slash STATIC ${DIR_SRCS} )
add_subdirectory(third/rocksdb)
add_subdirectory(src/slash)
add_subdirectory(src/pink)
add_subdirectory(src/blackwidow)