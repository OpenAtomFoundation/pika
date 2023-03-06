cmake_minimum_required (VERSION 3.10)

set (CMAKE_CXX_STANDARD 17)
project (blackwidow)

# Other CMake modules
add_subdirectory(tests)

add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)

aux_source_directory(./src  DIR_SRCS)

add_library(blackwidow STATIC ${DIR_SRCS} )

add_dependencies(blackwidow rocksdb gtest slash)
# TODO fix rocksdb include path
target_include_directories(blackwidow
    PUBLIC ${PROJECT_SOURCE_DIR}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
           ${INSTALL_INCLUDEDIR}
           ${ROCKSDB_SOURCE_DIR}
)

target_link_libraries(blackwidow
    PUBLIC ${ROCKSDB_LIBRARIE}
           ${SNAPPY_LIBRARIE}
           ${ZSTD_LIBRARIE}
           ${LZ4_LIBRARIE}
           ${ZLIB_LIBRARIE}
           ${JEMALLOC_LIBRARIE}
    PUBLIC slash)