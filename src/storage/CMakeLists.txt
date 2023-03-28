cmake_minimum_required (VERSION 3.10)

set (CMAKE_CXX_STANDARD 17)
project (storage)

# Other CMake modules
add_subdirectory(tests)

add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)

aux_source_directory(./src  DIR_SRCS)

add_library(storage STATIC ${DIR_SRCS} )

add_dependencies(storage rocksdb gtest pstd)
# TODO fix rocksdb include path
target_include_directories(storage
    PUBLIC ${PROJECT_SOURCE_DIR}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
           ${INSTALL_INCLUDEDIR}
           ${ROCKSDB_SOURCE_DIR}
)

target_link_libraries(storage
    PUBLIC ${ROCKSDB_LIBRARY}
           ${SNAPPY_LIBRARY}
           ${ZSTD_LIBRARY}
           ${LZ4_LIBRARY}
           ${ZLIB_LIBRARY}
           ${JEMALLOC_LIBRARY}
    PUBLIC pstd)
