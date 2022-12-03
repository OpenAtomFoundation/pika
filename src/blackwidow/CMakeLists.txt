cmake_minimum_required (VERSION 3.10)

set (CMAKE_CXX_STANDARD 17)
project (blackwidow)

add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)

aux_source_directory(./src  DIR_SRCS)

add_library(blackwidow STATIC ${DIR_SRCS} )

target_include_directories(blackwidow 
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    )
target_include_directories(blackwidow 
    PUBLIC ${PROJECT_SOURCE_DIR}
    PUBLIC ${PROJECT_SOURCE_DIR}/../../third/rocksdb/include
    PUBLIC ${PROJECT_SOURCE_DIR}/../../third/rocksdb
    )
target_link_libraries(blackwidow
    PUBLIC RocksDB
    PUBLIC slash
    )