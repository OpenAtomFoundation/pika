cmake_minimum_required (VERSION 3.18)

set (CMAKE_CXX_STANDARD 17)
project (cache)

aux_source_directory(./src  DIR_SRCS)
include_directories(include)
add_library(cache STATIC ${DIR_SRCS})
add_dependencies(cache net protobuf glog gflags rediscache ${LIBUNWIND_NAME})

target_link_libraries(cache
        PUBLIC ${GTEST_LIBRARY}
        PUBLIC pstd
        PUBLIC ${ROCKSDB_LIBRARY}
        PUBLIC storage
        PUBLIC ${GLOG_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${LIBUNWIND_LIBRARY}
        PUBLIC ${REDISCACHE_LIBRARY}
        )