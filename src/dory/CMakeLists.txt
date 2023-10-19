cmake_minimum_required (VERSION 3.18)

set (CMAKE_CXX_STANDARD 17)
project (dory)

aux_source_directory(./src  DIR_SRCS)
include_directories(include)
add_library(dory STATIC ${DIR_SRCS})
add_dependencies(dory net protobuf glog gflags ${LIBUNWIND_NAME})

target_link_libraries(dory
        PUBLIC ${GTEST_LIBRARY}
        PUBLIC pstd
        PUBLIC ${ROCKSDB_LIBRARY}
        PUBLIC storage
        PUBLIC ${GLOG_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${LIBUNWIND_LIBRARY}
        PUBLIC ${REDISCACHE_LIBRARY}
        )