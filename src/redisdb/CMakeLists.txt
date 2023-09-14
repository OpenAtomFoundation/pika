cmake_minimum_required (VERSION 3.18)

set (CMAKE_CXX_STANDARD 17)
project (redisdb)

file(GLOB DIR_SRCS *.c)
include_directories(include)
add_library(redisdb STATIC ${DIR_SRCS})
add_dependencies(redisdb net protobuf glog gflags ${LIBUNWIND_NAME})
target_link_libraries(redisdb
        PUBLIC ${GTEST_LIBRARY}
        PUBLIC pstd
        PUBLIC ${ROCKSDB_LIBRARY}
        PUBLIC storage
        PUBLIC ${GLOG_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${LIBUNWIND_LIBRARY}
        )
