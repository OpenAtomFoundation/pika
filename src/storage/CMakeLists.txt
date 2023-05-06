cmake_minimum_required(VERSION 3.18)

set (CMAKE_CXX_STANDARD 17)
project (storage)

# Other CMake modules
add_subdirectory(tests)
add_subdirectory(examples)
add_subdirectory(benchmark)

add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)
add_compile_options("-fno-builtin-memcmp")

set(CMAKE_SYSTEM_PROCESSOR ${CMAKE_HOST_SYSTEM_PROCESSOR})
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
  add_compile_options(-msse)
endif()

aux_source_directory(./src  DIR_SRCS)

add_library(storage STATIC ${DIR_SRCS} )

add_dependencies(storage rocksdb gtest glog gflags ${LIBUNWIND_NAME} pstd)
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
           ${GLOG_LIBRARY}
           ${GFLAGS_LIBRARY}
           ${LIBUNWIND_LIBRARY}
    PUBLIC pstd
           uring)
