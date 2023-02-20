cmake_minimum_required (VERSION 3.10)

set(CMAKE_CXX_STANDARD 17)
project (pika)

set(EP_BASE_SUFFIX "buildtrees")
set_property(DIRECTORY PROPERTY EP_BASE ${CMAKE_CURRENT_SOURCE_DIR}/${EP_BASE_SUFFIX})
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/modules/")
set(STAGED_INSTALL_PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/deps)
set(INSTALL_INCLUDEDIR ${STAGED_INSTALL_PREFIX}/include)
set(INSTALL_LIBDIR ${STAGED_INSTALL_PREFIX}/lib)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${STAGED_INSTALL_PREFIX})
message(STATUS "${PROJECT_NAME} staged install: ${STAGED_INSTALL_PREFIX}")
include(ExternalProject)

ExternalProject_Add(gtest
  URL
    https://github.com/google/googletest/archive/refs/tags/release-1.12.1.tar.gz
  URL_HASH
    MD5=e82199374acdfda3f425331028eb4e2a
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
  BUILD_ALWAYS
    1
)

set(GTEST_INCLUDE_DIR ${INSTALL_INCLUDEDIR})
set(GTEST_LIBRARY ${INSTALL_LIBDIR}/libgtest.a)

ExternalProject_Add(gflags
  URL
    https://github.com/gflags/gflags/archive/refs/tags/v2.2.2.tar.gz
  URL_HASH
    MD5=1a865b93bacfa963201af3f75b7bd64c
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  BUILD_ALWAYS
    1
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
    -DGFLAGS_NAMESPACE=gflags
    -DBUILD_STATIC_LIBS=ON
    -DBUILD_SHARED_LIBS=OFF

)
set(GFLAGS_LIBRARIE ${INSTALL_LIBDIR}/libgflags.a)
set(GFLAGS_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

ExternalProject_Add(glog
  DEPENDS
    gflags
    gtest
  URL
    https://github.com/google/glog/archive/refs/tags/v0.6.0.tar.gz
  URL_HASH
    MD5=c98a6068bc9b8ad9cebaca625ca73aa2
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  BUILD_ALWAYS
    1
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
    -DWITH_GFLAGS=ON
    -DBUILD_TESTING=OFF
    -DBUILD_STATIC_LIBS=ON
    -DBUILD_SHARED_LIBS=OFF
    -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
)
set(GLOG_LIBRARIE ${INSTALL_LIBDIR}/libglog.a)
set(GLOG_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

ExternalProject_Add(snappy
  DEPENDS
  URL
    https://github.com/google/snappy/archive/refs/tags/1.1.7.tar.gz
  URL_HASH
    MD5=ee9086291c9ae8deb4dac5e0b85bf54a
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
    -DSNAPPY_BUILD_TESTS=OFF
    -DBUILD_STATIC_LIBS=ON
    -DBUILD_SHARED_LIBS=OFF
  BUILD_ALWAYS
    1
)
set(SNAPPY_LIBRARIE ${INSTALL_LIBDIR}/libsnappy.a)
set(SNAPPY_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

ExternalProject_Add(zstd
  DEPENDS
  URL
    https://github.com/facebook/zstd/releases/download/v1.5.4/zstd-1.5.4.tar.gz
  URL_HASH
    MD5=2352b1f9ccc7446641046bb3d440c3ed
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  SOURCE_SUBDIR
    build/cmake
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
    -DBUILD_TESTING=OFF
    -DZSTD_BUILD_STATIC=ON
    -DZSTD_BUILD_SHARED=OFF
  BUILD_ALWAYS
    1
)
set(ZSTD_LIBRARIE ${INSTALL_LIBDIR}/libzstd.a)
set(ZSTD_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

ExternalProject_Add(lz4
  DEPENDS
  URL
    https://github.com/lz4/lz4/archive/refs/tags/v1.9.4.tar.gz
  URL_HASH
    MD5=e9286adb64040071c5e23498bf753261
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  SOURCE_SUBDIR
    build/cmake
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
    -DBUILD_TESTING=OFF
    -DBUILD_STATIC_LIBS=ON
    -DBUILD_SHARED_LIBS=OFF
  BUILD_ALWAYS
    1
)
set(LZ4_LIBRARIE ${INSTALL_LIBDIR}/liblz4.a)
set(LZ4_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

ExternalProject_Add(zlib
  DEPENDS
  URL
    https://github.com/madler/zlib/releases/download/v1.2.13/zlib-1.2.13.tar.gz
  URL_HASH
    MD5=9b8aa094c4e5765dabf4da391f00d15c
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
  BUILD_ALWAYS
    1
)
set(ZLIB_LIBRARIE ${INSTALL_LIBDIR}/libz.a)
set(ZLIB_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

ExternalProject_Add(gperftools
  DEPENDS
  URL
    https://github.com/gperftools/gperftools/releases/download/gperftools-2.10/gperftools-2.10.tar.gz
  URL_HASH
    MD5=62bf6c76ba855ed580de5e139bd2a483
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
    -DGPERFTOOLS_BUILD_STATIC=ON
    -DEFAULT_BUILD_MINIMAL=ON
)
set(TCMALLOC_LIBRARIE ${INSTALL_LIBDIR}/libtcmalloc.a)
set(TCMALLOC_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

ExternalProject_Add(jemalloc
  DEPENDS
  URL
    https://github.com/jemalloc/jemalloc/archive/refs/tags/5.3.0.tar.gz
  URL_HASH
    MD5=594dd8e0a1e8c1ef8a1b210a1a5aff5b
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  CONFIGURE_COMMAND
    <SOURCE_DIR>/autogen.sh --prefix=${STAGED_INSTALL_PREFIX}
  BUILD_IN_SOURCE
    1
  BUILD_COMMAND
    make -j8
  BUILD_ALWAYS
    1
  INSTALL_COMMAND
    make install
)
set(JEMALLOC_LIBRARIE ${INSTALL_LIBDIR}/libjemalloc.a)
set(JEMALLOC_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

ExternalProject_Add(rocksdb
  DEPENDS
    gflags
    gtest
    snappy
    zstd
    lz4
    zlib
    gperftools
    jemalloc
  URL
    https://github.com/facebook/rocksdb/archive/refs/tags/v7.7.3.tar.gz
  URL_HASH
    # MD5=594dd8e0a1e8c1ef8a1b210a1a5aff5b
  DOWNLOAD_NO_PROGRESS
    1
  UPDATE_COMMAND
    ""
  LOG_CONFIGURE
    1
  LOG_BUILD
    1
  LOG_INSTALL
    1
  BUILD_ALWAYS
    1
  CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
    -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
    -DWITH_BENCHMARK=OFF
    -DWITH_BENCHMARK_TOOLS=OFF
    -DWITH_TOOLS=OFF
    -DWITH_CORE_TOOLS=OFF
    -DWITH_TESTS=OFF
    -DWITH_TRACE_TOOLS=OFF
    -DWITH_EXAMPLES=OFF
    -DROCKSDB_BUILD_SHARED=OFF
    -DWITH_JEMALLOC=ON
    -DWITH_LZ4=ON
    -DWITH_SNAPPY=ON
    -DWITH_ZLIB=ON
    -DWITH_ZSTD=ON
    -DWITH_GFLAGS=ON
)
set(ROCKSDB_LIBRARIE ${INSTALL_LIBDIR}/librocksdb.a)
set(ROCKSDB_INCLUDE_DIR ${INSTALL_INCLUDEDIR})
set(ROCKSDB_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/${EP_BASE_SUFFIX}/Source/rocksdb)

add_subdirectory(src/slash)
add_subdirectory(src/pink)
add_subdirectory(src/blackwidow)
aux_source_directory(src  DIR_SRCS)

## generate version
string(TIMESTAMP TS "%Y-%m-%d %H:%M:%S" UTC)
set(PIKA_BUILD_DATE "${TS}" CACHE STRING "the time we first built pika")

find_package(Git)
if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_SHA COMMAND "${GIT_EXECUTABLE}" rev-parse HEAD )
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" RESULT_VARIABLE PIKA_GIT_MOD COMMAND "${GIT_EXECUTABLE}" diff-index HEAD --quiet)
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_DATE COMMAND "${GIT_EXECUTABLE}" log -1 --date=format:"%Y-%m-%d %T" --format="%ad")
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_TAG RESULT_VARIABLE rv COMMAND "${GIT_EXECUTABLE}" symbolic-ref -q --short HEAD OUTPUT_STRIP_TRAILING_WHITESPACE)
  if (rv AND NOT rv EQUAL 0)
    execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_TAG COMMAND "${GIT_EXECUTABLE}" describe --tags --exact-match OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()
else()
  set(PIKA_GIT_SHA 0)
  set(PIKA_GIT_MOD 1)
endif()
string(REGEX REPLACE "[^0-9a-fA-F]+" "" PIKA_GIT_SHA "${PIKA_GIT_SHA}")
string(REGEX REPLACE "[^0-9: /-]+" "" PIKA_GIT_DATE "${PIKA_GIT_DATE}")

message("pika GIT_SHA = ${PIKA_GIT_SHA}")
message("pika GIT_MOD = ${PIKA_GIT_MOD}")
message("pika GIT_DATE = ${PIKA_GIT_DATE}")
message("pika GIT_TAG = ${PIKA_GIT_TAG}")
message("pika BUILD_DATE = ${PIKA_BUILD_DATE}")

set(PIKA_BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/pika_build_version.cc)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/build_version.cc.in ${PIKA_BUILD_VERSION_CC} @ONLY)

find_package(Protobuf REQUIRED)
set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pika_inner_message.proto
)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
message("pika PROTO_SRCS = ${PROTO_SRCS}")
message("pika PROTO_HDRS = ${PROTO_HDRS}")

add_executable(${PROJECT_NAME}
               ${DIR_SRCS}
               ${PROTO_SRCS}
               ${PROTO_HDRS}
               ${PIKA_BUILD_VERSION_CC})

target_include_directories(${PROJECT_NAME} 
                           PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
                           PUBLIC ${PROJECT_SOURCE_DIR}
                           ${INSTALL_INCLUDEDIR}
                           )

target_link_libraries(${PROJECT_NAME}
  blackwidow
  pink
  slash
  ${GLOG_LIBRARIE}
  ${Protobuf_LIBRARIES}
  ${GFLAGS_LIBRARIE}
  ${SNAPPY_LIBRARIE}
  ${ZSTD_LIBRARIE}
  ${LZ4_LIBRARIE}
  ${ZLIB_LIBRARIE}
  ${JEMALLOC_LIBRARIE})