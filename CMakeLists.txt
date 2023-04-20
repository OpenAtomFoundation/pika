cmake_minimum_required(VERSION 3.18)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

set(CMAKE_CXX_STANDARD 17)
project(pika)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
enable_testing()

execute_process(COMMAND uname -p OUTPUT_VARIABLE HOST_ARCH)
string(TOLOWER ${HOST_ARCH} HOST_ARCH)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RElEASE)
endif()

string(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE)

if(${BUILD_TYPE} STREQUAL DEBUG)
  set(LIB_BUILD_TYPE DEBUG)
elseif(${BUILD_TYPE} STREQUAL MINSIZEREL)
  set(LIB_BUILD_TYPE MINSIZEREL)
elseif(${BUILD_TYPE} STREQUAL RELWITHDEBINFO)
  set(LIB_BUILD_TYPE RELWITHDEBINFO)
else()
  set(LIB_BUILD_TYPE RElEASE)
  set(CMAKE_CXX_FLAGS_RELEASE "-O2")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
   set(CMAKE_CXX_FLAGS "-pthread")
   add_definitions(-DOS_MACOSX)
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
   set(CMAKE_EXE_LINKER_FLAGS "-static-libgcc -static-libstdc++")
   set(CMAKE_CXX_FLAGS "-pthread -Wl,--no-as-needed -ldl")
   add_definitions(-DOS_LINUX)
else()
   message(FATAL_ERROR "only support linux or macOs")
endif()

if(HOST_ARCH MATCHES "x86_64" OR HOST_ARCH MATCHES "i386")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse -msse4.2")
elseif(HOST_ARCH MATCHES "arm")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv8-a+crc+crypto -moutline-atomics")
endif()

set(EP_BASE_SUFFIX "buildtrees")
set_property(DIRECTORY PROPERTY EP_BASE ${CMAKE_CURRENT_SOURCE_DIR}/${EP_BASE_SUFFIX})
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/modules/")
set(STAGED_INSTALL_PREFIX ${CMAKE_CURRENT_SOURCE_DIR}/deps)
set(CMAKE_UTILS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/utils)
set(INSTALL_INCLUDEDIR ${STAGED_INSTALL_PREFIX}/include)
set(INSTALL_LIBDIR ${STAGED_INSTALL_PREFIX}/lib)
set(INSTALL_LIBDIR_64 ${STAGED_INSTALL_PREFIX}/lib64)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${STAGED_INSTALL_PREFIX})

execute_process(COMMAND sh ${CMAKE_UTILS_DIR}/Get_OS_Version.sh
  OUTPUT_VARIABLE OS_VERSION)

message(STATUS "${PROJECT_NAME} staged install: ${STAGED_INSTALL_PREFIX}")
message(STATUS "Current platform: ${OS_VERSION} ")
cmake_host_system_information(RESULT CPU_CORE QUERY NUMBER_OF_LOGICAL_CORES)
message(STATUS "Cpu core ${CPU_CORE}")

find_program(AUTOCONF
        autoconf
        PATHS /usr/bin /usr/local/bin)

if (${AUTOCONF} MATCHES AUTOCONF-NOTFOUND)
  message(FATAL_ERROR "not find autoconf on localhost")
endif()

include(protogen.cmake)
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
  -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
  BUILD_ALWAYS
  1
  BUILD_COMMAND
  make -j${CPU_CORE}
)

if(${OS_VERSION} MATCHES "CentOS")
  set(GTEST_LIBRARY ${INSTALL_LIBDIR_64}/libgtest.a)
else()
  set(GTEST_LIBRARY ${INSTALL_LIBDIR}/libgtest.a)
endif()

set(GTEST_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

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
  -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
  -DGFLAGS_NAMESPACE=gflags
  -DBUILD_STATIC_LIBS=ON
  -DBUILD_SHARED_LIBS=OFF
  BUILD_COMMAND
  make -j${CPU_CORE}
)

if(${LIB_BUILD_TYPE} STREQUAL DEBUG)
  set(LIB_GFLAGS libgflags_debug.a)
else()
  set(LIB_GFLAGS libgflags.a)
endif()

set(GFLAGS_LIBRARY ${INSTALL_LIBDIR}/LIB_GFLAGS)
set(GFLAGS_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  ExternalProject_Add(unwind
    DEPENDS
    URL
    https://github.com/libunwind/libunwind/releases/download/v1.6.2/libunwind-1.6.2.tar.gz
    URL_HASH
    MD5=f625b6a98ac1976116c71708a73dc44a
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
    <SOURCE_DIR>/configure --prefix=${STAGED_INSTALL_PREFIX} --enable-minidebuginfo=no --enable-zlibdebuginfo=no
    BUILD_IN_SOURCE
    1
    BUILD_COMMAND
    make -j${CPU_CORE}
    INSTALL_COMMAND
    make install
  )
  set(LIBUNWIND_LIBRARY ${INSTALL_LIBDIR}/libunwind.a)
  set(LIBUNWIND_INCLUDE_DIR ${INSTALL_INCLUDEDIR})
  set(LIBUNWIND_NAME unwind)
  set(LIBUNWIND_ON ON)
else()
  set(LIBUNWIND_ON OFF)
endif()

ExternalProject_Add(glog
  DEPENDS
  gflags
  gtest
  ${LIBUNWIND_NAME}
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
  -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
  -DWITH_GFLAGS=ON
  -DBUILD_TESTING=OFF
  -DBUILD_SHARED_LIBS=OFF
  -DWITH_UNWIND=${LIBUNWIND_ON}
  -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
  BUILD_COMMAND
  make -j${CPU_CORE}
)

if(${LIB_BUILD_TYPE} STREQUAL DEBUG)
  set(LIB_GLOG libglogd.a)
else()
  set(LIB_GLOG libglog.a)
endif()

set(GLOG_LIBRARY ${INSTALL_LIBDIR}/${LIB_GLOG})
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
  -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
  -DSNAPPY_BUILD_TESTS=OFF
  -DBUILD_STATIC_LIBS=ON
  -DBUILD_SHARED_LIBS=OFF
  BUILD_ALWAYS
  1
  BUILD_COMMAND
  make -j${CPU_CORE}
)

if(${OS_VERSION} MATCHES "CentOS")
  set(SNAPPY_LIBRARY ${INSTALL_LIBDIR_64}/libsnappy.a)
else()
  set(SNAPPY_LIBRARY ${INSTALL_LIBDIR}/libsnappy.a)
endif()

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
  -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
  -DBUILD_TESTING=OFF
  -DZSTD_BUILD_STATIC=ON
  -DZSTD_BUILD_SHARED=OFF
  BUILD_ALWAYS
  1
  BUILD_COMMAND
  make -j${CPU_CORE}
)

if(${OS_VERSION} MATCHES "CentOS")
  set(ZSTD_LIBRARY ${INSTALL_LIBDIR_64}/libzstd.a)
else()
  set(ZSTD_LIBRARY ${INSTALL_LIBDIR}/libzstd.a)
endif()

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
  -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
  -DBUILD_TESTING=OFF
  -DBUILD_STATIC_LIBS=ON
  -DBUILD_SHARED_LIBS=OFF
  BUILD_ALWAYS
  1
  BUILD_COMMAND
  make -j${CPU_CORE}
)

if(${OS_VERSION} MATCHES "CentOS")
  set(LZ4_LIBRARY ${INSTALL_LIBDIR_64}/liblz4.a)
else()
  set(LZ4_LIBRARY ${INSTALL_LIBDIR}/liblz4.a)
endif()

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
  -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
  BUILD_ALWAYS
  1
  BUILD_COMMAND
  make -j${CPU_CORE}
)

set(ZLIB_LIBRARY ${INSTALL_LIBDIR}/libz.a)
set(ZLIB_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
  ExternalProject_Add(gperftools
    DEPENDS
    unwind
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
    -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
    -DGPERFTOOLS_BUILD_STATIC=ON
    -DEFAULT_BUILD_MINIMAL=ON
    BUILD_COMMAND
    make -j${CPU_CORE}
  )
  set(LIBGPERF_NAME gperftools)
endif()

if(${OS_VERSION} MATCHES "CentOS")
  set(TCMALLOC_LIBRARY ${INSTALL_LIBDIR_64}/liblibtcmalloclz4.a)
else()
  set(TCMALLOC_LIBRARY ${INSTALL_LIBDIR}/libtcmalloc.a)
endif()

set(TCMALLOC_INCLUDE_DIR ${INSTALL_INCLUDEDIR})

if(CMAKE_SYSTEM_NAME MATCHES "Linux")
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
    make -j${CPU_CORE}
    BUILD_ALWAYS
    1
    INSTALL_COMMAND
    make install
  )

  set(JEMALLOC_LIBRARY ${INSTALL_LIBDIR}/libjemalloc.a)
  set(JEMALLOC_INCLUDE_DIR ${INSTALL_INCLUDEDIR})
  set(LIBJEMALLOC_NAME jemalloc)
  set(JEMALLOC_ON ON)
else()
  set(JEMALLOC_ON OFF)
endif()

ExternalProject_Add(protobuf
  DEPENDS
  zlib
  URL
  https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protobuf-cpp-3.17.3.tar.gz
  URL_HASH
  MD5=3fe4c2647e0991c014a386a896d0a116
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
  cmake
  CMAKE_ARGS
  -DCMAKE_INSTALL_PREFIX=${STAGED_INSTALL_PREFIX}
  -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
  -DBUILD_SHARED_LIBS=FALSE
  -Dprotobuf_BUILD_TESTS=FALSE
  BUILD_IN_SOURCE
  1
  BUILD_ALWAYS
  1
  BUILD_COMMAND
  make -j${CPU_CORE}
)

if(${LIB_BUILD_TYPE} STREQUAL DEBUG)
  set(LIB_PROTOBUF libprotobufd.a)
else()
  set(LIB_PROTOBUF libprotobuf.a)
endif()

set(PROTOBUF_INCLUDE_DIR ${INSTALL_INCLUDEDIR})
set(PROTOBUF_LIBRARY ${INSTALL_LIBDIR}/LIB_PROTOBUF)
set(PROTOBUF_PROTOC ${STAGED_INSTALL_PREFIX}/bin/protoc)

ExternalProject_Add(rocksdb
  DEPENDS
  gflags
  gtest
  snappy
  zstd
  lz4
  zlib
  ${LIBGPERF_NAME}
  ${LIBJEMALLOC_NAME}
  URL
  https://github.com/facebook/rocksdb/archive/refs/tags/v7.7.3.tar.gz
  URL_HASH
  MD5=3c5d371a8bd8340b1975ad016151709d
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
  -DCMAKE_BUILD_TYPE=${LIB_BUILD_TYPE}
  -DUSE_RTTI=1
  -DWITH_BENCHMARK=OFF
  -DWITH_BENCHMARK_TOOLS=OFF
  -DWITH_TOOLS=OFF
  -DWITH_CORE_TOOLS=OFF
  -DWITH_TESTS=OFF
  -DWITH_TRACE_TOOLS=OFF
  -DWITH_EXAMPLES=OFF
  -DROCKSDB_BUILD_SHARED=OFF
  -DWITH_JEMALLOC=${JEMALLOC_ON}
  -DWITH_LZ4=ON
  -DWITH_SNAPPY=ON
  -DWITH_ZLIB=ON
  -DWITH_ZSTD=ON
  -DWITH_GFLAGS=ON
  BUILD_COMMAND
  make -j${CPU_CORE}
)

if(${OS_VERSION} MATCHES "CentOS")
  set(ROCKSDB_LIBRARY ${INSTALL_LIBDIR_64}/librocksdb.a)
else()
  set(ROCKSDB_LIBRARY ${INSTALL_LIBDIR}/librocksdb.a)
endif()

set(ROCKSDB_INCLUDE_DIR ${INSTALL_INCLUDEDIR})
set(ROCKSDB_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/${EP_BASE_SUFFIX}/Source/rocksdb)

add_subdirectory(src/pstd)
add_subdirectory(src/net)
add_subdirectory(src/storage)
aux_source_directory(src DIR_SRCS)

# # generate version
string(TIMESTAMP TS "%Y-%m-%d %H:%M:%S" UTC)
set(PIKA_BUILD_DATE "${TS}" CACHE STRING "the time we first built pika")

find_package(Git)

if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_SHA COMMAND "${GIT_EXECUTABLE}" rev-parse HEAD)
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" RESULT_VARIABLE PIKA_GIT_MOD COMMAND "${GIT_EXECUTABLE}" diff-index HEAD --quiet)
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_DATE COMMAND "${GIT_EXECUTABLE}" log -1 --date=format:"%Y-%m-%d %T" --format="%ad")
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_TAG RESULT_VARIABLE rv COMMAND "${GIT_EXECUTABLE}" symbolic-ref -q --short HEAD OUTPUT_STRIP_TRAILING_WHITESPACE)

  if(rv AND NOT rv EQUAL 0)
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

set(PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/src/pika_inner_message.proto)
custom_protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
message("pika PROTO_SRCS = ${PROTO_SRCS}")
message("pika PROTO_HDRS = ${PROTO_HDRS}")

add_executable(${PROJECT_NAME}
  ${DIR_SRCS}
  ${PROTO_SRCS}
  ${PROTO_HDRS}
  ${PIKA_BUILD_VERSION_CC})

target_link_directories(${PROJECT_NAME}
  PUBLIC ${INSTALL_LIBDIR_64}
  PUBLIC ${INSTALL_LIBDIR})

add_dependencies(${PROJECT_NAME}
  gflags
  gtest
  ${LIBUNWIND_NAME}
  glog
  snappy
  zstd
  lz4
  zlib
  ${LIBGPERF_NAME}
  ${LIBJEMALLOC_NAME}
  rocksdb
  protobuf
  pstd
  net
  storage)

target_include_directories(${PROJECT_NAME}
  PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
  PUBLIC ${PROJECT_SOURCE_DIR}
  ${INSTALL_INCLUDEDIR}
)

target_link_libraries(${PROJECT_NAME}
  storage
  net
  pstd
  ${LIB_GLOG}
  librocksdb.a
  ${LIB_PROTOBUF}
  ${LIB_GFLAGS}
  libsnappy.a
  libzstd.a
  liblz4.a
  libz.a
  ${LIBUNWIND_LIBRARY}
  ${JEMALLOC_LIBRARY})
