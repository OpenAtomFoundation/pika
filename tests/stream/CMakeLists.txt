cmake_minimum_required(VERSION 3.18)

include(GoogleTest)

file(GLOB_RECURSE TEST_SOURCES "*.cc")
find_package(GTest REQUIRED)

# add_definitions(-DSRC_UNIT_TESTS)
aux_source_directory(${CMAKE_SOURCE_DIR}/src ROOT_SRC_DIR)

set(PROTO_FILES ${CMAKE_SOURCE_DIR}/src/pika_inner_message.proto)
custom_protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

foreach(test_source ${TEST_SOURCES})
  get_filename_component(test_name ${test_source} NAME_WE)
  message(STATUS "Korpse Adding Test: ${test_name}")

  # Add the test target
  add_executable(${test_name}
    ${test_source}
    ${ROOT_SRC_DIR}
    ${PROTO_SRCS}
    ${PROTO_HDRS}
    ${PIKA_BUILD_VERSION_CC}
  )

  # to disable the main function in pika.cc
  target_compile_definitions(${test_name} PRIVATE SRC_UNIT_TESTS)

  target_link_libraries(${test_name}
    GTest::GTest
    GTest::Main)

  target_link_directories(${test_name}
    PUBLIC ${INSTALL_LIBDIR_64}
    PUBLIC ${INSTALL_LIBDIR})

  message(CMAKE_SOURCE_BINARY_DIR: ${CMAKE_SOURCE_BINARY_DIR})

  # Add dependencies for this target
  add_dependencies(${test_name}
    gflags
    gtest
    ${LIBUNWIND_NAME}
    glog
    fmt
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

  # Include directories for this target
  target_include_directories(${test_name}
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
    PUBLIC ${CMAKE_BINARY_DIR} # for find pika_inner_message.pb.h
    PUBLIC ${PROJECT_SOURCE_DIR}
    ${INSTALL_INCLUDEDIR}
  )

  # Libraries to link this target against
  target_link_libraries(${test_name}
    storage
    net
    pstd
    ${GLOG_LIBRARY}
    librocksdb.a
    ${LIB_PROTOBUF}
    ${LIB_GFLAGS}
    ${LIB_FMT}
    libsnappy.a
    libzstd.a
    liblz4.a
    libz.a
    ${LIBUNWIND_LIBRARY}
    ${JEMALLOC_LIBRARY}
  )

  # Now simply register the test with gtest
  gtest_discover_tests(${test_name})
endforeach()
