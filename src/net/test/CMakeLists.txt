cmake_minimum_required(VERSION 3.18)

include(GoogleTest)
aux_source_directory(../src  DIR_SRCS)
set(CMAKE_CXX_STANDARD 17)

file(GLOB_RECURSE NET_TEST_SOURCE "${PROJECT_SOURCE_DIR}/test/*.cc")


foreach(net_test_source ${NET_TEST_SOURCE})
  get_filename_component(net_test_filename ${net_test_source} NAME)
  string(REPLACE ".cc" "" net_test_name ${net_test_filename})


  add_executable(${net_test_name} ${net_test_source})
  target_include_directories(${net_test_name}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PUBLIC ${PROJECT_SOURCE_DIR}/..
    ${ROCKSDB_INCLUDE_DIR}
    ${ROCKSDB_SOURCE_DIR}
  )
  add_dependencies(${net_test_name} net gtest glog gflags ${LIBUNWIND_NAME} pstd)
  target_link_libraries(${net_test_name}
    PUBLIC net
    PUBLIC ${GTEST_LIBRARY}
    PUBLIC ${GLOG_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${LIBUNWIND_LIBRARY}
    PUBLIC ${GMOCK_LIBRARY}
    PUBLIC ${GTEST_MAIN_LIBRARY}
    PUBLIC pstd
  )
  add_test(NAME ${net_test_name}
    COMMAND ${net_test_name}
    WORKING_DIRECTORY .)
endforeach()