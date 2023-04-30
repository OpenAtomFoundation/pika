cmake_minimum_required(VERSION 3.18)

include(GoogleTest)
aux_source_directory(../src  DIR_SRCS)
set(CMAKE_CXX_STANDARD 11)

file(GLOB_RECURSE PSTD_TEST_SOURCE "${PROJECT_SOURCE_DIR}/tests/*.cc")


foreach(pstd_test_source ${PSTD_TEST_SOURCE})
  get_filename_component(pstd_test_filename ${pstd_test_source} NAME)
  string(REPLACE ".cc" "" pstd_test_name ${pstd_test_filename})


  add_executable(${pstd_test_name} ${pstd_test_source})
  target_include_directories(${pstd_test_name}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PUBLIC ${PROJECT_SOURCE_DIR}/..
    ${ROCKSDB_INCLUDE_DIR}
    ${ROCKSDB_SOURCE_DIR}
  )

  add_dependencies(${pstd_test_name} pstd gtest glog gflags ${LIBUNWIND_NAME})
  target_link_libraries(${pstd_test_name}
    PUBLIC pstd
    PUBLIC ${GTEST_LIBRARY}
    PUBLIC ${GTEST_MAIN_LIBRARY}
    PUBLIC ${GLOG_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${LIBUNWIND_LIBRARY}
  )
  add_test(NAME ${pstd_test_name}
    COMMAND ${pstd_test_name}
    WORKING_DIRECTORY .)
endforeach()
