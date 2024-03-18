cmake_minimum_required(VERSION 3.18)

include(GoogleTest)

file(GLOB_RECURSE BLACKWINDOW_TEST_SOURCE "${PROJECT_SOURCE_DIR}/tests/*.cc")

aux_source_directory(../src TEST_SRCS)

add_compile_definitions(PIKA_ROOT_DIR="${CMAKE_SOURCE_DIR}")

# set(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/build)
foreach(blackwindow_test_source ${BLACKWINDOW_TEST_SOURCE})
  get_filename_component(storage_test_filename ${blackwindow_test_source} NAME)
  string(REPLACE ".cc" "" blackwindow_test_name ${storage_test_filename})

  # Add the test target
  add_executable(${blackwindow_test_name} ${blackwindow_test_source})
  target_include_directories(${blackwindow_test_name}
    PUBLIC ${CMAKE_SOURCE_DIR}/include
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PUBLIC ${PROJECT_SOURCE_DIR}/..
    ${ROCKSDB_INCLUDE_DIR}
    ${ROCKSDB_SOURCE_DIR}
  )
  add_dependencies(${blackwindow_test_name} gtest glog gflags ${LIBUNWIND_NAME})
  target_link_libraries(${blackwindow_test_name}
    PUBLIC ${GTEST_LIBRARY}
    PUBLIC ${ROCKSDB_LIBRARY}
    PUBLIC pstd
    PUBLIC net
    PUBLIC storage
    PUBLIC ${GLOG_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${LIBUNWIND_LIBRARY}
  )
  if (USE_S3)
    target_link_libraries(${blackwindow_test_name}
      PUBLIC libaws-cpp-sdk-core.so 
      PUBLIC libaws-cpp-sdk-transfer.so
      PUBLIC libaws-cpp-sdk-kinesis.so
      PUBLIC libaws-cpp-sdk-s3.so
    )
  endif()
  add_test(NAME ${blackwindow_test_name}
    COMMAND ${blackwindow_test_name}
    WORKING_DIRECTORY .)
endforeach(blackwindow_test_source ${BLACKWINDOW_TEST_SOURCE})
