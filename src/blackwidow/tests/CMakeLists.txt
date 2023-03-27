cmake_minimum_required(VERSION 3.10)

include(GoogleTest)

file(GLOB_RECURSE BLACKWINDOW_TEST_SOURCE "${PROJECT_SOURCE_DIR}/tests/*.cc")

aux_source_directory(../stc TEST_SRCS)

foreach(blackwindow_test_source ${BLACKWINDOW_TEST_SOURCE})
  get_filename_component(blackwidow_test_filename ${blackwindow_test_source} NAME)
  message(${blackwidow_test_filename})
  string(REPLACE ".cc" "" blackwindow_test_name ${blackwidow_test_filename})

  # Add the test target
  add_executable(${blackwindow_test_name} ${blackwindow_test_source})
  target_include_directories(${blackwindow_test_name}
      PUBLIC ${PROJECT_SOURCE_DIR}/include
      PUBLIC ${PROJECT_SOURCE_DIR}/..
             ${ROCKSDB_INCLUDE_DIR}
             ${ROCKSDB_SOURCE_DIR}
      )
  add_dependencies(${blackwindow_test_name} gtest)     
  target_link_libraries(${blackwindow_test_name}
      PUBLIC ${GTEST_LIBRARY}
      PUBLIC pstd
      PUBLIC ${ROCKSDB_LIBRARY}
      PUBLIC blackwidow
      )
endforeach(blackwindow_test_source ${BLACKWINDOW_TEST_SOURCE})
