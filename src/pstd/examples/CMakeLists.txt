cmake_minimum_required (VERSION 3.18)

aux_source_directory(../src  DIR_SRCS)
set(CMAKE_CXX_STANDARD 17)

file(GLOB_RECURSE PSTD_EXAMPLES_SOURCE "${PROJECT_SOURCE_DIR}/examples/*.cc")


foreach(pstd_example_source ${PSTD_EXAMPLES_SOURCE})
get_filename_component(pstd_example_filename ${pstd_example_source} NAME)
  string(REPLACE ".cc" "" pstd_example_name ${pstd_example_filename})

  add_executable(${pstd_example_name} ${pstd_example_source})
  target_include_directories(${pstd_example_name}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PUBLIC ${PROJECT_SOURCE_DIR}/..
    ${ROCKSDB_INCLUDE_DIR}
    ${ROCKSDB_SOURCE_DIR}
  )
  add_dependencies(${pstd_example_name} pstd glog gflags ${LIBUNWIND_NAME})

  target_link_libraries(${pstd_example_name}
    PUBLIC storage
    PUBLIC pstd
    PUBLIC ${GLOG_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${LIBUNWIND_LIBRARY}
    PUBLIC pthread
  )
endforeach()
