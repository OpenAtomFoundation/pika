cmake_minimum_required (VERSION 3.10)

aux_source_directory(../src  DIR_SRCS)


file(GLOB_RECURSE STORAGE_EXAMPLES_SOURCE "${PROJECT_SOURCE_DIR}/examples/*.cc")


foreach(storage_example_source ${STORAGE_EXAMPLES_SOURCE})
  get_filename_component(storage_example_filename ${storage_example_source} NAME)
  string(REPLACE ".cc" "" storage_example_name ${storage_example_filename})

  add_executable(${storage_example_name} EXCLUDE_FROM_ALL ${storage_example_filename})
  target_include_directories(${storage_example_name}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PUBLIC ${PROJECT_SOURCE_DIR}/..
    ${ROCKSDB_INCLUDE_DIR}
    ${ROCKSDB_SOURCE_DIR}
  )
  add_dependencies(${storage_example_name} storage pstd glog gflags unwind)

  target_link_libraries(${storage_example_name}
    PUBLIC storage
    PUBLIC pstd
    PUBLIC ${GLOG_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${LIBUNWIND_LIBRARY}
    PUBLIC pthread
  )
endforeach()
