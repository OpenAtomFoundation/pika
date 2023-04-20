cmake_minimum_required (VERSION 3.10)

aux_source_directory(../src  DIR_SRCS)


file(GLOB_RECURSE STORAGE_BENCHMARK_SOURCE "${PROJECT_SOURCE_DIR}/benchmark/*.cc")


foreach(storage_benchmark_source ${STORAGE_BENCHMARK_SOURCE})
  get_filename_component(storage_benchmark_filename ${storage_benchmark_source} NAME)
  string(REPLACE ".cc" "" storage_benchmark_name ${storage_benchmark_filename})

  add_executable(${storage_benchmark_name} EXCLUDE_FROM_ALL ${storage_benchmark_filename})
  target_include_directories(${storage_benchmark_name}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PUBLIC ${PROJECT_SOURCE_DIR}/..
    ${ROCKSDB_INCLUDE_DIR}
    ${ROCKSDB_SOURCE_DIR}
  )
  add_dependencies(${storage_benchmark_name} storage pstd glog gflags ${LIBUNWIND_NAME})

  target_link_libraries(${storage_benchmark_name}
    PUBLIC storage
    PUBLIC pstd
    PUBLIC ${GLOG_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${LIBUNWIND_LIBRARY}
    PUBLIC pthread
  )
endforeach()
