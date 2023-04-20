cmake_minimum_required (VERSION 3.18)

aux_source_directory(../src  DIR_SRCS)
set(CMAKE_CXX_STANDARD 17)


file(GLOB PERFORMANCE_PROTO_FILES ${PROJECT_SOURCE_DIR}/examples/performance/*.proto)
message(PERFORMANCE_PROTO_FILES: ${PERFORMANCE_PROTO_FILES})
set(proto_cxx_files "")
set(proto_h_files "")

foreach(proto_files ${PERFORMANCE_PROTO_FILES})
  custom_protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${proto_files})
  list(APPEND proto_cxx_files ${PROTO_SRCS})
  list(APPEND proto_h_files ${PROTO_HDRS})
endforeach()


file(GLOB NET_EXAMPLES_PERFORMANCE_SOURCE ${PROJECT_SOURCE_DIR}/examples/performance/*.cc)


foreach(net_example_performance_source ${NET_EXAMPLES_PERFORMANCE_SOURCE})
  get_filename_component(net_example_performance_filename ${net_example_performance_source} NAME)
  string(REPLACE ".cc" "" net_example_performance_name ${net_example_performance_filename})

  add_executable(${net_example_performance_name} EXCLUDE_FROM_ALL ${net_example_performance_source} ${proto_cxx_files} ${proto_h_files})
  target_include_directories(${net_example_performance_name}
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PUBLIC ${PROJECT_SOURCE_DIR}/..
    ${ROCKSDB_INCLUDE_DIR}
    ${ROCKSDB_SOURCE_DIR}
  )
  add_dependencies(${net_example_performance_name} net pstd glog gflags ${LIBUNWIND_NAME} protobuf)

  target_link_libraries(${net_example_performance_name}
    PUBLIC net
    PUBLIC storage
    PUBLIC pstd
    PUBLIC ${GLOG_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${LIBUNWIND_LIBRARY}
    PUBLIC pthread
    PUBLIC ${PROTOBUF_LIBRARY}
  )
endforeach()
