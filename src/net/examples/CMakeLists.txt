cmake_minimum_required(VERSION 3.18)

set (CMAKE_CXX_STANDARD 17)
add_subdirectory(performance)

aux_source_directory(../src  DIR_SRCS)

file(GLOB NET_EXAMPLES_SOURCE "${PROJECT_SOURCE_DIR}/examples/*.cc")

# 这里需要将proto文件转换成cc和h文件，再编译
set(PROTO_FILES ${PROJECT_SOURCE_DIR}/examples/myproto.proto)
custom_protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})


foreach(net_example_source ${NET_EXAMPLES_SOURCE})
  get_filename_component(net_example_filename ${net_example_source} NAME)
  string(REPLACE ".cc" "" net_example_name ${net_example_filename})

  add_executable(${net_example_name} EXCLUDE_FROM_ALL ${net_example_source} ${PROTO_SRCS} ${PROTO_HDRS})
  target_include_directories(${net_example_name}
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
    PUBLIC ${PROJECT_SOURCE_DIR}/include
    PUBLIC ${PROJECT_SOURCE_DIR}/src
    ${ROCKSDB_INCLUDE_DIR}
    ${ROCKSDB_SOURCE_DIR}
  )
  add_dependencies(${net_example_name} net pstd glog gflags unwind protobuf)

  target_link_libraries(${net_example_name}
    PUBLIC net
    PUBLIC pstd
    PUBLIC storage
    PUBLIC ${GLOG_LIBRARY}
    PUBLIC ${GFLAGS_LIBRARY}
    PUBLIC ${LIBUNWIND_LIBRARY}
    PUBLIC pthread
    PUBLIC ${PROTOBUF_LIBRARY}
  )
endforeach()
