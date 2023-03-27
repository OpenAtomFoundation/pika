cmake_minimum_required (VERSION 3.10)

set (CMAKE_CXX_STANDARD 11)

project (net)

aux_source_directory(./src  DIR_SRCS)

add_library(net STATIC ${DIR_SRCS} )

add_dependencies(net 
  protobuf
)

message(STATUS "protobuf include dir: ${PROTOBUF_INCLUDE_DIR}")
target_include_directories(net
  PUBLIC ${PROJECT_SOURCE_DIR}/..
  PUBLIC ${INSTALL_INCLUDEDIR})