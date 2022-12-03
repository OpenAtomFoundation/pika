cmake_minimum_required (VERSION 3.10)

set (CMAKE_CXX_STANDARD 11)

project (pink)

aux_source_directory(./src  DIR_SRCS)

add_library(pink STATIC ${DIR_SRCS} )

target_include_directories(pink PUBLIC ${PROJECT_SOURCE_DIR}/..)