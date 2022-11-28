cmake_minimum_required (VERSION 3.0)

set (CMAKE_CXX_STANDARD 11)
project (slash)

aux_source_directory(./src  DIR_SRCS)

add_library(slash STATIC ${DIR_SRCS} )

target_include_directories(slash PUBLIC ${PROJECT_SOURCE_DIR}/..)