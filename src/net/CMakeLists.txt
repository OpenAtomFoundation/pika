cmake_minimum_required (VERSION 3.18)

set (CMAKE_CXX_STANDARD 17)

project (net)

aux_source_directory(./src  DIR_SRCS)

if(USE_SSL)
  add_definitions("-D__ENABLE_SSL")
endif()

add_subdirectory(test)
add_subdirectory(examples)

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  list(FILTER DIR_SRCS EXCLUDE REGEX ".net_kqueue.*")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin" OR ${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD")
  list(FILTER DIR_SRCS EXCLUDE REGEX ".net_epoll.*")
endif()

add_library(net STATIC ${DIR_SRCS} )

add_dependencies(net protobuf glog gflags ${LIBUNWIND_NAME})


target_include_directories(net
  PUBLIC ${PROJECT_SOURCE_DIR}/..
  PUBLIC ${INSTALL_INCLUDEDIR})

target_link_libraries(net
    PUBLIC ${GLOG_LIBRARY}
           ${GFLAGS_LIBRARY}
           ${LIBUNWIND_LIBRARY}
)
