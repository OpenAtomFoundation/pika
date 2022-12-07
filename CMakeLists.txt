cmake_minimum_required (VERSION 3.10)

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-maybe-uninitialized" CACHE STRING "Set C++ Compiler Flags" FORCE)

project (pika)

add_subdirectory(third/rocksdb)
add_subdirectory(src/slash)
add_subdirectory(src/pink)
add_subdirectory(src/blackwidow)

aux_source_directory(src  DIR_SRCS)

## generate version
string(TIMESTAMP TS "%Y-%m-%d %H:%M:%S" UTC)
set(PIKA_BUILD_DATE "${TS}" CACHE STRING "the time we first built pika")

find_package(Git)
if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_SHA COMMAND "${GIT_EXECUTABLE}" rev-parse HEAD )
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" RESULT_VARIABLE PIKA_GIT_MOD COMMAND "${GIT_EXECUTABLE}" diff-index HEAD --quiet)
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_DATE COMMAND "${GIT_EXECUTABLE}" log -1 --date=format:"%Y-%m-%d %T" --format="%ad")
  execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_TAG RESULT_VARIABLE rv COMMAND "${GIT_EXECUTABLE}" symbolic-ref -q --short HEAD OUTPUT_STRIP_TRAILING_WHITESPACE)
  if (rv AND NOT rv EQUAL 0)
    execute_process(WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" OUTPUT_VARIABLE PIKA_GIT_TAG COMMAND "${GIT_EXECUTABLE}" describe --tags --exact-match OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()
else()
  set(PIKA_GIT_SHA 0)
  set(PIKA_GIT_MOD 1)
endif()
string(REGEX REPLACE "[^0-9a-fA-F]+" "" PIKA_GIT_SHA "${PIKA_GIT_SHA}")
string(REGEX REPLACE "[^0-9: /-]+" "" PIKA_GIT_DATE "${PIKA_GIT_DATE}")

message("pika GIT_SHA = ${PIKA_GIT_SHA}")
message("pika GIT_MOD = ${PIKA_GIT_MOD}")
message("pika GIT_DATE = ${PIKA_GIT_DATE}")
message("pika GIT_TAG = ${PIKA_GIT_TAG}")
message("pika BUILD_DATE = ${PIKA_BUILD_DATE}")

set(PIKA_BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/pika_build_version.cc)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/src/build_version.cc.in ${PIKA_BUILD_VERSION_CC} @ONLY)


find_package(Protobuf REQUIRED)
set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pika_inner_message.proto
)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
message("pika PROTO_SRCS = ${PROTO_SRCS}")
message("pika PROTO_HDRS = ${PROTO_HDRS}")
add_executable(${PROJECT_NAME}
               ${DIR_SRCS}
               ${PROTO_SRCS}
               ${PROTO_HDRS}
               ${PIKA_BUILD_VERSION_CC})
target_include_directories(${PROJECT_NAME} 
                           PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
                           PUBLIC ${PROJECT_SOURCE_DIR})
target_link_libraries(${PROJECT_NAME}
  blackwidow
  pink
  slash
  rocksdb
  pthread
  gflags
  glog
  ${Protobuf_LIBRARIES}
  )