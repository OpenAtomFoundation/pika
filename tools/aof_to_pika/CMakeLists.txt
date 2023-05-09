set(CXXFLAGS "-Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11")
set(SRC_DIR ./src)

aux_source_directory(${SRC_DIR} BASE_OBJS)

add_executable(aof_to_pika ${BASE_OBJS})

target_include_directories(aof_to_pika PRIVATE ./include PRIVATE ../..)

target_link_libraries(aof_to_pika pthread)
set_target_properties(aof_to_pika PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    CMAKE_COMPILER_IS_GNUCXX TRUE
    COMPILE_FLAGS ${CXXFLAGS})

