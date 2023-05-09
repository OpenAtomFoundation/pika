set(CXXFLAGS "-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -Wall -W -DDEBUG -g -O2 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++17")

set(SRC_DIR .)
aux_source_directory(${SRC_DIR} BASE_OBJS)

add_executable(redisConn ${BASE_OBJS})

target_include_directories(redisConn PRIVATE ${INSTALL_INCLUDEDIR})

target_link_libraries(redisConn ${HIREDIS_LIBRARY})
set_target_properties(redisConn PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tools
    CMAKE_COMPILER_IS_GNUCXX TRUE
    COMPILE_FLAGS ${CXXFLAGS})