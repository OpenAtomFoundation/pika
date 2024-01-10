set(WARNING_FLAGS "-g -W -Wextra -Wall -Wsign-compare \
-Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings -Wno-unused-result \
-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers")

set(CXXFLAGS "${WARNING_FLAGS} -std=c++17 -g")

set(SRC_DIR .)
aux_source_directory(${SRC_DIR} BASE_OBJS)

add_executable(benchmark_client ${BASE_OBJS})

target_include_directories(benchmark_client PRIVATE ${INSTALL_INCLUDEDIR} PRIVATE ${PROJECT_SOURCE_DIR} ${ROCKSDB_SOURCE_DIR} ${GLOG_INCLUDE_DIR})

target_link_libraries(benchmark_client pthread)
target_link_libraries(benchmark_client ${ROCKSDB_LIBRARY})
target_link_libraries(benchmark_client ${GLOG_LIBRARY})
target_link_libraries(benchmark_client ${SNAPPY_LIBRARY})
target_link_libraries(benchmark_client pstd)
target_link_libraries(benchmark_client ${HIREDIS_LIBRARY})
set_target_properties(benchmark_client PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    CMAKE_COMPILER_IS_GNUCXX TRUE
    COMPILE_FLAGS ${CXXFLAGS})
add_dependencies(benchmark_client snappy hiredis)
