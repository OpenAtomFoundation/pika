set(CXXFLAGS "-O2 -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith \
-Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter \
-D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++17 -gdwarf-2 -Wno-redundant-decls")

set(SRC_DIR .)
aux_source_directory(${SRC_DIR} BASE_OBJS)

add_executable(pika_port ${BASE_OBJS})

target_include_directories(pika_port PRIVATE ${INSTALL_INCLUDEDIR}
                                       PRIVATE ${PROJECT_SOURCE_DIR})

target_link_libraries(pika_port ${ROCKSDB_LIBRARY} pthread rt net pstd storage ${GLOG_LIBRARY} 
                                ${GFLAGS_LIBRARY} ${SNAPPY_LIBRARY} ${ZLIB_LIBRARY} ${BZ2_LIBRARY}
                                ${LZ4_LIBRARY} ${ZSTD_LIBRARY})
set_target_properties(pika_port PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    CMAKE_COMPILER_IS_GNUCXX TRUE
    COMPILE_FLAGS ${CXXFLAGS})
add_dependencies(pika_port rocksdb glog gflags snappy zlib bz2 lz4 zstd)