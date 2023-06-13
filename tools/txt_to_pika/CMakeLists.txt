set(WARNING_FLAGS "-W -Wextra -Wall -Wsign-compare \
-Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers")

set(CXXFLAGS "${WARNING_FLAGS} -std=c++17 -g")

set(SRC_DIR .)
aux_source_directory(${SRC_DIR} BASE_OBJS)

add_executable(txt_to_pika ${BASE_OBJS})

target_include_directories(txt_to_pika PRIVATE ${INSTALL_INCLUDEDIR}
                                       PRIVATE ${PROJECT_SOURCE_DIR}
                                        ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(txt_to_pika storage net pstd ${ROCKSDB_LIBRARY} pthread ${SNAPPY_LIBRARY}
                                  ${ZLIB_LIBRARY} ${BZ2_LIBRARY} ${GLOG_LIBRARY} ${GFLAGS_LIBRARY} rt)
set_target_properties(txt_to_pika PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    CMAKE_COMPILER_IS_GNUCXX TRUE
    COMPILE_FLAGS ${CXXFLAGS})