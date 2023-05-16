set(WARNING_FLAGS "-W -Wextra -Wall -Wsign-compare \
-Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers")

set(CXXFLAGS "${WARNING_FLAGS} -std=c++17 -g")

set(SRC_DIR .)
aux_source_directory(${SRC_DIR} BASE_OBJS)

add_executable(manifest_generator ${BASE_OBJS})

target_include_directories(manifest_generator PRIVATE ${PROJECT_SOURCE_DIR})

target_link_libraries(manifest_generator net pstd pthread)
set_target_properties(manifest_generator PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    CMAKE_COMPILER_IS_GNUCXX TRUE
    COMPILE_FLAGS ${CXXFLAGS})