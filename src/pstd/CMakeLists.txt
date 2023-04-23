cmake_minimum_required(VERSION 3.18)

set (CMAKE_CXX_STANDARD 11)
project (pstd)

# 强制使用用户自定的 memcmp
add_compile_options("-fno-builtin-memcmp -pipe")


set(CMAKE_SYSTEM_PROCESSOR ${CMAKE_HOST_SYSTEM_PROCESSOR})
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64")
  add_compile_options(-msse)
endif()

add_compile_options(-W -Wextra -Wall -Wsign-compare)
add_compile_options(-Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings)
add_compile_options(-Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Woverloaded-virtual)
add_compile_options(-Wnon-virtual-dtor -Wno-missing-field-initializers)

set(DISABLE_WARNING_AS_ERROR ON)
if(NOT DISABLE_WARNING_AS_ERROR)
  add_compile_options(-Werror)
endif()


add_subdirectory(tests)
add_subdirectory(examples)

aux_source_directory(./src  DIR_SRCS)

add_library(pstd STATIC ${DIR_SRCS})

target_include_directories(pstd
    PUBLIC ${PROJECT_SOURCE_DIR}/..
           ${PROJECT_SOURCE_DIR}/include
           ${INSTALL_INCLUDEDIR}
)
