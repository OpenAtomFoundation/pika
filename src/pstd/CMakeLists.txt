cmake_minimum_required(VERSION 3.18)

set (CMAKE_CXX_STANDARD 11)
project (pstd)

#NOTE(LeeHao) : 禁止使用编译器自带的memcmp函数，而强制使用用户自定的memcmp函数实现,原来的makefile中有这个编译选项
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
endif(DISABLE_WARNING_AS_ERROR)


add_subdirectory(tests)
add_subdirectory(examples)

aux_source_directory(./src  DIR_SRCS)

add_library(pstd STATIC ${DIR_SRCS} )

target_include_directories(pstd
    PUBLIC ${PROJECT_SOURCE_DIR}/..
           ${PROJECT_SOURCE_DIR}/include
           ${INSTALL_INCLUDEDIR}
)
