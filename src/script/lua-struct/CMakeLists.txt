# define your own "large" integer type; not defining a proper type
# will default to 'long', which may cause problems with 'size_t'
set(INTTYPE '-DSTRUCT_INT="long long"')

set(CWARNS "-Wall -W -pedantic \
        -Waggregate-return \
        -Wcast-align \
        -Wmissing-prototypes \
        -Wnested-externs \
        -Wpointer-arith \
        -Wshadow \
        -Wwrite-strings")

set(CFLAGS "-D_POSIX_SOURCE ${CWARNS} ${INTTYPE} -O2")
add_library(lua-struct STATIC struct.c)
target_compile_options(lua-struct PRIVATE ${CFLAGS})
target_link_libraries(lua-struct ${LUA_LIBRARY})
target_include_directories(lua-struct PRIVATE ${LUA_INCLUDE_DIR})
add_dependencies(lua-struct lua)