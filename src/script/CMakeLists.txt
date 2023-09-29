# # if define USEJIT, we use LuaJIT, otherwise we use Lua
# SET(USEJIT OFF)
# MESSAGE(STATUS "USEJIT: ${USEJIT}")

# make command for platform
# linux
# if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
#     set(MAKE_LUA_COMMAND make linux)
# # macos
# elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
#     set(MAKE_LUA_COMMAND make macosx)
# endif()


set(SOL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} PARENT_SCOPE)

# if (NOT USEJIT)
#     MESSAGE(STATUS "Use Lua")
#     add_custom_target(lua 
#                       COMMAND ${MAKE_LUA_COMMAND}
#                       WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lua)
#     set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lua/src)
#     set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lua/src PARENT_SCOPE)
#     set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/lua/src/liblua.a)
#     set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/lua/src/liblua.a PARENT_SCOPE)
# else ()
#     MESSAGE(STATUS "Use LuaJIT")
#     add_custom_target(lua 
#                     COMMAND make
#                     WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT)
#     set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT/src)
#     set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT/src PARENT_SCOPE)
#     set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT/src/libluajit.a)
#     set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT/src/libluajit.a PARENT_SCOPE)
# endif()

add_subdirectory(lua-cjson)
add_subdirectory(lua-cmsgpack)
add_subdirectory(lua-struct)