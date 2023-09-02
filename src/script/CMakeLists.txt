# if define USEJIT, we use LuaJIT, otherwise we use Lua
SET(USEJIT OFF)
MESSAGE(STATUS "USEJIT: ${USEJIT}")
if (NOT USEJIT)
    MESSAGE(STATUS "Use Lua")
    add_custom_target(lua 
                      COMMAND make lua
                      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lua)
    set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lua)
    set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lua PARENT_SCOPE)
    set(SOL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} PARENT_SCOPE)
    set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/lua/liblua.a)
    set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/lua/liblua.a PARENT_SCOPE)
else ()
    MESSAGE(STATUS "Use LuaJIT")
    add_custom_target(lua 
                    COMMAND make
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT)
    set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT/src)
    set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT/src PARENT_SCOPE)
    set(SOL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} PARENT_SCOPE)
    set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT/src/libluajit.a)
    set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/LuaJIT/src/libluajit.a PARENT_SCOPE)
endif()

add_subdirectory(lua-cjson)
add_subdirectory(lua-cmsgpack)
add_subdirectory(lua-struct)