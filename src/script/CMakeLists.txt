add_custom_target(lua 
                  COMMAND make lua
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lua)
set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lua)
set(LUA_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lua PARENT_SCOPE)
set(SOL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR} PARENT_SCOPE)
set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/lua/liblua.a)
set(LUA_LIBRARY ${CMAKE_CURRENT_SOURCE_DIR}/lua/liblua.a PARENT_SCOPE)

add_subdirectory(lua-cjson)
add_subdirectory(lua-cmsgpack)
add_subdirectory(lua-struct)