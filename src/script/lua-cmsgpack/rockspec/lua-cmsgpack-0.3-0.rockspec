package = "lua-cmsgpack"
version = "0.3-0"
source = {
   url = "git://github.com/antirez/lua-cmsgpack.git",
   branch = "0.3.0"
}
description = {
   summary = "MessagePack C implementation and bindings for Lua 5.1",
   homepage = "http://github.com/antirez/lua-cmsgpack",
   license = "Two-clause BSD",
   maintainer = "Salvatore Sanfilippo <antirez@gmail.com>"
}
dependencies = {
   "lua >= 5.1"
}
build = {
   type = "builtin",
   modules = {
      cmsgpack = {
         sources = {
            "lua_cmsgpack.c",
         }
      }
   }
}
