README for lua-cmsgpack.c
===

Lua-cmsgpack is a [MessagePack](http://msgpack.org) implementation and bindings for
Lua 5.1/5.2/5.3 in a self contained C file without external dependencies.

This library is open source software licensed under the BSD two-clause license.

INSTALLATION
---

Using LuaRocks (http://luarocks.org):

* Install current stable release:

    sudo luarocks install lua-cmsgpack

* Install current Git master head from GitHub:

    sudo luarocks install lua-cmsgpack --from=rocks-cvs

* Install from current working copy

    cd lua-cmsgpack/
    sudo luarocks make rockspec/lua-cmsgpack-scm-1.rockspec

If you embed Lua and all modules into your C project, just add the
`lua_cmsgpack.c` file and call the following function after creating the Lua
interpreter:

    luaopen_cmsgpack(L);

USAGE
---

The exported API is very simple, consisting of four functions:

Basic API:

    msgpack = cmsgpack.pack(lua_object1, lua_object2, ..., lua_objectN)
    lua_object1, lua_object2, ..., lua_objectN = cmsgpack.unpack(msgpack)

Detailed API giving you more control over unpacking multiple values:

    resume_offset, lua_object1 = cmsgpack.unpack_one(msgpack)
    resume_offset1, lua_object2 = cmsgpack.unpack_one(msgpack, resume_offset)
    ...
    -1, lua_objectN = cmsgpack.unpack_one(msgpack, resume_offset_previous)

    resume_offset, lua_object1, lua_object2 = cmsgpack.unpack_limit(msgpack, 2)
    resume_offset2, lua_object3 = cmsgpack.unpack_limit(msgpack, 1, resume_offset1)

Functions:

  - `pack(arg1, arg2, ..., argn)` - pack any number of lua objects into one msgpack stream.  returns: msgpack
  - `unpack(msgpack)` - unpack all objects in msgpack to individual return values. returns: object1, object2, ..., objectN
  - `unpack_one(msgpack); unpack_one(msgpack, offset)` - unpacks the first object after offset. returns: offset, object
  - `unpack_limit(msgpack, limit); unpack_limit(msgpack, limit, offset)` - unpacks the first `limit` objects and returns: offset, object1, objet2, ..., objectN (up to limit, but may return fewer than limit if not that many objects remain to be unpacked)

When you reach the end of your input stream with `unpack_one` or `unpack_limit`, an offset of `-1` is returned.

You may `require "msgpack"` or you may `require "msgpack.safe"`.  The safe version returns errors as (nil, errstring).

However because of the nature of Lua numerical and table type a few behavior
of the library must be well understood to avoid problems:

* A table is converted into a MessagePack array type only if *all* the keys are
composed of incrementing integers starting at 1 end ending at N, without holes,
without additional non numerical keys. All the other tables are converted into
maps.
* An empty table is always converted into a MessagePack array, the rationale is that empty lists are much more common than empty maps (usually used to represent objects with fields).
* A Lua number is converted into an integer type if floor(number) == number, otherwise it is converted into the MessagePack float or double value.
* When a Lua number is converted to float or double, the former is preferred if there is no loss of precision compared to the double representation.
* When a MessagePack big integer (64 bit) is converted to a Lua number it is possible that the resulting number will not represent the original number but just an approximation. This is unavoidable because the Lua numerical type is usually a double precision floating point type.

TESTING
---

Build and test:

    mkdir build; cd build
    cmake ..
    make
    lua ../test.lua

You can build a 32-bit module on a 64-bit platform with:

    mkdir build; cd build
    cmake -DBuild32Bit=ON ..
    make
    lua ../test.lua

NESTED TABLES
---
Nested tables are handled correctly up to `LUACMSGPACK_MAX_NESTING` levels of
nesting (that is set to 16 by default).
Every table that is nested at a greater level than the maxium is encoded
as MessagePack nil value.

It is worth to note that in Lua it is possible to create tables that mutually
refer to each other, creating a cycle. For example:

    a = {x=nil,y=5}
    b = {x=a}
    a['x'] = b

This condition will simply make the encoder reach the max level of nesting,
thus avoiding an infinite loop.

CREDITS
---

This library was written by Salvatore Sanfilippo for Redis, but is maintained as a separated project by the author.

Some of the test vectors in "test.lua" are obtained from the Javascript [MessagePack-JS library](https://github.com/cuzic/MessagePack-JS).
