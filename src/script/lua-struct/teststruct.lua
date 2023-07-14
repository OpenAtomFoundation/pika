#!/usr/bin/env lua5.1

-- $Id: teststruct.lua,v 1.8 2018/05/16 11:03:52 roberto Exp $

-- load library
local lib = require"struct"


-- Lua 5.1 x Lua 5.2
local unpack = unpack or table.unpack

--
-- auxiliar function to print an hexadecimal `dump' of a given string
-- (not used by the test)
--
local function bp (s)
  s = string.gsub(s, "(.)", function(c)
        return string.format("\\%02x", string.byte(c))
      end)
  print(s)
end


local a,b,c,d,e,f,x

-- assume sizeof(int) == 4
assert(#lib.pack("I4", 67324752) == 4 and lib.size("I4", 4) == 4)

assert(lib.size('bbb') == 3)
assert(lib.pack('b', 10) == string.char(10))
assert(lib.pack('bbb', 10, 20, 30) == string.char(10, 20, 30))

assert(lib.size('h') == 2)  -- assume sizeof(short) == 2
assert(lib.pack('<h', 10) == string.char(10, 0))
assert(lib.pack('>h', 10) == string.char(0, 10))
assert(lib.pack('<h', -10) == string.char(256-10, 256-1))

x = lib.size('l') - 1
assert(lib.pack('<l', 10) == string.char(10) .. string.char(0):rep(x))
assert(lib.pack('>l', 10) == string.char(0):rep(x) .. string.char(10))
assert(lib.pack('<l', -10) == string.char(256-10) .. string.char(255):rep(x))

assert(lib.unpack('<h', string.char(10, 0)) == 10)
assert(lib.unpack('>h', string.char(0, 10)) == 10)
assert(lib.unpack('<h', string.char(256-10, 256-1)) == -10)

assert(lib.unpack('<i4', string.char(10, 0, 0, 1)) == 10 + 2^(3*8))
assert(lib.unpack('>i4', string.char(0, 1, 0, 10)) == 10 + 2^(2*8))
assert(lib.unpack('<i4', string.char(256-10, 256-1, 256-1, 256-1)) == -10)

assert(lib.size("<lihT") == lib.size(">LIHT"))
assert(lib.size("!4bi") > lib.size("!1bi"))

x = lib.size("T") - 1
assert(lib.pack('<T', 10) == string.char(10) .. string.char(0):rep(x))
assert(lib.pack('>T', 10) == string.char(0):rep(x) .. string.char(10))
assert(lib.pack('<T', -10) == string.char(256-10) .. string.char(255):rep(x))
  

-- minimum limits
lims = {{'B', 255}, {'b', 127}, {'b', -128},
        {'I1', 255}, {'i1', 127}, {'i1', -128},
        {'H', 2^16 - 1}, {'h', 2^15 - 1}, {'h', -2^15},
        {'I2', 2^16 - 1}, {'i2', 2^15 - 1}, {'i2', -2^15},
        {'L', 2^32 - 1}, {'l', 2^31 - 1}, {'l', -2^31},
        {'I4', 2^32 - 1}, {'i4', 2^31 - 1}, {'i4', -2^31},
       }

for _, a in pairs{'', '>', '<'} do
  for _, l in pairs(lims) do
    local fmt = a .. l[1]
    assert(lib.unpack(fmt, lib.pack(fmt, l[2])) == l[2])
  end
end


-- tests for fixed-sized ints
for _, i in pairs{1,2,4} do
  x = lib.pack('<i'..i, -3)
  assert(string.len(x) == i)
  assert(x == string.char(256-3) .. string.rep(string.char(256-1), i-1))
  assert(lib.unpack('<i'..i, x) == -3)
end


-- alignment
d = lib.pack("d", 5.1)
ali = {[1] = string.char(1)..d,
       [2] = string.char(1, 0)..d,
       [4] = string.char(1, 0, 0, 0)..d,
       [8] = string.char(1, 0, 0, 0, 0, 0, 0, 0)..d,
      }

for a,r in pairs(ali) do
  assert(lib.pack("!"..a.."bd", 1, 5.1) == r)
  local x,y = lib.unpack("!"..a.."bd", r)
  assert(x == 1 and y == 5.1)
end


print('+')

-- tests for non-power-of-two sizes
assert(lib.pack("<i3", 10) == string.char(10, 0, 0))
assert(lib.pack("<I3", -10) == string.char(256 - 10, 255, 255))
assert(lib.unpack("<i3", string.char(10, 0, 0)) == 10)
assert(lib.unpack(">I3", string.char(255, 255, 256 - 21)) == 2^(3*8) - 21)

-- tests for long long
if lib.unpack("i8", string.rep("\255", 8)) ~= -1 then
  print("no support for 'long long'")
else
  local lim = 800
  assert(lib.pack(">i8", 2^52) == "\0\16\0\0\0\0\0\0")
  local t = {}; for i = 1, lim do t[i] = 2^52 end
  assert(lib.pack(">" .. string.rep("i8", lim), unpack(t, 1, lim))
                  == string.rep("\0\16\0\0\0\0\0\0", lim))
  assert(lib.pack("<i8", 2^52 - 1) == "\255\255\255\255\255\255\15\0")
  assert(lib.pack(">i8", -2^52 - 1) == "\255\239\255\255\255\255\255\255")
  assert(lib.pack("<i8", -2^52 - 1) == "\255\255\255\255\255\255\239\255")

  assert(lib.unpack(">i8", "\255\239\255\255\255\255\255\255") == -2^52 - 1)
  assert(lib.unpack("<i8", "\255\255\255\255\255\255\239\255") == -2^52 - 1)
  assert(lib.unpack("<i8", "\255\255\254\255\255\255\015\000") ==
                            2^52 - 1 - 2^16)
  assert(lib.unpack(">i8", "\000\015\255\255\255\255\254\254") == 2^52 - 258)

  local fmt = ">" .. string.rep("i16", lim)
  local t1 = {lib.unpack(fmt, lib.pack(fmt, unpack(t)))}
  assert(t1[#t1] == 16*lim + 1  and #t == #t1 - 1)
  for i = 1, lim do assert(t[i] == t1[i]) end
  print'+'
end

-- strings
assert(lib.pack("c", "alo alo") == "a")
assert(lib.pack("c4", "alo alo") == "alo ")
assert(lib.pack("c5", "alo alo") == "alo a")
assert(lib.pack("!4b>c7", 1, "alo alo") == "\1alo alo")
assert(lib.pack("!2<s", "alo alo") == "alo alo\0")
assert(lib.pack(" c0 ", "alo alo") == "alo alo")
for _, f in pairs{"B", "l", "i2", "f", "d"} do
  for _, s in pairs{"", "a", "alo", string.rep("x", 200)} do
    local x = lib.pack(f.."c0", #s, s)
    assert(lib.unpack(f.."c0", x) == s)
  end
end

-- indices
x = lib.pack("!>iiiii", 1, 2, 3, 4, 5)
local i = 1
local k = 1
while i < #x do
  local v, j = lib.unpack("!>i", x, i)
  assert(j == i + 4 and v == k)
  i = j; k = k + 1
end

-- alignments are relative to 'absolute' positions
x = lib.pack("!8 xd", 12)
assert(lib.unpack("!8d", x, 3) == 12)


a,b,c,d = lib.unpack("<lhbxxH",
                     lib.pack("<lhbxxH", -2, 10, -10, 250))
assert(a == -2 and b == 10 and c == -10 and d == 250)


a, b, c, d = lib.unpack(">lBxxH", lib.pack(">lBxxH", -20, 10, 250))
assert(a == -20 and b == 10 and c == 250 and d == lib.size(">lBxxH") + 1)

a,b,c,d,e = lib.unpack(">fdfH",
                  '000'..lib.pack(">fdfH", 3.5, -24e-5, 200.5, 30000),
                  4)
assert(a == 3.5 and b == -24e-5 and c == 200.5 and d == 30000 and e == 22)

a,b,c,d,e = lib.unpack("<fdxxfH",
                  '000'..lib.pack("<fdxxfH", -13.5, 24e5, 200.5, 300),
                  4)
assert(a == -13.5 and b == 24e5 and c == 200.5 and d == 300 and e == 24)

x = lib.pack(" > I2 f i4 I2 ", 10, 20, -30, 40001)
assert(string.len(x) == 2 + lib.size("f") + 4 + 2)
assert(lib.unpack(">f", x, 3) == 20)
a,b,c,d = lib.unpack(">i2fi4I2", x)
assert(a == 10 and b == 20 and c == -30 and d == 40001)

local s = "hello hello"
x = lib.pack(" b c0 ", string.len(s), s)
assert(lib.unpack("bc0", x) == s)
x = lib.pack("Lc0", string.len(s), s)
assert(lib.unpack("  L  c0   ", x) == s)
x = lib.pack("cc3b", s, s, 0)
assert(x == "hhel\0")
assert(lib.unpack("xxxxb", x) == 0)

assert(lib.pack("<!8i4", 3) == string.char(3, 0, 0, 0))
assert(lib.pack("<!8xi4", 3) == string.char(0, 0, 0, 0, 3, 0, 0, 0))
assert(lib.pack("<!8xxi4", 3) == string.char(0, 0, 0, 0, 3, 0, 0, 0))
assert(lib.pack("<!8xxxi4", 3) == string.char(0, 0, 0, 0, 3, 0, 0, 0))

assert(lib.unpack("<!4i4", string.char(3, 0, 0, 0)) == 3)
assert(lib.unpack("<!4xi4", string.char(0, 0, 0, 0, 3, 0, 0, 0)) == 3)
assert(lib.unpack("<!4xxi4", string.char(0, 0, 0, 0, 3, 0, 0, 0)) == 3)
assert(lib.unpack("<!4xxxi4", string.char(0, 0, 0, 0, 3, 0, 0, 0)) == 3)

assert(lib.pack("<!2 b i4 h", 2, 3, 5) == string.char(2, 0, 3, 0, 0, 0, 5, 0))
a,b,c = lib.unpack("<!2bi4h", string.char(2, 0, 3, 0, 0, 0, 5, 0))
assert(a == 2 and b == 3 and c == 5)

assert(lib.pack("<!8bi4h", 2, 3, 5) ==
         string.char(2, 0, 0, 0, 3, 0, 0, 0, 5, 0))
a,b,c = lib.unpack("<!8bi4h", string.char(2, 0, 0, 0, 3, 0, 0, 0, 5, 0))
assert(a == 2 and b == 3 and c == 5)

assert(lib.pack(">sh", "aloi", 3) == "aloi\0\0\3")
assert(lib.pack(">!sh", "aloi", 3) == "aloi\0\0\0\3")
x = "aloi\0\0\0\0\3\2\0\0"
a, b, c = lib.unpack("<!si4", x)
assert(a == "aloi" and b == 2*256+3 and c == string.len(x)+1)

x = lib.pack("!4sss", "hi", "hello", "bye")
a,b,c = lib.unpack("sss", x)
assert(a == "hi" and b == "hello" and c == "bye")
a, i = lib.unpack("s", x, 1)
assert(a == "hi")
a, i = lib.unpack("s", x, i)
assert(a == "hello")
a, i = lib.unpack("s", x, i)
assert(a == "bye")


-- bug in 0.2  (arithmetic overflow with a "negative" size for 'c0')
assert(not pcall(lib.unpack, 'bc0', '\255'))

-- bug in 0.2  (arithmetic overflow with a negative initial position)
assert(not pcall(lib.unpack, 'i', 'ffffffff', -10))

-- no returns
do
  assert(lib.pack('>!') == '')
  local p, x = lib.unpack('>!', '')
  assert(p == 1 and x == nil)    -- return only the new position
end

-- test for weird conditions
assert(lib.pack(">>>h <!!!<h", 10, 10) == string.char(0, 10, 10, 0))
assert(not pcall(lib.pack, "!3l", 10))
assert(not pcall(lib.pack, "3", 10))
assert(lib.pack("") == "")
assert(lib.pack("   ") == "")
assert(lib.pack(">>><<<!!") == "")
assert(not pcall(lib.unpack, "c0", "alo"))
assert(not pcall(lib.unpack, "c0", "alo", 2))
assert(not pcall(lib.unpack, "s", "alo"))
assert(lib.unpack("s", "alo\0") == "alo")
assert(not pcall(lib.pack, "c4", "alo"))
assert(lib.pack("c3", "alo") == "alo")
assert(not pcall(lib.unpack, "c4", "alo"))
assert(lib.unpack("c3", "alo") == "alo")
assert(not pcall(lib.unpack, "bc0", "\4alo"))
assert(lib.unpack("bc0", "\3alo") == "alo")
assert(not pcall(lib.size, "bbc0"))
assert(not pcall(lib.size, "s"))

assert(not pcall(lib.unpack, "b", "alo", 4))
assert(lib.unpack("b", "alo\3", 4) == 3)


-- tests for large numbers
assert(lib.pack(">i8", 1000) == string.char(0, 0, 0, 0, 0, 0, 3, 232))
assert(lib.pack("<i8", 5000) == string.char(136, 19, 0, 0, 0, 0, 0, 0))
assert(lib.pack("<i32", 5001) ==
       string.char(137, 19) .. string.rep('\0', 30))
assert(lib.pack(">i32", 10000000) ==
       string.rep('\0', 29) .. string.char(0x98, 0x96, 0x80))

print'OK'

