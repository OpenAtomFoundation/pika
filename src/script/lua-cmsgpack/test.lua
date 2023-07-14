-- lua_cmsgpack.c lib tests
-- Copyright(C) 2012 Salvatore Sanfilippo, All Rights Reserved.
-- See the copyright notice at the end of lua_cmsgpack.c for more information.

local cmsgpack = require "cmsgpack"
local ok, cmsgpack_safe = pcall(require, 'cmsgpack.safe')
if not ok then cmsgpack_safe = nil end

print("------------------------------------")
print("Lua version: " .. (_G.jit and _G.jit.version or _G._VERSION))
print("------------------------------------")

local unpack = unpack or table.unpack

passed = 0
failed = 0
skipped = 0

function hex(s)
    local i
    local h = ""

    for i = 1, #s do
        h = h .. string.format("%02x",string.byte(s,i))
    end
    return h
end

function ascii_to_num(c)
    if (c >= string.byte("0") and c <= string.byte("9")) then
        return c - string.byte("0")
    elseif (c >= string.byte("A") and c <= string.byte("F")) then
        return (c - string.byte("A"))+10
    elseif (c >= string.byte("a") and c <= string.byte("f")) then
        return (c - string.byte("a"))+10
    else
        error "Wrong input for ascii to num convertion."
    end
end

function unhex(h)
    local i
    local s = ""
    for i = 1, #h, 2 do
        high = ascii_to_num(string.byte(h,i))
        low = ascii_to_num(string.byte(h,i+1))
        s = s .. string.char((high*16)+low)
    end
    return s
end

function test_error(name, fn)
    io.write("Testing generate error '",name,"' ...")
    local ok, ret, err = pcall(fn)
    -- 'ok' is an error because we are testing for expicit *failure*
    if ok then
        print("ERROR: result ", ret, err)
        failed = failed+1
    else
        print("ok")
        passed = passed+1
    end
end

local function test_multiple(name, ...)
    io.write("Multiple test '",name,"' ...")
    if not compare_objects({...},{cmsgpack.unpack(cmsgpack.pack(...))}) then
        print("ERROR:", {...}, cmsgpack.unpack(cmsgpack.pack(...)))
        failed = failed+1
    else
        print("ok")
        passed = passed+1
    end
end

function test_noerror(name, fn)
    io.write("Testing safe calling '",name,"' ...")
    if not cmsgpack_safe then
        print("skip: no `cmsgpack.safe` module")
        skipped = skipped + 1
        return
    end
    local ok, ret, err = pcall(fn)
    if not ok then
        print("ERROR: result ", ret, err)
        failed = failed+1
    else
        print("ok")
        passed = passed+1
    end
end

function compare_objects(a,b,depth)
    if (type(a) == "table") then
        local count = 0
        if not depth then
            depth = 1
        elseif depth == 10 then
            return true  -- assume if match down 10 levels, the rest is okay too
        end
        for k,v in pairs(a) do
            if not compare_objects(b[k],v, depth + 1) then return false end
            count = count + 1
        end
        -- All the 'a' keys are equal to their 'b' equivalents.
        -- Now we can check if there are extra fields in 'b'.
        for k,v in pairs(b) do count = count - 1 end
        if count == 0 then return true else return false end
    else
        return a == b
    end
end

function test_circular(name,obj)
    io.write("Circular test '",name,"' ...")
    if not compare_objects(obj,cmsgpack.unpack(cmsgpack.pack(obj))) then
        print("ERROR:", obj, cmsgpack.unpack(cmsgpack.pack(obj)))
        failed = failed+1
    else
        print("ok")
        passed = passed+1
    end
end

function test_stream(mod, name, ...)
    io.write("Stream test '", name, "' ...\n")
    if not mod then
        print("skip: no `cmsgpack.safe` module")
        skipped = skipped + 1
        return
    end
    local argc = select('#', ...)
    for i=1, argc do
        test_circular(name, select(i, ...))
    end
    local ret = {mod.unpack(mod.pack(unpack({...})))}
    for i=1, argc do
        local origin = select(i, ...)
        if (type(origin) == "table") then
            for k,v in pairs(origin) do
                local fail = not compare_objects(v, ret[i][k])
                if fail then
                    print("ERRORa:", k, v, " not match ", ret[i][k])
                    failed = failed + 1
                elseif not fail then
                    print("ok; matched stream table member")
                    passed = passed + 1
                end
            end
        else
            local fail = not compare_objects(origin, ret[i])
            if fail then
                print("ERRORc:", origin, " not match ", ret[i])
                failed = failed + 1
            elseif not fail then
                print("ok; matched individual stream member")
                passed = passed + 1
            end
        end
    end

end

function test_partial_unpack(name, count, ...)
    io.write("Testing partial unpack '",name,"' ...\n")
    local first = select(1, ...)
    local pack, unpacked, args, offset, cargs, ok, err
    if (type(first) == "table") then
        pack = first.p
        args = first.remaining
        offset = first.o
        cargs = {pack, count, offset}
    else
        pack = cmsgpack.pack(unpack({...}))
        args = {...}
        cargs = {pack, count}
    end
    if offset and offset < 0 then
        ok, unpacked, err = pcall(function()return {cmsgpack.unpack_limit(unpack(cargs))} end)
        if not ok then
            print("ok; received error as expected") --, unpacked)
            passed = passed + 1
            return
        end
    else
        unpacked = {cmsgpack.unpack_limit(unpack(cargs))}
        -- print ("GOT RETURNED:", unpack(unpacked))
    end

    if count == 0 and #unpacked == 1 then
        print("ok; received zero decodes as expected")
        passed = passed + 1
        return
    end

    if not (((#unpacked)-1) == count) then
        print(string.format("ERROR: received %d instead of %d objects:", (#unpacked)-1, count),
            unpack(select(1, unpacked)))
        failed = failed + 1
        return
    end

    for i=2, #unpacked do
        local origin = args[i-1]
        --print("Comparing ", origin, unpacked[i])
        if not compare_objects(origin, unpacked[i]) then
            print("ERROR:", origin, " not match ", unpacked[i])
            failed = failed + 1
        else
            print("ok; matched unpacked value to input")
            passed = passed + 1
        end
    end

    -- return the packed value and our continue offset
    return pack, unpacked[1]
end

function test_pack(name,obj,raw,optraw)
    io.write("Testing encoder '",name,"' ...")
    local result = hex(cmsgpack.pack(obj))
    if optraw and (result == optraw) then
        print("ok")
        passed = passed + 1
    elseif result ~= raw then
        print("ERROR:", obj, hex(cmsgpack.pack(obj)), raw)
        failed = failed+1
    else
        print("ok")
        passed = passed+1
    end
end

function test_unpack_one(name, packed, check, offset)
    io.write("Testing one unpack '",name,"' ...")
    local unpacked = {cmsgpack.unpack_one(unpack({packed, offset}))}

    if #unpacked > 2 then
        print("ERROR: unpacked more than one object:", unpack(unpacked))
        failed = failed + 1
    elseif not compare_objects(unpacked[2], check) then
        print("ERROR: unpacked unexpected result:", unpack(unpacked))
        failed = failed + 1
    else
        print("ok") --; unpacked", unpacked[2])
        passed = passed + 1
    end

    return unpacked[1]
end

function test_unpack(name,raw,obj)
    io.write("Testing decoder '",name,"' ...")
    if not compare_objects(cmsgpack.unpack(unhex(raw)),obj) then
        print("ERROR:", obj, raw, cmsgpack.unpack(unhex(raw)))
        failed = failed+1
    else
        print("ok")
        passed = passed+1
    end
end

function test_pack_and_unpack(name,obj,raw)
    test_pack(name,obj,raw)
    test_unpack(name,raw,obj)
end

local function test_global()
    io.write("Testing global variable ...")

    if _VERSION == "Lua 5.1" then
        if not _G.cmsgpack then
            print("ERROR: Lua 5.1 should set global")
            failed = failed+1
        else
            print("ok")
            passed = passed+1
        end
    else
        if _G.cmsgpack then
            print("ERROR: Lua 5.2 should not set global")
            failed = failed+1
        else
            print("ok")
            passed = passed+1
        end
    end
end

local function test_array()
    io.write("Testing array detection ...")

    local a = {a1 = 1, a2 = 1, a3 = 1, a4 = 1, a5 = 1, a6 = 1, a7 = 1, a8 = 1, a9 = 1}
    a[1] = 10 a[2] = 20 a[3] = 30
    a.a1,a.a2,a.a3,a.a4,a.a5,a.a6,a.a7,a.a8, a.a9 = nil

    local test_obj = {10,20,30}
    assert(compare_objects(test_obj, a))

    local etalon = cmsgpack.pack(test_obj)
    local encode = cmsgpack.pack(a)

    if etalon ~= encode then
        print("ERROR:")
        print("", "expected: ", hex(etalon))
        print("", "     got: ", hex(encode))
        failed = failed+1
    else
        print("ok")
        passed = passed+1
    end

    io.write("Testing array detection ...")

    a = {["1"] = 20, [2] = 30, [3] = 40}
    encode = cmsgpack.pack(a)
    if etalon == encode then
        print("ERROR:")
        print("", " incorrect: ", hex(etalon))
        failed = failed+1
    else
        print("ok")
        passed = passed+1
    end
end

test_global()
test_array()
test_circular("positive fixnum",17);
test_circular("negative fixnum",-1);
test_circular("true boolean",true);
test_circular("false boolean",false);
test_circular("float",1.5);
test_circular("positive uint8",101);
test_circular("negative int8",-101);
test_circular("positive uint16",20001);
test_circular("negative int16",-20001);
test_circular("positive uint32",20000001);
test_circular("negative int32",-20000001);
test_circular("positive uint64",200000000001);
test_circular("negative int64",-200000000001);
test_circular("uint8 max",0xff);
test_circular("uint16 max",0xffff);
test_circular("uint32 max",0xffffffff);
test_circular("int8 min",-128);
test_circular("int16 min",-32768);
test_circular("int32 min",-2147483648);
test_circular("nil",nil);
test_circular("fix string","abc");
test_circular("string16","xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab");
test_circular("fix array (1)",{1,2,3,"foo"})
test_circular("fix array (2)",{})
test_circular("fix array (3)",{1,{},{}})
test_circular("fix map",{a=5,b=10,c="string"})
test_circular("positive infinity", math.huge)
test_circular("negative infinity", -math.huge)
test_circular("high bits", 0xFFFFFFFF)
test_circular("higher bits", 0xFFFFFFFFFFFFFFFF)
test_circular("high bits", -0x7FFFFFFF)
test_circular("higher bits", -0x7FFFFFFFFFFFFFFF)

-- The following test vectors are taken from the Javascript lib at:
-- https://github.com/cuzic/MessagePack-JS/blob/master/test/test_pack.html

test_pack_and_unpack("positive fixnum",0,"00")
test_pack_and_unpack("negative fixnum",-1,"ff")
test_pack_and_unpack("uint8",255,"ccff")
test_pack_and_unpack("fix raw","a","a161")
test_pack_and_unpack("fix array",{0},"9100")
test_pack_and_unpack("fix map",{a=64},"81a16140")
test_pack_and_unpack("nil",nil,"c0")
test_pack_and_unpack("true",true,"c3")
test_pack_and_unpack("false",false,"c2")
test_pack_and_unpack("double",0.1,"cb3fb999999999999a")
test_pack_and_unpack("uint16",32768,"cd8000")
test_pack_and_unpack("uint32",1048576,"ce00100000")
test_pack_and_unpack("int8",-64,"d0c0")
test_pack_and_unpack("int16",-1024,"d1fc00")
test_pack_and_unpack("int32",-1048576,"d2fff00000")
test_pack_and_unpack("int64",-1099511627776,"d3ffffff0000000000")
test_pack_and_unpack("raw8","                                 ","d921202020202020202020202020202020202020202020202020202020202020202020")
test_pack_and_unpack("raw16","                                                                                                                                                                                                                                                                 ","da01012020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020")
test_pack_and_unpack("array 16",{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0},"dc001000000000000000000000000000000000")

-- Regression test for issue #4, cyclic references in tables.
a = {x=nil,y=5}
b = {x=a}
a['x'] = b
pack = cmsgpack.pack(a)
-- Note: the generated result isn't stable because the order of traversal for
-- a table isn't defined. So far we've only noticed two serializations of a
-- (and the second serialization only happens on Lua 5.3 sometimes)
test_pack("regression for issue #4 output matching",a,"82a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a17882a17905a17881a178c0", "82a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a178c0a17905a17905a17905a17905a17905a17905a17905a17905")
test_circular("regression for issue #4 circular",a)

-- test unpacking malformed input without crashing.  This actually returns one integer value (the ASCII code)
-- for each character in the string.  We don't care about the return value, just that we don't segfault.
cmsgpack.unpack("82a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a17882a17881a17")

-- Test unpacking input which may cause overflow memory access ("-1" for 32-bit size fields).
-- These should cause a Lua error but not a segfault.
test_error("unpack big string with missing input", function() cmsgpack.unpack("\219\255\255\255\255Z") end)
test_error("unpack big array with missing input", function() cmsgpack.unpack("\221\255\255\255\255Z") end)
test_error("unpack big map with missing input", function() cmsgpack.unpack("\223\255\255\255\255Z") end)

-- Tests from github.com/moteus
test_circular("map with number keys", {[1] = {1,2,3}})
test_circular("map with string keys", {["1"] = {1,2,3}})
test_circular("map with string keys", {["1"] = 20, [2] = 30, ["3"] = 40})
test_circular("map with float keys", {[1.5] = {1,2,3}})
test_error("unpack nil", function() cmsgpack.unpack(nil) end)
test_error("unpack table", function() cmsgpack.unpack({}) end)
test_error("unpack udata", function() cmsgpack.unpack(io.stdout) end)
test_noerror("unpack nil", function() cmsgpack_safe.unpack(nil) end)
test_noerror("unpack nil", function() cmsgpack_safe.unpack(nil) end)
test_noerror("unpack table", function() cmsgpack_safe.unpack({}) end)
test_noerror("unpack udata", function() cmsgpack_safe.unpack(io.stdout) end)
test_multiple("two ints", 1, 2)
test_multiple("holes", 1, nil, 2, nil, 4)

-- Streaming/Multi-Input Tests
test_stream(cmsgpack, "simple", {a=1}, {b=2}, {c=3}, 4, 5, 6, 7)
test_stream(cmsgpack_safe, "safe simple", {a=1}, {b=2}, {c=3}, 4, 5, 6, 7)
test_stream(cmsgpack, "oddities", {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, {0}, {a=64}, math.huge, -math.huge)
test_stream(cmsgpack_safe, "safe oddities", {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}, {0}, {a=64}, math.huge, -math.huge)
test_stream(cmsgpack, "strange things", nil, {}, {nil}, a, b, b, b, a, a, b, {c = a, d = b})
test_stream(cmsgpack_safe, "strange things", nil, {}, {nil}, a, b, b, b, a, a, b, {c = a, d = b})
test_error("pack nothing", function() cmsgpack.pack() end)
test_noerror("pack nothing safe", function() cmsgpack_safe.pack() end)
test_circular("large object test",
    {A=9483, a=9483, aa=9483, aal=9483, aalii=9483, aam=9483, Aani=9483,
    aardvark=9483, aardwolf=9483, Aaron=9483, Aaronic=9483, Aaronical=9483,
    Aaronite=9483, Aaronitic=9483, Aaru=9483, Ab=9483, aba=9483, Ababdeh=9483,
    Ababua=9483, abac=9483, abaca=9483, abacate=9483, abacay=9483,
    abacinate=9483, abacination=9483, abaciscus=9483, abacist=9483,
    aback=9483, abactinal=9483, abactinally=9483, abaction=9483, abactor=9483,
    abaculus=9483, abacus=9483, Abadite=9483, abaff=9483, abaft=9483,
    abaisance=9483, abaiser=9483, abaissed=9483, abalienate=9483,
    abalienation=9483, abalone=9483, Abama=9483, abampere=9483, abandon=9483,
    abandonable=9483, abandoned=9483, abandonedly=9483, abandonee=9483,
    abandoner=9483, abandonment=9483, Abanic=9483, Abantes=9483,
    abaptiston=9483, Abarambo=9483, Abaris=9483, abarthrosis=9483,
    abarticular=9483, abarticulation=9483, abas=9483, abase=9483, abased=9483,
    abasedly=9483, abasedness=9483, abasement=9483, abaser=9483, Abasgi=9483,
    abash=9483, abashed=9483, abashedly=9483, abashedness=9483,
    abashless=9483, abashlessly=9483, abashment=9483, abasia=9483,
    abasic=9483, abask=9483, Abassin=9483, abastardize=9483, abatable=9483,
    abate=9483, abatement=9483, abater=9483, abatis=9483, abatised=9483,
    abaton=9483, abator=9483, abattoir=9483, Abatua=9483, abature=9483,
    abave=9483, abaxial=9483, abaxile=9483, abaze=9483, abb=9483, Abba=9483,
    abbacomes=9483, abbacy=9483, Abbadide=9483, abbas=9483, abbasi=9483,
    abbassi=9483, Abbasside=9483, abbatial=9483, abbatical=9483, abbess=9483,
    abbey=9483, abbeystede=9483, Abbie=9483, abbot=9483, abbotcy=9483,
    abbotnullius=9483, abbotship=9483, abbreviate=9483, abbreviately=9483,
    abbreviation=9483, abbreviator=9483, abbreviatory=9483, abbreviature=9483,
    Abby=9483, abcoulomb=9483, abdal=9483, abdat=9483, Abderian=9483,
    Abderite=9483, abdest=9483, abdicable=9483, abdicant=9483, abdicate=9483,
    abdication=9483, abdicative=9483, abdicator=9483, Abdiel=9483,
    abditive=9483, abditory=9483, abdomen=9483, abdominal=9483,
    Abdominales=9483, abdominalian=9483, abdominally=9483,
    abdominoanterior=9483, abdominocardiac=9483, abdominocentesis=9483,
    abdominocystic=9483, abdominogenital=9483, abdominohysterectomy=9483,
    abdominohysterotomy=9483, abdominoposterior=9483, abdominoscope=9483,
    abdominoscopy=9483, abdominothoracic=9483, abdominous=9483,
    abdominovaginal=9483, abdominovesical=9483, abduce=9483, abducens=9483,
    abducent=9483, abduct=9483, abduction=9483, abductor=9483, Abe=9483,
    abeam=9483, abear=9483, abearance=9483, abecedarian=9483,
    abecedarium=9483, abecedary=9483, abed=9483, abeigh=9483, Abel=9483,
    abele=9483, Abelia=9483, Abelian=9483, Abelicea=9483, Abelite=9483,
    abelite=9483, Abelmoschus=9483, abelmosk=9483, Abelonian=9483,
    abeltree=9483, Abencerrages=9483, abenteric=9483, abepithymia=9483,
    Aberdeen=9483, aberdevine=9483, Aberdonian=9483, Aberia=9483,
    aberrance=9483, aberrancy=9483, aberrant=9483, aberrate=9483,
    aberration=9483, aberrational=9483, aberrator=9483, aberrometer=9483,
    aberroscope=9483, aberuncator=9483, abet=9483, abetment=9483,
    abettal=9483, abettor=9483, abevacuation=9483, abey=9483, abeyance=9483,
    abeyancy=9483, abeyant=9483, abfarad=9483, abhenry=9483, abhiseka=9483,
    abhominable=9483, abhor=9483, abhorrence=9483, abhorrency=9483,
    abhorrent=9483, abhorrently=9483, abhorrer=9483, abhorrible=9483,
    abhorring=9483, Abhorson=9483, abidal=9483, abidance=9483, abide=9483,
    abider=9483, abidi=9483, abiding=9483, abidingly=9483, abidingness=9483,
    Abie=9483, Abies=9483, abietate=9483, abietene=9483, abietic=9483,
    abietin=9483, Abietineae=9483, abietineous=9483, abietinic=9483,
    Abiezer=9483, Abigail=9483, abigail=9483, abigailship=9483, abigeat=9483,
    abigeus=9483, abilao=9483, ability=9483, abilla=9483, abilo=9483,
    abintestate=9483, abiogenesis=9483, abiogenesist=9483, abiogenetic=9483,
    abiogenetical=9483, abiogenetically=9483, abiogenist=9483,
    abiogenous=9483, abiogeny=9483, abiological=9483, abiologically=9483,
    abiology=9483, abiosis=9483, abiotic=9483, abiotrophic=9483,
    abiotrophy=9483, Abipon=9483, abir=9483, abirritant=9483, abirritate=9483,
    abirritation=9483, abirritative=9483, abiston=9483, Abitibi=9483,
    abiuret=9483, abject=9483, abjectedness=9483, abjection=9483,
    abjective=9483, abjectly=9483, abjectness=9483, abjoint=9483,
    abjudge=9483, abjudicate=9483, abjudication=9483, abjunction=9483,
    abjunctive=9483, abjuration=9483, abjuratory=9483, abjure=9483,
    abjurement=9483, abjurer=9483, abkar=9483, abkari=9483, Abkhas=9483,
    Abkhasian=9483, ablach=9483, ablactate=9483, ablactation=9483,
    ablare=9483, ablastemic=9483, ablastous=9483, ablate=9483, ablation=9483,
    ablatitious=9483, ablatival=9483, ablative=9483, ablator=9483,
    ablaut=9483, ablaze=9483, able=9483, ableeze=9483, ablegate=9483,
    ableness=9483, ablepharia=9483, ablepharon=9483, ablepharous=9483,
    Ablepharus=9483, ablepsia=9483, ableptical=9483, ableptically=9483,
    abler=9483, ablest=9483, ablewhackets=9483, ablins=9483, abloom=9483,
    ablow=9483, ablude=9483, abluent=9483, ablush=9483, ablution=9483,
    ablutionary=9483, abluvion=9483, ably=9483, abmho=9483, Abnaki=9483,
    abnegate=9483, abnegation=9483, abnegative=9483, abnegator=9483,
    Abner=9483, abnerval=9483, abnet=9483, abneural=9483, abnormal=9483,
    abnormalism=9483, abnormalist=9483, abnormality=9483, abnormalize=9483,
    abnormally=9483, abnormalness=9483, abnormity=9483, abnormous=9483,
    abnumerable=9483, Abo=9483, aboard=9483, Abobra=9483, abode=9483,
    abodement=9483, abody=9483, abohm=9483, aboil=9483, abolish=9483,
    abolisher=9483, abolishment=9483, abolition=9483, abolitionary=9483,
    abolitionism=9483, abolitionist=9483, abolitionize=9483, abolla=9483,
    aboma=9483, abomasum=9483, abomasus=9483, abominable=9483,
    abominableness=9483, abominably=9483, abominate=9483, abomination=9483,
    abominator=9483, abomine=9483, Abongo=9483, aboon=9483, aborad=9483,
    aboral=9483, aborally=9483, abord=9483, aboriginal=9483,
    aboriginality=9483, aboriginally=9483, aboriginary=9483, aborigine=9483,
    abort=9483, aborted=9483, aborticide=9483, abortient=9483,
    abortifacient=9483, abortin=9483, abortion=9483, abortional=9483,
    abortionist=9483, abortive=9483, abortively=9483, abortiveness=9483,
    abortus=9483, abouchement=9483, abound=9483, abounder=9483,
    abounding=9483, aboundingly=9483, about=9483, abouts=9483, above=9483,
    aboveboard=9483, abovedeck=9483, aboveground=9483, aboveproof=9483,
    abovestairs=9483, abox=9483, abracadabra=9483, abrachia=9483,
    abradant=9483, abrade=9483, abrader=9483, Abraham=9483, Abrahamic=9483,
    Abrahamidae=9483, Abrahamite=9483, Abrahamitic=9483, abraid=9483,
    Abram=9483, Abramis=9483, abranchial=9483, abranchialism=9483,
    abranchian=9483, Abranchiata=9483, abranchiate=9483, abranchious=9483,
    abrasax=9483, abrase=9483, abrash=9483, abrasiometer=9483, abrasion=9483,
    abrasive=9483, abrastol=9483, abraum=9483, abraxas=9483, abreact=9483,
    abreaction=9483, abreast=9483, abrenounce=9483, abret=9483, abrico=9483,
    abridge=9483, abridgeable=9483, abridged=9483, abridgedly=9483,
    abridger=9483, abridgment=9483, abrim=9483, abrin=9483, abristle=9483,
    abroach=9483, abroad=9483, Abrocoma=9483, abrocome=9483, abrogable=9483,
    abrogate=9483, abrogation=9483, abrogative=9483, abrogator=9483,
    Abroma=9483, Abronia=9483, abrook=9483, abrotanum=9483, abrotine=9483,
    abrupt=9483, abruptedly=9483, abruption=9483, abruptly=9483,
    abruptness=9483, Abrus=9483, Absalom=9483, absampere=9483, Absaroka=9483,
    absarokite=9483, abscess=9483, abscessed=9483, abscession=9483,
    abscessroot=9483, abscind=9483, abscise=9483, abscision=9483,
    absciss=9483, abscissa=9483, abscissae=9483, abscisse=9483,
    abscission=9483, absconce=9483, abscond=9483, absconded=9483,
    abscondedly=9483, abscondence=9483})

-- Test limited streaming
packed, offset = test_partial_unpack("unpack 1a out of 7", 1, "a", "b", "c", "d", "e", "f", "g")
packed, offset = test_partial_unpack("unpack 1b of remaining 7", 1, {p=packed,o=offset,remaining={"b"}})
packed, offset = test_partial_unpack("unpack 1c of remaining 7", 1, {p=packed,o=offset,remaining={"c"}})
packed, offset = test_partial_unpack("unpack 1d of remaining 7", 1, {p=packed,o=offset,remaining={"d"}})
packed, offset = test_partial_unpack("unpack 1e of remaining 7", 1, {p=packed,o=offset,remaining={"e"}})
packed, offset = test_partial_unpack("unpack 1f of remaining 7", 1, {p=packed,o=offset,remaining={"f"}})
packed, offset = test_partial_unpack("unpack 1g of remaining 7", 1, {p=packed,o=offset,remaining={"g"}})
packed, offset = test_partial_unpack("unpack 1nil of remaining 7", 0, {p=packed,o=offset})

packed, offset = test_partial_unpack("unpack 3 out of 7", 3, "a", "b", "c", "d", "e", "f", "g")
test_partial_unpack("unpack remaining 4", 4, {p=packed,o=offset,remaining={"d", "e", "f", "g"}})

test_unpack_one("simple", packed, "a")
offset = test_unpack_one("simple", cmsgpack.pack({f = 3, j = 2}, "m", "e", 7), {f = 3, j = 2})
test_unpack_one("simple", cmsgpack.pack({f = 3, j = 2}, "m", "e", 7), "m", offset)

-- Final report
print()
print("TEST PASSED:",passed)
print("TEST FAILED:",failed)
print("TEST SKIPPED:",skipped)

if failed > 0 then
   os.exit(1)
end
