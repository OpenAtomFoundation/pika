start_server {tags {"basic"}} {
    test {SET and GET an item} {
        r set x foobar
        r get x
    } {foobar}

    test {SET and GET an empty item} {
        r set x {}
        r get x
    } {}

    test {DEL against a single item} {
        r del x
        r get x
    } {}

    test {Vararg DEL} {
        r set foo1 a
        r set foo2 b
        r set foo3 c
        list [r del foo1 foo2 foo3 foo4] [r mget foo1 foo2 foo3]
    } {3 {{} {} {}}}

    test {DEL all keys} {
        foreach key {key_x key_y key_z foo_a foo_b foo_c} {
            r del $key
        }
    } {}

    test {Very big payload in GET/SET} {
        set buf [string repeat "abcd" 1000000]
        r set foo $buf
        r get foo
    } [string repeat "abcd" 1000000]

    tags {"slow"} {
        test {Very big payload random access} {
            set err {}
            array set payload {}
            for {set j 0} {$j < 100} {incr j} {
                set size [expr 1+[randomInt 100000]]
                set buf [string repeat "pl-$j" $size]
                set payload($j) $buf
                r set bigpayload_$j $buf
            }
            for {set j 0} {$j < 1000} {incr j} {
                set index [randomInt 100]
                set buf [r get bigpayload_$index]
                if {$buf != $payload($index)} {
                    set err "Values differ: I set '$payload($index)' but I read back '$buf'"
                    break
                }
            }
            unset payload
            set _ $err
        } {}

        test {SET 10000 numeric keys and access all them in reverse order} {
            set err {}
            for {set x 0} {$x < 10000} {incr x} {
                r set $x $x
            }
            set sum 0
            for {set x 9999} {$x >= 0} {incr x -1} {
                set val [r get $x]
                if {$val ne $x} {
                    set err "Eleemnt at position $x is $val instead of $x"
                    break
                }
            }
            set _ $err
        } {}
    }

    test {INCR against non existing key} {
        r del novar
        set res {}
        append res [r incr novar]
        append res [r get novar]
    } {11}

    test {INCR against key created by incr itself} {
        r incr novar
    } {2}

    test {INCR against key originally set with SET} {
        r set novar 100
        r incr novar
    } {101}

    test {INCR over 32bit value} {
        r set novar 17179869184
        r incr novar
    } {17179869185}

    test {INCRBY over 32bit value with over 32bit increment} {
        r set novar 17179869184
        r incrby novar 17179869184
    } {34359738368}

    test {INCR fails against key with spaces (left)} {
        r set novar "    11"
        catch {r incr novar} err
        format $err
    } {ERR*}

    test {INCR fails against key with spaces (right)} {
        r set novar "11    "
        catch {r incr novar} err
        format $err
    } {ERR*}

    test {INCR fails against key with spaces (both)} {
        r set novar "    11    "
        catch {r incr novar} err
        format $err
    } {ERR*}

    test {INCR fails against a key holding a list} {
        r rpush mylist 1
        catch {r incr mylist} err
        r rpop mylist
        format $err
    } {WRONGTYPE*}

    test {DECRBY over 32bit value with over 32bit increment, negative res} {
        r set novar 17179869184
        r decrby novar 17179869185
    } {-1}

    test {INCRBYFLOAT against non existing key} {
        r del novar
        list    [roundFloat [r incrbyfloat novar 1]] \
                [roundFloat [r get novar]] \
                [roundFloat [r incrbyfloat novar 0.25]] \
                [roundFloat [r get novar]]
    } {1 1 1.25 1.25}

    test {INCRBYFLOAT against key originally set with SET} {
        r set novar 1.5
        roundFloat [r incrbyfloat novar 1.5]
    } {3}

    test {INCRBYFLOAT over 32bit value} {
        r set novar 17179869184
        r incrbyfloat novar 1.5
    } {17179869185.5}

    test {INCRBYFLOAT over 32bit value with over 32bit increment} {
        r set novar 17179869184
        r incrbyfloat novar 17179869184
    } {34359738368}

    test {INCRBYFLOAT fails against key with spaces (left)} {
        set err {}
        r set novar "    11"
        catch {r incrbyfloat novar 1.0} err
        format $err
    } {ERR*valid*}

    test {INCRBYFLOAT fails against key with spaces (right)} {
        set err {}
        r set novar "11    "
        catch {r incrbyfloat novar 1.0} err
        format $err
    } {ERR*valid*}

    test {INCRBYFLOAT fails against key with spaces (both)} {
        set err {}
        r set novar " 11 "
        catch {r incrbyfloat novar 1.0} err
        format $err
    } {ERR*valid*}

    test {INCRBYFLOAT fails against a key holding a list} {
        r del mylist
        set err {}
        r rpush mylist 1
        catch {r incrbyfloat mylist 1.0} err
        r del mylist
        format $err
    } {WRONGTYPE*}

    test {INCRBYFLOAT does not allow NaN or Infinity} {
        r set foo 0
        set err {}
        catch {r incrbyfloat foo +inf} err
        set err
        # p.s. no way I can force NaN to test it from the API because
        # there is no way to increment / decrement by infinity nor to
        # perform divisions.
    } {ERR*would produce*}

    test {INCRBYFLOAT decrement} {
        r set foo 1
        roundFloat [r incrbyfloat foo -1.1]
    } {-0.1}

    test "SETNX target key missing" {
        r del novar
        assert_equal 1 [r setnx novar foobared]
        assert_equal "foobared" [r get novar]
    }

    test "SETNX target key exists" {
        r set novar foobared
        assert_equal 0 [r setnx novar blabla]
        assert_equal "foobared" [r get novar]
    }

    test "SETNX against not-expired volatile key" {
        r set x 10
        r expire x 10000
        assert_equal 0 [r setnx x 20]
        assert_equal 10 [r get x]
    }

    test "SETNX against expired volatile key" {
        # Make it very unlikely for the key this test uses to be expired by the
        # active expiry cycle. This is tightly coupled to the implementation of
        # active expiry and dbAdd() but currently the only way to test that
        # SETNX expires a key when it should have been.
        for {set x 0} {$x < 9999} {incr x} {
            r setex key-$x 3600 value
        }

        # This will be one of 10000 expiring keys. A cycle is executed every
        # 100ms, sampling 10 keys for being expired or not.  This key will be
        # expired for at most 1s when we wait 2s, resulting in a total sample
        # of 100 keys. The probability of the success of this test being a
        # false positive is therefore approx. 1%.
        r set x 10
        r expire x 1

        # Wait for the key to expire
        after 2000

        assert_equal 1 [r setnx x 20]
        assert_equal 20 [r get x]
    }

    test "DEL against expired key" {
        r debug set-active-expire 0
        r setex keyExpire 1 valExpire
        after 1100
        assert_equal 0 [r del keyExpire]
        r debug set-active-expire 1
    }

    test {EXISTS} {
        set res {}
        r set newkey test
        append res [r exists newkey]
        r del newkey
        append res [r exists newkey]
    } {10}

    test {Zero length value in key. SET/GET/EXISTS} {
        r set emptykey {}
        set res [r get emptykey]
        append res [r exists emptykey]
        r del emptykey
        append res [r exists emptykey]
    } {10}

    test {Commands pipelining} {
        set fd [r channel]
        puts -nonewline $fd "SET k1 xyzk\r\nGET k1\r\nPING\r\n"
        flush $fd
        set res {}
        append res [string match OK* [r read]]
        append res [r read]
        append res [string match PONG* [r read]]
        format $res
    } {1xyzk1}

    test {Non existing command} {
        catch {r foobaredcommand} err
        string match ERR* $err
    } {1}
    
    test {MGET} {
        r set foo BAR
        r set bar FOO
        r mget foo bar
    } {BAR FOO}

    test {MGET against non existing key} {
        r mget foo baazz bar
    } {BAR {} FOO}

    test {MGET against non-string key} {
        r sadd myset ciao
        r sadd myset bau
        r mget foo baazz bar myset
    } {BAR {} FOO {}}

    test {GETSET (set new value)} {
        r del foo
        list [r getset foo xyz] [r get foo]
    } {{} xyz}

    test {GETSET (replace old value)} {
        r set foo bar
        list [r getset foo xyz] [r get foo]
    } {bar xyz}
    
    test {MSET base case} {
        r mset x 10 y84 "foo bar" zsz "x x x x x x x\n\n\r\n"
        r mget x y84 zsz
    } [list 10 {foo bar} "x x x x x x x\n\n\r\n"]

    test "STRLEN against non-existing key" {
        assert_equal 0 [r strlen notakey]
    }

    test "STRLEN against integer-encoded value" {
        r set myinteger -555
        assert_equal 4 [r strlen myinteger]
    }

    test "STRLEN against plain string" {
        r set mystring "foozzz0123456789 baz"
        assert_equal 20 [r strlen mystring]
    }

    test "SETBIT against non-existing key" {
        r del mykey
        assert_equal 0 [r setbit mykey 1 1]
        assert_equal [binary format B* 01000000] [r get mykey]
    }

    test "SETBIT against string-encoded key" {
        # Ascii "@" is integer 64 = 01 00 00 00
        r set mykey "@"

        assert_equal 0 [r setbit mykey 2 1]
        assert_equal [binary format B* 01100000] [r get mykey]
        assert_equal 1 [r setbit mykey 1 0]
        assert_equal [binary format B* 00100000] [r get mykey]
    }

    test "SETBIT against integer-encoded key" {
        # Ascii "1" is integer 49 = 00 11 00 01
        r set mykey 1
        assert_encoding int mykey

        assert_equal 0 [r setbit mykey 6 1]
        assert_equal [binary format B* 00110011] [r get mykey]
        assert_equal 1 [r setbit mykey 2 0]
        assert_equal [binary format B* 00010011] [r get mykey]
    }

    test "SETBIT against key with wrong type" {
        r del mykey
        r lpush mykey "foo"
        assert_error "WRONGTYPE*" {r setbit mykey 0 1}
    }

    test "SETBIT with out of range bit offset" {
        r del mykey
        assert_error "*out of range*" {r setbit mykey [expr 4*1024*1024*1024] 1}
        assert_error "*out of range*" {r setbit mykey -1 1}
    }

    test "SETBIT with non-bit argument" {
        r del mykey
        assert_error "*out of range*" {r setbit mykey 0 -1}
        assert_error "*out of range*" {r setbit mykey 0  2}
        assert_error "*out of range*" {r setbit mykey 0 10}
        assert_error "*out of range*" {r setbit mykey 0 20}
    }

    test "SETBIT fuzzing" {
        set str ""
        set len [expr 256*8]
        r del mykey

        for {set i 0} {$i < 2000} {incr i} {
            set bitnum [randomInt $len]
            set bitval [randomInt 2]
            set fmt [format "%%-%ds%%d%%-s" $bitnum]
            set head [string range $str 0 $bitnum-1]
            set tail [string range $str $bitnum+1 end]
            set str [string map {" " 0} [format $fmt $head $bitval $tail]]

            r setbit mykey $bitnum $bitval
            assert_equal [binary format B* $str] [r get mykey]
        }
    }

    test "GETBIT against non-existing key" {
        r del mykey
        assert_equal 0 [r getbit mykey 0]
    }

    test "GETBIT against string-encoded key" {
        # Single byte with 2nd and 3rd bit set
        r set mykey "`"

        # In-range
        assert_equal 0 [r getbit mykey 0]
        assert_equal 1 [r getbit mykey 1]
        assert_equal 1 [r getbit mykey 2]
        assert_equal 0 [r getbit mykey 3]

        # Out-range
        assert_equal 0 [r getbit mykey 8]
        assert_equal 0 [r getbit mykey 100]
        assert_equal 0 [r getbit mykey 10000]
    }

    test "GETBIT against integer-encoded key" {
        r set mykey 1
        assert_encoding int mykey

        # Ascii "1" is integer 49 = 00 11 00 01
        assert_equal 0 [r getbit mykey 0]
        assert_equal 0 [r getbit mykey 1]
        assert_equal 1 [r getbit mykey 2]
        assert_equal 1 [r getbit mykey 3]

        # Out-range
        assert_equal 0 [r getbit mykey 8]
        assert_equal 0 [r getbit mykey 100]
        assert_equal 0 [r getbit mykey 10000]
    }

    test "SETRANGE against non-existing key" {
        r del mykey
        assert_equal 3 [r setrange mykey 0 foo]
        assert_equal "foo" [r get mykey]

        r del mykey
        assert_equal 0 [r setrange mykey 0 ""]
        assert_equal 0 [r exists mykey]

        r del mykey
        assert_equal 4 [r setrange mykey 1 foo]
        assert_equal "\000foo" [r get mykey]
    }

    test "SETRANGE against string-encoded key" {
        r set mykey "foo"
        assert_equal 3 [r setrange mykey 0 b]
        assert_equal "boo" [r get mykey]

        r set mykey "foo"
        assert_equal 3 [r setrange mykey 0 ""]
        assert_equal "foo" [r get mykey]

        r set mykey "foo"
        assert_equal 3 [r setrange mykey 1 b]
        assert_equal "fbo" [r get mykey]

        r set mykey "foo"
        assert_equal 7 [r setrange mykey 4 bar]
        assert_equal "foo\000bar" [r get mykey]
    }

    test "SETRANGE against integer-encoded key" {
        r set mykey 1234
        assert_encoding int mykey
        assert_equal 4 [r setrange mykey 0 2]
        assert_encoding raw mykey
        assert_equal 2234 [r get mykey]

        # Shouldn't change encoding when nothing is set
        r set mykey 1234
        assert_encoding int mykey
        assert_equal 4 [r setrange mykey 0 ""]
        assert_encoding int mykey
        assert_equal 1234 [r get mykey]

        r set mykey 1234
        assert_encoding int mykey
        assert_equal 4 [r setrange mykey 1 3]
        assert_encoding raw mykey
        assert_equal 1334 [r get mykey]

        r set mykey 1234
        assert_encoding int mykey
        assert_equal 6 [r setrange mykey 5 2]
        assert_encoding raw mykey
        assert_equal "1234\0002" [r get mykey]
    }

    test "SETRANGE against key with wrong type" {
        r del mykey
        r lpush mykey "foo"
        assert_error "WRONGTYPE*" {r setrange mykey 0 bar}
    }

    test "SETRANGE with out of range offset" {
        r del mykey
        assert_error "*maximum allowed size*" {r setrange mykey [expr 512*1024*1024-4] world}

        r set mykey "hello"
        assert_error "*out of range*" {r setrange mykey -1 world}
        assert_error "*maximum allowed size*" {r setrange mykey [expr 512*1024*1024-4] world}
    }

    test "GETRANGE against non-existing key" {
        r del mykey
        assert_equal "" [r getrange mykey 0 -1]
    }

    test "GETRANGE against string value" {
        r set mykey "Hello World"
        assert_equal "Hell" [r getrange mykey 0 3]
        assert_equal "Hello World" [r getrange mykey 0 -1]
        assert_equal "orld" [r getrange mykey -4 -1]
        assert_equal "" [r getrange mykey 5 3]
        assert_equal " World" [r getrange mykey 5 5000]
        assert_equal "Hello World" [r getrange mykey -5000 10000]
    }

    test "GETRANGE against integer-encoded value" {
        r set mykey 1234
        assert_equal "123" [r getrange mykey 0 2]
        assert_equal "1234" [r getrange mykey 0 -1]
        assert_equal "234" [r getrange mykey -3 -1]
        assert_equal "" [r getrange mykey 5 3]
        assert_equal "4" [r getrange mykey 3 5000]
        assert_equal "1234" [r getrange mykey -5000 10000]
    }

    test "GETRANGE fuzzing" {
        for {set i 0} {$i < 1000} {incr i} {
            r set bin [set bin [randstring 0 1024 binary]]
            set _start [set start [randomInt 1500]]
            set _end [set end [randomInt 1500]]
            if {$_start < 0} {set _start "end-[abs($_start)-1]"}
            if {$_end < 0} {set _end "end-[abs($_end)-1]"}
            assert_equal [string range $bin $_start $_end] [r getrange bin $start $end]
        }
    }

    test {Extended SET can detect syntax errors} {
        set e {}
        catch {r set foo bar non-existing-option} e
        set e
    } {*syntax*}

    test {Extended SET NX option} {
        r del foo
        set v1 [r set foo 1 nx]
        set v2 [r set foo 2 nx]
        list $v1 $v2 [r get foo]
    } {OK {} 1}

    test {Extended SET XX option} {
        r del foo
        set v1 [r set foo 1 xx]
        r set foo bar
        set v2 [r set foo 2 xx]
        list $v1 $v2 [r get foo]
    } {{} OK 2}

    test {Extended SET EX option} {
        r del foo
        r set foo bar ex 10
        set ttl [r ttl foo]
        assert {$ttl <= 10 && $ttl > 5}
    }

    test {Extended SET PX option} {
        r del foo
        r set foo bar px 10000
        set ttl [r ttl foo]
        assert {$ttl <= 10 && $ttl > 5}
    }

    test {Extended SET using multiple options at once} {
        r set foo val
        assert {[r set foo bar xx px 10000] eq {OK}}
        set ttl [r ttl foo]
        assert {$ttl <= 10 && $ttl > 5}
    }
}
