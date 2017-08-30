start_server {tags {"keys"}} {
  test {KEYS with pattern} {
    foreach key {key_x key_y key_z foo_a foo_b foo_c} {
      r set $key hello
    }
    assert_equal {foo_a foo_b foo_c} [r keys foo*]
    assert_equal {foo_a foo_b foo_c} [r keys f*]
    assert_equal {foo_a foo_b foo_c} [r keys f*o*]
  }
  
  test {KEYS to get all keys} {
    lsort [r keys *]
  } {foo_a foo_b foo_c key_x key_y key_z}

  test {KEYS select by type} {
    foreach key {key_x key_y key_z foo_a foo_b foo_c} {
      r del $key
    }
    r set kv_1 value
    r set kv_2 value
    r hset hash_1 hash_field 1
    r hset hash_2 hash_field 1
    r lpush list_1 value
    r lpush list_2 value
    r zadd zset_1 1 "a"
    r zadd zset_2 1 "a"
    r sadd set_1 "a"
    r sadd set_2 "a"
    assert_equal {kv_1 kv_2} [r keys * string]
    assert_equal {hash_1 hash_2} [r keys * hash]
    assert_equal {list_1 list_2} [r keys * list]
    assert_equal {zset_1 zset_2} [r keys * zset]
    assert_equal {set_1 set_2} [r keys * set]
    assert_equal {kv_1 kv_2 hash_1 hash_2 zset_1 zset_2 set_1 set_2 list_1 list_2} [r keys *]
    assert_equal {kv_1 kv_2} [r keys * string]
    assert_equal {hash_1 hash_2} [r keys * hash]
    assert_equal {list_1 list_2} [r keys * list]
    assert_equal {zset_1 zset_2} [r keys * zset]
    assert_equal {set_1 set_2} [r keys * set]
  }

  test {KEYS syntax error} {
    catch {r keys * a} e1
    catch {r keys * strings} e2
    catch {r keys * c d} e3
    catch {r keys} e4
    catch {r keys * set zset} e5
    assert_equal {ERR syntax error} [set e1]
    assert_equal {ERR syntax error} [set e2]
    assert_equal {ERR syntax error} [set e3]
    assert_equal {ERR wrong number of arguments for 'keys' command} [set e4]
    assert_equal {ERR syntax error} [set e5]
  }
}
