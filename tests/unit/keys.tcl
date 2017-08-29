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
    assert_equal {kv_1 kv_2} [r keys * k]
    assert_equal {hash_1 hash_2} [r keys * h]
    assert_equal {list_1 list_2} [r keys * l]
    assert_equal {zset_1 zset_2} [r keys * z]
    assert_equal {set_1 set_2} [r keys * s]
    assert_equal {kv_1 kv_2 hash_1 hash_2 zset_1 zset_2 set_1 set_2 list_1 list_2} [r keys *]
    assert_equal {kv_1 kv_2} [r keys * k]
    assert_equal {hash_1 hash_2} [r keys * h]
    assert_equal {list_1 list_2} [r keys * l]
    assert_equal {zset_1 zset_2} [r keys * z]
    assert_equal {set_1 set_2} [r keys * s]
  }

  test {KEYS syntax error} {
    catch {r keys * a} e1
    catch {r keys * b} e2
    catch {r keys * c} e3
    assert_equal {ERR syntax error} [set e1]
    assert_equal {ERR syntax error} [set e2]
    assert_equal {ERR syntax error} [set e3]
  }
}
