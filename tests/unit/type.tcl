start_server {tags {"type"}} {

    test "type none" {
        r flushdb
        assert_equal none [r type key]
    }

    test "type command" {
        r flushdb

        r set key1 key1
        assert_equal string [r type key1]

        r hset key2 key key2
        assert_equal hash [r type key2]

        r lpush key3 key3
        assert_equal list [r type key3]

        r zadd key4 100 key4
        assert_equal zset [r type key4]

        r sadd key5 key5
        assert_equal set [r type key5]
    }

    test "ptype none" {
        r flushdb
        assert_equal {} [r ptype key]
    }

    test "ptype command" {
        r flushdb

        r set key1 key1
        assert_equal string [r ptype key1]

        r hset key1 key key1
        assert_equal {string hash} [r ptype key1]

        r lpush key1 key1
        assert_equal {string hash list} [r ptype key1]

        r zadd key1 100 key1
        assert_equal {string hash list zset} [r ptype key1]

        r sadd key1 key1
        assert_equal {string hash list zset set} [r ptype key1]
    }
}