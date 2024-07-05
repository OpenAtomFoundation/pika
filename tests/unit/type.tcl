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
}