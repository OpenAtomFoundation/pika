import redis

prefix = "hash"


def add_rdb_data(c: redis.Redis):
    c.hset(f"{prefix}_rdb_k", "key0", "value0")
    for i in range(10000):
        c.hset(f"{prefix}_rdb_k_large", f"key{i}", f"value{i}")


def add_aof_data(c: redis.Redis):
    c.hset(f"{prefix}_aof_k", "key0", "value0")
    for i in range(10000):
        c.hset(f"{prefix}_aof_k_large", f"key{i}", f"value{i}")


def check_data(c: redis.Redis):
    assert c.hget(f"{prefix}_rdb_k", "key0") == b"value0"
    assert c.hmget(f"{prefix}_rdb_k_large", *[f"key{i}" for i in range(10000)]) == [f"value{i}".encode() for i in
                                                                                    range(10000)]

    assert c.hget(f"{prefix}_aof_k", "key0") == b"value0"
    assert c.hmget(f"{prefix}_aof_k_large", *[f"key{i}" for i in range(10000)]) == [f"value{i}".encode() for i in
                                                                                    range(10000)]
