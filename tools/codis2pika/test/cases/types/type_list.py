import redis

prefix = "list"

elements = [f"element_{i}" for i in range(10000)]


def add_rdb_data(c: redis.Redis):
    c.rpush(f"{prefix}_rdb_k", 0, 1, 2, 3, 4, 5, 6, 7)
    c.rpush(f"{prefix}_rdb_k0", *elements)


def add_aof_data(c: redis.Redis):
    c.rpush(f"{prefix}_aof_k", 0, 1, 2, 3, 4, 5, 6, 7)
    c.rpush(f"{prefix}_aof_k0", *elements)


def check_data(c: redis.Redis):
    assert c.lrange(f"{prefix}_rdb_k", 0, -1) == [b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7"]
    assert c.lrange(f"{prefix}_rdb_k0", 0, -1) == [f"element_{i}".encode() for i in range(10000)]

    assert c.lrange(f"{prefix}_aof_k", 0, -1) == [b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7"]
    assert c.lrange(f"{prefix}_aof_k0", 0, -1) == [f"element_{i}".encode() for i in range(10000)]
