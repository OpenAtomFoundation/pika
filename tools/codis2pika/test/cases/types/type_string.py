import redis

prefix = "string"


def add_rdb_data(c: redis.Redis):
    c.set(f"{prefix}_rdb_k", "v")
    c.set(f"{prefix}_rdb_int", 0)
    c.set(f"{prefix}_rdb_int0", -1)
    c.set(f"{prefix}_rdb_int1", 123456789)


def add_aof_data(c: redis.Redis):
    c.set(f"{prefix}_aof_k", "v")
    c.set(f"{prefix}_aof_int", 0)
    c.set(f"{prefix}_aof_int0", -1)
    c.set(f"{prefix}_aof_int1", 123456789)


def check_data(c: redis.Redis):
    assert c.get(f"{prefix}_rdb_k") == b"v"
    assert c.get(f"{prefix}_rdb_int") == b'0'
    assert c.get(f"{prefix}_rdb_int0") == b'-1'
    assert c.get(f"{prefix}_rdb_int1") == b'123456789'

    assert c.get(f"{prefix}_aof_k") == b"v"
    assert c.get(f"{prefix}_aof_int") == b'0'
    assert c.get(f"{prefix}_aof_int0") == b'-1'
    assert c.get(f"{prefix}_aof_int1") == b'123456789'
