import redis

prefix = "zset"
float_maps = {"a": 1.1111, "b": 2.2222, "c": 3.3333}
maps = {str(item): item for item in range(10000)}


def add_rdb_data(c: redis.Redis):
    c.zadd(f"{prefix}_rdb_k_float", float_maps)
    c.zadd(f"{prefix}_rdb_k", maps)


def add_aof_data(c: redis.Redis):
    c.zadd(f"{prefix}_aof_k_float", float_maps)
    c.zadd(f"{prefix}_aof_k", maps)


def check_data(c: redis.Redis):
    for k, v in c.zrange(f"{prefix}_rdb_k_float", 0, -1, withscores=True):
        assert float_maps[k.decode()] == v
    for k, v in c.zrange(f"{prefix}_rdb_k", 0, -1, withscores=True):
        assert maps[k.decode()] == v

    for k, v in c.zrange(f"{prefix}_aof_k_float", 0, -1, withscores=True):
        assert float_maps[k.decode()] == v
    for k, v in c.zrange(f"{prefix}_aof_k", 0, -1, withscores=True):
        assert maps[k.decode()] == v
