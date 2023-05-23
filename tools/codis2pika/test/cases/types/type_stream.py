import redis

prefix = "stream"

fields = {f"field_{i}": f"value_{i}" for i in range(64)}
STREAM_LENGTH = 128


def add_rdb_data(c: redis.Redis):
    c.xadd(f"{prefix}_rdb_k", {"key0": "value0"}, "*")
    for i in range(STREAM_LENGTH):
        c.xadd(f"{prefix}_rdb_k_large", fields=fields, id="*")
    

def add_aof_data(c: redis.Redis):
    c.xadd(f"{prefix}_aof_k", {"key0": "value0"}, "*")
    for i in range(STREAM_LENGTH):
        c.xadd(f"{prefix}_aof_k_large", fields=fields, id="*")


def check_data(c: redis.Redis):
    ret = c.xread(streams={f"{prefix}_rdb_k": "0-0"}, count=1)[0][1]
    assert ret[0][1] == {b"key0": b"value0"}

    ret = c.xread(streams={f"{prefix}_rdb_k_large": "0-0"}, count=STREAM_LENGTH)[0][1]
    for i in range(STREAM_LENGTH):
        assert ret[i][1] == {k.encode(): v.encode() for k, v in fields.items()}

    ret = c.xread(streams={f"{prefix}_aof_k": "0-0"}, count=1)[0][1]
    assert ret[0][1] == {b"key0": b"value0"}

    ret = c.xread(streams={f"{prefix}_aof_k_large": "0-0"}, count=STREAM_LENGTH)[0][1]
    for i in range(STREAM_LENGTH):
        assert ret[i][1] == {k.encode(): v.encode() for k, v in fields.items()}
