import time

import jury
from utils import *

from . import type_string, type_list, type_set, type_hash, type_zset, type_stream


def main():
    rs = RedisShake()
    r0 = Redis()
    r1 = Redis()
    t = get_empty_config()
    t["source"]["address"] = r0.get_address()
    t["target"]["type"] = "standalone"
    t["target"]["address"] = r1.get_address()

    timer = jury.Timer()
    type_string.add_rdb_data(r0.client)
    type_list.add_rdb_data(r0.client)
    type_set.add_rdb_data(r0.client)
    type_hash.add_rdb_data(r0.client)
    type_zset.add_rdb_data(r0.client)
    type_stream.add_rdb_data(r0.client)
    jury.log(f"add_rdb_data: {timer.elapsed_time()}s")

    # run redis-shake
    rs.run(t)
    time.sleep(1)

    timer = jury.Timer()
    type_string.add_aof_data(r0.client)
    type_list.add_aof_data(r0.client)
    type_set.add_aof_data(r0.client)
    type_hash.add_aof_data(r0.client)
    type_zset.add_aof_data(r0.client)
    type_stream.add_aof_data(r0.client)
    jury.log(f"add_aof_data: {timer.elapsed_time()}s")

    # wait sync need use http interface
    timer = jury.Timer()
    r0.client.set("finished", "1")
    cnt = 0
    while r1.client.get("finished") != b"1":
        time.sleep(0.5)
        cnt += 1
        if cnt > 20:
            raise Exception("sync timeout")
    jury.log(f"sync time: {timer.elapsed_time()}s")

    timer = jury.Timer()
    type_string.check_data(r1.client)
    type_list.check_data(r1.client)
    type_set.check_data(r1.client)
    type_hash.check_data(r1.client)
    type_zset.check_data(r1.client)
    type_stream.check_data(r1.client)
    jury.log(f"check_data: {timer.elapsed_time()}s")


if __name__ == '__main__':
    main()
