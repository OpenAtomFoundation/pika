import time

from utils import *


def main():
    r0 = Redis()
    r0.client.config_set("requirepass", "password")
    r0.client.execute_command("auth", "password")  # for Redis 4.0
    r1 = Redis()
    r1.client.config_set("requirepass", "password")
    r1.client.execute_command("auth", "password")  # for Redis 4.0

    t = get_empty_config()
    t["source"]["address"] = r0.get_address()
    t["source"]["password"] = "password"
    t["target"]["type"] = "standalone"
    t["target"]["address"] = r1.get_address()
    t["target"]["password"] = "password"

    rs = RedisShake()
    rs.run(t)

    # wait sync need use http interface
    r0.client.set("finished", "1")
    time.sleep(2)
    assert r1.client.get("finished") == b"1"
