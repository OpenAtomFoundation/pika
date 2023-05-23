import time

from utils import *


def main():
    r0 = Redis()
    try:
        r0.client.acl_list()
    except Exception:
        return
    r0.client.execute_command("acl", "setuser", "user0", ">password0", "~*", "+@all")
    r0.client.execute_command("acl", "setuser", "user0", "on")
    r0.client.execute_command("auth", "user0", "password0")  # for Redis 4.0
    r1 = Redis()
    r1.client.execute_command("acl", "setuser", "user1", ">password1", "~*", "+@all")
    r1.client.execute_command("acl", "setuser", "user1", "on")
    r1.client.execute_command("auth", "user1", "password1")  # for Redis 4.0

    t = get_empty_config()
    t["source"]["address"] = r0.get_address()
    t["source"]["username"] = "user0"
    t["source"]["password"] = "password0"
    t["target"]["type"] = "standalone"
    t["target"]["address"] = r1.get_address()
    t["target"]["username"] = "user1"
    t["target"]["password"] = "password1"

    rs = RedisShake()
    rs.run(t)

    # wait sync need use http interface
    r0.client.set("finished", "1")
    time.sleep(2)
    assert r1.client.get("finished") == b"1"
