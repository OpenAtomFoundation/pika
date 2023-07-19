# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# This is the basic replication tests for pika
#
# It's also the tests for the issues and pr below:
# relevent issue:
#     https://github.com/OpenAtomFoundation/pika/issues/1638
#     https://github.com/OpenAtomFoundation/pika/issues/1608
# relevent pr:
#     https://github.com/OpenAtomFoundation/pika/pull/1658
#     https://github.com/OpenAtomFoundation/pika/issues/1638
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
import threading
import time
import redis
import random
import string


def test_del_replication():
    print("start del multiple keys replication test")
    # 创建Redis客户端
    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)
    # 执行日志中的操作
    master.delete('blist0', 'blist1', 'blist2', 'blist3')
    master.delete('blist100', 'blist101', 'blist102', 'blist103')
    master.delete('blist0', 'blist1', 'blist2', 'blist3')

    master.rpush('blist3', 'v2')
    master.lpush('blist2', 'v2')
    master.lpop('blist3')
    master.rpop('blist2')

    master.lpush('blist2', 'v2')
    master.lpop('blist2')
    master.rpush('blist3', 'v2')
    master.rpop('blist3')

    master.lpush('blist2', 'v2')
    master.lpop('blist2')
    master.rpush('blist3', 'v2')
    master.lpush('blist2', 'v2')

    master.rpop('blist3')
    master.lpop('blist2')
    master.lpush('blist2', 'v2')
    master.rpush('blist3', 'v2')

    master.rpop('blist3')
    master.lpop('blist2')
    master.rpush('blist3', 'v2')
    master.lpush('blist2', 'v2')

    master.rpop('blist3')
    master.rpush('blist3', 'v2')
    master.lpush('blist2', 'v2')
    master.rpush('blist3', 'v2')

    master.rpush('blist3', 'v2')
    master.lpush('blist2', 'v2')
    master.lpush('blist2', 'v2')
    master.rpush('blist3', 'v2')

    master.lpush('blist2', 'v2')
    master.rpush('blist3', 'v2')
    master.delete('blist1', 'large', 'blist2')

    master.rpush('blist1', 'a', 'large', 'c')
    master.rpush('blist2', 'd', 'large', 'f')

    master.lpop('blist1')
    master.rpop('blist1')
    master.lpop('blist2')
    master.rpop('blist2')

    master.delete('blist3')
    master.lpop('blist2')
    master.rpop('blist1')

    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)
    # Retrieve all keys from the master and slave
    m_keys = master.keys()
    s_keys = slave.keys()

    # print(m_keys)
    # print(s_keys)
    # Check if the keys in the master and slave are the same
    assert set(m_keys) == set(s_keys), f'Expected: s_keys == m_keys, but got s_keys: {s_keys}, m_keys: {m_keys}'

    lists_ = ['blist1', 'blist2', 'blist3']
    for this_list in lists_:
        # Check if the length of the list stored at this_list is the same in master and slave
        assert master.llen(this_list) == slave.llen(this_list), \
            f'Expected: master.llen({this_list}) == slave.llen({this_list}), but got {master.llen(this_list)} != {slave.llen(this_list)}'
        # Check if each element in the list is the same in the master and slave
        for i in range(0, master.llen(this_list)):
            mv = master.lindex(this_list, i)
            sv = slave.lindex(this_list, i)
            assert mv == sv, \
                f"Expected: master.lindex({this_list}, {i}) == slave.lindex({this_list}, {i}), but got {mv} != {sv}"

    master.close()
    slave.close()
    print("Del multiple keys replication  OK [✓]")


def test_msetnx_replication():
    print("start test_msetnx_replication")
    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('1mset_key', '2mset_key', '3mset_key', '4mset_key')

    def random_mset_thread(keys_):
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        for i in range(0, 3):
            kvs1 = {}
            kvs2 = {}
            kvs3 = {}
            kvs4 = {}
            letters = string.ascii_letters
            for key in keys_:
                kvs1[key] = ''.join(random.choice(letters) for _ in range(5))
                kvs2[key] = ''.join(random.choice(letters) for _ in range(5))
                kvs3[key] = ''.join(random.choice(letters) for _ in range(5))
                kvs4[key] = ''.join(random.choice(letters) for _ in range(5))
            pika.set(keys_[2], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[3], ''.join(random.choice(letters) for _ in range(5)))
            pika.delete(*keys_)
            pika.msetnx(kvs1)
            pika.set(keys_[0], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[1], ''.join(random.choice(letters) for _ in range(5)))
            pika.delete(*keys_)
            pika.msetnx(kvs2)
            pika.set(keys_[1], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[2], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[0], ''.join(random.choice(letters) for _ in range(5)))
            pika.delete(*keys_)
            pika.msetnx(kvs3)
            pika.set(keys_[3], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[0], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[1], ''.join(random.choice(letters) for _ in range(5)))
            pika.delete(*keys_)
            pika.msetnx(kvs4)
            pika.set(keys_[3], ''.join(random.choice(letters) for _ in range(5)))

    keys = ['1mset_key', '2mset_key', '3mset_key', '4mset_key']
    threads = []
    for i in range(0, 50):
        t = threading.Thread(target=random_mset_thread, args=(keys,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)

    for key in keys:
        m_v = master.get(key)
        s_v = slave.get(key)
        assert m_v == s_v, f'Expected: master_v == slave_v, but got slave_v:{s_v}, master_v:{m_v}, using key:{key}'
    print("test_msetnx_replication  OK [✓]")


def test_mset_replication():
    print("start test_mset_replication")
    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    keys = ['1mset_key', '2mset_key', '3mset_key', '4mset_key']
    master.delete('1mset_key', '2mset_key', '3mset_key', '4mset_key')

    def random_mset_thread(keys_):
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        for i in range(0, 3):
            kvs1 = {}
            kvs2 = {}
            kvs3 = {}
            kvs4 = {}
            letters = string.ascii_letters
            for key in keys_:
                kvs1[key] = ''.join(random.choice(letters) for _ in range(5))
                kvs2[key] = ''.join(random.choice(letters) for _ in range(5))
                kvs3[key] = ''.join(random.choice(letters) for _ in range(5))
                kvs4[key] = ''.join(random.choice(letters) for _ in range(5))
            pika.set(keys_[2], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[3], ''.join(random.choice(letters) for _ in range(5)))
            pika.mset(kvs1)
            pika.set(keys_[0], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[1], ''.join(random.choice(letters) for _ in range(5)))
            pika.mset(kvs2)
            pika.set(keys_[1], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[2], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[0], ''.join(random.choice(letters) for _ in range(5)))
            pika.mset(kvs3)
            pika.set(keys_[3], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[0], ''.join(random.choice(letters) for _ in range(5)))
            pika.set(keys_[1], ''.join(random.choice(letters) for _ in range(5)))
            pika.mset(kvs4)
            pika.set(keys_[3], ''.join(random.choice(letters) for _ in range(5)))

    threads = []
    for i in range(0, 50):
        t = threading.Thread(target=random_mset_thread, args=(keys,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)

    for key in keys:
        m_v = master.get(key)
        s_v = slave.get(key)
        assert m_v == s_v, f'Expected: master_v == slave_v, but got slave_v:{s_v}, master_v:{m_v}, using key:{key}'
    print("test_mset_replication  OK [✓]")


def test_smove_replication():
    print("start test_smove_replication")

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)

    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('source_set', 'dest_set')

    def random_smove_thread():
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        letters = string.ascii_letters
        for i in range(0, 1):
            member = ''.join(random.choice(letters) for _ in range(5))
            pika.sadd('source_set', member)
            pika.sadd('source_set', member)
            pika.sadd('source_set', member)
            pika.srem('dest_set', member)
            pika.srem('dest_set', member)
            pika.smove('source_set', 'dest_set', member)

    threads = []
    for i in range(0, 10):
        t = threading.Thread(target=random_smove_thread)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)
    m_source_set = master.smembers('source_set')
    m_dest_set = master.smembers('dest_set')
    s_source_set = slave.smembers('source_set')
    s_dest_set = slave.smembers('dest_set')

    assert m_source_set == s_source_set, f'Expected: source_set on master == source_set on slave, but got source_set on slave:{s_source_set}, source_set on master:{m_source_set}'
    assert m_dest_set == s_dest_set, f'Expected: dest_set on master == dest_set on slave, but got dest_set on slave:{s_dest_set}, dest_set on master:{m_dest_set}'
    print("start test_smove_replication  OK [✓]")


def test_rpoplpush_replication():
    print("start test_rpoplpush_replication")
    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('blist0', 'blist1', 'blist')

    def rpoplpush_thread1():
        nonlocal master
        for i in range(0, 50):
            letters = string.ascii_letters
            random_str1 = ''.join(random.choice(letters) for _ in range(5))
            random_str2 = ''.join(random.choice(letters) for _ in range(5))
            random_str3 = ''.join(random.choice(letters) for _ in range(5))
            master.lpush('blist0', random_str1)
            master.rpoplpush('blist0', 'blist')
            master.lpush('blist', random_str1, random_str2, random_str3)

            master.lpop('blist')
            master.rpop('blist')
            master.lpush('blist0', random_str3)
            master.rpoplpush('blist0', 'blist')
            master.rpush('blist', random_str3, random_str2, random_str1)
            master.lpop('blist')
            master.lpush('blist0', random_str2)
            master.rpoplpush('blist0', 'blist')
            master.rpop('blist')

    t1 = threading.Thread(target=rpoplpush_thread1)
    t2 = threading.Thread(target=rpoplpush_thread1)
    t3 = threading.Thread(target=rpoplpush_thread1)
    t4 = threading.Thread(target=rpoplpush_thread1)
    t5 = threading.Thread(target=rpoplpush_thread1)
    t6 = threading.Thread(target=rpoplpush_thread1)
    t7 = threading.Thread(target=rpoplpush_thread1)
    t8 = threading.Thread(target=rpoplpush_thread1)
    t9 = threading.Thread(target=rpoplpush_thread1)
    t10 = threading.Thread(target=rpoplpush_thread1)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
    t9.start()
    t10.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()
    t9.join()
    t10.join()

    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)
    m_keys = master.keys()
    s_keys = slave.keys()
    assert s_keys == m_keys, f'Expected: s_keys == m_keys, but got s_keys: {s_keys}, m_keys: {m_keys}'

    for i in range(0, master.llen('blist')):
        # print(master.lindex('blist', i))
        # print(slave.lindex('blist', i))
        assert master.lindex('blist', i) == slave.lindex('blist', i), \
            f"Expected:master.lindex('blist', i) == slave.linex('blist', i), but got False when i = {i}"
    print("test_rpoplpush_replication OK [✓]")


def test_sdiffstore_replication():
    print("start test_sdiffstore_replication")

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('set1', 'set2', 'dest_set')

    def random_sdiffstore_thread():
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        letters = string.ascii_letters
        for i in range(0, 10):
            pika.sadd('set1', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(letters) for _ in range(5)))
            pika.sadd('dest_set', ''.join(random.choice(letters) for _ in range(5)))
            pika.sdiffstore('dest_set', 'set1', 'set2')

    threads = []
    for i in range(0, 10):
        t = threading.Thread(target=random_sdiffstore_thread)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)

    m_set1 = master.smembers('set1')
    m_set2 = master.smembers('set2')
    m_dest_set = master.smembers('dest_set')
    s_set1 = slave.smembers('set1')
    s_set2 = slave.smembers('set2')
    s_dest_set = slave.smembers('dest_set')

    assert m_set1 == s_set1, f'Expected: set1 on master == set1 on slave, but got set1 on slave:{s_set1}, set1 on master:{m_set1}'
    assert m_set2 == s_set2, f'Expected: set2 on master == set2 on slave, but got set2 on slave:{s_set2}, set2 on master:{m_set2}'
    assert m_dest_set == s_dest_set, f'Expected: dest_set on master == dest_set on slave, but got dest_set on slave:{s_dest_set}, dest_set on master:{m_dest_set}'
    print("test_sdiffstore_replication OK [✓]")


def test_sinterstore_replication():
    print("start test_sinterstore_replication")

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('set1', 'set2', 'dest_set')

    def random_sinterstore_thread():
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        letters = string.ascii_letters
        for i in range(0, 10):
            member = ''.join(random.choice(letters) for _ in range(5))
            member2 = ''.join(random.choice(letters) for _ in range(5))
            member3 = ''.join(random.choice(letters) for _ in range(5))
            member4 = ''.join(random.choice(letters) for _ in range(5))
            member5 = ''.join(random.choice(letters) for _ in range(5))
            member6 = ''.join(random.choice(letters) for _ in range(5))
            pika.sadd('set1', member)
            pika.sadd('set2', member)
            pika.sadd('set1', member2)
            pika.sadd('set2', member2)
            pika.sadd('set1', member3)
            pika.sadd('set2', member3)
            pika.sadd('set1', member4)
            pika.sadd('set2', member4)
            pika.sadd('set1', member5)
            pika.sadd('set2', member5)
            pika.sadd('set1', member6)
            pika.sadd('set2', member6)
            pika.sadd('dest_set', ''.join(random.choice(letters) for _ in range(5)))
            pika.sinterstore('dest_set', 'set1', 'set2')
            pika.sadd('dest_set', ''.join(random.choice(letters) for _ in range(5)))

    threads = []
    for i in range(0, 10):
        t = threading.Thread(target=random_sinterstore_thread)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)

    m_dest_set = master.smembers('dest_set')
    s_dest_set = slave.smembers('dest_set')

    assert m_dest_set == s_dest_set, f'Expected: dest_set on master == dest_set on slave, but got dest_set on slave:{s_dest_set}, dest_set on master:{m_dest_set}'
    print("test_sinterstore_replication OK [✓]")


def test_zunionstore_replication():
    print("start test_zunionstore_replication")

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('zset1', 'zset2', 'zset_out')

    def random_zunionstore_thread():
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        for i in range(0, 10):
            pika.zadd('zset1', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset2', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset2', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset1', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset2', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset1', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset2', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset2', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset1', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset1', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset2', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zadd('zset1', {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})
            pika.zunionstore('zset_out', ['zset1', 'zset2'])
            pika.zadd('zset_out',
                      {''.join(random.choice(string.ascii_letters) for _ in range(5)): random.randint(1, 5)})

    threads = []
    for i in range(0, 10):
        t = threading.Thread(target=random_zunionstore_thread)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)

    m_zset_out = master.zrange('zset_out', 0, -1, withscores=True)
    s_zset_out = slave.zrange('zset_out', 0, -1, withscores=True)

    assert m_zset_out == s_zset_out, f'Expected: zset_out on master == zset_out on slave, but got zset_out on slave:{s_zset_out}, zset_out on master:{m_zset_out}'
    print("test_zunionstore_replication OK [✓]")


def test_zinterstore_replication():
    print("start test_zinterstore_replication")

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('zset1', 'zset2', 'zset_out')

    def random_zinterstore_thread():
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        for i in range(0, 10):
            member = ''.join(random.choice(string.ascii_letters) for _ in range(5))
            member2 = ''.join(random.choice(string.ascii_letters) for _ in range(5))
            member3 = ''.join(random.choice(string.ascii_letters) for _ in range(5))
            member4 = ''.join(random.choice(string.ascii_letters) for _ in range(5))
            pika.zadd('zset1', {member: random.randint(1, 5)})
            pika.zadd('zset2', {member: random.randint(1, 5)})
            pika.zadd('zset1', {member2: random.randint(1, 5)})
            pika.zadd('zset2', {member2: random.randint(1, 5)})
            pika.zadd('zset1', {member3: random.randint(1, 5)})
            pika.zadd('zset2', {member3: random.randint(1, 5)})
            pika.zadd('zset1', {member4: random.randint(1, 5)})
            pika.zadd('zset2', {member4: random.randint(1, 5)})
            pika.zinterstore('zset_out', ['zset1', 'zset2'])


    threads = []
    for i in range(0, 10):
        t = threading.Thread(target=random_zinterstore_thread)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)

    m_zset_out = master.zrange('zset_out', 0, -1, withscores=True)
    s_zset_out = slave.zrange('zset_out', 0, -1, withscores=True)

    if len(m_zset_out) != len(s_zset_out):
        print(f"Length mismatch: Master has {len(m_zset_out)} elements, Slave has {len(s_zset_out)} elements")

    for i, (m_item, s_item) in enumerate(zip(m_zset_out, s_zset_out)):
        if m_item != s_item:
            print(f"Mismatch at rank {i + 1}: Master has {m_item}, Slave has {s_item}")

    assert m_zset_out == s_zset_out, f'Expected: zset_out on master == zset_out on slave, but got zset_out on slave:{s_zset_out}, zset_out on master:{m_zset_out}'

    print("test_zinterstore_replication OK [✓]")


def test_sunionstore_replication():
    print("start test_sunionstore_replication")

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('set1', 'set2', 'set_out')

    def random_sunionstore_thread():
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        for i in range(0, 10):
            pika.sadd('set1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sadd('set2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.sunionstore('set_out', ['set1', 'set2'])

    threads = []
    for i in range(0, 10):
        t = threading.Thread(target=random_sunionstore_thread)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)

    m_set_out = master.smembers('set_out')
    s_set_out = slave.smembers('set_out')

    assert m_set_out == s_set_out, f'Expected: set_out on master == set_out on slave, but got set_out on slave:{s_set_out}, set_out on master:{m_set_out}'
    print("test_sunionstore_replication OK [✓]")


def test_bitop_replication():
    print("start test_bitop_replication")

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('bitkey1', 'bitkey2', 'bitkey_out1', 'bitkey_out2')

    def random_bitop_thread():
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        for i in range(0, 100):  # Consider increasing the range to a larger number to get more meaningful results.
            offset1 = random.randint(0, 100)  # You may want to adjust the range based on your use case.
            offset2 = random.randint(0, 100)  # You may want to adjust the range based on your use case.
            value1 = random.choice([0, 1])
            value2 = random.choice([0, 1])
            pika.setbit('bitkey1', offset1, value1)
            pika.setbit('bitkey2', offset1, value1)
            pika.bitop('AND', 'bitkey_out1', 'bitkey1', 'bitkey2')
            pika.setbit('bitkey1', offset1 + offset2, value2)
            pika.setbit('bitkey2', offset2, value2)
            pika.bitop('OR', 'bitkey_out2', 'bitkey1', 'bitkey2')

    threads = []
    for i in range(0, 10):
        t = threading.Thread(target=random_bitop_thread)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)

    m_key_out_count1 = master.bitcount('bitkey_out1')
    s_key_out_count1 = slave.bitcount('bitkey_out1')

    m_key_out_count2 = master.bitcount('bitkey_out2')
    s_key_out_count2 = slave.bitcount('bitkey_out2')

    assert m_key_out_count1 == s_key_out_count1, f'Expected: bitcount of bitkey_out1 on master == bitcount of bitkey_out1 on slave, but got bitcount of bitkey_out1 on slave:{s_key_out_count1}, bitcount of bitkey_out1 on master:{m_key_out_count1}'
    assert m_key_out_count2 == s_key_out_count2, f'Expected: bitcount of bitkey_out2 on master == bitcount of bitkey_out2 on slave, but got bitcount of bitkey_out2 on slave:{s_key_out_count2}, bitcount of bitkey_out1 on master:{m_key_out_count2}'
    print("test_bitop_replication OK [✓]")


def test_pfmerge_replication():
    print("start test_pfmerge_replication")

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    if delay_slave_of:
        slave.slaveof("no", "one")
    else:
        slave.slaveof(master_ip, master_port)
    time.sleep(1)

    master.delete('hll1', 'hll2', 'hll_out')

    def random_pfmerge_thread():
        pika = redis.Redis(host=master_ip, port=int(master_port), db=0)
        for i in range(0, 1):
            pika.pfadd('hll1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.pfadd('hll2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.pfadd('hll2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.pfadd('hll1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.pfadd('hll2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.pfadd('hll1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.pfadd('hll2', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.pfadd('hll1', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.pfadd('hll_out', ''.join(random.choice(string.ascii_letters) for _ in range(5)))
            pika.pfmerge('hll_out', 'hll1', 'hll2')
            pika.pfadd('hll_out', ''.join(random.choice(string.ascii_letters) for _ in range(5)))

    threads = []
    for i in range(0, 50):
        t = threading.Thread(target=random_pfmerge_thread)
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if delay_slave_of:
        slave.slaveof(master_ip, master_port)
        time.sleep(25)
    else:
        time.sleep(10)

    m_hll_out = master.pfcount('hll_out')
    s_hll_out = slave.pfcount('hll_out')

    assert m_hll_out == s_hll_out, f'Expected: hll_out on master == hll_out on slave, but got hll_out on slave:{s_hll_out}, hll_out on master:{m_hll_out}'
    print("test_pfmerge_replication OK [✓]")

def test_migrateslot_replication():
    print("start test_migrateslot_replication")
    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)

    # open slot migrate
    master.config_set("slotmigrate", "yes")
    slave.config_set("slotmigrate", "no")

    setKey1 = "setKey_000"
    setKey2 = "setKey_001"
    setKey3 = "setKey_002"
    setKey4 = "setKey_store"

    slave.slaveof(master_ip, master_port)
    time.sleep(20)

    master.delete(setKey1)
    master.delete(setKey2)
    master.delete(setKey3)
    master.delete(setKey4)

    letters = string.ascii_letters
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey3, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey3, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey3, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey3, ''.join(random.choice(letters) for _ in range(5)))
    master.sdiffstore(setKey4, setKey1, setKey2)

    time.sleep(25)

    m_set1 = master.smembers(setKey1)
    m_set2 = master.smembers(setKey2)
    m_set3 = master.smembers(setKey3)
    m_dest_set = master.smembers(setKey4)
    s_set1 = slave.smembers(setKey1)
    s_set2 = slave.smembers(setKey2)
    s_set3 = slave.smembers(setKey3)
    s_dest_set = slave.smembers(setKey4)

    assert m_set1 == s_set1, f'Expected: set1 on master == set1 on slave, but got set1 on slave:{s_set1}, set1 on master:{m_set1}'
    assert m_set2 == s_set2, f'Expected: set2 on master == set2 on slave, but got set2 on slave:{s_set2}, set2 on master:{m_set2}'
    assert m_set3 == s_set3, f'Expected: set3 on master == set3 on slave, but got set3 on slave:{s_set3}, set3 on master:{m_set3}'
    assert m_dest_set == s_dest_set, f'Expected: dest_set on master == dest_set on slave, but got dest_set on slave:{s_dest_set}, dest_set on master:{m_dest_set}'

    # disconnect mster and slave
    slave.slaveof("no", "one")

    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey3, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey3, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey3, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey2, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey1, ''.join(random.choice(letters) for _ in range(5)))
    master.sadd(setKey3, ''.join(random.choice(letters) for _ in range(5)))
    master.sdiffstore(setKey4, setKey1, setKey2)

    # reconnect mster and slave
    slave.slaveof(master_ip, master_port)
    time.sleep(25)

    m_set1 = master.smembers(setKey1)
    m_set2 = master.smembers(setKey2)
    m_set3 = master.smembers(setKey3)
    m_dest_set = master.smembers(setKey4)
    time.sleep(15)
    s_set1 = slave.smembers(setKey1)
    s_set2 = slave.smembers(setKey2)
    s_set3 = slave.smembers(setKey3)
    s_dest_set = slave.smembers(setKey4)

    assert m_set1 == s_set1, f'Expected: set1 on master == set1 on slave, but got set1 on slave:{s_set1}, set1 on master:{m_set1}'
    assert m_set2 == s_set2, f'Expected: set2 on master == set2 on slave, but got set2 on slave:{s_set2}, set2 on master:{m_set2}'
    assert m_set3 == s_set3, f'Expected: set3 on master == set3 on slave, but got set3 on slave:{s_set3}, set3 on master:{m_set3}'
    assert m_dest_set == s_dest_set, f'Expected: dest_set on master == dest_set on slave, but got dest_set on slave:{s_dest_set}, dest_set on master:{m_dest_set}'

    # slave node should not has slot key
    s_keys = slave.keys()
    for key in s_keys:
        assert not (str(key).startswith("_internal:slotkey:4migrate:") or str(key).startswith("_internal:slottag:4migrate:")), f'Expected: slave should not has slot key, but got {key}'

    master.config_set("slotmigrate", "no")

    i_keys = master.keys("_internal:slotkey:4migrate*")
    master.delete(*i_keys)
    print("test_migrateslot_replication OK [✓]")

master_ip = '127.0.0.1'
master_port = '9221'
slave_ip = '127.0.0.1'
slave_port = '9231'


# ---------For Github Action---------Start
# Simulate the slave server goes down and After being disconnected for a while, it reconnects to the master server.
delay_slave_of = False #Don't change this  to True, unless you've added a long sleep after every slaveof

test_migrateslot_replication()
master = redis.Redis(host=master_ip, port=int(master_port), db=0)
slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
slave.slaveof(master_ip, master_port)
time.sleep(25)
test_rpoplpush_replication()
test_bitop_replication()
test_msetnx_replication()
test_mset_replication()
test_smove_replication()
test_del_replication()
test_pfmerge_replication()
test_sdiffstore_replication()
test_sinterstore_replication()
test_zunionstore_replication()
test_zinterstore_replication()
test_sunionstore_replication()
# ---------For Github Action---------End


# ---------For Local Stress Test---------Start
# delay_slave_of = False
# for i in range(0, 200):
#     test_rpoplpush_replication()
#     test_bitop_replication()
#     test_del_replication()
#     test_msetnx_replication()
#     test_mset_replication()
#     test_smove_replication()
#     test_pfmerge_replication()
#     test_sunionstore_replication()
#     test_sdiffstore_replication()
#     test_sinterstore_replication()
#     test_zunionstore_replication()
#     test_zinterstore_replication()
#     delay_slave_of = not delay_slave_of
# ---------For Local Stress Test---------End

