import threading
import time
import redis
import random
import string

# 测试rpoplpush的主从复制一致性:
# 相应 issue:https://github.com/OpenAtomFoundation/pika/issues/1608
# 相应 pr:https://github.com/OpenAtomFoundation/pika/pull/1616
def test_master_slave_replication():
    print("start test_master_slave_replication")
    master_ip = '127.0.0.1'
    master_port = '9221'
    slave_ip = '127.0.0.1'
    slave_port = '9231'

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    slave.slaveof(master_ip, master_port)
    master.delete('blist0', 'blist1', 'blist')

    time.sleep(10)
    m_keys = master.keys()
    s_keys = slave.keys()
    assert s_keys == m_keys, f'Expected: s_keys == m_keys, but got {s_keys == m_keys}'

    def thread1():
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

    t1 = threading.Thread(target=thread1)
    t2 = threading.Thread(target=thread1)
    t3 = threading.Thread(target=thread1)
    t4 = threading.Thread(target=thread1)
    t5 = threading.Thread(target=thread1)
    t6 = threading.Thread(target=thread1)
    t7 = threading.Thread(target=thread1)
    t8 = threading.Thread(target=thread1)
    t9 = threading.Thread(target=thread1)
    t10 = threading.Thread(target=thread1)
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

    time.sleep(10)
    m_keys = master.keys()
    s_keys = slave.keys()
    assert s_keys == m_keys, f'Expected: s_keys == m_keys, but got {s_keys == m_keys}'

    for i in range(0, master.llen('blist')):
        # print(master.lindex('blist', i))
        # print(slave.lindex('blist', i))
        assert master.lindex('blist', i) == slave.lindex('blist', i), \
            f"Expected:master.lindex('blist', i) == slave.linex('blist', i), but got False when i = {i}"
    print("test_master_slave_replication OK [✓]")


test_master_slave_replication()

