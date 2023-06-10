import threading
import time
import redis
import random
import string

# 全局变量
pika_instance_ip = '192.168.10.22'
pika_instance_port = '9221'


# 单个list不阻塞时的出列顺序测试（行为应当和lpop/rpop一样）
def test_single_existing_list():
    print("start test_single_existing_list")
    # 创建Redis客户端
    pika = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)

    # 清空测试环境
    pika.delete('blist')

    # 向列表a中插入元素
    pika.lpush('blist', 'a', 'b', 'large', 'c', 'd')
    # 此时blist1顺序为: d c large b a

    result = pika.blpop('blist', timeout=0)
    assert result[0] == b'blist' and result[1] == b'd', f"Expected (b'blist1', b'd'), but got {result}"
    result = pika.brpop('blist', timeout=0)
    assert result[0] == b'blist' and result[1] == b'a', f"Expected (b'blist1', b'a'), but got {result}"

    result = pika.blpop("blist", timeout=0)
    assert result[0] == b'blist' and result[1] == b'c', f"Expected (b'blist1', b'c'), but got {result}"
    result = pika.brpop('blist', timeout=0)
    assert result[0] == b'blist' and result[1] == b'b', f"Expected (b'blist1', b'b'), but got {result}"

    pika.close()
    print("test_single_existing_list Passed")


# 解阻塞测试（超时自动解阻塞，lpush解阻塞，rpush解阻塞，rpoplpush解阻塞）
def test_blpop_brpop_unblock_lrpush_rpoplpush():
    print("start test_blpop_brpop_unblock_lrpush_rpoplpush")
    pika = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)

    # 超时自动解阻塞测试(blpop)
    blocked = True
    blocked_lock = threading.Lock()
    pika.delete('blist')

    def blpop_thread1(timeout_):
        nonlocal blocked
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop('blist', timeout=timeout_)
        with blocked_lock:
            blocked = False
        client.close()

    thread = threading.Thread(target=blpop_thread1, args=(5,))
    thread.start()
    time.sleep(5.5)
    with blocked_lock:
        assert blocked == False, f"Expected False but got {blocked}"
    thread.join()

    # 超时自动解阻塞测试(brpop)
    blocked = True
    blocked_lock = threading.Lock()
    pika.delete('blist')

    def brpop_thread2(timeout_):
        nonlocal blocked
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.brpop('blist', timeout=timeout_)
        with blocked_lock:
            blocked = False
        client.close()

    thread = threading.Thread(target=brpop_thread2, args=(3,))
    thread.start()
    time.sleep(3.5)
    with blocked_lock:
        assert blocked == False, f"Expected False but got {blocked}"
    thread.join()

    # lpush解brpop阻塞
    blocked = True
    blocked_lock = threading.Lock()
    pika.delete('blist')

    def brpop_thread3():
        nonlocal blocked
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.brpop('blist', timeout=0)
        with blocked_lock:
            blocked = False
        client.close()

    thread = threading.Thread(target=brpop_thread3)
    thread.start()
    time.sleep(0.2)
    pika.lpush('blist', 'foo')
    time.sleep(0.2)
    with blocked_lock:
        assert blocked == False, f"Expected False but got {blocked}"
    thread.join()

    # lpush解blpop阻塞
    blocked = True
    blocked_lock = threading.Lock()
    pika.delete('blist')

    def blpop_thread31():
        nonlocal blocked
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop('blist', timeout=0)
        with blocked_lock:
            blocked = False
        client.close()

    thread = threading.Thread(target=blpop_thread31)
    thread.start()
    time.sleep(0.2)
    pika.lpush('blist', 'foo')
    time.sleep(0.2)
    with blocked_lock:
        assert blocked == False, f"Expected False but got {blocked}"
    thread.join()

    # rpush解blpop阻塞
    blocked = True
    blocked_lock = threading.Lock()
    pika.delete('blist')

    def blpop_thread4():
        nonlocal blocked
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop('blist', timeout=0)
        with blocked_lock:
            blocked = False
        client.close()

    thread = threading.Thread(target=blpop_thread4)
    thread.start()
    time.sleep(0.2)
    pika.rpush('blist', 'foo')
    time.sleep(0.2)
    with blocked_lock:
        assert blocked == False, f"Expected False but got {blocked}"
    thread.join()

    # rpush解brpop阻塞
    blocked = True
    blocked_lock = threading.Lock()
    pika.delete('blist')

    def brpop_thread41():
        nonlocal blocked
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.brpop('blist', timeout=0)
        with blocked_lock:
            blocked = False
        client.close()

    thread = threading.Thread(target=brpop_thread41)
    thread.start()
    time.sleep(0.2)
    pika.rpush('blist', 'foo')
    time.sleep(0.2)
    with blocked_lock:
        assert blocked == False, f"Expected False but got {blocked}"
    thread.join()

    # rpoplpush解blpop阻塞
    blocked = True
    blocked_lock = threading.Lock()
    pika.delete('blist')
    pika.lpush('blist0', 'v1')

    def blpop_thread5():
        nonlocal blocked
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop('blist', timeout=0)
        with blocked_lock:
            blocked = False
        client.close()

    thread = threading.Thread(target=blpop_thread5)
    thread.start()
    time.sleep(0.2)
    pika.rpoplpush('blist0', 'blist')
    time.sleep(0.2)
    with blocked_lock:
        assert blocked == False, f"Expected False but got {blocked}"
    thread.join()

    # rpoplpush解brpop阻塞
    blocked = True
    blocked_lock = threading.Lock()
    pika.delete('blist')
    pika.lpush('blist0', 'v1')

    def brpop_thread51():
        nonlocal blocked
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.brpop('blist', timeout=0)
        with blocked_lock:
            blocked = False
        client.close()

    thread = threading.Thread(target=brpop_thread51)
    thread.start()
    time.sleep(0.2)
    pika.rpoplpush('blist0', 'blist')
    time.sleep(0.2)
    with blocked_lock:
        assert blocked == False, f"Expected False but got {blocked}"
    thread.join()
    pika.close()
    print("test_blpop_brpop_unblock_lrpush_rpoplpush Passed")


def test_concurrency_block_unblock():
    print("start test_concurrency_block_unblock, it will cost some time, pls wait")
    pika = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
    pika.delete('blist0', 'blist1', 'blist2', 'blist3')

    def blpop_thread(list, timeout_):
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop(list, timeout=timeout_)
        client.close()

    def brpop_thread(list, timeout_):
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop(list, timeout=timeout_)
        client.close()

    def lpush_thread(list_, value_):
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        client.lpush(list_, value_)
        client.close()

    def rpush_thread(list_, value_):
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        client.rpush(list_, value_)
        client.close()

    pika.delete('blist0', 'blist1', 'blist2', 'blist3')
    pika.delete('blist100', 'blist101', 'blist102', 'blist103')

    lists = ['blist0', 'blist1', 'blist2', 'blist3']
    # 先增加一些阻塞连接作为干扰
    t_threads = []
    for i in range(0, 25):
        t1 = threading.Thread(target=blpop_thread, args=(['blist100', 'blist101', 'blist102', 'blist103'], 30))
        t2 = threading.Thread(target=brpop_thread, args=(['blist100', 'blist101', 'blist102', 'blist103'], 30))
        t1.start()
        t2.start()
        t_threads.append(t1)
        t_threads.append(t2)


    # 并发超时测试
    threads = []
    # 添加100个线程执行blpop/brpop,同时被阻塞,并且应当2s后超时自动解阻塞
    for i in range(0, 50):
        t1 = threading.Thread(target=blpop_thread, args=(lists, 2))
        t2 = threading.Thread(target=brpop_thread, args=(lists, 2))
        t1.start()
        t2.start()
        threads.append(t1)
        threads.append(t2)
    # 线程结束需要一些时间
    time.sleep(4)
    for t in threads:
        if t.is_alive():
            assert False, "Error: this thread is still running, means conn didn't got unblocked in time"
        else:
            pass
            # print("conn unblocked, OK")

    # 并发push解阻塞测试
    threads = []
    # 添加100个线程执行blpop/brpop,同时被阻塞
    for i in range(0, 50):
        t1 = threading.Thread(target=blpop_thread, args=(lists, 0))
        t2 = threading.Thread(target=brpop_thread, args=(lists, 0))
        t1.start()
        t2.start()
        threads.append(t1)
        threads.append(t2)
    # 确保线程都执行了blpop/brpop
    time.sleep(5)

    # push 200条数据，确保能解除前面100个conn的阻塞
    for i in range(0, 50):
        t1 = threading.Thread(target=lpush_thread, args=('blist2', 'v'))
        t2 = threading.Thread(target=rpush_thread, args=('blist0', 'v'))
        t3 = threading.Thread(target=lpush_thread, args=('blist1', 'v'))
        t4 = threading.Thread(target=rpush_thread, args=('blist3', 'v'))
        t1.start()
        t2.start()
        t3.start()
        t4.start()
    # 100个线程结束需要时间
    time.sleep(3)
    for t in threads:
        if t.is_alive():
            assert False, "Error: this thread is still running, means conn didn't got unblocked in time"
        else:
            pass
            # print("conn unblocked, OK")

    pika.delete('blist0', 'blist1', 'blist2', 'blist3')

    # 混合并发（一半自动解阻塞，一半push解阻塞）
    threads = []
    # 添加100个线程执行blpop/brpop,同时被阻塞
    for i in range(0, 25):
        t1 = threading.Thread(target=blpop_thread, args=(['blist0', 'blist1'], 3))
        t2 = threading.Thread(target=brpop_thread, args=(['blist0', 'blist1'], 3))
        t3 = threading.Thread(target=blpop_thread, args=(['blist2', 'blist3'], 0))
        t4 = threading.Thread(target=brpop_thread, args=(['blist2', 'blist3'], 0))
        t1.start()
        t2.start()
        t3.start()
        t4.start()
        threads.append(t1)
        threads.append(t2)

    #确保blpop/brpop都执行完了，并且其中50个conn马上要开始超时解除阻塞
    time.sleep(2.7)

    # 并发push 100条数据，确保能解除前面50个conn的阻塞
    for i in range(0, 25):
        t1 = threading.Thread(target=lpush_thread, args=('blist2', 'v'))
        t2 = threading.Thread(target=rpush_thread, args=('blist3', 'v'))
        t3 = threading.Thread(target=lpush_thread, args=('blist2', 'v'))
        t4 = threading.Thread(target=rpush_thread, args=('blist3', 'v'))
        t1.start()
        t2.start()
        t3.start()
        t4.start()

    # 100个线程结束需要时间
    time.sleep(3)
    for t in threads:
        if t.is_alive():
            assert False, "Error: this thread is still running, means conn didn't got unblocked in time"
        else:
            pass
            # print("conn unblocked, OK")


    for t in t_threads:
        t.join();
    pika.delete('blist0', 'blist1', 'blist2', 'blist3')

    print("test_concurrency_block_unblock Passed")
    pika.close()



# blpop/brpop多个list不阻塞时,从左到右选择第一个有元素的list进行pop
def test_multiple_existing_lists():
    print("start test_multiple_existing_lists")
    # 创建Redis客户端
    pika = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)

    # 清空测试环境
    pika.delete('blist1', 'large', 'large', 'blist2')

    # 向blist1和blist2列表中插入元素
    pika.rpush('blist1', 'a', "large", 'c')
    pika.rpush('blist2', 'd', "large", 'f')

    result = pika.blpop(['blist1', 'blist2'], timeout=1)
    assert result[0] == b'blist1' and result[1] == b'a', f"Expected (b'blist1', b'a'), but got {result}"
    result = pika.brpop(['blist1', 'blist2'], timeout=1)
    assert result[0] == b'blist1' and result[1] == b'c', f"Expected (b'blist1', b'c'), but got {result}"

    result = pika.llen('blist1')
    assert result == 1, f"Expected 1, but got {result}"
    result = pika.llen('blist2')
    assert result == 3, f"Expected 3, but got {result}"

    result = pika.blpop(['blist2', 'blist1'], timeout=1)
    assert result[0] == b'blist2' and result[1] == b'd', f"Expected (b'blist2', b'd'), but got {result}"
    result = pika.brpop(['blist2', 'blist1'], timeout=1)
    assert result[0] == b'blist2' and result[1] == b'f', f"Expected (b'blist2', b'f'), but got {result}"

    result = pika.llen('blist1')
    assert result == 1, f"Expected 1, but got {result}"
    result = pika.llen('blist2')
    assert result == 1, f"Expected 1, but got {result}"

    pika.delete("blist3")
    # blist3没有元素，应该从blist1/blist2中弹出元素
    result = pika.blpop(['blist3', 'blist2'], timeout=0)
    assert result[0] == b'blist2' and result[1] == b'large', f"Expected (b'blist2', b'large'), but got {result}"

    result = pika.brpop(['blist3', 'blist1'], timeout=0)
    assert result[0] == b'blist1' and result[1] == b'large', f"Expected (b'blist1', b'large'), but got {result}"

    pika.close()
    print("test_multiple_existing_lists Passed")


def test_blpop_brpop_same_key_multiple_times():
    print("start test_blpop_brpop_same_key_multiple_times")
    # 创建Redis客户端
    pika = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)

    # 清空测试环境
    pika.delete('list1', 'list2')

    def blpop_thread1():
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop(['list1', 'list2', 'list2', 'list1'], timeout=0)
        assert result[0] == b'list1' and result[1] == b'a', f"Expected (b'list1', b'a'), but got {result}"
        client.close()

    thread = threading.Thread(target=blpop_thread1)
    thread.start()
    # 确保BLPOP已经执行
    time.sleep(0.2)
    # 向list1插入元素
    pika.lpush('list1', 'a')
    # 等待线程结束
    thread.join()

    def blpop_thread2():
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop(['list1', 'list2', 'list2', 'list1'], timeout=0)
        assert result[0] == b'list2' and result[1] == b'b', f"Expected (b'list2', b'b'), but got {result}"
        client.close()

    thread = threading.Thread(target=blpop_thread2)
    thread.start()
    # 确保BLPOP已经执行
    time.sleep(0.2)
    # 向list2插入元素
    pika.lpush('list2', 'b')
    # 等待线程结束
    thread.join()

    # 提前插入元素
    pika.lpush('list1', 'c')
    pika.lpush('list2', 'd')
    result = pika.blpop(['list1', 'list2', 'list2', 'list1'], timeout=0)
    assert result[0] == b'list1' and result[1] == b'c', f"Expected (b'list1', b'c'), but got {result}"
    result = pika.blpop(['list1', 'list2', 'list2', 'list1'], timeout=0)
    assert result[0] == b'list2' and result[1] == b'd', f"Expected (b'list2', b'd'), but got {result}"

    # 下面是brpop
    # 清空测试环境
    pika.delete('list1', 'list2')

    def brpop_thread1():
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.brpop(['list1', 'list2', 'list2', 'list1'], timeout=0)
        assert result[0] == b'list1' and result[1] == b'a', f"Expected (b'list1', b'a'), but got {result}"
        client.close()

    thread = threading.Thread(target=brpop_thread1)
    thread.start()
    # 确保BRPOP已经执行
    time.sleep(0.2)
    # 向list1插入元素
    pika.rpush('list1', 'a')
    # 等待线程结束
    thread.join()

    def brpop_thread2():
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.brpop(['list1', 'list2', 'list2', 'list1'], timeout=0)
        assert result[0] == b'list2' and result[1] == b'b', f"Expected (b'list2', b'b'), but got {result}"
        client.close()

    thread = threading.Thread(target=brpop_thread2)
    thread.start()
    # 确保BRPOP已经执行
    time.sleep(0.2)
    # 向list2插入元素
    pika.rpush('list2', 'b')
    # 等待线程结束
    thread.join()

    # 提前插入元素
    pika.rpush('list1', 'c')
    pika.rpush('list2', 'd')
    result = pika.brpop(['list1', 'list2', 'list2', 'list1'], timeout=0)
    assert result[0] == b'list1' and result[1] == b'c', f"Expected (b'list1', b'c'), but got {result}"
    result = pika.brpop(['list1', 'list2', 'list2', 'list1'], timeout=0)
    assert result[0] == b'list2' and result[1] == b'd', f"Expected (b'list2', b'd'), but got {result}"

    pika.close()
    print("test_blpop_brpop_same_key_multiple_times Passed")


# 目标list被一条push增加了多个value，先完成多个value的入列再pop
def test_blpop_brpop_variadic_lpush():
    print("start test_blpop_brpop_variadic_lpush")

    # 创建Redis客户端
    pika = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)

    # 清空测试环境
    pika.delete('blist')

    def blpop_thread():
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop('blist', timeout=0)
        assert result[0] == b'blist' and result[1] == b'bar', f"Expected (b'blist', b'bar'), but got {result}"
        client.close()

    # 启动一个线程，执行BLPOP操作
    thread = threading.Thread(target=blpop_thread)
    thread.start()
    time.sleep(0.2)

    # 使用LPUSH命令向blist插入多个元素
    pika.lpush('blist', 'foo', 'bar')
    # lpush完毕后，blist内部顺序：bar foo
    # 等待线程结束
    thread.join()
    # 检查blist的第一个元素
    assert pika.lindex('blist', 0) == b'foo', "Expected 'foo'"

    # 下面是brpop的测试
    # 清空测试环境
    pika.delete('blist')

    def brpop_thread():
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.brpop('blist', timeout=0)
        assert result[0] == b'blist' and result[1] == b'bar', f"Expected (b'blist', b'bar'), but got {result}"
        client.close()

    # 启动一个线程，执行BLPOP操作
    thread = threading.Thread(target=brpop_thread)
    thread.start()
    time.sleep(0.2)

    # 使用LPUSH命令向blist插入多个元素
    pika.rpush('blist', 'foo', 'bar')
    # rpush完毕后，blist内部顺序：foo bar
    # 等待线程结束
    thread.join()
    # 检查blist的第一个元素
    assert pika.lindex('blist', 0) == b'foo', "Expected 'foo'"
    print("test_blpop_brpop_variadic_lpush Passed")


# 先被阻塞的先服务/阻塞最久的优先级最高
def test_serve_priority():
    print("start test_serve_priority")

    pika = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)

    pika.delete('blist')

    def blpop_thread(expect):
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.blpop('blist', timeout=0)
        assert result[0] == b'blist' and result[1] == expect, f"Expected (b'blist', {expect}), but got {result}"

    def brpop_thread(expect):
        client = redis.Redis(host=pika_instance_ip, port=int(pika_instance_port), db=0)
        result = client.brpop('blist', timeout=0)
        assert result[0] == b'blist' and result[1] == expect, f"Expected (b'blist', {expect}), but got {result}"

    # blpop测试
    t1 = threading.Thread(target=blpop_thread, args=(b'v1',))
    t1.start()
    time.sleep(0.2)
    t2 = threading.Thread(target=blpop_thread, args=(b'v2',))
    t2.start()
    time.sleep(0.2)
    pika.rpush('blist', 'v1', 'v2')
    t1.join()
    t2.join()

    # brpop测试
    t3 = threading.Thread(target=brpop_thread, args=(b'v4',))
    t3.start()
    time.sleep(0.2)
    t4 = threading.Thread(target=brpop_thread, args=(b'v3',))
    t4.start()
    time.sleep(0.2)
    pika.rpush('blist', 'v3', 'v4')

    t3.join()
    t4.join()

    pika.close()
    print("test_serve_priority Passed")


# 主从复制测试
def test_master_slave_replication():
    print("start test_master_slave_replication")

    master_ip = '192.168.10.22'
    master_port = '9221'
    slave_ip = '192.168.10.22'
    slave_port = '9231'

    master = redis.Redis(host=master_ip, port=int(master_port), db=0)
    slave = redis.Redis(host=slave_ip, port=int(slave_port), db=0)
    slave.slaveof(master_ip, master_port)
    master.delete('blist0', 'blist1', 'blist')

    time.sleep(1)
    m_keys = master.keys()
    s_keys = slave.keys()
    assert s_keys == m_keys, f'Expected: s_keys == m_keys, but got {s_keys == m_keys}'

    def thread1():
        nonlocal master
        for i in range(0, 40):
            letters = string.ascii_letters
            random_str1 = ''.join(random.choice(letters) for _ in range(5))
            random_str2 = ''.join(random.choice(letters) for _ in range(5))
            random_str3 = ''.join(random.choice(letters) for _ in range(5))
            master.lpush('blist0', random_str1)
            master.rpoplpush('blist0', 'blist')
            master.lpush('blist', random_str1, random_str2, random_str3)

            master.lpop('blist')
            master.rpop('blist')
            master.rpush('blist', random_str3, random_str2, random_str1)
            master.lpop('blist')
            master.rpop('blist')

    t1 = threading.Thread(target=thread1)
    t2 = threading.Thread(target=thread1)
    t3 = threading.Thread(target=thread1)
    t4 = threading.Thread(target=thread1)
    t5 = threading.Thread(target=thread1)
    t6 = threading.Thread(target=thread1)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    time.sleep(3)
    m_keys = master.keys()
    s_keys = slave.keys()
    assert s_keys == m_keys, f'Expected: s_keys == m_keys, but got {s_keys == m_keys}'

    for i in range(0, master.llen('blist')):
        print(master.lindex('blist', i))
        print(slave.lindex('blist', i))
        assert master.lindex('blist', i) == slave.lindex('blist', i), \
            f"Expected:master.lindex('blist', i) == slave.linex('blist', i), but got False when i = {i}"

    master.delete('blist0', 'blist1')

    # def blpop_thread():
    #     client = redis.Redis(host=master_ip, port=int(master_port), db=0)
    #     result = client.blpop(['blist0', 'blist1'], timeout=0)
    #     assert result[0] == b'blist1' and result[1] == b'v1', f"Expected: (blist1, v1), but got  = {result}"
    #     client.close()
    #
    # def brpop_thread():
    #     client = redis.Redis(host=master_ip, port=int(master_port), db=0)
    #     result = client.brpop(['blist0', 'blist1'], timeout=0)
    #     assert result[0] == b'blist0' and result[1] == b'v4', f"Expected: (blist0, v4), but got  = {result}"
    #     client.close()
    #
    # t1 = threading.Thread(target=blpop_thread)
    # t2 = threading.Thread(target=brpop_thread)
    # t1.start()
    # time.sleep(0.2)


    # t2.start()
    # time.sleep(0.2)
    # master.lpush('blist1', 'v1')
    # master.rpush('blist0', 'v2', 'v3', 'v4')
    #
    # t1.join()
    # t2.join()
    # time.sleep(1)
    # m_keys = master.keys()
    # s_keys = slave.keys()
    # assert s_keys == m_keys, f'Expected: s_keys == m_keys, but got {s_keys == m_keys}'
    # for i in range(0, master.llen('blist0')):
    #     print(master.lindex('blist0', i))
    #     print(slave.lindex('blist0', i))
    #     assert master.lindex('blist0', i) == slave.lindex('blist0', i), \
    #         f"Expected:master.lindex('blist0', i) == slave.linex('blist0', i), but got False when i = {i}"

    master.close()
    slave.close()
    print("test_master_slave_replication Passed")




test_single_existing_list()
test_blpop_brpop_unblock_lrpush_rpoplpush()
test_concurrency_block_unblock()
test_multiple_existing_lists()
test_blpop_brpop_same_key_multiple_times()
test_blpop_brpop_variadic_lpush()
test_serve_priority()
    # test_master_slave_replication()


# 待添加的测试：
# 事务与blpop/brpop
#     1 事务内执行blpop/brpop如果没有获取到元素不阻塞，直接返回
#     2 "BLPOP, LPUSH + DEL should not awake blocked client": 在事务内对一个空list进行了push后又del，当事务结束时list如果依旧是空的，则不应该去服务被阻塞的客户端（事务内的lpush不触发解阻塞动作，而是事务结束才做这个行为
# redis单测逻辑如下
#    test "BLPOP, LPUSH + DEL should not awake blocked client" {
#        set rd [redis_deferring_client]
#        r del list
#
#        $rd blpop list 0
#        r multi
#        r lpush list a
#        r del list
#        r exec
#        r del list
#        r lpush list b
#        $rd read
#    } {list b}

#     3 "BLPOP, LPUSH + DEL + SET should not awake blocked client": 这个测试用例与上一个类似，但在删除列表后，还使用SET命令将这个列表设置为一个字符串。
# redis单测逻辑如下
#    test "BLPOP, LPUSH + DEL + SET should not awake blocked client" {
#        set rd [redis_deferring_client]
#        r del list
#
#        $rd blpop list 0
#        r multi
#        r lpush list a
#        r del list
#        r set list foo
#        r exec
#        r del list
#        r lpush list b
#        $rd read
#    } {list b}

#     4 "MULTI/EXEC is isolated from the point of view of BLPOP": 这个测试用例检查了在使用BLPOP命令阻塞等待一个列表的元素时，如果在此期间在一个Redis事务中向这个列表推入多个元素，阻塞的客户端应该只会接收到事务执行前的列表状态。
# redis单测逻辑如下
#    test "MULTI/EXEC is isolated from the point of view of BLPOP" {
#        set rd [redis_deferring_client]
#        r del list
#        $rd blpop list 0
#        r multi
#        r lpush list a
#        r lpush list b
#        r lpush list c
#        r exec
#        $rd read
#    } {list c}
