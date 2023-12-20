import redis
import sys
import time
import threading
import signal

START_FLAG = True

def enqueue(client: redis.Redis, queue_name: str):
    while START_FLAG:
        now_ms = int(time.time() * 1000)
        pipeline = client.pipeline(transaction=False)
        for i in range(10):
            score = now_ms << 5 | i
            pipeline.zadd(queue_name, {str(i): score})
        pipeline.execute()
    print("enqueue exit")


def dequeue(client: redis.Redis, queue_name: str):
    loop = 0
    while START_FLAG:
        start_time = time.time()
        n = client.zcard(queue_name)
        if n <= 10:
            continue
        res = client.zremrangebyrank(queue_name, 0, 9)
        latency = time.time() - start_time
        loop += 1
        if loop % 20 == 0:
            print("latency: {}ms".format(int(latency * 1000000)/1000))
            loop = 0
    print("dequeue exit")


def compact(client: redis.Redis, queue_name: str):
    loop = 0
    while START_FLAG:
        time.sleep(1)
        loop += 1
        if loop % 60 == 0:
            client.execute_command("compactrange", "db0", "zset", queue_name, queue_name)
            print("compact queue {}".format(queue_name))
            loop = 0
    print("compact exit")


def main():
    if len(sys.argv) != 4:
        print("Usage: python redis_queue.py <redis_host> <port> <passwd>")
        sys.exit(1)
    host = sys.argv[1]
    port = int(sys.argv[2])
    passwd = sys.argv[3]
    client_enqueue = redis.Redis(host=host, port=port, password=passwd)
    client_dequeue = redis.Redis(host=host, port=port, password=passwd)
    client_compact = redis.Redis(host=host, port=port, password=passwd)
    queue_name = "test_queue"
    t1 = threading.Thread(target=enqueue, args=(client_enqueue, queue_name))
    t1.start()
    t2 = threading.Thread(target=dequeue, args=(client_dequeue, queue_name))
    t2.start()
    t3 = threading.Thread(target=compact, args=(client_compact, queue_name))
    t3.start()
    
    def signal_handler(signal, frame):  
        print("revc signal: {}".format(signal))
        global START_FLAG
        START_FLAG = False
        t1.join()
        t2.join()
        t3.join()
        print("exit")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
