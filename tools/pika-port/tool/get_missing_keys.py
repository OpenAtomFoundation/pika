import redis
import sys

"""print help"""
def printHelp():
    """ print help prompt
    """
    print 'Output redis missing keys that exists in pika:'
    print 'usage:'
    print '  example: ./get_missing_keys.py pika-host pika-port redis-host redis-port'

def scanKey(_pikaClient, _redisClient):
    num = 0
    n, l = _pikaClient.scan(0, count=500)
    while True:
        for i in l:
            s = _redisClient.exists(i)
            if s == False:
                print i
        if n == 0:
            return
        n,l = _pikaClient.scan(n, count=500)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        printHelp()
        sys.exit(1)

    pikaHost = sys.argv[1]
    pikaPort = int(sys.argv[2])
    redisHost = sys.argv[3]
    redisPort = int(sys.argv[4])
    pikaClient = redis.Redis(host=pikaHost, port=int(pikaPort))
    redisClient = redis.Redis(host=redisHost, port=int(redisPort))
    scanKey(pikaClient, redisClient)

