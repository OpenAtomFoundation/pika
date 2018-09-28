# -*- coding: utf-8 -*-
import redis
import string
from random import choice

def getKeys(length=8,chars=string.ascii_letters+string.digits):
    return ''.join([choice(chars) for i in range(length)])

def getValue(length=200,chars=string.ascii_letters+string.digits):
    return ''.join([choice(chars) for i in range(length)])

def setRedis(args):
    r = redis.StrictRedis(host=args[0], port=int(args[1]))
    num = 0
    while int(num) < 5000000:
        num = num + 1

        # key, value
        key1, value1 = "string:transfer:test:" + getKeys(12), getValue(30)
        r.set(key1, value1)

        # list
        l = "list:transfer:test:pika" + getKeys(15)
        r.lpush(l, 1)
        r.lpush(l, 2)
        r.lpush(l, 3)
        r.lpush(l, 4)
        r.lpush(l, 5)
        r.lpush(l, 6)
        r.lpush(l, 7)
        r.lpush(l, 8)
        r.lpush(l, 9)
        r.lpush(l, 10)

        # hashtable
        ht, field, value = "hashtable:transfer:test:pika" + getKeys(12), getKeys(12), getValue(30)
        r.hset(ht, field, value)

        # set
        s, field = "set:transfer:test:pika" + getKeys(12), getKeys(12)
        r.sadd(s, field)
        # r.sadd(s, str(num))

        # zset
        z, field = "transfer:test:pika" + getKeys(12), getKeys(12)
        r.zadd(z, num, field)

if __name__=="__main__":
    setRedis(['10.1.1.1', '10000'])
