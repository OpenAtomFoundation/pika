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

    # z, field = "set:transfer:test:pika:1", getKeys(12)
    # r.zadd(z, 0, field, 1, '1', 2, '2', 3, '3', 4, '4', 5, '5', 6, '6', 7, '7', 8, '8', 9, '9', 10, '10', 11, '11')

    num = 0
    max = 5000000
    while int(num) < int(max):
        num = num + 1

        # key, value
        key, value = "string:transfer:test:" + getKeys(12), getValue(30)
        r.set(key, value)

        # list
        l = "list:transfer:test:pika" + getKeys(15)
        r.lpush(l, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

        # hashtable
        ht, field, value = "hashtable:transfer:test:pika" + getKeys(12), getKeys(12), getValue(30)
        r.hmset(ht, {field : value,
                 field + str(1) : value,
                 field + str(2) : value,
                 field + str(3) : value,
                 field + str(4) : value,
                 field + str(5) : value,
                 field + str(6) : value,
                 field + str(7) : value,
                 field + str(8) : value,
                 field + str(9) : value,
                 field + str(0) : value,
                 field + str(10) : value,
                 field + str(11) : value,
               })

        # set
        s, field = "set:transfer:test:pika" + getKeys(12), getKeys(12)
        r.sadd(s, field, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)

        # zset
        z, field = "zset:transfer:test:pika" + getKeys(12), getKeys(12)
        r.zadd(z, 0, field, 1, '1', 2, '2', 3, '3', 4, '4', 5, '5', 6, '6', 7, '7', 8, '8', 9, '9', 10, '10', 11, '11')

if __name__=="__main__":
    setRedis(['10.1.1.1', '10000'])
