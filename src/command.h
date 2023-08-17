/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <map>
#include <vector>
#include "delegate.h"
#include "common.h"
#include "pstring.h"

namespace pikiwidb {

enum PCommandAttr {
  PAttr_read = 0x1,
  PAttr_write = 0x1 << 1,
};

class UnboundedBuffer;
using PCommandHandler = PError(const std::vector<PString>& params, UnboundedBuffer* reply);

// key commands
PCommandHandler type;
PCommandHandler exists;
PCommandHandler del;
PCommandHandler expire;
PCommandHandler pexpire;
PCommandHandler expireat;
PCommandHandler pexpireat;
PCommandHandler ttl;
PCommandHandler pttl;
PCommandHandler persist;
PCommandHandler move;
PCommandHandler keys;
PCommandHandler randomkey;
PCommandHandler rename;
PCommandHandler renamenx;
PCommandHandler scan;
PCommandHandler sort;

// server commands
PCommandHandler select;
PCommandHandler dbsize;
PCommandHandler bgsave;
PCommandHandler save;
PCommandHandler lastsave;
PCommandHandler flushdb;
PCommandHandler flushall;
PCommandHandler client;
PCommandHandler debug;
PCommandHandler shutdown;
PCommandHandler bgrewriteaof;
PCommandHandler ping;
PCommandHandler echo;
PCommandHandler info;
PCommandHandler monitor;
PCommandHandler auth;
PCommandHandler slowlog;
PCommandHandler config;

// string commands
PCommandHandler set;
PCommandHandler get;
PCommandHandler getrange;
PCommandHandler setrange;
PCommandHandler getset;
PCommandHandler append;
PCommandHandler bitcount;
PCommandHandler bitop;
PCommandHandler getbit;
PCommandHandler setbit;
PCommandHandler incr;
PCommandHandler incrby;
PCommandHandler incrbyfloat;
PCommandHandler decr;
PCommandHandler decrby;
PCommandHandler mget;
PCommandHandler mset;
PCommandHandler msetnx;
PCommandHandler setnx;
PCommandHandler setex;
PCommandHandler psetex;
PCommandHandler strlen;

// list commands
PCommandHandler lpush;
PCommandHandler rpush;
PCommandHandler lpushx;
PCommandHandler rpushx;
PCommandHandler lpop;
PCommandHandler rpop;
PCommandHandler lindex;
PCommandHandler llen;
PCommandHandler lset;
PCommandHandler ltrim;
PCommandHandler lrange;
PCommandHandler linsert;
PCommandHandler lrem;
PCommandHandler rpoplpush;
PCommandHandler blpop;
PCommandHandler brpop;
PCommandHandler brpoplpush;

// hash commands
PCommandHandler hget;
PCommandHandler hmget;
PCommandHandler hgetall;
PCommandHandler hset;
PCommandHandler hsetnx;
PCommandHandler hmset;
PCommandHandler hlen;
PCommandHandler hexists;
PCommandHandler hkeys;
PCommandHandler hvals;
PCommandHandler hdel;
PCommandHandler hincrby;
PCommandHandler hincrbyfloat;
PCommandHandler hscan;
PCommandHandler hstrlen;

// set commands
PCommandHandler sadd;
PCommandHandler scard;
PCommandHandler srem;
PCommandHandler sismember;
PCommandHandler smembers;
PCommandHandler sdiff;
PCommandHandler sdiffstore;
PCommandHandler sinter;
PCommandHandler sinterstore;
PCommandHandler sunion;
PCommandHandler sunionstore;
PCommandHandler smove;
PCommandHandler spop;
PCommandHandler srandmember;
PCommandHandler sscan;

// zset
PCommandHandler zadd;
PCommandHandler zcard;
PCommandHandler zrank;
PCommandHandler zrevrank;
PCommandHandler zrem;
PCommandHandler zincrby;
PCommandHandler zscore;
PCommandHandler zrange;
PCommandHandler zrevrange;
PCommandHandler zrangebyscore;
PCommandHandler zrevrangebyscore;
PCommandHandler zremrangebyrank;
PCommandHandler zremrangebyscore;

// pubsub
PCommandHandler subscribe;
PCommandHandler unsubscribe;
PCommandHandler publish;
PCommandHandler psubscribe;
PCommandHandler punsubscribe;
PCommandHandler pubsub;

// multi
PCommandHandler watch;
PCommandHandler unwatch;
PCommandHandler multi;
PCommandHandler exec;
PCommandHandler discard;

// replication
PCommandHandler sync;
PCommandHandler slaveof;
PCommandHandler replconf;

// modules
PCommandHandler module;

// help
PCommandHandler cmdlist;

extern Delegate<void(UnboundedBuffer&)> g_infoCollector;
extern void OnMemoryInfoCollect(UnboundedBuffer&);
extern void OnServerInfoCollect(UnboundedBuffer&);
extern void OnClientInfoCollect(UnboundedBuffer&);

struct PCommandInfo {
  PString cmd;
  int attr = -1;
  int params = -1;
  PCommandHandler* handler = nullptr;
  bool CheckParamsCount(int nParams) const;
};

class PCommandTable {
 public:
  PCommandTable();

  static void Init();

  static const PCommandInfo* GetCommandInfo(const PString& cmd);
  static PError ExecuteCmd(const std::vector<PString>& params, const PCommandInfo* info,
                           UnboundedBuffer* reply = nullptr);
  static PError ExecuteCmd(const std::vector<PString>& params, UnboundedBuffer* reply = nullptr);

  static bool AliasCommand(const std::map<PString, PString>& aliases);
  static bool AliasCommand(const PString& oldKey, const PString& newKey);

  static bool AddCommand(const PString& cmd, const PCommandInfo* info);
  static const PCommandInfo* DelCommand(const PString& cmd);

  friend PCommandHandler cmdlist;

 private:
  static const PCommandInfo s_info[];

  static std::map<PString, const PCommandInfo*, NocaseComp> s_handlers;
};

}  // namespace pikiwidb

