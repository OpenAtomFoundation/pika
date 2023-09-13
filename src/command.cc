/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "command.h"
#include "replication.h"

using std::size_t;

namespace pikiwidb {

const PCommandInfo PCommandTable::s_info[] = {
    // key
    {"type", PAttr_read, 2, &type},
    {"exists", PAttr_read, 2, &exists},
    {"del", PAttr_write, -2, &del},
    {"expire", PAttr_read, 3, &expire},
    {"ttl", PAttr_read, 2, &ttl},
    {"pexpire", PAttr_read, 3, &pexpire},
    {"pttl", PAttr_read, 2, &pttl},
    {"expireat", PAttr_read, 3, &expireat},
    {"pexpireat", PAttr_read, 3, &pexpireat},
    {"persist", PAttr_read, 2, &persist},
    {"move", PAttr_write, 3, &move},
    {"keys", PAttr_read, 2, &keys},
    {"randomkey", PAttr_read, 1, &randomkey},
    {"rename", PAttr_write, 3, &rename},
    {"renamenx", PAttr_write, 3, &renamenx},
    {"scan", PAttr_read, -2, &scan},
    {"sort", PAttr_read, -2, &sort},

    // server
    {"select", PAttr_read, 2, &select},
    {"dbsize", PAttr_read, 1, &dbsize},
    {"bgsave", PAttr_read, 1, &bgsave},
    {"save", PAttr_read, 1, &save},
    {"lastsave", PAttr_read, 1, &lastsave},
    {"flushdb", PAttr_write, 1, &flushdb},
    {"flushall", PAttr_write, 1, &flushall},
    {"client", PAttr_read, -2, &client},
    {"debug", PAttr_read, -2, &debug},
    {"shutdown", PAttr_read, -1, &shutdown},
    {"ping", PAttr_read, 1, &ping},
    {"echo", PAttr_read, 2, &echo},
    {"info", PAttr_read, -1, &info},
    {"monitor", PAttr_read, 1, &monitor},
    {"auth", PAttr_read, 2, &auth},
    {"slowlog", PAttr_read, -2, &slowlog},
    // {"config", PAttr_read, -3, &config},

    // string
    {"strlen", PAttr_read, 2, &strlen},
    {"mset", PAttr_write, -3, &mset},
    {"msetnx", PAttr_write, -3, &msetnx},
    {"setnx", PAttr_write, 3, &setnx},
    {"setex", PAttr_write, 4, &setex},
    {"psetex", PAttr_write, 4, &psetex},
    {"getset", PAttr_write, 3, &getset},
    {"mget", PAttr_read, -2, &mget},
    {"append", PAttr_write, 3, &append},
    {"bitcount", PAttr_read, -2, &bitcount},
    {"bitop", PAttr_write, -4, &bitop},
    {"getbit", PAttr_read, 3, &getbit},
    {"setbit", PAttr_write, 4, &setbit},
    {"incr", PAttr_write, 2, &incr},
    {"decr", PAttr_write, 2, &decr},
    {"incrby", PAttr_write, 3, &incrby},
    {"incrbyfloat", PAttr_write, 3, &incrbyfloat},
    {"decrby", PAttr_write, 3, &decrby},
    {"getrange", PAttr_read, 4, &getrange},
    {"setrange", PAttr_write, 4, &setrange},

    // list
    {"lpush", PAttr_write, -3, &lpush},
    {"rpush", PAttr_write, -3, &rpush},
    {"lpushx", PAttr_write, -3, &lpushx},
    {"rpushx", PAttr_write, -3, &rpushx},
    {"lpop", PAttr_write, 2, &lpop},
    {"rpop", PAttr_write, 2, &rpop},
    {"lindex", PAttr_read, 3, &lindex},
    {"llen", PAttr_read, 2, &llen},
    {"lset", PAttr_write, 4, &lset},
    {"ltrim", PAttr_write, 4, &ltrim},
    {"lrange", PAttr_read, 4, &lrange},
    {"linsert", PAttr_write, 5, &linsert},
    {"lrem", PAttr_write, 4, &lrem},
    {"rpoplpush", PAttr_write, 3, &rpoplpush},
    {"blpop", PAttr_write, -3, &blpop},
    {"brpop", PAttr_write, -3, &brpop},
    {"brpoplpush", PAttr_write, 4, &brpoplpush},

    // hash
    {"hget", PAttr_read, 3, &hget},
    {"hgetall", PAttr_read, 2, &hgetall},
    {"hmget", PAttr_read, -3, &hmget},
    {"hset", PAttr_write, 4, &hset},
    {"hsetnx", PAttr_write, 4, &hsetnx},
    {"hmset", PAttr_write, -4, &hmset},
    {"hlen", PAttr_read, 2, &hlen},
    {"hexists", PAttr_read, 3, &hexists},
    {"hkeys", PAttr_read, 2, &hkeys},
    {"hvals", PAttr_read, 2, &hvals},
    {"hdel", PAttr_write, -3, &hdel},
    {"hincrby", PAttr_write, 4, &hincrby},
    {"hincrbyfloat", PAttr_write, 4, &hincrbyfloat},
    {"hscan", PAttr_read, -3, &hscan},
    {"hstrlen", PAttr_read, 3, &hstrlen},

    // set
    {"sadd", PAttr_write, -3, &sadd},
    {"scard", PAttr_read, 2, &scard},
    {"sismember", PAttr_read, 3, &sismember},
    {"srem", PAttr_write, -3, &srem},
    {"smembers", PAttr_read, 2, &smembers},
    {"sdiff", PAttr_read, -2, &sdiff},
    {"sdiffstore", PAttr_write, -3, &sdiffstore},
    {"sinter", PAttr_read, -2, &sinter},
    {"sinterstore", PAttr_write, -3, &sinterstore},
    {"sunion", PAttr_read, -2, &sunion},
    {"sunionstore", PAttr_write, -3, &sunionstore},
    {"smove", PAttr_write, 4, &smove},
    {"spop", PAttr_write, 2, &spop},
    {"srandmember", PAttr_read, 2, &srandmember},
    {"sscan", PAttr_read, -3, &sscan},

    // zset
    {"zadd", PAttr_write, -4, &zadd},
    {"zcard", PAttr_read, 2, &zcard},
    {"zrank", PAttr_read, 3, &zrank},
    {"zrevrank", PAttr_read, 3, &zrevrank},
    {"zrem", PAttr_write, -3, &zrem},
    {"zincrby", PAttr_write, 4, &zincrby},
    {"zscore", PAttr_read, 3, &zscore},
    {"zrange", PAttr_read, -4, &zrange},
    {"zrevrange", PAttr_read, -4, &zrevrange},
    {"zrangebyscore", PAttr_read, -4, &zrangebyscore},
    {"zrevrangebyscore", PAttr_read, -4, &zrevrangebyscore},
    {"zremrangebyrank", PAttr_write, 4, &zremrangebyrank},
    {"zremrangebyscore", PAttr_write, 4, &zremrangebyscore},

    // pubsub
    {"subscribe", PAttr_read, -2, &subscribe},
    {"unsubscribe", PAttr_read, -1, &unsubscribe},
    {"publish", PAttr_read, 3, &publish},
    {"psubscribe", PAttr_read, -2, &psubscribe},
    {"punsubscribe", PAttr_read, -1, &punsubscribe},
    {"pubsub", PAttr_read, -2, &pubsub},

    // multi
    {"watch", PAttr_read, -2, &watch},
    {"unwatch", PAttr_read, 1, &unwatch},
    {"multi", PAttr_read, 1, &multi},
    {"exec", PAttr_read, 1, &exec},
    {"discard", PAttr_read, 1, &discard},

    // replication
    {"sync", PAttr_read, 1, &sync},
    {"psync", PAttr_read, 1, &sync},
    {"slaveof", PAttr_read, 3, &slaveof},
    {"replconf", PAttr_read, -3, &replconf},

    // help
    {"cmdlist", PAttr_read, 1, &cmdlist},
};

Delegate<void(UnboundedBuffer&)> g_infoCollector;

std::map<PString, const PCommandInfo*, NocaseComp> PCommandTable::s_handlers;

PCommandTable::PCommandTable() { Init(); }

void PCommandTable::Init() {
  for (const auto& info : s_info) {
    s_handlers[info.cmd] = &info;
  }

  g_infoCollector += OnMemoryInfoCollect;
  g_infoCollector += OnServerInfoCollect;
  g_infoCollector += OnClientInfoCollect;
  g_infoCollector += std::bind(&PReplication::OnInfoCommand, &PREPL, std::placeholders::_1);
}

const PCommandInfo* PCommandTable::GetCommandInfo(const PString& cmd) {
  auto it(s_handlers.find(cmd));
  if (it != s_handlers.end()) {
    return it->second;
  }

  return nullptr;
}

bool PCommandTable::AliasCommand(const std::map<PString, PString>& aliases) {
  for (const auto& pair : aliases) {
    if (!AliasCommand(pair.first, pair.second)) {
      return false;
    }
  }

  return true;
}

bool PCommandTable::AliasCommand(const PString& oldKey, const PString& newKey) {
  auto info = DelCommand(oldKey);
  if (!info) {
    return false;
  }

  return AddCommand(newKey, info);
}

const PCommandInfo* PCommandTable::DelCommand(const PString& cmd) {
  auto it(s_handlers.find(cmd));
  if (it != s_handlers.end()) {
    auto p = it->second;
    s_handlers.erase(it);
    return p;
  }

  return nullptr;
}

bool PCommandTable::AddCommand(const PString& cmd, const PCommandInfo* info) {
  if (cmd.empty() || cmd == "\"\"") {
    return true;
  }

  return s_handlers.insert(std::make_pair(cmd, info)).second;
}

PError PCommandTable::ExecuteCmd(const std::vector<PString>& params, const PCommandInfo* info, UnboundedBuffer* reply) {
  if (params.empty()) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  if (!info) {
    ReplyError(PError_unknowCmd, reply);
    return PError_unknowCmd;
  }

  if (!info->CheckParamsCount(static_cast<int>(params.size()))) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  return info->handler(params, reply);
}

PError PCommandTable::ExecuteCmd(const std::vector<PString>& params, UnboundedBuffer* reply) {
  if (params.empty()) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  auto it(s_handlers.find(params[0]));
  if (it == s_handlers.end()) {
    ReplyError(PError_unknowCmd, reply);
    return PError_unknowCmd;
  }

  const PCommandInfo* info = it->second;
  if (!info->CheckParamsCount(static_cast<int>(params.size()))) {
    ReplyError(PError_param, reply);
    return PError_param;
  }

  return info->handler(params, reply);
}

bool PCommandInfo::CheckParamsCount(int nParams) const {
  if (params > 0) {
    return params == nParams;
  }

  return nParams + params >= 0;
}

PError cmdlist(const std::vector<PString>& params, UnboundedBuffer* reply) {
  PreFormatMultiBulk(PCommandTable::s_handlers.size(), reply);
  for (const auto& kv : PCommandTable::s_handlers) {
    FormatBulk(kv.first, reply);
  }

  return PError_ok;
}

}  // namespace pikiwidb
