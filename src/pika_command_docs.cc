// Copyright (c) 2023-present, Qihoo, Inc. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "include/pika_admin.h"

static CommandCmd::EncodablePtr operator""_RedisInt(unsigned long long value) {
  return std::make_shared<CommandCmd::EncodableInt>(value);
}

static CommandCmd::EncodablePtr operator""_RedisString(const char* value,
                                                       std::size_t length) {
  return std::make_shared<CommandCmd::EncodableString>(
      std::string(value, length));
}

static CommandCmd::EncodablePtr operator""_RedisStatus(const char* value,
                                                       std::size_t length) {
  return std::make_shared<CommandCmd::EncodableStatus>(
      std::string(value, length));
}

static CommandCmd::EncodablePtr RedisMap(
    CommandCmd::EncodableMap::RedisMap values) {
  return std::make_shared<CommandCmd::EncodableMap>(std::move(values));
}

static CommandCmd::EncodablePtr RedisSet(
    std::vector<CommandCmd::EncodablePtr> values) {
  return std::make_shared<CommandCmd::EncodableSet>(std::move(values));
}

static CommandCmd::EncodablePtr RedisArray(
    std::vector<CommandCmd::EncodablePtr> values) {
  return std::make_shared<CommandCmd::EncodableArray>(std::move(values));
}

const std::string CommandCmd::kPikaField{"pika"};
const CommandCmd::EncodablePtr CommandCmd::kNotSupportedLiteral =
    "当前还未支持"_RedisString;
const CommandCmd::EncodablePtr CommandCmd::kCompatibleLiteral =
    "该接口完全支持，使用方式与redis没有任何区别"_RedisString;
const CommandCmd::EncodablePtr CommandCmd::kBitSpecLiteral =
    "BIT操作：与Redis不同，Pika的bit操作范围为2^21， bitmap的最大值为256Kb。redis setbit 只是对key的value值更新。但是pika使用rocksdb作为存储引擎，rocksdb只会新写入数据并且只在compact的时候才从硬盘删除旧数据。如果pika的bit操作范围和redis一致都是2^32的话，那么有可能每次对同一个key setbit时，rocksdb都会存储一个512M大小的value。这会产生 严重的性能隐患。因此我们对pika的bit操作范围作了取舍。"_RedisString;
const CommandCmd::EncodablePtr CommandCmd::kHyperLogLiteral =
    "50w以内误差均小于1%, 100w以内误差小于3%, 但付出了时间代价."_RedisString;
const CommandCmd::EncodablePtr CommandCmd::kPubSubLiteral =
    "暂不支持keyspace notifications"_RedisString;

const CommandCmd::EncodablePtr CommandCmd::kNotSupportedSpecialization =
    RedisMap({{kPikaField, kNotSupportedLiteral}});
const CommandCmd::EncodablePtr CommandCmd::kCompatibleSpecialization =
    RedisMap({{kPikaField, kCompatibleLiteral}});
const CommandCmd::EncodablePtr CommandCmd::kBitSpecialization =
    RedisMap({{kPikaField, kBitSpecLiteral}});
const CommandCmd::EncodablePtr CommandCmd::kHyperLogSpecialization =
    RedisMap({{kPikaField, kHyperLogLiteral}});
const CommandCmd::EncodablePtr CommandCmd::kPubSubSpecialization =
    RedisMap({{kPikaField, kPubSubLiteral}});

const std::
    unordered_map<std::string, CommandCmd::EncodablePtr>
        CommandCmd::
            kPikaSpecialization{
                {"pexpire",
                 RedisMap(
                     {{kPikaField,
                       "无法精确到毫秒，底层会自动截断按秒级别进行处理"_RedisString}})},
                {"pexpireat",
                 RedisMap(
                     {{kPikaField,
                       "无法精确到毫秒，底层会自动截断按秒级别进行处理"_RedisString}})},
                {"scan",
                 RedisMap(
                     {{kPikaField,
                       "会顺序迭代当前db的快照，由于pika允许重名五次，所以scan有优先输出顺序，依次为：string -> hash -> list -> zset -> set"_RedisString}})},
                {"type",
                 RedisMap(
                     {{kPikaField,
                       "另外由于pika允许重名五次，所以type有优先输出顺序，依次为：string -> hash -> list -> zset -> set，如果这个key在string中存在，那么只输出sting，如果不存在，那么则输出hash的，依次类推"_RedisString}})},
                {"keys",
                 RedisMap(
                     {{kPikaField,
                       "KEYS命令支持参数支持扫描指定类型的数据，用法如 \"keys * [string, hash, list, zset, set]\""_RedisString}})},
                {"bitop", kBitSpecialization},
                {"getbit", kBitSpecialization},
                {"setbit", kBitSpecialization},
                {"hset",
                 RedisMap(
                     {{kPikaField,
                       "暂不支持单条命令设置多个field value，如有需求请用HMSET"_RedisString}})},
                {"srandmember",
                 RedisMap(
                     {{kPikaField, "时间复杂度O( n )，耗时较多"_RedisString}})},
                {"zadd",
                 RedisMap(
                     {{kPikaField,
                       "的选项 [NX|XX] [CH] [INCR] 暂不支持"_RedisString}})},
                {"pfadd", kHyperLogSpecialization},
                {"pfcount", kHyperLogSpecialization},
                {"pfmerge", kHyperLogSpecialization},
                {"psubscribe", kPubSubSpecialization},
                {"pubsub", kPubSubSpecialization},
                {"publish", kPubSubSpecialization},
                {"punsubscribe", kPubSubSpecialization},
                {"subscribe", kPubSubSpecialization},
                {"unsubscribe", kPubSubSpecialization},
                {"info", RedisMap({{kPikaField,
                                    "info支持全部输出，也支持匹配形式的输出，例如可以通过info stats查看状态信息，需要注意的是key space与redis不同，pika对于key space的展示选择了分类型展示而非redis的分库展示（因为pika没有库），pika对于key space的统计是被动的，需要手动触发，然后pika会在后台进行统计，pika的key space统计是精确的。触发方式为执行：keyspace命令即可，然后pika会在后台统计，此时可以使用：keyspace readonly命令来进行查看，readonly参数可以避免反复进行统计，如果当前数据为0，则证明还在统计中"_RedisString}})},
                {"client",
                 RedisMap(
                     {{kPikaField,
                       "当前client命令支持client list及client kill，client list显示的内容少于redis"_RedisString}})},
                {"select",
                 RedisMap(
                     {{kPikaField,
                       "该命令在3.1.0版前无任何效果，自3.1.0版开始与Redis一致"_RedisString}})},
                {"ping",
                 RedisMap(
                     {{kPikaField,
                       "该命令仅支持无参数使用，即使用PING，客户端返回PONG"_RedisString}})},
                {"type",
                 RedisMap(
                     {{kPikaField,
                       "pika不同类型的key name 是允许重复的，例如：string 类型里有 key1，hash list set zset类型可以同时存在 key1，在使用 type命令查询时，只能得到一个，如果要查询同一个 name 所有的类型，需要使用 ptype 命令查询"_RedisString}})},
            };

const std::unordered_map<std::string, CommandCmd::EncodablePtr> CommandCmd::kCommandDocs{
    {"zremrangebyscore",
     RedisMap({
         {"summary",
          "Removes members in a sorted set within a range of scores. Deletes the sorted set if all members were removed."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "min"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "min"_RedisString},
                           }),
                           RedisMap({
                               {"name", "max"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "max"_RedisString},
                           }),
                       })},
     })},
    {"sunion",
     RedisMap({
         {"summary", "Returns the union of multiple sets."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(N) where N is the total number of elements in all given sets."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"debug",
     RedisMap({
         {"summary", "A container for debugging commands."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"doc_flags", RedisSet({
                           "syscmd"_RedisStatus,
                       })},
     })},
    {"readonly",
     RedisMap({
         {"summary",
          "Enables read-only queries for a connection to a Redis Cluster replica node."_RedisString},
         {"since", "3.0.0"_RedisString},
         {"group", "cluster"_RedisString},
         {"complexity", "O(1)"_RedisString},
     })},
    {"latency",
     RedisMap({
         {"summary",
          "A container for latency diagnostics commands."_RedisString},
         {"since", "2.8.13"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"latency|doctor",
               RedisMap({
                   {"summary",
                    "Returns a human-readable latency analysis report."_RedisString},
                   {"since", "2.8.13"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"latency|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "2.8.13"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"latency|histogram",
               RedisMap({
                   {"summary",
                    "Returns the cumulative distribution of latencies of a subset or all commands."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of commands with latency information being retrieved."_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "command"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "command"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"latency|history",
               RedisMap({
                   {"summary",
                    "Returns timestamp-latency samples for an event."_RedisString},
                   {"since", "2.8.13"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "event"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "event"_RedisString},
                                     }),
                                 })},
               })},
              {"latency|graph",
               RedisMap({
                   {"summary",
                    "Returns a latency graph for an event."_RedisString},
                   {"since", "2.8.13"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "event"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "event"_RedisString},
                                     }),
                                 })},
               })},
              {"latency|latest",
               RedisMap({
                   {"summary",
                    "Returns the latest latency samples for all events."_RedisString},
                   {"since", "2.8.13"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"latency|reset",
               RedisMap({
                   {"summary",
                    "Resets the latency data for one or more events."_RedisString},
                   {"since", "2.8.13"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "event"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "event"_RedisString},
                                         {"flags", RedisArray({
                                                       "optional"_RedisStatus,
                                                       "multiple"_RedisStatus,
                                                   })},
                                     }),
                                 })},
               })},
          })},
     })},
    {"setbit",
     RedisMap({
         {"summary",
          "Sets or clears the bit at offset of the string value. Creates the key if it doesn't exist."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "bitmap"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "offset"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "offset"_RedisString},
                           }),
                           RedisMap({
                               {"name", "value"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "value"_RedisString},
                           }),
                       })},
     })},
    {"lpush",
     RedisMap({
         {"summary",
          "Prepends one or more elements to a list. Creates the key if it doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"2.4.0"_RedisString,
                          "Accepts multiple `element` arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "element"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "element"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"punsubscribe",
     RedisMap({
         {"summary",
          "Stops listening to messages published to channels that match one or more patterns."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "pubsub"_RedisString},
         {"complexity",
          "O(N+M) where N is the number of patterns the client is already subscribed and M is the number of total patterns subscribed in the system (by any client)."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "pattern"_RedisString},
                               {"type", "pattern"_RedisString},
                               {"display_text", "pattern"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"role", RedisMap({
                 {"summary", "Returns the replication role."_RedisString},
                 {"since", "2.8.12"_RedisString},
                 {"group", "server"_RedisString},
                 {"complexity", "O(1)"_RedisString},
             })},
    {"lmove",
     RedisMap({
         {"summary",
          "Returns an element after popping it from one list and pushing it to another. Deletes the list if the last element was moved."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "source"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "source"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "destination"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "destination"_RedisString},
                  {"key_spec_index", 1_RedisInt},
              }),
              RedisMap({
                  {"name", "wherefrom"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "left"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "left"_RedisString},
                                        {"token", "LEFT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "right"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "right"_RedisString},
                                        {"token", "RIGHT"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "whereto"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "left"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "left"_RedisString},
                                        {"token", "LEFT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "right"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "right"_RedisString},
                                        {"token", "RIGHT"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"memory",
     RedisMap({
         {"summary",
          "A container for memory diagnostics commands."_RedisString},
         {"since", "4.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"memory|doctor",
               RedisMap({
                   {"summary", "Outputs a memory problems report."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"memory|malloc-stats",
               RedisMap({
                   {"summary", "Returns the allocator statistics."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "Depends on how much memory is allocated, could be slow"_RedisString},
               })},
              {"memory|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"memory|purge",
               RedisMap({
                   {"summary",
                    "Asks the allocator to release memory."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "Depends on how much memory is allocated, could be slow"_RedisString},
               })},
              {"memory|stats",
               RedisMap({
                   {"summary",
                    "Returns details about memory usage."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"memory|usage",
               RedisMap({
                   {"summary",
                    "Estimates the memory usage of a key."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of samples."_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "key"_RedisString},
                                         {"display_text", "key"_RedisString},
                                         {"key_spec_index", 0_RedisInt},
                                     }),
                                     RedisMap({
                                         {"name", "count"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "count"_RedisString},
                                         {"token", "SAMPLES"_RedisString},
                                         {"flags", RedisArray({
                                                       "optional"_RedisStatus,
                                                   })},
                                     }),
                                 })},
               })},
          })},
     })},
    {"time", RedisMap({
                 {"summary", "Returns the server time."_RedisString},
                 {"since", "2.6.0"_RedisString},
                 {"group", "server"_RedisString},
                 {"complexity", "O(1)"_RedisString},
             })},
    {"sunsubscribe",
     RedisMap({
         {"summary",
          "Stops listening to messages posted to shard channels."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "pubsub"_RedisString},
         {"complexity",
          "O(N) where N is the number of clients already subscribed to a shard channel."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "shardchannel"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "shardchannel"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"module",
     RedisMap({
         {"summary", "A container for module commands."_RedisString},
         {"since", "4.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"module|load",
               RedisMap({
                   {"summary", "Loads a module."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "path"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "path"_RedisString},
                                     }),
                                     RedisMap({
                                         {"name", "arg"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "arg"_RedisString},
                                         {"flags", RedisArray({
                                                       "optional"_RedisStatus,
                                                       "multiple"_RedisStatus,
                                                   })},
                                     }),
                                 })},
               })},
              {"module|loadex",
               RedisMap({
                   {"summary",
                    "Loads a module using extended parameters."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "path"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "path"_RedisString},
                        }),
                        RedisMap({
                            {"name", "configs"_RedisString},
                            {"type", "block"_RedisString},
                            {"token", "CONFIG"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                          "multiple_token"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "name"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "name"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "value"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "value"_RedisString},
                                 }),
                             })},
                        }),
                        RedisMap({
                            {"name", "args"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "args"_RedisString},
                            {"token", "ARGS"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"module|list",
               RedisMap({
                   {"summary", "Returns all loaded modules."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of loaded modules."_RedisString},
               })},
              {"module|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"module|unload",
               RedisMap({
                   {"summary", "Unloads a module."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "name"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "name"_RedisString},
                                     }),
                                 })},
               })},
          })},
     })},
    {"bzmpop",
     RedisMap({
         {"summary",
          "Removes and returns a member by score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(K) + O(M*log(N)) where K is the number of provided keys, N being the number of elements in the sorted set, and M being the number of elements popped."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "timeout"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "timeout"_RedisString},
              }),
              RedisMap({
                  {"name", "numkeys"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "numkeys"_RedisString},
              }),
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "where"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "min"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "min"_RedisString},
                                        {"token", "MIN"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "max"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "max"_RedisString},
                                        {"token", "MAX"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "count"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "count"_RedisString},
                  {"token", "COUNT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"readwrite",
     RedisMap({
         {"summary",
          "Enables read-write queries for a connection to a Reids Cluster replica node."_RedisString},
         {"since", "3.0.0"_RedisString},
         {"group", "cluster"_RedisString},
         {"complexity", "O(1)"_RedisString},
     })},
    {"zadd",
     RedisMap({
         {"summary",
          "Adds one or more members to a sorted set, or updates their scores. Creates the key if it doesn't exist."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)) for each item added, where N is the number of elements in the sorted set."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"2.4.0"_RedisString,
                          "Accepts multiple elements."_RedisString}),
              RedisArray(
                  {"3.0.2"_RedisString,
                   "Added the `XX`, `NX`, `CH` and `INCR` options."_RedisString}),
              RedisArray({"6.2.0"_RedisString,
                          "Added the `GT` and `LT` options."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "condition"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"since", "3.0.2"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "nx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "nx"_RedisString},
                                        {"token", "NX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "xx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "xx"_RedisString},
                                        {"token", "XX"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "comparison"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"since", "6.2.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "gt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "gt"_RedisString},
                                        {"token", "GT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "lt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "lt"_RedisString},
                                        {"token", "LT"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "change"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "change"_RedisString},
                  {"token", "CH"_RedisString},
                  {"since", "3.0.2"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "increment"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "increment"_RedisString},
                  {"token", "INCR"_RedisString},
                  {"since", "3.0.2"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "data"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "score"_RedisString},
                                        {"type", "double"_RedisString},
                                        {"display_text", "score"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "member"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "member"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"swapdb",
     RedisMap({
         {"summary", "Swaps two Redis databases."_RedisString},
         {"since", "4.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity",
          "O(N) where N is the count of clients watching or blocking on keys from both databases."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "index1"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "index1"_RedisString},
                           }),
                           RedisMap({
                               {"name", "index2"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "index2"_RedisString},
                           }),
                       })},
     })},
    {"incrby",
     RedisMap({
         {"summary",
          "Increments the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "increment"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "increment"_RedisString},
                           }),
                       })},
     })},
    {"zscore",
     RedisMap({
         {"summary",
          "Returns the score of a member in a sorted set."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                           }),
                       })},
     })},
    {"spop",
     RedisMap({
         {"summary",
          "Returns one or more random members from a set after removing them. Deletes the set if the last member was popped."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "Without the count argument O(1), otherwise O(N) where N is the value of the passed count."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"3.2.0"_RedisString,
                          "Added the `count` argument."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"since", "3.2.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"mset",
     RedisMap({
         {"summary",
          "Atomically creates or modifies the string values of one or more keys."_RedisString},
         {"since", "1.0.1"_RedisString},
         {"group", "string"_RedisString},
         {"complexity",
          "O(N) where N is the number of keys to set."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "data"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "key"_RedisString},
                                        {"type", "key"_RedisString},
                                        {"display_text", "key"_RedisString},
                                        {"key_spec_index", 0_RedisInt},
                                    }),
                                    RedisMap({
                                        {"name", "value"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "value"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"geosearch",
     RedisMap({
         {"summary",
          "Queries a geospatial index for members inside an area of a box or a circle."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "geo"_RedisString},
         {"complexity",
          "O(N+log(M)) where N is the number of elements in the grid-aligned bounding box area around the shape provided as the filter and M is the number of items inside the shape"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added support for uppercase unit names."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "from"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "member"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "member"_RedisString},
                           {"token", "FROMMEMBER"_RedisString},
                       }),
                       RedisMap({
                           {"name", "fromlonlat"_RedisString},
                           {"type", "block"_RedisString},
                           {"token", "FROMLONLAT"_RedisString},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "longitude"_RedisString},
                                    {"type", "double"_RedisString},
                                    {"display_text", "longitude"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "latitude"_RedisString},
                                    {"type", "double"_RedisString},
                                    {"display_text", "latitude"_RedisString},
                                }),
                            })},
                       }),
                   })},
              }),
              RedisMap({
                  {"name", "by"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "circle"_RedisString},
                           {"type", "block"_RedisString},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "radius"_RedisString},
                                    {"type", "double"_RedisString},
                                    {"display_text", "radius"_RedisString},
                                    {"token", "BYRADIUS"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "unit"_RedisString},
                                    {"type", "oneof"_RedisString},
                                    {"arguments",
                                     RedisArray({
                                         RedisMap({
                                             {"name", "m"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text", "m"_RedisString},
                                             {"token", "M"_RedisString},
                                         }),
                                         RedisMap({
                                             {"name", "km"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text", "km"_RedisString},
                                             {"token", "KM"_RedisString},
                                         }),
                                         RedisMap({
                                             {"name", "ft"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text", "ft"_RedisString},
                                             {"token", "FT"_RedisString},
                                         }),
                                         RedisMap({
                                             {"name", "mi"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text", "mi"_RedisString},
                                             {"token", "MI"_RedisString},
                                         }),
                                     })},
                                }),
                            })},
                       }),
                       RedisMap({
                           {"name", "box"_RedisString},
                           {"type", "block"_RedisString},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "width"_RedisString},
                                    {"type", "double"_RedisString},
                                    {"display_text", "width"_RedisString},
                                    {"token", "BYBOX"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "height"_RedisString},
                                    {"type", "double"_RedisString},
                                    {"display_text", "height"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "unit"_RedisString},
                                    {"type", "oneof"_RedisString},
                                    {"arguments",
                                     RedisArray({
                                         RedisMap({
                                             {"name", "m"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text", "m"_RedisString},
                                             {"token", "M"_RedisString},
                                         }),
                                         RedisMap({
                                             {"name", "km"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text", "km"_RedisString},
                                             {"token", "KM"_RedisString},
                                         }),
                                         RedisMap({
                                             {"name", "ft"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text", "ft"_RedisString},
                                             {"token", "FT"_RedisString},
                                         }),
                                         RedisMap({
                                             {"name", "mi"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text", "mi"_RedisString},
                                             {"token", "MI"_RedisString},
                                         }),
                                     })},
                                }),
                            })},
                       }),
                   })},
              }),
              RedisMap({
                  {"name", "order"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "asc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "asc"_RedisString},
                                        {"token", "ASC"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "desc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "desc"_RedisString},
                                        {"token", "DESC"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "count-block"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                        {"token", "COUNT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "any"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "any"_RedisString},
                                        {"token", "ANY"_RedisString},
                                        {"flags", RedisArray({
                                                      "optional"_RedisStatus,
                                                  })},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "withcoord"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withcoord"_RedisString},
                  {"token", "WITHCOORD"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withdist"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withdist"_RedisString},
                  {"token", "WITHDIST"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withhash"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withhash"_RedisString},
                  {"token", "WITHHASH"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"hget",
     RedisMap({
         {"summary", "Returns the value of a field in a hash."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "field"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "field"_RedisString},
                           }),
                       })},
     })},
    {"zscan",
     RedisMap({
         {"summary",
          "Iterates over members and scores of a sorted set."_RedisString},
         {"since", "2.8.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "cursor"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "cursor"_RedisString},
                           }),
                           RedisMap({
                               {"name", "pattern"_RedisString},
                               {"type", "pattern"_RedisString},
                               {"display_text", "pattern"_RedisString},
                               {"token", "MATCH"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"token", "COUNT"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"xreadgroup",
     RedisMap({
         {"summary",
          "Returns new or historical messages from a stream for a consumer in a group. Blocks until a message is available otherwise."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity",
          "For each stream mentioned: O(M) with M being the number of elements returned. If M is constant (e.g. always asking for the first 10 elements with COUNT), you can consider it O(1). On the other side when XREADGROUP blocks, XADD will pay the O(N) time in order to serve the N clients blocked on the stream getting new data."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "group-block"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "GROUP"_RedisString},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "group"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "group"_RedisString},
                       }),
                       RedisMap({
                           {"name", "consumer"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "consumer"_RedisString},
                       }),
                   })},
              }),
              RedisMap({
                  {"name", "count"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "count"_RedisString},
                  {"token", "COUNT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "milliseconds"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "milliseconds"_RedisString},
                  {"token", "BLOCK"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "noack"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "noack"_RedisString},
                  {"token", "NOACK"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "streams"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "STREAMS"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "key"_RedisString},
                                        {"type", "key"_RedisString},
                                        {"display_text", "key"_RedisString},
                                        {"key_spec_index", 0_RedisInt},
                                        {"flags", RedisArray({
                                                      "multiple"_RedisStatus,
                                                  })},
                                    }),
                                    RedisMap({
                                        {"name", "id"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "id"_RedisString},
                                        {"flags", RedisArray({
                                                      "multiple"_RedisStatus,
                                                  })},
                                    }),
                                })},
              }),
          })},
     })},
    {"copy",
     RedisMap({
         {"summary", "Copies the value of a key to a new key."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(N) worst case for collections, where N is the number of nested items. O(1) for string values."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "source"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "source"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "destination"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "destination"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                           }),
                           RedisMap({
                               {"name", "destination-db"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "destination-db"_RedisString},
                               {"token", "DB"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "replace"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "replace"_RedisString},
                               {"token", "REPLACE"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"getbit", RedisMap({
                   {"summary", "Returns a bit value by offset."_RedisString},
                   {"since", "2.2.0"_RedisString},
                   {"group", "bitmap"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "key"_RedisString},
                                         {"display_text", "key"_RedisString},
                                         {"key_spec_index", 0_RedisInt},
                                     }),
                                     RedisMap({
                                         {"name", "offset"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "offset"_RedisString},
                                     }),
                                 })},
               })},
    {"xautoclaim",
     RedisMap({
         {"summary",
          "Changes, or acquires, ownership of messages in a consumer group, as if the messages were delivered to as consumer group member."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity", "O(1) if COUNT is small."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added an element to the reply array, containing deleted entries the command cleared from the PEL"_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "group"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "group"_RedisString},
                           }),
                           RedisMap({
                               {"name", "consumer"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "consumer"_RedisString},
                           }),
                           RedisMap({
                               {"name", "min-idle-time"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "min-idle-time"_RedisString},
                           }),
                           RedisMap({
                               {"name", "start"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "start"_RedisString},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"token", "COUNT"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "justid"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "justid"_RedisString},
                               {"token", "JUSTID"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"lpushx",
     RedisMap({
         {"summary",
          "Prepends one or more elements to a list only when the list exists."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"4.0.0"_RedisString,
                          "Accepts multiple `element` arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "element"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "element"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"sdiffstore",
     RedisMap({
         {"summary",
          "Stores the difference of multiple sets in a key."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(N) where N is the total number of elements in all given sets."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "destination"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "destination"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"setrange",
     RedisMap({
         {"summary",
          "Overwrites a part of a string value with another by an offset. Creates the key if it doesn't exist."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity",
          "O(1), not counting the time taken to copy the new string in place. Usually, this string is very small so the amortized complexity is O(1). Otherwise, complexity is O(M) with M being the length of the value argument."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "offset"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "offset"_RedisString},
                           }),
                           RedisMap({
                               {"name", "value"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "value"_RedisString},
                           }),
                       })},
     })},
    {"eval_ro",
     RedisMap({
         {"summary",
          "Executes a read-only server-side Lua script."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "scripting"_RedisString},
         {"complexity", "Depends on the script that is executed."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "script"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "script"_RedisString},
                           }),
                           RedisMap({
                               {"name", "numkeys"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numkeys"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "arg"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "arg"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"bgsave",
     RedisMap({
         {"summary",
          "Asynchronously saves the database(s) to disk."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray({"3.2.2"_RedisString,
                          "Added the `SCHEDULE` option."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "schedule"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "schedule"_RedisString},
                               {"token", "SCHEDULE"_RedisString},
                               {"since", "3.2.2"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"discard",
     RedisMap({
         {"summary", "Discards a transaction."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "transactions"_RedisString},
         {"complexity",
          "O(N), when N is the number of queued commands"_RedisString},
     })},
    {"psync",
     RedisMap({
         {"summary", "An internal command used in replication."_RedisString},
         {"since", "2.8.0"_RedisString},
         {"group", "server"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "replicationid"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "replicationid"_RedisString},
                           }),
                           RedisMap({
                               {"name", "offset"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "offset"_RedisString},
                           }),
                       })},
     })},
    {"keys",
     RedisMap({
         {"summary", "Returns all key names that match a pattern."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(N) with N being the number of keys in the database, under the assumption that the key names in the database and the given pattern have limited length."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "pattern"_RedisString},
                               {"type", "pattern"_RedisString},
                               {"display_text", "pattern"_RedisString},
                           }),
                       })},
     })},
    {"flushall",
     RedisMap({
         {"summary", "Removes all keys from all databases."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity",
          "O(N) where N is the total number of keys in all databases"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"4.0.0"_RedisString,
                   "Added the `ASYNC` flushing mode modifier."_RedisString}),
              RedisArray(
                  {"6.2.0"_RedisString,
                   "Added the `SYNC` flushing mode modifier."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "flush-type"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "async"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "async"_RedisString},
                                        {"token", "ASYNC"_RedisString},
                                        {"since", "4.0.0"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "sync"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "sync"_RedisString},
                                        {"token", "SYNC"_RedisString},
                                        {"since", "6.2.0"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"incrbyfloat",
     RedisMap({
         {"summary",
          "Increment the floating point value of a key by a number. Uses 0 as initial value if the key doesn't exist."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "increment"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "increment"_RedisString},
                           }),
                       })},
     })},
    {"expireat",
     RedisMap({
         {"summary",
          "Sets the expiration time of a key to a Unix timestamp."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added options: `NX`, `XX`, `GT` and `LT`."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "unix-time-seconds"_RedisString},
                  {"type", "unix-time"_RedisString},
                  {"display_text", "unix-time-seconds"_RedisString},
              }),
              RedisMap({
                  {"name", "condition"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"since", "7.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "nx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "nx"_RedisString},
                                        {"token", "NX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "xx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "xx"_RedisString},
                                        {"token", "XX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "gt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "gt"_RedisString},
                                        {"token", "GT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "lt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "lt"_RedisString},
                                        {"token", "LT"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"zunion",
     RedisMap({
         {"summary", "Returns the union of multiple sorted sets."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(N)+O(M*log(M)) with N being the sum of the sizes of the input sorted sets, and M being the number of elements in the resulting sorted set."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "numkeys"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "numkeys"_RedisString},
              }),
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "weight"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "weight"_RedisString},
                  {"token", "WEIGHTS"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "aggregate"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"token", "AGGREGATE"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "sum"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "sum"_RedisString},
                                        {"token", "SUM"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "min"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "min"_RedisString},
                                        {"token", "MIN"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "max"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "max"_RedisString},
                                        {"token", "MAX"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "withscores"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withscores"_RedisString},
                  {"token", "WITHSCORES"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"monitor",
     RedisMap({
         {"summary",
          "Listens for all requests received by the server in real-time."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
     })},
    {"substr",
     RedisMap({
         {"summary", "Returns a substring from a string value."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity",
          "O(N) where N is the length of the returned string. The complexity is ultimately determined by the returned length, but because creating a substring from an existing string is very cheap, it can be considered O(1) for small strings."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "2.0.0"_RedisString},
         {"replaced_by", "`GETRANGE`"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "start"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "start"_RedisString},
                           }),
                           RedisMap({
                               {"name", "end"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "end"_RedisString},
                           }),
                       })},
     })},
    {"setex",
     RedisMap({
         {"summary",
          "Sets the string value and expiration time of a key. Creates the key if it doesn't exist."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "2.6.12"_RedisString},
         {"replaced_by", "`SET` with the `EX` argument"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "seconds"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "seconds"_RedisString},
                           }),
                           RedisMap({
                               {"name", "value"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "value"_RedisString},
                           }),
                       })},
     })},
    {"pfselftest",
     RedisMap({
         {"summary",
          "An internal command for testing HyperLogLog values."_RedisString},
         {"since", "2.8.9"_RedisString},
         {"group", "hyperloglog"_RedisString},
         {"complexity", "N/A"_RedisString},
         {"doc_flags", RedisSet({
                           "syscmd"_RedisStatus,
                       })},
     })},
    {"blpop",
     RedisMap({
         {"summary",
          "Removes and returns the first element in a list. Blocks until an element is available otherwise. Deletes the list if the last element was popped."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N) where N is the number of provided keys."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.0.0"_RedisString,
                   "`timeout` is interpreted as a double instead of an integer."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "timeout"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "timeout"_RedisString},
                           }),
                       })},
     })},
    {"ssubscribe",
     RedisMap({
         {"summary",
          "Listens for messages published to shard channels."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "pubsub"_RedisString},
         {"complexity",
          "O(N) where N is the number of shard channels to subscribe to."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "shardchannel"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "shardchannel"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"rpush",
     RedisMap({
         {"summary",
          "Appends one or more elements to a list. Creates the key if it doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"2.4.0"_RedisString,
                          "Accepts multiple `element` arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "element"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "element"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"sdiff",
     RedisMap({
         {"summary", "Returns the difference of multiple sets."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(N) where N is the total number of elements in all given sets."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"geosearchstore",
     RedisMap(
         {
             {"summary",
              "Queries a geospatial index for members inside an area of a box or a circle, optionally stores the result."_RedisString},
             {"since", "6.2.0"_RedisString},
             {"group", "geo"_RedisString},
             {"complexity",
              "O(N+log(M)) where N is the number of elements in the grid-aligned bounding box area around the shape provided as the filter and M is the number of items inside the shape"_RedisString},
             {"history",
              RedisSet({
                  RedisArray(
                      {"7.0.0"_RedisString,
                       "Added support for uppercase unit names."_RedisString}),
              })},
             {"arguments",
              RedisArray({
                  RedisMap({
                      {"name", "destination"_RedisString},
                      {"type", "key"_RedisString},
                      {"display_text", "destination"_RedisString},
                      {"key_spec_index", 0_RedisInt},
                  }),
                  RedisMap({
                      {"name", "source"_RedisString},
                      {"type", "key"_RedisString},
                      {"display_text", "source"_RedisString},
                      {"key_spec_index", 1_RedisInt},
                  }),
                  RedisMap({
                      {"name", "from"_RedisString},
                      {"type", "oneof"_RedisString},
                      {"arguments",
                       RedisArray({
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                               {"token", "FROMMEMBER"_RedisString},
                           }),
                           RedisMap({
                               {"name", "fromlonlat"_RedisString},
                               {"type", "block"_RedisString},
                               {"token", "FROMLONLAT"_RedisString},
                               {"arguments",
                                RedisArray({
                                    RedisMap({
                                        {"name", "longitude"_RedisString},
                                        {"type", "double"_RedisString},
                                        {"display_text",
                                         "longitude"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "latitude"_RedisString},
                                        {"type", "double"_RedisString},
                                        {"display_text",
                                         "latitude"_RedisString},
                                    }),
                                })},
                           }),
                       })},
                  }),
                  RedisMap({
                      {"name", "by"_RedisString},
                      {"type", "oneof"_RedisString},
                      {"arguments",
                       RedisArray({
                           RedisMap({
                               {"name", "circle"_RedisString},
                               {"type", "block"_RedisString},
                               {"arguments",
                                RedisArray({
                                    RedisMap({
                                        {"name", "radius"_RedisString},
                                        {"type", "double"_RedisString},
                                        {"display_text", "radius"_RedisString},
                                        {"token", "BYRADIUS"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "unit"_RedisString},
                                        {"type", "oneof"_RedisString},
                                        {"arguments",
                                         RedisArray({
                                             RedisMap({
                                                 {"name", "m"_RedisString},
                                                 {"type",
                                                  "pure-token"_RedisString},
                                                 {"display_text",
                                                  "m"_RedisString},
                                                 {"token", "M"_RedisString},
                                             }),
                                             RedisMap({
                                                 {"name", "km"_RedisString},
                                                 {"type",
                                                  "pure-token"_RedisString},
                                                 {"display_text",
                                                  "km"_RedisString},
                                                 {"token", "KM"_RedisString},
                                             }),
                                             RedisMap({
                                                 {"name", "ft"_RedisString},
                                                 {"type",
                                                  "pure-token"_RedisString},
                                                 {"display_text",
                                                  "ft"_RedisString},
                                                 {"token", "FT"_RedisString},
                                             }),
                                             RedisMap({
                                                 {"name", "mi"_RedisString},
                                                 {"type",
                                                  "pure-token"_RedisString},
                                                 {"display_text",
                                                  "mi"_RedisString},
                                                 {"token", "MI"_RedisString},
                                             }),
                                         })},
                                    }),
                                })},
                           }),
                           RedisMap({
                               {"name", "box"_RedisString},
                               {"type", "block"_RedisString},
                               {"arguments",
                                RedisArray({
                                    RedisMap({
                                        {"name", "width"_RedisString},
                                        {"type", "double"_RedisString},
                                        {"display_text", "width"_RedisString},
                                        {"token", "BYBOX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "height"_RedisString},
                                        {"type", "double"_RedisString},
                                        {"display_text", "height"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "unit"_RedisString},
                                        {"type", "oneof"_RedisString},
                                        {"arguments",
                                         RedisArray({
                                             RedisMap({
                                                 {"name", "m"_RedisString},
                                                 {"type",
                                                  "pure-token"_RedisString},
                                                 {"display_text",
                                                  "m"_RedisString},
                                                 {"token", "M"_RedisString},
                                             }),
                                             RedisMap({
                                                 {"name", "km"_RedisString},
                                                 {"type",
                                                  "pure-token"_RedisString},
                                                 {"display_text",
                                                  "km"_RedisString},
                                                 {"token", "KM"_RedisString},
                                             }),
                                             RedisMap({
                                                 {"name", "ft"_RedisString},
                                                 {"type",
                                                  "pure-token"_RedisString},
                                                 {"display_text",
                                                  "ft"_RedisString},
                                                 {"token", "FT"_RedisString},
                                             }),
                                             RedisMap({
                                                 {"name", "mi"_RedisString},
                                                 {"type",
                                                  "pure-token"_RedisString},
                                                 {"display_text",
                                                  "mi"_RedisString},
                                                 {"token", "MI"_RedisString},
                                             }),
                                         })},
                                    }),
                                })},
                           }),
                       })},
                  }),
                  RedisMap({
                      {"name", "order"_RedisString},
                      {"type", "oneof"_RedisString},
                      {"flags", RedisArray({
                                    "optional"_RedisStatus,
                                })},
                      {"arguments",
                       RedisArray({
                           RedisMap({
                               {"name", "asc"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "asc"_RedisString},
                               {"token", "ASC"_RedisString},
                           }),
                           RedisMap({
                               {"name", "desc"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "desc"_RedisString},
                               {"token", "DESC"_RedisString},
                           }),
                       })},
                  }),
                  RedisMap({
                      {"name", "count-block"_RedisString},
                      {"type", "block"_RedisString},
                      {"flags", RedisArray({
                                    "optional"_RedisStatus,
                                })},
                      {"arguments",
                       RedisArray({
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"token", "COUNT"_RedisString},
                           }),
                           RedisMap({
                               {"name", "any"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "any"_RedisString},
                               {"token", "ANY"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
                  }),
                  RedisMap({
                      {"name", "storedist"_RedisString},
                      {"type", "pure-token"_RedisString},
                      {"display_text", "storedist"_RedisString},
                      {"token", "STOREDIST"_RedisString},
                      {"flags", RedisArray({
                                    "optional"_RedisStatus,
                                })},
                  }),
              })},
         })},
    {"zmscore",
     RedisMap({
         {"summary",
          "Returns the score of one or more members in a sorted set."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(N) where N is the number of members being requested."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"smismember",
     RedisMap({
         {"summary",
          "Determines whether multiple members belong to a set."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(N) where N is the number of elements being checked for membership"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"append",
     RedisMap({
         {"summary",
          "Appends a string to the value of a key. Creates the key if it doesn't exist."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity",
          "O(1). The amortized time complexity is O(1) assuming the appended value is small and the already present value is of any size, since the dynamic string library used by Redis will double the free space available on every reallocation."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "value"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "value"_RedisString},
                           }),
                       })},
     })},
    {"zrangebylex",
     RedisMap({
         {"summary",
          "Returns members in a sorted set within a lexicographical range."_RedisString},
         {"since", "2.8.9"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N))."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by", "`ZRANGE` with the `BYLEX` argument"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "min"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "min"_RedisString},
              }),
              RedisMap({
                  {"name", "max"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "max"_RedisString},
              }),
              RedisMap({
                  {"name", "limit"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "LIMIT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "offset"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "offset"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"eval",
     RedisMap({
         {"summary", "Executes a server-side Lua script."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "scripting"_RedisString},
         {"complexity", "Depends on the script that is executed."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "script"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "script"_RedisString},
                           }),
                           RedisMap({
                               {"name", "numkeys"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numkeys"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "arg"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "arg"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"slaveof",
     RedisMap({
         {"summary",
          "Sets a Redis server as a replica of another, or promotes it to being a master."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "5.0.0"_RedisString},
         {"replaced_by", "`REPLICAOF`"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "host"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "host"_RedisString},
                           }),
                           RedisMap({
                               {"name", "port"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "port"_RedisString},
                           }),
                       })},
     })},
    {"reset", RedisMap({
                  {"summary", "Resets the connection."_RedisString},
                  {"since", "6.2.0"_RedisString},
                  {"group", "connection"_RedisString},
                  {"complexity", "O(1)"_RedisString},
              })},
    {"zinter",
     RedisMap({
         {"summary",
          "Returns the intersect of multiple sorted sets."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(N*K)+O(M*log(M)) worst case with N being the smallest input sorted set, K being the number of input sorted sets and M being the number of elements in the resulting sorted set."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "numkeys"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "numkeys"_RedisString},
              }),
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "weight"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "weight"_RedisString},
                  {"token", "WEIGHTS"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "aggregate"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"token", "AGGREGATE"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "sum"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "sum"_RedisString},
                                        {"token", "SUM"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "min"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "min"_RedisString},
                                        {"token", "MIN"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "max"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "max"_RedisString},
                                        {"token", "MAX"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "withscores"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withscores"_RedisString},
                  {"token", "WITHSCORES"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"pexpire",
     RedisMap({
         {"summary",
          "Sets the expiration time of a key in milliseconds."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added options: `NX`, `XX`, `GT` and `LT`."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "milliseconds"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "milliseconds"_RedisString},
              }),
              RedisMap({
                  {"name", "condition"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"since", "7.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "nx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "nx"_RedisString},
                                        {"token", "NX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "xx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "xx"_RedisString},
                                        {"token", "XX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "gt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "gt"_RedisString},
                                        {"token", "GT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "lt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "lt"_RedisString},
                                        {"token", "LT"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"command",
     RedisMap({
         {"summary",
          "Returns detailed information about all commands."_RedisString},
         {"since", "2.8.13"_RedisString},
         {"group", "server"_RedisString},
         {"complexity",
          "O(N) where N is the total number of Redis commands"_RedisString},
         {"subcommands",
          RedisMap({
              {"command|getkeys",
               RedisMap({
                   {"summary",
                    "Extracts the key names from an arbitrary command."_RedisString},
                   {"since", "2.8.13"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of arguments to the command"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "command"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "command"_RedisString},
                        }),
                        RedisMap({
                            {"name", "arg"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "arg"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"command|docs",
               RedisMap({
                   {"summary",
                    "Returns documentary information about one, multiple or all commands."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of commands to look up"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "command-name"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "command-name"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"command|count",
               RedisMap({
                   {"summary", "Returns a count of commands."_RedisString},
                   {"since", "2.8.13"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"command|getkeysandflags",
               RedisMap({
                   {"summary",
                    "Extracts the key names and access flags for an arbitrary command."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of arguments to the command"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "command"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "command"_RedisString},
                        }),
                        RedisMap({
                            {"name", "arg"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "arg"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"command|info",
               RedisMap({
                   {"summary",
                    "Returns information about one, multiple or all commands."_RedisString},
                   {"since", "2.8.13"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of commands to look up"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Allowed to be called with no argument to get info on all commands."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "command-name"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "command-name"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"command|list",
               RedisMap({
                   {"summary", "Returns a list of command names."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the total number of Redis commands"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "filterby"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"token", "FILTERBY"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "module-name"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text",
                                      "module-name"_RedisString},
                                     {"token", "MODULE"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "category"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "category"_RedisString},
                                     {"token", "ACLCAT"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "pattern"_RedisString},
                                     {"type", "pattern"_RedisString},
                                     {"display_text", "pattern"_RedisString},
                                     {"token", "PATTERN"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"command|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
          })},
     })},
    {"lrange",
     RedisMap({
         {"summary", "Returns a range of elements from a list."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(S+N) where S is the distance of start offset from HEAD for small lists, from nearest end (HEAD or TAIL) for large lists; and N is the number of elements in the specified range."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "start"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "start"_RedisString},
                           }),
                           RedisMap({
                               {"name", "stop"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "stop"_RedisString},
                           }),
                       })},
     })},
    {"lindex",
     RedisMap({
         {"summary",
          "Returns an element from a list by its index."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N) where N is the number of elements to traverse to get to the element at index. This makes asking for the first or the last element of the list O(1)."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "index"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "index"_RedisString},
                           }),
                       })},
     })},
    {"blmove",
     RedisMap({
         {"summary",
          "Pops an element from a list, pushes it to another list and returns it. Blocks until an element is available otherwise. Deletes the list if the last element was moved."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "source"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "source"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "destination"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "destination"_RedisString},
                  {"key_spec_index", 1_RedisInt},
              }),
              RedisMap({
                  {"name", "wherefrom"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "left"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "left"_RedisString},
                                        {"token", "LEFT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "right"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "right"_RedisString},
                                        {"token", "RIGHT"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "whereto"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "left"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "left"_RedisString},
                                        {"token", "LEFT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "right"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "right"_RedisString},
                                        {"token", "RIGHT"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "timeout"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "timeout"_RedisString},
              }),
          })},
     })},
    {"ttl",
     RedisMap({
         {"summary",
          "Returns the expiration time in seconds of a key."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history", RedisSet({
                         RedisArray({"2.8.0"_RedisString,
                                     "Added the -2 reply."_RedisString}),
                     })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"xread",
     RedisMap({
         {"summary",
          "Returns messages from multiple streams with IDs greater than the ones requested. Blocks until a message is available otherwise."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "count"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "count"_RedisString},
                  {"token", "COUNT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "milliseconds"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "milliseconds"_RedisString},
                  {"token", "BLOCK"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "streams"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "STREAMS"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "key"_RedisString},
                                        {"type", "key"_RedisString},
                                        {"display_text", "key"_RedisString},
                                        {"key_spec_index", 0_RedisInt},
                                        {"flags", RedisArray({
                                                      "multiple"_RedisStatus,
                                                  })},
                                    }),
                                    RedisMap({
                                        {"name", "id"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "id"_RedisString},
                                        {"flags", RedisArray({
                                                      "multiple"_RedisStatus,
                                                  })},
                                    }),
                                })},
              }),
          })},
     })},
    {"xgroup",
     RedisMap({
         {"summary", "A container for consumer groups commands."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"xgroup|delconsumer",
               RedisMap({
                   {"summary",
                    "Deletes a consumer from a consumer group."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "key"_RedisString},
                            {"type", "key"_RedisString},
                            {"display_text", "key"_RedisString},
                            {"key_spec_index", 0_RedisInt},
                        }),
                        RedisMap({
                            {"name", "group"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "group"_RedisString},
                        }),
                        RedisMap({
                            {"name", "consumer"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "consumer"_RedisString},
                        }),
                    })},
               })},
              {"xgroup|create",
               RedisMap({
                   {"summary", "Creates a consumer group."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added the `entries_read` named argument."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "key"_RedisString},
                            {"type", "key"_RedisString},
                            {"display_text", "key"_RedisString},
                            {"key_spec_index", 0_RedisInt},
                        }),
                        RedisMap({
                            {"name", "group"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "group"_RedisString},
                        }),
                        RedisMap({
                            {"name", "id-selector"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "id"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "id"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "new-id"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "new-id"_RedisString},
                                     {"token", "$"_RedisString},
                                 }),
                             })},
                        }),
                        RedisMap({
                            {"name", "mkstream"_RedisString},
                            {"type", "pure-token"_RedisString},
                            {"display_text", "mkstream"_RedisString},
                            {"token", "MKSTREAM"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                        RedisMap({
                            {"name", "entries-read"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "entries-read"_RedisString},
                            {"token", "ENTRIESREAD"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"xgroup|destroy",
               RedisMap({
                   {"summary", "Destroys a consumer group."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of entries in the group's pending entries list (PEL)."_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "key"_RedisString},
                                         {"display_text", "key"_RedisString},
                                         {"key_spec_index", 0_RedisInt},
                                     }),
                                     RedisMap({
                                         {"name", "group"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "group"_RedisString},
                                     }),
                                 })},
               })},
              {"xgroup|createconsumer",
               RedisMap({
                   {"summary",
                    "Creates a consumer in a consumer group."_RedisString},
                   {"since", "6.2.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "key"_RedisString},
                            {"type", "key"_RedisString},
                            {"display_text", "key"_RedisString},
                            {"key_spec_index", 0_RedisInt},
                        }),
                        RedisMap({
                            {"name", "group"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "group"_RedisString},
                        }),
                        RedisMap({
                            {"name", "consumer"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "consumer"_RedisString},
                        }),
                    })},
               })},
              {"xgroup|setid",
               RedisMap({
                   {"summary",
                    "Sets the last-delivered ID of a consumer group."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added the optional `entries_read` argument."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "key"_RedisString},
                            {"type", "key"_RedisString},
                            {"display_text", "key"_RedisString},
                            {"key_spec_index", 0_RedisInt},
                        }),
                        RedisMap({
                            {"name", "group"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "group"_RedisString},
                        }),
                        RedisMap({
                            {"name", "id-selector"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "id"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "id"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "new-id"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "new-id"_RedisString},
                                     {"token", "$"_RedisString},
                                 }),
                             })},
                        }),
                        RedisMap({
                            {"name", "entriesread"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "entries-read"_RedisString},
                            {"token", "ENTRIESREAD"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"xgroup|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
          })},
     })},
    {"hmget",
     RedisMap({
         {"summary", "Returns the values of all fields in a hash."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity",
          "O(N) where N is the number of fields being requested."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "field"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "field"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"quit", RedisMap({
                 {"summary", "Closes the connection."_RedisString},
                 {"since", "1.0.0"_RedisString},
                 {"group", "connection"_RedisString},
                 {"complexity", "O(1)"_RedisString},
                 {"doc_flags", RedisSet({
                                   "deprecated"_RedisStatus,
                               })},
                 {"deprecated_since", "7.2.0"_RedisString},
                 {"replaced_by", "just closing the connection"_RedisString},
             })},
    {"unlink",
     RedisMap({
         {"summary", "Asynchronously deletes one or more keys."_RedisString},
         {"since", "4.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(1) for each key removed regardless of its size. Then the command does O(N) work in a different thread in order to reclaim memory, where N is the number of allocations the deleted objects where composed of."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"mget",
     RedisMap({
         {"summary",
          "Atomically returns the string values of one or more keys."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity",
          "O(N) where N is the number of keys to retrieve."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"unwatch",
     RedisMap({
         {"summary",
          "Forgets about watched keys of a transaction."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "transactions"_RedisString},
         {"complexity", "O(1)"_RedisString},
     })},
    {"zpopmax",
     RedisMap({
         {"summary",
          "Returns the highest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)*M) with N being the number of elements in the sorted set, and M being the number of elements popped."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"lpos",
     RedisMap({
         {"summary",
          "Returns the index of matching elements in a list."_RedisString},
         {"since", "6.0.6"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N) where N is the number of elements in the list, for the average case. When searching for elements near the head or the tail of the list, or when the MAXLEN option is provided, the command may run in constant time."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "element"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "element"_RedisString},
                           }),
                           RedisMap({
                               {"name", "rank"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "rank"_RedisString},
                               {"token", "RANK"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "num-matches"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "num-matches"_RedisString},
                               {"token", "COUNT"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "len"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "len"_RedisString},
                               {"token", "MAXLEN"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"bitcount",
     RedisMap({
         {"summary",
          "Counts the number of set bits (population counting) in a string."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "bitmap"_RedisString},
         {"complexity", "O(N)"_RedisString},
         {"history",
          RedisSet({
              RedisArray({"7.0.0"_RedisString,
                          "Added the `BYTE|BIT` option."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "range"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "start"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "start"_RedisString},
                       }),
                       RedisMap({
                           {"name", "end"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "end"_RedisString},
                       }),
                       RedisMap({
                           {"name", "unit"_RedisString},
                           {"type", "oneof"_RedisString},
                           {"since", "7.0.0"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "byte"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text", "byte"_RedisString},
                                    {"token", "BYTE"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "bit"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text", "bit"_RedisString},
                                    {"token", "BIT"_RedisString},
                                }),
                            })},
                       }),
                   })},
              }),
          })},
     })},
    {"xdel",
     RedisMap({
         {"summary",
          "Returns the number of messages after removing them from a stream."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity",
          "O(1) for each single item to delete in the stream, regardless of the stream size."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "id"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "id"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"xpending",
     RedisMap({
         {"summary",
          "Returns the information and entries from a stream consumer group's pending entries list."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity",
          "O(N) with N being the number of elements returned, so asking for a small fixed number of entries per call is O(1). O(M), where M is the total number of entries scanned when used with the IDLE filter. When the command returns just the summary and the list of consumers is small, it runs in O(1) time; otherwise, an additional O(N) time for iterating every consumer."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.2.0"_RedisString,
                   "Added the `IDLE` option and exclusive range intervals."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "group"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "group"_RedisString},
              }),
              RedisMap({
                  {"name", "filters"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "min-idle-time"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "min-idle-time"_RedisString},
                           {"token", "IDLE"_RedisString},
                           {"since", "6.2.0"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                       }),
                       RedisMap({
                           {"name", "start"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "start"_RedisString},
                       }),
                       RedisMap({
                           {"name", "end"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "end"_RedisString},
                       }),
                       RedisMap({
                           {"name", "count"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "count"_RedisString},
                       }),
                       RedisMap({
                           {"name", "consumer"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "consumer"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                       }),
                   })},
              }),
          })},
     })},
    {"auth",
     RedisMap({
         {"summary", "Authenticates the connection."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "connection"_RedisString},
         {"complexity",
          "O(N) where N is the number of passwords defined for the user"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.0.0"_RedisString,
                   "Added ACL style (username and password)."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "username"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "username"_RedisString},
                               {"since", "6.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "password"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "password"_RedisString},
                           }),
                       })},
     })},
    {"select", RedisMap({
                   {"summary", "Changes the selected database."_RedisString},
                   {"since", "1.0.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "index"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "index"_RedisString},
                                     }),
                                 })},
               })},
    {"hmset",
     RedisMap({
         {"summary", "Sets the values of multiple fields."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity",
          "O(N) where N is the number of fields being set."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "4.0.0"_RedisString},
         {"replaced_by", "`HSET` with multiple field-value pairs"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "data"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "field"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "field"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "value"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "value"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"hstrlen",
     RedisMap({
         {"summary", "Returns the length of the value of a field."_RedisString},
         {"since", "3.2.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "field"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "field"_RedisString},
                           }),
                       })},
     })},
    {"decr",
     RedisMap({
         {"summary",
          "Decrements the integer value of a key by one. Uses 0 as initial value if the key doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"hdel",
     RedisMap({
         {"summary",
          "Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity",
          "O(N) where N is the number of fields to be removed."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"2.4.0"_RedisString,
                          "Accepts multiple `field` arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "field"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "field"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"replicaof",
     RedisMap({
         {"summary",
          "Configures a server as replica of another, or promotes it to a master."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "host"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "host"_RedisString},
                           }),
                           RedisMap({
                               {"name", "port"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "port"_RedisString},
                           }),
                       })},
     })},
    {"psubscribe",
     RedisMap({
         {"summary",
          "Listens for messages published to channels that match one or more patterns."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "pubsub"_RedisString},
         {"complexity",
          "O(N) where N is the number of patterns the client is already subscribed to."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "pattern"_RedisString},
                               {"type", "pattern"_RedisString},
                               {"display_text", "pattern"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"hset",
     RedisMap({
         {"summary",
          "Creates or modifies the value of a field in a hash."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity",
          "O(1) for each field/value pair added, so O(N) to add N field/value pairs when the command is called with multiple field/value pairs."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"4.0.0"_RedisString,
                   "Accepts multiple `field` and `value` arguments."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "data"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "field"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "field"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "value"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "value"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"brpop",
     RedisMap({
         {"summary",
          "Removes and returns the last element in a list. Blocks until an element is available otherwise. Deletes the list if the last element was popped."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N) where N is the number of provided keys."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.0.0"_RedisString,
                   "`timeout` is interpreted as a double instead of an integer."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "timeout"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "timeout"_RedisString},
                           }),
                       })},
     })},
    {"exists",
     RedisMap({
         {"summary", "Determines whether one or more keys exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(N) where N is the number of keys to check."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"3.0.3"_RedisString,
                          "Accepts multiple `key` arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"getrange",
     RedisMap({
         {"summary",
          "Returns a substring of the string stored at a key."_RedisString},
         {"since", "2.4.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity",
          "O(N) where N is the length of the returned string. The complexity is ultimately determined by the returned length, but because creating a substring from an existing string is very cheap, it can be considered O(1) for small strings."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "start"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "start"_RedisString},
                           }),
                           RedisMap({
                               {"name", "end"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "end"_RedisString},
                           }),
                       })},
     })},
    {"llen", RedisMap({
                 {"summary", "Returns the length of a list."_RedisString},
                 {"since", "1.0.0"_RedisString},
                 {"group", "list"_RedisString},
                 {"complexity", "O(1)"_RedisString},
                 {"arguments", RedisArray({
                                   RedisMap({
                                       {"name", "key"_RedisString},
                                       {"type", "key"_RedisString},
                                       {"display_text", "key"_RedisString},
                                       {"key_spec_index", 0_RedisInt},
                                   }),
                               })},
             })},
    {"xclaim",
     RedisMap({
         {"summary",
          "Changes, or acquires, ownership of a message in a consumer group, as if the message was delivered a consumer group member."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity",
          "O(log N) with N being the number of messages in the PEL of the consumer group."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "group"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "group"_RedisString},
              }),
              RedisMap({
                  {"name", "consumer"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "consumer"_RedisString},
              }),
              RedisMap({
                  {"name", "min-idle-time"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "min-idle-time"_RedisString},
              }),
              RedisMap({
                  {"name", "id"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "id"_RedisString},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "ms"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "ms"_RedisString},
                  {"token", "IDLE"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "unix-time-milliseconds"_RedisString},
                  {"type", "unix-time"_RedisString},
                  {"display_text", "unix-time-milliseconds"_RedisString},
                  {"token", "TIME"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "count"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "count"_RedisString},
                  {"token", "RETRYCOUNT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "force"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "force"_RedisString},
                  {"token", "FORCE"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "justid"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "justid"_RedisString},
                  {"token", "JUSTID"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "lastid"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "lastid"_RedisString},
                  {"token", "LASTID"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"zrevrange",
     RedisMap({
         {"summary",
          "Returns members in a sorted set within a range of indexes in reverse order."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by", "`ZRANGE` with the `REV` argument"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "start"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "start"_RedisString},
                           }),
                           RedisMap({
                               {"name", "stop"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "stop"_RedisString},
                           }),
                           RedisMap({
                               {"name", "withscores"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "withscores"_RedisString},
                               {"token", "WITHSCORES"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"xtrim",
     RedisMap({
         {"summary",
          "Deletes messages from the beginning of a stream."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity",
          "O(N), with N being the number of evicted entries. Constant times are very small however, since entries are organized in macro nodes containing multiple entries that can be released with a single deallocation."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.2.0"_RedisString,
                   "Added the `MINID` trimming strategy and the `LIMIT` option."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "trim"_RedisString},
                  {"type", "block"_RedisString},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "strategy"_RedisString},
                           {"type", "oneof"_RedisString},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "maxlen"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text", "maxlen"_RedisString},
                                    {"token", "MAXLEN"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "minid"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text", "minid"_RedisString},
                                    {"token", "MINID"_RedisString},
                                    {"since", "6.2.0"_RedisString},
                                }),
                            })},
                       }),
                       RedisMap({
                           {"name", "operator"_RedisString},
                           {"type", "oneof"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "equal"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text", "equal"_RedisString},
                                    {"token", "="_RedisString},
                                }),
                                RedisMap({
                                    {"name", "approximately"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text",
                                     "approximately"_RedisString},
                                    {"token", "~"_RedisString},
                                }),
                            })},
                       }),
                       RedisMap({
                           {"name", "threshold"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "threshold"_RedisString},
                       }),
                       RedisMap({
                           {"name", "count"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "count"_RedisString},
                           {"token", "LIMIT"_RedisString},
                           {"since", "6.2.0"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                       }),
                   })},
              }),
          })},
     })},
    {"acl",
     RedisMap({
         {"summary",
          "A container for Access List Control commands."_RedisString},
         {"since", "6.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"acl|list",
               RedisMap({
                   {"summary",
                    "Dumps the effective rules in ACL file format."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N). Where N is the number of configured users."_RedisString},
               })},
              {"acl|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"acl|users",
               RedisMap({
                   {"summary", "Lists all ACL users."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N). Where N is the number of configured users."_RedisString},
               })},
              {"acl|setuser",
               RedisMap({
                   {"summary",
                    "Creates and modifies an ACL user and its rules."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N). Where N is the number of rules provided."_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"6.2.0"_RedisString,
                             "Added Pub/Sub channel patterns."_RedisString}),
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added selectors and key based permissions."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "username"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "username"_RedisString},
                        }),
                        RedisMap({
                            {"name", "rule"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "rule"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"acl|log",
               RedisMap({
                   {"summary",
                    "Lists recent security events generated due to ACL rules."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) with N being the number of entries shown."_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"7.2.0"_RedisString,
                             "Added entry ID, timestamp created, and timestamp last updated."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "operation"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "count"_RedisString},
                                     {"type", "integer"_RedisString},
                                     {"display_text", "count"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "reset"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "reset"_RedisString},
                                     {"token", "RESET"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"acl|dryrun",
               RedisMap({
                   {"summary",
                    "Simulates the execution of a command by a user, without executing the command."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)."_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "username"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "username"_RedisString},
                        }),
                        RedisMap({
                            {"name", "command"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "command"_RedisString},
                        }),
                        RedisMap({
                            {"name", "arg"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "arg"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"acl|cat",
               RedisMap({
                   {"summary",
                    "Lists the ACL categories, or the commands inside a category."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(1) since the categories and commands are a fixed set."_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "category"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "category"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"acl|deluser",
               RedisMap({
                   {"summary",
                    "Deletes ACL users, and terminates their connections."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(1) amortized time considering the typical user."_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "username"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "username"_RedisString},
                            {"flags", RedisArray({
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"acl|save",
               RedisMap({
                   {"summary",
                    "Saves the effective ACL rules in the configured ACL file."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N). Where N is the number of configured users."_RedisString},
               })},
              {"acl|genpass",
               RedisMap({
                   {"summary",
                    "Generates a pseudorandom, secure password that can be used to identify ACL users."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "bits"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "bits"_RedisString},
                                         {"flags", RedisArray({
                                                       "optional"_RedisStatus,
                                                   })},
                                     }),
                                 })},
               })},
              {"acl|getuser",
               RedisMap({
                   {"summary", "Lists the ACL rules of a user."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N). Where N is the number of password, command and pattern rules that the user has."_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"6.2.0"_RedisString,
                             "Added Pub/Sub channel patterns."_RedisString}),
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added selectors and changed the format of key and channel patterns from a list to their rule representation."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "username"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "username"_RedisString},
                        }),
                    })},
               })},
              {"acl|load",
               RedisMap({
                   {"summary",
                    "Reloads the rules from the configured ACL file."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N). Where N is the number of configured users."_RedisString},
               })},
              {"acl|whoami",
               RedisMap({
                   {"summary",
                    "Returns the authenticated username of the current connection."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
          })},
     })},
    {"sadd",
     RedisMap({
         {"summary",
          "Adds one or more members to a set. Creates the key if it doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"2.4.0"_RedisString,
                          "Accepts multiple `member` arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"zlexcount",
     RedisMap({
         {"summary",
          "Returns the number of members in a sorted set within a lexicographical range."_RedisString},
         {"since", "2.8.9"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)) with N being the number of elements in the sorted set."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "min"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "min"_RedisString},
                           }),
                           RedisMap({
                               {"name", "max"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "max"_RedisString},
                           }),
                       })},
     })},
    {"sinter",
     RedisMap({
         {"summary", "Returns the intersect of multiple sets."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"georadiusbymember_ro",
     RedisMap({
         {"summary",
          "Returns members from a geospatial index that are within a distance from a member."_RedisString},
         {"since", "3.2.10"_RedisString},
         {"group", "geo"_RedisString},
         {"complexity",
          "O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center and radius and M is the number of items inside the index."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by",
          "`GEOSEARCH` with the `BYRADIUS` and `FROMMEMBER` arguments"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "member"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "member"_RedisString},
              }),
              RedisMap({
                  {"name", "radius"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "radius"_RedisString},
              }),
              RedisMap({
                  {"name", "unit"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "m"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "m"_RedisString},
                                        {"token", "M"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "km"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "km"_RedisString},
                                        {"token", "KM"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "ft"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "ft"_RedisString},
                                        {"token", "FT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "mi"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "mi"_RedisString},
                                        {"token", "MI"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "withcoord"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withcoord"_RedisString},
                  {"token", "WITHCOORD"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withdist"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withdist"_RedisString},
                  {"token", "WITHDIST"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withhash"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withhash"_RedisString},
                  {"token", "WITHHASH"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "count-block"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                        {"token", "COUNT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "any"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "any"_RedisString},
                                        {"token", "ANY"_RedisString},
                                        {"flags", RedisArray({
                                                      "optional"_RedisStatus,
                                                  })},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "order"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "asc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "asc"_RedisString},
                                        {"token", "ASC"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "desc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "desc"_RedisString},
                                        {"token", "DESC"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"smove",
     RedisMap({
         {"summary", "Moves a member from one set to another."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "source"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "source"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "destination"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "destination"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                           }),
                       })},
     })},
    {"del",
     RedisMap({
         {"summary", "Deletes one or more keys."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(N) where N is the number of keys that will be removed. When a key to remove holds a value other than a string, the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted set or hash. Removing a single key that holds a string value is O(1)."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"zrem",
     RedisMap({
         {"summary",
          "Removes one or more members from a sorted set. Deletes the sorted set if all members were removed."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(M*log(N)) with N being the number of elements in the sorted set and M the number of elements to be removed."_RedisString},
         {"history", RedisSet({
                         RedisArray({"2.4.0"_RedisString,
                                     "Accepts multiple elements."_RedisString}),
                     })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"bzpopmin",
     RedisMap({
         {"summary",
          "Removes and returns the member with the lowest score from one or more sorted sets. Blocks until a member is available otherwise. Deletes the sorted set if the last element was popped."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)) with N being the number of elements in the sorted set."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.0.0"_RedisString,
                   "`timeout` is interpreted as a double instead of an integer."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "timeout"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "timeout"_RedisString},
                           }),
                       })},
     })},
    {"xsetid",
     RedisMap({
         {"summary",
          "An internal command for replicating stream values."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added the `entries_added` and `max_deleted_entry_id` arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "last-id"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "last-id"_RedisString},
                           }),
                           RedisMap({
                               {"name", "entries-added"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "entries-added"_RedisString},
                               {"token", "ENTRIESADDED"_RedisString},
                               {"since", "7.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "max-deleted-id"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "max-deleted-id"_RedisString},
                               {"token", "MAXDELETEDID"_RedisString},
                               {"since", "7.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"zdiffstore",
     RedisMap({
         {"summary",
          "Stores the difference of multiple sorted sets in a key."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(L + (N-K)log(N)) worst case where L is the total number of elements in all the sets, N is the size of the first set, and K is the size of the result set."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "destination"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "destination"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "numkeys"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numkeys"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"setnx",
     RedisMap({
         {"summary",
          "Set the string value of a key only when the key doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "2.6.12"_RedisString},
         {"replaced_by", "`SET` with the `NX` argument"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "value"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "value"_RedisString},
                           }),
                       })},
     })},
    {"getset",
     RedisMap({
         {"summary",
          "Returns the previous string value of a key after setting it to a new value."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by", "`SET` with the `!GET` argument"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "value"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "value"_RedisString},
                           }),
                       })},
     })},
    {"unsubscribe",
     RedisMap({
         {"summary",
          "Stops listening to messages posted to channels."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "pubsub"_RedisString},
         {"complexity",
          "O(N) where N is the number of clients already subscribed to a channel."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "channel"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "channel"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"lcs",
     RedisMap({
         {"summary", "Finds the longest common substring."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity",
          "O(N*M) where N and M are the lengths of s1 and s2, respectively"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key1"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key1"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "key2"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key2"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "len"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "len"_RedisString},
                               {"token", "LEN"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "idx"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "idx"_RedisString},
                               {"token", "IDX"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "min-match-len"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "min-match-len"_RedisString},
                               {"token", "MINMATCHLEN"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "withmatchlen"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "withmatchlen"_RedisString},
                               {"token", "WITHMATCHLEN"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"lastsave",
     RedisMap({
         {"summary",
          "Returns the Unix timestamp of the last successful save to disk."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "O(1)"_RedisString},
     })},
    {"xrange",
     RedisMap(
         {
             {"summary",
              "Returns the messages from a stream within a range of IDs."_RedisString},
             {"since", "5.0.0"_RedisString},
             {"group", "stream"_RedisString},
             {"complexity",
              "O(N) with N being the number of elements being returned. If N is constant (e.g. always asking for the first 10 elements with COUNT), you can consider it O(1)."_RedisString},
             {"history",
              RedisSet({
                  RedisArray({"6.2.0"_RedisString,
                              "Added exclusive ranges."_RedisString}),
              })},
             {"arguments", RedisArray({
                               RedisMap({
                                   {"name", "key"_RedisString},
                                   {"type", "key"_RedisString},
                                   {"display_text", "key"_RedisString},
                                   {"key_spec_index", 0_RedisInt},
                               }),
                               RedisMap({
                                   {"name", "start"_RedisString},
                                   {"type", "string"_RedisString},
                                   {"display_text", "start"_RedisString},
                               }),
                               RedisMap({
                                   {"name", "end"_RedisString},
                                   {"type", "string"_RedisString},
                                   {"display_text", "end"_RedisString},
                               }),
                               RedisMap({
                                   {"name", "count"_RedisString},
                                   {"type", "integer"_RedisString},
                                   {"display_text", "count"_RedisString},
                                   {"token", "COUNT"_RedisString},
                                   {"flags", RedisArray({
                                                 "optional"_RedisStatus,
                                             })},
                               }),
                           })},
         })},
    {"set",
     RedisMap({
         {"summary",
          "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"2.6.12"_RedisString,
                   "Added the `EX`, `PX`, `NX` and `XX` options."_RedisString}),
              RedisArray({"6.0.0"_RedisString,
                          "Added the `KEEPTTL` option."_RedisString}),
              RedisArray(
                  {"6.2.0"_RedisString,
                   "Added the `GET`, `EXAT` and `PXAT` option."_RedisString}),
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Allowed the `NX` and `GET` options to be used together."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "value"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "value"_RedisString},
              }),
              RedisMap({
                  {"name", "condition"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"since", "2.6.12"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "nx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "nx"_RedisString},
                                        {"token", "NX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "xx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "xx"_RedisString},
                                        {"token", "XX"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "get"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "get"_RedisString},
                  {"token", "GET"_RedisString},
                  {"since", "6.2.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "expiration"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "seconds"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "seconds"_RedisString},
                           {"token", "EX"_RedisString},
                           {"since", "2.6.12"_RedisString},
                       }),
                       RedisMap({
                           {"name", "milliseconds"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "milliseconds"_RedisString},
                           {"token", "PX"_RedisString},
                           {"since", "2.6.12"_RedisString},
                       }),
                       RedisMap({
                           {"name", "unix-time-seconds"_RedisString},
                           {"type", "unix-time"_RedisString},
                           {"display_text", "unix-time-seconds"_RedisString},
                           {"token", "EXAT"_RedisString},
                           {"since", "6.2.0"_RedisString},
                       }),
                       RedisMap({
                           {"name", "unix-time-milliseconds"_RedisString},
                           {"type", "unix-time"_RedisString},
                           {"display_text",
                            "unix-time-milliseconds"_RedisString},
                           {"token", "PXAT"_RedisString},
                           {"since", "6.2.0"_RedisString},
                       }),
                       RedisMap({
                           {"name", "keepttl"_RedisString},
                           {"type", "pure-token"_RedisString},
                           {"display_text", "keepttl"_RedisString},
                           {"token", "KEEPTTL"_RedisString},
                           {"since", "6.0.0"_RedisString},
                       }),
                   })},
              }),
          })},
     })},
    {"geopos",
     RedisMap({
         {"summary",
          "Returns the longitude and latitude of members from a geospatial index."_RedisString},
         {"since", "3.2.0"_RedisString},
         {"group", "geo"_RedisString},
         {"complexity",
          "O(N) where N is the number of members requested."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"bgrewriteaof",
     RedisMap({
         {"summary",
          "Asynchronously rewrites the append-only file to disk."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "O(1)"_RedisString},
     })},
    {"hincrby",
     RedisMap({
         {"summary",
          "Increments the integer value of a field in a hash by a number. Uses 0 as initial value if the field doesn't exist."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "field"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "field"_RedisString},
                           }),
                           RedisMap({
                               {"name", "increment"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "increment"_RedisString},
                           }),
                       })},
     })},
    {"lolwut",
     RedisMap({
         {"summary", "Displays computer art and the Redis version"_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "version"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "version"_RedisString},
                               {"token", "VERSION"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"get", RedisMap({
                {"summary", "Returns the string value of a key."_RedisString},
                {"since", "1.0.0"_RedisString},
                {"group", "string"_RedisString},
                {"complexity", "O(1)"_RedisString},
                {"arguments", RedisArray({
                                  RedisMap({
                                      {"name", "key"_RedisString},
                                      {"type", "key"_RedisString},
                                      {"display_text", "key"_RedisString},
                                      {"key_spec_index", 0_RedisInt},
                                  }),
                              })},
            })},
    {"persist",
     RedisMap({
         {"summary", "Removes the expiration time of a key."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"pexpireat",
     RedisMap({
         {"summary",
          "Sets the expiration time of a key to a Unix milliseconds timestamp."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added options: `NX`, `XX`, `GT` and `LT`."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "unix-time-milliseconds"_RedisString},
                  {"type", "unix-time"_RedisString},
                  {"display_text", "unix-time-milliseconds"_RedisString},
              }),
              RedisMap({
                  {"name", "condition"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"since", "7.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "nx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "nx"_RedisString},
                                        {"token", "NX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "xx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "xx"_RedisString},
                                        {"token", "XX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "gt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "gt"_RedisString},
                                        {"token", "GT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "lt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "lt"_RedisString},
                                        {"token", "LT"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"sunionstore",
     RedisMap({
         {"summary", "Stores the union of multiple sets in a key."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(N) where N is the total number of elements in all given sets."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "destination"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "destination"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"migrate",
     RedisMap({
         {"summary",
          "Atomically transfers a key from one Redis instance to another."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "This command actually executes a DUMP+DEL in the source instance, and a RESTORE in the target instance. See the pages of these commands for time complexity. Also an O(N) data transfer between the two instances is performed."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"3.0.0"_RedisString,
                   "Added the `COPY` and `REPLACE` options."_RedisString}),
              RedisArray({"3.0.6"_RedisString,
                          "Added the `KEYS` option."_RedisString}),
              RedisArray({"4.0.7"_RedisString,
                          "Added the `AUTH` option."_RedisString}),
              RedisArray({"6.0.0"_RedisString,
                          "Added the `AUTH2` option."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "host"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "host"_RedisString},
              }),
              RedisMap({
                  {"name", "port"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "port"_RedisString},
              }),
              RedisMap({
                  {"name", "key-selector"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "key"_RedisString},
                           {"type", "key"_RedisString},
                           {"display_text", "key"_RedisString},
                           {"key_spec_index", 0_RedisInt},
                       }),
                       RedisMap({
                           {"name", "empty-string"_RedisString},
                           {"type", "pure-token"_RedisString},
                           {"display_text", "empty-string"_RedisString},
                           {"token", ""_RedisString},
                       }),
                   })},
              }),
              RedisMap({
                  {"name", "destination-db"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "destination-db"_RedisString},
              }),
              RedisMap({
                  {"name", "timeout"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "timeout"_RedisString},
              }),
              RedisMap({
                  {"name", "copy"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "copy"_RedisString},
                  {"token", "COPY"_RedisString},
                  {"since", "3.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "replace"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "replace"_RedisString},
                  {"token", "REPLACE"_RedisString},
                  {"since", "3.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "authentication"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "auth"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "password"_RedisString},
                           {"token", "AUTH"_RedisString},
                           {"since", "4.0.7"_RedisString},
                       }),
                       RedisMap({
                           {"name", "auth2"_RedisString},
                           {"type", "block"_RedisString},
                           {"token", "AUTH2"_RedisString},
                           {"since", "6.0.0"_RedisString},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "username"_RedisString},
                                    {"type", "string"_RedisString},
                                    {"display_text", "username"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "password"_RedisString},
                                    {"type", "string"_RedisString},
                                    {"display_text", "password"_RedisString},
                                }),
                            })},
                       }),
                   })},
              }),
              RedisMap({
                  {"name", "keys"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 1_RedisInt},
                  {"token", "KEYS"_RedisString},
                  {"since", "3.0.6"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                                "multiple"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"xadd",
     RedisMap({
         {"summary",
          "Appends a new message to a stream. Creates the key if it doesn't exist."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity",
          "O(1) when adding a new entry, O(N) when trimming where N being the number of entries evicted."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.2.0"_RedisString,
                   "Added the `NOMKSTREAM` option, `MINID` trimming strategy and the `LIMIT` option."_RedisString}),
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added support for the `<ms>-*` explicit ID form."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "nomkstream"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "nomkstream"_RedisString},
                  {"token", "NOMKSTREAM"_RedisString},
                  {"since", "6.2.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "trim"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "strategy"_RedisString},
                           {"type", "oneof"_RedisString},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "maxlen"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text", "maxlen"_RedisString},
                                    {"token", "MAXLEN"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "minid"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text", "minid"_RedisString},
                                    {"token", "MINID"_RedisString},
                                    {"since", "6.2.0"_RedisString},
                                }),
                            })},
                       }),
                       RedisMap({
                           {"name", "operator"_RedisString},
                           {"type", "oneof"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "equal"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text", "equal"_RedisString},
                                    {"token", "="_RedisString},
                                }),
                                RedisMap({
                                    {"name", "approximately"_RedisString},
                                    {"type", "pure-token"_RedisString},
                                    {"display_text",
                                     "approximately"_RedisString},
                                    {"token", "~"_RedisString},
                                }),
                            })},
                       }),
                       RedisMap({
                           {"name", "threshold"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "threshold"_RedisString},
                       }),
                       RedisMap({
                           {"name", "count"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "count"_RedisString},
                           {"token", "LIMIT"_RedisString},
                           {"since", "6.2.0"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                       }),
                   })},
              }),
              RedisMap({
                  {"name", "id-selector"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "auto-id"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "auto-id"_RedisString},
                                        {"token", "*"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "id"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "id"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "data"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "field"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "field"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "value"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "value"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"sinterstore",
     RedisMap({
         {"summary",
          "Stores the intersect of multiple sets in a key."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "destination"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "destination"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"zrank",
     RedisMap({
         {"summary",
          "Returns the index of a member in a sorted set ordered by ascending scores."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity", "O(log(N))"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.2.0"_RedisString,
                   "Added the optional `WITHSCORE` argument."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                           }),
                           RedisMap({
                               {"name", "withscore"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "withscore"_RedisString},
                               {"token", "WITHSCORE"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"pexpiretime",
     RedisMap({
         {"summary",
          "Returns the expiration time of a key as a Unix milliseconds timestamp."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"bitop",
     RedisMap({
         {"summary",
          "Performs bitwise operations on multiple strings, and stores the result."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "bitmap"_RedisString},
         {"complexity", "O(N)"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "operation"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "and"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "and"_RedisString},
                                        {"token", "AND"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "or"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "or"_RedisString},
                                        {"token", "OR"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "xor"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "xor"_RedisString},
                                        {"token", "XOR"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "not"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "not"_RedisString},
                                        {"token", "NOT"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "destkey"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "destkey"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 1_RedisInt},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"wait",
     RedisMap({
         {"summary",
          "Blocks until the asynchronous replication of all preceding write commands sent by the connection is completed."_RedisString},
         {"since", "3.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "numreplicas"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numreplicas"_RedisString},
                           }),
                           RedisMap({
                               {"name", "timeout"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "timeout"_RedisString},
                           }),
                       })},
     })},
    {"hexists",
     RedisMap({
         {"summary",
          "Determines whether a field exists in a hash."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "field"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "field"_RedisString},
                           }),
                       })},
     })},
    {"strlen",
     RedisMap({
         {"summary", "Returns the length of a string value."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"sort_ro",
     RedisMap({
         {"summary",
          "Returns the sorted elements of a list, a set, or a sorted set."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(N+M*log(M)) where N is the number of elements in the list or set to sort, and M the number of returned elements. When the elements are not sorted, complexity is O(N)."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "by-pattern"_RedisString},
                  {"type", "pattern"_RedisString},
                  {"display_text", "pattern"_RedisString},
                  {"key_spec_index", 1_RedisInt},
                  {"token", "BY"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "limit"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "LIMIT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "offset"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "offset"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "get-pattern"_RedisString},
                  {"type", "pattern"_RedisString},
                  {"display_text", "pattern"_RedisString},
                  {"key_spec_index", 1_RedisInt},
                  {"token", "GET"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                                "multiple"_RedisStatus,
                                "multiple_token"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "order"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "asc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "asc"_RedisString},
                                        {"token", "ASC"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "desc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "desc"_RedisString},
                                        {"token", "DESC"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "sorting"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "sorting"_RedisString},
                  {"token", "ALPHA"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"subscribe",
     RedisMap({
         {"summary", "Listens for messages published to channels."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "pubsub"_RedisString},
         {"complexity",
          "O(N) where N is the number of channels to subscribe to."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "channel"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "channel"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"touch",
     RedisMap({
         {"summary",
          "Returns the number of existing keys out of those specified after updating the time they were last accessed."_RedisString},
         {"since", "3.2.1"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(N) where N is the number of keys that will be touched."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"hvals",
     RedisMap({
         {"summary", "Returns all values in a hash."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(N) where N is the size of the hash."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"zmpop",
     RedisMap({
         {"summary",
          "Returns the highest- or lowest-scoring members from one or more sorted sets after removing them. Deletes the sorted set if the last member was popped."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(K) + O(M*log(N)) where K is the number of provided keys, N being the number of elements in the sorted set, and M being the number of elements popped."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "numkeys"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "numkeys"_RedisString},
              }),
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "where"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "min"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "min"_RedisString},
                                        {"token", "MIN"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "max"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "max"_RedisString},
                                        {"token", "MAX"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "count"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "count"_RedisString},
                  {"token", "COUNT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"object",
     RedisMap({
         {"summary",
          "A container for object introspection commands."_RedisString},
         {"since", "2.2.3"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"object|freq",
               RedisMap({
                   {"summary",
                    "Returns the logarithmic access frequency counter of a Redis object."_RedisString},
                   {"since", "4.0.0"_RedisString},
                   {"group", "generic"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "key"_RedisString},
                                         {"display_text", "key"_RedisString},
                                         {"key_spec_index", 0_RedisInt},
                                     }),
                                 })},
               })},
              {"object|encoding",
               RedisMap({
                   {"summary",
                    "Returns the internal encoding of a Redis object."_RedisString},
                   {"since", "2.2.3"_RedisString},
                   {"group", "generic"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "key"_RedisString},
                                         {"display_text", "key"_RedisString},
                                         {"key_spec_index", 0_RedisInt},
                                     }),
                                 })},
               })},
              {"object|idletime",
               RedisMap({
                   {"summary",
                    "Returns the time since the last access to a Redis object."_RedisString},
                   {"since", "2.2.3"_RedisString},
                   {"group", "generic"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "key"_RedisString},
                                         {"display_text", "key"_RedisString},
                                         {"key_spec_index", 0_RedisInt},
                                     }),
                                 })},
               })},
              {"object|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "6.2.0"_RedisString},
                   {"group", "generic"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"object|refcount",
               RedisMap({
                   {"summary",
                    "Returns the reference count of a value of a key."_RedisString},
                   {"since", "2.2.3"_RedisString},
                   {"group", "generic"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "key"_RedisString},
                                         {"display_text", "key"_RedisString},
                                         {"key_spec_index", 0_RedisInt},
                                     }),
                                 })},
               })},
          })},
     })},
    {"smembers",
     RedisMap({
         {"summary", "Returns all members of a set."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity", "O(N) where N is the set cardinality."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"save",
     RedisMap({
         {"summary",
          "Synchronously saves the database(s) to disk."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity",
          "O(N) where N is the total number of keys in all databases"_RedisString},
     })},
    {"script",
     RedisMap({
         {"summary",
          "A container for Lua scripts management commands."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "scripting"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"script|exists",
               RedisMap({
                   {"summary",
                    "Determines whether server-side Lua scripts exist in the script cache."_RedisString},
                   {"since", "2.6.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity",
                    "O(N) with N being the number of scripts to check (so checking a single script is an O(1) operation)."_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "sha1"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "sha1"_RedisString},
                                         {"flags", RedisArray({
                                                       "multiple"_RedisStatus,
                                                   })},
                                     }),
                                 })},
               })},
              {"script|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"script|debug",
               RedisMap({
                   {"summary",
                    "Sets the debug mode of server-side Lua scripts."_RedisString},
                   {"since", "3.2.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "mode"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "yes"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "yes"_RedisString},
                                     {"token", "YES"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "sync"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "sync"_RedisString},
                                     {"token", "SYNC"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "no"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "no"_RedisString},
                                     {"token", "NO"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"script|kill",
               RedisMap({
                   {"summary",
                    "Terminates a server-side Lua script during execution."_RedisString},
                   {"since", "2.6.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"script|flush",
               RedisMap({
                   {"summary",
                    "Removes all server-side Lua scripts from the script cache."_RedisString},
                   {"since", "2.6.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity",
                    "O(N) with N being the number of scripts in cache"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"6.2.0"_RedisString,
                             "Added the `ASYNC` and `SYNC` flushing mode modifiers."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "flush-type"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"since", "6.2.0"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "async"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "async"_RedisString},
                                     {"token", "ASYNC"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "sync"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "sync"_RedisString},
                                     {"token", "SYNC"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"script|load",
               RedisMap({
                   {"summary",
                    "Loads a server-side Lua script to the script cache."_RedisString},
                   {"since", "2.6.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity",
                    "O(N) with N being the length in bytes of the script body."_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "script"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "script"_RedisString},
                                     }),
                                 })},
               })},
          })},
     })},
    {"zrevrangebylex",
     RedisMap({
         {"summary",
          "Returns members in a sorted set within a lexicographical range in reverse order."_RedisString},
         {"since", "2.8.9"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N))."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by",
          "`ZRANGE` with the `REV` and `BYLEX` arguments"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "max"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "max"_RedisString},
              }),
              RedisMap({
                  {"name", "min"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "min"_RedisString},
              }),
              RedisMap({
                  {"name", "limit"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "LIMIT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "offset"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "offset"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"asking",
     RedisMap({
         {"summary",
          "Signals that a cluster client is following an -ASK redirect."_RedisString},
         {"since", "3.0.0"_RedisString},
         {"group", "cluster"_RedisString},
         {"complexity", "O(1)"_RedisString},
     })},
    {"hscan",
     RedisMap({
         {"summary", "Iterates over fields and values of a hash."_RedisString},
         {"since", "2.8.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity",
          "O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "cursor"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "cursor"_RedisString},
                           }),
                           RedisMap({
                               {"name", "pattern"_RedisString},
                               {"type", "pattern"_RedisString},
                               {"display_text", "pattern"_RedisString},
                               {"token", "MATCH"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"token", "COUNT"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"expiretime",
     RedisMap({
         {"summary",
          "Returns the expiration time of a key as a Unix timestamp."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"scard",
     RedisMap({
         {"summary", "Returns the number of members in a set."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"function",
     RedisMap({
         {"summary", "A container for function commands."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "scripting"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"function|delete",
               RedisMap({
                   {"summary",
                    "Deletes a library and its functions."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "library-name"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "library-name"_RedisString},
                        }),
                    })},
               })},
              {"function|kill",
               RedisMap({
                   {"summary",
                    "Terminates a function during execution."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"function|flush",
               RedisMap({
                   {"summary",
                    "Deletes all libraries and functions."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of functions deleted"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "flush-type"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "async"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "async"_RedisString},
                                     {"token", "ASYNC"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "sync"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "sync"_RedisString},
                                     {"token", "SYNC"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"function|load",
               RedisMap({
                   {"summary", "Creates a library."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity",
                    "O(1) (considering compilation time is redundant)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "replace"_RedisString},
                            {"type", "pure-token"_RedisString},
                            {"display_text", "replace"_RedisString},
                            {"token", "REPLACE"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                        RedisMap({
                            {"name", "function-code"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "function-code"_RedisString},
                        }),
                    })},
               })},
              {"function|restore",
               RedisMap({
                   {"summary",
                    "Restores all libraries from a payload."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of functions on the payload"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "serialized-value"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "serialized-value"_RedisString},
                        }),
                        RedisMap({
                            {"name", "policy"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "flush"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "flush"_RedisString},
                                     {"token", "FLUSH"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "append"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "append"_RedisString},
                                     {"token", "APPEND"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "replace"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "replace"_RedisString},
                                     {"token", "REPLACE"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"function|dump",
               RedisMap({
                   {"summary",
                    "Dumps all libraries into a serialized binary payload."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of functions"_RedisString},
               })},
              {"function|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"function|list",
               RedisMap({
                   {"summary",
                    "Returns information about all libraries."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of functions"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "library-name-pattern"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text",
                             "library-name-pattern"_RedisString},
                            {"token", "LIBRARYNAME"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                        RedisMap({
                            {"name", "withcode"_RedisString},
                            {"type", "pure-token"_RedisString},
                            {"display_text", "withcode"_RedisString},
                            {"token", "WITHCODE"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"function|stats",
               RedisMap({
                   {"summary",
                    "Returns information about a function during execution."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "scripting"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
          })},
     })},
    {"georadiusbymember",
     RedisMap({
         {"summary",
          "Queries a geospatial index for members within a distance from a member, optionally stores the result."_RedisString},
         {"since", "3.2.0"_RedisString},
         {"group", "geo"_RedisString},
         {"complexity",
          "O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center and radius and M is the number of items inside the index."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by",
          "`GEOSEARCH` and `GEOSEARCHSTORE` with the `BYRADIUS` and `FROMMEMBER` arguments"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added support for uppercase unit names."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "member"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "member"_RedisString},
              }),
              RedisMap({
                  {"name", "radius"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "radius"_RedisString},
              }),
              RedisMap({
                  {"name", "unit"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "m"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "m"_RedisString},
                                        {"token", "M"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "km"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "km"_RedisString},
                                        {"token", "KM"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "ft"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "ft"_RedisString},
                                        {"token", "FT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "mi"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "mi"_RedisString},
                                        {"token", "MI"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "withcoord"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withcoord"_RedisString},
                  {"token", "WITHCOORD"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withdist"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withdist"_RedisString},
                  {"token", "WITHDIST"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withhash"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withhash"_RedisString},
                  {"token", "WITHHASH"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "count-block"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                        {"token", "COUNT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "any"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "any"_RedisString},
                                        {"token", "ANY"_RedisString},
                                        {"flags", RedisArray({
                                                      "optional"_RedisStatus,
                                                  })},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "order"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "asc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "asc"_RedisString},
                                        {"token", "ASC"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "desc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "desc"_RedisString},
                                        {"token", "DESC"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "store"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "storekey"_RedisString},
                                        {"type", "key"_RedisString},
                                        {"display_text", "key"_RedisString},
                                        {"key_spec_index", 1_RedisInt},
                                        {"token", "STORE"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "storedistkey"_RedisString},
                                        {"type", "key"_RedisString},
                                        {"display_text", "key"_RedisString},
                                        {"key_spec_index", 2_RedisInt},
                                        {"token", "STOREDIST"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"zdiff",
     RedisMap({
         {"summary",
          "Returns the difference between multiple sorted sets."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(L + (N-K)log(N)) worst case where L is the total number of elements in all the sets, N is the size of the first set, and K is the size of the result set."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "numkeys"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numkeys"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "withscores"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "withscores"_RedisString},
                               {"token", "WITHSCORES"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"georadius_ro",
     RedisMap({
         {"summary",
          "Returns members from a geospatial index that are within a distance from a coordinate."_RedisString},
         {"since", "3.2.10"_RedisString},
         {"group", "geo"_RedisString},
         {"complexity",
          "O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center and radius and M is the number of items inside the index."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by",
          "`GEOSEARCH` with the `BYRADIUS` argument"_RedisString},
         {"history",
          RedisSet({
              RedisArray({"6.2.0"_RedisString,
                          "Added the `ANY` option for `COUNT`."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "longitude"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "longitude"_RedisString},
              }),
              RedisMap({
                  {"name", "latitude"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "latitude"_RedisString},
              }),
              RedisMap({
                  {"name", "radius"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "radius"_RedisString},
              }),
              RedisMap({
                  {"name", "unit"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "m"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "m"_RedisString},
                                        {"token", "M"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "km"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "km"_RedisString},
                                        {"token", "KM"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "ft"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "ft"_RedisString},
                                        {"token", "FT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "mi"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "mi"_RedisString},
                                        {"token", "MI"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "withcoord"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withcoord"_RedisString},
                  {"token", "WITHCOORD"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withdist"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withdist"_RedisString},
                  {"token", "WITHDIST"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withhash"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withhash"_RedisString},
                  {"token", "WITHHASH"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "count-block"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                        {"token", "COUNT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "any"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "any"_RedisString},
                                        {"token", "ANY"_RedisString},
                                        {"since", "6.2.0"_RedisString},
                                        {"flags", RedisArray({
                                                      "optional"_RedisStatus,
                                                  })},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "order"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "asc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "asc"_RedisString},
                                        {"token", "ASC"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "desc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "desc"_RedisString},
                                        {"token", "DESC"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"pubsub",
     RedisMap({
         {"summary", "A container for Pub/Sub commands."_RedisString},
         {"since", "2.8.0"_RedisString},
         {"group", "pubsub"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"pubsub|numsub",
               RedisMap({
                   {"summary",
                    "Returns a count of subscribers to channels."_RedisString},
                   {"since", "2.8.0"_RedisString},
                   {"group", "pubsub"_RedisString},
                   {"complexity",
                    "O(N) for the NUMSUB subcommand, where N is the number of requested channels"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "channel"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "channel"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"pubsub|numpat",
               RedisMap({
                   {"summary",
                    "Returns a count of unique pattern subscriptions."_RedisString},
                   {"since", "2.8.0"_RedisString},
                   {"group", "pubsub"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"pubsub|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "6.2.0"_RedisString},
                   {"group", "pubsub"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"pubsub|shardnumsub",
               RedisMap({
                   {"summary",
                    "Returns the count of subscribers of shard channels."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "pubsub"_RedisString},
                   {"complexity",
                    "O(N) for the SHARDNUMSUB subcommand, where N is the number of requested shard channels"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "shardchannel"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "shardchannel"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"pubsub|shardchannels",
               RedisMap({
                   {"summary",
                    "Returns the active shard channels."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "pubsub"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of active shard channels, and assuming constant time pattern matching (relatively short shard channels)."_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "pattern"_RedisString},
                            {"type", "pattern"_RedisString},
                            {"display_text", "pattern"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"pubsub|channels",
               RedisMap({
                   {"summary", "Returns the active channels."_RedisString},
                   {"since", "2.8.0"_RedisString},
                   {"group", "pubsub"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of active channels, and assuming constant time pattern matching (relatively short channels and patterns)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "pattern"_RedisString},
                            {"type", "pattern"_RedisString},
                            {"display_text", "pattern"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
          })},
     })},
    {"zrandmember",
     RedisMap({
         {"summary",
          "Returns one or more random members from a sorted set."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(N) where N is the number of members returned"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "options"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "count"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "count"_RedisString},
                       }),
                       RedisMap({
                           {"name", "withscores"_RedisString},
                           {"type", "pure-token"_RedisString},
                           {"display_text", "withscores"_RedisString},
                           {"token", "WITHSCORES"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                       }),
                   })},
              }),
          })},
     })},
    {"pfcount",
     RedisMap({
         {"summary",
          "Returns the approximated cardinality of the set(s) observed by the HyperLogLog key(s)."_RedisString},
         {"since", "2.8.9"_RedisString},
         {"group", "hyperloglog"_RedisString},
         {"complexity",
          "O(1) with a very small average constant time when called with a single key. O(N) with N being the number of keys, and much bigger constant times, when called with multiple keys."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"move", RedisMap({
                 {"summary", "Moves a key to another database."_RedisString},
                 {"since", "1.0.0"_RedisString},
                 {"group", "generic"_RedisString},
                 {"complexity", "O(1)"_RedisString},
                 {"arguments", RedisArray({
                                   RedisMap({
                                       {"name", "key"_RedisString},
                                       {"type", "key"_RedisString},
                                       {"display_text", "key"_RedisString},
                                       {"key_spec_index", 0_RedisInt},
                                   }),
                                   RedisMap({
                                       {"name", "db"_RedisString},
                                       {"type", "integer"_RedisString},
                                       {"display_text", "db"_RedisString},
                                   }),
                               })},
             })},
    {"blmpop",
     RedisMap({
         {"summary",
          "Pops the first element from one of multiple lists. Blocks until an element is available otherwise. Deletes the list if the last element was popped."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N+M) where N is the number of provided keys and M is the number of elements returned."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "timeout"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "timeout"_RedisString},
              }),
              RedisMap({
                  {"name", "numkeys"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "numkeys"_RedisString},
              }),
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "where"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "left"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "left"_RedisString},
                                        {"token", "LEFT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "right"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "right"_RedisString},
                                        {"token", "RIGHT"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "count"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "count"_RedisString},
                  {"token", "COUNT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"publish",
     RedisMap({
         {"summary", "Posts a message to a channel."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "pubsub"_RedisString},
         {"complexity",
          "O(N+M) where N is the number of clients subscribed to the receiving channel and M is the total number of subscribed patterns (by any client)."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "channel"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "channel"_RedisString},
                           }),
                           RedisMap({
                               {"name", "message"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "message"_RedisString},
                           }),
                       })},
     })},
    {"xlen",
     RedisMap({
         {"summary", "Return the number of messages in a stream."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"info",
     RedisMap({
         {"summary",
          "Returns information and statistics about the server."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added support for taking multiple section arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "section"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "section"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"sismember",
     RedisMap({
         {"summary",
          "Determines whether a member belongs to a set."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                           }),
                       })},
     })},
    {"cluster",
     RedisMap({
         {"summary", "A container for Redis Cluster commands."_RedisString},
         {"since", "3.0.0"_RedisString},
         {"group", "cluster"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"cluster|links",
               RedisMap({
                   {"summary",
                    "Returns a list of all TCP links to and from peer nodes."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the total number of Cluster nodes"_RedisString},
               })},
              {"cluster|flushslots",
               RedisMap({
                   {"summary",
                    "Deletes all slots information from a node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"cluster|setslot",
               RedisMap({
                   {"summary", "Binds a hash slot to a node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "slot"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "slot"_RedisString},
                        }),
                        RedisMap({
                            {"name", "subcommand"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "importing"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "node-id"_RedisString},
                                     {"token", "IMPORTING"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "migrating"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "node-id"_RedisString},
                                     {"token", "MIGRATING"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "node"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "node-id"_RedisString},
                                     {"token", "NODE"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "stable"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "stable"_RedisString},
                                     {"token", "STABLE"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"cluster|keyslot",
               RedisMap({
                   {"summary", "Returns the hash slot for a key."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of bytes in the key"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "key"_RedisString},
                                     }),
                                 })},
               })},
              {"cluster|addslotsrange",
               RedisMap({
                   {"summary",
                    "Assigns new hash slot ranges to a node."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the total number of the slots between the start slot and end slot arguments."_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "range"_RedisString},
                            {"type", "block"_RedisString},
                            {"flags", RedisArray({
                                          "multiple"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "start-slot"_RedisString},
                                     {"type", "integer"_RedisString},
                                     {"display_text", "start-slot"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "end-slot"_RedisString},
                                     {"type", "integer"_RedisString},
                                     {"display_text", "end-slot"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"cluster|saveconfig",
               RedisMap({
                   {"summary",
                    "Forces a node to save the cluster configuration to disk."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"cluster|failover",
               RedisMap({
                   {"summary",
                    "Forces a replica to perform a manual failover of its master."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "options"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "force"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "force"_RedisString},
                                     {"token", "FORCE"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "takeover"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "takeover"_RedisString},
                                     {"token", "TAKEOVER"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"cluster|replicate",
               RedisMap({
                   {"summary",
                    "Configure a node as replica of a master node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "node-id"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "node-id"_RedisString},
                        }),
                    })},
               })},
              {"cluster|shards",
               RedisMap({
                   {"summary",
                    "Returns the mapping of cluster slots to shards."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the total number of cluster nodes"_RedisString},
               })},
              {"cluster|meet",
               RedisMap({
                   {"summary",
                    "Forces a node to handshake with another node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"4.0.0"_RedisString,
                             "Added the optional `cluster_bus_port` argument."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "ip"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "ip"_RedisString},
                        }),
                        RedisMap({
                            {"name", "port"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "port"_RedisString},
                        }),
                        RedisMap({
                            {"name", "cluster-bus-port"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "cluster-bus-port"_RedisString},
                            {"since", "4.0.0"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"cluster|nodes",
               RedisMap({
                   {"summary",
                    "Returns the cluster configuration for a node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the total number of Cluster nodes"_RedisString},
               })},
              {"cluster|countkeysinslot",
               RedisMap({
                   {"summary",
                    "Returns the number of keys in a hash slot."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "slot"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "slot"_RedisString},
                                     }),
                                 })},
               })},
              {"cluster|myshardid",
               RedisMap({
                   {"summary", "Returns the shard ID of a node."_RedisString},
                   {"since", "7.2.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"cluster|slaves",
               RedisMap({
                   {"summary",
                    "Lists the replica nodes of a master node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"doc_flags", RedisSet({
                                     "deprecated"_RedisStatus,
                                 })},
                   {"deprecated_since", "5.0.0"_RedisString},
                   {"replaced_by", "`CLUSTER REPLICAS`"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "node-id"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "node-id"_RedisString},
                        }),
                    })},
               })},
              {"cluster|delslots",
               RedisMap({
                   {"summary",
                    "Sets hash slots as unbound for a node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the total number of hash slot arguments"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "slot"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "slot"_RedisString},
                                         {"flags", RedisArray({
                                                       "multiple"_RedisStatus,
                                                   })},
                                     }),
                                 })},
               })},
              {"cluster|myid",
               RedisMap({
                   {"summary", "Returns the ID of a node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"cluster|replicas",
               RedisMap({
                   {"summary",
                    "Lists the replica nodes of a master node."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "node-id"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "node-id"_RedisString},
                        }),
                    })},
               })},
              {"cluster|slots",
               RedisMap({
                   {"summary",
                    "Returns the mapping of cluster slots to nodes."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the total number of Cluster nodes"_RedisString},
                   {"doc_flags", RedisSet({
                                     "deprecated"_RedisStatus,
                                 })},
                   {"deprecated_since", "7.0.0"_RedisString},
                   {"replaced_by", "`CLUSTER SHARDS`"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray({"4.0.0"_RedisString,
                                    "Added node IDs."_RedisString}),
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added additional networking metadata field."_RedisString}),
                    })},
               })},
              {"cluster|info",
               RedisMap({
                   {"summary",
                    "Returns information about the state of a node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"cluster|forget",
               RedisMap({
                   {"summary",
                    "Removes a node from the nodes table."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "node-id"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "node-id"_RedisString},
                        }),
                    })},
               })},
              {"cluster|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"cluster|count-failure-reports",
               RedisMap({
                   {"summary",
                    "Returns the number of active failure reports active for a node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of failure reports"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "node-id"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "node-id"_RedisString},
                        }),
                    })},
               })},
              {"cluster|addslots",
               RedisMap({
                   {"summary", "Assigns new hash slots to a node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the total number of hash slot arguments"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "slot"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "slot"_RedisString},
                                         {"flags", RedisArray({
                                                       "multiple"_RedisStatus,
                                                   })},
                                     }),
                                 })},
               })},
              {"cluster|getkeysinslot",
               RedisMap({
                   {"summary",
                    "Returns the key names in a hash slot."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of requested keys"_RedisString},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "slot"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "slot"_RedisString},
                                     }),
                                     RedisMap({
                                         {"name", "count"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "count"_RedisString},
                                     }),
                                 })},
               })},
              {"cluster|delslotsrange",
               RedisMap({
                   {"summary",
                    "Sets hash slot ranges as unbound for a node."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the total number of the slots between the start slot and end slot arguments."_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "range"_RedisString},
                            {"type", "block"_RedisString},
                            {"flags", RedisArray({
                                          "multiple"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "start-slot"_RedisString},
                                     {"type", "integer"_RedisString},
                                     {"display_text", "start-slot"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "end-slot"_RedisString},
                                     {"type", "integer"_RedisString},
                                     {"display_text", "end-slot"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"cluster|set-config-epoch",
               RedisMap({
                   {"summary",
                    "Sets the configuration epoch for a new node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "config-epoch"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "config-epoch"_RedisString},
                        }),
                    })},
               })},
              {"cluster|reset",
               RedisMap({
                   {"summary", "Resets a node."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of known nodes. The command may execute a FLUSHALL as a side effect."_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "reset-type"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "hard"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "hard"_RedisString},
                                     {"token", "HARD"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "soft"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "soft"_RedisString},
                                     {"token", "SOFT"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"cluster|bumpepoch",
               RedisMap({
                   {"summary",
                    "Advances the cluster config epoch."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "cluster"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
          })},
     })},
    {"pttl",
     RedisMap({
         {"summary",
          "Returns the expiration time in milliseconds of a key."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history", RedisSet({
                         RedisArray({"2.8.0"_RedisString,
                                     "Added the -2 reply."_RedisString}),
                     })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"zcount",
     RedisMap({
         {"summary",
          "Returns the count of members in a sorted set that have scores within a range."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)) with N being the number of elements in the sorted set."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "min"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "min"_RedisString},
                           }),
                           RedisMap({
                               {"name", "max"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "max"_RedisString},
                           }),
                       })},
     })},
    {"replconf",
     RedisMap({
         {"summary",
          "An internal command for configuring the replication stream."_RedisString},
         {"since", "3.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"doc_flags", RedisSet({
                           "syscmd"_RedisStatus,
                       })},
     })},
    {"zintercard", RedisMap({
                       {"summary",
                        "Returns the number of members of the intersect of multiple sorted sets."_RedisString},
                       {"since", "7.0.0"_RedisString},
                       {"group", "sorted-set"_RedisString},
                       {"complexity",
                        "O(N*K) worst case with N being the smallest input sorted set, K being the number of input sorted sets."_RedisString},
                       {"arguments",
                        RedisArray({
                            RedisMap({
                                {"name", "numkeys"_RedisString},
                                {"type", "integer"_RedisString},
                                {"display_text", "numkeys"_RedisString},
                            }),
                            RedisMap({
                                {"name", "key"_RedisString},
                                {"type", "key"_RedisString},
                                {"display_text", "key"_RedisString},
                                {"key_spec_index", 0_RedisInt},
                                {"flags", RedisArray({
                                              "multiple"_RedisStatus,
                                          })},
                            }),
                            RedisMap({
                                {"name", "limit"_RedisString},
                                {"type", "integer"_RedisString},
                                {"display_text", "limit"_RedisString},
                                {"token", "LIMIT"_RedisString},
                                {"flags", RedisArray({
                                              "optional"_RedisStatus,
                                          })},
                            }),
                        })},
                   })},
    {"zremrangebylex",
     RedisMap({
         {"summary",
          "Removes members in a sorted set within a lexicographical range. Deletes the sorted set if all members were removed."_RedisString},
         {"since", "2.8.9"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "min"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "min"_RedisString},
                           }),
                           RedisMap({
                               {"name", "max"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "max"_RedisString},
                           }),
                       })},
     })},
    {"pfdebug",
     RedisMap({
         {"summary",
          "Internal commands for debugging HyperLogLog values."_RedisString},
         {"since", "2.8.9"_RedisString},
         {"group", "hyperloglog"_RedisString},
         {"complexity", "N/A"_RedisString},
         {"doc_flags", RedisSet({
                           "syscmd"_RedisStatus,
                       })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "subcommand"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "subcommand"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"hgetall",
     RedisMap({
         {"summary", "Returns all fields and values in a hash."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(N) where N is the size of the hash."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"dump",
     RedisMap({
         {"summary",
          "Returns a serialized representation of the value stored at a key."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(1) to access the key and additional O(N*M) to serialize it, where N is the number of Redis objects composing the value and M their average size. For small string values the time complexity is thus O(1)+O(1*M) where M is small, so simply O(1)."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"geohash",
     RedisMap({
         {"summary",
          "Returns members from a geospatial index as geohash strings."_RedisString},
         {"since", "3.2.0"_RedisString},
         {"group", "geo"_RedisString},
         {"complexity",
          "O(log(N)) for each member requested, where N is the number of elements in the sorted set."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"pfadd",
     RedisMap({
         {"summary",
          "Adds elements to a HyperLogLog key. Creates the key if it doesn't exist."_RedisString},
         {"since", "2.8.9"_RedisString},
         {"group", "hyperloglog"_RedisString},
         {"complexity", "O(1) to add every element."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "element"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "element"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"scan",
     RedisMap({
         {"summary",
          "Iterates over the key names in the database."_RedisString},
         {"since", "2.8.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"6.0.0"_RedisString,
                          "Added the `TYPE` subcommand."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "cursor"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "cursor"_RedisString},
                           }),
                           RedisMap({
                               {"name", "pattern"_RedisString},
                               {"type", "pattern"_RedisString},
                               {"display_text", "pattern"_RedisString},
                               {"token", "MATCH"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"token", "COUNT"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "type"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "type"_RedisString},
                               {"token", "TYPE"_RedisString},
                               {"since", "6.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"client",
     RedisMap({
         {"summary", "A container for client connection commands."_RedisString},
         {"since", "2.4.0"_RedisString},
         {"group", "connection"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"client|caching",
               RedisMap({
                   {"summary",
                    "Instructs the server whether to track the keys in the next request."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "mode"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "yes"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "yes"_RedisString},
                                     {"token", "YES"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "no"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "no"_RedisString},
                                     {"token", "NO"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"client|trackinginfo",
               RedisMap({
                   {"summary",
                    "Returns information about server-assisted client-side caching for the connection."_RedisString},
                   {"since", "6.2.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"client|getredir",
               RedisMap({
                   {"summary",
                    "Returns the client ID to which the connection's tracking notifications are redirected."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"client|info",
               RedisMap({
                   {"summary",
                    "Returns information about the connection."_RedisString},
                   {"since", "6.2.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"client|pause",
               RedisMap({
                   {"summary", "Suspends commands processing."_RedisString},
                   {"since", "3.0.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"6.2.0"_RedisString,
                             "`CLIENT PAUSE WRITE` mode added along with the `mode` option."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "timeout"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "timeout"_RedisString},
                        }),
                        RedisMap({
                            {"name", "mode"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"since", "6.2.0"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "write"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "write"_RedisString},
                                     {"token", "WRITE"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "all"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "all"_RedisString},
                                     {"token", "ALL"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"client|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"client|no-evict",
               RedisMap({
                   {"summary",
                    "Sets the client eviction mode of the connection."_RedisString},
                   {"since", "7.0.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "enabled"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "on"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "on"_RedisString},
                                     {"token", "ON"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "off"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "off"_RedisString},
                                     {"token", "OFF"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"client|no-touch",
               RedisMap({
                   {"summary",
                    "Controls whether commands sent by the client affect the LRU/LFU of accessed keys."_RedisString},
                   {"since", "7.2.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "enabled"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "on"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "on"_RedisString},
                                     {"token", "ON"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "off"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "off"_RedisString},
                                     {"token", "OFF"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"client|kill",
               RedisMap({
                   {"summary", "Terminates open connections."_RedisString},
                   {"since", "2.4.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of client connections"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray({"2.8.12"_RedisString,
                                    "Added new filter format."_RedisString}),
                        RedisArray(
                            {"2.8.12"_RedisString, "`ID` option."_RedisString}),
                        RedisArray(
                            {"3.2.0"_RedisString,
                             "Added `master` type in for `TYPE` option."_RedisString}),
                        RedisArray(
                            {"5.0.0"_RedisString,
                             "Replaced `slave` `TYPE` with `replica`. `slave` still supported for backward compatibility."_RedisString}),
                        RedisArray({"6.2.0"_RedisString,
                                    "`LADDR` option."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "filter"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "old-format"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "ip:port"_RedisString},
                                     {"deprecated_since", "2.8.12"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "new-format"_RedisString},
                                     {"type", "oneof"_RedisString},
                                     {"flags", RedisArray({
                                                   "multiple"_RedisStatus,
                                               })},
                                     {"arguments",
                                      RedisArray({
                                          RedisMap({
                                              {"name", "client-id"_RedisString},
                                              {"type", "integer"_RedisString},
                                              {"display_text",
                                               "client-id"_RedisString},
                                              {"token", "ID"_RedisString},
                                              {"since", "2.8.12"_RedisString},
                                              {"flags",
                                               RedisArray({
                                                   "optional"_RedisStatus,
                                               })},
                                          }),
                                          RedisMap({
                                              {"name",
                                               "client-type"_RedisString},
                                              {"type", "oneof"_RedisString},
                                              {"token", "TYPE"_RedisString},
                                              {"since", "2.8.12"_RedisString},
                                              {"flags",
                                               RedisArray({
                                                   "optional"_RedisStatus,
                                               })},
                                              {"arguments",
                                               RedisArray({
                                                   RedisMap({
                                                       {"name",
                                                        "normal"_RedisString},
                                                       {"type",
                                                        "pure-token"_RedisString},
                                                       {"display_text",
                                                        "normal"_RedisString},
                                                       {"token",
                                                        "NORMAL"_RedisString},
                                                   }),
                                                   RedisMap({
                                                       {"name",
                                                        "master"_RedisString},
                                                       {"type",
                                                        "pure-token"_RedisString},
                                                       {"display_text",
                                                        "master"_RedisString},
                                                       {"token",
                                                        "MASTER"_RedisString},
                                                       {"since",
                                                        "3.2.0"_RedisString},
                                                   }),
                                                   RedisMap({
                                                       {"name",
                                                        "slave"_RedisString},
                                                       {"type",
                                                        "pure-token"_RedisString},
                                                       {"display_text",
                                                        "slave"_RedisString},
                                                       {"token",
                                                        "SLAVE"_RedisString},
                                                   }),
                                                   RedisMap({
                                                       {"name",
                                                        "replica"_RedisString},
                                                       {"type",
                                                        "pure-token"_RedisString},
                                                       {"display_text",
                                                        "replica"_RedisString},
                                                       {"token",
                                                        "REPLICA"_RedisString},
                                                       {"since",
                                                        "5.0.0"_RedisString},
                                                   }),
                                                   RedisMap({
                                                       {"name",
                                                        "pubsub"_RedisString},
                                                       {"type",
                                                        "pure-token"_RedisString},
                                                       {"display_text",
                                                        "pubsub"_RedisString},
                                                       {"token",
                                                        "PUBSUB"_RedisString},
                                                   }),
                                               })},
                                          }),
                                          RedisMap({
                                              {"name", "username"_RedisString},
                                              {"type", "string"_RedisString},
                                              {"display_text",
                                               "username"_RedisString},
                                              {"token", "USER"_RedisString},
                                              {"flags",
                                               RedisArray({
                                                   "optional"_RedisStatus,
                                               })},
                                          }),
                                          RedisMap({
                                              {"name", "addr"_RedisString},
                                              {"type", "string"_RedisString},
                                              {"display_text",
                                               "ip:port"_RedisString},
                                              {"token", "ADDR"_RedisString},
                                              {"flags",
                                               RedisArray({
                                                   "optional"_RedisStatus,
                                               })},
                                          }),
                                          RedisMap({
                                              {"name", "laddr"_RedisString},
                                              {"type", "string"_RedisString},
                                              {"display_text",
                                               "ip:port"_RedisString},
                                              {"token", "LADDR"_RedisString},
                                              {"since", "6.2.0"_RedisString},
                                              {"flags",
                                               RedisArray({
                                                   "optional"_RedisStatus,
                                               })},
                                          }),
                                          RedisMap({
                                              {"name", "skipme"_RedisString},
                                              {"type", "oneof"_RedisString},
                                              {"token", "SKIPME"_RedisString},
                                              {"flags",
                                               RedisArray({
                                                   "optional"_RedisStatus,
                                               })},
                                              {"arguments",
                                               RedisArray({
                                                   RedisMap({
                                                       {"name",
                                                        "yes"_RedisString},
                                                       {"type",
                                                        "pure-token"_RedisString},
                                                       {"display_text",
                                                        "yes"_RedisString},
                                                       {"token",
                                                        "YES"_RedisString},
                                                   }),
                                                   RedisMap({
                                                       {"name",
                                                        "no"_RedisString},
                                                       {"type",
                                                        "pure-token"_RedisString},
                                                       {"display_text",
                                                        "no"_RedisString},
                                                       {"token",
                                                        "NO"_RedisString},
                                                   }),
                                               })},
                                          }),
                                      })},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"client|setinfo",
               RedisMap({
                   {"summary",
                    "Sets information specific to the client or connection."_RedisString},
                   {"since", "7.2.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "attr"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "libname"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "libname"_RedisString},
                                     {"token", "LIB-NAME"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "libver"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "libver"_RedisString},
                                     {"token", "LIB-VER"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"client|id",
               RedisMap({
                   {"summary",
                    "Returns the unique client ID of the connection."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"client|getname",
               RedisMap({
                   {"summary",
                    "Returns the name of the connection."_RedisString},
                   {"since", "2.6.9"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"client|tracking",
               RedisMap({
                   {"summary",
                    "Controls server-assisted client-side caching for the connection."_RedisString},
                   {"since", "6.0.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity",
                    "O(1). Some options may introduce additional complexity."_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "status"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "on"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "on"_RedisString},
                                     {"token", "ON"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "off"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "off"_RedisString},
                                     {"token", "OFF"_RedisString},
                                 }),
                             })},
                        }),
                        RedisMap({
                            {"name", "client-id"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "client-id"_RedisString},
                            {"token", "REDIRECT"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                        RedisMap({
                            {"name", "prefix"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "prefix"_RedisString},
                            {"token", "PREFIX"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                          "multiple_token"_RedisStatus,
                                      })},
                        }),
                        RedisMap({
                            {"name", "bcast"_RedisString},
                            {"type", "pure-token"_RedisString},
                            {"display_text", "bcast"_RedisString},
                            {"token", "BCAST"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                        RedisMap({
                            {"name", "optin"_RedisString},
                            {"type", "pure-token"_RedisString},
                            {"display_text", "optin"_RedisString},
                            {"token", "OPTIN"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                        RedisMap({
                            {"name", "optout"_RedisString},
                            {"type", "pure-token"_RedisString},
                            {"display_text", "optout"_RedisString},
                            {"token", "OPTOUT"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                        RedisMap({
                            {"name", "noloop"_RedisString},
                            {"type", "pure-token"_RedisString},
                            {"display_text", "noloop"_RedisString},
                            {"token", "NOLOOP"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"client|setname",
               RedisMap({
                   {"summary", "Sets the connection name."_RedisString},
                   {"since", "2.6.9"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "connection-name"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "connection-name"_RedisString},
                        }),
                    })},
               })},
              {"client|list",
               RedisMap({
                   {"summary", "Lists open connections."_RedisString},
                   {"since", "2.4.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of client connections"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"2.8.12"_RedisString,
                             "Added unique client `id` field."_RedisString}),
                        RedisArray(
                            {"5.0.0"_RedisString,
                             "Added optional `TYPE` filter."_RedisString}),
                        RedisArray({"6.0.0"_RedisString,
                                    "Added `user` field."_RedisString}),
                        RedisArray(
                            {"6.2.0"_RedisString,
                             "Added `argv-mem`, `tot-mem`, `laddr` and `redir` fields and the optional `ID` filter."_RedisString}),
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added `resp`, `multi-mem`, `rbs` and `rbp` fields."_RedisString}),
                        RedisArray({"7.0.3"_RedisString,
                                    "Added `ssub` field."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "client-type"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"token", "TYPE"_RedisString},
                            {"since", "5.0.0"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "normal"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "normal"_RedisString},
                                     {"token", "NORMAL"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "master"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "master"_RedisString},
                                     {"token", "MASTER"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "replica"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "replica"_RedisString},
                                     {"token", "REPLICA"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "pubsub"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "pubsub"_RedisString},
                                     {"token", "PUBSUB"_RedisString},
                                 }),
                             })},
                        }),
                        RedisMap({
                            {"name", "client-id"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "client-id"_RedisString},
                            {"token", "ID"_RedisString},
                            {"since", "6.2.0"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"client|reply",
               RedisMap({
                   {"summary",
                    "Instructs the server whether to reply to commands."_RedisString},
                   {"since", "3.2.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "action"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "on"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "on"_RedisString},
                                     {"token", "ON"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "off"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "off"_RedisString},
                                     {"token", "OFF"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "skip"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "skip"_RedisString},
                                     {"token", "SKIP"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"client|unblock",
               RedisMap({
                   {"summary",
                    "Unblocks a client blocked by a blocking command from a different connection."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity",
                    "O(log N) where N is the number of client connections"_RedisString},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "client-id"_RedisString},
                            {"type", "integer"_RedisString},
                            {"display_text", "client-id"_RedisString},
                        }),
                        RedisMap({
                            {"name", "unblock-type"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "timeout"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "timeout"_RedisString},
                                     {"token", "TIMEOUT"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "error"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "error"_RedisString},
                                     {"token", "ERROR"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"client|unpause",
               RedisMap({
                   {"summary",
                    "Resumes processing commands from paused clients."_RedisString},
                   {"since", "6.2.0"_RedisString},
                   {"group", "connection"_RedisString},
                   {"complexity",
                    "O(N) Where N is the number of paused clients"_RedisString},
               })},
          })},
     })},
    {"shutdown",
     RedisMap({
         {"summary",
          "Synchronously saves the database(s) to disk and shuts down the Redis server."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity",
          "O(N) when saving, where N is the total number of keys in all databases when saving data, otherwise O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added the `NOW`, `FORCE` and `ABORT` modifiers."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "save-selector"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "nosave"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "nosave"_RedisString},
                                        {"token", "NOSAVE"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "save"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "save"_RedisString},
                                        {"token", "SAVE"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "now"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "now"_RedisString},
                  {"token", "NOW"_RedisString},
                  {"since", "7.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "force"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "force"_RedisString},
                  {"token", "FORCE"_RedisString},
                  {"since", "7.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "abort"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "abort"_RedisString},
                  {"token", "ABORT"_RedisString},
                  {"since", "7.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"lmpop",
     RedisMap({
         {"summary",
          "Returns multiple elements from a list after removing them. Deletes the list if the last element was popped."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N+M) where N is the number of provided keys and M is the number of elements returned."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "numkeys"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "numkeys"_RedisString},
              }),
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "where"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "left"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "left"_RedisString},
                                        {"token", "LEFT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "right"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "right"_RedisString},
                                        {"token", "RIGHT"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "count"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "count"_RedisString},
                  {"token", "COUNT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"watch",
     RedisMap({
         {"summary",
          "Monitors changes to keys to determine the execution of a transaction."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "transactions"_RedisString},
         {"complexity", "O(1) for every key."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"hkeys",
     RedisMap({
         {"summary", "Returns all fields in a hash."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(N) where N is the size of the hash."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"zpopmin",
     RedisMap({
         {"summary",
          "Returns the lowest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)*M) with N being the number of elements in the sorted set, and M being the number of elements popped."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"ltrim",
     RedisMap({
         {"summary",
          "Removes elements from both ends a list. Deletes the list if all elements were trimmed."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N) where N is the number of elements to be removed by the operation."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "start"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "start"_RedisString},
                           }),
                           RedisMap({
                               {"name", "stop"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "stop"_RedisString},
                           }),
                       })},
     })},
    {"evalsha_ro",
     RedisMap({
         {"summary",
          "Executes a read-only server-side Lua script by SHA1 digest."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "scripting"_RedisString},
         {"complexity", "Depends on the script that is executed."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "sha1"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "sha1"_RedisString},
                           }),
                           RedisMap({
                               {"name", "numkeys"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numkeys"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "arg"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "arg"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"fcall",
     RedisMap({
         {"summary", "Invokes a function."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "scripting"_RedisString},
         {"complexity",
          "Depends on the function that is executed."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "function"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "function"_RedisString},
                           }),
                           RedisMap({
                               {"name", "numkeys"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numkeys"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "arg"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "arg"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"sort",
     RedisMap({
         {"summary",
          "Sorts the elements in a list, a set, or a sorted set, optionally storing the result."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(N+M*log(M)) where N is the number of elements in the list or set to sort, and M the number of returned elements. When the elements are not sorted, complexity is O(N)."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "by-pattern"_RedisString},
                  {"type", "pattern"_RedisString},
                  {"display_text", "pattern"_RedisString},
                  {"key_spec_index", 1_RedisInt},
                  {"token", "BY"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "limit"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "LIMIT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "offset"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "offset"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "get-pattern"_RedisString},
                  {"type", "pattern"_RedisString},
                  {"display_text", "pattern"_RedisString},
                  {"key_spec_index", 1_RedisInt},
                  {"token", "GET"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                                "multiple"_RedisStatus,
                                "multiple_token"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "order"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "asc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "asc"_RedisString},
                                        {"token", "ASC"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "desc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "desc"_RedisString},
                                        {"token", "DESC"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "sorting"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "sorting"_RedisString},
                  {"token", "ALPHA"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "destination"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "destination"_RedisString},
                  {"key_spec_index", 2_RedisInt},
                  {"token", "STORE"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"pfmerge",
     RedisMap({
         {"summary",
          "Merges one or more HyperLogLog values into a single key."_RedisString},
         {"since", "2.8.9"_RedisString},
         {"group", "hyperloglog"_RedisString},
         {"complexity",
          "O(N) to merge N HyperLogLogs, but with high constant times."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "destkey"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "destkey"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "sourcekey"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "sourcekey"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"georadius",
     RedisMap({
         {"summary",
          "Queries a geospatial index for members within a distance from a coordinate, optionally stores the result."_RedisString},
         {"since", "3.2.0"_RedisString},
         {"group", "geo"_RedisString},
         {"complexity",
          "O(N+log(M)) where N is the number of elements inside the bounding box of the circular area delimited by center and radius and M is the number of items inside the index."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by",
          "`GEOSEARCH` and `GEOSEARCHSTORE` with the `BYRADIUS` argument"_RedisString},
         {"history",
          RedisSet({
              RedisArray({"6.2.0"_RedisString,
                          "Added the `ANY` option for `COUNT`."_RedisString}),
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added support for uppercase unit names."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "longitude"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "longitude"_RedisString},
              }),
              RedisMap({
                  {"name", "latitude"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "latitude"_RedisString},
              }),
              RedisMap({
                  {"name", "radius"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "radius"_RedisString},
              }),
              RedisMap({
                  {"name", "unit"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "m"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "m"_RedisString},
                                        {"token", "M"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "km"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "km"_RedisString},
                                        {"token", "KM"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "ft"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "ft"_RedisString},
                                        {"token", "FT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "mi"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "mi"_RedisString},
                                        {"token", "MI"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "withcoord"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withcoord"_RedisString},
                  {"token", "WITHCOORD"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withdist"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withdist"_RedisString},
                  {"token", "WITHDIST"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "withhash"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withhash"_RedisString},
                  {"token", "WITHHASH"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "count-block"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                        {"token", "COUNT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "any"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "any"_RedisString},
                                        {"token", "ANY"_RedisString},
                                        {"since", "6.2.0"_RedisString},
                                        {"flags", RedisArray({
                                                      "optional"_RedisStatus,
                                                  })},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "order"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "asc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "asc"_RedisString},
                                        {"token", "ASC"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "desc"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "desc"_RedisString},
                                        {"token", "DESC"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "store"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "storekey"_RedisString},
                                        {"type", "key"_RedisString},
                                        {"display_text", "key"_RedisString},
                                        {"key_spec_index", 1_RedisInt},
                                        {"token", "STORE"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "storedistkey"_RedisString},
                                        {"type", "key"_RedisString},
                                        {"display_text", "key"_RedisString},
                                        {"key_spec_index", 2_RedisInt},
                                        {"token", "STOREDIST"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"zrevrangebyscore",
     RedisMap({
         {"summary",
          "Returns members in a sorted set within a range of scores in reverse order."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N))."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by",
          "`ZRANGE` with the `REV` and `BYSCORE` arguments"_RedisString},
         {"history",
          RedisSet({
              RedisArray({"2.1.6"_RedisString,
                          "`min` and `max` can be exclusive."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "max"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "max"_RedisString},
              }),
              RedisMap({
                  {"name", "min"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "min"_RedisString},
              }),
              RedisMap({
                  {"name", "withscores"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withscores"_RedisString},
                  {"token", "WITHSCORES"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "limit"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "LIMIT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "offset"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "offset"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"lset",
     RedisMap({
         {"summary",
          "Sets the value of an element in a list by its index."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N) where N is the length of the list. Setting either the first or the last element of the list is O(1)."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "index"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "index"_RedisString},
                           }),
                           RedisMap({
                               {"name", "element"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "element"_RedisString},
                           }),
                       })},
     })},
    {"xrevrange",
     RedisMap({
         {"summary",
          "Returns the messages from a stream within a range of IDs in reverse order."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity",
          "O(N) with N being the number of elements returned. If N is constant (e.g. always asking for the first 10 elements with COUNT), you can consider it O(1)."_RedisString},
         {"history", RedisSet({
                         RedisArray({"6.2.0"_RedisString,
                                     "Added exclusive ranges."_RedisString}),
                     })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "end"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "end"_RedisString},
                           }),
                           RedisMap({
                               {"name", "start"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "start"_RedisString},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"token", "COUNT"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"linsert",
     RedisMap({
         {"summary",
          "Inserts an element before or after another element in a list."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N) where N is the number of elements to traverse before seeing the value pivot. This means that inserting somewhere on the left end on the list (head) can be considered O(1) and inserting somewhere on the right end (tail) is O(N)."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "where"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "before"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "before"_RedisString},
                                        {"token", "BEFORE"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "after"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "after"_RedisString},
                                        {"token", "AFTER"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "pivot"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "pivot"_RedisString},
              }),
              RedisMap({
                  {"name", "element"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "element"_RedisString},
              }),
          })},
     })},
    {"incr",
     RedisMap({
         {"summary",
          "Increments the integer value of a key by one. Uses 0 as initial value if the key doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"hrandfield",
     RedisMap({
         {"summary",
          "Returns one or more random fields from a hash."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity",
          "O(N) where N is the number of fields returned"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "options"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "count"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "count"_RedisString},
                       }),
                       RedisMap({
                           {"name", "withvalues"_RedisString},
                           {"type", "pure-token"_RedisString},
                           {"display_text", "withvalues"_RedisString},
                           {"token", "WITHVALUES"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                       }),
                   })},
              }),
          })},
     })},
    {"rpushx",
     RedisMap({
         {"summary",
          "Appends an element to a list only when the list exists."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"4.0.0"_RedisString,
                          "Accepts multiple `element` arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "element"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "element"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"lrem",
     RedisMap({
         {"summary",
          "Removes elements from a list. Deletes the list if the last element was removed."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N+M) where N is the length of the list and M is the number of elements removed."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                           }),
                           RedisMap({
                               {"name", "element"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "element"_RedisString},
                           }),
                       })},
     })},
    {"hello",
     RedisMap({
         {"summary", "Handshakes with the Redis server."_RedisString},
         {"since", "6.0.0"_RedisString},
         {"group", "connection"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.2.0"_RedisString,
                   "`protover` made optional; when called without arguments the command reports the current connection's context."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "arguments"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "protover"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "protover"_RedisString},
                       }),
                       RedisMap({
                           {"name", "auth"_RedisString},
                           {"type", "block"_RedisString},
                           {"token", "AUTH"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "username"_RedisString},
                                    {"type", "string"_RedisString},
                                    {"display_text", "username"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "password"_RedisString},
                                    {"type", "string"_RedisString},
                                    {"display_text", "password"_RedisString},
                                }),
                            })},
                       }),
                       RedisMap({
                           {"name", "clientname"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "clientname"_RedisString},
                           {"token", "SETNAME"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                       }),
                   })},
              }),
          })},
     })},
    {"config",
     RedisMap({
         {"summary",
          "A container for server configuration commands."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"config|resetstat",
               RedisMap({
                   {"summary", "Resets the server's statistics."_RedisString},
                   {"since", "2.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"config|get",
               RedisMap({
                   {"summary",
                    "Returns the effective values of configuration parameters."_RedisString},
                   {"since", "2.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) when N is the number of configuration parameters provided"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added the ability to pass multiple pattern parameters in one call"_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "parameter"_RedisString},
                            {"type", "string"_RedisString},
                            {"display_text", "parameter"_RedisString},
                            {"flags", RedisArray({
                                          "multiple"_RedisStatus,
                                      })},
                        }),
                    })},
               })},
              {"config|rewrite",
               RedisMap({
                   {"summary",
                    "Persists the effective configuration to file."_RedisString},
                   {"since", "2.8.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"config|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"config|set",
               RedisMap({
                   {"summary",
                    "Sets configuration parameters in-flight."_RedisString},
                   {"since", "2.0.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) when N is the number of configuration parameters provided"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added the ability to set multiple parameters in one call."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "data"_RedisString},
                            {"type", "block"_RedisString},
                            {"flags", RedisArray({
                                          "multiple"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "parameter"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "parameter"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "value"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "value"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
          })},
     })},
    {"zincrby",
     RedisMap({
         {"summary",
          "Increments the score of a member in a sorted set."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)) where N is the number of elements in the sorted set."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "increment"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "increment"_RedisString},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                           }),
                       })},
     })},
    {"bitfield_ro",
     RedisMap({
         {"summary",
          "Performs arbitrary read-only bitfield integer operations on strings."_RedisString},
         {"since", "6.0.0"_RedisString},
         {"group", "bitmap"_RedisString},
         {"complexity", "O(1) for each subcommand specified"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "get-block"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "GET"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                                "multiple"_RedisStatus,
                                "multiple_token"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "encoding"_RedisString},
                           {"type", "string"_RedisString},
                           {"display_text", "encoding"_RedisString},
                       }),
                       RedisMap({
                           {"name", "offset"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "offset"_RedisString},
                       }),
                   })},
              }),
          })},
     })},
    {"expire",
     RedisMap({
         {"summary",
          "Sets the expiration time of a key in seconds."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.0.0"_RedisString,
                   "Added options: `NX`, `XX`, `GT` and `LT`."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "seconds"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "seconds"_RedisString},
              }),
              RedisMap({
                  {"name", "condition"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"since", "7.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "nx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "nx"_RedisString},
                                        {"token", "NX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "xx"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "xx"_RedisString},
                                        {"token", "XX"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "gt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "gt"_RedisString},
                                        {"token", "GT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "lt"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "lt"_RedisString},
                                        {"token", "LT"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"hincrbyfloat",
     RedisMap({
         {"summary",
          "Increments the floating point value of a field by a number. Uses 0 as initial value if the field doesn't exist."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "field"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "field"_RedisString},
                           }),
                           RedisMap({
                               {"name", "increment"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "increment"_RedisString},
                           }),
                       })},
     })},
    {"srandmember",
     RedisMap({
         {"summary",
          "Get one or multiple random members from a set"_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "Without the count argument O(1), otherwise O(N) where N is the absolute value of the passed count."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"2.6.0"_RedisString,
                          "Added the optional `count` argument."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"since", "2.6.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"multi", RedisMap({
                  {"summary", "Starts a transaction."_RedisString},
                  {"since", "1.2.0"_RedisString},
                  {"group", "transactions"_RedisString},
                  {"complexity", "O(1)"_RedisString},
              })},
    {"evalsha",
     RedisMap({
         {"summary",
          "Executes a server-side Lua script by SHA1 digest."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "scripting"_RedisString},
         {"complexity", "Depends on the script that is executed."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "sha1"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "sha1"_RedisString},
                           }),
                           RedisMap({
                               {"name", "numkeys"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numkeys"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "arg"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "arg"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"sscan",
     RedisMap({
         {"summary", "Iterates over members of a set."_RedisString},
         {"since", "2.8.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(1) for every call. O(N) for a complete iteration, including enough command calls for the cursor to return back to 0. N is the number of elements inside the collection."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "cursor"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "cursor"_RedisString},
                           }),
                           RedisMap({
                               {"name", "pattern"_RedisString},
                               {"type", "pattern"_RedisString},
                               {"display_text", "pattern"_RedisString},
                               {"token", "MATCH"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"token", "COUNT"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"exec",
     RedisMap({
         {"summary", "Executes all commands in a transaction."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "transactions"_RedisString},
         {"complexity", "Depends on commands in the transaction"_RedisString},
     })},
    {"geoadd", RedisMap({
                   {"summary",
                    "Adds one or more members to a geospatial index. The key is created if it doesn't exist."_RedisString},
                   {"since", "3.2.0"_RedisString},
                   {"group", "geo"_RedisString},
                   {"complexity",
                    "O(log(N)) for each item added, where N is the number of elements in the sorted set."_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"6.2.0"_RedisString,
                             "Added the `CH`, `NX` and `XX` options."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "key"_RedisString},
                            {"type", "key"_RedisString},
                            {"display_text", "key"_RedisString},
                            {"key_spec_index", 0_RedisInt},
                        }),
                        RedisMap({
                            {"name", "condition"_RedisString},
                            {"type", "oneof"_RedisString},
                            {"since", "6.2.0"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "nx"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "nx"_RedisString},
                                     {"token", "NX"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "xx"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "xx"_RedisString},
                                     {"token", "XX"_RedisString},
                                 }),
                             })},
                        }),
                        RedisMap({
                            {"name", "change"_RedisString},
                            {"type", "pure-token"_RedisString},
                            {"display_text", "change"_RedisString},
                            {"token", "CH"_RedisString},
                            {"since", "6.2.0"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                        }),
                        RedisMap({
                            {"name", "data"_RedisString},
                            {"type", "block"_RedisString},
                            {"flags", RedisArray({
                                          "multiple"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "longitude"_RedisString},
                                     {"type", "double"_RedisString},
                                     {"display_text", "longitude"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "latitude"_RedisString},
                                     {"type", "double"_RedisString},
                                     {"display_text", "latitude"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "member"_RedisString},
                                     {"type", "string"_RedisString},
                                     {"display_text", "member"_RedisString},
                                 }),
                             })},
                        }),
                    })},
               })},
    {"waitaof",
     RedisMap({
         {"summary",
          "Blocks until all of the preceding write commands sent by the connection are written to the append-only file of the master and/or replicas."_RedisString},
         {"since", "7.2.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "numlocal"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numlocal"_RedisString},
                           }),
                           RedisMap({
                               {"name", "numreplicas"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numreplicas"_RedisString},
                           }),
                           RedisMap({
                               {"name", "timeout"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "timeout"_RedisString},
                           }),
                       })},
     })},
    {"brpoplpush",
     RedisMap({
         {"summary",
          "Pops an element from a list, pushes it to another list and returns it. Block until an element is available otherwise. Deletes the list if the last element was popped."_RedisString},
         {"since", "2.2.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by",
          "`BLMOVE` with the `RIGHT` and `LEFT` arguments"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.0.0"_RedisString,
                   "`timeout` is interpreted as a double instead of an integer."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "source"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "source"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "destination"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "destination"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                           }),
                           RedisMap({
                               {"name", "timeout"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "timeout"_RedisString},
                           }),
                       })},
     })},
    {"xinfo",
     RedisMap({
         {"summary",
          "A container for stream introspection commands."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"xinfo|groups",
               RedisMap({
                   {"summary",
                    "Returns a list of the consumer groups of a stream."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added the `entries-read` and `lag` fields"_RedisString}),
                    })},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "key"_RedisString},
                                         {"display_text", "key"_RedisString},
                                         {"key_spec_index", 0_RedisInt},
                                     }),
                                 })},
               })},
              {"xinfo|consumers",
               RedisMap({
                   {"summary",
                    "Returns a list of the consumers in a consumer group."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray({"7.2.0"_RedisString,
                                    "Added the `inactive` field."_RedisString}),
                    })},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "key"_RedisString},
                                         {"type", "key"_RedisString},
                                         {"display_text", "key"_RedisString},
                                         {"key_spec_index", 0_RedisInt},
                                     }),
                                     RedisMap({
                                         {"name", "group"_RedisString},
                                         {"type", "string"_RedisString},
                                         {"display_text", "group"_RedisString},
                                     }),
                                 })},
               })},
              {"xinfo|stream",
               RedisMap({
                   {"summary",
                    "Returns information about a stream."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity", "O(1)"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray({"6.0.0"_RedisString,
                                    "Added the `FULL` modifier."_RedisString}),
                        RedisArray(
                            {"7.0.0"_RedisString,
                             "Added the `max-deleted-entry-id`, `entries-added`, `recorded-first-entry-id`, `entries-read` and `lag` fields"_RedisString}),
                        RedisArray(
                            {"7.2.0"_RedisString,
                             "Added the `active-time` field, and changed the meaning of `seen-time`."_RedisString}),
                    })},
                   {"arguments",
                    RedisArray({
                        RedisMap({
                            {"name", "key"_RedisString},
                            {"type", "key"_RedisString},
                            {"display_text", "key"_RedisString},
                            {"key_spec_index", 0_RedisInt},
                        }),
                        RedisMap({
                            {"name", "full-block"_RedisString},
                            {"type", "block"_RedisString},
                            {"flags", RedisArray({
                                          "optional"_RedisStatus,
                                      })},
                            {"arguments",
                             RedisArray({
                                 RedisMap({
                                     {"name", "full"_RedisString},
                                     {"type", "pure-token"_RedisString},
                                     {"display_text", "full"_RedisString},
                                     {"token", "FULL"_RedisString},
                                 }),
                                 RedisMap({
                                     {"name", "count"_RedisString},
                                     {"type", "integer"_RedisString},
                                     {"display_text", "count"_RedisString},
                                     {"token", "COUNT"_RedisString},
                                     {"flags", RedisArray({
                                                   "optional"_RedisStatus,
                                               })},
                                 }),
                             })},
                        }),
                    })},
               })},
              {"xinfo|help",
               RedisMap({
                   {"summary",
                    "Returns helpful text about the different subcommands."_RedisString},
                   {"since", "5.0.0"_RedisString},
                   {"group", "stream"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
          })},
     })},
    {"getdel",
     RedisMap({
         {"summary",
          "Returns the string value of a key after deleting the key."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"restore",
     RedisMap({
         {"summary",
          "Creates a key from the serialized representation of a value."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity",
          "O(1) to create the new key and additional O(N*M) to reconstruct the serialized value, where N is the number of Redis objects composing the value and M their average size. For small string values the time complexity is thus O(1)+O(1*M) where M is small, so simply O(1). However for sorted set values the complexity is O(N*M*log(N)) because inserting values into sorted sets is O(log(N))."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"3.0.0"_RedisString,
                          "Added the `REPLACE` modifier."_RedisString}),
              RedisArray({"5.0.0"_RedisString,
                          "Added the `ABSTTL` modifier."_RedisString}),
              RedisArray(
                  {"5.0.0"_RedisString,
                   "Added the `IDLETIME` and `FREQ` options."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "ttl"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "ttl"_RedisString},
                           }),
                           RedisMap({
                               {"name", "serialized-value"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "serialized-value"_RedisString},
                           }),
                           RedisMap({
                               {"name", "replace"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "replace"_RedisString},
                               {"token", "REPLACE"_RedisString},
                               {"since", "3.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "absttl"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "absttl"_RedisString},
                               {"token", "ABSTTL"_RedisString},
                               {"since", "5.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "seconds"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "seconds"_RedisString},
                               {"token", "IDLETIME"_RedisString},
                               {"since", "5.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "frequency"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "frequency"_RedisString},
                               {"token", "FREQ"_RedisString},
                               {"since", "5.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"xack",
     RedisMap({
         {"summary",
          "Returns the number of messages that were successfully acknowledged by the consumer group member of a stream."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "stream"_RedisString},
         {"complexity", "O(1) for each message ID processed."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "group"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "group"_RedisString},
                           }),
                           RedisMap({
                               {"name", "id"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "id"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"bzpopmax",
     RedisMap({
         {"summary",
          "Removes and returns the member with the highest score from one or more sorted sets. Blocks until a member available otherwise.  Deletes the sorted set if the last element was popped."_RedisString},
         {"since", "5.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)) with N being the number of elements in the sorted set."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.0.0"_RedisString,
                   "`timeout` is interpreted as a double instead of an integer."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "timeout"_RedisString},
                               {"type", "double"_RedisString},
                               {"display_text", "timeout"_RedisString},
                           }),
                       })},
     })},
    {"hsetnx",
     RedisMap({
         {"summary",
          "Sets the value of a field in a hash only when the field doesn't exist."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "field"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "field"_RedisString},
                           }),
                           RedisMap({
                               {"name", "value"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "value"_RedisString},
                           }),
                       })},
     })},
    {"zcard",
     RedisMap({
         {"summary",
          "Returns the number of members in a sorted set."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"getex",
     RedisMap({
         {"summary",
          "Returns the string value of a key after setting its expiration time."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "expiration"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "seconds"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "seconds"_RedisString},
                           {"token", "EX"_RedisString},
                       }),
                       RedisMap({
                           {"name", "milliseconds"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "milliseconds"_RedisString},
                           {"token", "PX"_RedisString},
                       }),
                       RedisMap({
                           {"name", "unix-time-seconds"_RedisString},
                           {"type", "unix-time"_RedisString},
                           {"display_text", "unix-time-seconds"_RedisString},
                           {"token", "EXAT"_RedisString},
                       }),
                       RedisMap({
                           {"name", "unix-time-milliseconds"_RedisString},
                           {"type", "unix-time"_RedisString},
                           {"display_text",
                            "unix-time-milliseconds"_RedisString},
                           {"token", "PXAT"_RedisString},
                       }),
                       RedisMap({
                           {"name", "persist"_RedisString},
                           {"type", "pure-token"_RedisString},
                           {"display_text", "persist"_RedisString},
                           {"token", "PERSIST"_RedisString},
                       }),
                   })},
              }),
          })},
     })},
    {"dbsize",
     RedisMap({
         {"summary", "Returns the number of keys in the database."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "O(1)"_RedisString},
     })},
    {"sintercard",
     RedisMap({
         {"summary",
          "Returns the number of members of the intersect of multiple sets."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(N*M) worst case where N is the cardinality of the smallest set and M is the number of sets."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "numkeys"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numkeys"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "limit"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "limit"_RedisString},
                               {"token", "LIMIT"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"geodist",
     RedisMap({
         {"summary",
          "Returns the distance between two members of a geospatial index."_RedisString},
         {"since", "3.2.0"_RedisString},
         {"group", "geo"_RedisString},
         {"complexity", "O(log(N))"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "member1"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "member1"_RedisString},
              }),
              RedisMap({
                  {"name", "member2"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "member2"_RedisString},
              }),
              RedisMap({
                  {"name", "unit"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "m"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "m"_RedisString},
                                        {"token", "M"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "km"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "km"_RedisString},
                                        {"token", "KM"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "ft"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "ft"_RedisString},
                                        {"token", "FT"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "mi"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "mi"_RedisString},
                                        {"token", "MI"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"renamenx",
     RedisMap({
         {"summary",
          "Renames a key only when the target key name doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"3.2.0"_RedisString,
                   "The command no longer returns an error when source and destination names are the same."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "newkey"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "newkey"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                           }),
                       })},
     })},
    {"flushdb",
     RedisMap({
         {"summary", "Remove all keys from the current database."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity",
          "O(N) where N is the number of keys in the selected database"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"4.0.0"_RedisString,
                   "Added the `ASYNC` flushing mode modifier."_RedisString}),
              RedisArray(
                  {"6.2.0"_RedisString,
                   "Added the `SYNC` flushing mode modifier."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "flush-type"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "async"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "async"_RedisString},
                                        {"token", "ASYNC"_RedisString},
                                        {"since", "4.0.0"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "sync"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "sync"_RedisString},
                                        {"token", "SYNC"_RedisString},
                                        {"since", "6.2.0"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"zrange",
     RedisMap({
         {"summary",
          "Returns members in a sorted set within a range of indexes."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned."_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"6.2.0"_RedisString,
                   "Added the `REV`, `BYSCORE`, `BYLEX` and `LIMIT` options."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "start"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "start"_RedisString},
              }),
              RedisMap({
                  {"name", "stop"_RedisString},
                  {"type", "string"_RedisString},
                  {"display_text", "stop"_RedisString},
              }),
              RedisMap({
                  {"name", "sortby"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"since", "6.2.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "byscore"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "byscore"_RedisString},
                                        {"token", "BYSCORE"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "bylex"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "bylex"_RedisString},
                                        {"token", "BYLEX"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "rev"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "rev"_RedisString},
                  {"token", "REV"_RedisString},
                  {"since", "6.2.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "limit"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "LIMIT"_RedisString},
                  {"since", "6.2.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "offset"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "offset"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "withscores"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withscores"_RedisString},
                  {"token", "WITHSCORES"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"zrevrank",
     RedisMap({
         {"summary",
          "Returns the index of a member in a sorted set ordered by descending scores."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity", "O(log(N))"_RedisString},
         {"history",
          RedisSet({
              RedisArray(
                  {"7.2.0"_RedisString,
                   "Added the optional `WITHSCORE` argument."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                           }),
                           RedisMap({
                               {"name", "withscore"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "withscore"_RedisString},
                               {"token", "WITHSCORE"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"decrby",
     RedisMap({
         {"summary",
          "Decrements a number from the integer value of a key. Uses 0 as initial value if the key doesn't exist."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "decrement"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "decrement"_RedisString},
                           }),
                       })},
     })},
    {"rename",
     RedisMap({
         {"summary",
          "Renames a key and overwrites the destination."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "newkey"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "newkey"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                           }),
                       })},
     })},
    {"rpoplpush",
     RedisMap({
         {"summary",
          "Returns the last element of a list after removing and pushing it to another list. Deletes the list if the last element was popped."_RedisString},
         {"since", "1.2.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by",
          "`LMOVE` with the `RIGHT` and `LEFT` arguments"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "source"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "source"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "destination"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "destination"_RedisString},
                               {"key_spec_index", 1_RedisInt},
                           }),
                       })},
     })},
    {"randomkey",
     RedisMap({
         {"summary",
          "Returns a random key name from the database."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "generic"_RedisString},
         {"complexity", "O(1)"_RedisString},
     })},
    {"fcall_ro",
     RedisMap({
         {"summary", "Invokes a read-only function."_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "scripting"_RedisString},
         {"complexity",
          "Depends on the function that is executed."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "function"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "function"_RedisString},
                           }),
                           RedisMap({
                               {"name", "numkeys"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "numkeys"_RedisString},
                           }),
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "arg"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "arg"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"failover",
     RedisMap({
         {"summary",
          "Starts a coordinated failover from a server to one of its replicas."_RedisString},
         {"since", "6.2.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "target"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "TO"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "host"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "host"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "port"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "port"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "force"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "force"_RedisString},
                                        {"token", "FORCE"_RedisString},
                                        {"flags", RedisArray({
                                                      "optional"_RedisStatus,
                                                  })},
                                    }),
                                })},
              }),
              RedisMap({
                  {"name", "abort"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "abort"_RedisString},
                  {"token", "ABORT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "milliseconds"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "milliseconds"_RedisString},
                  {"token", "TIMEOUT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
          })},
     })},
    {"lpop",
     RedisMap({
         {"summary",
          "Returns the first elements in a list after removing it. Deletes the list if the last element was popped."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N) where N is the number of elements returned"_RedisString},
         {"history",
          RedisSet({
              RedisArray({"6.2.0"_RedisString,
                          "Added the `count` argument."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"since", "6.2.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"echo", RedisMap({
                 {"summary", "Returns the given string."_RedisString},
                 {"since", "1.0.0"_RedisString},
                 {"group", "connection"_RedisString},
                 {"complexity", "O(1)"_RedisString},
                 {"arguments", RedisArray({
                                   RedisMap({
                                       {"name", "message"_RedisString},
                                       {"type", "string"_RedisString},
                                       {"display_text", "message"_RedisString},
                                   }),
                               })},
             })},
    {"rpop",
     RedisMap({
         {"summary",
          "Returns and removes the last elements of a list. Deletes the list if the last element was popped."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "list"_RedisString},
         {"complexity",
          "O(N) where N is the number of elements returned"_RedisString},
         {"history",
          RedisSet({
              RedisArray({"6.2.0"_RedisString,
                          "Added the `count` argument."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "count"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "count"_RedisString},
                               {"since", "6.2.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"zrangestore", RedisMap({
                        {"summary",
                         "Stores a range of members from sorted set in a key."_RedisString},
                        {"since", "6.2.0"_RedisString},
                        {"group", "sorted-set"_RedisString},
                        {"complexity",
                         "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements stored into the destination key."_RedisString},
                        {"arguments",
                         RedisArray({
                             RedisMap({
                                 {"name", "dst"_RedisString},
                                 {"type", "key"_RedisString},
                                 {"display_text", "dst"_RedisString},
                                 {"key_spec_index", 0_RedisInt},
                             }),
                             RedisMap({
                                 {"name", "src"_RedisString},
                                 {"type", "key"_RedisString},
                                 {"display_text", "src"_RedisString},
                                 {"key_spec_index", 1_RedisInt},
                             }),
                             RedisMap({
                                 {"name", "min"_RedisString},
                                 {"type", "string"_RedisString},
                                 {"display_text", "min"_RedisString},
                             }),
                             RedisMap({
                                 {"name", "max"_RedisString},
                                 {"type", "string"_RedisString},
                                 {"display_text", "max"_RedisString},
                             }),
                             RedisMap({
                                 {"name", "sortby"_RedisString},
                                 {"type", "oneof"_RedisString},
                                 {"flags", RedisArray({
                                               "optional"_RedisStatus,
                                           })},
                                 {"arguments",
                                  RedisArray({
                                      RedisMap({
                                          {"name", "byscore"_RedisString},
                                          {"type", "pure-token"_RedisString},
                                          {"display_text",
                                           "byscore"_RedisString},
                                          {"token", "BYSCORE"_RedisString},
                                      }),
                                      RedisMap({
                                          {"name", "bylex"_RedisString},
                                          {"type", "pure-token"_RedisString},
                                          {"display_text", "bylex"_RedisString},
                                          {"token", "BYLEX"_RedisString},
                                      }),
                                  })},
                             }),
                             RedisMap({
                                 {"name", "rev"_RedisString},
                                 {"type", "pure-token"_RedisString},
                                 {"display_text", "rev"_RedisString},
                                 {"token", "REV"_RedisString},
                                 {"flags", RedisArray({
                                               "optional"_RedisStatus,
                                           })},
                             }),
                             RedisMap({
                                 {"name", "limit"_RedisString},
                                 {"type", "block"_RedisString},
                                 {"token", "LIMIT"_RedisString},
                                 {"flags", RedisArray({
                                               "optional"_RedisStatus,
                                           })},
                                 {"arguments",
                                  RedisArray({
                                      RedisMap({
                                          {"name", "offset"_RedisString},
                                          {"type", "integer"_RedisString},
                                          {"display_text",
                                           "offset"_RedisString},
                                      }),
                                      RedisMap({
                                          {"name", "count"_RedisString},
                                          {"type", "integer"_RedisString},
                                          {"display_text", "count"_RedisString},
                                      }),
                                  })},
                             }),
                         })},
                    })},
    {"srem",
     RedisMap({
         {"summary",
          "Removes one or more members from a set. Deletes the set if the last member was removed."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "set"_RedisString},
         {"complexity",
          "O(N) where N is the number of members to be removed."_RedisString},
         {"history",
          RedisSet({
              RedisArray({"2.4.0"_RedisString,
                          "Accepts multiple `member` arguments."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "member"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "member"_RedisString},
                               {"flags", RedisArray({
                                             "multiple"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"restore-asking",
     RedisMap({
         {"summary",
          "An internal command for migrating keys in a cluster."_RedisString},
         {"since", "3.0.0"_RedisString},
         {"group", "server"_RedisString},
         {"complexity",
          "O(1) to create the new key and additional O(N*M) to reconstruct the serialized value, where N is the number of Redis objects composing the value and M their average size. For small string values the time complexity is thus O(1)+O(1*M) where M is small, so simply O(1). However for sorted set values the complexity is O(N*M*log(N)) because inserting values into sorted sets is O(log(N))."_RedisString},
         {"doc_flags", RedisSet({
                           "syscmd"_RedisStatus,
                       })},
         {"history",
          RedisSet({
              RedisArray({"3.0.0"_RedisString,
                          "Added the `REPLACE` modifier."_RedisString}),
              RedisArray({"5.0.0"_RedisString,
                          "Added the `ABSTTL` modifier."_RedisString}),
              RedisArray(
                  {"5.0.0"_RedisString,
                   "Added the `IDLETIME` and `FREQ` options."_RedisString}),
          })},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "ttl"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "ttl"_RedisString},
                           }),
                           RedisMap({
                               {"name", "serialized-value"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "serialized-value"_RedisString},
                           }),
                           RedisMap({
                               {"name", "replace"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "replace"_RedisString},
                               {"token", "REPLACE"_RedisString},
                               {"since", "3.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "absttl"_RedisString},
                               {"type", "pure-token"_RedisString},
                               {"display_text", "absttl"_RedisString},
                               {"token", "ABSTTL"_RedisString},
                               {"since", "5.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "seconds"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "seconds"_RedisString},
                               {"token", "IDLETIME"_RedisString},
                               {"since", "5.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                           RedisMap({
                               {"name", "frequency"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "frequency"_RedisString},
                               {"token", "FREQ"_RedisString},
                               {"since", "5.0.0"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"bitfield",
     RedisMap({
         {"summary",
          "Performs arbitrary bitfield integer operations on strings."_RedisString},
         {"since", "3.2.0"_RedisString},
         {"group", "bitmap"_RedisString},
         {"complexity", "O(1) for each subcommand specified"_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "operation"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                                "multiple"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "get-block"_RedisString},
                           {"type", "block"_RedisString},
                           {"token", "GET"_RedisString},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "encoding"_RedisString},
                                    {"type", "string"_RedisString},
                                    {"display_text", "encoding"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "offset"_RedisString},
                                    {"type", "integer"_RedisString},
                                    {"display_text", "offset"_RedisString},
                                }),
                            })},
                       }),
                       RedisMap({
                           {"name", "write"_RedisString},
                           {"type", "block"_RedisString},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "overflow-block"_RedisString},
                                    {"type", "oneof"_RedisString},
                                    {"token", "OVERFLOW"_RedisString},
                                    {"flags", RedisArray({
                                                  "optional"_RedisStatus,
                                              })},
                                    {"arguments",
                                     RedisArray({
                                         RedisMap({
                                             {"name", "wrap"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text",
                                              "wrap"_RedisString},
                                             {"token", "WRAP"_RedisString},
                                         }),
                                         RedisMap({
                                             {"name", "sat"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text",
                                              "sat"_RedisString},
                                             {"token", "SAT"_RedisString},
                                         }),
                                         RedisMap({
                                             {"name", "fail"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text",
                                              "fail"_RedisString},
                                             {"token", "FAIL"_RedisString},
                                         }),
                                     })},
                                }),
                                RedisMap({
                                    {"name", "write-operation"_RedisString},
                                    {"type", "oneof"_RedisString},
                                    {"arguments",
                                     RedisArray({
                                         RedisMap({
                                             {"name", "set-block"_RedisString},
                                             {"type", "block"_RedisString},
                                             {"token", "SET"_RedisString},
                                             {"arguments",
                                              RedisArray({
                                                  RedisMap({
                                                      {"name",
                                                       "encoding"_RedisString},
                                                      {"type",
                                                       "string"_RedisString},
                                                      {"display_text",
                                                       "encoding"_RedisString},
                                                  }),
                                                  RedisMap({
                                                      {"name",
                                                       "offset"_RedisString},
                                                      {"type",
                                                       "integer"_RedisString},
                                                      {"display_text",
                                                       "offset"_RedisString},
                                                  }),
                                                  RedisMap({
                                                      {"name",
                                                       "value"_RedisString},
                                                      {"type",
                                                       "integer"_RedisString},
                                                      {"display_text",
                                                       "value"_RedisString},
                                                  }),
                                              })},
                                         }),
                                         RedisMap({
                                             {"name",
                                              "incrby-block"_RedisString},
                                             {"type", "block"_RedisString},
                                             {"token", "INCRBY"_RedisString},
                                             {"arguments",
                                              RedisArray({
                                                  RedisMap({
                                                      {"name",
                                                       "encoding"_RedisString},
                                                      {"type",
                                                       "string"_RedisString},
                                                      {"display_text",
                                                       "encoding"_RedisString},
                                                  }),
                                                  RedisMap({
                                                      {"name",
                                                       "offset"_RedisString},
                                                      {"type",
                                                       "integer"_RedisString},
                                                      {"display_text",
                                                       "offset"_RedisString},
                                                  }),
                                                  RedisMap({
                                                      {"name",
                                                       "increment"_RedisString},
                                                      {"type",
                                                       "integer"_RedisString},
                                                      {"display_text",
                                                       "increment"_RedisString},
                                                  }),
                                              })},
                                         }),
                                     })},
                                }),
                            })},
                       }),
                   })},
              }),
          })},
     })},
    {"psetex",
     RedisMap({
         {"summary",
          "Sets both string value and expiration time in milliseconds of a key. The key is created if it doesn't exist."_RedisString},
         {"since", "2.6.0"_RedisString},
         {"group", "string"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "2.6.12"_RedisString},
         {"replaced_by", "`SET` with the `PX` argument"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "milliseconds"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "milliseconds"_RedisString},
                           }),
                           RedisMap({
                               {"name", "value"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "value"_RedisString},
                           }),
                       })},
     })},
    {"ping",
     RedisMap({
         {"summary", "Returns the server's liveliness response."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "connection"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "message"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "message"_RedisString},
                               {"flags", RedisArray({
                                             "optional"_RedisStatus,
                                         })},
                           }),
                       })},
     })},
    {"hlen",
     RedisMap({
         {"summary", "Returns the number of fields in a hash."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "hash"_RedisString},
         {"complexity", "O(1)"_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                       })},
     })},
    {"msetnx",
     RedisMap({
         {"summary",
          "Atomically modifies the string values of one or more keys only when all keys don't exist."_RedisString},
         {"since", "1.0.1"_RedisString},
         {"group", "string"_RedisString},
         {"complexity",
          "O(N) where N is the number of keys to set."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "data"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "key"_RedisString},
                                        {"type", "key"_RedisString},
                                        {"display_text", "key"_RedisString},
                                        {"key_spec_index", 0_RedisInt},
                                    }),
                                    RedisMap({
                                        {"name", "value"_RedisString},
                                        {"type", "string"_RedisString},
                                        {"display_text", "value"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"slowlog",
     RedisMap({
         {"summary", "A container for slow log commands."_RedisString},
         {"since", "2.2.12"_RedisString},
         {"group", "server"_RedisString},
         {"complexity", "Depends on subcommand."_RedisString},
         {"subcommands",
          RedisMap({
              {"slowlog|get",
               RedisMap({
                   {"summary", "Returns the slow log's entries."_RedisString},
                   {"since", "2.2.12"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of entries returned"_RedisString},
                   {"history",
                    RedisSet({
                        RedisArray(
                            {"4.0.0"_RedisString,
                             "Added client IP address, port and name to the reply."_RedisString}),
                    })},
                   {"arguments", RedisArray({
                                     RedisMap({
                                         {"name", "count"_RedisString},
                                         {"type", "integer"_RedisString},
                                         {"display_text", "count"_RedisString},
                                         {"flags", RedisArray({
                                                       "optional"_RedisStatus,
                                                   })},
                                     }),
                                 })},
               })},
              {"slowlog|reset",
               RedisMap({
                   {"summary",
                    "Clears all entries from the slow log."_RedisString},
                   {"since", "2.2.12"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity",
                    "O(N) where N is the number of entries in the slowlog"_RedisString},
               })},
              {"slowlog|len",
               RedisMap({
                   {"summary",
                    "Returns the number of entries in the slow log."_RedisString},
                   {"since", "2.2.12"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
              {"slowlog|help",
               RedisMap({
                   {"summary",
                    "Show helpful text about the different subcommands"_RedisString},
                   {"since", "6.2.0"_RedisString},
                   {"group", "server"_RedisString},
                   {"complexity", "O(1)"_RedisString},
               })},
          })},
     })},
    {"zremrangebyrank",
     RedisMap({
         {"summary",
          "Removes members in a sorted set within a range of indexes. Deletes the sorted set if all members were removed."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements removed by the operation."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "key"_RedisString},
                               {"type", "key"_RedisString},
                               {"display_text", "key"_RedisString},
                               {"key_spec_index", 0_RedisInt},
                           }),
                           RedisMap({
                               {"name", "start"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "start"_RedisString},
                           }),
                           RedisMap({
                               {"name", "stop"_RedisString},
                               {"type", "integer"_RedisString},
                               {"display_text", "stop"_RedisString},
                           }),
                       })},
     })},
    {"zrangebyscore",
     RedisMap({
         {"summary",
          "Returns members in a sorted set within a range of scores."_RedisString},
         {"since", "1.0.5"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements being returned. If M is constant (e.g. always asking for the first 10 elements with LIMIT), you can consider it O(log(N))."_RedisString},
         {"doc_flags", RedisSet({
                           "deprecated"_RedisStatus,
                       })},
         {"deprecated_since", "6.2.0"_RedisString},
         {"replaced_by", "`ZRANGE` with the `BYSCORE` argument"_RedisString},
         {"history",
          RedisSet({
              RedisArray({"2.0.0"_RedisString,
                          "Added the `WITHSCORES` modifier."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "min"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "min"_RedisString},
              }),
              RedisMap({
                  {"name", "max"_RedisString},
                  {"type", "double"_RedisString},
                  {"display_text", "max"_RedisString},
              }),
              RedisMap({
                  {"name", "withscores"_RedisString},
                  {"type", "pure-token"_RedisString},
                  {"display_text", "withscores"_RedisString},
                  {"token", "WITHSCORES"_RedisString},
                  {"since", "2.0.0"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "limit"_RedisString},
                  {"type", "block"_RedisString},
                  {"token", "LIMIT"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "offset"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "offset"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "count"_RedisString},
                                        {"type", "integer"_RedisString},
                                        {"display_text", "count"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"sync",
     RedisMap({
         {"summary", "An internal command used in replication."_RedisString},
         {"since", "1.0.0"_RedisString},
         {"group", "server"_RedisString},
     })},
    {"zinterstore",
     RedisMap({
         {"summary",
          "Stores the intersect of multiple sorted sets in a key."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(N*K)+O(M*log(M)) worst case with N being the smallest input sorted set, K being the number of input sorted sets and M being the number of elements in the resulting sorted set."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "destination"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "destination"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "numkeys"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "numkeys"_RedisString},
              }),
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 1_RedisInt},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "weight"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "weight"_RedisString},
                  {"token", "WEIGHTS"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "aggregate"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"token", "AGGREGATE"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "sum"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "sum"_RedisString},
                                        {"token", "SUM"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "min"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "min"_RedisString},
                                        {"token", "MIN"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "max"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "max"_RedisString},
                                        {"token", "MAX"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
    {"type", RedisMap({
                 {"summary",
                  "Determines the type of value stored at a key."_RedisString},
                 {"since", "1.0.0"_RedisString},
                 {"group", "generic"_RedisString},
                 {"complexity", "O(1)"_RedisString},
                 {"arguments", RedisArray({
                                   RedisMap({
                                       {"name", "key"_RedisString},
                                       {"type", "key"_RedisString},
                                       {"display_text", "key"_RedisString},
                                       {"key_spec_index", 0_RedisInt},
                                   }),
                               })},
             })},
    {"spublish",
     RedisMap({
         {"summary", "Post a message to a shard channel"_RedisString},
         {"since", "7.0.0"_RedisString},
         {"group", "pubsub"_RedisString},
         {"complexity",
          "O(N) where N is the number of clients subscribed to the receiving shard channel."_RedisString},
         {"arguments", RedisArray({
                           RedisMap({
                               {"name", "shardchannel"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "shardchannel"_RedisString},
                           }),
                           RedisMap({
                               {"name", "message"_RedisString},
                               {"type", "string"_RedisString},
                               {"display_text", "message"_RedisString},
                           }),
                       })},
     })},
    {"bitpos",
     RedisMap({
         {"summary",
          "Finds the first set (1) or clear (0) bit in a string."_RedisString},
         {"since", "2.8.7"_RedisString},
         {"group", "bitmap"_RedisString},
         {"complexity", "O(N)"_RedisString},
         {"history",
          RedisSet({
              RedisArray({"7.0.0"_RedisString,
                          "Added the `BYTE|BIT` option."_RedisString}),
          })},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "bit"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "bit"_RedisString},
              }),
              RedisMap({
                  {"name", "range"_RedisString},
                  {"type", "block"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments",
                   RedisArray({
                       RedisMap({
                           {"name", "start"_RedisString},
                           {"type", "integer"_RedisString},
                           {"display_text", "start"_RedisString},
                       }),
                       RedisMap({
                           {"name", "end-unit-block"_RedisString},
                           {"type", "block"_RedisString},
                           {"flags", RedisArray({
                                         "optional"_RedisStatus,
                                     })},
                           {"arguments",
                            RedisArray({
                                RedisMap({
                                    {"name", "end"_RedisString},
                                    {"type", "integer"_RedisString},
                                    {"display_text", "end"_RedisString},
                                }),
                                RedisMap({
                                    {"name", "unit"_RedisString},
                                    {"type", "oneof"_RedisString},
                                    {"since", "7.0.0"_RedisString},
                                    {"flags", RedisArray({
                                                  "optional"_RedisStatus,
                                              })},
                                    {"arguments",
                                     RedisArray({
                                         RedisMap({
                                             {"name", "byte"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text",
                                              "byte"_RedisString},
                                             {"token", "BYTE"_RedisString},
                                         }),
                                         RedisMap({
                                             {"name", "bit"_RedisString},
                                             {"type", "pure-token"_RedisString},
                                             {"display_text",
                                              "bit"_RedisString},
                                             {"token", "BIT"_RedisString},
                                         }),
                                     })},
                                }),
                            })},
                       }),
                   })},
              }),
          })},
     })},
    {"zunionstore",
     RedisMap({
         {"summary",
          "Stores the union of multiple sorted sets in a key."_RedisString},
         {"since", "2.0.0"_RedisString},
         {"group", "sorted-set"_RedisString},
         {"complexity",
          "O(N)+O(M log(M)) with N being the sum of the sizes of the input sorted sets, and M being the number of elements in the resulting sorted set."_RedisString},
         {"arguments",
          RedisArray({
              RedisMap({
                  {"name", "destination"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "destination"_RedisString},
                  {"key_spec_index", 0_RedisInt},
              }),
              RedisMap({
                  {"name", "numkeys"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "numkeys"_RedisString},
              }),
              RedisMap({
                  {"name", "key"_RedisString},
                  {"type", "key"_RedisString},
                  {"display_text", "key"_RedisString},
                  {"key_spec_index", 1_RedisInt},
                  {"flags", RedisArray({
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "weight"_RedisString},
                  {"type", "integer"_RedisString},
                  {"display_text", "weight"_RedisString},
                  {"token", "WEIGHTS"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                                "multiple"_RedisStatus,
                            })},
              }),
              RedisMap({
                  {"name", "aggregate"_RedisString},
                  {"type", "oneof"_RedisString},
                  {"token", "AGGREGATE"_RedisString},
                  {"flags", RedisArray({
                                "optional"_RedisStatus,
                            })},
                  {"arguments", RedisArray({
                                    RedisMap({
                                        {"name", "sum"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "sum"_RedisString},
                                        {"token", "SUM"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "min"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "min"_RedisString},
                                        {"token", "MIN"_RedisString},
                                    }),
                                    RedisMap({
                                        {"name", "max"_RedisString},
                                        {"type", "pure-token"_RedisString},
                                        {"display_text", "max"_RedisString},
                                        {"token", "MAX"_RedisString},
                                    }),
                                })},
              }),
          })},
     })},
};