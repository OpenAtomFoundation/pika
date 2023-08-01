// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_REDIS_PARSER_H_
#define NET_INCLUDE_REDIS_PARSER_H_

#include "net/include/net_define.h"

#include <vector>

#define REDIS_PARSER_REQUEST 1
#define REDIS_PARSER_RESPONSE 2

namespace net {

class RedisParser;

using RedisCmdArgsType = std::vector<std::string>;
using RedisParserDataCb = int32_t (*)(RedisParser *, const RedisCmdArgsType &);
using RedisParserMultiDataCb = int32_t (*)(RedisParser *, const std::vector<RedisCmdArgsType> &);
using RedisParserCb = int32_t (*)(RedisParser *);
using RedisParserType = int32_t;

enum RedisParserStatus {
  kRedisParserNone = 0,
  kRedisParserInitDone = 1,
  kRedisParserHalf = 2,
  kRedisParserDone = 3,
  kRedisParserError = 4,
};

enum RedisParserError {
  kRedisParserOk = 0,
  kRedisParserInitError = 1,
  kRedisParserFullError = 2,  // input overwhelm internal buffer
  kRedisParserProtoError = 3,
  kRedisParserDealError = 4,
  kRedisParserCompleteError = 5,
};

struct RedisParserSettings {
  RedisParserDataCb DealMessage;
  RedisParserMultiDataCb Complete;
  RedisParserSettings() {
    DealMessage = nullptr;
    Complete = nullptr;
  }
};

class RedisParser {
 public:
  RedisParser();
  RedisParserStatus RedisParserInit(RedisParserType type, const RedisParserSettings& settings);
  RedisParserStatus ProcessInputBuffer(const char* input_buf, int32_t length, int32_t* parsed_len);
  int32_t get_bulk_len() { return bulk_len_; }
  RedisParserError get_error_code() { return error_code_; }
  void* data = nullptr; /* A pointer to get hook to the "connection" or "socket" object */
 private:
  // for DEBUG
  void PrintCurrentStatus();

  void CacheHalfArgv();
  int32_t FindNextSeparators();
  int32_t GetNextNum(int32_t pos, int32_t* value);
  RedisParserStatus ProcessInlineBuffer();
  RedisParserStatus ProcessMultibulkBuffer();
  RedisParserStatus ProcessRequestBuffer();
  RedisParserStatus ProcessResponseBuffer();
  void SetParserStatus(RedisParserStatus status, RedisParserError error = kRedisParserOk);
  void ResetRedisParser();
  void ResetCommandStatus();

  RedisParserSettings parser_settings_;
  RedisParserStatus status_code_{kRedisParserNone};
  RedisParserError error_code_{kRedisParserOk};

  int32_t redis_type_ = -1;  // REDIS_REQ_INLINE or REDIS_REQ_MULTIBULK

  int32_t multibulk_len_ = 0;
  int32_t bulk_len_ = 0;
  std::string half_argv_;

  int32_t redis_parser_type_ = -1;  // REDIS_PARSER_REQUEST or REDIS_PARSER_RESPONSE

  RedisCmdArgsType argv_;
  std::vector<RedisCmdArgsType> argvs_;

  int32_t cur_pos_ = 0;
  const char* input_buf_{nullptr};
  std::string input_str_;
  int32_t length_ = 0;
};

}  // namespace net
#endif  // NET_INCLUDE_REDIS_PARSER_H_
