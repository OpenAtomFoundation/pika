// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/redis_parser.h"

#include <cassert> /* assert */

#include <glog/logging.h>

#include "pstd/include/pstd_string.h"
#include "pstd/include/xdebug.h"

namespace net {

static bool IsHexDigit(char ch) {
  return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F');
}

static int HexDigitToInt32(char ch) {
  if (ch <= '9' && ch >= '0') {
    return ch - '0';
  } else if (ch <= 'F' && ch >= 'A') {
    return ch - 'A';
  } else if (ch <= 'f' && ch >= 'a') {
    return ch - 'a';
  } else {
    return 0;
  }
}

static int split2args(const std::string& req_buf, RedisCmdArgsType& argv) {
  const char* p = req_buf.data();
  std::string arg;

  while (true) {
    // skip blanks
    while ((*p != 0) && (isspace(*p) != 0)) {
      p++;
    }
    if (*p != 0) {
      // get a token
      int inq = 0;   // set to 1 if we are in "quotes"
      int insq = 0;  // set to 1 if we are in 'single quotes'
      int done = 0;

      arg.clear();
      while (done == 0) {
        if (inq != 0) {
          if (*p == '\\' && *(p + 1) == 'x' && IsHexDigit(*(p + 2)) && IsHexDigit(*(p + 3))) {
            char byte = static_cast<char>(HexDigitToInt32(*(p + 2)) * 16 + HexDigitToInt32(*(p + 3)));
            arg.append(1, byte);
            p += 3;
          } else if (*p == '\\' && (*(p + 1) != 0)) {
            char c;

            p++;
            switch (*p) {
              case 'n':
                c = '\n';
                break;
              case 'r':
                c = '\r';
                break;
              case 't':
                c = '\t';
                break;
              case 'b':
                c = '\b';
                break;
              case 'a':
                c = '\a';
                break;
              default:
                c = *p;
                break;
            }
            arg.append(1, c);
          } else if (*p == '"') {
            /* closing quote must be followed by a space or
             * nothing at all. */
            if ((*(p + 1) != 0) && (isspace(*(p + 1)) == 0)) {
              argv.clear();
              return -1;
            }
            done = 1;
          } else if (*p == 0) {
            // unterminated quotes
            argv.clear();
            return -1;
          } else {
            arg.append(1, *p);
          }
        } else if (insq != 0) {
          if (*p == '\\' && *(p + 1) == '\'') {
            p++;
            arg.append(1, '\'');
          } else if (*p == '\'') {
            /* closing quote must be followed by a space or
             * nothing at all. */
            if ((*(p + 1) != 0) && (isspace(*(p + 1)) == 0)) {
              argv.clear();
              return -1;
            }
            done = 1;
          } else if (*p == 0) {
            // unterminated quotes
            argv.clear();
            return -1;
          } else {
            arg.append(1, *p);
          }
        } else {
          switch (*p) {
            case ' ':
            case '\n':
            case '\r':
            case '\t':
            case '\0':
              done = 1;
              break;
            case '"':
              inq = 1;
              break;
            case '\'':
              insq = 1;
              break;
            default:
              // current = sdscatlen(current,p,1);
              arg.append(1, *p);
              break;
          }
        }
        if (*p != 0) {
          p++;
        }
      }
      argv.push_back(arg);
    } else {
      return 0;
    }
  }
}

int RedisParser::FindNextSeparators() {
  if (cur_pos_ > length_ - 1) {
    return -1;
  }
  int pos = cur_pos_;
  while (pos <= length_ - 1) {
    if (input_buf_[pos] == '\n') {
      return pos;
    }
    pos++;
  }
  return -1;
}

int RedisParser::GetNextNum(int pos, long* value) {
  assert(pos > cur_pos_);
  //     cur_pos_       pos
  //      |    ----------|
  //      |    |
  //      *3\r\n
  // [cur_pos_ + 1, pos - cur_pos_ - 2]
  if (pstd::string2int(input_buf_ + cur_pos_ + 1, pos - cur_pos_ - 2, value) != 0) {
    return 0;  // Success
  }
  return -1;  // Failed
}

RedisParser::RedisParser() : redis_type_(0), bulk_len_(-1), redis_parser_type_(REDIS_PARSER_REQUEST) {}

void RedisParser::SetParserStatus(RedisParserStatus status, RedisParserError error) {
  if (status == kRedisParserHalf) {
    CacheHalfArgv();
  }
  status_code_ = status;
  error_code_ = error;
}

void RedisParser::CacheHalfArgv() {
  std::string tmp(input_buf_ + cur_pos_, length_ - cur_pos_);
  half_argv_ = tmp;
  cur_pos_ = length_;
}

RedisParserStatus RedisParser::RedisParserInit(RedisParserType type, const RedisParserSettings& settings) {
  if (status_code_ != kRedisParserNone) {
    SetParserStatus(kRedisParserError, kRedisParserInitError);
    return status_code_;
  }
  if (type != REDIS_PARSER_REQUEST && type != REDIS_PARSER_RESPONSE) {
    SetParserStatus(kRedisParserError, kRedisParserInitError);
    return status_code_;
  }
  redis_parser_type_ = type;
  parser_settings_ = settings;
  SetParserStatus(kRedisParserInitDone);
  return status_code_;
}

RedisParserStatus RedisParser::ProcessInlineBuffer() {
  int pos;
  int ret;
  pos = FindNextSeparators();
  if (pos == -1) {
    // change rbuf_len_ to length_
    if (length_ > REDIS_INLINE_MAXLEN) {
      SetParserStatus(kRedisParserError, kRedisParserFullError);
      return status_code_;
    } else {
      SetParserStatus(kRedisParserHalf);
      return status_code_;
    }
  }
  // args \r\n
  std::string req_buf(input_buf_ + cur_pos_, pos + 1 - cur_pos_);

  argv_.clear();
  ret = split2args(req_buf, argv_);
  cur_pos_ = pos + 1;

  if (ret == -1) {
    SetParserStatus(kRedisParserError, kRedisParserProtoError);
    return status_code_;
  }
  SetParserStatus(kRedisParserDone);
  return status_code_;
}

RedisParserStatus RedisParser::ProcessMultibulkBuffer() {
  int pos = 0;
  if (multibulk_len_ == 0) {
    /* The client should have been reset */
    pos = FindNextSeparators();
    if (pos != -1) {
      if (GetNextNum(pos, &multibulk_len_) != 0) {
        // Protocol error: invalid multibulk length
        SetParserStatus(kRedisParserError, kRedisParserProtoError);
        return status_code_;
      }
      cur_pos_ = pos + 1;
      argv_.clear();
      if (cur_pos_ > length_ - 1) {
        SetParserStatus(kRedisParserHalf);
        return status_code_;
      }
    } else {
      SetParserStatus(kRedisParserHalf);
      return status_code_;  // HALF
    }
  }
  while (multibulk_len_ != 0) {
    if (bulk_len_ == -1) {
      pos = FindNextSeparators();
      if (pos != -1) {
        if (input_buf_[cur_pos_] != '$') {
          SetParserStatus(kRedisParserError, kRedisParserProtoError);
          return status_code_;  // PARSE_ERROR
        }

        if (GetNextNum(pos, &bulk_len_) != 0) {
          // Protocol error: invalid bulk length
          SetParserStatus(kRedisParserError, kRedisParserProtoError);
          return status_code_;
        }
        cur_pos_ = pos + 1;
      }
      if (pos == -1 || cur_pos_ > length_ - 1) {
        SetParserStatus(kRedisParserHalf);
        return status_code_;
      }
    }
    if ((length_ - 1) - cur_pos_ + 1 < bulk_len_ + 2) {
      // Data not enough
      break;
    } else {
      argv_.emplace_back(input_buf_ + cur_pos_, bulk_len_);
      cur_pos_ = static_cast<int32_t>(cur_pos_ + bulk_len_ + 2);
      bulk_len_ = -1;
      multibulk_len_--;
    }
  }

  if (multibulk_len_ == 0) {
    SetParserStatus(kRedisParserDone);
    return status_code_;  // OK
  } else {
    SetParserStatus(kRedisParserHalf);
    return status_code_;  // HALF
  }
}

void RedisParser::PrintCurrentStatus() {
  LOG(INFO) << "status_code " << status_code_ << " error_code " << error_code_;
  LOG(INFO) << "multibulk_len_ " << multibulk_len_ << "bulk_len " << bulk_len_ << " redis_type " << redis_type_
            << " redis_parser_type " << redis_parser_type_;
  // for (auto& i : argv_) {
  //   UNUSED(i);
  //   log_info("parsed arguments: %s", i.c_str());
  // }
  LOG(INFO) << "cur_pos : " << cur_pos_;
  LOG(INFO) << "input_buf_ is clean ? " << (input_buf_ == nullptr);
  if (input_buf_) {
    LOG(INFO) << " input_buf " << input_buf_;
  }
  LOG(INFO) << "half_argv_ : " << half_argv_;
  LOG(INFO) << "input_buf len " << length_;
}

RedisParserStatus RedisParser::ProcessInputBuffer(const char* input_buf, int length, int* parsed_len) {
  if (status_code_ == kRedisParserInitDone || status_code_ == kRedisParserHalf || status_code_ == kRedisParserDone) {
    // TODO(): AZ: avoid copy
    std::string tmp_str(input_buf, length);
    input_str_ = half_argv_ + tmp_str;
    input_buf_ = input_str_.c_str();
    length_ = static_cast<int32_t>(length + half_argv_.size());
    if (redis_parser_type_ == REDIS_PARSER_REQUEST) {
      ProcessRequestBuffer();
    } else if (redis_parser_type_ == REDIS_PARSER_RESPONSE) {
      ProcessResponseBuffer();
    } else {
      SetParserStatus(kRedisParserError, kRedisParserInitError);
      return status_code_;
    }
    // cur_pos_ starts from 0, val of cur_pos_ is the parsed_len
    *parsed_len = cur_pos_;
    ResetRedisParser();
    // PrintCurrentStatus();
    return status_code_;
  }
  SetParserStatus(kRedisParserError, kRedisParserInitError);
  return status_code_;
}

// TODO(): AZ
RedisParserStatus RedisParser::ProcessResponseBuffer() {
  SetParserStatus(kRedisParserDone);
  return status_code_;
}

RedisParserStatus RedisParser::ProcessRequestBuffer() {
  RedisParserStatus ret;
  while (cur_pos_ <= length_ - 1) {
    if (redis_type_ == 0) {
      if (input_buf_[cur_pos_] == '*') {
        redis_type_ = REDIS_REQ_MULTIBULK;
      } else {
        redis_type_ = REDIS_REQ_INLINE;
      }
    }

    if (redis_type_ == REDIS_REQ_INLINE) {
      ret = ProcessInlineBuffer();
      if (ret != kRedisParserDone) {
        return ret;
      }
    } else if (redis_type_ == REDIS_REQ_MULTIBULK) {
      ret = ProcessMultibulkBuffer();
      if (ret != kRedisParserDone) {  // FULL_ERROR || HALF || PARSE_ERROR
        return ret;
      }
    } else {
      // Unknown requeset type;
      return kRedisParserError;
    }
    if (!argv_.empty()) {
      argvs_.push_back(argv_);
      if (parser_settings_.DealMessage) {
        if (parser_settings_.DealMessage(this, argv_) != 0) {
          SetParserStatus(kRedisParserError, kRedisParserDealError);
          return status_code_;
        }
      }
    }
    argv_.clear();
    // Reset
    ResetCommandStatus();
  }
  if (parser_settings_.Complete) {
    if (parser_settings_.Complete(this, argvs_) != 0) {
      SetParserStatus(kRedisParserError, kRedisParserCompleteError);
      return status_code_;
    }
  }
  argvs_.clear();
  SetParserStatus(kRedisParserDone);
  return status_code_;  // OK
}

void RedisParser::ResetCommandStatus() {
  redis_type_ = 0;
  multibulk_len_ = 0;
  bulk_len_ = -1;
  half_argv_.clear();
}

void RedisParser::ResetRedisParser() {
  cur_pos_ = 0;
  input_buf_ = nullptr;
  input_str_.clear();
  length_ = 0;
}

}  // namespace net
