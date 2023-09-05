/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd_context.h"

namespace pikiwidb {

std::string CmdRes::message() const {
  std::string result;
  switch (ret_) {
    case kNone:
      return message_;
    case kOk:
      return "+OK\r\n";
    case kPong:
      return "+PONG\r\n";
    case kSyntaxErr:
      return "-ERR syntax error\r\n";
    case kInvalidInt:
      return "-ERR value is not an integer or out of range\r\n";
    case kInvalidBitInt:
      return "-ERR bit is not an integer or out of range\r\n";
    case kInvalidBitOffsetInt:
      return "-ERR bit offset is not an integer or out of range\r\n";
    case kWrongBitOpNotNum:
      return "-ERR BITOP NOT must be called with a single source key.\r\n";

    case kInvalidBitPosArgument:
      return "-ERR The bit argument must be 1 or 0.\r\n";
    case kInvalidFloat:
      return "-ERR value is not a valid float\r\n";
    case kOverFlow:
      return "-ERR increment or decrement would overflow\r\n";
    case kNotFound:
      return "-ERR no such key\r\n";
    case kOutOfRange:
      return "-ERR index out of range\r\n";
    case kInvalidPwd:
      return "-ERR invalid password\r\n";
    case kNoneBgsave:
      return "-ERR No BGSave Works now\r\n";
    case kPurgeExist:
      return "-ERR binlog already in purging...\r\n";
    case kInvalidParameter:
      return "-ERR Invalid Argument\r\n";
    case kWrongNum:
      result = "-ERR wrong number of arguments for '";
      result.append(message_);
      result.append("' command\r\n");
      break;
    case kInvalidIndex:
      result = "-ERR invalid DB index for '";
      result.append(message_);
      result.append("'\r\n");
      break;
    case kInvalidDbType:
      result = "-ERR invalid DB for '";
      result.append(message_);
      result.append("'\r\n");
      break;
    case kInconsistentHashTag:
      return "-ERR parameters hashtag is inconsistent\r\n";
    case kInvalidDB:
      result = "-ERR invalid DB for '";
      result.append(message_);
      result.append("'\r\n");
      break;
    case kErrOther:
      result = "-ERR ";
      result.append(message_);
      result.append(NewLine);
      break;
    case KIncrByOverFlow:
      result = "-ERR increment would produce NaN or Infinity";
      result.append(message_);
      result.append(NewLine);
      break;
    default:
      break;
  }
  return result;
}

}  // namespace pikiwidb