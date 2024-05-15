//  Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_stream.h"
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "glog/logging.h"
#include "include/pika_command.h"
#include "include/pika_db.h"
#include "include/pika_slot_command.h"
#include "include/pika_define.h"
#include "storage/storage.h"

// s : rocksdb::Status
// res : CmdRes
#define TRY_CATCH_ERROR(s, res)                    \
  do {                                             \
    if (!s.ok()) {                                 \
      LOG(ERROR) << s.ToString();                  \
      res.SetRes(CmdRes::kErrOther, s.ToString()); \
      return;                                      \
    }                                              \
  } while (0)

void ParseAddOrTrimArgsOrReply(CmdRes &res, const PikaCmdArgsType &argv, storage::StreamAddTrimArgs &args, int *idpos,
                               bool is_xadd) {
  int i = 2;
  bool limit_given = false;
  for (; i < argv.size(); ++i) {
    size_t moreargs = argv.size() - 1 - i;
    const std::string &opt = argv[i];

    if (is_xadd && strcasecmp(opt.c_str(), "*") == 0 && opt.size() == 1) {
      // case: XADD mystream * field value [field value ...]
      break;

    } else if (strcasecmp(opt.c_str(), "maxlen") == 0 && moreargs) {
      // case: XADD mystream ... MAXLEN [= | ~] threshold ...
      if (args.trim_strategy != storage::StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return;
      }
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && (next == "~" || next == "=")) {
        // we allways not do approx trim, so we ignore the ~ and =
        i++;
      }
      // parse threshold as uint64
      if (!storage::StreamUtils::string2uint64(argv[i + 1].c_str(), args.maxlen)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid MAXLEN argument");
      }
      i++;
      args.trim_strategy = storage::StreamTrimStrategy::TRIM_STRATEGY_MAXLEN;
      args.trim_strategy_arg_idx = i;

    } else if (strcasecmp(opt.c_str(), "minid") == 0 && moreargs) {
      // case: XADD mystream ... MINID [= | ~] threshold ...
      if (args.trim_strategy != storage::StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return;
      }
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && (next == "~" || next == "=") && next.size() == 1) {
        // we allways not do approx trim, so we ignore the ~ and =
        i++;
      }
      // parse threshold as stremID
      if (!storage::StreamUtils::StreamParseID(argv[i + 1], args.minid, 0)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
        return;
      }
      i++;
      args.trim_strategy = storage::StreamTrimStrategy::TRIM_STRATEGY_MINID;
      args.trim_strategy_arg_idx = i;

    } else if (strcasecmp(opt.c_str(), "limit") == 0 && moreargs) {
      // case: XADD mystream ... ~ threshold LIMIT count ...
      // we do not need approx trim, so we do not support LIMIT option
      res.SetRes(CmdRes::kSyntaxErr, "syntax error, Pika do not support LIMIT option");
      return;

    } else if (is_xadd && strcasecmp(opt.c_str(), "nomkstream") == 0) {
      // case: XADD mystream ... NOMKSTREAM ...
      args.no_mkstream = true;

    } else if (is_xadd) {
      // case: XADD mystream ... ID ...
      if (!storage::StreamUtils::StreamParseStrictID(argv[i], args.id, 0, &args.seq_given)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
        return;
      }
      args.id_given = true;
      break;
    } else {
      res.SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }  // end for

  if (idpos) {
    *idpos = i;
  } else if (is_xadd) {
    res.SetRes(CmdRes::kErrOther, "idpos is null, xadd comand must parse idpos");
  }
}

/* XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds]
 * [NOACK] STREAMS key [key ...] id [id ...]
 * XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id
 * [id ...] */
void ParseReadOrReadGroupArgsOrReply(CmdRes &res, const PikaCmdArgsType &argv, storage::StreamReadGroupReadArgs &args,
                                     bool is_xreadgroup) {
  int streams_arg_idx{0};  // the index of stream keys arg
  size_t streams_cnt{0};   // the count of stream keys

  for (int i = 1; i < argv.size(); ++i) {
    size_t moreargs = argv.size() - i - 1;
    const std::string &o = argv[i];
    if (strcasecmp(o.c_str(), "BLOCK") == 0 && moreargs) {
      i++;
      if (!storage::StreamUtils::string2uint64(argv[i].c_str(), args.block)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid BLOCK argument");
        return;
      }
    } else if (strcasecmp(o.c_str(), "COUNT") == 0 && moreargs) {
      i++;
      if (!storage::StreamUtils::string2int32(argv[i].c_str(), args.count)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid COUNT argument");
        return;
      }
      if (args.count < 0) args.count = 0;
    } else if (strcasecmp(o.c_str(), "STREAMS") == 0 && moreargs) {
      streams_arg_idx = i + 1;
      streams_cnt = argv.size() - streams_arg_idx;
      if (streams_cnt % 2 != 0) {
        res.SetRes(CmdRes::kSyntaxErr, "Unbalanced list of streams: for each stream key an ID must be specified");
        return;
      }
      streams_cnt /= 2;
      break;
    } else if (strcasecmp(o.c_str(), "GROUP") == 0 && moreargs >= 2) {
      if (!is_xreadgroup) {
        res.SetRes(CmdRes::kSyntaxErr, "The GROUP option is only supported by XREADGROUP. You called XREAD instead.");
        return;
      }
      args.group_name = argv[i + 1];
      args.consumer_name = argv[i + 2];
      i += 2;
    } else if (strcasecmp(o.c_str(), "NOACK") == 0) {
      if (!is_xreadgroup) {
        res.SetRes(CmdRes::kSyntaxErr, "The NOACK option is only supported by XREADGROUP. You called XREAD instead.");
        return;
      }
      args.noack_ = true;
    } else {
      res.SetRes(CmdRes::kSyntaxErr);
      return;
    }
  }

  if (streams_arg_idx == 0) {
    res.SetRes(CmdRes::kSyntaxErr);
    return;
  }

  if (is_xreadgroup && args.group_name.empty()) {
    res.SetRes(CmdRes::kSyntaxErr, "Missing GROUP option for XREADGROUP");
    return;
  }

  // collect keys and ids
  for (auto i = streams_arg_idx + streams_cnt; i < argv.size(); ++i) {
    auto key_idx = i - streams_cnt;
    args.keys.push_back(argv[key_idx]);
    args.unparsed_ids.push_back(argv[i]);
    const std::string &key = argv[i - streams_cnt];
  }
}

void AppendMessagesToRes(CmdRes &res, std::vector<storage::IdMessage> &id_messages, const DB* db) {
  assert(db);
  res.AppendArrayLenUint64(id_messages.size());
  for (auto &fv : id_messages) {
    std::vector<std::string> message;
    if (!storage::StreamUtils::DeserializeMessage(fv.value, message)) {
      LOG(ERROR) << "Deserialize message failed";
      res.SetRes(CmdRes::kErrOther, "Deserialize message failed");
      return;
    }

    assert(message.size() % 2 == 0);
    res.AppendArrayLen(2);
    storage::streamID sid;
    sid.DeserializeFrom(fv.field);
    res.AppendString(sid.ToString());  // field here is the stream id
    res.AppendArrayLenUint64(message.size());
    for (auto &m : message) {
      res.AppendString(m);
    }
  }
}

void XAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXAdd);
    return;
  }
  key_ = argv_[1];

  int idpos{-1};
  ParseAddOrTrimArgsOrReply(res_, argv_, args_, &idpos, true);
  if (res_.ret() != CmdRes::kNone) {
    return;
  } else if (idpos < 0) {
    LOG(ERROR) << "Invalid idpos: " << idpos;
    res_.SetRes(CmdRes::kErrOther);
    return;
  }

  field_pos_ = idpos + 1;
  if ((argv_.size() - field_pos_) % 2 == 1 || (argv_.size() - field_pos_) < 2) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXAdd);
    return;
  }
}

void XAddCmd::Do() {
  std::string message;
  if (!storage::StreamUtils::SerializeMessage(argv_, message, field_pos_)) {
    res_.SetRes(CmdRes::kErrOther, "Serialize message failed");
    return;
  }

  auto s = db_->storage()->XAdd(key_, message, args_);
  if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // reset command's id in argvs if it not be given
  if (!args_.id_given || !args_.seq_given) {
    assert(field_pos_ > 0);
    argv_[field_pos_ - 1] = args_.id.ToString();
  }

  res_.AppendString(args_.id.ToString());
  AddSlotKey("m", key_, db_);
}

void XRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRange);
    return;
  }
  key_ = argv_[1];
  if (!storage::StreamUtils::StreamParseIntervalId(argv_[2], args_.start_sid, &args_.start_ex, 0) ||
      !storage::StreamUtils::StreamParseIntervalId(argv_[3], args_.end_sid, &args_.end_ex, UINT64_MAX)) {
    res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return;
  }
  if (args_.start_ex && args_.start_sid.ms == UINT64_MAX && args_.start_sid.seq == UINT64_MAX) {
    res_.SetRes(CmdRes::kInvalidParameter, "invalid start id");
    return;
  }
  if (args_.end_ex && args_.end_sid.ms == 0 && args_.end_sid.seq == 0) {
    res_.SetRes(CmdRes::kInvalidParameter, "invalid end id");
    return;
  }
  if (argv_.size() == 6) {
    if (!storage::StreamUtils::string2int32(argv_[5].c_str(), args_.limit)) {
      res_.SetRes(CmdRes::kInvalidParameter, "COUNT should be a integer greater than 0 and not bigger than INT32_MAX");
      return;
    }
  }
}

void XRangeCmd::Do() {
  std::vector<storage::IdMessage> id_messages;

  if (args_.start_sid <= args_.end_sid) {
    auto s = db_->storage()->XRange(key_, args_, id_messages);
    if (s_.IsInvalidArgument()) {
      res_.SetRes(CmdRes::kMultiKey);
      return;
    } else if (!s.ok() && !s.IsNotFound()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }
  AppendMessagesToRes(res_, id_messages, db_.get());
}

void XRevrangeCmd::Do() {
  std::vector<storage::IdMessage> id_messages;

  if (args_.start_sid >= args_.end_sid) {
    auto s = db_->storage()->XRevrange(key_, args_, id_messages);
    if (s_.IsInvalidArgument()) {
      res_.SetRes(CmdRes::kMultiKey);
      return;
    } else if (!s.ok() && !s.IsNotFound()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }

  AppendMessagesToRes(res_, id_messages, db_.get());
}

void XDelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXAdd);
    return;
  }

  key_ = argv_[1];
  for (int i = 2; i < argv_.size(); i++) {
    storage::streamID id;
    if (!storage::StreamUtils::StreamParseStrictID(argv_[i], id, 0, nullptr)) {
      res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
      return;
    }
    if (res_.ret() != CmdRes::kNone) {
      return;
    }
    ids_.emplace_back(id);
  }
}

void XDelCmd::Do() {
  int32_t count{0};
  auto s = db_->storage()->XDel(key_, ids_, count);
  if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }

  if (count > INT_MAX) {
    return res_.SetRes(CmdRes::kErrOther, "count is larger than INT_MAX");
  }

  res_.AppendInteger(count);
}

void XLenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXLen);
    return;
  }
  key_ = argv_[1];
}

void XLenCmd::Do() {
  int32_t len{0};
  auto s = db_->storage()->XLen(key_, len);
  if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kNotFound);
    return;
  } else if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  if (len > INT_MAX) {
    return res_.SetRes(CmdRes::kErrOther, "stream's length is larger than INT_MAX");
  }

  res_.AppendInteger(len);
  return;
}

void XReadCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRead);
    return;
  }

  ParseReadOrReadGroupArgsOrReply(res_, argv_, args_, false);
}

void XReadCmd::Do() {
  std::vector<std::vector<storage::IdMessage>> results;
  // The wrong key will not trigger error, just be ignored,
  // we need to save the right key，and return it to client.
  std::vector<std::string> reserved_keys;
  auto s = db_->storage()->XRead(args_, results, reserved_keys);

  if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
  } else if (!s.ok() && s.ToString() ==
                     "The > ID can be specified only when calling "
                     "XREADGROUP using the GROUP <group> "
                     "<consumer> option.") {
    res_.SetRes(CmdRes::kSyntaxErr, s.ToString());
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }

  if (results.empty()) {
    res_.AppendArrayLen(-1);
    return;
  }

  assert(results.size() == reserved_keys.size());

  // 2 do the scan
  res_.AppendArrayLenUint64(results.size());
  for (size_t i = 0; i < results.size(); ++i) {
    res_.AppendArrayLen(2);
    res_.AppendString(reserved_keys[i]);
    AppendMessagesToRes(res_, results[i], db_.get());
  }
}

void XTrimCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXTrim);
    return;
  }

  key_ = argv_[1];
  ParseAddOrTrimArgsOrReply(res_, argv_, args_, nullptr, false);
  if (res_.ret() != CmdRes::kNone) {
    return;
  }
}

void XTrimCmd::Do() {
  int32_t count{0};
  auto s = db_->storage()->XTrim(key_, args_, count);
  if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
    return;
  } else if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  if (count > INT_MAX) {
    return res_.SetRes(CmdRes::kErrOther, "count is larger than INT_MAX");
  }

  res_.AppendInteger(count);
  return;
}

void XInfoCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXInfo);
    return;
  }

  subcmd_ = argv_[1];
  key_ = argv_[2];
  if (!strcasecmp(subcmd_.c_str(), "STREAM")) {
    if (argv_.size() > 3 && strcasecmp(subcmd_.c_str(), "FULL") != 0) {
      is_full_ = true;
      if (argv_.size() > 4 && !storage::StreamUtils::string2uint64(argv_[4].c_str(), count_)) {
        res_.SetRes(CmdRes::kInvalidParameter, "invalid count");
        return;
      }
    } else if (argv_.size() > 3) {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }

  } else if (!strcasecmp(subcmd_.c_str(), "GROUPS")) {
    if (argv_.size() != 3) {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    cgroupname_ = argv_[3];

  } else if (!strcasecmp(subcmd_.c_str(), "CONSUMERS")) {
    if (argv_.size() != 4) {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    cgroupname_ = argv_[3];
    consumername_ = argv_[4];
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
}

void XInfoCmd::Do() {
  if (!strcasecmp(subcmd_.c_str(), "STREAM")) {
    this->StreamInfo(db_);
  } else if (!strcasecmp(subcmd_.c_str(), "GROUPS")) {
    // Korpse: TODO:
    // this->GroupsInfo(slot);
  } else if (!strcasecmp(subcmd_.c_str(), "CONSUMERS")) {
    // Korpse: TODO:
    // this->ConsumersInfo(slot);
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
}

void XInfoCmd::StreamInfo(std::shared_ptr<DB>& db) {
  storage::StreamInfoResult info;
  auto s = db_->storage()->XInfo(key_, info);
  if (s_.IsInvalidArgument()) {
    res_.SetRes(CmdRes::kMultiKey);
    return;
  } else if (!s.ok() && !s.IsNotFound()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  } else if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kNotFound);
    return;
  }

  // // 2 append the stream info
  res_.AppendArrayLen(10);
  res_.AppendString("length");
  res_.AppendInteger(static_cast<int64_t>(info.length));
  res_.AppendString("last-generated-id");
  res_.AppendString(info.last_id_str);
  res_.AppendString("max-deleted-entry-id");
  res_.AppendString(info.max_deleted_entry_id_str);
  res_.AppendString("entries-added");
  res_.AppendInteger(static_cast<int64_t>(info.entries_added));
  res_.AppendString("recorded-first-entry-id");
  res_.AppendString(info.first_id_str);
}
