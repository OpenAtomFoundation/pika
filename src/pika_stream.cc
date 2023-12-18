// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_stream.h"
#include <cassert>

#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "include/pika_stream_base.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
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

void ParseAddOrTrimArgsOrReply(CmdRes &res, const PikaCmdArgsType &argv, StreamAddTrimArgs &args, int *idpos,
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
      if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return;
      }
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && (next == "~" || next == "=")) {
        // we allways not do approx trim, so we ignore the ~ and =
        i++;
      }
      // parse threshold as uint64
      if (!StreamUtils::string2uint64(argv[i + 1].c_str(), args.maxlen)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid MAXLEN argument");
      }
      i++;
      args.trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_MAXLEN;
      args.trim_strategy_arg_idx = i;

    } else if (strcasecmp(opt.c_str(), "minid") == 0 && moreargs) {
      // case: XADD mystream ... MINID [= | ~] threshold ...
      if (args.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
        res.SetRes(CmdRes::kSyntaxErr, "syntax error, MAXLEN and MINID options at the same time are not compatible");
        return;
      }
      const auto &next = argv[i + 1];
      if (moreargs >= 2 && (next == "~" || next == "=") && next.size() == 1) {
        // we allways not do approx trim, so we ignore the ~ and =
        i++;
      }
      // parse threshold as stremID
      if (!StreamUtils::StreamParseID(argv[i + 1], args.minid, 0)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
        return;
      }
      i++;
      args.trim_strategy = StreamTrimStrategy::TRIM_STRATEGY_MINID;
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
      if (!StreamUtils::StreamParseStrictID(argv[i], args.id, 0, &args.seq_given)) {
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
void ParseReadOrReadGroupArgsOrReply(CmdRes &res, const PikaCmdArgsType &argv, StreamReadGroupReadArgs &args,
                                     bool is_xreadgroup) {
  int streams_arg_idx{0};  // the index of stream keys arg
  size_t streams_cnt{0};   // the count of stream keys

  for (int i = 1; i < argv.size(); ++i) {
    size_t moreargs = argv.size() - i - 1;
    const std::string &o = argv[i];
    if (strcasecmp(o.c_str(), "BLOCK") == 0 && moreargs) {
      i++;
      if (!StreamUtils::string2uint64(argv[i].c_str(), args.block)) {
        res.SetRes(CmdRes::kInvalidParameter, "Invalid BLOCK argument");
        return;
      }
    } else if (strcasecmp(o.c_str(), "COUNT") == 0 && moreargs) {
      i++;
      if (!StreamUtils::string2int32(argv[i].c_str(), args.count)) {
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

void AppendMessagesToRes(CmdRes &res, std::vector<storage::FieldValue> &field_values, const Slot *slot) {
  assert(slot);
  res.AppendArrayLenUint64(field_values.size());
  for (auto &fv : field_values) {
    std::vector<std::string> message;
    if (!StreamUtils::DeserializeMessage(fv.value, message)) {
      LOG(ERROR) << "Deserialize message failed";
      res.SetRes(CmdRes::kErrOther, "Deserialize message failed");
      return;
    }

    assert(message.size() % 2 == 0);
    res.AppendArrayLen(2);
    streamID sid;
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

  // check the conflict of stream used prefix, see detail in defination of STREAM_TREE_PREFIX
  if (key_ == STREAM_LAST_GENERATED_TREE_ID_FIELD) {
    res_.SetRes(CmdRes::kErrOther, "Can not use " + STREAM_LAST_GENERATED_TREE_ID_FIELD + "as stream key");
    return;
  }

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

void XAddCmd::Do(std::shared_ptr<Slot> slot) {
  // 1 get stream meta
  rocksdb::Status s;
  StreamMetaValue stream_meta;
  s = StreamStorage::GetStreamMeta(stream_meta, key_, slot.get());
  if (s.IsNotFound() && args_.no_mkstream) {
    res_.SetRes(CmdRes::kNotFound);
    return;
  } else if (s.IsNotFound()) {
    stream_meta.Init();
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, "error from XADD, get stream meta failed: " + s.ToString());
    return;
  }

  if (stream_meta.last_id().ms == UINT64_MAX && stream_meta.last_id().seq == UINT64_MAX) {
    res_.SetRes(CmdRes::kErrOther, "Fatal! Sequence number overflow !");
    return;
  }

  // 2 append the message to storage
  GenerateStreamIDOrReply(stream_meta);
  if (res_.ret() != CmdRes::kNone) {
    return;
  }

  // reset command's id in argvs if it not be given
  if (!args_.id_given || !args_.seq_given) {
    assert(field_pos_ > 0);
    argv_[field_pos_ - 1] = args_.id.ToString();
  }

  std::string message;
  if (!StreamUtils::SerializeMessage(argv_, message, field_pos_)) {
    res_.SetRes(CmdRes::kErrOther, "Serialize message failed");
    return;
  }

  // check the serialized current id is larger than last_id
#ifdef DEBUG
  std::string serialized_last_id;
  stream_meta.last_id().SerializeTo(serialized_last_id);
  assert(field > serialized_last_id);
#endif  // DEBUG

  s = StreamStorage::InsertStreamMessage(key_, args_.id, message, slot.get());
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, "error from XADD, insert stream message failed 1: " + s.ToString());
    return;
  }

  // 3 update stream meta
  if (stream_meta.length() == 0) {
    stream_meta.set_first_id(args_.id);
  }
  stream_meta.set_entries_added(stream_meta.entries_added() + 1);
  stream_meta.set_last_id(args_.id);
  stream_meta.set_length(stream_meta.length() + 1);

  // 4 trim the stream if needed
  if (args_.trim_strategy != StreamTrimStrategy::TRIM_STRATEGY_NONE) {
    int32_t count;
    TRY_CATCH_ERROR(StreamStorage::TrimStream(count, stream_meta, key_, args_, slot.get()), res_);
    (void)count;
  }

  // 5 update stream meta
  s = StreamStorage::SetStreamMeta(key_, stream_meta.value(), slot.get());
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, "error from XADD, get stream meta failed 2: " + s.ToString());
    return;
  }

  res_.AppendString(args_.id.ToString());
}

void XAddCmd::GenerateStreamIDOrReply(const StreamMetaValue &stream_meta) {
  auto &id = args_.id;
  if (args_.id_given && args_.seq_given && id.ms == 0 && id.seq == 0) {
    res_.SetRes(CmdRes::kInvalidParameter, "The ID specified in XADD must be greater than 0-0");
    return;
  }

  if (!args_.id_given || !args_.seq_given) {
    // if id not given, generate one
    if (!args_.id_given) {
      id.ms = StreamUtils::GetCurrentTimeMs();

      if (id.ms < stream_meta.last_id().ms) {
        id.ms = stream_meta.last_id().ms;
        if (stream_meta.last_id().seq == UINT64_MAX) {
          id.ms++;
          id.seq = 0;
        } else {
          id.seq++;
        }
        return;
      }
    }

    // generate seq
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms) {
      res_.SetRes(CmdRes::kErrOther, "The ID specified in XADD is equal or smaller");
      return;
    } else if (id.ms == last_id.ms) {
      if (last_id.seq == UINT64_MAX) {
        res_.SetRes(CmdRes::kErrOther, "The ID specified in XADD is equal or smaller");
        return;
      }
      id.seq = last_id.seq + 1;
    } else {
      id.seq = 0;
    }

  } else {
    //  Full ID given, check id
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms || (id.ms == last_id.ms && id.seq <= last_id.seq)) {
      res_.SetRes(CmdRes::kErrOther, "INVALID ID given");
      return;
    }
  }
}

void XRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRange);
    return;
  }
  key_ = argv_[1];
  if (!StreamUtils::StreamParseIntervalId(argv_[2], start_sid, &start_ex_, 0) ||
      !StreamUtils::StreamParseIntervalId(argv_[3], end_sid, &end_ex_, UINT64_MAX)) {
    res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return;
  }
  if (start_ex_ && start_sid.ms == UINT64_MAX && start_sid.seq == UINT64_MAX) {
    res_.SetRes(CmdRes::kInvalidParameter, "invalid start id");
    return;
  }
  if (end_ex_ && end_sid.ms == 0 && end_sid.seq == 0) {
    res_.SetRes(CmdRes::kInvalidParameter, "invalid end id");
    return;
  }
  if (argv_.size() == 6) {
    // pika's PKHScanRange() only sopport max count of INT32_MAX
    // but redis supports max count of UINT64_MAX
    if (!StreamUtils::string2int32(argv_[5].c_str(), count_)) {
      res_.SetRes(CmdRes::kInvalidParameter, "COUNT should be a integer greater than 0 and not bigger than INT32_MAX");
      return;
    }
  }
}

void XRangeCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<storage::FieldValue> field_values;

  if (start_sid <= end_sid) {
    StreamStorage::ScanStreamOptions options(key_, start_sid, end_sid, count_, start_ex_, end_ex_, false);
    std::string next_field;
    auto s = StreamStorage::ScanStream(options, field_values, next_field, slot.get());
    (void)next_field;
    if (!s.ok() && !s.IsNotFound()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }

  AppendMessagesToRes(res_, field_values, slot.get());
}

void XRevrangeCmd::Do(std::shared_ptr<Slot> slot) {
  std::vector<storage::FieldValue> field_values;

  if (start_sid >= end_sid) {
    StreamStorage::ScanStreamOptions options(key_, start_sid, end_sid, count_, start_ex_, end_ex_, true);
    std::string next_field;
    auto s = StreamStorage::ScanStream(options, field_values, next_field, slot.get());
    (void)next_field;
    if (!s.ok() && !s.IsNotFound()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }

  AppendMessagesToRes(res_, field_values, slot.get());
}

inline void XDelCmd::SetFirstIDOrReply(StreamMetaValue &stream_meta, const Slot *slot) {
  assert(slot);
  return SetFirstOrLastIDOrReply(stream_meta, slot, true);
}

inline void XDelCmd::SetLastIDOrReply(StreamMetaValue &stream_meta, const Slot *slot) {
  assert(slot);
  return SetFirstOrLastIDOrReply(stream_meta, slot, false);
}

inline void XDelCmd::SetFirstOrLastIDOrReply(StreamMetaValue &stream_meta, const Slot *slot, bool is_set_first) {
  assert(slot);
  if (stream_meta.length() == 0) {
    stream_meta.set_first_id(kSTREAMID_MIN);
    return;
  }

  std::vector<storage::FieldValue> field_values;
  std::string next_field;

  storage::Status s;
  if (is_set_first) {
    StreamStorage::ScanStreamOptions option(key_, kSTREAMID_MIN, kSTREAMID_MAX, 1);
    s = StreamStorage::ScanStream(option, field_values, next_field, slot);
  } else {
    bool is_reverse = true;
    StreamStorage::ScanStreamOptions option(key_, kSTREAMID_MAX, kSTREAMID_MIN, 1, false, false, is_reverse);
    s = StreamStorage::ScanStream(option, field_values, next_field, slot);
  }
  (void)next_field;

  if (!s.ok() && !s.IsNotFound()) {
    LOG(ERROR) << "Internal error: scan stream failed: " << s.ToString();
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  if (field_values.empty()) {
    LOG(ERROR) << "Internal error: no messages found but stream length is not 0";
    res_.SetRes(CmdRes::kErrOther, "Internal error: no messages found but stream length is not 0");
    return;
  }

  streamID id;
  id.DeserializeFrom(field_values[0].field);
  stream_meta.set_first_id(id);
}

void XDelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXDel);
    return;
  }

  key_ = argv_[1];
  for (int i = 2; i < argv_.size(); i++) {
    streamID id;
    if (!StreamUtils::StreamParseStrictID(argv_[i], id, 0, nullptr)) {
      res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
      return;
    }
    if (res_.ret() != CmdRes::kNone) {
      return;
    }
    ids_.emplace_back(id);
  }
}

void XDelCmd::Do(std::shared_ptr<Slot> slot) {
  // 1 try to get stream meta
  StreamMetaValue stream_meta;
  auto s = StreamStorage::GetStreamMeta(stream_meta, key_, slot.get());
  if (s.IsNotFound()) {
    res_.AppendInteger(0);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 2 do the delete
  int32_t count{0};
  s = StreamStorage::DeleteStreamMessage(key_, ids_, count, slot.get());
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 3 update stream meta
  stream_meta.set_length(stream_meta.length() - count);
  for (const auto &id : ids_) {
    if (id > stream_meta.max_deleted_entry_id()) {
      stream_meta.set_max_deleted_entry_id(id);
    }
    if (id == stream_meta.first_id()) {
      SetFirstIDOrReply(stream_meta, slot.get());
    } else if (id == stream_meta.last_id()) {
      SetLastIDOrReply(stream_meta, slot.get());
    }
  }

  s = StreamStorage::SetStreamMeta(key_, stream_meta.value(), slot.get());
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
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

void XLenCmd::Do(std::shared_ptr<Slot> slot) {
  rocksdb::Status s;
  StreamMetaValue stream_meta;
  s = StreamStorage::GetStreamMeta(stream_meta, key_, slot.get());
  if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kNotFound);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  if (stream_meta.length() > INT_MAX) {
    res_.SetRes(CmdRes::kErrOther, "stream's length is larger than INT_MAX");
    return;
  }
  res_.AppendInteger(static_cast<int32_t>(stream_meta.length()));
  return;
}

void XReadCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRead);
    return;
  }

  ParseReadOrReadGroupArgsOrReply(res_, argv_, args_, false);
}

void XReadCmd::Do(std::shared_ptr<Slot> slot) {
  rocksdb::Status s;

  // 1 prepare stream_metas
  std::vector<std::pair<StreamMetaValue, int>> streammeta_idx;
  for (int i = 0; i < args_.unparsed_ids.size(); i++) {
    const auto &key = args_.keys[i];
    const auto &unparsed_id = args_.unparsed_ids[i];

    StreamMetaValue stream_meta;
    auto s = StreamStorage::GetStreamMeta(stream_meta, key, slot.get());
    if (s.IsNotFound()) {
      continue;
    } else if (!s.ok()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }

    streammeta_idx.emplace_back(std::move(stream_meta), i);
  }

  if (streammeta_idx.empty()) {
    res_.AppendArrayLen(-1);
    return;
  }

  // 2 do the scan
  res_.AppendArrayLenUint64(streammeta_idx.size());
  for (const auto &stream_meta_id : streammeta_idx) {
    const auto &stream_meta = stream_meta_id.first;
    const auto &idx = stream_meta_id.second;
    const auto &unparsed_id = args_.unparsed_ids[idx];
    const auto &key = args_.keys[idx];

    // 2.1 try to parse id
    streamID id;
    if (unparsed_id == "<") {
      res_.SetRes(CmdRes::kSyntaxErr,
                  "The > ID can be specified only when calling "
                  "XREADGROUP using the GROUP <group> "
                  "<consumer> option.");
      return;
    } else if (unparsed_id == "$") {
      id = stream_meta.last_id();
    } else {
      if (!StreamUtils::StreamParseStrictID(unparsed_id, id, 0, nullptr)) {
        res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
        return;
      }
    }

    // 2.2 scan
    std::vector<storage::FieldValue> field_values;
    std::string next_field;
    StreamStorage::ScanStreamOptions options(key, id, kSTREAMID_MAX, args_.count, true);
    auto s = StreamStorage::ScanStream(options, field_values, next_field, slot.get());
    (void)next_field;
    if (!s.ok() && !s.IsNotFound()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }

    res_.AppendArrayLen(2);
    res_.AppendString(key);
    AppendMessagesToRes(res_, field_values, slot.get());
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

void XTrimCmd::Do(std::shared_ptr<Slot> slot) {
  // 1 try to get stream meta, if not found, return error
  StreamMetaValue stream_meta;
  auto s = StreamStorage::GetStreamMeta(stream_meta, key_, slot.get());
  if (s.IsNotFound()) {
    res_.AppendInteger(0);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 2 do the trim
  int32_t count{0};
  TRY_CATCH_ERROR(StreamStorage::TrimStream(count, stream_meta, key_, args_, slot.get()), res_);

  // 3 update stream meta
  TRY_CATCH_ERROR(StreamStorage::SetStreamMeta(key_, stream_meta.value(), slot.get()), res_);

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
      if (argv_.size() > 4 && !StreamUtils::string2uint64(argv_[4].c_str(), count_)) {
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

void XInfoCmd::Do(std::shared_ptr<Slot> slot) {
  if (!strcasecmp(subcmd_.c_str(), "STREAM")) {
    this->StreamInfo(slot);
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

void XInfoCmd::StreamInfo(std::shared_ptr<Slot> &slot) {
  // 1 try to get stream meta
  StreamMetaValue stream_meta;
  TRY_CATCH_ERROR(StreamStorage::GetStreamMeta(stream_meta, key_, slot.get()), res_);

  // 2 append the stream info
  res_.AppendArrayLen(10);
  res_.AppendString("length");
  res_.AppendInteger(static_cast<int64_t>(stream_meta.length()));
  res_.AppendString("last-generated-id");
  res_.AppendString(stream_meta.last_id().ToString());
  res_.AppendString("max-deleted-entry-id");
  res_.AppendString(stream_meta.max_deleted_entry_id().ToString());
  res_.AppendString("entries-added");
  res_.AppendInteger(static_cast<int64_t>(stream_meta.entries_added()));
  res_.AppendString("recorded-first-entry-id");
  res_.AppendString(stream_meta.first_id().ToString());

  // Korpse TODO: add group info
}
