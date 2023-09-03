#include "include/pika_stream.h"
#include <bits/stdint-uintn.h>
#include <strings.h>
#include <sys/types.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "include/pika_command.h"
#include "include/pika_data_distribution.h"
#include "include/pika_slot.h"
#include "include/pika_slot_command.h"
#include "include/pika_stream_base.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "pstd/include/pstd_string.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
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

void XAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXAdd);
    return;
  }
  key_ = argv_[1];
  int idpos{-1};
  StreamCmdBase::ParseAddOrTrimArgsOrReply(res_, argv_, args_, &idpos, true);
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
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  if (stream_meta.last_id().ms == UINT64_MAX && stream_meta.last_id().seq == UINT64_MAX) {
    LOG(ERROR) << "Fatal! Sequence number overflow !";
    res_.SetRes(CmdRes::kErrOther, "Fatal! Sequence number overflow !");
    return;
  }

  // 2 append the message to storage
  GenerateStreamIDOrReply(stream_meta);
  if (res_.ret() != CmdRes::kNone) {
    return;
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
    LOG(ERROR) << "Insert stream message failed";
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
    StreamCmdBase::TrimStreamOrReply(res_, stream_meta, key_, args_, slot.get());
    if (res_.ret() != CmdRes::kNone) {
      return;
    }
  }

  // 5 update stream meta
  s = StreamStorage::SetStreamMeta(key_, stream_meta.value(), slot.get());
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
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
      LOG(ERROR) << "Time backwards detected !";
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

  StreamCmdBase::AppendMessagesToRes(res_, field_values, slot.get());
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

  StreamCmdBase::AppendMessagesToRes(res_, field_values, slot.get());
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
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXAdd);
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
  res_.AppendInteger(static_cast<int>(stream_meta.length()));
  return;
}

void XReadCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRead);
    return;
  }

  StreamCmdBase::ParseReadOrReadGroupArgsOrReply(res_, argv_, args_, false);
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
    StreamCmdBase::AppendMessagesToRes(res_, field_values, slot.get());
  }
}

void XTrimCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXTrim);
    return;
  }

  key_ = argv_[1];
  StreamCmdBase::ParseAddOrTrimArgsOrReply(res_, argv_, args_, nullptr, true);
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
  auto count = StreamCmdBase::TrimStreamOrReply(res_, stream_meta, key_, args_, slot.get());

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