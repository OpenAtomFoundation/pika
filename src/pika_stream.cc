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

#define TRY_CATCH_ERROR(s, res)                    \
  do {                                             \
    if (!s.ok()) {                                 \
      LOG(ERROR) << s.ToString();                  \
      res.SetRes(CmdRes::kErrOther, s.ToString()); \
      return;                                      \
    }                                              \
  } while (0)

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

void XReadGroupCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRead);
    return;
  }
  StreamCmdBase::ParseReadOrReadGroupArgsOrReply(res_, argv_, args_, false);
}

void XReadGroupCmd::Do(std::shared_ptr<Slot> slot) {
  assert(args_.keys.size() == args_.unparsed_ids.size());
  rocksdb::Status s;
  streamID id;

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

    // 2.1 try to find cgroup_meta
    const auto tid = stream_meta.groups_id();
    std::string cgroup_meta_str;
    StreamCGroupMetaValue cgroup_meta;
    s = StreamStorage::GetTreeNodeValue(tid, args_.group_name, cgroup_meta_str, slot.get());
    if (s.IsNotFound()) {
      LOG(WARNING) << "CGroup meta not found";
      res_.SetRes(CmdRes::kInvalidParameter, "-NOGROUP No such key " + key + " or consumer group " + args_.group_name +
                                                 " in XREADGROUP with GROUP option");
      return;
    } else if (!s.ok()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
    cgroup_meta.ParseFrom(cgroup_meta_str);

    // 2.2 try to find consumer_meta, if not found, create it
    auto consumer_tid = cgroup_meta.consumers();
    StreamConsumerMetaValue consumer_meta;
    TRY_CATCH_ERROR(StreamUtils::GetOrCreateConsumer(consumer_tid, args_.consumer_name, slot.get(), consumer_meta),
                    res_);

    // 2.3 try to parse id
    if (unparsed_id == "$") {
      res_.SetRes(CmdRes::kSyntaxErr,
                  "The $ ID is meaningless in the context of "
                  "XREADGROUP: you want to read the history of "
                  "this consumer by specifying a proper ID, or "
                  "use the > ID to get new messages.The $ ID would "
                  "just return an empty result set.");
      return;
    } else if (unparsed_id == "<") {
      id = cgroup_meta.last_id();
    } else {
      LOG(INFO) << "given id: " << unparsed_id;
      if (!StreamUtils::StreamParseStrictID(unparsed_id, id, 0, nullptr)) {
        res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
        return;
      }
    }

    // 2.4 scan
    std::vector<storage::FieldValue> field_values;
    std::string next_field;
    StreamStorage::ScanStreamOptions options(key, id, kSTREAMID_MAX, args_.count);
    s = StreamStorage::ScanStream(options, field_values, next_field, slot.get());
    (void)next_field;
    if (!s.ok() && !s.IsNotFound()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
    StreamCmdBase::AppendMessagesToRes(res_, field_values, slot.get());

    // 2.5 add message to pel
    for (const auto &fv : field_values) {
      StreamPelMeta pel_meta;
      pel_meta.Init(args_.consumer_name, StreamUtils::GetCurrentTimeMs());
      StreamStorage::InsertTreeNodeValue(consumer_meta.pel_tid(), fv.field, pel_meta.value(), slot.get());
      StreamStorage::InsertTreeNodeValue(cgroup_meta.pel(), fv.field, pel_meta.value(), slot.get());
    }
  }
}

/* XGROUP CREATE <key> <groupname> <id or $> [MKSTREAM] [ENTRIESREAD entries_read]
 * XGROUP SETID <key> <groupname> <id or $> [ENTRIESREAD entries_read]
 * XGROUP DESTROY <key> <groupname>
 * XGROUP CREATECONSUMER <key> <groupname> <consumer>g
 * XGROUP DELCONSUMER <key> <groupname> <consumername> */
void XGroupCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }

  subcmd_ = argv_[1];
  key_ = argv_[2];
  cgroupname_ = argv_[3];

  if (argv_.size() >= 4) {
    // XGROUP CREATE ...[MKSTREAM] [ENTRIESREAD entries_read]
    // XGROUP SETID ...[ENTRIESREAD entries_read]
    int i = 5;
    bool create_subcmd = !strcasecmp(subcmd_.c_str(), "CREATE");
    bool setid_subcmd = !strcasecmp(subcmd_.c_str(), "SETID");
    while (i < argv_.size()) {
      // XGROUP CREATE ...[MKSTREAM] ...
      if (create_subcmd && !strcasecmp(argv_[i].c_str(), "MKSTREAM")) {
        mkstream_ = true;
        i++;
        // XGROUP <CREATE | SETID> ...[ENTRIESREAD entries_read]
      } else if ((create_subcmd || setid_subcmd) && !strcasecmp(argv_[i].c_str(), "ENTRIESREAD") &&
                 i + 1 < argv_.size()) {
        if (!StreamUtils::string2uint64(argv_[i + 1].c_str(), entries_read_)) {
          res_.SetRes(CmdRes::kInvalidParameter, "invalue parameter for ENTRIESREAD");
          return;
        }
      } else {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
    }
  }

  // Dispatch the different subcommands
  if (!strcasecmp(subcmd_.c_str(), "CREATE") && (argv_.size() >= 5 && argv_.size() <= 8)) {
    LOG(INFO) << "XGROUP CREATE";
    if (argv_[4] == "$") {
      id_given_ = false;
    } else {
      if (!StreamUtils::StreamParseStrictID(argv_[4], sid_, 0, &id_given_)) {
        res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
        return;
      }
    }
  } else if (!strcasecmp(subcmd_.c_str(), "SETID") && (argv_.size() == 5 || argv_.size() == 7)) {
    LOG(INFO) << "XGROUP SETID";
    if (argv_[4] == "$") {
      id_given_ = false;
    } else {
      if (!StreamUtils::StreamParseStrictID(argv_[4], sid_, 0, &id_given_)) {
        res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
        return;
      }
    }
  } else if (!strcasecmp(subcmd_.c_str(), "DESTROY") && argv_.size() == 4) {
    LOG(INFO) << "XGROUP DESTROY";
    // nothing todo
  } else if (!strcasecmp(subcmd_.c_str(), "CREATECONSUMEFR") && argv_.size() == 5) {
    consumername = argv_[4];
  } else if (!strcasecmp(subcmd_.c_str(), "DELCONSUMER") && argv_.size() == 5) {
    consumername = argv_[4];
  } else if (!strcasecmp(subcmd_.c_str(), "HELP")) {
    this->Help();
    return;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
}

void XGroupCmd::Do(std::shared_ptr<Slot> slot) {
  if (!strcasecmp(subcmd_.c_str(), "HELP")) {
    // this->Help(slot);
  }

  // 1 find stream meta and report error if needed
  std::string meta_value{};
  auto s = StreamStorage::GetStreamMeta(stream_meta_, key_, slot.get());
  if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kInvalidParameter,
                "The XGROUP subcommand requires the key to exist."
                "Note that for CREATE you may want to use the MKSTREAM "
                "option to create an empty stream automatically.");
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 2 dispatch to the different subcommands
  if (!strcasecmp(subcmd_.c_str(), "CREATE")) {
    this->Create(slot.get());
  } else if (!strcasecmp(subcmd_.c_str(), "SETID")) {
    // this->SetID(slot);
  } else if (!strcasecmp(subcmd_.c_str(), "DESTROY")) {
    this->Destroy(slot.get());
  } else if (!strcasecmp(subcmd_.c_str(), "CREATECONSUMEFR")) {
    this->CreateConsumer(slot.get());
  } else if (!strcasecmp(subcmd_.c_str(), "DELCONSUMER")) {
    // this->DeleteConsumer(slot);
  } else {
    LOG(ERROR) << "Unknown subcommand: " << subcmd_;
    res_.SetRes(CmdRes::kErrOther, "Unknown subcommand");
    return;
  }
}

void XGroupCmd::Create(const Slot *slot) {
  assert(slot);
  // 1 check if the group already exists
  treeID tid = stream_meta_.groups_id();
  StreamCGroupMetaValue cgroup_meta;
  if (tid == kINVALID_TREE_ID) {
    // this stream has no group tree, create one
    auto &tid_gen = TreeIDGenerator::GetInstance();
    TRY_CATCH_ERROR(tid_gen.GetNextTreeID(slot, tid), res_);
    stream_meta_.set_groups_id(tid);
  }
  auto &filed = cgroupname_;
  std::string cgroup_meta_value;

  // 2 if cgroup_meta exists, return error, otherwise create one
  auto s = StreamStorage::GetTreeNodeValue(tid, filed, cgroup_meta_value, slot);
  if (s.ok()) {
    LOG(INFO) << "CGroup meta found, faild to create";
    res_.SetRes(CmdRes::kErrOther, "-BUSYGROUP Consumer Group name already exists");
    return;
  } else if (s.IsNotFound()) {
    LOG(INFO) << "CGroup meta not found, create new one";
    treeID pel_id;
    treeID consumers_id;
    auto &tid_gen = TreeIDGenerator::GetInstance();
    TRY_CATCH_ERROR(tid_gen.GetNextTreeID(slot, pel_id), res_);
    TRY_CATCH_ERROR(tid_gen.GetNextTreeID(slot, consumers_id), res_);

    cgroup_meta.Init(pel_id, consumers_id);
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of tree id: " << tid;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 3 insert cgroup meta
  TRY_CATCH_ERROR(StreamStorage::InsertTreeNodeValue(tid, cgroupname_, cgroup_meta.value(), slot), res_);

  res_.SetRes(CmdRes::kOk);
  return;
}

void XGroupCmd::CreateConsumer(const Slot *slot) {
  assert(slot);
  // 1 try to get cgroup meta, if not found, return error
  treeID tid = stream_meta_.groups_id();
  StreamCGroupMetaValue cgroup_meta;
  if (tid == kINVALID_TREE_ID) {
    res_.SetRes(CmdRes::kInvalidParameter, "-NOGROUP No such consumer group" + cgroupname_ + "for key name" + key_);
    return;
  }

  std::string cgroup_meta_value;
  auto s = StreamStorage::GetTreeNodeValue(tid, cgroupname_, cgroup_meta_value, slot);
  if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kInvalidParameter, "-NOGROUP No such consumer group" + cgroupname_ + "for key name" + key_);
    return;
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of tree id: " << tid;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  cgroup_meta.ParseFrom(cgroup_meta_value);

  // 2 create and insert cgroup meta
  auto consumer_tid = cgroup_meta.consumers();
  std::string consumer_meta_value;
  s = StreamUtils::CreateConsumer(consumer_tid, consumername, slot);
  if (s.ok()) {
    res_.AppendInteger(1);
  } else {
    res_.AppendInteger(0);
  }
}

void XGroupCmd::Help(const Slot *slot) {
  assert(slot);
  res_.AppendString(
      std::string("CREATE <key> <groupname> <id|$> [option]\n"
                  "\tCreate a new consumer group. Options are:\n"
                  "\t* MKSTREAM\n"
                  "\t  Create the empty stream if it does not exist.\n"
                  "\t* ENTRIESREAD entries_read\n"
                  "\t  Set the group's entries_read counter (internal use).\n"
                  "CREATECONSUMER <key> <groupname> <consumer>\n"
                  "\tCreate a new consumer in the specified group.\n"
                  "DELCONSUMER <key> <groupname> <consumer>\n"
                  "\tRemove the specified consumer.\n"
                  "DESTROY <key> <groupname>\n"
                  "\tRemove the specified group.\n"
                  "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]\n"
                  "\tSet the current group ID and entries_read counter."));
}

void XGroupCmd::Destroy(const Slot *slot) {
  assert(slot);
  // 1 find the cgroup_tid
  StreamCmdBase::DestoryCGroupOrReply(res_, stream_meta_.groups_id(), cgroupname_, slot);
  if (res_.ret() == CmdRes::kNotFound) {
    res_.SetRes(CmdRes::kNone);
    res_.AppendInteger(0);
    return;
  } else if (res_.ret() != CmdRes::kNone) {
    LOG(ERROR) << "Failed to destory cgroup ";
    return;
  }
  res_.AppendInteger(1);
}

void XAckCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXAck);
    return;
  }

  key_ = argv_[1];
  cgroup_name_ = argv_[2];
  streamID tmp_id;
  for (size_t i = 3; i < argv_.size(); i++) {
    // check the id format
    if (!StreamUtils::StreamParseStrictID(argv_[i], tmp_id, 0, nullptr)) {
      res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
      return;
    }
    ids_.emplace_back(argv_[i]);
  }
}

void XAckCmd::Do(std::shared_ptr<Slot> slot) {
  // 1.try to get stream meta, if not found, return error
  StreamMetaValue stream_meta;
  auto s = StreamStorage::GetStreamMeta(stream_meta, key_, slot.get());
  if (s.IsNotFound()) {
    res_.AppendInteger(0);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 2.try to get cgroup meta, if not found, return error
  std::string cgroup_meta_value;
  s = StreamStorage::GetTreeNodeValue(stream_meta.groups_id(), cgroup_name_, cgroup_meta_value, slot.get());
  if (s.IsNotFound()) {
    res_.AppendInteger(0);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  StreamCGroupMetaValue cgroup_meta;
  cgroup_meta.ParseFrom(cgroup_meta_value);

  // 3.delete the id from the pending list
  std::unordered_map<std::string, StreamConsumerMetaValue> consumer_meta_map;
  for (auto &id : ids_) {
    // 3.1.get the pel meta
    std::string pel_meta_value;
    s = StreamStorage::GetTreeNodeValue(cgroup_meta.pel(), id, pel_meta_value, slot.get());
    if (!s.ok()) {
      LOG(INFO) << "id: " << id << " not found in cgroup: " << cgroup_name_ << "'s pel";
      continue;
    }
    StreamPelMeta pel_meta;
    pel_meta.ParseFrom(pel_meta_value);

    // 3.2 get the consumer meta of the message
    auto res = consumer_meta_map.find(pel_meta.consumer());
    StreamConsumerMetaValue cmeta;
    if (res == consumer_meta_map.end()) {
      // consumer is not in the map, get it's meta value and insert it into the map
      std::string consumer_meta_value;
      TRY_CATCH_ERROR(StreamStorage::GetTreeNodeValue(cgroup_meta.consumers(), pel_meta.consumer(), consumer_meta_value,
                                                      slot.get()),
                      res_);
      cmeta.ParseFrom(consumer_meta_value);
      consumer_meta_map.insert(std::make_pair(pel_meta.consumer(), cmeta));
    } else {
      cmeta = res->second;
    }

    // 3.2 delete the id from both consumer and cgroup's pel
    TRY_CATCH_ERROR(StreamStorage::DeleteTreeNode(cgroup_meta.pel(), id, slot.get()), res_);
    TRY_CATCH_ERROR(StreamStorage::DeleteTreeNode(cmeta.pel_tid(), id, slot.get()), res_);
  }
}

void XClaimCmd::DoInitial() {
  if (!CheckArg(argv_.size()) || argv_.size() < 6) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXClaim);
    return;
  }
  key_ = argv_[1];
  cgroup_name_ = argv_[2];
  consumer_name_ = argv_[3];
  if (!StreamUtils::string2uint64(argv_[4].c_str(), min_idle_time_)) {
    res_.SetRes(CmdRes::kInvalidParameter, "Invalid min-idle-time argument for XCLAIM");
    return;
  } else if (min_idle_time_ < 0) {
    min_idle_time_ = 0;
  }
  int i = 5;
  streamID id{0, 0};
  do {
    if (!StreamUtils::StreamParseStrictID(argv_[i++], id, 0, nullptr)) {
      res_.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
      return;
    }
    specified_ids_.emplace_back(id);
  } while (res_.ret() == CmdRes::kNone);
  parse_option_time_ = StreamUtils::GetCurrentTimeMs();
  // parse options
  for (; i < argv_.size(); ++i) {
    auto more_args = argv_.size() - i;
    const char *opt = argv_[i].c_str();
    if (!strcasecmp(opt, "FORCE")) {
      force = true;
    } else if (!strcasecmp(opt, "JUSTID")) {
      justid = true;
    } else if (!strcasecmp(opt, "IDLE") && more_args) {
      ++i;
      if (!StreamUtils::string2uint64(argv_[i].c_str(), deliverytime)) {
        res_.SetRes(CmdRes::kInvalidParameter, "Invalid IDLE option argument for XCLAIM");
        return;
      }
      deliverytime_flag = true;
      deliverytime = parse_option_time_ - deliverytime;
    } else if (!strcasecmp(opt, "TIME") && more_args) {
      ++i;
      if (!StreamUtils::string2uint64(argv_[i].c_str(), deliverytime)) {
        res_.SetRes(CmdRes::kInvalidParameter, "Invalid TIME option argument for XCLAIM");
        return;
      }
      deliverytime_flag = true;
    } else if (!strcasecmp(opt, "RETRYCOUNT") && more_args) {
      ++i;
      if (!StreamUtils::string2uint64(argv_[i].c_str(), retrycount)) {
        res_.SetRes(CmdRes::kInvalidParameter, "Invalid RETRYCOUNT option argument for XCLAIM");
        return;
      }
      retrycount_flag = true;
    } else if (!strcasecmp(opt, "LASTID") && more_args) {
      ++i;
      if (!StreamUtils::StreamParseStrictID(argv_[i], last_id, 0, nullptr)) {
        res_.SetRes(CmdRes::kInvalidParameter, "Invalid LASTID option argument for XCLAIM");
        return;
      }
    } else {
      res_.SetRes(CmdRes::kInvalidParameter, "Unrecognized XCLAIM option " + argv_[i]);
      return;
    }
  }

  if (deliverytime_flag) {
    if (deliverytime < 0 || deliverytime > parse_option_time_) deliverytime = parse_option_time_;
  } else {
    deliverytime = parse_option_time_;
  }
  res_.SetRes(CmdRes::kOk);
}

void XClaimCmd::Do(std::shared_ptr<Slot> slot) {
  // 1.try to get stream meta, if not found, return error
  StreamMetaValue stream_meta;
  auto s = StreamStorage::GetStreamMeta(stream_meta, key_, slot.get());
  if (s.IsNotFound()) {
    res_.AppendInteger(0);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 2.try to get cgroup meta, if not found, return error
  std::string cgroup_meta_value;
  StreamCGroupMetaValue cgroup_meta;
  s = StreamStorage::GetTreeNodeValue(stream_meta.groups_id(), cgroup_name_, cgroup_meta_value, slot.get());
  if (s.IsNotFound()) {
    res_.AppendInteger(0);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  cgroup_meta.ParseFrom(cgroup_meta_value);
  if (last_id > cgroup_meta.last_id()) {
    cgroup_meta.set_last_id(last_id);
    s = StreamStorage::InsertTreeNodeValue(stream_meta.groups_id(), cgroup_name_, cgroup_meta.value(), slot.get());
    if (!s.ok()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }
  auto consumer_tid = cgroup_meta.consumers();
  StreamConsumerMetaValue consumer_meta;
  StreamUtils::GetOrCreateConsumer(consumer_tid, consumer_name_, slot.get(), consumer_meta);
  if (res_.ret() != CmdRes::kNone) {
    return;
  }
  consumer_meta.set_seen_time(StreamUtils::GetCurrentTimeMs());
  for (auto &id : specified_ids_) {
    std::string serializedID;
    const std::string id_str = id.ToString();
    id.SerializeTo(serializedID);
    std::string pel_meta_value;
    auto pel_res = StreamStorage::GetTreeNodeValue(cgroup_meta.pel(), serializedID, pel_meta_value, slot.get());
    StreamPelMeta pel_meta;
    if (pel_res.ok()) {
      pel_meta.ParseFrom(pel_meta_value);
    } else if (!pel_res.IsNotFound()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
    // if item doesn't exist in stream, clear this entry from the PEL
    std::string msg_kvs;
    s = StreamStorage::GetStreamMessage(key_, serializedID, msg_kvs, slot.get());
    if (s.IsNotFound()) {
      if (pel_res.ok()) {
        // delete the id from both consumer and cgroup's pel
        TRY_CATCH_ERROR(StreamStorage::DeleteTreeNode(cgroup_meta.pel(), serializedID, slot.get()), res_);
        TRY_CATCH_ERROR(StreamStorage::DeleteTreeNode(consumer_meta.pel_tid(), serializedID, slot.get()), res_);
      }
      continue;
    } else if (!s.ok()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
    // if FORCE is passed, lets check if at least the entry exists in the stream.
    // In such case, we'll create a new entry in the PEL from scratch,
    // so that XCLAIM can also be used to create entries in the PEL.
    // Useful for AOF and replication of consumer groups
    if (force && pel_res.IsNotFound()) {
      pel_meta.Init(consumer_name_, StreamUtils::GetCurrentTimeMs());
      StreamStorage::InsertTreeNodeValue(cgroup_meta.pel(), serializedID, pel_meta.value(), slot.get());
    }

    if (pel_res.ok()) {
      std::string value;
      // get the consumer meta of the pel
      StreamConsumerMetaValue pel_consumer_meta;
      std::string pel_consumer_meta_value;
      TRY_CATCH_ERROR(StreamStorage::GetTreeNodeValue(cgroup_meta.consumers(), pel_meta.consumer(),
                                                      pel_consumer_meta_value, slot.get()),
                      res_);
      pel_consumer_meta.ParseFrom(pel_consumer_meta_value);

      TRY_CATCH_ERROR(StreamStorage::GetTreeNodeValue(pel_consumer_meta.pel_tid(), serializedID, value, slot.get()),
                      res_);

      // check if the minimum idle time satisfied
      if (min_idle_time_) {
        mstime_t this_idle = parse_option_time_ - pel_meta.delivery_time();
        if (this_idle < min_idle_time_) continue;
      }

      if (pel_meta.consumer() != consumer_name_) {
        // remove from old consumer pel
        s = StreamStorage::DeleteTreeNode(pel_consumer_meta.pel_tid(), serializedID, slot.get());
        if (!s.ok()) {
          res_.SetRes(CmdRes::kErrOther, s.ToString());
          return;
        }
        s = StreamStorage::InsertTreeNodeValue(consumer_meta.pel_tid(), serializedID, value, slot.get());
        if (!s.ok()) {
          res_.SetRes(CmdRes::kErrOther, s.ToString());
          return;
        }
        pel_meta.Init(consumer_name_, deliverytime);
      }
      if (retrycount_flag) {
        pel_meta.set_delivery_count(retrycount);
      } else if (!justid) {
        pel_meta.set_delivery_count(pel_meta.delivery_count() + 1);
      }
      s = StreamStorage::InsertTreeNodeValue(cgroup_meta.pel(), serializedID, pel_meta.value(), slot.get());
      if (!s.ok()) {
        res_.SetRes(CmdRes::kErrOther, s.ToString());
        return;
      }
      if (justid) {
        // append id
        results.emplace_back(id_str);
      } else {
        // append message
        std::vector<std::string> kOrvs;
        StreamUtils::DeserializeMessage(msg_kvs, kOrvs);
        for (const auto &kOrv : kOrvs) {
          results.emplace_back(kOrv);
        }
      }
      consumer_meta.set_active_time(StreamUtils::GetCurrentTimeMs());
      s = StreamStorage::InsertTreeNodeValue(cgroup_meta.consumers(), consumer_name_, consumer_meta.value(),
                                             slot.get());
      if (!s.ok()) {
        res_.SetRes(CmdRes::kErrOther, s.ToString());
        return;
      }
    }
  }
  res_.AppendArrayLen(static_cast<int64_t>(results.size()));
  for (auto &seg : results) {
    res_.AppendString(seg);
  }
  return;
}