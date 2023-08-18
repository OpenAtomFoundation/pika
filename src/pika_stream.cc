#include "include/pika_stream.h"
#include <strings.h>
#include <sys/types.h>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "include/pika_command.h"
#include "include/pika_data_distribution.h"
#include "include/pika_slot.h"
#include "include/pika_slot_command.h"
#include "include/pika_stream_cgroup_meta_value.h"
#include "include/pika_stream_consumer_meta_value.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_pel_meta_value.h"
#include "include/pika_stream_types.h"
#include "include/pika_stream_util.h"
#include "pstd/include/pstd_string.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

void XAddCmd::GenerateStreamIDOrRep(const StreamMetaValue &stream_meta) {
  auto &id = args_.id;
  if (args_.id_given && args_.seq_given && id.ms == 0 && id.seq == 0) {
    LOG(INFO) << "The ID specified in XADD must be greater than 0-0";
    res_.SetRes(CmdRes::kInvalidParameter);
    return;
  }

  if (!args_.id_given || !args_.seq_given) {
    // if id not given, generate one
    if (!args_.id_given) {
      LOG(INFO) << "ID not given, generate ms";
      id.ms = StreamUtil::GetCurrentTimeMs();
    }
    // generate seq
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms) {
      LOG(ERROR) << "Time backwards detected !";
      res_.SetRes(CmdRes::kErrOther, "Fatal! Time backwards detected !");
      return;
    } else if (id.ms == last_id.ms) {
      if (last_id.seq == UINT64_MAX) {
        res_.SetRes(CmdRes::kErrOther, "Fatal! Sequence number overflow !");
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
      LOG(ERROR) << "INVALID ID: " << id.ms << "-" << id.seq << " < " << last_id.ms << "-" << last_id.seq;
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
  StreamUtil::ParseAddOrTrimArgsOrRep(res_, argv_, args_, &idpos, true);
  if (res_.ret() != CmdRes::kNone) {
    return;
  } else if (idpos < 0) {
    LOG(ERROR) << "Invalid idpos: " << idpos;
    res_.SetRes(CmdRes::kErrOther);
    return;
  }

  field_pos_ = idpos + 1;
  if ((argv_.size() - field_pos_) % 2 == 1 || (argv_.size() - field_pos_) < 2) {
    LOG(INFO) << "Invalid field_values_ size: " << argv_.size() - field_pos_;
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXAdd);
    return;
  }
}

void XAddCmd::Do(std::shared_ptr<Slot> slot) {
  // 1 get stream meta
  std::string meta_value{};
  rocksdb::Status s;
  s = StreamUtil::GetStreamMeta(key_, meta_value, slot);
  StreamMetaValue stream_meta;
  if (s.IsNotFound() && args_.no_mkstream) {
    LOG(INFO) << "Stream not exists and no_mkstream";
    res_.SetRes(CmdRes::kNotFound);
    return;
  } else if (s.IsNotFound()) {
    LOG(INFO) << "Stream meta not found, create new one, key: " << key_;
    stream_meta.Init();
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of key: " << key_;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  } else {
    stream_meta.ParseFrom(meta_value);
  }

  // 2 append the message to storage
  std::string value;
  std::string field;
  GenerateStreamIDOrRep(stream_meta);
  if (res_.ret() != CmdRes::kNone) {
    return;
  }
  if (!StreamUtil::SerializeMessage(argv_, value, field_pos_)) {
    LOG(ERROR) << "Serialize message failed";
    res_.SetRes(CmdRes::kErrOther, "Serialize message failed");
    return;
  }
  args_.id.SerializeTo(field);

  // check the serialized current id is larger than last_id
#ifdef DEBUG
  std::string serialized_last_id;
  stream_meta.last_id().SerializeTo(serialized_last_id);
  assert(field > serialized_last_id);
#endif  // DEBUG

  s = StreamUtil::InsertStreamMessage(key_, field, value, slot);
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
    StreamUtil::TrimStreamOrRep(res_, stream_meta, key_, args_, slot);
    if (res_.ret() != CmdRes::kNone) {
      return;
    }
  }

  // 5 insert stream meta
  s = StreamUtil::UpdateStreamMeta(key_, stream_meta.value(), slot);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  res_.AppendString(args_.id.ToString());
}

void XLenCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXLen);
    return;
  }
  key_ = argv_[1];
}

void XLenCmd::Do(std::shared_ptr<Slot> slot) {
  std::string meta_value;
  rocksdb::Status s;
  s = StreamUtil::GetStreamMeta(key_, meta_value, slot);
  if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kNotFound);
    return;
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of key: " << key_;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  StreamMetaValue stream_meta;
  stream_meta.ParseFrom(meta_value);
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

  StreamUtil::ParseReadOrReadGroupArgsOrRep(res_, argv_, args_, false);
}

void XReadCmd::Do(std::shared_ptr<Slot> slot) {
  rocksdb::Status s;

  // 1 prepare stream_metas
  std::vector<std::pair<StreamMetaValue, int>> streammeta_idx;
  for (int i = 0; i < args_.unparsed_ids.size(); i++) {
    const auto &key = args_.keys[i];
    const auto &unparsed_id = args_.unparsed_ids[i];

    std::string meta_value{};
    StreamMetaValue stream_meta;
    auto s = StreamUtil::GetStreamMeta(key, meta_value, slot);
    if (s.IsNotFound()) {
      continue;
    } else if (!s.ok()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    } else {
      stream_meta.ParseFrom(meta_value);
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
      LOG(WARNING) << R"(given "<" in XREAD command)";
      res_.SetRes(CmdRes::kSyntaxErr,
                  "The > ID can be specified only when calling "
                  "XREADGROUP using the GROUP <group> "
                  "<consumer> option.");
      return;
    } else if (unparsed_id == "$") {
      LOG(INFO) << "given \"$\" in XREAD command";
      id = stream_meta.last_id();
    } else {
      LOG(INFO) << "given id: " << unparsed_id;
      StreamUtil::StreamParseStrictIDOrRep(res_, unparsed_id, id, 0, nullptr);
      if (res_.ret() != CmdRes::kNone) {
        return;
      }
    }

    // 2.2 scan
    res_.AppendArrayLen(2);
    res_.AppendString(key);
    StreamUtil::ScanAndAppendMessageToResOrRep(res_, key, id, kSTREAMID_MAX, args_.count, slot, nullptr);
    if (res_.ret() != CmdRes::kNone) {
      return;
    }
  }
}

void XReadGroupCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRead);
    return;
  }

  StreamUtil::ParseReadOrReadGroupArgsOrRep(res_, argv_, args_, false);
}

void XReadGroupCmd::Do(std::shared_ptr<Slot> slot) {
  assert(args_.keys.size() == args_.unparsed_ids.size());
  rocksdb::Status s;
  streamID id;
  for (int i = 0; i < args_.unparsed_ids.size(); i++) {
    const auto &key = args_.keys[i];
    const auto &unparsed_id = args_.unparsed_ids[i];

    // 1.try to find stream_meta
    std::string stream_meta_str{};
    StreamMetaValue stream_meta;
    s = StreamUtil::GetStreamMeta(key, stream_meta_str, slot);
    if (s.IsNotFound()) {
      LOG(INFO) << "Stream meta not found, skip";
      continue;
    } else if (!s.ok()) {
      LOG(ERROR) << "Unexpected error of key: " << key;
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    } else {
      stream_meta.ParseFrom(stream_meta_str);
    }

    // 2.try to find cgroup_meta
    treeID tid = stream_meta.groups_id();
    std::string cgroup_meta_str;
    StreamCGroupMetaValue cgroup_meta;
    s = StreamUtil::GetTreeNodeValue(tid, args_.group_name, cgroup_meta_str, slot);
    if (s.IsNotFound()) {
      LOG(WARNING) << "CGroup meta not found";
      res_.SetRes(CmdRes::kInvalidParameter, "-NOGROUP No such key " + key + " or consumer group " + args_.group_name +
                                                 " in XREADGROUP with GROUP option");
      return;
    } else if (!s.ok()) {
      LOG(ERROR) << "Unexpected error of tree id: " << tid;
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    } else {
      cgroup_meta.ParseFrom(cgroup_meta_str);
    }

    // 3.try to find consumer_meta, if not found, create it
    auto consumer_tid = cgroup_meta.consumers();
    StreamConsumerMetaValue consumer_meta;
    s = StreamUtil::GetOrCreateConsumer(consumer_tid, args_.consumer_name, slot, consumer_meta);
    if (!s.ok()) {
      LOG(ERROR) << "Unexpected error of tree id: " << consumer_tid;
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }

    // 4.read messages
    if (unparsed_id == "$") {
      LOG(WARNING) << R"(given "$" in XREADGROUP command)";
      res_.SetRes(CmdRes::kSyntaxErr,
                  "The $ ID is meaningless in the context of "
                  "XREADGROUP: you want to read the history of "
                  "this consumer by specifying a proper ID, or "
                  "use the > ID to get new messages.The $ ID would "
                  "just return an empty result set.");
      return;
    } else if (unparsed_id == "<") {
      LOG(INFO) << "given \"$\" in XREAD command";
      id = cgroup_meta.last_id();
    } else {
      LOG(INFO) << "given id: " << unparsed_id;
      StreamUtil::StreamParseStrictIDOrRep(res_, unparsed_id, id, 0, nullptr);
      if (res_.ret() != CmdRes::kNone) {
        return;
      }
    }

    res_.AppendArrayLen(2);
    res_.AppendString(key);

    // 5.add message to pending list
    std::vector<std::string> row_ids;
    StreamUtil::ScanAndAppendMessageToResOrRep(res_, key, id, kSTREAMID_MAX, args_.count, slot, &row_ids);
    if (res_.ret() != CmdRes::kNone) {
      return;
    }

    // add to both consumer and cgroup's pel
    for (const auto &id : row_ids) {
      StreamPelMeta pel_meta;
      pel_meta.Init(args_.consumer_name, StreamUtil::GetCurrentTimeMs());
      StreamUtil::InsertTreeNodeValue(consumer_meta.pel_tid(), id, pel_meta.value(), slot);
      StreamUtil::InsertTreeNodeValue(cgroup_meta.pel(), id, pel_meta.value(), slot);
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
        if (!StreamUtil::string2uint64(argv_[i + 1].c_str(), entries_read_)) {
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
      StreamUtil::StreamParseStrictIDOrRep(res_, argv_[4], sid_, 0, &id_given_);
      if (res_.ret() != CmdRes::kNone) {
        return;
      }
    }
  } else if (!strcasecmp(subcmd_.c_str(), "SETID") && (argv_.size() == 5 || argv_.size() == 7)) {
    LOG(INFO) << "XGROUP SETID";
    if (argv_[4] == "$") {
      id_given_ = false;
    } else {
      StreamUtil::StreamParseStrictIDOrRep(res_, argv_[4], sid_, 0, &id_given_);
      if (res_.ret() != CmdRes::kNone) {
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
  rocksdb::Status s;
  s = StreamUtil::GetStreamMeta(key_, meta_value, slot);
  if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kInvalidParameter,
                "The XGROUP subcommand requires the key to exist."
                "Note that for CREATE you may want to use the MKSTREAM "
                "option to create an empty stream automatically.");
    return;
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of key: " << key_;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  } else {
    LOG(INFO) << "Stream meta found, Parse";
    stream_meta_.ParseFrom(meta_value);
  }

  // 2 dispatch to the different subcommands
  if (!strcasecmp(subcmd_.c_str(), "CREATE")) {
    this->Create(slot);
  } else if (!strcasecmp(subcmd_.c_str(), "SETID")) {
    // this->SetID(slot);
  } else if (!strcasecmp(subcmd_.c_str(), "DESTROY")) {
    this->Destroy(slot);
  } else if (!strcasecmp(subcmd_.c_str(), "CREATECONSUMEFR")) {
    this->CreateConsumer(slot);
  } else if (!strcasecmp(subcmd_.c_str(), "DELCONSUMER")) {
    // this->DeleteConsumer(slot);
  } else {
    LOG(ERROR) << "Unknown subcommand: " << subcmd_;
    res_.SetRes(CmdRes::kErrOther, "Unknown subcommand");
    return;
  }
}

void XGroupCmd::Create(const std::shared_ptr<Slot> &slot) {
  // 1 check if the group already exists
  treeID tid = stream_meta_.groups_id();
  StreamCGroupMetaValue cgroup_meta;
  if (tid == kINVALID_TREE_ID) {
    // this stream has no group tree, create one
    auto &tid_gen = TreeIDGenerator::GetInstance();
    tid = tid_gen.GetNextTreeID(slot);
    stream_meta_.set_groups_id(tid);
  }
  auto &filed = cgroupname_;
  std::string cgroup_meta_value;

  // 2 if cgroup_meta exists, return error, otherwise create one
  auto s = StreamUtil::GetTreeNodeValue(tid, filed, cgroup_meta_value, slot);
  if (s.ok()) {
    LOG(INFO) << "CGroup meta found, faild to create";
    res_.SetRes(CmdRes::kErrOther, "-BUSYGROUP Consumer Group name already exists");
    return;
  } else if (s.IsNotFound()) {
    LOG(INFO) << "CGroup meta not found, create new one";
    auto &tid_gen = TreeIDGenerator::GetInstance();
    auto pel_tid = tid_gen.GetNextTreeID(slot);
    auto consumers_tid = tid_gen.GetNextTreeID(slot);
    cgroup_meta.Init(pel_tid, consumers_tid);
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of tree id: " << tid;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 3 insert cgroup meta
  s = StreamUtil::InsertTreeNodeValue(tid, cgroupname_, cgroup_meta.value(), slot);
  if (!s.ok()) {
    LOG(ERROR) << "Insert cgroup meta failed";
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  res_.SetRes(CmdRes::kOk);
  return;
}

void XGroupCmd::CreateConsumer(const std::shared_ptr<Slot> &slot) {
  // 1 try to get cgroup meta, if not found, return error
  treeID tid = stream_meta_.groups_id();
  StreamCGroupMetaValue cgroup_meta;
  if (tid == kINVALID_TREE_ID) {
    res_.SetRes(CmdRes::kInvalidParameter, "-NOGROUP No such consumer group" + cgroupname_ + "for key name" + key_);
    return;
  }

  std::string cgroup_meta_value;
  auto s = StreamUtil::GetTreeNodeValue(tid, cgroupname_, cgroup_meta_value, slot);
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
  if (StreamUtil::CreateConsumer(consumer_tid, consumername, slot)) {
    res_.AppendInteger(1);
  } else {
    res_.AppendInteger(0);
  }
}

void XGroupCmd::Help(const std::shared_ptr<Slot> &slot) {
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

void XGroupCmd::Destroy(const std::shared_ptr<Slot> &slot) {
  // 1 find the cgroup_tid
  auto s = StreamUtil::DestoryCGroup(stream_meta_.groups_id(), cgroupname_, slot);
  if (s.IsNotFound()) {
    res_.AppendInteger(0);
  } else if (!s.ok()) {
    LOG(ERROR) << "Destory cgroup failed";
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.AppendInteger(1);
}

void XRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRange);
    return;
  }
  key_ = argv_[1];
  StreamUtil::StreamParseIntervalIdOrRep(res_, argv_[2], start_sid, &start_ex_, 0);
  if (res_.ret() != CmdRes::kNone) {
    return;
  }
  StreamUtil::StreamParseIntervalIdOrRep(res_, argv_[3], end_sid, &end_ex_, UINT64_MAX);
  if (res_.ret() != CmdRes::kNone) {
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
    if (!StreamUtil::string2int32(argv_[5].c_str(), count_)) {
      res_.SetRes(CmdRes::kInvalidParameter, "COUNT should be a integer greater than 0 and not bigger than INT32_MAX");
      return;
    }
  }
}

void XRangeCmd::Do(std::shared_ptr<Slot> slot) {
  // FIXME: deal with start_ex_ and end_ex_
  StreamUtil::ScanAndAppendMessageToResOrRep(res_, key_, start_sid, end_sid, count_, slot, nullptr, start_ex_, end_ex_);
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
    StreamUtil::StreamParseStrictIDOrRep(res_, argv_[i], tmp_id, 0, nullptr);
    if (res_.ret() != CmdRes::kNone) {
      return;
    }
    ids_.emplace_back(argv_[i]);
  }
}

void XAckCmd::Do(std::shared_ptr<Slot> slot) {
  // 1.try to get stream meta, if not found, return error
  std::string stream_meta_value;
  auto s = StreamUtil::GetStreamMeta(key_, stream_meta_value, slot);
  if (s.IsNotFound()) {
    res_.AppendInteger(0);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  StreamMetaValue stream_meta;
  stream_meta.ParseFrom(stream_meta_value);

  // 2.try to get cgroup meta, if not found, return error
  std::string cgroup_meta_value;
  s = StreamUtil::GetTreeNodeValue(stream_meta.groups_id(), cgroup_name_, cgroup_meta_value, slot);
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
    s = StreamUtil::GetTreeNodeValue(cgroup_meta.pel(), id, pel_meta_value, slot);
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
      s = StreamUtil::GetTreeNodeValue(cgroup_meta.consumers(), pel_meta.consumer(), consumer_meta_value, slot);
      if (!s.ok()) {
        LOG(ERROR) << "consumer: " << pel_meta.consumer() << " not found in cgroup: " << cgroup_name_ << "'s consumers";
        res_.SetRes(CmdRes::kErrOther, s.ToString());
        return;
      }
      cmeta.ParseFrom(consumer_meta_value);
      consumer_meta_map.insert(std::make_pair(pel_meta.consumer(), cmeta));
    } else {
      cmeta = res->second;
    }

    // 3.2 delete the id from both consumer and cgroup's pel
    s = StreamUtil::DeleteTreeNode(cgroup_meta.pel(), id, slot);
    if (!s.ok()) {
      LOG(ERROR) << "delete id: " << id << " from cgroup: " << cgroup_name_ << "'s pel failed";
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
    s = StreamUtil::DeleteTreeNode(cmeta.pel_tid(), id, slot);
    if (!s.ok()) {
      LOG(ERROR) << "delete id: " << id << " from consumer: " << pel_meta.consumer() << "'s pel failed";
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }
}

void XTrimCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXTrim);
    return;
  }

  key_ = argv_[1];
  StreamUtil::ParseAddOrTrimArgsOrRep(res_, argv_, args_, nullptr, true);
  if (res_.ret() != CmdRes::kNone) {
    return;
  }
}

void XTrimCmd::Do(std::shared_ptr<Slot> slot) {
  // 1 try to get stream meta, if not found, return error
  std::string stream_meta_value;
  auto s = StreamUtil::GetStreamMeta(key_, stream_meta_value, slot);
  if (s.IsNotFound()) {
    res_.AppendInteger(0);
    return;
  } else if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  StreamMetaValue stream_meta;
  stream_meta.ParseFrom(stream_meta_value);

  // 2 do the trim
  auto count = StreamUtil::TrimStreamOrRep(res_, stream_meta, key_, args_, slot);

  // 3 update stream meta
  s = StreamUtil::UpdateStreamMeta(key_, stream_meta.value(), slot);
  if (!s.ok()) {
    LOG(ERROR) << "Insert stream message failed";
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  res_.AppendInteger(count);
  return;
}
