#include "include/pika_stream.h"
#include <strings.h>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "include/pika_command.h"
#include "include/pika_data_distribution.h"
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

// Korpse:FIXME: check the argv_ size
void XAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXAdd);
    return;
  }
  key_ = argv_[1];
  int idpos{-1};
  res_ = StreamUtil::ParseAddOrTrimArgs(argv_, args_, idpos, true);
  if (!res_.ok()) {
    return;
  } else if (idpos < 0) {
    LOG(ERROR) << "Invalid idpos: " << idpos;
    res_.SetRes(CmdRes::kErrOther);
  }

  field_pos_ = idpos + 1;
  if ((argv_.size() - idpos) % 2 == 1 || (argv_.size() - field_pos_) < 2) {
    LOG(INFO) << "Invalid field_values_ size: " << argv_.size() - field_pos_;
    res_.SetRes(CmdRes::kInvalidParameter);
    return;
  }
  res_.SetRes(CmdRes::kOk);
}

void XAddCmd::Do(std::shared_ptr<Slot> slot) {
  // 1. get stream meta
  std::string meta_value{};
  rocksdb::Status s;
  s = StreamUtil::GetStreamMeta(key_, meta_value, slot);
  StreamMetaValue stream_meta;
  if (s.ok()) {
    LOG(INFO) << "Stream meta found, Parse";
    stream_meta.ParseFrom(meta_value);
  } else if (s.IsNotFound() && args_.no_mkstream) {
    LOG(INFO) << "Stream not exists and not create";
    res_.SetRes(CmdRes::kInvalidParameter, "Stream not exists");
    return;
  } else if (s.IsNotFound()) {
    LOG(INFO) << "Stream meta not found, create new one";
    stream_meta.Init();
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of key: " << key_;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 2. append the message to storage
  std::string message_str;
  std::string id_str;
  res_ = GenerateStreamID(stream_meta, args_, args_.id);
  if (!res_.ok()) {
    return;
  }
  if (!StreamUtil::SerializeMessage(argv_, message_str, field_pos_)) {
    LOG(ERROR) << "Serialize message failed";
    res_.SetRes(CmdRes::kErrOther, "Serialize message failed");
    return;
  }
  if (!StreamUtil::StreamID2String(args_.id, id_str)) {
    LOG(ERROR) << "Serialize stream id failed";
    res_.SetRes(CmdRes::kErrOther, "Serialize stream id failed");
    return;
  }
  s = StreamUtil::InsertStreamMessage(key_, id_str, message_str, slot);
  if (!s.ok()) {
    LOG(ERROR) << "Insert stream message failed";
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 3. update stream meta, Korpse TODO: is there any arg left to update?
  auto message_len = (argv_.size() - field_pos_) / 2;
  if (stream_meta.entries_added() == 0) {
    stream_meta.set_first_id(args_.id);
  }
  stream_meta.set_entries_added(stream_meta.entries_added() + message_len);
  stream_meta.set_last_id(args_.id);

  // 4. Korpse TODO: trim the stream if needed

  // n. insert stream meta
  s = StreamUtil::InsertStreamMeta(key_, stream_meta.value(), slot);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.SetRes(CmdRes::kOk);
}

CmdRes XAddCmd::GenerateStreamID(const StreamMetaValue &stream_meta, const StreamAddTrimArgs &args_, streamID &id) {
  CmdRes res;
  if (args_.id_given && args_.seq_given && id.ms == 0 && id.seq == 0) {
    LOG(INFO) << "The ID specified in XADD must be greater than 0-0";
    res.SetRes(CmdRes::kInvalidParameter);
    return res;
  }
  // 2.2 if id not given, generate one
  if (!args_.id_given) {
    LOG(INFO) << "ID not given, generate id";
    auto last_id = stream_meta.last_id();
    id.ms = StreamUtil::GetCurrentTimeMs();
    if (id.ms < last_id.ms) {
      LOG(ERROR) << "Time backwards detected !";
      res.SetRes(CmdRes::kErrOther, "Fatal! Time backwards detected !");
      return res;
    } else if (id.ms == last_id.ms) {
      assert(last_id.seq < UINT64_MAX);
      id.seq = last_id.seq + 1;
    } else {
      id.seq = 0;
    }
  } else if (!args_.seq_given) {
    LOG(INFO) << "ID not given, generate id";
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms) {
      LOG(ERROR) << "Time backwards detected !";
      res.SetRes(CmdRes::kErrOther, "Fatal! Time backwards detected !");
      return res;
    }
    assert(last_id.seq < UINT64_MAX);
    id.seq = last_id.seq + 1;
  } else {
    LOG(INFO) << "ID given, check id";
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms || (id.ms == last_id.ms && id.seq <= last_id.seq)) {
      LOG(ERROR) << "INVALID ID: " << id.ms << "-" << id.seq << " < " << last_id.ms << "-" << last_id.seq;
      res.SetRes(CmdRes::kErrOther, "INVALID ID given");
      return res;
    }
  }
  res.SetRes(CmdRes::kOk);
  return res;
}

void XReadCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRead);
    return;
  }

  res_ = StreamUtil::ParseReadOrReadGroupArgs(argv_, args_, false);
}

void XReadCmd::Do(std::shared_ptr<Slot> slot) {
  assert(args_.unedited_keys.size() == args_.unparsed_ids.size());
  rocksdb::Status s;
  streamID id;
  for (int i = 0; i < args_.unparsed_ids.size(); i++) {
    const auto &key = args_.keys[i];
    const auto &unparsed_id = args_.unparsed_ids[i];
    // try to find stream_meta
    std::string meta_value{};
    StreamMetaValue stream_meta;
    s = StreamUtil::GetStreamMeta(key, meta_value, slot);
    if (s.IsNotFound()) {
      LOG(INFO) << "Stream meta not found, skip";
      continue;
    } else if (!s.ok()) {
      LOG(ERROR) << "Unexpected error of key: " << key;
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    } else {
      stream_meta.ParseFrom(meta_value);
    }

    // read messages
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
      res_ = StreamUtil::StreamParseStrictID(unparsed_id, id, 0, nullptr);
      if (!res_.ok()) {
        return;
      }
    }

    res_.AppendArrayLen(2);
    res_.AppendString(key);
    StreamUtil::ScanAndAppendMessageToRes(key, id, STREAMID_MAX, args_.count, res_, slot, nullptr);
  }
}

void XReadGroupCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRead);
    return;
  }

  res_ = StreamUtil::ParseReadOrReadGroupArgs(argv_, args_, false);
}

void XReadGroupCmd::Do(std::shared_ptr<Slot> slot) {
  assert(args_.unedited_keys.size() == args_.unparsed_ids.size());
  rocksdb::Status s;
  streamID id;
  for (int i = 0; i < args_.unparsed_ids.size(); i++) {
    const auto &key = args_.keys[i];
    const auto &unparsed_id = args_.unparsed_ids[i];

    // 1. try to find stream_meta
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

    // 2. try to find cgroup_meta
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

    // 3. try to find consumer_meta, if not found, create it
    auto consumer_tid = cgroup_meta.consumers();
    StreamConsumerMetaValue consumer_meta;
    s = StreamUtil::GetOrCreateConsumerMeta(consumer_tid, args_.consumer_name, slot, consumer_meta);
    if (!s.ok()) {
      LOG(ERROR) << "Unexpected error of tree id: " << consumer_tid;
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }

    // 4. read messages
    if (unparsed_id == "$") {
      LOG(WARNING) << R"(given "$" in XREADGROUP command)";
      res_.SetRes(CmdRes::kSyntaxErr,
                  "The $ ID is meaningless in the context of "
                  "XREADGROUP: you want to read the history of "
                  "this consumer by specifying a proper ID, or "
                  "use the > ID to get new messages. The $ ID would "
                  "just return an empty result set.");
      return;
    } else if (unparsed_id == "<") {
      LOG(INFO) << "given \"$\" in XREAD command";
      id = cgroup_meta.last_id();
    } else {
      LOG(INFO) << "given id: " << unparsed_id;
      res_ = StreamUtil::StreamParseStrictID(unparsed_id, id, 0, nullptr);
      if (!res_.ok()) {
        return;
      }
    }

    res_.AppendArrayLen(2);
    res_.AppendString(key);
    
    // 5. add message to pending list
    std::vector<std::string> row_ids;
    StreamUtil::ScanAndAppendMessageToRes(key, id, STREAMID_MAX, args_.count, res_, slot, &row_ids);

    // Korpse TODO: need to add the message to the pending list
    for (const auto &id : row_ids) {
      StreamPelMeta pel_meta(args_.consumer_name);
      pel_meta.Init(StreamUtil::GetCurrentTimeMs());
      StreamUtil::InsertTreeNodeValue(consumer_meta.pel(), id, pel_meta.value(), slot);
    }
  }
}

/* XGROUP CREATE <key> <groupname> <id or $> [MKSTREAM] [ENTRIESREAD entries_read]
 * XGROUP SETID <key> <groupname> <id or $> [ENTRIESREAD entries_read]
 * XGROUP DESTROY <key> <groupname>
 * XGROUP CREATECONSUMER <key> <groupname> <consumer>g
 * XGROUP DELCONSUMER <key> <groupname> <consumername> */
void XGROUP::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }

  assert(argv_.size() >= 4);  // Korpse TODO: Is this necessary?
  opt_ = argv_[1];
  key_ = argv_[2];
  group_name_ = argv_[3];

  if (argv_.size() >= 4) {
    // XGROUP CREATE ... [MKSTREAM] [ENTRIESREAD entries_read]
    // XGROUP SETID ... [ENTRIESREAD entries_read]
    int i = 5;
    bool create_subcmd = !strcasecmp(opt_.c_str(), "CREATE");
    bool setid_subcmd = !strcasecmp(opt_.c_str(), "SETID");
    while (i < argv_.size()) {
      // XGROUP CREATE ... [MKSTREAM] ...
      if (create_subcmd && !strcasecmp(argv_[i].c_str(), "MKSTREAM")) {
        mkstream_ = true;
        i++;
        // XGROUP <CREATE | SETID> ... [ENTRIESREAD entries_read]
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
  if (!strcasecmp(opt_.c_str(), "CREATE") && (argv_.size() >= 5 && argv_.size() <= 8)) {
    LOG(INFO) << "XGROUP CREATE";
    if (argv_[4] == "$") {
      id_given_ = false;
    } else {
      res_ = StreamUtil::StreamParseStrictID(argv_[4], sid_, 0, &id_given_);
      if (!res_.ok()) {
        return;
      }
    }
  } else if (!strcasecmp(opt_.c_str(), "SETID") && (argv_.size() == 5 || argv_.size() == 7)) {
    LOG(INFO) << "XGROUP SETID";
    if (argv_[4] == "$") {
      id_given_ = false;
    } else {
      res_ = StreamUtil::StreamParseStrictID(argv_[4], sid_, 0, &id_given_);
      if (!res_.ok()) {
        return;
      }
    }
  } else if (!strcasecmp(opt_.c_str(), "DESTROY") && argv_.size() == 4) {
    LOG(INFO) << "XGROUP DESTROY";
    // nothing todo
  } else if (!strcasecmp(opt_.c_str(), "CREATECONSUMEFR") && argv_.size() == 5) {
    consumername = argv_[4];
  } else if (!strcasecmp(opt_.c_str(), "DELCONSUMER") && argv_.size() == 5) {
    consumername = argv_[4];
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  res_.SetRes(CmdRes::kOk);
}

void XGROUP::Create(const std::shared_ptr<Slot> &slot) {
  assert(slot);
  // 1. try to get stream meta
  std::string meta_value{};
  rocksdb::Status s;
  s = StreamUtil::GetStreamMeta(key_, meta_value, slot);
  StreamMetaValue stream_meta;
  if (s.IsNotFound() && !mkstream_) {
    res_.SetRes(CmdRes::kInvalidParameter,
                "The XGROUP subcommand requires the key to exist. "
                "Note that for CREATE you may want to use the MKSTREAM "
                "option to create an empty stream automatically.");
    return;
  } else if (s.IsNotFound()) {
    stream_meta.Init();
    LOG(INFO) << "Stream meta not found, create new one";
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of key: " << key_;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  } else {
    LOG(INFO) << "Stream meta found, Parse";
    stream_meta.ParseFrom(meta_value);
  }

  // 2. check if the group already exists
  treeID tid = stream_meta.groups_id();
  StreamCGroupMetaValue cgroup_meta;
  if (tid == kINVALID_TREE_ID) {
    // this stream has no group tree, create one
    auto &tid_gen = TreeIDGenerator::GetInstance();
    tid = tid_gen.GetNextTreeID(slot);
    stream_meta.set_groups_id(tid);
  }
  auto &filed = group_name_;
  std::string cgroup_meta_value;

  // 3. if cgroup_meta exists, return error, otherwise create one
  s = StreamUtil::GetTreeNodeValue(tid, filed, cgroup_meta_value, slot);
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

  // 4. insert cgroup meta
  s = StreamUtil::InsertTreeNodeValue(tid, group_name_, cgroup_meta.value(), slot);
  if (!s.ok()) {
    LOG(ERROR) << "Insert cgroup meta failed";
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  res_.SetRes(CmdRes::kOk);
  return;
}

void XGROUP::CreateConsumer(const std::shared_ptr<Slot> &slot) {
  // 1. try to get stream meta, if not found, return error
  std::string meta_value{};
  rocksdb::Status s;
  s = StreamUtil::GetStreamMeta(key_, meta_value, slot);
  StreamMetaValue stream_meta;
  if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kInvalidParameter,
                "The XGROUP subcommand requires the key to exist. "
                "Note that for CREATE you may want to use the MKSTREAM "
                "option to create an empty stream automatically.");
    return;
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of key: " << key_;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  } else {
    LOG(INFO) << "Stream meta found, Parse";
    stream_meta.ParseFrom(meta_value);
  }

  // 2. try to get cgroup meta, if not found, return error
  treeID tid = stream_meta.groups_id();
  StreamCGroupMetaValue cgroup_meta;
  if (tid == kINVALID_TREE_ID) {
    res_.SetRes(CmdRes::kInvalidParameter, "-NOGROUP No such consumer group" + group_name_ + "for key name" + key_);
    return;
  }

  std::string cgroup_meta_value;
  s = StreamUtil::GetTreeNodeValue(tid, group_name_, cgroup_meta_value, slot);
  if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kInvalidParameter, "-NOGROUP No such consumer group" + group_name_ + "for key name" + key_);
    return;
  } else if (!s.ok()) {
    LOG(ERROR) << "Unexpected error of tree id: " << tid;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  cgroup_meta.ParseFrom(cgroup_meta_value);

  // 3. create and insert cgroup meta
  auto consumer_tid = cgroup_meta.consumers();
  std::string consumer_meta_value;
  if (StreamUtil::CreateConsumer(consumer_tid, consumername, slot)) {
    res_.AppendInteger(1);
  } else {
    res_.AppendInteger(0);
  }
}

void XRangeCmd::DoInitial() {
  if (!CheckArg(argv_.size()) || argv_.size() < 4) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameXRange);
    return;
  }
  key_ = argv_[1];
  bool start_ex = false;
  bool end_ex = false;
  res_ = StreamUtil::StreamParseIntervalId(argv_[2], start_sid, &start_ex, 0);
  if (!res_.ok()) {
    return;
  }
  res_ = StreamUtil::StreamParseIntervalId(argv_[3], end_sid, &end_ex, UINT64_MAX);
  if (!res_.ok()) {
    return;
  }
  if (start_ex && start_sid.ms == UINT64_MAX && start_sid.seq == UINT64_MAX) {
    res_.SetRes(CmdRes::kInvalidParameter, "invalid start id");
    return;
  }
  if (end_ex && end_sid.ms == 0 && end_sid.seq == 0) {
    res_.SetRes(CmdRes::kInvalidParameter, "invalid end id");
    return;
  }
  if (argv_.size() == 5) {
    // pika's PKHScanRange() only sopport max count of INT32_MAX
    // but redis supports max count of UINT64_MAX
    if (!StreamUtil::string2int32(argv_[4].c_str(), count_)) {
      res_.SetRes(CmdRes::kInvalidParameter, "COUNT should be a integer greater than 0 and not bigger than INT32_MAX");
      return;
    }
  }
  res_.SetRes(CmdRes::kOk);
}

void XRangeCmd::Do(std::shared_ptr<Slot> slot) {
  // FIXME: deal with start_ex and end_ex
  StreamUtil::ScanAndAppendMessageToRes(key_, start_sid, end_sid, count_, res_, slot, nullptr);
  return;
}