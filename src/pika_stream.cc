#include "include/pika_stream.h"
#include <strings.h>
#include <cassert>
#include <memory>
#include "include/pika_command.h"
#include "include/pika_data_distribution.h"
#include "include/pika_slot_command.h"
#include "include/pika_stream_cgroup_meta_value.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_consumer_meta_value.h"
#include "include/pika_stream_types.h"
#include "include/pika_stream_util.h"
#include "pstd/include/pstd_string.h"

// Korpse:FIXME: check the argv_ size
void XAddCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }
  key_ = argv_[1];
  int idpos{-1};
  res_ = StreamUtil::ParseAddOrTrimArgs(argv_, args_, idpos, true);
  if (!res_.ok()) {
    return;
  } else if (idpos < 0) {
    LOG(FATAL) << "Invalid idpos: " << idpos;
    res_.SetRes(CmdRes::kErrOther);
  }

  field_pos_ = idpos + 1;
  if ((argv_.size() - idpos) % 2 == 1 || (argv_.size() - field_pos_) < 2) {
    LOG(INFO) << "Invalid field_values_ size: " << argv_.size() - field_pos_;
    res_.SetRes(CmdRes::kInvalidParameter);
    return;
  }
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
    LOG(FATAL) << "Unexpected error of key: " << key_;
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
    LOG(FATAL) << "Serialize message failed";
    res_.SetRes(CmdRes::kErrOther, "Serialize message failed");
    return;
  }
  if (!StreamUtil::SerializeStreamID(args_.id, id_str)) {
    LOG(FATAL) << "Serialize stream id failed";
    res_.SetRes(CmdRes::kErrOther, "Serialize stream id failed");
    return;
  }
  s = StreamUtil::InsertStreamMessage(key_, id_str, message_str, slot);
  if (!s.ok()) {
    LOG(FATAL) << "Insert stream message failed";
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 3. update stream meta, Korpse TODO: is there any arg left to update?
  int message_len = (argv_.size() - field_pos_) / 2;
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
      LOG(FATAL) << "Time backwards detected !";
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
      LOG(FATAL) << "Time backwards detected !";
      res.SetRes(CmdRes::kErrOther, "Fatal! Time backwards detected !");
      return res;
    }
    assert(last_id.seq < UINT64_MAX);
    id.seq = last_id.seq + 1;
  } else {
    LOG(INFO) << "ID given, check id";
    auto last_id = stream_meta.last_id();
    if (id.ms < last_id.ms || (id.ms == last_id.ms && id.seq <= last_id.seq)) {
      LOG(FATAL) << "INVALID ID: " << id.ms << "-" << id.seq << " < " << last_id.ms << "-" << last_id.seq;
      res.SetRes(CmdRes::kErrOther, "INVALID ID given");
      return res;
    }
  }
  res.SetRes(CmdRes::kOk);
  return res;
}

void XReadCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameLIndex);
    return;
  }

  // Korpse TODO: finish this
}

/* XGROUP CREATE <key> <groupname> <id or $> [MKSTREAM] [ENTRIESREAD entries_read]
 * XGROUP SETID <key> <groupname> <id or $> [ENTRIESREAD entries_read]
 * XGROUP DESTROY <key> <groupname>
 * XGROUP CREATECONSUMER <key> <groupname> <consumer>
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
    consumer_name_ = argv_[4];
  } else if (!strcasecmp(opt_.c_str(), "DELCONSUMER") && argv_.size() == 5) {
    consumer_name_ = argv_[4];
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
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
    LOG(FATAL) << "Unexpected error of key: " << key_;
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

  std::string key;
  auto &filed = group_name_;
  std::string cgroup_meta_value;
  StreamUtil::GenerateKeyByTreeID(key, tid);

  // 3. if cgroup_meta exists, return error, otherwise create one
  s = StreamUtil::GetTreeNodeValue(key, filed, cgroup_meta_value, slot);
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
    LOG(FATAL) << "Unexpected error of key: " << key;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 4. insert cgroup meta
  s = StreamUtil::InsertTreeNodeValue(key, group_name_, cgroup_meta.value(), slot);
  if (!s.ok()) {
    LOG(FATAL) << "Insert cgroup meta failed";
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
    LOG(FATAL) << "Unexpected error of key: " << key_;
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
  std::string cgroup_key;
  std::string cgroup_meta_value;
  StreamUtil::GenerateKeyByTreeID(cgroup_key, tid);
  s = StreamUtil::GetTreeNodeValue(cgroup_key, group_name_, cgroup_meta_value, slot);
  if (s.IsNotFound()) {
    res_.SetRes(CmdRes::kInvalidParameter, "-NOGROUP No such consumer group" + group_name_ + "for key name" + key_);
    return;
  } else if (!s.ok()) {
    LOG(FATAL) << "Unexpected error of key: " << cgroup_key;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 3. create and insert cgroup meta
  auto consumer_tid = cgroup_meta.pel();
  std::string consumer_key;
  std::string consumer_meta_value;
  StreamUtil::GenerateKeyByTreeID(consumer_key, consumer_tid);
  s = StreamUtil::GetTreeNodeValue(consumer_key, consumer_name_, consumer_meta_value, slot);
  if (s.ok()) {
    LOG(INFO) << "Consumer meta found, faild to create";
    res_.AppendInteger(0);
  } else if (s.IsNotFound()) {
    LOG(INFO) << "Consumer meta not found, create new one";
    auto &tid_gen = TreeIDGenerator::GetInstance();
    auto pel_tid = tid_gen.GetNextTreeID(slot);
    StreamConsumerMetaValue consumer_meta;
    consumer_meta.Init(pel_tid);
    s = StreamUtil::InsertTreeNodeValue(consumer_key, consumer_name_, consumer_meta.value(), slot);
    if (!s.ok()) {
      LOG(FATAL) << "Insert consumer meta failed";
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
    res_.AppendInteger(1);
  }
}
