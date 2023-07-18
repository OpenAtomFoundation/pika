#include "include/pika_stream.h"
#include <memory>
#include "include/pika_command.h"
#include "include/pika_data_distribution.h"
#include "include/pika_slot_command.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_util.h"
#include "pstd/include/pstd_string.h"

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
  StreamMetaValue stream_meta(meta_value);
  if (s.IsNotFound()) {
    stream_meta.Init();
    LOG(INFO) << "Stream meta not found";
  } else if (!s.ok()) {
    LOG(FATAL) << "Unexpected error of key: " << key_;
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  // 2. append the message
  assert(argv_.size() - field_pos_ >= 2 && (argv_.size() - field_pos_) % 2 == 0);
  std::string message_str;
  std::string id_str;
  if (!StreamUtil::SerializeMessage(argv_, message_str, field_pos_)) {
    LOG(FATAL) << "Serialize message failed";
    res_.SetRes(CmdRes::kErrOther, "Serialize message failed");
    return;
  }
  // 2.1 if id not given, generate one
  if (!args_.id_given) {
    auto last_id = stream_meta.last_id();
    args_.id.ms = StreamUtil::GetCurrentTimeMs();
    if (args_.id.ms < last_id.ms) {
      LOG(FATAL) << "Time backwards detected !";
      res_.SetRes(CmdRes::kErrOther, "Fatal! Time backwards detected !");
      return;
    } else if (args_.id.ms == last_id.ms) {
      // FIXME: deal with overflow, and user given seq
      assert(last_id.seq < UINT64_MAX);
      args_.id.seq = last_id.seq + 1;
    } else {
      args_.id.seq = 0;
    }
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

  // 3. update stream meta
  int message_len = (argv_.size() - field_pos_) / 2;
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
