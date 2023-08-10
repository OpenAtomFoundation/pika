#ifndef SRC_STREAM_UTIL_H_
#define SRC_STREAM_UTIL_H_

#include <atomic>
#include <cassert>
#include <cstdint>
#include <mutex>
#include <vector>
#include "glog/logging.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"
#include "include/pika_slot_command.h"
#include "include/pika_stream_consumer_meta_value.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "rocksdb/status.h"
#include "storage/storage.h"

// get next tree id thread safly
class TreeIDGenerator {
 private:
  TreeIDGenerator() = default;
  void operator=(const TreeIDGenerator &) = delete;

 public:
  ~TreeIDGenerator() = default;

  // work in singeletone mode
  static TreeIDGenerator &GetInstance() {
    static TreeIDGenerator instance;
    return instance;
  }

  // FIXME: return rocksdb::Status instead of treeID
  treeID GetNextTreeID(const std::shared_ptr<Slot> &slot) {
    treeID expected_id = INVALID_TREE_ID;
    if (tree_id_.compare_exchange_strong(expected_id, START_TREE_ID)) {
      TryToFetchLastIdFromStorage(slot);
    }
    assert(tree_id_ != INVALID_TREE_ID);
    ++tree_id_;
    std::string tree_id_str = std::to_string(tree_id_);
    rocksdb::Status s = slot->db()->Set(STREAM_TREE_STRING_KEY, tree_id_str);
    LOG(INFO) << "Set tree id to " << tree_id_str << " tree id: " << tree_id_;
    return tree_id_;
  }

 private:
  void TryToFetchLastIdFromStorage(const std::shared_ptr<Slot> &slot) {
    std::string value;
    rocksdb::Status s = slot->db()->Get(STREAM_TREE_STRING_KEY, &value);
    if (s.ok()) {
      // found, set tree id
      auto id = std::stoi(value);
      if (id < START_TREE_ID) {
        LOG(FATAL) << "Invalid tree id: " << id << ", set to start tree id: " << START_TREE_ID;
      }

    } else {
      // not found, set start tree id and insert to db
      tree_id_ = START_TREE_ID;
      LOG(INFO) << "Tree id not found, set to start tree id: " << START_TREE_ID;
    }
  }

 private:
  static const treeID INVALID_TREE_ID = -1;
  static const treeID START_TREE_ID = 0;
  std::atomic<treeID> tree_id_ = INVALID_TREE_ID;
};

class StreamUtil {
 public:
  //===--------------------------------------------------------------------===//
  // Meta data get and insert
  //===--------------------------------------------------------------------===//

  // Korpse TODO: unit test
  static rocksdb::Status GetStreamMeta(const std::string &stream_key, std::string &meta_value,
                                       const std::shared_ptr<Slot> &slot);

  // will create stream meta hash if it dosent't exist.
  // return !s.ok() only when insert failed
  // Korpse TODO: unit test
  static rocksdb::Status InsertStreamMeta(const std::string &key, std::string &meta_value,
                                          const std::shared_ptr<Slot> &slot);

  static rocksdb::Status InsertStreamMessage(const std::string &key, const std::string &sid, const std::string &message,
                                             const std::shared_ptr<Slot> &slot);

  // get the abstracted tree node, e.g. get a message in pel, get a consumer meta or get a cgroup meta.
  // in cgroup tree, field is groupname
  // in consumer tree, field is consumername
  // in pel tree, field is messageID
  static rocksdb::Status GetTreeNodeValue(const treeID tid, std::string &field, std::string &value,
                                          const std::shared_ptr<Slot> &slot);

  // set the abstracted tree node, e.g. set a message in pel, add a consumer meta or add a cgroup meta.
  // in cgroup tree, field is groupname, value is cgroup meta
  // in consumer tree, field is consumername, value is consumer meta
  // in pel tree, field is messageID, value is pel meta
  static rocksdb::Status InsertTreeNodeValue(const treeID tid, const std::string &filed, const std::string &value,
                                             const std::shared_ptr<Slot> &slot);

  //===--------------------------------------------------------------------===//
  // Parse instraction args
  //===--------------------------------------------------------------------===//

  static CmdRes ParseAddOrTrimArgs(const PikaCmdArgsType &argv, StreamAddTrimArgs &args, int &idpos, bool is_xadd);
  static CmdRes ParseReadOrReadGroupArgs(const PikaCmdArgsType &argv, StreamReadGroupReadArgs &args,
                                         bool is_xreadgroup);

  //===--------------------------------------------------------------------===//
  // Serialize and deserialize
  //===--------------------------------------------------------------------===//

  static bool StreamID2String(const streamID &id, std::string &serialized_id);

  // be used when - and + are acceptable IDs.
  static CmdRes StreamParseID(const std::string &var, streamID &id, uint64_t missing_seq);

  // be used when we want to return an error if the special IDs + or - are provided.
  static CmdRes StreamParseStrictID(const std::string &var, streamID &id, uint64_t missing_seq, bool *seq_given);

  // Helper for parsing a stream ID that is a range query interval. When the
  // exclude argument is NULL, StreamParseID() is called and the interval
  // is treated as close (inclusive). Otherwise, the exclude argument is set if
  // the interval is open (the "(" prefix) and StreamParseStrictID() is
  // called in that case.
  static CmdRes StreamParseIntervalId(const std::string &var, streamID &id, bool *exclude, uint64_t missing_seq);

  // serialize the message to a string, format: {field1.size, field1, value1.size, value1, field2.size, field2, ...}
  static bool SerializeMessage(const std::vector<std::string> &field_values, std::string &serialized_message,
                               int field_pos);

  //===--------------------------------------------------------------------===//
  // Type convert
  //===--------------------------------------------------------------------===//

  // return false if the string is invalid
  static bool string2uint64(const char *s, uint64_t &value);
  static bool string2int64(const char *s, int64_t &value);
  static bool string2int32(const char *s, int32_t &value);

  //===--------------------------------------------------------------------===//
  // Other helper functions
  //===--------------------------------------------------------------------===//

  static uint64_t GetCurrentTimeMs();

  static void GenerateKeyByTreeID(std::string &field, const treeID tid);

  // used to support range scan cmd, like xread, xrange, xrevrange
  // do the scan in a stream and append messages to res
  // @skey: the key of the stream
  static void ScanAndAppendMessageToRes(const std::string &skey, const streamID &start_sid, const streamID &end_sid,
                                        int32_t count, CmdRes &res, const std::shared_ptr<Slot> &slot, std::vector<std::string> *row_ids) {
    std::string start_field;
    std::string end_field;
    rocksdb::Slice pattern = "*";
    std::string next_field;
    std::vector<storage::FieldValue> field_values;
    if (!StreamUtil::StreamID2String(start_sid, start_field) || !StreamUtil::StreamID2String(end_sid, end_field)) {
      LOG(ERROR) << "Serialize stream id failed";
      res.SetRes(CmdRes::kErrOther, "Serialize stream id failed");
    }
    rocksdb::Status s =
        slot->db()->PKHScanRange(skey, start_field, end_field, pattern, count, &field_values, &next_field);
    if (s.IsNotFound()) {
      LOG(INFO) << "XRange not found";
      res.AppendArrayLen(0);
      return;
    } else if (!s.ok()) {
      LOG(ERROR) << "PKHScanRange failed";
      res.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }

    // append the result to res_
    // the outer layer is an array, each element is a inner array witch has 2 elements
    // the inner array's first element is the field, the second element is an array of messages
    LOG(INFO) << "XRange Found " << field_values.size() << " messages";
    res.AppendArrayLenUint64(field_values.size());
    for (auto &fv : field_values) {

      // if ids is not null, we need to record the id of each message
      if (row_ids) {
        row_ids->push_back(fv.field);
      }

      std::vector<std::string> message;
      if (!DeserializeMessage(fv.value, message)) {
        LOG(ERROR) << "Deserialize message failed";
        res.SetRes(CmdRes::kErrOther, "Deserialize message failed");
        return;
      }

      assert(message.size() % 2 == 0);
      res.AppendArrayLen(2);
      res.AppendString(fv.field);  // field here is the stream id
      res.AppendArrayLenUint64(message.size());
      for (auto &m : message) {
        res.AppendString(m);
      }
    }
  }

  //  private:
  static CmdRes StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
                                     bool *seq_given);

  // note: filed_value here means the filed values in the message
  static bool DeserializeMessage(const std::string &message, std::vector<std::string> &parsed_message);

  static bool CreateConsumer(treeID consumer_tid, std::string &consumername, const std::shared_ptr<Slot> &slot) {
    std::string consumer_meta_value;
    auto s = StreamUtil::GetTreeNodeValue(consumer_tid, consumername, consumer_meta_value, slot);
    if (s.IsNotFound()) {
      LOG(INFO) << "Consumer meta not found, create new one";
      auto &tid_gen = TreeIDGenerator::GetInstance();
      auto pel_tid = tid_gen.GetNextTreeID(slot);
      StreamConsumerMetaValue consumer_meta;
      consumer_meta.Init(pel_tid);
      s = StreamUtil::InsertTreeNodeValue(consumer_tid, consumername, consumer_meta.value(), slot);
      if (!s.ok()) {
        LOG(ERROR) << "Insert consumer meta failed";
        return false;
      }
      return true;
    }
    // consumer meta already exists or other error
    return false;
  }

  // return ok if consumer meta exists or create a new one
  // @consumer_meta: used to return the consumer meta
  static rocksdb::Status GetOrCreateConsumerMeta(treeID consumer_tid, std::string &consumername, const std::shared_ptr<Slot> &slot,
                                      StreamConsumerMetaValue &consumer_meta) {
    std::string consumer_meta_value;
    auto s = StreamUtil::GetTreeNodeValue(consumer_tid, consumername, consumer_meta_value, slot);
    if (s.ok()) {
      consumer_meta.ParseFrom(consumer_meta_value);
    } else if (s.IsNotFound()) {
      LOG(INFO) << "Consumer meta not found, create new one";
      auto &tid_gen = TreeIDGenerator::GetInstance();
      auto pel_tid = tid_gen.GetNextTreeID(slot);
      consumer_meta.Init(pel_tid);
      s = StreamUtil::InsertTreeNodeValue(consumer_tid, consumername, consumer_meta.value(), slot);
      if (!s.ok()) {
        LOG(ERROR) << "Insert consumer meta failed";
        return s;
      }
    }
    // consumer meta already exists or other error
    return s;
  }

 private:
  // used when create the first stream meta
  static std::mutex create_stream_meta_hash_mutex_;

  // a flag to reduce the times of checking the existence of stream meta hash.
  static bool is_stream_meta_hash_created_;
};

#endif