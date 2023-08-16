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
#include "include/pika_stream_cgroup_meta_value.h"
#include "include/pika_stream_consumer_meta_value.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "rocksdb/metadata.h"
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
    treeID expected_id = kINVALID_TREE_ID;
    if (tree_id_.compare_exchange_strong(expected_id, START_TREE_ID)) {
      TryToFetchLastIdFromStorage(slot);
    }
    assert(tree_id_ != kINVALID_TREE_ID);
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
  static const treeID START_TREE_ID = 0;
  std::atomic<treeID> tree_id_ = kINVALID_TREE_ID;
};

class StreamUtil {
 public:
  //===--------------------------------------------------------------------===//
  // Meta data get and set
  //===--------------------------------------------------------------------===//

  // Korpse TODO: unit test
  static rocksdb::Status GetStreamMeta(const std::string &stream_key, std::string &meta_value,
                                       const std::shared_ptr<Slot> &slot);

  // will create stream meta hash if it dosent't exist.
  // return !s.ok() only when insert failed
  // Korpse TODO: unit test
  static rocksdb::Status UpdateStreamMeta(const std::string &key, std::string &meta_value,
                                          const std::shared_ptr<Slot> &slot);

  // delete the stream meta
  // @return true if the stream meta exists and deleted
  static void DeleteStreamMeta(const std::string &key, const std::shared_ptr<Slot> &slot);

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

  static rocksdb::Status DeleteTreeNode(const treeID tid, const std::string &field, const std::shared_ptr<Slot> &slot);

  static rocksdb::Status GetAllTreeNode(const treeID tid, std::vector<storage::FieldValue> &field_values,
                                        const std::shared_ptr<Slot> &slot);

  //===--------------------------------------------------------------------===//
  // Common functions for command implementation
  //===--------------------------------------------------------------------===//

  static CmdRes ParseAddOrTrimArgs(const PikaCmdArgsType &argv, StreamAddTrimArgs &args, int *idpos, bool is_xadd);
  static CmdRes ParseReadOrReadGroupArgs(const PikaCmdArgsType &argv, StreamReadGroupReadArgs &args,
                                         bool is_xreadgroup);

  struct TrimRet {
    // the count of deleted messages
    int32_t count{0};
    // the next field after trim
    std::string next_field;
    // the max deleted field, will be empty if no message is deleted
    std::string max_deleted_field;
  };

  static inline TrimRet TrimByMaxlen(StreamMetaValue &stream_meta, const std::string &key,
                                     const std::shared_ptr<Slot> &slot, CmdRes &res, const StreamAddTrimArgs &args);
  static inline TrimRet TrimByMinid(StreamMetaValue &stream_meta, const std::string &key,
                                    const std::shared_ptr<Slot> &slot, CmdRes &res, const StreamAddTrimArgs &args);

  static CmdRes TrimStream(const std::string &key, StreamAddTrimArgs &args, const std::shared_ptr<Slot> &slot);
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

  static inline CmdRes ScanStream(const std::string &skey, const streamID &start_sid, const streamID &end_sid,
                                  int32_t count, std::vector<storage::FieldValue> &field_values,
                                  std::string &next_field, const std::shared_ptr<Slot> &slot) {
    CmdRes res;
    std::string start_field;
    std::string end_field;
    rocksdb::Slice pattern = "*";  // match all the fields from start_field to end_field
    if (!StreamUtil::StreamID2String(start_sid, start_field)) {
      LOG(ERROR) << "Serialize stream id failed";
      res.SetRes(CmdRes::kErrOther, "Serialize stream id failed");
    }
    if (end_sid == kSTREAMID_MAX) {
      end_field = "";  // empty for no end_sid
    } else if (!StreamUtil::StreamID2String(end_sid, end_field)) {
      LOG(ERROR) << "Serialize stream id failed";
      res.SetRes(CmdRes::kErrOther, "Serialize stream id failed");
    }

    rocksdb::Status s =
        slot->db()->PKHScanRange(skey, start_field, end_field, pattern, count, &field_values, &next_field);
    if (s.IsNotFound()) {
      LOG(INFO) << "no message found in XRange";
      res.AppendArrayLen(0);
      return res;
    } else if (!s.ok()) {
      LOG(ERROR) << "PKHScanRange failed";
      res.SetRes(CmdRes::kErrOther, s.ToString());
      return res;
    }

    res.SetRes(CmdRes::kOk);
    return res;
  }

  // used to support range scan cmd, like xread, xrange, xrevrange
  // do the scan in a stream and append messages to res
  // @skey: the key of the stream
  // @row_ids: if not null, will append the id to it
  static CmdRes ScanAndAppendMessageToRes(const std::string &skey, const streamID &start_sid, const streamID &end_sid,
                                          int32_t count, const std::shared_ptr<Slot> &slot,
                                          std::vector<std::string> *row_ids);

  //  private:
  static CmdRes StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
                                     bool *seq_given);

  // note: filed_value here means the filed values in the message
  static bool DeserializeMessage(const std::string &message, std::vector<std::string> &parsed_message);

  // return true if created
  static bool CreateConsumer(treeID consumer_tid, std::string &consumername, const std::shared_ptr<Slot> &slot);

  // return ok if consumer meta exists or create a new one
  // @consumer_meta: used to return the consumer meta
  static rocksdb::Status GetOrCreateConsumer(treeID consumer_tid, std::string &consumername,
                                             const std::shared_ptr<Slot> &slot, StreamConsumerMetaValue &consumer_meta);

  // @return ok only when the cgroup meta exists and deleted
  static rocksdb::Status DestoryCGroup(treeID cgroup_tid, std::string &cgroupname, const std::shared_ptr<Slot> &slot);

  // note: the tree must exist
  // @return true if the tree exists and is deleted
  static bool DeleteTree(const treeID tid, const std::shared_ptr<Slot> &slot);

  // delete the pels, consumers, cgroups and stream meta of a stream
  // note: this function do not delete the stream data value
  static void DestoryStreams(std::vector<std::string> &keys, const std::shared_ptr<Slot> &slot) {
    for (const auto &key : keys) {
      // 1.try to get the stream meta
      std::string meta_value;
      auto s = StreamUtil::GetStreamMeta(key, meta_value, slot);
      if (s.IsNotFound()) {
        LOG(INFO) << "Stream meta not found, skip";
        continue;
      } else if (!s.ok()) {
        LOG(ERROR) << "Get stream meta failed";
        continue;
      }
      StreamMetaValue stream_meta;
      stream_meta.ParseFrom(meta_value);

      // 2 destroy all the cgroup
      // 2.1 find all the cgroups' meta
      auto cgroup_tid = stream_meta.groups_id();
      std::vector<storage::FieldValue> field_values;
      s = GetAllTreeNode(cgroup_tid, field_values, slot);
      if (!s.ok() && !s.IsNotFound()) {
        LOG(ERROR) << "Get all cgroups failed";
        continue;
      }
      // 2.2 loop all the cgroups, and destroy them
      for (auto &fv : field_values) {
        StreamUtil::DestoryCGroup(cgroup_tid, fv.field, slot);
      }
      // 2.3 delete the cgroup tree
      StreamUtil::DeleteTree(cgroup_tid, slot);

      // 3 delete stream meta
      StreamUtil::DeleteStreamMeta(key, slot);
    }
  }

 private:
  // used when create the first stream meta
  static std::mutex create_stream_meta_hash_mutex_;

  // a flag to reduce the times of checking the existence of stream meta hash.
  static bool is_stream_meta_hash_created_;
};

#endif