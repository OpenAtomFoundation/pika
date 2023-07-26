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
#include "include/pika_stream_meta_value.h"
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

  static CmdRes ParseAddOrTrimArgs(const PikaCmdArgsType &argv, StreamAddTrimArgs &args, int &idpos, bool is_xadd);
  static CmdRes ParseReadOrReadGroupArgs(const PikaCmdArgsType &argv, StreamReadGroupReadArgs &args,
                                         bool is_xreadgroup);
  // be used when - and + are acceptable IDs.
  static CmdRes StreamParseID(const std::string &var, streamID &id, uint64_t missing_seq);
  // be used when we want to return an error if the special IDs + or - are provided.
  static CmdRes StreamParseStrictID(const std::string &var, streamID &id, uint64_t missing_seq, bool *seq_given);

  // Korpse TODO: unit tests
  // return false if the string is invalid
  static bool string2uint64(const char *s, uint64_t &value);
  static bool string2int64(const char *s, int64_t &value);
  static bool SerializeMessage(const std::vector<std::string> &field_values, std::string &serialized_message,
                               int field_pos);
  static bool SerializeStreamID(const streamID &id, std::string &serialized_id);
  static uint64_t GetCurrentTimeMs();

 private:
  // Korpse TODO: unit test
  static CmdRes StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
                                     bool *seq_given);

 private:
  // used when create the first stream meta
  static std::mutex create_stream_meta_hash_mutex_;

  // a flag to reduce the times of checking the existence of stream meta hash.
  static bool is_stream_meta_hash_created_;
};

#endif