#ifndef SRC_STREAM_UTIL_H_
#define SRC_STREAM_UTIL_H_

#include <atomic>
#include <mutex>
#include <vector>
#include "glog/logging.h"
#include "include/pika_conf.h"
#include "include/pika_slot_command.h"
#include "include/pika_stream_meta_value.h"
#include "rocksdb/status.h"
#include "storage/storage.h"

// get next tree id thread safly
class TreeIDGenerator {
 public:
  TreeIDGenerator() = default;
  ~TreeIDGenerator() = default;

  // work in singeletone mode
  TreeIDGenerator &GetInstance() {
    static TreeIDGenerator instance;
    return instance;
  }

  treeID GetNextTreeID(const std::shared_ptr<Slot> &slot) {
    treeID expected_id = INVALID_TREE_ID;
    if (tree_id_.compare_exchange_strong(expected_id, START_TREE_ID)) {
      TryToFetchLastIdFromStorage(slot);
    }
    return ++tree_id_;
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
      tree_id_ = std::stoi(value);
      tree_id_ = START_TREE_ID;
      std::string tree_id_str = std::to_string(tree_id_);
      s = slot->db()->Set(STREAM_TREE_STRING_KEY, tree_id_str);
      assert(s.ok());
    }
  }

 private:
  static const treeID INVALID_TREE_ID = -1;
  static const treeID START_TREE_ID = 0;
  std::atomic<treeID> tree_id_ = INVALID_TREE_ID;
};

class StreamUtil {
 public:
  static CmdRes ParseAddOrTrimArgs(const PikaCmdArgsType &argv, StreamAddTrimArgs &args, int &idpos, bool is_xadd);
  static CmdRes GetStreamMeta(const std::string &stream_key, std::string &meta_value,
                              const std::shared_ptr<Slot> &slot);
  // be used when - and + are acceptable IDs.
  static CmdRes StreamParseID(const std::string &var, streamID &id, uint64_t missing_seq);
  // be used when we want to return an error if the special IDs + or - are provided.
  static CmdRes StreamParseStrictID(const std::string &var, streamID &id, uint64_t missing_seq, int *seq_given);

  static bool string2uint64(const char *s, uint64_t &value);

 private:
  // create the hash for stream meta atotomically, used only when create the first stream meta.
  // return OK if the hash has been successfully created or already exists.
  // return false when HSet failed.
  static rocksdb::Status CheckAndCreateStreamMetaHash(const std::string &key, std::string &value,
                                                      const std::shared_ptr<Slot> &slot);
  static CmdRes StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
                                     int *seq_given);

 private:
  // used when create the first stream meta
  static std::mutex create_stream_meta_hash_mutex_;

  // a flag to reduce the times of checking the existence of stream meta hash.
  static bool is_stream_meta_hash_created_;
};

#endif