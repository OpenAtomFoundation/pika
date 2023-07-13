#ifndef SRC_STREAM_UTIL_H_
#define SRC_STREAM_UTIL_H_

#include <atomic>
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
  static std::unique_ptr<ParsedStreamMetaFiledValue> GetStreamMeta(const std::string &stream_name,
                                                                   const std::shared_ptr<Slot> &slot);
  static CmdRes StreamGenericParseIDOrReply(const std::string &var, streamID *id, uint64_t missing_seq, bool strict,
                                            int *seq_given);
  // be used when - and + are acceptable IDs.
  static CmdRes StreamParseIDOrReply(const std::string &var, streamID *id, uint64_t missing_seq) {
    return StreamGenericParseIDOrReply(var, id, missing_seq, false, nullptr);
  }
  // be used when we want to return an error if the special IDs + or - are provided.
  static CmdRes StreamParseStrictIDOrReply(const std::string &var, streamID *id, uint64_t missing_seq, int *seq_given) {
    return StreamGenericParseIDOrReply(var, id, missing_seq, true, seq_given);
  }

  

  // basic util
  static bool string2uint64(const char *s, uint64_t &value);
};

#endif