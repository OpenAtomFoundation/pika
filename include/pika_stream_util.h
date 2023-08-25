#ifndef SRC_STREAM_UTIL_H_
#define SRC_STREAM_UTIL_H_

#include <atomic>
#include <cassert>
#include <cstddef>
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
#include "storage/src/coding.h"
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
  treeID GetNextTreeID(const std::shared_ptr<Slot> &slot);

 private:
  void TryToFetchLastIdFromStorage(const std::shared_ptr<Slot> &slot);

 private:
  static const treeID START_TREE_ID = 0;
  std::atomic<treeID> tree_id_ = kINVALID_TREE_ID;
};

// implement all the functions that related to blackwidow derctly.
// if we want to change the storage engine, we only need to rewrite this class.
class StreamStorage {
 public:
  StreamStorage() = default;
  ~StreamStorage() = default;

  // get and parse the stream meta if found
  // @return ok only when the stream meta exists
  static rocksdb::Status GetStreamMeta(StreamMetaValue &tream_meta, const std::string &key,
                                       const std::shared_ptr<Slot> &slot);

  // will create stream meta hash if it dosent't exist.
  // return !s.ok() only when insert failed
  static rocksdb::Status UpdateStreamMeta(const std::string &key, std::string &meta_value,
                                          const std::shared_ptr<Slot> &slot);

  static rocksdb::Status InsertStreamMessage(const std::string &key, const streamID &id, const std::string &message,
                                             const std::shared_ptr<Slot> &slot);
  static rocksdb::Status DeleteStreamMessage(const std::string &key, const std::vector<streamID> &id, int32_t &ret,
                                             const std::shared_ptr<Slot> &slot);
  static rocksdb::Status GetStreamMessage(const std::string &key, const std::string &sid, std::string &message,
                                          const std::shared_ptr<Slot> &slot);

  // get the abstracted tree node, e.g. get a message in pel, get a consumer meta or get a cgroup meta.
  // in cgroup tree, field is groupname
  // in consumer tree, field is consumername
  // in pel tree, field is messageID
  static rocksdb::Status GetTreeNodeValue(const treeID tid, std::string &field, std::string &value,
                                          const std::shared_ptr<Slot> &slot);
  static rocksdb::Status InsertTreeNodeValue(const treeID tid, const std::string &filed, const std::string &value,
                                             const std::shared_ptr<Slot> &slot);
  static rocksdb::Status DeleteTreeNode(const treeID tid, const std::string &field, const std::shared_ptr<Slot> &slot);
  static rocksdb::Status GetAllTreeNode(const treeID tid, std::vector<storage::FieldValue> &field_values,
                                        const std::shared_ptr<Slot> &slot);

  // delete the stream meta
  // @return true if the stream meta exists and deleted
  static void DeleteStreamMeta(const std::string &key, const std::shared_ptr<Slot> &slot);

  // note: the tree must exist
  // @return true if the tree exists and is deleted
  static bool DeleteTree(const treeID tid, const std::shared_ptr<Slot> &slot);
};

// Helper function of stream command.
// Should be reconstructed when transfer to another command framework.
class StreamCmdBase {
 public:
  StreamCmdBase() = default;
  ~StreamCmdBase() = default;

  // @res: if error occurs, res will be set, otherwise res will be none
  static void ParseAddOrTrimArgsOrRep(CmdRes &res, const PikaCmdArgsType &argv, StreamAddTrimArgs &args, int *idpos,
                                      bool is_xadd);
  static void ParseReadOrReadGroupArgsOrRep(CmdRes &res, const PikaCmdArgsType &argv, StreamReadGroupReadArgs &args,
                                            bool is_xreadgroup);
  // @return ok only when the cgroup meta exists and deleted
  static void DestoryCGroupOrRep(CmdRes &res, treeID cgroup_tid, std::string &cgroupname,
                                 const std::shared_ptr<Slot> &slot);
  struct TrimRet {
    // the count of deleted messages
    int32_t count{0};
    // the next field after trim
    std::string next_field;
    // the max deleted field, will be empty if no message is deleted
    std::string max_deleted_field;
  };

  // @res: if error occurs, res will be set, otherwise res will be none
  static TrimRet TrimByMaxlenOrRep(StreamMetaValue &stream_meta, const std::string &key,
                                   const std::shared_ptr<Slot> &slot, CmdRes &res, const StreamAddTrimArgs &args);

  // @res: if error occurs, res will be set, otherwise res will be none
  static TrimRet TrimByMinidOrRep(StreamMetaValue &stream_meta, const std::string &key,
                                  const std::shared_ptr<Slot> &slot, CmdRes &res, const StreamAddTrimArgs &args);

  // @res: if error occurs, res will be set, otherwise res will be none
  static int32_t TrimStreamOrRep(CmdRes &res, StreamMetaValue &stream_meta, const std::string &key,
                                 StreamAddTrimArgs &args, const std::shared_ptr<Slot> &slot);

  struct ScanStreamOptions {
    const std::string &skey;
    const streamID &start_sid;
    const streamID &end_sid;
    const int32_t count;
    ScanStreamOptions(const std::string &skey, const streamID &start_sid, const streamID &end_sid, const int32_t count)
        : skey(skey), start_sid(start_sid), end_sid(end_sid), count(count) {}
  };

  static void ScanStreamOrRep(CmdRes &res, const ScanStreamOptions &option,
                              std::vector<storage::FieldValue> &field_values, std::string &next_field,
                              const std::shared_ptr<Slot> &slot);

  // used to support range scan cmd, like xread, xrange, xrevrange
  // do the scan in a stream and append messages to res
  // @skey: the key of the stream
  // @row_ids: if not null, will append the id to it
  static void ScanAndAppendMessageToResOrRep(CmdRes &res, const ScanStreamOptions &option,
                                             const std::shared_ptr<Slot> &slot, std::vector<std::string> *row_ids,
                                             bool start_ex = false, bool end_ex = false);

  //===--------------------------------------------------------------------===//
  // StreamID operator
  //===--------------------------------------------------------------------===//

  static void StreamGenericParseIDOrRep(CmdRes &res, const std::string &var, streamID &id, uint64_t missing_seq,
                                        bool strict, bool *seq_given);
  // be used when - and + are acceptable IDs.
  static void StreamParseIDOrRep(CmdRes &res, const std::string &var, streamID &id, uint64_t missing_seq);

  // be used when we want to return an error if the special IDs + or - are provided.
  static void StreamParseStrictIDOrRep(CmdRes &res, const std::string &var, streamID &id, uint64_t missing_seq,
                                       bool *seq_given);

  // Helper for parsing a stream ID that is a range query interval. When the
  // exclude argument is NULL, StreamParseID() is called and the interval
  // is treated as close (inclusive). Otherwise, the exclude argument is set if
  // the interval is open (the "(" prefix) and StreamParseStrictID() is
  // called in that case.
  static void StreamParseIntervalIdOrRep(CmdRes &res, const std::string &var, streamID &id, bool *exclude,
                                         uint64_t missing_seq);

  // delete the pels, consumers, cgroups and stream meta of a stream
  // note: this function do not delete the stream data value
  static void DestoryStreamsOrRep(CmdRes &res, std::vector<std::string> &keys, const std::shared_ptr<Slot> &slot);

  static void GetOrCreateConsumer(CmdRes &res, treeID consumer_tid, std::string &consumername,
                                  const std::shared_ptr<Slot> &slot, StreamConsumerMetaValue &consumer_meta);
};

class StreamUtils {
 public:
  StreamUtils() = default;
  ~StreamUtils() = default;

  static bool string2uint64(const char *s, uint64_t &value);
  static bool string2int64(const char *s, int64_t &value);
  static bool string2int32(const char *s, int32_t &value);
  static std::string TreeID2Key(const treeID &tid);

  static uint64_t GetCurrentTimeMs();

  static int StreamIDCompare(const streamID &a, const streamID &b);

  // serialize the message to a string, format: {field1.size, field1, value1.size, value1, field2.size, field2, ...}
  static bool SerializeMessage(const std::vector<std::string> &field_values, std::string &serialized_message,
                               int field_pos);

  // note: filed_value here means the filed values in the message
  static bool DeserializeMessage(const std::string &message, std::vector<std::string> &parsed_message);

  // return true if created
  static bool CreateConsumer(treeID consumer_tid, std::string &consumername, const std::shared_ptr<Slot> &slot);
};

#endif