// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STREAM_UTIL_H_
#define SRC_STREAM_UTIL_H_

#include "include/pika_slot_command.h"
#include "include/pika_stream_meta_value.h"
#include "include/pika_stream_types.h"
#include "storage/storage.h"

// each abstracted tree has a prefix,
// prefix + treeID will be the key of the hash to store the tree,
// notice: we need to ban the use of this prefix when using "HSET".
static const std::string STERAM_TREE_PREFIX = "STR_TREE";

// key of the hash to store stream meta,
// notice: we need to ban the use of this key when using "HSET".
static const std::string STREAM_META_HASH_KEY = "STREAM";

// each stream's data (messages) are stored in a hash,
// to avoid stream key confilict with hash key, we add a prefix to each key for stream data,
// it's ok to use the same string as STREAM_META_HASH_KEY to be the prefix.
// notice: we need to ban the use of this prefix when using "HSET".
static const std::string STREAM_DATA_HASH_PREFIX = STREAM_META_HASH_KEY;

// I need to find a place to store the last generated tree id,
// so I stored it as a field-value pair in the hash storing stream meta.
// the field is a fixed string, and the value is the last generated tree id.
// notice: we need to ban the use of this key when using stream, to avoid conflict with stream's key.
static const std::string STREAM_LAST_GENERATED_TREE_ID_FIELD = "STREAM";

// the max number of each delete operation in XTRIM command，to avoid too much memory usage.
// eg. if a XTIRM command need to trim 10000 items, the implementation will use rocsDB's delete operation (10000 /
// kDEFAULT_TRIM_BATCH_SIZE) times
const static int32_t kDEFAULT_TRIM_BATCH_SIZE = 1000;

struct StreamAddTrimArgs {
  // XADD options
  streamID id;
  bool id_given{false};
  bool seq_given{false};
  bool no_mkstream{false};

  // XADD + XTRIM common options
  StreamTrimStrategy trim_strategy{0};
  int trim_strategy_arg_idx{0};

  // TRIM_STRATEGY_MAXLEN options
  uint64_t maxlen{0};
  streamID minid;
};

struct StreamReadGroupReadArgs {
  // XREAD + XREADGROUP common options
  std::vector<std::string> keys;
  std::vector<std::string> unparsed_ids;
  int32_t count{INT32_MAX};  // in redis this is uint64_t, but PKHScanRange only support int32_t
  uint64_t block{0};         // 0 means no block

  // XREADGROUP options
  std::string group_name;
  std::string consumer_name;
  bool noack_{false};
};

// get next tree id thread safe
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

  storage::Status GetNextTreeID(const Slot *slot, treeID &tid);

 private:
  static const treeID START_TREE_ID = 0;
  std::atomic<treeID> tree_id_ = kINVALID_TREE_ID;
};

// Implement all the functions that related to blackwidow derctly.
// if we want to change the storage engine, we need to rewrite this class.
class StreamStorage {
 public:
  struct ScanStreamOptions {
    std::string key;  // the key of the stream
    streamID start_sid;
    streamID end_sid;
    int32_t count;
    bool start_ex;    // exclude first message
    bool end_ex;      // exclude last message
    bool is_reverse;  // scan in reverse order
    ScanStreamOptions(std::string skey, streamID start_sid, streamID end_sid, int32_t count,
                      bool start_ex = false, bool end_ex = false, bool is_reverse = false)
        : key(skey),
          start_sid(start_sid),
          end_sid(end_sid),
          count(count),
          start_ex(start_ex),
          end_ex(end_ex),
          is_reverse(is_reverse) {}
  };

  static storage::Status ScanStream(const ScanStreamOptions &option, std::vector<storage::FieldValue> &field_values,
                                    std::string &next_field, const Slot *slot);

  // get and parse the stream meta if found
  // @return ok only when the stream meta exists
  static storage::Status GetStreamMeta(StreamMetaValue &tream_meta, const std::string &key, const Slot *slot);

  // will create stream meta hash if it dosent't exist.
  // return !s.ok() only when insert failed
  static storage::Status SetStreamMeta(const std::string &key, std::string &meta_value, const Slot *slot);

  static storage::Status InsertStreamMessage(const std::string &key, const streamID &id, const std::string &message,
                                             const Slot *slot);

  static storage::Status DeleteStreamMessage(const std::string &key, const std::vector<streamID> &ids, int32_t &ret,
                                             const Slot *slot);

  static storage::Status DeleteStreamMessage(const std::string &key, const std::vector<std::string> &serialized_ids,
                                             int32_t &ret, const Slot *slot);

  static storage::Status GetStreamMessage(const std::string &key, const std::string &sid, std::string &message,
                                          const Slot *slot);

  static storage::Status DeleteStreamData(const std::string &key, const Slot *slot);

  static storage::Status TrimStream(int32_t &res, StreamMetaValue &stream_meta, const std::string &key,
                                    StreamAddTrimArgs &args, const Slot *slot);

  // get the abstracted tree node, e.g. get a message in pel, get a consumer meta or get a cgroup meta.
  // the behavior of abstracted tree is similar to radix-tree in redis;
  // in cgroup tree, field is groupname
  // in consumer tree, field is consumername
  // in pel tree, field is messageID
  static storage::Status GetTreeNodeValue(const treeID tid, std::string &field, std::string &value, const Slot *slot);
  static storage::Status InsertTreeNodeValue(const treeID tid, const std::string &filed, const std::string &value,
                                             const Slot *slot);
  static storage::Status DeleteTreeNode(const treeID tid, const std::string &field, const Slot *slot);
  static storage::Status GetAllTreeNode(const treeID tid, std::vector<storage::FieldValue> &field_values,
                                        const Slot *slot);

  // delete the stream meta
  // @return true if the stream meta exists and deleted
  static storage::Status DeleteStreamMeta(const std::string &key, const Slot *slot);

  // note: the tree must exist
  // @return true if the tree exists and is deleted
  static storage::Status DeleteTree(const treeID tid, const Slot *slot);

  // get consumer meta value.
  // if the consumer meta value does not exist, create a new one and return it.
  static storage::Status GetOrCreateConsumer(treeID consumer_tid, std::string &consumername, const Slot *slot,
                                             StreamConsumerMetaValue &consumer_meta);

  static storage::Status CreateConsumer(treeID consumer_tid, std::string &consumername, const Slot *slot);

  // delete the pels, consumers, cgroups and stream meta of a stream
  // note: this function do not delete the stream data value
  static storage::Status DestoryStreams(std::vector<std::string> &keys, const Slot *slot);

 private:
  StreamStorage();
  ~StreamStorage();
  struct TrimRet {
    // the count of deleted messages
    int32_t count{0};
    // the next field after trim
    std::string next_field;
    // the max deleted field, will be empty if no message is deleted
    std::string max_deleted_field;
  };

  static storage::Status TrimByMaxlen(TrimRet &trim_ret, StreamMetaValue &stream_meta, const std::string &key,
                                      const Slot *slot, const StreamAddTrimArgs &args);

  static storage::Status TrimByMinid(TrimRet &trim_ret, StreamMetaValue &stream_meta, const std::string &key,
                                     const Slot *slot, const StreamAddTrimArgs &args);
};

// Helper function of stream command.
// Should be reconstructed when transfer to another command framework.
// any function that has Reply in its name will reply to the client if error occurs.
class StreamCmdBase {
 public:
 private:
  StreamCmdBase();
  ~StreamCmdBase();
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

  // serialize the message to a string.
  // format: {field1.size, field1, value1.size, value1, field2.size, field2, ...}
  static bool SerializeMessage(const std::vector<std::string> &field_values, std::string &serialized_message,
                               int field_pos);

  // deserialize the message from a string with the format of SerializeMessage.
  static bool DeserializeMessage(const std::string &message, std::vector<std::string> &parsed_message);

  // Parse a stream ID in the format given by clients to Pika, that is
  // <ms>-<seq>, and converts it into a streamID structure. The ID may be in incomplete
  // form, just stating the milliseconds time part of the stream. In such a case
  // the missing part is set according to the value of 'missing_seq' parameter.
  //
  // The IDs "-" and "+" specify respectively the minimum and maximum IDs
  // that can be represented. If 'strict' is set to 1, "-" and "+" will be
  // treated as an invalid ID.
  //
  // The ID form <ms>-* specifies a millisconds-only ID, leaving the sequence part
  // to be autogenerated. When a non-NULL 'seq_given' argument is provided, this
  // form is accepted and the argument is set to 0 unless the sequence part is
  // specified.
  static bool StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
                                   bool *seq_given);

  // Wrapper for streamGenericParseID() with 'strict' argument set to
  // 0, to be used when - and + are acceptable IDs.
  static bool StreamParseID(const std::string &var, streamID &id, uint64_t missing_seq);

  // Wrapper for streamGenericParseID() with 'strict' argument set to
  // 1, to be used when we want to return an error if the special IDs + or -
  // are provided.
  static bool StreamParseStrictID(const std::string &var, streamID &id, uint64_t missing_seq, bool *seq_given);

  // Helper for parsing a stream ID that is a range query interval. When the
  // exclude argument is NULL, streamParseID() is called and the interval
  // is treated as close (inclusive). Otherwise, the exclude argument is set if
  // the interval is open (the "(" prefix) and streamParseStrictID() is
  // called in that case.
  static bool StreamParseIntervalId(const std::string &var, streamID &id, bool *exclude, uint64_t missing_seq);
};

#endif
