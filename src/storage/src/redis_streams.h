#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include "include/storage/storage.h"
#include "pika_stream_base.h"
#include "pika_stream_meta_value.h"
#include "pika_stream_types.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "src/redis.h"
#include "storage/storage.h"

namespace storage {

class RedisStreams : public Redis {
 public:
  RedisStreams(Storage* const s, const DataType& type) : Redis(s, type) {}
  ~RedisStreams() override = default;

  //===--------------------------------------------------------------------===//
  // Commands
  //===--------------------------------------------------------------------===//
  Status XAdd(const Slice& key, const std::string& serialized_message, StreamAddTrimArgs& args);
  Status XDel(const Slice& key, const std::vector<streamID>& ids, size_t& count);
  Status XTrim(const Slice& key, StreamAddTrimArgs& args, size_t& count);
  Status XRange(const Slice& key, const StreamScanArgs& args, std::vector<IdMessage>& id_messages);
  Status XRevrange(const Slice& key, const StreamScanArgs& args, std::vector<IdMessage>& id_messages);
  Status XLen(const Slice& key, size_t& len);
  Status XRead(const StreamReadGroupReadArgs& args, std::vector<std::vector<storage::IdMessage>>& results,
               std::vector<std::string>& reserved_keys);

  //===--------------------------------------------------------------------===//
  // Common Commands
  //===--------------------------------------------------------------------===//
  Status Open(const StorageOptions& storage_options, const std::string& db_path) override;
  Status CompactRange(const rocksdb::Slice* begin, const rocksdb::Slice* end,
                      const ColumnFamilyType& type = kMetaAndData) override;
  Status GetProperty(const std::string& property, uint64_t* out) override;
  Status ScanKeyNum(KeyInfo* keyinfo) override;
  Status ScanKeys(const std::string& pattern, std::vector<std::string>* keys) override;
  Status PKPatternMatchDel(const std::string& pattern, int32_t* ret) override;

  //===--------------------------------------------------------------------===//
  // Keys Commands
  //===--------------------------------------------------------------------===//
  Status Del(const Slice& key) override;
  bool Scan(const std::string& start_key, const std::string& pattern, std::vector<std::string>* keys, int64_t* count,
            std::string* next_key) override;

  //===--------------------------------------------------------------------===//
  // Not needed for streams
  //===--------------------------------------------------------------------===//
  Status Expire(const Slice& key, int32_t ttl) override;
  bool PKExpireScan(const std::string& start_key, int32_t min_timestamp, int32_t max_timestamp,
                    std::vector<std::string>* keys, int64_t* leftover_visits, std::string* next_key) override;
  Status Expireat(const Slice& key, int32_t timestamp) override;
  Status Persist(const Slice& key) override;
  Status TTL(const Slice& key, int64_t* timestamp) override;

  //===--------------------------------------------------------------------===//
  // Storage API
  //===--------------------------------------------------------------------===//
  struct ScanStreamOptions {
    const rocksdb::Slice key;  // the key of the stream
    int32_t version;           // the version of the stream
    streamID start_sid;
    streamID end_sid;
    size_t limit;
    bool start_ex;    // exclude first message
    bool end_ex;      // exclude last message
    bool is_reverse;  // scan in reverse order
    ScanStreamOptions(const rocksdb::Slice skey, int32_t version, streamID start_sid, streamID end_sid, size_t count,
                      bool start_ex = false, bool end_ex = false, bool is_reverse = false)
        : key(skey),
          version(version),
          start_sid(start_sid),
          end_sid(end_sid),
          limit(count),
          start_ex(start_ex),
          end_ex(end_ex),
          is_reverse(is_reverse) {}
  };

  Status ScanStream(const ScanStreamOptions& option, std::vector<IdMessage>& id_messages, std::string& next_field,
                    rocksdb::ReadOptions& read_options);
  // get and parse the stream meta if found
  // @return ok only when the stream meta exists
  Status GetStreamMeta(StreamMetaValue& tream_meta, const rocksdb::Slice& key, rocksdb::ReadOptions& read_options);

  // Before calling this function, the caller should ensure that the ids are valid
  Status DeleteStreamMessages(const rocksdb::Slice& key, const StreamMetaValue& stream_meta,
                              const std::vector<streamID>& ids, rocksdb::ReadOptions& read_options);

  // Before calling this function, the caller should ensure that the ids are valid
  Status DeleteStreamMessages(const rocksdb::Slice& key, const StreamMetaValue& stream_meta,
                              const std::vector<std::string>& serialized_ids, rocksdb::ReadOptions& read_options);

  Status TrimStream(size_t& count, StreamMetaValue& stream_meta, const rocksdb::Slice& key, StreamAddTrimArgs& args,
                    rocksdb::ReadOptions& read_options);

 private:
  Status GenerateStreamID(const StreamMetaValue& stream_meta, StreamAddTrimArgs& args);

  Status ScanRange(const Slice& key, const int32_t version, const Slice& id_start, const std::string& id_end,
                   const Slice& pattern, int32_t limit, std::vector<IdMessage>& id_messages, std::string& next_id,
                   rocksdb::ReadOptions& read_options);
  Status ReScanRange(const Slice& key, const int32_t version, const Slice& id_start, const std::string& id_end,
                     const Slice& pattern, int32_t limit, std::vector<IdMessage>& id_values, std::string& next_id,
                     rocksdb::ReadOptions& read_options);

  struct TrimRet {
    // the count of deleted messages
    size_t count{0};
    // the next field after trim
    std::string next_field;
    // the max deleted field, will be empty if no message is deleted
    std::string max_deleted_field;
  };

  Status TrimByMaxlen(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                      const StreamAddTrimArgs& args, rocksdb::ReadOptions& read_options);

  Status TrimByMinid(TrimRet& trim_ret, StreamMetaValue& stream_meta, const rocksdb::Slice& key,
                     const StreamAddTrimArgs& args, rocksdb::ReadOptions& read_options);

  inline Status SetFirstID(const rocksdb::Slice& key, StreamMetaValue& stream_meta,
                           rocksdb::ReadOptions& read_options) {
    return SetFirstOrLastID(key, stream_meta, true, read_options);
  }

  inline Status SetLastID(const rocksdb::Slice& key, StreamMetaValue& stream_meta, rocksdb::ReadOptions& read_options) {
    return SetFirstOrLastID(key, stream_meta, false, read_options);
  }

  inline Status SetFirstOrLastID(const rocksdb::Slice& key, StreamMetaValue& stream_meta, bool is_set_first,
                                 rocksdb::ReadOptions& read_options) {
    if (stream_meta.length() == 0) {
      stream_meta.set_first_id(kSTREAMID_MIN);
      return Status::OK();
    }

    std::vector<storage::IdMessage> id_messages;
    std::string next_field;

    storage::Status s;
    if (is_set_first) {
      ScanStreamOptions option(key, stream_meta.version(), kSTREAMID_MIN, kSTREAMID_MAX, 1);
      s = ScanStream(option, id_messages, next_field, read_options);
    } else {
      bool is_reverse = true;
      ScanStreamOptions option(key, stream_meta.version(), kSTREAMID_MAX, kSTREAMID_MIN, 1, false, false, is_reverse);
      s = ScanStream(option, id_messages, next_field, read_options);
    }
    (void)next_field;

    if (!s.ok() && !s.IsNotFound()) {
      LOG(ERROR) << "Internal error: scan stream failed: " << s.ToString();
      return Status::Corruption("Internal error: scan stream failed: " + s.ToString());
    }

    if (id_messages.empty()) {
      LOG(ERROR) << "Internal error: no messages found but stream length is not 0";
      return Status::Corruption("Internal error: no messages found but stream length is not 0");
    }

    streamID id;
    id.DeserializeFrom(id_messages[0].field);
    stream_meta.set_first_id(id);
    return Status::OK();
  }
};
}  // namespace storage