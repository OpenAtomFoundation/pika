//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_STORAGE_STORAGE_H_
#define INCLUDE_STORAGE_STORAGE_H_

#include <unistd.h>
#include <list>
#include <map>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"

#include "pstd/include/pstd_mutex.h"

namespace storage {

inline constexpr double ZSET_SCORE_MAX = std::numeric_limits<double>::max();
inline constexpr double ZSET_SCORE_MIN = std::numeric_limits<double>::lowest();

inline const std::string PROPERTY_TYPE_ROCKSDB_CUR_SIZE_ALL_MEM_TABLES = "rocksdb.cur-size-all-mem-tables";
inline const std::string PROPERTY_TYPE_ROCKSDB_ESTIMATE_TABLE_READER_MEM = "rocksdb.estimate-table-readers-mem";
inline const std::string PROPERTY_TYPE_ROCKSDB_BACKGROUND_ERRORS = "rocksdb.background-errors";

inline const std::string ALL_DB = "all";
inline const std::string STRINGS_DB = "strings";
inline const std::string HASHES_DB = "hashes";
inline const std::string LISTS_DB = "lists";
inline const std::string ZSETS_DB = "zsets";
inline const std::string SETS_DB = "sets";

inline constexpr size_t BATCH_DELETE_LIMIT = 100;
inline constexpr size_t COMPACT_THRESHOLD_COUNT = 2000;

using Options = rocksdb::Options;
using BlockBasedTableOptions = rocksdb::BlockBasedTableOptions;
using Status = rocksdb::Status;
using Slice = rocksdb::Slice;

class RedisStrings;
class RedisHashes;
class RedisSets;
class RedisLists;
class RedisZSets;
class HyperLogLog;
enum class OptionType;

template <typename T1, typename T2>
class LRUCache;

struct StorageOptions {
  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;
  size_t block_cache_size = 0;
  bool share_block_cache = false;
  size_t statistics_max_size = 0;
  size_t small_compaction_threshold = 5000;
  size_t small_compaction_duration_threshold = 10000;
  Status ResetOptions(const OptionType& option_type, const std::unordered_map<std::string, std::string>& options_map);
};

struct KeyValue {
  std::string key;
  std::string value;
  bool operator==(const KeyValue& kv) const { return (kv.key == key && kv.value == value); }
  bool operator<(const KeyValue& kv) const { return key < kv.key; }
};

struct KeyInfo {
  uint64_t keys;
  uint64_t expires;
  uint64_t avg_ttl;
  uint64_t invaild_keys;
};

struct ValueStatus {
  std::string value;
  Status status;
  int64_t  ttl;
  bool operator==(const ValueStatus& vs) const { return (vs.value == value && vs.status == status && vs.ttl == ttl); }
};

struct FieldValue {
  std::string field;
  std::string value;
  bool operator==(const FieldValue& fv) const { return (fv.field == field && fv.value == value); }
};

struct KeyVersion {
  std::string key;
  int32_t version;
  bool operator==(const KeyVersion& kv) const { return (kv.key == key && kv.version == version); }
};

struct ScoreMember {
  double score;
  std::string member;
  bool operator==(const ScoreMember& sm) const { return (sm.score == score && sm.member == member); }
};

enum BeforeOrAfter { Before, After };

enum DataType { kAll, kStrings, kHashes, kLists, kZSets, kSets };

const char DataTypeTag[] = {'a', 'k', 'h', 'l', 'z', 's'};

enum class OptionType {
  kDB,
  kColumnFamily,
};

enum ColumnFamilyType { kMeta, kData, kMetaAndData };

enum AGGREGATE { SUM, MIN, MAX };

enum BitOpType { kBitOpAnd = 1, kBitOpOr, kBitOpXor, kBitOpNot, kBitOpDefault };

enum Operation { kNone = 0, kCleanAll, kCleanStrings, kCleanHashes, kCleanZSets, kCleanSets, kCleanLists, kCompactRange };

struct BGTask {
  DataType type;
  Operation operation;
  std::vector<std::string> argv;

  BGTask(const DataType& _type = DataType::kAll,
         const Operation& _opeation = Operation::kNone,
         const std::vector<std::string>& _argv = {}) : type(_type), operation(_opeation), argv(_argv) {}
};

class Storage {
 public:
  Storage();
  ~Storage();

  Status Open(const StorageOptions& storage_options, const std::string& db_path);

  Status GetStartKey(const DataType& dtype, int64_t cursor, std::string* start_key);

  Status StoreCursorStartKey(const DataType& dtype, int64_t cursor, const std::string& next_key);

  // Strings Commands

  // Set key to hold the string value. if key
  // already holds a value, it is overwritten
  Status Set(const Slice& key, const Slice& value);

  // Set key to hold the string value. if key exist
  Status Setxx(const Slice& key, const Slice& value, int32_t* ret, int32_t ttl = 0);

  // Get the value of key. If the key does not exist
  // the special value nil is returned
  Status Get(const Slice& key, std::string* value);

  // Get the value and ttl of key. If the key does not exist
  // the special value nil is returned. If the key has no ttl, ttl is -1
  Status GetWithTTL(const Slice& key, std::string* value, int64_t* ttl);

  // Atomically sets key to value and returns the old value stored at key
  // Returns an error when key exists but does not hold a string value.
  Status GetSet(const Slice& key, const Slice& value, std::string* old_value);

  // Sets or clears the bit at offset in the string value stored at key
  Status SetBit(const Slice& key, int64_t offset, int32_t value, int32_t* ret);

  // Returns the bit value at offset in the string value stored at key
  Status GetBit(const Slice& key, int64_t offset, int32_t* ret);

  // Sets the given keys to their respective values
  // MSET replaces existing values with new values
  Status MSet(const std::vector<KeyValue>& kvs);

  // Returns the values of all specified keys. For every key
  // that does not hold a string value or does not exist, the
  // special value nil is returned
  Status MGet(const std::vector<std::string>& keys, std::vector<ValueStatus>* vss);

  // Returns the values of all specified keyswithTTL. For every key
  // that does not hold a string value or does not exist, the
  // special value nil is returned
  Status MGetWithTTL(const std::vector<std::string>& keys, std::vector<ValueStatus>* vss);

  // Set key to hold string value if key does not exist
  // return 1 if the key was set
  // return 0 if the key was not set
  Status Setnx(const Slice& key, const Slice& value, int32_t* ret, int32_t ttl = 0);

  // Sets the given keys to their respective values.
  // MSETNX will not perform any operation at all even
  // if just a single key already exists.
  Status MSetnx(const std::vector<KeyValue>& kvs, int32_t* ret);

  // Set key to hold string new_value if key currently hold the give value
  // return 1 if the key currently hold the give value And override success
  // return 0 if the key doesn't exist And override fail
  // return -1 if the key currently does not hold the given value And override fail
  Status Setvx(const Slice& key, const Slice& value, const Slice& new_value, int32_t* ret, int32_t ttl = 0);

  // delete the key that holds a given value
  // return 1 if the key currently hold the give value And delete success
  // return 0 if the key doesn't exist And del fail
  // return -1 if the key currently does not hold the given value And del fail
  Status Delvx(const Slice& key, const Slice& value, int32_t* ret);

  // Set key to hold string value if key does not exist
  // return the length of the string after it was modified by the command
  Status Setrange(const Slice& key, int64_t start_offset, const Slice& value, int32_t* ret);

  // Returns the substring of the string value stored at key,
  // determined by the offsets start and end (both are inclusive)
  Status Getrange(const Slice& key, int64_t start_offset, int64_t end_offset, std::string* ret);

  Status GetrangeWithValue(const Slice& key, int64_t start_offset, int64_t end_offset,
                           std::string* ret, std::string* value, int64_t* ttl);

  // If key already exists and is a string, this command appends the value at
  // the end of the string
  // return the length of the string after the append operation
  Status Append(const Slice& key, const Slice& value, int32_t* ret);

  // Count the number of set bits (population counting) in a string.
  // return the number of bits set to 1
  // note: if need to specified offset, set have_range to true
  Status BitCount(const Slice& key, int64_t start_offset, int64_t end_offset, int32_t* ret, bool have_range);

  // Perform a bitwise operation between multiple keys
  // and store the result in the destination key
  Status BitOp(BitOpType op, const std::string& dest_key, const std::vector<std::string>& src_keys, std::string &value_to_dest, int64_t* ret);

  // Return the position of the first bit set to 1 or 0 in a string
  // BitPos key 0
  Status BitPos(const Slice& key, int32_t bit, int64_t* ret);
  // BitPos key 0 [start]
  Status BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t* ret);
  // BitPos key 0 [start] [end]
  Status BitPos(const Slice& key, int32_t bit, int64_t start_offset, int64_t end_offset, int64_t* ret);

  // Decrements the number stored at key by decrement
  // return the value of key after the decrement
  Status Decrby(const Slice& key, int64_t value, int64_t* ret);

  // Increments the number stored at key by increment.
  // If the key does not exist, it is set to 0 before performing the operation
  Status Incrby(const Slice& key, int64_t value, int64_t* ret);

  // Increment the string representing a floating point number
  // stored at key by the specified increment.
  Status Incrbyfloat(const Slice& key, const Slice& value, std::string* ret);

  // Set key to hold the string value and set key to timeout after a given
  // number of seconds
  Status Setex(const Slice& key, const Slice& value, int32_t ttl);

  // Returns the length of the string value stored at key. An error
  // is returned when key holds a non-string value.
  Status Strlen(const Slice& key, int32_t* len);

  // PKSETEXAT has the same effect and semantic as SETEX, but instead of
  // specifying the number of seconds representing the TTL (time to live), it
  // takes an absolute Unix timestamp (seconds since January 1, 1970). A
  // timestamp in the past will delete the key immediately.
  Status PKSetexAt(const Slice& key, const Slice& value, int32_t timestamp);

  // Hashes Commands

  // Sets field in the hash stored at key to value. If key does not exist, a new
  // key holding a hash is created. If field already exists in the hash, it is
  // overwritten.
  Status HSet(const Slice& key, const Slice& field, const Slice& value, int32_t* res);

  // Returns the value associated with field in the hash stored at key.
  // the value associated with field, or nil when field is not present in the
  // hash or key does not exist.
  Status HGet(const Slice& key, const Slice& field, std::string* value);

  // Sets the specified fields to their respective values in the hash stored at
  // key. This command overwrites any specified fields already existing in the
  // hash. If key does not exist, a new key holding a hash is created.
  Status HMSet(const Slice& key, const std::vector<FieldValue>& fvs);

  // Returns the values associated with the specified fields in the hash stored
  // at key.
  // For every field that does not exist in the hash, a nil value is returned.
  // Because a non-existing keys are treated as empty hashes, running HMGET
  // against a non-existing key will return a list of nil values.
  Status HMGet(const Slice& key, const std::vector<std::string>& fields, std::vector<ValueStatus>* vss);

  // Returns all fields and values of the hash stored at key. In the returned
  // value, every field name is followed by its value, so the length of the
  // reply is twice the size of the hash.
  Status HGetall(const Slice& key, std::vector<FieldValue>* fvs);

  Status HGetallWithTTL(const Slice& key, std::vector<FieldValue>* fvs, int64_t* ttl);

  // Returns all field names in the hash stored at key.
  Status HKeys(const Slice& key, std::vector<std::string>* fields);

  // Returns all values in the hash stored at key.
  Status HVals(const Slice& key, std::vector<std::string>* values);

  // Sets field in the hash stored at key to value, only if field does not yet
  // exist. If key does not exist, a new key holding a hash is created. If field
  // already exists, this operation has no effect.
  Status HSetnx(const Slice& key, const Slice& field, const Slice& value, int32_t* ret);

  // Returns the number of fields contained in the hash stored at key.
  // Return 0 when key does not exist.
  Status HLen(const Slice& key, int32_t* ret);

  // Returns the string length of the value associated with field in the hash
  // stored at key. If the key or the field do not exist, 0 is returned.
  Status HStrlen(const Slice& key, const Slice& field, int32_t* len);

  // Returns if field is an existing field in the hash stored at key.
  // Return Status::Ok() if the hash contains field.
  // Return Status::NotFound() if the hash does not contain field,
  // or key does not exist.
  Status HExists(const Slice& key, const Slice& field);

  // Increments the number stored at field in the hash stored at key by
  // increment. If key does not exist, a new key holding a hash is created. If
  // field does not exist the value is set to 0 before the operation is
  // performed.
  Status HIncrby(const Slice& key, const Slice& field, int64_t value, int64_t* ret);

  // Increment the specified field of a hash stored at key, and representing a
  // floating point number, by the specified increment. If the increment value
  // is negative, the result is to have the hash field value decremented instead
  // of incremented. If the field does not exist, it is set to 0 before
  // performing the operation. An error is returned if one of the following
  // conditions occur:
  //
  // The field contains a value of the wrong type (not a string).
  // The current field content or the specified increment are not parsable as a
  // double precision floating point number.
  Status HIncrbyfloat(const Slice& key, const Slice& field, const Slice& by, std::string* new_value);

  // Removes the specified fields from the hash stored at key. Specified fields
  // that do not exist within this hash are ignored. If key does not exist, it
  // is treated as an empty hash and this command returns 0.
  Status HDel(const Slice& key, const std::vector<std::string>& fields, int32_t* ret);

  // See SCAN for HSCAN documentation.
  Status HScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<FieldValue>* field_values, int64_t* next_cursor);

  // Iterate over a Hash table of fields
  // return next_field that the user need to use as the start_field argument
  // in the next call
  Status HScanx(const Slice& key, const std::string& start_field, const std::string& pattern, int64_t count,
                std::vector<FieldValue>* field_values, std::string* next_field);

  // Iterate over a Hash table of fields by specified range
  // return next_field that the user need to use as the start_field argument
  // in the next call
  Status PKHScanRange(const Slice& key, const Slice& field_start, const std::string& field_end, const Slice& pattern,
                      int32_t limit, std::vector<FieldValue>* field_values, std::string* next_field);

  // part from the reversed ordering, PKHRSCANRANGE is similar to PKHScanRange
  Status PKHRScanRange(const Slice& key, const Slice& field_start, const std::string& field_end, const Slice& pattern,
                       int32_t limit, std::vector<FieldValue>* field_values, std::string* next_field);

  // Sets Commands

  // Add the specified members to the set stored at key. Specified members that
  // are already a member of this set are ignored. If key does not exist, a new
  // set is created before adding the specified members.
  Status SAdd(const Slice& key, const std::vector<std::string>& members, int32_t* ret);

  // Returns the set cardinality (number of elements) of the set stored at key.
  Status SCard(const Slice& key, int32_t* ret);

  // Returns the members of the set resulting from the difference between the
  // first set and all the successive sets.
  //
  // For example:
  //   key1 = {a, b, c, d}
  //   key2 = {c}
  //   key3 = {a, c, e}
  //   SDIFF key1 key2 key3  = {b, d}
  Status SDiff(const std::vector<std::string>& keys, std::vector<std::string>* members);

  // This command is equal to SDIFF, but instead of returning the resulting set,
  // it is stored in destination.
  // If destination already exists, it is overwritten.
  //
  // For example:
  //   destination = {};
  //   key1 = {a, b, c, d}
  //   key2 = {c}
  //   key3 = {a, c, e}
  //   SDIFFSTORE destination key1 key2 key3
  //   destination = {b, d}
  Status SDiffstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret);

  // Returns the members of the set resulting from the intersection of all the
  // given sets.
  //
  // For example:
  //   key1 = {a, b, c, d}
  //   key2 = {c}
  //   key3 = {a, c, e}
  //   SINTER key1 key2 key3 = {c}
  Status SInter(const std::vector<std::string>& keys, std::vector<std::string>* members);

  // This command is equal to SINTER, but instead of returning the resulting
  // set, it is stored in destination.
  // If destination already exists, it is overwritten.
  //
  // For example:
  //   destination = {}
  //   key1 = {a, b, c, d}
  //   key2 = {a, c}
  //   key3 = {a, c, e}
  //   SINTERSTORE destination key1 key2 key3
  //   destination = {a, c}
  Status SInterstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret);

  // Returns if member is a member of the set stored at key.
  Status SIsmember(const Slice& key, const Slice& member, int32_t* ret);

  // Returns all the members of the set value stored at key.
  // This has the same effect as running SINTER with one argument key.
  Status SMembers(const Slice& key, std::vector<std::string>* members);

  Status SMembersWithTTL(const Slice& key, std::vector<std::string>* members, int64_t *ttl);

  // Remove the specified members from the set stored at key. Specified members
  // that are not a member of this set are ignored. If key does not exist, it is
  // treated as an empty set and this command returns 0.
  Status SRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret);

  // Removes and returns several random elements specified by count from the set value store at key.
  Status SPop(const Slice& key, std::vector<std::string>* members, int64_t count);

  // When called with just the key argument, return a random element from the
  // set value stored at key.
  // when called with the additional count argument, return an array of count
  // distinct elements if count is positive. If called with a negative count the
  // behavior changes and the command is allowed to return the same element
  // multiple times. In this case the number of returned elements is the
  // absolute value of the specified count
  Status SRandmember(const Slice& key, int32_t count, std::vector<std::string>* members);

  // Move member from the set at source to the set at destination. This
  // operation is atomic. In every given moment the element will appear to be a
  // member of source or destination for other clients.
  //
  // If the source set does not exist or does not contain the specified element,
  // no operation is performed and 0 is returned. Otherwise, the element is
  // removed from the source set and added to the destination set. When the
  // specified element already exists in the destination set, it is only removed
  // from the source set.
  Status SMove(const Slice& source, const Slice& destination, const Slice& member, int32_t* ret);

  // Returns the members of the set resulting from the union of all the given
  // sets.
  //
  // For example:
  //   key1 = {a, b, c, d}
  //   key2 = {c}
  //   key3 = {a, c, e}
  //   SUNION key1 key2 key3 = {a, b, c, d, e}
  Status SUnion(const std::vector<std::string>& keys, std::vector<std::string>* members);

  // This command is equal to SUNION, but instead of returning the resulting
  // set, it is stored in destination.
  // If destination already exists, it is overwritten.
  //
  // For example:
  //   key1 = {a, b}
  //   key2 = {c, d}
  //   key3 = {c, d, e}
  //   SUNIONSTORE destination key1 key2 key3
  //   destination = {a, b, c, d, e}
  Status SUnionstore(const Slice& destination, const std::vector<std::string>& keys, std::vector<std::string>& value_to_dest, int32_t* ret);

  // See SCAN for SSCAN documentation.
  Status SScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<std::string>* members, int64_t* next_cursor);

  // Lists Commands

  // Insert all the specified values at the head of the list stored at key. If
  // key does not exist, it is created as empty list before performing the push
  // operations.
  Status LPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret);

  // Insert all the specified values at the tail of the list stored at key. If
  // key does not exist, it is created as empty list before performing the push
  // operation.
  Status RPush(const Slice& key, const std::vector<std::string>& values, uint64_t* ret);

  // Returns the specified elements of the list stored at key. The offsets start
  // and stop are zero-based indexes, with 0 being the first element of the list
  // (the head of the list), 1 being the next element and so on.
  Status LRange(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret);

  Status LRangeWithTTL(const Slice& key, int64_t start, int64_t stop, std::vector<std::string>* ret, int64_t *ttl);

  // Removes the first count occurrences of elements equal to value from the
  // list stored at key. The count argument influences the operation in the
  // following ways
  Status LTrim(const Slice& key, int64_t start, int64_t stop);

  // Returns the length of the list stored at key. If key does not exist, it is
  // interpreted as an empty list and 0 is returned. An error is returned when
  // the value stored at key is not a list.
  Status LLen(const Slice& key, uint64_t* len);

  // Removes and returns the first elements of the list stored at key.
  Status LPop(const Slice& key, int64_t count, std::vector<std::string>* elements);

  // Removes and returns the last elements of the list stored at key.
  Status RPop(const Slice& key, int64_t count, std::vector<std::string>* elements);

  // Returns the element at index index in the list stored at key. The index is
  // zero-based, so 0 means the first element, 1 the second element and so on.
  // Negative indices can be used to designate elements starting at the tail of
  // the list. Here, -1 means the last element, -2 means the penultimate and so
  // forth.
  Status LIndex(const Slice& key, int64_t index, std::string* element);

  // Inserts value in the list stored at key either before or after the
  // reference value pivot.
  // When key does not exist, it is considered an empty list and no operation is
  // performed.
  // An error is returned when key exists but does not hold a list value.
  Status LInsert(const Slice& key, const BeforeOrAfter& before_or_after, const std::string& pivot,
                 const std::string& value, int64_t* ret);

  // Inserts value at the head of the list stored at key, only if key already
  // exists and holds a list. In contrary to LPUSH, no operation will be
  // performed when key does not yet exist.
  Status LPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len);

  // Inserts value at the tail of the list stored at key, only if key already
  // exists and holds a list. In contrary to RPUSH, no operation will be
  // performed when key does not yet exist.
  Status RPushx(const Slice& key, const std::vector<std::string>& values, uint64_t* len);

  // Removes the first count occurrences of elements equal to value from the
  // list stored at key. The count argument influences the operation in the
  // following ways:
  //
  // count > 0: Remove elements equal to value moving from head to tail.
  // count < 0: Remove elements equal to value moving from tail to head.
  // count = 0: Remove all elements equal to value.
  // For example, LREM list -2 "hello" will remove the last two occurrences of
  // "hello" in the list stored at list.
  //
  // Note that non-existing keys are treated like empty lists, so when key does
  // not exist, the command will always return 0.
  Status LRem(const Slice& key, int64_t count, const Slice& value, uint64_t* ret);

  // Sets the list element at index to value. For more information on the index
  // argument, see LINDEX.
  //
  // An error is returned for out of range indexes.
  Status LSet(const Slice& key, int64_t index, const Slice& value);

  // Atomically returns and removes the last element (tail) of the list stored
  // at source, and pushes the element at the first element (head) of the list
  // stored at destination.
  //
  // For example: consider source holding the list a,b,c, and destination
  // holding the list x,y,z. Executing RPOPLPUSH results in source holding a,b
  // and destination holding c,x,y,z.
  //
  // If source does not exist, the value nil is returned and no operation is
  // performed. If source and destination are the same, the operation is
  // equivalent to removing the last element from the list and pushing it as
  // first element of the list, so it can be considered as a list rotation
  // command.
  Status RPoplpush(const Slice& source, const Slice& destination, std::string* element);

  // Zsets Commands

  // Pop the maximum count score_members which have greater score in the sorted set.
  // And return the result in the score_members,If the total number of the sorted
  // set less than count, it will pop out the total number of sorted set. If two
  // ScoreMember's score were the same, the lexicographic predominant elements will
  // be pop out.
  Status ZPopMax(const Slice& key, int64_t count, std::vector<ScoreMember>* score_members);

  // Pop the minimum count score_members which have less score in the sorted set.
  // And return the result in the score_members,If the total number of the sorted
  // set less than count, it will pop out the total number of sorted set. If two
  // ScoreMember's score were the same, the lexicographic predominant elements will
  // not be pop out.
  Status ZPopMin(const Slice& key, int64_t count, std::vector<ScoreMember>* score_members);

  // Adds all the specified members with the specified scores to the sorted set
  // stored at key. It is possible to specify multiple score / member pairs. If
  // a specified member is already a member of the sorted set, the score is
  // updated and the element reinserted at the right position to ensure the
  // correct ordering.
  //
  // If key does not exist, a new sorted set with the specified members as sole
  // members is created, like if the sorted set was empty. If the key exists but
  // does not hold a sorted set, an error is returned.
  // The score values should be the string representation of a double precision
  // floating point number. +inf and -inf values are valid values as well.
  Status ZAdd(const Slice& key, const std::vector<ScoreMember>& score_members, int32_t* ret);

  // Returns the sorted set cardinality (number of elements) of the sorted set
  // stored at key.
  Status ZCard(const Slice& key, int32_t* ret);

  // Returns the number of elements in the sorted set at key with a score
  // between min and max.
  //
  // The min and max arguments have the same semantic as described for
  // ZRANGEBYSCORE.
  //
  // Note: the command has a complexity of just O(log(N)) because it uses
  // elements ranks (see ZRANK) to get an idea of the range. Because of this
  // there is no need to do a work proportional to the size of the range.
  Status ZCount(const Slice& key, double min, double max, bool left_close, bool right_close, int32_t* ret);

  // Increments the score of member in the sorted set stored at key by
  // increment. If member does not exist in the sorted set, it is added with
  // increment as its score (as if its previous score was 0.0). If key does not
  // exist, a new sorted set with the specified member as its sole member is
  // created.
  //
  // An error is returned when key exists but does not hold a sorted set.
  //
  // The score value should be the string representation of a numeric value, and
  // accepts double precision floating point numbers. It is possible to provide
  // a negative value to decrement the score.
  Status ZIncrby(const Slice& key, const Slice& member, double increment, double* ret);

  // Returns the specified range of elements in the sorted set stored at key.
  // The elements are considered to be ordered from the lowest to the highest
  // score. Lexicographical order is used for elements with equal score.
  //
  // See ZREVRANGE when you need the elements ordered from highest to lowest
  // score (and descending lexicographical order for elements with equal score).
  //
  // Both start and stop are zero-based indexes, where 0 is the first element, 1
  // is the next element and so on. They can also be negative numbers indicating
  // offsets from the end of the sorted set, with -1 being the last element of
  // the sorted set, -2 the penultimate element and so on.
  //
  // start and stop are inclusive ranges, so for example ZRANGE myzset 0 1 will
  // return both the first and the second element of the sorted set.
  //
  // Out of range indexes will not produce an error. If start is larger than the
  // largest index in the sorted set, or start > stop, an empty list is
  // returned. If stop is larger than the end of the sorted set Redis will treat
  // it like it is the last element of the sorted set.
  //
  // It is possible to pass the WITHSCORES option in order to return the scores
  // of the elements together with the elements. The returned list will contain
  // value1,score1,...,valueN,scoreN instead of value1,...,valueN. Client
  // libraries are free to return a more appropriate data type (suggestion: an
  // array with (value, score) arrays/tuples).
  Status ZRange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members);

  Status ZRangeWithTTL(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members,
                                int64_t *ttl);

  // Returns all the elements in the sorted set at key with a score between min
  // and max (including elements with score equal to min or max). The elements
  // are considered to be ordered from low to high scores.
  //
  // The elements having the same score are returned in lexicographical order
  // (this follows from a property of the sorted set implementation in Redis and
  // does not involve further computation).
  //
  // The optional LIMIT argument can be used to only get a range of the matching
  // elements (similar to SELECT LIMIT offset, count in SQL). Keep in mind that
  // if offset is large, the sorted set needs to be traversed for offset
  // elements before getting to the elements to return, which can add up to O(N)
  // time complexity.
  //
  // The optional WITHSCORES argument makes the command return both the element
  // and its score, instead of the element alone. This option is available since
  // Redis 2.0.
  //
  // Exclusive intervals and infinity
  // min and max can be -inf and +inf, so that you are not required to know the
  // highest or lowest score in the sorted set to get all elements from or up to
  // a certain score.
  //
  // By default, the interval specified by min and max is closed (inclusive). It
  // is possible to specify an open interval (exclusive) by prefixing the score
  // with the character (. For example:
  //
  // ZRANGEBYSCORE zset (1 5
  // Will return all elements with 1 < score <= 5 while:
  //
  // ZRANGEBYSCORE zset (5 (10
  // Will return all the elements with 5 < score < 10 (5 and 10 excluded).
  //
  // Return value
  // Array reply: list of elements in the specified score range (optionally with
  // their scores).
  Status ZRangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                       std::vector<ScoreMember>* score_members);

  // Returns all the elements in the sorted set at key with a score between min
  // and max (including elements with score equal to min or max). The elements
  // are considered to be ordered from low to high scores.
  //
  // The elements having the same score are returned in lexicographical order
  // (this follows from a property of the sorted set implementation in Redis and
  // does not involve further computation).
  //
  // The optional LIMIT argument can be used to only get a range of the matching
  // elements (similar to SELECT LIMIT offset, count in SQL). Keep in mind that
  // if offset is large, the sorted set needs to be traversed for offset
  // elements before getting to the elements to return, which can add up to O(N)
  // time complexity.
  //
  // The optional WITHSCORES argument makes the command return both the element
  // and its score, instead of the element alone. This option is available since
  // Redis 2.0.
  //
  // Exclusive intervals and infinity
  // min and max can be -inf and +inf, so that you are not required to know the
  // highest or lowest score in the sorted set to get all elements from or up to
  // a certain score.
  //
  // By default, the interval specified by min and max is closed (inclusive). It
  // is possible to specify an open interval (exclusive) by prefixing the score
  // with the character (. For example:
  //
  // ZRANGEBYSCORE zset (1 5
  // Will return all elements with 1 < score <= 5 while:
  //
  // ZRANGEBYSCORE zset (5 (10
  // Will return all the elements with 5 < score < 10 (5 and 10 excluded).
  //
  // Return value
  // Array reply: list of elements in the specified score range (optionally with
  // their scores).
  Status ZRangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close, int64_t count,
                       int64_t offset, std::vector<ScoreMember>* score_members);

  // Returns the rank of member in the sorted set stored at key, with the scores
  // ordered from low to high. The rank (or index) is 0-based, which means that
  // the member with the lowest score has rank 0.
  //
  // Use ZREVRANK to get the rank of an element with the scores ordered from
  // high to low.
  Status ZRank(const Slice& key, const Slice& member, int32_t* rank);

  // Removes the specified members from the sorted set stored at key. Non
  // existing members are ignored.
  //
  // An error is returned when key exists and does not hold a sorted set.
  Status ZRem(const Slice& key, const std::vector<std::string>& members, int32_t* ret);

  // Removes all elements in the sorted set stored at key with rank between
  // start and stop. Both start and stop are 0 -based indexes with 0 being the
  // element with the lowest score. These indexes can be negative numbers, where
  // they indicate offsets starting at the element with the highest score. For
  // example: -1 is the element with the highest score, -2 the element with the
  // second highest score and so forth.
  Status ZRemrangebyrank(const Slice& key, int32_t start, int32_t stop, int32_t* ret);

  // Removes all elements in the sorted set stored at key with a score between
  // min and max (inclusive).
  Status ZRemrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close, int32_t* ret);

  // Returns the specified range of elements in the sorted set stored at key.
  // The elements are considered to be ordered from the highest to the lowest
  // score. Descending lexicographical order is used for elements with equal
  // score.
  //
  // Apart from the reversed ordering, ZREVRANGE is similar to ZRANGE.
  Status ZRevrange(const Slice& key, int32_t start, int32_t stop, std::vector<ScoreMember>* score_members);

  // Returns all the elements in the sorted set at key with a score between max
  // and min (including elements with score equal to max or min). In contrary to
  // the default ordering of sorted sets, for this command the elements are
  // considered to be ordered from high to low scores.
  //
  // The elements having the same score are returned in reverse lexicographical
  // order.
  //
  // Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to
  // ZRANGEBYSCORE.
  Status ZRevrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close,
                          std::vector<ScoreMember>* score_members);

  // Returns all the elements in the sorted set at key with a score between max
  // and min (including elements with score equal to max or min). In contrary to
  // the default ordering of sorted sets, for this command the elements are
  // considered to be ordered from high to low scores.
  //
  // The elements having the same score are returned in reverse lexicographical
  // order.
  //
  // Apart from the reversed ordering, ZREVRANGEBYSCORE is similar to
  // ZRANGEBYSCORE.
  Status ZRevrangebyscore(const Slice& key, double min, double max, bool left_close, bool right_close, int64_t count,
                          int64_t offset, std::vector<ScoreMember>* score_members);

  // Returns the rank of member in the sorted set stored at key, with the scores
  // ordered from high to low. The rank (or index) is 0-based, which means that
  // the member with the highest score has rank 0.
  Status ZRevrank(const Slice& key, const Slice& member, int32_t* rank);

  // Returns the score of member in the sorted set at key.
  //
  // If member does not exist in the sorted set, or key does not exist, nil is
  // returned.
  Status ZScore(const Slice& key, const Slice& member, double* ret);

  // Computes the union of numkeys sorted sets given by the specified keys, and
  // stores the result in destination. It is mandatory to provide the number of
  // input keys (numkeys) before passing the input keys and the other (optional)
  // arguments.
  //
  // By default, the resulting score of an element is the sum of its scores in
  // the sorted sets where it exists.
  //
  // Using the WEIGHTS option, it is possible to specify a multiplication factor
  // for each input sorted set. This means that the score of every element in
  // every input sorted set is multiplied by this factor before being passed to
  // the aggregation function. When WEIGHTS is not given, the multiplication
  // factors default to 1.
  //
  // With the AGGREGATE option, it is possible to specify how the results of the
  // union are aggregated. This option defaults to SUM, where the score of an
  // element is summed across the inputs where it exists. When this option is
  // set to either MIN or MAX, the resulting set will contain the minimum or
  // maximum score of an element across the inputs where it exists.
  //
  // If destination already exists, it is overwritten.
  Status ZUnionstore(const Slice& destination, const std::vector<std::string>& keys, const std::vector<double>& weights,
                     AGGREGATE agg, std::map<std::string, double>& value_to_dest, int32_t* ret);

  // Computes the intersection of numkeys sorted sets given by the specified
  // keys, and stores the result in destination. It is mandatory to provide the
  // number of input keys (numkeys) before passing the input keys and the other
  // (optional) arguments.
  //
  // By default, the resulting score of an element is the sum of its scores in
  // the sorted sets where it exists. Because intersection requires an element
  // to be a member of every given sorted set, this results in the score of
  // every element in the resulting sorted set to be equal to the number of
  // input sorted sets.
  //
  // For a description of the WEIGHTS and AGGREGATE options, see ZUNIONSTORE.
  //
  // If destination already exists, it is overwritten.
  Status ZInterstore(const Slice& destination, const std::vector<std::string>& keys, const std::vector<double>& weights,
                     AGGREGATE agg, std::vector<ScoreMember>& value_to_dest, int32_t* ret);

  // When all the elements in a sorted set are inserted with the same score, in
  // order to force lexicographical ordering, this command returns all the
  // elements in the sorted set at key with a value between min and max.
  //
  // If the elements in the sorted set have different scores, the returned
  // elements are unspecified.
  //
  // The elements are considered to be ordered from lower to higher strings as
  // compared byte-by-byte using the memcmp() C function. Longer strings are
  // considered greater than shorter strings if the common part is identical.
  //
  // The optional LIMIT argument can be used to only get a range of the matching
  // elements (similar to SELECT LIMIT offset, count in SQL). Keep in mind that
  // if offset is large, the sorted set needs to be traversed for offset
  // elements before getting to the elements to return, which can add up to O(N)
  // time complexity.
  Status ZRangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                     std::vector<std::string>* members);

  // When all the elements in a sorted set are inserted with the same score, in
  // order to force lexicographical ordering, this command returns the number of
  // elements in the sorted set at key with a value between min and max.
  //
  // The min and max arguments have the same meaning as described for
  // ZRANGEBYLEX.
  //
  // Note: the command has a complexity of just O(log(N)) because it uses
  // elements ranks (see ZRANK) to get an idea of the range. Because of this
  // there is no need to do a work proportional to the size of the range.
  Status ZLexcount(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                   int32_t* ret);

  // When all the elements in a sorted set are inserted with the same score, in
  // order to force lexicographical ordering, this command removes all elements
  // in the sorted set stored at key between the lexicographical range specified
  // by min and max.
  //
  // The meaning of min and max are the same of the ZRANGEBYLEX command.
  // Similarly, this command actually returns the same elements that ZRANGEBYLEX
  // would return if called with the same min and max arguments.
  Status ZRemrangebylex(const Slice& key, const Slice& min, const Slice& max, bool left_close, bool right_close,
                        int32_t* ret);

  // See SCAN for ZSCAN documentation.
  Status ZScan(const Slice& key, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<ScoreMember>* score_members, int64_t* next_cursor);

  // Keys Commands

  // Note:
  // While any error happens, you need to check type_status for
  // the error message

  // Set a timeout on key
  // return -1 operation exception errors happen in database
  // return >=0 success
  int32_t Expire(const Slice& key, int32_t ttl, std::map<DataType, Status>* type_status);

  // Removes the specified keys
  // return -1 operation exception errors happen in database
  // return >=0 the number of keys that were removed
  int64_t Del(const std::vector<std::string>& keys, std::map<DataType, Status>* type_status);

  // Removes the specified keys of the specified type
  // return -1 operation exception errors happen in database
  // return >= 0 the number of keys that were removed
  int64_t DelByType(const std::vector<std::string>& keys, const DataType& type);

  // Iterate over a collection of elements
  // return an updated cursor that the user need to use as the cursor argument
  // in the next call
  int64_t Scan(const DataType& dtype, int64_t cursor, const std::string& pattern, int64_t count,
               std::vector<std::string>* keys);

  // Iterate over a collection of elements, obtaining the item which timeout
  // conforms to the inequality (min_ttl < item_ttl < max_ttl)
  // return an updated cursor that the user need to use as the cursor argument
  // in the next call
  int64_t PKExpireScan(const DataType& dtype, int64_t cursor, int32_t min_ttl, int32_t max_ttl, int64_t count,
                       std::vector<std::string>* keys);

  // Iterate over a collection of elements by specified range
  // return a next_key that the user need to use as the key_start argument
  // in the next call
  Status PKScanRange(const DataType& data_type, const Slice& key_start, const Slice& key_end, const Slice& pattern,
                     int32_t limit, std::vector<std::string>* keys, std::vector<KeyValue>* kvs, std::string* next_key);

  // part from the reversed ordering, PKRSCANRANGE is similar to PKScanRange
  Status PKRScanRange(const DataType& data_type, const Slice& key_start, const Slice& key_end, const Slice& pattern,
                      int32_t limit, std::vector<std::string>* keys, std::vector<KeyValue>* kvs, std::string* next_key);

  // Traverses the database of the specified type, removing the Key that matches
  // the pattern
  Status PKPatternMatchDel(const DataType& data_type, const std::string& pattern, int32_t* ret);

  // Iterate over a collection of elements
  // return next_key that the user need to use as the start_key argument
  // in the next call
  Status Scanx(const DataType& data_type, const std::string& start_key, const std::string& pattern, int64_t count,
               std::vector<std::string>* keys, std::string* next_key);

  // Returns if key exists.
  // return -1 operation exception errors happen in database
  // return >=0 the number of keys existing
  int64_t Exists(const std::vector<std::string>& keys, std::map<DataType, Status>* type_status);

  // Return the key exists type count
  // return param type_status: return every type status
  int64_t IsExist(const Slice& key, std::map<DataType, Status>* type_status);

  // EXPIREAT has the same effect and semantic as EXPIRE, but instead of
  // specifying the number of seconds representing the TTL (time to live), it
  // takes an absolute Unix timestamp (seconds since January 1, 1970). A
  // timestamp in the past will delete the key immediately.
  // return -1 operation exception errors happen in database
  // return 0 if key does not exist
  // return >=1 if the timueout was set
  int32_t Expireat(const Slice& key, int32_t timestamp, std::map<DataType, Status>* type_status);

  // Remove the existing timeout on key, turning the key from volatile (a key
  // with an expire set) to persistent (a key that will never expire as no
  // timeout is associated).
  // return -1 operation exception errors happen in database
  // return 0 if key does not exist or does not have an associated timeout
  // return >=1 if the timueout was set
  int32_t Persist(const Slice& key, std::map<DataType, Status>* type_status);

  // Returns the remaining time to live of a key that has a timeout.
  // return -3 operation exception errors happen in database
  // return -2 if the key does not exist
  // return -1 if the key exists but has not associated expire
  // return > 0 TTL in seconds
  std::map<DataType, int64_t> TTL(const Slice& key, std::map<DataType, Status>* type_status);

  // Reutrns the data all type of the key
  // if single is true, the query will return the first one
  Status GetType(const std::string& key, bool single, std::vector<std::string>& types);

  // Reutrns the data all type of the key
  Status Type(const std::string& key, std::vector<std::string>& types);

  Status Keys(const DataType& data_type, const std::string& pattern, std::vector<std::string>* keys);

  // Dynamic switch WAL
  void DisableWal(const bool is_wal_disable);

  // Iterate through all the data in the database.
  void ScanDatabase(const DataType& type);

  // HyperLogLog
  enum {
    kMaxKeys = 255,
    kPrecision = 17,
  };
  // Adds all the element arguments to the HyperLogLog data structure stored
  // at the variable name specified as first argument.
  Status PfAdd(const Slice& key, const std::vector<std::string>& values, bool* update);

  // When called with a single key, returns the approximated cardinality
  // computed by the HyperLogLog data structure stored at the specified
  // variable, which is 0 if the variable does not exist.
  Status PfCount(const std::vector<std::string>& keys, int64_t* result);

  // Merge multiple HyperLogLog values into an unique value that will
  // approximate the cardinality of the union of the observed Sets of the source
  // HyperLogLog structures.
  Status PfMerge(const std::vector<std::string>& keys, std::string& value_to_dest);

  // Admin Commands
  Status StartBGThread();
  Status RunBGTask();
  Status AddBGTask(const BGTask& bg_task);

  Status Compact(const DataType& type, bool sync = false);
  Status CompactRange(const DataType& type, const std::string& start, const std::string& end, bool sync = false);
  Status DoCompact(const DataType& type);
  Status DoCompactRange(const DataType& type, const std::string& start, const std::string& end);

  Status SetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys);
  Status SetSmallCompactionThreshold(uint32_t small_compaction_threshold);
  Status SetSmallCompactionDurationThreshold(uint32_t small_compaction_duration_threshold);

  std::string GetCurrentTaskType();
  Status GetUsage(const std::string& property, uint64_t* result);
  Status GetUsage(const std::string& property, std::map<std::string, uint64_t>* type_result);
  uint64_t GetProperty(const std::string& db_type, const std::string& property);

  Status GetKeyNum(std::vector<KeyInfo>* key_infos);
  Status StopScanKeyNum();

  rocksdb::DB* GetDBByType(const std::string& type);

  Status SetOptions(const OptionType& option_type, const std::string& db_type,
                    const std::unordered_map<std::string, std::string>& options);
  void SetCompactRangeOptions(const bool is_canceled);
  Status EnableDymayticOptions(const OptionType& option_type, 
                    const std::string& db_type, const std::unordered_map<std::string, std::string>& options);
  Status EnableAutoCompaction(const OptionType& option_type, 
                    const std::string& db_type, const std::unordered_map<std::string, std::string>& options);
  void GetRocksDBInfo(std::string& info);

 private:
  std::unique_ptr<RedisStrings> strings_db_;
  std::unique_ptr<RedisHashes> hashes_db_;
  std::unique_ptr<RedisSets> sets_db_;
  std::unique_ptr<RedisZSets> zsets_db_;
  std::unique_ptr<RedisLists> lists_db_;
  std::atomic<bool> is_opened_ = false;

  std::unique_ptr<LRUCache<std::string, std::string>> cursors_store_;

  // Storage start the background thread for compaction task
  pthread_t bg_tasks_thread_id_ = 0;
  pstd::Mutex bg_tasks_mutex_;
  pstd::CondVar bg_tasks_cond_var_;
  std::queue<BGTask> bg_tasks_queue_;

  std::atomic<int> current_task_type_ = kNone;
  std::atomic<bool> bg_tasks_should_exit_ = false;

  // For scan keys in data base
  std::atomic<bool> scan_keynum_exit_ = false;
};

}  //  namespace storage
#endif  //  INCLUDE_STORAGE_STORAGE_H_
