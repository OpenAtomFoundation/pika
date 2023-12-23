// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_stream_base.h"

// #include "pika_command.h"
#include "pika_stream_meta_value.h"
#include "pika_stream_types.h"
#include "storage/storage.h"

namespace storage {

#define GET_NEXT_TREE_ID_AND_CHECK(slot, tid)       \
  do {                                              \
    auto &tid_gen = TreeIDGenerator::GetInstance(); \
    auto s = tid_gen.GetNextTreeID(slot, tid);      \
    if (!s.ok()) {                                  \
      return s;                                     \
    }                                               \
  } while (0)

storage::Status TreeIDGenerator::GetNextTreeID(const rocksdb::DB *db, treeID &tid) {
  // assert(slot);
  // auto expected_id = kINVALID_TREE_ID;

  // // if tree_id_ is not initialized (equal to kINVALID_TREE_ID), try to fetch it from storage
  // if (tree_id_.compare_exchange_strong(expected_id, START_TREE_ID)) {
  //   std::string value;
  //   storage::Status s = db->HGet(STREAM_META_HASH_KEY, STREAM_LAST_GENERATED_TREE_ID_FIELD, &value);
  //   if (s.ok()) {
  //     treeID id;
  //     if (!StreamUtils::string2int32(value.c_str(), id)) {
  //       LOG(ERROR) << "Invalid tree id: " << value;
  //       return storage::Status::Corruption("Invalid tree id");
  //     }
  //     tree_id_.store(id);

  //   } else if (!s.IsNotFound()) {
  //     return s;
  //   }
  // }

  // tree_id_.fetch_add(1);
  // tid = tree_id_;

  // // insert tree id to storage
  // std::string tree_id_str = std::to_string(tree_id_);
  // int32_t res;
  // return db->HSet(STREAM_META_HASH_KEY, STREAM_LAST_GENERATED_TREE_ID_FIELD, tree_id_str, &res);
}

bool StreamUtils::StreamGenericParseID(const std::string &var, streamID &id, uint64_t missing_seq, bool strict,
                                       bool *seq_given) {
  char buf[128];
  if (var.size() > sizeof(buf) - 1) {
    return false;
  }

  memcpy(buf, var.data(), var.size());
  buf[var.size()] = '\0';

  if (strict && (buf[0] == '-' || buf[0] == '+') && buf[1] == '\0') {
    // res.SetRes(CmdRes::kInvalidParameter, "Invalid stream ID specified as stream ");
    return false;
  }

  if (seq_given != nullptr) {
    *seq_given = true;
  }

  if (buf[0] == '-' && buf[1] == '\0') {
    id.ms = 0;
    id.seq = 0;
    return true;
  } else if (buf[0] == '+' && buf[1] == '\0') {
    id.ms = UINT64_MAX;
    id.seq = UINT64_MAX;
    return true;
  }

  uint64_t ms;
  uint64_t seq;
  char *dot = strchr(buf, '-');
  if (dot) {
    *dot = '\0';
  }
  if (!StreamUtils::string2uint64(buf, ms)) {
    return false;
  };
  if (dot) {
    size_t seqlen = strlen(dot + 1);
    if (seq_given != nullptr && seqlen == 1 && *(dot + 1) == '*') {
      seq = 0;
      *seq_given = false;
    } else if (!StreamUtils::string2uint64(dot + 1, seq)) {
      return false;
    }
  } else {
    seq = missing_seq;
  }
  id.ms = ms;
  id.seq = seq;
  return true;
}

bool StreamUtils::StreamParseID(const std::string &var, streamID &id, uint64_t missing_seq) {
  return StreamGenericParseID(var, id, missing_seq, false, nullptr);
}

bool StreamUtils::StreamParseStrictID(const std::string &var, streamID &id, uint64_t missing_seq, bool *seq_given) {
  return StreamGenericParseID(var, id, missing_seq, true, seq_given);
}

bool StreamUtils::StreamParseIntervalId(const std::string &var, streamID &id, bool *exclude, uint64_t missing_seq) {
  if (exclude != nullptr) {
    *exclude = (var.size() > 1 && var[0] == '(');
  }
  if (exclude != nullptr && *exclude) {
    return StreamParseStrictID(var.substr(1), id, missing_seq, nullptr);
  } else {
    return StreamParseID(var, id, missing_seq);
  }
}

bool StreamUtils::string2uint64(const char *s, uint64_t &value) {
  if (!s || !*s) {
    return false;
  }

  char *end;
  errno = 0;
  uint64_t tmp = strtoull(s, &end, 10);
  if (*end || errno == ERANGE) {
    // Conversion either didn't consume the entire string, or overflow occurred
    return false;
  }

  value = tmp;
  return true;
}

bool StreamUtils::string2int64(const char *s, int64_t &value) {
  if (!s || !*s) {
    return false;
  }

  char *end;
  errno = 0;
  int64_t tmp = std::strtoll(s, &end, 10);
  if (*end || errno == ERANGE) {
    // Conversion either didn't consume the entire string, or overflow occurred
    return false;
  }

  value = tmp;
  return true;
}

bool StreamUtils::string2int32(const char *s, int32_t &value) {
  if (!s || !*s) {
    return false;
  }

  char *end;
  errno = 0;
  long tmp = strtol(s, &end, 10);
  if (*end || errno == ERANGE || tmp < INT_MIN || tmp > INT_MAX) {
    // Conversion either didn't consume the entire string,
    // or overflow or underflow occurred
    return false;
  }

  value = static_cast<int32_t>(tmp);
  return true;
}

std::string StreamUtils::TreeID2Key(const treeID &tid) {
  std::string key;
  key.reserve(STERAM_TREE_PREFIX.size() + sizeof(tid));
  key.append(STERAM_TREE_PREFIX);
  key.append(reinterpret_cast<const char *>(&tid), sizeof(tid));
  return key;
}

bool StreamUtils::SerializeMessage(const std::vector<std::string> &field_values, std::string &message, int field_pos) {
  assert(field_values.size() - field_pos >= 2 && (field_values.size() - field_pos) % 2 == 0);
  assert(message.empty());
  // count the size of serizlized message
  size_t size = 0;
  for (int i = field_pos; i < field_values.size(); i++) {
    size += field_values[i].size() + sizeof(size_t);
  }
  message.reserve(size);

  // serialize message
  for (int i = field_pos; i < field_values.size(); i++) {
    size_t len = field_values[i].size();
    message.append(reinterpret_cast<const char *>(&len), sizeof(len));
    message.append(field_values[i]);
  }

  return true;
}

bool StreamUtils::DeserializeMessage(const std::string &message, std::vector<std::string> &parsed_message) {
  size_t pos = 0;
  while (pos < message.size()) {
    // Read the length of the next field value from the message
    size_t len = *reinterpret_cast<const size_t *>(&message[pos]);
    pos += sizeof(size_t);

    // Check if the calculated end of the string is still within the message bounds
    if (pos + len > message.size()) {
      LOG(ERROR) << "Invalid message format, failed to parse message";
      return false;  // Error: not enough data in the message string
    }

    // Extract the field value and add it to the vector
    parsed_message.push_back(message.substr(pos, len));
    pos += len;
  }

  return true;
}

uint64_t StreamUtils::GetCurrentTimeMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

}  // namespace storage