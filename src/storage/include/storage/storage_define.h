//  Copyright (c) 2023-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_DEFINE_H_
#define STORAGE_DEFINE_H_

#include <algorithm>
#include <iostream>
#include "stdint.h"

#include "rocksdb/slice.h"

namespace storage {
using Slice = rocksdb::Slice;

// remove 'unused parameter' warning
#define UNUSED(expr) \
  do {               \
    (void)(expr);    \
  } while (0)

const int kPrefixReserveLength = 8;
const int kVersionLength = 8;
const int kScoreLength = 8;
const int kSuffixReserveLength = 16;
const int kListValueIndexLength = 16;
/*
 * Used to store a fixed-size value for the Type field
 */
const int kTypeLength = 1;
const int kTimestampLength = 8;

/*
 * kMetaCF is used to store the metadata of all types of
 * data and all information of type string
 */
enum ColumnFamilyIndex {
  kMetaCF = 0,
  kHashesDataCF = 1,
  kSetsDataCF = 2,
  kListsDataCF = 3,
  kZsetsDataCF = 4,
  kZsetsScoreCF = 5,
  kStreamsDataCF = 6,
};

const static char kNeedTransformCharacter = '\u0000';
const static char* kEncodedTransformCharacter = "\u0000\u0001";
const static char* kEncodedKeyDelim = "\u0000\u0000";
const static int kEncodedKeyDelimSize = 2;

inline char* EncodeUserKey(const Slice& user_key, char* dst_ptr, size_t nzero) {
  // no \u0000 exists in user_key, memcopy user_key directly.
  if (nzero == 0) {
    memcpy(dst_ptr, user_key.data(), user_key.size());
    dst_ptr += user_key.size();
    memcpy(dst_ptr, kEncodedKeyDelim, 2);
    dst_ptr += 2;
    return dst_ptr;
  }

  // \u0000 exists in user_key, iterate and replace.
  size_t pos = 0;
  const char* user_data = user_key.data();
  for (size_t i = 0; i < user_key.size(); i++) {
    if (user_data[i] == kNeedTransformCharacter) {
      size_t sub_len = i - pos;
      if (sub_len != 0) {
        memcpy(dst_ptr, user_data + pos, sub_len);
        dst_ptr += sub_len;
      }
      memcpy(dst_ptr, kEncodedTransformCharacter, 2);
      dst_ptr += 2;
      pos = i + 1;
    }
  }
  if (pos != user_key.size()) {
    memcpy(dst_ptr, user_data + pos, user_key.size() - pos);
  }

  memcpy(dst_ptr, kEncodedKeyDelim, 2);
  dst_ptr += 2;
  return dst_ptr;
}

inline const char* DecodeUserKey(const char* ptr, int length, std::string* user_key) {
  const char* ret_ptr = ptr;
  user_key->resize(length - kEncodedKeyDelimSize);
  bool zero_ahead = false;
  bool delim_found = false;
  int output_idx = 0;

  for (int idx = 0; idx < length; idx++) {
    switch (ptr[idx]) {
      case '\u0000': {
        delim_found = zero_ahead ? true : false;
        zero_ahead = true;
        break;
      }
      case '\u0001': {
        (*user_key)[output_idx++] = zero_ahead ? '\u0000' : ptr[idx];
        zero_ahead = false;
        break;
      }
      default: {
        (*user_key)[output_idx++] = ptr[idx];
        zero_ahead = false;
        break;
      }
    }
    if (delim_found) {
      user_key->resize(output_idx);
      ret_ptr = ptr + idx + 1;
      break;
    }
  }
  return ret_ptr;
}

inline const char* SeekUserkeyDelim(const char* ptr, int length) {
    bool zero_ahead = false;
    for (int i = 0; i < length; i++) {
        if (ptr[i] == kNeedTransformCharacter && zero_ahead) {
            return ptr + i + 1;
        }
        zero_ahead = ptr[i] == kNeedTransformCharacter;
    }
    //TODO: handle invalid format
    return ptr;
}

} // end namespace storage
#endif
