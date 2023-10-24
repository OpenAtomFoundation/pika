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

#include "src/debug.h"

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

const int kTimestampLength = 8;

enum ColumnFamilyIndex {
    kStringsCF = 0,
    kHashesMetaCF = 1,
    kHashesDataCF = 2,
    kSetsMetaCF = 3,
    kSetsDataCF = 4,
    kListsMetaCF = 5,
    kListsDataCF = 6,
    kZsetsMetaCF = 7,
    kZsetsDataCF = 8,
    kZsetsScoreCF = 9,
};

const static char kNeedTransformCharacter = '\u0000';
const static char* kEncodedTransformCharacter = "\u0000\u0001";
const static char* kEncodedKeyDelim = "\u0000\u0000";
const static int kEncodedKeyDelimSize = 2;

inline char* EncodeUserKey(const Slice& user_key, char* dst_ptr) {
  char* start = dst_ptr;
  std::for_each(user_key.data(), user_key.data() + user_key.size(),
      [&dst_ptr](auto & ch){
        if (ch == kNeedTransformCharacter) {
          memcpy(dst_ptr, kEncodedTransformCharacter, 2);
          dst_ptr += 2;
        } else {
          *dst_ptr = ch;
          dst_ptr++;
        }
      });
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
