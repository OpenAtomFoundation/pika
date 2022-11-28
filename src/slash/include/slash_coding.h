// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Endian-neutral encoding:
// * Fixed-length numbers are encoded with least-significant byte first
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format

#ifndef SLASH_CODING_H_
#define SLASH_CODING_H_

#include <stdint.h>
#include <string.h>
#include <string>

#include "slash/include/slash_slice.h"

namespace slash {

// Standard Put... routines append to a string
extern void PutFixed16(std::string* dst, uint16_t value);
extern void PutFixed32(std::string* dst, uint32_t value);
extern void PutFixed64(std::string* dst, uint64_t value);
extern void PutVarint32(std::string* dst, uint32_t value);
extern void PutVarint64(std::string* dst, uint64_t value);
extern void PutLengthPrefixedString(std::string* dst, const std::string& value);

extern void GetFixed16(std::string* dst, uint16_t* value);
extern void GetFixed32(std::string* dst, uint32_t* value);
extern void GetFixed64(std::string* dst, uint64_t* value);
extern bool GetVarint32(std::string* input, uint32_t* value);
extern bool GetVarint64(std::string* input, uint64_t* value);
extern const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                          Slice* result);
extern bool GetLengthPrefixedSlice(Slice* input, Slice* result);
extern bool GetLengthPrefixedString(std::string* input, std::string* result);

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// NULL on error.  These routines only look at bytes in the range
// [p..limit-1]
extern const char* GetVarint32Ptr(const char* p,const char* limit, uint32_t* v);
extern const char* GetVarint64Ptr(const char* p,const char* limit, uint64_t* v);

// Returns the length of the varint32 or varint64 encoding of "v"
extern int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// REQUIRES: dst has enough space for the value being written
extern void EncodeFixed16(char* dst, uint16_t value);
extern void EncodeFixed32(char* dst, uint32_t value);
extern void EncodeFixed64(char* dst, uint64_t value);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern char* EncodeVarint32(char* dst, uint32_t value);
extern char* EncodeVarint64(char* dst, uint64_t value);

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

inline uint16_t DecodeFixed16(const char* ptr) {
    // Load the raw bytes
    uint16_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
}

inline uint32_t DecodeFixed32(const char* ptr) {
    // Load the raw bytes
    uint32_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
}

inline uint64_t DecodeFixed64(const char* ptr) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
}

inline void GetFixed16(std::string* dst, uint16_t *value) {
  *value = DecodeFixed16(dst->data());
  dst->erase(0, sizeof(uint16_t));
}

inline void GetFixed32(std::string* dst, uint32_t *value) {
  *value = DecodeFixed32(dst->data());
  dst->erase(0, sizeof(uint32_t));
}

inline void GetFixed64(std::string* dst, uint64_t *value) {
  *value = DecodeFixed64(dst->data());
  dst->erase(0, sizeof(uint64_t));
}

// Internal routine for use by fallback path of GetVarint32Ptr
extern const char* GetVarint32PtrFallback(const char* p,
                                          const char* limit,
                                          uint32_t* value);
inline const char* GetVarint32Ptr(const char* p,
                                  const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

}  // namespace leveldb

#endif  // SLASH_CODING_H_
