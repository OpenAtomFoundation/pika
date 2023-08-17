/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "dump_interface.h"
#include "store.h"

namespace leveldb {
class DB;
}

namespace pikiwidb {

class UnboundedBuffer;

class PLeveldb : public PDumpInterface {
 public:
  PLeveldb();
  ~PLeveldb();

  bool Open(const char* path);
  bool IsOpen() const;

  PObject Get(const PString& key) override;
  bool Put(const PString& key) override;

  bool Put(const PString& key, const PObject& obj, int64_t ttl = 0) override;
  bool Delete(const PString& key) override;

 private:
  leveldb::DB* db_ = nullptr;

  // encoding stuff

  // value format: type + ttl(if has) + qobject
  void encodeObject(const PObject& obj, int64_t absttl, UnboundedBuffer& v);

  void encodeString(const PString& str, UnboundedBuffer& v);
  void encodeHash(const PHASH&, UnboundedBuffer& v);
  void encodeList(const PLIST&, UnboundedBuffer& v);
  void encodeSet(const PSET&, UnboundedBuffer& v);
  void encodeZSet(const PZSET&, UnboundedBuffer& v);

  // decoding stuff. the unit of @remainTtlSeconds is second.
  PObject decodeObject(const char* data, size_t len, int64_t& remainTtlSeconds);

  PString decodeString(const char* data, size_t len);
  PObject decodeHash(const char* data, size_t len);
  PObject decodeList(const char* data, size_t len);
  PObject decodeSet(const char* data, size_t len);
  PObject decodeZSet(const char* data, size_t len);
};

}  // namespace pikiwidb

