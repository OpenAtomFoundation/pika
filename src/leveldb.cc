/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */


#include "leveldb.h"
#include "leveldb/db.h"
#include "log.h"
#include "unbounded_buffer.h"

namespace pikiwidb {

PLeveldb::PLeveldb() : db_(nullptr) {}

PLeveldb::~PLeveldb() { delete db_; }

bool PLeveldb::IsOpen() const { return db_ != nullptr; }

bool PLeveldb::Open(const char* path) {
  leveldb::Options options;
  options.create_if_missing = true;
  // options.error_if_exists = true;

  auto s = leveldb::DB::Open(options, path, &db_);
  if (!s.ok()) {
    ERROR("Open db_ failed:{}", s.ToString());
  }

  return s.ok();
}

PObject PLeveldb::Get(const PString& key) {
  std::string value;
  auto status = db_->Get(leveldb::ReadOptions(), leveldb::Slice(key.data(), key.size()), &value);
  if (!status.ok()) {
    return PObject(PType_invalid);
  }

  int64_t remainTtlSeconds = 0;
  PObject obj = decodeObject(value.data(), value.size(), remainTtlSeconds);
  // trick: use obj.lru to store the remain seconds to be expired.
  if (remainTtlSeconds > 0) {
    obj.lru = static_cast<uint32_t>(remainTtlSeconds);
  } else {
    obj.lru = 0;
  }

  return obj;
}

bool PLeveldb::Put(const PString& key) {
  PObject* obj;
  PError ok = PSTORE.GetValue(key, obj, false);
  if (ok != PError_ok) {
    return false;
  }

  uint64_t now = ::Now();
  int64_t ttl = PSTORE.TTL(key, now);
  if (ttl > 0) {
    ttl += now;
  } else if (ttl == PStore::ExpireResult::expired) {
    return false;
  }

  return Put(key, *obj, ttl);
}

bool PLeveldb::Put(const PString& key, const PObject& obj, int64_t absttl) {
  UnboundedBuffer v;
  encodeObject(obj, absttl, v);

  leveldb::Slice lkey(key.data(), key.size());
  leveldb::Slice lval(v.ReadAddr(), v.ReadableSize());

  auto s = db_->Put(leveldb::WriteOptions(), lkey, lval);
  return s.ok();
}

bool PLeveldb::Delete(const PString& key) {
  leveldb::Slice lkey(key.data(), key.size());
  auto s = db_->Delete(leveldb::WriteOptions(), lkey);
  return s.ok();
}

void PLeveldb::encodeObject(const PObject& obj, int64_t absttl, UnboundedBuffer& v) {
  // value format: | ttl flag 1byte| ttl 8bytes if has|type 1byte| object contents

  // write ttl, if has
  int8_t ttlflag = (absttl > 0 ? 1 : 0);
  v.Write(&ttlflag, sizeof ttlflag);
  if (ttlflag) {
    v.Write(&absttl, sizeof absttl);
  }

  // write type
  int8_t type = obj.type;
  v.Write(&type, sizeof type);

  switch (obj.encoding) {
    case PEncode_raw:
    case PEncode_int: {
      auto str = GetDecodedString(&obj);
      encodeString(*str, v);
    } break;

    case PEncode_list:
      encodeList(obj.CastList(), v);
      break;

    case PEncode_set:
      encodeSet(obj.CastSet(), v);
      break;

    case PEncode_hash:
      encodeHash(obj.CastHash(), v);
      break;

    case PEncode_zset:
      encodeZSet(obj.CastSortedSet(), v);
      break;

    default:
      break;
  }
}

void PLeveldb::encodeString(const PString& str, UnboundedBuffer& v) {
  // write size
  auto len = static_cast<uint32_t>(str.size());
  v.Write(&len, 4);
  // write content
  v.Write(str.data(), len);
}

void PLeveldb::encodeHash(const PHASH& h, UnboundedBuffer& v) {
  // write size
  auto len = static_cast<uint32_t>(h->size());
  v.Write(&len, 4);

  for (const auto& e : *h) {
    encodeString(e.first, v);
    encodeString(e.second, v);
  }
}

void PLeveldb::encodeList(const PLIST& l, UnboundedBuffer& v) {
  // write size
  auto len = static_cast<uint32_t>(l->size());
  v.Write(&len, 4);

  for (const auto& e : *l) {
    encodeString(e, v);
  }
}

void PLeveldb::encodeSet(const PSET& s, UnboundedBuffer& v) {
  auto len = static_cast<uint32_t>(s->size());
  v.Write(&len, 4);

  for (const auto& e : *s) {
    encodeString(e, v);
  }
}

void PLeveldb::encodeZSet(const PZSET& ss, UnboundedBuffer& v) {
  auto len = static_cast<uint32_t>(ss->Size());
  v.Write(&len, 4);

  for (const auto& e : *ss) {
    encodeString(e.first, v);

    auto s(std::to_string(e.second));
    encodeString(s, v);
  }
}

PObject PLeveldb::decodeObject(const char* data, size_t len, int64_t& remainTtl) {
  // | type 1byte | ttl flag 1byte| ttl 8bytes, if has|

  remainTtl = 0;

  size_t offset = 0;

  int8_t hasttl = *(int8_t*)(data + offset);
  offset += sizeof hasttl;

  int64_t absttl = 0;
  if (hasttl) {
    absttl = *(int64_t*)(data + offset);
    offset += sizeof absttl;
  }

  if (absttl != 0) {
    int64_t now = static_cast<int64_t>(::Now());
    if (absttl <= now) {
      DEBUG("Load from leveldb is timeout {}", absttl);
      return PObject(PType_invalid);
    } else {
      // Only support seconds, because lru is 24bits, too short.
      remainTtl = (absttl - now) / 1000;
      INFO("Load from leveldb remainTtlSeconds: {}", remainTtl);
    }
  }

  int8_t type = *(int8_t*)(data + offset);
  offset += sizeof type;

  switch (type) {
    case PType_string: {
      return PObject::CreateString(decodeString(data + offset, len - offset));
    }
    case PType_list: {
      return decodeList(data + offset, len - offset);
    }
    case PType_set: {
      return decodeSet(data + offset, len - offset);
    }
    case PType_sortedSet: {
      return decodeZSet(data + offset, len - offset);
    }
    case PType_hash: {
      return decodeHash(data + offset, len - offset);
    }

    default:
      break;
  }

  assert(false);
  return PObject(PType_invalid);
}

PString PLeveldb::decodeString(const char* data, size_t len) {
  assert(len > 4);
  // read length
  uint32_t slen = *(uint32_t*)(data);
  // read content
  const char* sdata = data + 4;

  return PString(sdata, slen);
}

PObject PLeveldb::decodeHash(const char* data, size_t len) {
  assert(len >= 4);
  uint32_t hlen = *(uint32_t*)(data);

  PObject obj(PObject::CreateHash());
  PHASH hash(obj.CastHash());

  size_t offset = 4;
  for (uint32_t i = 0; i < hlen; ++i) {
    auto key = decodeString(data + offset, len - offset);
    offset += key.size() + 4;

    auto value = decodeString(data + offset, len - offset);
    offset += value.size() + 4;

    hash->insert(PHash::value_type(key, value));
    DEBUG("Load from leveldb: hash key : {} val : {} ", key, value);
  }

  return obj;
}

PObject PLeveldb::decodeList(const char* data, size_t len) {
  assert(len >= 4);
  uint32_t llen = *(uint32_t*)(data);

  PObject obj(PObject::CreateList());
  PLIST list(obj.CastList());

  size_t offset = 4;
  for (uint32_t i = 0; i < llen; ++i) {
    auto elem = decodeString(data + offset, len - offset);
    offset += elem.size() + 4;

    list->push_back(elem);
    DEBUG("Load list elem from leveldb: {}", elem);
  }

  return obj;
}

PObject PLeveldb::decodeSet(const char* data, size_t len) {
  assert(len >= 4);
  uint32_t slen = *(uint32_t*)(data);

  PObject obj(PObject::CreateSet());
  PSET set(obj.CastSet());

  size_t offset = 4;
  for (uint32_t i = 0; i < slen; ++i) {
    auto elem = decodeString(data + offset, len - offset);
    offset += elem.size() + 4;

    set->insert(elem);
    DEBUG("Load set elem from leveldb: {}", elem);
  }

  return obj;
}

PObject PLeveldb::decodeZSet(const char* data, size_t len) {
  assert(len >= 4);
  uint32_t sslen = *(uint32_t*)(data);

  PObject obj(PObject::CreateZSet());
  PZSET zset(obj.CastSortedSet());

  size_t offset = 4;
  for (uint32_t i = 0; i < sslen; ++i) {
    auto member = decodeString(data + offset, len - offset);
    offset += member.size() + 4;

    auto scoreStr = decodeString(data + offset, len - offset);
    offset += scoreStr.size() + 4;

    double score = std::stod(scoreStr);
    zset->AddMember(member, static_cast<long>(score));

    DEBUG("Load leveldb zset member : {}, score : {}", member, score);
  }

  return obj;
}

}  // namespace pikiwidb
