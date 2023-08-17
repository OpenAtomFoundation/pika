/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "common.h"
#include "dump_interface.h"
#include "hash.h"
#include "list.h"
#include "set.h"
#include "sorted_set.h"

#include <map>
#include <memory>
#include <vector>

namespace pikiwidb {

using PSTRING = PString*;
using PLIST = PList*;
using PSET = PSet*;
using PZSET = PSortedSet*;
using PHASH = PHash*;

using PTimeType = uint64_t;

// ref: https://github.com/redis/redis/blob/7c179f9bf4390512196b3a2b2ad6d0f4cb625c8a/src/server.h#L892C1-L892C1
static const int kLRUBits = 24;
// ref: https://github.com/redis/redis/blob/7c179f9bf4390512196b3a2b2ad6d0f4cb625c8a/src/server.h#L893
static const uint32_t kMaxLRUValue = (1 << kLRUBits) - 1;

uint32_t EstimateIdleTime(uint32_t lru);
// ref: https://github.com/redis/redis/blob/7c179f9bf4390512196b3a2b2ad6d0f4cb625c8a/src/server.h#L899
struct PObject {
 public:
  static uint32_t lruclock;

  unsigned int type : 4;
  unsigned int encoding : 4;
  unsigned int lru : kLRUBits; /* LRU time (relative to global lru_clock) or
                                * LFU data (least significant 8 bits frequency
                                * and most significant 16 bits access time). */

  void* value = nullptr;

  explicit PObject(PType = PType_invalid);
  ~PObject();

  PObject(const PObject& obj) = delete;
  PObject& operator=(const PObject& obj) = delete;

  PObject(PObject&& obj);
  PObject& operator=(PObject&& obj);

  void Clear();
  void Reset(void* newvalue = nullptr);

  static PObject CreateString(const PString& value);
  static PObject CreateString(long value);
  static PObject CreateList();
  static PObject CreateSet();
  static PObject CreateZSet();
  static PObject CreateHash();

  PSTRING CastString() const { return reinterpret_cast<PSTRING>(value); }
  PLIST CastList() const { return reinterpret_cast<PLIST>(value); }
  PSET CastSet() const { return reinterpret_cast<PSET>(value); }
  PZSET CastSortedSet() const { return reinterpret_cast<PZSET>(value); }
  PHASH CastHash() const { return reinterpret_cast<PHASH>(value); }

 private:
  void moveFrom(PObject&& obj);
  void freeValue();
};

class PClient;

using PDB = std::unordered_map<PString, PObject, my_hash, std::equal_to<PString> >;

const int kMaxDBNum = 65536;

class PStore {
 public:
  static PStore& Instance();

  PStore(const PStore&) = delete;
  void operator=(const PStore&) = delete;

  void Init(int dbNum = 16);

  int SelectDB(int dbno);
  int GetDB() const;

  // Key operation
  bool DeleteKey(const PString& key);
  bool ExistsKey(const PString& key) const;
  PType KeyType(const PString& key) const;
  PString RandomKey(PObject** val = nullptr) const;
  size_t DBSize() const { return dbs_[dbno_].size(); }
  size_t ScanKey(size_t cursor, size_t count, std::vector<PString>& res) const;

  // iterator
  PDB::const_iterator begin() const { return dbs_[dbno_].begin(); }
  PDB::const_iterator end() const { return dbs_[dbno_].end(); }
  PDB::iterator begin() { return dbs_[dbno_].begin(); }
  PDB::iterator end() { return dbs_[dbno_].end(); }

  const PObject* GetObject(const PString& key) const;
  PError GetValue(const PString& key, PObject*& value, bool touch = true);
  PError GetValueByType(const PString& key, PObject*& value, PType type = PType_invalid);
  // do not update lru time
  PError GetValueByTypeNoTouch(const PString& key, PObject*& value, PType type = PType_invalid);

  PObject* SetValue(const PString& key, PObject&& value);

  // for expire key
  enum ExpireResult : std::int8_t {
    notExpire = 0,
    persist = -1,
    expired = -2,
    notExist = -2,
  };
  void SetExpire(const PString& key, uint64_t when) const;
  int64_t TTL(const PString& key, uint64_t now);
  bool ClearExpire(const PString& key);
  int LoopCheckExpire(uint64_t now);
  void InitExpireTimer();

  // danger cmd
  void ClearCurrentDB() { dbs_[dbno_].clear(); }
  void ResetDB();

  // for blocked list
  bool BlockClient(const PString& key, PClient* client, uint64_t timeout, ListPosition pos, const PString* dstList = 0);
  size_t UnblockClient(PClient* client);
  size_t ServeClient(const PString& key, const PLIST& list);

  int LoopCheckBlocked(uint64_t now);
  void InitBlockedTimer();

  size_t BlockedSize() const;

  static int dirty_;

  // eviction timer for lru
  void InitEvictionTimer();
  // for backends
  void InitDumpBackends();
  void DumpToBackends(int dbno);
  void AddDirtyKey(const PString& key);
  void AddDirtyKey(const PString& key, const PObject* value);

 private:
  PStore() : dbno_(0) {}

  PError getValueByType(const PString& key, PObject*& value, PType type = PType_invalid, bool touch = true);

  ExpireResult expireIfNeed(const PString& key, uint64_t now);

  class ExpiredDB {
   public:
    void SetExpire(const PString& key, uint64_t when);
    int64_t TTL(const PString& key, uint64_t now);
    bool ClearExpire(const PString& key);
    ExpireResult ExpireIfNeed(const PString& key, uint64_t now);

    int LoopCheck(uint64_t now);

   private:
    using P_EXPIRE_DB = std::unordered_map<PString, uint64_t, my_hash, std::equal_to<PString> >;
    P_EXPIRE_DB expireKeys_;  // all the keys to be expired, unordered.
  };

  class BlockedClients {
   public:
    bool BlockClient(const PString& key, PClient* client, uint64_t timeout, ListPosition pos,
                     const PString* dstList = 0);
    size_t UnblockClient(PClient* client);
    size_t ServeClient(const PString& key, const PLIST& list);

    int LoopCheck(uint64_t now);
    size_t Size() const { return blockedClients_.size(); }

   private:
    using Clients = std::list<std::tuple<std::weak_ptr<PClient>, uint64_t, ListPosition> >;
    using WaitingList = std::unordered_map<PString, Clients>;

    WaitingList blockedClients_;
  };

  PError setValue(const PString& key, PObject& value, bool exclusive = false);

  // Because GetObject() must be const, so mutable them
  mutable std::vector<PDB> dbs_;
  mutable std::vector<ExpiredDB> expiredDBs_;
  std::vector<BlockedClients> blockedClients_;
  std::vector<std::unique_ptr<PDumpInterface> > backends_;

  using ToSyncDB = std::unordered_map<PString, const PObject*, my_hash, std::equal_to<PString> >;
  std::vector<ToSyncDB> waitSyncKeys_;
  int dbno_ = -1;
};

#define PSTORE PStore::Instance()

// ugly, but I don't want to write signalModifiedKey() every where
extern std::vector<PString> g_dirtyKeys;
extern void Propogate(const std::vector<PString>& params);
extern void Propogate(int dbno, const std::vector<PString>& params);

}  // namespace pikiwidb
