/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "aof.h"

#include <unistd.h>
#include <sstream>

#include "config.h"
#include "log.h"
#include "proto_parser.h"
#include "store.h"

namespace pikiwidb {

const char* const g_aofTmp = "pikiwidb_appendonly.aof.tmp";

pid_t g_rewritePid = -1;

/*****************************************************
 * when after fork(), the parent stop aof thread, which means
 * the coming aof data will be write to the tmp buffer, not to
 * aof file.
 ****************************************************/
void PAOFThreadController::RewriteDoneHandler(int exitRet, int whatSignal) {
  g_rewritePid = -1;

  if (exitRet == 0 && whatSignal == 0) {
    INFO("save aof success");
    ::rename(g_aofTmp, g_config.appendfilename.c_str());
  } else {
    ERROR("save aof failed with exit result {}, signal {}", exitRet, whatSignal);
    ::unlink(g_aofTmp);
  }

  PAOFThreadController::Instance().Start();
}

PAOFThreadController& PAOFThreadController::Instance() {
  static PAOFThreadController threadCtrl;
  return threadCtrl;
}

PAOFThreadController::~PAOFThreadController() { this->Stop(); }

bool PAOFThreadController::ProcessTmpBuffer(BufferSequence& bf) {
  aofBuffer_.ProcessBuffer(bf);
  return bf.count > 0;
}

void PAOFThreadController::SkipTmpBuffer(size_t n) { aofBuffer_.Skip(n); }

// main thread  call this
void PAOFThreadController::Start() {
  DEBUG("start aof thread");

  assert(!aofThread_ || !aofThread_->IsAlive());

  aofThread_ = std::make_shared<AOFThread>();
  aofThread_->SetAlive();

  aofThread_->thread_ = std::thread{&AOFThread::Run, aofThread_};
}

// when fork(), parent call stop;
void PAOFThreadController::Stop() {
  if (!aofThread_) {
    return;
  }

  DEBUG("stop aof thread");
  aofThread_->Stop();
  aofThread_ = nullptr;
}

// main thread call this
void PAOFThreadController::writeSelectDB(int db, AsyncBuffer& dst) {
  if (db == lastDB_) {
    return;
  }

  lastDB_ = db;

  WriteMultiBulkLong(2, dst);
  WriteBulkString("select", 6, dst);
  WriteBulkLong(db, dst);
}

void PAOFThreadController::SaveCommand(const std::vector<PString>& params, int db) {
  AsyncBuffer* dst;

  if (aofThread_ && aofThread_->IsAlive()) {
    dst = &aofThread_->buf_;
  } else {
    dst = &aofBuffer_;
  }

  writeSelectDB(db, *dst);
  pikiwidb::SaveCommand(params, *dst);
}

PAOFThreadController::AOFThread::~AOFThread() { file_.Close(); }

void PAOFThreadController::AOFThread::Stop() {
  alive_ = false;
  if (thread_.joinable()) {
    thread_.join();
  }
}

bool PAOFThreadController::AOFThread::Flush() {
  BufferSequence data;
  buf_.ProcessBuffer(data);
  if (data.count == 0) {
    return false;
  }

  if (!file_.IsOpen()) {
    file_.Open(g_config.appendfilename.c_str());
  }

  for (size_t i = 0; i < data.count; ++i) {
    file_.Write(data.buffers[i].iov_base, data.buffers[i].iov_len);
  }

  buf_.Skip(data.TotalBytes());

  return data.count != 0;
}

void PAOFThreadController::AOFThread::SaveCommand(const std::vector<PString>& params) {
  pikiwidb::SaveCommand(params, buf_);
}

void PAOFThreadController::AOFThread::Run() {
  assert(IsAlive());

  // CHECK aof temp buffer first!
  BufferSequence data;
  while (PAOFThreadController::Instance().ProcessTmpBuffer(data)) {
    if (!file_.IsOpen()) {
      file_.Open(g_config.appendfilename.c_str());
    }

    for (size_t i = 0; i < data.count; ++i) {
      file_.Write(data.buffers[i].iov_base, data.buffers[i].iov_len);
    }

    PAOFThreadController::Instance().SkipTmpBuffer(data.TotalBytes());
  }

  while (IsAlive()) {
    // sync incrementally, always, the redis sync policy is useless
    if (Flush()) {
      file_.Sync();
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  file_.Close();
}

static void SaveExpire(const PString& key, uint64_t absMs, OutputMemoryFile& file) {
  WriteBulkLong(3, file);
  WriteBulkString("expire", 6, file);
  WriteBulkString(key, file);
  WriteBulkLong(absMs, file);
}

// child  save the db to tmp file
static void SaveObject(const PString& key, const PObject& obj, OutputMemoryFile& file);
static void RewriteProcess() {
  OutputMemoryFile file;
  if (!file.Open(g_aofTmp, false)) {
    perror("open tmp failed");
    _exit(-1);
  }

  for (int dbno = 0; true; ++dbno) {
    if (PSTORE.SelectDB(dbno) == -1) {
      break;
    }

    if (PSTORE.DBSize() == 0) {
      continue;
    }

    // select db
    WriteMultiBulkLong(2, file);
    WriteBulkString("select", 6, file);
    WriteBulkLong(dbno, file);

    const auto now = ::Now();
    for (const auto& kv : PSTORE) {
      int64_t ttl = PSTORE.TTL(kv.first, now);
      if (ttl == PStore::ExpireResult::expired) {
        continue;
      }

      SaveObject(kv.first, kv.second, file);
      if (ttl > 0) {
        SaveExpire(kv.first, ttl + now, file);
      }
    }
  }
}

PError bgrewriteaof(const std::vector<PString>&, UnboundedBuffer* reply) {
  if (g_rewritePid != -1) {
    reply->PushData("-ERR aof already in progress\r\n", sizeof "-ERR aof already in progress\r\n" - 1);
    return PError_ok;
  } else {
    g_rewritePid = fork();
    switch (g_rewritePid) {
      case 0:
        RewriteProcess();
        _exit(0);

      case -1:
        ERROR("fork aof process failed, errno = {}", errno);
        reply->PushData("-ERR aof rewrite failed\r\n", sizeof "-ERR aof rewrite failed\r\n" - 1);
        return PError_ok;

      default:
        break;
    }
  }

  PAOFThreadController::Instance().Stop();
  FormatOK(reply);
  return PError_ok;
}

static void SaveStringObject(const PString& key, const PObject& obj, OutputMemoryFile& file) {
  WriteMultiBulkLong(3, file);
  WriteBulkString("set", 3, file);
  WriteBulkString(key, file);

  auto str = GetDecodedString(&obj);
  WriteBulkString(*str, file);
}

static void SaveListObject(const PString& key, const PObject& obj, OutputMemoryFile& file) {
  auto list = obj.CastList();
  if (list->empty()) {
    return;
  }

  WriteMultiBulkLong(list->size() + 2, file);  // rpush listname + elems
  WriteBulkString("rpush", 5, file);
  WriteBulkString(key, file);

  for (const auto& elem : *list) {
    WriteBulkString(elem, file);
  }
}

static void SaveSetObject(const PString& key, const PObject& obj, OutputMemoryFile& file) {
  auto set = obj.CastSet();
  if (set->empty()) {
    return;
  }

  WriteMultiBulkLong(set->size() + 2, file);  // sadd set_name + elems
  WriteBulkString("sadd", 4, file);
  WriteBulkString(key, file);

  for (const auto& elem : *set) {
    WriteBulkString(elem, file);
  }
}

static void SaveZSetObject(const PString& key, const PObject& obj, OutputMemoryFile& file) {
  auto zset = obj.CastSortedSet();
  if (zset->Size() == 0) {
    return;
  }

  WriteMultiBulkLong(2 * zset->Size() + 2, file);  // zadd zset_name + (score + member)
  WriteBulkString("zadd", 4, file);
  WriteBulkString(key, file);

  for (const auto& pair : *zset) {
    const PString& member = pair.first;
    double score = pair.second;

    char scoreStr[32];
    int len = Double2Str(scoreStr, sizeof scoreStr, score);

    WriteBulkString(scoreStr, len, file);
    WriteBulkString(member, file);
  }
}

static void SaveHashObject(const PString& key, const PObject& obj, OutputMemoryFile& file) {
  auto hash = obj.CastHash();
  if (hash->empty()) {
    return;
  }

  WriteMultiBulkLong(2 * hash->size() + 2, file);  // hmset hash_name + (key + value)
  WriteBulkString("hmset", 5, file);
  WriteBulkString(key, file);

  for (const auto& pair : *hash) {
    WriteBulkString(pair.first, file);
    WriteBulkString(pair.second, file);
  }
}

static void SaveObject(const PString& key, const PObject& obj, OutputMemoryFile& file) {
  switch (obj.type) {
    case PType_string:
      SaveStringObject(key, obj, file);
      break;

    case PType_list:
      SaveListObject(key, obj, file);
      break;

    case PType_set:
      SaveSetObject(key, obj, file);
      break;

    case PType_sortedSet:
      SaveZSetObject(key, obj, file);
      break;

    case PType_hash:
      SaveHashObject(key, obj, file);
      break;

    default:
      assert(false);
      break;
  }
}

PAOFLoader::PAOFLoader() {}

bool PAOFLoader::Load(const char* name) {
  if (::access(name, F_OK) != 0) {
    return false;
  }

  {
    // truncate tail trash zeroes
    OutputMemoryFile file;
    file.Open(name);
    file.TruncateTailZero();
  }

  // load file to memory
  InputMemoryFile file;
  if (!file.Open(name)) {
    return false;
  }

  size_t maxLen = std::numeric_limits<size_t>::max();
  const char* content = file.Read(maxLen);

  if (maxLen == 0) {
    return false;
  }

  PProtoParser parser;
  // extract commands from file content
  const char* const end = content + maxLen;
  while (content < end) {
    parser.Reset();
    if (PParseResult::ok != parser.ParseRequest(content, end)) {
      ERROR("Load aof failed");
      return false;
    }

    cmds_.push_back(parser.GetParams());
  }

  return true;
}

}  // namespace pikiwidb
