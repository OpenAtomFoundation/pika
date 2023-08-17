/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <list>
#include <memory>
#include <vector>

#include "memory_file.h"
#include "net/util.h"
#include "pstring.h"
#include "unbounded_buffer.h"

namespace pikiwidb {

// master side
enum PSlaveState {
  PSlaveState_none,
  PSlaveState_wait_bgsave_start,  // 有非sync的bgsave进行 要等待
  PSlaveState_wait_bgsave_end,    // sync bgsave正在进行
  // PSlaveState_send_rdb, // 这个slave在接受rdb文件
  PSlaveState_online,
};

struct PSlaveInfo {
  PSlaveState state;
  unsigned short listenPort;  // slave listening port

  PSlaveInfo() : state(PSlaveState_none), listenPort(0) {}
};

// slave side
enum PReplState {
  PReplState_none,
  PReplState_connecting,
  PReplState_connected,
  PReplState_wait_auth,      // wait auth to be confirmed
  PReplState_wait_replconf,  // wait replconf to be confirmed
  PReplState_wait_rdb,       // wait to recv rdb file
  PReplState_online,
};

struct PMasterInfo {
  SocketAddr addr;
  PReplState state;
  time_t downSince;

  // For recv rdb
  std::size_t rdbSize;
  std::size_t rdbRecved;

  PMasterInfo() {
    state = PReplState_none;
    downSince = 0;
    rdbSize = std::size_t(-1);
    rdbRecved = 0;
  }
};

// tmp filename
const char* const slaveRdbFile = "slave.rdb";

class PClient;

class PReplication {
 public:
  static PReplication& Instance();

  PReplication(const PReplication&) = delete;
  void operator=(const PReplication&) = delete;

  void Cron();

  // master side
  bool IsBgsaving() const;
  bool HasAnyWaitingBgsave() const;
  void AddSlave(PClient* cli);
  void TryBgsave();
  bool StartBgsave();
  void OnStartBgsave();
  void OnRdbSaveDone();
  void SendToSlaves(const std::vector<PString>& params);

  // slave side
  void SaveTmpRdb(const char* data, std::size_t& len);
  void SetMaster(const std::shared_ptr<PClient>& cli);
  void SetMasterState(PReplState s);
  void SetMasterAddr(const char* ip, unsigned short port);
  void SetRdbSize(std::size_t s);
  PReplState GetMasterState() const;
  SocketAddr GetMasterAddr() const;
  std::size_t GetRdbSize() const;

  // info command
  void OnInfoCommand(UnboundedBuffer& res);

 private:
  PReplication();
  void onStartBgsave(bool succ);

  // master side
  bool bgsaving_;
  UnboundedBuffer buffer_;
  std::list<std::weak_ptr<PClient> > slaves_;

  // slave side
  PMasterInfo masterInfo_;
  std::weak_ptr<PClient> master_;
  OutputMemoryFile rdb_;
};

}  // namespace pikiwidb

#define PREPL pikiwidb::PReplication::Instance()
