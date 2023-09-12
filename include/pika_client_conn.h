// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include <bitset>
#include <utility>

#include "include/pika_command.h"

class PikaClientConn : public net::RedisConn {
 public:
  using WriteCompleteCallback = std::function<void()>;

  struct BgTaskArg {
    std::shared_ptr<Cmd> cmd_ptr;
    std::shared_ptr<PikaClientConn> conn_ptr;
    std::vector<net::RedisCmdArgsType> redis_cmds;
    std::shared_ptr<std::string> resp_ptr;
    LogOffset offset;
    std::string db_name;
    uint32_t slot_id;
  };

  struct TxnStateBitMask {
   public:
    static constexpr uint8_t Start = 0;
    static constexpr uint8_t InitCmdFailed = 1;
    static constexpr uint8_t WatchFailed = 2;
    static constexpr uint8_t Execing = 3;
  };

  // Auth related
  class AuthStat {
   public:
    void Init();
    bool IsAuthed(const std::shared_ptr<Cmd>& cmd_ptr);
    bool ChecknUpdate(const std::string& message);

   private:
    enum StatType {
      kNoAuthed = 0,
      kAdminAuthed,
      kLimitAuthed,
    };
    StatType stat_;
  };

  PikaClientConn(int fd, const std::string& ip_port, net::Thread* server_thread, net::NetMultiplexer* mpx,
                 const net::HandleType& handle_type, int max_conn_rbuf_size);
  ~PikaClientConn() = default;

  void ProcessRedisCmds(const std::vector<net::RedisCmdArgsType>& argvs, bool async,
                                std::string* response) override;

  void BatchExecRedisCmd(const std::vector<net::RedisCmdArgsType>& argvs);
  int DealMessage(const net::RedisCmdArgsType& argv, std::string* response) override { return 0; }
  static void DoBackgroundTask(void* arg);
  static void DoExecTask(void* arg);

  bool IsPubSub() { return is_pubsub_; }
  void SetIsPubSub(bool is_pubsub) { is_pubsub_ = is_pubsub; }
  void SetCurrentDb(const std::string& db_name) { current_db_ = db_name; }
  const std::string& GetCurrentTable() override { return current_db_; }
  void SetWriteCompleteCallback(WriteCompleteCallback cb) { write_completed_cb_ = std::move(cb); }

  // Txn
  void PushCmdToQue(std::shared_ptr<Cmd> cmd);
  std::queue<std::shared_ptr<Cmd>> GetTxnCmdQue();
  void ClearTxnCmdQue();
  bool IsInTxn();
  bool IsTxnFailed();
  bool IsTxnInitFailed();
  bool IsTxnWatchFailed();
  bool IsTxnExecing(void);
  void SetTxnWatchFailState(bool is_failed);
  void SetTxnInitFailState(bool is_failed);
  void SetTxnStartState(bool is_start);

  void AddKeysToWatch(const std::vector<std::string> &db_keys);
  void RemoveWatchedKeys();
  void SetTxnFailedFromKeys(const std::vector<std::string> &db_keys);
  void SetAllTxnFailed();
  void SetTxnFailedFromDBs(std::string db_name);
  void ExitTxn();

  net::ServerThread* server_thread() { return server_thread_; }

  AuthStat& auth_stat() { return auth_stat_; }

  std::atomic<int> resp_num;
  std::vector<std::shared_ptr<std::string>> resp_array;

 private:
  net::ServerThread* const server_thread_;
  std::string current_db_;
  WriteCompleteCallback write_completed_cb_;
  bool is_pubsub_ = false;
  std::queue<std::shared_ptr<Cmd>> txn_cmd_que_;
  std::bitset<16> txn_state_;
  std::unordered_set<std::string> watched_db_keys_;
  std::mutex txn_state_mu_;

  std::shared_ptr<Cmd> DoCmd(const PikaCmdArgsType& argv, const std::string& opt,
                             const std::shared_ptr<std::string>& resp_ptr);

  void ProcessSlowlog(const PikaCmdArgsType& argv, uint64_t start_us, uint64_t do_duration);
  void ProcessMonitor(const PikaCmdArgsType& argv);

  void ExecRedisCmd(const PikaCmdArgsType& argv, const std::shared_ptr<std::string>& resp_ptr);
  void TryWriteResp();

  AuthStat auth_stat_;
};

struct ClientInfo {
  int fd;
  std::string ip_port;
  int64_t last_interaction = 0;
  std::shared_ptr<PikaClientConn> conn;
};

extern bool AddrCompare(const ClientInfo& lhs, const ClientInfo& rhs);
extern bool IdleCompare(const ClientInfo& lhs, const ClientInfo& rhs);

#endif
