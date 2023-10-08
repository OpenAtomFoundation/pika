/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <thread>

#include "net/event_loop.h"
#include "net/http_client.h"
#include "net/http_server.h"

namespace pikiwidb {

class IOThreadPool {
 public:
  static IOThreadPool& Instance();
  ~IOThreadPool();

  bool Init(const char* ip, int port, NewTcpConnectionCallback ccb);
  void Run(int argc, char* argv[]);
  void Exit();
  bool IsExit() const;
  EventLoop* BaseLoop();

  // choose a loop
  EventLoop* Next();

  // set worker threads, each thread has a EventLoop object
  bool SetWorkerNum(size_t n);

  // app name, for top command
  void SetName(const std::string& name);

  // TCP server
  bool Listen(const char* ip, int port, NewTcpConnectionCallback ccb);

  // TCP client
  void Connect(const char* ip, int port, NewTcpConnectionCallback ccb, TcpConnectionFailCallback fcb = TcpConnectionFailCallback(),
               EventLoop* loop = nullptr);

  // HTTP server
  std::shared_ptr<HttpServer> ListenHTTP(const char* ip, int port,
                                         HttpServer::OnNewClient cb = HttpServer::OnNewClient());

  // HTTP client
  std::shared_ptr<HttpClient> ConnectHTTP(const char* ip, int port, EventLoop* loop = nullptr);

  // for unittest only
  void Reset();

 private:
  IOThreadPool();
  void StartWorkers();

  static const size_t kMaxWorkers;

  std::string name_;
  std::string listen_ip_;
  int listen_port_;
  NewTcpConnectionCallback new_conn_cb_;

  EventLoop base_;

  std::atomic<size_t> worker_num_{0};
  std::vector<std::thread> workers_;
  std::vector<std::unique_ptr<EventLoop>> loops_;
  mutable std::atomic<size_t> current_loop_{0};

  enum class State {
    kNone,
    kStarted,
    kStopped,
  };
  std::atomic<State> state_;
};

}  // namespace pikiwidb