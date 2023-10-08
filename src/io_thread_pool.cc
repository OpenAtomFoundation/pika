/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "io_thread_pool.h"

#include <signal.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "log.h"
#include "util.h"

static void SignalHandler(int) { pikiwidb::IOThreadPool::Instance().Exit(); }

static void InitSignal() {
  struct sigaction sig;
  ::memset(&sig, 0, sizeof(sig));

  sig.sa_handler = SignalHandler;
  sigaction(SIGINT, &sig, NULL);

  // ignore sigpipe
  sig.sa_handler = SIG_IGN;
  sigaction(SIGPIPE, &sig, NULL);
}

namespace pikiwidb {

const size_t IOThreadPool::kMaxWorkers = 128;

IOThreadPool::~IOThreadPool() {}

IOThreadPool& IOThreadPool::Instance() {
  static IOThreadPool app;
  return app;
}

bool IOThreadPool::SetWorkerNum(size_t num) {
  if (num <= 1) {
    return true;
  }

  if (state_ != State::kNone) {
    ERROR("can only called before application run");
    return false;
  }

  if (!loops_.empty()) {
    ERROR("can only called once, not empty loops size: {}", loops_.size());
    return false;
  }

  if (num > kMaxWorkers) {
    ERROR("number of threads can't exceeds {}, now is {}", kMaxWorkers, num);
    return false;
  }

  worker_num_.store(num);
  workers_.reserve(num);
  loops_.reserve(num);

  return true;
}

bool IOThreadPool::SetSlaveNum(size_t num){
  if (num <= 1){
    return true;
  }

  if (state_ != State::kNone){
    ERROR("can only called before application run");
    return false;
  }

  if (!slave_loops_.empty()){
    ERROR("can only called once, not empty slave loops size: {}", slave_loops_.size());
    return false;
  }

  if (num > kMaxWorkers){
    ERROR("number of threads can't exceeds {}, now is {}",kMaxWorkers, num);
    return false;
  }

  slave_num_.store(num);
  slave_workers_.reserve(num);
  slave_loops_.reserve(num);

  return true;
}

bool IOThreadPool::Init(const char* ip, int port, NewTcpConnectionCallback cb) {
  auto f = std::bind(&IOThreadPool::Next, this);

  base_.Init();
  printf("base loop %s %p, g_baseLoop %p\n", base_.GetName().c_str(), &base_, base_.Self());
  if (!base_.Listen(ip, port, cb, f)) {
    ERROR("can not bind socket on addr {}:{}", ip, port);
    return false;
  }

  return true;
}

void IOThreadPool::Run(int ac, char* av[]) {
  assert(state_ == State::kNone);
  INFO("Process starting...");

  // start loops in thread pool
  StartWorkers();
  StartSlaves();
  state_ = State::kStarted;

  base_.Run();

  for (auto& w : workers_) {
    w.join();
  }
  
  for (auto& slave_worker : slave_workers_ ){
    slave_worker.join();
  }

  workers_.clear();
  slave_workers_.clear();

  INFO("Process stopped, goodbye...");
}

void IOThreadPool::Exit() {
  state_ = State::kStopped;

  BaseLoop()->Stop();

  for (size_t index = 0; index < loops_.size(); ++index) {
    EventLoop* loop = loops_[index].get();
    loop->Stop();
  }

  for (size_t index = 0; index < slave_loops_.size(); ++index){
    EventLoop* slave_loop = slave_loops_[index].get();
    slave_loop->Stop();
  }
}

bool IOThreadPool::IsExit() const { return state_ == State::kStopped; }

EventLoop* IOThreadPool::BaseLoop() { return &base_; }

EventLoop* IOThreadPool::Next() {
  if (loops_.empty()) {
    return BaseLoop();
  }

  auto& loop = loops_[current_loop_++ % loops_.size()];
  return loop.get();
}

EventLoop* IOThreadPool::SlaveNext(){
  if (slave_loops_.empty()){
    return BaseLoop();
  }

  auto& slave_loop = slave_loops_[current_slave_loop_++ % slave_loops_.size()];
  return slave_loop.get();
}

void IOThreadPool::StartWorkers() {
  // only called by main thread
  assert(state_ == State::kNone);

  size_t index = 1;
  while (loops_.size() < worker_num_) {
    std::unique_ptr<EventLoop> loop(new EventLoop);
    if (!name_.empty()) {
      loop->SetName(name_ + "_" + std::to_string(index++));
      printf("loop %p, name %s\n", loop.get(), loop->GetName().c_str());
    }
    loops_.push_back(std::move(loop));
  }

  for (index = 0; index < loops_.size(); ++index) {
    EventLoop* loop = loops_[index].get();
    std::thread t([loop]() {
      loop->Init();
      loop->Run();
    });
    printf("thread %lu, thread loop %p, loop name %s \n", index, loop, loop->GetName().c_str());
    workers_.push_back(std::move(t));
  }

  // state_ = State::kStarted;
}

void IOThreadPool::StartSlaves(){
  // only called by main thread
  assert(state_ == State::kNone);

  size_t index = 1;
  while(slave_loops_.size() < slave_num_){
    std::unique_ptr<EventLoop> slave_loop(new EventLoop);
    if (!name_.empty()) {
      slave_loop->SetName(name_ + "-slave_" + std::to_string(index++));
      printf("loop %p, name %s\n", slave_loop.get(), slave_loop->GetName().c_str());
    }
    slave_loops_.push_back(std::move(slave_loop));
  }

  for (index = 0; index < slave_loops_.size(); ++index){
    EventLoop* slave_loop = slave_loops_[index].get();
    std::thread t([slave_loop]() {
      slave_loop->Init();
      slave_loop->Run();
    });
    printf("thread %lu, thread loop %p, loop name %s \n", index, slave_loop, slave_loop->GetName().c_str());
    slave_workers_.push_back(std::move(t));
  }
}

void IOThreadPool::SetName(const std::string& name) { name_ = name; }

IOThreadPool::IOThreadPool() : state_(State::kNone) { InitSignal(); }

bool IOThreadPool::Listen(const char* ip, int port, NewTcpConnectionCallback ccb) {
  auto f = std::bind(&IOThreadPool::Next, this);
  auto loop = BaseLoop();
  return loop->Execute([loop, ip, port, ccb, f]() { return loop->Listen(ip, port, std::move(ccb), f); }).get();
}

void IOThreadPool::Connect(const char* ip, int port, NewTcpConnectionCallback ccb, TcpConnectionFailCallback fcb, EventLoop* loop) {
  if (!loop) {
    loop = Next();
  }

  std::string ipstr(ip);
  loop->Execute(
      [loop, ipstr, port, ccb, fcb]() { loop->Connect(ipstr.c_str(), port, std::move(ccb), std::move(fcb)); });
}

std::shared_ptr<HttpServer> IOThreadPool::ListenHTTP(const char* ip, int port, HttpServer::OnNewClient cb) {
  auto server = std::make_shared<HttpServer>();
  server->SetOnNewHttpContext(std::move(cb));

  // capture server to make it long live with TcpListener
  auto ncb = [server](TcpConnection* conn) { server->OnNewConnection(conn); };
  Listen(ip, port, ncb);

  return server;
}

std::shared_ptr<HttpClient> IOThreadPool::ConnectHTTP(const char* ip, int port, EventLoop* loop) {
  auto client = std::make_shared<HttpClient>();

  // capture client to make it long live with TcpConnection
  auto ncb = [client](TcpConnection* conn) { client->OnConnect(conn); };
  auto fcb = [client](EventLoop*, const char* ip, int port) { client->OnConnectFail(ip, port); };

  if (!loop) {
    loop = Next();
  }
  client->SetLoop(loop);
  Connect(ip, port, std::move(ncb), std::move(fcb), loop);

  return client;
}

void IOThreadPool::Reset() {
  state_ = State::kNone;
  BaseLoop()->Reset();
}

}  // namespace pikiwidb
