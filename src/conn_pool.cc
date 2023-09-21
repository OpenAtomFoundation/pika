#include "conn_pool.h"

#include <signal.h>

#include <cassert>
#include <cstdlib>
#include <cstring>

#include "log.h"
#include "util.h"

static void SignalHandler(int) { pikiwidb::ConnPool::Instance().Exit(); }

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

const size_t ConnPool::kMaxWorkers = 128;

ConnPool::~ConnPool() {}

ConnPool& ConnPool::Instance() {
  static ConnPool app;
  return app;
}

bool ConnPool::SetWorkerNum(size_t num) {
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

bool ConnPool::Init(const char* ip, int port, NewTcpConnCallback cb) {
  base_.Init();
  if (!base_.Listen(ip, port, cb)) {
    ERROR("can not bind socket on addr {}:{}", ip, port);
    return false;
  }

  return true;
}

void ConnPool::Run(int ac, char* av[]) {
  assert(state_ == State::kNone);
  INFO("Process starting...");

  // start loops in thread pool
  StartWorkers();
  base_.Run();

  for (auto& thd : workers_) {
    thd.join();
  }
  workers_.clear();

  INFO("Process stopped, goodbye...");
}

void ConnPool::Exit() {
  state_ = State::kStopped;

  BaseLoop()->Stop();
  for (size_t index = 0; index < loops_.size(); ++index) {
    EventLoop* loop = loops_[index].get();
    loop->Stop();
  }
}

bool ConnPool::IsExit() const { return state_ == State::kStopped; }

EventLoop* ConnPool::BaseLoop() { return &base_; }

EventLoop* ConnPool::Next() {
  if (loops_.empty()) {
    return BaseLoop();
  }

  auto& loop = loops_[current_loop_++ % loops_.size()];
  return loop.get();
}

void ConnPool::StartWorkers() {
  // only called by main thread
  assert(state_ == State::kNone);

  size_t index = 1;
  while (loops_.size() < worker_num_) {
    std::unique_ptr<EventLoop> loop(new EventLoop);
    if (!name_.empty()) {
      loop->SetName(name_ + "_" + std::to_string(index++));
    }
    loops_.push_back(std::move(loop));
  }

  for (index = 0; index < loops_.size(); ++index) {
    EventLoop* loop = loops_[index].get();
    std::thread t([loop]() {
      loop->Init();

      loop->Run();
    });
    workers_.push_back(std::move(t));
  }

  state_ = State::kStarted;
}

void ConnPool::SetName(const std::string& name) { name_ = name; }

ConnPool::ConnPool() : state_(State::kNone) { InitSignal(); }

bool ConnPool::Listen(const char* ip, int port, NewTcpConnCallback ccb) {
  auto loop = BaseLoop();
  return loop->Execute([loop, ip, port, ccb]() { return loop->Listen(ip, port, std::move(ccb)); }).get();
}

void ConnPool::Connect(const char* ip, int port, NewTcpConnCallback ccb, TcpConnFailCallback fcb, EventLoop* loop) {
  if (!loop) {
    loop = Next();
  }

  std::string ipstr(ip);
  loop->Execute(
      [loop, ipstr, port, ccb, fcb]() { loop->Connect(ipstr.c_str(), port, std::move(ccb), std::move(fcb)); });
}

std::shared_ptr<HttpServer> ConnPool::ListenHTTP(const char* ip, int port, HttpServer::OnNewClient cb) {
  auto server = std::make_shared<HttpServer>();
  server->SetOnNewHttpContext(std::move(cb));

  // capture server to make it long live with TcpListener
  auto ncb = [server](TcpObject* conn) { server->OnNewConnection(conn); };
  Listen(ip, port, ncb);

  return server;
}

std::shared_ptr<HttpClient> ConnPool::ConnectHTTP(const char* ip, int port, EventLoop* loop) {
  auto client = std::make_shared<HttpClient>();

  // capture client to make it long live with TcpObject
  auto ncb = [client](TcpObject* conn) { client->OnConnect(conn); };
  auto fcb = [client](EventLoop*, const char* ip, int port) { client->OnConnectFail(ip, port); };

  if (!loop) {
    loop = Next();
  }
  client->SetLoop(loop);
  Connect(ip, port, std::move(ncb), std::move(fcb), loop);

  return client;
}

void ConnPool::Reset() {
  state_ = State::kNone;
  BaseLoop()->Reset();
}

}  // namespace pikiwidb
