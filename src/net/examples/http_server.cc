// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <string>
#include <chrono>
#include <atomic>
#include <signal.h>

#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_hash.h"
#include "net/include/net_thread.h"
#include "net/include/server_thread.h"
#include "net/include/http_conn.h"

using namespace net;

class MyHTTPHandles : public net::HTTPHandles {
 public:
  std::string body_data;
  std::string body_md5;
  std::string zero_space;
  size_t write_pos = 0;
  std::chrono::system_clock::time_point start, end;
  std::chrono::duration<double, std::milli> diff;

  // Request handles
  virtual bool HandleRequest(const HTTPRequest* req) {
    req->Dump();
    body_data.clear();

    start = std::chrono::system_clock::now();

    // Continue receive body
    return false;
  }
  virtual void HandleBodyData(const char* data, size_t size) {
    std::cout << "ReqBodyPartHandle: " << size << std::endl;
    body_data.append(data, size);
  }

  // Response handles
  virtual void PrepareResponse(HTTPResponse* resp) {
    body_md5.assign(pstd::md5(body_data));

    resp->SetStatusCode(200);
    resp->SetContentLength(body_md5.size());
    write_pos = 0;
    end = std::chrono::system_clock::now();
    diff = end - start;
    std::cout << "Use: " << diff.count() << " ms" << std::endl;
  }

  virtual int WriteResponseBody(char* buf, size_t max_size) {
    size_t size = std::min(max_size, body_md5.size() - write_pos);
    memcpy(buf, body_md5.data() + write_pos, size);
    write_pos += size;
    return size;
  }
};

class MyConnFactory : public ConnFactory {
 public:
  virtual std::shared_ptr<NetConn> NewNetConn(int connfd, const std::string& ip_port,
                                                Thread* thread,
                                                void* worker_specific_data,
                                                NetEpoll* net_epoll) const {
    auto my_handles = std::make_shared<MyHTTPHandles>();
    return std::make_shared<net::HTTPConn>(connfd, ip_port, thread, my_handles,
                                       worker_specific_data);
  }
};

static std::atomic<bool> running(false);

static void IntSigHandle(const int sig) {
  printf("Catch Signal %d, cleanup...\n", sig);
  running.store(false);
  printf("server Exit");
}

static void SignalSetup() {
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, &IntSigHandle);
  signal(SIGQUIT, &IntSigHandle);
  signal(SIGTERM, &IntSigHandle);
}

int main(int argc, char* argv[]) {
  int port;
  if (argc < 2) {
    printf("Usage: ./http_server port");
  } else {
    port = atoi(argv[1]);
  }

  SignalSetup();

  ConnFactory* my_conn_factory = new MyConnFactory();
  ServerThread *st = NewDispatchThread(port, 4, my_conn_factory, 1000);

  if (st->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
  }
  st->StopThread();

  delete st;
  delete my_conn_factory;

  return 0;
}
