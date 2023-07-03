// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <signal.h>

#include <atomic>
#include <string>

#include "net/include/net_thread.h"
#include "net/include/server_thread.h"
#include "net/include/simple_http_conn.h"
#include "net_multiplexer.h"
#include "pstd/include/pstd_status.h"

using namespace net;

class MyHTTPConn : public net::SimpleHTTPConn {
 public:
  MyHTTPConn(const int fd, const std::string& ip_port, Thread* worker)
      : SimpleHTTPConn(fd, ip_port, worker) {}
  virtual void DealMessage(const net::Request* req, net::Response* res) {
    std::cout << "handle get" << std::endl;
    std::cout << " + method: " << req->method << std::endl;
    std::cout << " + path: " << req->path << std::endl;
    std::cout << " + version: " << req->version << std::endl;
    std::cout << " + content: " << req->content << std::endl;
    std::cout << " + headers: " << std::endl;
    for (auto& h : req->headers) {
      std::cout << "   + " << h.first << ":" << h.second << std::endl;
    }
    std::cout << " + query_params: " << std::endl;
    for (auto& q : req->query_params) {
      std::cout << "   + " << q.first << ":" << q.second << std::endl;
    }
    std::cout << " + post_params: " << std::endl;
    for (auto& q : req->post_params) {
      std::cout << "   + " << q.first << ":" << q.second << std::endl;
    }

    res->SetStatusCode(200);
    res->SetBody("china");
  }
};

class MyConnFactory : public ConnFactory {
 public:
  virtual std::shared_ptr<NetConn> NewNetConn(int connfd,
                                              const std::string& ip_port,
                                              Thread* thread,
                                              void* worker_specific_data,
                                              NetMultiplexer* net_epoll) const {
    return std::make_shared<MyHTTPConn>(connfd, ip_port, thread);
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
    printf("Usage: ./simple_http_server port");
  } else {
    port = atoi(argv[1]);
  }

  SignalSetup();

  std::unique_ptr<ConnFactory> my_conn_factory =
      std::make_unique<MyConnFactory>();
  std::unique_ptr<ServerThread> st(
      NewDispatchThread(port, 4, my_conn_factory.get(), 1000));

  if (st->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
  }
  st->StopThread();

  return 0;
}
