#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <atomic>

#include "myproto.pb.h"
#include "net/include/net_conn.h"
#include "net/include/net_thread.h"
#include "net/include/pb_conn.h"
#include "net/include/server_thread.h"
#include "net/src/net_multiplexer.h"

using namespace net;

class MyConn : public PbConn {
 public:
  MyConn(int32_t fd, const std::string& ip_port, Thread* thread, void* worker_specific_data);
  virtual ~MyConn();

 protected:
  virtual int32_t DealMessage();

 private:
  myproto::Ping ping_;
  myproto::PingRes ping_res_;
};

MyConn::MyConn(int32_t fd, const std::string& ip_port, Thread* thread, void* worker_specific_data)
    : PbConn(fd, ip_port, thread) {
  // Handle worker_specific_data ...
}

MyConn::~MyConn() {}

int32_t MyConn::DealMessage() {
  printf("In the myconn DealMessage branch\n");
  ping_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  printf("DealMessage receive (%s) port %d \n", ping_.address().c_str(), ping_.port());

  ping_res_.Clear();
  ping_res_.set_res(11234);
  ping_res_.set_mess("heiheidfdfdf");
  std::string res;
  ping_res_.SerializeToString(&res);
  WriteResp(res);
  return 0;
}

class MyConnFactory : public ConnFactory {
 public:
  virtual std::shared_ptr<NetConn> NewNetConn(int32_t connfd, const std::string& ip_port, Thread* thread,
                                              void* worker_specific_data, NetMultiplexer* net_epoll) const override {
    return std::make_shared<MyConn>(connfd, ip_port, thread, worker_specific_data);
  }
};

static std::atomic<bool> running(false);

static void IntSigHandle(const int32_t sig) {
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

int32_t main(int32_t argc, char* argv[]) {
  if (argc < 2) {
    printf("Usage: ./server port\n");
    exit(0);
  }

  int32_t my_port = (argc > 1) ? atoi(argv[1]) : 8221;

  SignalSetup();

  std::unique_ptr<ConnFactory> conn_factory = std::make_unique<MyConnFactory>();

  std::unique_ptr<ServerThread> my_thread(NewHolyThread(my_port, conn_factory.get()));
  if (my_thread->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
  }
  my_thread->StopThread();

  return 0;
}
