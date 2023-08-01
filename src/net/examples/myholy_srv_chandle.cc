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
  MyConn(int32_t fd, std::string ip_port, Thread* thread, void* private_data);
  virtual ~MyConn();

  Thread* thread() { return thread_; }

 protected:
  virtual int32_t DealMessage();

 private:
  Thread* thread_;
  int32_t* private_data_;
  myproto::Ping ping_;
  myproto::PingRes ping_res_;
};

MyConn::MyConn(int32_t fd, ::std::string ip_port, Thread* thread, void* worker_specific_data)
    : PbConn(fd, ip_port, thread), thread_(thread), private_data_(static_cast<int32_t*>(worker_specific_data)) {}

MyConn::~MyConn() {}

int32_t MyConn::DealMessage() {
  printf("In the myconn DealMessage branch\n");
  ping_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  printf("DealMessage receive (%s) port %d \n", ping_.address().c_str(), ping_.port());

  int32_t* data = static_cast<int32_t*>(private_data_);
  printf("Worker's Env: %d\n", *data);

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

class MyServerHandle : public ServerHandle {
 public:
  virtual void CronHandle() const override { printf("Cron operation\n"); }
  using ServerHandle::AccessHandle;
  virtual bool AccessHandle(std::string& ip) const override {
    printf("Access operation, receive:%s\n", ip.c_str());
    return true;
  }
  virtual int32_t CreateWorkerSpecificData(void** data) const {
    int32_t* num = new int32_t(1234);
    *data = static_cast<void*>(num);
    return 0;
  }
  virtual int32_t DeleteWorkerSpecificData(void* data) const {
    delete static_cast<int32_t*>(data);
    return 0;
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

  MyConnFactory conn_factory;
  MyServerHandle handle;

  std::unique_ptr<ServerThread> my_thread(NewHolyThread(my_port, &conn_factory, 1000, &handle));
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
