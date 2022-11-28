#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>

#include "pink/include/server_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/pb_conn.h"
#include "pink/include/pink_thread.h"
#include "myproto.pb.h"

using namespace pink;

class MyConn: public PbConn {
 public:
  MyConn(int fd, const std::string& ip_port, Thread *thread,
         void* worker_specific_data);
  virtual ~MyConn();

 protected:
  virtual int DealMessage();

 private:
  myproto::Ping ping_;
  myproto::PingRes ping_res_;
};

MyConn::MyConn(int fd, const std::string& ip_port,
               Thread *thread, void* worker_specific_data)
    : PbConn(fd, ip_port, thread) {
  // Handle worker_specific_data ...
}

MyConn::~MyConn() {
}

int MyConn::DealMessage() {
  printf("In the myconn DealMessage branch\n");
  ping_.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  printf ("DealMessage receive (%s) port %d \n", ping_.address().c_str(), ping_.port());

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
  virtual std::shared_ptr<PinkConn> NewPinkConn(int connfd, const std::string &ip_port,
                                                Thread *thread,
                                                void* worker_specific_data,
                                                PinkEpoll* pink_epoll) const {
    return std::make_shared<MyConn>(connfd, ip_port, thread, worker_specific_data);
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
  if (argc < 2) {
    printf ("Usage: ./server port\n");
    exit(0);
  }

  int my_port = (argc > 1) ? atoi(argv[1]) : 8221;

  SignalSetup();

  ConnFactory *conn_factory = new MyConnFactory();

  ServerThread* my_thread = NewHolyThread(my_port, conn_factory);
  if (my_thread->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
  }
  my_thread->StopThread();

  delete my_thread;
  delete conn_factory;

  return 0;
}
