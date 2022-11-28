#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <atomic>
#include <map>
#include <thread>

#include "pink/include/client_thread.h"
#include "pink/include/pink_conn.h"
#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"

using namespace pink;

class MyConn: public RedisConn {
 public:
  MyConn(int fd, const std::string& ip_port, Thread *thread,
         void* worker_specific_data);
  virtual ~MyConn() = default;

 protected:
  int DealMessage(const RedisCmdArgsType& argv, std::string* response) override;

 private:
};

MyConn::MyConn(int fd, const std::string& ip_port,
               Thread *thread, void* worker_specific_data)
    : RedisConn(fd, ip_port, thread) {
  // Handle worker_specific_data ...
}

ClientThread* client;
int sendto_port;
int MyConn::DealMessage(const RedisCmdArgsType& argv, std::string* response) {
  sleep(1);
  std::cout << "DealMessage" << std::endl; 
  std::string set = "*3\r\n$3\r\nSet\r\n$3\r\nabc\r\n$3\r\nabc\r\n"; 
  client->Write("127.0.0.1", sendto_port, set);
  return 0;
}

class MyConnFactory : public ConnFactory {
 public:
  virtual std::shared_ptr<PinkConn> NewPinkConn(int connfd, const std::string &ip_port,
                                Thread *thread,
                                void* worker_specific_data, pink::PinkEpoll* pink_epoll=nullptr) const {
    return std::make_shared<MyConn>(connfd, ip_port, thread, worker_specific_data);
  }
};

class MyClientHandle : public pink::ClientHandle {
 public:
  void CronHandle() const override {
  }
  void FdTimeoutHandle(int fd, const std::string& ip_port) const override;
  void FdClosedHandle(int fd, const std::string& ip_port) const override;
  bool AccessHandle(std::string& ip) const override {
    return true;
  }
  int CreateWorkerSpecificData(void** data) const override {
    return 0;
  }
  int DeleteWorkerSpecificData(void* data) const override {
    return 0;
  }
  void DestConnectFailedHandle(std::string ip_port, std::string reason) const override {
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

bool first_time = true;
void DoCronWork(ClientThread* client, int port) {
  if (first_time) {
    first_time = false;
    std::string ping = "*1\r\n$4\r\nPING\r\n";
    client->Write("127.0.0.1", port, ping);
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("client will send to 6379\n");
  } else {
    printf("client will send to %d\n", atoi(argv[1]));
  }

  sendto_port = (argc > 1) ? atoi(argv[1]) : 6379;

  SignalSetup();
  
  ConnFactory *conn_factory = new MyConnFactory();
  ClientHandle *handle = new ClientHandle();

  client = new ClientThread(conn_factory, 3000, 60, handle, NULL);
  
  if (client->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
    DoCronWork(client, sendto_port);
  }

  client->StopThread();
  delete client;
  delete conn_factory;
  return 0;
}
