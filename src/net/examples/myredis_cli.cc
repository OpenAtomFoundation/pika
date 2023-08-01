#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <atomic>
#include <map>
#include <thread>

#include "net/include/client_thread.h"
#include "net/include/net_conn.h"
#include "net/include/net_thread.h"
#include "net/include/redis_conn.h"
#include "net/src/net_multiplexer.h"

using namespace net;

class MyConn : public RedisConn {
 public:
  MyConn(int32_t fd, const std::string& ip_port, Thread* thread, void* worker_specific_data);
  virtual ~MyConn() = default;

 protected:
  int32_t DealMessage(const RedisCmdArgsType& argv, std::string* response) override;

 private:
};

MyConn::MyConn(int32_t fd, const std::string& ip_port, Thread* thread, void* worker_specific_data)
    : RedisConn(fd, ip_port, thread) {
  // Handle worker_specific_data ...
}

std::unique_ptr<ClientThread> client;
int32_t sendto_port;
int32_t MyConn::DealMessage(const RedisCmdArgsType& argv, std::string* response) {
  sleep(1);
  std::cout << "DealMessage" << std::endl;
  std::string set = "*3\r\n$3\r\nSet\r\n$3\r\nabc\r\n$3\r\nabc\r\n";
  client->Write("127.0.0.1", sendto_port, set);
  return 0;
}

class MyConnFactory : public ConnFactory {
 public:
  virtual std::shared_ptr<NetConn> NewNetConn(int32_t connfd, const std::string& ip_port, Thread* thread,
                                              void* worker_specific_data,
                                              net::NetMultiplexer* net_epoll = nullptr) const override {
    return std::make_shared<MyConn>(connfd, ip_port, thread, worker_specific_data);
  }
};

class MyClientHandle : public net::ClientHandle {
 public:
  void CronHandle() const override {}
  void FdTimeoutHandle(int32_t fd, const std::string& ip_port) const override;
  void FdClosedHandle(int32_t fd, const std::string& ip_port) const override;
  bool AccessHandle(std::string& ip) const override { return true; }
  int32_t CreateWorkerSpecificData(void** data) const override { return 0; }
  int32_t DeleteWorkerSpecificData(void* data) const override { return 0; }
  void DestConnectFailedHandle(std::string ip_port, std::string reason) const override {}
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

bool first_time = true;
void DoCronWork(ClientThread* client, int32_t port) {
  if (first_time) {
    first_time = false;
    std::string ping = "*1\r\n$4\r\nPING\r\n";
    client->Write("127.0.0.1", port, ping);
  }
}

int32_t main(int32_t argc, char* argv[]) {
  if (argc < 2) {
    printf("client will send to 6379\n");
  } else {
    printf("client will send to %d\n", atoi(argv[1]));
  }

  sendto_port = (argc > 1) ? atoi(argv[1]) : 6379;

  SignalSetup();

  std::unique_ptr<ConnFactory> conn_factory = std::make_unique<MyConnFactory>();
  //"handle" will be deleted within "client->StopThread()"
  ClientHandle* handle = new ClientHandle();

  client = std::make_unique<ClientThread>(conn_factory.get(), 3000, 60, handle, nullptr);

  if (client->StartThread() != 0) {
    printf("StartThread error happened!\n");
    exit(-1);
  }
  running.store(true);
  while (running.load()) {
    sleep(1);
    DoCronWork(client.get(), sendto_port);
  }

  client->StopThread();
  client.reset();
  return 0;
}
